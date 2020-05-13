use std::io::Read;
use std::io::SeekFrom;

use byteorder::{BigEndian, ReadBytesExt};
use serde::de::DeserializeOwned;
use thiserror::Error;
use tokio::fs::{File, OpenOptions};
use tokio::io;
use tokio::io::AsyncReadExt;

use crate::fs::*;
use crate::{CompressionFormat, Queue, QueueMetadata};
use std::time::Duration;

pub struct Receiver {
  current_file: Option<FdAdvisoryLock<File>>,
  current_file_index: u64,
  metadata: QueueMetadata,
  current_index: u64,
  #[cfg(feature = "compression-snappy")]
  snappy_decoder: Option<snap::read::FrameDecoder<FdAdvisoryLock<std::fs::File>>>,
  #[cfg(feature = "compression-lz4")]
  lz4_decoder: Option<lz4::Decoder<FdAdvisoryLock<std::fs::File>>>,
}

#[derive(Error, Debug)]
pub enum ReceiverError {
  #[error("underlying filesystem error")]
  IoError(#[from] io::Error),
  #[error("deserialization error")]
  DeserializationError(#[from] bincode::Error),
  #[error("could not acquire resource")]
  ResourceAcquisitionError(#[from] crate::fs::LockError),
  #[error("invalid file found `{0}`")]
  InvalidFile(u64),
  #[error("unsupported format `{0}`")]
  UnsupportedFormat(String),
  #[error("unexpected end of stream reached")]
  EndOfStream,
}

impl Receiver {
  pub(crate) async fn new(metadata: QueueMetadata) -> Result<Receiver, ReceiverError> {
    let receiver = Receiver {
      current_file: None,
      current_file_index: 0,
      metadata,
      current_index: 0,
      #[cfg(feature = "compression-snappy")]
      snappy_decoder: None,
      #[cfg(feature = "compression-lz4")]
      lz4_decoder: None,
    };

    Ok(receiver)
  }

  pub async fn next_bytes(&mut self) -> Result<Option<Vec<u8>>, ReceiverError> {
    loop {
      match self.try_read_bytes().await {
        Err(ReceiverError::IoError(_)) | Ok(None) => {
          match self.update_file().await {
            Ok(true) => (),
            Err(ReceiverError::EndOfStream) | Ok(false) => {
              tokio::time::delay_for(Duration::from_micros(10)).await;
              continue;
            }
            Err(e) => return Err(e)
          }
        }
        Ok(Some(buf)) => return Ok(Some(buf)),
        Err(e) => return Err(e)
      }
    }
  }

  pub async fn next<T: DeserializeOwned + Clone>(&mut self) -> Result<Option<T>, ReceiverError> {
    if let Some(bytes) = self.next_bytes().await? {
      let result: T = bincode::deserialize_from(&bytes[..])?;
      Ok(Some(result))
    } else {
      Ok(None)
    }
  }

  async fn try_read_bytes(&mut self) -> Result<Option<Vec<u8>>, ReceiverError> {
    if let Some(ref mut file) = self.current_file {
      file.seek(SeekFrom::Start(self.current_index)).await?;

      let data_size = file.read_u64().await?;
      let mut data_buf = vec![0u8; data_size as usize];
      file.read_exact(&mut data_buf).await?;

      self.current_index += data_size + std::mem::size_of::<u64>() as u64;

      Ok(Some(data_buf))
    } else {
      match self.metadata.compression {
        #[cfg(feature = "compression-snappy")]
        CompressionFormat::Snappy => {
          if let Some(ref mut decoder) = self.snappy_decoder {
            let (len, buf) = tokio::task::block_in_place(|| -> std::io::Result<(u64, Vec<u8>)> {
              let data_size = decoder.read_u64::<BigEndian>()?;
              let mut data_buf = vec![0u8; data_size as usize];
              decoder.read_exact(&mut data_buf)?;

              Ok((data_size + std::mem::size_of::<u64>() as u64, data_buf))
            })?;

            self.current_index += len;
            Ok(Some(buf))
          } else {
            Ok(None)
          }
        }

        #[cfg(feature = "compression-lz4")]
        CompressionFormat::LZ4 => {
          if let Some(ref mut decoder) = self.lz4_decoder {
            let (len, buf) = tokio::task::block_in_place(|| -> std::io::Result<(u64, Vec<u8>)> {
              let data_size = decoder.read_u64::<BigEndian>()?;
              let mut data_buf = vec![0u8; data_size as usize];
              decoder.read_exact(&mut data_buf)?;

              Ok((data_size + std::mem::size_of::<u64>() as u64, data_buf))
            })?;

            self.current_index += len;
            Ok(Some(buf))
          } else {
            Ok(None)
          }
        }
        _ => Ok(None),
      }
    }
  }

  async fn update_file(&mut self) -> Result<bool, ReceiverError> {
    if self.metadata.epoch_cycle == self.metadata.roll_cycle.epoch_cycle() {
      return Ok(false);
    }

    let mut index_file = OpenOptions::new()
      .read(true)
      .open(self.metadata.hot_storage_path.join("index.aui"))
      .await?
      .lock_shared()
      .await?;

    let row_len = (std::mem::size_of::<u64>() + std::mem::size_of::<u8>() * 2) as u64;
    if index_file.metadata().await?.len() < row_len * (self.current_file_index + 1) {
      // There's no next file
      return Err(ReceiverError::EndOfStream);
    }

    index_file
      .seek(SeekFrom::Start(row_len * (self.current_file_index)))
      .await?;

    let next_file_epoch_cycle = index_file.read_u64().await?;
    let next_file_compressed = index_file.read_u8().await?;
    let next_file_storage = index_file.read_u8().await?;

    let next_file_compressed = next_file_compressed == crate::W_TRUE;

    let storage_path = match next_file_storage {
      crate::W_HOT_STORAGE => self.metadata.hot_storage_path.clone(),
      crate::W_COOL_STORAGE => {
        if let Some((_, storage_path)) = self.metadata.cool_storage_path.as_ref() {
          storage_path.clone()
        } else {
          return Err(ReceiverError::InvalidFile(next_file_epoch_cycle));
        }
      }
      crate::W_COLD_STORAGE => {
        if let Some((_, storage_path)) = self.metadata.cold_storage_path.as_ref() {
          storage_path.clone()
        } else {
          return Err(ReceiverError::InvalidFile(next_file_epoch_cycle));
        }
      }
      _ => panic!("Invalid storage found."),
    };

    let file_path = Queue::get_file_path(
      &storage_path,
      &self.metadata,
      next_file_epoch_cycle,
      next_file_compressed,
    );

    if !file_path.exists() {
      return Err(ReceiverError::EndOfStream);
    }

    match self.metadata.compression {
      #[cfg(feature = "compression-snappy")]
      CompressionFormat::Snappy if next_file_compressed => {
        let data_file = tokio::task::block_in_place(|| std::fs::OpenOptions::new().read(true).open(file_path))?;
        let data_file = data_file.lock_shared().await?;
        self.snappy_decoder = Some(snap::read::FrameDecoder::new(data_file));
      }

      #[cfg(feature = "compression-lz4")]
      CompressionFormat::LZ4 if next_file_compressed => {
        let data_file = tokio::task::block_in_place(|| std::fs::OpenOptions::new().read(true).open(file_path))?;
        let data_file = data_file.lock_shared().await?;
        self.lz4_decoder = Some(lz4::Decoder::new(data_file)?);
      }

      _ => {
        #[cfg(feature = "compressor-snappy")]
        {
          self.snappy_decoder = None;
        }

        #[cfg(feature = "compressor-lz4")]
        {
          self.lz4_decoder = None;
        }

        let data_file = OpenOptions::new()
          .read(true)
          .open(file_path)
          .await?
          .lock_shared()
          .await?;

        self.current_file = Some(data_file);
      }
    }

    self.current_file_index += 1;
    self.current_index = 0;
    self.metadata.epoch_cycle = next_file_epoch_cycle;

    Ok(true)
  }
}
