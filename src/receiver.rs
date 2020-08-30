use std::error::Error;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use memmap::{Mmap, MmapMut, MmapOptions};
use serde::de::DeserializeOwned;
use serde::export::PhantomData;
use thiserror::Error;
use tokio::fs::OpenOptions;

use crate::fs::LockError;
use crate::index::Index;
use crate::metadata::{ChannelMetadata, WireFormat};
use crate::CompressionFormat;
use std::fmt::Debug;

// TODO create common InternalRead interface that will read files depending on whether they are
// compressed or not

#[allow(dead_code)]
pub struct Receiver<T: DeserializeOwned + Debug> {
  metadata: ChannelMetadata,
  index: Index,
  data: Mmap,

  compressed_reader: Option<Box<dyn Read + Send>>,

  hot_storage_path: PathBuf,
  cool_storage_path: Option<PathBuf>,
  cold_storage_path: Option<PathBuf>,

  current_index_position: i64,
  current_cycle_timestamp: i64,
  current_cycle_offset: i64,

  element_type: PhantomData<T>,
}

#[derive(Error, Debug)]
pub enum ReceiverError {
  #[error("Unexpected end of stream reached")]
  EndOfStream,
  #[error("File lock acquisition error")]
  FdLockError(#[from] LockError),
  #[error("I/O Error")]
  IoError(#[from] std::io::Error),
  #[error("Indexing error")]
  IndexError(#[from] crate::index::IndexError),
  #[cfg(feature = "format-bincode")]
  #[error("Bincode deserialization error")]
  BincodeDeserializationError(#[from] bincode::Error),
  #[cfg(feature = "format-json")]
  #[error("Json deserialization error")]
  JsonDeserializationError(#[from] serde_json::Error),
  #[error("Unknown error")]
  UnknownError(#[from] Box<dyn Error>),
}

impl<T: DeserializeOwned + Debug> Receiver<T> {
  pub(crate) async fn new(
    hot_storage_path: impl AsRef<Path>,
    cool_storage_path: Option<impl AsRef<Path>>,
    cold_storage_path: Option<impl AsRef<Path>>,
    metadata: ChannelMetadata,
  ) -> Result<Self, ReceiverError> {
    let index = Index::new(&hot_storage_path, metadata.index_block_size).await?;

    Ok(Receiver {
      metadata,
      index,
      data: MmapMut::map_anon(1)?.make_read_only()?,
      compressed_reader: None,
      hot_storage_path: hot_storage_path.as_ref().to_path_buf(),
      cool_storage_path: cool_storage_path.map(|p| p.as_ref().to_path_buf()),
      cold_storage_path: cold_storage_path.map(|p| p.as_ref().to_path_buf()),
      current_index_position: -1,
      current_cycle_timestamp: 0,
      current_cycle_offset: 0,
      element_type: Default::default(),
    })
  }

  pub async fn recv(&mut self) -> Result<Option<T>, ReceiverError> {
    let next_index_position = (self.current_index_position + 1) as u64;
    if self.index.len() < next_index_position + 1 {
      return Ok(None);
    }

    self.refresh_index().await?;

    let (next_cycle_timestamp, last_cursor_position) = {
      let index_element = &self.index[next_index_position];
      (
        index_element.cycle_timestamp.load(Ordering::SeqCst),
        index_element.last_cursor_position.load(Ordering::SeqCst),
      )
    };

    if next_cycle_timestamp == 0 {
      // Stream ended or element not yet committed.
      return Ok(None);
    }

    if (self.compressed_reader.is_none() && self.data.len() < last_cursor_position.abs() as usize)
      || next_cycle_timestamp != self.current_cycle_timestamp
    {
      self.current_cycle_timestamp = next_cycle_timestamp;
      self.map_data_file().await?;
    }

    let index_element = &self.index[next_index_position];

    // Detect offset. If there was a change in cycles, we set the offset to prevent writing in the
    // middle of the file.
    if next_index_position > 0
      && self.index[next_index_position - 1]
        .cycle_timestamp
        .load(Ordering::SeqCst)
        < self.current_cycle_timestamp
    {
      self.current_cycle_offset = self.index[next_index_position - 1]
        .last_cursor_position
        .load(Ordering::SeqCst);
    }

    let read_cursor_position = if next_index_position == 0 {
      0
    } else {
      (&self.index[self.current_index_position])
        .last_cursor_position
        .load(Ordering::SeqCst)
    };

    let element_size = index_element.last_cursor_position.load(Ordering::SeqCst) - read_cursor_position;

    let data: T = if let Some(ref mut reader) = self.compressed_reader {
      match self.metadata.wire_format {
        #[cfg(feature = "format-bincode")]
        WireFormat::Bincode => bincode::deserialize_from(reader.take(element_size as u64))?,
        #[cfg(feature = "format-json")]
        WireFormat::Json => serde_json::de::from_reader(reader.take(element_size as u64))?,
      }
    } else {
      let reader = unsafe {
        let target_ptr = self
          .data
          .as_ptr()
          .add((read_cursor_position - self.current_cycle_offset) as usize);
        std::slice::from_raw_parts(target_ptr, element_size as usize)
      };

      match self.metadata.wire_format {
        #[cfg(feature = "format-bincode")]
        WireFormat::Bincode => bincode::deserialize_from(reader)?,
        #[cfg(feature = "format-json")]
        WireFormat::Json => serde_json::de::from_reader(reader)?,
      }
    };

    self.current_index_position = next_index_position as i64;

    Ok(Some(data))
  }

  async fn refresh_index(&mut self) -> Result<(), ReceiverError> {
    let next_index_position = (self.current_index_position + 1) as u64;

    if self.index.len() > next_index_position && self.index.last_mapped_index() < next_index_position {
      self.index.remap().await?;
    }

    Ok(())
  }

  async fn map_data_file(&mut self) -> Result<(), ReceiverError> {
    // TODO search in cool and cold storages if the cycle is below the thresholds
    // TODO add a shared lock to prevent files in use from being moved between storages

    let data_file_name = self.metadata.roll_cycle.data_file_name(self.current_cycle_timestamp);
    let mut data_file_path = self.hot_storage_path.join(data_file_name.clone());

    let mut is_compressed = false;
    if !data_file_path.exists() {
      data_file_path.set_extension(self.metadata.compression_format.extension());
      is_compressed = true;
    }

    let data_file = OpenOptions::new().read(true).open(data_file_path).await?;
    let data_file_size = data_file.metadata().await?.len() as usize;
    self.data = unsafe { MmapOptions::new().map(&data_file.into_std().await) }?;

    if is_compressed {
      let bytes = unsafe { std::slice::from_raw_parts(self.data.as_ptr(), data_file_size) };
      self.compressed_reader = match self.metadata.compression_format {
        #[cfg(feature = "compression-snappy")]
        CompressionFormat::Snappy => Some(Box::new(snap::read::FrameDecoder::new(bytes))),
        #[cfg(feature = "compression-lz4")]
        CompressionFormat::LZ4 => Some(Box::new(lz4::Decoder::new(bytes)?)),
        _ => None,
      };
    } else {
      self.compressed_reader = None;
    }

    Ok(())
  }
}
