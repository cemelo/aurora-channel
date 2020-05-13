use std::alloc::Layout;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::Serialize;
use thiserror::Error;
use tokio::fs::{File, OpenOptions};
use tokio::io;
use tokio::io::AsyncWriteExt;

use crate::crypto::Encryption;
use crate::{Queue, QueueMetadata, WireFormat};

pub struct Sender {
  current_file: File,
  metadata: QueueMetadata,
  live_senders: Arc<AtomicU64>,
}

#[derive(Error, Debug)]
pub enum SenderError {
  #[error("underlying filesystem error")]
  IoError(#[from] io::Error),
  #[cfg(feature = "format-bincode")]
  #[error("serialization error")]
  BincodeSerializationError(#[from] bincode::Error),
  #[cfg(feature = "format-json")]
  #[error("serialization error")]
  JsonSerializationError(#[from] serde_json::error::Error),
  #[error("memory layout error")]
  LayoutError(#[from] std::alloc::LayoutErr),
  #[error("unsupported format `{0}`")]
  UnsupportedFormat(String),
}

impl Sender {
  pub(crate) async fn new(mut metadata: QueueMetadata, live_senders: Arc<AtomicU64>) -> Result<Sender, SenderError> {
    metadata.epoch_cycle = metadata.roll_cycle.epoch_cycle();
    let cycle_file_path = Queue::get_file_path(&metadata.hot_storage_path, &metadata, metadata.epoch_cycle, false);

    let data_file = OpenOptions::new()
      .write(true)
      .append(true)
      .create(true)
      .open(cycle_file_path)
      .await?;

    crate::indexer::append_file_to_index(metadata.hot_storage_path.clone(), metadata.epoch_cycle).await?;

    live_senders.fetch_add(1, Ordering::SeqCst);
    Ok(Sender {
      current_file: data_file,
      metadata,
      live_senders,
    })
  }

  pub async fn send<T: ?Sized>(&mut self, data: &T) -> Result<(), SenderError>
  where
    T: Serialize,
  {
    // We write the length using a 64-bit unsigned integer
    let buf_len = bincode::serialized_size(data)? as usize;

    let data_buf = match self.metadata.encryption {
      Encryption::PlainText => {
        let length = buf_len + std::mem::size_of::<u64>();

        // Unsafe used to avoid zeroing the buffer. Since the queue is supposed to be used in
        // an environment where there's a lot of small events, not zeroing the buffer might increase
        // performance a little. Ideally, we should avoid tons of allocations and use an arena, or
        // reuse a buffer or smt.
        let mut data_buf = unsafe {
          Vec::from_raw_parts(
            std::alloc::alloc(Layout::from_size_align(length, std::mem::align_of::<u8>())?),
            length,
            length,
          )
        };

        match self.metadata.wire_format {
          #[cfg(feature = "format-bincode")]
          WireFormat::Bincode => {
            data_buf[0..std::mem::size_of::<u64>()].copy_from_slice(&buf_len.to_be_bytes());
            bincode::serialize_into(&mut data_buf[std::mem::size_of::<u64>()..], data)?;
            data_buf
          }
          #[cfg(not(feature = "format-bincode"))]
          WireFormat::Bincode => return Err(SenderError::UnsupportedFormat("Bincode".into())),
          #[cfg(feature = "format-json")]
          WireFormat::Json => {
            data_buf[0..std::mem::size_of::<u64>()].copy_from_slice(&buf_len.to_be_bytes());
            serde_json::to_writer(&mut data_buf[std::mem::size_of::<u64>()..], data)?;
            data_buf
          }
          #[cfg(not(feature = "format-json"))]
          WireFormat::Json => return Err(SenderError::UnsupportedFormat("JSON".into())),
          #[cfg(not(any(feature = "format-bincode", feature = "format-json")))]
          _ => std::compile_error!("At least one serialization format should be selected."),
        }
      }
    };

    self.write_bytes(&data_buf).await
  }

  async fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), SenderError> {
    self.update_data_file().await?;
    self.current_file.write_all(bytes).await?;
    Ok(())
  }

  async fn update_data_file(&mut self) -> Result<(), SenderError> {
    if let Some(new_epoch_cycle) = self.metadata.roll_cycle.next_epoch_cycle(self.metadata.epoch_cycle) {
      self.metadata.epoch_cycle = new_epoch_cycle;
      self.current_file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(Queue::get_file_path(
          &self.metadata.hot_storage_path,
          &self.metadata,
          self.metadata.epoch_cycle,
          false,
        ))
        .await?;

      tokio::spawn(crate::indexer::append_file_to_index(
        self.metadata.hot_storage_path.clone(),
        self.metadata.epoch_cycle,
      ));
    }

    Ok(())
  }
}

impl Drop for Sender {
  fn drop(&mut self) {
    self.live_senders.fetch_sub(1, Ordering::SeqCst);
  }
}
