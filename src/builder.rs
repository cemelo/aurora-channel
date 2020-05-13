use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::crypto::Encryption;
use crate::fs::FdLock;
use crate::{CompressionFormat, Queue, QueueError, QueueMetadata, RollCycle, WireFormat};

#[derive(Clone)]
pub struct QueueBuilder {
  metadata: QueueMetadata,
  compression_cycle: u64,
  storage_cycle: u64,
}

#[allow(dead_code)]
impl QueueBuilder {
  pub fn new(hot_storage_path: PathBuf) -> Self {
    let mut metadata = QueueMetadata::default();
    metadata.hot_storage_path = hot_storage_path;

    QueueBuilder {
      metadata,
      compression_cycle: 0,
      storage_cycle: 0,
    }
  }

  pub fn with_wire_format(mut self, wire_format: WireFormat) -> Self {
    match wire_format {
      WireFormat::Bincode if !cfg!(feature = "format-bincode") => {
        panic!("Bincode codec not available")
      }
      WireFormat::Json if !cfg!(feature = "format-json") => {
        panic!("JSON codec not available")
      }
      _ => ()
    }

    self.metadata.wire_format = wire_format;
    self
  }

  pub fn with_compression_cycle(mut self, cycle: u64) -> Self {
    self.compression_cycle = cycle;
    self
  }

  pub fn with_storage_cycle(mut self, cycle: u64) -> Self {
    self.storage_cycle = cycle;
    self
  }

  pub fn with_cool_storage(mut self, path: PathBuf, threshold: u64) -> Self {
    self.metadata.cool_storage_path = Some((threshold, path));
    self
  }

  pub fn with_cold_storage(mut self, path: PathBuf, threshold: u64) -> Result<Self, QueueError> {
    if self.metadata.cool_storage_path.is_none() {
      Err(QueueError::InvalidQueueConfiguration(
        "Cold storage can only be defined after a cool storage is set".into(),
      ))
    } else if self.metadata.cool_storage_path.as_ref().unwrap().0 >= threshold {
      Err(QueueError::InvalidQueueConfiguration(
        "Cold storage threshold must be higher than cool storage".into(),
      ))
    } else {
      self.metadata.cold_storage_path = Some((threshold, path));
      Ok(self)
    }
  }

  pub fn with_name(mut self, name: String) -> Self {
    self.metadata.name = Some(name);
    self
  }

  pub fn with_roll_cycle(mut self, roll_cycle: RollCycle) -> Self {
    self.metadata.file_pattern = roll_cycle.pattern().into();
    self.metadata.roll_cycle = roll_cycle;
    self
  }

  pub fn with_compression(mut self, compression: CompressionFormat) -> Self {
    match compression {
      CompressionFormat::Snappy if !cfg!(feature = "compression-snappy") => {
        panic!("Snappy codec not available")
      }
      CompressionFormat::LZ4 if !cfg!(feature = "compression-lz4") => {
        panic!("LZ4 codec not available")
      }
      _ => ()
    }

    self.metadata.compression = compression;
    self
  }

  pub fn with_encryption(mut self, encryption: Encryption) -> Self {
    self.metadata.encryption = encryption;
    self
  }

  pub async fn build(self) -> Result<Queue, QueueError> {
    if let Some((_, ref path)) = self.metadata.cool_storage_path {
      if !Path::new(path).exists() {
        return Err(QueueError::CoolStorageNotFound);
      }
    }

    if let Some((_, ref path)) = self.metadata.cold_storage_path {
      if !Path::new(path).exists() {
        return Err(QueueError::ColdStorageNotFound);
      }
    }

    tokio::fs::create_dir_all(&self.metadata.hot_storage_path).await?;

    // Create queue metadata lock file
    let lock_file_path = &self.metadata.hot_storage_path.join("metadata.auq.lock");
    let _ = OpenOptions::new()
      .write(true)
      .create(true)
      .open(&lock_file_path)
      .await?
      .lock_exclusive()
      .await
      .map_err(|_| QueueError::InvalidQueueConfiguration("resource unavailable".into()))?;

    // Create index file
    let _ = OpenOptions::new()
      .write(true)
      .create(true)
      .open(&self.metadata.hot_storage_path.join("index.aui"))
      .await?;

    let metadata_file_path = &self.metadata.hot_storage_path.join("metadata.auq");

    let metadata = if metadata_file_path.exists() {
      // Read metadata file
      let mut metadata_file = OpenOptions::new().read(true).open(metadata_file_path).await?;

      let metadata_size = metadata_file.read_u64().await? as usize;
      let mut metadata_buf = vec![0u8; metadata_size];
      metadata_file.read_exact(&mut metadata_buf).await?;

      bincode::deserialize::<QueueMetadata>(&metadata_buf)?
    } else {
      // Save metadata
      let mut metadata_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(metadata_file_path)
        .await?;

      let metadata_buf = bincode::serialize(&self.metadata)?;

      metadata_file.seek(SeekFrom::Start(0)).await?;
      metadata_file.write_u64(metadata_buf.len() as u64).await?;
      metadata_file.write_all(&metadata_buf).await?;

      self.metadata
    };

    let queue = Queue::new(metadata, self.compression_cycle, self.storage_cycle);

    tokio::fs::remove_file(lock_file_path).await?;

    Ok(queue)
  }
}
