use std::convert::TryInto;
use std::ffi::OsStr;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::channel::Channel;
use crate::fs::FdLock;
use crate::metadata::{ChannelMetadata, CompressionFormat, ConversionError, RollCycle, WireFormat};

pub struct ChannelBuilder {
  hot_storage_path: PathBuf,

  cool_storage_path: Option<PathBuf>,
  cool_storage_cycle: u64,

  cold_storage_path: Option<PathBuf>,
  cold_storage_cycle: u64,

  index_block_size: u64,
  data_block_size: u64,

  roll_cycle: RollCycle,
  compression_format: CompressionFormat,
  wire_format: WireFormat,
}

#[derive(Error, Debug)]
pub enum ChannelConfigError {
  #[error("The path {0} does not exist or is a not a directory.")]
  InvalidSecondaryStoragePath(String),
  #[error("Invalid secondary storage cycle. Must be greater than {0}.")]
  InvalidSecondaryStorageCycle(u64),
  #[error("I/O Error")]
  IoError(#[from] std::io::Error),
  #[error("Could not convert primitive into enum value.")]
  ConversionError(#[from] ConversionError),
  #[error("Could not lock metadata file.")]
  MetadataLockError(#[from] crate::fs::LockError),
}

impl ChannelBuilder {
  pub fn new(hot_storage_path: impl AsRef<OsStr>) -> Self {
    let hot_storage_path = Path::new(hot_storage_path.as_ref()).to_path_buf();

    #[cfg(feature = "format-json")]
    let wire_format = WireFormat::Json;
    #[cfg(feature = "format-bincode")]
    let wire_format = WireFormat::Bincode;

    ChannelBuilder {
      hot_storage_path,
      cool_storage_path: None,
      cool_storage_cycle: 0,
      cold_storage_path: None,
      cold_storage_cycle: 0,
      index_block_size: 1024,
      data_block_size: 1_000_000,
      roll_cycle: RollCycle::Day,
      compression_format: CompressionFormat::Uncompressed,
      wire_format,
    }
  }

  pub fn cool_storage(
    mut self,
    cool_storage_path: impl AsRef<OsStr>,
    cool_storage_cycle: u64,
  ) -> Result<Self, ChannelConfigError> {
    let path = Path::new(cool_storage_path.as_ref());
    if !(path.exists() && path.is_dir()) {
      return Err(ChannelConfigError::InvalidSecondaryStoragePath(
        cool_storage_path.as_ref().to_string_lossy().to_string(),
      ));
    }

    self.cool_storage_path = Some(path.to_path_buf());
    self.cool_storage_cycle = cool_storage_cycle;
    Ok(self)
  }

  pub fn cold_storage(
    mut self,
    cold_storage_path: impl AsRef<OsStr>,
    cold_storage_cycle: u64,
  ) -> Result<Self, ChannelConfigError> {
    if cold_storage_cycle <= self.cool_storage_cycle {
      return Err(ChannelConfigError::InvalidSecondaryStorageCycle(
        self.cool_storage_cycle + 1,
      ));
    }

    let path = Path::new(cold_storage_path.as_ref());
    if !(path.exists() && path.is_dir()) {
      return Err(ChannelConfigError::InvalidSecondaryStoragePath(
        cold_storage_path.as_ref().to_string_lossy().to_string(),
      ));
    }

    self.cold_storage_path = Some(path.to_path_buf());
    self.cold_storage_cycle = cold_storage_cycle;
    Ok(self)
  }

  pub fn index_block_size(mut self, index_block_size: u64) -> Self {
    self.index_block_size = index_block_size;
    self
  }

  pub fn data_block_size(mut self, data_block_size: u64) -> Self {
    self.data_block_size = data_block_size;
    self
  }

  pub fn roll_cycle(mut self, roll_cycle: RollCycle) -> Self {
    self.roll_cycle = roll_cycle;
    self
  }

  pub fn wire_format(mut self, wire_format: WireFormat) -> Self {
    self.wire_format = wire_format;
    self
  }

  pub fn compression(mut self, compression: CompressionFormat) -> Self {
    self.compression_format = compression;
    self
  }

  pub async fn build<T: Serialize + DeserializeOwned + ?Sized>(self) -> Result<Channel<T>, ChannelConfigError> {
    let metadata_file = OpenOptions::new()
      .create(true)
      .write(true)
      .read(true)
      .open(self.hot_storage_path.join(crate::metadata::METADATA_FILE_NAME))
      .await?;

    let mut metadata_file = metadata_file.lock_exclusive().await?;

    let is_new_metadata = metadata_file.metadata().await?.len() == 0 || metadata_file.read_u8().await? == 0;
    let metadata = if is_new_metadata {
      metadata_file.seek(SeekFrom::Start(0)).await?;

      // Sets this metadata file as written
      metadata_file.write_u8(0xFF).await?;

      let metadata = ChannelMetadata {
        roll_cycle: self.roll_cycle,
        compression_format: self.compression_format,
        wire_format: self.wire_format,
        cool_storage_cycle: self.cool_storage_cycle,
        cold_storage_cycle: self.cold_storage_cycle,
        index_block_size: self.index_block_size,
        data_block_size: self.data_block_size,
      };

      metadata_file
        .write_u16(metadata.compression_format.clone() as u16)
        .await?;
      metadata_file.write_u16(metadata.wire_format.clone() as u16).await?;

      metadata_file.write_u64(metadata.roll_cycle.as_seconds()).await?;
      metadata_file.write_u64(metadata.cool_storage_cycle).await?;
      metadata_file.write_u64(metadata.cold_storage_cycle).await?;

      metadata_file.write_u64(metadata.index_block_size).await?;
      metadata_file.write_u64(metadata.data_block_size).await?;

      metadata
    } else {
      let compression_format: CompressionFormat = metadata_file.read_u16().await?.try_into()?;
      let wire_format: WireFormat = metadata_file.read_u16().await?.try_into()?;
      let roll_cycle: RollCycle = RollCycle::from_secs(metadata_file.read_u64().await?);

      let cool_storage_cycle = metadata_file.read_u64().await?;
      let cold_storage_cycle = metadata_file.read_u64().await?;

      let index_block_size = metadata_file.read_u64().await?;
      let data_block_size = metadata_file.read_u64().await?;

      ChannelMetadata {
        roll_cycle,
        compression_format,
        wire_format,
        cool_storage_cycle,
        cold_storage_cycle,
        index_block_size,
        data_block_size,
      }
    };

    Ok(Channel {
      hot_storage_path: self.hot_storage_path,
      cool_storage_path: self.cool_storage_path,
      cold_storage_path: self.cold_storage_path,
      metadata,
      element_type: Default::default(),
    })
  }
}
