#![feature(llvm_asm)]
#![feature(stdsimd)]

/// File backed channel implementation.
///
/// The index format:
///
///  - 0..32: Index Length
///  - 33..: (Last Written Position)
use std::convert::{TryFrom, TryInto};
use std::error::Error;
use std::ffi::OsStr;
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use chrono::{Datelike, Local, NaiveDateTime, Timelike};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::fs::FdLock;
use crate::receiver::Receiver;
use crate::sender::Sender;

mod atomic;
mod fs;
mod receiver;
mod sender;

#[derive(Error, Debug)]
pub enum ConversionError {
  #[error("Could not convert primitive to enum value.")]
  EnumConversionError,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum RollCycle {
  Second,
  Minute,
  Hour,
  Day,
  Month,
  Custom(u64),
}

impl RollCycle {
  pub fn file_name(&self, cycle_timestamp: i64) -> String {
    let now = NaiveDateTime::from_timestamp(cycle_timestamp, 0);
    let formatted_date_time = match self {
      RollCycle::Second | RollCycle::Custom(_) => now.format("%Y%m%d-%H%M%S"),
      RollCycle::Minute => now.format("%Y%m%d-%H%M"),
      RollCycle::Hour => now.format("%Y%m%d-%H"),
      RollCycle::Day => now.format("%Y%m%d"),
      RollCycle::Month => now.format("%Y%m"),
    };

    format!("{}.aqd", formatted_date_time)
  }

  pub fn current_cycle(&self) -> i64 {
    let now = Local::now();
    match self {
      RollCycle::Second => now.timestamp(),
      RollCycle::Minute => now.with_second(0).unwrap().timestamp(),
      RollCycle::Hour => now.with_minute(0).unwrap().with_second(0).unwrap().timestamp(),
      RollCycle::Day => now.date().and_hms(0, 0, 0).timestamp(),
      RollCycle::Month => now.date().with_day(1).unwrap().and_hms(0, 0, 0).timestamp(),
      RollCycle::Custom(cycle) => now.timestamp() / *cycle as i64 * *cycle as i64,
    }
  }

  pub fn from_secs(secs: u64) -> Self {
    match secs {
      1 => RollCycle::Second,
      60 => RollCycle::Minute,
      3_600 => RollCycle::Hour,
      86_400 => RollCycle::Day,
      2_592_000 => RollCycle::Month,
      secs => RollCycle::Custom(secs),
    }
  }

  #[inline(always)]
  pub fn as_seconds(&self) -> u64 {
    match self {
      RollCycle::Second => 1,
      RollCycle::Minute => 60,
      RollCycle::Hour => 3_600,
      RollCycle::Day => 86_400,
      RollCycle::Month => 2_592_000,
      RollCycle::Custom(secs) => *secs,
    }
  }
}

#[repr(u16)]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum CompressionFormat {
  Uncompressed = 0,
}

impl TryFrom<u16> for CompressionFormat {
  type Error = ConversionError;

  fn try_from(value: u16) -> Result<Self, Self::Error> {
    match value {
      0 => Ok(CompressionFormat::Uncompressed),
      _ => Err(ConversionError::EnumConversionError),
    }
  }
}

#[repr(u16)]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum WireFormat {
  Raw = 0,
}

impl TryFrom<u16> for WireFormat {
  type Error = ConversionError;

  fn try_from(value: u16) -> Result<Self, Self::Error> {
    match value {
      0 => Ok(WireFormat::Raw),
      _ => Err(ConversionError::EnumConversionError),
    }
  }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ChannelMetadata {
  roll_cycle: RollCycle,
  compression_format: CompressionFormat,
  wire_format: WireFormat,

  index_size: i64,
  cool_storage_cycle: u64,
  cold_storage_cycle: u64,
}

pub struct ChannelBuilder {
  hot_storage_path: PathBuf,

  cool_storage_path: Option<PathBuf>,
  cool_storage_cycle: u64,

  cold_storage_path: Option<PathBuf>,
  cold_storage_cycle: u64,

  index_size: i64,
  roll_cycle: RollCycle,
  compression_format: CompressionFormat,
  wire_format: WireFormat,
}

pub struct Channel {
  hot_storage_path: PathBuf,
  cool_storage_path: Option<PathBuf>,
  cold_storage_path: Option<PathBuf>,

  metadata: ChannelMetadata,
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

const METADATA_FILE_NAME: &str = "metadata.auq";

impl ChannelBuilder {
  pub fn new(hot_storage_path: impl AsRef<OsStr>) -> Self {
    let hot_storage_path = Path::new(hot_storage_path.as_ref()).to_path_buf();

    ChannelBuilder {
      hot_storage_path,
      cool_storage_path: None,
      cool_storage_cycle: 0,
      cold_storage_path: None,
      cold_storage_cycle: 0,
      index_size: (100 * std::mem::size_of::<i64>()) as i64,
      roll_cycle: RollCycle::Day,
      compression_format: CompressionFormat::Uncompressed,
      wire_format: WireFormat::Raw,
    }
  }

  pub fn cool_storage(
    &mut self,
    cool_storage_path: impl AsRef<OsStr>,
    cool_storage_cycle: u64,
  ) -> Result<&mut Self, ChannelConfigError> {
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
    &mut self,
    cold_storage_path: impl AsRef<OsStr>,
    cold_storage_cycle: u64,
  ) -> Result<&mut Self, ChannelConfigError> {
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

  pub fn roll_cycle(&mut self, roll_cycle: RollCycle) -> &mut Self {
    self.roll_cycle = roll_cycle;
    self
  }

  pub fn wire_format(&mut self, wire_format: WireFormat) -> &mut Self {
    self.wire_format = wire_format;
    self
  }

  pub fn compression(&mut self, compression: CompressionFormat) -> &mut Self {
    self.compression_format = compression;
    self
  }

  pub async fn build(self) -> Result<Channel, ChannelConfigError> {
    let metadata_file = OpenOptions::new()
      .create(true)
      .write(true)
      .read(true)
      .open(self.hot_storage_path.join(METADATA_FILE_NAME))?;

    let mut metadata_file = metadata_file.lock_exclusive().await?;

    if metadata_file.metadata()?.len() == 0 {
      metadata_file.set_len(self.index_size as u64 + 1)?;
    }

    let is_new_metadata = metadata_file.read_u8()? == 0;
    let metadata = if is_new_metadata {
      metadata_file.seek(SeekFrom::Start(0))?;

      // Sets this metadata file as written
      metadata_file.write_u8(0xFF)?;

      let metadata = ChannelMetadata {
        roll_cycle: self.roll_cycle,
        compression_format: self.compression_format,
        wire_format: self.wire_format,
        index_size: self.index_size,
        cool_storage_cycle: self.cool_storage_cycle,
        cold_storage_cycle: self.cold_storage_cycle,
      };

      metadata_file.write_u16::<LittleEndian>(metadata.compression_format.clone() as u16)?;
      metadata_file.write_u16::<LittleEndian>(metadata.wire_format.clone() as u16)?;

      metadata_file.write_u64::<LittleEndian>(metadata.roll_cycle.as_seconds())?;
      metadata_file.write_i64::<LittleEndian>(metadata.index_size)?;
      metadata_file.write_u64::<LittleEndian>(metadata.cool_storage_cycle)?;
      metadata_file.write_u64::<LittleEndian>(metadata.cold_storage_cycle)?;

      metadata
    } else {
      let compression_format: CompressionFormat = metadata_file.read_u16::<LittleEndian>()?.try_into()?;
      let wire_format: WireFormat = metadata_file.read_u16::<LittleEndian>()?.try_into()?;
      let roll_cycle: RollCycle = RollCycle::from_secs(metadata_file.read_u64::<LittleEndian>()?);

      let index_size = metadata_file.read_i64::<LittleEndian>()?;
      let cool_storage_cycle = metadata_file.read_u64::<LittleEndian>()?;
      let cold_storage_cycle = metadata_file.read_u64::<LittleEndian>()?;

      ChannelMetadata {
        roll_cycle,
        compression_format,
        wire_format,
        index_size,
        cool_storage_cycle,
        cold_storage_cycle,
      }
    };

    Ok(Channel {
      hot_storage_path: self.hot_storage_path,
      cool_storage_path: self.cool_storage_path,
      cold_storage_path: self.cold_storage_path,
      metadata,
    })
  }
}

impl Channel {
  pub async fn acquire_sender(&self) -> Result<Sender, Box<dyn Error>> {
    Ok(Sender::new(&self.hot_storage_path, self.metadata.clone()).await?)
  }

  pub fn subscribe(&self) -> Result<Receiver, Box<dyn Error>> {
    let current_file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open("/tmp/channel-sample/test-send")?;

    Ok(Receiver::new(&current_file, self.metadata.clone())?)
  }
}
