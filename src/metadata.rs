use std::convert::TryFrom;

use chrono::{Datelike, Local, NaiveDateTime, Timelike};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::DATA_FILE_EXTENSION;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChannelMetadata {
  pub roll_cycle: RollCycle,
  pub compression_format: CompressionFormat,
  pub wire_format: WireFormat,

  pub cool_storage_cycle: u64,
  pub cold_storage_cycle: u64,

  pub index_block_size: u64,
  pub data_block_size: u64,
}

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
  pub fn data_file_name(&self, cycle_timestamp: i64) -> String {
    let now = NaiveDateTime::from_timestamp(cycle_timestamp, 0);
    let formatted_date_time = match self {
      RollCycle::Second | RollCycle::Custom(_) => now.format("%Y%m%d-%H%M%S"),
      RollCycle::Minute => now.format("%Y%m%d-%H%M"),
      RollCycle::Hour => now.format("%Y%m%d-%H"),
      RollCycle::Day => now.format("%Y%m%d"),
      RollCycle::Month => now.format("%Y%m"),
    };

    format!("{}.{}", formatted_date_time, DATA_FILE_EXTENSION)
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
  #[cfg(feature = "compression-lz4")]
  LZ4 = 1,
  #[cfg(feature = "compression-snappy")]
  Snappy = 2,
}

impl CompressionFormat {
  pub fn extension(&self) -> String {
    match self {
      CompressionFormat::Uncompressed => DATA_FILE_EXTENSION.into(),
      #[cfg(feature = "compression-lz4")]
      CompressionFormat::LZ4 => format!("{}.lz4", DATA_FILE_EXTENSION),
      #[cfg(feature = "compression-snappy")]
      CompressionFormat::Snappy => format!("{}.sz", DATA_FILE_EXTENSION),
    }
  }
}

impl TryFrom<u16> for CompressionFormat {
  type Error = ConversionError;

  fn try_from(value: u16) -> Result<Self, Self::Error> {
    match value {
      0 => Ok(CompressionFormat::Uncompressed),
      #[cfg(feature = "compression-lz4")]
      1 => Ok(CompressionFormat::LZ4),
      #[cfg(feature = "compression-snappy")]
      2 => Ok(CompressionFormat::Snappy),
      _ => Err(ConversionError::EnumConversionError),
    }
  }
}

#[repr(u16)]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum WireFormat {
  #[cfg(feature = "format-bincode")]
  Bincode = 0,
  #[cfg(feature = "format-json")]
  Json = 1,
}

impl TryFrom<u16> for WireFormat {
  type Error = ConversionError;

  fn try_from(value: u16) -> Result<Self, Self::Error> {
    match value {
      #[cfg(feature = "format-bincode")]
      0 => Ok(WireFormat::Bincode),
      #[cfg(feature = "format-json")]
      1 => Ok(WireFormat::Json),
      _ => Err(ConversionError::EnumConversionError),
    }
  }
}
