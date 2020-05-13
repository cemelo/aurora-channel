use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use chrono::{DateTime, NaiveDateTime, Utc, Local};
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::compression::{CompressionFormat, Compressor};
use crate::crypto::Encryption;
use crate::receiver::{Receiver, ReceiverError};
use crate::sender::{Sender, SenderError};

#[allow(dead_code)]
pub struct Queue {
  pub(crate) metadata: QueueMetadata,
  pub(crate) compression_cycle: u64,
  pub(crate) storage_cycle: u64,
  live_senders: Arc<AtomicU64>,
}

#[derive(Error, Debug)]
pub enum QueueError {
  #[error("underlying filesystem error")]
  IoError(#[from] io::Error),
  #[error("serialization error")]
  SerializationError(#[from] bincode::Error),
  #[error("invalid queue param: `{0}`")]
  InvalidQueueConfiguration(String),
  #[error("cool storage not found")]
  CoolStorageNotFound,
  #[error("cold storage not found")]
  ColdStorageNotFound,
  #[error("sender error")]
  SenderError(#[from] SenderError),
  #[error("receiver error")]
  ReceiverError(#[from] ReceiverError),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum RollCycle {
  Second,
  Minute,
  Hour,
  Day,
  Custom(u64),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[repr(u8)]
pub enum WireFormat {
  Bincode = 0,
  Json = 1,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct QueueMetadata {
  pub name: Option<String>,
  pub epoch_cycle: u64,
  pub roll_cycle: RollCycle,
  pub wire_format: WireFormat,
  pub compression: CompressionFormat,
  pub encryption: Encryption,
  pub file_pattern: String,

  /// Represents the first level storage option.
  pub hot_storage_path: PathBuf,

  /// Represents the second level storage option.
  /// The first parameter is the epoch_cycle from which a file is moved into the cool storage.
  pub cool_storage_path: Option<(u64, PathBuf)>,

  /// Represents the third level storage option.
  /// The first parameter is the epoch_cycle from which a file is moved into the cold storage.
  pub cold_storage_path: Option<(u64, PathBuf)>,
}

impl Queue {
  pub(crate) fn new(metadata: QueueMetadata, compression_cycle: u64, storage_cycle: u64) -> Self {
    let live_senders = Arc::new(AtomicU64::new(1));
    if compression_cycle > 0 {
      let compressor = Compressor::new(metadata.clone());

      let live_senders = live_senders.clone();
      tokio::spawn(async move {
        while live_senders.load(Ordering::SeqCst) > 0 {
          tokio::time::delay_for(Duration::from_secs(compression_cycle)).await;
          if let Err(e) = compressor.run().await {
            warn!("Error while running the compressor: error = {:?}", e);
          }
        }

        debug!("Queue dropped. Compressor task stopped.");
      });
    }

    Queue {
      metadata,
      compression_cycle,
      storage_cycle,
      live_senders,
    }
  }

  pub async fn acquire_sender(&self) -> Result<Sender, SenderError> {
    Sender::new(self.metadata.clone(), self.live_senders.clone()).await
  }

  pub async fn subscribe(&self) -> Result<Receiver, ReceiverError> {
    Receiver::new(self.metadata.clone()).await
  }

  pub(crate) fn get_file_path(path: &Path, metadata: &QueueMetadata, epoch_cycle: u64, compressed: bool) -> PathBuf {
    let timestamp = metadata.roll_cycle.as_secs() * epoch_cycle;
    let epoch_time = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp as i64, 0), Utc);

    let topic_path = Path::new(path);
    if !compressed {
      topic_path.join(format!("{}.auq", epoch_time.format(&metadata.file_pattern)))
    } else {
      topic_path.join(format!(
        "{}.auq{}",
        epoch_time.format(&metadata.file_pattern),
        metadata.compression.file_extension()
      ))
    }
  }
}

impl Drop for Queue {
  fn drop(&mut self) {
    self.live_senders.fetch_sub(1, Ordering::SeqCst);
  }
}

impl RollCycle {
  pub(crate) fn as_secs(&self) -> u64 {
    match self {
      RollCycle::Second => 1,
      RollCycle::Minute => 60,
      RollCycle::Hour => 3600,
      RollCycle::Day => 86400,
      RollCycle::Custom(seconds) => *seconds,
    }
  }

  pub(crate) fn pattern(&self) -> &'static str {
    match self {
      RollCycle::Second => "%Y%m%d-%H%M%S",
      RollCycle::Minute => "%Y%m%d-%H%M",
      RollCycle::Hour => "%Y%m%d-%H",
      RollCycle::Day => "%Y%m%d",
      RollCycle::Custom(_) => "%Y%m%d-%H%M%S",
    }
  }

  pub(crate) fn epoch_cycle(&self) -> u64 {
    let timestamp = Local::now().timestamp() as u64;
    timestamp / self.as_secs()
  }

  pub(crate) fn next_epoch_cycle(&self, current_epoch_cycle: u64) -> Option<u64> {
    // Should return a value only if the current timestamp is past the current_epoch_cycle.

    let current_timestamp = std::time::SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
    let cycle_threshold = self.as_secs() * (current_epoch_cycle + 1);

    if current_timestamp >= cycle_threshold {
      Some(self.epoch_cycle())
    } else {
      None
    }
  }
}

impl Default for QueueMetadata {
  #[allow(dead_code)]
  fn default() -> Self {
    #[cfg(feature = "format-json")]
    let _wire_format = WireFormat::Json;
    #[cfg(feature = "format-bincode")]
    let _wire_format = WireFormat::Bincode;
    #[cfg(not(any(feature = "format-bincode", feature = "format-json")))]
    std::compile_error!("At least one wire format must be selected.");

    QueueMetadata {
      name: None,
      epoch_cycle: 0,
      roll_cycle: RollCycle::Day,
      wire_format: _wire_format,
      compression: CompressionFormat::Uncompressed,
      encryption: Encryption::PlainText,
      file_pattern: RollCycle::Day.pattern().into(),
      hot_storage_path: Path::new("").into(),
      cool_storage_path: None,
      cold_storage_path: None,
    }
  }
}

impl From<u64> for RollCycle {
  fn from(v: u64) -> Self {
    match v {
      1 => RollCycle::Second,
      60 => RollCycle::Minute,
      3600 => RollCycle::Hour,
      86400 => RollCycle::Day,
      _ => RollCycle::Custom(v),
    }
  }
}
