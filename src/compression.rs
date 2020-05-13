use std::convert::Into;
use std::io::Write;
use std::path::{Path, PathBuf};

use futures::io::SeekFrom;
use serde::{Deserialize, Serialize};
use tokio::fs::OpenOptions;
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::fs::{FdLock, SyncFdLock};
use crate::{Queue, QueueMetadata, W_FALSE, W_HOT_STORAGE, W_TRUE};

pub(crate) struct Compressor {
  metadata: QueueMetadata,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[repr(u8)]
pub enum CompressionFormat {
  Uncompressed = 0,
  LZ4 = 1,
  Snappy = 2,
}

impl Compressor {
  pub(crate) fn new(metadata: QueueMetadata) -> Self {
    Compressor { metadata }
  }

  /// Compresses old files in the hot storage.
  pub(crate) async fn run(&self) -> io::Result<()> {
    // Try locking on the index
    let index_path = self.metadata.hot_storage_path.join("index.aui");
    let mut index_file = if let Ok(lock) = OpenOptions::new()
      .read(true)
      .write(true)
      .open(&index_path)
      .await?
      .try_lock_exclusive()
    {
      lock
    } else {
      // We don't want to run the compressor when the index file is locked.
      return Ok(());
    };

    // Compress all old files
    let index_row_len = (std::mem::size_of::<u64>() + std::mem::size_of::<u8>() * 2) as u64;

    let mut index = 0;
    loop {
      if let Ok(epoch_cycle) = index_file.read_u64().await {
        let is_uncompressed = index_file.read_u8().await? == W_FALSE;
        let is_on_hot_storage = index_file.read_u8().await? == W_HOT_STORAGE;

        if epoch_cycle < self.metadata.roll_cycle.epoch_cycle() && is_on_hot_storage && is_uncompressed {
          let file_path = Queue::get_file_path(&self.metadata.hot_storage_path, &self.metadata, epoch_cycle, false);
          if !file_path.exists() || !file_path.is_file() {
            continue;
          }

          self.compress(file_path).await?;
          index_file
            .seek(SeekFrom::Start(
              index * index_row_len + (std::mem::size_of::<u64>() as u64),
            ))
            .await?;
          index_file.write_u8(W_TRUE).await?;
          index_file.seek(SeekFrom::Start(index * (index_row_len + 1))).await?;
        }
      } else {
        break;
      }

      index += 1;
    }

    Ok(())
  }

  async fn compress(&self, file_path: PathBuf) -> io::Result<()> {
    let mut target_file_name = file_path.file_name().unwrap().to_os_string();
    target_file_name.reserve(self.metadata.compression.file_extension().len());
    target_file_name.push(self.metadata.compression.file_extension());

    let target_file_path = file_path
      .parent()
      .map(|p| p.join(Path::new(&target_file_name)))
      .ok_or(io::Error::new(io::ErrorKind::Other, "could not create compressed file"))?;

    match self.metadata.compression {
      #[cfg(feature = "compression-snappy")]
      CompressionFormat::Snappy => {
        let compress_task = move || -> io::Result<PathBuf> {
          let mut source_file = std::fs::OpenOptions::new().read(true).open(&file_path)?;
          let target_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(target_file_path)?;

          let target_file = target_file
            .try_lock_exclusive()
            .map_err(|e| -> io::Error { e.into() })?;

          let mut encoder = snap::write::FrameEncoder::new(target_file);

          std::io::copy(&mut source_file, &mut encoder)?;

          encoder.flush()?;

          Ok(file_path)
        };

        if let Ok(Ok(file_path)) = tokio::task::spawn_blocking(compress_task).await {
          // Remove old uncompressed file. Wait for all readers to drop the shared locks.
          let uncompressed_file = OpenOptions::new().read(true).open(&file_path).await?;
          let _ = uncompressed_file
            .lock_exclusive()
            .await
            .map_err(|e| -> io::Error { e.into() })?;

          tokio::fs::remove_file(&file_path).await?;
        }
      }

      #[cfg(feature = "compression-lz4")]
      CompressionFormat::LZ4 => {
        let compress_task = move || -> io::Result<PathBuf> {
          let mut source_file = std::fs::OpenOptions::new().read(true).open(&file_path)?;
          let target_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(target_file_path)?;

          let target_file = target_file
            .try_lock_exclusive()
            .map_err(|e| -> io::Error { e.into() })?;

          let mut encoder = lz4::EncoderBuilder::new()
            .block_mode(lz4::BlockMode::Independent)
            .level(15)
            .build(target_file)?;

          std::io::copy(&mut source_file, &mut encoder)?;

          let (_, result) = encoder.finish();
          result?;

          Ok(file_path)
        };

        if let Ok(Ok(file_path)) = tokio::task::spawn_blocking(compress_task).await {
          // Remove old uncompressed file. Wait for all readers to drop the shared locks.
          let uncompressed_file = OpenOptions::new().read(true).open(&file_path).await?;
          let _ = uncompressed_file
            .lock_exclusive()
            .await
            .map_err(|e| -> io::Error { e.into() })?;

          tokio::fs::remove_file(&file_path).await?;
        }
      }
      _ => (),
    }

    Ok(())
  }
}

impl CompressionFormat {
  pub fn file_extension(&self) -> String {
    match self {
      CompressionFormat::Uncompressed => "",
      CompressionFormat::LZ4 => ".lz4",
      CompressionFormat::Snappy => ".snappy",
    }
      .into()
  }
}


#[cfg(test)]
mod test {
  use tokio::fs::File;
  use tokio::io;
  use tokio::io::AsyncWriteExt;

  use crate::crypto::Encryption;
  use crate::{CompressionFormat, Queue, QueueMetadata, RollCycle, WireFormat};

  #[tokio::test]
  #[cfg(feature = "compression-snappy")]
  async fn test_snappy_compressor() -> io::Result<()> {
    let hot_storage_dir = tempfile::tempdir()?;

    let metadata = QueueMetadata {
      name: None,
      epoch_cycle: 0,
      roll_cycle: RollCycle::Second,
      wire_format: WireFormat::Bincode,
      compression: CompressionFormat::Snappy,
      encryption: Encryption::PlainText,
      file_pattern: RollCycle::Second.pattern().into(),
      hot_storage_path: hot_storage_dir.path().to_path_buf(),
      cool_storage_path: None,
      cold_storage_path: None,
    };

    let uncompressed_file_name = Queue::get_file_path(hot_storage_dir.path(), &metadata, 0, false)
      .file_name()
      .unwrap()
      .to_owned();

    let compressed_file_path = Queue::get_file_path(hot_storage_dir.path(), &metadata, 0, true);

    let mut uncompressed = File::create(hot_storage_dir.path().join(&uncompressed_file_name)).await?;
    uncompressed.write_all(b"Hello world!").await?;

    // Index file
    File::create(hot_storage_dir.path().join("index.aui")).await?;
    crate::indexer::append_file_to_index(hot_storage_dir.path().to_path_buf(), 0).await.unwrap();

    let compressor = super::Compressor::new(metadata);

    compressor.run().await?;

    let mut contains_compressed = false;
    let mut read_dir = tokio::fs::read_dir(hot_storage_dir.path()).await?;
    while let Ok(Some(entry)) = read_dir.next_entry().await {
      assert_ne!(entry.path(), hot_storage_dir.path().join(&uncompressed_file_name));
      if entry.path() == compressed_file_path {
        contains_compressed = true;
      }
    }
    assert!(contains_compressed);

    Ok(())
  }
}
