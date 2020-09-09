use std::ffi::OsStr;
use std::fs::OpenOptions;
use std::path::PathBuf;

use thiserror::Error;

use crate::fs::{FdLock, SyncFdLock};
use crate::metadata::ChannelMetadata;
use crate::{CompressionFormat, DATA_FILE_EXTENSION};

#[derive(Error, Debug)]
pub enum CompressorError {
  #[error("Compression operation error")]
  CompressionError,
  #[error("I/O error")]
  IoError(#[from] std::io::Error),
  #[error("File locking error")]
  LockError(#[from] crate::fs::LockError),
  #[error("Compression operation error")]
  JoinError(#[from] tokio::task::JoinError),
}

#[tracing::instrument(name = "Compression", skip(hot_storage_path, metadata))]
pub async fn compress(hot_storage_path: PathBuf, metadata: ChannelMetadata) -> Result<(), CompressorError> {
  if let CompressionFormat::Uncompressed = metadata.compression_format {
    return Ok(());
  }

  // Look up for uncompressed files
  let mut dir_contents = tokio::fs::read_dir(&hot_storage_path).await?;
  while let Some(entry) = dir_contents.next_entry().await? {

    // Skip file corresponding to the current cycle
    if let Some(filename) = entry.path().file_name().and_then(|f| f.to_str()) {
      if filename == metadata.roll_cycle.data_file_name(metadata.roll_cycle.current_cycle()) {
        continue;
      }
    }

    match entry.path().extension() {
      Some(ext) if ext == OsStr::new(DATA_FILE_EXTENSION) => {
        let mut source = OpenOptions::new().read(true).open(entry.path())?;

        let mut compressed_file_path = entry.path().clone();
        compressed_file_path.set_extension(metadata.compression_format.extension());

        tracing::debug!("Compressing {:?} using {:?}", entry.path(), metadata.compression_format);

        let compressed_file = OpenOptions::new()
          .read(true)
          .write(true)
          .create(true)
          .open(compressed_file_path)?;

        // Try to lock the file to be compressed. If it can't be locked, another process is either
        // writing to it, or already trying to compress it.
        if let Ok(locked_compressed_file) = compressed_file.try_lock_exclusive() {
          // Read and compress target

          let (source, _) = match metadata.compression_format {
            #[cfg(feature = "compression-lz4")]
            CompressionFormat::LZ4 => {
              tokio::task::spawn_blocking(move || {
                let mut encoder = lz4::EncoderBuilder::new()
                  .auto_flush(true)
                  .block_mode(lz4::BlockMode::Independent)
                  .level(8)
                  .build(locked_compressed_file)?;

                tracing::debug!("Encoding source data.");
                std::io::copy(&mut source, &mut encoder)?;

                let (lock, result) = encoder.finish();
                result.map(|_| (source, lock))
              })
              .await??
            }

            #[cfg(feature = "compression-snappy")]
            CompressionFormat::Snappy => {
              tokio::task::spawn_blocking(move || {
                let mut encoder = snap::write::FrameEncoder::new(locked_compressed_file);

                tracing::debug!("Encoding source data.");
                std::io::copy(&mut source, &mut encoder)?;

                encoder
                  .into_inner()
                  .map(|l| (source, l))
                  .map_err(|_| CompressorError::CompressionError)
              })
              .await??
            }

            _ => (source, locked_compressed_file),
          };

          // Compression completed. Delete original file.
          if let Ok(_) = source.lock_exclusive().await {
            tracing::debug!("Removing source file {:?}", entry.path());
            tokio::fs::remove_file(entry.path()).await?;
          }
        }
      }
      _ => (),
    }
  }

  Ok(())
}
