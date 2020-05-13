use std::io::SeekFrom;
use std::path::PathBuf;

use tokio::fs::OpenOptions;
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::fs::FdLock;
use crate::{W_FALSE, W_HOT_STORAGE};

pub(crate) async fn append_file_to_index(hot_storage_path: PathBuf, epoch_cycle: u64) -> io::Result<()> {
  let index_path = hot_storage_path.join("index.aui");
  let mut index_file = OpenOptions::new()
    .read(true)
    .append(true)
    .open(&index_path)
    .await?
    .lock_exclusive()
    .await
    .map_err(|e| -> io::Error { e.into() })?;

  let row_len = -((std::mem::size_of::<u64>() + std::mem::size_of::<u8>() * 2) as i64);

  if let Ok(_) = index_file.seek(SeekFrom::End(row_len)).await {
    let last_epoch_cycle = index_file.read_u64().await?;
    if last_epoch_cycle >= epoch_cycle {
      return Ok(())
    }
  }

  index_file.write_u64(epoch_cycle).await?;
  index_file.write_u8(W_FALSE).await?;
  index_file.write_u8(W_HOT_STORAGE).await?;

  index_file.flush().await?;

  Ok(())
}
