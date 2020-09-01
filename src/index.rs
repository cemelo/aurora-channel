use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use memmap::{MmapMut, MmapOptions};
use thiserror::Error;
use tokio::fs::OpenOptions;

use crate::fs::FdLock;

const INDEX_FILE_NAME: &str = "index.aui";

pub struct Index {
  storage_path: PathBuf,
  block_size: u64,
  data: MmapMut,
}

#[derive(Debug, Error)]
pub enum IndexError {
  #[error("I/O error")]
  IoError(#[from] std::io::Error),
  #[error("File locking error")]
  LockError(#[from] crate::fs::LockError),
}

#[repr(C)]
pub struct IndexElement {
  pub cycle_timestamp: AtomicI64,
  pub last_cursor_position: AtomicU64,
}

impl Index {
  pub async fn new(storage_path: impl AsRef<Path>, block_size: u64) -> Result<Self, IndexError> {
    let index_file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(storage_path.as_ref().join(INDEX_FILE_NAME))
      .await?;

    let mut locked_index_file = index_file.lock_exclusive().await?;
    if locked_index_file.metadata().await?.len() == 0 {
      locked_index_file
        .set_len(std::mem::size_of::<AtomicU64>() as u64 + (block_size * std::mem::size_of::<IndexElement>() as u64))
        .await?;
    }

    let index_file = locked_index_file.unlock().into_std().await;
    let index_data = unsafe { MmapOptions::new().map_mut(&index_file)? };

    Ok(Index {
      storage_path: storage_path.as_ref().to_path_buf(),
      block_size,
      data: index_data,
    })
  }

  pub fn is_empty(&self) -> bool {
    self.len() == 0
  }

  pub fn len(&self) -> u64 {
    let index_size = unsafe { &*self.data.as_ptr().cast::<AtomicU64>() };
    index_size.load(Ordering::SeqCst)
  }

  pub fn last_mapped_index(&self) -> u64 {
    let map_size = self.data.len() - size_of::<AtomicU64>();
    (map_size / size_of::<IndexElement>()) as u64
  }

  #[inline(always)]
  pub async fn expand(&mut self) -> Result<(), IndexError> {
    // If there's more than 20% of index space left, do not expand
    if self.data.len() * 8 / 10 > (self.len() as usize * size_of::<IndexElement>()) {
      return Ok(());
    }

    let new_length = (self.data.len() - size_of::<AtomicU64>()) + self.block_size as usize;

    let index_file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(self.storage_path.join(INDEX_FILE_NAME))
      .await?;

    let mut locked_file = index_file.lock_exclusive().await?;
    if locked_file.metadata().await?.len() < new_length as u64 {
      locked_file.set_len(new_length as u64).await?;
    }

    let index_file = locked_file.unlock().into_std().await;
    let index_data = unsafe { MmapOptions::new().map_mut(&index_file)? };

    self.data = index_data;

    Ok(())
  }

  pub fn increment_size(&self, order: Ordering) {
    let index_size = unsafe { &*self.data.as_ptr().cast::<AtomicU64>() };
    index_size.fetch_add(1, order);
  }

  pub async fn remap(&mut self) -> Result<(), IndexError> {
    let index_file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(self.storage_path.join(INDEX_FILE_NAME))
      .await?;

    self.data = unsafe { MmapOptions::new().map_mut(&index_file.into_std().await)? };

    Ok(())
  }

  pub fn index_deref(&self, index: usize) -> &IndexElement {
    let available_slots = (self.data.len() - size_of::<AtomicI64>()) / size_of::<IndexElement>();
    if index > available_slots {
      panic!("Out of bounds {} for length {}", index, self.len());
    }

    unsafe { self.index_deref_unchecked(index) }
  }

  pub unsafe fn index_deref_unchecked(&self, index: usize) -> &IndexElement {
    &*self
      .data
      .as_ptr()
      .cast::<AtomicI64>()
      .add(1)
      .cast::<IndexElement>()
      .add(index)
  }
}

impl std::ops::Index<u64> for Index {
  type Output = IndexElement;

  fn index(&self, index: u64) -> &Self::Output {
    self.index_deref(index as usize)
  }
}

impl std::ops::Index<i64> for Index {
  type Output = IndexElement;

  fn index(&self, index: i64) -> &Self::Output {
    self.index_deref(index as usize)
  }
}
