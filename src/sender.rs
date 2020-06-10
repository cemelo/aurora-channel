use std::fs::{File, OpenOptions};

use memmap::{MmapMut, MmapOptions};
use thiserror::Error;

use crate::fs::{FdAdvisoryLock, FdLock, SyncFdLock};
use crate::{ChannelMetadata, WireFormat};
use serde::Serialize;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

// Should we change this to some constant over the average element size?
const FILE_EXPANSION_FACTOR: f64 = 0.5;
const INITIAL_FILE_SIZE: u64 = 1024;

/// Sending half of a file backed channel.
/// May be used from many threads. Messages can be sent with [`send`][Sender::send].
pub struct Sender {
  /// The hot storage path.
  hot_storage_path: PathBuf,

  /// The channel metadata.
  metadata: ChannelMetadata,

  /// The shared memory used to store the channel queue.
  data: MmapMut,

  /// The shared memory used to store the index.
  index: MmapMut,

  /// The starting offset of mapped memory in relation to the backing file.
  ///
  /// This is used to perform conversions between relative and absolute addressing, and to avoid
  /// mapping the entire file in memory for a particular sender.
  data_cursor_offset: usize,

  current_cycle: i64,
}

#[derive(Error, Debug)]
pub enum SenderError {
  #[error("I/O Error")]
  IoError(#[from] std::io::Error),
  #[error("File Locking Error")]
  LockError(#[from] crate::fs::LockError),
}

impl Sender {
  pub(crate) async fn new(hot_storage_path: impl AsRef<Path>, metadata: ChannelMetadata) -> Result<Self, SenderError> {
    let (current_cycle, file) = Sender::get_current_file(&hot_storage_path, &metadata).await?;
    let (index, data, data_cursor_offset) = Sender::get_sender_params_from_file(&file, &metadata)?;

    Ok(Sender {
      hot_storage_path: hot_storage_path.as_ref().to_path_buf(),
      metadata,
      data,
      index,
      data_cursor_offset,
      current_cycle,
    })
  }

  pub async fn send<T: Serialize + Sized>(&mut self, data: &T) -> Result<(), SenderError> {
    // First we check whether we should move to the next file based on the roll cycle.
    if self.current_cycle < self.metadata.roll_cycle.current_cycle() {
      let (current_cycle, file) = Sender::get_current_file(&self.hot_storage_path, &self.metadata).await?;
      let (index, data, data_cursor_offset) = Sender::get_sender_params_from_file(&file, &self.metadata)?;

      self.index = index;
      self.data = data;
      self.data_cursor_offset = data_cursor_offset;
      self.current_cycle = current_cycle;
    }

    let current_index_position = self.acquire_next_index_position();

    // Set the initial cursor position to the last written byte.
    let mut cursor_position = if current_index_position == 0 {
      self.metadata.index_size + 1
    } else {
      self.read_from_index(current_index_position as usize - 1)
    };

    // Wait until all previous writes have committed their object size.
    loop {
      if cursor_position != 0 {
        break;
      } else {
        // The index is guaranteed to be zeroed so, if a position is equal to zero, it means this operation
        // is still in uncommitted state. A negative cursor position means an uncommitted operation,
        // but with a committed range.

        async_std::task::sleep(Duration::from_micros(10)).await;
        cursor_position = self.read_from_index(current_index_position as usize - 1).abs();
      }
    }

    // At this point, the previous write was completed and now we can proceed.
    match self.metadata.wire_format {
      WireFormat::Raw => {
        let element_size = std::mem::size_of::<T>() as i64;

        // Commit element size with a negative position.
        self.write_to_index(current_index_position as usize, -(cursor_position + element_size));

        let data_write_offset = cursor_position as usize - self.data_cursor_offset;
        unsafe {
          let target_ptr = self.data.as_mut_ptr().offset(data_write_offset as isize);
          std::ptr::copy(data, target_ptr.cast(), 1);
        }

        // Commit operation by flipping the signal.
        self.write_to_index(current_index_position as usize, cursor_position + element_size);
      }
    }

    Ok(())
  }

  /// Acquires a position in the index and data file.
  fn acquire_next_index_position(&mut self) -> usize {
    let next_free_index_position_register = unsafe { &*self.index.as_ptr().cast::<AtomicU64>() };
    let current_index_position = next_free_index_position_register.fetch_add(1, Ordering::SeqCst);

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    unsafe {
      // Write back the cache line if available in the target platform.
      crate::atomic::x86_64::clwb(self.index.as_ptr());
    }

    current_index_position as usize
  }

  async fn get_current_file(
    hot_storage_path: impl AsRef<Path>,
    metadata: &ChannelMetadata,
  ) -> Result<(i64, FdAdvisoryLock<File>), SenderError> {
    // Get the current queue file.
    let current_cycle = metadata.roll_cycle.current_cycle();
    let file = OpenOptions::new().read(true).write(true).create(true).open(
      hot_storage_path
        .as_ref()
        .join(metadata.roll_cycle.file_name(current_cycle)),
    )?;

    // Try locking the file. If we can't acquire an exclusive (write) lock, try acquiring a (read) lock.
    let file = if let Ok(locked_file) = file.try_lock_exclusive() {
      if locked_file.metadata()?.len() == 0 {
        locked_file.set_len(metadata.index_size as u64 + INITIAL_FILE_SIZE)?;
      }

      locked_file
    } else {
      let file = OpenOptions::new().read(true).write(true).create(true).open(
        hot_storage_path
          .as_ref()
          .join(metadata.roll_cycle.file_name(current_cycle)),
      )?;

      file.lock_shared().await?
    };

    Ok((current_cycle, file))
  }

  fn get_sender_params_from_file(
    file: &File,
    metadata: &ChannelMetadata,
  ) -> Result<(MmapMut, MmapMut, usize), SenderError> {
    let index = unsafe { MmapOptions::new().len(metadata.index_size as usize).map_mut(&file)? };

    // The first 8-bytes of every queue file specify the next free index position. Newly created queue
    // files have these guaranteed to be zeroed.
    let next_free_index_position = unsafe { (*(index.as_ptr().cast::<AtomicU64>())).load(Ordering::SeqCst) } as usize;

    let data_cursor_offset = if next_free_index_position == 0 {
      // Set the cursor position to the beginning of the data section. The cursor address is relative to
      // the backing file, and not to the mapped memory section.
      metadata.index_size as i64 + 1
    } else {
      // If this sender is created over an existing queue (writable_index > 0), we need to move the cursor
      // to the free position.
      unsafe { *(index.as_ptr().cast::<i64>().add(next_free_index_position - 1)) }
    };

    // This is used merely to expand the mapped memory by a factor relative to the file size. In the
    // future we should have a better system, probably based on a factor of the file growth instead.
    let file_length = file.metadata()?.len();

    let data = unsafe {
      MmapOptions::new()
        .offset(data_cursor_offset.abs() as u64)
        .len((file_length as f64 * FILE_EXPANSION_FACTOR) as usize)
        .map_mut(&file)?
    };

    Ok((index, data, data_cursor_offset.abs() as usize))
  }

  fn read_from_index(&self, position: usize) -> i64 {
    unsafe { *self.index.as_ptr().cast::<i64>().add(1 + position) }
  }

  fn write_to_index(&mut self, position: usize, size: i64) {
    unsafe {
      let idx_element_ptr = self.index.as_mut_ptr().cast::<i64>().add(position + 1);
      *idx_element_ptr = size;
    }
  }
}
