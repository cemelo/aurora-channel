use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use memmap::{MmapMut, MmapOptions};
use serde::export::PhantomData;
use serde::Serialize;
use thiserror::Error;
use tokio::fs::OpenOptions;

use crate::fs::FdLock;
use crate::index::Index;
use crate::metadata::{ChannelMetadata, WireFormat};

/// Sending half of a file backed channel.
/// May be used from a single thread only. Messages can be sent with [`send`][Sender::send].
///
/// Index file format:
///
/// ```text
/// 0                7
/// +-----------------+
/// |   Index Size    |
/// +-----------------+
/// |      Cycle      |
/// +-----------------+
/// | Cursor Position |
/// +-----------------+
///         ...
/// ```
pub struct Sender<T: Serialize + ?Sized> {
  /// The hot storage path.
  hot_storage_path: PathBuf,

  /// The channel metadata.
  metadata: ChannelMetadata,

  /// The shared memory used to store the channel queue.
  data: MmapMut,

  /// The shared memory used to store the index.
  index: Index,

  /// The current roll cycle.
  current_cycle: i64,

  data_type: PhantomData<T>,
}

#[derive(Error, Debug)]
pub enum SenderError {
  #[error("I/O error")]
  IoError(#[from] std::io::Error),
  #[error("File locking error")]
  LockError(#[from] crate::fs::LockError),
  #[error("Indexing error")]
  IndexError(#[from] crate::index::IndexError),
  #[cfg(feature = "format-bincode")]
  #[error("Bincode serialization error")]
  BincodeSerializationError(#[from] bincode::Error),
  #[cfg(feature = "format-json")]
  #[error("Json serialization error")]
  JsonSerializationError(#[from] serde_json::Error),
  #[error("Unknown error")]
  UnknownError(#[from] Box<dyn Error>),
}

impl<T: Serialize + ?Sized> Sender<T> {
  pub(crate) async fn new(hot_storage_path: impl AsRef<Path>, metadata: ChannelMetadata) -> Result<Self, SenderError> {
    let index = Index::new(&hot_storage_path, metadata.index_block_size).await?;

    let mut sender = Sender {
      hot_storage_path: hot_storage_path.as_ref().to_path_buf(),
      metadata,
      index,
      current_cycle: 0,
      data_type: Default::default(),

      data: MmapMut::map_anon(1)?,
    };

    sender.update_data_file().await?;

    Ok(sender)
  }

  pub async fn send(&mut self, data: &T) -> Result<(), SenderError> {
    // Checks and updates the current cycle
    if self.current_cycle < self.metadata.roll_cycle.current_cycle() {
      self.update_data_file().await?;
    }

    let element_size = match self.metadata.wire_format {
      #[cfg(feature = "format-bincode")]
      WireFormat::Bincode => bincode::serialized_size(data)?,
      #[cfg(feature = "format-json")]
      WireFormat::Json => serde_json::ser::to_string(data)?.len() as u64,
    };

    // Acquire the next index position
    let (element_index, write_cursor_position) = self.reserve_writeable_slot(element_size).await?;

    // Check whether there's any space left in the data file
    while !self.has_enough_memory_available(write_cursor_position, element_size) {
      self.expand_memory().await?;
    }

    // SAFETY: we always check whether the memory mapped file has enough room to write the current
    // element. Atomic semantics also guarantee that the space to be written was already
    // reserved in `self.reserve_writeable_slot(element_size: u64)`.
    unsafe {
      let target_ptr = self.data.as_mut_ptr().add(write_cursor_position as usize);
      let data_slice = std::slice::from_raw_parts_mut(target_ptr, element_size as usize);

      match self.metadata.wire_format {
        #[cfg(feature = "format-bincode")]
        WireFormat::Bincode => bincode::serialize_into(data_slice, data)?,
        #[cfg(feature = "format-json")]
        WireFormat::Json => serde_json::ser::to_writer(data_slice, data)?,
      }
    }

    // Commit operation by storing the current cycle
    self.index[element_index]
      .cycle_timestamp
      .store(self.current_cycle, Ordering::SeqCst);

    Ok(())
  }

  async fn update_data_file(&mut self) -> Result<(), SenderError> {
    // Update the current cycle
    self.current_cycle = self.metadata.roll_cycle.current_cycle();

    let data_file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(
        self
          .hot_storage_path
          .join(&self.metadata.roll_cycle.data_file_name(self.current_cycle)),
      )
      .await?;

    let mut locked_data_file = data_file.lock_exclusive().await?;
    if locked_data_file.metadata().await?.len() == 0 {
      locked_data_file.set_len(self.metadata.data_block_size).await?;
    }

    let data_file = locked_data_file.unlock().into_std().await;
    self.data = unsafe { MmapOptions::new().map_mut(&data_file)? };

    Ok(())
  }

  async fn reserve_writeable_slot(&mut self, element_size: u64) -> Result<(u64, i64), SenderError> {
    loop {
      self.index.expand().await?;

      let current_index_size = self.index.len();
      let current_index_element = &self.index[current_index_size];

      let starting_cursor_position = if current_index_size == 0 {
        0
      } else {
        let prev_index_element = &self.index[current_index_size - 1];
        prev_index_element.last_cursor_position.load(Ordering::SeqCst).abs()
      };

      if current_index_element.last_cursor_position.compare_and_swap(
        0,
        starting_cursor_position + element_size as i64,
        Ordering::SeqCst,
      ) == 0
      {
        // Commit succeeded. Increase the index size.
        self.index.increment_size(Ordering::SeqCst);

        // #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        // unsafe {
        //   // Write back the cache lines if available in the target platform.
        //   crate::atomic::x86_64::clwb(self.index.as_ptr().cast::<AtomicU64>());
        //   crate::atomic::x86_64::clwb(
        //     self
        //       .index
        //       .as_ptr()
        //       .cast::<AtomicI64>()
        //       .add(current_index_position as usize),
        //   );
        // }

        return Ok((current_index_size, starting_cursor_position.abs()));
      }
    }
  }

  fn has_enough_memory_available(&self, position: i64, element_size: u64) -> bool {
    self.data.len() as u64 >= position as u64 + element_size
  }

  async fn expand_memory(&mut self) -> Result<(), SenderError> {
    let data_file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(
        self
          .hot_storage_path
          .join(&self.metadata.roll_cycle.data_file_name(self.current_cycle)),
      )
      .await?;

    let mut locked_data_file = data_file.lock_exclusive().await?;

    // Only resizes file if it's length is too close to the mapped memory size.
    // This should prevent concurrent tasks from resizing the file at the same point.
    let current_length = locked_data_file.metadata().await?.len();
    if current_length < self.data.len() as u64 + self.metadata.data_block_size {
      locked_data_file
        .set_len(current_length + self.metadata.data_block_size)
        .await?;
    }

    let data_file = locked_data_file.unlock().into_std().await;
    self.data = unsafe { MmapOptions::new().map_mut(&data_file)? };

    Ok(())
  }
}
