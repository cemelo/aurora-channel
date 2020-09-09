use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use memmap::{MmapMut, MmapOptions};
use serde::export::PhantomData;
use serde::Serialize;
use thiserror::Error;
use tokio::fs::OpenOptions;

use crate::compressor::compress;
use crate::fs::FdLock;
use crate::index::Index;
use crate::metadata::{ChannelMetadata, WireFormat};

/// Sending half of a file backed channel.
///
/// May be used from a single thread only. Messages can be sent with [`send`][Sender::send].
///
/// # Examples
///
/// ```rust
/// use aurora_channel::*;
/// use std::error::Error;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn Error>> {
///   let channel = ChannelBuilder::new("/tmp/channel")
///     .wire_format(WireFormat::Bincode)
///     .roll_cycle(RollCycle::Second)
///     .compression(CompressionFormat::Uncompressed)
///     .build::<u32>()
///     .await?;
///
///   let mut sender = channel.acquire_sender().await?;
///   sender.send(&1).await?;
///
///   Ok(())
/// }
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
  current_cycle_timestamp: i64,

  /// The current roll cycle offset.
  write_cursor_position: u64,

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
      current_cycle_timestamp: 0,
      write_cursor_position: 0,
      data_type: Default::default(),

      data: MmapMut::map_anon(1)?,
    };

    sender.update_data_file().await?;

    Ok(sender)
  }

  /// Attempts to enqueue a value into the journal, which can be consumed by the [`Receiver`]
  /// handles.
  ///
  /// A successful send occurs when the sender handle is able to fully commit a write to the
  /// journal.
  ///
  /// # Write Semantics
  ///
  /// A write to the journal is composed of three phases:
  ///
  /// - index slot acquisition;
  /// - data serialization and persistence;
  /// - write commitment.
  ///
  ///
  ///
  /// Values sent through this interface are always appended to the file corresponding to the most
  /// current cycle. This means the handle will first check whether it is referring to the current
  /// cycle and move to another cycle if necessary. As a consequence, it's possible that a write
  /// to the journal will only be committed after the cycle completed.
  ///
  /// # Return
  ///
  /// On success, the number of bytes written to the journal is returned.
  ///
  /// # Note
  ///
  /// A return value of `Ok` **always** means the journal was updated. However, it does not mean
  /// the value will be observed by any or all of the active [`Receiver`] handles. [`Receiver`]
  /// handles may be dropped before receiving the sent message.
  ///
  /// [`Receiver`]: crate::Receiver
  pub async fn send(&mut self, data: &T) -> Result<u64, SenderError> {
    // Checks whether the current cycle was changed and update the memory mapped file accordingly.
    if self.current_cycle_timestamp < self.metadata.roll_cycle.current_cycle() {
      self.update_data_file().await?;
    }

    let element_size = match self.metadata.wire_format {
      #[cfg(feature = "format-bincode")]
      WireFormat::Bincode => bincode::serialized_size(data)?,
      #[cfg(feature = "format-json")]
      WireFormat::Json => serde_json::ser::to_string(data)?.len() as u64,
    };

    // Acquire a position in the index.
    let (element_index, write_cursor_position) = self.reserve_writeable_slot(element_size).await?;

    // Check whether there's any space left in the data file
    while !self.has_enough_memory_available(write_cursor_position, element_size) {
      self.expand_memory().await?;
    }

    // SAFETY: we always check whether the memory mapped file has enough room to write the current
    // element. Atomic semantics also guarantee that the space to be written was already
    // reserved in `self.reserve_writeable_slot(element_size: u64)`.
    unsafe {
      let target_ptr = self
        .data
        .as_mut_ptr()
        .add((write_cursor_position - self.write_cursor_position) as usize);
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
      .store(self.current_cycle_timestamp, Ordering::SeqCst);

    Ok(element_size)
  }

  async fn update_data_file(&mut self) -> Result<(), SenderError> {
    // Update the current cycle
    self.current_cycle_timestamp = self.metadata.roll_cycle.current_cycle();

    let data_file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(
        self
          .hot_storage_path
          .join(&self.metadata.roll_cycle.data_file_name(self.current_cycle_timestamp)),
      )
      .await?;

    let mut locked_data_file = data_file.lock_exclusive().await?;
    if locked_data_file.metadata().await?.len() == 0 {
      locked_data_file.set_len(self.metadata.data_block_size).await?;
    }

    let data_file = locked_data_file.unlock().into_std().await;
    self.data = unsafe { MmapOptions::new().map_mut(&data_file)? };

    // Try to compress uncompressed closed files
    tokio::spawn(compress(
      self.hot_storage_path.clone(),
      self.metadata.clone(),
    ));

    Ok(())
  }

  async fn reserve_writeable_slot(&mut self, element_size: u64) -> Result<(u64, u64), SenderError> {
    // Look at the last element of the index.
    // If the index is empty, the cursor position is set to zero.
    // If the index is not empty, the cursor will be zero if the previous cycle is less than the current
    // cycle, otherwise, it will be set as that position.

    // Compare-and-swap loop responsible for acquiring a position in the index
    'cas: loop {
      self.index.expand().await?;

      let current_index_size = self.index.len();
      let current_index_element = &self.index[current_index_size];

      let mut starting_cursor_position = 0;
      if !self.index.is_empty() {
        let prev_index_element = &self.index[current_index_size - 1];
        let prev_cycle_timestamp = prev_index_element.cycle_timestamp.load(Ordering::SeqCst).abs();

        // Ensures the writing cycle is always greater than the committed cycle
        if prev_cycle_timestamp > self.current_cycle_timestamp {
          self.update_data_file().await?;
          continue 'cas;
        }

        // If the cycle is the same, we increase the cursor, otherwise we keep it as zero
        if prev_cycle_timestamp == self.current_cycle_timestamp {
          starting_cursor_position = prev_index_element.last_cursor_position.load(Ordering::SeqCst);
        }
      };

      if current_index_element.last_cursor_position.compare_and_swap(
        0,
        starting_cursor_position + element_size,
        Ordering::SeqCst,
      ) == 0
      {
        // Slot acquisition succeeded. Increase the index size.
        self.index.increment_size(Ordering::SeqCst);

        // Store the cycle timestamp in with negative bit set to indicate an uncommitted operation.
        current_index_element
          .cycle_timestamp
          .store(-self.current_cycle_timestamp, Ordering::SeqCst);

        return Ok((current_index_size, starting_cursor_position));
      }
    }
  }

  fn has_enough_memory_available(&self, position: u64, element_size: u64) -> bool {
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
          .join(&self.metadata.roll_cycle.data_file_name(self.current_cycle_timestamp)),
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
