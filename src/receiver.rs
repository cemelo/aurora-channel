use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use memmap::{Mmap, MmapMut, MmapOptions};
use serde::de::DeserializeOwned;
use serde::export::PhantomData;
use thiserror::Error;
use tokio::fs::OpenOptions;

use crate::metadata::{ChannelMetadata, WireFormat};
use crate::fs::LockError;
use crate::index::Index;

#[allow(dead_code)]
pub struct Receiver<T: DeserializeOwned> {
  metadata: ChannelMetadata,
  index: Index,
  data: Mmap,

  hot_storage_path: PathBuf,
  cool_storage_path: Option<PathBuf>,
  cold_storage_path: Option<PathBuf>,

  current_index_position: i64,
  current_cycle_timestamp: i64,

  element_type: PhantomData<T>,
}

#[derive(Error, Debug)]
pub enum ReceiverError {
  #[error("Unexpected end of stream reached")]
  EndOfStream,
  #[error("File lock acquisition error")]
  FdLockError(#[from] LockError),
  #[error("I/O Error")]
  IoError(#[from] std::io::Error),
  #[error("Indexing error")]
  IndexError(#[from] crate::index::IndexError),
  #[cfg(feature = "format-bincode")]
  #[error("Bincode deserialization error")]
  BincodeDeserializationError(#[from] bincode::Error),
  #[cfg(feature = "format-json")]
  #[error("Json deserialization error")]
  JsonDeserializationError(#[from] serde_json::Error),
  #[error("Unknown error")]
  UnknownError(#[from] Box<dyn Error>),
}

impl<T: DeserializeOwned> Receiver<T> {
  pub(crate) async fn new(
    hot_storage_path: impl AsRef<Path>,
    cool_storage_path: Option<impl AsRef<Path>>,
    cold_storage_path: Option<impl AsRef<Path>>,
    metadata: ChannelMetadata,
  ) -> Result<Self, ReceiverError> {
    let index = Index::new(&hot_storage_path, metadata.index_block_size).await?;

    Ok(Receiver {
      metadata,
      index,
      data: MmapMut::map_anon(1)?.make_read_only()?,
      hot_storage_path: hot_storage_path.as_ref().to_path_buf(),
      cool_storage_path: cool_storage_path.map(|p| p.as_ref().to_path_buf()),
      cold_storage_path: cold_storage_path.map(|p| p.as_ref().to_path_buf()),
      current_index_position: -1,
      current_cycle_timestamp: 0,
      element_type: Default::default(),
    })
  }

  pub async fn recv(&mut self) -> Result<Option<T>, ReceiverError> {
    let next_index_position = (self.current_index_position + 1) as u64;
    if self.index.len() < next_index_position + 1 {
      return Ok(None);
    }

    self.refresh_index().await?;

    let (next_cycle_timestamp, last_cursor_position) = {
      let index_element = &self.index[next_index_position];
      (
        index_element.cycle_timestamp.load(Ordering::SeqCst),
        index_element.last_cursor_position.load(Ordering::SeqCst),
      )
    };

    if next_cycle_timestamp == 0 {
      // Stream ended or element not yet committed.
      return Ok(None);
    }

    if self.data.len() < last_cursor_position.abs() as usize || next_cycle_timestamp != self.current_cycle_timestamp {
      self.current_cycle_timestamp = next_cycle_timestamp;
      self.map_data_file().await?;
    }

    let index_element = &self.index[next_index_position];

    // Read data
    let read_cursor_position = if next_index_position == 0 {
      0
    } else {
      (&self.index[self.current_index_position])
        .last_cursor_position
        .load(Ordering::SeqCst)
    };

    let element_size = index_element.last_cursor_position.load(Ordering::SeqCst) - read_cursor_position;
    let reader = unsafe {
      let target_ptr = self.data.as_ptr().add(read_cursor_position as usize);
      std::slice::from_raw_parts(target_ptr, element_size as usize)
    };

    let data: T = match self.metadata.wire_format {
      #[cfg(feature = "format-bincode")]
      WireFormat::Bincode => bincode::deserialize_from(reader)?,
      #[cfg(feature = "format-json")]
      WireFormat::Json => serde_json::de::from_reader(reader)?,
    };

    self.current_index_position = next_index_position as i64;

    Ok(Some(data))
  }

  async fn refresh_index(&mut self) -> Result<(), ReceiverError> {
    let next_index_position = (self.current_index_position + 1) as u64;

    if self.index.len() > next_index_position && self.index.last_mapped_index() < next_index_position {
      self.index.remap().await?;
    }

    Ok(())
  }

  async fn map_data_file(&mut self) -> Result<(), ReceiverError> {
    // TODO search in cool and cold storages if the cycle is below the thresholds
    // TODO add a shared lock to prevent files in use from being moved between storages

    let data_file_name = self.metadata.roll_cycle.data_file_name(self.current_cycle_timestamp);
    let data_file = OpenOptions::new()
      .read(true)
      .open(self.hot_storage_path.join(data_file_name))
      .await?;

    self.data = unsafe { MmapOptions::new().map(&data_file.into_std().await) }?;

    Ok(())
  }
}

// impl Receiver {
//   pub(crate) async fn new(
//     hot_storage_path: impl AsRef<Path>,
//     cool_storage_path: Option<impl AsRef<Path>>,
//     cold_storage_path: Option<impl AsRef<Path>>,
//     metadata: ChannelMetadata,
//   ) -> Result<Self, ReceiverError> {
//     // Find first file available to be read
//     let mut cycles_index = OpenOptions::new()
//       .read(true)
//       .open(hot_storage_path.as_ref().join(INDEX_FILE_NAME))
//       .map_err(|e| ReceiverError::EndOfStream)?;
//
//     // Very simple way to wait until we have the next file available
//     while cycles_index.metadata()?.len() < std::mem::size_of::<i64>() as u64 {
//       async_std::task::sleep(Duration::from_micros(10)).await;
//       cycles_index.sync_data()?;
//     }
//
//     let current_cycle = cycles_index.read_i64::<LittleEndian>()?;
//
//     // TODO change to lookup for the file in each storage.
//     let file = OpenOptions::new().read(true).open(
//       hot_storage_path
//         .as_ref()
//         .join(metadata.roll_cycle.data_file_name(current_cycle)),
//     )?;
//
//     // Locking prevents any background jobs from moving this file to cool or cold storage.
//     let file = file.lock_shared().await?;
//
//     let data_cursor_offset = 0; //metadata.index_size as u64 + 1;
//     let data_mapped_size = MAX_MAPPED_MEMORY_SIZE.min(file.metadata()?.len() -
// data_cursor_offset);
//
//     let index = unsafe { MmapOptions::new().len(metadata.index_block_size as usize).map(&file)?
// };     let data = unsafe {
//       MmapOptions::new()
//         //.offset(metadata.index_size as u64 + 1)
//         .len(data_mapped_size as usize)
//         .map(&file)?
//     };
//
//     Ok(Receiver {
//       metadata,
//       data,
//       index,
//       cycles_index,
//       hot_storage_path: hot_storage_path.as_ref().to_path_buf(),
//       cool_storage_path: cool_storage_path.map(|p| p.as_ref().to_path_buf()),
//       cold_storage_path: cold_storage_path.map(|p| p.as_ref().to_path_buf()),
//       current_cycle,
//       current_data_file: file,
//       current_index_position: 0,
//       current_cursor_position: 0,
//       data_cursor_offset,
//     })
//   }
//
//   async fn update_internal_state(&mut self) -> Result<(), ReceiverError> {
//     // If we reached the end of mapped memory, check if there's still data to be read.
//     let last_written_byte_position = self.read_index_at(self.free_index_position() as usize -
// 1).abs();
//
//     if (self.data_cursor_offset + self.current_cursor_position) == last_written_byte_position as
// u64 {       // We reached EOF. If we're in a cycle greater than the current, we try to get that
// file -- as,       // ideally, cycles don't move backwards.
//       let current_cycle = self.metadata.roll_cycle.current_cycle();
//       if self.current_cycle == current_cycle {
//         // Cycle didn't change, so we just send an EndOfStream error.
//         return Err(ReceiverError::EndOfStream);
//       } else {
//         // There's a new cycle. Let's try first to move to the next file present in the cycle
// index.         if let Ok(next_indexed_cycle) = self.cycles_index.read_i64::<LittleEndian>() {
//           self.current_cycle = next_indexed_cycle;
//           self.current_index_position = 0;
//           self.current_cursor_position = 0;
//
//           // TODO change to lookup for the file in each storage.
//           let file = OpenOptions::new().read(true).create(true).open(
//             self
//               .hot_storage_path
//               .join(self.metadata.roll_cycle.data_file_name(self.current_cycle)),
//           )?;
//
//           // self.data_cursor_offset = self.metadata.index_size as u64 + 1;
//           self.current_data_file = file.lock_shared().await?;
//           let data_mapped_size =
//             MAX_MAPPED_MEMORY_SIZE.min(self.current_data_file.metadata()?.len() -
// self.data_cursor_offset);
//
//           let index = unsafe {
//             MmapOptions::new()
//               // .len(self.metadata.index_size as usize)
//               .map(&self.current_data_file)?
//           };
//
//           self.index = index;
//           self.data = unsafe {
//             MmapOptions::new()
//               // .offset(self.metadata.index_size as u64 + 1)
//               .len(data_mapped_size as usize)
//               .map(&self.current_data_file)?
//           };
//         } else {
//           return Err(ReceiverError::EndOfStream);
//         }
//       }
//     } else {
//       // We did not reach EOF. Let's pull the next chunk into memory.
//       self.data_cursor_offset = self.current_cursor_position;
//       let data_mapped_size =
//         MAX_MAPPED_MEMORY_SIZE.min(self.current_data_file.metadata()?.len() -
// self.data_cursor_offset);
//
//       self.data = unsafe {
//         MmapOptions::new()
//           .offset(self.data_cursor_offset)
//           .len(data_mapped_size as usize)
//           .map(&self.current_data_file)?
//       };
//
//       self.current_cursor_position = 0;
//     }
//
//     Ok(())
//   }
//
//   fn has_enough_bytes_available(&self) -> bool {
//     let last_byte_to_read = self.read_index_at(self.current_index_position as usize);
//     let has_enough_mapped_memory = (self.data.len() + self.data_cursor_offset as usize) >
// last_byte_to_read as usize;     let has_additional_indexed_items = self.free_index_position() >
// self.current_index_position;
//
//     has_enough_mapped_memory && has_additional_indexed_items
//   }
//
//   pub async fn recv<'a, T: 'a + Deserialize<'a> + Clone + Sized>(&mut self) -> Result<T,
// ReceiverError> {     if !self.has_enough_bytes_available() {
//       self.update_internal_state().await?;
//     }
//
//     // Read only committed transactions
//     let mut last_written_byte = -1;
//     while last_written_byte < 0 {
//       last_written_byte = self.read_index_at(self.current_index_position as usize);
//       async_std::task::sleep(Duration::from_micros(10)).await;
//     }
//
//     last_written_byte -= self.data_cursor_offset as i64;
//
//     let data_bytes = unsafe {
//       std::slice::from_raw_parts(
//         self.data.as_ptr().offset(self.current_cursor_position as isize),
//         (last_written_byte as usize - self.current_cursor_position as usize),
//       )
//     };
//
//     let data = bincode::deserialize::<'a, T>(data_bytes)?;
//
//     self.current_index_position += 1;
//     self.current_cursor_position = last_written_byte as u64;
//
//     Ok(data)
//   }
//
//   fn free_index_position(&self) -> u64 {
//     unsafe { *self.index.as_ptr().cast::<u64>() }
//   }
//
//   fn read_index_at(&self, position: usize) -> i64 {
//     unsafe { *self.index.as_ptr().cast::<i64>().add(position + 1) }
//   }
// }
