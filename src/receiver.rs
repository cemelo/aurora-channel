use crate::ChannelMetadata;
use memmap::{MmapMut, MmapOptions};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::fs::File;
use std::mem::MaybeUninit;
use thiserror::Error;

pub struct Receiver {
  metadata: ChannelMetadata,
  data_map: MmapMut,
  index_map: MmapMut,

  current_index_position: u64,
  current_data_position: u64,
}

#[derive(Error, Debug)]
pub enum ReceiverError {
  #[error("I/O Error")]
  IoError(#[from] std::io::Error),
}

impl Receiver {
  pub(crate) fn new(file: &File, metadata: ChannelMetadata) -> Result<Self, ReceiverError> {
    let index_map = unsafe { MmapOptions::new().len(metadata.index_size as usize).map_mut(file)? };

    unsafe {
      dbg!((index_map.as_ptr().cast::<u64>()));
      dbg!(*(index_map.as_ptr().cast::<u64>()));
    }

    let open_index_position = unsafe { *(index_map.as_ptr().cast::<u64>()) };
    let mut last_written_position = metadata.index_size + 1;

    if open_index_position > 0 {
      last_written_position = unsafe {
        *(index_map
          .as_ptr()
          .cast::<i64>()
          .offset(open_index_position as isize - 1))
      };
    }

    // The map starts at the last offset
    let data_map = unsafe {
      MmapOptions::new()
        .offset(metadata.index_size as u64 + 1)
        .map_mut(file)?
    };

    Ok(Receiver {
      metadata,
      data_map,
      index_map,
      current_index_position: 0,
      current_data_position: 0,
    })
  }

  pub fn recv<'a, T: Deserialize<'a> + Clone + Sized>(&mut self) -> &'a T {
    // Read only committed transactions
    let mut last_written_byte = -1;
    while last_written_byte < 0 {
      last_written_byte = self.read_index_at(self.current_index_position as usize);
    }

    last_written_byte -= (self.metadata.index_size + 1);

    let data = unsafe {
      &*(self
        .data_map
        .as_ptr()
        .offset(self.current_data_position as isize)
        .cast())
    };

    self.current_index_position += 1;
    self.current_data_position = last_written_byte as u64;

    data
  }

  fn read_index_at(&self, position: usize) -> i64 {
    unsafe { *self.index_map.as_ptr().cast::<u64>().add(1).cast::<i64>().add(position) }
  }
}
