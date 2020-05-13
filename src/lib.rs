#![feature(backtrace)]

pub use crate::builder::QueueBuilder;
pub use crate::compression::CompressionFormat;
pub use crate::queue::*;
pub use crate::receiver::*;
pub use crate::sender::*;

mod builder;
mod compression;
mod crypto;
mod fs;
mod indexer;
mod queue;
mod receiver;
mod sender;

const W_TRUE: u8 = 0xFFu8;
const W_FALSE: u8 = 0u8;

const W_HOT_STORAGE: u8 = 0xFFu8;
const W_COOL_STORAGE: u8 = 0xF0u8;
const W_COLD_STORAGE: u8 = 0x0Fu8;