//! A journaled and unbounded MPMC broadcast channel.
//!
//! Aurora is a persistence channel implementation ideal for writing applications that must rely on
//! deterministic message replaying and durable message storage. At a high level, it is based on
//! three major components:
//!
//! * [Channel builder][builder];
//! * [Sender] handle;
//! * [Receiver] handle;
//!
//! [builder]: crate::ChannelBuilder
//! [Sender]: crate::Sender;
//! [Receiver]: crate::Receiver;
//!
//! # Channel Internals
//!
//! The channel implementation is based on the following concepts...

pub use crate::buider::{ChannelBuilder, ChannelConfigError};
pub use crate::metadata::{CompressionFormat, ConversionError, RollCycle, WireFormat};
pub use crate::receiver::{Receiver, ReceiverError};
pub use crate::sender::{Sender, SenderError};

mod buider;
mod channel;
mod compressor;
mod fs;
mod index;
mod metadata;
mod receiver;
mod sender;

const DATA_FILE_EXTENSION: &str = "aqd";
const METADATA_FILE_NAME: &str = "metadata.auq";
