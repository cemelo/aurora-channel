#![feature(option_result_contains)]

pub use crate::buider::ChannelBuilder;
pub use crate::metadata::{CompressionFormat, ConversionError, RollCycle, WireFormat};
pub use crate::receiver::Receiver;
pub use crate::sender::Sender;

mod buider;
mod channel;
mod fs;
mod index;
mod metadata;
mod receiver;
mod sender;
mod compressor;

const DATA_FILE_EXTENSION: &str = "aqd";
const METADATA_FILE_NAME: &str = "metadata.auq";