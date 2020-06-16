# Aurora-Channel

A journaled and unbounded MPMC broadcast channel for writing persistent messaging applications.

*NOTE*: this is experimental software, not suitable for usage in production systems.
 
## Overview

_Aurora-channel_ is a journaled and unbounded MPMC channel ideal for writing persistent interprocess communication.
It uses shared memory backed by memory mapped files ([memmap](https://crates.io/crates/memmap)). Its main use case
is to provide durable topics and deterministic message replaying with low latency and high throughput — favoring
latency whenever they conflict. At a high level, it provides:

- (mostly) lock free writes and reads (in platforms that provide atomic primitives);
- unbounded sub-millisecond IPC;
- journaled writes and deterministic replay support.

It uses BSD advisory locks through the [flock]() syscall to coordinate read/write access to the metadata and 
data/index file resizes. It also uses append only writes and, thus, does not support inserts or deletions.

## Performance

According to the benchmarks, this queue provides sub millisecond latency for messages ranging from 40 bytes up to
32KiB using a single threaded executor — and possibly with larger messages, but remains to be benchmarked. The
hotpath is probably in the file resizing process, i.e., with larger initial sizes for both index and data files,
throughput and latency are improved, but disk usage is, obviously, higher.

In this second iteration of the project, using memory mapped files instead of relying on `O_APPEND` greatly improved
latency and throughput, while imposing higher complexity to the codebase.

Bellow are the results of the single_threaded benchmark ran on macOS using an Intel Core i9 2.4 GHz.

    latency and throughput/40                                                                             
                            time:   [5.4725 us 5.5100 us 5.5543 us]
                            thrpt:  [6.8680 MiB/s 6.9233 MiB/s 6.9706 MiB/s]
    Found 127 outliers among 1000 measurements (12.70%)
      12 (1.20%) low severe
      31 (3.10%) low mild
      51 (5.10%) high mild
      33 (3.30%) high severe
      
    latency and throughput/160                                                                             
                            time:   [5.7418 us 5.7749 us 5.8101 us]
                            thrpt:  [26.263 MiB/s 26.423 MiB/s 26.575 MiB/s]
    Found 61 outliers among 1000 measurements (6.10%)
      52 (5.20%) high mild
      9 (0.90%) high severe
      
    latency and throughput/220                                                                              
                            time:   [6.1388 us 6.2502 us 6.4248 us]
                            thrpt:  [32.656 MiB/s 33.568 MiB/s 34.177 MiB/s]
    Found 39 outliers among 1000 measurements (3.90%)
      23 (2.30%) high mild
      16 (1.60%) high severe
      
    latency and throughput/512                                                                              
                            time:   [6.9746 us 7.1113 us 7.2637 us]
                            thrpt:  [67.222 MiB/s 68.663 MiB/s 70.008 MiB/s]
    Found 77 outliers among 1000 measurements (7.70%)
      37 (3.70%) high mild
      40 (4.00%) high severe
      
    latency and throughput/1024                                                                              
                            time:   [7.6675 us 7.7255 us 7.7909 us]
                            thrpt:  [125.35 MiB/s 126.41 MiB/s 127.36 MiB/s]
    Found 84 outliers among 1000 measurements (8.40%)
      53 (5.30%) high mild
      31 (3.10%) high severe

    latency and throughput/16384                                                                              
                            time:   [59.298 us 59.948 us 60.648 us]
                            thrpt:  [257.63 MiB/s 260.64 MiB/s 263.50 MiB/s]
    Found 57 outliers among 1000 measurements (5.70%)
      13 (1.30%) low mild
      24 (2.40%) high mild
      20 (2.00%) high severe
    
    latency and throughput/32768                                                                              
                            time:   [97.801 us 99.226 us 100.72 us]
                            thrpt:  [310.27 MiB/s 314.94 MiB/s 319.53 MiB/s]
    Found 98 outliers among 1000 measurements (9.80%)
      4 (0.40%) low mild
      40 (4.00%) high mild
      54 (5.40%) high severe

## Example

```rust
use std::error::Error;

use aurora_channel::{ChannelBuilder, WireFormat};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  let tempdir = tempfile::tempdir()?;
  let channel = ChannelBuilder::new(tempdir.path())
    .wire_format(WireFormat::Bincode)
    .build::<String>()
    .await?;

  let mut sender = channel.acquire_sender().await?;
  let mut receiver = channel.subscribe().await?;

  sender.send(&"this is a message".to_string()).await?;
  let message = receiver.recv().await?;

  Ok(())
}
```

## Internals

The process relies in three basic files: metadata, index and data. 

`metadata.auq` stores the initial channel configuration, and is responsible for keeping configuration consistent among
different processes. This means that, for any existing channel/queue, it's configuration will remain the same even
when a different set is provided on instantiation.

`index.aui` stores the channel index. This file has a very simple structure (detailed below), and is responsible for
the coordination between multiple senders and also between senders and receivers.

    +------------------+-----------------+------------------+-----+-----------------+------------------+
    | Index Size (u64) | Cycle [0] (i64) | Cursor [0] (i64) | ... | Cycle [N] (i64) | Cursor [N] (i64) |
    +------------------+-----------------+------------------+-----+-----------------+------------------+

We rely on atomic primitives to allocate space in the data file for a particular sender using the following algorithm:

  1. load the current index size (n);
  2. compare and swap the nth index element's cursor to the last position to be written in the file;
  3. if it succeeds, proceed; otherwise, go to 1);
  4. write the contents to the data file;
  5. store the current cycle timestamp into the nth index element;
  6. a write is considered committed.
  
Finally, the multiple data files and their naming will depend on which `RollCycle` is selected. A `Receiver` will only
read committed writes. An uncommitted write will prevent all receivers from proceeding, and how to deal with this
issue remains an open problem.

## Portability

This project is, currently, focused on *nix operating systems. Windows support _might_ come in the future.

## TODO

A few ideas are also still to be implemented, namely:

  - [ ] Support for moving old events between hot, cool and cold storage;
  - [ ] On-the-fly encryption support;
  - [ ] On-the-fly compression support;
  - [ ] Named receivers and persisten positioning storage;

## Code of Conduct

This project follows the [Rust Code of Conduct](https://www.rust-lang.org/conduct.html).