# Aurora-Queue

An asynchronous MPMC journaled and unbounded queue for writing persistent messaging applications.

*NOTE*: this is experimental software, not suitable for usage in production systems.
 
## Overview

_Aurora-queue_ is an async durable queue for writing persistent messaging over POSIX primitives. It's main use
case is to provide durable topics for applications that require fault tolerance and deterministic replay of
messages. At a high level, it provides:

- wait free writing of messages;
- unbounded sub-millisecond IPC;
- on-the-fly unobtrusive message compression ([Snappy](https://github.com/google/snappy) or [LZ4](https://github.com/lz4/lz4));

It uses BSD advisory locks through the [flock]() syscall to coordinate reade/write access to the metadata, index files,
and the compression task. It also uses append only writes and, thus, does not support inserts or deletions.

## Performance

According to the benchmarks, this queue provides sub millisecond latency for messages ranging from 512 bytes up to
20KiB using a single threaded executor. Throughput, on the other hand, still suffers from the lack of optimization and
is probably bound by the high amount of syscalls present.

Bellow are the results of the `single_threaded` benchmark ran on macOS using an Intel Core i9 2.4 GHz.

```
latency and throughput/40                                                                              
                        time:   [50.711 us 50.773 us 50.834 us]
                        thrpt:  [768.44 KiB/s 769.36 KiB/s 770.30 KiB/s]
Found 66 outliers among 1000 measurements (6.60%)
  2 (0.20%) low mild
  32 (3.20%) high mild
  32 (3.20%) high severe
latency and throughput/160                                                                              
                        time:   [52.547 us 52.719 us 52.912 us]
                        thrpt:  [2.8838 MiB/s 2.8944 MiB/s 2.9038 MiB/s]
Found 38 outliers among 1000 measurements (3.80%)
  12 (1.20%) high mild
  26 (2.60%) high severe
latency and throughput/220                                                                              
                        time:   [55.261 us 55.772 us 56.345 us]
                        thrpt:  [3.7236 MiB/s 3.7619 MiB/s 3.7967 MiB/s]
Found 48 outliers among 1000 measurements (4.80%)
  16 (1.60%) high mild
  32 (3.20%) high severe
latency and throughput/512                                                                              
                        time:   [52.519 us 52.582 us 52.647 us]
                        thrpt:  [9.2746 MiB/s 9.2860 MiB/s 9.2973 MiB/s]
Found 27 outliers among 1000 measurements (2.70%)
  13 (1.30%) high mild
  14 (1.40%) high severe
```

## Example

```rust
use aurora_queue::{CompressionFormat, QueueBuilder, QueueError, RollCycle, Sender, SenderError};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let tmpdir = tempfile::tempdir()?;
  let queue = QueueBuilder::new(tmpdir.path().to_path_buf())
    .with_roll_cycle(RollCycle::Day)
    .with_compression(CompressionFormat::LZ4)
    .build()
    .await?;

  let mut sender = queue.acquire_sender().await?;
  let mut receiver = queue.subscribe().await?;

  tokio::spawn(async move {
    sender.send("Hello, world!").await;
  });

  let read = receiver.next::<String>().await?;
  println!("{}", read);

  Ok(())
}
```

## TODO

A few ideas are also still to be implemented, and, apart from optimization, this is what's currently on the roadmap:

- [ ] Support for moving old events between hot, cool and cold storage;
- [ ] On-the-fly encryption support;

## License

This library is distributed under the terms of the Mozilla Public License, version 2.0.

## Code of Conduct

This project follows the [Rust Code of Conduct](https://www.rust-lang.org/conduct.html).