# Aurora-Channel

An asynchronous, journaled and unbounded MPMC channel for writing persistent messaging applications.

*NOTE*: this is experimental software, not suitable for usage in production systems.
 
## Overview

_Aurora-channel_ is a journaled and unbounded MPMC channel ideal for writing persistent interprocess communication.
It uses shared memory backed by memory mapped files ([memmap](https://crates.io/crates/memmap)). Its main use case
is to provide durable topics and deterministic message replaying with low latency and high throughput &#8212; favoring
latency whenever these conflict. At a high level, it provides:

- lock free writes and reads (in platforms that provide atomic primitives);
- unbounded sub-millisecond IPC;
- on-the-fly unobtrusive message compression ([Snappy](https://github.com/google/snappy)
  or [LZ4](https://github.com/lz4/lz4));

It uses BSD advisory locks through the [flock]() syscall to coordinate reade/write access to the metadata and the 
compression task. It also uses append only writes and, thus, does not support inserts or deletions.
