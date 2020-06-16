use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tokio::runtime::Builder;

use aurora_channel::*;

async fn write_then_read(data: &String, sender: &mut Sender<String>, receiver: &mut Receiver<String>) {
  sender.send(data).await.unwrap();
  assert_eq!(black_box(receiver.recv().await.unwrap().as_ref()), Some(data));
}

fn latency_benchmark(c: &mut Criterion) {
  let mut group = c.benchmark_group("latency and throughput");
  group.sample_size(1000);
  group.measurement_time(Duration::from_secs(60));

  for size in [16_384, 32_768].iter() {
    group.throughput(Throughput::Bytes(*size as u64));
    group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
      let mut runtime = Builder::new()
        .enable_io()
        .enable_time()
        .basic_scheduler()
        .build()
        .unwrap();

      let tempdir = tempfile::tempdir().unwrap();

      let channel = runtime
        .block_on(ChannelBuilder::new(tempdir.path().to_path_buf()).build::<String>())
        .unwrap();

      let mut sender = runtime.block_on(channel.acquire_sender()).unwrap();
      let mut receiver = runtime.block_on(channel.subscribe()).unwrap();

      let data = std::iter::repeat("0").take(size).collect::<String>();

      b.iter(|| runtime.block_on(write_then_read(&data, &mut sender, &mut receiver)))
    });
  }

  group.finish();
}

criterion_group!(benches, latency_benchmark);
criterion_main!(benches);
