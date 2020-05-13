use std::time::Duration;

use aurora_queue::{CompressionFormat, QueueBuilder, QueueError, RollCycle, Sender, SenderError};

#[tokio::main]
async fn main() -> Result<(), QueueError> {
  let queue = QueueBuilder::new("/tmp/aurora-queue".into())
    .with_roll_cycle(RollCycle::Second)
    .with_compression(CompressionFormat::Snappy)
    .with_compression_cycle(1)
    .build()
    .await?;

  let sender = queue.acquire_sender().await?;
  run_sender(sender).await?;

  Ok(())
}

async fn run_sender(mut sender: Sender) -> Result<(), SenderError> {
  for i in 0..1_000 {
    sender.send(&(chrono::Local::now().timestamp_nanos())).await?;
  }

  sender.send(&0i64).await?;

  Ok(())
}
