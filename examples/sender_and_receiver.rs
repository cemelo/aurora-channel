use aurora_queue::{CompressionFormat, QueueBuilder, QueueError, RollCycle, Sender, SenderError};

#[tokio::main]
async fn main() -> Result<(), QueueError> {
  let tmpdir = tempfile::tempdir()?;
  let queue = QueueBuilder::new(tmpdir.path().to_path_buf())
    .with_roll_cycle(RollCycle::Day)
    .with_compression(CompressionFormat::LZ4)
    .build()
    .await?;

  let mut sender = queue.acquire_sender().await?;
  let mut receiver = queue.subscribe().await?;
  for i in 0..1_500_000 {
    sender.send("test").await?;
    let read = receiver.next::<String>().await?;

    if read.is_none() {
      panic!("{}", i);
    }
  }

  Ok(())
}
