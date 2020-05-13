use aurora_queue::{CompressionFormat, QueueBuilder, QueueError, Receiver, ReceiverError, RollCycle};

#[tokio::main]
async fn main() -> Result<(), QueueError> {
  let queue = QueueBuilder::new("/tmp/aurora-queue".into())
    .with_roll_cycle(RollCycle::Second)
    .with_compression(CompressionFormat::Snappy)
    .build()
    .await?;

  let receiver = queue.subscribe().await?;
  run_receiver(receiver).await?;

  Ok(())
}

async fn run_receiver(mut receiver: Receiver) -> Result<(), ReceiverError> {
  println!("Ready");

  let mut read_result = receiver.next::<i64>().await?;
  while read_result.is_none() {
    read_result = receiver.next::<i64>().await?;
  }

  loop {
    let timestamp = chrono::Local::now().timestamp_nanos();

    if let Some(next) = read_result {

      if next == 0i64 {
        break;
      }

      println!("{} {}", next, timestamp - next)
    }

    read_result = receiver.next::<i64>().await?;
  }

  Ok(())
}
