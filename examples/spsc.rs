use std::error::Error;

use aurora_channel::{ChannelBuilder, WireFormat, RollCycle, CompressionFormat};
use tokio::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  let channel = ChannelBuilder::new("/tmp/channel")
    .wire_format(WireFormat::Json)
    .roll_cycle(RollCycle::Second)
    .compression(CompressionFormat::LZ4)
    .build::<String>()
    .await?;

  let mut sender = channel.acquire_sender().await?;
  let mut receiver = channel.subscribe().await?;

  let receiving_task = tokio::spawn(async move {
    loop {
      match receiver.recv().await.unwrap() {
        None => {}
        Some(message) if message == "exit" => {
          println!("{}", message);
          break;
        },
        Some(message) => {
          println!("{}", message);
        },
      }
    }
  });

  let sending_task = tokio::spawn(async move {
    // sender.send(&"Message: 1".to_string()).await.unwrap();
    // sender.send(&"Message: 2".to_string()).await.unwrap();
    // sender.send(&"Message: 3".to_string()).await.unwrap();
    // tokio::time::delay_for(Duration::from_secs(1)).await;
    //
    // sender.send(&"Message: 4".to_string()).await.unwrap();
    // sender.send(&"Message: 5".to_string()).await.unwrap();
    // sender.send(&"Message: 6".to_string()).await.unwrap();
    // tokio::time::delay_for(Duration::from_secs(1)).await;
    //
    // sender.send(&"Message: 7".to_string()).await.unwrap();
    // sender.send(&"Message: 8".to_string()).await.unwrap();
    // sender.send(&"Message: 9".to_string()).await.unwrap();
    // tokio::time::delay_for(Duration::from_secs(1)).await;
    //
    // sender.send(&"exit".to_string()).await.unwrap();
    // tokio::time::delay_for(Duration::from_secs(1)).await;
  });

  let (res_a, res_b) = futures::future::join(receiving_task, sending_task).await;

  res_a?;
  res_b?;

  Ok(())
}
