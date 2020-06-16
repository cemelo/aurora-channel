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

  let receiving_task = tokio::spawn(async move {
    loop {
      match receiver.recv().await.unwrap() {
        None => {}
        Some(message) => {
          println!("{}", message);
          break
        },
      }
    }
  });

  let sending_task = tokio::spawn(async move {
    sender.send(&"this is a message".to_string()).await.unwrap();
  });

  let (res_a, res_b) = futures::future::join(receiving_task, sending_task).await;

  res_a?;
  res_b?;

  Ok(())
}
