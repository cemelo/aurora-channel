use async_std::prelude::*;
use aurora_channel::ChannelBuilder;
use std::error::Error;

#[async_std::main]
async fn main() -> Result<(), Box<dyn Error>> {
  let channel = ChannelBuilder::new("/tmp/channel-sample").build().await?;

  let mut sender = channel.acquire_sender().await?;
  sender.send(&0xAAAAu16).await?;
  sender.send(&0xBBBBu16).await?;
  sender.send(&0xCCCCu16).await?;
  sender.send(&0xDDDDu16).await?;
  sender.send(&0xEEEEu16).await?;
  sender.send(&0xFFFFu16).await?;

  // let mut receiver = channel.subscribe()?;
  // println!("{:#04x}", receiver.recv::<u16>());
  // println!("{:#04x}", receiver.recv::<u16>());
  // println!("{:#04x}", receiver.recv::<u16>());
  // println!("{:#04x}", receiver.recv::<u16>());
  // println!("{:#04x}", receiver.recv::<u16>());
  // println!("{:#04x}", receiver.recv::<u16>());

  Ok(())
}
