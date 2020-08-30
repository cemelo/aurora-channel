use std::path::PathBuf;

use serde::de::DeserializeOwned;
use serde::export::PhantomData;
use serde::Serialize;

use crate::metadata::ChannelMetadata;
use crate::receiver::Receiver;
use crate::receiver::ReceiverError;
use crate::sender::Sender;
use crate::sender::SenderError;
use std::fmt::Debug;

pub struct Channel<T: Serialize + DeserializeOwned + ?Sized> {
  pub(crate) hot_storage_path: PathBuf,
  pub(crate) cool_storage_path: Option<PathBuf>,
  pub(crate) cold_storage_path: Option<PathBuf>,

  pub(crate) metadata: ChannelMetadata,

  pub(crate) element_type: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned + Debug + ?Sized> Channel<T> {
  pub async fn acquire_sender(&self) -> Result<Sender<T>, SenderError> {
    Ok(Sender::new(&self.hot_storage_path, self.metadata.clone()).await?)
  }

  pub async fn subscribe(&self) -> Result<Receiver<T>, ReceiverError> {
    Ok(
      Receiver::new(
        &self.hot_storage_path,
        self.cool_storage_path.as_ref(),
        self.cold_storage_path.as_ref(),
        self.metadata.clone(),
      )
      .await?,
    )
  }
}
