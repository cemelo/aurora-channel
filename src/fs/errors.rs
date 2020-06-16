use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum LockError {
  #[error("file locked")]
  ResourceUnavailable,
  #[error("invalid file descriptor")]
  BadFileDescriptor,
  #[error("the call was interrupted by delivery of a signal caught by a handler while waiting to acquire a lock")]
  Interrupted,
  #[error("the kernel ran out of memory for allocating lock records.")]
  NoLockMemory,
  #[error("unknown error")]
  Other(i32),
}

impl Into<std::io::Error> for LockError {
  fn into(self) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, self)
  }
}
