use std::fmt::Arguments;
use std::ops::{Deref, DerefMut};
use std::os::unix::io::AsRawFd;
use std::pin::Pin;

use futures::io::{IoSlice, IoSliceMut, SeekFrom};
use futures::task::{Context, Poll};
use futures::Future;
use libc::{flock, EBADF, EINTR, ENOLCK, EWOULDBLOCK, LOCK_EX, LOCK_NB, LOCK_SH, LOCK_UN};

use crate::fs::errors::LockError;

pub trait FdLock: AsRawFd + Unpin + Sized {
  /// Acquires an exclusive lock to the file descriptor.
  ///
  /// The underlying future will run on a loop until the lock is acquired.
  fn lock_exclusive(self) -> FutureFdLock<Self> {
    FutureFdLock {
      inner_fd: Some(self),
      lock_type: LOCK_EX,
    }
  }

  /// Acquires a shared lock to the file descriptor.
  ///
  /// The underlying future will run on a loop until the lock is acquired.
  fn lock_shared(self) -> FutureFdLock<Self> {
    FutureFdLock {
      inner_fd: Some(self),
      lock_type: LOCK_SH,
    }
  }
}

impl<T: AsRawFd + Unpin> FdLock for T {}

pub trait SyncFdLock: AsRawFd + Sized {
  fn try_lock_exclusive(self) -> Result<FdAdvisoryLock<Self>, LockError> {
    try_lock(&self, LOCK_SH)?;
    Ok(FdAdvisoryLock { fd: self })
  }

  fn try_lock_shared(self) -> Result<FdAdvisoryLock<Self>, LockError> {
    try_lock(&self, LOCK_SH)?;
    Ok(FdAdvisoryLock { fd: self })
  }
}

impl<T: AsRawFd> SyncFdLock for T {}

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct FutureFdLock<T: AsRawFd + Unpin> {
  inner_fd: Option<T>,
  lock_type: i32,
}

impl<T: AsRawFd + Unpin> Future for FutureFdLock<T> {
  type Output = Result<FdAdvisoryLock<T>, LockError>;

  fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
    let me = &mut *self;

    if let Some(fd) = me.inner_fd.take() {
      match try_lock(&fd, me.lock_type) {
        Ok(_) => Poll::Ready(Ok(FdAdvisoryLock { fd })),
        Err(LockError::ResourceUnavailable) => {
          me.inner_fd = Some(fd);
          cx.waker().wake_by_ref();
          Poll::Pending
        }
        Err(error) => Poll::Ready(Err(error)),
      }
    } else {
      Poll::Ready(Err(LockError::BadFileDescriptor))
    }
  }
}

pub(in crate::fs) fn try_lock<T: AsRawFd>(fd: &T, lock_type: i32) -> Result<(), LockError> {
  if unsafe { flock(fd.as_raw_fd(), lock_type | LOCK_NB) } == -1 {
    match errno::errno().0 {
      EBADF => Err(LockError::BadFileDescriptor),
      ENOLCK => Err(LockError::NoLockMemory),
      EINTR => Err(LockError::Interrupted),
      EWOULDBLOCK => Err(LockError::ResourceUnavailable),
      err => Err(LockError::Other(err)),
    }
  } else {
    Ok(())
  }
}

#[derive(Debug)]
pub struct FdAdvisoryLock<T: AsRawFd> {
  fd: T,
}

impl<T: AsRawFd> Deref for FdAdvisoryLock<T> {
  type Target = T;

  fn deref(&self) -> &Self::Target {
    &self.fd
  }
}

impl<T: AsRawFd> DerefMut for FdAdvisoryLock<T> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.fd
  }
}

impl<T: AsRawFd> Drop for FdAdvisoryLock<T> {
  fn drop(&mut self) {
    if unsafe { flock(self.fd.as_raw_fd(), LOCK_UN | LOCK_NB) } != 0 {
      panic!("Could not unlock the file descriptor");
    }
  }
}

impl<T: AsRawFd + std::io::Read> std::io::Read for FdAdvisoryLock<T> {
  fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    self.fd.read(buf)
  }

  fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> std::io::Result<usize> {
    self.fd.read_vectored(bufs)
  }

  fn read_to_end(&mut self, buf: &mut Vec<u8>) -> std::io::Result<usize> {
    self.fd.read_to_end(buf)
  }

  fn read_to_string(&mut self, buf: &mut String) -> std::io::Result<usize> {
    self.fd.read_to_string(buf)
  }

  fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
    self.fd.read_exact(buf)
  }
}

impl<T: AsRawFd + std::io::Write> std::io::Write for FdAdvisoryLock<T> {
  fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
    self.fd.write(buf)
  }

  fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
    self.fd.write_vectored(bufs)
  }

  fn flush(&mut self) -> std::io::Result<()> {
    self.fd.flush()
  }

  fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
    self.fd.write_all(buf)
  }

  fn write_fmt(&mut self, fmt: Arguments<'_>) -> std::io::Result<()> {
    self.fd.write_fmt(fmt)
  }
}

impl<T: AsRawFd + std::io::Seek> std::io::Seek for FdAdvisoryLock<T> {
  fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
    self.fd.seek(pos)
  }
}

#[cfg(test)]
mod test {
  use std::fs::File;

  use libc::{LOCK_EX, LOCK_SH};

  use crate::fs::FdLock;

  use super::{try_lock, LockError};

  #[test]
  fn should_acquire_exclusive_lock() -> std::io::Result<()> {
    let fd_a = tempfile::tempfile()?;
    let res_a = try_lock::<File>(&fd_a, LOCK_EX);

    assert!(res_a.is_ok());

    Ok(())
  }

  #[test]
  fn should_acquire_shared_lock() -> std::io::Result<()> {
    let fd_a = tempfile::tempfile()?;
    let res_a = try_lock::<File>(&fd_a, LOCK_SH);

    assert!(res_a.is_ok());

    Ok(())
  }

  #[test]
  fn should_allow_multiple_shared_locks() -> std::io::Result<()> {
    let fd_a = tempfile::NamedTempFile::new()?;
    let fd_b = File::open(fd_a.path())?;

    let res_a = try_lock::<File>(fd_a.as_file(), LOCK_SH);
    let res_b = try_lock::<File>(&fd_b, LOCK_SH);

    assert!(res_a.is_ok());
    assert!(res_b.is_ok());

    Ok(())
  }

  #[test]
  fn should_fail_when_second_exclusive_lock_acquired() -> std::io::Result<()> {
    let fd_a = tempfile::NamedTempFile::new()?;
    let fd_b = File::open(fd_a.path())?;

    let res_a = try_lock::<File>(fd_a.as_file(), LOCK_EX);
    let res_b = try_lock::<File>(&fd_b, LOCK_EX);

    assert!(res_a.is_ok());
    assert!(res_b.is_err());
    assert_eq!(res_b.unwrap_err(), LockError::ResourceUnavailable);

    Ok(())
  }

  #[test]
  fn should_fail_when_shared_lock_is_requested_after_exclusive_lock() -> std::io::Result<()> {
    let fd_a = tempfile::NamedTempFile::new()?;
    let fd_b = File::open(fd_a.path())?;

    let res_a = try_lock::<File>(fd_a.as_file(), LOCK_EX);
    let res_b = try_lock::<File>(&fd_b, LOCK_SH);

    assert!(res_a.is_ok());
    assert!(res_b.is_err());
    assert_eq!(res_b.unwrap_err(), LockError::ResourceUnavailable);

    Ok(())
  }

  #[test]
  fn should_fail_when_exclusive_lock_is_requested_after_shared_lock() -> std::io::Result<()> {
    let fd_a = tempfile::NamedTempFile::new()?;
    let fd_b = File::open(fd_a.path())?;

    let res_a = try_lock::<File>(fd_a.as_file(), LOCK_SH);
    let res_b = try_lock::<File>(&fd_b, LOCK_EX);

    assert!(res_a.is_ok());
    assert!(res_b.is_err());
    assert_eq!(res_b.unwrap_err(), LockError::ResourceUnavailable);

    Ok(())
  }

  #[tokio::test]
  async fn should_suspend_task_until_resource_is_available() {
    let fd_a = tempfile::NamedTempFile::new().unwrap();
    let fd_b = File::open(fd_a.path()).unwrap();

    let acquire_exclusive = tokio::spawn(async move {
      let lock = fd_a.lock_exclusive().await.unwrap();
      tokio::time::delay_for(std::time::Duration::from_millis(200)).await;

      drop(lock);
    });

    let acquire_shared = tokio::spawn(async move {
      tokio::time::delay_for(std::time::Duration::from_millis(100)).await;
      let lock_result = tokio::time::timeout(std::time::Duration::from_secs(1), fd_b.lock_shared()).await;

      match lock_result {
        Ok(_) => {}
        Err(_) => assert!(false),
      }
    });

    assert!(acquire_exclusive.await.is_ok());
    assert!(acquire_shared.await.is_ok());
  }
}
