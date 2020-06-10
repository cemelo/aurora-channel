use lazy_static::lazy_static;

lazy_static! {
  static ref CLWB_AVAILABLE: bool = unsafe { has_clwb() };
  static ref CLFLUSHOPT_AVAILABLE: bool = unsafe { has_clflushopt() };
}

/// Writes back to memory the cache line (if modified) that contains the linear address specified.
///
/// If the [`CLWB`](CLWB) instruction is not available, this function falls back
/// to [`CLFLUSHOPT`](CLFLUSHOPT) and [`CLFLUSH`](CLFLUSH) respectively.
///
/// The main difference between `CLWB` and `CLFLUSHOPT` is that, while both instructions flush the
/// target cache line (at all levels) into NVM, the latter also evicts the cache line, which might
/// cause undesirable cache misses.
///
/// [CLWB]: https://www.felixcloutier.com/x86/clwb
/// [CLFLUSHOPT]: https://www.felixcloutier.com/x86/clflushopt
/// [CLFLUSH]: https://www.felixcloutier.com/x86/clflush
pub(crate) unsafe fn clwb<T>(address: *const T) {
  if *CLWB_AVAILABLE {
    llvm_asm!(
      "clwb $0"
      :
      : "m"(address)
      :
      : "volatile"
    );
  } else if *CLFLUSHOPT_AVAILABLE {
    llvm_asm!(
      "clflushopt $0"
      :
      : "m"(address)
      :
      : "volatile"
    );
  } else {
    llvm_asm!(
      "clflush $0"
      :
      : "m"(address)
      :
      : "volatile"
    );
  }
}

unsafe fn has_clwb() -> bool {
  if core::arch::x86_64::has_cpuid() {
    (core::arch::x86_64::__cpuid_count(7, 0).ebx >> 23) & 0b1 == 0
  } else {
    false
  }
}

unsafe fn has_clflushopt() -> bool {
  if core::arch::x86_64::has_cpuid() {
    (core::arch::x86_64::__cpuid_count(7, 0).ebx >> 22) & 0b1 == 0
  } else {
    false
  }
}

#[cfg(test)]
mod tests {
  use std::error::Error;
  use std::sync::atomic::{AtomicPtr, Ordering};

  use super::clwb;

  #[test]
  fn test_has_clwb() -> Result<(), Box<dyn Error>> {
    let boxed_number = Box::new(0u64);
    let boxed_ptr = Box::into_raw(boxed_number);

    unsafe { clwb(boxed_ptr) };
    unsafe { Box::from_raw(boxed_ptr) };

    Ok(())
  }
}
