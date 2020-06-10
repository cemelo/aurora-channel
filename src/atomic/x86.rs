
pub(in crate::atomic) unsafe fn compare_and_swap(address: *mut usize, old_value: usize, new_value: usize) -> usize {
  llvm_asm!(
    ""
  )
}