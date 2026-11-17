//! macOS-specific primitives (`__ulock_wait` / `__ulock_wake`).

use core::sync::atomic::AtomicU32;
use std::io;
use std::time::Duration;

use crate::sync::SyncError;

const UL_COMPARE_AND_WAIT: u32 = 1;
const ULF_WAKE_ALL: u32 = 0x0000_0100;
const ULF_NO_ERRNO: u32 = 0x0100_0000;

// SAFETY: `__ulock_wait` and `__ulock_wake` are XNU libsystem_kernel C-ABI functions accepting
// operation flags, a 4-byte-aligned `*const u32` futex key, and scalar wait/wake parameters.
unsafe extern "C" {
    fn __ulock_wait(operation: u32, addr: *const u32, value: u64, timeout: u32) -> i32;
    fn __ulock_wake(operation: u32, addr: *const u32, wake_value: u64) -> i32;
}

pub(crate) fn wait_on_address(word: &AtomicU32, expected: u32) -> Result<(), SyncError> {
    ulock_wait(word, expected)
}

pub(crate) fn wait_on_address_timeout(
    word: &AtomicU32,
    expected: u32,
    timeout: Duration,
) -> Result<bool, SyncError> {
    if timeout.is_zero() {
        return Ok(false);
    }
    ulock_wait_timeout(word, expected, timeout)
}

pub(crate) fn wake_one(word: &AtomicU32) -> Result<(), SyncError> {
    ulock_wake(word, false)
}

pub(crate) fn wake_all(word: &AtomicU32) -> Result<(), SyncError> {
    ulock_wake(word, true)
}

fn ulock_wait(word: &AtomicU32, expected: u32) -> Result<(), SyncError> {
    let addr = word as *const AtomicU32 as *const u32;
    let op = UL_COMPARE_AND_WAIT | ULF_NO_ERRNO;
    // SAFETY: `addr` points to a valid, 4-byte-aligned `AtomicU32` borrowed for this call.
    let rc = unsafe { __ulock_wait(op, addr, expected as u64, 0) };
    if rc >= 0 {
        return Ok(());
    }
    match -rc {
        libc::EINTR | libc::EAGAIN | libc::EFAULT => Ok(()),
        errno => Err(SyncError::Syscall(io::Error::from_raw_os_error(errno))),
    }
}

fn ulock_wait_timeout(
    word: &AtomicU32,
    expected: u32,
    timeout: Duration,
) -> Result<bool, SyncError> {
    let addr = word as *const AtomicU32 as *const u32;
    let op = UL_COMPARE_AND_WAIT | ULF_NO_ERRNO;
    let micros = timeout.as_micros().min(u32::MAX as u128).max(1) as u32;
    // SAFETY: `addr` points to a valid, 4-byte-aligned `AtomicU32` borrowed for this call.
    let rc = unsafe { __ulock_wait(op, addr, expected as u64, micros) };
    if rc >= 0 {
        return Ok(true);
    }
    match -rc {
        libc::ETIMEDOUT => Ok(false),
        libc::EINTR | libc::EAGAIN | libc::EFAULT => Ok(true),
        errno => Err(SyncError::Syscall(io::Error::from_raw_os_error(errno))),
    }
}

fn ulock_wake(word: &AtomicU32, wake_all: bool) -> Result<(), SyncError> {
    let addr = word as *const AtomicU32 as *const u32;
    let mut op = UL_COMPARE_AND_WAIT | ULF_NO_ERRNO;
    if wake_all {
        op |= ULF_WAKE_ALL;
    }
    // SAFETY: `addr` points to a valid, 4-byte-aligned `AtomicU32` borrowed for this call.
    let rc = unsafe { __ulock_wake(op, addr, 0) };
    if rc >= 0 {
        return Ok(());
    }
    match -rc {
        libc::ENOENT | libc::EINTR | libc::EAGAIN => Ok(()),
        errno => Err(SyncError::Syscall(io::Error::from_raw_os_error(errno))),
    }
}
