//! macOS-specific primitives (shm_open, ulock/pthread).
//!
//! The `macos-ulock` feature enables the ulock fast-path; default is a
//! spin + nanosleep fallback until a pthread shared-cond implementation lands.

use core::sync::atomic::{AtomicU32, Ordering};
#[cfg(feature = "macos-ulock")]
use std::io;
use std::time::{Duration, Instant};

use crate::sync::SyncError;

#[cfg(not(feature = "macos-ulock"))]
const SPIN_ITERS: usize = 1024;
#[cfg(not(feature = "macos-ulock"))]
const SLEEP_NS: i64 = 1_000_000; // 1ms

pub(crate) fn wait_on_address(word: &AtomicU32, expected: u32) -> Result<(), SyncError> {
    #[cfg(feature = "macos-ulock")]
    {
        ulock_wait(word, expected)
    }
    #[cfg(not(feature = "macos-ulock"))]
    {
        spin_sleep_wait(word, expected)
    }
}

pub(crate) fn wait_on_address_timeout(
    word: &AtomicU32,
    expected: u32,
    timeout: Duration,
) -> Result<bool, SyncError> {
    if timeout.is_zero() {
        return Ok(false);
    }
    #[cfg(feature = "macos-ulock")]
    {
        ulock_wait_timeout(word, expected, timeout)
    }
    #[cfg(not(feature = "macos-ulock"))]
    {
        spin_sleep_wait_timeout(word, expected, timeout)
    }
}

pub(crate) fn wake_one(word: &AtomicU32) -> Result<(), SyncError> {
    #[cfg(feature = "macos-ulock")]
    {
        ulock_wake(word, false)
    }
    #[cfg(not(feature = "macos-ulock"))]
    {
        let _ = word.load(Ordering::Acquire);
        Ok(())
    }
}

pub(crate) fn wake_all(word: &AtomicU32) -> Result<(), SyncError> {
    #[cfg(feature = "macos-ulock")]
    {
        ulock_wake(word, true)
    }
    #[cfg(not(feature = "macos-ulock"))]
    {
        let _ = word.load(Ordering::Acquire);
        Ok(())
    }
}

#[cfg(not(feature = "macos-ulock"))]
fn spin_sleep_wait(word: &AtomicU32, expected: u32) -> Result<(), SyncError> {
    let mut spins = 0usize;
    loop {
        if word.load(Ordering::Acquire) != expected {
            return Ok(());
        }
        if spins < SPIN_ITERS {
            spins += 1;
            std::hint::spin_loop();
            continue;
        }
        let ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: SLEEP_NS,
        };
        unsafe {
            libc::nanosleep(&ts, std::ptr::null_mut());
        }
    }
}

#[cfg(not(feature = "macos-ulock"))]
fn spin_sleep_wait_timeout(
    word: &AtomicU32,
    expected: u32,
    timeout: Duration,
) -> Result<bool, SyncError> {
    let deadline = Instant::now() + timeout;
    let mut spins = 0usize;
    loop {
        if word.load(Ordering::Acquire) != expected {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        if spins < SPIN_ITERS {
            spins += 1;
            std::hint::spin_loop();
            continue;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        let sleep_ns = (remaining.as_nanos().min(SLEEP_NS as u128)) as i64;
        let ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: sleep_ns.max(0),
        };
        unsafe {
            libc::nanosleep(&ts, std::ptr::null_mut());
        }
    }
}

#[cfg(feature = "macos-ulock")]
const UL_COMPARE_AND_WAIT: u32 = 1;
#[cfg(feature = "macos-ulock")]
const ULF_WAKE_ALL: u32 = 0x0000_0100;
#[cfg(feature = "macos-ulock")]
const ULF_NO_ERRNO: u32 = 0x0100_0000;

#[cfg(feature = "macos-ulock")]
unsafe extern "C" {
    fn __ulock_wait(operation: u32, addr: *const u32, value: u64, timeout: u32) -> i32;
    fn __ulock_wake(operation: u32, addr: *const u32, wake_value: u64) -> i32;
}

#[cfg(feature = "macos-ulock")]
fn ulock_wait(word: &AtomicU32, expected: u32) -> Result<(), SyncError> {
    let addr = word as *const AtomicU32 as *const u32;
    let op = UL_COMPARE_AND_WAIT | ULF_NO_ERRNO;
    let rc = unsafe { __ulock_wait(op, addr, expected as u64, 0) };
    if rc >= 0 {
        return Ok(());
    }
    match -rc {
        libc::EINTR | libc::EAGAIN | libc::EFAULT => Ok(()),
        errno => Err(SyncError::Syscall(io::Error::from_raw_os_error(errno))),
    }
}

#[cfg(feature = "macos-ulock")]
fn ulock_wait_timeout(
    word: &AtomicU32,
    expected: u32,
    timeout: Duration,
) -> Result<bool, SyncError> {
    let addr = word as *const AtomicU32 as *const u32;
    let op = UL_COMPARE_AND_WAIT | ULF_NO_ERRNO;
    let micros = timeout.as_micros().min(u32::MAX as u128).max(1) as u32;
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

#[cfg(feature = "macos-ulock")]
fn ulock_wake(word: &AtomicU32, wake_all: bool) -> Result<(), SyncError> {
    let addr = word as *const AtomicU32 as *const u32;
    let mut op = UL_COMPARE_AND_WAIT | ULF_NO_ERRNO;
    if wake_all {
        op |= ULF_WAKE_ALL;
    }
    let rc = unsafe { __ulock_wake(op, addr, 0) };
    if rc >= 0 {
        return Ok(());
    }
    match -rc {
        libc::ENOENT | libc::EINTR | libc::EAGAIN => Ok(()),
        errno => Err(SyncError::Syscall(io::Error::from_raw_os_error(errno))),
    }
}
