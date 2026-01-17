//! Linux-specific wait/wake primitives (futex).

use core::sync::atomic::AtomicU32;
use std::io;

use crate::sync::SyncError;

pub(crate) fn wait_on_address(word: &AtomicU32, expected: u32) -> Result<(), SyncError> {
    let addr = word as *const AtomicU32 as *const u32;
    let res = unsafe {
        libc::syscall(
            libc::SYS_futex,
            addr,
            libc::FUTEX_WAIT,
            expected,
            std::ptr::null::<libc::timespec>(),
        )
    } as i64;
    if res == 0 {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::EAGAIN) | Some(libc::EINTR) => Ok(()),
        _ => Err(SyncError::Syscall(err)),
    }
}

pub(crate) fn wake_one(word: &AtomicU32) -> Result<(), SyncError> {
    let addr = word as *const AtomicU32 as *const u32;
    let res = unsafe { libc::syscall(libc::SYS_futex, addr, libc::FUTEX_WAKE, 1) } as i64;
    if res >= 0 {
        Ok(())
    } else {
        Err(SyncError::Syscall(io::Error::last_os_error()))
    }
}

pub(crate) fn wake_all(word: &AtomicU32) -> Result<(), SyncError> {
    let addr = word as *const AtomicU32 as *const u32;
    let res = unsafe { libc::syscall(libc::SYS_futex, addr, libc::FUTEX_WAKE, i32::MAX) } as i64;
    if res >= 0 {
        Ok(())
    } else {
        Err(SyncError::Syscall(io::Error::last_os_error()))
    }
}
