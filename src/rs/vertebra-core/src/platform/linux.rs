//! Linux-specific wait/wake primitives (futex) and process management (pidfd).

use core::sync::atomic::AtomicU32;
use std::io;
use std::os::fd::{FromRawFd, OwnedFd};
use std::time::Duration;

use crate::sync::SyncError;

/// Open a process file descriptor for monitoring process lifetime.
///
/// Returns a pollable FD that becomes readable when the process exits.
/// Available on Linux 5.3+ (kernel 5.3 added `pidfd_open(2)`).
///
/// Register the returned FD with epoll/`AsyncFd` to detect worker death
/// without polling or signal handlers.
pub fn pidfd_open(pid: i32) -> io::Result<OwnedFd> {
    // SYS_pidfd_open = 434 on x86_64, 434 on aarch64
    // SAFETY: `SYS_pidfd_open` accepts scalar `(pid, 0)` arguments with no pointer dereferences.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) } as i32;
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `pidfd_open` returned `fd >= 0`, transferring exclusive ownership of a valid FD.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

pub(crate) fn wait_on_address(word: &AtomicU32, expected: u32) -> Result<(), SyncError> {
    let addr = word as *const AtomicU32 as *const u32;
    // SAFETY: `addr` points to a valid, 4-byte-aligned `AtomicU32` borrowed for this call,
    // and a null `timespec` requests an unbounded `FUTEX_WAIT`.
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

pub(crate) fn wait_on_address_timeout(
    word: &AtomicU32,
    expected: u32,
    timeout: Duration,
) -> Result<bool, SyncError> {
    if timeout.is_zero() {
        return Ok(false);
    }
    let addr = word as *const AtomicU32 as *const u32;
    let secs = timeout.as_secs().min(i64::MAX as u64) as i64;
    let ts = libc::timespec {
        tv_sec: secs as libc::time_t,
        tv_nsec: timeout.subsec_nanos() as libc::c_long,
    };
    // SAFETY: `addr` points to a valid, 4-byte-aligned `AtomicU32` and `&ts` points to an
    // initialized stack `timespec` valid for the duration of the syscall.
    let res = unsafe {
        libc::syscall(
            libc::SYS_futex,
            addr,
            libc::FUTEX_WAIT,
            expected,
            &ts as *const libc::timespec,
        )
    } as i64;
    if res == 0 {
        return Ok(true);
    }
    let err = io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::ETIMEDOUT) => Ok(false),
        Some(libc::EAGAIN) | Some(libc::EINTR) => Ok(true),
        _ => Err(SyncError::Syscall(err)),
    }
}

pub(crate) fn wake_one(word: &AtomicU32) -> Result<(), SyncError> {
    let addr = word as *const AtomicU32 as *const u32;
    // SAFETY: `addr` points to a valid, 4-byte-aligned `AtomicU32` borrowed for this call.
    let res = unsafe { libc::syscall(libc::SYS_futex, addr, libc::FUTEX_WAKE, 1) } as i64;
    if res >= 0 {
        Ok(())
    } else {
        Err(SyncError::Syscall(io::Error::last_os_error()))
    }
}

pub(crate) fn wake_all(word: &AtomicU32) -> Result<(), SyncError> {
    let addr = word as *const AtomicU32 as *const u32;
    // SAFETY: `addr` points to a valid, 4-byte-aligned `AtomicU32` borrowed for this call.
    let res = unsafe { libc::syscall(libc::SYS_futex, addr, libc::FUTEX_WAKE, i32::MAX) } as i64;
    if res >= 0 {
        Ok(())
    } else {
        Err(SyncError::Syscall(io::Error::last_os_error()))
    }
}
