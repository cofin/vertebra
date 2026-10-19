//! Windows-specific wait/wake primitives (WaitOnAddress).

use core::sync::atomic::AtomicU32;
use std::io;
use std::time::Duration;

use crate::sync::SyncError;
use windows_sys::Win32::Foundation::{ERROR_TIMEOUT, GetLastError};
use windows_sys::Win32::System::Threading::{WaitOnAddress, WakeByAddressAll, WakeByAddressSingle};

pub(crate) fn wait_on_address(word: &AtomicU32, expected: u32) -> Result<(), SyncError> {
    let expected_val = expected;
    let res = unsafe {
        WaitOnAddress(
            word as *const AtomicU32 as *const core::ffi::c_void,
            &expected_val as *const u32 as *const core::ffi::c_void,
            core::mem::size_of::<u32>(),
            u32::MAX,
        )
    };
    if res == 0 {
        return Err(SyncError::Syscall(io::Error::from_raw_os_error(
            unsafe { GetLastError() } as i32,
        )));
    }
    Ok(())
}

pub(crate) fn wait_on_address_timeout(
    word: &AtomicU32,
    expected: u32,
    timeout: Duration,
) -> Result<bool, SyncError> {
    if timeout.is_zero() {
        return Ok(false);
    }
    let expected_val = expected;
    let timeout_ms = timeout.as_millis().min(u32::MAX as u128) as u32;
    let res = unsafe {
        WaitOnAddress(
            word as *const AtomicU32 as *const core::ffi::c_void,
            &expected_val as *const u32 as *const core::ffi::c_void,
            core::mem::size_of::<u32>(),
            timeout_ms,
        )
    };
    if res == 0 {
        let err = unsafe { GetLastError() };
        if err == ERROR_TIMEOUT {
            return Ok(false);
        }
        return Err(SyncError::Syscall(io::Error::from_raw_os_error(err as i32)));
    }
    Ok(true)
}

pub(crate) fn wake_one(word: &AtomicU32) -> Result<(), SyncError> {
    unsafe { WakeByAddressSingle(word as *const AtomicU32 as *const core::ffi::c_void) };
    Ok(())
}

pub(crate) fn wake_all(word: &AtomicU32) -> Result<(), SyncError> {
    unsafe { WakeByAddressAll(word as *const AtomicU32 as *const core::ffi::c_void) };
    Ok(())
}
