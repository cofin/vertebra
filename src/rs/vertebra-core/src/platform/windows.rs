//! Windows-specific wait/wake primitives (WaitOnAddress).

use core::sync::atomic::AtomicU32;
use std::io;

use crate::sync::SyncError;
use windows_sys::Win32::Foundation::GetLastError;
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

pub(crate) fn wake_one(word: &AtomicU32) -> Result<(), SyncError> {
    unsafe { WakeByAddressSingle(word as *const AtomicU32 as *const core::ffi::c_void) };
    Ok(())
}

pub(crate) fn wake_all(word: &AtomicU32) -> Result<(), SyncError> {
    unsafe { WakeByAddressAll(word as *const AtomicU32 as *const core::ffi::c_void) };
    Ok(())
}
