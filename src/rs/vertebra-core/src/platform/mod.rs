//! Platform-specific shared memory and wait primitives.

#[cfg(target_os = "linux")]
pub mod linux;
#[cfg(target_os = "macos")]
pub mod macos;
#[cfg(windows)]
pub mod windows;

#[cfg(target_os = "linux")]
pub(crate) use linux::{wait_on_address, wake_all, wake_one};
#[cfg(target_os = "macos")]
pub(crate) use macos::{wait_on_address, wake_all, wake_one};
#[cfg(windows)]
pub(crate) use windows::{wait_on_address, wake_all, wake_one};

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub(crate) fn wait_on_address(
    _word: &core::sync::atomic::AtomicU32,
    _expected: u32,
) -> Result<(), crate::sync::SyncError> {
    Err(crate::sync::SyncError::Unsupported)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub(crate) fn wake_one(
    _word: &core::sync::atomic::AtomicU32,
) -> Result<(), crate::sync::SyncError> {
    Err(crate::sync::SyncError::Unsupported)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub(crate) fn wake_all(
    _word: &core::sync::atomic::AtomicU32,
) -> Result<(), crate::sync::SyncError> {
    Err(crate::sync::SyncError::Unsupported)
}
