//! Shared memory region abstraction.
//!
//! Provides cross-platform shared memory primitives:
//! - Linux: `memfd_create()` with file sealing
//! - Unix (macOS/BSD): `shm_open()` with auto-cleanup
//! - Windows: `CreateFileMappingW()`
//!
//! Ported from litecore::shm

use core::ptr::NonNull;
use std::io;

#[cfg(unix)]
use std::ffi::CString;
#[cfg(unix)]
use std::ffi::NulError;
#[cfg(unix)]
use std::os::fd::RawFd;
#[cfg(windows)]
use std::os::windows::io::RawHandle;

#[cfg(unix)]
const INVALID_HANDLE: RawFd = -1;
#[cfg(windows)]
const INVALID_HANDLE: RawHandle = std::ptr::null_mut();

#[cfg(unix)]
pub type ShmHandle = RawFd;
#[cfg(windows)]
pub type ShmHandle = RawHandle;

/// Errors that can occur during shared memory operations.
#[derive(Debug, thiserror::Error)]
pub enum ShmError {
    #[error("invalid shared memory size")]
    InvalidSize,
    #[error("invalid shared memory name")]
    InvalidName,
    #[error("system call failed: {0}")]
    Sys(#[from] io::Error),
    #[cfg(unix)]
    #[error("invalid shared memory name: {0}")]
    Name(#[from] NulError),
    #[error("shared memory mapping failed")]
    MapFailed,
}

/// Represents a mapped shared memory region.
///
/// The region is automatically unmapped and cleaned up when dropped.
#[derive(Debug)]
pub struct ShmRegion {
    ptr: NonNull<u8>,
    len: usize,
    handle: ShmHandle,
    owns_handle: bool,
    owns_mapping: bool,
    #[cfg(unix)]
    unlink_name: Option<CString>,
}

// SAFETY: ShmRegion is Send because the underlying memory is shared and
// designed for cross-process communication. The atomic operations in the
// ring buffer ensure thread-safety.
unsafe impl Send for ShmRegion {}
unsafe impl Sync for ShmRegion {}

impl ShmRegion {
    /// Create a new shared memory region and map it into the current process.
    ///
    /// The size will be rounded up to the nearest page boundary.
    pub fn create(size: usize) -> Result<Self, ShmError> {
        if size == 0 {
            return Err(ShmError::InvalidSize);
        }
        let size = align_to_page(size)?;

        #[cfg(target_os = "linux")]
        {
            if let Ok(region) = create_memfd_region(size) {
                return Ok(region);
            }
        }

        #[cfg(unix)]
        {
            return create_shm_open_region(size);
        }

        #[cfg(windows)]
        {
            return create_windows_region(size);
        }

        #[allow(unreachable_code)]
        Err(ShmError::InvalidSize)
    }

    /// Map an existing shared memory handle into this process.
    ///
    /// # Safety
    /// The caller must ensure the handle is valid and sized for `size` bytes.
    pub unsafe fn map(handle: ShmHandle, size: usize) -> Result<Self, ShmError> {
        if size == 0 {
            return Err(ShmError::InvalidSize);
        }
        let size = align_to_page(size)?;

        #[cfg(unix)]
        {
            return map_fd_region(handle, size);
        }

        #[cfg(windows)]
        {
            return map_windows_region(handle, size);
        }

        #[allow(unreachable_code)]
        Err(ShmError::InvalidSize)
    }

    /// Open an existing named region (Unix only).
    #[cfg(unix)]
    pub fn open_named(name: &str, size: usize) -> Result<Self, ShmError> {
        validate_name(name)?;
        let size = align_to_page(size)?;
        let cname = CString::new(name)?;

        // SAFETY: shm_open expects a valid C string, we validated name.
        let fd = unsafe { libc::shm_open(cname.as_ptr(), libc::O_RDWR, 0o600) };
        if fd < 0 {
            return Err(io::Error::last_os_error().into());
        }

        unsafe { map_fd_region(fd, size) }
    }

    /// Create a named shared memory region (Unix only).
    ///
    /// Uses unlink-before-create strategy: any existing region with the same name
    /// is unlinked first, then a new one is created with O_EXCL for atomicity.
    /// This handles stale files from previous crashed runs.
    #[cfg(unix)]
    pub fn create_named(name: &str, size: usize) -> Result<Self, ShmError> {
        validate_name(name)?;
        let size = align_to_page(size)?;
        let cname = CString::new(name)?;

        // Unlink any existing region first (ignore ENOENT if it doesn't exist).
        // This handles stale files from crashed previous runs.
        unsafe {
            let result = libc::shm_unlink(cname.as_ptr());
            if result < 0 {
                let err = io::Error::last_os_error();
                // ENOENT is expected if the file doesn't exist
                if err.raw_os_error() != Some(libc::ENOENT) {
                    // Log but don't fail - we'll try to create anyway
                }
            }
        }

        // SAFETY: shm_open expects a valid C string, we validated name.
        // Use O_EXCL to ensure atomic creation (race-safe after unlink).
        let fd = unsafe {
            libc::shm_open(
                cname.as_ptr(),
                libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
                0o600,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error().into());
        }

        unsafe {
            if libc::ftruncate(fd, size as libc::off_t) != 0 {
                let err = io::Error::last_os_error();
                libc::close(fd);
                let _ = libc::shm_unlink(cname.as_ptr());
                return Err(err.into());
            }
        }

        // SAFETY: map the region for read/write.
        let mut region = unsafe { map_fd_region(fd, size) }?;
        region.owns_handle = true;
        region.owns_mapping = true;
        region.unlink_name = Some(cname);
        Ok(region)
    }

    /// Get the raw bytes as an immutable slice.
    ///
    /// # Safety
    /// The caller must ensure no mutable aliasing occurs across processes.
    pub unsafe fn as_slice(&self) -> &[u8] {
        core::slice::from_raw_parts(self.ptr.as_ptr(), self.len)
    }

    /// Get the raw bytes as a mutable slice.
    ///
    /// # Safety
    /// The caller must ensure exclusive write access to this mapping.
    pub unsafe fn as_mut_slice(&mut self) -> &mut [u8] {
        core::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len)
    }

    /// Get the size of the mapped region in bytes.
    pub fn size(&self) -> usize {
        self.len
    }

    /// Create a ShmRegion from a raw pointer for tests.
    ///
    /// # Safety
    /// Caller must ensure `ptr` is valid for `len` bytes.
    pub unsafe fn from_raw(ptr: *mut u8, len: usize) -> Self {
        let ptr = NonNull::new(ptr).expect("ptr must be non-null");
        Self {
            ptr,
            len,
            handle: INVALID_HANDLE,
            owns_handle: false,
            owns_mapping: false,
            #[cfg(unix)]
            unlink_name: None,
        }
    }

    /// Get the length of the mapped region.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if the region is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Get the raw pointer to the mapped memory.
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Get the underlying handle (file descriptor on Unix, HANDLE on Windows).
    pub fn handle(&self) -> Option<ShmHandle> {
        if self.handle == INVALID_HANDLE {
            None
        } else {
            Some(self.handle)
        }
    }
}

impl Drop for ShmRegion {
    fn drop(&mut self) {
        unsafe {
            #[cfg(unix)]
            {
                if self.owns_mapping {
                    libc::munmap(self.ptr.as_ptr().cast(), self.len);
                }
                if self.owns_handle && self.handle != INVALID_HANDLE {
                    libc::close(self.handle);
                }
                if let Some(name) = self.unlink_name.take() {
                    let _ = libc::shm_unlink(name.as_ptr());
                }
            }
            #[cfg(windows)]
            {
                use windows_sys::Win32::Foundation::CloseHandle;
                use windows_sys::Win32::System::Memory::UnmapViewOfFile;

                if self.owns_mapping {
                    UnmapViewOfFile(self.ptr.as_ptr().cast());
                }
                if self.owns_handle && !self.handle.is_null() {
                    CloseHandle(self.handle);
                }
            }
        }
    }
}

/// Create a shared memory region using Linux's memfd_create.
///
/// memfd_create provides anonymous memory with file sealing support,
/// which prevents the region from being resized after initialization.
#[cfg(target_os = "linux")]
fn create_memfd_region(size: usize) -> Result<ShmRegion, ShmError> {
    let name = CString::new("litecore")?;

    let fd = unsafe {
        libc::syscall(
            libc::SYS_memfd_create,
            name.as_ptr(),
            libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
        )
    } as RawFd;

    if fd < 0 {
        return Err(io::Error::last_os_error().into());
    }

    // SAFETY: syscall expects valid C string pointer and flags.
    unsafe {
        if libc::ftruncate(fd, size as libc::off_t) != 0 {
            let err = io::Error::last_os_error();
            libc::close(fd);
            return Err(err.into());
        }

        // Seal the region to prevent size changes (security)
        let seals = libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_SEAL;
        let _ = libc::fcntl(fd, libc::F_ADD_SEALS, seals);

        // SAFETY: mapping shared memory of `size` bytes from fd.
        let ptr = libc::mmap(
            std::ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        );

        if ptr == libc::MAP_FAILED {
            let err = io::Error::last_os_error();
            libc::close(fd);
            return Err(err.into());
        }

        let ptr = NonNull::new(ptr.cast::<u8>()).ok_or(ShmError::MapFailed)?;

        Ok(ShmRegion {
            ptr,
            len: size,
            handle: fd,
            owns_handle: true,
            owns_mapping: true,
            unlink_name: None,
        })
    }
}

/// Create a shared memory region using POSIX shm_open.
///
/// Used on macOS/BSD and as fallback on Linux if memfd_create fails.
#[cfg(unix)]
fn create_shm_open_region(size: usize) -> Result<ShmRegion, ShmError> {
    let mut attempt = 0;
    let pid = std::process::id();

    loop {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| ShmError::InvalidSize)?
            .as_nanos();
        let name = format!("/litecore-{}-{}-{}", pid, now, attempt);
        let cname = CString::new(name)?;

        // SAFETY: shm_open expects a valid C string name.
        let fd = unsafe {
            libc::shm_open(
                cname.as_ptr(),
                libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
                0o600,
            )
        };

        if fd >= 0 {
            unsafe {
                if libc::ftruncate(fd, size as libc::off_t) != 0 {
                    let err = io::Error::last_os_error();
                    libc::close(fd);
                    let _ = libc::shm_unlink(cname.as_ptr());
                    return Err(err.into());
                }

                // SAFETY: mapping shared memory of `size` bytes from fd.
                let ptr = libc::mmap(
                    std::ptr::null_mut(),
                    size,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_SHARED,
                    fd,
                    0,
                );

                if ptr == libc::MAP_FAILED {
                    let err = io::Error::last_os_error();
                    libc::close(fd);
                    let _ = libc::shm_unlink(cname.as_ptr());
                    return Err(err.into());
                }

                // Immediately unlink to ensure cleanup on process exit
                let _ = libc::shm_unlink(cname.as_ptr());

                let ptr = NonNull::new(ptr.cast::<u8>()).ok_or(ShmError::MapFailed)?;

                return Ok(ShmRegion {
                    ptr,
                    len: size,
                    handle: fd,
                    owns_handle: true,
                    owns_mapping: true,
                    unlink_name: None,
                });
            }
        }

        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EEXIST) {
            attempt += 1;
            continue;
        }
        return Err(err.into());
    }
}

/// Map an existing file descriptor as shared memory.
#[cfg(unix)]
unsafe fn map_fd_region(fd: RawFd, size: usize) -> Result<ShmRegion, ShmError> {
    // SAFETY: map shared memory from an existing fd.
    let ptr = libc::mmap(
        std::ptr::null_mut(),
        size,
        libc::PROT_READ | libc::PROT_WRITE,
        libc::MAP_SHARED,
        fd,
        0,
    );

    if ptr == libc::MAP_FAILED {
        return Err(io::Error::last_os_error().into());
    }

    let ptr = NonNull::new(ptr.cast::<u8>()).ok_or(ShmError::MapFailed)?;

    Ok(ShmRegion {
        ptr,
        len: size,
        handle: fd,
        owns_handle: true,
        owns_mapping: true,
        unlink_name: None,
    })
}

/// Create a shared memory region on Windows using CreateFileMappingW.
#[cfg(windows)]
fn create_windows_region(size: usize) -> Result<ShmRegion, ShmError> {
    use windows_sys::Win32::Foundation::GetLastError;
    use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
    use windows_sys::Win32::System::Memory::{
        CreateFileMappingW, MapViewOfFile, FILE_MAP_ALL_ACCESS, PAGE_READWRITE,
    };

    let size_u64 = size as u64;
    let handle = unsafe {
        CreateFileMappingW(
            INVALID_HANDLE_VALUE,
            std::ptr::null_mut(),
            PAGE_READWRITE,
            (size_u64 >> 32) as u32,
            (size_u64 & 0xFFFF_FFFF) as u32,
            std::ptr::null(),
        )
    };

    if handle.is_null() {
        return Err(io::Error::from_raw_os_error(unsafe { GetLastError() } as i32).into());
    }

    let ptr = unsafe { MapViewOfFile(handle, FILE_MAP_ALL_ACCESS, 0, 0, size) } as *mut u8;
    if ptr.is_null() {
        return Err(io::Error::from_raw_os_error(unsafe { GetLastError() } as i32).into());
    }

    let ptr = NonNull::new(ptr).ok_or(ShmError::MapFailed)?;

    Ok(ShmRegion {
        ptr,
        len: size,
        handle,
        owns_handle: true,
        owns_mapping: true,
    })
}

/// Map an existing Windows handle as shared memory.
#[cfg(windows)]
unsafe fn map_windows_region(handle: RawHandle, size: usize) -> Result<ShmRegion, ShmError> {
    use windows_sys::Win32::Foundation::GetLastError;
    use windows_sys::Win32::System::Memory::{MapViewOfFile, FILE_MAP_ALL_ACCESS};

    let ptr = MapViewOfFile(handle, FILE_MAP_ALL_ACCESS, 0, 0, size) as *mut u8;
    if ptr.is_null() {
        return Err(io::Error::from_raw_os_error(GetLastError() as i32).into());
    }

    let ptr = NonNull::new(ptr).ok_or(ShmError::MapFailed)?;

    Ok(ShmRegion {
        ptr,
        len: size,
        handle,
        owns_handle: true,
        owns_mapping: true,
    })
}

/// Validate a shared memory name (must start with '/' and be <= 255 chars).
#[cfg(unix)]
fn validate_name(name: &str) -> Result<(), ShmError> {
    if !name.starts_with('/') {
        return Err(ShmError::InvalidName);
    }
    if name.len() > 255 {
        return Err(ShmError::InvalidName);
    }
    Ok(())
}

/// Unlink a named shared memory region (Unix only).
///
/// This removes the name from the filesystem so no new processes can open it.
/// Existing mappings remain valid until all processes unmap.
#[cfg(unix)]
pub fn shm_unlink_named(name: &str) -> Result<(), ShmError> {
    validate_name(name)?;
    let cname = CString::new(name)?;

    // SAFETY: shm_unlink expects a valid C string name.
    let result = unsafe { libc::shm_unlink(cname.as_ptr()) };
    if result < 0 {
        return Err(io::Error::last_os_error().into());
    }
    Ok(())
}

/// Round size up to the nearest page boundary.
fn align_to_page(size: usize) -> Result<usize, ShmError> {
    let page = page_size()?;
    Ok(size.div_ceil(page) * page)
}

/// Get the system page size.
fn page_size() -> Result<usize, ShmError> {
    #[cfg(unix)]
    unsafe {
        let size = libc::sysconf(libc::_SC_PAGESIZE);
        if size <= 0 {
            return Err(ShmError::InvalidSize);
        }
        Ok(size as usize)
    }

    #[cfg(windows)]
    {
        use windows_sys::Win32::System::SystemInformation::{GetSystemInfo, SYSTEM_INFO};
        unsafe {
            let mut info = std::mem::zeroed::<SYSTEM_INFO>();
            GetSystemInfo(&mut info);
            if info.dwPageSize == 0 {
                return Err(ShmError::InvalidSize);
            }
            Ok(info.dwPageSize as usize)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_region_and_write() {
        let mut region = ShmRegion::create(1024).unwrap();
        assert!(region.len() >= 1024);
        unsafe {
            let slice = region.as_mut_slice();
            slice[0] = 42;
            assert_eq!(region.as_slice()[0], 42);
        }
    }

    #[test]
    fn region_size_aligned_to_page() {
        let region = ShmRegion::create(1).unwrap();
        // Size should be at least one page
        assert!(region.len() >= 4096);
    }

    #[test]
    fn zero_size_rejected() {
        assert!(matches!(ShmRegion::create(0), Err(ShmError::InvalidSize)));
    }

    #[cfg(unix)]
    #[test]
    fn create_and_open_named() {
        let name = format!("/litecore-test-{}", std::process::id());

        let mut region = match ShmRegion::create_named(&name, 4096) {
            Ok(region) => region,
            Err(ShmError::Sys(err)) if err.kind() == io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("create_named failed: {err:?}"),
        };

        unsafe {
            region.as_mut_slice()[0] = 7;
        }

        let opened = ShmRegion::open_named(&name, 4096).unwrap();
        unsafe {
            assert_eq!(opened.as_slice()[0], 7);
        }

        drop(region);
        drop(opened);
    }

    #[cfg(unix)]
    #[test]
    fn invalid_name_rejected() {
        let err = ShmRegion::create_named("litecore", 1024).unwrap_err();
        assert!(matches!(err, ShmError::InvalidName));
    }
}
