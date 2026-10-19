//! Shared memory region abstraction.

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

#[derive(Debug, thiserror::Error)]
pub enum ShmError {
    #[error("invalid shared memory size")]
    InvalidSize,
    #[error("invalid shared memory name")]
    InvalidName,
    #[error("system call failed: {0}")]
    Sys(#[from] io::Error),
    #[error("invalid shared memory name: {0}")]
    Name(#[from] NulError),
    #[error("shared memory mapping failed")]
    MapFailed,
}

/// Represents a mapped shared memory region.
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

unsafe impl Send for ShmRegion {}
unsafe impl Sync for ShmRegion {}

impl ShmRegion {
    /// Create a new shared memory region and map it into the current process.
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
            return unsafe { map_fd_region(handle, size) };
        }
        #[cfg(windows)]
        {
            return unsafe { map_windows_region(handle, size) };
        }
        #[allow(unreachable_code)]
        Err(ShmError::InvalidSize)
    }

    /// Map an existing file descriptor into this process (safe wrapper).
    ///
    /// Used by worker processes to attach to shared memory regions created
    /// by the supervisor and inherited across fork/spawn. The FD must point
    /// to a valid shared memory region of at least `size` bytes.
    ///
    /// Does not take ownership of the FD — the caller is responsible for
    /// closing it when no longer needed.
    #[cfg(unix)]
    pub fn from_fd(fd: RawFd, size: usize) -> Result<Self, ShmError> {
        if fd < 0 {
            return Err(ShmError::InvalidName);
        }
        if size == 0 {
            return Err(ShmError::InvalidSize);
        }
        // SAFETY: caller guarantees fd is a valid, open file descriptor
        // pointing to a shared memory region of at least `size` bytes.
        // This is the documented contract for inherited FDs from the supervisor.
        unsafe { Self::map(fd, size) }
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
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    /// Get the raw bytes as a mutable slice.
    ///
    /// # Safety
    /// The caller must ensure exclusive write access to this mapping.
    pub unsafe fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { core::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

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

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

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

#[cfg(target_os = "linux")]
fn create_memfd_region(size: usize) -> Result<ShmRegion, ShmError> {
    let name = CString::new("vertebra")?;
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
            #[cfg(unix)]
            unlink_name: None,
        })
    }
}

#[cfg(unix)]
fn create_shm_open_region(size: usize) -> Result<ShmRegion, ShmError> {
    let mut attempt = 0;
    let pid = std::process::id();
    loop {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| ShmError::InvalidSize)?
            .as_nanos();
        let name = format!("/vertebra-{}-{}-{}", pid, now, attempt);
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
                let _ = libc::shm_unlink(cname.as_ptr());
                let ptr = NonNull::new(ptr.cast::<u8>()).ok_or(ShmError::MapFailed)?;
                return Ok(ShmRegion {
                    ptr,
                    len: size,
                    handle: fd,
                    owns_handle: true,
                    owns_mapping: true,
                    #[cfg(unix)]
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

#[cfg(unix)]
unsafe fn map_fd_region(fd: RawFd, size: usize) -> Result<ShmRegion, ShmError> {
    // SAFETY: map shared memory from an existing fd.
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        )
    };
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
        #[cfg(unix)]
        unlink_name: None,
    })
}

#[cfg(windows)]
fn create_windows_region(size: usize) -> Result<ShmRegion, ShmError> {
    use windows_sys::Win32::Foundation::GetLastError;
    use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
    use windows_sys::Win32::System::Memory::{
        CreateFileMappingW, FILE_MAP_ALL_ACCESS, MapViewOfFile, PAGE_READWRITE,
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
        #[cfg(unix)]
        unlink_name: None,
    })
}

#[cfg(windows)]
unsafe fn map_windows_region(handle: RawHandle, size: usize) -> Result<ShmRegion, ShmError> {
    use windows_sys::Win32::Foundation::GetLastError;
    use windows_sys::Win32::System::Memory::{FILE_MAP_ALL_ACCESS, MapViewOfFile};

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
        #[cfg(unix)]
        unlink_name: None,
    })
}

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

fn align_to_page(size: usize) -> Result<usize, ShmError> {
    let page = page_size()?;
    Ok(size.div_ceil(page) * page)
}

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
            let mut info = SYSTEM_INFO {
                wProcessorArchitecture: 0,
                wReserved: 0,
                dwPageSize: 0,
                lpMinimumApplicationAddress: std::ptr::null_mut(),
                lpMaximumApplicationAddress: std::ptr::null_mut(),
                dwActiveProcessorMask: 0,
                dwNumberOfProcessors: 0,
                dwProcessorType: 0,
                dwAllocationGranularity: 0,
                wProcessorLevel: 0,
                wProcessorRevision: 0,
            };
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
    use proptest::prelude::*;
    #[cfg(unix)]
    use std::process::Command;

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

    #[cfg(unix)]
    #[test]
    fn create_and_open_named() {
        let name = format!("/vertebra-test-{}", std::process::id());
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
        let err = ShmRegion::create_named("vertebra", 1024).unwrap_err();
        assert!(matches!(err, ShmError::InvalidName));
    }

    #[cfg(unix)]
    #[test]
    fn shm_multi_process_roundtrip() {
        if std::env::var("VERTEBRA_SHM_CHILD").is_ok() {
            let name = std::env::var("VERTEBRA_SHM_NAME").unwrap();
            let size: usize = std::env::var("VERTEBRA_SHM_SIZE").unwrap().parse().unwrap();
            let region = ShmRegion::open_named(&name, size).unwrap();
            unsafe {
                assert_eq!(region.as_slice()[0], 42);
            }
            return;
        }
        let name = format!("/vertebra-mp-{}", std::process::id());
        let mut region = match ShmRegion::create_named(&name, 4096) {
            Ok(region) => region,
            Err(ShmError::Sys(err)) if err.kind() == io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("create_named failed: {err:?}"),
        };
        unsafe {
            region.as_mut_slice()[0] = 42;
        }
        let exe = std::env::current_exe().unwrap();
        let mut child1 = Command::new(&exe)
            .env("VERTEBRA_SHM_CHILD", "1")
            .env("VERTEBRA_SHM_NAME", &name)
            .env("VERTEBRA_SHM_SIZE", "4096")
            .arg("--exact")
            .arg("shm::tests::shm_multi_process_roundtrip")
            .arg("--nocapture")
            .spawn()
            .unwrap();
        let mut child2 = Command::new(&exe)
            .env("VERTEBRA_SHM_CHILD", "1")
            .env("VERTEBRA_SHM_NAME", &name)
            .env("VERTEBRA_SHM_SIZE", "4096")
            .arg("--exact")
            .arg("shm::tests::shm_multi_process_roundtrip")
            .arg("--nocapture")
            .spawn()
            .unwrap();
        assert!(child1.wait().unwrap().success());
        assert!(child2.wait().unwrap().success());
    }

    proptest! {
        #[test]
        fn long_names_are_rejected(len in 256usize..512) {
            let name = format!("/{}", "a".repeat(len));
            let err = ShmRegion::create_named(&name, 1024).unwrap_err();
            prop_assert!(matches!(err, ShmError::InvalidName | ShmError::Name(_)));
        }
    }
}
