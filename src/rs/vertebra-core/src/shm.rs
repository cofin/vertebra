//! Shared memory region abstraction.

use core::ptr::NonNull;
use std::ffi::{NulError, c_void};
use std::io;
use std::sync::Arc;
#[cfg(unix)]
use std::sync::Mutex;
use std::sync::atomic::AtomicPtr;

#[cfg(unix)]
use std::ffi::CString;
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
    unlink_name: Mutex<Option<CString>>,
}

// SAFETY: `ShmRegion` owns an OS virtual-memory mapping (`mmap`/`MapViewOfFile`) that can be
// safely transferred and shared across threads when callers synchronize interior writes.
unsafe impl Send for ShmRegion {}
// SAFETY: `ShmRegion` exposes mutable access only through `unsafe fn as_mut_slice` and guards
// `unlink_name` behind a `Mutex`, so concurrent shared references cannot cause data races.
unsafe impl Sync for ShmRegion {}

/// C-ABI shared-memory ownership capsule for zero-copy cross-extension handoff.
#[repr(C)]
#[derive(Debug)]
pub struct ShmCapsule {
    pub ptr: *mut u8,
    pub len: usize,
    pub ctx: AtomicPtr<c_void>,
    pub drop_fn: unsafe extern "C" fn(*mut c_void),
    pub unlink_fn: Option<unsafe extern "C" fn(*mut c_void) -> bool>,
}

// SAFETY: `ShmCapsule` transfers ownership of a thread-safe `Arc<ShmRegion>` mapping (`Send + Sync`).
unsafe impl Send for ShmCapsule {}
// SAFETY: `ShmCapsule` synchronizes ownership claims via `AtomicPtr` (`ctx`) without unsynchronized interior mutation.
unsafe impl Sync for ShmCapsule {}

unsafe extern "C" fn drop_shm_region_arc(ctx: *mut c_void) {
    if !ctx.is_null() {
        // SAFETY: `ctx` was produced via `Arc::into_raw` on an `Arc<ShmRegion>` in
        // `ShmCapsule::from_arc` and ownership is surrendered at most once to `drop_fn`.
        unsafe {
            drop(Arc::from_raw(ctx.cast::<ShmRegion>()));
        }
    }
}

unsafe extern "C" fn unlink_shm_region_arc(ctx: *mut c_void) -> bool {
    if ctx.is_null() {
        return false;
    }
    #[cfg(unix)]
    {
        // SAFETY: `ctx` points to a live `ShmRegion` inside an `Arc<ShmRegion>` whose strong
        // count remains non-zero while the caller holds ownership of `ctx`.
        let region = unsafe { &*(ctx as *const ShmRegion) };
        region.unlink_now()
    }
    #[cfg(not(unix))]
    {
        false
    }
}

impl ShmCapsule {
    /// Wrap an [`Arc<ShmRegion>`] into a C-ABI [`ShmCapsule`] whose `drop_fn` releases the `Arc`
    /// and whose `unlink_fn` invokes [`ShmRegion::unlink_now`] once.
    pub fn from_arc(region: Arc<ShmRegion>) -> Self {
        let ptr = region.as_ptr();
        let len = region.len();
        let ctx = AtomicPtr::new(Arc::into_raw(region) as *mut c_void);
        Self {
            ptr,
            len,
            ctx,
            drop_fn: drop_shm_region_arc,
            unlink_fn: Some(unlink_shm_region_arc),
        }
    }
}

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

    /// Map an existing shared memory file descriptor / handle into this process.
    pub fn from_fd(handle: ShmHandle, size: usize) -> Result<Self, ShmError> {
        unsafe { Self::map(handle, size) }
    }

    /// Open an existing named region (Unix only).
    #[cfg(unix)]
    pub fn open_named(name: &str, size: usize) -> Result<Self, ShmError> {
        if size == 0 {
            return Err(ShmError::InvalidSize);
        }
        validate_name(name)?;
        let size = align_to_page(size)?;
        let cname = CString::new(name)?;
        // SAFETY: `cname` is a validated NUL-terminated `CString`.
        let fd = if name.starts_with("/tmp/vtb-") {
            unsafe { libc::open(cname.as_ptr(), libc::O_RDWR, 0o600) }
        } else {
            unsafe { libc::shm_open(cname.as_ptr(), libc::O_RDWR, 0o600) }
        };
        if fd < 0 {
            return Err(io::Error::last_os_error().into());
        }
        // SAFETY: `libc::stat` is plain-old-data valid when zero-initialized, and `fd` is owned here.
        unsafe {
            let mut st: libc::stat = core::mem::zeroed();
            if libc::fstat(fd, &mut st) != 0 {
                let err = io::Error::last_os_error();
                libc::close(fd);
                return Err(err.into());
            }
            if st.st_size < 0 || (size as u64) > (st.st_size as u64) {
                libc::close(fd);
                return Err(ShmError::InvalidSize);
            }
        }
        // SAFETY: `fd` is an open shared-memory descriptor backed by at least `size` page-aligned bytes.
        match unsafe { map_fd_region(fd, size) } {
            Ok(region) => Ok(region),
            Err(err) => {
                // SAFETY: `fd` was opened above and is closed on map failure.
                unsafe {
                    libc::close(fd);
                }
                Err(err)
            }
        }
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
        let mut region = match unsafe { map_fd_region(fd, size) } {
            Ok(region) => region,
            Err(err) => {
                unsafe {
                    libc::close(fd);
                    let _ = libc::shm_unlink(cname.as_ptr());
                }
                return Err(err);
            }
        };
        region.owns_handle = true;
        region.owns_mapping = true;
        *region.unlink_name.get_mut().expect("unlink_name mutex") = Some(cname);
        Ok(region)
    }

    /// Disarm automatic `shm_unlink` on drop so a peer process can open the named region.
    #[cfg(unix)]
    pub fn disarm_unlink(&mut self) {
        *self
            .unlink_name
            .get_mut()
            .unwrap_or_else(|e| e.into_inner()) = None;
    }

    /// Unlink the named POSIX `/dev/shm` segment or `/tmp/vtb-*` backing file immediately
    /// while keeping the virtual-memory mapping valid until [`ShmRegion`] drops.
    #[cfg(unix)]
    pub fn unlink_now(&self) -> bool {
        let name = self
            .unlink_name
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        let Some(name) = name else {
            return false;
        };
        // SAFETY: `name` is a valid NUL-terminated `CString` created during `create_named`
        // or `create_file_backed`.
        let rc = unsafe {
            if name.as_bytes().starts_with(b"/tmp/vtb-") {
                libc::unlink(name.as_ptr())
            } else {
                libc::shm_unlink(name.as_ptr())
            }
        };
        rc == 0
    }

    /// Create a file-backed shared memory region under `/tmp/vtb-*` (Unix only).
    #[cfg(unix)]
    pub fn create_file_backed(path: &str, size: usize) -> Result<Self, ShmError> {
        if !path.starts_with("/tmp/vtb-") || path.len() > 255 {
            return Err(ShmError::InvalidName);
        }
        let size = align_to_page(size)?;
        let cname = CString::new(path)?;
        // SAFETY: `cname` is a valid NUL-terminated `CString`.
        unsafe {
            let _ = libc::unlink(cname.as_ptr());
        }
        // SAFETY: `cname` is a valid NUL-terminated `CString` and flags create the file atomically.
        let fd = unsafe {
            libc::open(
                cname.as_ptr(),
                libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
                0o600,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error().into());
        }
        // SAFETY: `fd` is a valid open file descriptor owned by this function.
        unsafe {
            if libc::ftruncate(fd, size as libc::off_t) != 0 {
                let err = io::Error::last_os_error();
                libc::close(fd);
                let _ = libc::unlink(cname.as_ptr());
                return Err(err.into());
            }
        }
        // SAFETY: `fd` has been truncated to `size` bytes.
        let mut region = match unsafe { map_fd_region(fd, size) } {
            Ok(region) => region,
            Err(err) => {
                // SAFETY: `fd` and `cname` are valid and mapping failed, so clean up both.
                unsafe {
                    libc::close(fd);
                    let _ = libc::unlink(cname.as_ptr());
                }
                return Err(err);
            }
        };
        region.owns_handle = true;
        region.owns_mapping = true;
        *region.unlink_name.get_mut().expect("unlink_name mutex") = Some(cname);
        Ok(region)
    }

    /// Create a uniquely named one-shot overflow shared memory region with unlink disarmed.
    ///
    /// Automatically falls back to `/tmp/vtb-*` when `/dev/shm` free space is constrained
    /// (`< size + 8 MiB`) or `VERTEBRA_OVERFLOW_BACKEND=tmp` is set.
    /// The consumer is responsible for calling [`shm_unlink_named`] after opening the region.
    #[cfg(unix)]
    pub fn create_overflow_region(prefix: &str, size: usize) -> Result<(String, Self), ShmError> {
        use core::sync::atomic::{AtomicU64, Ordering};
        static OVERFLOW_SEQ: AtomicU64 = AtomicU64::new(0);

        let pid = std::process::id();
        let seq = OVERFLOW_SEQ.fetch_add(1, Ordering::Relaxed);
        let prefer_tmp = std::env::var("VERTEBRA_OVERFLOW_BACKEND")
            .is_ok_and(|v| v.trim().eq_ignore_ascii_case("tmp"))
            || !dev_shm_has_headroom(size.saturating_add(8 * 1024 * 1024));

        if !prefer_tmp {
            let name = format!("/vtb-{prefix}-{pid:x}-{seq:x}");
            if let Ok(mut region) = Self::create_named(&name, size) {
                region.disarm_unlink();
                return Ok((name, region));
            }
        }

        let tmp_name = format!("/tmp/vtb-{prefix}-{pid:x}-{seq:x}");
        let mut region = Self::create_file_backed(&tmp_name, size)?;
        region.disarm_unlink();
        Ok((tmp_name, region))
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
            unlink_name: Mutex::new(None),
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

    /// Return the OS-reported backing byte length for file/SHM-backed regions, if available.
    pub fn backing_len(&self) -> Option<usize> {
        #[cfg(unix)]
        {
            let fd = self.handle()?;
            // SAFETY: `libc::stat` is plain-old-data valid when zero-initialized, and `fd` is live.
            unsafe {
                let mut st: libc::stat = core::mem::zeroed();
                if libc::fstat(fd, &mut st) == 0 && st.st_size >= 0 {
                    return usize::try_from(st.st_size).ok();
                }
            }
        }
        None
    }
}

impl Drop for ShmRegion {
    fn drop(&mut self) {
        // SAFETY: `self` has exclusive access during drop and owns `ptr`/`handle` according
        // to `owns_mapping` and `owns_handle`.
        unsafe {
            #[cfg(unix)]
            {
                if self.owns_mapping {
                    libc::munmap(self.ptr.as_ptr().cast(), self.len);
                }
                if self.owns_handle && self.handle != INVALID_HANDLE {
                    libc::close(self.handle);
                }
                let unlink_opt = self
                    .unlink_name
                    .get_mut()
                    .unwrap_or_else(|e| e.into_inner())
                    .take();
                if let Some(name) = unlink_opt {
                    if name.as_bytes().starts_with(b"/tmp/vtb-") {
                        let _ = libc::unlink(name.as_ptr());
                    } else {
                        let _ = libc::shm_unlink(name.as_ptr());
                    }
                }
            }
            #[cfg(windows)]
            {
                use windows_sys::Win32::Foundation::CloseHandle;
                use windows_sys::Win32::System::Memory::{
                    MEMORY_MAPPED_VIEW_ADDRESS, UnmapViewOfFile,
                };

                if self.owns_mapping {
                    UnmapViewOfFile(MEMORY_MAPPED_VIEW_ADDRESS {
                        Value: self.ptr.as_ptr().cast(),
                    });
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
        match map_fd_region(fd, size) {
            Ok(region) => Ok(region),
            Err(err) => {
                libc::close(fd);
                Err(err)
            }
        }
    }
}

#[cfg(unix)]
fn create_shm_open_region(size: usize) -> Result<ShmRegion, ShmError> {
    use core::sync::atomic::{AtomicU64, Ordering};
    static SHM_COUNTER: AtomicU64 = AtomicU64::new(0);

    let mut attempt = 0u32;
    let pid = std::process::id();
    let seq = SHM_COUNTER.fetch_add(1, Ordering::Relaxed);
    loop {
        // macOS XNU limits POSIX shared memory names (PSHMNAMLEN) to 31 bytes
        // including the leading slash. Keep the generated name compact.
        let name = format!("/vtb-{pid:x}-{seq:x}-{attempt:x}");
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
                match map_fd_region(fd, size) {
                    Ok(region) => {
                        let _ = libc::shm_unlink(cname.as_ptr());
                        return Ok(region);
                    }
                    Err(err) => {
                        libc::close(fd);
                        let _ = libc::shm_unlink(cname.as_ptr());
                        return Err(err);
                    }
                }
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
        unlink_name: Mutex::new(None),
    })
}

#[cfg(windows)]
fn create_windows_region(size: usize) -> Result<ShmRegion, ShmError> {
    use windows_sys::Win32::Foundation::{CloseHandle, GetLastError, INVALID_HANDLE_VALUE};
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
    let view = unsafe { MapViewOfFile(handle, FILE_MAP_ALL_ACCESS, 0, 0, size) };
    let ptr = view.Value.cast::<u8>();
    if ptr.is_null() {
        let err = io::Error::from_raw_os_error(unsafe { GetLastError() } as i32);
        unsafe {
            CloseHandle(handle);
        }
        return Err(err.into());
    }
    let ptr = NonNull::new(ptr).ok_or(ShmError::MapFailed)?;
    Ok(ShmRegion {
        ptr,
        len: size,
        handle,
        owns_handle: true,
        owns_mapping: true,
        #[cfg(unix)]
        unlink_name: Mutex::new(None),
    })
}

#[cfg(windows)]
unsafe fn map_windows_region(handle: RawHandle, size: usize) -> Result<ShmRegion, ShmError> {
    use windows_sys::Win32::Foundation::GetLastError;
    use windows_sys::Win32::System::Memory::{FILE_MAP_ALL_ACCESS, MapViewOfFile};

    let view = unsafe { MapViewOfFile(handle, FILE_MAP_ALL_ACCESS, 0, 0, size) };
    let ptr = view.Value.cast::<u8>();
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
        unlink_name: Mutex::new(None),
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

/// Return `true` when `/dev/shm` has at least `min_free_bytes` available (or on non-Linux hosts).
pub fn dev_shm_has_headroom(min_free_bytes: usize) -> bool {
    #[cfg(target_os = "linux")]
    unsafe {
        let mut st: libc::statvfs = core::mem::zeroed();
        if libc::statvfs(c"/dev/shm".as_ptr(), &mut st) == 0 {
            let avail = (st.f_bavail as u64).saturating_mul(st.f_frsize as u64);
            return avail >= min_free_bytes as u64;
        }
    }
    let _ = min_free_bytes;
    true
}

/// Unlink a named shared memory region or `/tmp/vtb-*` overflow mapping (Unix only).
///
/// This removes the name from the filesystem so no new processes can open it.
/// Existing mappings remain valid until all processes unmap.
#[cfg(unix)]
pub fn shm_unlink_named(name: &str) -> Result<(), ShmError> {
    validate_name(name)?;
    let cname = CString::new(name)?;
    let result = if name.starts_with("/tmp/vtb-") {
        unsafe { libc::unlink(cname.as_ptr()) }
    } else {
        unsafe { libc::shm_unlink(cname.as_ptr()) }
    };
    if result < 0 {
        return Err(io::Error::last_os_error().into());
    }
    Ok(())
}

fn align_to_page(size: usize) -> Result<usize, ShmError> {
    let page = os_page_size()?;
    size.div_ceil(page)
        .checked_mul(page)
        .ok_or(ShmError::InvalidSize)
}

/// Return the operating system virtual memory page size in bytes.
pub fn os_page_size() -> Result<usize, ShmError> {
    #[cfg(unix)]
    // SAFETY: `libc::sysconf(_SC_PAGESIZE)` is a thread-safe POSIX query taking no pointers.
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
        // SAFETY: `SYSTEM_INFO` is plain-old-data valid when zero-initialized.
        unsafe {
            let mut info: SYSTEM_INFO = core::mem::zeroed();
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

    #[cfg(unix)]
    #[test]
    fn unlink_now_removes_name_while_mapping_stays_readable() {
        let name = format!("/vtb-unlink-now-{}", std::process::id());
        let mut region = match ShmRegion::create_named(&name, 4096) {
            Ok(region) => region,
            Err(ShmError::Sys(err)) if err.kind() == io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("create_named failed: {err:?}"),
        };
        // SAFETY: `region` is exclusively owned in this test.
        unsafe {
            region.as_mut_slice()[0..4].copy_from_slice(b"ABCD");
        }
        assert!(ShmRegion::open_named(&name, 4096).is_ok());
        assert!(region.unlink_now());
        assert!(!region.unlink_now());
        assert!(ShmRegion::open_named(&name, 4096).is_err());
        // SAFETY: `region` mapping remains valid until `region` drops.
        unsafe {
            assert_eq!(&region.as_slice()[0..4], b"ABCD");
        }
    }

    #[test]
    fn shm_capsule_from_arc_preserves_mapping_and_drops_cleanly() {
        use std::sync::atomic::Ordering;

        let mut region = ShmRegion::create(4096).unwrap();
        // SAFETY: `region` is exclusively owned before wrapping in `Arc`.
        unsafe {
            region.as_mut_slice()[0..4].copy_from_slice(&[9, 8, 7, 6]);
        }
        let arc_region = Arc::new(region);
        let weak = Arc::downgrade(&arc_region);
        let capsule = ShmCapsule::from_arc(arc_region);
        assert!(!capsule.ptr.is_null());
        assert!(capsule.len >= 4096);
        assert!(weak.upgrade().is_some());
        // SAFETY: `capsule.ptr` points to at least `capsule.len` valid bytes while `ctx` is live.
        unsafe {
            let slice = core::slice::from_raw_parts(capsule.ptr, 4);
            assert_eq!(slice, &[9, 8, 7, 6]);
        }
        let ctx = capsule.ctx.swap(core::ptr::null_mut(), Ordering::AcqRel);
        assert!(!ctx.is_null());
        // SAFETY: `ctx` was claimed once via atomic swap and is surrendered once to `drop_fn`.
        unsafe {
            (capsule.drop_fn)(ctx);
        }
        assert!(weak.upgrade().is_none());
    }

    #[cfg(unix)]
    #[test]
    fn shm_capsule_concurrent_atomic_swap_and_single_unlink() {
        use std::sync::Barrier;
        use std::sync::atomic::Ordering;

        let name = format!("/vtb-cap-race-{}", std::process::id());
        let region = match ShmRegion::create_named(&name, 4096) {
            Ok(region) => region,
            Err(ShmError::Sys(err)) if err.kind() == io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("create_named failed: {err:?}"),
        };
        let arc_region = Arc::new(region);
        let weak = Arc::downgrade(&arc_region);
        let capsule = Arc::new(ShmCapsule::from_arc(arc_region));

        let thread_count = 8usize;
        let barrier = Arc::new(Barrier::new(thread_count));
        let claimed: Vec<usize> = std::thread::scope(|s| {
            let mut handles = Vec::with_capacity(thread_count);
            for _ in 0..thread_count {
                let cap = Arc::clone(&capsule);
                let bar = Arc::clone(&barrier);
                handles.push(s.spawn(move || {
                    bar.wait();
                    cap.ctx.swap(core::ptr::null_mut(), Ordering::AcqRel) as usize
                }));
            }
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .filter(|&p| p != 0)
                .collect()
        });

        assert_eq!(claimed.len(), 1);
        let ctx = claimed[0] as *mut c_void;
        let unlink_fn = capsule.unlink_fn.expect("unlink_fn present");
        // SAFETY: `ctx` is owned exclusively by this test after winning the atomic swap.
        assert!(unsafe { unlink_fn(ctx) });
        // SAFETY: `ctx` is still live; second unlink must be a no-op returning `false`.
        assert!(!unsafe { unlink_fn(ctx) });

        let replacement = ShmRegion::create_named(&name, 4096).unwrap();
        // SAFETY: `ctx` is surrendered once to `drop_fn`, whose disarmed `unlink_name` must not
        // unlink the newly created `replacement` segment at `name`.
        unsafe {
            (capsule.drop_fn)(ctx);
        }
        assert!(weak.upgrade().is_none());
        assert!(ShmRegion::open_named(&name, 4096).is_ok());
        drop(replacement);
    }

    #[cfg(unix)]
    #[test]
    fn open_named_rejects_under_page_truncated_backing() {
        let tmp_name = format!("/tmp/vtb-trunc-{}", std::process::id());
        let cname = CString::new(tmp_name.as_str()).unwrap();
        // SAFETY: `cname` is a valid NUL-terminated path under `/tmp/vtb-*`.
        unsafe {
            let _ = libc::unlink(cname.as_ptr());
            let fd = libc::open(
                cname.as_ptr(),
                libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
                0o600,
            );
            assert!(fd >= 0);
            assert_eq!(libc::ftruncate(fd, 64), 0);
            libc::close(fd);
        }
        let err = ShmRegion::open_named(&tmp_name, 64).unwrap_err();
        let _ = shm_unlink_named(&tmp_name);
        assert!(matches!(err, ShmError::InvalidSize));
    }

    #[cfg(unix)]
    proptest! {
        #[test]
        fn long_names_are_rejected(len in 256usize..512) {
            let name = format!("/{}", "a".repeat(len));
            let err = ShmRegion::create_named(&name, 1024).unwrap_err();
            prop_assert!(matches!(err, ShmError::InvalidName | ShmError::Name(_)));
        }
    }
}
