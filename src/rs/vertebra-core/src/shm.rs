//! Shared memory region abstraction.

use core::ptr::NonNull;
use std::ffi::{NulError, c_void};
use std::io;
use std::sync::Arc;
#[cfg(unix)]
use std::sync::Mutex;
use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicU64, Ordering};

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

/// Shared-memory ownership header state indicating creator initialization is in progress.
pub const SHM_INIT_PENDING: u32 = 0;
/// Shared-memory ownership header state indicating the region is ready for attachers.
pub const SHM_INIT_READY: u32 = 1;
const SHM_INIT_REAPING: u32 = 2;

const SHM_OWNER_MAGIC: u64 = u64::from_le_bytes(*b"VTBOWNR1");
const INIT_WAIT_TIMEOUT_MS: u64 = 2000;

/// Metadata stored in the ownership trailer page of a named POSIX shared-memory segment.
#[repr(C)]
#[derive(Debug)]
pub struct ShmOwnerHeader {
    pub init_state: AtomicU32,
    pub owner_pid: AtomicU32,
    pub owner_generation: AtomicU64,
}

#[repr(C)]
struct ShmOwnerPage {
    header: ShmOwnerHeader,
    magic: AtomicU64,
    user_len: u64,
}

/// Outcome of [`ShmRegion::open_or_create_named`].
#[derive(Debug)]
pub enum NamedAttach {
    Created(ShmRegion),
    Attached(ShmRegion),
}

#[derive(Debug, thiserror::Error)]
pub enum ShmError {
    #[error("invalid shared memory size")]
    InvalidSize,
    #[error("invalid shared memory name")]
    InvalidName,
    #[error("shared memory region is already owned by live pid {owner_pid}")]
    InUse { owner_pid: u32 },
    #[error("timed out waiting for shared memory initialization")]
    InitTimeout,
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
    map_ptr: NonNull<u8>,
    map_len: usize,
    owner_header_offset: Option<usize>,
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

// SAFETY: C-ABI destructor callback invoked at most once with the raw `Arc<ShmRegion>` pointer
// previously produced by `ShmCapsule::from_arc` (or null).
unsafe extern "C" fn drop_shm_region_arc(ctx: *mut c_void) {
    if !ctx.is_null() {
        // SAFETY: `ctx` was produced via `Arc::into_raw` on an `Arc<ShmRegion>` in
        // `ShmCapsule::from_arc` and ownership is surrendered at most once to `drop_fn`.
        unsafe {
            drop(Arc::from_raw(ctx.cast::<ShmRegion>()));
        }
    }
}

// SAFETY: C-ABI unlink callback invoked while the caller still owns a live `ctx` pointer
// produced by `ShmCapsule::from_arc` (or null).
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
            // SAFETY: Caller guarantees `handle` is a valid open FD sized for `size` page-aligned bytes.
            return unsafe { map_fd_region(handle, size) };
        }
        #[cfg(windows)]
        {
            // SAFETY: Caller guarantees `handle` is a valid mapping handle sized for `size` page-aligned bytes.
            return unsafe { map_windows_region(handle, size) };
        }
        #[allow(unreachable_code)]
        Err(ShmError::InvalidSize)
    }

    /// Map an existing shared memory file descriptor / handle into this process.
    pub fn from_fd(handle: ShmHandle, size: usize) -> Result<Self, ShmError> {
        // SAFETY: Caller transfers an open `handle` backed by at least `size` bytes; `Self::map`
        // validates non-zero page-aligned size and returns any OS mapping error.
        unsafe { Self::map(handle, size) }
    }

    /// Open an existing named region (Unix only).
    #[cfg(unix)]
    pub fn open_named(name: &str, size: usize) -> Result<Self, ShmError> {
        if size == 0 {
            return Err(ShmError::InvalidSize);
        }
        validate_name(name)?;
        let req_user_len = align_to_page(size)?;
        let page = os_page_size()?;
        let cname = CString::new(name)?;
        let start = std::time::Instant::now();
        let deadline = std::time::Duration::from_millis(INIT_WAIT_TIMEOUT_MS);

        loop {
            let fd = if name.starts_with("/tmp/vtb-") {
                // SAFETY: `cname` is a validated NUL-terminated `/tmp/vtb-*` path `CString`.
                unsafe { libc::open(cname.as_ptr(), libc::O_RDWR, 0o600) }
            } else {
                // SAFETY: `cname` is a validated NUL-terminated POSIX SHM name `CString`.
                unsafe { libc::shm_open(cname.as_ptr(), libc::O_RDWR, 0o600) }
            };
            if fd < 0 {
                return Err(io::Error::last_os_error().into());
            }

            // SAFETY: `libc::stat` is plain-old-data valid when zero-initialized, and `fd` is owned here.
            let file_len = unsafe {
                let mut st: libc::stat = core::mem::zeroed();
                if libc::fstat(fd, &mut st) != 0 {
                    let err = io::Error::last_os_error();
                    libc::close(fd);
                    return Err(err.into());
                }
                if st.st_size < 0 {
                    libc::close(fd);
                    return Err(ShmError::InvalidSize);
                }
                st.st_size as usize
            };

            if file_len == 0 {
                // SAFETY: `fd` was opened above and is not yet owned by a `ShmRegion`.
                unsafe {
                    libc::close(fd);
                }
                if start.elapsed() >= deadline {
                    return Err(ShmError::InitTimeout);
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
                continue;
            }

            if file_len < req_user_len {
                // SAFETY: `fd` was opened above and is closed on size validation failure.
                unsafe {
                    libc::close(fd);
                }
                return Err(ShmError::InvalidSize);
            }

            let has_trailer_shape = !name.starts_with("/tmp/vtb-")
                && file_len >= page * 2
                && file_len.is_multiple_of(page);
            let map_len = if has_trailer_shape {
                file_len
            } else {
                req_user_len
            };

            // SAFETY: `fd` is an open shared-memory descriptor backed by at least `map_len` bytes.
            let mut region = match unsafe { map_fd_region(fd, map_len) } {
                Ok(region) => region,
                Err(err) => {
                    // SAFETY: `fd` was opened above and is closed on map failure.
                    unsafe {
                        libc::close(fd);
                    }
                    return Err(err);
                }
            };

            if has_trailer_shape {
                let trailer_off = file_len - page;
                // SAFETY: `trailer_off` is page-aligned within `0..map_len` with at least one full page.
                let owner_page =
                    unsafe { &*(region.ptr.as_ptr().add(trailer_off) as *const ShmOwnerPage) };
                if owner_page.magic.load(Ordering::Acquire) == SHM_OWNER_MAGIC {
                    let state = owner_page.header.init_state.load(Ordering::Acquire);
                    if state == SHM_INIT_PENDING || state == SHM_INIT_REAPING {
                        let pid = owner_page.header.owner_pid.load(Ordering::Acquire);
                        if state == SHM_INIT_PENDING && pid != 0 && !is_pid_alive(pid) {
                            drop(region);
                            return Err(io::Error::from_raw_os_error(libc::ENOENT).into());
                        }
                        drop(region);
                        if start.elapsed() >= deadline {
                            return Err(ShmError::InitTimeout);
                        }
                        std::thread::sleep(std::time::Duration::from_millis(1));
                        continue;
                    }
                    let Ok(available_user_len) = usize::try_from(owner_page.user_len) else {
                        drop(region);
                        return Err(ShmError::InvalidSize);
                    };
                    if available_user_len > trailer_off {
                        drop(region);
                        return Err(ShmError::InvalidSize);
                    }
                    if available_user_len < req_user_len {
                        let pid = owner_page.header.owner_pid.load(Ordering::Acquire);
                        drop(region);
                        if pid != 0 && is_pid_alive(pid) {
                            return Err(ShmError::InUse { owner_pid: pid });
                        }
                        return Err(ShmError::InvalidSize);
                    }
                    region.len = req_user_len;
                    region.owner_header_offset = Some(trailer_off);
                } else {
                    drop(region);
                    #[cfg(test)]
                    notify_magic_wait_hook();
                    if start.elapsed() >= deadline {
                        return Err(ShmError::InitTimeout);
                    }
                    std::thread::sleep(std::time::Duration::from_millis(1));
                    continue;
                }
            }
            return Ok(region);
        }
    }

    /// Create a named shared-memory region on Unix, running `init` before publishing readiness.
    #[cfg(unix)]
    pub(crate) fn create_named_with(
        name: &str,
        size: usize,
        init: impl FnOnce(&mut ShmRegion) -> Result<(), ShmError>,
    ) -> Result<Self, ShmError> {
        let mut init_opt = Some(init);
        let mut min_generation = 1u64;
        for _ in 0..8 {
            match Self::try_create_named_with_gen(name, size, min_generation, &mut init_opt) {
                Ok(region) => return Ok(region),
                Err(CreateOrOpenConflict::Retry(next_gen)) => {
                    min_generation = next_gen;
                }
                Err(CreateOrOpenConflict::Err(err)) => return Err(err),
            }
        }
        Err(ShmError::InitTimeout)
    }

    /// Create a named shared-memory region on Unix.
    #[cfg(unix)]
    pub fn create_named(name: &str, size: usize) -> Result<Self, ShmError> {
        Self::create_named_with(name, size, |_| Ok(()))
    }

    /// Atomically open an existing initialized named segment or create and initialize it once.
    #[cfg(unix)]
    pub fn open_or_create_named(
        name: &str,
        size: usize,
        init: impl FnOnce(&mut ShmRegion) -> Result<(), ShmError>,
    ) -> Result<NamedAttach, ShmError> {
        if size == 0 {
            return Err(ShmError::InvalidSize);
        }
        validate_shm_name(name)?;
        let req_user_len = align_to_page(size)?;
        let page = os_page_size()?;
        let cname = CString::new(name)?;
        let start = std::time::Instant::now();
        let deadline = std::time::Duration::from_millis(INIT_WAIT_TIMEOUT_MS);
        let mut next_generation = 1u64;
        let mut init_opt = Some(init);

        loop {
            match Self::try_create_exclusive(&cname, name, req_user_len, page, next_generation) {
                Ok(mut region) => {
                    if let Some(init_fn) = init_opt.take()
                        && let Err(err) = init_fn(&mut region)
                    {
                        return Err(err);
                    }
                    region.mark_owner_ready();
                    crate::cleanup::register_owned(name);
                    return Ok(NamedAttach::Created(region));
                }
                Err(ShmError::Sys(ref err)) if err.raw_os_error() == Some(libc::EEXIST) => {}
                Err(err) => return Err(err),
            }

            match Self::try_attach_existing_or_reap(&cname, req_user_len, page, start, deadline) {
                Ok(Some(region)) => return Ok(NamedAttach::Attached(region)),
                Ok(None) => {
                    if start.elapsed() >= deadline {
                        return Err(ShmError::InitTimeout);
                    }
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Err(AttachOrRetry::RetryWithGeneration(owner_gen)) => {
                    next_generation = next_generation.max(owner_gen);
                    if start.elapsed() >= deadline {
                        return Err(ShmError::InitTimeout);
                    }
                }
                Err(AttachOrRetry::Err(err)) => return Err(err),
            }
        }
    }

    #[cfg(unix)]
    fn try_create_named_with_gen<F>(
        name: &str,
        size: usize,
        generation: u64,
        init_opt: &mut Option<F>,
    ) -> Result<Self, CreateOrOpenConflict>
    where
        F: FnOnce(&mut ShmRegion) -> Result<(), ShmError>,
    {
        if size == 0 {
            return Err(CreateOrOpenConflict::Err(ShmError::InvalidSize));
        }
        validate_shm_name(name).map_err(CreateOrOpenConflict::Err)?;
        let user_len = align_to_page(size).map_err(CreateOrOpenConflict::Err)?;
        let page = os_page_size().map_err(CreateOrOpenConflict::Err)?;
        let cname = CString::new(name).map_err(|e| CreateOrOpenConflict::Err(e.into()))?;

        match Self::try_create_exclusive(&cname, name, user_len, page, generation) {
            Ok(mut region) => {
                if let Some(init_fn) = init_opt.take()
                    && let Err(err) = init_fn(&mut region)
                {
                    return Err(CreateOrOpenConflict::Err(err));
                }
                region.mark_owner_ready();
                crate::cleanup::register_owned(name);
                return Ok(region);
            }
            Err(ShmError::Sys(ref err)) if err.raw_os_error() == Some(libc::EEXIST) => {}
            Err(err) => return Err(CreateOrOpenConflict::Err(err)),
        }

        let start = std::time::Instant::now();
        let deadline = std::time::Duration::from_millis(INIT_WAIT_TIMEOUT_MS);
        let probe = Self::inspect_existing_for_create(&cname, page, start, deadline)?;
        Err(CreateOrOpenConflict::Retry(probe))
    }

    #[cfg(unix)]
    fn try_create_exclusive(
        cname: &CString,
        name: &str,
        user_len: usize,
        page: usize,
        generation: u64,
    ) -> Result<Self, ShmError> {
        let total_len = user_len.checked_add(page).ok_or(ShmError::InvalidSize)?;
        // SAFETY: `cname` is a valid NUL-terminated `CString` and `O_CREAT | O_EXCL` creates atomically.
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
        // SAFETY: `fd` is exclusively owned here and truncated before mapping.
        unsafe {
            if libc::ftruncate(fd, total_len as libc::off_t) != 0 {
                let err = io::Error::last_os_error();
                libc::close(fd);
                let _ = libc::shm_unlink(cname.as_ptr());
                return Err(err.into());
            }
        }
        // SAFETY: `fd` has length `total_len` bytes.
        let mut region = match unsafe { map_fd_region(fd, total_len) } {
            Ok(region) => region,
            Err(err) => {
                // SAFETY: clean up descriptor and name after mmap failure.
                unsafe {
                    libc::close(fd);
                    let _ = libc::shm_unlink(cname.as_ptr());
                }
                return Err(err);
            }
        };
        let trailer_off = user_len;
        // SAFETY: `trailer_off` points to page-aligned trailer within `0..total_len`.
        let owner_page =
            unsafe { &mut *(region.ptr.as_ptr().add(trailer_off) as *mut ShmOwnerPage) };
        owner_page
            .header
            .init_state
            .store(SHM_INIT_PENDING, Ordering::Relaxed);
        owner_page
            .header
            .owner_pid
            .store(std::process::id(), Ordering::Relaxed);
        owner_page
            .header
            .owner_generation
            .store(generation, Ordering::Relaxed);
        owner_page.user_len = user_len as u64;
        owner_page.magic.store(SHM_OWNER_MAGIC, Ordering::Release);

        region.len = user_len;
        region.owner_header_offset = Some(trailer_off);
        region.owns_handle = true;
        region.owns_mapping = true;
        *region.unlink_name.get_mut().expect("unlink_name mutex") = Some(cname.clone());
        let _ = name;
        Ok(region)
    }

    #[cfg(unix)]
    fn inspect_existing_for_create(
        cname: &CString,
        page: usize,
        start: std::time::Instant,
        deadline: std::time::Duration,
    ) -> Result<u64, CreateOrOpenConflict> {
        loop {
            // SAFETY: `cname` is a validated NUL-terminated POSIX SHM name `CString`.
            let fd = unsafe { libc::shm_open(cname.as_ptr(), libc::O_RDWR, 0o600) };
            if fd < 0 {
                let err = io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::ENOENT) {
                    return Ok(1);
                }
                return Err(CreateOrOpenConflict::Err(err.into()));
            }
            let (file_len, dev, ino) = match stat_fd(fd) {
                Ok(info) => info,
                Err(err) => {
                    // SAFETY: `fd` was opened above and is closed on `stat_fd` failure.
                    unsafe {
                        libc::close(fd);
                    }
                    return Err(CreateOrOpenConflict::Err(err));
                }
            };
            if file_len == 0 {
                // SAFETY: `fd` was opened above and is closed before retrying.
                unsafe {
                    libc::close(fd);
                }
                if start.elapsed() >= deadline {
                    return Err(CreateOrOpenConflict::Err(ShmError::InitTimeout));
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
                continue;
            }
            if file_len < page * 2 || !file_len.is_multiple_of(page) {
                // SAFETY: `fd` was opened above and is closed on invalid trailer sizing.
                unsafe {
                    libc::close(fd);
                }
                return Err(CreateOrOpenConflict::Err(ShmError::InvalidSize));
            }
            let trailer_off = file_len - page;
            // SAFETY: maps only the single trailer page at page-aligned `trailer_off`.
            let trailer_ptr = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    page,
                    libc::PROT_READ,
                    libc::MAP_SHARED,
                    fd,
                    trailer_off as libc::off_t,
                )
            };
            if trailer_ptr == libc::MAP_FAILED {
                let err = io::Error::last_os_error();
                // SAFETY: `fd` was opened above and is closed after `mmap` failure.
                unsafe {
                    libc::close(fd);
                }
                return Err(CreateOrOpenConflict::Err(err.into()));
            }
            // SAFETY: `trailer_ptr` is a page-aligned mapping of `page` bytes (`>= size_of::<ShmOwnerPage>()`).
            let (magic, state, pid, owner_gen) = unsafe {
                let p = &*(trailer_ptr as *const ShmOwnerPage);
                (
                    p.magic.load(Ordering::Acquire),
                    p.header.init_state.load(Ordering::Acquire),
                    p.header.owner_pid.load(Ordering::Acquire),
                    p.header.owner_generation.load(Ordering::Acquire),
                )
            };
            // SAFETY: `trailer_ptr` was mapped above with length `page`.
            unsafe {
                libc::munmap(trailer_ptr, page);
            }
            if magic != SHM_OWNER_MAGIC || state == SHM_INIT_PENDING || state == SHM_INIT_REAPING {
                if magic == SHM_OWNER_MAGIC
                    && state == SHM_INIT_PENDING
                    && pid != 0
                    && !is_pid_alive(pid)
                {
                    let _ = try_unlink_stale_locked(fd, cname, dev, ino, trailer_off, page);
                    // SAFETY: `fd` is owned by this function and closed on retry.
                    unsafe {
                        libc::close(fd);
                    }
                    return Ok(owner_gen.wrapping_add(1));
                }
                // SAFETY: `fd` is owned by this function and closed before waiting.
                unsafe {
                    libc::close(fd);
                }
                if start.elapsed() >= deadline {
                    if state == SHM_INIT_PENDING && pid != 0 && is_pid_alive(pid) {
                        return Err(CreateOrOpenConflict::Err(ShmError::InUse {
                            owner_pid: pid,
                        }));
                    }
                    return Err(CreateOrOpenConflict::Err(ShmError::InitTimeout));
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
                continue;
            }
            if pid != 0 && is_pid_alive(pid) {
                // SAFETY: `fd` is owned by this function and closed before returning `InUse`.
                unsafe {
                    libc::close(fd);
                }
                return Err(CreateOrOpenConflict::Err(ShmError::InUse {
                    owner_pid: pid,
                }));
            }
            let _ = try_unlink_stale_locked(fd, cname, dev, ino, trailer_off, page);
            // SAFETY: `fd` is owned by this function and closed on stale reap.
            unsafe {
                libc::close(fd);
            }
            return Ok(owner_gen.wrapping_add(1));
        }
    }

    #[cfg(unix)]
    fn try_attach_existing_or_reap(
        cname: &CString,
        req_user_len: usize,
        page: usize,
        start: std::time::Instant,
        deadline: std::time::Duration,
    ) -> Result<Option<Self>, AttachOrRetry> {
        // SAFETY: `cname` is a validated NUL-terminated POSIX SHM name `CString`.
        let fd = unsafe { libc::shm_open(cname.as_ptr(), libc::O_RDWR, 0o600) };
        if fd < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ENOENT) {
                return Err(AttachOrRetry::RetryWithGeneration(1));
            }
            return Err(AttachOrRetry::Err(err.into()));
        }
        let (file_len, dev, ino) = match stat_fd(fd) {
            Ok(info) => info,
            Err(err) => {
                // SAFETY: `fd` was opened above and is closed on `stat_fd` failure.
                unsafe {
                    libc::close(fd);
                }
                return Err(AttachOrRetry::Err(err));
            }
        };
        if file_len == 0 {
            // SAFETY: `fd` was opened above and is closed while creator finishes truncating.
            unsafe {
                libc::close(fd);
            }
            return Ok(None);
        }
        if file_len < page * 2 || !file_len.is_multiple_of(page) {
            // SAFETY: `fd` was opened above and is closed on invalid trailer sizing.
            unsafe {
                libc::close(fd);
            }
            return Err(AttachOrRetry::Err(ShmError::InvalidSize));
        }
        // SAFETY: `fd` is an open SHM descriptor verified to be `file_len` bytes long.
        let mut region = match unsafe { map_fd_region(fd, file_len) } {
            Ok(r) => r,
            Err(err) => {
                // SAFETY: `fd` was opened above and is closed on `mmap` failure.
                unsafe {
                    libc::close(fd);
                }
                return Err(AttachOrRetry::Err(err));
            }
        };
        let trailer_off = file_len - page;
        // SAFETY: `trailer_off` is page-aligned within `0..file_len`.
        let owner_page = unsafe { &*(region.ptr.as_ptr().add(trailer_off) as *const ShmOwnerPage) };
        let magic = owner_page.magic.load(Ordering::Acquire);
        if magic != SHM_OWNER_MAGIC {
            drop(region);
            return Ok(None);
        }
        let state = owner_page.header.init_state.load(Ordering::Acquire);
        let pid = owner_page.header.owner_pid.load(Ordering::Acquire);
        let owner_gen = owner_page.header.owner_generation.load(Ordering::Acquire);
        if state == SHM_INIT_PENDING {
            if pid != 0 && !is_pid_alive(pid) {
                let _ = try_unlink_stale_locked(fd, cname, dev, ino, trailer_off, page);
                drop(region);
                if start.elapsed() >= deadline {
                    return Err(AttachOrRetry::Err(ShmError::InitTimeout));
                }
                return Err(AttachOrRetry::RetryWithGeneration(
                    owner_gen.wrapping_add(1),
                ));
            }
            drop(region);
            return Ok(None);
        }
        if state == SHM_INIT_REAPING {
            drop(region);
            return Ok(None);
        }
        if state != SHM_INIT_READY {
            drop(region);
            return Err(AttachOrRetry::Err(ShmError::InvalidSize));
        }
        if !is_pid_alive(pid) {
            let _ = try_unlink_stale_locked(fd, cname, dev, ino, trailer_off, page);
            drop(region);
            if start.elapsed() >= deadline {
                return Err(AttachOrRetry::Err(ShmError::InitTimeout));
            }
            return Err(AttachOrRetry::RetryWithGeneration(
                owner_gen.wrapping_add(1),
            ));
        }
        let Ok(user_len) = usize::try_from(owner_page.user_len) else {
            drop(region);
            return Err(AttachOrRetry::Err(ShmError::InvalidSize));
        };
        if user_len == 0 || user_len > trailer_off {
            drop(region);
            return Err(AttachOrRetry::Err(ShmError::InvalidSize));
        }
        if user_len < req_user_len {
            drop(region);
            return Err(AttachOrRetry::Err(ShmError::InUse { owner_pid: pid }));
        }
        region.len = user_len;
        region.owner_header_offset = Some(trailer_off);
        Ok(Some(region))
    }

    fn mark_owner_ready(&self) {
        if let Some(off) = self.owner_header_offset {
            // SAFETY: `off` points to `ShmOwnerPage` within `self.map_ptr..self.map_ptr + self.map_len`.
            let owner_page = unsafe { &*(self.map_ptr.as_ptr().add(off) as *const ShmOwnerPage) };
            owner_page
                .header
                .init_state
                .store(SHM_INIT_READY, Ordering::Release);
        }
    }

    /// Return the registered owner PID from the segment ownership trailer, if present.
    pub fn owner_pid(&self) -> Option<u32> {
        let off = self.owner_header_offset?;
        // SAFETY: `off` points to a validated `ShmOwnerPage` inside `self.map_ptr..self.map_ptr + self.map_len`.
        let owner_page = unsafe { &*(self.map_ptr.as_ptr().add(off) as *const ShmOwnerPage) };
        if owner_page.magic.load(Ordering::Acquire) != SHM_OWNER_MAGIC {
            return None;
        }
        let pid = owner_page.header.owner_pid.load(Ordering::Acquire);
        if pid == 0 { None } else { Some(pid) }
    }

    /// Return the owner generation counter stored in the ownership trailer, if present.
    pub fn owner_generation(&self) -> Option<u64> {
        let off = self.owner_header_offset?;
        // SAFETY: `off` points to a validated `ShmOwnerPage` inside `self.map_ptr..self.map_ptr + self.map_len`.
        let owner_page = unsafe { &*(self.map_ptr.as_ptr().add(off) as *const ShmOwnerPage) };
        if owner_page.magic.load(Ordering::Acquire) != SHM_OWNER_MAGIC {
            return None;
        }
        Some(owner_page.header.owner_generation.load(Ordering::Acquire))
    }

    /// Return `true` when the segment has a registered live owner process.
    pub fn owner_alive(&self) -> bool {
        self.owner_pid().is_some_and(is_pid_alive)
    }

    /// Disarm automatic `shm_unlink` on drop so a peer process can open the named region.
    #[cfg(unix)]
    pub fn disarm_unlink(&mut self) {
        if let Some(name) = self
            .unlink_name
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            && let Ok(s) = name.to_str()
        {
            crate::cleanup::unregister_owned(s);
        }
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
        if let Ok(s) = name.to_str() {
            crate::cleanup::unregister_owned(s);
        }
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
        let Some(rest) = path.strip_prefix("/tmp/vtb-") else {
            return Err(ShmError::InvalidName);
        };
        if rest.is_empty() || rest.contains('/') || rest.contains('\0') || path.len() > 255 {
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
        crate::cleanup::register_owned(path);
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
        // SAFETY: `self.ptr` is a `NonNull` valid mapping of `self.len` bytes for `&self`'s
        // lifetime, and the caller guarantees no conflicting unsynchronized mutable aliasing.
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    /// Get the raw bytes as a mutable slice.
    ///
    /// # Safety
    /// The caller must ensure exclusive write access to this mapping.
    pub unsafe fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: `self.ptr` is a `NonNull` valid mapping of `self.len` bytes with exclusive
        // `&mut self` access, and the caller guarantees cross-process write exclusivity.
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
            map_ptr: ptr,
            map_len: len,
            owner_header_offset: None,
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
                    libc::munmap(self.map_ptr.as_ptr().cast(), self.map_len);
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
                    if let Ok(s) = name.to_str() {
                        crate::cleanup::unregister_owned(s);
                    }
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
    // SAFETY: `name` is a valid NUL-terminated `CString` passed to `SYS_memfd_create`.
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
    // SAFETY: `fd` is a newly created owned memfd truncated to page-aligned `size` before sealing and mapping.
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
        // SAFETY: `cname` is a valid NUL-terminated `CString` and `O_CREAT | O_EXCL` creates atomically.
        let fd = unsafe {
            libc::shm_open(
                cname.as_ptr(),
                libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
                0o600,
            )
        };
        if fd >= 0 {
            // SAFETY: `fd` is exclusively owned, truncated to page-aligned `size`, mapped, and
            // unlinked from `/dev/shm` so only the anonymous FD mapping remains.
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

/// # Safety
/// Caller must ensure `fd` is a valid open file descriptor backed by at least `size` bytes.
#[cfg(unix)]
unsafe fn map_fd_region(fd: RawFd, size: usize) -> Result<ShmRegion, ShmError> {
    // SAFETY: Caller guarantees `fd` is open and at least `size` bytes; `mmap` checks errors
    // and `NonNull::new` rejects null pointers.
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
        map_ptr: ptr,
        map_len: size,
        owner_header_offset: None,
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
    // SAFETY: `CreateFileMappingW` with `INVALID_HANDLE_VALUE` and null name creates an anonymous
    // pagefile-backed section of `size` bytes.
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
        // SAFETY: `GetLastError` reads the calling thread's last-error TLS value.
        return Err(io::Error::from_raw_os_error(unsafe { GetLastError() } as i32).into());
    }
    // SAFETY: `handle` is a valid non-null file-mapping object sized for `size` bytes.
    let view = unsafe { MapViewOfFile(handle, FILE_MAP_ALL_ACCESS, 0, 0, size) };
    let ptr = view.Value.cast::<u8>();
    if ptr.is_null() {
        // SAFETY: `GetLastError` reads the calling thread's last-error TLS value.
        let err = io::Error::from_raw_os_error(unsafe { GetLastError() } as i32);
        // SAFETY: `handle` was created above and must be closed when `MapViewOfFile` fails.
        unsafe {
            CloseHandle(handle);
        }
        return Err(err.into());
    }
    let ptr = NonNull::new(ptr).ok_or(ShmError::MapFailed)?;
    Ok(ShmRegion {
        ptr,
        len: size,
        map_ptr: ptr,
        map_len: size,
        owner_header_offset: None,
        handle,
        owns_handle: true,
        owns_mapping: true,
        #[cfg(unix)]
        unlink_name: Mutex::new(None),
    })
}

/// # Safety
/// Caller must ensure `handle` is a valid Windows file-mapping handle backed by at least `size` bytes.
#[cfg(windows)]
unsafe fn map_windows_region(handle: RawHandle, size: usize) -> Result<ShmRegion, ShmError> {
    use windows_sys::Win32::Foundation::GetLastError;
    use windows_sys::Win32::System::Memory::{FILE_MAP_ALL_ACCESS, MapViewOfFile};

    // SAFETY: Caller guarantees `handle` is a valid mapping handle of at least `size` bytes.
    let view = unsafe { MapViewOfFile(handle, FILE_MAP_ALL_ACCESS, 0, 0, size) };
    let ptr = view.Value.cast::<u8>();
    if ptr.is_null() {
        // SAFETY: `GetLastError` reads the calling thread's last-error TLS value.
        return Err(io::Error::from_raw_os_error(unsafe { GetLastError() } as i32).into());
    }
    let ptr = NonNull::new(ptr).ok_or(ShmError::MapFailed)?;
    Ok(ShmRegion {
        ptr,
        len: size,
        map_ptr: ptr,
        map_len: size,
        owner_header_offset: None,
        handle,
        owns_handle: true,
        owns_mapping: true,
        #[cfg(unix)]
        unlink_name: Mutex::new(None),
    })
}

#[cfg(unix)]
enum CreateOrOpenConflict {
    Retry(u64),
    Err(ShmError),
}

#[cfg(unix)]
enum AttachOrRetry {
    RetryWithGeneration(u64),
    Err(ShmError),
}

#[cfg(unix)]
fn is_pid_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    let Ok(raw_pid) = libc::pid_t::try_from(pid) else {
        return false;
    };
    // SAFETY: `kill(pid, 0)` performs a non-destructive liveness probe without sending a signal.
    let rc = unsafe { libc::kill(raw_pid, 0) };
    if rc == 0 {
        return true;
    }
    io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(not(unix))]
fn is_pid_alive(_pid: u32) -> bool {
    false
}

#[cfg(unix)]
fn stat_fd(fd: RawFd) -> Result<(usize, libc::dev_t, libc::ino_t), ShmError> {
    // SAFETY: zeroed `libc::stat` is valid for `fstat` on an owned `fd`.
    unsafe {
        let mut st: libc::stat = core::mem::zeroed();
        if libc::fstat(fd, &mut st) != 0 {
            return Err(io::Error::last_os_error().into());
        }
        if st.st_size < 0 {
            return Err(ShmError::InvalidSize);
        }
        Ok((st.st_size as usize, st.st_dev, st.st_ino))
    }
}

#[cfg(unix)]
fn try_unlink_stale_locked(
    fd: RawFd,
    cname: &CString,
    expected_dev: libc::dev_t,
    expected_ino: libc::ino_t,
    trailer_off: usize,
    page: usize,
) -> bool {
    // SAFETY: `fd` is an open file descriptor to the candidate stale segment.
    let flocked = if unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) } == 0 {
        true
    } else {
        let errno = io::Error::last_os_error().raw_os_error();
        #[allow(unreachable_patterns)]
        if !matches!(errno, Some(libc::ENOTSUP) | Some(libc::EOPNOTSUPP)) {
            return false;
        }
        // Fallback for kernels (such as macOS XNU) where `flock` is unsupported on `shm_open` FDs:
        // claim exclusive reap ownership by atomically moving `init_state` to `SHM_INIT_REAPING`.
        // SAFETY: `trailer_off` is page-aligned within `fd`.
        let trailer_ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                page,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                trailer_off as libc::off_t,
            )
        };
        if trailer_ptr == libc::MAP_FAILED {
            return false;
        }
        // SAFETY: `trailer_ptr` maps one page holding `ShmOwnerPage`.
        let claimed = unsafe {
            let p = &*(trailer_ptr as *const ShmOwnerPage);
            if p.magic.load(Ordering::Acquire) != SHM_OWNER_MAGIC
                || is_pid_alive(p.header.owner_pid.load(Ordering::Acquire))
            {
                false
            } else {
                let state = p.header.init_state.load(Ordering::Acquire);
                (state == SHM_INIT_PENDING || state == SHM_INIT_READY)
                    && p.header
                        .init_state
                        .compare_exchange(
                            state,
                            SHM_INIT_REAPING,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
            }
        };
        // SAFETY: `trailer_ptr` was mapped with length `page`.
        unsafe {
            libc::munmap(trailer_ptr, page);
        }
        if !claimed {
            return false;
        }
        false
    };
    // SAFETY: `cname` is a validated NUL-terminated SHM name.
    let cur_fd = unsafe { libc::shm_open(cname.as_ptr(), libc::O_RDONLY, 0) };
    let matches_inode = if cur_fd >= 0 {
        let matched = stat_fd(cur_fd).is_ok_and(|(_, d, i)| d == expected_dev && i == expected_ino);
        // SAFETY: `cur_fd` was opened above.
        unsafe {
            libc::close(cur_fd);
        }
        matched
    } else {
        false
    };
    let unlinked = if matches_inode {
        // SAFETY: `cname` refers to the verified stale `(dev, ino)` segment.
        unsafe { libc::shm_unlink(cname.as_ptr()) == 0 }
    } else {
        false
    };
    if flocked {
        // SAFETY: `fd` holds the exclusive flock acquired above.
        unsafe {
            libc::flock(fd, libc::LOCK_UN);
        }
    }
    unlinked
}

/// Inspect a named segment and unlink it only when its [`ShmOwnerHeader`] identifies a dead PID.
#[cfg(unix)]
pub(crate) fn try_unlink_if_dead_owner(name: &str) -> bool {
    let Ok(()) = validate_shm_name(name) else {
        return false;
    };
    let Ok(page) = os_page_size() else {
        return false;
    };
    let Ok(cname) = CString::new(name) else {
        return false;
    };
    // SAFETY: `cname` is a validated NUL-terminated SHM name.
    let fd = unsafe { libc::shm_open(cname.as_ptr(), libc::O_RDWR, 0o600) };
    if fd < 0 {
        return false;
    }
    let Ok((file_len, dev, ino)) = stat_fd(fd) else {
        // SAFETY: `fd` was opened above.
        unsafe {
            libc::close(fd);
        }
        return false;
    };
    if file_len < page * 2 || !file_len.is_multiple_of(page) {
        // SAFETY: `fd` was opened above.
        unsafe {
            libc::close(fd);
        }
        return false;
    }
    let trailer_off = file_len - page;
    // SAFETY: maps only the page-aligned owner trailer page read-only.
    let trailer_ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            page,
            libc::PROT_READ,
            libc::MAP_SHARED,
            fd,
            trailer_off as libc::off_t,
        )
    };
    if trailer_ptr == libc::MAP_FAILED {
        // SAFETY: `fd` was opened above.
        unsafe {
            libc::close(fd);
        }
        return false;
    }
    // SAFETY: `trailer_ptr` maps one page holding `ShmOwnerPage`.
    let should_reap = unsafe {
        let p = &*(trailer_ptr as *const ShmOwnerPage);
        p.magic.load(Ordering::Acquire) == SHM_OWNER_MAGIC
            && p.header.owner_pid.load(Ordering::Acquire) != 0
            && !is_pid_alive(p.header.owner_pid.load(Ordering::Acquire))
    };
    // SAFETY: `trailer_ptr` was mapped with length `page`.
    unsafe {
        libc::munmap(trailer_ptr, page);
    }
    let reaped = if should_reap {
        try_unlink_stale_locked(fd, &cname, dev, ino, trailer_off, page)
    } else {
        false
    };
    // SAFETY: `fd` was opened above.
    unsafe {
        libc::close(fd);
    }
    reaped
}

/// Read the owner PID from an existing named segment's trailer without mutating or attaching it.
#[cfg(unix)]
pub fn probe_owner_pid(name: &str) -> Option<u32> {
    validate_shm_name(name).ok()?;
    let page = os_page_size().ok()?;
    let cname = CString::new(name).ok()?;
    // SAFETY: `cname` is a validated NUL-terminated POSIX SHM name `CString`.
    let fd = unsafe { libc::shm_open(cname.as_ptr(), libc::O_RDONLY, 0) };
    if fd < 0 {
        return None;
    }
    let (file_len, _, _) = match stat_fd(fd) {
        Ok(info) => info,
        Err(_) => {
            // SAFETY: `fd` was opened above and is closed on `stat_fd` failure.
            unsafe {
                libc::close(fd);
            }
            return None;
        }
    };
    if file_len < page * 2 || !file_len.is_multiple_of(page) {
        // SAFETY: `fd` was opened above and is closed on invalid trailer sizing.
        unsafe {
            libc::close(fd);
        }
        return None;
    }
    let trailer_off = file_len - page;
    // SAFETY: `trailer_off` is page-aligned within `file_len`, mapping one trailer page read-only.
    let trailer_ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            page,
            libc::PROT_READ,
            libc::MAP_SHARED,
            fd,
            trailer_off as libc::off_t,
        )
    };
    if trailer_ptr == libc::MAP_FAILED {
        // SAFETY: `fd` was opened above and is closed on `mmap` failure.
        unsafe {
            libc::close(fd);
        }
        return None;
    }
    // SAFETY: `trailer_ptr` maps `page` bytes (`>= size_of::<ShmOwnerPage>()`) aligned to a page boundary.
    let pid = unsafe {
        let p = &*(trailer_ptr as *const ShmOwnerPage);
        if p.magic.load(Ordering::Acquire) == SHM_OWNER_MAGIC {
            let v = p.header.owner_pid.load(Ordering::Acquire);
            if v == 0 { None } else { Some(v) }
        } else {
            None
        }
    };
    // SAFETY: `trailer_ptr` was mapped with length `page`, and `fd` is owned here.
    unsafe {
        libc::munmap(trailer_ptr, page);
        libc::close(fd);
    }
    pid
}

#[cfg(unix)]
fn validate_shm_name(name: &str) -> Result<(), ShmError> {
    let Some(rest) = name.strip_prefix('/') else {
        return Err(ShmError::InvalidName);
    };
    #[cfg(target_os = "macos")]
    let max_len = 31usize;
    #[cfg(not(target_os = "macos"))]
    let max_len = 255usize;
    if rest.is_empty() || rest.contains('/') || rest.contains('\0') || name.len() > max_len {
        return Err(ShmError::InvalidName);
    }
    Ok(())
}

#[cfg(unix)]
fn validate_name(name: &str) -> Result<(), ShmError> {
    if let Some(rest) = name.strip_prefix("/tmp/vtb-") {
        if rest.is_empty() || rest.contains('/') || rest.contains('\0') || name.len() > 255 {
            return Err(ShmError::InvalidName);
        }
        return Ok(());
    }
    validate_shm_name(name)
}

/// Return `true` when `/dev/shm` has at least `min_free_bytes` available (or on non-Linux hosts).
pub fn dev_shm_has_headroom(min_free_bytes: usize) -> bool {
    // SAFETY: `libc::statvfs` is POD valid when zero-initialized and `c"/dev/shm"` is a static NUL-terminated C string.
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
        // SAFETY: `cname` is a validated NUL-terminated `/tmp/vtb-*` path `CString`.
        unsafe { libc::unlink(cname.as_ptr()) }
    } else {
        // SAFETY: `cname` is a validated NUL-terminated POSIX SHM name `CString`.
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
pub(crate) fn unique_test_shm_name(prefix: &str) -> String {
    static TEST_SHM_SEQ: AtomicU64 = AtomicU64::new(0);
    let pid = std::process::id();
    let seq = TEST_SHM_SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    format!("{prefix}-{pid:x}-{seq:x}-{nanos:x}")
}

#[cfg(all(test, unix))]
type MagicWaitSignal = Arc<(Mutex<bool>, std::sync::Condvar)>;

#[cfg(all(test, unix))]
std::thread_local! {
    static MAGIC_WAIT_HOOK: std::cell::RefCell<Option<MagicWaitSignal>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(all(test, unix))]
fn notify_magic_wait_hook() {
    MAGIC_WAIT_HOOK.with(|hook| {
        if let Some(pair) = hook.borrow().as_ref() {
            let (lock, cvar) = &**pair;
            let mut waited = lock.lock().unwrap_or_else(|e| e.into_inner());
            *waited = true;
            cvar.notify_all();
        }
    });
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
        // SAFETY: `region` is exclusively owned in this test and mapped for at least 1024 bytes.
        unsafe {
            let slice = region.as_mut_slice();
            slice[0] = 42;
            assert_eq!(region.as_slice()[0], 42);
        }
    }

    #[cfg(unix)]
    #[test]
    fn create_and_open_named() {
        let name = unique_test_shm_name("/vtb-test");
        let mut region = match ShmRegion::create_named(&name, 4096) {
            Ok(region) => region,
            Err(ShmError::Sys(err)) if err.kind() == io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("create_named failed: {err:?}"),
        };
        // SAFETY: `region` is exclusively owned prior to opening `opened`.
        unsafe {
            region.as_mut_slice()[0] = 7;
        }
        let opened = ShmRegion::open_named(&name, 4096).unwrap();
        // SAFETY: `opened` maps the same segment with no concurrent writes during this read.
        unsafe {
            assert_eq!(opened.as_slice()[0], 7);
        }
        drop(region);
        drop(opened);
    }

    #[cfg(unix)]
    #[test]
    fn invalid_name_rejected() {
        for bad in ["vertebra", "/", "/bad/nested/seg", "/tmp/vtb-nested/seg"] {
            let err = ShmRegion::create_named(bad, 1024).unwrap_err();
            assert!(
                matches!(err, ShmError::InvalidName),
                "expected InvalidName for {bad:?}: {err:?}"
            );
            let open_err = ShmRegion::open_named(bad, 1024).unwrap_err();
            assert!(
                matches!(open_err, ShmError::InvalidName),
                "expected InvalidName for {bad:?}: {open_err:?}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn shm_multi_process_roundtrip() {
        if std::env::var("VERTEBRA_SHM_CHILD").is_ok() {
            let name = std::env::var("VERTEBRA_SHM_NAME").unwrap();
            let size: usize = std::env::var("VERTEBRA_SHM_SIZE").unwrap().parse().unwrap();
            let region = ShmRegion::open_named(&name, size).unwrap();
            // SAFETY: parent wrote byte `0` before spawning children and makes no further writes.
            unsafe {
                assert_eq!(region.as_slice()[0], 42);
            }
            return;
        }
        let name = unique_test_shm_name("/vtb-mp");
        let mut region = match ShmRegion::create_named(&name, 4096) {
            Ok(region) => region,
            Err(ShmError::Sys(err)) if err.kind() == io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("create_named failed: {err:?}"),
        };
        // SAFETY: `region` is exclusively owned in the parent before spawning child processes.
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
        let name = unique_test_shm_name("/vtb-ul");
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

        let name = unique_test_shm_name("/vtb-cap");
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
        let tmp_name = unique_test_shm_name("/tmp/vtb-trunc");
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
    #[test]
    fn open_named_waits_for_magic_publication_and_times_out_when_uninitialized() {
        struct HookReset;
        impl Drop for HookReset {
            fn drop(&mut self) {
                MAGIC_WAIT_HOOK.with(|hook| {
                    *hook.borrow_mut() = None;
                });
            }
        }

        let page = os_page_size().unwrap();
        let ready_name = unique_test_shm_name("/vtb-wm");
        let cname_ready = CString::new(ready_name.as_str()).unwrap();
        let total_len = page * 2;
        // SAFETY: `cname_ready` is a valid POSIX SHM name under `/vtb-*`.
        let fd = unsafe {
            let _ = libc::shm_unlink(cname_ready.as_ptr());
            let fd = libc::shm_open(
                cname_ready.as_ptr(),
                libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
                0o600,
            );
            assert!(fd >= 0);
            assert_eq!(libc::ftruncate(fd, total_len as libc::off_t), 0);
            fd
        };
        // SAFETY: `fd` holds `total_len` zeroed bytes.
        let writer_region = unsafe { map_fd_region(fd, total_len) }.unwrap();
        let wait_signal = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        MAGIC_WAIT_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Arc::clone(&wait_signal));
        });
        let _hook_reset = HookReset;
        let publisher_signal = Arc::clone(&wait_signal);
        let publisher = std::thread::spawn(move || {
            let mut reg = writer_region;
            let (lock, cvar) = &*publisher_signal;
            let guard = lock.lock().unwrap_or_else(|e| e.into_inner());
            let _guard = cvar
                .wait_while(guard, |waited| !*waited)
                .unwrap_or_else(|e| e.into_inner());
            // SAFETY: exclusive test writer before `SHM_OWNER_MAGIC` publication.
            unsafe {
                reg.as_mut_slice()[0..4].copy_from_slice(b"OKAY");
                let owner_page = &mut *(reg.as_ptr().add(page) as *mut ShmOwnerPage);
                owner_page
                    .header
                    .init_state
                    .store(SHM_INIT_READY, Ordering::Relaxed);
                owner_page
                    .header
                    .owner_pid
                    .store(std::process::id(), Ordering::Relaxed);
                owner_page
                    .header
                    .owner_generation
                    .store(1, Ordering::Relaxed);
                owner_page.user_len = page as u64;
                owner_page.magic.store(SHM_OWNER_MAGIC, Ordering::Release);
            }
            reg
        });

        let opened = ShmRegion::open_named(&ready_name, page).unwrap();
        assert!(*wait_signal.0.lock().unwrap_or_else(|e| e.into_inner()));
        // SAFETY: writer published `b"OKAY"` before releasing `SHM_OWNER_MAGIC`.
        unsafe {
            assert_eq!(&opened.as_slice()[0..4], b"OKAY");
        }
        drop(opened);
        let _ = shm_unlink_named(&ready_name);
        drop(publisher.join().unwrap());

        let timeout_name = unique_test_shm_name("/vtb-mto");
        let cname_to = CString::new(timeout_name.as_str()).unwrap();
        // SAFETY: creating raw zeroed SHM segment without `SHM_OWNER_MAGIC`.
        unsafe {
            let _ = libc::shm_unlink(cname_to.as_ptr());
            let fd_to = libc::shm_open(
                cname_to.as_ptr(),
                libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
                0o600,
            );
            assert!(fd_to >= 0);
            assert_eq!(libc::ftruncate(fd_to, total_len as libc::off_t), 0);
            libc::close(fd_to);
        }
        let err = ShmRegion::open_named(&timeout_name, page).unwrap_err();
        let _ = shm_unlink_named(&timeout_name);
        assert!(matches!(err, ShmError::InitTimeout));
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
