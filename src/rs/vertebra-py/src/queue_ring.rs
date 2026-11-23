//! Cross-process POSIX shared-memory queue ring and record store.

#[cfg(unix)]
use core::ptr::NonNull;
#[cfg(unix)]
use core::sync::atomic::{AtomicU32, AtomicU64, Ordering};
#[cfg(unix)]
use std::ffi::CString;
#[cfg(unix)]
use std::time::{Duration, Instant};

use pyo3::exceptions::PyValueError;
#[cfg(unix)]
use pyo3::exceptions::{PyOSError, PyOverflowError, PyRuntimeError};
use pyo3::prelude::*;
#[cfg(unix)]
use vertebra_core::sync::{wait_on_address_timeout, wake_all, wake_one};

#[cfg(unix)]
const RING_MAGIC: u64 = u64::from_le_bytes(*b"VTBQRING");
#[cfg(unix)]
const RING_VERSION: u32 = 1;
#[cfg(unix)]
const INIT_PENDING: u32 = 0;
#[cfg(unix)]
const INIT_READY: u32 = 1;
#[cfg(unix)]
const MAX_RECORD_KEY_BYTES: usize = 256;
#[cfg(unix)]
const ATTACH_TIMEOUT_MS: u64 = 2000;

#[cfg(unix)]
#[repr(C, align(64))]
struct RingHeader {
    magic: AtomicU64,
    init_state: AtomicU32,
    version: u32,
    capacity: u64,
    slot_bytes: u64,
    ring_lock: AtomicU32,
    record_lock: AtomicU32,
    notify_epoch: AtomicU32,
    _pad: u32,
    head: AtomicU64,
    tail: AtomicU64,
    record_hwm: AtomicU64,
}

#[cfg(unix)]
#[repr(C)]
struct RingSlotMeta {
    len: u32,
    _pad: u32,
}

#[cfg(unix)]
#[repr(C)]
struct RecordSlotMeta {
    key_hash: u64,
    data_len: u32,
    key_len: u16,
    occupied: u8,
    _pad: u8,
    key: [u8; MAX_RECORD_KEY_BYTES],
}

#[cfg(unix)]
#[derive(Clone, Copy)]
struct RingLayout {
    capacity: usize,
    slot_bytes: usize,
    aligned_slot_bytes: usize,
    ring_meta_off: usize,
    ring_payload_off: usize,
    record_meta_off: usize,
    record_payload_off: usize,
    total_bytes: usize,
}

#[cfg(unix)]
impl RingLayout {
    fn compute(capacity: usize, slot_bytes: usize) -> PyResult<Self> {
        if capacity == 0 || slot_bytes == 0 {
            return Err(PyValueError::new_err(
                "capacity and slot_bytes must be positive",
            ));
        }
        let aligned_slot_bytes = slot_bytes
            .checked_next_multiple_of(8)
            .ok_or_else(|| PyValueError::new_err("slot_bytes overflow"))?;
        let ring_meta_off = core::mem::size_of::<RingHeader>();
        let ring_meta_bytes = capacity
            .checked_mul(core::mem::size_of::<RingSlotMeta>())
            .ok_or_else(|| PyValueError::new_err("ring metadata size overflow"))?;
        let ring_payload_off = ring_meta_off
            .checked_add(ring_meta_bytes)
            .ok_or_else(|| PyValueError::new_err("ring payload offset overflow"))?;
        let payload_area_bytes = capacity
            .checked_mul(aligned_slot_bytes)
            .ok_or_else(|| PyValueError::new_err("ring payload size overflow"))?;
        let record_meta_off = ring_payload_off
            .checked_add(payload_area_bytes)
            .ok_or_else(|| PyValueError::new_err("record metadata offset overflow"))?;
        let record_meta_bytes = capacity
            .checked_mul(core::mem::size_of::<RecordSlotMeta>())
            .ok_or_else(|| PyValueError::new_err("record metadata size overflow"))?;
        let record_payload_off = record_meta_off
            .checked_add(record_meta_bytes)
            .ok_or_else(|| PyValueError::new_err("record payload offset overflow"))?;
        let raw_total = record_payload_off
            .checked_add(payload_area_bytes)
            .ok_or_else(|| PyValueError::new_err("total segment size overflow"))?;
        let page = os_page_size();
        let total_bytes = raw_total
            .checked_next_multiple_of(page)
            .ok_or_else(|| PyValueError::new_err("page-aligned size overflow"))?;
        Ok(Self {
            capacity,
            slot_bytes,
            aligned_slot_bytes,
            ring_meta_off,
            ring_payload_off,
            record_meta_off,
            record_payload_off,
            total_bytes,
        })
    }
}

#[cfg(unix)]
fn os_page_size() -> usize {
    // SAFETY: `sysconf(_SC_PAGESIZE)` queries the OS page size without pointers.
    let raw = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if raw > 0 {
        return raw as usize;
    }
    4096
}

pub(crate) fn derive_shm_path(name: &str) -> PyResult<String> {
    if name.is_empty() || name.contains('\0') {
        return Err(PyValueError::new_err(
            "ShmQueueRing name must be non-empty and contain no NUL bytes",
        ));
    }
    let digest = vertebra_core::broker::stable_hash64(name.as_bytes());
    Ok(format!("/vtbq_{digest:016x}"))
}

#[cfg(unix)]
struct ShmMutex<'a>(&'a AtomicU32);

#[cfg(unix)]
impl<'a> ShmMutex<'a> {
    fn lock(&self) -> ShmMutexGuard<'a> {
        let mut spins = 0u32;
        loop {
            if self
                .0
                .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                return ShmMutexGuard(self.0);
            }
            if spins < 64 {
                spins += 1;
                core::hint::spin_loop();
                continue;
            }
            let cur = self.0.load(Ordering::Relaxed);
            if cur == 2
                || (cur == 1
                    && self
                        .0
                        .compare_exchange(1, 2, Ordering::Relaxed, Ordering::Relaxed)
                        .is_ok())
            {
                let _ = wait_on_address_timeout(self.0, 2, Duration::from_millis(2));
            }
        }
    }
}

#[cfg(unix)]
struct ShmMutexGuard<'a>(&'a AtomicU32);

#[cfg(unix)]
impl Drop for ShmMutexGuard<'_> {
    fn drop(&mut self) {
        if self.0.swap(0, Ordering::Release) == 2 {
            let _ = wake_one(self.0);
        }
    }
}

#[cfg(unix)]
struct MappedRing {
    ptr: NonNull<u8>,
    map_len: usize,
    layout: RingLayout,
}

// SAFETY: `MappedRing` owns a process-wide `MAP_SHARED` mapping synchronized via atomics and futexes.
#[cfg(unix)]
unsafe impl Send for MappedRing {}
// SAFETY: All interior mutations to the mapped SHM region are serialized via atomics and `ShmMutex`.
#[cfg(unix)]
unsafe impl Sync for MappedRing {}

#[cfg(unix)]
impl Drop for MappedRing {
    fn drop(&mut self) {
        // SAFETY: `self.ptr` and `self.map_len` were produced by a successful `libc::mmap`.
        unsafe {
            libc::munmap(self.ptr.as_ptr().cast(), self.map_len);
        }
    }
}

#[cfg(unix)]
impl MappedRing {
    fn header(&self) -> &RingHeader {
        // SAFETY: offset 0 holds an aligned, initialized `RingHeader` within `0..self.map_len`.
        unsafe { &*self.ptr.as_ptr().cast::<RingHeader>() }
    }

    fn ring_meta(&self, idx: usize) -> &RingSlotMeta {
        let off = self.layout.ring_meta_off + idx * core::mem::size_of::<RingSlotMeta>();
        // SAFETY: `idx < self.layout.capacity`, so `off` is within the mapped region and 8-byte aligned.
        unsafe { &*(self.ptr.as_ptr().add(off).cast::<RingSlotMeta>()) }
    }

    #[allow(clippy::mut_from_ref)]
    fn ring_meta_mut(&self, idx: usize) -> &mut RingSlotMeta {
        let off = self.layout.ring_meta_off + idx * core::mem::size_of::<RingSlotMeta>();
        // SAFETY: Caller holds `ring_lock`, ensuring exclusive mutable access to ring slot metadata.
        unsafe { &mut *(self.ptr.as_ptr().add(off).cast::<RingSlotMeta>()) }
    }

    fn ring_payload(&self, idx: usize) -> &[u8] {
        let off = self.layout.ring_payload_off + idx * self.layout.aligned_slot_bytes;
        // SAFETY: `idx < self.layout.capacity`, and caller holds `ring_lock`.
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr().add(off), self.layout.slot_bytes) }
    }

    #[allow(clippy::mut_from_ref)]
    fn ring_payload_mut(&self, idx: usize) -> &mut [u8] {
        let off = self.layout.ring_payload_off + idx * self.layout.aligned_slot_bytes;
        // SAFETY: `idx < self.layout.capacity`, and caller holds `ring_lock`.
        unsafe {
            core::slice::from_raw_parts_mut(self.ptr.as_ptr().add(off), self.layout.slot_bytes)
        }
    }

    fn record_meta(&self, idx: usize) -> &RecordSlotMeta {
        let off = self.layout.record_meta_off + idx * core::mem::size_of::<RecordSlotMeta>();
        // SAFETY: `idx < self.layout.capacity`, so `off` is within the mapped region and 8-byte aligned.
        unsafe { &*(self.ptr.as_ptr().add(off).cast::<RecordSlotMeta>()) }
    }

    #[allow(clippy::mut_from_ref)]
    fn record_meta_mut(&self, idx: usize) -> &mut RecordSlotMeta {
        let off = self.layout.record_meta_off + idx * core::mem::size_of::<RecordSlotMeta>();
        // SAFETY: Caller holds `record_lock`, ensuring exclusive mutable access to record slot metadata.
        unsafe { &mut *(self.ptr.as_ptr().add(off).cast::<RecordSlotMeta>()) }
    }

    fn record_payload(&self, idx: usize) -> &[u8] {
        let off = self.layout.record_payload_off + idx * self.layout.aligned_slot_bytes;
        // SAFETY: `idx < self.layout.capacity`, and caller holds `record_lock`.
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr().add(off), self.layout.slot_bytes) }
    }

    #[allow(clippy::mut_from_ref)]
    fn record_payload_mut(&self, idx: usize) -> &mut [u8] {
        let off = self.layout.record_payload_off + idx * self.layout.aligned_slot_bytes;
        // SAFETY: `idx < self.layout.capacity`, and caller holds `record_lock`.
        unsafe {
            core::slice::from_raw_parts_mut(self.ptr.as_ptr().add(off), self.layout.slot_bytes)
        }
    }

    fn find_record_slot(&self, key_bytes: &[u8], key_hash: u64, hwm: usize) -> Option<usize> {
        let limit = hwm.min(self.layout.capacity);
        for idx in 0..limit {
            let meta = self.record_meta(idx);
            if meta.occupied == 1
                && meta.key_hash == key_hash
                && meta.key_len as usize == key_bytes.len()
                && &meta.key[..key_bytes.len()] == key_bytes
            {
                return Some(idx);
            }
        }
        None
    }

    fn find_free_record_slot(&self) -> Option<usize> {
        (0..self.layout.capacity).find(|&idx| self.record_meta(idx).occupied == 0)
    }
}

#[cfg(unix)]
fn open_or_create_ring(shm_path: &str, capacity: usize, slot_bytes: usize) -> PyResult<MappedRing> {
    let req_layout = RingLayout::compute(capacity, slot_bytes)?;
    let cname = CString::new(shm_path)
        .map_err(|e| PyValueError::new_err(format!("Invalid SHM path: {e}")))?;
    let start = Instant::now();
    let deadline = Duration::from_millis(ATTACH_TIMEOUT_MS);

    loop {
        // SAFETY: `cname` is a NUL-terminated CString; `O_CREAT | O_EXCL` atomically selects one creator.
        let fd = unsafe {
            libc::shm_open(
                cname.as_ptr(),
                libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
                0o600,
            )
        };
        if fd >= 0 {
            // SAFETY: `fd` is exclusively owned here and sized to `req_layout.total_bytes`.
            if unsafe { libc::ftruncate(fd, req_layout.total_bytes as libc::off_t) } != 0 {
                let err = std::io::Error::last_os_error();
                // SAFETY: clean up failed creator FD and SHM path.
                unsafe {
                    libc::close(fd);
                    let _ = libc::shm_unlink(cname.as_ptr());
                }
                return Err(PyOSError::new_err(err.to_string()));
            }
            // SAFETY: `fd` is truncated to `req_layout.total_bytes` bytes.
            let raw_ptr = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    req_layout.total_bytes,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_SHARED,
                    fd,
                    0,
                )
            };
            // SAFETY: `fd` is no longer needed after `mmap`.
            unsafe {
                libc::close(fd);
            }
            if raw_ptr == libc::MAP_FAILED {
                let err = std::io::Error::last_os_error();
                // SAFETY: `cname` was created by this process; clean up on map failure.
                unsafe {
                    let _ = libc::shm_unlink(cname.as_ptr());
                }
                return Err(PyOSError::new_err(err.to_string()));
            }
            let ptr = NonNull::new(raw_ptr.cast::<u8>())
                .ok_or_else(|| PyRuntimeError::new_err("mmap returned null"))?;
            let mapped = MappedRing {
                ptr,
                map_len: req_layout.total_bytes,
                layout: req_layout,
            };
            // SAFETY: offset 0 in newly truncated SHM holds `RingHeader`.
            let hdr = unsafe { &mut *mapped.ptr.as_ptr().cast::<RingHeader>() };
            hdr.init_state.store(INIT_PENDING, Ordering::Relaxed);
            hdr.version = RING_VERSION;
            hdr.capacity = req_layout.capacity as u64;
            hdr.slot_bytes = req_layout.slot_bytes as u64;
            hdr.ring_lock.store(0, Ordering::Relaxed);
            hdr.record_lock.store(0, Ordering::Relaxed);
            hdr.notify_epoch.store(0, Ordering::Relaxed);
            hdr.head.store(0, Ordering::Relaxed);
            hdr.tail.store(0, Ordering::Relaxed);
            hdr.record_hwm.store(0, Ordering::Relaxed);
            hdr.magic.store(RING_MAGIC, Ordering::Release);
            hdr.init_state.store(INIT_READY, Ordering::Release);
            return Ok(mapped);
        }

        let create_err = std::io::Error::last_os_error();
        if create_err.raw_os_error() != Some(libc::EEXIST) {
            return Err(PyOSError::new_err(create_err.to_string()));
        }

        // SAFETY: `cname` is a NUL-terminated CString for opening an existing SHM segment.
        let attach_fd = unsafe { libc::shm_open(cname.as_ptr(), libc::O_RDWR, 0o600) };
        if attach_fd < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ENOENT) {
                if start.elapsed() >= deadline {
                    return Err(PyRuntimeError::new_err(
                        "Timed out attaching to ShmQueueRing",
                    ));
                }
                continue;
            }
            return Err(PyOSError::new_err(err.to_string()));
        }

        // SAFETY: `st` is zero-initialized POD passed to `fstat`.
        let mut st: libc::stat = unsafe { core::mem::zeroed() };
        // SAFETY: `attach_fd` is an open SHM descriptor.
        if unsafe { libc::fstat(attach_fd, &mut st) } != 0 {
            let err = std::io::Error::last_os_error();
            // SAFETY: `attach_fd` is owned here.
            unsafe {
                libc::close(attach_fd);
            }
            return Err(PyOSError::new_err(err.to_string()));
        }
        let file_len = usize::try_from(st.st_size).unwrap_or(0);
        if file_len < core::mem::size_of::<RingHeader>() {
            // SAFETY: `attach_fd` is owned here; creator may still be calling `ftruncate`.
            unsafe {
                libc::close(attach_fd);
            }
            if start.elapsed() >= deadline {
                return Err(PyRuntimeError::new_err(
                    "Timed out waiting for ShmQueueRing initialization",
                ));
            }
            std::thread::sleep(Duration::from_millis(1));
            continue;
        }

        // SAFETY: `attach_fd` is verified to have at least `file_len` bytes.
        let raw_ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                file_len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                attach_fd,
                0,
            )
        };
        // SAFETY: `attach_fd` is no longer needed after `mmap`.
        unsafe {
            libc::close(attach_fd);
        }
        if raw_ptr == libc::MAP_FAILED {
            return Err(PyOSError::new_err(
                std::io::Error::last_os_error().to_string(),
            ));
        }
        let ptr = NonNull::new(raw_ptr.cast::<u8>())
            .ok_or_else(|| PyRuntimeError::new_err("mmap returned null"))?;

        loop {
            // SAFETY: `file_len >= size_of::<RingHeader>()` and `ptr` is mapped for `file_len`.
            let hdr = unsafe { &*ptr.as_ptr().cast::<RingHeader>() };
            if hdr.magic.load(Ordering::Acquire) == RING_MAGIC
                && hdr.init_state.load(Ordering::Acquire) == INIT_READY
            {
                let seg_cap = usize::try_from(hdr.capacity).unwrap_or(0);
                let seg_slot = usize::try_from(hdr.slot_bytes).unwrap_or(0);
                let seg_layout = RingLayout::compute(seg_cap, seg_slot)?;
                if file_len < seg_layout.total_bytes {
                    // SAFETY: `ptr` was mapped with `file_len`.
                    unsafe {
                        libc::munmap(ptr.as_ptr().cast(), file_len);
                    }
                    return Err(PyValueError::new_err(
                        "Existing ShmQueueRing segment is smaller than header layout",
                    ));
                }
                return Ok(MappedRing {
                    ptr,
                    map_len: file_len,
                    layout: seg_layout,
                });
            }
            if start.elapsed() >= deadline {
                // SAFETY: `ptr` was mapped with `file_len`.
                unsafe {
                    libc::munmap(ptr.as_ptr().cast(), file_len);
                }
                return Err(PyRuntimeError::new_err(
                    "Timed out waiting for ShmQueueRing header readiness",
                ));
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

/// Cross-process named shared-memory queue ring and record table with futex wakeups.
#[pyclass(module = "vertebra._vertebra")]
pub struct ShmQueueRing {
    name: String,
    #[cfg(unix)]
    shm_path: String,
    capacity: usize,
    slot_bytes: usize,
    #[cfg(unix)]
    mapping: Option<MappedRing>,
}

impl ShmQueueRing {
    #[cfg(unix)]
    fn ring(&self) -> PyResult<&MappedRing> {
        self.mapping
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("ShmQueueRing is closed"))
    }

    #[cfg(unix)]
    fn wait_inner(&self, ring: &MappedRing, timeout_ms: u64) -> PyResult<bool> {
        let hdr = ring.header();
        let expected = hdr.notify_epoch.load(Ordering::Acquire);
        if hdr.tail.load(Ordering::Acquire) > hdr.head.load(Ordering::Acquire) {
            return Ok(true);
        }
        if timeout_ms == 0 {
            return Ok(false);
        }
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        loop {
            if hdr.notify_epoch.load(Ordering::Acquire) != expected
                || hdr.tail.load(Ordering::Acquire) > hdr.head.load(Ordering::Acquire)
            {
                return Ok(true);
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(false);
            }
            let remaining = deadline - now;
            let woke = wait_on_address_timeout(&hdr.notify_epoch, expected, remaining)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            if !woke {
                return Ok(hdr.notify_epoch.load(Ordering::Acquire) != expected
                    || hdr.tail.load(Ordering::Acquire) > hdr.head.load(Ordering::Acquire));
            }
        }
    }
}

#[pymethods]
impl ShmQueueRing {
    #[new]
    #[pyo3(signature = (name, capacity = 4096, slot_bytes = 4096))]
    fn new(name: &str, capacity: usize, slot_bytes: usize) -> PyResult<Self> {
        let shm_path = derive_shm_path(name)?;
        #[cfg(unix)]
        {
            let mapping = open_or_create_ring(&shm_path, capacity, slot_bytes)?;
            let actual_cap = mapping.layout.capacity;
            let actual_slot = mapping.layout.slot_bytes;
            Ok(Self {
                name: name.to_owned(),
                shm_path,
                capacity: actual_cap,
                slot_bytes: actual_slot,
                mapping: Some(mapping),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = (capacity, slot_bytes, shm_path);
            Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "ShmQueueRing is only supported on Unix platforms",
            ))
        }
    }

    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    #[getter]
    fn capacity(&self) -> usize {
        self.capacity
    }

    #[getter]
    fn slot_bytes(&self) -> usize {
        self.slot_bytes
    }

    fn push(&self, payload: &[u8]) -> PyResult<usize> {
        #[cfg(unix)]
        {
            let ring = self.ring()?;
            if payload.len() > ring.layout.slot_bytes {
                return Err(PyValueError::new_err(format!(
                    "Payload length {} exceeds slot_bytes {}",
                    payload.len(),
                    ring.layout.slot_bytes
                )));
            }
            let hdr = ring.header();
            let depth = {
                let _guard = ShmMutex(&hdr.ring_lock).lock();
                let head = hdr.head.load(Ordering::Acquire);
                let tail = hdr.tail.load(Ordering::Acquire);
                if tail.saturating_sub(head) as usize >= ring.layout.capacity {
                    return Err(PyOverflowError::new_err("ShmQueueRing is full"));
                }
                let idx = (tail as usize) % ring.layout.capacity;
                ring.ring_meta_mut(idx).len = payload.len() as u32;
                ring.ring_payload_mut(idx)[..payload.len()].copy_from_slice(payload);
                let new_tail = tail + 1;
                hdr.tail.store(new_tail, Ordering::Release);
                new_tail.saturating_sub(head) as usize
            };
            hdr.notify_epoch.fetch_add(1, Ordering::AcqRel);
            let _ = wake_one(&hdr.notify_epoch);
            Ok(depth)
        }
        #[cfg(not(unix))]
        {
            let _ = payload;
            Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "ShmQueueRing is only supported on Unix platforms",
            ))
        }
    }

    fn pop(&self) -> PyResult<Option<Vec<u8>>> {
        #[cfg(unix)]
        {
            let ring = self.ring()?;
            let hdr = ring.header();
            let _guard = ShmMutex(&hdr.ring_lock).lock();
            let head = hdr.head.load(Ordering::Acquire);
            let tail = hdr.tail.load(Ordering::Acquire);
            if head >= tail {
                return Ok(None);
            }
            let idx = (head as usize) % ring.layout.capacity;
            let len = ring.ring_meta(idx).len as usize;
            let clamped = len.min(ring.layout.slot_bytes);
            let out = ring.ring_payload(idx)[..clamped].to_vec();
            hdr.head.store(head + 1, Ordering::Release);
            Ok(Some(out))
        }
        #[cfg(not(unix))]
        {
            Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "ShmQueueRing is only supported on Unix platforms",
            ))
        }
    }

    fn put_record(&self, task_id: &str, data: &[u8]) -> PyResult<bool> {
        #[cfg(unix)]
        {
            let key_bytes = task_id.as_bytes();
            if key_bytes.is_empty() || key_bytes.len() > MAX_RECORD_KEY_BYTES {
                return Err(PyValueError::new_err(format!(
                    "Record key length must be 1..={MAX_RECORD_KEY_BYTES} bytes"
                )));
            }
            let ring = self.ring()?;
            if data.len() > ring.layout.slot_bytes {
                return Err(PyValueError::new_err(format!(
                    "Record data length {} exceeds slot_bytes {}",
                    data.len(),
                    ring.layout.slot_bytes
                )));
            }
            let key_hash = vertebra_core::broker::stable_hash64(key_bytes);
            let hdr = ring.header();
            let _guard = ShmMutex(&hdr.record_lock).lock();
            let hwm = hdr.record_hwm.load(Ordering::Acquire) as usize;
            let slot_idx = if let Some(existing) = ring.find_record_slot(key_bytes, key_hash, hwm) {
                existing
            } else if let Some(free) = ring.find_free_record_slot() {
                if free + 1 > hwm {
                    hdr.record_hwm.store((free + 1) as u64, Ordering::Release);
                }
                free
            } else {
                return Err(PyOverflowError::new_err(
                    "ShmQueueRing record table is full",
                ));
            };
            ring.record_payload_mut(slot_idx)[..data.len()].copy_from_slice(data);
            let meta = ring.record_meta_mut(slot_idx);
            meta.key_hash = key_hash;
            meta.key_len = key_bytes.len() as u16;
            meta.data_len = data.len() as u32;
            meta.key[..key_bytes.len()].copy_from_slice(key_bytes);
            meta.occupied = 1;
            Ok(true)
        }
        #[cfg(not(unix))]
        {
            let _ = (task_id, data);
            Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "ShmQueueRing is only supported on Unix platforms",
            ))
        }
    }

    #[pyo3(signature = (task_id, expected = None, data = None))]
    fn cas_record(
        &self,
        task_id: &str,
        expected: Option<&[u8]>,
        data: Option<&[u8]>,
    ) -> PyResult<bool> {
        #[cfg(unix)]
        {
            let key_bytes = task_id.as_bytes();
            if key_bytes.is_empty() || key_bytes.len() > MAX_RECORD_KEY_BYTES {
                return Err(PyValueError::new_err(format!(
                    "Record key length must be 1..={MAX_RECORD_KEY_BYTES} bytes"
                )));
            }
            let ring = self.ring()?;
            if let Some(new_data) = data
                && new_data.len() > ring.layout.slot_bytes
            {
                return Err(PyValueError::new_err(format!(
                    "Record data length {} exceeds slot_bytes {}",
                    new_data.len(),
                    ring.layout.slot_bytes
                )));
            }
            let key_hash = vertebra_core::broker::stable_hash64(key_bytes);
            let hdr = ring.header();
            let _guard = ShmMutex(&hdr.record_lock).lock();
            let hwm = hdr.record_hwm.load(Ordering::Acquire) as usize;
            let existing = ring.find_record_slot(key_bytes, key_hash, hwm);

            match (existing, expected, data) {
                (None, Some(_), _) => Ok(false),
                (None, None, None) => Ok(false),
                (None, None, Some(new_data)) => {
                    let Some(free) = ring.find_free_record_slot() else {
                        return Err(PyOverflowError::new_err(
                            "ShmQueueRing record table is full",
                        ));
                    };
                    if free + 1 > hwm {
                        hdr.record_hwm.store((free + 1) as u64, Ordering::Release);
                    }
                    ring.record_payload_mut(free)[..new_data.len()].copy_from_slice(new_data);
                    let meta = ring.record_meta_mut(free);
                    meta.key_hash = key_hash;
                    meta.key_len = key_bytes.len() as u16;
                    meta.data_len = new_data.len() as u32;
                    meta.key[..key_bytes.len()].copy_from_slice(key_bytes);
                    meta.occupied = 1;
                    Ok(true)
                }
                (Some(_), None, Some(_)) => Ok(false),
                (Some(idx), None, None) => {
                    ring.record_meta_mut(idx).occupied = 0;
                    Ok(true)
                }
                (Some(idx), Some(exp), maybe_new) => {
                    let meta = ring.record_meta(idx);
                    let cur_len = (meta.data_len as usize).min(ring.layout.slot_bytes);
                    if &ring.record_payload(idx)[..cur_len] != exp {
                        return Ok(false);
                    }
                    match maybe_new {
                        Some(new_data) => {
                            ring.record_payload_mut(idx)[..new_data.len()]
                                .copy_from_slice(new_data);
                            ring.record_meta_mut(idx).data_len = new_data.len() as u32;
                        }
                        None => {
                            ring.record_meta_mut(idx).occupied = 0;
                        }
                    }
                    Ok(true)
                }
            }
        }
        #[cfg(not(unix))]
        {
            let _ = (task_id, expected, data);
            Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "ShmQueueRing is only supported on Unix platforms",
            ))
        }
    }

    fn get_record(&self, task_id: &str) -> PyResult<Option<Vec<u8>>> {
        #[cfg(unix)]
        {
            let key_bytes = task_id.as_bytes();
            if key_bytes.is_empty() || key_bytes.len() > MAX_RECORD_KEY_BYTES {
                return Ok(None);
            }
            let ring = self.ring()?;
            let key_hash = vertebra_core::broker::stable_hash64(key_bytes);
            let hdr = ring.header();
            let _guard = ShmMutex(&hdr.record_lock).lock();
            let hwm = hdr.record_hwm.load(Ordering::Acquire) as usize;
            let Some(idx) = ring.find_record_slot(key_bytes, key_hash, hwm) else {
                return Ok(None);
            };
            let len = (ring.record_meta(idx).data_len as usize).min(ring.layout.slot_bytes);
            Ok(Some(ring.record_payload(idx)[..len].to_vec()))
        }
        #[cfg(not(unix))]
        {
            let _ = task_id;
            Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "ShmQueueRing is only supported on Unix platforms",
            ))
        }
    }

    fn delete_record(&self, task_id: &str) -> PyResult<bool> {
        #[cfg(unix)]
        {
            let key_bytes = task_id.as_bytes();
            if key_bytes.is_empty() || key_bytes.len() > MAX_RECORD_KEY_BYTES {
                return Ok(false);
            }
            let ring = self.ring()?;
            let key_hash = vertebra_core::broker::stable_hash64(key_bytes);
            let hdr = ring.header();
            let _guard = ShmMutex(&hdr.record_lock).lock();
            let hwm = hdr.record_hwm.load(Ordering::Acquire) as usize;
            let Some(idx) = ring.find_record_slot(key_bytes, key_hash, hwm) else {
                return Ok(false);
            };
            ring.record_meta_mut(idx).occupied = 0;
            Ok(true)
        }
        #[cfg(not(unix))]
        {
            let _ = task_id;
            Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "ShmQueueRing is only supported on Unix platforms",
            ))
        }
    }

    #[pyo3(signature = (prefix = ""))]
    fn list_records(&self, prefix: &str) -> PyResult<Vec<(String, Vec<u8>)>> {
        #[cfg(unix)]
        {
            let prefix_bytes = prefix.as_bytes();
            let ring = self.ring()?;
            let hdr = ring.header();
            let _guard = ShmMutex(&hdr.record_lock).lock();
            let hwm = (hdr.record_hwm.load(Ordering::Acquire) as usize).min(ring.layout.capacity);
            let mut out = Vec::new();
            for idx in 0..hwm {
                let meta = ring.record_meta(idx);
                if meta.occupied != 1 {
                    continue;
                }
                let klen = (meta.key_len as usize).min(MAX_RECORD_KEY_BYTES);
                let key_slice = &meta.key[..klen];
                if !prefix_bytes.is_empty() && !key_slice.starts_with(prefix_bytes) {
                    continue;
                }
                let Ok(key_str) = core::str::from_utf8(key_slice) else {
                    continue;
                };
                let dlen = (meta.data_len as usize).min(ring.layout.slot_bytes);
                out.push((
                    key_str.to_owned(),
                    ring.record_payload(idx)[..dlen].to_vec(),
                ));
            }
            Ok(out)
        }
        #[cfg(not(unix))]
        {
            let _ = prefix;
            Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "ShmQueueRing is only supported on Unix platforms",
            ))
        }
    }

    #[pyo3(signature = (timeout_ms = 100))]
    fn wait(&self, py: Python<'_>, timeout_ms: u64) -> PyResult<bool> {
        #[cfg(unix)]
        {
            let ring = self.ring()?;
            py.detach(|| self.wait_inner(ring, timeout_ms))
        }
        #[cfg(not(unix))]
        {
            let _ = (py, timeout_ms);
            Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "ShmQueueRing is only supported on Unix platforms",
            ))
        }
    }

    #[pyo3(signature = (wake_all_waiters = false))]
    fn wake(&self, wake_all_waiters: bool) -> PyResult<()> {
        #[cfg(unix)]
        {
            let ring = self.ring()?;
            let hdr = ring.header();
            hdr.notify_epoch.fetch_add(1, Ordering::AcqRel);
            if wake_all_waiters {
                wake_all(&hdr.notify_epoch).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            } else {
                wake_one(&hdr.notify_epoch).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            }
            Ok(())
        }
        #[cfg(not(unix))]
        {
            let _ = wake_all_waiters;
            Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "ShmQueueRing is only supported on Unix platforms",
            ))
        }
    }

    fn pending_len(&self) -> PyResult<usize> {
        #[cfg(unix)]
        {
            let ring = self.ring()?;
            let hdr = ring.header();
            let head = hdr.head.load(Ordering::Acquire);
            let tail = hdr.tail.load(Ordering::Acquire);
            Ok(tail.saturating_sub(head) as usize)
        }
        #[cfg(not(unix))]
        {
            Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "ShmQueueRing is only supported on Unix platforms",
            ))
        }
    }

    fn close(&mut self) -> PyResult<()> {
        #[cfg(unix)]
        {
            let _ = self.shm_path.len();
            self.mapping.take();
            Ok(())
        }
        #[cfg(not(unix))]
        {
            Ok(())
        }
    }

    #[staticmethod]
    fn unlink(name: &str) -> PyResult<()> {
        let shm_path = derive_shm_path(name)?;
        #[cfg(unix)]
        {
            let cname = CString::new(shm_path)
                .map_err(|e| PyValueError::new_err(format!("Invalid SHM path: {e}")))?;
            // SAFETY: `cname` is a validated NUL-terminated POSIX SHM path `CString`.
            let rc = unsafe { libc::shm_unlink(cname.as_ptr()) };
            if rc != 0 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() != Some(libc::ENOENT) {
                    return Err(PyOSError::new_err(err.to_string()));
                }
            }
            Ok(())
        }
        #[cfg(not(unix))]
        {
            let _ = shm_path;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::sync::Arc;
    #[cfg(unix)]
    use std::thread;

    #[cfg(unix)]
    fn unique_name(tag: &str) -> String {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        format!(
            "vertebra-queue-ring-test-{tag}-{pid}-{now}-long-suffix-over-31-bytes",
            pid = std::process::id()
        )
    }

    #[test]
    fn shm_path_is_bounded_and_deterministic() {
        let long_name = format!("vertebra-long-ring-name-{}", "x".repeat(80));
        let path_a = derive_shm_path(&long_name).expect("derive");
        let path_b = derive_shm_path(&long_name).expect("derive");
        assert_eq!(path_a, path_b);
        assert!(path_a.starts_with("/vtbq_"));
        assert!(path_a.len() <= 31);
        assert!(derive_shm_path("").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn ring_persists_across_drop_and_supports_push_pop_records_and_wait_wake() {
        let name = unique_name("persist");
        {
            let mut ring = ShmQueueRing::new(&name, 8, 64).expect("create");
            assert_eq!(ring.name(), name);
            assert_eq!(ring.pending_len().expect("len"), 0);
            assert!(ring.push(b"msg-1").expect("push") == 1);
            assert!(ring.put_record("task:1", b"payload-1").expect("put"));
            assert!(ring.put_record("other:2", b"payload-2").expect("put"));
            assert!(ring.push(&[0u8; 65]).is_err());
            ring.close().expect("close");
        }

        {
            let ring = Arc::new(ShmQueueRing::new(&name, 8, 64).expect("reattach"));
            assert_eq!(ring.pending_len().expect("len"), 1);
            assert_eq!(
                ring.pop().expect("pop").as_deref(),
                Some(b"msg-1".as_slice())
            );
            assert_eq!(ring.pending_len().expect("len"), 0);
            assert_eq!(
                ring.get_record("task:1").expect("get").as_deref(),
                Some(b"payload-1".as_slice())
            );
            let listed = ring.list_records("task:").expect("list");
            assert_eq!(listed, vec![("task:1".to_owned(), b"payload-1".to_vec())]);
            assert!(
                !ring
                    .cas_record("task:1", Some(b"wrong"), Some(b"nope"))
                    .expect("cas")
            );
            assert!(ring.delete_record("task:1").expect("delete"));
            assert_eq!(ring.get_record("task:1").expect("get"), None);
        }
        ShmQueueRing::unlink(&name).expect("unlink");
    }

    #[cfg(unix)]
    #[test]
    fn ring_cas_overflow_and_futex_wait_wake() {
        let name = unique_name("futex");
        let ring = Arc::new(ShmQueueRing::new(&name, 2, 16).expect("create"));
        assert!(ring.cas_record("k1", None, Some(b"v1")).expect("insert"));
        assert!(!ring.cas_record("k1", None, Some(b"v2")).expect("dup"));
        assert!(
            !ring
                .cas_record("k1", Some(b"bad"), Some(b"v2"))
                .expect("bad")
        );
        assert!(ring.cas_record("k1", Some(b"v1"), Some(b"v2")).expect("ok"));
        assert_eq!(
            ring.get_record("k1").expect("get").as_deref(),
            Some(b"v2".as_slice())
        );
        assert!(ring.cas_record("k1", Some(b"v2"), None).expect("del"));
        assert_eq!(ring.get_record("k1").expect("get"), None);

        assert_eq!(ring.push(b"a").expect("p1"), 1);
        assert_eq!(ring.push(b"b").expect("p2"), 2);
        assert!(ring.push(b"c").is_err());
        assert_eq!(ring.pop().expect("pop1").as_deref(), Some(b"a".as_slice()));
        assert_eq!(ring.pop().expect("pop2").as_deref(), Some(b"b".as_slice()));

        assert!(
            !ring
                .wait_inner(ring.ring().expect("mapped"), 20)
                .expect("idle wait")
        );

        let waiter_ring = Arc::clone(&ring);
        let handle = thread::spawn(move || {
            waiter_ring
                .wait_inner(waiter_ring.ring().expect("mapped"), 1000)
                .expect("wait")
        });
        thread::sleep(Duration::from_millis(10));
        ring.push(b"wake").expect("push wake");
        assert!(handle.join().expect("join"));

        ShmQueueRing::unlink(&name).expect("unlink");
    }
}
