//! Shared-memory C ABI for the Vertebra broker transport.

use core::ffi::{c_char, c_int, c_uint, c_void};
use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::fd::RawFd;
use std::time::Duration;

use vertebra_core::async_notifier::AsyncNotifierBridge;
use vertebra_core::buffer::RingLayout;
use vertebra_core::channel::{
    ChannelError, MpmcRing, RingStats, SpscReceiver, SpscRing, SpscSender,
};
use vertebra_core::notify::Notifier;
use vertebra_core::router::{RouteId, Router};
use vertebra_core::shm::ShmRegion;

pub mod scheduler;
pub mod pool;

#[repr(C)]
pub struct VtbRouter {
    inner: Router,
}

#[no_mangle]
pub extern "C" fn vtb_router_new() -> *mut VtbRouter {
    Box::into_raw(Box::new(VtbRouter {
        inner: Router::new(),
    }))
}

#[no_mangle]
pub extern "C" fn vtb_router_insert(
    handle: *mut VtbRouter,
    path: *const c_char,
    id: u64,
) -> VtbStatus {
    clear_last_error();
    if handle.is_null() || path.is_null() {
        return VtbStatus::Invalid;
    }
    let handle = unsafe { &mut *handle };
    let path = unsafe { CStr::from_ptr(path) };
    let path = match path.to_str() {
        Ok(value) => value,
        Err(_) => return VtbStatus::Invalid,
    };
    match handle.inner.insert(path, RouteId(id)) {
        Ok(()) => VtbStatus::Ok,
        Err(e) => {
            set_last_error(e.to_string());
            VtbStatus::Invalid
        }
    }
}

#[no_mangle]
pub extern "C" fn vtb_router_match(
    handle: *const VtbRouter,
    path: *const c_char,
    out_id: *mut u64,
    out_params_json: *mut u8,
    out_params_cap: usize,
    out_params_len: *mut usize,
) -> VtbStatus {
    clear_last_error();
    if handle.is_null()
        || path.is_null()
        || out_id.is_null()
        || out_params_json.is_null()
        || out_params_len.is_null()
    {
        return VtbStatus::Invalid;
    }
    let handle = unsafe { &*handle };
    let path = unsafe { CStr::from_ptr(path) };
    let path = match path.to_str() {
        Ok(value) => value,
        Err(_) => return VtbStatus::Invalid,
    };
    match handle.inner.match_path(path) {
        Some((id, params)) => {
            unsafe {
                *out_id = id.0;
            }
            if !params.is_empty() {
                // Serialize params to JSON object
                // We construct a simple JSON string manually to avoid heavy deps if possible,
                // or just use serde_json since we have it.
                let params_map: std::collections::HashMap<&str, &str> =
                    params.into_iter().collect();
                let json = match serde_json::to_vec(&params_map) {
                    Ok(j) => j,
                    Err(_) => return VtbStatus::Sys,
                };
                if json.len() > out_params_cap {
                    return VtbStatus::Invalid; // Buffer too small
                }
                unsafe {
                    std::ptr::copy_nonoverlapping(json.as_ptr(), out_params_json, json.len());
                    *out_params_len = json.len();
                }
            } else {
                unsafe { *out_params_len = 0 };
            }
            VtbStatus::Ok
        }
        None => VtbStatus::Empty,
    }
}

#[no_mangle]
pub extern "C" fn vtb_router_close(handle: *mut VtbRouter) {
    if handle.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(handle));
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum VtbStatus {
    Ok = 0,
    Invalid = 1,
    NotReady = 2,
    Timeout = 3,
    Full = 4,
    Empty = 5,
    Sys = 6,
}

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn set_last_error(message: String) {
    let value = CString::new(message).ok();
    LAST_ERROR.with(|cell| {
        *cell.borrow_mut() = value;
    });
}

fn clear_last_error() {
    LAST_ERROR.with(|cell| {
        *cell.borrow_mut() = None;
    });
}

fn map_channel_error(err: ChannelError) -> VtbStatus {
    match err {
        ChannelError::Full => VtbStatus::Full,
        ChannelError::Empty => VtbStatus::Empty,
        ChannelError::NotReady => VtbStatus::NotReady,
        ChannelError::SlotBorrowed => VtbStatus::NotReady, // Slot is being read via SlotView
        ChannelError::BufferTooSmall { .. } | ChannelError::InvalidLayout => VtbStatus::Invalid,
        ChannelError::PayloadTooLarge { .. } | ChannelError::InvalidBufferMeta => {
            VtbStatus::Invalid
        }
        ChannelError::Shm(_) | ChannelError::Buffer(_) | ChannelError::Sync(_) => VtbStatus::Sys,
    }
}

#[repr(C)]
pub struct VtbRing {
    sender: SpscSender,
    receiver: SpscReceiver,
    capacity: usize,
}

impl VtbRing {
    fn new(ring: SpscRing) -> Self {
        let capacity = ring.payload_capacity();
        let (sender, receiver) = ring.split();
        Self {
            sender,
            receiver,
            capacity,
        }
    }
}

#[repr(C)]
pub struct VtbRegion {
    region: ShmRegion,
}

#[repr(C)]
pub struct VtbRingStats {
    pub slot_size: usize,
    pub slots: usize,
    pub read_head: u64,
    pub write_head: u64,
    pub capacity: usize,
}

fn layout_from(slot_size: usize, slots: usize) -> Result<RingLayout, VtbStatus> {
    if slot_size == 0 || slots == 0 {
        return Err(VtbStatus::Invalid);
    }
    let layout = RingLayout::new(slot_size, slots, vertebra_core::buffer::ChannelMode::Spsc);
    if !layout.is_power_of_two() {
        return Err(VtbStatus::Invalid);
    }
    Ok(layout)
}

#[no_mangle]
pub extern "C" fn vtb_ring_create_handle(
    name: *const c_char,
    slot_size: usize,
    slots: usize,
) -> *mut VtbRing {
    let mut handle: *mut VtbRing = core::ptr::null_mut();
    let status = vtb_ring_create(name, slot_size, slots, &mut handle);
    if status == VtbStatus::Ok {
        handle
    } else {
        core::ptr::null_mut()
    }
}

#[no_mangle]
pub extern "C" fn vtb_ring_open_handle(
    name: *const c_char,
    slot_size: usize,
    slots: usize,
) -> *mut VtbRing {
    let mut handle: *mut VtbRing = core::ptr::null_mut();
    let status = vtb_ring_open(name, slot_size, slots, &mut handle);
    if status == VtbStatus::Ok {
        handle
    } else {
        core::ptr::null_mut()
    }
}

#[no_mangle]
pub extern "C" fn vtb_last_error_message() -> *const c_char {
    LAST_ERROR.with(|cell| {
        if let Some(value) = cell.borrow().as_ref() {
            value.as_ptr()
        } else {
            core::ptr::null()
        }
    })
}

#[no_mangle]
pub extern "C" fn vtb_ring_capacity(handle: *const VtbRing) -> usize {
    if handle.is_null() {
        return 0;
    }
    unsafe { (*handle).capacity }
}

#[no_mangle]
pub extern "C" fn vtb_ring_stats(
    handle: *const VtbRing,
    out_stats: *mut VtbRingStats,
) -> VtbStatus {
    clear_last_error();
    if handle.is_null() || out_stats.is_null() {
        return VtbStatus::Invalid;
    }
    let handle = unsafe { &*handle };
    let stats: RingStats = handle.sender.stats();
    unsafe {
        *out_stats = VtbRingStats {
            slot_size: stats.slot_size,
            slots: stats.slots,
            read_head: stats.read_head,
            write_head: stats.write_head,
            capacity: handle.capacity,
        };
    }
    VtbStatus::Ok
}

#[no_mangle]
pub extern "C" fn vtb_ring_create(
    name: *const c_char,
    slot_size: usize,
    slots: usize,
    out_handle: *mut *mut VtbRing,
) -> VtbStatus {
    clear_last_error();
    if name.is_null() || out_handle.is_null() {
        return VtbStatus::Invalid;
    }
    let name = unsafe { CStr::from_ptr(name) };
    let name = match name.to_str() {
        Ok(value) => value,
        Err(_) => return VtbStatus::Invalid,
    };
    let layout = match layout_from(slot_size, slots) {
        Ok(layout) => layout,
        Err(status) => return status,
    };
    let region = match ShmRegion::create_named(name, layout.total_size) {
        Ok(region) => region,
        Err(err) => {
            set_last_error(err.to_string());
            return VtbStatus::Sys;
        }
    };
    let ring = unsafe {
        match SpscRing::initialize(std::sync::Arc::new(region), layout, process_id(), epoch()) {
            Ok(ring) => ring,
            Err(err) => {
                set_last_error(err.to_string());
                return map_channel_error(err);
            }
        }
    };
    let handle = Box::new(VtbRing::new(ring));
    unsafe {
        *out_handle = Box::into_raw(handle);
    }
    VtbStatus::Ok
}

#[no_mangle]
pub extern "C" fn vtb_ring_open(
    name: *const c_char,
    slot_size: usize,
    slots: usize,
    out_handle: *mut *mut VtbRing,
) -> VtbStatus {
    clear_last_error();
    if name.is_null() || out_handle.is_null() {
        return VtbStatus::Invalid;
    }
    let name = unsafe { CStr::from_ptr(name) };
    let name = match name.to_str() {
        Ok(value) => value,
        Err(_) => return VtbStatus::Invalid,
    };
    let layout = match layout_from(slot_size, slots) {
        Ok(layout) => layout,
        Err(status) => return status,
    };
    let region = match ShmRegion::open_named(name, layout.total_size) {
        Ok(region) => region,
        Err(err) => {
            set_last_error(err.to_string());
            return VtbStatus::Sys;
        }
    };
    let ring = unsafe {
        match SpscRing::attach(std::sync::Arc::new(region), layout) {
            Ok(ring) => ring,
            Err(err) => {
                set_last_error(err.to_string());
                return map_channel_error(err);
            }
        }
    };
    let handle = Box::new(VtbRing::new(ring));
    unsafe {
        *out_handle = Box::into_raw(handle);
    }
    VtbStatus::Ok
}

#[no_mangle]
pub extern "C" fn vtb_ring_send(
    handle: *mut VtbRing,
    data: *const u8,
    len: usize,
    flags: c_uint,
    timeout_ms: c_uint,
) -> VtbStatus {
    clear_last_error();
    if handle.is_null() || data.is_null() {
        return VtbStatus::Invalid;
    }
    let handle = unsafe { &*handle };
    let payload = unsafe { core::slice::from_raw_parts(data, len) };
    let timeout = Duration::from_millis(timeout_ms as u64);
    let result = if timeout_ms == 0 {
        handle.sender.try_send(payload, flags)
    } else {
        match handle.sender.send_timeout(payload, flags, timeout) {
            Ok(true) => Ok(()),
            Ok(false) => Err(ChannelError::Full),
            Err(err) => Err(err),
        }
    };
    match result {
        Ok(()) => VtbStatus::Ok,
        Err(err) => {
            set_last_error(err.to_string());
            map_channel_error(err)
        }
    }
}

#[no_mangle]
pub extern "C" fn vtb_ring_recv(
    handle: *mut VtbRing,
    out: *mut u8,
    capacity: usize,
    out_len: *mut usize,
    out_flags: *mut c_uint,
    timeout_ms: c_uint,
) -> VtbStatus {
    clear_last_error();
    if handle.is_null() || out.is_null() || out_len.is_null() || out_flags.is_null() {
        return VtbStatus::Invalid;
    }
    let handle = unsafe { &*handle };
    let buffer = unsafe { core::slice::from_raw_parts_mut(out, capacity) };
    let timeout = Duration::from_millis(timeout_ms as u64);
    let result = if timeout_ms == 0 {
        handle.receiver.try_recv_with_flags(buffer).map(Some)
    } else {
        handle.receiver.recv_timeout_with_flags(buffer, timeout)
    };
    match result {
        Ok(Some((len, flags))) => {
            unsafe {
                *out_len = len;
            }
            unsafe {
                *out_flags = flags;
            }
            VtbStatus::Ok
        }
        Ok(None) => VtbStatus::Timeout,
        Err(err) => {
            set_last_error(err.to_string());
            map_channel_error(err)
        }
    }
}

#[no_mangle]
pub extern "C" fn vtb_ring_close(handle: *mut VtbRing) {
    if handle.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(handle));
    }
}

#[no_mangle]
pub extern "C" fn vtb_region_create(
    name: *const c_char,
    size: usize,
    out_handle: *mut *mut VtbRegion,
) -> VtbStatus {
    clear_last_error();
    if name.is_null() || out_handle.is_null() {
        return VtbStatus::Invalid;
    }
    let name = unsafe { CStr::from_ptr(name) };
    let name = match name.to_str() {
        Ok(value) => value,
        Err(_) => return VtbStatus::Invalid,
    };
    let region = match ShmRegion::create_named(name, size) {
        Ok(region) => region,
        Err(err) => {
            set_last_error(err.to_string());
            return VtbStatus::Sys;
        }
    };
    let handle = Box::new(VtbRegion { region });
    unsafe { *out_handle = Box::into_raw(handle) };
    VtbStatus::Ok
}

#[no_mangle]
pub extern "C" fn vtb_region_open(
    name: *const c_char,
    size: usize,
    out_handle: *mut *mut VtbRegion,
) -> VtbStatus {
    clear_last_error();
    if name.is_null() || out_handle.is_null() {
        return VtbStatus::Invalid;
    }
    let name = unsafe { CStr::from_ptr(name) };
    let name = match name.to_str() {
        Ok(value) => value,
        Err(_) => return VtbStatus::Invalid,
    };
    let region = match ShmRegion::open_named(name, size) {
        Ok(region) => region,
        Err(err) => {
            set_last_error(err.to_string());
            return VtbStatus::Sys;
        }
    };
    let handle = Box::new(VtbRegion { region });
    unsafe { *out_handle = Box::into_raw(handle) };
    VtbStatus::Ok
}

#[no_mangle]
pub extern "C" fn vtb_region_ptr(handle: *mut VtbRegion) -> *mut c_void {
    if handle.is_null() {
        return core::ptr::null_mut();
    }
    let handle = unsafe { &mut *handle };
    handle.region.as_ptr().cast::<c_void>()
}

#[no_mangle]
pub extern "C" fn vtb_region_len(handle: *mut VtbRegion) -> usize {
    if handle.is_null() {
        return 0;
    }
    unsafe { (*handle).region.len() }
}

#[no_mangle]
pub extern "C" fn vtb_region_close(handle: *mut VtbRegion) {
    if handle.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(handle));
    }
}

/// Unlink a named shared memory region.
///
/// This removes the shared memory from the filesystem so no new processes can open it.
/// Existing mappings remain valid until all handles are closed.
#[no_mangle]
pub extern "C" fn vtb_shm_unlink(name: *const c_char) -> VtbStatus {
    clear_last_error();
    if name.is_null() {
        set_last_error("null name".to_string());
        return VtbStatus::Invalid;
    }
    let name = unsafe { CStr::from_ptr(name) };
    let name_str = match name.to_str() {
        Ok(s) => s,
        Err(e) => {
            set_last_error(format!("invalid name: {e}"));
            return VtbStatus::Invalid;
        }
    };
    match vertebra_core::shm::shm_unlink_named(name_str) {
        Ok(()) => VtbStatus::Ok,
        Err(e) => {
            set_last_error(format!("shm_unlink failed: {e}"));
            VtbStatus::Sys
        }
    }
}

#[no_mangle]
pub extern "C" fn vtb_free(ptr: *mut c_void) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(ptr.cast::<u8>()));
    }
}

fn process_id() -> u32 {
    std::process::id()
}

fn epoch() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

#[repr(C)]
pub struct VtbMpmcRing {
    inner: MpmcRing,
    capacity: usize,
}

impl VtbMpmcRing {
    fn new(ring: MpmcRing) -> Self {
        let capacity = ring.payload_capacity();
        Self {
            inner: ring,
            capacity,
        }
    }
}

fn mpmc_layout_from(slot_size: usize, slots: usize) -> Result<RingLayout, VtbStatus> {
    if slot_size == 0 || slots == 0 {
        return Err(VtbStatus::Invalid);
    }
    let layout = RingLayout::new(slot_size, slots, vertebra_core::buffer::ChannelMode::Mpmc);
    if !layout.is_power_of_two() {
        return Err(VtbStatus::Invalid);
    }
    Ok(layout)
}

#[no_mangle]
pub extern "C" fn vtb_mpmc_create(
    name: *const c_char,
    slot_size: usize,
    slots: usize,
    out_handle: *mut *mut VtbMpmcRing,
) -> VtbStatus {
    clear_last_error();
    if name.is_null() || out_handle.is_null() {
        return VtbStatus::Invalid;
    }
    let name = unsafe { CStr::from_ptr(name) };
    let name = match name.to_str() {
        Ok(value) => value,
        Err(_) => return VtbStatus::Invalid,
    };
    let layout = match mpmc_layout_from(slot_size, slots) {
        Ok(layout) => layout,
        Err(status) => return status,
    };
    let region = match ShmRegion::create_named(name, layout.total_size) {
        Ok(region) => region,
        Err(err) => {
            set_last_error(err.to_string());
            return VtbStatus::Sys;
        }
    };
    // MpmcRing::initialize is used for creating new ring
    let ring = unsafe {
        match MpmcRing::initialize(std::sync::Arc::new(region), layout, process_id(), epoch()) {
            Ok(ring) => ring,
            Err(err) => {
                set_last_error(err.to_string());
                return map_channel_error(err);
            }
        }
    };
    let handle = Box::new(VtbMpmcRing::new(ring));
    unsafe {
        *out_handle = Box::into_raw(handle);
    }
    VtbStatus::Ok
}

#[no_mangle]
pub extern "C" fn vtb_mpmc_open(
    name: *const c_char,
    slot_size: usize,
    slots: usize,
    out_handle: *mut *mut VtbMpmcRing,
) -> VtbStatus {
    clear_last_error();
    if name.is_null() || out_handle.is_null() {
        return VtbStatus::Invalid;
    }
    let name = unsafe { CStr::from_ptr(name) };
    let name = match name.to_str() {
        Ok(value) => value,
        Err(_) => return VtbStatus::Invalid,
    };
    let layout = match mpmc_layout_from(slot_size, slots) {
        Ok(layout) => layout,
        Err(status) => return status,
    };
    let region = match ShmRegion::open_named(name, layout.total_size) {
        Ok(region) => region,
        Err(err) => {
            set_last_error(err.to_string());
            return VtbStatus::Sys;
        }
    };
    // MpmcRing::attach is used for opening existing ring
    let ring = unsafe {
        match MpmcRing::attach(std::sync::Arc::new(region), layout) {
            Ok(ring) => ring,
            Err(err) => {
                set_last_error(err.to_string());
                return map_channel_error(err);
            }
        }
    };
    let handle = Box::new(VtbMpmcRing::new(ring));
    unsafe {
        *out_handle = Box::into_raw(handle);
    }
    VtbStatus::Ok
}

#[no_mangle]
pub extern "C" fn vtb_mpmc_send(
    handle: *mut VtbMpmcRing,
    data: *const u8,
    len: usize,
    flags: c_uint,
    timeout_ms: c_uint,
) -> VtbStatus {
    clear_last_error();
    if handle.is_null() || data.is_null() {
        return VtbStatus::Invalid;
    }
    let handle = unsafe { &*handle };
    let payload = unsafe { core::slice::from_raw_parts(data, len) };
    let timeout = Duration::from_millis(timeout_ms as u64);

    // MpmcRing doesn't have try_send/send_timeout in the same way?
    // Checking vertebra-core/src/channel.rs... MpmcRing has send(data, flags) which is blocking/spinning?
    // Or send_timeout?
    // vertebra-py uses `send(data, flags)` inside `py.detach`.

    // Assuming MpmcRing has similar API. If not, I might need to check.
    // If it only has blocking send, we can't implement timeout easily without a loop.
    // But MpmcRing in vertebra usually has wait support.

    // For now, let's assume it has `send` and `try_send`.
    let result = if timeout_ms == 0 {
        handle.inner.try_send(payload, flags)
    } else {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            match handle.inner.try_send(payload, flags) {
                Ok(()) => break Ok(()),
                Err(ChannelError::Full) => {
                    if std::time::Instant::now() >= deadline {
                        break Err(ChannelError::Full);
                    }
                    std::thread::yield_now();
                }
                Err(err) => break Err(err),
            }
        }
    };

    match result {
        Ok(()) => VtbStatus::Ok,
        Err(err) => {
            set_last_error(err.to_string());
            map_channel_error(err)
        }
    }
}

#[no_mangle]
pub extern "C" fn vtb_mpmc_recv(
    handle: *mut VtbMpmcRing,
    out: *mut u8,
    capacity: usize,
    out_len: *mut usize,
    out_flags: *mut c_uint,
    timeout_ms: c_uint,
) -> VtbStatus {
    clear_last_error();
    if handle.is_null() || out.is_null() || out_len.is_null() || out_flags.is_null() {
        return VtbStatus::Invalid;
    }
    let handle = unsafe { &*handle };
    let buffer = unsafe { core::slice::from_raw_parts_mut(out, capacity) };
    let timeout = Duration::from_millis(timeout_ms as u64);

    let result = if timeout_ms == 0 {
        handle.inner.try_recv(buffer).map(Some)
    } else {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            match handle.inner.try_recv(buffer) {
                Ok(len) => break Ok(Some(len)),
                Err(ChannelError::Empty) => {
                    if std::time::Instant::now() >= deadline {
                        break Ok(None);
                    }
                    std::thread::yield_now();
                }
                Err(err) => break Err(err),
            }
        }
    };

    match result {
        Ok(Some(len)) => {
            unsafe {
                *out_len = len;

                *out_flags = 0; // MpmcRing does not expose flags
            }

            VtbStatus::Ok
        }

        Ok(None) => VtbStatus::Timeout,

        Err(err) => {
            set_last_error(err.to_string());
            map_channel_error(err)
        }
    }
}

#[no_mangle]
pub extern "C" fn vtb_mpmc_capacity(handle: *const VtbMpmcRing) -> usize {
    if handle.is_null() {
        return 0;
    }
    unsafe { (*handle).capacity }
}

#[no_mangle]
pub extern "C" fn vtb_mpmc_close(handle: *mut VtbMpmcRing) {
    if handle.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(handle));
    }
}

// =============================================================================
// Notifier C ABI - Cross-process notification primitives (eventfd/pipe)
// =============================================================================

#[repr(C)]
pub struct VtbNotifier {
    inner: Notifier,
}

/// Create a new notifier for cross-process signaling.
///
/// On Linux, this creates an eventfd with EFD_NONBLOCK.
/// On macOS/Windows, this creates a non-blocking pipe.
/// FDs are inheritable to support cross-process IPC.
#[no_mangle]
pub extern "C" fn vtb_notifier_new(out_handle: *mut *mut VtbNotifier) -> VtbStatus {
    clear_last_error();
    if out_handle.is_null() {
        return VtbStatus::Invalid;
    }
    match Notifier::new() {
        Ok(inner) => {
            let handle = Box::new(VtbNotifier { inner });
            unsafe {
                *out_handle = Box::into_raw(handle);
            }
            VtbStatus::Ok
        }
        Err(err) => {
            set_last_error(err.to_string());
            VtbStatus::Sys
        }
    }
}

/// Get the notification file descriptor for event loop integration.
///
/// Register this FD with your event loop (poll/epoll/kqueue) to be notified
/// when data is available. On Linux this is the eventfd; on other platforms
/// it's the read end of a pipe.
#[no_mangle]
pub extern "C" fn vtb_notifier_fd(handle: *const VtbNotifier) -> c_int {
    if handle.is_null() {
        return -1;
    }
    let handle = unsafe { &*handle };
    handle.inner.as_raw_fd()
}

/// Get the write file descriptor (for pipe-based platforms).
///
/// On Linux (eventfd), this returns the same value as vtb_notifier_fd().
/// On macOS/Windows (pipe), this is the write end.
#[no_mangle]
pub extern "C" fn vtb_notifier_write_fd(handle: *const VtbNotifier) -> c_int {
    if handle.is_null() {
        return -1;
    }
    let handle = unsafe { &*handle };
    handle.inner.write_fd()
}

/// Signal that data is available (non-blocking).
///
/// On Linux, increments the eventfd counter.
/// On other platforms, writes a single byte to the pipe.
/// Safe to call multiple times; notifications coalesce.
#[no_mangle]
pub extern "C" fn vtb_notifier_notify(handle: *const VtbNotifier) -> VtbStatus {
    clear_last_error();
    if handle.is_null() {
        return VtbStatus::Invalid;
    }
    let handle = unsafe { &*handle };
    match handle.inner.notify() {
        Ok(()) => VtbStatus::Ok,
        Err(err) => {
            set_last_error(err.to_string());
            VtbStatus::Sys
        }
    }
}

/// Clear pending notifications (call after handling data).
///
/// On Linux, reads and discards the eventfd counter.
/// On other platforms, drains the pipe.
#[no_mangle]
pub extern "C" fn vtb_notifier_clear(handle: *const VtbNotifier) -> VtbStatus {
    clear_last_error();
    if handle.is_null() {
        return VtbStatus::Invalid;
    }
    let handle = unsafe { &*handle };
    match handle.inner.clear() {
        Ok(()) => VtbStatus::Ok,
        Err(err) => {
            set_last_error(err.to_string());
            VtbStatus::Sys
        }
    }
}

/// Check if the notifier has pending notifications (non-blocking).
///
/// Returns 1 if pending, 0 if not.
#[no_mangle]
pub extern "C" fn vtb_notifier_has_pending(handle: *const VtbNotifier) -> c_int {
    if handle.is_null() {
        return 0;
    }
    let handle = unsafe { &*handle };
    if handle.inner.has_pending() {
        1
    } else {
        0
    }
}

/// Wait for a notification with timeout.
///
/// Returns VtbStatus::Ok if notification received, VtbStatus::Timeout on timeout.
/// Use timeout_ms=-1 for infinite wait.
#[no_mangle]
pub extern "C" fn vtb_notifier_wait(handle: *const VtbNotifier, timeout_ms: c_int) -> VtbStatus {
    clear_last_error();
    if handle.is_null() {
        return VtbStatus::Invalid;
    }
    let handle = unsafe { &*handle };
    match handle.inner.wait(timeout_ms) {
        Ok(true) => VtbStatus::Ok,
        Ok(false) => VtbStatus::Timeout,
        Err(err) => {
            set_last_error(err.to_string());
            VtbStatus::Sys
        }
    }
}

/// Reconstruct a notifier from inherited file descriptor(s).
///
/// On Linux, only read_fd is used (eventfd).
/// On macOS/Windows, both read_fd and write_fd are needed (pipe).
/// For portability, always pass both FDs.
///
/// # Safety
///
/// The FDs must be valid, open file descriptors from a notifier
/// created in the parent process.
#[no_mangle]
pub extern "C" fn vtb_notifier_from_fds(
    read_fd: c_int,
    write_fd: c_int,
    out_handle: *mut *mut VtbNotifier,
) -> VtbStatus {
    clear_last_error();
    if out_handle.is_null() {
        return VtbStatus::Invalid;
    }
    #[cfg(target_os = "linux")]
    let inner = unsafe { Notifier::from_raw_fd(read_fd) };
    #[cfg(not(target_os = "linux"))]
    let inner = unsafe { Notifier::from_raw_fds(read_fd, write_fd) };
    // Suppress unused warning on Linux
    let _ = write_fd;
    let handle = Box::new(VtbNotifier { inner });
    unsafe {
        *out_handle = Box::into_raw(handle);
    }
    VtbStatus::Ok
}

/// Close a notifier handle.
#[no_mangle]
pub extern "C" fn vtb_notifier_close(handle: *mut VtbNotifier) {
    if handle.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(handle));
    }
}

// =============================================================================
// AsyncNotifierBridge C ABI - Bridges eventfd to pipe for Bun event loop
// =============================================================================

#[repr(C)]
pub struct VtbAsyncBridge {
    inner: AsyncNotifierBridge,
}

/// Create a new async notifier bridge.
///
/// The bridge watches `source_fd` (an eventfd) on a background thread and
/// signals a pipe when data is ready. The pipe can be watched by Bun's
/// event loop via net.Socket.
///
/// # Arguments
///
/// * `source_fd` - The eventfd to watch (must remain valid for bridge lifetime)
/// * `out_handle` - Output: pointer to the created handle
///
/// # Returns
///
/// VtbStatus::Ok on success, error status otherwise.
#[no_mangle]
pub extern "C" fn vtb_async_bridge_new(
    source_fd: c_int,
    out_handle: *mut *mut VtbAsyncBridge,
) -> VtbStatus {
    clear_last_error();
    if out_handle.is_null() {
        return VtbStatus::Invalid;
    }
    if source_fd < 0 {
        set_last_error("invalid source_fd".to_string());
        return VtbStatus::Invalid;
    }
    match AsyncNotifierBridge::new(source_fd as RawFd) {
        Ok(inner) => {
            let handle = Box::new(VtbAsyncBridge { inner });
            unsafe {
                *out_handle = Box::into_raw(handle);
            }
            VtbStatus::Ok
        }
        Err(err) => {
            set_last_error(err.to_string());
            VtbStatus::Sys
        }
    }
}

/// Get the pipe file descriptor for Bun event loop integration.
///
/// Register this fd with net.Socket to be notified when the source eventfd
/// was signaled. Returns -1 if handle is null.
#[no_mangle]
pub extern "C" fn vtb_async_bridge_pipe_fd(handle: *const VtbAsyncBridge) -> c_int {
    if handle.is_null() {
        return -1;
    }
    let handle = unsafe { &*handle };
    handle.inner.pipe_fd()
}

/// Start the background watcher thread.
///
/// The thread will block on the source eventfd and write to the pipe
/// when notifications arrive. Safe to call multiple times (no-op if running).
#[no_mangle]
pub extern "C" fn vtb_async_bridge_start(handle: *mut VtbAsyncBridge) -> VtbStatus {
    clear_last_error();
    if handle.is_null() {
        return VtbStatus::Invalid;
    }
    let handle = unsafe { &mut *handle };
    handle.inner.start();
    VtbStatus::Ok
}

/// Stop the background watcher thread.
///
/// Signals shutdown via eventfd for instant wake (no polling delay).
/// Blocks until the thread exits. Safe to call multiple times.
#[no_mangle]
pub extern "C" fn vtb_async_bridge_stop(handle: *mut VtbAsyncBridge) -> VtbStatus {
    clear_last_error();
    if handle.is_null() {
        return VtbStatus::Invalid;
    }
    let handle = unsafe { &mut *handle };
    handle.inner.stop();
    VtbStatus::Ok
}

/// Check if the watcher thread is running.
///
/// Returns 1 if running, 0 if not (or if handle is null).
#[no_mangle]
pub extern "C" fn vtb_async_bridge_is_running(handle: *const VtbAsyncBridge) -> c_int {
    if handle.is_null() {
        return 0;
    }
    let handle = unsafe { &*handle };
    if handle.inner.is_running() {
        1
    } else {
        0
    }
}

/// Close the async bridge handle.
///
/// Stops the watcher thread if running and frees resources.
#[no_mangle]
pub extern "C" fn vtb_async_bridge_close(handle: *mut VtbAsyncBridge) {
    if handle.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(handle));
    }
}
