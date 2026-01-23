//! Shared-memory C ABI for the Vertebra broker transport.

use core::ffi::{c_char, c_uint, c_void};
use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::time::Duration;

use vertebra_core::buffer::RingLayout;
use vertebra_core::channel::{ChannelError, RingStats, SpscReceiver, SpscRing, SpscSender};
use vertebra_core::router::{RouteId, Router};
use vertebra_core::shm::ShmRegion;

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
) -> VtbStatus {
    clear_last_error();
    if handle.is_null() || path.is_null() || out_id.is_null() {
        return VtbStatus::Invalid;
    }
    let handle = unsafe { &*handle };
    let path = unsafe { CStr::from_ptr(path) };
    let path = match path.to_str() {
        Ok(value) => value,
        Err(_) => return VtbStatus::Invalid,
    };
    match handle.inner.match_path(path) {
        Some(id) => {
            unsafe {
                *out_id = id.0;
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
