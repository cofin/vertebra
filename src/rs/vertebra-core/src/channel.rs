//! Lock-free ring buffer channel (SPSC implementation).

use core::ptr;
use core::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_utils::Backoff;

use crate::buffer::{
    ABI_VERSION, CacheLine, ChannelMode, MAGIC, RegionState, RingLayout, ShmHeader, SlotHeader,
};
use crate::notify::Notifier;
use crate::shm::{ShmError, ShmRegion};
use crate::sync::{self, SyncError};

pub(crate) const SLOT_FLAG_ABORTED: u32 = 0x8000_0000;

#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    #[error("ring layout is invalid")]
    InvalidLayout,
    #[error("region is not ready")]
    NotReady,
    #[error("channel is full")]
    Full,
    #[error("channel is empty")]
    Empty,
    #[error("payload too large: {len} > {capacity}")]
    PayloadTooLarge { len: usize, capacity: usize },
    #[error("buffer too small: {len} > {capacity}")]
    BufferTooSmall { len: usize, capacity: usize },
    #[error("corrupted slot in shared memory")]
    CorruptedSlot,
    #[error("corrupted shared-memory segment: {0}")]
    CorruptedSegment(&'static str),
    #[error("slot already borrowed")]
    SlotBorrowed,
    #[error("shared memory error: {0}")]
    Shm(#[from] ShmError),
    #[error("sync failure: {0}")]
    Sync(#[from] SyncError),
}

/// Zero-copy view into a ring buffer slot.
///
/// This struct holds a reference to the data in the ring buffer without copying.
/// The slot is released (read cursor advanced) when the SlotView is dropped.
///
/// # Safety
/// The SlotView borrows data from the ring buffer. The ring must outlive the view.
pub struct SlotView {
    ring: Arc<SpscRing>,
    slot_index: u64,
    data_ptr: *const u8,
    length: usize,
    flags: u32,
    released: bool,
}

// SAFETY: `SlotView` holds an `Arc<SpscRing>` keeping the shared-memory backing valid, and
// `SpscRing` enforces single-borrow semantics (`slot_borrowed`) across threads until drop.
unsafe impl Send for SlotView {}
// SAFETY: Shared references to `SlotView` only expose immutable payload bytes via `as_slice()`
// while the slot cannot be overwritten until `SlotView::drop` advances `read_head`.
unsafe impl Sync for SlotView {}

impl SlotView {
    /// Get the payload data as a byte slice (zero-copy).
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: data_ptr points to valid memory in the ring buffer,
        // and the ring is kept alive by Arc.
        unsafe { std::slice::from_raw_parts(self.data_ptr, self.length) }
    }

    /// Get the payload length.
    #[inline]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Check if payload is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Get the message flags.
    #[inline]
    pub fn flags(&self) -> u32 {
        self.flags
    }
}

impl core::fmt::Debug for SlotView {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SlotView")
            .field("slot_index", &self.slot_index)
            .field("length", &self.length)
            .field("flags", &self.flags)
            .field("released", &self.released)
            .finish()
    }
}

impl SlotView {
    /// Explicitly release the slot back to the ring buffer.
    ///
    /// This is called automatically on drop, but can be called early.
    pub fn release(&mut self) {
        if self.released {
            return;
        }
        self.released = true;

        // Advance read head to release the slot
        let next = self.slot_index.wrapping_add(1) & self.ring.mask;
        self.ring.read_head().store(next, Ordering::Release);
        self.ring.wait_read_ptr().fetch_add(1, Ordering::SeqCst);
        if self.ring.send_waiters().load(Ordering::SeqCst) > 0 {
            let _ = crate::sync::wake_one(self.ring.wait_read_ptr());
        }

        // Clear the borrowed flag to allow new borrows
        self.ring.slot_borrowed.store(false, Ordering::Release);
    }
}

impl Drop for SlotView {
    fn drop(&mut self) {
        self.release();
    }
}

impl AsRef<[u8]> for SlotView {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

/// SPSC ring channel backed by a shared memory region.
#[derive(Debug)]
pub struct SpscRing {
    /// Keeps the shared memory region alive for the lifetime of the ring.
    /// Not accessed directly after construction, but the Arc prevents deallocation.
    #[allow(dead_code)] // Intentional: Arc kept alive for memory lifetime, not accessed
    region: Arc<ShmRegion>,
    header: *mut ShmHeader,
    data: *mut u8,
    layout: RingLayout,
    mask: u64,
    /// Tracks if a SlotView is currently borrowing data (prevents concurrent borrows).
    slot_borrowed: std::sync::atomic::AtomicBool,
    /// Producer-local cached snapshot of `read_head` to avoid cross-core SHM loads while space remains.
    cached_read_head: CacheLine<AtomicU64>,
    /// Consumer-local cached snapshot of `write_head` to avoid cross-core SHM loads while draining bursts.
    cached_write_head: CacheLine<AtomicU64>,
    /// Last observed `ShmHeader.generation` to invalidate local head caches across `reset()`.
    cached_generation: AtomicU32,
}

// SAFETY: `SpscRing` owns an `Arc<ShmRegion>` and synchronizes all header and slot accesses
// via atomic Acquire/Release/SeqCst operations on cache-line-aligned header fields.
unsafe impl Send for SpscRing {}
// SAFETY: Concurrent access through `&SpscRing` is governed by atomic head/tail/waiter fields
// and single-producer / single-consumer protocol invariants.
unsafe impl Sync for SpscRing {}

#[derive(Debug, Copy, Clone)]
pub struct RingStats {
    pub slot_size: usize,
    pub slots: usize,
    pub read_head: u64,
    pub write_head: u64,
}

fn validate_region_for_layout(
    region: &ShmRegion,
    layout: &RingLayout,
    min_align: usize,
) -> Result<(), ChannelError> {
    if layout.slots == 0
        || !layout.is_power_of_two()
        || layout.slot_size < core::mem::size_of::<SlotHeader>()
        || !layout
            .slot_size
            .is_multiple_of(core::mem::align_of::<SlotHeader>())
        || layout.header_size < core::mem::size_of::<ShmHeader>()
    {
        return Err(ChannelError::InvalidLayout);
    }
    let Some(ring_offset) = layout.header_size.checked_add(layout.seq_size) else {
        return Err(ChannelError::CorruptedSegment("ring offset overflow"));
    };
    if layout.total_size < ring_offset {
        return Err(ChannelError::InvalidLayout);
    }
    let Some(required_ring_size) = layout.slots.checked_mul(layout.slot_size) else {
        return Err(ChannelError::CorruptedSegment("ring size overflow"));
    };
    if layout.ring_size < required_ring_size {
        return Err(ChannelError::InvalidLayout);
    }
    let Some(ring_end) = ring_offset.checked_add(layout.ring_size) else {
        return Err(ChannelError::CorruptedSegment("ring bounds overflow"));
    };
    if ring_end > layout.total_size {
        return Err(ChannelError::CorruptedSegment(
            "ring exceeds layout total_size",
        ));
    }
    if region.len() < layout.total_size {
        return Err(ChannelError::CorruptedSegment(
            "region shorter than layout total_size",
        ));
    }
    let base_addr = region.as_ptr() as usize;
    if min_align > 0 && !base_addr.is_multiple_of(min_align) {
        return Err(ChannelError::CorruptedSegment(
            "region pointer misaligned for header",
        ));
    }
    if layout.mode == ChannelMode::Mpmc {
        let seq_align = core::mem::align_of::<CacheLine<AtomicU64>>();
        let Some(seq_bytes) = layout
            .slots
            .checked_mul(core::mem::size_of::<CacheLine<AtomicU64>>())
        else {
            return Err(ChannelError::CorruptedSegment("sequence array overflow"));
        };
        if layout.seq_size < seq_bytes {
            return Err(ChannelError::InvalidLayout);
        }
        let Some(seq_end) = layout.seq_offset().checked_add(seq_bytes) else {
            return Err(ChannelError::CorruptedSegment("sequence offset overflow"));
        };
        if seq_end > ring_offset || seq_end > region.len() {
            return Err(ChannelError::CorruptedSegment(
                "sequence array exceeds region bounds",
            ));
        }
        let Some(seq_addr) = base_addr.checked_add(layout.seq_offset()) else {
            return Err(ChannelError::CorruptedSegment("sequence address overflow"));
        };
        if !seq_addr.is_multiple_of(seq_align) {
            return Err(ChannelError::CorruptedSegment(
                "sequence array pointer misaligned",
            ));
        }
    }
    Ok(())
}

impl SpscRing {
    /// Initialize a shared region in-place and return a ring handle.
    ///
    /// # Safety
    /// Caller must ensure exclusive initialization (single writer) and that
    /// the region is at least `layout.total_size` bytes.
    pub unsafe fn initialize(
        region: Arc<ShmRegion>,
        layout: RingLayout,
        creator_pid: u32,
        creator_epoch: u64,
    ) -> Result<Self, ChannelError> {
        if !layout.is_power_of_two() || layout.slot_size < core::mem::size_of::<SlotHeader>() {
            return Err(ChannelError::InvalidLayout);
        }
        if region.len() < layout.total_size {
            return Err(ChannelError::InvalidLayout);
        }
        let header = region.as_ptr().cast::<ShmHeader>();
        // SAFETY: Caller guarantees exclusive initialization, `ShmHeader` alignment, and
        // `region.len() >= layout.total_size` (verified above) covering `ShmHeader`.
        let header_ref = unsafe { &mut *header };
        header_ref.magic = MAGIC;
        header_ref.abi_version = ABI_VERSION;
        header_ref.flags = 0;
        header_ref.total_size = layout.total_size as u64;
        header_ref.ring_offset = layout.header_size as u64;
        header_ref.ring_size = layout.ring_size as u64;
        header_ref.generation.store(1, Ordering::Relaxed);
        header_ref
            .state
            .store(RegionState::Ready as u32, Ordering::Release);
        header_ref.creator_pid = creator_pid;
        header_ref.creator_epoch = creator_epoch;
        header_ref.read_head.0.store(0, Ordering::Relaxed);
        header_ref.write_head.0.store(0, Ordering::Relaxed);
        header_ref.wait_read.0.store(0, Ordering::Relaxed);
        header_ref.wait_write.0.store(0, Ordering::Relaxed);
        header_ref.send_waiters.0.store(0, Ordering::Relaxed);
        header_ref.recv_waiters.0.store(0, Ordering::Relaxed);
        header_ref.consumer_sleeping.0.store(0, Ordering::Relaxed);
        header_ref.active_flags.0.store(0, Ordering::Relaxed);
        // SAFETY: `ShmHeader` fields were just initialized above; `from_region` validates
        // alignment and layout bounds before constructing `Self`.
        unsafe { Self::from_region(region, layout) }
    }

    /// Attach to an existing shared region and validate the header.
    ///
    /// # Safety
    /// Caller must ensure the region stays mapped for the lifetime of the ring.
    pub unsafe fn attach(region: Arc<ShmRegion>, layout: RingLayout) -> Result<Self, ChannelError> {
        validate_region_for_layout(&region, &layout, core::mem::align_of::<ShmHeader>())?;
        let header = region.as_ptr().cast::<ShmHeader>();
        // SAFETY: `validate_region_for_layout` verified `region` is aligned and covers `ShmHeader`.
        let header_ref = unsafe { &*header };
        if header_ref.magic != MAGIC || header_ref.abi_version != ABI_VERSION {
            return Err(ChannelError::NotReady);
        }
        if header_ref.state.load(Ordering::Acquire) != RegionState::Ready as u32 {
            return Err(ChannelError::NotReady);
        }
        // SAFETY: header magic, ABI version, and ready state are verified above.
        unsafe { Self::from_region(region, layout) }
    }

    /// Create a ring without initializing the header.
    ///
    /// # Safety
    /// Caller must ensure the region is already initialized correctly.
    pub unsafe fn from_region(
        region: Arc<ShmRegion>,
        layout: RingLayout,
    ) -> Result<Self, ChannelError> {
        validate_region_for_layout(&region, &layout, core::mem::align_of::<ShmHeader>())?;
        let header = region.as_ptr().cast::<ShmHeader>();
        // SAFETY: `validate_region_for_layout` verified `region` is aligned and covers `ShmHeader`.
        let header_ref = unsafe { &*header };
        let init_read = header_ref.read_head.0.load(Ordering::Acquire);
        let init_write = header_ref.write_head.0.load(Ordering::Acquire);
        let init_gen = header_ref.generation.load(Ordering::Acquire);
        // SAFETY: `validate_region_for_layout` verified `layout.header_size <= layout.total_size <= region.len()`.
        let data = unsafe { region.as_ptr().add(layout.header_size) };
        Ok(Self {
            region,
            header,
            data,
            layout,
            mask: (layout.slots as u64).wrapping_sub(1),
            slot_borrowed: std::sync::atomic::AtomicBool::new(false),
            cached_read_head: CacheLine(AtomicU64::new(init_read)),
            cached_write_head: CacheLine(AtomicU64::new(init_write)),
            cached_generation: AtomicU32::new(init_gen),
        })
    }

    pub fn payload_capacity(&self) -> usize {
        self.layout.slot_size - core::mem::size_of::<SlotHeader>()
    }

    /// Get the underlying shared memory region's handle (FD on Unix).
    ///
    /// Used for FD inheritance across process boundaries.
    #[cfg(unix)]
    pub fn region_handle(&self) -> Option<std::os::fd::RawFd> {
        self.region.handle()
    }

    pub fn stats(&self) -> RingStats {
        RingStats {
            slot_size: self.layout.slot_size,
            slots: self.layout.slots,
            read_head: self.read_head().load(Ordering::Acquire),
            write_head: self.write_head().load(Ordering::Acquire),
        }
    }

    /// Check if data is available in the ring buffer.
    ///
    /// This is a fast check using Relaxed ordering - suitable for spin loops
    /// where the actual recv will use proper Acquire ordering.
    #[inline]
    pub fn has_data(&self) -> bool {
        self.sync_generation();
        let head = self.read_head().load(Ordering::Relaxed);
        let cached_tail = self.cached_write_head.0.load(Ordering::Acquire);
        if head != cached_tail {
            return true;
        }
        let tail = self.write_head().load(Ordering::Relaxed);
        head != tail
    }

    /// Check if data is available using `Acquire` ordering on both cursors.
    #[inline]
    pub fn has_data_acquire(&self) -> bool {
        self.read_head().load(Ordering::Acquire) != self.write_head().load(Ordering::Acquire)
    }

    #[inline]
    fn sync_generation(&self) {
        // SAFETY: `self.header` points to a `ShmHeader` validated by `validate_region_for_layout`
        // and kept alive by `self.region`; `generation` is an `AtomicU32` safe for shared Acquire reads.
        let generation = unsafe { (*self.header).generation.load(Ordering::Acquire) };
        let cached = self.cached_generation.load(Ordering::Relaxed);
        if generation != cached {
            let read = self.read_head().load(Ordering::Acquire);
            let write = self.write_head().load(Ordering::Acquire);
            self.cached_read_head.0.store(read, Ordering::Release);
            self.cached_write_head.0.store(write, Ordering::Release);
            self.cached_generation.store(generation, Ordering::Release);
        }
    }

    #[inline]
    fn load_consumer_tail(&self, head: u64) -> u64 {
        self.sync_generation();
        let mut tail = self.cached_write_head.0.load(Ordering::Acquire);
        if head == tail {
            tail = self.write_head().load(Ordering::Acquire);
            self.cached_write_head.0.store(tail, Ordering::Release);
        }
        tail
    }

    #[inline]
    fn has_producer_space(&self, next: u64) -> bool {
        self.sync_generation();
        let mut head = self.cached_read_head.0.load(Ordering::Acquire);
        if next == head {
            head = self.read_head().load(Ordering::Acquire);
            self.cached_read_head.0.store(head, Ordering::Release);
            if next == head {
                return false;
            }
        }
        true
    }

    /// Access the Dekker `consumer_sleeping` flag in `ShmHeader`.
    #[inline]
    pub fn consumer_sleeping(&self) -> &AtomicU32 {
        // SAFETY: `self.header` is aligned and valid for the lifetime of `self.region`.
        unsafe { &(*self.header).consumer_sleeping.0 }
    }

    /// Access the `active_flags` bitmask in `ShmHeader`.
    #[inline]
    pub fn active_flags(&self) -> &AtomicU32 {
        // SAFETY: `self.header` is aligned and valid for the lifetime of `self.region`.
        unsafe { &(*self.header).active_flags.0 }
    }

    /// Arm the Dekker `consumer_sleeping` flag (`1`) and re-verify the ring is empty.
    ///
    /// Returns `true` when the ring remains empty and the caller may safely block on its
    /// `Notifier`; returns `false` (and resets `consumer_sleeping` to `0`) if unread data is
    /// already present.
    #[inline]
    pub fn prepare_sleep(&self) -> bool {
        self.consumer_sleeping().store(1, Ordering::SeqCst);
        std::sync::atomic::fence(Ordering::SeqCst);
        if self.read_head().load(Ordering::Acquire) != self.write_head().load(Ordering::Acquire) {
            self.consumer_sleeping().store(0, Ordering::SeqCst);
            return false;
        }
        true
    }

    /// Disarm the Dekker `consumer_sleeping` flag (`0`) after waking or cancelling a wait.
    #[inline]
    pub fn cancel_sleep(&self) {
        self.consumer_sleeping().store(0, Ordering::SeqCst);
    }

    /// Atomically check and clear `consumer_sleeping` after publishing writes (`SeqCst` Dekker).
    #[inline]
    pub fn should_notify_and_clear(&self) -> bool {
        std::sync::atomic::fence(Ordering::SeqCst);
        self.consumer_sleeping().swap(0, Ordering::SeqCst) != 0
    }

    /// Notify `notifier` only if the consumer has armed `consumer_sleeping == 1`.
    #[inline]
    pub fn notify_if_sleeping(&self, notifier: &Notifier) -> std::io::Result<bool> {
        if self.should_notify_and_clear() {
            notifier.notify()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn try_send(&self, payload: &[u8], flags: u32) -> Result<(), ChannelError> {
        if payload.len() > self.payload_capacity() {
            return Err(ChannelError::PayloadTooLarge {
                len: payload.len(),
                capacity: self.payload_capacity(),
            });
        }
        let tail = self.write_head().load(Ordering::Relaxed);
        let next = tail.wrapping_add(1) & self.mask;
        if !self.has_producer_space(next) {
            return Err(ChannelError::Full);
        }
        // SAFETY: `has_producer_space(next)` confirmed slot `tail` is free and owned exclusively by
        // the producer, and `payload.len() <= self.payload_capacity()` was checked above.
        unsafe {
            self.write_slot(tail, payload, flags);
        }
        self.publish_write(next)
    }

    /// Zero-copy send: reserve a slot and call `writer` to populate `len` bytes of payload.
    pub fn try_send_with<F>(&self, len: usize, flags: u32, writer: F) -> Result<(), ChannelError>
    where
        F: FnOnce(&mut [u8]),
    {
        if len > self.payload_capacity() {
            return Err(ChannelError::PayloadTooLarge {
                len,
                capacity: self.payload_capacity(),
            });
        }
        let tail = self.write_head().load(Ordering::Relaxed);
        let next = tail.wrapping_add(1) & self.mask;
        if !self.has_producer_space(next) {
            return Err(ChannelError::Full);
        }
        struct SpscSendGuard {
            header: *mut SlotHeader,
            armed: bool,
        }
        impl Drop for SpscSendGuard {
            fn drop(&mut self) {
                if self.armed {
                    // SAFETY: `self.header` points to the un-published slot exclusively held by the
                    // producer until `publish_write` stores `write_head` with Release ordering.
                    unsafe {
                        (*self.header).len = 0;
                    }
                }
            }
        }

        // SAFETY: `slot_ptr(tail)` returns a `SlotHeader`-aligned pointer inside the validated ring
        // region; the slot is exclusively owned by the producer until `publish_write`, and
        // `len <= self.payload_capacity()` guarantees `payload_slice` stays within the slot.
        unsafe {
            let slot = self.slot_ptr(tail);
            let header = slot.cast::<SlotHeader>();
            let mut guard = SpscSendGuard {
                header,
                armed: true,
            };
            (*header).len = len as u32;
            (*header).flags = flags;
            let payload_ptr = slot.add(core::mem::size_of::<SlotHeader>());
            let payload_slice = core::slice::from_raw_parts_mut(payload_ptr, len);
            writer(payload_slice);
            guard.armed = false;
        }
        self.publish_write(next)
    }

    #[inline]
    fn publish_write(&self, next: u64) -> Result<(), ChannelError> {
        self.write_head().store(next, Ordering::Release);
        self.wait_write_ptr().fetch_add(1, Ordering::SeqCst);
        if self.recv_waiters().load(Ordering::SeqCst) > 0 {
            sync::wake_one(self.wait_write_ptr())?;
        }
        Ok(())
    }

    #[inline]
    fn publish_read(&self, next: u64) -> Result<(), ChannelError> {
        self.read_head().store(next, Ordering::Release);
        self.wait_read_ptr().fetch_add(1, Ordering::SeqCst);
        if self.send_waiters().load(Ordering::SeqCst) > 0 {
            sync::wake_one(self.wait_read_ptr())?;
        }
        Ok(())
    }

    fn park_send(&self, deadline: Option<Instant>) -> Result<bool, ChannelError> {
        let remaining = match deadline {
            Some(dl) => {
                let now = Instant::now();
                if now >= dl {
                    return Ok(false);
                }
                Some(dl - now)
            }
            None => None,
        };
        let wait_word = self.wait_read_ptr();
        let expected = wait_word.load(Ordering::SeqCst);

        self.send_waiters().fetch_add(1, Ordering::SeqCst);

        let head = self.read_head().load(Ordering::SeqCst);
        let tail = self.write_head().load(Ordering::SeqCst);
        let next = tail.wrapping_add(1) & self.mask;
        if next != head {
            self.send_waiters().fetch_sub(1, Ordering::SeqCst);
            return Ok(true);
        }

        let woke = match remaining {
            Some(rem) => sync::wait_on_address_timeout(wait_word, expected, rem),
            None => sync::wait_on_address(wait_word, expected).map(|()| true),
        };
        self.send_waiters().fetch_sub(1, Ordering::SeqCst);
        Ok(woke?)
    }

    fn park_recv(&self, deadline: Option<Instant>) -> Result<bool, ChannelError> {
        let remaining = match deadline {
            Some(dl) => {
                let now = Instant::now();
                if now >= dl {
                    return Ok(false);
                }
                Some(dl - now)
            }
            None => None,
        };
        let wait_word = self.wait_write_ptr();
        let expected = wait_word.load(Ordering::SeqCst);

        self.recv_waiters().fetch_add(1, Ordering::SeqCst);

        let head = self.read_head().load(Ordering::SeqCst);
        let tail = self.write_head().load(Ordering::SeqCst);
        if head != tail {
            self.recv_waiters().fetch_sub(1, Ordering::SeqCst);
            return Ok(true);
        }

        let woke = match remaining {
            Some(rem) => sync::wait_on_address_timeout(wait_word, expected, rem),
            None => sync::wait_on_address(wait_word, expected).map(|()| true),
        };
        self.recv_waiters().fetch_sub(1, Ordering::SeqCst);
        Ok(woke?)
    }

    /// Blocking zero-copy send: waits for space if full, then calls `writer` to populate `len` bytes.
    pub fn send_with<F>(&self, len: usize, flags: u32, mut writer: F) -> Result<(), ChannelError>
    where
        F: FnMut(&mut [u8]),
    {
        let mut spins = 0u32;
        let backoff = Backoff::new();
        loop {
            match self.try_send_with(len, flags, &mut writer) {
                Ok(()) => return Ok(()),
                Err(ChannelError::Full) => {
                    if spins < 32 {
                        spins += 1;
                        backoff.spin();
                        continue;
                    }
                    spins = 0;
                    backoff.reset();
                    self.park_send(None)?;
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn send(&self, payload: &[u8], flags: u32) -> Result<(), ChannelError> {
        let mut spins = 0u32;
        let backoff = Backoff::new();
        loop {
            match self.try_send(payload, flags) {
                Ok(()) => return Ok(()),
                Err(ChannelError::Full) => {
                    if spins < 32 {
                        spins += 1;
                        backoff.spin();
                        continue;
                    }
                    spins = 0;
                    backoff.reset();
                    self.park_send(None)?;
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn send_timeout(
        &self,
        payload: &[u8],
        flags: u32,
        timeout: Duration,
    ) -> Result<bool, ChannelError> {
        let deadline = Instant::now() + timeout;
        let mut spins = 0u32;
        let backoff = Backoff::new();
        loop {
            match self.try_send(payload, flags) {
                Ok(()) => return Ok(true),
                Err(ChannelError::Full) => {
                    if spins < 32 {
                        if Instant::now() >= deadline {
                            return Ok(false);
                        }
                        spins += 1;
                        backoff.spin();
                        continue;
                    }
                    spins = 0;
                    backoff.reset();
                    if !self.park_send(Some(deadline))? {
                        return Ok(false);
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn try_recv(&self, out: &mut [u8]) -> Result<usize, ChannelError> {
        if self.slot_borrowed.load(Ordering::Acquire) {
            return Err(ChannelError::SlotBorrowed);
        }
        let head = self.read_head().load(Ordering::Relaxed);
        let tail = self.load_consumer_tail(head);
        if head == tail {
            return Err(ChannelError::Empty);
        }
        // SAFETY: `load_consumer_tail` synchronized the producer's Release write via Acquire load;
        // `head != tail` guarantees slot `head` is published and exclusively readable by the consumer.
        let len = unsafe { self.read_slot(head, out)? };
        let next = head.wrapping_add(1) & self.mask;
        self.publish_read(next)?;
        Ok(len)
    }

    pub fn try_recv_with_flags(&self, out: &mut [u8]) -> Result<(usize, u32), ChannelError> {
        if self.slot_borrowed.load(Ordering::Acquire) {
            return Err(ChannelError::SlotBorrowed);
        }
        let head = self.read_head().load(Ordering::Relaxed);
        let tail = self.load_consumer_tail(head);
        if head == tail {
            return Err(ChannelError::Empty);
        }
        // SAFETY: `load_consumer_tail` synchronized the producer's Release write via Acquire load;
        // `head != tail` guarantees slot `head` is published and exclusively readable by the consumer.
        let (len, flags) = unsafe { self.read_slot_with_flags(head, out)? };
        let next = head.wrapping_add(1) & self.mask;
        self.publish_read(next)?;
        Ok((len, flags))
    }

    pub fn recv(&self, out: &mut [u8]) -> Result<usize, ChannelError> {
        loop {
            match self.try_recv(out) {
                Ok(len) => return Ok(len),
                Err(ChannelError::Empty) => {
                    self.park_recv(None)?;
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn recv_timeout(
        &self,
        out: &mut [u8],
        timeout: Duration,
    ) -> Result<Option<usize>, ChannelError> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.try_recv(out) {
                Ok(len) => return Ok(Some(len)),
                Err(ChannelError::Empty) => {
                    if !self.park_recv(Some(deadline))? {
                        return Ok(None);
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Receive with hybrid spin-wait for low-latency sync scenarios.
    ///
    /// Spins checking for data availability for `spin_us` microseconds before
    /// falling back to futex-based blocking. This captures immediate responses
    /// with minimal overhead while still allowing efficient blocking for longer
    /// waits.
    ///
    /// # Arguments
    ///
    /// * `out` - Output buffer for received data
    /// * `spin_us` - Microseconds to spin before blocking (typically 2-10)
    /// * `timeout` - Total timeout (includes spin time)
    ///
    /// # Returns
    ///
    /// * `Ok(Some(len))` - Data received, returns length
    /// * `Ok(None)` - Timeout expired
    /// * `Err` - Error during receive
    ///
    /// # Performance
    ///
    /// For immediate data (common case):
    /// - Returns in ~1-3µs without syscall
    ///
    /// For delayed data arriving during spin:
    /// - Returns in ~spin_us without syscall
    ///
    /// For delayed data arriving after spin:
    /// - Falls back to futex wait (~5-10µs wakeup latency)
    pub fn recv_hybrid(
        &self,
        out: &mut [u8],
        spin_us: u64,
        timeout: Duration,
    ) -> Result<Option<usize>, ChannelError> {
        // Fast path: data already available
        match self.try_recv(out) {
            Ok(len) => return Ok(Some(len)),
            Err(ChannelError::Empty) => {}
            Err(e) => return Err(e),
        }

        let start = Instant::now();
        let spin_deadline = start + Duration::from_micros(spin_us);
        let timeout_deadline = start + timeout;

        // Spin phase: tight loop checking for data
        while Instant::now() < spin_deadline {
            if self.has_data() {
                match self.try_recv(out) {
                    Ok(len) => return Ok(Some(len)),
                    Err(ChannelError::Empty) => {} // Spurious, retry
                    Err(e) => return Err(e),
                }
            }
            std::hint::spin_loop();
        }

        // Park phase: fall back to futex-based wait
        loop {
            match self.try_recv(out) {
                Ok(len) => return Ok(Some(len)),
                Err(ChannelError::Empty) => {
                    if !self.park_recv(Some(timeout_deadline))? {
                        return Ok(None);
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub fn recv_with_flags(&self, out: &mut [u8]) -> Result<(usize, u32), ChannelError> {
        loop {
            match self.try_recv_with_flags(out) {
                Ok(result) => return Ok(result),
                Err(ChannelError::Empty) => {
                    self.park_recv(None)?;
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Try to receive a message as a zero-copy view.
    ///
    /// Returns a `SlotView` that provides direct access to the ring buffer data.
    /// The slot is released when the view is dropped.
    ///
    /// # Note
    /// Only one SlotView can be active at a time per ring. The next recv will
    /// return `SlotBorrowed` until the current SlotView is released.
    pub fn try_recv_view(self: &Arc<Self>) -> Result<SlotView, ChannelError> {
        if self.slot_borrowed.load(Ordering::Acquire) {
            return Err(ChannelError::SlotBorrowed);
        }

        let head = self.read_head().load(Ordering::Relaxed);
        let tail = self.load_consumer_tail(head);
        if head == tail {
            return Err(ChannelError::Empty);
        }

        if self
            .slot_borrowed
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Err(ChannelError::SlotBorrowed);
        }

        let head = self.read_head().load(Ordering::Acquire);
        let tail = self.load_consumer_tail(head);
        if head == tail {
            self.slot_borrowed.store(false, Ordering::Release);
            return Err(ChannelError::Empty);
        }

        // SAFETY: `load_consumer_tail` synchronized the producer's Release write via Acquire load;
        // `head != tail` guarantees slot `head` is published and valid until `SlotView` drops.
        let (data_ptr, length, flags) = unsafe { self.peek_slot(head) };
        if length > self.payload_capacity() {
            self.slot_borrowed.store(false, Ordering::Release);
            return Err(ChannelError::CorruptedSlot);
        }

        Ok(SlotView {
            ring: Arc::clone(self),
            slot_index: head,
            data_ptr,
            length,
            flags,
            released: false,
        })
    }

    /// Receive a message as a zero-copy view, blocking until data is available.
    pub fn recv_view(self: &Arc<Self>) -> Result<SlotView, ChannelError> {
        loop {
            match self.try_recv_view() {
                Ok(view) => return Ok(view),
                Err(ChannelError::Empty) => {
                    self.park_recv(None)?;
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Receive a message as a zero-copy view with timeout.
    pub fn recv_view_timeout(
        self: &Arc<Self>,
        timeout: Duration,
    ) -> Result<Option<SlotView>, ChannelError> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.try_recv_view() {
                Ok(view) => return Ok(Some(view)),
                Err(ChannelError::Empty) => {
                    if !self.park_recv(Some(deadline))? {
                        return Ok(None);
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn recv_timeout_with_flags(
        &self,
        out: &mut [u8],
        timeout: Duration,
    ) -> Result<Option<(usize, u32)>, ChannelError> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.try_recv_with_flags(out) {
                Ok(result) => return Ok(Some(result)),
                Err(ChannelError::Empty) => {
                    if !self.park_recv(Some(deadline))? {
                        return Ok(None);
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Drain up to `max` messages from the ring without blocking.
    ///
    /// This is more efficient than calling `try_recv` in a loop because:
    /// 1. Only one atomic read_head store at the end (amortized)
    /// 2. Only one conditional wake check at the end
    ///
    /// # Arguments
    ///
    /// * `max` - Maximum number of messages to receive
    ///
    /// # Returns
    ///
    /// Vector of `(payload, flags)` tuples. Returns empty Vec if ring is empty.
    ///
    /// # Performance
    ///
    /// For N messages: O(1) atomic stores instead of O(N).
    /// Wake syscall amortized across batch.
    /// Drain up to `max` messages without blocking.
    ///
    /// Returns a vector of `(payload, flags)` tuples, or an empty `Vec` if the ring is empty or
    /// currently borrowed by a `SlotView`.
    ///
    /// # Errors
    /// Returns [`ChannelError::CorruptedSlot`] if an unread slot header claims a payload length
    /// exceeding [`Self::payload_capacity`]. Any slots validated prior to the corrupted slot are
    /// committed to `read_head`; `read_head` does not advance past the corrupted slot.
    pub fn try_recv_batch(&self, max: usize) -> Result<Vec<(Vec<u8>, u32)>, ChannelError> {
        let mut results = Vec::with_capacity(max.min(self.layout.slots));
        if self.slot_borrowed.load(Ordering::Acquire) {
            return Ok(results);
        }

        // PERF: Relaxed loads for availability check
        let mut head = self.read_head().load(Ordering::Relaxed);
        let tail = self.write_head().load(Ordering::Relaxed);

        if head == tail {
            return Ok(results); // Empty - fast return
        }

        // CRITICAL: Acquire fence before reading any slot data
        std::sync::atomic::fence(Ordering::Acquire);

        // Drain available messages up to max
        let mut count = 0;
        let mut batch_err = None;
        while head != tail && count < max {
            match self.read_slot_to_vec(head) {
                Ok((payload, flags)) => {
                    results.push((payload, flags));
                    head = head.wrapping_add(1) & self.mask;
                    count += 1;
                }
                Err(err) => {
                    batch_err = Some(err);
                    break;
                }
            }
        }

        if count > 0 {
            // Single atomic store for all reads
            self.read_head().store(head, Ordering::Release);
            self.wait_read_ptr()
                .fetch_add(count as u32, Ordering::SeqCst);

            // PERF: Single conditional wake for entire batch
            if self.send_waiters().load(Ordering::SeqCst) > 0 {
                // Wake all waiting senders since we freed multiple slots
                let _ = sync::wake_all(self.wait_read_ptr());
            }
        }

        if let Some(err) = batch_err {
            return Err(err);
        }

        Ok(results)
    }

    /// Drain up to `max_items` slots in-place without allocating heap `Vec<u8>` buffers,
    /// advancing `read_head` and waking waiting senders once for the entire drained batch.
    pub fn drain_with<F>(&self, max_items: usize, mut f: F) -> Result<usize, ChannelError>
    where
        F: FnMut(&[u8], u32),
    {
        if max_items == 0 {
            return Ok(0);
        }
        if self.slot_borrowed.load(Ordering::Acquire) {
            return Err(ChannelError::SlotBorrowed);
        }

        let initial_head = self.read_head().load(Ordering::Relaxed);
        let initial_tail = self.load_consumer_tail(initial_head);
        if initial_head == initial_tail {
            return Ok(0);
        }

        if self
            .slot_borrowed
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Err(ChannelError::SlotBorrowed);
        }

        struct DrainCommitGuard<'a> {
            ring: &'a SpscRing,
            head: u64,
            count: usize,
        }
        impl Drop for DrainCommitGuard<'_> {
            fn drop(&mut self) {
                if self.count > 0 {
                    self.ring.read_head().store(self.head, Ordering::Release);
                    self.ring
                        .wait_read_ptr()
                        .fetch_add(self.count as u32, Ordering::SeqCst);
                    if self.ring.send_waiters().load(Ordering::SeqCst) > 0 {
                        let _ = sync::wake_all(self.ring.wait_read_ptr());
                    }
                }
                self.ring.slot_borrowed.store(false, Ordering::Release);
            }
        }

        let head = self.read_head().load(Ordering::Acquire);
        let mut tail = self.load_consumer_tail(head);
        if head == tail {
            self.slot_borrowed.store(false, Ordering::Release);
            return Ok(0);
        }

        let mut guard = DrainCommitGuard {
            ring: self,
            head,
            count: 0,
        };
        let cap = self.payload_capacity();

        loop {
            while guard.head != tail && guard.count < max_items {
                // SAFETY: guard.head != tail and Acquire load of write_head synchronized slot writes.
                let (data_ptr, length, flags) = unsafe { self.peek_slot(guard.head) };
                if length > cap {
                    return Err(ChannelError::CorruptedSlot);
                }
                // SAFETY: length <= payload_capacity() and slot remains borrowed until DrainCommitGuard drops.
                let slice = unsafe { core::slice::from_raw_parts(data_ptr, length) };
                guard.head = guard.head.wrapping_add(1) & self.mask;
                guard.count += 1;
                f(slice, flags);
            }

            if guard.count >= max_items {
                break;
            }
            let fresh_tail = self.write_head().load(Ordering::Acquire);
            if fresh_tail == tail {
                break;
            }
            self.cached_write_head
                .0
                .store(fresh_tail, Ordering::Release);
            tail = fresh_tail;
        }

        Ok(guard.count)
    }

    /// Read a slot into a new `Vec` without requiring a pre-allocated buffer.
    ///
    /// # Errors
    /// Returns [`ChannelError::CorruptedSlot`] if the slot header's `len` exceeds
    /// [`Self::payload_capacity`].
    pub fn read_slot_to_vec(&self, index: u64) -> Result<(Vec<u8>, u32), ChannelError> {
        std::sync::atomic::fence(Ordering::Acquire);
        // SAFETY: `self.slot_ptr(index)` masks `index` with `self.mask` (`slots - 1`) inside the
        // validated `layout.ring_size` region, and `layout.slot_size >= size_of::<SlotHeader>()`.
        let slot = unsafe { self.slot_ptr(index) };
        // SAFETY: `slot` lies within the validated ring region and is aligned to `SlotHeader`.
        let header = unsafe { &*slot.cast::<SlotHeader>() };
        let len = header.len as usize;
        if len > self.payload_capacity() {
            return Err(ChannelError::CorruptedSlot);
        }
        let flags = header.flags;
        // SAFETY: `SlotHeader` sits at the start of the slot; `payload_ptr` is within the slot's
        // `payload_capacity()` bytes and `len <= self.payload_capacity()` was checked above.
        let payload_ptr = unsafe { slot.add(core::mem::size_of::<SlotHeader>()) };

        let mut payload = vec![0u8; len];
        // SAFETY: `payload_ptr` points to `len` readable bytes inside the slot and `payload` is a
        // freshly allocated non-overlapping `Vec<u8>` of `len` bytes.
        unsafe {
            ptr::copy_nonoverlapping(payload_ptr, payload.as_mut_ptr(), len);
        }
        Ok((payload, flags))
    }

    /// Reset the ring to empty state. Used for reclaiming rings from dead workers.
    ///
    /// # Safety
    /// Caller must ensure no other thread is reading or writing to this ring.
    /// Typically called after a worker process has died and before respawning.
    pub unsafe fn reset(&self) {
        self.read_head().store(0, Ordering::Release);
        self.write_head().store(0, Ordering::Release);
        self.wait_read_ptr().store(0, Ordering::Release);
        self.wait_write_ptr().store(0, Ordering::Release);
        self.send_waiters().store(0, Ordering::Release);
        self.recv_waiters().store(0, Ordering::Release);
        self.consumer_sleeping().store(0, Ordering::Release);
        self.active_flags().store(0, Ordering::Release);
        self.cached_read_head.0.store(0, Ordering::Release);
        self.cached_write_head.0.store(0, Ordering::Release);
        self.slot_borrowed.store(false, Ordering::Release);
        // SAFETY: `self.header` points to a validated `ShmHeader` kept alive by `self.region`, and
        // `generation` is an `AtomicU32` safe to reference under shared `&self`.
        let header = unsafe { &*self.header };
        let next_gen = header
            .generation
            .fetch_add(1, Ordering::Release)
            .wrapping_add(1);
        self.cached_generation.store(next_gen, Ordering::Release);
    }

    fn read_head(&self) -> &core::sync::atomic::AtomicU64 {
        // SAFETY: `self.header` is aligned and valid for the lifetime of `self.region`.
        unsafe { &(*self.header).read_head.0 }
    }

    fn write_head(&self) -> &core::sync::atomic::AtomicU64 {
        // SAFETY: `self.header` is aligned and valid for the lifetime of `self.region`.
        unsafe { &(*self.header).write_head.0 }
    }

    fn wait_read_ptr(&self) -> &core::sync::atomic::AtomicU32 {
        // SAFETY: `self.header` is aligned and valid for the lifetime of `self.region`.
        unsafe { &(*self.header).wait_read.0 }
    }

    fn wait_write_ptr(&self) -> &core::sync::atomic::AtomicU32 {
        // SAFETY: `self.header` is aligned and valid for the lifetime of `self.region`.
        unsafe { &(*self.header).wait_write.0 }
    }

    /// Counter for threads blocked waiting to send (ring full).
    fn send_waiters(&self) -> &core::sync::atomic::AtomicU32 {
        // SAFETY: `self.header` is aligned and valid for the lifetime of `self.region`.
        unsafe { &(*self.header).send_waiters.0 }
    }

    /// Counter for threads blocked waiting to recv (ring empty).
    fn recv_waiters(&self) -> &core::sync::atomic::AtomicU32 {
        // SAFETY: `self.header` is aligned and valid for the lifetime of `self.region`.
        unsafe { &(*self.header).recv_waiters.0 }
    }

    unsafe fn slot_ptr(&self, index: u64) -> *mut u8 {
        let offset = (index as usize & self.mask as usize) * self.layout.slot_size;
        // SAFETY: `index & self.mask < self.layout.slots` and `validate_region_for_layout` verified
        // `slots * slot_size <= layout.ring_size` within `self.region`.
        unsafe { self.data.add(offset) }
    }

    unsafe fn write_slot(&self, index: u64, payload: &[u8], flags: u32) {
        // SAFETY: Caller guarantees exclusive producer ownership of slot `index` and
        // `payload.len() <= self.payload_capacity()`; `slot_ptr(index)` is `SlotHeader`-aligned.
        unsafe {
            let slot = self.slot_ptr(index);
            let header = slot.cast::<SlotHeader>();
            (*header).len = payload.len() as u32;
            (*header).flags = flags;
            let payload_ptr = slot.add(core::mem::size_of::<SlotHeader>());
            ptr::copy_nonoverlapping(payload.as_ptr(), payload_ptr, payload.len());
        }
    }

    unsafe fn read_slot(&self, index: u64, out: &mut [u8]) -> Result<usize, ChannelError> {
        // SAFETY: Caller guarantees slot `index` was published via Acquire load of `write_head`
        // and no producer overwrites it before `publish_read`; `len` is bounds-checked before copy.
        unsafe {
            let slot = self.slot_ptr(index);
            let header = &*slot.cast::<SlotHeader>();
            let len = header.len as usize;
            if len > self.payload_capacity() {
                return Err(ChannelError::CorruptedSlot);
            }
            if len > out.len() {
                return Err(ChannelError::BufferTooSmall {
                    len,
                    capacity: out.len(),
                });
            }
            let payload_ptr = slot.add(core::mem::size_of::<SlotHeader>());
            ptr::copy_nonoverlapping(payload_ptr, out.as_mut_ptr(), len);
            Ok(len)
        }
    }

    unsafe fn read_slot_with_flags(
        &self,
        index: u64,
        out: &mut [u8],
    ) -> Result<(usize, u32), ChannelError> {
        // SAFETY: Caller guarantees slot `index` was published via Acquire load of `write_head`
        // and no producer overwrites it before `publish_read`; `len` is bounds-checked before copy.
        unsafe {
            let slot = self.slot_ptr(index);
            let header = &*slot.cast::<SlotHeader>();
            let len = header.len as usize;
            if len > self.payload_capacity() {
                return Err(ChannelError::CorruptedSlot);
            }
            if len > out.len() {
                return Err(ChannelError::BufferTooSmall {
                    len,
                    capacity: out.len(),
                });
            }
            let payload_ptr = slot.add(core::mem::size_of::<SlotHeader>());
            ptr::copy_nonoverlapping(payload_ptr, out.as_mut_ptr(), len);
            Ok((len, header.flags))
        }
    }

    /// Peek at a slot without copying or advancing the cursor.
    ///
    /// Returns (data_ptr, length, flags).
    ///
    /// # Safety
    /// Caller must ensure the index is valid (head <= index < tail).
    unsafe fn peek_slot(&self, index: u64) -> (*const u8, usize, u32) {
        // SAFETY: Caller guarantees `index` is published and readable (`head <= index < tail`);
        // `slot_ptr(index)` is `SlotHeader`-aligned inside the validated ring region.
        unsafe {
            let slot = self.slot_ptr(index);
            let header = &*slot.cast::<SlotHeader>();
            let len = header.len as usize;
            let flags = header.flags;
            let payload_ptr = slot.add(core::mem::size_of::<SlotHeader>());
            (payload_ptr, len, flags)
        }
    }
}

/// Zero-copy view into an [`MpmcRing`] slot.
///
/// Holds a reference to the ring buffer and defers advancing the slot's
/// sequence number (`head + mask + 1`) until the view is dropped or explicitly
/// released. Multiple `MpmcSlotView`s for distinct slots may coexist concurrently.
pub struct MpmcSlotView {
    ring: Arc<MpmcRing>,
    slot_index: usize,
    release_sequence: u64,
    data_ptr: *const u8,
    length: usize,
    flags: u32,
    released: bool,
}

// SAFETY: `MpmcSlotView` holds an `Arc<MpmcRing>` keeping the underlying SHM region mapped,
// and the slot remains exclusively owned by this consumer until `release_sequence` is stored.
unsafe impl Send for MpmcSlotView {}
// SAFETY: Shared references to `MpmcSlotView` only expose immutable payload bytes via `as_slice()`
// while the slot cannot be reclaimed by producers until `release_sequence` is published on drop.
unsafe impl Sync for MpmcSlotView {}

impl MpmcSlotView {
    /// Get the payload data as a byte slice (zero-copy).
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: data_ptr points to valid memory in the ring slot with validated length,
        // and the slot cannot be overwritten until release() publishes release_sequence.
        unsafe { std::slice::from_raw_parts(self.data_ptr, self.length) }
    }

    /// Get the payload length.
    #[inline]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Check if payload is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Get the message flags.
    #[inline]
    pub fn flags(&self) -> u32 {
        self.flags
    }

    /// Explicitly release the slot back to the MPMC ring.
    pub fn release(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        self.ring
            .sequence(self.slot_index)
            .store(self.release_sequence, Ordering::Release);
    }
}

impl core::fmt::Debug for MpmcSlotView {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MpmcSlotView")
            .field("slot_index", &self.slot_index)
            .field("length", &self.length)
            .field("flags", &self.flags)
            .field("released", &self.released)
            .finish()
    }
}

impl Drop for MpmcSlotView {
    fn drop(&mut self) {
        self.release();
    }
}

impl AsRef<[u8]> for MpmcSlotView {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

#[derive(Debug)]
pub struct MpmcRing {
    /// Keeps the shared memory region alive for the lifetime of the ring.
    /// Not accessed directly after construction, but the Arc prevents deallocation.
    #[allow(dead_code)] // Intentional: Arc kept alive for memory lifetime, not accessed
    region: Arc<ShmRegion>,
    header: *mut ShmHeader,
    sequences: *mut CacheLine<AtomicU64>,
    data: *mut u8,
    layout: RingLayout,
    mask: u64,
}

// SAFETY: `MpmcRing` owns an `Arc<ShmRegion>` and synchronizes all header, sequence, and slot
// accesses across producers and consumers via atomic CAS and Acquire/Release operations.
unsafe impl Send for MpmcRing {}
// SAFETY: Concurrent access through `&MpmcRing` is governed by atomic `head`/`tail`/`sequence`
// slot ownership protocol without unsynchronized interior mutation.
unsafe impl Sync for MpmcRing {}

impl MpmcRing {
    /// Initialize a shared region in-place and return a ring handle.
    ///
    /// # Safety
    /// Caller must ensure exclusive initialization (single writer) and that
    /// the region is at least `layout.total_size` bytes.
    pub unsafe fn initialize(
        region: Arc<ShmRegion>,
        layout: RingLayout,
        creator_pid: u32,
        creator_epoch: u64,
    ) -> Result<Self, ChannelError> {
        if !layout.is_power_of_two()
            || layout.slot_size < core::mem::size_of::<SlotHeader>()
            || layout.mode != ChannelMode::Mpmc
        {
            return Err(ChannelError::InvalidLayout);
        }
        if region.len() < layout.total_size {
            return Err(ChannelError::InvalidLayout);
        }

        let header = region.as_ptr().cast::<ShmHeader>();
        // SAFETY: Caller guarantees exclusive initialization, `ShmHeader` alignment, and
        // `region.len() >= layout.total_size` (verified above) covering `ShmHeader`.
        let header_ref = unsafe { &mut *header };
        header_ref.magic = MAGIC;
        header_ref.abi_version = ABI_VERSION;
        header_ref.flags = 0;
        header_ref.total_size = layout.total_size as u64;
        header_ref.ring_offset = layout.ring_offset() as u64;
        header_ref.ring_size = layout.ring_size as u64;
        header_ref.generation.store(1, Ordering::Relaxed);
        header_ref
            .state
            .store(RegionState::Ready as u32, Ordering::Release);
        header_ref.creator_pid = creator_pid;
        header_ref.creator_epoch = creator_epoch;
        header_ref.read_head.0.store(0, Ordering::Relaxed);
        header_ref.write_head.0.store(0, Ordering::Relaxed);
        header_ref.wait_read.0.store(0, Ordering::Relaxed);
        header_ref.wait_write.0.store(0, Ordering::Relaxed);
        header_ref.send_waiters.0.store(0, Ordering::Relaxed);
        header_ref.recv_waiters.0.store(0, Ordering::Relaxed);
        header_ref.consumer_sleeping.0.store(0, Ordering::Relaxed);
        header_ref.active_flags.0.store(0, Ordering::Relaxed);

        // SAFETY: Caller guarantees exclusive initialization and valid region bounds for `layout`;
        // `sequences` covers `layout.slots` cache-line-aligned entries before `Self::from_region`.
        unsafe {
            let sequences = region
                .as_ptr()
                .add(layout.seq_offset())
                .cast::<CacheLine<AtomicU64>>();
            for i in 0..layout.slots {
                (*sequences.add(i)).0.store(i as u64, Ordering::Relaxed);
            }
            Self::from_region(region, layout)
        }
    }

    /// Attach to an existing shared region.
    ///
    /// # Safety
    /// Caller must ensure the region stays mapped for the lifetime of the ring.
    pub unsafe fn attach(region: Arc<ShmRegion>, layout: RingLayout) -> Result<Self, ChannelError> {
        validate_region_for_layout(
            &region,
            &layout,
            core::mem::align_of::<ShmHeader>().max(core::mem::align_of::<CacheLine<AtomicU64>>()),
        )?;
        if layout.mode != ChannelMode::Mpmc {
            return Err(ChannelError::InvalidLayout);
        }
        let header = region.as_ptr().cast::<ShmHeader>();
        // SAFETY: `validate_region_for_layout` verified `region` is aligned and covers `ShmHeader`.
        let header_ref = unsafe { &*header };
        if header_ref.magic != MAGIC || header_ref.abi_version != ABI_VERSION {
            return Err(ChannelError::NotReady);
        }
        if header_ref.state.load(Ordering::Acquire) != RegionState::Ready as u32 {
            return Err(ChannelError::NotReady);
        }
        // SAFETY: header magic, ABI version, ready state, and MPMC layout are verified above.
        unsafe { Self::from_region(region, layout) }
    }

    /// Create a ring without initializing the header.
    ///
    /// # Safety
    /// Caller must ensure the region is already initialized correctly.
    pub unsafe fn from_region(
        region: Arc<ShmRegion>,
        layout: RingLayout,
    ) -> Result<Self, ChannelError> {
        validate_region_for_layout(
            &region,
            &layout,
            core::mem::align_of::<ShmHeader>().max(core::mem::align_of::<CacheLine<AtomicU64>>()),
        )?;
        if layout.mode != ChannelMode::Mpmc {
            return Err(ChannelError::InvalidLayout);
        }
        let header = region.as_ptr().cast::<ShmHeader>();
        // SAFETY: `validate_region_for_layout` verified `seq_offset` alignment and bounds for `slots` entries.
        let sequences = unsafe {
            region
                .as_ptr()
                .add(layout.seq_offset())
                .cast::<CacheLine<AtomicU64>>()
        };
        // SAFETY: `validate_region_for_layout` verified `ring_offset + ring_size <= region.len()`.
        let data = unsafe { region.as_ptr().add(layout.ring_offset()) };
        Ok(Self {
            region,
            header,
            sequences,
            data,
            layout,
            mask: (layout.slots as u64).wrapping_sub(1),
        })
    }

    /// Create a new anonymous MPMC ring.
    pub fn new(slot_size: usize, slots: usize) -> Result<Self, ChannelError> {
        let layout = RingLayout::new(slot_size, slots, ChannelMode::Mpmc);
        let region = Arc::new(ShmRegion::create(layout.total_size)?);
        // SAFETY: `region` was freshly created with `layout.total_size` bytes and exclusive ownership.
        unsafe { Self::initialize(region, layout, 0, 0) }
    }

    pub fn payload_capacity(&self) -> usize {
        self.layout.slot_size - core::mem::size_of::<SlotHeader>()
    }

    /// Get the underlying shared memory region's handle (FD on Unix).
    #[cfg(unix)]
    pub fn region_handle(&self) -> Option<std::os::fd::RawFd> {
        self.region.handle()
    }

    pub fn stats(&self) -> RingStats {
        RingStats {
            slot_size: self.layout.slot_size,
            slots: self.layout.slots,
            read_head: self.head().load(Ordering::Acquire),
            write_head: self.tail().load(Ordering::Acquire),
        }
    }

    /// Check if data is available in the ring buffer.
    ///
    /// This is a fast check using Relaxed ordering - suitable for spin loops.
    #[inline]
    pub fn has_data(&self) -> bool {
        let head = self.head().load(Ordering::Relaxed);
        let tail = self.tail().load(Ordering::Relaxed);
        head != tail
    }

    /// Check if at least one slot is currently available for writing.
    #[inline]
    pub fn has_capacity(&self) -> bool {
        let tail = self.tail().load(Ordering::Relaxed);
        let index = (tail & self.mask) as usize;
        let seq = self.sequence(index).load(Ordering::Acquire);
        (seq as i64 - tail as i64) >= 0
    }

    fn head(&self) -> &AtomicU64 {
        // SAFETY: `self.header` is aligned and valid for the lifetime of `self.region`.
        unsafe { &(*self.header).read_head.0 }
    }

    fn tail(&self) -> &AtomicU64 {
        // SAFETY: `self.header` is aligned and valid for the lifetime of `self.region`.
        unsafe { &(*self.header).write_head.0 }
    }

    fn sequence(&self, index: usize) -> &AtomicU64 {
        // SAFETY: Callers mask `index` with `self.mask` (`slots - 1`) and `validate_region_for_layout`
        // verified `self.sequences` is aligned and covers `slots` `CacheLine<AtomicU64>` entries.
        unsafe { &(*self.sequences.add(index)).0 }
    }

    /// Access the Dekker `consumer_sleeping` flag in `ShmHeader`.
    #[inline]
    pub fn consumer_sleeping(&self) -> &AtomicU32 {
        // SAFETY: `self.header` is aligned and valid for the lifetime of `self.region`.
        unsafe { &(*self.header).consumer_sleeping.0 }
    }

    /// Access the `active_flags` bitmask in `ShmHeader`.
    #[inline]
    pub fn active_flags(&self) -> &AtomicU32 {
        // SAFETY: `self.header` is aligned and valid for the lifetime of `self.region`.
        unsafe { &(*self.header).active_flags.0 }
    }

    /// Check whether the current head slot has a committed message ready to claim.
    #[inline]
    pub fn has_ready_slot(&self) -> bool {
        let head = self.head().load(Ordering::Acquire);
        let index = (head & self.mask) as usize;
        let seq = self.sequence(index).load(Ordering::Acquire);
        (seq as i64 - (head + 1) as i64) >= 0
    }

    /// Arm the Dekker `consumer_sleeping` flag (`1`) and re-verify no committed slot is ready.
    #[inline]
    pub fn prepare_sleep(&self) -> bool {
        self.consumer_sleeping().store(1, Ordering::SeqCst);
        std::sync::atomic::fence(Ordering::SeqCst);
        if self.has_ready_slot() {
            self.consumer_sleeping().store(0, Ordering::SeqCst);
            return false;
        }
        true
    }

    /// Disarm the Dekker `consumer_sleeping` flag (`0`).
    #[inline]
    pub fn cancel_sleep(&self) {
        self.consumer_sleeping().store(0, Ordering::SeqCst);
    }

    /// Atomically check and clear `consumer_sleeping` after publishing a slot (`SeqCst` Dekker).
    #[inline]
    pub fn should_notify_and_clear(&self) -> bool {
        std::sync::atomic::fence(Ordering::SeqCst);
        self.consumer_sleeping().swap(0, Ordering::SeqCst) != 0
    }

    /// Notify `notifier` only if the consumer has armed `consumer_sleeping == 1`.
    #[inline]
    pub fn notify_if_sleeping(&self, notifier: &Notifier) -> std::io::Result<bool> {
        if self.should_notify_and_clear() {
            notifier.notify()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn try_send(&self, payload: &[u8], flags: u32) -> Result<(), ChannelError> {
        if payload.len() > self.payload_capacity() {
            return Err(ChannelError::PayloadTooLarge {
                len: payload.len(),
                capacity: self.payload_capacity(),
            });
        }

        let mut tail = self.tail().load(Ordering::Relaxed);
        loop {
            let index = (tail & self.mask) as usize;
            let seq = self.sequence(index).load(Ordering::Acquire);
            let diff = seq as i64 - tail as i64;

            if diff == 0 {
                match self.tail().compare_exchange(
                    tail,
                    tail + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: CAS claimed exclusive producer ownership of `index` (`seq == tail`),
                        // and `payload.len() <= self.payload_capacity()` was checked above.
                        unsafe {
                            self.write_slot(index, payload, flags);
                        }
                        self.sequence(index).store(tail + 1, Ordering::Release);
                        return Ok(());
                    }
                    Err(actual) => {
                        tail = actual;
                        continue;
                    }
                }
            } else if diff < 0 {
                return Err(ChannelError::Full);
            } else {
                tail = self.tail().load(Ordering::Relaxed);
            }
        }
    }

    /// Zero-copy send: reserve a slot and call `f` to write the payload.
    ///
    /// # Arguments
    ///
    /// * `flags` - Protocol flags for the slot
    /// * `f` - Closure that receives a mutable slice to the slot payload and returns number of bytes written.
    pub fn try_send_with<F>(&self, flags: u32, f: F) -> Result<(), ChannelError>
    where
        F: FnOnce(&mut [u8]) -> usize,
    {
        let mut tail = self.tail().load(Ordering::Relaxed);
        loop {
            let index = (tail & self.mask) as usize;
            let seq = self.sequence(index).load(Ordering::Acquire);
            let diff = seq as i64 - tail as i64;

            if diff == 0 {
                match self.tail().compare_exchange(
                    tail,
                    tail + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        struct MpmcSendGuard<'a> {
                            header: *mut SlotHeader,
                            seq: &'a AtomicU64,
                            release_seq: u64,
                            armed: bool,
                        }
                        impl<'a> Drop for MpmcSendGuard<'a> {
                            fn drop(&mut self) {
                                if self.armed {
                                    // SAFETY: `self.header` points to the claimed slot exclusively
                                    // owned by this producer until `self.seq.store(Release)` below.
                                    unsafe {
                                        (*self.header).len = 0;
                                        (*self.header).flags = SLOT_FLAG_ABORTED;
                                    }
                                    self.seq.store(self.release_seq, Ordering::Release);
                                }
                            }
                        }

                        // SAFETY: `index` is masked by `self.mask` inside the validated ring region.
                        let slot = unsafe { self.slot_ptr(index) };
                        let header = slot.cast::<SlotHeader>();
                        let mut guard = MpmcSendGuard {
                            header,
                            seq: self.sequence(index),
                            release_seq: tail + 1,
                            armed: true,
                        };

                        // SAFETY: `SlotHeader` sits at the start of `slot`, leaving
                        // `self.payload_capacity()` writable bytes inside the claimed slot.
                        let payload_ptr = unsafe { slot.add(core::mem::size_of::<SlotHeader>()) };
                        // SAFETY: `payload_ptr` is exclusively owned by this producer for
                        // `self.payload_capacity()` bytes until `sequence.store(Release)`.
                        let payload_slice = unsafe {
                            core::slice::from_raw_parts_mut(payload_ptr, self.payload_capacity())
                        };
                        let written = f(payload_slice);
                        if written > self.payload_capacity() {
                            return Err(ChannelError::PayloadTooLarge {
                                len: written,
                                capacity: self.payload_capacity(),
                            });
                        }
                        // SAFETY: `header` points to the exclusively claimed `SlotHeader`.
                        unsafe {
                            (*header).len = written as u32;
                            (*header).flags = flags;
                        }
                        guard.armed = false;
                        self.sequence(index).store(tail + 1, Ordering::Release);
                        return Ok(());
                    }
                    Err(actual) => {
                        tail = actual;
                        continue;
                    }
                }
            } else if diff < 0 {
                return Err(ChannelError::Full);
            } else {
                tail = self.tail().load(Ordering::Relaxed);
            }
        }
    }

    pub fn send(&self, payload: &[u8], flags: u32) -> Result<(), ChannelError> {
        let backoff = Backoff::new();
        loop {
            match self.try_send(payload, flags) {
                Ok(()) => return Ok(()),
                Err(ChannelError::Full) => {
                    backoff.snooze();
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn try_recv(&self, out: &mut [u8]) -> Result<usize, ChannelError> {
        let mut head = self.head().load(Ordering::Relaxed);
        loop {
            let index = (head & self.mask) as usize;
            let seq = self.sequence(index).load(Ordering::Acquire);
            let diff = seq as i64 - (head + 1) as i64;

            if diff == 0 {
                match self.head().compare_exchange(
                    head,
                    head + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: `index` is masked by `self.mask` inside the validated ring region.
                        let slot = unsafe { self.slot_ptr(index) };
                        // SAFETY: Acquire load of `sequence` synchronized producer writes and CAS
                        // granted exclusive consumer ownership of `SlotHeader` until release below.
                        let header = unsafe { &*slot.cast::<SlotHeader>() };
                        if (header.flags & SLOT_FLAG_ABORTED) != 0 {
                            self.sequence(index)
                                .store(head + self.mask + 1, Ordering::Release);
                            head = self.head().load(Ordering::Relaxed);
                            continue;
                        }
                        // SAFETY: Slot `index` is exclusively claimed by this consumer until
                        // `sequence.store(Release)` advances the slot's cycle.
                        let res = unsafe { self.read_slot(index, out) };
                        self.sequence(index)
                            .store(head + self.mask + 1, Ordering::Release);
                        return res;
                    }
                    Err(actual) => {
                        head = actual;
                        continue;
                    }
                }
            } else if diff < 0 {
                return Err(ChannelError::Empty);
            } else {
                head = self.head().load(Ordering::Relaxed);
            }
        }
    }

    pub fn try_recv_with_flags(&self, out: &mut [u8]) -> Result<(usize, u32), ChannelError> {
        let mut head = self.head().load(Ordering::Relaxed);
        loop {
            let index = (head & self.mask) as usize;
            let seq = self.sequence(index).load(Ordering::Acquire);
            let diff = seq as i64 - (head + 1) as i64;

            if diff == 0 {
                match self.head().compare_exchange(
                    head,
                    head + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: `index` is masked by `self.mask` inside the validated ring region.
                        let slot = unsafe { self.slot_ptr(index) };
                        // SAFETY: Acquire load of `sequence` synchronized producer writes and CAS
                        // granted exclusive consumer ownership of `SlotHeader` until release below.
                        let header = unsafe { &*slot.cast::<SlotHeader>() };
                        if (header.flags & SLOT_FLAG_ABORTED) != 0 {
                            self.sequence(index)
                                .store(head + self.mask + 1, Ordering::Release);
                            head = self.head().load(Ordering::Relaxed);
                            continue;
                        }
                        // SAFETY: Slot `index` is exclusively claimed by this consumer until
                        // `sequence.store(Release)` advances the slot's cycle.
                        let res = unsafe { self.read_slot_with_flags(index, out) };
                        self.sequence(index)
                            .store(head + self.mask + 1, Ordering::Release);
                        return res;
                    }
                    Err(actual) => {
                        head = actual;
                        continue;
                    }
                }
            } else if diff < 0 {
                return Err(ChannelError::Empty);
            } else {
                head = self.head().load(Ordering::Relaxed);
            }
        }
    }

    pub fn recv(&self, out: &mut [u8]) -> Result<usize, ChannelError> {
        let backoff = Backoff::new();
        loop {
            match self.try_recv(out) {
                Ok(len) => return Ok(len),
                Err(ChannelError::Empty) => backoff.snooze(),
                Err(err) => return Err(err),
            }
        }
    }

    pub fn recv_timeout(
        &self,
        out: &mut [u8],
        timeout: Duration,
    ) -> Result<Option<usize>, ChannelError> {
        let deadline = Instant::now() + timeout;
        let backoff = Backoff::new();
        loop {
            match self.try_recv(out) {
                Ok(len) => return Ok(Some(len)),
                Err(ChannelError::Empty) => {
                    if Instant::now() >= deadline {
                        return Ok(None);
                    }
                    backoff.snooze();
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Try to receive a message as a zero-copy [`MpmcSlotView`].
    ///
    /// Multiple `MpmcSlotView`s for distinct slots may coexist simultaneously; each slot's
    /// sequence counter is released independently when its view is dropped.
    pub fn try_recv_view(self: &Arc<Self>) -> Result<MpmcSlotView, ChannelError> {
        let mut head = self.head().load(Ordering::Relaxed);
        loop {
            let index = (head & self.mask) as usize;
            let seq = self.sequence(index).load(Ordering::Acquire);
            let diff = seq as i64 - (head + 1) as i64;

            if diff == 0 {
                match self.head().compare_exchange(
                    head,
                    head + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let release_sequence = head + self.mask + 1;
                        // SAFETY: Acquire load of `sequence` synchronized producer writes and CAS
                        // granted exclusive consumer ownership of slot `index` until `release_sequence`.
                        let (len, flags, payload_ptr) = unsafe {
                            let slot = self.slot_ptr(index);
                            let header = &*slot.cast::<SlotHeader>();
                            (
                                header.len as usize,
                                header.flags,
                                slot.add(core::mem::size_of::<SlotHeader>()),
                            )
                        };
                        if (flags & SLOT_FLAG_ABORTED) != 0 {
                            self.sequence(index)
                                .store(release_sequence, Ordering::Release);
                            head = self.head().load(Ordering::Relaxed);
                            continue;
                        }
                        if len > self.payload_capacity() {
                            self.sequence(index)
                                .store(release_sequence, Ordering::Release);
                            return Err(ChannelError::CorruptedSlot);
                        }
                        return Ok(MpmcSlotView {
                            ring: Arc::clone(self),
                            slot_index: index,
                            release_sequence,
                            data_ptr: payload_ptr,
                            length: len,
                            flags,
                            released: false,
                        });
                    }
                    Err(actual) => {
                        head = actual;
                        continue;
                    }
                }
            } else if diff < 0 {
                return Err(ChannelError::Empty);
            } else {
                head = self.head().load(Ordering::Relaxed);
            }
        }
    }

    /// Zero-copy receive: claim a slot and pass a slice of its payload and flags to `reader`
    /// before releasing the slot's sequence number.
    pub fn try_recv_with<F, R>(&self, reader: F) -> Result<Option<(R, u32)>, ChannelError>
    where
        F: FnOnce(&[u8], u32) -> R,
    {
        let mut head = self.head().load(Ordering::Relaxed);
        loop {
            let index = (head & self.mask) as usize;
            let seq = self.sequence(index).load(Ordering::Acquire);
            let diff = seq as i64 - (head + 1) as i64;

            if diff == 0 {
                match self.head().compare_exchange(
                    head,
                    head + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: Acquire load of `sequence` synchronized producer writes and CAS
                        // granted exclusive consumer ownership of slot `index` until `sequence.store(Release)`.
                        let (len, flags, payload_ptr) = unsafe {
                            let slot = self.slot_ptr(index);
                            let header = &*slot.cast::<SlotHeader>();
                            (
                                header.len as usize,
                                header.flags,
                                slot.add(core::mem::size_of::<SlotHeader>()),
                            )
                        };
                        if (flags & SLOT_FLAG_ABORTED) != 0 {
                            self.sequence(index)
                                .store(head + self.mask + 1, Ordering::Release);
                            head = self.head().load(Ordering::Relaxed);
                            continue;
                        }
                        if len > self.payload_capacity() {
                            self.sequence(index)
                                .store(head + self.mask + 1, Ordering::Release);
                            return Err(ChannelError::CorruptedSlot);
                        }
                        // SAFETY: `len <= self.payload_capacity()` was checked above and the slot
                        // remains exclusively owned by this consumer until `sequence.store(Release)`.
                        let result = unsafe {
                            let payload_slice = core::slice::from_raw_parts(payload_ptr, len);
                            reader(payload_slice, flags)
                        };
                        self.sequence(index)
                            .store(head + self.mask + 1, Ordering::Release);
                        return Ok(Some((result, flags)));
                    }
                    Err(actual) => {
                        head = actual;
                        continue;
                    }
                }
            } else if diff < 0 {
                return Ok(None);
            } else {
                head = self.head().load(Ordering::Relaxed);
            }
        }
    }

    /// Zero-copy receive with timeout.
    pub fn recv_timeout_with<F, R>(
        &self,
        timeout: Duration,
        mut reader: F,
    ) -> Result<Option<(R, u32)>, ChannelError>
    where
        F: FnMut(&[u8], u32) -> R,
    {
        let deadline = Instant::now() + timeout;
        let backoff = Backoff::new();
        loop {
            match self.try_recv_with(&mut reader) {
                Ok(Some(res)) => return Ok(Some(res)),
                Ok(None) => {
                    if Instant::now() >= deadline {
                        return Ok(None);
                    }
                    backoff.snooze();
                }
                Err(err) => return Err(err),
            }
        }
    }

    unsafe fn slot_ptr(&self, index: usize) -> *mut u8 {
        let offset = index * self.layout.slot_size;
        // SAFETY: Callers mask `index` with `self.mask` (`slots - 1`) and
        // `validate_region_for_layout` verified `slots * slot_size <= layout.ring_size`.
        unsafe { self.data.add(offset) }
    }

    unsafe fn write_slot(&self, index: usize, payload: &[u8], flags: u32) {
        // SAFETY: Caller guarantees exclusive producer ownership of slot `index` and
        // `payload.len() <= self.payload_capacity()`; `slot_ptr(index)` is `SlotHeader`-aligned.
        unsafe {
            let slot = self.slot_ptr(index);
            let header = slot.cast::<SlotHeader>();
            (*header).len = payload.len() as u32;
            (*header).flags = flags;
            let payload_ptr = slot.add(core::mem::size_of::<SlotHeader>());
            ptr::copy_nonoverlapping(payload.as_ptr(), payload_ptr, payload.len());
        }
    }

    unsafe fn read_slot(&self, index: usize, out: &mut [u8]) -> Result<usize, ChannelError> {
        // SAFETY: Caller guarantees slot `index` was synchronized via Acquire load of `sequence`
        // and exclusively claimed until release; `len` is bounds-checked before copy.
        unsafe {
            let slot = self.slot_ptr(index);
            let header = &*slot.cast::<SlotHeader>();
            let len = header.len as usize;
            if len > self.payload_capacity() {
                return Err(ChannelError::CorruptedSlot);
            }
            if len > out.len() {
                return Err(ChannelError::BufferTooSmall {
                    len,
                    capacity: out.len(),
                });
            }
            let payload_ptr = slot.add(core::mem::size_of::<SlotHeader>());
            ptr::copy_nonoverlapping(payload_ptr, out.as_mut_ptr(), len);
            Ok(len)
        }
    }

    unsafe fn read_slot_with_flags(
        &self,
        index: usize,
        out: &mut [u8],
    ) -> Result<(usize, u32), ChannelError> {
        // SAFETY: Caller guarantees slot `index` was synchronized via Acquire load of `sequence`
        // and exclusively claimed until release; `len` is bounds-checked before copy.
        unsafe {
            let slot = self.slot_ptr(index);
            let header = &*slot.cast::<SlotHeader>();
            let len = header.len as usize;
            if len > self.payload_capacity() {
                return Err(ChannelError::CorruptedSlot);
            }
            if len > out.len() {
                return Err(ChannelError::BufferTooSmall {
                    len,
                    capacity: out.len(),
                });
            }
            let payload_ptr = slot.add(core::mem::size_of::<SlotHeader>());
            ptr::copy_nonoverlapping(payload_ptr, out.as_mut_ptr(), len);
            Ok((len, header.flags))
        }
    }

    /// Drain up to `max` messages without blocking.
    ///
    /// For MPMC rings, this uses a loop of CAS operations internally,
    /// but amortizes the Vec allocations and Python/Rust boundary crossings.
    ///
    /// # Arguments
    ///
    /// * `max` - Maximum number of messages to receive
    ///
    /// # Returns
    ///
    /// Vector of `(payload, flags)` tuples. Returns empty Vec if ring is empty.
    pub fn try_recv_batch(&self, max: usize) -> Vec<(Vec<u8>, u32)> {
        let mut results = Vec::with_capacity(max.min(self.layout.slots));
        let capacity = self.payload_capacity();
        let mut buffer = vec![0u8; capacity];

        for _ in 0..max {
            match self.try_recv_with_flags(&mut buffer) {
                Ok((len, flags)) => {
                    results.push((buffer[..len].to_vec(), flags));
                }
                Err(ChannelError::Empty) => break,
                Err(_) => break, // Stop on any error
            }
        }

        results
    }

    /// Reset the ring to empty state.
    ///
    /// # Safety
    /// Caller must ensure no other thread is reading or writing to this ring.
    pub unsafe fn reset(&self) {
        self.head().store(0, Ordering::Release);
        self.tail().store(0, Ordering::Release);
        self.consumer_sleeping().store(0, Ordering::Release);
        self.active_flags().store(0, Ordering::Release);
        // SAFETY: Caller guarantees exclusive access (no concurrent readers/writers); `self.sequences`
        // covers `self.layout.slots` entries and `self.header` is valid for `self.region`.
        unsafe {
            for i in 0..self.layout.slots {
                (*self.sequences.add(i))
                    .0
                    .store(i as u64, Ordering::Release);
            }
            // Bump generation
            let header = &*self.header;
            header.generation.fetch_add(1, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::RingLayout;
    use proptest::prelude::*;
    use std::sync::Arc;

    #[test]
    fn spsc_round_trip() {
        let layout = RingLayout::new(256, 8, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                .unwrap();
        // SAFETY: `alloc_layout` has non-zero size (`layout.total_size`).
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        // SAFETY: `ptr` was allocated with `alloc_layout` (`layout.total_size` bytes) and outlives `region`.
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        // SAFETY: exclusive initialization of freshly allocated `region` matching `layout`.
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };
        let payload = b"hello";
        ring.send(payload, 0).unwrap();
        let mut out = [0u8; 32];
        let len = ring.recv(&mut out).unwrap();
        assert_eq!(&out[..len], payload);
        drop(ring);
        drop(region);
        // SAFETY: `ptr` was allocated with `alloc_layout` and all ring/region handles are dropped.
        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
        }
    }

    #[test]
    fn spsc_timeouts() {
        let layout = RingLayout::new(256, 2, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                .unwrap();
        // SAFETY: `alloc_layout` has non-zero size (`layout.total_size`).
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        // SAFETY: `ptr` was allocated with `alloc_layout` (`layout.total_size` bytes) and outlives `region`.
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        // SAFETY: exclusive initialization of freshly allocated `region` matching `layout`.
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };
        let payload = [1u8; 8];
        ring.send(&payload, 0).unwrap();
        let sent = ring
            .send_timeout(&payload, 0, std::time::Duration::from_millis(1))
            .unwrap();
        assert!(!sent);
        let mut out = [0u8; 8];
        ring.recv(&mut out).unwrap();
        let recv = ring
            .recv_timeout(&mut out, std::time::Duration::from_millis(1))
            .unwrap();
        assert!(recv.is_none());
        drop(ring);
        drop(region);
        // SAFETY: `ptr` was allocated with `alloc_layout` and all ring/region handles are dropped.
        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
        }
    }

    #[test]
    fn spsc_full_empty_edges() {
        let layout = RingLayout::new(128, 2, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                .unwrap();
        // SAFETY: `alloc_layout` has non-zero size (`layout.total_size`).
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        // SAFETY: `ptr` was allocated with `alloc_layout` (`layout.total_size` bytes) and outlives `region`.
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        // SAFETY: exclusive initialization of freshly allocated `region` matching `layout`.
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };
        let payload = [1u8; 8];
        ring.try_send(&payload, 0).unwrap();
        assert!(matches!(
            ring.try_send(&payload, 0),
            Err(ChannelError::Full)
        ));
        let mut out = [0u8; 8];
        ring.try_recv(&mut out).unwrap();
        assert!(matches!(ring.try_recv(&mut out), Err(ChannelError::Empty)));
        drop(ring);
        drop(region);
        // SAFETY: `ptr` was allocated with `alloc_layout` and all ring/region handles are dropped.
        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
        }
    }

    #[test]
    fn spsc_multi_threaded() {
        let layout = RingLayout::new(128, 8, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                .unwrap();
        // SAFETY: `alloc_layout` has non-zero size (`layout.total_size`).
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        // SAFETY: `ptr` was allocated with `alloc_layout` (`layout.total_size` bytes) and outlives `region`.
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        // SAFETY: exclusive initialization of freshly allocated `region` matching `layout`.
        let ring =
            Arc::new(unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() });
        let ring_tx = Arc::clone(&ring);
        let ring_rx = Arc::clone(&ring);
        std::thread::scope(|s| {
            s.spawn(|| {
                for i in 0u8..20 {
                    ring_tx.send(&[i], 0).unwrap();
                }
            });
            s.spawn(|| {
                let mut out = [0u8; 8];
                for i in 0u8..20 {
                    let len = ring_rx.recv(&mut out).unwrap();
                    assert_eq!(len, 1);
                    assert_eq!(out[0], i);
                }
            });
        });
        drop(ring);
        drop(region);
        // SAFETY: `ptr` was allocated with `alloc_layout` and all ring/region handles are dropped.
        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
        }
    }

    #[test]
    fn spsc_recv_hybrid_immediate() {
        let layout = RingLayout::new(256, 8, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                .unwrap();
        // SAFETY: `alloc_layout` has non-zero size (`layout.total_size`).
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        // SAFETY: `ptr` was allocated with `alloc_layout` (`layout.total_size` bytes) and outlives `region`.
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        // SAFETY: exclusive initialization of freshly allocated `region` matching `layout`.
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };

        // Pre-fill with data
        let payload = b"hello";
        ring.send(payload, 0).unwrap();

        // recv_hybrid should return immediately
        let mut out = [0u8; 32];
        let len = ring
            .recv_hybrid(&mut out, 5, std::time::Duration::from_millis(100))
            .unwrap()
            .expect("should receive immediately");
        assert_eq!(&out[..len], payload);

        drop(ring);
        drop(region);
        // SAFETY: `ptr` was allocated with `alloc_layout` and all ring/region handles are dropped.
        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
        }
    }

    #[test]
    fn spsc_recv_hybrid_wakeup() {
        use std::sync::atomic::AtomicBool;

        let layout = RingLayout::new(256, 8, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                .unwrap();
        // SAFETY: `alloc_layout` has non-zero size (`layout.total_size`).
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        // SAFETY: `ptr` was allocated with `alloc_layout` (`layout.total_size` bytes) and outlives `region`.
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        // SAFETY: exclusive initialization of freshly allocated `region` matching `layout`.
        let ring =
            Arc::new(unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() });

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let done = Arc::new(AtomicBool::new(false));
        let ring_clone = Arc::clone(&ring);
        let barrier_clone = Arc::clone(&barrier);
        let done_clone = Arc::clone(&done);
        let handle = std::thread::spawn(move || {
            barrier_clone.wait();
            while ring_clone.recv_waiters().load(Ordering::Acquire) == 0
                && !done_clone.load(Ordering::Acquire)
            {
                std::thread::yield_now();
            }
            ring_clone.send(b"delayed", 0).unwrap();
        });

        // recv_hybrid should spin briefly then block, then wake on send
        barrier.wait();
        let mut out = [0u8; 32];
        let len = ring
            .recv_hybrid(&mut out, 5, std::time::Duration::from_millis(500))
            .unwrap()
            .expect("should wake up on send");
        done.store(true, Ordering::Release);
        assert_eq!(&out[..len], b"delayed");

        handle.join().unwrap();

        // SAFETY: `ring` and `region` are dropped after `handle.join()` or no longer accessed; wait:
        // `ring` and `region` are still in scope, so drop them before freeing `ptr`.
        drop(ring);
        drop(region);
        // SAFETY: `ptr` was allocated with `alloc_layout` and all references via `region`/`ring` are dropped.
        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
        }
    }

    #[test]
    fn spsc_recv_hybrid_timeout() {
        let layout = RingLayout::new(256, 8, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                .unwrap();
        // SAFETY: `alloc_layout` has non-zero size (`layout.total_size`).
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        // SAFETY: `ptr` was allocated with `alloc_layout` (`layout.total_size` bytes) and outlives `region`.
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        // SAFETY: exclusive initialization of freshly allocated `region` matching `layout`.
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };

        // recv_hybrid should timeout with no data
        let mut out = [0u8; 32];
        let result = ring
            .recv_hybrid(&mut out, 5, std::time::Duration::from_millis(10))
            .unwrap();
        assert!(result.is_none());

        drop(ring);
        drop(region);
        // SAFETY: `ptr` was allocated with `alloc_layout` and all ring/region handles are dropped.
        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
        }
    }

    #[test]
    fn mpmc_multi_threaded() {
        let ring = Arc::new(MpmcRing::new(64, 64).unwrap());
        let total = 200u32;
        let received = Arc::new(std::sync::Mutex::new(vec![false; total as usize]));
        std::thread::scope(|s| {
            for producer in 0..2u32 {
                let ring = Arc::clone(&ring);
                s.spawn(move || {
                    for i in 0..(total / 2) {
                        let value = producer * (total / 2) + i;
                        let payload = value.to_le_bytes();
                        ring.send(&payload, 0).unwrap();
                    }
                });
            }
            for _ in 0..2 {
                let ring = Arc::clone(&ring);
                let received = Arc::clone(&received);
                s.spawn(move || {
                    let mut out = [0u8; 8];
                    for _ in 0..(total / 2) {
                        let len = ring.recv(&mut out).unwrap();
                        assert_eq!(len, 4);
                        let value = u32::from_le_bytes(out[0..4].try_into().unwrap());
                        received.lock().unwrap()[value as usize] = true;
                    }
                });
            }
        });
        let received = received.lock().unwrap();
        assert!(received.iter().all(|&seen| seen));
    }

    proptest! {
        #[test]
        fn spsc_fifo_order(values in proptest::collection::vec(0u8..=255, 1..32)) {
            let layout = RingLayout::new(64, 64, ChannelMode::Spsc);
            let alloc_layout =
                std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                    .unwrap();
            // SAFETY: `alloc_layout` has non-zero size (`layout.total_size`).
            let ptr = unsafe { std::alloc::alloc(alloc_layout) };
            assert!(!ptr.is_null());
            // SAFETY: `ptr` was allocated with `alloc_layout` (`layout.total_size` bytes) and outlives `region`.
            let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
            // SAFETY: exclusive initialization of freshly allocated `region` matching `layout`.
            let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };
            for value in &values {
                ring.send(&[*value], 0).unwrap();
            }
            let mut out = [0u8; 8];
            for value in values {
                let len = ring.recv(&mut out).unwrap();
                prop_assert_eq!(len, 1);
                prop_assert_eq!(out[0], value);
            }
            drop(ring);
            drop(region);
            // SAFETY: `ptr` was allocated with `alloc_layout` and all ring/region handles are dropped.
            unsafe {
                std::alloc::dealloc(ptr, alloc_layout);
            }
        }

        #[test]
        fn spsc_lossless(values in proptest::collection::vec(0u8..=255, 1..32)) {
            let layout = RingLayout::new(64, 64, ChannelMode::Spsc);
            let alloc_layout =
                std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                    .unwrap();
            // SAFETY: `alloc_layout` has non-zero size (`layout.total_size`).
            let ptr = unsafe { std::alloc::alloc(alloc_layout) };
            assert!(!ptr.is_null());
            // SAFETY: `ptr` was allocated with `alloc_layout` (`layout.total_size` bytes) and outlives `region`.
            let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
            // SAFETY: exclusive initialization of freshly allocated `region` matching `layout`.
            let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };
            for value in &values {
                ring.send(&[*value], 0).unwrap();
            }
            let mut out = [0u8; 8];
            let mut received = Vec::new();
            for _ in 0..values.len() {
                let len = ring.recv(&mut out).unwrap();
                prop_assert_eq!(len, 1);
                received.push(out[0]);
            }
            prop_assert_eq!(received, values);
            drop(ring);
            drop(region);
            // SAFETY: `ptr` was allocated with `alloc_layout` and all ring/region handles are dropped.
            unsafe {
                std::alloc::dealloc(ptr, alloc_layout);
            }
        }
    }

    #[test]
    fn slot_view_zero_copy() {
        let layout = RingLayout::new(256, 8, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                .unwrap();
        // SAFETY: `alloc_layout` has non-zero size (`layout.total_size`).
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        // SAFETY: `ptr` was allocated with `alloc_layout` (`layout.total_size` bytes) and outlives `region`.
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        // SAFETY: exclusive initialization of freshly allocated `region` matching `layout`.
        let ring =
            Arc::new(unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() });

        // Send a message
        let payload = b"hello zero-copy";
        ring.send(payload, 42).unwrap();

        // Receive as SlotView (zero-copy)
        let view = ring.try_recv_view().unwrap();
        assert_eq!(view.as_slice(), payload);
        assert_eq!(view.len(), payload.len());
        assert_eq!(view.flags(), 42);

        // While view is held, slot is borrowed
        assert!(matches!(
            ring.try_recv_view(),
            Err(ChannelError::SlotBorrowed)
        ));

        // Drop view to release slot
        drop(view);

        // Now ring is truly empty
        assert!(matches!(ring.try_recv_view(), Err(ChannelError::Empty)));

        drop(ring);
        drop(region);
        // SAFETY: `ptr` was allocated with `alloc_layout` and all ring/region handles are dropped.
        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
        }
    }

    #[test]
    fn spsc_copy_recv_while_slot_view_borrowed_returns_busy() {
        let layout = RingLayout::new(256, 8, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                .unwrap();
        // SAFETY: `alloc_layout` has non-zero size (`layout.total_size`).
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        // SAFETY: `ptr` was allocated with `alloc_layout` (`layout.total_size` bytes) and outlives `region`.
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        // SAFETY: exclusive initialization of freshly allocated `region` matching `layout`.
        let ring =
            Arc::new(unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() });

        ring.send(b"borrowed-slot", 7).unwrap();
        let view = ring.try_recv_view().unwrap();

        let mut buf = [0u8; 32];
        assert!(matches!(
            ring.try_recv(&mut buf),
            Err(ChannelError::SlotBorrowed)
        ));
        assert!(matches!(
            ring.try_recv_with_flags(&mut buf),
            Err(ChannelError::SlotBorrowed)
        ));
        assert!(ring.try_recv_batch(4).unwrap().is_empty());

        drop(view);
        assert!(matches!(ring.try_recv(&mut buf), Err(ChannelError::Empty)));

        drop(ring);
        drop(region);
        // SAFETY: `ptr` was allocated with `alloc_layout` and all ring/region handles are dropped.
        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
        }
    }

    #[test]
    fn slot_view_as_ref() {
        let layout = RingLayout::new(256, 8, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                .unwrap();
        // SAFETY: `alloc_layout` has non-zero size (`layout.total_size`).
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        // SAFETY: `ptr` was allocated with `alloc_layout` (`layout.total_size` bytes) and outlives `region`.
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        // SAFETY: exclusive initialization of freshly allocated `region` matching `layout`.
        let ring =
            Arc::new(unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() });

        ring.send(b"test", 0).unwrap();
        let view = ring.try_recv_view().unwrap();

        // Test AsRef<[u8]> implementation
        let slice: &[u8] = view.as_ref();
        assert_eq!(slice, b"test");

        drop(view);
        drop(ring);
        drop(region);
        // SAFETY: `ptr` was allocated with `alloc_layout` and all ring/region handles are dropped.
        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
        }
    }

    #[test]
    fn slot_view_explicit_release() {
        let layout = RingLayout::new(256, 8, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                .unwrap();
        // SAFETY: `alloc_layout` has non-zero size (`layout.total_size`).
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        // SAFETY: `ptr` was allocated with `alloc_layout` (`layout.total_size` bytes) and outlives `region`.
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        // SAFETY: exclusive initialization of freshly allocated `region` matching `layout`.
        let ring =
            Arc::new(unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() });

        ring.send(b"msg1", 0).unwrap();
        ring.send(b"msg2", 0).unwrap();

        let mut view1 = ring.try_recv_view().unwrap();
        assert_eq!(view1.as_slice(), b"msg1");

        // Explicitly release
        view1.release();

        // Now we can get the next message
        let view2 = ring.try_recv_view().unwrap();
        assert_eq!(view2.as_slice(), b"msg2");

        drop(view2);
        drop(view1);
        drop(ring);
        drop(region);
        // SAFETY: `ptr` was allocated with `alloc_layout` and all ring/region handles are dropped.
        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
        }
    }

    #[test]
    fn spsc_try_recv_batch_empty() {
        let layout = RingLayout::new(256, 8, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                .unwrap();
        // SAFETY: `alloc_layout` has non-zero size (`layout.total_size`).
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        // SAFETY: `ptr` was allocated with `alloc_layout` (`layout.total_size` bytes) and outlives `region`.
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        // SAFETY: exclusive initialization of freshly allocated `region` matching `layout`.
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };

        // Empty ring returns empty Vec
        let batch = ring.try_recv_batch(10).unwrap();
        assert!(batch.is_empty());

        drop(ring);
        drop(region);
        // SAFETY: `ptr` was allocated with `alloc_layout` and all ring/region handles are dropped.
        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
        }
    }

    #[test]
    fn spsc_try_recv_batch_partial() {
        let layout = RingLayout::new(256, 8, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                .unwrap();
        // SAFETY: `alloc_layout` has non-zero size (`layout.total_size`).
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        // SAFETY: `ptr` was allocated with `alloc_layout` (`layout.total_size` bytes) and outlives `region`.
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        // SAFETY: exclusive initialization of freshly allocated `region` matching `layout`.
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };

        // Send 3 messages
        ring.send(b"msg1", 1).unwrap();
        ring.send(b"msg2", 2).unwrap();
        ring.send(b"msg3", 3).unwrap();

        // Request max 2 - should get partial drain
        let batch = ring.try_recv_batch(2).unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].0, b"msg1");
        assert_eq!(batch[0].1, 1);
        assert_eq!(batch[1].0, b"msg2");
        assert_eq!(batch[1].1, 2);

        // Third message should still be available
        let batch = ring.try_recv_batch(10).unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].0, b"msg3");
        assert_eq!(batch[0].1, 3);

        drop(ring);
        drop(region);
        // SAFETY: `ptr` was allocated with `alloc_layout` and all ring/region handles are dropped.
        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
        }
    }

    #[test]
    fn spsc_try_recv_batch_full_drain() {
        let layout = RingLayout::new(256, 8, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                .unwrap();
        // SAFETY: `alloc_layout` has non-zero size (`layout.total_size`).
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        // SAFETY: `ptr` was allocated with `alloc_layout` (`layout.total_size` bytes) and outlives `region`.
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        // SAFETY: exclusive initialization of freshly allocated `region` matching `layout`.
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };

        // Send 5 messages
        for i in 0..5 {
            ring.send(&[i as u8], i as u32).unwrap();
        }

        // Request more than available - should drain all
        let batch = ring.try_recv_batch(100).unwrap();
        assert_eq!(batch.len(), 5);
        for (i, (payload, flags)) in batch.iter().enumerate() {
            assert_eq!(payload, &[i as u8]);
            assert_eq!(*flags, i as u32);
        }

        // Ring should now be empty
        assert!(ring.try_recv_batch(1).unwrap().is_empty());

        drop(ring);
        drop(region);
        // SAFETY: `ptr` was allocated with `alloc_layout` and all ring/region handles are dropped.
        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
        }
    }

    #[test]
    fn spsc_try_recv_batch_preserves_order() {
        let layout = RingLayout::new(256, 16, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                .unwrap();
        // SAFETY: `alloc_layout` has non-zero size (`layout.total_size`).
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        // SAFETY: `ptr` was allocated with `alloc_layout` (`layout.total_size` bytes) and outlives `region`.
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        // SAFETY: exclusive initialization of freshly allocated `region` matching `layout`.
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };

        // Send messages with increasing values
        for i in 0u8..10 {
            ring.send(&[i], 0).unwrap();
        }

        // Batch receive should preserve FIFO order
        let batch = ring.try_recv_batch(10).unwrap();
        assert_eq!(batch.len(), 10);
        for (i, (payload, _)) in batch.iter().enumerate() {
            assert_eq!(payload, &[i as u8], "Message {} out of order", i);
        }

        drop(ring);
        drop(region);
        // SAFETY: `ptr` was allocated with `alloc_layout` and all ring/region handles are dropped.
        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
        }
    }

    #[test]
    fn mpmc_try_recv_batch_empty() {
        let ring = MpmcRing::new(64, 8).unwrap();

        // Empty ring returns empty Vec
        let batch = ring.try_recv_batch(10);
        assert!(batch.is_empty());
    }

    #[test]
    fn mpmc_try_recv_batch_partial() {
        let ring = MpmcRing::new(64, 8).unwrap();

        // Send 3 messages
        ring.send(b"msg1", 1).unwrap();
        ring.send(b"msg2", 2).unwrap();
        ring.send(b"msg3", 3).unwrap();

        // Request max 2 - should get partial drain
        let batch = ring.try_recv_batch(2);
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].0, b"msg1");
        assert_eq!(batch[0].1, 1);
        assert_eq!(batch[1].0, b"msg2");
        assert_eq!(batch[1].1, 2);

        // Third message should still be available
        let batch = ring.try_recv_batch(10);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].0, b"msg3");
        assert_eq!(batch[0].1, 3);
    }

    #[test]
    fn mpmc_try_recv_batch_full_drain() {
        let ring = MpmcRing::new(64, 8).unwrap();

        // Send 5 messages
        for i in 0..5 {
            ring.send(&[i], i as u32).unwrap();
        }

        // Batch receive all messages
        let batch = ring.try_recv_batch(100);
        assert_eq!(batch.len(), 5);

        // Verify all messages received with correct flags
        for (i, (payload, flags)) in batch.iter().enumerate() {
            assert_eq!(payload, &[i as u8]);
            assert_eq!(*flags, i as u32);
        }

        // Ring should now be empty
        assert!(ring.try_recv_batch(1).is_empty());
    }

    #[test]
    fn spsc_read_slot_to_vec_and_try_recv_batch_reject_corrupted_slot_without_advancing_head() {
        let layout = RingLayout::new(256, 8, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                .unwrap();
        // SAFETY: `alloc_layout` has non-zero size (`layout.total_size`).
        let ptr = unsafe { std::alloc::alloc_zeroed(alloc_layout) };
        assert!(!ptr.is_null());
        // SAFETY: `ptr` is valid for `layout.total_size` bytes and outlives `region`.
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        // SAFETY: exclusive initialization of freshly allocated `region` matching `layout`.
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };

        ring.send(b"valid-0", 1).unwrap();
        ring.send(b"valid-1", 2).unwrap();
        ring.send(b"to-corrupt", 3).unwrap();

        let bad_len = ring.payload_capacity() as u32 + 64;
        // SAFETY: slot 2 lies inside the initialized ring region and is aligned to `SlotHeader`.
        unsafe {
            let slot2 = ring.slot_ptr(2).cast::<SlotHeader>();
            (*slot2).len = bad_len;
        }

        assert!(matches!(
            ring.read_slot_to_vec(2),
            Err(ChannelError::CorruptedSlot)
        ));
        assert!(matches!(
            ring.try_recv_batch(4),
            Err(ChannelError::CorruptedSlot)
        ));
        assert_eq!(ring.read_head().load(Ordering::Acquire), 2);
        assert!(matches!(
            ring.try_recv_batch(4),
            Err(ChannelError::CorruptedSlot)
        ));
        assert_eq!(ring.read_head().load(Ordering::Acquire), 2);

        drop(ring);
        drop(region);
        // SAFETY: `ptr` was allocated with `alloc_layout` and all ring/region handles are dropped.
        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
        }
    }

    #[test]
    fn validate_region_rejects_invalid_layout_short_and_misaligned_segments() {
        let layout = RingLayout::new(256, 8, ChannelMode::Spsc);
        let alloc_layout = std::alloc::Layout::from_size_align(
            layout.total_size + 64,
            crate::buffer::CACHELINE_BYTES,
        )
        .unwrap();
        // SAFETY: `alloc_layout` has non-zero size.
        let ptr = unsafe { std::alloc::alloc_zeroed(alloc_layout) };
        assert!(!ptr.is_null());
        // SAFETY: `ptr` is valid for `layout.total_size` bytes and outlives `valid_region`.
        let valid_region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });

        let mut zero_slots = layout;
        zero_slots.slots = 0;
        assert!(matches!(
            validate_region_for_layout(&valid_region, &zero_slots, 1),
            Err(ChannelError::InvalidLayout)
        ));

        let mut non_pow2 = layout;
        non_pow2.slots = 3;
        assert!(matches!(
            validate_region_for_layout(&valid_region, &non_pow2, 1),
            Err(ChannelError::InvalidLayout)
        ));

        let mut total_lt_offset = layout;
        total_lt_offset.total_size = layout.ring_offset() - 1;
        assert!(matches!(
            validate_region_for_layout(&valid_region, &total_lt_offset, 1),
            Err(ChannelError::InvalidLayout)
        ));

        let mut overflow_ring = layout;
        overflow_ring.ring_size = usize::MAX;
        assert!(matches!(
            validate_region_for_layout(&valid_region, &overflow_ring, 1),
            Err(ChannelError::CorruptedSegment(_))
        ));

        // SAFETY: `ptr` is valid for `layout.total_size - 8` bytes and outlives `short_region`.
        let short_region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size - 8) });
        // SAFETY: `SpscRing::attach` rejects `short_region` before reading the header.
        assert!(matches!(
            unsafe { SpscRing::attach(Arc::clone(&short_region), layout) },
            Err(ChannelError::CorruptedSegment(_))
        ));
        // SAFETY: `SpscRing::from_region` rejects `short_region` before reading the header.
        assert!(matches!(
            unsafe { SpscRing::from_region(Arc::clone(&short_region), layout) },
            Err(ChannelError::CorruptedSegment(_))
        ));

        // SAFETY: `ptr.add(1)` is within the `layout.total_size + 64` allocation.
        let misaligned_region =
            Arc::new(unsafe { ShmRegion::from_raw(ptr.add(1), layout.total_size) });
        // SAFETY: `SpscRing::attach` rejects `misaligned_region` before reading the header.
        assert!(matches!(
            unsafe { SpscRing::attach(Arc::clone(&misaligned_region), layout) },
            Err(ChannelError::CorruptedSegment(_))
        ));
        // SAFETY: `SpscRing::from_region` rejects `misaligned_region` before reading the header.
        assert!(matches!(
            unsafe { SpscRing::from_region(Arc::clone(&misaligned_region), layout) },
            Err(ChannelError::CorruptedSegment(_))
        ));

        let mpmc_layout = RingLayout::new(256, 8, ChannelMode::Mpmc);
        let mpmc_alloc = std::alloc::Layout::from_size_align(
            mpmc_layout.total_size + 64,
            crate::buffer::CACHELINE_BYTES,
        )
        .unwrap();
        // SAFETY: `mpmc_alloc` has non-zero size.
        let mpmc_ptr = unsafe { std::alloc::alloc_zeroed(mpmc_alloc) };
        assert!(!mpmc_ptr.is_null());
        // SAFETY: `mpmc_ptr` is valid for `mpmc_layout.total_size - 8` bytes.
        let mpmc_short =
            Arc::new(unsafe { ShmRegion::from_raw(mpmc_ptr, mpmc_layout.total_size - 8) });
        // SAFETY: `MpmcRing::attach` rejects `mpmc_short` before reading the header.
        assert!(matches!(
            unsafe { MpmcRing::attach(Arc::clone(&mpmc_short), mpmc_layout) },
            Err(ChannelError::CorruptedSegment(_))
        ));
        // SAFETY: `MpmcRing::from_region` rejects `mpmc_short` before reading the header.
        assert!(matches!(
            unsafe { MpmcRing::from_region(Arc::clone(&mpmc_short), mpmc_layout) },
            Err(ChannelError::CorruptedSegment(_))
        ));

        // SAFETY: `mpmc_ptr.add(1)` is within the `mpmc_layout.total_size + 64` allocation.
        let mpmc_misaligned =
            Arc::new(unsafe { ShmRegion::from_raw(mpmc_ptr.add(1), mpmc_layout.total_size) });
        // SAFETY: `MpmcRing::attach` rejects `mpmc_misaligned` before reading the header.
        assert!(matches!(
            unsafe { MpmcRing::attach(Arc::clone(&mpmc_misaligned), mpmc_layout) },
            Err(ChannelError::CorruptedSegment(_))
        ));
        // SAFETY: `MpmcRing::from_region` rejects `mpmc_misaligned` before reading the header.
        assert!(matches!(
            unsafe { MpmcRing::from_region(Arc::clone(&mpmc_misaligned), mpmc_layout) },
            Err(ChannelError::CorruptedSegment(_))
        ));

        let mut bad_seq_offset_layout = mpmc_layout;
        bad_seq_offset_layout.header_size += 1;
        bad_seq_offset_layout.total_size += 1;
        // SAFETY: `mpmc_ptr` is valid for `bad_seq_offset_layout.total_size` bytes inside `mpmc_alloc`.
        let mpmc_bad_seq_region =
            Arc::new(unsafe { ShmRegion::from_raw(mpmc_ptr, bad_seq_offset_layout.total_size) });
        assert!(matches!(
            validate_region_for_layout(
                &mpmc_bad_seq_region,
                &bad_seq_offset_layout,
                core::mem::align_of::<ShmHeader>()
            ),
            Err(ChannelError::CorruptedSegment(_))
        ));

        drop(mpmc_bad_seq_region);
        drop(mpmc_misaligned);
        drop(mpmc_short);
        drop(misaligned_region);
        drop(short_region);
        drop(valid_region);
        // SAFETY: `ptr` and `mpmc_ptr` were allocated with `alloc_layout` and `mpmc_alloc`.
        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
            std::alloc::dealloc(mpmc_ptr, mpmc_alloc);
        }
    }
}
