//! Lock-free ring buffer channels.
//!
//! Provides:
//! - SpscRing: Single-Producer Single-Consumer for response rings
//! - MpmcRing: Multi-Producer Multi-Consumer for request ring
//! - SlotView: Zero-copy access with Drop-based release
//! - Hybrid spin-wait for low-latency scenarios
//!
//! Ported from litecore::channel

use core::ptr;
use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_utils::Backoff;

use crate::buffer::{
    BufferError, BufferPool, ChannelMode, RegionState, RingLayout, ShmHeader, SlabLayout, SlotHeader,
    ABI_VERSION, MAGIC, SLOT_ALIGN,
};
use crate::shm::{ShmError, ShmRegion};
use crate::sync::{self, SyncError};

/// Channel operation errors.
#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    /// Ring layout is invalid (non-power-of-two slots, etc.).
    #[error("ring layout is invalid")]
    InvalidLayout,
    /// Region is not in ready state.
    #[error("region is not ready")]
    NotReady,
    /// Channel is full (non-blocking send).
    #[error("channel is full")]
    Full,
    /// Channel is empty (non-blocking recv).
    #[error("channel is empty")]
    Empty,
    /// Payload exceeds slot capacity.
    #[error("payload too large: {len} > {capacity}")]
    PayloadTooLarge { len: usize, capacity: usize },
    /// Output buffer too small for message.
    #[error("buffer too small: {len} > {capacity}")]
    BufferTooSmall { len: usize, capacity: usize },
    /// Buffer metadata is invalid.
    #[error("buffer metadata invalid")]
    InvalidBufferMeta,
    /// Slot is already borrowed by a SlotView.
    #[error("slot already borrowed")]
    SlotBorrowed,
    /// Buffer pool error.
    #[error("buffer error: {0}")]
    Buffer(#[from] BufferError),
    /// Shared memory error.
    #[error("shared memory error: {0}")]
    Shm(#[from] ShmError),
    /// Synchronization error.
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

// SAFETY: SlotView holds an Arc to the ring, ensuring the underlying memory is valid.
// The data pointer points into the ring's shared memory which is thread-safe.
unsafe impl Send for SlotView {}
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
        self.ring.wait_read_ptr().fetch_add(1, Ordering::Release);
        let _ = crate::sync::wake_one(self.ring.wait_read_ptr());

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
    #[allow(dead_code)]
    region: Arc<ShmRegion>,
    header: *mut ShmHeader,
    data: *mut u8,
    layout: RingLayout,
    mask: u64,
    /// Index of the ring in ShmHeader (0..2).
    ring_index: usize,
    /// Tracks if a SlotView is currently borrowing data.
    slot_borrowed: std::sync::atomic::AtomicBool,
}

unsafe impl Send for SpscRing {}
unsafe impl Sync for SpscRing {}

/// Ring buffer statistics.
#[derive(Debug, Copy, Clone)]
pub struct RingStats {
    pub slot_size: usize,
    pub slots: usize,
    pub read_head: u64,
    pub write_head: u64,
}

impl SpscRing {
    /// Initialize a shared region in-place and return a ring handle.
    ///
    /// # Safety
    /// Caller must ensure exclusive initialization and valid mapping.
    pub unsafe fn initialize(
        region: Arc<ShmRegion>,
        layout: RingLayout,
        creator_pid: u32,
        creator_epoch: u64,
        ring_index: usize,
    ) -> Result<Self, ChannelError> {
        if !layout.is_power_of_two() || layout.slot_size < core::mem::size_of::<SlotHeader>() {
            return Err(ChannelError::InvalidLayout);
        }
        if region.len() < layout.total_size {
            return Err(ChannelError::InvalidLayout);
        }
        if ring_index >= 3 {
            return Err(ChannelError::InvalidLayout);
        }

        let header = region.as_ptr().cast::<ShmHeader>();
        let header_ref = &mut *header;
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
        
        // Initialize specific ring
        header_ref.rings[ring_index].read_head.0.store(0, Ordering::Relaxed);
        header_ref.rings[ring_index].write_head.0.store(0, Ordering::Relaxed);
        header_ref.rings[ring_index].wait_read.0.store(0, Ordering::Relaxed);
        header_ref.rings[ring_index].wait_write.0.store(0, Ordering::Relaxed);
        header_ref.rings[ring_index].send_waiters.0.store(0, Ordering::Relaxed);
        header_ref.rings[ring_index].recv_waiters.0.store(0, Ordering::Relaxed);

        Self::from_region(region, layout, ring_index)
    }

    /// Attach to an existing shared region and validate the header.
    ///
    /// # Safety
    /// Caller must ensure the region stays mapped for the lifetime of the ring.
    pub unsafe fn attach(
        region: Arc<ShmRegion>,
        layout: RingLayout,
        ring_index: usize,
    ) -> Result<Self, ChannelError> {
        let header = region.as_ptr().cast::<ShmHeader>();
        let header_ref = &*header;
        if header_ref.magic != MAGIC || header_ref.abi_version != ABI_VERSION {
            return Err(ChannelError::NotReady);
        }
        if header_ref.state.load(Ordering::Acquire) != RegionState::Ready as u32 {
            return Err(ChannelError::NotReady);
        }
        Self::from_region(region, layout, ring_index)
    }

    /// Create a ring without initializing the header.
    ///
    /// # Safety
    /// Caller must ensure the region is already initialized correctly.
    pub unsafe fn from_region(
        region: Arc<ShmRegion>,
        layout: RingLayout,
        ring_index: usize,
    ) -> Result<Self, ChannelError> {
        if !layout.is_power_of_two() {
            return Err(ChannelError::InvalidLayout);
        }
        if ring_index >= 3 {
            return Err(ChannelError::InvalidLayout);
        }
        let header = region.as_ptr().cast::<ShmHeader>();
        let data = region.as_ptr().add(layout.ring_offset());
        Ok(Self {
            region,
            header,
            data,
            layout,
            mask: (layout.slots as u64).wrapping_sub(1),
            ring_index,
            slot_borrowed: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Get the maximum payload size for a slot.
    pub fn payload_capacity(&self) -> usize {
        self.layout.slot_size - core::mem::size_of::<SlotHeader>()
    }

    /// Get ring statistics.
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
    /// Fast check using Relaxed ordering - suitable for spin loops.
    #[inline]
    pub fn has_data(&self) -> bool {
        let head = self.read_head().load(Ordering::Relaxed);
        let tail = self.write_head().load(Ordering::Relaxed);
        head != tail
    }

    /// Split the ring into sender/receiver handles.
    pub fn split(self) -> (SpscSender, SpscReceiver) {
        let ring = Arc::new(self);
        (
            SpscSender {
                ring: Arc::clone(&ring),
            },
            SpscReceiver { ring },
        )
    }

    /// Try to send a message without blocking.
    pub fn try_send(&self, payload: &[u8], flags: u32) -> Result<(), ChannelError> {
        if payload.len() > self.payload_capacity() {
            return Err(ChannelError::PayloadTooLarge {
                len: payload.len(),
                capacity: self.payload_capacity(),
            });
        }

        let head = self.read_head().load(Ordering::Relaxed);
        let tail = self.write_head().load(Ordering::Relaxed);
        let next = tail.wrapping_add(1) & self.mask;
        if next == head {
            return Err(ChannelError::Full);
        }

        unsafe {
            self.write_slot(tail, payload, flags);
        }
        self.write_head().store(next, Ordering::Release);
        self.wait_write_ptr().fetch_add(1, Ordering::Release);

        // Conditional wake - only syscall if receivers are waiting
        if self.recv_waiters().load(Ordering::Acquire) > 0 {
            sync::wake_one(self.wait_write_ptr())?;
        }
        Ok(())
    }

    /// Send a message, blocking until space is available.
    pub fn send(&self, payload: &[u8], flags: u32) -> Result<(), ChannelError> {
        loop {
            match self.try_send(payload, flags) {
                Ok(()) => return Ok(()),
                Err(ChannelError::Full) => {
                    let wait_word = self.wait_read_ptr();
                    let expected = wait_word.load(Ordering::Acquire);

                    self.send_waiters().fetch_add(1, Ordering::Release);

                    // Double-check for space after incrementing counter
                    let head = self.read_head().load(Ordering::Acquire);
                    let tail = self.write_head().load(Ordering::Acquire);
                    let next = tail.wrapping_add(1) & self.mask;
                    if next != head {
                        self.send_waiters().fetch_sub(1, Ordering::Relaxed);
                        continue;
                    }

                    let result = sync::wait_on_address(wait_word, expected);
                    self.send_waiters().fetch_sub(1, Ordering::Relaxed);
                    result?;
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Send with timeout.
    pub fn send_timeout(
        &self,
        payload: &[u8],
        flags: u32,
        timeout: Duration,
    ) -> Result<bool, ChannelError> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.try_send(payload, flags) {
                Ok(()) => return Ok(true),
                Err(ChannelError::Full) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Ok(false);
                    }
                    let remaining = deadline - now;
                    let wait_word = self.wait_read_ptr();
                    let expected = wait_word.load(Ordering::Acquire);

                    self.send_waiters().fetch_add(1, Ordering::Release);

                    let head = self.read_head().load(Ordering::Acquire);
                    let tail = self.write_head().load(Ordering::Acquire);
                    let next = tail.wrapping_add(1) & self.mask;
                    if next != head {
                        self.send_waiters().fetch_sub(1, Ordering::Relaxed);
                        continue;
                    }

                    let timed_out = !sync::wait_on_address_timeout(wait_word, expected, remaining)?;
                    self.send_waiters().fetch_sub(1, Ordering::Relaxed);
                    if timed_out {
                        return Ok(false);
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Try to receive a message without blocking.
    pub fn try_recv(&self, out: &mut [u8]) -> Result<usize, ChannelError> {
        let head = self.read_head().load(Ordering::Relaxed);
        let tail = self.write_head().load(Ordering::Relaxed);
        if head == tail {
            return Err(ChannelError::Empty);
        }

        std::sync::atomic::fence(Ordering::Acquire);
        let len = unsafe { self.read_slot(head, out)? };
        let next = head.wrapping_add(1) & self.mask;
        self.read_head().store(next, Ordering::Release);
        self.wait_read_ptr().fetch_add(1, Ordering::Release);

        if self.send_waiters().load(Ordering::Acquire) > 0 {
            sync::wake_one(self.wait_read_ptr())?;
        }
        Ok(len)
    }

    /// Try to receive with flags.
    pub fn try_recv_with_flags(&self, out: &mut [u8]) -> Result<(usize, u32), ChannelError> {
        let head = self.read_head().load(Ordering::Relaxed);
        let tail = self.write_head().load(Ordering::Relaxed);
        if head == tail {
            return Err(ChannelError::Empty);
        }

        std::sync::atomic::fence(Ordering::Acquire);
        let (len, flags) = unsafe { self.read_slot_with_flags(head, out)? };
        let next = head.wrapping_add(1) & self.mask;
        self.read_head().store(next, Ordering::Release);
        self.wait_read_ptr().fetch_add(1, Ordering::Release);

        if self.send_waiters().load(Ordering::Acquire) > 0 {
            sync::wake_one(self.wait_read_ptr())?;
        }
        Ok((len, flags))
    }

    /// Receive a message, blocking until data is available.
    pub fn recv(&self, out: &mut [u8]) -> Result<usize, ChannelError> {
        loop {
            match self.try_recv(out) {
                Ok(len) => return Ok(len),
                Err(ChannelError::Empty) => {
                    let wait_word = self.wait_write_ptr();
                    let expected = wait_word.load(Ordering::Acquire);

                    self.recv_waiters().fetch_add(1, Ordering::Release);

                    let head = self.read_head().load(Ordering::Acquire);
                    let tail = self.write_head().load(Ordering::Acquire);
                    if head != tail {
                        self.recv_waiters().fetch_sub(1, Ordering::Relaxed);
                        continue;
                    }

                    let result = sync::wait_on_address(wait_word, expected);
                    self.recv_waiters().fetch_sub(1, Ordering::Relaxed);
                    result?;
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Receive with timeout.
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
                    let now = Instant::now();
                    if now >= deadline {
                        return Ok(None);
                    }
                    let remaining = deadline - now;
                    let wait_word = self.wait_write_ptr();
                    let expected = wait_word.load(Ordering::Acquire);

                    self.recv_waiters().fetch_add(1, Ordering::Release);

                    let head = self.read_head().load(Ordering::Acquire);
                    let tail = self.write_head().load(Ordering::Acquire);
                    if head != tail {
                        self.recv_waiters().fetch_sub(1, Ordering::Relaxed);
                        continue;
                    }

                    let timed_out = !sync::wait_on_address_timeout(wait_word, expected, remaining)?;
                    self.recv_waiters().fetch_sub(1, Ordering::Relaxed);
                    if timed_out {
                        return Ok(None);
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Receive with hybrid spin-wait for low-latency scenarios.
    ///
    /// Spins for `spin_us` microseconds before falling back to futex-based blocking.
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

        // Spin phase
        while Instant::now() < spin_deadline {
            if self.has_data() {
                match self.try_recv(out) {
                    Ok(len) => return Ok(Some(len)),
                    Err(ChannelError::Empty) => {}
                    Err(e) => return Err(e),
                }
            }
            std::hint::spin_loop();
        }

        // Park phase
        loop {
            match self.try_recv(out) {
                Ok(len) => return Ok(Some(len)),
                Err(ChannelError::Empty) => {
                    let now = Instant::now();
                    if now >= timeout_deadline {
                        return Ok(None);
                    }
                    let remaining = timeout_deadline - now;
                    let wait_word = self.wait_write_ptr();
                    let expected = wait_word.load(Ordering::Acquire);

                    self.recv_waiters().fetch_add(1, Ordering::Release);

                    if self.has_data() {
                        self.recv_waiters().fetch_sub(1, Ordering::Relaxed);
                        continue;
                    }

                    let timed_out = !sync::wait_on_address_timeout(wait_word, expected, remaining)?;
                    self.recv_waiters().fetch_sub(1, Ordering::Relaxed);
                    if timed_out {
                        return Ok(None);
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Try to receive a message as a zero-copy view.
    pub fn try_recv_view(self: &Arc<Self>) -> Result<SlotView, ChannelError> {
        if self
            .slot_borrowed
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Err(ChannelError::SlotBorrowed);
        }

        let head = self.read_head().load(Ordering::Acquire);
        let tail = self.write_head().load(Ordering::Acquire);
        if head == tail {
            self.slot_borrowed.store(false, Ordering::Release);
            return Err(ChannelError::Empty);
        }

        let (data_ptr, length, flags) = unsafe { self.peek_slot(head) };

        Ok(SlotView {
            ring: Arc::clone(self),
            slot_index: head,
            data_ptr,
            length,
            flags,
            released: false,
        })
    }

    /// Drain up to `max` messages without blocking.
    pub fn try_recv_batch(&self, max: usize) -> Vec<(Vec<u8>, u32)> {
        let mut results = Vec::with_capacity(max.min(self.layout.slots));

        let mut head = self.read_head().load(Ordering::Relaxed);
        let tail = self.write_head().load(Ordering::Relaxed);

        if head == tail {
            return results;
        }

        std::sync::atomic::fence(Ordering::Acquire);

        let mut count = 0;
        while head != tail && count < max {
            let (payload, flags) = unsafe { self.read_slot_to_vec(head) };
            results.push((payload, flags));
            head = head.wrapping_add(1) & self.mask;
            count += 1;
        }

        if count > 0 {
            self.read_head().store(head, Ordering::Release);
            self.wait_read_ptr()
                .fetch_add(count as u32, Ordering::Release);

            if self.send_waiters().load(Ordering::Acquire) > 0 {
                let _ = sync::wake_all(self.wait_read_ptr());
            }
        }

        results
    }

    fn read_head(&self) -> &core::sync::atomic::AtomicU64 {
        unsafe { &(*self.header).rings[self.ring_index].read_head.0 }
    }

    fn write_head(&self) -> &core::sync::atomic::AtomicU64 {
        unsafe { &(*self.header).rings[self.ring_index].write_head.0 }
    }

    fn wait_read_ptr(&self) -> &core::sync::atomic::AtomicU32 {
        unsafe { &(*self.header).rings[self.ring_index].wait_read.0 }
    }

    fn wait_write_ptr(&self) -> &core::sync::atomic::AtomicU32 {
        unsafe { &(*self.header).rings[self.ring_index].wait_write.0 }
    }

    fn send_waiters(&self) -> &core::sync::atomic::AtomicU32 {
        unsafe { &(*self.header).rings[self.ring_index].send_waiters.0 }
    }

    fn recv_waiters(&self) -> &core::sync::atomic::AtomicU32 {
        unsafe { &(*self.header).rings[self.ring_index].recv_waiters.0 }
    }

    unsafe fn slot_ptr(&self, index: u64) -> *mut u8 {
        let offset = (index as usize & self.mask as usize) * self.layout.slot_size;
        self.data.add(offset)
    }

    unsafe fn write_slot(&self, index: u64, payload: &[u8], flags: u32) {
        let slot = self.slot_ptr(index);
        let header = slot.cast::<SlotHeader>();
        (*header).len = payload.len() as u32;
        (*header).flags = flags;
        let payload_ptr = slot.add(core::mem::size_of::<SlotHeader>());
        ptr::copy_nonoverlapping(payload.as_ptr(), payload_ptr, payload.len());
    }

    unsafe fn read_slot(&self, index: u64, out: &mut [u8]) -> Result<usize, ChannelError> {
        let slot = self.slot_ptr(index);
        let header = &*slot.cast::<SlotHeader>();
        let len = header.len as usize;
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

    unsafe fn read_slot_with_flags(
        &self,
        index: u64,
        out: &mut [u8],
    ) -> Result<(usize, u32), ChannelError> {
        let slot = self.slot_ptr(index);
        let header = &*slot.cast::<SlotHeader>();
        let len = header.len as usize;
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

    unsafe fn read_slot_to_vec(&self, index: u64) -> (Vec<u8>, u32) {
        let slot = self.slot_ptr(index);
        let header = &*slot.cast::<SlotHeader>();
        let len = header.len as usize;
        let flags = header.flags;
        let payload_ptr = slot.add(core::mem::size_of::<SlotHeader>());

        let mut payload = vec![0u8; len];
        ptr::copy_nonoverlapping(payload_ptr, payload.as_mut_ptr(), len);
        (payload, flags)
    }

    unsafe fn peek_slot(&self, index: u64) -> (*const u8, usize, u32) {
        let slot = self.slot_ptr(index);
        let header = &*slot.cast::<SlotHeader>();
        let len = header.len as usize;
        let flags = header.flags;
        let payload_ptr = slot.add(core::mem::size_of::<SlotHeader>());
        (payload_ptr, len, flags)
    }
}

/// SPSC sender handle.
#[derive(Debug)]
pub struct SpscSender {
    ring: Arc<SpscRing>,
}

impl SpscSender {
    pub fn payload_capacity(&self) -> usize {
        self.ring.payload_capacity()
    }

    pub fn stats(&self) -> RingStats {
        self.ring.stats()
    }

    pub fn send(&self, payload: &[u8], flags: u32) -> Result<(), ChannelError> {
        self.ring.send(payload, flags)
    }

    pub fn try_send(&self, payload: &[u8], flags: u32) -> Result<(), ChannelError> {
        self.ring.try_send(payload, flags)
    }

    pub fn send_timeout(
        &self,
        payload: &[u8],
        flags: u32,
        timeout: Duration,
    ) -> Result<bool, ChannelError> {
        self.ring.send_timeout(payload, flags, timeout)
    }
}

/// SPSC receiver handle.
#[derive(Debug)]
pub struct SpscReceiver {
    ring: Arc<SpscRing>,
}

impl SpscReceiver {
    pub fn recv(&self, out: &mut [u8]) -> Result<usize, ChannelError> {
        self.ring.recv(out)
    }

    pub fn try_recv(&self, out: &mut [u8]) -> Result<usize, ChannelError> {
        self.ring.try_recv(out)
    }

    pub fn stats(&self) -> RingStats {
        self.ring.stats()
    }

    pub fn recv_timeout(
        &self,
        out: &mut [u8],
        timeout: Duration,
    ) -> Result<Option<usize>, ChannelError> {
        self.ring.recv_timeout(out, timeout)
    }
}

/// MPMC ring channel backed by a shared memory region.
#[derive(Debug)]
pub struct MpmcRing {
    #[allow(dead_code)]
    region: Arc<ShmRegion>,
    header: *mut ShmHeader,
    sequences: *mut AtomicU64,
    data: *mut u8,
    layout: RingLayout,
    mask: u64,
    ring_index: usize,
}

unsafe impl Send for MpmcRing {}
unsafe impl Sync for MpmcRing {}

impl MpmcRing {
    /// Initialize a shared region in-place and return a ring handle.
    ///
    /// # Safety
    /// Caller must ensure exclusive initialization and valid mapping.
    pub unsafe fn initialize(
        region: Arc<ShmRegion>,
        layout: RingLayout,
        creator_pid: u32,
        creator_epoch: u64,
        ring_index: usize,
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
        if ring_index >= 3 {
            return Err(ChannelError::InvalidLayout);
        }

        let header = region.as_ptr().cast::<ShmHeader>();
        let header_ref = &mut *header;
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
        
        // Initialize specific ring
        header_ref.rings[ring_index].read_head.0.store(0, Ordering::Relaxed);
        header_ref.rings[ring_index].write_head.0.store(0, Ordering::Relaxed);
        header_ref.rings[ring_index].wait_read.0.store(0, Ordering::Relaxed);
        header_ref.rings[ring_index].wait_write.0.store(0, Ordering::Relaxed);
        header_ref.rings[ring_index].send_waiters.0.store(0, Ordering::Relaxed);
        header_ref.rings[ring_index].recv_waiters.0.store(0, Ordering::Relaxed);

        // Initialize sequences
        let sequences = region.as_ptr().add(layout.seq_offset()).cast::<AtomicU64>();
        for i in 0..layout.slots {
            (*sequences.add(i)).store(i as u64, Ordering::Relaxed);
        }

        Self::from_region(region, layout, ring_index)
    }

    /// Attach to an existing shared region.
    ///
    /// # Safety
    /// Caller must ensure the region stays mapped for the lifetime of the ring.
    pub unsafe fn attach(
        region: Arc<ShmRegion>,
        layout: RingLayout,
        ring_index: usize,
    ) -> Result<Self, ChannelError> {
        let header = region.as_ptr().cast::<ShmHeader>();
        let header_ref = &*header;
        if header_ref.magic != MAGIC || header_ref.abi_version != ABI_VERSION {
            return Err(ChannelError::NotReady);
        }
        if header_ref.state.load(Ordering::Acquire) != RegionState::Ready as u32 {
            return Err(ChannelError::NotReady);
        }
        if layout.mode != ChannelMode::Mpmc {
            return Err(ChannelError::InvalidLayout);
        }
        Self::from_region(region, layout, ring_index)
    }

    pub unsafe fn initialize_sequences(
        region: &ShmRegion,
        layout: &RingLayout,
    ) -> Result<(), ChannelError> {
        if layout.mode != ChannelMode::Mpmc {
            return Ok(());
        }
        let sequences = region.as_ptr().add(layout.seq_offset()).cast::<AtomicU64>();
        for i in 0..layout.slots {
            (*sequences.add(i)).store(i as u64, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Create a ring without initializing the header.
    ///
    /// # Safety
    /// Caller must ensure the region is already initialized correctly.
    pub unsafe fn from_region(
        region: Arc<ShmRegion>,
        layout: RingLayout,
        ring_index: usize,
    ) -> Result<Self, ChannelError> {
        if !layout.is_power_of_two() {
            return Err(ChannelError::InvalidLayout);
        }
        if ring_index >= 3 {
            return Err(ChannelError::InvalidLayout);
        }
        let header = region.as_ptr().cast::<ShmHeader>();
        let sequences = region.as_ptr().add(layout.seq_offset()).cast::<AtomicU64>();
        let data = region.as_ptr().add(layout.ring_offset());
        Ok(Self {
            region,
            header,
            sequences,
            data,
            layout,
            mask: (layout.slots as u64).wrapping_sub(1),
            ring_index,
        })
    }

    /// Create a new anonymous MPMC ring.
    pub fn new(slot_size: usize, slots: usize) -> Result<Self, ChannelError> {
        let layout = RingLayout::new(slot_size, slots, ChannelMode::Mpmc);
        let region = Arc::new(ShmRegion::create(layout.total_size)?);
        unsafe { Self::initialize(region, layout, 0, 0, 0) }
    }

    pub fn payload_capacity(&self) -> usize {
        self.layout.slot_size - core::mem::size_of::<SlotHeader>()
    }

    pub fn stats(&self) -> RingStats {
        RingStats {
            slot_size: self.layout.slot_size,
            slots: self.layout.slots,
            read_head: self.head().load(Ordering::Acquire),
            write_head: self.tail().load(Ordering::Acquire),
        }
    }

    #[inline]
    pub fn has_data(&self) -> bool {
        let head = self.head().load(Ordering::Relaxed);
        let tail = self.tail().load(Ordering::Relaxed);
        head != tail
    }

    fn head(&self) -> &AtomicU64 {
        unsafe { &(*self.header).rings[self.ring_index].read_head.0 }
    }

    fn tail(&self) -> &AtomicU64 {
        unsafe { &(*self.header).rings[self.ring_index].write_head.0 }
    }

    fn sequence(&self, index: usize) -> &AtomicU64 {
        unsafe { &*self.sequences.add(index) }
    }

    pub fn try_send(&self, payload: &[u8], flags: u32) -> Result<(), ChannelError> {
        if payload.len() > self.payload_capacity() {
            return Err(ChannelError::PayloadTooLarge {
                len: payload.len(),
                capacity: self.payload_capacity(),
            });
        }

        let tail = self.tail().load(Ordering::Relaxed);
        let index = (tail & self.mask) as usize;
        let seq = self.sequence(index).load(Ordering::Acquire);
        let diff = seq as i64 - tail as i64;

        if diff == 0 {
            if self
                .tail()
                .compare_exchange(tail, tail + 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                unsafe {
                    self.write_slot(index, payload, flags);
                }
                self.sequence(index).store(tail + 1, Ordering::Release);
                return Ok(());
            }
        } else if diff < 0 {
            return Err(ChannelError::Full);
        }

        Err(ChannelError::Full)
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
        let head = self.head().load(Ordering::Relaxed);
        let index = (head & self.mask) as usize;
        let seq = self.sequence(index).load(Ordering::Acquire);
        let diff = seq as i64 - (head + 1) as i64;

        if diff == 0
            && self
                .head()
                .compare_exchange(head, head + 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            let len = unsafe { self.read_slot(index, out)? };
            self.sequence(index)
                .store(head + self.mask + 1, Ordering::Release);
            return Ok(len);
        }

        Err(ChannelError::Empty)
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
                Err(_) => break,
            }
        }

        results
    }

    pub fn try_recv_with_flags(&self, out: &mut [u8]) -> Result<(usize, u32), ChannelError> {
        let head = self.head().load(Ordering::Relaxed);
        let index = (head & self.mask) as usize;
        let seq = self.sequence(index).load(Ordering::Acquire);
        let diff = seq as i64 - (head + 1) as i64;

        if diff == 0
            && self
                .head()
                .compare_exchange(head, head + 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            let (len, flags) = unsafe { self.read_slot_with_flags(index, out)? };
            self.sequence(index)
                .store(head + self.mask + 1, Ordering::Release);
            return Ok((len, flags));
        }

        Err(ChannelError::Empty)
    }

    unsafe fn slot_ptr(&self, index: usize) -> *mut u8 {
        let offset = index * self.layout.slot_size;
        self.data.add(offset)
    }

    unsafe fn write_slot(&self, index: usize, payload: &[u8], flags: u32) {
        let slot = self.slot_ptr(index);
        let header = slot.cast::<SlotHeader>();
        (*header).len = payload.len() as u32;
        (*header).flags = flags;
        let payload_ptr = slot.add(core::mem::size_of::<SlotHeader>());
        ptr::copy_nonoverlapping(payload.as_ptr(), payload_ptr, payload.len());
    }

    unsafe fn read_slot(&self, index: usize, out: &mut [u8]) -> Result<usize, ChannelError> {
        let slot = self.slot_ptr(index);
        let header = &*slot.cast::<SlotHeader>();
        let len = header.len as usize;
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

    unsafe fn read_slot_with_flags(
        &self,
        index: usize,
        out: &mut [u8],
    ) -> Result<(usize, u32), ChannelError> {
        let slot = self.slot_ptr(index);
        let header = &*slot.cast::<SlotHeader>();
        let len = header.len as usize;
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

/// Slab allocator managing 3 MPMC rings.
#[derive(Debug)]
pub struct SlabAllocator {
    pub region: Arc<ShmRegion>,
    pub small: Arc<MpmcRing>,
    pub medium: Arc<MpmcRing>,
    pub large: Arc<MpmcRing>,
}

impl SlabAllocator {
    pub fn new(
        small_slots: usize,
        medium_slots: usize,
        large_slots: usize,
        small_size: usize,
        medium_size: usize,
        large_size: usize,
        creator_pid: u32,
        creator_epoch: u64,
    ) -> Result<Self, ChannelError> {
        let layout = SlabLayout::new(
            small_slots,
            medium_slots,
            large_slots,
            small_size,
            medium_size,
            large_size,
        );
        let region = Arc::new(ShmRegion::create(layout.total_size)?);

        unsafe {
            Self::initialize_header(&region, &layout, creator_pid, creator_epoch)?;

            MpmcRing::initialize_sequences(&region, &layout.small)?;
            MpmcRing::initialize_sequences(&region, &layout.medium)?;
            MpmcRing::initialize_sequences(&region, &layout.large)?;

            let small = Arc::new(MpmcRing::attach(Arc::clone(&region), layout.small, 0)?);
            let medium = Arc::new(MpmcRing::attach(Arc::clone(&region), layout.medium, 1)?);
            let large = Arc::new(MpmcRing::attach(Arc::clone(&region), layout.large, 2)?);

            Ok(Self {
                region,
                small,
                medium,
                large,
            })
        }
    }

    unsafe fn initialize_header(
        region: &ShmRegion,
        layout: &SlabLayout,
        creator_pid: u32,
        creator_epoch: u64,
    ) -> Result<(), ChannelError> {
        if region.len() < layout.total_size {
            return Err(ChannelError::InvalidLayout);
        }

        let header = region.as_ptr().cast::<ShmHeader>();
        let header_ref = &mut *header;
        header_ref.magic = MAGIC;
        header_ref.abi_version = ABI_VERSION;
        header_ref.flags = 0;
        header_ref.total_size = layout.total_size as u64;
        // Ring offset in header is legacy/informational, maybe point to Medium or first ring?
        // Let's point to Medium (index 1) as "default" ring for tools unaware of slabs?
        // Or just use 0 (Small).
        header_ref.ring_offset = layout.small.ring_offset() as u64;
        header_ref.ring_size = layout.small.ring_size as u64;
        header_ref.generation.store(1, Ordering::Relaxed);
        header_ref
            .state
            .store(RegionState::Ready as u32, Ordering::Release);
        header_ref.creator_pid = creator_pid;
        header_ref.creator_epoch = creator_epoch;

        // Initialize all 3 rings metadata
        for i in 0..3 {
            header_ref.rings[i].read_head.0.store(0, Ordering::Relaxed);
            header_ref.rings[i].write_head.0.store(0, Ordering::Relaxed);
            header_ref.rings[i].wait_read.0.store(0, Ordering::Relaxed);
            header_ref.rings[i].wait_write.0.store(0, Ordering::Relaxed);
            header_ref.rings[i].send_waiters.0.store(0, Ordering::Relaxed);
            header_ref.rings[i].recv_waiters.0.store(0, Ordering::Relaxed);
        }

        Ok(())
    }
}

/// Buffer message with pooled buffer ownership.
#[derive(Debug)]
pub struct BufferMessage {
    pub buffer: crate::buffer::Buffer,
    pub len: usize,
    pub flags: u32,
}

#[repr(C)]
#[derive(Copy, Clone)]
struct BufferMeta {
    index: u32,
    len: u32,
    flags: u32,
    _reserved: u32,
}

const BUFFER_META_BYTES: usize = core::mem::size_of::<BufferMeta>();

fn encode_buffer_meta(meta: BufferMeta) -> [u8; BUFFER_META_BYTES] {
    let mut out = [0u8; BUFFER_META_BYTES];
    out[0..4].copy_from_slice(&meta.index.to_le_bytes());
    out[4..8].copy_from_slice(&meta.len.to_le_bytes());
    out[8..12].copy_from_slice(&meta.flags.to_le_bytes());
    out[12..16].copy_from_slice(&meta._reserved.to_le_bytes());
    out
}

fn decode_buffer_meta(bytes: &[u8]) -> Result<BufferMeta, ChannelError> {
    if bytes.len() < BUFFER_META_BYTES {
        return Err(ChannelError::InvalidBufferMeta);
    }
    let index = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    let len = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    let flags = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let reserved = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
    Ok(BufferMeta {
        index,
        len,
        flags,
        _reserved: reserved,
    })
}

/// Buffer sender for zero-copy buffer passing.
#[derive(Debug)]
pub struct BufferSender {
    ring: Arc<SpscRing>,
    #[allow(dead_code)]
    pool: Arc<BufferPool>,
}

/// Buffer receiver for zero-copy buffer passing.
#[derive(Debug)]
pub struct BufferReceiver {
    ring: Arc<SpscRing>,
    pool: Arc<BufferPool>,
}

impl BufferSender {
    pub fn send(
        &self,
        buffer: crate::buffer::Buffer,
        len: usize,
        flags: u32,
    ) -> Result<(), ChannelError> {
        let payload_capacity = buffer.len();
        if len > payload_capacity {
            return Err(ChannelError::PayloadTooLarge {
                len,
                capacity: payload_capacity,
            });
        }
        let index = buffer.index();
        let index_u32 = u32::try_from(index).map_err(|_| ChannelError::InvalidBufferMeta)?;
        let len_u32 = u32::try_from(len).map_err(|_| ChannelError::InvalidBufferMeta)?;
        let meta = BufferMeta {
            index: index_u32,
            len: len_u32,
            flags,
            _reserved: 0,
        };
        let payload = encode_buffer_meta(meta);
        self.ring.send(&payload, 0)?;
        let _ = buffer.into_raw_index();
        Ok(())
    }

    pub fn try_send(
        &self,
        buffer: crate::buffer::Buffer,
        len: usize,
        flags: u32,
    ) -> Result<(), ChannelError> {
        let payload_capacity = buffer.len();
        if len > payload_capacity {
            return Err(ChannelError::PayloadTooLarge {
                len,
                capacity: payload_capacity,
            });
        }
        let index = buffer.index();
        let index_u32 = u32::try_from(index).map_err(|_| ChannelError::InvalidBufferMeta)?;
        let len_u32 = u32::try_from(len).map_err(|_| ChannelError::InvalidBufferMeta)?;
        let meta = BufferMeta {
            index: index_u32,
            len: len_u32,
            flags,
            _reserved: 0,
        };
        let payload = encode_buffer_meta(meta);
        self.ring.try_send(&payload, 0)?;
        let _ = buffer.into_raw_index();
        Ok(())
    }
}

impl BufferReceiver {
    pub fn recv(&self) -> Result<BufferMessage, ChannelError> {
        let mut payload = [0u8; BUFFER_META_BYTES];
        let len = self.ring.recv(&mut payload)?;
        if len != BUFFER_META_BYTES {
            return Err(ChannelError::InvalidBufferMeta);
        }
        let meta = decode_buffer_meta(&payload)?;
        let buffer = unsafe { self.pool.from_raw(meta.index as usize) }?;
        Ok(BufferMessage {
            buffer,
            len: meta.len as usize,
            flags: meta.flags,
        })
    }

    pub fn try_recv(&self) -> Result<Option<BufferMessage>, ChannelError> {
        let mut payload = [0u8; BUFFER_META_BYTES];
        match self.ring.try_recv(&mut payload) {
            Ok(len) => {
                if len != BUFFER_META_BYTES {
                    return Err(ChannelError::InvalidBufferMeta);
                }
                let meta = decode_buffer_meta(&payload)?;
                let buffer = unsafe { self.pool.from_raw(meta.index as usize) }?;
                Ok(Some(BufferMessage {
                    buffer,
                    len: meta.len as usize,
                    flags: meta.flags,
                }))
            }
            Err(ChannelError::Empty) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

/// Factory for creating buffer channels.
#[derive(Debug)]
pub struct BufferChannel;

impl BufferChannel {
    pub fn split(
        pool: Arc<BufferPool>,
        slots: usize,
    ) -> Result<(BufferSender, BufferReceiver), ChannelError> {
        let slot_payload = BUFFER_META_BYTES;
        let slot_size = align_up(
            core::mem::size_of::<SlotHeader>() + slot_payload,
            SLOT_ALIGN,
        );
        let layout = RingLayout::new(slot_size, slots, ChannelMode::Spsc);
        let region = Arc::new(ShmRegion::create(layout.total_size)?);
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0, 0)? };
        let ring = Arc::new(ring);
        Ok((
            BufferSender {
                ring: Arc::clone(&ring),
                pool: Arc::clone(&pool),
            },
            BufferReceiver { ring, pool },
        ))
    }
}

fn align_up(value: usize, align: usize) -> usize {
    value.div_ceil(align) * align
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{BufferPool, CACHELINE_BYTES};

    #[test]
    fn spsc_round_trip() {
        let layout = RingLayout::new(256, 8, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, CACHELINE_BYTES).unwrap();
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0, 0).unwrap() };
        let payload = b"hello";
        ring.send(payload, 0).unwrap();
        let mut out = [0u8; 32];
        let len = ring.recv(&mut out).unwrap();
        assert_eq!(&out[..len], payload);
        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
        }
    }

    #[test]
    fn spsc_full_empty_edges() {
        let layout = RingLayout::new(128, 2, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, CACHELINE_BYTES).unwrap();
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0, 0).unwrap() };
        let payload = [1u8; 8];
        ring.try_send(&payload, 0).unwrap();
        assert!(matches!(
            ring.try_send(&payload, 0),
            Err(ChannelError::Full)
        ));
        let mut out = [0u8; 8];
        ring.try_recv(&mut out).unwrap();
        assert!(matches!(ring.try_recv(&mut out), Err(ChannelError::Empty)));
        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
        }
    }

    #[test]
    fn spsc_multi_threaded() {
        let layout = RingLayout::new(128, 8, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, CACHELINE_BYTES).unwrap();
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0, 0).unwrap() };
        let (tx, rx) = ring.split();
        std::thread::scope(|s| {
            s.spawn(|| {
                for i in 0u8..20 {
                    tx.send(&[i], 0).unwrap();
                }
            });
            s.spawn(|| {
                let mut out = [0u8; 8];
                for i in 0u8..20 {
                    let len = rx.recv(&mut out).unwrap();
                    assert_eq!(len, 1);
                    assert_eq!(out[0], i);
                }
            });
        });
        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
        }
    }

    #[test]
    fn buffer_channel_round_trip() {
        let pool = BufferPool::new(64, 2);
        let (tx, rx) = BufferChannel::split(Arc::clone(&pool), 8).unwrap();
        let mut buffer = pool.acquire().unwrap();
        buffer.as_mut_slice()[0..4].copy_from_slice(b"ping");
        tx.send(buffer, 4, 7).unwrap();
        let msg = rx.recv().unwrap();
        assert_eq!(msg.len, 4);
        assert_eq!(msg.flags, 7);
        assert_eq!(&msg.buffer.as_slice()[..4], b"ping");
    }

    #[test]
    fn slot_view_zero_copy() {
        let layout = RingLayout::new(256, 8, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, CACHELINE_BYTES).unwrap();
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        let ring =
            Arc::new(unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0, 0).unwrap() });

        let payload = b"hello zero-copy";
        ring.send(payload, 42).unwrap();

        let view = ring.try_recv_view().unwrap();
        assert_eq!(view.as_slice(), payload);
        assert_eq!(view.len(), payload.len());
        assert_eq!(view.flags(), 42);

        assert!(matches!(
            ring.try_recv_view(),
            Err(ChannelError::SlotBorrowed)
        ));

        drop(view);
        assert!(matches!(ring.try_recv_view(), Err(ChannelError::Empty)));

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

    #[test]
    fn spsc_try_recv_batch() {
        let layout = RingLayout::new(256, 8, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, CACHELINE_BYTES).unwrap();
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0, 0).unwrap() };

        for i in 0..5 {
            ring.send(&[i as u8], i as u32).unwrap();
        }

        let batch = ring.try_recv_batch(100);
        assert_eq!(batch.len(), 5);
        for (i, (payload, flags)) in batch.iter().enumerate() {
            assert_eq!(payload, &[i as u8]);
            assert_eq!(*flags, i as u32);
        }

        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
        }
    }

    #[test]
    fn mpmc_try_recv_batch() {
        let ring = MpmcRing::new(64, 8).unwrap();

        ring.send(b"msg1", 1).unwrap();
        ring.send(b"msg2", 2).unwrap();
        ring.send(b"msg3", 3).unwrap();

        let batch = ring.try_recv_batch(2);
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].0, b"msg1");
        assert_eq!(batch[0].1, 1);

        let batch = ring.try_recv_batch(10);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].0, b"msg3");
    }
}
