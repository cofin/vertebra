//! Lock-free ring buffer channel (SPSC implementation).

use core::ptr;
use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_utils::Backoff;

use crate::buffer::{
    ABI_VERSION, Buffer, BufferError, BufferPool, ChannelMode, MAGIC, RegionState, RingLayout,
    SLOT_ALIGN, ShmHeader, SlotHeader,
};
use crate::shm::{ShmError, ShmRegion};
use crate::sync::{self, SyncError};

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
    #[error("buffer metadata invalid")]
    InvalidBufferMeta,
    #[error("slot already borrowed")]
    SlotBorrowed,
    #[error("buffer error: {0}")]
    Buffer(#[from] BufferError),
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
    /// Not accessed directly after construction, but the Arc prevents deallocation.
    #[allow(dead_code)] // Intentional: Arc kept alive for memory lifetime, not accessed
    region: Arc<ShmRegion>,
    header: *mut ShmHeader,
    data: *mut u8,
    layout: RingLayout,
    mask: u64,
    /// Tracks if a SlotView is currently borrowing data (prevents concurrent borrows).
    slot_borrowed: std::sync::atomic::AtomicBool,
}

unsafe impl Send for SpscRing {}
unsafe impl Sync for SpscRing {}

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
        // SAFETY: caller guarantees exclusive initialization and valid mapping.
        let header = region.as_ptr().cast::<ShmHeader>();
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
        unsafe { Self::from_region(region, layout) }
    }

    /// Attach to an existing shared region and validate the header.
    ///
    /// # Safety
    /// Caller must ensure the region stays mapped for the lifetime of the ring.
    pub unsafe fn attach(region: Arc<ShmRegion>, layout: RingLayout) -> Result<Self, ChannelError> {
        // SAFETY: caller guarantees region is initialized and mapped.
        let header = region.as_ptr().cast::<ShmHeader>();
        let header_ref = unsafe { &*header };
        if header_ref.magic != MAGIC || header_ref.abi_version != ABI_VERSION {
            return Err(ChannelError::NotReady);
        }
        if header_ref.state.load(Ordering::Acquire) != RegionState::Ready as u32 {
            return Err(ChannelError::NotReady);
        }
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
        if !layout.is_power_of_two() {
            return Err(ChannelError::InvalidLayout);
        }
        // SAFETY: region is a valid shared memory mapping.
        let header = region.as_ptr().cast::<ShmHeader>();
        let data = unsafe { region.as_ptr().add(layout.header_size) };
        Ok(Self {
            region,
            header,
            data,
            layout,
            mask: (layout.slots as u64).wrapping_sub(1),
            slot_borrowed: std::sync::atomic::AtomicBool::new(false),
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
        // Use Relaxed for spin loop efficiency - try_recv uses Acquire
        let head = self.read_head().load(Ordering::Relaxed);
        let tail = self.write_head().load(Ordering::Relaxed);
        head != tail
    }

    /// Split the ring into sender/receiver handles.
    ///
    /// The caller must ensure only one sender and one receiver are used.
    pub fn split(self) -> (SpscSender, SpscReceiver) {
        let ring = Arc::new(self);
        (
            SpscSender {
                ring: Arc::clone(&ring),
            },
            SpscReceiver { ring },
        )
    }

    pub fn try_send(&self, payload: &[u8], flags: u32) -> Result<(), ChannelError> {
        if payload.len() > self.payload_capacity() {
            return Err(ChannelError::PayloadTooLarge {
                len: payload.len(),
                capacity: self.payload_capacity(),
            });
        }
        // PERF: Relaxed loads for availability check - just testing if full.
        // The Release store on write_head ensures slot data is visible to receivers.
        let head = self.read_head().load(Ordering::Relaxed);
        let tail = self.write_head().load(Ordering::Relaxed);
        let next = tail.wrapping_add(1) & self.mask;
        if next == head {
            return Err(ChannelError::Full);
        }
        unsafe {
            self.write_slot(tail, payload, flags);
        }
        // Release ensures all slot writes are visible before advancing tail
        self.write_head().store(next, Ordering::Release);
        self.wait_write_ptr().fetch_add(1, Ordering::Release);
        // PERF: Conditional wake - only syscall if receivers are actually waiting.
        // This eliminates ~8µs per send when receiver is actively polling.
        if self.recv_waiters().load(Ordering::Acquire) > 0 {
            sync::wake_one(self.wait_write_ptr())?;
        }
        Ok(())
    }

    pub fn send(&self, payload: &[u8], flags: u32) -> Result<(), ChannelError> {
        loop {
            match self.try_send(payload, flags) {
                Ok(()) => return Ok(()),
                Err(ChannelError::Full) => {
                    let wait_word = self.wait_read_ptr();
                    let expected = wait_word.load(Ordering::Acquire);

                    // CRITICAL: Increment waiter count BEFORE checking for space.
                    // Uses Release to ensure the increment is visible before we park.
                    self.send_waiters().fetch_add(1, Ordering::Release);

                    // CRITICAL: Double-check for space after incrementing counter.
                    let head = self.read_head().load(Ordering::Acquire);
                    let tail = self.write_head().load(Ordering::Acquire);
                    let next = tail.wrapping_add(1) & self.mask;
                    if next != head {
                        // Space freed! Decrement counter and retry.
                        self.send_waiters().fetch_sub(1, Ordering::Relaxed);
                        continue;
                    }

                    // Safe to park - receiver will see send_waiters > 0 and wake us.
                    let result = sync::wait_on_address(wait_word, expected);
                    self.send_waiters().fetch_sub(1, Ordering::Relaxed);
                    result?;
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

                    // Increment waiter count before checking for space
                    self.send_waiters().fetch_add(1, Ordering::Release);

                    // Double-check for space after incrementing counter
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

    pub fn try_recv(&self, out: &mut [u8]) -> Result<usize, ChannelError> {
        // PERF: Relaxed loads for availability check - just testing if empty.
        let head = self.read_head().load(Ordering::Relaxed);
        let tail = self.write_head().load(Ordering::Relaxed);
        if head == tail {
            return Err(ChannelError::Empty);
        }
        // CRITICAL: Acquire fence ensures we see all writes sender made to slot
        // before we read the payload. This synchronizes with sender's Release store.
        std::sync::atomic::fence(Ordering::Acquire);
        let len = unsafe { self.read_slot(head, out)? };
        let next = head.wrapping_add(1) & self.mask;
        self.read_head().store(next, Ordering::Release);
        self.wait_read_ptr().fetch_add(1, Ordering::Release);
        // PERF: Conditional wake - only syscall if senders are actually waiting.
        // This eliminates ~8µs per recv when sender is actively pushing.
        if self.send_waiters().load(Ordering::Acquire) > 0 {
            sync::wake_one(self.wait_read_ptr())?;
        }
        Ok(len)
    }

    pub fn try_recv_with_flags(&self, out: &mut [u8]) -> Result<(usize, u32), ChannelError> {
        // PERF: Relaxed loads for availability check
        let head = self.read_head().load(Ordering::Relaxed);
        let tail = self.write_head().load(Ordering::Relaxed);
        if head == tail {
            return Err(ChannelError::Empty);
        }
        // CRITICAL: Acquire fence before reading slot data
        std::sync::atomic::fence(Ordering::Acquire);
        let (len, flags) = unsafe { self.read_slot_with_flags(head, out)? };
        let next = head.wrapping_add(1) & self.mask;
        self.read_head().store(next, Ordering::Release);
        self.wait_read_ptr().fetch_add(1, Ordering::Release);
        // PERF: Conditional wake - only syscall if senders are actually waiting.
        if self.send_waiters().load(Ordering::Acquire) > 0 {
            sync::wake_one(self.wait_read_ptr())?;
        }
        Ok((len, flags))
    }

    pub fn recv(&self, out: &mut [u8]) -> Result<usize, ChannelError> {
        loop {
            match self.try_recv(out) {
                Ok(len) => return Ok(len),
                Err(ChannelError::Empty) => {
                    let wait_word = self.wait_write_ptr();
                    let expected = wait_word.load(Ordering::Acquire);

                    // CRITICAL: Increment waiter count BEFORE checking for data.
                    // This ensures sender sees us waiting before it checks the counter.
                    // Uses Release to ensure the increment is visible before we park.
                    self.recv_waiters().fetch_add(1, Ordering::Release);

                    // CRITICAL: Double-check for data after incrementing counter.
                    // If sender wrote data between try_recv and fetch_add, we'd miss it
                    // without this re-check (lost wakeup bug).
                    let head = self.read_head().load(Ordering::Acquire);
                    let tail = self.write_head().load(Ordering::Acquire);
                    if head != tail {
                        // Data arrived! Decrement counter and retry.
                        self.recv_waiters().fetch_sub(1, Ordering::Relaxed);
                        continue;
                    }

                    // Safe to park - sender will see recv_waiters > 0 and wake us.
                    let result = sync::wait_on_address(wait_word, expected);
                    self.recv_waiters().fetch_sub(1, Ordering::Relaxed);
                    result?;
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
                    let now = Instant::now();
                    if now >= deadline {
                        return Ok(None);
                    }
                    let remaining = deadline - now;
                    let wait_word = self.wait_write_ptr();
                    let expected = wait_word.load(Ordering::Acquire);

                    // Increment waiter count before checking for data
                    self.recv_waiters().fetch_add(1, Ordering::Release);

                    // Double-check for data after incrementing counter
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
        use std::time::Instant;

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
                    let now = Instant::now();
                    if now >= timeout_deadline {
                        return Ok(None);
                    }
                    let remaining = timeout_deadline - now;
                    let wait_word = self.wait_write_ptr();
                    let expected = wait_word.load(Ordering::Acquire);

                    // Increment waiter count before checking for data
                    self.recv_waiters().fetch_add(1, Ordering::Release);

                    // Double-check for data before blocking
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

    pub fn recv_with_flags(&self, out: &mut [u8]) -> Result<(usize, u32), ChannelError> {
        loop {
            match self.try_recv_with_flags(out) {
                Ok(result) => return Ok(result),
                Err(ChannelError::Empty) => {
                    let wait_word = self.wait_write_ptr();
                    let expected = wait_word.load(Ordering::Acquire);

                    // Increment waiter count before checking for data
                    self.recv_waiters().fetch_add(1, Ordering::Release);

                    // Double-check for data after incrementing counter
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

    /// Try to receive a message as a zero-copy view.
    ///
    /// Returns a `SlotView` that provides direct access to the ring buffer data.
    /// The slot is released when the view is dropped.
    ///
    /// # Note
    /// Only one SlotView can be active at a time per ring. The next recv will
    /// return `SlotBorrowed` until the current SlotView is released.
    pub fn try_recv_view(self: &Arc<Self>) -> Result<SlotView, ChannelError> {
        // Check if a slot is already borrowed
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
            // Release the borrow since there's no data
            self.slot_borrowed.store(false, Ordering::Release);
            return Err(ChannelError::Empty);
        }

        // Get slot data without advancing the cursor
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

    /// Receive a message as a zero-copy view, blocking until data is available.
    pub fn recv_view(self: &Arc<Self>) -> Result<SlotView, ChannelError> {
        loop {
            match self.try_recv_view() {
                Ok(view) => return Ok(view),
                Err(ChannelError::Empty) => {
                    let wait_word = self.wait_write_ptr();
                    let expected = wait_word.load(Ordering::Acquire);

                    // Increment waiter count before checking for data
                    self.recv_waiters().fetch_add(1, Ordering::Release);

                    // Double-check for data after incrementing counter
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
                    let now = Instant::now();
                    if now >= deadline {
                        return Ok(None);
                    }
                    let remaining = deadline - now;
                    let wait_word = self.wait_write_ptr();
                    let expected = wait_word.load(Ordering::Acquire);

                    // Increment waiter count before checking for data
                    self.recv_waiters().fetch_add(1, Ordering::Release);

                    // Double-check for data after incrementing counter
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
                    let now = Instant::now();
                    if now >= deadline {
                        return Ok(None);
                    }
                    let remaining = deadline - now;
                    let wait_word = self.wait_write_ptr();
                    let expected = wait_word.load(Ordering::Acquire);

                    // Increment waiter count before checking for data
                    self.recv_waiters().fetch_add(1, Ordering::Release);

                    // Double-check for data after incrementing counter
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
    pub fn try_recv_batch(&self, max: usize) -> Vec<(Vec<u8>, u32)> {
        let mut results = Vec::with_capacity(max.min(self.layout.slots));

        // PERF: Relaxed loads for availability check
        let mut head = self.read_head().load(Ordering::Relaxed);
        let tail = self.write_head().load(Ordering::Relaxed);

        if head == tail {
            return results; // Empty - fast return
        }

        // CRITICAL: Acquire fence before reading any slot data
        std::sync::atomic::fence(Ordering::Acquire);

        // Drain available messages up to max
        let mut count = 0;
        while head != tail && count < max {
            // Read slot data (safe: head < tail means slot is written)
            let (payload, flags) = unsafe { self.read_slot_to_vec(head) };
            results.push((payload, flags));
            head = head.wrapping_add(1) & self.mask;
            count += 1;
        }

        if count > 0 {
            // Single atomic store for all reads
            self.read_head().store(head, Ordering::Release);
            self.wait_read_ptr()
                .fetch_add(count as u32, Ordering::Release);

            // PERF: Single conditional wake for entire batch
            if self.send_waiters().load(Ordering::Acquire) > 0 {
                // Wake all waiting senders since we freed multiple slots
                let _ = sync::wake_all(self.wait_read_ptr());
            }
        }

        results
    }

    /// Helper to read a slot into a new Vec without requiring a pre-allocated buffer.
    ///
    /// # Safety
    /// Caller must ensure the slot at `index` contains valid data.
    unsafe fn read_slot_to_vec(&self, index: u64) -> (Vec<u8>, u32) {
        unsafe {
            let slot = self.slot_ptr(index);
            let header = &*slot.cast::<SlotHeader>();
            let len = header.len as usize;
            let flags = header.flags;
            let payload_ptr = slot.add(core::mem::size_of::<SlotHeader>());

            let mut payload = vec![0u8; len];
            ptr::copy_nonoverlapping(payload_ptr, payload.as_mut_ptr(), len);
            (payload, flags)
        }
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
        self.slot_borrowed.store(false, Ordering::Release);
        // Bump generation to invalidate any stale references
        let header = unsafe { &*self.header };
        header.generation.fetch_add(1, Ordering::Release);
    }

    fn read_head(&self) -> &core::sync::atomic::AtomicU64 {
        unsafe { &(*self.header).read_head.0 }
    }

    fn write_head(&self) -> &core::sync::atomic::AtomicU64 {
        unsafe { &(*self.header).write_head.0 }
    }

    fn wait_read_ptr(&self) -> &core::sync::atomic::AtomicU32 {
        unsafe { &(*self.header).wait_read.0 }
    }

    fn wait_write_ptr(&self) -> &core::sync::atomic::AtomicU32 {
        unsafe { &(*self.header).wait_write.0 }
    }

    /// Counter for threads blocked waiting to send (ring full).
    fn send_waiters(&self) -> &core::sync::atomic::AtomicU32 {
        unsafe { &(*self.header).send_waiters.0 }
    }

    /// Counter for threads blocked waiting to recv (ring empty).
    fn recv_waiters(&self) -> &core::sync::atomic::AtomicU32 {
        unsafe { &(*self.header).recv_waiters.0 }
    }

    unsafe fn slot_ptr(&self, index: u64) -> *mut u8 {
        let offset = (index as usize & self.mask as usize) * self.layout.slot_size;
        unsafe { self.data.add(offset) }
    }

    unsafe fn write_slot(&self, index: u64, payload: &[u8], flags: u32) {
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
        unsafe {
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
    }

    unsafe fn read_slot_with_flags(
        &self,
        index: u64,
        out: &mut [u8],
    ) -> Result<(usize, u32), ChannelError> {
        unsafe {
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

    /// Peek at a slot without copying or advancing the cursor.
    ///
    /// Returns (data_ptr, length, flags).
    ///
    /// # Safety
    /// Caller must ensure the index is valid (head <= index < tail).
    unsafe fn peek_slot(&self, index: u64) -> (*const u8, usize, u32) {
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

    /// Blocking send. Uses wait/wake to apply backpressure when full.
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

    pub fn recv_with_flags(&self, out: &mut [u8]) -> Result<(usize, u32), ChannelError> {
        self.ring.recv_with_flags(out)
    }

    pub fn try_recv_with_flags(&self, out: &mut [u8]) -> Result<(usize, u32), ChannelError> {
        self.ring.try_recv_with_flags(out)
    }

    pub fn recv_timeout(
        &self,
        out: &mut [u8],
        timeout: Duration,
    ) -> Result<Option<usize>, ChannelError> {
        self.ring.recv_timeout(out, timeout)
    }

    pub fn recv_timeout_with_flags(
        &self,
        out: &mut [u8],
        timeout: Duration,
    ) -> Result<Option<(usize, u32)>, ChannelError> {
        self.ring.recv_timeout_with_flags(out, timeout)
    }
}

#[derive(Debug)]
pub struct BufferMessage {
    pub buffer: Buffer,
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

#[derive(Debug)]
pub struct BufferSender {
    ring: Arc<SpscRing>,
    /// Keeps the buffer pool alive. The sender owns buffers but doesn't
    /// directly access the pool after sending (receiver reclaims them).
    #[allow(dead_code)] // Intentional: Arc kept for pool lifetime symmetry with receiver
    pool: Arc<BufferPool>,
}

#[derive(Debug)]
pub struct BufferReceiver {
    ring: Arc<SpscRing>,
    pool: Arc<BufferPool>,
}

impl BufferSender {
    pub fn send(&self, buffer: Buffer, len: usize, flags: u32) -> Result<(), ChannelError> {
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
        let mut buffer = Some(buffer);
        self.ring.send(&payload, 0)?;
        if let Some(buffer) = buffer.take() {
            let _ = buffer.into_raw_index();
        }
        Ok(())
    }

    pub fn try_send(&self, buffer: Buffer, len: usize, flags: u32) -> Result<(), ChannelError> {
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
        let mut buffer = Some(buffer);
        match self.ring.try_send(&payload, 0) {
            Ok(()) => {
                if let Some(buffer) = buffer.take() {
                    let _ = buffer.into_raw_index();
                }
                Ok(())
            }
            Err(err) => Err(err),
        }
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
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0)? };
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

#[derive(Debug)]
pub struct MpmcRing {
    /// Keeps the shared memory region alive for the lifetime of the ring.
    /// Not accessed directly after construction, but the Arc prevents deallocation.
    #[allow(dead_code)] // Intentional: Arc kept alive for memory lifetime, not accessed
    region: Arc<ShmRegion>,
    header: *mut ShmHeader,
    sequences: *mut AtomicU64,
    data: *mut u8,
    layout: RingLayout,
    mask: u64,
}

unsafe impl Send for MpmcRing {}
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

        // Initialize sequences
        unsafe {
            let sequences = region.as_ptr().add(layout.seq_offset()).cast::<AtomicU64>();
            for i in 0..layout.slots {
                (*sequences.add(i)).store(i as u64, Ordering::Relaxed);
            }
            Self::from_region(region, layout)
        }
    }

    /// Attach to an existing shared region.
    ///
    /// # Safety
    /// Caller must ensure the region stays mapped for the lifetime of the ring.
    pub unsafe fn attach(region: Arc<ShmRegion>, layout: RingLayout) -> Result<Self, ChannelError> {
        let header = region.as_ptr().cast::<ShmHeader>();
        let header_ref = unsafe { &*header };
        if header_ref.magic != MAGIC || header_ref.abi_version != ABI_VERSION {
            return Err(ChannelError::NotReady);
        }
        if header_ref.state.load(Ordering::Acquire) != RegionState::Ready as u32 {
            return Err(ChannelError::NotReady);
        }
        if layout.mode != ChannelMode::Mpmc {
            return Err(ChannelError::InvalidLayout);
        }
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
        if !layout.is_power_of_two() {
            return Err(ChannelError::InvalidLayout);
        }
        let header = region.as_ptr().cast::<ShmHeader>();
        let sequences = unsafe { region.as_ptr().add(layout.seq_offset()).cast::<AtomicU64>() };
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

    fn head(&self) -> &AtomicU64 {
        unsafe { &(*self.header).read_head.0 }
    }

    fn tail(&self) -> &AtomicU64 {
        unsafe { &(*self.header).write_head.0 }
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
                        unsafe {
                            let slot = self.slot_ptr(index);
                            let header = slot.cast::<SlotHeader>();
                            let payload_ptr = slot.add(core::mem::size_of::<SlotHeader>());
                            let payload_slice = core::slice::from_raw_parts_mut(
                                payload_ptr,
                                self.payload_capacity(),
                            );
                            let written = f(payload_slice);
                            (*header).len = written as u32;
                            (*header).flags = flags;
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
                        let len = unsafe { self.read_slot(index, out)? };
                        self.sequence(index)
                            .store(head + self.mask + 1, Ordering::Release);
                        return Ok(len);
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
                        let (len, flags) = unsafe { self.read_slot_with_flags(index, out)? };
                        self.sequence(index)
                            .store(head + self.mask + 1, Ordering::Release);
                        return Ok((len, flags));
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

    unsafe fn slot_ptr(&self, index: usize) -> *mut u8 {
        let offset = index * self.layout.slot_size;
        unsafe { self.data.add(offset) }
    }

    unsafe fn write_slot(&self, index: usize, payload: &[u8], flags: u32) {
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
        unsafe {
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
    }

    unsafe fn read_slot_with_flags(
        &self,
        index: usize,
        out: &mut [u8],
    ) -> Result<(usize, u32), ChannelError> {
        unsafe {
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
        // Reset sequences
        unsafe {
            for i in 0..self.layout.slots {
                (*self.sequences.add(i)).store(i as u64, Ordering::Release);
            }
            // Bump generation
            let header = &*self.header;
            header.generation.fetch_add(1, Ordering::Release);
        }
    }
}

fn align_up(value: usize, align: usize) -> usize {
    value.div_ceil(align) * align
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
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };
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
    fn spsc_timeouts() {
        let layout = RingLayout::new(256, 2, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                .unwrap();
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
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
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
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
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };
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
    fn spsc_recv_hybrid_immediate() {
        let layout = RingLayout::new(256, 8, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                .unwrap();
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
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

        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
        }
    }

    #[test]
    fn spsc_recv_hybrid_wakeup() {
        let layout = RingLayout::new(256, 8, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                .unwrap();
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        let ring =
            Arc::new(unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() });

        let ring_clone = Arc::clone(&ring);
        let handle = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_micros(50));
            ring_clone.send(b"delayed", 0).unwrap();
        });

        // recv_hybrid should spin briefly then block, then wake on send
        let mut out = [0u8; 32];
        let len = ring
            .recv_hybrid(&mut out, 5, std::time::Duration::from_millis(100))
            .unwrap()
            .expect("should wake up on send");
        assert_eq!(&out[..len], b"delayed");

        handle.join().unwrap();

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
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };

        // recv_hybrid should timeout with no data
        let mut out = [0u8; 32];
        let result = ring
            .recv_hybrid(&mut out, 5, std::time::Duration::from_millis(10))
            .unwrap();
        assert!(result.is_none());

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
            let ptr = unsafe { std::alloc::alloc(alloc_layout) };
            let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
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
            let ptr = unsafe { std::alloc::alloc(alloc_layout) };
            let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
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
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
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
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        let ring =
            Arc::new(unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() });

        ring.send(b"test", 0).unwrap();
        let view = ring.try_recv_view().unwrap();

        // Test AsRef<[u8]> implementation
        let slice: &[u8] = view.as_ref();
        assert_eq!(slice, b"test");

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
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
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
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };

        // Empty ring returns empty Vec
        let batch = ring.try_recv_batch(10);
        assert!(batch.is_empty());

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
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };

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
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };

        // Send 5 messages
        for i in 0..5 {
            ring.send(&[i as u8], i as u32).unwrap();
        }

        // Request more than available - should drain all
        let batch = ring.try_recv_batch(100);
        assert_eq!(batch.len(), 5);
        for (i, (payload, flags)) in batch.iter().enumerate() {
            assert_eq!(payload, &[i as u8]);
            assert_eq!(*flags, i as u32);
        }

        // Ring should now be empty
        assert!(ring.try_recv_batch(1).is_empty());

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
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };

        // Send messages with increasing values
        for i in 0u8..10 {
            ring.send(&[i], 0).unwrap();
        }

        // Batch receive should preserve FIFO order
        let batch = ring.try_recv_batch(10);
        assert_eq!(batch.len(), 10);
        for (i, (payload, _)) in batch.iter().enumerate() {
            assert_eq!(payload, &[i as u8], "Message {} out of order", i);
        }

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
}
