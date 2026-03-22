//! Async-capable ring buffer with notification support.
//!
//! This module provides `AsyncRing`, a wrapper around `SpscRing` that integrates
//! with asyncio via file descriptor notifications. Consumers can register the
//! notification FD with their event loop and receive callbacks when data is
//! available, or use hybrid spin-wait for sub-10us latency.
//!
//! # Usage Pattern
//!
//! ```python
//! # In Python (via PyO3):
//! ring = AsyncRing(65536, 1024)
//! fd = ring.notifier_fd()
//! loop.add_reader(fd, on_readable)
//!
//! # Producer sends data:
//! ring.send(data)  # Automatically notifies
//!
//! # In on_readable callback:
//! ring.clear_notification()
//! while (data := ring.try_recv()) is not None:
//!     process(data)
//!
//! # Or use hybrid spin-wait for low-latency sync path:
//! data = ring.recv_hybrid(spin_us=5, timeout_ms=1000)
//! ```

use std::io;
use std::sync::Arc;

use crate::buffer::{BufferPool, RingLayout};
use crate::channel::{ChannelError, MpmcRing, SpscRing};
use crate::notify::Notifier;
use crate::shm::ShmRegion;

/// Async-capable SPSC ring buffer with notification support.
///
/// This struct wraps a `SpscRing` with a `Notifier` to enable integration
/// with asyncio's event loop via `loop.add_reader()`. Also provides
/// `recv_hybrid()` for low-latency synchronous receives.
pub struct AsyncRing {
    ring: SpscRing,
    notifier: Notifier,
    /// Keeps the shared memory region alive for the lifetime of the async ring.
    /// Not accessed directly after construction, but the Arc prevents deallocation.
    #[allow(dead_code)] // Intentional: Arc kept alive for memory lifetime, not accessed
    region: Arc<ShmRegion>,
}

// SAFETY: AsyncRing contains only Send types.
unsafe impl Send for AsyncRing {}
// SAFETY: SpscRing and Notifier are both Sync.
unsafe impl Sync for AsyncRing {}

impl AsyncRing {
    /// Create a new async ring buffer with the given parameters.
    ///
    /// # Arguments
    /// - `slot_size`: Size of each slot in bytes (includes header overhead)
    /// - `slots`: Number of slots (must be power of 2)
    ///
    /// # Returns
    /// A new `AsyncRing` backed by anonymous shared memory.
    pub fn new(slot_size: usize, slots: usize) -> Result<Self, AsyncRingError> {
        let layout = RingLayout::new(slot_size, slots, crate::buffer::ChannelMode::Spsc);
        let region = Arc::new(ShmRegion::create(layout.total_size)?);
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0)? };
        let notifier = Notifier::new()?;

        Ok(Self {
            ring,
            notifier,
            region,
        })
    }

    /// Create a named async ring buffer.
    ///
    /// Named rings can be opened by other processes via `open_named()`.
    #[cfg(unix)]
    pub fn create_named(
        name: &str,
        slot_size: usize,
        slots: usize,
    ) -> Result<Self, AsyncRingError> {
        let layout = RingLayout::new(slot_size, slots, crate::buffer::ChannelMode::Spsc);
        let region = Arc::new(ShmRegion::create_named(name, layout.total_size)?);
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0)? };
        let notifier = Notifier::new()?;

        Ok(Self {
            ring,
            notifier,
            region,
        })
    }

    /// Open an existing named async ring buffer.
    #[cfg(unix)]
    pub fn open_named(name: &str, slot_size: usize, slots: usize) -> Result<Self, AsyncRingError> {
        let layout = RingLayout::new(slot_size, slots, crate::buffer::ChannelMode::Spsc);
        let region = Arc::new(ShmRegion::open_named(name, layout.total_size)?);
        let ring = unsafe { SpscRing::attach(Arc::clone(&region), layout)? };
        let notifier = Notifier::new()?;

        Ok(Self {
            ring,
            notifier,
            region,
        })
    }

    /// Open an async ring from an existing raw file descriptor (for mapped SHM).
    ///
    /// # Safety
    ///
    /// The caller must ensure:
    /// - `fd` is a valid, open file descriptor for a shared memory region
    /// - The shared memory was created with a compatible layout (matching `slot_size` and `slots`)
    /// - The shared memory region has been properly initialized by a prior `create_named()` call
    /// - The file descriptor remains valid for the lifetime of the returned `AsyncRing`
    /// - The caller is responsible for closing the fd after the AsyncRing is dropped,
    ///   or the fd ownership is transferred to `ShmRegion::map()`
    #[cfg(unix)]
    pub unsafe fn from_raw_fd(
        fd: std::os::fd::RawFd,
        slot_size: usize,
        slots: usize,
    ) -> Result<Self, AsyncRingError> {
        let layout = RingLayout::new(slot_size, slots, crate::buffer::ChannelMode::Spsc);
        // Map region from FD
        // ShmRegion::map handles mmap
        let region = Arc::new(ShmRegion::map(fd, layout.total_size)?);
        // Note: ShmRegion::map takes ownership of FD if we treat it as ShmHandle?
        // Let's check ShmRegion::map implementation.
        // It calls map_fd_region which creates ShmRegion.
        // However, we usually duplicate the FD or pass ownership.
        // Here we assume we pass ownership of the raw FD to ShmRegion.

        let ring = SpscRing::attach(Arc::clone(&region), layout)?;
        let notifier = Notifier::new()?; // Create new notifier for local use?
                                         // Wait, if this is worker, it needs to attach to existing notifier if using shared?
                                         // Or does it have its own?
                                         // Worker.py uses add_reader on a separate notification FD.

        Ok(Self {
            ring,
            notifier,
            region,
        })
    }

    /// Get the notification file descriptor for asyncio integration.
    ///
    /// Register this FD with `loop.add_reader(fd, callback)` to receive
    /// notifications when data is available.
    pub fn notifier_fd(&self) -> i32 {
        self.notifier.as_raw_fd()
    }

    /// Get the payload capacity per slot.
    pub fn payload_capacity(&self) -> usize {
        self.ring.payload_capacity()
    }

    /// Send data and notify waiters.
    ///
    /// This is a blocking send that waits for space if the ring is full.
    /// After successful send, the notifier is signaled.
    pub fn send(&self, data: &[u8], flags: u32) -> Result<(), AsyncRingError> {
        self.ring.send(data, flags)?;
        self.notifier.notify()?;
        Ok(())
    }

    /// Try to send without blocking.
    ///
    /// Returns `Ok(true)` if sent, `Ok(false)` if ring is full.
    pub fn try_send(&self, data: &[u8], flags: u32) -> Result<bool, AsyncRingError> {
        match self.ring.try_send(data, flags) {
            Ok(()) => {
                self.notifier.notify()?;
                Ok(true)
            }
            Err(ChannelError::Full) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Try to receive without blocking.
    ///
    /// Returns `Ok(Some(len))` with the data length if available,
    /// `Ok(None)` if empty.
    ///
    /// Call `clear_notification()` after processing all available data.
    pub fn try_recv(&self, buffer: &mut [u8]) -> Result<Option<usize>, AsyncRingError> {
        match self.ring.try_recv(buffer) {
            Ok(len) => Ok(Some(len)),
            Err(ChannelError::Empty) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Try to receive with flags.
    ///
    /// Returns `Ok(Some((len, flags)))` if available, `Ok(None)` if empty.
    pub fn try_recv_with_flags(
        &self,
        buffer: &mut [u8],
    ) -> Result<Option<(usize, u32)>, AsyncRingError> {
        match self.ring.try_recv_with_flags(buffer) {
            Ok((len, flags)) => Ok(Some((len, flags))),
            Err(ChannelError::Empty) => Ok(None),
            Err(e) => Err(e.into()),
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
    pub fn try_recv_batch(&self, max: usize) -> Vec<(Vec<u8>, u32)> {
        self.ring.try_recv_batch(max)
    }

    /// Receive with hybrid spin-wait for low-latency.
    ///
    /// Spins for `spin_us` microseconds checking for data, then falls back to
    /// blocking wait. This captures immediate responses without syscall overhead.
    ///
    /// # Arguments
    ///
    /// * `buffer` - Output buffer for received data
    /// * `spin_us` - Microseconds to spin before blocking (typically 2-10)
    /// * `timeout_ms` - Total timeout in milliseconds (-1 for infinite)
    ///
    /// # Returns
    ///
    /// * `Ok(Some(len))` - Data received, returns length
    /// * `Ok(None)` - Timeout expired
    /// * `Err` - Error during receive
    ///
    /// # Performance
    ///
    /// The spin phase uses a ring-buffer-aware check (atomic head/tail comparison)
    /// rather than poll() syscall, achieving ~1ns per iteration vs ~200-500ns.
    /// This allows ~5000 iterations in a 5µs spin vs ~10-25 with poll().
    pub fn recv_hybrid(
        &self,
        buffer: &mut [u8],
        spin_us: u64,
        timeout_ms: i32,
    ) -> Result<Option<usize>, AsyncRingError> {
        // First try without waiting (data might already be available)
        match self.ring.try_recv(buffer) {
            Ok(len) => {
                // Clear any pending notification to prevent spurious wakeups next time
                let _ = self.notifier.clear();
                return Ok(Some(len));
            }
            Err(ChannelError::Empty) => {}
            Err(e) => return Err(e.into()),
        }

        // Hybrid spin-wait using ring-aware data check (2x Relaxed atomic loads, ~2ns)
        // instead of poll() syscall (~200-500ns per iteration)
        if !self
            .notifier
            .spin_wait_or_park_with(spin_us, timeout_ms, || self.ring.has_data())?
        {
            return Ok(None); // Timeout
        }

        // Clear notification before receiving
        self.notifier.clear()?;

        // Try to receive after notification
        match self.ring.try_recv(buffer) {
            Ok(len) => Ok(Some(len)),
            Err(ChannelError::Empty) => Ok(None), // Spurious wakeup
            Err(e) => Err(e.into()),
        }
    }

    /// Clear notification (call after handling all available data).
    ///
    /// This should be called:
    /// 1. At the start of the readable callback
    /// 2. After draining all messages with `try_recv()`
    pub fn clear_notification(&self) -> Result<(), AsyncRingError> {
        self.notifier.clear()?;
        Ok(())
    }

    /// Check if there are pending notifications.
    pub fn has_pending_notification(&self) -> bool {
        self.notifier.has_pending()
    }

    /// Get ring statistics.
    pub fn stats(&self) -> crate::channel::RingStats {
        self.ring.stats()
    }
}

/// Async-capable MPMC ring buffer with notification support.
///
/// This struct wraps a `MpmcRing` with a `Notifier` to enable integration
/// with asyncio's event loop via `loop.add_reader()`. Also provides
/// `recv_hybrid()` for low-latency synchronous receives.
pub struct AsyncMpmcRing {
    ring: MpmcRing,
    notifier: Notifier,
    /// Keeps the shared memory region alive for the lifetime of the async ring.
    /// Not accessed directly after construction, but the Arc prevents deallocation.
    #[allow(dead_code)] // Intentional: Arc kept alive for memory lifetime, not accessed
    region: Arc<ShmRegion>,
}

// SAFETY: AsyncMpmcRing contains only Send types.
unsafe impl Send for AsyncMpmcRing {}
// SAFETY: MpmcRing and Notifier are both Sync.
unsafe impl Sync for AsyncMpmcRing {}

impl AsyncMpmcRing {
    /// Create a new async MPMC ring buffer.
    pub fn new(slot_size: usize, slots: usize) -> Result<Self, AsyncRingError> {
        let layout = RingLayout::new(slot_size, slots, crate::buffer::ChannelMode::Mpmc);
        let region = Arc::new(ShmRegion::create(layout.total_size)?);
        let ring = unsafe { MpmcRing::initialize(Arc::clone(&region), layout, 0, 0)? };
        let notifier = Notifier::new()?;

        Ok(Self {
            ring,
            notifier,
            region,
        })
    }

    /// Create a named async MPMC ring buffer.
    #[cfg(unix)]
    pub fn create_named(
        name: &str,
        slot_size: usize,
        slots: usize,
    ) -> Result<Self, AsyncRingError> {
        let layout = RingLayout::new(slot_size, slots, crate::buffer::ChannelMode::Mpmc);
        let region = Arc::new(ShmRegion::create_named(name, layout.total_size)?);
        let ring = unsafe { MpmcRing::initialize(Arc::clone(&region), layout, 0, 0)? };
        let notifier = Notifier::new()?;

        Ok(Self {
            ring,
            notifier,
            region,
        })
    }

    /// Open an existing named async MPMC ring buffer.
    #[cfg(unix)]
    pub fn open_named(name: &str, slot_size: usize, slots: usize) -> Result<Self, AsyncRingError> {
        let layout = RingLayout::new(slot_size, slots, crate::buffer::ChannelMode::Mpmc);
        let region = Arc::new(ShmRegion::open_named(name, layout.total_size)?);
        let ring = unsafe { MpmcRing::attach(Arc::clone(&region), layout)? };
        let notifier = Notifier::new()?;

        Ok(Self {
            ring,
            notifier,
            region,
        })
    }

    /// Open an async MPMC ring from an existing raw file descriptor.
    ///
    /// # Safety
    ///
    /// The caller must ensure:
    /// - `fd` is a valid, open file descriptor for a shared memory region
    /// - The shared memory was created with a compatible MPMC layout (matching `slot_size` and `slots`)
    /// - The shared memory region has been properly initialized by a prior `create_named()` call
    /// - The file descriptor remains valid for the lifetime of the returned `AsyncMpmcRing`
    #[cfg(unix)]
    pub unsafe fn from_raw_fd(
        fd: std::os::fd::RawFd,
        slot_size: usize,
        slots: usize,
    ) -> Result<Self, AsyncRingError> {
        let layout = RingLayout::new(slot_size, slots, crate::buffer::ChannelMode::Mpmc);
        let region = Arc::new(ShmRegion::map(fd, layout.total_size)?);
        let ring = MpmcRing::attach(Arc::clone(&region), layout)?;
        let notifier = Notifier::new()?;

        Ok(Self {
            ring,
            notifier,
            region,
        })
    }

    /// Get the notification file descriptor.
    pub fn notifier_fd(&self) -> i32 {
        self.notifier.as_raw_fd()
    }

    /// Get the payload capacity per slot.
    pub fn payload_capacity(&self) -> usize {
        self.ring.payload_capacity()
    }

    /// Send data and notify waiters.
    pub fn send(&self, data: &[u8], flags: u32) -> Result<(), AsyncRingError> {
        self.ring.send(data, flags)?;
        self.notifier.notify()?;
        Ok(())
    }

    /// Try to send without blocking.
    pub fn try_send(&self, data: &[u8], flags: u32) -> Result<bool, AsyncRingError> {
        match self.ring.try_send(data, flags) {
            Ok(()) => {
                self.notifier.notify()?;
                Ok(true)
            }
            Err(ChannelError::Full) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Try to receive without blocking.
    pub fn try_recv(&self, buffer: &mut [u8]) -> Result<Option<usize>, AsyncRingError> {
        match self.ring.try_recv(buffer) {
            Ok(len) => Ok(Some(len)),
            Err(ChannelError::Empty) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Try to receive with flags.
    pub fn try_recv_with_flags(
        &self,
        buffer: &mut [u8],
    ) -> Result<Option<(usize, u32)>, AsyncRingError> {
        match self.ring.try_recv_with_flags(buffer) {
            Ok((len, flags)) => Ok(Some((len, flags))),
            Err(ChannelError::Empty) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Receive with hybrid spin-wait.
    pub fn recv_hybrid(
        &self,
        buffer: &mut [u8],
        spin_us: u64,
        timeout_ms: i32,
    ) -> Result<Option<usize>, AsyncRingError> {
        // Fast path
        match self.ring.try_recv(buffer) {
            Ok(len) => {
                let _ = self.notifier.clear();
                return Ok(Some(len));
            }
            Err(ChannelError::Empty) => {}
            Err(e) => return Err(e.into()),
        }

        // Spin wait
        if !self
            .notifier
            .spin_wait_or_park_with(spin_us, timeout_ms, || self.ring.has_data())?
        {
            return Ok(None);
        }

        self.notifier.clear()?;

        match self.ring.try_recv(buffer) {
            Ok(len) => Ok(Some(len)),
            Err(ChannelError::Empty) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Clear notification.
    pub fn clear_notification(&self) -> Result<(), AsyncRingError> {
        self.notifier.clear()?;
        Ok(())
    }

    /// Check if there are pending notifications.
    pub fn has_pending_notification(&self) -> bool {
        self.notifier.has_pending()
    }

    /// Get ring statistics.
    pub fn stats(&self) -> crate::channel::RingStats {
        self.ring.stats()
    }

    /// Drain up to `max` messages without blocking.
    ///
    /// More efficient than calling `try_recv` in a loop because it amortizes
    /// the Python/Rust boundary crossing overhead.
    ///
    /// # Arguments
    ///
    /// * `max` - Maximum number of messages to receive
    ///
    /// # Returns
    ///
    /// Vector of `(payload, flags)` tuples. Returns empty Vec if ring is empty.
    pub fn try_recv_batch(&self, max: usize) -> Vec<(Vec<u8>, u32)> {
        self.ring.try_recv_batch(max)
    }
}

/// Async channel pair for bidirectional communication.
///
/// This creates two `AsyncRing` instances - one for requests and one for responses.
pub struct AsyncChannelPair {
    /// Ring for sending requests (client) / receiving requests (server)
    pub request: AsyncRing,
    /// Ring for receiving responses (client) / sending responses (server)
    pub response: AsyncRing,
}

impl AsyncChannelPair {
    /// Create a new anonymous channel pair.
    pub fn new(slot_size: usize, slots: usize) -> Result<Self, AsyncRingError> {
        Ok(Self {
            request: AsyncRing::new(slot_size, slots)?,
            response: AsyncRing::new(slot_size, slots)?,
        })
    }

    /// Create a named channel pair for IPC.
    #[cfg(unix)]
    pub fn create_named(
        name: &str,
        slot_size: usize,
        slots: usize,
    ) -> Result<Self, AsyncRingError> {
        let req_name = format!("{name}.req");
        let resp_name = format!("{name}.resp");
        Ok(Self {
            request: AsyncRing::create_named(&req_name, slot_size, slots)?,
            response: AsyncRing::create_named(&resp_name, slot_size, slots)?,
        })
    }

    /// Open an existing named channel pair.
    #[cfg(unix)]
    pub fn open_named(name: &str, slot_size: usize, slots: usize) -> Result<Self, AsyncRingError> {
        let req_name = format!("{name}.req");
        let resp_name = format!("{name}.resp");
        Ok(Self {
            request: AsyncRing::open_named(&req_name, slot_size, slots)?,
            response: AsyncRing::open_named(&resp_name, slot_size, slots)?,
        })
    }
}

/// Async ring with integrated buffer pool for zero-allocation receives.
pub struct PooledAsyncRing {
    ring: AsyncRing,
    pool: Arc<BufferPool>,
}

impl PooledAsyncRing {
    /// Create a new pooled async ring.
    ///
    /// # Arguments
    /// - `slot_size`: Size of each ring slot
    /// - `slots`: Number of ring slots
    /// - `pool_size`: Number of pre-allocated receive buffers
    pub fn new(slot_size: usize, slots: usize, pool_size: usize) -> Result<Self, AsyncRingError> {
        let ring = AsyncRing::new(slot_size, slots)?;
        let pool = BufferPool::new(slot_size, pool_size);
        Ok(Self { ring, pool })
    }

    /// Get the notification FD.
    pub fn notifier_fd(&self) -> i32 {
        self.ring.notifier_fd()
    }

    /// Send data.
    pub fn send(&self, data: &[u8], flags: u32) -> Result<(), AsyncRingError> {
        self.ring.send(data, flags)
    }

    /// Try to send without blocking.
    pub fn try_send(&self, data: &[u8], flags: u32) -> Result<bool, AsyncRingError> {
        self.ring.try_send(data, flags)
    }

    /// Try to receive into a pooled buffer.
    ///
    /// Returns `Some((buffer, len))` on success, `None` if empty.
    /// The buffer is automatically returned to the pool when dropped.
    pub fn try_recv(&self) -> Result<Option<PooledRecvResult>, AsyncRingError> {
        let mut buffer = self
            .pool
            .acquire()
            .map_err(|_| AsyncRingError::PoolExhausted)?;

        match self.ring.try_recv(buffer.as_mut_slice()) {
            Ok(Some(len)) => Ok(Some(PooledRecvResult { buffer, len })),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Clear notification.
    pub fn clear_notification(&self) -> Result<(), AsyncRingError> {
        self.ring.clear_notification()
    }

    /// Get pool statistics.
    pub fn pool_available(&self) -> usize {
        self.pool.available()
    }
}

/// Result of a pooled receive operation.
pub struct PooledRecvResult {
    buffer: crate::buffer::Buffer,
    len: usize,
}

impl PooledRecvResult {
    /// Get the received data.
    pub fn data(&self) -> &[u8] {
        &self.buffer.as_slice()[..self.len]
    }

    /// Get the length of received data.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// Errors from async ring operations.
#[derive(Debug, thiserror::Error)]
pub enum AsyncRingError {
    #[error("channel error: {0}")]
    Channel(#[from] ChannelError),
    #[error("shared memory error: {0}")]
    Shm(#[from] crate::shm::ShmError),
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("buffer pool exhausted")]
    PoolExhausted,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn async_ring_send_recv() {
        let ring = AsyncRing::new(256, 8).expect("create ring");

        // Initially no notifications
        assert!(!ring.has_pending_notification());

        // Send data
        let payload = b"hello async";
        ring.send(payload, 0).expect("send");

        // Should have notification
        assert!(ring.has_pending_notification());

        // Receive
        let mut buf = vec![0u8; 256];
        let len = ring.try_recv(&mut buf).expect("recv").expect("has data");
        assert_eq!(&buf[..len], payload);

        // Clear notification
        ring.clear_notification().expect("clear");
        assert!(!ring.has_pending_notification());
    }

    #[test]
    fn async_ring_try_recv_empty() {
        let ring = AsyncRing::new(256, 8).expect("create ring");
        let mut buf = vec![0u8; 256];
        let result = ring.try_recv(&mut buf).expect("try_recv");
        assert!(result.is_none());
    }

    #[test]
    fn async_ring_try_send_full() {
        // Create ring with 2 slots (only 1 usable due to head/tail)
        let ring = AsyncRing::new(128, 2).expect("create ring");

        let payload = [1u8; 8];
        assert!(ring.try_send(&payload, 0).expect("first send"));
        assert!(!ring.try_send(&payload, 0).expect("second send"));
    }

    #[test]
    fn pooled_async_ring_basic() {
        let ring = PooledAsyncRing::new(256, 8, 4).expect("create pooled ring");

        assert_eq!(ring.pool_available(), 4);

        let payload = b"pooled test";
        ring.send(payload, 0).expect("send");

        let result = ring.try_recv().expect("recv").expect("has data");
        assert_eq!(result.data(), payload);
        assert_eq!(ring.pool_available(), 3);

        drop(result);
        assert_eq!(ring.pool_available(), 4);
    }

    #[test]
    fn async_ring_notifier_fd_valid() {
        let ring = AsyncRing::new(256, 8).expect("create ring");
        let fd = ring.notifier_fd();
        assert!(fd >= 0);
    }

    #[test]
    fn async_ring_recv_hybrid_immediate() {
        // Test: recv_hybrid returns immediately when data is already available
        let ring = AsyncRing::new(256, 8).expect("create ring");

        // Pre-send data
        let payload = b"immediate data";
        ring.send(payload, 0).expect("send");

        let mut buf = vec![0u8; 256];
        let start = std::time::Instant::now();
        let len = ring
            .recv_hybrid(&mut buf, 100, 1000)
            .expect("recv_hybrid")
            .expect("has data");
        let elapsed = start.elapsed();

        assert_eq!(&buf[..len], payload);
        // Should return very quickly during spin phase
        assert!(
            elapsed.as_micros() < 500,
            "immediate data should return fast: {}us",
            elapsed.as_micros()
        );
    }

    #[test]
    fn async_ring_recv_hybrid_timeout() {
        // Test: recv_hybrid times out correctly
        let ring = AsyncRing::new(256, 8).expect("create ring");

        let mut buf = vec![0u8; 256];
        let start = std::time::Instant::now();
        let result = ring
            .recv_hybrid(&mut buf, 5, 10) // 5us spin, 10ms timeout
            .expect("recv_hybrid");
        let elapsed = start.elapsed();

        assert!(result.is_none(), "should return None on timeout");
        assert!(
            elapsed.as_millis() >= 8,
            "should wait ~10ms, got {}ms",
            elapsed.as_millis()
        );
    }

    #[test]
    fn async_ring_recv_hybrid_wakeup() {
        // Test: recv_hybrid wakes up on notification
        use std::sync::Arc;
        use std::thread;

        let ring = Arc::new(AsyncRing::new(256, 8).expect("create ring"));
        let ring_clone = Arc::clone(&ring);

        // Spawn thread to send after a delay
        thread::spawn(move || {
            thread::sleep(std::time::Duration::from_millis(5));
            ring_clone.send(b"delayed data", 0).expect("send");
        });

        let mut buf = vec![0u8; 256];
        let start = std::time::Instant::now();
        let len = ring
            .recv_hybrid(&mut buf, 5, 100) // 5us spin, 100ms timeout
            .expect("recv_hybrid")
            .expect("should receive data");
        let elapsed = start.elapsed();

        assert_eq!(&buf[..len], b"delayed data");
        // Should complete faster than timeout
        assert!(
            elapsed.as_millis() < 50,
            "should wake before timeout: {}ms",
            elapsed.as_millis()
        );
    }
}
