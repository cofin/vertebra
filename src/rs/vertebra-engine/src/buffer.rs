//! Shared memory layout and buffer management.
//!
//! Defines:
//! - Cache-line aligned headers for false-sharing prevention
//! - ShmHeader with cache-line aligned atomics for ring buffer metadata
//! - SlotHeader for individual message slots
//! - RingLayout for memory calculations
//! - BufferPool for buffer reuse
//!
//! Ported from litecore::buffer

use core::sync::atomic::{AtomicU32, AtomicU64};
use std::cell::UnsafeCell;
use std::sync::{Arc, Mutex};

/// Cache line size used to isolate hot atomics.
///
/// Apple Silicon (M1/M2/M3) uses 128-byte cache lines.
/// x86-64 and other ARM64 use 64-byte cache lines.
#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
pub const CACHELINE_BYTES: usize = 128;
#[cfg(not(all(target_arch = "aarch64", target_os = "macos")))]
pub const CACHELINE_BYTES: usize = 64;

/// ABI version for shared memory layouts.
/// Increment this when layout changes are incompatible.
pub const ABI_VERSION: u32 = 2;

/// Magic marker for validating mapped regions ("LITECORE").
pub const MAGIC: u64 = u64::from_le_bytes(*b"LITECORE");

/// Default slot sizes for ring buffers.
/// 64KB is a good default for HTTP request/response bodies.
pub const DEFAULT_SLOT_SIZE: usize = 65536;

/// Slot alignment for payloads (bytes).
pub const SLOT_ALIGN: usize = 64;

/// Shared memory region state.
#[repr(u32)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum RegionState {
    /// Region is not yet initialized.
    Uninitialized = 0,
    /// Region is ready for use.
    Ready = 1,
    /// Region is stale (server restarted, old mapping).
    Stale = 2,
}

impl From<u32> for RegionState {
    fn from(value: u32) -> Self {
        match value {
            0 => RegionState::Uninitialized,
            1 => RegionState::Ready,
            2 => RegionState::Stale,
            _ => RegionState::Uninitialized,
        }
    }
}

/// Cache-line aligned wrapper to avoid false sharing.
///
/// On Apple Silicon, cache lines are 128 bytes.
/// On x86-64 and other ARM64, cache lines are 64 bytes.
#[cfg_attr(all(target_arch = "aarch64", target_os = "macos"), repr(align(128)))]
#[cfg_attr(
    not(all(target_arch = "aarch64", target_os = "macos")),
    repr(align(64))
)]
#[repr(C)]
pub struct CacheLine<T>(pub T);

impl<T: Default> Default for CacheLine<T> {
    fn default() -> Self {
        Self(T::default())
    }
}

/// Fixed header layout for all shared-memory regions.
///
/// The header is placed at the start of the mapping followed by the ring buffer.
/// This layout must match the Python ctypes.Structure definition exactly.
///
/// # Layout (for 64-byte cache lines):
/// - Bytes 0-63: Core metadata
/// - Bytes 64-127: read_head (cache-line aligned)
/// - Bytes 128-191: write_head (cache-line aligned)
/// - Bytes 192-255: wait_read (cache-line aligned)
/// - Bytes 256-319: wait_write (cache-line aligned)
/// - Bytes 320-383: send_waiters (cache-line aligned)
/// - Bytes 384-447: recv_waiters (cache-line aligned)
#[repr(C)]
pub struct ShmHeader {
    /// Magic marker for validation ("LITECORE").
    pub magic: u64,
    /// ABI version for compatibility checking.
    pub abi_version: u32,
    /// Flags (reserved for future use).
    pub flags: u32,
    /// Total size of the shared memory region.
    pub total_size: u64,
    /// Offset from region start to ring buffer.
    pub ring_offset: u64,
    /// Size of the ring buffer in bytes.
    pub ring_size: u64,
    /// Generation counter for detecting stale mappings.
    pub generation: AtomicU32,
    /// Region state (see RegionState).
    pub state: AtomicU32,
    /// PID of the process that created this region.
    pub creator_pid: u32,
    /// Reserved for alignment.
    pub reserved0: u32,
    /// Epoch timestamp when region was created.
    pub creator_epoch: u64,

    /// Ring buffer metadata (Small, Medium, Large).
    pub rings: [RingMetadata; 3],
}

/// Ring buffer metadata with cache-line isolation.
#[repr(C)]
pub struct RingMetadata {
    /// Read head position (consumer advances this).
    pub read_head: CacheLine<AtomicU64>,
    /// Write head position (producer advances this).
    pub write_head: CacheLine<AtomicU64>,
    /// Futex address for read waits.
    pub wait_read: CacheLine<AtomicU32>,
    /// Futex address for write waits.
    pub wait_write: CacheLine<AtomicU32>,
    /// Number of threads blocked waiting to send (ring full).
    /// Used for conditional wake optimization - only wake if waiters > 0.
    pub send_waiters: CacheLine<AtomicU32>,
    /// Number of threads blocked waiting to recv (ring empty).
    /// Used for conditional wake optimization - only wake if waiters > 0.
    pub recv_waiters: CacheLine<AtomicU32>,
}

impl ShmHeader {
    /// Validate the header magic and ABI version.
    pub fn validate(&self) -> Result<(), BufferError> {
        if self.magic != MAGIC {
            return Err(BufferError::InvalidMagic);
        }
        if self.abi_version != ABI_VERSION {
            return Err(BufferError::AbiMismatch {
                expected: ABI_VERSION,
                found: self.abi_version,
            });
        }
        Ok(())
    }

    /// Check if the region is ready for use.
    pub fn is_ready(&self) -> bool {
        let state = self.state.load(std::sync::atomic::Ordering::Acquire);
        RegionState::from(state) == RegionState::Ready
    }

    /// Mark the region as ready.
    pub fn set_ready(&self) {
        self.state.store(
            RegionState::Ready as u32,
            std::sync::atomic::Ordering::Release,
        );
    }

    /// Mark the region as stale.
    pub fn set_stale(&self) {
        self.state.store(
            RegionState::Stale as u32,
            std::sync::atomic::Ordering::Release,
        );
    }
}

/// Channel configuration for the shared-memory ring.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ChannelMode {
    /// Single-producer, single-consumer (no sequence array needed).
    Spsc,
    /// Multi-producer, multi-consumer (per-slot sequence numbers).
    Mpmc,
}

/// Slot header prefix for individual messages in the ring buffer.
///
/// This header is placed at the start of each slot in the ring buffer.
/// The payload follows immediately after.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct SlotHeader {
    /// Length of the payload in bytes.
    pub len: u32,
    /// Flags for this slot.
    pub flags: u32,
}

impl SlotHeader {
    /// Size of the header in bytes.
    pub const SIZE: usize = std::mem::size_of::<Self>();

    /// Create a new slot header.
    pub const fn new(len: u32, flags: u32) -> Self {
        Self { len, flags }
    }
}

/// Slot flags.
pub mod slot_flags {
    /// Slot contains a complete message.
    pub const COMPLETE: u32 = 0;
    /// Slot contains a partial message (more to follow).
    pub const PARTIAL: u32 = 1 << 0;
    /// Slot contains the final fragment of a multi-slot message.
    pub const FINAL_FRAGMENT: u32 = 1 << 1;
    /// Slot contains an error message.
    pub const ERROR: u32 = 1 << 2;
    /// Slot is a request (for bidirectional channels).
    pub const REQUEST: u32 = 1 << 3;
    /// Slot is a response (for bidirectional channels).
    pub const RESPONSE: u32 = 1 << 4;
}

/// Buffer pool error types.
#[derive(Debug, thiserror::Error)]
pub enum BufferError {
    /// Buffer pool is exhausted (no free buffers).
    #[error("buffer pool exhausted")]
    Exhausted,
    /// Buffer index is out of range.
    #[error("buffer index out of range")]
    InvalidIndex,
    /// Invalid magic number in header.
    #[error("invalid magic number in shared memory header")]
    InvalidMagic,
    /// ABI version mismatch.
    #[error("ABI version mismatch: expected {expected}, found {found}")]
    AbiMismatch { expected: u32, found: u32 },
}

/// Simple buffer pool for zero-copy workflows.
///
/// Provides pre-allocated buffers that can be acquired and released.
/// Buffers automatically return to the pool when dropped.
#[derive(Debug)]
pub struct BufferPool {
    buffer_size: usize,
    count: usize,
    storage: UnsafeCell<Vec<u8>>,
    free_list: Mutex<Vec<usize>>,
}

// SAFETY: BufferPool uses a Mutex for the free list and UnsafeCell is only
// accessed through Buffer handles that ensure exclusive access.
unsafe impl Send for BufferPool {}
unsafe impl Sync for BufferPool {}

impl BufferPool {
    /// Create a new buffer pool with the given buffer size and count.
    pub fn new(buffer_size: usize, count: usize) -> Arc<Self> {
        let total = buffer_size * count;
        let storage = UnsafeCell::new(vec![0u8; total]);
        let free_list = (0..count).collect();
        Arc::new(Self {
            buffer_size,
            count,
            storage,
            free_list: Mutex::new(free_list),
        })
    }

    /// Acquire a buffer from the pool.
    ///
    /// Returns `Err(BufferError::Exhausted)` if no buffers are available.
    pub fn acquire(self: &Arc<Self>) -> Result<Buffer, BufferError> {
        let mut guard = self.free_list.lock().map_err(|_| BufferError::Exhausted)?;
        let index = guard.pop().ok_or(BufferError::Exhausted)?;
        Ok(Buffer {
            pool: Arc::clone(self),
            index,
        })
    }

    /// Get the total number of buffers in the pool.
    pub fn count(&self) -> usize {
        self.count
    }

    /// Get the number of currently available buffers.
    pub fn available(&self) -> usize {
        self.free_list.lock().map(|guard| guard.len()).unwrap_or(0)
    }

    /// Get the size of each buffer.
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    fn release(&self, index: usize) {
        if let Ok(mut guard) = self.free_list.lock() {
            if cfg!(debug_assertions) && guard.contains(&index) {
                panic!("double free detected for buffer index {index}");
            }
            guard.push(index);
        }
    }

    /// Reconstruct a buffer from a raw index.
    ///
    /// # Safety
    /// Caller must ensure the index originated from this pool and is currently checked out.
    pub unsafe fn from_raw(self: &Arc<Self>, index: usize) -> Result<Buffer, BufferError> {
        if index >= self.count {
            return Err(BufferError::InvalidIndex);
        }
        Ok(Buffer {
            pool: Arc::clone(self),
            index,
        })
    }
}

/// Handle to an acquired buffer. Returns to pool on Drop.
#[derive(Debug)]
pub struct Buffer {
    pool: Arc<BufferPool>,
    index: usize,
}

impl Buffer {
    /// Get an immutable slice of the buffer contents.
    pub fn as_slice(&self) -> &[u8] {
        let start = self.index * self.pool.buffer_size;
        let end = start + self.pool.buffer_size;
        // SAFETY: caller holds Buffer handle, ensuring exclusive access.
        let storage: &Vec<u8> = unsafe { &*self.pool.storage.get() };
        &storage[start..end]
    }

    /// Get a mutable slice of the buffer contents.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        let start = self.index * self.pool.buffer_size;
        let end = start + self.pool.buffer_size;
        // SAFETY: caller holds Buffer handle, ensuring exclusive access.
        let storage: &mut Vec<u8> = unsafe { &mut *self.pool.storage.get() };
        &mut storage[start..end]
    }

    /// Get the size of this buffer.
    pub fn len(&self) -> usize {
        self.pool.buffer_size
    }

    /// Check if this buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.pool.buffer_size == 0
    }

    /// Get the index of this buffer in the pool.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Consume the buffer and return the raw index.
    ///
    /// This prevents the buffer from being returned to the pool.
    /// Use `BufferPool::from_raw` to reconstruct the buffer.
    pub fn into_raw_index(self) -> usize {
        let index = self.index;
        std::mem::forget(self);
        index
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        self.pool.release(self.index);
    }
}

/// Describes a ring layout derived from slot size + slot count.
#[derive(Debug, Copy, Clone)]
pub struct RingLayout {
    /// Channel mode (SPSC or MPMC).
    pub mode: ChannelMode,
    /// Size of each slot in bytes.
    pub slot_size: usize,
    /// Number of slots in the ring.
    pub slots: usize,
    /// Size of the sequence array in bytes (0 for SPSC).
    pub seq_size: usize,
    /// Size of the ring buffer data area in bytes.
    pub ring_size: usize,
    /// Offset to the sequence array (from region start).
    pub seq_offset: usize,
    /// Offset to the ring buffer data (from region start).
    pub ring_offset: usize,
    /// Total size of the shared memory region.
    pub total_size: usize,
}

impl RingLayout {
    /// Create a new ring layout with the given parameters.
    pub fn new(slot_size: usize, slots: usize, mode: ChannelMode) -> Self {
        // MPMC needs per-slot sequence numbers for coordination
        let seq_size = match mode {
            ChannelMode::Spsc => 0,
            ChannelMode::Mpmc => slots * core::mem::size_of::<AtomicU64>(),
        };
        let ring_size = slot_size * slots;
        let header_size = align_up(
            core::mem::size_of::<ShmHeader>(),
            CACHELINE_BYTES.max(SLOT_ALIGN),
        );
        let seq_offset = header_size;
        let ring_offset = header_size + seq_size;
        let total_size = ring_offset + ring_size;
        Self {
            mode,
            slot_size,
            slots,
            seq_size,
            ring_size,
            seq_offset,
            ring_offset,
            total_size,
        }
    }

    /// Check if the slot count is a power of two.
    ///
    /// Power-of-two slot counts allow using bitwise AND for wraparound
    /// instead of modulo, which is faster.
    pub fn is_power_of_two(&self) -> bool {
        self.slots.is_power_of_two()
    }

    /// Get the offset to the sequence array from the region start.
    pub fn seq_offset(&self) -> usize {
        self.seq_offset
    }

    /// Get the offset to the ring buffer data from the region start.
    pub fn ring_offset(&self) -> usize {
        self.ring_offset
    }

    /// Get the mask for fast index wraparound (only valid if power-of-two slots).
    pub fn index_mask(&self) -> usize {
        debug_assert!(self.is_power_of_two());
        self.slots - 1
    }
}

/// Layout for a multi-tiered slab allocator.
#[derive(Debug, Copy, Clone)]
pub struct SlabLayout {
    pub small: RingLayout,
    pub medium: RingLayout,
    pub large: RingLayout,
    pub total_size: usize,
}

impl SlabLayout {
    /// Create a new slab layout.
    ///
    /// Configures 3 MPMC rings (Small, Medium, Large) in a single region.
    pub fn new(
        small_slots: usize,
        medium_slots: usize,
        large_slots: usize,
        small_size: usize,
        medium_size: usize,
        large_size: usize,
    ) -> Self {
        let header_size = align_up(
            core::mem::size_of::<ShmHeader>(),
            CACHELINE_BYTES.max(SLOT_ALIGN),
        );

        // Helper to create layout for one tier
        let make_layout = |slots: usize, slot_size: usize, start_offset: usize| -> RingLayout {
            let seq_size = slots * core::mem::size_of::<AtomicU64>();
            let ring_size = slots * slot_size;
            let seq_offset = start_offset;
            let ring_offset = align_up(seq_offset + seq_size, CACHELINE_BYTES.max(SLOT_ALIGN));
            let end_offset = ring_offset + ring_size;
            
            RingLayout {
                mode: ChannelMode::Mpmc,
                slot_size,
                slots,
                seq_size,
                ring_size,
                seq_offset,
                ring_offset,
                total_size: end_offset, // Not used for individual rings in slab, but valid for offset calculation
            }
        };

        let small = make_layout(small_slots, small_size, header_size);
        // Align next section to page boundary or cache line? Cache line should be enough for shared memory threads.
        // But maybe page alignment is better for TLB?
        // Let's stick to cache line alignment for compactness, unless we need separate mmap views (we don't).
        let medium_start = align_up(small.total_size, CACHELINE_BYTES);
        let medium = make_layout(medium_slots, medium_size, medium_start);
        
        let large_start = align_up(medium.total_size, CACHELINE_BYTES);
        let large = make_layout(large_slots, large_size, large_start);

        Self {
            small,
            medium,
            large,
            total_size: large.total_size,
        }
    }
}

/// Align a value up to the given alignment.
fn align_up(value: usize, align: usize) -> usize {
    value.div_ceil(align) * align
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_alignment_matches_cacheline() {
        assert_eq!(core::mem::align_of::<CacheLine<u8>>(), CACHELINE_BYTES);
    }

    #[test]
    fn slot_header_size_is_eight_bytes() {
        assert_eq!(SlotHeader::SIZE, 8);
    }

    #[test]
    fn buffer_pool_acquire_release() {
        let pool = BufferPool::new(64, 2);
        let mut a = pool.acquire().expect("buffer a");
        let _b = pool.acquire().expect("buffer b");
        assert!(pool.acquire().is_err());
        a.as_mut_slice()[0] = 7;
        drop(a);
        assert!(pool.acquire().is_ok());
    }

    #[test]
    fn buffer_pool_exhaustion() {
        let pool = BufferPool::new(32, 1);
        let _buf = pool.acquire().unwrap();
        assert!(matches!(pool.acquire(), Err(BufferError::Exhausted)));
    }

    #[test]
    fn buffer_pool_concurrent_acquire_release() {
        let pool = BufferPool::new(64, 4);
        std::thread::scope(|s| {
            for _ in 0..4 {
                let pool = Arc::clone(&pool);
                s.spawn(move || {
                    let mut buf = pool.acquire().unwrap();
                    buf.as_mut_slice()[0] = 1;
                });
            }
        });
        assert_eq!(pool.available(), 4);
    }

    #[test]
    fn ring_layout_spsc_no_sequences() {
        let layout = RingLayout::new(1024, 16, ChannelMode::Spsc);
        assert_eq!(layout.seq_size, 0);
        assert!(layout.is_power_of_two());
    }

    #[test]
    fn ring_layout_mpmc_includes_sequences() {
        let slots = 64;
        let slot_size = 256;
        let layout = RingLayout::new(slot_size, slots, ChannelMode::Mpmc);
        let expected_seq_size = slots * core::mem::size_of::<AtomicU64>();
        assert_eq!(layout.seq_size, expected_seq_size);
        assert_eq!(
            layout.total_size,
            layout.seq_offset + expected_seq_size + (slot_size * slots)
        );
    }

    #[test]
    fn ring_layout_index_mask() {
        let layout = RingLayout::new(256, 16, ChannelMode::Spsc);
        assert_eq!(layout.index_mask(), 15);
        assert_eq!(17 & layout.index_mask(), 1); // Wraparound test
    }

    #[test]
    fn region_state_conversion() {
        assert_eq!(RegionState::from(0), RegionState::Uninitialized);
        assert_eq!(RegionState::from(1), RegionState::Ready);
        assert_eq!(RegionState::from(2), RegionState::Stale);
        assert_eq!(RegionState::from(99), RegionState::Uninitialized);
    }

    #[test]
    fn buffer_into_raw_and_reconstruct() {
        let pool = BufferPool::new(64, 2);
        let buf = pool.acquire().unwrap();
        let idx = buf.into_raw_index();
        assert_eq!(pool.available(), 1); // Buffer not returned

        // SAFETY: Index is valid and checked out
        let buf2 = unsafe { pool.from_raw(idx).unwrap() };
        drop(buf2);
        assert_eq!(pool.available(), 2); // Now returned
    }
}
