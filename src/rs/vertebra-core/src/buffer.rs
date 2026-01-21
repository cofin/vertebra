//! Shared memory layout and ring buffer metadata.

use core::sync::atomic::{AtomicU32, AtomicU64};
use std::cell::UnsafeCell;
use std::sync::{Arc, Mutex};

use arrow::error::ArrowError;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

/// Cache line size used to isolate hot atomics.
///
/// Keep this in sync with `CacheLine<T>` alignment below.
#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
pub const CACHELINE_BYTES: usize = 128;
#[cfg(not(all(target_arch = "aarch64", target_os = "macos")))]
pub const CACHELINE_BYTES: usize = 64;

/// ABI version for shared memory layouts.
pub const ABI_VERSION: u32 = 2;

/// Magic marker for validating mapped regions ("VERTEBRA").
pub const MAGIC: u64 = u64::from_le_bytes(*b"VERTEBRA");

/// Supported fixed payload sizes (bytes) for ring slots.
pub const SLOT_SIZES: [usize; 3] = [256, 1024, 4096];

/// Slot alignment for payloads (bytes).
pub const SLOT_ALIGN: usize = 64;

/// Shared memory region state.
#[repr(u32)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum RegionState {
    Uninitialized = 0,
    Ready = 1,
    Stale = 2,
}

/// Cache-line aligned wrapper to avoid false sharing.
#[cfg_attr(all(target_arch = "aarch64", target_os = "macos"), repr(align(128)))]
#[cfg_attr(
    not(all(target_arch = "aarch64", target_os = "macos")),
    repr(align(64))
)]
#[repr(C)]
pub struct CacheLine<T>(pub T);

/// Fixed header layout for all shared-memory regions.
///
/// The header is placed at the start of the mapping followed by the ring buffer.
#[repr(C)]
pub struct ShmHeader {
    pub magic: u64,
    pub abi_version: u32,
    pub flags: u32,
    pub total_size: u64,
    pub ring_offset: u64,
    pub ring_size: u64,
    pub generation: AtomicU32,
    pub state: AtomicU32,
    pub creator_pid: u32,
    pub reserved0: u32,
    pub creator_epoch: u64,
    pub reserved1: [u8; 32],
    pub read_head: CacheLine<AtomicU64>,
    pub write_head: CacheLine<AtomicU64>,
    pub wait_read: CacheLine<AtomicU32>,
    pub wait_write: CacheLine<AtomicU32>,
}

/// Channel configuration for the shared-memory ring.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ChannelMode {
    Spsc,
    Mpmc,
}

/// Slot header prefix (fixed-size).
#[repr(C)]
pub struct SlotHeader {
    pub len: u32,
    pub flags: u32,
}

/// Simple buffer pool for zero-copy workflows.
#[derive(Debug)]
pub struct BufferPool {
    buffer_size: usize,
    count: usize,
    storage: UnsafeCell<Vec<u8>>,
    free_list: Mutex<Vec<usize>>,
}

unsafe impl Send for BufferPool {}
unsafe impl Sync for BufferPool {}

#[derive(Debug, thiserror::Error)]
pub enum BufferError {
    #[error("buffer pool exhausted")]
    Exhausted,
    #[error("buffer index out of range")]
    InvalidIndex,
}

impl BufferPool {
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

    pub fn acquire(self: &Arc<Self>) -> Result<Buffer, BufferError> {
        let mut guard = self.free_list.lock().map_err(|_| BufferError::Exhausted)?;
        let index = guard.pop().ok_or(BufferError::Exhausted)?;
        Ok(Buffer {
            pool: Arc::clone(self),
            index,
        })
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn available(&self) -> usize {
        self.free_list.lock().map(|guard| guard.len()).unwrap_or(0)
    }

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
    pub fn as_slice(&self) -> &[u8] {
        let start = self.index * self.pool.buffer_size;
        let end = start + self.pool.buffer_size;
        // SAFETY: caller holds Buffer handle, ensuring exclusive access.
        let storage: &Vec<u8> = unsafe { &*self.pool.storage.get() };
        &storage[start..end]
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        let start = self.index * self.pool.buffer_size;
        let end = start + self.pool.buffer_size;
        // SAFETY: caller holds Buffer handle, ensuring exclusive access.
        let storage: &mut Vec<u8> = unsafe { &mut *self.pool.storage.get() };
        &mut storage[start..end]
    }

    pub fn len(&self) -> usize {
        self.pool.buffer_size
    }

    pub fn is_empty(&self) -> bool {
        self.pool.buffer_size == 0
    }

    pub fn index(&self) -> usize {
        self.index
    }

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

/// Write a RecordBatch into an Arrow IPC stream buffer.
pub fn write_arrow_ipc(batch: &RecordBatch, buffer: &mut [u8]) -> Result<usize, ArrowError> {
    let mut cursor = std::io::Cursor::new(buffer);
    let mut writer = StreamWriter::try_new(&mut cursor, batch.schema().as_ref())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(cursor.position() as usize)
}

/// Read RecordBatches from an Arrow IPC stream buffer.
pub fn read_arrow_ipc(buffer: &[u8]) -> Result<Vec<RecordBatch>, ArrowError> {
    let cursor = std::io::Cursor::new(buffer);
    let reader = StreamReader::try_new(cursor, None)?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(batches)
}

/// Describes a ring layout derived from slot size + slot count.
#[derive(Debug, Copy, Clone)]
pub struct RingLayout {
    pub mode: ChannelMode,
    pub slot_size: usize,
    pub slots: usize,
    pub seq_size: usize,
    pub ring_size: usize,
    pub header_size: usize,
    pub total_size: usize,
}

impl RingLayout {
    pub fn new(slot_size: usize, slots: usize, mode: ChannelMode) -> Self {
        let seq_size = match mode {
            ChannelMode::Spsc => 0,
            ChannelMode::Mpmc => slots * core::mem::size_of::<AtomicU64>(),
        };
        let ring_size = slot_size * slots;
        let header_size = align_up(
            core::mem::size_of::<ShmHeader>(),
            CACHELINE_BYTES.max(SLOT_ALIGN),
        );
        let total_size = header_size + seq_size + ring_size;
        Self {
            mode,
            slot_size,
            slots,
            seq_size,
            ring_size,
            header_size,
            total_size,
        }
    }

    pub fn is_power_of_two(&self) -> bool {
        self.slots.is_power_of_two()
    }

    pub fn seq_offset(&self) -> usize {
        self.header_size
    }

    pub fn ring_offset(&self) -> usize {
        self.header_size + self.seq_size
    }
}

fn align_up(value: usize, align: usize) -> usize {
    value.div_ceil(align) * align
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use proptest::prelude::*;
    use std::sync::Arc;

    #[test]
    fn header_alignment_matches_cacheline() {
        assert_eq!(core::mem::align_of::<CacheLine<u8>>(), CACHELINE_BYTES);
    }

    #[test]
    fn slot_sizes_are_power_of_two() {
        for size in SLOT_SIZES {
            assert!(size.is_power_of_two());
        }
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
    fn ring_layout_mpmc_includes_sequences() {
        let slots = 64;
        let slot_size = 256;
        let layout = RingLayout::new(slot_size, slots, ChannelMode::Mpmc);
        let expected_seq_size = slots * core::mem::size_of::<AtomicU64>();
        assert_eq!(layout.seq_size, expected_seq_size);
        assert_eq!(layout.total_size, layout.header_size + expected_seq_size + (slot_size * slots));
    }

    proptest! {
        #[test]
        fn buffer_pool_no_double_free(count in 1usize..8, buffer_size in 8usize..256) {
            let pool = BufferPool::new(buffer_size, count);
            let mut buffers = Vec::new();
            for _ in 0..count {
                buffers.push(pool.acquire().unwrap());
            }
            drop(buffers);
            prop_assert_eq!(pool.available(), count);
        }
    }

    #[test]
    fn arrow_ipc_round_trip() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        let mut buffer = vec![0u8; 1024];
        let written = write_arrow_ipc(&batch, &mut buffer).unwrap();
        let batches = read_arrow_ipc(&buffer[..written]).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }
}
