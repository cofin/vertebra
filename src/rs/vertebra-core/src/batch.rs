//! Batch arena allocator for zero-copy request batching.
//!
//! `BatchArena` pre-allocates a contiguous block of memory large enough for
//! `capacity` `BatchNode` entries. Allocation is a bump pointer advance
//! (one `fetch_add` on an `AtomicUsize`). Freeing is a bulk reset
//! (`store(0)`), amortized across an entire batch.
//!
//! # Design Invariants
//!
//! - Only ONE thread (the HTTP collector) calls `push`. No locking on alloc.
//! - Multiple threads (Python workers) may call read-only getters concurrently.
//! - `reset()` is called by Rust ONLY after all responses for this batch have
//!   been submitted and acknowledged. Python MUST NOT hold any `BatchNode`
//!   pointers after `submit_batch` returns.
//! - The arena is NOT shared across processes; it lives in regular heap memory.
//!   The ring buffer carries `u64` arena pointers (cast from `*const BatchNode`).
//!
//! # Allocation Rate Target
//!
//! Target: <1 MB/s allocation rate at 10 k req/s with avg 80-byte path strings.
//! With 128-byte `BatchNode` and 64-entry batches: 8 192 bytes/batch =
//! ~82 MB/s at 10 k req/s batches. Keep batch sizes ≤64 to stay well under
//! the target.
//!
//! # Memory Layout
//!
//! ```text
//! Arena slab (heap):
//! ┌─────────────────────────────────────────────────────────┐
//! │ BatchNode[0] │ BatchNode[1] │ ... │ BatchNode[N-1]      │
//! └─────────────────────────────────────────────────────────┘
//!      ▲               ▲
//!  bump=0          bump=1 (after first push)
//! ```

use std::alloc::{alloc, dealloc, Layout};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::proxy::RequestInfo;

/// A single node in a batch — one HTTP request waiting for Python to handle.
///
/// # Memory Layout (`#[repr(C)]`, 64-byte aligned)
///
/// ```text
/// Offset  Size  Field
/// ──────  ────  ─────────────────────────────────────────────
///      0    64  request_info: RequestInfo  (64-byte C-ABI struct)
///     64     8  body_ptr: *const u8        (pointer into body slab, or null)
///     72     8  body_len: usize
///     80     8  headers_ptr: *const u8     (pointer into headers slab, or null)
///     88     8  headers_len: usize
///     96     8  correlation_id: u64
///    104     8  _padding: [u8; 8]
/// ──────
///    112 bytes (padded to 128 for alignment)
/// ```
///
/// Field meanings:
/// - `request_info`: Full 64-byte C-ABI request metadata (method, handler_id, offsets).
/// - `body_ptr` / `body_len`: Raw pointer to body bytes in a separate body slab.
///   May be null for requests with no body.
/// - `headers_ptr` / `headers_len`: Raw pointer to serialised header bytes.
///   Format is a flat array of `(name_len: u16, name: [u8], value_len: u16, value: [u8])`.
/// - `correlation_id`: Monotonically increasing `u64` set by the HTTP collector.
///   Echoed back in `ResponseInfo` to match responses to requests.
#[repr(C, align(64))]
pub struct BatchNode {
    /// Compact HTTP request metadata.
    pub request_info: RequestInfo,
    /// Pointer to body bytes (may be null). Lifetime: arena reset.
    pub body_ptr: *const u8,
    /// Length of body in bytes.
    pub body_len: usize,
    /// Pointer to serialised header bytes (may be null).
    pub headers_ptr: *const u8,
    /// Length of headers buffer in bytes.
    pub headers_len: usize,
    /// Monotonic correlation identifier set by the HTTP collector.
    pub correlation_id: u64,
    _padding: [u8; 8],
}

// SAFETY: BatchNode contains raw pointers, but the arena owns the backing
// memory and guarantees the pointers are valid until `reset()`. Python reads
// are read-only. Safe to send across threads because only one thread writes.
unsafe impl Send for BatchNode {}
unsafe impl Sync for BatchNode {}

impl BatchNode {
    /// Construct a `BatchNode` from its components.
    pub fn new(
        request_info: RequestInfo,
        body_ptr: *const u8,
        body_len: usize,
        headers_ptr: *const u8,
        headers_len: usize,
        correlation_id: u64,
    ) -> Self {
        Self {
            request_info,
            body_ptr,
            body_len,
            headers_ptr,
            headers_len,
            correlation_id,
            _padding: [0u8; 8],
        }
    }

    /// Return the body as a byte slice.
    ///
    /// # Safety
    /// The caller must ensure the arena has not been reset since this node was
    /// allocated.
    pub unsafe fn body(&self) -> &[u8] {
        if self.body_ptr.is_null() || self.body_len == 0 {
            return &[];
        }
        std::slice::from_raw_parts(self.body_ptr, self.body_len)
    }

    /// Return the raw header bytes.
    ///
    /// # Safety
    /// Same lifetime contract as `body()`.
    pub unsafe fn headers_raw(&self) -> &[u8] {
        if self.headers_ptr.is_null() || self.headers_len == 0 {
            return &[];
        }
        std::slice::from_raw_parts(self.headers_ptr, self.headers_len)
    }
}

/// Batch arena error type.
#[derive(Debug, thiserror::Error)]
pub enum BatchError {
    #[error("arena is full (capacity: {capacity})")]
    Full { capacity: usize },
    #[error("arena capacity must be non-zero")]
    ZeroCapacity,
    #[error("allocation failed (OOM)")]
    Alloc,
}

/// Pre-allocated bump arena for `BatchNode` slabs.
///
/// Thread-safety: only the HTTP collector calls `push`. Workers call read-only
/// getters through shared pointers. `reset()` is called by Rust after all
/// responses are acknowledged (single writer, no concurrent `push`).
pub struct BatchArena {
    /// Raw pointer to the start of the slab.
    slab: NonNull<BatchNode>,
    /// Maximum number of `BatchNode` entries.
    capacity: usize,
    /// Bump cursor — next free slot index.
    bump: AtomicUsize,
    /// Layout used for deallocation.
    layout: Layout,
}

// SAFETY: `BatchArena` is designed for one writer + many readers.
// The slab pointer is stable for the lifetime of the arena.
unsafe impl Send for BatchArena {}
unsafe impl Sync for BatchArena {}

impl BatchArena {
    /// Create a new `BatchArena` capable of holding `capacity` `BatchNode` entries.
    ///
    /// Allocates `capacity * size_of::<BatchNode>()` bytes on the heap, aligned
    /// to 64 bytes.
    pub fn new(capacity: usize) -> Result<Self, BatchError> {
        if capacity == 0 {
            return Err(BatchError::ZeroCapacity);
        }
        let layout = Layout::array::<BatchNode>(capacity)
            .map_err(|_| BatchError::Alloc)?
            .align_to(64)
            .map_err(|_| BatchError::Alloc)?
            .pad_to_align();
        let ptr = unsafe { alloc(layout) };
        let slab = NonNull::new(ptr.cast::<BatchNode>()).ok_or(BatchError::Alloc)?;
        Ok(Self {
            slab,
            capacity,
            bump: AtomicUsize::new(0),
            layout,
        })
    }

    /// Push a `BatchNode` into the arena.
    ///
    /// Returns a raw pointer to the written node on success.
    /// Returns `BatchError::Full` when the arena is exhausted.
    ///
    /// # Safety (callers)
    /// Only call from the single HTTP collector thread.
    pub fn push(&self, node: BatchNode) -> Result<*const BatchNode, BatchError> {
        let idx = self.bump.fetch_add(1, Ordering::AcqRel);
        if idx >= self.capacity {
            // Undo the increment to prevent overflow wrapping.
            self.bump.fetch_sub(1, Ordering::AcqRel);
            return Err(BatchError::Full {
                capacity: self.capacity,
            });
        }
        // SAFETY: `idx < capacity` verified above; slab is valid heap memory.
        let ptr = unsafe { self.slab.as_ptr().add(idx) };
        unsafe { ptr.write(node) };
        Ok(ptr as *const BatchNode)
    }

    /// Reset the arena for reuse.
    ///
    /// All outstanding `BatchNode` pointers MUST be invalidated before calling
    /// this. Call only after all Python workers have called `submit_batch`.
    pub fn reset(&self) {
        self.bump.store(0, Ordering::Release);
    }

    /// Number of entries currently allocated.
    pub fn len(&self) -> usize {
        self.bump.load(Ordering::Acquire).min(self.capacity)
    }

    /// Returns `true` if no entries have been allocated since the last reset.
    pub fn is_empty(&self) -> bool {
        self.bump.load(Ordering::Acquire) == 0
    }

    /// Returns the maximum number of entries this arena can hold.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Return a raw slice over all allocated nodes (read-only).
    ///
    /// # Safety
    /// The caller must ensure no concurrent `push` or `reset` occurs.
    pub unsafe fn as_slice(&self) -> &[BatchNode] {
        let len = self.len();
        std::slice::from_raw_parts(self.slab.as_ptr(), len)
    }

    /// Return a pointer to the node at `index` without bounds-checking.
    ///
    /// # Safety
    /// `index` must be < `self.len()`.
    pub unsafe fn get_unchecked(&self, index: usize) -> *const BatchNode {
        self.slab.as_ptr().add(index) as *const BatchNode
    }
}

impl std::fmt::Debug for BatchArena {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchArena")
            .field("capacity", &self.capacity)
            .field("len", &self.len())
            .finish()
    }
}

impl Drop for BatchArena {
    fn drop(&mut self) {
        // SAFETY: slab was allocated with `self.layout`.
        unsafe { dealloc(self.slab.as_ptr().cast(), self.layout) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy::{HttpMethod, RequestInfo};

    fn make_node(correlation_id: u64) -> BatchNode {
        BatchNode::new(
            RequestInfo {
                method: HttpMethod::GET,
                _pad: [0u8; 7],
                handler_id: 1,
                path_offset: 0,
                path_len: 5,
                headers_offset: 0,
                body_offset: 0,
                body_len: 0,
                correlation_id,
            },
            std::ptr::null(),
            0,
            std::ptr::null(),
            0,
            correlation_id,
        )
    }

    #[test]
    fn arena_basic_push_and_len() {
        let arena = BatchArena::new(8).unwrap();
        assert!(arena.is_empty());
        let ptr = arena.push(make_node(1)).unwrap();
        assert!(!ptr.is_null());
        assert_eq!(arena.len(), 1);
    }

    #[test]
    fn arena_full_returns_error() {
        let arena = BatchArena::new(2).unwrap();
        arena.push(make_node(1)).unwrap();
        arena.push(make_node(2)).unwrap();
        let err = arena.push(make_node(3)).unwrap_err();
        assert!(matches!(err, BatchError::Full { .. }));
    }

    #[test]
    fn arena_reset_allows_reuse() {
        let arena = BatchArena::new(4).unwrap();
        for i in 0..4 {
            arena.push(make_node(i as u64)).unwrap();
        }
        assert_eq!(arena.len(), 4);
        arena.reset();
        assert!(arena.is_empty());
        arena.push(make_node(99)).unwrap();
        assert_eq!(arena.len(), 1);
    }

    #[test]
    fn arena_zero_capacity_fails() {
        let err = BatchArena::new(0).unwrap_err();
        assert!(matches!(err, BatchError::ZeroCapacity));
    }

    #[test]
    fn batch_node_is_128_bytes() {
        // 64 (RequestInfo) + 8+8+8+8+8+8 (6 fields) = 112, padded to 128 for alignment.
        assert_eq!(std::mem::size_of::<BatchNode>(), 128);
    }

    #[test]
    fn batch_node_alignment_is_64() {
        assert_eq!(std::mem::align_of::<BatchNode>(), 64);
    }

    #[test]
    fn arena_node_correlation_ids_preserved() {
        let arena = BatchArena::new(16).unwrap();
        for i in 0..16u64 {
            arena.push(make_node(i)).unwrap();
        }
        // Read back via raw slice
        let slice = unsafe { arena.as_slice() };
        for (i, node) in slice.iter().enumerate() {
            assert_eq!(node.correlation_id, i as u64);
        }
    }

    #[test]
    fn batch_node_body_null_returns_empty() {
        let node = make_node(0);
        let body = unsafe { node.body() };
        assert!(body.is_empty());
    }

    #[test]
    fn batch_node_with_body() {
        let data = b"hello world";
        let node = BatchNode::new(
            RequestInfo {
                method: HttpMethod::POST,
                _pad: [0u8; 7],
                handler_id: 2,
                path_offset: 0,
                path_len: 4,
                headers_offset: 0,
                body_offset: 0,
                body_len: data.len() as u64,
                correlation_id: 42,
            },
            data.as_ptr(),
            data.len(),
            std::ptr::null(),
            0,
            42,
        );
        let body = unsafe { node.body() };
        assert_eq!(body, b"hello world");
    }
}
