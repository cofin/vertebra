//! (arena+slab reset after snapshot)

pub mod ring_pair;

use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;

use self::ring_pair::{ResponseData, RingPair, RingPairError};
use crate::channel::{ChannelError, MpmcSlotView, SlotView};

/// Tagged enum for supported transport backends.
pub enum Backend {
    /// In-process MPMC channel (used by embedded workers).
    InProcess(crossbeam_channel::Receiver<Arc<BatchSnapshot>>),
    /// Cross-process SPSC ring pair (used by forked workers).
    SharedMemory(Arc<RingPair>),
}

impl Backend {
    /// Signal the response notifier (SharedMemory only).
    pub fn notify_response(&self) -> Result<(), std::io::Error> {
        match self {
            Backend::InProcess(_) => Ok(()),
            Backend::SharedMemory(ring_pair) => ring_pair.response_notifier.notify(),
        }
    }

    /// Notify the response notifier only when the supervisor consumer is sleeping.
    pub fn notify_response_if_sleeping(&self) -> Result<bool, std::io::Error> {
        match self {
            Backend::InProcess(_) => Ok(false),
            Backend::SharedMemory(ring_pair) => ring_pair.notify_response_if_sleeping(),
        }
    }

    /// Arm Dekker sleep on the tiered response rings (`SharedMemory` only).
    pub fn prepare_response_sleep(&self) -> bool {
        match self {
            Backend::InProcess(_) => false,
            Backend::SharedMemory(ring_pair) => ring_pair.prepare_response_sleep(),
        }
    }

    /// Cancel Dekker sleep on the tiered response rings (`SharedMemory` only).
    pub fn cancel_response_sleep(&self) {
        if let Backend::SharedMemory(ring_pair) = self {
            ring_pair.cancel_response_sleep();
        }
    }

    /// Clear the request notifier (SharedMemory only).
    pub fn clear_request(&self) -> Result<(), std::io::Error> {
        match self {
            Backend::InProcess(_) => Ok(()),
            Backend::SharedMemory(ring_pair) => ring_pair.request_notifier.clear(),
        }
    }

    /// Signal the request notifier (SharedMemory only).
    pub fn notify_request(&self) -> Result<(), std::io::Error> {
        match self {
            Backend::InProcess(_) => Ok(()),
            Backend::SharedMemory(ring_pair) => ring_pair.request_notifier.notify(),
        }
    }

    /// Notify the request notifier only when the worker consumer is sleeping.
    pub fn notify_request_if_sleeping(&self) -> Result<bool, std::io::Error> {
        match self {
            Backend::InProcess(_) => Ok(false),
            Backend::SharedMemory(ring_pair) => ring_pair.notify_request_if_sleeping(),
        }
    }

    /// Arm Dekker sleep on the request ring (`SharedMemory` only).
    pub fn prepare_request_sleep(&self) -> bool {
        match self {
            Backend::InProcess(_) => false,
            Backend::SharedMemory(ring_pair) => ring_pair.prepare_request_sleep(),
        }
    }

    /// Cancel Dekker sleep on the request ring (`SharedMemory` only).
    pub fn cancel_request_sleep(&self) {
        if let Backend::SharedMemory(ring_pair) = self {
            ring_pair.cancel_request_sleep();
        }
    }
}

/// Primary abstraction for request batch delivery.
///
/// Enables Python workers to poll for batches without knowing whether they
/// are running in the same process as the broker or in a separate process.
pub struct Transport {
    backend: Backend,
}

impl Transport {
    /// Access the backend enum (for FD extraction in PyO3 layer).
    pub fn backend(&self) -> &Backend {
        &self.backend
    }

    /// Create a new in-process transport.
    pub fn new_in_process(rx: crossbeam_channel::Receiver<Arc<BatchSnapshot>>) -> Self {
        Self {
            backend: Backend::InProcess(rx),
        }
    }

    /// Create a new shared-memory transport backed by a ring pair.
    pub fn new_shared_memory(ring_pair: Arc<RingPair>) -> Self {
        Self {
            backend: Backend::SharedMemory(ring_pair),
        }
    }

    /// Poll for the next batch snapshot.
    ///
    /// This is the primary entry point for Python worker loops.
    /// Works transparently for both in-process and shared-memory backends.
    pub fn poll_batch(&self, timeout: Duration) -> Option<Arc<BatchSnapshot>> {
        match &self.backend {
            Backend::InProcess(rx) => rx.recv_timeout(timeout).ok(),
            Backend::SharedMemory(ring_pair) => {
                if let Ok(view) = ring_pair.request_ring.try_recv_view() {
                    return BatchSnapshot::from_mpmc_slot(view).ok();
                }
                if timeout.is_zero() {
                    return None;
                }
                for _ in 0..256 {
                    std::hint::spin_loop();
                    if let Ok(view) = ring_pair.request_ring.try_recv_view() {
                        return BatchSnapshot::from_mpmc_slot(view).ok();
                    }
                }

                struct SleepCancelGuard<'a>(&'a RingPair);
                impl Drop for SleepCancelGuard<'_> {
                    fn drop(&mut self) {
                        self.0.cancel_request_sleep();
                    }
                }

                let deadline = std::time::Instant::now() + timeout;
                loop {
                    if let Ok(view) = ring_pair.request_ring.try_recv_view() {
                        return BatchSnapshot::from_mpmc_slot(view).ok();
                    }
                    let now = std::time::Instant::now();
                    if now >= deadline {
                        return None;
                    }
                    let remaining = deadline.saturating_duration_since(now);
                    if ring_pair.prepare_request_sleep() {
                        let _guard = SleepCancelGuard(ring_pair);
                        let wait_ms = (remaining.as_millis() as i32).clamp(1, 5);
                        let notified = ring_pair.request_notifier.wait(wait_ms);
                        drop(_guard);
                        if let Ok(true) = notified {
                            let _ = ring_pair.request_notifier.clear();
                        }
                    }
                }
            }
        }
    }

    /// Poll for the next batch without blocking.
    pub fn poll_batch_async(&self) -> Option<Arc<BatchSnapshot>> {
        match &self.backend {
            Backend::InProcess(rx) => rx.try_recv().ok(),
            Backend::SharedMemory(ring_pair) => ring_pair
                .request_ring
                .try_recv_view()
                .ok()
                .and_then(|view| BatchSnapshot::from_mpmc_slot(view).ok()),
        }
    }

    /// Return `true` when the request transport currently holds no unread batches.
    pub fn is_empty(&self) -> bool {
        match &self.backend {
            Backend::InProcess(rx) => rx.is_empty(),
            Backend::SharedMemory(ring_pair) => !ring_pair.request_ring.has_data(),
        }
    }

    /// Send a response back through the transport.
    ///
    /// Only meaningful for `Backend::SharedMemory` — routes to the tiered
    /// response ring. For `Backend::InProcess`, this is a no-op (responses
    /// flow through oneshot channels in the dispatcher).
    pub fn send_response(
        &self,
        correlation_id: u64,
        status: u16,
        headers_raw: &[u8],
        body: &[u8],
    ) -> Result<(), TransportError> {
        match &self.backend {
            Backend::InProcess(_) => Ok(()), // no-op
            Backend::SharedMemory(ring_pair) => ring_pair
                .response_rings
                .send_response(correlation_id, status, headers_raw, body)
                .map_err(|e| match e {
                    RingPairError::ResponseTooLarge(_) => TransportError::ResponseTooLarge,
                    RingPairError::Channel(ChannelError::Full) => TransportError::RingFull,
                    _ => TransportError::Disconnected,
                }),
        }
    }

    /// Send a response back through the transport, blocking until the selected
    /// response ring has space if it is currently full.
    pub fn send_response_blocking(
        &self,
        correlation_id: u64,
        status: u16,
        headers_raw: &[u8],
        body: &[u8],
    ) -> Result<(), TransportError> {
        match &self.backend {
            Backend::InProcess(_) => Ok(()),
            Backend::SharedMemory(ring_pair) => ring_pair
                .response_rings
                .send_response_blocking(correlation_id, status, headers_raw, body)
                .map_err(|e| match e {
                    RingPairError::ResponseTooLarge(_) => TransportError::ResponseTooLarge,
                    RingPairError::Channel(ChannelError::Full) => TransportError::RingFull,
                    _ => TransportError::Disconnected,
                }),
        }
    }

    /// Send a pre-encoded raw response frame back through the shared-memory transport.
    pub fn send_response_bytes(&self, raw_frame: &[u8]) -> Result<(), TransportError> {
        match &self.backend {
            Backend::InProcess(_) => Ok(()),
            Backend::SharedMemory(ring_pair) => {
                ring_pair
                    .send_response_bytes(raw_frame)
                    .map_err(|e| match e {
                        RingPairError::ResponseTooLarge(_) => TransportError::ResponseTooLarge,
                        RingPairError::Channel(ChannelError::Full) => TransportError::RingFull,
                        _ => TransportError::Disconnected,
                    })
            }
        }
    }

    /// Try to receive a response from the shared-memory transport.
    ///
    /// Returns `None` if no response is available or if using in-process backend.
    pub fn try_recv_response(&self) -> Option<ResponseData> {
        match &self.backend {
            Backend::InProcess(_) => None,
            Backend::SharedMemory(ring_pair) => {
                ring_pair.response_rings.try_recv_response().ok().flatten()
            }
        }
    }

    /// Signal that one or more responses have been written to the rings.
    ///
    /// This should be called by the worker after one or more `send_response`
    /// calls to wake up the supervisor.
    pub fn notify_response(&self) -> Result<(), TransportError> {
        self.backend
            .notify_response()
            .map_err(|_| TransportError::Disconnected)
    }

    /// Signal the response notifier only when the supervisor consumer is sleeping.
    pub fn notify_response_if_sleeping(&self) -> Result<bool, TransportError> {
        self.backend
            .notify_response_if_sleeping()
            .map_err(|_| TransportError::Disconnected)
    }

    /// Arm Dekker sleep on the tiered response rings (`true` when empty).
    pub fn prepare_response_sleep(&self) -> bool {
        self.backend.prepare_response_sleep()
    }

    /// Cancel Dekker sleep on the tiered response rings.
    pub fn cancel_response_sleep(&self) {
        self.backend.cancel_response_sleep();
    }

    /// Clear the request notification signal.
    ///
    /// Should be called after receiving a batch in async mode.
    pub fn clear_request(&self) -> Result<(), TransportError> {
        self.backend
            .clear_request()
            .map_err(|_| TransportError::Disconnected)
    }

    /// Signal that a new batch is available in the request ring.
    pub fn notify_request(&self) -> Result<(), TransportError> {
        self.backend
            .notify_request()
            .map_err(|_| TransportError::Disconnected)
    }

    /// Signal the request notifier only when the worker consumer is sleeping.
    pub fn notify_request_if_sleeping(&self) -> Result<bool, TransportError> {
        self.backend
            .notify_request_if_sleeping()
            .map_err(|_| TransportError::Disconnected)
    }

    /// Arm Dekker sleep on the request ring (`true` when empty).
    pub fn prepare_request_sleep(&self) -> bool {
        self.backend.prepare_request_sleep()
    }

    /// Cancel Dekker sleep on the request ring.
    pub fn cancel_request_sleep(&self) {
        self.backend.cancel_request_sleep();
    }
}

/// Error type for transport operations.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("transport arena full (capacity: {0})")]
    ArenaFull(usize),
    #[error("transport slab full (capacity: {0} bytes)")]
    SlabFull(usize),
    #[error("no requests to seal")]
    EmptyBatch,
    #[error("transport channel disconnected")]
    Disconnected,
    #[error("ring full")]
    RingFull,
    #[error("response too large")]
    ResponseTooLarge,
    #[error("batch deserialization failed: {0}")]
    DeserializeFailed(&'static str),
}

/// Compact request descriptor with slab offsets (no raw pointers).
///
/// All variable-length data (path, query, headers, body) lives in a
/// [`DataSlab`]. This struct holds only u32 offsets and lengths.
#[derive(Debug, Clone, Copy, Default)]
#[repr(C)]
pub struct TransportNode {
    /// HTTP method as a compact byte tag (0=GET, 1=POST, …).
    pub method_tag: u8,
    /// HTTP version tag (0=1.0, 1=1.1, 2=2, 3=3).
    pub http_version_tag: u8,
    pub _pad: [u8; 2],
    /// Handler ID from the native router (FNV-1a hash).
    pub handler_id: u64,
    /// Monotonic correlation ID for request/response matching.
    pub correlation_id: u64,
    /// Offset of path bytes in the data slab.
    pub path_offset: u32,
    /// Length of path bytes.
    pub path_len: u32,
    /// Offset of query string bytes in the data slab.
    pub query_offset: u32,
    /// Length of query string bytes (0 = no query).
    pub query_len: u32,
    /// Offset of encoded header bytes in the data slab.
    pub headers_offset: u32,
    /// Length of encoded header bytes.
    pub headers_len: u32,
    /// Offset of body bytes in the data slab.
    pub body_offset: u32,
    /// Length of body bytes.
    pub body_len: u32,
}

impl TransportNode {
    /// Convert the method tag to its string representation.
    pub fn method_name(&self) -> &'static str {
        match self.method_tag {
            0 => "GET",
            1 => "POST",
            2 => "PUT",
            3 => "DELETE",
            4 => "PATCH",
            5 => "HEAD",
            6 => "OPTIONS",
            7 => "TRACE",
            8 => "CONNECT",
            _ => "UNKNOWN",
        }
    }

    /// Convert the HTTP version tag to its string representation.
    pub fn http_version(&self) -> &'static str {
        match self.http_version_tag {
            0 => "1.0",
            1 => "1.1",
            2 => "2",
            3 => "3",
            _ => "1.1",
        }
    }
}

/// Bump-allocated byte slab for variable-length request data.
///
/// Thread-safety: single writer (HTTP dispatcher), reader only after seal.
/// The atomic offset ensures correct ordering between writer and reader.
pub struct DataSlab {
    data: UnsafeCell<Vec<u8>>,
    capacity: usize,
    offset: AtomicUsize,
}

// SAFETY: Single writer (dispatcher), reader only after batch is sealed.
// The atomic offset provides the necessary synchronization.
unsafe impl Send for DataSlab {}
unsafe impl Sync for DataSlab {}

impl DataSlab {
    /// Create a new data slab with the given capacity in bytes.
    pub fn new(capacity: usize) -> Self {
        Self {
            data: UnsafeCell::new(vec![0u8; capacity]),
            capacity,
            offset: AtomicUsize::new(0),
        }
    }

    /// Write bytes into the slab. Returns `(offset, len)` on success.
    ///
    /// Empty writes return `(0, 0)` without consuming slab space.
    pub fn write(&self, bytes: &[u8]) -> Result<(u32, u32), TransportError> {
        if bytes.is_empty() {
            return Ok((0, 0));
        }
        let len = bytes.len();
        let off = self.offset.fetch_add(len, Ordering::AcqRel);
        if off + len > self.capacity {
            self.offset.fetch_sub(len, Ordering::AcqRel);
            return Err(TransportError::SlabFull(self.capacity));
        }
        // SAFETY: offset is unique per write (atomic fetch_add), single writer.
        unsafe {
            let data = &mut *self.data.get();
            data[off..off + len].copy_from_slice(bytes);
        }
        Ok((off as u32, len as u32))
    }

    /// Snapshot the used portion of the slab into an owned `Vec`.
    pub fn snapshot(&self) -> Vec<u8> {
        let used = self.offset.load(Ordering::Acquire);
        self.snapshot_used(used)
    }

    /// Snapshot exactly `used` bytes into an owned `Vec`.
    pub fn snapshot_used(&self, used: usize) -> Vec<u8> {
        // SAFETY: no concurrent writes after seal_batch().
        let data = unsafe { &*self.data.get() };
        data[..used].to_vec()
    }

    /// Reset the slab for reuse.
    pub fn reset(&self) {
        self.offset.store(0, Ordering::Release);
    }

    /// Roll back the slab offset to a previous position after a partial write failure.
    pub fn truncate_to(&self, used: usize) {
        self.offset.store(used, Ordering::Release);
    }

    /// Current usage in bytes.
    pub fn used(&self) -> usize {
        self.offset.load(Ordering::Acquire)
    }

    /// Take the internal buffer and replace it with `replacement`.
    ///
    /// Returns the previous buffer (truncated to `used` bytes).
    /// Resets the offset to 0. The caller must hold the batch write lock.
    ///
    /// This is O(1) — a pointer swap, not a copy.
    pub fn take_and_replace(&self, replacement: Vec<u8>) -> Vec<u8> {
        let used = self.offset.load(Ordering::Acquire);
        // SAFETY: caller holds exclusive write lock; no concurrent readers
        // until the next seal.
        let data = unsafe { &mut *self.data.get() };
        let mut taken = std::mem::replace(data, replacement);
        taken.truncate(used);
        self.offset.store(0, Ordering::Release);
        taken
    }
}

/// Fixed-capacity arena for [`TransportNode`] entries.
///
/// Thread-safety: single writer (dispatcher), reader only after seal.
pub struct TransportArena {
    nodes: UnsafeCell<Vec<TransportNode>>,
    capacity: usize,
    len: AtomicU32,
}

// SAFETY: Single writer (dispatcher), reader only after batch is sealed.
unsafe impl Send for TransportArena {}
unsafe impl Sync for TransportArena {}

impl TransportArena {
    /// Create a new arena with the given node capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            nodes: UnsafeCell::new(vec![TransportNode::default(); capacity]),
            capacity,
            len: AtomicU32::new(0),
        }
    }

    /// Push a node into the arena. Returns the node index.
    pub fn push(&self, node: TransportNode) -> Result<u32, TransportError> {
        let idx = self.len.fetch_add(1, Ordering::AcqRel);
        if idx as usize >= self.capacity {
            self.len.fetch_sub(1, Ordering::AcqRel);
            return Err(TransportError::ArenaFull(self.capacity));
        }
        // SAFETY: idx is unique (atomic fetch_add), single writer.
        unsafe {
            let nodes = &mut *self.nodes.get();
            nodes[idx as usize] = node;
        }
        Ok(idx)
    }

    /// Snapshot all written nodes into an owned `Vec`.
    pub fn snapshot(&self) -> Vec<TransportNode> {
        let len = self.len.load(Ordering::Acquire) as usize;
        self.snapshot_len(len)
    }

    /// Snapshot exactly `len` nodes into an owned `Vec`.
    pub fn snapshot_len(&self, len: usize) -> Vec<TransportNode> {
        let nodes = unsafe { &*self.nodes.get() };
        nodes[..len].to_vec()
    }

    /// Reset the arena for reuse.
    pub fn reset(&self) {
        self.len.store(0, Ordering::Release);
    }

    /// Current node count.
    pub fn len(&self) -> u32 {
        self.len.load(Ordering::Acquire)
    }

    /// Returns `true` if no nodes have been pushed.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Take the internal node buffer and replace it with `replacement`.
    ///
    /// Returns the previous buffer (truncated to `len` nodes).
    /// Resets the length to 0. The caller must hold the batch write lock.
    ///
    /// This is O(1) — a pointer swap, not a copy.
    pub fn take_and_replace(&self, replacement: Vec<TransportNode>) -> Vec<TransportNode> {
        let len = self.len.load(Ordering::Acquire) as usize;
        // SAFETY: caller holds exclusive write lock; no concurrent readers
        // until the next seal.
        let nodes = unsafe { &mut *self.nodes.get() };
        let mut taken = std::mem::replace(nodes, replacement);
        taken.truncate(len);
        self.len.store(0, Ordering::Release);
        taken
    }
}

/// Pre-allocated pool of buffer pairs for zero-copy batch sealing.
///
/// The pool holds `capacity` pairs of `(Vec<u8>, Vec<TransportNode>)`.
/// On seal, the bridge takes a fresh pair from the pool and swaps it
/// into the arena/slab (O(1) pointer swap). When the `BatchSnapshot`
/// is dropped, the buffers return to the pool for reuse.
pub struct BufferPool {
    pairs: std::sync::Mutex<Vec<(Vec<u8>, Vec<TransportNode>)>>,
    slab_capacity: usize,
    arena_capacity: usize,
}

impl BufferPool {
    /// Create a pool with `count` pre-allocated buffer pairs.
    pub fn new(count: usize, slab_capacity: usize, arena_capacity: usize) -> Self {
        let mut pairs = Vec::with_capacity(count);
        for _ in 0..count {
            pairs.push((
                vec![0u8; slab_capacity],
                vec![TransportNode::default(); arena_capacity],
            ));
        }
        Self {
            pairs: std::sync::Mutex::new(pairs),
            slab_capacity,
            arena_capacity,
        }
    }

    /// Take a buffer pair from the pool, or allocate a fresh one if empty.
    pub fn take(&self) -> (Vec<u8>, Vec<TransportNode>) {
        self.pairs.lock().unwrap().pop().unwrap_or_else(|| {
            (
                vec![0u8; self.slab_capacity],
                vec![TransportNode::default(); self.arena_capacity],
            )
        })
    }

    /// Return a buffer pair to the pool for reuse.
    ///
    /// Buffers are restored to full length without zeroing — the slab is
    /// write-before-read (request data is serialized into it via
    /// `copy_from_slice`), so stale bytes beyond the write offset are
    /// never read.  Skipping the zeroing saves ~136KB per batch cycle.
    pub fn return_pair(&self, mut slab: Vec<u8>, mut nodes: Vec<TransportNode>) {
        // SAFETY: Both Vecs were originally allocated at full capacity and
        // filled with initialized bytes. `take_and_replace` may have
        // truncated them, but the backing memory up to `capacity()` is
        // still initialized (truncate doesn't deallocate or uninitialize).
        // `set_len` restores the length so `DataSlab::write()` can index
        // into the full range.
        unsafe {
            slab.set_len(self.slab_capacity.min(slab.capacity()));
        }
        // TransportNode is Copy + has no Drop, so restoring length is safe.
        unsafe {
            nodes.set_len(self.arena_capacity.min(nodes.capacity()));
        }
        self.pairs.lock().unwrap().push((slab, nodes));
    }
}

impl std::fmt::Debug for BufferPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let count = self.pairs.lock().map(|p| p.len()).unwrap_or(0);
        f.debug_struct("BufferPool")
            .field("available", &count)
            .field("slab_capacity", &self.slab_capacity)
            .field("arena_capacity", &self.arena_capacity)
            .finish()
    }
}

/// Zero-copy in-process request payload backed directly by [`bytes::Bytes`].
///
/// Used by embedded topologies to pass HTTP request components from Hyper into
/// Python workers without copying through a fixed [`DataSlab`] or acquiring a
/// batch writer mutex.
#[derive(Debug, Clone)]
pub struct InProcessRequest {
    pub node: TransportNode,
    pub path: bytes::Bytes,
    pub query: bytes::Bytes,
    pub headers_raw: bytes::Bytes,
    pub body: bytes::Bytes,
    pub path_params_raw: bytes::Bytes,
}

impl InProcessRequest {
    /// Construct a zero-copy in-process request entry.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        method_tag: u8,
        http_version_tag: u8,
        handler_id: u64,
        correlation_id: u64,
        path: bytes::Bytes,
        query: bytes::Bytes,
        headers_raw: bytes::Bytes,
        body: bytes::Bytes,
        path_params_raw: bytes::Bytes,
    ) -> Self {
        Self {
            node: TransportNode {
                method_tag,
                http_version_tag,
                _pad: [0; 2],
                handler_id,
                correlation_id,
                path_offset: 0,
                path_len: path.len().min(u32::MAX as usize) as u32,
                query_offset: 0,
                query_len: query.len().min(u32::MAX as usize) as u32,
                headers_offset: 0,
                headers_len: headers_raw.len().min(u32::MAX as usize) as u32,
                body_offset: 0,
                body_len: body.len().min(u32::MAX as usize) as u32,
            },
            path,
            query,
            headers_raw,
            body,
            path_params_raw,
        }
    }
}

enum ShmSlotHolder {
    Mpmc(MpmcSlotView),
    Spsc(SlotView),
    #[cfg(unix)]
    Overflow {
        region: crate::shm::ShmRegion,
        len: usize,
    },
}

impl ShmSlotHolder {
    #[inline]
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Mpmc(view) => view.as_slice(),
            Self::Spsc(view) => view.as_slice(),
            #[cfg(unix)]
            Self::Overflow { region, len } => unsafe { &region.as_slice()[..*len] },
        }
    }
}

enum SnapshotBacking {
    Slab {
        slab: Vec<u8>,
        nodes: Vec<TransportNode>,
        pool: Option<std::sync::Arc<BufferPool>>,
        #[cfg(feature = "compression")]
        decompressed_slab: std::sync::OnceLock<Vec<u8>>,
    },
    InProcess {
        requests: Vec<InProcessRequest>,
    },
    ShmSlot {
        slot: ShmSlotHolder,
        node_count: usize,
        slab_start: usize,
        slab_len: usize,
        #[cfg(feature = "compression")]
        decompressed_slab: std::sync::OnceLock<Vec<u8>>,
    },
}

/// Immutable batch snapshot for zero-object request proxies.
///
/// Created by `poll_batch()` and shared via `Arc` across all
/// `Request` instances in a batch. Fields are read lazily from either a
/// contiguous slab, zero-copy [`InProcessRequest`] `Bytes` slices, or an
/// active shared-memory ring slot view.
///
/// When pool-backed, dropping the snapshot returns the buffers to the
/// pool for reuse (zero allocation on steady-state). When SHM-slot-backed,
/// dropping the snapshot releases the ring slot back to the shared-memory ring.
pub struct BatchSnapshot {
    backing: SnapshotBacking,
    /// Protocol flags for this batch (carries compression codec info).
    flags: u32,
}

impl std::fmt::Debug for BatchSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.backing {
            SnapshotBacking::Slab {
                slab, nodes, pool, ..
            } => f
                .debug_struct("BatchSnapshot")
                .field("kind", &"slab")
                .field("slab_len", &slab.len())
                .field("nodes_len", &nodes.len())
                .field("pooled", &pool.is_some())
                .field("flags", &self.flags)
                .finish(),
            SnapshotBacking::InProcess { requests } => f
                .debug_struct("BatchSnapshot")
                .field("kind", &"inprocess")
                .field("nodes_len", &requests.len())
                .field("flags", &self.flags)
                .finish(),
            SnapshotBacking::ShmSlot {
                node_count,
                slab_len,
                ..
            } => f
                .debug_struct("BatchSnapshot")
                .field("kind", &"shm_slot")
                .field("slab_len", slab_len)
                .field("nodes_len", node_count)
                .field("flags", &self.flags)
                .finish(),
        }
    }
}

impl Drop for BatchSnapshot {
    fn drop(&mut self) {
        if let SnapshotBacking::Slab {
            slab, nodes, pool, ..
        } = &mut self.backing
            && let Some(pool) = pool.take()
        {
            let slab = std::mem::take(slab);
            let nodes = std::mem::take(nodes);
            pool.return_pair(slab, nodes);
        }
    }
}

impl BatchSnapshot {
    /// Create a new snapshot from slab data and transport nodes (no pool).
    pub fn new(slab: Vec<u8>, nodes: Vec<TransportNode>) -> Self {
        Self::new_with_flags(slab, nodes, 0)
    }

    /// Create a new snapshot with protocol flags (no pool).
    pub fn new_with_flags(slab: Vec<u8>, nodes: Vec<TransportNode>, flags: u32) -> Self {
        Self {
            backing: SnapshotBacking::Slab {
                slab,
                nodes,
                pool: None,
                #[cfg(feature = "compression")]
                decompressed_slab: std::sync::OnceLock::new(),
            },
            flags,
        }
    }

    /// Create a pool-backed snapshot. Buffers return to the pool on drop.
    pub fn new_pooled(
        slab: Vec<u8>,
        nodes: Vec<TransportNode>,
        pool: std::sync::Arc<BufferPool>,
    ) -> Self {
        Self::new_pooled_with_flags(slab, nodes, pool, 0)
    }

    /// Create a pool-backed snapshot with protocol flags.
    pub fn new_pooled_with_flags(
        slab: Vec<u8>,
        nodes: Vec<TransportNode>,
        pool: std::sync::Arc<BufferPool>,
        flags: u32,
    ) -> Self {
        Self {
            backing: SnapshotBacking::Slab {
                slab,
                nodes,
                pool: Some(pool),
                #[cfg(feature = "compression")]
                decompressed_slab: std::sync::OnceLock::new(),
            },
            flags,
        }
    }

    /// Create a zero-copy snapshot directly from in-process `Bytes`-backed requests.
    pub fn from_inprocess(requests: Vec<InProcessRequest>) -> Self {
        Self::from_inprocess_with_flags(requests, 0)
    }

    /// Create a zero-copy in-process snapshot with protocol flags.
    pub fn from_inprocess_with_flags(requests: Vec<InProcessRequest>, flags: u32) -> Self {
        Self {
            backing: SnapshotBacking::InProcess { requests },
            flags,
        }
    }

    /// Create a zero-copy snapshot backed directly by an [`MpmcSlotView`].
    pub fn from_mpmc_slot(view: MpmcSlotView) -> Result<Arc<Self>, TransportError> {
        Self::from_shm_slot_holder(ShmSlotHolder::Mpmc(view))
    }

    /// Create a zero-copy snapshot backed directly by an SPSC [`SlotView`].
    pub fn from_spsc_slot(view: SlotView) -> Result<Arc<Self>, TransportError> {
        Self::from_shm_slot_holder(ShmSlotHolder::Spsc(view))
    }

    fn from_shm_slot_holder(slot: ShmSlotHolder) -> Result<Arc<Self>, TransportError> {
        let data = slot.as_slice();
        if data.len() < BATCH_HEADER_SIZE {
            return Err(TransportError::DeserializeFailed("too short"));
        }

        let flags = u32::from_le_bytes(data[0..4].try_into().unwrap());
        #[cfg(unix)]
        if flags == REQUEST_OVERFLOW_MAGIC {
            let total_len = u64::from_le_bytes(data[4..12].try_into().unwrap()) as usize;
            let name_len = u32::from_le_bytes(data[12..16].try_into().unwrap()) as usize;
            if (5..=128).contains(&name_len)
                && data.len() == 16 + name_len
                && (data[16..16 + name_len].starts_with(b"/vtb-")
                    || data[16..16 + name_len].starts_with(b"/tmp/vtb-"))
            {
                let name = std::str::from_utf8(&data[16..16 + name_len])
                    .map_err(|_| TransportError::DeserializeFailed("invalid overflow region name"))?
                    .to_owned();
                drop(slot);
                let region = crate::shm::ShmRegion::open_named(&name, total_len).map_err(|_| {
                    TransportError::DeserializeFailed("failed to open overflow region")
                })?;
                let _ = crate::shm::shm_unlink_named(&name);
                return Self::from_shm_slot_holder(ShmSlotHolder::Overflow {
                    region,
                    len: total_len,
                });
            }
        }

        let node_count = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
        let slab_len = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;

        let node_size = core::mem::size_of::<TransportNode>();
        let node_bytes = node_count
            .checked_mul(node_size)
            .ok_or(TransportError::DeserializeFailed("node_count overflow"))?;
        let slab_start = BATCH_HEADER_SIZE
            .checked_add(node_bytes)
            .ok_or(TransportError::DeserializeFailed("length overflow"))?;
        let expected = slab_start
            .checked_add(slab_len)
            .ok_or(TransportError::DeserializeFailed("length overflow"))?;
        if data.len() < expected {
            return Err(TransportError::DeserializeFailed("truncated"));
        }

        let nodes_ptr = data[BATCH_HEADER_SIZE..].as_ptr();
        if !(nodes_ptr as usize).is_multiple_of(core::mem::align_of::<TransportNode>()) {
            return Self::deserialize_from(data);
        }

        // SAFETY: alignment and bounds (`BATCH_HEADER_SIZE + node_bytes <= data.len()`) were
        // verified above, and `TransportNode` is `#[repr(C)]` with primitive integer fields only.
        let nodes =
            unsafe { core::slice::from_raw_parts(nodes_ptr.cast::<TransportNode>(), node_count) };
        Self::validate_nodes(nodes, slab_len)?;

        Ok(Arc::new(Self {
            backing: SnapshotBacking::ShmSlot {
                slot,
                node_count,
                slab_start,
                slab_len,
                #[cfg(feature = "compression")]
                decompressed_slab: std::sync::OnceLock::new(),
            },
            flags,
        }))
    }

    fn validate_nodes(nodes: &[TransportNode], slab_len: usize) -> Result<(), TransportError> {
        for node in nodes {
            let path_offset = node.path_offset as usize;
            let path_len = node.path_len as usize;
            let path_end = path_offset
                .checked_add(path_len)
                .ok_or(TransportError::DeserializeFailed("path offset overflow"))?;
            if path_end > slab_len {
                return Err(TransportError::DeserializeFailed("path out of bounds"));
            }

            let query_offset = node.query_offset as usize;
            let query_len = node.query_len as usize;
            let query_end = query_offset
                .checked_add(query_len)
                .ok_or(TransportError::DeserializeFailed("query offset overflow"))?;
            if query_end > slab_len {
                return Err(TransportError::DeserializeFailed("query out of bounds"));
            }

            let headers_offset = node.headers_offset as usize;
            let headers_len = node.headers_len as usize;
            let headers_end = headers_offset
                .checked_add(headers_len)
                .ok_or(TransportError::DeserializeFailed("headers offset overflow"))?;
            if headers_end > slab_len {
                return Err(TransportError::DeserializeFailed("headers out of bounds"));
            }

            let body_offset = node.body_offset as usize;
            let body_len = node.body_len as usize;
            let body_end = body_offset
                .checked_add(body_len)
                .ok_or(TransportError::DeserializeFailed("body offset overflow"))?;
            if body_end > slab_len {
                return Err(TransportError::DeserializeFailed("body out of bounds"));
            }
        }
        Ok(())
    }

    #[inline]
    fn shm_nodes(slot: &ShmSlotHolder, node_count: usize) -> &[TransportNode] {
        let data = slot.as_slice();
        // SAFETY: `from_shm_slot_holder` verified `BATCH_HEADER_SIZE + node_count * size_of::<TransportNode>() <= data.len()`
        // and `align_of::<TransportNode>()` alignment; `slot` keeps the SHM mapping and slot lease alive.
        unsafe {
            core::slice::from_raw_parts(
                data[BATCH_HEADER_SIZE..].as_ptr().cast::<TransportNode>(),
                node_count,
            )
        }
    }

    /// Number of requests in this batch.
    pub fn len(&self) -> usize {
        match &self.backing {
            SnapshotBacking::Slab { nodes, .. } => nodes.len(),
            SnapshotBacking::InProcess { requests } => requests.len(),
            SnapshotBacking::ShmSlot { node_count, .. } => *node_count,
        }
    }

    /// Returns `true` if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Protocol flags for this batch.
    pub fn flags(&self) -> u32 {
        self.flags
    }

    /// Get the effective slab bytes for slab-backed snapshots, decompressing lazily if needed.
    fn slab_bytes<'a>(
        &'a self,
        slab: &'a [u8],
        #[cfg(feature = "compression")] decompressed_slab: &'a std::sync::OnceLock<Vec<u8>>,
    ) -> &'a [u8] {
        #[cfg(feature = "compression")]
        {
            use crate::protocol::codec::CompressionCodec;
            use crate::protocol::flags;

            if self.flags & flags::COMPRESSED != 0 {
                return decompressed_slab.get_or_init(|| {
                    let codec = CompressionCodec::from_flags(self.flags);
                    crate::protocol::codec::decompress(slab, codec)
                        .expect("batch slab decompression should not fail")
                });
            }
        }
        slab
    }

    /// Get the transport node at the given index.
    pub fn node(&self, index: usize) -> &TransportNode {
        match &self.backing {
            SnapshotBacking::Slab { nodes, .. } => &nodes[index],
            SnapshotBacking::InProcess { requests } => &requests[index].node,
            SnapshotBacking::ShmSlot {
                slot, node_count, ..
            } => &Self::shm_nodes(slot, *node_count)[index],
        }
    }

    /// Read path bytes for the request at `index`.
    pub fn path_bytes(&self, index: usize) -> &[u8] {
        match &self.backing {
            SnapshotBacking::Slab {
                slab,
                nodes,
                #[cfg(feature = "compression")]
                decompressed_slab,
                ..
            } => {
                let n = &nodes[index];
                if n.path_len == 0 {
                    return &[];
                }
                #[cfg(feature = "compression")]
                let s = self.slab_bytes(slab, decompressed_slab);
                #[cfg(not(feature = "compression"))]
                let s = self.slab_bytes(slab);
                &s[n.path_offset as usize..(n.path_offset + n.path_len) as usize]
            }
            SnapshotBacking::InProcess { requests } => &requests[index].path,
            SnapshotBacking::ShmSlot {
                slot,
                node_count,
                slab_start,
                slab_len,
                #[cfg(feature = "compression")]
                decompressed_slab,
            } => {
                let n = &Self::shm_nodes(slot, *node_count)[index];
                let off = n.path_offset as usize;
                let len = n.path_len as usize;
                if len == 0 {
                    return &[];
                }
                let raw_slab = &slot.as_slice()[*slab_start..*slab_start + *slab_len];
                #[cfg(feature = "compression")]
                let s = self.slab_bytes(raw_slab, decompressed_slab);
                #[cfg(not(feature = "compression"))]
                let s = self.slab_bytes(raw_slab);
                s.get(off..off.saturating_add(len)).unwrap_or(&[])
            }
        }
    }

    /// Read path as a UTF-8 string.
    pub fn path(&self, index: usize) -> &str {
        std::str::from_utf8(self.path_bytes(index)).unwrap_or("")
    }

    /// Read query bytes (None if no query string).
    pub fn query_bytes(&self, index: usize) -> Option<&[u8]> {
        match &self.backing {
            SnapshotBacking::Slab {
                slab,
                nodes,
                #[cfg(feature = "compression")]
                decompressed_slab,
                ..
            } => {
                let n = &nodes[index];
                if n.query_len == 0 {
                    return None;
                }
                #[cfg(feature = "compression")]
                let s = self.slab_bytes(slab, decompressed_slab);
                #[cfg(not(feature = "compression"))]
                let s = self.slab_bytes(slab);
                Some(&s[n.query_offset as usize..(n.query_offset + n.query_len) as usize])
            }
            SnapshotBacking::InProcess { requests } => {
                let q = &requests[index].query;
                if q.is_empty() { None } else { Some(q) }
            }
            SnapshotBacking::ShmSlot {
                slot,
                node_count,
                slab_start,
                slab_len,
                #[cfg(feature = "compression")]
                decompressed_slab,
            } => {
                let n = &Self::shm_nodes(slot, *node_count)[index];
                let off = n.query_offset as usize;
                let len = n.query_len as usize;
                if len == 0 {
                    return None;
                }
                let raw_slab = &slot.as_slice()[*slab_start..*slab_start + *slab_len];
                #[cfg(feature = "compression")]
                let s = self.slab_bytes(raw_slab, decompressed_slab);
                #[cfg(not(feature = "compression"))]
                let s = self.slab_bytes(raw_slab);
                s.get(off..off.saturating_add(len))
            }
        }
    }

    /// Read query as a UTF-8 string.
    pub fn query(&self, index: usize) -> Option<&str> {
        self.query_bytes(index)
            .map(|b| std::str::from_utf8(b).unwrap_or(""))
    }

    /// Read raw encoded header bytes.
    pub fn headers_raw(&self, index: usize) -> &[u8] {
        match &self.backing {
            SnapshotBacking::Slab {
                slab,
                nodes,
                #[cfg(feature = "compression")]
                decompressed_slab,
                ..
            } => {
                let n = &nodes[index];
                if n.headers_len == 0 {
                    return &[];
                }
                #[cfg(feature = "compression")]
                let s = self.slab_bytes(slab, decompressed_slab);
                #[cfg(not(feature = "compression"))]
                let s = self.slab_bytes(slab);
                &s[n.headers_offset as usize..(n.headers_offset + n.headers_len) as usize]
            }
            SnapshotBacking::InProcess { requests } => &requests[index].headers_raw,
            SnapshotBacking::ShmSlot {
                slot,
                node_count,
                slab_start,
                slab_len,
                #[cfg(feature = "compression")]
                decompressed_slab,
            } => {
                let n = &Self::shm_nodes(slot, *node_count)[index];
                let off = n.headers_offset as usize;
                let len = n.headers_len as usize;
                if len == 0 {
                    return &[];
                }
                let raw_slab = &slot.as_slice()[*slab_start..*slab_start + *slab_len];
                #[cfg(feature = "compression")]
                let s = self.slab_bytes(raw_slab, decompressed_slab);
                #[cfg(not(feature = "compression"))]
                let s = self.slab_bytes(raw_slab);
                s.get(off..off.saturating_add(len)).unwrap_or(&[])
            }
        }
    }

    /// Read body bytes.
    pub fn body(&self, index: usize) -> &[u8] {
        match &self.backing {
            SnapshotBacking::Slab {
                slab,
                nodes,
                #[cfg(feature = "compression")]
                decompressed_slab,
                ..
            } => {
                let n = &nodes[index];
                if n.body_len == 0 {
                    return &[];
                }
                #[cfg(feature = "compression")]
                let s = self.slab_bytes(slab, decompressed_slab);
                #[cfg(not(feature = "compression"))]
                let s = self.slab_bytes(slab);
                &s[n.body_offset as usize..(n.body_offset + n.body_len) as usize]
            }
            SnapshotBacking::InProcess { requests } => &requests[index].body,
            SnapshotBacking::ShmSlot {
                slot,
                node_count,
                slab_start,
                slab_len,
                #[cfg(feature = "compression")]
                decompressed_slab,
            } => {
                let n = &Self::shm_nodes(slot, *node_count)[index];
                let off = n.body_offset as usize;
                let len = n.body_len as usize;
                if len == 0 {
                    return &[];
                }
                let raw_slab = &slot.as_slice()[*slab_start..*slab_start + *slab_len];
                #[cfg(feature = "compression")]
                let s = self.slab_bytes(raw_slab, decompressed_slab);
                #[cfg(not(feature = "compression"))]
                let s = self.slab_bytes(raw_slab);
                s.get(off..off.saturating_add(len)).unwrap_or(&[])
            }
        }
    }

    /// Read body as an owned [`bytes::Bytes`] buffer (`O(1)` clone for in-process snapshots).
    pub fn body_bytes_owned(&self, index: usize) -> bytes::Bytes {
        match &self.backing {
            SnapshotBacking::InProcess { requests } => requests[index].body.clone(),
            SnapshotBacking::Slab { .. } | SnapshotBacking::ShmSlot { .. } => {
                bytes::Bytes::copy_from_slice(self.body(index))
            }
        }
    }

    /// Read raw encoded path parameter bytes (`empty` when unpopulated).
    pub fn path_params_raw(&self, index: usize) -> &[u8] {
        match &self.backing {
            SnapshotBacking::Slab { .. } | SnapshotBacking::ShmSlot { .. } => &[],
            SnapshotBacking::InProcess { requests } => &requests[index].path_params_raw,
        }
    }

    /// Compute the serialized size for ring slot allocation.
    ///
    /// Wire format: `[flags:u32][node_count:u32][slab_len:u32][pad:u32][nodes...][slab...]`
    pub fn serialized_size(&self) -> usize {
        match &self.backing {
            SnapshotBacking::Slab { slab, nodes, .. } => {
                let node_bytes = nodes.len() * core::mem::size_of::<TransportNode>();
                BATCH_HEADER_SIZE + node_bytes + slab.len()
            }
            SnapshotBacking::InProcess { requests } => {
                let node_bytes = requests.len() * core::mem::size_of::<TransportNode>();
                let slab_len: usize = requests
                    .iter()
                    .map(|r| r.path.len() + r.query.len() + r.headers_raw.len() + r.body.len())
                    .sum();
                BATCH_HEADER_SIZE + node_bytes + slab_len
            }
            SnapshotBacking::ShmSlot {
                slab_start,
                slab_len,
                ..
            } => *slab_start + *slab_len,
        }
    }

    /// Serialize this batch snapshot into a ring slot buffer.
    ///
    /// Returns the number of bytes written. The caller must ensure `slot`
    /// is at least `serialized_size()` bytes.
    ///
    /// # Panics
    /// Panics if the slot is too small.
    pub fn serialize_into(&self, slot: &mut [u8]) -> usize {
        let size = self.serialized_size();
        assert!(
            slot.len() >= size,
            "slot too small: {} < {}",
            slot.len(),
            size
        );

        match &self.backing {
            SnapshotBacking::Slab { slab, nodes, .. } => {
                let mut offset = 0;
                slot[offset..offset + 4].copy_from_slice(&self.flags.to_le_bytes());
                offset += 4;
                slot[offset..offset + 4].copy_from_slice(&(nodes.len() as u32).to_le_bytes());
                offset += 4;
                slot[offset..offset + 4].copy_from_slice(&(slab.len() as u32).to_le_bytes());
                offset += 4;
                slot[offset..offset + 4].copy_from_slice(&0u32.to_le_bytes());
                offset += 4;

                let node_bytes = nodes.len() * core::mem::size_of::<TransportNode>();
                if node_bytes > 0 {
                    // SAFETY: TransportNode is #[repr(C)], Copy, and contains only primitive types.
                    let src = nodes.as_ptr() as *const u8;
                    unsafe {
                        core::ptr::copy_nonoverlapping(
                            src,
                            slot[offset..].as_mut_ptr(),
                            node_bytes,
                        );
                    }
                    offset += node_bytes;
                }

                slot[offset..offset + slab.len()].copy_from_slice(slab);
                offset + slab.len()
            }
            SnapshotBacking::InProcess { requests } => {
                let mut writer = ShmBatchWriter::new(self.flags);
                writer.nodes.reserve(requests.len());
                writer.components.reserve(requests.len() * 4);
                for req in requests {
                    let mut node = req.node;
                    node.path_len = req.path.len() as u32;
                    node.query_len = req.query.len() as u32;
                    node.headers_len = req.headers_raw.len() as u32;
                    node.body_len = req.body.len() as u32;
                    writer.nodes.push(node);
                    writer.components.push(&req.path);
                    if !req.query.is_empty() {
                        writer.components.push(&req.query);
                    }
                    writer.components.push(&req.headers_raw);
                    writer.components.push(&req.body);
                }
                writer.serialize_into(slot)
            }
            SnapshotBacking::ShmSlot { slot: shm_slot, .. } => {
                slot[..size].copy_from_slice(&shm_slot.as_slice()[..size]);
                size
            }
        }
    }

    /// Deserialize a batch snapshot from ring slot data.
    ///
    /// Copies nodes and slab from the slot into owned `Vec`s.
    pub fn deserialize_from(data: &[u8]) -> Result<Arc<Self>, TransportError> {
        if data.len() < BATCH_HEADER_SIZE {
            return Err(TransportError::DeserializeFailed("too short"));
        }

        let flags = u32::from_le_bytes(data[0..4].try_into().unwrap());
        #[cfg(unix)]
        if flags == REQUEST_OVERFLOW_MAGIC {
            let total_len = u64::from_le_bytes(data[4..12].try_into().unwrap()) as usize;
            let name_len = u32::from_le_bytes(data[12..16].try_into().unwrap()) as usize;
            if (5..=128).contains(&name_len)
                && data.len() == 16 + name_len
                && (data[16..16 + name_len].starts_with(b"/vtb-")
                    || data[16..16 + name_len].starts_with(b"/tmp/vtb-"))
            {
                let name = std::str::from_utf8(&data[16..16 + name_len]).map_err(|_| {
                    TransportError::DeserializeFailed("invalid overflow region name")
                })?;
                let region = crate::shm::ShmRegion::open_named(name, total_len).map_err(|_| {
                    TransportError::DeserializeFailed("failed to open overflow region")
                })?;
                let _ = crate::shm::shm_unlink_named(name);
                return Self::deserialize_from(unsafe { &region.as_slice()[..total_len] });
            }
        }

        let node_count = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
        let slab_len = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
        // data[12..16] is padding

        let node_size = core::mem::size_of::<TransportNode>();
        let node_bytes = node_count
            .checked_mul(node_size)
            .ok_or(TransportError::DeserializeFailed("node_count overflow"))?;
        let expected = BATCH_HEADER_SIZE
            .checked_add(node_bytes)
            .and_then(|sum| sum.checked_add(slab_len))
            .ok_or(TransportError::DeserializeFailed("length overflow"))?;
        if data.len() < expected {
            return Err(TransportError::DeserializeFailed("truncated"));
        }

        // Read nodes via memcpy
        let mut nodes = vec![TransportNode::default(); node_count];
        if node_count > 0 {
            // SAFETY: TransportNode is #[repr(C)], all-primitive fields.
            // Source data was written by serialize_into with the same layout.
            let dst = nodes.as_mut_ptr() as *mut u8;
            unsafe {
                core::ptr::copy_nonoverlapping(data[BATCH_HEADER_SIZE..].as_ptr(), dst, node_bytes);
            }
        }

        Self::validate_nodes(&nodes, slab_len)?;

        // Read slab
        let slab_start = BATCH_HEADER_SIZE + node_bytes;
        let slab = data[slab_start..slab_start + slab_len].to_vec();

        Ok(Arc::new(Self::new_with_flags(slab, nodes, flags)))
    }
}

/// Fixed header size for batch snapshot wire format.
const BATCH_HEADER_SIZE: usize = 16; // flags(4) + node_count(4) + slab_len(4) + pad(4)
/// Magic marker (`"VQOV"`) for out-of-line SHM request batch overflow envelopes.
pub const REQUEST_OVERFLOW_MAGIC: u32 = 0x56_51_4F_56;

/// Helper for packing multiple request components into a single contiguous slab.
pub struct ShmBatchWriter<'a> {
    pub nodes: Vec<TransportNode>,
    pub components: Vec<&'a [u8]>,
    pub flags: u32,
}

impl<'a> ShmBatchWriter<'a> {
    pub fn new(flags: u32) -> Self {
        Self {
            nodes: Vec::new(),
            components: Vec::new(),
            flags,
        }
    }

    pub fn serialized_size(&self) -> usize {
        let slab_len: usize = self.components.iter().map(|c| c.len()).sum();
        const BATCH_HEADER_SIZE: usize = 16;
        BATCH_HEADER_SIZE + self.nodes.len() * std::mem::size_of::<TransportNode>() + slab_len
    }

    pub fn serialize_into(mut self, out: &mut [u8]) -> usize {
        let node_count = self.nodes.len() as u32;
        let slab_len: usize = self.components.iter().map(|c| c.len()).sum();

        let mut offset = 0;
        out[offset..offset + 4].copy_from_slice(&self.flags.to_le_bytes());
        offset += 4;
        out[offset..offset + 4].copy_from_slice(&node_count.to_le_bytes());
        offset += 4;
        out[offset..offset + 4].copy_from_slice(&(slab_len as u32).to_le_bytes());
        offset += 4;
        out[offset..offset + 4].fill(0); // padding
        offset += 4;

        // Pack nodes and resolve offsets
        let mut slab_offset = 0;
        for node in &mut self.nodes {
            node.path_offset = slab_offset as u32;
            slab_offset += node.path_len as usize;
            if node.query_len > 0 {
                node.query_offset = slab_offset as u32;
                slab_offset += node.query_len as usize;
            }
            node.headers_offset = slab_offset as u32;
            slab_offset += node.headers_len as usize;
            node.body_offset = slab_offset as u32;
            slab_offset += node.body_len as usize;
        }

        let nodes_bytes = unsafe {
            core::slice::from_raw_parts(
                self.nodes.as_ptr().cast::<u8>(),
                self.nodes.len() * std::mem::size_of::<TransportNode>(),
            )
        };
        out[offset..offset + nodes_bytes.len()].copy_from_slice(nodes_bytes);
        offset += nodes_bytes.len();

        for comp in self.components {
            out[offset..offset + comp.len()].copy_from_slice(comp);
            offset += comp.len();
        }
        offset
    }

    /// Write an oversized batch into a named overflow SHM region and return `(region_name, envelope)`.
    #[cfg(unix)]
    pub fn write_overflow_envelope(self) -> Result<(String, Vec<u8>), crate::shm::ShmError> {
        let total_len = self.serialized_size();
        let (name, mut region) = crate::shm::ShmRegion::create_overflow_region("rq", total_len)?;
        let written = self.serialize_into(unsafe { &mut region.as_mut_slice()[..total_len] });
        debug_assert_eq!(written, total_len);
        let name_bytes = name.as_bytes();
        let mut envelope = Vec::with_capacity(16 + name_bytes.len());
        envelope.extend_from_slice(&REQUEST_OVERFLOW_MAGIC.to_le_bytes());
        envelope.extend_from_slice(&(total_len as u64).to_le_bytes());
        envelope.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        envelope.extend_from_slice(name_bytes);
        Ok((name, envelope))
    }
}

impl BatchSnapshot {
    /// Write an oversized snapshot into a named overflow SHM region and return `(region_name, envelope)`.
    #[cfg(unix)]
    pub fn write_overflow_envelope(&self) -> Result<(String, Vec<u8>), crate::shm::ShmError> {
        let total_len = self.serialized_size();
        let (name, mut region) = crate::shm::ShmRegion::create_overflow_region("rq", total_len)?;
        let written = self.serialize_into(unsafe { &mut region.as_mut_slice()[..total_len] });
        debug_assert_eq!(written, total_len);
        let name_bytes = name.as_bytes();
        let mut envelope = Vec::with_capacity(16 + name_bytes.len());
        envelope.extend_from_slice(&REQUEST_OVERFLOW_MAGIC.to_le_bytes());
        envelope.extend_from_slice(&(total_len as u64).to_le_bytes());
        envelope.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        envelope.extend_from_slice(name_bytes);
        Ok((name, envelope))
    }
}

/// Zero-allocation iterator over raw encoded headers.
///
/// Format per header: `[name_len: u8][name: bytes][value_len: u16 LE][value: bytes]`
pub fn iter_raw_headers(data: &[u8]) -> impl Iterator<Item = (&[u8], &[u8])> + '_ {
    let mut pos = 0;
    std::iter::from_fn(move || {
        if pos >= data.len() {
            return None;
        }
        let name_len = data[pos] as usize;
        pos += 1;
        if pos + name_len > data.len() {
            pos = data.len();
            return None;
        }
        let name = &data[pos..pos + name_len];
        pos += name_len;

        if pos + 2 > data.len() {
            pos = data.len();
            return None;
        }
        let value_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        if pos + value_len > data.len() {
            pos = data.len();
            return None;
        }
        let value = &data[pos..pos + value_len];
        pos += value_len;

        Some((name, value))
    })
}

/// Encode headers into the compact binary format.
///
/// Format per header: `[name_len: u8][name: bytes][value_len: u16 LE][value: bytes]`
pub fn encode_headers_compact(headers: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    if headers.is_empty() {
        return Vec::new();
    }
    let estimated_size: usize = headers.iter().map(|(k, v)| 3 + k.len() + v.len()).sum();
    let mut buf = Vec::with_capacity(estimated_size);
    for (name, value) in headers {
        let name_len = name.len().min(u8::MAX as usize);
        let value_len = value.len().min(u16::MAX as usize);
        buf.push(name_len as u8);
        buf.extend_from_slice(&name[..name_len]);
        buf.extend_from_slice(&(value_len as u16).to_le_bytes());
        buf.extend_from_slice(&value[..value_len]);
    }
    buf
}

/// Decode headers from the compact binary format.
///
/// Format per header: `[name_len: u8][name: bytes][value_len: u16 LE][value: bytes]`
pub fn decode_headers_raw(data: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    iter_raw_headers(data)
        .map(|(k, v)| (k.to_vec(), v.to_vec()))
        .collect()
}

// Compile-time layout assertions. If TransportNode's size or alignment changes,
// the pure-Python ring_reader.py parser will break — these catch drift at build time.
const _: () = {
    assert!(std::mem::size_of::<TransportNode>() == 56);
    assert!(std::mem::align_of::<TransportNode>() == 8);
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_node_layout_matches_ring_reader() {
        // Must match ring_reader.py _NODE_SIZE and _NODE_FMT
        assert_eq!(std::mem::size_of::<TransportNode>(), 56);
        assert_eq!(std::mem::align_of::<TransportNode>(), 8);

        // Verify field offsets via a zeroed node with known values
        let node = TransportNode {
            method_tag: 1,
            http_version_tag: 2,
            _pad: [0; 2],
            handler_id: 0x_DEAD_BEEF_CAFE_BABE,
            correlation_id: 0x_1234_5678_9ABC_DEF0,
            path_offset: 100,
            path_len: 200,
            query_offset: 300,
            query_len: 400,
            headers_offset: 500,
            headers_len: 600,
            body_offset: 700,
            body_len: 800,
        };

        let bytes: [u8; 56] = unsafe { std::mem::transmute::<TransportNode, [u8; 56]>(node) };

        // method_tag at offset 0
        assert_eq!(bytes[0], 1);
        // http_version_tag at offset 1
        assert_eq!(bytes[1], 2);
        // handler_id at offset 8 (LE)
        assert_eq!(
            u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            0x_DEAD_BEEF_CAFE_BABE
        );
        // correlation_id at offset 16
        assert_eq!(
            u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            0x_1234_5678_9ABC_DEF0
        );
        // path_offset at offset 24
        assert_eq!(u32::from_le_bytes(bytes[24..28].try_into().unwrap()), 100);
    }

    // --- DataSlab tests ---

    #[test]
    fn slab_write_and_snapshot() {
        let slab = DataSlab::new(1024);
        let (off, len) = slab.write(b"hello").unwrap();
        assert_eq!(off, 0);
        assert_eq!(len, 5);

        let snap = slab.snapshot();
        assert_eq!(&snap, b"hello");
    }

    #[test]
    fn slab_write_multiple() {
        let slab = DataSlab::new(1024);
        let (off1, _) = slab.write(b"hello").unwrap();
        let (off2, _) = slab.write(b"world").unwrap();
        assert_eq!(off1, 0);
        assert_eq!(off2, 5);

        let snap = slab.snapshot();
        assert_eq!(&snap, b"helloworld");
    }

    #[test]
    fn slab_full_returns_error() {
        let slab = DataSlab::new(4);
        slab.write(b"abc").unwrap();
        let err = slab.write(b"de").unwrap_err();
        assert!(matches!(err, TransportError::SlabFull(4)));
    }

    #[test]
    fn slab_reset_allows_reuse() {
        let slab = DataSlab::new(8);
        slab.write(b"12345678").unwrap();
        slab.reset();
        slab.write(b"abcd").unwrap();
        let snap = slab.snapshot();
        assert_eq!(&snap, b"abcd");
    }

    #[test]
    fn slab_empty_write_returns_zero() {
        let slab = DataSlab::new(8);
        let (off, len) = slab.write(b"").unwrap();
        assert_eq!(off, 0);
        assert_eq!(len, 0);
        assert_eq!(slab.used(), 0);
    }

    // --- TransportArena tests ---

    #[test]
    fn arena_push_and_snapshot() {
        let arena = TransportArena::new(4);
        let node = TransportNode {
            handler_id: 42,
            correlation_id: 1,
            path_len: 5,
            ..Default::default()
        };
        let idx = arena.push(node).unwrap();
        assert_eq!(idx, 0);
        assert_eq!(arena.len(), 1);

        let snap = arena.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].handler_id, 42);
    }

    #[test]
    fn arena_full_returns_error() {
        let arena = TransportArena::new(2);
        arena.push(TransportNode::default()).unwrap();
        arena.push(TransportNode::default()).unwrap();
        let err = arena.push(TransportNode::default()).unwrap_err();
        assert!(matches!(err, TransportError::ArenaFull(2)));
    }

    #[test]
    fn arena_reset_allows_reuse() {
        let arena = TransportArena::new(2);
        arena.push(TransportNode::default()).unwrap();
        arena.push(TransportNode::default()).unwrap();
        arena.reset();
        assert!(arena.is_empty());
        arena
            .push(TransportNode {
                handler_id: 99,
                ..Default::default()
            })
            .unwrap();
        let snap = arena.snapshot();
        assert_eq!(snap[0].handler_id, 99);
    }

    // --- TransportNode tests ---

    #[test]
    fn transport_node_method_names() {
        for (tag, name) in [
            (0, "GET"),
            (1, "POST"),
            (2, "PUT"),
            (3, "DELETE"),
            (4, "PATCH"),
            (5, "HEAD"),
            (6, "OPTIONS"),
            (7, "TRACE"),
            (8, "CONNECT"),
            (255, "UNKNOWN"),
        ] {
            let node = TransportNode {
                method_tag: tag,
                ..Default::default()
            };
            assert_eq!(node.method_name(), name);
        }
    }

    #[test]
    fn transport_node_http_versions() {
        for (tag, version) in [(0, "1.0"), (1, "1.1"), (2, "2"), (3, "3"), (255, "1.1")] {
            let node = TransportNode {
                http_version_tag: tag,
                ..Default::default()
            };
            assert_eq!(node.http_version(), version);
        }
    }

    // --- BatchSnapshot tests ---

    #[test]
    fn snapshot_accessors() {
        let mut slab = Vec::new();
        slab.extend_from_slice(b"/hello"); // 0..6
        slab.extend_from_slice(b"q=1"); // 6..9
        slab.extend_from_slice(b"raw-headers"); // 9..20
        slab.extend_from_slice(b"body data"); // 20..29

        let nodes = vec![TransportNode {
            method_tag: 0,
            http_version_tag: 1,
            _pad: [0; 2],
            handler_id: 42,
            correlation_id: 1,
            path_offset: 0,
            path_len: 6,
            query_offset: 6,
            query_len: 3,
            headers_offset: 9,
            headers_len: 11,
            body_offset: 20,
            body_len: 9,
        }];

        let snapshot = BatchSnapshot::new(slab, nodes);
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot.path(0), "/hello");
        assert_eq!(snapshot.query(0), Some("q=1"));
        assert_eq!(snapshot.headers_raw(0), b"raw-headers");
        assert_eq!(snapshot.body(0), b"body data");
        assert_eq!(snapshot.node(0).handler_id, 42);
        assert_eq!(snapshot.node(0).method_name(), "GET");
    }

    #[test]
    fn snapshot_empty_fields() {
        let nodes = vec![TransportNode::default()];
        let snapshot = BatchSnapshot::new(Vec::new(), nodes);
        assert_eq!(snapshot.path(0), "");
        assert_eq!(snapshot.query(0), None);
        assert_eq!(snapshot.headers_raw(0), &[] as &[u8]);
        assert_eq!(snapshot.body(0), &[] as &[u8]);
    }

    #[test]
    fn snapshot_multiple_nodes() {
        let mut slab = Vec::new();
        slab.extend_from_slice(b"/a"); // 0..2
        slab.extend_from_slice(b"/b"); // 2..4

        let nodes = vec![
            TransportNode {
                correlation_id: 1,
                path_offset: 0,
                path_len: 2,
                ..Default::default()
            },
            TransportNode {
                correlation_id: 2,
                path_offset: 2,
                path_len: 2,
                ..Default::default()
            },
        ];

        let snapshot = BatchSnapshot::new(slab, nodes);
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot.path(0), "/a");
        assert_eq!(snapshot.path(1), "/b");
    }

    // --- decode_headers_raw tests ---

    #[test]
    fn decode_empty_headers() {
        assert!(decode_headers_raw(&[]).is_empty());
    }

    #[test]
    fn decode_single_header() {
        let mut buf = Vec::new();
        let name = b"host";
        let value = b"localhost";
        buf.push(name.len() as u8);
        buf.extend_from_slice(name);
        buf.extend_from_slice(&(value.len() as u16).to_le_bytes());
        buf.extend_from_slice(value);

        let headers = decode_headers_raw(&buf);
        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0], (b"host".to_vec(), b"localhost".to_vec()));
    }

    #[test]
    fn decode_multiple_headers() {
        let mut buf = Vec::new();
        for (name, value) in &[("host", "example.com"), ("content-type", "text/plain")] {
            buf.push(name.len() as u8);
            buf.extend_from_slice(name.as_bytes());
            buf.extend_from_slice(&(value.len() as u16).to_le_bytes());
            buf.extend_from_slice(value.as_bytes());
        }

        let headers = decode_headers_raw(&buf);
        assert_eq!(headers.len(), 2);
        assert_eq!(headers[0], (b"host".to_vec(), b"example.com".to_vec()));
        assert_eq!(
            headers[1],
            (b"content-type".to_vec(), b"text/plain".to_vec())
        );
    }

    #[test]
    fn decode_truncated_header_is_safe() {
        // Name length says 10 but only 3 bytes available
        let buf = vec![10u8, b'a', b'b', b'c'];
        let headers = decode_headers_raw(&buf);
        assert!(headers.is_empty());
    }

    #[test]
    fn batch_snapshot_serialize_empty() {
        let snap = BatchSnapshot::new(vec![], vec![]);
        assert_eq!(snap.serialized_size(), BATCH_HEADER_SIZE);

        let mut buf = vec![0u8; snap.serialized_size()];
        let written = snap.serialize_into(&mut buf);
        assert_eq!(written, BATCH_HEADER_SIZE);

        let deserialized = BatchSnapshot::deserialize_from(&buf).expect("deserialize");
        assert_eq!(deserialized.len(), 0);
        assert!(deserialized.is_empty());
    }

    #[test]
    fn batch_snapshot_serialize_single_node() {
        let slab = b"GET /hello HTTP/1.1\r\n".to_vec();
        let node = TransportNode {
            method_tag: 0,
            http_version_tag: 1,
            _pad: [0; 2],
            handler_id: 42,
            correlation_id: 99,
            path_offset: 4,
            path_len: 6,
            query_offset: 0,
            query_len: 0,
            headers_offset: 0,
            headers_len: 0,
            body_offset: 0,
            body_len: 0,
        };

        let snap = BatchSnapshot::new(slab.clone(), vec![node]);
        let size = snap.serialized_size();
        assert_eq!(size, BATCH_HEADER_SIZE + 56 + slab.len());

        let mut buf = vec![0u8; size];
        snap.serialize_into(&mut buf);

        let deserialized = BatchSnapshot::deserialize_from(&buf).expect("deserialize");
        assert_eq!(deserialized.len(), 1);
        assert_eq!(deserialized.node(0).handler_id, 42);
        assert_eq!(deserialized.node(0).correlation_id, 99);
        assert_eq!(deserialized.node(0).method_tag, 0);
        assert_eq!(deserialized.node(0).path_offset, 4);
        assert_eq!(deserialized.node(0).path_len, 6);
        assert_eq!(deserialized.path(0), "/hello");
    }

    #[test]
    fn batch_snapshot_serialize_with_flags() {
        let snap = BatchSnapshot::new_with_flags(vec![0; 10], vec![], 0x42);
        let mut buf = vec![0u8; snap.serialized_size()];
        snap.serialize_into(&mut buf);

        let deserialized = BatchSnapshot::deserialize_from(&buf).expect("deserialize");
        assert_eq!(deserialized.flags(), 0x42);
    }

    #[test]
    fn batch_snapshot_serialize_multi_node() {
        let slab = vec![0u8; 256];
        let nodes: Vec<TransportNode> = (0..64)
            .map(|i| TransportNode {
                method_tag: 0,
                http_version_tag: 1,
                _pad: [0; 2],
                handler_id: i as u64,
                correlation_id: i as u64 + 1000,
                path_offset: 0,
                path_len: 0,
                query_offset: 0,
                query_len: 0,
                headers_offset: 0,
                headers_len: 0,
                body_offset: 0,
                body_len: 0,
            })
            .collect();

        let snap = BatchSnapshot::new(slab, nodes);
        assert_eq!(snap.len(), 64);

        let mut buf = vec![0u8; snap.serialized_size()];
        snap.serialize_into(&mut buf);

        let deserialized = BatchSnapshot::deserialize_from(&buf).expect("deserialize");
        assert_eq!(deserialized.len(), 64);
        for i in 0..64 {
            assert_eq!(deserialized.node(i).handler_id, i as u64);
            assert_eq!(deserialized.node(i).correlation_id, i as u64 + 1000);
        }
    }

    #[test]
    fn batch_snapshot_deserialize_too_short() {
        let buf = vec![0u8; 8]; // less than BATCH_HEADER_SIZE
        assert!(BatchSnapshot::deserialize_from(&buf).is_err());
    }

    #[test]
    fn batch_snapshot_deserialize_truncated() {
        // Valid header claiming 10 nodes, but no node data
        let mut buf = vec![0u8; BATCH_HEADER_SIZE];
        buf[4..8].copy_from_slice(&10u32.to_le_bytes()); // node_count = 10
        assert!(BatchSnapshot::deserialize_from(&buf).is_err());
    }

    #[test]
    fn batch_snapshot_rejects_out_of_range_node_offsets() {
        let node = TransportNode {
            body_offset: 100,
            body_len: 50, // 100 + 50 = 150 > slab_len (10)
            ..Default::default()
        };
        let slab = vec![0u8; 10];
        let snap = BatchSnapshot::new(slab, vec![node]);
        let mut buf = vec![0u8; snap.serialized_size()];
        snap.serialize_into(&mut buf);

        let err =
            BatchSnapshot::deserialize_from(&buf).expect_err("should reject out of range offset");
        assert!(matches!(err, TransportError::DeserializeFailed(_)));
    }

    #[test]
    fn backend_parity_same_data() {
        use crate::transport::ring_pair::RingPairConfig;
        // Create a batch snapshot
        let slab = b"/api/users".to_vec();
        let node = TransportNode {
            method_tag: 0,
            http_version_tag: 1,
            _pad: [0; 2],
            handler_id: 7,
            correlation_id: 100,
            path_offset: 0,
            path_len: 10,
            query_offset: 0,
            query_len: 0,
            headers_offset: 0,
            headers_len: 0,
            body_offset: 0,
            body_len: 0,
        };
        let snap = Arc::new(BatchSnapshot::new(slab, vec![node]));

        // InProcess path
        let (tx, rx) = crossbeam_channel::bounded(4);
        let transport_in = Transport::new_in_process(rx);
        tx.send(Arc::clone(&snap)).unwrap();
        let result_in = transport_in
            .poll_batch(Duration::from_millis(100))
            .expect("inprocess should receive");
        assert_eq!(result_in.len(), 1);
        assert_eq!(result_in.path(0), "/api/users");
        assert_eq!(result_in.node(0).handler_id, 7);

        // SharedMemory path
        let pair = crate::transport::ring_pair::RingPair::create(RingPairConfig::default())
            .expect("create pair");
        // Serialize into request ring
        let mut slot_buf = vec![0u8; snap.serialized_size()];
        snap.serialize_into(&mut slot_buf);
        pair.request_ring.try_send(&slot_buf, 0).expect("send");

        let transport_shm = Transport::new_shared_memory(Arc::new(pair));
        let result_shm = transport_shm
            .poll_batch(Duration::from_millis(100))
            .expect("shm should receive");

        // Verify parity
        assert_eq!(result_shm.len(), result_in.len());
        assert_eq!(result_shm.path(0), result_in.path(0));
        assert_eq!(result_shm.node(0).handler_id, result_in.node(0).handler_id);
        assert_eq!(
            result_shm.node(0).correlation_id,
            result_in.node(0).correlation_id
        );
    }

    #[test]
    fn batch_snapshot_stress_10k_round_trips() {
        use crate::transport::ring_pair::{RingPair, RingPairConfig};

        let pair = RingPair::create(RingPairConfig::default()).expect("create");

        for i in 0..10_000u64 {
            let slab = format!("/path/{i}").into_bytes();
            let path_len = slab.len() as u32;
            let node = TransportNode {
                method_tag: (i % 9) as u8,
                http_version_tag: 1,
                _pad: [0; 2],
                handler_id: i,
                correlation_id: i + 1_000_000,
                path_offset: 0,
                path_len,
                query_offset: 0,
                query_len: 0,
                headers_offset: 0,
                headers_len: 0,
                body_offset: 0,
                body_len: 0,
            };

            let snap = BatchSnapshot::new(slab, vec![node]);
            let mut buf = vec![0u8; snap.serialized_size()];
            snap.serialize_into(&mut buf);

            pair.request_ring.try_send(&buf, 0).expect("send");
            let mut recv_buf = vec![0u8; pair.request_ring.payload_capacity()];
            let len = pair.request_ring.try_recv(&mut recv_buf).expect("recv");

            let deserialized =
                BatchSnapshot::deserialize_from(&recv_buf[..len]).expect("deserialize");
            assert_eq!(deserialized.node(0).handler_id, i);
            assert_eq!(deserialized.node(0).correlation_id, i + 1_000_000);
            assert_eq!(deserialized.path(0), format!("/path/{i}"));
        }
    }
}
