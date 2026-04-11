//! Dispatcher: routes filled batches to workers.
//!
//! Three dispatch modes:
//! - **Direct**: batch sent via `tokio::sync::mpsc`, responses via oneshot on
//!   each `PendingRequest`. No bridge, no arena, no correlation IDs.
//! - **Bridge**: requests written into the arena-backed `BatchBridge`,
//!   responses polled from the bridge channel. Uses DashMap for correlation.
//! - **SharedMemory**: requests serialized into per-worker MPMC rings,
//!   responses read from tiered response rings. Uses O(1) `CorrelationSlab`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

use vertebra_core::bridge::{BatchBridge, WriteRequest};
use vertebra_core::transport::ring_pair::RingPair;
use vertebra_core::transport::{ShmBatchWriter, Transport, TransportNode};

use crate::collector::{Batch, PendingResponse};
use crate::correlation::CorrelationSlab;

/// How batches are dispatched to workers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchMode {
    /// Direct: batch sent via channel, responses via oneshot (fast path).
    Direct,
    /// Bridge: batch written to arena/slab, responses via bridge channel (data-intensive).
    Bridge,
    /// SharedMemory: batch serialized into per-worker MPMC rings (multi-process).
    SharedMemory,
}

/// A batch with its requests, ready for the direct dispatch worker.
pub struct DirectBatch {
    pub requests: Vec<DirectRequest>,
}

/// A single request in the direct dispatch path — carries the oneshot reply channel.
pub struct DirectRequest {
    pub handler_id: u64,
    pub method_tag: u8,
    pub http_version_tag: u8,
    /// Protocol flags (see `vertebra_core::protocol::flags`).
    pub flags: u32,
    pub path: String,
    pub query: Option<String>,
    pub headers_mp: bytes::Bytes,
    pub body: bytes::Bytes,
    /// Reply channel. `None` for fire-and-forget requests.
    pub reply: Option<oneshot::Sender<PendingResponse>>,
}

/// Per-worker state for SHM dispatch.
struct WorkerState {
    ring_pair: Arc<RingPair>,
    transport: Transport,
}

struct ShmDispatchBatch {
    flags: u32,
    nodes: Vec<TransportNode>,
    correlation_ids: Vec<u64>,
    components: Vec<bytes::Bytes>,
}

impl ShmDispatchBatch {
    fn new(flags: u32) -> Self {
        Self {
            flags,
            nodes: Vec::new(),
            correlation_ids: Vec::new(),
            components: Vec::new(),
        }
    }
}

struct TransportRequestView<'a> {
    path: &'a str,
    query: Option<&'a str>,
    headers_mp: &'a bytes::Bytes,
    body: &'a bytes::Bytes,
}

/// SharedMemory dispatcher with round-robin routing and sharded O(1) correlation.
pub struct ShmDispatcher {
    workers: Vec<WorkerState>,
    next_worker: AtomicUsize,
    correlation_shards: Vec<Mutex<CorrelationSlab>>,
    shard_mask: usize,
}

impl ShmDispatcher {
    /// Create a new SHM dispatcher.
    pub fn new(ring_pairs: Vec<Arc<RingPair>>, max_inflight: usize) -> Arc<Self> {
        let workers: Vec<WorkerState> = ring_pairs
            .into_iter()
            .map(|rp| WorkerState {
                transport: Transport::new_shared_memory(Arc::clone(&rp)),
                ring_pair: rp,
            })
            .collect();

        let shard_count = 8;
        let shard_mask = shard_count - 1;
        let shard_capacity = max_inflight / shard_count;

        let correlation_shards = (0..shard_count)
            .map(|_| Mutex::new(CorrelationSlab::new(shard_capacity)))
            .collect();

        let this = Arc::new(Self {
            workers,
            next_worker: AtomicUsize::new(0),
            correlation_shards,
            shard_mask,
        });

        // Spawn periodic sweep task (1s intervals, 30s timeout)
        let sweep_inner = Arc::clone(&this);
        tokio::spawn(async move {
            info!("SHM correlation sweep task started");
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            let timeout = Duration::from_secs(30);
            loop {
                interval.tick().await;
                sweep_inner.sweep_expired(timeout);
            }
        });

        this
    }

    /// Dispatch a batch to workers via MPMC rings.
    pub fn dispatch_shm(&self, batch: Batch) -> usize {
        use vertebra_core::protocol::flags;

        let worker_count = self.workers.len();
        if worker_count == 0 {
            error!("no SHM workers configured, dropping batch");
            return 0;
        }

        let start = self
            .next_worker
            .fetch_add(batch.requests.len().max(1), Ordering::Relaxed);
        let mut pending_batches: HashMap<(usize, u32), ShmDispatchBatch> = HashMap::new();
        let mut dispatched_count = 0;

        for (offset, req) in batch.requests.into_iter().enumerate() {
            let crate::collector::PendingRequest {
                handler_id,
                method_tag,
                http_version_tag,
                flags: req_flags,
                path,
                query,
                headers_mp,
                body,
                reply,
            } = req;
            let request_view = TransportRequestView {
                path: &path,
                query: query.as_deref(),
                headers_mp: &headers_mp,
                body: &body,
            };
            let is_fnf = req_flags & flags::FIRE_AND_FORGET != 0;
            let mut reply = Some(reply);

            let preferred_worker = (start + offset) % worker_count;
            let worker_idx = if pending_batches.contains_key(&(preferred_worker, req_flags)) {
                Some(preferred_worker)
            } else {
                (0..worker_count)
                    .map(|attempt| (preferred_worker + attempt) % worker_count)
                    .find(|&idx| worker_ring_has_capacity(&self.workers[idx]))
            };

            let Some(worker_idx) = worker_idx else {
                warn!("all worker rings full, responding with 503");
                if !is_fnf {
                    if let Some(reply) = reply.take() {
                        let _ = reply.send(service_unavailable_response());
                    }
                }
                continue;
            };

            let batch_key = (worker_idx, req_flags);
            let max_payload = self.workers[worker_idx]
                .ring_pair
                .request_ring
                .payload_capacity();
            if let Some(pending) = pending_batches.get(&batch_key) {
                if !batch_can_fit_request(pending, &request_view, max_payload) {
                    let flushed = pending_batches
                        .remove(&batch_key)
                        .expect("pending SHM batch should exist");
                    self.flush_shm_dispatch_batch(worker_idx, flushed);
                }
            }

            if batch_snapshot_serialized_size(1, request_slab_size(&request_view)) > max_payload {
                warn!(
                    worker = worker_idx,
                    "request batch too large for SHM slot, responding with 503"
                );
                if !is_fnf {
                    if let Some(reply) = reply.take() {
                        let _ = reply.send(service_unavailable_response());
                    }
                }
                continue;
            }

            let correlation_id = if is_fnf {
                0
            } else {
                let shard_idx = (start + offset) & self.shard_mask;
                let mut slab = self.correlation_shards[shard_idx].lock().unwrap();
                if slab.in_flight() >= slab.capacity() {
                    drop(slab);
                    warn!(shard = shard_idx, "correlation shard full, responding with 503");
                    if let Some(reply) = reply.take() {
                        let _ = reply.send(service_unavailable_response());
                    }
                    continue;
                }
                let internal_id = slab.insert(
                    reply.take().expect("reply must be present"),
                    worker_idx as u32,
                )
                .expect("correlation slot should be available after capacity check");
                
                (internal_id << 8) | (shard_idx as u64)
            };

            let pending = pending_batches
                .entry(batch_key)
                .or_insert_with(|| ShmDispatchBatch::new(req_flags));
            
            pending.nodes.push(TransportNode {
                method_tag,
                http_version_tag,
                _pad: [0; 2],
                handler_id,
                correlation_id,
                path_offset: 0,
                path_len: path.len() as u32,
                query_offset: 0,
                query_len: query.as_ref().map_or(0, |q| q.len() as u32),
                headers_offset: 0,
                headers_len: headers_mp.len() as u32,
                body_offset: 0,
                body_len: body.len() as u32,
            });
            pending.components.push(bytes::Bytes::from(path));
            if let Some(q) = query {
                pending.components.push(bytes::Bytes::from(q));
            }
            pending.components.push(headers_mp);
            pending.components.push(body);
            if !is_fnf {
                pending.correlation_ids.push(correlation_id);
            }
            dispatched_count += 1;
        }

        for ((worker_idx, _flags), pending) in pending_batches {
            self.flush_shm_dispatch_batch(worker_idx, pending);
        }
        
        dispatched_count
    }

    /// Access a specific correlation shard.
    pub fn correlation_shard(&self, shard_idx: usize) -> &Mutex<CorrelationSlab> {
        &self.correlation_shards[shard_idx]
    }

    /// Get the number of correlation shards.
    pub fn shard_count(&self) -> usize {
        self.correlation_shards.len()
    }

    /// Sweep for expired entries across all shards.
    pub fn sweep_expired(&self, timeout: Duration) {
        for shard in &self.correlation_shards {
            let mut slab = shard.lock().unwrap();
            slab.sweep_expired(timeout);
        }
    }

    /// Get all ring pairs (for the response reader).
    pub fn ring_pairs(&self) -> Vec<Arc<RingPair>> {
        self.workers
            .iter()
            .map(|w| Arc::clone(&w.ring_pair))
            .collect()
    }

    fn flush_shm_dispatch_batch(&self, worker_idx: usize, pending: ShmDispatchBatch) {
        let worker = &self.workers[worker_idx];
        let writer = ShmBatchWriter {
            nodes: pending.nodes,
            components: pending.components.iter().map(|c| c.as_ref()).collect(),
            flags: pending.flags,
        };

        // Final optimization: Zero-alloc serialization directly into ring slot.
        match worker.ring_pair.request_ring.try_send_with(pending.flags, |slot| {
            writer.serialize_into(slot)
        }) {
            Ok(()) => {
                // PERF: Lazy Notify
                let stats = worker.ring_pair.request_ring.stats();
                if stats.write_head == stats.read_head.wrapping_add(1) & ((stats.slots as u64) - 1) {
                    let _ = worker.transport.notify_request();
                }
            }
            Err(vertebra_core::channel::ChannelError::Full) => {
                debug!(worker = worker_idx, "request ring full during flush");
                if !pending.correlation_ids.is_empty() {
                    for cid in pending.correlation_ids {
                        let shard_idx = (cid & 0xFF) as usize;
                        let internal_id = cid >> 8;
                        let mut slab = self.correlation_shards[shard_idx].lock().unwrap();
                        slab.complete(internal_id, service_unavailable_response());
                    }
                }
            }
            Err(e) => {
                error!(worker = worker_idx, error = %e, "ring send error");
                if !pending.correlation_ids.is_empty() {
                    for cid in pending.correlation_ids {
                        let shard_idx = (cid & 0xFF) as usize;
                        let internal_id = cid >> 8;
                        let mut slab = self.correlation_shards[shard_idx].lock().unwrap();
                        slab.complete(internal_id, service_unavailable_response());
                    }
                }
            }
        }
    }
}

fn request_slab_size(req: &TransportRequestView<'_>) -> usize {
    req.path.len() + req.query.map_or(0, str::len) + req.headers_mp.len() + req.body.len()
}

fn batch_snapshot_serialized_size(node_count: usize, slab_len: usize) -> usize {
    const BATCH_HEADER_SIZE: usize = 16;
    BATCH_HEADER_SIZE + node_count * std::mem::size_of::<TransportNode>() + slab_len
}

fn batch_can_fit_request(
    pending: &ShmDispatchBatch,
    req: &TransportRequestView<'_>,
    max_payload: usize,
) -> bool {
    let next_nodes = pending.nodes.len() + 1;
    let current_slab_len: usize = pending.components.iter().map(|c| c.len()).sum();
    let next_slab = current_slab_len + request_slab_size(req);
    batch_snapshot_serialized_size(next_nodes, next_slab) <= max_payload
}

fn service_unavailable_response() -> PendingResponse {
    PendingResponse {
        status: 503,
        headers_mp: bytes::Bytes::new(),
        body: bytes::Bytes::from_static(b"Service Unavailable"),
    }
}

fn worker_ring_has_capacity(worker: &WorkerState) -> bool {
    let stats = worker.ring_pair.request_ring.stats();
    let mask = (stats.slots as u64).wrapping_sub(1);
    let next = stats.write_head.wrapping_add(1) & mask;
    next != stats.read_head
}

/// Dispatcher state shared between the write task and the response router.
struct Inner {
    /// In-flight correlation ID -> reply channel (bridge mode only).
    pending: DashMap<u64, oneshot::Sender<PendingResponse>>,
    /// Next monotonic correlation ID (bridge mode only).
    next_id: AtomicU64,
    /// Direct dispatch channel (Some if direct mode).
    direct_tx: Option<mpsc::Sender<DirectBatch>>,
    /// SHM dispatcher (Some if SharedMemory mode).
    shm_dispatcher: Option<Arc<ShmDispatcher>>,
    /// Dispatch mode.
    mode: DispatchMode,
}

/// The dispatcher.
///
/// Manages in-flight request tracking and dispatches batches to workers
/// via the direct channel, arena-backed BatchBridge, or MPMC ring pairs.
pub struct Dispatcher {
    inner: Arc<Inner>,
}

impl Default for Dispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl Dispatcher {
    /// Create a new dispatcher in bridge mode (default).
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                pending: DashMap::new(),
                next_id: AtomicU64::new(1),
                direct_tx: None,
                shm_dispatcher: None,
                mode: DispatchMode::Bridge,
            }),
        }
    }

    /// Create a new dispatcher in direct mode.
    pub fn new_direct(channel_capacity: usize) -> (Self, mpsc::Receiver<DirectBatch>) {
        let (tx, rx) = mpsc::channel(channel_capacity);
        let dispatcher = Self {
            inner: Arc::new(Inner {
                pending: DashMap::new(),
                next_id: AtomicU64::new(1),
                direct_tx: Some(tx),
                shm_dispatcher: None,
                mode: DispatchMode::Direct,
            }),
        };
        (dispatcher, rx)
    }

    /// Create a new dispatcher in SharedMemory mode.
    pub fn new_shm(ring_pairs: Vec<Arc<RingPair>>, max_inflight: usize) -> Self {
        let shm = ShmDispatcher::new(ring_pairs, max_inflight);
        Self {
            inner: Arc::new(Inner {
                pending: DashMap::new(),
                next_id: AtomicU64::new(1),
                direct_tx: None,
                shm_dispatcher: Some(shm),
                mode: DispatchMode::SharedMemory,
            }),
        }
    }

    /// Get the SHM dispatcher (for response reader access).
    pub fn shm_dispatcher(&self) -> Option<&Arc<ShmDispatcher>> {
        self.inner.shm_dispatcher.as_ref()
    }

    /// Dispatch a batch to workers.
    ///
    /// Returns the number of requests successfully dispatched.
    pub fn dispatch(&self, batch: Batch) -> usize {
        match self.inner.mode {
            DispatchMode::Direct => self.dispatch_direct(batch),
            DispatchMode::Bridge => self.dispatch_bridge(batch),
            DispatchMode::SharedMemory => {
                if let Some(shm) = &self.inner.shm_dispatcher {
                    shm.dispatch_shm(batch)
                } else {
                    error!("SHM dispatcher not configured");
                    0
                }
            }
        }
    }

    /// Direct dispatch: send batch through channel, responses flow back via oneshot.
    fn dispatch_direct(&self, batch: Batch) -> usize {
        use vertebra_core::protocol::flags;

        let tx = match &self.inner.direct_tx {
            Some(tx) => tx,
            None => {
                error!("direct dispatch channel not configured");
                return 0;
            }
        };

        let count = batch.requests.len();
        let requests = batch
            .requests
            .into_iter()
            .map(|req| {
                let is_fnf = req.flags & flags::FIRE_AND_FORGET != 0;
                DirectRequest {
                    handler_id: req.handler_id,
                    method_tag: req.method_tag,
                    http_version_tag: req.http_version_tag,
                    flags: req.flags,
                    path: req.path,
                    query: req.query,
                    headers_mp: req.headers_mp,
                    body: req.body,
                    reply: if is_fnf { None } else { Some(req.reply) },
                }
            })
            .collect();

        if tx.try_send(DirectBatch { requests }).is_err() {
            error!("direct dispatch channel full or closed, dropping batch");
            0
        } else {
            count
        }
    }

    /// Bridge dispatch: write into arena/slab, seal, and track correlation IDs.
    fn dispatch_bridge(&self, batch: Batch) -> usize {
        use vertebra_core::protocol::flags;

        let bridge = match BatchBridge::get() {
            Some(b) => b,
            None => {
                error!("batch bridge not initialized, dropping batch");
                return 0;
            }
        };
        let writer = bridge.batch_writer();
        let mut dispatched_count = 0;

        for req in batch.requests {
            let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
            let is_fnf = req.flags & flags::FIRE_AND_FORGET != 0;

            if !is_fnf {
                self.inner.pending.insert(id, req.reply);
            }

            if let Err(e) = writer.push_request(&WriteRequest {
                method_tag: req.method_tag,
                http_version_tag: req.http_version_tag,
                handler_id: req.handler_id,
                correlation_id: id,
                path: req.path.as_bytes(),
                query: req.query.as_ref().map(|q| q.as_bytes()),
                headers_raw: &req.headers_mp,
                body: &req.body,
            }) {
                error!(error = %e, "transport push failed, dropping request");
                if !is_fnf {
                    self.inner.pending.remove(&id);
                }
            } else {
                dispatched_count += 1;
            }
        }

        if let Err(e) = writer.seal() {
            error!(error = %e, "transport seal failed");
        }
        
        dispatched_count
    }

    /// Complete a request with a response (bridge or SHM mode).
    pub fn complete(&self, correlation_id: u64, response: PendingResponse) {
        match self.inner.mode {
            DispatchMode::SharedMemory => {
                if let Some(shm) = &self.inner.shm_dispatcher {
                    let shard_idx = (correlation_id & 0xFF) as usize;
                    let internal_id = correlation_id >> 8;
                    let mut slab = shm.correlation_shard(shard_idx).lock().unwrap();
                    if !slab.complete(internal_id, response) {
                        debug!(
                            id = correlation_id,
                            shard = shard_idx,
                            "no pending SHM request for correlation ID"
                        );
                    }
                }
            }
            _ => {
                if let Some((_, tx)) = self.inner.pending.remove(&correlation_id) {
                    let _ = tx.send(response);
                } else {
                    debug!(id = correlation_id, "no pending request for correlation ID");
                }
            }
        }
    }

    /// Complete multiple requests with a single pass through the tracking state.
    pub fn complete_many(&self, responses: Vec<(u64, PendingResponse)>) {
        match self.inner.mode {
            DispatchMode::SharedMemory => {
                if let Some(shm) = &self.inner.shm_dispatcher {
                    for (correlation_id, response) in responses {
                        let shard_idx = (correlation_id & 0xFF) as usize;
                        let internal_id = correlation_id >> 8;
                        let mut slab = shm.correlation_shard(shard_idx).lock().unwrap();
                        if !slab.complete(internal_id, response) {
                            debug!(
                                id = correlation_id,
                                shard = shard_idx,
                                "no pending SHM request for correlation ID"
                            );
                        }
                    }
                }
            }
            _ => {
                for (correlation_id, response) in responses {
                    if let Some((_, tx)) = self.inner.pending.remove(&correlation_id) {
                        let _ = tx.send(response);
                    } else {
                        debug!(id = correlation_id, "no pending request for correlation ID");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatcher_creates_with_empty_state() {
        let d = Dispatcher::new();
        assert!(d.inner.pending.is_empty());
    }

    #[test]
    fn complete_with_no_pending_is_noop() {
        let d = Dispatcher::new();
        d.complete(
            999,
            PendingResponse {
                status: 200,
                headers_mp: bytes::Bytes::new(),
                body: bytes::Bytes::from_static(b"ok"),
            },
        );
    }

    #[tokio::test]
    async fn direct_mode_sends_batch() {
        let (d, mut rx) = Dispatcher::new_direct(16);
        let (reply_tx, _reply_rx) = oneshot::channel();

        let batch = Batch {
            requests: vec![crate::collector::PendingRequest {
                handler_id: 1,
                method_tag: 0,
                http_version_tag: 1,
                flags: 0,
                path: "/test".to_owned(),
                query: None,
                headers_mp: bytes::Bytes::new(),
                body: bytes::Bytes::new(),
                reply: reply_tx,
            }],
        };

        d.dispatch(batch);

        let direct_batch = rx.recv().await.expect("should receive direct batch");
        assert_eq!(direct_batch.requests.len(), 1);
        assert_eq!(direct_batch.requests[0].path, "/test");
        assert!(direct_batch.requests[0].reply.is_some());
    }

    #[tokio::test]
    async fn direct_mode_fnf_has_no_reply() {
        let (d, mut rx) = Dispatcher::new_direct(16);
        let (reply_tx, _reply_rx) = oneshot::channel();

        let batch = Batch {
            requests: vec![crate::collector::PendingRequest {
                handler_id: 1,
                method_tag: 0,
                http_version_tag: 1,
                flags: vertebra_core::protocol::flags::FIRE_AND_FORGET,
                path: "/fire".to_owned(),
                query: None,
                headers_mp: bytes::Bytes::new(),
                body: bytes::Bytes::new(),
                reply: reply_tx,
            }],
        };

        d.dispatch(batch);

        let direct_batch = rx.recv().await.expect("should receive direct batch");
        assert_eq!(direct_batch.requests.len(), 1);
        assert!(
            direct_batch.requests[0].reply.is_none(),
            "F&F request should have no reply channel"
        );
    }

    #[tokio::test]
    async fn shm_dispatcher_round_robin() {
        use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};

        let pairs: Vec<Arc<RingPair>> = (0..4)
            .map(|_| Arc::new(RingPair::create(RingPairConfig::default()).unwrap()))
            .collect();

        let dispatcher = Dispatcher::new_shm(pairs.clone(), 1024);

        // Dispatch 8 requests — should round-robin across 4 workers
        for i in 0..8u64 {
            let (reply_tx, _reply_rx) = oneshot::channel();
            let batch = Batch {
                requests: vec![crate::collector::PendingRequest {
                    handler_id: i,
                    method_tag: 0,
                    http_version_tag: 1,
                    flags: 0,
                    path: format!("/test/{i}"),
                    query: None,
                    headers_mp: bytes::Bytes::new(),
                    body: bytes::Bytes::new(),
                    reply: reply_tx,
                }],
            };
            dispatcher.dispatch(batch);
        }

        // Each worker ring should have 2 requests
        let mut total = 0;
        for pair in &pairs {
            let mut buf = vec![0u8; pair.request_ring.payload_capacity()];
            let mut count = 0;
            while pair.request_ring.try_recv(&mut buf).is_ok() {
                count += 1;
            }
            total += count;
            assert!(
                (1..=3).contains(&count),
                "worker got {count} requests, expected ~2"
            );
        }
        assert_eq!(total, 8);
    }

    #[tokio::test]
    async fn shm_dispatcher_complete_through_correlation() {
        use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};

        let pair = Arc::new(RingPair::create(RingPairConfig::default()).unwrap());
        let dispatcher = Dispatcher::new_shm(vec![pair.clone()], 1024);

        let (reply_tx, mut reply_rx) = oneshot::channel();
        let batch = Batch {
            requests: vec![crate::collector::PendingRequest {
                handler_id: 1,
                method_tag: 0,
                http_version_tag: 1,
                flags: 0,
                path: "/hello".to_owned(),
                query: None,
                headers_mp: bytes::Bytes::new(),
                body: bytes::Bytes::new(),
                reply: reply_tx,
            }],
        };

        dispatcher.dispatch(batch);

        // Read the request from the ring to get the correlation ID
        let mut buf = vec![0u8; pair.request_ring.payload_capacity()];
        let len = pair.request_ring.try_recv(&mut buf).expect("should recv");
        let snap = vertebra_core::transport::BatchSnapshot::deserialize_from(&buf[..len]).expect("deserialize");
        let correlation_id = snap.node(0).correlation_id;
        assert!(correlation_id > 0);

        // Complete the correlation
        dispatcher.complete(
            correlation_id,
            PendingResponse {
                status: 200,
                headers_mp: bytes::Bytes::new(),
                body: bytes::Bytes::from_static(b"world"),
            },
        );

        // The oneshot should have the response
        let resp = reply_rx.try_recv().expect("should receive response");
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body.as_ref(), b"world");
    }
}
