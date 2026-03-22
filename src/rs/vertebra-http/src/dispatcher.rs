//! Dispatcher: routes filled batches to workers.
//!
//! Three dispatch modes:
//! - **Direct**: batch sent via `tokio::sync::mpsc`, responses via oneshot on
//!   each `PendingRequest`. No bridge, no arena, no correlation IDs.
//! - **Bridge**: requests written into the arena-backed `BatchBridge`,
//!   responses polled from the bridge channel. Uses DashMap for correlation.
//! - **SharedMemory**: requests serialized into per-worker SPSC rings,
//!   responses read from tiered response rings. Uses O(1) `CorrelationSlab`.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, warn};

use vertebra_core::bridge::{BatchBridge, WriteRequest};
use vertebra_core::transport::ring_pair::RingPair;
use vertebra_core::transport::{BatchSnapshot, TransportNode};

use crate::collector::{Batch, PendingResponse};
use crate::correlation::CorrelationSlab;

/// How batches are dispatched to workers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchMode {
    /// Direct: batch sent via channel, responses via oneshot (fast path).
    Direct,
    /// Bridge: batch written to arena/slab, responses via bridge channel (data-intensive).
    Bridge,
    /// SharedMemory: batch serialized into per-worker SPSC rings (multi-process).
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
    /// Shadow tail index — locally cached to avoid cross-core cache invalidation.
    /// Only refreshed when the ring appears full.
    shadow_tail: AtomicU64,
}

/// SharedMemory dispatcher with round-robin routing and O(1) correlation.
pub struct ShmDispatcher {
    workers: Vec<WorkerState>,
    next_worker: AtomicUsize,
    correlation: Mutex<CorrelationSlab>,
}

impl ShmDispatcher {
    /// Create a new SHM dispatcher.
    pub fn new(ring_pairs: Vec<Arc<RingPair>>, max_inflight: usize) -> Self {
        let workers: Vec<WorkerState> = ring_pairs
            .into_iter()
            .map(|rp| WorkerState {
                ring_pair: rp,
                shadow_tail: AtomicU64::new(0),
            })
            .collect();

        Self {
            workers,
            next_worker: AtomicUsize::new(0),
            correlation: Mutex::new(CorrelationSlab::new(max_inflight)),
        }
    }

    /// Dispatch a batch to workers via SPSC rings.
    ///
    /// Each request in the batch is:
    /// 1. Given a correlation ID from the slab
    /// 2. Serialized as a single-request BatchSnapshot
    /// 3. Written to the next available worker's request ring (round-robin)
    /// 4. Worker notified via eventfd
    pub fn dispatch_shm(&self, batch: Batch) {
        use vertebra_core::protocol::flags;

        let worker_count = self.workers.len();
        if worker_count == 0 {
            error!("no SHM workers configured, dropping batch");
            return;
        }

        for req in batch.requests {
            let is_fnf = req.flags & flags::FIRE_AND_FORGET != 0;

            // Build the transport node first (borrows req fields)
            // We need to extract reply before moving req fields
            let reply = req.reply;
            let req_flags = req.flags;

            // Build a single-request BatchSnapshot
            let (slab_data, mut node) = build_transport_node_from_parts(
                req.handler_id,
                req.method_tag,
                req.http_version_tag,
                &req.path,
                req.query.as_deref(),
                &req.headers_mp,
                &req.body,
                0, // correlation_id set below
            );

            // Allocate correlation ID (unless F&F)
            let correlation_id = if is_fnf {
                0
            } else {
                let worker_idx = self.next_worker.load(Ordering::Relaxed) % worker_count;
                let mut slab = self.correlation.lock().unwrap();
                match slab.insert(reply, worker_idx as u32) {
                    Some(id) => id,
                    None => {
                        error!("correlation slab full, dropping request");
                        continue;
                    }
                }
            };

            node.correlation_id = correlation_id;

            let snap = BatchSnapshot::new_with_flags(slab_data, vec![node], req_flags);
            let mut buf = vec![0u8; snap.serialized_size()];
            snap.serialize_into(&mut buf);

            // Round-robin worker selection with shadow-index backpressure
            let start = self.next_worker.fetch_add(1, Ordering::Relaxed);
            let mut dispatched = false;

            for attempt in 0..worker_count {
                let idx = (start + attempt) % worker_count;
                let worker = &self.workers[idx];

                match worker.ring_pair.request_ring.try_send(&buf, 0) {
                    Ok(()) => {
                        // Notify worker
                        let _ = worker.ring_pair.request_notifier.notify();
                        dispatched = true;
                        break;
                    }
                    Err(vertebra_core::channel::ChannelError::Full) => {
                        // Ring full — try next worker
                        debug!(worker = idx, "request ring full, trying next worker");
                        continue;
                    }
                    Err(e) => {
                        error!(worker = idx, error = %e, "ring send error");
                        continue;
                    }
                }
            }

            if !dispatched {
                warn!("all worker rings full, responding with 503");
                if !is_fnf {
                    let mut slab = self.correlation.lock().unwrap();
                    slab.complete(
                        correlation_id,
                        PendingResponse {
                            status: 503,
                            headers_mp: bytes::Bytes::new(),
                            body: bytes::Bytes::from_static(b"Service Unavailable"),
                        },
                    );
                }
            }
        }
    }

    /// Access the correlation slab (for response reader to complete entries).
    pub fn correlation(&self) -> &Mutex<CorrelationSlab> {
        &self.correlation
    }

    /// Get all ring pairs (for the response reader).
    pub fn ring_pairs(&self) -> Vec<Arc<RingPair>> {
        self.workers
            .iter()
            .map(|w| Arc::clone(&w.ring_pair))
            .collect()
    }
}

/// Build a TransportNode + slab data from request parts.
fn build_transport_node_from_parts(
    handler_id: u64,
    method_tag: u8,
    http_version_tag: u8,
    path: &str,
    query: Option<&str>,
    headers_mp: &[u8],
    body: &[u8],
    correlation_id: u64,
) -> (Vec<u8>, TransportNode) {
    let path_bytes = path.as_bytes();
    let query_bytes = query.map(|q| q.as_bytes());
    let headers_bytes = headers_mp;
    let body_bytes = body;

    // Build slab: path + query + headers + body
    let mut slab = Vec::with_capacity(
        path_bytes.len()
            + query_bytes.map_or(0, |q| q.len())
            + headers_bytes.len()
            + body_bytes.len(),
    );

    let path_offset = slab.len() as u32;
    slab.extend_from_slice(path_bytes);
    let path_len = path_bytes.len() as u32;

    let query_offset = slab.len() as u32;
    let query_len = query_bytes.map_or(0, |q| {
        slab.extend_from_slice(q);
        q.len()
    }) as u32;

    let headers_offset = slab.len() as u32;
    slab.extend_from_slice(headers_bytes);
    let headers_len = headers_bytes.len() as u32;

    let body_offset = slab.len() as u32;
    slab.extend_from_slice(body_bytes);
    let body_len = body_bytes.len() as u32;

    let node = TransportNode {
        method_tag,
        http_version_tag,
        _pad: [0; 2],
        handler_id,
        correlation_id,
        path_offset,
        path_len,
        query_offset,
        query_len,
        headers_offset,
        headers_len,
        body_offset,
        body_len,
    };

    (slab, node)
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
/// via the direct channel, arena-backed BatchBridge, or SPSC ring pairs.
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
        let shm = Arc::new(ShmDispatcher::new(ring_pairs, max_inflight));
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
    pub fn dispatch(&self, batch: Batch) {
        match self.inner.mode {
            DispatchMode::Direct => self.dispatch_direct(batch),
            DispatchMode::Bridge => self.dispatch_bridge(batch),
            DispatchMode::SharedMemory => {
                if let Some(shm) = &self.inner.shm_dispatcher {
                    shm.dispatch_shm(batch);
                } else {
                    error!("SHM dispatcher not configured");
                }
            }
        }
    }

    /// Direct dispatch: send batch through channel, responses flow back via oneshot.
    fn dispatch_direct(&self, batch: Batch) {
        use vertebra_core::protocol::flags;

        let tx = match &self.inner.direct_tx {
            Some(tx) => tx,
            None => {
                error!("direct dispatch channel not configured");
                return;
            }
        };

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
        }
    }

    /// Bridge dispatch: write into arena/slab, seal, and track correlation IDs.
    fn dispatch_bridge(&self, batch: Batch) {
        use vertebra_core::protocol::flags;

        let bridge = match BatchBridge::get() {
            Some(b) => b,
            None => {
                error!("batch bridge not initialized, dropping batch");
                return;
            }
        };
        let writer = bridge.batch_writer();

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
            }
        }

        if let Err(e) = writer.seal() {
            error!(error = %e, "transport seal failed");
        }
    }

    /// Complete a request with a response (bridge or SHM mode).
    pub fn complete(&self, correlation_id: u64, response: PendingResponse) {
        match self.inner.mode {
            DispatchMode::SharedMemory => {
                if let Some(shm) = &self.inner.shm_dispatcher {
                    let mut slab = shm.correlation().lock().unwrap();
                    if !slab.complete(correlation_id, response) {
                        debug!(
                            id = correlation_id,
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

    #[test]
    fn shm_dispatcher_round_robin() {
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

    #[test]
    fn shm_dispatcher_complete_through_correlation() {
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
        let snap = BatchSnapshot::deserialize_from(&buf[..len]).expect("deserialize");
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
