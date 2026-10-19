//! Shared dispatcher types used across all three dispatch modes, plus small
//! helpers for the SHM accounting path.

use std::sync::Arc;

use tokio::sync::oneshot;

use vertebra_core::transport::ring_pair::RingPair;
use vertebra_core::transport::{Transport, TransportNode};

use crate::collector::PendingResponse;

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
pub(super) struct WorkerState {
    pub(super) ring_pair: Arc<RingPair>,
    pub(super) transport: Transport,
}

/// A batch accumulating requests for a specific worker before flushing to its ring.
pub(super) struct ShmDispatchBatch {
    pub(super) flags: u32,
    pub(super) nodes: Vec<TransportNode>,
    pub(super) correlation_ids: Vec<u64>,
    pub(super) components: Vec<bytes::Bytes>,
    pub(super) slab_len: usize,
}

impl ShmDispatchBatch {
    pub(super) fn new(flags: u32) -> Self {
        Self {
            flags,
            nodes: Vec::new(),
            correlation_ids: Vec::new(),
            components: Vec::new(),
            slab_len: 0,
        }
    }
}

/// Borrowed view of the body-shaped components of a pending request.
pub(super) struct TransportRequestView<'a> {
    pub(super) path: &'a str,
    pub(super) query: Option<&'a str>,
    pub(super) headers_mp: &'a bytes::Bytes,
    pub(super) body: &'a bytes::Bytes,
}

pub(super) fn request_slab_size(req: &TransportRequestView<'_>) -> usize {
    req.path.len() + req.query.map_or(0, str::len) + req.headers_mp.len() + req.body.len()
}

pub(super) fn batch_snapshot_serialized_size(node_count: usize, slab_len: usize) -> usize {
    const BATCH_HEADER_SIZE: usize = 16;
    BATCH_HEADER_SIZE + node_count * std::mem::size_of::<TransportNode>() + slab_len
}

pub(super) fn batch_can_fit_request(
    pending: &ShmDispatchBatch,
    req: &TransportRequestView<'_>,
    max_payload: usize,
) -> bool {
    let next_nodes = pending.nodes.len() + 1;
    let next_slab = pending.slab_len + request_slab_size(req);
    batch_snapshot_serialized_size(next_nodes, next_slab) <= max_payload
}

pub(super) fn worker_ring_has_capacity(worker: &WorkerState) -> bool {
    let stats = worker.ring_pair.request_ring.stats();
    let mask = (stats.slots as u64).wrapping_sub(1);
    let next = stats.write_head.wrapping_add(1) & mask;
    next != stats.read_head
}

pub(super) fn service_unavailable_response() -> PendingResponse {
    PendingResponse {
        status: 503,
        headers_mp: bytes::Bytes::new(),
        body: bytes::Bytes::from_static(b"Service Unavailable"),
    }
}
