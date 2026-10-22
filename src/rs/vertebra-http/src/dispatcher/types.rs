//! Shared dispatcher types used across all three dispatch modes, plus small
//! helpers for the SHM accounting path.

use std::sync::Arc;

use vertebra_core::transport::ring_pair::RingPair;
use vertebra_core::transport::{Transport, TransportNode};

use crate::body::PendingResponse;

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

    pub(super) fn clear(&mut self) {
        self.flags = 0;
        self.nodes.clear();
        self.correlation_ids.clear();
        self.components.clear();
        self.slab_len = 0;
    }

    pub(super) fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

/// Reusable batch scratch space preallocated for workers.
pub(super) struct ShmBatchScratch {
    pub(super) batches: Vec<ShmDispatchBatch>,
}

impl ShmBatchScratch {
    pub(super) fn new(worker_count: usize) -> Self {
        Self {
            batches: (0..worker_count)
                .map(|_| ShmDispatchBatch::new(0))
                .collect(),
        }
    }

    pub(super) fn clear(&mut self) {
        for b in &mut self.batches {
            b.clear();
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
    worker.ring_pair.request_ring.has_capacity()
}

pub(super) fn service_unavailable_response() -> PendingResponse {
    PendingResponse {
        status: 503,
        headers_mp: bytes::Bytes::new(),
        body: bytes::Bytes::from_static(b"Service Unavailable"),
        stream_rx: None,
    }
}
