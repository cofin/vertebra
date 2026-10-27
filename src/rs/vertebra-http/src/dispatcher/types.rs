//! Shared dispatcher types used across all three dispatch modes, plus small
//! helpers for the SHM accounting path.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use vertebra_core::transport::TransportNode;
use vertebra_core::transport::ring_pair::RingPair;

use crate::body::PendingResponse;

/// Per-worker state for SHM dispatch.
pub(super) struct WorkerState {
    pub(super) ring_pair: Arc<RingPair>,
    pub(super) healthy: AtomicBool,
}

/// Borrowed view of the body-shaped components of a pending request.
pub(super) struct TransportRequestView<'a> {
    pub(super) path: &'a str,
    pub(super) query: Option<&'a str>,
    pub(super) headers_mp: &'a bytes::Bytes,
    pub(super) body: &'a bytes::Bytes,
}

#[inline]
pub(super) fn request_slab_size(req: &TransportRequestView<'_>) -> usize {
    req.path.len() + req.query.map_or(0, str::len) + req.headers_mp.len() + req.body.len()
}

#[inline]
pub(super) fn batch_snapshot_serialized_size(node_count: usize, slab_len: usize) -> usize {
    const BATCH_HEADER_SIZE: usize = 16;
    BATCH_HEADER_SIZE + node_count * std::mem::size_of::<TransportNode>() + slab_len
}

#[inline]
pub(super) fn worker_ring_has_capacity(worker: &WorkerState) -> bool {
    worker.healthy.load(Ordering::Acquire) && worker.ring_pair.request_ring.has_capacity()
}

pub(super) fn service_unavailable_response() -> PendingResponse {
    PendingResponse {
        status: 503,
        headers_mp: bytes::Bytes::new(),
        body: bytes::Bytes::from_static(b"Service Unavailable"),
        stream_rx: None,
    }
}
