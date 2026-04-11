//! Shared dispatcher types used across all three dispatch modes.

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
}

impl ShmDispatchBatch {
    pub(super) fn new(flags: u32) -> Self {
        Self {
            flags,
            nodes: Vec::new(),
            correlation_ids: Vec::new(),
            components: Vec::new(),
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
