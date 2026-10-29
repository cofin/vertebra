//! Dispatcher: routes filled batches to workers.
//!
//! Three dispatch backends:
//! - **Direct** (`direct.rs`): batch sent via `tokio::sync::mpsc`, responses
//!   via oneshot on each `PendingRequest`.
//! - **Bridge** (`bridge.rs`): zero-copy `InProcessRequest` batches submitted
//!   to `BatchBridge` with 8-shard O(1) `InFlightRequests` correlation and
//!   direct `ResponseSink` completion.
//! - **Shm** (`shm.rs`): whole batches serialized directly into per-worker MPMC
//!   rings with warm-worker affinity and sharded O(1) `InFlightRequests`.

mod bridge;
pub mod correlation;
mod direct;
mod shm;
pub mod shm_reader;
#[cfg(test)]
mod tests;
pub mod types;

pub use self::bridge::BridgeCorrelation;
pub use self::correlation::{CorrelationId, InFlightRequests};
pub use self::shm::ShmDispatcher;

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::backpressure::{BackpressureConfig, BackpressureController};
use crate::batcher::{Batch, PendingRequest};
use crate::body::PendingResponse;

/// Active dispatch backend variant.
pub enum DispatchBackend {
    Direct(mpsc::Sender<Batch>),
    Bridge(Arc<BridgeCorrelation>),
    Shm(Arc<ShmDispatcher>),
}

/// Manages in-flight request tracking and dispatches batches to workers.
pub struct Dispatcher {
    backend: DispatchBackend,
    backpressure: Option<Arc<BackpressureController>>,
}

impl Default for Dispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl Dispatcher {
    /// Create a new dispatcher in bridge mode (default) and register its direct `ResponseSink`.
    pub fn new() -> Self {
        let corr = Arc::new(BridgeCorrelation::default());
        if let Some(bridge) = vertebra_core::bridge::BatchBridge::get() {
            corr.ensure_sink_registered(bridge);
        }
        Self {
            backend: DispatchBackend::Bridge(corr),
            backpressure: None,
        }
    }

    /// Create a new dispatcher in direct mode.
    pub fn new_direct(channel_capacity: usize) -> (Self, mpsc::Receiver<Batch>) {
        let (tx, rx) = mpsc::channel(channel_capacity);
        let dispatcher = Self {
            backend: DispatchBackend::Direct(tx),
            backpressure: None,
        };
        (dispatcher, rx)
    }

    /// Create a new dispatcher in SharedMemory mode.
    pub fn new_shm(
        ring_pairs: Vec<Arc<vertebra_core::transport::ring_pair::RingPair>>,
        max_inflight: usize,
    ) -> Self {
        let shm = ShmDispatcher::new(ring_pairs, max_inflight);
        Self {
            backend: DispatchBackend::Shm(shm),
            backpressure: None,
        }
    }

    /// Create a dispatcher from an existing `Arc<ShmDispatcher>`.
    pub fn from_shm(shm: Arc<ShmDispatcher>) -> Self {
        Self {
            backend: DispatchBackend::Shm(shm),
            backpressure: None,
        }
    }

    /// Spawn a background correlation timeout sweeper when running in `Bridge` mode.
    pub fn spawn_bridge_sweeper(
        &self,
        shutdown: CancellationToken,
    ) -> Option<tokio::task::JoinHandle<()>> {
        match &self.backend {
            DispatchBackend::Bridge(corr) => Some(corr.spawn_sweeper(shutdown)),
            _ => None,
        }
    }

    /// Access the backend variant.
    pub fn backend(&self) -> &DispatchBackend {
        &self.backend
    }

    /// Attach a backpressure controller.
    #[must_use]
    pub fn with_backpressure(mut self, config: BackpressureConfig) -> Self {
        self.backpressure = Some(Arc::new(BackpressureController::new(config)));
        self
    }

    /// Access the backpressure controller, if attached.
    pub fn backpressure(&self) -> Option<&Arc<BackpressureController>> {
        self.backpressure.as_ref()
    }

    /// Get the SHM dispatcher (for response reader access).
    pub fn shm_dispatcher(&self) -> Option<&Arc<ShmDispatcher>> {
        match &self.backend {
            DispatchBackend::Shm(shm) => Some(shm),
            _ => None,
        }
    }

    /// Return the total number of in-flight requests currently tracked by the dispatcher.
    #[inline]
    pub fn in_flight(&self) -> usize {
        match &self.backend {
            DispatchBackend::Direct(_) => 0,
            DispatchBackend::Bridge(bridge) => bridge.in_flight(),
            DispatchBackend::Shm(shm) => shm.in_flight(),
        }
    }

    /// Dispatch a batch to workers.
    pub fn dispatch(&self, mut batch: Batch) -> usize {
        self.dispatch_requests(&mut batch.requests)
    }

    /// Dispatch requests in-place, draining `requests` while preserving its heap capacity.
    #[inline]
    pub fn dispatch_requests(&self, requests: &mut Vec<PendingRequest>) -> usize {
        self.dispatch_requests_from_shard(None, requests)
    }

    /// Dispatch requests from a specific batcher shard (`Some(shard_idx)`), preserving sticky worker affinity.
    #[inline]
    pub fn dispatch_requests_from_shard(
        &self,
        batcher_shard: Option<usize>,
        requests: &mut Vec<PendingRequest>,
    ) -> usize {
        match &self.backend {
            DispatchBackend::Direct(tx) => direct::dispatch_direct_vec(tx, requests),
            DispatchBackend::Bridge(bridge) => bridge::dispatch_bridge_vec(bridge, requests),
            DispatchBackend::Shm(shm) => shm.dispatch_shm_vec_from_shard(batcher_shard, requests),
        }
    }

    /// Complete a request with a response (bridge or SHM mode), sending outside the shard lock.
    pub fn complete(&self, correlation_id: u64, response: PendingResponse) {
        match &self.backend {
            DispatchBackend::Shm(shm) => {
                if !shm.complete_one(correlation_id, response) {
                    debug!(
                        id = correlation_id,
                        "no pending SHM request for correlation ID"
                    );
                }
            }
            DispatchBackend::Bridge(bridge) => {
                if !bridge.complete(correlation_id, response) {
                    debug!(id = correlation_id, "no pending request for correlation ID");
                }
            }
            DispatchBackend::Direct(_) => {}
        }
    }

    /// Complete multiple requests with grouped per-shard locks, sending outside locks.
    pub fn complete_many(&self, responses: Vec<(u64, PendingResponse)>) {
        match &self.backend {
            DispatchBackend::Shm(shm) => {
                shm.complete_many(responses);
            }
            DispatchBackend::Bridge(bridge) => {
                bridge.complete_many(responses);
            }
            DispatchBackend::Direct(_) => {}
        }
    }

    /// Forward a raw SHM `ResponseSlot` frame to the SHM dispatcher.
    pub fn handle_raw_response_slot(&self, raw_slot: &[u8]) -> bool {
        match &self.backend {
            DispatchBackend::Shm(shm) => shm.handle_raw_response_slot(raw_slot),
            _ => false,
        }
    }
}
