//! Dispatcher: routes filled batches to workers.
//!
//! Three dispatch backends:
//! - **Direct** (`direct.rs`): batch sent via `tokio::sync::mpsc`, responses
//!   via oneshot on each `PendingRequest`.
//! - **Bridge** (`bridge.rs`): requests written into the arena-backed
//!   `BatchBridge`, responses polled from the bridge channel. Uses `BridgeCorrelation`.
//! - **Shm** (`shm.rs`): requests serialized into per-worker MPMC
//!   rings, responses read from tiered response rings. Uses O(1)
//!   `InFlightRequests`.

mod bridge;
pub mod correlation;
mod direct;
mod shm;
pub mod shm_reader;
#[cfg(test)]
mod tests;
pub mod types;

pub use self::bridge_types::BridgeCorrelation;
pub use self::correlation::{CorrelationId, InFlightRequests};
pub use self::shm::ShmDispatcher;

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

use crate::backpressure::{BackpressureConfig, BackpressureController};
use crate::batcher::Batch;
use crate::body::PendingResponse;

mod bridge_types {
    use super::*;

    /// In-flight correlation tracking for bridge mode.
    pub struct BridgeCorrelation {
        pub pending: DashMap<u64, oneshot::Sender<PendingResponse>>,
        pub next_id: AtomicU64,
    }

    impl Default for BridgeCorrelation {
        fn default() -> Self {
            Self {
                pending: DashMap::new(),
                next_id: AtomicU64::new(1),
            }
        }
    }
}

/// Active dispatch backend variant.
pub enum DispatchBackend {
    Direct(mpsc::Sender<Batch>),
    Bridge(Arc<BridgeCorrelation>),
    Shm(Arc<ShmDispatcher>),
}

/// The dispatcher.
///
/// Manages in-flight request tracking and dispatches batches to workers
/// via the direct channel, arena-backed BatchBridge, or MPMC ring pairs.
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
    /// Create a new dispatcher in bridge mode (default).
    pub fn new() -> Self {
        Self {
            backend: DispatchBackend::Bridge(Arc::new(BridgeCorrelation::default())),
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

    /// Dispatch a batch to workers.
    pub fn dispatch(&self, batch: Batch) -> usize {
        match &self.backend {
            DispatchBackend::Direct(tx) => direct::dispatch_direct(tx, batch),
            DispatchBackend::Bridge(bridge) => bridge::dispatch_bridge(bridge, batch),
            DispatchBackend::Shm(shm) => shm.dispatch_shm(batch),
        }
    }

    /// Complete a request with a response (bridge or SHM mode).
    pub fn complete(&self, correlation_id: u64, response: PendingResponse) {
        match &self.backend {
            DispatchBackend::Shm(shm) => {
                let cid = CorrelationId::from_raw(correlation_id);
                let shard_idx = cid.shard() & (shm.shard_count() - 1);
                let mut slab = shm.correlation_shard(shard_idx).lock().unwrap();
                if !slab.complete(cid, response) {
                    debug!(
                        id = correlation_id,
                        shard = shard_idx,
                        "no pending SHM request for correlation ID"
                    );
                }
            }
            DispatchBackend::Bridge(bridge) => {
                if let Some((_, tx)) = bridge.pending.remove(&correlation_id) {
                    let _ = tx.send(response);
                } else {
                    debug!(id = correlation_id, "no pending request for correlation ID");
                }
            }
            DispatchBackend::Direct(_) => {}
        }
    }

    /// Complete multiple requests with grouped per-shard locks in SHM mode.
    pub fn complete_many(&self, responses: Vec<(u64, PendingResponse)>) {
        match &self.backend {
            DispatchBackend::Shm(shm) => {
                shm.complete_many(responses);
            }
            DispatchBackend::Bridge(bridge) => {
                for (correlation_id, response) in responses {
                    if let Some((_, tx)) = bridge.pending.remove(&correlation_id) {
                        let _ = tx.send(response);
                    } else {
                        debug!(id = correlation_id, "no pending request for correlation ID");
                    }
                }
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
