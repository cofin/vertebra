//! Dispatcher: routes filled batches to workers.
//!
//! Three dispatch modes, each in its own submodule:
//! - **Direct** (`direct.rs`): batch sent via `tokio::sync::mpsc`, responses
//!   via oneshot on each `PendingRequest`. No bridge, no arena, no
//!   correlation IDs.
//! - **Bridge** (`bridge.rs`): requests written into the arena-backed
//!   `BatchBridge`, responses polled from the bridge channel. Uses DashMap
//!   for correlation.
//! - **SharedMemory** (`shm.rs`): requests serialized into per-worker MPMC
//!   rings, responses read from tiered response rings. Uses O(1)
//!   `CorrelationSlab`.
//!
//! Shared request/worker types live in `types.rs`.

mod bridge;
mod direct;
mod shm;
#[cfg(test)]
mod tests;
pub mod types;

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error};

use crate::backpressure::{BackpressureConfig, BackpressureController};
use crate::collector::{Batch, PendingResponse};

pub use self::shm::ShmDispatcher;
pub use self::types::{DirectBatch, DirectRequest};

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

/// Dispatcher state shared between the write task and the response router.
pub(super) struct Inner {
    /// In-flight correlation ID -> reply channel (bridge mode only).
    pub(super) pending: DashMap<u64, oneshot::Sender<PendingResponse>>,
    /// Next monotonic correlation ID (bridge mode only).
    pub(super) next_id: AtomicU64,
    /// Direct dispatch channel (Some if direct mode).
    pub(super) direct_tx: Option<mpsc::Sender<DirectBatch>>,
    /// SHM dispatcher (Some if SharedMemory mode).
    pub(super) shm_dispatcher: Option<Arc<ShmDispatcher>>,
    /// Dispatch mode.
    pub(super) mode: DispatchMode,
}

/// The dispatcher.
///
/// Manages in-flight request tracking and dispatches batches to workers
/// via the direct channel, arena-backed BatchBridge, or MPMC ring pairs.
///
/// The optional `backpressure` controller is exposed via
/// [`Dispatcher::backpressure`] for callers that wrap dispatch with
/// explicit admission control. The dispatcher itself never shortcuts the
/// dispatch path based on the controller — the HTTP ingress layer owns
/// that decision so it can return a proper HTTP response on rejection.
pub struct Dispatcher {
    inner: Arc<Inner>,
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
            inner: Arc::new(Inner {
                pending: DashMap::new(),
                next_id: AtomicU64::new(1),
                direct_tx: None,
                shm_dispatcher: None,
                mode: DispatchMode::Bridge,
            }),
            backpressure: None,
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
            backpressure: None,
        };
        (dispatcher, rx)
    }

    /// Create a new dispatcher in SharedMemory mode.
    pub fn new_shm(ring_pairs: Vec<Arc<vertebra_core::transport::ring_pair::RingPair>>, max_inflight: usize) -> Self {
        let shm = ShmDispatcher::new(ring_pairs, max_inflight);
        Self {
            inner: Arc::new(Inner {
                pending: DashMap::new(),
                next_id: AtomicU64::new(1),
                direct_tx: None,
                shm_dispatcher: Some(shm),
                mode: DispatchMode::SharedMemory,
            }),
            backpressure: None,
        }
    }

    /// Attach a backpressure controller. The dispatcher itself never
    /// consults the controller — the HTTP ingress path must call
    /// [`BackpressureController::try_acquire`] before calling
    /// [`Dispatcher::dispatch`] and handle the rejection.
    #[must_use]
    pub fn with_backpressure(mut self, config: BackpressureConfig) -> Self {
        self.backpressure = Some(Arc::new(BackpressureController::new(config)));
        self
    }

    /// Access the backpressure controller, if one has been attached.
    pub fn backpressure(&self) -> Option<&Arc<BackpressureController>> {
        self.backpressure.as_ref()
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
            DispatchMode::Direct => direct::dispatch_direct(&self.inner, batch),
            DispatchMode::Bridge => bridge::dispatch_bridge(&self.inner, batch),
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
