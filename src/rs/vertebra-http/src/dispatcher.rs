//! Dispatcher: routes filled batches to either the direct channel or the
//! arena-backed BatchBridge.
//!
//! Two dispatch modes:
//! - **Direct**: batch sent via `tokio::sync::mpsc`, responses via oneshot on
//!   each `PendingRequest`. No bridge, no arena, no correlation IDs.
//! - **Bridge**: requests written into the arena/slab, responses polled from
//!   the bridge channel. Uses correlation IDs for request/response matching.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error};

use vertebra_core::bridge::{BatchBridge, WriteRequest};

use crate::collector::{Batch, PendingResponse};

/// How batches are dispatched to workers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchMode {
    /// Direct: batch sent via channel, responses via oneshot (fast path).
    Direct,
    /// Bridge: batch written to arena/slab, responses via bridge channel (data-intensive).
    Bridge,
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

/// Dispatcher state shared between the write task and the response router.
struct Inner {
    /// In-flight correlation ID -> reply channel (bridge mode only).
    pending: DashMap<u64, oneshot::Sender<PendingResponse>>,
    /// Next monotonic correlation ID (bridge mode only).
    next_id: AtomicU64,
    /// Direct dispatch channel (Some if direct mode).
    direct_tx: Option<mpsc::Sender<DirectBatch>>,
    /// Dispatch mode.
    mode: DispatchMode,
}

/// The dispatcher.
///
/// Manages in-flight request tracking and dispatches batches to workers
/// via either the direct channel path or the arena-backed `BatchBridge`.
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
                mode: DispatchMode::Bridge,
            }),
        }
    }

    /// Create a new dispatcher in direct mode.
    ///
    /// Returns the dispatcher and a receiver for direct batches.
    pub fn new_direct(channel_capacity: usize) -> (Self, mpsc::Receiver<DirectBatch>) {
        let (tx, rx) = mpsc::channel(channel_capacity);
        let dispatcher = Self {
            inner: Arc::new(Inner {
                pending: DashMap::new(),
                next_id: AtomicU64::new(1),
                direct_tx: Some(tx),
                mode: DispatchMode::Direct,
            }),
        };
        (dispatcher, rx)
    }

    /// Dispatch a batch to workers.
    ///
    /// In direct mode, sends the batch through the channel with oneshot reply
    /// channels still attached. In bridge mode, writes into the arena/slab.
    pub fn dispatch(&self, batch: Batch) {
        if self.inner.mode == DispatchMode::Direct {
            self.dispatch_direct(batch);
        } else {
            self.dispatch_bridge(batch);
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
                    // F&F: no reply channel needed — caller won't wait for response.
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

            // F&F: skip pending tracking — no response expected.
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

    /// Complete a request with a response from a Python worker (bridge mode).
    pub fn complete(&self, correlation_id: u64, response: PendingResponse) {
        if let Some((_, tx)) = self.inner.pending.remove(&correlation_id) {
            let _ = tx.send(response);
        } else {
            debug!(id = correlation_id, "no pending request for correlation ID");
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
        // Should not panic
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
}
