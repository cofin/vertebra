//! Bridge dispatch: submit zero-copy [`InProcessRequest`] batches to [`BatchBridge`]
//! and track correlation IDs in an 8-shard O(1) [`InFlightRequests`] slab with
//! direct [`ResponseSink`] completion outside shard locks.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};
use vertebra_core::bridge::{BatchBridge, BridgeResponse};
use vertebra_core::buffer::CacheLine;
use vertebra_core::protocol::flags;
use vertebra_core::transport::InProcessRequest;

use super::correlation::{CorrelationId, InFlightRequests};
use super::types::service_unavailable_response;
use crate::batcher::PendingRequest;
use crate::body::PendingResponse;

const BRIDGE_SHARDS: usize = 8;
const BRIDGE_SHARD_MASK: usize = BRIDGE_SHARDS - 1;
const DEFAULT_BRIDGE_MAX_INFLIGHT: usize = 65_536;

/// Sharded O(1) in-flight correlation table for embedded bridge mode.
pub struct BridgeCorrelation {
    shards: [Mutex<InFlightRequests>; BRIDGE_SHARDS],
    next_batch: AtomicUsize,
    active_requests: CacheLine<AtomicUsize>,
    sink_registered: AtomicBool,
}

impl Default for BridgeCorrelation {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_BRIDGE_MAX_INFLIGHT)
    }
}

impl BridgeCorrelation {
    /// Construct an 8-shard correlation slab with total capacity `max_inflight`.
    pub fn with_capacity(max_inflight: usize) -> Self {
        let per_shard = (max_inflight / BRIDGE_SHARDS).max(64);
        Self {
            shards: std::array::from_fn(|_| Mutex::new(InFlightRequests::new(per_shard))),
            next_batch: AtomicUsize::new(0),
            active_requests: CacheLine(AtomicUsize::new(0)),
            sink_registered: AtomicBool::new(false),
        }
    }

    /// Return `true` when zero requests are currently in flight.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.in_flight() == 0
    }

    /// Return the total number of in-flight requests across all shards.
    #[inline]
    pub fn in_flight(&self) -> usize {
        self.active_requests.0.load(Ordering::Acquire)
    }

    /// Register a direct `ResponseSink` callback on `bridge` holding a `Weak` handle to `self`.
    #[inline]
    pub fn ensure_sink_registered(self: &Arc<Self>, bridge: &BatchBridge) {
        if self.sink_registered.load(Ordering::Acquire) {
            return;
        }
        if self
            .sink_registered
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let weak: Weak<Self> = Arc::downgrade(self);
            bridge.set_response_sink(Arc::new(move |resp: BridgeResponse| {
                if let Some(corr) = weak.upgrade() {
                    let _ = corr.complete(
                        resp.request_id,
                        PendingResponse {
                            status: resp.status,
                            headers_mp: resp.headers_raw,
                            body: resp.body,
                            stream_rx: None,
                        },
                    );
                }
            }));
        }
    }

    /// Complete a single correlation ID, releasing the shard `Mutex` before `oneshot::send`.
    #[inline]
    pub fn complete(&self, correlation_id: u64, response: PendingResponse) -> bool {
        let cid = CorrelationId::from_raw(correlation_id);
        let shard_idx = cid.shard() & BRIDGE_SHARD_MASK;
        let tx = {
            let mut slab = self.shards[shard_idx].lock().unwrap();
            slab.take_completed(cid)
        };
        if let Some(tx) = tx {
            self.active_requests.0.fetch_sub(1, Ordering::Release);
            let _ = tx.send(response);
            true
        } else {
            false
        }
    }

    /// Complete a batch of responses grouped by shard, releasing each shard lock before `oneshot::send`.
    pub fn complete_many(&self, responses: Vec<(u64, PendingResponse)>) {
        if responses.is_empty() {
            return;
        }
        let mut buckets: [Vec<(CorrelationId, PendingResponse)>; BRIDGE_SHARDS] =
            std::array::from_fn(|_| Vec::new());
        for (raw_id, resp) in responses {
            let cid = CorrelationId::from_raw(raw_id);
            buckets[cid.shard() & BRIDGE_SHARD_MASK].push((cid, resp));
        }

        let mut ready: Vec<(oneshot::Sender<PendingResponse>, PendingResponse)> = Vec::new();
        for (shard_idx, bucket) in buckets.into_iter().enumerate() {
            if bucket.is_empty() {
                continue;
            }
            ready.clear();
            {
                let mut slab = self.shards[shard_idx].lock().unwrap();
                for (cid, resp) in bucket {
                    if let Some(tx) = slab.take_completed(cid) {
                        ready.push((tx, resp));
                    } else {
                        debug!(
                            id = cid.raw(),
                            shard = shard_idx,
                            "no pending bridge request for correlation ID"
                        );
                    }
                }
            }
            if !ready.is_empty() {
                self.active_requests
                    .0
                    .fetch_sub(ready.len(), Ordering::Release);
                for (tx, resp) in ready.drain(..) {
                    let _ = tx.send(resp);
                }
            }
        }
    }

    /// Sweep expired entries across all shards, sending `504 Gateway Timeout` outside locks.
    pub fn sweep_expired(&self, timeout: Duration) -> usize {
        let mut total = 0usize;
        for shard in &self.shards {
            let expired = {
                let mut slab = shard.lock().unwrap();
                slab.take_expired(timeout)
            };
            if !expired.is_empty() {
                total += expired.len();
                self.active_requests
                    .0
                    .fetch_sub(expired.len(), Ordering::Release);
                for tx in expired {
                    let _ = tx.send(PendingResponse {
                        status: 504,
                        headers_mp: Bytes::new(),
                        body: Bytes::from_static(b"Gateway Timeout"),
                        stream_rx: None,
                    });
                }
            }
        }
        total
    }

    /// Spawn a periodic background correlation sweeper holding a `Weak` reference.
    pub fn spawn_sweeper(
        self: &Arc<Self>,
        shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            let timeout = Duration::from_secs(30);
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => break,
                    _ = interval.tick() => {
                        if let Some(this) = weak.upgrade() {
                            this.sweep_expired(timeout);
                        } else {
                            break;
                        }
                    }
                }
            }
        })
    }

    fn fail_correlations(&self, ids: &[u64]) {
        for &raw in ids {
            let _ = self.complete(raw, service_unavailable_response());
        }
    }
}

pub(super) fn dispatch_bridge_vec(
    bridge_corr: &Arc<BridgeCorrelation>,
    requests: &mut Vec<PendingRequest>,
) -> usize {
    if requests.is_empty() {
        return 0;
    }

    let Some(bridge) = BatchBridge::get() else {
        for req in requests.drain(..) {
            if (req.flags & flags::FIRE_AND_FORGET) == 0 {
                let _ = req.reply.send(service_unavailable_response());
            }
        }
        error!("batch bridge not initialized, rejecting batch with 503");
        return 0;
    };

    bridge_corr.ensure_sink_registered(bridge);

    let batch_len = requests.len();
    let mut inproc_reqs = Vec::with_capacity(batch_len);
    let mut allocated_ids = Vec::with_capacity(batch_len);
    let mut overflow_replies = Vec::new();

    let mut shard_idx = bridge_corr.next_batch.fetch_add(1, Ordering::Relaxed) & BRIDGE_SHARD_MASK;
    {
        let mut shard_guard = bridge_corr.shards[shard_idx].lock().unwrap();
        for req in requests.drain(..) {
            let is_fnf = (req.flags & flags::FIRE_AND_FORGET) != 0;
            let correlation_id = if is_fnf {
                0
            } else {
                let mut reply_opt = Some(req.reply);
                if shard_guard.in_flight() >= shard_guard.capacity() {
                    drop(shard_guard);
                    let mut found = None;
                    for attempt in 1..BRIDGE_SHARDS {
                        let alt_idx = (shard_idx + attempt) & BRIDGE_SHARD_MASK;
                        let alt_guard = bridge_corr.shards[alt_idx].lock().unwrap();
                        if alt_guard.in_flight() < alt_guard.capacity() {
                            found = Some((alt_idx, alt_guard));
                            break;
                        }
                    }
                    if let Some((new_idx, new_guard)) = found {
                        shard_idx = new_idx;
                        shard_guard = new_guard;
                    } else {
                        shard_guard = bridge_corr.shards[shard_idx].lock().unwrap();
                        if let Some(tx) = reply_opt.take() {
                            overflow_replies.push(tx);
                        }
                        continue;
                    }
                }
                let cid = shard_guard
                    .insert(shard_idx as u8, reply_opt.take().unwrap(), 0)
                    .expect("correlation slot available after capacity check");
                let raw = cid.raw();
                allocated_ids.push(raw);
                raw
            };

            let path_bytes = req.path_and_query.path().as_bytes();
            let path = if path_bytes.is_empty() {
                Bytes::new()
            } else if path_bytes == b"/" {
                Bytes::from_static(b"/")
            } else {
                Bytes::copy_from_slice(path_bytes)
            };
            let query = req
                .path_and_query
                .query()
                .map_or_else(Bytes::new, |q| Bytes::copy_from_slice(q.as_bytes()));

            inproc_reqs.push(InProcessRequest::new(
                req.method_tag,
                req.http_version_tag,
                req.handler_id,
                correlation_id,
                path,
                query,
                req.headers_mp,
                req.body,
                Bytes::new(),
            ));
        }
    }

    if !allocated_ids.is_empty() {
        bridge_corr
            .active_requests
            .0
            .fetch_add(allocated_ids.len(), Ordering::Release);
    }

    if !overflow_replies.is_empty() {
        warn!(
            rejected = overflow_replies.len(),
            "all bridge correlation shards full, responding with 503"
        );
        for tx in overflow_replies {
            let _ = tx.send(service_unavailable_response());
        }
    }

    if inproc_reqs.is_empty() {
        return 0;
    }

    let dispatched_count = inproc_reqs.len();
    if let Err(e) = bridge.submit_inprocess_batch(inproc_reqs) {
        error!(error = %e, "in-process batch submit failed");
        bridge_corr.fail_correlations(&allocated_ids);
        return 0;
    }

    dispatched_count
}
