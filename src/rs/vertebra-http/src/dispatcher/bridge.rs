//! Bridge dispatch: submit zero-copy [`InProcessRequest`] batches to [`BatchBridge`]
//! and track correlation IDs in an 8-shard O(1) [`InFlightRequests`] slab with
//! direct [`BatchResponseSink`] and [`ResponseSink`] completion outside shard locks.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};
use vertebra_core::bridge::{BatchBridge, BridgeResponse, StreamDelivery};
use vertebra_core::buffer::CacheLine;
use vertebra_core::protocol::flags;
use vertebra_core::transport::InProcessRequest;

use super::correlation::{CorrelationId, InFlightRequests};
use super::streams::{DEFAULT_STREAM_QUEUE_DEPTH, ResponseStreamRegistry};
use super::types::service_unavailable_response;
use crate::batcher::PendingRequest;
use crate::body::PendingResponse;

const BRIDGE_SHARDS: usize = 8;
const BRIDGE_SHARD_MASK: usize = BRIDGE_SHARDS - 1;
pub(super) const DEFAULT_BRIDGE_MAX_INFLIGHT: usize = 65_536;

/// Sharded O(1) in-flight correlation table for embedded bridge mode.
pub struct BridgeCorrelation {
    shards: [Mutex<InFlightRequests>; BRIDGE_SHARDS],
    next_batch: AtomicUsize,
    active_requests: CacheLine<AtomicUsize>,
    sink_registered: AtomicBool,
    streams: ResponseStreamRegistry,
}

impl Default for BridgeCorrelation {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_BRIDGE_MAX_INFLIGHT)
    }
}

impl BridgeCorrelation {
    /// Construct an 8-shard correlation slab with total capacity `max_inflight`.
    pub fn with_capacity(max_inflight: usize) -> Self {
        Self::with_capacity_and_queue_depth(max_inflight, DEFAULT_STREAM_QUEUE_DEPTH)
    }

    /// Construct an 8-shard correlation slab with total capacity `max_inflight` and stream queue depth.
    pub fn with_capacity_and_queue_depth(max_inflight: usize, stream_queue_depth: usize) -> Self {
        let per_shard = (max_inflight / BRIDGE_SHARDS).max(64);
        Self {
            shards: std::array::from_fn(|_| Mutex::new(InFlightRequests::new(per_shard))),
            next_batch: AtomicUsize::new(0),
            active_requests: CacheLine(AtomicUsize::new(0)),
            sink_registered: AtomicBool::new(false),
            streams: ResponseStreamRegistry::with_capacity(stream_queue_depth),
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

    /// Shared progressive delivery state for this correlation table.
    pub fn stream_registry(&self) -> &ResponseStreamRegistry {
        &self.streams
    }

    /// Allocate a correlation, answering with 503 when every shard is full.
    pub fn register_pending(&self, reply: oneshot::Sender<PendingResponse>) -> Option<u64> {
        let start = self.next_batch.fetch_add(1, Ordering::Relaxed) & BRIDGE_SHARD_MASK;
        for attempt in 0..BRIDGE_SHARDS {
            let shard_idx = (start + attempt) & BRIDGE_SHARD_MASK;
            let mut shard = self.shards[shard_idx].lock().unwrap();
            if shard.in_flight() < shard.capacity() {
                let cid = shard
                    .insert(shard_idx as u8, reply, 0)
                    .expect("correlation slot available after capacity check");
                self.active_requests.0.fetch_add(1, Ordering::Release);
                return Some(cid.raw());
            }
        }
        let _ = reply.send(service_unavailable_response());
        None
    }

    /// Register direct `BatchResponseSink`, `ResponseSink`, and `StreamSink` callbacks on `bridge`.
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
            let weak_batch: Weak<Self> = Arc::downgrade(self);
            bridge.set_batch_response_sink(Arc::new(move |resps: Vec<BridgeResponse>| {
                if let Some(corr) = weak_batch.upgrade() {
                    corr.complete_bridge_batch(resps);
                }
            }));
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
            let weak = Arc::downgrade(self);
            bridge.set_stream_sink(Arc::new(move |frame| match weak.upgrade() {
                Some(corr) => corr
                    .streams
                    .deliver_blocking(frame, |id, response| corr.complete(id, response)),
                None => StreamDelivery::Closed,
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
            tx.send(response).is_ok()
        } else {
            false
        }
    }

    /// Dispatch a single request directly through the in-process `BatchBridge`.
    ///
    /// Returns `Some(correlation_id)` on success, or `None` if the bridge is
    /// unavailable, correlation capacity is exhausted, or submission fails.
    pub fn dispatch_single(self: &Arc<Self>, req: PendingRequest) -> Option<u64> {
        let bridge = BatchBridge::get()?;
        self.ensure_sink_registered(bridge);

        let is_fnf = (req.flags & flags::FIRE_AND_FORGET) != 0;
        let correlation_id = if is_fnf {
            0
        } else {
            self.register_pending(req.reply)?
        };

        let inproc_req = InProcessRequest::from_path_and_query(
            req.method_tag,
            req.http_version_tag,
            req.handler_id,
            correlation_id,
            req.path_and_query,
            req.headers_mp,
            req.body,
            Bytes::new(),
        );

        if let Err(e) = bridge.submit_inprocess_batch(vec![inproc_req]) {
            error!(error = %e, "in-process single request submit failed");
            if !is_fnf {
                let _ = self.cancel_correlation(correlation_id);
            }
            return None;
        }

        Some(correlation_id)
    }

    /// Cancel a pending correlation ID without sending a response on its reply channel.
    ///
    /// Returns `true` if the correlation entry was present and removed, or `false` if it
    /// had already been completed or cancelled.
    #[inline]
    pub fn cancel_correlation(&self, correlation_id: u64) -> bool {
        let cid = CorrelationId::from_raw(correlation_id);
        let shard_idx = cid.shard() & BRIDGE_SHARD_MASK;
        let tx = {
            let mut slab = self.shards[shard_idx].lock().unwrap();
            slab.take_completed(cid)
        };
        if let Some(tx) = tx {
            drop(tx);
            self.active_requests.0.fetch_sub(1, Ordering::Release);
            true
        } else {
            false
        }
    }

    /// Complete a batch of `BridgeResponse` items directly from `BatchResponseSink`.
    pub fn complete_bridge_batch(&self, responses: Vec<BridgeResponse>) {
        self.complete_many_inner(
            responses,
            |r| r.request_id,
            |r| PendingResponse {
                status: r.status,
                headers_mp: r.headers_raw,
                body: r.body,
                stream_rx: None,
            },
        );
    }

    /// Complete a batch of responses grouped by shard, releasing each shard lock before `oneshot::send`.
    pub fn complete_many(&self, responses: Vec<(u64, PendingResponse)>) {
        self.complete_many_inner(responses, |r| r.0, |r| r.1);
    }

    #[inline]
    fn complete_many_inner<T, FId, FResp>(&self, responses: Vec<T>, get_id: FId, into_resp: FResp)
    where
        FId: Fn(&T) -> u64,
        FResp: Fn(T) -> PendingResponse,
    {
        if responses.is_empty() {
            return;
        }
        if responses.len() == 1 {
            let item = responses.into_iter().next().unwrap();
            let raw_id = get_id(&item);
            let _ = self.complete(raw_id, into_resp(item));
            return;
        }

        let first_shard =
            CorrelationId::from_raw(get_id(&responses[0])).shard() & BRIDGE_SHARD_MASK;
        let all_same_shard = responses[1..].iter().all(|item| {
            (CorrelationId::from_raw(get_id(item)).shard() & BRIDGE_SHARD_MASK) == first_shard
        });

        if all_same_shard {
            let mut ready: Vec<(oneshot::Sender<PendingResponse>, PendingResponse)> =
                Vec::with_capacity(responses.len());
            {
                let mut slab = self.shards[first_shard].lock().unwrap();
                for item in responses {
                    let cid = CorrelationId::from_raw(get_id(&item));
                    if let Some(tx) = slab.take_completed(cid) {
                        ready.push((tx, into_resp(item)));
                    } else {
                        debug!(
                            id = cid.raw(),
                            shard = first_shard,
                            "no pending bridge request for correlation ID"
                        );
                    }
                }
            }
            if !ready.is_empty() {
                self.active_requests
                    .0
                    .fetch_sub(ready.len(), Ordering::Release);
                for (tx, resp) in ready {
                    let _ = tx.send(resp);
                }
            }
            return;
        }

        let mut buckets: [Vec<(CorrelationId, PendingResponse)>; BRIDGE_SHARDS] =
            std::array::from_fn(|_| Vec::new());
        for item in responses {
            let cid = CorrelationId::from_raw(get_id(&item));
            buckets[cid.shard() & BRIDGE_SHARD_MASK].push((cid, into_resp(item)));
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
        self.streams.sweep_closed();
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
    batcher_shard: Option<usize>,
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
    let mut rejected_replies: Vec<oneshot::Sender<PendingResponse>> = Vec::new();

    let start = batcher_shard
        .unwrap_or_else(|| bridge_corr.next_batch.fetch_add(1, Ordering::Relaxed))
        & BRIDGE_SHARD_MASK;

    let mut current_attempt = 0usize;
    let mut current_shard_idx = start;
    let mut current_guard = None;

    for req in requests.drain(..) {
        let is_fnf = (req.flags & flags::FIRE_AND_FORGET) != 0;
        let correlation_id = if is_fnf {
            0
        } else {
            let mut allocated = None;
            let mut reply = Some(req.reply);

            while current_attempt < BRIDGE_SHARDS {
                let guard = current_guard
                    .get_or_insert_with(|| bridge_corr.shards[current_shard_idx].lock().unwrap());
                if guard.in_flight() < guard.capacity() {
                    let cid = guard
                        .insert(current_shard_idx as u8, reply.take().unwrap(), 0)
                        .expect("correlation slot available after capacity check");
                    allocated = Some(cid.raw());
                    break;
                }
                current_guard = None;
                current_attempt += 1;
                if current_attempt < BRIDGE_SHARDS {
                    current_shard_idx = (start + current_attempt) & BRIDGE_SHARD_MASK;
                }
            }

            if let Some(id) = allocated {
                allocated_ids.push(id);
                id
            } else {
                if let Some(tx) = reply {
                    rejected_replies.push(tx);
                }
                continue;
            }
        };

        inproc_reqs.push(InProcessRequest::from_path_and_query(
            req.method_tag,
            req.http_version_tag,
            req.handler_id,
            correlation_id,
            req.path_and_query,
            req.headers_mp,
            req.body,
            Bytes::new(),
        ));
    }

    drop(current_guard);

    if !allocated_ids.is_empty() {
        bridge_corr
            .active_requests
            .0
            .fetch_add(allocated_ids.len(), Ordering::Release);
    }

    if !rejected_replies.is_empty() {
        warn!(
            rejected = rejected_replies.len(),
            "all bridge correlation shards full, responding with 503"
        );
        for tx in rejected_replies {
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

#[cfg(test)]
mod tests {
    use super::*;

    static TEST_BRIDGE_LOCK: Mutex<()> = Mutex::new(());

    struct TestBridgeGuard {
        _guard: std::sync::MutexGuard<'static, ()>,
        bridge: &'static BatchBridge,
    }

    impl Drop for TestBridgeGuard {
        fn drop(&mut self) {
            self.bridge.clear_batch_response_sink();
            self.bridge.clear_response_sink();
            self.bridge.clear_stream_sink();
            drain_bridge_batches(self.bridge);
        }
    }

    fn lock_test_bridge() -> TestBridgeGuard {
        let guard = TEST_BRIDGE_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _ = BatchBridge::init(64, 65_536, 16, 1);
        let bridge = BatchBridge::get().expect("global BatchBridge initialized");
        drain_bridge_batches(bridge);
        TestBridgeGuard {
            _guard: guard,
            bridge,
        }
    }

    fn drain_bridge_batches(bridge: &BatchBridge) {
        while bridge.poll_batch(Duration::ZERO).is_some() {}
    }

    fn make_pending(
        path: &str,
        req_flags: u32,
    ) -> (PendingRequest, oneshot::Receiver<PendingResponse>) {
        let (tx, rx) = oneshot::channel();
        let req = PendingRequest {
            method_tag: 0,
            http_version_tag: 1,
            handler_id: 1,
            flags: req_flags,
            path_and_query: path.parse().unwrap(),
            headers_mp: Bytes::new(),
            body: Bytes::new(),
            reply: tx,
        };
        (req, rx)
    }

    #[test]
    fn dispatch_bridge_vec_single_shard_batch_allocation_and_batch_sink_delivery() {
        let guard = lock_test_bridge();
        let bridge = guard.bridge;

        let corr = Arc::new(BridgeCorrelation::with_capacity(1024));
        let mut reqs = Vec::new();
        let mut rxs = Vec::new();
        for i in 0..16 {
            let (req, rx) = make_pending(&format!("/item/{i}?q={i}"), 0);
            reqs.push(req);
            rxs.push(rx);
        }

        let dispatched = dispatch_bridge_vec(&corr, Some(0), &mut reqs);
        assert_eq!(dispatched, 16);
        assert_eq!(corr.in_flight(), 16);

        let snap = bridge
            .poll_batch(Duration::from_millis(100))
            .expect("inprocess batch delivered");
        assert_eq!(snap.len(), 16);

        let mut resps = Vec::with_capacity(16);
        for i in 0..16 {
            let raw_id = snap.node(i).correlation_id;
            assert_eq!(CorrelationId::from_raw(raw_id).shard(), 0);
            assert_eq!(snap.path(i), format!("/item/{i}"));
            assert_eq!(snap.query(i), Some(format!("q={i}")).as_deref());
            resps.push(BridgeResponse {
                request_id: raw_id,
                status: 200,
                headers_raw: Bytes::new(),
                body: Bytes::from(format!("ok-{i}")),
            });
        }

        bridge
            .submit_responses(resps)
            .expect("batch response sink delivery");
        assert_eq!(corr.in_flight(), 0);
        for (i, mut rx) in rxs.into_iter().enumerate() {
            let resp = rx.try_recv().expect("response delivered");
            assert_eq!(resp.status, 200);
            assert_eq!(resp.body.as_ref(), format!("ok-{i}").as_bytes());
        }
    }

    #[test]
    fn dispatch_bridge_vec_spills_shards_and_rejects_overflow_outside_locks() {
        let guard = lock_test_bridge();
        let bridge = guard.bridge;

        let corr = Arc::new(BridgeCorrelation::with_capacity(512));
        let mut reqs = Vec::new();
        let mut rxs = Vec::new();
        for _ in 0..520 {
            let (req, rx) = make_pending("/spill", 0);
            reqs.push(req);
            rxs.push(rx);
        }

        let dispatched = dispatch_bridge_vec(&corr, Some(0), &mut reqs);
        assert_eq!(dispatched, 512);
        assert_eq!(corr.in_flight(), 512);

        let mut ok_ids = Vec::new();
        let mut rejected_503 = 0usize;
        if let Some(snap) = bridge.poll_batch(Duration::from_millis(100)) {
            for i in 0..snap.len() {
                ok_ids.push(snap.node(i).correlation_id);
            }
        }
        assert_eq!(ok_ids.len(), 512);
        let shards: std::collections::HashSet<usize> = ok_ids
            .iter()
            .map(|&id| CorrelationId::from_raw(id).shard())
            .collect();
        assert!(
            shards.len() > 1,
            "expected shard spill across multiple shards"
        );

        for rx in &mut rxs[512..] {
            let resp = rx.try_recv().expect("overflow rejected with 503");
            assert_eq!(resp.status, 503);
            rejected_503 += 1;
        }
        assert_eq!(rejected_503, 8);

        let resps = ok_ids
            .into_iter()
            .map(|id| {
                (
                    id,
                    PendingResponse {
                        status: 200,
                        headers_mp: Bytes::new(),
                        body: Bytes::new(),
                        stream_rx: None,
                    },
                )
            })
            .collect();
        corr.complete_many(resps);
        assert_eq!(corr.in_flight(), 0);
    }

    #[test]
    fn complete_many_handles_single_shard_stale_id_and_mixed_shards() {
        let corr = Arc::new(BridgeCorrelation::with_capacity(1024));
        let mut ids_s0 = Vec::new();
        let mut rxs_s0 = Vec::new();
        {
            let mut shard0 = corr.shards[0].lock().unwrap();
            for _ in 0..8 {
                let (tx, rx) = oneshot::channel();
                let cid = shard0.insert(0, tx, 0).unwrap();
                ids_s0.push(cid.raw());
                rxs_s0.push(rx);
            }
            corr.active_requests.0.fetch_add(8, Ordering::Release);
        }

        let mut single_batch: Vec<(u64, PendingResponse)> = ids_s0
            .into_iter()
            .map(|id| {
                (
                    id,
                    PendingResponse {
                        status: 200,
                        headers_mp: Bytes::new(),
                        body: Bytes::from_static(b"s0"),
                        stream_rx: None,
                    },
                )
            })
            .collect();
        let stale_same_shard = CorrelationId::encode(0, 999, 1).raw();
        single_batch.push((
            stale_same_shard,
            PendingResponse {
                status: 200,
                headers_mp: Bytes::new(),
                body: Bytes::new(),
                stream_rx: None,
            },
        ));
        corr.complete_many(single_batch);
        assert_eq!(corr.in_flight(), 0);
        for mut rx in rxs_s0 {
            assert_eq!(rx.try_recv().unwrap().body.as_ref(), b"s0");
        }

        let mut mixed_batch = Vec::new();
        let mut mixed_rxs = Vec::new();
        for shard_idx in [0u8, 1u8, 3u8] {
            let (tx, rx) = oneshot::channel();
            let cid = corr.shards[shard_idx as usize]
                .lock()
                .unwrap()
                .insert(shard_idx, tx, 0)
                .unwrap();
            corr.active_requests.0.fetch_add(1, Ordering::Release);
            mixed_batch.push((
                cid.raw(),
                PendingResponse {
                    status: 201,
                    headers_mp: Bytes::new(),
                    body: Bytes::from(format!("s{shard_idx}")),
                    stream_rx: None,
                },
            ));
            mixed_rxs.push((shard_idx, rx));
        }
        corr.complete_many(mixed_batch);
        assert_eq!(corr.in_flight(), 0);
        for (s, mut rx) in mixed_rxs {
            let resp = rx.try_recv().unwrap();
            assert_eq!(resp.status, 201);
            assert_eq!(resp.body.as_ref(), format!("s{s}").as_bytes());
        }
    }

    #[test]
    fn dispatch_bridge_vec_handles_mixed_fire_and_forget_batch() {
        let guard = lock_test_bridge();
        let bridge = guard.bridge;

        let corr = Arc::new(BridgeCorrelation::with_capacity(512));
        let (fnf_req, _fnf_rx) = make_pending("/fnf", flags::FIRE_AND_FORGET);
        let (norm_req, mut norm_rx) = make_pending("/normal", 0);
        let mut reqs = vec![fnf_req, norm_req];

        let dispatched = dispatch_bridge_vec(&corr, Some(2), &mut reqs);
        assert_eq!(dispatched, 2);
        assert_eq!(corr.in_flight(), 1);

        let snap = bridge
            .poll_batch(Duration::from_millis(100))
            .expect("mixed batch delivered");
        assert_eq!(snap.node(0).correlation_id, 0);
        let norm_id = snap.node(1).correlation_id;
        assert_ne!(norm_id, 0);
        assert_eq!(CorrelationId::from_raw(norm_id).shard(), 2);

        assert!(corr.complete(
            norm_id,
            PendingResponse {
                status: 204,
                headers_mp: Bytes::new(),
                body: Bytes::new(),
                stream_rx: None,
            }
        ));
        assert_eq!(corr.in_flight(), 0);
        assert_eq!(norm_rx.try_recv().unwrap().status, 204);
    }
}
