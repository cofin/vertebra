//! Shared-memory dispatcher: serializes whole batches directly into per-worker
//! MPMC rings with warm-worker affinity, Dekker sleep notifications, and sharded
//! O(1) correlation releasing shard locks before `oneshot::send`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use vertebra_core::buffer::CacheLine;
use vertebra_core::transport::ring_pair::RingPair;
use vertebra_core::transport::{ShmBatchWriter, TransportNode};

use super::shm_reader::PendingRequests;
use super::types::{
    TransportRequestView, WorkerState, batch_snapshot_serialized_size, request_slab_size,
    service_unavailable_response, worker_ring_has_capacity,
};
use crate::batcher::Batch;
use crate::body::PendingResponse;
use crate::dispatcher::correlation::{CorrelationId, InFlightRequests};

const SHM_CORRELATION_SHARDS: usize = 8;
const SHM_CORRELATION_MASK: usize = SHM_CORRELATION_SHARDS - 1;

struct StagedShmRequest {
    node: TransportNode,
    flags: u32,
    slab_bytes: usize,
    path_and_query: http::uri::PathAndQuery,
    headers_mp: bytes::Bytes,
    body: bytes::Bytes,
}

/// SharedMemory dispatcher with whole-batch routing, warm-worker affinity, and sharded O(1) correlation.
pub struct ShmDispatcher {
    workers: Vec<WorkerState>,
    last_worker: AtomicUsize,
    next_worker: AtomicUsize,
    next_batch: AtomicUsize,
    active_requests: CacheLine<AtomicUsize>,
    correlation_shards: [Mutex<InFlightRequests>; SHM_CORRELATION_SHARDS],
    pending_streams: Arc<PendingRequests>,
    lease_table: Mutex<Option<Arc<vertebra_core::broker::ShmLeaseTable>>>,
}

impl ShmDispatcher {
    /// Create a new SHM dispatcher without spawning background tasks.
    pub fn new(ring_pairs: Vec<Arc<RingPair>>, max_inflight: usize) -> Arc<Self> {
        let workers: Vec<WorkerState> = ring_pairs
            .into_iter()
            .map(|rp| WorkerState { ring_pair: rp })
            .collect();

        let shard_capacity = max_inflight / SHM_CORRELATION_SHARDS;
        let correlation_shards =
            std::array::from_fn(|_| Mutex::new(InFlightRequests::new(shard_capacity)));

        Arc::new(Self {
            workers,
            last_worker: AtomicUsize::new(0),
            next_worker: AtomicUsize::new(1),
            next_batch: AtomicUsize::new(0),
            active_requests: CacheLine(AtomicUsize::new(0)),
            correlation_shards,
            pending_streams: Arc::new(PendingRequests::new()),
            lease_table: Mutex::new(None),
        })
    }

    #[inline]
    fn decrement_active(&self, count: usize) {
        if count == 0 {
            return;
        }
        let _ = self
            .active_requests
            .0
            .try_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                Some(cur.saturating_sub(count))
            });
    }

    /// Attach a shared-memory lease table for dead-worker lease reclamation.
    pub fn set_lease_table(&self, lease_table: Arc<vertebra_core::broker::ShmLeaseTable>) {
        *self.lease_table.lock().unwrap() = Some(lease_table);
    }

    /// Builder-style attachment of a shared-memory lease table.
    pub fn with_lease_table(
        self: Arc<Self>,
        lease_table: Arc<vertebra_core::broker::ShmLeaseTable>,
    ) -> Arc<Self> {
        self.set_lease_table(lease_table);
        self
    }

    /// Reclaim active connection leases held by an exited worker PID.
    pub fn reclaim_worker_leases(&self, worker_pid: u32) -> u32 {
        if let Some(ref table) = *self.lease_table.lock().unwrap() {
            table.reclaim_worker(worker_pid)
        } else {
            0
        }
    }

    /// Spawn periodic correlation sweeper task holding a Weak reference.
    pub fn spawn_sweeper(
        self: &Arc<Self>,
        shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let weak: Weak<Self> = Arc::downgrade(self);
        tokio::spawn(async move {
            info!("SHM correlation sweep task started");
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            let timeout = Duration::from_secs(30);
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        debug!("SHM correlation sweep task cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        if let Some(this) = weak.upgrade() {
                            this.sweep_expired(timeout);
                        } else {
                            debug!("SHM dispatcher dropped, exiting sweep task");
                            break;
                        }
                    }
                }
            }
        })
    }

    /// Dispatch a batch to a worker via its MPMC ring without global scratch locks.
    pub fn dispatch_shm(&self, mut batch: Batch) -> usize {
        self.dispatch_shm_vec(&mut batch.requests)
    }

    /// Dispatch requests in-place while preserving `requests` vector capacity.
    pub fn dispatch_shm_vec(&self, requests: &mut Vec<crate::batcher::PendingRequest>) -> usize {
        use vertebra_core::protocol::flags;

        if requests.is_empty() {
            return 0;
        }

        let worker_count = self.workers.len();
        if worker_count == 0 {
            for req in requests.drain(..) {
                if (req.flags & flags::FIRE_AND_FORGET) == 0 {
                    let _ = req.reply.send(service_unavailable_response());
                }
            }
            error!("no SHM workers configured, dropping batch");
            return 0;
        }

        let prev_active = self.active_requests.0.load(Ordering::Relaxed);
        let preferred_worker = if prev_active == 0 {
            self.last_worker.load(Ordering::Relaxed) % worker_count
        } else {
            self.next_worker.fetch_add(1, Ordering::Relaxed) % worker_count
        };

        let worker_idx = (0..worker_count)
            .map(|attempt| (preferred_worker + attempt) % worker_count)
            .find(|&idx| worker_ring_has_capacity(&self.workers[idx]));

        let Some(worker_idx) = worker_idx else {
            warn!("all worker rings full, responding with 503");
            for req in requests.drain(..) {
                if (req.flags & flags::FIRE_AND_FORGET) == 0 {
                    let _ = req.reply.send(service_unavailable_response());
                }
            }
            return 0;
        };

        if prev_active == 0 {
            self.next_worker
                .store(worker_idx.wrapping_add(1), Ordering::Relaxed);
        }
        self.last_worker.store(worker_idx, Ordering::Relaxed);

        let max_payload = self.workers[worker_idx]
            .ring_pair
            .request_ring
            .payload_capacity();

        let batch_len = requests.len();
        let mut staged = Vec::with_capacity(batch_len);
        let mut rejected_replies = Vec::new();
        let mut allocated_non_fnf = 0usize;

        let batch_seq = self.next_batch.fetch_add(1, Ordering::Relaxed);
        let mut shard_idx = batch_seq & SHM_CORRELATION_MASK;

        {
            let mut shard_guard = self.correlation_shards[shard_idx].lock().unwrap();
            for req in requests.drain(..) {
                let crate::batcher::PendingRequest {
                    handler_id,
                    method_tag,
                    http_version_tag,
                    flags: req_flags,
                    path_and_query,
                    headers_mp,
                    body,
                    reply,
                } = req;

                let path_str = path_and_query.path();
                let query_str = path_and_query.query();
                let req_slab_bytes = request_slab_size(&TransportRequestView {
                    path: path_str,
                    query: query_str,
                    headers_mp: &headers_mp,
                    body: &body,
                });

                let is_fnf = (req_flags & flags::FIRE_AND_FORGET) != 0;
                if batch_snapshot_serialized_size(1, req_slab_bytes) > max_payload {
                    if !is_fnf {
                        rejected_replies.push(reply);
                    }
                    continue;
                }

                let correlation_id = if is_fnf {
                    0
                } else {
                    let mut reply_opt = Some(reply);
                    if shard_guard.in_flight() >= shard_guard.capacity() {
                        drop(shard_guard);
                        let mut found = None;
                        for attempt in 1..SHM_CORRELATION_SHARDS {
                            let alt_idx = (shard_idx + attempt) & SHM_CORRELATION_MASK;
                            let alt_guard = self.correlation_shards[alt_idx].lock().unwrap();
                            if alt_guard.in_flight() < alt_guard.capacity() {
                                found = Some((alt_idx, alt_guard));
                                break;
                            }
                        }
                        if let Some((new_idx, new_guard)) = found {
                            shard_idx = new_idx;
                            shard_guard = new_guard;
                        } else {
                            shard_guard = self.correlation_shards[shard_idx].lock().unwrap();
                            if let Some(tx) = reply_opt.take() {
                                rejected_replies.push(tx);
                            }
                            continue;
                        }
                    }
                    let cid = shard_guard
                        .insert(
                            shard_idx as u8,
                            reply_opt.take().unwrap(),
                            worker_idx as u32,
                        )
                        .expect("correlation slot should be available after capacity check");
                    allocated_non_fnf += 1;
                    cid.raw()
                };

                let node = TransportNode {
                    method_tag,
                    http_version_tag,
                    _pad: [0; 2],
                    handler_id,
                    correlation_id,
                    path_offset: 0,
                    path_len: path_str.len() as u32,
                    query_offset: 0,
                    query_len: query_str.map_or(0, |q| q.len() as u32),
                    headers_offset: 0,
                    headers_len: headers_mp.len() as u32,
                    body_offset: 0,
                    body_len: body.len() as u32,
                };
                staged.push(StagedShmRequest {
                    node,
                    flags: req_flags,
                    slab_bytes: req_slab_bytes,
                    path_and_query,
                    headers_mp,
                    body,
                });
            }
        }

        if allocated_non_fnf > 0 {
            self.active_requests
                .0
                .fetch_add(allocated_non_fnf, Ordering::AcqRel);
        }

        if !rejected_replies.is_empty() {
            warn!(
                worker = worker_idx,
                rejected = rejected_replies.len(),
                "rejecting SHM requests exceeding slot or correlation capacity with 503"
            );
            for tx in rejected_replies {
                let _ = tx.send(service_unavailable_response());
            }
        }

        if staged.is_empty() {
            return 0;
        }

        let worker = &self.workers[worker_idx];
        let mut dispatched_count = 0usize;
        let mut start = 0usize;
        while start < staged.len() {
            let sub_flags = staged[start].flags;
            let mut end = start + 1;
            let mut sub_slab = staged[start].slab_bytes;

            while end < staged.len() && staged[end].flags == sub_flags {
                let next_slab = sub_slab + staged[end].slab_bytes;
                if batch_snapshot_serialized_size((end - start) + 1, next_slab) > max_payload {
                    break;
                }
                sub_slab = next_slab;
                end += 1;
            }

            let slice = &staged[start..end];
            let mut nodes = Vec::with_capacity(slice.len());
            let mut components = Vec::with_capacity(slice.len() * 4);
            for item in slice {
                nodes.push(item.node);
                components.push(item.path_and_query.path().as_bytes());
                if let Some(q) = item.path_and_query.query() {
                    components.push(q.as_bytes());
                }
                components.push(item.headers_mp.as_ref());
                components.push(item.body.as_ref());
            }
            let writer = ShmBatchWriter {
                nodes,
                components,
                flags: sub_flags,
            };

            match worker
                .ring_pair
                .request_ring
                .try_send_with(sub_flags, |slot| writer.serialize_into(slot))
            {
                Ok(()) => {
                    dispatched_count += slice.len();
                }
                Err(e) => {
                    debug!(worker = worker_idx, error = %e, "request ring send failed during sub-batch flush");
                    self.fail_staged_slice(slice);
                }
            }
            start = end;
        }

        if dispatched_count > 0 {
            let _ = worker.ring_pair.notify_request_if_sleeping();
        }

        dispatched_count
    }

    /// Access a specific correlation shard.
    pub fn correlation_shard(&self, shard_idx: usize) -> &Mutex<InFlightRequests> {
        &self.correlation_shards[shard_idx]
    }

    /// Get the number of correlation shards.
    pub const fn shard_count(&self) -> usize {
        SHM_CORRELATION_SHARDS
    }

    /// Sweep for expired entries across all shards, sending `504` outside shard locks.
    pub fn sweep_expired(&self, timeout: Duration) -> usize {
        let mut total = 0;
        for shard in &self.correlation_shards {
            let expired = {
                let mut slab = shard.lock().unwrap();
                slab.take_expired(timeout)
            };
            if !expired.is_empty() {
                total += expired.len();
                self.decrement_active(expired.len());
                for reply in expired {
                    let _ = reply.send(PendingResponse {
                        status: 504,
                        headers_mp: bytes::Bytes::new(),
                        body: bytes::Bytes::from_static(b"Gateway Timeout"),
                        stream_rx: None,
                    });
                }
            }
        }
        total
    }

    /// Sweep for entries targeting a specific dead worker, sending `502` outside shard locks.
    pub fn sweep_worker(&self, worker_id: u32) -> usize {
        let mut total = 0;
        for shard in &self.correlation_shards {
            let targets = {
                let mut slab = shard.lock().unwrap();
                slab.take_for_worker(worker_id)
            };
            if !targets.is_empty() {
                total += targets.len();
                self.decrement_active(targets.len());
                for reply in targets {
                    let _ = reply.send(PendingResponse {
                        status: 502,
                        headers_mp: bytes::Bytes::new(),
                        body: bytes::Bytes::from_static(b"Bad Gateway"),
                        stream_rx: None,
                    });
                }
            }
        }
        total
    }

    /// Complete a single correlation ID, releasing the shard lock before `oneshot::send`.
    #[inline]
    pub fn complete_one(&self, correlation_id: u64, response: PendingResponse) -> bool {
        let cid = CorrelationId::from_raw(correlation_id);
        let shard_idx = cid.shard() & SHM_CORRELATION_MASK;
        let tx = {
            let mut slab = self.correlation_shards[shard_idx].lock().unwrap();
            slab.take_completed(cid)
        };
        if let Some(tx) = tx {
            self.decrement_active(1);
            let _ = tx.send(response);
            true
        } else {
            false
        }
    }

    /// Complete multiple requests with grouped per-shard locks, sending outside locks.
    pub fn complete_many(&self, mut responses: Vec<(u64, PendingResponse)>) {
        if responses.is_empty() {
            return;
        }
        if responses.len() == 1 {
            let (cid, resp) = responses.pop().unwrap();
            if !self.complete_one(cid, resp) {
                debug!(id = cid, "no pending SHM request for correlation ID");
            }
            return;
        }

        let mut buckets: [Vec<(CorrelationId, PendingResponse)>; SHM_CORRELATION_SHARDS] =
            std::array::from_fn(|_| Vec::new());

        for (correlation_id, response) in responses {
            let cid = CorrelationId::from_raw(correlation_id);
            let shard_idx = cid.shard() & SHM_CORRELATION_MASK;
            buckets[shard_idx].push((cid, response));
        }

        let mut ready: Vec<(oneshot::Sender<PendingResponse>, PendingResponse)> = Vec::new();
        for (shard_idx, bucket) in buckets.into_iter().enumerate() {
            if bucket.is_empty() {
                continue;
            }
            ready.clear();
            {
                let mut slab = self.correlation_shards[shard_idx].lock().unwrap();
                for (cid, response) in bucket {
                    if let Some(tx) = slab.take_completed(cid) {
                        ready.push((tx, response));
                    } else {
                        debug!(
                            id = cid.raw(),
                            shard = shard_idx,
                            "no pending SHM request for correlation ID"
                        );
                    }
                }
            }
            if !ready.is_empty() {
                self.decrement_active(ready.len());
                for (tx, response) in ready.drain(..) {
                    let _ = tx.send(response);
                }
            }
        }
    }

    /// Get all ring pairs (for the response reader).
    pub fn ring_pairs(&self) -> Vec<Arc<RingPair>> {
        self.workers
            .iter()
            .map(|w| Arc::clone(&w.ring_pair))
            .collect()
    }

    /// Inspect an incoming `ResponseSlot` frame from SHM.
    pub fn handle_raw_response_slot(&self, raw_slot: &[u8]) -> bool {
        use bytes::Bytes;
        use http::{HeaderMap, HeaderName, HeaderValue};
        use hyper::body::Frame;
        use vertebra_core::ipc::{
            FLAG_ERROR, FLAG_FINAL, FLAG_HEADERS_SENT, FLAG_STREAMING, FLAG_TRAILERS, ResponseSlot,
        };

        let Some(correlation_id) = ResponseSlot::peek_request_id(raw_slot) else {
            return false;
        };
        let Some(flags) = ResponseSlot::peek_flags(raw_slot) else {
            return false;
        };
        let Ok(slot) = ResponseSlot::decode_from(raw_slot) else {
            return false;
        };

        let is_streaming = (flags & FLAG_STREAMING) != 0;
        let is_final = (flags & (FLAG_FINAL | FLAG_ERROR)) != 0;

        if !is_streaming || ((flags & FLAG_HEADERS_SENT) != 0 && is_final) {
            let encoded_headers: Vec<(Vec<u8>, Vec<u8>)> = slot
                .headers
                .into_iter()
                .map(|(k, v)| (k.into_bytes(), v.into_bytes()))
                .collect();
            let headers_mp = crate::response::encode_response_headers(&encoded_headers);
            return self.complete_one(
                correlation_id,
                PendingResponse::unary(slot.status, headers_mp, Bytes::from(slot.body)),
            );
        }

        if (flags & FLAG_HEADERS_SENT) != 0 {
            let mut raw_rx = self.pending_streams.register_stream(correlation_id, 64);
            let _ = self.pending_streams.deliver_raw_frame(raw_slot.to_vec());

            let encoded_headers: Vec<(Vec<u8>, Vec<u8>)> = slot
                .headers
                .into_iter()
                .map(|(k, v)| (k.into_bytes(), v.into_bytes()))
                .collect();
            let headers_mp = crate::response::encode_response_headers(&encoded_headers);
            let (frame_tx, frame_rx) = tokio::sync::mpsc::channel(64);

            tokio::spawn(async move {
                while let Some(raw_frame) = raw_rx.recv().await {
                    let Ok(chunk_slot) = ResponseSlot::decode_from(&raw_frame) else {
                        break;
                    };
                    if (chunk_slot.flags & FLAG_TRAILERS) != 0 && !chunk_slot.headers.is_empty() {
                        let mut trailers = HeaderMap::new();
                        for (k, v) in chunk_slot.headers {
                            if let (Ok(name), Ok(val)) = (
                                HeaderName::from_bytes(k.as_bytes()),
                                HeaderValue::from_str(&v),
                            ) {
                                trailers.insert(name, val);
                            }
                        }
                        if frame_tx.send(Ok(Frame::trailers(trailers))).await.is_err() {
                            break;
                        }
                    } else if !chunk_slot.body.is_empty()
                        && frame_tx
                            .send(Ok(Frame::data(Bytes::from(chunk_slot.body))))
                            .await
                            .is_err()
                    {
                        break;
                    }
                    if (chunk_slot.flags & (FLAG_FINAL | FLAG_ERROR)) != 0 {
                        break;
                    }
                }
            });

            return self.complete_one(
                correlation_id,
                PendingResponse::streaming(slot.status, headers_mp, frame_rx),
            );
        }

        self.pending_streams.deliver_raw_frame(raw_slot.to_vec())
    }

    fn fail_staged_slice(&self, slice: &[StagedShmRequest]) {
        use vertebra_core::protocol::flags;
        for item in slice {
            if (item.flags & flags::FIRE_AND_FORGET) == 0 {
                let _ = self.complete_one(item.node.correlation_id, service_unavailable_response());
            }
        }
    }
}
