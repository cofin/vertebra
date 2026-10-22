//! Shared-memory dispatcher: serializes requests into per-worker MPMC rings
//! and tracks correlation via a sharded O(1) slab.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use vertebra_core::transport::ring_pair::RingPair;
use vertebra_core::transport::{ShmBatchWriter, Transport, TransportNode};

use super::shm_reader::PendingRequests;
use super::types::{
    ShmBatchScratch, ShmDispatchBatch, TransportRequestView, WorkerState, batch_can_fit_request,
    batch_snapshot_serialized_size, request_slab_size, service_unavailable_response,
    worker_ring_has_capacity,
};
use crate::batcher::Batch;
use crate::body::PendingResponse;
use crate::dispatcher::correlation::{CorrelationId, InFlightRequests};

/// SharedMemory dispatcher with round-robin routing and sharded O(1) correlation.
pub struct ShmDispatcher {
    workers: Vec<WorkerState>,
    next_worker: AtomicUsize,
    next_batch: AtomicUsize,
    correlation_shards: Vec<Mutex<InFlightRequests>>,
    shard_mask: usize,
    scratch: Mutex<ShmBatchScratch>,
    pending_streams: Arc<PendingRequests>,
}

impl ShmDispatcher {
    /// Create a new SHM dispatcher without spawning background tasks.
    pub fn new(ring_pairs: Vec<Arc<RingPair>>, max_inflight: usize) -> Arc<Self> {
        let workers: Vec<WorkerState> = ring_pairs
            .into_iter()
            .map(|rp| WorkerState {
                transport: Transport::new_shared_memory(Arc::clone(&rp)),
                ring_pair: rp,
            })
            .collect();

        let worker_count = workers.len();
        let shard_count = 8;
        let shard_mask = shard_count - 1;
        let shard_capacity = max_inflight / shard_count;

        let correlation_shards = (0..shard_count)
            .map(|_| Mutex::new(InFlightRequests::new(shard_capacity)))
            .collect();

        Arc::new(Self {
            workers,
            next_worker: AtomicUsize::new(0),
            next_batch: AtomicUsize::new(0),
            correlation_shards,
            shard_mask,
            scratch: Mutex::new(ShmBatchScratch::new(worker_count)),
            pending_streams: Arc::new(PendingRequests::new()),
        })
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

    /// Dispatch a batch to workers via MPMC rings with single-lock per batch.
    pub fn dispatch_shm(&self, batch: Batch) -> usize {
        use vertebra_core::protocol::flags;

        let worker_count = self.workers.len();
        if worker_count == 0 {
            error!("no SHM workers configured, dropping batch");
            return 0;
        }

        let start = self
            .next_worker
            .fetch_add(batch.requests.len().max(1), Ordering::Relaxed);
        let batch_seq = self.next_batch.fetch_add(1, Ordering::Relaxed);
        let mut shard_idx = batch_seq & self.shard_mask;

        let mut scratch = self.scratch.lock().unwrap();
        scratch.clear();

        let mut dispatched_count = 0;
        let mut shard_guard = self.correlation_shards[shard_idx].lock().unwrap();

        for (offset, req) in batch.requests.into_iter().enumerate() {
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
            let request_view = TransportRequestView {
                path: path_str,
                query: query_str,
                headers_mp: &headers_mp,
                body: &body,
            };
            let is_fnf = req_flags & flags::FIRE_AND_FORGET != 0;
            let mut reply = Some(reply);

            let preferred_worker = (start + offset) % worker_count;
            let worker_idx = if !scratch.batches[preferred_worker].is_empty() {
                Some(preferred_worker)
            } else {
                (0..worker_count)
                    .map(|attempt| (preferred_worker + attempt) % worker_count)
                    .find(|&idx| worker_ring_has_capacity(&self.workers[idx]))
            };

            let Some(worker_idx) = worker_idx else {
                warn!("all worker rings full, responding with 503");
                if !is_fnf && let Some(reply) = reply.take() {
                    let _ = reply.send(service_unavailable_response());
                }
                continue;
            };

            let max_payload = self.workers[worker_idx]
                .ring_pair
                .request_ring
                .payload_capacity();

            let pending = &mut scratch.batches[worker_idx];
            if !pending.is_empty()
                && (pending.flags != req_flags
                    || !batch_can_fit_request(pending, &request_view, max_payload))
            {
                dispatched_count += self.flush_shm_dispatch_batch(worker_idx, pending);
                pending.clear();
            }

            let req_slab_bytes = request_slab_size(&request_view);
            if batch_snapshot_serialized_size(1, req_slab_bytes) > max_payload {
                warn!(
                    worker = worker_idx,
                    "request batch too large for SHM slot, responding with 503"
                );
                if !is_fnf && let Some(reply) = reply.take() {
                    let _ = reply.send(service_unavailable_response());
                }
                continue;
            }

            let correlation_id = if is_fnf {
                0
            } else {
                if shard_guard.in_flight() >= shard_guard.capacity() {
                    drop(shard_guard);
                    let mut found = None;
                    for attempt in 1..self.shard_count() {
                        let alt_idx = (shard_idx + attempt) & self.shard_mask;
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
                        warn!(
                            shard = shard_idx,
                            "all correlation shards full, responding with 503"
                        );
                        if let Some(reply) = reply.take() {
                            let _ = reply.send(service_unavailable_response());
                        }
                        continue;
                    }
                }
                let cid = shard_guard
                    .insert(
                        shard_idx as u8,
                        reply.take().expect("reply must be present"),
                        worker_idx as u32,
                    )
                    .expect("correlation slot should be available after capacity check");

                cid.raw()
            };

            let pending = &mut scratch.batches[worker_idx];
            pending.flags = req_flags;
            pending.nodes.push(TransportNode {
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
            });
            pending.slab_len += req_slab_bytes;
            pending
                .components
                .push(bytes::Bytes::copy_from_slice(path_str.as_bytes()));
            if let Some(q) = query_str {
                pending
                    .components
                    .push(bytes::Bytes::copy_from_slice(q.as_bytes()));
            }
            pending.components.push(headers_mp);
            pending.components.push(body);
            if !is_fnf {
                pending.correlation_ids.push(correlation_id);
            }
        }

        drop(shard_guard);

        for (worker_idx, pending) in scratch.batches.iter_mut().enumerate() {
            if !pending.is_empty() {
                dispatched_count += self.flush_shm_dispatch_batch(worker_idx, pending);
                pending.clear();
            }
        }

        dispatched_count
    }

    /// Access a specific correlation shard.
    pub fn correlation_shard(&self, shard_idx: usize) -> &Mutex<InFlightRequests> {
        &self.correlation_shards[shard_idx]
    }

    /// Get the number of correlation shards.
    pub fn shard_count(&self) -> usize {
        self.correlation_shards.len()
    }

    /// Sweep for expired entries across all shards.
    pub fn sweep_expired(&self, timeout: Duration) -> usize {
        let mut total = 0;
        for shard in &self.correlation_shards {
            let mut slab = shard.lock().unwrap();
            total += slab.sweep_expired(timeout);
        }
        total
    }

    /// Sweep for entries targeting a specific dead worker.
    pub fn sweep_worker(&self, worker_id: u32) -> usize {
        let mut total = 0;
        for shard in &self.correlation_shards {
            let mut slab = shard.lock().unwrap();
            total += slab.sweep_for_worker(worker_id);
        }
        total
    }

    /// Complete multiple requests with grouped per-shard locks.
    pub fn complete_many(&self, responses: Vec<(u64, PendingResponse)>) {
        if responses.is_empty() {
            return;
        }
        let shard_count = self.shard_count();
        let shard_mask = self.shard_mask;
        let mut buckets: Vec<Vec<(CorrelationId, PendingResponse)>> =
            (0..shard_count).map(|_| Vec::new()).collect();

        for (correlation_id, response) in responses {
            let cid = CorrelationId::from_raw(correlation_id);
            let shard_idx = cid.shard() & shard_mask;
            buckets[shard_idx].push((cid, response));
        }

        for (shard_idx, bucket) in buckets.into_iter().enumerate() {
            if bucket.is_empty() {
                continue;
            }
            let mut slab = self.correlation_shards[shard_idx].lock().unwrap();
            for (cid, response) in bucket {
                if !slab.complete(cid, response) {
                    debug!(
                        id = cid.raw(),
                        shard = shard_idx,
                        "no pending SHM request for correlation ID"
                    );
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
            let cid = CorrelationId::from_raw(correlation_id);
            let shard_idx = cid.shard() & self.shard_mask;
            let mut slab = self.correlation_shards[shard_idx].lock().unwrap();
            return slab.complete(
                cid,
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

            let cid = CorrelationId::from_raw(correlation_id);
            let shard_idx = cid.shard() & self.shard_mask;
            let mut slab = self.correlation_shards[shard_idx].lock().unwrap();
            return slab.complete(
                cid,
                PendingResponse::streaming(slot.status, headers_mp, frame_rx),
            );
        }

        self.pending_streams.deliver_raw_frame(raw_slot.to_vec())
    }

    fn flush_shm_dispatch_batch(&self, worker_idx: usize, pending: &ShmDispatchBatch) -> usize {
        let worker = &self.workers[worker_idx];
        let node_count = pending.nodes.len();
        let writer = ShmBatchWriter {
            nodes: pending.nodes.clone(),
            components: pending.components.iter().map(|c| c.as_ref()).collect(),
            flags: pending.flags,
        };

        match worker
            .ring_pair
            .request_ring
            .try_send_with(pending.flags, |slot| writer.serialize_into(slot))
        {
            Ok(()) => {
                let stats = worker.ring_pair.request_ring.stats();
                if stats.write_head == stats.read_head.wrapping_add(1) {
                    let _ = worker.transport.notify_request();
                }
                node_count
            }
            Err(vertebra_core::channel::ChannelError::Full) => {
                debug!(worker = worker_idx, "request ring full during flush");
                self.fail_correlations(&pending.correlation_ids);
                0
            }
            Err(e) => {
                error!(worker = worker_idx, error = %e, "ring send error");
                self.fail_correlations(&pending.correlation_ids);
                0
            }
        }
    }

    fn fail_correlations(&self, correlation_ids: &[u64]) {
        if correlation_ids.is_empty() {
            return;
        }
        for &cid_raw in correlation_ids {
            let cid = CorrelationId::from_raw(cid_raw);
            let shard_idx = cid.shard() & self.shard_mask;
            let mut slab = self.correlation_shards[shard_idx].lock().unwrap();
            slab.complete(cid, service_unavailable_response());
        }
    }
}
