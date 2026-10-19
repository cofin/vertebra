//! Shared-memory dispatcher: serializes requests into per-worker MPMC rings
//! and tracks correlation via a sharded O(1) slab.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing::{debug, error, info, warn};

use vertebra_core::transport::ring_pair::RingPair;
use vertebra_core::transport::{ShmBatchWriter, Transport, TransportNode};

use super::types::{
    ShmDispatchBatch, TransportRequestView, WorkerState, batch_can_fit_request,
    batch_snapshot_serialized_size, request_slab_size, service_unavailable_response,
    worker_ring_has_capacity,
};
use crate::collector::Batch;
use crate::correlation::CorrelationSlab;

/// SharedMemory dispatcher with round-robin routing and sharded O(1) correlation.
pub struct ShmDispatcher {
    workers: Vec<WorkerState>,
    next_worker: AtomicUsize,
    correlation_shards: Vec<Mutex<CorrelationSlab>>,
    shard_mask: usize,
}

impl ShmDispatcher {
    /// Create a new SHM dispatcher.
    pub fn new(ring_pairs: Vec<Arc<RingPair>>, max_inflight: usize) -> Arc<Self> {
        let workers: Vec<WorkerState> = ring_pairs
            .into_iter()
            .map(|rp| WorkerState {
                transport: Transport::new_shared_memory(Arc::clone(&rp)),
                ring_pair: rp,
            })
            .collect();

        let shard_count = 8;
        let shard_mask = shard_count - 1;
        let shard_capacity = max_inflight / shard_count;

        let correlation_shards = (0..shard_count)
            .map(|_| Mutex::new(CorrelationSlab::new(shard_capacity)))
            .collect();

        let this = Arc::new(Self {
            workers,
            next_worker: AtomicUsize::new(0),
            correlation_shards,
            shard_mask,
        });

        // Spawn periodic sweep task (1s intervals, 30s timeout)
        let sweep_inner = Arc::clone(&this);
        tokio::spawn(async move {
            info!("SHM correlation sweep task started");
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            let timeout = Duration::from_secs(30);
            loop {
                interval.tick().await;
                sweep_inner.sweep_expired(timeout);
            }
        });

        this
    }

    /// Dispatch a batch to workers via MPMC rings.
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
        let mut pending_batches: HashMap<(usize, u32), ShmDispatchBatch> = HashMap::new();
        let mut dispatched_count = 0;

        for (offset, req) in batch.requests.into_iter().enumerate() {
            let crate::collector::PendingRequest {
                handler_id,
                method_tag,
                http_version_tag,
                flags: req_flags,
                path,
                query,
                headers_mp,
                body,
                reply,
            } = req;
            let request_view = TransportRequestView {
                path: &path,
                query: query.as_deref(),
                headers_mp: &headers_mp,
                body: &body,
            };
            let is_fnf = req_flags & flags::FIRE_AND_FORGET != 0;
            let mut reply = Some(reply);

            let preferred_worker = (start + offset) % worker_count;
            let worker_idx = if pending_batches.contains_key(&(preferred_worker, req_flags)) {
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

            let batch_key = (worker_idx, req_flags);
            let max_payload = self.workers[worker_idx]
                .ring_pair
                .request_ring
                .payload_capacity();
            if let Some(pending) = pending_batches.get(&batch_key)
                && !batch_can_fit_request(pending, &request_view, max_payload)
            {
                let flushed = pending_batches
                    .remove(&batch_key)
                    .expect("pending SHM batch should exist");
                dispatched_count += self.flush_shm_dispatch_batch(worker_idx, flushed);
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
                let shard_idx = (start + offset) & self.shard_mask;
                let mut slab = self.correlation_shards[shard_idx].lock().unwrap();
                if slab.in_flight() >= slab.capacity() {
                    drop(slab);
                    warn!(
                        shard = shard_idx,
                        "correlation shard full, responding with 503"
                    );
                    if let Some(reply) = reply.take() {
                        let _ = reply.send(service_unavailable_response());
                    }
                    continue;
                }
                let internal_id = slab
                    .insert(
                        reply.take().expect("reply must be present"),
                        worker_idx as u32,
                    )
                    .expect("correlation slot should be available after capacity check");

                (internal_id << 8) | (shard_idx as u64)
            };

            let pending = pending_batches
                .entry(batch_key)
                .or_insert_with(|| ShmDispatchBatch::new(req_flags));

            pending.nodes.push(TransportNode {
                method_tag,
                http_version_tag,
                _pad: [0; 2],
                handler_id,
                correlation_id,
                path_offset: 0,
                path_len: path.len() as u32,
                query_offset: 0,
                query_len: query.as_ref().map_or(0, |q| q.len() as u32),
                headers_offset: 0,
                headers_len: headers_mp.len() as u32,
                body_offset: 0,
                body_len: body.len() as u32,
            });
            pending.slab_len += req_slab_bytes;
            pending.components.push(bytes::Bytes::from(path));
            if let Some(q) = query {
                pending.components.push(bytes::Bytes::from(q));
            }
            pending.components.push(headers_mp);
            pending.components.push(body);
            if !is_fnf {
                pending.correlation_ids.push(correlation_id);
            }
        }

        for ((worker_idx, _flags), pending) in pending_batches {
            dispatched_count += self.flush_shm_dispatch_batch(worker_idx, pending);
        }

        dispatched_count
    }

    /// Access a specific correlation shard.
    pub fn correlation_shard(&self, shard_idx: usize) -> &Mutex<CorrelationSlab> {
        &self.correlation_shards[shard_idx]
    }

    /// Get the number of correlation shards.
    pub fn shard_count(&self) -> usize {
        self.correlation_shards.len()
    }

    /// Sweep for expired entries across all shards.
    pub fn sweep_expired(&self, timeout: Duration) {
        for shard in &self.correlation_shards {
            let mut slab = shard.lock().unwrap();
            slab.sweep_expired(timeout);
        }
    }

    /// Get all ring pairs (for the response reader).
    pub fn ring_pairs(&self) -> Vec<Arc<RingPair>> {
        self.workers
            .iter()
            .map(|w| Arc::clone(&w.ring_pair))
            .collect()
    }

    fn flush_shm_dispatch_batch(&self, worker_idx: usize, pending: ShmDispatchBatch) -> usize {
        let worker = &self.workers[worker_idx];
        let node_count = pending.nodes.len();
        let writer = ShmBatchWriter {
            nodes: pending.nodes,
            components: pending.components.iter().map(|c| c.as_ref()).collect(),
            flags: pending.flags,
        };

        // Final optimization: Zero-alloc serialization directly into ring slot.
        match worker
            .ring_pair
            .request_ring
            .try_send_with(pending.flags, |slot| writer.serialize_into(slot))
        {
            Ok(()) => {
                // PERF: Lazy Notify — only signal when the ring transitioned from empty to non-empty.
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
        for &cid in correlation_ids {
            let shard_idx = (cid & 0xFF) as usize;
            let internal_id = cid >> 8;
            let mut slab = self.correlation_shards[shard_idx].lock().unwrap();
            slab.complete(internal_id, service_unavailable_response());
        }
    }
}
