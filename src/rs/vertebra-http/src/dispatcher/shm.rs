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

use super::streams::{DEFAULT_STREAM_QUEUE_DEPTH, ResponseStreamRegistry};
use super::types::{
    TransportRequestView, WorkerState, batch_snapshot_serialized_size, request_slab_size,
    service_unavailable_response, worker_ring_has_capacity,
};
use crate::batcher::Batch;
use crate::body::PendingResponse;
use crate::dispatcher::correlation::{CorrelationId, InFlightRequests};
use vertebra_core::bridge::{ResponseStreamFrame, StreamDelivery};

const SHM_CORRELATION_SHARDS: usize = 8;
const SHM_CORRELATION_MASK: usize = SHM_CORRELATION_SHARDS - 1;

/// Failure reason when dispatching a targeted single request to a specific SHM worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShmTargetError {
    /// `worker_id` is out of range (`worker_id >= worker_count`).
    NotFound,
    /// Target worker is currently marked unhealthy.
    Unhealthy,
    /// Target worker request ring has no available entry.
    RingFull,
    /// All correlation shards are at capacity.
    CorrelationFull,
    /// Serialized request exceeds ring payload capacity or failed ring transmission.
    SendFailed,
}

struct StagedShmRequest {
    node: TransportNode,
    flags: u32,
    slab_bytes: usize,
    path_and_query: http::uri::PathAndQuery,
    headers_mp: bytes::Bytes,
    body: bytes::Bytes,
}

/// Reusable per-batcher scratch storage for `ShmDispatcher` to avoid per-batch heap allocations.
#[derive(Default)]
pub struct ShmDispatchScratch {
    staged: Vec<StagedShmRequest>,
    nodes: Vec<TransportNode>,
    components: Vec<&'static [u8]>,
    rejected_replies: Vec<oneshot::Sender<PendingResponse>>,
}

impl ShmDispatchScratch {
    #[inline]
    fn reclaim_writer(
        nodes_slot: &mut Vec<TransportNode>,
        components_slot: &mut Vec<&'static [u8]>,
        writer: ShmBatchWriter<'_>,
    ) {
        let ShmBatchWriter {
            mut nodes,
            components,
            ..
        } = writer;
        nodes.clear();
        *nodes_slot = nodes;
        *components_slot = recycle_slice_vec(components);
    }
}

/// Reclaim the heap allocation of an empty `Vec<&'a [u8]>` as `Vec<&'b [u8]>` without unsafe code.
#[inline]
fn recycle_slice_vec<'a, 'b>(mut v: Vec<&'a [u8]>) -> Vec<&'b [u8]> {
    v.clear();
    v.into_iter().map(|_| -> &'b [u8] { &[] }).collect()
}

/// SharedMemory dispatcher with whole-batch routing, warm-worker affinity, and sharded O(1) correlation.
pub struct ShmDispatcher {
    workers: Vec<WorkerState>,
    last_worker: AtomicUsize,
    next_worker: AtomicUsize,
    next_batch: AtomicUsize,
    active_requests: CacheLine<AtomicUsize>,
    correlation_shards: [Mutex<InFlightRequests>; SHM_CORRELATION_SHARDS],
    streams: ResponseStreamRegistry,
    lease_table: Mutex<Option<Arc<vertebra_core::broker::ShmLeaseTable>>>,
}

impl ShmDispatcher {
    /// Create a new SHM dispatcher without spawning background tasks.
    pub fn new(ring_pairs: Vec<Arc<RingPair>>, max_inflight: usize) -> Arc<Self> {
        Self::new_with_queue_depth(ring_pairs, max_inflight, DEFAULT_STREAM_QUEUE_DEPTH)
    }

    /// Create a new SHM dispatcher with a configured per-stream frame queue depth.
    pub fn new_with_queue_depth(
        ring_pairs: Vec<Arc<RingPair>>,
        max_inflight: usize,
        stream_queue_depth: usize,
    ) -> Arc<Self> {
        let workers: Vec<WorkerState> = ring_pairs
            .into_iter()
            .map(|rp| WorkerState {
                ring_pair: rp,
                healthy: std::sync::atomic::AtomicBool::new(true),
                in_flight: CacheLine(AtomicUsize::new(0)),
            })
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
            streams: ResponseStreamRegistry::with_capacity(stream_queue_depth),
            lease_table: Mutex::new(None),
        })
    }

    /// Update the dispatch health status of `worker_id`.
    ///
    /// Unhealthy workers are skipped by least-loaded worker selection until
    /// their replacement process finishes respawning.
    pub fn set_worker_healthy(&self, worker_id: usize, healthy: bool) {
        if let Some(worker) = self.workers.get(worker_id) {
            worker.healthy.store(healthy, Ordering::Release);
        }
    }

    /// Return whether `worker_id` is currently marked healthy for dispatch.
    pub fn is_worker_healthy(&self, worker_id: usize) -> bool {
        self.workers
            .get(worker_id)
            .is_some_and(|w| w.healthy.load(Ordering::Acquire))
    }

    /// Return the total number of configured SHM workers.
    #[inline]
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Return the IDs of all SHM workers currently marked healthy, in ascending order.
    pub fn healthy_worker_ids(&self) -> Vec<usize> {
        (0..self.workers.len())
            .filter(|&id| self.is_worker_healthy(id))
            .collect()
    }

    /// Dispatch a single request directly to a specific SHM worker without mutating
    /// warm-worker affinity (`last_worker` / `next_worker`).
    pub fn dispatch_to_worker(
        &self,
        worker_id: usize,
        req: crate::batcher::PendingRequest,
    ) -> Result<u64, ShmTargetError> {
        use vertebra_core::protocol::flags;

        let Some(worker) = self.workers.get(worker_id) else {
            return Err(ShmTargetError::NotFound);
        };
        if !worker.healthy.load(Ordering::Acquire) {
            return Err(ShmTargetError::Unhealthy);
        }
        if !worker.ring_pair.request_ring.has_capacity() {
            return Err(ShmTargetError::RingFull);
        }

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
        let max_payload = worker.ring_pair.request_ring.payload_capacity();
        if batch_snapshot_serialized_size(1, req_slab_bytes) > max_payload {
            return Err(ShmTargetError::SendFailed);
        }

        let is_fnf = (req_flags & flags::FIRE_AND_FORGET) != 0;
        let correlation_id = if is_fnf {
            0
        } else {
            let start_shard = worker_id & SHM_CORRELATION_MASK;
            let mut reply_opt = Some(reply);
            let mut allocated = None;
            for attempt in 0..SHM_CORRELATION_SHARDS {
                let shard_idx = (start_shard + attempt) & SHM_CORRELATION_MASK;
                let mut shard_guard = self.correlation_shards[shard_idx].lock().unwrap();
                if shard_guard.in_flight() < shard_guard.capacity() {
                    let cid = shard_guard
                        .insert(shard_idx as u8, reply_opt.take().unwrap(), worker_id as u32)
                        .expect("correlation slot should be available after capacity check");
                    allocated = Some(cid.raw());
                    break;
                }
            }
            let Some(cid_raw) = allocated else {
                return Err(ShmTargetError::CorrelationFull);
            };
            self.active_requests.0.fetch_add(1, Ordering::AcqRel);
            worker.in_flight.0.fetch_add(1, Ordering::AcqRel);
            cid_raw
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

        let mut components = Vec::with_capacity(4);
        components.push(path_str.as_bytes());
        if let Some(q) = query_str {
            components.push(q.as_bytes());
        }
        components.push(headers_mp.as_ref());
        components.push(body.as_ref());

        let mut writer = ShmBatchWriter {
            nodes: vec![node],
            components,
            flags: req_flags,
        };

        match worker
            .ring_pair
            .request_ring
            .try_send_with(req_flags, |slot| writer.serialize_into(slot))
        {
            Ok(()) => {
                let _ = worker.ring_pair.notify_request_if_sleeping();
                Ok(correlation_id)
            }
            Err(_) => {
                if !is_fnf {
                    let _ = self.cancel_correlation(correlation_id);
                }
                Err(ShmTargetError::RingFull)
            }
        }
    }

    /// Cancel a pending correlation ID without sending a response on its reply channel.
    ///
    /// Returns `true` if the correlation entry was present and removed, or `false` if it
    /// had already been completed or cancelled.
    #[inline]
    pub fn cancel_correlation(&self, correlation_id: u64) -> bool {
        let cid = CorrelationId::from_raw(correlation_id);
        let shard_idx = cid.shard() & SHM_CORRELATION_MASK;
        let removed = {
            let mut slab = self.correlation_shards[shard_idx].lock().unwrap();
            slab.take_completed_with_worker(cid)
        };
        if let Some((reply, worker_id)) = removed {
            drop(reply);
            self.decrement_active(1);
            self.decrement_worker_active(worker_id as usize, 1);
            true
        } else {
            false
        }
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

    #[inline]
    fn decrement_worker_active(&self, worker_idx: usize, count: usize) {
        if count == 0 {
            return;
        }
        if let Some(worker) = self.workers.get(worker_idx) {
            let _ = worker
                .in_flight
                .0
                .try_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                    Some(cur.saturating_sub(count))
                });
        }
    }

    /// Return the current number of in-flight requests assigned to `worker_id`.
    #[inline]
    pub fn worker_in_flight(&self, worker_id: usize) -> usize {
        self.workers
            .get(worker_id)
            .map_or(0, |w| w.in_flight.0.load(Ordering::Acquire))
    }

    /// Return the total number of in-flight requests across all SHM workers.
    #[inline]
    pub fn in_flight(&self) -> usize {
        self.active_requests.0.load(Ordering::Relaxed)
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
        self.dispatch_shm_vec_from_shard(None, requests)
    }

    #[inline]
    fn select_worker(&self, batcher_shard: Option<usize>, prev_active: usize) -> Option<usize> {
        let worker_count = self.workers.len();
        if worker_count == 0 {
            return None;
        }
        if worker_count == 1 {
            return worker_ring_has_capacity(&self.workers[0]).then_some(0);
        }

        if let Some(shard) = batcher_shard {
            if prev_active == 0 {
                let last = self.last_worker.load(Ordering::Relaxed) % worker_count;
                if worker_ring_has_capacity(&self.workers[last]) {
                    return Some(last);
                }
            }

            let home = shard % worker_count;
            let home_worker = &self.workers[home];
            if worker_ring_has_capacity(home_worker)
                && home_worker.in_flight.0.load(Ordering::Relaxed) <= 64
            {
                return Some(home);
            }

            return (0..worker_count)
                .map(|attempt| (home + attempt) % worker_count)
                .filter(|&idx| worker_ring_has_capacity(&self.workers[idx]))
                .min_by_key(|&idx| self.workers[idx].in_flight.0.load(Ordering::Relaxed));
        }

        let preferred_worker = if prev_active == 0 {
            self.last_worker.load(Ordering::Relaxed) % worker_count
        } else {
            self.next_worker.fetch_add(1, Ordering::Relaxed) % worker_count
        };

        (0..worker_count)
            .map(|attempt| (preferred_worker + attempt) % worker_count)
            .find(|&idx| worker_ring_has_capacity(&self.workers[idx]))
    }

    /// Dispatch requests from an optional batcher shard, preserving sticky shard-to-worker affinity.
    pub fn dispatch_shm_vec_from_shard(
        &self,
        batcher_shard: Option<usize>,
        requests: &mut Vec<crate::batcher::PendingRequest>,
    ) -> usize {
        let mut scratch = ShmDispatchScratch::default();
        self.dispatch_shm_vec_from_shard_with_scratch(batcher_shard, requests, &mut scratch)
    }

    /// Dispatch requests from an optional batcher shard using caller-owned reusable scratch buffers.
    pub fn dispatch_shm_vec_from_shard_with_scratch(
        &self,
        batcher_shard: Option<usize>,
        requests: &mut Vec<crate::batcher::PendingRequest>,
        scratch: &mut ShmDispatchScratch,
    ) -> usize {
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
        let worker_idx = self.select_worker(batcher_shard, prev_active);

        let Some(worker_idx) = worker_idx else {
            warn!("all worker rings full, responding with 503");
            for req in requests.drain(..) {
                if (req.flags & flags::FIRE_AND_FORGET) == 0 {
                    let _ = req.reply.send(service_unavailable_response());
                }
            }
            return 0;
        };

        if batcher_shard.is_none() && prev_active == 0 {
            self.next_worker
                .store(worker_idx.wrapping_add(1), Ordering::Relaxed);
        }
        self.last_worker.store(worker_idx, Ordering::Relaxed);

        let max_payload = self.workers[worker_idx]
            .ring_pair
            .request_ring
            .payload_capacity();

        let batch_len = requests.len();
        scratch.staged.clear();
        scratch.staged.reserve(batch_len);
        scratch.rejected_replies.clear();
        let mut allocated_non_fnf = 0usize;

        let mut shard_idx = if batcher_shard.is_some() {
            worker_idx & SHM_CORRELATION_MASK
        } else {
            self.next_batch.fetch_add(1, Ordering::Relaxed) & SHM_CORRELATION_MASK
        };

        let now = std::time::Instant::now();
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
                #[cfg(not(unix))]
                if batch_snapshot_serialized_size(1, req_slab_bytes) > max_payload {
                    if !is_fnf {
                        scratch.rejected_replies.push(reply);
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
                                scratch.rejected_replies.push(tx);
                            }
                            continue;
                        }
                    }
                    let cid = shard_guard
                        .insert_with_now(
                            shard_idx as u8,
                            reply_opt.take().unwrap(),
                            worker_idx as u32,
                            now,
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
                scratch.staged.push(StagedShmRequest {
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
            self.workers[worker_idx]
                .in_flight
                .0
                .fetch_add(allocated_non_fnf, Ordering::AcqRel);
        }

        if !scratch.rejected_replies.is_empty() {
            warn!(
                worker = worker_idx,
                rejected = scratch.rejected_replies.len(),
                "rejecting SHM requests exceeding slot or correlation capacity with 503"
            );
            for tx in scratch.rejected_replies.drain(..) {
                let _ = tx.send(service_unavailable_response());
            }
        }

        if scratch.staged.is_empty() {
            return 0;
        }

        let worker = &self.workers[worker_idx];
        let mut dispatched_count = 0usize;
        let mut start = 0usize;
        let staged_len = scratch.staged.len();
        while start < staged_len {
            let sub_flags = scratch.staged[start].flags;
            let mut end = start + 1;
            let mut sub_slab = scratch.staged[start].slab_bytes;

            while end < staged_len && scratch.staged[end].flags == sub_flags {
                let next_slab = sub_slab + scratch.staged[end].slab_bytes;
                if batch_snapshot_serialized_size((end - start) + 1, next_slab) > max_payload {
                    break;
                }
                sub_slab = next_slab;
                end += 1;
            }

            let slice = &scratch.staged[start..end];
            let mut nodes = std::mem::take(&mut scratch.nodes);
            nodes.clear();
            nodes.reserve(slice.len());
            let mut components = recycle_slice_vec(std::mem::take(&mut scratch.components));
            components.reserve(slice.len() * 4);
            for item in slice {
                nodes.push(item.node);
                components.push(item.path_and_query.path().as_bytes());
                if let Some(q) = item.path_and_query.query() {
                    components.push(q.as_bytes());
                }
                components.push(item.headers_mp.as_ref());
                components.push(item.body.as_ref());
            }
            let mut writer = ShmBatchWriter {
                nodes,
                components,
                flags: sub_flags,
            };

            #[cfg(unix)]
            if writer.serialized_size() > max_payload {
                match writer.write_overflow_envelope() {
                    Ok((overflow_name, envelope)) => {
                        match worker.ring_pair.request_ring.try_send_with(
                            sub_flags | flags::SHM_OVERFLOW,
                            |slot| {
                                slot[..envelope.len()].copy_from_slice(&envelope);
                                envelope.len()
                            },
                        ) {
                            Ok(()) => {
                                dispatched_count += slice.len();
                            }
                            Err(e) => {
                                let _ = vertebra_core::shm::shm_unlink_named(&overflow_name);
                                debug!(
                                    worker = worker_idx,
                                    error = %e,
                                    "request ring send failed during overflow sub-batch flush"
                                );
                                self.fail_staged_slice(slice);
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            worker = worker_idx,
                            error = %e,
                            "failed to allocate SHM overflow region for oversized request"
                        );
                        self.fail_staged_slice(slice);
                    }
                }
                ShmDispatchScratch::reclaim_writer(
                    &mut scratch.nodes,
                    &mut scratch.components,
                    writer,
                );
                start = end;
                continue;
            }

            match worker
                .ring_pair
                .request_ring
                .try_send_with(sub_flags, |slot| writer.serialize_into(slot))
            {
                Ok(()) => {
                    dispatched_count += slice.len();
                }
                Err(e) => {
                    debug!(
                        worker = worker_idx,
                        error = %e,
                        "request ring send failed during sub-batch flush"
                    );
                    self.fail_staged_slice(slice);
                }
            }
            ShmDispatchScratch::reclaim_writer(&mut scratch.nodes, &mut scratch.components, writer);
            start = end;
        }

        scratch.staged.clear();

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
        self.streams.sweep_closed();
        let mut total = 0;
        for shard in &self.correlation_shards {
            let expired = {
                let mut slab = shard.lock().unwrap();
                slab.take_expired_with_worker(timeout)
            };
            if !expired.is_empty() {
                total += expired.len();
                self.decrement_active(expired.len());
                for (reply, worker_id) in expired {
                    self.decrement_worker_active(worker_id as usize, 1);
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
                self.decrement_worker_active(worker_id as usize, targets.len());
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
        total + self.streams.close_worker(worker_id)
    }

    /// Complete a single correlation ID, releasing the shard lock before `oneshot::send`.
    #[inline]
    pub fn complete_one(&self, correlation_id: u64, response: PendingResponse) -> bool {
        let cid = CorrelationId::from_raw(correlation_id);
        let shard_idx = cid.shard() & SHM_CORRELATION_MASK;
        let completed = {
            let mut slab = self.correlation_shards[shard_idx].lock().unwrap();
            let completed = slab.take_completed_with_worker(cid);
            if response.stream_rx.is_some()
                && let Some((_, worker_id)) = &completed
            {
                self.streams.set_worker(correlation_id, *worker_id);
            }
            completed
        };
        if let Some((tx, worker_id)) = completed {
            self.decrement_active(1);
            self.decrement_worker_active(worker_id as usize, 1);
            let _ = tx.send(response);
            true
        } else {
            false
        }
    }

    #[inline]
    fn flush_completed_ready(
        &self,
        ready: &mut Vec<(oneshot::Sender<PendingResponse>, PendingResponse, u32)>,
    ) {
        if ready.is_empty() {
            return;
        }
        self.decrement_active(ready.len());
        let mut batch_worker = u32::MAX;
        let mut batch_worker_count = 0usize;
        for (tx, response, worker_id) in ready.drain(..) {
            if worker_id == batch_worker {
                batch_worker_count += 1;
            } else {
                if batch_worker_count > 0 {
                    self.decrement_worker_active(batch_worker as usize, batch_worker_count);
                }
                batch_worker = worker_id;
                batch_worker_count = 1;
            }
            let _ = tx.send(response);
        }
        if batch_worker_count > 0 {
            self.decrement_worker_active(batch_worker as usize, batch_worker_count);
        }
    }

    /// Complete multiple requests with single-shard fast path or grouped per-shard locks, sending outside locks.
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

        let first_shard = CorrelationId::from_raw(responses[0].0).shard() & SHM_CORRELATION_MASK;
        let all_same_shard = responses[1..].iter().all(|(id, _)| {
            (CorrelationId::from_raw(*id).shard() & SHM_CORRELATION_MASK) == first_shard
        });
        if all_same_shard {
            let mut ready: Vec<(oneshot::Sender<PendingResponse>, PendingResponse, u32)> =
                Vec::with_capacity(responses.len());
            {
                let mut slab = self.correlation_shards[first_shard].lock().unwrap();
                for (raw_id, response) in responses {
                    let cid = CorrelationId::from_raw(raw_id);
                    if let Some((tx, worker_id)) = slab.take_completed_with_worker(cid) {
                        ready.push((tx, response, worker_id));
                    } else {
                        debug!(
                            id = cid.raw(),
                            shard = first_shard,
                            "no pending SHM request for correlation ID"
                        );
                    }
                }
            }
            self.flush_completed_ready(&mut ready);
            return;
        }

        let mut buckets: [Vec<(CorrelationId, PendingResponse)>; SHM_CORRELATION_SHARDS] =
            std::array::from_fn(|_| Vec::new());

        for (correlation_id, response) in responses {
            let cid = CorrelationId::from_raw(correlation_id);
            let shard_idx = cid.shard() & SHM_CORRELATION_MASK;
            buckets[shard_idx].push((cid, response));
        }

        let mut ready: Vec<(oneshot::Sender<PendingResponse>, PendingResponse, u32)> = Vec::new();
        for (shard_idx, bucket) in buckets.into_iter().enumerate() {
            if bucket.is_empty() {
                continue;
            }
            ready.clear();
            {
                let mut slab = self.correlation_shards[shard_idx].lock().unwrap();
                for (cid, response) in bucket {
                    if let Some((tx, worker_id)) = slab.take_completed_with_worker(cid) {
                        ready.push((tx, response, worker_id));
                    } else {
                        debug!(
                            id = cid.raw(),
                            shard = shard_idx,
                            "no pending SHM request for correlation ID"
                        );
                    }
                }
            }
            self.flush_completed_ready(&mut ready);
        }
    }

    /// Get all ring pairs (for the response reader).
    pub fn ring_pairs(&self) -> Vec<Arc<RingPair>> {
        self.workers
            .iter()
            .map(|w| Arc::clone(&w.ring_pair))
            .collect()
    }

    /// Deliver an ordered frame to a live streaming response.
    pub async fn deliver_stream_frame(&self, frame: ResponseStreamFrame) -> StreamDelivery {
        self.streams
            .deliver(frame, |id, response| self.complete_one(id, response))
            .await
    }

    pub fn active_streams(&self) -> usize {
        self.streams.active_streams()
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::uri::PathAndQuery;
    use vertebra_core::transport::BatchSnapshot;
    use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};

    fn make_pending(
        path: &'static str,
    ) -> (
        crate::batcher::PendingRequest,
        oneshot::Receiver<PendingResponse>,
    ) {
        let (tx, rx) = oneshot::channel();
        (
            crate::batcher::PendingRequest {
                handler_id: 1,
                method_tag: 0,
                http_version_tag: 1,
                flags: 0,
                path_and_query: PathAndQuery::from_static(path),
                headers_mp: Bytes::new(),
                body: Bytes::from_static(b"payload"),
                reply: tx,
            },
            rx,
        )
    }

    #[test]
    fn dispatch_with_scratch_reuses_buffers_and_completes_single_and_mixed_shards() {
        let rp = Arc::new(RingPair::create(RingPairConfig::default()).expect("ring pair"));
        let shm = ShmDispatcher::new(vec![Arc::clone(&rp)], 64);
        let mut scratch = ShmDispatchScratch::default();

        // Batch 1: single-shard dispatch + complete_many single-shard fast path.
        let (r1, mut rx1) = make_pending("/one");
        let (r2, mut rx2) = make_pending("/two?k=v");
        let mut batch = vec![r1, r2];
        assert_eq!(
            shm.dispatch_shm_vec_from_shard_with_scratch(Some(0), &mut batch, &mut scratch),
            2
        );
        assert!(scratch.staged.is_empty(), "staged cleared after dispatch");
        assert!(scratch.nodes.capacity() >= 2, "nodes capacity retained");
        assert!(
            scratch.components.capacity() >= 4,
            "components capacity retained"
        );

        let view1 = rp.request_ring.try_recv_view().expect("batch 1 view");
        let snap1 = BatchSnapshot::from_mpmc_slot(view1).expect("batch 1 snap");
        assert_eq!(snap1.len(), 2);
        let cid1 = snap1.node(0).correlation_id;
        let cid2 = snap1.node(1).correlation_id;
        assert_eq!(
            CorrelationId::from_raw(cid1).shard(),
            CorrelationId::from_raw(cid2).shard()
        );
        shm.complete_many(vec![
            (
                cid1,
                PendingResponse {
                    status: 200,
                    headers_mp: Bytes::new(),
                    body: Bytes::from_static(b"r1"),
                    stream_rx: None,
                },
            ),
            (
                cid2,
                PendingResponse {
                    status: 201,
                    headers_mp: Bytes::new(),
                    body: Bytes::from_static(b"r2"),
                    stream_rx: None,
                },
            ),
        ]);
        assert_eq!(rx1.try_recv().unwrap().body.as_ref(), b"r1");
        assert_eq!(rx2.try_recv().unwrap().body.as_ref(), b"r2");
        assert_eq!(shm.in_flight(), 0);

        // Batch 2 & 3 on different shards to exercise mixed-shard complete_many fallback.
        let (r3, mut rx3) = make_pending("/three");
        let (r4, mut rx4) = make_pending("/four");
        let mut b2 = vec![r3];
        let mut b3 = vec![r4];
        assert_eq!(
            shm.dispatch_shm_vec_from_shard_with_scratch(None, &mut b2, &mut scratch),
            1
        );
        assert_eq!(
            shm.dispatch_shm_vec_from_shard_with_scratch(None, &mut b3, &mut scratch),
            1
        );
        let snap2 =
            BatchSnapshot::from_mpmc_slot(rp.request_ring.try_recv_view().unwrap()).unwrap();
        let snap3 =
            BatchSnapshot::from_mpmc_slot(rp.request_ring.try_recv_view().unwrap()).unwrap();
        let cid3 = snap2.node(0).correlation_id;
        let cid4 = snap3.node(0).correlation_id;
        assert_ne!(
            CorrelationId::from_raw(cid3).shard(),
            CorrelationId::from_raw(cid4).shard()
        );
        shm.complete_many(vec![
            (
                cid3,
                PendingResponse {
                    status: 200,
                    headers_mp: Bytes::new(),
                    body: Bytes::from_static(b"r3"),
                    stream_rx: None,
                },
            ),
            (
                cid4,
                PendingResponse {
                    status: 200,
                    headers_mp: Bytes::new(),
                    body: Bytes::from_static(b"r4"),
                    stream_rx: None,
                },
            ),
        ]);
        assert_eq!(rx3.try_recv().unwrap().body.as_ref(), b"r3");
        assert_eq!(rx4.try_recv().unwrap().body.as_ref(), b"r4");
        assert_eq!(shm.in_flight(), 0);
        assert_eq!(shm.worker_in_flight(0), 0);
    }

    #[test]
    fn scratch_handles_mixed_flag_sub_batches_overflow_and_stale_single_shard_ids() {
        let rp = Arc::new(RingPair::create(RingPairConfig::default()).expect("ring pair"));
        let shm = ShmDispatcher::new(vec![Arc::clone(&rp)], 64);
        let mut scratch = ShmDispatchScratch::default();

        let (mut r_a, mut rx_a) = make_pending("/flag-a");
        r_a.flags = 0;
        let (mut r_b, mut rx_b) = make_pending("/flag-b");
        r_b.flags = 1;
        let mut batch = vec![r_a, r_b];
        assert_eq!(
            shm.dispatch_shm_vec_from_shard_with_scratch(Some(0), &mut batch, &mut scratch),
            2
        );
        let s1 = BatchSnapshot::from_mpmc_slot(rp.request_ring.try_recv_view().unwrap()).unwrap();
        let s2 = BatchSnapshot::from_mpmc_slot(rp.request_ring.try_recv_view().unwrap()).unwrap();
        assert_eq!(s1.flags(), 0);
        assert_eq!(s2.flags(), 1);
        let cid_a = s1.node(0).correlation_id;
        let cid_b = s2.node(0).correlation_id;
        let stale_same_shard =
            CorrelationId::encode(CorrelationId::from_raw(cid_a).shard() as u8, 0, 999).raw();
        shm.complete_many(vec![
            (
                cid_a,
                PendingResponse {
                    status: 200,
                    headers_mp: Bytes::new(),
                    body: Bytes::from_static(b"a"),
                    stream_rx: None,
                },
            ),
            (
                stale_same_shard,
                PendingResponse {
                    status: 200,
                    headers_mp: Bytes::new(),
                    body: Bytes::from_static(b"stale"),
                    stream_rx: None,
                },
            ),
            (
                cid_b,
                PendingResponse {
                    status: 200,
                    headers_mp: Bytes::new(),
                    body: Bytes::from_static(b"b"),
                    stream_rx: None,
                },
            ),
        ]);
        assert_eq!(rx_a.try_recv().unwrap().body.as_ref(), b"a");
        assert_eq!(rx_b.try_recv().unwrap().body.as_ref(), b"b");
        assert_eq!(shm.in_flight(), 0);

        #[cfg(unix)]
        {
            let big_body = Bytes::from(vec![b'z'; rp.request_ring.payload_capacity() + 2048]);
            let (mut r_big, mut rx_big) = make_pending("/overflow");
            r_big.body = big_body.clone();
            let mut overflow_batch = vec![r_big];
            assert_eq!(
                shm.dispatch_shm_vec_from_shard_with_scratch(
                    Some(0),
                    &mut overflow_batch,
                    &mut scratch
                ),
                1
            );
            assert!(scratch.nodes.capacity() >= 1);
            let view = rp.request_ring.try_recv_view().expect("overflow view");
            let snap = BatchSnapshot::from_mpmc_slot(view).expect("overflow snap");
            assert_eq!(snap.body(0), big_body.as_ref());
            assert!(shm.complete_one(
                snap.node(0).correlation_id,
                PendingResponse {
                    status: 200,
                    headers_mp: Bytes::new(),
                    body: Bytes::from_static(b"ok"),
                    stream_rx: None,
                },
            ));
            assert_eq!(rx_big.try_recv().unwrap().status, 200);
            assert_eq!(shm.in_flight(), 0);
        }
    }
}
