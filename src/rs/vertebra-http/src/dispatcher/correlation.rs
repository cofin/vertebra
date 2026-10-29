//! Slab-based O(1) correlation table for request/response matching.
//!
//! Each in-flight request gets a slot in the slab. The correlation ID embeds
//! the shard index (lower 8 bits), slot index (middle 24 bits), and a generation
//! counter (upper 32 bits) to prevent ABA problems when slots are reused without
//! truncating generation bits.

use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use crate::body::PendingResponse;

/// Packed 64-bit correlation identifier encoding shard (8 bits), slot index (24 bits),
/// and generation counter (32 bits).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CorrelationId(u64);

impl CorrelationId {
    /// Maximum supported slot index fitting within 24 bits.
    pub const MAX_SLOT: u32 = 0x00FF_FFFF;

    /// Encode shard (8 bits), slot (24 bits), and generation (32 bits) into a 64-bit ID.
    #[inline]
    pub const fn encode(shard: u8, slot: u32, generation: u32) -> Self {
        debug_assert!(slot <= Self::MAX_SLOT);
        let raw =
            ((generation as u64) << 32) | (((slot & Self::MAX_SLOT) as u64) << 8) | (shard as u64);
        Self(raw)
    }

    /// Construct a `CorrelationId` directly from a raw 64-bit value.
    #[inline]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Extract the underlying raw 64-bit integer.
    #[inline]
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Extract the 8-bit shard index.
    #[inline]
    pub const fn shard(self) -> usize {
        (self.0 & 0xFF) as usize
    }

    /// Extract the 24-bit slot index.
    #[inline]
    pub const fn slot(self) -> usize {
        ((self.0 >> 8) & (Self::MAX_SLOT as u64)) as usize
    }

    /// Extract the 32-bit generation counter.
    #[inline]
    pub const fn generation(self) -> u32 {
        (self.0 >> 32) as u32
    }
}

/// Entry in the correlation slab.
struct CorrelationEntry {
    reply: oneshot::Sender<PendingResponse>,
    created_at: Instant,
    worker_id: u32,
}

/// O(1) correlation table using a pre-allocated slab with generation counters.
pub struct InFlightRequests {
    slots: Vec<Option<CorrelationEntry>>,
    free_list: Vec<u32>,
    generations: Vec<u32>,
    capacity: usize,
}

impl InFlightRequests {
    /// Create a new slab with the given capacity.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity <= (CorrelationId::MAX_SLOT as usize) + 1);
        let free_list: Vec<u32> = (0..capacity as u32).rev().collect();
        Self {
            slots: (0..capacity).map(|_| None).collect(),
            free_list,
            generations: vec![0; capacity],
            capacity,
        }
    }

    /// Insert a reply channel and return a correlation ID preserving 32-bit generation.
    pub fn insert(
        &mut self,
        shard: u8,
        reply: oneshot::Sender<PendingResponse>,
        worker_id: u32,
    ) -> Option<CorrelationId> {
        let index = self.free_list.pop()? as usize;
        self.generations[index] = self.generations[index].wrapping_add(1);
        let generation = self.generations[index];

        self.slots[index] = Some(CorrelationEntry {
            reply,
            created_at: Instant::now(),
            worker_id,
        });

        Some(CorrelationId::encode(shard, index as u32, generation))
    }

    /// Pop a matching reply sender and target worker ID from the slab so the caller can invoke `send` outside locks.
    #[inline]
    pub fn take_completed_with_worker(
        &mut self,
        id: CorrelationId,
    ) -> Option<(oneshot::Sender<PendingResponse>, u32)> {
        let index = id.slot();
        let expected_gen = id.generation();

        if index >= self.capacity || self.generations[index] != expected_gen {
            return None;
        }

        let entry = self.slots[index].take()?;
        self.free_list.push(index as u32);
        Some((entry.reply, entry.worker_id))
    }

    /// Pop a matching reply sender from the slab so the caller can invoke `send` outside locks.
    #[inline]
    pub fn take_completed(
        &mut self,
        id: CorrelationId,
    ) -> Option<oneshot::Sender<PendingResponse>> {
        self.take_completed_with_worker(id).map(|(reply, _)| reply)
    }

    /// Complete a correlation by sending the response through the oneshot.
    pub fn complete(&mut self, id: CorrelationId, response: PendingResponse) -> bool {
        if let Some(reply) = self.take_completed(id) {
            let _ = reply.send(response);
            true
        } else {
            false
        }
    }

    /// Remove and return expired `(reply, worker_id)` pairs older than `timeout`.
    pub fn take_expired_with_worker(
        &mut self,
        timeout: Duration,
    ) -> Vec<(oneshot::Sender<PendingResponse>, u32)> {
        let now = Instant::now();
        let mut expired = Vec::new();

        for index in 0..self.capacity {
            let is_expired = if let Some(entry) = &self.slots[index] {
                now.duration_since(entry.created_at) >= timeout
            } else {
                false
            };

            if is_expired && let Some(entry) = self.slots[index].take() {
                self.free_list.push(index as u32);
                expired.push((entry.reply, entry.worker_id));
            }
        }

        expired
    }

    /// Remove and return expired reply senders older than `timeout`.
    pub fn take_expired(&mut self, timeout: Duration) -> Vec<oneshot::Sender<PendingResponse>> {
        self.take_expired_with_worker(timeout)
            .into_iter()
            .map(|(reply, _)| reply)
            .collect()
    }

    /// Sweep for expired entries older than `timeout`.
    pub fn sweep_expired(&mut self, timeout: Duration) -> usize {
        let expired = self.take_expired(timeout);
        let count = expired.len();
        for reply in expired {
            let _ = reply.send(PendingResponse {
                status: 504,
                headers_mp: bytes::Bytes::new(),
                body: bytes::Bytes::from_static(b"Gateway Timeout"),
                stream_rx: None,
            });
        }
        count
    }

    /// Remove and return reply senders targeting a specific dead worker.
    pub fn take_for_worker(
        &mut self,
        dead_worker_id: u32,
    ) -> Vec<oneshot::Sender<PendingResponse>> {
        let mut targets = Vec::new();

        for index in 0..self.capacity {
            let is_target = if let Some(entry) = &self.slots[index] {
                entry.worker_id == dead_worker_id
            } else {
                false
            };

            if is_target && let Some(entry) = self.slots[index].take() {
                self.free_list.push(index as u32);
                targets.push(entry.reply);
            }
        }

        targets
    }

    /// Sweep for entries targeting a specific dead worker.
    pub fn sweep_for_worker(&mut self, dead_worker_id: u32) -> usize {
        let targets = self.take_for_worker(dead_worker_id);
        let count = targets.len();
        for reply in targets {
            let _ = reply.send(PendingResponse {
                status: 502,
                headers_mp: bytes::Bytes::new(),
                body: bytes::Bytes::from_static(b"Bad Gateway"),
                stream_rx: None,
            });
        }
        count
    }

    /// Number of active in-flight requests.
    pub fn in_flight(&self) -> usize {
        self.capacity - self.free_list.len()
    }

    /// Total capacity of the slab.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Set generation for testing generation wraps and high-bit preservation.
    #[doc(hidden)]
    pub fn set_generation(&mut self, slot: usize, generation: u32) {
        if slot < self.capacity {
            self.generations[slot] = generation;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_slab_is_empty() {
        let slab = InFlightRequests::new(1024);
        assert_eq!(slab.in_flight(), 0);
        assert_eq!(slab.capacity(), 1024);
    }

    #[test]
    fn insert_and_complete_round_trip() {
        let mut slab = InFlightRequests::new(64);
        let (tx, mut rx) = oneshot::channel();

        let id = slab.insert(0, tx, 0).expect("should insert");
        assert_eq!(slab.in_flight(), 1);

        let completed = slab.complete(
            id,
            PendingResponse {
                status: 200,
                headers_mp: bytes::Bytes::new(),
                body: bytes::Bytes::from_static(b"ok"),
                stream_rx: None,
            },
        );
        assert!(completed);
        assert_eq!(slab.in_flight(), 0);

        let resp = rx.try_recv().expect("should receive response");
        assert_eq!(resp.status, 200);
    }

    #[test]
    fn insert_1000_complete_random_order() {
        let mut slab = InFlightRequests::new(2048);
        let mut ids_and_rxs = Vec::new();

        for i in 0..1000u64 {
            let (tx, rx) = oneshot::channel();
            let id = slab
                .insert((i % 4) as u8, tx, (i % 4) as u32)
                .expect("should insert");
            ids_and_rxs.push((id, rx));
        }
        assert_eq!(slab.in_flight(), 1000);

        // Complete in reverse order
        for (id, mut rx) in ids_and_rxs.into_iter().rev() {
            slab.complete(
                id,
                PendingResponse {
                    status: 200,
                    headers_mp: bytes::Bytes::new(),
                    body: bytes::Bytes::from_static(b"ok"),
                    stream_rx: None,
                },
            );
            let resp = rx.try_recv().expect("should receive");
            assert_eq!(resp.status, 200);
        }
        assert_eq!(slab.in_flight(), 0);
    }

    #[test]
    fn generation_prevents_aba() {
        let mut slab = InFlightRequests::new(4);
        let (tx1, _rx1) = oneshot::channel();
        let id1 = slab.insert(0, tx1, 0).expect("insert");

        // Complete the first entry
        slab.complete(
            id1,
            PendingResponse {
                status: 200,
                headers_mp: bytes::Bytes::new(),
                body: bytes::Bytes::from_static(b"ok"),
                stream_rx: None,
            },
        );

        // Insert a new entry in the same slot
        let (tx2, _rx2) = oneshot::channel();
        let id2 = slab.insert(0, tx2, 0).expect("insert");

        // id1 and id2 have different generations
        assert_ne!(id1, id2);
        assert_eq!(id1.slot(), id2.slot());
        assert_ne!(id1.generation(), id2.generation());

        // Completing with the OLD id should fail
        let stale_complete = slab.complete(
            id1,
            PendingResponse {
                status: 500,
                headers_mp: bytes::Bytes::new(),
                body: bytes::Bytes::from_static(b"stale"),
                stream_rx: None,
            },
        );
        assert!(!stale_complete, "stale ID should not complete");
        assert_eq!(slab.in_flight(), 1, "entry should still be in-flight");
    }

    #[test]
    fn capacity_exhaustion() {
        let mut slab = InFlightRequests::new(2);
        let (tx1, _rx1) = oneshot::channel();
        let (tx2, _rx2) = oneshot::channel();
        let (tx3, _rx3) = oneshot::channel();

        assert!(slab.insert(0, tx1, 0).is_some());
        assert!(slab.insert(0, tx2, 0).is_some());
        assert!(slab.insert(0, tx3, 0).is_none(), "should be full");
    }

    #[test]
    fn complete_invalid_id() {
        let mut slab = InFlightRequests::new(4);
        let result = slab.complete(
            CorrelationId::from_raw(9999),
            PendingResponse {
                status: 200,
                headers_mp: bytes::Bytes::new(),
                body: bytes::Bytes::from_static(b"ok"),
                stream_rx: None,
            },
        );
        assert!(!result);
    }

    #[test]
    fn sweep_for_worker() {
        let mut slab = InFlightRequests::new(64);

        // Insert entries for workers 0, 1, 2
        for w in 0..3u32 {
            for _ in 0..5 {
                let (tx, _rx) = oneshot::channel();
                slab.insert(w as u8, tx, w).expect("insert");
            }
        }
        assert_eq!(slab.in_flight(), 15);

        // Kill worker 1 — sweep its entries
        let swept = slab.sweep_for_worker(1);
        assert_eq!(swept, 5);
        assert_eq!(slab.in_flight(), 10);
    }

    #[test]
    fn sweep_expired() {
        let mut slab = InFlightRequests::new(64);
        let (tx, _rx) = oneshot::channel();
        slab.insert(0, tx, 0).expect("insert");

        let expired = slab.sweep_expired(Duration::ZERO);
        assert_eq!(expired, 1);
        assert_eq!(slab.in_flight(), 0);
    }
}
