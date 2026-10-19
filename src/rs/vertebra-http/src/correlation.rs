//! Slab-based O(1) correlation table for request/response matching.
//!
//! Each in-flight request gets a slot in the slab. The correlation ID embeds
//! both the slot index (lower 32 bits) and a generation counter (upper 32 bits)
//! to prevent ABA problems when slots are reused.
//!
//! This replaces `DashMap<u64, oneshot::Sender>` for the SharedMemory dispatch
//! path, eliminating hash computation and shard lock contention.

use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use crate::collector::PendingResponse;

/// Entry in the correlation slab.
struct CorrelationEntry {
    reply: oneshot::Sender<PendingResponse>,
    created_at: Instant,
    worker_id: u32,
}

/// O(1) correlation table using a pre-allocated slab with generation counters.
pub struct CorrelationSlab {
    slots: Vec<Option<CorrelationEntry>>,
    free_list: Vec<u32>,
    generations: Vec<u32>,
    capacity: usize,
}

impl CorrelationSlab {
    /// Create a new slab with the given capacity.
    ///
    /// Typical values: 65536 for 64K concurrent in-flight requests.
    pub fn new(capacity: usize) -> Self {
        let free_list: Vec<u32> = (0..capacity as u32).rev().collect();
        Self {
            slots: (0..capacity).map(|_| None).collect(),
            free_list,
            generations: vec![0; capacity],
            capacity,
        }
    }

    /// Insert a reply channel and return a correlation ID.
    ///
    /// The correlation ID encodes `(generation << 32) | slot_index`.
    /// Returns `None` if the slab is full (backpressure signal).
    pub fn insert(
        &mut self,
        reply: oneshot::Sender<PendingResponse>,
        worker_id: u32,
    ) -> Option<u64> {
        let index = self.free_list.pop()? as usize;
        self.generations[index] = self.generations[index].wrapping_add(1);
        let generation = self.generations[index];

        self.slots[index] = Some(CorrelationEntry {
            reply,
            created_at: Instant::now(),
            worker_id,
        });

        Some(((generation as u64) << 32) | (index as u64))
    }

    /// Complete a correlation by sending the response through the oneshot.
    ///
    /// Returns `true` if the correlation was found and completed.
    /// Returns `false` if the ID was invalid, already completed, or generation mismatched.
    pub fn complete(&mut self, correlation_id: u64, response: PendingResponse) -> bool {
        let index = (correlation_id & 0xFFFF_FFFF) as usize;
        let expected_gen = (correlation_id >> 32) as u32;

        if index >= self.capacity {
            return false;
        }
        if self.generations[index] != expected_gen {
            return false;
        }

        if let Some(entry) = self.slots[index].take() {
            let _ = entry.reply.send(response);
            self.free_list.push(index as u32);
            true
        } else {
            false
        }
    }

    /// Sweep for expired entries older than `timeout`.
    ///
    /// Returns the correlation IDs of expired entries (for error responses).
    /// The entries are removed from the slab and their slots returned to the free list.
    pub fn sweep_expired(&mut self, timeout: Duration) -> Vec<u64> {
        let now = Instant::now();
        let mut expired = Vec::new();

        for index in 0..self.capacity {
            if let Some(entry) = &self.slots[index]
                && now.duration_since(entry.created_at) >= timeout
            {
                let generation = self.generations[index];
                let id = ((generation as u64) << 32) | (index as u64);
                expired.push(id);
            }
        }

        // Remove expired entries
        for &id in &expired {
            let index = (id & 0xFFFF_FFFF) as usize;
            if let Some(entry) = self.slots[index].take() {
                // Send 504 Gateway Timeout through the oneshot
                let _ = entry.reply.send(PendingResponse {
                    status: 504,
                    headers_mp: bytes::Bytes::new(),
                    body: bytes::Bytes::from_static(b"Gateway Timeout"),
                });
                self.free_list.push(index as u32);
            }
        }

        expired
    }

    /// Sweep for entries targeting a specific dead worker.
    ///
    /// Returns correlation IDs of entries for the dead worker.
    /// Sends 502 Bad Gateway through their oneshots.
    pub fn sweep_for_worker(&mut self, dead_worker_id: u32) -> Vec<u64> {
        let mut swept = Vec::new();

        for index in 0..self.capacity {
            if let Some(entry) = &self.slots[index]
                && entry.worker_id == dead_worker_id
            {
                let generation = self.generations[index];
                swept.push(((generation as u64) << 32) | (index as u64));
            }
        }

        for &id in &swept {
            let index = (id & 0xFFFF_FFFF) as usize;
            if let Some(entry) = self.slots[index].take() {
                let _ = entry.reply.send(PendingResponse {
                    status: 502,
                    headers_mp: bytes::Bytes::new(),
                    body: bytes::Bytes::from_static(b"Bad Gateway"),
                });
                self.free_list.push(index as u32);
            }
        }

        swept
    }

    /// Number of currently in-flight entries.
    pub fn in_flight(&self) -> usize {
        self.capacity - self.free_list.len()
    }

    /// Total capacity of the slab.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_slab_is_empty() {
        let slab = CorrelationSlab::new(1024);
        assert_eq!(slab.in_flight(), 0);
        assert_eq!(slab.capacity(), 1024);
    }

    #[test]
    fn insert_and_complete_round_trip() {
        let mut slab = CorrelationSlab::new(64);
        let (tx, mut rx) = oneshot::channel();

        let id = slab.insert(tx, 0).expect("should insert");
        assert_eq!(slab.in_flight(), 1);

        let completed = slab.complete(
            id,
            PendingResponse {
                status: 200,
                headers_mp: bytes::Bytes::new(),
                body: bytes::Bytes::from_static(b"ok"),
            },
        );
        assert!(completed);
        assert_eq!(slab.in_flight(), 0);

        let resp = rx.try_recv().expect("should receive response");
        assert_eq!(resp.status, 200);
    }

    #[test]
    fn insert_1000_complete_random_order() {
        let mut slab = CorrelationSlab::new(2048);
        let mut ids_and_rxs = Vec::new();

        for i in 0..1000u64 {
            let (tx, rx) = oneshot::channel();
            let id = slab.insert(tx, (i % 4) as u32).expect("should insert");
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
                },
            );
            let resp = rx.try_recv().expect("should receive");
            assert_eq!(resp.status, 200);
        }
        assert_eq!(slab.in_flight(), 0);
    }

    #[test]
    fn generation_prevents_aba() {
        let mut slab = CorrelationSlab::new(4);
        let (tx1, _rx1) = oneshot::channel();
        let id1 = slab.insert(tx1, 0).expect("insert");

        // Complete the first entry
        slab.complete(
            id1,
            PendingResponse {
                status: 200,
                headers_mp: bytes::Bytes::new(),
                body: bytes::Bytes::from_static(b"ok"),
            },
        );

        // Insert a new entry in the same slot
        let (tx2, _rx2) = oneshot::channel();
        let id2 = slab.insert(tx2, 0).expect("insert");

        // id1 and id2 have different generations
        assert_ne!(id1, id2);
        assert_eq!(id1 & 0xFFFF_FFFF, id2 & 0xFFFF_FFFF); // same slot
        assert_ne!(id1 >> 32, id2 >> 32); // different generation

        // Completing with the OLD id should fail
        let stale_complete = slab.complete(
            id1,
            PendingResponse {
                status: 500,
                headers_mp: bytes::Bytes::new(),
                body: bytes::Bytes::from_static(b"stale"),
            },
        );
        assert!(!stale_complete, "stale ID should not complete");
        assert_eq!(slab.in_flight(), 1, "entry should still be in-flight");
    }

    #[test]
    fn capacity_exhaustion() {
        let mut slab = CorrelationSlab::new(2);
        let (tx1, _rx1) = oneshot::channel();
        let (tx2, _rx2) = oneshot::channel();
        let (tx3, _rx3) = oneshot::channel();

        assert!(slab.insert(tx1, 0).is_some());
        assert!(slab.insert(tx2, 0).is_some());
        assert!(slab.insert(tx3, 0).is_none(), "should be full");
    }

    #[test]
    fn complete_invalid_id() {
        let mut slab = CorrelationSlab::new(4);
        let result = slab.complete(
            9999,
            PendingResponse {
                status: 200,
                headers_mp: bytes::Bytes::new(),
                body: bytes::Bytes::from_static(b"ok"),
            },
        );
        assert!(!result);
    }

    #[test]
    fn sweep_for_worker() {
        let mut slab = CorrelationSlab::new(64);

        // Insert entries for workers 0, 1, 2
        for w in 0..3u32 {
            for _ in 0..5 {
                let (tx, _rx) = oneshot::channel();
                slab.insert(tx, w).expect("insert");
            }
        }
        assert_eq!(slab.in_flight(), 15);

        // Kill worker 1 — sweep its entries
        let swept = slab.sweep_for_worker(1);
        assert_eq!(swept.len(), 5);
        assert_eq!(slab.in_flight(), 10);
    }

    #[test]
    fn sweep_expired() {
        let mut slab = CorrelationSlab::new(64);
        let (tx, _rx) = oneshot::channel();
        slab.insert(tx, 0).expect("insert");

        // Sweep with a very short timeout — entry should be "expired" immediately
        // (since Instant::now() in the entry is ≤ now in the sweep)
        let expired = slab.sweep_expired(Duration::ZERO);
        assert_eq!(expired.len(), 1);
        assert_eq!(slab.in_flight(), 0);
    }
}
