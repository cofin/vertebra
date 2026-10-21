//! Lock-free Rust Pub/Sub broker (`PubSubBroker`) with `DashMap<String, broadcast::Sender<Bytes>>`,
//! wildcard pattern subscriptions, per-topic `Last-Event-ID` ring replay, `1024`-entry
//! `idempotencyKey` deduplication, `SseHub` integration, and shared-memory cross-worker
//! broadcast slot forwarding.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use tokio::sync::broadcast;

use crate::sse::{SseEvent, SseHub};

/// Maximum number of recent idempotency keys retained per topic.
pub const MAX_IDEMPOTENCY_KEYS: usize = 1024;

/// Default per-topic `tokio::sync::broadcast` channel capacity.
pub const DEFAULT_BROADCAST_CAPACITY: usize = 1024;

/// Envelope representing a published topic event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubEnvelope {
    pub event_id: u64,
    pub origin_worker_id: u32,
    pub topic: String,
    pub payload: Bytes,
    pub idempotency_key: Option<String>,
}

impl PubSubEnvelope {
    /// Encode this envelope into a compact binary frame for SHM cross-worker ring slots.
    ///
    /// Layout:
    /// `[event_id: u64 LE][origin_worker_id: u32 LE][topic_len: u16 LE][idem_len: u16 LE][payload_len: u32 LE][topic][idem][payload]`
    pub fn encode_shm_frame(&self) -> Bytes {
        let topic_bytes = self.topic.as_bytes();
        let idem_bytes = self
            .idempotency_key
            .as_deref()
            .map(str::as_bytes)
            .unwrap_or(&[]);
        let total_len =
            8 + 4 + 2 + 2 + 4 + topic_bytes.len() + idem_bytes.len() + self.payload.len();
        let mut buf = BytesMut::with_capacity(total_len);
        buf.extend_from_slice(&self.event_id.to_le_bytes());
        buf.extend_from_slice(&self.origin_worker_id.to_le_bytes());
        buf.extend_from_slice(&(topic_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(&(idem_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(topic_bytes);
        buf.extend_from_slice(idem_bytes);
        buf.extend_from_slice(&self.payload);
        buf.freeze()
    }

    /// Decode a `PubSubEnvelope` from a raw SHM slot byte slice.
    pub fn decode_shm_frame(raw: &[u8]) -> Option<Self> {
        if raw.len() < 20 {
            return None;
        }
        let event_id = u64::from_le_bytes(raw[0..8].try_into().ok()?);
        let origin_worker_id = u32::from_le_bytes(raw[8..12].try_into().ok()?);
        let topic_len = u16::from_le_bytes(raw[12..14].try_into().ok()?) as usize;
        let idem_len = u16::from_le_bytes(raw[14..16].try_into().ok()?) as usize;
        let payload_len = u32::from_le_bytes(raw[16..20].try_into().ok()?) as usize;

        let expected_len = 20 + topic_len + idem_len + payload_len;
        if raw.len() < expected_len {
            return None;
        }

        let topic_end = 20 + topic_len;
        let idem_end = topic_end + idem_len;
        let payload_end = idem_end + payload_len;

        let topic = std::str::from_utf8(&raw[20..topic_end]).ok()?.to_owned();
        let idempotency_key = if idem_len > 0 {
            Some(
                std::str::from_utf8(&raw[topic_end..idem_end])
                    .ok()?
                    .to_owned(),
            )
        } else {
            None
        };
        let payload = Bytes::copy_from_slice(&raw[idem_end..payload_end]);

        Some(Self {
            event_id,
            origin_worker_id,
            topic,
            payload,
            idempotency_key,
        })
    }
}

struct ShmSlot {
    seq: AtomicU64,
    data: Mutex<Vec<u8>>,
}

/// Shared-memory style multi-reader broadcast slot ring for cross-worker Pub/Sub fan-out.
pub struct ShmBroadcastRing {
    write_seq: AtomicU64,
    slot_capacity: usize,
    slots: Vec<ShmSlot>,
}

impl ShmBroadcastRing {
    /// Create a new broadcast ring with `num_slots` slots, each up to `slot_capacity` bytes.
    pub fn new(num_slots: usize, slot_capacity: usize) -> Self {
        let count = num_slots.max(2).next_power_of_two();
        let mut slots = Vec::with_capacity(count);
        for _ in 0..count {
            slots.push(ShmSlot {
                seq: AtomicU64::new(0),
                data: Mutex::new(Vec::with_capacity(slot_capacity)),
            });
        }
        Self {
            write_seq: AtomicU64::new(0),
            slot_capacity: slot_capacity.max(256),
            slots,
        }
    }

    /// Publish a binary frame into the next ring slot.
    pub fn write_frame(&self, frame: &[u8]) -> bool {
        if frame.len() > self.slot_capacity {
            return false;
        }
        let seq = self.write_seq.fetch_add(1, Ordering::AcqRel) + 1;
        let idx = (seq as usize) & (self.slots.len() - 1);
        let slot = &self.slots[idx];
        {
            let mut guard = slot.data.lock().expect("shm slot lock");
            guard.clear();
            guard.extend_from_slice(frame);
        }
        slot.seq.store(seq, Ordering::Release);
        true
    }

    /// Read all frames newer than `*cursor`, advancing `*cursor` to the latest sequence.
    pub fn read_since(&self, cursor: &mut u64) -> Vec<Vec<u8>> {
        let head = self.write_seq.load(Ordering::Acquire);
        if head <= *cursor {
            return Vec::new();
        }
        let ring_len = self.slots.len() as u64;
        let start = (*cursor + 1).max(head.saturating_sub(ring_len - 1));
        let mut out = Vec::with_capacity((head - start + 1) as usize);

        for seq in start..=head {
            let idx = (seq as usize) & (self.slots.len() - 1);
            let slot = &self.slots[idx];
            if slot.seq.load(Ordering::Acquire) == seq {
                let guard = slot.data.lock().expect("shm slot lock");
                out.push(guard.clone());
            }
        }
        *cursor = head;
        out
    }
}

struct IdempotencyWindow {
    order: VecDeque<String>,
    set: HashSet<String>,
}

impl IdempotencyWindow {
    fn new() -> Self {
        Self {
            order: VecDeque::with_capacity(MAX_IDEMPOTENCY_KEYS),
            set: HashSet::with_capacity(MAX_IDEMPOTENCY_KEYS),
        }
    }

    fn check_and_insert(&mut self, key: &str) -> bool {
        if self.set.contains(key) {
            return false;
        }
        if self.order.len() >= MAX_IDEMPOTENCY_KEYS
            && let Some(evicted) = self.order.pop_front()
        {
            self.set.remove(&evicted);
        }
        let owned = key.to_owned();
        self.set.insert(owned.clone());
        self.order.push_back(owned);
        true
    }
}

/// Per-topic subscriber and history state.
pub struct TopicSubscribers {
    pub next_event_id: AtomicU64,
    pub envelope_tx: broadcast::Sender<PubSubEnvelope>,
    pub history: RwLock<VecDeque<(u64, PubSubEnvelope)>>,
    idempotency: Mutex<IdempotencyWindow>,
    pub max_history: usize,
}

impl TopicSubscribers {
    fn new(max_history: usize) -> Self {
        let (envelope_tx, _) = broadcast::channel(DEFAULT_BROADCAST_CAPACITY);
        Self {
            next_event_id: AtomicU64::new(1),
            envelope_tx,
            history: RwLock::new(VecDeque::with_capacity(max_history.max(1))),
            idempotency: Mutex::new(IdempotencyWindow::new()),
            max_history: max_history.max(1),
        }
    }
}

/// Return `true` if `topic` matches a wildcard `pattern` (`*` or prefix ending with `*`).
pub fn topic_matches_pattern(pattern: &str, topic: &str) -> bool {
    if pattern == "*" || pattern == topic {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return topic.starts_with(prefix);
    }
    false
}

/// Unified lock-free Rust Pub/Sub broker supporting `O(1)` `DashMap<String, broadcast::Sender<Bytes>>`
/// fan-out, wildcard pattern subscriptions, `Last-Event-ID` ring replay, `1024`-entry idempotency
/// deduplication, and SHM cross-worker forwarding.
pub struct PubSubBroker {
    worker_id: u32,
    max_history: usize,
    raw_channels: DashMap<String, broadcast::Sender<Bytes>>,
    pattern_channels: DashMap<String, broadcast::Sender<Bytes>>,
    topic_meta: DashMap<String, Arc<TopicSubscribers>>,
    sse_hub: Option<Arc<SseHub>>,
    shm_ring: Option<Arc<ShmBroadcastRing>>,
}

pub type PubSubHub = PubSubBroker;

impl PubSubBroker {
    /// Create a new `PubSubBroker` for `worker_id`.
    pub fn new(
        worker_id: u32,
        max_history: usize,
        sse_hub: Option<Arc<SseHub>>,
        shm_ring: Option<Arc<ShmBroadcastRing>>,
    ) -> Self {
        Self {
            worker_id,
            max_history: max_history.max(1),
            raw_channels: DashMap::new(),
            pattern_channels: DashMap::new(),
            topic_meta: DashMap::new(),
            sse_hub,
            shm_ring,
        }
    }

    fn get_or_create_raw_tx(&self, topic: &str) -> broadcast::Sender<Bytes> {
        if let Some(tx) = self.raw_channels.get(topic) {
            return tx.value().clone();
        }
        self.raw_channels
            .entry(topic.to_owned())
            .or_insert_with(|| {
                let (tx, _) = broadcast::channel(DEFAULT_BROADCAST_CAPACITY);
                tx
            })
            .value()
            .clone()
    }

    fn get_or_create_pattern_tx(&self, pattern: &str) -> broadcast::Sender<Bytes> {
        if let Some(tx) = self.pattern_channels.get(pattern) {
            return tx.value().clone();
        }
        self.pattern_channels
            .entry(pattern.to_owned())
            .or_insert_with(|| {
                let (tx, _) = broadcast::channel(DEFAULT_BROADCAST_CAPACITY);
                tx
            })
            .value()
            .clone()
    }

    fn get_or_create_meta(&self, topic: &str) -> Arc<TopicSubscribers> {
        if let Some(meta) = self.topic_meta.get(topic) {
            return Arc::clone(meta.value());
        }
        Arc::clone(
            self.topic_meta
                .entry(topic.to_owned())
                .or_insert_with(|| Arc::new(TopicSubscribers::new(self.max_history)))
                .value(),
        )
    }

    /// Subscribe to raw payload `Bytes` published on `topic` (`O(1)` lookup).
    pub fn subscribe_raw(&self, topic: &str) -> broadcast::Receiver<Bytes> {
        self.get_or_create_raw_tx(topic).subscribe()
    }

    /// Subscribe to raw payload `Bytes` matching a wildcard pattern (e.g. `"orders.*"`).
    pub fn subscribe_pattern(&self, pattern: &str) -> broadcast::Receiver<Bytes> {
        self.get_or_create_pattern_tx(pattern).subscribe()
    }

    /// Subscribe to structured `PubSubEnvelope` events with `Last-Event-ID` catch-up replay.
    pub fn subscribe_with_replay(
        &self,
        topic: &str,
        last_event_id: Option<u64>,
    ) -> (Vec<PubSubEnvelope>, broadcast::Receiver<PubSubEnvelope>) {
        let meta = self.get_or_create_meta(topic);
        let rx = meta.envelope_tx.subscribe();
        let replayed = match last_event_id {
            Some(since_id) => {
                let guard = meta.history.read().expect("topic history read lock");
                guard
                    .iter()
                    .filter(|(id, _)| *id > since_id)
                    .map(|(_, env)| env.clone())
                    .collect()
            }
            None => Vec::new(),
        };
        (replayed, rx)
    }

    /// Publish `payload` to `topic`, returning `Some(event_id)` on delivery or `None` if
    /// suppressed by the `1024`-entry `idempotency_key` deduplication window.
    pub fn pubsub_publish(
        &self,
        topic: &str,
        payload: Bytes,
        idempotency_key: Option<&str>,
    ) -> Option<u64> {
        self.publish_internal(topic, payload, idempotency_key, self.worker_id, true)
    }

    fn publish_internal(
        &self,
        topic: &str,
        payload: Bytes,
        idempotency_key: Option<&str>,
        origin_worker_id: u32,
        forward_to_shm: bool,
    ) -> Option<u64> {
        let meta = self.get_or_create_meta(topic);

        if let Some(key) = idempotency_key
            && !key.is_empty()
        {
            let mut guard = meta.idempotency.lock().expect("idempotency mutex");
            if !guard.check_and_insert(key) {
                return None;
            }
        }

        let event_id = meta.next_event_id.fetch_add(1, Ordering::Relaxed);
        let envelope = PubSubEnvelope {
            event_id,
            origin_worker_id,
            topic: topic.to_owned(),
            payload: payload.clone(),
            idempotency_key: idempotency_key.map(str::to_owned),
        };

        {
            let mut history = meta.history.write().expect("topic history write lock");
            if history.len() >= meta.max_history {
                history.pop_front();
            }
            history.push_back((event_id, envelope.clone()));
        }

        let raw_tx = self.get_or_create_raw_tx(topic);
        let _ = raw_tx.send(payload.clone());
        let _ = meta.envelope_tx.send(envelope.clone());

        if !self.pattern_channels.is_empty() {
            for entry in self.pattern_channels.iter() {
                if topic_matches_pattern(entry.key(), topic) {
                    let _ = entry.value().send(payload.clone());
                }
            }
        }

        if let Some(ref sse_hub) = self.sse_hub {
            let text = String::from_utf8_lossy(&payload).into_owned();
            let sse_event = SseEvent::new(text)
                .with_id(event_id.to_string())
                .with_event(topic.to_owned());
            sse_hub.publish(topic, sse_event);
        }

        if forward_to_shm && let Some(ref shm) = self.shm_ring {
            let frame = envelope.encode_shm_frame();
            shm.write_frame(&frame);
        }

        Some(event_id)
    }

    /// Drain any new cross-worker frames from the shared `ShmBroadcastRing` and fan them
    /// out to local WebSocket and SSE subscribers, skipping frames originating from `self.worker_id`.
    pub fn poll_shm_ring(&self, cursor: &mut u64) -> usize {
        let Some(ref shm) = self.shm_ring else {
            return 0;
        };
        let frames = shm.read_since(cursor);
        let mut delivered = 0;
        for raw in frames {
            if let Some(env) = PubSubEnvelope::decode_shm_frame(&raw)
                && env.origin_worker_id != self.worker_id
            {
                self.publish_internal(
                    &env.topic,
                    env.payload,
                    env.idempotency_key.as_deref(),
                    env.origin_worker_id,
                    false,
                );
                delivered += 1;
            }
        }
        delivered
    }
}
