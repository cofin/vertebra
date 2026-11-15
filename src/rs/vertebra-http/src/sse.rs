//! RFC 8895 / WHATWG `EventSource` Server-Sent Events framing, `Last-Event-ID`
//! bounded topic replay buffer, and `DashMap`-backed `SseHub` with non-blocking
//! Tokio heartbeat tasks.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use http::header::{self, HeaderValue};
use http::{Response, StatusCode};
use hyper::body::Frame;
use tokio::sync::{broadcast, mpsc, watch};

use crate::body::FrameReceiver;

/// Keepalive comment frame emitted on idle intervals.
pub const SSE_KEEPALIVE_PING: &[u8] = b": ping\r\n\r\n";

/// Default per-topic broadcast channel capacity.
pub const DEFAULT_TOPIC_CHANNEL_CAPACITY: usize = 256;

pub type SseStreamingResponse = Response<FrameReceiver>;

/// A single Server-Sent Events message frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    pub id: Option<String>,
    pub event: Option<String>,
    pub data: String,
    pub retry_ms: Option<u64>,
    pub comment: Option<String>,
}

impl SseEvent {
    /// Construct a new `SseEvent` with the given `data` payload.
    pub fn new(data: impl Into<String>) -> Self {
        Self {
            id: None,
            event: None,
            data: data.into(),
            retry_ms: None,
            comment: None,
        }
    }

    /// Set the `id:` field.
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Set the `event:` field.
    pub fn with_event(mut self, event: impl Into<String>) -> Self {
        self.event = Some(event.into());
        self
    }

    /// Set the `retry:` reconnection interval in milliseconds.
    pub fn with_retry_ms(mut self, retry_ms: u64) -> Self {
        self.retry_ms = Some(retry_ms);
        self
    }

    /// Set an optional `: comment` preamble.
    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    /// Encode this event into `dst` according to RFC 8895 / WHATWG EventSource.
    pub fn encode_into(&self, dst: &mut BytesMut) {
        if let Some(ref comment) = self.comment {
            for line in split_whatwg_lines(comment) {
                dst.extend_from_slice(b": ");
                dst.extend_from_slice(line.as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
        }

        if let Some(ref id) = self.id {
            let clean_id = sanitize_header_field(id);
            if !clean_id.is_empty() {
                dst.extend_from_slice(b"id: ");
                dst.extend_from_slice(clean_id.as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
        }

        if let Some(ref event) = self.event {
            let clean_event = sanitize_header_field(event);
            if !clean_event.is_empty() {
                dst.extend_from_slice(b"event: ");
                dst.extend_from_slice(clean_event.as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
        }

        if let Some(retry_ms) = self.retry_ms {
            dst.extend_from_slice(b"retry: ");
            dst.extend_from_slice(retry_ms.to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
        }

        for line in split_whatwg_lines(&self.data) {
            dst.extend_from_slice(b"data: ");
            dst.extend_from_slice(line.as_bytes());
            dst.extend_from_slice(b"\r\n");
        }

        dst.extend_from_slice(b"\r\n");
    }

    /// Encode this event into an immutable `Bytes` frame.
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.data.len() + 64);
        self.encode_into(&mut buf);
        buf.freeze()
    }
}

/// Encode an `SseEvent` into a wire-ready `Bytes` buffer.
pub fn encode_sse_event(event: &SseEvent) -> Bytes {
    event.encode()
}

/// Strip `\r`, `\n`, and `\0` from single-line SSE header fields (`id`, `event`)
/// to prevent CRLF frame injection.
fn sanitize_header_field(raw: &str) -> String {
    if !raw.contains(['\r', '\n', '\0']) {
        return raw.to_owned();
    }
    raw.chars()
        .filter(|&ch| ch != '\r' && ch != '\n' && ch != '\0')
        .collect()
}

/// Split text across `\r\n`, `\r`, or `\n` per the WHATWG EventSource line grammar.
pub fn split_whatwg_lines(input: &str) -> Vec<&str> {
    if input.is_empty() {
        return vec![""];
    }
    let bytes = input.as_bytes();
    let mut lines = Vec::new();
    let mut start = 0;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\r' => {
                lines.push(&input[start..i]);
                if i + 1 < bytes.len() && bytes[i + 1] == b'\n' {
                    i += 2;
                } else {
                    i += 1;
                }
                start = i;
            }
            b'\n' => {
                lines.push(&input[start..i]);
                i += 1;
                start = i;
            }
            _ => {
                i += 1;
            }
        }
    }
    lines.push(&input[start..]);
    lines
}

/// Bounded ring buffer storing encoded SSE frames per topic for `Last-Event-ID` catch-up replay.
#[derive(Debug)]
pub struct SseTopicBuffer {
    capacity: usize,
    entries: VecDeque<(String, Bytes)>,
}

impl SseTopicBuffer {
    /// Create a new bounded ring buffer holding at most `capacity` events.
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(1);
        Self {
            capacity: cap,
            entries: VecDeque::with_capacity(cap),
        }
    }

    /// Return the number of buffered events currently retained.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Return `true` if no events are buffered.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Push an event into the ring buffer, evicting the oldest entry if at capacity.
    pub fn push(&mut self, event: SseEvent) -> Bytes {
        let encoded = event.encode();
        if let Some(id) = event.id {
            let clean_id = sanitize_header_field(&id);
            if self.entries.len() >= self.capacity {
                self.entries.pop_front();
            }
            self.entries.push_back((clean_id, encoded.clone()));
        }
        encoded
    }

    /// Push a pre-encoded event with an explicit ID into the ring buffer.
    pub fn push_encoded(&mut self, id: String, encoded: Bytes) {
        if self.entries.len() >= self.capacity {
            self.entries.pop_front();
        }
        self.entries
            .push_back((sanitize_header_field(&id), encoded));
    }

    /// Return all frames strictly newer than `last_event_id` in publication order.
    pub fn replay_since(&self, last_event_id: &str) -> Vec<Bytes> {
        let target = sanitize_header_field(last_event_id);
        if let Some(pos) = self.entries.iter().rposition(|(id, _)| id == &target) {
            self.entries
                .iter()
                .skip(pos + 1)
                .map(|(_, frame)| frame.clone())
                .collect()
        } else {
            Vec::new()
        }
    }
}

struct SseTopicChannel {
    next_seq: AtomicU64,
    buffer: RwLock<SseTopicBuffer>,
    tx: broadcast::Sender<Bytes>,
}

impl SseTopicChannel {
    fn new(replay_capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(DEFAULT_TOPIC_CHANNEL_CAPACITY);
        Self {
            next_seq: AtomicU64::new(1),
            buffer: RwLock::new(SseTopicBuffer::new(replay_capacity)),
            tx,
        }
    }
}

/// Concurrent `DashMap`-backed SSE topic hub with `Last-Event-ID` replay and Tokio heartbeat multiplexing.
pub struct SseHub {
    topics: DashMap<String, Arc<SseTopicChannel>>,
    replay_capacity: usize,
    heartbeat_interval: Duration,
    shutdown_tx: watch::Sender<bool>,
}

impl SseHub {
    /// Create a new `SseHub` with per-topic `replay_capacity` and keepalive `heartbeat_interval`.
    pub fn new(replay_capacity: usize, heartbeat_interval: Duration) -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        Self {
            topics: DashMap::new(),
            replay_capacity: replay_capacity.max(1),
            heartbeat_interval: heartbeat_interval.max(Duration::from_millis(10)),
            shutdown_tx,
        }
    }

    /// Close all active SSE subscription streams cleanly on server shutdown.
    pub fn close_all(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    fn get_or_create_topic(&self, topic: &str) -> Arc<SseTopicChannel> {
        if let Some(existing) = self.topics.get(topic) {
            return Arc::clone(existing.value());
        }
        Arc::clone(
            self.topics
                .entry(topic.to_owned())
                .or_insert_with(|| Arc::new(SseTopicChannel::new(self.replay_capacity)))
                .value(),
        )
    }

    /// Publish an `SseEvent` to `topic`, assigning a monotonic ID if none is set,
    /// recording it in the topic's replay ring, and broadcasting to active subscribers.
    pub fn publish(&self, topic: &str, mut event: SseEvent) -> Bytes {
        let channel = self.get_or_create_topic(topic);
        let assigned_id = match event.id {
            Some(ref id) => sanitize_header_field(id),
            None => {
                let seq = channel.next_seq.fetch_add(1, Ordering::Relaxed);
                let id_str = seq.to_string();
                event.id = Some(id_str.clone());
                id_str
            }
        };
        let encoded = event.encode();
        {
            let mut guard = channel.buffer.write().expect("sse buffer write lock");
            guard.push_encoded(assigned_id, encoded.clone());
        }
        let _ = channel.tx.send(encoded.clone());
        encoded
    }

    /// Publish raw pre-encoded SSE bytes to `topic`.
    pub fn publish_raw(&self, topic: &str, frame: Bytes) {
        let channel = self.get_or_create_topic(topic);
        let _ = channel.tx.send(frame);
    }

    /// Subscribe to `topic`, replaying missed frames since `last_event_id` and spawning
    /// a non-blocking Tokio task that multiplexes live frames with `: ping\r\n\r\n` keepalives.
    pub fn subscribe_response(
        &self,
        topic: &str,
        last_event_id: Option<&str>,
    ) -> SseStreamingResponse {
        let channel = self.get_or_create_topic(topic);
        let mut broadcast_rx = channel.tx.subscribe();
        let replayed = match last_event_id {
            Some(last_id) if !last_id.is_empty() => {
                let guard = channel.buffer.read().expect("sse buffer read lock");
                guard.replay_since(last_id)
            }
            _ => Vec::new(),
        };

        let (tx, rx) = mpsc::channel::<Result<Frame<Bytes>, Infallible>>(64);
        let heartbeat_interval = self.heartbeat_interval;
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            if *shutdown_rx.borrow_and_update() {
                return;
            }
            for frame_bytes in replayed {
                if tx.send(Ok(Frame::data(frame_bytes))).await.is_err() {
                    return;
                }
            }

            let mut ticker = tokio::time::interval(heartbeat_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;

            loop {
                tokio::select! {
                    biased;
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow_and_update() {
                            break;
                        }
                    }
                    msg = broadcast_rx.recv() => {
                        match msg {
                            Ok(bytes) => {
                                if tx.send(Ok(Frame::data(bytes))).await.is_err() {
                                    break;
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                let comment = Bytes::from(format!(": lagged {skipped}\r\n\r\n"));
                                if tx.send(Ok(Frame::data(comment))).await.is_err() {
                                    break;
                                }
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                    _ = ticker.tick() => {
                        let ping = Bytes::from_static(SSE_KEEPALIVE_PING);
                        if tx.send(Ok(Frame::data(ping))).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });

        Response::builder()
            .status(StatusCode::OK)
            .header(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/event-stream; charset=utf-8"),
            )
            .header(
                header::CACHE_CONTROL,
                HeaderValue::from_static("no-cache, no-transform"),
            )
            .header(header::CONNECTION, HeaderValue::from_static("keep-alive"))
            .header("x-accel-buffering", HeaderValue::from_static("no"))
            .body(FrameReceiver::new(rx))
            .expect("valid SSE streaming response")
    }
}
