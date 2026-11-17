//! Bounded progressive response delivery shared by embedded and SHM dispatchers.

use std::convert::Infallible;

use bytes::Bytes;
use dashmap::DashMap;
use http::{HeaderMap, HeaderName, HeaderValue};
use hyper::body::Frame;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};
use vertebra_core::bridge::{ResponseStreamFrame, StreamDelivery};

use crate::body::PendingResponse;
use crate::response::encode_response_headers;

type FrameSender = mpsc::Sender<Result<Frame<Bytes>, Infallible>>;

#[derive(Clone)]
struct StreamEntry {
    sender: FrameSender,
    worker: Option<u32>,
    cancelled: CancellationToken,
}

/// Default bounded per-stream frame queue capacity.
pub const DEFAULT_STREAM_QUEUE_DEPTH: usize = 64;

/// Per-response bounded channels with producer backpressure and disconnect reporting.
pub struct ResponseStreamRegistry {
    streams: DashMap<u64, StreamEntry>,
    capacity: usize,
}

impl Default for ResponseStreamRegistry {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_STREAM_QUEUE_DEPTH)
    }
}

impl ResponseStreamRegistry {
    /// Limit buffered frames per stream, with a minimum capacity of one.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            streams: DashMap::new(),
            capacity: capacity.max(1),
        }
    }

    /// Return the configured per-stream bounded frame capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Close and cancel a single stream if still active.
    pub fn close(&self, request_id: u64) -> bool {
        self.remove(request_id).is_some()
    }

    /// Deliver on a detached producer thread, blocking while the client applies backpressure.
    pub fn deliver_blocking(
        &self,
        frame: ResponseStreamFrame,
        complete: impl FnOnce(u64, PendingResponse) -> bool,
    ) -> StreamDelivery {
        if tokio::runtime::Handle::try_current().is_ok() {
            error!("blocking response stream delivery attempted inside Tokio runtime");
            return StreamDelivery::Closed;
        }
        if frame.is_first() {
            return self.route_first(frame, complete);
        }
        let Some(sender) = self.take_sender(&frame) else {
            return StreamDelivery::Closed;
        };
        if sender.sender.is_closed() {
            self.remove(frame.request_id);
            return StreamDelivery::Closed;
        }
        if frame.is_error() {
            return StreamDelivery::Accepted;
        }
        let trailers = frame.has_trailers().then(|| Self::trailers(&frame));
        let terminal = frame.is_terminal();
        let request_id = frame.request_id;
        let delivered = (frame.body.is_empty()
            || sender
                .sender
                .blocking_send(Ok(Frame::data(frame.body)))
                .is_ok())
            && trailers.is_none_or(|trailers| {
                sender
                    .sender
                    .blocking_send(Ok(Frame::trailers(trailers)))
                    .is_ok()
            });
        self.finish(request_id, terminal, delivered)
    }

    /// Deliver from an async reader without dropping frames when its channel fills.
    pub async fn deliver(
        &self,
        frame: ResponseStreamFrame,
        complete: impl FnOnce(u64, PendingResponse) -> bool,
    ) -> StreamDelivery {
        if frame.is_first() {
            return self.route_first(frame, complete);
        }
        let Some(sender) = self.take_sender(&frame) else {
            return StreamDelivery::Closed;
        };
        if sender.sender.is_closed() {
            self.remove(frame.request_id);
            return StreamDelivery::Closed;
        }
        if frame.is_error() {
            return StreamDelivery::Accepted;
        }
        let trailers = frame.has_trailers().then(|| Self::trailers(&frame));
        let terminal = frame.is_terminal();
        let request_id = frame.request_id;
        let mut delivered =
            frame.body.is_empty() || Self::send(&sender, Frame::data(frame.body)).await;
        if delivered && let Some(trailers) = trailers {
            delivered = Self::send(&sender, Frame::trailers(trailers)).await;
        }
        self.finish(request_id, terminal, delivered)
    }

    async fn send(entry: &StreamEntry, frame: Frame<Bytes>) -> bool {
        tokio::select! {
            biased;
            _ = entry.cancelled.cancelled() => false,
            sent = entry.sender.send(Ok(frame)) => sent.is_ok(),
        }
    }

    /// Associate a stream with its worker before releasing the correlation shard lock.
    pub fn set_worker(&self, request_id: u64, worker: u32) {
        if let Some(mut entry) = self.streams.get_mut(&request_id) {
            entry.worker = Some(worker);
        }
    }

    /// Close streams owned by a terminated worker, waking backpressured async deliveries.
    pub fn close_worker(&self, worker: u32) -> usize {
        let mut closed = 0;
        self.streams.retain(|_, entry| {
            if entry.worker == Some(worker) {
                entry.cancelled.cancel();
                closed += 1;
                false
            } else {
                true
            }
        });
        closed
    }

    /// Reclaim dropped receivers during the dispatcher's existing periodic sweep.
    pub fn sweep_closed(&self) {
        self.streams.retain(|_, entry| {
            if entry.sender.is_closed() {
                entry.cancelled.cancel();
                false
            } else {
                true
            }
        });
    }

    fn remove(&self, request_id: u64) -> Option<StreamEntry> {
        let entry = self.streams.remove(&request_id).map(|(_, entry)| entry);
        if let Some(entry) = &entry {
            entry.cancelled.cancel();
        }
        entry
    }

    /// Count responses whose producers still own an active stream channel.
    pub fn active_streams(&self) -> usize {
        self.streams.len()
    }

    fn route_first(
        &self,
        frame: ResponseStreamFrame,
        complete: impl FnOnce(u64, PendingResponse) -> bool,
    ) -> StreamDelivery {
        let headers = frame
            .iter_headers()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect::<Vec<_>>();
        let headers_mp = encode_response_headers(&headers);
        if frame.is_terminal() {
            return if complete(
                frame.request_id,
                PendingResponse::unary(frame.status, headers_mp, frame.body),
            ) {
                StreamDelivery::Accepted
            } else {
                StreamDelivery::Closed
            };
        }
        let (sender, receiver) = mpsc::channel(self.capacity);
        if !frame.body.is_empty() {
            sender
                .try_send(Ok(Frame::data(frame.body)))
                .expect("new stream channel has capacity");
        }
        let entry = StreamEntry {
            sender,
            worker: None,
            cancelled: CancellationToken::new(),
        };
        self.streams.insert(frame.request_id, entry.clone());
        if !complete(
            frame.request_id,
            PendingResponse::streaming(frame.status, headers_mp, receiver),
        ) || entry.sender.is_closed()
            || entry.cancelled.is_cancelled()
        {
            self.remove(frame.request_id);
            return StreamDelivery::Closed;
        }
        StreamDelivery::Accepted
    }

    fn take_sender(&self, frame: &ResponseStreamFrame) -> Option<StreamEntry> {
        if frame.is_error() {
            let sender = self.remove(frame.request_id);
            if sender.is_some() {
                warn!(
                    id = frame.request_id,
                    "worker terminated response stream with an error"
                );
            }
            return sender;
        }
        let sender = self
            .streams
            .get(&frame.request_id)
            .map(|entry| entry.value().clone());
        if sender.is_none() {
            debug!(
                id = frame.request_id,
                "no active response stream for continuation"
            );
        }
        sender
    }

    fn trailers(frame: &ResponseStreamFrame) -> HeaderMap {
        let mut trailers = HeaderMap::new();
        for (name, value) in frame.iter_headers() {
            if let (Ok(name), Ok(value)) =
                (HeaderName::from_bytes(name), HeaderValue::from_bytes(value))
            {
                trailers.append(name, value);
            }
        }
        trailers
    }

    fn finish(&self, request_id: u64, terminal: bool, delivered: bool) -> StreamDelivery {
        if terminal || !delivered {
            self.remove(request_id);
        }
        if delivered {
            StreamDelivery::Accepted
        } else {
            StreamDelivery::Closed
        }
    }
}
