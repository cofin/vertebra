//! Bounded progressive response delivery shared by embedded and SHM dispatchers.

use std::convert::Infallible;

use bytes::Bytes;
use dashmap::DashMap;
use http::{HeaderMap, HeaderName, HeaderValue};
use hyper::body::Frame;
use tokio::sync::mpsc;
use tracing::{debug, error, warn};
use vertebra_core::bridge::{ResponseStreamFrame, StreamDelivery};

use crate::body::PendingResponse;
use crate::response::encode_response_headers;

type FrameSender = mpsc::Sender<Result<Frame<Bytes>, Infallible>>;

/// Per-response bounded channels with producer backpressure and disconnect reporting.
pub struct ResponseStreamRegistry {
    streams: DashMap<u64, FrameSender>,
    capacity: usize,
}

impl Default for ResponseStreamRegistry {
    fn default() -> Self {
        Self::with_capacity(64)
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
        if sender.is_closed() {
            self.streams.remove(&frame.request_id);
            return StreamDelivery::Closed;
        }
        if frame.is_error() {
            return StreamDelivery::Accepted;
        }
        let trailers = frame.has_trailers().then(|| Self::trailers(&frame));
        let terminal = frame.is_terminal();
        let request_id = frame.request_id;
        let delivered = (frame.body.is_empty()
            || sender.blocking_send(Ok(Frame::data(frame.body))).is_ok())
            && trailers
                .is_none_or(|trailers| sender.blocking_send(Ok(Frame::trailers(trailers))).is_ok());
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
        if sender.is_closed() {
            self.streams.remove(&frame.request_id);
            return StreamDelivery::Closed;
        }
        if frame.is_error() {
            return StreamDelivery::Accepted;
        }
        let trailers = frame.has_trailers().then(|| Self::trailers(&frame));
        let terminal = frame.is_terminal();
        let request_id = frame.request_id;
        let mut delivered =
            frame.body.is_empty() || sender.send(Ok(Frame::data(frame.body))).await.is_ok();
        if delivered && let Some(trailers) = trailers {
            delivered = sender.send(Ok(Frame::trailers(trailers))).await.is_ok();
        }
        self.finish(request_id, terminal, delivered)
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
        if !complete(
            frame.request_id,
            PendingResponse::streaming(frame.status, headers_mp, receiver),
        ) {
            return StreamDelivery::Closed;
        }
        if sender.is_closed() {
            return StreamDelivery::Closed;
        }
        self.streams.insert(frame.request_id, sender);
        StreamDelivery::Accepted
    }

    fn take_sender(&self, frame: &ResponseStreamFrame) -> Option<FrameSender> {
        if frame.is_error() {
            let sender = self
                .streams
                .remove(&frame.request_id)
                .map(|(_, sender)| sender);
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
            self.streams.remove(&request_id);
        }
        if delivered {
            StreamDelivery::Accepted
        } else {
            StreamDelivery::Closed
        }
    }
}
