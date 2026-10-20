//! Batch collector: aggregates incoming HTTP requests into batches.
//!
//! A batch is drained (pushed to the dispatcher) when either:
//! 1. The slot count reaches `policy.batch_size()`, or
//! 2. `policy.timeout()` has elapsed since the first request in the current batch.

use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use http::HeaderMap;
use hyper::body::Frame;
use tokio::sync::mpsc;
use tracing::{debug, trace};

use crate::dispatcher::Dispatcher;
use crate::policy::BatchPolicy;

/// Wrapper around a bounded frame receiver for progressive HTTP response streaming.
pub struct PendingResponseBody {
    rx: mpsc::Receiver<Result<Frame<Bytes>, Infallible>>,
}

impl PendingResponseBody {
    /// Wrap a frame receiver channel for streaming responses.
    pub fn new(rx: mpsc::Receiver<Result<Frame<Bytes>, Infallible>>) -> Self {
        Self { rx }
    }

    /// Consume the wrapper and return the underlying receiver.
    pub fn into_inner(self) -> mpsc::Receiver<Result<Frame<Bytes>, Infallible>> {
        self.rx
    }
}

impl std::fmt::Debug for PendingResponseBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingResponseBody")
            .finish_non_exhaustive()
    }
}

/// Progressive response frame collector that either buffers in memory or forwards
/// `Frame::data` and `Frame::trailers` immediately over a bounded `mpsc::Sender`.
pub struct ResponseCollector {
    chunks: Vec<Bytes>,
    trailers: Option<HeaderMap>,
    stream_tx: Option<mpsc::Sender<Result<Frame<Bytes>, Infallible>>>,
}

impl Default for ResponseCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl ResponseCollector {
    /// Create an in-memory collector.
    pub fn new() -> Self {
        Self {
            chunks: Vec::new(),
            trailers: None,
            stream_tx: None,
        }
    }

    /// Create a streaming collector that forwards frames immediately over `tx`.
    pub fn with_stream_sender(tx: mpsc::Sender<Result<Frame<Bytes>, Infallible>>) -> Self {
        Self {
            chunks: Vec::new(),
            trailers: None,
            stream_tx: Some(tx),
        }
    }

    /// Push a body data chunk to the stream channel or in-memory buffer.
    pub async fn push_chunk(
        &self,
        chunk: Bytes,
    ) -> Result<(), mpsc::error::SendError<Result<Frame<Bytes>, Infallible>>> {
        if let Some(tx) = &self.stream_tx {
            tx.send(Ok(Frame::data(chunk))).await?;
        }
        Ok(())
    }

    /// Push trailing headers (`Frame::trailers`) to the stream channel.
    pub async fn push_trailers(
        &self,
        trailers: HeaderMap,
    ) -> Result<(), mpsc::error::SendError<Result<Frame<Bytes>, Infallible>>> {
        if let Some(tx) = &self.stream_tx {
            tx.send(Ok(Frame::trailers(trailers))).await?;
        }
        Ok(())
    }

    /// Append a chunk to the in-memory buffer (synchronous collection path).
    pub fn collect_chunk(&mut self, chunk: Bytes) {
        self.chunks.push(chunk);
    }

    /// Set trailing headers on the in-memory collector.
    pub fn set_trailers(&mut self, trailers: HeaderMap) {
        self.trailers = Some(trailers);
    }

    /// Return trailing headers if any were collected.
    pub fn trailers(&self) -> Option<&HeaderMap> {
        self.trailers.as_ref()
    }

    /// Consume buffered chunks into a single `Bytes` buffer.
    pub fn into_bytes(self) -> Bytes {
        if self.chunks.is_empty() {
            return Bytes::new();
        }
        if self.chunks.len() == 1 {
            return self.chunks.into_iter().next().unwrap();
        }
        let total: usize = self.chunks.iter().map(Bytes::len).sum();
        let mut out = Vec::with_capacity(total);
        for chunk in self.chunks {
            out.extend_from_slice(&chunk);
        }
        Bytes::from(out)
    }
}

/// A single request item queued for batching.
#[derive(Debug)]
pub struct PendingRequest {
    /// Resolved handler ID from the router.
    pub handler_id: u64,
    /// HTTP method as a compact byte tag (1 byte).
    pub method_tag: u8,
    /// HTTP version tag (0=1.0, 1=1.1, 2=2, 3=3).
    pub http_version_tag: u8,
    /// Protocol flags (see `vertebra_core::protocol::flags`).
    pub flags: u32,
    /// Request URI path.
    pub path: String,
    /// Query string, if present.
    pub query: Option<String>,
    /// Serialised request headers.
    pub headers_mp: bytes::Bytes,
    /// Body bytes (may be empty).
    pub body: bytes::Bytes,
    /// One-shot channel for sending the response back to the Hyper task.
    pub reply: tokio::sync::oneshot::Sender<PendingResponse>,
}

/// A completed or streaming response returned from a Python worker through the ring buffer.
#[derive(Debug)]
pub struct PendingResponse {
    pub status: u16,
    pub headers_mp: bytes::Bytes,
    pub body: bytes::Bytes,
    pub stream_rx: Option<PendingResponseBody>,
}

impl PendingResponse {
    /// Construct a unary in-memory response.
    pub fn unary(status: u16, headers_mp: bytes::Bytes, body: bytes::Bytes) -> Self {
        Self {
            status,
            headers_mp,
            body,
            stream_rx: None,
        }
    }

    /// Construct a progressive streaming response.
    pub fn streaming(
        status: u16,
        headers_mp: bytes::Bytes,
        rx: mpsc::Receiver<Result<Frame<Bytes>, Infallible>>,
    ) -> Self {
        Self {
            status,
            headers_mp,
            body: Bytes::new(),
            stream_rx: Some(PendingResponseBody::new(rx)),
        }
    }
}

/// A batch ready to be pushed to the dispatcher.
pub struct Batch {
    pub requests: Vec<PendingRequest>,
}

/// Encodes an HTTP method as a single byte tag for compact storage.
pub fn method_tag(method: &hyper::Method) -> u8 {
    match method.as_str() {
        "GET" => 0,
        "POST" => 1,
        "PUT" => 2,
        "DELETE" => 3,
        "PATCH" => 4,
        "HEAD" => 5,
        "OPTIONS" => 6,
        "TRACE" => 7,
        "CONNECT" => 8,
        _ => 255,
    }
}

/// The request batcher task.
///
/// Spawn one of these per worker thread or per CPU. It receives `PendingRequest`
/// values over a channel, accumulates them, and drains when the policy fires.
pub struct RequestBatcher {
    rx: mpsc::Receiver<PendingRequest>,
    dispatcher: Arc<Dispatcher>,
    policy: BatchPolicy,
}

impl RequestBatcher {
    /// Create a new batcher and return the sender half of the channel.
    pub fn new(
        dispatcher: Arc<Dispatcher>,
        policy: BatchPolicy,
        channel_capacity: usize,
    ) -> (Self, mpsc::Sender<PendingRequest>) {
        let (tx, rx) = mpsc::channel(channel_capacity);
        (
            Self {
                rx,
                dispatcher,
                policy,
            },
            tx,
        )
    }

    /// Spawn a pool of batchers and return their senders.
    pub fn spawn_pool(
        dispatcher: Arc<Dispatcher>,
        policy: BatchPolicy,
        shard_count: usize,
        channel_capacity: usize,
    ) -> Vec<mpsc::Sender<PendingRequest>> {
        let mut senders = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            let (collector, tx) =
                Self::new(Arc::clone(&dispatcher), policy.clone(), channel_capacity);
            tokio::spawn(collector.run());
            senders.push(tx);
        }
        senders
    }

    /// Run the batcher loop until the channel is closed.
    pub async fn run(mut self) {
        let mut pending: Vec<PendingRequest> = Vec::with_capacity(self.policy.batch_size());

        while let Some(req) = self.rx.recv().await {
            let max_size = self.policy.batch_size();
            pending.push(req);

            while pending.len() < max_size {
                match self.rx.try_recv() {
                    Ok(r) => pending.push(r),
                    Err(_) => break,
                }
            }

            trace!(depth = pending.len(), "queued request(s)");
            self.drain(&mut pending).await;
        }

        if !pending.is_empty() {
            self.drain(&mut pending).await;
        }
        debug!("RequestBatcher shutting down");
    }

    async fn drain(&mut self, pending: &mut Vec<PendingRequest>) {
        let count = pending.len();
        debug!(count, "draining batch");
        let batch = Batch {
            requests: std::mem::replace(pending, Vec::with_capacity(self.policy.batch_size())),
        };
        self.dispatcher.dispatch(batch);
        self.policy.record_batch(count);
    }
}
