//! Request batcher: aggregates incoming HTTP requests into batches.
//!
//! A batch is drained (pushed to the dispatcher) opportunistically without linger,
//! bounded by `BatchLimit { max: usize }`.

use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tracing::{debug, trace};

pub use crate::body::PendingResponse;
use crate::dispatcher::Dispatcher;

/// Configurable upper bound on requests per batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchLimit {
    pub max: usize,
}

impl BatchLimit {
    #[inline]
    pub const fn new(max: usize) -> Self {
        Self { max }
    }

    #[inline]
    pub const fn batch_size(&self) -> usize {
        self.max
    }
}

impl Default for BatchLimit {
    fn default() -> Self {
        Self { max: 64 }
    }
}

impl From<usize> for BatchLimit {
    fn from(max: usize) -> Self {
        Self { max }
    }
}

/// Compact representation of HTTP method and version tags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestTags {
    pub method_tag: u8,
    pub http_version_tag: u8,
}

impl RequestTags {
    #[inline]
    pub const fn new(method_tag: u8, http_version_tag: u8) -> Self {
        Self {
            method_tag,
            http_version_tag,
        }
    }

    #[inline]
    pub fn from_parts(method: &hyper::Method, version: hyper::Version) -> Self {
        Self {
            method_tag: method_tag(method),
            http_version_tag: http_version_tag(version),
        }
    }
}

/// Encodes an HTTP method as a single byte tag for compact storage.
#[inline]
pub fn method_tag(method: &hyper::Method) -> u8 {
    crate::routing::standard_method_tag(method.as_str())
}

/// Encodes an HTTP version as a single byte tag.
#[inline]
pub fn http_version_tag(version: hyper::Version) -> u8 {
    match version {
        hyper::Version::HTTP_09 => 0,
        hyper::Version::HTTP_10 => 1,
        hyper::Version::HTTP_11 => 2,
        hyper::Version::HTTP_2 => 3,
        hyper::Version::HTTP_3 => 4,
        _ => 1,
    }
}

/// A request pending dispatch to a worker.
pub struct PendingRequest {
    pub handler_id: u64,
    pub method_tag: u8,
    pub http_version_tag: u8,
    pub flags: u32,
    pub path_and_query: http::uri::PathAndQuery,
    pub headers_mp: bytes::Bytes,
    pub body: bytes::Bytes,
    pub reply: oneshot::Sender<PendingResponse>,
}

impl PendingRequest {
    #[inline]
    pub fn path(&self) -> &str {
        self.path_and_query.path()
    }

    #[inline]
    pub fn query(&self) -> Option<&str> {
        self.path_and_query.query()
    }
}

/// A batch of requests ready to be pushed to the dispatcher.
pub struct Batch {
    pub requests: Vec<PendingRequest>,
}

/// The request batcher task.
///
/// Spawn one of these per worker thread or CPU. It receives `PendingRequest`
/// values over a channel and drains opportunistically without linger.
pub struct RequestBatcher {
    rx: mpsc::Receiver<PendingRequest>,
    dispatcher: Arc<Dispatcher>,
    limit: BatchLimit,
}

impl RequestBatcher {
    /// Create a new batcher and return the sender half of the channel.
    pub fn new(
        dispatcher: Arc<Dispatcher>,
        limit: BatchLimit,
        channel_capacity: usize,
    ) -> (Self, mpsc::Sender<PendingRequest>) {
        let (tx, rx) = mpsc::channel(channel_capacity);
        (
            Self {
                rx,
                dispatcher,
                limit,
            },
            tx,
        )
    }

    /// Spawn a pool of batchers and return their senders.
    pub fn spawn_pool(
        dispatcher: Arc<Dispatcher>,
        limit: BatchLimit,
        shard_count: usize,
        channel_capacity: usize,
    ) -> Vec<mpsc::Sender<PendingRequest>> {
        let mut senders = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            let (collector, tx) = Self::new(Arc::clone(&dispatcher), limit, channel_capacity);
            tokio::spawn(collector.run());
            senders.push(tx);
        }
        senders
    }

    /// Run the batcher loop until the channel is closed.
    pub async fn run(mut self) {
        let max_size = self.limit.max;
        let mut pending: Vec<PendingRequest> = Vec::with_capacity(max_size);

        while let Some(req) = self.rx.recv().await {
            pending.push(req);

            while pending.len() < max_size {
                match self.rx.try_recv() {
                    Ok(r) => pending.push(r),
                    Err(_) => break,
                }
            }

            trace!(depth = pending.len(), "queued request(s)");
            self.drain(&mut pending);
        }

        if !pending.is_empty() {
            self.drain(&mut pending);
        }
        debug!("RequestBatcher shutting down");
    }

    fn drain(&mut self, pending: &mut Vec<PendingRequest>) {
        let count = pending.len();
        debug!(count, "draining batch");
        let batch = Batch {
            requests: std::mem::replace(pending, Vec::with_capacity(self.limit.max)),
        };
        self.dispatcher.dispatch(batch);
    }
}
