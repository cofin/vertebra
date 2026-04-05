//! Batch collector: aggregates incoming HTTP requests into batches.
//!
//! A batch is drained (pushed to the dispatcher) when either:
//! 1. The slot count reaches `policy.batch_size()`, or
//! 2. `policy.timeout()` has elapsed since the first request in the current batch.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tracing::{debug, trace};

use crate::dispatcher::Dispatcher;
use crate::policy::DrainPolicy;

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

/// A completed response returned from a Python worker through the ring buffer.
#[derive(Debug)]
pub struct PendingResponse {
    pub status: u16,
    pub headers_mp: bytes::Bytes,
    pub body: bytes::Bytes,
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

/// The batch collector task.
///
/// Spawn one of these per worker thread or per CPU. It receives `PendingRequest`
/// values over a channel, accumulates them, and drains when the policy fires.
pub struct BatchCollector {
    rx: mpsc::Receiver<PendingRequest>,
    dispatcher: Arc<Dispatcher>,
    policy: DrainPolicy,
}

impl BatchCollector {
    /// Create a new collector and return the sender half of the channel.
    pub fn new(
        dispatcher: Arc<Dispatcher>,
        policy: DrainPolicy,
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

    /// Spawn a pool of collectors and return their senders.
    pub fn spawn_pool(
        dispatcher: Arc<Dispatcher>,
        policy: DrainPolicy,
        shard_count: usize,
        channel_capacity: usize,
    ) -> Vec<mpsc::Sender<PendingRequest>> {
        let mut senders = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            let (collector, tx) = Self::new(Arc::clone(&dispatcher), policy.clone(), channel_capacity);
            tokio::spawn(collector.run());
            senders.push(tx);
        }
        senders
    }

    /// Run the collector loop until the channel is closed.
    pub async fn run(mut self) {
        let mut pending: Vec<PendingRequest> = Vec::with_capacity(self.policy.batch_size());
        let mut deadline: Option<Instant> = None;
        let timeout = self.policy.timeout();

        loop {
            let sleep = match deadline {
                Some(d) => {
                    let now = Instant::now();
                    if now >= d {
                        Duration::ZERO
                    } else {
                        d - now
                    }
                }
                None => Duration::from_secs(3600),
            };

            tokio::select! {
                biased;

                maybe_req = self.rx.recv() => {
                    match maybe_req {
                        None => {
                            if !pending.is_empty() {
                                self.drain(&mut pending).await;
                            }
                            debug!("BatchCollector shutting down");
                            return;
                        }
                        Some(req) => {
                            if deadline.is_none() {
                                deadline = Some(Instant::now() + timeout);
                            }
                            let has_priority =
                                req.flags & vertebra_core::protocol::flags::PRIORITY != 0;
                            pending.push(req);

                            // Eagerly drain any buffered requests (try_recv loop).
                            while pending.len() < self.policy.batch_size() {
                                match self.rx.try_recv() {
                                    Ok(r) => pending.push(r),
                                    Err(_) => break,
                                }
                            }

                            trace!(depth = pending.len(), "queued request(s)");

                            // PRIORITY: drain immediately — don't wait for batch
                            // size or timeout. The priority request (and any
                            // already-accumulated requests) go out in one batch.
                            if has_priority
                                || pending.len() >= self.policy.batch_size()
                                || self.policy.matches_preferred(pending.len())
                            {
                                self.drain(&mut pending).await;
                                deadline = None;
                            }
                        }
                    }
                }

                _ = tokio::time::sleep(sleep), if deadline.is_some() => {
                    if !pending.is_empty() {
                        self.drain(&mut pending).await;
                    }
                    deadline = None;
                }
            }
        }
    }

    async fn drain(&mut self, pending: &mut Vec<PendingRequest>) {
        let count = pending.len();
        debug!(count, "draining batch");
        let batch = Batch {
            requests: std::mem::replace(pending, Vec::with_capacity(self.policy.batch_size())),
        };
        self.dispatcher.dispatch(batch);
        self.policy.adapt(count);
    }
}
