//! Async response reader for SharedMemory workers.
//!
//! Registers each worker's response notification eventfd/pipe with Tokio's reactor
//! via `AsyncFd`, achieving zero-poll wakeup — the kernel notifies Tokio directly
//! when a worker writes a response and signals the eventfd.
//!
//! On each wakeup, drains all available responses from the tiered response rings
//! and completes the corresponding correlations in the dispatcher.

use std::sync::Arc;

#[cfg(unix)]
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};

use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

#[cfg(unix)]
use tokio::io::unix::AsyncFd;

use vertebra_core::transport::ring_pair::RingPair;

use crate::body::PendingResponse;
use crate::dispatcher::Dispatcher;

pub use vertebra_core::ipc::{
    FLAG_ERROR, FLAG_FINAL, FLAG_HEADERS_SENT, FLAG_STREAMING, FLAG_TRAILERS,
};

/// Responder channel variant for an in-flight request ID.
pub enum PendingResponder {
    /// Single-frame unary response channel.
    Unary(oneshot::Sender<Vec<u8>>),
    /// Multi-frame streaming response channel kept open until `FLAG_FINAL` or `FLAG_ERROR`.
    Stream(mpsc::Sender<Vec<u8>>),
}

/// Concurrent correlation map supporting both unary and multi-frame streaming responses.
#[derive(Default)]
pub struct PendingRequests {
    map: DashMap<u64, PendingResponder>,
}

impl PendingRequests {
    /// Create an empty pending request registry.
    pub fn new() -> Self {
        Self {
            map: DashMap::new(),
        }
    }

    /// Number of currently active request correlations.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Return `true` when no request correlations are active.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Register a single-shot unary response channel for `request_id`.
    pub fn register_unary(&self, request_id: u64) -> oneshot::Receiver<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        self.map.insert(request_id, PendingResponder::Unary(tx));
        rx
    }

    /// Register a bounded multi-frame streaming channel for `request_id`.
    pub fn register_stream(&self, request_id: u64, capacity: usize) -> mpsc::Receiver<Vec<u8>> {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        self.map.insert(request_id, PendingResponder::Stream(tx));
        rx
    }

    /// Inspect raw frame header `[request_id: u64][status: u16][flags: u16]` and route to the registered responder.
    pub fn deliver_raw_frame(&self, raw_bytes: Vec<u8>) -> bool {
        if raw_bytes.len() < 12 {
            return false;
        }
        let request_id = u64::from_le_bytes(raw_bytes[0..8].try_into().unwrap());
        let flags = u16::from_le_bytes(raw_bytes[10..12].try_into().unwrap());
        let is_streaming = (flags & FLAG_STREAMING) != 0;
        let is_terminal = !is_streaming || (flags & (FLAG_FINAL | FLAG_ERROR)) != 0;

        if is_terminal {
            let Some((_, responder)) = self.map.remove(&request_id) else {
                return false;
            };
            match responder {
                PendingResponder::Unary(tx) => tx.send(raw_bytes).is_ok(),
                PendingResponder::Stream(tx) => tx.try_send(raw_bytes).is_ok(),
            }
        } else {
            let Some(entry) = self.map.get(&request_id) else {
                return false;
            };
            match entry.value() {
                PendingResponder::Stream(tx) => tx.try_send(raw_bytes).is_ok(),
                PendingResponder::Unary(_) => false,
            }
        }
    }
}

/// Wrapper around a raw FD for `AsyncFd` registration.
#[cfg(unix)]
struct NotifierFd {
    fd: RawFd,
}

#[cfg(unix)]
impl AsRawFd for NotifierFd {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

#[cfg(unix)]
impl AsFd for NotifierFd {
    fn as_fd(&self) -> BorrowedFd<'_> {
        unsafe { BorrowedFd::borrow_raw(self.fd) }
    }
}

/// Spawn response reader tasks, returning a JoinHandle.
pub fn spawn_response_readers(
    workers: Vec<Arc<RingPair>>,
    dispatcher: Arc<Dispatcher>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_response_reader(workers, dispatcher, shutdown))
}

/// Run the response reader system by spawning a per-worker reader task.
pub async fn run_response_reader(
    workers: Vec<Arc<RingPair>>,
    dispatcher: Arc<Dispatcher>,
    shutdown: CancellationToken,
) {
    if workers.is_empty() {
        return;
    }

    debug!(workers = workers.len(), "response reader started");

    let mut set = JoinSet::new();

    for (i, worker) in workers.into_iter().enumerate() {
        let dispatcher = Arc::clone(&dispatcher);
        let shutdown = shutdown.clone();
        set.spawn(async move {
            run_worker_reader(i, worker, dispatcher, shutdown).await;
        });
    }

    while set.join_next().await.is_some() {}

    debug!("response reader stopped");
}

/// Internal loop for a single worker's response ring.
async fn run_worker_reader(
    worker_id: usize,
    worker: Arc<RingPair>,
    dispatcher: Arc<Dispatcher>,
    shutdown: CancellationToken,
) {
    #[cfg(unix)]
    {
        let fd = worker.response_notifier.as_raw_fd();
        match unsafe { AsyncFd::register(NotifierFd { fd }) } {
            Ok(async_fd) => {
                run_worker_reader_async(worker_id, worker, dispatcher, shutdown, async_fd).await;
            }
            Err(e) => {
                error!(worker_id, error = %e, "failed to register response notifier with Tokio; falling back to polling");
                run_worker_reader_polling(worker_id, worker, dispatcher, shutdown).await;
            }
        }
    }

    #[cfg(not(unix))]
    {
        run_worker_reader_polling(worker_id, worker, dispatcher, shutdown).await;
    }
}

/// Linux/Unix: Optimized loop using AsyncFd with cancellation select.
#[cfg(unix)]
async fn run_worker_reader_async(
    worker_id: usize,
    worker: Arc<RingPair>,
    dispatcher: Arc<Dispatcher>,
    shutdown: CancellationToken,
    async_fd: AsyncFd<NotifierFd>,
) {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                break;
            }
            res = async_fd.readable() => {
                match res {
                    Ok(mut guard) => {
                        let _ = worker.response_notifier.clear();
                        guard.clear_ready();
                        drain_worker(&worker, &dispatcher);
                    }
                    Err(e) => {
                        error!(worker_id, error = %e, "AsyncFd readable error; stopping worker reader");
                        break;
                    }
                }
            }
        }
    }

    // Final drain after shutdown
    drain_worker(&worker, &dispatcher);
}

/// Fallback: Periodic polling of the response rings with cancellation select.
async fn run_worker_reader_polling(
    _worker_id: usize,
    worker: Arc<RingPair>,
    dispatcher: Arc<Dispatcher>,
    shutdown: CancellationToken,
) {
    use std::time::Duration;
    let poll_interval = Duration::from_micros(100);

    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(poll_interval) => {
                drain_worker(&worker, &dispatcher);
            }
        }
    }

    drain_worker(&worker, &dispatcher);
}

/// Drain all available responses from a worker's tiered rings.
fn drain_worker(worker: &Arc<RingPair>, dispatcher: &Dispatcher) -> bool {
    let mut any_received = false;
    let mut completions = Vec::new();
    loop {
        match worker.response_rings.try_recv_response() {
            Ok(Some(resp)) => {
                any_received = true;
                completions.push((
                    resp.correlation_id,
                    PendingResponse {
                        status: resp.status,
                        headers_mp: resp.headers_raw,
                        body: resp.body,
                        stream_rx: None,
                    },
                ));
            }
            Ok(None) => break,
            Err(e) => {
                warn!(error = %e, "error reading response ring");
                break;
            }
        }
    }
    if !completions.is_empty() {
        dispatcher.complete_many(completions);
    }
    any_received
}
