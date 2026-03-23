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
use std::os::fd::{AsRawFd, RawFd};

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

#[cfg(unix)]
use tokio::io::unix::AsyncFd;

use vertebra_core::transport::ring_pair::RingPair;

use crate::collector::PendingResponse;
use crate::dispatcher::Dispatcher;

/// Wrapper around a raw FD for `AsyncFd` registration.
///
/// Does NOT own the FD — the `RingPair` owns it. This is just a handle
/// for Tokio's reactor to poll on.
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

/// Run the response reader system by spawning a per-worker reader task.
///
/// Each worker task uses AsyncFd-based wakeup if supported by the platform
/// and OS. The system waits for all worker tasks to exit during shutdown.
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

    // Wait for all worker tasks to complete (happens on shutdown or error)
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
        match AsyncFd::new(NotifierFd { fd }) {
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

/// Linux/Unix: Optimized loop using AsyncFd to wait for eventfd signals.
#[cfg(unix)]
async fn run_worker_reader_async(
    worker_id: usize,
    worker: Arc<RingPair>,
    dispatcher: Arc<Dispatcher>,
    shutdown: CancellationToken,
    async_fd: AsyncFd<NotifierFd>,
) {
    loop {
        if shutdown.is_cancelled() {
            break;
        }

        // Wait for the worker to signal the eventfd
        match async_fd.readable().await {
            Ok(mut guard) => {
                // IMPORTANT: The sequence here is critical to avoid race conditions.
                // 1. Clear the eventfd/pipe (read 8 bytes) to consume the signal.
                let _ = worker.response_notifier.clear();

                // 2. Call clear_ready() to tell Tokio to re-poll later.
                // We do this BEFORE draining so that if a new notification arrives
                // while we are draining, the FD will be marked readable again
                // for the next iteration.
                guard.clear_ready();

                // 3. Drain all available responses from the rings.
                drain_worker(&worker, &dispatcher);
            }
            Err(e) => {
                error!(worker_id, error = %e, "AsyncFd readable error; stopping worker reader");
                break;
            }
        }
    }

    // Final drain after shutdown
    drain_worker(&worker, &dispatcher);
}

/// Fallback: Periodic polling of the response rings.
async fn run_worker_reader_polling(
    _worker_id: usize,
    worker: Arc<RingPair>,
    dispatcher: Arc<Dispatcher>,
    shutdown: CancellationToken,
) {
    use std::time::Duration;
    let poll_interval = Duration::from_micros(100);

    loop {
        if shutdown.is_cancelled() {
            break;
        }

        if !drain_worker(&worker, &dispatcher) {
            // Nothing received, sleep briefly
            tokio::select! {
                _ = tokio::time::sleep(poll_interval) => {}
                _ = shutdown.cancelled() => break,
            }
        }
    }

    // Final drain
    drain_worker(&worker, &dispatcher);
}

/// Drain all available responses from a worker's tiered rings.
///
/// Returns `true` if at least one response was received.
fn drain_worker(worker: &Arc<RingPair>, dispatcher: &Dispatcher) -> bool {
    let mut any_received = false;
    loop {
        match worker.response_rings.try_recv_response() {
            Ok(Some(resp)) => {
                any_received = true;
                dispatcher.complete(
                    resp.correlation_id,
                    PendingResponse {
                        status: resp.status,
                        headers_mp: bytes::Bytes::from(resp.headers_raw),
                        body: bytes::Bytes::from(resp.body),
                    },
                );
            }
            Ok(None) => break,
            Err(e) => {
                warn!(error = %e, "error reading response ring");
                break;
            }
        }
    }
    any_received
}
