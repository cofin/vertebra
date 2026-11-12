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

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

#[cfg(unix)]
use tokio::io::unix::AsyncFd;

use vertebra_core::bridge::ResponseStreamFrame;
use vertebra_core::transport::ring_pair::RingPair;

use crate::body::PendingResponse;
use crate::dispatcher::Dispatcher;

pub use vertebra_core::ipc::{
    FLAG_ERROR, FLAG_FINAL, FLAG_HEADERS_SENT, FLAG_STREAMING, FLAG_TRAILERS,
};

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

/// Linux/Unix: Optimized loop using AsyncFd with Dekker `consumer_sleeping` and cancellation select.
#[cfg(unix)]
async fn run_worker_reader_async(
    worker_id: usize,
    worker: Arc<RingPair>,
    dispatcher: Arc<Dispatcher>,
    shutdown: CancellationToken,
    async_fd: AsyncFd<NotifierFd>,
) {
    loop {
        let mut drained_any = false;
        while drain_worker(worker_id, &worker, &dispatcher, &shutdown).await {
            drained_any = true;
        }

        if drained_any {
            tokio::task::yield_now().await;
            if drain_worker(worker_id, &worker, &dispatcher, &shutdown).await {
                continue;
            }
        }

        if !worker.prepare_response_sleep() {
            continue;
        }

        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                worker.cancel_response_sleep();
                break;
            }
            res = async_fd.readable() => {
                worker.cancel_response_sleep();
                match res {
                    Ok(mut guard) => {
                        let _ = guard.try_io(|inner| {
                            let mut buf = [0u8; 8];
                            loop {
                                let rc = unsafe {
                                    libc::read(
                                        inner.get_ref().as_raw_fd(),
                                        buf.as_mut_ptr().cast::<libc::c_void>(),
                                        buf.len(),
                                    )
                                };
                                if rc < 0 {
                                    return Err(std::io::Error::last_os_error());
                                }
                                if rc == 0 {
                                    return Ok(());
                                }
                            }
                        });
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
    while drain_worker(worker_id, &worker, &dispatcher, &shutdown).await {}
    if let Some(shm) = dispatcher.shm_dispatcher() {
        shm.sweep_worker(worker_id as u32);
    }
}

/// Fallback: Periodic polling of the response rings with cancellation select.
async fn run_worker_reader_polling(
    worker_id: usize,
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
                while drain_worker(worker_id, &worker, &dispatcher, &shutdown).await {}
            }
        }
    }

    while drain_worker(worker_id, &worker, &dispatcher, &shutdown).await {}
    if let Some(shm) = dispatcher.shm_dispatcher() {
        shm.sweep_worker(worker_id as u32);
    }
}

/// Drain owned stream frames before awaiting client capacity; unary completions keep their fast path.
async fn drain_worker(
    worker_id: usize,
    worker: &Arc<RingPair>,
    dispatcher: &Dispatcher,
    shutdown: &CancellationToken,
) -> bool {
    use bytes::Bytes;
    let mut completions = Vec::new();
    let mut stream_frames = Vec::new();
    match worker.drain_frames_with(1024, |frame| {
        let headers = Bytes::copy_from_slice(frame.headers_raw);
        let body = Bytes::copy_from_slice(frame.body);
        if frame.flags & FLAG_STREAMING != 0 {
            stream_frames.push(ResponseStreamFrame {
                request_id: frame.correlation_id,
                status: frame.status,
                flags: frame.flags,
                headers_raw: headers,
                body,
            });
        } else {
            completions.push((
                frame.correlation_id,
                PendingResponse::unary(frame.status, headers, body),
            ));
        }
    }) {
        Ok(_) => {}
        Err(error) => warn!(error = %error, "error reading response ring"),
    }
    let any_received = !completions.is_empty() || !stream_frames.is_empty();
    if completions.len() == 1 {
        let (id, response) = completions.pop().unwrap();
        dispatcher.complete(id, response);
    } else if !completions.is_empty() {
        dispatcher.complete_many(completions);
    }
    for frame in stream_frames {
        tokio::select! {
            biased;
            _ = dispatcher.deliver_stream_frame(frame) => {},
            _ = shutdown.cancelled() => {
                if let Some(shm) = dispatcher.shm_dispatcher() { shm.sweep_worker(worker_id as u32); }
                return false;
            }
        }
    }
    any_received
}
