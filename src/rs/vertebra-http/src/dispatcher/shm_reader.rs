//! Async response reader for SharedMemory workers.
//!
//! Registers each worker's response notification eventfd/pipe with Tokio's reactor
//! via `AsyncFd`, achieving zero-poll wakeup — the kernel notifies Tokio directly
//! when a worker writes a response and signals the eventfd.
//!
//! On each wakeup, drains all available responses from the tiered response rings
//! and completes the corresponding correlations in the dispatcher.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[cfg(unix)]
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};

use bytes::Bytes;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

#[cfg(unix)]
use tokio::io::unix::AsyncFd;

use vertebra_core::bridge::{ResponseStreamFrame, StreamDelivery};
use vertebra_core::transport::ring_pair::RingPair;

use super::streams::DEFAULT_STREAM_QUEUE_DEPTH;
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
        // SAFETY: `self.fd` is borrowed from `worker.response_notifier`, and `worker: Arc<RingPair>`
        // keeps that `Notifier` FD open for the entire lifetime of `AsyncFd<NotifierFd>`.
        unsafe { BorrowedFd::borrow_raw(self.fd) }
    }
}

struct ReaderHandle {
    shutdown: CancellationToken,
    handle: JoinHandle<()>,
}

/// Manages per-worker SHM response reader lifecycles for safe ring reclamation and respawn.
#[derive(Default)]
pub struct ResponseReaderRegistry {
    readers: Mutex<HashMap<usize, ReaderHandle>>,
}

impl ResponseReaderRegistry {
    /// Create an empty reader registry.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Spawn one response reader task per worker ring pair.
    pub fn spawn_all(
        workers: Vec<Arc<RingPair>>,
        dispatcher: Arc<Dispatcher>,
        shutdown: CancellationToken,
    ) -> Arc<Self> {
        Self::spawn_all_with_ready(workers, dispatcher, shutdown, None)
    }

    /// Spawn one response reader task per worker, notifying `ready` as each enters its wait loop.
    pub fn spawn_all_with_ready(
        workers: Vec<Arc<RingPair>>,
        dispatcher: Arc<Dispatcher>,
        shutdown: CancellationToken,
        ready: Option<Arc<Notify>>,
    ) -> Arc<Self> {
        let registry = Self::new();
        for (worker_id, worker) in workers.into_iter().enumerate() {
            registry.spawn_worker_reader_with_ready(
                worker_id,
                worker,
                Arc::clone(&dispatcher),
                shutdown.clone(),
                ready.clone(),
            );
        }
        registry
    }

    /// Spawn (or respawn) the response reader task for `worker_id`.
    pub fn spawn_worker_reader(
        &self,
        worker_id: usize,
        worker: Arc<RingPair>,
        dispatcher: Arc<Dispatcher>,
        shutdown: CancellationToken,
    ) {
        self.spawn_worker_reader_with_ready(worker_id, worker, dispatcher, shutdown, None);
    }

    /// Spawn the response reader task for `worker_id`, notifying `ready` when armed.
    pub fn spawn_worker_reader_with_ready(
        &self,
        worker_id: usize,
        worker: Arc<RingPair>,
        dispatcher: Arc<Dispatcher>,
        shutdown: CancellationToken,
        ready: Option<Arc<Notify>>,
    ) {
        let worker_shutdown = shutdown.child_token();
        let task_shutdown = worker_shutdown.clone();
        let handle = tokio::spawn(async move {
            run_worker_reader(worker_id, worker, dispatcher, task_shutdown, ready).await;
        });

        let previous = {
            let mut guard = self.readers.lock().unwrap();
            guard.insert(
                worker_id,
                ReaderHandle {
                    shutdown: worker_shutdown,
                    handle,
                },
            )
        };
        if let Some(prev) = previous {
            warn!(
                worker_id,
                "replacing active response reader without prior stop"
            );
            prev.shutdown.cancel();
        }
    }

    /// Cancel and await termination of `worker_id`'s response reader before ring reclamation.
    pub async fn stop_and_join_worker(&self, worker_id: usize) {
        let entry = {
            let mut guard = self.readers.lock().unwrap();
            guard.remove(&worker_id)
        };
        if let Some(entry) = entry {
            entry.shutdown.cancel();
            if let Err(err) = entry.handle.await {
                error!(worker_id, error = %err, "worker response reader task failed");
            }
        }
    }

    /// Cancel and await termination of every registered response reader task.
    pub async fn stop_and_join_all(&self) {
        let entries: Vec<(usize, ReaderHandle)> = {
            let mut guard = self.readers.lock().unwrap();
            guard.drain().collect()
        };
        for (_, entry) in &entries {
            entry.shutdown.cancel();
        }
        for (worker_id, entry) in entries {
            if let Err(err) = entry.handle.await {
                error!(worker_id, error = %err, "worker response reader task failed");
            }
        }
    }

    /// Await all currently registered response reader tasks without cancelling them.
    pub async fn join_all(&self) {
        let entries: Vec<(usize, ReaderHandle)> = {
            let mut guard = self.readers.lock().unwrap();
            guard.drain().collect()
        };
        for (worker_id, entry) in entries {
            if let Err(err) = entry.handle.await {
                error!(worker_id, error = %err, "worker response reader task failed");
            }
        }
    }

    /// Return the number of registered worker reader handles.
    pub fn active_readers(&self) -> usize {
        self.readers.lock().unwrap().len()
    }
}

/// Spawn response reader tasks, returning a JoinHandle.
pub fn spawn_response_readers(
    workers: Vec<Arc<RingPair>>,
    dispatcher: Arc<Dispatcher>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(run_response_reader(workers, dispatcher, shutdown))
}

/// Spawn response reader tasks and signal `ready` once each worker reader enters its wait loop.
pub fn spawn_response_readers_with_ready(
    workers: Vec<Arc<RingPair>>,
    dispatcher: Arc<Dispatcher>,
    shutdown: CancellationToken,
    ready: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(run_response_reader_with_ready(
        workers,
        dispatcher,
        shutdown,
        Some(ready),
    ))
}

/// Run the response reader system by spawning a per-worker reader task.
pub async fn run_response_reader(
    workers: Vec<Arc<RingPair>>,
    dispatcher: Arc<Dispatcher>,
    shutdown: CancellationToken,
) {
    run_response_reader_with_ready(workers, dispatcher, shutdown, None).await;
}

async fn run_response_reader_with_ready(
    workers: Vec<Arc<RingPair>>,
    dispatcher: Arc<Dispatcher>,
    shutdown: CancellationToken,
    ready: Option<Arc<Notify>>,
) {
    if workers.is_empty() {
        if let Some(signal) = ready {
            signal.notify_one();
        }
        return;
    }

    debug!(workers = workers.len(), "response reader started");
    let registry =
        ResponseReaderRegistry::spawn_all_with_ready(workers, dispatcher, shutdown, ready);
    registry.join_all().await;
    debug!("response reader stopped");
}

struct StreamPump {
    tx: mpsc::Sender<ResponseStreamFrame>,
    handle: JoinHandle<()>,
}

#[derive(Default)]
struct StreamPumpTable {
    streams: HashMap<u64, Option<StreamPump>>,
    finishing: Vec<JoinHandle<()>>,
}

impl StreamPumpTable {
    fn prune_finished(&mut self) {
        self.streams
            .retain(|_, pump| !matches!(pump, Some(entry) if entry.tx.is_closed()));
        self.finishing.retain(|handle| !handle.is_finished());
    }

    async fn dispatch(
        &mut self,
        frame: ResponseStreamFrame,
        dispatcher: &Arc<Dispatcher>,
        shutdown: &CancellationToken,
    ) {
        if frame.is_first() {
            let request_id = frame.request_id;
            let terminal = frame.is_terminal();
            let delivery = dispatcher.deliver_stream_frame(frame).await;
            if !terminal && delivery == StreamDelivery::Accepted {
                self.streams.insert(request_id, None);
            }
            return;
        }

        let request_id = frame.request_id;
        let terminal_or_error = frame.is_terminal() || frame.is_error();
        let Some(slot) = self.streams.get_mut(&request_id) else {
            return;
        };

        if let Some(pump) = slot {
            match pump.tx.try_send(frame) {
                Ok(()) => {
                    if terminal_or_error && let Some(Some(pump)) = self.streams.remove(&request_id)
                    {
                        drop(pump.tx);
                        self.finishing.push(pump.handle);
                    }
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    warn!(
                        request_id,
                        "stalled response stream staging queue saturated; closing stream"
                    );
                    if let Some(Some(pump)) = self.streams.remove(&request_id) {
                        pump.handle.abort();
                        drop(pump.tx);
                    }
                    let close_frame = ResponseStreamFrame {
                        request_id,
                        status: 500,
                        flags: FLAG_STREAMING | FLAG_ERROR,
                        headers_raw: Bytes::new(),
                        body: Bytes::new(),
                    };
                    let _ = dispatcher.deliver_stream_frame(close_frame).await;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    self.streams.remove(&request_id);
                }
            }
            return;
        }

        if frame.is_error() {
            self.streams.remove(&request_id);
            let _ = dispatcher.deliver_stream_frame(frame).await;
            return;
        }

        let (tx, mut rx) = mpsc::channel(DEFAULT_STREAM_QUEUE_DEPTH);
        let task_dispatcher = Arc::clone(dispatcher);
        let task_shutdown = shutdown.clone();
        let handle = tokio::spawn(async move {
            let mut next = Some(frame);
            loop {
                let current = match next.take() {
                    Some(frame) => frame,
                    None => tokio::select! {
                        biased;
                        _ = task_shutdown.cancelled() => break,
                        maybe_frame = rx.recv() => {
                            let Some(frame) = maybe_frame else { break };
                            frame
                        }
                    },
                };
                let closed = tokio::select! {
                    biased;
                    _ = task_shutdown.cancelled() => true,
                    delivery = task_dispatcher.deliver_stream_frame(current) => {
                        delivery == StreamDelivery::Closed
                    }
                };
                if closed {
                    break;
                }
            }
        });

        if terminal_or_error {
            self.streams.remove(&request_id);
            drop(tx);
            self.finishing.push(handle);
        } else {
            *slot = Some(StreamPump { tx, handle });
        }
    }

    async fn shutdown(&mut self) {
        for (_, entry) in self.streams.drain() {
            if let Some(pump) = entry {
                drop(pump.tx);
                let _ = pump.handle.await;
            }
        }
        for handle in self.finishing.drain(..) {
            let _ = handle.await;
        }
    }
}

/// Internal loop for a single worker's response ring.
async fn run_worker_reader(
    worker_id: usize,
    worker: Arc<RingPair>,
    dispatcher: Arc<Dispatcher>,
    shutdown: CancellationToken,
    ready: Option<Arc<Notify>>,
) {
    #[cfg(unix)]
    {
        let fd = worker.response_notifier.as_raw_fd();
        // SAFETY: `worker: Arc<RingPair>` owns `response_notifier` and outlives the `AsyncFd`
        // passed to `run_worker_reader_async` (parameters drop in reverse order, so `async_fd`
        // drops before `worker`), keeping `fd` open across reactor registration.
        match unsafe { AsyncFd::register(NotifierFd { fd }) } {
            Ok(async_fd) => {
                run_worker_reader_async(worker_id, worker, dispatcher, shutdown, async_fd, ready)
                    .await;
            }
            Err(e) => {
                error!(worker_id, error = %e, "failed to register response notifier with Tokio; falling back to polling");
                run_worker_reader_polling(worker_id, worker, dispatcher, shutdown, ready).await;
            }
        }
    }

    #[cfg(not(unix))]
    {
        run_worker_reader_polling(worker_id, worker, dispatcher, shutdown, ready).await;
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
    mut ready: Option<Arc<Notify>>,
) {
    let mut streams = StreamPumpTable::default();

    loop {
        let mut drained_any = false;
        while drain_worker(&worker, &dispatcher, &shutdown, &mut streams).await {
            drained_any = true;
        }

        if drained_any {
            tokio::task::yield_now().await;
            if drain_worker(&worker, &dispatcher, &shutdown, &mut streams).await {
                continue;
            }
        }

        if !worker.prepare_response_sleep() {
            continue;
        }

        if let Some(signal) = ready.take() {
            signal.notify_one();
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
                                // SAFETY: `inner.get_ref().as_raw_fd()` is the open notifier FD
                                // owned by `worker`, and `buf` is a stack `[u8; 8]` writable for
                                // `buf.len()` (8 bytes, matching an eventfd counter or pipe drain).
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

    if let Some(signal) = ready.take() {
        signal.notify_one();
    }

    // Final drain after shutdown
    while drain_worker(&worker, &dispatcher, &shutdown, &mut streams).await {}
    streams.shutdown().await;
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
    mut ready: Option<Arc<Notify>>,
) {
    use std::time::Duration;
    let poll_interval = Duration::from_micros(100);
    let mut streams = StreamPumpTable::default();

    if let Some(signal) = ready.take() {
        signal.notify_one();
    }

    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(poll_interval) => {
                while drain_worker(&worker, &dispatcher, &shutdown, &mut streams).await {}
            }
        }
    }

    while drain_worker(&worker, &dispatcher, &shutdown, &mut streams).await {}
    streams.shutdown().await;
    if let Some(shm) = dispatcher.shm_dispatcher() {
        shm.sweep_worker(worker_id as u32);
    }
}

/// Drain unary completions and route stream frames through per-stream bounded pumps.
async fn drain_worker(
    worker: &Arc<RingPair>,
    dispatcher: &Arc<Dispatcher>,
    shutdown: &CancellationToken,
    streams: &mut StreamPumpTable,
) -> bool {
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
    let had_completions = !completions.is_empty();
    let any_received = had_completions || !stream_frames.is_empty();
    if completions.len() == 1 {
        let (id, response) = completions.pop().unwrap();
        dispatcher.complete(id, response);
    } else if had_completions {
        dispatcher.complete_many(completions);
    }
    if shutdown.is_cancelled() {
        return had_completions && stream_frames.is_empty();
    }
    if !stream_frames.is_empty() {
        streams.prune_finished();
        for frame in stream_frames {
            streams.dispatch(frame, dispatcher, shutdown).await;
        }
    }
    any_received
}
