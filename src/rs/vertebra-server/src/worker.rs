#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;
use std::process::Command;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{oneshot, Mutex};
use vertebra_core::buffer::{ChannelMode, RingLayout};
use vertebra_core::channel::{ChannelError, SpscReceiver, SpscRing, SpscSender};
use vertebra_core::shm::ShmRegion;
use vertebra_core::AsyncNotifier;

/// Sender half of a response channel for request correlation.
pub type ResponseSender = oneshot::Sender<Vec<u8>>;

/// Worker connection with support for concurrent request correlation.
///
/// Unlike the old `Worker` struct, `WorkerConnection` supports multiple
/// in-flight requests via the `pending` map. Each request gets a unique
/// sequence ID, and responses are routed back to the correct waiter.
///
/// Thread-safety:
/// - `req_tx` is wrapped in a Mutex to serialize sends (SPSC requirement)
/// - `pending_count` uses AtomicU32 for fast load balancing queries
/// - `pending` DashMap handles concurrent insert/remove
pub struct WorkerConnection {
    /// Request sender - wrapped in Mutex to enforce SPSC single-producer invariant.
    /// The critical section is small (just memcpy into ring buffer).
    req_tx: Mutex<SpscSender>,
    pub resp_rx: SpscReceiver,
    /// Notifier for Rust→Python direction (request ready)
    pub req_notifier: AsyncNotifier,
    /// Notifier for Python→Rust direction (response ready)
    pub resp_notifier: AsyncNotifier,
    /// Map of pending requests awaiting responses.
    /// Key: request sequence ID, Value: oneshot sender for response.
    pub pending: DashMap<u64, ResponseSender>,
    /// Fast atomic counter for load balancing (faster than DashMap.len()).
    pending_count: AtomicU32,
    /// Monotonic counter for generating unique request IDs.
    next_seq: AtomicU64,
    pub pid: u32,
    pub process: std::process::Child,
}

impl WorkerConnection {
    /// Generate the next unique request ID for this worker.
    pub fn next_request_id(&self) -> u64 {
        self.next_seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Get the count of currently pending requests (fast atomic read).
    pub fn pending_count(&self) -> u32 {
        self.pending_count.load(Ordering::Relaxed)
    }

    /// Increment pending count. Call BEFORE sending request.
    pub fn increment_pending(&self) -> u32 {
        self.pending_count.fetch_add(1, Ordering::Relaxed)
    }

    /// Decrement pending count. Call AFTER response received or on error.
    pub fn decrement_pending(&self) -> u32 {
        self.pending_count.fetch_sub(1, Ordering::Relaxed)
    }

    /// Send a request to the Python worker (mutex-protected for SPSC safety).
    /// The critical section is minimal - just the memcpy into the ring buffer.
    pub async fn send_request(&self, data: &[u8], flags: u32) -> Result<(), ChannelError> {
        let tx = self.req_tx.lock().await;
        tx.send(data, flags)
    }
}

pub struct WorkerManager {
    workers: usize,
    app_path: String,
    python_path: String,
    worker_handles: Vec<WorkerConnection>,
}

impl WorkerManager {
    pub fn new(workers: usize, app_path: &str, python_path: &str) -> Self {
        Self {
            workers,
            app_path: app_path.to_string(),
            python_path: python_path.to_string(),
            worker_handles: Vec::new(),
        }
    }

    pub fn worker_count(&self) -> usize {
        self.workers
    }

    pub fn into_workers(self) -> Vec<WorkerConnection> {
        self.worker_handles
    }

    pub fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!(
            "Starting {} workers for app: {}",
            self.workers, self.app_path
        );

        let layout = RingLayout::new(1024 * 64, 128, ChannelMode::Spsc); // 64KB slots, 128 slots

        for i in 0..self.workers {
            let req_region = Arc::new(ShmRegion::create(layout.total_size)?);
            let resp_region = Arc::new(ShmRegion::create(layout.total_size)?);

            // Server side: Req Sender, Resp Receiver
            // Worker side: Req Receiver, Resp Sender
            let req_ring = unsafe { SpscRing::initialize(req_region.clone(), layout, 0, 0) }?;
            let resp_ring = unsafe { SpscRing::initialize(resp_region.clone(), layout, 0, 0) }?;

            let (req_tx, _req_rx) = req_ring.split();
            let (_resp_tx, resp_rx) = resp_ring.split();

            // Create separate notifiers for each direction
            // req_notifier: Rust→Python (signals request available)
            // resp_notifier: Python→Rust (signals response ready)
            let req_notifier = AsyncNotifier::new()?;
            let resp_notifier = AsyncNotifier::new()?;
            let req_notify_fd = req_notifier.as_raw_fd();
            let resp_notify_fd = resp_notifier.as_raw_fd();

            // Get Shm FDs
            // Note: On Linux/Unix, handle() returns Some(RawFd)
            let req_fd = req_region.handle().ok_or("No handle for request region")?;
            let resp_fd = resp_region
                .handle()
                .ok_or("No handle for response region")?;

            // Clear FD_CLOEXEC so child inherits them
            #[cfg(target_os = "linux")]
            unsafe {
                let flags = libc::fcntl(req_notify_fd, libc::F_GETFD);
                libc::fcntl(req_notify_fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC);

                let flags = libc::fcntl(resp_notify_fd, libc::F_GETFD);
                libc::fcntl(resp_notify_fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC);

                let flags = libc::fcntl(req_fd, libc::F_GETFD);
                libc::fcntl(req_fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC);

                let flags = libc::fcntl(resp_fd, libc::F_GETFD);
                libc::fcntl(resp_fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC);
            }

            let mut cmd = Command::new(&self.python_path);
            cmd.arg("-m")
                .arg("vertebra.server.worker")
                .arg("--app")
                .arg(&self.app_path)
                .arg("--worker-id")
                .arg(i.to_string())
                .arg("--slots")
                .arg(layout.slots.to_string())
                .arg("--slot-size")
                .arg(layout.slot_size.to_string())
                // Dual notification FDs - separate for each direction
                // VERTEBRA_REQ_NOTIFY_FD: Rust→Python (Python reads from this when requests arrive)
                // VERTEBRA_RESP_NOTIFY_FD: Python→Rust (Python writes to this after sending response)
                .env("VERTEBRA_REQ_NOTIFY_FD", req_notify_fd.to_string())
                .env("VERTEBRA_RESP_NOTIFY_FD", resp_notify_fd.to_string())
                .env("VERTEBRA_REQ_SHM_FD", req_fd.to_string())
                .env("VERTEBRA_RESP_SHM_FD", resp_fd.to_string())
                .env("VERTEBRA_SHM_SIZE", layout.total_size.to_string())
                .env("PYTHONUNBUFFERED", "1");

            #[cfg(target_os = "linux")]
            unsafe {
                cmd.pre_exec(|| {
                    // Send SIGTERM to child if parent dies
                    libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
                    Ok(())
                });
            }

            let child = cmd.spawn()?;
            println!(
                "Spawned worker {} (PID: {}) req_notify_fd: {} resp_notify_fd: {}",
                i,
                child.id(),
                req_notify_fd,
                resp_notify_fd
            );

            self.worker_handles.push(WorkerConnection {
                req_tx: Mutex::new(req_tx),
                resp_rx,
                req_notifier,
                resp_notifier,
                pending: DashMap::new(),
                pending_count: AtomicU32::new(0),
                next_seq: AtomicU64::new(1), // Start at 1, 0 reserved for "no correlation"
                pid: child.id(),
                process: child,
            });
        }

        Ok(())
    }
}
