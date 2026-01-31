//! Worker process management.
//!
//! Provides:
//! - WorkerConfig: Configuration for worker processes
//! - WorkerConnection: Communication channels per worker
//! - WorkerManager: Spawns and manages N worker processes
//! - FD passing via Unix domain socket
//! - Health check and automatic restart
//! - Watchdog for stuck slots
//!
//! # Architecture
//!
//! ```text
//!                    ┌─────────────────────────────────────────┐
//!                    │            WorkerManager                 │
//!                    │  - Spawns N worker processes             │
//!                    │  - Passes shared memory FDs              │
//!                    │  - Monitors worker health                │
//!                    └─────────────────────────────────────────┘
//!                                      │
//!              ┌───────────────────────┼───────────────────────┐
//!              │                       │                       │
//!              ▼                       ▼                       ▼
//!     ┌────────────────┐      ┌────────────────┐      ┌────────────────┐
//!     │   Worker 0     │      │   Worker 1     │      │   Worker N-1   │
//!     │ (Python process)│     │ (Python process)│     │ (Python process)│
//!     │                │      │                │      │                │
//!     │ Response Ring ◄┘      │ Response Ring ◄┘      │ Response Ring ◄┘
//!     │   (SPSC)       │      │   (SPSC)       │      │   (SPSC)       │
//!     └───────┬────────┘      └───────┬────────┘      └───────┬────────┘
//!             │                       │                       │
//!             └───────────────────────┼───────────────────────┘
//!                                     │
//!                                     ▼
//!                    ┌─────────────────────────────────────────┐
//!                    │           Request Ring (MPMC)           │
//!                    │  All workers consume from shared ring   │
//!                    └─────────────────────────────────────────┘
//! ```

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::buffer::{ChannelMode, RingLayout};
use crate::channel::{ChannelError, MpmcRing, SlabAllocator, SpscRing};
use crate::shm::{ShmError, ShmRegion};

/// Worker configuration.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Number of worker processes to spawn.
    pub worker_count: usize,
    /// Size of each slot in the request ring (bytes).
    pub request_slot_size: usize,
    /// Number of slots in the request ring.
    pub request_slot_count: usize,
    /// Number of small request slots.
    pub small_slot_count: usize,
    /// Number of large request slots.
    pub large_slot_count: usize,
    /// Size of each slot in response rings (bytes).
    /// NOTE: Deprecated - use tiered response ring config instead.
    pub response_slot_size: usize,
    /// Number of slots per response ring.
    /// NOTE: Deprecated - use tiered response ring config instead.
    pub response_slot_count: usize,
    /// Small response ring: slot size (default: 4KB).
    pub response_small_slot_size: usize,
    /// Small response ring: slot count (default: 64).
    pub response_small_slot_count: usize,
    /// Medium response ring: slot size (default: 64KB).
    pub response_medium_slot_size: usize,
    /// Medium response ring: slot count (default: 16).
    pub response_medium_slot_count: usize,
    /// Large response ring: slot size (default: 512KB).
    pub response_large_slot_size: usize,
    /// Large response ring: slot count (default: 4).
    pub response_large_slot_count: usize,
    /// Health check interval.
    pub health_check_interval: Duration,
    /// Watchdog timeout for stuck slots.
    pub watchdog_timeout: Duration,
    /// Python app module (e.g., "myapp.main").
    pub app_module: String,
    /// Python app callable (e.g., "app").
    pub app_callable: String,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            worker_count: num_cpus(),
            request_slot_size: 65536, // 64KB
            request_slot_count: 64,
            small_slot_count: 1024,
            large_slot_count: 8,
            response_slot_size: 65536, // 64KB (legacy, kept for compatibility)
            response_slot_count: 32,   // (legacy, kept for compatibility)
            // Tiered response rings:
            // Small: 4KB slots, 64 count = 256KB total
            response_small_slot_size: 4096,
            response_small_slot_count: 64,
            // Medium: 64KB slots, 16 count = 1MB total
            response_medium_slot_size: 65536,
            response_medium_slot_count: 16,
            // Large: 512KB slots, 4 count = 2MB total
            response_large_slot_size: 524288,
            response_large_slot_count: 4,
            health_check_interval: Duration::from_secs(5),
            watchdog_timeout: Duration::from_secs(30),
            app_module: String::new(),
            app_callable: String::from("app"),
        }
    }
}

/// Get the number of available CPUs.
fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1)
}

/// Worker state.
#[repr(u32)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum WorkerState {
    /// Worker has not been started.
    Idle = 0,
    /// Worker is starting up.
    Starting = 1,
    /// Worker is running and ready.
    Running = 2,
    /// Worker is shutting down.
    ShuttingDown = 3,
    /// Worker has exited.
    Exited = 4,
    /// Worker has failed (crashed or timed out).
    Failed = 5,
}

impl From<u32> for WorkerState {
    fn from(value: u32) -> Self {
        match value {
            0 => WorkerState::Idle,
            1 => WorkerState::Starting,
            2 => WorkerState::Running,
            3 => WorkerState::ShuttingDown,
            4 => WorkerState::Exited,
            5 => WorkerState::Failed,
            _ => WorkerState::Idle,
        }
    }
}

/// Worker error types.
#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    /// Failed to create shared memory region.
    #[error("shared memory error: {0}")]
    Shm(#[from] ShmError),
    /// Channel error.
    #[error("channel error: {0}")]
    Channel(#[from] ChannelError),
    /// Worker process failed to start.
    #[error("worker failed to start: {0}")]
    StartFailed(String),
    /// Worker health check failed.
    #[error("worker health check failed")]
    HealthCheckFailed,
    /// Worker timed out.
    #[error("worker timed out")]
    Timeout,
    /// Invalid worker ID.
    #[error("invalid worker id: {0}")]
    InvalidWorkerId(u32),
    /// Worker is not running.
    #[error("worker not running")]
    NotRunning,
}

/// Per-worker connection state.
///
/// Each worker has:
/// - A shared reference to the MPMC request ring (all workers share this)
/// - Three dedicated SPSC response rings (tiered by size)
///
/// # Response Ring Tiers
///
/// - Small (4KB slots, 64 count, 256KB total): Headers-only, small JSON
/// - Medium (64KB slots, 16 count, 1MB total): Typical responses
/// - Large (512KB slots, 4 count, 2MB total): Large payloads, streaming chunks
pub struct WorkerConnection {
    /// Worker ID (0-indexed).
    pub id: u32,
    /// Worker state.
    state: AtomicU32,
    /// Last heartbeat timestamp (unix epoch nanos).
    last_heartbeat: AtomicU64,
    /// Requests processed by this worker.
    requests_processed: AtomicU64,
    /// Small response ring (4KB slots, 64 count).
    pub response_ring_small: Arc<SpscRing>,
    /// Medium response ring (64KB slots, 16 count).
    pub response_ring_medium: Arc<SpscRing>,
    /// Large response ring (512KB slots, 4 count).
    pub response_ring_large: Arc<SpscRing>,
    /// Legacy alias for response_ring_medium (for backward compatibility).
    pub response_ring: Arc<SpscRing>,
    /// Shared memory region for small response ring (needed for FD passing).
    pub response_region_small: Arc<ShmRegion>,
    /// Shared memory region for medium response ring (needed for FD passing).
    pub response_region_medium: Arc<ShmRegion>,
    /// Shared memory region for large response ring (needed for FD passing).
    pub response_region_large: Arc<ShmRegion>,
    /// Legacy alias for response_region_medium (for backward compatibility).
    pub response_region: Arc<ShmRegion>,
}

impl WorkerConnection {
    /// Create a new worker connection with tiered response rings.
    ///
    /// # Arguments
    ///
    /// * `id` - Worker ID
    /// * `small_slot_size` - Small ring slot size (default: 4KB)
    /// * `small_slot_count` - Small ring slot count (default: 64)
    /// * `medium_slot_size` - Medium ring slot size (default: 64KB)
    /// * `medium_slot_count` - Medium ring slot count (default: 16)
    /// * `large_slot_size` - Large ring slot size (default: 512KB)
    /// * `large_slot_count` - Large ring slot count (default: 4)
    pub fn new(
        id: u32,
        small_slot_size: usize,
        small_slot_count: usize,
        medium_slot_size: usize,
        medium_slot_count: usize,
        large_slot_size: usize,
        large_slot_count: usize,
    ) -> Result<Self, WorkerError> {
        // Create small response ring (4KB slots, 64 count = 256KB)
        let small_layout = RingLayout::new(small_slot_size, small_slot_count, ChannelMode::Spsc);
        let small_region = Arc::new(ShmRegion::create(small_layout.total_size)?);
        let small_ring = unsafe {
            SpscRing::initialize(Arc::clone(&small_region), small_layout, 0, 0, 0)?
        };

        // Create medium response ring (64KB slots, 16 count = 1MB)
        let medium_layout = RingLayout::new(medium_slot_size, medium_slot_count, ChannelMode::Spsc);
        let medium_region = Arc::new(ShmRegion::create(medium_layout.total_size)?);
        let medium_ring = unsafe {
            SpscRing::initialize(Arc::clone(&medium_region), medium_layout, 0, 0, 0)?
        };

        // Create large response ring (512KB slots, 4 count = 2MB)
        let large_layout = RingLayout::new(large_slot_size, large_slot_count, ChannelMode::Spsc);
        let large_region = Arc::new(ShmRegion::create(large_layout.total_size)?);
        let large_ring = unsafe {
            SpscRing::initialize(Arc::clone(&large_region), large_layout, 0, 0, 0)?
        };

        let response_ring_small = Arc::new(small_ring);
        let response_ring_medium = Arc::new(medium_ring);
        let response_ring_large = Arc::new(large_ring);

        Ok(Self {
            id,
            state: AtomicU32::new(WorkerState::Idle as u32),
            last_heartbeat: AtomicU64::new(0),
            requests_processed: AtomicU64::new(0),
            response_ring_small,
            response_ring_medium: Arc::clone(&response_ring_medium),
            response_ring_large,
            // Legacy alias points to medium ring for backward compatibility
            response_ring: response_ring_medium,
            response_region_small: small_region,
            response_region_medium: Arc::clone(&medium_region),
            response_region_large: large_region,
            // Legacy alias points to medium region for backward compatibility
            response_region: medium_region,
        })
    }

    /// Get the current worker state.
    pub fn state(&self) -> WorkerState {
        WorkerState::from(self.state.load(Ordering::Acquire))
    }

    /// Set the worker state.
    pub fn set_state(&self, state: WorkerState) {
        self.state.store(state as u32, Ordering::Release);
    }

    /// Update the heartbeat timestamp.
    pub fn heartbeat(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        self.last_heartbeat.store(now, Ordering::Release);
    }

    /// Get the last heartbeat timestamp.
    pub fn last_heartbeat(&self) -> u64 {
        self.last_heartbeat.load(Ordering::Acquire)
    }

    /// Check if the worker is alive (heartbeat within timeout).
    pub fn is_alive(&self, timeout: Duration) -> bool {
        if self.state() != WorkerState::Running {
            return false;
        }
        let last = self.last_heartbeat();
        if last == 0 {
            return false;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let elapsed = Duration::from_nanos(now.saturating_sub(last));
        elapsed < timeout
    }

    /// Increment the requests processed counter.
    pub fn increment_requests(&self) {
        self.requests_processed.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the total requests processed by this worker.
    pub fn requests_processed(&self) -> u64 {
        self.requests_processed.load(Ordering::Relaxed)
    }

    /// Get the shared memory file descriptor for the legacy response ring.
    /// NOTE: Deprecated - use `response_fd_small`, `response_fd_medium`, `response_fd_large`.
    #[cfg(unix)]
    pub fn response_fd(&self) -> i32 {
        self.response_region.handle().unwrap_or(-1)
    }

    /// Get the shared memory file descriptor for the small response ring.
    #[cfg(unix)]
    pub fn response_fd_small(&self) -> i32 {
        self.response_region_small.handle().unwrap_or(-1)
    }

    /// Get the shared memory file descriptor for the medium response ring.
    #[cfg(unix)]
    pub fn response_fd_medium(&self) -> i32 {
        self.response_region_medium.handle().unwrap_or(-1)
    }

    /// Get the shared memory file descriptor for the large response ring.
    #[cfg(unix)]
    pub fn response_fd_large(&self) -> i32 {
        self.response_region_large.handle().unwrap_or(-1)
    }
}

impl std::fmt::Debug for WorkerConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerConnection")
            .field("id", &self.id)
            .field("state", &self.state())
            .field("last_heartbeat", &self.last_heartbeat())
            .field("requests_processed", &self.requests_processed())
            .finish()
    }
}

/// Worker manager statistics.
#[derive(Debug, Clone)]
pub struct WorkerStats {
    /// Number of workers configured.
    pub worker_count: usize,
    /// Number of workers currently running.
    pub running_count: usize,
    /// Total requests processed across all workers.
    pub total_requests: u64,
    /// Requests processed per worker.
    pub per_worker_requests: Vec<u64>,
}

/// Manages a pool of worker processes.
pub struct WorkerManager {
    /// Configuration.
    config: WorkerConfig,
    /// Shared slab allocator.
    pub allocator: Arc<SlabAllocator>,
    /// Shared request ring (MPMC) - pointing to Medium slab.
    pub request_ring: Arc<MpmcRing>,
    /// Shared memory region for request ring (needed for FD passing).
    pub request_region: Arc<ShmRegion>,
    /// Per-worker connections.
    workers: Vec<WorkerConnection>,
    /// Shutdown flag.
    shutdown: AtomicBool,
}

impl WorkerManager {
    /// Create a new worker manager with the given configuration.
    pub fn new(config: WorkerConfig) -> Result<Self, WorkerError> {
        // Create shared slab allocator
        let allocator = Arc::new(SlabAllocator::new(
            config.small_slot_count,
            config.request_slot_count,
            config.large_slot_count,
            4096, // Small
            config.request_slot_size, // Medium
            524288, // Large
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        )?);

        // Alias Medium ring as request_ring for legacy/default usage
        let request_ring = Arc::clone(&allocator.medium);
        let request_region = Arc::clone(&allocator.region);

        // Create per-worker connections with tiered response rings
        let mut workers = Vec::with_capacity(config.worker_count);
        for id in 0..config.worker_count {
            workers.push(WorkerConnection::new(
                id as u32,
                config.response_small_slot_size,
                config.response_small_slot_count,
                config.response_medium_slot_size,
                config.response_medium_slot_count,
                config.response_large_slot_size,
                config.response_large_slot_count,
            )?);
        }

        Ok(Self {
            config,
            allocator,
            request_ring,
            request_region,
            workers,
            shutdown: AtomicBool::new(false),
        })
    }

    /// Get the worker count.
    pub fn worker_count(&self) -> usize {
        self.config.worker_count
    }

    /// Get a worker connection by ID.
    pub fn worker(&self, id: u32) -> Result<&WorkerConnection, WorkerError> {
        self.workers
            .get(id as usize)
            .ok_or(WorkerError::InvalidWorkerId(id))
    }

    /// Get all worker connections.
    pub fn workers(&self) -> &[WorkerConnection] {
        &self.workers
    }

    /// Mark a worker as starting.
    pub fn mark_starting(&self, worker_id: u32) -> Result<(), WorkerError> {
        let worker = self.worker(worker_id)?;
        worker.set_state(WorkerState::Starting);
        Ok(())
    }

    /// Mark a worker as running and record heartbeat.
    pub fn mark_running(&self, worker_id: u32) -> Result<(), WorkerError> {
        let worker = self.worker(worker_id)?;
        worker.set_state(WorkerState::Running);
        worker.heartbeat();
        Ok(())
    }

    /// Mark a worker as failed.
    pub fn mark_failed(&self, worker_id: u32) -> Result<(), WorkerError> {
        let worker = self.worker(worker_id)?;
        worker.set_state(WorkerState::Failed);
        Ok(())
    }

    /// Record a heartbeat from a worker.
    pub fn record_heartbeat(&self, worker_id: u32) -> Result<(), WorkerError> {
        let worker = self.worker(worker_id)?;
        worker.heartbeat();
        Ok(())
    }

    /// Check health of all workers.
    pub fn check_health(&self) -> Vec<u32> {
        let timeout = self.config.watchdog_timeout;
        let mut unhealthy = Vec::new();
        for worker in &self.workers {
            if worker.state() == WorkerState::Running && !worker.is_alive(timeout) {
                unhealthy.push(worker.id);
            }
        }
        unhealthy
    }

    /// Get statistics for all workers.
    pub fn stats(&self) -> WorkerStats {
        let mut running_count = 0;
        let mut total_requests = 0;
        let mut per_worker_requests = Vec::with_capacity(self.workers.len());

        for worker in &self.workers {
            if worker.state() == WorkerState::Running {
                running_count += 1;
            }
            let requests = worker.requests_processed();
            total_requests += requests;
            per_worker_requests.push(requests);
        }

        WorkerStats {
            worker_count: self.config.worker_count,
            running_count,
            total_requests,
            per_worker_requests,
        }
    }

    /// Get the shared memory file descriptor for the request ring.
    #[cfg(unix)]
    pub fn request_fd(&self) -> i32 {
        self.request_region.handle().unwrap_or(-1)
    }

    /// Get worker info for spawning (FDs and configuration).
    #[cfg(unix)]
    pub fn worker_spawn_info(&self, worker_id: u32) -> Result<WorkerSpawnInfo, WorkerError> {
        let worker = self.worker(worker_id)?;
        Ok(WorkerSpawnInfo {
            worker_id,
            request_fd: self.request_fd(),
            response_fd: worker.response_fd(),
            response_fd_small: worker.response_fd_small(),
            response_fd_medium: worker.response_fd_medium(),
            response_fd_large: worker.response_fd_large(),
            request_slot_size: self.config.request_slot_size,
            request_slot_count: self.config.request_slot_count,
            small_slot_count: self.config.small_slot_count,
            large_slot_count: self.config.large_slot_count,
            response_slot_size: self.config.response_slot_size,
            response_slot_count: self.config.response_slot_count,
            response_small_slot_size: self.config.response_small_slot_size,
            response_small_slot_count: self.config.response_small_slot_count,
            response_medium_slot_size: self.config.response_medium_slot_size,
            response_medium_slot_count: self.config.response_medium_slot_count,
            response_large_slot_size: self.config.response_large_slot_size,
            response_large_slot_count: self.config.response_large_slot_count,
            app_module: self.config.app_module.clone(),
            app_callable: self.config.app_callable.clone(),
        })
    }

    /// Signal shutdown to all workers.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        for worker in &self.workers {
            worker.set_state(WorkerState::ShuttingDown);
        }
    }

    /// Check if shutdown has been signaled.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }
}

impl std::fmt::Debug for WorkerManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerManager")
            .field("worker_count", &self.config.worker_count)
            .field("request_slot_size", &self.config.request_slot_size)
            .field("request_slot_count", &self.config.request_slot_count)
            .field("shutdown", &self.is_shutdown())
            .finish()
    }
}

/// Information needed to spawn a worker process.
#[cfg(unix)]
#[derive(Debug, Clone)]
pub struct WorkerSpawnInfo {
    /// Worker ID.
    pub worker_id: u32,
    /// File descriptor for the shared request ring.
    pub request_fd: i32,
    /// File descriptor for this worker's response ring (legacy, points to medium).
    pub response_fd: i32,
    /// File descriptor for the small response ring.
    pub response_fd_small: i32,
    /// File descriptor for the medium response ring.
    pub response_fd_medium: i32,
    /// File descriptor for the large response ring.
    pub response_fd_large: i32,
    /// Request slot size.
    pub request_slot_size: usize,
    /// Request slot count.
    pub request_slot_count: usize,
    /// Small slot count.
    pub small_slot_count: usize,
    /// Large slot count.
    pub large_slot_count: usize,
    /// Response slot size (legacy, kept for compatibility).
    pub response_slot_size: usize,
    /// Response slot count (legacy, kept for compatibility).
    pub response_slot_count: usize,
    /// Small response ring: slot size.
    pub response_small_slot_size: usize,
    /// Small response ring: slot count.
    pub response_small_slot_count: usize,
    /// Medium response ring: slot size.
    pub response_medium_slot_size: usize,
    /// Medium response ring: slot count.
    pub response_medium_slot_count: usize,
    /// Large response ring: slot size.
    pub response_large_slot_size: usize,
    /// Large response ring: slot count.
    pub response_large_slot_count: usize,
    /// Python app module.
    pub app_module: String,
    /// Python app callable.
    pub app_callable: String,
}

#[cfg(unix)]
impl WorkerSpawnInfo {
    /// Spawn a Python worker process with FD passing.
    ///
    /// This spawns `python -m litecore._worker` with the appropriate arguments
    /// and keeps the request/response FDs open across fork.
    ///
    /// Returns the child process handle.
    pub fn spawn(&self) -> Result<std::process::Child, WorkerError> {
        use std::os::unix::process::CommandExt;
        use std::process::Command;

        let request_fd = self.request_fd;
        let response_fd = self.response_fd;
        let response_fd_small = self.response_fd_small;
        let response_fd_medium = self.response_fd_medium;
        let response_fd_large = self.response_fd_large;

        // Build the command
        let mut cmd = Command::new("python");
        cmd.args([
            "-m",
            "litecore._worker",
            &format!("--request-fd={}", request_fd),
            &format!("--response-fd={}", response_fd),
            &format!("--response-fd-small={}", response_fd_small),
            &format!("--response-fd-medium={}", response_fd_medium),
            &format!("--response-fd-large={}", response_fd_large),
            &format!("--worker-id={}", self.worker_id),
            &format!("--slot-size={}", self.request_slot_size),
            &format!("--request-slots={}", self.request_slot_count),
            &format!("--small-slots={}", self.small_slot_count),
            &format!("--large-slots={}", self.large_slot_count),
            &format!("--response-slots={}", self.response_slot_count),
            &format!("--response-small-slot-size={}", self.response_small_slot_size),
            &format!("--response-small-slot-count={}", self.response_small_slot_count),
            &format!("--response-medium-slot-size={}", self.response_medium_slot_size),
            &format!("--response-medium-slot-count={}", self.response_medium_slot_count),
            &format!("--response-large-slot-size={}", self.response_large_slot_size),
            &format!("--response-large-slot-count={}", self.response_large_slot_count),
        ]);

        // Add app path if configured
        if !self.app_module.is_empty() {
            let app_path = if self.app_callable.is_empty() {
                self.app_module.clone()
            } else {
                format!("{}:{}", self.app_module, self.app_callable)
            };
            cmd.arg(format!("--app={}", app_path));
        }

        // Use pre_spawn hook to keep FDs open across fork
        // By default, FDs > 2 have CLOEXEC set and are closed on fork
        // SAFETY: We only call fcntl which is async-signal-safe
        unsafe {
            cmd.pre_exec(move || {
                // Clear CLOEXEC flag on all FDs so they're inherited
                for fd in [request_fd, response_fd, response_fd_small, response_fd_medium, response_fd_large] {
                    let flags = libc::fcntl(fd, libc::F_GETFD);
                    if flags != -1 {
                        libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC);
                    }
                }
                Ok(())
            });
        }

        cmd.spawn()
            .map_err(|e| WorkerError::StartFailed(e.to_string()))
    }
}

/// Spawn all workers and return their process handles.
#[cfg(unix)]
pub fn spawn_all_workers(manager: &WorkerManager) -> Result<Vec<std::process::Child>, WorkerError> {
    let mut children = Vec::with_capacity(manager.worker_count());

    for worker_id in 0..manager.worker_count() as u32 {
        manager.mark_starting(worker_id)?;
        let info = manager.worker_spawn_info(worker_id)?;
        let child = info.spawn()?;
        children.push(child);
    }

    Ok(children)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_manager_creation() {
        let config = WorkerConfig {
            worker_count: 2,
            request_slot_size: 1024,
            request_slot_count: 8,
            response_slot_size: 1024,
            response_slot_count: 4,
            ..Default::default()
        };
        let manager = WorkerManager::new(config).unwrap();
        assert_eq!(manager.worker_count(), 2);
        assert!(!manager.is_shutdown());
    }

    #[test]
    fn worker_state_transitions() {
        let config = WorkerConfig {
            worker_count: 1,
            request_slot_size: 1024,
            request_slot_count: 8,
            response_slot_size: 1024,
            response_slot_count: 4,
            ..Default::default()
        };
        let manager = WorkerManager::new(config).unwrap();

        let worker = manager.worker(0).unwrap();
        assert_eq!(worker.state(), WorkerState::Idle);

        manager.mark_starting(0).unwrap();
        assert_eq!(worker.state(), WorkerState::Starting);

        manager.mark_running(0).unwrap();
        assert_eq!(worker.state(), WorkerState::Running);
    }

    #[test]
    fn worker_heartbeat() {
        let config = WorkerConfig {
            worker_count: 1,
            request_slot_size: 1024,
            request_slot_count: 8,
            response_slot_size: 1024,
            response_slot_count: 4,
            watchdog_timeout: Duration::from_secs(1),
            ..Default::default()
        };
        let manager = WorkerManager::new(config).unwrap();

        let worker = manager.worker(0).unwrap();
        worker.set_state(WorkerState::Running);
        assert!(!worker.is_alive(Duration::from_secs(1))); // No heartbeat yet

        worker.heartbeat();
        assert!(worker.is_alive(Duration::from_secs(1)));
    }

    #[test]
    fn worker_stats() {
        let config = WorkerConfig {
            worker_count: 2,
            request_slot_size: 1024,
            request_slot_count: 8,
            response_slot_size: 1024,
            response_slot_count: 4,
            ..Default::default()
        };
        let manager = WorkerManager::new(config).unwrap();

        manager.mark_running(0).unwrap();
        manager.workers[0].increment_requests();
        manager.workers[0].increment_requests();
        manager.workers[1].increment_requests();

        let stats = manager.stats();
        assert_eq!(stats.worker_count, 2);
        assert_eq!(stats.running_count, 1);
        assert_eq!(stats.total_requests, 3);
        assert_eq!(stats.per_worker_requests, vec![2, 1]);
    }

    #[test]
    fn worker_shutdown() {
        let config = WorkerConfig {
            worker_count: 2,
            request_slot_size: 1024,
            request_slot_count: 8,
            response_slot_size: 1024,
            response_slot_count: 4,
            ..Default::default()
        };
        let manager = WorkerManager::new(config).unwrap();

        manager.mark_running(0).unwrap();
        manager.mark_running(1).unwrap();
        assert!(!manager.is_shutdown());

        manager.shutdown();
        assert!(manager.is_shutdown());
        assert_eq!(
            manager.worker(0).unwrap().state(),
            WorkerState::ShuttingDown
        );
        assert_eq!(
            manager.worker(1).unwrap().state(),
            WorkerState::ShuttingDown
        );
    }

    #[test]
    fn worker_health_check() {
        let config = WorkerConfig {
            worker_count: 2,
            request_slot_size: 1024,
            request_slot_count: 8,
            response_slot_size: 1024,
            response_slot_count: 4,
            watchdog_timeout: Duration::from_millis(1),
            ..Default::default()
        };
        let manager = WorkerManager::new(config).unwrap();

        // Start workers
        manager.mark_running(0).unwrap();
        manager.worker(0).unwrap().heartbeat();
        manager.mark_running(1).unwrap();
        manager.worker(1).unwrap().heartbeat();

        // Both healthy initially
        std::thread::sleep(Duration::from_millis(2));

        // After timeout, both are unhealthy
        let unhealthy = manager.check_health();
        assert_eq!(unhealthy.len(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn worker_spawn_info() {
        let config = WorkerConfig {
            worker_count: 1,
            request_slot_size: 1024,
            request_slot_count: 8,
            response_slot_size: 1024,
            response_slot_count: 4,
            app_module: "myapp".to_string(),
            app_callable: "create_app".to_string(),
            ..Default::default()
        };
        let manager = WorkerManager::new(config).unwrap();

        let info = manager.worker_spawn_info(0).unwrap();
        assert_eq!(info.worker_id, 0);
        assert!(info.request_fd > 0);
        assert!(info.response_fd > 0);
        assert_eq!(info.app_module, "myapp");
        assert_eq!(info.app_callable, "create_app");
    }

    #[test]
    fn invalid_worker_id() {
        let config = WorkerConfig {
            worker_count: 1,
            request_slot_size: 1024,
            request_slot_count: 8,
            response_slot_size: 1024,
            response_slot_count: 4,
            ..Default::default()
        };
        let manager = WorkerManager::new(config).unwrap();

        assert!(matches!(
            manager.worker(99),
            Err(WorkerError::InvalidWorkerId(99))
        ));
    }
}
