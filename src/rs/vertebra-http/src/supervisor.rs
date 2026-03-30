//! Supervisor for multi-process workers with shared-memory ring pairs.
//!
//! `SupervisorRingFactory` creates SPSC ring pairs before spawning Python
//! worker processes. Each worker inherits the ring FDs and reconstructs
//! its transport from environment variables.

use std::process::{Child, Command};
use std::sync::Arc;

use tracing::{error, info};

use vertebra_core::process;
use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig, RingPairError};

/// Creates and manages SPSC ring pairs for multi-process workers.
pub struct SupervisorRingFactory {
    pairs: Vec<Arc<RingPair>>,
}

impl SupervisorRingFactory {
    /// Create ring pairs for `worker_count` workers.
    ///
    /// All shared memory regions and notifier FDs are created before any
    /// workers are spawned. On Unix, FDs are made inheritable so child
    /// processes can access them after spawn.
    pub fn new(worker_count: usize, config: RingPairConfig) -> Result<Self, RingPairError> {
        let mut pairs = Vec::with_capacity(worker_count);
        for i in 0..worker_count {
            let pair = RingPair::create(config.clone())?;

            // Make all FDs inheritable (clear CLOEXEC)
            #[cfg(unix)]
            {
                let fds = pair.all_fds();
                process::make_inheritable(&fds);
            }

            info!(worker_id = i, "created ring pair for worker");
            pairs.push(Arc::new(pair));
        }

        Ok(Self { pairs })
    }

    /// Get the ring pair for a specific worker.
    pub fn ring_pair(&self, worker_id: usize) -> Arc<RingPair> {
        Arc::clone(&self.pairs[worker_id])
    }

    /// Number of workers this factory was created for.
    pub fn worker_count(&self) -> usize {
        self.pairs.len()
    }

    /// All ring pairs (for dispatcher registration).
    pub fn all_ring_pairs(&self) -> &[Arc<RingPair>] {
        &self.pairs
    }

    /// Spawn a single Python worker process.
    ///
    /// The worker inherits the ring pair FDs and receives configuration
    /// via the `VERTEBRA_*` environment variable protocol.
    #[cfg(unix)]
    pub fn spawn_worker(
        &self,
        worker_id: usize,
        app_path: &str,
        python_bin: &str,
    ) -> Result<Child, SupervisorError> {
        let pair = &self.pairs[worker_id];
        let fds = pair.fd_set();
        let env_vars = fds.to_env_vars(worker_id);

        let mut cmd = Command::new(python_bin);
        cmd.args(["-m", "vertebra.supervisor._worker"]);

        // Set FD protocol env vars
        for (key, value) in &env_vars {
            cmd.env(key, value);
        }

        // Set app import path
        cmd.env("VERTEBRA_APP", app_path);

        // Propagate thread count
        if let Ok(threads) = std::env::var("VERTEBRA_THREADS") {
            cmd.env("VERTEBRA_THREADS", threads);
        }

        // Set parent death signal (Linux only)
        #[cfg(target_os = "linux")]
        process::set_parent_death_signal(&mut cmd);

        let child = cmd.spawn().map_err(SupervisorError::Spawn)?;
        info!(worker_id, pid = child.id(), "spawned worker process");

        Ok(child)
    }

    /// Spawn all worker processes.
    #[cfg(unix)]
    pub fn spawn_all(
        &self,
        app_path: &str,
        python_bin: &str,
    ) -> Result<Vec<Child>, SupervisorError> {
        let mut children = Vec::with_capacity(self.pairs.len());
        for i in 0..self.pairs.len() {
            match self.spawn_worker(i, app_path, python_bin) {
                Ok(child) => children.push(child),
                Err(e) => {
                    // Kill already-spawned workers before returning error
                    error!(worker_id = i, error = %e, "failed to spawn worker");
                    for mut c in children {
                        let _ = c.kill();
                    }
                    return Err(e);
                }
            }
        }
        info!(count = self.pairs.len(), "all workers spawned");
        Ok(children)
    }

    /// Gracefully shut down all workers.
    ///
    /// Sends SIGTERM, waits up to `timeout`, then SIGKILL any remaining.
    #[cfg(unix)]
    pub fn shutdown_workers(children: &mut [Child], timeout: std::time::Duration) {
        use std::time::Instant;

        // Send SIGTERM to all
        for child in children.iter_mut() {
            let pid = child.id();
            unsafe {
                libc::kill(pid as i32, libc::SIGTERM);
            }
        }

        let deadline = Instant::now() + timeout;

        // Wait for each, with timeout
        for child in children.iter_mut() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match child.try_wait() {
                Ok(Some(_)) => continue, // Already exited
                Ok(None) => {
                    // Still running, wait briefly
                    std::thread::sleep(remaining.min(std::time::Duration::from_millis(100)));
                }
                Err(_) => continue,
            }
        }

        // SIGKILL any remaining
        for child in children.iter_mut() {
            match child.try_wait() {
                Ok(Some(_)) => {} // Already exited
                _ => {
                    let _ = child.kill();
                    let _ = child.wait(); // Reap zombie
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Worker Lifecycle Management
// ---------------------------------------------------------------------------

/// State of a managed worker process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerState {
    Running,
    Dead,
    Respawning,
}

/// Tracks a single worker's lifecycle for crash detection and respawn.
pub struct WorkerLifecycle {
    pub child: Option<std::process::Child>,
    #[cfg(target_os = "linux")]
    pub pidfd: Option<std::os::fd::OwnedFd>,
    pub ring_pair: Arc<RingPair>,
    pub state: WorkerState,
    pub crash_count: u32,
    pub last_crash: Option<std::time::Instant>,
    pub last_spawn: Option<std::time::Instant>,
    pub worker_id: usize,
}

impl WorkerLifecycle {
    /// Create a new lifecycle tracker for a just-spawned worker.
    pub fn new(child: std::process::Child, ring_pair: Arc<RingPair>, worker_id: usize) -> Self {
        #[cfg(target_os = "linux")]
        let pidfd = {
            use vertebra_core::platform::linux::pidfd_open;
            pidfd_open(child.id() as i32).ok()
        };

        Self {
            child: Some(child),
            #[cfg(target_os = "linux")]
            pidfd,
            ring_pair,
            state: WorkerState::Running,
            crash_count: 0,
            last_crash: None,
            last_spawn: Some(std::time::Instant::now()),
            worker_id,
        }
    }

    /// Check if the worker process has exited (non-blocking).
    pub fn check_alive(&mut self) -> bool {
        if let Some(ref mut child) = self.child {
            match child.try_wait() {
                Ok(Some(_status)) => {
                    self.state = WorkerState::Dead;
                    false
                }
                Ok(None) => true, // still running
                Err(_) => {
                    self.state = WorkerState::Dead;
                    false
                }
            }
        } else {
            false
        }
    }

    /// Record a crash and update backoff state.
    pub fn record_crash(&mut self) {
        let now = std::time::Instant::now();

        // If worker ran for > 60s before crashing, reset crash count (not a crash loop)
        if let Some(spawn_time) = self.last_spawn {
            if now.duration_since(spawn_time) > std::time::Duration::from_secs(60) {
                self.crash_count = 0;
            }
        }

        self.crash_count += 1;
        self.last_crash = Some(now);
        self.state = WorkerState::Dead;
    }

    /// Compute the respawn delay based on crash count (exponential backoff).
    ///
    /// 1s, 2s, 4s, 8s, 16s, 32s max. Resets if worker ran > 60s.
    pub fn respawn_delay(&self) -> std::time::Duration {
        let exponent = self.crash_count.saturating_sub(1).min(5);
        std::time::Duration::from_secs(1u64 << exponent)
    }

    /// Check if the circuit breaker has tripped (> 10 crashes in 5 minutes).
    pub fn circuit_breaker_tripped(&self) -> bool {
        if self.crash_count <= 10 {
            return false;
        }
        // Check if crashes are within 5 minutes
        if let Some(first_crash) = self.last_crash {
            let window = std::time::Duration::from_secs(300);
            if first_crash.elapsed() < window {
                return true;
            }
        }
        false
    }
}

// ---------------------------------------------------------------------------
// Stale SHM Cleanup
// ---------------------------------------------------------------------------

/// Clean up stale shared memory regions from previous crashed supervisors.
///
/// Scans `/dev/shm/` for files matching `vertebra-*` and unlinks them.
/// Only affects named regions — memfd regions don't appear in `/dev/shm/`.
#[cfg(unix)]
pub fn cleanup_stale_shm() {
    use std::fs;

    let shm_dir = std::path::Path::new("/dev/shm");
    if !shm_dir.exists() {
        return;
    }

    match fs::read_dir(shm_dir) {
        Ok(entries) => {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with("vertebra-") || name.starts_with("vertebra_") {
                        tracing::warn!(name, "cleaning up stale SHM region");
                        let _ = fs::remove_file(entry.path());
                    }
                }
            }
        }
        Err(e) => {
            tracing::debug!(error = %e, "could not scan /dev/shm for stale regions");
        }
    }
}

/// Install panic hook that calls `cleanup_all()` for SHM cleanup.
pub fn install_panic_cleanup_hook() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        vertebra_core::cleanup::cleanup_all();
        default_hook(info);
    }));
}

/// Errors from supervisor operations.
#[derive(Debug, thiserror::Error)]
pub enum SupervisorError {
    #[error("failed to create ring pair: {0}")]
    RingPair(#[from] RingPairError),
    #[error("failed to spawn worker: {0}")]
    Spawn(std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factory_creates_ring_pairs() {
        let factory =
            SupervisorRingFactory::new(4, RingPairConfig::default()).expect("create factory");
        assert_eq!(factory.worker_count(), 4);

        // Each ring pair should be independently usable
        for i in 0..4 {
            let pair = factory.ring_pair(i);
            let data = format!("hello worker {i}");
            pair.request_ring
                .try_send(data.as_bytes(), 0)
                .expect("send");
            let mut buf = vec![0u8; 1024];
            let len = pair.request_ring.try_recv(&mut buf).expect("recv");
            assert_eq!(&buf[..len], data.as_bytes());
        }
    }

    #[test]
    fn factory_ring_pairs_are_independent() {
        let factory =
            SupervisorRingFactory::new(2, RingPairConfig::default()).expect("create factory");

        // Send to worker 0
        factory
            .ring_pair(0)
            .request_ring
            .try_send(b"for-worker-0", 0)
            .expect("send");

        // Worker 1's ring should be empty
        let mut buf = vec![0u8; 1024];
        assert!(factory
            .ring_pair(1)
            .request_ring
            .try_recv(&mut buf)
            .is_err());

        // Worker 0's ring should have the data
        let len = factory
            .ring_pair(0)
            .request_ring
            .try_recv(&mut buf)
            .expect("recv");
        assert_eq!(&buf[..len], b"for-worker-0");
    }

    #[test]
    fn worker_lifecycle_respawn_delay() {
        let pair = Arc::new(RingPair::create(RingPairConfig::default()).unwrap());
        let child = std::process::Command::new("true").spawn().unwrap();
        let mut lifecycle = WorkerLifecycle::new(child, pair, 0);

        // First crash: 1s delay
        lifecycle.record_crash();
        assert_eq!(lifecycle.respawn_delay(), std::time::Duration::from_secs(1));

        // Second crash: 2s delay
        lifecycle.record_crash();
        assert_eq!(lifecycle.respawn_delay(), std::time::Duration::from_secs(2));

        // Third: 4s
        lifecycle.record_crash();
        assert_eq!(lifecycle.respawn_delay(), std::time::Duration::from_secs(4));

        // Caps at 32s
        for _ in 0..10 {
            lifecycle.record_crash();
        }
        assert_eq!(
            lifecycle.respawn_delay(),
            std::time::Duration::from_secs(32)
        );
    }

    #[test]
    fn circuit_breaker_trips_after_10_crashes() {
        let pair = Arc::new(RingPair::create(RingPairConfig::default()).unwrap());
        let child = std::process::Command::new("true").spawn().unwrap();
        let mut lifecycle = WorkerLifecycle::new(child, pair, 0);

        for _ in 0..10 {
            lifecycle.record_crash();
        }
        assert!(
            !lifecycle.circuit_breaker_tripped(),
            "10 crashes should not trip"
        );

        lifecycle.record_crash(); // 11th
        assert!(
            lifecycle.circuit_breaker_tripped(),
            "11 crashes should trip"
        );
    }

    #[test]
    fn ring_pair_reclaim_resets_state() {
        let pair = RingPair::create(RingPairConfig::default()).unwrap();

        // Write some data to the request ring
        pair.request_ring.try_send(b"test data", 0).expect("send");

        // Reclaim (simulating worker death)
        unsafe {
            pair.reclaim();
        }

        // Ring should be empty after reclaim
        let mut buf = vec![0u8; 1024];
        assert!(
            pair.request_ring.try_recv(&mut buf).is_err(),
            "ring should be empty after reclaim"
        );

        // Ring should be usable for new data
        pair.request_ring
            .try_send(b"new data", 0)
            .expect("send after reclaim");
        let len = pair
            .request_ring
            .try_recv(&mut buf)
            .expect("recv after reclaim");
        assert_eq!(&buf[..len], b"new data");
    }

    #[cfg(unix)]
    #[test]
    fn stale_shm_cleanup_is_safe() {
        // Just verify it doesn't crash — we don't want to create actual /dev/shm files in tests
        cleanup_stale_shm();
    }

    #[cfg(unix)]
    #[test]
    fn factory_fds_are_inheritable() {
        let factory =
            SupervisorRingFactory::new(1, RingPairConfig::default()).expect("create factory");
        let pair = factory.ring_pair(0);
        let fds = pair.all_fds();

        // Verify FDs don't have CLOEXEC set
        for fd in &fds {
            let flags = unsafe { libc::fcntl(*fd, libc::F_GETFD) };
            assert!(flags >= 0, "fcntl should succeed");
            assert!(
                (flags & libc::FD_CLOEXEC) == 0,
                "FD_CLOEXEC should be cleared for inheritance"
            );
        }
    }
}
