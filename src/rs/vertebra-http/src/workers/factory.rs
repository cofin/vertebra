//! Ring factory for multi-process workers with shared-memory ring pairs.
//!
//! Creates SPSC ring pairs before spawning Python worker processes.
//! Each worker inherits the ring FDs and reconstructs its transport from environment variables.

use std::process::{Child, Command};
use std::sync::Arc;

use tracing::{error, info};

use vertebra_core::process;
use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig, RingPairError};

/// State of a managed worker process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerHealth {
    Running,
    Dead,
    Respawning,
}

/// Tracks a single worker's lifecycle for crash detection and respawn.
pub struct WorkerLifecycle {
    pub child: Option<Child>,
    #[cfg(target_os = "linux")]
    pub pidfd: Option<std::os::fd::OwnedFd>,
    pub ring_pair: Arc<RingPair>,
    pub state: WorkerHealth,
    pub crash_count: u32,
    pub last_crash: Option<std::time::Instant>,
    pub last_spawn: Option<std::time::Instant>,
    pub worker_id: usize,
    pub app_path: String,
    pub python_bin: String,
}

impl WorkerLifecycle {
    /// Create a new lifecycle tracker for a just-spawned worker.
    pub fn new(
        child: Child,
        ring_pair: Arc<RingPair>,
        worker_id: usize,
        app_path: String,
        python_bin: String,
    ) -> Self {
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
            state: WorkerHealth::Running,
            crash_count: 0,
            last_crash: None,
            last_spawn: Some(std::time::Instant::now()),
            worker_id,
            app_path,
            python_bin,
        }
    }

    /// Check if the worker process has exited (non-blocking).
    pub fn check_alive(&mut self) -> bool {
        if let Some(ref mut child) = self.child {
            match child.try_wait() {
                Ok(Some(_status)) => {
                    self.state = WorkerHealth::Dead;
                    false
                }
                Ok(None) => true,
                Err(_) => {
                    self.state = WorkerHealth::Dead;
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
        if let Some(spawn_time) = self.last_spawn
            && now.duration_since(spawn_time) > std::time::Duration::from_secs(60)
        {
            self.crash_count = 0;
        }

        self.crash_count += 1;
        self.last_crash = Some(now);
        self.state = WorkerHealth::Dead;
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
        if let Some(first_crash) = self.last_crash {
            let window = std::time::Duration::from_secs(300);
            if first_crash.elapsed() < window {
                return true;
            }
        }
        false
    }

    /// Respawn the worker process.
    #[cfg(unix)]
    pub fn respawn(&mut self) -> Result<(), RingFactoryError> {
        let fds = self.ring_pair.fd_set();
        let env_vars = fds.to_env_vars(self.worker_id);

        let mut cmd = Command::new(&self.python_bin);
        cmd.args(["-m", "vertebra.supervisor._worker"]);

        for (key, value) in &env_vars {
            cmd.env(key, value);
        }

        cmd.env("VERTEBRA_APP", &self.app_path);

        if let Ok(threads) = std::env::var("VERTEBRA_THREADS") {
            cmd.env("VERTEBRA_THREADS", threads);
        }

        #[cfg(target_os = "linux")]
        process::set_parent_death_signal(&mut cmd);

        let child = cmd.spawn().map_err(RingFactoryError::Spawn)?;
        info!(
            worker_id = self.worker_id,
            pid = child.id(),
            "respawned worker process"
        );

        #[cfg(target_os = "linux")]
        {
            use vertebra_core::platform::linux::pidfd_open;
            self.pidfd = pidfd_open(child.id() as i32).ok();
        }

        self.child = Some(child);
        self.state = WorkerHealth::Running;
        self.last_spawn = Some(std::time::Instant::now());

        Ok(())
    }
}

/// Creates and manages SPSC ring pairs for multi-process workers.
pub struct RingFactory {
    pairs: Vec<Arc<RingPair>>,
    lifecycles: Vec<WorkerLifecycle>,
}

impl RingFactory {
    /// Create ring pairs for `worker_count` workers.
    pub fn new(worker_count: usize, config: RingPairConfig) -> Result<Self, RingPairError> {
        let mut pairs = Vec::with_capacity(worker_count);
        for i in 0..worker_count {
            let pair = RingPair::create(config.clone())?;

            #[cfg(unix)]
            {
                let fds = pair.all_fds();
                process::make_inheritable(&fds);
            }

            info!(worker_id = i, "created ring pair for worker");
            pairs.push(Arc::new(pair));
        }

        Ok(Self {
            pairs,
            lifecycles: Vec::with_capacity(worker_count),
        })
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

    /// Get mutable reference to lifecycles.
    pub fn lifecycles_mut(&mut self) -> &mut [WorkerLifecycle] {
        &mut self.lifecycles
    }

    /// Get mutable reference to a specific worker's lifecycle.
    pub fn lifecycle_mut(&mut self, worker_id: usize) -> Option<&mut WorkerLifecycle> {
        self.lifecycles
            .iter_mut()
            .find(|l| l.worker_id == worker_id)
    }

    /// Spawn a single Python worker process.
    #[cfg(unix)]
    pub fn spawn_worker(
        &self,
        worker_id: usize,
        app_path: &str,
        python_bin: &str,
    ) -> Result<Child, RingFactoryError> {
        let pair = &self.pairs[worker_id];
        let fds = pair.fd_set();
        let env_vars = fds.to_env_vars(worker_id);

        let mut cmd = Command::new(python_bin);
        cmd.args(["-m", "vertebra.supervisor._worker"]);

        for (key, value) in &env_vars {
            cmd.env(key, value);
        }

        cmd.env("VERTEBRA_APP", app_path);

        if let Ok(threads) = std::env::var("VERTEBRA_THREADS") {
            cmd.env("VERTEBRA_THREADS", threads);
        }

        #[cfg(target_os = "linux")]
        process::set_parent_death_signal(&mut cmd);

        let child = cmd.spawn().map_err(RingFactoryError::Spawn)?;
        info!(worker_id, pid = child.id(), "spawned worker process");

        Ok(child)
    }

    /// Spawn all worker processes and initialize lifecycles.
    #[cfg(unix)]
    pub fn spawn_all(
        &mut self,
        app_path: &str,
        python_bin: &str,
    ) -> Result<Vec<Child>, RingFactoryError> {
        let mut children = Vec::with_capacity(self.pairs.len());
        self.lifecycles.clear();

        for i in 0..self.pairs.len() {
            match self.spawn_worker(i, app_path, python_bin) {
                Ok(child) => {
                    children.push(child);
                }
                Err(e) => {
                    error!(worker_id = i, error = %e, "failed to spawn worker");
                    for mut c in children {
                        let _ = c.kill();
                    }
                    return Err(e);
                }
            }
        }

        // We can't duplicate Child, so lifecycles take ownership of children if needed,
        // or we return the children for main.rs and store Option<Child> in lifecycles.
        // For supervisor monitoring, we keep child handles in lifecycles.
        // Let's store children in lifecycles and return them or keep them in factory.
        info!(count = self.pairs.len(), "all workers spawned");
        Ok(children)
    }

    /// Attach spawned children to lifecycles.
    pub fn attach_children(&mut self, children: Vec<Child>, app_path: &str, python_bin: &str) {
        self.lifecycles = children
            .into_iter()
            .enumerate()
            .map(|(i, child)| {
                WorkerLifecycle::new(
                    child,
                    Arc::clone(&self.pairs[i]),
                    i,
                    app_path.to_owned(),
                    python_bin.to_owned(),
                )
            })
            .collect();
    }

    /// Add a lifecycle directly (e.g. for testing).
    pub fn add_lifecycle(&mut self, lifecycle: WorkerLifecycle) {
        self.lifecycles.push(lifecycle);
    }

    /// Gracefully shut down all workers.
    #[cfg(unix)]
    pub fn shutdown_workers(children: &mut [Child], timeout: std::time::Duration) {
        use std::time::Instant;

        for child in children.iter_mut() {
            let pid = child.id();
            unsafe {
                libc::kill(pid as i32, libc::SIGTERM);
            }
        }

        let deadline = Instant::now() + timeout;

        for child in children.iter_mut() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match child.try_wait() {
                Ok(Some(_)) => continue,
                Ok(None) => {
                    std::thread::sleep(remaining.min(std::time::Duration::from_millis(100)));
                }
                Err(_) => continue,
            }
        }

        for child in children.iter_mut() {
            match child.try_wait() {
                Ok(Some(_)) => {}
                _ => {
                    let _ = child.kill();
                    let _ = child.wait();
                }
            }
        }
    }
}

/// Clean up stale shared memory regions from previous crashed supervisors.
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
                if let Some(name) = entry.file_name().to_str()
                    && (name.starts_with("vertebra-") || name.starts_with("vertebra_"))
                {
                    tracing::warn!(name, "cleaning up stale SHM region");
                    let _ = fs::remove_file(entry.path());
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

/// Errors from ring factory operations.
#[derive(Debug, thiserror::Error)]
pub enum RingFactoryError {
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
        let factory = RingFactory::new(4, RingPairConfig::default()).expect("create factory");
        assert_eq!(factory.worker_count(), 4);

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
        let factory = RingFactory::new(2, RingPairConfig::default()).expect("create factory");

        factory
            .ring_pair(0)
            .request_ring
            .try_send(b"for-worker-0", 0)
            .expect("send");

        let mut buf = vec![0u8; 1024];
        assert!(
            factory
                .ring_pair(1)
                .request_ring
                .try_recv(&mut buf)
                .is_err()
        );

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
        let child = Command::new("true").spawn().unwrap();
        let mut lifecycle = WorkerLifecycle::new(child, pair, 0, "app:app".into(), "python".into());

        lifecycle.record_crash();
        assert_eq!(lifecycle.respawn_delay(), std::time::Duration::from_secs(1));

        lifecycle.record_crash();
        assert_eq!(lifecycle.respawn_delay(), std::time::Duration::from_secs(2));

        lifecycle.record_crash();
        assert_eq!(lifecycle.respawn_delay(), std::time::Duration::from_secs(4));

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
        let child = Command::new("true").spawn().unwrap();
        let mut lifecycle = WorkerLifecycle::new(child, pair, 0, "app:app".into(), "python".into());

        for _ in 0..10 {
            lifecycle.record_crash();
        }
        assert!(
            !lifecycle.circuit_breaker_tripped(),
            "10 crashes should not trip"
        );

        lifecycle.record_crash();
        assert!(
            lifecycle.circuit_breaker_tripped(),
            "11 crashes should trip"
        );
    }

    #[test]
    fn ring_pair_reclaim_resets_state() {
        let pair = RingPair::create(RingPairConfig::default()).unwrap();

        pair.request_ring.try_send(b"test data", 0).expect("send");

        unsafe {
            pair.reclaim();
        }

        let mut buf = vec![0u8; 1024];
        assert!(
            pair.request_ring.try_recv(&mut buf).is_err(),
            "ring should be empty after reclaim"
        );

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
        cleanup_stale_shm();
    }

    #[cfg(unix)]
    #[test]
    fn factory_fds_are_inheritable() {
        let factory = RingFactory::new(1, RingPairConfig::default()).expect("create factory");
        let pair = factory.ring_pair(0);
        let fds = pair.all_fds();

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
