//! Ring factory for multi-process workers with shared-memory ring pairs.
//!
//! Creates SPSC ring pairs before spawning Python worker processes.
//! Each worker inherits the ring FDs and reconstructs its transport from environment variables.

use std::process::{Child, Command};
use std::sync::Arc;

use tracing::{error, info};

use vertebra_core::broker::ShmLeaseTable;
use vertebra_core::process;
use vertebra_core::shm::ShmRegion;
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
    pub last_pid: u32,
    pub last_exit_code: Option<i32>,
    pub app_path: String,
    pub python_bin: String,
    #[cfg(unix)]
    pub broker_shm_fd: std::os::fd::RawFd,
    pub broker_shm_size: usize,
    pub broker_pool_size: u32,
    pub broker_max_overflow: u32,
    pub lease_table: Option<Arc<ShmLeaseTable>>,
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
        let last_pid = child.id();

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
            last_pid,
            last_exit_code: None,
            app_path,
            python_bin,
            #[cfg(unix)]
            broker_shm_fd: -1,
            broker_shm_size: 0,
            broker_pool_size: 20,
            broker_max_overflow: 10,
            lease_table: None,
        }
    }

    /// Attach a shared-memory lease table to this worker lifecycle.
    pub fn with_lease_table(mut self, lease_table: Arc<ShmLeaseTable>) -> Self {
        self.lease_table = Some(lease_table);
        self
    }

    /// Set the shared-memory lease table for this worker lifecycle.
    pub fn set_lease_table(&mut self, lease_table: Arc<ShmLeaseTable>) {
        self.lease_table = Some(lease_table);
    }

    /// Reclaim any active connection leases held by this worker's last PID.
    pub fn reclaim_worker_leases(&self) -> u32 {
        if let Some(ref table) = self.lease_table {
            table.reclaim_worker(self.last_pid)
        } else {
            0
        }
    }

    /// Check if the worker process has exited (non-blocking).
    pub fn check_alive(&mut self) -> bool {
        if let Some(ref mut child) = self.child {
            match child.try_wait() {
                Ok(Some(status)) => {
                    self.last_exit_code = status.code();
                    self.state = WorkerHealth::Dead;
                    false
                }
                Ok(None) => true,
                Err(_) => {
                    self.last_exit_code = None;
                    self.state = WorkerHealth::Dead;
                    false
                }
            }
        } else {
            false
        }
    }

    /// Return the OS exit code observed for the last exited child process, if any.
    pub fn last_exit_code(&self) -> Option<i32> {
        self.last_exit_code
    }

    /// Check if the worker is alive, and if it exited, reclaim its broker leases.
    pub fn check_alive_and_reclaim(&mut self, broker_table: &ShmLeaseTable) -> (bool, u32) {
        if self.check_alive() {
            (true, 0)
        } else {
            let reclaimed = broker_table.reclaim_worker(self.last_pid);
            if reclaimed > 0 {
                tracing::warn!(
                    worker_id = self.worker_id,
                    pid = self.last_pid,
                    reclaimed,
                    "reclaimed broker leases from exited worker"
                );
            }
            (false, reclaimed)
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

        if self.broker_shm_fd >= 0 && self.broker_shm_size > 0 {
            cmd.env("VERTEBRA_BROKER_SHM_FD", self.broker_shm_fd.to_string());
            cmd.env("VERTEBRA_BROKER_SHM_SIZE", self.broker_shm_size.to_string());
            cmd.env(
                "VERTEBRA_BROKER_POOL_SIZE",
                self.broker_pool_size.to_string(),
            );
            cmd.env(
                "VERTEBRA_BROKER_MAX_OVERFLOW",
                self.broker_max_overflow.to_string(),
            );
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

        self.last_pid = child.id();
        self.last_exit_code = None;
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
    broker_table: Arc<ShmLeaseTable>,
    #[cfg(unix)]
    broker_shm_fd: std::os::fd::RawFd,
    broker_shm_size: usize,
}

impl RingFactory {
    /// Create ring pairs for `worker_count` workers.
    pub fn new(worker_count: usize, config: RingPairConfig) -> Result<Self, RingPairError> {
        Self::with_broker_config(worker_count, config, 20, 10)
    }

    /// Create ring pairs and an atomic shared-memory connection broker lease table.
    pub fn with_broker_config(
        worker_count: usize,
        config: RingPairConfig,
        pool_size: u32,
        max_overflow: u32,
    ) -> Result<Self, RingPairError> {
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

        let broker_shm_size =
            ShmLeaseTable::required_size_with_statements(pool_size, max_overflow, 512);
        let region = ShmRegion::create(broker_shm_size).map_err(RingPairError::Shm)?;

        #[cfg(unix)]
        let broker_shm_fd = {
            let fd = region.handle().unwrap_or(-1);
            if fd >= 0 {
                process::make_inheritable(&[fd]);
            }
            fd
        };

        let broker_table = Arc::new(
            ShmLeaseTable::init_in_region_with_statements(region, pool_size, max_overflow, 512)
                .map_err(|e| RingPairError::Io(std::io::Error::other(e.to_string())))?,
        );

        Ok(Self {
            pairs,
            lifecycles: Vec::with_capacity(worker_count),
            broker_table,
            #[cfg(unix)]
            broker_shm_fd,
            broker_shm_size,
        })
    }

    /// Return a reference to the shared-memory connection broker lease table.
    pub fn broker_table(&self) -> &Arc<ShmLeaseTable> {
        &self.broker_table
    }

    /// Reclaim all connection leases held by an exited worker PID.
    pub fn reclaim_worker_leases(&self, worker_pid: u32) -> u32 {
        self.broker_table.reclaim_worker(worker_pid)
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

        #[cfg(unix)]
        {
            cmd.env("VERTEBRA_BROKER_SHM_FD", self.broker_shm_fd.to_string());
            cmd.env("VERTEBRA_BROKER_SHM_SIZE", self.broker_shm_size.to_string());
            cmd.env(
                "VERTEBRA_BROKER_POOL_SIZE",
                self.broker_table.pool_size().to_string(),
            );
            cmd.env(
                "VERTEBRA_BROKER_MAX_OVERFLOW",
                self.broker_table.max_overflow().to_string(),
            );
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
        let pool_size = self.broker_table.pool_size();
        let max_overflow = self.broker_table.max_overflow();
        self.lifecycles = children
            .into_iter()
            .enumerate()
            .map(|(i, child)| {
                let mut lc = WorkerLifecycle::new(
                    child,
                    Arc::clone(&self.pairs[i]),
                    i,
                    app_path.to_owned(),
                    python_bin.to_owned(),
                );
                #[cfg(unix)]
                {
                    lc.broker_shm_fd = self.broker_shm_fd;
                }
                lc.broker_shm_size = self.broker_shm_size;
                lc.broker_pool_size = pool_size;
                lc.broker_max_overflow = max_overflow;
                lc.lease_table = Some(Arc::clone(&self.broker_table));
                lc
            })
            .collect();
    }

    /// Add a lifecycle directly (e.g. for testing).
    pub fn add_lifecycle(&mut self, mut lifecycle: WorkerLifecycle) {
        #[cfg(unix)]
        if lifecycle.broker_shm_fd < 0 {
            lifecycle.broker_shm_fd = self.broker_shm_fd;
        }
        if lifecycle.broker_shm_size == 0 {
            lifecycle.broker_shm_size = self.broker_shm_size;
        }
        lifecycle.broker_pool_size = self.broker_table.pool_size();
        lifecycle.broker_max_overflow = self.broker_table.max_overflow();
        if lifecycle.lease_table.is_none() {
            lifecycle.lease_table = Some(Arc::clone(&self.broker_table));
        }
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

/// Compute `(tokio_threads, py_threads)` per `SO_REUSEPORT` embedded worker process
/// given `available_cpus`, `worker_count`, and optional requested `threads_hint`.
pub fn compute_embedded_worker_budgets(
    available_cpus: usize,
    worker_count: usize,
    threads_hint: Option<usize>,
) -> (usize, usize) {
    let cpus = available_cpus.max(1);
    let workers = worker_count.max(1);
    let tokio_threads = (cpus / (2 * workers)).max(1);
    let max_py_per_worker = (cpus / workers).max(1);
    let py_threads = threads_hint
        .filter(|&n| n > 0)
        .map_or(max_py_per_worker, |n| n.min(max_py_per_worker).max(1));
    (tokio_threads, py_threads)
}

#[cfg(unix)]
struct EmbeddedWorkerEntry {
    worker_id: usize,
    child: Option<Child>,
    last_pid: u32,
    state: WorkerHealth,
    crash_count: u32,
    last_crash: Option<std::time::Instant>,
    last_spawn: Option<std::time::Instant>,
}

#[cfg(unix)]
impl EmbeddedWorkerEntry {
    fn check_alive(&mut self) -> bool {
        if let Some(ref mut child) = self.child {
            match child.try_wait() {
                Ok(Some(_)) | Err(_) => {
                    self.state = WorkerHealth::Dead;
                    false
                }
                Ok(None) => true,
            }
        } else {
            false
        }
    }

    fn record_crash(&mut self) {
        let now = std::time::Instant::now();
        if let Some(spawn_time) = self.last_spawn
            && now.duration_since(spawn_time) > std::time::Duration::from_secs(60)
        {
            self.crash_count = 0;
        }
        self.crash_count += 1;
        self.last_crash = Some(now);
        self.state = WorkerHealth::Dead;
    }

    fn respawn_delay(&self) -> std::time::Duration {
        let exponent = self.crash_count.saturating_sub(1).min(5);
        std::time::Duration::from_secs(1u64 << exponent)
    }

    fn circuit_breaker_tripped(&self) -> bool {
        if self.crash_count <= 10 {
            return false;
        }
        if let Some(last_crash) = self.last_crash {
            return last_crash.elapsed() < std::time::Duration::from_secs(300);
        }
        false
    }
}

/// Multi-process `SO_REUSEPORT` supervisor for `Topology::Embedded` (`workers > 1`).
///
/// Pre-initializes a shared-memory `ShmLeaseTable` region for cross-worker connection
/// pooling and spawns `W` independent `vertebra-server` processes binding with `SO_REUSEPORT`.
#[cfg(unix)]
pub struct EmbeddedSupervisor {
    worker_count: usize,
    exe_path: std::path::PathBuf,
    broker_table: Arc<ShmLeaseTable>,
    broker_shm_fd: std::os::fd::RawFd,
    broker_shm_size: usize,
    entries: Vec<EmbeddedWorkerEntry>,
}

#[cfg(unix)]
impl EmbeddedSupervisor {
    /// Initialize shared broker SHM region and prepare `worker_count` embedded worker slots.
    pub fn new(
        worker_count: usize,
        pool_size: u32,
        max_overflow: u32,
        stmt_capacity: u32,
    ) -> Result<Self, RingFactoryError> {
        let exe_path = std::env::current_exe().map_err(RingFactoryError::Spawn)?;
        let stmt_cap = stmt_capacity.max(1);
        let broker_shm_size =
            ShmLeaseTable::required_size_with_statements(pool_size, max_overflow, stmt_cap);
        let region = ShmRegion::create(broker_shm_size).map_err(RingPairError::Shm)?;
        let broker_shm_fd = {
            let fd = region.handle().unwrap_or(-1);
            if fd >= 0 {
                process::make_inheritable(&[fd]);
            }
            fd
        };
        let broker_table = Arc::new(
            ShmLeaseTable::init_in_region_with_statements(
                region,
                pool_size,
                max_overflow,
                stmt_cap,
            )
            .map_err(|e| RingPairError::Io(std::io::Error::other(e.to_string())))?,
        );

        Ok(Self {
            worker_count,
            exe_path,
            broker_table,
            broker_shm_fd,
            broker_shm_size,
            entries: Vec::with_capacity(worker_count),
        })
    }

    /// Return a reference to the shared-memory connection broker lease table.
    pub fn broker_table(&self) -> &Arc<ShmLeaseTable> {
        &self.broker_table
    }

    fn spawn_one(&self, worker_id: usize) -> Result<Child, RingFactoryError> {
        let available_cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let threads_hint = std::env::var("VERTEBRA_THREADS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok());
        let (tokio_threads, py_threads) =
            compute_embedded_worker_budgets(available_cpus, self.worker_count, threads_hint);

        let mut cmd = Command::new(&self.exe_path);
        cmd.env("VERTEBRA_EMBEDDED_WORKER", "1")
            .env("VERTEBRA_REUSE_PORT", "1")
            .env("VERTEBRA_TOPOLOGY", "embedded")
            .env("VERTEBRA_WORKERS", "1")
            .env("VERTEBRA_WORKER_ID", worker_id.to_string())
            .env("VERTEBRA_THREADS", py_threads.to_string());

        if std::env::var_os("VERTEBRA_TOKIO_THREADS").is_none() {
            cmd.env("VERTEBRA_TOKIO_THREADS", tokio_threads.to_string());
        }

        if self.broker_shm_fd >= 0 && self.broker_shm_size > 0 {
            cmd.env("VERTEBRA_BROKER_SHM_FD", self.broker_shm_fd.to_string())
                .env("VERTEBRA_BROKER_SHM_SIZE", self.broker_shm_size.to_string())
                .env(
                    "VERTEBRA_BROKER_POOL_SIZE",
                    self.broker_table.pool_size().to_string(),
                )
                .env(
                    "VERTEBRA_BROKER_MAX_OVERFLOW",
                    self.broker_table.max_overflow().to_string(),
                );
        }

        #[cfg(target_os = "linux")]
        process::set_parent_death_signal(&mut cmd);

        let child = cmd.spawn().map_err(RingFactoryError::Spawn)?;
        info!(
            worker_id,
            pid = child.id(),
            tokio_threads,
            py_threads,
            "spawned SO_REUSEPORT embedded worker process"
        );
        Ok(child)
    }

    /// Spawn all `W` `SO_REUSEPORT` embedded worker processes.
    pub fn spawn_all(&mut self) -> Result<(), RingFactoryError> {
        self.entries.clear();
        for worker_id in 0..self.worker_count {
            match self.spawn_one(worker_id) {
                Ok(child) => {
                    let pid = child.id();
                    self.entries.push(EmbeddedWorkerEntry {
                        worker_id,
                        child: Some(child),
                        last_pid: pid,
                        state: WorkerHealth::Running,
                        crash_count: 0,
                        last_crash: None,
                        last_spawn: Some(std::time::Instant::now()),
                    });
                }
                Err(e) => {
                    error!(worker_id, error = %e, "failed to spawn embedded worker");
                    let mut spawned: Vec<Child> = self
                        .entries
                        .iter_mut()
                        .filter_map(|x| x.child.take())
                        .collect();
                    RingFactory::shutdown_workers(&mut spawned, std::time::Duration::from_secs(2));
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    fn poll_entries(&mut self) {
        let mut to_respawn = Vec::new();
        for (idx, entry) in self.entries.iter_mut().enumerate() {
            if entry.state == WorkerHealth::Running && !entry.check_alive() {
                let reclaimed = self.broker_table.reclaim_worker(entry.last_pid);
                tracing::warn!(
                    worker_id = entry.worker_id,
                    pid = entry.last_pid,
                    reclaimed,
                    "embedded worker exited unexpectedly"
                );
                entry.record_crash();
                if !entry.circuit_breaker_tripped() {
                    entry.state = WorkerHealth::Respawning;
                }
            } else if entry.state == WorkerHealth::Respawning
                && let Some(last_crash) = entry.last_crash
                && last_crash.elapsed() >= entry.respawn_delay()
            {
                if !entry.circuit_breaker_tripped() {
                    to_respawn.push(idx);
                } else {
                    error!(
                        worker_id = entry.worker_id,
                        "circuit breaker tripped for embedded worker"
                    );
                    entry.state = WorkerHealth::Dead;
                }
            }
        }

        for idx in to_respawn {
            let worker_id = self.entries[idx].worker_id;
            match self.spawn_one(worker_id) {
                Ok(child) => {
                    let pid = child.id();
                    let entry = &mut self.entries[idx];
                    entry.last_pid = pid;
                    entry.child = Some(child);
                    entry.state = WorkerHealth::Running;
                    entry.last_spawn = Some(std::time::Instant::now());
                }
                Err(e) => {
                    error!(worker_id, error = %e, "failed to respawn embedded worker");
                }
            }
        }
    }

    /// Supervise child workers until `shutdown` resolves, then gracefully terminate all children.
    pub async fn run(&mut self, shutdown: impl std::future::Future<Output = ()>) {
        tokio::pin!(shutdown);
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    info!("shutting down SO_REUSEPORT embedded workers");
                    break;
                }
                _ = interval.tick() => {
                    self.poll_entries();
                }
            }
        }
        let mut children: Vec<Child> = self
            .entries
            .iter_mut()
            .filter_map(|e| e.child.take())
            .collect();
        RingFactory::shutdown_workers(&mut children, std::time::Duration::from_secs(5));
    }
}

/// Clean up stale shared memory regions from previous crashed supervisors.
pub fn cleanup_stale_shm() {
    #[cfg(unix)]
    {
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

    #[test]
    fn factory_broker_colocates_statement_cache_and_reclaims_leases() {
        let mut factory =
            RingFactory::with_broker_config(1, RingPairConfig::default(), 4, 2).expect("factory");
        assert_eq!(factory.broker_table().stmt_capacity(), 512);
        let stmt_cache = factory
            .broker_table()
            .statement_cache()
            .expect("co-located statement cache");
        assert!(stmt_cache.insert(1, b"SELECT 1", 0, b"SELECT 1", b""));
        assert!(stmt_cache.lookup(1, b"SELECT 1").is_some());

        let child = Command::new("true").spawn().expect("spawn true");
        let worker_pid = child.id();
        let lifecycle = WorkerLifecycle::new(
            child,
            factory.ring_pair(0),
            0,
            "app:app".into(),
            "python".into(),
        );
        factory.add_lifecycle(lifecycle);

        let _grant = factory
            .broker_table()
            .try_acquire(worker_pid, 60_000_000_000, 777)
            .expect("grant");
        assert_eq!(factory.broker_table().stats().active_leases, 1);
        assert_eq!(factory.lifecycles_mut()[0].reclaim_worker_leases(), 1);
        assert_eq!(factory.broker_table().stats().active_leases, 0);
    }

    #[test]
    fn embedded_worker_budgets_prevent_core_oversubscription() {
        assert_eq!(compute_embedded_worker_budgets(8, 4, Some(48)), (1, 2));
        assert_eq!(compute_embedded_worker_budgets(8, 8, Some(48)), (1, 1));
        assert_eq!(compute_embedded_worker_budgets(8, 4, Some(1)), (1, 1));
        assert_eq!(compute_embedded_worker_budgets(1, 1, Some(48)), (1, 1));
    }

    #[cfg(unix)]
    #[test]
    fn embedded_supervisor_initializes_inheritable_broker_table() {
        let sup = EmbeddedSupervisor::new(4, 8, 2, 128).expect("embedded supervisor");
        assert_eq!(sup.broker_table().pool_size(), 8);
        assert_eq!(sup.broker_table().max_overflow(), 2);
        assert_eq!(sup.broker_table().stmt_capacity(), 128);
        assert!(sup.broker_shm_fd >= 0);
        let flags = unsafe { libc::fcntl(sup.broker_shm_fd, libc::F_GETFD) };
        assert!(flags >= 0 && (flags & libc::FD_CLOEXEC) == 0);
    }
}
