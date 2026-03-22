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
    config: RingPairConfig,
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

        Ok(Self { pairs, config })
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
