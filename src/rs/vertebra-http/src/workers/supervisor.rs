//! Worker process supervisor for crash detection, request failure, ring reclamation, and respawn.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use super::factory::{RingFactory, WorkerHealth, WorkerLifecycle};
use crate::dispatcher::ShmDispatcher;

/// Supervisor monitors child worker processes, sweeps in-flight requests on crash,
/// reclaims rings, and manages exponential backoff respawning.
pub struct Supervisor;

impl Supervisor {
    /// Spawn the supervisor background task.
    pub fn spawn(
        factory: Arc<Mutex<RingFactory>>,
        shm: Arc<ShmDispatcher>,
        shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        debug!("worker supervisor task cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        Self::poll_workers(&factory, &shm).await;
                    }
                }
            }
        })
    }

    /// Poll all managed workers for exit and backoff respawn eligibility.
    pub async fn poll_workers(factory: &Arc<Mutex<RingFactory>>, shm: &Arc<ShmDispatcher>) {
        let mut factory_lock = factory.lock().unwrap();
        let lifecycles = factory_lock.lifecycles_mut();

        for lifecycle in lifecycles.iter_mut() {
            if lifecycle.state == WorkerHealth::Running && !lifecycle.check_alive() {
                Self::process_worker_exit(lifecycle, shm);
            } else if lifecycle.state == WorkerHealth::Respawning
                && let Some(last_crash) = lifecycle.last_crash
                && last_crash.elapsed() >= lifecycle.respawn_delay()
            {
                if !lifecycle.circuit_breaker_tripped() {
                    info!(worker_id = lifecycle.worker_id, "respawning worker");
                    #[cfg(unix)]
                    if let Err(e) = lifecycle.respawn() {
                        error!(worker_id = lifecycle.worker_id, error = %e, "failed to respawn worker");
                    }
                } else {
                    error!(
                        worker_id = lifecycle.worker_id,
                        "circuit breaker tripped for worker, marking dead"
                    );
                    lifecycle.state = WorkerHealth::Dead;
                }
            }
        }
    }

    /// Handle worker crash for a specific worker by ID.
    pub fn handle_worker_exit(
        factory: &Arc<Mutex<RingFactory>>,
        shm: &Arc<ShmDispatcher>,
        worker_id: usize,
    ) {
        let mut factory_lock = factory.lock().unwrap();
        if let Some(lifecycle) = factory_lock.lifecycle_mut(worker_id) {
            Self::process_worker_exit(lifecycle, shm);
        } else {
            // If lifecycle not in factory (e.g. standalone test), handle directly on SHM
            warn!(
                worker_id,
                "lifecycle not registered in factory, sweeping SHM directly"
            );
            shm.sweep_worker(worker_id as u32);
        }
    }

    fn process_worker_exit(lifecycle: &mut WorkerLifecycle, shm: &Arc<ShmDispatcher>) {
        warn!(
            worker_id = lifecycle.worker_id,
            "worker exited unexpectedly"
        );

        // (a) Sweep in-flight requests immediately with 502 Bad Gateway
        let swept = shm.sweep_worker(lifecycle.worker_id as u32);
        info!(
            worker_id = lifecycle.worker_id,
            swept, "swept in-flight requests for exited worker"
        );

        // (b) Reclaim ring pair (reset ring indices, clear poison flags)
        unsafe {
            lifecycle.ring_pair.reclaim();
        }

        // (c) Record crash
        lifecycle.record_crash();

        // (d) Set state to Respawning if circuit breaker not tripped
        if !lifecycle.circuit_breaker_tripped() {
            lifecycle.state = WorkerHealth::Respawning;
        } else {
            error!(
                worker_id = lifecycle.worker_id,
                "circuit breaker tripped for worker, marking dead"
            );
            lifecycle.state = WorkerHealth::Dead;
        }
    }
}
