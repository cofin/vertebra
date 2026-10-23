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
        let broker_table = Arc::clone(factory_lock.broker_table());
        let lifecycles = factory_lock.lifecycles_mut();

        for lifecycle in lifecycles.iter_mut() {
            if lifecycle.state == WorkerHealth::Running && !lifecycle.check_alive() {
                Self::process_worker_exit(lifecycle, shm, &broker_table);
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
        let broker_table = Arc::clone(factory_lock.broker_table());
        if let Some(lifecycle) = factory_lock.lifecycle_mut(worker_id) {
            Self::process_worker_exit(lifecycle, shm, &broker_table);
        } else {
            // If lifecycle not in factory (e.g. standalone test), handle directly on SHM
            warn!(
                worker_id,
                "lifecycle not registered in factory, sweeping SHM directly"
            );
            shm.sweep_worker(worker_id as u32);
        }
    }

    fn process_worker_exit(
        lifecycle: &mut WorkerLifecycle,
        shm: &Arc<ShmDispatcher>,
        broker_table: &Arc<vertebra_core::broker::ShmLeaseTable>,
    ) {
        warn!(
            worker_id = lifecycle.worker_id,
            pid = lifecycle.last_pid,
            "worker exited unexpectedly"
        );

        let reclaimed = broker_table.reclaim_worker(lifecycle.last_pid);
        if reclaimed > 0 {
            warn!(
                worker_id = lifecycle.worker_id,
                pid = lifecycle.last_pid,
                reclaimed,
                "reclaimed broker leases from exited worker"
            );
        }

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use vertebra_core::transport::ring_pair::RingPairConfig;

    #[test]
    fn supervisor_reclaims_broker_leases_on_worker_exit() {
        let mut factory =
            RingFactory::with_broker_config(1, RingPairConfig::default(), 2, 1).expect("factory");
        let pair = factory.ring_pair(0);
        let child = Command::new("true").spawn().expect("spawn true");
        let worker_pid = child.id();
        let lifecycle = WorkerLifecycle::new(child, pair, 0, "app:app".into(), "python".into());
        factory.add_lifecycle(lifecycle);

        let lease1 = factory
            .broker_table()
            .try_acquire(worker_pid, 60_000_000_000, 11)
            .expect("lease 1");
        let lease2 = factory
            .broker_table()
            .try_acquire(worker_pid, 60_000_000_000, 22)
            .expect("lease 2");
        assert_eq!(lease1.slot_index, 0);
        assert_eq!(lease2.slot_index, 1);
        assert_eq!(factory.broker_table().stats().active_leases, 2);

        let shm = ShmDispatcher::new(factory.all_ring_pairs().to_vec(), 64)
            .with_lease_table(Arc::clone(factory.broker_table()));
        let factory = Arc::new(Mutex::new(factory));

        Supervisor::handle_worker_exit(&factory, &shm, 0);

        let stats = factory.lock().unwrap().broker_table().stats();
        assert_eq!(stats.active_leases, 0);
        assert_eq!(stats.total_reclaimed, 2);
    }

    #[tokio::test]
    async fn supervisor_poll_workers_reclaims_broker_leases_on_exit() {
        let mut factory =
            RingFactory::with_broker_config(1, RingPairConfig::default(), 2, 0).expect("factory");
        let pair = factory.ring_pair(0);
        let mut child = Command::new("true").spawn().expect("spawn true");
        let worker_pid = child.id();
        let _ = child.wait();
        let lifecycle = WorkerLifecycle::new(child, pair, 0, "app:app".into(), "python".into());
        factory.add_lifecycle(lifecycle);

        let _lease = factory
            .broker_table()
            .try_acquire(worker_pid, 60_000_000_000, 99)
            .expect("lease");
        assert_eq!(factory.broker_table().stats().active_leases, 1);

        let shm = ShmDispatcher::new(factory.all_ring_pairs().to_vec(), 64);
        let factory = Arc::new(Mutex::new(factory));

        Supervisor::poll_workers(&factory, &shm).await;

        let stats = factory.lock().unwrap().broker_table().stats();
        assert_eq!(stats.active_leases, 0);
        assert_eq!(stats.total_reclaimed, 1);
    }
}
