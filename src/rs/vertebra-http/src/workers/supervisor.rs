//! Worker process supervisor for crash detection, request failure, ring reclamation, and respawn.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use super::factory::{RingFactory, WorkerHealth, WorkerLifecycle};
use crate::dispatcher::ShmDispatcher;

/// Process exit code signaling permanent worker startup or lifespan failure.
pub const EXIT_STARTUP_FAILURE: i32 = 78;

/// Supervisor monitors child worker processes, sweeps in-flight requests on crash,
/// reclaims rings, and manages exponential backoff respawning.
pub struct Supervisor;

impl Supervisor {
    /// Spawn the supervisor background task.
    pub fn spawn(
        factory: Arc<Mutex<RingFactory>>,
        shm: Arc<ShmDispatcher>,
        shutdown: CancellationToken,
        startup_failed: Arc<AtomicBool>,
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
                        if Self::poll_workers(&factory, &shm).await {
                            startup_failed.store(true, Ordering::Release);
                            shutdown.cancel();
                            break;
                        }
                    }
                }
            }
        })
    }

    /// Poll all managed workers for exit and backoff respawn eligibility.
    ///
    /// Returns `true` when any worker exits with [`EXIT_STARTUP_FAILURE`], signaling
    /// the server to abort startup rather than respawning.
    pub async fn poll_workers(factory: &Arc<Mutex<RingFactory>>, shm: &Arc<ShmDispatcher>) -> bool {
        let mut factory_lock = factory.lock().unwrap();
        let broker_table = Arc::clone(factory_lock.broker_table());
        let lifecycles = factory_lock.lifecycles_mut();
        let mut startup_failure_detected = false;

        for lifecycle in lifecycles.iter_mut() {
            if lifecycle.state == WorkerHealth::Running && !lifecycle.check_alive() {
                if Self::process_worker_exit(lifecycle, shm, &broker_table) {
                    startup_failure_detected = true;
                }
            } else if lifecycle.state == WorkerHealth::Respawning
                && let Some(last_crash) = lifecycle.last_crash
                && last_crash.elapsed() >= lifecycle.respawn_delay()
            {
                if !lifecycle.circuit_breaker_tripped() {
                    info!(worker_id = lifecycle.worker_id, "respawning worker");
                    #[cfg(unix)]
                    match lifecycle.respawn() {
                        Ok(()) => {
                            shm.set_worker_healthy(lifecycle.worker_id, true);
                        }
                        Err(e) => {
                            error!(worker_id = lifecycle.worker_id, error = %e, "failed to respawn worker");
                        }
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

        startup_failure_detected
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
            shm.set_worker_healthy(worker_id, false);
            shm.sweep_worker(worker_id as u32);
        }
    }

    fn process_worker_exit(
        lifecycle: &mut WorkerLifecycle,
        shm: &Arc<ShmDispatcher>,
        broker_table: &Arc<vertebra_core::broker::ShmLeaseTable>,
    ) -> bool {
        let exit_code = lifecycle.last_exit_code();
        if exit_code == Some(EXIT_STARTUP_FAILURE) {
            error!(
                worker_id = lifecycle.worker_id,
                pid = lifecycle.last_pid,
                exit_code = EXIT_STARTUP_FAILURE,
                "worker exited with permanent startup failure; aborting respawn"
            );
        } else {
            warn!(
                worker_id = lifecycle.worker_id,
                pid = lifecycle.last_pid,
                exit_code = ?exit_code,
                "worker exited unexpectedly"
            );
        }

        shm.set_worker_healthy(lifecycle.worker_id, false);

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

        if exit_code == Some(EXIT_STARTUP_FAILURE) {
            lifecycle.state = WorkerHealth::Dead;
            return true;
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

        false
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

        let aborted = Supervisor::poll_workers(&factory, &shm).await;
        assert!(!aborted);

        let stats = factory.lock().unwrap().broker_table().stats();
        assert_eq!(stats.active_leases, 0);
        assert_eq!(stats.total_reclaimed, 1);
    }

    #[tokio::test]
    async fn supervisor_treats_exit_78_as_permanent_dead_and_exit_1_as_respawn() {
        let mut factory =
            RingFactory::with_broker_config(2, RingPairConfig::default(), 2, 0).expect("factory");

        let mut startup_fail_child = Command::new("sh")
            .args(["-c", "exit 78"])
            .spawn()
            .expect("spawn exit 78");
        let _ = startup_fail_child.wait();
        let lc_fail = WorkerLifecycle::new(
            startup_fail_child,
            factory.ring_pair(0),
            0,
            "app:app".into(),
            "python".into(),
        );
        factory.add_lifecycle(lc_fail);

        let mut transient_child = Command::new("sh")
            .args(["-c", "exit 1"])
            .spawn()
            .expect("spawn exit 1");
        let _ = transient_child.wait();
        let lc_transient = WorkerLifecycle::new(
            transient_child,
            factory.ring_pair(1),
            1,
            "app:app".into(),
            "python".into(),
        );
        factory.add_lifecycle(lc_transient);

        let shm = ShmDispatcher::new(factory.all_ring_pairs().to_vec(), 64);
        let factory = Arc::new(Mutex::new(factory));

        let aborted = Supervisor::poll_workers(&factory, &shm).await;
        assert!(aborted);

        let mut guard = factory.lock().unwrap();
        let fail_lc = guard.lifecycle_mut(0).expect("worker 0");
        assert_eq!(fail_lc.last_exit_code(), Some(EXIT_STARTUP_FAILURE));
        assert_eq!(fail_lc.state, WorkerHealth::Dead);
        assert_eq!(fail_lc.crash_count, 0);

        let trans_lc = guard.lifecycle_mut(1).expect("worker 1");
        assert_eq!(trans_lc.last_exit_code(), Some(1));
        assert_eq!(trans_lc.state, WorkerHealth::Respawning);
        assert_eq!(trans_lc.crash_count, 1);
    }

    #[tokio::test]
    async fn supervisor_spawn_cancels_shutdown_on_startup_failure() {
        let mut factory =
            RingFactory::with_broker_config(1, RingPairConfig::default(), 2, 0).expect("factory");
        let mut child = Command::new("sh")
            .args(["-c", "exit 78"])
            .spawn()
            .expect("spawn exit 78");
        let _ = child.wait();
        let lifecycle = WorkerLifecycle::new(
            child,
            factory.ring_pair(0),
            0,
            "app:app".into(),
            "python".into(),
        );
        factory.add_lifecycle(lifecycle);

        let shm = ShmDispatcher::new(factory.all_ring_pairs().to_vec(), 64);
        let factory = Arc::new(Mutex::new(factory));
        let shutdown = CancellationToken::new();
        let startup_failed = Arc::new(AtomicBool::new(false));

        let handle = Supervisor::spawn(
            Arc::clone(&factory),
            Arc::clone(&shm),
            shutdown.clone(),
            Arc::clone(&startup_failed),
        );

        tokio::time::timeout(Duration::from_secs(2), async {
            let _ = handle.await;
        })
        .await
        .expect("supervisor task should terminate promptly on exit 78");

        assert!(startup_failed.load(Ordering::Acquire));
        assert!(shutdown.is_cancelled());
    }
}
