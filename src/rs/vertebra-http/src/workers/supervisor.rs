//! Worker process supervisor for crash detection, request failure, ring reclamation, and respawn.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use super::factory::{RingFactory, WorkerHealth, WorkerLifecycle};
use crate::dispatcher::shm_reader::ResponseReaderRegistry;
use crate::dispatcher::{Dispatcher, ShmDispatcher};

/// Process exit code signaling permanent worker startup or lifespan failure.
pub const EXIT_STARTUP_FAILURE: i32 = 78;

/// Supervisor monitors child worker processes, sweeps in-flight requests on crash,
/// reclaims rings, and manages exponential backoff respawning.
pub struct Supervisor;

impl Supervisor {
    /// Spawn the supervisor background task without reader lifecycle management.
    pub fn spawn(
        factory: Arc<Mutex<RingFactory>>,
        shm: Arc<ShmDispatcher>,
        shutdown: CancellationToken,
        startup_failed: Arc<AtomicBool>,
    ) -> tokio::task::JoinHandle<()> {
        Self::spawn_inner(factory, shm, None, shutdown, startup_failed)
    }

    /// Spawn the supervisor background task with per-worker reader stop/join and respawn hooks.
    pub fn spawn_with_readers(
        factory: Arc<Mutex<RingFactory>>,
        shm: Arc<ShmDispatcher>,
        dispatcher: Arc<Dispatcher>,
        readers: Arc<ResponseReaderRegistry>,
        shutdown: CancellationToken,
        startup_failed: Arc<AtomicBool>,
    ) -> tokio::task::JoinHandle<()> {
        Self::spawn_inner(
            factory,
            shm,
            Some((dispatcher, readers)),
            shutdown,
            startup_failed,
        )
    }

    fn spawn_inner(
        factory: Arc<Mutex<RingFactory>>,
        shm: Arc<ShmDispatcher>,
        reader_ctx: Option<(Arc<Dispatcher>, Arc<ResponseReaderRegistry>)>,
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
                        let ctx = reader_ctx
                            .as_ref()
                            .map(|(dispatcher, readers)| (dispatcher, readers, &shutdown));
                        if Self::poll_workers_with_readers(&factory, &shm, ctx).await {
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
        Self::poll_workers_with_readers(factory, shm, None).await
    }

    /// Poll all managed workers, stopping and joining exited worker readers before ring reclaim
    /// and restarting readers after worker respawn.
    pub async fn poll_workers_with_readers(
        factory: &Arc<Mutex<RingFactory>>,
        shm: &Arc<ShmDispatcher>,
        reader_ctx: Option<(
            &Arc<Dispatcher>,
            &Arc<ResponseReaderRegistry>,
            &CancellationToken,
        )>,
    ) -> bool {
        let exited_workers: Vec<(usize, Option<i32>)> = {
            let mut factory_lock = factory.lock().unwrap();
            let broker_table = Arc::clone(factory_lock.broker_table());
            let lifecycles = factory_lock.lifecycles_mut();
            let mut exited = Vec::new();

            for lifecycle in lifecycles.iter_mut() {
                if lifecycle.state == WorkerHealth::Running && !lifecycle.check_alive() {
                    let exit_code = lifecycle.last_exit_code();
                    Self::prepare_worker_exit(lifecycle, shm, &broker_table, exit_code);
                    exited.push((lifecycle.worker_id, exit_code));
                }
            }

            exited
        };

        if let Some((_, readers, _)) = reader_ctx {
            for (worker_id, _) in &exited_workers {
                readers.stop_and_join_worker(*worker_id).await;
            }
        }

        let mut startup_failure_detected = false;
        let mut respawned_workers = Vec::new();

        {
            let mut factory_lock = factory.lock().unwrap();
            let lifecycles = factory_lock.lifecycles_mut();

            for (worker_id, exit_code) in exited_workers {
                if let Some(lifecycle) = lifecycles.iter_mut().find(|l| l.worker_id == worker_id)
                    && Self::finish_worker_exit(lifecycle, exit_code)
                {
                    startup_failure_detected = true;
                }
            }

            for lifecycle in lifecycles.iter_mut() {
                if lifecycle.state == WorkerHealth::Respawning
                    && let Some(last_crash) = lifecycle.last_crash
                    && last_crash.elapsed() >= lifecycle.respawn_delay()
                {
                    if !lifecycle.circuit_breaker_tripped() {
                        info!(worker_id = lifecycle.worker_id, "respawning worker");
                        #[cfg(unix)]
                        match lifecycle.respawn() {
                            Ok(()) => {
                                respawned_workers
                                    .push((lifecycle.worker_id, Arc::clone(&lifecycle.ring_pair)));
                            }
                            Err(e) => {
                                error!(
                                    worker_id = lifecycle.worker_id,
                                    error = %e,
                                    "failed to respawn worker"
                                );
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
        }

        if let Some((dispatcher, readers, shutdown)) = reader_ctx {
            for (worker_id, ring_pair) in &respawned_workers {
                readers.spawn_worker_reader(
                    *worker_id,
                    Arc::clone(ring_pair),
                    Arc::clone(dispatcher),
                    shutdown.clone(),
                );
            }
        }
        for (worker_id, _) in respawned_workers {
            shm.set_worker_healthy(worker_id, true);
        }

        startup_failure_detected
    }

    /// Handle worker crash for a specific worker by ID without async reader stopping.
    pub fn handle_worker_exit(
        factory: &Arc<Mutex<RingFactory>>,
        shm: &Arc<ShmDispatcher>,
        worker_id: usize,
    ) {
        let mut factory_lock = factory.lock().unwrap();
        let broker_table = Arc::clone(factory_lock.broker_table());
        if let Some(lifecycle) = factory_lock.lifecycle_mut(worker_id) {
            let exit_code = lifecycle.last_exit_code();
            Self::prepare_worker_exit(lifecycle, shm, &broker_table, exit_code);
            Self::finish_worker_exit(lifecycle, exit_code);
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

    /// Handle worker crash for a specific worker by ID, stopping and joining its reader before reclaim.
    pub async fn handle_worker_exit_with_readers(
        factory: &Arc<Mutex<RingFactory>>,
        shm: &Arc<ShmDispatcher>,
        readers: Option<&ResponseReaderRegistry>,
        worker_id: usize,
    ) {
        let exited = {
            let mut factory_lock = factory.lock().unwrap();
            let broker_table = Arc::clone(factory_lock.broker_table());
            if let Some(lifecycle) = factory_lock.lifecycle_mut(worker_id) {
                let exit_code = lifecycle.last_exit_code();
                Self::prepare_worker_exit(lifecycle, shm, &broker_table, exit_code);
                Some(exit_code)
            } else {
                warn!(
                    worker_id,
                    "lifecycle not registered in factory, sweeping SHM directly"
                );
                shm.set_worker_healthy(worker_id, false);
                shm.sweep_worker(worker_id as u32);
                None
            }
        };

        if let Some(readers) = readers {
            readers.stop_and_join_worker(worker_id).await;
        }

        if let Some(exit_code) = exited {
            let mut factory_lock = factory.lock().unwrap();
            if let Some(lifecycle) = factory_lock.lifecycle_mut(worker_id) {
                Self::finish_worker_exit(lifecycle, exit_code);
            }
        }
    }

    fn prepare_worker_exit(
        lifecycle: &WorkerLifecycle,
        shm: &Arc<ShmDispatcher>,
        broker_table: &Arc<vertebra_core::broker::ShmLeaseTable>,
        exit_code: Option<i32>,
    ) {
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
    }

    fn finish_worker_exit(lifecycle: &mut WorkerLifecycle, exit_code: Option<i32>) -> bool {
        // (b) Reclaim ring pair (reset ring indices, clear poison flags) after reader stops
        // SAFETY: Both `check_alive()` confirming child exit and `prepare_worker_exit()` setting
        // `shm.set_worker_healthy(..., false)` precede this call, and async supervisor paths
        // (`poll_workers_with_readers` and `handle_worker_exit_with_readers`) join
        // `stop_and_join_worker` first so no concurrent worker process or response reader
        // accesses the rings during `reclaim()`.
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
        let mut factory = RingFactory::with_broker_config(1, RingPairConfig::default(), 2, 1, 512)
            .expect("factory");
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
        let mut factory = RingFactory::with_broker_config(1, RingPairConfig::default(), 2, 0, 512)
            .expect("factory");
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
        let mut factory = RingFactory::with_broker_config(2, RingPairConfig::default(), 2, 0, 512)
            .expect("factory");

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
        let mut factory = RingFactory::with_broker_config(1, RingPairConfig::default(), 2, 0, 512)
            .expect("factory");
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

    #[tokio::test]
    async fn supervisor_poll_workers_with_readers_stops_reader_before_reclaim() {
        use crate::dispatcher::shm_reader::ResponseReaderRegistry;

        let mut factory = RingFactory::with_broker_config(2, RingPairConfig::default(), 2, 0, 512)
            .expect("factory");
        let mut exited_child = Command::new("sh")
            .args(["-c", "exit 1"])
            .spawn()
            .expect("spawn exit 1");
        let _ = exited_child.wait();
        let lc0 = WorkerLifecycle::new(
            exited_child,
            factory.ring_pair(0),
            0,
            "app:app".into(),
            "python".into(),
        );
        factory.add_lifecycle(lc0);

        let running_child = Command::new("sleep").arg("5").spawn().expect("spawn sleep");
        let lc1 = WorkerLifecycle::new(
            running_child,
            factory.ring_pair(1),
            1,
            "app:app".into(),
            "python".into(),
        );
        factory.add_lifecycle(lc1);

        let ring_pairs = factory.all_ring_pairs().to_vec();
        let shm = ShmDispatcher::new(ring_pairs.clone(), 64);
        let dispatcher = Arc::new(Dispatcher::from_shm(Arc::clone(&shm)));
        let shutdown = CancellationToken::new();
        let readers = ResponseReaderRegistry::spawn_all(
            ring_pairs,
            Arc::clone(&dispatcher),
            shutdown.clone(),
        );
        assert_eq!(readers.active_readers(), 2);

        let factory = Arc::new(Mutex::new(factory));
        let aborted = Supervisor::poll_workers_with_readers(
            &factory,
            &shm,
            Some((&dispatcher, &readers, &shutdown)),
        )
        .await;
        assert!(!aborted);
        assert_eq!(readers.active_readers(), 1);
        assert!(!shm.is_worker_healthy(0));
        assert!(shm.is_worker_healthy(1));

        shutdown.cancel();
        readers.stop_and_join_all().await;
        assert_eq!(readers.active_readers(), 0);

        let mut guard = factory.lock().unwrap();
        if let Some(lc1) = guard.lifecycle_mut(1)
            && let Some(mut child) = lc1.child.take()
        {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
