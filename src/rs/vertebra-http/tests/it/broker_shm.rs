use std::sync::Mutex;

use vertebra_core::transport::ring_pair::RingPairConfig;
use vertebra_http::config::ServerConfig;
use vertebra_http::workers::{RingFactory, WorkerLifecycle};

static ENV_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn test_server_config_broker_env_parsing() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    struct EnvReset;
    impl Drop for EnvReset {
        fn drop(&mut self) {
            // SAFETY: serialized under `ENV_LOCK`; no other test in this integration binary
            // concurrently reads or mutates `VERTEBRA_BROKER_*` environment variables.
            unsafe {
                std::env::remove_var("VERTEBRA_BROKER_POOL_SIZE");
                std::env::remove_var("VERTEBRA_BROKER_MAX_OVERFLOW");
                std::env::remove_var("VERTEBRA_BROKER_STMT_CAPACITY");
            }
        }
    }
    let _reset = EnvReset;

    // SAFETY: serialized under `ENV_LOCK`; no other test in this integration binary
    // concurrently reads or mutates `VERTEBRA_BROKER_*` environment variables.
    unsafe {
        std::env::set_var("VERTEBRA_BROKER_POOL_SIZE", "32");
        std::env::set_var("VERTEBRA_BROKER_MAX_OVERFLOW", "16");
        std::env::set_var("VERTEBRA_BROKER_STMT_CAPACITY", "1024");
    }

    let config = ServerConfig::from_env();
    assert_eq!(config.workers.broker_pool_size, 32);
    assert_eq!(config.workers.broker_max_overflow, 16);
    assert_eq!(config.workers.broker_stmt_capacity, 1024);
}

#[test]
fn test_ring_factory_broker_initialization_and_lifecycle_reclamation() {
    let factory = RingFactory::with_broker_config(1, RingPairConfig::default(), 10, 5, 1024)
        .expect("failed to create RingFactory with broker");

    let table = factory.broker_table();
    assert_eq!(table.stats().active_leases, 0);
    assert_eq!(table.pool_size(), 10);
    assert_eq!(table.max_overflow(), 5);
    assert_eq!(table.stmt_capacity(), 1024);

    let child = std::process::Command::new("sleep")
        .arg("10")
        .spawn()
        .expect("failed to spawn sleep child");
    let child_pid = child.id();

    let mut lifecycle = WorkerLifecycle::new(
        child,
        factory.ring_pair(0),
        0,
        "app:app".into(),
        "python".into(),
    );
    assert_eq!(lifecycle.last_pid, child_pid);

    let g0 = table.try_acquire(child_pid, 60_000_000_000, 0).unwrap();
    let g1 = table.try_acquire(child_pid, 60_000_000_000, 0).unwrap();
    let g2 = table.try_acquire(child_pid, 60_000_000_000, 0).unwrap();
    assert_eq!(table.stats().active_leases, 3);

    let _ = lifecycle.child.as_mut().unwrap().kill();
    let _ = lifecycle.child.as_mut().unwrap().wait();

    let (alive, reclaimed) = lifecycle.check_alive_and_reclaim(table);
    assert!(!alive);
    assert_eq!(reclaimed, 3);
    assert_eq!(table.stats().active_leases, 0);

    assert_eq!(
        table.release(g0.slot_index, g0.generation),
        Err(vertebra_core::broker::BrokerError::GenerationMismatch)
    );
    assert_eq!(
        table.release(g1.slot_index, g1.generation),
        Err(vertebra_core::broker::BrokerError::GenerationMismatch)
    );
    assert_eq!(
        table.release(g2.slot_index, g2.generation),
        Err(vertebra_core::broker::BrokerError::GenerationMismatch)
    );
}
