//! Worker management and process supervision for multi-process SHM and embedded modes.

pub mod factory;
pub mod supervisor;

#[cfg(unix)]
pub use self::factory::EmbeddedSupervisor;
pub use self::factory::{
    RingFactory, RingFactoryError, WorkerHealth, WorkerLifecycle, cleanup_stale_shm,
    compute_embedded_worker_budgets, install_panic_cleanup_hook,
};
pub use self::supervisor::Supervisor;
