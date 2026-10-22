//! Worker management and process supervision for multi-process SHM mode.

pub mod factory;
pub mod supervisor;

pub use self::factory::{
    RingFactory, RingFactoryError, WorkerHealth, WorkerLifecycle, cleanup_stale_shm,
    install_panic_cleanup_hook,
};
pub use self::supervisor::Supervisor;
