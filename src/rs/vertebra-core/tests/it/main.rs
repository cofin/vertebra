//! Consolidated integration test binary for `vertebra-core`.

use std::sync::atomic::{AtomicUsize, Ordering};

mod broker;
mod mpmc;
mod ring;
mod shm;
mod shm_streaming;

static SHM_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Generate a unique POSIX shared-memory segment name for concurrent integration tests.
pub(crate) fn unique_shm_name(prefix: &str) -> String {
    let seq = SHM_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{}-{seq}", std::process::id())
}
