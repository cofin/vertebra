//! Named shared-memory cleanup registry.
//!
//! Tracks all named SHM regions created in this process so that a crash handler
//! can call `cleanup_all()` to prevent `/dev/shm` leakage. Each entry is added
//! by `ShmRegion::create_named` via `register_name` and removed on `Drop` via
//! `deregister_name`.
//!
//! # Crash Handler Usage
//!
//! ```no_run
//! use vertebra_core::cleanup;
//!
//! // In signal handler (SIGTERM / SIGINT / panic hook):
//! cleanup::cleanup_all();
//! ```
//!
//! # Note
//! This module is Unix-only. On Windows, named sections are automatically
//! reclaimed by the kernel when the last handle is closed.

use std::sync::Mutex;

#[cfg(unix)]
use crate::shm::shm_unlink_named;

static REGISTRY: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Register a named SHM region for cleanup tracking.
///
/// Called by `ShmRegion::create_named` on Unix.
pub fn register_name(name: &str) {
    if let Ok(mut guard) = REGISTRY.lock() {
        guard.push(name.to_owned());
    }
}

/// Deregister a named SHM region (called from `ShmRegion::Drop`).
///
/// This removes the name from the registry. The actual unlink is still
/// performed by `ShmRegion::Drop`; this just prevents `cleanup_all` from
/// attempting a duplicate unlink.
pub fn deregister_name(name: &str) {
    if let Ok(mut guard) = REGISTRY.lock() {
        guard.retain(|n| n != name);
    }
}

/// List all currently registered names (for diagnostics / tests).
pub fn registered_names() -> Vec<String> {
    REGISTRY.lock().map(|g| g.clone()).unwrap_or_default()
}

/// Unlink all registered named SHM regions.
///
/// Intended for crash handlers and test teardown. Safe to call multiple times;
/// subsequent calls are no-ops for already-unlinked regions (ENOENT is ignored).
#[cfg(unix)]
pub fn cleanup_all() {
    let names = {
        let mut guard = match REGISTRY.lock() {
            Ok(g) => g,
            Err(_) => return, // Poisoned — best effort
        };
        std::mem::take(&mut *guard)
    };
    for name in names {
        let _ = shm_unlink_named(&name);
    }
}

/// No-op on non-Unix platforms (kernel handles cleanup automatically).
#[cfg(not(unix))]
pub fn cleanup_all() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_deregister_roundtrip() {
        let name = format!("/vertebra-cleanup-roundtrip-{}", std::process::id());
        register_name(&name);
        assert!(registered_names().contains(&name));
        deregister_name(&name);
        assert!(!registered_names().contains(&name));
    }

    #[test]
    fn cleanup_all_clears_registry() {
        let a = format!("/vertebra-cleanup-a-{}", std::process::id());
        let b = format!("/vertebra-cleanup-b-{}", std::process::id());
        register_name(&a);
        register_name(&b);
        assert!(registered_names().contains(&a));
        assert!(registered_names().contains(&b));
        cleanup_all(); // Will ENOENT on unlink — that's fine
        assert!(!registered_names().contains(&a));
        assert!(!registered_names().contains(&b));
    }

    #[test]
    fn deregister_nonexistent_is_noop() {
        deregister_name("/vertebra-does-not-exist");
        // No panic — just succeeds
    }
}
