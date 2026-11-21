//! RAII guard owning one Python broker lease release callable.

use pyo3::prelude::*;

/// Owns a single Python release callable for a broker lease and invokes it at most once.
///
/// The callable is invoked explicitly via [`LeaseGuard::release`] or automatically when the
/// guard is dropped. Because `Py<PyAny>` is `Send`, the guard may be moved to and dropped on
/// worker threads.
#[derive(Debug)]
pub struct LeaseGuard {
    release: Option<Py<PyAny>>,
}

impl LeaseGuard {
    /// Wrap a zero-argument Python callable that releases a broker lease.
    #[must_use]
    pub fn new(release: Py<PyAny>) -> Self {
        Self {
            release: Some(release),
        }
    }

    /// Invoke the stored release callable at most once, reporting Python exceptions as unraisable.
    pub fn release(&mut self, py: Python<'_>) {
        if let Some(callable) = self.release.take()
            && let Err(err) = callable.call0(py)
        {
            err.write_unraisable(py, Some(callable.bind(py)));
        }
    }

    /// Return `true` while the guard still owns an unreleased callable.
    #[must_use]
    pub fn is_held(&self) -> bool {
        self.release.is_some()
    }
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        if self.release.is_none() {
            return;
        }
        Python::attach(|py| self.release(py));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use pyo3::exceptions::PyRuntimeError;
    use pyo3::prelude::*;
    use pyo3::types::{PyCFunction, PyTuple};

    use super::LeaseGuard;

    fn assert_send<T: Send>() {}

    fn counting_callable(py: Python<'_>, counter: Arc<AtomicUsize>) -> Py<PyAny> {
        PyCFunction::new_closure(py, None, None, move |_args: &Bound<'_, PyTuple>, _| {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok::<(), PyErr>(())
        })
        .expect("counting callable")
        .into_any()
        .unbind()
    }

    #[test]
    fn lease_guard_is_send() {
        assert_send::<LeaseGuard>();
    }

    #[test]
    fn lease_guard_release_once() {
        Python::initialize();
        let calls = Arc::new(AtomicUsize::new(0));
        Python::attach(|py| {
            let mut guard = LeaseGuard::new(counting_callable(py, Arc::clone(&calls)));
            assert!(guard.is_held());
            guard.release(py);
            assert!(!guard.is_held());
            guard.release(py);
            assert!(!guard.is_held());
        });
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn lease_guard_drop_releases() {
        Python::initialize();
        let calls = Arc::new(AtomicUsize::new(0));
        Python::attach(|py| {
            let guard = LeaseGuard::new(counting_callable(py, Arc::clone(&calls)));
            assert!(guard.is_held());
            drop(guard);
        });
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn lease_guard_drop_releases_from_worker_thread() {
        Python::initialize();
        let calls = Arc::new(AtomicUsize::new(0));
        let guard = Python::attach(|py| LeaseGuard::new(counting_callable(py, Arc::clone(&calls))));
        std::thread::spawn(move || {
            drop(guard);
        })
        .join()
        .expect("worker thread join");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn lease_guard_drop_after_release() {
        Python::initialize();
        let calls = Arc::new(AtomicUsize::new(0));
        Python::attach(|py| {
            let mut guard = LeaseGuard::new(counting_callable(py, Arc::clone(&calls)));
            guard.release(py);
            assert!(!guard.is_held());
            drop(guard);
        });
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn lease_guard_error_in_callable_does_not_panic() {
        Python::initialize();
        Python::attach(|py| {
            let failing =
                PyCFunction::new_closure(py, None, None, |_args: &Bound<'_, PyTuple>, _| {
                    Err::<(), PyErr>(PyRuntimeError::new_err("broker release failed"))
                })
                .expect("failing callable")
                .into_any()
                .unbind();
            let mut guard = LeaseGuard::new(failing);
            assert!(guard.is_held());
            guard.release(py);
            assert!(!guard.is_held());
        });
    }
}
