//! Vectorized batch object for O(1) GIL-released acquisition.

use crate::request::VertebraRequest;
use pyo3::prelude::*;
use std::sync::Arc;
use vertebra_core::transport::BatchSnapshot;

/// Vectorized container for a sealed request batch.
///
/// Instead of building a Python list of N proxies (O(N) GIL time),
/// `VertebraBatch` wraps the `Arc<BatchSnapshot>` and provides O(1)
/// access. Proxies are created lazily or accessed via vectorized methods.
#[pyclass]
pub struct VertebraBatch {
    pub(crate) snapshot: Arc<BatchSnapshot>,
}

#[pymethods]
impl VertebraBatch {
    /// Total number of requests in this batch.
    pub fn __len__(&self) -> usize {
        self.snapshot.len()
    }

    /// Access a specific request proxy by index.
    pub fn __getitem__(&self, index: usize) -> PyResult<VertebraRequest> {
        if index >= self.snapshot.len() {
            return Err(pyo3::exceptions::PyIndexError::new_err(
                "batch index out of range",
            ));
        }
        Ok(VertebraRequest::new(Arc::clone(&self.snapshot), index))
    }

    /// Return an iterator over request proxies.
    pub fn __iter__(slf: PyRef<'_, Self>) -> PyResult<Py<VertebraBatchIter>> {
        let iter = VertebraBatchIter {
            snapshot: Arc::clone(&slf.snapshot),
            index: 0,
        };
        Py::new(slf.py(), iter)
    }
}

/// Iterator over requests in a `VertebraBatch`.
#[pyclass]
pub struct VertebraBatchIter {
    snapshot: Arc<BatchSnapshot>,
    index: usize,
}

#[pymethods]
impl VertebraBatchIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<VertebraRequest> {
        if self.index < self.snapshot.len() {
            let req = VertebraRequest::new(Arc::clone(&self.snapshot), self.index);
            self.index += 1;
            Some(req)
        } else {
            None
        }
    }
}
