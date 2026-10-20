//! Vectorized batch object for O(1) GIL-released acquisition.

use crate::request::Request;
use pyo3::prelude::*;
use std::sync::Arc;
use vertebra_core::transport::BatchSnapshot;

/// Vectorized container for a sealed request batch.
///
/// Instead of building a Python list of N proxies (O(N) GIL time),
/// `RequestBatch` wraps the `Arc<BatchSnapshot>` and provides O(1)
/// access. Proxies are created lazily or accessed via vectorized methods.
#[pyclass(name = "RequestBatch")]
pub struct RequestBatch {
    pub(crate) snapshot: Arc<BatchSnapshot>,
}

#[pymethods]
impl RequestBatch {
    /// Total number of requests in this batch.
    pub fn __len__(&self) -> usize {
        self.snapshot.len()
    }

    /// Access a specific request proxy by index.
    pub fn __getitem__(&self, index: usize) -> PyResult<Request> {
        if index >= self.snapshot.len() {
            return Err(pyo3::exceptions::PyIndexError::new_err(
                "batch index out of range",
            ));
        }
        Ok(Request::new(Arc::clone(&self.snapshot), index))
    }

    /// Return an iterator over request proxies.
    pub fn __iter__(slf: PyRef<'_, Self>) -> PyResult<Py<RequestBatchIterator>> {
        let iter = RequestBatchIterator {
            snapshot: Arc::clone(&slf.snapshot),
            index: 0,
        };
        Py::new(slf.py(), iter)
    }
}

/// Iterator over requests in a `RequestBatch`.
#[pyclass(name = "RequestBatchIterator")]
pub struct RequestBatchIterator {
    snapshot: Arc<BatchSnapshot>,
    index: usize,
}

#[pymethods]
impl RequestBatchIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<Request> {
        if self.index < self.snapshot.len() {
            let req = Request::new(Arc::clone(&self.snapshot), self.index);
            self.index += 1;
            Some(req)
        } else {
            None
        }
    }
}
