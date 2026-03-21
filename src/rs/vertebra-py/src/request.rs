//! Zero-object request proxy for the Vertebra batch runner.
//!
//! Each `VertebraRequest` holds an `Arc<BatchSnapshot>` and an index into it.
//! Fields are materialized as Python objects on demand from the snapshot's
//! contiguous data slab — unused fields are never allocated.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyString};

use vertebra_core::transport::{decode_headers_raw, BatchSnapshot};

use crate::headers::VertebraHeaders;

/// Zero-object request proxy backed by a shared [`BatchSnapshot`].
///
/// The snapshot's data slab is a single contiguous allocation shared across
/// all requests in the batch. Each getter reads bytes from the slab and
/// creates a Python object only when accessed.
#[pyclass(name = "VertebraRequest", frozen)]
pub struct VertebraRequest {
    batch: Arc<BatchSnapshot>,
    index: usize,
}

impl VertebraRequest {
    /// Create a new request proxy pointing to `index` in `batch`.
    pub fn new(batch: Arc<BatchSnapshot>, index: usize) -> Self {
        Self { batch, index }
    }
}

#[pymethods]
impl VertebraRequest {
    #[getter]
    fn method<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        let name = self.batch.node(self.index).method_name();
        PyString::intern(py, name)
    }

    #[getter]
    fn path<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        let path = self.batch.path(self.index);
        PyString::new(py, path)
    }

    #[getter]
    fn query<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyString>> {
        self.batch.query(self.index).map(|q| PyString::new(py, q))
    }

    #[getter]
    fn body<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        let body = self.batch.body(self.index);
        if body.is_empty() {
            None
        } else {
            Some(PyBytes::new(py, body))
        }
    }

    #[getter]
    fn handler_id(&self) -> u64 {
        self.batch.node(self.index).handler_id
    }

    #[getter]
    fn request_id(&self) -> u64 {
        self.batch.node(self.index).correlation_id
    }

    #[getter]
    fn http_version<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        let version = self.batch.node(self.index).http_version();
        PyString::intern(py, version)
    }

    #[getter]
    fn headers(&self) -> VertebraHeaders {
        let raw = self.batch.headers_raw(self.index);
        let decoded = decode_headers_raw(raw);
        VertebraHeaders::new(decoded)
    }

    fn as_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new(py);
        d.set_item("method", self.method(py))?;
        d.set_item("path", self.path(py))?;
        d.set_item("query", self.query(py))?;
        d.set_item("body", self.body(py))?;
        d.set_item("handler_id", self.handler_id())?;
        d.set_item("request_id", self.request_id())?;
        Ok(d)
    }

    fn __repr__(&self) -> String {
        let n = self.batch.node(self.index);
        format!(
            "VertebraRequest(method={:?}, path={:?}, handler_id={}, request_id={})",
            n.method_name(),
            self.batch.path(self.index),
            n.handler_id,
            n.correlation_id,
        )
    }
}
