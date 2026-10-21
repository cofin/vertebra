//! Zero-object request proxy for the Vertebra batch runner.
//!
//! Each `Request` holds an `Arc<BatchSnapshot>` and an index into it.
//! Fields are materialized as Python objects on demand from the snapshot's
//! contiguous data slab — unused fields are never allocated.

use std::sync::{Arc, OnceLock};

use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyString};

use vertebra_core::transport::{BatchSnapshot, TransportNode};

use crate::asgi_scope::AsgiScope;
use crate::headers::Headers;
use crate::rsgi_protocol::RsgiHttpProtocol;
use crate::rsgi_scope::RsgiScope;
use crate::wsgi_environ::WsgiEnviron;

/// Zero-object request proxy backed by a shared [`BatchSnapshot`].
///
/// The snapshot's data slab is a single contiguous allocation shared across
/// all requests in the batch. Each getter reads bytes from the slab and
/// creates a Python object only when accessed.
///
/// Headers are cached on first access via `OnceLock` to avoid repeated
/// `Bytes::copy_from_slice` calls — WSGI environ may hit `.headers.get()`
/// multiple times per request (CONTENT_TYPE, CONTENT_LENGTH, HTTP_* keys).
#[pyclass(name = "Request", frozen, freelist = 128)]
pub struct Request {
    batch: Arc<BatchSnapshot>,
    index: usize,
    cached_headers: OnceLock<Headers>,
}

impl Request {
    /// Create a new request proxy pointing to `index` in `batch`.
    pub fn new(batch: Arc<BatchSnapshot>, index: usize) -> Self {
        Self {
            batch,
            index,
            cached_headers: OnceLock::new(),
        }
    }

    fn get_headers(&self) -> Headers {
        self.cached_headers
            .get_or_init(|| {
                let raw = self.batch.headers_raw(self.index);
                Headers::new(bytes::Bytes::copy_from_slice(raw))
            })
            .clone()
    }
}

#[pymethods]
impl Request {
    /// Construct a single-node `Request` from raw method, path, and body bytes.
    #[staticmethod]
    #[pyo3(signature = (method, path, body=None, request_id=1, handler_id=1))]
    pub fn from_parts(
        py: Python<'_>,
        method: &str,
        path: &str,
        body: Option<&Bound<'_, PyAny>>,
        request_id: u64,
        handler_id: u64,
    ) -> PyResult<Self> {
        let body_bytes = match body {
            Some(b) => crate::response::extract_buffer_bytes(py, b)?,
            None => bytes::Bytes::new(),
        };
        let method_tag = match method {
            "GET" => 0,
            "POST" => 1,
            "PUT" => 2,
            "DELETE" => 3,
            "PATCH" => 4,
            "HEAD" => 5,
            "OPTIONS" => 6,
            "TRACE" => 7,
            "CONNECT" => 8,
            _ => 255,
        };
        let path_bytes = path.as_bytes();
        let mut slab = Vec::with_capacity(path_bytes.len() + body_bytes.len());
        slab.extend_from_slice(path_bytes);
        slab.extend_from_slice(&body_bytes);

        let node = TransportNode {
            method_tag,
            http_version_tag: 1,
            _pad: [0; 2],
            handler_id,
            correlation_id: request_id,
            path_offset: 0,
            path_len: path_bytes.len() as u32,
            query_offset: 0,
            query_len: 0,
            headers_offset: 0,
            headers_len: 0,
            body_offset: path_bytes.len() as u32,
            body_len: body_bytes.len() as u32,
        };
        let snapshot = Arc::new(BatchSnapshot::new(slab, vec![node]));
        Ok(Self::new(snapshot, 0))
    }

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
    fn raw_path<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        let path = self.batch.path(self.index).as_bytes();
        PyBytes::new(py, path)
    }

    #[getter]
    fn query<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyString>> {
        self.batch.query(self.index).map(|q| PyString::new(py, q))
    }

    #[getter]
    fn raw_query<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        let query = self.batch.query(self.index).unwrap_or("");
        PyBytes::new(py, query.as_bytes())
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

    /// Return a zero-copy read-only Python `memoryview` over the request body bytes in the batch slab.
    ///
    /// The returned `memoryview` borrows directly from the active `BatchSnapshot` slab and is
    /// valid for the duration of the request handler turn (while the `BatchSnapshot` remains alive).
    /// Call `bytes(request.body_view)` if the payload must be retained after the handler returns.
    #[getter]
    pub fn body_view(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let slice = self.batch.body(self.index);
        let ptr = if slice.is_empty() {
            std::ptr::NonNull::<u8>::dangling().as_ptr()
        } else {
            slice.as_ptr()
        };
        unsafe {
            let mv = ffi::PyMemoryView_FromMemory(
                ptr as *mut std::os::raw::c_char,
                slice.len() as ffi::Py_ssize_t,
                ffi::PyBUF_READ,
            );
            if mv.is_null() {
                return Err(PyErr::fetch(py));
            }
            Ok(Bound::from_owned_ptr(py, mv).unbind())
        }
    }

    #[getter]
    fn handler_id(&self) -> u64 {
        self.batch.node(self.index).handler_id
    }

    /// Protocol flags for this request's batch.
    #[getter]
    fn flags(&self) -> u32 {
        self.batch.flags()
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
    fn headers(&self) -> Headers {
        self.get_headers()
    }

    /// Return a lazy ASGI scope backed by this request's snapshot slot.
    ///
    /// Middleware that needs a real `dict` can call `scope.copy()`.
    #[pyo3(signature = (root_path=None))]
    fn asgi_scope(&self, root_path: Option<String>) -> AsgiScope {
        AsgiScope::new(
            Arc::clone(&self.batch),
            self.index,
            root_path.unwrap_or_default(),
        )
    }

    /// Return a lazy WSGI environ backed by this request's snapshot slot.
    ///
    /// Middleware that needs a real `dict` can call `environ.copy()`.
    #[pyo3(signature = (server_name=None, server_port=None, url_scheme=None))]
    fn wsgi_environ(
        &self,
        server_name: Option<String>,
        server_port: Option<u16>,
        url_scheme: Option<String>,
    ) -> WsgiEnviron {
        WsgiEnviron::new(
            Arc::clone(&self.batch),
            self.index,
            server_name.unwrap_or_else(|| "localhost".to_string()),
            server_port.unwrap_or(8000),
            url_scheme.unwrap_or_else(|| "http".to_string()),
        )
    }

    /// Return an RSGI v1.6 scope backed by this request's snapshot slot.
    #[pyo3(signature = (scheme="http", server="127.0.0.1:8000", client="127.0.0.1:0"))]
    fn rsgi_scope(
        &self,
        py: Python<'_>,
        scheme: &str,
        server: &str,
        client: &str,
    ) -> PyResult<RsgiScope> {
        RsgiScope::from_batch(py, &self.batch, self.index, "http", scheme, server, client)
    }

    /// Return a native RSGI v1.6 HTTP protocol handler for this request.
    fn rsgi_protocol(&self) -> RsgiHttpProtocol {
        RsgiHttpProtocol::new(
            bytes::Bytes::copy_from_slice(self.batch.body(self.index)),
            self.batch.node(self.index).correlation_id,
        )
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
            "Request(method={:?}, path={:?}, handler_id={}, request_id={})",
            n.method_name(),
            self.batch.path(self.index),
            n.handler_id,
            n.correlation_id,
        )
    }
}
