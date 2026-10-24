//! Zero-object request proxy and batch container for the Vertebra batch runner.

use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyString};

use vertebra_core::transport::{BatchSnapshot, encode_headers_compact};

use crate::asgi::AsgiScope;
use crate::headers::Headers;
use crate::rsgi::http::RsgiHttpProtocol;
use crate::rsgi::scope::RsgiScope;
use crate::synthetic::SyntheticRequest;
use crate::wsgi::environ::WsgiEnviron;

#[inline]
pub(crate) fn intern_http_method<'py>(py: Python<'py>, name: &str) -> Bound<'py, PyString> {
    match name {
        "GET" => pyo3::intern!(py, "GET").clone(),
        "POST" => pyo3::intern!(py, "POST").clone(),
        "PUT" => pyo3::intern!(py, "PUT").clone(),
        "DELETE" => pyo3::intern!(py, "DELETE").clone(),
        "PATCH" => pyo3::intern!(py, "PATCH").clone(),
        "HEAD" => pyo3::intern!(py, "HEAD").clone(),
        "OPTIONS" => pyo3::intern!(py, "OPTIONS").clone(),
        "CONNECT" => pyo3::intern!(py, "CONNECT").clone(),
        "TRACE" => pyo3::intern!(py, "TRACE").clone(),
        _ => PyString::new(py, name),
    }
}

#[inline]
pub(crate) fn intern_http_version<'py>(py: Python<'py>, ver: &str) -> Bound<'py, PyString> {
    match ver {
        "1.1" => pyo3::intern!(py, "1.1").clone(),
        "1.0" => pyo3::intern!(py, "1.0").clone(),
        "2" => pyo3::intern!(py, "2").clone(),
        "2.0" => pyo3::intern!(py, "2.0").clone(),
        "3" => pyo3::intern!(py, "3").clone(),
        "HTTP/1.1" => pyo3::intern!(py, "HTTP/1.1").clone(),
        "HTTP/1.0" => pyo3::intern!(py, "HTTP/1.0").clone(),
        "HTTP/2" => pyo3::intern!(py, "HTTP/2").clone(),
        "HTTP/3" => pyo3::intern!(py, "HTTP/3").clone(),
        _ => PyString::new(py, ver),
    }
}

/// Zero-object request proxy backed by a shared [`BatchSnapshot`].
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(name = "Request", frozen, freelist = 128, immutable_type)
)]
#[cfg_attr(Py_GIL_DISABLED, pyclass(name = "Request", frozen, immutable_type))]
pub struct Request {
    pub(crate) batch: Arc<BatchSnapshot>,
    pub(crate) index: usize,
    cached_headers: OnceLock<Headers>,
    cached_body: OnceLock<Bytes>,
}

impl Request {
    /// Create a new request proxy pointing to `index` in `batch`.
    pub fn new(batch: Arc<BatchSnapshot>, index: usize) -> Self {
        Self {
            batch,
            index,
            cached_headers: OnceLock::new(),
            cached_body: OnceLock::new(),
        }
    }

    pub fn get_headers(&self) -> Headers {
        self.cached_headers
            .get_or_init(|| {
                let raw = self.batch.headers_raw(self.index);
                Headers::new(Bytes::copy_from_slice(raw))
            })
            .clone()
    }

    pub fn get_body_bytes(&self) -> &Bytes {
        self.cached_body
            .get_or_init(|| Bytes::copy_from_slice(self.batch.body(self.index)))
    }
}

#[pymethods]
impl Request {
    /// Construct a single-node `Request` from raw method, path, and body bytes.
    #[staticmethod]
    #[pyo3(signature = (method, path, body=None, request_id=1, handler_id=1, headers=None))]
    pub fn from_parts(
        py: Python<'_>,
        method: &str,
        path: &str,
        body: Option<&Bound<'_, PyAny>>,
        request_id: u64,
        handler_id: u64,
        headers: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let body_bytes = match body {
            Some(b) => crate::response::extract_buffer_bytes(py, b)?,
            None => Bytes::new(),
        };

        let raw_pairs = crate::response::extract_headers_list(headers)?;
        let raw_headers = if raw_pairs.is_empty() {
            Vec::new()
        } else {
            encode_headers_compact(&raw_pairs)
        };

        let snapshot = SyntheticRequest::build(
            method,
            path,
            None,
            &raw_headers,
            &body_bytes,
            request_id,
            handler_id,
            "1.1",
        );

        Ok(Self::new(snapshot, 0))
    }

    #[getter]
    fn method<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        let name = self.batch.node(self.index).method_name();
        intern_http_method(py, name)
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
        let body = self.get_body_bytes();
        if body.is_empty() {
            None
        } else {
            Some(PyBytes::new(py, body))
        }
    }

    /// Return a zero-copy read-only Python `memoryview` anchored to this `Request`.
    #[getter]
    pub fn body_view(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        unsafe {
            let mv = ffi::PyMemoryView_FromObject(slf.as_ptr());
            if mv.is_null() {
                return Err(PyErr::fetch(py));
            }
            Ok(Bound::from_owned_ptr(py, mv).unbind())
        }
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: std::os::raw::c_int,
    ) -> PyResult<()> {
        let py = slf.py();
        let req = slf.get();
        let slice = req.batch.body(req.index);
        let ptr = if slice.is_empty() {
            std::ptr::NonNull::<u8>::dangling().as_ptr()
        } else {
            slice.as_ptr()
        };
        let ret = ffi::PyBuffer_FillInfo(
            view,
            slf.as_ptr(),
            ptr as *mut std::os::raw::c_void,
            slice.len() as ffi::Py_ssize_t,
            1,
            flags,
        );
        if ret == -1 {
            return Err(PyErr::fetch(py));
        }
        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer) {}

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
        intern_http_version(py, version)
    }

    #[getter]
    fn headers(&self) -> Headers {
        self.get_headers()
    }

    /// Return a lazy ASGI scope backed by this request's snapshot slot.
    #[pyo3(signature = (root_path=None))]
    fn asgi_scope(&self, root_path: Option<String>) -> AsgiScope {
        AsgiScope::new(
            Arc::clone(&self.batch),
            self.index,
            root_path.unwrap_or_default(),
        )
    }

    /// Return a lazy WSGI environ backed by this request's snapshot slot.
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
            self.get_body_bytes().clone(),
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

/// Vectorized container for a sealed request batch.
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(name = "RequestBatch", freelist = 128, immutable_type)
)]
#[cfg_attr(Py_GIL_DISABLED, pyclass(name = "RequestBatch", immutable_type))]
pub struct RequestBatch {
    pub(crate) snapshot: Arc<BatchSnapshot>,
}

#[pymethods]
impl RequestBatch {
    pub fn __len__(&self) -> usize {
        self.snapshot.len()
    }

    pub fn __getitem__(&self, index: usize) -> PyResult<Request> {
        if index >= self.snapshot.len() {
            return Err(pyo3::exceptions::PyIndexError::new_err(
                "batch index out of range",
            ));
        }
        Ok(Request::new(Arc::clone(&self.snapshot), index))
    }

    pub fn __iter__(slf: PyRef<'_, Self>) -> PyResult<Py<RequestBatchIterator>> {
        let iter = RequestBatchIterator {
            snapshot: Arc::clone(&slf.snapshot),
            index: 0,
        };
        Py::new(slf.py(), iter)
    }
}

/// Iterator over requests in a `RequestBatch`.
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(name = "RequestBatchIterator", freelist = 128, immutable_type)
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(name = "RequestBatchIterator", immutable_type)
)]
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
