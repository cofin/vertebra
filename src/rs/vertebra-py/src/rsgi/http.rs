//! RSGI v1.6 HTTP protocol implementation (`RsgiHttpProtocol`, `RsgiTransport`).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::file_io::read_file_range_sync;
use crate::response::Response;

/// Immediately completed Python awaitable wrapping a precomputed result value.
#[pyclass(name = "_RsgiReadyAwaitable", freelist = 128)]
pub struct RsgiReadyAwaitable {
    value: Option<Py<PyAny>>,
}

impl RsgiReadyAwaitable {
    pub fn none(py: Python<'_>) -> PyResult<Py<Self>> {
        Py::new(py, Self { value: None })
    }

    pub fn with_value(value: Py<PyAny>, py: Python<'_>) -> PyResult<Py<Self>> {
        Py::new(py, Self { value: Some(value) })
    }
}

#[pymethods]
impl RsgiReadyAwaitable {
    fn __await__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let val = self.value.take().unwrap_or_else(|| py.None());
        Err(pyo3::exceptions::PyStopIteration::new_err(val))
    }
}

static RSGI_DONE: pyo3::sync::PyOnceLock<Py<RsgiReadyAwaitable>> = pyo3::sync::PyOnceLock::new();

pub fn get_rsgi_done(py: Python<'_>) -> PyResult<Py<RsgiReadyAwaitable>> {
    RSGI_DONE
        .get_or_try_init(py, || RsgiReadyAwaitable::none(py))
        .map(|p| p.clone_ref(py))
}

/// Progressive streaming transport returned by `RsgiHttpProtocol.response_stream(status, headers)`.
#[pyclass(name = "RsgiTransport", frozen, freelist = 128)]
#[derive(Clone)]
pub struct RsgiTransport {
    chunks: Arc<Mutex<Vec<u8>>>,
}

impl RsgiTransport {
    pub fn new(chunks: Arc<Mutex<Vec<u8>>>) -> Self {
        Self { chunks }
    }
}

#[pymethods]
impl RsgiTransport {
    pub fn send_bytes(&self, py: Python<'_>, data: &[u8]) -> PyResult<Py<RsgiReadyAwaitable>> {
        let mut guard = self
            .chunks
            .lock()
            .map_err(|_| PyRuntimeError::new_err("stream chunks lock poisoned"))?;
        guard.extend_from_slice(data);
        get_rsgi_done(py)
    }

    pub fn send_str(&self, py: Python<'_>, data: &str) -> PyResult<Py<RsgiReadyAwaitable>> {
        let mut guard = self
            .chunks
            .lock()
            .map_err(|_| PyRuntimeError::new_err("stream chunks lock poisoned"))?;
        guard.extend_from_slice(data.as_bytes());
        get_rsgi_done(py)
    }
}

#[derive(Default)]
struct HttpProtocolState {
    status: u16,
    headers: Vec<(Vec<u8>, Vec<u8>)>,
    body: Vec<u8>,
    file_path: Option<String>,
    file_range: Option<(u64, u64)>,
    streaming: bool,
}

/// RSGI v1.6 HTTP protocol handler supporting single-call unary, file, file-range, and streaming responses.
#[pyclass(name = "RsgiHttpProtocol", frozen, freelist = 128)]
pub struct RsgiHttpProtocol {
    request_body: Bytes,
    request_id: u64,
    state: Mutex<HttpProtocolState>,
    stream_chunks: Arc<Mutex<Vec<u8>>>,
    disconnect_future: Mutex<Option<Py<PyAny>>>,
    disconnected: AtomicBool,
}

impl RsgiHttpProtocol {
    pub fn new(request_body: Bytes, request_id: u64) -> Self {
        Self {
            request_body,
            request_id,
            state: Mutex::new(HttpProtocolState {
                status: 200,
                ..HttpProtocolState::default()
            }),
            stream_chunks: Arc::new(Mutex::new(Vec::new())),
            disconnect_future: Mutex::new(None),
            disconnected: AtomicBool::new(false),
        }
    }

    fn encode_headers(headers: Vec<(String, String)>) -> Vec<(Vec<u8>, Vec<u8>)> {
        headers
            .into_iter()
            .map(|(k, v)| (k.into_bytes(), v.into_bytes()))
            .collect()
    }
}

#[pymethods]
impl RsgiHttpProtocol {
    #[new]
    #[pyo3(signature = (body=None, request_id=0))]
    pub fn py_new(body: Option<&[u8]>, request_id: u64) -> Self {
        Self::new(Bytes::copy_from_slice(body.unwrap_or(b"")), request_id)
    }

    /// Await the complete request body (`raw = await protocol()`).
    pub fn __call__(&self, py: Python<'_>) -> PyResult<Py<RsgiReadyAwaitable>> {
        let py_bytes = PyBytes::new(py, &self.request_body).into_any().unbind();
        RsgiReadyAwaitable::with_value(py_bytes, py)
    }

    /// Await client disconnect notification.
    pub fn client_disconnect(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.disconnected.load(Ordering::Acquire) {
            let awaitable = get_rsgi_done(py)?;
            return Ok(awaitable.into_any());
        }

        let mut guard = self
            .disconnect_future
            .lock()
            .map_err(|_| PyRuntimeError::new_err("disconnect lock poisoned"))?;
        if let Some(ref fut) = *guard {
            return Ok(fut.clone_ref(py));
        }

        let asyncio = py.import("asyncio")?;
        let loop_obj = asyncio.call_method0("get_running_loop")?;
        let fut = loop_obj.call_method0("create_future")?;
        let fut_py = fut.unbind();
        *guard = Some(fut_py.clone_ref(py));
        Ok(fut_py)
    }

    /// Signal client disconnect to resolve `client_disconnect()` awaitables.
    pub fn notify_disconnect(&self, py: Python<'_>) -> PyResult<()> {
        self.disconnected.store(true, Ordering::Release);
        let mut guard = self
            .disconnect_future
            .lock()
            .map_err(|_| PyRuntimeError::new_err("disconnect lock poisoned"))?;
        if let Some(fut) = guard.take() {
            let _ = fut.bind(py).call_method1("set_result", (py.None(),));
        }
        Ok(())
    }

    /// Emit an empty-body HTTP response in a single FFI call.
    pub fn response_empty(&self, status: u16, headers: Vec<(String, String)>) -> PyResult<()> {
        let mut st = self
            .state
            .lock()
            .map_err(|_| PyRuntimeError::new_err("protocol state lock poisoned"))?;
        st.status = status;
        st.headers = Self::encode_headers(headers);
        st.body.clear();
        Ok(())
    }

    /// Emit a UTF-8 string HTTP response in a single FFI call.
    pub fn response_str(
        &self,
        status: u16,
        headers: Vec<(String, String)>,
        body: &str,
    ) -> PyResult<()> {
        let mut st = self
            .state
            .lock()
            .map_err(|_| PyRuntimeError::new_err("protocol state lock poisoned"))?;
        st.status = status;
        st.headers = Self::encode_headers(headers);
        st.body = body.as_bytes().to_vec();
        Ok(())
    }

    /// Emit a binary HTTP response in a single FFI call.
    pub fn response_bytes(
        &self,
        status: u16,
        headers: Vec<(String, String)>,
        body: &[u8],
    ) -> PyResult<()> {
        let mut st = self
            .state
            .lock()
            .map_err(|_| PyRuntimeError::new_err("protocol state lock poisoned"))?;
        st.status = status;
        st.headers = Self::encode_headers(headers);
        st.body = body.to_vec();
        Ok(())
    }

    /// Emit a file response with explicit file reading inside `py.detach`.
    pub fn response_file(
        &self,
        py: Python<'_>,
        status: u16,
        headers: Vec<(String, String)>,
        file: String,
    ) -> PyResult<()> {
        let path_owned = file.clone();
        let body_bytes = py
            .detach(|| read_file_range_sync(&path_owned, None, None))
            .map_err(|e| PyIOError::new_err(format!("failed to read file {file}: {e}")))?;

        let mut st = self
            .state
            .lock()
            .map_err(|_| PyRuntimeError::new_err("protocol state lock poisoned"))?;
        st.status = status;
        let mut encoded = Self::encode_headers(headers);
        if !encoded
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case(b"content-length"))
        {
            encoded.push((
                b"content-length".to_vec(),
                body_bytes.len().to_string().into_bytes(),
            ));
        }
        st.headers = encoded;
        st.body = body_bytes.to_vec();
        st.file_path = Some(file);
        st.file_range = None;
        Ok(())
    }

    /// RSGI v1.6 partial-file response for `[start, end)` byte ranges.
    pub fn response_file_range(
        &self,
        py: Python<'_>,
        status: u16,
        headers: Vec<(String, String)>,
        file: String,
        start: u64,
        end: u64,
    ) -> PyResult<()> {
        if start >= end {
            return Err(PyValueError::new_err(
                "Invalid file range: start must be less than end",
            ));
        }
        let path_owned = file.clone();
        let body_bytes = py
            .detach(|| read_file_range_sync(&path_owned, Some(start), Some(end)))
            .map_err(|e| PyIOError::new_err(format!("failed to read range from {file}: {e}")))?;

        let mut st = self
            .state
            .lock()
            .map_err(|_| PyRuntimeError::new_err("protocol state lock poisoned"))?;
        st.status = status;
        let mut encoded = Self::encode_headers(headers);
        if !encoded
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case(b"content-length"))
        {
            encoded.push((
                b"content-length".to_vec(),
                body_bytes.len().to_string().into_bytes(),
            ));
        }
        st.headers = encoded;
        st.body = body_bytes.to_vec();
        st.file_path = Some(file);
        st.file_range = Some((start, end));
        Ok(())
    }

    /// Start a streaming response and return an `RsgiTransport`.
    pub fn response_stream(
        &self,
        status: u16,
        headers: Vec<(String, String)>,
    ) -> PyResult<RsgiTransport> {
        let mut st = self
            .state
            .lock()
            .map_err(|_| PyRuntimeError::new_err("protocol state lock poisoned"))?;
        st.status = status;
        st.headers = Self::encode_headers(headers);
        st.streaming = true;
        Ok(RsgiTransport::new(Arc::clone(&self.stream_chunks)))
    }

    #[getter]
    pub fn file_path(&self) -> Option<String> {
        self.state.lock().ok().and_then(|st| st.file_path.clone())
    }

    #[getter]
    pub fn file_range(&self) -> Option<(u64, u64)> {
        self.state.lock().ok().and_then(|st| st.file_range)
    }

    /// Materialize the final `Response` object after `__rsgi__` completes.
    pub fn take_response(&self) -> PyResult<Response> {
        let mut st = self
            .state
            .lock()
            .map_err(|_| PyRuntimeError::new_err("protocol state lock poisoned"))?;
        let body_bytes = if st.streaming {
            let mut chunks = self
                .stream_chunks
                .lock()
                .map_err(|_| PyRuntimeError::new_err("stream chunks lock poisoned"))?;
            Bytes::from(std::mem::take(&mut *chunks))
        } else {
            Bytes::from(std::mem::take(&mut st.body))
        };
        Ok(Response {
            status: st.status,
            body: body_bytes,
            headers: std::mem::take(&mut st.headers),
            request_id: self.request_id,
        })
    }
}
