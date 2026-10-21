//! RSGI v1.6 HTTP and WebSocket protocol implementations (`RsgiHttpProtocol`, `RsgiTransport`, `RsgiWebsocketProtocol`).

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyString};

use crate::response::Response;

/// Immediately completed Python awaitable wrapping a precomputed result value.
#[pyclass(name = "_RsgiReadyAwaitable", freelist = 128)]
pub struct RsgiReadyAwaitable {
    value: Option<Py<PyAny>>,
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
        if let Ok(mut guard) = self.chunks.lock() {
            guard.extend_from_slice(data);
        }
        Py::new(py, RsgiReadyAwaitable { value: None })
    }

    pub fn send_str(&self, py: Python<'_>, data: &str) -> PyResult<Py<RsgiReadyAwaitable>> {
        if let Ok(mut guard) = self.chunks.lock() {
            guard.extend_from_slice(data.as_bytes());
        }
        Py::new(py, RsgiReadyAwaitable { value: None })
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
        Py::new(
            py,
            RsgiReadyAwaitable {
                value: Some(py_bytes),
            },
        )
    }

    /// Await client disconnect notification.
    pub fn client_disconnect(&self, py: Python<'_>) -> PyResult<Py<RsgiReadyAwaitable>> {
        Py::new(py, RsgiReadyAwaitable { value: None })
    }

    /// Emit an empty-body HTTP response in a single FFI call.
    pub fn response_empty(&self, status: u16, headers: Vec<(String, String)>) {
        if let Ok(mut st) = self.state.lock() {
            st.status = status;
            st.headers = Self::encode_headers(headers);
            st.body.clear();
        }
    }

    /// Emit a UTF-8 string HTTP response in a single FFI call.
    pub fn response_str(&self, status: u16, headers: Vec<(String, String)>, body: &str) {
        if let Ok(mut st) = self.state.lock() {
            st.status = status;
            st.headers = Self::encode_headers(headers);
            st.body = body.as_bytes().to_vec();
        }
    }

    /// Emit a binary HTTP response in a single FFI call.
    pub fn response_bytes(&self, status: u16, headers: Vec<(String, String)>, body: &[u8]) {
        if let Ok(mut st) = self.state.lock() {
            st.status = status;
            st.headers = Self::encode_headers(headers);
            st.body = body.to_vec();
        }
    }

    /// Emit a file response and annotate `x-vertebra-sendfile` for Rust zero-copy file streaming.
    pub fn response_file(
        &self,
        py: Python<'_>,
        status: u16,
        headers: Vec<(String, String)>,
        file: String,
    ) -> PyResult<()> {
        let path_owned = file.clone();
        py.detach(|| std::fs::metadata(&path_owned))
            .map_err(|e| PyIOError::new_err(format!("failed to stat file {file}: {e}")))?;
        if let Ok(mut st) = self.state.lock() {
            st.status = status;
            let mut encoded = Self::encode_headers(headers);
            encoded.push((b"x-vertebra-sendfile".to_vec(), file.as_bytes().to_vec()));
            st.headers = encoded;
            st.body.clear();
            st.file_path = Some(file);
            st.file_range = None;
        }
        Ok(())
    }

    /// RSGI v1.6 partial-file response for `[start, end)` byte ranges (`206 Partial Content`).
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
        let (clamped_end, total_len) =
            match py.detach(|| stat_file_range_sync(&path_owned, start, end)) {
                Ok(tuple) => tuple,
                Err(FileRangeError::InvalidRange(range_msg)) => {
                    return Err(PyValueError::new_err(range_msg));
                }
                Err(FileRangeError::Io(io_err)) => {
                    return Err(PyIOError::new_err(format!(
                        "failed to stat range from {file}: {io_err}"
                    )));
                }
            };

        if let Ok(mut st) = self.state.lock() {
            st.status = status;
            let mut encoded = Self::encode_headers(headers);
            encoded.push((b"x-vertebra-sendfile".to_vec(), file.as_bytes().to_vec()));
            encoded.push((
                b"x-vertebra-sendfile-range".to_vec(),
                format!("{start}-{clamped_end}/{total_len}").into_bytes(),
            ));
            st.headers = encoded;
            st.body.clear();
            st.file_path = Some(file);
            st.file_range = Some((start, clamped_end));
        }
        Ok(())
    }

    /// Start a streaming response and return an `RsgiTransport`.
    pub fn response_stream(&self, status: u16, headers: Vec<(String, String)>) -> RsgiTransport {
        if let Ok(mut st) = self.state.lock() {
            st.status = status;
            st.headers = Self::encode_headers(headers);
            st.streaming = true;
        }
        RsgiTransport::new(Arc::clone(&self.stream_chunks))
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
    pub fn take_response(&self) -> Response {
        let mut st = self.state.lock().expect("lock rsgi protocol state");
        let body_bytes = if st.streaming {
            let chunks = self.stream_chunks.lock().expect("lock rsgi stream chunks");
            Bytes::copy_from_slice(&chunks)
        } else {
            Bytes::from(std::mem::take(&mut st.body))
        };
        Response {
            status: st.status,
            body: body_bytes,
            headers: std::mem::take(&mut st.headers),
            request_id: self.request_id,
        }
    }
}

#[derive(Default)]
struct WsProtocolState {
    accepted: bool,
    incoming: Vec<Py<PyAny>>,
    read_pos: usize,
    outbound: Vec<Vec<u8>>,
    close_code: Option<u16>,
}

/// RSGI v1.6 WebSocket protocol handler.
#[pyclass(name = "RsgiWebsocketProtocol", frozen, freelist = 128)]
pub struct RsgiWebsocketProtocol {
    state: Mutex<WsProtocolState>,
}

#[pymethods]
impl RsgiWebsocketProtocol {
    #[new]
    #[pyo3(signature = (incoming=None))]
    pub fn py_new(incoming: Option<&Bound<'_, PyList>>) -> Self {
        let mut items = Vec::new();
        if let Some(list) = incoming {
            for item in list.iter() {
                items.push(item.unbind());
            }
        }
        Self {
            state: Mutex::new(WsProtocolState {
                incoming: items,
                ..WsProtocolState::default()
            }),
        }
    }

    pub fn accept(&self, py: Python<'_>) -> PyResult<Py<RsgiReadyAwaitable>> {
        if let Ok(mut st) = self.state.lock() {
            st.accepted = true;
        }
        let val = true.into_pyobject(py)?.to_owned().into_any().unbind();
        Py::new(py, RsgiReadyAwaitable { value: Some(val) })
    }

    pub fn receive(&self, py: Python<'_>) -> PyResult<Py<RsgiReadyAwaitable>> {
        let next_msg = if let Ok(mut st) = self.state.lock() {
            if st.read_pos < st.incoming.len() {
                let item = st.incoming[st.read_pos].clone_ref(py);
                st.read_pos += 1;
                Some(item)
            } else {
                None
            }
        } else {
            None
        };
        Py::new(py, RsgiReadyAwaitable { value: next_msg })
    }

    pub fn send_bytes(&self, py: Python<'_>, data: &[u8]) -> PyResult<Py<RsgiReadyAwaitable>> {
        if let Ok(mut st) = self.state.lock() {
            st.outbound.push(data.to_vec());
        }
        Py::new(py, RsgiReadyAwaitable { value: None })
    }

    pub fn send_str(&self, py: Python<'_>, data: &str) -> PyResult<Py<RsgiReadyAwaitable>> {
        if let Ok(mut st) = self.state.lock() {
            st.outbound.push(data.as_bytes().to_vec());
        }
        Py::new(py, RsgiReadyAwaitable { value: None })
    }

    #[pyo3(signature = (status=None))]
    pub fn close(&self, status: Option<u16>) {
        if let Ok(mut st) = self.state.lock() {
            st.close_code = Some(status.unwrap_or(1000));
        }
    }

    #[getter]
    pub fn close_code(&self) -> Option<u16> {
        self.state.lock().ok().and_then(|st| st.close_code)
    }

    pub fn sent_messages<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let st = self.state.lock().expect("lock ws state");
        let items: Vec<Bound<'py, PyAny>> = st
            .outbound
            .iter()
            .map(|b| PyBytes::new(py, b).into_any())
            .collect();
        PyList::new(py, &items)
    }
}

enum FileRangeError {
    InvalidRange(&'static str),
    Io(std::io::Error),
}

impl From<std::io::Error> for FileRangeError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

fn stat_file_range_sync(path: &str, start: u64, end: u64) -> Result<(u64, u64), FileRangeError> {
    let metadata = std::fs::metadata(path)?;
    let total_len = metadata.len();
    let clamped_end = end.min(total_len);
    if start >= clamped_end {
        return Err(FileRangeError::InvalidRange(
            "Invalid file range: start exceeds file length",
        ));
    }
    Ok((clamped_end, total_len))
}

#[allow(dead_code)]
fn _assert_py_string<'py>(py: Python<'py>, s: &str) -> Bound<'py, PyString> {
    PyString::new(py, s)
}
