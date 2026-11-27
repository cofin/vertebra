//! RSGI v1.6 HTTP protocol implementation (`RsgiHttpProtocol`, `RsgiTransport`, `drive_rsgi_batch`).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use pyo3::exceptions::{
    PyConnectionResetError, PyIOError, PyRuntimeError, PyStopIteration, PyValueError,
};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};

use vertebra_core::ipc::{FLAG_ERROR, FLAG_FINAL, FLAG_HEADERS_SENT, FLAG_STREAMING};
use vertebra_core::transport::{BatchSnapshot, decode_headers_raw};

use crate::asgi::{
    BatchTargets, ERROR_500_BODY, ERROR_500_HEADERS_RAW, FlushedProtocolResponse,
    LoopExecutionMode, PreparedRawBody, ProtocolFinalizer, classify_loop_mode,
    collect_batch_targets, encode_py_headers_into, log_app_exception, mark_immortal_if_supported,
    push_compact_header, spawn_stepped_coroutine,
};
use crate::file_io::read_file_range_sync;
use crate::response::Response;
use crate::rsgi::scope::{RsgiScope, RsgiServerMeta};
use crate::transport::{ResponseSubmitter, with_buffer_slice};

/// Immediately completed frozen Python awaitable wrapping a precomputed result value.
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(name = "_RsgiReadyAwaitable", frozen, freelist = 128, immutable_type)
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(name = "_RsgiReadyAwaitable", frozen, immutable_type)
)]
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

    fn __next__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let val = match self.value {
            Some(ref v) => v.clone_ref(py),
            None => py.None(),
        };
        Err(PyStopIteration::new_err(val))
    }
}

static RSGI_DONE: pyo3::sync::PyOnceLock<Py<RsgiReadyAwaitable>> = pyo3::sync::PyOnceLock::new();

#[inline]
pub fn get_rsgi_done(py: Python<'_>) -> PyResult<Py<RsgiReadyAwaitable>> {
    RSGI_DONE
        .get_or_try_init(py, || {
            let done = RsgiReadyAwaitable::none(py)?;
            mark_immortal_if_supported(done.bind(py).as_any());
            Ok(done)
        })
        .map(|p| p.clone_ref(py))
}

struct RsgiStreamInner {
    headers_raw: Vec<u8>,
    stream_started: bool,
    stream_closed: bool,
    disconnect_future: Option<Py<PyAny>>,
}

/// Lazily allocated shared state for streaming RSGI responses (`response_stream`).
pub(crate) struct RsgiStreamState {
    request_id: u64,
    direct_submit: bool,
    status: u16,
    inner: Mutex<RsgiStreamInner>,
    stream_chunks: Option<Arc<Mutex<Vec<u8>>>>,
    disconnected: AtomicBool,
}

impl RsgiStreamState {
    fn notify_disconnect(&self, py: Python<'_>) -> PyResult<()> {
        self.disconnected.store(true, Ordering::Release);
        let fut = {
            let mut guard = self
                .inner
                .lock()
                .map_err(|_| PyRuntimeError::new_err("stream state lock poisoned"))?;
            guard.disconnect_future.take()
        };
        if let Some(fut) = fut {
            let _ = fut
                .bind(py)
                .call_method1(pyo3::intern!(py, "set_result"), (py.None(),));
        }
        Ok(())
    }
}

/// Progressive streaming transport returned by `RsgiHttpProtocol.response_stream(status, headers)`.
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(name = "RsgiTransport", frozen, freelist = 128, immutable_type)
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(name = "RsgiTransport", frozen, immutable_type)
)]
#[derive(Clone)]
pub struct RsgiTransport {
    stream: Arc<RsgiStreamState>,
}

impl RsgiTransport {
    pub fn new(chunks: Arc<Mutex<Vec<u8>>>) -> Self {
        Self {
            stream: Arc::new(RsgiStreamState {
                request_id: 0,
                direct_submit: false,
                status: 200,
                inner: Mutex::new(RsgiStreamInner {
                    headers_raw: Vec::new(),
                    stream_started: false,
                    stream_closed: false,
                    disconnect_future: None,
                }),
                stream_chunks: Some(chunks),
                disconnected: AtomicBool::new(false),
            }),
        }
    }

    pub(crate) fn from_stream(stream: Arc<RsgiStreamState>) -> Self {
        Self { stream }
    }

    fn send_chunk(&self, py: Python<'_>, data: &[u8]) -> PyResult<Py<RsgiReadyAwaitable>> {
        if !self.stream.direct_submit {
            if let Some(ref chunks) = self.stream.stream_chunks {
                let mut guard = chunks
                    .lock()
                    .map_err(|_| PyRuntimeError::new_err("stream chunks lock poisoned"))?;
                guard.extend_from_slice(data);
            }
            return get_rsgi_done(py);
        }

        if self.stream.disconnected.load(Ordering::Acquire) {
            if let Ok(mut st) = self.stream.inner.lock() {
                st.stream_closed = true;
            }
            return Err(PyConnectionResetError::new_err("stream closed by peer"));
        }

        let (flags, headers_raw) = {
            let mut st = self
                .stream
                .inner
                .lock()
                .map_err(|_| PyRuntimeError::new_err("stream state lock poisoned"))?;
            if st.stream_closed {
                return Err(PyConnectionResetError::new_err("stream closed by peer"));
            }
            if !st.stream_started {
                st.stream_started = true;
                (
                    FLAG_STREAMING | FLAG_HEADERS_SENT,
                    std::mem::take(&mut st.headers_raw),
                )
            } else {
                if data.is_empty() {
                    return get_rsgi_done(py);
                }
                (FLAG_STREAMING, Vec::new())
            }
        };

        match ResponseSubmitter::submit_stream_frame_inner(
            py,
            self.stream.request_id,
            self.stream.status,
            flags,
            &headers_raw,
            data,
        ) {
            Ok(()) => get_rsgi_done(py),
            Err(err) => {
                if err.is_instance_of::<PyConnectionResetError>(py) {
                    if let Ok(mut st) = self.stream.inner.lock() {
                        st.stream_closed = true;
                    }
                    let _ = self.stream.notify_disconnect(py);
                }
                Err(err)
            }
        }
    }
}

#[pymethods]
impl RsgiTransport {
    pub fn send_bytes(&self, py: Python<'_>, data: &[u8]) -> PyResult<Py<RsgiReadyAwaitable>> {
        self.send_chunk(py, data)
    }

    pub fn send_str(&self, py: Python<'_>, data: &str) -> PyResult<Py<RsgiReadyAwaitable>> {
        self.send_chunk(py, data.as_bytes())
    }
}

enum RsgiBodySource {
    Snapshot {
        batch: Arc<BatchSnapshot>,
        index: usize,
    },
    Owned(Bytes),
}

impl RsgiBodySource {
    #[inline]
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Snapshot { batch, index } => batch.body(*index),
            Self::Owned(b) => b.as_ref(),
        }
    }
}

#[derive(Default)]
struct HttpProtocolState {
    started: bool,
    submitted: bool,
    immediate_submit: bool,
    has_content_length: bool,
    status: u16,
    headers_raw: Vec<u8>,
    single_body_py: Option<Py<PyBytes>>,
    body: Vec<u8>,
    file_path: Option<String>,
    file_range: Option<(u64, u64)>,
    loop_obj: Option<Py<PyAny>>,
    disconnect_future: Option<Py<PyAny>>,
    stream: Option<Arc<RsgiStreamState>>,
}

/// RSGI v1.6 HTTP protocol handler supporting single-call unary, file, file-range, and streaming responses.
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(name = "RsgiHttpProtocol", frozen, freelist = 128, immutable_type)
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(name = "RsgiHttpProtocol", frozen, immutable_type)
)]
pub struct RsgiHttpProtocol {
    body_source: RsgiBodySource,
    request_id: u64,
    direct_submit: bool,
    disconnected: AtomicBool,
    state: Mutex<HttpProtocolState>,
}

impl RsgiHttpProtocol {
    pub fn new(request_body: Bytes, request_id: u64) -> Self {
        Self {
            body_source: RsgiBodySource::Owned(request_body),
            request_id,
            direct_submit: false,
            disconnected: AtomicBool::new(false),
            state: Mutex::new(HttpProtocolState {
                status: 200,
                ..HttpProtocolState::default()
            }),
        }
    }

    #[inline]
    pub(crate) fn from_batch(
        batch: Arc<BatchSnapshot>,
        index: usize,
        direct_submit: bool,
        immediate_submit: bool,
        loop_obj: Option<Py<PyAny>>,
    ) -> Self {
        let request_id = batch.node(index).correlation_id;
        Self {
            body_source: RsgiBodySource::Snapshot { batch, index },
            request_id,
            direct_submit,
            disconnected: AtomicBool::new(false),
            state: Mutex::new(HttpProtocolState {
                status: 200,
                immediate_submit,
                loop_obj,
                ..HttpProtocolState::default()
            }),
        }
    }

    #[inline]
    fn extract_prepared(request_id: u64, st: &mut HttpProtocolState) -> FlushedProtocolResponse {
        st.submitted = true;
        if let Some(ref stream) = st.stream {
            let buf = stream
                .stream_chunks
                .as_ref()
                .and_then(|chunks| chunks.lock().ok().map(|mut g| std::mem::take(&mut *g)))
                .unwrap_or_default();
            let headers_raw = stream
                .inner
                .lock()
                .map(|mut g| std::mem::take(&mut g.headers_raw))
                .unwrap_or_default();
            let body = if buf.is_empty() {
                PreparedRawBody::Empty
            } else {
                PreparedRawBody::Owned(buf)
            };
            return FlushedProtocolResponse {
                request_id,
                status: stream.status,
                headers_raw,
                body,
            };
        }

        let body = if let Some(py_b) = st.single_body_py.take() {
            PreparedRawBody::PyBytes(py_b)
        } else if !st.body.is_empty() {
            PreparedRawBody::Owned(std::mem::take(&mut st.body))
        } else {
            PreparedRawBody::Empty
        };
        FlushedProtocolResponse {
            request_id,
            status: st.status,
            headers_raw: std::mem::take(&mut st.headers_raw),
            body,
        }
    }

    fn finish_direct_stream(&self, py: Python<'_>, stream: &RsgiStreamState) -> PyResult<()> {
        let finalize_stream = {
            let Ok(mut inner) = stream.inner.lock() else {
                return Ok(());
            };
            if inner.stream_closed {
                None
            } else {
                inner.stream_closed = true;
                if !inner.stream_started {
                    inner.stream_started = true;
                    Some((
                        stream.status,
                        FLAG_STREAMING | FLAG_HEADERS_SENT | FLAG_FINAL,
                        std::mem::take(&mut inner.headers_raw),
                    ))
                } else {
                    Some((stream.status, FLAG_STREAMING | FLAG_FINAL, Vec::new()))
                }
            }
        };
        if let Some((status, flags, headers_raw)) = finalize_stream {
            match ResponseSubmitter::submit_stream_frame_inner(
                py,
                self.request_id,
                status,
                flags,
                &headers_raw,
                b"",
            ) {
                Ok(()) => {}
                Err(err) if err.is_instance_of::<PyConnectionResetError>(py) => {
                    let _ = self.notify_disconnect(py);
                }
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    pub(crate) fn take_completed_for_batch(
        &self,
        py: Python<'_>,
        is_return: bool,
    ) -> PyResult<Option<FlushedProtocolResponse>> {
        let mut direct_stream: Option<Arc<RsgiStreamState>> = None;
        let maybe_unary = {
            let Ok(mut st) = self.state.lock() else {
                return Ok(None);
            };
            if self.direct_submit
                && let Some(ref stream) = st.stream
            {
                if is_return {
                    direct_stream = Some(Arc::clone(stream));
                    st.submitted = true;
                }
                None
            } else if st.submitted {
                None
            } else {
                let is_streaming = st.stream.is_some();
                let ready = if is_return {
                    true
                } else {
                    st.started && !is_streaming
                };
                if !ready {
                    st.immediate_submit = true;
                    None
                } else if !st.started {
                    st.submitted = true;
                    Some(FlushedProtocolResponse {
                        request_id: self.request_id,
                        status: 500,
                        headers_raw: ERROR_500_HEADERS_RAW.to_vec(),
                        body: PreparedRawBody::Owned(ERROR_500_BODY.to_vec()),
                    })
                } else {
                    Some(Self::extract_prepared(self.request_id, &mut st))
                }
            }
        };
        if let Some(stream) = direct_stream {
            self.finish_direct_stream(py, &stream)?;
        }
        Ok(maybe_unary)
    }

    pub(crate) fn take_error_for_batch(&self, py: Python<'_>) -> Option<FlushedProtocolResponse> {
        let mut direct_stream: Option<Arc<RsgiStreamState>> = None;
        let maybe_unary = {
            let mut st = self.state.lock().ok()?;
            if self.direct_submit
                && let Some(ref stream) = st.stream
            {
                direct_stream = Some(Arc::clone(stream));
                None
            } else if st.submitted {
                None
            } else {
                st.submitted = true;
                Some(FlushedProtocolResponse {
                    request_id: self.request_id,
                    status: 500,
                    headers_raw: ERROR_500_HEADERS_RAW.to_vec(),
                    body: PreparedRawBody::Owned(ERROR_500_BODY.to_vec()),
                })
            }
        };
        if let Some(stream) = direct_stream {
            let mut error_stream_status: Option<u16> = None;
            let mut send_500 = false;
            {
                let mut inner = stream.inner.lock().ok()?;
                if inner.stream_started {
                    if !inner.stream_closed {
                        inner.stream_closed = true;
                        error_stream_status = Some(stream.status);
                    }
                } else if !inner.stream_closed && !stream.disconnected.load(Ordering::Acquire) {
                    inner.stream_closed = true;
                    send_500 = true;
                }
            }
            if let Some(status) = error_stream_status {
                let _ = ResponseSubmitter::submit_stream_frame_inner(
                    py,
                    self.request_id,
                    status,
                    FLAG_STREAMING | FLAG_ERROR,
                    b"",
                    b"",
                );
                return None;
            }
            if send_500 {
                return Some(FlushedProtocolResponse {
                    request_id: self.request_id,
                    status: 500,
                    headers_raw: ERROR_500_HEADERS_RAW.to_vec(),
                    body: PreparedRawBody::Owned(ERROR_500_BODY.to_vec()),
                });
            }
            return None;
        }
        maybe_unary
    }

    pub(crate) fn finalize_async_return(&self, py: Python<'_>) -> PyResult<()> {
        let mut direct_stream: Option<Arc<RsgiStreamState>> = None;
        let maybe_resp = {
            let mut st = self
                .state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("protocol state lock poisoned"))?;
            if !self.direct_submit {
                None
            } else if let Some(ref stream) = st.stream {
                direct_stream = Some(Arc::clone(stream));
                st.submitted = true;
                None
            } else if st.submitted {
                None
            } else if !st.started {
                st.submitted = true;
                Some(FlushedProtocolResponse {
                    request_id: self.request_id,
                    status: 500,
                    headers_raw: ERROR_500_HEADERS_RAW.to_vec(),
                    body: PreparedRawBody::Owned(ERROR_500_BODY.to_vec()),
                })
            } else {
                Some(Self::extract_prepared(self.request_id, &mut st))
            }
        };
        if let Some(stream) = direct_stream {
            return self.finish_direct_stream(py, &stream);
        }
        if let Some(flushed) = maybe_resp {
            ResponseSubmitter::submit_raw_inner(
                py,
                flushed.request_id,
                flushed.status,
                &flushed.headers_raw,
                flushed.body.as_slice(py),
            )?;
        }
        Ok(())
    }

    pub(crate) fn finalize_async_error(&self, py: Python<'_>) -> PyResult<()> {
        let mut direct_stream: Option<Arc<RsgiStreamState>> = None;
        let should_send_500 = {
            let mut st = self
                .state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("protocol state lock poisoned"))?;
            if !self.direct_submit {
                false
            } else if let Some(ref stream) = st.stream {
                direct_stream = Some(Arc::clone(stream));
                false
            } else if st.submitted {
                false
            } else {
                st.submitted = true;
                true
            }
        };
        if let Some(stream) = direct_stream {
            let mut error_stream_status: Option<u16> = None;
            let mut send_500 = false;
            {
                let mut inner = stream
                    .inner
                    .lock()
                    .map_err(|_| PyRuntimeError::new_err("stream state lock poisoned"))?;
                if inner.stream_started {
                    if !inner.stream_closed {
                        inner.stream_closed = true;
                        error_stream_status = Some(stream.status);
                    }
                } else if !inner.stream_closed && !stream.disconnected.load(Ordering::Acquire) {
                    inner.stream_closed = true;
                    send_500 = true;
                }
            }
            if let Some(status) = error_stream_status {
                let _ = ResponseSubmitter::submit_stream_frame_inner(
                    py,
                    self.request_id,
                    status,
                    FLAG_STREAMING | FLAG_ERROR,
                    b"",
                    b"",
                );
                return Ok(());
            }
            if send_500 {
                ResponseSubmitter::submit_raw_inner(
                    py,
                    self.request_id,
                    500,
                    ERROR_500_HEADERS_RAW,
                    ERROR_500_BODY,
                )?;
            }
            return Ok(());
        }
        if should_send_500 {
            ResponseSubmitter::submit_raw_inner(
                py,
                self.request_id,
                500,
                ERROR_500_HEADERS_RAW,
                ERROR_500_BODY,
            )?;
        }
        Ok(())
    }
}

#[pymethods]
impl RsgiHttpProtocol {
    #[new]
    #[pyo3(signature = (body=None, request_id=0, *, direct_submit=false))]
    pub fn py_new(
        py: Python<'_>,
        body: Option<&Bound<'_, PyAny>>,
        request_id: u64,
        direct_submit: bool,
    ) -> PyResult<Self> {
        let owned = match body {
            Some(b) if !b.is_none() => crate::response::extract_buffer_bytes(py, b)?,
            _ => Bytes::new(),
        };
        Ok(Self {
            body_source: RsgiBodySource::Owned(owned),
            request_id,
            direct_submit,
            disconnected: AtomicBool::new(false),
            state: Mutex::new(HttpProtocolState {
                status: 200,
                immediate_submit: direct_submit,
                ..HttpProtocolState::default()
            }),
        })
    }

    /// Await the complete request body (`raw = await protocol()`).
    pub fn __call__(&self, py: Python<'_>) -> PyResult<Py<RsgiReadyAwaitable>> {
        let py_bytes = PyBytes::new(py, self.body_source.as_slice())
            .into_any()
            .unbind();
        RsgiReadyAwaitable::with_value(py_bytes, py)
    }

    /// Await client disconnect notification.
    pub fn client_disconnect(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.disconnected.load(Ordering::Acquire) {
            let awaitable = get_rsgi_done(py)?;
            return Ok(awaitable.into_any());
        }

        let (stream, maybe_loop) = {
            let st = self
                .state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("protocol state lock poisoned"))?;
            if st.stream.is_none()
                && let Some(ref fut) = st.disconnect_future
            {
                return Ok(fut.clone_ref(py));
            }
            (
                st.stream.clone(),
                st.loop_obj.as_ref().map(|l| l.clone_ref(py)),
            )
        };

        if let Some(ref stream) = stream {
            if stream.disconnected.load(Ordering::Acquire) {
                let awaitable = get_rsgi_done(py)?;
                return Ok(awaitable.into_any());
            }
            let inner = stream
                .inner
                .lock()
                .map_err(|_| PyRuntimeError::new_err("stream state lock poisoned"))?;
            if let Some(ref fut) = inner.disconnect_future {
                return Ok(fut.clone_ref(py));
            }
        }

        let fut = if let Some(lp) = maybe_loop {
            lp.bind(py)
                .call_method0(pyo3::intern!(py, "create_future"))?
        } else {
            let asyncio = py.import(pyo3::intern!(py, "asyncio"))?;
            let loop_obj = asyncio.call_method0(pyo3::intern!(py, "get_running_loop"))?;
            loop_obj.call_method0(pyo3::intern!(py, "create_future"))?
        };
        let fut_py = fut.unbind();

        if let Some(stream) = stream {
            let mut inner = stream
                .inner
                .lock()
                .map_err(|_| PyRuntimeError::new_err("stream state lock poisoned"))?;
            if let Some(ref existing) = inner.disconnect_future {
                return Ok(existing.clone_ref(py));
            }
            if stream.disconnected.load(Ordering::Acquire) {
                let _ = fut_py
                    .bind(py)
                    .call_method1(pyo3::intern!(py, "set_result"), (py.None(),));
                return Ok(fut_py);
            }
            inner.disconnect_future = Some(fut_py.clone_ref(py));
            return Ok(fut_py);
        }

        let mut st = self
            .state
            .lock()
            .map_err(|_| PyRuntimeError::new_err("protocol state lock poisoned"))?;
        if let Some(ref existing) = st.disconnect_future {
            return Ok(existing.clone_ref(py));
        }
        if self.disconnected.load(Ordering::Acquire) {
            let _ = fut_py
                .bind(py)
                .call_method1(pyo3::intern!(py, "set_result"), (py.None(),));
            return Ok(fut_py);
        }
        st.disconnect_future = Some(fut_py.clone_ref(py));
        Ok(fut_py)
    }

    /// Signal client disconnect to resolve `client_disconnect()` awaitables.
    pub fn notify_disconnect(&self, py: Python<'_>) -> PyResult<()> {
        self.disconnected.store(true, Ordering::Release);
        let (fut, stream) = {
            let mut st = self
                .state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("protocol state lock poisoned"))?;
            (st.disconnect_future.take(), st.stream.clone())
        };
        if let Some(fut) = fut {
            let _ = fut
                .bind(py)
                .call_method1(pyo3::intern!(py, "set_result"), (py.None(),));
        }
        if let Some(stream) = stream {
            stream.notify_disconnect(py)?;
        }
        Ok(())
    }

    /// Emit an empty-body HTTP response in a single FFI call.
    pub fn response_empty(
        &self,
        py: Python<'_>,
        status: u16,
        headers: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let immediate_headers = {
            let mut st = self
                .state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("protocol state lock poisoned"))?;
            st.started = true;
            st.status = status;
            st.headers_raw.clear();
            st.has_content_length = false;
            st.single_body_py = None;
            st.body.clear();
            let mut has_cl = false;
            encode_py_headers_into(py, headers, &mut st.headers_raw, &mut has_cl)?;
            st.has_content_length = has_cl;
            if self.direct_submit && st.immediate_submit && !st.submitted {
                st.submitted = true;
                Some(std::mem::take(&mut st.headers_raw))
            } else {
                None
            }
        };
        if let Some(hdrs) = immediate_headers {
            ResponseSubmitter::submit_raw_inner(py, self.request_id, status, &hdrs, b"")?;
        }
        Ok(())
    }

    /// Emit a UTF-8 string HTTP response in a single FFI call.
    pub fn response_str(
        &self,
        py: Python<'_>,
        status: u16,
        headers: &Bound<'_, PyAny>,
        body: &str,
    ) -> PyResult<()> {
        let immediate_headers = {
            let mut st = self
                .state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("protocol state lock poisoned"))?;
            st.started = true;
            st.status = status;
            st.headers_raw.clear();
            st.has_content_length = false;
            st.single_body_py = None;
            let mut has_cl = false;
            encode_py_headers_into(py, headers, &mut st.headers_raw, &mut has_cl)?;
            st.has_content_length = has_cl;
            if self.direct_submit && st.immediate_submit && !st.submitted {
                st.submitted = true;
                st.body.clear();
                Some(std::mem::take(&mut st.headers_raw))
            } else {
                st.body.clear();
                st.body.extend_from_slice(body.as_bytes());
                None
            }
        };
        if let Some(hdrs) = immediate_headers {
            ResponseSubmitter::submit_raw_inner(
                py,
                self.request_id,
                status,
                &hdrs,
                body.as_bytes(),
            )?;
        }
        Ok(())
    }

    /// Emit a binary HTTP response in a single FFI call.
    pub fn response_bytes(
        &self,
        py: Python<'_>,
        status: u16,
        headers: &Bound<'_, PyAny>,
        body: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let immediate_headers = {
            let mut st = self
                .state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("protocol state lock poisoned"))?;
            st.started = true;
            st.status = status;
            st.headers_raw.clear();
            st.has_content_length = false;
            st.single_body_py = None;
            st.body.clear();
            let mut has_cl = false;
            encode_py_headers_into(py, headers, &mut st.headers_raw, &mut has_cl)?;
            st.has_content_length = has_cl;
            if self.direct_submit && st.immediate_submit && !st.submitted {
                st.submitted = true;
                Some(std::mem::take(&mut st.headers_raw))
            } else {
                if let Ok(py_b) = body.cast::<PyBytes>() {
                    if !py_b.as_bytes().is_empty() {
                        st.single_body_py = Some(py_b.clone().unbind());
                    }
                } else {
                    with_buffer_slice(py, body, |slice| {
                        if !slice.is_empty() {
                            st.body.extend_from_slice(slice);
                        }
                        Ok(())
                    })?;
                }
                None
            }
        };
        if let Some(hdrs) = immediate_headers {
            with_buffer_slice(py, body, |slice| {
                ResponseSubmitter::submit_raw_inner(py, self.request_id, status, &hdrs, slice)
            })?;
        }
        Ok(())
    }

    /// Emit a file response with explicit file reading inside `py.detach`.
    pub fn response_file(
        &self,
        py: Python<'_>,
        status: u16,
        headers: &Bound<'_, PyAny>,
        file: String,
    ) -> PyResult<()> {
        let path_owned = file.clone();
        let body_bytes = py
            .detach(|| read_file_range_sync(&path_owned, None, None))
            .map_err(|e| PyIOError::new_err(format!("failed to read file {file}: {e}")))?;

        let immediate_headers = {
            let mut st = self
                .state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("protocol state lock poisoned"))?;
            st.started = true;
            st.status = status;
            st.headers_raw.clear();
            st.single_body_py = None;
            let mut has_cl = false;
            encode_py_headers_into(py, headers, &mut st.headers_raw, &mut has_cl)?;
            if !has_cl {
                let cl_str = body_bytes.len().to_string();
                push_compact_header(&mut st.headers_raw, b"content-length", cl_str.as_bytes());
                has_cl = true;
            }
            st.has_content_length = has_cl;
            st.file_path = Some(file);
            st.file_range = None;
            if self.direct_submit && st.immediate_submit && !st.submitted {
                st.submitted = true;
                st.body.clear();
                Some(std::mem::take(&mut st.headers_raw))
            } else {
                st.body = body_bytes.to_vec();
                None
            }
        };
        if let Some(hdrs) = immediate_headers {
            ResponseSubmitter::submit_raw_inner(py, self.request_id, status, &hdrs, &body_bytes)?;
        }
        Ok(())
    }

    /// RSGI v1.6 partial-file response for `[start, end)` byte ranges.
    pub fn response_file_range(
        &self,
        py: Python<'_>,
        status: u16,
        headers: &Bound<'_, PyAny>,
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

        let immediate_headers = {
            let mut st = self
                .state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("protocol state lock poisoned"))?;
            st.started = true;
            st.status = status;
            st.headers_raw.clear();
            st.single_body_py = None;
            let mut has_cl = false;
            encode_py_headers_into(py, headers, &mut st.headers_raw, &mut has_cl)?;
            if !has_cl {
                let cl_str = body_bytes.len().to_string();
                push_compact_header(&mut st.headers_raw, b"content-length", cl_str.as_bytes());
                has_cl = true;
            }
            st.has_content_length = has_cl;
            st.file_path = Some(file);
            st.file_range = Some((start, end));
            if self.direct_submit && st.immediate_submit && !st.submitted {
                st.submitted = true;
                st.body.clear();
                Some(std::mem::take(&mut st.headers_raw))
            } else {
                st.body = body_bytes.to_vec();
                None
            }
        };
        if let Some(hdrs) = immediate_headers {
            ResponseSubmitter::submit_raw_inner(py, self.request_id, status, &hdrs, &body_bytes)?;
        }
        Ok(())
    }

    /// Start a streaming response and return an `RsgiTransport`.
    pub fn response_stream(
        &self,
        py: Python<'_>,
        status: u16,
        headers: &Bound<'_, PyAny>,
    ) -> PyResult<RsgiTransport> {
        let mut st = self
            .state
            .lock()
            .map_err(|_| PyRuntimeError::new_err("protocol state lock poisoned"))?;
        st.started = true;
        st.status = status;
        st.headers_raw.clear();
        st.single_body_py = None;
        let mut has_cl = false;
        encode_py_headers_into(py, headers, &mut st.headers_raw, &mut has_cl)?;
        st.has_content_length = has_cl;
        if self.direct_submit {
            st.submitted = true;
        }
        let existing_fut = st.disconnect_future.take();
        let stream = Arc::new(RsgiStreamState {
            request_id: self.request_id,
            direct_submit: self.direct_submit,
            status,
            inner: Mutex::new(RsgiStreamInner {
                headers_raw: std::mem::take(&mut st.headers_raw),
                stream_started: false,
                stream_closed: false,
                disconnect_future: existing_fut,
            }),
            stream_chunks: (!self.direct_submit).then(|| Arc::new(Mutex::new(Vec::new()))),
            disconnected: AtomicBool::new(self.disconnected.load(Ordering::Acquire)),
        });
        st.stream = Some(Arc::clone(&stream));
        Ok(RsgiTransport::from_stream(stream))
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
    pub fn take_response(&self, py: Python<'_>) -> PyResult<Response> {
        let mut st = self
            .state
            .lock()
            .map_err(|_| PyRuntimeError::new_err("protocol state lock poisoned"))?;
        if let Some(ref stream) = st.stream {
            let body_bytes = match stream.stream_chunks {
                Some(ref chunks) => {
                    let mut guard = chunks
                        .lock()
                        .map_err(|_| PyRuntimeError::new_err("stream chunks lock poisoned"))?;
                    Bytes::from(std::mem::take(&mut *guard))
                }
                None => Bytes::new(),
            };
            let mut inner = stream
                .inner
                .lock()
                .map_err(|_| PyRuntimeError::new_err("stream state lock poisoned"))?;
            let headers = decode_headers_raw(&inner.headers_raw);
            inner.headers_raw.clear();
            return Ok(Response {
                status: stream.status,
                body: body_bytes,
                headers,
                request_id: self.request_id,
            });
        }

        let body_bytes = if let Some(py_b) = st.single_body_py.take() {
            Bytes::copy_from_slice(py_b.bind(py).as_bytes())
        } else {
            Bytes::from(std::mem::take(&mut st.body))
        };
        let headers = decode_headers_raw(&st.headers_raw);
        st.headers_raw.clear();
        Ok(Response {
            status: st.status,
            body: body_bytes,
            headers,
            request_id: self.request_id,
        })
    }
}

#[pyclass(name = "_RsgiBatchLoopCallback", frozen, immutable_type)]
struct RsgiBatchLoopCallback {
    app: Py<PyAny>,
    targets: BatchTargets,
    loop_obj: Py<PyAny>,
    root_path: String,
    scheme: String,
    server: String,
    client: String,
}

#[pymethods]
impl RsgiBatchLoopCallback {
    fn __call__(&self, py: Python<'_>) -> PyResult<()> {
        let _ = drive_rsgi_targets_inline(
            py,
            self.app.bind(py),
            &self.targets,
            Some(self.loop_obj.bind(py)),
            false,
            &self.root_path,
            &self.scheme,
            &self.server,
            &self.client,
        )?;
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn drive_rsgi_targets_inline(
    py: Python<'_>,
    app: &Bound<'_, PyAny>,
    targets: &BatchTargets,
    active_loop: Option<&Bound<'_, PyAny>>,
    drive_until_complete: bool,
    root_path: &str,
    scheme: &str,
    server: &str,
    client: &str,
) -> PyResult<usize> {
    if targets.is_empty() {
        return Ok(0);
    }

    let target_callable = match app.getattr(pyo3::intern!(py, "__rsgi__")) {
        Ok(rsgi_fn) => rsgi_fn,
        Err(_) => app.clone(),
    };
    let normalized_root = root_path.trim_end_matches('/');
    let shared_meta = Arc::new(RsgiServerMeta::new(
        "http",
        scheme,
        server,
        client,
        normalized_root,
    ));
    let loop_py_opt = active_loop.map(|l| l.clone().unbind());
    let mut ready_responses: Vec<FlushedProtocolResponse> = Vec::with_capacity(targets.len());
    let mut spawned_tasks: Vec<Bound<'_, PyAny>> = Vec::new();
    let mut completed_inline = 0usize;

    targets.for_each(|snap, idx| {
        let scope = RsgiScope::from_batch_with_meta(snap, idx, Arc::clone(&shared_meta));
        let scope_py = Py::new(py, scope)?;
        let proto = RsgiHttpProtocol::from_batch(
            Arc::clone(snap),
            idx,
            true,
            false,
            loop_py_opt.as_ref().map(|l| l.clone_ref(py)),
        );
        let proto_py = Py::new(py, proto)?;
        let proto_bound = proto_py.bind(py);

        let call_res = target_callable.call1((scope_py.bind(py), proto_bound));
        let coro = match call_res {
            Ok(c) => c,
            Err(err) => {
                log_app_exception(py, "RSGI", &err);
                if let Some(err_resp) = proto_bound.get().take_error_for_batch(py) {
                    ready_responses.push(err_resp);
                }
                completed_inline += 1;
                return Ok(());
            }
        };

        if coro.is_none() {
            if let Some(resp) = proto_bound.get().take_completed_for_batch(py, true)? {
                ready_responses.push(resp);
            }
            completed_inline += 1;
            return Ok(());
        }

        let mut yielded: *mut ffi::PyObject = std::ptr::null_mut();
        // SAFETY: GIL is attached (`py`); `coro.as_ptr()` and `Py_None()` stay borrowed for
        // `PyIter_Send`, which writes a fresh owned `PyObject*` into `yielded` on `PYGEN_NEXT`/`PYGEN_RETURN`.
        let send_res = unsafe { ffi::PyIter_Send(coro.as_ptr(), ffi::Py_None(), &mut yielded) };
        match send_res {
            ffi::PySendResult::PYGEN_RETURN => {
                if !yielded.is_null() {
                    // SAFETY: `PYGEN_RETURN` with non-null `yielded` transfers a fresh owned
                    // `PyObject*`; adopting and dropping it prevents a refcount leak.
                    unsafe {
                        let _ = Bound::from_owned_ptr(py, yielded);
                    }
                }
                if let Some(resp) = proto_bound.get().take_completed_for_batch(py, true)? {
                    ready_responses.push(resp);
                }
                completed_inline += 1;
            }
            ffi::PySendResult::PYGEN_ERROR => {
                let err = PyErr::fetch(py);
                log_app_exception(py, "RSGI", &err);
                if let Some(err_resp) = proto_bound.get().take_error_for_batch(py) {
                    ready_responses.push(err_resp);
                }
                completed_inline += 1;
            }
            ffi::PySendResult::PYGEN_NEXT => {
                // SAFETY: `PYGEN_NEXT` guarantees `yielded` is a non-null freshly owned `PyObject*`
                // produced by `PyIter_Send` while the GIL is attached.
                let first_yielded = unsafe { Bound::from_owned_ptr(py, yielded) }.unbind();
                if let Some(resp) = proto_bound.get().take_completed_for_batch(py, false)? {
                    ready_responses.push(resp);
                }
                let lp = match active_loop {
                    Some(l) => l.clone(),
                    None => py
                        .import(pyo3::intern!(py, "asyncio"))?
                        .call_method0(pyo3::intern!(py, "get_running_loop"))?,
                };
                let task = spawn_stepped_coroutine(
                    py,
                    &lp,
                    coro.unbind(),
                    first_yielded,
                    ProtocolFinalizer::Rsgi(proto_py),
                    "RSGI",
                )?;
                if drive_until_complete {
                    spawned_tasks.push(task);
                }
            }
        }
        Ok(())
    })?;

    if !ready_responses.is_empty() {
        ResponseSubmitter::submit_batch_flushed(py, ready_responses)?;
    }

    if drive_until_complete
        && !spawned_tasks.is_empty()
        && let Some(lp) = active_loop
    {
        let asyncio = py.import(pyo3::intern!(py, "asyncio"))?;
        let gather = asyncio.getattr(pyo3::intern!(py, "gather"))?;
        let args = PyTuple::new(py, &spawned_tasks)?;
        let gathered = gather.call1(args)?;
        lp.call_method1(pyo3::intern!(py, "run_until_complete"), (gathered,))?;
    }

    Ok(completed_inline)
}

/// Drive a batch of RSGI v1.6 HTTP requests using C-API `PyIter_Send` single-step execution.
#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(
    name = "drive_rsgi_batch",
    signature = (
        app,
        batch,
        *,
        loop_obj=None,
        root_path="",
        scheme="http",
        server="127.0.0.1:8000",
        client="127.0.0.1:0",
        indices=None
    )
)]
pub fn drive_rsgi_batch(
    py: Python<'_>,
    app: &Bound<'_, PyAny>,
    batch: &Bound<'_, PyAny>,
    loop_obj: Option<&Bound<'_, PyAny>>,
    root_path: &str,
    scheme: &str,
    server: &str,
    client: &str,
    indices: Option<Vec<usize>>,
) -> PyResult<usize> {
    let targets = collect_batch_targets(batch, indices)?;
    if targets.is_empty() {
        return Ok(0);
    }

    match classify_loop_mode(py, loop_obj)? {
        LoopExecutionMode::Inline {
            active_loop,
            drive_until_complete,
        } => drive_rsgi_targets_inline(
            py,
            app,
            &targets,
            active_loop.as_ref(),
            drive_until_complete,
            root_path,
            scheme,
            server,
            client,
        ),
        LoopExecutionMode::CrossThread(lp) => {
            let cb = Py::new(
                py,
                RsgiBatchLoopCallback {
                    app: app.clone().unbind(),
                    targets,
                    loop_obj: lp.clone().unbind(),
                    root_path: root_path.to_string(),
                    scheme: scheme.to_string(),
                    server: server.to_string(),
                    client: client.to_string(),
                },
            )?;
            lp.call_method1(pyo3::intern!(py, "call_soon_threadsafe"), (cb,))?;
            Ok(0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::types::PyList;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn disconnect_before_first_send_suppresses_spurious_500_on_error_finalization() {
        let guard = crate::transport::lock_test_bridge();
        let bridge = guard.bridge;
        let unary_count = Arc::new(AtomicUsize::new(0));
        let unary_count_sink = Arc::clone(&unary_count);
        bridge.set_response_sink(Arc::new(move |_| {
            unary_count_sink.fetch_add(1, Ordering::SeqCst);
        }));

        Python::attach(|py| {
            let proto = RsgiHttpProtocol {
                body_source: RsgiBodySource::Owned(Bytes::new()),
                request_id: 980,
                direct_submit: true,
                disconnected: AtomicBool::new(false),
                state: Mutex::new(HttpProtocolState {
                    status: 200,
                    ..HttpProtocolState::default()
                }),
            };
            proto.notify_disconnect(py).expect("notify disconnect");
            let empty_hdrs = PyList::empty(py);
            let transport = proto
                .response_stream(py, 200, empty_hdrs.as_any())
                .expect("create response_stream");
            let _ = transport.send_str(py, "after-disconnect");

            assert!(proto.take_error_for_batch(py).is_none());
            proto
                .finalize_async_error(py)
                .expect("finalize_async_error must succeed without submitting 500");
        });

        bridge.clear_response_sink();
        assert_eq!(unary_count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn unary_protocol_defers_stream_allocation_and_buffered_stream_collects_chunks() {
        Python::attach(|py| {
            let unary = RsgiHttpProtocol::new(Bytes::from_static(b"req"), 42);
            assert!(
                unary.state.lock().unwrap().stream.is_none(),
                "unary protocol must not allocate stream state eagerly"
            );
            let empty_hdrs = PyList::empty(py);
            unary
                .response_str(py, 200, empty_hdrs.as_any(), "hello")
                .expect("unary response_str");
            assert!(unary.state.lock().unwrap().stream.is_none());
            let resp = unary.take_response(py).expect("take unary response");
            assert_eq!(resp.status, 200);
            assert_eq!(resp.body.as_ref(), b"hello");

            let streaming = RsgiHttpProtocol::new(Bytes::new(), 43);
            let transport = streaming
                .response_stream(py, 202, empty_hdrs.as_any())
                .expect("response_stream");
            assert!(streaming.state.lock().unwrap().stream.is_some());
            transport.send_str(py, "part1-").expect("chunk 1");
            transport.send_bytes(py, b"part2").expect("chunk 2");
            let stream_resp = streaming.take_response(py).expect("take streamed response");
            assert_eq!(stream_resp.status, 202);
            assert_eq!(stream_resp.body.as_ref(), b"part1-part2");
        });
    }
}
