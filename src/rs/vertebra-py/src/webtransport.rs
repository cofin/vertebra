//! PyO3 bindings for embedded HTTP/3 WebTransport sessions and bidirectional streams.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use bytes::Bytes;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyConnectionResetError, PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyWeakref, PyWeakrefMethods, PyWeakrefReference};
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc};

use vertebra_core::bridge::{
    BatchBridge, WebTransportBridgeError, WebTransportFuture, WebTransportSessionHandle,
    WebTransportSessionOps, WebTransportStreamHandle, WebTransportStreamOps,
};
use vertebra_core::py_async::spawn_rust_task_with_attr;

use crate::async_bridge::{CORE_COMPLETION_DRAINER_ATTR, RustDbTaskAwaitable};

fn map_wt_err(_py: Python<'_>, err: WebTransportBridgeError) -> PyErr {
    match err {
        WebTransportBridgeError::SessionClosed(id) => {
            PyConnectionResetError::new_err(format!("WebTransport session {id} is closed"))
        }
        WebTransportBridgeError::Stream(msg) => {
            if msg.contains("closed") || msg.contains("finished") || msg.contains("reset") {
                PyConnectionResetError::new_err(msg)
            } else {
                PyRuntimeError::new_err(msg)
            }
        }
        WebTransportBridgeError::Datagram(msg) | WebTransportBridgeError::Protocol(msg) => {
            PyRuntimeError::new_err(msg)
        }
    }
}

fn extract_buffer_bytes(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Bytes> {
    if let Ok(py_bytes) = data.cast::<PyBytes>() {
        return Ok(Bytes::copy_from_slice(py_bytes.as_bytes()));
    }
    let py_buf = PyBuffer::<u8>::get(data)?;
    let len = py_buf.item_count();
    if len == 0 {
        drop(py_buf);
        return Ok(Bytes::new());
    }
    if py_buf.is_c_contiguous() {
        // SAFETY: The GIL is held (`py`), `py_buf` is a valid C-contiguous `PyBuffer<u8>`
        // with `len > 0` whose underlying buffer view pins the memory via its exporter
        // refcount, so `buf_ptr()` is non-null and valid for `len` bytes while `py_buf`
        // remains alive. We copy into owned `Bytes` and drop `py_buf` before detaching.
        let owned = unsafe {
            Bytes::copy_from_slice(std::slice::from_raw_parts(
                py_buf.buf_ptr() as *const u8,
                len,
            ))
        };
        drop(py_buf);
        Ok(owned)
    } else {
        let owned = py_buf.to_vec(py)?;
        drop(py_buf);
        Ok(Bytes::from(owned))
    }
}

/// Bidirectional WebTransport stream exposed to Python `asyncio` handlers.
#[pyclass(
    name = "WebTransportStream",
    module = "vertebra._vertebra",
    frozen,
    immutable_type
)]
pub struct WebTransportStream {
    inner: WebTransportStreamHandle,
}

#[pymethods]
impl WebTransportStream {
    #[getter]
    fn stream_id(&self) -> u64 {
        self.inner.stream_id()
    }

    #[pyo3(signature = (max_bytes=65536))]
    fn recv<'py>(&self, py: Python<'py>, max_bytes: usize) -> PyResult<RustDbTaskAwaitable> {
        let stream = Arc::clone(&self.inner);
        spawn_rust_task_with_attr(
            py,
            async move { stream.recv(max_bytes).await },
            |py, bytes| Ok(PyBytes::new(py, &bytes).into_any().unbind()),
            map_wt_err,
            CORE_COMPLETION_DRAINER_ATTR,
        )
    }

    fn send<'py>(&self, py: Python<'py>, data: &Bound<'_, PyAny>) -> PyResult<RustDbTaskAwaitable> {
        let payload = extract_buffer_bytes(py, data)?;
        let stream = Arc::clone(&self.inner);
        spawn_rust_task_with_attr(
            py,
            async move { stream.send(payload).await },
            |py, ()| Ok(py.None()),
            map_wt_err,
            CORE_COMPLETION_DRAINER_ATTR,
        )
    }

    fn finish<'py>(&self, py: Python<'py>) -> PyResult<RustDbTaskAwaitable> {
        let stream = Arc::clone(&self.inner);
        spawn_rust_task_with_attr(
            py,
            async move { stream.finish().await },
            |py, ()| Ok(py.None()),
            map_wt_err,
            CORE_COMPLETION_DRAINER_ATTR,
        )
    }
}

/// Accepted HTTP/3 WebTransport session exposed to Python `asyncio` handlers.
#[pyclass(
    name = "WebTransportSession",
    module = "vertebra._vertebra",
    frozen,
    immutable_type
)]
pub struct WebTransportSession {
    inner: WebTransportSessionHandle,
}

#[pymethods]
impl WebTransportSession {
    #[getter]
    fn session_id(&self) -> u64 {
        self.inner.session_id()
    }

    #[getter]
    fn path(&self) -> &str {
        self.inner.path()
    }

    #[getter]
    fn headers<'py>(&self, py: Python<'py>) -> PyResult<Py<PyAny>> {
        let pairs: Vec<(Bound<'py, PyBytes>, Bound<'py, PyBytes>)> = self
            .inner
            .headers()
            .iter()
            .map(|(k, v)| (PyBytes::new(py, k), PyBytes::new(py, v)))
            .collect();
        Ok(PyList::new(py, pairs)?.into_any().unbind())
    }

    fn accept_bi<'py>(&self, py: Python<'py>) -> PyResult<RustDbTaskAwaitable> {
        let session = Arc::clone(&self.inner);
        spawn_rust_task_with_attr(
            py,
            async move { session.accept_bi().await },
            |py, stream| {
                let obj = Py::new(py, WebTransportStream { inner: stream })?;
                Ok(obj.into_any())
            },
            map_wt_err,
            CORE_COMPLETION_DRAINER_ATTR,
        )
    }

    fn open_bi<'py>(&self, py: Python<'py>) -> PyResult<RustDbTaskAwaitable> {
        let session = Arc::clone(&self.inner);
        spawn_rust_task_with_attr(
            py,
            async move { session.open_bi().await },
            |py, stream| {
                let obj = Py::new(py, WebTransportStream { inner: stream })?;
                Ok(obj.into_any())
            },
            map_wt_err,
            CORE_COMPLETION_DRAINER_ATTR,
        )
    }

    fn recv_datagram<'py>(&self, py: Python<'py>) -> PyResult<RustDbTaskAwaitable> {
        let session = Arc::clone(&self.inner);
        spawn_rust_task_with_attr(
            py,
            async move { session.recv_datagram().await },
            |py, payload| Ok(PyBytes::new(py, &payload).into_any().unbind()),
            map_wt_err,
            CORE_COMPLETION_DRAINER_ATTR,
        )
    }

    fn send_datagram(&self, py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<()> {
        let payload = extract_buffer_bytes(py, data)?;
        let session = Arc::clone(&self.inner);
        py.detach(move || session.send_datagram(&payload))
            .map_err(|err| map_wt_err(py, err))
    }

    #[pyo3(signature = (error_code=0, reason=""))]
    fn close<'py>(
        &self,
        py: Python<'py>,
        error_code: u32,
        reason: &str,
    ) -> PyResult<RustDbTaskAwaitable> {
        let session = Arc::clone(&self.inner);
        let owned_reason = reason.to_string();
        spawn_rust_task_with_attr(
            py,
            async move { session.close(error_code, owned_reason).await },
            |py, ()| Ok(py.None()),
            map_wt_err,
            CORE_COMPLETION_DRAINER_ATTR,
        )
    }
}

#[pyclass(
    name = "_WebTransportSessionTrampoline",
    module = "vertebra._vertebra",
    frozen,
    immutable_type
)]
struct WebTransportSessionTrampoline {
    handler: Py<PyAny>,
    session: Py<WebTransportSession>,
    weak_loop: Py<PyWeakref>,
}

#[pymethods]
impl WebTransportSessionTrampoline {
    fn __call__(&self, py: Python<'_>) -> PyResult<()> {
        let result = self.handler.bind(py).call1((self.session.bind(py),))?;
        let inspect = py.import("inspect")?;
        let is_awaitable: bool = inspect.call_method1("isawaitable", (&result,))?.extract()?;
        if is_awaitable {
            let loop_open = self.weak_loop.bind(py).upgrade().is_some_and(|loop_bound| {
                !loop_bound
                    .call_method0("is_closed")
                    .and_then(|v| v.extract::<bool>())
                    .unwrap_or(true)
            });
            if loop_open {
                let asyncio = py.import("asyncio")?;
                let _ = asyncio.call_method1("ensure_future", (&result,))?;
            } else if let Ok(close_fn) = result.getattr("close") {
                let _ = close_fn.call0();
            }
        }
        Ok(())
    }
}

/// Register or clear the embedded Python WebTransport session handler on `BatchBridge`.
#[pyfunction]
#[pyo3(name = "register_webtransport_handler", signature = (handler=None, r#loop=None))]
pub fn register_webtransport_handler(
    py: Python<'_>,
    handler: Option<Py<PyAny>>,
    r#loop: Option<Bound<'_, PyAny>>,
) -> PyResult<()> {
    let bridge = BatchBridge::get().ok_or_else(|| {
        PyRuntimeError::new_err("transport not initialized; call init_transport() first")
    })?;
    let Some(handler_cb) = handler else {
        bridge.clear_webtransport_sink();
        return Ok(());
    };
    if !handler_cb.bind(py).is_callable() {
        return Err(PyTypeError::new_err(
            "webtransport handler must be callable",
        ));
    }

    let loop_bound = match r#loop {
        Some(obj) if !obj.is_none() => obj,
        _ => py.import("asyncio")?.call_method0("get_running_loop")?,
    };
    let weak_loop = PyWeakrefReference::new(&loop_bound)?
        .into_any()
        .cast_into::<PyWeakref>()?
        .unbind();

    bridge.set_webtransport_sink(Arc::new(
        move |session_handle: WebTransportSessionHandle| {
            let _ = Python::try_attach(|py| {
                let Some(loop_bound) = weak_loop.bind(py).upgrade() else {
                    return;
                };
                let is_closed = loop_bound
                    .call_method0("is_closed")
                    .and_then(|v| v.extract::<bool>())
                    .unwrap_or(true);
                if is_closed {
                    return;
                }
                let Ok(py_session) = Py::new(
                    py,
                    WebTransportSession {
                        inner: session_handle,
                    },
                ) else {
                    return;
                };
                let Ok(trampoline) = Py::new(
                    py,
                    WebTransportSessionTrampoline {
                        handler: handler_cb.clone_ref(py),
                        session: py_session,
                        weak_loop: weak_loop.clone_ref(py),
                    },
                ) else {
                    return;
                };
                let _ = loop_bound.call_method1("call_soon_threadsafe", (trampoline,));
            });
        },
    ));

    Ok(())
}

struct SyntheticStreamState {
    stream_id: u64,
    inbound: StdMutex<VecDeque<u8>>,
    inbound_eof: AtomicBool,
    inbound_notify: Notify,
    outbound: StdMutex<Vec<u8>>,
    outbound_finished: AtomicBool,
    session_closed: Arc<AtomicBool>,
}

impl SyntheticStreamState {
    fn new(stream_id: u64, session_closed: Arc<AtomicBool>) -> Self {
        Self {
            stream_id,
            inbound: StdMutex::new(VecDeque::new()),
            inbound_eof: AtomicBool::new(false),
            inbound_notify: Notify::new(),
            outbound: StdMutex::new(Vec::new()),
            outbound_finished: AtomicBool::new(false),
            session_closed,
        }
    }
}

impl WebTransportStreamOps for SyntheticStreamState {
    fn stream_id(&self) -> u64 {
        self.stream_id
    }

    fn recv(
        self: Arc<Self>,
        max_bytes: usize,
    ) -> WebTransportFuture<Result<Bytes, WebTransportBridgeError>> {
        Box::pin(async move {
            if max_bytes == 0 {
                return Ok(Bytes::new());
            }
            loop {
                {
                    let mut guard = self.inbound.lock().unwrap_or_else(|e| e.into_inner());
                    if !guard.is_empty() {
                        let count = max_bytes.min(guard.len());
                        let out: Vec<u8> = guard.drain(..count).collect();
                        return Ok(Bytes::from(out));
                    }
                    if self.inbound_eof.load(Ordering::Acquire) {
                        return Ok(Bytes::new());
                    }
                    if self.session_closed.load(Ordering::Acquire) {
                        return Err(WebTransportBridgeError::Stream(
                            "stream closed by peer".to_string(),
                        ));
                    }
                }
                self.inbound_notify.notified().await;
            }
        })
    }

    fn send(
        self: Arc<Self>,
        data: Bytes,
    ) -> WebTransportFuture<Result<(), WebTransportBridgeError>> {
        Box::pin(async move {
            if self.session_closed.load(Ordering::Acquire) {
                return Err(WebTransportBridgeError::Stream(
                    "stream closed by peer".to_string(),
                ));
            }
            if self.outbound_finished.load(Ordering::Acquire) {
                return Err(WebTransportBridgeError::Stream(
                    "stream already finished".to_string(),
                ));
            }
            let mut guard = self.outbound.lock().unwrap_or_else(|e| e.into_inner());
            guard.extend_from_slice(&data);
            Ok(())
        })
    }

    fn finish(self: Arc<Self>) -> WebTransportFuture<Result<(), WebTransportBridgeError>> {
        Box::pin(async move {
            if self.session_closed.load(Ordering::Acquire) {
                return Err(WebTransportBridgeError::Stream(
                    "stream closed by peer".to_string(),
                ));
            }
            self.outbound_finished.store(true, Ordering::Release);
            Ok(())
        })
    }
}

struct SyntheticSessionState {
    session_id: u64,
    path: String,
    headers: Vec<(Vec<u8>, Vec<u8>)>,
    closed: Arc<AtomicBool>,
    close_info: StdMutex<Option<(u32, String)>>,
    bidi_in_tx: StdMutex<Option<mpsc::UnboundedSender<WebTransportStreamHandle>>>,
    bidi_in_rx: AsyncMutex<mpsc::UnboundedReceiver<WebTransportStreamHandle>>,
    bidi_out_tx: StdMutex<Option<mpsc::UnboundedSender<Arc<SyntheticStreamState>>>>,
    bidi_out_rx: StdMutex<mpsc::UnboundedReceiver<Arc<SyntheticStreamState>>>,
    dgram_in_tx: StdMutex<Option<mpsc::UnboundedSender<Bytes>>>,
    dgram_in_rx: AsyncMutex<mpsc::UnboundedReceiver<Bytes>>,
    dgram_out: StdMutex<VecDeque<Bytes>>,
    next_server_stream_id: AtomicU64,
    streams: StdMutex<Vec<Arc<SyntheticStreamState>>>,
}

impl SyntheticSessionState {
    fn new(session_id: u64, path: String, headers: Vec<(Vec<u8>, Vec<u8>)>) -> Arc<Self> {
        let (bidi_in_tx, bidi_in_rx) = mpsc::unbounded_channel();
        let (bidi_out_tx, bidi_out_rx) = mpsc::unbounded_channel();
        let (dgram_in_tx, dgram_in_rx) = mpsc::unbounded_channel();
        Arc::new(Self {
            session_id,
            path,
            headers,
            closed: Arc::new(AtomicBool::new(false)),
            close_info: StdMutex::new(None),
            bidi_in_tx: StdMutex::new(Some(bidi_in_tx)),
            bidi_in_rx: AsyncMutex::new(bidi_in_rx),
            bidi_out_tx: StdMutex::new(Some(bidi_out_tx)),
            bidi_out_rx: StdMutex::new(bidi_out_rx),
            dgram_in_tx: StdMutex::new(Some(dgram_in_tx)),
            dgram_in_rx: AsyncMutex::new(dgram_in_rx),
            dgram_out: StdMutex::new(VecDeque::new()),
            next_server_stream_id: AtomicU64::new(1),
            streams: StdMutex::new(Vec::new()),
        })
    }

    fn mark_closed(&self) {
        self.closed.store(true, Ordering::Release);
        drop(
            self.bidi_in_tx
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take(),
        );
        drop(
            self.bidi_out_tx
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take(),
        );
        drop(
            self.dgram_in_tx
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take(),
        );
        let streams = self.streams.lock().unwrap_or_else(|e| e.into_inner());
        for stream in streams.iter() {
            stream.inbound_notify.notify_waiters();
        }
    }
}

impl WebTransportSessionOps for SyntheticSessionState {
    fn session_id(&self) -> u64 {
        self.session_id
    }

    fn path(&self) -> &str {
        &self.path
    }

    fn headers(&self) -> &[(Vec<u8>, Vec<u8>)] {
        &self.headers
    }

    fn accept_bi(
        self: Arc<Self>,
    ) -> WebTransportFuture<Result<WebTransportStreamHandle, WebTransportBridgeError>> {
        Box::pin(async move {
            let mut guard = self.bidi_in_rx.lock().await;
            guard
                .recv()
                .await
                .ok_or(WebTransportBridgeError::SessionClosed(self.session_id))
        })
    }

    fn open_bi(
        self: Arc<Self>,
    ) -> WebTransportFuture<Result<WebTransportStreamHandle, WebTransportBridgeError>> {
        Box::pin(async move {
            if self.closed.load(Ordering::Acquire) {
                return Err(WebTransportBridgeError::SessionClosed(self.session_id));
            }
            let stream_id = self.next_server_stream_id.fetch_add(4, Ordering::Relaxed);
            let stream = Arc::new(SyntheticStreamState::new(
                stream_id,
                Arc::clone(&self.closed),
            ));
            self.streams
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(Arc::clone(&stream));
            let sent = {
                let guard = self.bidi_out_tx.lock().unwrap_or_else(|e| e.into_inner());
                guard
                    .as_ref()
                    .is_some_and(|tx| tx.send(Arc::clone(&stream)).is_ok())
            };
            if !sent {
                return Err(WebTransportBridgeError::SessionClosed(self.session_id));
            }
            Ok(stream as WebTransportStreamHandle)
        })
    }

    fn recv_datagram(
        self: Arc<Self>,
    ) -> WebTransportFuture<Result<Bytes, WebTransportBridgeError>> {
        Box::pin(async move {
            let mut guard = self.dgram_in_rx.lock().await;
            guard
                .recv()
                .await
                .ok_or(WebTransportBridgeError::SessionClosed(self.session_id))
        })
    }

    fn send_datagram(&self, data: &[u8]) -> Result<(), WebTransportBridgeError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(WebTransportBridgeError::SessionClosed(self.session_id));
        }
        let mut guard = self.dgram_out.lock().unwrap_or_else(|e| e.into_inner());
        guard.push_back(Bytes::copy_from_slice(data));
        Ok(())
    }

    fn close(
        self: Arc<Self>,
        error_code: u32,
        reason: String,
    ) -> WebTransportFuture<Result<(), WebTransportBridgeError>> {
        Box::pin(async move {
            {
                let mut info = self.close_info.lock().unwrap_or_else(|e| e.into_inner());
                if info.is_none() {
                    *info = Some((error_code, reason));
                }
            }
            self.mark_closed();
            Ok(())
        })
    }
}

#[pyclass(
    name = "_SyntheticWebTransportStreamPeer",
    module = "vertebra._vertebra",
    frozen,
    immutable_type
)]
struct SyntheticWebTransportStreamPeer {
    inner: Arc<SyntheticStreamState>,
}

#[pymethods]
impl SyntheticWebTransportStreamPeer {
    #[getter]
    fn stream_id(&self) -> u64 {
        self.inner.stream_id
    }

    fn push_data(&self, py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<()> {
        let payload = extract_buffer_bytes(py, data)?;
        {
            let mut guard = self.inner.inbound.lock().unwrap_or_else(|e| e.into_inner());
            guard.extend(&payload);
        }
        self.inner.inbound_notify.notify_waiters();
        Ok(())
    }

    fn finish_input(&self) {
        self.inner.inbound_eof.store(true, Ordering::Release);
        self.inner.inbound_notify.notify_waiters();
    }

    fn take_written<'py>(&self, py: Python<'py>) -> Py<PyBytes> {
        let mut guard = self
            .inner
            .outbound
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let out = PyBytes::new(py, &guard);
        guard.clear();
        out.unbind()
    }

    fn is_finished(&self) -> bool {
        self.inner.outbound_finished.load(Ordering::Acquire)
    }
}

#[pyclass(
    name = "_SyntheticWebTransportPeer",
    module = "vertebra._vertebra",
    frozen,
    immutable_type
)]
struct SyntheticWebTransportPeer {
    inner: Arc<SyntheticSessionState>,
}

#[pymethods]
impl SyntheticWebTransportPeer {
    #[pyo3(signature = (stream_id=8))]
    fn push_client_stream(&self, stream_id: u64) -> PyResult<SyntheticWebTransportStreamPeer> {
        let stream = Arc::new(SyntheticStreamState::new(
            stream_id,
            Arc::clone(&self.inner.closed),
        ));
        self.inner
            .streams
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(Arc::clone(&stream));
        let sent = {
            let guard = self
                .inner
                .bidi_in_tx
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            guard.as_ref().is_some_and(|tx| {
                tx.send(Arc::clone(&stream) as WebTransportStreamHandle)
                    .is_ok()
            })
        };
        if !sent {
            return Err(PyConnectionResetError::new_err(
                "synthetic WebTransport session is closed",
            ));
        }
        Ok(SyntheticWebTransportStreamPeer { inner: stream })
    }

    fn pop_server_stream(&self) -> Option<SyntheticWebTransportStreamPeer> {
        let mut guard = self
            .inner
            .bidi_out_rx
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        guard
            .try_recv()
            .ok()
            .map(|inner| SyntheticWebTransportStreamPeer { inner })
    }

    fn push_datagram(&self, py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<()> {
        let payload = extract_buffer_bytes(py, data)?;
        let sent = {
            let guard = self
                .inner
                .dgram_in_tx
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            guard.as_ref().is_some_and(|tx| tx.send(payload).is_ok())
        };
        if !sent {
            return Err(PyConnectionResetError::new_err(
                "synthetic WebTransport session is closed",
            ));
        }
        Ok(())
    }

    fn pop_sent_datagram<'py>(&self, py: Python<'py>) -> Option<Py<PyBytes>> {
        let mut guard = self
            .inner
            .dgram_out
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        guard.pop_front().map(|b| PyBytes::new(py, &b).unbind())
    }

    fn closed_info(&self) -> (bool, Option<u32>, Option<String>) {
        let is_closed = self.inner.closed.load(Ordering::Acquire);
        let info = self
            .inner
            .close_info
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        match info {
            Some((code, reason)) => (is_closed, Some(code), Some(reason)),
            None => (is_closed, None, None),
        }
    }

    fn close_from_peer(&self) {
        self.inner.mark_closed();
    }
}

#[pyfunction]
#[pyo3(
    name = "_synthetic_webtransport_pair",
    signature = (session_id=4, path="/wt", headers=None, submit_to_bridge=false)
)]
fn synthetic_webtransport_pair(
    py: Python<'_>,
    session_id: u64,
    path: &str,
    headers: Option<Vec<(Vec<u8>, Vec<u8>)>>,
    submit_to_bridge: bool,
) -> PyResult<(Py<PyAny>, SyntheticWebTransportPeer)> {
    let state =
        SyntheticSessionState::new(session_id, path.to_string(), headers.unwrap_or_default());
    let peer = SyntheticWebTransportPeer {
        inner: Arc::clone(&state),
    };
    if submit_to_bridge {
        let bridge = BatchBridge::get().ok_or_else(|| {
            PyRuntimeError::new_err("transport not initialized; call init_transport() first")
        })?;
        let submitted = bridge.submit_webtransport_session(state as WebTransportSessionHandle);
        let submitted_py = submitted.into_pyobject(py)?.to_owned().into_any().unbind();
        Ok((submitted_py, peer))
    } else {
        let session = Py::new(
            py,
            WebTransportSession {
                inner: state as WebTransportSessionHandle,
            },
        )?;
        Ok((session.into_any(), peer))
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<WebTransportStream>()?;
    m.add_class::<WebTransportSession>()?;
    m.add_class::<SyntheticWebTransportPeer>()?;
    m.add_class::<SyntheticWebTransportStreamPeer>()?;
    m.add_function(wrap_pyfunction!(register_webtransport_handler, m)?)?;
    m.add_function(wrap_pyfunction!(synthetic_webtransport_pair, m)?)?;
    Ok(())
}
