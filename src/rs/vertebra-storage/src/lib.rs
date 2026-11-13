//! `vertebra-storage` crate and `vertebra._storage` PyO3 extension module.

pub mod gcp_auth;
pub mod oci;
pub mod r2;
pub mod signing;
pub mod store;

use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{
    PyBufferError, PyFileExistsError, PyFileNotFoundError, PyOSError, PyPermissionError,
    PyRuntimeError, PyStopAsyncIteration, PyStopIteration, PyValueError,
};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{PyBytes, PyCapsule, PyType, PyWeakref, PyWeakrefMethods, PyWeakrefReference};
use tokio::task::AbortHandle;
use vertebra_core::gcp_auth::{
    GCP_TOKEN_CACHE_CAPSULE_NAME, GcpTokenCacheCapsule, SharedGcpTokenCacheHandle,
};
use vertebra_core::notify::{AsyncCompletionQueue, Notifier};

use crate::oci::{OciCavageCredentials, OciParConfig, build_oci_par_url, sign_oci_cavage_request};
use crate::r2::chunk_uniform_parts;
use crate::signing::{
    AwsSigV4HeaderConfig, AzureSasConfig, GcsSignConfig, GcsV4CanonicalRequest, HmacSigner,
    S3SignConfig, resolve_gcs_endpoint_origin, sign_aws_sigv4_headers, sign_azure_sas_url,
    sign_gcs_v4_with_iam_endpoint_and_cache, sign_s3_v4_url, verify_s3_v4_url,
};
use crate::store::{
    MultipartUploadSession, ObjectStoreEngine, PutConditionalOptions, StorageError,
    StoredObjectMeta, block_on_storage, runtime_handle, validate_expected_hex,
};

fn make_precondition_err(message: String) -> PyErr {
    Python::attach(|py| {
        if let Ok(typing_mod) = py.import("vertebra.typing")
            && let Ok(exc_attr) = typing_mod.getattr("PreconditionError")
            && let Ok(exc_ty) = exc_attr.cast_into::<PyType>()
        {
            return PyErr::from_type(exc_ty, (message,));
        }
        PyOSError::new_err(message)
    })
}

fn make_integrity_err(message: String) -> PyErr {
    Python::attach(|py| {
        if let Ok(typing_mod) = py.import("vertebra.typing")
            && let Ok(exc_attr) = typing_mod.getattr("IntegrityError")
            && let Ok(exc_ty) = exc_attr.cast_into::<PyType>()
        {
            return PyErr::from_type(exc_ty, (message,));
        }
        PyOSError::new_err(message)
    })
}

fn to_py_err(err: StorageError) -> PyErr {
    match err {
        StorageError::InvalidUrl(u, msg) => PyValueError::new_err(format!("{u}: {msg}")),
        StorageError::InvalidInput(msg) => PyValueError::new_err(msg),
        StorageError::AlreadyExists { path, message } => {
            PyFileExistsError::new_err(format!("Object already exists at '{path}': {message}"))
        }
        StorageError::Precondition { path, message } => {
            make_precondition_err(format!("Precondition failed for '{path}': {message}"))
        }
        err @ StorageError::Integrity { .. } => make_integrity_err(err.to_string()),
        StorageError::ObjectStore(object_store::Error::NotFound { path, source }) => {
            PyFileNotFoundError::new_err(format!("Object not found: {path} ({source})"))
        }
        StorageError::ObjectStore(object_store::Error::AlreadyExists { path, source }) => {
            PyFileExistsError::new_err(format!("Object already exists at '{path}': {source}"))
        }
        StorageError::ObjectStore(object_store::Error::Precondition { path, source }) => {
            make_precondition_err(format!("Precondition failed for '{path}': {source}"))
        }
        StorageError::Io(io_err) => match io_err.kind() {
            std::io::ErrorKind::NotFound => PyFileNotFoundError::new_err(io_err.to_string()),
            std::io::ErrorKind::AlreadyExists => PyFileExistsError::new_err(io_err.to_string()),
            std::io::ErrorKind::PermissionDenied => PyPermissionError::new_err(io_err.to_string()),
            _ => PyRuntimeError::new_err(io_err.to_string()),
        },
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

fn validate_chunk_size(chunk_size: Option<usize>) -> PyResult<Option<usize>> {
    if chunk_size == Some(0) {
        return Err(PyValueError::new_err("chunk_size must be >= 1"));
    }
    Ok(chunk_size)
}

fn validate_concurrency(concurrency: i64) -> PyResult<usize> {
    if concurrency < 1 {
        return Err(PyValueError::new_err("concurrency must be >= 1"));
    }
    usize::try_from(concurrency).map_err(|_| PyValueError::new_err("concurrency is too large"))
}

fn metas_to_py_list(py: Python<'_>, metas: Vec<StoredObjectMeta>) -> PyResult<Py<PyAny>> {
    let converted: Vec<PyObjectMeta> = metas.into_iter().map(PyObjectMeta::from).collect();
    Ok(converted.into_pyobject(py)?.into_any().unbind())
}

fn extract_token_cache_capsule(
    capsule_any: &Bound<'_, PyAny>,
) -> PyResult<SharedGcpTokenCacheHandle> {
    let capsule = capsule_any.cast::<PyCapsule>().map_err(|_| {
        PyValueError::new_err("token_cache_capsule must be a valid GcpTokenCache PyCapsule")
    })?;
    let ptr = capsule
        .pointer_checked(Some(GCP_TOKEN_CACHE_CAPSULE_NAME))
        .map_err(|_| {
            PyValueError::new_err(
                "token_cache_capsule name mismatch; expected vertebra.GcpTokenCacheCapsule",
            )
        })?
        .as_ptr()
        .cast::<GcpTokenCacheCapsule>();
    // SAFETY: `pointer_checked` verified the exact `GCP_TOKEN_CACHE_CAPSULE_NAME` tag on a live `PyCapsule`
    // created by `PyCapsule::new_with_value(..., GcpTokenCacheCapsule, GCP_TOKEN_CACHE_CAPSULE_NAME)`.
    unsafe { SharedGcpTokenCacheHandle::from_capsule_ptr(ptr) }.map_err(PyValueError::new_err)
}

async fn pull_next_from_stream(
    stream_opt: &mut Option<BoxStream<'static, Result<Bytes, StorageError>>>,
    pending: &mut Bytes,
    chunk_size: Option<usize>,
) -> Result<Option<Bytes>, StorageError> {
    loop {
        if let Some(max_len) = chunk_size
            && !pending.is_empty()
        {
            let take = pending.len().min(max_len);
            return Ok(Some(pending.split_to(take)));
        }
        let Some(stream) = stream_opt.as_mut() else {
            return Ok(None);
        };
        match stream.next().await {
            Some(Ok(bytes)) => {
                if bytes.is_empty() {
                    continue;
                }
                if let Some(max_len) = chunk_size {
                    if bytes.len() <= max_len {
                        return Ok(Some(bytes));
                    }
                    *pending = bytes;
                    return Ok(Some(pending.split_to(max_len)));
                }
                return Ok(Some(bytes));
            }
            Some(Err(err)) => {
                *stream_opt = None;
                *pending = Bytes::new();
                return Err(err);
            }
            None => {
                *stream_opt = None;
                return Ok(None);
            }
        }
    }
}

fn extract_buffer_bytes(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Bytes> {
    if let Ok(storage_buf) = data.cast::<StorageBuffer>() {
        return Ok(storage_buf.borrow().data.clone());
    }
    if let Ok(py_bytes) = data.cast::<PyBytes>() {
        return Ok(Bytes::from_owner(PyBackedBytes::from(py_bytes.clone())));
    }
    let buf = PyBuffer::<u8>::get(data)?;
    if !buf.is_c_contiguous() {
        return Err(PyBufferError::new_err(
            "storage input buffer must be C-contiguous",
        ));
    }
    let copied = buf.to_vec(py)?;
    drop(buf);
    Ok(Bytes::from(copied))
}

/// Zero-copy read-only Python buffer wrapping Rust `bytes::Bytes`.
#[pyclass(
    name = "StorageBuffer",
    module = "vertebra._storage",
    frozen,
    immutable_type,
    from_py_object
)]
#[derive(Clone, Debug)]
pub struct StorageBuffer {
    pub data: Bytes,
}

#[pymethods]
impl StorageBuffer {
    pub fn __len__(&self) -> usize {
        self.data.len()
    }

    pub fn to_bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.data)
    }

    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut pyo3::ffi::Py_buffer,
        flags: std::os::raw::c_int,
    ) -> PyResult<()> {
        if view.is_null() {
            return Err(PyBufferError::new_err("null Py_buffer pointer"));
        }
        let py = slf.py();
        let slice = &slf.get().data[..];
        let ptr = if slice.is_empty() {
            std::ptr::NonNull::<u8>::dangling().as_ptr()
        } else {
            slice.as_ptr()
        };
        // SAFETY: `view` was verified non-null, `slf.as_ptr()` is a valid borrowed object
        // reference whose refcount `PyBuffer_FillInfo` increments once, and `ptr` remains
        // immutable and valid for `slice.len()` bytes for the lifetime of `slf`.
        let ret = unsafe {
            pyo3::ffi::PyBuffer_FillInfo(
                view,
                slf.as_ptr(),
                ptr as *mut std::os::raw::c_void,
                slice.len() as isize,
                1,
                flags,
            )
        };
        if ret == -1 {
            return Err(PyErr::fetch(py));
        }
        Ok(())
    }

    unsafe fn __releasebuffer__(&self, _view: *mut pyo3::ffi::Py_buffer) {}
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        format!("Rust task panicked: {text}")
    } else if let Some(text) = payload.downcast_ref::<String>() {
        format!("Rust task panicked: {text}")
    } else {
        "Rust task panicked".to_string()
    }
}

fn make_task_panic_err(py: Python<'_>, message: String) -> PyErr {
    if let Ok(core_mod) = py.import("vertebra._vertebra")
        && let Ok(exc_attr) = core_mod.getattr("TaskPanicError")
        && let Ok(exc_ty) = exc_attr.cast_into::<PyType>()
    {
        return PyErr::from_type(exc_ty, (message,));
    }
    PyRuntimeError::new_err(message)
}

#[pyclass(
    name = "_StorageTaskCanceller",
    module = "vertebra._storage",
    frozen,
    immutable_type
)]
struct StorageTaskCanceller {
    abort: AbortHandle,
}

#[pymethods]
impl StorageTaskCanceller {
    fn __call__(&self, future: &Bound<'_, PyAny>) -> PyResult<()> {
        if future.call_method0("cancelled")?.extract::<bool>()? {
            self.abort.abort();
        }
        Ok(())
    }
}

type OutcomeProducer = Box<dyn FnOnce(Python<'_>) -> PyResult<Py<PyAny>> + Send>;

struct CompletedStorageTask {
    future: Py<PyAny>,
    produce: OutcomeProducer,
}

struct RustCompletionRing {
    completion: AsyncCompletionQueue<CompletedStorageTask>,
    fallback_wakeup: Mutex<Option<(Py<PyWeakref>, &'static str)>>,
}

impl RustCompletionRing {
    fn signal_fallback(&self) {
        if self.completion.notifier().is_some() {
            return;
        }
        Python::attach(|py| {
            let target = {
                let mut guard = self
                    .fallback_wakeup
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                match guard.as_ref() {
                    Some((weak_loop, attr_name)) => match weak_loop.bind(py).upgrade() {
                        Some(loop_bound) => Some((loop_bound, *attr_name)),
                        None => {
                            *guard = None;
                            None
                        }
                    },
                    None => None,
                }
            };
            if let Some((loop_bound, attr_name)) = target
                && let Ok(drainer) = loop_bound.getattr(attr_name)
            {
                let _ = loop_bound.call_method1("call_soon_threadsafe", (drainer,));
            }
        });
    }
}

#[pyclass(
    name = "_StorageCompletionDrainer",
    module = "vertebra._storage",
    frozen,
    immutable_type
)]
struct StorageCompletionDrainer {
    ring: Arc<RustCompletionRing>,
}

#[pymethods]
impl StorageCompletionDrainer {
    fn __call__(&self, py: Python<'_>) -> PyResult<()> {
        self.ring.completion.clear_notifier();
        let mut batch = Vec::new();
        while self.ring.completion.drain_into(&mut batch) {
            for item in batch.drain(..) {
                let fut = item.future.bind(py);
                let cancelled: bool = fut
                    .call_method0("cancelled")
                    .and_then(|v| v.extract())
                    .unwrap_or(false);
                if cancelled {
                    continue;
                }
                match catch_unwind(AssertUnwindSafe(|| (item.produce)(py))) {
                    Ok(Ok(val)) => {
                        let _ = fut.call_method1("set_result", (val,));
                    }
                    Ok(Err(err)) => {
                        let _ = fut.call_method1("set_exception", (err.into_value(py),));
                    }
                    Err(payload) => {
                        let err = make_task_panic_err(py, panic_message(payload));
                        let _ = fut.call_method1("set_exception", (err.into_value(py),));
                    }
                }
            }
        }
        Ok(())
    }
}

fn ensure_loop_completion_ring(
    py: Python<'_>,
    loop_obj: &Bound<'_, PyAny>,
) -> PyResult<Arc<RustCompletionRing>> {
    const ATTR_NAME: &str = "_vertebra_storage_drainer";
    if let Ok(existing) = loop_obj.getattr(ATTR_NAME)
        && let Ok(drainer) = existing.cast::<StorageCompletionDrainer>()
    {
        return Ok(Arc::clone(&drainer.get().ring));
    }

    #[cfg(unix)]
    if let Ok(notifier) = Notifier::new() {
        let fd = notifier.as_raw_fd();
        let temp_ring = Arc::new(RustCompletionRing {
            completion: AsyncCompletionQueue::new(Some(notifier)),
            fallback_wakeup: Mutex::new(None),
        });
        let drainer_py = Py::new(
            py,
            StorageCompletionDrainer {
                ring: Arc::clone(&temp_ring),
            },
        )?;
        if loop_obj
            .call_method1("add_reader", (fd, drainer_py.bind(py)))
            .is_ok()
        {
            if let Err(err) = loop_obj.setattr(ATTR_NAME, drainer_py.bind(py)) {
                let _ = loop_obj.call_method1("remove_reader", (fd,));
                return Err(err);
            }
            return Ok(temp_ring);
        }
    }

    let weak_loop = PyWeakrefReference::new(loop_obj)?
        .into_any()
        .cast_into::<PyWeakref>()?
        .unbind();
    let ring = Arc::new(RustCompletionRing {
        completion: AsyncCompletionQueue::new(None),
        fallback_wakeup: Mutex::new(Some((weak_loop, ATTR_NAME))),
    });
    let drainer_py = Py::new(
        py,
        StorageCompletionDrainer {
            ring: Arc::clone(&ring),
        },
    )?;
    loop_obj.setattr(ATTR_NAME, drainer_py.bind(py))?;
    Ok(ring)
}

/// Native Rust awaitable backed by the shared Tokio runtime without `asyncio.to_thread`.
#[pyclass(
    name = "RustTaskAwaitable",
    module = "vertebra._storage",
    frozen,
    immutable_type
)]
pub struct RustTaskAwaitable {
    future: Py<PyAny>,
}

#[pymethods]
impl RustTaskAwaitable {
    fn __await__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let iter = self.future.bind(py).call_method0("__await__")?;
        Ok(iter.unbind())
    }
}

fn spawn_rust_task<F, T, M>(py: Python<'_>, fut: F, map_fn: M) -> PyResult<RustTaskAwaitable>
where
    F: Future<Output = Result<T, StorageError>> + Send + 'static,
    T: Send + 'static,
    M: FnOnce(Python<'_>, T) -> PyResult<Py<PyAny>> + Send + 'static,
{
    let asyncio = py.import("asyncio")?;
    let loop_obj = asyncio.call_method0("get_running_loop")?;
    let py_fut = loop_obj.call_method0("create_future")?;
    let ring = ensure_loop_completion_ring(py, &loop_obj)?;
    let fut_for_cb = py_fut.clone().unbind();
    let handle = runtime_handle();
    let worker = handle.spawn(fut);
    let abort = worker.abort_handle();
    let canceller = match Py::new(
        py,
        StorageTaskCanceller {
            abort: abort.clone(),
        },
    ) {
        Ok(cb) => cb,
        Err(err) => {
            abort.abort();
            return Err(err);
        }
    };
    if let Err(err) = py_fut.call_method1("add_done_callback", (canceller,)) {
        abort.abort();
        return Err(err);
    }
    let fut_for_await = py_fut.unbind();

    handle.spawn(async move {
        let produce: OutcomeProducer = match worker.await {
            Ok(Ok(val)) => Box::new(move |py| map_fn(py, val)),
            Ok(Err(err)) => Box::new(move |_| Err(to_py_err(err))),
            Err(join) if join.is_panic() => {
                let message = panic_message(join.into_panic());
                Box::new(move |py| Err(make_task_panic_err(py, message)))
            }
            Err(_) => Box::new(|_| {
                Err(pyo3::exceptions::asyncio::CancelledError::new_err(
                    "Rust storage task was cancelled",
                ))
            }),
        };
        let should_signal = ring.completion.push(CompletedStorageTask {
            future: fut_for_cb,
            produce,
        });
        if should_signal {
            ring.signal_fallback();
        }
    });

    Ok(RustTaskAwaitable {
        future: fut_for_await,
    })
}

#[pyclass(
    name = "ObjectMeta",
    module = "vertebra._storage",
    frozen,
    immutable_type,
    from_py_object
)]
#[derive(Clone, Debug)]
pub struct PyObjectMeta {
    #[pyo3(get)]
    pub path: String,
    #[pyo3(get)]
    pub size: u64,
    #[pyo3(get)]
    pub last_modified: String,
    #[pyo3(get)]
    pub etag: Option<String>,
    #[pyo3(get)]
    pub version: Option<String>,
    #[pyo3(get)]
    pub content_type: Option<String>,
    #[pyo3(get)]
    pub attributes: HashMap<String, String>,
    #[pyo3(get)]
    pub sha256: Option<String>,
    #[pyo3(get)]
    pub crc32c: Option<String>,
    #[pyo3(get)]
    pub md5: Option<String>,
}

impl From<StoredObjectMeta> for PyObjectMeta {
    fn from(value: StoredObjectMeta) -> Self {
        Self {
            path: value.path,
            size: value.size,
            last_modified: value.last_modified,
            etag: value.etag,
            version: value.version,
            content_type: value.content_type,
            attributes: value.attributes,
            sha256: value.sha256,
            crc32c: value.crc32c,
            md5: value.md5,
        }
    }
}

#[pymethods]
impl PyObjectMeta {
    fn __repr__(&self) -> String {
        format!("ObjectMeta(path={:?}, size={})", self.path, self.size)
    }
}

struct SyncStreamState {
    stream: Option<BoxStream<'static, Result<Bytes, StorageError>>>,
    pending: Bytes,
    chunk_size: Option<usize>,
}

#[pyclass(
    name = "SyncStreamReader",
    module = "vertebra._storage",
    frozen,
    immutable_type
)]
pub struct PySyncStreamReader {
    state: Mutex<SyncStreamState>,
}

#[pymethods]
impl PySyncStreamReader {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<StorageBuffer> {
        match self.next_chunk(py)? {
            Some(buf) => Ok(buf),
            None => Err(PyStopIteration::new_err(())),
        }
    }

    fn next_chunk(&self, py: Python<'_>) -> PyResult<Option<StorageBuffer>> {
        let next = py
            .detach(|| {
                let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
                let state = &mut *guard;
                block_on_storage(pull_next_from_stream(
                    &mut state.stream,
                    &mut state.pending,
                    state.chunk_size,
                ))
            })
            .map_err(to_py_err)?;
        Ok(next.map(|data| StorageBuffer { data }))
    }

    fn close(&self, py: Python<'_>) {
        py.detach(|| {
            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            guard.stream = None;
            guard.pending = Bytes::new();
        });
    }
}

enum AsyncStreamInit {
    Pending {
        engine: ObjectStoreEngine,
        path: String,
        expected_sha256: Option<String>,
    },
    Open(Option<BoxStream<'static, Result<Bytes, StorageError>>>),
}

struct AsyncStreamState {
    init: AsyncStreamInit,
    pending: Bytes,
    chunk_size: Option<usize>,
}

impl AsyncStreamState {
    async fn next_bytes(&mut self) -> Result<Option<Bytes>, StorageError> {
        if matches!(self.init, AsyncStreamInit::Pending { .. })
            && let AsyncStreamInit::Pending {
                engine,
                path,
                expected_sha256,
            } = std::mem::replace(&mut self.init, AsyncStreamInit::Open(None))
        {
            let stream = match expected_sha256.as_deref() {
                Some(expected) => engine.get_stream_verified(&path, expected).await?,
                None => engine.get_stream(&path).await?,
            };
            self.init = AsyncStreamInit::Open(Some(stream));
        }
        let AsyncStreamInit::Open(ref mut stream_opt) = self.init else {
            return Ok(None);
        };
        pull_next_from_stream(stream_opt, &mut self.pending, self.chunk_size).await
    }
}

#[pyclass(
    name = "AsyncStreamReader",
    module = "vertebra._storage",
    frozen,
    immutable_type
)]
pub struct PyAsyncStreamReader {
    state: Arc<tokio::sync::Mutex<AsyncStreamState>>,
}

#[pymethods]
impl PyAsyncStreamReader {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__(&self, py: Python<'_>) -> PyResult<RustTaskAwaitable> {
        let state = Arc::clone(&self.state);
        spawn_rust_task(
            py,
            async move {
                let mut guard = state.lock().await;
                guard.next_bytes().await
            },
            |py, next| match next {
                Some(data) => Ok(Py::new(py, StorageBuffer { data })?.into_any()),
                None => Err(PyStopAsyncIteration::new_err(())),
            },
        )
    }

    fn next_chunk(&self, py: Python<'_>) -> PyResult<RustTaskAwaitable> {
        let state = Arc::clone(&self.state);
        spawn_rust_task(
            py,
            async move {
                let mut guard = state.lock().await;
                guard.next_bytes().await
            },
            |py, next| match next {
                Some(data) => Ok(Py::new(py, StorageBuffer { data })?.into_any()),
                None => Ok(py.None()),
            },
        )
    }

    fn aclose(&self, py: Python<'_>) -> PyResult<RustTaskAwaitable> {
        let state = Arc::clone(&self.state);
        spawn_rust_task(
            py,
            async move {
                let mut guard = state.lock().await;
                guard.init = AsyncStreamInit::Open(None);
                guard.pending = Bytes::new();
                Ok(())
            },
            |py, ()| Ok(py.None()),
        )
    }
}

#[pyclass(
    name = "SyncMultipartUpload",
    module = "vertebra._storage",
    frozen,
    immutable_type
)]
pub struct PySyncMultipartUpload {
    session: Mutex<MultipartUploadSession>,
}

#[pymethods]
impl PySyncMultipartUpload {
    fn write_part(&self, py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<()> {
        let chunk = extract_buffer_bytes(py, data)?;
        py.detach(|| {
            let mut guard = self.session.lock().unwrap_or_else(|e| e.into_inner());
            block_on_storage(guard.write_chunk(chunk))
        })
        .map_err(to_py_err)
    }

    fn complete(&self, py: Python<'_>) -> PyResult<PyObjectMeta> {
        let meta = py
            .detach(|| {
                let mut guard = self.session.lock().unwrap_or_else(|e| e.into_inner());
                block_on_storage(guard.complete_with_meta())
            })
            .map_err(to_py_err)?;
        Ok(PyObjectMeta::from(meta))
    }

    fn abort(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            let mut guard = self.session.lock().unwrap_or_else(|e| e.into_inner());
            block_on_storage(guard.abort())
        })
        .map_err(to_py_err)
    }
}

#[pyclass(
    name = "AsyncMultipartUpload",
    module = "vertebra._storage",
    frozen,
    immutable_type
)]
pub struct PyAsyncMultipartUpload {
    session: Arc<tokio::sync::Mutex<MultipartUploadSession>>,
}

#[pymethods]
impl PyAsyncMultipartUpload {
    fn write_part(&self, py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<RustTaskAwaitable> {
        let chunk = extract_buffer_bytes(py, data)?;
        let session = Arc::clone(&self.session);
        spawn_rust_task(
            py,
            async move {
                let mut guard = session.lock().await;
                guard.write_chunk(chunk).await
            },
            |py, ()| Ok(py.None()),
        )
    }

    fn complete(&self, py: Python<'_>) -> PyResult<RustTaskAwaitable> {
        let session = Arc::clone(&self.session);
        spawn_rust_task(
            py,
            async move {
                let mut guard = session.lock().await;
                guard.complete_with_meta().await
            },
            |py, meta| Ok(Py::new(py, PyObjectMeta::from(meta))?.into_any()),
        )
    }

    fn abort(&self, py: Python<'_>) -> PyResult<RustTaskAwaitable> {
        let session = Arc::clone(&self.session);
        spawn_rust_task(
            py,
            async move {
                let mut guard = session.lock().await;
                guard.abort().await
            },
            |py, ()| Ok(py.None()),
        )
    }
}

#[pyclass(
    name = "SyncObjectStore",
    module = "vertebra._storage",
    frozen,
    immutable_type
)]
pub struct PySyncObjectStore {
    engine: ObjectStoreEngine,
}

#[pymethods]
impl PySyncObjectStore {
    #[new]
    #[pyo3(signature = (url, options=None, token_cache_capsule=None))]
    fn new(
        py: Python<'_>,
        url: &str,
        options: Option<HashMap<String, String>>,
        token_cache_capsule: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let token_cache = token_cache_capsule
            .map(extract_token_cache_capsule)
            .transpose()?;
        let engine = py
            .detach(|| ObjectStoreEngine::from_url_with_cache(url, options, token_cache))
            .map_err(to_py_err)?;
        Ok(Self { engine })
    }

    #[getter]
    fn scheme(&self) -> &str {
        self.engine.scheme()
    }

    fn to_async(&self) -> PyAsyncObjectStore {
        PyAsyncObjectStore {
            engine: self.engine.clone(),
        }
    }

    #[pyo3(signature = (path, data, *, content_type=None, attributes=None, if_not_exists=false, if_match=None, if_none_match=None, checksum=false, expected_sha256=None, expected_crc32c=None, expected_md5=None))]
    #[allow(clippy::too_many_arguments)]
    fn put(
        &self,
        py: Python<'_>,
        path: &str,
        data: &Bound<'_, PyAny>,
        content_type: Option<String>,
        attributes: Option<HashMap<String, String>>,
        if_not_exists: bool,
        if_match: Option<String>,
        if_none_match: Option<String>,
        checksum: bool,
        expected_sha256: Option<String>,
        expected_crc32c: Option<String>,
        expected_md5: Option<String>,
    ) -> PyResult<PyObjectMeta> {
        let payload = extract_buffer_bytes(py, data)?;
        let opts = PutConditionalOptions {
            if_not_exists,
            if_match,
            if_none_match,
            content_type,
            attributes: attributes.unwrap_or_default(),
            checksum,
            expected_sha256,
            expected_crc32c,
            expected_md5,
        };
        let engine = self.engine.clone();
        let meta = py
            .detach(|| block_on_storage(async move { engine.put_opts(path, payload, opts).await }))
            .map_err(to_py_err)?;
        Ok(PyObjectMeta::from(meta))
    }

    #[pyo3(signature = (path, file_path, *, checksum=false, expected_sha256=None, expected_crc32c=None, expected_md5=None))]
    #[allow(clippy::too_many_arguments)]
    fn put_file(
        &self,
        py: Python<'_>,
        path: &str,
        file_path: &str,
        checksum: bool,
        expected_sha256: Option<String>,
        expected_crc32c: Option<String>,
        expected_md5: Option<String>,
    ) -> PyResult<PyObjectMeta> {
        let opts = PutConditionalOptions {
            checksum,
            expected_sha256,
            expected_crc32c,
            expected_md5,
            ..Default::default()
        };
        let engine = self.engine.clone();
        let meta = py
            .detach(|| {
                block_on_storage(async move { engine.put_file_opts(path, file_path, opts).await })
            })
            .map_err(to_py_err)?;
        Ok(PyObjectMeta::from(meta))
    }

    #[pyo3(signature = (path, file_path, *, expected_sha256=None))]
    fn get_file(
        &self,
        py: Python<'_>,
        path: &str,
        file_path: &str,
        expected_sha256: Option<String>,
    ) -> PyResult<PyObjectMeta> {
        let engine = self.engine.clone();
        let meta = py
            .detach(|| {
                block_on_storage(async move {
                    match expected_sha256.as_deref() {
                        Some(expected) => engine.get_file_verified(path, file_path, expected).await,
                        None => engine.get_file(path, file_path).await,
                    }
                })
            })
            .map_err(to_py_err)?;
        Ok(PyObjectMeta::from(meta))
    }

    #[pyo3(signature = (pairs, *, concurrency=16))]
    fn get_files(
        &self,
        py: Python<'_>,
        pairs: Vec<(String, String)>,
        concurrency: i64,
    ) -> PyResult<Vec<PyObjectMeta>> {
        let concurrency = validate_concurrency(concurrency)?;
        let engine = self.engine.clone();
        let metas = py
            .detach(|| block_on_storage(async move { engine.get_files(pairs, concurrency).await }))
            .map_err(to_py_err)?;
        Ok(metas.into_iter().map(PyObjectMeta::from).collect())
    }

    #[pyo3(signature = (pairs, *, concurrency=16))]
    fn put_files(
        &self,
        py: Python<'_>,
        pairs: Vec<(String, String)>,
        concurrency: i64,
    ) -> PyResult<Vec<PyObjectMeta>> {
        let concurrency = validate_concurrency(concurrency)?;
        let engine = self.engine.clone();
        let metas = py
            .detach(|| block_on_storage(async move { engine.put_files(pairs, concurrency).await }))
            .map_err(to_py_err)?;
        Ok(metas.into_iter().map(PyObjectMeta::from).collect())
    }

    #[pyo3(signature = (sources, dst, *, content_type=None))]
    fn compose(
        &self,
        py: Python<'_>,
        sources: Vec<String>,
        dst: &str,
        content_type: Option<String>,
    ) -> PyResult<PyObjectMeta> {
        let engine = self.engine.clone();
        let meta = py
            .detach(|| {
                block_on_storage(async move { engine.compose(sources, dst, content_type).await })
            })
            .map_err(to_py_err)?;
        Ok(PyObjectMeta::from(meta))
    }

    #[pyo3(signature = (path, *, content_type=None, attributes=None, if_not_exists=false, if_match=None, if_none_match=None, checksum=false, expected_sha256=None, expected_crc32c=None, expected_md5=None))]
    #[allow(clippy::too_many_arguments)]
    fn create_multipart(
        &self,
        py: Python<'_>,
        path: &str,
        content_type: Option<String>,
        attributes: Option<HashMap<String, String>>,
        if_not_exists: bool,
        if_match: Option<String>,
        if_none_match: Option<String>,
        checksum: bool,
        expected_sha256: Option<String>,
        expected_crc32c: Option<String>,
        expected_md5: Option<String>,
    ) -> PyResult<PySyncMultipartUpload> {
        let opts = PutConditionalOptions {
            if_not_exists,
            if_match,
            if_none_match,
            content_type,
            attributes: attributes.unwrap_or_default(),
            checksum,
            expected_sha256,
            expected_crc32c,
            expected_md5,
        };
        let engine = self.engine.clone();
        let session = py
            .detach(|| block_on_storage(async move { engine.create_multipart(path, opts).await }))
            .map_err(to_py_err)?;
        Ok(PySyncMultipartUpload {
            session: Mutex::new(session),
        })
    }

    fn put_multipart(
        &self,
        py: Python<'_>,
        path: &str,
        parts: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<PyObjectMeta> {
        let mut chunks = Vec::with_capacity(parts.len());
        for part in &parts {
            chunks.push(extract_buffer_bytes(py, part)?);
        }
        let engine = self.engine.clone();
        let meta = py
            .detach(|| block_on_storage(async move { engine.put_multipart(path, chunks).await }))
            .map_err(to_py_err)?;
        Ok(PyObjectMeta::from(meta))
    }

    #[pyo3(signature = (path, *, expected_sha256=None))]
    fn get<'py>(
        &self,
        py: Python<'py>,
        path: &str,
        expected_sha256: Option<String>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let engine = self.engine.clone();
        let data = py
            .detach(|| {
                block_on_storage(async move {
                    match expected_sha256.as_deref() {
                        Some(expected) => engine.get_verified(path, expected).await,
                        None => engine.get(path).await,
                    }
                })
            })
            .map_err(to_py_err)?;
        Ok(PyBytes::new(py, &data))
    }

    fn get_buffer(&self, py: Python<'_>, path: &str) -> PyResult<StorageBuffer> {
        let engine = self.engine.clone();
        let data = py
            .detach(|| block_on_storage(async move { engine.get(path).await }))
            .map_err(to_py_err)?;
        Ok(StorageBuffer { data })
    }

    #[pyo3(signature = (path, *, chunk_size=None, expected_sha256=None))]
    fn get_stream(
        &self,
        py: Python<'_>,
        path: &str,
        chunk_size: Option<usize>,
        expected_sha256: Option<String>,
    ) -> PyResult<PySyncStreamReader> {
        let chunk_size = validate_chunk_size(chunk_size)?;
        let engine = self.engine.clone();
        let stream = py
            .detach(|| {
                block_on_storage(async move {
                    match expected_sha256.as_deref() {
                        Some(expected) => engine.get_stream_verified(path, expected).await,
                        None => engine.get_stream(path).await,
                    }
                })
            })
            .map_err(to_py_err)?;
        Ok(PySyncStreamReader {
            state: Mutex::new(SyncStreamState {
                stream: Some(stream),
                pending: Bytes::new(),
                chunk_size,
            }),
        })
    }

    fn get_range<'py>(
        &self,
        py: Python<'py>,
        path: &str,
        offset: u64,
        length: u64,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let engine = self.engine.clone();
        let data = py
            .detach(|| {
                block_on_storage(async move { engine.get_range(path, offset, length).await })
            })
            .map_err(to_py_err)?;
        Ok(PyBytes::new(py, &data))
    }

    fn head(&self, py: Python<'_>, path: &str) -> PyResult<PyObjectMeta> {
        let engine = self.engine.clone();
        let meta = py
            .detach(|| block_on_storage(async move { engine.head(path).await }))
            .map_err(to_py_err)?;
        Ok(PyObjectMeta::from(meta))
    }

    fn exists(&self, py: Python<'_>, path: &str) -> PyResult<bool> {
        let engine = self.engine.clone();
        py.detach(|| block_on_storage(async move { engine.exists(path).await }))
            .map_err(to_py_err)
    }

    fn delete(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        let engine = self.engine.clone();
        py.detach(|| block_on_storage(async move { engine.delete(path).await }))
            .map_err(to_py_err)
    }

    #[pyo3(signature = (src, dst, *, if_not_exists=false))]
    fn copy(&self, py: Python<'_>, src: &str, dst: &str, if_not_exists: bool) -> PyResult<()> {
        let engine = self.engine.clone();
        py.detach(|| block_on_storage(async move { engine.copy(src, dst, if_not_exists).await }))
            .map_err(to_py_err)
    }

    #[pyo3(signature = (src, dst, *, if_not_exists=false))]
    fn rename(&self, py: Python<'_>, src: &str, dst: &str, if_not_exists: bool) -> PyResult<()> {
        let engine = self.engine.clone();
        py.detach(|| block_on_storage(async move { engine.rename(src, dst, if_not_exists).await }))
            .map_err(to_py_err)
    }

    #[pyo3(signature = (prefix=None))]
    fn list(&self, py: Python<'_>, prefix: Option<&str>) -> PyResult<Vec<PyObjectMeta>> {
        let engine = self.engine.clone();
        let items = py
            .detach(|| block_on_storage(async move { engine.list(prefix).await }))
            .map_err(to_py_err)?;
        Ok(items.into_iter().map(PyObjectMeta::from).collect())
    }

    #[pyo3(signature = (prefix=None, offset=None, limit=1000))]
    fn list_page(
        &self,
        py: Python<'_>,
        prefix: Option<&str>,
        offset: Option<&str>,
        limit: usize,
    ) -> PyResult<(Vec<PyObjectMeta>, Option<String>)> {
        if limit == 0 {
            return Err(PyValueError::new_err("limit must be >= 1"));
        }
        let engine = self.engine.clone();
        let (items, cursor) = py
            .detach(|| {
                block_on_storage(async move { engine.list_page(prefix, offset, limit).await })
            })
            .map_err(to_py_err)?;
        Ok((items.into_iter().map(PyObjectMeta::from).collect(), cursor))
    }

    #[pyo3(signature = (prefix=None))]
    fn list_with_delimiter(
        &self,
        py: Python<'_>,
        prefix: Option<&str>,
    ) -> PyResult<(Vec<PyObjectMeta>, Vec<String>)> {
        let engine = self.engine.clone();
        let (items, prefixes) = py
            .detach(|| block_on_storage(async move { engine.list_with_delimiter(prefix).await }))
            .map_err(to_py_err)?;
        Ok((
            items.into_iter().map(PyObjectMeta::from).collect(),
            prefixes,
        ))
    }
}

#[pyclass(
    name = "AsyncObjectStore",
    module = "vertebra._storage",
    frozen,
    immutable_type
)]
pub struct PyAsyncObjectStore {
    engine: ObjectStoreEngine,
}

#[pymethods]
impl PyAsyncObjectStore {
    #[new]
    #[pyo3(signature = (url, options=None, token_cache_capsule=None))]
    fn new(
        py: Python<'_>,
        url: &str,
        options: Option<HashMap<String, String>>,
        token_cache_capsule: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let token_cache = token_cache_capsule
            .map(extract_token_cache_capsule)
            .transpose()?;
        let engine = py
            .detach(|| ObjectStoreEngine::from_url_with_cache(url, options, token_cache))
            .map_err(to_py_err)?;
        Ok(Self { engine })
    }

    #[getter]
    fn scheme(&self) -> &str {
        self.engine.scheme()
    }

    fn to_sync(&self) -> PySyncObjectStore {
        PySyncObjectStore {
            engine: self.engine.clone(),
        }
    }

    #[pyo3(signature = (path, data, *, content_type=None, attributes=None, if_not_exists=false, if_match=None, if_none_match=None, checksum=false, expected_sha256=None, expected_crc32c=None, expected_md5=None))]
    #[allow(clippy::too_many_arguments)]
    fn put(
        &self,
        py: Python<'_>,
        path: String,
        data: &Bound<'_, PyAny>,
        content_type: Option<String>,
        attributes: Option<HashMap<String, String>>,
        if_not_exists: bool,
        if_match: Option<String>,
        if_none_match: Option<String>,
        checksum: bool,
        expected_sha256: Option<String>,
        expected_crc32c: Option<String>,
        expected_md5: Option<String>,
    ) -> PyResult<RustTaskAwaitable> {
        let payload = extract_buffer_bytes(py, data)?;
        let opts = PutConditionalOptions {
            if_not_exists,
            if_match,
            if_none_match,
            content_type,
            attributes: attributes.unwrap_or_default(),
            checksum,
            expected_sha256,
            expected_crc32c,
            expected_md5,
        };
        let engine = self.engine.clone();
        spawn_rust_task(
            py,
            async move { engine.put_opts(&path, payload, opts).await },
            |py, meta| Ok(Py::new(py, PyObjectMeta::from(meta))?.into_any()),
        )
    }

    #[pyo3(signature = (path, file_path, *, checksum=false, expected_sha256=None, expected_crc32c=None, expected_md5=None))]
    #[allow(clippy::too_many_arguments)]
    fn put_file(
        &self,
        py: Python<'_>,
        path: String,
        file_path: String,
        checksum: bool,
        expected_sha256: Option<String>,
        expected_crc32c: Option<String>,
        expected_md5: Option<String>,
    ) -> PyResult<RustTaskAwaitable> {
        let opts = PutConditionalOptions {
            checksum,
            expected_sha256,
            expected_crc32c,
            expected_md5,
            ..Default::default()
        };
        let engine = self.engine.clone();
        spawn_rust_task(
            py,
            async move { engine.put_file_opts(&path, &file_path, opts).await },
            |py, meta| Ok(Py::new(py, PyObjectMeta::from(meta))?.into_any()),
        )
    }

    #[pyo3(signature = (path, file_path, *, expected_sha256=None))]
    fn get_file(
        &self,
        py: Python<'_>,
        path: String,
        file_path: String,
        expected_sha256: Option<String>,
    ) -> PyResult<RustTaskAwaitable> {
        let engine = self.engine.clone();
        spawn_rust_task(
            py,
            async move {
                match expected_sha256.as_deref() {
                    Some(expected) => engine.get_file_verified(&path, &file_path, expected).await,
                    None => engine.get_file(&path, &file_path).await,
                }
            },
            |py, meta| Ok(Py::new(py, PyObjectMeta::from(meta))?.into_any()),
        )
    }

    #[pyo3(signature = (pairs, *, concurrency=16))]
    fn get_files(
        &self,
        py: Python<'_>,
        pairs: Vec<(String, String)>,
        concurrency: i64,
    ) -> PyResult<RustTaskAwaitable> {
        let concurrency = validate_concurrency(concurrency)?;
        let engine = self.engine.clone();
        spawn_rust_task(
            py,
            async move { engine.get_files(pairs, concurrency).await },
            metas_to_py_list,
        )
    }

    #[pyo3(signature = (pairs, *, concurrency=16))]
    fn put_files(
        &self,
        py: Python<'_>,
        pairs: Vec<(String, String)>,
        concurrency: i64,
    ) -> PyResult<RustTaskAwaitable> {
        let concurrency = validate_concurrency(concurrency)?;
        let engine = self.engine.clone();
        spawn_rust_task(
            py,
            async move { engine.put_files(pairs, concurrency).await },
            metas_to_py_list,
        )
    }

    #[pyo3(signature = (sources, dst, *, content_type=None))]
    fn compose(
        &self,
        py: Python<'_>,
        sources: Vec<String>,
        dst: String,
        content_type: Option<String>,
    ) -> PyResult<RustTaskAwaitable> {
        let engine = self.engine.clone();
        spawn_rust_task(
            py,
            async move { engine.compose(sources, &dst, content_type).await },
            |py, meta| Ok(Py::new(py, PyObjectMeta::from(meta))?.into_any()),
        )
    }

    #[pyo3(signature = (path, *, content_type=None, attributes=None, if_not_exists=false, if_match=None, if_none_match=None, checksum=false, expected_sha256=None, expected_crc32c=None, expected_md5=None))]
    #[allow(clippy::too_many_arguments)]
    fn create_multipart(
        &self,
        py: Python<'_>,
        path: String,
        content_type: Option<String>,
        attributes: Option<HashMap<String, String>>,
        if_not_exists: bool,
        if_match: Option<String>,
        if_none_match: Option<String>,
        checksum: bool,
        expected_sha256: Option<String>,
        expected_crc32c: Option<String>,
        expected_md5: Option<String>,
    ) -> PyResult<RustTaskAwaitable> {
        let opts = PutConditionalOptions {
            if_not_exists,
            if_match,
            if_none_match,
            content_type,
            attributes: attributes.unwrap_or_default(),
            checksum,
            expected_sha256,
            expected_crc32c,
            expected_md5,
        };
        let engine = self.engine.clone();
        spawn_rust_task(
            py,
            async move { engine.create_multipart(&path, opts).await },
            |py, session| {
                Ok(Py::new(
                    py,
                    PyAsyncMultipartUpload {
                        session: Arc::new(tokio::sync::Mutex::new(session)),
                    },
                )?
                .into_any())
            },
        )
    }

    fn put_multipart(
        &self,
        py: Python<'_>,
        path: String,
        parts: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<RustTaskAwaitable> {
        let mut chunks = Vec::with_capacity(parts.len());
        for part in &parts {
            chunks.push(extract_buffer_bytes(py, part)?);
        }
        let engine = self.engine.clone();
        spawn_rust_task(
            py,
            async move { engine.put_multipart(&path, chunks).await },
            |py, meta| Ok(Py::new(py, PyObjectMeta::from(meta))?.into_any()),
        )
    }

    #[pyo3(signature = (path, *, expected_sha256=None))]
    fn get(
        &self,
        py: Python<'_>,
        path: String,
        expected_sha256: Option<String>,
    ) -> PyResult<RustTaskAwaitable> {
        let engine = self.engine.clone();
        spawn_rust_task(
            py,
            async move {
                match expected_sha256.as_deref() {
                    Some(expected) => engine.get_verified(&path, expected).await,
                    None => engine.get(&path).await,
                }
            },
            |py, data| Ok(PyBytes::new(py, &data).into_any().unbind()),
        )
    }

    fn get_buffer(&self, py: Python<'_>, path: String) -> PyResult<RustTaskAwaitable> {
        let engine = self.engine.clone();
        spawn_rust_task(py, async move { engine.get(&path).await }, |py, data| {
            Ok(Py::new(py, StorageBuffer { data })?.into_any())
        })
    }

    #[pyo3(signature = (path, *, chunk_size=None, expected_sha256=None))]
    fn get_stream(
        &self,
        path: String,
        chunk_size: Option<usize>,
        expected_sha256: Option<String>,
    ) -> PyResult<PyAsyncStreamReader> {
        let chunk_size = validate_chunk_size(chunk_size)?;
        if let Some(ref expected) = expected_sha256 {
            validate_expected_hex("expected_sha256", expected, 64).map_err(to_py_err)?;
        }
        Ok(PyAsyncStreamReader {
            state: Arc::new(tokio::sync::Mutex::new(AsyncStreamState {
                init: AsyncStreamInit::Pending {
                    engine: self.engine.clone(),
                    path,
                    expected_sha256,
                },
                pending: Bytes::new(),
                chunk_size,
            })),
        })
    }

    fn get_range(
        &self,
        py: Python<'_>,
        path: String,
        offset: u64,
        length: u64,
    ) -> PyResult<RustTaskAwaitable> {
        let engine = self.engine.clone();
        spawn_rust_task(
            py,
            async move { engine.get_range(&path, offset, length).await },
            |py, data| Ok(PyBytes::new(py, &data).into_any().unbind()),
        )
    }

    fn head(&self, py: Python<'_>, path: String) -> PyResult<RustTaskAwaitable> {
        let engine = self.engine.clone();
        spawn_rust_task(py, async move { engine.head(&path).await }, |py, meta| {
            Ok(Py::new(py, PyObjectMeta::from(meta))?.into_any())
        })
    }

    fn exists(&self, py: Python<'_>, path: String) -> PyResult<RustTaskAwaitable> {
        let engine = self.engine.clone();
        spawn_rust_task(
            py,
            async move { engine.exists(&path).await },
            |py, exists| Ok(exists.into_pyobject(py)?.to_owned().into_any().unbind()),
        )
    }

    fn delete(&self, py: Python<'_>, path: String) -> PyResult<RustTaskAwaitable> {
        let engine = self.engine.clone();
        spawn_rust_task(py, async move { engine.delete(&path).await }, |py, ()| {
            Ok(py.None())
        })
    }

    #[pyo3(signature = (src, dst, *, if_not_exists=false))]
    fn copy(
        &self,
        py: Python<'_>,
        src: String,
        dst: String,
        if_not_exists: bool,
    ) -> PyResult<RustTaskAwaitable> {
        let engine = self.engine.clone();
        spawn_rust_task(
            py,
            async move { engine.copy(&src, &dst, if_not_exists).await },
            |py, ()| Ok(py.None()),
        )
    }

    #[pyo3(signature = (src, dst, *, if_not_exists=false))]
    fn rename(
        &self,
        py: Python<'_>,
        src: String,
        dst: String,
        if_not_exists: bool,
    ) -> PyResult<RustTaskAwaitable> {
        let engine = self.engine.clone();
        spawn_rust_task(
            py,
            async move { engine.rename(&src, &dst, if_not_exists).await },
            |py, ()| Ok(py.None()),
        )
    }

    #[pyo3(signature = (prefix=None))]
    fn list(&self, py: Python<'_>, prefix: Option<String>) -> PyResult<RustTaskAwaitable> {
        let engine = self.engine.clone();
        spawn_rust_task(
            py,
            async move { engine.list(prefix.as_deref()).await },
            |py, items| {
                let converted: Vec<PyObjectMeta> =
                    items.into_iter().map(PyObjectMeta::from).collect();
                Ok(converted.into_pyobject(py)?.into_any().unbind())
            },
        )
    }

    #[pyo3(signature = (prefix=None, offset=None, limit=1000))]
    fn list_page(
        &self,
        py: Python<'_>,
        prefix: Option<String>,
        offset: Option<String>,
        limit: usize,
    ) -> PyResult<RustTaskAwaitable> {
        if limit == 0 {
            return Err(PyValueError::new_err("limit must be >= 1"));
        }
        let engine = self.engine.clone();
        spawn_rust_task(
            py,
            async move {
                engine
                    .list_page(prefix.as_deref(), offset.as_deref(), limit)
                    .await
            },
            |py, (items, cursor)| {
                let converted: Vec<PyObjectMeta> =
                    items.into_iter().map(PyObjectMeta::from).collect();
                Ok((converted, cursor).into_pyobject(py)?.into_any().unbind())
            },
        )
    }

    #[pyo3(signature = (prefix=None))]
    fn list_with_delimiter(
        &self,
        py: Python<'_>,
        prefix: Option<String>,
    ) -> PyResult<RustTaskAwaitable> {
        let engine = self.engine.clone();
        spawn_rust_task(
            py,
            async move { engine.list_with_delimiter(prefix.as_deref()).await },
            |py, (items, prefixes)| {
                let converted: Vec<PyObjectMeta> =
                    items.into_iter().map(PyObjectMeta::from).collect();
                Ok((converted, prefixes).into_pyobject(py)?.into_any().unbind())
            },
        )
    }
}

#[pyfunction]
#[pyo3(signature = (base_url, secret, method, expires_in_secs=3600, timestamp=None))]
fn py_sign_hmac_url(
    py: Python<'_>,
    base_url: &str,
    secret: &str,
    method: &str,
    expires_in_secs: u64,
    timestamp: Option<i64>,
) -> PyResult<String> {
    let timestamp = timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp());
    py.detach(|| {
        let signer = HmacSigner::new(secret.as_bytes());
        signer.sign_url(base_url, method, expires_in_secs, timestamp)
    })
    .map_err(to_py_err)
}

#[pyfunction]
fn py_verify_hmac_url(
    py: Python<'_>,
    signed_url: &str,
    secret: &str,
    method: &str,
    timestamp: i64,
) -> PyResult<bool> {
    py.detach(|| {
        let signer = HmacSigner::new(secret.as_bytes());
        signer.verify_url(signed_url, method, timestamp)
    })
    .map_err(to_py_err)
}

#[pyfunction]
#[pyo3(signature = (bucket, region, access_key_id, secret_access_key, method, object_path, expires_in_secs=3600, timestamp=None, endpoint=None, virtual_hosted_style=false))]
#[allow(clippy::too_many_arguments)]
fn py_sign_s3_v4_url(
    py: Python<'_>,
    bucket: &str,
    region: &str,
    access_key_id: &str,
    secret_access_key: &str,
    method: &str,
    object_path: &str,
    expires_in_secs: u64,
    timestamp: Option<i64>,
    endpoint: Option<String>,
    virtual_hosted_style: bool,
) -> PyResult<String> {
    let timestamp = timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp());
    py.detach(|| {
        let cfg = S3SignConfig {
            bucket: bucket.to_string(),
            region: region.to_string(),
            access_key_id: access_key_id.to_string(),
            secret_access_key: secret_access_key.to_string(),
            endpoint,
            virtual_hosted_style,
        };
        sign_s3_v4_url(&cfg, method, object_path, expires_in_secs, timestamp)
    })
    .map_err(to_py_err)
}

#[pyfunction]
#[pyo3(signature = (bucket, region, access_key_id, secret_access_key, method, signed_url, timestamp, endpoint=None, virtual_hosted_style=false))]
#[allow(clippy::too_many_arguments)]
fn py_verify_s3_v4_url(
    py: Python<'_>,
    bucket: &str,
    region: &str,
    access_key_id: &str,
    secret_access_key: &str,
    method: &str,
    signed_url: &str,
    timestamp: i64,
    endpoint: Option<String>,
    virtual_hosted_style: bool,
) -> PyResult<bool> {
    py.detach(|| {
        let cfg = S3SignConfig {
            bucket: bucket.to_string(),
            region: region.to_string(),
            access_key_id: access_key_id.to_string(),
            secret_access_key: secret_access_key.to_string(),
            endpoint,
            virtual_hosted_style,
        };
        verify_s3_v4_url(&cfg, method, signed_url, timestamp)
    })
    .map_err(to_py_err)
}

#[pyfunction]
#[pyo3(signature = (bucket, service_account_email, method, object_path, expires_in_secs=3600, timestamp=None, iam_endpoint="https://iamcredentials.googleapis.com", access_token=None, private_key_pem=None, endpoint=None, token_cache_capsule=None))]
#[allow(clippy::too_many_arguments)]
fn py_sign_gcs_v4_iam_url(
    py: Python<'_>,
    bucket: &str,
    service_account_email: &str,
    method: &str,
    object_path: &str,
    expires_in_secs: u64,
    timestamp: Option<i64>,
    iam_endpoint: &str,
    access_token: Option<String>,
    private_key_pem: Option<String>,
    endpoint: Option<String>,
    token_cache_capsule: Option<&Bound<'_, PyAny>>,
) -> PyResult<String> {
    let token_cache = token_cache_capsule
        .map(extract_token_cache_capsule)
        .transpose()?;
    let timestamp = timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp());
    let cfg = GcsSignConfig {
        bucket: bucket.to_string(),
        service_account_email: service_account_email.to_string(),
        private_key_pem,
        access_token,
        endpoint,
    };
    py.detach(|| {
        block_on_storage(async move {
            sign_gcs_v4_with_iam_endpoint_and_cache(
                &cfg,
                token_cache.as_ref(),
                method,
                object_path,
                expires_in_secs,
                timestamp,
                iam_endpoint,
            )
            .await
        })
    })
    .map_err(to_py_err)
}

#[pyfunction]
#[pyo3(signature = (tenancy_ocid, user_ocid, fingerprint, private_key_pem, method, url, body=None, content_type=None, timestamp=None))]
#[allow(clippy::too_many_arguments)]
fn py_sign_oci_cavage_headers(
    py: Python<'_>,
    tenancy_ocid: &str,
    user_ocid: &str,
    fingerprint: &str,
    private_key_pem: &str,
    method: &str,
    url: &str,
    body: Option<&[u8]>,
    content_type: Option<&str>,
    timestamp: Option<i64>,
) -> PyResult<HashMap<String, String>> {
    let timestamp = timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp());
    let creds = OciCavageCredentials {
        tenancy_ocid: tenancy_ocid.to_string(),
        user_ocid: user_ocid.to_string(),
        fingerprint: fingerprint.to_string(),
        private_key_pem: private_key_pem.to_string(),
    };
    let signed = py
        .detach(|| sign_oci_cavage_request(&creds, method, url, body, content_type, timestamp))
        .map_err(to_py_err)?;
    let mut headers = HashMap::new();
    headers.insert("Authorization".to_string(), signed.authorization);
    headers.insert("Date".to_string(), signed.date);
    headers.insert("Host".to_string(), signed.host);
    if let Some(sha) = signed.x_content_sha256 {
        headers.insert("x-content-sha256".to_string(), sha);
    }
    if let Some(ct) = signed.content_type {
        headers.insert("content-type".to_string(), ct);
    }
    if let Some(cl) = signed.content_length {
        headers.insert("content-length".to_string(), cl);
    }
    Ok(headers)
}

#[pyfunction]
#[pyo3(signature = (account, container, access_key, method, blob_path, expires_in_secs=3600, timestamp=None, endpoint=None))]
#[allow(clippy::too_many_arguments)]
fn py_sign_azure_sas_url(
    py: Python<'_>,
    account: &str,
    container: &str,
    access_key: &str,
    method: &str,
    blob_path: &str,
    expires_in_secs: u64,
    timestamp: Option<i64>,
    endpoint: Option<String>,
) -> PyResult<String> {
    let timestamp = timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp());
    let cfg = AzureSasConfig {
        account: account.to_string(),
        container: container.to_string(),
        access_key_b64: access_key.to_string(),
        endpoint,
    };
    py.detach(|| sign_azure_sas_url(&cfg, method, blob_path, expires_in_secs, timestamp))
        .map_err(to_py_err)
}

#[pyfunction]
#[pyo3(signature = (data, part_size=8388608))]
fn py_chunk_r2_uniform_parts(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    part_size: usize,
) -> PyResult<Vec<StorageBuffer>> {
    let payload = extract_buffer_bytes(py, data)?;
    let parts = py.detach(|| chunk_uniform_parts(payload, part_size));
    Ok(parts
        .into_iter()
        .map(|d| StorageBuffer { data: d })
        .collect())
}

#[pyfunction]
#[pyo3(signature = (service, region, access_key_id, secret_access_key, method, url, payload, content_type="application/json", timestamp=None, session_token=None))]
#[allow(clippy::too_many_arguments)]
fn py_sign_aws_sigv4_headers(
    py: Python<'_>,
    service: &str,
    region: &str,
    access_key_id: &str,
    secret_access_key: &str,
    method: &str,
    url: &str,
    payload: &[u8],
    content_type: &str,
    timestamp: Option<i64>,
    session_token: Option<String>,
) -> PyResult<HashMap<String, String>> {
    let now_ts = timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp());
    let cfg = AwsSigV4HeaderConfig {
        service: service.to_string(),
        region: region.to_string(),
        access_key_id: access_key_id.to_string(),
        secret_access_key: secret_access_key.to_string(),
        session_token,
    };
    py.detach(|| sign_aws_sigv4_headers(&cfg, method, url, payload, content_type, now_ts))
        .map_err(to_py_err)
}

#[pyfunction]
#[pyo3(signature = (bucket, service_account_email, method, object_path, expires_in_secs=3600, timestamp=None, iam_endpoint="https://iamcredentials.googleapis.com", access_token=None, private_key_pem=None, endpoint=None, token_cache_capsule=None))]
#[allow(clippy::too_many_arguments)]
fn py_sign_gcs_v4_iam_url_async(
    py: Python<'_>,
    bucket: String,
    service_account_email: String,
    method: String,
    object_path: String,
    expires_in_secs: u64,
    timestamp: Option<i64>,
    iam_endpoint: &str,
    access_token: Option<String>,
    private_key_pem: Option<String>,
    endpoint: Option<String>,
    token_cache_capsule: Option<&Bound<'_, PyAny>>,
) -> PyResult<RustTaskAwaitable> {
    let token_cache = token_cache_capsule
        .map(extract_token_cache_capsule)
        .transpose()?;
    let timestamp = timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp());
    let iam_endpoint = iam_endpoint.to_string();
    let cfg = GcsSignConfig {
        bucket,
        service_account_email,
        private_key_pem,
        access_token,
        endpoint,
    };
    spawn_rust_task(
        py,
        async move {
            sign_gcs_v4_with_iam_endpoint_and_cache(
                &cfg,
                token_cache.as_ref(),
                &method,
                &object_path,
                expires_in_secs,
                timestamp,
                &iam_endpoint,
            )
            .await
        },
        |py, signed| Ok(signed.into_pyobject(py)?.into_any().unbind()),
    )
}

#[pyfunction]
#[pyo3(signature = (bucket, namespace, region, par_token, object_path, expires_in_secs=3600, endpoint=None))]
#[allow(clippy::too_many_arguments)]
fn sign_oci_par_url(
    py: Python<'_>,
    bucket: &str,
    namespace: &str,
    region: &str,
    par_token: &str,
    object_path: &str,
    expires_in_secs: u64,
    endpoint: Option<String>,
) -> PyResult<String> {
    let cfg = OciParConfig {
        region: region.to_string(),
        namespace: namespace.to_string(),
        bucket: bucket.to_string(),
        par_token: par_token.to_string(),
        endpoint,
    };
    py.detach(|| build_oci_par_url(&cfg, object_path, expires_in_secs))
        .map_err(PyErr::from)
}

#[pyfunction]
#[pyo3(signature = (bucket, service_account_email, method, object_path, expires_in_secs=3600, timestamp=None, endpoint=None))]
#[allow(clippy::too_many_arguments)]
fn build_gcs_v4_canonical_request(
    py: Python<'_>,
    bucket: &str,
    service_account_email: &str,
    method: &str,
    object_path: &str,
    expires_in_secs: u64,
    timestamp: Option<i64>,
    endpoint: Option<&str>,
) -> PyResult<(String, String, String, String)> {
    let now_unix = timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp());
    let req = py
        .detach(|| {
            crate::signing::build_gcs_v4_canonical_request(
                bucket,
                service_account_email,
                method,
                object_path,
                expires_in_secs,
                now_unix,
                endpoint,
            )
        })
        .map_err(PyErr::from)?;
    Ok((
        req.canonical_uri,
        req.canonical_querystring,
        req.canonical_request,
        req.string_to_sign,
    ))
}

#[pyfunction]
#[pyo3(signature = (canonical_uri, canonical_querystring, signature_hex, endpoint=None))]
fn assemble_gcs_iam_v4_url(
    py: Python<'_>,
    canonical_uri: &str,
    canonical_querystring: &str,
    signature_hex: &str,
    endpoint: Option<&str>,
) -> PyResult<String> {
    let (base_url, _, prefix) = resolve_gcs_endpoint_origin(endpoint).map_err(PyErr::from)?;
    let resolved_uri = if !prefix.is_empty()
        && !canonical_uri.starts_with(&format!("/{prefix}/"))
        && canonical_uri != format!("/{prefix}")
    {
        format!("/{prefix}{canonical_uri}")
    } else {
        canonical_uri.to_string()
    };
    let canonical = GcsV4CanonicalRequest {
        canonical_uri: resolved_uri,
        canonical_querystring: canonical_querystring.to_string(),
        canonical_request: String::new(),
        string_to_sign: String::new(),
        base_url,
    };
    py.detach(|| crate::signing::assemble_gcs_iam_v4_url(&canonical, signature_hex))
        .map_err(PyErr::from)
}

#[pymodule(gil_used = false)]
pub fn _storage(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySyncObjectStore>()?;
    m.add_class::<PyAsyncObjectStore>()?;
    m.add_class::<PyObjectMeta>()?;
    m.add_class::<PySyncStreamReader>()?;
    m.add_class::<PyAsyncStreamReader>()?;
    m.add_class::<PySyncMultipartUpload>()?;
    m.add_class::<PyAsyncMultipartUpload>()?;
    m.add_class::<RustTaskAwaitable>()?;
    m.add_class::<StorageBuffer>()?;
    m.add_function(wrap_pyfunction!(py_sign_hmac_url, m)?)?;
    m.add_function(wrap_pyfunction!(py_verify_hmac_url, m)?)?;
    m.add_function(wrap_pyfunction!(py_sign_s3_v4_url, m)?)?;
    m.add_function(wrap_pyfunction!(py_verify_s3_v4_url, m)?)?;
    m.add_function(wrap_pyfunction!(py_sign_gcs_v4_iam_url, m)?)?;
    m.add_function(wrap_pyfunction!(py_sign_gcs_v4_iam_url_async, m)?)?;
    m.add_function(wrap_pyfunction!(py_sign_oci_cavage_headers, m)?)?;
    m.add_function(wrap_pyfunction!(py_sign_azure_sas_url, m)?)?;
    m.add_function(wrap_pyfunction!(py_chunk_r2_uniform_parts, m)?)?;
    m.add_function(wrap_pyfunction!(py_sign_aws_sigv4_headers, m)?)?;
    m.add_function(wrap_pyfunction!(sign_oci_par_url, m)?)?;
    m.add_function(wrap_pyfunction!(build_gcs_v4_canonical_request, m)?)?;
    m.add_function(wrap_pyfunction!(assemble_gcs_iam_v4_url, m)?)?;
    Ok(())
}
