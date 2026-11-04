//! `vertebra-storage` crate and `vertebra._storage` PyO3 extension module.

pub mod gcp_auth;
pub mod oci;
pub mod r2;
pub mod signing;
pub mod store;

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyFileNotFoundError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use vertebra_core::notify::{AsyncCompletionQueue, Notifier};

use crate::gcp_auth::{
    GcpTokenCache as NativeGcpTokenCache, GcpTokenSnapshot as NativeGcpTokenSnapshot,
    current_unix_ms,
};
use crate::oci::{OciCavageCredentials, sign_oci_cavage_request};
use crate::r2::chunk_uniform_parts;
use crate::signing::{
    AwsSigV4HeaderConfig, AzureSasConfig, GcsSignConfig, HmacSigner, S3SignConfig,
    sign_aws_sigv4_headers, sign_azure_sas_url, sign_gcs_v4_with_iam_endpoint, sign_s3_v4_url,
    verify_s3_v4_url,
};
use crate::store::{
    ObjectStoreEngine, StorageError, StoredObjectMeta, block_on_storage, runtime_handle,
};

fn to_py_err(err: StorageError) -> PyErr {
    match err {
        StorageError::InvalidUrl(u, msg) => PyValueError::new_err(format!("{u}: {msg}")),
        StorageError::ObjectStore(object_store::Error::NotFound { path, source }) => {
            PyFileNotFoundError::new_err(format!("Object not found: {path} ({source})"))
        }
        StorageError::Io(io_err) if io_err.kind() == std::io::ErrorKind::NotFound => {
            PyFileNotFoundError::new_err(io_err.to_string())
        }
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

fn extract_buffer_bytes(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Bytes> {
    if let Ok(storage_buf) = data.cast::<StorageBuffer>() {
        return Ok(storage_buf.borrow().data.clone());
    }
    if let Ok(py_bytes) = data.cast::<PyBytes>() {
        return Ok(Bytes::copy_from_slice(py_bytes.as_bytes()));
    }
    let buf = PyBuffer::<u8>::get(data)?;
    if !buf.is_c_contiguous() {
        return Err(PyValueError::new_err("buffer must be C-contiguous"));
    }
    let slice = unsafe { std::slice::from_raw_parts(buf.buf_ptr() as *const u8, buf.len_bytes()) };
    let copied = py.detach(|| Bytes::copy_from_slice(slice));
    Ok(copied)
}

/// Zero-copy read-only Python buffer wrapping Rust `bytes::Bytes`.
#[pyclass(name = "StorageBuffer", frozen, immutable_type, from_py_object)]
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

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut pyo3::ffi::Py_buffer,
        flags: std::os::raw::c_int,
    ) -> PyResult<()> {
        let py = slf.py();
        let slice = &slf.get().data[..];
        let ptr = if slice.is_empty() {
            std::ptr::NonNull::<u8>::dangling().as_ptr()
        } else {
            slice.as_ptr()
        };
        let ret = pyo3::ffi::PyBuffer_FillInfo(
            view,
            slf.as_ptr(),
            ptr as *mut std::os::raw::c_void,
            slice.len() as isize,
            1,
            flags,
        );
        if ret == -1 {
            return Err(PyErr::fetch(py));
        }
        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn __releasebuffer__(&self, _view: *mut pyo3::ffi::Py_buffer) {}
}

type OutcomeProducer = Box<dyn FnOnce(Python<'_>) -> PyResult<Py<PyAny>> + Send>;

struct CompletedStorageTask {
    future: Py<PyAny>,
    produce: OutcomeProducer,
}

struct RustCompletionRing {
    completion: AsyncCompletionQueue<CompletedStorageTask>,
    fallback_wakeup: Mutex<Option<(Py<PyAny>, Py<PyAny>)>>,
}

impl RustCompletionRing {
    fn signal_fallback(&self) {
        if self.completion.notifier().is_some() {
            return;
        }
        let pair = self
            .fallback_wakeup
            .lock()
            .expect("lock storage fallback wakeup")
            .as_ref()
            .map(|(loop_ref, drainer_ref)| {
                Python::attach(|py| (loop_ref.clone_ref(py), drainer_ref.clone_ref(py)))
            });
        if let Some((loop_ref, drainer_ref)) = pair {
            Python::attach(|py| {
                let _ = loop_ref
                    .bind(py)
                    .call_method1("call_soon_threadsafe", (&drainer_ref,));
            });
        }
    }
}

#[pyclass(name = "_StorageCompletionDrainer", frozen, immutable_type)]
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
                match (item.produce)(py) {
                    Ok(val) => {
                        let _ = fut.call_method1("set_result", (val,));
                    }
                    Err(err) => {
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
        return Ok(Arc::clone(&drainer.borrow().ring));
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
            let _ = loop_obj.setattr(ATTR_NAME, drainer_py.bind(py));
            return Ok(temp_ring);
        }
    }

    let ring = Arc::new(RustCompletionRing {
        completion: AsyncCompletionQueue::new(None),
        fallback_wakeup: Mutex::new(None),
    });
    let drainer_py = Py::new(
        py,
        StorageCompletionDrainer {
            ring: Arc::clone(&ring),
        },
    )?;
    *ring
        .fallback_wakeup
        .lock()
        .expect("lock storage fallback wakeup") = Some((
        loop_obj.clone().unbind(),
        drainer_py.clone_ref(py).into_any(),
    ));
    let _ = loop_obj.setattr(ATTR_NAME, drainer_py.bind(py));
    Ok(ring)
}

/// Native Rust awaitable backed by the shared Tokio runtime without `asyncio.to_thread`.
#[pyclass(name = "RustTaskAwaitable", frozen, immutable_type)]
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
    let fut_for_await = py_fut.unbind();

    runtime_handle().spawn(async move {
        let output = fut.await;
        let should_signal = ring.completion.push(CompletedStorageTask {
            future: fut_for_cb,
            produce: Box::new(move |py| match output {
                Ok(val) => map_fn(py, val),
                Err(err) => Err(to_py_err(err)),
            }),
        });
        if should_signal {
            ring.signal_fallback();
        }
    });

    Ok(RustTaskAwaitable {
        future: fut_for_await,
    })
}

#[pyclass(name = "GcpTokenSnapshot", frozen, immutable_type, from_py_object)]
#[derive(Clone, Debug)]
pub struct PyGcpTokenSnapshot {
    #[pyo3(get)]
    pub access_token: String,
    #[pyo3(get)]
    pub expires_at_unix_ms: u64,
    #[pyo3(get)]
    pub generation: u64,
    #[pyo3(get)]
    pub quota_project_id: Option<String>,
}

impl From<&NativeGcpTokenSnapshot> for PyGcpTokenSnapshot {
    fn from(value: &NativeGcpTokenSnapshot) -> Self {
        Self {
            access_token: value.access_token.clone(),
            expires_at_unix_ms: value.expires_at_unix_ms,
            generation: value.generation,
            quota_project_id: value.quota_project_id.clone(),
        }
    }
}

#[pymethods]
impl PyGcpTokenSnapshot {
    #[new]
    #[pyo3(signature = (*, access_token, expires_at_unix_ms, generation, quota_project_id=None))]
    fn new(
        access_token: String,
        expires_at_unix_ms: u64,
        generation: u64,
        quota_project_id: Option<String>,
    ) -> Self {
        Self {
            access_token,
            expires_at_unix_ms,
            generation,
            quota_project_id,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "GcpTokenSnapshot(generation={}, expires_at_unix_ms={}, quota_project_id={:?})",
            self.generation, self.expires_at_unix_ms, self.quota_project_id
        )
    }
}

#[pyclass(name = "SyncGcpTokenCache", frozen, immutable_type)]
pub struct PySyncGcpTokenCache {
    inner: Arc<NativeGcpTokenCache>,
}

#[pymethods]
impl PySyncGcpTokenCache {
    #[new]
    #[pyo3(signature = (options))]
    fn new(py: Python<'_>, options: HashMap<String, String>) -> PyResult<Self> {
        let inner = py
            .detach(|| {
                NativeGcpTokenCache::from_options(&options, crate::gcp_auth::GCS_OAUTH_SCOPES)
            })
            .ok_or_else(|| {
                PyValueError::new_err("unable to configure GcpTokenCache from options")
            })?;
        Ok(Self { inner })
    }

    #[getter]
    fn generation(&self) -> u64 {
        self.inner.generation()
    }

    #[getter]
    fn refresh_skew_ms(&self) -> u64 {
        self.inner.refresh_skew_ms()
    }

    #[getter]
    fn has_service_account_key(&self) -> bool {
        self.inner.service_account_key().is_some()
    }

    #[getter]
    fn service_account_email(&self) -> Option<String> {
        self.inner
            .service_account_key()
            .map(|k| k.client_email.clone())
    }

    #[pyo3(signature = (*, now_unix_ms=None))]
    fn needs_refresh(&self, now_unix_ms: Option<u64>) -> bool {
        self.inner
            .needs_refresh(now_unix_ms.unwrap_or_else(current_unix_ms))
    }

    fn invalidate(&self, failed_generation: u64) {
        self.inner.invalidate(failed_generation);
    }

    fn sign_jwt_for_audience(&self, py: Python<'_>, audience: &str) -> PyResult<String> {
        py.detach(|| self.inner.sign_jwt_for_audience(audience))
            .map_err(PyValueError::new_err)
    }

    #[pyo3(signature = (*, force_refresh=false))]
    fn get_token(&self, py: Python<'_>, force_refresh: bool) -> PyResult<PyGcpTokenSnapshot> {
        if !force_refresh && let Some(snap) = self.inner.get_cached(current_unix_ms()) {
            return Ok(PyGcpTokenSnapshot::from(snap.as_ref()));
        }
        let cache = Arc::clone(&self.inner);
        let snap = py
            .detach(|| {
                block_on_storage(
                    async move { cache.get_or_refresh_with_force(force_refresh).await },
                )
            })
            .map_err(PyRuntimeError::new_err)?;
        Ok(PyGcpTokenSnapshot::from(snap.as_ref()))
    }
}

#[pyclass(name = "AsyncGcpTokenCache", frozen, immutable_type)]
pub struct PyAsyncGcpTokenCache {
    inner: Arc<NativeGcpTokenCache>,
}

#[pymethods]
impl PyAsyncGcpTokenCache {
    #[new]
    #[pyo3(signature = (options))]
    fn new(py: Python<'_>, options: HashMap<String, String>) -> PyResult<Self> {
        let inner = py
            .detach(|| {
                NativeGcpTokenCache::from_options(&options, crate::gcp_auth::GCS_OAUTH_SCOPES)
            })
            .ok_or_else(|| {
                PyValueError::new_err("unable to configure GcpTokenCache from options")
            })?;
        Ok(Self { inner })
    }

    #[getter]
    fn generation(&self) -> u64 {
        self.inner.generation()
    }

    #[getter]
    fn refresh_skew_ms(&self) -> u64 {
        self.inner.refresh_skew_ms()
    }

    #[getter]
    fn has_service_account_key(&self) -> bool {
        self.inner.service_account_key().is_some()
    }

    #[getter]
    fn service_account_email(&self) -> Option<String> {
        self.inner
            .service_account_key()
            .map(|k| k.client_email.clone())
    }

    #[pyo3(signature = (*, now_unix_ms=None))]
    fn needs_refresh(&self, now_unix_ms: Option<u64>) -> bool {
        self.inner
            .needs_refresh(now_unix_ms.unwrap_or_else(current_unix_ms))
    }

    fn invalidate(&self, failed_generation: u64) {
        self.inner.invalidate(failed_generation);
    }

    fn sign_jwt_for_audience(&self, py: Python<'_>, audience: &str) -> PyResult<String> {
        py.detach(|| self.inner.sign_jwt_for_audience(audience))
            .map_err(PyValueError::new_err)
    }

    #[pyo3(signature = (*, force_refresh=false))]
    fn get_token(&self, py: Python<'_>, force_refresh: bool) -> PyResult<RustTaskAwaitable> {
        let cache = Arc::clone(&self.inner);
        spawn_rust_task(
            py,
            async move {
                cache
                    .get_or_refresh_with_force(force_refresh)
                    .await
                    .map_err(StorageError::Signing)
            },
            |py, snap| Ok(Py::new(py, PyGcpTokenSnapshot::from(snap.as_ref()))?.into_any()),
        )
    }
}

#[pyclass(frozen, immutable_type, from_py_object)]
#[derive(Clone, Debug)]
pub struct ObjectMeta {
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
}

impl From<StoredObjectMeta> for ObjectMeta {
    fn from(value: StoredObjectMeta) -> Self {
        Self {
            path: value.path,
            size: value.size,
            last_modified: value.last_modified,
            etag: value.etag,
            version: value.version,
        }
    }
}

#[pymethods]
impl ObjectMeta {
    fn __repr__(&self) -> String {
        format!("ObjectMeta(path={:?}, size={})", self.path, self.size)
    }
}

#[pyclass(name = "SyncObjectStore", frozen, immutable_type)]
pub struct SyncObjectStore {
    engine: ObjectStoreEngine,
}

#[pymethods]
impl SyncObjectStore {
    #[new]
    #[pyo3(signature = (url, options=None))]
    fn new(py: Python<'_>, url: &str, options: Option<HashMap<String, String>>) -> PyResult<Self> {
        let engine = py
            .detach(|| ObjectStoreEngine::from_url(url, options.as_ref()))
            .map_err(to_py_err)?;
        Ok(Self { engine })
    }

    #[getter]
    fn scheme(&self) -> &str {
        self.engine.scheme()
    }

    fn put(&self, py: Python<'_>, path: &str, data: &Bound<'_, PyAny>) -> PyResult<ObjectMeta> {
        let payload = extract_buffer_bytes(py, data)?;
        let engine = self.engine.clone();
        let meta = py
            .detach(|| block_on_storage(async move { engine.put(path, payload).await }))
            .map_err(to_py_err)?;
        Ok(ObjectMeta::from(meta))
    }

    fn put_file(&self, py: Python<'_>, path: &str, file_path: &str) -> PyResult<ObjectMeta> {
        let engine = self.engine.clone();
        let meta = py
            .detach(|| block_on_storage(async move { engine.put_file(path, file_path).await }))
            .map_err(to_py_err)?;
        Ok(ObjectMeta::from(meta))
    }

    fn put_multipart(
        &self,
        py: Python<'_>,
        path: &str,
        parts: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<ObjectMeta> {
        let mut chunks = Vec::with_capacity(parts.len());
        for part in &parts {
            chunks.push(extract_buffer_bytes(py, part)?);
        }
        let engine = self.engine.clone();
        let meta = py
            .detach(|| block_on_storage(async move { engine.put_multipart(path, chunks).await }))
            .map_err(to_py_err)?;
        Ok(ObjectMeta::from(meta))
    }

    fn get<'py>(&self, py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyBytes>> {
        let engine = self.engine.clone();
        let data = py
            .detach(|| block_on_storage(async move { engine.get(path).await }))
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

    fn head(&self, py: Python<'_>, path: &str) -> PyResult<ObjectMeta> {
        let engine = self.engine.clone();
        let meta = py
            .detach(|| block_on_storage(async move { engine.head(path).await }))
            .map_err(to_py_err)?;
        Ok(ObjectMeta::from(meta))
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

    fn copy(&self, py: Python<'_>, src: &str, dst: &str) -> PyResult<()> {
        let engine = self.engine.clone();
        py.detach(|| block_on_storage(async move { engine.copy(src, dst).await }))
            .map_err(to_py_err)
    }

    #[pyo3(signature = (prefix=None))]
    fn list(&self, py: Python<'_>, prefix: Option<&str>) -> PyResult<Vec<ObjectMeta>> {
        let engine = self.engine.clone();
        let items = py
            .detach(|| block_on_storage(async move { engine.list(prefix).await }))
            .map_err(to_py_err)?;
        Ok(items.into_iter().map(ObjectMeta::from).collect())
    }
}

#[pyclass(name = "AsyncObjectStore", frozen, immutable_type)]
pub struct AsyncObjectStore {
    engine: ObjectStoreEngine,
}

#[pymethods]
impl AsyncObjectStore {
    #[new]
    #[pyo3(signature = (url, options=None))]
    fn new(py: Python<'_>, url: &str, options: Option<HashMap<String, String>>) -> PyResult<Self> {
        let engine = py
            .detach(|| ObjectStoreEngine::from_url(url, options.as_ref()))
            .map_err(to_py_err)?;
        Ok(Self { engine })
    }

    #[getter]
    fn scheme(&self) -> &str {
        self.engine.scheme()
    }

    fn put(
        &self,
        py: Python<'_>,
        path: String,
        data: &Bound<'_, PyAny>,
    ) -> PyResult<RustTaskAwaitable> {
        let payload = extract_buffer_bytes(py, data)?;
        let engine = self.engine.clone();
        spawn_rust_task(
            py,
            async move { engine.put(&path, payload).await },
            |py, meta| Ok(Py::new(py, ObjectMeta::from(meta))?.into_any()),
        )
    }

    fn put_file(
        &self,
        py: Python<'_>,
        path: String,
        file_path: String,
    ) -> PyResult<RustTaskAwaitable> {
        let engine = self.engine.clone();
        spawn_rust_task(
            py,
            async move { engine.put_file(&path, &file_path).await },
            |py, meta| Ok(Py::new(py, ObjectMeta::from(meta))?.into_any()),
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
            |py, meta| Ok(Py::new(py, ObjectMeta::from(meta))?.into_any()),
        )
    }

    fn get(&self, py: Python<'_>, path: String) -> PyResult<RustTaskAwaitable> {
        let engine = self.engine.clone();
        spawn_rust_task(py, async move { engine.get(&path).await }, |py, data| {
            Ok(PyBytes::new(py, &data).into_any().unbind())
        })
    }

    fn get_buffer(&self, py: Python<'_>, path: String) -> PyResult<RustTaskAwaitable> {
        let engine = self.engine.clone();
        spawn_rust_task(py, async move { engine.get(&path).await }, |py, data| {
            Ok(Py::new(py, StorageBuffer { data })?.into_any())
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
            Ok(Py::new(py, ObjectMeta::from(meta))?.into_any())
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

    fn copy(&self, py: Python<'_>, src: String, dst: String) -> PyResult<RustTaskAwaitable> {
        let engine = self.engine.clone();
        spawn_rust_task(
            py,
            async move { engine.copy(&src, &dst).await },
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
                let converted: Vec<ObjectMeta> = items.into_iter().map(ObjectMeta::from).collect();
                Ok(converted.into_pyobject(py)?.into_any().unbind())
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
#[pyo3(signature = (bucket, region, access_key_id, secret_access_key, method, object_path, expires_in_secs=3600, timestamp=None, endpoint=None))]
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
) -> PyResult<String> {
    let timestamp = timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp());
    py.detach(|| {
        let cfg = S3SignConfig {
            bucket: bucket.to_string(),
            region: region.to_string(),
            access_key_id: access_key_id.to_string(),
            secret_access_key: secret_access_key.to_string(),
            endpoint,
        };
        sign_s3_v4_url(&cfg, method, object_path, expires_in_secs, timestamp)
    })
    .map_err(to_py_err)
}

#[pyfunction]
#[pyo3(signature = (bucket, region, access_key_id, secret_access_key, method, signed_url, timestamp, endpoint=None))]
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
) -> PyResult<bool> {
    py.detach(|| {
        let cfg = S3SignConfig {
            bucket: bucket.to_string(),
            region: region.to_string(),
            access_key_id: access_key_id.to_string(),
            secret_access_key: secret_access_key.to_string(),
            endpoint,
        };
        verify_s3_v4_url(&cfg, method, signed_url, timestamp)
    })
    .map_err(to_py_err)
}

#[pyfunction]
#[pyo3(signature = (bucket, service_account_email, method, object_path, expires_in_secs=3600, timestamp=None, iam_endpoint="https://iamcredentials.googleapis.com", access_token=None, private_key_pem=None))]
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
) -> PyResult<String> {
    let timestamp = timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp());
    let cfg = GcsSignConfig {
        bucket: bucket.to_string(),
        service_account_email: service_account_email.to_string(),
        private_key_pem,
        access_token,
    };
    py.detach(|| {
        block_on_storage(async move {
            sign_gcs_v4_with_iam_endpoint(
                &cfg,
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
#[pyo3(signature = (bucket, service_account_email, method, object_path, expires_in_secs=3600, timestamp=None, iam_endpoint="https://iamcredentials.googleapis.com", access_token=None, private_key_pem=None))]
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
) -> PyResult<RustTaskAwaitable> {
    let timestamp = timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp());
    let iam_endpoint = iam_endpoint.to_string();
    let cfg = GcsSignConfig {
        bucket,
        service_account_email,
        private_key_pem,
        access_token,
    };
    spawn_rust_task(
        py,
        async move {
            sign_gcs_v4_with_iam_endpoint(
                &cfg,
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

#[pymodule(gil_used = false)]
pub fn _storage(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<SyncObjectStore>()?;
    m.add_class::<AsyncObjectStore>()?;
    m.add_class::<ObjectMeta>()?;
    m.add_class::<RustTaskAwaitable>()?;
    m.add_class::<StorageBuffer>()?;
    m.add_class::<PyGcpTokenSnapshot>()?;
    m.add_class::<PySyncGcpTokenCache>()?;
    m.add_class::<PyAsyncGcpTokenCache>()?;
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
    Ok(())
}
