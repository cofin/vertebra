//! `vertebra-storage` crate and `vertebra._storage` PyO3 extension module.

pub mod oci;
pub mod r2;
pub mod signing;
pub mod store;

use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;

use bytes::Bytes;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyFileNotFoundError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

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
#[pyclass(name = "StorageBuffer", frozen, from_py_object)]
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
        let slice = &slf.borrow().data[..];
        let ptr = if slice.is_empty() {
            std::ptr::NonNull::<u8>::dangling().as_ptr()
        } else {
            slice.as_ptr()
        };
        let ret = pyo3::ffi::PyBuffer_FillInfo(
            view,
            slf.into_any().into_ptr(),
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

#[pyclass(name = "_TaskCompletionCallback", frozen)]
struct TaskCompletionCallback {
    future: Py<PyAny>,
    outcome: Mutex<Option<PyResult<Py<PyAny>>>>,
}

#[pymethods]
impl TaskCompletionCallback {
    fn __call__(&self, py: Python<'_>) -> PyResult<()> {
        let mut guard = self.outcome.lock().expect("lock task completion outcome");
        if let Some(res) = guard.take() {
            let fut = self.future.bind(py);
            let cancelled: bool = fut
                .call_method0("cancelled")
                .and_then(|v| v.extract())
                .unwrap_or(false);
            if cancelled {
                return Ok(());
            }
            match res {
                Ok(val) => {
                    let _ = fut.call_method1("set_result", (val,));
                }
                Err(err) => {
                    let _ = fut.call_method1("set_exception", (err.into_value(py),));
                }
            }
        }
        Ok(())
    }
}

/// Native Rust awaitable backed by the shared Tokio runtime without `asyncio.to_thread`.
#[pyclass(name = "RustTaskAwaitable", frozen)]
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
    let loop_ref = loop_obj.unbind();
    let fut_for_cb = py_fut.clone().unbind();
    let fut_for_await = py_fut.unbind();

    runtime_handle().spawn(async move {
        let output = fut.await;
        Python::attach(|py| {
            let mapped = match output {
                Ok(val) => map_fn(py, val),
                Err(err) => Err(to_py_err(err)),
            };
            if let Ok(cb) = Py::new(
                py,
                TaskCompletionCallback {
                    future: fut_for_cb,
                    outcome: Mutex::new(Some(mapped)),
                },
            ) {
                let _ = loop_ref
                    .bind(py)
                    .call_method1("call_soon_threadsafe", (cb,));
            }
        });
    });

    Ok(RustTaskAwaitable {
        future: fut_for_await,
    })
}

#[pyclass(frozen, from_py_object)]
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

#[pyclass(frozen)]
pub struct ObjectStore {
    engine: ObjectStoreEngine,
}

#[pymethods]
impl ObjectStore {
    #[new]
    #[pyo3(signature = (url, options=None))]
    fn new(url: &str, options: Option<HashMap<String, String>>) -> PyResult<Self> {
        let engine = ObjectStoreEngine::from_url(url, options.as_ref()).map_err(to_py_err)?;
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

    fn put_async(
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

    fn put_file(&self, py: Python<'_>, path: &str, file_path: &str) -> PyResult<ObjectMeta> {
        let engine = self.engine.clone();
        let meta = py
            .detach(|| block_on_storage(async move { engine.put_file(path, file_path).await }))
            .map_err(to_py_err)?;
        Ok(ObjectMeta::from(meta))
    }

    fn put_file_async(
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

    fn get_async(&self, py: Python<'_>, path: String) -> PyResult<RustTaskAwaitable> {
        let engine = self.engine.clone();
        spawn_rust_task(py, async move { engine.get(&path).await }, |py, data| {
            Ok(PyBytes::new(py, &data).into_any().unbind())
        })
    }

    fn get_buffer_async(&self, py: Python<'_>, path: String) -> PyResult<RustTaskAwaitable> {
        let engine = self.engine.clone();
        spawn_rust_task(py, async move { engine.get(&path).await }, |py, data| {
            Ok(Py::new(py, StorageBuffer { data })?.into_any())
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

    fn get_range_async(
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

    fn head(&self, py: Python<'_>, path: &str) -> PyResult<ObjectMeta> {
        let engine = self.engine.clone();
        let meta = py
            .detach(|| block_on_storage(async move { engine.head(path).await }))
            .map_err(to_py_err)?;
        Ok(ObjectMeta::from(meta))
    }

    fn head_async(&self, py: Python<'_>, path: String) -> PyResult<RustTaskAwaitable> {
        let engine = self.engine.clone();
        spawn_rust_task(py, async move { engine.head(&path).await }, |py, meta| {
            Ok(Py::new(py, ObjectMeta::from(meta))?.into_any())
        })
    }

    fn exists(&self, py: Python<'_>, path: &str) -> PyResult<bool> {
        let engine = self.engine.clone();
        py.detach(|| block_on_storage(async move { engine.exists(path).await }))
            .map_err(to_py_err)
    }

    fn exists_async(&self, py: Python<'_>, path: String) -> PyResult<RustTaskAwaitable> {
        let engine = self.engine.clone();
        spawn_rust_task(
            py,
            async move { engine.exists(&path).await },
            |py, exists| Ok(exists.into_pyobject(py)?.to_owned().into_any().unbind()),
        )
    }

    fn delete(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        let engine = self.engine.clone();
        py.detach(|| block_on_storage(async move { engine.delete(path).await }))
            .map_err(to_py_err)
    }

    fn delete_async(&self, py: Python<'_>, path: String) -> PyResult<RustTaskAwaitable> {
        let engine = self.engine.clone();
        spawn_rust_task(py, async move { engine.delete(&path).await }, |py, ()| {
            Ok(py.None())
        })
    }

    fn copy(&self, py: Python<'_>, src: &str, dst: &str) -> PyResult<()> {
        let engine = self.engine.clone();
        py.detach(|| block_on_storage(async move { engine.copy(src, dst).await }))
            .map_err(to_py_err)
    }

    fn copy_async(&self, py: Python<'_>, src: String, dst: String) -> PyResult<RustTaskAwaitable> {
        let engine = self.engine.clone();
        spawn_rust_task(
            py,
            async move { engine.copy(&src, &dst).await },
            |py, ()| Ok(py.None()),
        )
    }

    #[pyo3(signature = (prefix=None))]
    fn list(&self, py: Python<'_>, prefix: Option<&str>) -> PyResult<Vec<ObjectMeta>> {
        let engine = self.engine.clone();
        let items = py
            .detach(|| block_on_storage(async move { engine.list(prefix).await }))
            .map_err(to_py_err)?;
        Ok(items.into_iter().map(ObjectMeta::from).collect())
    }

    #[pyo3(signature = (prefix=None))]
    fn list_async(&self, py: Python<'_>, prefix: Option<String>) -> PyResult<RustTaskAwaitable> {
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
#[pyo3(signature = (bucket, service_account_email, method, object_path, expires_in_secs=3600, timestamp=None, iam_endpoint="https://iamcredentials.googleapis.com", access_token=None))]
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
) -> PyResult<String> {
    let timestamp = timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp());
    let cfg = GcsSignConfig {
        bucket: bucket.to_string(),
        service_account_email: service_account_email.to_string(),
        private_key_pem: None,
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

#[pymodule(gil_used = false)]
pub fn _storage(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ObjectStore>()?;
    m.add_class::<ObjectMeta>()?;
    m.add_class::<RustTaskAwaitable>()?;
    m.add_class::<StorageBuffer>()?;
    m.add_function(wrap_pyfunction!(py_sign_hmac_url, m)?)?;
    m.add_function(wrap_pyfunction!(py_verify_hmac_url, m)?)?;
    m.add_function(wrap_pyfunction!(py_sign_s3_v4_url, m)?)?;
    m.add_function(wrap_pyfunction!(py_verify_s3_v4_url, m)?)?;
    m.add_function(wrap_pyfunction!(py_sign_gcs_v4_iam_url, m)?)?;
    m.add_function(wrap_pyfunction!(py_sign_oci_cavage_headers, m)?)?;
    m.add_function(wrap_pyfunction!(py_sign_azure_sas_url, m)?)?;
    m.add_function(wrap_pyfunction!(py_chunk_r2_uniform_parts, m)?)?;
    m.add_function(wrap_pyfunction!(py_sign_aws_sigv4_headers, m)?)?;
    Ok(())
}
