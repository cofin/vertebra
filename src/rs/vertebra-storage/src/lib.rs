//! `vertebra-storage` crate and `vertebra._storage` PyO3 extension module.

pub mod signing;
pub mod store;

use std::collections::HashMap;

use bytes::Bytes;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyFileNotFoundError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::signing::{
    GcsSignConfig, HmacSigner, S3SignConfig, sign_gcs_v4_with_iam_endpoint, sign_s3_v4_url,
    verify_s3_v4_url,
};
use crate::store::{ObjectStoreEngine, StorageError, StoredObjectMeta, block_on_storage};

fn to_py_err(err: StorageError) -> PyErr {
    match err {
        StorageError::InvalidUrl(u, msg) => PyValueError::new_err(format!("{u}: {msg}")),
        StorageError::ObjectStore(object_store::Error::NotFound { path, source }) => {
            PyFileNotFoundError::new_err(format!("Object not found: {path} ({source})"))
        }
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

fn extract_buffer_bytes(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Bytes> {
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
pub struct NativeObjectStore {
    engine: ObjectStoreEngine,
}

#[pymethods]
impl NativeObjectStore {
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

#[pyfunction]
#[pyo3(signature = (base_url, secret, method, expires_in_secs=3600, timestamp=1700000000))]
fn py_sign_hmac_url(
    py: Python<'_>,
    base_url: &str,
    secret: &str,
    method: &str,
    expires_in_secs: u64,
    timestamp: i64,
) -> PyResult<String> {
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
#[pyo3(signature = (bucket, region, access_key_id, secret_access_key, method, object_path, expires_in_secs=3600, timestamp=1700000000, endpoint=None))]
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
    timestamp: i64,
    endpoint: Option<String>,
) -> PyResult<String> {
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
#[pyo3(signature = (bucket, service_account_email, method, object_path, expires_in_secs=3600, timestamp=1700000000, iam_endpoint="https://iamcredentials.googleapis.com", access_token=None))]
#[allow(clippy::too_many_arguments)]
fn py_sign_gcs_v4_iam_url(
    py: Python<'_>,
    bucket: &str,
    service_account_email: &str,
    method: &str,
    object_path: &str,
    expires_in_secs: u64,
    timestamp: i64,
    iam_endpoint: &str,
    access_token: Option<String>,
) -> PyResult<String> {
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

#[pymodule(gil_used = false)]
pub fn _storage(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NativeObjectStore>()?;
    m.add_class::<ObjectMeta>()?;
    m.add_function(wrap_pyfunction!(py_sign_hmac_url, m)?)?;
    m.add_function(wrap_pyfunction!(py_verify_hmac_url, m)?)?;
    m.add_function(wrap_pyfunction!(py_sign_s3_v4_url, m)?)?;
    m.add_function(wrap_pyfunction!(py_verify_s3_v4_url, m)?)?;
    m.add_function(wrap_pyfunction!(py_sign_gcs_v4_iam_url, m)?)?;
    Ok(())
}
