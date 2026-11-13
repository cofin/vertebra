use std::collections::HashMap;
use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use vertebra_core::gcp_auth::current_unix_ms;
use vertebra_core::gcp_identity::{
    GcpIdTokenCache as NativeGcpIdTokenCache, GcpIdTokenSnapshot as NativeGcpIdTokenSnapshot,
    validate_id_token_audience,
};

use crate::async_bridge::{RustDbTaskAwaitable, block_on_db, spawn_rust_db_task};
use crate::errors::auth_error_to_py;

#[pyclass(name = "GcpIdTokenSnapshot", frozen, immutable_type, from_py_object)]
#[derive(Clone)]
pub struct PyGcpIdTokenSnapshot {
    #[pyo3(get)]
    pub id_token: String,
    #[pyo3(get)]
    pub audience: String,
    #[pyo3(get)]
    pub expires_at_unix_ms: u64,
    #[pyo3(get)]
    pub generation: u64,
}

impl From<&NativeGcpIdTokenSnapshot> for PyGcpIdTokenSnapshot {
    fn from(snap: &NativeGcpIdTokenSnapshot) -> Self {
        Self {
            id_token: snap.id_token.clone(),
            audience: snap.audience.clone(),
            expires_at_unix_ms: snap.expires_at_unix_ms,
            generation: snap.generation,
        }
    }
}

#[pymethods]
impl PyGcpIdTokenSnapshot {
    #[new]
    #[pyo3(signature = (*, id_token, audience, expires_at_unix_ms, generation))]
    fn new(id_token: String, audience: String, expires_at_unix_ms: u64, generation: u64) -> Self {
        Self {
            id_token,
            audience,
            expires_at_unix_ms,
            generation,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "GcpIdTokenSnapshot(audience={:?}, expires_at_unix_ms={}, generation={}, id_token='[REDACTED]')",
            self.audience, self.expires_at_unix_ms, self.generation
        )
    }
}

fn build_id_token_cache(
    py: Python<'_>,
    options: Option<HashMap<String, String>>,
    allow_ambient_adc: bool,
) -> PyResult<Arc<NativeGcpIdTokenCache>> {
    let mut opts = options.unwrap_or_default();
    if !allow_ambient_adc {
        opts.insert("allow_ambient_adc".to_string(), "false".to_string());
    }
    py.detach(|| NativeGcpIdTokenCache::from_options(&opts))
        .map_err(PyValueError::new_err)
}

#[pyclass(name = "SyncGcpIdTokenCache", frozen, immutable_type)]
pub struct PySyncGcpIdTokenCache {
    inner: Arc<NativeGcpIdTokenCache>,
}

#[pymethods]
impl PySyncGcpIdTokenCache {
    #[new]
    #[pyo3(signature = (options=None, *, allow_ambient_adc=true))]
    fn new(
        py: Python<'_>,
        options: Option<HashMap<String, String>>,
        allow_ambient_adc: bool,
    ) -> PyResult<Self> {
        let inner = build_id_token_cache(py, options, allow_ambient_adc)?;
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

    #[pyo3(signature = (audience, *, now_unix_ms=None))]
    fn needs_refresh(&self, audience: &str, now_unix_ms: Option<u64>) -> PyResult<bool> {
        validate_id_token_audience(audience).map_err(PyValueError::new_err)?;
        Ok(self
            .inner
            .needs_refresh(audience, now_unix_ms.unwrap_or_else(current_unix_ms)))
    }

    fn invalidate(&self, audience: &str, failed_generation: u64) {
        self.inner.invalidate(audience, failed_generation);
    }

    fn to_async(&self) -> PyAsyncGcpIdTokenCache {
        PyAsyncGcpIdTokenCache {
            inner: Arc::clone(&self.inner),
        }
    }

    #[pyo3(signature = (audience, *, force_refresh=false))]
    fn get_token(
        &self,
        py: Python<'_>,
        audience: &str,
        force_refresh: bool,
    ) -> PyResult<PyGcpIdTokenSnapshot> {
        let aud = validate_id_token_audience(audience)
            .map_err(PyValueError::new_err)?
            .to_string();
        if !force_refresh && let Some(snap) = self.inner.get_cached(&aud, current_unix_ms()) {
            return Ok(PyGcpIdTokenSnapshot::from(snap.as_ref()));
        }
        let cache = Arc::clone(&self.inner);
        let snap = py
            .detach(|| block_on_db(async move { cache.get_or_refresh(&aud, force_refresh).await }))
            .map_err(|err| auth_error_to_py(py, err))?;
        Ok(PyGcpIdTokenSnapshot::from(snap.as_ref()))
    }
}

#[pyclass(name = "AsyncGcpIdTokenCache", frozen, immutable_type)]
pub struct PyAsyncGcpIdTokenCache {
    inner: Arc<NativeGcpIdTokenCache>,
}

#[pymethods]
impl PyAsyncGcpIdTokenCache {
    #[new]
    #[pyo3(signature = (options=None, *, allow_ambient_adc=true))]
    fn new(
        py: Python<'_>,
        options: Option<HashMap<String, String>>,
        allow_ambient_adc: bool,
    ) -> PyResult<Self> {
        let inner = build_id_token_cache(py, options, allow_ambient_adc)?;
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

    #[pyo3(signature = (audience, *, now_unix_ms=None))]
    fn needs_refresh(&self, audience: &str, now_unix_ms: Option<u64>) -> PyResult<bool> {
        validate_id_token_audience(audience).map_err(PyValueError::new_err)?;
        Ok(self
            .inner
            .needs_refresh(audience, now_unix_ms.unwrap_or_else(current_unix_ms)))
    }

    fn invalidate(&self, audience: &str, failed_generation: u64) {
        self.inner.invalidate(audience, failed_generation);
    }

    fn to_sync(&self) -> PySyncGcpIdTokenCache {
        PySyncGcpIdTokenCache {
            inner: Arc::clone(&self.inner),
        }
    }

    #[pyo3(signature = (audience, *, force_refresh=false))]
    fn get_token(
        &self,
        py: Python<'_>,
        audience: &str,
        force_refresh: bool,
    ) -> PyResult<RustDbTaskAwaitable> {
        let aud = validate_id_token_audience(audience)
            .map_err(PyValueError::new_err)?
            .to_string();
        let cache = Arc::clone(&self.inner);
        spawn_rust_db_task(
            py,
            async move { cache.get_or_refresh(&aud, force_refresh).await },
            |py, snap| Ok(Py::new(py, PyGcpIdTokenSnapshot::from(snap.as_ref()))?.into_any()),
            auth_error_to_py,
        )
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyGcpIdTokenSnapshot>()?;
    m.add_class::<PySyncGcpIdTokenCache>()?;
    m.add_class::<PyAsyncGcpIdTokenCache>()?;
    Ok(())
}
