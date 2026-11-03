//! PyO3 bindings for `vertebra_core::gcp_auth` (`GcpTokenCache` and `GcpTokenSnapshot`).

use std::collections::HashMap;
use std::sync::Arc;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use vertebra_core::gcp_auth::{
    GcpTokenCache as NativeGcpTokenCache, GcpTokenSnapshot as NativeGcpTokenSnapshot,
    current_unix_ms,
};

use crate::completion::{RustDbTaskAwaitable, block_on_db, spawn_rust_db_task};

/// Immutable snapshot of a cached Google Cloud OAuth2 bearer token.
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

/// Lock-free Google Cloud OAuth2 token cache with single-flight refresh and audience JWT signing.
#[pyclass(name = "GcpTokenCache", frozen, immutable_type)]
pub struct PyGcpTokenCache {
    inner: Arc<NativeGcpTokenCache>,
}

impl PyGcpTokenCache {
    pub fn inner(&self) -> &Arc<NativeGcpTokenCache> {
        &self.inner
    }
}

#[pymethods]
impl PyGcpTokenCache {
    #[new]
    #[pyo3(signature = (options))]
    fn new(py: Python<'_>, options: HashMap<String, String>) -> PyResult<Self> {
        let inner = py
            .detach(|| {
                NativeGcpTokenCache::from_options(
                    &options,
                    vertebra_core::gcp_auth::DEFAULT_GCP_SCOPES,
                )
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
                block_on_db(async move { cache.get_or_refresh_with_force(force_refresh).await })
            })
            .map_err(PyRuntimeError::new_err)?;
        Ok(PyGcpTokenSnapshot::from(snap.as_ref()))
    }

    #[pyo3(signature = (*, force_refresh=false))]
    fn get_token_async(
        &self,
        py: Python<'_>,
        force_refresh: bool,
    ) -> PyResult<RustDbTaskAwaitable> {
        let cache = Arc::clone(&self.inner);
        spawn_rust_db_task(
            py,
            async move { cache.get_or_refresh_with_force(force_refresh).await },
            |py, snap| Ok(Py::new(py, PyGcpTokenSnapshot::from(snap.as_ref()))?.into_any()),
            PyRuntimeError::new_err,
        )
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyGcpTokenSnapshot>()?;
    m.add_class::<PyGcpTokenCache>()?;
    Ok(())
}
