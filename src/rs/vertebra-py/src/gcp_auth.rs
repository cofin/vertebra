//! PyO3 bindings for `vertebra_core::gcp_auth` (`GcpTokenCache` and `GcpTokenSnapshot`).

use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyCapsule};
use vertebra_core::gcp_auth::{
    GCP_TOKEN_CACHE_CAPSULE_NAME, GcpRefreshCompleteFn, GcpTokenCache as NativeGcpTokenCache,
    GcpTokenCacheCapsule, GcpTokenSnapshot as NativeGcpTokenSnapshot, current_unix_ms,
    gcp_http2_post as native_gcp_http2_post, gcp_http2_request as native_gcp_http2_request,
    invoke_capsule_refresh_on_runtime, validate_gcp_http_method,
};

use crate::async_bridge::{
    RustDbTaskAwaitable, block_on_db, db_runtime_handle, spawn_rust_db_task,
};
use crate::errors::{auth_error_to_py, exception_types};

// SAFETY: C-ABI callback registered inside `GcpTokenCacheCapsule`; callers must supply a live
// `Arc<NativeGcpTokenCache>` pointer as `ctx`, valid `out_ctx`, and valid `complete_cb` pointer.
unsafe extern "C" fn refresh_token_cache_on_db_runtime(
    ctx: *const c_void,
    force_refresh: bool,
    out_ctx: *mut c_void,
    complete_cb: GcpRefreshCompleteFn,
) {
    let runtime = db_runtime_handle();
    // SAFETY: `ctx` and `complete_cb` originate from `GcpTokenCacheCapsule` and `SharedGcpTokenCacheHandle`.
    unsafe {
        invoke_capsule_refresh_on_runtime(runtime, ctx, force_refresh, out_ctx, complete_cb);
    }
}

fn build_token_cache_pycapsule(
    py: Python<'_>,
    cache: &Arc<NativeGcpTokenCache>,
) -> PyResult<Py<PyCapsule>> {
    let capsule = GcpTokenCacheCapsule::new(Arc::clone(cache), refresh_token_cache_on_db_runtime);
    Ok(PyCapsule::new_with_value(py, capsule, GCP_TOKEN_CACHE_CAPSULE_NAME)?.unbind())
}

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

fn build_token_cache(
    py: Python<'_>,
    options: Option<HashMap<String, String>>,
    scopes: Option<Vec<String>>,
    allow_ambient_adc: bool,
) -> PyResult<Arc<NativeGcpTokenCache>> {
    let mut opts = options.unwrap_or_default();
    if !allow_ambient_adc {
        opts.entry("allow_ambient_adc".to_string())
            .or_insert_with(|| "false".to_string());
    }
    let scopes_vec: Option<Vec<&str>> = scopes
        .as_ref()
        .map(|s| s.iter().map(|x| x.as_str()).collect());
    let effective_scopes: &[&str] = scopes_vec
        .as_deref()
        .unwrap_or(vertebra_core::gcp_auth::DEFAULT_GCP_SCOPES);
    py.detach(|| NativeGcpTokenCache::from_options(&opts, effective_scopes))
        .ok_or_else(|| PyValueError::new_err("unable to configure GcpTokenCache from options"))
}

/// Synchronous lock-free Google Cloud OAuth2 token cache with single-flight refresh.
#[pyclass(name = "SyncGcpTokenCache", frozen, immutable_type)]
pub struct PySyncGcpTokenCache {
    inner: Arc<NativeGcpTokenCache>,
}

impl PySyncGcpTokenCache {
    pub fn inner(&self) -> &Arc<NativeGcpTokenCache> {
        &self.inner
    }
}

#[pymethods]
impl PySyncGcpTokenCache {
    #[new]
    #[pyo3(signature = (options=None, scopes=None, *, allow_ambient_adc=true))]
    fn new(
        py: Python<'_>,
        options: Option<HashMap<String, String>>,
        scopes: Option<Vec<String>>,
        allow_ambient_adc: bool,
    ) -> PyResult<Self> {
        let inner = build_token_cache(py, options, scopes, allow_ambient_adc)?;
        Ok(Self { inner })
    }

    fn to_async(&self) -> PyAsyncGcpTokenCache {
        PyAsyncGcpTokenCache {
            inner: Arc::clone(&self.inner),
        }
    }

    fn _capsule(&self, py: Python<'_>) -> PyResult<Py<PyCapsule>> {
        build_token_cache_pycapsule(py, &self.inner)
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
            .map_err(|e| auth_error_to_py(py, e))?;
        Ok(PyGcpTokenSnapshot::from(snap.as_ref()))
    }
}

/// Asynchronous lock-free Google Cloud OAuth2 token cache returning `RustDbTaskAwaitable`.
#[pyclass(name = "AsyncGcpTokenCache", frozen, immutable_type)]
pub struct PyAsyncGcpTokenCache {
    inner: Arc<NativeGcpTokenCache>,
}

impl PyAsyncGcpTokenCache {
    pub fn inner(&self) -> &Arc<NativeGcpTokenCache> {
        &self.inner
    }
}

#[pymethods]
impl PyAsyncGcpTokenCache {
    #[new]
    #[pyo3(signature = (options=None, scopes=None, *, allow_ambient_adc=true))]
    fn new(
        py: Python<'_>,
        options: Option<HashMap<String, String>>,
        scopes: Option<Vec<String>>,
        allow_ambient_adc: bool,
    ) -> PyResult<Self> {
        let inner = build_token_cache(py, options, scopes, allow_ambient_adc)?;
        Ok(Self { inner })
    }

    fn to_sync(&self) -> PySyncGcpTokenCache {
        PySyncGcpTokenCache {
            inner: Arc::clone(&self.inner),
        }
    }

    fn _capsule(&self, py: Python<'_>) -> PyResult<Py<PyCapsule>> {
        build_token_cache_pycapsule(py, &self.inner)
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
    fn get_token(&self, py: Python<'_>, force_refresh: bool) -> PyResult<RustDbTaskAwaitable> {
        let cache = Arc::clone(&self.inner);
        spawn_rust_db_task(
            py,
            async move { cache.get_or_refresh_with_force(force_refresh).await },
            |py, snap| Ok(Py::new(py, PyGcpTokenSnapshot::from(snap.as_ref()))?.into_any()),
            auth_error_to_py,
        )
    }
}

/// Execute a connection-pooled HTTP/2 or HTTP/1.1 unary POST request synchronously off the GIL.
#[pyfunction]
#[pyo3(signature = (url, headers, body, timeout_ms=30_000))]
pub fn gcp_http2_post_sync<'py>(
    py: Python<'py>,
    url: &str,
    headers: HashMap<String, String>,
    body: &[u8],
    timeout_ms: u64,
) -> PyResult<(u16, HashMap<String, String>, Bound<'py, PyBytes>)> {
    let url_owned = url.to_string();
    let payload = body.to_vec();
    let (status, resp_headers, resp_body) = py
        .detach(|| {
            block_on_db(async move {
                native_gcp_http2_post(&url_owned, &headers, payload, timeout_ms).await
            })
        })
        .map_err(|e| auth_error_to_py(py, e))?;
    Ok((status, resp_headers, PyBytes::new(py, &resp_body)))
}

fn gcp_error_to_py(py: Python<'_>, message: String) -> PyErr {
    match exception_types(py) {
        Ok(types) => {
            let err = PyErr::from_type(types.google_cloud.bind(py).clone(), (message.clone(),));
            let val = err.value(py);
            let (code, code_name) = if message.contains("timed out") {
                (4i32, "DEADLINE_EXCEEDED")
            } else {
                (14i32, "UNAVAILABLE")
            };
            if let Err(e) = val
                .setattr("code", code)
                .and_then(|()| val.setattr("code_name", code_name))
                .and_then(|()| val.setattr("message", message))
                .and_then(|()| val.setattr("http_status", Option::<u16>::None))
                .and_then(|()| val.setattr("retry_delay_ms", Option::<u64>::None))
                .and_then(|()| val.setattr("reason", Option::<String>::None))
            {
                return e;
            }
            err
        }
        Err(e) => e,
    }
}

fn validate_request_args(method: &str, timeout_ms: i64) -> PyResult<(&'static str, u64)> {
    let normalized_method = validate_gcp_http_method(method).map_err(PyValueError::new_err)?;
    if timeout_ms <= 0 {
        return Err(PyValueError::new_err("timeout_ms must be > 0"));
    }
    Ok((normalized_method, timeout_ms as u64))
}

/// Execute a connection-pooled HTTP/2 or HTTP/1.1 unary POST request asynchronously via `RustDbTaskAwaitable`.
#[pyfunction]
#[pyo3(signature = (url, headers, body, timeout_ms=30_000))]
pub fn gcp_http2_post_async(
    py: Python<'_>,
    url: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
    timeout_ms: u64,
) -> PyResult<RustDbTaskAwaitable> {
    spawn_rust_db_task(
        py,
        async move { native_gcp_http2_post(&url, &headers, body, timeout_ms).await },
        |py, (status, resp_headers, resp_body)| {
            let tuple = (status, resp_headers, PyBytes::new(py, &resp_body)).into_pyobject(py)?;
            Ok(tuple.into_any().unbind())
        },
        auth_error_to_py,
    )
}

/// Execute a connection-pooled HTTP/2 or HTTP/1.1 request synchronously off the GIL.
#[pyfunction]
#[pyo3(signature = (method, url, headers, body=None, timeout_ms=30_000))]
pub fn gcp_http2_request_sync<'py>(
    py: Python<'py>,
    method: &str,
    url: &str,
    headers: HashMap<String, String>,
    body: Option<&[u8]>,
    timeout_ms: i64,
) -> PyResult<(u16, HashMap<String, String>, Bound<'py, PyBytes>)> {
    let (normalized_method, timeout_u64) = validate_request_args(method, timeout_ms)?;
    let url_owned = url.to_string();
    let payload = body.map(<[u8]>::to_vec);
    let (status, resp_headers, resp_body) = py
        .detach(|| {
            block_on_db(async move {
                native_gcp_http2_request(
                    normalized_method,
                    &url_owned,
                    &headers,
                    payload,
                    timeout_u64,
                )
                .await
            })
        })
        .map_err(|e| gcp_error_to_py(py, e))?;
    Ok((status, resp_headers, PyBytes::new(py, &resp_body)))
}

/// Execute a connection-pooled HTTP/2 or HTTP/1.1 request asynchronously via `RustDbTaskAwaitable`.
#[pyfunction]
#[pyo3(signature = (method, url, headers, body=None, timeout_ms=30_000))]
pub fn gcp_http2_request_async(
    py: Python<'_>,
    method: &str,
    url: String,
    headers: HashMap<String, String>,
    body: Option<Vec<u8>>,
    timeout_ms: i64,
) -> PyResult<RustDbTaskAwaitable> {
    let (normalized_method, timeout_u64) = validate_request_args(method, timeout_ms)?;
    spawn_rust_db_task(
        py,
        async move {
            native_gcp_http2_request(normalized_method, &url, &headers, body, timeout_u64).await
        },
        |py, (status, resp_headers, resp_body)| {
            let tuple = (status, resp_headers, PyBytes::new(py, &resp_body)).into_pyobject(py)?;
            Ok(tuple.into_any().unbind())
        },
        gcp_error_to_py,
    )
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyGcpTokenSnapshot>()?;
    m.add_class::<PySyncGcpTokenCache>()?;
    m.add_class::<PyAsyncGcpTokenCache>()?;
    m.add_function(wrap_pyfunction!(gcp_http2_post_sync, m)?)?;
    m.add_function(wrap_pyfunction!(gcp_http2_post_async, m)?)?;
    m.add_function(wrap_pyfunction!(gcp_http2_request_sync, m)?)?;
    m.add_function(wrap_pyfunction!(gcp_http2_request_async, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn gcp_http2_post_roundtrip_with_local_http_server() {
        let result = block_on_db(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let addr = listener.local_addr().expect("addr");
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.expect("accept");
                let mut buf = vec![0u8; 4096];
                let _ = socket.read(&mut buf).await.expect("read");
                let resp = b"HTTP/1.1 200 OK\r\nContent-Type: application/grpc+proto\r\ngrpc-status: 0\r\nContent-Length: 5\r\n\r\n\x00\x00\x00\x00\x00";
                socket.write_all(resp).await.expect("write");
            });

            let mut headers = HashMap::new();
            headers.insert(
                "content-type".to_string(),
                "application/grpc+proto".to_string(),
            );
            let url = format!("http://{addr}/google.ai.generativelanguage.v1beta.GenerativeService/BatchEmbedContents");
            let out = native_gcp_http2_post(&url, &headers, vec![0, 0, 0, 0, 0], 5_000).await?;
            let _ = server.await;
            Ok::<_, String>(out)
        })
        .expect("gcp_http2_post succeeded");

        assert_eq!(result.0, 200);
        assert_eq!(result.1.get("grpc-status").map(String::as_str), Some("0"));
        assert_eq!(result.2, vec![0, 0, 0, 0, 0]);
    }

    #[test]
    fn gcp_http2_request_roundtrip_across_methods_and_optional_body() {
        let observed = block_on_db(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let addr = listener.local_addr().expect("addr");
            let server = tokio::spawn(async move {
                let mut captured = Vec::new();
                for _ in 0..5 {
                    let (mut socket, _) = listener.accept().await.expect("accept");
                    let mut buf = vec![0u8; 4096];
                    let n = socket.read(&mut buf).await.expect("read");
                    let raw = String::from_utf8_lossy(&buf[..n]).to_string();
                    captured.push(raw);
                    let resp = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Test-Resp: ok\r\nContent-Length: 9\r\n\r\n{\"ok\": 1}";
                    socket.write_all(resp).await.expect("write");
                }
                captured
            });

            let mut headers = HashMap::new();
            headers.insert("x-custom-hdr".to_string(), "val-1".to_string());
            let base = format!("http://{addr}/v1/entryGroups");

            let get_res = native_gcp_http2_request("get", &base, &headers, None, 5_000).await?;
            let del_res = native_gcp_http2_request("Delete", &base, &headers, None, 5_000).await?;
            let post_res = native_gcp_http2_request(
                "POST",
                &base,
                &headers,
                Some(b"{\"op\":\"create\"}".to_vec()),
                5_000,
            )
            .await?;
            let patch_res = native_gcp_http2_request(
                "patch",
                &base,
                &headers,
                Some(b"{\"op\":\"patch\"}".to_vec()),
                5_000,
            )
            .await?;
            let put_res = native_gcp_http2_request(
                "PUT",
                &base,
                &headers,
                Some(b"{\"op\":\"put\"}".to_vec()),
                5_000,
            )
            .await?;

            for res in [&get_res, &del_res, &post_res, &patch_res, &put_res] {
                assert_eq!(res.0, 200);
                assert_eq!(res.1.get("x-test-resp").map(String::as_str), Some("ok"));
                assert_eq!(res.2, b"{\"ok\": 1}");
            }

            let captured = server.await.expect("server join");
            Ok::<_, String>(captured)
        })
        .expect("gcp_http2_request roundtrips");

        assert_eq!(observed.len(), 5);
        assert!(observed[0].starts_with("GET /v1/entryGroups "));
        assert!(!observed[0].to_ascii_lowercase().contains("content-length:"));
        assert!(observed[0].ends_with("\r\n\r\n"));

        assert!(observed[1].starts_with("DELETE /v1/entryGroups "));
        assert!(!observed[1].to_ascii_lowercase().contains("content-length:"));
        assert!(observed[1].ends_with("\r\n\r\n"));

        assert!(observed[2].starts_with("POST /v1/entryGroups "));
        assert!(observed[2].contains("{\"op\":\"create\"}"));

        assert!(observed[3].starts_with("PATCH /v1/entryGroups "));
        assert!(observed[3].contains("{\"op\":\"patch\"}"));

        assert!(observed[4].starts_with("PUT /v1/entryGroups "));
        assert!(observed[4].contains("{\"op\":\"put\"}"));
    }

    #[test]
    fn gcp_http2_request_validates_method_and_timeout_before_network() {
        Python::attach(|py| {
            for bad_method in ["", "TRACE", "CONNECT", "HEAD", "INVALID"] {
                let err = validate_request_args(bad_method, 1_000).unwrap_err();
                assert!(err.is_instance_of::<PyValueError>(py));
            }
            for bad_timeout in [0, -1, -1000] {
                let err = validate_request_args("GET", bad_timeout).unwrap_err();
                assert!(err.is_instance_of::<PyValueError>(py));
            }
            assert_eq!(
                validate_request_args("patch", 500).expect("valid"),
                ("PATCH", 500)
            );
        });

        let err = block_on_db(async {
            let headers = HashMap::new();
            native_gcp_http2_request("TRACE", "http://127.0.0.1:9/unused", &headers, None, 1_000)
                .await
        })
        .unwrap_err();
        assert!(err.contains("unsupported HTTP method"));
    }

    #[test]
    fn gcp_http2_request_unreachable_endpoint_errors() {
        let addr = block_on_db(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            listener.local_addr().expect("addr")
        });
        let url = format!("http://{addr}/unreachable");
        let err = block_on_db(async {
            let headers = HashMap::new();
            native_gcp_http2_request("GET", &url, &headers, None, 2_000).await
        })
        .unwrap_err();
        assert!(err.contains("GCP HTTP/2 GET"));
    }
}
