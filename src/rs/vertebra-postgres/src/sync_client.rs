//! Synchronous physical PostgreSQL session client (`vertebra._postgres.SyncPostgresClient`).

use std::collections::HashMap;
use std::sync::Arc;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyString, PyTuple};
use vertebra_db::py_async::block_on_db;

use crate::config::{PostgresConfig, RawPostgresOptions};
use crate::errors::postgres_error_to_py;
use crate::session::{ClientBeginConnect, ClientSession};

/// Extract a `Mapping[str, str]` snapshot while attached to the Python runtime.
pub(crate) fn extract_string_map(
    obj: Option<&Bound<'_, PyAny>>,
    field_name: &str,
) -> PyResult<Option<HashMap<String, String>>> {
    let Some(mapping) = obj.filter(|v| !v.is_none()) else {
        return Ok(None);
    };
    if mapping.is_instance_of::<PyString>()
        || !mapping.hasattr("items")?
        || !mapping.hasattr("__getitem__")?
    {
        return Err(PyTypeError::new_err(format!(
            "{field_name} must be a Mapping[str, str]"
        )));
    }
    let items = mapping.call_method0("items").map_err(|_| {
        PyTypeError::new_err(format!("{field_name} must be a Mapping[str, str]"))
    })?;
    let mut out = HashMap::new();
    for entry in items.try_iter()? {
        let item = entry?;
        let pair = item.cast_into::<PyTuple>().map_err(|_| {
            PyTypeError::new_err(format!(
                "{field_name} items must be (str, str) pairs"
            ))
        })?;
        if pair.len() != 2 {
            return Err(PyTypeError::new_err(format!(
                "{field_name} items must be 2-tuples of (str, str)"
            )));
        }
        let k_obj = pair.get_item(0)?;
        let v_obj = pair.get_item(1)?;
        if !k_obj.is_instance_of::<PyString>() || !v_obj.is_instance_of::<PyString>() {
            return Err(PyTypeError::new_err(format!(
                "{field_name} keys and values must be str"
            )));
        }
        out.insert(k_obj.extract::<String>()?, v_obj.extract::<String>()?);
    }
    Ok(Some(out))
}

pub(crate) fn extract_strict_i64(obj: &Bound<'_, PyAny>) -> PyResult<i64> {
    if obj.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err("expected int, not bool"));
    }
    obj.extract::<i64>()
        .map_err(|_| PyTypeError::new_err("expected int"))
}

pub(crate) fn extract_strict_opt_i64(obj: &Bound<'_, PyAny>) -> PyResult<Option<i64>> {
    if obj.is_none() {
        return Ok(None);
    }
    extract_strict_i64(obj).map(Some)
}

pub(crate) fn extract_strict_f64(obj: &Bound<'_, PyAny>) -> PyResult<f64> {
    if obj.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err("expected float or int, not bool"));
    }
    obj.extract::<f64>()
        .map_err(|_| PyTypeError::new_err("expected float or int"))
}

pub(crate) fn extract_session_gucs(
    obj: &Bound<'_, PyAny>,
) -> PyResult<Option<HashMap<String, String>>> {
    extract_string_map(Some(obj), "session_gucs")
}

pub(crate) fn extract_cloud_auth_options(
    obj: &Bound<'_, PyAny>,
) -> PyResult<Option<HashMap<String, String>>> {
    extract_string_map(Some(obj), "cloud_auth_options")
}

pub(crate) fn extract_iam_auth_options(
    obj: &Bound<'_, PyAny>,
) -> PyResult<Option<HashMap<String, String>>> {
    extract_string_map(Some(obj), "iam_auth_options")
}

/// Shared 25-parameter constructor validator used by both `SyncPostgresClient` and `AsyncPostgresClient`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_validated_config(
    py: Python<'_>,
    user: String,
    database: String,
    host: String,
    port: i64,
    password: Option<String>,
    unix_socket: Option<String>,
    sslmode: String,
    ca_pem: Option<Vec<u8>>,
    client_cert_pem: Option<Vec<u8>>,
    client_key_pem: Option<Vec<u8>>,
    server_name: Option<String>,
    channel_binding: String,
    connect_timeout: f64,
    application_name: String,
    session_gucs: Option<HashMap<String, String>>,
    keepalives: bool,
    keepalives_idle: Option<i64>,
    keepalives_interval: Option<i64>,
    keepalives_count: Option<i64>,
    cloud_sql_instance: Option<String>,
    cloud_sql_ip_type: String,
    alloydb_instance_uri: Option<String>,
    alloydb_ip_type: String,
    enable_iam_auth: bool,
    cloud_auth_options: Option<HashMap<String, String>>,
    iam_auth_options: Option<HashMap<String, String>>,
) -> PyResult<PostgresConfig> {
    let raw = RawPostgresOptions {
        user,
        database,
        host,
        port,
        password,
        unix_socket,
        sslmode,
        ca_pem,
        client_cert_pem,
        client_key_pem,
        server_name,
        channel_binding,
        connect_timeout,
        application_name,
        session_gucs: session_gucs.unwrap_or_default(),
        keepalives,
        keepalives_idle,
        keepalives_interval,
        keepalives_count,
        cloud_sql_instance,
        cloud_sql_ip_type,
        alloydb_instance_uri,
        alloydb_ip_type,
        enable_iam_auth,
        cloud_auth_options: cloud_auth_options.unwrap_or_default(),
        iam_auth_options: iam_auth_options.unwrap_or_default(),
    };

    PostgresConfig::validate(raw).map_err(|e| postgres_error_to_py(py, e))
}

/// Physical synchronous native PostgreSQL client (`vertebra._postgres.SyncPostgresClient`).
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(
        name = "SyncPostgresClient",
        module = "vertebra._postgres",
        frozen,
        immutable_type,
        freelist = 128
    )
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(
        name = "SyncPostgresClient",
        module = "vertebra._postgres",
        frozen,
        immutable_type
    )
)]
pub struct PySyncPostgresClient {
    pub(crate) session: Arc<ClientSession>,
}

#[pymethods]
impl PySyncPostgresClient {
    #[new]
    #[pyo3(signature = (
        *,
        user,
        database,
        host = "localhost",
        port = 5432,
        password = None,
        unix_socket = None,
        sslmode = "verify-full",
        ca_pem = None,
        client_cert_pem = None,
        client_key_pem = None,
        server_name = None,
        channel_binding = "prefer",
        connect_timeout = 10.0,
        application_name = "vertebra",
        session_gucs = None,
        keepalives = true,
        keepalives_idle = None,
        keepalives_interval = None,
        keepalives_count = None,
        cloud_sql_instance = None,
        cloud_sql_ip_type = "PRIVATE",
        alloydb_instance_uri = None,
        alloydb_ip_type = "PRIVATE",
        enable_iam_auth = false,
        cloud_auth_options = None,
        iam_auth_options = None
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        py: Python<'_>,
        user: String,
        database: String,
        host: &str,
        #[pyo3(from_py_with = extract_strict_i64)] port: i64,
        password: Option<String>,
        unix_socket: Option<String>,
        sslmode: &str,
        ca_pem: Option<Vec<u8>>,
        client_cert_pem: Option<Vec<u8>>,
        client_key_pem: Option<Vec<u8>>,
        server_name: Option<String>,
        channel_binding: &str,
        #[pyo3(from_py_with = extract_strict_f64)] connect_timeout: f64,
        application_name: &str,
        #[pyo3(from_py_with = extract_session_gucs)] session_gucs: Option<HashMap<String, String>>,
        keepalives: bool,
        #[pyo3(from_py_with = extract_strict_opt_i64)] keepalives_idle: Option<i64>,
        #[pyo3(from_py_with = extract_strict_opt_i64)] keepalives_interval: Option<i64>,
        #[pyo3(from_py_with = extract_strict_opt_i64)] keepalives_count: Option<i64>,
        cloud_sql_instance: Option<String>,
        cloud_sql_ip_type: &str,
        alloydb_instance_uri: Option<String>,
        alloydb_ip_type: &str,
        enable_iam_auth: bool,
        #[pyo3(from_py_with = extract_cloud_auth_options)] cloud_auth_options: Option<
            HashMap<String, String>,
        >,
        #[pyo3(from_py_with = extract_iam_auth_options)] iam_auth_options: Option<
            HashMap<String, String>,
        >,
    ) -> PyResult<Self> {
        let cfg = build_validated_config(
            py,
            user,
            database,
            host.to_string(),
            port,
            password,
            unix_socket,
            sslmode.to_string(),
            ca_pem,
            client_cert_pem,
            client_key_pem,
            server_name,
            channel_binding.to_string(),
            connect_timeout,
            application_name.to_string(),
            session_gucs,
            keepalives,
            keepalives_idle,
            keepalives_interval,
            keepalives_count,
            cloud_sql_instance,
            cloud_sql_ip_type.to_string(),
            alloydb_instance_uri,
            alloydb_ip_type.to_string(),
            enable_iam_auth,
            cloud_auth_options,
            iam_auth_options,
        )?;
        Ok(Self {
            session: Arc::new(ClientSession::new(cfg)),
        })
    }

    /// Establish a physical PostgreSQL session, releasing the GIL while awaiting startup completion.
    pub fn connect(&self, py: Python<'_>) -> PyResult<()> {
        match self
            .session
            .begin_connect()
            .map_err(|e| postgres_error_to_py(py, e))?
        {
            ClientBeginConnect::AlreadyReady => Ok(()),
            ClientBeginConnect::Attempt(attempt) => {
                let pending = py
                    .detach(|| block_on_db(attempt.run()))
                    .map_err(|e| postgres_error_to_py(py, e))?;
                pending.commit().map_err(|e| postgres_error_to_py(py, e))
            }
        }
    }

    /// Close the physical PostgreSQL session idempotently, sending `Terminate` (`'X'`) once if `Ready`.
    pub fn close(&self, py: Python<'_>) -> PyResult<()> {
        let maybe_conn = self
            .session
            .begin_close()
            .map_err(|e| postgres_error_to_py(py, e))?;
        if let Some(mut conn) = maybe_conn {
            py.detach(|| block_on_db(async move { conn.close().await }))
                .map_err(|e| postgres_error_to_py(py, e))?;
        }
        Ok(())
    }

    /// Return `True` unless an authenticated session has reached `ReadyForQuery(Idle)` and has not been closed.
    #[getter]
    pub fn closed(&self) -> bool {
        self.session.is_closed()
    }

    /// Return a newly allocated dictionary copy of the session's `ParameterStatus` values.
    #[getter]
    pub fn parameters<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (k, v) in self.session.parameters() {
            dict.set_item(k, v)?;
        }
        Ok(dict)
    }

    fn __repr__(&self) -> String {
        format!(
            "SyncPostgresClient(host={:?}, port={}, database={:?}, user={:?}, closed={})",
            self.session.config().host,
            self.session.config().port,
            self.session.config().database,
            self.session.config().user,
            self.session.is_closed(),
        )
    }
}
