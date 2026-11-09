//! Asynchronous physical PostgreSQL session client (`vertebra._postgres.AsyncPostgresClient`).

use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use vertebra_db::py_async::{RustDbTaskAwaitable, spawn_rust_db_task_with_attr};

use crate::errors::{PostgresError, postgres_error_to_py};
use crate::session::{ClientBeginConnect, ClientSession};
use crate::sync_client::{
    build_validated_config, extract_cloud_auth_options, extract_iam_auth_options,
    extract_session_gucs, extract_strict_f64, extract_strict_i64, extract_strict_opt_i64,
};

/// Event-loop attribute name isolating the `vertebra._postgres` completion drainer from sibling extensions.
pub const POSTGRES_COMPLETION_DRAINER_ATTR: &str = "_vertebra_postgres_completion_drainer";

/// Physical asynchronous native PostgreSQL client (`vertebra._postgres.AsyncPostgresClient`).
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(
        name = "AsyncPostgresClient",
        module = "vertebra._postgres",
        frozen,
        immutable_type,
        freelist = 128
    )
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(
        name = "AsyncPostgresClient",
        module = "vertebra._postgres",
        frozen,
        immutable_type
    )
)]
pub struct PyAsyncPostgresClient {
    pub(crate) session: Arc<ClientSession>,
}

#[pymethods]
impl PyAsyncPostgresClient {
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

    /// Establish a physical PostgreSQL session asynchronously and publish `Ready` exclusively inside `map_ok`.
    pub fn connect(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        match self
            .session
            .begin_connect()
            .map_err(|e| postgres_error_to_py(py, e))?
        {
            ClientBeginConnect::AlreadyReady => spawn_rust_db_task_with_attr(
                py,
                async { Ok::<(), PostgresError>(()) },
                |py, ()| Ok(py.None()),
                postgres_error_to_py,
                POSTGRES_COMPLETION_DRAINER_ATTR,
            ),
            ClientBeginConnect::Attempt(attempt) => spawn_rust_db_task_with_attr(
                py,
                async move { attempt.run().await },
                |py, pending| {
                    pending.commit().map_err(|e| postgres_error_to_py(py, e))?;
                    Ok(py.None())
                },
                postgres_error_to_py,
                POSTGRES_COMPLETION_DRAINER_ATTR,
            ),
        }
    }

    /// Synchronously mark the session `Closed` and schedule asynchronous `Terminate` (`'X'`) shutdown.
    pub fn close(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        let maybe_conn = self
            .session
            .begin_close()
            .map_err(|e| postgres_error_to_py(py, e))?;
        spawn_rust_db_task_with_attr(
            py,
            async move {
                if let Some(mut conn) = maybe_conn {
                    conn.close().await?;
                }
                Ok::<(), PostgresError>(())
            },
            |py, ()| Ok(py.None()),
            postgres_error_to_py,
            POSTGRES_COMPLETION_DRAINER_ATTR,
        )
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
            "AsyncPostgresClient(host={:?}, port={}, database={:?}, user={:?}, closed={})",
            self.session.config().host,
            self.session.config().port,
            self.session.config().database,
            self.session.config().user,
            self.session.is_closed(),
        )
    }
}
