//! Python synchronous and asynchronous physical PostgreSQL connection pool facades
//! (`vertebra._postgres.SyncPostgresPool`, `vertebra._postgres.AsyncPostgresPool`).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use pyo3::prelude::*;
use vertebra_db::py_async::{RustDbTaskAwaitable, block_on_db, spawn_rust_db_task_with_attr};

use crate::async_client::{POSTGRES_COMPLETION_DRAINER_ATTR, PyAsyncPostgresClient};
use crate::config::PostgresConfig;
use crate::errors::{PostgresError, postgres_error_to_py};
use crate::pool::{PooledConnection, PostgresPool, PostgresPoolOptions};
use crate::python_values::{extract_parameters, query_result_to_python};
use crate::session::statements::PoolerMode;
use crate::session::{CommandBudget, SessionHandle};
use crate::sync_client::{
    PySyncPostgresClient, build_validated_config, extract_cloud_auth_options,
    extract_iam_auth_options, extract_session_gucs, extract_strict_f64, extract_strict_i64,
    extract_strict_opt_f64, extract_strict_opt_i64,
};

fn validate_positive_duration(value: f64, field_name: &str) -> Result<Duration, PostgresError> {
    if !value.is_finite() || value <= 0.0 {
        return Err(PostgresError::Config(format!(
            "{field_name} must be a positive finite number of seconds, got {value}"
        )));
    }
    Duration::try_from_secs_f64(value)
        .map_err(|_| PostgresError::Config(format!("{field_name} out of Duration bounds: {value}")))
        .and_then(|d| {
            if d.is_zero() {
                Err(PostgresError::Config(format!(
                    "{field_name} must be greater than zero"
                )))
            } else {
                Ok(d)
            }
        })
}

fn validate_non_negative_usize(value: i64, field_name: &str) -> Result<usize, PostgresError> {
    if value < 0 {
        return Err(PostgresError::Config(format!(
            "{field_name} must be >= 0, got {value}"
        )));
    }
    usize::try_from(value)
        .map_err(|_| PostgresError::Config(format!("{field_name} overflowed usize: {value}")))
}

#[allow(clippy::too_many_arguments)]
fn build_validated_pool_config_and_options(
    py: Python<'_>,
    user: String,
    database: String,
    host: &str,
    port: i64,
    password: Option<String>,
    unix_socket: Option<String>,
    sslmode: &str,
    ca_pem: Option<Vec<u8>>,
    client_cert_pem: Option<Vec<u8>>,
    client_key_pem: Option<Vec<u8>>,
    server_name: Option<String>,
    channel_binding: &str,
    connect_timeout: f64,
    application_name: &str,
    session_gucs: Option<HashMap<String, String>>,
    keepalives: bool,
    keepalives_idle: Option<i64>,
    keepalives_interval: Option<i64>,
    keepalives_count: Option<i64>,
    cloud_sql_instance: Option<String>,
    cloud_sql_ip_type: &str,
    alloydb_instance_uri: Option<String>,
    alloydb_ip_type: &str,
    enable_iam_auth: bool,
    cloud_auth_options: Option<HashMap<String, String>>,
    iam_auth_options: Option<HashMap<String, String>>,
    pooler_mode: &str,
    protocol_level_pooler_cache: bool,
    statement_cache_size: i64,
    min_size: i64,
    max_size: i64,
    acquire_timeout: f64,
    reset_timeout: f64,
    close_timeout: f64,
) -> PyResult<(PostgresConfig, PostgresPoolOptions)> {
    let config = build_validated_config(
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

    let parsed_mode = PoolerMode::parse(pooler_mode).map_err(|e| postgres_error_to_py(py, e))?;
    let stmt_cache = validate_non_negative_usize(statement_cache_size, "statement_cache_size")
        .map_err(|e| postgres_error_to_py(py, e))?;
    let min_sz = validate_non_negative_usize(min_size, "min_size")
        .map_err(|e| postgres_error_to_py(py, e))?;
    if max_size < 1 {
        return Err(postgres_error_to_py(
            py,
            PostgresError::Config(format!("max_size must be >= 1, got {max_size}")),
        ));
    }
    let max_sz = validate_non_negative_usize(max_size, "max_size")
        .map_err(|e| postgres_error_to_py(py, e))?;

    let acq_dur = validate_positive_duration(acquire_timeout, "acquire_timeout")
        .map_err(|e| postgres_error_to_py(py, e))?;
    let rst_dur = validate_positive_duration(reset_timeout, "reset_timeout")
        .map_err(|e| postgres_error_to_py(py, e))?;
    let cls_dur = validate_positive_duration(close_timeout, "close_timeout")
        .map_err(|e| postgres_error_to_py(py, e))?;

    let options = PostgresPoolOptions {
        pooler_mode: parsed_mode,
        protocol_level_pooler_cache,
        statement_cache_size: stmt_cache,
        min_size: min_sz,
        max_size: max_sz,
        acquire_timeout: acq_dur,
        reset_timeout: rst_dur,
        close_timeout: cls_dur,
        max_idle_lifetime: None,
        max_lifetime: None,
    };
    options
        .validate()
        .map_err(|e| postgres_error_to_py(py, e))?;
    Ok((config, options))
}

/// Armed guard wrapping an acquired [`PooledConnection`] across async `DbOutcomeProducer` delivery.
///
/// If the Python `asyncio.Future` is cancelled before `DbCompletionDrainer` runs or `Py::new` fails,
/// dropping this guard drops the embedded [`PooledConnection`], immediately returning the lease to
/// [`PostgresPool`] on the Tokio runtime with zero leaked permits.
struct ArmedPooledLease {
    lease: Option<PooledConnection>,
}

impl ArmedPooledLease {
    const fn new(lease: PooledConnection) -> Self {
        Self { lease: Some(lease) }
    }

    fn take(&mut self) -> PooledConnection {
        self.lease.take().expect("ArmedPooledLease still armed")
    }
}

/// Synchronous physical PostgreSQL connection pool (`vertebra._postgres.SyncPostgresPool`).
#[pyclass(
    name = "SyncPostgresPool",
    module = "vertebra._postgres",
    frozen,
    immutable_type
)]
pub struct PySyncPostgresPool {
    pool: Arc<PostgresPool>,
}

impl PySyncPostgresPool {
    /// Borrow the underlying [`PostgresPool`].
    pub const fn pool(&self) -> &Arc<PostgresPool> {
        &self.pool
    }
}

#[pymethods]
impl PySyncPostgresPool {
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
        iam_auth_options = None,
        pooler_mode = "direct",
        protocol_level_pooler_cache = false,
        statement_cache_size = 256,
        min_size = 1,
        max_size = 10,
        acquire_timeout = 30.0,
        reset_timeout = 10.0,
        close_timeout = 30.0
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
        pooler_mode: &str,
        protocol_level_pooler_cache: bool,
        #[pyo3(from_py_with = extract_strict_i64)] statement_cache_size: i64,
        #[pyo3(from_py_with = extract_strict_i64)] min_size: i64,
        #[pyo3(from_py_with = extract_strict_i64)] max_size: i64,
        #[pyo3(from_py_with = extract_strict_f64)] acquire_timeout: f64,
        #[pyo3(from_py_with = extract_strict_f64)] reset_timeout: f64,
        #[pyo3(from_py_with = extract_strict_f64)] close_timeout: f64,
    ) -> PyResult<Self> {
        let (config, options) = build_validated_pool_config_and_options(
            py,
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
            session_gucs,
            keepalives,
            keepalives_idle,
            keepalives_interval,
            keepalives_count,
            cloud_sql_instance,
            cloud_sql_ip_type,
            alloydb_instance_uri,
            alloydb_ip_type,
            enable_iam_auth,
            cloud_auth_options,
            iam_auth_options,
            pooler_mode,
            protocol_level_pooler_cache,
            statement_cache_size,
            min_size,
            max_size,
            acquire_timeout,
            reset_timeout,
            close_timeout,
        )?;
        let pool = PostgresPool::new(config, options).map_err(|e| postgres_error_to_py(py, e))?;
        Ok(Self { pool })
    }

    /// Acquire a pooled physical session lease, returning a connected `SyncPostgresClient`.
    pub fn acquire(&self, py: Python<'_>) -> PyResult<PySyncPostgresClient> {
        let pool = Arc::clone(&self.pool);
        let lease = py
            .detach(move || block_on_db(async move { pool.acquire().await }))
            .map_err(|e| postgres_error_to_py(py, e))?;
        Ok(PySyncPostgresClient::from_session(Arc::new(
            SessionHandle::from_pooled(lease),
        )))
    }

    /// Execute a single extended query directly on the pool without per-query `DISCARD ALL`.
    #[pyo3(signature = (sql, parameters = None, *, parameter_oids = None, timeout = None))]
    pub fn query(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
        parameter_oids: Option<&Bound<'_, PyAny>>,
        timeout: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let timeout_secs = extract_strict_opt_f64(timeout)?;
        let timeout_dur = CommandBudget::validate_optional_seconds(timeout_secs)
            .map_err(|e| postgres_error_to_py(py, e))?;
        let none_bound = py.None().into_bound(py);
        let params_ref = parameters.unwrap_or(&none_bound);
        let (params, oids) = extract_parameters(py, params_ref, parameter_oids)
            .map_err(|e| postgres_error_to_py(py, e))?;
        let budget = CommandBudget::start_from_duration(timeout_dur)
            .map_err(|e| postgres_error_to_py(py, e))?;
        let pool = Arc::clone(&self.pool);
        let sql_owned = sql.to_owned();
        let res = py
            .detach(move || {
                block_on_db(async move {
                    pool.query_oneshot(&sql_owned, &params, oids.as_deref(), budget)
                        .await
                })
            })
            .map_err(|e| postgres_error_to_py(py, e))?;
        query_result_to_python(py, &res)
    }

    /// Drain and close the connection pool under configured `close_timeout`.
    pub fn close(&self, py: Python<'_>) -> PyResult<()> {
        let pool = Arc::clone(&self.pool);
        let deadline = pool.options().close_timeout;
        py.detach(move || block_on_db(async move { pool.close(deadline).await }))
            .map_err(|e| postgres_error_to_py(py, e))
    }

    /// Compare candidate connection and preparation policy options against this pool without I/O.
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
        iam_auth_options = None,
        pooler_mode = "direct",
        protocol_level_pooler_cache = false,
        statement_cache_size = 256,
        min_size = 1,
        max_size = 10,
        acquire_timeout = 30.0,
        reset_timeout = 10.0,
        close_timeout = 30.0
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn validate_configuration(
        &self,
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
        pooler_mode: &str,
        protocol_level_pooler_cache: bool,
        #[pyo3(from_py_with = extract_strict_i64)] statement_cache_size: i64,
        #[pyo3(from_py_with = extract_strict_i64)] min_size: i64,
        #[pyo3(from_py_with = extract_strict_i64)] max_size: i64,
        #[pyo3(from_py_with = extract_strict_f64)] acquire_timeout: f64,
        #[pyo3(from_py_with = extract_strict_f64)] reset_timeout: f64,
        #[pyo3(from_py_with = extract_strict_f64)] close_timeout: f64,
    ) -> PyResult<()> {
        let (config, options) = build_validated_pool_config_and_options(
            py,
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
            session_gucs,
            keepalives,
            keepalives_idle,
            keepalives_interval,
            keepalives_count,
            cloud_sql_instance,
            cloud_sql_ip_type,
            alloydb_instance_uri,
            alloydb_ip_type,
            enable_iam_auth,
            cloud_auth_options,
            iam_auth_options,
            pooler_mode,
            protocol_level_pooler_cache,
            statement_cache_size,
            min_size,
            max_size,
            acquire_timeout,
            reset_timeout,
            close_timeout,
        )?;
        self.pool
            .validate_configuration(&config, &options)
            .map_err(|e| postgres_error_to_py(py, e))
    }

    #[getter]
    pub fn closed(&self) -> bool {
        self.pool.is_closed()
    }

    fn __repr__(&self) -> String {
        let cfg = self.pool.config();
        let opts = self.pool.options();
        format!(
            "SyncPostgresPool(host={:?}, port={}, database={:?}, user={:?}, max_size={}, closed={})",
            cfg.host,
            cfg.port,
            cfg.database,
            cfg.user,
            opts.max_size,
            self.pool.is_closed()
        )
    }
}

/// Asynchronous physical PostgreSQL connection pool (`vertebra._postgres.AsyncPostgresPool`).
#[pyclass(
    name = "AsyncPostgresPool",
    module = "vertebra._postgres",
    frozen,
    immutable_type
)]
pub struct PyAsyncPostgresPool {
    pool: Arc<PostgresPool>,
}

impl PyAsyncPostgresPool {
    /// Borrow the underlying [`PostgresPool`].
    pub const fn pool(&self) -> &Arc<PostgresPool> {
        &self.pool
    }
}

#[pymethods]
impl PyAsyncPostgresPool {
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
        iam_auth_options = None,
        pooler_mode = "direct",
        protocol_level_pooler_cache = false,
        statement_cache_size = 256,
        min_size = 1,
        max_size = 10,
        acquire_timeout = 30.0,
        reset_timeout = 10.0,
        close_timeout = 30.0
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
        pooler_mode: &str,
        protocol_level_pooler_cache: bool,
        #[pyo3(from_py_with = extract_strict_i64)] statement_cache_size: i64,
        #[pyo3(from_py_with = extract_strict_i64)] min_size: i64,
        #[pyo3(from_py_with = extract_strict_i64)] max_size: i64,
        #[pyo3(from_py_with = extract_strict_f64)] acquire_timeout: f64,
        #[pyo3(from_py_with = extract_strict_f64)] reset_timeout: f64,
        #[pyo3(from_py_with = extract_strict_f64)] close_timeout: f64,
    ) -> PyResult<Self> {
        let (config, options) = build_validated_pool_config_and_options(
            py,
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
            session_gucs,
            keepalives,
            keepalives_idle,
            keepalives_interval,
            keepalives_count,
            cloud_sql_instance,
            cloud_sql_ip_type,
            alloydb_instance_uri,
            alloydb_ip_type,
            enable_iam_auth,
            cloud_auth_options,
            iam_auth_options,
            pooler_mode,
            protocol_level_pooler_cache,
            statement_cache_size,
            min_size,
            max_size,
            acquire_timeout,
            reset_timeout,
            close_timeout,
        )?;
        let pool = PostgresPool::new(config, options).map_err(|e| postgres_error_to_py(py, e))?;
        Ok(Self { pool })
    }

    /// Acquire a pooled physical session lease asynchronously, recovering the lease safely on cancelled `Future`s.
    pub fn acquire(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        let pool = Arc::clone(&self.pool);
        spawn_rust_db_task_with_attr(
            py,
            async move { pool.acquire().await.map(ArmedPooledLease::new) },
            |py, mut armed| {
                let lease = armed.take();
                let client = PyAsyncPostgresClient::from_session(Arc::new(
                    SessionHandle::from_pooled(lease),
                ));
                Ok(Py::new(py, client)?.into_any())
            },
            postgres_error_to_py,
            POSTGRES_COMPLETION_DRAINER_ATTR,
        )
    }

    /// Execute a single extended query asynchronously on the pool without per-query `DISCARD ALL`.
    #[pyo3(signature = (sql, parameters = None, *, parameter_oids = None, timeout = None))]
    pub fn query<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        parameters: Option<&Bound<'py, PyAny>>,
        parameter_oids: Option<&Bound<'py, PyAny>>,
        timeout: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let timeout_secs = extract_strict_opt_f64(timeout)?;
        let timeout_dur = CommandBudget::validate_optional_seconds(timeout_secs)
            .map_err(|e| postgres_error_to_py(py, e))?;
        let none_bound = py.None().into_bound(py);
        let params_ref = parameters.unwrap_or(&none_bound);
        let (params, oids) = extract_parameters(py, params_ref, parameter_oids)
            .map_err(|e| postgres_error_to_py(py, e))?;
        let budget = CommandBudget::start_from_duration(timeout_dur)
            .map_err(|e| postgres_error_to_py(py, e))?;
        let pool = Arc::clone(&self.pool);
        let sql_owned = sql.to_owned();
        spawn_rust_db_task_with_attr(
            py,
            async move {
                pool.query_oneshot(&sql_owned, &params, oids.as_deref(), budget)
                    .await
            },
            |py, res| query_result_to_python(py, &res),
            postgres_error_to_py,
            POSTGRES_COMPLETION_DRAINER_ATTR,
        )
    }

    /// Drain and close the connection pool asynchronously under configured `close_timeout`.
    pub fn close(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        let pool = Arc::clone(&self.pool);
        let deadline = pool.options().close_timeout;
        spawn_rust_db_task_with_attr(
            py,
            async move { pool.close(deadline).await },
            |py, ()| Ok(py.None()),
            postgres_error_to_py,
            POSTGRES_COMPLETION_DRAINER_ATTR,
        )
    }

    /// Compare candidate connection and preparation policy options against this pool without I/O.
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
        iam_auth_options = None,
        pooler_mode = "direct",
        protocol_level_pooler_cache = false,
        statement_cache_size = 256,
        min_size = 1,
        max_size = 10,
        acquire_timeout = 30.0,
        reset_timeout = 10.0,
        close_timeout = 30.0
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn validate_configuration(
        &self,
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
        pooler_mode: &str,
        protocol_level_pooler_cache: bool,
        #[pyo3(from_py_with = extract_strict_i64)] statement_cache_size: i64,
        #[pyo3(from_py_with = extract_strict_i64)] min_size: i64,
        #[pyo3(from_py_with = extract_strict_i64)] max_size: i64,
        #[pyo3(from_py_with = extract_strict_f64)] acquire_timeout: f64,
        #[pyo3(from_py_with = extract_strict_f64)] reset_timeout: f64,
        #[pyo3(from_py_with = extract_strict_f64)] close_timeout: f64,
    ) -> PyResult<()> {
        let (config, options) = build_validated_pool_config_and_options(
            py,
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
            session_gucs,
            keepalives,
            keepalives_idle,
            keepalives_interval,
            keepalives_count,
            cloud_sql_instance,
            cloud_sql_ip_type,
            alloydb_instance_uri,
            alloydb_ip_type,
            enable_iam_auth,
            cloud_auth_options,
            iam_auth_options,
            pooler_mode,
            protocol_level_pooler_cache,
            statement_cache_size,
            min_size,
            max_size,
            acquire_timeout,
            reset_timeout,
            close_timeout,
        )?;
        self.pool
            .validate_configuration(&config, &options)
            .map_err(|e| postgres_error_to_py(py, e))
    }

    #[getter]
    pub fn closed(&self) -> bool {
        self.pool.is_closed()
    }

    fn __repr__(&self) -> String {
        let cfg = self.pool.config();
        let opts = self.pool.options();
        format!(
            "AsyncPostgresPool(host={:?}, port={}, database={:?}, user={:?}, max_size={}, closed={})",
            cfg.host,
            cfg.port,
            cfg.database,
            cfg.user,
            opts.max_size,
            self.pool.is_closed()
        )
    }
}

/// Register `SyncPostgresPool` and `AsyncPostgresPool` on the `vertebra._postgres` module.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySyncPostgresPool>()?;
    m.add_class::<PyAsyncPostgresPool>()?;
    Ok(())
}
