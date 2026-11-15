//! `vertebra-mssql` crate and `vertebra._mssql` PyO3 extension module.
//!
//! Provides pure-Rust Microsoft SQL Server (TDS 7.3+) wire protocol execution, `PRELOGIN` and
//! TLS-in-TDS / strict TLS negotiation, Cloud SQL for SQL Server (`:3307`) mTLS dialing,
//! `sp_executesql` `@P1` RPC execution, `BEGIN`/`COMMIT`/`ROLLBACK`/`SAVE TRANSACTION` control,
//! out-of-band `ATTENTION` (`0x06`) cancellation, `VTBARROW` zero-copy shared-memory Arrow IPC
//! materialization (`ShmCapsule`), and `BulkLoadRequest` ingestion.

pub mod arrow_export;
pub mod broker;
pub mod bulk_load;
pub mod client;
pub mod codecs;
pub mod config;
pub mod errors;
pub mod params;
pub mod transport;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyCapsule, PyDict, PyList, PyString};
use vertebra_core::notify::SyncCompletionQueue;
use vertebra_db::py_async::{
    RustDbTaskAwaitable, construct_py_arrow_cursor, db_runtime_handle, spawn_rust_db_task_with_attr,
};
use vertebra_db::{
    AuthType, CloudSqlInstanceUri, IpType, new_broker_engine_pycapsule, rewrite_mssql_placeholders,
};

use crate::broker::MssqlBrokerBackend;

use crate::client::MssqlClient;
use crate::codecs::{
    cell_to_py, first_row_to_py_dict, query_result_to_py_envelope,
    query_result_to_py_rows_and_cols, rows_to_py_dicts,
};
use crate::config::{MssqlClientConfig, MssqlCloudSqlConfig, MssqlEncryptionMode};
use crate::errors::{MssqlError, mssql_error_to_py};
use crate::params::{prepare_many_sql_and_params, prepare_sql_and_params};

/// Event-loop attribute name used by `vertebra._mssql` to attach its independent
/// [`vertebra_core::notify::AsyncCompletionQueue`] drainer without colliding with sibling extensions.
pub const MSSQL_COMPLETION_DRAINER_ATTR: &str = "_vertebra_mssql_completion_drainer";

/// Whether this build targets free-threaded Python (3.14t+).
#[cfg(Py_GIL_DISABLED)]
pub const FREE_THREADED: bool = true;
#[cfg(not(Py_GIL_DISABLED))]
pub const FREE_THREADED: bool = false;

/// Drive a Rust future synchronously while releasing the Python GIL and signaling completion
/// through a [`SyncCompletionQueue`].
fn run_sync_with_completion_queue<F, T>(py: Python<'_>, fut: F) -> T
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    py.detach(|| {
        let queue = Arc::new(SyncCompletionQueue::<T>::new());
        let q_producer = Arc::clone(&queue);
        db_runtime_handle().spawn(async move {
            let out = fut.await;
            q_producer.push(out);
        });
        queue.pop_blocking()
    })
}

enum ArrowIngestSource {
    IpcBytes(Vec<u8>),
    ShmName(String),
}

fn extract_arrow_ingest_source(source: &Bound<'_, PyAny>) -> Result<ArrowIngestSource, MssqlError> {
    if let Ok(b) = source.cast::<PyBytes>() {
        return Ok(ArrowIngestSource::IpcBytes(b.as_bytes().to_vec()));
    }
    if let Ok(s) = source.cast::<PyString>() {
        let name = s.to_str().map_err(|e| MssqlError::data(e.to_string()))?;
        return Ok(ArrowIngestSource::ShmName(name.to_string()));
    }
    if let Ok(ipc_obj) = source.call_method0("to_ipc_bytes")
        && let Ok(b) = ipc_obj.cast::<PyBytes>()
    {
        return Ok(ArrowIngestSource::IpcBytes(b.as_bytes().to_vec()));
    }
    if let Ok(shm_attr) = source.getattr("shm_name")
        && let Ok(shm_str) = shm_attr.extract::<String>()
        && !shm_str.is_empty()
    {
        return Ok(ArrowIngestSource::ShmName(shm_str));
    }
    Err(MssqlError::programming(
        "load_from_arrow source must be an ArrowCursor, Arrow IPC bytes, or a VTBARROW shared-memory segment name",
    ))
}

#[allow(clippy::too_many_arguments)]
fn build_mssql_config(
    dsn: &str,
    host: Option<String>,
    port: Option<i64>,
    database: Option<String>,
    user: Option<String>,
    password: Option<String>,
    application_name: Option<String>,
    encryption: Option<&str>,
    trust_server_certificate: Option<bool>,
    ca_pem: Option<Vec<u8>>,
    client_cert_pem: Option<Vec<u8>>,
    client_key_pem: Option<Vec<u8>>,
    server_name: Option<String>,
    connect_timeout_ms: Option<u64>,
    query_timeout_ms: Option<u64>,
    pool_size: u32,
    max_overflow: u32,
    statement_cache_capacity: u32,
    read_only_intent: bool,
    cloudsql_instance: Option<String>,
    cloudsql_ip_type: &str,
    cloudsql_auth_type: &str,
    cloudsql_proxy_addr: Option<String>,
) -> Result<MssqlClientConfig, MssqlError> {
    let mut cfg = if dsn.trim().is_empty() {
        MssqlClientConfig::default()
    } else {
        MssqlClientConfig::from_dsn(dsn)?
    };

    if let Some(h) = host {
        if h.trim().is_empty() {
            return Err(MssqlError::config("MSSQL host must not be empty"));
        }
        cfg.host = h;
    }
    if let Some(p) = port {
        let parsed_port = u16::try_from(p).ok().filter(|v| *v >= 1).ok_or_else(|| {
            MssqlError::config(format!(
                "MSSQL TCP port `{p}` is out of bounds; expected 1..=65535"
            ))
        })?;
        cfg.port = parsed_port;
    }
    if let Some(db) = database.filter(|s| !s.trim().is_empty()) {
        cfg.database = db;
    }
    if let Some(u) = user.filter(|s| !s.trim().is_empty()) {
        cfg.user = u;
    }
    if let Some(pw) = password {
        cfg.password = pw;
    }
    if let Some(app) = application_name.filter(|s| !s.trim().is_empty()) {
        cfg.application_name = app;
    }
    if let Some(enc) = encryption.filter(|s| !s.trim().is_empty()) {
        cfg.encryption = MssqlEncryptionMode::parse(enc)?;
    }
    if let Some(trust) = trust_server_certificate {
        cfg.trust_server_certificate = trust;
    }
    if ca_pem.is_some() {
        cfg.ca_pem = ca_pem;
    }
    if client_cert_pem.is_some() {
        cfg.client_cert_pem = client_cert_pem;
    }
    if client_key_pem.is_some() {
        cfg.client_key_pem = client_key_pem;
    }
    if let Some(sni) = server_name.filter(|s| !s.trim().is_empty()) {
        cfg.server_name = Some(sni);
    }
    if let Some(ms) = connect_timeout_ms {
        cfg.connect_timeout = Duration::from_millis(ms.max(1));
    }
    if let Some(ms) = query_timeout_ms {
        cfg.query_timeout = Duration::from_millis(ms.max(1));
    }
    cfg.pool_size = pool_size.max(1);
    cfg.max_overflow = max_overflow;
    cfg.statement_cache_capacity = statement_cache_capacity;
    if read_only_intent {
        cfg.read_only_intent = true;
    }

    if let Some(inst_str) = cloudsql_instance.filter(|s| !s.trim().is_empty()) {
        let uri = CloudSqlInstanceUri::parse(&inst_str).map_err(MssqlError::config)?;
        let ip_type = IpType::parse(cloudsql_ip_type).map_err(MssqlError::config)?;
        let auth_type = AuthType::parse(cloudsql_auth_type);
        let proxy_addr = match cloudsql_proxy_addr.filter(|s| !s.trim().is_empty()) {
            Some(raw_addr) => Some(raw_addr.parse::<SocketAddr>().map_err(|e| {
                MssqlError::config(format!("invalid cloudsql_proxy_addr `{raw_addr}`: {e}"))
            })?),
            None => None,
        };
        cfg.cloudsql = Some(MssqlCloudSqlConfig {
            instance_uri: uri,
            ip_type,
            auth_type,
            proxy_addr,
            server_ca_pem: cfg.ca_pem.clone(),
            client_cert_pem: cfg.client_cert_pem.clone(),
            client_key_pem: cfg.client_key_pem.clone(),
        });
    }

    cfg.validate()?;
    Ok(cfg)
}

fn lease_stats_to_py(py: Python<'_>, client: &MssqlClient) -> PyResult<Py<PyAny>> {
    let st = client.broker().lease_stats();
    let dict = PyDict::new(py);
    dict.set_item("pool_size", st.pool_size)?;
    dict.set_item("max_overflow", st.max_overflow)?;
    dict.set_item("active_leases", st.active_leases)?;
    dict.set_item("primary_in_use", st.primary_in_use)?;
    dict.set_item("overflow_in_use", st.overflow_in_use)?;
    dict.set_item("waiting_threads", st.waiting_threads)?;
    dict.set_item("total_acquired", st.total_acquired)?;
    dict.set_item("total_released", st.total_released)?;
    dict.set_item("total_expired", st.total_expired)?;
    dict.set_item("total_timeouts", st.total_timeouts)?;
    dict.set_item("total_reclaimed", st.total_reclaimed)?;
    Ok(dict.into_any().unbind())
}

fn stmt_cache_stats_to_py(py: Python<'_>, client: &MssqlClient) -> PyResult<Py<PyAny>> {
    let Some(st) = client.broker().statement_cache_stats() else {
        return Ok(py.None());
    };
    let dict = PyDict::new(py);
    dict.set_item("capacity", st.capacity)?;
    dict.set_item("hits", st.hits)?;
    dict.set_item("misses", st.misses)?;
    dict.set_item("inserts", st.inserts)?;
    dict.set_item("evictions", st.evictions)?;
    Ok(dict.into_any().unbind())
}

/// Synchronous pure-Rust Microsoft SQL Server (TDS 7.3+) client exposed to Python (`vertebra._mssql.SyncMSSQLClient`).
#[pyclass(
    name = "SyncMSSQLClient",
    module = "vertebra._mssql",
    frozen,
    immutable_type
)]
pub struct PySyncMSSQLClient {
    inner: Arc<MssqlClient>,
}

impl PySyncMSSQLClient {
    /// Borrow the underlying [`MssqlClient`].
    pub fn inner(&self) -> &Arc<MssqlClient> {
        &self.inner
    }
}

#[pymethods]
impl PySyncMSSQLClient {
    #[new]
    #[pyo3(signature = (
        dsn="mssql://localhost:1433/master",
        *,
        host=None,
        port=None,
        database=None,
        user=None,
        password=None,
        application_name=None,
        encryption=None,
        trust_server_certificate=None,
        ca_pem=None,
        client_cert_pem=None,
        client_key_pem=None,
        server_name=None,
        connect_timeout_ms=None,
        query_timeout_ms=None,
        pool_size=5,
        max_overflow=10,
        statement_cache_capacity=128,
        read_only_intent=false,
        cloudsql_instance=None,
        cloudsql_ip_type="PUBLIC",
        cloudsql_auth_type="PASSWORD",
        cloudsql_proxy_addr=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        dsn: &str,
        host: Option<String>,
        port: Option<i64>,
        database: Option<String>,
        user: Option<String>,
        password: Option<String>,
        application_name: Option<String>,
        encryption: Option<&str>,
        trust_server_certificate: Option<bool>,
        ca_pem: Option<Vec<u8>>,
        client_cert_pem: Option<Vec<u8>>,
        client_key_pem: Option<Vec<u8>>,
        server_name: Option<String>,
        connect_timeout_ms: Option<u64>,
        query_timeout_ms: Option<u64>,
        pool_size: u32,
        max_overflow: u32,
        statement_cache_capacity: u32,
        read_only_intent: bool,
        cloudsql_instance: Option<String>,
        cloudsql_ip_type: &str,
        cloudsql_auth_type: &str,
        cloudsql_proxy_addr: Option<String>,
    ) -> PyResult<Self> {
        let cfg = build_mssql_config(
            dsn,
            host,
            port,
            database,
            user,
            password,
            application_name,
            encryption,
            trust_server_certificate,
            ca_pem,
            client_cert_pem,
            client_key_pem,
            server_name,
            connect_timeout_ms,
            query_timeout_ms,
            pool_size,
            max_overflow,
            statement_cache_capacity,
            read_only_intent,
            cloudsql_instance,
            cloudsql_ip_type,
            cloudsql_auth_type,
            cloudsql_proxy_addr,
        )
        .map_err(|e| mssql_error_to_py(py, e))?;
        let client = MssqlClient::new(cfg).map_err(|e| mssql_error_to_py(py, e))?;
        Ok(Self {
            inner: Arc::new(client),
        })
    }

    #[getter]
    fn dsn(&self) -> &str {
        &self.inner.config().dsn
    }

    #[getter]
    fn host(&self) -> &str {
        &self.inner.config().host
    }

    #[getter]
    fn port(&self) -> u16 {
        self.inner.config().port
    }

    #[getter]
    fn database(&self) -> &str {
        &self.inner.config().database
    }

    #[getter]
    fn user(&self) -> &str {
        &self.inner.config().user
    }

    #[getter]
    fn application_name(&self) -> &str {
        &self.inner.config().application_name
    }

    #[getter]
    fn encryption(&self) -> &str {
        self.inner.config().encryption.as_str()
    }

    #[getter]
    fn trust_server_certificate(&self) -> bool {
        self.inner.config().trust_server_certificate
    }

    #[getter]
    fn pool_size(&self) -> u32 {
        self.inner.config().pool_size
    }

    #[getter]
    fn max_overflow(&self) -> u32 {
        self.inner.config().max_overflow
    }

    #[getter]
    fn statement_cache_capacity(&self) -> u32 {
        self.inner.config().statement_cache_capacity
    }

    #[getter]
    fn read_only_intent(&self) -> bool {
        self.inner.config().read_only_intent
    }

    #[getter]
    fn cloudsql_instance(&self) -> Option<String> {
        self.inner.config().cloudsql.as_ref().map(|c| {
            format!(
                "{}:{}:{}",
                c.instance_uri.project, c.instance_uri.region, c.instance_uri.instance
            )
        })
    }

    #[getter]
    fn in_transaction(&self) -> bool {
        self.inner.in_transaction()
    }

    #[getter]
    fn routing_destination(&self) -> Option<(String, u16)> {
        self.inner.routing_destination()
    }

    #[getter]
    fn closed(&self) -> bool {
        self.inner.is_closed()
    }

    fn __repr__(&self) -> String {
        let cfg = self.inner.config();
        format!(
            "SyncMSSQLClient(dsn={cfg}, host={:?}, port={}, database={:?}, user={:?}, password=\"[REDACTED]\", encryption={:?}, cloudsql_instance={:?}, closed={})",
            cfg.host,
            cfg.port,
            cfg.database,
            cfg.user,
            cfg.encryption.as_str(),
            self.cloudsql_instance(),
            self.inner.is_closed()
        )
    }

    fn connect(&self, py: Python<'_>) -> PyResult<()> {
        let client = Arc::clone(&self.inner);
        run_sync_with_completion_queue(py, async move { client.connect().await })
            .map_err(|e| mssql_error_to_py(py, e))
    }

    fn ping(&self, py: Python<'_>) -> PyResult<bool> {
        let client = Arc::clone(&self.inner);
        run_sync_with_completion_queue(py, async move { client.ping().await })
            .map_err(|e| mssql_error_to_py(py, e))
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let client = Arc::clone(&self.inner);
        run_sync_with_completion_queue(py, async move { client.close().await })
            .map_err(|e| mssql_error_to_py(py, e))
    }

    fn cancel(&self, py: Python<'_>) -> PyResult<()> {
        let client = Arc::clone(&self.inner);
        run_sync_with_completion_queue(py, async move { client.cancel().await })
            .map_err(|e| mssql_error_to_py(py, e))
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn execute(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<u64> {
        let (rewritten, params) =
            prepare_sql_and_params(sql, parameters).map_err(|e| mssql_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        run_sync_with_completion_queue(py, async move { client.execute(rewritten, params).await })
            .map_err(|e| mssql_error_to_py(py, e))
    }

    fn execute_many(
        &self,
        py: Python<'_>,
        sql: &str,
        parameter_sets: &Bound<'_, PyAny>,
    ) -> PyResult<u64> {
        let batches = prepare_many_sql_and_params(sql, parameter_sets)
            .map_err(|e| mssql_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        run_sync_with_completion_queue(py, async move { client.execute_many(batches).await })
            .map_err(|e| mssql_error_to_py(py, e))
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn execute_with_description(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let (rewritten, params) =
            prepare_sql_and_params(sql, parameters).map_err(|e| mssql_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        let res =
            run_sync_with_completion_queue(
                py,
                async move { client.query(rewritten, params).await },
            )
            .map_err(|e| mssql_error_to_py(py, e))?;
        query_result_to_py_rows_and_cols(py, res)
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn query(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let (rewritten, params) =
            prepare_sql_and_params(sql, parameters).map_err(|e| mssql_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        let res =
            run_sync_with_completion_queue(
                py,
                async move { client.query(rewritten, params).await },
            )
            .map_err(|e| mssql_error_to_py(py, e))?;
        query_result_to_py_envelope(py, res)
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn fetch_all(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let (rewritten, params) =
            prepare_sql_and_params(sql, parameters).map_err(|e| mssql_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        let res =
            run_sync_with_completion_queue(
                py,
                async move { client.query(rewritten, params).await },
            )
            .map_err(|e| mssql_error_to_py(py, e))?;
        rows_to_py_dicts(py, &res)
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn fetch_one(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let (rewritten, params) =
            prepare_sql_and_params(sql, parameters).map_err(|e| mssql_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        let res =
            run_sync_with_completion_queue(
                py,
                async move { client.fetch_one(rewritten, params).await },
            )
            .map_err(|e| mssql_error_to_py(py, e))?;
        first_row_to_py_dict(py, &res)
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn fetch_optional(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let (rewritten, params) =
            prepare_sql_and_params(sql, parameters).map_err(|e| mssql_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        let res = run_sync_with_completion_queue(py, async move {
            client.fetch_optional(rewritten, params).await
        })
        .map_err(|e| mssql_error_to_py(py, e))?;
        first_row_to_py_dict(py, &res)
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn select_value(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let (rewritten, params) =
            prepare_sql_and_params(sql, parameters).map_err(|e| mssql_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        let cell = run_sync_with_completion_queue(py, async move {
            client.select_value(rewritten, params).await
        })
        .map_err(|e| mssql_error_to_py(py, e))?;
        cell_to_py(py, &cell)
    }

    #[pyo3(signature = (isolation_level=None))]
    fn begin(&self, py: Python<'_>, isolation_level: Option<String>) -> PyResult<()> {
        let client = Arc::clone(&self.inner);
        run_sync_with_completion_queue(py, async move {
            client.begin_transaction(isolation_level).await
        })
        .map_err(|e| mssql_error_to_py(py, e))
    }

    fn commit(&self, py: Python<'_>) -> PyResult<()> {
        let client = Arc::clone(&self.inner);
        run_sync_with_completion_queue(py, async move { client.commit_transaction().await })
            .map_err(|e| mssql_error_to_py(py, e))
    }

    fn rollback(&self, py: Python<'_>) -> PyResult<()> {
        let client = Arc::clone(&self.inner);
        run_sync_with_completion_queue(py, async move { client.rollback_transaction().await })
            .map_err(|e| mssql_error_to_py(py, e))
    }

    fn savepoint(&self, py: Python<'_>, name: String) -> PyResult<()> {
        let client = Arc::clone(&self.inner);
        run_sync_with_completion_queue(py, async move { client.savepoint(name).await })
            .map_err(|e| mssql_error_to_py(py, e))
    }

    fn rollback_to_savepoint(&self, py: Python<'_>, name: String) -> PyResult<()> {
        let client = Arc::clone(&self.inner);
        run_sync_with_completion_queue(py, async move { client.rollback_to_savepoint(name).await })
            .map_err(|e| mssql_error_to_py(py, e))
    }

    fn release_savepoint(&self, py: Python<'_>, name: String) -> PyResult<()> {
        let client = Arc::clone(&self.inner);
        run_sync_with_completion_queue(py, async move { client.release_savepoint(name).await })
            .map_err(|e| mssql_error_to_py(py, e))
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn select_to_arrow(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let (rewritten, params) =
            prepare_sql_and_params(sql, parameters).map_err(|e| mssql_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        let (shm_name, byte_len, row_count, region) =
            run_sync_with_completion_queue(py, async move {
                client.select_to_arrow(rewritten, params).await
            })
            .map_err(|e| mssql_error_to_py(py, e))?;
        construct_py_arrow_cursor(py, &shm_name, byte_len, row_count, region)
    }

    #[pyo3(signature = (sql, parameters=None, batch_size=1024))]
    fn stream_arrow_batches(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
        batch_size: usize,
    ) -> PyResult<Py<PyAny>> {
        let (rewritten, params) =
            prepare_sql_and_params(sql, parameters).map_err(|e| mssql_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        let batches = run_sync_with_completion_queue(py, async move {
            client
                .stream_arrow_batches(rewritten, params, batch_size)
                .await
        })
        .map_err(|e| mssql_error_to_py(py, e))?;

        let list = PyList::empty(py);
        for (shm_name, byte_len, row_count, region) in batches {
            let cursor = construct_py_arrow_cursor(py, &shm_name, byte_len, row_count, region)?;
            list.append(cursor)?;
        }
        Ok(list.into_any().unbind())
    }

    #[pyo3(signature = (table, source, columns=None))]
    fn load_from_arrow(
        &self,
        py: Python<'_>,
        table: String,
        source: &Bound<'_, PyAny>,
        columns: Option<Vec<String>>,
    ) -> PyResult<u64> {
        let ingest_source =
            extract_arrow_ingest_source(source).map_err(|e| mssql_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        run_sync_with_completion_queue(py, async move {
            match ingest_source {
                ArrowIngestSource::IpcBytes(bytes) => {
                    client.load_from_arrow_ipc(table, bytes, columns).await
                }
                ArrowIngestSource::ShmName(name) => {
                    client.load_from_arrow_shm(table, name, columns).await
                }
            }
        })
        .map_err(|e| mssql_error_to_py(py, e))
    }

    fn lease_stats(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        lease_stats_to_py(py, &self.inner)
    }

    fn statement_cache_stats(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        stmt_cache_stats_to_py(py, &self.inner)
    }
}

/// Asynchronous pure-Rust Microsoft SQL Server (TDS 7.3+) client exposed to Python (`vertebra._mssql.AsyncMSSQLClient`).
#[pyclass(
    name = "AsyncMSSQLClient",
    module = "vertebra._mssql",
    frozen,
    immutable_type
)]
pub struct PyAsyncMSSQLClient {
    inner: Arc<MssqlClient>,
}

impl PyAsyncMSSQLClient {
    /// Borrow the underlying [`MssqlClient`].
    pub fn inner(&self) -> &Arc<MssqlClient> {
        &self.inner
    }
}

#[pymethods]
impl PyAsyncMSSQLClient {
    #[new]
    #[pyo3(signature = (
        dsn="mssql://localhost:1433/master",
        *,
        host=None,
        port=None,
        database=None,
        user=None,
        password=None,
        application_name=None,
        encryption=None,
        trust_server_certificate=None,
        ca_pem=None,
        client_cert_pem=None,
        client_key_pem=None,
        server_name=None,
        connect_timeout_ms=None,
        query_timeout_ms=None,
        pool_size=5,
        max_overflow=10,
        statement_cache_capacity=128,
        read_only_intent=false,
        cloudsql_instance=None,
        cloudsql_ip_type="PUBLIC",
        cloudsql_auth_type="PASSWORD",
        cloudsql_proxy_addr=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        dsn: &str,
        host: Option<String>,
        port: Option<i64>,
        database: Option<String>,
        user: Option<String>,
        password: Option<String>,
        application_name: Option<String>,
        encryption: Option<&str>,
        trust_server_certificate: Option<bool>,
        ca_pem: Option<Vec<u8>>,
        client_cert_pem: Option<Vec<u8>>,
        client_key_pem: Option<Vec<u8>>,
        server_name: Option<String>,
        connect_timeout_ms: Option<u64>,
        query_timeout_ms: Option<u64>,
        pool_size: u32,
        max_overflow: u32,
        statement_cache_capacity: u32,
        read_only_intent: bool,
        cloudsql_instance: Option<String>,
        cloudsql_ip_type: &str,
        cloudsql_auth_type: &str,
        cloudsql_proxy_addr: Option<String>,
    ) -> PyResult<Self> {
        let cfg = build_mssql_config(
            dsn,
            host,
            port,
            database,
            user,
            password,
            application_name,
            encryption,
            trust_server_certificate,
            ca_pem,
            client_cert_pem,
            client_key_pem,
            server_name,
            connect_timeout_ms,
            query_timeout_ms,
            pool_size,
            max_overflow,
            statement_cache_capacity,
            read_only_intent,
            cloudsql_instance,
            cloudsql_ip_type,
            cloudsql_auth_type,
            cloudsql_proxy_addr,
        )
        .map_err(|e| mssql_error_to_py(py, e))?;
        let client = MssqlClient::new(cfg).map_err(|e| mssql_error_to_py(py, e))?;
        Ok(Self {
            inner: Arc::new(client),
        })
    }

    #[getter]
    fn dsn(&self) -> &str {
        &self.inner.config().dsn
    }

    #[getter]
    fn host(&self) -> &str {
        &self.inner.config().host
    }

    #[getter]
    fn port(&self) -> u16 {
        self.inner.config().port
    }

    #[getter]
    fn database(&self) -> &str {
        &self.inner.config().database
    }

    #[getter]
    fn user(&self) -> &str {
        &self.inner.config().user
    }

    #[getter]
    fn application_name(&self) -> &str {
        &self.inner.config().application_name
    }

    #[getter]
    fn encryption(&self) -> &str {
        self.inner.config().encryption.as_str()
    }

    #[getter]
    fn trust_server_certificate(&self) -> bool {
        self.inner.config().trust_server_certificate
    }

    #[getter]
    fn pool_size(&self) -> u32 {
        self.inner.config().pool_size
    }

    #[getter]
    fn max_overflow(&self) -> u32 {
        self.inner.config().max_overflow
    }

    #[getter]
    fn statement_cache_capacity(&self) -> u32 {
        self.inner.config().statement_cache_capacity
    }

    #[getter]
    fn read_only_intent(&self) -> bool {
        self.inner.config().read_only_intent
    }

    #[getter]
    fn cloudsql_instance(&self) -> Option<String> {
        self.inner.config().cloudsql.as_ref().map(|c| {
            format!(
                "{}:{}:{}",
                c.instance_uri.project, c.instance_uri.region, c.instance_uri.instance
            )
        })
    }

    #[getter]
    fn in_transaction(&self) -> bool {
        self.inner.in_transaction()
    }

    #[getter]
    fn routing_destination(&self) -> Option<(String, u16)> {
        self.inner.routing_destination()
    }

    #[getter]
    fn closed(&self) -> bool {
        self.inner.is_closed()
    }

    fn __repr__(&self) -> String {
        let cfg = self.inner.config();
        format!(
            "AsyncMSSQLClient(dsn={cfg}, host={:?}, port={}, database={:?}, user={:?}, password=\"[REDACTED]\", encryption={:?}, cloudsql_instance={:?}, closed={})",
            cfg.host,
            cfg.port,
            cfg.database,
            cfg.user,
            cfg.encryption.as_str(),
            self.cloudsql_instance(),
            self.inner.is_closed()
        )
    }

    fn connect(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.connect().await },
            |py, ()| Ok(py.None()),
            mssql_error_to_py,
            MSSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    fn ping(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.ping().await },
            |py, ok| Ok(ok.into_pyobject(py)?.to_owned().into_any().unbind()),
            mssql_error_to_py,
            MSSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    fn close(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.close().await },
            |py, ()| Ok(py.None()),
            mssql_error_to_py,
            MSSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    fn cancel(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.cancel().await },
            |py, ()| Ok(py.None()),
            mssql_error_to_py,
            MSSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn execute(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let (rewritten, params) =
            prepare_sql_and_params(sql, parameters).map_err(|e| mssql_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.execute(rewritten, params).await },
            |py, count| Ok(count.into_pyobject(py)?.into_any().unbind()),
            mssql_error_to_py,
            MSSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    fn execute_many(
        &self,
        py: Python<'_>,
        sql: &str,
        parameter_sets: &Bound<'_, PyAny>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let batches = prepare_many_sql_and_params(sql, parameter_sets)
            .map_err(|e| mssql_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.execute_many(batches).await },
            |py, count| Ok(count.into_pyobject(py)?.into_any().unbind()),
            mssql_error_to_py,
            MSSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn execute_with_description(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let (rewritten, params) =
            prepare_sql_and_params(sql, parameters).map_err(|e| mssql_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.query(rewritten, params).await },
            query_result_to_py_rows_and_cols,
            mssql_error_to_py,
            MSSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn query(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let (rewritten, params) =
            prepare_sql_and_params(sql, parameters).map_err(|e| mssql_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.query(rewritten, params).await },
            query_result_to_py_envelope,
            mssql_error_to_py,
            MSSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn fetch_all(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let (rewritten, params) =
            prepare_sql_and_params(sql, parameters).map_err(|e| mssql_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.query(rewritten, params).await },
            |py, res| rows_to_py_dicts(py, &res),
            mssql_error_to_py,
            MSSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn fetch_one(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let (rewritten, params) =
            prepare_sql_and_params(sql, parameters).map_err(|e| mssql_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.fetch_one(rewritten, params).await },
            |py, res| first_row_to_py_dict(py, &res),
            mssql_error_to_py,
            MSSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn fetch_optional(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let (rewritten, params) =
            prepare_sql_and_params(sql, parameters).map_err(|e| mssql_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.fetch_optional(rewritten, params).await },
            |py, res| first_row_to_py_dict(py, &res),
            mssql_error_to_py,
            MSSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn select_value(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let (rewritten, params) =
            prepare_sql_and_params(sql, parameters).map_err(|e| mssql_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.select_value(rewritten, params).await },
            |py, cell| cell_to_py(py, &cell),
            mssql_error_to_py,
            MSSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (isolation_level=None))]
    fn begin(
        &self,
        py: Python<'_>,
        isolation_level: Option<String>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.begin_transaction(isolation_level).await },
            |py, ()| Ok(py.None()),
            mssql_error_to_py,
            MSSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    fn commit(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.commit_transaction().await },
            |py, ()| Ok(py.None()),
            mssql_error_to_py,
            MSSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    fn rollback(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.rollback_transaction().await },
            |py, ()| Ok(py.None()),
            mssql_error_to_py,
            MSSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    fn savepoint(&self, py: Python<'_>, name: String) -> PyResult<RustDbTaskAwaitable> {
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.savepoint(name).await },
            |py, ()| Ok(py.None()),
            mssql_error_to_py,
            MSSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    fn rollback_to_savepoint(&self, py: Python<'_>, name: String) -> PyResult<RustDbTaskAwaitable> {
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.rollback_to_savepoint(name).await },
            |py, ()| Ok(py.None()),
            mssql_error_to_py,
            MSSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    fn release_savepoint(&self, py: Python<'_>, name: String) -> PyResult<RustDbTaskAwaitable> {
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.release_savepoint(name).await },
            |py, ()| Ok(py.None()),
            mssql_error_to_py,
            MSSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn select_to_arrow(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let (rewritten, params) =
            prepare_sql_and_params(sql, parameters).map_err(|e| mssql_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.select_to_arrow(rewritten, params).await },
            |py, (shm_name, byte_len, row_count, region)| {
                construct_py_arrow_cursor(py, &shm_name, byte_len, row_count, region)
            },
            mssql_error_to_py,
            MSSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None, batch_size=1024))]
    fn stream_arrow_batches(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
        batch_size: usize,
    ) -> PyResult<RustDbTaskAwaitable> {
        let (rewritten, params) =
            prepare_sql_and_params(sql, parameters).map_err(|e| mssql_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move {
                client
                    .stream_arrow_batches(rewritten, params, batch_size)
                    .await
            },
            |py, batches| {
                let list = PyList::empty(py);
                for (shm_name, byte_len, row_count, region) in batches {
                    let cursor =
                        construct_py_arrow_cursor(py, &shm_name, byte_len, row_count, region)?;
                    list.append(cursor)?;
                }
                Ok(list.into_any().unbind())
            },
            mssql_error_to_py,
            MSSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (table, source, columns=None))]
    fn load_from_arrow(
        &self,
        py: Python<'_>,
        table: String,
        source: &Bound<'_, PyAny>,
        columns: Option<Vec<String>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let ingest_source =
            extract_arrow_ingest_source(source).map_err(|e| mssql_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move {
                match ingest_source {
                    ArrowIngestSource::IpcBytes(bytes) => {
                        client.load_from_arrow_ipc(table, bytes, columns).await
                    }
                    ArrowIngestSource::ShmName(name) => {
                        client.load_from_arrow_shm(table, name, columns).await
                    }
                }
            },
            |py, count| Ok(count.into_pyobject(py)?.into_any().unbind()),
            mssql_error_to_py,
            MSSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    fn lease_stats(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        lease_stats_to_py(py, &self.inner)
    }

    fn statement_cache_stats(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        stmt_cache_stats_to_py(py, &self.inner)
    }
}

#[pyfunction(name = "rewrite_mssql_placeholders")]
fn py_rewrite_mssql_placeholders(sql: &str) -> (String, usize) {
    rewrite_mssql_placeholders(sql)
}

/// Export the `vertebra.BrokerEngineCapsule` PyCapsule for SQL Server (`DIALECT_TAG_MSSQL = 5`).
#[pyfunction]
pub fn broker_engine_capsule(py: Python<'_>) -> PyResult<Bound<'_, PyCapsule>> {
    new_broker_engine_pycapsule(py, Arc::new(MssqlBrokerBackend::new_capsule_backend()))
}

#[pymodule(gil_used = false)]
pub fn _mssql(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("FREE_THREADED", FREE_THREADED)?;
    m.add("GIL_USED", false)?;
    m.add_function(wrap_pyfunction!(broker_engine_capsule, m)?)?;
    m.add_function(wrap_pyfunction!(py_rewrite_mssql_placeholders, m)?)?;
    m.add_class::<PySyncMSSQLClient>()?;
    m.add_class::<PyAsyncMSSQLClient>()?;
    Ok(())
}
