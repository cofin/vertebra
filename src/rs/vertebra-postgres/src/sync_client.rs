//! Synchronous physical PostgreSQL session client (`vertebra._postgres.SyncPostgresClient`).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyString, PyTuple};
use vertebra_db::py_async::block_on_db;

use crate::config::{PostgresConfig, RawPostgresOptions};
use crate::errors::{PostgresError, postgres_error_to_py};
use crate::portal::Portal;
use crate::python_values::{extract_parameters, query_result_to_python};
use crate::session::pipeline_scope::{
    PipelineScopeToken, extract_pipeline_batch, outcomes_to_python, pipeline_error_to_py,
};
use crate::session::{ClientBeginConnect, CloseAction, CommandBudget, SessionHandle};

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
    let items = mapping
        .call_method0("items")
        .map_err(|_| PyTypeError::new_err(format!("{field_name} must be a Mapping[str, str]")))?;
    let mut out = HashMap::new();
    for entry in items.try_iter()? {
        let item = entry?;
        let pair = item.cast_into::<PyTuple>().map_err(|_| {
            PyTypeError::new_err(format!("{field_name} items must be (str, str) pairs"))
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

pub(crate) fn extract_strict_u64(obj: &Bound<'_, PyAny>) -> PyResult<u64> {
    if obj.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err("expected int, not bool"));
    }
    let val = obj
        .extract::<i128>()
        .map_err(|_| PyTypeError::new_err("expected int"))?;
    if val < 0 {
        return Err(postgres_error_to_py(
            obj.py(),
            PostgresError::Interface("savepoint token must be >= 0".to_string()),
        ));
    }
    u64::try_from(val).map_err(|_| {
        postgres_error_to_py(
            obj.py(),
            PostgresError::Interface("savepoint token overflowed u64".to_string()),
        )
    })
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

pub(crate) fn extract_strict_opt_f64(obj: Option<&Bound<'_, PyAny>>) -> PyResult<Option<f64>> {
    let Some(val) = obj.filter(|v| !v.is_none()) else {
        return Ok(None);
    };
    extract_strict_f64(val).map(Some)
}

pub(crate) fn extract_positive_i32(obj: &Bound<'_, PyAny>, field_name: &str) -> PyResult<i32> {
    if obj.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err(format!(
            "expected int {field_name}, not bool"
        )));
    }
    let val = obj
        .extract::<i128>()
        .map_err(|_| PyTypeError::new_err(format!("expected int {field_name}")))?;
    if val <= 0 {
        return Err(postgres_error_to_py(
            obj.py(),
            PostgresError::Interface(format!("{field_name} must be >= 1, got {val}")),
        ));
    }
    i32::try_from(val).map_err(|_| {
        postgres_error_to_py(
            obj.py(),
            PostgresError::Interface(format!("{field_name} {val} exceeds i32::MAX")),
        )
    })
}

pub(crate) fn extract_max_rows(obj: &Bound<'_, PyAny>) -> PyResult<i32> {
    extract_positive_i32(obj, "max_rows")
}

pub(crate) fn extract_batch_size(obj: &Bound<'_, PyAny>) -> PyResult<i32> {
    extract_positive_i32(obj, "batch_size")
}

pub(crate) fn extract_strict_bool(obj: &Bound<'_, PyAny>) -> PyResult<bool> {
    if !obj.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err("expected bool"));
    }
    obj.extract::<bool>()
}

pub(crate) fn extract_scope_id(obj: &Bound<'_, PyAny>) -> PyResult<PipelineScopeToken> {
    if obj.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err("expected int scope_id, not bool"));
    }
    let val = obj
        .extract::<i128>()
        .map_err(|_| PyTypeError::new_err("expected int scope_id"))?;
    if val <= 0 {
        return Err(postgres_error_to_py(
            obj.py(),
            PostgresError::Interface("pipeline scope_id must be >= 1".to_string()),
        ));
    }
    let raw = u64::try_from(val).map_err(|_| {
        postgres_error_to_py(
            obj.py(),
            PostgresError::Interface("pipeline scope_id overflowed u64".to_string()),
        )
    })?;
    Ok(PipelineScopeToken(raw))
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
        ..RawPostgresOptions::default()
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
    pub(crate) session: Arc<SessionHandle>,
}

impl PySyncPostgresClient {
    /// Wrap an existing [`SessionHandle`] (such as a checked-out pool lease).
    pub fn from_session(session: Arc<SessionHandle>) -> Self {
        Self { session }
    }

    /// Register `SyncPostgresClient` and `SyncPostgresPortal` on the `vertebra._postgres` module.
    pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add_class::<Self>()?;
        m.add_class::<PySyncPostgresPortal>()
    }
}

/// Session-pinned synchronous PostgreSQL Protocol 3.0 portal (`vertebra._postgres.SyncPostgresPortal`).
#[pyclass(
    name = "SyncPostgresPortal",
    module = "vertebra._postgres",
    frozen,
    immutable_type
)]
pub struct PySyncPostgresPortal {
    portal: tokio::sync::Mutex<Portal>,
    exhausted_flag: Arc<AtomicBool>,
    closed_flag: Arc<AtomicBool>,
}

impl PySyncPostgresPortal {
    pub(crate) fn from_portal(portal: Portal) -> Self {
        let (exhausted_flag, closed_flag) = portal.status_flags();
        Self {
            portal: tokio::sync::Mutex::new(portal),
            exhausted_flag,
            closed_flag,
        }
    }
}

#[pymethods]
impl PySyncPostgresPortal {
    /// Fetch up to `max_rows` rows (`>= 1`) from the suspended server portal as a `PostgresQueryResult`.
    pub fn fetch(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = extract_max_rows)] max_rows: i32,
    ) -> PyResult<Py<PyAny>> {
        let res = py
            .detach(|| {
                block_on_db(async {
                    let mut guard = self.portal.lock().await;
                    guard.fetch(max_rows).await
                })
            })
            .map_err(|e| postgres_error_to_py(py, e))?;
        query_result_to_python(py, &res)
    }

    /// Close the portal idempotently and complete/roll back its pinned `TransactionScope`.
    pub fn close(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            block_on_db(async {
                let mut guard = self.portal.lock().await;
                guard.close().await
            })
        })
        .map_err(|e| postgres_error_to_py(py, e))
    }

    /// Return `True` once the server portal has reached `CommandComplete` or `EmptyQueryResponse`.
    #[getter]
    pub fn exhausted(&self) -> bool {
        self.exhausted_flag.load(Ordering::Acquire)
    }

    /// Return `True` once the portal has been closed or exhausted.
    #[getter]
    pub fn closed(&self) -> bool {
        self.closed_flag.load(Ordering::Acquire)
    }

    fn __repr__(&self) -> String {
        format!(
            "SyncPostgresPortal(exhausted={}, closed={})",
            self.exhausted(),
            self.closed()
        )
    }
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
            session: Arc::new(SessionHandle::from_config(cfg)),
        })
    }

    /// Establish a physical PostgreSQL session, releasing the GIL while awaiting startup completion.
    pub fn connect(&self, py: Python<'_>) -> PyResult<()> {
        match self
            .session
            .begin_client_connect()
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

    /// Close the physical PostgreSQL session idempotently, sending `Terminate` (`'X'`) once if direct or releasing if pooled.
    pub fn close(&self, py: Python<'_>) -> PyResult<()> {
        let action = self
            .session
            .begin_close_action()
            .map_err(|e| postgres_error_to_py(py, e))?;
        match action {
            CloseAction::Immediate(Some(backing)) => py
                .detach(|| block_on_db(async move { backing.close().await }))
                .map_err(|e| postgres_error_to_py(py, e))?,
            CloseAction::Immediate(None) => {}
            CloseAction::WaitInFlight(notified) => {
                let timeout = self.session.config().connect_timeout;
                py.detach(|| {
                    block_on_db(async move {
                        let _ = tokio::time::timeout(timeout, notified).await;
                    })
                });
            }
        }
        Ok(())
    }

    /// Trigger an out-of-band generation-fenced protocol `CancelRequest` for any currently active operation without waiting on the session execution lock.
    pub fn cancel(&self, py: Python<'_>) -> PyResult<()> {
        let session = Arc::clone(&self.session);
        py.detach(move || {
            block_on_db(async move {
                let _ = session.cancel().await;
            });
        });
        Ok(())
    }

    /// Execute a single-statement PostgreSQL Protocol 3.0 extended query, returning `PostgresQueryResult`.
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
        self.session
            .ensure_ordinary_reservable()
            .map_err(|e| postgres_error_to_py(py, e))?;
        let none_bound = py.None().into_bound(py);
        let params_ref = parameters.unwrap_or(&none_bound);
        let (params, oids) = extract_parameters(py, params_ref, parameter_oids)
            .map_err(|e| postgres_error_to_py(py, e))?;
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        let budget = match CommandBudget::start_from_duration(timeout_dur) {
            Ok(b) => b,
            Err(err) => {
                session.unreserve_operation_generation(op_gen);
                return Err(postgres_error_to_py(py, err));
            }
        };
        let sql_owned = sql.to_owned();
        let res = py
            .detach(move || {
                block_on_db(async move {
                    session
                        .query_as_with_generation_and_budget(
                            op_gen,
                            None,
                            &sql_owned,
                            &params,
                            oids.as_deref(),
                            budget,
                        )
                        .await
                })
            })
            .map_err(|e| postgres_error_to_py(py, e))?;
        query_result_to_python(py, &res)
    }

    /// Execute a multi-statement PostgreSQL Protocol 3.0 simple query (`'Q'`), returning `tuple[PostgresQueryResult, ...]`.
    #[pyo3(signature = (sql, *, timeout = None))]
    pub fn query_multi(
        &self,
        py: Python<'_>,
        sql: &str,
        timeout: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let timeout_secs = extract_strict_opt_f64(timeout)?;
        let timeout_dur = CommandBudget::validate_optional_seconds(timeout_secs)
            .map_err(|e| postgres_error_to_py(py, e))?;
        self.session
            .ensure_ordinary_reservable()
            .map_err(|e| postgres_error_to_py(py, e))?;
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        let budget = match CommandBudget::start_from_duration(timeout_dur) {
            Ok(b) => b,
            Err(err) => {
                session.unreserve_operation_generation(op_gen);
                return Err(postgres_error_to_py(py, err));
            }
        };
        let sql_owned = sql.to_owned();
        let results = py
            .detach(move || {
                block_on_db(async move {
                    session
                        .simple_query_with_generation_and_budget(op_gen, &sql_owned, budget)
                        .await
                })
            })
            .map_err(|e| postgres_error_to_py(py, e))?;
        let mut py_results = Vec::with_capacity(results.len());
        for res in &results {
            py_results.push(query_result_to_python(py, res)?);
        }
        Ok(PyTuple::new(py, py_results)?.into_any().unbind())
    }

    /// Open a session-pinned PostgreSQL portal inside an explicit transaction or nested savepoint scope.
    #[pyo3(signature = (sql, parameters = None, *, parameter_oids = None, batch_size = 65536))]
    pub fn open_portal(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
        parameter_oids: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = extract_batch_size)] batch_size: i32,
    ) -> PyResult<PySyncPostgresPortal> {
        self.session
            .ensure_ordinary_reservable()
            .map_err(|e| postgres_error_to_py(py, e))?;
        let none_bound = py.None().into_bound(py);
        let params_ref = parameters.unwrap_or(&none_bound);
        let (params, oids) = extract_parameters(py, params_ref, parameter_oids)
            .map_err(|e| postgres_error_to_py(py, e))?;
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        let sql_owned = sql.to_owned();
        let portal = py
            .detach(move || {
                block_on_db(async move {
                    session
                        .open_portal_with_generation(
                            op_gen,
                            &sql_owned,
                            &params,
                            oids.as_deref(),
                            batch_size,
                        )
                        .await
                })
            })
            .map_err(|e| postgres_error_to_py(py, e))?;
        Ok(PySyncPostgresPortal::from_portal(portal))
    }

    /// Open a retained pipeline transaction (`BEGIN` at `'I'`) or savepoint (`SAVEPOINT` at `'T'`) and return its integer scope identifier.
    pub fn pipeline_begin(&self, py: Python<'_>) -> PyResult<u64> {
        let session = Arc::clone(&self.session);
        let token = py
            .detach(move || block_on_db(async move { session.pipeline_begin().await }))
            .map_err(|e| postgres_error_to_py(py, e))?;
        Ok(token.0)
    }

    /// Submit `operations` inside `scope_id` as one single-`Sync` extended pipeline batch while retaining the open transaction/savepoint scope.
    pub fn pipeline_results(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = extract_scope_id)] scope_id: PipelineScopeToken,
        operations: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let batch = match extract_pipeline_batch(py, operations) {
            Ok(b) => b,
            Err(err) => {
                let _ = self
                    .session
                    .mark_pipeline_scope_failed(scope_id, err.clone());
                return Err(pipeline_error_to_py(py, err));
            }
        };
        let session = Arc::clone(&self.session);
        let outcomes = py
            .detach(move || {
                block_on_db(async move { session.pipeline_results(scope_id, batch).await })
            })
            .map_err(|e| pipeline_error_to_py(py, e))?;
        outcomes_to_python(py, &outcomes)
    }

    /// Commit/release (`success=True` with no prior failure) or roll back (`success=False` or persistent failure) the retained pipeline scope `scope_id`.
    pub fn pipeline_finish(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = extract_scope_id)] scope_id: PipelineScopeToken,
        #[pyo3(from_py_with = extract_strict_bool)] success: bool,
    ) -> PyResult<()> {
        let session = Arc::clone(&self.session);
        py.detach(move || {
            block_on_db(async move { session.pipeline_finish(scope_id, success).await })
        })
        .map_err(|e| pipeline_error_to_py(py, e))
    }

    /// Start an explicit transaction (`BEGIN`).
    pub fn begin(&self, py: Python<'_>) -> PyResult<()> {
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        py.detach(move || {
            block_on_db(async move {
                session.begin_as_with_generation(op_gen, None).await?;
                Ok::<(), PostgresError>(())
            })
        })
        .map_err(|e| postgres_error_to_py(py, e))
    }

    /// Commit the current transaction (`COMMIT`).
    pub fn commit(&self, py: Python<'_>) -> PyResult<()> {
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        py.detach(move || {
            block_on_db(async move {
                session.commit_as_with_generation(op_gen, None).await?;
                Ok::<(), PostgresError>(())
            })
        })
        .map_err(|e| postgres_error_to_py(py, e))
    }

    /// Roll back the current transaction (`ROLLBACK`).
    pub fn rollback(&self, py: Python<'_>) -> PyResult<()> {
        let session = Arc::clone(&self.session);
        py.detach(move || {
            block_on_db(async move {
                session.rollback_client().await?;
                Ok::<(), PostgresError>(())
            })
        })
        .map_err(|e| postgres_error_to_py(py, e))
    }

    /// Create a numeric savepoint (`SAVEPOINT vtb_sp_<token>`).
    pub fn savepoint(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = extract_strict_u64)] token: u64,
    ) -> PyResult<()> {
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        py.detach(move || {
            block_on_db(async move {
                session
                    .savepoint_as_with_generation(op_gen, None, token)
                    .await?;
                Ok::<(), PostgresError>(())
            })
        })
        .map_err(|e| postgres_error_to_py(py, e))
    }

    /// Release an active numeric savepoint (`RELEASE SAVEPOINT vtb_sp_<token>`).
    pub fn release_savepoint(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = extract_strict_u64)] token: u64,
    ) -> PyResult<()> {
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        py.detach(move || {
            block_on_db(async move {
                session
                    .release_savepoint_as_with_generation(op_gen, None, token)
                    .await?;
                Ok::<(), PostgresError>(())
            })
        })
        .map_err(|e| postgres_error_to_py(py, e))
    }

    /// Roll back to an active numeric savepoint (`ROLLBACK TO SAVEPOINT vtb_sp_<token>`).
    pub fn rollback_to_savepoint(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = extract_strict_u64)] token: u64,
    ) -> PyResult<()> {
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        py.detach(move || {
            block_on_db(async move {
                session
                    .rollback_to_savepoint_as_with_generation(op_gen, None, token)
                    .await?;
                Ok::<(), PostgresError>(())
            })
        })
        .map_err(|e| postgres_error_to_py(py, e))
    }

    /// Return `True` unless an authenticated session has reached `ReadyForQuery(Idle)` and has not been closed.
    #[getter]
    pub fn closed(&self) -> bool {
        self.session.is_client_closed()
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
            self.session.is_client_closed(),
        )
    }
}
