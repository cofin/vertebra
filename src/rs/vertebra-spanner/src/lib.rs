//! `vertebra-spanner` crate and `vertebra._spanner` PyO3 extension module.

pub mod arrow_ipc;
pub mod client;
pub mod codec;
pub mod cursor;
pub mod errors;
pub mod rw_tx;

use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use vertebra_db::py_async::{RustDbTaskAwaitable, block_on_db, spawn_rust_db_task_with_attr};

use crate::client::{
    SpannerBeginTransactionOptions, SpannerClient as NativeSpannerClient, SpannerClientConfig,
    SpannerReadWriteTx,
};
use crate::codec::{
    SpannerBatchWriteResult, SpannerCommitResult, SpannerDialect, SpannerQueryResult,
    SpannerStaleness, SpannerStructField, SpannerValue, parse_batch_dml_statements_json,
    parse_mutation_groups_json, parse_mutations_json, parse_spanner_parameters_json,
};
use crate::cursor::{
    CursorRpcOptions, CursorSource, PyAsyncSpannerCursor, PySyncSpannerCursor,
    cursor_not_supported_err,
};
use crate::errors::spanner_error_to_py;

pub(crate) const SPANNER_COMPLETION_DRAINER_ATTR: &str = "_vertebra_spanner_completion_drainer";

pub use vertebra_db::py_broker::FREE_THREADED;

fn parse_staleness(mode: &str, ms_or_micros: i64) -> SpannerStaleness {
    match mode.trim().to_ascii_lowercase().as_str() {
        "exact_staleness" | "exact_staleness_ms" => {
            SpannerStaleness::ExactStalenessMs(ms_or_micros.max(0) as u64)
        }
        "max_staleness" | "max_staleness_ms" => {
            SpannerStaleness::MaxStalenessMs(ms_or_micros.max(0) as u64)
        }
        "read_timestamp" | "read_timestamp_micros" => {
            SpannerStaleness::ReadTimestampUnixMicros(ms_or_micros)
        }
        "min_read_timestamp" | "min_read_timestamp_micros" => {
            SpannerStaleness::MinReadTimestampUnixMicros(ms_or_micros)
        }
        _ => SpannerStaleness::Strong,
    }
}

pub(crate) fn spanner_val_to_py(py: Python<'_>, val: &SpannerValue) -> PyResult<Py<PyAny>> {
    match val {
        SpannerValue::Null => Ok(py.None()),
        SpannerValue::Bool(b) => Ok((*b).into_pyobject(py)?.to_owned().into_any().unbind()),
        SpannerValue::Int64(n) => Ok(n.into_pyobject(py)?.into_any().unbind()),
        SpannerValue::Float64(f) => Ok(f.into_pyobject(py)?.into_any().unbind()),
        SpannerValue::String(s)
        | SpannerValue::Json(s)
        | SpannerValue::Numeric(s)
        | SpannerValue::Timestamp(s)
        | SpannerValue::Date(s)
        | SpannerValue::Uuid(s) => Ok(s.into_pyobject(py)?.into_any().unbind()),
        SpannerValue::Bytes(b) => Ok(PyBytes::new(py, b).into_any().unbind()),
        SpannerValue::Array(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(spanner_val_to_py(py, item)?)?;
            }
            Ok(list.into_any().unbind())
        }
        SpannerValue::Struct(fields) => {
            let dict = PyDict::new(py);
            for (k, v) in fields {
                dict.set_item(k, spanner_val_to_py(py, v)?)?;
            }
            Ok(dict.into_any().unbind())
        }
    }
}

fn struct_field_to_py(py: Python<'_>, f: &SpannerStructField) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("name", &f.name)?;
    dict.set_item("type", f.field_type.code.as_str())?;
    Ok(dict.into_any().unbind())
}

fn query_result_to_py(py: Python<'_>, res: SpannerQueryResult) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("columns", res.columns)?;

    let schema_list = PyList::empty(py);
    for f in &res.schema {
        schema_list.append(struct_field_to_py(py, f)?)?;
    }
    dict.set_item("schema", schema_list)?;

    let rows_list = PyList::empty(py);
    for row in &res.rows {
        let row_list = PyList::empty(py);
        for cell in row {
            row_list.append(spanner_val_to_py(py, cell)?)?;
        }
        rows_list.append(row_list)?;
    }
    dict.set_item("rows", rows_list)?;
    dict.set_item("row_count_exact", res.row_count_exact)?;
    dict.set_item("row_count_lower_bound", res.row_count_lower_bound)?;
    Ok(dict.into_any().unbind())
}

fn commit_result_to_py(py: Python<'_>, res: SpannerCommitResult) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("commit_timestamp_micros", res.commit_timestamp_micros)?;
    dict.set_item("mutation_count", res.mutation_count)?;
    dict.set_item("retry_count", res.retry_count)?;
    Ok(dict.into_any().unbind())
}

fn batch_write_results_to_py(
    py: Python<'_>,
    items: Vec<SpannerBatchWriteResult>,
) -> PyResult<Py<PyAny>> {
    let list = PyList::empty(py);
    for item in items {
        let dict = PyDict::new(py);
        dict.set_item("indexes", item.indexes)?;
        dict.set_item("status_code", item.status_code)?;
        dict.set_item("status_message", item.status_message)?;
        dict.set_item("commit_timestamp_micros", item.commit_timestamp_micros)?;
        list.append(dict)?;
    }
    Ok(list.into_any().unbind())
}

#[allow(clippy::too_many_arguments)]
fn build_spanner_config(
    database: String,
    database_role: Option<String>,
    endpoint: Option<String>,
    dialect: &str,
    pool_size: usize,
    route_to_leader_enabled: bool,
    max_commit_delay_ms: Option<u64>,
    auth_options: Option<HashMap<String, String>>,
    admin_endpoint: Option<String>,
    ca_pem: Option<Vec<u8>>,
) -> SpannerClientConfig {
    let mut cfg = SpannerClientConfig::new(database);
    cfg.database_role = database_role.filter(|s| !s.trim().is_empty());
    if let Some(ep) = endpoint.filter(|s| !s.trim().is_empty()) {
        cfg.endpoint = ep;
    }
    cfg.dialect = SpannerDialect::from_str_opt(Some(dialect));
    cfg.pool_size = pool_size.max(1);
    cfg.route_to_leader_enabled = route_to_leader_enabled;
    cfg.max_commit_delay_ms = max_commit_delay_ms;
    if let Some(auth) = auth_options {
        cfg.auth_options = auth;
    }
    cfg.admin_endpoint = admin_endpoint.filter(|s| !s.trim().is_empty());
    cfg.ca_pem = ca_pem;
    cfg
}

/// Synchronous pure-Rust Google Cloud Spanner v1 client exposed to Python (`vertebra._spanner.SyncSpannerClient`).
#[pyclass(
    name = "SyncSpannerClient",
    module = "vertebra._spanner",
    frozen,
    immutable_type
)]
pub struct PySyncSpannerClient {
    inner: Arc<NativeSpannerClient>,
}

impl PySyncSpannerClient {
    pub fn inner(&self) -> &Arc<NativeSpannerClient> {
        &self.inner
    }
}

#[pymethods]
impl PySyncSpannerClient {
    #[new]
    #[pyo3(signature = (
        database,
        database_role=None,
        endpoint=None,
        dialect="googlesql",
        pool_size=4,
        route_to_leader_enabled=true,
        max_commit_delay_ms=None,
        auth_options=None,
        admin_endpoint=None,
        ca_pem=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        database: String,
        database_role: Option<String>,
        endpoint: Option<String>,
        dialect: &str,
        pool_size: usize,
        route_to_leader_enabled: bool,
        max_commit_delay_ms: Option<u64>,
        auth_options: Option<HashMap<String, String>>,
        admin_endpoint: Option<String>,
        ca_pem: Option<Vec<u8>>,
    ) -> PyResult<Self> {
        let cfg = build_spanner_config(
            database,
            database_role,
            endpoint,
            dialect,
            pool_size,
            route_to_leader_enabled,
            max_commit_delay_ms,
            auth_options,
            admin_endpoint,
            ca_pem,
        );
        let client = py
            .detach(|| NativeSpannerClient::new(cfg))
            .map_err(|e| spanner_error_to_py(py, e))?;
        Ok(Self {
            inner: Arc::new(client),
        })
    }

    #[getter]
    fn database(&self) -> &str {
        &self.inner.config().database
    }

    #[getter]
    fn database_role(&self) -> Option<&str> {
        self.inner.config().database_role.as_deref()
    }

    #[getter]
    fn endpoint(&self) -> &str {
        self.inner.effective_endpoint()
    }

    #[getter]
    fn admin_endpoint(&self) -> Option<&str> {
        self.inner.config().admin_endpoint.as_deref()
    }

    #[getter]
    fn dialect(&self) -> &str {
        self.inner.config().dialect.as_str()
    }

    #[getter]
    fn pool_size(&self) -> usize {
        self.inner.config().pool_size
    }

    #[getter]
    fn route_to_leader_enabled(&self) -> bool {
        self.inner.config().route_to_leader_enabled
    }

    #[getter]
    fn max_commit_delay_ms(&self) -> Option<u64> {
        self.inner.config().max_commit_delay_ms
    }

    fn close(&self) {
        self.inner.close();
    }

    #[pyo3(signature = (sql, params_json=None, staleness_mode="strong", staleness_ms=0))]
    fn query(
        &self,
        py: Python<'_>,
        sql: &str,
        params_json: Option<&str>,
        staleness_mode: &str,
        staleness_ms: i64,
    ) -> PyResult<Py<PyAny>> {
        self.inner
            .ensure_open()
            .map_err(|e| spanner_error_to_py(py, e))?;
        let params =
            parse_spanner_parameters_json(params_json).map_err(|e| spanner_error_to_py(py, e))?;
        let staleness = parse_staleness(staleness_mode, staleness_ms);
        let res = py
            .detach(|| block_on_db(self.inner.query_owned(sql, params, staleness)))
            .map_err(|e| spanner_error_to_py(py, e))?;
        query_result_to_py(py, res)
    }

    #[pyo3(signature = (sql, params_json=None, request_tag=None, transaction_tag=None))]
    fn execute_dml(
        &self,
        py: Python<'_>,
        sql: &str,
        params_json: Option<&str>,
        request_tag: Option<String>,
        transaction_tag: Option<String>,
    ) -> PyResult<i64> {
        self.inner
            .ensure_open()
            .map_err(|e| spanner_error_to_py(py, e))?;
        let params =
            parse_spanner_parameters_json(params_json).map_err(|e| spanner_error_to_py(py, e))?;
        py.detach(|| {
            block_on_db(self.inner.execute_dml_owned(
                sql,
                params,
                request_tag.as_deref(),
                transaction_tag.as_deref(),
            ))
        })
        .map_err(|e| spanner_error_to_py(py, e))
    }

    #[pyo3(signature = (statements_json, request_tag=None, transaction_tag=None))]
    fn execute_batch_dml(
        &self,
        py: Python<'_>,
        statements_json: &str,
        request_tag: Option<String>,
        transaction_tag: Option<String>,
    ) -> PyResult<Vec<i64>> {
        self.inner
            .ensure_open()
            .map_err(|e| spanner_error_to_py(py, e))?;
        let stmts = parse_batch_dml_statements_json(statements_json)
            .map_err(|e| spanner_error_to_py(py, e))?;
        py.detach(|| {
            block_on_db(self.inner.execute_batch_dml_owned(
                stmts,
                request_tag.as_deref(),
                transaction_tag.as_deref(),
            ))
        })
        .map_err(|e| spanner_error_to_py(py, e))
    }

    #[pyo3(signature = (sql, params_json=None, exclude_txn_from_change_streams=false))]
    fn execute_partitioned_dml(
        &self,
        py: Python<'_>,
        sql: &str,
        params_json: Option<&str>,
        exclude_txn_from_change_streams: bool,
    ) -> PyResult<i64> {
        self.inner
            .ensure_open()
            .map_err(|e| spanner_error_to_py(py, e))?;
        let params =
            parse_spanner_parameters_json(params_json).map_err(|e| spanner_error_to_py(py, e))?;
        py.detach(|| {
            block_on_db(self.inner.execute_partitioned_dml_owned(
                sql,
                params,
                exclude_txn_from_change_streams,
            ))
        })
        .map_err(|e| spanner_error_to_py(py, e))
    }

    #[pyo3(signature = (
        mutations_json,
        transaction_tag=None,
        exclude_txn_from_change_streams=false,
        max_commit_delay_ms=None
    ))]
    fn apply_mutations(
        &self,
        py: Python<'_>,
        mutations_json: &str,
        transaction_tag: Option<String>,
        exclude_txn_from_change_streams: bool,
        max_commit_delay_ms: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        self.inner
            .ensure_open()
            .map_err(|e| spanner_error_to_py(py, e))?;
        let mutations =
            parse_mutations_json(mutations_json).map_err(|e| spanner_error_to_py(py, e))?;
        let res = py
            .detach(|| {
                block_on_db(self.inner.apply_mutations_owned(
                    mutations,
                    transaction_tag.as_deref(),
                    exclude_txn_from_change_streams,
                    max_commit_delay_ms,
                ))
            })
            .map_err(|e| spanner_error_to_py(py, e))?;
        commit_result_to_py(py, res)
    }

    #[pyo3(signature = (mutation_groups_json, exclude_txn_from_change_streams=false))]
    fn batch_write(
        &self,
        py: Python<'_>,
        mutation_groups_json: &str,
        exclude_txn_from_change_streams: bool,
    ) -> PyResult<Py<PyAny>> {
        self.inner
            .ensure_open()
            .map_err(|e| spanner_error_to_py(py, e))?;
        let groups = parse_mutation_groups_json(mutation_groups_json)
            .map_err(|e| spanner_error_to_py(py, e))?;
        let res = py
            .detach(|| {
                block_on_db(
                    self.inner
                        .batch_write_owned(groups, exclude_txn_from_change_streams),
                )
            })
            .map_err(|e| spanner_error_to_py(py, e))?;
        batch_write_results_to_py(py, res)
    }

    fn update_ddl(&self, py: Python<'_>, statements: Vec<String>) -> PyResult<String> {
        self.inner
            .ensure_open()
            .map_err(|e| spanner_error_to_py(py, e))?;
        py.detach(|| block_on_db(self.inner.update_database_ddl(&statements)))
            .map_err(|e| spanner_error_to_py(py, e))
    }

    #[pyo3(signature = (
        sql,
        params_json=None,
        max_partitions=4,
        data_boost_enabled=false,
        staleness_mode="strong",
        staleness_ms=0
    ))]
    #[allow(clippy::too_many_arguments)]
    fn partitioned_query(
        &self,
        py: Python<'_>,
        sql: &str,
        params_json: Option<&str>,
        max_partitions: i64,
        data_boost_enabled: bool,
        staleness_mode: &str,
        staleness_ms: i64,
    ) -> PyResult<Py<PyAny>> {
        self.inner
            .ensure_open()
            .map_err(|e| spanner_error_to_py(py, e))?;
        let params =
            parse_spanner_parameters_json(params_json).map_err(|e| spanner_error_to_py(py, e))?;
        let staleness = parse_staleness(staleness_mode, staleness_ms);
        let res = py
            .detach(|| {
                block_on_db(self.inner.partitioned_query_owned(
                    sql,
                    params,
                    max_partitions,
                    data_boost_enabled,
                    staleness,
                ))
            })
            .map_err(|e| spanner_error_to_py(py, e))?;
        query_result_to_py(py, res)
    }

    #[pyo3(signature = (
        transaction_tag=None,
        previous_transaction_id=None,
        exclude_txn_from_change_streams=false,
        max_commit_delay_ms=None
    ))]
    fn begin_read_write_transaction(
        &self,
        py: Python<'_>,
        transaction_tag: Option<String>,
        previous_transaction_id: Option<Vec<u8>>,
        exclude_txn_from_change_streams: bool,
        max_commit_delay_ms: Option<u64>,
    ) -> PyResult<PySyncSpannerReadWriteTransaction> {
        self.inner
            .ensure_open()
            .map_err(|e| spanner_error_to_py(py, e))?;
        let options = SpannerBeginTransactionOptions {
            transaction_tag,
            previous_transaction_id,
            exclude_txn_from_change_streams,
            max_commit_delay_ms,
        };
        let tx = py
            .detach(|| block_on_db(self.inner.begin_read_write_transaction(options)))
            .map_err(|e| spanner_error_to_py(py, e))?;
        Ok(PySyncSpannerReadWriteTransaction { inner: tx })
    }

    #[pyo3(signature = (
        *,
        staleness_mode="strong",
        staleness_value=0,
        request_tag=None,
        transaction_tag=None,
        param_encoder=None,
        lazy_begin=false,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn cursor(
        &self,
        py: Python<'_>,
        staleness_mode: &str,
        staleness_value: i64,
        request_tag: Option<String>,
        transaction_tag: Option<String>,
        param_encoder: Option<Py<PyAny>>,
        lazy_begin: bool,
    ) -> PyResult<PySyncSpannerCursor> {
        if lazy_begin {
            return Err(cursor_not_supported_err(
                py,
                "Spanner client cursors require autocommit and do not support lazy_begin",
            ));
        }
        let options = CursorRpcOptions {
            staleness: parse_staleness(staleness_mode, staleness_value),
            request_tag,
            transaction_tag,
        };
        Ok(PySyncSpannerCursor::new(
            CursorSource::Client(Arc::clone(&self.inner)),
            options,
            param_encoder,
        ))
    }
}

/// Asynchronous pure-Rust Google Cloud Spanner v1 client exposed to Python (`vertebra._spanner.AsyncSpannerClient`).
#[pyclass(
    name = "AsyncSpannerClient",
    module = "vertebra._spanner",
    frozen,
    immutable_type
)]
pub struct PyAsyncSpannerClient {
    inner: Arc<NativeSpannerClient>,
}

impl PyAsyncSpannerClient {
    pub fn inner(&self) -> &Arc<NativeSpannerClient> {
        &self.inner
    }
}

#[pymethods]
impl PyAsyncSpannerClient {
    #[new]
    #[pyo3(signature = (
        database,
        database_role=None,
        endpoint=None,
        dialect="googlesql",
        pool_size=4,
        route_to_leader_enabled=true,
        max_commit_delay_ms=None,
        auth_options=None,
        admin_endpoint=None,
        ca_pem=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        database: String,
        database_role: Option<String>,
        endpoint: Option<String>,
        dialect: &str,
        pool_size: usize,
        route_to_leader_enabled: bool,
        max_commit_delay_ms: Option<u64>,
        auth_options: Option<HashMap<String, String>>,
        admin_endpoint: Option<String>,
        ca_pem: Option<Vec<u8>>,
    ) -> PyResult<Self> {
        let cfg = build_spanner_config(
            database,
            database_role,
            endpoint,
            dialect,
            pool_size,
            route_to_leader_enabled,
            max_commit_delay_ms,
            auth_options,
            admin_endpoint,
            ca_pem,
        );
        let client = py
            .detach(|| NativeSpannerClient::new(cfg))
            .map_err(|e| spanner_error_to_py(py, e))?;
        Ok(Self {
            inner: Arc::new(client),
        })
    }

    #[getter]
    fn database(&self) -> &str {
        &self.inner.config().database
    }

    #[getter]
    fn database_role(&self) -> Option<&str> {
        self.inner.config().database_role.as_deref()
    }

    #[getter]
    fn endpoint(&self) -> &str {
        self.inner.effective_endpoint()
    }

    #[getter]
    fn admin_endpoint(&self) -> Option<&str> {
        self.inner.config().admin_endpoint.as_deref()
    }

    #[getter]
    fn dialect(&self) -> &str {
        self.inner.config().dialect.as_str()
    }

    #[getter]
    fn pool_size(&self) -> usize {
        self.inner.config().pool_size
    }

    #[getter]
    fn route_to_leader_enabled(&self) -> bool {
        self.inner.config().route_to_leader_enabled
    }

    #[getter]
    fn max_commit_delay_ms(&self) -> Option<u64> {
        self.inner.config().max_commit_delay_ms
    }

    fn close(&self) {
        self.inner.close();
    }

    #[pyo3(signature = (sql, params_json=None, staleness_mode="strong", staleness_ms=0))]
    fn query(
        &self,
        py: Python<'_>,
        sql: String,
        params_json: Option<String>,
        staleness_mode: &str,
        staleness_ms: i64,
    ) -> PyResult<RustDbTaskAwaitable> {
        self.inner
            .ensure_open()
            .map_err(|e| spanner_error_to_py(py, e))?;
        let params = parse_spanner_parameters_json(params_json.as_deref())
            .map_err(|e| spanner_error_to_py(py, e))?;
        let staleness = parse_staleness(staleness_mode, staleness_ms);
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.query_owned(&sql, params, staleness).await },
            query_result_to_py,
            spanner_error_to_py,
            SPANNER_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, params_json=None, request_tag=None, transaction_tag=None))]
    fn execute_dml(
        &self,
        py: Python<'_>,
        sql: String,
        params_json: Option<String>,
        request_tag: Option<String>,
        transaction_tag: Option<String>,
    ) -> PyResult<RustDbTaskAwaitable> {
        self.inner
            .ensure_open()
            .map_err(|e| spanner_error_to_py(py, e))?;
        let params = parse_spanner_parameters_json(params_json.as_deref())
            .map_err(|e| spanner_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move {
                client
                    .execute_dml_owned(
                        &sql,
                        params,
                        request_tag.as_deref(),
                        transaction_tag.as_deref(),
                    )
                    .await
            },
            |py, count| Ok(count.into_pyobject(py)?.into_any().unbind()),
            spanner_error_to_py,
            SPANNER_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (statements_json, request_tag=None, transaction_tag=None))]
    fn execute_batch_dml(
        &self,
        py: Python<'_>,
        statements_json: String,
        request_tag: Option<String>,
        transaction_tag: Option<String>,
    ) -> PyResult<RustDbTaskAwaitable> {
        self.inner
            .ensure_open()
            .map_err(|e| spanner_error_to_py(py, e))?;
        let stmts = parse_batch_dml_statements_json(&statements_json)
            .map_err(|e| spanner_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move {
                client
                    .execute_batch_dml_owned(
                        stmts,
                        request_tag.as_deref(),
                        transaction_tag.as_deref(),
                    )
                    .await
            },
            |py, counts| Ok(counts.into_pyobject(py)?.into_any().unbind()),
            spanner_error_to_py,
            SPANNER_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, params_json=None, exclude_txn_from_change_streams=false))]
    fn execute_partitioned_dml(
        &self,
        py: Python<'_>,
        sql: String,
        params_json: Option<String>,
        exclude_txn_from_change_streams: bool,
    ) -> PyResult<RustDbTaskAwaitable> {
        self.inner
            .ensure_open()
            .map_err(|e| spanner_error_to_py(py, e))?;
        let params = parse_spanner_parameters_json(params_json.as_deref())
            .map_err(|e| spanner_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move {
                client
                    .execute_partitioned_dml_owned(&sql, params, exclude_txn_from_change_streams)
                    .await
            },
            |py, count| Ok(count.into_pyobject(py)?.into_any().unbind()),
            spanner_error_to_py,
            SPANNER_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (
        mutations_json,
        transaction_tag=None,
        exclude_txn_from_change_streams=false,
        max_commit_delay_ms=None
    ))]
    fn apply_mutations(
        &self,
        py: Python<'_>,
        mutations_json: String,
        transaction_tag: Option<String>,
        exclude_txn_from_change_streams: bool,
        max_commit_delay_ms: Option<u64>,
    ) -> PyResult<RustDbTaskAwaitable> {
        self.inner
            .ensure_open()
            .map_err(|e| spanner_error_to_py(py, e))?;
        let mutations =
            parse_mutations_json(&mutations_json).map_err(|e| spanner_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move {
                client
                    .apply_mutations_owned(
                        mutations,
                        transaction_tag.as_deref(),
                        exclude_txn_from_change_streams,
                        max_commit_delay_ms,
                    )
                    .await
            },
            commit_result_to_py,
            spanner_error_to_py,
            SPANNER_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (mutation_groups_json, exclude_txn_from_change_streams=false))]
    fn batch_write(
        &self,
        py: Python<'_>,
        mutation_groups_json: String,
        exclude_txn_from_change_streams: bool,
    ) -> PyResult<RustDbTaskAwaitable> {
        self.inner
            .ensure_open()
            .map_err(|e| spanner_error_to_py(py, e))?;
        let groups = parse_mutation_groups_json(&mutation_groups_json)
            .map_err(|e| spanner_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move {
                client
                    .batch_write_owned(groups, exclude_txn_from_change_streams)
                    .await
            },
            batch_write_results_to_py,
            spanner_error_to_py,
            SPANNER_COMPLETION_DRAINER_ATTR,
        )
    }

    fn update_ddl(&self, py: Python<'_>, statements: Vec<String>) -> PyResult<RustDbTaskAwaitable> {
        self.inner
            .ensure_open()
            .map_err(|e| spanner_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.update_database_ddl(&statements).await },
            |py, op_name| Ok(op_name.into_pyobject(py)?.into_any().unbind()),
            spanner_error_to_py,
            SPANNER_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (
        sql,
        params_json=None,
        max_partitions=4,
        data_boost_enabled=false,
        staleness_mode="strong",
        staleness_ms=0
    ))]
    #[allow(clippy::too_many_arguments)]
    fn partitioned_query(
        &self,
        py: Python<'_>,
        sql: String,
        params_json: Option<String>,
        max_partitions: i64,
        data_boost_enabled: bool,
        staleness_mode: &str,
        staleness_ms: i64,
    ) -> PyResult<RustDbTaskAwaitable> {
        self.inner
            .ensure_open()
            .map_err(|e| spanner_error_to_py(py, e))?;
        let params = parse_spanner_parameters_json(params_json.as_deref())
            .map_err(|e| spanner_error_to_py(py, e))?;
        let staleness = parse_staleness(staleness_mode, staleness_ms);
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move {
                client
                    .partitioned_query_owned(
                        &sql,
                        params,
                        max_partitions,
                        data_boost_enabled,
                        staleness,
                    )
                    .await
            },
            query_result_to_py,
            spanner_error_to_py,
            SPANNER_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (
        transaction_tag=None,
        previous_transaction_id=None,
        exclude_txn_from_change_streams=false,
        max_commit_delay_ms=None
    ))]
    fn begin_read_write_transaction(
        &self,
        py: Python<'_>,
        transaction_tag: Option<String>,
        previous_transaction_id: Option<Vec<u8>>,
        exclude_txn_from_change_streams: bool,
        max_commit_delay_ms: Option<u64>,
    ) -> PyResult<RustDbTaskAwaitable> {
        self.inner
            .ensure_open()
            .map_err(|e| spanner_error_to_py(py, e))?;
        let options = SpannerBeginTransactionOptions {
            transaction_tag,
            previous_transaction_id,
            exclude_txn_from_change_streams,
            max_commit_delay_ms,
        };
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.begin_read_write_transaction(options).await },
            |py, tx| {
                let handle = Py::new(py, PyAsyncSpannerReadWriteTransaction { inner: tx })?;
                Ok(handle.into_any())
            },
            spanner_error_to_py,
            SPANNER_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (
        *,
        staleness_mode="strong",
        staleness_value=0,
        request_tag=None,
        transaction_tag=None,
        param_encoder=None,
        lazy_begin=false,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn cursor(
        &self,
        py: Python<'_>,
        staleness_mode: &str,
        staleness_value: i64,
        request_tag: Option<String>,
        transaction_tag: Option<String>,
        param_encoder: Option<Py<PyAny>>,
        lazy_begin: bool,
    ) -> PyResult<PyAsyncSpannerCursor> {
        if lazy_begin {
            return Err(cursor_not_supported_err(
                py,
                "Spanner client cursors require autocommit and do not support lazy_begin",
            ));
        }
        let options = CursorRpcOptions {
            staleness: parse_staleness(staleness_mode, staleness_value),
            request_tag,
            transaction_tag,
        };
        Ok(PyAsyncSpannerCursor::new(
            CursorSource::Client(Arc::clone(&self.inner)),
            options,
            param_encoder,
        ))
    }
}

/// Stateful synchronous interactive Cloud Spanner `ReadWrite` transaction handle.
#[pyclass(
    name = "SyncSpannerReadWriteTransaction",
    module = "vertebra._spanner",
    frozen,
    immutable_type
)]
pub struct PySyncSpannerReadWriteTransaction {
    inner: SpannerReadWriteTx,
}

#[pymethods]
impl PySyncSpannerReadWriteTransaction {
    #[getter]
    fn transaction_id<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.inner.transaction_id())
    }

    #[pyo3(signature = (sql, params_json=None, request_tag=None))]
    fn query(
        &self,
        py: Python<'_>,
        sql: &str,
        params_json: Option<&str>,
        request_tag: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let params =
            parse_spanner_parameters_json(params_json).map_err(|e| spanner_error_to_py(py, e))?;
        let res = py
            .detach(|| block_on_db(self.inner.query(sql, &params, request_tag.as_deref())))
            .map_err(|e| spanner_error_to_py(py, e))?;
        query_result_to_py(py, res)
    }

    #[pyo3(signature = (sql, params_json=None, request_tag=None))]
    fn execute_dml(
        &self,
        py: Python<'_>,
        sql: &str,
        params_json: Option<&str>,
        request_tag: Option<String>,
    ) -> PyResult<i64> {
        let params =
            parse_spanner_parameters_json(params_json).map_err(|e| spanner_error_to_py(py, e))?;
        py.detach(|| block_on_db(self.inner.execute_dml(sql, &params, request_tag.as_deref())))
            .map_err(|e| spanner_error_to_py(py, e))
    }

    #[pyo3(signature = (statements_json, request_tag=None))]
    fn execute_batch_dml(
        &self,
        py: Python<'_>,
        statements_json: &str,
        request_tag: Option<String>,
    ) -> PyResult<Vec<i64>> {
        let stmts = parse_batch_dml_statements_json(statements_json)
            .map_err(|e| spanner_error_to_py(py, e))?;
        py.detach(|| block_on_db(self.inner.execute_batch_dml(&stmts, request_tag.as_deref())))
            .map_err(|e| spanner_error_to_py(py, e))
    }

    #[pyo3(signature = (mutations_json=None))]
    fn commit(&self, py: Python<'_>, mutations_json: Option<&str>) -> PyResult<Py<PyAny>> {
        let mutations = match mutations_json {
            Some(json) => parse_mutations_json(json).map_err(|e| spanner_error_to_py(py, e))?,
            None => Vec::new(),
        };
        let res = py
            .detach(|| block_on_db(self.inner.commit(&mutations)))
            .map_err(|e| spanner_error_to_py(py, e))?;
        commit_result_to_py(py, res)
    }

    fn rollback(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| block_on_db(self.inner.rollback()))
            .map_err(|e| spanner_error_to_py(py, e))
    }

    #[pyo3(signature = (*, request_tag=None, param_encoder=None))]
    fn cursor(
        &self,
        request_tag: Option<String>,
        param_encoder: Option<Py<PyAny>>,
    ) -> PySyncSpannerCursor {
        let options = CursorRpcOptions {
            staleness: SpannerStaleness::Strong,
            request_tag,
            transaction_tag: None,
        };
        PySyncSpannerCursor::new(
            CursorSource::ReadWriteTx(Box::new(self.inner.clone())),
            options,
            param_encoder,
        )
    }
}

/// Stateful asynchronous interactive Cloud Spanner `ReadWrite` transaction handle.
#[pyclass(
    name = "AsyncSpannerReadWriteTransaction",
    module = "vertebra._spanner",
    frozen,
    immutable_type
)]
pub struct PyAsyncSpannerReadWriteTransaction {
    inner: SpannerReadWriteTx,
}

#[pymethods]
impl PyAsyncSpannerReadWriteTransaction {
    #[getter]
    fn transaction_id<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.inner.transaction_id())
    }

    #[pyo3(signature = (sql, params_json=None, request_tag=None))]
    fn query(
        &self,
        py: Python<'_>,
        sql: String,
        params_json: Option<String>,
        request_tag: Option<String>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let params = parse_spanner_parameters_json(params_json.as_deref())
            .map_err(|e| spanner_error_to_py(py, e))?;
        let tx = self.inner.clone();
        spawn_rust_db_task_with_attr(
            py,
            async move { tx.query(&sql, &params, request_tag.as_deref()).await },
            query_result_to_py,
            spanner_error_to_py,
            SPANNER_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, params_json=None, request_tag=None))]
    fn execute_dml(
        &self,
        py: Python<'_>,
        sql: String,
        params_json: Option<String>,
        request_tag: Option<String>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let params = parse_spanner_parameters_json(params_json.as_deref())
            .map_err(|e| spanner_error_to_py(py, e))?;
        let tx = self.inner.clone();
        spawn_rust_db_task_with_attr(
            py,
            async move { tx.execute_dml(&sql, &params, request_tag.as_deref()).await },
            |py, count| Ok(count.into_pyobject(py)?.into_any().unbind()),
            spanner_error_to_py,
            SPANNER_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (statements_json, request_tag=None))]
    fn execute_batch_dml(
        &self,
        py: Python<'_>,
        statements_json: String,
        request_tag: Option<String>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let stmts = parse_batch_dml_statements_json(&statements_json)
            .map_err(|e| spanner_error_to_py(py, e))?;
        let tx = self.inner.clone();
        spawn_rust_db_task_with_attr(
            py,
            async move { tx.execute_batch_dml(&stmts, request_tag.as_deref()).await },
            |py, counts| Ok(counts.into_pyobject(py)?.into_any().unbind()),
            spanner_error_to_py,
            SPANNER_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (mutations_json=None))]
    fn commit(
        &self,
        py: Python<'_>,
        mutations_json: Option<String>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let mutations = match mutations_json.as_deref() {
            Some(json) => parse_mutations_json(json).map_err(|e| spanner_error_to_py(py, e))?,
            None => Vec::new(),
        };
        let tx = self.inner.clone();
        spawn_rust_db_task_with_attr(
            py,
            async move { tx.commit(&mutations).await },
            commit_result_to_py,
            spanner_error_to_py,
            SPANNER_COMPLETION_DRAINER_ATTR,
        )
    }

    fn rollback(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        let tx = self.inner.clone();
        spawn_rust_db_task_with_attr(
            py,
            async move { tx.rollback().await },
            |py, ()| Ok(py.None()),
            spanner_error_to_py,
            SPANNER_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (*, request_tag=None, param_encoder=None))]
    fn cursor(
        &self,
        request_tag: Option<String>,
        param_encoder: Option<Py<PyAny>>,
    ) -> PyAsyncSpannerCursor {
        let options = CursorRpcOptions {
            staleness: SpannerStaleness::Strong,
            request_tag,
            transaction_tag: None,
        };
        PyAsyncSpannerCursor::new(
            CursorSource::ReadWriteTx(Box::new(self.inner.clone())),
            options,
            param_encoder,
        )
    }
}

#[pymodule(gil_used = false)]
pub fn _spanner(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("FREE_THREADED", FREE_THREADED)?;
    m.add("GIL_USED", false)?;
    m.add_class::<PySyncSpannerClient>()?;
    m.add_class::<PyAsyncSpannerClient>()?;
    m.add_class::<PySyncSpannerReadWriteTransaction>()?;
    m.add_class::<PyAsyncSpannerReadWriteTransaction>()?;
    cursor::register(m)?;
    Ok(())
}
