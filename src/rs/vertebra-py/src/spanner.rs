//! PyO3 bindings for `vertebra_db::spanner` (`SpannerClient`, multiplexed session management,
//! `ReadOnly` staleness queries, multiplexed `ReadWrite` transactions with `ExecuteBatchDml` and
//! `MultiplexedSessionPrecommitToken` tracking, `PartitionedDml`, `BatchWrite`, `UpdateDatabaseDdl`,
//! and parallel `PartitionQuery` + Data Boost into `VTBARROW` shared-memory `ArrowCursor`).

use std::collections::HashMap;
use std::sync::Arc;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use vertebra_db::spanner::{
    ProtobufValue, SpannerBatchDmlStatement, SpannerBatchWriteResult,
    SpannerClient as NativeSpannerClient, SpannerClientConfig, SpannerCommitResult, SpannerDialect,
    SpannerError, SpannerMutation, SpannerMutationGroup, SpannerQueryResult, SpannerStaleness,
    SpannerStructField, SpannerTypeCode, SpannerValue, parse_spanner_parameters_json,
};

use crate::completion::{RustDbTaskAwaitable, block_on_db, spawn_rust_db_task};
use crate::db::ArrowCursor;

fn spanner_err_to_py(err: SpannerError) -> PyErr {
    match err {
        SpannerError::InvalidArgument(msg) => PyValueError::new_err(msg),
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

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

fn spanner_val_to_py(py: Python<'_>, val: &SpannerValue) -> PyResult<Py<PyAny>> {
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
    dict.set_item(
        "transaction_id",
        res.transaction_id.as_deref().map(|b| PyBytes::new(py, b)),
    )?;
    dict.set_item("read_timestamp_micros", res.read_timestamp_micros)?;
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

fn json_cell_to_protobuf_value(val: &serde_json::Value) -> ProtobufValue {
    match val {
        serde_json::Value::Null => ProtobufValue::Null,
        serde_json::Value::Bool(b) => ProtobufValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                ProtobufValue::String(i.to_string())
            } else {
                ProtobufValue::Number(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => ProtobufValue::String(s.clone()),
        serde_json::Value::Array(arr) => {
            ProtobufValue::List(arr.iter().map(json_cell_to_protobuf_value).collect())
        }
        serde_json::Value::Object(map) => {
            ProtobufValue::String(serde_json::Value::Object(map.clone()).to_string())
        }
    }
}

fn parse_mutation_value(item: &serde_json::Value) -> Result<SpannerMutation, SpannerError> {
    let op = item
        .get("op")
        .and_then(|v| v.as_str())
        .unwrap_or("insert_or_update")
        .to_ascii_lowercase();
    let table = item
        .get("table")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if table.is_empty() {
        return Err(SpannerError::InvalidArgument(
            "mutation 'table' must not be empty".to_string(),
        ));
    }
    if op == "delete" {
        let all = item.get("all").and_then(|v| v.as_bool()).unwrap_or(false);
        let keys = item
            .get("keys")
            .and_then(|v| v.as_array())
            .map(|rows| {
                rows.iter()
                    .map(|r| {
                        if let Some(arr) = r.as_array() {
                            arr.iter().map(json_cell_to_protobuf_value).collect()
                        } else {
                            vec![json_cell_to_protobuf_value(r)]
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();
        return Ok(SpannerMutation::delete(table, keys, all));
    }

    let columns: Vec<String> = item
        .get("columns")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|c| c.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    let values: Vec<Vec<ProtobufValue>> = item
        .get("values")
        .and_then(|v| v.as_array())
        .map(|rows| {
            rows.iter()
                .map(|r| {
                    r.as_array()
                        .map(|cells| cells.iter().map(json_cell_to_protobuf_value).collect())
                        .unwrap_or_default()
                })
                .collect()
        })
        .unwrap_or_default();

    match op.as_str() {
        "insert" => Ok(SpannerMutation::insert(table, columns, values)),
        "update" => Ok(SpannerMutation::update(table, columns, values)),
        "replace" => Ok(SpannerMutation::replace(table, columns, values)),
        _ => Ok(SpannerMutation::insert_or_update(table, columns, values)),
    }
}

fn parse_mutations_json(raw: &str) -> Result<Vec<SpannerMutation>, SpannerError> {
    let val: serde_json::Value = serde_json::from_str(raw)
        .map_err(|e| SpannerError::InvalidArgument(format!("invalid mutations_json: {e}")))?;
    let Some(arr) = val.as_array() else {
        return Err(SpannerError::InvalidArgument(
            "mutations_json must be a JSON array".to_string(),
        ));
    };
    arr.iter().map(parse_mutation_value).collect()
}

fn parse_mutation_groups_json(raw: &str) -> Result<Vec<SpannerMutationGroup>, SpannerError> {
    let val: serde_json::Value = serde_json::from_str(raw)
        .map_err(|e| SpannerError::InvalidArgument(format!("invalid mutation_groups_json: {e}")))?;
    let Some(arr) = val.as_array() else {
        return Err(SpannerError::InvalidArgument(
            "mutation_groups_json must be a JSON array".to_string(),
        ));
    };
    let mut groups = Vec::with_capacity(arr.len());
    for group_val in arr {
        let muts_arr = if let Some(inner) = group_val.get("mutations").and_then(|v| v.as_array()) {
            inner
        } else if let Some(inner) = group_val.as_array() {
            inner
        } else {
            return Err(SpannerError::InvalidArgument(
                "each mutation group must be a list of mutations or {'mutations': [...]}"
                    .to_string(),
            ));
        };
        let mut mutations = Vec::with_capacity(muts_arr.len());
        for m in muts_arr {
            mutations.push(parse_mutation_value(m)?);
        }
        groups.push(SpannerMutationGroup::new(mutations));
    }
    Ok(groups)
}

fn parse_batch_dml_statements_json(
    raw: &str,
) -> Result<Vec<SpannerBatchDmlStatement>, SpannerError> {
    let val: serde_json::Value = serde_json::from_str(raw)
        .map_err(|e| SpannerError::InvalidArgument(format!("invalid statements_json: {e}")))?;
    let Some(arr) = val.as_array() else {
        return Err(SpannerError::InvalidArgument(
            "statements_json must be a JSON array".to_string(),
        ));
    };
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        let sql = item
            .get("sql")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let params_raw = item.get("params").map(serde_json::Value::to_string);
        let params = parse_spanner_parameters_json(params_raw.as_deref())?;
        let _ = SpannerTypeCode::Unspecified;
        out.push(SpannerBatchDmlStatement { sql, params });
    }
    Ok(out)
}

/// Pure-Rust Google Cloud Spanner v1 client exposed to Python (`vertebra._vertebra.SpannerClient`).
#[pyclass(
    name = "SpannerClient",
    module = "vertebra._vertebra",
    frozen,
    immutable_type
)]
pub struct PySpannerClient {
    inner: Arc<NativeSpannerClient>,
}

#[pymethods]
impl PySpannerClient {
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
        ca_pem: Option<Vec<u8>>,
    ) -> PyResult<Self> {
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
        cfg.ca_pem = ca_pem;

        let client = py
            .detach(|| NativeSpannerClient::new(cfg))
            .map_err(spanner_err_to_py)?;
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
    fn endpoint(&self) -> String {
        self.inner.config().effective_endpoint()
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

    #[pyo3(signature = (sql, params_json=None, staleness_mode="strong", staleness_ms=0))]
    fn query(
        &self,
        py: Python<'_>,
        sql: &str,
        params_json: Option<&str>,
        staleness_mode: &str,
        staleness_ms: i64,
    ) -> PyResult<Py<PyAny>> {
        let params = parse_spanner_parameters_json(params_json).map_err(spanner_err_to_py)?;
        let staleness = parse_staleness(staleness_mode, staleness_ms);
        let client = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        let res = py
            .detach(|| {
                block_on_db(async move {
                    client
                        .query_read_only(&sql_owned, &params, staleness)
                        .await
                        .map_err(|e| e.to_string())
                })
            })
            .map_err(PyRuntimeError::new_err)?;
        query_result_to_py(py, res)
    }

    #[pyo3(signature = (sql, params_json=None, staleness_mode="strong", staleness_ms=0))]
    fn query_async(
        &self,
        py: Python<'_>,
        sql: String,
        params_json: Option<String>,
        staleness_mode: &str,
        staleness_ms: i64,
    ) -> PyResult<RustDbTaskAwaitable> {
        let params =
            parse_spanner_parameters_json(params_json.as_deref()).map_err(spanner_err_to_py)?;
        let staleness = parse_staleness(staleness_mode, staleness_ms);
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task(
            py,
            async move { client.query_read_only(&sql, &params, staleness).await },
            query_result_to_py,
            spanner_err_to_py,
        )
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
        let params = parse_spanner_parameters_json(params_json).map_err(spanner_err_to_py)?;
        let client = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        py.detach(|| {
            block_on_db(async move {
                let c_inner = Arc::clone(&client);
                let (count, _) = client
                    .run_in_read_write_transaction(5, move |mut tx| {
                        let c = Arc::clone(&c_inner);
                        let s = sql_owned.clone();
                        let p = params.clone();
                        let rtag = request_tag.clone();
                        tx.transaction_tag = transaction_tag.clone();
                        async move {
                            let res = c
                                .execute_in_read_write_tx(&mut tx, &s, &p, rtag.as_deref())
                                .await
                                .map_err(|e| (e, tx.transaction_id.clone()))?;
                            let affected = res
                                .row_count_exact
                                .or(res.row_count_lower_bound)
                                .unwrap_or(0);
                            Ok((affected, tx))
                        }
                    })
                    .await
                    .map_err(|e| e.to_string())?;
                Ok::<i64, String>(count)
            })
        })
        .map_err(PyRuntimeError::new_err)
    }

    #[pyo3(signature = (sql, params_json=None, request_tag=None, transaction_tag=None))]
    fn execute_dml_async(
        &self,
        py: Python<'_>,
        sql: String,
        params_json: Option<String>,
        request_tag: Option<String>,
        transaction_tag: Option<String>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let params =
            parse_spanner_parameters_json(params_json.as_deref()).map_err(spanner_err_to_py)?;
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task(
            py,
            async move {
                let c_inner = Arc::clone(&client);
                let (count, _) = client
                    .run_in_read_write_transaction(5, move |mut tx| {
                        let c = Arc::clone(&c_inner);
                        let s = sql.clone();
                        let p = params.clone();
                        let rtag = request_tag.clone();
                        tx.transaction_tag = transaction_tag.clone();
                        async move {
                            let res = c
                                .execute_in_read_write_tx(&mut tx, &s, &p, rtag.as_deref())
                                .await
                                .map_err(|e| (e, tx.transaction_id.clone()))?;
                            let affected = res
                                .row_count_exact
                                .or(res.row_count_lower_bound)
                                .unwrap_or(0);
                            Ok((affected, tx))
                        }
                    })
                    .await?;
                Ok(count)
            },
            |py, count| Ok(count.into_pyobject(py)?.into_any().unbind()),
            spanner_err_to_py,
        )
    }

    #[pyo3(signature = (statements_json, request_tag=None, transaction_tag=None))]
    fn execute_batch_dml(
        &self,
        py: Python<'_>,
        statements_json: &str,
        request_tag: Option<String>,
        transaction_tag: Option<String>,
    ) -> PyResult<Vec<i64>> {
        let stmts = parse_batch_dml_statements_json(statements_json).map_err(spanner_err_to_py)?;
        let client = Arc::clone(&self.inner);
        py.detach(|| {
            block_on_db(async move {
                let c_inner = Arc::clone(&client);
                let (counts, _) = client
                    .run_in_read_write_transaction(5, move |mut tx| {
                        let c = Arc::clone(&c_inner);
                        let s = stmts.clone();
                        let rtag = request_tag.clone();
                        tx.transaction_tag = transaction_tag.clone();
                        async move {
                            let res = c
                                .execute_batch_dml(&mut tx, &s, rtag.as_deref())
                                .await
                                .map_err(|e| (e, tx.transaction_id.clone()))?;
                            Ok((res.row_counts, tx))
                        }
                    })
                    .await
                    .map_err(|e| e.to_string())?;
                Ok::<Vec<i64>, String>(counts)
            })
        })
        .map_err(PyRuntimeError::new_err)
    }

    #[pyo3(signature = (statements_json, request_tag=None, transaction_tag=None))]
    fn execute_batch_dml_async(
        &self,
        py: Python<'_>,
        statements_json: String,
        request_tag: Option<String>,
        transaction_tag: Option<String>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let stmts = parse_batch_dml_statements_json(&statements_json).map_err(spanner_err_to_py)?;
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task(
            py,
            async move {
                let c_inner = Arc::clone(&client);
                let (counts, _) = client
                    .run_in_read_write_transaction(5, move |mut tx| {
                        let c = Arc::clone(&c_inner);
                        let s = stmts.clone();
                        let rtag = request_tag.clone();
                        tx.transaction_tag = transaction_tag.clone();
                        async move {
                            let res = c
                                .execute_batch_dml(&mut tx, &s, rtag.as_deref())
                                .await
                                .map_err(|e| (e, tx.transaction_id.clone()))?;
                            Ok((res.row_counts, tx))
                        }
                    })
                    .await?;
                Ok(counts)
            },
            |py, counts| Ok(counts.into_pyobject(py)?.into_any().unbind()),
            spanner_err_to_py,
        )
    }

    #[pyo3(signature = (sql, params_json=None, exclude_txn_from_change_streams=false))]
    fn execute_partitioned_dml(
        &self,
        py: Python<'_>,
        sql: &str,
        params_json: Option<&str>,
        exclude_txn_from_change_streams: bool,
    ) -> PyResult<i64> {
        let params = parse_spanner_parameters_json(params_json).map_err(spanner_err_to_py)?;
        let client = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        py.detach(|| {
            block_on_db(async move {
                client
                    .execute_partitioned_dml(&sql_owned, &params, exclude_txn_from_change_streams)
                    .await
                    .map_err(|e| e.to_string())
            })
        })
        .map_err(PyRuntimeError::new_err)
    }

    #[pyo3(signature = (sql, params_json=None, exclude_txn_from_change_streams=false))]
    fn execute_partitioned_dml_async(
        &self,
        py: Python<'_>,
        sql: String,
        params_json: Option<String>,
        exclude_txn_from_change_streams: bool,
    ) -> PyResult<RustDbTaskAwaitable> {
        let params =
            parse_spanner_parameters_json(params_json.as_deref()).map_err(spanner_err_to_py)?;
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task(
            py,
            async move {
                client
                    .execute_partitioned_dml(&sql, &params, exclude_txn_from_change_streams)
                    .await
            },
            |py, count| Ok(count.into_pyobject(py)?.into_any().unbind()),
            spanner_err_to_py,
        )
    }

    #[pyo3(signature = (mutations_json, transaction_tag=None, exclude_txn_from_change_streams=false))]
    fn apply_mutations(
        &self,
        py: Python<'_>,
        mutations_json: &str,
        transaction_tag: Option<String>,
        exclude_txn_from_change_streams: bool,
    ) -> PyResult<Py<PyAny>> {
        let mutations = parse_mutations_json(mutations_json).map_err(spanner_err_to_py)?;
        let client = Arc::clone(&self.inner);
        let res = py
            .detach(|| {
                block_on_db(async move {
                    let (_, commit_res) = client
                        .run_in_read_write_transaction(5, move |mut tx| {
                            tx.mutations = mutations.clone();
                            tx.transaction_tag = transaction_tag.clone();
                            tx.exclude_txn_from_change_streams = exclude_txn_from_change_streams;
                            async move { Ok(((), tx)) }
                        })
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok::<SpannerCommitResult, String>(commit_res)
                })
            })
            .map_err(PyRuntimeError::new_err)?;
        commit_result_to_py(py, res)
    }

    #[pyo3(signature = (mutations_json, transaction_tag=None, exclude_txn_from_change_streams=false))]
    fn apply_mutations_async(
        &self,
        py: Python<'_>,
        mutations_json: String,
        transaction_tag: Option<String>,
        exclude_txn_from_change_streams: bool,
    ) -> PyResult<RustDbTaskAwaitable> {
        let mutations = parse_mutations_json(&mutations_json).map_err(spanner_err_to_py)?;
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task(
            py,
            async move {
                let (_, commit_res) = client
                    .run_in_read_write_transaction(5, move |mut tx| {
                        tx.mutations = mutations.clone();
                        tx.transaction_tag = transaction_tag.clone();
                        tx.exclude_txn_from_change_streams = exclude_txn_from_change_streams;
                        async move { Ok(((), tx)) }
                    })
                    .await?;
                Ok(commit_res)
            },
            commit_result_to_py,
            spanner_err_to_py,
        )
    }

    #[pyo3(signature = (mutation_groups_json, exclude_txn_from_change_streams=false))]
    fn batch_write(
        &self,
        py: Python<'_>,
        mutation_groups_json: &str,
        exclude_txn_from_change_streams: bool,
    ) -> PyResult<Py<PyAny>> {
        let groups = parse_mutation_groups_json(mutation_groups_json).map_err(spanner_err_to_py)?;
        let client = Arc::clone(&self.inner);
        let res = py
            .detach(|| {
                block_on_db(async move {
                    client
                        .batch_write(&groups, exclude_txn_from_change_streams)
                        .await
                        .map_err(|e| e.to_string())
                })
            })
            .map_err(PyRuntimeError::new_err)?;
        batch_write_results_to_py(py, res)
    }

    #[pyo3(signature = (mutation_groups_json, exclude_txn_from_change_streams=false))]
    fn batch_write_async(
        &self,
        py: Python<'_>,
        mutation_groups_json: String,
        exclude_txn_from_change_streams: bool,
    ) -> PyResult<RustDbTaskAwaitable> {
        let groups =
            parse_mutation_groups_json(&mutation_groups_json).map_err(spanner_err_to_py)?;
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task(
            py,
            async move {
                client
                    .batch_write(&groups, exclude_txn_from_change_streams)
                    .await
            },
            batch_write_results_to_py,
            spanner_err_to_py,
        )
    }

    fn update_ddl(&self, py: Python<'_>, statements: Vec<String>) -> PyResult<String> {
        let client = Arc::clone(&self.inner);
        py.detach(|| {
            block_on_db(async move {
                client
                    .update_database_ddl(&statements)
                    .await
                    .map_err(|e| e.to_string())
            })
        })
        .map_err(PyRuntimeError::new_err)
    }

    fn update_ddl_async(
        &self,
        py: Python<'_>,
        statements: Vec<String>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task(
            py,
            async move { client.update_database_ddl(&statements).await },
            |py, op_name| Ok(op_name.into_pyobject(py)?.into_any().unbind()),
            spanner_err_to_py,
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
    fn execute_partitioned_arrow(
        &self,
        py: Python<'_>,
        sql: &str,
        params_json: Option<&str>,
        max_partitions: i64,
        data_boost_enabled: bool,
        staleness_mode: &str,
        staleness_ms: i64,
    ) -> PyResult<ArrowCursor> {
        let params = parse_spanner_parameters_json(params_json).map_err(spanner_err_to_py)?;
        let staleness = parse_staleness(staleness_mode, staleness_ms);
        let client = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        let (shm_name, byte_len, row_count, region) = py
            .detach(|| {
                block_on_db(async move {
                    client
                        .execute_partitioned_arrow(
                            &sql_owned,
                            &params,
                            max_partitions,
                            data_boost_enabled,
                            staleness,
                        )
                        .await
                        .map_err(|e| e.to_string())
                })
            })
            .map_err(PyRuntimeError::new_err)?;
        Ok(ArrowCursor::from_shm_region(
            shm_name, byte_len, row_count, region,
        ))
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
    fn execute_partitioned_arrow_async(
        &self,
        py: Python<'_>,
        sql: String,
        params_json: Option<String>,
        max_partitions: i64,
        data_boost_enabled: bool,
        staleness_mode: &str,
        staleness_ms: i64,
    ) -> PyResult<RustDbTaskAwaitable> {
        let params =
            parse_spanner_parameters_json(params_json.as_deref()).map_err(spanner_err_to_py)?;
        let staleness = parse_staleness(staleness_mode, staleness_ms);
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task(
            py,
            async move {
                client
                    .execute_partitioned_arrow(
                        &sql,
                        &params,
                        max_partitions,
                        data_boost_enabled,
                        staleness,
                    )
                    .await
            },
            |py, (shm_name, byte_len, row_count, region)| {
                let cursor = ArrowCursor::from_shm_region(shm_name, byte_len, row_count, region);
                Ok(Py::new(py, cursor)?.into_any())
            },
            spanner_err_to_py,
        )
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySpannerClient>()?;
    Ok(())
}
