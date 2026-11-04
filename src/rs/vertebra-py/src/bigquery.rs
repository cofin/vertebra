//! PyO3 bindings for `vertebra_db::bigquery` (`BigQueryClient`, REST v2 query/dry-run, and
//! Storage Read/Write API v1 zero-copy `ArrowCursor` pipelines).

use std::collections::HashMap;
use std::sync::Arc;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use vertebra_db::bigquery::{
    BigQueryAppendResult, BigQueryArrowCompression, BigQueryCellValue,
    BigQueryClient as NativeBigQueryClient, BigQueryClientConfig, BigQueryDryRunResult,
    BigQueryError, BigQueryFieldSchema, BigQueryQueryOptions, BigQueryQueryResult,
    parse_query_parameters_json,
};

use crate::completion::{RustDbTaskAwaitable, block_on_db, spawn_rust_db_task};
use crate::db::ArrowCursor;

fn bq_err_to_py(err: BigQueryError) -> PyErr {
    match err {
        BigQueryError::InvalidArgument(msg) => PyValueError::new_err(msg),
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

fn cell_to_py(py: Python<'_>, cell: &BigQueryCellValue) -> PyResult<Py<PyAny>> {
    match cell {
        BigQueryCellValue::Null => Ok(py.None()),
        BigQueryCellValue::Bool(b) => Ok((*b).into_pyobject(py)?.to_owned().into_any().unbind()),
        BigQueryCellValue::Int64(n) => Ok(n.into_pyobject(py)?.into_any().unbind()),
        BigQueryCellValue::Float64(f) => Ok(f.into_pyobject(py)?.into_any().unbind()),
        BigQueryCellValue::String(s) | BigQueryCellValue::Json(s) => {
            Ok(s.into_pyobject(py)?.into_any().unbind())
        }
        BigQueryCellValue::Bytes(b) => Ok(PyBytes::new(py, b).into_any().unbind()),
        BigQueryCellValue::Array(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(cell_to_py(py, item)?)?;
            }
            Ok(list.into_any().unbind())
        }
        BigQueryCellValue::Struct(fields) => {
            let dict = PyDict::new(py);
            for (k, v) in fields {
                dict.set_item(k, cell_to_py(py, v)?)?;
            }
            Ok(dict.into_any().unbind())
        }
    }
}

fn field_schema_to_py(py: Python<'_>, field: &BigQueryFieldSchema) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("name", &field.name)?;
    dict.set_item("type", &field.field_type)?;
    dict.set_item("mode", &field.mode)?;
    if !field.fields.is_empty() {
        let sub = PyList::empty(py);
        for child in &field.fields {
            sub.append(field_schema_to_py(py, child)?)?;
        }
        dict.set_item("fields", sub)?;
    }
    Ok(dict.into_any().unbind())
}

fn query_result_to_py(py: Python<'_>, res: BigQueryQueryResult) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("columns", res.columns)?;

    let schema_list = PyList::empty(py);
    for f in &res.schema {
        schema_list.append(field_schema_to_py(py, f)?)?;
    }
    dict.set_item("schema", schema_list)?;

    let rows_list = PyList::empty(py);
    for row in &res.rows {
        let row_list = PyList::empty(py);
        for cell in row {
            row_list.append(cell_to_py(py, cell)?)?;
        }
        rows_list.append(row_list)?;
    }
    dict.set_item("rows", rows_list)?;
    dict.set_item("total_rows", res.total_rows)?;
    dict.set_item("total_bytes_processed", res.total_bytes_processed)?;
    dict.set_item("total_bytes_billed", res.total_bytes_billed)?;
    dict.set_item("slot_ms", res.slot_ms)?;
    dict.set_item("cache_hit", res.cache_hit)?;
    dict.set_item("job_id", res.job_id)?;
    dict.set_item("session_id", res.session_id)?;
    dict.set_item("num_dml_affected_rows", res.num_dml_affected_rows)?;
    Ok(dict.into_any().unbind())
}

fn dry_run_result_to_py(py: Python<'_>, res: BigQueryDryRunResult) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("total_bytes_processed", res.total_bytes_processed)?;
    dict.set_item("total_bytes_billed", res.total_bytes_billed)?;
    dict.set_item("cache_hit", res.cache_hit)?;
    dict.set_item("referenced_tables", res.referenced_tables)?;

    let schema_list = PyList::empty(py);
    for f in &res.schema {
        schema_list.append(field_schema_to_py(py, f)?)?;
    }
    dict.set_item("schema", schema_list)?;
    Ok(dict.into_any().unbind())
}

fn append_result_to_py(py: Python<'_>, res: BigQueryAppendResult) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("appended_rows", res.appended_rows)?;
    dict.set_item("write_stream", res.write_stream)?;
    Ok(dict.into_any().unbind())
}

/// Pure-Rust Google Cloud BigQuery client exposed to Python (`vertebra._vertebra.BigQueryClient`).
/// `ca_pem` applies to the REST API client; the Storage Read/Write API clients use the system trust store and ignore it.
#[pyclass(
    name = "BigQueryClient",
    module = "vertebra._vertebra",
    frozen,
    immutable_type
)]
pub struct PyBigQueryClient {
    inner: Arc<NativeBigQueryClient>,
}

#[pymethods]
impl PyBigQueryClient {
    #[new]
    #[pyo3(signature = (
        project_id,
        dataset_id=None,
        location=None,
        rest_endpoint=None,
        storage_endpoint=None,
        maximum_bytes_billed=None,
        job_timeout_ms=None,
        priority=None,
        labels=None,
        auth_options=None,
        pool_size=4,
        ca_pem=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        project_id: String,
        dataset_id: Option<String>,
        location: Option<String>,
        rest_endpoint: Option<String>,
        storage_endpoint: Option<String>,
        maximum_bytes_billed: Option<i64>,
        job_timeout_ms: Option<u64>,
        priority: Option<String>,
        labels: Option<HashMap<String, String>>,
        auth_options: Option<HashMap<String, String>>,
        pool_size: usize,
        ca_pem: Option<Vec<u8>>,
    ) -> PyResult<Self> {
        let mut cfg = BigQueryClientConfig::new(project_id);
        cfg.dataset_id = dataset_id.filter(|s| !s.trim().is_empty());
        cfg.location = location.filter(|s| !s.trim().is_empty());
        if let Some(ep) = rest_endpoint.filter(|s| !s.trim().is_empty()) {
            cfg.rest_endpoint = ep;
        }
        if let Some(sep) = storage_endpoint.filter(|s| !s.trim().is_empty()) {
            cfg.storage_endpoint = sep;
        }
        cfg.maximum_bytes_billed = maximum_bytes_billed;
        cfg.job_timeout_ms = job_timeout_ms;
        cfg.priority = priority.filter(|s| !s.trim().is_empty());
        if let Some(lbls) = labels {
            cfg.labels = lbls;
        }
        if let Some(auth) = auth_options {
            cfg.auth_options = auth;
        }
        cfg.pool_size = pool_size.max(1);
        cfg.ca_pem = ca_pem;

        let client = py
            .detach(|| NativeBigQueryClient::new(cfg))
            .map_err(bq_err_to_py)?;
        Ok(Self {
            inner: Arc::new(client),
        })
    }

    #[getter]
    fn project_id(&self) -> &str {
        &self.inner.config().project_id
    }

    #[getter]
    fn dataset_id(&self) -> Option<&str> {
        self.inner.config().dataset_id.as_deref()
    }

    #[getter]
    fn location(&self) -> Option<&str> {
        self.inner.config().location.as_deref()
    }

    #[getter]
    fn rest_endpoint(&self) -> String {
        self.inner.config().effective_rest_endpoint()
    }

    #[getter]
    fn storage_endpoint(&self) -> String {
        self.inner.config().effective_storage_endpoint()
    }

    #[getter]
    fn maximum_bytes_billed(&self) -> Option<i64> {
        self.inner.config().maximum_bytes_billed
    }

    #[getter]
    fn job_timeout_ms(&self) -> Option<u64> {
        self.inner.config().job_timeout_ms
    }

    #[getter]
    fn priority(&self) -> Option<&str> {
        self.inner.config().priority.as_deref()
    }

    #[getter]
    fn pool_size(&self) -> usize {
        self.inner.config().pool_size
    }

    #[pyo3(signature = (
        sql,
        params_json=None,
        session_id=None,
        create_session=false,
        destination_table=None,
        write_disposition=None,
        maximum_bytes_billed=None,
        job_timeout_ms=None,
        priority=None,
        labels=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn query(
        &self,
        py: Python<'_>,
        sql: &str,
        params_json: Option<&str>,
        session_id: Option<String>,
        create_session: bool,
        destination_table: Option<String>,
        write_disposition: Option<String>,
        maximum_bytes_billed: Option<i64>,
        job_timeout_ms: Option<u64>,
        priority: Option<String>,
        labels: Option<HashMap<String, String>>,
    ) -> PyResult<Py<PyAny>> {
        let params = parse_query_parameters_json(params_json).map_err(bq_err_to_py)?;
        let opts = BigQueryQueryOptions {
            session_id,
            create_session,
            dry_run: false,
            destination_table,
            write_disposition,
            maximum_bytes_billed,
            job_timeout_ms,
            max_results: None,
            priority,
            labels: labels.unwrap_or_default(),
        };
        let client = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        let res = py
            .detach(|| {
                block_on_db(async move {
                    client
                        .query_with_options(&sql_owned, &params, &opts)
                        .await
                        .map_err(|e| e.to_string())
                })
            })
            .map_err(PyRuntimeError::new_err)?;
        query_result_to_py(py, res)
    }

    #[pyo3(signature = (
        sql,
        params_json=None,
        session_id=None,
        create_session=false,
        destination_table=None,
        write_disposition=None,
        maximum_bytes_billed=None,
        job_timeout_ms=None,
        priority=None,
        labels=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn query_async(
        &self,
        py: Python<'_>,
        sql: String,
        params_json: Option<String>,
        session_id: Option<String>,
        create_session: bool,
        destination_table: Option<String>,
        write_disposition: Option<String>,
        maximum_bytes_billed: Option<i64>,
        job_timeout_ms: Option<u64>,
        priority: Option<String>,
        labels: Option<HashMap<String, String>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let params = parse_query_parameters_json(params_json.as_deref()).map_err(bq_err_to_py)?;
        let opts = BigQueryQueryOptions {
            session_id,
            create_session,
            dry_run: false,
            destination_table,
            write_disposition,
            maximum_bytes_billed,
            job_timeout_ms,
            max_results: None,
            priority,
            labels: labels.unwrap_or_default(),
        };
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task(
            py,
            async move { client.query_with_options(&sql, &params, &opts).await },
            query_result_to_py,
            bq_err_to_py,
        )
    }

    #[pyo3(signature = (sql, params_json=None))]
    fn dry_run(&self, py: Python<'_>, sql: &str, params_json: Option<&str>) -> PyResult<Py<PyAny>> {
        let params = parse_query_parameters_json(params_json).map_err(bq_err_to_py)?;
        let client = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        let res = py
            .detach(|| {
                block_on_db(async move {
                    client
                        .dry_run(&sql_owned, &params)
                        .await
                        .map_err(|e| e.to_string())
                })
            })
            .map_err(PyRuntimeError::new_err)?;
        dry_run_result_to_py(py, res)
    }

    #[pyo3(signature = (sql, params_json=None))]
    fn dry_run_async(
        &self,
        py: Python<'_>,
        sql: String,
        params_json: Option<String>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let params = parse_query_parameters_json(params_json.as_deref()).map_err(bq_err_to_py)?;
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task(
            py,
            async move { client.dry_run(&sql, &params).await },
            dry_run_result_to_py,
            bq_err_to_py,
        )
    }

    #[pyo3(signature = (
        project_id,
        dataset_id,
        table_id,
        selected_fields=None,
        row_restriction=None,
        max_streams=4,
        compression="LZ4_FRAME"
    ))]
    #[allow(clippy::too_many_arguments)]
    fn read_table_arrow(
        &self,
        py: Python<'_>,
        project_id: &str,
        dataset_id: &str,
        table_id: &str,
        selected_fields: Option<Vec<String>>,
        row_restriction: Option<String>,
        max_streams: i32,
        compression: &str,
    ) -> PyResult<ArrowCursor> {
        let codec = BigQueryArrowCompression::from_str_opt(Some(compression));
        let fields = selected_fields.unwrap_or_default();
        let client = Arc::clone(&self.inner);
        let p = project_id.to_string();
        let d = dataset_id.to_string();
        let t = table_id.to_string();
        let (shm_name, byte_len, row_count, region) = py
            .detach(|| {
                block_on_db(async move {
                    client
                        .read_table_to_shm_arrow_with_compression(
                            &p,
                            &d,
                            &t,
                            &fields,
                            row_restriction.as_deref(),
                            max_streams,
                            codec,
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
        project_id,
        dataset_id,
        table_id,
        selected_fields=None,
        row_restriction=None,
        max_streams=4,
        compression="LZ4_FRAME"
    ))]
    #[allow(clippy::too_many_arguments)]
    fn read_table_arrow_async(
        &self,
        py: Python<'_>,
        project_id: String,
        dataset_id: String,
        table_id: String,
        selected_fields: Option<Vec<String>>,
        row_restriction: Option<String>,
        max_streams: i32,
        compression: &str,
    ) -> PyResult<RustDbTaskAwaitable> {
        let codec = BigQueryArrowCompression::from_str_opt(Some(compression));
        let fields = selected_fields.unwrap_or_default();
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task(
            py,
            async move {
                client
                    .read_table_to_shm_arrow_with_compression(
                        &project_id,
                        &dataset_id,
                        &table_id,
                        &fields,
                        row_restriction.as_deref(),
                        max_streams,
                        codec,
                    )
                    .await
            },
            |py, (shm_name, byte_len, row_count, region)| {
                let cursor = ArrowCursor::from_shm_region(shm_name, byte_len, row_count, region);
                Ok(Py::new(py, cursor)?.into_any())
            },
            bq_err_to_py,
        )
    }

    #[pyo3(signature = (parent_table, arrow_ipc_bytes, mode="_default"))]
    fn append_arrow(
        &self,
        py: Python<'_>,
        parent_table: &str,
        arrow_ipc_bytes: &[u8],
        mode: &str,
    ) -> PyResult<Py<PyAny>> {
        let client = Arc::clone(&self.inner);
        let table = parent_table.to_string();
        let payload = arrow_ipc_bytes.to_vec();
        let mode_owned = mode.to_string();
        let res = py
            .detach(|| {
                block_on_db(async move {
                    client
                        .append_arrow_stream(&table, &payload, &mode_owned)
                        .await
                        .map_err(|e| e.to_string())
                })
            })
            .map_err(PyRuntimeError::new_err)?;
        append_result_to_py(py, res)
    }

    #[pyo3(signature = (parent_table, arrow_ipc_bytes, mode="_default"))]
    fn append_arrow_async(
        &self,
        py: Python<'_>,
        parent_table: String,
        arrow_ipc_bytes: Vec<u8>,
        mode: &str,
    ) -> PyResult<RustDbTaskAwaitable> {
        let client = Arc::clone(&self.inner);
        let mode_owned = mode.to_string();
        spawn_rust_db_task(
            py,
            async move {
                client
                    .append_arrow_stream(&parent_table, &arrow_ipc_bytes, &mode_owned)
                    .await
            },
            append_result_to_py,
            bq_err_to_py,
        )
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyBigQueryClient>()?;
    Ok(())
}
