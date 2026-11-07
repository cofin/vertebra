//! `vertebra-bigquery` crate and `vertebra._bigquery` PyO3 extension module.

pub mod arrow_ipc;
pub mod client;
pub mod codec;
pub mod errors;

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{PyBytes, PyDict, PyList};
use vertebra_db::py_async::{
    RustDbTaskAwaitable, block_on_db, construct_py_arrow_cursor, spawn_rust_db_task_with_attr,
};

use crate::client::{BigQueryClient as NativeBigQueryClient, BigQueryClientConfig};
use crate::codec::{
    BigQueryAppendResult, BigQueryArrowCompression, BigQueryCellValue, BigQueryDryRunResult,
    BigQueryFieldSchema, BigQueryQueryOptions, BigQueryQueryResult, parse_query_parameters_json,
};
use crate::errors::bigquery_error_to_py;

const BIGQUERY_COMPLETION_DRAINER_ATTR: &str = "_vertebra_bigquery_completion_drainer";

/// Whether this build targets free-threaded Python (3.14t+).
#[cfg(Py_GIL_DISABLED)]
pub const FREE_THREADED: bool = true;
#[cfg(not(Py_GIL_DISABLED))]
pub const FREE_THREADED: bool = false;

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

fn extract_arrow_ipc_bytes(arrow_ipc_bytes: &Bound<'_, PyAny>) -> PyResult<Bytes> {
    if let Ok(backed) = arrow_ipc_bytes.extract::<PyBackedBytes>() {
        return Ok(Bytes::from_owner(backed));
    }
    let buf = PyBuffer::<u8>::get(arrow_ipc_bytes)?;
    if !buf.is_c_contiguous() {
        return Err(PyValueError::new_err(
            "arrow_ipc_bytes buffer must be C-contiguous",
        ));
    }
    Ok(Bytes::from(buf.to_vec(arrow_ipc_bytes.py())?))
}

#[allow(clippy::too_many_arguments)]
fn build_bigquery_config(
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
) -> BigQueryClientConfig {
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
    cfg
}

/// Synchronous pure-Rust Google Cloud BigQuery client exposed to Python (`vertebra._bigquery.SyncBigQueryClient`).
#[pyclass(
    name = "SyncBigQueryClient",
    module = "vertebra._bigquery",
    frozen,
    immutable_type
)]
pub struct PySyncBigQueryClient {
    inner: Arc<NativeBigQueryClient>,
}

impl PySyncBigQueryClient {
    pub fn inner(&self) -> &Arc<NativeBigQueryClient> {
        &self.inner
    }
}

#[pymethods]
impl PySyncBigQueryClient {
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
        let cfg = build_bigquery_config(
            project_id,
            dataset_id,
            location,
            rest_endpoint,
            storage_endpoint,
            maximum_bytes_billed,
            job_timeout_ms,
            priority,
            labels,
            auth_options,
            pool_size,
            ca_pem,
        );
        let client = py
            .detach(|| NativeBigQueryClient::new(cfg))
            .map_err(|e| bigquery_error_to_py(py, e))?;
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
    fn rest_endpoint(&self) -> &str {
        self.inner.rest_endpoint()
    }

    #[getter]
    fn storage_endpoint(&self) -> &str {
        self.inner.storage_endpoint()
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

    fn close(&self) {}

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
        let params =
            parse_query_parameters_json(params_json).map_err(|e| bigquery_error_to_py(py, e))?;
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
        let res = py
            .detach(|| block_on_db(self.inner.query_with_options(sql, &params, &opts)))
            .map_err(|e| bigquery_error_to_py(py, e))?;
        query_result_to_py(py, res)
    }

    #[pyo3(signature = (sql, params_json=None))]
    fn dry_run(&self, py: Python<'_>, sql: &str, params_json: Option<&str>) -> PyResult<Py<PyAny>> {
        let params =
            parse_query_parameters_json(params_json).map_err(|e| bigquery_error_to_py(py, e))?;
        let res = py
            .detach(|| block_on_db(self.inner.dry_run(sql, &params)))
            .map_err(|e| bigquery_error_to_py(py, e))?;
        dry_run_result_to_py(py, res)
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
    ) -> PyResult<Py<PyAny>> {
        let codec = BigQueryArrowCompression::from_str_opt(Some(compression));
        let fields = selected_fields.unwrap_or_default();
        let (shm_name, byte_len, row_count, region) = py
            .detach(|| {
                block_on_db(self.inner.read_table_to_shm_arrow_with_compression(
                    project_id,
                    dataset_id,
                    table_id,
                    &fields,
                    row_restriction.as_deref(),
                    max_streams,
                    codec,
                ))
            })
            .map_err(|e| bigquery_error_to_py(py, e))?;
        construct_py_arrow_cursor(py, &shm_name, byte_len, row_count, region)
    }

    #[pyo3(signature = (parent_table, arrow_ipc_bytes, mode="_default"))]
    fn append_arrow(
        &self,
        py: Python<'_>,
        parent_table: &str,
        arrow_ipc_bytes: &Bound<'_, PyAny>,
        mode: &str,
    ) -> PyResult<Py<PyAny>> {
        let payload = extract_arrow_ipc_bytes(arrow_ipc_bytes)?;
        let res = py
            .detach(|| {
                block_on_db(
                    self.inner
                        .append_arrow_stream_bytes(parent_table, payload, mode),
                )
            })
            .map_err(|e| bigquery_error_to_py(py, e))?;
        append_result_to_py(py, res)
    }
}

/// Asynchronous pure-Rust Google Cloud BigQuery client exposed to Python (`vertebra._bigquery.AsyncBigQueryClient`).
#[pyclass(
    name = "AsyncBigQueryClient",
    module = "vertebra._bigquery",
    frozen,
    immutable_type
)]
pub struct PyAsyncBigQueryClient {
    inner: Arc<NativeBigQueryClient>,
}

impl PyAsyncBigQueryClient {
    pub fn inner(&self) -> &Arc<NativeBigQueryClient> {
        &self.inner
    }
}

#[pymethods]
impl PyAsyncBigQueryClient {
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
        let cfg = build_bigquery_config(
            project_id,
            dataset_id,
            location,
            rest_endpoint,
            storage_endpoint,
            maximum_bytes_billed,
            job_timeout_ms,
            priority,
            labels,
            auth_options,
            pool_size,
            ca_pem,
        );
        let client = py
            .detach(|| NativeBigQueryClient::new(cfg))
            .map_err(|e| bigquery_error_to_py(py, e))?;
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
    fn rest_endpoint(&self) -> &str {
        self.inner.rest_endpoint()
    }

    #[getter]
    fn storage_endpoint(&self) -> &str {
        self.inner.storage_endpoint()
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

    fn close(&self) {}

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
        let params = parse_query_parameters_json(params_json.as_deref())
            .map_err(|e| bigquery_error_to_py(py, e))?;
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
        spawn_rust_db_task_with_attr(
            py,
            async move { client.query_with_options(&sql, &params, &opts).await },
            query_result_to_py,
            bigquery_error_to_py,
            BIGQUERY_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, params_json=None))]
    fn dry_run(
        &self,
        py: Python<'_>,
        sql: String,
        params_json: Option<String>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let params = parse_query_parameters_json(params_json.as_deref())
            .map_err(|e| bigquery_error_to_py(py, e))?;
        let client = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.dry_run(&sql, &params).await },
            dry_run_result_to_py,
            bigquery_error_to_py,
            BIGQUERY_COMPLETION_DRAINER_ATTR,
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
        spawn_rust_db_task_with_attr(
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
                construct_py_arrow_cursor(py, &shm_name, byte_len, row_count, region)
            },
            bigquery_error_to_py,
            BIGQUERY_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (parent_table, arrow_ipc_bytes, mode="_default"))]
    fn append_arrow(
        &self,
        py: Python<'_>,
        parent_table: String,
        arrow_ipc_bytes: &Bound<'_, PyAny>,
        mode: &str,
    ) -> PyResult<RustDbTaskAwaitable> {
        let payload = extract_arrow_ipc_bytes(arrow_ipc_bytes)?;
        let client = Arc::clone(&self.inner);
        let mode_owned = mode.to_string();
        spawn_rust_db_task_with_attr(
            py,
            async move {
                client
                    .append_arrow_stream_bytes(&parent_table, payload, &mode_owned)
                    .await
            },
            append_result_to_py,
            bigquery_error_to_py,
            BIGQUERY_COMPLETION_DRAINER_ATTR,
        )
    }
}

#[pymodule(gil_used = false)]
pub fn _bigquery(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("FREE_THREADED", FREE_THREADED)?;
    m.add("GIL_USED", false)?;
    m.add_class::<PySyncBigQueryClient>()?;
    m.add_class::<PyAsyncBigQueryClient>()?;
    Ok(())
}
