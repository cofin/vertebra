//! Synchronous (`SyncMySQLClient`) and asynchronous (`AsyncMySQLClient`) PyO3 clients for MySQL 8.4/9
//! and MariaDB built on `mysql_async` (`rustls` + `ring`) and `vertebra_db::py_async`.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use mysql_async::prelude::*;
use mysql_async::{Conn, IsolationLevel, Opts, Params, Pool, Row, TxOpts, Value};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyList, PyString, PyTuple};
use tokio::sync::Mutex as AsyncMutex;
use vertebra_db::py_async::{
    RustDbTaskAwaitable, block_on_db, construct_py_arrow_cursor, spawn_rust_db_task_with_attr,
};

use crate::arrow_ipc::{
    decode_arrow_ipc_to_mysql_rows, encode_result_set_to_shm, encode_result_set_to_shm_batches,
};
use crate::config::MySQLConnectConfig;
use crate::errors::MySQLError;
use crate::protocol::{
    PlaceholderPlan, is_text_only_statement, normalize_mysql_placeholders, split_sql_statements,
};

pub const MYSQL_COMPLETION_DRAINER_ATTR: &str = "_vertebra_mysql_completion_drainer";

/// Intermediate Rust representation of a decoded MySQL cell value that can be produced on a Tokio
/// worker thread without holding the Python GIL and converted into a Python object inside the
/// completion callback.
#[derive(Debug, Clone, PartialEq)]
pub enum DecodedCell {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    Decimal(String),
    Json(String),
    Date(i32, u32, u32),
    DateTime(i32, u32, u32, u32, u32, u32, u32),
    Time(bool, u32, u32, u32, u32, u32),
}

impl DecodedCell {
    pub fn into_py_any(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Self::Null => Ok(py.None()),
            Self::Bool(b) => Ok(PyBool::new(py, b).to_owned().into_any().unbind()),
            Self::Int(i) => Ok(i.into_pyobject(py)?.into_any().unbind()),
            Self::UInt(u) => Ok(u.into_pyobject(py)?.into_any().unbind()),
            Self::Float(f) => Ok(PyFloat::new(py, f).into_any().unbind()),
            Self::Text(s) => Ok(PyString::new(py, &s).into_any().unbind()),
            Self::Bytes(b) => Ok(PyBytes::new(py, &b).into_any().unbind()),
            Self::Decimal(s) => {
                let dec_cls = py.import("decimal")?.getattr("Decimal")?;
                Ok(dec_cls.call1((s.as_str(),))?.into_any().unbind())
            }
            Self::Json(s) => match py.import("vertebra.utils.serializers") {
                Ok(mod_ser) => match mod_ser.getattr("decode_json")?.call1((s.as_str(),)) {
                    Ok(val) => Ok(val.into_any().unbind()),
                    Err(_) => Ok(PyString::new(py, &s).into_any().unbind()),
                },
                Err(_) => Ok(PyString::new(py, &s).into_any().unbind()),
            },
            Self::Date(y, m, d) => {
                let dt_mod = py.import("datetime")?;
                let date_cls = dt_mod.getattr("date")?;
                Ok(date_cls.call1((y, m, d))?.into_any().unbind())
            }
            Self::DateTime(y, m, d, hh, mm, ss, us) => {
                let dt_mod = py.import("datetime")?;
                let dt_cls = dt_mod.getattr("datetime")?;
                let utc = dt_mod.getattr("timezone")?.getattr("utc")?;
                Ok(dt_cls
                    .call1((y, m, d, hh, mm, ss, us, utc))?
                    .into_any()
                    .unbind())
            }
            Self::Time(is_neg, days, hours, minutes, seconds, micros) => {
                let dt_mod = py.import("datetime")?;
                if is_neg || days > 0 || hours >= 24 {
                    let td_cls = dt_mod.getattr("timedelta")?;
                    let total_secs = (days as i64) * 86_400
                        + (hours as i64) * 3_600
                        + (minutes as i64) * 60
                        + (seconds as i64);
                    let signed_us = if is_neg {
                        -(total_secs * 1_000_000 + (micros as i64))
                    } else {
                        total_secs * 1_000_000 + (micros as i64)
                    };
                    Ok(td_cls.call1((0, 0, signed_us))?.into_any().unbind())
                } else {
                    let time_cls = dt_mod.getattr("time")?;
                    Ok(time_cls
                        .call1((hours, minutes, seconds, micros))?
                        .into_any()
                        .unbind())
                }
            }
        }
    }
}

/// Decoded result set with column metadata (`(name, type_code)`), rows, affected count, and `last_insert_id`.
#[derive(Debug, Clone)]
pub struct DecodedResultSet {
    pub columns: Vec<(String, u8)>,
    pub rows: Vec<Vec<DecodedCell>>,
    pub affected_rows: u64,
    pub last_insert_id: Option<u64>,
}

impl DecodedResultSet {
    pub fn rows_to_py_dicts(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let list = PyList::empty(py);
        for row in &self.rows {
            let dict = PyDict::new(py);
            for ((col_name, _), cell) in self.columns.iter().zip(row.iter()) {
                dict.set_item(col_name.as_str(), cell.clone().into_py_any(py)?)?;
            }
            list.append(dict)?;
        }
        Ok(list.into_any().unbind())
    }

    pub fn into_execute_with_description_py(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let rows_py = self.rows_to_py_dicts(py)?;
        let desc_list = PyList::empty(py);
        for (name, code) in &self.columns {
            let tup = PyTuple::new(
                py,
                [
                    PyString::new(py, name).into_any(),
                    (*code as u32).into_pyobject(py)?.into_any(),
                ],
            )?;
            desc_list.append(tup)?;
        }
        let last_id_py = match self.last_insert_id {
            Some(id) if id > 0 => id.into_pyobject(py)?.into_any().unbind(),
            _ => py.None(),
        };
        let out = PyTuple::new(
            py,
            [
                rows_py.into_bound(py),
                desc_list.into_any(),
                self.affected_rows.into_pyobject(py)?.into_any(),
                last_id_py.into_bound(py),
            ],
        )?;
        Ok(out.into_any().unbind())
    }
}

/// Converts a single Python parameter object into a `mysql_async::Value` while holding the GIL.
pub fn py_param_to_mysql_value(val: &Bound<'_, PyAny>) -> Result<Value, MySQLError> {
    if val.is_none() {
        return Ok(Value::NULL);
    }
    if let Ok(b) = val.cast::<PyBool>() {
        return Ok(Value::Int(if b.is_true() { 1 } else { 0 }));
    }
    if let Ok(i) = val.extract::<i64>() {
        return Ok(Value::Int(i));
    }
    if let Ok(u) = val.extract::<u64>() {
        return Ok(Value::UInt(u));
    }
    if let Ok(f) = val.extract::<f64>() {
        return Ok(Value::Double(f));
    }
    if let Ok(s) = val.extract::<String>() {
        return Ok(Value::Bytes(s.into_bytes()));
    }
    if let Ok(b) = val.extract::<Vec<u8>>() {
        return Ok(Value::Bytes(b));
    }
    let type_name = val
        .get_type()
        .name()
        .map(|n| n.to_string())
        .unwrap_or_default();
    if type_name == "Decimal" {
        let s = val
            .str()
            .map_err(|e| MySQLError::data(format!("Failed to format Decimal parameter: {e}")))?
            .to_string();
        return Ok(Value::Bytes(s.into_bytes()));
    }
    if type_name == "datetime" {
        let dt_val = if let Ok(tz) = val.getattr("tzinfo")
            && !tz.is_none()
            && let Ok(dt_mod) = val.py().import("datetime")
            && let Ok(utc) = dt_mod.getattr("timezone").and_then(|t| t.getattr("utc"))
            && let Ok(converted) = val.call_method1("astimezone", (utc,))
        {
            converted
        } else {
            val.clone()
        };
        let year: u16 = dt_val
            .getattr("year")
            .and_then(|v| v.extract())
            .unwrap_or(1970);
        let month: u8 = dt_val
            .getattr("month")
            .and_then(|v| v.extract())
            .unwrap_or(1);
        let day: u8 = dt_val.getattr("day").and_then(|v| v.extract()).unwrap_or(1);
        let hour: u8 = dt_val
            .getattr("hour")
            .and_then(|v| v.extract())
            .unwrap_or(0);
        let minute: u8 = dt_val
            .getattr("minute")
            .and_then(|v| v.extract())
            .unwrap_or(0);
        let second: u8 = dt_val
            .getattr("second")
            .and_then(|v| v.extract())
            .unwrap_or(0);
        let micro: u32 = dt_val
            .getattr("microsecond")
            .and_then(|v| v.extract())
            .unwrap_or(0);
        return Ok(Value::Date(year, month, day, hour, minute, second, micro));
    }
    if type_name == "date" {
        let year: u16 = val.getattr("year").and_then(|v| v.extract()).unwrap_or(1970);
        let month: u8 = val.getattr("month").and_then(|v| v.extract()).unwrap_or(1);
        let day: u8 = val.getattr("day").and_then(|v| v.extract()).unwrap_or(1);
        return Ok(Value::Date(year, month, day, 0, 0, 0, 0));
    }
    if type_name == "time" {
        let hour: u8 = val.getattr("hour").and_then(|v| v.extract()).unwrap_or(0);
        let minute: u8 = val.getattr("minute").and_then(|v| v.extract()).unwrap_or(0);
        let second: u8 = val.getattr("second").and_then(|v| v.extract()).unwrap_or(0);
        let micro: u32 = val
            .getattr("microsecond")
            .and_then(|v| v.extract())
            .unwrap_or(0);
        return Ok(Value::Time(false, 0, hour, minute, second, micro));
    }
    if type_name == "timedelta" {
        let total_seconds: f64 = val
            .call_method0("total_seconds")
            .and_then(|v| v.extract())
            .unwrap_or(0.0);
        let is_neg = total_seconds < 0.0;
        let abs_us = (total_seconds.abs() * 1_000_000.0).round() as u64;
        let micros = (abs_us % 1_000_000) as u32;
        let total_secs = abs_us / 1_000_000;
        let days = (total_secs / 86_400) as u32;
        let rem = total_secs % 86_400;
        let hours = (rem / 3_600) as u8;
        let minutes = ((rem % 3_600) / 60) as u8;
        let seconds = (rem % 60) as u8;
        return Ok(Value::Time(is_neg, days, hours, minutes, seconds, micros));
    }
    if val.is_instance_of::<PyDict>() || val.is_instance_of::<PyList>() {
        let py = val.py();
        if let Ok(ser) = py.import("vertebra.utils.serializers")
            && let Ok(encoded) = ser.getattr("encode_json").and_then(|f| f.call1((val,)))
        {
            if let Ok(b) = encoded.extract::<Vec<u8>>() {
                return Ok(Value::Bytes(b));
            }
            if let Ok(s) = encoded.extract::<String>() {
                return Ok(Value::Bytes(s.into_bytes()));
            }
        }
    }
    let text = val
        .str()
        .map_err(|e| MySQLError::data(format!("Unsupported MySQL parameter value: {e}")))?
        .to_string();
    Ok(Value::Bytes(text.into_bytes()))
}

/// Extracts Python query parameters (`None`, positional sequence, or mapping) and normalizes SQL
/// placeholders into `?` markers and `mysql_async::Params`.
pub fn prepare_sql_and_params(
    sql: &str,
    parameters: Option<&Bound<'_, PyAny>>,
) -> Result<(String, Params), MySQLError> {
    let (normalized_sql, plan) = normalize_mysql_placeholders(sql);
    let Some(params_obj) = parameters else {
        return Ok((normalized_sql, Params::Empty));
    };
    if params_obj.is_none() {
        return Ok((normalized_sql, Params::Empty));
    }

    if let Ok(dict) = params_obj.cast::<PyDict>() {
        if dict.is_empty() {
            return Ok((normalized_sql, Params::Empty));
        }
        match plan {
            PlaceholderPlan::Named(names) => {
                let mut positional = Vec::with_capacity(names.len());
                for name in names {
                    let item = dict.get_item(name.as_str()).ok().flatten().ok_or_else(|| {
                        MySQLError::programming(format!(
                            "Missing named SQL parameter '{name}'"
                        ))
                    })?;
                    positional.push(py_param_to_mysql_value(&item)?);
                }
                return Ok((normalized_sql, Params::Positional(positional)));
            }
            PlaceholderPlan::Positional => {
                let mut positional = Vec::with_capacity(dict.len());
                for (_, v) in dict.iter() {
                    positional.push(py_param_to_mysql_value(&v)?);
                }
                return Ok((normalized_sql, Params::Positional(positional)));
            }
        }
    }

    if let Ok(seq) = params_obj.extract::<Vec<Bound<'_, PyAny>>>() {
        if seq.is_empty() {
            return Ok((normalized_sql, Params::Empty));
        }
        let mut positional = Vec::with_capacity(seq.len());
        for item in &seq {
            positional.push(py_param_to_mysql_value(item)?);
        }
        return Ok((normalized_sql, Params::Positional(positional)));
    }

    Err(MySQLError::programming(
        "MySQL query parameters must be a sequence, mapping, or None",
    ))
}

fn decode_mysql_row(row: Row, columns: &[(String, u8)]) -> Result<Vec<DecodedCell>, MySQLError> {
    let mut cells = Vec::with_capacity(columns.len());
    for (idx, (_, col_type)) in columns.iter().enumerate() {
        let raw = row.as_ref(idx).cloned().unwrap_or(Value::NULL);
        cells.push(decode_mysql_value(raw, *col_type)?);
    }
    Ok(cells)
}

pub fn decode_mysql_value(val: Value, col_type: u8) -> Result<DecodedCell, MySQLError> {
    use mysql_async::consts::ColumnType;
    match val {
        Value::NULL => Ok(DecodedCell::Null),
        Value::Int(i) => Ok(DecodedCell::Int(i)),
        Value::UInt(u) => Ok(DecodedCell::UInt(u)),
        Value::Float(f) => Ok(DecodedCell::Float(f as f64)),
        Value::Double(d) => Ok(DecodedCell::Float(d)),
        Value::Date(y, m, d, hh, mm, ss, us) => {
            if col_type == ColumnType::MYSQL_TYPE_DATE as u8
                || col_type == ColumnType::MYSQL_TYPE_NEWDATE as u8
            {
                Ok(DecodedCell::Date(y as i32, m as u32, d as u32))
            } else {
                Ok(DecodedCell::DateTime(
                    y as i32, m as u32, d as u32, hh as u32, mm as u32, ss as u32, us,
                ))
            }
        }
        Value::Time(is_neg, days, hh, mm, ss, us) => Ok(DecodedCell::Time(
            is_neg,
            days,
            hh as u32,
            mm as u32,
            ss as u32,
            us,
        )),
        Value::Bytes(bytes) => decode_bytes_by_column_type(bytes, col_type),
    }
}

fn decode_bytes_by_column_type(bytes: Vec<u8>, col_type: u8) -> Result<DecodedCell, MySQLError> {
    use mysql_async::consts::ColumnType;
    if col_type == ColumnType::MYSQL_TYPE_DECIMAL as u8
        || col_type == ColumnType::MYSQL_TYPE_NEWDECIMAL as u8
    {
        let s = String::from_utf8(bytes)
            .map_err(|e| MySQLError::data(format!("Invalid UTF-8 in DECIMAL value: {e}")))?;
        return Ok(DecodedCell::Decimal(s));
    }
    if col_type == ColumnType::MYSQL_TYPE_JSON as u8 {
        let s = String::from_utf8(bytes)
            .map_err(|e| MySQLError::data(format!("Invalid UTF-8 in JSON value: {e}")))?;
        return Ok(DecodedCell::Json(s));
    }
    if (col_type == ColumnType::MYSQL_TYPE_TINY as u8
        || col_type == ColumnType::MYSQL_TYPE_SHORT as u8
        || col_type == ColumnType::MYSQL_TYPE_LONG as u8
        || col_type == ColumnType::MYSQL_TYPE_LONGLONG as u8
        || col_type == ColumnType::MYSQL_TYPE_INT24 as u8
        || col_type == ColumnType::MYSQL_TYPE_YEAR as u8)
        && let Ok(s) = std::str::from_utf8(&bytes)
    {
        let trimmed = s.trim();
        if let Ok(i) = trimmed.parse::<i64>() {
            return Ok(DecodedCell::Int(i));
        }
        if let Ok(u) = trimmed.parse::<u64>() {
            return Ok(DecodedCell::UInt(u));
        }
    }
    if (col_type == ColumnType::MYSQL_TYPE_FLOAT as u8
        || col_type == ColumnType::MYSQL_TYPE_DOUBLE as u8)
        && let Ok(s) = std::str::from_utf8(&bytes)
        && let Ok(f) = s.trim().parse::<f64>()
    {
        return Ok(DecodedCell::Float(f));
    }
    if (col_type == ColumnType::MYSQL_TYPE_DATE as u8
        || col_type == ColumnType::MYSQL_TYPE_NEWDATE as u8)
        && let Ok(s) = std::str::from_utf8(&bytes)
        && let Some((y, m, d)) = parse_iso_date(s.trim())
    {
        return Ok(DecodedCell::Date(y, m, d));
    }
    if (col_type == ColumnType::MYSQL_TYPE_DATETIME as u8
        || col_type == ColumnType::MYSQL_TYPE_TIMESTAMP as u8
        || col_type == ColumnType::MYSQL_TYPE_DATETIME2 as u8
        || col_type == ColumnType::MYSQL_TYPE_TIMESTAMP2 as u8)
        && let Ok(s) = std::str::from_utf8(&bytes)
        && let Some((y, m, d, hh, mm, ss, us)) = parse_iso_datetime(s.trim())
    {
        return Ok(DecodedCell::DateTime(y, m, d, hh, mm, ss, us));
    }
    if (col_type == ColumnType::MYSQL_TYPE_TIME as u8
        || col_type == ColumnType::MYSQL_TYPE_TIME2 as u8)
        && let Ok(s) = std::str::from_utf8(&bytes)
        && let Some((neg, days, hh, mm, ss, us)) = parse_iso_time(s.trim())
    {
        return Ok(DecodedCell::Time(neg, days, hh, mm, ss, us));
    }
    if col_type == ColumnType::MYSQL_TYPE_BIT as u8
        || col_type == ColumnType::MYSQL_TYPE_VECTOR as u8
        || col_type == ColumnType::MYSQL_TYPE_BLOB as u8
        || col_type == ColumnType::MYSQL_TYPE_TINY_BLOB as u8
        || col_type == ColumnType::MYSQL_TYPE_MEDIUM_BLOB as u8
        || col_type == ColumnType::MYSQL_TYPE_LONG_BLOB as u8
    {
        return Ok(DecodedCell::Bytes(bytes));
    }
    match String::from_utf8(bytes) {
        Ok(s) => Ok(DecodedCell::Text(s)),
        Err(err) => Ok(DecodedCell::Bytes(err.into_bytes())),
    }
}

fn parse_iso_date(s: &str) -> Option<(i32, u32, u32)> {
    let mut parts = s.split('-');
    let y = parts.next()?.parse::<i32>().ok()?;
    let m = parts.next()?.parse::<u32>().ok()?;
    let d = parts.next()?.parse::<u32>().ok()?;
    Some((y, m, d))
}

fn parse_iso_datetime(s: &str) -> Option<(i32, u32, u32, u32, u32, u32, u32)> {
    let (date_part, time_part) = s.split_once(' ').or_else(|| s.split_once('T'))?;
    let (y, m, d) = parse_iso_date(date_part)?;
    let (_neg, _days, hh, mm, ss, us) = parse_iso_time(time_part)?;
    Some((y, m, d, hh, mm, ss, us))
}

fn parse_iso_time(s: &str) -> Option<(bool, u32, u32, u32, u32, u32)> {
    let (neg, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s),
    };
    let (hms, frac) = match rest.split_once('.') {
        Some((h, f)) => (h, Some(f)),
        None => (rest, None),
    };
    let mut parts = hms.split(':');
    let total_hours = parts.next()?.parse::<u32>().ok()?;
    let mm = parts.next()?.parse::<u32>().ok()?;
    let ss = parts.next()?.parse::<u32>().ok()?;
    let days = total_hours / 24;
    let hh = total_hours % 24;
    let us = match frac {
        Some(f) if !f.is_empty() => {
            let mut padded = f.chars().take(6).collect::<String>();
            while padded.len() < 6 {
                padded.push('0');
            }
            padded.parse::<u32>().ok()?
        }
        _ => 0,
    };
    Some((neg, days, hh, mm, ss, us))
}

/// Shared state backing both [`SyncMySQLClient`] and [`AsyncMySQLClient`].
pub struct MySQLClientCore {
    pub config: MySQLConnectConfig,
    pool: AsyncMutex<Option<Pool>>,
    tx_conn: AsyncMutex<Option<Conn>>,
    in_transaction: AtomicBool,
    closed: AtomicBool,
    active_connection_id: AtomicU32,
    last_insert_id: AtomicU64,
    has_last_insert_id: AtomicBool,
    statement_lru: StdMutex<VecDeque<String>>,
}

impl MySQLClientCore {
    pub fn new(config: MySQLConnectConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            pool: AsyncMutex::new(None),
            tx_conn: AsyncMutex::new(None),
            in_transaction: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            active_connection_id: AtomicU32::new(0),
            last_insert_id: AtomicU64::new(0),
            has_last_insert_id: AtomicBool::new(false),
            statement_lru: StdMutex::new(VecDeque::new()),
        })
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub fn in_transaction(&self) -> bool {
        self.in_transaction.load(Ordering::Acquire)
    }

    pub fn connection_id(&self) -> Option<u32> {
        let id = self.active_connection_id.load(Ordering::Acquire);
        if id > 0 { Some(id) } else { None }
    }

    pub fn lastrowid(&self) -> Option<u64> {
        if self.has_last_insert_id.load(Ordering::Acquire) {
            Some(self.last_insert_id.load(Ordering::Acquire))
        } else {
            None
        }
    }

    pub fn statement_cache_len(&self) -> usize {
        self.statement_lru.lock().map(|g| g.len()).unwrap_or(0)
    }

    fn record_prepared_statement(&self, sql: &str) {
        let cap = self.config.statement_cache_size;
        if cap == 0 {
            return;
        }
        if let Ok(mut guard) = self.statement_lru.lock() {
            if let Some(pos) = guard.iter().position(|s| s == sql) {
                guard.remove(pos);
            } else if guard.len() >= cap {
                guard.pop_front();
            }
            guard.push_back(sql.to_string());
        }
    }

    async fn build_effective_opts(&self, include_pool_constraints: bool) -> Result<Opts, MySQLError> {
        let _dialer = self.config.build_cloud_sql_dialer()?;
        let pw = self.config.resolve_effective_password().await?;
        self.config
            .build_mysql_opts(None, pw, include_pool_constraints)
    }

    async fn get_pool(&self) -> Result<Pool, MySQLError> {
        if self.is_closed() {
            return Err(MySQLError::connection("MySQL client is closed"));
        }
        let mut guard = self.pool.lock().await;
        if let Some(ref pool) = *guard {
            return Ok(pool.clone());
        }
        let opts = self.build_effective_opts(true).await?;
        let pool = Pool::new(opts);
        *guard = Some(pool.clone());
        Ok(pool)
    }

    async fn run_on_conn<F, T>(&self, op: F) -> Result<T, MySQLError>
    where
        F: for<'a> FnOnce(
            &'a mut Conn,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<T, MySQLError>> + Send + 'a>,
        >,
    {
        if self.is_closed() {
            return Err(MySQLError::connection("MySQL client is closed"));
        }
        if self.in_transaction() {
            let mut tx_guard = self.tx_conn.lock().await;
            if let Some(ref mut conn) = *tx_guard {
                self.active_connection_id
                    .store(conn.id(), Ordering::Release);
                return op(conn).await;
            }
        }
        let pool = self.get_pool().await?;
        let mut conn = pool.get_conn().await.map_err(MySQLError::from)?;
        self.active_connection_id
            .store(conn.id(), Ordering::Release);
        op(&mut conn).await
    }

    pub async fn execute_result_set(
        &self,
        sql: String,
        params: Params,
    ) -> Result<DecodedResultSet, MySQLError> {
        let use_text = matches!(params, Params::Empty) && is_text_only_statement(&sql);
        if !use_text {
            self.record_prepared_statement(&sql);
        }
        let rs = self
            .run_on_conn(move |conn| {
                Box::pin(async move {
                    let columns: Vec<(String, u8)>;
                    let mut rows: Vec<Vec<DecodedCell>> = Vec::new();
                    let res_affected: u64;
                    let res_last_id: Option<u64>;
                    if use_text {
                        let mut result =
                            conn.query_iter(sql.as_str()).await.map_err(MySQLError::from)?;
                        columns = result
                            .columns()
                            .as_ref()
                            .map(|cols| {
                                cols.iter()
                                    .map(|c| (c.name_str().into_owned(), c.column_type() as u8))
                                    .collect()
                            })
                            .unwrap_or_default();
                        let raw_rows: Vec<Row> =
                            result.collect().await.map_err(MySQLError::from)?;
                        res_affected = result.affected_rows();
                        res_last_id = result.last_insert_id();
                        for row in raw_rows {
                            rows.push(decode_mysql_row(row, &columns)?);
                        }
                    } else {
                        let stmt = conn.prep(sql.as_str()).await.map_err(MySQLError::from)?;
                        let mut result = conn
                            .exec_iter(&stmt, params)
                            .await
                            .map_err(MySQLError::from)?;
                        columns = result
                            .columns()
                            .as_ref()
                            .map(|cols| {
                                cols.iter()
                                    .map(|c| (c.name_str().into_owned(), c.column_type() as u8))
                                    .collect()
                            })
                            .unwrap_or_default();
                        let raw_rows: Vec<Row> =
                            result.collect().await.map_err(MySQLError::from)?;
                        res_affected = result.affected_rows();
                        res_last_id = result.last_insert_id();
                        for row in raw_rows {
                            rows.push(decode_mysql_row(row, &columns)?);
                        }
                    }
                    let affected = res_affected.max(conn.affected_rows());
                    let last_id = res_last_id.or_else(|| conn.last_insert_id());
                    Ok(DecodedResultSet {
                        columns,
                        rows,
                        affected_rows: affected,
                        last_insert_id: last_id,
                    })
                })
            })
            .await?;
        if let Some(id) = rs.last_insert_id.filter(|&id| id > 0) {
            self.last_insert_id.store(id, Ordering::Release);
            self.has_last_insert_id.store(true, Ordering::Release);
        }
        Ok(rs)
    }

    pub async fn execute_all_sets(
        &self,
        sql: String,
        params: Params,
    ) -> Result<Vec<DecodedResultSet>, MySQLError> {
        let use_text = matches!(params, Params::Empty);
        if !use_text {
            self.record_prepared_statement(&sql);
        }
        self.run_on_conn(move |conn| {
            Box::pin(async move {
                let mut sets = Vec::new();
                if use_text {
                    let mut result = conn.query_iter(sql.as_str()).await.map_err(MySQLError::from)?;
                    while !result.is_empty() {
                        let cols: Vec<(String, u8)> = result
                            .columns()
                            .as_ref()
                            .map(|c| {
                                c.iter()
                                    .map(|col| (col.name_str().into_owned(), col.column_type() as u8))
                                    .collect()
                            })
                            .unwrap_or_default();
                        let raw_rows: Vec<Row> = result.collect().await.map_err(MySQLError::from)?;
                        let mut decoded_rows = Vec::with_capacity(raw_rows.len());
                        for r in raw_rows {
                            decoded_rows.push(decode_mysql_row(r, &cols)?);
                        }
                        sets.push(DecodedResultSet {
                            columns: cols,
                            rows: decoded_rows,
                            affected_rows: result.affected_rows(),
                            last_insert_id: result.last_insert_id(),
                        });
                    }
                } else {
                    let stmt = conn.prep(sql.as_str()).await.map_err(MySQLError::from)?;
                    let mut result = conn.exec_iter(&stmt, params).await.map_err(MySQLError::from)?;
                    while !result.is_empty() {
                        let cols: Vec<(String, u8)> = result
                            .columns()
                            .as_ref()
                            .map(|c| {
                                c.iter()
                                    .map(|col| (col.name_str().into_owned(), col.column_type() as u8))
                                    .collect()
                            })
                            .unwrap_or_default();
                        let raw_rows: Vec<Row> = result.collect().await.map_err(MySQLError::from)?;
                        let mut decoded_rows = Vec::with_capacity(raw_rows.len());
                        for r in raw_rows {
                            decoded_rows.push(decode_mysql_row(r, &cols)?);
                        }
                        sets.push(DecodedResultSet {
                            columns: cols,
                            rows: decoded_rows,
                            affected_rows: result.affected_rows(),
                            last_insert_id: result.last_insert_id(),
                        });
                    }
                }
                Ok(sets)
            })
        })
        .await
    }

    pub async fn execute_many_batch(
        &self,
        sql: String,
        param_sets: Vec<Params>,
    ) -> Result<u64, MySQLError> {
        if param_sets.is_empty() {
            return Ok(0);
        }
        self.record_prepared_statement(&sql);
        let (total_affected, latest_id) = self
            .run_on_conn(move |conn| {
                Box::pin(async move {
                    let stmt = conn.prep(sql.as_str()).await.map_err(MySQLError::from)?;
                    let mut total_affected = 0u64;
                    let mut latest_id: Option<u64> = None;
                    for params in param_sets {
                        conn.exec_drop(&stmt, params).await.map_err(MySQLError::from)?;
                        total_affected += conn.affected_rows();
                        if let Some(id) = conn.last_insert_id().filter(|&id| id > 0) {
                            latest_id = Some(id);
                        }
                    }
                    Ok((total_affected, latest_id))
                })
            })
            .await?;
        if let Some(id) = latest_id {
            self.last_insert_id.store(id, Ordering::Release);
            self.has_last_insert_id.store(true, Ordering::Release);
        }
        Ok(total_affected)
    }

    pub async fn execute_script(&self, script: String) -> Result<u64, MySQLError> {
        let statements = split_sql_statements(&script);
        if statements.is_empty() {
            return Ok(0);
        }
        self.run_on_conn(move |conn| {
            Box::pin(async move {
                let mut total = 0u64;
                for stmt in statements {
                    conn.query_drop(stmt.as_str())
                        .await
                        .map_err(MySQLError::from)?;
                    total += conn.affected_rows();
                }
                Ok(total)
            })
        })
        .await
    }

    pub async fn begin(
        &self,
        isolation_level: Option<String>,
        readonly: bool,
    ) -> Result<(), MySQLError> {
        if self.is_closed() {
            return Err(MySQLError::connection("MySQL client is closed"));
        }
        let mut tx_guard = self.tx_conn.lock().await;
        if self.in_transaction() && tx_guard.is_some() {
            return Ok(());
        }
        let pool = self.get_pool().await?;
        let mut conn = pool.get_conn().await.map_err(MySQLError::from)?;
        self.active_connection_id
            .store(conn.id(), Ordering::Release);

        if let Some(ref iso) = isolation_level {
            let level_clause = match iso.trim().to_ascii_uppercase().as_str() {
                "READ UNCOMMITTED" | "READ_UNCOMMITTED" => "READ UNCOMMITTED",
                "READ COMMITTED" | "READ_COMMITTED" => "READ COMMITTED",
                "REPEATABLE READ" | "REPEATABLE_READ" => "REPEATABLE READ",
                "SERIALIZABLE" => "SERIALIZABLE",
                other => {
                    return Err(MySQLError::programming(format!(
                        "Unsupported MySQL transaction isolation level '{other}'"
                    )));
                }
            };
            conn.query_drop(format!("SET TRANSACTION ISOLATION LEVEL {level_clause}"))
                .await
                .map_err(MySQLError::from)?;
        }
        let start_cmd = if readonly {
            "START TRANSACTION READ ONLY"
        } else {
            "START TRANSACTION"
        };
        conn.query_drop(start_cmd).await.map_err(MySQLError::from)?;
        *tx_guard = Some(conn);
        self.in_transaction.store(true, Ordering::Release);
        Ok(())
    }

    pub async fn commit(&self) -> Result<(), MySQLError> {
        let mut tx_guard = self.tx_conn.lock().await;
        if let Some(mut conn) = tx_guard.take() {
            let res = conn.query_drop("COMMIT").await.map_err(MySQLError::from);
            self.in_transaction.store(false, Ordering::Release);
            res?;
        } else {
            self.in_transaction.store(false, Ordering::Release);
        }
        Ok(())
    }

    pub async fn rollback(&self) -> Result<(), MySQLError> {
        let mut tx_guard = self.tx_conn.lock().await;
        if let Some(mut conn) = tx_guard.take() {
            let res = conn.query_drop("ROLLBACK").await.map_err(MySQLError::from);
            self.in_transaction.store(false, Ordering::Release);
            res?;
        } else {
            self.in_transaction.store(false, Ordering::Release);
        }
        Ok(())
    }

    pub async fn savepoint(&self, name: String) -> Result<String, MySQLError> {
        let valid = validate_savepoint_identifier(&name)?;
        if !self.in_transaction() {
            self.begin(None, false).await?;
        }
        let mut tx_guard = self.tx_conn.lock().await;
        let conn = tx_guard
            .as_mut()
            .ok_or_else(|| MySQLError::operational("No active MySQL transaction for SAVEPOINT"))?;
        conn.query_drop(format!("SAVEPOINT `{valid}`"))
            .await
            .map_err(MySQLError::from)?;
        Ok(valid)
    }

    pub async fn release_savepoint(&self, name: String) -> Result<(), MySQLError> {
        let valid = validate_savepoint_identifier(&name)?;
        let mut tx_guard = self.tx_conn.lock().await;
        let conn = tx_guard.as_mut().ok_or_else(|| {
            MySQLError::operational("No active MySQL transaction for RELEASE SAVEPOINT")
        })?;
        conn.query_drop(format!("RELEASE SAVEPOINT `{valid}`"))
            .await
            .map_err(MySQLError::from)?;
        Ok(())
    }

    pub async fn rollback_to_savepoint(&self, name: String) -> Result<(), MySQLError> {
        let valid = validate_savepoint_identifier(&name)?;
        let mut tx_guard = self.tx_conn.lock().await;
        let conn = tx_guard.as_mut().ok_or_else(|| {
            MySQLError::operational("No active MySQL transaction for ROLLBACK TO SAVEPOINT")
        })?;
        conn.query_drop(format!("ROLLBACK TO SAVEPOINT `{valid}`"))
            .await
            .map_err(MySQLError::from)?;
        Ok(())
    }

    pub async fn ping(&self) -> Result<bool, MySQLError> {
        self.run_on_conn(|conn| {
            Box::pin(async move {
                conn.ping().await.map_err(MySQLError::from)?;
                Ok(true)
            })
        })
        .await
    }

    /// Out-of-band query cancellation (`KILL QUERY <conn_id>`) over a dedicated control connection.
    pub async fn cancel_query_by_id(&self, target_conn_id: u32) -> Result<bool, MySQLError> {
        if target_conn_id == 0 {
            return Ok(false);
        }
        let opts = self.build_effective_opts(false).await?;
        let mut control_conn = Conn::new(opts).await.map_err(MySQLError::from)?;
        let kill_res = control_conn
            .query_drop(format!("KILL QUERY {target_conn_id}"))
            .await
            .map_err(MySQLError::from);
        let _ = control_conn.disconnect().await;
        kill_res?;
        Ok(true)
    }

    pub async fn cancel_active_query(&self) -> Result<bool, MySQLError> {
        let conn_id = self.active_connection_id.load(Ordering::Acquire);
        if conn_id == 0 {
            return Ok(false);
        }
        self.cancel_query_by_id(conn_id).await
    }

    pub async fn close(&self) -> Result<(), MySQLError> {
        self.closed.store(true, Ordering::Release);
        if let Some(mut conn) = self.tx_conn.lock().await.take() {
            let _ = conn.query_drop("ROLLBACK").await;
            let _ = conn.disconnect().await;
        }
        self.in_transaction.store(false, Ordering::Release);
        if let Some(pool) = self.pool.lock().await.take() {
            let _ = pool.disconnect().await;
        }
        let _ = IsolationLevel::RepeatableRead;
        let _ = TxOpts::default();
        Ok(())
    }

    pub async fn load_rows_batched(
        &self,
        table_name: String,
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
        batch_size: usize,
    ) -> Result<u64, MySQLError> {
        if rows.is_empty() || columns.is_empty() {
            return Ok(0);
        }
        let valid_table = validate_mysql_identifier(&table_name)?;
        let mut quoted_cols = Vec::with_capacity(columns.len());
        for col in &columns {
            let valid_col = validate_mysql_identifier(col)?;
            quoted_cols.push(format!("`{valid_col}`"));
        }
        let cols_clause = quoted_cols.join(", ");
        let single_row_placeholders =
            format!("({})", vec!["?"; columns.len()].join(", "));
        let chunk_len = batch_size.max(1);

        let mut total_inserted = 0u64;
        for chunk in rows.chunks(chunk_len) {
            let values_clause = vec![single_row_placeholders.as_str(); chunk.len()].join(", ");
            let sql = format!("INSERT INTO `{valid_table}` ({cols_clause}) VALUES {values_clause}");
            let mut flat_params = Vec::with_capacity(chunk.len() * columns.len());
            for row in chunk {
                for idx in 0..columns.len() {
                    flat_params.push(row.get(idx).cloned().unwrap_or(Value::NULL));
                }
            }
            let rs = self
                .execute_result_set(sql, Params::Positional(flat_params))
                .await?;
            total_inserted += if rs.affected_rows > 0 {
                rs.affected_rows
            } else {
                chunk.len() as u64
            };
        }
        Ok(total_inserted)
    }
}

fn validate_mysql_identifier(ident: &str) -> Result<String, MySQLError> {
    let trimmed = ident.trim().trim_matches('`');
    if trimmed.is_empty()
        || !trimmed
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    {
        return Err(MySQLError::programming(format!(
            "Invalid MySQL identifier '{ident}'"
        )));
    }
    Ok(trimmed.replace('.', "`.`"))
}

fn extract_arrow_load_payload(
    py: Python<'_>,
    source: &Bound<'_, PyAny>,
    columns: Option<Vec<String>>,
) -> Result<(Vec<String>, Vec<Vec<Value>>), MySQLError> {
    if let Ok(raw_bytes) = source.extract::<Vec<u8>>() {
        return decode_arrow_ipc_to_mysql_rows(&raw_bytes, columns.as_deref());
    }
    if let Ok(view_fn) = source.getattr("buffer_view")
        && let Ok(view) = view_fn.call0()
        && let Ok(raw_bytes) = view.extract::<Vec<u8>>()
    {
        return decode_arrow_ipc_to_mysql_rows(&raw_bytes, columns.as_deref());
    }
    if let Ok(inner_shm) = source.getattr("_shm_cursor")
        && !inner_shm.is_none()
        && let Ok(view_fn) = inner_shm.getattr("buffer_view")
        && let Ok(view) = view_fn.call0()
        && let Ok(raw_bytes) = view.extract::<Vec<u8>>()
    {
        return decode_arrow_ipc_to_mysql_rows(&raw_bytes, columns.as_deref());
    }

    if source.hasattr("schema").unwrap_or(false)
        && source.hasattr("num_rows").unwrap_or(false)
        && let Ok(pa_ipc) = py.import("pyarrow.ipc")
        && let Ok(pa) = py.import("pyarrow")
        && let Ok(sink) = pa.call_method0("BufferOutputStream")
        && let Ok(schema) = source.getattr("schema")
        && let Ok(writer) = pa_ipc.call_method1("new_stream", (&sink, schema))
    {
        let write_ok = writer
            .call_method1("write", (source,))
            .or_else(|_| writer.call_method1("write_table", (source,)))
            .or_else(|_| writer.call_method1("write_batch", (source,)))
            .is_ok();
        let _ = writer.call_method0("close");
        if write_ok
            && let Ok(buf) = sink.call_method0("getvalue")
            && let Ok(py_bytes) = buf.call_method0("to_pybytes")
            && let Ok(raw_bytes) = py_bytes.extract::<Vec<u8>>()
        {
            return decode_arrow_ipc_to_mysql_rows(&raw_bytes, columns.as_deref());
        }
    }

    let items: Vec<Bound<'_, PyAny>> = if let Ok(to_pylist) = source.getattr("to_pylist")
        && let Ok(list_obj) = to_pylist.call0()
    {
        list_obj.extract::<Vec<Bound<'_, PyAny>>>().unwrap_or_default()
    } else if let Ok(seq) = source.extract::<Vec<Bound<'_, PyAny>>>() {
        seq
    } else {
        return Err(MySQLError::programming(
            "load_from_arrow expects Arrow IPC bytes, ArrowCursor, pyarrow Table/RecordBatch, or a sequence of row dicts/tuples",
        ));
    };

    if items.is_empty() {
        return Ok((columns.unwrap_or_default(), Vec::new()));
    }
    if let Ok(first_dict) = items[0].cast::<PyDict>() {
        let resolved_cols = if let Some(cols) = columns {
            cols
        } else {
            let mut inferred = Vec::with_capacity(first_dict.len());
            for (k, _) in first_dict.iter() {
                inferred.push(k.extract::<String>().map_err(|e| {
                    MySQLError::programming(format!("Column key must be str: {e}"))
                })?);
            }
            inferred
        };
        let mut out_rows = Vec::with_capacity(items.len());
        for item in &items {
            let dict = item.cast::<PyDict>().map_err(|_| {
                MySQLError::programming("Expected all rows in sequence to be mappings")
            })?;
            let mut row = Vec::with_capacity(resolved_cols.len());
            for col in &resolved_cols {
                match dict.get_item(col.as_str()).ok().flatten() {
                    Some(v) => row.push(py_param_to_mysql_value(&v)?),
                    None => row.push(Value::NULL),
                }
            }
            out_rows.push(row);
        }
        return Ok((resolved_cols, out_rows));
    }

    let resolved_cols = columns.ok_or_else(|| {
        MySQLError::programming("columns must be specified when loading positional row tuples")
    })?;
    let mut out_rows = Vec::with_capacity(items.len());
    for item in &items {
        let seq = item.extract::<Vec<Bound<'_, PyAny>>>().map_err(|_| {
            MySQLError::programming("Row item must be a mapping or positional sequence")
        })?;
        let mut row = Vec::with_capacity(resolved_cols.len());
        for idx in 0..resolved_cols.len() {
            match seq.get(idx) {
                Some(v) => row.push(py_param_to_mysql_value(v)?),
                None => row.push(Value::NULL),
            }
        }
        out_rows.push(row);
    }
    Ok((resolved_cols, out_rows))
}

fn validate_savepoint_identifier(name: &str) -> Result<String, MySQLError> {
    let trimmed = name.trim();
    if trimmed.is_empty()
        || !trimmed
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(MySQLError::programming(format!(
            "Invalid MySQL savepoint identifier '{name}'"
        )));
    }
    Ok(trimmed.to_string())
}

/// Synchronous MySQL 8.4/9 and MariaDB client backed by `mysql_async` on the shared Tokio runtime.
#[pyclass(module = "vertebra._mysql", frozen, immutable_type)]
pub struct SyncMySQLClient {
    pub(crate) core: Arc<MySQLClientCore>,
}

#[pymethods]
impl SyncMySQLClient {
    #[new]
    #[pyo3(signature = (
        dsn = "mysql://root@127.0.0.1:3306/mysql",
        *,
        host = None,
        port = None,
        user = None,
        password = None,
        database = None,
        unix_socket = None,
        ssl_mode = "prefer",
        ssl_ca = None,
        ssl_cert = None,
        ssl_key = None,
        connect_timeout_ms = 10000,
        read_timeout_ms = 30000,
        write_timeout_ms = 30000,
        pool_min_size = 1,
        pool_max_size = 10,
        statement_cache_size = 128,
        charset = "utf8mb4",
        autocommit = true,
        init_command = None,
        cloud_sql_instance = None,
        cloud_sql_ip_type = "PUBLIC",
        enable_iam_auth = false,
        credentials_path = None,
        credentials_json = None,
        target_service_account = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dsn: &str,
        host: Option<String>,
        port: Option<u16>,
        user: Option<String>,
        password: Option<String>,
        database: Option<String>,
        unix_socket: Option<String>,
        ssl_mode: &str,
        ssl_ca: Option<String>,
        ssl_cert: Option<String>,
        ssl_key: Option<String>,
        connect_timeout_ms: u64,
        read_timeout_ms: u64,
        write_timeout_ms: u64,
        pool_min_size: usize,
        pool_max_size: usize,
        statement_cache_size: usize,
        charset: &str,
        autocommit: bool,
        init_command: Option<String>,
        cloud_sql_instance: Option<String>,
        cloud_sql_ip_type: &str,
        enable_iam_auth: bool,
        credentials_path: Option<String>,
        credentials_json: Option<String>,
        target_service_account: Option<String>,
    ) -> PyResult<Self> {
        let config = MySQLConnectConfig::from_dsn_and_kwargs(
            dsn,
            host,
            port,
            user,
            password,
            database,
            unix_socket,
            Some(ssl_mode),
            ssl_ca,
            ssl_cert,
            ssl_key,
            connect_timeout_ms,
            read_timeout_ms,
            write_timeout_ms,
            pool_min_size,
            pool_max_size,
            statement_cache_size,
            charset,
            autocommit,
            init_command,
            cloud_sql_instance,
            cloud_sql_ip_type,
            enable_iam_auth,
            credentials_path,
            credentials_json,
            target_service_account,
        )?;
        Ok(Self {
            core: MySQLClientCore::new(config),
        })
    }

    #[getter]
    pub fn dsn(&self) -> &str {
        &self.core.config.dsn
    }

    #[getter]
    pub fn host(&self) -> &str {
        &self.core.config.host
    }

    #[getter]
    pub fn port(&self) -> u16 {
        self.core.config.port
    }

    #[getter]
    pub fn user(&self) -> &str {
        &self.core.config.user
    }

    #[getter]
    pub fn database(&self) -> &str {
        &self.core.config.database
    }

    #[getter]
    pub fn ssl_mode(&self) -> &str {
        self.core.config.ssl_mode.as_str()
    }

    #[getter]
    pub fn statement_cache_size(&self) -> usize {
        self.core.config.statement_cache_size
    }

    #[getter]
    pub fn enable_iam_auth(&self) -> bool {
        self.core.config.enable_iam_auth
    }

    #[getter]
    pub fn cloud_sql_instance(&self) -> Option<&str> {
        self.core.config.cloud_sql_instance.as_deref()
    }

    #[getter]
    pub fn in_transaction(&self) -> bool {
        self.core.in_transaction()
    }

    #[getter]
    pub fn is_closed(&self) -> bool {
        self.core.is_closed()
    }

    #[getter]
    pub fn connection_id(&self) -> Option<u32> {
        self.core.connection_id()
    }

    #[getter]
    pub fn lastrowid(&self) -> Option<u64> {
        self.core.lastrowid()
    }

    pub fn statement_cache_len(&self) -> usize {
        self.core.statement_cache_len()
    }

    pub fn __repr__(&self) -> String {
        self.core.config.redacted_repr("SyncMySQLClient")
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn execute(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<u64> {
        let (norm_sql, params) = prepare_sql_and_params(sql, parameters)?;
        let core = Arc::clone(&self.core);
        let rs = py.detach(|| block_on_db(async move {
            core.execute_result_set(norm_sql, params).await
        }))?;
        Ok(rs.affected_rows)
    }

    pub fn execute_many(
        &self,
        py: Python<'_>,
        sql: &str,
        parameter_sets: &Bound<'_, PyAny>,
    ) -> PyResult<u64> {
        let seq = parameter_sets.extract::<Vec<Bound<'_, PyAny>>>()?;
        let mut compiled_sets = Vec::with_capacity(seq.len());
        let mut normalized_sql = String::new();
        for (idx, item) in seq.iter().enumerate() {
            let (norm, params) = prepare_sql_and_params(sql, Some(item))?;
            if idx == 0 {
                normalized_sql = norm;
            }
            compiled_sets.push(params);
        }
        let core = Arc::clone(&self.core);
        let affected = py.detach(|| block_on_db(async move {
            core.execute_many_batch(normalized_sql, compiled_sets).await
        }))?;
        Ok(affected)
    }

    pub fn execute_script(&self, py: Python<'_>, sql: &str) -> PyResult<u64> {
        let script = sql.to_string();
        let core = Arc::clone(&self.core);
        let affected = py.detach(|| block_on_db(async move { core.execute_script(script).await }))?;
        Ok(affected)
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn execute_with_description(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let (norm_sql, params) = prepare_sql_and_params(sql, parameters)?;
        let core = Arc::clone(&self.core);
        let rs = py.detach(|| block_on_db(async move {
            core.execute_result_set(norm_sql, params).await
        }))?;
        rs.into_execute_with_description_py(py)
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn fetch_all(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let (norm_sql, params) = prepare_sql_and_params(sql, parameters)?;
        let core = Arc::clone(&self.core);
        let rs = py.detach(|| block_on_db(async move {
            core.execute_result_set(norm_sql, params).await
        }))?;
        rs.rows_to_py_dicts(py)
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn fetch_one(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let (norm_sql, params) = prepare_sql_and_params(sql, parameters)?;
        let core = Arc::clone(&self.core);
        let mut rs = py.detach(|| block_on_db(async move {
            core.execute_result_set(norm_sql, params).await
        }))?;
        if rs.rows.is_empty() {
            return Err(MySQLError::not_found("Query returned no rows").to_py_err(py));
        }
        rs.rows.truncate(1);
        let list = rs.rows_to_py_dicts(py)?;
        Ok(list.bind(py).get_item(0)?.unbind())
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn fetch_optional(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let (norm_sql, params) = prepare_sql_and_params(sql, parameters)?;
        let core = Arc::clone(&self.core);
        let mut rs = py.detach(|| block_on_db(async move {
            core.execute_result_set(norm_sql, params).await
        }))?;
        if rs.rows.is_empty() {
            return Ok(py.None());
        }
        rs.rows.truncate(1);
        let list = rs.rows_to_py_dicts(py)?;
        Ok(list.bind(py).get_item(0)?.unbind())
    }

    #[pyo3(signature = (sql, parameters=None, size=100))]
    pub fn fetch_many(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
        size: usize,
    ) -> PyResult<Py<PyAny>> {
        let (norm_sql, params) = prepare_sql_and_params(sql, parameters)?;
        let core = Arc::clone(&self.core);
        let mut rs = py.detach(|| block_on_db(async move {
            core.execute_result_set(norm_sql, params).await
        }))?;
        rs.rows.truncate(size);
        rs.rows_to_py_dicts(py)
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn select_value(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let (norm_sql, params) = prepare_sql_and_params(sql, parameters)?;
        let core = Arc::clone(&self.core);
        let rs = py.detach(|| block_on_db(async move {
            core.execute_result_set(norm_sql, params).await
        }))?;
        let Some(first_row) = rs.rows.into_iter().next() else {
            return Err(MySQLError::not_found("Query returned no rows").to_py_err(py));
        };
        let Some(first_cell) = first_row.into_iter().next() else {
            return Ok(py.None());
        };
        first_cell.into_py_any(py)
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn fetch_all_sets(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let (norm_sql, params) = prepare_sql_and_params(sql, parameters)?;
        let core = Arc::clone(&self.core);
        let sets = py.detach(|| block_on_db(async move {
            core.execute_all_sets(norm_sql, params).await
        }))?;
        let outer = PyList::empty(py);
        for set in sets {
            outer.append(set.rows_to_py_dicts(py)?)?;
        }
        Ok(outer.into_any().unbind())
    }

    #[pyo3(signature = (isolation_level=None, readonly=false))]
    pub fn begin(
        &self,
        py: Python<'_>,
        isolation_level: Option<String>,
        readonly: bool,
    ) -> PyResult<()> {
        let core = Arc::clone(&self.core);
        py.detach(|| block_on_db(async move { core.begin(isolation_level, readonly).await }))?;
        Ok(())
    }

    pub fn commit(&self, py: Python<'_>) -> PyResult<()> {
        let core = Arc::clone(&self.core);
        py.detach(|| block_on_db(async move { core.commit().await }))?;
        Ok(())
    }

    pub fn rollback(&self, py: Python<'_>) -> PyResult<()> {
        let core = Arc::clone(&self.core);
        py.detach(|| block_on_db(async move { core.rollback().await }))?;
        Ok(())
    }

    #[pyo3(signature = (name="vtb_sp"))]
    pub fn savepoint(&self, py: Python<'_>, name: &str) -> PyResult<String> {
        let core = Arc::clone(&self.core);
        let sp = name.to_string();
        let out = py.detach(|| block_on_db(async move { core.savepoint(sp).await }))?;
        Ok(out)
    }

    #[pyo3(signature = (name="vtb_sp"))]
    pub fn release_savepoint(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        let core = Arc::clone(&self.core);
        let sp = name.to_string();
        py.detach(|| block_on_db(async move { core.release_savepoint(sp).await }))?;
        Ok(())
    }

    #[pyo3(signature = (name="vtb_sp"))]
    pub fn rollback_to_savepoint(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        let core = Arc::clone(&self.core);
        let sp = name.to_string();
        py.detach(|| block_on_db(async move { core.rollback_to_savepoint(sp).await }))?;
        Ok(())
    }

    pub fn ping(&self, py: Python<'_>) -> PyResult<bool> {
        let core = Arc::clone(&self.core);
        let ok = py.detach(|| block_on_db(async move { core.ping().await }))?;
        Ok(ok)
    }

    pub fn cancel(&self, py: Python<'_>) -> PyResult<bool> {
        let core = Arc::clone(&self.core);
        let cancelled = py.detach(|| block_on_db(async move { core.cancel_active_query().await }))?;
        Ok(cancelled)
    }

    pub fn cancel_query(&self, py: Python<'_>, connection_id: u32) -> PyResult<bool> {
        let core = Arc::clone(&self.core);
        let cancelled = py.detach(|| block_on_db(async move {
            core.cancel_query_by_id(connection_id).await
        }))?;
        Ok(cancelled)
    }

    pub fn close(&self, py: Python<'_>) -> PyResult<()> {
        let core = Arc::clone(&self.core);
        py.detach(|| block_on_db(async move { core.close().await }))?;
        Ok(())
    }

    #[pyo3(signature = (sql, parameters=None, *, batch_size=65536))]
    pub fn select_to_arrow(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
        batch_size: usize,
    ) -> PyResult<Py<PyAny>> {
        let (norm_sql, params) = prepare_sql_and_params(sql, parameters)?;
        let core = Arc::clone(&self.core);
        let (shm_name, byte_len, row_count, region) = py.detach(|| {
            block_on_db(async move {
                let rs = core.execute_result_set(norm_sql, params).await?;
                encode_result_set_to_shm(&rs.columns, &rs.rows, batch_size)
            })
        })?;
        construct_py_arrow_cursor(py, &shm_name, byte_len, row_count, region)
    }

    #[pyo3(signature = (sql, parameters=None, *, batch_size=65536))]
    pub fn stream_arrow_batches(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
        batch_size: usize,
    ) -> PyResult<Py<PyAny>> {
        let (norm_sql, params) = prepare_sql_and_params(sql, parameters)?;
        let core = Arc::clone(&self.core);
        let batches = py.detach(|| {
            block_on_db(async move {
                let rs = core.execute_result_set(norm_sql, params).await?;
                encode_result_set_to_shm_batches(&rs.columns, &rs.rows, batch_size)
            })
        })?;
        let out = PyList::empty(py);
        for (shm_name, byte_len, row_count, region) in batches {
            let cursor = construct_py_arrow_cursor(py, &shm_name, byte_len, row_count, region)?;
            out.append(cursor.into_bound(py))?;
        }
        Ok(out.into_any().unbind())
    }

    #[pyo3(signature = (table_name, source, *, columns=None, batch_size=1000))]
    pub fn load_from_arrow(
        &self,
        py: Python<'_>,
        table_name: &str,
        source: &Bound<'_, PyAny>,
        columns: Option<Vec<String>>,
        batch_size: usize,
    ) -> PyResult<u64> {
        let (resolved_cols, rows) = extract_arrow_load_payload(py, source, columns)?;
        let table = table_name.to_string();
        let core = Arc::clone(&self.core);
        let inserted = py.detach(|| {
            block_on_db(async move {
                core.load_rows_batched(table, resolved_cols, rows, batch_size)
                    .await
            })
        })?;
        Ok(inserted)
    }
}

/// Asynchronous MySQL 8.4/9 and MariaDB client backed by `mysql_async` and `vertebra_db::py_async`
/// (`_vertebra_mysql_completion_drainer`).
#[pyclass(module = "vertebra._mysql", frozen, immutable_type)]
pub struct AsyncMySQLClient {
    pub(crate) core: Arc<MySQLClientCore>,
}

#[pymethods]
impl AsyncMySQLClient {
    #[new]
    #[pyo3(signature = (
        dsn = "mysql://root@127.0.0.1:3306/mysql",
        *,
        host = None,
        port = None,
        user = None,
        password = None,
        database = None,
        unix_socket = None,
        ssl_mode = "prefer",
        ssl_ca = None,
        ssl_cert = None,
        ssl_key = None,
        connect_timeout_ms = 10000,
        read_timeout_ms = 30000,
        write_timeout_ms = 30000,
        pool_min_size = 1,
        pool_max_size = 10,
        statement_cache_size = 128,
        charset = "utf8mb4",
        autocommit = true,
        init_command = None,
        cloud_sql_instance = None,
        cloud_sql_ip_type = "PUBLIC",
        enable_iam_auth = false,
        credentials_path = None,
        credentials_json = None,
        target_service_account = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dsn: &str,
        host: Option<String>,
        port: Option<u16>,
        user: Option<String>,
        password: Option<String>,
        database: Option<String>,
        unix_socket: Option<String>,
        ssl_mode: &str,
        ssl_ca: Option<String>,
        ssl_cert: Option<String>,
        ssl_key: Option<String>,
        connect_timeout_ms: u64,
        read_timeout_ms: u64,
        write_timeout_ms: u64,
        pool_min_size: usize,
        pool_max_size: usize,
        statement_cache_size: usize,
        charset: &str,
        autocommit: bool,
        init_command: Option<String>,
        cloud_sql_instance: Option<String>,
        cloud_sql_ip_type: &str,
        enable_iam_auth: bool,
        credentials_path: Option<String>,
        credentials_json: Option<String>,
        target_service_account: Option<String>,
    ) -> PyResult<Self> {
        let config = MySQLConnectConfig::from_dsn_and_kwargs(
            dsn,
            host,
            port,
            user,
            password,
            database,
            unix_socket,
            Some(ssl_mode),
            ssl_ca,
            ssl_cert,
            ssl_key,
            connect_timeout_ms,
            read_timeout_ms,
            write_timeout_ms,
            pool_min_size,
            pool_max_size,
            statement_cache_size,
            charset,
            autocommit,
            init_command,
            cloud_sql_instance,
            cloud_sql_ip_type,
            enable_iam_auth,
            credentials_path,
            credentials_json,
            target_service_account,
        )?;
        Ok(Self {
            core: MySQLClientCore::new(config),
        })
    }

    #[getter]
    pub fn dsn(&self) -> &str {
        &self.core.config.dsn
    }

    #[getter]
    pub fn host(&self) -> &str {
        &self.core.config.host
    }

    #[getter]
    pub fn port(&self) -> u16 {
        self.core.config.port
    }

    #[getter]
    pub fn user(&self) -> &str {
        &self.core.config.user
    }

    #[getter]
    pub fn database(&self) -> &str {
        &self.core.config.database
    }

    #[getter]
    pub fn ssl_mode(&self) -> &str {
        self.core.config.ssl_mode.as_str()
    }

    #[getter]
    pub fn statement_cache_size(&self) -> usize {
        self.core.config.statement_cache_size
    }

    #[getter]
    pub fn enable_iam_auth(&self) -> bool {
        self.core.config.enable_iam_auth
    }

    #[getter]
    pub fn cloud_sql_instance(&self) -> Option<&str> {
        self.core.config.cloud_sql_instance.as_deref()
    }

    #[getter]
    pub fn in_transaction(&self) -> bool {
        self.core.in_transaction()
    }

    #[getter]
    pub fn is_closed(&self) -> bool {
        self.core.is_closed()
    }

    #[getter]
    pub fn connection_id(&self) -> Option<u32> {
        self.core.connection_id()
    }

    #[getter]
    pub fn lastrowid(&self) -> Option<u64> {
        self.core.lastrowid()
    }

    pub fn statement_cache_len(&self) -> usize {
        self.core.statement_cache_len()
    }

    pub fn __repr__(&self) -> String {
        self.core.config.redacted_repr("AsyncMySQLClient")
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn execute(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let (norm_sql, params) = prepare_sql_and_params(sql, parameters)?;
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr(
            py,
            async move {
                let rs = core.execute_result_set(norm_sql, params).await?;
                Ok(rs.affected_rows)
            },
            |py, n: u64| Ok(n.into_pyobject(py)?.into_any().unbind()),
            |py, e: MySQLError| e.to_py_err(py),
            MYSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn execute_many(
        &self,
        py: Python<'_>,
        sql: &str,
        parameter_sets: &Bound<'_, PyAny>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let seq = parameter_sets.extract::<Vec<Bound<'_, PyAny>>>()?;
        let mut compiled_sets = Vec::with_capacity(seq.len());
        let mut normalized_sql = String::new();
        for (idx, item) in seq.iter().enumerate() {
            let (norm, params) = prepare_sql_and_params(sql, Some(item))?;
            if idx == 0 {
                normalized_sql = norm;
            }
            compiled_sets.push(params);
        }
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr(
            py,
            async move { core.execute_many_batch(normalized_sql, compiled_sets).await },
            |py, n: u64| Ok(n.into_pyobject(py)?.into_any().unbind()),
            |py, e: MySQLError| e.to_py_err(py),
            MYSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn execute_script(
        &self,
        py: Python<'_>,
        sql: &str,
    ) -> PyResult<RustDbTaskAwaitable> {
        let script = sql.to_string();
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr(
            py,
            async move { core.execute_script(script).await },
            |py, n: u64| Ok(n.into_pyobject(py)?.into_any().unbind()),
            |py, e: MySQLError| e.to_py_err(py),
            MYSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn execute_with_description(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let (norm_sql, params) = prepare_sql_and_params(sql, parameters)?;
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr(
            py,
            async move { core.execute_result_set(norm_sql, params).await },
            |py, r: DecodedResultSet| r.into_execute_with_description_py(py),
            |py, e: MySQLError| e.to_py_err(py),
            MYSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn fetch_all(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let (norm_sql, params) = prepare_sql_and_params(sql, parameters)?;
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr(
            py,
            async move { core.execute_result_set(norm_sql, params).await },
            |py, r: DecodedResultSet| r.rows_to_py_dicts(py),
            |py, e: MySQLError| e.to_py_err(py),
            MYSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn fetch_one(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let (norm_sql, params) = prepare_sql_and_params(sql, parameters)?;
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr(
            py,
            async move { core.execute_result_set(norm_sql, params).await },
            |py, mut r: DecodedResultSet| {
                if r.rows.is_empty() {
                    return Err(MySQLError::not_found("Query returned no rows").to_py_err(py));
                }
                r.rows.truncate(1);
                let list = r.rows_to_py_dicts(py)?;
                Ok(list.bind(py).get_item(0)?.unbind())
            },
            |py, e: MySQLError| e.to_py_err(py),
            MYSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn fetch_optional(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let (norm_sql, params) = prepare_sql_and_params(sql, parameters)?;
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr(
            py,
            async move { core.execute_result_set(norm_sql, params).await },
            |py, mut r: DecodedResultSet| {
                if r.rows.is_empty() {
                    return Ok(py.None());
                }
                r.rows.truncate(1);
                let list = r.rows_to_py_dicts(py)?;
                Ok(list.bind(py).get_item(0)?.unbind())
            },
            |py, e: MySQLError| e.to_py_err(py),
            MYSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None, size=100))]
    pub fn fetch_many(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
        size: usize,
    ) -> PyResult<RustDbTaskAwaitable> {
        let (norm_sql, params) = prepare_sql_and_params(sql, parameters)?;
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr(
            py,
            async move { core.execute_result_set(norm_sql, params).await },
            move |py, mut r: DecodedResultSet| {
                r.rows.truncate(size);
                r.rows_to_py_dicts(py)
            },
            |py, e: MySQLError| e.to_py_err(py),
            MYSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn select_value(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let (norm_sql, params) = prepare_sql_and_params(sql, parameters)?;
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr(
            py,
            async move { core.execute_result_set(norm_sql, params).await },
            |py, r: DecodedResultSet| {
                let Some(first_row) = r.rows.into_iter().next() else {
                    return Err(MySQLError::not_found("Query returned no rows").to_py_err(py));
                };
                let Some(first_cell) = first_row.into_iter().next() else {
                    return Ok(py.None());
                };
                first_cell.into_py_any(py)
            },
            |py, e: MySQLError| e.to_py_err(py),
            MYSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn fetch_all_sets(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let (norm_sql, params) = prepare_sql_and_params(sql, parameters)?;
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr(
            py,
            async move { core.execute_all_sets(norm_sql, params).await },
            |py, list: Vec<DecodedResultSet>| {
                let outer = PyList::empty(py);
                for s in list {
                    outer.append(s.rows_to_py_dicts(py)?)?;
                }
                Ok(outer.into_any().unbind())
            },
            |py, e: MySQLError| e.to_py_err(py),
            MYSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (isolation_level=None, readonly=false))]
    pub fn begin(
        &self,
        py: Python<'_>,
        isolation_level: Option<String>,
        readonly: bool,
    ) -> PyResult<RustDbTaskAwaitable> {
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr(
            py,
            async move { core.begin(isolation_level, readonly).await },
            |py, (): ()| Ok(py.None()),
            |py, e: MySQLError| e.to_py_err(py),
            MYSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn commit(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr(
            py,
            async move { core.commit().await },
            |py, (): ()| Ok(py.None()),
            |py, e: MySQLError| e.to_py_err(py),
            MYSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn rollback(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr(
            py,
            async move { core.rollback().await },
            |py, (): ()| Ok(py.None()),
            |py, e: MySQLError| e.to_py_err(py),
            MYSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (name="vtb_sp"))]
    pub fn savepoint(
        &self,
        py: Python<'_>,
        name: &str,
    ) -> PyResult<RustDbTaskAwaitable> {
        let core = Arc::clone(&self.core);
        let sp = name.to_string();
        spawn_rust_db_task_with_attr(
            py,
            async move { core.savepoint(sp).await },
            |py, s: String| Ok(PyString::new(py, &s).into_any().unbind()),
            |py, e: MySQLError| e.to_py_err(py),
            MYSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (name="vtb_sp"))]
    pub fn release_savepoint(
        &self,
        py: Python<'_>,
        name: &str,
    ) -> PyResult<RustDbTaskAwaitable> {
        let core = Arc::clone(&self.core);
        let sp = name.to_string();
        spawn_rust_db_task_with_attr(
            py,
            async move { core.release_savepoint(sp).await },
            |py, (): ()| Ok(py.None()),
            |py, e: MySQLError| e.to_py_err(py),
            MYSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (name="vtb_sp"))]
    pub fn rollback_to_savepoint(
        &self,
        py: Python<'_>,
        name: &str,
    ) -> PyResult<RustDbTaskAwaitable> {
        let core = Arc::clone(&self.core);
        let sp = name.to_string();
        spawn_rust_db_task_with_attr(
            py,
            async move { core.rollback_to_savepoint(sp).await },
            |py, (): ()| Ok(py.None()),
            |py, e: MySQLError| e.to_py_err(py),
            MYSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn ping(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr(
            py,
            async move { core.ping().await },
            |py, b: bool| Ok(PyBool::new(py, b).to_owned().into_any().unbind()),
            |py, e: MySQLError| e.to_py_err(py),
            MYSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn cancel(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr(
            py,
            async move { core.cancel_active_query().await },
            |py, b: bool| Ok(PyBool::new(py, b).to_owned().into_any().unbind()),
            |py, e: MySQLError| e.to_py_err(py),
            MYSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn cancel_query(
        &self,
        py: Python<'_>,
        connection_id: u32,
    ) -> PyResult<RustDbTaskAwaitable> {
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr(
            py,
            async move { core.cancel_query_by_id(connection_id).await },
            |py, b: bool| Ok(PyBool::new(py, b).to_owned().into_any().unbind()),
            |py, e: MySQLError| e.to_py_err(py),
            MYSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn close(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr(
            py,
            async move { core.close().await },
            |py, (): ()| Ok(py.None()),
            |py, e: MySQLError| e.to_py_err(py),
            MYSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None, *, batch_size=65536))]
    pub fn select_to_arrow(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
        batch_size: usize,
    ) -> PyResult<RustDbTaskAwaitable> {
        let (norm_sql, params) = prepare_sql_and_params(sql, parameters)?;
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr(
            py,
            async move {
                let rs = core.execute_result_set(norm_sql, params).await?;
                encode_result_set_to_shm(&rs.columns, &rs.rows, batch_size)
            },
            |py, (shm_name, byte_len, row_count, region)| {
                construct_py_arrow_cursor(py, &shm_name, byte_len, row_count, region)
            },
            |py, e: MySQLError| e.to_py_err(py),
            MYSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None, *, batch_size=65536))]
    pub fn stream_arrow_batches(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
        batch_size: usize,
    ) -> PyResult<RustDbTaskAwaitable> {
        let (norm_sql, params) = prepare_sql_and_params(sql, parameters)?;
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr(
            py,
            async move {
                let rs = core.execute_result_set(norm_sql, params).await?;
                encode_result_set_to_shm_batches(&rs.columns, &rs.rows, batch_size)
            },
            |py, batches| {
                let out = PyList::empty(py);
                for (shm_name, byte_len, row_count, region) in batches {
                    let cursor =
                        construct_py_arrow_cursor(py, &shm_name, byte_len, row_count, region)?;
                    out.append(cursor.into_bound(py))?;
                }
                Ok(out.into_any().unbind())
            },
            |py, e: MySQLError| e.to_py_err(py),
            MYSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (table_name, source, *, columns=None, batch_size=1000))]
    pub fn load_from_arrow(
        &self,
        py: Python<'_>,
        table_name: &str,
        source: &Bound<'_, PyAny>,
        columns: Option<Vec<String>>,
        batch_size: usize,
    ) -> PyResult<RustDbTaskAwaitable> {
        let (resolved_cols, rows) = extract_arrow_load_payload(py, source, columns)?;
        let table = table_name.to_string();
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr(
            py,
            async move {
                core.load_rows_batched(table, resolved_cols, rows, batch_size)
                    .await
            },
            |py, n: u64| Ok(n.into_pyobject(py)?.into_any().unbind()),
            |py, e: MySQLError| e.to_py_err(py),
            MYSQL_COMPLETION_DRAINER_ATTR,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::MySQLErrorKind;
    use crate::loopback::MySQLWireTestServer;

    fn make_test_core(dsn: &str, stmt_cache_size: usize) -> Arc<MySQLClientCore> {
        let config = MySQLConnectConfig::from_dsn_and_kwargs(
            dsn,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("disable"),
            None,
            None,
            None,
            5_000,
            5_000,
            5_000,
            1,
            1,
            stmt_cache_size,
            "utf8mb4",
            true,
            None,
            None,
            "PUBLIC",
            false,
            None,
            None,
            None,
        )
        .expect("config");
        MySQLClientCore::new(config)
    }

    #[test]
    fn test_loopback_prepared_statement_lru_eviction_and_codecs() {
        let server = MySQLWireTestServer::start_default().expect("server");
        let core = make_test_core(&server.dsn(), 2);

        block_on_db(async {
            core.execute_result_set(
                "CREATE TABLE t_codecs (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    dec_val DECIMAL(18, 4),
                    dt_val DATETIME(6),
                    d_val DATE,
                    t_val TIME(6),
                    j_val JSON,
                    b_val VARBINARY(64)
                )"
                .to_string(),
                Params::Empty,
            )
            .await
            .expect("create table");

            let ins = core
                .execute_result_set(
                    "INSERT INTO t_codecs (dec_val, dt_val, d_val, t_val, j_val, b_val) VALUES (?, ?, ?, ?, ?, ?)".to_string(),
                    Params::Positional(vec![
                        Value::Bytes(b"12345.6789".to_vec()),
                        Value::Date(2026, 11, 4, 20, 30, 15, 654_321),
                        Value::Date(2026, 11, 4, 0, 0, 0, 0),
                        Value::Time(false, 0, 14, 22, 33, 125_000),
                        Value::Bytes(b"{\"k\":42}".to_vec()),
                        Value::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]),
                    ]),
                )
                .await
                .expect("insert");
            assert_eq!(ins.affected_rows, 1);
            assert_eq!(core.lastrowid(), Some(1));

            let sel = core
                .execute_result_set(
                    "SELECT dec_val, dt_val, d_val, t_val, j_val, b_val FROM t_codecs WHERE id = ?"
                        .to_string(),
                    Params::Positional(vec![Value::Int(1)]),
                )
                .await
                .expect("select");
            assert_eq!(sel.rows.len(), 1);
            let r = &sel.rows[0];
            assert!(matches!(&r[0], DecodedCell::Decimal(s) if s == "12345.6789"));
            assert!(matches!(
                r[1],
                DecodedCell::DateTime(2026, 11, 4, 20, 30, 15, 654_321)
            ));
            assert!(matches!(r[2], DecodedCell::Date(2026, 11, 4)));
            assert!(matches!(r[3], DecodedCell::Time(false, 0, 14, 22, 33, 125_000)));
            assert!(matches!(&r[4], DecodedCell::Json(s) if s == "{\"k\":42}"));
            assert!(matches!(&r[5], DecodedCell::Bytes(b) if b == &[0xDE, 0xAD, 0xBE, 0xEF]));

            // Exceed statement_cache_size=2 to trigger COM_STMT_CLOSE on eviction
            let _ = core
                .execute_result_set(
                    "SELECT id FROM t_codecs WHERE id = ?".to_string(),
                    Params::Positional(vec![Value::Int(1)]),
                )
                .await
                .expect("q2");
            let _ = core
                .execute_result_set(
                    "SELECT dec_val FROM t_codecs WHERE id = ?".to_string(),
                    Params::Positional(vec![Value::Int(1)]),
                )
                .await
                .expect("q3");
            assert_eq!(core.statement_cache_len(), 2);
            assert!(server.prepare_count() >= 3);
            assert!(server.close_stmt_count() >= 1);

            core.close().await.expect("close");
        });
    }

    #[test]
    fn test_loopback_transactions_savepoints_errors_and_kill_query() {
        let server = MySQLWireTestServer::start_default().expect("server");
        let core = make_test_core(&server.dsn(), 16);

        block_on_db(async {
            core.execute_result_set(
                "CREATE TABLE tx_items (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL)".to_string(),
                Params::Empty,
            )
            .await
            .expect("create");

            core.begin(Some("REPEATABLE READ".to_string()), false)
                .await
                .expect("begin");
            core.execute_result_set(
                "INSERT INTO tx_items (id, name) VALUES (?, ?)".to_string(),
                Params::Positional(vec![Value::Int(1), Value::Bytes(b"first".to_vec())]),
            )
            .await
            .expect("insert 1");

            // Duplicate key -> 1062 IntegrityError
            let dup_err = core
                .execute_result_set(
                    "INSERT INTO tx_items (id, name) VALUES (?, ?)".to_string(),
                    Params::Positional(vec![Value::Int(1), Value::Bytes(b"dup".to_vec())]),
                )
                .await
                .expect_err("duplicate key should fail");
            assert_eq!(dup_err.kind, MySQLErrorKind::IntegrityError);

            core.savepoint("sp1".to_string()).await.expect("sp1");
            core.execute_result_set(
                "INSERT INTO tx_items (id, name) VALUES (?, ?)".to_string(),
                Params::Positional(vec![Value::Int(2), Value::Bytes(b"second".to_vec())]),
            )
            .await
            .expect("insert 2");
            core.rollback_to_savepoint("sp1".to_string())
                .await
                .expect("rollback sp1");
            core.release_savepoint("sp1".to_string())
                .await
                .expect("release sp1");
            core.commit().await.expect("commit");

            let rows = core
                .execute_result_set("SELECT id FROM tx_items".to_string(), Params::Empty)
                .await
                .expect("select");
            assert_eq!(rows.rows.len(), 1);

            // Multi-result set execution
            let sets = core
                .execute_all_sets("SELECT 1 AS a; SELECT 2 AS b".to_string(), Params::Empty)
                .await
                .expect("multi sets");
            assert_eq!(sets.len(), 2);

            // Out-of-band KILL QUERY cancellation
            let core_cancel = Arc::clone(&core);
            let cancel_handle = tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(80)).await;
                core_cancel.cancel_active_query().await
            });
            let sleep_err = core
                .execute_result_set("SELECT SLEEP(5)".to_string(), Params::Empty)
                .await
                .expect_err("sleep should be interrupted by KILL QUERY");
            assert_eq!(sleep_err.kind, MySQLErrorKind::QueryCancelled);
            let cancelled = cancel_handle.await.expect("join").expect("cancel");
            assert!(cancelled);
            assert!(server.kill_query_count() >= 1);

            core.close().await.expect("close");
        });
    }

    #[test]
    fn test_loopback_arrow_shm_select_and_bulk_load_roundtrip() {
        let server = MySQLWireTestServer::start_default().expect("server");
        let core = make_test_core(&server.dsn(), 16);

        block_on_db(async {
            core.execute_result_set(
                "CREATE TABLE t_arrow (id INT PRIMARY KEY, label VARCHAR(64), amount DECIMAL(12, 2))"
                    .to_string(),
                Params::Empty,
            )
            .await
            .expect("create");

            let inserted = core
                .load_rows_batched(
                    "t_arrow".to_string(),
                    vec![
                        "id".to_string(),
                        "label".to_string(),
                        "amount".to_string(),
                    ],
                    vec![
                        vec![
                            Value::Int(1),
                            Value::Bytes(b"one".to_vec()),
                            Value::Bytes(b"10.25".to_vec()),
                        ],
                        vec![
                            Value::Int(2),
                            Value::Bytes(b"two".to_vec()),
                            Value::Bytes(b"20.50".to_vec()),
                        ],
                        vec![
                            Value::Int(3),
                            Value::Bytes(b"three".to_vec()),
                            Value::Bytes(b"30.75".to_vec()),
                        ],
                    ],
                    2,
                )
                .await
                .expect("batched load");
            assert_eq!(inserted, 3);

            let rs = core
                .execute_result_set(
                    "SELECT id, label, amount FROM t_arrow ORDER BY id".to_string(),
                    Params::Empty,
                )
                .await
                .expect("select");
            assert_eq!(rs.rows.len(), 3);

            let (shm_name, byte_len, row_count, region) =
                encode_result_set_to_shm(&rs.columns, &rs.rows, 2).expect("shm encode");
            assert!(shm_name.starts_with("/vtb-my-"));
            assert!(byte_len > 0);
            assert_eq!(row_count, 3);

            let batches =
                encode_result_set_to_shm_batches(&rs.columns, &rs.rows, 2).expect("shm batches");
            assert_eq!(batches.len(), 2);
            assert_eq!(batches[0].2, 2);
            assert_eq!(batches[1].2, 1);

            // SAFETY: `region` is valid for `byte_len + ShmArrowFrameHeader::SIZE`.
            let raw_slice = unsafe { region.as_slice() };
            let (cols, decoded_rows) = decode_arrow_ipc_to_mysql_rows(
                &raw_slice[..vertebra_db::arrow_shm::ShmArrowFrameHeader::SIZE + byte_len],
                None,
            )
            .expect("decode");
            assert_eq!(cols, vec!["id", "label", "amount"]);
            assert_eq!(decoded_rows.len(), 3);

            core.close().await.expect("close");
        });
    }
}
