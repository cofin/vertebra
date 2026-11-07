//! BigQuery GoogleSQL parameter encoding, REST v2 response decoding, and SQL classification helpers.

use std::collections::HashMap;

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD as BASE64_STD, STANDARD_NO_PAD as BASE64_NO_PAD};
use serde_json::{Map, Value, json};

use crate::client::BigQueryError;

/// Typed GoogleSQL query parameter value for BigQuery REST v2.
#[derive(Debug, Clone, PartialEq)]
pub enum BigQueryParamValue {
    Null(String),
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Numeric(String),
    BigNumeric(String),
    String(String),
    Bytes(Vec<u8>),
    Date(String),
    Time(String),
    DateTime(String),
    TimestampMicros(i64),
    Json(String),
    Geography(String),
    Interval(String),
    Range {
        elem_type: String,
        start: Option<String>,
        end: Option<String>,
    },
    Array {
        elem_type: String,
        values: Vec<BigQueryParamValue>,
    },
    Struct {
        fields: Vec<(String, BigQueryParamValue)>,
    },
}

impl BigQueryParamValue {
    /// Return the scalar GoogleSQL type name for this value.
    pub fn sql_type_name(&self) -> &str {
        match self {
            Self::Null(t) => t.as_str(),
            Self::Bool(_) => "BOOL",
            Self::Int64(_) => "INT64",
            Self::Float64(_) => "FLOAT64",
            Self::Numeric(_) => "NUMERIC",
            Self::BigNumeric(_) => "BIGNUMERIC",
            Self::String(_) => "STRING",
            Self::Bytes(_) => "BYTES",
            Self::Date(_) => "DATE",
            Self::Time(_) => "TIME",
            Self::DateTime(_) => "DATETIME",
            Self::TimestampMicros(_) => "TIMESTAMP",
            Self::Json(_) => "JSON",
            Self::Geography(_) => "GEOGRAPHY",
            Self::Interval(_) => "INTERVAL",
            Self::Range { .. } => "RANGE",
            Self::Array { .. } => "ARRAY",
            Self::Struct { .. } => "STRUCT",
        }
    }

    /// Encode the `QueryParameterType` JSON structure for BigQuery REST v2.
    pub fn to_parameter_type_json(&self) -> Value {
        match self {
            Self::Range { elem_type, .. } => json!({
                "type": "RANGE",
                "rangeElementType": { "type": elem_type }
            }),
            Self::Array { elem_type, values } => {
                let array_type = if let Some(first @ Self::Struct { .. }) = values.first() {
                    first.to_parameter_type_json()
                } else {
                    json!({ "type": elem_type })
                };
                json!({
                    "type": "ARRAY",
                    "arrayType": array_type
                })
            }
            Self::Struct { fields } => {
                let struct_types: Vec<Value> = fields
                    .iter()
                    .map(|(name, val)| {
                        json!({
                            "name": name,
                            "type": val.to_parameter_type_json()
                        })
                    })
                    .collect();
                json!({
                    "type": "STRUCT",
                    "structTypes": struct_types
                })
            }
            other => json!({ "type": other.sql_type_name() }),
        }
    }

    /// Encode the `QueryParameterValue` JSON structure for BigQuery REST v2.
    ///
    /// Typed `NULL` values emit `{}` (omitting `"value"`) as required by BigQuery REST v2.
    pub fn to_parameter_value_json(&self) -> Value {
        match self {
            Self::Null(_) => json!({}),
            Self::Bool(b) => json!({ "value": if *b { "true" } else { "false" } }),
            Self::Int64(n) => json!({ "value": n.to_string() }),
            Self::Float64(f) => json!({ "value": f.to_string() }),
            Self::Numeric(s)
            | Self::BigNumeric(s)
            | Self::String(s)
            | Self::Date(s)
            | Self::Time(s)
            | Self::DateTime(s)
            | Self::Json(s)
            | Self::Geography(s)
            | Self::Interval(s) => json!({ "value": s }),
            Self::Bytes(b) => json!({ "value": BASE64_STD.encode(b) }),
            Self::TimestampMicros(micros) => {
                let formatted = chrono::DateTime::from_timestamp_micros(*micros)
                    .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string())
                    .unwrap_or_else(|| micros.to_string());
                json!({ "value": formatted })
            }
            Self::Range { start, end, .. } => {
                let start_str = start.as_deref().unwrap_or("UNBOUNDED");
                let end_str = end.as_deref().unwrap_or("UNBOUNDED");
                json!({
                    "value": format!("[{start_str}, {end_str})"),
                    "rangeValue": {
                        "start": start.as_ref().map(|s| json!({ "value": s })).unwrap_or_else(|| json!({})),
                        "end": end.as_ref().map(|e| json!({ "value": e })).unwrap_or_else(|| json!({})),
                    }
                })
            }
            Self::Array { values, .. } => {
                let arr: Vec<Value> = values.iter().map(Self::to_parameter_value_json).collect();
                json!({ "arrayValues": arr })
            }
            Self::Struct { fields } => {
                let mut map = Map::with_capacity(fields.len());
                for (name, val) in fields {
                    map.insert(name.clone(), val.to_parameter_value_json());
                }
                Value::Object({
                    let mut outer = Map::with_capacity(1);
                    outer.insert("structValues".to_string(), Value::Object(map));
                    outer
                })
            }
        }
    }

    /// Parse a [`BigQueryParamValue`] from a JSON descriptor or raw JSON scalar/array/object.
    pub fn from_json_descriptor(val: &Value) -> Result<Self, BigQueryError> {
        if let Some(obj) = val.as_object()
            && let Some(type_str) = obj.get("type").and_then(Value::as_str)
        {
            let upper = type_str.to_ascii_uppercase();
            let raw_val = obj.get("value").unwrap_or(&Value::Null);
            if raw_val.is_null() && upper != "ARRAY" && upper != "STRUCT" && upper != "RANGE" {
                return Ok(Self::Null(upper));
            }
            return match upper.as_str() {
                "NULL" => {
                    let sql_type = obj
                        .get("sql_type")
                        .and_then(Value::as_str)
                        .unwrap_or("STRING")
                        .to_ascii_uppercase();
                    Ok(Self::Null(sql_type))
                }
                "BOOL" | "BOOLEAN" => {
                    let b = match raw_val {
                        Value::Bool(b) => *b,
                        Value::String(s) => s.eq_ignore_ascii_case("true") || s == "1",
                        Value::Number(n) => n.as_i64().unwrap_or(0) != 0,
                        _ => false,
                    };
                    Ok(Self::Bool(b))
                }
                "INT64" | "INTEGER" => {
                    let i = match raw_val {
                        Value::Number(n) => n.as_i64().ok_or_else(|| {
                            BigQueryError::InvalidArgument(format!("invalid INT64 value: {n}"))
                        })?,
                        Value::String(s) => s.parse::<i64>().map_err(|e| {
                            BigQueryError::InvalidArgument(format!("invalid INT64 string: {e}"))
                        })?,
                        other => {
                            return Err(BigQueryError::InvalidArgument(format!(
                                "invalid INT64 parameter: {other}"
                            )));
                        }
                    };
                    Ok(Self::Int64(i))
                }
                "FLOAT64" | "FLOAT" => {
                    let f = match raw_val {
                        Value::Number(n) => n.as_f64().ok_or_else(|| {
                            BigQueryError::InvalidArgument(format!("invalid FLOAT64 value: {n}"))
                        })?,
                        Value::String(s) => s.parse::<f64>().map_err(|e| {
                            BigQueryError::InvalidArgument(format!("invalid FLOAT64 string: {e}"))
                        })?,
                        other => {
                            return Err(BigQueryError::InvalidArgument(format!(
                                "invalid FLOAT64 parameter: {other}"
                            )));
                        }
                    };
                    Ok(Self::Float64(f))
                }
                "NUMERIC" => Ok(Self::Numeric(value_to_string(raw_val))),
                "BIGNUMERIC" => Ok(Self::BigNumeric(value_to_string(raw_val))),
                "STRING" => Ok(Self::String(value_to_string(raw_val))),
                "BYTES" => {
                    let s = raw_val.as_str().unwrap_or("");
                    let decoded = BASE64_STD
                        .decode(s)
                        .or_else(|_| BASE64_NO_PAD.decode(s))
                        .unwrap_or_else(|_| s.as_bytes().to_vec());
                    Ok(Self::Bytes(decoded))
                }
                "DATE" => Ok(Self::Date(value_to_string(raw_val))),
                "TIME" => Ok(Self::Time(value_to_string(raw_val))),
                "DATETIME" => Ok(Self::DateTime(value_to_string(raw_val))),
                "TIMESTAMP" | "TIMESTAMP_MICROS" => match raw_val {
                    Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            Ok(Self::TimestampMicros(i))
                        } else if let Some(f) = n.as_f64() {
                            Ok(Self::TimestampMicros((f * 1_000_000.0).round() as i64))
                        } else {
                            Err(BigQueryError::InvalidArgument(
                                "invalid TIMESTAMP number".to_string(),
                            ))
                        }
                    }
                    Value::String(s) => {
                        if let Ok(i) = s.parse::<i64>() {
                            Ok(Self::TimestampMicros(i))
                        } else if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                            Ok(Self::TimestampMicros(dt.timestamp_micros()))
                        } else {
                            Ok(Self::DateTime(s.clone()))
                        }
                    }
                    other => Err(BigQueryError::InvalidArgument(format!(
                        "invalid TIMESTAMP parameter: {other}"
                    ))),
                },
                "JSON" => {
                    let s = match raw_val {
                        Value::String(s) => s.clone(),
                        other => serde_json::to_string(other).unwrap_or_default(),
                    };
                    Ok(Self::Json(s))
                }
                "GEOGRAPHY" => Ok(Self::Geography(value_to_string(raw_val))),
                "INTERVAL" => Ok(Self::Interval(value_to_string(raw_val))),
                "RANGE" => {
                    let elem_type = obj
                        .get("elem_type")
                        .or_else(|| obj.get("element_type"))
                        .and_then(Value::as_str)
                        .unwrap_or("DATE")
                        .to_ascii_uppercase();
                    let start = obj.get("start").and_then(|v| {
                        if v.is_null() {
                            None
                        } else {
                            Some(value_to_string(v))
                        }
                    });
                    let end = obj.get("end").and_then(|v| {
                        if v.is_null() {
                            None
                        } else {
                            Some(value_to_string(v))
                        }
                    });
                    Ok(Self::Range {
                        elem_type,
                        start,
                        end,
                    })
                }
                "ARRAY" => {
                    let elem_type = obj
                        .get("elem_type")
                        .or_else(|| obj.get("element_type"))
                        .and_then(Value::as_str)
                        .unwrap_or("STRING")
                        .to_ascii_uppercase();
                    let items = obj
                        .get("values")
                        .or_else(|| obj.get("value"))
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default();
                    let mut values = Vec::with_capacity(items.len());
                    for item in &items {
                        values.push(Self::from_json_descriptor(item)?);
                    }
                    Ok(Self::Array { elem_type, values })
                }
                "STRUCT" | "RECORD" => {
                    let mut fields = Vec::new();
                    if let Some(arr) = obj
                        .get("fields")
                        .or_else(|| obj.get("value"))
                        .and_then(Value::as_array)
                    {
                        for item in arr {
                            if let Some(fobj) = item.as_object()
                                && let Some(fname) = fobj.get("name").and_then(Value::as_str)
                            {
                                let fval_raw = fobj.get("value").unwrap_or(item);
                                fields.push((
                                    fname.to_string(),
                                    Self::from_json_descriptor(fval_raw)?,
                                ));
                            }
                        }
                    } else if let Some(map) = obj
                        .get("fields")
                        .or_else(|| obj.get("value"))
                        .and_then(Value::as_object)
                    {
                        for (k, v) in map {
                            fields.push((k.clone(), Self::from_json_descriptor(v)?));
                        }
                    }
                    Ok(Self::Struct { fields })
                }
                other => Ok(Self::String(format!("{other}:{raw_val}"))),
            };
        }

        match val {
            Value::Null => Ok(Self::Null("STRING".to_string())),
            Value::Bool(b) => Ok(Self::Bool(*b)),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(Self::Int64(i))
                } else if let Some(f) = n.as_f64() {
                    Ok(Self::Float64(f))
                } else {
                    Ok(Self::Numeric(n.to_string()))
                }
            }
            Value::String(s) => Ok(Self::String(s.clone())),
            Value::Array(arr) => {
                let mut values = Vec::with_capacity(arr.len());
                for item in arr {
                    values.push(Self::from_json_descriptor(item)?);
                }
                let elem_type = values
                    .first()
                    .map(|v| v.sql_type_name().to_string())
                    .unwrap_or_else(|| "STRING".to_string());
                Ok(Self::Array { elem_type, values })
            }
            Value::Object(map) => {
                let mut fields = Vec::with_capacity(map.len());
                for (k, v) in map {
                    fields.push((k.clone(), Self::from_json_descriptor(v)?));
                }
                Ok(Self::Struct { fields })
            }
        }
    }
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Named (`@name`) or positional (`?`) BigQuery query parameter.
#[derive(Debug, Clone, PartialEq)]
pub struct BigQueryQueryParameter {
    pub name: Option<String>,
    pub value: BigQueryParamValue,
}

impl BigQueryQueryParameter {
    /// Construct a named (`@name`) query parameter.
    pub fn named(name: impl Into<String>, value: BigQueryParamValue) -> Self {
        let raw = name.into();
        let cleaned = raw.strip_prefix('@').unwrap_or(&raw).to_string();
        Self {
            name: Some(cleaned),
            value,
        }
    }

    /// Construct a positional (`?`) query parameter.
    pub fn positional(value: BigQueryParamValue) -> Self {
        Self { name: None, value }
    }

    /// Serialize this parameter to the BigQuery REST v2 `QueryParameter` JSON representation.
    pub fn to_rest_json(&self) -> Value {
        let mut obj = Map::with_capacity(3);
        if let Some(ref name) = self.name {
            obj.insert("name".to_string(), Value::String(name.clone()));
        }
        obj.insert(
            "parameterType".to_string(),
            self.value.to_parameter_type_json(),
        );
        obj.insert(
            "parameterValue".to_string(),
            self.value.to_parameter_value_json(),
        );
        Value::Object(obj)
    }
}

/// Parse a JSON string of query parameters into `Vec<BigQueryQueryParameter>`.
pub fn parse_query_parameters_json(
    params_json: Option<&str>,
) -> Result<Vec<BigQueryQueryParameter>, BigQueryError> {
    let Some(raw) = params_json else {
        return Ok(Vec::new());
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "null" || trimmed == "[]" || trimmed == "{}" {
        return Ok(Vec::new());
    }
    let parsed: Value = serde_json::from_str(trimmed)
        .map_err(|e| BigQueryError::InvalidArgument(format!("invalid params_json: {e}")))?;

    match parsed {
        Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in &items {
                if let Some(obj) = item.as_object()
                    && (obj.contains_key("parameterType") || obj.contains_key("parameterValue"))
                {
                    let name = obj
                        .get("name")
                        .and_then(Value::as_str)
                        .map(|s| s.strip_prefix('@').unwrap_or(s).to_string());
                    let ptype = obj
                        .get("parameterType")
                        .and_then(|t| t.get("type"))
                        .and_then(Value::as_str)
                        .unwrap_or("STRING");
                    let pval = obj.get("parameterValue").unwrap_or(&Value::Null);
                    let val = if let Some(v) = pval.get("value") {
                        BigQueryParamValue::from_json_descriptor(&json!({
                            "type": ptype,
                            "value": v
                        }))?
                    } else {
                        BigQueryParamValue::Null(ptype.to_string())
                    };
                    out.push(BigQueryQueryParameter { name, value: val });
                } else if let Some(obj) = item.as_object()
                    && obj.contains_key("type")
                {
                    let name = obj
                        .get("name")
                        .and_then(Value::as_str)
                        .map(|s| s.strip_prefix('@').unwrap_or(s).to_string());
                    let val = BigQueryParamValue::from_json_descriptor(item)?;
                    out.push(BigQueryQueryParameter { name, value: val });
                } else {
                    let val = BigQueryParamValue::from_json_descriptor(item)?;
                    out.push(BigQueryQueryParameter::positional(val));
                }
            }
            Ok(out)
        }
        Value::Object(map) => {
            let mut out = Vec::with_capacity(map.len());
            for (k, v) in &map {
                let val = BigQueryParamValue::from_json_descriptor(v)?;
                out.push(BigQueryQueryParameter::named(k.clone(), val));
            }
            Ok(out)
        }
        other => Err(BigQueryError::InvalidArgument(format!(
            "params_json must be a JSON array or object, got: {other}"
        ))),
    }
}

/// Decoded cell value from a BigQuery REST v2 result row.
#[derive(Debug, Clone, PartialEq)]
pub enum BigQueryCellValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Json(String),
    Array(Vec<BigQueryCellValue>),
    Struct(Vec<(String, BigQueryCellValue)>),
}

/// Column schema descriptor returned by BigQuery REST v2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryFieldSchema {
    pub name: String,
    pub field_type: String,
    pub mode: String,
    pub fields: Vec<BigQueryFieldSchema>,
}

impl BigQueryFieldSchema {
    pub fn from_rest_json(val: &Value) -> Option<Self> {
        let obj = val.as_object()?;
        let name = obj.get("name")?.as_str()?.to_string();
        let field_type = obj
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("STRING")
            .to_ascii_uppercase();
        let mode = obj
            .get("mode")
            .and_then(Value::as_str)
            .unwrap_or("NULLABLE")
            .to_ascii_uppercase();
        let fields = obj
            .get("fields")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(Self::from_rest_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Some(Self {
            name,
            field_type,
            mode,
            fields,
        })
    }

    pub fn to_rest_json(&self) -> Value {
        let mut obj = Map::with_capacity(4);
        obj.insert("name".to_string(), Value::String(self.name.clone()));
        obj.insert("type".to_string(), Value::String(self.field_type.clone()));
        obj.insert("mode".to_string(), Value::String(self.mode.clone()));
        if !self.fields.is_empty() {
            obj.insert(
                "fields".to_string(),
                Value::Array(self.fields.iter().map(Self::to_rest_json).collect()),
            );
        }
        Value::Object(obj)
    }
}

/// Decoded result set and FinOps telemetry from a BigQuery query execution.
#[derive(Debug, Clone)]
pub struct BigQueryQueryResult {
    pub columns: Vec<String>,
    pub schema: Vec<BigQueryFieldSchema>,
    pub rows: Vec<Vec<BigQueryCellValue>>,
    pub total_rows: u64,
    pub total_bytes_processed: i64,
    pub total_bytes_billed: i64,
    pub slot_ms: i64,
    pub cache_hit: bool,
    pub job_id: Option<String>,
    pub session_id: Option<String>,
    pub num_dml_affected_rows: Option<i64>,
}

/// Result of a BigQuery `dryRun: true` cost-estimation request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryDryRunResult {
    pub total_bytes_processed: i64,
    pub total_bytes_billed: i64,
    pub cache_hit: bool,
    pub referenced_tables: Vec<String>,
    pub schema: Vec<BigQueryFieldSchema>,
}

/// Result of a Storage Write API v1 `append_arrow_stream` operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryAppendResult {
    pub appended_rows: u64,
    pub write_stream: String,
}

/// Per-row error returned inside `AppendRowsResponse.row_errors`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryRowError {
    pub index: i64,
    pub code: i32,
    pub message: String,
}

/// Per-query execution options for [`crate::client::BigQueryClient::query_with_options`].
#[derive(Debug, Clone, Default)]
pub struct BigQueryQueryOptions {
    pub session_id: Option<String>,
    pub create_session: bool,
    pub dry_run: bool,
    pub destination_table: Option<String>,
    pub write_disposition: Option<String>,
    pub maximum_bytes_billed: Option<i64>,
    pub job_timeout_ms: Option<u64>,
    pub priority: Option<String>,
    pub labels: HashMap<String, String>,
    pub max_results: Option<u32>,
}

/// Arrow compression codec for BigQuery Storage Read API v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum BigQueryArrowCompression {
    Unspecified = 0,
    Lz4Frame = 1,
    Zstd = 2,
}

impl BigQueryArrowCompression {
    /// Parse an optional compression codec name (`LZ4_FRAME`, `ZSTD`, `UNSPECIFIED`).
    pub fn from_str_opt(val: Option<&str>) -> Self {
        match val.unwrap_or("").trim().to_ascii_uppercase().as_str() {
            "LZ4_FRAME" | "LZ4" => Self::Lz4Frame,
            "ZSTD" => Self::Zstd,
            "UNSPECIFIED" | "NONE" | "" => Self::Unspecified,
            _ => Self::Lz4Frame,
        }
    }
}

/// Write stream mode for BigQuery Storage Write API v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BigQueryWriteMode {
    Default,
    Committed,
    Pending,
    Buffered,
}

impl BigQueryWriteMode {
    pub fn from_str_mode(mode: &str) -> Result<Self, BigQueryError> {
        match mode.trim().to_ascii_uppercase().as_str() {
            "_DEFAULT" | "DEFAULT" | "" => Ok(Self::Default),
            "COMMITTED" => Ok(Self::Committed),
            "PENDING" => Ok(Self::Pending),
            "BUFFERED" => Ok(Self::Buffered),
            other => Err(BigQueryError::InvalidArgument(format!(
                "unsupported BigQuery Storage Write mode `{other}` (expected _default, COMMITTED, PENDING, or BUFFERED)"
            ))),
        }
    }
}

/// Split a BigQuery table reference into `[project, dataset, table]`, `[dataset, table]`, or `[table]`,
/// preserving domain-scoped Google Cloud project IDs (`google.com:my-project`) and legacy `:` separators.
pub fn split_bigquery_table_parts(raw: &str) -> Vec<String> {
    let cleaned = raw.trim().replace('`', "");
    if cleaned.is_empty() {
        return Vec::new();
    }
    if let Some((first, rest)) = cleaned.split_once(':') {
        if first.contains('.') {
            if let Some((proj_slug, ds_and_table)) = rest.split_once(':') {
                let mut parts = vec![format!("{first}:{proj_slug}")];
                parts.extend(
                    ds_and_table
                        .split('.')
                        .filter(|s| !s.is_empty())
                        .map(str::to_string),
                );
                return parts;
            }
            let mut dot_iter = rest.split('.').filter(|s| !s.is_empty());
            if let Some(proj_slug) = dot_iter.next() {
                let mut parts = vec![format!("{first}:{proj_slug}")];
                parts.extend(dot_iter.map(str::to_string));
                return parts;
            }
            return vec![cleaned];
        }
        let mut parts = vec![first.to_string()];
        parts.extend(
            rest.split('.')
                .filter(|s| !s.is_empty())
                .map(str::to_string),
        );
        return parts;
    }
    cleaned
        .split('.')
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

pub(crate) fn rest_error_details(error: &Value) -> (Option<String>, Option<String>) {
    let canonical_status = error
        .get("status")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let reason = error
        .get("errors")
        .and_then(Value::as_array)
        .and_then(|arr| arr.first())
        .and_then(|first| first.get("reason"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    (canonical_status, reason)
}

pub(crate) fn check_rest_error(resp: &Value) -> Result<(), BigQueryError> {
    if let Some(err) = resp.get("error") {
        let code = err.get("code").and_then(Value::as_u64).unwrap_or(500) as u16;
        let msg = err
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("unknown BigQuery error")
            .to_string();
        let (canonical_status, reason) = rest_error_details(err);
        return Err(BigQueryError::Api {
            status: code,
            message: msg,
            reason,
            canonical_status,
        });
    }
    if let Some(status) = resp.get("status")
        && let Some(err_res) = status.get("errorResult")
    {
        let msg = err_res
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("BigQuery job failed")
            .to_string();
        let reason = err_res
            .get("reason")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        return Err(BigQueryError::Api {
            status: 400,
            message: msg,
            reason,
            canonical_status: None,
        });
    }
    Ok(())
}

pub(crate) fn parse_json_i64(val: &Value) -> Option<i64> {
    match val {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

pub(crate) fn parse_json_u64(val: &Value) -> Option<u64> {
    match val {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.parse::<u64>().ok(),
        _ => None,
    }
}

pub(crate) fn decode_single_query_response(
    resp: &Value,
) -> Result<BigQueryQueryResult, BigQueryError> {
    let schema: Vec<BigQueryFieldSchema> = resp
        .get("schema")
        .and_then(|s| s.get("fields"))
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(BigQueryFieldSchema::from_rest_json)
                .collect()
        })
        .unwrap_or_default();

    let columns: Vec<String> = schema.iter().map(|f| f.name.clone()).collect();
    let mut rows = Vec::new();
    if let Some(raw_rows) = resp.get("rows").and_then(Value::as_array) {
        rows.reserve(raw_rows.len());
        for raw_row in raw_rows {
            rows.push(decode_table_row(raw_row, &schema));
        }
    }

    let stats = resp.get("statistics");
    let query_stats = stats.and_then(|s| s.get("query"));

    let total_rows = resp
        .get("totalRows")
        .and_then(parse_json_u64)
        .unwrap_or(rows.len() as u64);
    let total_bytes_processed = resp
        .get("totalBytesProcessed")
        .or_else(|| query_stats.and_then(|q| q.get("totalBytesProcessed")))
        .or_else(|| stats.and_then(|s| s.get("totalBytesProcessed")))
        .and_then(parse_json_i64)
        .unwrap_or(0);
    let total_bytes_billed = resp
        .get("totalBytesBilled")
        .or_else(|| query_stats.and_then(|q| q.get("totalBytesBilled")))
        .and_then(parse_json_i64)
        .unwrap_or(total_bytes_processed);
    let slot_ms = resp
        .get("totalSlotMs")
        .or_else(|| query_stats.and_then(|q| q.get("totalSlotMs")))
        .or_else(|| stats.and_then(|s| s.get("totalSlotMs")))
        .and_then(parse_json_i64)
        .unwrap_or(0);
    let cache_hit = resp
        .get("cacheHit")
        .or_else(|| query_stats.and_then(|q| q.get("cacheHit")))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let job_id = resp
        .get("jobReference")
        .and_then(|jr| jr.get("jobId"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let session_id = resp
        .get("sessionInfo")
        .or_else(|| stats.and_then(|s| s.get("sessionInfo")))
        .and_then(|si| si.get("sessionId"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let num_dml_affected_rows = resp
        .get("numDmlAffectedRows")
        .or_else(|| query_stats.and_then(|q| q.get("numDmlAffectedRows")))
        .and_then(parse_json_i64);

    Ok(BigQueryQueryResult {
        columns,
        schema,
        rows,
        total_rows,
        total_bytes_processed,
        total_bytes_billed,
        slot_ms,
        cache_hit,
        job_id,
        session_id,
        num_dml_affected_rows,
    })
}

pub(crate) fn decode_table_row(
    row_val: &Value,
    schema: &[BigQueryFieldSchema],
) -> Vec<BigQueryCellValue> {
    let Some(cells) = row_val.get("f").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut decoded = Vec::with_capacity(cells.len());
    for (idx, cell) in cells.iter().enumerate() {
        let v = cell.get("v").unwrap_or(&Value::Null);
        if let Some(field_schema) = schema.get(idx) {
            decoded.push(decode_cell_value(v, field_schema));
        } else {
            decoded.push(decode_untyped_cell(v));
        }
    }
    decoded
}

pub(crate) fn decode_cell_value(v: &Value, field: &BigQueryFieldSchema) -> BigQueryCellValue {
    if field.mode.eq_ignore_ascii_case("REPEATED") {
        let Some(arr) = v.as_array() else {
            return BigQueryCellValue::Array(Vec::new());
        };
        let mut scalar_field = field.clone();
        scalar_field.mode = "NULLABLE".to_string();
        let mut items = Vec::with_capacity(arr.len());
        for item in arr {
            let inner = item.get("v").unwrap_or(item);
            items.push(decode_cell_value(inner, &scalar_field));
        }
        return BigQueryCellValue::Array(items);
    }

    if v.is_null() {
        return BigQueryCellValue::Null;
    }

    match field.field_type.as_str() {
        "RECORD" | "STRUCT" => {
            let mut fields_out = Vec::with_capacity(field.fields.len());
            if let Some(sub_cells) = v.get("f").and_then(Value::as_array) {
                for (idx, sub_field) in field.fields.iter().enumerate() {
                    let sub_v = sub_cells
                        .get(idx)
                        .and_then(|c| c.get("v"))
                        .unwrap_or(&Value::Null);
                    fields_out.push((sub_field.name.clone(), decode_cell_value(sub_v, sub_field)));
                }
            } else if let Some(obj) = v.as_object() {
                for sub_field in &field.fields {
                    let sub_v = obj.get(&sub_field.name).unwrap_or(&Value::Null);
                    fields_out.push((sub_field.name.clone(), decode_cell_value(sub_v, sub_field)));
                }
            }
            BigQueryCellValue::Struct(fields_out)
        }
        "BOOL" | "BOOLEAN" => match v {
            Value::Bool(b) => BigQueryCellValue::Bool(*b),
            Value::String(s) => BigQueryCellValue::Bool(s.eq_ignore_ascii_case("true") || s == "1"),
            Value::Number(n) => BigQueryCellValue::Bool(n.as_i64().unwrap_or(0) != 0),
            _ => BigQueryCellValue::Bool(false),
        },
        "INT64" | "INTEGER" => match v {
            Value::Number(n) => BigQueryCellValue::Int64(n.as_i64().unwrap_or(0)),
            Value::String(s) => s
                .parse::<i64>()
                .map(BigQueryCellValue::Int64)
                .unwrap_or_else(|_| BigQueryCellValue::String(s.clone())),
            other => BigQueryCellValue::String(other.to_string()),
        },
        "FLOAT64" | "FLOAT" => match v {
            Value::Number(n) => BigQueryCellValue::Float64(n.as_f64().unwrap_or(0.0)),
            Value::String(s) => s
                .parse::<f64>()
                .map(BigQueryCellValue::Float64)
                .unwrap_or_else(|_| BigQueryCellValue::String(s.clone())),
            other => BigQueryCellValue::String(other.to_string()),
        },
        "BYTES" => {
            let raw = v.as_str().unwrap_or("");
            let bytes = BASE64_STD
                .decode(raw)
                .or_else(|_| BASE64_NO_PAD.decode(raw))
                .unwrap_or_else(|_| raw.as_bytes().to_vec());
            BigQueryCellValue::Bytes(bytes)
        }
        "TIMESTAMP" => match v {
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    BigQueryCellValue::Int64(i)
                } else if let Some(f) = n.as_f64() {
                    BigQueryCellValue::Int64((f * 1_000_000.0).round() as i64)
                } else {
                    BigQueryCellValue::String(n.to_string())
                }
            }
            Value::String(s) => {
                if let Ok(i) = s.parse::<i64>() {
                    BigQueryCellValue::Int64(i)
                } else if let Ok(f) = s.parse::<f64>() {
                    BigQueryCellValue::Int64((f * 1_000_000.0).round() as i64)
                } else {
                    BigQueryCellValue::String(s.clone())
                }
            }
            other => BigQueryCellValue::String(other.to_string()),
        },
        "JSON" => match v {
            Value::String(s) => BigQueryCellValue::Json(s.clone()),
            other => BigQueryCellValue::Json(serde_json::to_string(other).unwrap_or_default()),
        },
        _ => BigQueryCellValue::String(value_to_string(v)),
    }
}

pub(crate) fn decode_untyped_cell(v: &Value) -> BigQueryCellValue {
    match v {
        Value::Null => BigQueryCellValue::Null,
        Value::Bool(b) => BigQueryCellValue::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                BigQueryCellValue::Int64(i)
            } else {
                BigQueryCellValue::Float64(n.as_f64().unwrap_or(0.0))
            }
        }
        Value::String(s) => BigQueryCellValue::String(s.clone()),
        Value::Array(arr) => {
            BigQueryCellValue::Array(arr.iter().map(decode_untyped_cell).collect())
        }
        Value::Object(map) => BigQueryCellValue::Struct(
            map.iter()
                .map(|(k, val)| (k.clone(), decode_untyped_cell(val)))
                .collect(),
        ),
    }
}

/// Return `true` when `sql` requires promotion from `jobs.query` to `jobs.insert`.
pub fn requires_jobs_insert_sql(sql: &str) -> bool {
    let trimmed = strip_leading_sql_comments(sql).trim();
    let upper = trimmed.to_ascii_uppercase();
    if upper.starts_with("EXPORT DATA")
        || upper.starts_with("LOAD DATA")
        || upper.starts_with("DECLARE ")
        || upper.starts_with("CREATE TEMP ")
        || upper.starts_with("CREATE TEMPORARY ")
    {
        return true;
    }
    if upper.starts_with("BEGIN")
        && !upper.starts_with("BEGIN TRANSACTION")
        && upper != "BEGIN"
        && upper != "BEGIN;"
        && upper.contains(';')
        && (upper.ends_with("END") || upper.ends_with("END;"))
    {
        return true;
    }
    false
}

pub(crate) fn strip_leading_sql_comments(mut s: &str) -> &str {
    loop {
        s = s.trim_start();
        if s.starts_with("--") {
            if let Some(nl) = s.find('\n') {
                s = &s[nl + 1..];
            } else {
                return "";
            }
        } else if s.starts_with("/*") {
            if let Some(end) = s.find("*/") {
                s = &s[end + 2..];
            } else {
                return "";
            }
        } else {
            return s;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_bigquery_table_parts_handles_domain_scoped_projects() {
        assert_eq!(
            split_bigquery_table_parts("`google.com:analytics-prod.events.clicks`"),
            vec!["google.com:analytics-prod", "events", "clicks"]
        );
        assert_eq!(
            split_bigquery_table_parts("my-proj:ds.tbl"),
            vec!["my-proj", "ds", "tbl"]
        );
    }

    #[test]
    fn requires_jobs_insert_sql_ignores_leading_comments() {
        assert!(requires_jobs_insert_sql(
            "/* trace */ -- line\nEXPORT DATA OPTIONS(uri='gs://b/p*') AS SELECT 1"
        ));
        assert!(!requires_jobs_insert_sql("SELECT 1"));
        assert!(!requires_jobs_insert_sql("BEGIN TRANSACTION"));
    }
}
