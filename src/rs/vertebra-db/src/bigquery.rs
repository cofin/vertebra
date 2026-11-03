//! Pure-Rust Google Cloud BigQuery REST v2 (`jobs.query` + `jobs.insert`) and Storage Read/Write API v1
//! (`BigQueryRead` -> `VTBARROW` `ShmRegion` & `BigQueryWrite.AppendRows`) client engine.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD as BASE64_STD, STANDARD_NO_PAD as BASE64_NO_PAD};
use bytes::{BufMut, Bytes, BytesMut};
use google_cloud_bigquery::client::Read;
use google_cloud_bigquery::model as bq_model;
use serde_json::{Map, Value, json};
use thiserror::Error;
use tokio::sync::mpsc;
use vertebra_core::gcp_auth::GcpTokenCache;
use vertebra_core::shm::ShmRegion;

use crate::arrow_shm::{ARROW_IPC_EOS_MARKER, ShmArrowStreamWriter};
use crate::gcp::{install_rustls_ring_provider, official_credentials};
use crate::grpc::{
    GrpcChannelPool, GrpcCode, GrpcPoolConfig, GrpcRequestMetadata, GrpcStatus, ProtoError,
    ProtoReader, ProtoWriter, WireType, generate_request_uuid_v4,
};

/// Default BigQuery REST v2 endpoint.
pub const DEFAULT_BIGQUERY_REST_ENDPOINT: &str = "https://bigquery.googleapis.com";

/// Default BigQuery Storage API v1 gRPC endpoint.
pub const DEFAULT_BIGQUERY_STORAGE_ENDPOINT: &str = "https://bigquerystorage.googleapis.com:443";

/// Maximum Arrow payload size per `AppendRowsRequest` message (`9 MiB` safe bound under `10 MiB` limit).
pub const MAX_APPEND_ROWS_CHUNK_BYTES: usize = 9 * 1024 * 1024;

static SHM_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_bigquery_shm_name() -> String {
    let seq = SHM_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    format!("/vtb-bq-{pid:x}-{seq:x}")
}

/// Errors produced by [`BigQueryClient`].
#[derive(Debug, Error)]
pub enum BigQueryError {
    #[error("BigQuery authentication error: {0}")]
    Auth(String),
    #[error("BigQuery HTTP transport error: {0}")]
    Http(String),
    #[error("BigQuery REST API error (HTTP {status}): {message}")]
    Api { status: u16, message: String },
    #[error("BigQuery client error: {0}")]
    Client(#[from] google_cloud_bigquery::Error),
    #[error("BigQuery gRPC error: {0}")]
    Grpc(#[from] GrpcStatus),
    #[error("BigQuery protobuf wire error: {0}")]
    Proto(#[from] ProtoError),
    #[error("BigQuery shared-memory Arrow error: {0}")]
    Shm(String),
    #[error("BigQuery Storage Write row errors: {0:?}")]
    RowErrors(Vec<BigQueryRowError>),
    #[error("BigQuery invalid argument: {0}")]
    InvalidArgument(String),
}

/// Configuration for [`BigQueryClient`].
#[derive(Debug, Clone)]
pub struct BigQueryClientConfig {
    pub project_id: String,
    pub dataset_id: Option<String>,
    pub location: Option<String>,
    pub rest_endpoint: String,
    pub storage_endpoint: String,
    pub maximum_bytes_billed: Option<i64>,
    pub job_timeout_ms: Option<u64>,
    pub priority: Option<String>,
    pub labels: HashMap<String, String>,
    pub auth_options: HashMap<String, String>,
    pub pool_size: usize,
    /// PEM roots added to the REST (`jobs.query`/`jobs.insert`) client. The Storage Read/Write gRPC clients use the system trust store plus bundled roots and ignore this value.
    pub ca_pem: Option<Vec<u8>>,
}

impl BigQueryClientConfig {
    /// Create a new configuration for `project_id` with production defaults.
    pub fn new(project_id: impl Into<String>) -> Self {
        Self {
            project_id: project_id.into(),
            dataset_id: None,
            location: None,
            rest_endpoint: DEFAULT_BIGQUERY_REST_ENDPOINT.to_string(),
            storage_endpoint: DEFAULT_BIGQUERY_STORAGE_ENDPOINT.to_string(),
            maximum_bytes_billed: None,
            job_timeout_ms: None,
            priority: None,
            labels: HashMap::new(),
            auth_options: HashMap::new(),
            pool_size: 4,
            ca_pem: None,
        }
    }

    /// Resolve the effective REST base URL.
    pub fn effective_rest_endpoint(&self) -> String {
        let trimmed = self.rest_endpoint.trim().trim_end_matches('/');
        if trimmed.is_empty() {
            return DEFAULT_BIGQUERY_REST_ENDPOINT.to_string();
        }
        if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
            return trimmed.to_string();
        }
        format!("http://{trimmed}")
    }

    /// Resolve the effective Storage gRPC endpoint URL.
    pub fn effective_storage_endpoint(&self) -> String {
        let trimmed = self.storage_endpoint.trim().trim_end_matches('/');
        if trimmed.is_empty() {
            return DEFAULT_BIGQUERY_STORAGE_ENDPOINT.to_string();
        }
        if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
            return trimmed.to_string();
        }
        format!("http://{trimmed}")
    }
}

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

/// Per-query execution options for [`BigQueryClient::query_with_options`].
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

/// Pure-Rust Google Cloud BigQuery client over REST v2 and Storage Read/Write API v1.
pub struct BigQueryClient {
    config: BigQueryClientConfig,
    http_client: reqwest::Client,
    token_cache: Option<Arc<GcpTokenCache>>,
    injected_read: Option<Read>,
    read_client: tokio::sync::OnceCell<Read>,
    grpc_pool: tokio::sync::OnceCell<Arc<GrpcChannelPool>>,
}

impl BigQueryClient {
    /// Construct a new [`BigQueryClient`] from `config`.
    pub fn new(config: BigQueryClientConfig) -> Result<Self, BigQueryError> {
        let is_cleartext = config.effective_rest_endpoint().starts_with("http://")
            || config.effective_storage_endpoint().starts_with("http://");

        let mut builder = reqwest::Client::builder()
            .pool_max_idle_per_host(config.pool_size.max(1))
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(120));

        if is_cleartext {
            builder = builder.no_proxy();
        }

        if let Some(ref pem) = config.ca_pem
            && let Ok(cert) = reqwest::Certificate::from_pem(pem)
        {
            builder = builder.add_root_certificate(cert);
        }

        let http_client = builder
            .build()
            .map_err(|e| BigQueryError::Http(e.to_string()))?;

        let mut auth_opts = config.auth_options.clone();
        if !auth_opts.contains_key("scopes") {
            auth_opts.insert(
                "scopes".to_string(),
                "https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/cloud-platform".to_string(),
            );
        }
        if is_cleartext {
            auth_opts
                .entry("allow_ambient_adc".to_string())
                .or_insert_with(|| "false".to_string());
        }
        let token_cache = GcpTokenCache::from_options(&auth_opts);

        Ok(Self {
            config,
            http_client,
            token_cache,
            injected_read: None,
            read_client: tokio::sync::OnceCell::new(),
            grpc_pool: tokio::sync::OnceCell::new(),
        })
    }

    /// Construct a [`BigQueryClient`] with an injected Storage [`Read`] client (for stub testing).
    pub fn with_read_client(
        config: BigQueryClientConfig,
        read: Read,
    ) -> Result<Self, BigQueryError> {
        let mut client = Self::new(config)?;
        client.injected_read = Some(read);
        Ok(client)
    }

    async fn storage_read(&self) -> Result<&Read, BigQueryError> {
        if let Some(ref read) = self.injected_read {
            return Ok(read);
        }
        self.read_client
            .get_or_try_init(|| async {
                install_rustls_ring_provider();
                Read::builder()
                    .with_endpoint(self.config.effective_storage_endpoint())
                    .with_credentials(official_credentials(self.token_cache.clone()))
                    .build()
                    .await
                    .map_err(|e| {
                        BigQueryError::Http(format!("failed to build BigQuery Read client: {e}"))
                    })
            })
            .await
    }

    /// Return a reference to the client configuration.
    pub fn config(&self) -> &BigQueryClientConfig {
        &self.config
    }

    /// Execute a GoogleSQL statement or query over BigQuery REST v2 (`jobs.query` fast-path with
    /// automatic promotion to `jobs.insert`, poll continuation, and `pageToken` drain).
    pub async fn query(
        &self,
        sql: &str,
        params: &[BigQueryQueryParameter],
        session_id: Option<&str>,
    ) -> Result<BigQueryQueryResult, BigQueryError> {
        let opts = BigQueryQueryOptions {
            session_id: session_id.map(str::to_string),
            ..Default::default()
        };
        self.query_with_options(sql, params, &opts).await
    }

    /// Execute a GoogleSQL query with explicit [`BigQueryQueryOptions`].
    pub async fn query_with_options(
        &self,
        sql: &str,
        params: &[BigQueryQueryParameter],
        options: &BigQueryQueryOptions,
    ) -> Result<BigQueryQueryResult, BigQueryError> {
        if options.dry_run
            || options.destination_table.is_some()
            || options.priority.as_deref() == Some("BATCH")
            || requires_jobs_insert_sql(sql)
        {
            return self.execute_jobs_insert_query(sql, params, options).await;
        }
        self.execute_fastpath_query(sql, params, options).await
    }

    /// Execute a `dryRun: true` query via `jobs.insert` to estimate bytes processed and extract output schema.
    pub async fn dry_run(
        &self,
        sql: &str,
        params: &[BigQueryQueryParameter],
    ) -> Result<BigQueryDryRunResult, BigQueryError> {
        let opts = BigQueryQueryOptions {
            dry_run: true,
            ..Default::default()
        };
        let payload = self.build_jobs_insert_payload(sql, params, &opts);
        let url = format!(
            "{}/bigquery/v2/projects/{}/jobs",
            self.config.effective_rest_endpoint(),
            self.config.project_id
        );
        let resp = self
            .send_rest_json(reqwest::Method::POST, &url, Some(&payload))
            .await?;
        check_rest_error(&resp)?;

        let stats = resp.get("statistics");
        let query_stats = stats.and_then(|s| s.get("query"));
        let total_bytes_processed = query_stats
            .and_then(|q| q.get("totalBytesProcessed"))
            .or_else(|| stats.and_then(|s| s.get("totalBytesProcessed")))
            .and_then(parse_json_i64)
            .unwrap_or(0);
        let total_bytes_billed = query_stats
            .and_then(|q| q.get("totalBytesBilled"))
            .or_else(|| stats.and_then(|s| s.get("totalBytesBilled")))
            .and_then(parse_json_i64)
            .unwrap_or(total_bytes_processed);
        let cache_hit = query_stats
            .and_then(|q| q.get("cacheHit"))
            .and_then(Value::as_bool)
            .unwrap_or(false);

        let referenced_tables = query_stats
            .and_then(|q| q.get("referencedTables"))
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        let p = t.get("projectId")?.as_str()?;
                        let d = t.get("datasetId")?.as_str()?;
                        let tbl = t.get("tableId")?.as_str()?;
                        Some(format!("{p}.{d}.{tbl}"))
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let schema = query_stats
            .and_then(|q| q.get("schema"))
            .or_else(|| resp.get("schema"))
            .and_then(|s| s.get("fields"))
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(BigQueryFieldSchema::from_rest_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        Ok(BigQueryDryRunResult {
            total_bytes_processed,
            total_bytes_billed,
            cache_hit,
            referenced_tables,
            schema,
        })
    }

    /// Read a BigQuery table via Storage Read API v1 (`CreateReadSession` + parallel `ReadRows` streams)
    /// directly into a zero-copy `VTBARROW` shared-memory `ShmRegion`.
    pub async fn read_table_to_shm_arrow(
        &self,
        project_id: &str,
        dataset_id: &str,
        table_id: &str,
        selected_fields: &[String],
        row_restriction: Option<&str>,
        max_streams: i32,
    ) -> Result<(String, usize, usize, Arc<ShmRegion>), BigQueryError> {
        self.read_table_to_shm_arrow_with_compression(
            project_id,
            dataset_id,
            table_id,
            selected_fields,
            row_restriction,
            max_streams,
            BigQueryArrowCompression::Lz4Frame,
        )
        .await
    }

    /// Read a BigQuery table via Storage Read API v1 with an explicit Arrow compression codec.
    #[allow(clippy::too_many_arguments)]
    pub async fn read_table_to_shm_arrow_with_compression(
        &self,
        project_id: &str,
        dataset_id: &str,
        table_id: &str,
        selected_fields: &[String],
        row_restriction: Option<&str>,
        max_streams: i32,
        compression: BigQueryArrowCompression,
    ) -> Result<(String, usize, usize, Arc<ShmRegion>), BigQueryError> {
        let read = self.storage_read().await?;
        let eff_project = if project_id.trim().is_empty() {
            &self.config.project_id
        } else {
            project_id.trim()
        };
        let table_resource =
            format!("projects/{eff_project}/datasets/{dataset_id}/tables/{table_id}");
        let parent = format!("projects/{eff_project}");

        let mut read_opts = bq_model::read_session::TableReadOptions::new();
        let mut has_read_opts = false;
        if !selected_fields.is_empty() {
            read_opts = read_opts.set_selected_fields(selected_fields.to_vec());
            has_read_opts = true;
        }
        if let Some(filter) = row_restriction.filter(|s| !s.is_empty()) {
            read_opts = read_opts.set_row_restriction(filter);
            has_read_opts = true;
        }
        let codec = match compression {
            BigQueryArrowCompression::Unspecified => None,
            BigQueryArrowCompression::Lz4Frame => {
                Some(bq_model::arrow_serialization_options::CompressionCodec::Lz4Frame)
            }
            BigQueryArrowCompression::Zstd => {
                Some(bq_model::arrow_serialization_options::CompressionCodec::Zstd)
            }
        };
        if let Some(c) = codec {
            read_opts = read_opts.set_arrow_serialization_options(
                bq_model::ArrowSerializationOptions::new().set_buffer_compression(c),
            );
            has_read_opts = true;
        }

        let mut req_session = bq_model::ReadSession::new()
            .set_table(table_resource)
            .set_data_format(bq_model::DataFormat::Arrow);
        if has_read_opts {
            req_session = req_session.set_read_options(read_opts);
        }

        let session = read
            .create_read_session()
            .set_parent(parent)
            .set_read_session(req_session)
            .set_max_stream_count(max_streams.max(1))
            .send()
            .await?;

        let schema_bytes = session
            .arrow_schema()
            .map(|s| s.serialized_schema.clone())
            .filter(|b| !b.is_empty())
            .ok_or_else(|| {
                BigQueryError::InvalidArgument(
                    "CreateReadSession returned no Arrow schema".to_string(),
                )
            })?;

        let shm_name = next_bigquery_shm_name();
        let mut writer = ShmArrowStreamWriter::create(&shm_name, 64 * 1024, &schema_bytes)
            .map_err(BigQueryError::Shm)?;

        let mut join_set = tokio::task::JoinSet::new();
        for (stream_idx, stream) in session.streams.into_iter().enumerate() {
            let read_clone = read.clone();
            let stream_name = stream.name;
            join_set.spawn(async move {
                let mut reader = read_clone
                    .read_rows()
                    .set_read_stream(stream_name)
                    .set_offset(0)
                    .into_reader();
                let mut batches = Vec::new();
                while let Some(resp) = reader.next().await {
                    let resp = resp?;
                    if let Some(batch) = resp.arrow_record_batch()
                        && !batch.serialized_record_batch.is_empty()
                    {
                        let rows = resp.row_count.max(0) as usize;
                        batches.push((batch.serialized_record_batch.clone(), rows));
                    }
                }
                Ok::<(usize, Vec<(Bytes, usize)>), BigQueryError>((stream_idx, batches))
            });
        }

        let mut results_by_stream = Vec::with_capacity(join_set.len());
        while let Some(joined) = join_set.join_next().await {
            let pair =
                joined.map_err(|e| BigQueryError::Http(format!("ReadRows task failed: {e}")))??;
            results_by_stream.push(pair);
        }
        results_by_stream.sort_by_key(|(idx, _)| *idx);

        for (_, batches) in results_by_stream {
            for (batch_bytes, batch_rows) in batches {
                writer
                    .append_raw_stream_chunk(&batch_bytes, batch_rows)
                    .map_err(BigQueryError::Shm)?;
            }
        }

        let total_rows = writer.row_count();
        let (final_shm_name, byte_len, region) = writer.finish().map_err(BigQueryError::Shm)?;
        Ok((final_shm_name, byte_len, total_rows, region))
    }

    /// Append an Arrow IPC stream to `parent_table` (`projects/{p}/datasets/{d}/tables/{t}`) over
    /// BigQuery Storage Write API v1 (`_default`, `COMMITTED`, `PENDING`, or `BUFFERED`).
    pub async fn append_arrow_stream(
        &self,
        parent_table: &str,
        arrow_ipc_bytes: &[u8],
        mode: &str,
    ) -> Result<BigQueryAppendResult, BigQueryError> {
        let write_mode = BigQueryWriteMode::from_str_mode(mode)?;
        let pool = self.get_or_init_grpc_pool().await?;
        let normalized_table = self.normalize_table_resource(parent_table)?;

        let (schema_bytes, chunks) =
            split_arrow_ipc_stream(arrow_ipc_bytes, MAX_APPEND_ROWS_CHUNK_BYTES)?;
        if chunks.is_empty() {
            let default_stream = format!("{normalized_table}/streams/_default");
            return Ok(BigQueryAppendResult {
                appended_rows: 0,
                write_stream: default_stream,
            });
        }

        let write_stream_name = match write_mode {
            BigQueryWriteMode::Default => format!("{normalized_table}/streams/_default"),
            BigQueryWriteMode::Committed => {
                self.create_write_stream(&pool, &normalized_table, 1)
                    .await?
            }
            BigQueryWriteMode::Pending => {
                self.create_write_stream(&pool, &normalized_table, 2)
                    .await?
            }
            BigQueryWriteMode::Buffered => {
                self.create_write_stream(&pool, &normalized_table, 3)
                    .await?
            }
        };

        let (tx, rx) = mpsc::channel::<Bytes>(chunks.len().max(1));
        let mut cumulative_rows: i64 = 0;
        for (idx, (chunk_bytes, chunk_rows)) in chunks.iter().enumerate() {
            let offset = match write_mode {
                BigQueryWriteMode::Default => None,
                BigQueryWriteMode::Committed
                | BigQueryWriteMode::Pending
                | BigQueryWriteMode::Buffered => Some(cumulative_rows),
            };
            cumulative_rows += *chunk_rows;
            let req = encode_append_rows_request(
                if idx == 0 { &write_stream_name } else { "" },
                offset,
                if idx == 0 {
                    Some(schema_bytes.as_ref())
                } else {
                    None
                },
                chunk_bytes,
                *chunk_rows,
            );
            tx.send(req).await.map_err(|e| {
                BigQueryError::Http(format!("failed to enqueue AppendRows chunk: {e}"))
            })?;
        }
        drop(tx);

        let routing = format!("write_stream={write_stream_name}");
        let meta = self.build_grpc_metadata(Some(routing)).await?;
        let mut resp_stream = pool
            .bidi_streaming(
                "/google.cloud.bigquery.storage.v1.BigQueryWrite/AppendRows",
                rx,
                &meta,
            )
            .await?;

        while let Some(frame) = resp_stream.next_message().await? {
            let decoded = decode_append_rows_response(&frame)?;
            if !decoded.row_errors.is_empty() {
                return Err(BigQueryError::RowErrors(decoded.row_errors));
            }
            if let Some(status) = decoded.error
                && status.code != GrpcCode::Ok
            {
                return Err(BigQueryError::Grpc(status));
            }
        }

        match write_mode {
            BigQueryWriteMode::Default | BigQueryWriteMode::Committed => {}
            BigQueryWriteMode::Buffered => {
                let flush_req = encode_flush_rows_request(
                    &write_stream_name,
                    cumulative_rows.saturating_sub(1),
                );
                let flush_meta = self
                    .build_grpc_metadata(Some(format!("write_stream={write_stream_name}")))
                    .await?;
                let _ = pool
                    .unary(
                        "/google.cloud.bigquery.storage.v1.BigQueryWrite/FlushRows",
                        flush_req,
                        &flush_meta,
                    )
                    .await?;
            }
            BigQueryWriteMode::Pending => {
                let fin_req = encode_finalize_write_stream_request(&write_stream_name);
                let fin_meta = self
                    .build_grpc_metadata(Some(format!("name={write_stream_name}")))
                    .await?;
                let _ = pool
                    .unary(
                        "/google.cloud.bigquery.storage.v1.BigQueryWrite/FinalizeWriteStream",
                        fin_req,
                        &fin_meta,
                    )
                    .await?;

                let commit_req = encode_batch_commit_write_streams_request(
                    &normalized_table,
                    std::slice::from_ref(&write_stream_name),
                );
                let commit_meta = self
                    .build_grpc_metadata(Some(format!("parent={normalized_table}")))
                    .await?;
                let (commit_resp, _) = pool
                    .unary(
                        "/google.cloud.bigquery.storage.v1.BigQueryWrite/BatchCommitWriteStreams",
                        commit_req,
                        &commit_meta,
                    )
                    .await?;
                check_batch_commit_response(&commit_resp)?;
            }
        }

        Ok(BigQueryAppendResult {
            appended_rows: cumulative_rows.max(0) as u64,
            write_stream: write_stream_name,
        })
    }

    async fn create_write_stream(
        &self,
        pool: &GrpcChannelPool,
        parent_table: &str,
        stream_type: i32,
    ) -> Result<String, BigQueryError> {
        let req = encode_create_write_stream_request(parent_table, stream_type);
        let meta = self
            .build_grpc_metadata(Some(format!("parent={parent_table}")))
            .await?;
        let (resp, _) = pool
            .unary(
                "/google.cloud.bigquery.storage.v1.BigQueryWrite/CreateWriteStream",
                req,
                &meta,
            )
            .await?;
        decode_write_stream_name(&resp)
    }

    fn normalize_table_resource(&self, table: &str) -> Result<String, BigQueryError> {
        let trimmed = table.trim();
        if trimmed.starts_with("projects/") {
            return Ok(trimmed.to_string());
        }
        let parts = split_bigquery_table_parts(trimmed);
        match parts.as_slice() {
            [p, d, t] => Ok(format!("projects/{p}/datasets/{d}/tables/{t}")),
            [d, t] => Ok(format!(
                "projects/{}/datasets/{d}/tables/{t}",
                self.config.project_id
            )),
            [t] if !t.is_empty() => {
                let ds = self.config.dataset_id.as_deref().ok_or_else(|| {
                    BigQueryError::InvalidArgument(format!(
                        "unqualified table `{t}` requires dataset_id in BigQueryClientConfig"
                    ))
                })?;
                Ok(format!(
                    "projects/{}/datasets/{ds}/tables/{t}",
                    self.config.project_id
                ))
            }
            _ => Err(BigQueryError::InvalidArgument(format!(
                "invalid BigQuery table identifier: `{table}`"
            ))),
        }
    }

    async fn get_or_init_grpc_pool(&self) -> Result<Arc<GrpcChannelPool>, BigQueryError> {
        let pool = self
            .grpc_pool
            .get_or_try_init(|| async {
                let mut pool_cfg = GrpcPoolConfig::new(self.config.effective_storage_endpoint());
                pool_cfg.pool_size = self.config.pool_size.max(1);
                if let Some(ref pem) = self.config.ca_pem {
                    pool_cfg.ca_pem = Some(pem.clone());
                }
                let created = GrpcChannelPool::new(pool_cfg)?;
                Ok::<Arc<GrpcChannelPool>, BigQueryError>(Arc::new(created))
            })
            .await?;
        Ok(Arc::clone(pool))
    }

    async fn build_grpc_metadata(
        &self,
        routing_params: Option<String>,
    ) -> Result<GrpcRequestMetadata, BigQueryError> {
        let mut meta = GrpcRequestMetadata {
            routing_params,
            ..GrpcRequestMetadata::default()
        };
        if let Some(ref cache) = self.token_cache {
            let snap = cache.get_or_refresh().await.map_err(BigQueryError::Auth)?;
            meta.bearer_token = Some(snap.access_token.clone());
            meta.quota_project = snap
                .quota_project_id
                .clone()
                .or_else(|| self.config.auth_options.get("quota_project_id").cloned());
        } else if let Some(qp) = self.config.auth_options.get("quota_project_id") {
            meta.quota_project = Some(qp.clone());
        }
        Ok(meta)
    }

    async fn execute_fastpath_query(
        &self,
        sql: &str,
        params: &[BigQueryQueryParameter],
        options: &BigQueryQueryOptions,
    ) -> Result<BigQueryQueryResult, BigQueryError> {
        let payload = self.build_fastpath_query_payload(sql, params, options);
        let url = format!(
            "{}/bigquery/v2/projects/{}/queries",
            self.config.effective_rest_endpoint(),
            self.config.project_id
        );
        let resp = self
            .send_rest_json(reqwest::Method::POST, &url, Some(&payload))
            .await?;
        self.collect_query_response_pages(resp).await
    }

    async fn execute_jobs_insert_query(
        &self,
        sql: &str,
        params: &[BigQueryQueryParameter],
        options: &BigQueryQueryOptions,
    ) -> Result<BigQueryQueryResult, BigQueryError> {
        let payload = self.build_jobs_insert_payload(sql, params, options);
        let url = format!(
            "{}/bigquery/v2/projects/{}/jobs",
            self.config.effective_rest_endpoint(),
            self.config.project_id
        );
        let resp = self
            .send_rest_json(reqwest::Method::POST, &url, Some(&payload))
            .await?;
        check_rest_error(&resp)?;

        let job_ref = resp.get("jobReference");
        let job_id = job_ref
            .and_then(|jr| jr.get("jobId"))
            .and_then(Value::as_str)
            .map(str::to_string);
        let location = job_ref
            .and_then(|jr| jr.get("location"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| self.config.location.clone());

        let Some(ref jid) = job_id else {
            return decode_single_query_response(&resp);
        };

        let query_results_url = self.build_get_query_results_url(jid, location.as_deref(), None);
        match self
            .send_rest_json(reqwest::Method::GET, &query_results_url, None)
            .await
        {
            Ok(qresp) => self.collect_query_response_pages(qresp).await,
            Err(BigQueryError::Api { status: 404, .. }) => decode_single_query_response(&resp),
            Err(e) => Err(e),
        }
    }

    async fn collect_query_response_pages(
        &self,
        mut current: Value,
    ) -> Result<BigQueryQueryResult, BigQueryError> {
        check_rest_error(&current)?;

        let mut job_id = current
            .get("jobReference")
            .and_then(|jr| jr.get("jobId"))
            .and_then(Value::as_str)
            .map(str::to_string);
        let mut location = current
            .get("jobReference")
            .and_then(|jr| jr.get("location"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| self.config.location.clone());

        while !current
            .get("jobComplete")
            .and_then(Value::as_bool)
            .unwrap_or(true)
        {
            let jid = job_id.as_deref().ok_or_else(|| {
                BigQueryError::InvalidArgument(
                    "BigQuery response has jobComplete=false without jobReference.jobId"
                        .to_string(),
                )
            })?;
            let poll_url = self.build_get_query_results_url(jid, location.as_deref(), None);
            current = self
                .send_rest_json(reqwest::Method::GET, &poll_url, None)
                .await?;
            check_rest_error(&current)?;
            if job_id.is_none() {
                job_id = current
                    .get("jobReference")
                    .and_then(|jr| jr.get("jobId"))
                    .and_then(Value::as_str)
                    .map(str::to_string);
            }
            if location.is_none() {
                location = current
                    .get("jobReference")
                    .and_then(|jr| jr.get("location"))
                    .and_then(Value::as_str)
                    .map(str::to_string);
            }
        }

        let mut result = decode_single_query_response(&current)?;
        if result.job_id.is_none() {
            result.job_id = job_id.clone();
        }

        let mut page_token = current
            .get("pageToken")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string);

        while let Some(ref token) = page_token {
            let jid = result
                .job_id
                .as_deref()
                .or(job_id.as_deref())
                .ok_or_else(|| {
                    BigQueryError::InvalidArgument(
                        "BigQuery response returned pageToken without jobReference.jobId"
                            .to_string(),
                    )
                })?;
            let page_url =
                self.build_get_query_results_url(jid, location.as_deref(), Some(token.as_str()));
            let page_resp = self
                .send_rest_json(reqwest::Method::GET, &page_url, None)
                .await?;
            check_rest_error(&page_resp)?;
            if result.schema.is_empty()
                && let Some(fields) = page_resp
                    .get("schema")
                    .and_then(|s| s.get("fields"))
                    .and_then(Value::as_array)
            {
                result.schema = fields
                    .iter()
                    .filter_map(BigQueryFieldSchema::from_rest_json)
                    .collect();
                result.columns = result.schema.iter().map(|f| f.name.clone()).collect();
            }
            if let Some(raw_rows) = page_resp.get("rows").and_then(Value::as_array) {
                for raw_row in raw_rows {
                    result.rows.push(decode_table_row(raw_row, &result.schema));
                }
            }
            page_token = page_resp
                .get("pageToken")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string);
        }

        if result.total_rows == 0 && !result.rows.is_empty() {
            result.total_rows = result.rows.len() as u64;
        }
        Ok(result)
    }

    fn build_get_query_results_url(
        &self,
        job_id: &str,
        location: Option<&str>,
        page_token: Option<&str>,
    ) -> String {
        let mut url = format!(
            "{}/bigquery/v2/projects/{}/queries/{}?formatOptions.useInt64Timestamp=true",
            self.config.effective_rest_endpoint(),
            self.config.project_id,
            job_id
        );
        if let Some(loc) = location.filter(|s| !s.is_empty()) {
            url.push_str(&format!("&location={loc}"));
        }
        if let Some(tok) = page_token.filter(|s| !s.is_empty()) {
            url.push_str(&format!("&pageToken={tok}"));
        }
        url
    }

    fn build_fastpath_query_payload(
        &self,
        sql: &str,
        params: &[BigQueryQueryParameter],
        options: &BigQueryQueryOptions,
    ) -> Value {
        let mut body = Map::new();
        body.insert("query".to_string(), Value::String(sql.to_string()));
        body.insert("useLegacySql".to_string(), Value::Bool(false));
        body.insert(
            "jobCreationMode".to_string(),
            Value::String("JOB_CREATION_OPTIONAL".to_string()),
        );
        body.insert(
            "formatOptions".to_string(),
            json!({ "useInt64Timestamp": true }),
        );
        body.insert(
            "requestId".to_string(),
            Value::String(generate_request_uuid_v4()),
        );

        if let Some(ref ds) = self.config.dataset_id
            && !ds.is_empty()
        {
            body.insert(
                "defaultDataset".to_string(),
                json!({
                    "projectId": self.config.project_id,
                    "datasetId": ds,
                }),
            );
        }
        if let Some(ref loc) = self.config.location
            && !loc.is_empty()
        {
            body.insert("location".to_string(), Value::String(loc.clone()));
        }
        if let Some(max_bytes) = options
            .maximum_bytes_billed
            .or(self.config.maximum_bytes_billed)
        {
            body.insert(
                "maximumBytesBilled".to_string(),
                Value::String(max_bytes.to_string()),
            );
        }
        if let Some(timeout_ms) = options.job_timeout_ms.or(self.config.job_timeout_ms) {
            body.insert(
                "timeoutMs".to_string(),
                Value::String(timeout_ms.to_string()),
            );
        }
        if let Some(max_res) = options.max_results {
            body.insert("maxResults".to_string(), json!(max_res));
        }

        let mut merged_labels = self.config.labels.clone();
        for (k, v) in &options.labels {
            merged_labels.insert(k.clone(), v.clone());
        }
        if !merged_labels.is_empty() {
            body.insert("labels".to_string(), json!(merged_labels));
        }

        if options.create_session {
            body.insert("createSession".to_string(), Value::Bool(true));
        }
        if let Some(ref sid) = options.session_id
            && !sid.is_empty()
        {
            body.insert(
                "connectionProperties".to_string(),
                json!([{ "key": "session_id", "value": sid }]),
            );
        }

        if !params.is_empty() {
            let mode = if params.iter().any(|p| p.name.is_some()) {
                "NAMED"
            } else {
                "POSITIONAL"
            };
            body.insert("parameterMode".to_string(), Value::String(mode.to_string()));
            body.insert(
                "queryParameters".to_string(),
                Value::Array(
                    params
                        .iter()
                        .map(BigQueryQueryParameter::to_rest_json)
                        .collect(),
                ),
            );
        }

        Value::Object(body)
    }

    fn build_jobs_insert_payload(
        &self,
        sql: &str,
        params: &[BigQueryQueryParameter],
        options: &BigQueryQueryOptions,
    ) -> Value {
        let mut query_cfg = Map::new();
        query_cfg.insert("query".to_string(), Value::String(sql.to_string()));
        query_cfg.insert("useLegacySql".to_string(), Value::Bool(false));

        if let Some(ref ds) = self.config.dataset_id
            && !ds.is_empty()
        {
            query_cfg.insert(
                "defaultDataset".to_string(),
                json!({
                    "projectId": self.config.project_id,
                    "datasetId": ds,
                }),
            );
        }
        if let Some(max_bytes) = options
            .maximum_bytes_billed
            .or(self.config.maximum_bytes_billed)
        {
            query_cfg.insert(
                "maximumBytesBilled".to_string(),
                Value::String(max_bytes.to_string()),
            );
        }
        if let Some(prio) = options.priority.as_ref().or(self.config.priority.as_ref()) {
            query_cfg.insert("priority".to_string(), Value::String(prio.clone()));
        }
        if options.create_session {
            query_cfg.insert("createSession".to_string(), Value::Bool(true));
        }
        if let Some(ref sid) = options.session_id
            && !sid.is_empty()
        {
            query_cfg.insert(
                "connectionProperties".to_string(),
                json!([{ "key": "session_id", "value": sid }]),
            );
        }
        if let Some(ref dest) = options.destination_table
            && !dest.is_empty()
        {
            let parts = split_bigquery_table_parts(dest);
            let dest_json = match parts.as_slice() {
                [p, d, t] => json!({ "projectId": p, "datasetId": d, "tableId": t }),
                [d, t] => {
                    json!({ "projectId": self.config.project_id, "datasetId": d, "tableId": t })
                }
                _ => json!({
                    "projectId": self.config.project_id,
                    "datasetId": self.config.dataset_id.clone().unwrap_or_default(),
                    "tableId": dest.trim().replace('`', ""),
                }),
            };
            query_cfg.insert("destinationTable".to_string(), dest_json);
            if let Some(ref wd) = options.write_disposition
                && !wd.is_empty()
            {
                query_cfg.insert("writeDisposition".to_string(), Value::String(wd.clone()));
            }
        }
        if !params.is_empty() {
            let mode = if params.iter().any(|p| p.name.is_some()) {
                "NAMED"
            } else {
                "POSITIONAL"
            };
            query_cfg.insert("parameterMode".to_string(), Value::String(mode.to_string()));
            query_cfg.insert(
                "queryParameters".to_string(),
                Value::Array(
                    params
                        .iter()
                        .map(BigQueryQueryParameter::to_rest_json)
                        .collect(),
                ),
            );
        }

        let mut configuration = Map::new();
        configuration.insert("query".to_string(), Value::Object(query_cfg));
        if options.dry_run {
            configuration.insert("dryRun".to_string(), Value::Bool(true));
        }
        if let Some(timeout_ms) = options.job_timeout_ms.or(self.config.job_timeout_ms) {
            configuration.insert(
                "jobTimeoutMs".to_string(),
                Value::String(timeout_ms.to_string()),
            );
        }
        let mut merged_labels = self.config.labels.clone();
        for (k, v) in &options.labels {
            merged_labels.insert(k.clone(), v.clone());
        }
        if !merged_labels.is_empty() {
            configuration.insert("labels".to_string(), json!(merged_labels));
        }

        let mut job_ref = Map::new();
        job_ref.insert(
            "projectId".to_string(),
            Value::String(self.config.project_id.clone()),
        );
        job_ref.insert(
            "jobId".to_string(),
            Value::String(generate_request_uuid_v4()),
        );
        if let Some(ref loc) = self.config.location
            && !loc.is_empty()
        {
            job_ref.insert("location".to_string(), Value::String(loc.clone()));
        }

        json!({
            "jobReference": Value::Object(job_ref),
            "configuration": Value::Object(configuration),
        })
    }

    async fn send_rest_json(
        &self,
        method: reqwest::Method,
        url: &str,
        body: Option<&Value>,
    ) -> Result<Value, BigQueryError> {
        let mut snapshot = None;
        if let Some(ref cache) = self.token_cache {
            snapshot = Some(cache.get_or_refresh().await.map_err(BigQueryError::Auth)?);
        }

        let mut resp = self
            .dispatch_http_once(method.clone(), url, body, snapshot.as_deref())
            .await?;

        if resp.status() == reqwest::StatusCode::UNAUTHORIZED
            && let (Some(cache), Some(snap)) = (&self.token_cache, &snapshot)
        {
            cache.invalidate(snap.generation);
            let refreshed = cache.get_or_refresh().await.map_err(BigQueryError::Auth)?;
            resp = self
                .dispatch_http_once(method, url, body, Some(&refreshed))
                .await?;
        }

        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| BigQueryError::Http(e.to_string()))?;

        if !status.is_success() {
            let message = serde_json::from_str::<Value>(&text)
                .ok()
                .and_then(|v| {
                    v.get("error")
                        .and_then(|e| e.get("message"))
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
                .unwrap_or_else(|| text.clone());
            return Err(BigQueryError::Api {
                status: status.as_u16(),
                message,
            });
        }

        if text.trim().is_empty() {
            return Ok(json!({}));
        }
        serde_json::from_str::<Value>(&text)
            .map_err(|e| BigQueryError::Http(format!("invalid JSON from BigQuery REST API: {e}")))
    }

    async fn dispatch_http_once(
        &self,
        method: reqwest::Method,
        url: &str,
        body: Option<&Value>,
        snapshot: Option<&vertebra_core::gcp_auth::GcpTokenSnapshot>,
    ) -> Result<reqwest::Response, BigQueryError> {
        let mut req = self.http_client.request(method, url);
        if let Some(snap) = snapshot {
            req = req.header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", snap.access_token),
            );
            if let Some(qp) = snap
                .quota_project_id
                .as_ref()
                .or_else(|| self.config.auth_options.get("quota_project_id"))
            {
                req = req.header("x-goog-user-project", qp.as_str());
            }
        } else if let Some(qp) = self.config.auth_options.get("quota_project_id") {
            req = req.header("x-goog-user-project", qp.as_str());
        }
        if let Some(json_body) = body {
            req = req.json(json_body);
        }
        req.send()
            .await
            .map_err(|e| BigQueryError::Http(e.to_string()))
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

fn check_rest_error(resp: &Value) -> Result<(), BigQueryError> {
    if let Some(err) = resp.get("error") {
        let code = err.get("code").and_then(Value::as_u64).unwrap_or(500) as u16;
        let msg = err
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("unknown BigQuery error")
            .to_string();
        return Err(BigQueryError::Api {
            status: code,
            message: msg,
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
        return Err(BigQueryError::Api {
            status: 400,
            message: msg,
        });
    }
    if let Some(errors) = resp.get("errors").and_then(Value::as_array)
        && let Some(first) = errors.first()
    {
        let msg = first
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("BigQuery query error")
            .to_string();
        return Err(BigQueryError::Api {
            status: 400,
            message: msg,
        });
    }
    Ok(())
}

fn parse_json_i64(val: &Value) -> Option<i64> {
    match val {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

fn parse_json_u64(val: &Value) -> Option<u64> {
    match val {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.parse::<u64>().ok(),
        _ => None,
    }
}

fn decode_single_query_response(resp: &Value) -> Result<BigQueryQueryResult, BigQueryError> {
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

fn decode_table_row(row_val: &Value, schema: &[BigQueryFieldSchema]) -> Vec<BigQueryCellValue> {
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

fn decode_cell_value(v: &Value, field: &BigQueryFieldSchema) -> BigQueryCellValue {
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

fn decode_untyped_cell(v: &Value) -> BigQueryCellValue {
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

fn strip_leading_sql_comments(mut s: &str) -> &str {
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

/// Encode a `CreateWriteStreamRequest` protobuf message (`COMMITTED = 1`, `PENDING = 2`, `BUFFERED = 3`).
pub fn encode_create_write_stream_request(parent: &str, stream_type: i32) -> Bytes {
    let mut req = ProtoWriter::new();
    req.write_str(1, parent);
    req.write_nested(2, |ws| {
        ws.write_i32(2, stream_type);
    });
    req.finish()
}

/// Decode a `WriteStream` protobuf message and return its `name` (`field 1`).
pub fn decode_write_stream_name(buf: &[u8]) -> Result<String, BigQueryError> {
    let mut reader = ProtoReader::new(buf);
    while let Some((field_no, wire_type)) = reader.next_tag()? {
        if field_no == 1 && wire_type == WireType::LengthDelimited {
            return Ok(reader.read_str()?.to_string());
        }
        reader.skip_field(wire_type)?;
    }
    Err(BigQueryError::InvalidArgument(
        "CreateWriteStream response missing WriteStream.name".to_string(),
    ))
}

/// Encode an `AppendRowsRequest` protobuf message for `BigQueryWrite.AppendRows`.
pub fn encode_append_rows_request(
    write_stream: &str,
    offset: Option<i64>,
    writer_schema_ipc: Option<&[u8]>,
    record_batch_ipc: &[u8],
    row_count: i64,
) -> Bytes {
    let mut req = ProtoWriter::with_capacity(record_batch_ipc.len() + 128);
    if !write_stream.is_empty() {
        req.write_str(1, write_stream);
    }
    if let Some(off) = offset {
        req.write_nested(2, |off_msg| {
            off_msg.write_i64(1, off);
        });
    }
    req.write_nested(4, |arrow_data| {
        if let Some(schema_bytes) = writer_schema_ipc
            && !schema_bytes.is_empty()
        {
            arrow_data.write_nested(1, |schema_msg| {
                schema_msg.write_bytes(1, schema_bytes);
            });
        }
        arrow_data.write_nested(2, |batch_msg| {
            batch_msg.write_bytes(1, record_batch_ipc);
            if row_count != 0 {
                batch_msg.write_i64(2, row_count);
            }
        });
    });
    req.finish()
}

/// Decoded `AppendRowsRequest` message for wire testing and inspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedAppendRowsRequest {
    pub write_stream: String,
    pub offset: Option<i64>,
    pub writer_schema: Option<Bytes>,
    pub serialized_record_batch: Bytes,
    pub row_count: i64,
}

/// Decode an `AppendRowsRequest` protobuf message (accepting `arrow_rows` on field `4` or `5`).
pub fn decode_append_rows_request(buf: &[u8]) -> Result<DecodedAppendRowsRequest, ProtoError> {
    let mut reader = ProtoReader::new(buf);
    let mut write_stream = String::new();
    let mut offset = None;
    let mut writer_schema = None;
    let mut serialized_record_batch = Bytes::new();
    let mut row_count = 0i64;

    while let Some((field_no, wire_type)) = reader.next_tag()? {
        match (field_no, wire_type) {
            (1, WireType::LengthDelimited) => {
                write_stream = reader.read_str()?.to_string();
            }
            (2, WireType::LengthDelimited) => {
                let mut off_reader = reader.read_nested()?;
                let mut off_val = 0i64;
                while let Some((of, ow)) = off_reader.next_tag()? {
                    if of == 1 && ow == WireType::Varint {
                        off_val = off_reader.read_i64()?;
                    } else {
                        off_reader.skip_field(ow)?;
                    }
                }
                offset = Some(off_val);
            }
            (2, WireType::Varint) => {
                offset = Some(reader.read_i64()?);
            }
            (4 | 5, WireType::LengthDelimited) => {
                let mut arrow_reader = reader.read_nested()?;
                while let Some((af, aw)) = arrow_reader.next_tag()? {
                    match (af, aw) {
                        (1, WireType::LengthDelimited) => {
                            let mut sreader = arrow_reader.read_nested()?;
                            while let Some((sf, sw)) = sreader.next_tag()? {
                                if sf == 1 && sw == WireType::LengthDelimited {
                                    writer_schema =
                                        Some(Bytes::copy_from_slice(sreader.read_bytes()?));
                                } else {
                                    sreader.skip_field(sw)?;
                                }
                            }
                        }
                        (2, WireType::LengthDelimited) => {
                            let mut breader = arrow_reader.read_nested()?;
                            while let Some((bf, bw)) = breader.next_tag()? {
                                match (bf, bw) {
                                    (1, WireType::LengthDelimited) => {
                                        serialized_record_batch =
                                            Bytes::copy_from_slice(breader.read_bytes()?);
                                    }
                                    (2, WireType::Varint) => {
                                        row_count = breader.read_i64()?;
                                    }
                                    (_, wt) => breader.skip_field(wt)?,
                                }
                            }
                        }
                        (_, wt) => arrow_reader.skip_field(wt)?,
                    }
                }
            }
            (_, wt) => reader.skip_field(wt)?,
        }
    }

    Ok(DecodedAppendRowsRequest {
        write_stream,
        offset,
        writer_schema,
        serialized_record_batch,
        row_count,
    })
}

/// Decoded `AppendRowsResponse` protobuf message.
#[derive(Debug, Clone)]
pub struct DecodedAppendRowsResponse {
    pub appended_offset: Option<i64>,
    pub error: Option<GrpcStatus>,
    pub row_errors: Vec<BigQueryRowError>,
}

/// Decode an `AppendRowsResponse` protobuf message.
pub fn decode_append_rows_response(buf: &[u8]) -> Result<DecodedAppendRowsResponse, ProtoError> {
    let mut reader = ProtoReader::new(buf);
    let mut appended_offset = None;
    let mut error = None;
    let mut row_errors = Vec::new();

    while let Some((field_no, wire_type)) = reader.next_tag()? {
        match (field_no, wire_type) {
            (1, WireType::LengthDelimited) => {
                let mut res_reader = reader.read_nested()?;
                while let Some((rf, rw)) = res_reader.next_tag()? {
                    if rf == 1 && rw == WireType::LengthDelimited {
                        let mut off_reader = res_reader.read_nested()?;
                        let mut val = 0i64;
                        while let Some((of, ow)) = off_reader.next_tag()? {
                            if of == 1 && ow == WireType::Varint {
                                val = off_reader.read_i64()?;
                            } else {
                                off_reader.skip_field(ow)?;
                            }
                        }
                        appended_offset = Some(val);
                    } else if rf == 1 && rw == WireType::Varint {
                        appended_offset = Some(res_reader.read_i64()?);
                    } else {
                        res_reader.skip_field(rw)?;
                    }
                }
            }
            (2, WireType::LengthDelimited) => {
                let mut status_reader = reader.read_nested()?;
                let mut code = 0i32;
                let mut message = String::new();
                while let Some((sf, sw)) = status_reader.next_tag()? {
                    match (sf, sw) {
                        (1, WireType::Varint) => code = status_reader.read_i32()?,
                        (2, WireType::LengthDelimited) => {
                            message = status_reader.read_str()?.to_string();
                        }
                        (_, wt) => status_reader.skip_field(wt)?,
                    }
                }
                error = Some(GrpcStatus {
                    code: GrpcCode::from_i32(code),
                    raw_code: code,
                    message,
                    details: Vec::new(),
                });
            }
            (4 | 5, WireType::LengthDelimited) => {
                let mut err_reader = reader.read_nested()?;
                let mut index = 0i64;
                let mut code = 0i32;
                let mut message = String::new();
                while let Some((ef, ew)) = err_reader.next_tag()? {
                    match (ef, ew) {
                        (1, WireType::Varint) => index = err_reader.read_i64()?,
                        (2, WireType::Varint) => code = err_reader.read_i32()?,
                        (3, WireType::LengthDelimited) => {
                            message = err_reader.read_str()?.to_string();
                        }
                        (_, wt) => err_reader.skip_field(wt)?,
                    }
                }
                row_errors.push(BigQueryRowError {
                    index,
                    code,
                    message,
                });
            }
            (_, wt) => reader.skip_field(wt)?,
        }
    }

    Ok(DecodedAppendRowsResponse {
        appended_offset,
        error,
        row_errors,
    })
}

/// Encode a `FinalizeWriteStreamRequest` protobuf message.
pub fn encode_finalize_write_stream_request(name: &str) -> Bytes {
    let mut req = ProtoWriter::new();
    req.write_str(1, name);
    req.finish()
}

/// Encode a `BatchCommitWriteStreamsRequest` protobuf message.
pub fn encode_batch_commit_write_streams_request(parent: &str, write_streams: &[String]) -> Bytes {
    let mut req = ProtoWriter::new();
    req.write_str(1, parent);
    for ws in write_streams {
        req.write_str(2, ws);
    }
    req.finish()
}

/// Encode a `FlushRowsRequest` protobuf message.
pub fn encode_flush_rows_request(write_stream: &str, offset: i64) -> Bytes {
    let mut req = ProtoWriter::new();
    req.write_str(1, write_stream);
    req.write_nested(2, |off_msg| {
        off_msg.write_i64(1, offset);
    });
    req.finish()
}

fn check_batch_commit_response(buf: &[u8]) -> Result<(), BigQueryError> {
    let mut reader = ProtoReader::new(buf);
    while let Some((field_no, wire_type)) = reader.next_tag()? {
        if field_no == 2 && wire_type == WireType::LengthDelimited {
            let mut err_reader = reader.read_nested()?;
            let mut stream = String::new();
            let mut message = String::new();
            while let Some((ef, ew)) = err_reader.next_tag()? {
                match (ef, ew) {
                    (1, WireType::LengthDelimited) => stream = err_reader.read_str()?.to_string(),
                    (3, WireType::LengthDelimited) => message = err_reader.read_str()?.to_string(),
                    (_, wt) => err_reader.skip_field(wt)?,
                }
            }
            return Err(BigQueryError::InvalidArgument(format!(
                "BatchCommitWriteStreams failed on stream `{stream}`: {message}"
            )));
        }
        reader.skip_field(wire_type)?;
    }
    Ok(())
}

/// Split an Arrow IPC stream into its encapsulated `Schema` frame and `<= max_chunk_bytes`
/// `RecordBatch` frames `(batch_bytes, row_count)`.
pub fn split_arrow_ipc_stream(
    stream_bytes: &[u8],
    max_chunk_bytes: usize,
) -> Result<(Bytes, Vec<(Bytes, i64)>), BigQueryError> {
    if stream_bytes.is_empty() {
        return Ok((Bytes::new(), Vec::new()));
    }
    let effective_limit = max_chunk_bytes.max(1024);

    if let Some((schema, batches)) = try_parse_encapsulated_arrow_stream(stream_bytes) {
        let mut chunked = Vec::new();
        let mut accum = BytesMut::new();
        let mut accum_rows = 0i64;

        for (batch_bytes, rows) in batches {
            if !accum.is_empty() && accum.len() + batch_bytes.len() > effective_limit {
                chunked.push((accum.split().freeze(), accum_rows));
                accum_rows = 0;
            }
            accum.put_slice(&batch_bytes);
            accum_rows += rows;
            if accum.len() >= effective_limit {
                chunked.push((accum.split().freeze(), accum_rows));
                accum_rows = 0;
            }
        }
        if !accum.is_empty() {
            chunked.push((accum.freeze(), accum_rows));
        }
        return Ok((schema, chunked));
    }

    let stripped = if stream_bytes.len() >= ARROW_IPC_EOS_MARKER.len()
        && stream_bytes[stream_bytes.len() - ARROW_IPC_EOS_MARKER.len()..] == ARROW_IPC_EOS_MARKER
    {
        &stream_bytes[..stream_bytes.len() - ARROW_IPC_EOS_MARKER.len()]
    } else {
        stream_bytes
    };
    if stripped.is_empty() {
        return Ok((Bytes::new(), Vec::new()));
    }

    let mut chunks = Vec::new();
    for slice in stripped.chunks(effective_limit) {
        chunks.push((Bytes::copy_from_slice(slice), 1));
    }
    Ok((
        Bytes::copy_from_slice(&stripped[..stripped.len().min(64)]),
        chunks,
    ))
}

fn try_parse_encapsulated_arrow_stream(buf: &[u8]) -> Option<(Bytes, Vec<(Bytes, i64)>)> {
    if buf.len() < 8 || buf[0..4] != [0xFF, 0xFF, 0xFF, 0xFF] {
        return None;
    }

    let mut pos = 0usize;
    let mut schema_frame: Option<Bytes> = None;
    let mut batches: Vec<(Bytes, i64)> = Vec::new();

    while pos + 8 <= buf.len() {
        if buf[pos..pos + 4] != [0xFF, 0xFF, 0xFF, 0xFF] {
            break;
        }
        let meta_size =
            u32::from_le_bytes([buf[pos + 4], buf[pos + 5], buf[pos + 6], buf[pos + 7]]) as usize;
        if meta_size == 0 {
            break;
        }
        let padded_meta = (meta_size + 7) & !7;
        let meta_start = pos + 8;
        let meta_end = meta_start.checked_add(padded_meta)?;
        if meta_end > buf.len() {
            return None;
        }

        let fb = &buf[meta_start..meta_start + meta_size];
        let (header_type, body_len, row_count) = inspect_arrow_message_flatbuffer(fb)?;
        let padded_body = (body_len + 7) & !7;
        let frame_end = meta_end.checked_add(padded_body)?;
        if frame_end > buf.len() {
            return None;
        }

        let frame_bytes = Bytes::copy_from_slice(&buf[pos..frame_end]);
        if header_type == 1 && schema_frame.is_none() {
            schema_frame = Some(frame_bytes);
        } else {
            batches.push((frame_bytes, row_count.max(1)));
        }
        pos = frame_end;
    }

    let schema = schema_frame?;
    Some((schema, batches))
}

fn inspect_arrow_message_flatbuffer(fb: &[u8]) -> Option<(u8, usize, i64)> {
    if fb.len() < 12 {
        return None;
    }
    let root_offset = u32::from_le_bytes([fb[0], fb[1], fb[2], fb[3]]) as usize;
    if root_offset + 4 > fb.len() {
        return None;
    }
    let vtable_rel = i32::from_le_bytes([
        fb[root_offset],
        fb[root_offset + 1],
        fb[root_offset + 2],
        fb[root_offset + 3],
    ]);
    let vtable_pos = (root_offset as isize).checked_sub(vtable_rel as isize)? as usize;
    if vtable_pos + 4 > fb.len() {
        return None;
    }
    let vtable_len = u16::from_le_bytes([fb[vtable_pos], fb[vtable_pos + 1]]) as usize;
    if vtable_len < 6 || vtable_pos + vtable_len > fb.len() {
        return None;
    }

    let read_vtable_slot = |slot_offset: usize| -> Option<usize> {
        if slot_offset + 2 > vtable_len {
            return Some(0);
        }
        let field_off = u16::from_le_bytes([
            fb[vtable_pos + slot_offset],
            fb[vtable_pos + slot_offset + 1],
        ]) as usize;
        Some(field_off)
    };

    let header_type_off = read_vtable_slot(6)?;
    let header_type = if header_type_off > 0 && root_offset + header_type_off < fb.len() {
        fb[root_offset + header_type_off]
    } else {
        0
    };
    if !(1..=3).contains(&header_type) {
        return None;
    }

    let body_len_off = read_vtable_slot(10)?;
    let body_len = if body_len_off > 0 && root_offset + body_len_off + 8 <= fb.len() {
        let p = root_offset + body_len_off;
        i64::from_le_bytes([
            fb[p],
            fb[p + 1],
            fb[p + 2],
            fb[p + 3],
            fb[p + 4],
            fb[p + 5],
            fb[p + 6],
            fb[p + 7],
        ])
        .max(0) as usize
    } else {
        0
    };

    let mut row_count = 0i64;
    if header_type == 3 {
        let header_off = read_vtable_slot(8)?;
        if header_off > 0 && root_offset + header_off + 4 <= fb.len() {
            let union_loc = root_offset + header_off;
            let rel = u32::from_le_bytes([
                fb[union_loc],
                fb[union_loc + 1],
                fb[union_loc + 2],
                fb[union_loc + 3],
            ]) as usize;
            let rb_table = union_loc.checked_add(rel)?;
            if rb_table + 4 <= fb.len() {
                let rb_vtable_rel = i32::from_le_bytes([
                    fb[rb_table],
                    fb[rb_table + 1],
                    fb[rb_table + 2],
                    fb[rb_table + 3],
                ]);
                let rb_vtable = (rb_table as isize).checked_sub(rb_vtable_rel as isize)? as usize;
                if rb_vtable + 6 <= fb.len() {
                    let len_off =
                        u16::from_le_bytes([fb[rb_vtable + 4], fb[rb_vtable + 5]]) as usize;
                    if len_off > 0 && rb_table + len_off + 8 <= fb.len() {
                        let lp = rb_table + len_off;
                        row_count = i64::from_le_bytes([
                            fb[lp],
                            fb[lp + 1],
                            fb[lp + 2],
                            fb[lp + 3],
                            fb[lp + 4],
                            fb[lp + 5],
                            fb[lp + 6],
                            fb[lp + 7],
                        ]);
                    }
                }
            }
        }
    }

    Some((header_type, body_len, row_count))
}
