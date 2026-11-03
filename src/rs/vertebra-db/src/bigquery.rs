//! Pure-Rust Google Cloud BigQuery REST v2 (`jobs.query` + `jobs.insert`) and Storage Read/Write API v1
//! (`BigQueryRead` -> `VTBARROW` `ShmRegion` & `BigQueryWrite.AppendRows`) client engine.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD as BASE64_STD, STANDARD_NO_PAD as BASE64_NO_PAD};
use bytes::Bytes;
use google_cloud_bigquery::client::{Read, Write};
use google_cloud_bigquery::error::{AppendError, CommitError, WriterBuilderError};
use google_cloud_bigquery::model as bq_model;
use google_cloud_bigquery::write::retry_policy::RetryableErrors as WriteRetryableErrors;
use google_cloud_bigquery::write::stream_type::{BufferedStream, CommittedStream, PendingStream};
use google_cloud_gax::retry_policy::RetryPolicyExt as _;
use serde_json::{Map, Value, json};
use thiserror::Error;
use vertebra_core::gcp_auth::GcpTokenCache;
use vertebra_core::id::uuid4_str;
use vertebra_core::shm::ShmRegion;

use crate::arrow_shm::{ARROW_IPC_EOS_MARKER, ShmArrowStreamWriter};
use crate::gcp::{install_rustls_ring_provider, official_credentials};

/// Default BigQuery REST v2 endpoint.
pub const DEFAULT_BIGQUERY_REST_ENDPOINT: &str = "https://bigquery.googleapis.com";

/// Default BigQuery Storage API v1 gRPC endpoint.
pub const DEFAULT_BIGQUERY_STORAGE_ENDPOINT: &str = "https://bigquerystorage.googleapis.com:443";

/// Maximum Arrow payload size per `AppendRowsRequest` message (`9 MiB` safe bound under `10 MiB` limit).
pub const MAX_APPEND_ROWS_CHUNK_BYTES: usize = 9 * 1024 * 1024;

/// Default OAuth2 scopes for BigQuery REST v2 and Storage API v1 authentication.
pub const BIGQUERY_OAUTH_SCOPES: &[&str] = &[
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform",
];

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
    #[error("BigQuery shared-memory Arrow error: {0}")]
    Shm(String),
    #[error("BigQuery Storage Write row errors: {0:?}")]
    RowErrors(Vec<BigQueryRowError>),
    #[error("BigQuery Storage Write verification failed: {0}")]
    AppendVerification(String),
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
    write_client: tokio::sync::OnceCell<Write>,
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
        if is_cleartext {
            auth_opts
                .entry("allow_ambient_adc".to_string())
                .or_insert_with(|| "false".to_string());
        }
        let token_cache = GcpTokenCache::from_options(&auth_opts, BIGQUERY_OAUTH_SCOPES);

        Ok(Self {
            config,
            http_client,
            token_cache,
            injected_read: None,
            read_client: tokio::sync::OnceCell::new(),
            write_client: tokio::sync::OnceCell::new(),
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

    async fn storage_write(&self) -> Result<&Write, BigQueryError> {
        self.write_client
            .get_or_try_init(|| async {
                install_rustls_ring_provider();
                Write::builder()
                    .with_endpoint(self.config.effective_storage_endpoint())
                    .with_credentials(official_credentials(self.token_cache.clone()))
                    .build()
                    .await
                    .map_err(|e| {
                        BigQueryError::Http(format!("failed to build BigQuery Write client: {e}"))
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
        let table = self.normalize_table_resource(parent_table)?;
        let plan = ArrowAppendPlan::build(arrow_ipc_bytes, write_mode)?;
        if plan.requests.is_empty() {
            return Ok(BigQueryAppendResult {
                appended_rows: 0,
                write_stream: format!("{table}/streams/_default"),
            });
        }

        let write = self.storage_write().await?;
        let schema = bq_model::ArrowSchema::new().set_serialized_schema(plan.schema.clone());

        match write_mode {
            BigQueryWriteMode::Default => {
                let writer = write
                    .open_default_stream(&table)
                    .with_retry_policy(WriteRetryableErrors.with_attempt_limit(3))
                    .build_arrow(schema)
                    .await
                    .map_err(map_writer_builder_error)?;
                let mut pending = Vec::with_capacity(plan.requests.len());
                for req in &plan.requests {
                    pending.push(writer.append(build_arrow_record_batch(req)).send());
                }
                let mut offsets = Vec::with_capacity(pending.len());
                for fut in pending {
                    let resp = fut.await.map_err(map_append_error)?;
                    offsets.push(resp.offset);
                }
                let appended_rows = plan.verify_responses(&offsets)?;
                Ok(BigQueryAppendResult {
                    appended_rows,
                    write_stream: format!("{table}/streams/_default"),
                })
            }
            BigQueryWriteMode::Committed => {
                let writer = write
                    .create_stream::<CommittedStream>(&table)
                    .build_arrow(schema)
                    .await
                    .map_err(map_writer_builder_error)?;
                let write_stream = writer.write_stream().to_string();
                let mut pending = Vec::with_capacity(plan.requests.len());
                for req in &plan.requests {
                    pending.push(
                        writer
                            .append(build_arrow_record_batch(req))
                            .set_offset(req.offset.unwrap_or(0))
                            .send(),
                    );
                }
                let mut offsets = Vec::with_capacity(pending.len());
                for fut in pending {
                    let resp = fut.await.map_err(map_append_error)?;
                    offsets.push(resp.offset);
                }
                let appended_rows = plan.verify_responses(&offsets)?;
                Ok(BigQueryAppendResult {
                    appended_rows,
                    write_stream,
                })
            }
            BigQueryWriteMode::Pending => {
                let writer = write
                    .create_stream::<PendingStream>(&table)
                    .build_arrow(schema)
                    .await
                    .map_err(map_writer_builder_error)?;
                let write_stream = writer.write_stream().to_string();
                let mut pending = Vec::with_capacity(plan.requests.len());
                for req in &plan.requests {
                    pending.push(
                        writer
                            .append(build_arrow_record_batch(req))
                            .set_offset(req.offset.unwrap_or(0))
                            .send(),
                    );
                }
                let mut offsets = Vec::with_capacity(pending.len());
                for fut in pending {
                    let resp = fut.await.map_err(map_append_error)?;
                    offsets.push(resp.offset);
                }
                let appended_rows = plan.verify_responses(&offsets)?;
                writer.finalize().await?;
                writer.commit().await.map_err(map_commit_error)?;
                Ok(BigQueryAppendResult {
                    appended_rows,
                    write_stream,
                })
            }
            BigQueryWriteMode::Buffered => {
                let writer = write
                    .create_stream::<BufferedStream>(&table)
                    .build_arrow(schema)
                    .await
                    .map_err(map_writer_builder_error)?;
                let write_stream = writer.write_stream().to_string();
                let mut pending = Vec::with_capacity(plan.requests.len());
                for req in &plan.requests {
                    pending.push(
                        writer
                            .append(build_arrow_record_batch(req))
                            .set_offset(req.offset.unwrap_or(0))
                            .send(),
                    );
                }
                let mut offsets = Vec::with_capacity(pending.len());
                for fut in pending {
                    let resp = fut.await.map_err(map_append_error)?;
                    offsets.push(resp.offset);
                }
                let appended_rows = plan.verify_responses(&offsets)?;
                if plan.total_rows() > 0 {
                    writer.flush(plan.total_rows() - 1).await?;
                }
                Ok(BigQueryAppendResult {
                    appended_rows,
                    write_stream,
                })
            }
        }
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
        body.insert("requestId".to_string(), Value::String(uuid4_str()));

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
        job_ref.insert("jobId".to_string(), Value::String(uuid4_str()));
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

#[allow(deprecated)]
fn build_arrow_record_batch(req: &ArrowAppendRequest) -> bq_model::ArrowRecordBatch {
    bq_model::ArrowRecordBatch::new()
        .set_serialized_record_batch(req.record_batch.clone())
        .set_row_count(req.row_count)
}

fn map_writer_builder_error(err: WriterBuilderError) -> BigQueryError {
    BigQueryError::AppendVerification(err.to_string())
}

fn map_append_error(err: AppendError) -> BigQueryError {
    match err {
        AppendError::RowErrors { row_errors, .. } => BigQueryError::RowErrors(
            row_errors
                .into_iter()
                .map(|e| BigQueryRowError {
                    index: e.index,
                    code: e.code.value().unwrap_or(0),
                    message: e.message,
                })
                .collect(),
        ),
        AppendError::Rpc { source, .. } => BigQueryError::Client(source),
        other => BigQueryError::AppendVerification(other.to_string()),
    }
}

fn map_commit_error(err: CommitError) -> BigQueryError {
    match err {
        CommitError::Rpc { source, .. } => BigQueryError::Client(source),
        CommitError::FailedTransaction { stream_errors, .. } => {
            if let Some(first) = stream_errors.first() {
                BigQueryError::AppendVerification(format!(
                    "BatchCommitWriteStreams rejected stream {}: {}",
                    first.entity, first.error_message
                ))
            } else {
                BigQueryError::AppendVerification(
                    "BatchCommitWriteStreams rejected transaction".to_string(),
                )
            }
        }
        other => BigQueryError::AppendVerification(other.to_string()),
    }
}

/// One `AppendRowsRequest` worth of Arrow data: a single encapsulated record-batch message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArrowAppendRequest {
    pub record_batch: Bytes,
    pub row_count: i64,
    pub offset: Option<i64>,
}

/// Ordered Storage Write requests derived from one Arrow IPC stream.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ArrowAppendPlan {
    pub schema: Bytes,
    pub requests: Vec<ArrowAppendRequest>,
}

impl ArrowAppendPlan {
    /// Parse an Arrow IPC stream into its schema message and one [`ArrowAppendRequest`] per record batch.
    pub fn build(stream: &[u8], mode: BigQueryWriteMode) -> Result<Self, BigQueryError> {
        if stream.is_empty() {
            return Ok(Self::default());
        }

        let mut pos = 0usize;
        let mut schema: Option<Bytes> = None;
        let mut requests = Vec::new();
        let mut running_rows = 0i64;

        while pos < stream.len() {
            if stream[pos..].starts_with(&ARROW_IPC_EOS_MARKER) {
                break;
            }
            if stream.len() - pos < 8 {
                return Err(BigQueryError::InvalidArgument(format!(
                    "truncated Arrow IPC message header at byte {pos}"
                )));
            }
            if stream[pos..pos + 4] != [0xFF, 0xFF, 0xFF, 0xFF] {
                return Err(BigQueryError::InvalidArgument(format!(
                    "Arrow IPC message at byte {pos} lacks the 0xFFFFFFFF continuation marker"
                )));
            }
            let meta_size = u32::from_le_bytes([
                stream[pos + 4],
                stream[pos + 5],
                stream[pos + 6],
                stream[pos + 7],
            ]) as usize;
            if meta_size == 0 {
                break;
            }
            let padded_meta = meta_size.checked_add(7).map(|n| n & !7).ok_or_else(|| {
                BigQueryError::InvalidArgument(format!("truncated Arrow IPC message at byte {pos}"))
            })?;
            let meta_start = pos + 8;
            let meta_end = meta_start
                .checked_add(padded_meta)
                .filter(|&end| end <= stream.len())
                .ok_or_else(|| {
                    BigQueryError::InvalidArgument(format!(
                        "truncated Arrow IPC message at byte {pos}"
                    ))
                })?;

            let fb = &stream[meta_start..meta_start + meta_size];
            let header = ArrowMessageHeader::parse(fb)?;
            let padded_body = header
                .body_len
                .checked_add(7)
                .map(|n| n & !7)
                .ok_or_else(|| {
                    BigQueryError::InvalidArgument(format!(
                        "truncated Arrow IPC message at byte {pos}"
                    ))
                })?;
            let frame_end = meta_end
                .checked_add(padded_body)
                .filter(|&end| end <= stream.len())
                .ok_or_else(|| {
                    BigQueryError::InvalidArgument(format!(
                        "truncated Arrow IPC message at byte {pos}"
                    ))
                })?;

            let frame_bytes = Bytes::copy_from_slice(&stream[pos..frame_end]);
            match header.header_type {
                1 => {
                    if schema.is_some() {
                        return Err(BigQueryError::InvalidArgument(
                            "Arrow IPC stream contains more than one Schema message".to_string(),
                        ));
                    }
                    schema = Some(frame_bytes);
                }
                2 => {
                    return Err(BigQueryError::InvalidArgument(
                        "dictionary-encoded Arrow columns are not supported by AppendRows; cast dictionary columns to their value type before appending".to_string(),
                    ));
                }
                3 => {
                    if schema.is_none() {
                        return Err(BigQueryError::InvalidArgument(
                            "Arrow IPC stream must start with a Schema message".to_string(),
                        ));
                    }
                    let index = requests.len();
                    let len = frame_bytes.len();
                    if len > MAX_APPEND_ROWS_CHUNK_BYTES {
                        return Err(BigQueryError::InvalidArgument(format!(
                            "Arrow record batch {index} is {len} bytes; AppendRows accepts at most {MAX_APPEND_ROWS_CHUNK_BYTES} bytes per request; split the batch"
                        )));
                    }
                    let offset = match mode {
                        BigQueryWriteMode::Default => None,
                        BigQueryWriteMode::Committed
                        | BigQueryWriteMode::Pending
                        | BigQueryWriteMode::Buffered => Some(running_rows),
                    };
                    running_rows += header.row_count;
                    requests.push(ArrowAppendRequest {
                        record_batch: frame_bytes,
                        row_count: header.row_count,
                        offset,
                    });
                }
                t => {
                    return Err(BigQueryError::InvalidArgument(format!(
                        "unsupported Arrow IPC message type {t}"
                    )));
                }
            }
            pos = frame_end;
        }

        let Some(schema) = schema else {
            return Ok(Self::default());
        };
        Ok(Self { schema, requests })
    }

    /// Return the total number of rows across all planned record-batch requests.
    pub fn total_rows(&self) -> i64 {
        self.requests.iter().map(|r| r.row_count).sum()
    }

    /// Verify that `AppendRows` returned one response per request with matching offsets.
    pub fn verify_responses(&self, offsets: &[Option<i64>]) -> Result<u64, BigQueryError> {
        let got = offsets.len();
        let expected = self.requests.len();
        if got != expected {
            return Err(BigQueryError::AppendVerification(format!(
                "AppendRows returned {got} responses for {expected} requests"
            )));
        }
        for (i, (req, got_off)) in self.requests.iter().zip(offsets.iter()).enumerate() {
            if let Some(expected_off) = req.offset
                && *got_off != Some(expected_off)
            {
                return Err(BigQueryError::AppendVerification(format!(
                    "AppendRows response {i} reported offset {got_off:?}; expected {expected_off}"
                )));
            }
        }
        Ok(self.total_rows().max(0) as u64)
    }
}

struct ArrowMessageHeader {
    header_type: u8,
    body_len: usize,
    row_count: i64,
}

impl ArrowMessageHeader {
    fn parse(fb: &[u8]) -> Result<Self, BigQueryError> {
        let root_offset = read_u32_le(fb, 0)? as usize;
        let vtable_rel = read_i32_le(fb, root_offset)? as i64;
        let vtable_pos_i64 = (root_offset as i64) - vtable_rel;
        if vtable_pos_i64 < 0 || (vtable_pos_i64 as usize) >= fb.len() {
            return Err(BigQueryError::InvalidArgument(
                "malformed Arrow flatbuffer: vtable offset out of range".to_string(),
            ));
        }
        let vtable_pos = vtable_pos_i64 as usize;
        let vtable_len = read_u16_le(fb, vtable_pos)? as usize;
        if vtable_len < 4
            || vtable_pos
                .checked_add(vtable_len)
                .is_none_or(|end| end > fb.len())
        {
            return Err(BigQueryError::InvalidArgument(
                "malformed Arrow flatbuffer: vtable length out of range".to_string(),
            ));
        }

        let read_vtable_slot = |slot_offset: usize| -> Result<usize, BigQueryError> {
            if slot_offset + 2 > vtable_len {
                return Ok(0);
            }
            Ok(read_u16_le(fb, vtable_pos + slot_offset)? as usize)
        };

        let header_type_off = read_vtable_slot(6)?;
        if header_type_off == 0 {
            return Err(BigQueryError::InvalidArgument(
                "malformed Arrow flatbuffer: header_type slot unset".to_string(),
            ));
        }
        let header_type_pos = root_offset.checked_add(header_type_off).ok_or_else(|| {
            BigQueryError::InvalidArgument(
                "malformed Arrow flatbuffer: header_type offset overflow".to_string(),
            )
        })?;
        let header_type = *fb.get(header_type_pos).ok_or_else(|| {
            BigQueryError::InvalidArgument(format!(
                "malformed Arrow flatbuffer: offset {header_type_pos} out of range"
            ))
        })?;

        let body_len_off = read_vtable_slot(10)?;
        let body_len_i64 = if body_len_off == 0 {
            0
        } else {
            let p = root_offset.checked_add(body_len_off).ok_or_else(|| {
                BigQueryError::InvalidArgument(
                    "malformed Arrow flatbuffer: bodyLength offset overflow".to_string(),
                )
            })?;
            read_i64_le(fb, p)?
        };
        if body_len_i64 < 0 {
            return Err(BigQueryError::InvalidArgument(format!(
                "malformed Arrow flatbuffer: negative bodyLength {body_len_i64}"
            )));
        }
        let body_len = usize::try_from(body_len_i64).map_err(|_| {
            BigQueryError::InvalidArgument(
                "malformed Arrow flatbuffer: bodyLength overflows usize".to_string(),
            )
        })?;

        let mut row_count = 0i64;
        if header_type == 3 {
            let header_off = read_vtable_slot(8)?;
            if header_off > 0 {
                let union_loc = root_offset.checked_add(header_off).ok_or_else(|| {
                    BigQueryError::InvalidArgument(
                        "malformed Arrow flatbuffer: header offset overflow".to_string(),
                    )
                })?;
                let rel = read_u32_le(fb, union_loc)? as i64;
                let rb_table_i64 = (union_loc as i64) + rel;
                if rb_table_i64 < 0 || (rb_table_i64 as usize) >= fb.len() {
                    return Err(BigQueryError::InvalidArgument(
                        "malformed Arrow flatbuffer: RecordBatch table offset out of range"
                            .to_string(),
                    ));
                }
                let rb_table = rb_table_i64 as usize;
                let rb_vtable_rel = read_i32_le(fb, rb_table)? as i64;
                let rb_vtable_i64 = (rb_table as i64) - rb_vtable_rel;
                if rb_vtable_i64 < 0 || (rb_vtable_i64 as usize) >= fb.len() {
                    return Err(BigQueryError::InvalidArgument(
                        "malformed Arrow flatbuffer: RecordBatch vtable offset out of range"
                            .to_string(),
                    ));
                }
                let rb_vtable = rb_vtable_i64 as usize;
                let rb_vtable_len = read_u16_le(fb, rb_vtable)? as usize;
                if rb_vtable_len < 4
                    || rb_vtable
                        .checked_add(rb_vtable_len)
                        .is_none_or(|end| end > fb.len())
                {
                    return Err(BigQueryError::InvalidArgument(
                        "malformed Arrow flatbuffer: RecordBatch vtable length out of range"
                            .to_string(),
                    ));
                }
                if 6 <= rb_vtable_len {
                    let len_off = read_u16_le(fb, rb_vtable + 4)? as usize;
                    if len_off > 0 {
                        let lp = rb_table.checked_add(len_off).ok_or_else(|| {
                            BigQueryError::InvalidArgument(
                                "malformed Arrow flatbuffer: RecordBatch.length offset overflow"
                                    .to_string(),
                            )
                        })?;
                        row_count = read_i64_le(fb, lp)?;
                    }
                }
            }
            if row_count < 0 {
                return Err(BigQueryError::InvalidArgument(format!(
                    "malformed Arrow flatbuffer: negative RecordBatch length {row_count}"
                )));
            }
        }

        Ok(Self {
            header_type,
            body_len,
            row_count,
        })
    }
}

fn read_u16_le(fb: &[u8], at: usize) -> Result<u16, BigQueryError> {
    let slice = fb.get(at..at + 2).ok_or_else(|| {
        BigQueryError::InvalidArgument(format!(
            "malformed Arrow flatbuffer: offset {at} out of range"
        ))
    })?;
    Ok(u16::from_le_bytes([slice[0], slice[1]]))
}

fn read_u32_le(fb: &[u8], at: usize) -> Result<u32, BigQueryError> {
    let slice = fb.get(at..at + 4).ok_or_else(|| {
        BigQueryError::InvalidArgument(format!(
            "malformed Arrow flatbuffer: offset {at} out of range"
        ))
    })?;
    Ok(u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]))
}

fn read_i32_le(fb: &[u8], at: usize) -> Result<i32, BigQueryError> {
    let slice = fb.get(at..at + 4).ok_or_else(|| {
        BigQueryError::InvalidArgument(format!(
            "malformed Arrow flatbuffer: offset {at} out of range"
        ))
    })?;
    Ok(i32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]))
}

fn read_i64_le(fb: &[u8], at: usize) -> Result<i64, BigQueryError> {
    let slice = fb.get(at..at + 8).ok_or_else(|| {
        BigQueryError::InvalidArgument(format!(
            "malformed Arrow flatbuffer: offset {at} out of range"
        ))
    })?;
    Ok(i64::from_le_bytes([
        slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7],
    ]))
}
