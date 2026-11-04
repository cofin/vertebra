//! Google Cloud Spanner v1 engine (`google.spanner.v1.Spanner` &
//! `google.spanner.admin.database.v1.DatabaseAdmin`) backed by the official
//! `google-cloud-spanner` Rust client.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use futures::future::try_join_all;
use google_cloud_longrunning::model::operation::Result as OperationResult;
use google_cloud_spanner::batch::BatchDml;
use google_cloud_spanner::channel_pool::StaticChannelPoolConfig;
use google_cloud_spanner::client::{DatabaseAdmin, DatabaseClient, Spanner, SpannerBuilderExt};
use google_cloud_spanner::key::{Key, KeySet};
use google_cloud_spanner::model as spanner_model;
use google_cloud_spanner::mutation::{Mutation, MutationGroup};
use google_cloud_spanner::omni::TlsConfig;
use google_cloud_spanner::result::ResultSet;
use google_cloud_spanner::statement::Statement;
use google_cloud_spanner::transaction::{ReadWriteTransaction, Timestamp, TimestampBound};
use google_cloud_spanner::types::Type as OfficialType;
use google_cloud_spanner::value::{Kind, Value as OfficialValue};
use tokio::sync::OnceCell;
use url::Url;
use vertebra_core::gcp_auth::GcpTokenCache;

use vertebra_db::gcp::{install_rustls_ring_provider, official_credentials};

/// Default Cloud Spanner gRPC endpoint.
pub const DEFAULT_SPANNER_ENDPOINT: &str = "https://spanner.googleapis.com";

/// Default Cloud Spanner DatabaseAdmin REST endpoint.
pub const DEFAULT_SPANNER_ADMIN_ENDPOINT: &str = "https://spanner.googleapis.com";

/// Default OAuth2 scopes for Cloud Spanner data-plane and platform authentication.
pub const SPANNER_OAUTH_SCOPES: &[&str] = &[
    "https://www.googleapis.com/auth/spanner.data",
    "https://www.googleapis.com/auth/cloud-platform",
];

/// Default deadline (`600 s`) for `UpdateDatabaseDdl` long-running operation polling.
pub const DEFAULT_DDL_TIMEOUT: Duration = Duration::from_secs(600);

/// Maximum mutated cells per `BatchWrite` or `Commit` batch (`80,000`, below Spanner's `100,000` limit).
pub const MAX_MUTATION_CELLS_PER_BATCH: usize = 80_000;

/// Cloud Spanner SQL dialect (`GoogleSQL` or `Spangres`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SpannerDialect {
    #[default]
    GoogleSql,
    Spangres,
}

impl SpannerDialect {
    /// Parse a dialect identifier string (`"googlesql"` or `"spangres"` / `"postgresql"` / `"pg"`).
    pub fn from_str_opt(raw: Option<&str>) -> Self {
        match raw
            .unwrap_or("googlesql")
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "spangres" | "postgresql" | "postgres" | "pg" | "dialect_postgresql" => Self::Spangres,
            _ => Self::GoogleSql,
        }
    }

    /// Canonical lowercase dialect identifier.
    #[inline]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::GoogleSql => "googlesql",
            Self::Spangres => "spangres",
        }
    }
}

/// Configuration for [`SpannerClient`].
#[derive(Clone, Debug)]
pub struct SpannerClientConfig {
    /// Fully-qualified database resource (`projects/{p}/instances/{i}/databases/{d}`).
    pub database: String,
    /// Optional fine-grained access control database role (`Session.creator_role`).
    pub database_role: Option<String>,
    /// Cloud Spanner gRPC endpoint (`https://spanner.googleapis.com` by default).
    pub endpoint: String,
    /// REST endpoint for DDL (`UpdateDatabaseDdl`); derived when absent.
    pub admin_endpoint: Option<String>,
    /// Maximum duration to poll an `UpdateDatabaseDdl` operation before returning [`SpannerError::DdlTimeout`].
    pub ddl_timeout: Duration,
    /// SQL dialect (`GoogleSql` or `Spangres`).
    pub dialect: SpannerDialect,
    /// Number of HTTP/2 gRPC channels in the pool (default `4`).
    pub pool_size: usize,
    /// Whether leader-routing metadata (`x-goog-spanner-route-to-leader: true`) is enabled.
    pub route_to_leader_enabled: bool,
    /// Optional `CommitRequest.max_commit_delay` in milliseconds for write batching throughput.
    pub max_commit_delay_ms: Option<u64>,
    /// Options forwarded to `GcpTokenCache::from_options`.
    pub auth_options: HashMap<String, String>,
    /// Optional custom PEM root certificate bundle for non-googleapis `https://` endpoints.
    pub ca_pem: Option<Vec<u8>>,
}

impl SpannerClientConfig {
    /// Create a new [`SpannerClientConfig`] for `database` (`projects/{p}/instances/{i}/databases/{d}`).
    pub fn new(database: impl Into<String>) -> Self {
        Self {
            database: database.into(),
            database_role: None,
            endpoint: DEFAULT_SPANNER_ENDPOINT.to_string(),
            admin_endpoint: None,
            ddl_timeout: DEFAULT_DDL_TIMEOUT,
            dialect: SpannerDialect::GoogleSql,
            pool_size: 4,
            route_to_leader_enabled: true,
            max_commit_delay_ms: None,
            auth_options: HashMap::new(),
            ca_pem: None,
        }
    }

    /// Format a canonical database resource URI from `(project_id, instance_id, database_id)`.
    pub fn format_database_uri(project_id: &str, instance_id: &str, database_id: &str) -> String {
        format!(
            "projects/{}/instances/{}/databases/{}",
            project_id.trim(),
            instance_id.trim(),
            database_id.trim()
        )
    }

    /// Effective gRPC endpoint URL.
    pub fn effective_endpoint(&self) -> String {
        let trimmed = self.endpoint.trim().trim_end_matches('/');
        if trimmed.is_empty() {
            return DEFAULT_SPANNER_ENDPOINT.to_string();
        }
        if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
            return trimmed.to_string();
        }
        format!("http://{trimmed}")
    }

    /// Return `true` when targeting a cleartext `http://` endpoint.
    pub fn is_cleartext(&self) -> bool {
        self.effective_endpoint().starts_with("http://")
    }
}

/// Errors produced by [`SpannerClient`].
#[derive(Debug, thiserror::Error)]
pub enum SpannerError {
    #[error("Spanner invalid argument: {0}")]
    InvalidArgument(String),
    #[error("Spanner auth error: {0}")]
    Auth(String),
    #[error("Spanner client error: {0}")]
    Client(#[from] google_cloud_spanner::Error),
    #[error("Spanner protocol error: {0}")]
    Protocol(String),
    #[error("UpdateDatabaseDdl operation {operation} failed with code {code}: {message}")]
    Ddl {
        operation: String,
        code: i32,
        message: String,
    },
    #[error(
        "UpdateDatabaseDdl operation {operation} did not finish within {timeout_ms} ms; it may still be running"
    )]
    DdlTimeout { operation: String, timeout_ms: u64 },
}

/// Cloud Spanner `TypeCode` (`google.spanner.v1.TypeCode`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum SpannerTypeCode {
    #[default]
    Unspecified = 0,
    Bool = 1,
    Int64 = 2,
    Float64 = 3,
    Timestamp = 4,
    Date = 5,
    String = 6,
    Bytes = 7,
    Array = 8,
    Struct = 9,
    Numeric = 10,
    Json = 11,
    Proto = 13,
    Enum = 14,
    Float32 = 15,
    Interval = 16,
    Uuid = 17,
}

impl SpannerTypeCode {
    /// Convert a wire `i32` enum value into [`SpannerTypeCode`].
    pub const fn from_i32(v: i32) -> Self {
        match v {
            1 => Self::Bool,
            2 => Self::Int64,
            3 => Self::Float64,
            4 => Self::Timestamp,
            5 => Self::Date,
            6 => Self::String,
            7 => Self::Bytes,
            8 => Self::Array,
            9 => Self::Struct,
            10 => Self::Numeric,
            11 => Self::Json,
            13 => Self::Proto,
            14 => Self::Enum,
            15 => Self::Float32,
            16 => Self::Interval,
            17 => Self::Uuid,
            _ => Self::Unspecified,
        }
    }

    /// Parse a type name string (such as `"INT64"`, `"JSON"`, `"ARRAY"`, `"PG_JSONB"`).
    pub fn from_type_str(s: &str) -> Self {
        match s.trim().to_ascii_uppercase().as_str() {
            "BOOL" | "BOOLEAN" => Self::Bool,
            "INT64" | "INTEGER" | "BIGINT" => Self::Int64,
            "FLOAT64" | "DOUBLE" | "DOUBLE PRECISION" => Self::Float64,
            "FLOAT32" | "REAL" => Self::Float32,
            "TIMESTAMP" | "TIMESTAMPTZ" => Self::Timestamp,
            "DATE" => Self::Date,
            "STRING" | "VARCHAR" | "TEXT" => Self::String,
            "BYTES" | "BYTEA" => Self::Bytes,
            "ARRAY" => Self::Array,
            "STRUCT" => Self::Struct,
            "NUMERIC" | "DECIMAL" | "PG_NUMERIC" => Self::Numeric,
            "JSON" | "JSONB" | "PG_JSONB" => Self::Json,
            "PROTO" => Self::Proto,
            "ENUM" => Self::Enum,
            "INTERVAL" => Self::Interval,
            "UUID" => Self::Uuid,
            _ => Self::String,
        }
    }

    /// Canonical uppercase type code name.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Unspecified => "TYPE_CODE_UNSPECIFIED",
            Self::Bool => "BOOL",
            Self::Int64 => "INT64",
            Self::Float64 => "FLOAT64",
            Self::Timestamp => "TIMESTAMP",
            Self::Date => "DATE",
            Self::String => "STRING",
            Self::Bytes => "BYTES",
            Self::Array => "ARRAY",
            Self::Struct => "STRUCT",
            Self::Numeric => "NUMERIC",
            Self::Json => "JSON",
            Self::Proto => "PROTO",
            Self::Enum => "ENUM",
            Self::Float32 => "FLOAT32",
            Self::Interval => "INTERVAL",
            Self::Uuid => "UUID",
        }
    }
}

/// Cloud Spanner `TypeAnnotationCode` (`google.spanner.v1.TypeAnnotationCode`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum SpannerTypeAnnotation {
    #[default]
    Unspecified = 0,
    PgNumeric = 2,
    PgJsonb = 3,
    PgOid = 4,
}

impl SpannerTypeAnnotation {
    /// Convert a wire `i32` enum value into [`SpannerTypeAnnotation`].
    pub const fn from_i32(v: i32) -> Self {
        match v {
            2 => Self::PgNumeric,
            3 => Self::PgJsonb,
            4 => Self::PgOid,
            _ => Self::Unspecified,
        }
    }
}

/// Field descriptor inside a `StructType` (`google.spanner.v1.StructType.Field`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpannerStructField {
    pub name: String,
    pub field_type: SpannerType,
}

/// Cloud Spanner `Type` descriptor (`google.spanner.v1.Type`).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct SpannerType {
    pub code: SpannerTypeCode,
    pub array_element_type: Option<Box<SpannerType>>,
    pub struct_fields: Vec<SpannerStructField>,
    pub type_annotation: SpannerTypeAnnotation,
    pub proto_type_fqn: Option<String>,
}

impl SpannerType {
    /// Construct a scalar [`SpannerType`].
    pub fn scalar(code: SpannerTypeCode) -> Self {
        Self {
            code,
            array_element_type: None,
            struct_fields: Vec::new(),
            type_annotation: SpannerTypeAnnotation::Unspecified,
            proto_type_fqn: None,
        }
    }

    /// Construct an `ARRAY<T>` [`SpannerType`].
    pub fn array(elem: SpannerType) -> Self {
        Self {
            code: SpannerTypeCode::Array,
            array_element_type: Some(Box::new(elem)),
            struct_fields: Vec::new(),
            type_annotation: SpannerTypeAnnotation::Unspecified,
            proto_type_fqn: None,
        }
    }

    /// Construct a `STRUCT<...>` [`SpannerType`].
    pub fn struct_type(fields: Vec<SpannerStructField>) -> Self {
        Self {
            code: SpannerTypeCode::Struct,
            array_element_type: None,
            struct_fields: fields,
            type_annotation: SpannerTypeAnnotation::Unspecified,
            proto_type_fqn: None,
        }
    }
}

/// Wire representation of `google.protobuf.Value` used inside Spanner query parameters,
/// results, and `Mutation` key/value lists.
#[derive(Clone, Debug, PartialEq, Default)]
pub enum ProtobufValue {
    #[default]
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Struct(Vec<(String, ProtobufValue)>),
    List(Vec<ProtobufValue>),
}

/// Decoded Cloud Spanner cell value hydrated from official `ResultSet` rows.
#[derive(Clone, Debug, PartialEq)]
pub enum SpannerValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Json(String),
    Numeric(String),
    Timestamp(String),
    Date(String),
    Uuid(String),
    Array(Vec<SpannerValue>),
    Struct(Vec<(String, SpannerValue)>),
}

impl SpannerValue {
    /// Decode a [`ProtobufValue`] using its [`SpannerType`] schema.
    pub fn from_protobuf_value(
        raw: &ProtobufValue,
        col_type: &SpannerType,
    ) -> Result<Self, SpannerError> {
        if matches!(raw, ProtobufValue::Null) {
            return Ok(Self::Null);
        }
        match col_type.code {
            SpannerTypeCode::Bool => match raw {
                ProtobufValue::Bool(b) => Ok(Self::Bool(*b)),
                ProtobufValue::String(s) => Ok(Self::Bool(s.eq_ignore_ascii_case("true"))),
                other => Err(SpannerError::Protocol(format!(
                    "expected BOOL value, got {other:?}"
                ))),
            },
            SpannerTypeCode::Int64 | SpannerTypeCode::Enum => match raw {
                ProtobufValue::String(s) => s
                    .parse::<i64>()
                    .map(Self::Int64)
                    .map_err(|e| SpannerError::Protocol(format!("invalid INT64 {s:?}: {e}"))),
                ProtobufValue::Number(n) => Ok(Self::Int64(*n as i64)),
                other => Err(SpannerError::Protocol(format!(
                    "expected INT64 StringValue, got {other:?}"
                ))),
            },
            SpannerTypeCode::Float64 | SpannerTypeCode::Float32 => match raw {
                ProtobufValue::Number(n) => Ok(Self::Float64(*n)),
                ProtobufValue::String(s) => {
                    let parsed = match s.as_str() {
                        "Infinity" | "+Infinity" | "inf" => f64::INFINITY,
                        "-Infinity" | "-inf" => f64::NEG_INFINITY,
                        "NaN" | "nan" => f64::NAN,
                        other => other.parse::<f64>().map_err(|e| {
                            SpannerError::Protocol(format!("invalid FLOAT {other:?}: {e}"))
                        })?,
                    };
                    Ok(Self::Float64(parsed))
                }
                other => Err(SpannerError::Protocol(format!(
                    "expected FLOAT NumberValue/StringValue, got {other:?}"
                ))),
            },
            SpannerTypeCode::Bytes | SpannerTypeCode::Proto => match raw {
                ProtobufValue::String(b64) => BASE64_STANDARD
                    .decode(b64.as_bytes())
                    .map(Self::Bytes)
                    .map_err(|e| SpannerError::Protocol(format!("invalid base64 BYTES: {e}"))),
                other => Err(SpannerError::Protocol(format!(
                    "expected base64 StringValue for BYTES, got {other:?}"
                ))),
            },
            SpannerTypeCode::Json => match raw {
                ProtobufValue::String(s) => Ok(Self::Json(s.clone())),
                other => Err(SpannerError::Protocol(format!(
                    "expected StringValue for JSON, got {other:?}"
                ))),
            },
            SpannerTypeCode::Numeric => match raw {
                ProtobufValue::String(s) => Ok(Self::Numeric(s.clone())),
                ProtobufValue::Number(n) => Ok(Self::Numeric(n.to_string())),
                other => Err(SpannerError::Protocol(format!(
                    "expected StringValue for NUMERIC, got {other:?}"
                ))),
            },
            SpannerTypeCode::Timestamp => match raw {
                ProtobufValue::String(s) => Ok(Self::Timestamp(s.clone())),
                other => Err(SpannerError::Protocol(format!(
                    "expected StringValue for TIMESTAMP, got {other:?}"
                ))),
            },
            SpannerTypeCode::Date => match raw {
                ProtobufValue::String(s) => Ok(Self::Date(s.clone())),
                other => Err(SpannerError::Protocol(format!(
                    "expected StringValue for DATE, got {other:?}"
                ))),
            },
            SpannerTypeCode::Uuid => match raw {
                ProtobufValue::String(s) => Ok(Self::Uuid(s.clone())),
                other => Err(SpannerError::Protocol(format!(
                    "expected StringValue for UUID, got {other:?}"
                ))),
            },
            SpannerTypeCode::Array => match raw {
                ProtobufValue::List(items) => {
                    let elem_type = col_type
                        .array_element_type
                        .as_deref()
                        .cloned()
                        .unwrap_or_else(|| SpannerType::scalar(SpannerTypeCode::String));
                    let mut out = Vec::with_capacity(items.len());
                    for item in items {
                        out.push(Self::from_protobuf_value(item, &elem_type)?);
                    }
                    Ok(Self::Array(out))
                }
                other => Err(SpannerError::Protocol(format!(
                    "expected ListValue for ARRAY, got {other:?}"
                ))),
            },
            SpannerTypeCode::Struct => match raw {
                ProtobufValue::List(items) => {
                    let mut fields = Vec::with_capacity(items.len());
                    for (idx, item) in items.iter().enumerate() {
                        let (fname, ftype) = col_type
                            .struct_fields
                            .get(idx)
                            .map(|f| (f.name.clone(), f.field_type.clone()))
                            .unwrap_or_else(|| {
                                (
                                    format!("field_{idx}"),
                                    SpannerType::scalar(SpannerTypeCode::String),
                                )
                            });
                        fields.push((fname, Self::from_protobuf_value(item, &ftype)?));
                    }
                    Ok(Self::Struct(fields))
                }
                ProtobufValue::Struct(entries) => {
                    let mut fields = Vec::with_capacity(entries.len());
                    for (k, v) in entries {
                        let ftype = col_type
                            .struct_fields
                            .iter()
                            .find(|f| f.name == *k)
                            .map(|f| f.field_type.clone())
                            .unwrap_or_else(|| SpannerType::scalar(SpannerTypeCode::String));
                        fields.push((k.clone(), Self::from_protobuf_value(v, &ftype)?));
                    }
                    Ok(Self::Struct(fields))
                }
                other => Err(SpannerError::Protocol(format!(
                    "expected ListValue/StructValue for STRUCT, got {other:?}"
                ))),
            },
            SpannerTypeCode::String | SpannerTypeCode::Interval | SpannerTypeCode::Unspecified => {
                match raw {
                    ProtobufValue::String(s) => Ok(Self::String(s.clone())),
                    ProtobufValue::Bool(b) => Ok(Self::Bool(*b)),
                    ProtobufValue::Number(n) => Ok(Self::Float64(*n)),
                    ProtobufValue::List(items) => {
                        let elem = SpannerType::scalar(SpannerTypeCode::String);
                        let mut out = Vec::with_capacity(items.len());
                        for item in items {
                            out.push(Self::from_protobuf_value(item, &elem)?);
                        }
                        Ok(Self::Array(out))
                    }
                    ProtobufValue::Struct(entries) => {
                        let elem = SpannerType::scalar(SpannerTypeCode::String);
                        let mut out = Vec::with_capacity(entries.len());
                        for (k, v) in entries {
                            out.push((k.clone(), Self::from_protobuf_value(v, &elem)?));
                        }
                        Ok(Self::Struct(out))
                    }
                    ProtobufValue::Null => Ok(Self::Null),
                }
            }
        }
    }

    /// Convert this [`SpannerValue`] into its wire [`ProtobufValue`] and [`SpannerType`].
    pub fn to_protobuf_and_type(&self) -> (ProtobufValue, SpannerType) {
        match self {
            Self::Null => (
                ProtobufValue::Null,
                SpannerType::scalar(SpannerTypeCode::String),
            ),
            Self::Bool(b) => (
                ProtobufValue::Bool(*b),
                SpannerType::scalar(SpannerTypeCode::Bool),
            ),
            Self::Int64(n) => (
                ProtobufValue::String(n.to_string()),
                SpannerType::scalar(SpannerTypeCode::Int64),
            ),
            Self::Float64(f) => (
                ProtobufValue::Number(*f),
                SpannerType::scalar(SpannerTypeCode::Float64),
            ),
            Self::String(s) => (
                ProtobufValue::String(s.clone()),
                SpannerType::scalar(SpannerTypeCode::String),
            ),
            Self::Bytes(b) => (
                ProtobufValue::String(BASE64_STANDARD.encode(b)),
                SpannerType::scalar(SpannerTypeCode::Bytes),
            ),
            Self::Json(j) => (
                ProtobufValue::String(j.clone()),
                SpannerType::scalar(SpannerTypeCode::Json),
            ),
            Self::Numeric(n) => (
                ProtobufValue::String(n.clone()),
                SpannerType::scalar(SpannerTypeCode::Numeric),
            ),
            Self::Timestamp(ts) => (
                ProtobufValue::String(ts.clone()),
                SpannerType::scalar(SpannerTypeCode::Timestamp),
            ),
            Self::Date(d) => (
                ProtobufValue::String(d.clone()),
                SpannerType::scalar(SpannerTypeCode::Date),
            ),
            Self::Uuid(u) => (
                ProtobufValue::String(u.clone()),
                SpannerType::scalar(SpannerTypeCode::Uuid),
            ),
            Self::Array(items) => {
                let mut pb_items = Vec::with_capacity(items.len());
                let mut elem_type = SpannerType::scalar(SpannerTypeCode::String);
                for (i, item) in items.iter().enumerate() {
                    let (pb, ty) = item.to_protobuf_and_type();
                    if i == 0 || !matches!(item, Self::Null) {
                        elem_type = ty;
                    }
                    pb_items.push(pb);
                }
                (ProtobufValue::List(pb_items), SpannerType::array(elem_type))
            }
            Self::Struct(fields) => {
                let mut pb_items = Vec::with_capacity(fields.len());
                let mut st_fields = Vec::with_capacity(fields.len());
                for (k, v) in fields {
                    let (pb, ty) = v.to_protobuf_and_type();
                    pb_items.push(pb);
                    st_fields.push(SpannerStructField {
                        name: k.clone(),
                        field_type: ty,
                    });
                }
                (
                    ProtobufValue::List(pb_items),
                    SpannerType::struct_type(st_fields),
                )
            }
        }
    }

    pub fn to_serde_json(&self) -> serde_json::Value {
        match self {
            Self::Null => serde_json::Value::Null,
            Self::Bool(b) => serde_json::Value::Bool(*b),
            Self::Int64(n) => serde_json::json!(*n),
            Self::Float64(f) => serde_json::json!(*f),
            Self::String(s)
            | Self::Json(s)
            | Self::Numeric(s)
            | Self::Timestamp(s)
            | Self::Date(s)
            | Self::Uuid(s) => serde_json::Value::String(s.clone()),
            Self::Bytes(b) => serde_json::Value::String(BASE64_STANDARD.encode(b)),
            Self::Array(items) => {
                serde_json::Value::Array(items.iter().map(Self::to_serde_json).collect())
            }
            Self::Struct(fields) => {
                let mut map = serde_json::Map::with_capacity(fields.len());
                for (k, v) in fields {
                    map.insert(k.clone(), v.to_serde_json());
                }
                serde_json::Value::Object(map)
            }
        }
    }
}

/// Named query parameter (`name`, `ProtobufValue`, `SpannerType`) bound in `ExecuteSqlRequest`.
#[derive(Clone, Debug, PartialEq)]
pub struct SpannerQueryParameter {
    pub name: String,
    pub value: ProtobufValue,
    pub param_type: SpannerType,
}

impl SpannerQueryParameter {
    /// Construct a typed [`SpannerQueryParameter`] from a [`SpannerValue`].
    pub fn from_value(name: impl Into<String>, val: &SpannerValue) -> Self {
        let (value, param_type) = val.to_protobuf_and_type();
        Self {
            name: name.into(),
            value,
            param_type,
        }
    }
}

fn parse_typed_spanner_param(name: String, item: &serde_json::Value) -> SpannerQueryParameter {
    let type_str = item
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("STRING");
    let code = SpannerTypeCode::from_type_str(type_str);
    let elem_type_str = item.get("array_type").and_then(|v| v.as_str());
    let param_type = if code == SpannerTypeCode::Array {
        let elem_code = SpannerTypeCode::from_type_str(elem_type_str.unwrap_or("STRING"));
        SpannerType::array(SpannerType::scalar(elem_code))
    } else {
        let mut ty = SpannerType::scalar(code);
        if type_str.eq_ignore_ascii_case("PG_JSONB") {
            ty.type_annotation = SpannerTypeAnnotation::PgJsonb;
        } else if type_str.eq_ignore_ascii_case("PG_NUMERIC") {
            ty.type_annotation = SpannerTypeAnnotation::PgNumeric;
        }
        ty
    };
    let value = json_to_protobuf_value(item.get("value").unwrap_or(&serde_json::Value::Null), code);
    SpannerQueryParameter {
        name,
        value,
        param_type,
    }
}

fn inferred_json_to_spanner_value(val: &serde_json::Value) -> SpannerValue {
    match val {
        serde_json::Value::Null => SpannerValue::Null,
        serde_json::Value::Bool(b) => SpannerValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                SpannerValue::Int64(i)
            } else {
                SpannerValue::Float64(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => SpannerValue::String(s.clone()),
        serde_json::Value::Array(items) => {
            SpannerValue::Array(items.iter().map(inferred_json_to_spanner_value).collect())
        }
        serde_json::Value::Object(map) => {
            SpannerValue::Json(serde_json::Value::Object(map.clone()).to_string())
        }
    }
}

/// Parse a JSON-serialized array (`[{"name": "p1", "type": "INT64", "value": "42"}, ...]`) or
/// object (`{"p1": 42, "p2": {"type": "INT64", "value": null}}`) of query parameters into
/// [`SpannerQueryParameter`] items.
pub fn parse_spanner_parameters_json(
    params_json: Option<&str>,
) -> Result<Vec<SpannerQueryParameter>, SpannerError> {
    let Some(raw) = params_json.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(Vec::new());
    };
    let parsed: serde_json::Value = serde_json::from_str(raw)
        .map_err(|e| SpannerError::InvalidArgument(format!("invalid params_json: {e}")))?;
    match &parsed {
        serde_json::Value::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for item in arr {
                let name = item
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                out.push(parse_typed_spanner_param(name, item));
            }
            Ok(out)
        }
        serde_json::Value::Object(map) => {
            let mut out = Vec::with_capacity(map.len());
            for (key, entry) in map {
                let name = key.trim_start_matches(['@', '$']).to_string();
                if entry.is_object() && entry.get("type").and_then(|v| v.as_str()).is_some() {
                    out.push(parse_typed_spanner_param(name, entry));
                } else {
                    let sv = inferred_json_to_spanner_value(entry);
                    out.push(SpannerQueryParameter::from_value(name, &sv));
                }
            }
            Ok(out)
        }
        _ => Err(SpannerError::InvalidArgument(
            "params_json must be a JSON array or object".to_string(),
        )),
    }
}

fn json_to_protobuf_value(val: &serde_json::Value, code: SpannerTypeCode) -> ProtobufValue {
    if val.is_null() {
        return ProtobufValue::Null;
    }
    if code == SpannerTypeCode::Json {
        return match val {
            serde_json::Value::String(s) => ProtobufValue::String(s.clone()),
            other => ProtobufValue::String(other.to_string()),
        };
    }
    match val {
        serde_json::Value::Null => ProtobufValue::Null,
        serde_json::Value::Bool(b) => ProtobufValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if matches!(
                code,
                SpannerTypeCode::Int64 | SpannerTypeCode::Numeric | SpannerTypeCode::Enum
            ) {
                ProtobufValue::String(n.to_string())
            } else {
                ProtobufValue::Number(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => ProtobufValue::String(s.clone()),
        serde_json::Value::Array(items) => ProtobufValue::List(
            items
                .iter()
                .map(|elem| json_to_protobuf_value(elem, SpannerTypeCode::String))
                .collect(),
        ),
        serde_json::Value::Object(map) => ProtobufValue::Struct(
            map.iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        json_to_protobuf_value(v, SpannerTypeCode::String),
                    )
                })
                .collect(),
        ),
    }
}

/// Rewrite Spangres-style `$1..$N` positional placeholders into GoogleSQL `@p1..@pN`
/// outside GoogleSQL string/bytes literals (including triple-quoted and raw forms),
/// backtick identifiers, and `--`, `#`, and `/* */` comments, returning
/// `(rewritten_sql, ["p1", .., "pN"])`.
pub fn rewrite_spangres_placeholders(sql: &str) -> (String, Vec<String>) {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut out = String::with_capacity(len + 16);
    let mut max_ordinal: usize = 0;
    let mut i = 0;

    while i < len {
        let b = bytes[i];
        if (b == b'\'' || b == b'"') && i + 2 < len && bytes[i + 1] == b && bytes[i + 2] == b {
            let end = skip_triple_quoted(sql, i, b);
            out.push_str(&sql[i..end]);
            i = end;
            continue;
        }
        if b == b'\'' || b == b'"' || b == b'`' {
            let end = skip_quoted(sql, i, b);
            out.push_str(&sql[i..end]);
            i = end;
            continue;
        }
        if b == b'#' || (b == b'-' && i + 1 < len && bytes[i + 1] == b'-') {
            let end = skip_line_comment(sql, i);
            out.push_str(&sql[i..end]);
            i = end;
            continue;
        }
        if b == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
            let end = skip_block_comment(sql, i);
            out.push_str(&sql[i..end]);
            i = end;
            continue;
        }
        if b == b'$' && i + 1 < len && bytes[i + 1].is_ascii_digit() && bytes[i + 1] != b'0' {
            let start = i + 1;
            let mut end = start;
            while end < len && bytes[end].is_ascii_digit() {
                end += 1;
            }
            if let Ok(ord) = sql[start..end].parse::<usize>() {
                if ord > max_ordinal {
                    max_ordinal = ord;
                }
                out.push_str(&format!("@p{ord}"));
                i = end;
                continue;
            }
        }
        let ch = sql[i..].chars().next().unwrap_or('\0');
        out.push(ch);
        i += ch.len_utf8().max(1);
    }

    let names = (1..=max_ordinal).map(|n| format!("p{n}")).collect();
    (out, names)
}

fn skip_quoted(sql: &str, start: usize, quote: u8) -> usize {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut i = start + 1;
    while i < len {
        if bytes[i] == b'\\' {
            i += 1;
            if i < len {
                let ch = sql[i..].chars().next().unwrap_or('\0');
                i += ch.len_utf8().max(1);
            }
            continue;
        }
        if bytes[i] == quote {
            return i + 1;
        }
        let ch = sql[i..].chars().next().unwrap_or('\0');
        i += ch.len_utf8().max(1);
    }
    len
}

fn skip_triple_quoted(sql: &str, start: usize, quote: u8) -> usize {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut i = start + 3;
    while i < len {
        if bytes[i] == b'\\' {
            i += 1;
            if i < len {
                let ch = sql[i..].chars().next().unwrap_or('\0');
                i += ch.len_utf8().max(1);
            }
            continue;
        }
        if i + 2 < len && bytes[i] == quote && bytes[i + 1] == quote && bytes[i + 2] == quote {
            return i + 3;
        }
        let ch = sql[i..].chars().next().unwrap_or('\0');
        i += ch.len_utf8().max(1);
    }
    len
}

fn skip_line_comment(sql: &str, start: usize) -> usize {
    let bytes = sql.as_bytes();
    let mut i = start;
    while i < bytes.len() && bytes[i] != b'\n' {
        i += 1;
    }
    i
}

fn skip_block_comment(sql: &str, start: usize) -> usize {
    let bytes = sql.as_bytes();
    let mut i = start + 2;
    while i + 1 < bytes.len() {
        if bytes[i] == b'*' && bytes[i + 1] == b'/' {
            return i + 2;
        }
        i += 1;
    }
    bytes.len()
}

/// Read-only staleness bound (`google.spanner.v1.TransactionOptions.ReadOnly`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SpannerStaleness {
    #[default]
    Strong,
    ExactStalenessMs(u64),
    MaxStalenessMs(u64),
    ReadTimestampUnixMicros(i64),
    MinReadTimestampUnixMicros(i64),
}

/// Write or delete mutation (`google.spanner.v1.Mutation`).
#[derive(Clone, Debug, PartialEq)]
pub enum SpannerMutation {
    Insert {
        table: String,
        columns: Vec<String>,
        values: Vec<Vec<ProtobufValue>>,
    },
    Update {
        table: String,
        columns: Vec<String>,
        values: Vec<Vec<ProtobufValue>>,
    },
    InsertOrUpdate {
        table: String,
        columns: Vec<String>,
        values: Vec<Vec<ProtobufValue>>,
    },
    Replace {
        table: String,
        columns: Vec<String>,
        values: Vec<Vec<ProtobufValue>>,
    },
    Delete {
        table: String,
        keys: Vec<Vec<ProtobufValue>>,
        all: bool,
    },
}

impl SpannerMutation {
    /// Construct an `INSERT` mutation.
    pub fn insert(
        table: impl Into<String>,
        columns: Vec<String>,
        values: Vec<Vec<ProtobufValue>>,
    ) -> Self {
        Self::Insert {
            table: table.into(),
            columns,
            values,
        }
    }

    /// Construct an `UPDATE` mutation.
    pub fn update(
        table: impl Into<String>,
        columns: Vec<String>,
        values: Vec<Vec<ProtobufValue>>,
    ) -> Self {
        Self::Update {
            table: table.into(),
            columns,
            values,
        }
    }

    /// Construct an `INSERT OR UPDATE` (upsert) mutation.
    pub fn insert_or_update(
        table: impl Into<String>,
        columns: Vec<String>,
        values: Vec<Vec<ProtobufValue>>,
    ) -> Self {
        Self::InsertOrUpdate {
            table: table.into(),
            columns,
            values,
        }
    }

    /// Construct a `REPLACE` mutation.
    pub fn replace(
        table: impl Into<String>,
        columns: Vec<String>,
        values: Vec<Vec<ProtobufValue>>,
    ) -> Self {
        Self::Replace {
            table: table.into(),
            columns,
            values,
        }
    }

    /// Construct a `DELETE` mutation for specific primary-key tuples or `all = true`.
    pub fn delete(table: impl Into<String>, keys: Vec<Vec<ProtobufValue>>, all: bool) -> Self {
        Self::Delete {
            table: table.into(),
            keys,
            all,
        }
    }

    /// Estimate the number of mutated cells for Spanner's `80,000` cell limit accounting.
    pub fn cell_count(&self) -> usize {
        match self {
            Self::Insert {
                columns, values, ..
            }
            | Self::Update {
                columns, values, ..
            }
            | Self::InsertOrUpdate {
                columns, values, ..
            }
            | Self::Replace {
                columns, values, ..
            } => columns.len().max(1).saturating_mul(values.len().max(1)),
            Self::Delete { keys, .. } => keys.len().max(1),
        }
    }
}

/// Group of mutations executed atomically inside a `BatchWrite` RPC (`google.spanner.v1.BatchWriteRequest.MutationGroup`).
#[derive(Clone, Debug, PartialEq)]
pub struct SpannerMutationGroup {
    pub mutations: Vec<SpannerMutation>,
}

impl SpannerMutationGroup {
    pub fn new(mutations: Vec<SpannerMutation>) -> Self {
        Self { mutations }
    }

    pub fn cell_count(&self) -> usize {
        self.mutations.iter().map(SpannerMutation::cell_count).sum()
    }
}

/// Complete decoded result set returned by `query` and `partitioned_query`.
#[derive(Clone, Debug, Default)]
pub struct SpannerQueryResult {
    pub columns: Vec<String>,
    pub schema: Vec<SpannerStructField>,
    pub rows: Vec<Vec<SpannerValue>>,
    pub row_count_exact: Option<i64>,
    pub row_count_lower_bound: Option<i64>,
}

/// Statement item inside `ExecuteBatchDmlRequest`.
#[derive(Clone, Debug)]
pub struct SpannerBatchDmlStatement {
    pub sql: String,
    pub params: Vec<SpannerQueryParameter>,
}

/// Result of `Commit`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpannerCommitResult {
    pub commit_timestamp_micros: Option<i64>,
    pub mutation_count: Option<i64>,
    pub retry_count: u32,
}

/// Result entry yielded by `BatchWrite`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpannerBatchWriteResult {
    pub indexes: Vec<i32>,
    pub status_code: i32,
    pub status_message: String,
    pub commit_timestamp_micros: Option<i64>,
}

fn endpoint_is_googleapis(endpoint: &str) -> bool {
    Url::parse(endpoint)
        .ok()
        .and_then(|u| {
            u.host_str()
                .map(|h| h == "googleapis.com" || h.ends_with(".googleapis.com"))
        })
        .unwrap_or(false)
}

fn validate_spanner_config(config: &SpannerClientConfig) -> Result<(), SpannerError> {
    if config.database.trim().is_empty() {
        return Err(SpannerError::InvalidArgument(
            "SpannerClientConfig.database must not be empty".to_string(),
        ));
    }
    let endpoint = config.effective_endpoint();
    if config.ca_pem.is_some() {
        if endpoint_is_googleapis(&endpoint) {
            return Err(SpannerError::InvalidArgument(
                "SpannerClientConfig.ca_pem applies only to non-googleapis endpoints (Spanner Omni or private test servers); Cloud Spanner uses the system trust store".to_string(),
            ));
        }
        if Url::parse(&endpoint)
            .ok()
            .is_some_and(|u| u.scheme() == "http")
        {
            return Err(SpannerError::InvalidArgument(
                "ca_pem requires an https:// endpoint".to_string(),
            ));
        }
    }
    Ok(())
}

fn wkt_timestamp(us: i64) -> Result<Timestamp, SpannerError> {
    Timestamp::new(
        us.div_euclid(1_000_000),
        (us.rem_euclid(1_000_000) * 1_000) as i32,
    )
    .map_err(|e| SpannerError::InvalidArgument(e.to_string()))
}

fn timestamp_bound(staleness: SpannerStaleness) -> Result<TimestampBound, SpannerError> {
    match staleness {
        SpannerStaleness::Strong => Ok(TimestampBound::strong()),
        SpannerStaleness::ExactStalenessMs(ms) => {
            Ok(TimestampBound::exact_staleness(Duration::from_millis(ms)))
        }
        SpannerStaleness::MaxStalenessMs(ms) => {
            Ok(TimestampBound::max_staleness(Duration::from_millis(ms)))
        }
        SpannerStaleness::ReadTimestampUnixMicros(us) => {
            Ok(TimestampBound::read_timestamp(wkt_timestamp(us)?))
        }
        SpannerStaleness::MinReadTimestampUnixMicros(us) => {
            Ok(TimestampBound::min_read_timestamp(wkt_timestamp(us)?))
        }
    }
}

fn official_value(value: &ProtobufValue) -> OfficialValue {
    match value {
        ProtobufValue::Null => OfficialValue::null(),
        ProtobufValue::Bool(b) => (*b).into(),
        ProtobufValue::Number(n) => (*n).into(),
        ProtobufValue::String(s) => s.clone().into(),
        ProtobufValue::List(items) => items
            .iter()
            .map(official_value)
            .collect::<Vec<OfficialValue>>()
            .into(),
        ProtobufValue::Struct(entries) => entries
            .iter()
            .map(|(_, v)| official_value(v))
            .collect::<Vec<OfficialValue>>()
            .into(),
    }
}

fn official_type(ty: &SpannerType) -> OfficialType {
    let mut model = spanner_model::Type::new()
        .set_code(spanner_model::TypeCode::from(ty.code as i32))
        .set_type_annotation(spanner_model::TypeAnnotationCode::from(
            ty.type_annotation as i32,
        ));
    if let Some(ref elem) = ty.array_element_type {
        model = model.set_array_element_type(spanner_model::Type::from(official_type(elem)));
    }
    if !ty.struct_fields.is_empty() {
        let fields = ty
            .struct_fields
            .iter()
            .map(|f| {
                spanner_model::struct_type::Field::new()
                    .set_name(&f.name)
                    .set_type(spanner_model::Type::from(official_type(&f.field_type)))
            })
            .collect::<Vec<_>>();
        model = model.set_struct_type(spanner_model::StructType::new().set_fields(fields));
    }
    if let Some(ref fqn) = ty.proto_type_fqn {
        model = model.set_proto_type_fqn(fqn.clone());
    }
    OfficialType::from(model)
}

fn protobuf_value(value: &OfficialValue) -> Result<ProtobufValue, SpannerError> {
    match value.kind() {
        Kind::Null => Ok(ProtobufValue::Null),
        Kind::Bool => value
            .as_bool()
            .map(ProtobufValue::Bool)
            .ok_or_else(|| SpannerError::Protocol("unsupported Spanner value kind".into())),
        Kind::Number => value
            .as_f64()
            .map(ProtobufValue::Number)
            .ok_or_else(|| SpannerError::Protocol("unsupported Spanner value kind".into())),
        Kind::String => value
            .as_str()
            .map(|s| ProtobufValue::String(s.to_string()))
            .ok_or_else(|| SpannerError::Protocol("unsupported Spanner value kind".into())),
        Kind::List => {
            let items = value
                .as_list()
                .ok_or_else(|| SpannerError::Protocol("unsupported Spanner value kind".into()))?;
            let mut out = Vec::with_capacity(items.len());
            for item in items.iter() {
                out.push(protobuf_value(item)?);
            }
            Ok(ProtobufValue::List(out))
        }
        _ => Err(SpannerError::Protocol(
            "unsupported Spanner value kind".into(),
        )),
    }
}

fn spanner_type(ty: &OfficialType) -> SpannerType {
    spanner_type_from_model(&spanner_model::Type::from(ty.clone()))
}

fn spanner_type_from_model(model: &spanner_model::Type) -> SpannerType {
    let code = SpannerTypeCode::from_i32(model.code.value().unwrap_or(0));
    let type_annotation =
        SpannerTypeAnnotation::from_i32(model.type_annotation.value().unwrap_or(0));
    let array_element_type = model
        .array_element_type
        .as_deref()
        .map(|elem| Box::new(spanner_type_from_model(elem)));
    let struct_fields = model
        .struct_type
        .as_ref()
        .map(|st| {
            st.fields
                .iter()
                .map(|f| SpannerStructField {
                    name: f.name.clone(),
                    field_type: f
                        .r#type
                        .as_deref()
                        .map(spanner_type_from_model)
                        .unwrap_or_default(),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let proto_type_fqn = if model.proto_type_fqn.is_empty() {
        None
    } else {
        Some(model.proto_type_fqn.clone())
    };
    SpannerType {
        code,
        array_element_type,
        struct_fields,
        type_annotation,
        proto_type_fqn,
    }
}

async fn collect_query_result(mut rs: ResultSet) -> Result<SpannerQueryResult, SpannerError> {
    let mut raw_rows = Vec::new();
    while let Some(row) = rs.next().await {
        raw_rows.push(row?.into_values());
    }
    let meta = rs.metadata();
    let columns = meta.column_names().to_vec();
    let schema = columns
        .iter()
        .zip(meta.column_types().iter())
        .map(|(name, ty)| SpannerStructField {
            name: name.clone(),
            field_type: spanner_type(ty),
        })
        .collect::<Vec<_>>();
    let mut rows = Vec::with_capacity(raw_rows.len());
    for raw_row in &raw_rows {
        let mut decoded_row = Vec::with_capacity(raw_row.len());
        for (idx, cell) in raw_row.iter().enumerate() {
            let pb = protobuf_value(cell)?;
            let field_ty = schema
                .get(idx)
                .map(|f| &f.field_type)
                .cloned()
                .unwrap_or_else(|| SpannerType::scalar(SpannerTypeCode::String));
            decoded_row.push(SpannerValue::from_protobuf_value(&pb, &field_ty)?);
        }
        rows.push(decoded_row);
    }
    let row_count_exact = rs.stats().and_then(|s| s.row_count_exact().copied());
    let row_count_lower_bound = rs.stats().and_then(|s| s.row_count_lower_bound().copied());
    Ok(SpannerQueryResult {
        columns,
        schema,
        rows,
        row_count_exact,
        row_count_lower_bound,
    })
}

fn official_mutation_rows(mutation: &SpannerMutation) -> Vec<Mutation> {
    match mutation {
        SpannerMutation::Insert {
            table,
            columns,
            values,
        } => values
            .iter()
            .map(|row| {
                let mut builder = Mutation::new_insert_builder(table);
                for (col, cell) in columns.iter().zip(row.iter()) {
                    builder = builder.set(col).to(official_value(cell));
                }
                builder.build()
            })
            .collect(),
        SpannerMutation::Update {
            table,
            columns,
            values,
        } => values
            .iter()
            .map(|row| {
                let mut builder = Mutation::new_update_builder(table);
                for (col, cell) in columns.iter().zip(row.iter()) {
                    builder = builder.set(col).to(official_value(cell));
                }
                builder.build()
            })
            .collect(),
        SpannerMutation::InsertOrUpdate {
            table,
            columns,
            values,
        } => values
            .iter()
            .map(|row| {
                let mut builder = Mutation::new_insert_or_update_builder(table);
                for (col, cell) in columns.iter().zip(row.iter()) {
                    builder = builder.set(col).to(official_value(cell));
                }
                builder.build()
            })
            .collect(),
        SpannerMutation::Replace {
            table,
            columns,
            values,
        } => values
            .iter()
            .map(|row| {
                let mut builder = Mutation::new_replace_builder(table);
                for (col, cell) in columns.iter().zip(row.iter()) {
                    builder = builder.set(col).to(official_value(cell));
                }
                builder.build()
            })
            .collect(),
        SpannerMutation::Delete { table, keys, all } => {
            let key_set = if *all {
                KeySet::all()
            } else {
                keys.iter()
                    .fold(KeySet::builder(), |builder, key_tuple| {
                        builder.add_key(Key::new(key_tuple.iter().map(official_value).collect()))
                    })
                    .build()
            };
            vec![Mutation::delete(table, key_set)]
        }
    }
}

fn commit_result(
    resp: &spanner_model::CommitResponse,
) -> Result<SpannerCommitResult, SpannerError> {
    let Some(ref ts) = resp.commit_timestamp else {
        return Err(SpannerError::Protocol(
            "Commit returned no commit_timestamp; the transaction outcome is unknown".into(),
        ));
    };
    let micros = ts
        .seconds()
        .saturating_mul(1_000_000)
        .saturating_add(i64::from(ts.nanos()) / 1_000);
    let mutation_count = resp.commit_stats.as_ref().map(|s| s.mutation_count);
    Ok(SpannerCommitResult {
        commit_timestamp_micros: Some(micros),
        mutation_count,
        retry_count: 0,
    })
}

/// Google Cloud Spanner v1 client.
pub struct SpannerClient {
    config: SpannerClientConfig,
    token_cache: Option<Arc<GcpTokenCache>>,
    injected_spanner: Option<Spanner>,
    injected_admin: Option<DatabaseAdmin>,
    official: OnceCell<Spanner>,
    database_client: OnceCell<DatabaseClient>,
    admin_client: OnceCell<DatabaseAdmin>,
}

impl SpannerClient {
    /// Construct a new [`SpannerClient`].
    pub fn new(config: SpannerClientConfig) -> Result<Self, SpannerError> {
        validate_spanner_config(&config)?;
        let token_cache = if config.is_cleartext() && config.auth_options.is_empty() {
            None
        } else {
            GcpTokenCache::from_options(&config.auth_options, SPANNER_OAUTH_SCOPES)
        };
        Ok(Self {
            config,
            token_cache,
            injected_spanner: None,
            injected_admin: None,
            official: OnceCell::new(),
            database_client: OnceCell::new(),
            admin_client: OnceCell::new(),
        })
    }

    /// Construct a [`SpannerClient`] with pre-built [`Spanner`] and optional [`DatabaseAdmin`] handles.
    pub fn with_clients(
        spanner: Spanner,
        admin: Option<DatabaseAdmin>,
        config: SpannerClientConfig,
    ) -> Result<Self, SpannerError> {
        validate_spanner_config(&config)?;
        Ok(Self {
            config,
            token_cache: None,
            injected_spanner: Some(spanner),
            injected_admin: admin,
            official: OnceCell::new(),
            database_client: OnceCell::new(),
            admin_client: OnceCell::new(),
        })
    }

    /// Client configuration reference.
    #[inline]
    pub fn config(&self) -> &SpannerClientConfig {
        &self.config
    }

    /// Resolve the effective DatabaseAdmin REST endpoint for `UpdateDatabaseDdl`.
    pub fn effective_admin_endpoint(&self) -> Result<String, SpannerError> {
        if let Some(ref explicit) = self.config.admin_endpoint {
            let trimmed = explicit.trim().trim_end_matches('/');
            if !trimmed.is_empty() {
                return Ok(trimmed.to_string());
            }
        }
        let endpoint = self.config.effective_endpoint();
        if endpoint_is_googleapis(&endpoint) {
            return Ok(DEFAULT_SPANNER_ADMIN_ENDPOINT.to_string());
        }
        if let Ok(mut parsed) = Url::parse(&endpoint)
            && parsed.scheme() == "http"
            && parsed.port() == Some(9010)
        {
            let _ = parsed.set_port(Some(9020));
            return Ok(parsed.as_str().trim_end_matches('/').to_string());
        }
        Err(SpannerError::InvalidArgument(format!(
            "set SpannerClientConfig.admin_endpoint: the DDL REST endpoint cannot be derived from {endpoint}"
        )))
    }

    async fn official_spanner(&self) -> Result<&Spanner, SpannerError> {
        if let Some(ref spanner) = self.injected_spanner {
            return Ok(spanner);
        }
        self.official
            .get_or_try_init(|| async {
                install_rustls_ring_provider();
                let mut builder = Spanner::builder()
                    .with_endpoint(self.config.effective_endpoint())
                    .with_credentials(official_credentials(self.token_cache.clone()))
                    .with_channel_pool(StaticChannelPoolConfig::new(self.config.pool_size.max(1)));
                if let Some(ref pem) = self.config.ca_pem {
                    builder = builder
                        .with_omni_tls(TlsConfig::new().with_root_certificate_pem(pem.clone()));
                }
                builder.build().await.map_err(|e| {
                    SpannerError::Protocol(format!("failed to build Spanner client: {e}"))
                })
            })
            .await
    }

    async fn database_client(&self) -> Result<&DatabaseClient, SpannerError> {
        self.database_client
            .get_or_try_init(|| async {
                let spanner = self.official_spanner().await?;
                let mut builder = spanner
                    .database_client(&self.config.database)
                    .with_leader_aware_routing(self.config.route_to_leader_enabled);
                if let Some(ref role) = self.config.database_role
                    && !role.is_empty()
                {
                    builder = builder.with_database_role(role.clone());
                }
                Ok(builder.build().await?)
            })
            .await
    }

    async fn database_admin(&self) -> Result<&DatabaseAdmin, SpannerError> {
        if let Some(ref admin) = self.injected_admin {
            return Ok(admin);
        }
        self.admin_client
            .get_or_try_init(|| async {
                install_rustls_ring_provider();
                let admin_ep = self.effective_admin_endpoint()?;
                DatabaseAdmin::builder()
                    .with_endpoint(admin_ep)
                    .with_credentials(official_credentials(self.token_cache.clone()))
                    .build()
                    .await
                    .map_err(|e| {
                        SpannerError::Protocol(format!("failed to build DatabaseAdmin: {e}"))
                    })
            })
            .await
    }

    fn statement(&self, sql: &str, params: &[SpannerQueryParameter]) -> Statement {
        self.statement_with_tag(sql, params, None)
    }

    fn statement_with_tag(
        &self,
        sql: &str,
        params: &[SpannerQueryParameter],
        request_tag: Option<&str>,
    ) -> Statement {
        let effective_sql = if self.config.dialect == SpannerDialect::GoogleSql && sql.contains('$')
        {
            rewrite_spangres_placeholders(sql).0
        } else {
            sql.to_string()
        };
        let mut builder = Statement::builder(effective_sql);
        for param in params {
            builder = builder.add_typed_param(
                &param.name,
                official_value(&param.value),
                official_type(&param.param_type),
            );
        }
        if let Some(tag) = request_tag.filter(|t| !t.is_empty()) {
            builder = builder.set_request_tag(tag);
        }
        builder.build()
    }

    /// Execute a single-use read-only query with the specified [`SpannerStaleness`] bound.
    pub async fn query(
        &self,
        sql: &str,
        params: &[SpannerQueryParameter],
        staleness: SpannerStaleness,
    ) -> Result<SpannerQueryResult, SpannerError> {
        let db = self.database_client().await?;
        let bound = timestamp_bound(staleness)?;
        let rs = db
            .single_use()
            .set_timestamp_bound(bound)
            .build()
            .execute_query(self.statement(sql, params))
            .await?;
        collect_query_result(rs).await
    }

    /// Execute a batch read-only `PartitionQuery` and run all partitions concurrently.
    pub async fn partitioned_query(
        &self,
        sql: &str,
        params: &[SpannerQueryParameter],
        max_partitions: i64,
        data_boost_enabled: bool,
        staleness: SpannerStaleness,
    ) -> Result<SpannerQueryResult, SpannerError> {
        let db = self.database_client().await?;
        let bound = timestamp_bound(staleness)?;
        let batch_tx = db
            .batch_read_only_transaction()
            .set_timestamp_bound(bound)
            .build()
            .await?;
        let options =
            spanner_model::PartitionOptions::new().set_max_partitions(max_partitions.max(1));
        let partitions = batch_tx
            .partition_query(self.statement(sql, params), options)
            .await?;
        let part_results = try_join_all(partitions.into_iter().map(|part| async move {
            let rs = part.set_data_boost(data_boost_enabled).execute(db).await?;
            collect_query_result(rs).await
        }))
        .await?;

        let mut columns = Vec::new();
        let mut schema = Vec::new();
        let mut rows = Vec::new();
        let mut row_count_exact: Option<i64> = None;
        let mut row_count_lower_bound: Option<i64> = None;
        for part in part_results {
            if schema.is_empty() && !part.schema.is_empty() {
                columns = part.columns;
                schema = part.schema;
            }
            rows.extend(part.rows);
            if let Some(exact) = part.row_count_exact {
                row_count_exact = Some(row_count_exact.unwrap_or(0) + exact);
            }
            if let Some(lower) = part.row_count_lower_bound {
                row_count_lower_bound = Some(row_count_lower_bound.unwrap_or(0) + lower);
            }
        }
        Ok(SpannerQueryResult {
            columns,
            schema,
            rows,
            row_count_exact,
            row_count_lower_bound,
        })
    }

    /// Execute a single DML statement inside a read-write transaction and return the modified row count.
    pub async fn execute_dml(
        &self,
        sql: &str,
        params: &[SpannerQueryParameter],
        request_tag: Option<&str>,
        transaction_tag: Option<&str>,
    ) -> Result<i64, SpannerError> {
        let db = self.database_client().await?;
        let stmt = self.statement_with_tag(sql, params, request_tag);
        let mut runner_builder = db.read_write_transaction();
        if let Some(tag) = transaction_tag.filter(|t| !t.is_empty()) {
            runner_builder = runner_builder.set_transaction_tag(tag);
        }
        if let Some(delay_ms) = self.config.max_commit_delay_ms
            && delay_ms > 0
        {
            runner_builder = runner_builder.set_max_commit_delay(Duration::from_millis(delay_ms));
        }
        let outcome = runner_builder
            .build()
            .run(move |tx: ReadWriteTransaction| {
                let stmt = stmt.clone();
                async move { tx.execute_update(stmt).await }
            })
            .await?;
        let _ = commit_result(&outcome.commit_response)?;
        Ok(outcome.result)
    }

    /// Execute a batch of DML statements inside a read-write transaction and return per-statement row counts.
    pub async fn execute_batch_dml(
        &self,
        statements: &[SpannerBatchDmlStatement],
        request_tag: Option<&str>,
        transaction_tag: Option<&str>,
    ) -> Result<Vec<i64>, SpannerError> {
        if statements.is_empty() {
            return Ok(Vec::new());
        }
        let db = self.database_client().await?;
        let mut batch_builder = BatchDml::builder();
        for item in statements {
            batch_builder = batch_builder.add_statement(self.statement(&item.sql, &item.params));
        }
        if let Some(tag) = request_tag.filter(|t| !t.is_empty()) {
            batch_builder = batch_builder.set_request_tag(tag);
        }
        let batch = batch_builder.build();

        let mut runner_builder = db.read_write_transaction();
        if let Some(tag) = transaction_tag.filter(|t| !t.is_empty()) {
            runner_builder = runner_builder.set_transaction_tag(tag);
        }
        if let Some(delay_ms) = self.config.max_commit_delay_ms
            && delay_ms > 0
        {
            runner_builder = runner_builder.set_max_commit_delay(Duration::from_millis(delay_ms));
        }
        let outcome = runner_builder
            .build()
            .run(move |tx: ReadWriteTransaction| {
                let batch = batch.clone();
                async move { tx.execute_batch_update(batch).await }
            })
            .await?;
        let _ = commit_result(&outcome.commit_response)?;
        Ok(outcome.result)
    }

    /// Execute a Partitioned DML (`PDML`) statement and return the lower-bound modified row count.
    pub async fn execute_partitioned_dml(
        &self,
        sql: &str,
        params: &[SpannerQueryParameter],
        exclude_txn_from_change_streams: bool,
    ) -> Result<i64, SpannerError> {
        let db = self.database_client().await?;
        let count = db
            .partitioned_dml_transaction()
            .set_exclude_txn_from_change_streams(exclude_txn_from_change_streams)
            .build()
            .execute_update(self.statement(sql, params))
            .await?;
        Ok(count)
    }

    /// Commit a list of [`SpannerMutation`] items in a write-only transaction.
    pub async fn apply_mutations(
        &self,
        mutations: &[SpannerMutation],
        transaction_tag: Option<&str>,
        exclude_txn_from_change_streams: bool,
    ) -> Result<SpannerCommitResult, SpannerError> {
        if mutations.is_empty() {
            return Ok(SpannerCommitResult {
                commit_timestamp_micros: None,
                mutation_count: Some(0),
                retry_count: 0,
            });
        }
        let db = self.database_client().await?;
        let official_mutations = mutations
            .iter()
            .flat_map(official_mutation_rows)
            .collect::<Vec<_>>();
        let mut tx_builder = db
            .write_only_transaction()
            .set_exclude_txn_from_change_streams(exclude_txn_from_change_streams);
        if let Some(tag) = transaction_tag.filter(|t| !t.is_empty()) {
            tx_builder = tx_builder.set_transaction_tag(tag);
        }
        if let Some(delay_ms) = self.config.max_commit_delay_ms
            && delay_ms > 0
        {
            tx_builder = tx_builder.set_max_commit_delay(Duration::from_millis(delay_ms));
        }
        let resp = tx_builder.build().write(official_mutations).await?;
        commit_result(&resp)
    }

    /// Execute `BatchWrite` across `mutation_groups`, chunking batches so each RPC stays at or below
    /// [`MAX_MUTATION_CELLS_PER_BATCH`] (`80,000` cells).
    pub async fn batch_write(
        &self,
        mutation_groups: &[SpannerMutationGroup],
        exclude_txn_from_change_streams: bool,
    ) -> Result<Vec<SpannerBatchWriteResult>, SpannerError> {
        if mutation_groups.is_empty() {
            return Ok(Vec::new());
        }
        let db = self.database_client().await?;
        let mut results = Vec::new();
        let mut chunk: Vec<&SpannerMutationGroup> = Vec::new();
        let mut chunk_cells: usize = 0;
        let mut chunk_start: i32 = 0;

        for group in mutation_groups {
            let group_cells = group.cell_count();
            if !chunk.is_empty() && chunk_cells + group_cells > MAX_MUTATION_CELLS_PER_BATCH {
                let chunk_len = chunk.len() as i32;
                let mut part = self
                    .batch_write_chunk(db, &chunk, chunk_start, exclude_txn_from_change_streams)
                    .await?;
                results.append(&mut part);
                chunk_start += chunk_len;
                chunk.clear();
                chunk_cells = 0;
            }
            chunk.push(group);
            chunk_cells += group_cells;
        }
        if !chunk.is_empty() {
            let mut part = self
                .batch_write_chunk(db, &chunk, chunk_start, exclude_txn_from_change_streams)
                .await?;
            results.append(&mut part);
        }
        Ok(results)
    }

    async fn batch_write_chunk(
        &self,
        db: &DatabaseClient,
        groups: &[&SpannerMutationGroup],
        chunk_start: i32,
        exclude_txn_from_change_streams: bool,
    ) -> Result<Vec<SpannerBatchWriteResult>, SpannerError> {
        let official_groups = groups
            .iter()
            .map(|g| {
                MutationGroup::new(
                    g.mutations
                        .iter()
                        .flat_map(official_mutation_rows)
                        .collect(),
                )
            })
            .collect::<Vec<_>>();
        let mut stream = db
            .batch_write_transaction()
            .set_exclude_txn_from_change_streams(exclude_txn_from_change_streams)
            .build()
            .execute_streaming(official_groups)
            .await?;
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            let resp = item?;
            let (status_code, status_message) = resp
                .status
                .as_ref()
                .map(|st| (st.code, st.message.clone()))
                .unwrap_or((0, String::new()));
            let commit_timestamp_micros = resp.commit_timestamp.as_ref().map(|ts| {
                ts.seconds()
                    .saturating_mul(1_000_000)
                    .saturating_add(i64::from(ts.nanos()) / 1_000)
            });
            out.push(SpannerBatchWriteResult {
                indexes: resp.indexes.into_iter().map(|i| i + chunk_start).collect(),
                status_code,
                status_message,
                commit_timestamp_micros,
            });
        }
        Ok(out)
    }

    /// Execute batched DDL statements via `DatabaseAdmin::update_database_ddl` and poll `get_operation`
    /// until `done == true` or `config.ddl_timeout` elapses.
    pub async fn update_database_ddl(&self, statements: &[String]) -> Result<String, SpannerError> {
        if statements.is_empty() {
            return Ok(String::new());
        }
        let admin = self.database_admin().await?;
        let deadline = Instant::now() + self.config.ddl_timeout;
        let timeout_ms = self.config.ddl_timeout.as_millis() as u64;
        let mut op = admin
            .update_database_ddl()
            .set_database(&self.config.database)
            .set_statements(statements.to_vec())
            .send()
            .await
            .map_err(|e| SpannerError::Protocol(format!("UpdateDatabaseDdl failed: {e}")))?;

        let mut backoff = Duration::from_millis(100);
        loop {
            if op.done {
                if let Some(OperationResult::Error(status)) = op.result {
                    return Err(SpannerError::Ddl {
                        operation: op.name,
                        code: status.code,
                        message: status.message,
                    });
                }
                return Ok(op.name);
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(SpannerError::DdlTimeout {
                    operation: op.name,
                    timeout_ms,
                });
            }
            let sleep_for = backoff.min(deadline.saturating_duration_since(now));
            tokio::time::sleep(sleep_for).await;
            backoff = (backoff * 2).min(Duration::from_secs(5));
            let op_name = op.name.clone();
            op = admin
                .get_operation()
                .set_name(&op_name)
                .send()
                .await
                .map_err(|e| {
                    SpannerError::Protocol(format!("GetOperation({op_name}) failed: {e}"))
                })?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_spanner_parameters_json_accepts_object_descriptors_typed_null_and_json_arrays() {
        let raw = r#"{
            "p1": 42,
            "p2": {"type": "INT64", "value": null},
            "p3": {"type": "JSON", "value": ["alpha", 42, {"ok": true}]},
            "p4": {"type": "NUMERIC", "value": "12345.6789"}
        }"#;
        let params = parse_spanner_parameters_json(Some(raw)).expect("valid object params");
        assert_eq!(params.len(), 4);

        let by_name: HashMap<String, SpannerQueryParameter> =
            params.into_iter().map(|p| (p.name.clone(), p)).collect();
        assert_eq!(by_name["p1"].param_type.code, SpannerTypeCode::Int64);
        assert_eq!(by_name["p1"].value, ProtobufValue::String("42".to_string()));

        assert_eq!(by_name["p2"].param_type.code, SpannerTypeCode::Int64);
        assert_eq!(by_name["p2"].value, ProtobufValue::Null);

        assert_eq!(by_name["p3"].param_type.code, SpannerTypeCode::Json);
        assert_eq!(
            by_name["p3"].value,
            ProtobufValue::String(r#"["alpha",42,{"ok":true}]"#.to_string())
        );

        assert_eq!(by_name["p4"].param_type.code, SpannerTypeCode::Numeric);
        assert_eq!(
            by_name["p4"].value,
            ProtobufValue::String("12345.6789".to_string())
        );
    }
}
