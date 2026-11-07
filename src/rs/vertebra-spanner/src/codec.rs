//! Cloud Spanner SQL parameter, mutation, batch DML, and value codecs.

use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use google_cloud_spanner::key::{Key, KeySet};
use google_cloud_spanner::model as spanner_model;
use google_cloud_spanner::mutation::Mutation;
use google_cloud_spanner::transaction::{Timestamp, TimestampBound};
use google_cloud_spanner::types::Type as OfficialType;
use google_cloud_spanner::value::{Kind, Value as OfficialValue};

use crate::client::SpannerError;

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
                    let default_elem = SpannerType::scalar(SpannerTypeCode::String);
                    let elem_type = col_type
                        .array_element_type
                        .as_deref()
                        .unwrap_or(&default_elem);
                    let mut out = Vec::with_capacity(items.len());
                    for item in items {
                        out.push(Self::from_protobuf_value(item, elem_type)?);
                    }
                    Ok(Self::Array(out))
                }
                other => Err(SpannerError::Protocol(format!(
                    "expected ListValue for ARRAY, got {other:?}"
                ))),
            },
            SpannerTypeCode::Struct => match raw {
                ProtobufValue::List(items) => {
                    let default_field_ty = SpannerType::scalar(SpannerTypeCode::String);
                    let mut fields = Vec::with_capacity(items.len());
                    for (idx, item) in items.iter().enumerate() {
                        let (fname, ftype) = match col_type.struct_fields.get(idx) {
                            Some(f) => (f.name.clone(), &f.field_type),
                            None => (format!("field_{idx}"), &default_field_ty),
                        };
                        fields.push((fname, Self::from_protobuf_value(item, ftype)?));
                    }
                    Ok(Self::Struct(fields))
                }
                ProtobufValue::Struct(entries) => {
                    let default_field_ty = SpannerType::scalar(SpannerTypeCode::String);
                    let mut fields = Vec::with_capacity(entries.len());
                    for (k, v) in entries {
                        let ftype = col_type
                            .struct_fields
                            .iter()
                            .find(|f| f.name == *k)
                            .map(|f| &f.field_type)
                            .unwrap_or(&default_field_ty);
                        fields.push((k.clone(), Self::from_protobuf_value(v, ftype)?));
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

    /// Decode an owned [`ProtobufValue`] using its [`SpannerType`] schema without cloning inner strings.
    pub fn from_protobuf_value_owned(
        raw: ProtobufValue,
        col_type: &SpannerType,
    ) -> Result<Self, SpannerError> {
        if matches!(raw, ProtobufValue::Null) {
            return Ok(Self::Null);
        }
        match col_type.code {
            SpannerTypeCode::Bool => match raw {
                ProtobufValue::Bool(b) => Ok(Self::Bool(b)),
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
                ProtobufValue::Number(n) => Ok(Self::Int64(n as i64)),
                other => Err(SpannerError::Protocol(format!(
                    "expected INT64 StringValue, got {other:?}"
                ))),
            },
            SpannerTypeCode::Float64 | SpannerTypeCode::Float32 => match raw {
                ProtobufValue::Number(n) => Ok(Self::Float64(n)),
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
                ProtobufValue::String(s) => Ok(Self::Json(s)),
                other => Err(SpannerError::Protocol(format!(
                    "expected StringValue for JSON, got {other:?}"
                ))),
            },
            SpannerTypeCode::Numeric => match raw {
                ProtobufValue::String(s) => Ok(Self::Numeric(s)),
                ProtobufValue::Number(n) => Ok(Self::Numeric(n.to_string())),
                other => Err(SpannerError::Protocol(format!(
                    "expected StringValue for NUMERIC, got {other:?}"
                ))),
            },
            SpannerTypeCode::Timestamp => match raw {
                ProtobufValue::String(s) => Ok(Self::Timestamp(s)),
                other => Err(SpannerError::Protocol(format!(
                    "expected StringValue for TIMESTAMP, got {other:?}"
                ))),
            },
            SpannerTypeCode::Date => match raw {
                ProtobufValue::String(s) => Ok(Self::Date(s)),
                other => Err(SpannerError::Protocol(format!(
                    "expected StringValue for DATE, got {other:?}"
                ))),
            },
            SpannerTypeCode::Uuid => match raw {
                ProtobufValue::String(s) => Ok(Self::Uuid(s)),
                other => Err(SpannerError::Protocol(format!(
                    "expected StringValue for UUID, got {other:?}"
                ))),
            },
            SpannerTypeCode::Array => match raw {
                ProtobufValue::List(items) => {
                    let default_elem = SpannerType::scalar(SpannerTypeCode::String);
                    let elem_type = col_type
                        .array_element_type
                        .as_deref()
                        .unwrap_or(&default_elem);
                    let mut out = Vec::with_capacity(items.len());
                    for item in items {
                        out.push(Self::from_protobuf_value_owned(item, elem_type)?);
                    }
                    Ok(Self::Array(out))
                }
                other => Err(SpannerError::Protocol(format!(
                    "expected ListValue for ARRAY, got {other:?}"
                ))),
            },
            SpannerTypeCode::Struct => match raw {
                ProtobufValue::List(items) => {
                    let default_field_ty = SpannerType::scalar(SpannerTypeCode::String);
                    let mut fields = Vec::with_capacity(items.len());
                    for (idx, item) in items.into_iter().enumerate() {
                        let (fname, ftype) = match col_type.struct_fields.get(idx) {
                            Some(f) => (f.name.clone(), &f.field_type),
                            None => (format!("field_{idx}"), &default_field_ty),
                        };
                        fields.push((fname, Self::from_protobuf_value_owned(item, ftype)?));
                    }
                    Ok(Self::Struct(fields))
                }
                ProtobufValue::Struct(entries) => {
                    let default_field_ty = SpannerType::scalar(SpannerTypeCode::String);
                    let mut fields = Vec::with_capacity(entries.len());
                    for (k, v) in entries {
                        let ftype = col_type
                            .struct_fields
                            .iter()
                            .find(|f| f.name == k)
                            .map(|f| &f.field_type)
                            .unwrap_or(&default_field_ty);
                        fields.push((k, Self::from_protobuf_value_owned(v, ftype)?));
                    }
                    Ok(Self::Struct(fields))
                }
                other => Err(SpannerError::Protocol(format!(
                    "expected ListValue/StructValue for STRUCT, got {other:?}"
                ))),
            },
            SpannerTypeCode::String | SpannerTypeCode::Interval | SpannerTypeCode::Unspecified => {
                match raw {
                    ProtobufValue::String(s) => Ok(Self::String(s)),
                    ProtobufValue::Bool(b) => Ok(Self::Bool(b)),
                    ProtobufValue::Number(n) => Ok(Self::Float64(n)),
                    ProtobufValue::List(items) => {
                        let elem = SpannerType::scalar(SpannerTypeCode::String);
                        let mut out = Vec::with_capacity(items.len());
                        for item in items {
                            out.push(Self::from_protobuf_value_owned(item, &elem)?);
                        }
                        Ok(Self::Array(out))
                    }
                    ProtobufValue::Struct(entries) => {
                        let elem = SpannerType::scalar(SpannerTypeCode::String);
                        let mut out = Vec::with_capacity(entries.len());
                        for (k, v) in entries {
                            out.push((k, Self::from_protobuf_value_owned(v, &elem)?));
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

fn is_known_spanner_type_str(s: &str) -> bool {
    matches!(
        s.trim().to_ascii_uppercase().as_str(),
        "BOOL"
            | "BOOLEAN"
            | "INT64"
            | "INTEGER"
            | "BIGINT"
            | "FLOAT64"
            | "DOUBLE"
            | "DOUBLE PRECISION"
            | "FLOAT32"
            | "REAL"
            | "TIMESTAMP"
            | "TIMESTAMPTZ"
            | "DATE"
            | "STRING"
            | "VARCHAR"
            | "TEXT"
            | "BYTES"
            | "BYTEA"
            | "ARRAY"
            | "STRUCT"
            | "NUMERIC"
            | "DECIMAL"
            | "PG_NUMERIC"
            | "JSON"
            | "JSONB"
            | "PG_JSONB"
            | "PROTO"
            | "ENUM"
            | "INTERVAL"
            | "UUID"
    )
}

fn scalar_spanner_type_from_str(type_str: &str) -> SpannerType {
    let code = SpannerTypeCode::from_type_str(type_str);
    let mut ty = SpannerType::scalar(code);
    if type_str.eq_ignore_ascii_case("PG_JSONB") {
        ty.type_annotation = SpannerTypeAnnotation::PgJsonb;
    } else if type_str.eq_ignore_ascii_case("PG_NUMERIC") {
        ty.type_annotation = SpannerTypeAnnotation::PgNumeric;
    }
    ty
}

fn is_typed_spanner_descriptor(val: &serde_json::Value) -> bool {
    let Some(map) = val.as_object() else {
        return false;
    };
    let Some(type_str) = map.get("type").and_then(serde_json::Value::as_str) else {
        return false;
    };
    is_known_spanner_type_str(type_str)
        && map
            .keys()
            .all(|k| matches!(k.as_str(), "name" | "type" | "value" | "array_type"))
}

fn parse_typed_spanner_value_and_type(item: &serde_json::Value) -> (ProtobufValue, SpannerType) {
    let type_str = item
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("STRING");
    let code = SpannerTypeCode::from_type_str(type_str);
    let raw_value = item.get("value").unwrap_or(&serde_json::Value::Null);
    if code == SpannerTypeCode::Array {
        if let Some(elem_type_str) = item.get("array_type").and_then(|v| v.as_str()) {
            let param_type = SpannerType::array(scalar_spanner_type_from_str(elem_type_str));
            let value = json_to_protobuf_value(raw_value, &param_type);
            (value, param_type)
        } else if let serde_json::Value::Array(items) = raw_value {
            parse_inferred_array_value_and_type(items)
        } else {
            let param_type = SpannerType::array(SpannerType::scalar(SpannerTypeCode::String));
            let value = json_to_protobuf_value(raw_value, &param_type);
            (value, param_type)
        }
    } else {
        let param_type = scalar_spanner_type_from_str(type_str);
        let value = json_to_protobuf_value(raw_value, &param_type);
        (value, param_type)
    }
}

fn parse_typed_spanner_param(name: String, item: &serde_json::Value) -> SpannerQueryParameter {
    let (value, param_type) = parse_typed_spanner_value_and_type(item);
    SpannerQueryParameter {
        name,
        value,
        param_type,
    }
}

fn parse_inferred_array_value_and_type(
    items: &[serde_json::Value],
) -> (ProtobufValue, SpannerType) {
    let mut pb_items = Vec::with_capacity(items.len());
    let mut elem_type = SpannerType::scalar(SpannerTypeCode::String);
    let mut found_typed_elem = false;
    for item in items {
        let (pb, ty) = parse_inferred_spanner_value_and_type(item);
        if !found_typed_elem && !item.is_null() {
            elem_type = ty;
            found_typed_elem = true;
        }
        pb_items.push(pb);
    }
    (ProtobufValue::List(pb_items), SpannerType::array(elem_type))
}

fn parse_inferred_spanner_value_and_type(val: &serde_json::Value) -> (ProtobufValue, SpannerType) {
    if is_typed_spanner_descriptor(val) {
        return parse_typed_spanner_value_and_type(val);
    }
    match val {
        serde_json::Value::Null => (
            ProtobufValue::Null,
            SpannerType::scalar(SpannerTypeCode::String),
        ),
        serde_json::Value::Bool(b) => (
            ProtobufValue::Bool(*b),
            SpannerType::scalar(SpannerTypeCode::Bool),
        ),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                (
                    ProtobufValue::String(i.to_string()),
                    SpannerType::scalar(SpannerTypeCode::Int64),
                )
            } else {
                (
                    ProtobufValue::Number(n.as_f64().unwrap_or(0.0)),
                    SpannerType::scalar(SpannerTypeCode::Float64),
                )
            }
        }
        serde_json::Value::String(s) => (
            ProtobufValue::String(s.clone()),
            SpannerType::scalar(SpannerTypeCode::String),
        ),
        serde_json::Value::Array(items) => parse_inferred_array_value_and_type(items),
        serde_json::Value::Object(_) => (
            ProtobufValue::String(val.to_string()),
            SpannerType::scalar(SpannerTypeCode::Json),
        ),
    }
}

/// Parse a [`serde_json::Value`] array or object of query parameters into [`SpannerQueryParameter`] items.
pub fn parse_spanner_parameters_value(
    parsed: &serde_json::Value,
) -> Result<Vec<SpannerQueryParameter>, SpannerError> {
    match parsed {
        serde_json::Value::Null => Ok(Vec::new()),
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
                let (value, param_type) = parse_inferred_spanner_value_and_type(entry);
                out.push(SpannerQueryParameter {
                    name,
                    value,
                    param_type,
                });
            }
            Ok(out)
        }
        _ => Err(SpannerError::InvalidArgument(
            "params_json must be a JSON array or object".to_string(),
        )),
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
    if raw == "null" || raw == "[]" || raw == "{}" {
        return Ok(Vec::new());
    }
    let parsed: serde_json::Value = serde_json::from_str(raw)
        .map_err(|e| SpannerError::InvalidArgument(format!("invalid params_json: {e}")))?;
    parse_spanner_parameters_value(&parsed)
}

fn json_to_protobuf_value(val: &serde_json::Value, param_type: &SpannerType) -> ProtobufValue {
    if val.is_null() {
        return ProtobufValue::Null;
    }
    if param_type.code == SpannerTypeCode::Json {
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
                param_type.code,
                SpannerTypeCode::Int64 | SpannerTypeCode::Numeric | SpannerTypeCode::Enum
            ) {
                ProtobufValue::String(n.to_string())
            } else {
                ProtobufValue::Number(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => ProtobufValue::String(s.clone()),
        serde_json::Value::Array(items) => {
            let default_elem = SpannerType::scalar(SpannerTypeCode::String);
            let elem_type = param_type
                .array_element_type
                .as_deref()
                .unwrap_or(&default_elem);
            ProtobufValue::List(
                items
                    .iter()
                    .map(|elem| {
                        let unwrap_descriptor = is_typed_spanner_descriptor(elem)
                            && (elem_type.code != SpannerTypeCode::Json
                                || elem
                                    .get("type")
                                    .and_then(serde_json::Value::as_str)
                                    .is_some_and(|t| {
                                        SpannerTypeCode::from_type_str(t) == SpannerTypeCode::Json
                                    }));
                        if unwrap_descriptor {
                            let elem_val = elem.get("value").unwrap_or(&serde_json::Value::Null);
                            json_to_protobuf_value(elem_val, elem_type)
                        } else {
                            json_to_protobuf_value(elem, elem_type)
                        }
                    })
                    .collect(),
            )
        }
        serde_json::Value::Object(map) => {
            let string_type = SpannerType::scalar(SpannerTypeCode::String);
            ProtobufValue::Struct(
                map.iter()
                    .map(|(k, v)| (k.clone(), json_to_protobuf_value(v, &string_type)))
                    .collect(),
            )
        }
    }
}

/// Parse one JSON mutation object into a [`SpannerMutation`], sharing the inferred Spanner value codec.
pub fn parse_mutation_value(item: &serde_json::Value) -> Result<SpannerMutation, SpannerError> {
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
        let all = item
            .get("all")
            .or_else(|| item.get("all_rows"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let keys = item
            .get("keys")
            .and_then(|v| v.as_array())
            .map(|rows| {
                rows.iter()
                    .map(|r| {
                        if let Some(arr) = r.as_array() {
                            arr.iter()
                                .map(|cell| parse_inferred_spanner_value_and_type(cell).0)
                                .collect()
                        } else {
                            vec![parse_inferred_spanner_value_and_type(r).0]
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
                        .map(|cells| {
                            cells
                                .iter()
                                .map(|cell| parse_inferred_spanner_value_and_type(cell).0)
                                .collect()
                        })
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

/// Parse a JSON array of Spanner mutations into [`SpannerMutation`] items.
pub fn parse_mutations_json(raw: &str) -> Result<Vec<SpannerMutation>, SpannerError> {
    let val: serde_json::Value = serde_json::from_str(raw)
        .map_err(|e| SpannerError::InvalidArgument(format!("invalid mutations_json: {e}")))?;
    let Some(arr) = val.as_array() else {
        return Err(SpannerError::InvalidArgument(
            "mutations_json must be a JSON array".to_string(),
        ));
    };
    arr.iter().map(parse_mutation_value).collect()
}

/// Parse a JSON array of Spanner mutation groups into [`SpannerMutationGroup`] items.
pub fn parse_mutation_groups_json(raw: &str) -> Result<Vec<SpannerMutationGroup>, SpannerError> {
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

/// Parse a JSON array of `ExecuteBatchDml` statements without re-serializing inner parameter objects.
pub fn parse_batch_dml_statements_json(
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
        let params = match item.get("params") {
            Some(p) => parse_spanner_parameters_value(p)?,
            None => Vec::new(),
        };
        out.push(SpannerBatchDmlStatement { sql, params });
    }
    Ok(out)
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

pub(crate) fn wkt_timestamp(us: i64) -> Result<Timestamp, SpannerError> {
    Timestamp::new(
        us.div_euclid(1_000_000),
        (us.rem_euclid(1_000_000) * 1_000) as i32,
    )
    .map_err(|e| SpannerError::InvalidArgument(e.to_string()))
}

pub(crate) fn timestamp_bound(staleness: SpannerStaleness) -> Result<TimestampBound, SpannerError> {
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

pub(crate) fn official_value(value: &ProtobufValue) -> OfficialValue {
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

pub(crate) fn official_value_owned(value: ProtobufValue) -> OfficialValue {
    match value {
        ProtobufValue::Null => OfficialValue::null(),
        ProtobufValue::Bool(b) => b.into(),
        ProtobufValue::Number(n) => n.into(),
        ProtobufValue::String(s) => s.into(),
        ProtobufValue::List(items) => items
            .into_iter()
            .map(official_value_owned)
            .collect::<Vec<OfficialValue>>()
            .into(),
        ProtobufValue::Struct(entries) => entries
            .into_iter()
            .map(|(_, v)| official_value_owned(v))
            .collect::<Vec<OfficialValue>>()
            .into(),
    }
}

pub(crate) fn official_type(ty: &SpannerType) -> OfficialType {
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

pub(crate) fn protobuf_value(value: &OfficialValue) -> Result<ProtobufValue, SpannerError> {
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

pub(crate) fn spanner_type(ty: &OfficialType) -> SpannerType {
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

pub(crate) fn official_mutation_rows(mutation: &SpannerMutation) -> Vec<Mutation> {
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

pub(crate) fn official_mutation_rows_owned(mutation: SpannerMutation) -> Vec<Mutation> {
    match mutation {
        SpannerMutation::Insert {
            table,
            columns,
            values,
        } => values
            .into_iter()
            .map(|row| {
                let mut builder = Mutation::new_insert_builder(&table);
                for (col, cell) in columns.iter().zip(row) {
                    builder = builder.set(col).to(official_value_owned(cell));
                }
                builder.build()
            })
            .collect(),
        SpannerMutation::Update {
            table,
            columns,
            values,
        } => values
            .into_iter()
            .map(|row| {
                let mut builder = Mutation::new_update_builder(&table);
                for (col, cell) in columns.iter().zip(row) {
                    builder = builder.set(col).to(official_value_owned(cell));
                }
                builder.build()
            })
            .collect(),
        SpannerMutation::InsertOrUpdate {
            table,
            columns,
            values,
        } => values
            .into_iter()
            .map(|row| {
                let mut builder = Mutation::new_insert_or_update_builder(&table);
                for (col, cell) in columns.iter().zip(row) {
                    builder = builder.set(col).to(official_value_owned(cell));
                }
                builder.build()
            })
            .collect(),
        SpannerMutation::Replace {
            table,
            columns,
            values,
        } => values
            .into_iter()
            .map(|row| {
                let mut builder = Mutation::new_replace_builder(&table);
                for (col, cell) in columns.iter().zip(row) {
                    builder = builder.set(col).to(official_value_owned(cell));
                }
                builder.build()
            })
            .collect(),
        SpannerMutation::Delete { table, keys, all } => {
            let key_set = if all {
                KeySet::all()
            } else {
                keys.into_iter()
                    .fold(KeySet::builder(), |builder, key_tuple| {
                        builder.add_key(Key::new(
                            key_tuple.into_iter().map(official_value_owned).collect(),
                        ))
                    })
                    .build()
            };
            vec![Mutation::delete(table, key_set)]
        }
    }
}

pub(crate) fn commit_result(
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

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

    #[test]
    fn parse_spanner_parameters_json_handles_typed_arrays_explicit_array_type_and_null_literal() {
        assert!(
            parse_spanner_parameters_json(Some("null"))
                .expect("null literal")
                .is_empty()
        );
        assert!(
            parse_spanner_parameters_json(Some("[]"))
                .expect("empty array")
                .is_empty()
        );
        assert!(
            parse_spanner_parameters_json(Some("{}"))
                .expect("empty object")
                .is_empty()
        );

        let raw = r#"{
            "dec_arr": [{"type": "NUMERIC", "value": "1.5"}, null],
            "leading_null_dec": [null, {"type": "NUMERIC", "value": "2.25"}],
            "json_arr": [{"type": "JSON", "value": "{\"k\":1}"}],
            "untyped_event_arr": [{"type": "click_event", "value": 42}],
            "explicit_int_arr": {"type": "ARRAY", "array_type": "INT64", "value": [1, 2, null]},
            "explicit_json_arr": {"type": "ARRAY", "array_type": "JSON", "value": [{"k": 1}]}
        }"#;
        let params = parse_spanner_parameters_json(Some(raw)).expect("valid array params");
        let by_name: HashMap<String, SpannerQueryParameter> =
            params.into_iter().map(|p| (p.name.clone(), p)).collect();

        assert_eq!(by_name["dec_arr"].param_type.code, SpannerTypeCode::Array);
        assert_eq!(
            by_name["dec_arr"]
                .param_type
                .array_element_type
                .as_ref()
                .map(|t| t.code),
            Some(SpannerTypeCode::Numeric)
        );
        assert_eq!(
            by_name["dec_arr"].value,
            ProtobufValue::List(vec![
                ProtobufValue::String("1.5".to_string()),
                ProtobufValue::Null,
            ])
        );

        assert_eq!(
            by_name["leading_null_dec"]
                .param_type
                .array_element_type
                .as_ref()
                .map(|t| t.code),
            Some(SpannerTypeCode::Numeric)
        );
        assert_eq!(
            by_name["leading_null_dec"].value,
            ProtobufValue::List(vec![
                ProtobufValue::Null,
                ProtobufValue::String("2.25".to_string()),
            ])
        );

        assert_eq!(
            by_name["json_arr"]
                .param_type
                .array_element_type
                .as_ref()
                .map(|t| t.code),
            Some(SpannerTypeCode::Json)
        );
        assert_eq!(
            by_name["json_arr"].value,
            ProtobufValue::List(vec![ProtobufValue::String("{\"k\":1}".to_string())])
        );

        assert_eq!(
            by_name["untyped_event_arr"]
                .param_type
                .array_element_type
                .as_ref()
                .map(|t| t.code),
            Some(SpannerTypeCode::Json)
        );
        assert_eq!(
            by_name["untyped_event_arr"].value,
            ProtobufValue::List(vec![ProtobufValue::String(
                "{\"type\":\"click_event\",\"value\":42}".to_string()
            )])
        );

        assert_eq!(
            by_name["explicit_int_arr"]
                .param_type
                .array_element_type
                .as_ref()
                .map(|t| t.code),
            Some(SpannerTypeCode::Int64)
        );
        assert_eq!(
            by_name["explicit_int_arr"].value,
            ProtobufValue::List(vec![
                ProtobufValue::String("1".to_string()),
                ProtobufValue::String("2".to_string()),
                ProtobufValue::Null,
            ])
        );

        assert_eq!(
            by_name["explicit_json_arr"]
                .param_type
                .array_element_type
                .as_ref()
                .map(|t| t.code),
            Some(SpannerTypeCode::Json)
        );
        assert_eq!(
            by_name["explicit_json_arr"].value,
            ProtobufValue::List(vec![ProtobufValue::String("{\"k\":1}".to_string())])
        );
    }

    #[test]
    fn parse_mutations_and_batch_dml_reuse_inferred_value_codec() {
        let muts = parse_mutations_json(
            r#"[
                {
                    "op": "insert_or_update",
                    "table": "accounts",
                    "columns": ["id", "balance", "active", "meta"],
                    "values": [[7, {"type": "NUMERIC", "value": "99.95"}, true, {"tier": "pro"}]]
                },
                {
                    "op": "delete",
                    "table": "accounts",
                    "keys": [[9]]
                }
            ]"#,
        )
        .expect("valid mutations");
        assert_eq!(muts.len(), 2);
        assert_eq!(
            muts[0],
            SpannerMutation::insert_or_update(
                "accounts",
                vec![
                    "id".to_string(),
                    "balance".to_string(),
                    "active".to_string(),
                    "meta".to_string(),
                ],
                vec![vec![
                    ProtobufValue::String("7".to_string()),
                    ProtobufValue::String("99.95".to_string()),
                    ProtobufValue::Bool(true),
                    ProtobufValue::String(r#"{"tier":"pro"}"#.to_string()),
                ]],
            )
        );
        assert_eq!(
            muts[1],
            SpannerMutation::delete(
                "accounts",
                vec![vec![ProtobufValue::String("9".to_string())]],
                false,
            )
        );

        let batch = parse_batch_dml_statements_json(
            r#"[{"sql": "UPDATE accounts SET active = @a WHERE id = @id", "params": {"a": false, "id": 7}}]"#,
        )
        .expect("valid batch dml");
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].params.len(), 2);
    }
}
