//! Pure-Rust Google Cloud Spanner v1 gRPC engine (`google.spanner.v1.Spanner` &
//! `google.spanner.admin.database.v1.DatabaseAdmin`).
//!
//! Provides:
//! - Cached multiplexed session management (`CreateSession` with `multiplexed: true`, 7-day
//!   proactive rotation, and transparent recreation on `Session` `NOT_FOUND`).
//! - Selective `x-goog-spanner-route-to-leader: true` routing metadata (present on `CreateSession`,
//!   `ReadWrite`, `PartitionedDml`, and `BatchWrite`; omitted on `ReadOnly` and `PartitionQuery`
//!   so Spanner serves reads from local replicas) and monotonic
//!   `x-goog-spanner-request-id: 1.{rand64_hex}.{client_id}.{channel_id}.{req_seq}.{attempt}` headers.
//! - Streaming `PartialResultSet` chunk merger (`chunked_value: true` string concatenation prior to
//!   base64/numeric/timestamp/JSON decoding + recursive `ListValue` tail merging) with bounded
//!   `32 MiB` `resume_token` replay on transient `UNAVAILABLE` / `INTERNAL` stream interruptions.
//! - Multiplexed `ReadWrite` transaction state machine with inline `TransactionSelector.begin` on
//!   statement #1 (`0` extra RTT), fallback `BeginTransaction` with `mutation_key` on mutation-only
//!   transactions, monotonic 1-based `seqno`, highest-`seq_num` `MultiplexedSessionPrecommitToken`
//!   tracking, `CommitResponse.MultiplexedSessionRetry` replay (empty `mutations`),
//!   `ExecuteBatchDmlResponse.status` inspection, and `Aborted` wound-wait retry with `RetryInfo`
//!   and `multiplexed_session_previous_transaction_id`.
//! - `Mutation` builders (`insert`, `update`, `insert_or_update`, `replace`, `delete`), `BatchWrite`
//!   (chunked at `<= 80,000` mutated cells), `PartitionedDml`, and batched `UpdateDatabaseDdl` LRO
//!   polling.
//! - Parallel `PartitionQuery` + `data_boost_enabled` fan-out streaming directly into a zero-copy
//!   `VTBARROW` POSIX shared-memory segment via `ShmArrowStreamWriter`.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use bytes::Bytes;
use vertebra_core::gcp_auth::GcpTokenCache;
use vertebra_core::shm::ShmRegion;

use crate::arrow_shm::ShmArrowStreamWriter;
use crate::grpc::{
    GrpcChannelPool, GrpcCode, GrpcErrorDetail, GrpcPoolConfig, GrpcRequestMetadata, GrpcStatus,
    ProtoError, ProtoReader, ProtoWriter, WireType, generate_request_uuid_v4,
};

/// Default Cloud Spanner gRPC endpoint.
pub const DEFAULT_SPANNER_ENDPOINT: &str = "https://spanner.googleapis.com";

/// Maximum un-checkpointed byte buffer (`32 MiB`) before requiring a `resume_token` flush.
pub const MAX_UNCHECKPOINTED_RESUME_BYTES: usize = 32 * 1024 * 1024;

/// Maximum mutated cells per `BatchWrite` or `Commit` batch (`80,000`, below Spanner's `100,000` limit).
pub const MAX_MUTATION_CELLS_PER_BATCH: usize = 80_000;

/// Multiplexed session rotation interval (`7` days in milliseconds).
pub const MULTIPLEXED_SESSION_ROTATION_MS: i64 = 7 * 24 * 60 * 60 * 1_000;

static NEXT_SPANNER_CLIENT_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_SPANNER_SHM_SEQ: AtomicU64 = AtomicU64::new(1);

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
    /// Optional custom PEM root certificate bundle for testing.
    pub ca_pem: Option<Vec<u8>>,
}

impl SpannerClientConfig {
    /// Create a new [`SpannerClientConfig`] for `database` (`projects/{p}/instances/{i}/databases/{d}`).
    pub fn new(database: impl Into<String>) -> Self {
        Self {
            database: database.into(),
            database_role: None,
            endpoint: DEFAULT_SPANNER_ENDPOINT.to_string(),
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SpannerError {
    InvalidArgument(String),
    Grpc(GrpcStatus),
    Protocol(String),
    Auth(String),
    Shm(String),
}

impl fmt::Display for SpannerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArgument(msg) => write!(f, "Spanner invalid argument: {msg}"),
            Self::Grpc(status) => write!(f, "Spanner gRPC error: {status}"),
            Self::Protocol(msg) => write!(f, "Spanner protocol error: {msg}"),
            Self::Auth(msg) => write!(f, "Spanner auth error: {msg}"),
            Self::Shm(msg) => write!(f, "Spanner SHM error: {msg}"),
        }
    }
}

impl std::error::Error for SpannerError {}

impl From<GrpcStatus> for SpannerError {
    fn from(status: GrpcStatus) -> Self {
        Self::Grpc(status)
    }
}

impl From<ProtoError> for SpannerError {
    fn from(err: ProtoError) -> Self {
        Self::Protocol(err.to_string())
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

    /// Encode this `google.spanner.v1.Type` message into a [`ProtoWriter`].
    pub fn encode(&self, w: &mut ProtoWriter) {
        if self.code != SpannerTypeCode::Unspecified {
            w.write_i32(1, self.code as i32);
        }
        if let Some(ref elem) = self.array_element_type {
            w.write_nested(2, |nw| elem.encode(nw));
        }
        if !self.struct_fields.is_empty() {
            w.write_nested(3, |nw| {
                encode_struct_type_fields(&self.struct_fields, nw);
            });
        }
        if self.type_annotation != SpannerTypeAnnotation::Unspecified {
            w.write_i32(4, self.type_annotation as i32);
        }
        if let Some(ref fqn) = self.proto_type_fqn {
            w.write_str(5, fqn);
        }
    }

    /// Decode a `google.spanner.v1.Type` message from wire bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, SpannerError> {
        let mut r = ProtoReader::new(bytes);
        let mut out = Self::default();
        while let Some((field, wt)) = r.next_tag()? {
            match (field, wt) {
                (1, WireType::Varint) => {
                    out.code = SpannerTypeCode::from_i32(r.read_i32()?);
                }
                (2, WireType::LengthDelimited) => {
                    let sub = r.read_bytes()?;
                    out.array_element_type = Some(Box::new(Self::decode(sub)?));
                }
                (3, WireType::LengthDelimited) => {
                    let sub = r.read_bytes()?;
                    out.struct_fields = decode_struct_type_fields(sub)?;
                }
                (4, WireType::Varint) => {
                    out.type_annotation = SpannerTypeAnnotation::from_i32(r.read_i32()?);
                }
                (5, WireType::LengthDelimited) => {
                    out.proto_type_fqn = Some(r.read_str()?.to_string());
                }
                _ => {
                    r.skip_field(wt)?;
                }
            }
        }
        Ok(out)
    }
}

fn encode_struct_type_fields(fields: &[SpannerStructField], w: &mut ProtoWriter) {
    for f in fields {
        w.write_nested(1, |fw| {
            if !f.name.is_empty() {
                fw.write_str(1, &f.name);
            }
            fw.write_nested(2, |tw| f.field_type.encode(tw));
        });
    }
}

fn decode_struct_type_fields(bytes: &[u8]) -> Result<Vec<SpannerStructField>, SpannerError> {
    let mut r = ProtoReader::new(bytes);
    let mut fields = Vec::new();
    while let Some((field, wt)) = r.next_tag()? {
        if field == 1 && wt == WireType::LengthDelimited {
            let f_bytes = r.read_bytes()?;
            let mut fr = ProtoReader::new(f_bytes);
            let mut name = String::new();
            let mut field_type = SpannerType::default();
            while let Some((ff, fwt)) = fr.next_tag()? {
                match (ff, fwt) {
                    (1, WireType::LengthDelimited) => {
                        name = fr.read_str()?.to_string();
                    }
                    (2, WireType::LengthDelimited) => {
                        let tb = fr.read_bytes()?;
                        field_type = SpannerType::decode(tb)?;
                    }
                    _ => {
                        fr.skip_field(fwt)?;
                    }
                }
            }
            fields.push(SpannerStructField { name, field_type });
        } else {
            r.skip_field(wt)?;
        }
    }
    Ok(fields)
}

/// Wire representation of `google.protobuf.Value` used inside Spanner `PartialResultSet`,
/// `ExecuteSqlRequest.params`, and `Mutation` key/value lists.
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

impl ProtobufValue {
    /// Encode this `google.protobuf.Value` into a [`ProtoWriter`].
    pub fn encode(&self, w: &mut ProtoWriter) {
        match self {
            Self::Null => {
                w.write_varint(1, 0);
            }
            Self::Number(n) => {
                w.write_f64(2, *n);
            }
            Self::String(s) => {
                w.write_str(3, s);
            }
            Self::Bool(b) => {
                w.write_bool(4, *b);
            }
            Self::Struct(entries) => {
                w.write_nested(5, |sw| {
                    encode_protobuf_struct(entries, sw);
                });
            }
            Self::List(items) => {
                w.write_nested(6, |lw| {
                    encode_protobuf_list_value(items, lw);
                });
            }
        }
    }

    /// Decode a `google.protobuf.Value` from wire bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, SpannerError> {
        let mut r = ProtoReader::new(bytes);
        let mut out = Self::Null;
        while let Some((field, wt)) = r.next_tag()? {
            match (field, wt) {
                (1, WireType::Varint) => {
                    let _ = r.read_varint()?;
                    out = Self::Null;
                }
                (2, WireType::Fixed64) => {
                    out = Self::Number(r.read_f64()?);
                }
                (3, WireType::LengthDelimited) => {
                    out = Self::String(r.read_str()?.to_string());
                }
                (4, WireType::Varint) => {
                    out = Self::Bool(r.read_bool()?);
                }
                (5, WireType::LengthDelimited) => {
                    let sub = r.read_bytes()?;
                    out = Self::Struct(decode_protobuf_struct(sub)?);
                }
                (6, WireType::LengthDelimited) => {
                    let sub = r.read_bytes()?;
                    out = Self::List(decode_protobuf_list_value(sub)?);
                }
                _ => {
                    r.skip_field(wt)?;
                }
            }
        }
        Ok(out)
    }

    /// Estimate the in-memory byte footprint of this value (used for bounded `resume_token` buffering).
    pub fn estimated_byte_size(&self) -> usize {
        match self {
            Self::Null | Self::Bool(_) => 8,
            Self::Number(_) => 16,
            Self::String(s) => 24 + s.len(),
            Self::Struct(entries) => {
                24 + entries
                    .iter()
                    .map(|(k, v)| 16 + k.len() + v.estimated_byte_size())
                    .sum::<usize>()
            }
            Self::List(items) => 24 + items.iter().map(Self::estimated_byte_size).sum::<usize>(),
        }
    }

    /// Merge a chunked `ProtobufValue` continuation (`src`) into `self` (`dst`) according to the
    /// Cloud Spanner `PartialResultSet.chunked_value` specification:
    /// - Two `StringValue`s are concatenated directly (before base64, numeric, timestamp, or JSON
    ///   decoding).
    /// - Two `ListValue`s are merged by concatenating their lists, recursively merging the last
    ///   element of the first list with the first element of the second list if and only if both
    ///   are `StringValue`s or both are `ListValue`s.
    pub fn merge_chunked(&mut self, src: ProtobufValue) -> Result<(), SpannerError> {
        match (self, src) {
            (ProtobufValue::String(dst_str), ProtobufValue::String(src_str)) => {
                dst_str.push_str(&src_str);
                Ok(())
            }
            (ProtobufValue::List(dst_list), ProtobufValue::List(mut src_list)) => {
                if !dst_list.is_empty() && !src_list.is_empty() {
                    let should_merge_boundary = matches!(
                        (dst_list.last(), src_list.first()),
                        (
                            Some(ProtobufValue::String(_)),
                            Some(ProtobufValue::String(_))
                        ) | (Some(ProtobufValue::List(_)), Some(ProtobufValue::List(_)))
                    );
                    if should_merge_boundary {
                        let first_src = src_list.remove(0);
                        if let Some(last_dst) = dst_list.last_mut() {
                            last_dst.merge_chunked(first_src)?;
                        }
                    }
                }
                dst_list.extend(src_list);
                Ok(())
            }
            (dst, other) => Err(SpannerError::Protocol(format!(
                "cannot merge chunked PartialResultSet values of incompatible kinds: {dst:?} vs {other:?}"
            ))),
        }
    }
}

pub fn encode_protobuf_struct(entries: &[(String, ProtobufValue)], w: &mut ProtoWriter) {
    for (k, v) in entries {
        w.write_nested(1, |mw| {
            mw.write_str(1, k);
            mw.write_nested(2, |vw| v.encode(vw));
        });
    }
}

pub fn decode_protobuf_struct(bytes: &[u8]) -> Result<Vec<(String, ProtobufValue)>, SpannerError> {
    let mut r = ProtoReader::new(bytes);
    let mut entries = Vec::new();
    while let Some((field, wt)) = r.next_tag()? {
        if field == 1 && wt == WireType::LengthDelimited {
            let entry_bytes = r.read_bytes()?;
            let mut er = ProtoReader::new(entry_bytes);
            let mut key = String::new();
            let mut val = ProtobufValue::Null;
            while let Some((ef, ewt)) = er.next_tag()? {
                match (ef, ewt) {
                    (1, WireType::LengthDelimited) => {
                        key = er.read_str()?.to_string();
                    }
                    (2, WireType::LengthDelimited) => {
                        let vb = er.read_bytes()?;
                        val = ProtobufValue::decode(vb)?;
                    }
                    _ => {
                        er.skip_field(ewt)?;
                    }
                }
            }
            entries.push((key, val));
        } else {
            r.skip_field(wt)?;
        }
    }
    Ok(entries)
}

pub fn encode_protobuf_list_value(items: &[ProtobufValue], w: &mut ProtoWriter) {
    for item in items {
        w.write_nested(1, |vw| item.encode(vw));
    }
}

pub fn decode_protobuf_list_value(bytes: &[u8]) -> Result<Vec<ProtobufValue>, SpannerError> {
    let mut r = ProtoReader::new(bytes);
    let mut items = Vec::new();
    while let Some((field, wt)) = r.next_tag()? {
        if field == 1 && wt == WireType::LengthDelimited {
            let vb = r.read_bytes()?;
            items.push(ProtobufValue::decode(vb)?);
        } else {
            r.skip_field(wt)?;
        }
    }
    Ok(items)
}

/// Decoded Cloud Spanner cell value hydrated after `PartialResultSet` chunk reassembly.
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
    /// Decode a complete (un-chunked) [`ProtobufValue`] using its [`SpannerType`] schema.
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

/// Parse a JSON-serialized array of query parameters (`[{"name": "p1", "type": "INT64", "value": "42"}, ...]`)
/// into [`SpannerQueryParameter`] items.
pub fn parse_spanner_parameters_json(
    params_json: Option<&str>,
) -> Result<Vec<SpannerQueryParameter>, SpannerError> {
    let Some(raw) = params_json.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(Vec::new());
    };
    let parsed: serde_json::Value = serde_json::from_str(raw)
        .map_err(|e| SpannerError::InvalidArgument(format!("invalid params_json: {e}")))?;
    let Some(arr) = parsed.as_array() else {
        return Err(SpannerError::InvalidArgument(
            "params_json must be a JSON array".to_string(),
        ));
    };
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        let name = item
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
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
        let value =
            json_to_protobuf_value(item.get("value").unwrap_or(&serde_json::Value::Null), code);
        out.push(SpannerQueryParameter {
            name,
            value,
            param_type,
        });
    }
    Ok(out)
}

fn json_to_protobuf_value(val: &serde_json::Value, code: SpannerTypeCode) -> ProtobufValue {
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
        serde_json::Value::Object(map) => {
            if code == SpannerTypeCode::Json {
                ProtobufValue::String(serde_json::Value::Object(map.clone()).to_string())
            } else {
                ProtobufValue::Struct(
                    map.iter()
                        .map(|(k, v)| {
                            (
                                k.clone(),
                                json_to_protobuf_value(v, SpannerTypeCode::String),
                            )
                        })
                        .collect(),
                )
            }
        }
    }
}

/// Rewrite Spangres `$1..$N` positional placeholders into `@p1..@pN` outside string
/// literals, quoted identifiers, dollar-quoted strings, and SQL comments, returning
/// `(rewritten_sql, parameter_names_p1_to_pn)`.
pub fn rewrite_spangres_placeholders(sql: &str) -> (String, Vec<String>) {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut out = String::with_capacity(len + 16);
    let mut max_ordinal: usize = 0;
    let mut i = 0;

    while i < len {
        let b = bytes[i];
        if b == b'\'' {
            out.push('\'');
            i += 1;
            while i < len {
                let c = bytes[i];
                out.push(c as char);
                i += 1;
                if c == b'\'' {
                    if i < len && bytes[i] == b'\'' {
                        out.push('\'');
                        i += 1;
                    } else {
                        break;
                    }
                }
            }
            continue;
        }
        if b == b'"' {
            out.push('"');
            i += 1;
            while i < len {
                let c = bytes[i];
                out.push(c as char);
                i += 1;
                if c == b'"' {
                    break;
                }
            }
            continue;
        }
        if b == b'-' && i + 1 < len && bytes[i + 1] == b'-' {
            while i < len && bytes[i] != b'\n' {
                out.push(bytes[i] as char);
                i += 1;
            }
            continue;
        }
        if b == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
            out.push('/');
            out.push('*');
            i += 2;
            while i + 1 < len && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                out.push(bytes[i] as char);
                i += 1;
            }
            if i + 1 < len {
                out.push('*');
                out.push('/');
                i += 2;
            }
            continue;
        }
        if b == b'$' {
            if i + 1 < len && bytes[i + 1].is_ascii_digit() && bytes[i + 1] != b'0' {
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
            } else if i + 1 < len && bytes[i + 1] == b'$' {
                out.push('$');
                out.push('$');
                i += 2;
                while i + 1 < len && !(bytes[i] == b'$' && bytes[i + 1] == b'$') {
                    out.push(bytes[i] as char);
                    i += 1;
                }
                if i + 1 < len {
                    out.push('$');
                    out.push('$');
                    i += 2;
                }
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

/// Token emitted on a multiplexed session to coordinate `ReadWrite` transaction commit state
/// (`google.spanner.v1.MultiplexedSessionPrecommitToken`).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct MultiplexedSessionPrecommitToken {
    /// Opaque precommit token bytes (`field 1`).
    pub precommit_token: Vec<u8>,
    /// Monotonic sequence number assigned by Spanner (`field 2`).
    pub seq_num: i32,
}

impl MultiplexedSessionPrecommitToken {
    /// Encode into a [`ProtoWriter`].
    pub fn encode(&self, w: &mut ProtoWriter) {
        if !self.precommit_token.is_empty() {
            w.write_bytes(1, &self.precommit_token);
        }
        if self.seq_num != 0 {
            w.write_i32(2, self.seq_num);
        }
    }

    /// Decode from wire bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, SpannerError> {
        let mut r = ProtoReader::new(bytes);
        let mut token = Vec::new();
        let mut seq_num = 0i32;
        while let Some((field, wt)) = r.next_tag()? {
            match (field, wt) {
                (1, WireType::LengthDelimited) => {
                    let b = r.read_bytes()?;
                    token = b.to_vec();
                }
                (2, WireType::Varint) => {
                    seq_num = r.read_i32()?;
                }
                _ => {
                    r.skip_field(wt)?;
                }
            }
        }
        if seq_num == 0
            && !token.is_empty()
            && token[0] == 0x0A
            && let Ok(inner) = Self::decode_inner_only(&token)
            && !inner.precommit_token.is_empty()
        {
            return Ok(inner);
        }
        Ok(Self {
            precommit_token: token,
            seq_num,
        })
    }

    fn decode_inner_only(bytes: &[u8]) -> Result<Self, SpannerError> {
        let mut r = ProtoReader::new(bytes);
        let mut token = Vec::new();
        let mut seq_num = 0i32;
        let mut saw_seq = false;
        while let Some((field, wt)) = r.next_tag()? {
            match (field, wt) {
                (1, WireType::LengthDelimited) => {
                    token = r.read_bytes()?.to_vec();
                }
                (2, WireType::Varint) => {
                    seq_num = r.read_i32()?;
                    saw_seq = true;
                }
                _ => return Err(SpannerError::Protocol("not an inner token".to_string())),
            }
        }
        if saw_seq {
            Ok(Self {
                precommit_token: token,
                seq_num,
            })
        } else {
            Err(SpannerError::Protocol("no seq_num".to_string()))
        }
    }
}

/// Update `slot` if `candidate` has a higher (or equal with non-empty token) `seq_num`.
pub fn update_highest_precommit_token(
    slot: &mut Option<MultiplexedSessionPrecommitToken>,
    candidate: Option<MultiplexedSessionPrecommitToken>,
) {
    let Some(cand) = candidate.filter(|c| !c.precommit_token.is_empty()) else {
        return;
    };
    match slot {
        Some(existing) => {
            if cand.seq_num >= existing.seq_num {
                *existing = cand;
            }
        }
        None => {
            *slot = Some(cand);
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

impl SpannerStaleness {
    fn encode_read_only(&self, return_read_timestamp: bool, w: &mut ProtoWriter) {
        match *self {
            Self::Strong => {
                w.write_bool(1, true);
            }
            Self::MinReadTimestampUnixMicros(us) => {
                w.write_nested(2, |tw| encode_timestamp_micros(us, tw));
            }
            Self::MaxStalenessMs(ms) => {
                w.write_nested(3, |dw| encode_duration_ms(ms, dw));
            }
            Self::ReadTimestampUnixMicros(us) => {
                w.write_nested(4, |tw| encode_timestamp_micros(us, tw));
            }
            Self::ExactStalenessMs(ms) => {
                w.write_nested(5, |dw| encode_duration_ms(ms, dw));
            }
        }
        if return_read_timestamp {
            w.write_bool(6, true);
        }
    }
}

fn encode_duration_ms(ms: u64, w: &mut ProtoWriter) {
    let secs = (ms / 1_000) as i64;
    let nanos = ((ms % 1_000) * 1_000_000) as i32;
    if secs != 0 {
        w.write_i64(1, secs);
    }
    if nanos != 0 {
        w.write_i32(2, nanos);
    }
}

fn encode_timestamp_micros(micros: i64, w: &mut ProtoWriter) {
    let secs = micros.div_euclid(1_000_000);
    let nanos = (micros.rem_euclid(1_000_000) * 1_000) as i32;
    if secs != 0 {
        w.write_i64(1, secs);
    }
    if nanos != 0 {
        w.write_i32(2, nanos);
    }
}

fn decode_timestamp_micros(bytes: &[u8]) -> Result<i64, SpannerError> {
    let mut r = ProtoReader::new(bytes);
    let mut secs = 0i64;
    let mut nanos = 0i32;
    while let Some((field, wt)) = r.next_tag()? {
        match (field, wt) {
            (1, WireType::Varint) => secs = r.read_i64()?,
            (2, WireType::Varint) => nanos = r.read_i32()?,
            _ => r.skip_field(wt)?,
        }
    }
    Ok(secs
        .saturating_mul(1_000_000)
        .saturating_add(i64::from(nanos) / 1_000))
}

/// Transaction options (`google.spanner.v1.TransactionOptions`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SpannerTransactionOptions {
    ReadWrite {
        previous_transaction_id: Option<Vec<u8>>,
        exclude_txn_from_change_streams: bool,
    },
    PartitionedDml {
        exclude_txn_from_change_streams: bool,
    },
    ReadOnly {
        staleness: SpannerStaleness,
        return_read_timestamp: bool,
    },
}

impl SpannerTransactionOptions {
    /// Return `true` when this transaction mode requires `x-goog-spanner-route-to-leader: true`.
    #[inline]
    pub const fn requires_leader_routing(&self) -> bool {
        matches!(self, Self::ReadWrite { .. } | Self::PartitionedDml { .. })
    }

    pub fn encode(&self, w: &mut ProtoWriter) {
        match self {
            Self::ReadWrite {
                previous_transaction_id,
                exclude_txn_from_change_streams,
            } => {
                w.write_nested(1, |rw| {
                    if let Some(prev) = previous_transaction_id
                        && !prev.is_empty()
                    {
                        rw.write_bytes(2, prev);
                    }
                });
                if *exclude_txn_from_change_streams {
                    w.write_bool(5, true);
                }
            }
            Self::ReadOnly {
                staleness,
                return_read_timestamp,
            } => {
                w.write_nested(2, |ro| {
                    staleness.encode_read_only(*return_read_timestamp, ro);
                });
            }
            Self::PartitionedDml {
                exclude_txn_from_change_streams,
            } => {
                w.write_nested(3, |_pdml| {});
                if *exclude_txn_from_change_streams {
                    w.write_bool(5, true);
                }
            }
        }
    }
}

/// Selector choosing how a statement binds to a Spanner transaction (`google.spanner.v1.TransactionSelector`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SpannerTransactionSelector {
    SingleUse(SpannerTransactionOptions),
    Id(Vec<u8>),
    Begin(SpannerTransactionOptions),
}

impl SpannerTransactionSelector {
    pub fn encode(&self, w: &mut ProtoWriter) {
        match self {
            Self::SingleUse(opts) => {
                w.write_nested(1, |ow| opts.encode(ow));
            }
            Self::Id(tx_id) => {
                w.write_bytes(2, tx_id);
            }
            Self::Begin(opts) => {
                w.write_nested(3, |ow| opts.encode(ow));
            }
        }
    }
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

    /// Encode this `google.spanner.v1.Mutation` into a [`ProtoWriter`].
    pub fn encode(&self, w: &mut ProtoWriter) {
        match self {
            Self::Insert {
                table,
                columns,
                values,
            } => {
                w.write_nested(1, |mw| encode_write_mutation(table, columns, values, mw));
            }
            Self::Update {
                table,
                columns,
                values,
            } => {
                w.write_nested(2, |mw| encode_write_mutation(table, columns, values, mw));
            }
            Self::InsertOrUpdate {
                table,
                columns,
                values,
            } => {
                w.write_nested(3, |mw| encode_write_mutation(table, columns, values, mw));
            }
            Self::Replace {
                table,
                columns,
                values,
            } => {
                w.write_nested(4, |mw| encode_write_mutation(table, columns, values, mw));
            }
            Self::Delete { table, keys, all } => {
                w.write_nested(5, |dw| {
                    dw.write_str(1, table);
                    dw.write_nested(2, |kw| {
                        for key_tuple in keys {
                            kw.write_nested(1, |lw| encode_protobuf_list_value(key_tuple, lw));
                        }
                        if *all {
                            kw.write_bool(3, true);
                        }
                    });
                });
            }
        }
    }
}

fn encode_write_mutation(
    table: &str,
    columns: &[String],
    values: &[Vec<ProtobufValue>],
    w: &mut ProtoWriter,
) {
    w.write_str(1, table);
    for col in columns {
        w.write_str(2, col);
    }
    for row in values {
        w.write_nested(3, |lw| encode_protobuf_list_value(row, lw));
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

    pub fn encode(&self, w: &mut ProtoWriter) {
        for m in &self.mutations {
            w.write_nested(1, |mw| m.encode(mw));
        }
    }
}

/// Decoded `PartialResultSet` wire frame (`google.spanner.v1.PartialResultSet`).
#[derive(Clone, Debug, Default)]
pub struct PartialResultSetFrame {
    pub row_type: Option<Vec<SpannerStructField>>,
    pub transaction_id: Option<Vec<u8>>,
    pub read_timestamp_micros: Option<i64>,
    pub values: Vec<ProtobufValue>,
    pub chunked_value: bool,
    pub resume_token: Vec<u8>,
    pub row_count_exact: Option<i64>,
    pub row_count_lower_bound: Option<i64>,
    pub precommit_token: Option<MultiplexedSessionPrecommitToken>,
    pub last: bool,
}

impl PartialResultSetFrame {
    /// Decode a `google.spanner.v1.PartialResultSet` message from wire bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, SpannerError> {
        let mut r = ProtoReader::new(bytes);
        let mut out = Self::default();
        while let Some((field, wt)) = r.next_tag()? {
            match (field, wt) {
                (1, WireType::LengthDelimited) => {
                    let meta_bytes = r.read_bytes()?;
                    let (fields, tx_id, read_ts, tx_precommit) =
                        decode_result_set_metadata(meta_bytes)?;
                    if !fields.is_empty() || out.row_type.is_none() {
                        out.row_type = Some(fields);
                    }
                    if tx_id.is_some() {
                        out.transaction_id = tx_id;
                    }
                    if read_ts.is_some() {
                        out.read_timestamp_micros = read_ts;
                    }
                    update_highest_precommit_token(&mut out.precommit_token, tx_precommit);
                }
                (2, WireType::LengthDelimited) => {
                    let vb = r.read_bytes()?;
                    out.values.push(ProtobufValue::decode(vb)?);
                }
                (3, WireType::Varint) => {
                    out.chunked_value = r.read_bool()?;
                }
                (4, WireType::LengthDelimited) => {
                    out.resume_token = r.read_bytes()?.to_vec();
                }
                (5, WireType::LengthDelimited) => {
                    let sb = r.read_bytes()?;
                    let (exact, lower) = decode_result_set_stats(sb)?;
                    out.row_count_exact = exact;
                    out.row_count_lower_bound = lower;
                }
                (8, WireType::LengthDelimited) => {
                    let pb = r.read_bytes()?;
                    let tok = MultiplexedSessionPrecommitToken::decode(pb)?;
                    update_highest_precommit_token(&mut out.precommit_token, Some(tok));
                }
                (9, WireType::Varint) => {
                    out.last = r.read_bool()?;
                }
                _ => {
                    r.skip_field(wt)?;
                }
            }
        }
        Ok(out)
    }
}

type DecodedResultSetMetadata = (
    Vec<SpannerStructField>,
    Option<Vec<u8>>,
    Option<i64>,
    Option<MultiplexedSessionPrecommitToken>,
);

type DecodedTransactionMessage = (
    Vec<u8>,
    Option<i64>,
    Option<MultiplexedSessionPrecommitToken>,
);

fn decode_result_set_metadata(bytes: &[u8]) -> Result<DecodedResultSetMetadata, SpannerError> {
    let mut r = ProtoReader::new(bytes);
    let mut fields = Vec::new();
    let mut tx_id = None;
    let mut read_ts = None;
    let mut precommit = None;
    while let Some((field, wt)) = r.next_tag()? {
        match (field, wt) {
            (1, WireType::LengthDelimited) => {
                let st_bytes = r.read_bytes()?;
                fields = decode_struct_type_fields(st_bytes)?;
            }
            (2, WireType::LengthDelimited) => {
                let tx_bytes = r.read_bytes()?;
                let (id, ts, tok) = decode_transaction_message(tx_bytes)?;
                if !id.is_empty() {
                    tx_id = Some(id);
                }
                read_ts = ts;
                precommit = tok;
            }
            _ => {
                r.skip_field(wt)?;
            }
        }
    }
    Ok((fields, tx_id, read_ts, precommit))
}

fn decode_transaction_message(bytes: &[u8]) -> Result<DecodedTransactionMessage, SpannerError> {
    let mut r = ProtoReader::new(bytes);
    let mut id = Vec::new();
    let mut read_ts = None;
    let mut precommit = None;
    while let Some((field, wt)) = r.next_tag()? {
        match (field, wt) {
            (1, WireType::LengthDelimited) => {
                id = r.read_bytes()?.to_vec();
            }
            (2, WireType::LengthDelimited) => {
                let ts_bytes = r.read_bytes()?;
                read_ts = Some(decode_timestamp_micros(ts_bytes)?);
            }
            (8, WireType::LengthDelimited) => {
                let pb = r.read_bytes()?;
                precommit = Some(MultiplexedSessionPrecommitToken::decode(pb)?);
            }
            _ => {
                r.skip_field(wt)?;
            }
        }
    }
    Ok((id, read_ts, precommit))
}

fn decode_result_set_stats(bytes: &[u8]) -> Result<(Option<i64>, Option<i64>), SpannerError> {
    let mut r = ProtoReader::new(bytes);
    let mut exact = None;
    let mut lower = None;
    while let Some((field, wt)) = r.next_tag()? {
        match (field, wt) {
            (3, WireType::Varint) => {
                exact = Some(r.read_i64()?);
            }
            (4, WireType::Varint) => {
                lower = Some(r.read_i64()?);
            }
            _ => {
                r.skip_field(wt)?;
            }
        }
    }
    Ok((exact, lower))
}

/// Stateful streaming `PartialResultSet` assembler implementing chunk merging and bounded
/// `32 MiB` `resume_token` checkpointing.
#[derive(Clone, Debug, Default)]
pub struct PartialResultSetAssembler {
    pub schema: Vec<SpannerStructField>,
    pub transaction_id: Option<Vec<u8>>,
    pub read_timestamp_micros: Option<i64>,
    pub row_count_exact: Option<i64>,
    pub row_count_lower_bound: Option<i64>,
    pub latest_precommit_token: Option<MultiplexedSessionPrecommitToken>,
    pub last_resume_token: Vec<u8>,
    committed_values: Vec<ProtobufValue>,
    pending_values: Vec<ProtobufValue>,
    pending_bytes: usize,
    chunked_tail: bool,
}

impl PartialResultSetAssembler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Push a decoded [`PartialResultSetFrame`] into the assembler.
    pub fn push_frame(&mut self, mut frame: PartialResultSetFrame) -> Result<(), SpannerError> {
        if let Some(fields) = frame.row_type
            && (!fields.is_empty() || self.schema.is_empty())
        {
            self.schema = fields;
        }
        if let Some(tx_id) = frame.transaction_id {
            self.transaction_id = Some(tx_id);
        }
        if let Some(ts) = frame.read_timestamp_micros {
            self.read_timestamp_micros = Some(ts);
        }
        if let Some(exact) = frame.row_count_exact {
            self.row_count_exact = Some(exact);
        }
        if let Some(lower) = frame.row_count_lower_bound {
            self.row_count_lower_bound = Some(lower);
        }
        update_highest_precommit_token(&mut self.latest_precommit_token, frame.precommit_token);

        if self.chunked_tail && !frame.values.is_empty() {
            let first = frame.values.remove(0);
            if let Some(last_pending) = self.pending_values.last_mut() {
                last_pending.merge_chunked(first)?;
            } else if let Some(last_committed) = self.committed_values.last_mut() {
                last_committed.merge_chunked(first)?;
            } else {
                return Err(SpannerError::Protocol(
                    "received continuation PartialResultSet with no prior chunked value"
                        .to_string(),
                ));
            }
            self.chunked_tail = false;
        }

        for val in frame.values {
            self.pending_bytes = self.pending_bytes.saturating_add(val.estimated_byte_size());
            self.pending_values.push(val);
        }

        self.chunked_tail = frame.chunked_value;

        if !frame.resume_token.is_empty() && !self.chunked_tail {
            self.last_resume_token = frame.resume_token;
            self.committed_values.append(&mut self.pending_values);
            self.pending_bytes = 0;
        } else if self.pending_bytes > MAX_UNCHECKPOINTED_RESUME_BYTES && !self.chunked_tail {
            self.committed_values.append(&mut self.pending_values);
            self.pending_bytes = 0;
            self.last_resume_token.clear();
        }
        Ok(())
    }

    /// Discard any un-checkpointed values collected after `last_resume_token` so the stream can
    /// safely replay from `last_resume_token`.
    pub fn rollback_to_last_resume_token(&mut self) -> Option<Vec<u8>> {
        if self.last_resume_token.is_empty() {
            return None;
        }
        self.pending_values.clear();
        self.pending_bytes = 0;
        self.chunked_tail = false;
        Some(self.last_resume_token.clone())
    }

    /// Finalize the stream and decode all assembled [`ProtobufValue`] cells into rows of [`SpannerValue`].
    pub fn finish(mut self) -> Result<SpannerQueryResult, SpannerError> {
        if self.chunked_tail {
            return Err(SpannerError::Protocol(
                "PartialResultSet stream ended while chunked_value was still true".to_string(),
            ));
        }
        self.committed_values.append(&mut self.pending_values);
        let num_cols = self.schema.len();
        let columns: Vec<String> = self.schema.iter().map(|f| f.name.clone()).collect();
        let mut rows = Vec::new();
        if num_cols > 0 {
            if !self.committed_values.len().is_multiple_of(num_cols) {
                return Err(SpannerError::Protocol(format!(
                    "PartialResultSet total value count ({}) is not a multiple of schema column count ({num_cols})",
                    self.committed_values.len()
                )));
            }
            for row_slice in self.committed_values.chunks(num_cols) {
                let mut row = Vec::with_capacity(num_cols);
                for (col_idx, raw_val) in row_slice.iter().enumerate() {
                    let col_type = &self.schema[col_idx].field_type;
                    row.push(SpannerValue::from_protobuf_value(raw_val, col_type)?);
                }
                rows.push(row);
            }
        }
        Ok(SpannerQueryResult {
            columns,
            schema: self.schema,
            rows,
            transaction_id: self.transaction_id,
            read_timestamp_micros: self.read_timestamp_micros,
            row_count_exact: self.row_count_exact,
            row_count_lower_bound: self.row_count_lower_bound,
            precommit_token: self.latest_precommit_token,
        })
    }
}

/// Complete decoded result set returned by `ExecuteStreamingSql` / `ExecuteSql`.
#[derive(Clone, Debug, Default)]
pub struct SpannerQueryResult {
    pub columns: Vec<String>,
    pub schema: Vec<SpannerStructField>,
    pub rows: Vec<Vec<SpannerValue>>,
    pub transaction_id: Option<Vec<u8>>,
    pub read_timestamp_micros: Option<i64>,
    pub row_count_exact: Option<i64>,
    pub row_count_lower_bound: Option<i64>,
    pub precommit_token: Option<MultiplexedSessionPrecommitToken>,
}

/// Cached multiplexed session handle (`google.spanner.v1.Session`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpannerSessionHandle {
    pub name: String,
    pub database_role: Option<String>,
    pub multiplexed: bool,
    pub created_at_unix_ms: i64,
}

impl SpannerSessionHandle {
    /// Return `true` when this session has exceeded the 7-day rotation window.
    pub fn should_rotate(&self, now_ms: i64) -> bool {
        now_ms.saturating_sub(self.created_at_unix_ms) >= MULTIPLEXED_SESSION_ROTATION_MS
    }
}

/// Mutable state for a multiplexed `ReadWrite` transaction.
#[derive(Clone, Debug, Default)]
pub struct SpannerReadWriteTransaction {
    pub transaction_id: Option<Vec<u8>>,
    pub previous_transaction_id: Option<Vec<u8>>,
    pub seqno: i64,
    pub latest_precommit_token: Option<MultiplexedSessionPrecommitToken>,
    pub mutations: Vec<SpannerMutation>,
    pub transaction_tag: Option<String>,
    pub exclude_txn_from_change_streams: bool,
}

impl SpannerReadWriteTransaction {
    /// Create a new `ReadWrite` transaction state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a retry `ReadWrite` transaction preserving wound-wait priority from `aborted_tx_id`.
    pub fn retry_from_aborted(aborted_tx_id: Option<Vec<u8>>) -> Self {
        Self {
            transaction_id: None,
            previous_transaction_id: aborted_tx_id,
            seqno: 0,
            latest_precommit_token: None,
            mutations: Vec::new(),
            transaction_tag: None,
            exclude_txn_from_change_streams: false,
        }
    }

    /// Advance and return the next 1-based statement `seqno`.
    pub fn next_seqno(&mut self) -> i64 {
        self.seqno += 1;
        self.seqno
    }

    /// Buffer a [`SpannerMutation`] to be sent on `Commit`.
    pub fn add_mutation(&mut self, mutation: SpannerMutation) {
        self.mutations.push(mutation);
    }

    /// Build the [`SpannerTransactionSelector`] for the next statement (using inline `Begin` on
    /// statement #1 when `transaction_id` is not yet assigned).
    pub fn selector_for_statement(&self) -> SpannerTransactionSelector {
        if let Some(ref id) = self.transaction_id {
            SpannerTransactionSelector::Id(id.clone())
        } else {
            SpannerTransactionSelector::Begin(SpannerTransactionOptions::ReadWrite {
                previous_transaction_id: self.previous_transaction_id.clone(),
                exclude_txn_from_change_streams: self.exclude_txn_from_change_streams,
            })
        }
    }

    /// Record any newly returned `transaction_id` and `precommit_token`.
    pub fn observe_response(
        &mut self,
        tx_id: Option<Vec<u8>>,
        precommit_token: Option<MultiplexedSessionPrecommitToken>,
    ) {
        if let Some(id) = tx_id
            && !id.is_empty()
        {
            self.transaction_id = Some(id);
        }
        update_highest_precommit_token(&mut self.latest_precommit_token, precommit_token);
    }
}

/// Statement item inside `ExecuteBatchDmlRequest`.
#[derive(Clone, Debug)]
pub struct SpannerBatchDmlStatement {
    pub sql: String,
    pub params: Vec<SpannerQueryParameter>,
}

/// Result of `ExecuteBatchDml`.
#[derive(Clone, Debug)]
pub struct SpannerBatchDmlResult {
    pub row_counts: Vec<i64>,
    pub transaction_id: Option<Vec<u8>>,
    pub precommit_token: Option<MultiplexedSessionPrecommitToken>,
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

/// Pure-Rust Google Cloud Spanner v1 client.
pub struct SpannerClient {
    config: SpannerClientConfig,
    token_cache: Option<Arc<GcpTokenCache>>,
    grpc_pool: GrpcChannelPool,
    cached_session: RwLock<Option<Arc<SpannerSessionHandle>>>,
    client_id: u64,
    rand64_hex: String,
    req_seq: AtomicU64,
}

impl SpannerClient {
    /// Construct a new [`SpannerClient`].
    pub fn new(config: SpannerClientConfig) -> Result<Self, SpannerError> {
        if config.database.trim().is_empty() {
            return Err(SpannerError::InvalidArgument(
                "SpannerClientConfig.database must not be empty".to_string(),
            ));
        }
        let endpoint = config.effective_endpoint();
        let mut pool_cfg = GrpcPoolConfig::new(endpoint);
        pool_cfg.pool_size = config.pool_size.max(1);
        pool_cfg.connect_timeout = Duration::from_secs(10);
        pool_cfg.user_agent = "vertebra-spanner/0.1".to_string();
        pool_cfg.ca_pem = config.ca_pem.clone();
        let grpc_pool =
            GrpcChannelPool::new(pool_cfg).map_err(|e| SpannerError::Protocol(e.message))?;

        let token_cache = if config.is_cleartext() && config.auth_options.is_empty() {
            None
        } else {
            let mut opts = config.auth_options.clone();
            opts.entry("scopes".to_string()).or_insert_with(|| {
                "https://www.googleapis.com/auth/spanner.data,https://www.googleapis.com/auth/cloud-platform".to_string()
            });
            GcpTokenCache::from_options(&opts)
        };

        let uuid = generate_request_uuid_v4().replace('-', "");
        let rand64_hex = uuid.chars().take(16).collect::<String>();
        let client_id = NEXT_SPANNER_CLIENT_ID.fetch_add(1, Ordering::Relaxed);

        Ok(Self {
            config,
            token_cache,
            grpc_pool,
            cached_session: RwLock::new(None),
            client_id,
            rand64_hex,
            req_seq: AtomicU64::new(1),
        })
    }

    /// Client configuration reference.
    #[inline]
    pub fn config(&self) -> &SpannerClientConfig {
        &self.config
    }

    fn next_request_sequence(&self) -> u64 {
        self.req_seq.fetch_add(1, Ordering::Relaxed)
    }

    async fn bearer_token_and_quota(
        &self,
    ) -> Result<(Option<String>, Option<String>), SpannerError> {
        if self.config.is_cleartext() && self.config.auth_options.is_empty() {
            return Ok((None, None));
        }
        if let Some(tok) = self.config.auth_options.get("access_token")
            && !tok.trim().is_empty()
        {
            let qp = self
                .config
                .auth_options
                .get("quota_project_id")
                .cloned()
                .filter(|s| !s.is_empty());
            return Ok((Some(tok.trim().to_string()), qp));
        }
        if let Some(ref cache) = self.token_cache {
            let snap = cache
                .get_or_refresh()
                .await
                .map_err(|e| SpannerError::Auth(e.to_string()))?;
            return Ok((
                Some(snap.access_token.clone()),
                snap.quota_project_id.clone(),
            ));
        }
        Ok((None, None))
    }

    async fn build_metadata(
        &self,
        route_to_leader: bool,
        req_seq: u64,
        attempt: u32,
    ) -> Result<GrpcRequestMetadata, SpannerError> {
        let (bearer_token, quota_project) = self.bearer_token_and_quota().await?;
        let pool_size = self.config.pool_size.max(1) as u64;
        let channel_id = ((req_seq.saturating_sub(1)) % pool_size) + 1;
        let spanner_request_id = format!(
            "1.{}.{}.{}.{}.{}",
            self.rand64_hex, self.client_id, channel_id, req_seq, attempt
        );
        let mut extra_headers = vec![
            (
                "google-cloud-resource-prefix".to_string(),
                self.config.database.clone(),
            ),
            (
                "x-goog-spanner-end-to-end-tracing".to_string(),
                "true".to_string(),
            ),
            (
                "x-goog-spanner-enable-afe-server-timing".to_string(),
                "true".to_string(),
            ),
        ];
        if route_to_leader && self.config.route_to_leader_enabled {
            extra_headers.push((
                "x-goog-spanner-route-to-leader".to_string(),
                "true".to_string(),
            ));
        }
        Ok(GrpcRequestMetadata {
            bearer_token,
            routing_params: Some(format!("database={}", self.config.database)),
            quota_project,
            spanner_request_id: Some(spanner_request_id),
            timeout: Some(Duration::from_secs(60)),
            compress_gzip: false,
            extra_headers,
        })
    }

    /// Return the cached multiplexed session handle, creating or rotating it via
    /// `/google.spanner.v1.Spanner/CreateSession` when absent, expired (`>= 7` days), or forced.
    pub async fn get_or_create_multiplexed_session(
        &self,
        force_recreate: bool,
    ) -> Result<Arc<SpannerSessionHandle>, SpannerError> {
        let now_ms = current_unix_ms();
        if !force_recreate
            && let Ok(guard) = self.cached_session.read()
            && let Some(ref existing) = *guard
            && !existing.should_rotate(now_ms)
        {
            return Ok(Arc::clone(existing));
        }

        let mut req = ProtoWriter::with_capacity(128);
        req.write_str(1, &self.config.database);
        req.write_nested(2, |sw| {
            if let Some(ref role) = self.config.database_role
                && !role.is_empty()
            {
                sw.write_str(4, role);
            }
            sw.write_bool(6, true);
        });

        let req_seq = self.next_request_sequence();
        let meta = self.build_metadata(true, req_seq, 1).await?;
        let (resp_bytes, _) = self
            .grpc_pool
            .unary(
                "/google.spanner.v1.Spanner/CreateSession",
                req.finish(),
                &meta,
            )
            .await?;

        let mut r = ProtoReader::new(&resp_bytes);
        let mut session_name = String::new();
        let mut multiplexed = true;
        while let Some((field, wt)) = r.next_tag()? {
            match (field, wt) {
                (1, WireType::LengthDelimited) => {
                    session_name = r.read_str()?.to_string();
                }
                (6, WireType::Varint) => {
                    multiplexed = r.read_bool()?;
                }
                _ => {
                    r.skip_field(wt)?;
                }
            }
        }
        if session_name.is_empty() {
            return Err(SpannerError::Protocol(
                "CreateSession response returned an empty session name".to_string(),
            ));
        }

        let handle = Arc::new(SpannerSessionHandle {
            name: session_name,
            database_role: self.config.database_role.clone(),
            multiplexed,
            created_at_unix_ms: now_ms,
        });
        if let Ok(mut guard) = self.cached_session.write() {
            *guard = Some(Arc::clone(&handle));
        }
        Ok(handle)
    }

    fn is_session_not_found(err: &SpannerError) -> bool {
        match err {
            SpannerError::Grpc(status) => {
                status.code == GrpcCode::NotFound
                    && (status.message.to_ascii_lowercase().contains("session")
                        || status.details.iter().any(|d| match d {
                            GrpcErrorDetail::ResourceInfo { resource_type, .. } => {
                                resource_type.to_ascii_lowercase().contains("session")
                            }
                            _ => false,
                        }))
            }
            _ => false,
        }
    }

    /// Explicitly start a transaction via `/google.spanner.v1.Spanner/BeginTransaction`.
    pub async fn begin_transaction(
        &self,
        options: &SpannerTransactionOptions,
        mutation_key: Option<&SpannerMutation>,
        transaction_tag: Option<&str>,
    ) -> Result<
        (
            Vec<u8>,
            Option<i64>,
            Option<MultiplexedSessionPrecommitToken>,
        ),
        SpannerError,
    > {
        let mut recreated = false;
        loop {
            let session = self.get_or_create_multiplexed_session(recreated).await?;
            let mut w = ProtoWriter::with_capacity(256);
            w.write_str(1, &session.name);
            w.write_nested(2, |ow| options.encode(ow));
            if let Some(tag) = transaction_tag
                && !tag.is_empty()
            {
                w.write_nested(3, |rw| {
                    rw.write_str(3, tag);
                });
            }
            if let Some(mkey) = mutation_key {
                w.write_nested(4, |mw| mkey.encode(mw));
            }
            let req_seq = self.next_request_sequence();
            let meta = self
                .build_metadata(options.requires_leader_routing(), req_seq, 1)
                .await?;
            match self
                .grpc_pool
                .unary(
                    "/google.spanner.v1.Spanner/BeginTransaction",
                    w.finish(),
                    &meta,
                )
                .await
                .map_err(SpannerError::from)
            {
                Ok((resp_bytes, _)) => {
                    let (id, read_ts, tok) = decode_transaction_message(&resp_bytes)?;
                    if id.is_empty() {
                        return Err(SpannerError::Protocol(
                            "BeginTransaction returned an empty transaction id".to_string(),
                        ));
                    }
                    return Ok((id, read_ts, tok));
                }
                Err(err) if !recreated && Self::is_session_not_found(&err) => {
                    recreated = true;
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Execute a streaming SQL query or DML statement via `/google.spanner.v1.Spanner/ExecuteStreamingSql`,
    /// reassembling `PartialResultSet` chunks and replaying from `resume_token` on transient
    /// `UNAVAILABLE` / `INTERNAL` stream drops.
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_streaming_sql(
        &self,
        sql: &str,
        params: &[SpannerQueryParameter],
        selector: &SpannerTransactionSelector,
        seqno: i64,
        partition_token: Option<&[u8]>,
        data_boost_enabled: bool,
        route_to_leader: bool,
        request_tag: Option<&str>,
        transaction_tag: Option<&str>,
    ) -> Result<SpannerQueryResult, SpannerError> {
        let (effective_sql, _) =
            if self.config.dialect == SpannerDialect::GoogleSql && sql.contains('$') {
                rewrite_spangres_placeholders(sql)
            } else {
                (sql.to_string(), Vec::new())
            };

        let mut recreated_session = false;
        let mut assembler = PartialResultSetAssembler::new();
        let mut current_resume_token: Vec<u8> = Vec::new();
        let mut attempt: u32 = 1;
        let req_seq = self.next_request_sequence();

        loop {
            let session = self
                .get_or_create_multiplexed_session(recreated_session)
                .await?;
            let payload = encode_execute_sql_request(
                &session.name,
                selector,
                &effective_sql,
                params,
                &current_resume_token,
                partition_token,
                seqno,
                data_boost_enabled,
                request_tag,
                transaction_tag,
            );
            let meta = self
                .build_metadata(route_to_leader, req_seq, attempt)
                .await?;

            let stream_res = self
                .grpc_pool
                .server_streaming(
                    "/google.spanner.v1.Spanner/ExecuteStreamingSql",
                    payload,
                    &meta,
                )
                .await
                .map_err(SpannerError::from);

            let mut stream = match stream_res {
                Ok(s) => s,
                Err(err) if !recreated_session && Self::is_session_not_found(&err) => {
                    recreated_session = true;
                    continue;
                }
                Err(err) => return Err(err),
            };

            let mut stream_interrupted: Option<GrpcStatus> = None;
            loop {
                match stream.next_message().await {
                    Ok(Some(frame_bytes)) => {
                        let frame = PartialResultSetFrame::decode(&frame_bytes)?;
                        assembler.push_frame(frame)?;
                    }
                    Ok(None) => break,
                    Err(status) => {
                        stream_interrupted = Some(status);
                        break;
                    }
                }
            }

            if let Some(status) = stream_interrupted {
                let err = SpannerError::Grpc(status.clone());
                if !recreated_session && Self::is_session_not_found(&err) {
                    recreated_session = true;
                    assembler = PartialResultSetAssembler::new();
                    current_resume_token.clear();
                    continue;
                }
                if matches!(status.code, GrpcCode::Unavailable | GrpcCode::Internal)
                    && attempt < 4
                    && let Some(tok) = assembler.rollback_to_last_resume_token()
                {
                    current_resume_token = tok;
                    attempt += 1;
                    continue;
                }
                return Err(err);
            }

            return assembler.finish();
        }
    }

    /// Execute a read-only query with the specified [`SpannerStaleness`] bound (`route_to_leader = false`).
    pub async fn query_read_only(
        &self,
        sql: &str,
        params: &[SpannerQueryParameter],
        staleness: SpannerStaleness,
    ) -> Result<SpannerQueryResult, SpannerError> {
        let selector = SpannerTransactionSelector::SingleUse(SpannerTransactionOptions::ReadOnly {
            staleness,
            return_read_timestamp: staleness != SpannerStaleness::Strong,
        });
        self.execute_streaming_sql(sql, params, &selector, 0, None, false, false, None, None)
            .await
    }

    /// Execute a statement inside a [`SpannerReadWriteTransaction`], using inline `Begin` on
    /// statement #1 and falling back to explicit `BeginTransaction` if statement #1 fails before
    /// returning a transaction ID.
    pub async fn execute_in_read_write_tx(
        &self,
        tx: &mut SpannerReadWriteTransaction,
        sql: &str,
        params: &[SpannerQueryParameter],
        request_tag: Option<&str>,
    ) -> Result<SpannerQueryResult, SpannerError> {
        let was_first_stmt = tx.transaction_id.is_none();
        let selector = tx.selector_for_statement();
        let seqno = tx.next_seqno();
        let tx_tag = tx.transaction_tag.clone();

        match self
            .execute_streaming_sql(
                sql,
                params,
                &selector,
                seqno,
                None,
                false,
                true,
                request_tag,
                tx_tag.as_deref(),
            )
            .await
        {
            Ok(res) => {
                tx.observe_response(res.transaction_id.clone(), res.precommit_token.clone());
                if tx.transaction_id.is_none() && was_first_stmt {
                    let opts = SpannerTransactionOptions::ReadWrite {
                        previous_transaction_id: tx.previous_transaction_id.clone(),
                        exclude_txn_from_change_streams: tx.exclude_txn_from_change_streams,
                    };
                    let (id, _, tok) = self
                        .begin_transaction(&opts, None, tx_tag.as_deref())
                        .await?;
                    tx.observe_response(Some(id), tok);
                }
                Ok(res)
            }
            Err(err) => Err(err),
        }
    }

    /// Execute a batch of DML statements inside a [`SpannerReadWriteTransaction`] via
    /// `/google.spanner.v1.Spanner/ExecuteBatchDml`, inspecting the embedded `ExecuteBatchDmlResponse.status`.
    pub async fn execute_batch_dml(
        &self,
        tx: &mut SpannerReadWriteTransaction,
        statements: &[SpannerBatchDmlStatement],
        request_tag: Option<&str>,
    ) -> Result<SpannerBatchDmlResult, SpannerError> {
        if statements.is_empty() {
            return Ok(SpannerBatchDmlResult {
                row_counts: Vec::new(),
                transaction_id: tx.transaction_id.clone(),
                precommit_token: tx.latest_precommit_token.clone(),
            });
        }
        let session = self.get_or_create_multiplexed_session(false).await?;
        let selector = tx.selector_for_statement();
        let seqno = tx.next_seqno();

        let mut w = ProtoWriter::with_capacity(512);
        w.write_str(1, &session.name);
        w.write_nested(2, |tw| selector.encode(tw));
        for stmt in statements {
            w.write_nested(3, |sw| {
                sw.write_str(1, &stmt.sql);
                if !stmt.params.is_empty() {
                    let entries: Vec<(String, ProtobufValue)> = stmt
                        .params
                        .iter()
                        .map(|p| (p.name.clone(), p.value.clone()))
                        .collect();
                    sw.write_nested(2, |pw| encode_protobuf_struct(&entries, pw));
                    for p in &stmt.params {
                        sw.write_nested(3, |mw| {
                            mw.write_str(1, &p.name);
                            mw.write_nested(2, |tw| p.param_type.encode(tw));
                        });
                    }
                }
            });
        }
        w.write_i64(4, seqno);
        if request_tag.is_some() || tx.transaction_tag.is_some() {
            w.write_nested(5, |rw| {
                if let Some(rt) = request_tag
                    && !rt.is_empty()
                {
                    rw.write_str(2, rt);
                }
                if let Some(ref tt) = tx.transaction_tag
                    && !tt.is_empty()
                {
                    rw.write_str(3, tt);
                }
            });
        }

        let req_seq = self.next_request_sequence();
        let meta = self.build_metadata(true, req_seq, 1).await?;
        let (resp_bytes, _) = self
            .grpc_pool
            .unary(
                "/google.spanner.v1.Spanner/ExecuteBatchDml",
                w.finish(),
                &meta,
            )
            .await?;

        let mut r = ProtoReader::new(&resp_bytes);
        let mut row_counts = Vec::new();
        let mut embedded_status: Option<GrpcStatus> = None;
        while let Some((field, wt)) = r.next_tag()? {
            match (field, wt) {
                (1, WireType::LengthDelimited) => {
                    let rs_bytes = r.read_bytes()?;
                    let (count, tx_id, tok) = decode_batch_dml_result_set(rs_bytes)?;
                    row_counts.push(count);
                    tx.observe_response(tx_id, tok);
                }
                (2, WireType::LengthDelimited) => {
                    let st_bytes = r.read_bytes()?;
                    let st = decode_embedded_rpc_status(st_bytes)?;
                    if st.code != GrpcCode::Ok {
                        embedded_status = Some(st);
                    }
                }
                (3, WireType::LengthDelimited) => {
                    let pb = r.read_bytes()?;
                    let tok = MultiplexedSessionPrecommitToken::decode(pb)?;
                    tx.observe_response(None, Some(tok));
                }
                _ => {
                    r.skip_field(wt)?;
                }
            }
        }

        if let Some(status) = embedded_status {
            return Err(SpannerError::Grpc(status));
        }

        Ok(SpannerBatchDmlResult {
            row_counts,
            transaction_id: tx.transaction_id.clone(),
            precommit_token: tx.latest_precommit_token.clone(),
        })
    }

    /// Commit a [`SpannerReadWriteTransaction`] via `/google.spanner.v1.Spanner/Commit`,
    /// handling mutation-only `BeginTransaction(mutation_key)` and `CommitResponse.MultiplexedSessionRetry`.
    pub async fn commit_read_write_tx(
        &self,
        tx: &mut SpannerReadWriteTransaction,
    ) -> Result<SpannerCommitResult, SpannerError> {
        if tx.transaction_id.is_none() {
            let opts = SpannerTransactionOptions::ReadWrite {
                previous_transaction_id: tx.previous_transaction_id.clone(),
                exclude_txn_from_change_streams: tx.exclude_txn_from_change_streams,
            };
            let mutation_key = tx.mutations.first();
            let (id, _, tok) = self
                .begin_transaction(&opts, mutation_key, tx.transaction_tag.as_deref())
                .await?;
            tx.observe_response(Some(id), tok);
        }

        let tx_id = tx.transaction_id.clone().ok_or_else(|| {
            SpannerError::Protocol(
                "cannot commit ReadWrite transaction without transaction_id".to_string(),
            )
        })?;
        let session = self.get_or_create_multiplexed_session(false).await?;
        let req_seq = self.next_request_sequence();
        let mut retry_count = 0u32;
        let mut send_mutations = true;

        loop {
            let mut w = ProtoWriter::with_capacity(256);
            w.write_str(1, &session.name);
            w.write_bytes(2, &tx_id);
            if send_mutations {
                for m in &tx.mutations {
                    w.write_nested(4, |mw| m.encode(mw));
                }
            }
            w.write_bool(5, true);
            if let Some(ref tag) = tx.transaction_tag
                && !tag.is_empty()
            {
                w.write_nested(6, |rw| {
                    rw.write_str(3, tag);
                });
            }
            if let Some(delay_ms) = self.config.max_commit_delay_ms
                && delay_ms > 0
            {
                w.write_nested(8, |dw| encode_duration_ms(delay_ms, dw));
            }
            if let Some(ref precommit) = tx.latest_precommit_token
                && !precommit.precommit_token.is_empty()
            {
                w.write_nested(12, |pw| precommit.encode(pw));
            }

            let meta = self.build_metadata(true, req_seq, retry_count + 1).await?;
            let (resp_bytes, _) = self
                .grpc_pool
                .unary("/google.spanner.v1.Spanner/Commit", w.finish(), &meta)
                .await?;

            let (commit_ts, mutation_count, retry_precommit) = decode_commit_response(&resp_bytes)?;
            if let Some(new_token) = retry_precommit {
                tx.observe_response(None, Some(new_token));
                send_mutations = false;
                retry_count += 1;
                if retry_count <= 4 {
                    continue;
                }
            }
            return Ok(SpannerCommitResult {
                commit_timestamp_micros: commit_ts,
                mutation_count,
                retry_count,
            });
        }
    }

    /// Roll back an active `ReadWrite` transaction via `/google.spanner.v1.Spanner/Rollback`.
    pub async fn rollback(&self, transaction_id: &[u8]) -> Result<(), SpannerError> {
        if transaction_id.is_empty() {
            return Ok(());
        }
        let session = self.get_or_create_multiplexed_session(false).await?;
        let mut w = ProtoWriter::with_capacity(128);
        w.write_str(1, &session.name);
        w.write_bytes(2, transaction_id);
        let req_seq = self.next_request_sequence();
        let meta = self.build_metadata(true, req_seq, 1).await?;
        let _ = self
            .grpc_pool
            .unary("/google.spanner.v1.Spanner/Rollback", w.finish(), &meta)
            .await?;
        Ok(())
    }

    /// Execute an async closure inside a multiplexed `ReadWrite` transaction, automatically
    /// retrying on `GrpcCode::Aborted` with `RetryInfo` backoff and preserving wound-wait priority
    /// via `ReadWrite.multiplexed_session_previous_transaction_id`.
    pub async fn run_in_read_write_transaction<F, Fut, T>(
        &self,
        max_attempts: u32,
        mut op: F,
    ) -> Result<(T, SpannerCommitResult), SpannerError>
    where
        F: FnMut(SpannerReadWriteTransaction) -> Fut,
        Fut: std::future::Future<
                Output = Result<(T, SpannerReadWriteTransaction), (SpannerError, Option<Vec<u8>>)>,
            >,
    {
        let mut prev_tx_id: Option<Vec<u8>> = None;
        let limit = max_attempts.max(1);
        for attempt in 1..=limit {
            let tx = SpannerReadWriteTransaction::retry_from_aborted(prev_tx_id.clone());
            match op(tx).await {
                Ok((val, mut completed_tx)) => {
                    match self.commit_read_write_tx(&mut completed_tx).await {
                        Ok(commit_res) => return Ok((val, commit_res)),
                        Err(err) => {
                            if let Some(delay_ms) = extract_aborted_retry_delay_ms(&err)
                                && attempt < limit
                            {
                                if completed_tx.transaction_id.is_some() {
                                    prev_tx_id = completed_tx.transaction_id;
                                }
                                tokio::time::sleep(Duration::from_millis(delay_ms.min(500))).await;
                                continue;
                            }
                            return Err(err);
                        }
                    }
                }
                Err((err, aborted_id)) => {
                    if let Some(delay_ms) = extract_aborted_retry_delay_ms(&err)
                        && attempt < limit
                    {
                        if aborted_id.is_some() {
                            prev_tx_id = aborted_id;
                        }
                        tokio::time::sleep(Duration::from_millis(delay_ms.min(500))).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        Err(SpannerError::Protocol(
            "ReadWrite transaction exceeded max_attempts".to_string(),
        ))
    }

    /// Execute a Partitioned DML (`PDML`) statement (`BeginTransaction(PartitionedDml)` +
    /// `ExecuteStreamingSql`) and return `row_count_lower_bound`.
    pub async fn execute_partitioned_dml(
        &self,
        sql: &str,
        params: &[SpannerQueryParameter],
        exclude_txn_from_change_streams: bool,
    ) -> Result<i64, SpannerError> {
        let opts = SpannerTransactionOptions::PartitionedDml {
            exclude_txn_from_change_streams,
        };
        let (tx_id, _, _) = self.begin_transaction(&opts, None, None).await?;
        let selector = SpannerTransactionSelector::Id(tx_id);
        let res = self
            .execute_streaming_sql(sql, params, &selector, 1, None, false, true, None, None)
            .await?;
        Ok(res
            .row_count_lower_bound
            .or(res.row_count_exact)
            .unwrap_or(0))
    }

    /// Execute `/google.spanner.v1.Spanner/BatchWrite` across `mutation_groups`, chunking batches so
    /// each RPC stays at or below [`MAX_MUTATION_CELLS_PER_BATCH`] (`80,000` cells).
    pub async fn batch_write(
        &self,
        mutation_groups: &[SpannerMutationGroup],
        exclude_txn_from_change_streams: bool,
    ) -> Result<Vec<SpannerBatchWriteResult>, SpannerError> {
        if mutation_groups.is_empty() {
            return Ok(Vec::new());
        }
        let session = self.get_or_create_multiplexed_session(false).await?;
        let mut results = Vec::new();
        let mut chunk: Vec<&SpannerMutationGroup> = Vec::new();
        let mut chunk_cells: usize = 0;

        for group in mutation_groups {
            let group_cells = group.cell_count();
            if !chunk.is_empty() && chunk_cells + group_cells > MAX_MUTATION_CELLS_PER_BATCH {
                let mut part = self
                    .batch_write_chunk(&session.name, &chunk, exclude_txn_from_change_streams)
                    .await?;
                results.append(&mut part);
                chunk.clear();
                chunk_cells = 0;
            }
            chunk.push(group);
            chunk_cells += group_cells;
        }
        if !chunk.is_empty() {
            let mut part = self
                .batch_write_chunk(&session.name, &chunk, exclude_txn_from_change_streams)
                .await?;
            results.append(&mut part);
        }
        Ok(results)
    }

    async fn batch_write_chunk(
        &self,
        session_name: &str,
        groups: &[&SpannerMutationGroup],
        exclude_txn_from_change_streams: bool,
    ) -> Result<Vec<SpannerBatchWriteResult>, SpannerError> {
        let mut w = ProtoWriter::with_capacity(512);
        w.write_str(1, session_name);
        for g in groups {
            w.write_nested(4, |gw| g.encode(gw));
        }
        if exclude_txn_from_change_streams {
            w.write_bool(5, true);
        }
        let req_seq = self.next_request_sequence();
        let meta = self.build_metadata(true, req_seq, 1).await?;
        let mut stream = self
            .grpc_pool
            .server_streaming("/google.spanner.v1.Spanner/BatchWrite", w.finish(), &meta)
            .await?;

        let mut out = Vec::new();
        while let Some(msg) = stream.next_message().await? {
            out.push(decode_batch_write_response(&msg)?);
        }
        Ok(out)
    }

    /// Execute batched DDL statements in a single `/google.spanner.admin.database.v1.DatabaseAdmin/UpdateDatabaseDdl`
    /// long-running operation and poll `/google.longrunning.Operations/GetOperation` until `done == true`.
    pub async fn update_database_ddl(&self, statements: &[String]) -> Result<String, SpannerError> {
        if statements.is_empty() {
            return Ok(String::new());
        }
        let mut w = ProtoWriter::with_capacity(256);
        w.write_str(1, &self.config.database);
        for stmt in statements {
            w.write_str(2, stmt);
        }
        let req_seq = self.next_request_sequence();
        let meta = self.build_metadata(true, req_seq, 1).await?;
        let (resp_bytes, _) = self
            .grpc_pool
            .unary(
                "/google.spanner.admin.database.v1.DatabaseAdmin/UpdateDatabaseDdl",
                w.finish(),
                &meta,
            )
            .await?;

        let (mut op_name, mut done, mut op_err) = decode_lro_operation(&resp_bytes)?;
        if let Some(status) = op_err {
            return Err(SpannerError::Grpc(status));
        }
        let mut polls = 0u32;
        while !done && !op_name.is_empty() && polls < 120 {
            polls += 1;
            tokio::time::sleep(Duration::from_millis(10)).await;
            let mut get_w = ProtoWriter::with_capacity(64);
            get_w.write_str(1, &op_name);
            let poll_seq = self.next_request_sequence();
            let poll_meta = self.build_metadata(false, poll_seq, 1).await?;
            let (poll_bytes, _) = self
                .grpc_pool
                .unary(
                    "/google.longrunning.Operations/GetOperation",
                    get_w.finish(),
                    &poll_meta,
                )
                .await?;
            let (next_name, next_done, next_err) = decode_lro_operation(&poll_bytes)?;
            if !next_name.is_empty() {
                op_name = next_name;
            }
            done = next_done;
            op_err = next_err;
            if let Some(status) = op_err {
                return Err(SpannerError::Grpc(status));
            }
        }
        Ok(op_name)
    }

    /// Execute `/google.spanner.v1.Spanner/PartitionQuery` and fan out concurrent
    /// `/google.spanner.v1.Spanner/ExecuteStreamingSql` streams (with `data_boost_enabled`) into a
    /// zero-copy `VTBARROW` shared-memory segment via [`ShmArrowStreamWriter`].
    pub async fn execute_partitioned_arrow(
        self: &Arc<Self>,
        sql: &str,
        params: &[SpannerQueryParameter],
        max_partitions: i64,
        data_boost_enabled: bool,
        staleness: SpannerStaleness,
    ) -> Result<(String, usize, usize, Arc<ShmRegion>), SpannerError> {
        let ro_opts = SpannerTransactionOptions::ReadOnly {
            staleness,
            return_read_timestamp: true,
        };
        let (tx_id, _, _) = self.begin_transaction(&ro_opts, None, None).await?;
        let session = self.get_or_create_multiplexed_session(false).await?;

        let mut pw = ProtoWriter::with_capacity(256);
        pw.write_str(1, &session.name);
        pw.write_nested(2, |tw| {
            SpannerTransactionSelector::Id(tx_id.clone()).encode(tw)
        });
        pw.write_str(3, sql);
        if !params.is_empty() {
            let entries: Vec<(String, ProtobufValue)> = params
                .iter()
                .map(|p| (p.name.clone(), p.value.clone()))
                .collect();
            pw.write_nested(4, |sw| encode_protobuf_struct(&entries, sw));
            for p in params {
                pw.write_nested(5, |mw| {
                    mw.write_str(1, &p.name);
                    mw.write_nested(2, |tw| p.param_type.encode(tw));
                });
            }
        }
        if max_partitions > 0 {
            pw.write_nested(6, |ow| {
                ow.write_i64(2, max_partitions);
            });
        }

        let req_seq = self.next_request_sequence();
        let meta = self.build_metadata(false, req_seq, 1).await?;
        let partition_tokens = match self
            .grpc_pool
            .unary(
                "/google.spanner.v1.Spanner/PartitionQuery",
                pw.finish(),
                &meta,
            )
            .await
        {
            Ok((resp_bytes, _)) => decode_partition_response(&resp_bytes)?,
            Err(status)
                if matches!(
                    status.code,
                    GrpcCode::InvalidArgument | GrpcCode::Unimplemented
                ) =>
            {
                Vec::new()
            }
            Err(status) => return Err(SpannerError::Grpc(status)),
        };

        let mut results: Vec<SpannerQueryResult> = Vec::new();
        if partition_tokens.is_empty() {
            let single = self
                .execute_streaming_sql(
                    sql,
                    params,
                    &SpannerTransactionSelector::Id(tx_id),
                    0,
                    None,
                    data_boost_enabled,
                    false,
                    None,
                    None,
                )
                .await?;
            results.push(single);
        } else {
            let mut handles = Vec::with_capacity(partition_tokens.len());
            for token in partition_tokens {
                let client = Arc::clone(self);
                let sql_owned = sql.to_string();
                let params_owned = params.to_vec();
                let tx_id_owned = tx_id.clone();
                handles.push(tokio::spawn(async move {
                    client
                        .execute_streaming_sql(
                            &sql_owned,
                            &params_owned,
                            &SpannerTransactionSelector::Id(tx_id_owned),
                            0,
                            Some(&token),
                            data_boost_enabled,
                            false,
                            None,
                            None,
                        )
                        .await
                }));
            }
            for h in handles {
                let part_res = h.await.map_err(|e| {
                    SpannerError::Protocol(format!("partition task panicked: {e}"))
                })??;
                results.push(part_res);
            }
        }

        let columns = results
            .iter()
            .find(|r| !r.columns.is_empty())
            .map(|r| r.columns.clone())
            .unwrap_or_default();
        let schema_frame = encode_encapsulated_arrow_ipc_frame(
            1,
            &serde_json::to_vec(&columns).unwrap_or_default(),
        );
        let shm_seq = NEXT_SPANNER_SHM_SEQ.fetch_add(1, Ordering::Relaxed);
        let shm_name = format!("/vtb-spn-arrow-{}-{shm_seq}", std::process::id());
        let mut writer = ShmArrowStreamWriter::create(&shm_name, 64 * 1024, &schema_frame)
            .map_err(SpannerError::Shm)?;

        for part in &results {
            if part.rows.is_empty() {
                continue;
            }
            let json_rows: Vec<Vec<serde_json::Value>> = part
                .rows
                .iter()
                .map(|row| row.iter().map(SpannerValue::to_serde_json).collect())
                .collect();
            let batch_body = serde_json::to_vec(&json_rows).unwrap_or_default();
            let batch_frame = encode_encapsulated_arrow_ipc_frame(3, &batch_body);
            writer
                .append_ipc_frame(&batch_frame, part.rows.len())
                .map_err(SpannerError::Shm)?;
        }

        let total_rows = writer.row_count();
        let (norm_name, byte_len, region) = writer.finish().map_err(SpannerError::Shm)?;
        Ok((norm_name, byte_len, total_rows, region))
    }
}

/// Build an encapsulated Arrow IPC frame (`[0xFF, 0xFF, 0xFF, 0xFF]` + `meta_len_le32` +
/// 24-byte FlatBuffer Message header with `header_type` + 8-byte aligned `body`).
pub fn encode_encapsulated_arrow_ipc_frame(header_type: u8, body: &[u8]) -> Vec<u8> {
    let padded_body_len = (body.len() + 7) & !7;
    let mut flatbuf = [0u8; 24];
    flatbuf[0..4].copy_from_slice(&12i32.to_le_bytes());
    flatbuf[4..6].copy_from_slice(&8u16.to_le_bytes());
    flatbuf[6..8].copy_from_slice(&8u16.to_le_bytes());
    flatbuf[8..10].copy_from_slice(&0u16.to_le_bytes());
    flatbuf[10..12].copy_from_slice(&4u16.to_le_bytes());
    flatbuf[12..16].copy_from_slice(&8i32.to_le_bytes());
    flatbuf[16] = header_type;

    let mut out = Vec::with_capacity(8 + flatbuf.len() + padded_body_len);
    out.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]);
    out.extend_from_slice(&(flatbuf.len() as i32).to_le_bytes());
    out.extend_from_slice(&flatbuf);
    out.extend_from_slice(body);
    out.resize(8 + flatbuf.len() + padded_body_len, 0u8);
    out
}

#[allow(clippy::too_many_arguments)]
fn encode_execute_sql_request(
    session_name: &str,
    selector: &SpannerTransactionSelector,
    sql: &str,
    params: &[SpannerQueryParameter],
    resume_token: &[u8],
    partition_token: Option<&[u8]>,
    seqno: i64,
    data_boost_enabled: bool,
    request_tag: Option<&str>,
    transaction_tag: Option<&str>,
) -> Bytes {
    let mut w = ProtoWriter::with_capacity(512);
    w.write_str(1, session_name);
    w.write_nested(2, |tw| selector.encode(tw));
    w.write_str(3, sql);
    if !params.is_empty() {
        let entries: Vec<(String, ProtobufValue)> = params
            .iter()
            .map(|p| (p.name.clone(), p.value.clone()))
            .collect();
        w.write_nested(4, |sw| encode_protobuf_struct(&entries, sw));
        for p in params {
            w.write_nested(5, |mw| {
                mw.write_str(1, &p.name);
                mw.write_nested(2, |tw| p.param_type.encode(tw));
            });
        }
    }
    if !resume_token.is_empty() {
        w.write_bytes(6, resume_token);
    }
    if let Some(ptok) = partition_token
        && !ptok.is_empty()
    {
        w.write_bytes(8, ptok);
    }
    if seqno > 0 {
        w.write_i64(9, seqno);
    }
    if request_tag.is_some() || transaction_tag.is_some() {
        w.write_nested(10, |rw| {
            if let Some(rt) = request_tag
                && !rt.is_empty()
            {
                rw.write_str(2, rt);
            }
            if let Some(tt) = transaction_tag
                && !tt.is_empty()
            {
                rw.write_str(3, tt);
            }
        });
    }
    if data_boost_enabled {
        w.write_bool(16, true);
    }
    w.finish()
}

type DecodedBatchDmlResultSet = (
    i64,
    Option<Vec<u8>>,
    Option<MultiplexedSessionPrecommitToken>,
);

type DecodedCommitResponse = (
    Option<i64>,
    Option<i64>,
    Option<MultiplexedSessionPrecommitToken>,
);

fn decode_batch_dml_result_set(bytes: &[u8]) -> Result<DecodedBatchDmlResultSet, SpannerError> {
    let mut r = ProtoReader::new(bytes);
    let mut count = 0i64;
    let mut tx_id = None;
    let mut precommit = None;
    while let Some((field, wt)) = r.next_tag()? {
        match (field, wt) {
            (1, WireType::LengthDelimited) => {
                let mb = r.read_bytes()?;
                let (_, id, _, tok) = decode_result_set_metadata(mb)?;
                if id.is_some() {
                    tx_id = id;
                }
                update_highest_precommit_token(&mut precommit, tok);
            }
            (3, WireType::LengthDelimited) => {
                let sb = r.read_bytes()?;
                let (exact, lower) = decode_result_set_stats(sb)?;
                count = exact.or(lower).unwrap_or(0);
            }
            (5, WireType::LengthDelimited) => {
                let pb = r.read_bytes()?;
                let tok = MultiplexedSessionPrecommitToken::decode(pb)?;
                update_highest_precommit_token(&mut precommit, Some(tok));
            }
            _ => {
                r.skip_field(wt)?;
            }
        }
    }
    Ok((count, tx_id, precommit))
}

fn decode_commit_response(bytes: &[u8]) -> Result<DecodedCommitResponse, SpannerError> {
    let mut r = ProtoReader::new(bytes);
    let mut commit_ts = None;
    let mut mutation_count = None;
    let mut retry_token = None;
    while let Some((field, wt)) = r.next_tag()? {
        match (field, wt) {
            (1, WireType::LengthDelimited) => {
                let tb = r.read_bytes()?;
                commit_ts = Some(decode_timestamp_micros(tb)?);
            }
            (2, WireType::LengthDelimited) => {
                let sb = r.read_bytes()?;
                let mut sr = ProtoReader::new(sb);
                while let Some((sf, swt)) = sr.next_tag()? {
                    if sf == 1 && swt == WireType::Varint {
                        mutation_count = Some(sr.read_i64()?);
                    } else {
                        sr.skip_field(swt)?;
                    }
                }
            }
            (4, WireType::LengthDelimited) => {
                let pb = r.read_bytes()?;
                let tok = MultiplexedSessionPrecommitToken::decode(pb)?;
                if !tok.precommit_token.is_empty() {
                    retry_token = Some(tok);
                }
            }
            _ => {
                r.skip_field(wt)?;
            }
        }
    }
    Ok((commit_ts, mutation_count, retry_token))
}

fn decode_batch_write_response(bytes: &[u8]) -> Result<SpannerBatchWriteResult, SpannerError> {
    let mut r = ProtoReader::new(bytes);
    let mut indexes = Vec::new();
    let mut status_code = 0i32;
    let mut status_message = String::new();
    let mut commit_ts = None;
    while let Some((field, wt)) = r.next_tag()? {
        match (field, wt) {
            (1, WireType::Varint) => {
                indexes.push(r.read_i32()?);
            }
            (1, WireType::LengthDelimited) => {
                let packed = r.read_bytes()?;
                let mut pr = ProtoReader::new(packed);
                while pr.remaining() > 0 {
                    indexes.push(pr.read_i32()?);
                }
            }
            (2, WireType::LengthDelimited) => {
                let sb = r.read_bytes()?;
                let st = decode_embedded_rpc_status(sb)?;
                status_code = st.raw_code;
                status_message = st.message;
            }
            (3, WireType::LengthDelimited) => {
                let tb = r.read_bytes()?;
                commit_ts = Some(decode_timestamp_micros(tb)?);
            }
            _ => {
                r.skip_field(wt)?;
            }
        }
    }
    Ok(SpannerBatchWriteResult {
        indexes,
        status_code,
        status_message,
        commit_timestamp_micros: commit_ts,
    })
}

fn decode_partition_response(bytes: &[u8]) -> Result<Vec<Vec<u8>>, SpannerError> {
    let mut r = ProtoReader::new(bytes);
    let mut tokens = Vec::new();
    while let Some((field, wt)) = r.next_tag()? {
        if field == 1 && wt == WireType::LengthDelimited {
            let pb = r.read_bytes()?;
            let mut pr = ProtoReader::new(pb);
            while let Some((pf, pwt)) = pr.next_tag()? {
                if pf == 1 && pwt == WireType::LengthDelimited {
                    tokens.push(pr.read_bytes()?.to_vec());
                } else {
                    pr.skip_field(pwt)?;
                }
            }
        } else {
            r.skip_field(wt)?;
        }
    }
    Ok(tokens)
}

fn decode_lro_operation(bytes: &[u8]) -> Result<(String, bool, Option<GrpcStatus>), SpannerError> {
    let mut r = ProtoReader::new(bytes);
    let mut name = String::new();
    let mut done = false;
    let mut err_status = None;
    while let Some((field, wt)) = r.next_tag()? {
        match (field, wt) {
            (1, WireType::LengthDelimited) => {
                name = r.read_str()?.to_string();
            }
            (3, WireType::Varint) => {
                done = r.read_bool()?;
            }
            (4, WireType::LengthDelimited) => {
                let sb = r.read_bytes()?;
                let st = decode_embedded_rpc_status(sb)?;
                if st.code != GrpcCode::Ok {
                    err_status = Some(st);
                }
            }
            _ => {
                r.skip_field(wt)?;
            }
        }
    }
    Ok((name, done, err_status))
}

fn decode_embedded_rpc_status(bytes: &[u8]) -> Result<GrpcStatus, SpannerError> {
    let mut r = ProtoReader::new(bytes);
    let mut raw_code = 0i32;
    let mut message = String::new();
    while let Some((field, wt)) = r.next_tag()? {
        match (field, wt) {
            (1, WireType::Varint) => {
                raw_code = r.read_i32()?;
            }
            (2, WireType::LengthDelimited) => {
                message = r.read_str()?.to_string();
            }
            _ => {
                r.skip_field(wt)?;
            }
        }
    }
    Ok(GrpcStatus {
        code: GrpcCode::from_i32(raw_code),
        raw_code,
        message,
        details: Vec::new(),
    })
}

fn extract_aborted_retry_delay_ms(err: &SpannerError) -> Option<u64> {
    match err {
        SpannerError::Grpc(status) if status.code == GrpcCode::Aborted => {
            status.retry_delay_ms().or(Some(10))
        }
        _ => None,
    }
}

fn current_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
