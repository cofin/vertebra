//! PyO3 bindings for `vertebra-db` cloud dialers, wire codecs, and shared-memory Arrow IPC cursors.

use std::collections::HashMap;
use std::ffi::{CStr, c_int, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_array::ffi::FFI_ArrowSchema;
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::types::{
    Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    Decimal256Array, DictionaryArray, DurationMicrosecondArray, DurationMillisecondArray,
    DurationNanosecondArray, DurationSecondArray, FixedSizeBinaryArray, FixedSizeListArray,
    Float16Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeBinaryArray,
    LargeListArray, LargeStringArray, ListArray, MapArray, NullArray, RecordBatch,
    RecordBatchIterator, RecordBatchReader, StringArray, StringViewArray, StructArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::Buffer as ArrowBuffer;
use arrow_ipc::reader::RecordBatchDecoder;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyBufferError, PyNotImplementedError, PyRuntimeError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyCapsule, PyDict, PyList, PyString};
use vertebra_core::shm::{ShmCapsule, ShmRegion};
use vertebra_db::arrow_shm::{
    ShmArrowFrameHeader, open_shm_arrow_region, read_shm_arrow_frame, release_shm_arrow_frame,
    write_shm_arrow_region, write_shm_arrow_region_unregistered,
};
use vertebra_db::dialer::{
    AlloyDbDialer as RsAlloyDbDialer, AuthType, AwsAuroraDsqlDialer as RsAwsAuroraDsqlDialer,
    AwsRdsDialer as RsAwsRdsDialer, AzureEntraDialer as RsAzureEntraDialer,
    CloudSqlDialer as RsCloudSqlDialer, IpType, aws_rds_root_ca_pem as rs_aws_rds_root_ca_pem,
    decode_alloydb_metadata_response as rs_decode_alloydb_metadata_response,
    encode_alloydb_metadata_exchange as rs_encode_alloydb_metadata_exchange,
};
use vertebra_db::duckdb::DuckdbConfig;
use vertebra_db::mssql::rewrite_mssql_placeholders as rs_rewrite_mssql_placeholders;
use vertebra_db::oracle::{
    OracleSessionAttrs, decode_oracle_vector_binary as rs_decode_oracle_vector_binary,
    decode_oracle_vector_f32 as rs_decode_oracle_vector_f32,
    decode_oracle_vector_f64 as rs_decode_oracle_vector_f64,
    decode_oracle_vector_i8 as rs_decode_oracle_vector_i8,
    decode_oson_payload as rs_decode_oson_payload,
    encode_oracle_vector_binary as rs_encode_oracle_vector_binary,
    encode_oracle_vector_f32 as rs_encode_oracle_vector_f32,
    encode_oracle_vector_f64 as rs_encode_oracle_vector_f64,
    encode_oracle_vector_i8 as rs_encode_oracle_vector_i8,
    encode_oson_payload as rs_encode_oson_payload,
};
use vertebra_db::postgres::{
    decode_pgcopy_binary_rows, decode_pgvector_bit as rs_decode_pgvector_bit,
    decode_pgvector_f16 as rs_decode_pgvector_f16, decode_pgvector_f32 as rs_decode_pgvector_f32,
    encode_pgcopy_binary_rows, encode_pgvector_bit as rs_encode_pgvector_bit,
    encode_pgvector_f16 as rs_encode_pgvector_f16, encode_pgvector_f32 as rs_encode_pgvector_f32,
    strip_pg_jsonb_header as rs_strip_pg_jsonb_header,
};
use vertebra_db::py_async::{SHM_CAPSULE_NAME, wrap_shm_region_capsule};

use crate::vector::VectorView;
use vertebra_db::oracle::tns::{
    parse_tnsnames_ora as rs_parse_tnsnames_ora, resolve_tns_alias as rs_resolve_tns_alias,
};

/// Zero-sidecar Google Cloud SQL (`:3307`) mTLS dialer.
#[pyclass(name = "CloudSQLDialer", frozen, immutable_type, from_py_object)]
#[derive(Clone, Debug)]
pub struct CloudSQLDialer {
    inner: RsCloudSqlDialer,
}

#[pymethods]
impl CloudSQLDialer {
    #[new]
    #[pyo3(signature = (instance_uri, ip_type="PUBLIC", iam_auth=true))]
    pub fn new(instance_uri: &str, ip_type: &str, iam_auth: bool) -> PyResult<Self> {
        let parsed_ip = IpType::parse(ip_type).map_err(PyValueError::new_err)?;
        let auth = if iam_auth {
            AuthType::Iam
        } else {
            AuthType::Password
        };
        let inner =
            RsCloudSqlDialer::new(instance_uri, parsed_ip, auth).map_err(PyValueError::new_err)?;
        Ok(Self { inner })
    }

    #[getter]
    pub fn admin_api_path(&self) -> String {
        self.inner.admin_api_path()
    }

    #[getter]
    pub fn server_port(&self) -> u16 {
        self.inner.server_port
    }

    #[getter]
    pub fn project(&self) -> String {
        self.inner.uri.project.clone()
    }

    #[getter]
    pub fn region(&self) -> String {
        self.inner.uri.region.clone()
    }

    #[getter]
    pub fn instance(&self) -> String {
        self.inner.uri.instance.clone()
    }

    #[getter]
    pub fn expected_server_cn(&self) -> String {
        self.inner.uri.expected_server_cn()
    }

    pub fn generate_ephemeral_keypair(&self, py: Python<'_>) -> PyResult<(String, String)> {
        let dialer = self.inner.clone();
        py.detach(move || dialer.generate_ephemeral_keypair())
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)
    }
}

/// Zero-sidecar Google Cloud AlloyDB (`:5433`) mTLS and `MetadataExchangeRequest` dialer.
#[pyclass(name = "AlloyDBDialer", frozen, immutable_type, from_py_object)]
#[derive(Clone, Debug)]
pub struct AlloyDBDialer {
    inner: RsAlloyDbDialer,
}

#[pymethods]
impl AlloyDBDialer {
    #[new]
    #[pyo3(signature = (cluster_uri, ip_type="PRIVATE", iam_auth=true))]
    pub fn new(cluster_uri: &str, ip_type: &str, iam_auth: bool) -> PyResult<Self> {
        let parsed_ip = IpType::parse(ip_type).map_err(PyValueError::new_err)?;
        let auth = if iam_auth {
            AuthType::Iam
        } else {
            AuthType::Password
        };
        let inner =
            RsAlloyDbDialer::new(cluster_uri, parsed_ip, auth).map_err(PyValueError::new_err)?;
        Ok(Self { inner })
    }

    #[getter]
    pub fn server_port(&self) -> u16 {
        self.inner.server_port
    }

    #[getter]
    pub fn admin_api_path(&self) -> String {
        self.inner.uri.admin_api_path()
    }

    #[getter]
    pub fn project(&self) -> String {
        self.inner.uri.project.clone()
    }

    #[getter]
    pub fn location(&self) -> String {
        self.inner.uri.location.clone()
    }

    #[getter]
    pub fn cluster(&self) -> String {
        self.inner.uri.cluster.clone()
    }

    #[getter]
    pub fn instance(&self) -> String {
        self.inner.uri.instance.clone()
    }

    #[pyo3(signature = (user_agent="vertebra-db/0.1", token=""))]
    pub fn encode_metadata_exchange<'py>(
        &self,
        py: Python<'py>,
        user_agent: &str,
        token: &str,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = self.inner.encode_metadata_exchange(user_agent, token);
        PyBytes::new_with(py, bytes.len(), |buf| {
            buf.copy_from_slice(&bytes);
            Ok(())
        })
    }
}

fn validate_optional_port(port: Option<i64>, label: &str) -> PyResult<Option<u16>> {
    match port {
        Some(raw) => {
            if !(1..=i64::from(u16::MAX)).contains(&raw) {
                return Err(PyValueError::new_err(format!(
                    "{label} port must be between 1 and 65535, got {raw}"
                )));
            }
            Ok(Some(raw as u16))
        }
        None => Ok(None),
    }
}

fn validate_ttl_seconds(ttl_seconds: i64, label: &str) -> PyResult<u32> {
    if !(1..=i64::from(u32::MAX)).contains(&ttl_seconds) {
        return Err(PyValueError::new_err(format!(
            "{label} ttl_seconds must be a positive integer, got {ttl_seconds}"
        )));
    }
    Ok(ttl_seconds as u32)
}

/// Native AWS RDS, Aurora, and RDS Proxy IAM (`rds-db`) and TLS dialer.
#[pyclass(name = "AwsRdsDialer", frozen, immutable_type, from_py_object)]
#[derive(Clone, Debug)]
pub struct AwsRdsDialer {
    inner: RsAwsRdsDialer,
}

#[pymethods]
impl AwsRdsDialer {
    #[new]
    #[pyo3(signature = (endpoint_host, port=None, region=None, username="postgres"))]
    pub fn new(
        endpoint_host: &str,
        port: Option<i64>,
        region: Option<&str>,
        username: &str,
    ) -> PyResult<Self> {
        let parsed_port = validate_optional_port(port, "AWS RDS")?;
        let inner = RsAwsRdsDialer::new(endpoint_host, parsed_port, region, username)
            .map_err(PyValueError::new_err)?;
        Ok(Self { inner })
    }

    #[getter]
    pub fn endpoint_host(&self) -> String {
        self.inner.endpoint_host.clone()
    }

    #[getter]
    pub fn port(&self) -> u16 {
        self.inner.server_port
    }

    #[getter]
    pub fn server_port(&self) -> u16 {
        self.inner.server_port
    }

    #[getter]
    pub fn region(&self) -> String {
        self.inner.region.clone()
    }

    #[getter]
    pub fn username(&self) -> String {
        self.inner.username.clone()
    }

    #[getter]
    pub fn identifier(&self) -> Option<String> {
        self.inner.identifier.clone()
    }

    #[getter]
    pub fn cluster_or_proxy(&self) -> Option<String> {
        self.inner.cluster_or_proxy.clone()
    }

    #[getter]
    pub fn is_cluster_endpoint(&self) -> bool {
        self.inner.is_cluster_endpoint
    }

    #[getter]
    pub fn is_proxy_endpoint(&self) -> bool {
        self.inner.is_proxy_endpoint
    }

    #[getter]
    pub fn ca_pem(&self) -> &'static str {
        self.inner.ca_pem()
    }

    #[pyo3(signature = (access_key_id, secret_access_key, session_token=None, ttl_seconds=900, now_ts=None))]
    pub fn sign_iam_token(
        &self,
        py: Python<'_>,
        access_key_id: &str,
        secret_access_key: &str,
        session_token: Option<&str>,
        ttl_seconds: i64,
        now_ts: Option<f64>,
    ) -> PyResult<String> {
        let ttl = validate_ttl_seconds(ttl_seconds, "AWS RDS")?;
        let dialer = self.inner.clone();
        let ak = access_key_id.to_owned();
        let sk = secret_access_key.to_owned();
        let st = session_token.map(str::to_owned);
        py.detach(move || dialer.sign_iam_token(&ak, &sk, st.as_deref(), ttl, now_ts))
            .map_err(PyValueError::new_err)
    }
}

/// Native AWS Aurora DSQL IAM (`dsql` `DbConnect` / `DbConnectAdmin`) and TLS dialer.
#[pyclass(name = "AwsAuroraDsqlDialer", frozen, immutable_type, from_py_object)]
#[derive(Clone, Debug)]
pub struct AwsAuroraDsqlDialer {
    inner: RsAwsAuroraDsqlDialer,
}

#[pymethods]
impl AwsAuroraDsqlDialer {
    #[new]
    #[pyo3(signature = (endpoint_host, port=None, region=None, admin=false))]
    pub fn new(
        endpoint_host: &str,
        port: Option<i64>,
        region: Option<&str>,
        admin: bool,
    ) -> PyResult<Self> {
        let parsed_port = validate_optional_port(port, "AWS Aurora DSQL")?;
        let inner = RsAwsAuroraDsqlDialer::new(endpoint_host, parsed_port, region, admin)
            .map_err(PyValueError::new_err)?;
        Ok(Self { inner })
    }

    #[getter]
    pub fn endpoint_host(&self) -> String {
        self.inner.endpoint_host.clone()
    }

    #[getter]
    pub fn hostname(&self) -> String {
        self.inner.endpoint_host.clone()
    }

    #[getter]
    pub fn port(&self) -> u16 {
        self.inner.server_port
    }

    #[getter]
    pub fn server_port(&self) -> u16 {
        self.inner.server_port
    }

    #[getter]
    pub fn region(&self) -> String {
        self.inner.region.clone()
    }

    #[getter]
    pub fn cluster_id(&self) -> Option<String> {
        self.inner.cluster_id.clone()
    }

    #[getter]
    pub fn admin(&self) -> bool {
        self.inner.admin
    }

    #[getter]
    pub fn action(&self) -> &'static str {
        self.inner.action()
    }

    #[pyo3(signature = (access_key_id, secret_access_key, session_token=None, ttl_seconds=900, now_ts=None))]
    pub fn sign_iam_token(
        &self,
        py: Python<'_>,
        access_key_id: &str,
        secret_access_key: &str,
        session_token: Option<&str>,
        ttl_seconds: i64,
        now_ts: Option<f64>,
    ) -> PyResult<String> {
        let ttl = validate_ttl_seconds(ttl_seconds, "AWS Aurora DSQL")?;
        let dialer = self.inner.clone();
        let ak = access_key_id.to_owned();
        let sk = secret_access_key.to_owned();
        let st = session_token.map(str::to_owned);
        py.detach(move || dialer.sign_iam_token(&ak, &sk, st.as_deref(), ttl, now_ts))
            .map_err(PyValueError::new_err)
    }
}

/// Native Microsoft Entra ID database endpoint parser and OAuth2 scope resolver.
#[pyclass(name = "AzureEntraDialer", frozen, immutable_type, from_py_object)]
#[derive(Clone, Debug)]
pub struct AzureEntraDialer {
    inner: RsAzureEntraDialer,
}

#[pymethods]
impl AzureEntraDialer {
    #[new]
    #[pyo3(signature = (endpoint_host, port=None, engine=None))]
    pub fn new(endpoint_host: &str, port: Option<i64>, engine: Option<&str>) -> PyResult<Self> {
        let parsed_port = validate_optional_port(port, "Azure Entra")?;
        let inner = RsAzureEntraDialer::new(endpoint_host, parsed_port, engine)
            .map_err(PyValueError::new_err)?;
        Ok(Self { inner })
    }

    #[getter]
    pub fn endpoint_host(&self) -> String {
        self.inner.endpoint_host.clone()
    }

    #[getter]
    pub fn port(&self) -> u16 {
        self.inner.server_port
    }

    #[getter]
    pub fn server_port(&self) -> u16 {
        self.inner.server_port
    }

    #[getter]
    pub fn engine(&self) -> &'static str {
        self.inner.engine.as_str()
    }

    #[getter]
    pub fn scope(&self) -> &'static str {
        self.inner.scope()
    }

    #[getter]
    pub fn cloud(&self) -> &'static str {
        self.inner.cloud.as_str()
    }

    #[getter]
    pub fn authority_host(&self) -> &'static str {
        self.inner.authority_host()
    }

    #[getter]
    pub fn server_name(&self) -> Option<String> {
        self.inner.server_name.clone()
    }
}

/// Return the bundled official AWS RDS Global Root CA PEM certificates.
#[pyfunction]
pub fn aws_rds_root_ca_pem() -> &'static str {
    rs_aws_rds_root_ca_pem()
}

const ARROW_ARRAY_STREAM_CAPSULE_NAME: &CStr = c"arrow_array_stream";
const ARROW_SCHEMA_CAPSULE_NAME: &CStr = c"arrow_schema";
const MAX_ARROW_CURSOR_BYTE_LEN: usize = (isize::MAX as usize) - ShmArrowFrameHeader::SIZE;
const IPC_CONTINUATION_MARKER: [u8; 4] = [0xFF; 4];
const UNIX_EPOCH_ORDINAL: i64 = 719_163;

#[inline]
fn read_ipc_meta_header(payload: &[u8], mut pos: usize) -> Result<(usize, usize), String> {
    if payload.len().saturating_sub(pos) < 4 {
        return Err("corrupt Arrow IPC stream: truncated message length".to_string());
    }
    let mut word = [0u8; 4];
    word.copy_from_slice(&payload[pos..pos + 4]);
    pos += 4;
    if word == IPC_CONTINUATION_MARKER {
        if payload.len() - pos < 4 {
            return Err("corrupt Arrow IPC stream: truncated continuation length".to_string());
        }
        word.copy_from_slice(&payload[pos..pos + 4]);
        pos += 4;
    }
    let raw_meta_len = i32::from_le_bytes(word);
    if raw_meta_len == 0 {
        return Ok((pos, 0));
    }
    let meta_len = usize::try_from(raw_meta_len).map_err(|_| {
        format!("invalid Arrow IPC stream: negative metadata length {raw_meta_len}")
    })?;
    if payload.len() - pos < meta_len {
        return Err("corrupt Arrow IPC stream: truncated message metadata".to_string());
    }
    Ok((pos, meta_len))
}

#[inline]
fn read_ipc_message_body_len(meta: &[u8]) -> Result<usize, String> {
    const VT_BODYLENGTH: usize = arrow_ipc::Message::VT_BODYLENGTH as usize;

    if meta.len() < 4 {
        return Err("corrupt Arrow IPC stream: truncated message root offset".to_string());
    }
    let table_pos = u32::from_le_bytes([meta[0], meta[1], meta[2], meta[3]]) as usize;
    let table_hdr_end = table_pos
        .checked_add(4)
        .ok_or_else(|| "corrupt Arrow IPC stream: message table offset overflow".to_string())?;
    if table_hdr_end > meta.len() {
        return Err("corrupt Arrow IPC stream: message table offset out of bounds".to_string());
    }
    let vtable_rel = i32::from_le_bytes([
        meta[table_pos],
        meta[table_pos + 1],
        meta[table_pos + 2],
        meta[table_pos + 3],
    ]);
    let vtable_pos_i128 = (table_pos as i128)
        .checked_sub(i128::from(vtable_rel))
        .ok_or_else(|| "corrupt Arrow IPC stream: vtable offset overflow".to_string())?;
    let vtable_pos = usize::try_from(vtable_pos_i128)
        .map_err(|_| "corrupt Arrow IPC stream: negative vtable offset".to_string())?;
    let vtable_hdr_end = vtable_pos
        .checked_add(4)
        .ok_or_else(|| "corrupt Arrow IPC stream: vtable header overflow".to_string())?;
    if vtable_hdr_end > meta.len() {
        return Err("corrupt Arrow IPC stream: vtable offset out of bounds".to_string());
    }
    let vtable_len = u16::from_le_bytes([meta[vtable_pos], meta[vtable_pos + 1]]) as usize;
    let table_len = u16::from_le_bytes([meta[vtable_pos + 2], meta[vtable_pos + 3]]) as usize;
    if vtable_len < 4
        || vtable_pos
            .checked_add(vtable_len)
            .is_none_or(|end| end > meta.len())
        || table_pos
            .checked_add(table_len)
            .is_none_or(|end| end > meta.len())
    {
        return Err("corrupt Arrow IPC stream: invalid message vtable or table bounds".to_string());
    }
    if VT_BODYLENGTH + 2 > vtable_len {
        return Ok(0);
    }
    let field_rel = u16::from_le_bytes([
        meta[vtable_pos + VT_BODYLENGTH],
        meta[vtable_pos + VT_BODYLENGTH + 1],
    ]) as usize;
    if field_rel == 0 {
        return Ok(0);
    }
    let field_pos = table_pos
        .checked_add(field_rel)
        .ok_or_else(|| "corrupt Arrow IPC stream: bodyLength offset overflow".to_string())?;
    let field_end = field_pos
        .checked_add(8)
        .ok_or_else(|| "corrupt Arrow IPC stream: bodyLength end overflow".to_string())?;
    if field_end > meta.len() {
        return Err("corrupt Arrow IPC stream: bodyLength field out of bounds".to_string());
    }
    let raw_body_len = i64::from_le_bytes([
        meta[field_pos],
        meta[field_pos + 1],
        meta[field_pos + 2],
        meta[field_pos + 3],
        meta[field_pos + 4],
        meta[field_pos + 5],
        meta[field_pos + 6],
        meta[field_pos + 7],
    ]);
    usize::try_from(raw_body_len).map_err(|_| {
        format!("corrupt Arrow IPC stream: negative message body length {raw_body_len}")
    })
}

fn scan_ipc_frame_bounds(payload: &[u8]) -> Result<(), String> {
    if payload.is_empty() {
        return Err("invalid Arrow IPC stream: empty stream".to_string());
    }
    let mut pos = 0usize;
    let mut frame_count = 0usize;
    while pos < payload.len() {
        let (meta_start, meta_len) = read_ipc_meta_header(payload, pos)?;
        if meta_len == 0 {
            break;
        }
        let meta_end = meta_start + meta_len;
        let body_len = read_ipc_message_body_len(&payload[meta_start..meta_end])?;
        if payload.len() - meta_end < body_len {
            return Err("corrupt Arrow IPC stream: truncated message body".to_string());
        }
        frame_count += 1;
        pos = meta_end + body_len;
    }
    if frame_count == 0 {
        return Err("invalid Arrow IPC stream: missing schema message".to_string());
    }
    Ok(())
}

fn parse_ipc_schema_frame(payload: &[u8]) -> Result<(CompiledIpcLayout, usize), String> {
    if payload.is_empty() {
        return Err("invalid Arrow IPC stream: empty stream".to_string());
    }
    let (meta_start, meta_len) = read_ipc_meta_header(payload, 0)?;
    if meta_len == 0 {
        return Err("invalid Arrow IPC stream: missing schema message".to_string());
    }
    let meta_end = meta_start + meta_len;
    let message = arrow_ipc::root_as_message(&payload[meta_start..meta_end])
        .map_err(|e| format!("invalid Arrow IPC stream: {e:?}"))?;
    if message.header_type() != arrow_ipc::MessageHeader::Schema {
        return Err(format!(
            "invalid Arrow IPC stream: expected Schema header first, got {:?}",
            message.header_type()
        ));
    }
    let ipc_schema = message
        .header_as_schema()
        .ok_or_else(|| "invalid Arrow IPC stream: missing schema header".to_string())?;
    let body_len = usize::try_from(message.bodyLength()).map_err(|_| {
        format!(
            "corrupt Arrow IPC stream: negative message body length {}",
            message.bodyLength()
        )
    })?;
    if payload.len() - meta_end < body_len {
        return Err("corrupt Arrow IPC stream: truncated message body".to_string());
    }
    let layout = CompiledIpcLayout::compile(ipc_schema)?;
    Ok((layout, meta_end + body_len))
}

fn parse_ipc_schema_only(payload: &[u8]) -> Result<arrow_schema::SchemaRef, String> {
    let (layout, _) = parse_ipc_schema_frame(payload)?;
    Ok(layout.schema)
}

type IpcArrowError = arrow_schema::ArrowError;
type IpcBatchResult = Result<arrow_array::RecordBatch, IpcArrowError>;

#[inline]
fn ipc_range_within_body(offset: i64, length: i64, body_len: usize) -> bool {
    match (usize::try_from(offset), usize::try_from(length)) {
        (Ok(offset), Ok(length)) => offset
            .checked_add(length)
            .is_some_and(|end| end <= body_len),
        _ => false,
    }
}

/// Rejects record-batch metadata whose buffers fall outside the message body or whose field
/// nodes carry negative, overflowing, or inconsistent counts, which `arrow-ipc` slices without
/// checking.
fn validate_arrow_ipc_batch_ranges(
    batch: &arrow_ipc::RecordBatch<'_>,
    body_len: usize,
) -> Result<(), String> {
    if usize::try_from(batch.length()).is_err() {
        return Err(format!(
            "corrupt Arrow IPC stream: invalid record batch length {}",
            batch.length()
        ));
    }
    if let Some(buffers) = batch.buffers() {
        for (index, buffer) in buffers.iter().enumerate() {
            if !ipc_range_within_body(buffer.offset(), buffer.length(), body_len) {
                return Err(format!(
                    "corrupt Arrow IPC stream: buffer {index} (offset {}, length {}) exceeds message body length {body_len}",
                    buffer.offset(),
                    buffer.length()
                ));
            }
        }
    }
    if let Some(nodes) = batch.nodes() {
        for (index, node) in nodes.iter().enumerate() {
            let valid = match (
                usize::try_from(node.length()),
                usize::try_from(node.null_count()),
            ) {
                (Ok(node_len), Ok(null_count)) => null_count <= node_len,
                _ => false,
            };
            if !valid {
                return Err(format!(
                    "corrupt Arrow IPC stream: field node {index} has invalid length {} or null count {}",
                    node.length(),
                    node.null_count()
                ));
            }
        }
    }
    Ok(())
}

#[inline]
fn ipc_int_byte_width(int: Option<arrow_ipc::Int<'_>>, default: i64) -> i64 {
    match int.map(|int| int.bitWidth()) {
        Some(8) => 1,
        Some(16) => 2,
        Some(32) => 4,
        Some(64) => 8,
        Some(_) => 1,
        None => default,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum IpcFieldRole {
    Plain,
    DictionaryValues,
    RunEnds,
}

#[derive(Clone, Copy)]
enum LayoutOp {
    NoBuffers,
    SkipBuffers(u8),
    FixedSize {
        element_width: usize,
        trailing_buffers: u8,
        label: &'static str,
    },
    TypedBuffer {
        width: i64,
        trailing_buffers: u8,
        label: &'static str,
    },
    ListView {
        width: i64,
    },
    VariadicView,
    Union {
        dense: bool,
    },
}

struct CompiledIpcLayout {
    schema: arrow_schema::SchemaRef,
    root_ops: Vec<LayoutOp>,
    dict_ops: HashMap<i64, Vec<LayoutOp>>,
}

impl CompiledIpcLayout {
    fn compile(ipc_schema: arrow_ipc::Schema<'_>) -> Result<Self, String> {
        let mut root_ops = Vec::new();
        let mut dict_ops = HashMap::new();
        for field in ipc_schema.fields().into_iter().flatten() {
            Self::compile_field(&mut root_ops, &mut dict_ops, &field, IpcFieldRole::Plain)?;
        }
        let schema = match catch_unwind(AssertUnwindSafe(|| {
            arrow_ipc::convert::try_fb_to_schema(ipc_schema)
        })) {
            Ok(Ok(schema)) => Arc::new(schema),
            Ok(Err(err)) => return Err(format!("invalid Arrow IPC stream: {err}")),
            Err(payload) => {
                return Err(format!(
                    "invalid Arrow IPC stream: schema decoder panicked: {}",
                    ipc_panic_message(payload.as_ref())
                ));
            }
        };
        Ok(Self {
            schema,
            root_ops,
            dict_ops,
        })
    }

    fn compile_children(
        ops: &mut Vec<LayoutOp>,
        dict_ops: &mut HashMap<i64, Vec<LayoutOp>>,
        field: &arrow_ipc::Field<'_>,
    ) -> Result<(), String> {
        for child in field.children().into_iter().flatten() {
            Self::compile_field(ops, dict_ops, &child, IpcFieldRole::Plain)?;
        }
        Ok(())
    }

    fn compile_field(
        ops: &mut Vec<LayoutOp>,
        dict_ops: &mut HashMap<i64, Vec<LayoutOp>>,
        field: &arrow_ipc::Field<'_>,
        role: IpcFieldRole,
    ) -> Result<(), String> {
        use arrow_ipc::Type;

        if role != IpcFieldRole::DictionaryValues
            && let Some(dictionary) = field.dictionary()
        {
            let width = ipc_int_byte_width(dictionary.indexType(), 4);
            ops.push(LayoutOp::TypedBuffer {
                width,
                trailing_buffers: 0,
                label: "dictionary key",
            });
            let mut value_ops = Vec::new();
            Self::compile_field(
                &mut value_ops,
                dict_ops,
                field,
                IpcFieldRole::DictionaryValues,
            )?;
            dict_ops.insert(dictionary.id(), value_ops);
            return Ok(());
        }
        match field.type_type() {
            Type::Null => ops.push(LayoutOp::NoBuffers),
            Type::Utf8 | Type::Binary => ops.push(LayoutOp::TypedBuffer {
                width: 4,
                trailing_buffers: 1,
                label: "offset",
            }),
            Type::LargeUtf8 | Type::LargeBinary => ops.push(LayoutOp::TypedBuffer {
                width: 8,
                trailing_buffers: 1,
                label: "offset",
            }),
            Type::Utf8View | Type::BinaryView => ops.push(LayoutOp::VariadicView),
            Type::List | Type::Map => {
                ops.push(LayoutOp::TypedBuffer {
                    width: 4,
                    trailing_buffers: 0,
                    label: "offset",
                });
                Self::compile_children(ops, dict_ops, field)?;
            }
            Type::LargeList => {
                ops.push(LayoutOp::TypedBuffer {
                    width: 8,
                    trailing_buffers: 0,
                    label: "offset",
                });
                Self::compile_children(ops, dict_ops, field)?;
            }
            Type::ListView => {
                ops.push(LayoutOp::ListView { width: 4 });
                Self::compile_children(ops, dict_ops, field)?;
            }
            Type::LargeListView => {
                ops.push(LayoutOp::ListView { width: 8 });
                Self::compile_children(ops, dict_ops, field)?;
            }
            Type::FixedSizeList => {
                let raw_width = field
                    .type_as_fixed_size_list()
                    .map(|list| list.listSize())
                    .unwrap_or(0);
                let element_width = usize::try_from(raw_width).map_err(|_| {
                    format!(
                        "invalid Arrow IPC stream: field {:?} declares negative fixed size {raw_width}",
                        field.name().unwrap_or_default()
                    )
                })?;
                ops.push(LayoutOp::FixedSize {
                    element_width,
                    trailing_buffers: 0,
                    label: "fixed-size list",
                });
                Self::compile_children(ops, dict_ops, field)?;
            }
            Type::FixedSizeBinary => {
                let raw_width = field
                    .type_as_fixed_size_binary()
                    .map(|binary| binary.byteWidth())
                    .unwrap_or(0);
                let element_width = usize::try_from(raw_width).map_err(|_| {
                    format!(
                        "invalid Arrow IPC stream: field {:?} declares negative fixed size {raw_width}",
                        field.name().unwrap_or_default()
                    )
                })?;
                ops.push(LayoutOp::FixedSize {
                    element_width,
                    trailing_buffers: 1,
                    label: "fixed-size binary",
                });
            }
            Type::Struct_ => {
                ops.push(LayoutOp::SkipBuffers(1));
                Self::compile_children(ops, dict_ops, field)?;
            }
            Type::RunEndEncoded => {
                ops.push(LayoutOp::NoBuffers);
                let mut child_role = IpcFieldRole::RunEnds;
                for child in field.children().into_iter().flatten() {
                    Self::compile_field(ops, dict_ops, &child, child_role)?;
                    child_role = IpcFieldRole::Plain;
                }
            }
            Type::Union => {
                let dense = field
                    .type_as_union()
                    .is_some_and(|union| union.mode() == arrow_ipc::UnionMode::Dense);
                ops.push(LayoutOp::Union { dense });
                Self::compile_children(ops, dict_ops, field)?;
            }
            Type::Int if role == IpcFieldRole::RunEnds => {
                let width = ipc_int_byte_width(field.type_as_int(), 1);
                ops.push(LayoutOp::TypedBuffer {
                    width,
                    trailing_buffers: 0,
                    label: "run end",
                });
            }
            _ => ops.push(LayoutOp::SkipBuffers(2)),
        }
        Ok(())
    }
}

#[inline]
fn skip_ipc_buffers<'a, B>(buffers: &mut B, count: usize) -> bool
where
    B: Iterator<Item = &'a arrow_ipc::Buffer>,
{
    for _ in 0..count {
        if buffers.next().is_none() {
            return false;
        }
    }
    true
}

/// Compressed buffers are skipped: this build of `arrow-ipc` has no codecs and rejects
/// every non-empty compressed buffer before interpreting it.
#[inline]
fn check_ipc_typed_buffer<'a, B>(
    buffers: &mut B,
    compressed: bool,
    width: i64,
    label: &str,
) -> Result<bool, String>
where
    B: Iterator<Item = &'a arrow_ipc::Buffer>,
{
    let Some(buffer) = buffers.next() else {
        return Ok(false);
    };
    if !compressed && buffer.length() % width != 0 {
        return Err(format!(
            "corrupt Arrow IPC stream: {label} buffer length {} is not a multiple of {width} bytes",
            buffer.length()
        ));
    }
    Ok(true)
}

#[inline]
fn check_ipc_union_buffer<'a, B>(
    buffers: &mut B,
    compressed: bool,
    rows: i64,
    width: i64,
    label: &str,
) -> Result<bool, String>
where
    B: Iterator<Item = &'a arrow_ipc::Buffer>,
{
    let Some(buffer) = buffers.next() else {
        return Ok(false);
    };
    if rows == 0 {
        return Ok(true);
    }
    if compressed {
        return Err(
            "invalid Arrow IPC stream: compressed union columns are not supported".to_string(),
        );
    }
    let required = usize::try_from(rows)
        .ok()
        .and_then(|rows| rows.checked_mul(width as usize));
    let buf_len = usize::try_from(buffer.length()).ok();
    if required.is_none_or(|required| buf_len.is_none_or(|len| len < required))
        || buffer.offset() % width != 0
    {
        return Err(format!(
            "corrupt Arrow IPC stream: union {label} buffer (offset {}, length {}) cannot hold {rows} rows",
            buffer.offset(),
            buffer.length()
        ));
    }
    Ok(true)
}

/// Validates one record batch against a pre-compiled `LayoutOp` sequence, rejecting buffer shapes
/// and length overflows that `arrow-ipc` and `arrow-data` assert on instead of returning an error.
fn validate_compiled_batch_layout(
    batch: &arrow_ipc::RecordBatch<'_>,
    ops: &[LayoutOp],
    version: arrow_ipc::MetadataVersion,
) -> Result<(), String> {
    let mut nodes = batch.nodes().into_iter().flatten();
    let mut buffers = batch.buffers().into_iter().flatten();
    let mut variadic_counts = batch.variadicBufferCounts().into_iter().flatten();
    let legacy_union_validity = version < arrow_ipc::MetadataVersion::V5;
    let compressed = batch.compression().is_some();

    for &op in ops {
        let Some(node) = nodes.next() else {
            break;
        };
        match op {
            LayoutOp::NoBuffers => {}
            LayoutOp::SkipBuffers(count) => {
                if !skip_ipc_buffers(&mut buffers, usize::from(count)) {
                    break;
                }
            }
            LayoutOp::FixedSize {
                element_width,
                trailing_buffers,
                label,
            } => {
                let node_len = usize::try_from(node.length()).map_err(|_| {
                    format!(
                        "corrupt Arrow IPC stream: {label} length {} overflows usize",
                        node.length()
                    )
                })?;
                if node_len.checked_mul(element_width).is_none() {
                    return Err(format!(
                        "corrupt Arrow IPC stream: {label} length {node_len} * width {element_width} overflows usize"
                    ));
                }
                if !skip_ipc_buffers(&mut buffers, 1 + usize::from(trailing_buffers)) {
                    break;
                }
            }
            LayoutOp::TypedBuffer {
                width,
                trailing_buffers,
                label,
            } => {
                if !skip_ipc_buffers(&mut buffers, 1)
                    || !check_ipc_typed_buffer(&mut buffers, compressed, width, label)?
                    || !skip_ipc_buffers(&mut buffers, usize::from(trailing_buffers))
                {
                    break;
                }
            }
            LayoutOp::ListView { width } => {
                if !skip_ipc_buffers(&mut buffers, 1)
                    || !check_ipc_typed_buffer(&mut buffers, compressed, width, "offset")?
                    || !check_ipc_typed_buffer(&mut buffers, compressed, width, "size")?
                {
                    break;
                }
            }
            LayoutOp::VariadicView => {
                let Some(count) = variadic_counts
                    .next()
                    .and_then(|count| usize::try_from(count).ok())
                else {
                    break;
                };
                if !skip_ipc_buffers(&mut buffers, 1)
                    || !check_ipc_typed_buffer(&mut buffers, compressed, 16, "view")?
                    || !skip_ipc_buffers(&mut buffers, count)
                {
                    break;
                }
            }
            LayoutOp::Union { dense } => {
                if legacy_union_validity && !skip_ipc_buffers(&mut buffers, 1) {
                    break;
                }
                if !check_ipc_union_buffer(&mut buffers, compressed, node.length(), 1, "type id")? {
                    break;
                }
                if dense
                    && !check_ipc_union_buffer(
                        &mut buffers,
                        compressed,
                        node.length(),
                        4,
                        "offset",
                    )?
                {
                    break;
                }
            }
        }
    }
    Ok(())
}

fn ipc_panic_message(payload: &(dyn std::any::Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("unknown panic")
}

/// Zero-copy single-pass Arrow IPC batch iterator that parses each message FlatBuffer once,
/// validates buffer ranges and compiled field layouts, and runs decoding inside `catch_unwind`.
struct SinglePassIpcBatches {
    buffer: ArrowBuffer,
    pos: usize,
    layout: CompiledIpcLayout,
    dictionaries_by_id: HashMap<i64, arrow_array::ArrayRef>,
    validated_flag: Option<Arc<AtomicBool>>,
    poisoned: bool,
}

impl SinglePassIpcBatches {
    fn decode_next_batch(&mut self) -> Result<Option<RecordBatch>, IpcArrowError> {
        loop {
            let slice = self.buffer.as_slice();
            if self.pos >= slice.len() {
                if let Some(ref flag) = self.validated_flag {
                    flag.store(true, Ordering::Release);
                }
                return Ok(None);
            }
            let (meta_start, meta_len) =
                read_ipc_meta_header(slice, self.pos).map_err(IpcArrowError::IpcError)?;
            if meta_len == 0 {
                self.pos = slice.len();
                if let Some(ref flag) = self.validated_flag {
                    flag.store(true, Ordering::Release);
                }
                return Ok(None);
            }
            let meta_end = meta_start + meta_len;
            let message =
                arrow_ipc::root_as_message(&slice[meta_start..meta_end]).map_err(|e| {
                    IpcArrowError::ParseError(format!("invalid Arrow IPC message: {e:?}"))
                })?;
            let body_len = usize::try_from(message.bodyLength()).map_err(|_| {
                IpcArrowError::IpcError(format!(
                    "corrupt Arrow IPC stream: negative message body length {}",
                    message.bodyLength()
                ))
            })?;
            if slice.len() - meta_end < body_len {
                return Err(IpcArrowError::IpcError(
                    "corrupt Arrow IPC stream: truncated message body".to_string(),
                ));
            }
            let body_end = meta_end + body_len;
            self.pos = body_end;
            let version = message.version();
            let raw_body = self.buffer.slice_with_length(meta_end, body_len);
            let body = if !raw_body.is_empty() && ((raw_body.as_ptr() as usize) & 7) != 0 {
                ArrowBuffer::from_slice_ref(raw_body.as_slice())
            } else {
                raw_body
            };
            match message.header_type() {
                arrow_ipc::MessageHeader::RecordBatch => {
                    let batch = message.header_as_record_batch().ok_or_else(|| {
                        IpcArrowError::IpcError(
                            "invalid Arrow IPC stream: missing record batch header".to_string(),
                        )
                    })?;
                    validate_arrow_ipc_batch_ranges(&batch, body_len)
                        .map_err(IpcArrowError::IpcError)?;
                    validate_compiled_batch_layout(&batch, &self.layout.root_ops, version)
                        .map_err(IpcArrowError::IpcError)?;
                    let record_batch = RecordBatchDecoder::try_new(
                        &body,
                        batch,
                        Arc::clone(&self.layout.schema),
                        &self.dictionaries_by_id,
                        &version,
                    )?
                    .with_require_alignment(false)
                    .read_record_batch()?;
                    return Ok(Some(record_batch));
                }
                arrow_ipc::MessageHeader::DictionaryBatch => {
                    let dict = message.header_as_dictionary_batch().ok_or_else(|| {
                        IpcArrowError::IpcError(
                            "invalid Arrow IPC stream: missing dictionary batch header".to_string(),
                        )
                    })?;
                    if let Some(batch) = dict.data() {
                        validate_arrow_ipc_batch_ranges(&batch, body_len)
                            .map_err(IpcArrowError::IpcError)?;
                        if let Some(ops) = self.layout.dict_ops.get(&dict.id()) {
                            validate_compiled_batch_layout(&batch, ops, version)
                                .map_err(IpcArrowError::IpcError)?;
                        }
                    }
                    arrow_ipc::reader::read_dictionary(
                        &body,
                        dict,
                        self.layout.schema.as_ref(),
                        &mut self.dictionaries_by_id,
                        &version,
                    )?;
                }
                arrow_ipc::MessageHeader::Schema => {
                    return Err(IpcArrowError::IpcError(
                        "invalid Arrow IPC stream: unexpected Schema message after stream header"
                            .to_string(),
                    ));
                }
                other => {
                    return Err(IpcArrowError::ParseError(format!(
                        "unsupported Arrow IPC message header: {other:?}"
                    )));
                }
            }
        }
    }
}

impl Iterator for SinglePassIpcBatches {
    type Item = IpcBatchResult;

    fn next(&mut self) -> Option<Self::Item> {
        if self.poisoned {
            return None;
        }
        match catch_unwind(AssertUnwindSafe(|| self.decode_next_batch())) {
            Ok(Ok(Some(batch))) => Some(Ok(batch)),
            Ok(Ok(None)) => None,
            Ok(Err(err)) => {
                self.poisoned = true;
                Some(Err(err))
            }
            Err(payload) => {
                self.poisoned = true;
                Some(Err(IpcArrowError::IpcError(format!(
                    "decoder panicked: {}",
                    ipc_panic_message(payload.as_ref())
                ))))
            }
        }
    }
}

type GuardedIpcReader = RecordBatchIterator<SinglePassIpcBatches>;

fn open_single_pass_ipc_stream(
    buffer: ArrowBuffer,
    skip_frame_scan: bool,
    validated_flag: Option<Arc<AtomicBool>>,
) -> Result<GuardedIpcReader, String> {
    if !skip_frame_scan {
        scan_ipc_frame_bounds(buffer.as_slice())?;
    }
    let (layout, pos) = parse_ipc_schema_frame(buffer.as_slice())?;
    let schema = Arc::clone(&layout.schema);
    Ok(RecordBatchIterator::new(
        SinglePassIpcBatches {
            buffer,
            pos,
            layout,
            dictionaries_by_id: HashMap::new(),
            validated_flag,
            poisoned: false,
        },
        schema,
    ))
}

#[derive(Clone, Debug)]
struct OwnedArrowIpcSlice {
    backing: ArrowBacking,
    start: usize,
    end: usize,
}

impl OwnedArrowIpcSlice {
    fn try_new(backing: ArrowBacking, byte_len: usize) -> Result<Self, String> {
        let (start, end) = backing
            .checked_bounds(byte_len)
            .ok_or_else(|| "ArrowCursor byte_len overflow".to_string())?;
        let (ptr, total_len) = backing.as_const_ptr_and_len();
        if ptr.is_null() || total_len < end {
            return Err("ArrowCursor backing smaller than declared byte_len".to_string());
        }
        Ok(Self {
            backing,
            start,
            end,
        })
    }
}

impl AsRef<[u8]> for OwnedArrowIpcSlice {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        match &self.backing {
            ArrowBacking::Bytes(bytes) => &bytes[self.start..self.end],
            ArrowBacking::Region(region) => {
                // SAFETY: `try_new` verified `start <= end <= region.len()`, `end <= isize::MAX`,
                // and a non-null `region.as_ptr()`; `Arc<ShmRegion>` pins the immutable mapping.
                let frame =
                    unsafe { core::slice::from_raw_parts(region.as_ptr().cast_const(), self.end) };
                &frame[self.start..self.end]
            }
            ArrowBacking::Capsule(cap) => {
                // SAFETY: `try_new` verified `start <= end <= cap.len`, `end <= isize::MAX`,
                // and a non-null `cap.ptr`; `Arc<CapsuleBacking>` pins the immutable mapping.
                let frame = unsafe { core::slice::from_raw_parts(cap.ptr.cast_const(), self.end) };
                &frame[self.start..self.end]
            }
        }
    }
}

fn open_lazy_arrow_ipc_stream(
    carrier: OwnedArrowIpcSlice,
    skip_frame_scan: bool,
    validated_flag: Option<Arc<AtomicBool>>,
) -> Result<GuardedIpcReader, String> {
    let buffer = ArrowBuffer::from(bytes::Bytes::from_owner(carrier));
    open_single_pass_ipc_stream(buffer, skip_frame_scan, validated_flag)
}

fn validate_and_summarize_ipc_bytes(payload: Arc<[u8]>) -> Result<(usize, Arc<[String]>), String> {
    let byte_len = payload.len();
    let carrier = OwnedArrowIpcSlice::try_new(ArrowBacking::Bytes(payload), byte_len)?;
    let reader = open_lazy_arrow_ipc_stream(carrier, false, None)?;
    let columns: Arc<[String]> = reader
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect::<Vec<_>>()
        .into();
    let mut row_count = 0usize;
    for batch_res in reader {
        let batch = batch_res.map_err(|e| format!("corrupt Arrow IPC batch: {e}"))?;
        row_count = row_count
            .checked_add(batch.num_rows())
            .ok_or_else(|| "ArrowCursor row_count overflow".to_string())?;
    }
    Ok((row_count, columns))
}

fn extract_ipc_buffer_arc(py: Python<'_>, payload: &Bound<'_, PyAny>) -> PyResult<Arc<[u8]>> {
    if let Ok(py_bytes) = payload.cast::<PyBytes>() {
        return Ok(Arc::<[u8]>::from(py_bytes.as_bytes()));
    }
    if let Ok(py_buf) = PyBuffer::<u8>::get(payload) {
        if py_buf.is_c_contiguous() {
            let len = py_buf.item_count();
            if len == 0 {
                return Ok(Arc::<[u8]>::from(&[][..]));
            }
            let ptr = py_buf.buf_ptr() as *const u8;
            if !ptr.is_null() {
                // SAFETY: The GIL is held (`py`), and `py_buf` pins a valid C-contiguous `u8`
                // buffer of `len > 0` bytes for the duration of this copy.
                let slice = unsafe { core::slice::from_raw_parts(ptr, len) };
                return Ok(Arc::<[u8]>::from(slice));
            }
        }
        let vec = py_buf.to_vec(py)?;
        return Ok(Arc::<[u8]>::from(vec));
    }
    // SAFETY: `payload.as_ptr()` is a valid borrowed `PyObject*` while the GIL is held; fallback
    // handles typed `memoryview` buffers (such as `int32` views) via `PyMemoryView_FromObject`.
    let mv_ptr = unsafe { ffi::PyMemoryView_FromObject(payload.as_ptr()) };
    if mv_ptr.is_null() {
        return Err(PyErr::fetch(py));
    }
    // SAFETY: `mv_ptr` is a non-null owned `PyObject*` returned by `PyMemoryView_FromObject`.
    let mv = unsafe { Bound::from_owned_ptr(py, mv_ptr) };
    let byte_mv = mv.call_method1(pyo3::intern!(py, "cast"), ("B",))?;
    let py_buf = PyBuffer::<u8>::get(&byte_mv)?;
    let vec = py_buf.to_vec(py)?;
    Ok(Arc::<[u8]>::from(vec))
}

struct PyArrowDecodeHelpers<'py> {
    decimal_cls: Option<Bound<'py, PyAny>>,
    date_cls: Option<Bound<'py, PyAny>>,
    time_cls: Option<Bound<'py, PyAny>>,
    timedelta_cls: Option<Bound<'py, PyAny>>,
    epoch_naive: Option<Bound<'py, PyAny>>,
    epoch_utc: Option<Bound<'py, PyAny>>,
    timezone_cls: Option<Bound<'py, PyAny>>,
    zoneinfo_cls: Option<Bound<'py, PyAny>>,
}

impl<'py> PyArrowDecodeHelpers<'py> {
    fn new() -> Self {
        Self {
            decimal_cls: None,
            date_cls: None,
            time_cls: None,
            timedelta_cls: None,
            epoch_naive: None,
            epoch_utc: None,
            timezone_cls: None,
            zoneinfo_cls: None,
        }
    }

    fn decimal_cls(&mut self, py: Python<'py>) -> PyResult<&Bound<'py, PyAny>> {
        if self.decimal_cls.is_none() {
            let cls = py
                .import(pyo3::intern!(py, "decimal"))?
                .getattr(pyo3::intern!(py, "Decimal"))?;
            self.decimal_cls = Some(cls);
        }
        Ok(self.decimal_cls.as_ref().expect("initialized"))
    }

    fn ensure_datetime(&mut self, py: Python<'py>) -> PyResult<()> {
        if self.date_cls.is_none() {
            let dt_mod = py.import(pyo3::intern!(py, "datetime"))?;
            let dt_cls = dt_mod.getattr(pyo3::intern!(py, "datetime"))?;
            let tz_cls = dt_mod.getattr(pyo3::intern!(py, "timezone"))?;
            let utc = tz_cls.getattr(pyo3::intern!(py, "utc"))?;
            let epoch_naive = dt_cls.call1((1970, 1, 1))?;
            let kwargs = PyDict::new(py);
            kwargs.set_item(pyo3::intern!(py, "tzinfo"), &utc)?;
            let epoch_utc = dt_cls.call((1970, 1, 1), Some(&kwargs))?;
            self.date_cls = Some(dt_mod.getattr(pyo3::intern!(py, "date"))?);
            self.time_cls = Some(dt_mod.getattr(pyo3::intern!(py, "time"))?);
            self.timedelta_cls = Some(dt_mod.getattr(pyo3::intern!(py, "timedelta"))?);
            self.epoch_naive = Some(epoch_naive);
            self.epoch_utc = Some(epoch_utc);
            self.timezone_cls = Some(tz_cls);
        }
        Ok(())
    }

    fn date_from_days(&mut self, py: Python<'py>, days: i64) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_datetime(py)?;
        let ordinal = UNIX_EPOCH_ORDINAL
            .checked_add(days)
            .ok_or_else(|| PyValueError::new_err("Arrow date ordinal overflow"))?;
        self.date_cls
            .as_ref()
            .expect("initialized")
            .call_method1(pyo3::intern!(py, "fromordinal"), (ordinal,))
    }

    fn time_from_us(&mut self, py: Python<'py>, total_us: i64) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_datetime(py)?;
        let secs = total_us.div_euclid(1_000_000);
        let us = total_us.rem_euclid(1_000_000);
        let hour = secs / 3_600;
        let min = (secs % 3_600) / 60;
        let sec = secs % 60;
        self.time_cls
            .as_ref()
            .expect("initialized")
            .call1((hour, min, sec, us))
    }

    fn timedelta_from_days_us(
        &mut self,
        py: Python<'py>,
        days: i64,
        us: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_datetime(py)?;
        self.timedelta_cls
            .as_ref()
            .expect("initialized")
            .call1((days, 0, us))
    }

    fn resolve_tzinfo(&mut self, py: Python<'py>, tz: &str) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_datetime(py)?;
        if let Some(rest) = tz.strip_prefix('+').or_else(|| tz.strip_prefix('-')) {
            let sign: i64 = if tz.starts_with('-') { -1 } else { 1 };
            let (hh_s, mm_s) = match rest.split_once(':') {
                Some((h, m)) => (h, m),
                None if rest.len() == 4 => (&rest[..2], &rest[2..]),
                None if rest.len() == 2 => (rest, "00"),
                _ => ("", ""),
            };
            if let (Ok(hh), Ok(mm)) = (hh_s.parse::<i64>(), mm_s.parse::<i64>()) {
                let total_secs = sign * (hh * 3_600 + mm * 60);
                let delta = self
                    .timedelta_cls
                    .as_ref()
                    .expect("initialized")
                    .call1((0, total_secs, 0))?;
                return self
                    .timezone_cls
                    .as_ref()
                    .expect("initialized")
                    .call1((delta,));
            }
        }
        if self.zoneinfo_cls.is_none() {
            let cls = py
                .import(pyo3::intern!(py, "zoneinfo"))?
                .getattr(pyo3::intern!(py, "ZoneInfo"))?;
            self.zoneinfo_cls = Some(cls);
        }
        self.zoneinfo_cls
            .as_ref()
            .expect("initialized")
            .call1((tz,))
    }

    fn timestamp_from_days_us(
        &mut self,
        py: Python<'py>,
        days: i64,
        rem_us: i64,
        tz: Option<&str>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let delta = self.timedelta_from_days_us(py, days, rem_us)?;
        if let Some(tz_str) = tz.filter(|s| !s.is_empty()) {
            let dt_utc = self
                .epoch_utc
                .as_ref()
                .expect("initialized")
                .call_method1(pyo3::intern!(py, "__add__"), (delta,))?;
            if tz_str.eq_ignore_ascii_case("UTC")
                || tz_str == "Z"
                || tz_str == "+00:00"
                || tz_str == "+0000"
            {
                return Ok(dt_utc);
            }
            let tz_obj = self.resolve_tzinfo(py, tz_str)?;
            return dt_utc.call_method1(pyo3::intern!(py, "astimezone"), (tz_obj,));
        }
        self.epoch_naive
            .as_ref()
            .expect("initialized")
            .call_method1(pyo3::intern!(py, "__add__"), (delta,))
    }
}

fn arrow_array_value_to_py<'py>(
    py: Python<'py>,
    arr: &dyn Array,
    row: usize,
    helpers: &mut PyArrowDecodeHelpers<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    if arr.as_any().is::<NullArray>() || arr.is_null(row) {
        return Ok(py.None().into_bound(py));
    }
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return Ok(PyBool::new(py, a.value(row)).to_owned().into_any());
    }
    macro_rules! try_into_py {
        ($($ty:ty),+ $(,)?) => {
            $(
                if let Some(a) = arr.as_any().downcast_ref::<$ty>() {
                    return Ok(a.value(row).into_pyobject(py)?.into_any());
                }
            )+
        };
    }
    try_into_py!(
        Int8Array,
        Int16Array,
        Int32Array,
        Int64Array,
        UInt8Array,
        UInt16Array,
        UInt32Array,
        UInt64Array,
        Float32Array,
        Float64Array,
    );
    if let Some(a) = arr.as_any().downcast_ref::<Float16Array>() {
        return Ok(f32::from(a.value(row)).into_pyobject(py)?.into_any());
    }
    macro_rules! try_str {
        ($($ty:ty),+ $(,)?) => {
            $(
                if let Some(a) = arr.as_any().downcast_ref::<$ty>() {
                    return Ok(PyString::new(py, a.value(row)).into_any());
                }
            )+
        };
    }
    try_str!(StringArray, LargeStringArray, StringViewArray);
    macro_rules! try_bytes {
        ($($ty:ty),+ $(,)?) => {
            $(
                if let Some(a) = arr.as_any().downcast_ref::<$ty>() {
                    return Ok(PyBytes::new(py, a.value(row)).into_any());
                }
            )+
        };
    }
    try_bytes!(
        BinaryArray,
        LargeBinaryArray,
        BinaryViewArray,
        FixedSizeBinaryArray,
    );
    macro_rules! try_decimal {
        ($($ty:ty),+ $(,)?) => {
            $(
                if let Some(a) = arr.as_any().downcast_ref::<$ty>() {
                    return helpers.decimal_cls(py)?.call1((a.value_as_string(row),));
                }
            )+
        };
    }
    try_decimal!(Decimal128Array, Decimal256Array);
    if let Some(a) = arr.as_any().downcast_ref::<Date32Array>() {
        return helpers.date_from_days(py, i64::from(a.value(row)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Date64Array>() {
        return helpers.date_from_days(py, a.value(row).div_euclid(86_400_000));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Time32SecondArray>() {
        return helpers.time_from_us(py, i64::from(a.value(row)) * 1_000_000);
    }
    if let Some(a) = arr.as_any().downcast_ref::<Time32MillisecondArray>() {
        return helpers.time_from_us(py, i64::from(a.value(row)) * 1_000);
    }
    if let Some(a) = arr.as_any().downcast_ref::<Time64MicrosecondArray>() {
        return helpers.time_from_us(py, a.value(row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Time64NanosecondArray>() {
        return helpers.time_from_us(py, a.value(row).div_euclid(1_000));
    }
    macro_rules! try_temporal {
        ($method:ident, $has_tz:tt, $(($ty:ty, $day_div:expr, $us_mul:expr, $pre_div:expr)),+ $(,)?) => {
            $(
                if let Some(a) = arr.as_any().downcast_ref::<$ty>() {
                    let v = a.value(row).div_euclid($pre_div);
                    let days = v.div_euclid($day_div);
                    let us = v.rem_euclid($day_div) * $us_mul;
                    return try_temporal!(@call helpers, $method, py, days, us, a, $has_tz);
                }
            )+
        };
        (@call $helpers:ident, $method:ident, $py:ident, $days:ident, $us:ident, $a:ident, true) => {
            $helpers.$method($py, $days, $us, $a.timezone())
        };
        (@call $helpers:ident, $method:ident, $py:ident, $days:ident, $us:ident, $_a:ident, false) => {
            $helpers.$method($py, $days, $us)
        };
    }
    try_temporal!(
        timestamp_from_days_us,
        true,
        (TimestampSecondArray, 86_400, 1_000_000, 1),
        (TimestampMillisecondArray, 86_400_000, 1_000, 1),
        (TimestampMicrosecondArray, 86_400_000_000, 1, 1),
        (TimestampNanosecondArray, 86_400_000_000, 1, 1_000),
    );
    try_temporal!(
        timedelta_from_days_us,
        false,
        (DurationSecondArray, 86_400, 1_000_000, 1),
        (DurationMillisecondArray, 86_400_000, 1_000, 1),
        (DurationMicrosecondArray, 86_400_000_000, 1, 1),
        (DurationNanosecondArray, 86_400_000_000, 1, 1_000),
    );
    if let Some(a) = arr.as_any().downcast_ref::<IntervalDayTimeArray>() {
        let v = a.value(row);
        return helpers.timedelta_from_days_us(
            py,
            i64::from(v.days),
            i64::from(v.milliseconds) * 1_000,
        );
    }
    if let Some(a) = arr.as_any().downcast_ref::<IntervalYearMonthArray>() {
        return Ok((a.value(row), 0i32, 0i64).into_pyobject(py)?.into_any());
    }
    if let Some(a) = arr.as_any().downcast_ref::<IntervalMonthDayNanoArray>() {
        let v = a.value(row);
        if v.months == 0 {
            return helpers.timedelta_from_days_us(py, i64::from(v.days), v.nanoseconds / 1_000);
        }
        return Ok((v.months, v.days, v.nanoseconds)
            .into_pyobject(py)?
            .into_any());
    }
    macro_rules! try_list {
        ($($ty:ty),+ $(,)?) => {
            $(
                if let Some(a) = arr.as_any().downcast_ref::<$ty>() {
                    let child = a.value(row);
                    let out = PyList::empty(py);
                    for i in 0..child.len() {
                        out.append(arrow_array_value_to_py(py, child.as_ref(), i, helpers)?)?;
                    }
                    return Ok(out.into_any());
                }
            )+
        };
    }
    try_list!(ListArray, LargeListArray, FixedSizeListArray);
    if let Some(a) = arr.as_any().downcast_ref::<StructArray>() {
        let dict = PyDict::new(py);
        for (field, col) in a.fields().iter().zip(a.columns().iter()) {
            let val = arrow_array_value_to_py(py, col.as_ref(), row, helpers)?;
            dict.set_item(field.name(), val)?;
        }
        return Ok(dict.into_any());
    }
    if let Some(a) = arr.as_any().downcast_ref::<MapArray>() {
        let entries = a.value(row);
        let keys_col = entries.column(0);
        let vals_col = entries.column(1);
        let out = PyList::empty(py);
        for i in 0..entries.len() {
            let k = arrow_array_value_to_py(py, keys_col.as_ref(), i, helpers)?;
            let v = arrow_array_value_to_py(py, vals_col.as_ref(), i, helpers)?;
            out.append((k, v))?;
        }
        return Ok(out.into_any());
    }
    let mut dict_lookup = |idx: usize, values: &dyn Array| -> PyResult<Bound<'py, PyAny>> {
        if idx >= values.len() {
            return Err(PyValueError::new_err(
                "Arrow dictionary key index out of bounds",
            ));
        }
        arrow_array_value_to_py(py, values, idx, helpers)
    };
    macro_rules! try_dict {
        ($err:expr; $($key_ty:ty),+ $(,)?) => {
            $(
                if let Some(a) = arr.as_any().downcast_ref::<DictionaryArray<$key_ty>>() {
                    let idx = usize::try_from(a.keys().value(row))
                        .map_err(|_| PyValueError::new_err($err))?;
                    return dict_lookup(idx, a.values().as_ref());
                }
            )+
        };
    }
    try_dict!("negative Arrow dictionary key"; Int8Type, Int16Type, Int32Type, Int64Type);
    try_dict!("Arrow dictionary key overflow"; UInt8Type, UInt16Type, UInt32Type, UInt64Type);
    Err(PyNotImplementedError::new_err(format!(
        "ArrowCursor.to_pylist does not support Arrow data type {:?}",
        arr.data_type()
    )))
}

const PYLIST_SIGNAL_CHECK_INTERVAL: usize = 4096;
const PYLIST_BATCH_SIGNAL_CHECK_MASK: usize = 1023;
const MAX_PYLIST_ROWS: usize = (isize::MAX as usize) / core::mem::size_of::<*mut ffi::PyObject>();

fn append_record_batch_to_pylist<'py>(
    py: Python<'py>,
    batch: &RecordBatch,
    col_keys: &[Bound<'py, PyString>],
    helpers: &mut PyArrowDecodeHelpers<'py>,
    out: &Bound<'py, PyList>,
    processed_rows: &mut usize,
) -> PyResult<()> {
    let cols = batch.columns();
    for row in 0..batch.num_rows() {
        if *processed_rows > 0 && (*processed_rows & (PYLIST_SIGNAL_CHECK_INTERVAL - 1)) == 0 {
            py.detach(|| ());
            py.check_signals()?;
        }
        let dict = PyDict::new(py);
        for (key, col) in col_keys.iter().zip(cols.iter()) {
            let val = arrow_array_value_to_py(py, col.as_ref(), row, helpers)?;
            dict.set_item(key, val)?;
        }
        out.append(dict)?;
        *processed_rows += 1;
    }
    Ok(())
}

fn try_decode_requested_schema<'a, S>(ffi: &'a FFI_ArrowSchema) -> Result<S, S::Error>
where
    S: TryFrom<&'a FFI_ArrowSchema>,
{
    S::try_from(ffi)
}

#[inline]
fn checked_arrow_frame_end(byte_len: usize) -> Option<usize> {
    if byte_len > MAX_ARROW_CURSOR_BYTE_LEN {
        return None;
    }
    ShmArrowFrameHeader::SIZE.checked_add(byte_len)
}

#[inline]
fn normalize_optional_shm_name(shm_name: Option<String>) -> String {
    match shm_name {
        Some(name) if !name.is_empty() => {
            if name.starts_with('/') {
                name
            } else {
                format!("/{name}")
            }
        }
        _ => String::new(),
    }
}

#[derive(Debug)]
struct CapsuleBacking {
    ptr: *mut u8,
    len: usize,
    ctx: *mut c_void,
    drop_fn: unsafe extern "C" fn(*mut c_void),
    unlink_fn: Option<unsafe extern "C" fn(*mut c_void) -> bool>,
    unlinked: AtomicBool,
}

// SAFETY: `CapsuleBacking` exclusively owns a thread-safe `ShmCapsule` context (`Arc<ShmRegion>`)
// claimed via atomic `AcqRel` swap, and releases it once in `Drop`.
unsafe impl Send for CapsuleBacking {}
// SAFETY: `ptr` references immutable published `VTBARROW` shared memory, `unlink_now` synchronizes
// via `AtomicBool` and `ShmRegion`'s internal `Mutex`, and `ctx` is only released in `Drop`.
unsafe impl Sync for CapsuleBacking {}

impl CapsuleBacking {
    fn unlink_now(&self) -> bool {
        if self.ctx.is_null() || self.unlinked.swap(true, Ordering::AcqRel) {
            return false;
        }
        let Some(unlink_fn) = self.unlink_fn else {
            return false;
        };
        // SAFETY: `self.ctx` remains live for the lifetime of `self` because `drop_fn` is only
        // invoked in `CapsuleBacking::drop` when the last `Arc<CapsuleBacking>` reference drops.
        unsafe { unlink_fn(self.ctx) }
    }
}

impl Drop for CapsuleBacking {
    fn drop(&mut self) {
        if !self.ctx.is_null() {
            // SAFETY: `CapsuleBacking` exclusively owns `self.ctx` after `_from_shm_capsule`
            // atomically clears the `PyCapsule` slot, and drops it once when the last `Arc` drops.
            unsafe {
                (self.drop_fn)(self.ctx);
            }
        }
    }
}

#[derive(Clone, Debug)]
enum ArrowBacking {
    Region(Arc<ShmRegion>),
    Capsule(Arc<CapsuleBacking>),
    Bytes(Arc<[u8]>),
}

impl ArrowBacking {
    #[inline]
    fn as_const_ptr_and_len(&self) -> (*const u8, usize) {
        match self {
            Self::Region(r) => (r.as_ptr().cast_const(), r.len()),
            Self::Capsule(c) => (c.ptr.cast_const(), c.len),
            Self::Bytes(b) => (b.as_ptr(), b.len()),
        }
    }

    #[inline]
    fn checked_bounds(&self, byte_len: usize) -> Option<(usize, usize)> {
        match self {
            Self::Region(_) | Self::Capsule(_) => {
                checked_arrow_frame_end(byte_len).map(|end| (ShmArrowFrameHeader::SIZE, end))
            }
            Self::Bytes(_) => (byte_len <= isize::MAX as usize).then_some((0, byte_len)),
        }
    }
}

/// Shared-memory or in-memory Arrow IPC cursor backed by `vertebra_core::shm::ShmRegion` or `Arc<[u8]>`.
#[pyclass(frozen, immutable_type, from_py_object)]
#[derive(Clone, Debug)]
pub struct ArrowCursor {
    #[pyo3(get)]
    pub shm_name: String,
    #[pyo3(get)]
    pub byte_len: usize,
    #[pyo3(get)]
    pub row_count: usize,
    columns_cache: Option<Arc<[String]>>,
    backing: Option<ArrowBacking>,
    released: Arc<AtomicBool>,
    validated: Arc<AtomicBool>,
}

impl ArrowCursor {
    pub fn from_shm_region(
        shm_name: String,
        byte_len: usize,
        row_count: usize,
        region: Arc<ShmRegion>,
    ) -> Self {
        Self {
            shm_name,
            byte_len,
            row_count,
            columns_cache: None,
            backing: Some(ArrowBacking::Region(region)),
            released: Arc::new(AtomicBool::new(false)),
            validated: Arc::new(AtomicBool::new(false)),
        }
    }

    fn resolve_arrow_backing(&self) -> PyResult<OwnedArrowIpcSlice> {
        if self.released.load(Ordering::Acquire) {
            return Err(PyValueError::new_err("ArrowCursor has been released"));
        }
        if let Some(ref backing) = self.backing {
            return OwnedArrowIpcSlice::try_new(backing.clone(), self.byte_len)
                .map_err(PyValueError::new_err);
        }
        let end = checked_arrow_frame_end(self.byte_len)
            .ok_or_else(|| PyValueError::new_err("ArrowCursor byte_len overflow"))?;
        if self.shm_name.is_empty() {
            return Err(PyValueError::new_err(
                "ArrowCursor has no shared-memory backing",
            ));
        }
        let (region, header_len) =
            open_shm_arrow_region(&self.shm_name).map_err(PyValueError::new_err)?;
        let required = checked_arrow_frame_end(header_len)
            .ok_or_else(|| PyValueError::new_err("ArrowCursor byte_len overflow"))?;
        if region.len() < required || region.len() < end {
            return Err(PyValueError::new_err(
                "ArrowCursor ShmRegion smaller than declared byte_len",
            ));
        }
        OwnedArrowIpcSlice::try_new(ArrowBacking::Region(region), self.byte_len)
            .map_err(PyValueError::new_err)
    }
}

#[pymethods]
impl ArrowCursor {
    #[new]
    #[pyo3(signature = (shm_name, byte_len, row_count=0))]
    pub fn new(shm_name: String, byte_len: usize, row_count: usize) -> PyResult<Self> {
        let required = checked_arrow_frame_end(byte_len)
            .ok_or_else(|| PyValueError::new_err("ArrowCursor byte_len overflow"))?;
        let norm_name = normalize_optional_shm_name(Some(shm_name));
        let backing = if norm_name.is_empty() {
            None
        } else {
            open_shm_arrow_region(&norm_name)
                .ok()
                .filter(|(r, _)| r.len() >= required)
                .map(|(r, _)| ArrowBacking::Region(r))
        };
        Ok(Self {
            shm_name: norm_name,
            byte_len,
            row_count,
            columns_cache: None,
            backing,
            released: Arc::new(AtomicBool::new(false)),
            validated: Arc::new(AtomicBool::new(false)),
        })
    }

    #[staticmethod]
    pub fn from_ipc_bytes(py: Python<'_>, payload: &Bound<'_, PyAny>) -> PyResult<Self> {
        let bytes = extract_ipc_buffer_arc(py, payload)?;
        if bytes.len() > isize::MAX as usize {
            return Err(PyValueError::new_err("ArrowCursor byte_len overflow"));
        }
        let byte_len = bytes.len();
        let bytes_for_scan = Arc::clone(&bytes);
        let (row_count, columns) = py
            .detach(move || validate_and_summarize_ipc_bytes(bytes_for_scan))
            .map_err(PyValueError::new_err)?;
        Ok(Self {
            shm_name: String::new(),
            byte_len,
            row_count,
            columns_cache: Some(columns),
            backing: Some(ArrowBacking::Bytes(bytes)),
            released: Arc::new(AtomicBool::new(false)),
            validated: Arc::new(AtomicBool::new(true)),
        })
    }

    #[getter]
    pub fn columns(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        if self.released.load(Ordering::Acquire) {
            return Err(PyValueError::new_err("ArrowCursor has been released"));
        }
        if let Some(ref cols) = self.columns_cache {
            return Ok(cols.to_vec());
        }
        let carrier = self.resolve_arrow_backing()?;
        py.detach(move || -> Result<Vec<String>, String> {
            let schema = parse_ipc_schema_only(carrier.as_ref())?;
            Ok(schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>())
        })
        .map_err(PyValueError::new_err)
    }

    #[staticmethod]
    #[pyo3(signature = (shm_name, payload, row_count=0))]
    pub fn write_ipc_to_shm(
        py: Python<'_>,
        shm_name: &str,
        payload: &[u8],
        row_count: usize,
    ) -> PyResult<Self> {
        let (norm_name, len, region) = py
            .detach(|| write_shm_arrow_region(shm_name, payload))
            .map_err(PyRuntimeError::new_err)?;
        Ok(Self {
            shm_name: norm_name,
            byte_len: len,
            row_count,
            columns_cache: None,
            backing: Some(ArrowBacking::Region(region)),
            released: Arc::new(AtomicBool::new(false)),
            validated: Arc::new(AtomicBool::new(false)),
        })
    }

    #[staticmethod]
    pub fn _allocate_shm_capsule<'py>(
        py: Python<'py>,
        shm_name: &str,
        payload: &[u8],
    ) -> PyResult<(String, usize, Bound<'py, PyAny>)> {
        let (norm_name, len, region) = py
            .detach(|| write_shm_arrow_region_unregistered(shm_name, payload))
            .map_err(PyRuntimeError::new_err)?;
        let capsule = wrap_shm_region_capsule(py, region)?;
        Ok((norm_name, len, capsule))
    }

    #[staticmethod]
    #[pyo3(signature = (shm_name, byte_len, row_count, capsule))]
    pub fn _from_shm_capsule(
        py: Python<'_>,
        shm_name: Option<String>,
        byte_len: usize,
        row_count: usize,
        capsule: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        if byte_len > MAX_ARROW_CURSOR_BYTE_LEN {
            return Err(PyValueError::new_err("ArrowCursor byte_len overflow"));
        }
        let required = ShmArrowFrameHeader::SIZE
            .checked_add(byte_len)
            .ok_or_else(|| PyValueError::new_err("ArrowCursor byte_len overflow"))?;

        // SAFETY: `capsule.as_ptr()` is a valid borrowed `PyObject*` and `SHM_CAPSULE_NAME` is a
        // static NUL-terminated C string; CPython checks exact `PyCapsule` type and name.
        let raw = unsafe { ffi::PyCapsule_GetPointer(capsule.as_ptr(), SHM_CAPSULE_NAME.as_ptr()) }
            .cast::<ShmCapsule>();
        if raw.is_null() {
            return Err(PyErr::fetch(py));
        }

        // SAFETY: `raw` points to a valid `#[repr(C)]` `ShmCapsule` kept alive by `capsule`;
        // shared immutable access avoids aliasing UB under concurrent free-threaded calls.
        let cap_ref: &ShmCapsule = unsafe { &*(raw as *const ShmCapsule) };
        if cap_ref.ptr.is_null() {
            return Err(PyValueError::new_err("ShmCapsule mapping pointer is null"));
        }
        if cap_ref.len < required {
            return Err(PyValueError::new_err(
                "ShmCapsule mapping smaller than declared VTBARROW frame length",
            ));
        }

        let ctx = cap_ref.ctx.swap(core::ptr::null_mut(), Ordering::AcqRel);
        if ctx.is_null() {
            return Err(PyValueError::new_err("ShmCapsule already consumed"));
        }

        let norm_name = normalize_optional_shm_name(shm_name);
        Ok(Self {
            shm_name: norm_name,
            byte_len,
            row_count,
            columns_cache: None,
            backing: Some(ArrowBacking::Capsule(Arc::new(CapsuleBacking {
                ptr: cap_ref.ptr,
                len: cap_ref.len,
                ctx,
                drop_fn: cap_ref.drop_fn,
                unlink_fn: cap_ref.unlink_fn,
                unlinked: AtomicBool::new(false),
            }))),
            released: Arc::new(AtomicBool::new(false)),
            validated: Arc::new(AtomicBool::new(false)),
        })
    }

    #[staticmethod]
    pub fn read_ipc_from_shm<'py>(
        py: Python<'py>,
        shm_name: &str,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let payload = py
            .detach(|| read_shm_arrow_frame(shm_name))
            .map_err(PyRuntimeError::new_err)?;
        Ok(PyBytes::new(py, &payload))
    }

    pub fn read_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        if self.released.load(Ordering::Acquire) {
            return Err(PyValueError::new_err("ArrowCursor has been released"));
        }
        if let Some(ref backing) = self.backing {
            let (start, end) = backing
                .checked_bounds(self.byte_len)
                .ok_or_else(|| PyValueError::new_err("ArrowCursor byte_len overflow"))?;
            let (ptr, len) = backing.as_const_ptr_and_len();
            if !ptr.is_null() && len >= end {
                return PyBytes::new_with(py, end - start, |buf| {
                    let carrier = OwnedArrowIpcSlice {
                        backing: backing.clone(),
                        start,
                        end,
                    };
                    buf.copy_from_slice(carrier.as_ref());
                    Ok(())
                });
            }
        }
        if self.shm_name.is_empty() {
            return Err(PyValueError::new_err(
                "ArrowCursor has no shared-memory backing",
            ));
        }
        let _ = checked_arrow_frame_end(self.byte_len)
            .ok_or_else(|| PyValueError::new_err("ArrowCursor byte_len overflow"))?;
        Self::read_ipc_from_shm(py, &self.shm_name)
    }

    pub fn buffer_view<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        // SAFETY: `slf.as_ptr()` is a valid borrowed `PyObject*` implementing the buffer protocol.
        let ptr = unsafe { ffi::PyMemoryView_FromObject(slf.as_ptr()) };
        if ptr.is_null() {
            return Err(PyErr::fetch(py));
        }
        // SAFETY: `ptr` is a non-null owned `PyObject*` returned by `PyMemoryView_FromObject`.
        Ok(unsafe { Bound::from_owned_ptr(py, ptr) })
    }

    pub fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let carrier = self.resolve_arrow_backing()?;
        let ffi_schema = py
            .detach(move || {
                let schema = parse_ipc_schema_only(carrier.as_ref())?;
                FFI_ArrowSchema::try_from(schema.as_ref())
                    .map_err(|e| format!("failed to export FFI_ArrowSchema: {e}"))
            })
            .map_err(PyValueError::new_err)?;
        PyCapsule::new_with_value(py, ffi_schema, ARROW_SCHEMA_CAPSULE_NAME)
    }

    #[pyo3(signature = (requested_schema=None))]
    pub fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let requested_ffi_schema = if let Some(req) = requested_schema {
            let cap = req.cast::<PyCapsule>().map_err(|_| {
                PyValueError::new_err("requested_schema must be an arrow_schema PyCapsule")
            })?;
            let ptr = cap
                .pointer_checked(Some(ARROW_SCHEMA_CAPSULE_NAME))
                .map_err(|_| {
                    PyValueError::new_err("requested_schema PyCapsule must be named arrow_schema")
                })?;
            // SAFETY: `pointer_checked` verified the `arrow_schema` capsule name and a
            // non-null `FFI_ArrowSchema*` kept alive by `cap` for this call.
            let ffi_schema = unsafe { &*ptr.as_ptr().cast::<FFI_ArrowSchema>() };
            if ffi_schema.release().is_none() {
                return Err(PyValueError::new_err(
                    "requested_schema arrow_schema PyCapsule has already been released",
                ));
            }
            Some(ffi_schema)
        } else {
            None
        };

        let carrier = self.resolve_arrow_backing()?;
        let skip_frame_scan = self.validated.load(Ordering::Acquire);
        let validated_flag =
            (!skip_frame_scan && self.backing.is_some()).then(|| Arc::clone(&self.validated));
        let batch_reader = py
            .detach(move || open_lazy_arrow_ipc_stream(carrier, skip_frame_scan, validated_flag))
            .map_err(PyValueError::new_err)?;

        if let Some(ffi_schema) = requested_ffi_schema {
            let stream_schema = batch_reader.schema();
            let requested = try_decode_requested_schema(ffi_schema).map_err(|e| {
                PyValueError::new_err(format!("invalid requested_schema FFI_ArrowSchema: {e}"))
            })?;
            if *stream_schema != requested {
                return Err(PyNotImplementedError::new_err(
                    "ArrowCursor.__arrow_c_stream__ does not support schema casting",
                ));
            }
        }

        let ffi_stream = FFI_ArrowArrayStream::new(Box::new(batch_reader));
        PyCapsule::new_with_value(py, ffi_stream, ARROW_ARRAY_STREAM_CAPSULE_NAME)
    }

    pub fn to_pyarrow<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let this = slf.get();
        if this.released.load(Ordering::Acquire) {
            return Err(PyValueError::new_err("ArrowCursor has been released"));
        }
        let pa = py.import(pyo3::intern!(py, "pyarrow"))?;
        let pa_buf = if let Some(ref backing) = this.backing {
            let (start, end) = backing
                .checked_bounds(this.byte_len)
                .ok_or_else(|| PyBufferError::new_err("ArrowCursor byte_len overflow"))?;
            let (base_ptr, region_len) = backing.as_const_ptr_and_len();
            if base_ptr.is_null() || region_len < end {
                return Err(PyBufferError::new_err(
                    "ArrowCursor ShmRegion smaller than declared byte_len",
                ));
            }
            // SAFETY: `base_ptr` is non-null with `start <= end <= region_len` and
            // `end <= isize::MAX`; `base=slf` pins `this.backing` inside PyArrow's foreign buffer.
            let payload_ptr = unsafe { base_ptr.add(start) };
            let kwargs = PyDict::new(py);
            kwargs.set_item(pyo3::intern!(py, "base"), &slf)?;
            pa.call_method(
                pyo3::intern!(py, "foreign_buffer"),
                (payload_ptr as usize, this.byte_len),
                Some(&kwargs),
            )?
        } else {
            let mv = Self::buffer_view(slf)?;
            pa.call_method1(pyo3::intern!(py, "py_buffer"), (mv,))?
        };
        let reader = pa
            .getattr(pyo3::intern!(py, "ipc"))?
            .call_method1(pyo3::intern!(py, "open_stream"), (pa_buf,))?;
        reader.call_method0(pyo3::intern!(py, "read_all"))
    }

    pub fn to_arrow_table<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        Self::to_pyarrow(slf)
    }

    pub fn to_polars<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        if slf.get().released.load(Ordering::Acquire) {
            return Err(PyValueError::new_err("ArrowCursor has been released"));
        }
        let pl = py.import(pyo3::intern!(py, "polars"))?;
        let out = pl.call_method1(pyo3::intern!(py, "from_arrow"), (slf,))?;
        if let Ok(series_cls) = pl.getattr(pyo3::intern!(py, "Series"))
            && out.is_instance(&series_cls)?
        {
            return out
                .getattr(pyo3::intern!(py, "struct"))?
                .call_method0(pyo3::intern!(py, "unnest"));
        }
        Ok(out)
    }

    pub fn to_pylist<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let carrier = self.resolve_arrow_backing()?;
        let skip_frame_scan = self.validated.load(Ordering::Acquire);
        let validated_flag =
            (!skip_frame_scan && self.backing.is_some()).then(|| Arc::clone(&self.validated));
        let mut reader = py
            .detach(move || open_lazy_arrow_ipc_stream(carrier, skip_frame_scan, validated_flag))
            .map_err(PyValueError::new_err)?;
        let col_keys: Vec<Bound<'py, PyString>> = reader
            .schema()
            .fields()
            .iter()
            .map(|f| PyString::new(py, f.name()))
            .collect();
        let out = PyList::empty(py);
        let mut helpers = PyArrowDecodeHelpers::new();
        let mut total_rows = 0usize;
        let mut processed_rows = 0usize;
        let mut batch_count = 0usize;
        loop {
            if batch_count > 0 && (batch_count & PYLIST_BATCH_SIGNAL_CHECK_MASK) == 0 {
                py.detach(|| ());
                py.check_signals()?;
            }
            batch_count += 1;
            let next_res = py.detach(|| reader.next());
            let Some(batch_res) = next_res else {
                break;
            };
            let batch = batch_res
                .map_err(|e| PyValueError::new_err(format!("corrupt Arrow IPC batch: {e}")))?;
            total_rows = total_rows
                .checked_add(batch.num_rows())
                .filter(|&count| count <= MAX_PYLIST_ROWS)
                .ok_or_else(|| {
                    PyValueError::new_err(format!(
                        "ArrowCursor.to_pylist row count exceeds maximum Python list size ({MAX_PYLIST_ROWS})"
                    ))
                })?;
            append_record_batch_to_pylist(
                py,
                &batch,
                &col_keys,
                &mut helpers,
                &out,
                &mut processed_rows,
            )?;
        }
        Ok(out)
    }

    pub fn release(&self, py: Python<'_>) -> bool {
        let was_released = self.released.swap(true, Ordering::AcqRel);
        let name = self.shm_name.clone();
        let backing = self.backing.clone();
        py.detach(move || {
            let removed = release_shm_arrow_frame(&name);
            match backing {
                Some(ArrowBacking::Region(region)) => {
                    #[cfg(unix)]
                    let unlinked = region.unlink_now();
                    #[cfg(not(unix))]
                    let unlinked = {
                        let _ = region;
                        false
                    };
                    removed || unlinked
                }
                Some(ArrowBacking::Capsule(cap)) => {
                    let unlinked = cap.unlink_now();
                    removed || unlinked
                }
                Some(ArrowBacking::Bytes(_)) => !was_released,
                None => removed,
            }
        })
    }

    pub fn close(&self, py: Python<'_>) -> bool {
        self.release(py)
    }

    pub fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (exc_type=None, exc_val=None, exc_tb=None))]
    pub fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: Option<&Bound<'_, PyAny>>,
        exc_val: Option<&Bound<'_, PyAny>>,
        exc_tb: Option<&Bound<'_, PyAny>>,
    ) {
        let _ = (exc_type, exc_val, exc_tb);
        let _ = self.release(py);
    }

    // SAFETY: Called by CPython's buffer protocol with the GIL attached (`slf` is a live
    // `ArrowCursor`, `view` is checked non-null). `PyBuffer_FillInfo` sets `(*view).obj` to an
    // incremented owning reference to `slf`, pinning `this.backing` (`Arc<ShmRegion>`,
    // `Arc<CapsuleBacking>`, or `Arc<[u8]>`) for the lifetime of any exported `memoryview`.
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let py = slf.py();
        if view.is_null() {
            return Err(PyBufferError::new_err("Py_buffer pointer is null"));
        }
        if (flags & ffi::PyBUF_WRITABLE) == ffi::PyBUF_WRITABLE {
            return Err(PyBufferError::new_err("ArrowCursor buffer is read-only"));
        }
        let this = slf.get();
        if this.released.load(Ordering::Acquire) {
            return Err(PyBufferError::new_err("ArrowCursor has been released"));
        }
        let Some(ref backing) = this.backing else {
            return Err(PyBufferError::new_err(
                "ArrowCursor has no anchored ShmRegion",
            ));
        };
        let Some((start, end)) = backing.checked_bounds(this.byte_len) else {
            return Err(PyBufferError::new_err("ArrowCursor byte_len overflow"));
        };
        let (base_ptr, region_len) = backing.as_const_ptr_and_len();
        if base_ptr.is_null() || region_len < end {
            return Err(PyBufferError::new_err(
                "ArrowCursor ShmRegion smaller than declared byte_len",
            ));
        }
        // SAFETY: `base_ptr` is non-null with `start <= end <= region_len` and
        // `end <= isize::MAX as usize`, `view` is a valid non-null `Py_buffer*`, `readonly` is `1`,
        // and `slf.as_ptr()` is a borrowed reference whose refcount `PyBuffer_FillInfo` increments
        // to keep `this.backing` alive until the exported buffer is released.
        let ret = unsafe {
            let ptr = base_ptr.add(start).cast_mut().cast::<c_void>();
            ffi::PyBuffer_FillInfo(
                view,
                slf.as_ptr(),
                ptr,
                this.byte_len as ffi::Py_ssize_t,
                1,
                flags,
            )
        };
        if ret == -1 {
            return Err(PyErr::fetch(py));
        }
        Ok(())
    }

    // SAFETY: Paired PEP 3118 release hook invoked by CPython on `PyBuffer_Release`; `ArrowCursor`
    // allocates no per-view state and `(*view).obj` drop releases the pinned exporter reference.
    unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer) {}
}

/// Encode a 4-byte length-prefixed AlloyDB `MetadataExchangeRequest` protobuf wire frame.
#[pyfunction]
#[pyo3(signature = (user_agent="vertebra-db/0.1", oauth2_token="", iam_auth=true))]
pub fn encode_alloydb_metadata_exchange<'py>(
    py: Python<'py>,
    user_agent: &str,
    oauth2_token: &str,
    iam_auth: bool,
) -> PyResult<Bound<'py, PyBytes>> {
    let auth = if iam_auth {
        AuthType::Iam
    } else {
        AuthType::Password
    };
    let bytes = rs_encode_alloydb_metadata_exchange(user_agent, oauth2_token, auth);
    PyBytes::new_with(py, bytes.len(), |buf| {
        buf.copy_from_slice(&bytes);
        Ok(())
    })
}

/// Decode a 4-byte length-prefixed AlloyDB `MetadataExchangeResponse` protobuf wire frame.
#[pyfunction]
pub fn decode_alloydb_metadata_response(data: &[u8]) -> PyResult<(u32, String)> {
    rs_decode_alloydb_metadata_response(data).map_err(PyValueError::new_err)
}

/// Strip the 1-byte PostgreSQL binary `JSONB` version header (`0x01`).
#[pyfunction]
pub fn strip_pg_jsonb_header<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
    let stripped = rs_strip_pg_jsonb_header(data).map_err(PyValueError::new_err)?;
    PyBytes::new_with(py, stripped.len(), |buf| {
        buf.copy_from_slice(stripped);
        Ok(())
    })
}

/// Prepend the 1-byte PostgreSQL binary `JSONB` version header (`0x01`).
#[pyfunction]
pub fn encode_pg_jsonb<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
    PyBytes::new_with(py, 1 + data.len(), |buf| {
        buf[0] = 0x01;
        buf[1..].copy_from_slice(data);
        Ok(())
    })
}

/// Type alias for a batch of binary PostgreSQL `COPY` rows.
pub type PgCopyRowBatch = Vec<Vec<Option<Vec<u8>>>>;

/// Type alias for a resolved Oracle TNS alias tuple `(host, port, service_name, sid, security_cert_dn)`.
pub type ResolvedTnsAlias = (String, u16, Option<String>, Option<String>, Option<String>);

/// Encode rows into a PostgreSQL binary `COPY` stream (`PGCOPY\n\xff\r\n\0`).
#[pyfunction]
pub fn encode_pgcopy_rows<'py>(
    py: Python<'py>,
    rows: PgCopyRowBatch,
) -> PyResult<Bound<'py, PyBytes>> {
    let encoded = py.detach(move || encode_pgcopy_binary_rows(&rows));
    PyBytes::new_with(py, encoded.len(), |buf| {
        buf.copy_from_slice(&encoded);
        Ok(())
    })
}

/// Decode a PostgreSQL binary `COPY` stream (`PGCOPY\n\xff\r\n\0`) into rows of optional byte slices.
#[pyfunction]
pub fn decode_pgcopy_rows(py: Python<'_>, data: &[u8]) -> PyResult<PgCopyRowBatch> {
    py.detach(|| decode_pgcopy_binary_rows(data))
        .map_err(PyValueError::new_err)
}

/// Parse an Oracle `tnsnames.ora` file into a dictionary of uppercase alias -> descriptor string.
#[pyfunction]
pub fn parse_tnsnames_ora(py: Python<'_>, contents: &str) -> HashMap<String, String> {
    py.detach(|| rs_parse_tnsnames_ora(contents))
}

/// Resolve a TNS alias from `tnsnames.ora` into `(host, port, service_name, sid, security_cert_dn)`.
#[pyfunction]
pub fn resolve_tns_alias(py: Python<'_>, contents: &str, alias: &str) -> Option<ResolvedTnsAlias> {
    py.detach(|| rs_resolve_tns_alias(contents, alias))
        .map(|d| (d.host, d.port, d.service_name, d.sid, d.security_cert_dn))
}

fn with_contiguous_slice_or_vec<T, R, F>(obj: &Bound<'_, PyAny>, f: F) -> PyResult<R>
where
    T: pyo3::buffer::Element + for<'a, 'py> pyo3::FromPyObject<'a, 'py>,
    for<'a, 'py> <T as pyo3::FromPyObject<'a, 'py>>::Error: Into<PyErr>,
    F: FnOnce(&[T]) -> Result<R, String>,
{
    if let Ok(buf) = PyBuffer::<T>::get(obj)
        && buf.is_c_contiguous()
    {
        let len = buf.item_count();
        let ptr = buf.buf_ptr() as *const T;
        let slice = if len == 0 || ptr.is_null() {
            &[]
        } else {
            // SAFETY: `buf` pins a contiguous `T` PEP 3118 buffer while the GIL is held in `f`.
            unsafe { std::slice::from_raw_parts(ptr, len) }
        };
        return f(slice).map_err(PyValueError::new_err);
    }
    let vec: Vec<T> = obj.extract()?;
    f(&vec).map_err(PyValueError::new_err)
}

fn with_f32_slice_or_vec<R, F>(obj: &Bound<'_, PyAny>, f: F) -> PyResult<R>
where
    F: FnOnce(&[f32]) -> Result<R, String>,
{
    if let Ok(view_ref) = obj.extract::<PyRef<'_, VectorView>>() {
        return f(view_ref.row_f32()?).map_err(PyValueError::new_err);
    }
    with_contiguous_slice_or_vec(obj, f)
}

fn with_i8_slice_or_vec<R, F>(obj: &Bound<'_, PyAny>, f: F) -> PyResult<R>
where
    F: FnOnce(&[i8]) -> Result<R, String>,
{
    if let Ok(view_ref) = obj.extract::<PyRef<'_, VectorView>>() {
        return f(view_ref.row_i8()?).map_err(PyValueError::new_err);
    }
    with_contiguous_slice_or_vec(obj, f)
}

fn with_u8_slice_or_vec<R, F>(obj: &Bound<'_, PyAny>, f: F) -> PyResult<R>
where
    F: FnOnce(&[u8]) -> Result<R, String>,
{
    if let Ok(view_ref) = obj.extract::<PyRef<'_, VectorView>>() {
        return f(view_ref.row_bytes()?).map_err(PyValueError::new_err);
    }
    if let Ok(bytes) = obj.extract::<&[u8]>() {
        return f(bytes).map_err(PyValueError::new_err);
    }
    with_contiguous_slice_or_vec(obj, f)
}

fn bytes_to_py<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
    PyBytes::new_with(py, data.len(), |buf| {
        buf.copy_from_slice(data);
        Ok(())
    })
}

macro_rules! define_vector_codec_pair {
    (
        $(#[$enc_meta:meta])*
        $enc_name:ident ($arg:ident, $helper:ident, $rs_enc:ident => ok),
        $(#[$dec_meta:meta])*
        $dec_name:ident -> $dec_ty:ty, $rs_dec:ident
    ) => {
        $(#[$enc_meta])*
        #[pyfunction]
        pub fn $enc_name<'py>(
            py: Python<'py>,
            $arg: &Bound<'py, PyAny>,
        ) -> PyResult<Bound<'py, PyBytes>> {
            let bytes = $helper($arg, |slice| Ok($rs_enc(slice)))?;
            bytes_to_py(py, &bytes)
        }

        $(#[$dec_meta])*
        #[pyfunction]
        pub fn $dec_name(py: Python<'_>, data: &[u8]) -> PyResult<Vec<$dec_ty>> {
            py.detach(|| $rs_dec(data)).map_err(PyValueError::new_err)
        }
    };
    (
        $(#[$enc_meta:meta])*
        $enc_name:ident ($arg:ident, $helper:ident, $rs_enc:ident => res),
        $(#[$dec_meta:meta])*
        $dec_name:ident -> $dec_ty:ty, $rs_dec:ident
    ) => {
        $(#[$enc_meta])*
        #[pyfunction]
        pub fn $enc_name<'py>(
            py: Python<'py>,
            $arg: &Bound<'py, PyAny>,
        ) -> PyResult<Bound<'py, PyBytes>> {
            let bytes = $helper($arg, $rs_enc)?;
            bytes_to_py(py, &bytes)
        }

        $(#[$dec_meta])*
        #[pyfunction]
        pub fn $dec_name(py: Python<'_>, data: &[u8]) -> PyResult<Vec<$dec_ty>> {
            py.detach(|| $rs_dec(data)).map_err(PyValueError::new_err)
        }
    };
}

define_vector_codec_pair!(
    /// Encode an Oracle 26ai `VECTOR(*, FLOAT32)` binary frame.
    encode_oracle_vector_f32(values, with_f32_slice_or_vec, rs_encode_oracle_vector_f32 => ok),
    /// Decode an Oracle 26ai `VECTOR(*, FLOAT32)` binary frame.
    decode_oracle_vector_f32 -> f32, rs_decode_oracle_vector_f32
);
define_vector_codec_pair!(
    /// Encode an Oracle 26ai `VECTOR(*, FLOAT64)` binary frame.
    encode_oracle_vector_f64(values, with_contiguous_slice_or_vec, rs_encode_oracle_vector_f64 => ok),
    /// Decode an Oracle 26ai `VECTOR(*, FLOAT64)` binary frame.
    decode_oracle_vector_f64 -> f64, rs_decode_oracle_vector_f64
);
define_vector_codec_pair!(
    /// Encode an Oracle 26ai `VECTOR(*, INT8)` binary frame.
    encode_oracle_vector_i8(values, with_i8_slice_or_vec, rs_encode_oracle_vector_i8 => ok),
    /// Decode an Oracle 26ai `VECTOR(*, INT8)` binary frame.
    decode_oracle_vector_i8 -> i8, rs_decode_oracle_vector_i8
);
define_vector_codec_pair!(
    /// Encode an Oracle 26ai `VECTOR(*, BINARY)` packed bit-vector binary frame.
    encode_oracle_vector_binary(bits, with_u8_slice_or_vec, rs_encode_oracle_vector_binary => ok),
    /// Decode an Oracle 26ai `VECTOR(*, BINARY)` packed bit-vector binary frame.
    decode_oracle_vector_binary -> u8, rs_decode_oracle_vector_binary
);
define_vector_codec_pair!(
    /// Encode a `pgvector` binary `vector` (`[u16 dim BE][u16 unused=0 BE][f32 BE; dim]`).
    encode_pgvector_f32(values, with_f32_slice_or_vec, rs_encode_pgvector_f32 => res),
    /// Decode a `pgvector` binary `vector` (`[u16 dim BE][u16 unused=0 BE][f32 BE; dim]`).
    decode_pgvector_f32 -> f32, rs_decode_pgvector_f32
);
define_vector_codec_pair!(
    /// Encode a `pgvector` binary `halfvec` (`[u16 dim BE][u16 unused=0 BE][f16 BE; dim]`).
    encode_pgvector_f16(values, with_f32_slice_or_vec, rs_encode_pgvector_f16 => res),
    /// Decode a `pgvector` binary `halfvec` (`[u16 dim BE][u16 unused=0 BE][f16 BE; dim]`).
    decode_pgvector_f16 -> f32, rs_decode_pgvector_f16
);

/// Encode a PostgreSQL / `pgvector` binary `bit` (`VARBIT`).
#[pyfunction]
pub fn encode_pgvector_bit<'py>(
    py: Python<'py>,
    packed_bytes: &Bound<'py, PyAny>,
    bit_len: usize,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = with_u8_slice_or_vec(packed_bytes, |slice| rs_encode_pgvector_bit(slice, bit_len))?;
    bytes_to_py(py, &bytes)
}

/// Decode a PostgreSQL / `pgvector` binary `bit` (`VARBIT`) into `(packed_bytes, bit_len)`.
#[pyfunction]
pub fn decode_pgvector_bit<'py>(
    py: Python<'py>,
    data: &[u8],
) -> PyResult<(Bound<'py, PyBytes>, usize)> {
    let (raw, bit_len) = py
        .detach(|| rs_decode_pgvector_bit(data))
        .map_err(PyValueError::new_err)?;
    Ok((bytes_to_py(py, &raw)?, bit_len))
}

/// Encode JSON bytes with an Oracle 26ai `OSON` binary header (`\xff\x4a\x5a`).
#[pyfunction]
pub fn encode_oson_payload<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = py.detach(|| rs_encode_oson_payload(data));
    bytes_to_py(py, &bytes)
}

/// Validate an Oracle 26ai `OSON` binary header and return the underlying JSON payload bytes.
#[pyfunction]
pub fn decode_oson_payload<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
    let slice = rs_decode_oson_payload(data).map_err(PyValueError::new_err)?;
    bytes_to_py(py, slice)
}

/// Encode zero-RTT piggybacked Oracle TTC session attributes (`CLIENT_IDENTIFIER`, `MODULE`, `ACTION`, `ECID`).
#[pyfunction]
#[pyo3(signature = (client_id=None, module=None, action=None, ecid=None))]
pub fn encode_ttc_session_attrs<'py>(
    py: Python<'py>,
    client_id: Option<String>,
    module: Option<String>,
    action: Option<String>,
    ecid: Option<String>,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = py.detach(move || {
        let attrs = OracleSessionAttrs {
            client_identifier: client_id,
            module,
            action,
            ecid,
        };
        attrs.encode_ttc_piggyback_session_attrs()
    });
    PyBytes::new_with(py, bytes.len(), |buf| {
        buf.copy_from_slice(&bytes);
        Ok(())
    })
}

/// Rewrite `?` and `$1`/`$2` placeholders to SQL Server `@P1, @P2, ...` while preserving literals and comments.
#[pyfunction]
pub fn rewrite_mssql_placeholders(py: Python<'_>, sql: &str) -> (String, usize) {
    py.detach(|| rs_rewrite_mssql_placeholders(sql))
}

/// Generate DuckDB 2.0-safe post-initialization `SET` statements.
#[pyfunction]
#[pyo3(signature = (
    path=":memory:",
    memory_limit_mb=2048,
    threads=4,
    parquet_metadata_cache=true,
    httpfs_connection_caching=true,
    enable_http_metadata_cache=true
))]
pub fn duckdb_bootstrap_statements(
    path: &str,
    memory_limit_mb: usize,
    threads: usize,
    parquet_metadata_cache: bool,
    httpfs_connection_caching: bool,
    enable_http_metadata_cache: bool,
) -> Vec<String> {
    let cfg = DuckdbConfig {
        path: path.to_string(),
        memory_limit_mb,
        threads,
        parquet_metadata_cache,
        httpfs_connection_caching,
        enable_http_metadata_cache,
    };
    cfg.bootstrap_statements()
}

/// Register all `vertebra-db` classes and functions on the `_vertebra` Python module.
pub fn register_db_bindings(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<CloudSQLDialer>()?;
    m.add_class::<AlloyDBDialer>()?;
    m.add_class::<AwsRdsDialer>()?;
    m.add_class::<AwsAuroraDsqlDialer>()?;
    m.add_class::<AzureEntraDialer>()?;
    m.add_class::<ArrowCursor>()?;

    m.add_function(wrap_pyfunction!(aws_rds_root_ca_pem, m)?)?;
    m.add_function(wrap_pyfunction!(encode_alloydb_metadata_exchange, m)?)?;
    m.add_function(wrap_pyfunction!(decode_alloydb_metadata_response, m)?)?;
    m.add_function(wrap_pyfunction!(strip_pg_jsonb_header, m)?)?;
    m.add_function(wrap_pyfunction!(encode_pg_jsonb, m)?)?;
    m.add_function(wrap_pyfunction!(encode_pgcopy_rows, m)?)?;
    m.add_function(wrap_pyfunction!(decode_pgcopy_rows, m)?)?;
    m.add_function(wrap_pyfunction!(parse_tnsnames_ora, m)?)?;
    m.add_function(wrap_pyfunction!(resolve_tns_alias, m)?)?;
    m.add_function(wrap_pyfunction!(encode_oracle_vector_f32, m)?)?;
    m.add_function(wrap_pyfunction!(decode_oracle_vector_f32, m)?)?;
    m.add_function(wrap_pyfunction!(encode_oracle_vector_f64, m)?)?;
    m.add_function(wrap_pyfunction!(decode_oracle_vector_f64, m)?)?;
    m.add_function(wrap_pyfunction!(encode_oracle_vector_i8, m)?)?;
    m.add_function(wrap_pyfunction!(decode_oracle_vector_i8, m)?)?;
    m.add_function(wrap_pyfunction!(encode_oracle_vector_binary, m)?)?;
    m.add_function(wrap_pyfunction!(decode_oracle_vector_binary, m)?)?;
    m.add_function(wrap_pyfunction!(encode_pgvector_f32, m)?)?;
    m.add_function(wrap_pyfunction!(decode_pgvector_f32, m)?)?;
    m.add_function(wrap_pyfunction!(encode_pgvector_f16, m)?)?;
    m.add_function(wrap_pyfunction!(decode_pgvector_f16, m)?)?;
    m.add_function(wrap_pyfunction!(encode_pgvector_bit, m)?)?;
    m.add_function(wrap_pyfunction!(decode_pgvector_bit, m)?)?;
    m.add_function(wrap_pyfunction!(encode_oson_payload, m)?)?;
    m.add_function(wrap_pyfunction!(decode_oson_payload, m)?)?;
    m.add_function(wrap_pyfunction!(encode_ttc_session_attrs, m)?)?;
    m.add_function(wrap_pyfunction!(rewrite_mssql_placeholders, m)?)?;
    m.add_function(wrap_pyfunction!(duckdb_bootstrap_statements, m)?)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use pyo3::prelude::*;

    use super::ArrowCursor;

    #[test]
    fn arrow_cursor_from_shm_capsule_exposes_zero_copy_view_and_unlinks_on_release() {
        Python::initialize();
        Python::attach(|py| {
            let payload = b"VTBARROW-RUST-CAPSULE-TEST-PAYLOAD";
            let unslashed = format!("vtb-py-cap-{}", std::process::id());
            let (norm_name, byte_len, capsule) =
                ArrowCursor::_allocate_shm_capsule(py, &unslashed, payload).unwrap();
            assert_eq!(byte_len, payload.len());
            assert_eq!(norm_name, format!("/{unslashed}"));

            assert!(ArrowCursor::new(norm_name.clone(), usize::MAX, 0).is_err());
            let bad_name = Some(unslashed.clone());
            let bad = ArrowCursor::_from_shm_capsule(py, bad_name, usize::MAX, 5, &capsule);
            assert!(bad.is_err());

            let ok = ArrowCursor::_from_shm_capsule(py, Some(unslashed), byte_len, 5, &capsule);
            let cursor = ok.expect("from capsule after rejected overflow");
            assert_eq!(cursor.shm_name, norm_name);
            assert_eq!(cursor.row_count, 5);
            let dup_name = Some(norm_name.clone());
            let dup = ArrowCursor::_from_shm_capsule(py, dup_name, byte_len, 5, &capsule);
            assert!(dup.is_err());

            let py_cursor = Bound::new(py, cursor).expect("bound cursor");
            let mv = ArrowCursor::buffer_view(py_cursor.clone()).expect("buffer view");
            let view_bytes: Vec<u8> = py
                .import("builtins")
                .unwrap()
                .getattr("bytes")
                .unwrap()
                .call1((&mv,))
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(view_bytes, payload);

            assert!(py_cursor.borrow().release(py));
            #[cfg(target_os = "linux")]
            if !norm_name.starts_with("/tmp/") {
                let shm_path = format!("/dev/shm{norm_name}");
                assert!(!std::path::Path::new(&shm_path).exists());
            }

            let still_readable: Vec<u8> = py
                .import("builtins")
                .unwrap()
                .getattr("bytes")
                .unwrap()
                .call1((&mv,))
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(still_readable, payload);
            assert!(!py_cursor.borrow().release(py));
        });
    }

    fn fuzz_ipc_stream() -> Vec<u8> {
        use std::sync::Arc;

        use arrow_array::builder::{
            FixedSizeBinaryBuilder, FixedSizeListBuilder, Int32Builder, LargeListBuilder,
            LargeStringBuilder, ListBuilder, ListViewBuilder, MapBuilder, StringBuilder,
            StringDictionaryBuilder, StringRunBuilder, StringViewBuilder, UnionBuilder,
        };
        use arrow_array::types::{Float64Type, Int16Type, Int32Type};
        use arrow_array::{
            ArrayRef, BooleanArray, Decimal128Array, Int32Array, Int64Array, RecordBatch,
            StringArray, StructArray, TimestampMicrosecondArray,
        };
        use arrow_ipc::writer::StreamWriter;

        let build = |base: i32| -> RecordBatch {
            let mut dense = UnionBuilder::new_dense();
            dense.append::<Int32Type>("i", base).unwrap();
            dense.append::<Float64Type>("f", 1.5).unwrap();
            dense.append_null::<Int32Type>("i").unwrap();
            let mut sparse = UnionBuilder::new_sparse();
            sparse.append::<Int32Type>("i", base).unwrap();
            sparse.append::<Float64Type>("f", 2.5).unwrap();
            sparse.append::<Int32Type>("i", base + 1).unwrap();
            let mut large_label = LargeStringBuilder::new();
            large_label.append_value("large");
            large_label.append_null();
            large_label.append_value("x");
            let mut view = StringViewBuilder::new();
            view.append_value("a string longer than twelve bytes");
            view.append_null();
            view.append_value("short");
            let mut attrs = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
            attrs.keys().append_value("k");
            attrs.values().append_value(base);
            attrs.append(true).unwrap();
            attrs.append(false).unwrap();
            attrs.append(true).unwrap();
            let mut pair = FixedSizeListBuilder::new(Int32Builder::new(), 4);
            pair.values()
                .append_slice(&[base, base + 1, base + 2, base + 3]);
            pair.append(true);
            pair.values().append_slice(&[0, 0, 0, 0]);
            pair.append(false);
            pair.values()
                .append_slice(&[base + 4, base + 5, base + 6, base + 7]);
            pair.append(true);
            let mut runs = StringRunBuilder::<Int16Type>::new();
            runs.append_value("run");
            runs.append_value("run");
            runs.append_null();
            let mut large_tags = LargeListBuilder::new(Int32Builder::new());
            large_tags.append_value([Some(base)]);
            large_tags.append_null();
            large_tags.append_value([Some(base + 1), None]);
            let mut tag_view = ListViewBuilder::new(Int32Builder::new());
            tag_view.append_value([Some(base)]);
            tag_view.append_null();
            tag_view.append_value([Some(base + 1), None]);
            let mut digest = FixedSizeBinaryBuilder::new(4);
            digest.append_value(b"abcd").unwrap();
            digest.append_null();
            digest.append_value(b"wxyz").unwrap();
            let mut tags = ListBuilder::new(Int32Builder::new());
            tags.append_value([Some(base), None]);
            tags.append_null();
            tags.append_value([Some(base + 2)]);
            let mut role = StringDictionaryBuilder::<Int32Type>::new();
            role.append_value("primary");
            role.append_null();
            role.append_value("replica");
            let point = StructArray::from(
                RecordBatch::try_from_iter(vec![(
                    "x",
                    Arc::new(Int32Array::from(vec![Some(base), None, Some(3)])) as ArrayRef,
                )])
                .unwrap(),
            );
            RecordBatch::try_from_iter(vec![
                (
                    "id",
                    Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
                ),
                (
                    "label",
                    Arc::new(StringArray::from(vec![Some("a"), None, Some("ccc")])) as ArrayRef,
                ),
                (
                    "flag",
                    Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])) as ArrayRef,
                ),
                ("tags", Arc::new(tags.finish()) as ArrayRef),
                ("point", Arc::new(point) as ArrayRef),
                ("role", Arc::new(role.finish()) as ArrayRef),
                ("large_label", Arc::new(large_label.finish()) as ArrayRef),
                ("view", Arc::new(view.finish()) as ArrayRef),
                ("attrs", Arc::new(attrs.finish()) as ArrayRef),
                ("pair", Arc::new(pair.finish()) as ArrayRef),
                ("runs", Arc::new(runs.finish()) as ArrayRef),
                ("large_tags", Arc::new(large_tags.finish()) as ArrayRef),
                ("tag_view", Arc::new(tag_view.finish()) as ArrayRef),
                ("digest", Arc::new(digest.finish()) as ArrayRef),
                (
                    "amount",
                    Arc::new(Decimal128Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
                ),
                (
                    "seen_at",
                    Arc::new(TimestampMicrosecondArray::from(vec![
                        Some(1),
                        None,
                        Some(3),
                    ])) as ArrayRef,
                ),
                ("dense", Arc::new(dense.build().unwrap()) as ArrayRef),
                ("sparse", Arc::new(sparse.build().unwrap()) as ArrayRef),
            ])
            .unwrap()
        };
        let first = build(10);
        let second = build(20);
        let mut buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buf, &first.schema()).unwrap();
        writer.write(&first).unwrap();
        writer.write(&second).unwrap();
        writer.finish().unwrap();
        drop(writer);
        buf
    }

    fn drain_ipc_stream(payload: &[u8]) -> Result<usize, String> {
        let reader = super::open_single_pass_ipc_stream(
            arrow_buffer::Buffer::from_slice_ref(payload),
            false,
            None,
        )?;
        let mut total_rows = 0usize;
        for batch in reader {
            let batch = batch.map_err(|e| e.to_string())?;
            total_rows += batch.num_rows();
        }
        Ok(total_rows)
    }

    #[test]
    fn arrow_ipc_range_validation_rejects_every_panicking_single_byte_mutation() {
        use std::collections::HashMap;
        use std::panic::{AssertUnwindSafe, catch_unwind};

        let valid = fuzz_ipc_stream();
        assert_eq!(drain_ipc_stream(&valid).unwrap(), 6);

        const FLIPS: usize = 1;
        let mut state = 0x9E37_79B9_7F4A_7C15_u64;
        let mut panics = Vec::new();
        let mut rejected = 0usize;
        for offset in 0..valid.len() {
            for _ in 0..FLIPS {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let flip = (state % 255) as u8 + 1;
                let mut mutated = valid.clone();
                mutated[offset] ^= flip;
                if super::scan_ipc_frame_bounds(&mutated).is_err() {
                    rejected += 1;
                    continue;
                }
                let Ok((layout, pos)) = super::parse_ipc_schema_frame(&mutated) else {
                    rejected += 1;
                    continue;
                };
                let mut batches = super::SinglePassIpcBatches {
                    buffer: arrow_buffer::Buffer::from_vec(mutated),
                    pos,
                    layout,
                    dictionaries_by_id: HashMap::new(),
                    validated_flag: None,
                    poisoned: false,
                };
                let outcome = catch_unwind(AssertUnwindSafe(|| {
                    loop {
                        match batches.decode_next_batch() {
                            Ok(Some(_)) => {}
                            Ok(None) => break false,
                            Err(_) => break true,
                        }
                    }
                }));
                match outcome {
                    Ok(true) => rejected += 1,
                    Ok(false) => {}
                    Err(payload) => panics.push((
                        offset,
                        flip,
                        super::ipc_panic_message(payload.as_ref()).to_string(),
                    )),
                }
            }
        }
        assert!(rejected > 0);
        assert!(panics.is_empty(), "unguarded decoder panics: {panics:?}");
    }

    #[test]
    fn arrow_ipc_guarded_reader_reports_out_of_range_buffers_as_errors() {
        let valid = fuzz_ipc_stream();
        let mut saw_range_error = false;
        for offset in 0..valid.len() {
            let mut mutated = valid.clone();
            mutated[offset] ^= 0x80;
            match super::open_single_pass_ipc_stream(
                arrow_buffer::Buffer::from_vec(mutated),
                false,
                None,
            ) {
                Ok(reader) => {
                    for batch in reader {
                        if let Err(err) = batch {
                            saw_range_error |=
                                err.to_string().contains("exceeds message body length");
                            break;
                        }
                    }
                }
                Err(message) => {
                    saw_range_error |= message.contains("exceeds message body length");
                }
            }
        }
        assert!(saw_range_error);
    }

    #[test]
    fn arrow_ipc_multi_byte_mutations_never_panic() {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        let valid = fuzz_ipc_stream();
        let ranges = ipc_message_meta_ranges(&valid);
        assert!(ranges.len() >= 2);
        let schema_range = ranges[0];
        let batch_offsets: Vec<usize> = ranges[1..]
            .iter()
            .flat_map(|&(start, end)| start..end)
            .collect();
        let schema_offsets: Vec<usize> = (schema_range.0..schema_range.1).collect();
        assert!(!schema_offsets.is_empty() && !batch_offsets.is_empty());

        let mut state = 0xD1B5_4A32_D192_ED03_u64;
        let mut next_u64 = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };

        let mut panics = Vec::new();
        for &mutation_count in &[2usize, 4usize] {
            for trial in 0..512usize {
                let mut mutated = valid.clone();
                for idx in 0..mutation_count {
                    let pool = if idx % 2 == 0 {
                        &schema_offsets
                    } else {
                        &batch_offsets
                    };
                    let offset = pool[(next_u64() as usize) % pool.len()];
                    let flip = (next_u64() % 255) as u8 + 1;
                    mutated[offset] ^= flip;
                }
                let outcome = catch_unwind(AssertUnwindSafe(|| {
                    if let Ok(reader) = super::open_single_pass_ipc_stream(
                        arrow_buffer::Buffer::from_vec(mutated),
                        false,
                        None,
                    ) {
                        for batch in reader {
                            if batch.is_err() {
                                break;
                            }
                        }
                    }
                }));
                if let Err(payload) = outcome {
                    panics.push((
                        mutation_count,
                        trial,
                        super::ipc_panic_message(payload.as_ref()).to_string(),
                    ));
                }
            }
        }
        assert!(panics.is_empty(), "multi-byte mutation panics: {panics:?}");
    }

    #[test]
    fn arrow_ipc_misaligned_dense_union_decodes_without_panicking() {
        use std::sync::Arc;

        use arrow_array::builder::UnionBuilder;
        use arrow_array::types::{Float64Type, Int32Type};
        use arrow_array::{ArrayRef, RecordBatch};
        use arrow_ipc::writer::StreamWriter;

        let mut dense = UnionBuilder::new_dense();
        dense.append::<Int32Type>("i", 42).unwrap();
        dense.append::<Float64Type>("f", 3.5).unwrap();
        dense.append_null::<Int32Type>("i").unwrap();
        let batch = RecordBatch::try_from_iter(vec![(
            "dense",
            Arc::new(dense.build().unwrap()) as ArrayRef,
        )])
        .unwrap();

        let mut ipc_bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc_bytes, &batch.schema()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        let mut prefixed = Vec::with_capacity(1 + ipc_bytes.len());
        prefixed.push(0xAA);
        prefixed.extend_from_slice(&ipc_bytes);
        let carrier = super::OwnedArrowIpcSlice {
            backing: super::ArrowBacking::Bytes(Arc::<[u8]>::from(prefixed.as_slice())),
            start: 1,
            end: 1 + ipc_bytes.len(),
        };
        assert_eq!((carrier.as_ref().as_ptr() as usize) & 1, 1);

        let reader = super::open_lazy_arrow_ipc_stream(carrier, false, None).unwrap();
        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[test]
    fn arrow_ipc_frame_scan_and_validated_flag_semantics() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        assert!(super::scan_ipc_frame_bounds(&[]).is_err());
        let eos_only = [0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0];
        assert!(super::scan_ipc_frame_bounds(&eos_only).is_err());
        assert!(super::parse_ipc_schema_only(&eos_only).is_err());

        let valid = fuzz_ipc_stream();
        let schema = super::parse_ipc_schema_only(&valid).unwrap();
        assert_eq!(schema.fields().len(), 18);

        let flag = Arc::new(AtomicBool::new(false));
        let mut reader = super::open_single_pass_ipc_stream(
            arrow_buffer::Buffer::from_slice_ref(&valid),
            false,
            Some(Arc::clone(&flag)),
        )
        .unwrap();
        assert!(!flag.load(Ordering::Acquire));
        let mut rows = 0usize;
        for batch in &mut reader {
            rows += batch.unwrap().num_rows();
        }
        assert_eq!(rows, 6);
        assert!(flag.load(Ordering::Acquire));
        assert!(reader.next().is_none());
    }

    #[test]
    fn arrow_cursor_c_stream_exports_empty_and_multi_batch_ipc_and_rejects_released_or_corrupt() {
        use std::sync::Arc;

        use arrow_array::ffi_stream::{FFI_ArrowArrayStream, RecordBatchStreamReader};
        use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchReader, StringArray};
        use arrow_ipc::writer::StreamWriter;
        use pyo3::types::PyBytes;

        let batch1 = RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
            (
                "name",
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            ),
        ])
        .unwrap();
        let batch2 = RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from(vec![3])) as ArrayRef),
            ("name", Arc::new(StringArray::from(vec!["c"])) as ArrayRef),
        ])
        .unwrap();
        let schema = batch1.schema();

        let mut empty_buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut empty_buf, &schema).unwrap();
            writer.finish().unwrap();
        }
        let empty_carrier = super::OwnedArrowIpcSlice::try_new(
            super::ArrowBacking::Bytes(Arc::<[u8]>::from(empty_buf.as_slice())),
            empty_buf.len(),
        )
        .unwrap();
        let empty_reader = super::open_lazy_arrow_ipc_stream(empty_carrier, false, None).unwrap();
        assert_eq!(empty_reader.schema(), schema);
        let empty_ffi = FFI_ArrowArrayStream::new(Box::new(empty_reader));
        let empty_imported = RecordBatchStreamReader::try_new(empty_ffi).unwrap();
        assert_eq!(empty_imported.schema(), schema);
        let empty_batches: Vec<RecordBatch> =
            empty_imported.collect::<Result<Vec<_>, _>>().unwrap();
        assert!(empty_batches.is_empty());

        let mut multi_buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut multi_buf, &schema).unwrap();
            writer.write(&batch1).unwrap();
            writer.write(&batch2).unwrap();
            writer.finish().unwrap();
        }
        let multi_carrier = super::OwnedArrowIpcSlice::try_new(
            super::ArrowBacking::Bytes(Arc::<[u8]>::from(multi_buf.as_slice())),
            multi_buf.len(),
        )
        .unwrap();
        let multi_reader = super::open_lazy_arrow_ipc_stream(multi_carrier, false, None).unwrap();
        let multi_ffi = FFI_ArrowArrayStream::new(Box::new(multi_reader));
        let multi_imported = RecordBatchStreamReader::try_new(multi_ffi).unwrap();
        assert_eq!(multi_imported.schema(), schema);
        let multi_batches: Vec<RecordBatch> =
            multi_imported.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(multi_batches.len(), 2);
        assert_eq!(multi_batches[0].num_rows(), 2);
        assert_eq!(multi_batches[1].num_rows(), 1);

        let empty_slice = super::OwnedArrowIpcSlice::try_new(
            super::ArrowBacking::Bytes(Arc::<[u8]>::from(&b""[..])),
            0,
        )
        .unwrap();
        assert!(super::open_lazy_arrow_ipc_stream(empty_slice, false, None).is_err());
        let truncated = &multi_buf[..multi_buf.len() / 2];
        let trunc_slice = super::OwnedArrowIpcSlice::try_new(
            super::ArrowBacking::Bytes(Arc::<[u8]>::from(truncated)),
            truncated.len(),
        )
        .unwrap();
        assert!(super::open_lazy_arrow_ipc_stream(trunc_slice, false, None).is_err());

        Python::initialize();
        Python::attach(|py| {
            let shm_name = format!("vtb-py-cstream-{}", std::process::id());
            let cursor = ArrowCursor::write_ipc_to_shm(py, &shm_name, &multi_buf, 3).unwrap();
            assert_eq!(cursor.columns(py).unwrap(), vec!["id", "name"]);
            let schema_cap = cursor.__arrow_c_schema__(py).unwrap();
            assert!(
                schema_cap
                    .pointer_checked(Some(super::ARROW_SCHEMA_CAPSULE_NAME))
                    .is_ok()
            );
            drop(schema_cap);
            let capsule = cursor.__arrow_c_stream__(py, None).unwrap();
            assert!(
                capsule
                    .pointer_checked(Some(super::ARROW_ARRAY_STREAM_CAPSULE_NAME))
                    .is_ok()
            );
            drop(capsule);

            let py_bytes = PyBytes::new(py, &multi_buf);
            let bytes_cursor = ArrowCursor::from_ipc_bytes(py, py_bytes.as_any()).unwrap();
            assert_eq!(bytes_cursor.row_count, 3);
            assert_eq!(bytes_cursor.byte_len, multi_buf.len());
            assert_eq!(bytes_cursor.columns(py).unwrap(), vec!["id", "name"]);
            assert_eq!(bytes_cursor.read_bytes(py).unwrap().as_bytes(), multi_buf);
            let rows = bytes_cursor.to_pylist(py).unwrap();
            assert_eq!(rows.len(), 3);
            let bound_bytes_cursor = Bound::new(py, bytes_cursor).unwrap();
            let mv_before = ArrowCursor::buffer_view(bound_bytes_cursor.clone()).unwrap();
            drop(mv_before);
            assert!(bound_bytes_cursor.borrow().release(py));
            assert!(!bound_bytes_cursor.borrow().release(py));
            assert!(bound_bytes_cursor.borrow().read_bytes(py).is_err());
            assert!(ArrowCursor::buffer_view(bound_bytes_cursor).is_err());

            assert!(cursor.release(py));
            assert!(cursor.__arrow_c_schema__(py).is_err());
            assert!(cursor.__arrow_c_stream__(py, None).is_err());
            assert!(cursor.read_bytes(py).is_err());
        });
    }

    fn fb_u32(buf: &[u8], pos: usize) -> usize {
        u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize
    }

    fn fb_table_field_pos(buf: &[u8], table_pos: usize, vtable_offset: usize) -> usize {
        let vtable_rel = i32::from_le_bytes(buf[table_pos..table_pos + 4].try_into().unwrap());
        let vtable_pos = table_pos.checked_sub_signed(vtable_rel as isize).unwrap();
        let vtable_len =
            u16::from_le_bytes(buf[vtable_pos..vtable_pos + 2].try_into().unwrap()) as usize;
        assert!(vtable_offset < vtable_len);
        let field_rel = u16::from_le_bytes(
            buf[vtable_pos + vtable_offset..vtable_pos + vtable_offset + 2]
                .try_into()
                .unwrap(),
        ) as usize;
        assert!(field_rel != 0);
        table_pos + field_rel
    }

    fn fb_follow_table(buf: &[u8], field_pos: usize) -> usize {
        field_pos + fb_u32(buf, field_pos)
    }

    fn ipc_message_meta_ranges(payload: &[u8]) -> Vec<(usize, usize)> {
        let mut ranges = Vec::new();
        let mut pos = 0usize;
        while pos < payload.len() {
            let mut word = [0u8; 4];
            word.copy_from_slice(&payload[pos..pos + 4]);
            pos += 4;
            if word == super::IPC_CONTINUATION_MARKER {
                word.copy_from_slice(&payload[pos..pos + 4]);
                pos += 4;
            }
            let meta_len = i32::from_le_bytes(word);
            if meta_len == 0 {
                break;
            }
            let meta_len = usize::try_from(meta_len).unwrap();
            let meta_start = pos;
            let meta_end = pos + meta_len;
            let message = arrow_ipc::root_as_message(&payload[meta_start..meta_end]).unwrap();
            let body_len = usize::try_from(message.bodyLength()).unwrap();
            ranges.push((meta_start, meta_end));
            pos = meta_end + body_len;
        }
        ranges
    }

    fn patch_first_batch_length_and_node0(
        payload: &mut [u8],
        header_type: arrow_ipc::MessageHeader,
        new_length: i64,
    ) {
        let ranges = ipc_message_meta_ranges(payload);
        let (meta_start, meta_end) = ranges
            .into_iter()
            .find(|&(start, end)| {
                arrow_ipc::root_as_message(&payload[start..end])
                    .unwrap()
                    .header_type()
                    == header_type
            })
            .expect("matching IPC message not found");
        let meta = &mut payload[meta_start..meta_end];
        let msg_table = fb_u32(meta, 0);
        let header_field =
            fb_table_field_pos(meta, msg_table, arrow_ipc::Message::VT_HEADER as usize);
        let header_table = fb_follow_table(meta, header_field);
        let batch_table = if header_type == arrow_ipc::MessageHeader::DictionaryBatch {
            let data_field = fb_table_field_pos(
                meta,
                header_table,
                arrow_ipc::DictionaryBatch::VT_DATA as usize,
            );
            fb_follow_table(meta, data_field)
        } else {
            header_table
        };
        let len_pos = fb_table_field_pos(
            meta,
            batch_table,
            arrow_ipc::RecordBatch::VT_LENGTH as usize,
        );
        meta[len_pos..len_pos + 8].copy_from_slice(&new_length.to_le_bytes());
        let nodes_field =
            fb_table_field_pos(meta, batch_table, arrow_ipc::RecordBatch::VT_NODES as usize);
        let nodes_vec = fb_follow_table(meta, nodes_field);
        let node0_len_pos = nodes_vec + 4;
        meta[node0_len_pos..node0_len_pos + 8].copy_from_slice(&new_length.to_le_bytes());
    }

    #[test]
    fn arrow_ipc_fixed_size_overflow() {
        use std::sync::Arc;

        use arrow_array::builder::{FixedSizeListBuilder, Int32Builder};
        use arrow_array::{ArrayRef, RecordBatch};
        use arrow_ipc::writer::StreamWriter;

        let mut list = FixedSizeListBuilder::new(Int32Builder::new(), 4);
        list.values().append_slice(&[1, 2, 3, 4]);
        list.append(true);
        let batch =
            RecordBatch::try_from_iter(vec![("fsl", Arc::new(list.finish()) as ArrayRef)]).unwrap();
        let mut buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);

        patch_first_batch_length_and_node0(
            &mut buf,
            arrow_ipc::MessageHeader::RecordBatch,
            0x4000_0000_0000_0000_i64,
        );
        let err = drain_ipc_stream(&buf)
            .expect_err("FixedSizeList multiplication overflow must be rejected");
        assert!(err.contains("fixed-size list"), "unexpected error: {err}");
        assert!(err.contains("overflow"), "unexpected error: {err}");
    }

    fn patch_schema_first_field_type_i32(payload: &mut [u8], vtable_offset: usize, new_val: i32) {
        let ranges = ipc_message_meta_ranges(payload);
        let (meta_start, meta_end) = ranges
            .into_iter()
            .find(|&(start, end)| {
                arrow_ipc::root_as_message(&payload[start..end])
                    .unwrap()
                    .header_type()
                    == arrow_ipc::MessageHeader::Schema
            })
            .expect("Schema IPC message not found");
        let meta = &mut payload[meta_start..meta_end];
        let msg_table = fb_u32(meta, 0);
        let header_field =
            fb_table_field_pos(meta, msg_table, arrow_ipc::Message::VT_HEADER as usize);
        let schema_table = fb_follow_table(meta, header_field);
        let fields_field =
            fb_table_field_pos(meta, schema_table, arrow_ipc::Schema::VT_FIELDS as usize);
        let fields_vec = fb_follow_table(meta, fields_field);
        let field0_table = fb_follow_table(meta, fields_vec + 4);
        let type_field =
            fb_table_field_pos(meta, field0_table, arrow_ipc::Field::VT_TYPE_ as usize);
        let type_table = fb_follow_table(meta, type_field);
        let val_pos = fb_table_field_pos(meta, type_table, vtable_offset);
        meta[val_pos..val_pos + 4].copy_from_slice(&new_val.to_le_bytes());
    }

    #[test]
    fn arrow_ipc_fixed_size_binary_overflow_and_zero_width() {
        use std::sync::Arc;

        use arrow_array::builder::{FixedSizeBinaryBuilder, FixedSizeListBuilder, Int32Builder};
        use arrow_array::{ArrayRef, RecordBatch};
        use arrow_ipc::writer::StreamWriter;

        let mut bin = FixedSizeBinaryBuilder::new(16);
        bin.append_value([0u8; 16]).unwrap();
        let batch =
            RecordBatch::try_from_iter(vec![("fsb", Arc::new(bin.finish()) as ArrayRef)]).unwrap();
        let mut buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);

        patch_first_batch_length_and_node0(
            &mut buf,
            arrow_ipc::MessageHeader::RecordBatch,
            0x1000_0000_0000_0000_i64,
        );
        let err = drain_ipc_stream(&buf)
            .expect_err("FixedSizeBinary multiplication overflow must be rejected");
        assert!(err.contains("fixed-size binary"), "unexpected error: {err}");
        assert!(err.contains("overflow"), "unexpected error: {err}");

        let mut empty_list = FixedSizeListBuilder::new(Int32Builder::new(), 0);
        let mut empty_bin = FixedSizeBinaryBuilder::new(0);
        let zero_batch = RecordBatch::try_from_iter(vec![
            ("zero_fsl", Arc::new(empty_list.finish()) as ArrayRef),
            ("zero_fsb", Arc::new(empty_bin.finish()) as ArrayRef),
        ])
        .unwrap();
        let mut zero_buf = Vec::new();
        let mut zero_writer = StreamWriter::try_new(&mut zero_buf, &zero_batch.schema()).unwrap();
        zero_writer.write(&zero_batch).unwrap();
        zero_writer.finish().unwrap();
        drop(zero_writer);
        drain_ipc_stream(&zero_buf).unwrap();
    }

    #[test]
    fn arrow_ipc_negative_fixed_size_rejected() {
        use std::sync::Arc;

        use arrow_array::builder::{FixedSizeBinaryBuilder, FixedSizeListBuilder, Int32Builder};
        use arrow_array::{ArrayRef, RecordBatch};
        use arrow_ipc::writer::StreamWriter;

        let mut list = FixedSizeListBuilder::new(Int32Builder::new(), 4);
        list.values().append_slice(&[1, 2, 3, 4]);
        list.append(true);
        let list_batch =
            RecordBatch::try_from_iter(vec![("fsl", Arc::new(list.finish()) as ArrayRef)]).unwrap();
        let mut list_buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut list_buf, &list_batch.schema()).unwrap();
        writer.write(&list_batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        patch_schema_first_field_type_i32(
            &mut list_buf,
            arrow_ipc::FixedSizeList::VT_LISTSIZE as usize,
            -4,
        );
        let err = drain_ipc_stream(&list_buf).expect_err("negative listSize must be rejected");
        assert!(
            err.contains("negative fixed size"),
            "unexpected error: {err}"
        );

        let mut bin = FixedSizeBinaryBuilder::new(16);
        bin.append_value([0u8; 16]).unwrap();
        let bin_batch =
            RecordBatch::try_from_iter(vec![("fsb", Arc::new(bin.finish()) as ArrayRef)]).unwrap();
        let mut bin_buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut bin_buf, &bin_batch.schema()).unwrap();
        writer.write(&bin_batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        patch_schema_first_field_type_i32(
            &mut bin_buf,
            arrow_ipc::FixedSizeBinary::VT_BYTEWIDTH as usize,
            -4,
        );
        let err = drain_ipc_stream(&bin_buf).expect_err("negative byteWidth must be rejected");
        assert!(
            err.contains("negative fixed size"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn arrow_ipc_dictionary_fixed_size_list_overflow_rejected() {
        use std::sync::Arc;

        use arrow_array::builder::{FixedSizeListBuilder, Int32Builder};
        use arrow_array::types::Int32Type;
        use arrow_array::{ArrayRef, DictionaryArray, Int32Array, RecordBatch};
        use arrow_ipc::writer::StreamWriter;

        let mut values = FixedSizeListBuilder::new(Int32Builder::new(), 4);
        values.values().append_slice(&[1, 2, 3, 4]);
        values.append(true);
        let dict = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0]),
            Arc::new(values.finish()) as ArrayRef,
        )
        .unwrap();
        let batch =
            RecordBatch::try_from_iter(vec![("dict_fsl", Arc::new(dict) as ArrayRef)]).unwrap();
        let mut buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);

        patch_first_batch_length_and_node0(
            &mut buf,
            arrow_ipc::MessageHeader::DictionaryBatch,
            0x4000_0000_0000_0000_i64,
        );
        let err =
            drain_ipc_stream(&buf).expect_err("dictionary FixedSizeList overflow must be rejected");
        assert!(err.contains("fixed-size list"), "unexpected error: {err}");
        assert!(err.contains("overflow"), "unexpected error: {err}");
    }

    #[test]
    fn arrow_ipc_release_catch_unwind_catches_panic() {
        let outcome = std::panic::catch_unwind(|| {
            panic!("release-unwind-check");
        });
        let payload = outcome.expect_err("catch_unwind must return Err without aborting");
        assert_eq!(
            super::ipc_panic_message(payload.as_ref()),
            "release-unwind-check"
        );
    }

    #[test]
    fn arrow_cursor_validated_cache_transitions() {
        use std::sync::Arc;
        use std::sync::atomic::Ordering;

        use arrow_array::{ArrayRef, Int64Array, RecordBatch};
        use arrow_ipc::writer::StreamWriter;
        use pyo3::types::PyBytes;

        let batch = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
        )])
        .unwrap();
        let mut valid_buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut valid_buf, &batch.schema()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        let mut corrupt_buf = valid_buf.clone();
        patch_first_batch_length_and_node0(
            &mut corrupt_buf,
            arrow_ipc::MessageHeader::RecordBatch,
            0x1000_0000_0000_0000_i64,
        );

        Python::initialize();
        Python::attach(|py| {
            let py_bytes = PyBytes::new(py, &valid_buf);
            let bytes_cursor = ArrowCursor::from_ipc_bytes(py, py_bytes.as_any()).unwrap();
            assert!(bytes_cursor.validated.load(Ordering::Acquire));
            assert_eq!(bytes_cursor.to_pylist(py).unwrap().len(), 2);
            assert!(bytes_cursor.release(py));

            let valid_shm = format!("vtb-py-val-ok-{}", std::process::id());
            let shm_cursor = ArrowCursor::write_ipc_to_shm(py, &valid_shm, &valid_buf, 2).unwrap();
            assert!(!shm_cursor.validated.load(Ordering::Acquire));
            assert_eq!(shm_cursor.to_pylist(py).unwrap().len(), 2);
            assert!(shm_cursor.validated.load(Ordering::Acquire));
            assert_eq!(shm_cursor.to_pylist(py).unwrap().len(), 2);
            assert!(shm_cursor.release(py));

            let corrupt_shm = format!("vtb-py-val-bad-{}", std::process::id());
            let bad_cursor =
                ArrowCursor::write_ipc_to_shm(py, &corrupt_shm, &corrupt_buf, 0).unwrap();
            assert!(!bad_cursor.validated.load(Ordering::Acquire));
            assert!(bad_cursor.to_pylist(py).is_err());
            assert!(!bad_cursor.validated.load(Ordering::Acquire));
            assert!(bad_cursor.to_pylist(py).is_err());
            assert!(!bad_cursor.validated.load(Ordering::Acquire));
            assert!(bad_cursor.release(py));
        });
    }

    #[test]
    fn arrow_cursor_to_pylist_rejects_inflated_and_cumulative_row_counts() {
        use std::sync::Arc;

        use arrow_array::{ArrayRef, NullArray, RecordBatch, RecordBatchOptions};
        use arrow_ipc::writer::StreamWriter;
        use arrow_schema::Schema;
        use pyo3::types::PyBytes;

        let null_batch =
            RecordBatch::try_from_iter(vec![("n", Arc::new(NullArray::new(1)) as ArrayRef)])
                .unwrap();
        let mut null_buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut null_buf, &null_batch.schema()).unwrap();
            writer.write(&null_batch).unwrap();
            writer.finish().unwrap();
        }
        let inflated = 0x4000_0000_0000_0000_i64;
        patch_first_batch_length_and_node0(
            &mut null_buf,
            arrow_ipc::MessageHeader::RecordBatch,
            inflated,
        );
        let null_ranges = ipc_message_meta_ranges(&null_buf);
        let (meta_start, meta_end) = null_ranges
            .into_iter()
            .find(|&(start, end)| {
                arrow_ipc::root_as_message(&null_buf[start..end])
                    .unwrap()
                    .header_type()
                    == arrow_ipc::MessageHeader::RecordBatch
            })
            .unwrap();
        {
            let meta = &mut null_buf[meta_start..meta_end];
            let msg_table = fb_u32(meta, 0);
            let header_field =
                fb_table_field_pos(meta, msg_table, arrow_ipc::Message::VT_HEADER as usize);
            let batch_table = fb_follow_table(meta, header_field);
            let nodes_field =
                fb_table_field_pos(meta, batch_table, arrow_ipc::RecordBatch::VT_NODES as usize);
            let nodes_vec = fb_follow_table(meta, nodes_field);
            let node0_null_pos = nodes_vec + 4 + 8;
            meta[node0_null_pos..node0_null_pos + 8].copy_from_slice(&inflated.to_le_bytes());
        }

        let empty_schema = Arc::new(Schema::empty());
        let zero_col_batch = RecordBatch::try_new_with_options(
            Arc::clone(&empty_schema),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .unwrap();
        let mut zero_col_buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut zero_col_buf, &empty_schema).unwrap();
            writer.write(&zero_col_batch).unwrap();
            writer.finish().unwrap();
        }
        let zc_ranges = ipc_message_meta_ranges(&zero_col_buf);
        let (zc_start, zc_end) = zc_ranges
            .into_iter()
            .find(|&(start, end)| {
                arrow_ipc::root_as_message(&zero_col_buf[start..end])
                    .unwrap()
                    .header_type()
                    == arrow_ipc::MessageHeader::RecordBatch
            })
            .unwrap();
        {
            let meta = &mut zero_col_buf[zc_start..zc_end];
            let msg_table = fb_u32(meta, 0);
            let header_field =
                fb_table_field_pos(meta, msg_table, arrow_ipc::Message::VT_HEADER as usize);
            let batch_table = fb_follow_table(meta, header_field);
            let len_pos = fb_table_field_pos(
                meta,
                batch_table,
                arrow_ipc::RecordBatch::VT_LENGTH as usize,
            );
            meta[len_pos..len_pos + 8].copy_from_slice(&inflated.to_le_bytes());
        }

        let mut cumulative_buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut cumulative_buf, &empty_schema).unwrap();
            writer.write(&zero_col_batch).unwrap();
            writer.write(&zero_col_batch).unwrap();
            writer.finish().unwrap();
        }
        let cum_batches: Vec<(usize, usize)> = ipc_message_meta_ranges(&cumulative_buf)
            .into_iter()
            .filter(|&(start, end)| {
                arrow_ipc::root_as_message(&cumulative_buf[start..end])
                    .unwrap()
                    .header_type()
                    == arrow_ipc::MessageHeader::RecordBatch
            })
            .collect();
        assert_eq!(cum_batches.len(), 2);
        let (b2_start, b2_end) = cum_batches[1];
        {
            let meta = &mut cumulative_buf[b2_start..b2_end];
            let msg_table = fb_u32(meta, 0);
            let header_field =
                fb_table_field_pos(meta, msg_table, arrow_ipc::Message::VT_HEADER as usize);
            let batch_table = fb_follow_table(meta, header_field);
            let len_pos = fb_table_field_pos(
                meta,
                batch_table,
                arrow_ipc::RecordBatch::VT_LENGTH as usize,
            );
            let max_rows = super::MAX_PYLIST_ROWS as i64;
            meta[len_pos..len_pos + 8].copy_from_slice(&max_rows.to_le_bytes());
        }

        Python::initialize();
        Python::attach(|py| {
            for payload in [&null_buf, &zero_col_buf, &cumulative_buf] {
                let py_bytes = PyBytes::new(py, payload);
                let cursor = ArrowCursor::from_ipc_bytes(py, py_bytes.as_any()).unwrap();
                let err = cursor
                    .to_pylist(py)
                    .expect_err("inflated or cumulative overflow must fail");
                assert!(
                    err.to_string().contains("maximum Python list size"),
                    "unexpected error: {err}"
                );
                assert!(cursor.release(py));
            }
        });
    }
}
