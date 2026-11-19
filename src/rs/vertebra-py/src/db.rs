//! PyO3 bindings for `vertebra-db` cloud dialers, wire codecs, and shared-memory Arrow IPC cursors.

use std::collections::HashMap;
use std::ffi::{CStr, c_int, c_void};
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
    LargeListArray, LargeStringArray, ListArray, MapArray, NullArray, RecordBatch, StringArray,
    StringViewArray, StructArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_ipc::reader::StreamReader;
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

fn validate_arrow_ipc_stream_framing(payload: &[u8]) -> Result<(), String> {
    if payload.is_empty() {
        return Err("invalid Arrow IPC stream: empty stream".to_string());
    }
    let mut pos = 0usize;
    let mut saw_schema = false;
    while pos < payload.len() {
        if payload.len() - pos < 4 {
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
            break;
        }
        let meta_len = usize::try_from(raw_meta_len).map_err(|_| {
            format!("invalid Arrow IPC stream: negative metadata length {raw_meta_len}")
        })?;
        if payload.len() - pos < meta_len {
            return Err("corrupt Arrow IPC stream: truncated message metadata".to_string());
        }
        let message = arrow_ipc::root_as_message(&payload[pos..pos + meta_len])
            .map_err(|e| format!("invalid Arrow IPC stream: {e:?}"))?;
        pos += meta_len;
        if !saw_schema {
            if message.header_type() != arrow_ipc::MessageHeader::Schema {
                return Err(format!(
                    "invalid Arrow IPC stream: expected Schema header first, got {:?}",
                    message.header_type()
                ));
            }
            saw_schema = true;
        }
        let body_len = usize::try_from(message.bodyLength()).map_err(|_| {
            format!(
                "corrupt Arrow IPC stream: negative message body length {}",
                message.bodyLength()
            )
        })?;
        if payload.len() - pos < body_len {
            return Err("corrupt Arrow IPC stream: truncated message body".to_string());
        }
        pos += body_len;
    }
    if !saw_schema {
        return Err("invalid Arrow IPC stream: missing schema message".to_string());
    }
    Ok(())
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
) -> Result<StreamReader<std::io::Cursor<OwnedArrowIpcSlice>>, String> {
    validate_arrow_ipc_stream_framing(carrier.as_ref())?;
    StreamReader::try_new(std::io::Cursor::new(carrier), None)
        .map_err(|e| format!("invalid Arrow IPC stream: {e}"))
}

fn open_arrow_ipc_schema_reader(
    carrier: OwnedArrowIpcSlice,
) -> Result<StreamReader<std::io::Cursor<OwnedArrowIpcSlice>>, String> {
    StreamReader::try_new(std::io::Cursor::new(carrier), None)
        .map_err(|e| format!("invalid Arrow IPC stream: {e}"))
}

fn validate_and_summarize_ipc_bytes(payload: &[u8]) -> Result<(usize, Arc<[String]>), String> {
    validate_arrow_ipc_stream_framing(payload)?;
    let reader = StreamReader::try_new(std::io::Cursor::new(payload), None)
        .map_err(|e| format!("invalid Arrow IPC stream: {e}"))?;
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
    if let Some(a) = arr.as_any().downcast_ref::<Int8Array>() {
        return Ok(a.value(row).into_pyobject(py)?.into_any());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int16Array>() {
        return Ok(a.value(row).into_pyobject(py)?.into_any());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        return Ok(a.value(row).into_pyobject(py)?.into_any());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return Ok(a.value(row).into_pyobject(py)?.into_any());
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt8Array>() {
        return Ok(a.value(row).into_pyobject(py)?.into_any());
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt16Array>() {
        return Ok(a.value(row).into_pyobject(py)?.into_any());
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt32Array>() {
        return Ok(a.value(row).into_pyobject(py)?.into_any());
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
        return Ok(a.value(row).into_pyobject(py)?.into_any());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float16Array>() {
        return Ok(f32::from(a.value(row)).into_pyobject(py)?.into_any());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float32Array>() {
        return Ok(a.value(row).into_pyobject(py)?.into_any());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return Ok(a.value(row).into_pyobject(py)?.into_any());
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return Ok(PyString::new(py, a.value(row)).into_any());
    }
    if let Some(a) = arr.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(PyString::new(py, a.value(row)).into_any());
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringViewArray>() {
        return Ok(PyString::new(py, a.value(row)).into_any());
    }
    if let Some(a) = arr.as_any().downcast_ref::<BinaryArray>() {
        return Ok(PyBytes::new(py, a.value(row)).into_any());
    }
    if let Some(a) = arr.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(PyBytes::new(py, a.value(row)).into_any());
    }
    if let Some(a) = arr.as_any().downcast_ref::<BinaryViewArray>() {
        return Ok(PyBytes::new(py, a.value(row)).into_any());
    }
    if let Some(a) = arr.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        return Ok(PyBytes::new(py, a.value(row)).into_any());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Decimal128Array>() {
        let text = a.value_as_string(row);
        return helpers.decimal_cls(py)?.call1((text,));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Decimal256Array>() {
        let text = a.value_as_string(row);
        return helpers.decimal_cls(py)?.call1((text,));
    }
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
    if let Some(a) = arr.as_any().downcast_ref::<TimestampSecondArray>() {
        let v = a.value(row);
        return helpers.timestamp_from_days_us(
            py,
            v.div_euclid(86_400),
            v.rem_euclid(86_400) * 1_000_000,
            a.timezone(),
        );
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMillisecondArray>() {
        let v = a.value(row);
        return helpers.timestamp_from_days_us(
            py,
            v.div_euclid(86_400_000),
            v.rem_euclid(86_400_000) * 1_000,
            a.timezone(),
        );
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        let v = a.value(row);
        return helpers.timestamp_from_days_us(
            py,
            v.div_euclid(86_400_000_000),
            v.rem_euclid(86_400_000_000),
            a.timezone(),
        );
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampNanosecondArray>() {
        let us = a.value(row).div_euclid(1_000);
        return helpers.timestamp_from_days_us(
            py,
            us.div_euclid(86_400_000_000),
            us.rem_euclid(86_400_000_000),
            a.timezone(),
        );
    }
    if let Some(a) = arr.as_any().downcast_ref::<DurationSecondArray>() {
        let v = a.value(row);
        return helpers.timedelta_from_days_us(
            py,
            v.div_euclid(86_400),
            v.rem_euclid(86_400) * 1_000_000,
        );
    }
    if let Some(a) = arr.as_any().downcast_ref::<DurationMillisecondArray>() {
        let v = a.value(row);
        return helpers.timedelta_from_days_us(
            py,
            v.div_euclid(86_400_000),
            v.rem_euclid(86_400_000) * 1_000,
        );
    }
    if let Some(a) = arr.as_any().downcast_ref::<DurationMicrosecondArray>() {
        let v = a.value(row);
        return helpers.timedelta_from_days_us(
            py,
            v.div_euclid(86_400_000_000),
            v.rem_euclid(86_400_000_000),
        );
    }
    if let Some(a) = arr.as_any().downcast_ref::<DurationNanosecondArray>() {
        let us = a.value(row).div_euclid(1_000);
        return helpers.timedelta_from_days_us(
            py,
            us.div_euclid(86_400_000_000),
            us.rem_euclid(86_400_000_000),
        );
    }
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
    if let Some(a) = arr.as_any().downcast_ref::<ListArray>() {
        let child = a.value(row);
        let out = PyList::empty(py);
        for i in 0..child.len() {
            out.append(arrow_array_value_to_py(py, child.as_ref(), i, helpers)?)?;
        }
        return Ok(out.into_any());
    }
    if let Some(a) = arr.as_any().downcast_ref::<LargeListArray>() {
        let child = a.value(row);
        let out = PyList::empty(py);
        for i in 0..child.len() {
            out.append(arrow_array_value_to_py(py, child.as_ref(), i, helpers)?)?;
        }
        return Ok(out.into_any());
    }
    if let Some(a) = arr.as_any().downcast_ref::<FixedSizeListArray>() {
        let child = a.value(row);
        let out = PyList::empty(py);
        for i in 0..child.len() {
            out.append(arrow_array_value_to_py(py, child.as_ref(), i, helpers)?)?;
        }
        return Ok(out.into_any());
    }
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
    if let Some(a) = arr.as_any().downcast_ref::<DictionaryArray<Int8Type>>() {
        let idx = usize::try_from(a.keys().value(row))
            .map_err(|_| PyValueError::new_err("negative Arrow dictionary key"))?;
        return arrow_array_value_to_py(py, a.values().as_ref(), idx, helpers);
    }
    if let Some(a) = arr.as_any().downcast_ref::<DictionaryArray<Int16Type>>() {
        let idx = usize::try_from(a.keys().value(row))
            .map_err(|_| PyValueError::new_err("negative Arrow dictionary key"))?;
        return arrow_array_value_to_py(py, a.values().as_ref(), idx, helpers);
    }
    if let Some(a) = arr.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
        let idx = usize::try_from(a.keys().value(row))
            .map_err(|_| PyValueError::new_err("negative Arrow dictionary key"))?;
        return arrow_array_value_to_py(py, a.values().as_ref(), idx, helpers);
    }
    if let Some(a) = arr.as_any().downcast_ref::<DictionaryArray<Int64Type>>() {
        let idx = usize::try_from(a.keys().value(row))
            .map_err(|_| PyValueError::new_err("negative Arrow dictionary key"))?;
        return arrow_array_value_to_py(py, a.values().as_ref(), idx, helpers);
    }
    if let Some(a) = arr.as_any().downcast_ref::<DictionaryArray<UInt8Type>>() {
        let idx = usize::from(a.keys().value(row));
        return arrow_array_value_to_py(py, a.values().as_ref(), idx, helpers);
    }
    if let Some(a) = arr.as_any().downcast_ref::<DictionaryArray<UInt16Type>>() {
        let idx = usize::from(a.keys().value(row));
        return arrow_array_value_to_py(py, a.values().as_ref(), idx, helpers);
    }
    if let Some(a) = arr.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() {
        let idx = a.keys().value(row) as usize;
        return arrow_array_value_to_py(py, a.values().as_ref(), idx, helpers);
    }
    if let Some(a) = arr.as_any().downcast_ref::<DictionaryArray<UInt64Type>>() {
        let idx = usize::try_from(a.keys().value(row))
            .map_err(|_| PyValueError::new_err("Arrow dictionary key overflow"))?;
        return arrow_array_value_to_py(py, a.values().as_ref(), idx, helpers);
    }
    Err(PyNotImplementedError::new_err(format!(
        "ArrowCursor.to_pylist does not support Arrow data type {:?}",
        arr.data_type()
    )))
}

fn append_record_batch_to_pylist<'py>(
    py: Python<'py>,
    batch: &RecordBatch,
    col_keys: &[Bound<'py, PyString>],
    helpers: &mut PyArrowDecodeHelpers<'py>,
    out: &Bound<'py, PyList>,
) -> PyResult<()> {
    let cols = batch.columns();
    for row in 0..batch.num_rows() {
        let dict = PyDict::new(py);
        for (key, col) in col_keys.iter().zip(cols.iter()) {
            let val = arrow_array_value_to_py(py, col.as_ref(), row, helpers)?;
            dict.set_item(key, val)?;
        }
        out.append(dict)?;
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
            .detach(move || validate_and_summarize_ipc_bytes(&bytes_for_scan))
            .map_err(PyValueError::new_err)?;
        Ok(Self {
            shm_name: String::new(),
            byte_len,
            row_count,
            columns_cache: Some(columns),
            backing: Some(ArrowBacking::Bytes(bytes)),
            released: Arc::new(AtomicBool::new(false)),
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
            let reader = open_arrow_ipc_schema_reader(carrier)?;
            Ok(reader
                .schema()
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
                let reader = open_arrow_ipc_schema_reader(carrier)?;
                FFI_ArrowSchema::try_from(reader.schema().as_ref())
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
            Some(ffi_schema)
        } else {
            None
        };

        let carrier = self.resolve_arrow_backing()?;
        let batch_reader = py
            .detach(move || open_lazy_arrow_ipc_stream(carrier))
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
        let pa = py.import(pyo3::intern!(py, "pyarrow"))?;
        let this = slf.get();
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
        let mut reader = py
            .detach(move || open_lazy_arrow_ipc_stream(carrier))
            .map_err(PyValueError::new_err)?;
        let col_keys: Vec<Bound<'py, PyString>> = reader
            .schema()
            .fields()
            .iter()
            .map(|f| PyString::new(py, f.name()))
            .collect();
        let out = PyList::empty(py);
        let mut helpers = PyArrowDecodeHelpers::new();
        loop {
            let next_res = py.detach(|| reader.next());
            let Some(batch_res) = next_res else {
                break;
            };
            let batch = batch_res
                .map_err(|e| PyValueError::new_err(format!("corrupt Arrow IPC batch: {e}")))?;
            append_record_batch_to_pylist(py, &batch, &col_keys, &mut helpers, &out)?;
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

fn with_f32_slice_or_vec<R, F>(obj: &Bound<'_, PyAny>, f: F) -> PyResult<R>
where
    F: FnOnce(&[f32]) -> Result<R, String>,
{
    if let Ok(view_ref) = obj.extract::<PyRef<'_, VectorView>>() {
        let slice = view_ref.row_f32()?;
        return f(slice).map_err(PyValueError::new_err);
    }
    if let Ok(buf) = PyBuffer::<f32>::get(obj)
        && buf.is_c_contiguous()
    {
        let len = buf.item_count();
        let ptr = buf.buf_ptr() as *const f32;
        let slice = if len == 0 || ptr.is_null() {
            &[]
        } else {
            // SAFETY: `buf` pins a contiguous `f32` PEP 3118 buffer while the GIL is held in `f`.
            unsafe { std::slice::from_raw_parts(ptr, len) }
        };
        return f(slice).map_err(PyValueError::new_err);
    }
    let vec: Vec<f32> = obj.extract()?;
    f(&vec).map_err(PyValueError::new_err)
}

fn with_f64_slice_or_vec<R, F>(obj: &Bound<'_, PyAny>, f: F) -> PyResult<R>
where
    F: FnOnce(&[f64]) -> Result<R, String>,
{
    if let Ok(buf) = PyBuffer::<f64>::get(obj)
        && buf.is_c_contiguous()
    {
        let len = buf.item_count();
        let ptr = buf.buf_ptr() as *const f64;
        let slice = if len == 0 || ptr.is_null() {
            &[]
        } else {
            // SAFETY: `buf` pins a contiguous `f64` PEP 3118 buffer while the GIL is held in `f`.
            unsafe { std::slice::from_raw_parts(ptr, len) }
        };
        return f(slice).map_err(PyValueError::new_err);
    }
    let vec: Vec<f64> = obj.extract()?;
    f(&vec).map_err(PyValueError::new_err)
}

fn with_i8_slice_or_vec<R, F>(obj: &Bound<'_, PyAny>, f: F) -> PyResult<R>
where
    F: FnOnce(&[i8]) -> Result<R, String>,
{
    if let Ok(view_ref) = obj.extract::<PyRef<'_, VectorView>>() {
        let slice = view_ref.row_i8()?;
        return f(slice).map_err(PyValueError::new_err);
    }
    if let Ok(buf) = PyBuffer::<i8>::get(obj)
        && buf.is_c_contiguous()
    {
        let len = buf.item_count();
        let ptr = buf.buf_ptr() as *const i8;
        let slice = if len == 0 || ptr.is_null() {
            &[]
        } else {
            // SAFETY: `buf` pins a contiguous `i8` PEP 3118 buffer while the GIL is held in `f`.
            unsafe { std::slice::from_raw_parts(ptr, len) }
        };
        return f(slice).map_err(PyValueError::new_err);
    }
    let vec: Vec<i8> = obj.extract()?;
    f(&vec).map_err(PyValueError::new_err)
}

fn with_u8_slice_or_vec<R, F>(obj: &Bound<'_, PyAny>, f: F) -> PyResult<R>
where
    F: FnOnce(&[u8]) -> Result<R, String>,
{
    if let Ok(view_ref) = obj.extract::<PyRef<'_, VectorView>>() {
        let slice = view_ref.row_bytes()?;
        return f(slice).map_err(PyValueError::new_err);
    }
    if let Ok(bytes) = obj.extract::<&[u8]>() {
        return f(bytes).map_err(PyValueError::new_err);
    }
    if let Ok(buf) = PyBuffer::<u8>::get(obj)
        && buf.is_c_contiguous()
    {
        let len = buf.item_count();
        let ptr = buf.buf_ptr() as *const u8;
        let slice = if len == 0 || ptr.is_null() {
            &[]
        } else {
            // SAFETY: `buf` pins a contiguous `u8` PEP 3118 buffer while the GIL is held in `f`.
            unsafe { std::slice::from_raw_parts(ptr, len) }
        };
        return f(slice).map_err(PyValueError::new_err);
    }
    let vec: Vec<u8> = obj.extract()?;
    f(&vec).map_err(PyValueError::new_err)
}

/// Encode an Oracle 26ai `VECTOR(*, FLOAT32)` binary frame.
#[pyfunction]
pub fn encode_oracle_vector_f32<'py>(
    py: Python<'py>,
    values: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = with_f32_slice_or_vec(values, |slice| Ok(rs_encode_oracle_vector_f32(slice)))?;
    PyBytes::new_with(py, bytes.len(), |buf| {
        buf.copy_from_slice(&bytes);
        Ok(())
    })
}

/// Decode an Oracle 26ai `VECTOR(*, FLOAT32)` binary frame.
#[pyfunction]
pub fn decode_oracle_vector_f32(py: Python<'_>, data: &[u8]) -> PyResult<Vec<f32>> {
    py.detach(|| rs_decode_oracle_vector_f32(data))
        .map_err(PyValueError::new_err)
}

/// Encode an Oracle 26ai `VECTOR(*, FLOAT64)` binary frame.
#[pyfunction]
pub fn encode_oracle_vector_f64<'py>(
    py: Python<'py>,
    values: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = with_f64_slice_or_vec(values, |slice| Ok(rs_encode_oracle_vector_f64(slice)))?;
    PyBytes::new_with(py, bytes.len(), |buf| {
        buf.copy_from_slice(&bytes);
        Ok(())
    })
}

/// Decode an Oracle 26ai `VECTOR(*, FLOAT64)` binary frame.
#[pyfunction]
pub fn decode_oracle_vector_f64(py: Python<'_>, data: &[u8]) -> PyResult<Vec<f64>> {
    py.detach(|| rs_decode_oracle_vector_f64(data))
        .map_err(PyValueError::new_err)
}

/// Encode an Oracle 26ai `VECTOR(*, INT8)` binary frame.
#[pyfunction]
pub fn encode_oracle_vector_i8<'py>(
    py: Python<'py>,
    values: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = with_i8_slice_or_vec(values, |slice| Ok(rs_encode_oracle_vector_i8(slice)))?;
    PyBytes::new_with(py, bytes.len(), |buf| {
        buf.copy_from_slice(&bytes);
        Ok(())
    })
}

/// Decode an Oracle 26ai `VECTOR(*, INT8)` binary frame.
#[pyfunction]
pub fn decode_oracle_vector_i8(py: Python<'_>, data: &[u8]) -> PyResult<Vec<i8>> {
    py.detach(|| rs_decode_oracle_vector_i8(data))
        .map_err(PyValueError::new_err)
}

/// Encode an Oracle 26ai `VECTOR(*, BINARY)` packed bit-vector binary frame.
#[pyfunction]
pub fn encode_oracle_vector_binary<'py>(
    py: Python<'py>,
    bits: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = with_u8_slice_or_vec(bits, |slice| Ok(rs_encode_oracle_vector_binary(slice)))?;
    PyBytes::new_with(py, bytes.len(), |buf| {
        buf.copy_from_slice(&bytes);
        Ok(())
    })
}

/// Decode an Oracle 26ai `VECTOR(*, BINARY)` packed bit-vector binary frame.
#[pyfunction]
pub fn decode_oracle_vector_binary(py: Python<'_>, data: &[u8]) -> PyResult<Vec<u8>> {
    py.detach(|| rs_decode_oracle_vector_binary(data))
        .map_err(PyValueError::new_err)
}

/// Encode a `pgvector` binary `vector` (`[u16 dim BE][u16 unused=0 BE][f32 BE; dim]`).
#[pyfunction]
pub fn encode_pgvector_f32<'py>(
    py: Python<'py>,
    values: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = with_f32_slice_or_vec(values, rs_encode_pgvector_f32)?;
    PyBytes::new_with(py, bytes.len(), |buf| {
        buf.copy_from_slice(&bytes);
        Ok(())
    })
}

/// Decode a `pgvector` binary `vector` (`[u16 dim BE][u16 unused=0 BE][f32 BE; dim]`).
#[pyfunction]
pub fn decode_pgvector_f32(py: Python<'_>, data: &[u8]) -> PyResult<Vec<f32>> {
    py.detach(|| rs_decode_pgvector_f32(data))
        .map_err(PyValueError::new_err)
}

/// Encode a `pgvector` binary `halfvec` (`[u16 dim BE][u16 unused=0 BE][f16 BE; dim]`).
#[pyfunction]
pub fn encode_pgvector_f16<'py>(
    py: Python<'py>,
    values: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = with_f32_slice_or_vec(values, rs_encode_pgvector_f16)?;
    PyBytes::new_with(py, bytes.len(), |buf| {
        buf.copy_from_slice(&bytes);
        Ok(())
    })
}

/// Decode a `pgvector` binary `halfvec` (`[u16 dim BE][u16 unused=0 BE][f16 BE; dim]`).
#[pyfunction]
pub fn decode_pgvector_f16(py: Python<'_>, data: &[u8]) -> PyResult<Vec<f32>> {
    py.detach(|| rs_decode_pgvector_f16(data))
        .map_err(PyValueError::new_err)
}

/// Encode a PostgreSQL / `pgvector` binary `bit` (`VARBIT`).
#[pyfunction]
pub fn encode_pgvector_bit<'py>(
    py: Python<'py>,
    packed_bytes: &Bound<'py, PyAny>,
    bit_len: usize,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = with_u8_slice_or_vec(packed_bytes, |slice| rs_encode_pgvector_bit(slice, bit_len))?;
    PyBytes::new_with(py, bytes.len(), |buf| {
        buf.copy_from_slice(&bytes);
        Ok(())
    })
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
    let py_bytes = PyBytes::new_with(py, raw.len(), |buf| {
        buf.copy_from_slice(&raw);
        Ok(())
    })?;
    Ok((py_bytes, bit_len))
}

/// Encode JSON bytes with an Oracle 26ai `OSON` binary header (`\xff\x4a\x5a`).
#[pyfunction]
pub fn encode_oson_payload<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = py.detach(|| rs_encode_oson_payload(data));
    PyBytes::new_with(py, bytes.len(), |buf| {
        buf.copy_from_slice(&bytes);
        Ok(())
    })
}

/// Validate an Oracle 26ai `OSON` binary header and return the underlying JSON payload bytes.
#[pyfunction]
pub fn decode_oson_payload<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
    let slice = rs_decode_oson_payload(data).map_err(PyValueError::new_err)?;
    PyBytes::new_with(py, slice.len(), |buf| {
        buf.copy_from_slice(slice);
        Ok(())
    })
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
        let empty_reader = super::open_lazy_arrow_ipc_stream(empty_carrier).unwrap();
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
        let multi_reader = super::open_lazy_arrow_ipc_stream(multi_carrier).unwrap();
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
        assert!(super::open_lazy_arrow_ipc_stream(empty_slice).is_err());
        let truncated = &multi_buf[..multi_buf.len() / 2];
        let trunc_slice = super::OwnedArrowIpcSlice::try_new(
            super::ArrowBacking::Bytes(Arc::<[u8]>::from(truncated)),
            truncated.len(),
        )
        .unwrap();
        assert!(super::open_lazy_arrow_ipc_stream(trunc_slice).is_err());

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
}
