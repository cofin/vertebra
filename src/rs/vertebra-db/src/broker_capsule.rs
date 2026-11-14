//! Cross-`cdylib` C-ABI capsule contract, DBRPC MsgPack framing, and SQL placeholder normalizer.

use std::any::Any;
use std::collections::HashMap;
use std::ffi::{CStr, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

#[cfg(feature = "pyo3")]
use pyo3::prelude::*;
#[cfg(feature = "pyo3")]
use pyo3::types::PyCapsule;
use vertebra_core::dbrpc::{DBRPC_HEADER_SIZE, DbRpcHeader, DbRpcOp};

/// Canonical `PyCapsule` name used for cross-extension [`BrokerEngineCapsule`] registration.
pub const BROKER_ENGINE_CAPSULE_NAME: &CStr = c"vertebra.BrokerEngineCapsule";

/// Active binary ABI version expected by [`BrokerEngineCapsule`].
pub const BROKER_ENGINE_ABI_VERSION: u32 = 1;

/// Visitor callback invoked inside `extern "C"` vtable functions to transfer UTF-8 or byte payloads.
pub type BrokerBytesVisitFn =
    unsafe extern "C" fn(out_ctx: *mut c_void, ptr: *const u8, len: usize);

/// Sentinel return value emitted by [`BrokerEngineCapsule::normalize_fn`] when a panic is trapped.
const NORMALIZE_PANIC_SENTINEL: u32 = u32::MAX;

/// Pluggable native database engine backend registered with the shared-memory broker by dialect tag.
pub trait BrokerEngineBackend: Send + Sync {
    /// Canonical dialect tag (`vertebra_core::broker::DIALECT_TAG_*`).
    fn dialect_tag(&self) -> u16;

    /// Canonical lowercase dialect identifier (for example `"mysql"`).
    fn dialect_name(&self) -> &'static str;

    /// Normalize SQL placeholders for `ShmStatementCache` plan storage, returning `(normalized_sql, param_count)`.
    fn normalize_statement(&self, sql: &str) -> (String, usize);
}

/// Extension trait implemented by native engine backends that host broker-owned physical pools.
pub trait BrokerPoolBackend: BrokerEngineBackend {
    /// Validate `dsn`, initialize a bounded native pool (at most `max_size`), and return its `pool_id`.
    fn open_pool(&self, dsn: &str, max_size: u32) -> Result<u64, String>;

    /// Execute a 23-byte-prefixed DBRPC request frame on `pool_id`, returning an encoded `DbRpcResponse`.
    fn execute(&self, pool_id: u64, frame: &[u8]) -> Result<Vec<u8>, String>;

    /// Idempotently close and drain `pool_id`, ignoring unknown or already-closed identifiers.
    fn close_pool(&self, pool_id: u64);
}

struct CapsuleState {
    backend: Arc<dyn BrokerPoolBackend>,
}

/// Versioned C-ABI vtable exported by optional engine extensions via `broker_engine_capsule()`.
#[repr(C)]
pub struct BrokerEngineCapsule {
    pub abi_version: u32,
    pub dialect_tag: u16,
    pub name_ptr: *const u8,
    pub name_len: usize,
    pub ctx: *const c_void,
    pub clone_ctx_fn: unsafe extern "C" fn(*const c_void) -> *const c_void,
    pub drop_ctx_fn: unsafe extern "C" fn(*const c_void),
    pub normalize_fn: unsafe extern "C" fn(
        *const c_void,
        *const u8,
        usize,
        *mut c_void,
        BrokerBytesVisitFn,
    ) -> u32,
    pub open_pool_fn: unsafe extern "C" fn(
        *const c_void,
        *const u8,
        usize,
        u32,
        *mut u64,
        *mut c_void,
        BrokerBytesVisitFn,
    ) -> i32,
    pub execute_fn: unsafe extern "C" fn(
        *const c_void,
        u64,
        *const u8,
        usize,
        *mut c_void,
        BrokerBytesVisitFn,
    ) -> i32,
    pub close_pool_fn: unsafe extern "C" fn(*const c_void, u64),
}

// SAFETY: Every pointer in `BrokerEngineCapsule` refers to immutable vtable code, static UTF-8 name
// bytes, or an `Arc<CapsuleState>` whose inner `dyn BrokerPoolBackend` is `Send + Sync`.
unsafe impl Send for BrokerEngineCapsule {}
// SAFETY: Shared access only invokes `Sync` methods on the underlying `Arc<dyn BrokerPoolBackend>`.
unsafe impl Sync for BrokerEngineCapsule {}

impl Drop for BrokerEngineCapsule {
    fn drop(&mut self) {
        if !self.ctx.is_null() {
            let ptr = self.ctx;
            self.ctx = std::ptr::null();
            // SAFETY: `self.ctx` was created with `Arc::into_raw` and is released once on drop.
            unsafe {
                (self.drop_ctx_fn)(ptr);
            }
        }
    }
}

fn format_panic_payload(payload: Box<dyn Any + Send>) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        format!("broker engine backend panicked: {text}")
    } else if let Some(text) = payload.downcast_ref::<String>() {
        format!("broker engine backend panicked: {text}")
    } else {
        "broker engine backend panicked".to_string()
    }
}

unsafe fn emit_bytes(out_ctx: *mut c_void, visit_cb: BrokerBytesVisitFn, bytes: &[u8]) {
    // SAFETY: The caller guarantees `out_ctx` and `visit_cb` remain valid for this synchronous call.
    unsafe {
        visit_cb(out_ctx, bytes.as_ptr(), bytes.len());
    }
}

unsafe fn slice_from_raw<'a>(ptr: *const u8, len: usize) -> &'a [u8] {
    if len == 0 || ptr.is_null() {
        &[]
    } else {
        // SAFETY: The caller passes a valid readable buffer of `len` bytes for this synchronous call.
        unsafe { std::slice::from_raw_parts(ptr, len) }
    }
}

unsafe extern "C" fn capsule_clone_ctx(ctx: *const c_void) -> *const c_void {
    if ctx.is_null() {
        return std::ptr::null();
    }
    let state_ptr = ctx.cast::<CapsuleState>();
    // SAFETY: `ctx` originates from `Arc::into_raw(Arc<CapsuleState>)` with a strong count >= 1.
    unsafe {
        Arc::increment_strong_count(state_ptr);
    }
    ctx
}

unsafe extern "C" fn capsule_drop_ctx(ctx: *const c_void) {
    if ctx.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: `ctx` transfers one owned `Arc<CapsuleState>` strong count back to `Arc::from_raw`.
        unsafe {
            drop(Arc::from_raw(ctx.cast::<CapsuleState>()));
        }
    }));
}

unsafe extern "C" fn capsule_normalize(
    ctx: *const c_void,
    sql_ptr: *const u8,
    sql_len: usize,
    out_ctx: *mut c_void,
    visit_cb: BrokerBytesVisitFn,
) -> u32 {
    let outcome = catch_unwind(AssertUnwindSafe(|| -> Result<(String, usize), String> {
        if ctx.is_null() {
            return Err("null BrokerEngineCapsule context".to_string());
        }
        // SAFETY: `sql_ptr..sql_ptr + sql_len` is valid for the duration of the FFI call.
        let sql_bytes = unsafe { slice_from_raw(sql_ptr, sql_len) };
        let sql = std::str::from_utf8(sql_bytes)
            .map_err(|e| format!("invalid UTF-8 SQL payload in normalize: {e}"))?;
        // SAFETY: `ctx` points to a live `CapsuleState` owned by caller or capsule.
        let state = unsafe { &*ctx.cast::<CapsuleState>() };
        Ok(state.backend.normalize_statement(sql))
    }));
    match outcome {
        Ok(Ok((norm, count))) => {
            let capped = u32::try_from(count).unwrap_or(NORMALIZE_PANIC_SENTINEL - 1);
            // SAFETY: Synchronous visitor invocation before `norm` drops.
            unsafe {
                emit_bytes(out_ctx, visit_cb, norm.as_bytes());
            }
            capped
        }
        Ok(Err(msg)) => {
            // SAFETY: Synchronous error delivery before `msg` drops.
            unsafe {
                emit_bytes(out_ctx, visit_cb, msg.as_bytes());
            }
            NORMALIZE_PANIC_SENTINEL
        }
        Err(payload) => {
            let msg = format_panic_payload(payload);
            // SAFETY: Synchronous error delivery before `msg` drops.
            unsafe {
                emit_bytes(out_ctx, visit_cb, msg.as_bytes());
            }
            NORMALIZE_PANIC_SENTINEL
        }
    }
}

unsafe extern "C" fn capsule_open_pool(
    ctx: *const c_void,
    dsn_ptr: *const u8,
    dsn_len: usize,
    max_size: u32,
    out_pool_id: *mut u64,
    out_ctx: *mut c_void,
    visit_cb: BrokerBytesVisitFn,
) -> i32 {
    let outcome = catch_unwind(AssertUnwindSafe(|| -> Result<u64, String> {
        if ctx.is_null() {
            return Err("null BrokerEngineCapsule context".to_string());
        }
        if out_pool_id.is_null() {
            return Err("null out_pool_id pointer".to_string());
        }
        // SAFETY: `dsn_ptr..dsn_ptr + dsn_len` is valid for the duration of the FFI call.
        let dsn_bytes = unsafe { slice_from_raw(dsn_ptr, dsn_len) };
        let dsn = std::str::from_utf8(dsn_bytes)
            .map_err(|e| format!("invalid UTF-8 DSN payload in open_pool: {e}"))?;
        // SAFETY: `ctx` points to a live `CapsuleState`.
        let state = unsafe { &*ctx.cast::<CapsuleState>() };
        state.backend.open_pool(dsn, max_size)
    }));
    match outcome {
        Ok(Ok(pool_id)) => {
            // SAFETY: `out_pool_id` was verified non-null above.
            unsafe {
                *out_pool_id = pool_id;
            }
            0
        }
        Ok(Err(msg)) => {
            // SAFETY: Synchronous error delivery before `msg` drops.
            unsafe {
                emit_bytes(out_ctx, visit_cb, msg.as_bytes());
            }
            1
        }
        Err(payload) => {
            let msg = format_panic_payload(payload);
            // SAFETY: Synchronous error delivery before `msg` drops.
            unsafe {
                emit_bytes(out_ctx, visit_cb, msg.as_bytes());
            }
            -1
        }
    }
}

unsafe extern "C" fn capsule_execute(
    ctx: *const c_void,
    pool_id: u64,
    frame_ptr: *const u8,
    frame_len: usize,
    out_ctx: *mut c_void,
    visit_cb: BrokerBytesVisitFn,
) -> i32 {
    let outcome = catch_unwind(AssertUnwindSafe(|| -> Result<Vec<u8>, String> {
        if ctx.is_null() {
            return Err("null BrokerEngineCapsule context".to_string());
        }
        // SAFETY: `frame_ptr..frame_ptr + frame_len` is valid for the duration of the FFI call.
        let frame = unsafe { slice_from_raw(frame_ptr, frame_len) };
        // SAFETY: `ctx` points to a live `CapsuleState`.
        let state = unsafe { &*ctx.cast::<CapsuleState>() };
        state.backend.execute(pool_id, frame)
    }));
    match outcome {
        Ok(Ok(bytes)) => {
            // SAFETY: Synchronous response byte delivery before `bytes` drops.
            unsafe {
                emit_bytes(out_ctx, visit_cb, &bytes);
            }
            0
        }
        Ok(Err(msg)) => {
            // SAFETY: Synchronous error delivery before `msg` drops.
            unsafe {
                emit_bytes(out_ctx, visit_cb, msg.as_bytes());
            }
            1
        }
        Err(payload) => {
            let msg = format_panic_payload(payload);
            // SAFETY: Synchronous error delivery before `msg` drops.
            unsafe {
                emit_bytes(out_ctx, visit_cb, msg.as_bytes());
            }
            -1
        }
    }
}

unsafe extern "C" fn capsule_close_pool(ctx: *const c_void, pool_id: u64) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if ctx.is_null() {
            return;
        }
        // SAFETY: `ctx` points to a live `CapsuleState`.
        let state = unsafe { &*ctx.cast::<CapsuleState>() };
        state.backend.close_pool(pool_id);
    }));
}

/// Construct a raw `#[repr(C)]` [`BrokerEngineCapsule`] wrapping `backend`.
#[must_use]
pub fn new_broker_engine_capsule(backend: Arc<dyn BrokerPoolBackend>) -> BrokerEngineCapsule {
    let dialect_tag = backend.dialect_tag();
    let name = backend.dialect_name();
    let state = Arc::new(CapsuleState { backend });
    BrokerEngineCapsule {
        abi_version: BROKER_ENGINE_ABI_VERSION,
        dialect_tag,
        name_ptr: name.as_ptr(),
        name_len: name.len(),
        ctx: Arc::into_raw(state).cast::<c_void>(),
        clone_ctx_fn: capsule_clone_ctx,
        drop_ctx_fn: capsule_drop_ctx,
        normalize_fn: capsule_normalize,
        open_pool_fn: capsule_open_pool,
        execute_fn: capsule_execute,
        close_pool_fn: capsule_close_pool,
    }
}

/// Construct a Python `PyCapsule` (`c"vertebra.BrokerEngineCapsule"`) wrapping `backend`.
#[cfg(feature = "pyo3")]
pub fn new_broker_engine_pycapsule<'py>(
    py: Python<'py>,
    backend: Arc<dyn BrokerPoolBackend>,
) -> PyResult<Bound<'py, PyCapsule>> {
    let capsule = new_broker_engine_capsule(backend);
    PyCapsule::new_with_value(py, capsule, BROKER_ENGINE_CAPSULE_NAME)
}

unsafe extern "C" fn collect_bytes_cb(out_ctx: *mut c_void, ptr: *const u8, len: usize) {
    if out_ctx.is_null() || len == 0 || ptr.is_null() {
        return;
    }
    // SAFETY: Caller passes `&mut Vec<u8>` as `out_ctx` and keeps `ptr..ptr+len` alive.
    let buf = unsafe { &mut *out_ctx.cast::<Vec<u8>>() };
    // SAFETY: `ptr..ptr+len` is valid for this synchronous visitor invocation.
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    buf.extend_from_slice(slice);
}

/// Refcounted host handle wrapping an imported foreign [`BrokerEngineCapsule`] vtable.
pub struct ForeignEngineCapsule {
    dialect_tag: u16,
    dialect_name: String,
    ctx: *const c_void,
    clone_ctx_fn: unsafe extern "C" fn(*const c_void) -> *const c_void,
    drop_ctx_fn: unsafe extern "C" fn(*const c_void),
    normalize_fn: unsafe extern "C" fn(
        *const c_void,
        *const u8,
        usize,
        *mut c_void,
        BrokerBytesVisitFn,
    ) -> u32,
    open_pool_fn: unsafe extern "C" fn(
        *const c_void,
        *const u8,
        usize,
        u32,
        *mut u64,
        *mut c_void,
        BrokerBytesVisitFn,
    ) -> i32,
    execute_fn: unsafe extern "C" fn(
        *const c_void,
        u64,
        *const u8,
        usize,
        *mut c_void,
        BrokerBytesVisitFn,
    ) -> i32,
    close_pool_fn: unsafe extern "C" fn(*const c_void, u64),
}

// SAFETY: The wrapped C-ABI vtable holds `Send + Sync` state guarded by atomic refcounting.
unsafe impl Send for ForeignEngineCapsule {}
// SAFETY: Concurrent shared calls only invoke thread-safe vtable callbacks on `ctx`.
unsafe impl Sync for ForeignEngineCapsule {}

impl Clone for ForeignEngineCapsule {
    fn clone(&self) -> Self {
        // SAFETY: `self.ctx` has strong count >= 1 while `self` is alive.
        let cloned_ctx = unsafe { (self.clone_ctx_fn)(self.ctx) };
        Self {
            dialect_tag: self.dialect_tag,
            dialect_name: self.dialect_name.clone(),
            ctx: cloned_ctx,
            clone_ctx_fn: self.clone_ctx_fn,
            drop_ctx_fn: self.drop_ctx_fn,
            normalize_fn: self.normalize_fn,
            open_pool_fn: self.open_pool_fn,
            execute_fn: self.execute_fn,
            close_pool_fn: self.close_pool_fn,
        }
    }
}

impl Drop for ForeignEngineCapsule {
    fn drop(&mut self) {
        if !self.ctx.is_null() {
            let ptr = self.ctx;
            self.ctx = std::ptr::null();
            // SAFETY: `self` owns one refcounted reference in `ptr`, released once on drop.
            unsafe {
                (self.drop_ctx_fn)(ptr);
            }
        }
    }
}

impl ForeignEngineCapsule {
    /// Validate ABI version `1`, clone the capsule context, and wrap it as a safe [`ForeignEngineCapsule`].
    ///
    /// # Safety
    ///
    /// `ptr` must point to an initialized, aligned [`BrokerEngineCapsule`] whose fields obey the
    /// ABI v1 ownership and static/synchronous lifetime contract.
    pub unsafe fn from_capsule_ptr(ptr: *const BrokerEngineCapsule) -> Result<Self, String> {
        if ptr.is_null() {
            return Err("null BrokerEngineCapsule pointer".to_string());
        }
        // SAFETY: Caller guarantees `ptr` points to a live `BrokerEngineCapsule`.
        let raw = unsafe { &*ptr };
        let name_bytes = if raw.name_len == 0 || raw.name_ptr.is_null() {
            &[]
        } else {
            // SAFETY: `name_ptr..name_ptr+name_len` holds UTF-8 bytes describing the engine dialect.
            unsafe { std::slice::from_raw_parts(raw.name_ptr, raw.name_len) }
        };
        let dialect_name = String::from_utf8_lossy(name_bytes).into_owned();
        let label = if dialect_name.is_empty() {
            format!("dialect_tag={}", raw.dialect_tag)
        } else {
            dialect_name.clone()
        };
        if raw.abi_version != BROKER_ENGINE_ABI_VERSION {
            return Err(format!(
                "BrokerEngineCapsule '{label}' ABI version mismatch: expected {BROKER_ENGINE_ABI_VERSION}, got {}",
                raw.abi_version
            ));
        }
        if raw.ctx.is_null() {
            return Err(format!(
                "BrokerEngineCapsule '{label}' has a null ctx pointer"
            ));
        }
        // SAFETY: Calling `clone_ctx_fn` increments strong count and yields an owned handle.
        let owned_ctx = unsafe { (raw.clone_ctx_fn)(raw.ctx) };
        if owned_ctx.is_null() {
            return Err(format!(
                "BrokerEngineCapsule '{label}' clone_ctx_fn returned a null pointer"
            ));
        }
        Ok(Self {
            dialect_tag: raw.dialect_tag,
            dialect_name,
            ctx: owned_ctx,
            clone_ctx_fn: raw.clone_ctx_fn,
            drop_ctx_fn: raw.drop_ctx_fn,
            normalize_fn: raw.normalize_fn,
            open_pool_fn: raw.open_pool_fn,
            execute_fn: raw.execute_fn,
            close_pool_fn: raw.close_pool_fn,
        })
    }

    /// Dialect tag exposed by this capsule.
    #[must_use]
    pub const fn dialect_tag(&self) -> u16 {
        self.dialect_tag
    }

    /// Lowercase dialect name exposed by this capsule.
    #[must_use]
    pub fn dialect_name(&self) -> &str {
        &self.dialect_name
    }

    /// Normalize SQL placeholders via `normalize_fn`.
    pub fn normalize(&self, sql: &str) -> Result<(String, usize), String> {
        let mut out = Vec::with_capacity(sql.len());
        // SAFETY: All pointers passed into `normalize_fn` remain valid for the synchronous call.
        let count = unsafe {
            (self.normalize_fn)(
                self.ctx,
                sql.as_ptr(),
                sql.len(),
                (&mut out as *mut Vec<u8>).cast::<c_void>(),
                collect_bytes_cb,
            )
        };
        if count == NORMALIZE_PANIC_SENTINEL {
            let msg = String::from_utf8_lossy(&out).into_owned();
            return Err(if msg.is_empty() {
                format!("engine '{}' normalize panicked", self.dialect_name)
            } else {
                msg
            });
        }
        let norm = String::from_utf8(out).map_err(|e| {
            format!(
                "invalid normalized SQL UTF-8 from '{}': {e}",
                self.dialect_name
            )
        })?;
        Ok((norm, count as usize))
    }

    /// Open a broker-hosted native pool via `open_pool_fn`.
    pub fn open_pool(&self, dsn: &str, max_size: u32) -> Result<u64, String> {
        let mut pool_id: u64 = 0;
        let mut err_buf = Vec::new();
        // SAFETY: `out_pool_id` and `err_buf` live across this synchronous FFI call.
        let rc = unsafe {
            (self.open_pool_fn)(
                self.ctx,
                dsn.as_ptr(),
                dsn.len(),
                max_size,
                &mut pool_id,
                (&mut err_buf as *mut Vec<u8>).cast::<c_void>(),
                collect_bytes_cb,
            )
        };
        if rc == 0 {
            return Ok(pool_id);
        }
        let msg = String::from_utf8_lossy(&err_buf).into_owned();
        Err(if msg.is_empty() {
            format!(
                "engine '{}' open_pool failed with code {rc}",
                self.dialect_name
            )
        } else {
            msg
        })
    }

    /// Execute a DBRPC request frame against `pool_id` via `execute_fn`.
    pub fn execute(&self, pool_id: u64, frame: &[u8]) -> Result<Vec<u8>, String> {
        let mut out = Vec::new();
        // SAFETY: `frame` and `out` remain valid across this synchronous FFI call.
        let rc = unsafe {
            (self.execute_fn)(
                self.ctx,
                pool_id,
                frame.as_ptr(),
                frame.len(),
                (&mut out as *mut Vec<u8>).cast::<c_void>(),
                collect_bytes_cb,
            )
        };
        if rc == 0 {
            return Ok(out);
        }
        let msg = String::from_utf8_lossy(&out).into_owned();
        Err(if msg.is_empty() {
            format!(
                "engine '{}' execute failed with code {rc}",
                self.dialect_name
            )
        } else {
            msg
        })
    }

    /// Close `pool_id` idempotently via `close_pool_fn`.
    pub fn close_pool(&self, pool_id: u64) {
        // SAFETY: `self.ctx` is valid while `&self` is borrowed.
        unsafe {
            (self.close_pool_fn)(self.ctx, pool_id);
        }
    }
}

/// Scalar value carried across MessagePack-encoded `DbRpcRequest.params` and `DbRpcResponse.rows`.
#[derive(Clone, Debug, PartialEq)]
pub enum DbRpcScalar {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Str(String),
    Bytes(Vec<u8>),
}

/// Decoded 23-byte `DbRpcHeader` plus MessagePack `DbRpcRequest` payload.
#[derive(Clone, Debug, PartialEq)]
pub struct DbRpcDecodedRequest {
    pub version: u16,
    pub op: DbRpcOp,
    pub request_id: u64,
    pub lease_slot: u32,
    pub lease_generation: u64,
    pub sql: Option<String>,
    pub params: Vec<DbRpcScalar>,
    pub savepoint: Option<String>,
    pub chunk_rows: u64,
}

/// MessagePack-serializable `DbRpcResponse` payload matching `vertebra.extensions.broker.dbrpc.DbRpcResponse`.
#[derive(Clone, Debug, PartialEq)]
pub struct DbRpcEncodedResponse {
    pub request_id: u64,
    pub status: u8,
    pub rowcount: i64,
    pub columns: Option<Vec<String>>,
    pub rows: Option<Vec<Vec<DbRpcScalar>>>,
    pub arrow_shm: Option<String>,
    pub lastrowid: Option<u64>,
    pub error_class: Option<String>,
    pub error_message: Option<String>,
}

impl DbRpcEncodedResponse {
    /// Construct a successful DML/DDL/transaction control response without row data.
    #[must_use]
    pub fn ok_count(request_id: u64, rowcount: i64, lastrowid: Option<u64>) -> Self {
        Self {
            request_id,
            status: 0,
            rowcount,
            columns: None,
            rows: None,
            arrow_shm: None,
            lastrowid,
            error_class: None,
            error_message: None,
        }
    }

    /// Construct a successful query response with inline columns and rows.
    #[must_use]
    pub fn ok_rows(
        request_id: u64,
        rowcount: i64,
        columns: Vec<String>,
        rows: Vec<Vec<DbRpcScalar>>,
    ) -> Self {
        Self {
            request_id,
            status: 0,
            rowcount,
            columns: Some(columns),
            rows: Some(rows),
            arrow_shm: None,
            lastrowid: None,
            error_class: None,
            error_message: None,
        }
    }

    /// Construct a successful analytical query response backed by a `VTBARROW` POSIX SHM segment.
    #[must_use]
    pub fn ok_arrow_shm(
        request_id: u64,
        rowcount: i64,
        columns: Vec<String>,
        shm_name: String,
    ) -> Self {
        Self {
            request_id,
            status: 0,
            rowcount,
            columns: Some(columns),
            rows: None,
            arrow_shm: Some(shm_name),
            lastrowid: None,
            error_class: None,
            error_message: None,
        }
    }

    /// Construct a structured database execution error response with `status = 1`.
    #[must_use]
    pub fn error(
        request_id: u64,
        error_class: impl Into<String>,
        error_message: impl Into<String>,
    ) -> Self {
        Self {
            request_id,
            status: 1,
            rowcount: -1,
            columns: None,
            rows: None,
            arrow_shm: None,
            lastrowid: None,
            error_class: Some(error_class.into()),
            error_message: Some(error_message.into()),
        }
    }
}

struct MsgPackReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> MsgPackReader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn read_u8(&mut self) -> Result<u8, String> {
        let b = *self
            .buf
            .get(self.pos)
            .ok_or_else(|| "truncated msgpack frame".to_string())?;
        self.pos += 1;
        Ok(b)
    }

    fn read_exact(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| "msgpack length overflow".to_string())?;
        let slice = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| "truncated msgpack payload".to_string())?;
        self.pos = end;
        Ok(slice)
    }

    fn read_u16(&mut self) -> Result<u16, String> {
        let s = self.read_exact(2)?;
        Ok(u16::from_be_bytes([s[0], s[1]]))
    }

    fn read_u32(&mut self) -> Result<u32, String> {
        let s = self.read_exact(4)?;
        Ok(u32::from_be_bytes([s[0], s[1], s[2], s[3]]))
    }

    fn read_u64(&mut self) -> Result<u64, String> {
        let s = self.read_exact(8)?;
        Ok(u64::from_be_bytes([
            s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7],
        ]))
    }

    fn read_array_len(&mut self) -> Result<Option<usize>, String> {
        let marker = self.read_u8()?;
        match marker {
            0xc0 => Ok(None),
            0x90..=0x9f => Ok(Some(usize::from(marker & 0x0f))),
            0xdc => Ok(Some(usize::from(self.read_u16()?))),
            0xdd => usize::try_from(self.read_u32()?)
                .map(Some)
                .map_err(|_| "msgpack array length exceeds usize".to_string()),
            other => Err(format!("expected msgpack array marker, got 0x{other:02x}")),
        }
    }

    fn read_scalar(&mut self) -> Result<DbRpcScalar, String> {
        let marker = self.read_u8()?;
        match marker {
            0x00..=0x7f => Ok(DbRpcScalar::Int(i64::from(marker))),
            0xe0..=0xff => Ok(DbRpcScalar::Int(i64::from(marker as i8))),
            0xc0 => Ok(DbRpcScalar::Null),
            0xc2 => Ok(DbRpcScalar::Bool(false)),
            0xc3 => Ok(DbRpcScalar::Bool(true)),
            0xc4 => {
                let len = usize::from(self.read_u8()?);
                Ok(DbRpcScalar::Bytes(self.read_exact(len)?.to_vec()))
            }
            0xc5 => {
                let len = usize::from(self.read_u16()?);
                Ok(DbRpcScalar::Bytes(self.read_exact(len)?.to_vec()))
            }
            0xc6 => {
                let len = usize::try_from(self.read_u32()?)
                    .map_err(|_| "msgpack bin length overflow".to_string())?;
                Ok(DbRpcScalar::Bytes(self.read_exact(len)?.to_vec()))
            }
            0xca => {
                let s = self.read_exact(4)?;
                Ok(DbRpcScalar::Float(f64::from(f32::from_be_bytes([
                    s[0], s[1], s[2], s[3],
                ]))))
            }
            0xcb => {
                let s = self.read_exact(8)?;
                Ok(DbRpcScalar::Float(f64::from_be_bytes([
                    s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7],
                ])))
            }
            0xcc => Ok(DbRpcScalar::Int(i64::from(self.read_u8()?))),
            0xcd => Ok(DbRpcScalar::Int(i64::from(self.read_u16()?))),
            0xce => Ok(DbRpcScalar::Int(i64::from(self.read_u32()?))),
            0xcf => {
                let v = self.read_u64()?;
                match i64::try_from(v) {
                    Ok(signed) => Ok(DbRpcScalar::Int(signed)),
                    Err(_) => Ok(DbRpcScalar::UInt(v)),
                }
            }
            0xd0 => Ok(DbRpcScalar::Int(i64::from(self.read_u8()? as i8))),
            0xd1 => Ok(DbRpcScalar::Int(i64::from(self.read_u16()? as i16))),
            0xd2 => Ok(DbRpcScalar::Int(i64::from(self.read_u32()? as i32))),
            0xd3 => Ok(DbRpcScalar::Int(self.read_u64()? as i64)),
            0xa0..=0xbf => {
                let len = usize::from(marker & 0x1f);
                let s = std::str::from_utf8(self.read_exact(len)?)
                    .map_err(|e| format!("invalid UTF-8 in msgpack str: {e}"))?;
                Ok(DbRpcScalar::Str(s.to_owned()))
            }
            0xd9 => {
                let len = usize::from(self.read_u8()?);
                let s = std::str::from_utf8(self.read_exact(len)?)
                    .map_err(|e| format!("invalid UTF-8 in msgpack str: {e}"))?;
                Ok(DbRpcScalar::Str(s.to_owned()))
            }
            0xda => {
                let len = usize::from(self.read_u16()?);
                let s = std::str::from_utf8(self.read_exact(len)?)
                    .map_err(|e| format!("invalid UTF-8 in msgpack str: {e}"))?;
                Ok(DbRpcScalar::Str(s.to_owned()))
            }
            0xdb => {
                let len = usize::try_from(self.read_u32()?)
                    .map_err(|_| "msgpack str length overflow".to_string())?;
                let s = std::str::from_utf8(self.read_exact(len)?)
                    .map_err(|e| format!("invalid UTF-8 in msgpack str: {e}"))?;
                Ok(DbRpcScalar::Str(s.to_owned()))
            }
            other => Err(format!("unsupported msgpack scalar marker 0x{other:02x}")),
        }
    }

    fn read_u64_scalar(&mut self, field: &str) -> Result<u64, String> {
        match self.read_scalar()? {
            DbRpcScalar::Int(v) if v >= 0 => Ok(v as u64),
            DbRpcScalar::UInt(v) => Ok(v),
            other => Err(format!(
                "expected non-negative int for {field}, got {other:?}"
            )),
        }
    }

    fn read_opt_string(&mut self, field: &str) -> Result<Option<String>, String> {
        match self.read_scalar()? {
            DbRpcScalar::Null => Ok(None),
            DbRpcScalar::Str(s) => Ok(Some(s)),
            other => Err(format!("expected str | None for {field}, got {other:?}")),
        }
    }

    fn read_params(&mut self) -> Result<Vec<DbRpcScalar>, String> {
        let Some(len) = self.read_array_len()? else {
            return Ok(Vec::new());
        };
        let mut params = Vec::with_capacity(len);
        for _ in 0..len {
            params.push(self.read_scalar()?);
        }
        Ok(params)
    }
}

fn write_mp_u64(buf: &mut Vec<u8>, v: u64) {
    if v <= 0x7f {
        buf.push(v as u8);
    } else if v <= u64::from(u8::MAX) {
        buf.push(0xcc);
        buf.push(v as u8);
    } else if v <= u64::from(u16::MAX) {
        buf.push(0xcd);
        buf.extend_from_slice(&(v as u16).to_be_bytes());
    } else if v <= u64::from(u32::MAX) {
        buf.push(0xce);
        buf.extend_from_slice(&(v as u32).to_be_bytes());
    } else {
        buf.push(0xcf);
        buf.extend_from_slice(&v.to_be_bytes());
    }
}

fn write_mp_i64(buf: &mut Vec<u8>, v: i64) {
    if v >= 0 {
        write_mp_u64(buf, v as u64);
    } else if v >= -32 {
        buf.push(v as i8 as u8);
    } else if v >= i64::from(i8::MIN) {
        buf.push(0xd0);
        buf.push(v as i8 as u8);
    } else if v >= i64::from(i16::MIN) {
        buf.push(0xd1);
        buf.extend_from_slice(&(v as i16).to_be_bytes());
    } else if v >= i64::from(i32::MIN) {
        buf.push(0xd2);
        buf.extend_from_slice(&(v as i32).to_be_bytes());
    } else {
        buf.push(0xd3);
        buf.extend_from_slice(&v.to_be_bytes());
    }
}

fn write_mp_str(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    let len = bytes.len();
    if len <= 31 {
        buf.push(0xa0 | (len as u8));
    } else if len <= usize::from(u8::MAX) {
        buf.push(0xd9);
        buf.push(len as u8);
    } else if len <= usize::from(u16::MAX) {
        buf.push(0xda);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xdb);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
    buf.extend_from_slice(bytes);
}

fn write_mp_bin(buf: &mut Vec<u8>, bytes: &[u8]) {
    let len = bytes.len();
    if len <= usize::from(u8::MAX) {
        buf.push(0xc4);
        buf.push(len as u8);
    } else if len <= usize::from(u16::MAX) {
        buf.push(0xc5);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xc6);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
    buf.extend_from_slice(bytes);
}

fn write_mp_array_hdr(buf: &mut Vec<u8>, len: usize) {
    if len <= 15 {
        buf.push(0x90 | (len as u8));
    } else if len <= usize::from(u16::MAX) {
        buf.push(0xdc);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xdd);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
}

fn write_mp_scalar(buf: &mut Vec<u8>, val: &DbRpcScalar) {
    match val {
        DbRpcScalar::Null => buf.push(0xc0),
        DbRpcScalar::Bool(false) => buf.push(0xc2),
        DbRpcScalar::Bool(true) => buf.push(0xc3),
        DbRpcScalar::Int(v) => write_mp_i64(buf, *v),
        DbRpcScalar::UInt(v) => write_mp_u64(buf, *v),
        DbRpcScalar::Float(v) => {
            buf.push(0xcb);
            buf.extend_from_slice(&v.to_be_bytes());
        }
        DbRpcScalar::Str(s) => write_mp_str(buf, s),
        DbRpcScalar::Bytes(b) => write_mp_bin(buf, b),
    }
}

/// Decode a 23-byte binary [`DbRpcHeader`] followed by a MessagePack `DbRpcRequest` array.
pub fn decode_dbrpc_request_frame(frame: &[u8]) -> Result<DbRpcDecodedRequest, String> {
    if frame.len() < DBRPC_HEADER_SIZE {
        return Err(format!(
            "truncated dbrpc request frame: expected at least {DBRPC_HEADER_SIZE} bytes, got {}",
            frame.len()
        ));
    }
    let header = DbRpcHeader::decode(&frame[..DBRPC_HEADER_SIZE]).map_err(|e| e.to_string())?;
    let mut reader = MsgPackReader::new(&frame[DBRPC_HEADER_SIZE..]);
    let arr_len = reader
        .read_array_len()?
        .ok_or_else(|| "expected non-null DbRpcRequest msgpack array".to_string())?;
    if arr_len < 5 {
        return Err(format!(
            "truncated DbRpcRequest array: expected at least 5 fields, got {arr_len}"
        ));
    }
    let version_u64 = reader.read_u64_scalar("version")?;
    let op_u64 = reader.read_u64_scalar("op")?;
    let request_id = reader.read_u64_scalar("request_id")?;
    let lease_slot_u64 = reader.read_u64_scalar("lease_slot")?;
    let lease_generation = reader.read_u64_scalar("lease_generation")?;

    let version =
        u16::try_from(version_u64).map_err(|_| format!("invalid dbrpc version {version_u64}"))?;
    let op_u8 = u8::try_from(op_u64).map_err(|_| format!("invalid dbrpc op {op_u64}"))?;
    let op = DbRpcOp::from_u8(op_u8).map_err(|e| e.to_string())?;
    let lease_slot = u32::try_from(lease_slot_u64)
        .map_err(|_| format!("invalid lease_slot {lease_slot_u64}"))?;

    if version != header.version || op != header.op || request_id != header.request_id {
        return Err("dbrpc header metadata does not match body payload".to_string());
    }

    let sql = if arr_len > 5 {
        reader.read_opt_string("sql")?
    } else {
        None
    };
    let params = if arr_len > 6 {
        reader.read_params()?
    } else {
        Vec::new()
    };
    let savepoint = if arr_len > 7 {
        reader.read_opt_string("savepoint")?
    } else {
        None
    };
    let chunk_rows = if arr_len > 8 {
        reader.read_u64_scalar("chunk_rows")?
    } else {
        0
    };
    for idx in 9..arr_len {
        let _ = reader
            .read_scalar()
            .map_err(|e| format!("field {idx}: {e}"))?;
    }

    Ok(DbRpcDecodedRequest {
        version,
        op,
        request_id,
        lease_slot,
        lease_generation,
        sql,
        params,
        savepoint,
        chunk_rows,
    })
}

/// Serialize a [`DbRpcEncodedResponse`] into a 9-element MessagePack array matching `msgspec`.
#[must_use]
pub fn encode_dbrpc_response_frame(resp: &DbRpcEncodedResponse) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    write_mp_array_hdr(&mut buf, 9);
    write_mp_u64(&mut buf, resp.request_id);
    write_mp_u64(&mut buf, u64::from(resp.status));
    write_mp_i64(&mut buf, resp.rowcount);
    match resp.columns.as_ref() {
        Some(cols) => {
            write_mp_array_hdr(&mut buf, cols.len());
            for col in cols {
                write_mp_str(&mut buf, col);
            }
        }
        None => buf.push(0xc0),
    }
    match resp.rows.as_ref() {
        Some(rows) => {
            write_mp_array_hdr(&mut buf, rows.len());
            for row in rows {
                write_mp_array_hdr(&mut buf, row.len());
                for cell in row {
                    write_mp_scalar(&mut buf, cell);
                }
            }
        }
        None => buf.push(0xc0),
    }
    match resp.arrow_shm.as_deref() {
        Some(name) => write_mp_str(&mut buf, name),
        None => buf.push(0xc0),
    }
    match resp.lastrowid {
        Some(id) => write_mp_u64(&mut buf, id),
        None => buf.push(0xc0),
    }
    match resp.error_class.as_deref() {
        Some(cls) => write_mp_str(&mut buf, cls),
        None => buf.push(0xc0),
    }
    match resp.error_message.as_deref() {
        Some(msg) => write_mp_str(&mut buf, msg),
        None => buf.push(0xc0),
    }
    buf
}

/// Target placeholder syntax emitted by [`normalize_placeholders`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlaceholderStyle {
    QuestionMark,
    DollarNumbered,
    ColonNumbered,
}

fn push_placeholder(out: &mut String, style: PlaceholderStyle, slot: usize) {
    match style {
        PlaceholderStyle::QuestionMark => out.push('?'),
        PlaceholderStyle::DollarNumbered => {
            out.push('$');
            out.push_str(&slot.to_string());
        }
        PlaceholderStyle::ColonNumbered => {
            out.push(':');
            out.push_str(&slot.to_string());
        }
    }
}

/// Single-pass SQL placeholder rewriter preserving single/double quotes, SQL comments, and `::`/`=`.
#[must_use]
pub fn normalize_placeholders(sql: &str, style: PlaceholderStyle) -> (String, usize) {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut out = String::with_capacity(len);
    let mut named_slots: HashMap<String, usize> = HashMap::new();
    let mut next_slot = 0usize;
    let mut max_slot = 0usize;
    let mut i = 0usize;

    while i < len {
        let ch = bytes[i];
        if ch == b'\'' || ch == b'"' {
            let quote = ch;
            out.push(quote as char);
            i += 1;
            while i < len {
                let c = bytes[i];
                if c == b'\\' && quote == b'\'' && i + 1 < len {
                    let next_ch = sql[i + 1..].chars().next().unwrap_or('\0');
                    out.push('\\');
                    out.push(next_ch);
                    i += 1 + next_ch.len_utf8();
                    continue;
                }
                let ch_utf8 = sql[i..].chars().next().unwrap_or('\0');
                out.push(ch_utf8);
                i += ch_utf8.len_utf8();
                if c == quote {
                    if i < len && bytes[i] == quote {
                        out.push(quote as char);
                        i += 1;
                        continue;
                    }
                    break;
                }
            }
            continue;
        }

        if ch == b'-' && i + 1 < len && bytes[i + 1] == b'-' {
            while i < len && bytes[i] != b'\n' {
                let ch_utf8 = sql[i..].chars().next().unwrap_or('\0');
                out.push(ch_utf8);
                i += ch_utf8.len_utf8();
            }
            continue;
        }

        if ch == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
            out.push_str("/*");
            i += 2;
            while i < len {
                if bytes[i] == b'*' && i + 1 < len && bytes[i + 1] == b'/' {
                    out.push_str("*/");
                    i += 2;
                    break;
                }
                let ch_utf8 = sql[i..].chars().next().unwrap_or('\0');
                out.push(ch_utf8);
                i += ch_utf8.len_utf8();
            }
            continue;
        }

        if ch == b'?' {
            next_slot += 1;
            max_slot = max_slot.max(next_slot);
            push_placeholder(&mut out, style, next_slot);
            i += 1;
            continue;
        }

        if ch == b'%' && i + 1 < len {
            if bytes[i + 1] == b'%' {
                out.push('%');
                i += 2;
                continue;
            }
            if bytes[i + 1] == b's' {
                next_slot += 1;
                max_slot = max_slot.max(next_slot);
                push_placeholder(&mut out, style, next_slot);
                i += 2;
                continue;
            }
            if bytes[i + 1] == b'('
                && let Some(end_paren) = sql[i + 2..].find(")s")
            {
                let name = &sql[i + 2..i + 2 + end_paren];
                if !name.is_empty() {
                    let slot = *named_slots.entry(name.to_owned()).or_insert_with(|| {
                        next_slot += 1;
                        next_slot
                    });
                    max_slot = max_slot.max(slot);
                    push_placeholder(&mut out, style, slot);
                    i += 2 + end_paren + 2;
                    continue;
                }
            }
        }

        if ch == b'$' && i + 1 < len && bytes[i + 1].is_ascii_digit() && bytes[i + 1] != b'0' {
            let start = i + 1;
            let mut end = start;
            while end < len && bytes[end].is_ascii_digit() {
                end += 1;
            }
            if let Ok(ordinal) = sql[start..end].parse::<usize>()
                && ordinal >= 1
            {
                let slot = match style {
                    PlaceholderStyle::DollarNumbered | PlaceholderStyle::ColonNumbered => ordinal,
                    PlaceholderStyle::QuestionMark => {
                        next_slot += 1;
                        next_slot
                    }
                };
                max_slot = max_slot.max(slot);
                push_placeholder(&mut out, style, slot);
                i = end;
                continue;
            }
        }

        if ch == b':' && i + 1 < len {
            let next = bytes[i + 1];
            if next == b':' || next == b'=' {
                out.push(':');
                out.push(next as char);
                i += 2;
                continue;
            }
            if next.is_ascii_digit() && next != b'0' {
                let start = i + 1;
                let mut end = start;
                while end < len && bytes[end].is_ascii_digit() {
                    end += 1;
                }
                if let Ok(ordinal) = sql[start..end].parse::<usize>()
                    && ordinal >= 1
                {
                    let slot = match style {
                        PlaceholderStyle::DollarNumbered | PlaceholderStyle::ColonNumbered => {
                            ordinal
                        }
                        PlaceholderStyle::QuestionMark => {
                            next_slot += 1;
                            next_slot
                        }
                    };
                    max_slot = max_slot.max(slot);
                    push_placeholder(&mut out, style, slot);
                    i = end;
                    continue;
                }
            }
            if next.is_ascii_alphabetic() || next == b'_' {
                let start = i + 1;
                let mut end = start;
                while end < len && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
                    end += 1;
                }
                let name = &sql[start..end];
                let slot = match style {
                    PlaceholderStyle::QuestionMark => {
                        next_slot += 1;
                        next_slot
                    }
                    PlaceholderStyle::DollarNumbered | PlaceholderStyle::ColonNumbered => {
                        *named_slots.entry(name.to_owned()).or_insert_with(|| {
                            next_slot += 1;
                            next_slot
                        })
                    }
                };
                max_slot = max_slot.max(slot);
                push_placeholder(&mut out, style, slot);
                i = end;
                continue;
            }
        }

        let ch_utf8 = sql[i..].chars().next().unwrap_or('\0');
        out.push(ch_utf8);
        i += ch_utf8.len_utf8();
    }

    (out, max_slot)
}
