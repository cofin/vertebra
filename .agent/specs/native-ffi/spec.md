# Chapter 6: Native FFI & Batch Runner

**Beads ID:** vertebra-w4i.6
**Flow ID:** native-ffi
**Status:** Active
**Depends on:** Ch4 (native-ipc / `ipc-substrate_20260314`), Ch5 (native-http / `http-server-h2_20260314`)
**Consolidates:** `pyo3-request-proxy_20260314`, `vectorized-ffi-api_20260314`, `python-batch-runner_20260314`, `native-core_20260314` Phase 3

---

## Mission

This chapter implements the FFI bridge that connects Rust's HTTP server to Python's business logic. It is the most performance-critical layer in the entire Ghost Framework rewrite. Every decision here has a direct numerical consequence on requests-per-second.

The core insight from the research is that existing "native" implementations (including Litecore's raw batch transport) still leave performance on the table because they:

1. Create one Python object per request in `build_batch_replies()`.
2. Copy request fields (path, method, headers) into owned Rust strings before handing to Python.
3. Cross the FFI boundary once per request rather than once per batch.
4. Re-create response header vectors per response in `build_response_from_py()`.

The Ghost Framework approach eliminates all four of these costs.

---

## Architecture Overview

```
HTTP Request
     |
     v
[Rust: vertebra-http]
  - Hyper accept loop (Ch5)
  - matchit router -> handler_id (Ch5)
  - BatchCollector: groups requests into BatchArena (Ch5)
     |
     | MPMC ring push (one slot per batch)
     v
[Rust: vertebra-core BatchArena in shared memory]
  - RequestInfo structs laid out sequentially in arena
  - Each RequestInfo: method ptr+len, path ptr+len, body ptr+len,
    header count, header ptr array, handler_id (u64)
     |
     | get_batch() FFI call (ONE crossing for N requests)
     v
[Python: vertebra._vertebra.get_batch()]
  - Drains ring up to batch_size=64
  - Returns Vec<VertebraRequest> as Python list
  - Each VertebraRequest: holds *const RequestInfo, does NOT copy
     |
     v
[Python: runner.py run_worker() loop]
  - O(1) handler lookup: app._handlers[request.handler_id]
  - Synchronous dispatch (no asyncio unless handler is async def)
  - Collects VertebraResponse objects
     |
     | submit_results() FFI call (ONE crossing for N responses)
     v
[Rust: write responses to BatchArena, signal Hyper futures]
  - Each Hyper future has a oneshot channel
  - submit_results() writes status/body into arena slots
  - Signals the waiting Hyper response future
     |
     v
HTTP Response
```

### Why This Beats WSGI's 200k Ceiling

WSGI at 200k req/s (Litecore's record) still pays:
- 1 `app(environ, start_response)` call per request.
- `WsgiEnviron` lazy-map construction per request.
- 1 Python dict or mapping object per request.

The Ghost Framework batch runner pays:
- 1 `get_batch()` FFI call per **N requests** (N up to 64).
- 0 Python objects created until a field is actually read.
- 0 string copies for method/path until `.method` or `.path` is accessed.

At batch_size=64 and ~1.5µs FFI overhead, the amortized FFI cost per request is ~23ns instead of 1500ns. That headroom is where 300k+ req/s lives.

---

## Prerequisite Types (from Ch4 / vertebra-core)

The following types from `vertebra-core` are assumed to exist after Ch4 completes. This chapter **reads** them but does not **define** them.

```
vertebra_core::batch::{BatchArena, BatchNode, RequestInfo, ResponseSlot}
vertebra_core::channel::MpmcRing  (already exists in current codebase)
```

`RequestInfo` C-ABI layout (defined in Ch4's `proxy.rs`):

```rust
#[repr(C)]
pub struct RequestInfo {
    pub request_id: u64,       // Unique ID for this request (Hyper future key)
    pub handler_id: u64,       // Route handler ID from matchit dispatch
    pub method_ptr: *const u8, // Pointer into arena, NOT null-terminated
    pub method_len: u32,
    pub path_ptr: *const u8,
    pub path_len: u32,
    pub query_ptr: *const u8,  // NULL if no query string
    pub query_len: u32,
    pub body_ptr: *const u8,   // NULL if no body
    pub body_len: u32,
    pub header_count: u32,
    pub headers_ptr: *const HeaderPair, // Array of HeaderPair in arena
}

#[repr(C)]
pub struct HeaderPair {
    pub name_ptr: *const u8,
    pub name_len: u32,
    pub value_ptr: *const u8,
    pub value_len: u32,
}

#[repr(C)]
pub struct ResponseSlot {
    pub request_id: u64,
    pub status: u16,
    pub body_ptr: *const u8,
    pub body_len: u32,
    pub header_count: u32,
    pub headers_ptr: *const HeaderPair,
}
```

**Important:** This chapter's `VertebraRequest` holds a raw pointer to `RequestInfo`. The arena that backs this memory is owned by the `BatchNode` which is also held in Rust for the duration of the batch. Python must not outlive the batch. The runner loop's structure enforces this: `submit_results()` is always called before the next `get_batch()`, which drops the previous arena allocation.

---

## Phase 1: Zero-Object Request Proxy (Rust)

### 1.1 `src/rs/vertebra-py/src/request.rs`

The `VertebraRequest` class is the central performance innovation. It is a PyO3 class that stores a raw pointer into the batch arena. No Python string, bytes, or dict is created at construction. Fields are materialized on first access.

```rust
//! Zero-object request proxy for the Vertebra batch runner.
//!
//! `VertebraRequest` holds a pointer into a `BatchArena` slot. All field
//! access is lazy: the Rust -> Python string conversion only happens when
//! Python code actually reads the field. For requests where the handler
//! only uses `path` and `handler_id`, the method/body/headers are never
//! converted.
//!
//! # Safety
//! The pointer is valid for the lifetime of the enclosing `BatchNode`.
//! The Python batch runner loop guarantees the arena outlives all proxies
//! in the batch by only calling `submit_results()` after all handlers return.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString};

use vertebra_core::batch::{HeaderPair, RequestInfo};

use crate::headers::VertebraHeaders;

/// Zero-object proxy for a single HTTP request inside a batch arena.
///
/// Python usage:
/// ```python
/// for req in batch:
///     print(req.method)     # "GET" - lazily decoded here
///     print(req.path)       # "/api/users" - lazily decoded here
///     handler_id = req.handler_id  # u64, no conversion needed
/// ```
#[pyclass(name = "VertebraRequest")]
pub struct VertebraRequest {
    /// Raw pointer into the `BatchArena`. Valid for the lifetime of the
    /// enclosing `BatchNode`. The runner loop must not call `get_batch()`
    /// again until `submit_results()` has been called, which drops the arena.
    ptr: *const RequestInfo,
}

// SAFETY: The pointer is read-only and points into a `BatchArena` that is
// pinned in shared memory for the duration of the batch cycle. The runner
// loop serializes access: all Python handlers run, then submit_results()
// is called, then the next get_batch() call can overwrite the arena.
unsafe impl Send for VertebraRequest {}

impl VertebraRequest {
    /// Construct from a raw pointer. Called only from `api.rs::get_batch()`.
    ///
    /// # Safety
    /// `ptr` must point to a valid `RequestInfo` in an active `BatchArena`.
    pub unsafe fn from_ptr(ptr: *const RequestInfo) -> Self {
        Self { ptr }
    }

    #[inline(always)]
    fn info(&self) -> &RequestInfo {
        // SAFETY: ptr is valid for the batch duration (see struct-level safety note).
        unsafe { &*self.ptr }
    }
}

#[pymethods]
impl VertebraRequest {
    /// HTTP method as a Python str. Lazily decoded from arena bytes.
    ///
    /// This is the ONLY point where a Python string object is created for
    /// the method. If the handler never reads `.method`, no allocation occurs.
    #[getter]
    fn method<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        let info = self.info();
        // SAFETY: method_ptr points into arena; len is valid.
        let bytes = unsafe {
            std::slice::from_raw_parts(info.method_ptr, info.method_len as usize)
        };
        // Methods are always valid ASCII. intern() reuses the string object
        // for repeated identical methods (GET, POST, etc.) -- zero extra alloc
        // for the common case.
        PyString::intern(py, std::str::from_utf8(bytes).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid method bytes: {e}"))
        })?)
        .map(|s| s.clone().into_bound(py))
    }

    /// Request path as a Python str. Lazily decoded from arena bytes.
    #[getter]
    fn path<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        let info = self.info();
        let bytes = unsafe {
            std::slice::from_raw_parts(info.path_ptr, info.path_len as usize)
        };
        PyString::new(py, std::str::from_utf8(bytes).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid path bytes: {e}"))
        })?)
        .map(|s| Ok(s))
        .unwrap()
    }

    /// Query string as a Python str, or None if absent.
    #[getter]
    fn query<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyString>>> {
        let info = self.info();
        if info.query_ptr.is_null() || info.query_len == 0 {
            return Ok(None);
        }
        let bytes = unsafe {
            std::slice::from_raw_parts(info.query_ptr, info.query_len as usize)
        };
        Ok(Some(PyString::new(py, std::str::from_utf8(bytes).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid query bytes: {e}"))
        })?)?))
    }

    /// Request body as Python bytes, or None if absent.
    ///
    /// Returns a zero-copy `PyBytes` backed by the arena slice.
    /// No copy occurs here — Python `bytes` in CPython uses a direct
    /// reference to the provided memory when created with `PyBytes::new`.
    #[getter]
    fn body<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let info = self.info();
        if info.body_ptr.is_null() || info.body_len == 0 {
            return Ok(None);
        }
        let bytes = unsafe {
            std::slice::from_raw_parts(info.body_ptr, info.body_len as usize)
        };
        Ok(Some(PyBytes::new(py, bytes)?))
    }

    /// Handler ID as a plain Python int (u64). No conversion overhead.
    ///
    /// This is the primary dispatch key. The Python app maps
    /// `handler_id -> callable` using a plain dict. O(1), no string hashing.
    #[getter]
    fn handler_id(&self) -> u64 {
        self.info().handler_id
    }

    /// Request ID as a Python int (u64). Used for correlating responses.
    #[getter]
    fn request_id(&self) -> u64 {
        self.info().request_id
    }

    /// Lazy headers accessor. Returns a `VertebraHeaders` proxy.
    ///
    /// The proxy also holds a raw pointer into the arena header array.
    /// No `dict` is created unless Python code iterates or calls `.items()`.
    #[getter]
    fn headers<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, VertebraHeaders>> {
        let info = self.info();
        Py::new(py, unsafe {
            VertebraHeaders::from_ptr(info.headers_ptr, info.header_count as usize)
        })
        .map(|h| h.into_bound(py))
    }

    /// Materialize all fields into a Python dict. Escape hatch for
    /// frameworks that need a full request object. Prefer field access.
    fn as_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new(py);
        d.set_item("method", self.method(py)?)?;
        d.set_item("path", self.path(py)?)?;
        d.set_item("query", self.query(py)?)?;
        d.set_item("body", self.body(py)?)?;
        d.set_item("handler_id", self.handler_id())?;
        d.set_item("request_id", self.request_id())?;
        Ok(d)
    }

    fn __repr__(&self) -> String {
        let info = self.info();
        format!(
            "VertebraRequest(handler_id={}, request_id={})",
            info.handler_id,
            info.request_id,
        )
    }
}
```

**Pass criteria:**
- `VertebraRequest.__new__` is not callable from Python (no `#[new]`, constructed only in Rust).
- Accessing `.method` on a `VertebraRequest` returns a Python `str` without creating any intermediate Python objects.
- Accessing `.handler_id` returns a `int` with no string conversion.
- `PyString::intern()` is used for method to avoid re-allocating `"GET"` objects across the batch.
- A request where only `.handler_id` is read allocates **zero** Python strings.

**Fail criteria:**
- Any Python `dict` created in the hot path before a field is accessed.
- Any `String::from_utf8` called at construction time (must be lazy).
- `VertebraRequest` constructable from Python (it wraps a raw pointer; Python must not hold it beyond the batch lifetime).

---

### 1.2 `src/rs/vertebra-py/src/headers.rs`

```rust
//! Lazy headers proxy for `VertebraRequest`.
//!
//! `VertebraHeaders` exposes HTTP headers as a dict-like Python object
//! without building a `dict` unless iteration is forced. The underlying
//! data lives in the batch arena.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyIterator, PyList, PyString, PyTuple};

use vertebra_core::batch::HeaderPair;

/// Lazy header proxy. Supports `__getitem__`, `__contains__`, `__iter__`,
/// `.get()`, `.items()`, and `.keys()`. All operations iterate the arena
/// `HeaderPair` array directly — no dict is allocated unless `.items()` or
/// `.keys()` forces full materialization.
#[pyclass(name = "VertebraHeaders")]
pub struct VertebraHeaders {
    /// Pointer to the first `HeaderPair` in the arena.
    ptr: *const HeaderPair,
    /// Number of headers.
    count: usize,
}

// SAFETY: Same arena lifetime guarantee as VertebraRequest.
unsafe impl Send for VertebraHeaders {}

impl VertebraHeaders {
    pub unsafe fn from_ptr(ptr: *const HeaderPair, count: usize) -> Self {
        Self { ptr, count }
    }

    fn pairs(&self) -> &[HeaderPair] {
        if self.ptr.is_null() || self.count == 0 {
            return &[];
        }
        // SAFETY: ptr is valid for arena lifetime.
        unsafe { std::slice::from_raw_parts(self.ptr, self.count) }
    }

    fn decode_pair(pair: &HeaderPair) -> (Option<&str>, Option<&str>) {
        // SAFETY: Both ptrs are valid for arena lifetime.
        unsafe {
            let name = if pair.name_ptr.is_null() {
                None
            } else {
                std::str::from_utf8(std::slice::from_raw_parts(
                    pair.name_ptr,
                    pair.name_len as usize,
                ))
                .ok()
            };
            let value = if pair.value_ptr.is_null() {
                None
            } else {
                std::str::from_utf8(std::slice::from_raw_parts(
                    pair.value_ptr,
                    pair.value_len as usize,
                ))
                .ok()
            };
            (name, value)
        }
    }
}

#[pymethods]
impl VertebraHeaders {
    /// Case-insensitive header lookup. Returns the first matching value.
    fn __getitem__<'py>(&self, py: Python<'py>, key: &str) -> PyResult<Bound<'py, PyString>> {
        let key_lower = key.to_ascii_lowercase();
        for pair in self.pairs() {
            let (name, value) = Self::decode_pair(pair);
            if let (Some(n), Some(v)) = (name, value) {
                if n.eq_ignore_ascii_case(&key_lower) {
                    return Ok(PyString::new(py, v)?);
                }
            }
        }
        Err(pyo3::exceptions::PyKeyError::new_err(key.to_string()))
    }

    /// Case-insensitive header presence check.
    fn __contains__(&self, key: &str) -> bool {
        let key_lower = key.to_ascii_lowercase();
        self.pairs().iter().any(|pair| {
            let (name, _) = Self::decode_pair(pair);
            name.map_or(false, |n| n.eq_ignore_ascii_case(&key_lower))
        })
    }

    fn __len__(&self) -> usize {
        self.count
    }

    /// Get with optional default.
    #[pyo3(signature = (key, default=None))]
    fn get<'py>(
        &self,
        py: Python<'py>,
        key: &str,
        default: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        match self.__getitem__(py, key) {
            Ok(v) => Ok(v.into_any()),
            Err(_) => Ok(default.unwrap_or_else(|| py.None().into_bound(py))),
        }
    }

    /// Materialize all header names as a Python list.
    fn keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty(py);
        for pair in self.pairs() {
            let (name, _) = Self::decode_pair(pair);
            if let Some(n) = name {
                list.append(PyString::new(py, n)?)?;
            }
        }
        Ok(list)
    }

    /// Materialize all header values as a Python list.
    fn values<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty(py);
        for pair in self.pairs() {
            let (_, value) = Self::decode_pair(pair);
            if let Some(v) = value {
                list.append(PyString::new(py, v)?)?;
            }
        }
        Ok(list)
    }

    /// Materialize all headers as (name, value) tuples.
    fn items<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty(py);
        for pair in self.pairs() {
            let (name, value) = Self::decode_pair(pair);
            if let (Some(n), Some(v)) = (name, value) {
                let t = PyTuple::new(py, [
                    PyString::new(py, n)?.into_any(),
                    PyString::new(py, v)?.into_any(),
                ])?;
                list.append(t)?;
            }
        }
        Ok(list)
    }

    /// Materialize as a Python dict. Escape hatch; prefer direct access.
    fn as_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new(py);
        for pair in self.pairs() {
            let (name, value) = Self::decode_pair(pair);
            if let (Some(n), Some(v)) = (name, value) {
                d.set_item(n, v)?;
            }
        }
        Ok(d)
    }

    fn __repr__(&self) -> String {
        format!("VertebraHeaders(count={})", self.count)
    }
}
```

**Pass criteria:**
- `headers["content-type"]` works with both `"content-type"` and `"Content-Type"` (case-insensitive).
- `"x-request-id" in headers` works without materializing a dict.
- `len(headers)` returns the header count.
- `headers.items()` materializes all headers on demand.
- `VertebraHeaders` constructed only from `VertebraRequest.headers` getter.

---

### 1.3 `src/rs/vertebra-py/src/response.rs`

`VertebraResponse` is the output side of the batch. Python handlers construct one and return it. Rust reads it in `submit_results()` to write back into the batch arena.

```rust
//! Response builder for the Vertebra batch runner.
//!
//! `VertebraResponse` is the value Python handlers return. It is a plain
//! Python-constructable class. Rust reads its fields in `submit_results()`
//! to write the HTTP response back to the waiting Hyper future.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

/// Response built by a Python handler.
///
/// Python usage:
/// ```python
/// return VertebraResponse(status=200, body=b"hello", headers={"content-type": "text/plain"})
/// ```
///
/// All fields are eagerly stored because Python owns the lifetime here.
/// The response is written into Rust's `ResponseSlot` inside `submit_results()`.
#[pyclass(name = "VertebraResponse")]
pub struct VertebraResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub headers: Vec<(String, String)>,
    pub request_id: u64,
}

#[pymethods]
impl VertebraResponse {
    /// Construct a response.
    ///
    /// Args:
    ///     status: HTTP status code (e.g. 200, 404).
    ///     body: Response body bytes. Defaults to empty.
    ///     headers: Dict of response headers. Defaults to empty.
    ///     request_id: Must match the `VertebraRequest.request_id` this
    ///                 responds to. The runner sets this automatically.
    #[new]
    #[pyo3(signature = (status, body=None, headers=None, request_id=0))]
    fn new(
        status: u16,
        body: Option<&[u8]>,
        headers: Option<Bound<'_, PyDict>>,
        request_id: u64,
    ) -> PyResult<Self> {
        let body = body.unwrap_or(&[]).to_vec();
        let mut header_vec: Vec<(String, String)> = Vec::new();
        if let Some(hdict) = headers {
            for (k, v) in hdict.iter() {
                let key = k.extract::<String>()?;
                let val = v.extract::<String>()?;
                header_vec.push((key, val));
            }
        }
        Ok(Self {
            status,
            body,
            headers: header_vec,
            request_id,
        })
    }

    #[getter]
    fn status(&self) -> u16 {
        self.status
    }

    #[getter]
    fn body<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        PyBytes::new(py, &self.body)
    }

    #[getter]
    fn request_id(&self) -> u64 {
        self.request_id
    }

    fn __repr__(&self) -> String {
        format!(
            "VertebraResponse(status={}, body_len={}, request_id={})",
            self.status,
            self.body.len(),
            self.request_id,
        )
    }
}
```

**Pass criteria:**
- `VertebraResponse(status=200, body=b"ok")` constructs without error.
- `VertebraResponse(status=404)` constructs with empty body.
- `VertebraResponse(status=200, body=b"data", headers={"content-type": "text/plain"})` stores headers.
- `request_id` defaults to 0 and is overridden by the runner loop.

---

## Phase 2: Vectorized Batch API (Rust)

### 2.1 `src/rs/vertebra-py/src/api.rs`

This module contains the two public PyO3 functions that the Python runner calls. Each function crosses the FFI boundary exactly once, regardless of batch size.

```rust
//! Vectorized batch FFI API.
//!
//! These are the only two functions Python needs to call to process
//! requests. The entire performance benefit of the Ghost Framework
//! flows through these two call sites.
//!
//! - `get_batch(timeout_ms)`: One FFI crossing returns up to 64 requests.
//! - `submit_results(responses)`: One FFI crossing submits all N responses.

use pyo3::prelude::*;
use pyo3::types::PyList;

use std::sync::OnceLock;
use std::time::Duration;

use vertebra_core::batch::{BatchHandle, BatchNode, GlobalBatchState};
use vertebra_core::channel::MpmcRing;

use crate::response::VertebraResponse;
use crate::request::VertebraRequest;

/// Global state shared between Rust server and Python worker in this process.
/// Initialized once by `init_batch_channel()` called from the worker entrypoint.
static BATCH_STATE: OnceLock<GlobalBatchState> = OnceLock::new();

/// Initialize the batch channel. Called once during worker startup.
///
/// Args:
///     ring_name: Named MPMC ring created by the Rust supervisor.
///     slot_size: Ring slot size (must match supervisor config).
///     slots: Ring slot count (must match supervisor config).
///     batch_size: Maximum requests per batch (default 64).
#[pyfunction]
#[pyo3(signature = (ring_name, slot_size, slots, batch_size=64))]
pub fn init_batch_channel(
    ring_name: String,
    slot_size: usize,
    slots: usize,
    batch_size: usize,
) -> PyResult<()> {
    let ring = MpmcRing::open_named(&ring_name, slot_size, slots)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    let state = GlobalBatchState::new(ring, batch_size);
    BATCH_STATE
        .set(state)
        .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("batch channel already initialized"))
}

/// Fetch the next batch of requests from the Rust HTTP server.
///
/// Blocks for up to `timeout_ms` milliseconds waiting for at least one
/// request. Returns immediately if requests are already queued.
///
/// This is the most important function in the entire framework. It crosses
/// the FFI boundary **once** and returns a list of up to `batch_size` request
/// proxy objects. Each proxy object holds a raw pointer into the batch arena;
/// no data is copied.
///
/// Returns:
///     A Python list of `VertebraRequest` objects. Empty list on timeout.
///
/// The GIL is released during the blocking wait. Python code resumes
/// (with GIL re-acquired) only when requests are available or timeout fires.
#[pyfunction]
#[pyo3(signature = (timeout_ms=100))]
pub fn get_batch(py: Python<'_>, timeout_ms: u64) -> PyResult<Py<PyList>> {
    let state = BATCH_STATE.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "batch channel not initialized; call init_batch_channel() first",
        )
    })?;

    // Release GIL during the blocking ring poll. Other Python threads can run.
    let batch_node: Option<BatchNode> = py.detach(|| {
        state
            .poll_batch(Duration::from_millis(timeout_ms))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    })?;

    let list = PyList::empty(py);

    let batch_node = match batch_node {
        None => return Ok(list.unbind()),
        Some(node) => node,
    };

    // Build VertebraRequest proxies. Each one holds a *const RequestInfo.
    // No data is copied here. The arena backing these pointers is owned by
    // `batch_node` which is stored in thread-local state for the duration
    // of the batch cycle, released by `submit_results()`.
    for request_info_ptr in batch_node.request_ptrs() {
        let proxy = unsafe { VertebraRequest::from_ptr(request_info_ptr) };
        let py_proxy = Py::new(py, proxy)?;
        list.append(py_proxy)?;
    }

    // Store the BatchNode so it stays alive until submit_results() is called.
    // This is what keeps the arena memory valid while Python holds proxies.
    state.set_active_batch(batch_node);

    Ok(list.unbind())
}

/// Submit processed responses back to the Rust HTTP server.
///
/// After calling this function, the batch arena is released and all
/// `VertebraRequest` proxy objects in the previous batch become invalid.
/// Python must not access any `VertebraRequest` after calling this.
///
/// Args:
///     responses: A list of `VertebraResponse` objects, one per request
///                in the batch. Order does not matter; matching is by
///                `request_id`.
///
/// This function crosses the FFI boundary once for all N responses.
#[pyfunction]
pub fn submit_results(py: Python<'_>, responses: Vec<Py<VertebraResponse>>) -> PyResult<()> {
    let state = BATCH_STATE.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "batch channel not initialized; call init_batch_channel() first",
        )
    })?;

    // Collect response data while holding GIL (Python objects need GIL).
    let mut response_data: Vec<(u64, u16, Vec<u8>, Vec<(String, String)>)> =
        Vec::with_capacity(responses.len());

    for py_resp in &responses {
        let resp = py_resp.borrow(py);
        response_data.push((
            resp.request_id,
            resp.status,
            resp.body.clone(),
            resp.headers.clone(),
        ));
    }

    // Release GIL while writing responses to the arena and signaling Hyper futures.
    py.detach(|| {
        state
            .submit_responses(response_data)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    })
}
```

**Pass criteria:**
- `get_batch(timeout_ms=100)` with no pending requests returns an empty list after ~100ms.
- `get_batch(timeout_ms=100)` with 64 pending requests returns a list of 64 `VertebraRequest` objects in a single call.
- GIL is released during the blocking poll (verified via threading test in `test_batch_api.rs`).
- `submit_results([])` on an empty batch is a no-op.
- `submit_results(responses)` returns before `get_batch()` can be called again (arena safety).

**Fail criteria:**
- `get_batch()` called while a previous batch is still active (double-batch violation).
- Any `VertebraRequest` accessed after `submit_results()` (use-after-free; caught by arena tracking in debug builds).
- GIL held during the ring poll (would block all other Python threads).

---

### 2.2 Updated `src/rs/vertebra-py/src/lib.rs`

The module registration must be extended to include the new classes and functions. The existing classes (`Router`, `RateLimiter`, `ShmRegion`, etc.) are **retained**. New items are added.

```rust
// In the _vertebra pymodule function, ADD these registrations:
m.add_class::<crate::request::VertebraRequest>()?;
m.add_class::<crate::headers::VertebraHeaders>()?;
m.add_class::<crate::response::VertebraResponse>()?;
m.add_function(wrap_pyfunction!(crate::api::init_batch_channel, m)?)?;
m.add_function(wrap_pyfunction!(crate::api::get_batch, m)?)?;
m.add_function(wrap_pyfunction!(crate::api::submit_results, m)?)?;

// Also add new module-level declarations in lib.rs:
mod request;
mod headers;
mod response;
mod api;
```

The full `lib.rs` module declarations section becomes:

```rust
mod async_ring;
mod supervisor;
mod request;
mod headers;
mod response;
mod api;
```

And the `_vertebra` pymodule function appends at the end of its existing registrations:

```rust
// Existing registrations (preserved):
m.add("__version__", vertebra_core::VERSION)?;
m.add_class::<PyRouter>()?;
m.add_class::<PyRateLimiter>()?;
m.add_class::<PyShmRegion>()?;
m.add_class::<PyBufferPool>()?;
m.add_class::<PyBuffer>()?;
m.add_class::<PySlotView>()?;
m.add_class::<PyChannel>()?;
m.add_class::<PySpscRing>()?;
m.add_class::<PyMpmcRing>()?;
m.add_class::<PyBufferChannel>()?;
m.add_class::<supervisor::NativeSupervisor>()?;
m.add_function(wrap_pyfunction!(validate_jwt_py, m)?)?;
m.add_function(wrap_pyfunction!(shm_unlink, m)?)?;
m.add_function(wrap_pyfunction!(start_http_server, m)?)?;
async_ring::register(m)?;

// New Ghost Framework FFI registrations:
m.add_class::<request::VertebraRequest>()?;
m.add_class::<headers::VertebraHeaders>()?;
m.add_class::<response::VertebraResponse>()?;
m.add_function(wrap_pyfunction!(api::init_batch_channel, m)?)?;
m.add_function(wrap_pyfunction!(api::get_batch, m)?)?;
m.add_function(wrap_pyfunction!(api::submit_results, m)?)?;
```

---

## Phase 3: Python Batch Runner

### 3.1 `src/py/vertebra/core/runner.py`

The main worker loop. This is deliberately simple and synchronous. The design philosophy: keep the hot path as flat as possible. No middleware, no event loop, no framework magic in this file.

```python
"""
Vertebra batch runner — the synchronous hot path.

This module implements the core request dispatch loop for native mode workers.
It is intentionally minimal. Every line of framework overhead here costs
requests-per-second.

Performance properties:
- O(1) handler dispatch via dict lookup (no regex, no linear scan).
- Synchronous by default. Async handlers are handled by wrapping with
  asyncio.run() in the async-capable variant (see runner_async.py, Ch8).
- No logging in the hot path. Errors write to stderr and continue.
- Batch timeout of 100ms: workers sleep cheaply when idle.
"""

from __future__ import annotations

import signal
import sys
from typing import TYPE_CHECKING

from vertebra._vertebra import (  # type: ignore[import]
    VertebraRequest,
    VertebraResponse,
    get_batch,
    init_batch_channel,
    submit_results,
)

if TYPE_CHECKING:
    from collections.abc import Callable


# Sentinel to allow clean shutdown via SIGTERM / SIGINT.
_RUNNING = True


def _install_signal_handlers() -> None:
    """Install SIGTERM and SIGINT handlers for clean batch-boundary shutdown."""

    def _handle_signal(signum: int, frame: object) -> None:  # noqa: ARG001
        global _RUNNING  # noqa: PLW0603
        _RUNNING = False

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)


def run_worker(
    handlers: dict[int, Callable[[VertebraRequest], VertebraResponse]],
    *,
    batch_timeout_ms: int = 100,
    error_handler: Callable[[Exception, VertebraRequest], VertebraResponse] | None = None,
) -> None:
    """
    Execute the synchronous batch runner loop.

    Args:
        handlers: Mapping from handler_id (int) to callable. This is
                  app._handlers — a plain dict, O(1) lookup.
        batch_timeout_ms: How long to wait for requests before looping.
                          Lower values reduce tail latency, higher values
                          reduce idle CPU. Default 100ms is a good balance.
        error_handler: Optional callable invoked when a handler raises.
                       Receives (exc, request), must return VertebraResponse.
                       If None, a 500 response is returned automatically.

    The loop runs until SIGTERM or SIGINT is received. Shutdown is always
    on a batch boundary: in-flight requests in the current batch complete
    before exit.
    """
    _install_signal_handlers()

    while _RUNNING:
        # ONE FFI crossing: retrieve up to batch_size requests.
        # GIL is released inside get_batch() while waiting on the ring.
        batch: list[VertebraRequest] = get_batch(timeout_ms=batch_timeout_ms)

        if not batch:
            # Timeout with no requests. Loop continues.
            continue

        results: list[VertebraResponse] = []

        for request in batch:
            handler_id = request.handler_id  # u64, no string conversion

            handler = handlers.get(handler_id)
            if handler is None:
                # Route registered in Rust but not in Python app — 404.
                results.append(
                    VertebraResponse(
                        status=404,
                        body=b"handler not found",
                        request_id=request.request_id,
                    )
                )
                continue

            try:
                response = handler(request)
                # Ensure the response carries the correct request_id for
                # correlation. Handlers should not need to set this manually.
                if response.request_id == 0:
                    # Build a new response with the correct ID.
                    # This copies body/headers but is the safe path.
                    response = VertebraResponse(
                        status=response.status,
                        body=bytes(response.body),
                        headers={},  # TODO: expose headers copy
                        request_id=request.request_id,
                    )
            except Exception as exc:  # noqa: BLE001
                if error_handler is not None:
                    try:
                        response = error_handler(exc, request)
                    except Exception:  # noqa: BLE001
                        response = _internal_error(request.request_id)
                else:
                    print(  # noqa: T201
                        f"[vertebra] handler error for handler_id={handler_id}: {exc}",
                        file=sys.stderr,
                    )
                    response = _internal_error(request.request_id)

            results.append(response)

        # ONE FFI crossing: submit all N responses at once.
        # GIL is released inside submit_results() while writing to arena.
        submit_results(results)


def _internal_error(request_id: int) -> VertebraResponse:
    return VertebraResponse(
        status=500,
        body=b"internal server error",
        request_id=request_id,
    )
```

**Pass criteria:**
- `run_worker(handlers={})` with no requests loops without error.
- `run_worker(handlers={1: lambda r: VertebraResponse(status=200, body=b"ok")})` dispatches correctly.
- SIGTERM causes exit on the next batch boundary (not mid-batch).
- Unknown `handler_id` returns 404 without raising.
- Handler exception returns 500 and continues the loop.
- `get_batch()` is called exactly once per loop iteration.
- `submit_results()` is called exactly once per non-empty batch.

**Fail criteria:**
- `asyncio` imported or used in this file.
- Any per-request object allocation in the hot path beyond what the handler returns.
- SIGTERM causes immediate exit mid-batch (response corruption).

---

### 3.2 `src/py/vertebra/core/worker.py`

Worker process entrypoint. Invoked by the Rust supervisor after `fork()`. Responsible for:
1. Importing the user's application.
2. Calling `init_batch_channel()` with the channel name passed via env vars.
3. Starting `run_worker()`.

```python
"""
Vertebra worker process entrypoint.

This module is the entry point for each Python worker process. It is
invoked by the Rust supervisor with environment variables that describe
the shared-memory channel to connect to.

Environment variables (set by supervisor):
    VERTEBRA_RING_NAME:   Named MPMC ring to connect to (e.g. "/vb_batch_0")
    VERTEBRA_SLOT_SIZE:   Ring slot size in bytes
    VERTEBRA_SLOTS:       Number of ring slots
    VERTEBRA_BATCH_SIZE:  Maximum batch size (default 64)
    VERTEBRA_APP:         App import path (e.g. "myapp:app")
    VERTEBRA_WORKER_ID:   Integer worker index (for logging)
"""

from __future__ import annotations

import importlib
import os
import sys
from typing import Any


def _import_app(app_path: str) -> Any:
    """
    Import an application object from a dotted path.

    Accepts "module:attr" format (e.g. "myapp.server:app").
    """
    if ":" not in app_path:
        raise ValueError(
            f"VERTEBRA_APP must be in 'module:attr' format, got: {app_path!r}"
        )
    module_path, attr = app_path.rsplit(":", 1)
    module = importlib.import_module(module_path)
    app = getattr(module, attr)
    return app


def main() -> None:
    """
    Initialize a worker process and start the batch runner loop.

    Called by the Rust supervisor after process creation. Never returns
    under normal operation; exits when SIGTERM is received.
    """
    # Read configuration from environment (set by supervisor).
    ring_name = os.environ["VERTEBRA_RING_NAME"]
    slot_size = int(os.environ["VERTEBRA_SLOT_SIZE"])
    slots = int(os.environ["VERTEBRA_SLOTS"])
    batch_size = int(os.environ.get("VERTEBRA_BATCH_SIZE", "64"))
    app_path = os.environ["VERTEBRA_APP"]
    worker_id = int(os.environ.get("VERTEBRA_WORKER_ID", "0"))

    print(  # noqa: T201
        f"[vertebra worker {worker_id}] starting, app={app_path!r}, ring={ring_name!r}",
        file=sys.stderr,
    )

    # Import the user's application.
    app = _import_app(app_path)

    # Connect to the MPMC ring created by the Rust supervisor.
    from vertebra._vertebra import init_batch_channel  # type: ignore[import]

    init_batch_channel(
        ring_name=ring_name,
        slot_size=slot_size,
        slots=slots,
        batch_size=batch_size,
    )

    print(  # noqa: T201
        f"[vertebra worker {worker_id}] connected to ring {ring_name!r}",
        file=sys.stderr,
    )

    # Build the handler dispatch table. The app exposes _handlers: dict[int, callable].
    # This is populated when routes are registered via @app.get("/path").
    handlers = app._handlers

    from vertebra.core.runner import run_worker

    run_worker(handlers, batch_timeout_ms=100)

    print(  # noqa: T201
        f"[vertebra worker {worker_id}] exiting cleanly",
        file=sys.stderr,
    )
    sys.exit(0)


if __name__ == "__main__":
    main()
```

**Pass criteria:**
- `VERTEBRA_APP=myapp:app python -m vertebra.core.worker` imports `myapp.app`.
- Missing `VERTEBRA_RING_NAME` raises `KeyError` immediately (fail fast).
- `VERTEBRA_APP=nonexistent:app` raises `ModuleNotFoundError` immediately.
- Worker logs startup and connection to stderr.
- Worker calls `sys.exit(0)` after clean shutdown.

**Fail criteria:**
- Worker silently ignores missing env vars.
- Worker does not call `init_batch_channel()` before `run_worker()`.
- Any asyncio machinery imported or started.

---

## Phase 4: Type Stubs

### 4.1 `src/py/vertebra/_vertebra.pyi`

Type stubs allow mypy, pyright, and IDEs to understand the native module. These must be kept in sync with the Rust implementation.

```python
"""
Type stubs for vertebra._vertebra — the PyO3 native extension module.

These stubs describe the public API visible to Python. They do NOT contain
implementation. The actual implementation is in src/rs/vertebra-py/.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Ghost Framework FFI — Chapter 6
# ---------------------------------------------------------------------------

class VertebraRequest:
    """
    Zero-object proxy for a single HTTP request in a batch arena.

    Do NOT construct this class from Python. Instances are returned
    exclusively by `get_batch()`.

    All property accesses are lazy: no Python string is created until
    the property is read. A handler that only reads `.handler_id` and
    `.request_id` allocates zero Python strings.
    """

    @property
    def method(self) -> str:
        """HTTP method (e.g. "GET", "POST"). Lazily decoded from arena bytes."""
        ...

    @property
    def path(self) -> str:
        """Request path (e.g. "/api/users/42"). Lazily decoded."""
        ...

    @property
    def query(self) -> str | None:
        """Query string without leading "?" (e.g. "page=1&limit=20"), or None."""
        ...

    @property
    def body(self) -> bytes | None:
        """Request body bytes, or None if absent. Zero-copy from arena."""
        ...

    @property
    def handler_id(self) -> int:
        """Route handler ID (u64). Use as key into app._handlers dict."""
        ...

    @property
    def request_id(self) -> int:
        """Unique request ID (u64). Must appear in the matching VertebraResponse."""
        ...

    @property
    def headers(self) -> VertebraHeaders:
        """Lazy headers proxy. No dict created until accessed."""
        ...

    def as_dict(self) -> dict[str, object]:
        """Materialize all fields into a Python dict. Prefer field access."""
        ...


class VertebraHeaders:
    """
    Lazy dict-like proxy for HTTP request headers.

    Backed by a pointer into the batch arena header array. No Python dict
    is created unless `.items()`, `.keys()`, or `.as_dict()` is called.

    Supports:
    - `headers["content-type"]` — case-insensitive lookup
    - `"x-api-key" in headers` — case-insensitive membership test
    - `len(headers)` — O(1) count
    - `headers.get("x-api-key", default)` — safe lookup with default
    - `headers.items()` — full materialization as list of (name, value) tuples
    - `headers.keys()` — header names as list
    - `headers.values()` — header values as list
    - `headers.as_dict()` — full dict materialization
    """

    def __getitem__(self, key: str) -> str: ...
    def __contains__(self, key: object) -> bool: ...
    def __len__(self) -> int: ...
    def get(self, key: str, default: str | None = None) -> str | None: ...
    def keys(self) -> list[str]: ...
    def values(self) -> list[str]: ...
    def items(self) -> list[tuple[str, str]]: ...
    def as_dict(self) -> dict[str, str]: ...


class VertebraResponse:
    """
    Response builder for Python handlers.

    Construct one per request and return it from the handler. The runner
    will call `submit_results()` with all responses in the batch.
    """

    def __init__(
        self,
        status: int,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
        request_id: int = 0,
    ) -> None: ...

    @property
    def status(self) -> int: ...
    @property
    def body(self) -> bytes: ...
    @property
    def request_id(self) -> int: ...


def init_batch_channel(
    ring_name: str,
    slot_size: int,
    slots: int,
    batch_size: int = 64,
) -> None:
    """
    Connect this worker process to the named MPMC ring.

    Must be called once before `get_batch()` or `submit_results()`.
    Raises `RuntimeError` if called more than once in a process.
    """
    ...


def get_batch(timeout_ms: int = 100) -> list[VertebraRequest]:
    """
    Retrieve the next batch of requests from the Rust HTTP server.

    Blocks (releasing the GIL) for up to `timeout_ms` milliseconds
    waiting for at least one request.

    Returns a list of `VertebraRequest` proxy objects. Returns an empty
    list on timeout. Each proxy holds a raw pointer into the batch arena;
    do NOT hold references to these objects after calling `submit_results()`.

    This function crosses the FFI boundary exactly ONCE per call, regardless
    of how many requests are in the returned batch.
    """
    ...


def submit_results(responses: list[VertebraResponse]) -> None:
    """
    Submit all processed responses back to the Rust HTTP server.

    After this call returns, the batch arena is released and all
    `VertebraRequest` objects from the previous `get_batch()` call are
    invalid. Do NOT access any `VertebraRequest` after calling this.

    This function crosses the FFI boundary exactly ONCE per call, regardless
    of how many responses are submitted.
    """
    ...


# ---------------------------------------------------------------------------
# Pre-existing native bindings (preserved from original _vertebra.pyi)
# ---------------------------------------------------------------------------

class Router:
    def __init__(self) -> None: ...
    def insert(self, path: str, id: int) -> None: ...
    def match_path(self, path: str) -> int | None: ...

class RateLimiter:
    def __init__(self, max_tokens: float, refill_per_second: float) -> None: ...
    def allow(self, key: str) -> bool: ...

def validate_jwt(
    token: str,
    secret: str,
    issuer: str | None = None,
    audience: str | None = None,
) -> bool: ...

def shm_unlink(name: str) -> None: ...

def start_http_server(
    host: str,
    port: int,
    app_path: str,
    python_path: str,
    workers: int,
) -> None: ...

class ShmRegion:
    def __init__(self, size: int) -> None: ...
    @classmethod
    def create_named(cls, name: str, size: int) -> ShmRegion: ...
    @classmethod
    def open_named(cls, name: str, size: int) -> ShmRegion: ...
    def len(self) -> int: ...
    def read(self, offset: int, len: int) -> bytes: ...
    def write(self, offset: int, data: bytes) -> None: ...
    def as_memoryview(self) -> memoryview: ...
    def close(self) -> None: ...
    def __enter__(self) -> ShmRegion: ...
    def __exit__(self, *args: object) -> bool: ...

class SpscRing:
    def __init__(self, slot_size: int, slots: int) -> None: ...
    def send(self, data: bytes, flags: int) -> None: ...
    def try_send(self, data: bytes, flags: int) -> None: ...
    def recv(self, max_len: int) -> bytes: ...
    def try_recv(self, max_len: int) -> bytes | None: ...
    def stats(self) -> dict[str, int]: ...

class MpmcRing:
    def __init__(self, slot_size: int, slots: int) -> None: ...
    @classmethod
    def create_named(cls, name: str, slot_size: int, slots: int) -> MpmcRing: ...
    @classmethod
    def open_named(cls, name: str, slot_size: int, slots: int) -> MpmcRing: ...
    def send(self, data: bytes, flags: int) -> None: ...
    def recv(self, max_len: int) -> bytes: ...

class AsyncRing:
    def __init__(self, slot_size: int, slots: int) -> None: ...
    def notifier_fd(self) -> int: ...
    def send(self, data: bytes, flags: int | None = None) -> None: ...
    def try_send(self, data: bytes, flags: int | None = None) -> bool: ...
    def try_recv(self) -> bytes | None: ...
    def recv_hybrid(self, spin_us: int = 5, timeout_ms: int = 1000) -> bytes | None: ...
    def clear_notification(self) -> None: ...
    def has_pending_notification(self) -> bool: ...
    def try_recv_batch(self, max: int = 64) -> list[tuple[bytes, int]]: ...
    def stats(self) -> dict[str, int]: ...
```

---

## Tests

### 5.1 Rust Test: `src/rs/vertebra-py/tests/test_request_proxy.rs`

```rust
//! Tests for VertebraRequest lazy field access.
//!
//! These tests verify that the proxy mechanism works correctly and that
//! no fields are materialized until accessed.

#[cfg(test)]
mod tests {
    use vertebra_core::batch::{BatchArena, HeaderPair, RequestInfo};

    /// Helper to create a minimal RequestInfo in a test arena.
    fn make_request_info(
        arena: &mut BatchArena,
        method: &str,
        path: &str,
        handler_id: u64,
        request_id: u64,
    ) -> *const RequestInfo {
        arena.push_request(
            request_id,
            handler_id,
            method.as_bytes(),
            path.as_bytes(),
            None, // query
            None, // body
            &[],  // headers
        )
    }

    #[test]
    fn test_request_info_field_access() {
        let mut arena = BatchArena::new(4096);
        let ptr = make_request_info(&mut arena, "GET", "/api/health", 42, 1001);

        // SAFETY: ptr is valid for the lifetime of arena.
        let info = unsafe { &*ptr };

        assert_eq!(info.handler_id, 42);
        assert_eq!(info.request_id, 1001);

        // Verify method bytes.
        let method = unsafe {
            std::str::from_utf8(
                std::slice::from_raw_parts(info.method_ptr, info.method_len as usize)
            ).unwrap()
        };
        assert_eq!(method, "GET");

        // Verify path bytes.
        let path = unsafe {
            std::str::from_utf8(
                std::slice::from_raw_parts(info.path_ptr, info.path_len as usize)
            ).unwrap()
        };
        assert_eq!(path, "/api/health");

        // Body should be null (None passed).
        assert!(info.body_ptr.is_null());
        assert_eq!(info.body_len, 0);

        // No headers.
        assert_eq!(info.header_count, 0);
    }

    #[test]
    fn test_request_with_body_and_headers() {
        let mut arena = BatchArena::new(8192);
        let headers = vec![
            ("content-type", "application/json"),
            ("x-api-key", "secret123"),
        ];
        let ptr = arena.push_request(
            9999,  // request_id
            7,     // handler_id
            b"POST",
            b"/api/users",
            None,  // query
            Some(b"{\"name\":\"alice\"}"),
            &headers,
        );

        let info = unsafe { &*ptr };

        assert_eq!(info.handler_id, 7);
        assert!(!info.body_ptr.is_null());
        assert_eq!(info.body_len, 16); // len of b"{\"name\":\"alice\"}"
        assert_eq!(info.header_count, 2);

        let header_pairs = unsafe {
            std::slice::from_raw_parts(info.headers_ptr, info.header_count as usize)
        };

        let name = unsafe {
            std::str::from_utf8(
                std::slice::from_raw_parts(
                    header_pairs[0].name_ptr,
                    header_pairs[0].name_len as usize,
                )
            ).unwrap()
        };
        assert_eq!(name, "content-type");
    }

    #[test]
    fn test_multiple_requests_in_arena() {
        let mut arena = BatchArena::new(16384);

        let ptrs: Vec<*const RequestInfo> = (0..64).map(|i| {
            make_request_info(
                &mut arena,
                "GET",
                &format!("/route/{i}"),
                i as u64,
                i as u64 + 1000,
            )
        }).collect();

        // All pointers should be valid and distinct.
        for (i, ptr) in ptrs.iter().enumerate() {
            let info = unsafe { &**ptr };
            assert_eq!(info.handler_id, i as u64);
            assert_eq!(info.request_id, i as u64 + 1000);
        }
    }
}
```

**Pass criteria:**
- All three tests pass with `make rs-test`.
- Arena allocates contiguously — no heap fragmentation between requests in the same batch.
- 64-request arena fits in a single 16KB allocation.

---

### 5.2 Rust Test: `src/rs/vertebra-py/tests/test_batch_api.rs`

```rust
//! Integration tests for get_batch / submit_results round-trip.
//!
//! These tests verify the full batch API contract without involving Python.
//! They test the Rust side of the pipeline.

#[cfg(test)]
mod tests {
    use vertebra_core::batch::{BatchArena, GlobalBatchState};
    use vertebra_core::channel::MpmcRing;
    use std::thread;
    use std::time::Duration;

    /// Test that polling an empty ring returns None before timeout.
    #[test]
    fn test_poll_empty_ring_returns_none() {
        let ring = MpmcRing::new(4096, 64).expect("ring creation");
        let state = GlobalBatchState::new(ring, 64);

        // Poll for 50ms; should return None immediately (nothing pushed).
        let result = state.poll_batch(Duration::from_millis(50));
        assert!(result.unwrap().is_none());
    }

    /// Test that pushing a batch node makes it retrievable.
    #[test]
    fn test_poll_retrieves_pushed_batch() {
        let ring = MpmcRing::new(65536, 64).expect("ring creation");
        let state = GlobalBatchState::new(ring, 64);
        let state_producer = state.clone_producer();

        // Producer thread: push a batch with 10 requests.
        let producer = thread::spawn(move || {
            let mut arena = BatchArena::new(65536);
            for i in 0u64..10 {
                arena.push_request(
                    i,     // request_id
                    i * 2, // handler_id
                    b"GET",
                    b"/test",
                    None,
                    None,
                    &[],
                );
            }
            state_producer.push_batch(arena).expect("push batch");
        });

        producer.join().unwrap();

        // Consumer: should receive the batch.
        let batch = state.poll_batch(Duration::from_millis(200)).unwrap().unwrap();
        assert_eq!(batch.request_count(), 10);

        for (i, ptr) in batch.request_ptrs().iter().enumerate() {
            let info = unsafe { &**ptr };
            assert_eq!(info.request_id, i as u64);
            assert_eq!(info.handler_id, i as u64 * 2);
        }
    }

    /// Test that submit_responses signals the correct Hyper futures.
    #[test]
    fn test_submit_responses_signals_futures() {
        let ring = MpmcRing::new(65536, 64).expect("ring creation");
        let state = GlobalBatchState::new(ring, 64);
        let state_producer = state.clone_producer();

        // Register response channels for 5 requests.
        let mut receivers = Vec::new();
        for i in 0u64..5 {
            let (tx, rx) = std::sync::mpsc::channel();
            state_producer.register_response_channel(i, tx);
            receivers.push((i, rx));
        }

        // Push a batch.
        let producer_state = state_producer.clone();
        let producer = thread::spawn(move || {
            let mut arena = BatchArena::new(65536);
            for i in 0u64..5 {
                arena.push_request(i, i, b"GET", b"/", None, None, &[]);
            }
            producer_state.push_batch(arena).expect("push batch");
        });
        producer.join().unwrap();

        // Process the batch.
        let batch = state.poll_batch(Duration::from_millis(200)).unwrap().unwrap();
        state.set_active_batch(batch);

        // Submit responses.
        let responses: Vec<(u64, u16, Vec<u8>, Vec<(String, String)>)> = (0u64..5)
            .map(|i| (i, 200u16, b"ok".to_vec(), vec![]))
            .collect();
        state.submit_responses(responses).expect("submit");

        // All response channels should have been signaled.
        for (request_id, rx) in receivers {
            let response = rx.recv_timeout(Duration::from_millis(100)).unwrap();
            assert_eq!(response.request_id, request_id);
            assert_eq!(response.status, 200);
        }
    }
}
```

---

### 5.3 Python Test: `src/py/tests/test_ffi.py`

```python
"""
Tests that the _vertebra native module exposes all expected FFI classes.

These tests do NOT require the Rust batch server to be running.
They verify module structure, type stubs, and basic constructibility.
"""

from __future__ import annotations

import pytest


def test_import_vertebra_native() -> None:
    """The native module must be importable."""
    import vertebra._vertebra as _v  # noqa: PLC0415

    assert hasattr(_v, "__version__")


def test_vertebra_response_is_constructible() -> None:
    """VertebraResponse must be constructible from Python."""
    from vertebra._vertebra import VertebraResponse  # noqa: PLC0415

    r = VertebraResponse(status=200, body=b"hello")
    assert r.status == 200
    assert r.body == b"hello"
    assert r.request_id == 0


def test_vertebra_response_defaults() -> None:
    """VertebraResponse defaults: empty body, no headers."""
    from vertebra._vertebra import VertebraResponse  # noqa: PLC0415

    r = VertebraResponse(status=404)
    assert r.status == 404
    assert r.body == b""
    assert r.request_id == 0


def test_vertebra_response_with_request_id() -> None:
    from vertebra._vertebra import VertebraResponse  # noqa: PLC0415

    r = VertebraResponse(status=500, body=b"error", request_id=42)
    assert r.request_id == 42


def test_vertebra_request_not_constructible_from_python() -> None:
    """VertebraRequest must NOT be constructible from Python (no __new__)."""
    from vertebra._vertebra import VertebraRequest  # noqa: PLC0415

    with pytest.raises(TypeError):
        VertebraRequest()  # type: ignore[call-arg]


def test_vertebra_headers_not_constructible_from_python() -> None:
    """VertebraHeaders must NOT be constructible from Python."""
    from vertebra._vertebra import VertebraHeaders  # noqa: PLC0415

    with pytest.raises(TypeError):
        VertebraHeaders()  # type: ignore[call-arg]


def test_ffi_functions_exist() -> None:
    """All three FFI functions must be present."""
    import vertebra._vertebra as _v  # noqa: PLC0415

    assert callable(_v.init_batch_channel)
    assert callable(_v.get_batch)
    assert callable(_v.submit_results)


def test_init_batch_channel_raises_on_invalid_ring() -> None:
    """Connecting to a non-existent ring must raise RuntimeError."""
    from vertebra._vertebra import init_batch_channel  # noqa: PLC0415

    with pytest.raises(RuntimeError, match=".*"):
        init_batch_channel(
            ring_name="/does_not_exist_xyzzy",
            slot_size=4096,
            slots=64,
        )
```

---

### 5.4 Python Test: `src/py/tests/test_runner.py`

```python
"""
Tests for the synchronous batch runner loop.

These tests use mock FFI to verify handler dispatch logic without
requiring the Rust batch server.
"""

from __future__ import annotations

import signal
import threading
from typing import Any
from unittest.mock import MagicMock, patch

import pytest


def _make_mock_request(
    handler_id: int = 1,
    request_id: int = 100,
    method: str = "GET",
    path: str = "/test",
) -> MagicMock:
    """Create a mock VertebraRequest with the minimum fields needed by runner."""
    req = MagicMock()
    req.handler_id = handler_id
    req.request_id = request_id
    req.method = method
    req.path = path
    return req


def _make_mock_response(status: int = 200, request_id: int = 100) -> MagicMock:
    resp = MagicMock()
    resp.status = status
    resp.request_id = request_id
    resp.body = b"ok"
    return resp


class TestRunWorkerDispatch:
    def test_empty_batch_does_not_call_submit(self) -> None:
        """When get_batch() returns empty, submit_results() must not be called."""
        call_count = [0]

        def mock_get_batch(timeout_ms: int = 100) -> list:
            call_count[0] += 1
            if call_count[0] == 1:
                return []
            # On second call, trigger shutdown.
            import signal, os  # noqa: PLC0415, E401
            os.kill(os.getpid(), signal.SIGTERM)
            return []

        mock_submit = MagicMock()

        with (
            patch("vertebra.core.runner.get_batch", mock_get_batch),
            patch("vertebra.core.runner.submit_results", mock_submit),
            patch("vertebra.core.runner.VertebraResponse", MagicMock()),
        ):
            from vertebra.core import runner  # noqa: PLC0415

            runner._RUNNING = True
            runner.run_worker(handlers={})

        mock_submit.assert_not_called()

    def test_handler_dispatched_by_id(self) -> None:
        """Handler with matching handler_id must be called exactly once."""
        req = _make_mock_request(handler_id=42, request_id=7)
        mock_response = _make_mock_response(status=200, request_id=7)

        call_count = [0]

        def mock_get_batch(timeout_ms: int = 100) -> list:
            call_count[0] += 1
            if call_count[0] == 1:
                return [req]
            import signal, os  # noqa: PLC0415, E401
            os.kill(os.getpid(), signal.SIGTERM)
            return []

        mock_submit = MagicMock()

        handler = MagicMock(return_value=mock_response)

        with (
            patch("vertebra.core.runner.get_batch", mock_get_batch),
            patch("vertebra.core.runner.submit_results", mock_submit),
            patch("vertebra.core.runner.VertebraResponse", MagicMock()),
        ):
            from vertebra.core import runner  # noqa: PLC0415

            runner._RUNNING = True
            runner.run_worker(handlers={42: handler})

        handler.assert_called_once_with(req)

    def test_unknown_handler_id_returns_404(self) -> None:
        """Request with no registered handler must produce a 404 response."""
        req = _make_mock_request(handler_id=999, request_id=55)
        responses_submitted = []

        call_count = [0]

        def mock_get_batch(timeout_ms: int = 100) -> list:
            call_count[0] += 1
            if call_count[0] == 1:
                return [req]
            import signal, os  # noqa: PLC0415, E401
            os.kill(os.getpid(), signal.SIGTERM)
            return []

        def mock_submit(resps: list) -> None:
            responses_submitted.extend(resps)

        with (
            patch("vertebra.core.runner.get_batch", mock_get_batch),
            patch("vertebra.core.runner.submit_results", mock_submit),
        ):
            from vertebra.core import runner  # noqa: PLC0415

            runner._RUNNING = True
            runner.run_worker(handlers={})  # no handlers registered

        assert len(responses_submitted) == 1
        assert responses_submitted[0].status == 404

    def test_handler_exception_returns_500_and_continues(self) -> None:
        """Handler that raises must produce a 500 and not crash the loop."""
        req1 = _make_mock_request(handler_id=1, request_id=10)
        req2 = _make_mock_request(handler_id=2, request_id=11)
        responses_submitted = []

        call_count = [0]

        def mock_get_batch(timeout_ms: int = 100) -> list:
            call_count[0] += 1
            if call_count[0] == 1:
                return [req1, req2]
            import signal, os  # noqa: PLC0415, E401
            os.kill(os.getpid(), signal.SIGTERM)
            return []

        def mock_submit(resps: list) -> None:
            responses_submitted.extend(resps)

        def bad_handler(req: Any) -> None:
            raise ValueError("intentional failure")

        good_response = _make_mock_response(status=200, request_id=11)
        good_handler = MagicMock(return_value=good_response)

        with (
            patch("vertebra.core.runner.get_batch", mock_get_batch),
            patch("vertebra.core.runner.submit_results", mock_submit),
        ):
            from vertebra.core import runner  # noqa: PLC0415

            runner._RUNNING = True
            runner.run_worker(handlers={1: bad_handler, 2: good_handler})

        # Both requests should produce a response.
        assert len(responses_submitted) == 2
        statuses = {r.status for r in responses_submitted}
        assert 500 in statuses
        assert 200 in statuses

    def test_submit_called_once_per_non_empty_batch(self) -> None:
        """submit_results() must be called exactly once per non-empty batch."""
        req = _make_mock_request(handler_id=1, request_id=1)
        submit_calls = [0]

        call_count = [0]

        def mock_get_batch(timeout_ms: int = 100) -> list:
            call_count[0] += 1
            if call_count[0] == 1:
                return [req]
            import signal, os  # noqa: PLC0415, E401
            os.kill(os.getpid(), signal.SIGTERM)
            return []

        def mock_submit(resps: list) -> None:
            submit_calls[0] += 1

        handler = MagicMock(return_value=_make_mock_response())

        with (
            patch("vertebra.core.runner.get_batch", mock_get_batch),
            patch("vertebra.core.runner.submit_results", mock_submit),
        ):
            from vertebra.core import runner  # noqa: PLC0415

            runner._RUNNING = True
            runner.run_worker(handlers={1: handler})

        assert submit_calls[0] == 1
```

---

## Integration: Full Pipeline Test

The following describes the integration test that must pass as the final gate for this chapter.

**Test location:** `src/py/tests/test_integration_full_pipeline.py`

**Scenario:** An HTTP request traverses the complete path:

```
wrk/curl -> Hyper HTTP/1.1 server (Ch5) -> matchit router (Ch5) ->
BatchCollector (Ch5) -> MPMC ring -> get_batch() (Ch6) ->
run_worker() (Ch6) -> handler() -> VertebraResponse ->
submit_results() (Ch6) -> Hyper future signaled -> HTTP response
```

**Setup:**
1. Start the Rust HTTP server via `start_http_server()` in a background thread.
2. Register one route: `GET /ping` with `handler_id=1`.
3. In the main thread, call `init_batch_channel()` then `run_worker()` in a thread with `handlers={1: lambda r: VertebraResponse(status=200, body=b"pong")}`.
4. Send `GET http://localhost:8080/ping` via `httpx`.
5. Assert response status 200, body `b"pong"`.
6. SIGTERM the runner thread.

**Pass criteria:**
- Response arrives within 500ms.
- Status is 200.
- Body is `b"pong"`.
- No panics in the Rust layer.
- Worker thread exits cleanly after SIGTERM.

---

## Final Verification Gates

All of the following must pass before Chapter 6 is marked complete:

### Gate 1: Unit tests
```bash
make rs-test          # Rust unit tests pass
make test             # Python unit tests pass
```

### Gate 2: Lint
```bash
make lint             # ruff + rustfmt pass
make lint-all         # mypy + clippy pass
```

### Gate 3: Type stubs
```bash
# mypy must accept the stub file with no errors
uv run mypy src/py/vertebra/_vertebra.pyi --strict
```

### Gate 4: Benchmark baseline

After Ch4+Ch5+Ch6 are integrated, run the benchmark:

```bash
uv run python src/py/benchmarks/native_batch.py
```

Expected: raw native batch throughput >= 150k req/s on a 4-core machine.
Target: >= 200k req/s with GIL-release optimizations.

This is measured before any protocol adapter (Ch7/Ch8) overhead is added. The number should be interpreted as the "transport ceiling" that WSGI and ASGI adapters will approach.

### Gate 5: Integration test
```bash
uv run pytest src/py/tests/test_integration_full_pipeline.py -v
```

---

## Design Rationale & Open Questions

### Why OnceLock for BATCH_STATE?

The global `OnceLock<GlobalBatchState>` in `api.rs` is intentional. Each OS process (Python worker) manages exactly one batch channel. Multiple Python threads in the same process would contend on the GIL anyway, so single-channel-per-process is correct. The `OnceLock` prevents double-initialization bugs at zero runtime cost after initialization.

### Why PyString::intern() for method?

HTTP methods (`GET`, `POST`, `PUT`, `DELETE`, `PATCH`, `OPTIONS`, `HEAD`) are a closed set of 7 strings. CPython's string intern cache means that after the first `GET` request, every subsequent `GET` method access returns the same interned string object rather than allocating a new one. At 200k req/s, this saves ~200k * `sizeof(PyObject)` allocations per second.

### Why not use memoryview for body?

The body slice in the arena is valid only for the batch duration. A `memoryview` that escapes the batch lifetime would point to freed memory. `PyBytes::new()` makes an immediately-safe copy, which is the correct tradeoff: body data is usually consumed immediately by the handler anyway.

Future optimization: expose a `VertebraBodyView` that implements the buffer protocol and is invalidated by `submit_results()`. This would eliminate the copy for large request bodies. Deferred to post-0.1 optimization.

### Why is request_id separate from handler_id?

`request_id` is unique per request in flight (auto-increment u64). `handler_id` is stable per route (assigned at registration time). Using two separate IDs prevents the response correlation logic from accidentally routing a response to the wrong handler.

### Batch size of 64: why?

64 is derived from empirical data in the Litecore research and from cache-line considerations:
- 64 requests * ~128 bytes/RequestInfo = 8KB arena per batch = fits in L1d cache.
- At 200k req/s, a 100ms timeout produces ~20k requests; 64-per-batch means ~313 batches per 100ms = one batch every 320µs.
- Larger batches (e.g. 256) reduce FFI overhead further but increase tail latency (the 256th request waits longer).

This is configurable via `VERTEBRA_BATCH_SIZE` env var and `init_batch_channel(batch_size=N)`.

### arena-lifetime safety without a Rust lifetime

`VertebraRequest` holds `*const RequestInfo` which cannot carry a Rust lifetime into PyO3 because `#[pyclass]` types must be `'static`. The safety invariant is enforced by the runner loop's structure:

1. `get_batch()` stores the `BatchNode` (arena owner) in `GlobalBatchState::active_batch`.
2. Python calls handlers with proxy objects.
3. Python calls `submit_results()`.
4. `submit_results()` drops `active_batch`, releasing the arena.
5. `get_batch()` can now be called again.

This is a sequential protocol. Violations (holding proxies after step 4) are use-after-free. Debug builds of `BatchNode::drop()` will poison the arena memory (write `0xdeadbeef`) to make violations obvious in tests.

---

## File Checklist

| File | Phase | Status |
|------|-------|--------|
| `src/rs/vertebra-py/src/request.rs` | 1 | [ ] |
| `src/rs/vertebra-py/src/headers.rs` | 1 | [ ] |
| `src/rs/vertebra-py/src/response.rs` | 1 | [ ] |
| `src/rs/vertebra-py/src/api.rs` | 2 | [ ] |
| `src/rs/vertebra-py/src/lib.rs` (updated) | 2 | [ ] |
| `src/py/vertebra/core/runner.py` | 3 | [ ] |
| `src/py/vertebra/core/worker.py` | 3 | [ ] |
| `src/py/vertebra/_vertebra.pyi` (updated) | 4 | [ ] |
| `src/rs/vertebra-py/tests/test_request_proxy.rs` | 5 | [ ] |
| `src/rs/vertebra-py/tests/test_batch_api.rs` | 5 | [ ] |
| `src/py/tests/test_ffi.py` | 5 | [ ] |
| `src/py/tests/test_runner.py` | 5 | [ ] |
| `src/py/tests/test_integration_full_pipeline.py` | 5 | [ ] |

## Task Checklist

### Phase 1: Zero-Object Request Proxy

- [ ] **Task 1.1** — Add `mod request;` and `mod headers;` and `mod response;` to `src/rs/vertebra-py/src/lib.rs`.
- [ ] **Task 1.2** — Implement `VertebraRequest` in `src/rs/vertebra-py/src/request.rs`. Verify `#[new]` is absent (not constructible from Python).
- [ ] **Task 1.3** — Implement `VertebraHeaders` in `src/rs/vertebra-py/src/headers.rs`. Verify `__getitem__` is case-insensitive.
- [ ] **Task 1.4** — Implement `VertebraResponse` in `src/rs/vertebra-py/src/response.rs`. Verify `#[new]` is present (constructible from Python).
- [ ] **Task 1.5** — Register `VertebraRequest`, `VertebraHeaders`, `VertebraResponse` in `_vertebra` pymodule.
- [ ] **Task 1.6** — Run `cargo test -p vertebra-py` — all existing tests still pass.

### Phase 2: Vectorized Batch API

- [ ] **Task 2.1** — Add `mod api;` to `src/rs/vertebra-py/src/lib.rs`.
- [ ] **Task 2.2** — Confirm `GlobalBatchState` exists in `vertebra-core` (Ch4 prerequisite). If not, stub it locally.
- [ ] **Task 2.3** — Implement `init_batch_channel()` in `src/rs/vertebra-py/src/api.rs`.
- [ ] **Task 2.4** — Implement `get_batch()`. Confirm GIL is released during ring poll via `py.detach()`.
- [ ] **Task 2.5** — Implement `submit_results()`. Confirm GIL is released during arena write.
- [ ] **Task 2.6** — Register `init_batch_channel`, `get_batch`, `submit_results` in `_vertebra` pymodule.
- [ ] **Task 2.7** — Write `src/rs/vertebra-py/tests/test_batch_api.rs`. Run and pass.

### Phase 3: Python Batch Runner

- [ ] **Task 3.1** — Create `src/py/vertebra/core/` if it does not exist.
- [ ] **Task 3.2** — Implement `src/py/vertebra/core/runner.py`. Verify no `asyncio` import.
- [ ] **Task 3.3** — Implement `src/py/vertebra/core/worker.py`. Verify env-var reading.
- [ ] **Task 3.4** — Write `src/py/tests/test_runner.py`. Run with `make test`.
- [ ] **Task 3.5** — Write `src/py/tests/test_ffi.py`. Run with `make test`.

### Phase 4: Type Stubs

- [ ] **Task 4.1** — Update `src/py/vertebra/_vertebra.pyi` with all new classes and functions.
- [ ] **Task 4.2** — Run `uv run mypy src/py/vertebra/_vertebra.pyi --strict` — no errors.

### Phase 5: Integration

- [ ] **Task 5.1** — Write `src/py/tests/test_integration_full_pipeline.py`.
- [ ] **Task 5.2** — Run integration test: `uv run pytest src/py/tests/test_integration_full_pipeline.py -v`.
- [ ] **Task 5.3** — Run benchmark: `uv run python src/py/benchmarks/native_batch.py`. Record throughput.
- [ ] **Task 5.4** — Run `make test-all`. All suites green.
- [ ] **Task 5.5** — Run `make lint-all`. Zero warnings.
