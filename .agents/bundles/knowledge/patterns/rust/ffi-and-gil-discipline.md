---
type: Pattern
title: "PyO3 0.30.1 FFI & GIL Discipline"
description: "PyO3 0.30.1 conventions across Python 3.12-3.15/3.15t: gil_used = false, conditional freelist vs cyclic GC, writable PyBuffer copy discipline, weakref type caches, shared vertebra-core py_async drainer isolation, and _vertebra.pyi rules."
status: stable
tags:
  - patterns
  - rust
  - pyo3
  - ffi
  - gil
  - python315
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
  - by: agent:flow-executor
    at: "2026-11-17T04:20:00Z"
    commit: 5a833832
sources:
  - id: src-vertebra-core-src-py-async-rs
    resource: src/rs/vertebra-core/src/py_async.rs
    title: "Shared PyO3 Tokio completion bridge (src/rs/vertebra-core/src/py_async.rs)"
  - id: src-vertebra-py-build-rs
    resource: src/rs/vertebra-py/build.rs
    title: "Rust implementation module (src/rs/vertebra-py/build.rs)"
  - id: src-vertebra-py-src-lib-rs
    resource: src/rs/vertebra-py/src/lib.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/lib.rs)"
  - id: src-vertebra-py-src-transport-rs
    resource: src/rs/vertebra-py/src/transport.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/transport.rs)"
  - id: src-vertebra-py-src-request-rs
    resource: src/rs/vertebra-py/src/request.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/request.rs)"
  - id: src-vertebra-py-src-response-rs
    resource: src/rs/vertebra-py/src/response.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/response.rs)"
  - id: src-vertebra-py-src-headers-rs
    resource: src/rs/vertebra-py/src/headers.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/headers.rs)"
  - id: src-vertebra-py-src-asgi-rs
    resource: src/rs/vertebra-py/src/asgi.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/asgi.rs)"
  - id: src-vertebra-py-src-msgspec-bridge-rs
    resource: src/rs/vertebra-py/src/msgspec_bridge.rs
    title: "Weakref-keyed decoder and validator caches (src/rs/vertebra-py/src/msgspec_bridge.rs)"
  - id: src-vertebra-py-src-db-rs
    resource: src/rs/vertebra-py/src/db.rs
    title: "ArrowCursor SHM IPC read GIL detachment (src/rs/vertebra-py/src/db.rs)"
  - id: src-vertebra-py-src-wsgi-environ-rs
    resource: src/rs/vertebra-py/src/wsgi/environ.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/wsgi/environ.rs)"
  - id: src-vertebra-py-src-wsgi-input-rs
    resource: src/rs/vertebra-py/src/wsgi/input.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/wsgi/input.rs)"
  - id: src-vertebra-py-src-wsgi-runner-rs
    resource: src/rs/vertebra-py/src/wsgi/runner.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/wsgi/runner.rs)"
  - id: src-vertebra-py-src-rsgi-scope-rs
    resource: src/rs/vertebra-py/src/rsgi/scope.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/rsgi/scope.rs)"
  - id: src-vertebra-py-src-rsgi-http-rs
    resource: src/rs/vertebra-py/src/rsgi/http.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/rsgi/http.rs)"
  - id: src-vertebra-py-src-async-bridge-rs
    resource: src/rs/vertebra-py/src/async_bridge.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/async_bridge.rs)"
  - id: src-vertebra-py-src-webtransport-rs
    resource: src/rs/vertebra-py/src/webtransport.rs
    title: "WebTransport PyO3 bindings and PyBuffer copy helper (src/rs/vertebra-py/src/webtransport.rs)"
  - id: src-vertebra-py-src-errors-rs
    resource: src/rs/vertebra-py/src/errors.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/errors.rs)"
  - id: src-vertebra-http-src-main-rs
    resource: src/rs/vertebra-http/src/main.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/main.rs)"
  - id: src-vertebra-storage-src-lib-rs
    resource: src/rs/vertebra-storage/src/lib.rs
    title: "Storage PyO3 completion bridge drainer attribute (src/rs/vertebra-storage/src/lib.rs)"
  - id: src-vertebra-db-src-py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/py_async.rs)"
  - id: src-vertebra-bigquery-src-lib-rs
    resource: src/rs/vertebra-bigquery/src/lib.rs
    title: "Rust implementation module (src/rs/vertebra-bigquery/src/lib.rs)"
  - id: src-vertebra-spanner-src-lib-rs
    resource: src/rs/vertebra-spanner/src/lib.rs
    title: "Rust implementation module (src/rs/vertebra-spanner/src/lib.rs)"
  - id: src-vertebra-postgres-src-lib-rs
    resource: src/rs/vertebra-postgres/src/lib.rs
    title: "Rust implementation module (src/rs/vertebra-postgres/src/lib.rs)"
  - id: src-vertebra-postgres-src-replication-mod-rs
    resource: src/rs/vertebra-postgres/src/replication/mod.rs
    title: "PostgreSQL native replication bridge (src/rs/vertebra-postgres/src/replication/mod.rs)"
  - id: src-vertebra-postgres-src-replication-supervisor-rs
    resource: src/rs/vertebra-postgres/src/replication/supervisor.rs
    title: "PostgreSQL replication supervisor and delivery leases (src/rs/vertebra-postgres/src/replication/supervisor.rs)"
  - id: src-vertebra-postgres-src-async-client-rs
    resource: src/rs/vertebra-postgres/src/async_client.rs
    title: "PostgreSQL async client and PendingDeliveryGuard listener bridge (src/rs/vertebra-postgres/src/async_client.rs)"
  - id: src-vertebra-pyi
    resource: src/py/vertebra/_vertebra.pyi
    title: "Native _vertebra extension type stubs (src/py/vertebra/_vertebra.pyi)"
---

# PyO3 0.30.1 FFI & GIL Discipline

<!-- truth: start -->
- PyO3 extension crates (`vertebra-py`, `vertebra-bundler`, `vertebra-storage`, `vertebra-bigquery`, `vertebra-duckdb`, `vertebra-mssql`, `vertebra-mysql`, `vertebra-oracle`, `vertebra-postgres`, `vertebra-redis`, `vertebra-sqlite`, `vertebra-spanner`, `vertebra-valkey`) declare `#[pymodule(gil_used = false)]` and maintain `.pyi` stub parity in `src/py/vertebra/` (omitting `__new__` on non-instantiable classes, typing buffer parameters with `collections.abc.Buffer`, and avoiding `Any` on concrete native signatures).
- Blocking ring, condition, I/O, and `ArrowCursor::read_ipc_from_shm` SHM-mapping calls release the GIL via `py.detach(|| ...)`, non-blocking small response writes (`<= 64 KiB`) copy inline before detaching only on full-ring retry, and writable (`!readonly()`) or non-contiguous `PyBuffer<u8>` inputs copy via `to_vec(py)` and drop `PyBuffer` before `py.detach`.
- Non-GC hot-path `#[pyclass]` types gate `freelist = 128` behind `#[cfg_attr(not(Py_GIL_DISABLED), ...)]` and keep `immutable_type`, whereas cyclic-GC types (`_SteppedCoroutine`, `CachedCallableBox`) implement `__traverse__`/`__clear__` without `freelist`, type-keyed decoder caches store `PyWeakrefReference` pairs, and `Request` anchors `PyBuffer_FillInfo` with borrowed `slf.as_ptr()`.
- `vertebra-core::py_async` (behind Cargo feature `pyo3`, without `extension-module`) provides the canonical completion bridge; `_vertebra` (`_vertebra_core_completion_drainer`), `_storage` (`_vertebra_storage_drainer`), and separately linked database extensions pass distinct `drainer_attr` names and transfer `ArrowCursor` backing through producer-owned `vertebra.ShmCapsule` callbacks without calling `Python::attach` on Tokio threads.
<!-- truth: end -->

The 13 PyO3 extensions are `_vertebra` ([`vertebra-py`](../../../../../src/rs/vertebra-py/src/lib.rs)[^src-vertebra-py-src-lib-rs]), `_bundler` (`vertebra-bundler`), `_storage` ([`vertebra-storage`](../../../../../src/rs/vertebra-storage/src/lib.rs)[^src-vertebra-storage-src-lib-rs]), `_bigquery` ([`vertebra-bigquery`](../../../../../src/rs/vertebra-bigquery/src/lib.rs)[^src-vertebra-bigquery-src-lib-rs]), `_duckdb` (`vertebra-duckdb`), `_mssql` (`vertebra-mssql`), `_mysql` (`vertebra-mysql`), `_oracle` (`vertebra-oracle`), `_postgres` ([`vertebra-postgres`](../../../../../src/rs/vertebra-postgres/src/lib.rs)[^src-vertebra-postgres-src-lib-rs]), `_redis` (`vertebra-redis`), `_spanner` ([`vertebra-spanner`](../../../../../src/rs/vertebra-spanner/src/lib.rs)[^src-vertebra-spanner-src-lib-rs]), `_sqlite` (`vertebra-sqlite`), and `_valkey` (`vertebra-valkey`), typed by `src/py/vertebra/*.pyi`. All use PyO3 `0.30.1` with the `Bound<'py, T>` API and declare `#[pymodule(gil_used = false)]` so importing them on `python3.14t` and `python3.15t` leaves the GIL disabled. Keep that attribute on every new `#[pymodule]`.

## Release the GIL Around Blocking Work, Inline Fast Non-Blocking Submits & Copy Writable Buffers

Any `#[pyfunction]` or `#[pymethods]` entrypoint that blocks on a ring, condition, filesystem, or SHM mapping releases the GIL with `py.detach(|| ...)`: [`Response::from_file`](../../../../../src/rs/vertebra-py/src/response.rs)[^src-vertebra-py-src-response-rs] validates `offset.checked_add(length)` before detaching into `read_file_range_sync`, and [`ArrowCursor::read_ipc_from_shm`](../../../../../src/rs/vertebra-py/src/db.rs)[^src-vertebra-py-src-db-rs] runs `read_shm_arrow_frame` (`shm_open`, `mmap`, and payload copy) inside `py.detach`. Conversely, small non-blocking response writes (`<= 64 KiB` in `submit_response_raw` and `submit_batch_raw` in [`transport.rs`](../../../../../src/rs/vertebra-py/src/transport.rs)[^src-vertebra-py-src-transport-rs]) copy the `PyBuffer` payload directly into the SHM `SpscRing` slot or `ResponseSink` *without* `py.detach` thread-state churn, detaching only when a full ring requires a blocking spin/futex retry.

Whenever a FFI function reads a `PyAny` buffer input (`bytes`, `bytearray`, `memoryview`) across or after GIL release, borrow only immutable `PyBytes` or `py_buf.readonly() && py_buf.is_c_contiguous()` views while the GIL is held; copy writable (`!py_buf.readonly()`) or non-contiguous `PyBuffer<u8>` views into owned Rust memory via `py_buf.to_vec(py)?` and drop `py_buf` before `py.detach` (`with_buffer_slice` and `submit_raw_response` in [`transport.rs`](../../../../../src/rs/vertebra-py/src/transport.rs)[^src-vertebra-py-src-transport-rs], `extract_buffer_bytes` in [`webtransport.rs`](../../../../../src/rs/vertebra-py/src/webtransport.rs)[^src-vertebra-py-src-webtransport-rs]).

```rust
#[pyfunction]
#[pyo3(name = "get_batch", signature = (timeout_ms=100))]
pub fn get_batch(py: Python<'_>, timeout_ms: u64) -> PyResult<Option<RequestBatch>> {
    if let Some(transport) = SHM_TRANSPORT.get() {
        if let Some(s) = transport.poll_batch_async() {
            return Ok(Some(RequestBatch { snapshot: s }));
        }
        let snapshot = py.detach(|| transport.poll_batch(Duration::from_millis(timeout_ms)));
        return Ok(snapshot.map(|s| RequestBatch { snapshot: s }));
    }

    let bridge = BatchBridge::get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "transport not initialized; call init_transport() first",
        )
    })?;

    if let Some(s) = bridge.try_poll_batch() {
        return Ok(Some(RequestBatch { snapshot: s }));
    }

    let snapshot = py.detach(|| bridge.poll_batch(Duration::from_millis(timeout_ms)));
    Ok(snapshot.map(|s| RequestBatch { snapshot: s }))
}
```

## One `_vertebra` Module When Embedding

`vertebra-server` links `vertebra-py` statically. [`main.rs`](../../../../../src/rs/vertebra-http/src/main.rs)[^src-vertebra-http-src-main-rs] calls `append_to_inittab!(pymodule_init)` before the first `Python::attach`, and worker initialization imports `_vertebra` and aliases `sys.modules["vertebra._vertebra"] = sys.modules["_vertebra"]` before importing any Vertebra Python code. Any new embedding entrypoint must do the same; otherwise Python loads the wheel's `_vertebra.cpython-*.so`, which has its own uninitialized `static BRIDGE: OnceLock<BatchBridge>` and `SHM_TRANSPORT`.

## Conditional `freelist`, Cyclic GC, Weakref Caches, Anchored `Py_buffer`, `PyIter_Send` & `.pyi` Rules

- **Conditional `freelist` & `immutable_type` vs Cyclic GC**: Non-GC hot-path `#[pyclass]` types (`Request`, `Response`, `Headers`, `HeadersIterator`, `RequestBatch`, `RequestBatchIterator`, `AsgiScope`, `AsgiScopeIterator`, `AsgiProtocol`, `PyEmptyAwaitable`, `PyValueAwaitable`, `RsgiScope`, `RsgiHttpProtocol`, `_RsgiReadyAwaitable`, `RsgiTransport`, `WsgiEnviron`, `WsgiEnvironIterator`, `WsgiInput`, `WsgiFileWrapper`, `_WsgiStartResponse`) apply `#[cfg_attr(not(Py_GIL_DISABLED), pyclass(..., freelist = 128, immutable_type))]` and `#[cfg_attr(Py_GIL_DISABLED, pyclass(..., immutable_type))]` because PyO3 `0.30.1` guards `freelist` behind a global `Mutex` under `Py_GIL_DISABLED` (contending across `3.14t`/`3.15t` worker threads vs mimalloc thread-local heaps), while `immutable_type` (`Py_TPFLAGS_IMMUTABLETYPE`) enables CPython 3.14/3.15 JIT and inline-cache specialization. By contrast, [`SteppedCoroutine`](../../../../../src/rs/vertebra-py/src/asgi.rs)[^src-vertebra-py-src-asgi-rs] (`_SteppedCoroutine`, holding `coro`, `first_yielded`, `finalizer`, and `self_task`) and [`CachedCallableBox`](../../../../../src/rs/vertebra-py/src/msgspec_bridge.rs)[^src-vertebra-py-src-msgspec-bridge-rs] implement `__traverse__` (`PyVisit`) and `__clear__` without `freelist` because PyO3 disallows `freelist` on cyclic-GC (`PyGC_Head`) types.
- **Weakref Type Caches (`msgspec_bridge.rs`)**: [`msgspec_bridge.rs`](../../../../../src/rs/vertebra-py/src/msgspec_bridge.rs)[^src-vertebra-py-src-msgspec-bridge-rs] backs `MSGSPEC_DECODER_CACHE`, `PYDANTIC_VALIDATOR_CACHE`, `PROTOBUF_DECODER_CACHE`, and `STRUCT_FIELDS_CACHE` with `WeakCacheEntry { target_ref: Py<PyWeakrefReference>, box_ref: Py<PyWeakrefReference> }` pointing to a GC-aware `CachedCallableBox` attribute stored on the target type, registering a weakref cleanup closure on `target_ref` and checking `live_target.as_ptr() == target.as_ptr()` on lookup so dynamic types are collected cleanly without raw-pointer ABA reuse.
- **Anchored Zero-Copy Buffer Protocol (`Request`)**: [`Request`](../../../../../src/rs/vertebra-py/src/request.rs)[^src-vertebra-py-src-request-rs] implements `__getbuffer__` passing borrowed `slf.as_ptr()` to `PyBuffer_FillInfo` (which calls `Py_XNewRef` internally, keeping `Arc<BatchSnapshot>` alive until the `memoryview` is released) and exposes `memoryview(req)` (`req.body_view`) with PEP 688 `def __buffer__(self, flags: int, /) -> memoryview: ...` in `_vertebra.pyi`. [`WsgiInput::from_snapshot`](../../../../../src/rs/vertebra-py/src/wsgi/input.rs)[^src-vertebra-py-src-wsgi-input-rs] similarly pins `Arc<BatchSnapshot>` directly for zero-copy `wsgi.input` reads.
- **PEP 782 `PyBytesWriter` & Python 3.15 PEP 814 / Immortal Constants**: Use `PyBytes::new_with(py, len, |dst| ...)` (backed by CPython 3.15's PEP 782 `PyBytesWriter` C-API in PyO3 `0.30.1`) to populate byte buffers in place without intermediate `Vec<u8>` copies. [`vertebra-py/build.rs`](../../../../../src/rs/vertebra-py/build.rs)[^src-vertebra-py-build-rs] runs `pyo3_build_config::use_pyo3_cfgs()`, gating PEP 814 `PyFrozenDict::empty(py)` (`build_empty_capability_entry` in `asgi.rs`) and `PyUnstable_SetImmortal` on static protocol constants under `#[cfg(Py_3_15)]`.
- **C-API `PyIter_Send` Coroutine Stepping & Native `drive_wsgi_batch`**: [`drive_asgi_batch`](../../../../../src/rs/vertebra-py/src/asgi.rs)[^src-vertebra-py-src-asgi-rs] and [`drive_rsgi_batch`](../../../../../src/rs/vertebra-py/src/rsgi/http.rs)[^src-vertebra-py-src-rsgi-http-rs] step ASGI/RSGI coroutines directly via `pyo3::ffi::PyIter_Send(coro_ptr, Py_None(), &mut result_ptr)`: `PYGEN_RETURN` completes in `0` `asyncio.Task` allocations, while `PYGEN_NEXT` wraps the already-stepped coroutine in `_SteppedCoroutine` (`first_yielded`) and schedules `loop.create_task` once. `RsgiReadyAwaitable` (`_RsgiReadyAwaitable`) is `frozen` (`&self` on `__await__`/`__next__`) so concurrent awaits on `3.14t`/`3.15t` never trigger `PyBorrowMutError`. For PEP 3333 WSGI, [`drive_wsgi_batch`](../../../../../src/rs/vertebra-py/src/wsgi/runner.rs)[^src-vertebra-py-src-wsgi-runner-rs] drives an entire `RequestBatch` or `Sequence[Request]` in Rust using lazy `WsgiEnviron`, zero-copy `WsgiInput::from_snapshot`, `_WsgiStartResponse` binary header encoding, and `PreparedRawBody::PyBytes` single-chunk zero-copy submission.
- **`_vertebra.pyi` Stub Parity Rules**: [`_vertebra.pyi`](../../../../../src/py/vertebra/_vertebra.pyi)[^src-vertebra-pyi] omits `__new__` on classes without a native constructor (`Request`, `Headers`, `HeadersIterator`, `RequestBatch`, `RequestBatchIterator`, `AsgiScopeIterator`, `WsgiEnviron`, `WsgiEnvironIterator`, `_WsgiStartResponse`, `RsgiTransport`), widens `RsgiHttpProtocol.response_bytes` and buffer-protocol parameters (`Response.from_buffer`, `pubsub_publish`, `WebTransportStream.send`, `WebTransportSession.send_datagram`) to `collections.abc.Buffer`, and replaces `Any` with concrete types wherever the Rust signature is concrete so `mypy.stubtest vertebra._vertebra` and `pyright` stay clean.

## Intern Repeated Keys, `PyOnceLock` Exceptions & Shared `vertebra-core::py_async` Isolation

Use `pyo3::intern!(py, "...")` for fixed header names and scope/environ keys ([`headers.rs`](../../../../../src/rs/vertebra-py/src/headers.rs)[^src-vertebra-py-src-headers-rs], [`asgi.rs`](../../../../../src/rs/vertebra-py/src/asgi.rs), [`rsgi/scope.rs`](../../../../../src/rs/vertebra-py/src/rsgi/scope.rs)[^src-vertebra-py-src-rsgi-scope-rs], [`wsgi/environ.rs`](../../../../../src/rs/vertebra-py/src/wsgi/environ.rs)[^src-vertebra-py-src-wsgi-environ-rs]) and `PyOnceLock` for process-wide cached objects such as [`GcpExceptionTypes`](../../../../../src/rs/vertebra-py/src/errors.rs)[^src-vertebra-py-src-errors-rs].

[`vertebra_core::py_async`](../../../../../src/rs/vertebra-core/src/py_async.rs)[^src-vertebra-core-src-py-async-rs] (gated behind Cargo feature `pyo3 = ["dep:pyo3", "tokio/rt"]` without `extension-module`) owns the single canonical Tokio `AsyncCompletionQueue` bridge (`spawn_rust_task_with_attr`, `spawn_rust_task_with_attr_and_future`, `spawn_rust_task_with_attr_and_cancel`, `RustTaskAwaitable`, `CompletionDrainer`). [`vertebra_py::async_bridge`](../../../../../src/rs/vertebra-py/src/async_bridge.rs)[^src-vertebra-py-src-async-bridge-rs] (`CORE_COMPLETION_DRAINER_ATTR = "_vertebra_core_completion_drainer"`), [`vertebra_storage`](../../../../../src/rs/vertebra-storage/src/lib.rs)[^src-vertebra-storage-src-lib-rs] (`STORAGE_COMPLETION_DRAINER_ATTR = "_vertebra_storage_drainer"`), and [`vertebra_db::py_async`](../../../../../src/rs/vertebra-db/src/py_async.rs)[^src-vertebra-db-src-py-async-rs] (compiled into `_bigquery`, `_duckdb`, `_mssql`, `_mysql`, `_oracle`, `_postgres`, `_redis`, `_sqlite`, `_spanner`, and `_valkey` with per-extension `drainer_attr` names and `ShmCapsule` `PyCapsule` handoff for `ArrowCursor`) re-export `vertebra_core::py_async` and never call `Python::attach` on Tokio worker threads: a paired completion task awaits the worker `JoinHandle`, enqueues an `OutcomeProducer` onto the per-loop `AsyncCompletionQueue` drained via `loop.add_reader`, converts worker or `map_ok` panics (`JoinError::is_panic()` / `catch_unwind`) into `TaskPanicError`, and attaches `_RustTaskCanceller` (`add_done_callback`) so cancelling the Python `asyncio.Future` aborts the underlying Tokio `AbortHandle` while `CompletionDrainer::__call__` skips cancelled futures when draining `AsyncCompletionQueue`. When ordered replication delivery requires verifying `asyncio.Future` cancellation before publishing state, [`replication/mod.rs`](../../../../../src/rs/vertebra-postgres/src/replication/mod.rs)[^src-vertebra-postgres-src-replication-mod-rs] uses `spawn_rust_db_task_with_attr_and_future` with `ReplicationInFlightGuard` and `ReplicationEventDelivery` ([`supervisor.rs`](../../../../../src/rs/vertebra-postgres/src/replication/supervisor.rs)[^src-vertebra-postgres-src-replication-supervisor-rs]), while `PyAsyncPostgresListener::next` in [`async_client.rs`](../../../../../src/rs/vertebra-postgres/src/async_client.rs)[^src-vertebra-postgres-src-async-client-rs] pairs `spawn_rust_db_task_with_attr_and_cancel` with `PendingDeliveryGuard`. `_vertebra` and the database extensions export `FREE_THREADED` (`cfg(Py_GIL_DISABLED)`) and `GIL_USED = False`, whereas `_bundler` exports `GIL_USED = False` only and `_storage` relies on `#[pymodule(gil_used = false)]`.

## Cross-extension ownership

Treat separately linked extension globals and PyO3 class static variables as independent. Give each extension a unique `spawn_rust_task_with_attr` / `spawn_rust_db_task_with_attr` / `spawn_rust_db_task_with_attr_and_future` attribute name (such as `_vertebra_core_completion_drainer` for `_vertebra`, `_vertebra_storage_drainer` for `_storage`, and `_vertebra_postgres_completion_drainer` for `_postgres`), and resolve canonical Python exceptions and `ArrowCursor` through `vertebra._vertebra`. Reason: sharing an `rlib` implementation (`vertebra-core::py_async` or `vertebra-db::py_async`) does not create shared process static variables across `cdylib` files.[^src-vertebra-core-src-py-async-rs] [^src-vertebra-db-src-py-async-rs]

Transfer Arrow mappings through `construct_py_arrow_cursor` and the named C ABI capsule. Keep allocation cleanup in the producing extension through `drop_fn` and `unlink_fn`; consume `ctx` with the existing atomic ownership claim. Reason: the consumer must preserve the producer's allocator and mapping lifetime. The [extension architecture](../../architecture/worker/pyo3-ffi-and-batch-dispatch.md#database--storage-extension-boundaries) defines the seam; [runtime gotchas](../../gotchas/pyo3-and-python-runtime.md#cross-extension-arrow-capsules-and-release) covers failure paths.

[^src-vertebra-core-src-py-async-rs]: `src/rs/vertebra-core/src/py_async.rs` — Shared PyO3 Tokio completion bridge (`src/rs/vertebra-core/src/py_async.rs`).
[^src-vertebra-py-build-rs]: `src/rs/vertebra-py/build.rs` — Rust implementation module (src/rs/vertebra-py/build.rs).
[^src-vertebra-py-src-lib-rs]: `src/rs/vertebra-py/src/lib.rs` — Rust implementation module (src/rs/vertebra-py/src/lib.rs).
[^src-vertebra-py-src-transport-rs]: `src/rs/vertebra-py/src/transport.rs` — Rust implementation module (src/rs/vertebra-py/src/transport.rs).
[^src-vertebra-py-src-request-rs]: `src/rs/vertebra-py/src/request.rs` — Rust implementation module (src/rs/vertebra-py/src/request.rs).
[^src-vertebra-py-src-response-rs]: `src/rs/vertebra-py/src/response.rs` — Rust implementation module (src/rs/vertebra-py/src/response.rs).
[^src-vertebra-py-src-headers-rs]: `src/rs/vertebra-py/src/headers.rs` — Rust implementation module (src/rs/vertebra-py/src/headers.rs).
[^src-vertebra-py-src-asgi-rs]: `src/rs/vertebra-py/src/asgi.rs` — Rust implementation module (src/rs/vertebra-py/src/asgi.rs).
[^src-vertebra-py-src-msgspec-bridge-rs]: `src/rs/vertebra-py/src/msgspec_bridge.rs` — Weakref-keyed decoder and validator caches (src/rs/vertebra-py/src/msgspec_bridge.rs).
[^src-vertebra-py-src-db-rs]: `src/rs/vertebra-py/src/db.rs` — ArrowCursor SHM IPC read GIL detachment (src/rs/vertebra-py/src/db.rs).
[^src-vertebra-py-src-wsgi-environ-rs]: `src/rs/vertebra-py/src/wsgi/environ.rs` — Rust implementation module (src/rs/vertebra-py/src/wsgi/environ.rs).
[^src-vertebra-py-src-wsgi-input-rs]: `src/rs/vertebra-py/src/wsgi/input.rs` — Rust implementation module (src/rs/vertebra-py/src/wsgi/input.rs).
[^src-vertebra-py-src-wsgi-runner-rs]: `src/rs/vertebra-py/src/wsgi/runner.rs` — Rust implementation module (src/rs/vertebra-py/src/wsgi/runner.rs).
[^src-vertebra-py-src-rsgi-scope-rs]: `src/rs/vertebra-py/src/rsgi/scope.rs` — Rust implementation module (src/rs/vertebra-py/src/rsgi/scope.rs).
[^src-vertebra-py-src-rsgi-http-rs]: `src/rs/vertebra-py/src/rsgi/http.rs` — Rust implementation module (src/rs/vertebra-py/src/rsgi/http.rs).
[^src-vertebra-py-src-async-bridge-rs]: `src/rs/vertebra-py/src/async_bridge.rs` — Rust implementation module (src/rs/vertebra-py/src/async_bridge.rs).
[^src-vertebra-py-src-webtransport-rs]: `src/rs/vertebra-py/src/webtransport.rs` — WebTransport PyO3 bindings and PyBuffer copy helper (src/rs/vertebra-py/src/webtransport.rs).
[^src-vertebra-py-src-errors-rs]: `src/rs/vertebra-py/src/errors.rs` — Rust implementation module (src/rs/vertebra-py/src/errors.rs).
[^src-vertebra-http-src-main-rs]: `src/rs/vertebra-http/src/main.rs` — Rust implementation module (src/rs/vertebra-http/src/main.rs).
[^src-vertebra-storage-src-lib-rs]: `src/rs/vertebra-storage/src/lib.rs` — Storage PyO3 completion bridge drainer attribute (src/rs/vertebra-storage/src/lib.rs).
[^src-vertebra-db-src-py-async-rs]: `src/rs/vertebra-db/src/py_async.rs` — Rust implementation module (src/rs/vertebra-db/src/py_async.rs).
[^src-vertebra-bigquery-src-lib-rs]: `src/rs/vertebra-bigquery/src/lib.rs` — Rust implementation module (src/rs/vertebra-bigquery/src/lib.rs).
[^src-vertebra-spanner-src-lib-rs]: `src/rs/vertebra-spanner/src/lib.rs` — Rust implementation module (src/rs/vertebra-spanner/src/lib.rs).
[^src-vertebra-postgres-src-lib-rs]: `src/rs/vertebra-postgres/src/lib.rs` — Rust implementation module (src/rs/vertebra-postgres/src/lib.rs).
[^src-vertebra-postgres-src-replication-mod-rs]: `src/rs/vertebra-postgres/src/replication/mod.rs` — PostgreSQL native replication bridge (src/rs/vertebra-postgres/src/replication/mod.rs).
[^src-vertebra-postgres-src-replication-supervisor-rs]: `src/rs/vertebra-postgres/src/replication/supervisor.rs` — PostgreSQL replication supervisor and delivery leases (src/rs/vertebra-postgres/src/replication/supervisor.rs).
[^src-vertebra-postgres-src-async-client-rs]: `src/rs/vertebra-postgres/src/async_client.rs` — PostgreSQL async client and PendingDeliveryGuard listener bridge (src/rs/vertebra-postgres/src/async_client.rs).
[^src-vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Native `_vertebra` extension type stubs (`src/py/vertebra/_vertebra.pyi`).
