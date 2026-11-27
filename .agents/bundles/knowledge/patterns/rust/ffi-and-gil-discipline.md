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
  - by: agent:flow-executor
    at: "2026-11-21T21:06:00Z"
    commit: 06e99fbf
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
- All 13 PyO3 extension crates declare `#[pymodule(gil_used = false)]` and maintain `.pyi` stub parity in `src/py/vertebra/` (omitting `__new__` on non-instantiable classes, typing buffers as `collections.abc.Buffer`, and avoiding `Any` on concrete signatures).
- Blocking ring, condition, I/O, and SHM-mapping calls release the GIL via `py.detach(|| ...)`. Small non-blocking writes (`<= 64 KiB`) copy inline before detaching only on full-ring retry; writable (`!readonly()`) or non-contiguous `PyBuffer<u8>` inputs copy via `to_vec(py)` and drop before `py.detach`.
- Non-GC hot-path `#[pyclass]` types gate `freelist = 128` behind `#[cfg_attr(not(Py_GIL_DISABLED), ...)]` and keep `immutable_type`. Cyclic-GC types (`_SteppedCoroutine`, `CachedCallableBox`) implement `__traverse__`/`__clear__` without `freelist`; decoder caches store `PyWeakrefReference` pairs; `Request` anchors `PyBuffer_FillInfo` with borrowed `slf.as_ptr()`.
- `vertebra-core::py_async` provides the Tokio completion bridge; each extension supplies a distinct `drainer_attr` name and transfers `ArrowCursor` mappings through producer-owned `vertebra.ShmCapsule` callbacks without calling `Python::attach` on Tokio threads.
- Zero-I/O async methods return `ReadyAwaitable` (`ready_ok` / `ready_err`) without spawning a Tokio task; database bind extractors check exact `tuple`/`list`/`dict` types before protocol casts.
<!-- truth: end -->

All 13 PyO3 extensions (`_vertebra`[^src-vertebra-py-src-lib-rs], `_bundler`, `_storage`[^src-vertebra-storage-src-lib-rs], `_bigquery`[^src-vertebra-bigquery-src-lib-rs], `_duckdb`, `_mssql`, `_mysql`, `_oracle`, `_postgres`[^src-vertebra-postgres-src-lib-rs], `_redis`, `_spanner`[^src-vertebra-spanner-src-lib-rs], `_sqlite`, and `_valkey`) use PyO3 0.30.1 (`Bound<'py, T>` API) and declare `#[pymodule(gil_used = false)]` so importing them under free-threaded Python leaves the GIL disabled. Keep that attribute on every new `#[pymodule]`.

## GIL Release, Inline Writes & Buffer Discipline

- **Detach Around Blocking Work**: Any entrypoint blocking on a ring, condition, filesystem, or SHM mapping must call `py.detach(|| ...)`. [`Response::from_file`](../../../../../src/rs/vertebra-py/src/response.rs)[^src-vertebra-py-src-response-rs] validates `offset.checked_add(length)` before detaching, and [`ArrowCursor::read_ipc_from_shm`](../../../../../src/rs/vertebra-py/src/db.rs)[^src-vertebra-py-src-db-rs] runs `shm_open`/`mmap`/copy inside `py.detach`.
- **Inline Small Non-Blocking Submits**: Small response writes (`<= 64 KiB` in `submit_response_raw` / `submit_batch_raw` in [`transport.rs`](../../../../../src/rs/vertebra-py/src/transport.rs)[^src-vertebra-py-src-transport-rs]) copy directly into the ring slot or `ResponseSink` without `py.detach` overhead, detaching only on full-ring retry.
- **Writable `PyBuffer` Copy Rule**: Borrow immutable `PyBytes` or `readonly() && is_c_contiguous()` views only while the GIL is held; copy writable or non-contiguous `PyBuffer<u8>` views via `to_vec(py)?` and drop `PyBuffer` before `py.detach` (`with_buffer_slice` in [`transport.rs`](../../../../../src/rs/vertebra-py/src/transport.rs), `extract_buffer_bytes` in [`webtransport.rs`](../../../../../src/rs/vertebra-py/src/webtransport.rs)[^src-vertebra-py-src-webtransport-rs]).
- **Exact-Type Bind Extraction**: Database parameter extractors test exact `tuple`, `list`, and `dict` types before falling back to sequence/mapping protocols, returning empty containers without allocating.

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

## Single `_vertebra` Module When Embedding

`vertebra-server` links `vertebra-py` statically. [`main.rs`](../../../../../src/rs/vertebra-http/src/main.rs)[^src-vertebra-http-src-main-rs] calls `append_to_inittab!(pymodule_init)` before the first `Python::attach`, and worker init aliases `sys.modules["vertebra._vertebra"] = sys.modules["_vertebra"]` before importing Vertebra Python modules. Without this, Python loads the wheel's `_vertebra*.so` with uninitialized `BRIDGE` and `SHM_TRANSPORT` static globals.

## Class Flags, GC, Buffer Anchoring & `.pyi` Parity

- **Conditional `freelist` vs. Cyclic GC**: Hot-path non-GC `#[pyclass]` types gate `freelist = 128` behind `#[cfg_attr(not(Py_GIL_DISABLED), ...)]` because PyO3 guards `freelist` with a global mutex under free-threaded Python. Keep `immutable_type` (`Py_TPFLAGS_IMMUTABLETYPE`) for JIT/inline-cache specialization. Cyclic-GC types ([`SteppedCoroutine`](../../../../../src/rs/vertebra-py/src/asgi.rs)[^src-vertebra-py-src-asgi-rs], [`CachedCallableBox`](../../../../../src/rs/vertebra-py/src/msgspec_bridge.rs)[^src-vertebra-py-src-msgspec-bridge-rs]) implement `__traverse__` and `__clear__` without `freelist`.
- **Weakref Type Caches**: [`msgspec_bridge.rs`](../../../../../src/rs/vertebra-py/src/msgspec_bridge.rs)[^src-vertebra-py-src-msgspec-bridge-rs] stores `WeakCacheEntry` weakref pairs pointing to a `CachedCallableBox` on the target type and verifies pointer identity on lookup so dynamic types collect without ABA reuse.
- **Anchored Zero-Copy Buffer Protocol**: [`Request`](../../../../../src/rs/vertebra-py/src/request.rs)[^src-vertebra-py-src-request-rs] passes borrowed `slf.as_ptr()` to `PyBuffer_FillInfo` (which calls `Py_XNewRef` internally to pin `Arc<BatchSnapshot>` until `memoryview` release). [`WsgiInput::from_snapshot`](../../../../../src/rs/vertebra-py/src/wsgi/input.rs)[^src-vertebra-py-src-wsgi-input-rs] similarly pins `Arc<BatchSnapshot>`.
- **Python 3.15 C-API & Interning**: Use `PyBytes::new_with` (PEP 782 `PyBytesWriter`) to fill bytes in place. [`vertebra-py/build.rs`](../../../../../src/rs/vertebra-py/build.rs)[^src-vertebra-py-build-rs] enables `pyo3_build_config::use_pyo3_cfgs()`, gating `PyFrozenDict::empty` and `PyUnstable_SetImmortal` under `#[cfg(Py_3_15)]`. Use `pyo3::intern!` for fixed header/scope/environ keys ([`headers.rs`](../../../../../src/rs/vertebra-py/src/headers.rs)[^src-vertebra-py-src-headers-rs], [`rsgi/scope.rs`](../../../../../src/rs/vertebra-py/src/rsgi/scope.rs)[^src-vertebra-py-src-rsgi-scope-rs], [`wsgi/environ.rs`](../../../../../src/rs/vertebra-py/src/wsgi/environ.rs)[^src-vertebra-py-src-wsgi-environ-rs]) and `PyOnceLock` for cached exception types ([`errors.rs`](../../../../../src/rs/vertebra-py/src/errors.rs)[^src-vertebra-py-src-errors-rs]).
- **Batch Coroutine & WSGI Drivers**: [`drive_asgi_batch`](../../../../../src/rs/vertebra-py/src/asgi.rs) and [`drive_rsgi_batch`](../../../../../src/rs/vertebra-py/src/rsgi/http.rs)[^src-vertebra-py-src-rsgi-http-rs] step coroutines via `PyIter_Send` (0 `Task` allocations on `PYGEN_RETURN`, `_SteppedCoroutine` promotion on `PYGEN_NEXT`; `RsgiReadyAwaitable` stays `frozen`). [`drive_wsgi_batch`](../../../../../src/rs/vertebra-py/src/wsgi/runner.rs)[^src-vertebra-py-src-wsgi-runner-rs] drives WSGI batches inline over lazy `WsgiEnviron` and `PreparedRawBody::PyBytes`.
- **Stub Parity (`_vertebra.pyi`)**: [`_vertebra.pyi`](../../../../../src/py/vertebra/_vertebra.pyi)[^src-vertebra-pyi] omits `__new__` on non-instantiable classes, types buffer arguments as `collections.abc.Buffer`, and avoids `Any` where Rust types are concrete so `mypy.stubtest` and `pyright` stay clean.

## Shared `py_async` Bridge & Cross-Extension Ownership

- **Completed Awaitables**: Zero-I/O async methods (such as buffered cursor `fetchone`/`fetchmany`/`fetchall`/`close`) return `ReadyAwaitable` (`ready_ok` / `ready_err` from [`vertebra_core::py_async`](../../../../../src/rs/vertebra-core/src/py_async.rs)[^src-vertebra-core-src-py-async-rs]) without spawning a Tokio task; stubs keep `Awaitable[T]`.
- **Per-Loop Completion Drainers**: `vertebra_core::py_async` (feature `pyo3`, no `extension-module`) is re-exported by [`vertebra_py::async_bridge`](../../../../../src/rs/vertebra-py/src/async_bridge.rs)[^src-vertebra-py-src-async-bridge-rs], [`vertebra_storage`](../../../../../src/rs/vertebra-storage/src/lib.rs), and [`vertebra_db::py_async`](../../../../../src/rs/vertebra-db/src/py_async.rs)[^src-vertebra-db-src-py-async-rs]. Tokio worker threads never call `Python::attach`; completions enqueue onto a per-loop `AsyncCompletionQueue` drained via `loop.add_reader`, mapping worker panics to `TaskPanicError` and wiring Python `Future` cancellation to Tokio `AbortHandle` (`_RustTaskCanceller`). Ordered PostgreSQL replication ([`replication/mod.rs`](../../../../../src/rs/vertebra-postgres/src/replication/mod.rs)[^src-vertebra-postgres-src-replication-mod-rs], [`supervisor.rs`](../../../../../src/rs/vertebra-postgres/src/replication/supervisor.rs)[^src-vertebra-postgres-src-replication-supervisor-rs]) and listener delivery ([`async_client.rs`](../../../../../src/rs/vertebra-postgres/src/async_client.rs)[^src-vertebra-postgres-src-async-client-rs]) use `spawn_rust_db_task_with_attr_and_future` / `_and_cancel` with delivery guards.
- **Independent `cdylib` Globals**: Because linking an `rlib` into separate `cdylib` extensions creates distinct static globals per extension, give each extension a unique `drainer_attr` (`_vertebra_core_completion_drainer`, `_vertebra_storage_drainer`, `_vertebra_postgres_completion_drainer`, etc.) and resolve canonical exceptions and `ArrowCursor` through `vertebra._vertebra`.
- **Producer-Owned Arrow Capsules**: Transfer SHM Arrow mappings through `construct_py_arrow_cursor` and the named C-ABI capsule so `drop_fn` and `unlink_fn` run in the producing extension's allocator. See [extension boundaries](../../architecture/worker/pyo3-ffi-and-batch-dispatch.md#database--storage-extension-boundaries) and [runtime gotchas](../../gotchas/pyo3-and-python-runtime.md#cross-extension-arrow-capsules-and-release).

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
