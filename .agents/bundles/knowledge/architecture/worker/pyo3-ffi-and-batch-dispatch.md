---
type: Guide
title: "PyO3 FFI & Vectorized Batch Dispatch"
description: "The vertebra._vertebra module surface, anchored zero-copy Request/Headers/Response/RequestBatch types, and how Worker and run_worker partition and execute batches."
status: stable
tags: [architecture, worker, pyo3, ffi, batch-dispatch]
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-review
    at: "2026-11-05T00:35:59Z"
    commit: 4ce057e5aff909fcda2488cc0f6c4882ad0d38f0
sources:
  - id: src-vertebra-py-src-lib-rs
    resource: src/rs/vertebra-py/src/lib.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/lib.rs)"
  - id: src-vertebra-py-src-transport-rs
    resource: src/rs/vertebra-py/src/transport.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/transport.rs)"
  - id: src-vertebra-py-src-request-rs
    resource: src/rs/vertebra-py/src/request.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/request.rs)"
  - id: src-vertebra-py-src-headers-rs
    resource: src/rs/vertebra-py/src/headers.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/headers.rs)"
  - id: src-vertebra-py-src-response-rs
    resource: src/rs/vertebra-py/src/response.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/response.rs)"
  - id: src-vertebra-py-src-msgspec-bridge-rs
    resource: src/rs/vertebra-py/src/msgspec_bridge.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/msgspec_bridge.rs)"
  - id: src-vertebra-pyi
    resource: src/py/vertebra/_vertebra.pyi
    title: "Python implementation module (src/py/vertebra/_vertebra.pyi)"
  - id: src-bundler-pyi
    resource: src/py/vertebra/_bundler.pyi
    title: "Python implementation module (src/py/vertebra/_bundler.pyi)"
  - id: src-storage-pyi
    resource: src/py/vertebra/_storage.pyi
    title: "Python implementation module (src/py/vertebra/_storage.pyi)"
  - id: src-bigquery-pyi
    resource: src/py/vertebra/_bigquery.pyi
    title: "Python implementation module (src/py/vertebra/_bigquery.pyi)"
  - id: src-spanner-pyi
    resource: src/py/vertebra/_spanner.pyi
    title: "Python implementation module (src/py/vertebra/_spanner.pyi)"
  - id: src-server-worker-py
    resource: src/py/vertebra/server/worker.py
    title: "Python implementation module (src/py/vertebra/server/worker.py)"
  - id: src-server-dispatch-py
    resource: src/py/vertebra/server/_dispatch.py
    title: "Python implementation module (src/py/vertebra/server/_dispatch.py)"
  - id: src-server-loops-py
    resource: src/py/vertebra/server/_loops.py
    title: "Python implementation module (src/py/vertebra/server/_loops.py)"
  - id: src-server-fastpath-py
    resource: src/py/vertebra/server/_fastpath.py
    title: "Python implementation module (src/py/vertebra/server/_fastpath.py)"
  - id: src-db-py-async
    resource: src/rs/vertebra-db/src/py_async.rs
    title: "Per-extension completion queues and SHM capsule construction"
  - id: src-core-shm-capsule
    resource: src/rs/vertebra-core/src/shm.rs
    title: "C ABI capsule and shared-memory mapping ownership"
  - id: src-py-arrow-cursor
    resource: src/rs/vertebra-py/src/db.rs
    title: "ArrowCursor capsule adoption and release"
---

# PyO3 FFI & Vectorized Batch Dispatch

<!-- truth: start -->
- Vertebra ships five `#[pymodule(gil_used = false)]` PyO3 extensions (`vertebra-py`, `vertebra-bundler`, `vertebra-storage`, `vertebra-bigquery`, `vertebra-spanner`) typed in `src/py/vertebra/{_vertebra,_bundler,_storage,_bigquery,_spanner}.pyi`.
- `vertebra._vertebra` (`#[pymodule(gil_used = false)]`) anchors `Request`, `Headers`, `Response`, and `RequestBatch` to `Arc<BatchSnapshot>` with PEP 688 zero-copy `memoryview` buffer access.
- `Worker` pre-compiles routes into `BatchDispatchPlan` and `HandlerSpec` tables, flushes inline sync handlers via `submit_batch_raw`, and routes single-root ASGI/WSGI mounts directly to Rust batch drivers.
<!-- truth: end -->

[`vertebra-py`](../../../../../src/rs/vertebra-py/src/lib.rs)[^src-vertebra-py-src-lib-rs] builds the `vertebra._vertebra` extension (`#[pymodule(gil_used = false)]`, typed in [`_vertebra.pyi`](../../../../../src/py/vertebra/_vertebra.pyi)[^src-vertebra-pyi]), alongside `_bundler` (`vertebra-bundler`, [`_bundler.pyi`](../../../../../src/py/vertebra/_bundler.pyi)[^src-bundler-pyi]), `_storage` (`vertebra-storage`, [`_storage.pyi`](../../../../../src/py/vertebra/_storage.pyi)[^src-storage-pyi]), `_bigquery` (`vertebra-bigquery`, [`_bigquery.pyi`](../../../../../src/py/vertebra/_bigquery.pyi)[^src-bigquery-pyi]), and `_spanner` (`vertebra-spanner`, [`_spanner.pyi`](../../../../../src/py/vertebra/_spanner.pyi)[^src-spanner-pyi]). [`Worker`](../../../../../src/py/vertebra/server/worker.py)[^src-server-worker-py] (`run_worker`) pulls `RequestBatch` snapshots across FFI, executes inline vectorized sync, WSGI (`drive_wsgi_batch`), and single-thread `uvloop`/`winloop`/`rloop` async/ASGI/RSGI batches (`drive_native_batch`, `drive_asgi_batch`, `drive_rsgi_batch`), and flushes raw responses back to Rust.

## Module surface (`lib.rs`, `transport.rs`)

- **Constants**: `__version__`, `FREE_THREADED` (`cfg(Py_GIL_DISABLED)`), `GIL_USED` (`False`).
- **Request-path classes**: `Request`, `Headers`, `HeadersIterator`, `Response`, `RequestBatch`, `RequestBatchIterator`. All non-subclassed `#[pyclass]` types declare `immutable_type` (`Py_TPFLAGS_IMMUTABLETYPE` for CPython 3.14/3.15 inline-cache specialization) and gate `freelist = 128` behind `#[cfg_attr(not(Py_GIL_DISABLED), ...)]` so free-threaded `3.14t`/`3.15t` builds use thread-local `mimalloc` without global `Mutex` freelist contention.
- **Protocol & batch-driver surface**: `AsgiScope` (including zero-awaitable `body_bytes()`), `AsgiScopeIterator`, `AsgiProtocol` (including `__call__`, `body_bytes()`, and single-call `send_raw_response`), `PyEmptyAwaitable`, `PyValueAwaitable`, `drive_asgi_batch`, `drive_native_batch`, `RsgiScope`, `RsgiHttpProtocol`, `RsgiTransport`, `_RsgiReadyAwaitable`, `drive_rsgi_batch`, `WsgiEnviron`, `WsgiEnvironIterator`, `WsgiInput`, `WsgiFileWrapper`, `_WsgiStartResponse` (including `send_raw_response`), `run_wsgi_callable`, `drive_wsgi_batch`, and `stream_file_chunks` (see [ASGI, RSGI & WSGI Runners](asgi-and-wsgi-runners.md)).
- **Transport & Dekker sleep FFI**:
  - `init_transport(mode="inprocess", ...)`: attaches either the process-wide `BatchBridge` (`inprocess`) or inherited `RingPair::from_env` FDs (`shm`).
  - `get_batch(timeout_ms=100)` and `try_get_batch()`: return a `RequestBatch` wrapping `Arc<BatchSnapshot>` (`Backing::InProcess` or zero-copy `Backing::ShmSlot(SlotView)`), checking `poll_batch_async()` / `try_poll_batch()` before `py.detach` so ready batches return without releasing the GIL, and spinning briefly in user-space before arming Dekker sleep.
  - `get_request_notify_fd()`, `clear_request_notification()`, `prepare_request_sleep()`, and `cancel_request_sleep()`: drive lost-wakeup-free Dekker `SeqCst` `eventfd`/pipe polling on both `RingPair` (`shm`) and single-consumer `BatchBridge` (`inprocess` when `consumer_count <= 1`).
  - `submit_response_raw(request_id, status, headers_raw, body)` and `submit_batch_raw(items)`: accept any contiguous buffer-protocol object (`bytes`, `bytearray`, `memoryview`, including `req.body_view`), write directly into the SHM response ring or `BatchBridge` inline without `py.detach` for payloads `<= 64 KiB`, and coalesce `notify_if_sleeping` to once per batch.
  - `submit_response(resp)`, `submit_results(responses)`, and `submit_raw_response(...)`: object and multi-frame `ResponseSlot` streaming submission helpers.

## Snapshot-backed types & C-API zero-copy bridge (`request.rs`, `headers.rs`, `response.rs`, `msgspec_bridge.rs`)

- [`RequestBatch`](../../../../../src/rs/vertebra-py/src/request.rs)[^src-vertebra-py-src-request-rs] wraps `Arc<BatchSnapshot>` and yields `Request` instances without copying request headers or bodies.
- [`Request`](../../../../../src/rs/vertebra-py/src/request.rs) (`frozen`, `immutable_type`, conditional `freelist = 128`) holds `Arc<BatchSnapshot>` + item index and implements CPython's buffer protocol (`__getbuffer__` / `__releasebuffer__`, typed with PEP 688 `def __buffer__(self, flags: int, /) -> memoryview: ...` in `_vertebra.pyi`). `__getbuffer__` passes borrowed `slf.as_ptr()` to `PyBuffer_FillInfo` (which calls `Py_XNewRef` internally), so `memoryview(req)` and `req.body_view` return an anchored zero-copy read-only `memoryview` pinning `Request` -> `Arc<BatchSnapshot>` safely even after `del req`, and flowing zero-copy into `submit_response_raw` / `submit_batch_raw`.
- [`msgspec_bridge.rs`](../../../../../src/rs/vertebra-py/src/msgspec_bridge.rs)[^src-vertebra-py-src-msgspec-bridge-rs] exposes C-API zero-copy decode, vectorcall row hydration, and gRPC LPM framing primitives on `Request`, `WsgiInput`, `AsgiScope`, and `vertebra._vertebra`:
  - `Request.decode_msgspec(target_type=None, format="json")`, `WsgiInput.decode_msgspec(...)`, and `AsgiScope.decode_msgspec(body_view, target_type=None, format="json")` invoke cached `msgspec.json.Decoder(ty).decode` or `msgspec.msgpack.Decoder(ty).decode` via `PyObject_CallOneArg(decoder_fn, slf.as_ptr())` so `msgspec._core` reads the zero-copy `__getbuffer__` pointer in-place.
  - `Request.decode_pydantic_json(adapter_or_model)` invokes `TypeAdapter.validate_json` / `BaseModel.model_validate_json` directly over the `Request` buffer protocol view.
  - `hydrate_one_vectorcall`, `hydrate_rows_vectorcall`, `instantiate_struct_vectorcall`, and `orm_to_struct_vectorcall` invoke `PyObject_Vectorcall` with stack-allocated/scratch `[*mut PyObject]` argument slices (`PY_VECTORCALL_ARGUMENTS_OFFSET`) to construct `msgspec.Struct`, `@dataclass`, `@attrs.define`, `NamedTuple`, `TypedDict`, or Pydantic v2 instances without intermediate `dict` or `zip()` allocations.
  - `pack_grpc_lpm_frame(buf, compressed=False)` and `decode_grpc_lpm_frames_zero_copy(payload)` prepend and slice 5-byte gRPC Length-Prefixed-Message headers (`[u8 compressed, u32be length]`) in place over `bytearray` and `PyMemoryView_FromMemory` views.
- [`Headers`](../../../../../src/rs/vertebra-py/src/headers.rs)[^src-vertebra-py-src-headers-rs] and [`Response`](../../../../../src/rs/vertebra-py/src/response.rs)[^src-vertebra-py-src-response-rs] allocate byte outputs via `PyBytes::new_with` (PEP 782 `PyBytesWriter` zero-copy initialization on Python 3.15+) and intern common HTTP header names via `pyo3::intern!`.

## Startup handler compilation & batch execution (`worker.py`, `_dispatch.py`, `_loops.py`)

- [`compile_handler_table`](../../../../../src/py/vertebra/server/_dispatch.py)[^src-server-dispatch-py] inspects every registered route once at startup into a frozen [`HandlerSpec`](../../../../../src/py/vertebra/server/_dispatch.py) (`msgspec.Struct, frozen=True, gc=False, cache_hash=True`) carrying `(has_params, is_async, param_names, invoker, sync_to_thread)`. Route decorators (`@app.get(..., sync_to_thread=False)`) default `sync_to_thread` to `False`.
- **Inline vectorized sync execution**: default synchronous native routes (`sync_to_thread=False`) run inline in the batch loop and flush together via a single `submit_batch_raw(raw_responses)` call (`0` `ThreadPoolExecutor` submissions and `0` `Future` allocations). Routes opting into `sync_to_thread=True` execute once on `ThreadPoolExecutor` without duplicate inline execution.
- **Zero-copy buffer serialization, C-API `drive_native_batch` & async snapshot release**: `_serialize_result` accepts `bytes`, `bytearray`, and `memoryview` (`_RAW_BUFFER_TYPES`) directly so `return req.body_view` incurs zero payload copies, and encodes structured objects via `encode_json_bytes`. When an `async def` native handler has only synchronous guards and dependencies, `compile_handler_spec` compiles `_invoke_compiled_sync_to_coro` so parameter extraction executes synchronously and returns the handler's direct coroutine without an outer wrapper frame. On the worker event loop, `dispatch_batch` steps `partitioned.async_native` coroutines in C via `drive_native_batch` (`PyIter_Send`): coroutines returning on the first step (`PYGEN_RETURN`) serialize and flush immediately with **zero** `asyncio.Task` allocations, while suspending coroutines (`PYGEN_NEXT`) promote via `SteppedCoroutine` (`ProtocolFinalizer::Native`) after dropping the local `Request` reference so I/O waits never pin `Arc<BatchSnapshot>`.
- **`O(1)` mount resolution & auto-attached `FastPath` stacking**: `vertebra-http` stamps mounted requests with `MOUNT_HANDLER_BASE | mount_idx` (`0xFFFF_0000_0000_0000 | idx`), allowing `Worker._process_batch` and `partition_batch` to index directly into `mounts[mount_idx]` in `O(1)` without decoding `req.path`. `_instantiate_mount_runners` calls [`attach_framework_fastpath`](../../../../../src/py/vertebra/server/_fastpath.py)[^src-server-fastpath-py] on every mounted `Litestar`, `FastAPI`, `Starlette`, `Flask`, or `Django` sub-application so stacked `app.mount("/prefix", sub_app)` trees automatically execute through each framework's C-API `FastPath` with `mount_prefix` / `root_path` stripping.
- **Unified `LoopBackend` (`auto`, `uvloop`, `winloop`, `rloop`, `asyncio`) & same-thread `add_reader` polling**: [`_loops.py`](../../../../../src/py/vertebra/server/_loops.py)[^src-server-loops-py] exposes `LoopBackend(StrEnum)` and `resolve_loop_backend`, selecting `uvloop` (`>=0.23.0`) on POSIX, `winloop` (`>=0.8.1`) on Windows, opt-in `rloop` (`>=0.5.0`), or stdlib `asyncio` via `create_event_loop` without deprecated `asyncio.AbstractEventLoopPolicy` APIs, and honoring `--loop` / `VERTEBRA_LOOP` and `GilAuditReport` `quarantined_modules`. When `loop_shards == 1` and a request notify FD is active, `Worker._run_inline_event_loop` registers `loop.add_reader(notify_fd, ...)` with Dekker `prepare_request_sleep()` / `cancel_request_sleep()` so polling and coroutine execution share one OS thread with **zero** cross-thread `call_soon_threadsafe` wakeups.

## Database extension boundary

`vertebra._vertebra` owns the canonical Python database exception classes and `ArrowCursor`. The optional `_bigquery` and `_spanner` modules import those classes at runtime and share Rust helpers through `vertebra-db`; they do not link against the core extension's Python class static variables.[^src-db-py-async]

Each extension attaches its own completion drainer to the running event loop through `spawn_rust_db_task_with_attr`. Distinct attribute names keep the separately linked completion queues and PyO3 classes independent when several engines use one loop. Python future cancellation invokes the worker's Tokio abort handle, and the completion path maps worker panics to `TaskPanicError`.[^src-db-py-async]

Arrow-producing extensions use an unregistered SHM region and pass a named `vertebra.ShmCapsule` to the core `ArrowCursor._from_shm_capsule`. The capsule carries a data pointer, length, atomic ownership context, and producer-owned release callbacks. The core cursor validates the capsule and atomically claims its context once; its backing invokes the producer callback when the last owner drops. This keeps the Arrow data in place across extension boundaries.[^src-core-shm-capsule][^src-py-arrow-cursor]

See [ADR-004](../../decisions/adr-004-optional-driver-wheels-and-shm-ownership.md) for the packaging and ownership trade-offs.

Provenance sources: [^src-vertebra-py-src-transport-rs].

[^src-vertebra-py-src-lib-rs]: `src/rs/vertebra-py/src/lib.rs` — Rust implementation module (src/rs/vertebra-py/src/lib.rs).
[^src-vertebra-py-src-transport-rs]: `src/rs/vertebra-py/src/transport.rs` — Rust implementation module (src/rs/vertebra-py/src/transport.rs).
[^src-vertebra-py-src-request-rs]: `src/rs/vertebra-py/src/request.rs` — Rust implementation module (src/rs/vertebra-py/src/request.rs).
[^src-vertebra-py-src-headers-rs]: `src/rs/vertebra-py/src/headers.rs` — Rust implementation module (src/rs/vertebra-py/src/headers.rs).
[^src-vertebra-py-src-response-rs]: `src/rs/vertebra-py/src/response.rs` — Rust implementation module (src/rs/vertebra-py/src/response.rs).
[^src-vertebra-py-src-msgspec-bridge-rs]: `src/rs/vertebra-py/src/msgspec_bridge.rs` — Rust implementation module (src/rs/vertebra-py/src/msgspec_bridge.rs).
[^src-vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Python implementation module (src/py/vertebra/_vertebra.pyi).
[^src-bundler-pyi]: `src/py/vertebra/_bundler.pyi` — Python implementation module (src/py/vertebra/_bundler.pyi).
[^src-storage-pyi]: `src/py/vertebra/_storage.pyi` — Python implementation module (src/py/vertebra/_storage.pyi).
[^src-bigquery-pyi]: `src/py/vertebra/_bigquery.pyi` — Python implementation module (src/py/vertebra/_bigquery.pyi).
[^src-spanner-pyi]: `src/py/vertebra/_spanner.pyi` — Python implementation module (src/py/vertebra/_spanner.pyi).
[^src-server-worker-py]: `src/py/vertebra/server/worker.py` — Python implementation module (src/py/vertebra/server/worker.py).
[^src-server-dispatch-py]: `src/py/vertebra/server/_dispatch.py` — Python implementation module (src/py/vertebra/server/_dispatch.py).
[^src-server-loops-py]: `src/py/vertebra/server/_loops.py` — Python implementation module (src/py/vertebra/server/_loops.py).
[^src-server-fastpath-py]: `src/py/vertebra/server/_fastpath.py` — Python implementation module (src/py/vertebra/server/_fastpath.py).

[^src-db-py-async]: `src/rs/vertebra-db/src/py_async.rs` — Per-extension completion queues and SHM capsule construction.

[^src-core-shm-capsule]: `src/rs/vertebra-core/src/shm.rs` — C ABI capsule and shared-memory mapping ownership.

[^src-py-arrow-cursor]: `src/rs/vertebra-py/src/db.rs` — ArrowCursor capsule adoption and release.
