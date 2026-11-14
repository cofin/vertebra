---
type: Guide
title: PyO3 FFI & Vectorized Batch Dispatch
description: The vertebra._vertebra module surface, anchored zero-copy Request/Headers/Response/RequestBatch
  types, FastPathProvider dispatch plans, and Worker batch execution.
status: stable
tags:
- architecture
- worker
- pyo3
- ffi
- batch-dispatch
generated:
  by: flow/0.26.0
  at: '2026-11-04T04:33:00Z'
verified:
- by: agent:flow-executor
  at: '2026-11-13T06:06:00Z'
  commit: 05d88d00
- by: agent:flow-learn
  at: '2026-11-08T18:26:31.591504Z'
  commit: 067fa901
sources:
- id: src-vertebra-py-src-lib-rs
  resource: src/rs/vertebra-py/src/lib.rs
  title: Rust implementation module (src/rs/vertebra-py/src/lib.rs)
- id: src-vertebra-py-src-transport-rs
  resource: src/rs/vertebra-py/src/transport.rs
  title: Rust implementation module (src/rs/vertebra-py/src/transport.rs)
- id: src-vertebra-py-src-request-rs
  resource: src/rs/vertebra-py/src/request.rs
  title: Rust implementation module (src/rs/vertebra-py/src/request.rs)
- id: src-vertebra-py-src-headers-rs
  resource: src/rs/vertebra-py/src/headers.rs
  title: Rust implementation module (src/rs/vertebra-py/src/headers.rs)
- id: src-vertebra-py-src-response-rs
  resource: src/rs/vertebra-py/src/response.rs
  title: Rust implementation module (src/rs/vertebra-py/src/response.rs)
- id: src-vertebra-py-src-msgspec-bridge-rs
  resource: src/rs/vertebra-py/src/msgspec_bridge.rs
  title: Rust implementation module (src/rs/vertebra-py/src/msgspec_bridge.rs)
- id: src-vertebra-py-src-gcp-auth-rs
  resource: src/rs/vertebra-py/src/gcp_auth.rs
  title: Native GCP token cache and HTTP/2 post exports in vertebra._vertebra
- id: src-vertebra-py-src-gcp-identity-rs
  resource: src/rs/vertebra-py/src/gcp_identity.rs
  title: Native GCP OIDC ID token cache exports in vertebra._vertebra
- id: src-vertebra-py-src-errors-rs
  resource: src/rs/vertebra-py/src/errors.rs
  title: Canonical DatabaseError and GoogleCloudError exception hierarchy
- id: src-vertebra-py-src-vector-rs
  resource: src/rs/vertebra-py/src/vector.rs
  title: Zero-copy VectorView, VectorBatchView, VectorCache, and StaticEmbedderCore
- id: src-vertebra-py-src-text-rs
  resource: src/rs/vertebra-py/src/text.rs
  title: Native text, identifier, humanize, and UUID/NanoID primitives
- id: src-vertebra-pyi
  resource: src/py/vertebra/_vertebra.pyi
  title: Python implementation module (src/py/vertebra/_vertebra.pyi)
- id: src-bundler-pyi
  resource: src/py/vertebra/_bundler.pyi
  title: Python implementation module (src/py/vertebra/_bundler.pyi)
- id: src-storage-pyi
  resource: src/py/vertebra/_storage.pyi
  title: Python implementation module (src/py/vertebra/_storage.pyi)
- id: src-bigquery-pyi
  resource: src/py/vertebra/_bigquery.pyi
  title: Python implementation module (src/py/vertebra/_bigquery.pyi)
- id: src-spanner-pyi
  resource: src/py/vertebra/_spanner.pyi
  title: Python implementation module (src/py/vertebra/_spanner.pyi)
- id: src-postgres-pyi
  resource: src/py/vertebra/_postgres.pyi
  title: Python stub module (src/py/vertebra/_postgres.pyi)
- id: src-vertebra-postgres-src-lib-rs
  resource: src/rs/vertebra-postgres/src/lib.rs
  title: Native PostgreSQL PyO3 extension exports and gil_used = false
- id: src-vertebra-postgres-src-replication-mod-rs
  resource: src/rs/vertebra-postgres/src/replication/mod.rs
  title: AsyncPostgresReplication bridge, future cancellation, and delivery gating
- id: src-server-worker-py
  resource: src/py/vertebra/server/worker.py
  title: Python implementation module (src/py/vertebra/server/worker.py)
- id: src-server-dispatch-py
  resource: src/py/vertebra/server/_dispatch.py
  title: Python implementation module (src/py/vertebra/server/_dispatch.py)
- id: src-server-loops-py
  resource: src/py/vertebra/server/_loops.py
  title: Python implementation module (src/py/vertebra/server/_loops.py)
- id: src-server-fastpath-py
  resource: src/py/vertebra/server/_fastpath.py
  title: Python implementation module (src/py/vertebra/server/_fastpath.py)
- id: src-server-fastpath-core-py
  resource: src/py/vertebra/server/_fastpath_core.py
  title: FastPathProvider protocol and DirectPlan route bypass contracts
- id: src-utils-zero-copy-py
  resource: src/py/vertebra/utils/serializers/_zero_copy.py
  title: Direct vectorcall and gRPC LPM native bindings
- id: src-db-py-async
  resource: src/rs/vertebra-db/src/py_async.rs
  title: Per-extension completion queues and SHM capsule construction
- id: src-core-shm-capsule
  resource: src/rs/vertebra-core/src/shm.rs
  title: C ABI capsule and shared-memory mapping ownership
- id: src-py-arrow-cursor
  resource: src/rs/vertebra-py/src/db.rs
  title: ArrowCursor capsule adoption, dialers, and native DB exports
- id: src-storage-native-completion
  resource: src/rs/vertebra-storage/src/lib.rs::spawn_rust_task
  title: Native storage completion and cancellation
- id: src-storage-core-gcp
  resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache
  title: Canonical GCP token authority
- id: src-storage-python-facade
  resource: src/py/vertebra/storage/_store.py
  title: Storage facade and shared cache handles
- id: src-storage-codec-bridge
  resource: src/py/vertebra/storage/_arrow.py::_CodecBridge
  title: Caller-loop I/O and worker codec ownership
---

# PyO3 FFI & Vectorized Batch Dispatch

<!-- truth: start -->
- Extensions (`vertebra-py`, `vertebra-bundler`, `vertebra-storage`, `vertebra-bigquery`, `vertebra-duckdb`, `vertebra-mssql`, `vertebra-mysql`, `vertebra-oracle`, `vertebra-postgres`, `vertebra-spanner`) compile with `#[pymodule(gil_used = false)]` and stub coverage in `src/py/vertebra/*.pyi`.
- `vertebra._vertebra` anchors `Request`, `Headers`, `Response`, and `RequestBatch` to `Arc<BatchSnapshot>` with PEP 688 zero-copy `memoryview` access and progressive `submit_stream_frame` / `max_stream_frame_payload` streaming.
- `Worker` compiles routes into `BatchDispatchPlan` and `HandlerSpec` tables, flushes inline sync handlers via `submit_batch_raw`, streams tuple iterables progressively, and drives ASGI/RSGI/WSGI mounts directly from Rust.
- Optional database extensions (`_bigquery`, `_duckdb`, `_mssql`, `_mysql`, `_oracle`, `_postgres`, `_spanner`) isolate per-loop completion drainers (`spawn_rust_db_task_with_attr` / `_and_future`), guard ordered replication leases with `PendingDeliveryGuard`, and transfer Arrow tables via `vertebra.ShmCapsule`.
<!-- truth: end -->

[`vertebra-py`](../../../../../src/rs/vertebra-py/src/lib.rs)[^src-vertebra-py-src-lib-rs] builds `vertebra._vertebra` (`#[pymodule(gil_used = false)]`, [`_vertebra.pyi`](../../../../../src/py/vertebra/_vertebra.pyi)[^src-vertebra-pyi]) alongside [`_bundler.pyi`](../../../../../src/py/vertebra/_bundler.pyi)[^src-bundler-pyi], [`_storage.pyi`](../../../../../src/py/vertebra/_storage.pyi)[^src-storage-pyi], [`_bigquery.pyi`](../../../../../src/py/vertebra/_bigquery.pyi)[^src-bigquery-pyi], `_duckdb.pyi`, `_mssql.pyi`, `_mysql.pyi`, `_oracle.pyi`, [`_postgres.pyi`](../../../../../src/py/vertebra/_postgres.pyi)[^src-postgres-pyi] ([`vertebra-postgres`](../../../../../src/rs/vertebra-postgres/src/lib.rs)[^src-vertebra-postgres-src-lib-rs]), and [`_spanner.pyi`](../../../../../src/py/vertebra/_spanner.pyi)[^src-spanner-pyi]. [`Worker`](../../../../../src/py/vertebra/server/worker.py)[^src-server-worker-py] pulls `RequestBatch` snapshots across FFI, executes inline sync, WSGI (`drive_wsgi_batch`), and single-thread event-loop async/ASGI/RSGI batches (`drive_native_batch`, `drive_asgi_batch`, `drive_rsgi_batch`), and flushes responses to Rust.

## Module surface (`lib.rs`, `transport.rs`)

- **Runtime flags & hot classes**: `__version__`, `FREE_THREADED`, `GIL_USED` (`False`), `Request`, `Headers`, `HeadersIterator`, `Response`, `RequestBatch`, and `RequestBatchIterator`. Non-subclassed `#[pyclass]` types declare `immutable_type` (`Py_TPFLAGS_IMMUTABLETYPE`) and gate `freelist = 128` behind `#[cfg_attr(not(Py_GIL_DISABLED), ...)]` to avoid free-threaded global mutex contention.
- **Protocol & stream drivers**: `AsgiScope`, `AsgiProtocol` (`body_bytes()`, `send_raw_response`, progressive `submit_stream_frame`), `PyEmptyAwaitable`, `PyValueAwaitable`, `drive_asgi_batch`, `drive_native_batch`, `RsgiScope`, `RsgiHttpProtocol`, `RsgiTransport`, `_RsgiReadyAwaitable`, `drive_rsgi_batch`, `WsgiEnviron`, `WsgiInput`, `_WsgiStartResponse`, `drive_wsgi_batch`, and `stream_file_chunks` (see [ASGI, RSGI & WSGI Runners](asgi-and-wsgi-runners.md)).
- **Transport & Dekker sleep FFI**: `init_transport(mode="inprocess", ...)` attaches `BatchBridge` or inherited `RingPair::from_env` FDs. `get_batch` and `try_get_batch` return zero-copy `RequestBatch` views checking `poll_batch_async` / `try_poll_batch` before `py.detach`. `get_request_notify_fd`, `clear_request_notification`, `prepare_request_sleep`, and `cancel_request_sleep` drive Dekker `SeqCst` wakeups. `submit_response_raw` and `submit_batch_raw` accept contiguous buffer-protocol objects inline for `<= 64 KiB` payloads; `submit_stream_frame` and `max_stream_frame_payload` manage chunked `VRSF` delivery (alongside `submit_response`, `submit_results`, `submit_raw_response`).
- **Database, GCP auth/identity, vectors & text**: [`gcp_auth.rs`](../../../../../src/rs/vertebra-py/src/gcp_auth.rs)[^src-vertebra-py-src-gcp-auth-rs] & [`gcp_identity.rs`](../../../../../src/rs/vertebra-py/src/gcp_identity.rs)[^src-vertebra-py-src-gcp-identity-rs] export `SyncGcpTokenCache`, `AsyncGcpTokenCache`, `GcpTokenSnapshot`, `SyncGcpIdTokenCache`, `AsyncGcpIdTokenCache`, `GcpIdTokenSnapshot`, and `gcp_http2_post_{sync,async}`; [`db.rs`](../../../../../src/rs/vertebra-py/src/db.rs)[^src-py-arrow-cursor] & [`errors.rs`](../../../../../src/rs/vertebra-py/src/errors.rs)[^src-vertebra-py-src-errors-rs] export `RustDbTaskAwaitable`, `TaskPanicError`, canonical `DatabaseError`/`GoogleCloudError` subclasses, dialers, `ArrowCursor`, and `LeaseBroker`/`StatementCache`; [`vector.rs`](../../../../../src/rs/vertebra-py/src/vector.rs)[^src-vertebra-py-src-vector-rs] & [`text.rs`](../../../../../src/rs/vertebra-py/src/text.rs)[^src-vertebra-py-src-text-rs] export `VectorView`/`VectorCache`/`StaticEmbedderCore`, `encode_gemini_embed_contents_grpc_frame`, `decode_gemini_batch_embed_contents_grpc_frame`, and fail-loud `getrandom` UUID/NanoID primitives.

## Snapshot types & C-API zero-copy bridge (`request.rs`, `msgspec_bridge.rs`)

- [`RequestBatch`](../../../../../src/rs/vertebra-py/src/request.rs)[^src-vertebra-py-src-request-rs] and `Request` share `Arc<BatchSnapshot>` without copying payloads; `Request.__getbuffer__` pins `slf.as_ptr()` in `PyBuffer_FillInfo` so `memoryview(req)` and `req.body_view` stay valid past `del req`.
- [`msgspec_bridge.rs`](../../../../../src/rs/vertebra-py/src/msgspec_bridge.rs)[^src-vertebra-py-src-msgspec-bridge-rs] provides `Request.decode_msgspec` / `WsgiInput.decode_msgspec` / `AsgiScope.decode_msgspec`, `Request.decode_pydantic_json`, `hydrate_{one,rows}_vectorcall`, `instantiate_struct_vectorcall`, `orm_to_struct_vectorcall`, and `pack_grpc_lpm_frame` / `decode_grpc_lpm_frames_zero_copy`. [`_zero_copy.py`](../../../../../src/py/vertebra/utils/serializers/_zero_copy.py)[^src-utils-zero-copy-py] imports those native primitives directly from `vertebra._vertebra` with zero pure-Python fallbacks.
- [`Headers`](../../../../../src/rs/vertebra-py/src/headers.rs)[^src-vertebra-py-src-headers-rs] and [`Response`](../../../../../src/rs/vertebra-py/src/response.rs)[^src-vertebra-py-src-response-rs] allocate via `PyBytes::new_with` and intern common header names via `pyo3::intern!`.

## Startup compilation & batch execution (`worker.py`, `_dispatch.py`, `_loops.py`)

- [`compile_handler_table`](../../../../../src/py/vertebra/server/_dispatch.py)[^src-server-dispatch-py] compiles routes into frozen `HandlerSpec` entries (`sync_to_thread=False` default runs inline and flushes via `submit_batch_raw`). `_serialize_result` accepts `_RAW_BUFFER_TYPES` copy-free; returning `(status, headers, AsyncIterable | Iterable)` streams non-empty chunks progressively over `submit_stream_frame` / `max_stream_frame_payload`.
- `drive_native_batch` steps `async def` handlers in C via `PyIter_Send`, flushing `PYGEN_RETURN` with zero `asyncio.Task` allocations and promoting `PYGEN_NEXT` coroutines after dropping local `Request` pins.
- [`_fastpath_core.py`](../../../../../src/py/vertebra/server/_fastpath_core.py)[^src-server-fastpath-core-py] defines `CompiledRouteBase`, canonical `__vertebra_fastpath__` attachment helpers (`get_attached_fastpath` / `set_attached_fastpath`), `FastPathProvider`, and `DirectPlan`; [`attach_framework_fastpath`](../../../../../src/py/vertebra/server/_fastpath.py)[^src-server-fastpath-py] wraps mounted Litestar, FastAPI, Starlette, Flask, and Django apps in `O(1)` (`MOUNT_HANDLER_BASE | mount_idx`).
- [`_loops.py`](../../../../../src/py/vertebra/server/_loops.py)[^src-server-loops-py] resolves `LoopBackend` (`uvloop`, `winloop`, `rloop`, `zuvloop`, `asyncio`) via `create_event_loop`; single-loop workers register `loop.add_reader(notify_fd, ...)` with Dekker guards.

## Database & storage extension boundaries

- `vertebra._vertebra` owns canonical database exceptions and `ArrowCursor`; optional drivers import them at runtime, drain per-loop queues through `spawn_rust_db_task_with_attr`[^src-db-py-async], and adopt unregistered `ShmCapsule`[^src-core-shm-capsule][^src-py-arrow-cursor] mappings zero-copy.
- [`AsyncPostgresReplication`](../../../../../src/rs/vertebra-postgres/src/replication/mod.rs)[^src-vertebra-postgres-src-replication-mod-rs] uses `spawn_rust_db_task_with_attr_and_future` to re-check `py_fut.cancelled()` under the GIL before `delivery.publish()`, guarding pre-publish leases via `PendingDeliveryGuard` / `ReplicationEventDelivery`[^src-db-py-async].
- `vertebra._storage` owns `RustTaskAwaitable`[^src-storage-native-completion], shares `GcpTokenCache`[^src-storage-core-gcp][^src-storage-python-facade], and coordinates synchronous PyArrow codec callbacks via `_CodecBridge`[^src-storage-codec-bridge]. See [ADR-004](../../decisions/adr-004-optional-driver-wheels-and-shm-ownership.md).

Provenance sources: [^src-vertebra-py-src-transport-rs].

[^src-vertebra-py-src-lib-rs]: `src/rs/vertebra-py/src/lib.rs` — Rust implementation module.
[^src-vertebra-py-src-transport-rs]: `src/rs/vertebra-py/src/transport.rs` — Rust transport & stream FFI.
[^src-vertebra-py-src-request-rs]: `src/rs/vertebra-py/src/request.rs` — Zero-copy `RequestBatch` and `Request`.
[^src-vertebra-py-src-headers-rs]: `src/rs/vertebra-py/src/headers.rs` — `Headers` native wrapper.
[^src-vertebra-py-src-response-rs]: `src/rs/vertebra-py/src/response.rs` — `Response` native wrapper.
[^src-vertebra-py-src-msgspec-bridge-rs]: `src/rs/vertebra-py/src/msgspec_bridge.rs` — C-API decode, vectorcall & LPM codecs.
[^src-vertebra-py-src-gcp-auth-rs]: `src/rs/vertebra-py/src/gcp_auth.rs` — Native GCP token cache & HTTP/2 post.
[^src-vertebra-py-src-gcp-identity-rs]: `src/rs/vertebra-py/src/gcp_identity.rs` — Native GCP OIDC ID token cache exports.
[^src-vertebra-py-src-errors-rs]: `src/rs/vertebra-py/src/errors.rs` — Canonical DB & Google Cloud exception classes.
[^src-vertebra-py-src-vector-rs]: `src/rs/vertebra-py/src/vector.rs` — Vector views, cache & embedder core.
[^src-vertebra-py-src-text-rs]: `src/rs/vertebra-py/src/text.rs` — Native text & fail-loud UUID/NanoID bindings.
[^src-vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Core extension type stubs.
[^src-bundler-pyi]: `src/py/vertebra/_bundler.pyi` — Bundler extension type stubs.
[^src-storage-pyi]: `src/py/vertebra/_storage.pyi` — Storage extension type stubs.
[^src-bigquery-pyi]: `src/py/vertebra/_bigquery.pyi` — BigQuery extension type stubs.
[^src-spanner-pyi]: `src/py/vertebra/_spanner.pyi` — Spanner extension type stubs.
[^src-postgres-pyi]: `src/py/vertebra/_postgres.pyi` — PostgreSQL extension type stubs.
[^src-vertebra-postgres-src-lib-rs]: `src/rs/vertebra-postgres/src/lib.rs` — PostgreSQL extension exports & `gil_used = false`.
[^src-vertebra-postgres-src-replication-mod-rs]: `src/rs/vertebra-postgres/src/replication/mod.rs` — `AsyncPostgresReplication` cancel-safe delivery bridge.
[^src-server-worker-py]: `src/py/vertebra/server/worker.py` — Worker batch loop & lifespan runner.
[^src-server-dispatch-py]: `src/py/vertebra/server/_dispatch.py` — Route compilation & streaming dispatch.
[^src-server-loops-py]: `src/py/vertebra/server/_loops.py` — Unified event loop backend resolution.
[^src-server-fastpath-py]: `src/py/vertebra/server/_fastpath.py` — Framework FastPath attachment.
[^src-server-fastpath-core-py]: `src/py/vertebra/server/_fastpath_core.py` — `CompiledRouteBase`, `__vertebra_fastpath__` helpers, `DirectPlan`.
[^src-utils-zero-copy-py]: `src/py/vertebra/utils/serializers/_zero_copy.py` — Direct `vertebra._vertebra` vectorcall/LPM bindings.
[^src-db-py-async]: `src/rs/vertebra-db/src/py_async.rs` — Per-extension completion queues & SHM capsule construction.
[^src-core-shm-capsule]: `src/rs/vertebra-core/src/shm.rs` — C ABI capsule & shared-memory mapping ownership.
[^src-py-arrow-cursor]: `src/rs/vertebra-py/src/db.rs` — `ArrowCursor` capsule adoption & dialers.
[^src-storage-native-completion]: `src/rs/vertebra-storage/src/lib.rs::spawn_rust_task` — Native storage completion & cancellation.
[^src-storage-core-gcp]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache` — Canonical GCP token authority.
[^src-storage-python-facade]: `src/py/vertebra/storage/_store.py` — Storage facade & shared cache handles.
[^src-storage-codec-bridge]: `src/py/vertebra/storage/_arrow.py::_CodecBridge` — Caller-loop I/O & worker codec ownership.
