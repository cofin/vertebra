---
type: Gotcha
title: "PyO3 FFI, Embedded Globals & Python Runtime Gotchas"
description: "Runtime pitfalls in embedded PyO3 module initialization, free-threaded 3.14t/3.15t freelists vs cyclic GC (PyGC_Head), writable PyBuffer copies, ArrowCursor GIL detachment, worker-scoped executor teardown, coroutine stepping, _vertebra.pyi stubs, and cross-extension Arrow capsules."
status: stable
tags:
  - gotchas
  - pyo3
  - asgi
  - python
  - free-threaded
  - litestar
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-27T22:10:00Z"
    commit: f3b624a2
  - by: agent:flow-executor
    at: "2026-11-17T04:20:00Z"
    commit: 5a833832
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
  - by: agent:codex
    at: "2026-11-05T06:09:26Z"
    commit: 80ff8a65e43a2bcef9e48a7763e89a7352118232
sources:
  - id: src-vertebra-http-src-main-rs
    resource: src/rs/vertebra-http/src/main.rs
    title: "Embedded interpreter init and inittab registration"
  - id: src-vertebra-py-src-lib-rs
    resource: src/rs/vertebra-py/src/lib.rs
    title: "Core PyO3 extension module exports and gil_used = false"
  - id: src-vertebra-py-src-request-rs
    resource: src/rs/vertebra-py/src/request.rs
    title: "Zero-copy Request buffer protocol and PyBuffer_FillInfo refcounting"
  - id: src-vertebra-py-src-transport-rs
    resource: src/rs/vertebra-py/src/transport.rs
    title: "Writable and non-contiguous PyBuffer copying before py.detach and ShmResponseLock"
  - id: src-vertebra-py-src-asgi-rs
    resource: src/rs/vertebra-py/src/asgi.rs
    title: "SteppedCoroutine cyclic GC slots without freelist"
  - id: src-vertebra-py-src-msgspec-bridge-rs
    resource: src/rs/vertebra-py/src/msgspec_bridge.rs
    title: "Weakref-keyed decoder caches and CachedCallableBox GC traversal"
  - id: src-vertebra-py-src-rsgi-http-rs
    resource: src/rs/vertebra-py/src/rsgi/http.rs
    title: "Frozen RsgiReadyAwaitable singleton and PyIter_Send stepping"
  - id: src-vertebra-py-src-wsgi-runner-rs
    resource: src/rs/vertebra-py/src/wsgi/runner.rs
    title: "PEP 3333 _WsgiStartResponse called vs headers_sent state machine"
  - id: src-vertebra-py-src-wsgi-pool-rs
    resource: src/rs/vertebra-py/src/wsgi/pool.rs
    title: "WsgiExecutor GilSafePyObject drop safety and GIL-detached shutdown"
  - id: src-server-gil-audit-py
    resource: src/py/vertebra/server/_gil_audit.py
    title: "Free-threaded extension GIL audit enforcement"
  - id: src-server-asgi-runner-py
    resource: src/py/vertebra/server/_asgi/_runner.py
    title: "Task-free ASGI batch stepping and task promotion"
  - id: src-server-dispatch-py
    resource: src/py/vertebra/server/_dispatch.py
    title: "Worker-scoped ThreadPoolExecutor lifecycle and shutdown"
  - id: src-server-worker-py
    resource: src/py/vertebra/server/worker.py
    title: "Single-root ASGI, RSGI, and WSGI batch routing and executor teardown in Worker"
  - id: src-vertebra-pyi
    resource: src/py/vertebra/_vertebra.pyi
    title: "Native _vertebra type stub rules for Buffer and non-instantiable classes"
  - id: src-supervisor-supervisor-py
    resource: src/py/vertebra/supervisor/_supervisor.py
    title: "Server binary mtime resolution across target directories"
  - id: src-extensions-litestar-plugin-py
    resource: src/py/vertebra/extensions/litestar/plugin.py
    title: "Guarded Litestar compression offload activation"
  - id: src-extensions-litestar-fastpath-py
    resource: src/py/vertebra/extensions/litestar/fastpath.py
    title: "Litestar v2/v3 ownership_layers and fast-path disqualifiers"
  - id: src-utils-text-py
    resource: src/py/vertebra/utils/text.py
    title: "Direct native text primitive imports from vertebra._vertebra"
  - id: src-utils-init-py
    resource: src/py/vertebra/utils/__init__.py
    title: "Thin vertebra.utils package contract"
  - id: src-embeddings-google-py
    resource: src/py/vertebra/embeddings/_google.py
    title: "Direct GCP token cache and HTTP/2 imports from vertebra._vertebra"
  - id: src-tools-docs-generate-cli-docs-py
    resource: tools/docs/generate_cli_docs.py
    title: "Doc-only native module stubbing in _make_stub_getattr"
  - id: src-pyproject-toml
    resource: pyproject.toml
    title: "Root maturin, coverage, and optional dependency configuration"
  - id: src-provider-bridge
    resource: src/py/vertebra/agent/tools/__init__.py
    title: "Owned tool invocation snapshots and recursive type-sensitive approval"
  - id: src-stack-implementation
    resource: src/py/vertebra/extensions/sqlspec/_driver.py
    title: "Prepared SQLSpec result metadata and native bind placeholders"
  - id: src-channel-implementation
    resource: src/py/vertebra/server/_channels.py
    title: "Non-null shared-feed identity and independent output channels"
  - id: src-core-py-async
    resource: src/rs/vertebra-core/src/py_async.rs
    title: "Shared PyO3 Tokio completion bridge with per-extension drainer_attr"
  - id: src-db-arrow-shm
    resource: src/rs/vertebra-db/src/arrow_shm.rs
    title: "Unregistered Arrow SHM writers for cross-extension transfer"
  - id: src-db-py-async
    resource: src/rs/vertebra-db/src/py_async.rs
    title: "Per-extension completion drainers and capsule destructors"
  - id: src-core-shm-capsule
    resource: src/rs/vertebra-core/src/shm.rs
    title: "Named vertebra.ShmCapsule ABI and unlink vs mapping lifetime"
  - id: src-py-arrow-cursor
    resource: src/rs/vertebra-py/src/db.rs
    title: "Single-consumer ArrowCursor capsule adoption, release, and SHM read GIL detachment"
  - id: src-vertebra-postgres-src-lib-rs
    resource: src/rs/vertebra-postgres/src/lib.rs
    title: "Native PostgreSQL PyO3 extension exports and gil_used = false"
  - id: src-vertebra-postgres-src-replication-mod-rs
    resource: src/rs/vertebra-postgres/src/replication/mod.rs
    title: "Replication future cancellation and RAII delivery drop ordering"
  - id: src-vertebra-postgres-src-replication-supervisor-rs
    resource: src/rs/vertebra-postgres/src/replication/supervisor.rs
    title: "PostgreSQL replication supervisor and ReplicationEventDelivery leases"
  - id: src-vertebra-postgres-src-async-client-rs
    resource: src/rs/vertebra-postgres/src/async_client.rs
    title: "PostgreSQL async listener PendingDeliveryGuard cancellation bridge"
---

# PyO3 FFI, Embedded Globals & Python Runtime Gotchas

<!-- truth: start -->
- Declare `#[pymodule(gil_used = false)]` on all 13 PyO3 extensions (`vertebra-py`, `vertebra-bundler`, `vertebra-storage`, `vertebra-bigquery`, `vertebra-duckdb`, `vertebra-mssql`, `vertebra-mysql`, `vertebra-oracle`, `vertebra-postgres`, `vertebra-redis`, `vertebra-sqlite`, `vertebra-spanner`, `vertebra-valkey`), omit `#[pyclass(freelist = N)]` under `Py_GIL_DISABLED` (`3.14t`/`3.15t`) and on cyclic-GC (`PyGC_Head`) classes (`_SteppedCoroutine`, `CachedCallableBox`) that implement `__traverse__`/`__clear__`, and register embedded `_vertebra` via `append_to_inittab!` plus `sys.modules` aliasing.
- Pass borrowed `slf.as_ptr()` (never `into_ptr()`) to `PyBuffer_FillInfo`, copy writable (`!readonly()`) or non-contiguous `PyBuffer<u8>` views via `to_vec(py)` before `py.detach`, run `ArrowCursor::read_ipc_from_shm` inside `py.detach`, serialize multi-thread SHM response writes via `ShmResponseLock`, wrap background-thread `Py<PyAny>` handles in `GilSafePyObject`, shut down the worker-scoped `ThreadPoolExecutor` and Rust `WsgiExecutor` (detaching the GIL across joins) on worker exit, keep `_vertebra.pyi` constructors/`Buffer` types synchronized (`stubtest` + `pyright`), run `maturin develop` for `vertebra-py` from repository root without `-m`, and keep `tools/docs/generate_cli_docs.py` `fallback_none_attrs` synchronized with `--only-group doc` builds.
- Transfer cross-extension Arrow buffers through unregistered `ShmRegion` writers, `vertebra.ShmCapsule` single-claim adoption, and per-extension `vertebra-core::py_async` drainer attributes; gate ordered stream commits with `spawn_rust_db_task_with_attr_and_future` (`ReplicationInFlightGuard` / `ReplicationEventDelivery`) and listener delivery with `spawn_rust_db_task_with_attr_and_cancel` (`PendingDeliveryGuard`) so cancelled futures retire without advancing stream state.
<!-- truth: end -->

## 1. Embedded Split-Globals & Free-Threaded `3.14t`/`3.15t` Discipline

- **Dual `.so` Split-Globals in `Topology::Embedded`**: `vertebra-server` statically links `vertebra-py` (`_vertebra`), whereas `.venv` holds a separate `_vertebra.cpython-*.so` with its own uninitialized `static BRIDGE: OnceLock<BatchBridge>`. [`main.rs`](../../../../src/rs/vertebra-http/src/main.rs)[^src-vertebra-http-src-main-rs] must call `pyo3::append_to_inittab!(pymodule_init)` before the first `Python::attach` and alias `sys.modules["vertebra._vertebra"] = sys.modules["_vertebra"]` before importing Vertebra Python modules.
- **`#[pymodule(gil_used = false)]`, Conditional `freelist` & Cyclic GC (`PyGC_Head`)**: All 13 PyO3 extensions (`vertebra-py`, `vertebra-bundler`, `vertebra-storage`, `vertebra-bigquery`, `vertebra-duckdb`, `vertebra-mssql`, `vertebra-mysql`, `vertebra-oracle`, `vertebra-postgres`[^src-vertebra-postgres-src-lib-rs], `vertebra-redis`, `vertebra-sqlite`, `vertebra-spanner`, `vertebra-valkey`) must declare `#[pymodule(gil_used = false)]` ([`vertebra-py/src/lib.rs`](../../../../src/rs/vertebra-py/src/lib.rs)[^src-vertebra-py-src-lib-rs]); [`GilAuditReport.audit()`](../../../../src/py/vertebra/server/_gil_audit.py)[^src-server-gil-audit-py] raises `RuntimeError` when `VERTEBRA_STRICT_FREE_THREADING=1`. Because PyO3 `0.30.1` guards `freelist` with a global `Mutex` under `Py_GIL_DISABLED`, gate `freelist = 128` behind `#[cfg_attr(not(Py_GIL_DISABLED), ...)]`. Never add `freelist = N` to cyclic-GC (`PyGC_Head`) classes that implement `__traverse__` and `__clear__`: [`SteppedCoroutine`](../../../../src/rs/vertebra-py/src/asgi.rs)[^src-vertebra-py-src-asgi-rs] (`_SteppedCoroutine`, holding `coro`, `first_yielded`, `finalizer`, and `self_task`) and [`CachedCallableBox`](../../../../src/rs/vertebra-py/src/msgspec_bridge.rs)[^src-vertebra-py-src-msgspec-bridge-rs] omit `freelist = 128` because PyO3 rejects `freelist` on GC-tracked types, and [`msgspec_bridge.rs`](../../../../src/rs/vertebra-py/src/msgspec_bridge.rs)[^src-vertebra-py-src-msgspec-bridge-rs] pairs `target_ref`/`box_ref` `PyWeakrefReference` entries rather than raw pointer keys so dynamic Python types cannot alias freed memory or leak across GC cycles.
- **`RsgiReadyAwaitable` Must Be `frozen`**: In [`rsgi/http.rs`](../../../../src/rs/vertebra-py/src/rsgi/http.rs)[^src-vertebra-py-src-rsgi-http-rs], `_RsgiReadyAwaitable` is shared across concurrent coroutines on `3.14t`/`3.15t`; `__await__` and `__next__` must take `&self` (`frozen`) to avoid `PyBorrowMutError`.

## 2. Coroutine Stepping, PEP 3333 State, Buffer Refcounts, Executor Lifecycle & Stubs

- **Task-Free `drive_asgi_batch` vs `asyncio.current_task()`**: `drive_asgi_batch` steps each coroutine's synchronous prefix via `PyIter_Send` before allocating an `asyncio.Task` ([`_asgi/_runner.py`](../../../../src/py/vertebra/server/_asgi/_runner.py)[^src-server-asgi-runner-py]), so middleware observes `asyncio.current_task() is None` until the first suspension point. [`Worker._process_batch`](../../../../src/py/vertebra/server/worker.py)[^src-server-worker-py] routes single-root `"/"` ASGI, RSGI, and WSGI mounts directly through `drive_asgi_batch`, `drive_rsgi_batch`, and `drive_wsgi_batch(single_root_wsgi.app, batch, root_path="")` rather than `partition_batch`.
- **Worker-Scoped `ThreadPoolExecutor` & Rust `WsgiExecutor` Lifecycle**: Never allocate a fresh `ThreadPoolExecutor` per batch; [`_dispatch.py`](../../../../src/py/vertebra/server/_dispatch.py)[^src-server-dispatch-py] caches one lazily initialized worker-scoped pool (`get_worker_executor(min_workers=32)`, `"vertebra-worker"` threads) under `_EXECUTOR_LOCK`. On worker exit, [`Worker.run()`](../../../../src/py/vertebra/server/worker.py)[^src-server-worker-py] shuts down any per-worker sync `executor`, calls `shutdown_worker_executor(wait=True)`, and calls `shutdown_wsgi_executor()` before `loop_pool.stop()`. In [`wsgi/pool.rs`](../../../../src/rs/vertebra-py/src/wsgi/pool.rs)[^src-vertebra-py-src-wsgi-pool-rs], `shutdown_wsgi_executor()` detaches the GIL (`py.detach(|| exec.shutdown())`) before joining watchdog/worker threads to avoid GIL deadlock, `GilSafePyObject` ensures background threads never decref `Py<PyAny>` without `Python::attach`, and `ShmResponseLock` in [`transport.rs`](../../../../src/rs/vertebra-py/src/transport.rs)[^src-vertebra-py-src-transport-rs] serializes concurrent SHM response ring writes (detaching the GIL on lock contention).
- **PEP 3333 `_WsgiStartResponse` `called` vs `headers_sent`**: [`_WsgiStartResponse`](../../../../src/rs/vertebra-py/src/wsgi/runner.rs)[^src-vertebra-py-src-wsgi-runner-rs] tracks `called` and `headers_sent` separately (setting `headers_sent = true` only on non-empty `write()` or body yield) so error middleware can replace headers before body output and re-raise `exc_info[1]` once `headers_sent` is `true`.
- **`PyBuffer_FillInfo` Ownership & Writable `PyBuffer` Detach Hazard**: In [`Request::__getbuffer__`](../../../../src/rs/vertebra-py/src/request.rs)[^src-vertebra-py-src-request-rs], pass borrowed `slf.as_ptr()` to `PyBuffer_FillInfo` — never `slf.into_ptr()` — because `PyBuffer_FillInfo` calls `Py_XNewRef` internally. `Request.body_view` delegates to `PyMemoryView_FromObject(slf)` to keep `Arc<BatchSnapshot>` anchored, paired with PEP 688 `__buffer__` in [`_vertebra.pyi`](../../../../src/py/vertebra/_vertebra.pyi)[^src-vertebra-pyi]. Never hold a borrowed slice into a writable (`!py_buf.readonly()`) or non-contiguous `PyBuffer<u8>` (`bytearray`, strided `memoryview`) across `py.detach`: [`transport.rs`](../../../../src/rs/vertebra-py/src/transport.rs)[^src-vertebra-py-src-transport-rs] (`with_buffer_slice`, `submit_raw_response`) and `webtransport.rs` (`extract_buffer_bytes`) copy non-readonly or non-contiguous buffers via `py_buf.to_vec(py)?` and drop `PyBuffer` before releasing the GIL.
- **Non-Instantiable `.pyi` Constructors & `Buffer` Typing**: Classes with no native Python constructor (`Request`, `Headers`, `HeadersIterator`, `RequestBatch`, `RequestBatchIterator`, `AsgiScopeIterator`, `WsgiEnviron`, `WsgiEnvironIterator`, `_WsgiStartResponse`, `RsgiTransport`) must omit `__new__` in [`_vertebra.pyi`](../../../../src/py/vertebra/_vertebra.pyi)[^src-vertebra-pyi], and buffer-protocol parameters (such as `RsgiHttpProtocol.response_bytes`) must be typed as `collections.abc.Buffer` without `Any` leakage so `mypy.stubtest vertebra._vertebra` and `pyright` pass.
- **`PYO3_PYTHON`, Root `maturin develop` & Binary Freshness**: Build `vertebra-http` with `PYO3_PYTHON="$(CURDIR)/.venv/bin/python"` while `prepare_embedded_python_env` and `ensure_embedded_python_path` in [`main.rs`](../../../../src/rs/vertebra-http/src/main.rs) anchor `.venv` resolution. Crate-local `pyproject.toml` files in `vertebra-{bundler,storage,bigquery,duckdb,mssql,mysql,oracle,postgres,redis,sqlite,spanner,valkey}` require `-m <Cargo.toml>`, whereas `vertebra-py` uses root [`pyproject.toml`](../../../../pyproject.toml)[^src-pyproject-toml] and must run `maturin develop` from repository root without `-m`. [`find_server_binary()`](../../../../src/py/vertebra/supervisor/_supervisor.py)[^src-supervisor-supervisor-py] checks `VERTEBRA_SERVER_BINARY`, `CARGO_TARGET_DIR`, packaged `vertebra/bin`, and then workspace target directories (`release` before `debug`, newest by `st_mtime_ns`); keep `duckdb`, `spikard`, and `granian` isolated in `[dependency-groups].bench`.

## 3. Litestar v2/v3 Offload, Batched Audit Logs & Driver Teardown

- **Litestar v2/v3 Fast-Path & Offload Disqualifiers**: Litestar v2 `resolve_opts()` mutates in place and returns `None`, storing `after_request`, `type_decoders`, and `response_class` on `ownership_layers` ([`fastpath.py`](../../../../src/py/vertebra/extensions/litestar/fastpath.py)[^src-extensions-litestar-fastpath-py]). Litestar v3 renames `zstd_level` to `zstd_compress_level`, and [`VertebraPlugin.on_app_init`](../../../../src/py/vertebra/extensions/litestar/plugin.py)[^src-extensions-litestar-plugin-py] must clear `app_config.compression_config = None` only when `VERTEBRA_SERVER_ACTIVE=1` or `VERTEBRA_RUST_OFFLOAD=1` for Rust-supported codecs (`zstd`, `gzip`, `brotli`), disqualifying `LitestarFastPathBypass` when Python `CompressionConfig` or `CORSConfig` remains active.
- **`AsyncLogCore` Batched JSONL Chunks, TNS Validation & Socket Teardown**: Native `AsyncLogCore` drains multi-record `\n`-delimited `bytes` chunks whereas Python `BoundedLogQueue` yields one record per item; split drained chunks with `chunk.splitlines()` in tests. Validate `OracleConnectDescriptor.to_tns_descriptor()` fields against `^[A-Za-z0-9._-]+$`, and pair async driver `writer.close()` calls with bounded `asyncio.wait_for(writer.wait_closed(), timeout=...)`.

## 4. `make docs` Stubs, Direct `_vertebra` Primitives, Thin `utils` & CI Guards

- **Direct `vertebra._vertebra` Primitives vs `--only-group doc` Stubs**: `src/py/vertebra/utils/{text,humanize,uuids}.py` ([`text.py`](../../../../src/py/vertebra/utils/text.py)[^src-utils-text-py]) import native primitives (`camelize`, `snake_case`, `slugify`, `validate_identifier`, `humanize`, `uuid4_u128`, `uuid7_u128`, `nanoid_generate`) directly from `vertebra._vertebra` with zero pure-Python `_py_*` fallbacks, and [`embeddings/_google.py`](../../../../src/py/vertebra/embeddings/_google.py)[^src-embeddings-google-py] imports `SyncGcpTokenCache`, `AsyncGcpTokenCache`, and `gcp_http2_post_async` directly from `vertebra._vertebra`. For CI's `uv sync --no-install-project --only-group doc` (`make docs`), `ensure_native_stub_module` in [`generate_cli_docs.py`](../../../../tools/docs/generate_cli_docs.py)[^src-tools-docs-generate-cli-docs-py] stubs `vertebra._vertebra` and `vertebra._bundler` while `_make_stub_getattr` sets `fallback_none_attrs` strictly to `{"AsyncObjectStore", "SchemaDigestCache", "SyncObjectStore", "field_id", "handler_id", "state_id"}` (returning `_StubNativeBase` subclasses for other attributes); `vertebra._storage` is left unstubbed so `vertebra.storage` uses its Python fallbacks, and optional web frameworks resolve via `import_optional(...)`.
- **Thin `vertebra.utils`, Coverage Omit, TS Fallback, `msgspec.Struct` & `GuardProtocol`**: [`utils/__init__.py`](../../../../src/py/vertebra/utils/__init__.py)[^src-utils-init-py] must contain only its module docstring with zero attributes (`test_utils_package_is_thin`). Keep `[tool.coverage.report]` omitting `server/_js/**` and `tests/**` in [`pyproject.toml`](../../../../pyproject.toml) (and pass `-i` on `coverage xml`/`html` for Litestar's synthetic `dto_transfer_function_*` frames). Fall back from `.gitignore`d `server/_js/bundle/` to `src/ts/vertebra-client/{dist,src}/` when `make py-test` runs without `make ts-build`. Document `msgspec.Struct` fields with per-field docstrings (never class `Attributes:` blocks), and check `getattr(guard, "check", None)` before `guard(ctx)` on `GuardProtocol` subclasses.

## 5. SQLSpec Binds, Owned Tool Snapshots & Channel Feed Identity

- **Owned Tool Snapshots & Type-Sensitive Bind Approval**: [`build_tools`](../../../../src/py/vertebra/agent/tools/__init__.py)[^src-provider-bridge] materializes defaults, deep-copies bound arguments into independent invocation and ticket snapshots, maps `ToolPolicyError` only at the agent bridge, and compares converted scalar types as well as values recursively (`True == 1 == 1.0` maps to distinct `BOOL`/`INT64`/`FLOAT64` binds).
- **Early Tenant Rejection, Prepared SQLSpec Results & Converter Evidence**: BigQuery and Spanner config/driver constructors reject non-null tenant enforcement at construction before base/broker setup. [`sqlspec/_driver.py`](../../../../src/py/vertebra/extensions/sqlspec/_driver.py)[^src-stack-implementation] uses the prepared statement's `returns_rows()`, `SQLResult`/`StackResult` factories, wrapper-owned placeholder styles, and `is_many=True` batch preparation rather than leading-keyword heuristics; treat `pytest.importorskip` converter skips as distinct from converter-enabled runs.
- **Non-Null Shared-Feed Identity**: [`_channels.py`](../../../../src/py/vertebra/server/_channels.py)[^src-channel-implementation] suppresses duplicate local delivery only for non-null shared feed object identity (`None is None` never suppresses delivery).

## Cross-extension Arrow capsules and release

- **Unregistered SHM Writers, `ArrowCursor` GIL Detachment & Single Ownership Claim**: Optional extensions have private static maps and must materialize capsule buffers via `write_shm_arrow_region_unregistered` or `ShmArrowStreamWriter::finish_unregistered` ([`arrow_shm.rs`](../../../../src/rs/vertebra-db/src/arrow_shm.rs)[^src-db-arrow-shm]). `ArrowCursor._from_shm_capsule` ([`db.rs`](../../../../src/rs/vertebra-py/src/db.rs)[^src-py-arrow-cursor]) validates the `vertebra.ShmCapsule` name, pointer, and length before atomically claiming `ctx` once (`ValueError` on second claim), leaving unconsumed cleanup in the producer's destructor ([`py_async.rs`](../../../../src/rs/vertebra-db/src/py_async.rs)[^src-db-py-async]). When reading IPC payloads directly by name, `ArrowCursor::read_ipc_from_shm` (and `ArrowCursor::read_bytes` fallback) calls `read_shm_arrow_frame` inside `py.detach(|| ...)` so `shm_open`, `mmap`, and payload copies never hold the GIL.
- **Unlink vs Unmap & Distinct Completion Drainers**: `release()` and `close()` unlink the Unix SHM name while retained cursor views keep the mapping readable until the producer's `drop_fn` runs ([`shm.rs`](../../../../src/rs/vertebra-core/src/shm.rs)[^src-core-shm-capsule]). All extensions share the canonical completion bridge in [`vertebra-core/src/py_async.rs`](../../../../src/rs/vertebra-core/src/py_async.rs)[^src-core-py-async] (`spawn_rust_task_with_attr`) via `vertebra-db::py_async` / `vertebra-py::async_bridge`, and each separately linked `.so` must pass a unique drainer attribute name (`_vertebra_core_completion_drainer`, `_vertebra_storage_drainer`, `_vertebra_postgres_completion_drainer`, etc.; see [architecture](../architecture/worker/pyo3-ffi-and-batch-dispatch.md#database--storage-extension-boundaries) and [ADR-004](../decisions/adr-004-optional-driver-wheels-and-shm-ownership.md)).
- **Cancelled Stream Futures & Ordered Delivery Leases**: Cancelling an `asyncio.Future` after a Tokio delivery resolves but before or during drainer execution skips delivery unless the outcome producer re-checks cancellation. Pair `spawn_rust_db_task_with_attr_and_future` (`py_fut.cancelled()`) with RAII `ReplicationInFlightGuard` ([`replication/mod.rs`](../../../../src/rs/vertebra-postgres/src/replication/mod.rs)[^src-vertebra-postgres-src-replication-mod-rs]) and `ReplicationEventDelivery` ([`supervisor.rs`](../../../../src/rs/vertebra-postgres/src/replication/supervisor.rs)[^src-vertebra-postgres-src-replication-supervisor-rs]), and pair `PyAsyncPostgresListener::next` (`spawn_rust_db_task_with_attr_and_cancel`) with `PendingDeliveryGuard` ([`async_client.rs`](../../../../src/rs/vertebra-postgres/src/async_client.rs)[^src-vertebra-postgres-src-async-client-rs]) so cancelled or dropped futures retire cleanly without losing ordered records or advancing generation state.

[^src-vertebra-http-src-main-rs]: `src/rs/vertebra-http/src/main.rs` — Embedded interpreter init and inittab registration.
[^src-vertebra-py-src-lib-rs]: `src/rs/vertebra-py/src/lib.rs` — Core PyO3 extension module exports and gil_used = false.
[^src-vertebra-py-src-request-rs]: `src/rs/vertebra-py/src/request.rs` — Zero-copy Request buffer protocol and PyBuffer_FillInfo refcounting.
[^src-vertebra-py-src-transport-rs]: `src/rs/vertebra-py/src/transport.rs` — Writable and non-contiguous PyBuffer copying before py.detach and ShmResponseLock.
[^src-vertebra-py-src-asgi-rs]: `src/rs/vertebra-py/src/asgi.rs` — SteppedCoroutine cyclic GC slots without freelist.
[^src-vertebra-py-src-msgspec-bridge-rs]: `src/rs/vertebra-py/src/msgspec_bridge.rs` — Weakref-keyed decoder caches and CachedCallableBox GC traversal.
[^src-vertebra-py-src-rsgi-http-rs]: `src/rs/vertebra-py/src/rsgi/http.rs` — Frozen RsgiReadyAwaitable singleton and PyIter_Send stepping.
[^src-vertebra-py-src-wsgi-runner-rs]: `src/rs/vertebra-py/src/wsgi/runner.rs` — PEP 3333 _WsgiStartResponse called vs headers_sent state machine.
[^src-vertebra-py-src-wsgi-pool-rs]: `src/rs/vertebra-py/src/wsgi/pool.rs` — WsgiExecutor GilSafePyObject drop safety and GIL-detached shutdown.
[^src-server-gil-audit-py]: `src/py/vertebra/server/_gil_audit.py` — Free-threaded extension GIL audit enforcement.
[^src-server-asgi-runner-py]: `src/py/vertebra/server/_asgi/_runner.py` — Task-free ASGI batch stepping and task promotion.
[^src-server-dispatch-py]: `src/py/vertebra/server/_dispatch.py` — Worker-scoped ThreadPoolExecutor lifecycle and shutdown.
[^src-server-worker-py]: `src/py/vertebra/server/worker.py` — Single-root ASGI, RSGI, and WSGI batch routing and executor teardown in Worker.
[^src-vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Native _vertebra type stub rules for Buffer and non-instantiable classes.
[^src-supervisor-supervisor-py]: `src/py/vertebra/supervisor/_supervisor.py` — Server binary mtime resolution across target directories.
[^src-extensions-litestar-plugin-py]: `src/py/vertebra/extensions/litestar/plugin.py` — Guarded Litestar compression offload activation.
[^src-extensions-litestar-fastpath-py]: `src/py/vertebra/extensions/litestar/fastpath.py` — Litestar v2/v3 ownership_layers and fast-path disqualifiers.
[^src-utils-text-py]: `src/py/vertebra/utils/text.py` — Direct native text primitive imports from vertebra._vertebra.
[^src-utils-init-py]: `src/py/vertebra/utils/__init__.py` — Thin vertebra.utils package contract.
[^src-embeddings-google-py]: `src/py/vertebra/embeddings/_google.py` — Direct GCP token cache and HTTP/2 imports from vertebra._vertebra.
[^src-tools-docs-generate-cli-docs-py]: `tools/docs/generate_cli_docs.py` — Doc-only native module stubbing in _make_stub_getattr.
[^src-pyproject-toml]: `pyproject.toml` — Root maturin, coverage, and optional dependency configuration.
[^src-provider-bridge]: `src/py/vertebra/agent/tools/__init__.py` — Owned tool invocation snapshots and recursive type-sensitive approval.
[^src-stack-implementation]: `src/py/vertebra/extensions/sqlspec/_driver.py` — Prepared SQLSpec result metadata and native bind placeholders.
[^src-channel-implementation]: `src/py/vertebra/server/_channels.py` — Non-null shared-feed identity and independent output channels.
[^src-core-py-async]: `src/rs/vertebra-core/src/py_async.rs` — Shared PyO3 Tokio completion bridge with per-extension drainer_attr.
[^src-db-arrow-shm]: `src/rs/vertebra-db/src/arrow_shm.rs` — Unregistered Arrow SHM writers for cross-extension transfer.
[^src-db-py-async]: `src/rs/vertebra-db/src/py_async.rs` — Per-extension completion drainers and capsule destructors.
[^src-core-shm-capsule]: `src/rs/vertebra-core/src/shm.rs` — Named vertebra.ShmCapsule ABI and unlink vs mapping lifetime.
[^src-py-arrow-cursor]: `src/rs/vertebra-py/src/db.rs` — Single-consumer ArrowCursor capsule adoption, release, and SHM read GIL detachment.
[^src-vertebra-postgres-src-lib-rs]: `src/rs/vertebra-postgres/src/lib.rs` — Native PostgreSQL PyO3 extension exports and gil_used = false.
[^src-vertebra-postgres-src-replication-mod-rs]: `src/rs/vertebra-postgres/src/replication/mod.rs` — Replication future cancellation and RAII delivery drop ordering.
[^src-vertebra-postgres-src-replication-supervisor-rs]: `src/rs/vertebra-postgres/src/replication/supervisor.rs` — PostgreSQL replication supervisor and ReplicationEventDelivery leases.
[^src-vertebra-postgres-src-async-client-rs]: `src/rs/vertebra-postgres/src/async_client.rs` — PostgreSQL async listener PendingDeliveryGuard cancellation bridge.
