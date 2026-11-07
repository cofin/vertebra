---
type: Gotcha
title: "PyO3 FFI, Embedded Globals & Python Runtime Gotchas"
description: "Runtime pitfalls in embedded PyO3 module initialization, free-threaded 3.14t/3.15t freelists, buffer refcounting, coroutine stepping, doc-only stubs, SQLSpec binds, and cross-extension Arrow capsules."
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
    at: "2026-11-07T03:55:00Z"
    commit: cdb978d3
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
  - id: src-vertebra-py-src-rsgi-http-rs
    resource: src/rs/vertebra-py/src/rsgi/http.rs
    title: "Frozen RsgiReadyAwaitable singleton and PyIter_Send stepping"
  - id: src-vertebra-py-src-wsgi-runner-rs
    resource: src/rs/vertebra-py/src/wsgi/runner.rs
    title: "PEP 3333 _WsgiStartResponse called vs headers_sent state machine"
  - id: src-server-gil-audit-py
    resource: src/py/vertebra/server/_gil_audit.py
    title: "Free-threaded extension GIL audit enforcement"
  - id: src-server-asgi-runner-py
    resource: src/py/vertebra/server/_asgi/_runner.py
    title: "Task-free ASGI batch stepping and task promotion"
  - id: src-server-worker-py
    resource: src/py/vertebra/server/worker.py
    title: "Single-root ASGI and WSGI batch routing in Worker"
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
    resource: src/py/vertebra/extensions/sqlspec/adapters.py
    title: "Prepared SQLSpec result metadata and native bind placeholders"
  - id: src-channel-implementation
    resource: src/py/vertebra/server/_channels.py
    title: "Non-null shared-feed identity and independent output channels"
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
    title: "Single-consumer ArrowCursor capsule adoption and release"
---

# PyO3 FFI, Embedded Globals & Python Runtime Gotchas

<!-- truth: start -->
- Declare `#[pymodule(gil_used = false)]` on all five PyO3 extensions, omit `#[pyclass(freelist = N)]` under `Py_GIL_DISABLED` (`3.14t`/`3.15t`), and register embedded `_vertebra` via `append_to_inittab!` plus `sys.modules` aliasing.
- Pass borrowed `slf.as_ptr()` (never `into_ptr()`) to `PyBuffer_FillInfo`, run `maturin develop` for `vertebra-py` from repository root without `-m`, and keep `tools/docs/generate_cli_docs.py` `fallback_none_attrs` synchronized with `--only-group doc` builds.
- Transfer cross-extension Arrow buffers exclusively through unregistered `ShmRegion` writers, `vertebra.ShmCapsule` single-claim adoption, and per-extension completion drainer attributes.
<!-- truth: end -->

## 1. Embedded Split-Globals & Free-Threaded `3.14t`/`3.15t` Discipline

- **Dual `.so` Split-Globals in `Topology::Embedded`**: `vertebra-server` statically links `vertebra-py` (`_vertebra`), whereas `.venv` holds a separate `_vertebra.cpython-*.so` with its own uninitialized `static BRIDGE: OnceLock<BatchBridge>`. [`main.rs`](../../../../src/rs/vertebra-http/src/main.rs)[^src-vertebra-http-src-main-rs] must call `pyo3::append_to_inittab!(pymodule_init)` before the first `Python::attach` and alias `sys.modules["vertebra._vertebra"] = sys.modules["_vertebra"]` before importing Vertebra Python modules.
- **`#[pymodule(gil_used = false)]` & Conditional `freelist`**: All five extensions (`vertebra-py`, `vertebra-bundler`, `vertebra-storage`, `vertebra-bigquery`, `vertebra-spanner`) must declare `#[pymodule(gil_used = false)]` ([`vertebra-py/src/lib.rs`](../../../../src/rs/vertebra-py/src/lib.rs)[^src-vertebra-py-src-lib-rs]); [`GilAuditReport.audit()`](../../../../src/py/vertebra/server/_gil_audit.py)[^src-server-gil-audit-py] raises `RuntimeError` when `VERTEBRA_STRICT_FREE_THREADING=1`. Because PyO3 `0.30.1` guards `freelist` with a global `Mutex` under `Py_GIL_DISABLED`, gate `freelist = 128` behind `#[cfg_attr(not(Py_GIL_DISABLED), ...)]`.
- **`RsgiReadyAwaitable` Must Be `frozen`**: In [`rsgi/http.rs`](../../../../src/rs/vertebra-py/src/rsgi/http.rs)[^src-vertebra-py-src-rsgi-http-rs], `_RsgiReadyAwaitable` is shared across concurrent coroutines on `3.14t`/`3.15t`; `__await__` and `__next__` must take `&self` (`frozen`) to avoid `PyBorrowMutError`.

## 2. Coroutine Stepping, PEP 3333 State, Buffer Refcounts & Multi-Extension Packaging

- **Task-Free `drive_asgi_batch` vs `asyncio.current_task()`**: `drive_asgi_batch` steps each coroutine's synchronous prefix via `PyIter_Send` before allocating an `asyncio.Task` ([`_asgi/_runner.py`](../../../../src/py/vertebra/server/_asgi/_runner.py)[^src-server-asgi-runner-py]), so middleware observes `asyncio.current_task() is None` until the first suspension point. [`Worker._process_batch`](../../../../src/py/vertebra/server/worker.py)[^src-server-worker-py] must route single-root `"/"` ASGI and WSGI mounts directly through `drive_asgi_batch` and `single_root_wsgi.spawn_batch(batch)` rather than `partition_batch`.
- **PEP 3333 `_WsgiStartResponse` `called` vs `headers_sent`**: [`_WsgiStartResponse`](../../../../src/rs/vertebra-py/src/wsgi/runner.rs)[^src-vertebra-py-src-wsgi-runner-rs] tracks `called` and `headers_sent` separately (setting `headers_sent = true` only on non-empty `write()` or body yield) so error middleware can replace headers before body output and re-raise `exc_info[1]` once `headers_sent` is `true`.
- **`PyBuffer_FillInfo` Refcount Ownership**: In [`Request::__getbuffer__`](../../../../src/rs/vertebra-py/src/request.rs)[^src-vertebra-py-src-request-rs], pass borrowed `slf.as_ptr()` to `PyBuffer_FillInfo` — never `slf.into_ptr()` — because `PyBuffer_FillInfo` calls `Py_XNewRef` internally. `Request.body_view` delegates to `PyMemoryView_FromObject(slf)` to keep `Arc<BatchSnapshot>` anchored, paired with PEP 688 `__buffer__` in `.pyi` stubs.
- **`PYO3_PYTHON`, Root `maturin develop` & Binary Freshness**: Build `vertebra-http` with `PYO3_PYTHON="$(CURDIR)/.venv/bin/python"` while `prepare_embedded_python_env` and `ensure_embedded_python_path` in [`main.rs`](../../../../src/rs/vertebra-http/src/main.rs) anchor `.venv` resolution. Crate-local `pyproject.toml` files in `vertebra-{bundler,storage,bigquery,spanner}` require `-m <Cargo.toml>`, whereas `vertebra-py` uses root [`pyproject.toml`](../../../../pyproject.toml)[^src-pyproject-toml] and must run `maturin develop` from repository root without `-m`. [`find_server_binary()`](../../../../src/py/vertebra/supervisor/_supervisor.py)[^src-supervisor-supervisor-py] selects the newest binary by `st_mtime_ns` across target directories; keep `duckdb`, `spikard`, and `granian` isolated in `[dependency-groups].bench`.

## 3. Litestar v2/v3 Offload, Batched Audit Logs & Driver Teardown

- **Litestar v2/v3 Fast-Path & Offload Disqualifiers**: Litestar v2 `resolve_opts()` mutates in place and returns `None`, storing `after_request`, `type_decoders`, and `response_class` on `ownership_layers` ([`fastpath.py`](../../../../src/py/vertebra/extensions/litestar/fastpath.py)[^src-extensions-litestar-fastpath-py]). Litestar v3 renames `zstd_level` to `zstd_compress_level`, and [`VertebraPlugin.on_app_init`](../../../../src/py/vertebra/extensions/litestar/plugin.py)[^src-extensions-litestar-plugin-py] must clear `app_config.compression_config = None` only when `VERTEBRA_SERVER_ACTIVE=1` or `VERTEBRA_RUST_OFFLOAD=1` for Rust-supported codecs (`zstd`, `gzip`, `brotli`), disqualifying `LitestarFastPathBypass` when Python `CompressionConfig` or `CORSConfig` remains active.
- **`AsyncLogCore` Batched JSONL Chunks, TNS Validation & Socket Teardown**: Native `AsyncLogCore` drains multi-record `\n`-delimited `bytes` chunks whereas Python `BoundedLogQueue` yields one record per item; split drained chunks with `chunk.splitlines()` in tests. Validate `OracleConnectDescriptor.to_tns_descriptor()` fields against `^[A-Za-z0-9._-]+$`, and pair async driver `writer.close()` calls with bounded `asyncio.wait_for(writer.wait_closed(), timeout=...)`.

## 4. `make docs` Stubs, Direct `_vertebra` Primitives, Thin `utils` & CI Guards

- **Direct `vertebra._vertebra` Primitives vs `--only-group doc` Stubs**: `src/py/vertebra/utils/{text,humanize,uuids}.py` ([`text.py`](../../../../src/py/vertebra/utils/text.py)[^src-utils-text-py]) import native primitives (`camelize`, `snake_case`, `slugify`, `validate_identifier`, `humanize`, `uuid4_u128`, `uuid7_u128`, `nanoid_generate`) directly from `vertebra._vertebra` with zero pure-Python `_py_*` fallbacks, and [`embeddings/_google.py`](../../../../src/py/vertebra/embeddings/_google.py)[^src-embeddings-google-py] imports `SyncGcpTokenCache`, `AsyncGcpTokenCache`, and `gcp_http2_post_async` directly from `vertebra._vertebra`. For CI's `uv sync --no-install-project --only-group doc` (`make docs`), `ensure_native_stub_module` in [`generate_cli_docs.py`](../../../../tools/docs/generate_cli_docs.py)[^src-tools-docs-generate-cli-docs-py] stubs `vertebra._vertebra` and `vertebra._bundler` while `_make_stub_getattr` sets `fallback_none_attrs` strictly to `{"AsyncObjectStore", "SchemaDigestCache", "SyncObjectStore", "field_id", "handler_id", "state_id"}` (returning `_StubNativeBase` subclasses for other attributes); `vertebra._storage` stays unstubbed so `vertebra.storage` uses its Python fallbacks, and optional web frameworks resolve via `import_optional(...)`.
- **Thin `vertebra.utils`, Coverage `ignore_errors`, TS Fallback, `msgspec.Struct` & `GuardProtocol`**: [`utils/__init__.py`](../../../../src/py/vertebra/utils/__init__.py)[^src-utils-init-py] must contain only its module docstring with zero attributes (`test_utils_package_is_thin`). Keep `[tool.coverage.report] ignore_errors = true` in [`pyproject.toml`](../../../../pyproject.toml) (`-i` on `coverage xml`/`html`) for Litestar's synthetic `dto_transfer_function_*` frames. Fall back from `.gitignore`d `server/_js/bundle/` to `src/ts/vertebra-client/{dist,src}/` when `make py-test` runs without `make ts-build`. Document `msgspec.Struct` fields with per-field docstrings (never class `Attributes:` blocks), and check `getattr(guard, "check", None)` before `guard(ctx)` on `GuardProtocol` subclasses.

## 5. SQLSpec Binds, Owned Tool Snapshots & Channel Feed Identity

- **Owned Tool Snapshots & Type-Sensitive Bind Approval**: [`build_tools`](../../../../src/py/vertebra/agent/tools/__init__.py)[^src-provider-bridge] materializes defaults, deep-copies bound arguments into independent invocation and ticket snapshots, maps `ToolPolicyError` only at the agent bridge, and compares converted scalar types as well as values recursively (`True == 1 == 1.0` maps to distinct `BOOL`/`INT64`/`FLOAT64` binds).
- **Early Tenant Rejection, Prepared SQLSpec Results & Converter Evidence**: BigQuery and Spanner config/driver constructors reject non-null tenant enforcement at construction before base/broker setup. [`sqlspec/adapters.py`](../../../../src/py/vertebra/extensions/sqlspec/adapters.py)[^src-stack-implementation] uses the prepared statement's `returns_rows()`, `SQLResult`/`StackResult` factories, wrapper-owned placeholder styles, and `is_many=True` batch preparation rather than leading-keyword heuristics; treat `pytest.importorskip` converter skips as distinct from converter-enabled runs.
- **Non-Null Shared-Feed Identity**: [`_channels.py`](../../../../src/py/vertebra/server/_channels.py)[^src-channel-implementation] suppresses duplicate local delivery only for non-null shared feed object identity (`None is None` never suppresses delivery).

## Cross-extension Arrow capsules and release

- **Unregistered SHM Writers & Single Ownership Claim**: Optional extensions have private static maps and must materialize capsule buffers via `write_shm_arrow_region_unregistered` or `ShmArrowStreamWriter::finish_unregistered` ([`arrow_shm.rs`](../../../../src/rs/vertebra-db/src/arrow_shm.rs)[^src-db-arrow-shm]). `ArrowCursor._from_shm_capsule` ([`db.rs`](../../../../src/rs/vertebra-py/src/db.rs)[^src-py-arrow-cursor]) validates the `vertebra.ShmCapsule` name, pointer, and length before atomically claiming `ctx` once (`ValueError` on second claim), leaving unconsumed cleanup in the producer's destructor ([`py_async.rs`](../../../../src/rs/vertebra-db/src/py_async.rs)[^src-db-py-async]).
- **Unlink vs Unmap & Distinct Completion Drainers**: `release()` and `close()` unlink the Unix SHM name while retained cursor views keep the mapping readable until the producer's `drop_fn` runs ([`shm.rs`](../../../../src/rs/vertebra-core/src/shm.rs)[^src-core-shm-capsule]). Each separately linked extension must pass a unique drainer attribute name to `spawn_rust_db_task_with_attr` ([architecture](../architecture/worker/pyo3-ffi-and-batch-dispatch.md#database-extension-boundary), [ADR-004](../decisions/adr-004-optional-driver-wheels-and-shm-ownership.md)).

[^src-vertebra-http-src-main-rs]: `src/rs/vertebra-http/src/main.rs` — Embedded interpreter init and inittab registration.
[^src-vertebra-py-src-lib-rs]: `src/rs/vertebra-py/src/lib.rs` — Core PyO3 extension module exports and gil_used = false.
[^src-vertebra-py-src-request-rs]: `src/rs/vertebra-py/src/request.rs` — Zero-copy Request buffer protocol and PyBuffer_FillInfo refcounting.
[^src-vertebra-py-src-rsgi-http-rs]: `src/rs/vertebra-py/src/rsgi/http.rs` — Frozen RsgiReadyAwaitable singleton and PyIter_Send stepping.
[^src-vertebra-py-src-wsgi-runner-rs]: `src/rs/vertebra-py/src/wsgi/runner.rs` — PEP 3333 _WsgiStartResponse called vs headers_sent state machine.
[^src-server-gil-audit-py]: `src/py/vertebra/server/_gil_audit.py` — Free-threaded extension GIL audit enforcement.
[^src-server-asgi-runner-py]: `src/py/vertebra/server/_asgi/_runner.py` — Task-free ASGI batch stepping and task promotion.
[^src-server-worker-py]: `src/py/vertebra/server/worker.py` — Single-root ASGI and WSGI batch routing in Worker.
[^src-supervisor-supervisor-py]: `src/py/vertebra/supervisor/_supervisor.py` — Server binary mtime resolution across target directories.
[^src-extensions-litestar-plugin-py]: `src/py/vertebra/extensions/litestar/plugin.py` — Guarded Litestar compression offload activation.
[^src-extensions-litestar-fastpath-py]: `src/py/vertebra/extensions/litestar/fastpath.py` — Litestar v2/v3 ownership_layers and fast-path disqualifiers.
[^src-utils-text-py]: `src/py/vertebra/utils/text.py` — Direct native text primitive imports from vertebra._vertebra.
[^src-utils-init-py]: `src/py/vertebra/utils/__init__.py` — Thin vertebra.utils package contract.
[^src-embeddings-google-py]: `src/py/vertebra/embeddings/_google.py` — Direct GCP token cache and HTTP/2 imports from vertebra._vertebra.
[^src-tools-docs-generate-cli-docs-py]: `tools/docs/generate_cli_docs.py` — Doc-only native module stubbing in _make_stub_getattr.
[^src-pyproject-toml]: `pyproject.toml` — Root maturin, coverage, and optional dependency configuration.
[^src-provider-bridge]: `src/py/vertebra/agent/tools/__init__.py` — Owned tool invocation snapshots and recursive type-sensitive approval.
[^src-stack-implementation]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Prepared SQLSpec result metadata and native bind placeholders.
[^src-channel-implementation]: `src/py/vertebra/server/_channels.py` — Non-null shared-feed identity and independent output channels.
[^src-db-arrow-shm]: `src/rs/vertebra-db/src/arrow_shm.rs` — Unregistered Arrow SHM writers for cross-extension transfer.
[^src-db-py-async]: `src/rs/vertebra-db/src/py_async.rs` — Per-extension completion drainers and capsule destructors.
[^src-core-shm-capsule]: `src/rs/vertebra-core/src/shm.rs` — Named vertebra.ShmCapsule ABI and unlink vs mapping lifetime.
[^src-py-arrow-cursor]: `src/rs/vertebra-py/src/db.rs` — Single-consumer ArrowCursor capsule adoption and release.
