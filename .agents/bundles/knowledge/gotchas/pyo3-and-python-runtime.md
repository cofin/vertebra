---
type: Guide
title: PyO3 FFI, Embedded Globals & Python Runtime Gotchas
description: Footguns in Embedded PyO3 module initialization, gil_used = false and
  conditional freelist on 3.14t/3.15t, PyBuffer_FillInfo refcounting, C-API ASGI/RSGI
  stepping, and Litestar v2/v3 parity.
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
  at: '2026-11-04T04:33:00Z'
verified:
- by: agent:codex
  at: '2026-11-05T06:09:26Z'
  commit: 5ad85a1b406f5b3ed01ce8d1f536e02383bc89e1
- by: agent:flow-review
  at: '2026-11-05T00:35:59Z'
  commit: 4ce057e5aff909fcda2488cc0f6c4882ad0d38f0
sources:
- id: src-stack-implementation
  resource: src/py/vertebra/extensions/sqlspec/adapters.py
  title: Prepared SQLSpec result construction and native bind execution
- id: src-stack-tests
  resource: src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
  title: Native bind, classification, Arrow and batched execution regressions
- id: src-converter-tests
  resource: src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
  title: Optional psycopg query and parameter converter tests
- id: src-channel-implementation
  resource: src/py/vertebra/server/_channels.py
  title: Non-null shared-feed identity and independent output channels
- id: src-channel-regressions
  resource: src/py/tests/unit/db/test_native_db_drivers.py
  title: Shared-feed, absent-feed and per-output duplicate suppression tests
- id: src-provider-bridge
  resource: src/py/vertebra/agent/tools/__init__.py
  title: Generic typed invocation and caller-bound provider confirmation
- id: src-provider-contracts
  resource: src/py/vertebra/_typing.py
  title: Agent-neutral provider and policy error contracts
- id: src-provider-snapshot-tests
  resource: src/py/tests/unit/agent/test_runtime_and_workflows.py
  title: Owned invocation and approval snapshots under caller mutation
- id: src-bigquery-bind-types
  resource: src/py/vertebra/db/adapters/bigquery/core.py
  title: Type-sensitive native BOOL, INT64 and FLOAT64 parameter encoding
- id: src-gcp-tenant-bigquery
  resource: src/py/vertebra/extensions/sqlspec/bigquery.py
  title: Early unsupported tenant enforcement in BigQuery constructors
- id: src-gcp-tenant-spanner
  resource: src/py/vertebra/extensions/sqlspec/spanner.py
  title: Early unsupported tenant enforcement in Spanner constructors
- id: src-vertebra-py-src-lib-rs
  resource: src/rs/vertebra-py/src/lib.rs
  title: Rust implementation module (src/rs/vertebra-py/src/lib.rs)
- id: src-vertebra-py-src-transport-rs
  resource: src/rs/vertebra-py/src/transport.rs
  title: Rust implementation module (src/rs/vertebra-py/src/transport.rs)
- id: src-vertebra-py-src-request-rs
  resource: src/rs/vertebra-py/src/request.rs
  title: Rust implementation module (src/rs/vertebra-py/src/request.rs)
- id: src-vertebra-py-src-rsgi-http-rs
  resource: src/rs/vertebra-py/src/rsgi/http.rs
  title: Rust implementation module (src/rs/vertebra-py/src/rsgi/http.rs)
- id: src-vertebra-py-src-wsgi-runner-rs
  resource: src/rs/vertebra-py/src/wsgi/runner.rs
  title: Rust implementation module (src/rs/vertebra-py/src/wsgi/runner.rs)
- id: src-vertebra-bundler-src-lib-rs
  resource: src/rs/vertebra-bundler/src/lib.rs
  title: Rust implementation module (src/rs/vertebra-bundler/src/lib.rs)
- id: src-vertebra-storage-src-lib-rs
  resource: src/rs/vertebra-storage/src/lib.rs
  title: Rust implementation module (src/rs/vertebra-storage/src/lib.rs)
- id: src-vertebra-http-src-main-rs
  resource: src/rs/vertebra-http/src/main.rs
  title: Rust implementation module (src/rs/vertebra-http/src/main.rs)
- id: src-server-worker-py
  resource: src/py/vertebra/server/worker.py
  title: Python implementation module (src/py/vertebra/server/worker.py)
- id: src-server-asgi-runner-py
  resource: src/py/vertebra/server/_asgi/_runner.py
  title: Python implementation module (src/py/vertebra/server/_asgi/_runner.py)
- id: src-server-asgi-wire-py
  resource: src/py/vertebra/server/_asgi/_wire.py
  title: Python implementation module (src/py/vertebra/server/_asgi/_wire.py)
- id: src-server-wsgi-adapter-py
  resource: src/py/vertebra/server/_wsgi/_adapter.py
  title: Python implementation module (src/py/vertebra/server/_wsgi/_adapter.py)
- id: src-server-gil-audit-py
  resource: src/py/vertebra/server/_gil_audit.py
  title: Python implementation module (src/py/vertebra/server/_gil_audit.py)
- id: src-supervisor-supervisor-py
  resource: src/py/vertebra/supervisor/_supervisor.py
  title: Python implementation module (src/py/vertebra/supervisor/_supervisor.py)
- id: src-extensions-litestar-plugin-py
  resource: src/py/vertebra/extensions/litestar/plugin.py
  title: Python implementation module (src/py/vertebra/extensions/litestar/plugin.py)
- id: src-extensions-litestar-offload-py
  resource: src/py/vertebra/extensions/litestar/offload.py
  title: Python implementation module (src/py/vertebra/extensions/litestar/offload.py)
- id: src-extensions-litestar-fastpath-py
  resource: src/py/vertebra/extensions/litestar/fastpath.py
  title: Python implementation module (src/py/vertebra/extensions/litestar/fastpath.py)
- id: src-extensions-litestar-store-py
  resource: src/py/vertebra/extensions/litestar/store.py
  title: Python implementation module (src/py/vertebra/extensions/litestar/store.py)
- id: src-utils-serializers-zero-copy-py
  resource: src/py/vertebra/utils/serializers/_zero_copy.py
  title: Python implementation module (src/py/vertebra/utils/serializers/_zero_copy.py)
- id: src-utils-text-py
  resource: src/py/vertebra/utils/text.py
  title: Python implementation module (src/py/vertebra/utils/text.py)
- id: src-utils-init-py
  resource: src/py/vertebra/utils/__init__.py
  title: Python implementation module (src/py/vertebra/utils/__init__.py)
- id: src-tools-docs-generate-cli-docs-py
  resource: tools/docs/generate_cli_docs.py
  title: Repository tooling module (tools/docs/generate_cli_docs.py)
- id: src-pyproject-toml
  resource: pyproject.toml
  title: Python package metadata, dependencies, and tool configuration
- id: src-db-arrow-shm
  resource: src/rs/vertebra-db/src/arrow_shm.rs
  title: Registered and unregistered Arrow SHM writers
- id: src-db-py-async
  resource: src/rs/vertebra-db/src/py_async.rs
  title: Capsule destructor and per-extension completion queues
- id: src-core-shm-capsule
  resource: src/rs/vertebra-core/src/shm.rs
  title: SHM capsule and unlink versus mapping lifetime
- id: src-py-arrow-cursor
  resource: src/rs/vertebra-py/src/db.rs
  title: Single-consumer capsule adoption and cursor release
---

# PyO3 FFI, Embedded Globals & Python Runtime Gotchas

<!-- truth: start -->
- Declare `#[pymodule(gil_used = false)]` on every PyO3 extension, omit `#[pyclass(freelist = N)]` under `Py_GIL_DISABLED` (`3.14t`/`3.15t`), and register embedded `_vertebra` via `append_to_inittab!` plus `sys.modules` aliasing.
- Pass borrowed `slf.as_ptr()` (never `into_ptr()`) to `PyBuffer_FillInfo`, run `maturin develop` for `vertebra-py` from the repository root without `-m`, and guard native/framework imports for `--only-group doc` builds.
<!-- truth: end -->

## 1. Dual `.so` Split-Globals Trap in `Topology::Embedded`

- **Symptom**: `RuntimeError: transport not initialized; call init_transport() first` even though Rust initialized `BatchBridge` right before spawning the Python worker thread.
- **Root Cause**: The `vertebra-server` binary (package `vertebra-http`, feature `python`) statically links `vertebra-py` (`_vertebra`), while `.venv/lib/python3.*/site-packages/vertebra/_vertebra.cpython-*.so` is a separate dynamically loadable shared object. If Python executes a file-backed `import vertebra._vertebra`, `dlopen` loads the `.so` from disk with a **second** copy of `static BRIDGE: OnceLock<BatchBridge>` that is uninitialized.
- **Rule**: In [`src/rs/vertebra-http/src/main.rs`](../../../../src/rs/vertebra-http/src/main.rs)[^src-vertebra-http-src-main-rs], always call `pyo3::append_to_inittab!(pymodule_init)` (imported from `_vertebra`) before the first `Python::attach` (`auto-initialize`), and on the worker thread alias `sys.modules["vertebra._vertebra"] = sys.modules["_vertebra"]` before importing the application or `vertebra.server._dispatch`.

## 2. `#[pymodule(gil_used = false)]`, Global `freelist` Mutex & `RsgiReadyAwaitable` on `3.14t`/`3.15t`

- **Missing `gil_used = false` Re-Enables the GIL**: Every `#[pymodule]` ([`vertebra-py/src/lib.rs`](../../../../src/rs/vertebra-py/src/lib.rs)[^src-vertebra-py-src-lib-rs], [`vertebra-bundler/src/lib.rs`](../../../../src/rs/vertebra-bundler/src/lib.rs)[^src-vertebra-bundler-src-lib-rs], [`vertebra-storage/src/lib.rs`](../../../../src/rs/vertebra-storage/src/lib.rs)[^src-vertebra-storage-src-lib-rs]) must keep `#[pymodule(gil_used = false)]`. [`GilAuditReport.audit()`](../../../../src/py/vertebra/server/_gil_audit.py)[^src-server-gil-audit-py] inspects loaded extensions at startup and raises `RuntimeError` when `VERTEBRA_STRICT_FREE_THREADING=1`.
- **PyO3 `0.30.1` `freelist` Global `Mutex` Contention**: Under `Py_GIL_DISABLED`, PyO3 `0.30.1` protects `#[pyclass(freelist = N)]` with a global `Mutex`, serializing allocations across worker threads. Gate `freelist = 128` behind `#[cfg_attr(not(Py_GIL_DISABLED), pyclass(..., freelist = 128, immutable_type))]` and omit `freelist` under `#[cfg_attr(Py_GIL_DISABLED, pyclass(..., immutable_type))]` so free-threaded builds use mimalloc thread-local heaps directly.
- **`RsgiReadyAwaitable` Must Be `frozen`**: In [`rsgi/http.rs`](../../../../src/rs/vertebra-py/src/rsgi/http.rs)[^src-vertebra-py-src-rsgi-http-rs], `RsgiReadyAwaitable` (`_RsgiReadyAwaitable`) is shared as a cached singleton across concurrent coroutines on `3.14t`/`3.15t`; `__await__` and `__next__` must take `&self` (`frozen`), never `&mut self` (which raises `PyBorrowMutError` under concurrent awaits).

## 3. Task-Free `drive_asgi_batch` / `AsgiRunner` vs `asyncio.current_task()` (`_asgi/_runner.py`)

- **Symptom**: ASGI middlewares or libraries that inspect `asyncio.current_task()` before their first suspension point observe `None` under [`AsgiRunner`](../../../../src/py/vertebra/server/_asgi/_runner.py)[^src-server-asgi-runner-py] batch execution.
- **Root Cause**: Rust `drive_asgi_batch` (via `PyIter_Send`) steps the synchronous prefix of each ASGI coroutine directly on the event-loop thread without allocating an `asyncio.Task`. Only when a coroutine suspends (`PYGEN_NEXT`) does `_SteppedCoroutine` promote it into a real `asyncio.Task` (whereas `AsgiRunner.run_single` runs inside an existing task or coroutine context).
- **Rule**: Do not assume `asyncio.current_task()` is non-`None` prior to the first real suspension point in batch-dispatched ASGI middleware. Also ensure [`Worker._process_batch`](../../../../src/py/vertebra/server/worker.py)[^src-server-worker-py] routes single-root ASGI mounts (`"/"` without native routes) through `drive_asgi_batch(single_root_asgi.dispatch_app, batch, ...)` and single-root WSGI mounts through `single_root_wsgi.spawn_batch(batch)` (`drive_wsgi_batch`) rather than falling through to `partition_batch`.
- **PEP 3333 `_WsgiStartResponse` `called` vs `headers_sent` State Machine (`wsgi/runner.rs`)**: PEP 3333 permits a WSGI application to call `start_response(status, headers, exc_info)` a second time to replace the status and headers as long as no non-empty body chunk has been yielded yet, and requires re-raising `exc_info[1]` only if `headers_sent` is already `true`. [`_WsgiStartResponse`](../../../../src/rs/vertebra-py/src/wsgi/runner.rs)[^src-vertebra-py-src-wsgi-runner-rs] tracks `called` and `headers_sent` separately (flipping `headers_sent = true` on `write(non_empty)` or the first non-empty yielded chunk in `collect_wsgi_body`) so error-recovery middleware can overwrite headers before body transmission.

## 4. `PyBuffer_FillInfo` Refcount Ownership, `PYO3_PYTHON` Embedding, Multi-Extension `maturin` & `CARGO_TARGET_DIR` Freshness

- **`PyBuffer_FillInfo` Calls `Py_XNewRef` Internally**: In [`Request::__getbuffer__`](../../../../src/rs/vertebra-py/src/request.rs)[^src-vertebra-py-src-request-rs], pass borrowed `slf.as_ptr()` to `pyo3::ffi::PyBuffer_FillInfo` — never `slf.into_ptr()`. `PyBuffer_FillInfo` already increments `view.obj`'s reference count via `Py_XNewRef`, so passing an owned pointer leaks one reference per `memoryview(req)` (`Request` + `Arc<BatchSnapshot>`) on release. Note `Request.body_view` now delegates to `PyMemoryView_FromObject(slf)` so the `Arc<BatchSnapshot>` stays anchored until the `memoryview` is released or GC'd, and `.pyi` stubs must declare PEP 688 `def __buffer__(self, flags: int, /) -> memoryview: ...` for `mypy.stubtest`.
- **`PYO3_PYTHON` & Relocatable `uv` Interpreters in `Topology::Embedded` (`main.rs`)**: Building `vertebra-http` without `PYO3_PYTHON="$(CURDIR)/.venv/bin/python"` can link `vertebra-server` against a bare system `libpython` missing stdlib C extensions (`_socket`) or `.venv` `site-packages`. Both `Makefile` (`dev` and `build`) and [`main.rs`](../../../../src/rs/vertebra-http/src/main.rs) (`prepare_embedded_python_env` prepending `.venv/bin` to `PATH` before CPython init and `ensure_embedded_python_path` injecting `.venv` `site-packages`) ensure the embedded interpreter always resolves the project `.venv`.
- **Native Request vs Pure-Python Framework Types**: Native request-path classes (`Request`, `Response`, `Headers`, `AsgiScope`, `WsgiEnviron`) live in [`vertebra._vertebra`](../../../../src/rs/vertebra-py/src/lib.rs), whereas [`_asgi/_wire.py`](../../../../src/py/vertebra/server/_asgi/_wire.py)[^src-server-asgi-wire-py] provides wire/header helpers, [`_wsgi/_adapter.py`](../../../../src/py/vertebra/server/_wsgi/_adapter.py)[^src-server-wsgi-adapter-py] defines `WsgiAdapter`, and `vertebra.framework._router` defines pure-Python framework `Request`/`Response` classes. Import native FFI types explicitly from `vertebra._vertebra` for `isinstance` checks.
- **Root vs Crate-Local `pyproject.toml` in Multi-Extension `maturin develop`**: `src/rs/vertebra-bundler/pyproject.toml` and `src/rs/vertebra-storage/pyproject.toml` declare their `module-name` (`vertebra._bundler`, `vertebra._storage`) locally and require `-m <Cargo.toml>`, whereas `vertebra-py` relies on the repository-root [`pyproject.toml`](../../../../pyproject.toml)[^src-pyproject-toml] (`[tool.maturin] module-name = "vertebra._vertebra"`, `python-source = "src/py"`). Always run `maturin develop` for `vertebra-py` from repository root **without** `-m src/rs/vertebra-py/Cargo.toml` (as enforced in `Makefile` and `vertebra.build.packaging.pgo._maturin_develop_command`).
- **`CARGO_TARGET_DIR` Binary Freshness**: [`find_server_binary()`](../../../../src/py/vertebra/supervisor/_supervisor.py)[^src-supervisor-supervisor-py] compares `st_mtime_ns` across `CARGO_TARGET_DIR`, `target/{release,debug}`, `src/rs/target/{release,debug}`, and `/tmp/vtb-cargo-target` so `make dev` never shadows a newer `make build` binary (or vice versa), and always run `make dev` after Rust PyO3 edits before `pytest`. Keep `duckdb`, `spikard`, and `granian` isolated in `[dependency-groups].bench` in [`pyproject.toml`](../../../../pyproject.toml).

## 5. Litestar v2 vs v3 Offload & Fast-Path Disqualifier Traps (`extensions/litestar/`)

- **Litestar v2 In-Place `resolve_opts()` & `ownership_layers`**: Litestar v2 `BaseRouteHandler.resolve_opts()` mutates the handler in place and returns `None` (unlike v3 helpers), and v2 stores router/controller/app `after_request`, `type_decoders`, and `response_class` on `handler.ownership_layers` rather than calling `resolve_*` methods unconditionally. Always inspect both `resolve_*()` and `ownership_layers` in [`fastpath.py`](../../../../src/py/vertebra/extensions/litestar/fastpath.py)[^src-extensions-litestar-fastpath-py] and handle `None` returns from `resolve_opts()` in [`router.py`](../../../../src/py/vertebra/extensions/litestar/router.py).
- **Litestar v3 `zstd_compress_level` & Guarded Python Compression Clearing**: Litestar v3 `CompressionConfig` renames `zstd_level` to `zstd_compress_level` ([`offload.py`](../../../../src/py/vertebra/extensions/litestar/offload.py)[^src-extensions-litestar-offload-py] checks both). [`VertebraPlugin.on_app_init`](../../../../src/py/vertebra/extensions/litestar/plugin.py)[^src-extensions-litestar-plugin-py] must clear `app_config.compression_config = None` **only** when `VERTEBRA_SERVER_ACTIVE=1` or `VERTEBRA_RUST_OFFLOAD=1` and the backend is supported by Rust (`zstd`, `gzip`, `brotli`); under Litestar `TestClient` or external ASGI servers, Python `CompressionConfig` and `CORSConfig` remain active and must disqualify `LitestarFastPathBypass` so headers and compressed bodies are never skipped.

## 6. `AsyncLogCore` Batched Newline-Delimited JSONL Buffers vs Pure-Python Fallback

- **Symptom**: `assert len(log_sink.drain()) == 4` passes in pure-Python fallback tests but fails with `assert 1 == 4` when running with the compiled `_vertebra` extension in CI.
- **Root Cause**: The Rust native `AsyncLogCore` batches multiple JSONL records into a single `bytes` chunk (`b'{...}\n{...}\n{...}\n{...}\n'`) for zero-copy I/O efficiency, whereas the pure-Python `BoundedLogQueue` fallback appends separate single-record `bytes` items to a Python `collections.deque`.
- **Rule**: When asserting on drained audit logs in unit tests, always split chunks on line boundaries: `[line for chunk in log_sink.drain() for line in chunk.splitlines() if line.strip()]`.

## 7. TNS Descriptor Validation & Socket Teardown Timeouts

- **TNS Character Validation**: Oracle Connect Descriptors constructed via `OracleConnectDescriptor.to_tns_descriptor()` must strictly validate host, port, protocol, service, and SID inputs against safe identifier regexes (`^[A-Za-z0-9._-]+$`) to prevent TNS descriptor injection attacks.
- **Socket Teardown in Async Database Drivers**: In asyncio driver implementations (such as `AsyncRedisDriver` or loopback server mock harnesses), calling `writer.close()` must be paired with `asyncio.wait_for(writer.wait_closed(), timeout=...)` with a bounded timeout (e.g. 50 ms) to avoid hanging worker event loops during process shutdown.

## 8. `make docs` (`--only-group doc`), `coverage xml` (`dto_transfer_function_*`), and Unbuilt TS Bundles in CI

- **Guarding `vertebra._vertebra`, `vertebra._storage` & Optional Frameworks for `make docs`**: CI's `build-docs` job runs `uv sync --no-install-project --only-group doc` followed by `make docs` (`tools.docs.scan_public_docstrings` and `tools.docs.generate_cli_docs --check`). Neither the compiled `vertebra._vertebra`/`vertebra._storage` extensions nor optional web frameworks (`litestar`, `fastapi`, `django`, `flask`, `starlette`) are installed in that environment, and `ensure_native_stub_module` in [`tools/docs/generate_cli_docs.py`](../../../../tools/docs/generate_cli_docs.py)[^src-tools-docs-generate-cli-docs-py] stubs only `vertebra._vertebra` and `vertebra._bundler` so `vertebra.storage` uses its pure-Python fallbacks (`SyncGcpTokenCache` / `AsyncGcpTokenCache`, `SyncObjectStore` / `AsyncObjectStore`, `_PythonMemoryStore`). Any module imported transitively by `vertebra` or `vertebra.cli` (such as [`_zero_copy.py`](../../../../src/py/vertebra/utils/serializers/_zero_copy.py)[^src-utils-serializers-zero-copy-py], [`text.py`](../../../../src/py/vertebra/utils/text.py)[^src-utils-text-py], [`extensions/litestar/store.py`](../../../../src/py/vertebra/extensions/litestar/store.py)[^src-extensions-litestar-store-py], or [`embeddings/_google.py`](../../../../src/py/vertebra/embeddings/_google.py)) must import storage primitives from `vertebra.storage` (never directly from `vertebra._storage`), resolve `vertebra._vertebra` native functions with `callable(fn) and not isinstance(fn, type)` (and register function names in `_make_stub_getattr` `fallback_none_attrs` so stub `type(attr_name, (), {})` classes are never mistaken for compiled Rust functions), and resolve optional framework base classes via `import_optional(...)` with pure-Python fallbacks.
- **Thin `vertebra.utils` Package Contract (`src/py/vertebra/utils/__init__.py`)**: `src/py/tests/unit/test_utils_init.py::test_utils_package_is_thin` enforces that [`src/py/vertebra/utils/__init__.py`](../../../../src/py/vertebra/utils/__init__.py)[^src-utils-init-py] contains only its module docstring (`"""Vertebra utilities — import each submodule explicitly at call sites."""`) with zero non-module attributes in `dir(vertebra.utils)` (including `from __future__ import annotations` or `TYPE_CHECKING`). Always import `vertebra.utils.*` submodules explicitly at call sites.
- **Litestar Dynamic `dto_transfer_function_*` Code Objects in `coverage xml`**: Litestar's `MsgspecDTO` dynamically `compile()`s transfer functions (`dto_transfer_function_<hash>`) with synthetic filenames that have no backing source file on disk. Without `[tool.coverage.report] ignore_errors = true` in [`pyproject.toml`](../../../../pyproject.toml) (plus `-i` on `coverage xml -i` / `coverage html -i` in `Makefile`), `make coverage` fails with `No source for code: '.../dto_transfer_function_...'`.
- **Fallback to `src/ts/vertebra-client/src` When `make py-test` Runs Without `make ts-build`**: In CI, `test-python` runs `make py-test` without running `make ts-build` first, so `src/py/vertebra/server/_js/bundle/` (which is `.gitignore`d) is absent. `ImportMapGenerator.sync_wheel_client` and `DevMiddleware` must fall back to `src/ts/vertebra-client/dist/` or `src/ts/vertebra-client/src/` when the wheel bundle directory does not exist.
- **`msgspec.Struct` Per-Field Docstrings in `scan_public_docstrings`**: `tools.docs.scan_public_docstrings::scan_undocumented_typeddicts` flags `Attributes:` sections in public `msgspec.Struct` class docstrings (`has_duplicate_class_attributes=True`). Document `msgspec.Struct` fields using inline per-field docstrings immediately below each field definition rather than a class-level `Attributes:` block.
- **`GuardProtocol` Subclass Invocation (`agent/core/guards.py`)**: `GuardProtocol` is a `typing.Protocol` defining `async def __call__(self, ctx: AgentContext) -> None: ...`. Any guard class that subclasses `GuardProtocol` and implements `async def check(self, context: Any) -> None:` still has `callable(guard) is True` because it inherits the no-op `Protocol.__call__` stub. Always check `check_fn = getattr(guard, "check", None)` before falling back to `guard(ctx)` in `Tool.invoke` and `GuardChain.execute`.

## SQLSpec Preparation, Native Binds and Optional Evidence

- **A generic callable wrapper needs the original signature and an owned invocation**: `build_tools` supports named parameters, rejects positional-only/variadic or reserved declarations, and exposes resolved types to `Tool.invoke`; an input schema alone around `**kwargs` does not preserve conversion. Defaults are materialized and bound arguments deep-copied before policy, approval and suspension. Execution uses that same snapshot, while the ticket owns an independent copy; never validate one object and execute later from mutable caller data. Native `ToolPolicyError` becomes an agent permission error only in the generic bridge.[^src-provider-bridge][^src-provider-contracts][^src-provider-snapshot-tests]
- **Python equality does not identify an approved native bind**: `True == 1 == 1.0`, but BigQuery assigns distinct `BOOL`/`INT64`/`FLOAT64` types. The provider bridge compares converted types and values recursively, including container contents and Struct/dataclass fields; a scalar type change requires new approval even inside a nested record. Keep this check alongside the owned invocation snapshot.[^src-provider-bridge][^src-provider-snapshot-tests][^src-bigquery-bind-types]
- **Reject unsupported tenant sessions before constructing them**: BigQuery/Spanner sync/async config and driver constructors reject non-null tenant enforcement before base/core/config/broker access. Per-method rejection misses alternate script/Arrow/batch/snapshot routes and can already have caused side effects.[^src-gcp-tenant-bigquery][^src-gcp-tenant-spanner]
- **Prepared metadata is part of the result contract**: `StatementStack` row-producing operations still use the `execute` method. Use the prepared statement's `returns_rows()` and supported `SQLResult`/`StackResult` factories; a leading SQL keyword or a synthetic `rows=` result constructor does not preserve the installed upstream contract.[^src-stack-implementation][^src-stack-tests]
- **Valid parser output can still be invalid native input**: Check emitted placeholders and parameter shape at the real native driver's external-client boundary. PostgreSQL sync pyformat, async numeric, MySQL pyformat, BigQuery/GoogleSQL Spanner named-at and Spangres numeric defaults are wrapper-owned; preserve explicit caller config and the same prepared statement's classification. Arrow options are execution settings, not bind names, and batched sets need `is_many=True` preparation.[^src-stack-implementation][^src-stack-tests]
- **A skipped converter is no bind acceptance**: PostgreSQL primitive tests use `pytest.importorskip` for psycopg's actual query/parameter converter. A converter-enabled run is separate evidence from a suite where those cases skip; temporary cached-package paths belong in run evidence, never production imports or maintained documentation.[^src-converter-tests]
- **Absent feeds are not a shared feed**: `None is None` must not suppress local channel delivery. Shared-feed duplicate suppression requires non-null object identity; a different explicit output topic adds a delivery there while preserving the original feed observation. Do not generalize this local rule to distributed exactly-once semantics.[^src-channel-implementation][^src-channel-regressions]

## Cross-extension Arrow capsules and release

- **Independent globals**: A region registered in an optional extension's private `ACTIVE_ARROW_SHM_REGIONS` map cannot be released through the core extension's copy of that map. Use `write_shm_arrow_region_unregistered` or `ShmArrowStreamWriter::finish_unregistered` for capsule transfer.[^src-db-arrow-shm]
- **Single ownership claim**: `ArrowCursor._from_shm_capsule` validates the capsule name, pointer, and declared size before atomically taking `ctx`. A second adoption raises `ValueError`. Keep failed construction and unconsumed-capsule cleanup in the producer's destructor.[^src-db-py-async][^src-py-arrow-cursor]
- **Unlink and unmap differ**: `release()` and `close()` unlink the Unix SHM name while retained cursor backing keeps the mapping readable. The producer's `drop_fn` releases its mapping ownership when the last backing owner drops. Tests must distinguish name removal from memory lifetime.[^src-core-shm-capsule][^src-py-arrow-cursor]
- **Completion drainer identity**: Give every extension a distinct event-loop drainer attribute. Reusing another extension's attribute crosses separately linked PyO3 classes and completion queues.[^src-db-py-async]

The [architecture chapter](../architecture/worker/pyo3-ffi-and-batch-dispatch.md#database-extension-boundary) describes the handoff, and [ADR-004](../decisions/adr-004-optional-driver-wheels-and-shm-ownership.md) records its rationale.

Provenance sources: [^src-vertebra-py-src-transport-rs].

[^src-vertebra-py-src-lib-rs]: `src/rs/vertebra-py/src/lib.rs` — Rust implementation module (src/rs/vertebra-py/src/lib.rs).
[^src-vertebra-py-src-transport-rs]: `src/rs/vertebra-py/src/transport.rs` — Rust implementation module (src/rs/vertebra-py/src/transport.rs).
[^src-vertebra-py-src-request-rs]: `src/rs/vertebra-py/src/request.rs` — Rust implementation module (src/rs/vertebra-py/src/request.rs).
[^src-vertebra-py-src-rsgi-http-rs]: `src/rs/vertebra-py/src/rsgi/http.rs` — Rust implementation module (src/rs/vertebra-py/src/rsgi/http.rs).
[^src-vertebra-py-src-wsgi-runner-rs]: `src/rs/vertebra-py/src/wsgi/runner.rs` — Rust implementation module (src/rs/vertebra-py/src/wsgi/runner.rs).
[^src-vertebra-bundler-src-lib-rs]: `src/rs/vertebra-bundler/src/lib.rs` — Rust implementation module (src/rs/vertebra-bundler/src/lib.rs).
[^src-vertebra-storage-src-lib-rs]: `src/rs/vertebra-storage/src/lib.rs` — Rust implementation module (src/rs/vertebra-storage/src/lib.rs).
[^src-vertebra-http-src-main-rs]: `src/rs/vertebra-http/src/main.rs` — Rust implementation module (src/rs/vertebra-http/src/main.rs).
[^src-server-worker-py]: `src/py/vertebra/server/worker.py` — Python implementation module (src/py/vertebra/server/worker.py).
[^src-server-asgi-runner-py]: `src/py/vertebra/server/_asgi/_runner.py` — Python implementation module (src/py/vertebra/server/_asgi/_runner.py).
[^src-server-asgi-wire-py]: `src/py/vertebra/server/_asgi/_wire.py` — Python implementation module (src/py/vertebra/server/_asgi/_wire.py).
[^src-server-wsgi-adapter-py]: `src/py/vertebra/server/_wsgi/_adapter.py` — Python implementation module (src/py/vertebra/server/_wsgi/_adapter.py).
[^src-server-gil-audit-py]: `src/py/vertebra/server/_gil_audit.py` — Python implementation module (src/py/vertebra/server/_gil_audit.py).
[^src-supervisor-supervisor-py]: `src/py/vertebra/supervisor/_supervisor.py` — Python implementation module (src/py/vertebra/supervisor/_supervisor.py).
[^src-extensions-litestar-plugin-py]: `src/py/vertebra/extensions/litestar/plugin.py` — Python implementation module (src/py/vertebra/extensions/litestar/plugin.py).
[^src-extensions-litestar-offload-py]: `src/py/vertebra/extensions/litestar/offload.py` — Python implementation module (src/py/vertebra/extensions/litestar/offload.py).
[^src-extensions-litestar-fastpath-py]: `src/py/vertebra/extensions/litestar/fastpath.py` — Python implementation module (src/py/vertebra/extensions/litestar/fastpath.py).
[^src-extensions-litestar-store-py]: `src/py/vertebra/extensions/litestar/store.py` — Python implementation module (src/py/vertebra/extensions/litestar/store.py).
[^src-utils-serializers-zero-copy-py]: `src/py/vertebra/utils/serializers/_zero_copy.py` — Python implementation module (src/py/vertebra/utils/serializers/_zero_copy.py).
[^src-utils-text-py]: `src/py/vertebra/utils/text.py` — Python implementation module (src/py/vertebra/utils/text.py).
[^src-utils-init-py]: `src/py/vertebra/utils/__init__.py` — Python implementation module (src/py/vertebra/utils/__init__.py).
[^src-tools-docs-generate-cli-docs-py]: `tools/docs/generate_cli_docs.py` — Repository tooling module (tools/docs/generate_cli_docs.py).
[^src-pyproject-toml]: `pyproject.toml` — Python package metadata, dependencies, and tool configuration.

[^src-db-arrow-shm]: `src/rs/vertebra-db/src/arrow_shm.rs` — Registered and unregistered Arrow SHM writers.

[^src-db-py-async]: `src/rs/vertebra-db/src/py_async.rs` — Capsule destructor and per-extension completion queues.

[^src-core-shm-capsule]: `src/rs/vertebra-core/src/shm.rs` — SHM capsule and unlink versus mapping lifetime.

[^src-py-arrow-cursor]: `src/rs/vertebra-py/src/db.rs` — Single-consumer capsule adoption and cursor release.

[^src-stack-implementation]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Prepared SQLSpec result construction and native bind execution.
[^src-stack-tests]: `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` — Native bind, classification, Arrow and batched execution regressions.
[^src-converter-tests]: `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` — Optional psycopg query and parameter converter tests.
[^src-channel-implementation]: `src/py/vertebra/server/_channels.py` — Non-null shared-feed identity and independent output channels.
[^src-channel-regressions]: `src/py/tests/unit/db/test_native_db_drivers.py` — Shared-feed, absent-feed and per-output duplicate suppression tests.
[^src-provider-bridge]: `src/py/vertebra/agent/tools/__init__.py` — Generic typed invocation and caller-bound provider confirmation.
[^src-provider-contracts]: `src/py/vertebra/_typing.py` — Agent-neutral provider and policy error contracts.
[^src-provider-snapshot-tests]: `src/py/tests/unit/agent/test_runtime_and_workflows.py` — Owned invocation and approval snapshots under caller mutation.
[^src-bigquery-bind-types]: `src/py/vertebra/db/adapters/bigquery/core.py` — Type-sensitive native BOOL, INT64 and FLOAT64 parameter encoding.
[^src-gcp-tenant-bigquery]: `src/py/vertebra/extensions/sqlspec/bigquery.py` — Early unsupported tenant enforcement in BigQuery constructors.
[^src-gcp-tenant-spanner]: `src/py/vertebra/extensions/sqlspec/spanner.py` — Early unsupported tenant enforcement in Spanner constructors.
