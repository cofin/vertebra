---
type: Spec
flow_id: async-loop-and-framework-fastpath
title: "Unified Event Loop Backends, C-API Coroutine Stepping, Rust Completion Ring, 6-Framework Stacked FastPath & Shared Native Database Pool"
description: "Unified LoopBackend (uvloop 0.23+, winloop 0.8+, rloop, asyncio), C-API PyIter_Send native coroutine driver (drive_native_batch), AsgiProtocol/AsgiSend/WsgiStartResponse send_raw_response, per-loop RustCompletionRing + Notifier in vertebra-storage and SyncPool, zero-closure stacked FastPath, and cross-framework shared native database pool/broker across Vertebra Native, Litestar, FastAPI, Starlette, Flask, and Django."
state: planned
parent_prd: null
research:
  - research/event-loop-and-async-optimizations_20261101.md
plan_revision: 2
plan_commit: 34429ff8c50d4622d0cacbd1288525f8a1211dc8
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-01T23:48:00Z
updated_at: 2026-11-02T00:03:00Z
tags:
  - feature
  - server
  - pyo3
  - asgi
  - wsgi
  - fastpath
  - database
  - uvloop
  - winloop
  - rloop
---

# Flow: Unified Event Loop Backends, C-API Coroutine Stepping, Rust Completion Ring, 6-Framework Stacked FastPath & Shared Native Database Pool

## 1. Specification & Context

### 1.1 Problem Statement & Objectives
Vertebra's Rust HTTP ingress (`vertebra-http`) and C-API ASGI/RSGI runners (`drive_asgi_batch`, `drive_rsgi_batch`) already step coroutines once in C via `PyIter_Send` and flush batch responses without `asyncio.Task` allocations on non-suspending routes. However, five significant async and cross-framework bottlenecks remain:
1. **Event Loop Backend Gaps & Python 3.14/3.15 Policy Deprecations**: `pyproject.toml` pins `uvloop>=0.21.0` (versions `<0.23.0` silently ignored `asyncio.eager_task_factory` and lacked `3.15`/`3.15t` wheels) and omits `winloop` on Windows and optional `rloop`. Meanwhile, `src/py/vertebra/server/_gil_audit.py::GilAuditReport.quarantine_unsafe_modules` calls deprecated `asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())` (which is also a functional no-op because `create_event_loop()` calls `uvloop.new_event_loop()` directly).
2. **Native `async def` Coroutine/Task Allocation Overhead**: In `src/py/vertebra/server/_dispatch.py`, native `async def` routes with parameters allocate an outer `_invoke_compiled_async` coroutine frame even when all extractors/guards/dependencies are synchronous, wrap again in `_await_async_item` + `_AsyncErrorStub`, and call `loop.create_task()` on every request instead of stepping once via C-API `PyIter_Send`.
3. **Per-Operation Tokio GIL Contention & Self-Pipe Syscalls in `_storage` and `SyncPool`**: In `src/rs/vertebra-storage/src/lib.rs::spawn_rust_task`, every completed Tokio storage task calls `Python::attach` on the Tokio worker thread and invokes `loop.call_soon_threadsafe` (1 self-pipe `write`/`read` syscall per operation), while `src/py/vertebra/utils/sync_tools.py::SyncPool.submit_async` dispatches through `loop.run_in_executor` (triggering per-operation `call_soon_threadsafe` wakeups across all `vertebra.db` async drivers and `SHMBrokerTransport.acquire_async`).
4. **Fragmented 6-Framework `FastPath` & Unaccelerated `App.mount()` Stacking**:
   - `Litestar`, `FastAPI`, and `Starlette` `wrap_asgi` allocate `buffered_messages = []`, `_buffering_receive`, and `_tracked_send` closures before checking route match, then allocate two ASGI response dicts (`http.response.start`, `http.response.body`) and await `send()` twice.
   - `FastAPI` `dispatch_handler_direct` unconditionally instantiates `starlette.requests.Request` even when unused and dispatches synchronous `def` endpoints via `anyio.to_thread.run_sync(lambda: ...)`.
   - `Flask` and `Django` `FastPath` bail out (`return None`) on all `async def` views, decode response headers back to `latin-1` strings in `dispatch_wsgi`, and `MsgspecView.as_view` re-instantiates `cls()` on every request.
   - Mounting any sub-application via `app.mount("/prefix", sub_app)` wraps the sub-application in `AsgiRunner` or `WsgiAdapter` without attaching its framework `FastPath` or stripping `mount_prefix` during `FastPath` route matching.
5. **Duplicate Native Database Pools & Isolated `LeaseBroker` Instances Across Stacked Frameworks**:
   - When multiple mounted frameworks declare native database adapters pointing to the same database endpoint (e.g., a Litestar app with `PostgreSQLAsyncConfig` or `SQLAlchemyAsyncConfig("postgresql+vtb://...")` mounted alongside a Django app with `vertebra.extensions.django.db.backends.postgresql`), `DatabaseWrapperMixin` creates an isolated `LeaseBroker`, `DBAPIConnection` / `AsyncDBAPIConnection` instantiate a new driver per connection, and each framework opens separate connection pools and separate `CloudSQLDialer` / `AlloyDBDialer` instances.

**Non-Goals**:
- Implementing a full custom `asyncio.AbstractEventLoop` in `vertebra-py` (which would duplicate ~5,000+ lines of SSL/TLS, UDS, DNS, and subprocess state machines already provided by `uvloop`/`winloop`/`asyncio`).
- Modifying HTTP wire framing in `vertebra-http`.

### 1.2 Upstream Execution Baseline
- **Branch**: `feat/async-loop-and-framework-fastpath` branched from `main` (`696f25a`, knowledge sync `34429ff8c50d4622d0cacbd1288525f8a1211dc8`).
- **Promoted Research**: [`research/event-loop-and-async-optimizations_20261101.md`](research/event-loop-and-async-optimizations_20261101.md).

### 1.3 Code Analysis & Seam Summary
| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Loop Backends & GIL Audit | `src/py/vertebra/server/_loops.py::LoopBackend`, `src/py/vertebra/server/_loops.py::create_event_loop`, `src/py/vertebra/server/_loops.py::EventLoopPool`, `src/py/vertebra/server/_gil_audit.py::GilAuditReport`, `src/py/vertebra/cli/_launch.py::ServerLaunchEnv` | **In-process**: Factory-based loop selection (`auto`, `uvloop`, `winloop`, `rloop`, `asyncio`) honoring `VERTEBRA_LOOP` and `quarantined_modules` with zero `EventLoopPolicy` calls. |
| Rust C-API Stepper & Direct Send | `src/rs/vertebra-py/src/asgi.rs::AsgiScope`, `src/rs/vertebra-py/src/asgi.rs::AsgiProtocol`, `src/rs/vertebra-py/src/asgi.rs::SteppedCoroutine`, `src/rs/vertebra-py/src/asgi.rs::drive_native_batch`, `src/rs/vertebra-py/src/wsgi/runner.rs::WsgiStartResponse`, `src/py/vertebra/server/_asgi/_queues.py::AsgiSend`, `src/py/vertebra/server/_dispatch.py::compile_handler_spec` | **In-process**: Single-step C-API `PyIter_Send` native coroutine driver + `AsgiScope.body_bytes()` + `AsgiProtocol.__call__` + direct `send_raw_response` on `AsgiProtocol`, `AsgiSend`, and `WsgiStartResponse` (`_WsgiStartResponse`). |
| Storage & SyncPool Completion Ring | `src/rs/vertebra-storage/src/lib.rs::spawn_rust_task`, `src/py/vertebra/utils/sync_tools.py::SyncPool` | **Local-substitutable**: Per-loop MPSC completion queue + `vertebra_core::notify::Notifier` (`eventfd`/`kqueue`) registered via `loop.add_reader` in `_storage` and batched single-wakeup completion draining in `SyncPool.submit_async`. |
| ASGI Framework FastPaths | `src/py/vertebra/server/_fastpath.py::wrap_asgi_fastpath`, `src/py/vertebra/extensions/litestar/fastpath.py::LitestarFastPathBypass`, `src/py/vertebra/extensions/fastapi/fastpath.py::FastAPIFastPath`, `src/py/vertebra/extensions/starlette/fastpath.py::StarletteFastPath` | **In-process**: Shared zero-closure `wrap_asgi_fastpath`, `AsgiScope.body_bytes()` zero-copy body extraction, `send_raw_response`, lazy `Request` allocation, and `sync_to_thread=False` inline sync execution. |
| Flask & Django FastPaths | `src/py/vertebra/extensions/flask/fastpath.py::FlaskFastPath`, `src/py/vertebra/extensions/django/fastpath.py::DjangoFastPath`, `src/py/vertebra/extensions/django/fastpath.py::MsgspecView` | **In-process**: Full `async def` view execution, `wrap_asgi`, cached stateless `MsgspecView` instances, and `WsgiStartResponse.send_raw_response`. |
| Multi-Framework Stacking & Shared Native DB Pool | `src/py/vertebra/server/_fastpath.py::attach_framework_fastpath`, `src/py/vertebra/server/worker.py::_instantiate_mount_runners`, `src/py/vertebra/db/_registry.py::DatabaseRegistry`, `src/py/vertebra/extensions/django/db/backends/__init__.py::DatabaseWrapperMixin`, `src/py/vertebra/extensions/sqlspec/adapters.py::SyncAdapterConfig`, `src/py/vertebra/extensions/sqlalchemy/dialects.py::DBAPIConnection` | **In-process**: Auto-detection and prefix-aware `FastPath` attachment across `app.mount("/prefix", sub_app)` for all 6 frameworks, plus canonical endpoint fingerprinting and shared native driver/pool/dialer/`LeaseBroker` reuse via `DatabaseRegistry.default()`. |

### 1.4 Relevant Patterns, Domain Terms & Knowledge Chapters
- [PyO3 FFI & Vectorized Batch Dispatch](../../knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md)
- [ASGI, RSGI & WSGI Runners](../../knowledge/architecture/worker/asgi-and-wsgi-runners.md)
- [Shared-Memory Connection Broker & Statement Cache](../../knowledge/domains/data/broker-and-shm-leases.md)
- [Framework Extensions: Litestar, Starlette, FastAPI, Flask, and Django](../../knowledge/domains/extensions/litestar.md)
- [SQLSpec, Advanced Alchemy & SQLAlchemy Dialect Extensions](../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md)
- [Hot-Path Caching & Single-Wake Async Dispatch](../../knowledge/patterns/python/hot-path-and-async-dispatch.md)
- [PyO3 FFI, Embedded Globals & Python Runtime Gotchas](../../knowledge/gotchas/pyo3-and-python-runtime.md)
- Skills: [`pyo3-bridge`](../../../skills/pyo3-bridge/SKILL.md), [`python-framework`](../../../skills/python-framework/SKILL.md)

---

## 2. Requirements & Architecture

### 2.1 Functional Requirements
- **R1 (`LoopBackend` & Policy Deprecation Removal)**: Provide `LoopBackend(StrEnum)` (`AUTO = "auto"`, `UVLOOP = "uvloop"`, `WINLOOP = "winloop"`, `RLOOP = "rloop"`, `ASYNCIO = "asyncio"`), `resolve_loop_backend`, and updated `create_event_loop` / `EventLoopPool` in `src/py/vertebra/server/_loops.py` + `--loop` (`VERTEBRA_LOOP`) in `vtb run`/`vtb dev`; bump `uvloop>=0.23.0; sys_platform != 'win32'`, add `winloop>=0.8.1; sys_platform == 'win32'`, and add optional `rloop = ["rloop>=0.5.0; sys_platform != 'win32'"]` + Mypy overrides in `pyproject.toml`; remove all `asyncio.set_event_loop_policy` / `asyncio.DefaultEventLoopPolicy` calls from `src/py/vertebra/server/_gil_audit.py` and update `uvloop` quarantine threshold in `detect_unsafe_modules()` to `<= (0, 21, 99)` (matching prefix `"uvloop=="` entries in `quarantined_modules`).
- **R2 (C-API `drive_native_batch`, `AsgiScope.body_bytes()`, & Direct Raw Send)**: Flatten `compile_handler_spec` in `src/py/vertebra/server/_dispatch.py` so `async def` handlers with only synchronous parameter extractors/guards/dependencies return `handler(*args)` from a plain `def` invoker (1 coroutine frame instead of 2); add `drive_native_batch` in `src/rs/vertebra-py/src/asgi.rs` (`ProtocolFinalizer::Native` on `SteppedCoroutine`) to step native `async def` coroutines via `PyIter_Send` and encode `PYGEN_RETURN` results via `encode_py_headers_into` + `ResponseSubmitter::submit_batch_raw_inner`; expose `body_bytes(&self, py: Python<'_>) -> Bound<'_, PyBytes>` on `AsgiScope` and `AsgiProtocol`; implement `__call__` on `AsgiProtocol` so `proto_bound` can be passed directly as `send`; and add `send_raw_response` to `AsgiProtocol` (`src/rs/vertebra-py/src/asgi.rs`), `AsgiSend` (`src/py/vertebra/server/_asgi/_queues.py`), and `WsgiStartResponse` (`src/rs/vertebra-py/src/wsgi/runner.rs`).
- **R3 (`RustCompletionRing` + `Notifier` for `_storage` & Batched `SyncPool` Wakeups)**: Replace per-op Tokio-thread `Python::attach` + `loop.call_soon_threadsafe` in `src/rs/vertebra-storage/src/lib.rs::spawn_rust_task` with a per-loop `RustCompletionRing` (MPSC completion queue + `AtomicBool` wake-coalescing flag + `vertebra_core::notify::Notifier` registered via `loop.add_reader` on Unix with `call_soon_threadsafe` fallback when `add_reader` is unsupported); add per-loop batched completion draining in `src/py/vertebra/utils/sync_tools.py::SyncPool.submit_async` so concurrent async database operations across all `vertebra.db` drivers and `SHMBrokerTransport.acquire_async` coalesce into a single event-loop wakeup per batch.
- **R4 (Unified Zero-Closure ASGI `FastPath` for Litestar, FastAPI, and Starlette)**: Introduce `src/py/vertebra/server/_fastpath.py` with `wrap_asgi_fastpath`, `extract_scope_body_fast` (synchronously extracting `scope.body_bytes()` on native `AsgiScope` before falling back to `await receive()`), and `send_asgi_direct` (`send.send_raw_response` single C-API call); add `wrap_asgi(self, app: Any) -> Any` to `LitestarFastPathBypass` and wire across `Litestar`, `FastAPI`, and `Starlette` `FastPath`; make `starlette.requests.Request` instantiation lazy in `FastAPI` `dispatch_handler_direct` and default fastpath sync endpoints in `FastAPI` and `Starlette` to inline execution (`sync_to_thread: bool = False`, configurable via `sync_to_thread` or `__vertebra_sync_to_thread__`).
- **R5 (Full `async def` & Direct WSGI/ASGI `FastPath` for Flask and Django)**: Enable `async def` views in `Flask` (`src/py/vertebra/extensions/flask/fastpath.py`) and `Django` (`src/py/vertebra/extensions/django/fastpath.py`), including `async def` HTTP method handlers and cached stateless instances in `MsgspecView.as_view`; add `dispatch_async` and `wrap_asgi` to both `FlaskFastPath` and `DjangoFastPath`; make the target callable optional in `wrap_wsgi`; and wire `WsgiStartResponse.send_raw_response` in `dispatch_wsgi`.
- **R6 (Auto-Attached & Prefix-Aware Multi-Framework Stacking on `App.mount`)**: Add `attach_framework_fastpath(app: Any, *, mount_prefix: str = "", protocol: str = "auto") -> Any` in `src/py/vertebra/server/_fastpath.py` and wire it into `src/py/vertebra/server/worker.py::_instantiate_mount_runners` and `src/py/vertebra/server/_wsgi/_adapter.py::WsgiAdapter` so mounting any `Litestar`, `FastAPI`, `Starlette`, `Flask`, or `Django` sub-application via `app.mount("/prefix", sub_app)` automatically strips the mount `root_path` prefix when matching `FastPath` routes and executes every stacked framework through its C-API zero-copy `FastPath`.
- **R7 (Cross-Framework Shared Native Database Pool, Cloud Dialer & SHM `LeaseBroker` Reuse)**: Add canonical endpoint fingerprinting (`DatabaseRegistry.pool_fingerprint(config_or_url)`) and process-wide shared pool/broker resolution (`DatabaseRegistry.default()`, `DatabaseRegistry.default_broker()`, `get_shared_driver(config_or_url)`, `get_shared_sync_driver(config_or_url)`) in `src/py/vertebra/db/_registry.py`, and wire `share_pool: bool = True` across `DatabaseWrapperMixin` (`src/py/vertebra/extensions/django/db/backends/__init__.py`), `SyncAdapterConfig` / `AsyncAdapterConfig` (`src/py/vertebra/extensions/sqlspec/adapters.py`), and `DBAPIConnection` / `AsyncDBAPIConnection` (`src/py/vertebra/extensions/sqlalchemy/dialects.py`) so native drivers declared independently in `Litestar`, `Django`, `FastAPI`, `Starlette`, `Flask`, and `Vertebra Native` apps automatically reuse the same underlying connection pool, `CloudSQLDialer` / `AlloyDBDialer`, SHM `LeaseBroker`, and `ShmStatementCache`.

### 2.2 Public Interfaces, Deep Modules & Seam Contracts
- `src/py/vertebra/server/_loops.py`:
  - `class LoopBackend(StrEnum)`: `AUTO = "auto"`, `UVLOOP = "uvloop"`, `WINLOOP = "winloop"`, `RLOOP = "rloop"`, `ASYNCIO = "asyncio"`.
  - `def resolve_loop_backend(backend: LoopBackend | str | None = None, *, quarantined_modules: frozenset[str] = frozenset()) -> LoopBackend`.
  - `def create_event_loop(backend: LoopBackend | str | None = None, *, quarantined_modules: frozenset[str] = frozenset()) -> asyncio.AbstractEventLoop`.
- `src/rs/vertebra-py/src/asgi.rs`, `src/rs/vertebra-py/src/wsgi/runner.rs`, & `src/py/vertebra/server/_asgi/_queues.py`:
  - `AsgiScope.body_bytes(self) -> bytes`
  - `AsgiProtocol.body_bytes(self) -> bytes`
  - `AsgiProtocol.__call__(self, message: dict[str, Any]) -> PyEmptyAwaitable`
  - `AsgiProtocol.send_raw_response(self, status: int, headers: list[tuple[bytes, bytes]] | None = None, body: bytes = b"") -> PyEmptyAwaitable`
  - `AsgiSend.send_raw_response(self, status: int, headers: list[tuple[bytes, bytes]] | None = None, body: bytes = b"") -> PyEmptyAwaitable`
  - `_WsgiStartResponse.send_raw_response(self, status: int, headers: list[tuple[str | bytes, str | bytes]] | None = None, body: bytes = b"") -> list[bytes]`
  - `drive_native_batch(items: list[tuple[int, Any, int, int]], loop_obj: Any = None) -> int`
- `src/py/vertebra/server/_fastpath.py`:
  - `async def extract_scope_body_fast(scope: Any, receive: Any, body: bytes | None = None, *, method: str = "GET") -> bytes`
  - `async def send_asgi_direct(send: Any, status_code: int, encoded_headers: list[tuple[bytes, bytes]], raw_body: bytes) -> None`
  - `def wrap_asgi_fastpath(bypass: Any, fallback_app: Callable[..., Any]) -> Callable[..., Any]`
  - `def attach_framework_fastpath(app: Any, *, mount_prefix: str = "", protocol: str = "auto") -> Any`
- `src/py/vertebra/db/_registry.py`:
  - `DatabaseRegistry.default() -> DatabaseRegistry`
  - `DatabaseRegistry.default_broker(*, pool_size: int = 32, max_overflow: int = 16) -> LeaseBroker`
  - `DatabaseRegistry.pool_fingerprint(config_or_url: AnyDatabaseConfig | str) -> tuple[Any, ...]`
  - `DatabaseRegistry.get_shared_driver(self, config_or_url: AnyDatabaseConfig | str) -> AnyDatabaseDriver`
  - `DatabaseRegistry.get_shared_sync_driver(self, config_or_url: AnyDatabaseConfig | str) -> AnySyncDatabaseDriver`

### 2.3 Non-Functional & Safety Constraints
- Zero inline `#` comments in Python files; use Google-style docstrings and PEP 585/604/695 type annotations exclusively.
- Zero backward-compatibility aliases (`X = Y` module-level assignments) per `PY-07`, and import shared types from `vertebra.typing` per `PY-08`.
- Preserve `#[pymodule(gil_used = false)]` across `_vertebra`, `_bundler`, and `_storage`, and gate `freelist = 128` behind `#[cfg_attr(not(Py_GIL_DISABLED), ...)]`.
- Ensure `make docs` (`--only-group doc`) continues to succeed without compiled `.so` extensions or optional web frameworks installed.

### 2.4 Acceptance Scenarios (Given / When / Then)
- **Scenario 1 (`LoopBackend` & 3.14t/3.15t Quarantine)**:
  - *Given* `create_event_loop` and `GilAuditReport.quarantine_unsafe_modules()`,
  - *When* audited on Python 3.12–3.15t with `VERTEBRA_LOOP=auto|uvloop|winloop|rloop|asyncio` or a quarantined loop module (`"uvloop==0.21.0"`),
  - *Then* `quarantine_unsafe_modules()` emits zero `DeprecationWarning`s (`DefaultEventLoopPolicy` is never called) and `create_event_loop` selects the expected loop implementation or falls back cleanly to `asyncio.new_event_loop()`.
- **Scenario 2 (C-API `drive_native_batch` & `send_raw_response`)**:
  - *Given* a native `App` batch containing both zero-suspension `async def` handlers (with path/query/typed `msgspec.Struct` parameters) and suspending `async def` handlers (`await asyncio.sleep(...)`),
  - *When* dispatched via `dispatch_batch` and `drive_native_batch`,
  - *Then* zero-suspension `async def` handlers complete synchronously in C via `PyIter_Send` -> `PYGEN_RETURN` without calling `loop.create_task`, while suspending `async def` handlers promote to `SteppedCoroutine` tasks and submit accurate responses.
- **Scenario 3 (`RustCompletionRing` in `_storage` & Batched `SyncPool` Wakeups)**:
  - *Given* 64 concurrent `ObjectStore.get_async` / `put_async` calls and 64 concurrent `SyncPool.submit_async` / `AsyncPostgreSQLDriver` / `AsyncSQLiteDriver` calls on an active event loop,
  - *When* awaited concurrently via `asyncio.gather`,
  - *Then* Tokio and worker pool threads enqueue completions into the per-loop completion queue without calling `Python::attach` on Tokio threads, coalesce wakeups, and resolve all Python futures accurately.
- **Scenario 4 (Unified 6-Framework `FastPath`, Stacked `App.mount` & Shared Native Database Pool)**:
  - *Given* a Vertebra `App` with native `async def` routes that mounts a `Litestar` app at `/litestar`, a `FastAPI` app at `/fastapi`, a `Starlette` app at `/starlette`, a `Flask` app (with both `def` and `async def` views) at `/flask`, and a `Django` app (with `def`, `async def`, and `MsgspecView` views) at `/django`, where both `Litestar` (`PostgreSQLSyncConfig` / `SQLiteSyncConfig` or `+vtb` dialect) and `Django` (`vertebra.extensions.django.db.backends.postgresql` / `sqlite3`) declare native database adapters targeting the same database endpoint,
  - *When* requests are dispatched to each framework (both standalone and under its mounted prefix),
  - *Then* every framework executes through its zero-closure C-API `FastPath` (`send_raw_response`) AND shares the exact same underlying native driver connection pool, `CloudSQLDialer` / `AlloyDBDialer`, SHM `LeaseBroker`, and `ShmStatementCache`.

### 2.5 Alternatives Considered
- **Alternative A (Selected): Unified `LoopBackend` + C-API `PyIter_Send` (`drive_native_batch`) + `RustCompletionRing` + 6-Framework Stacked `FastPath` + Shared `DatabaseRegistry.default()` Pool**. Reuses `vertebra-py`'s `SteppedCoroutine` and `vertebra-core`'s `Notifier` while keeping 100% stdlib `asyncio` compatibility (`uvloop`/`winloop`/`rloop`/`asyncio`), unifying fast-path dispatch across all 6 frameworks, and deduplicating native database pools and SHM brokers across stacked apps.
- **Alternative B (Rejected): Custom Rust `AbstractEventLoop` in `vertebra-py`**. Rejected because Vertebra already handles all HTTP/1/2/3, TLS, and socket I/O in `vertebra-http`; reimplementing ~5,000+ lines of stdlib `AbstractEventLoop` methods (SSL/TLS bio, UDS, DNS, subprocess pipes) in `vertebra-py` would risk third-party library incompatibilities for negligible (~150 ns/batch) gain over `uvloop`/`winloop` + `PyIter_Send`.
- **Alternative C (Rejected): Per-Framework Isolated `wrap_asgi` and Isolated Database Pools Without Mount Stacking**. Rejected because it leaves duplicated closure/dict allocations in `Litestar`, `FastAPI`, and `Starlette`, leaves `Flask` and `Django` `async def` views unaccelerated, forces `app.mount()` sub-applications onto slow fallback paths, and multiplies database connection pools across stacked frameworks.

---

## 3. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task(s) | Verification Strategy | Confirmed Public Seam & Canonical Test Surface |
| --- | --- | --- | --- |
| R1 (`LoopBackend` & `_gil_audit.py` policy deprecation removal) | `t01` | `behavior_tdd` | `src/py/tests/unit/server/test_runtime_tuning.py` |
| R2 (C-API `drive_native_batch`, `AsgiScope.body_bytes()`, `AsgiProtocol.__call__`, flattened `compile_handler_spec`, `send_raw_response`) | `t02` | `behavior_tdd` | `src/py/tests/unit/server/test_asgi_capi_runner.py`, `src/py/tests/unit/server/test_wsgi_batch_runner.py` |
| R3 (`RustCompletionRing` + `Notifier` in `_storage` & batched `SyncPool.submit_async` wakeups) | `t03` | `behavior_tdd` | `src/py/tests/unit/storage/test_gcs_token_refresh.py`, `src/py/tests/unit/db/test_native_db_drivers.py` |
| R4 (Zero-closure ASGI `FastPath` for Litestar, FastAPI, Starlette + lazy `Request` & inline sync) | `t04` | `behavior_tdd` | `src/py/tests/unit/server/test_litestar_direct_fastpath.py`, `src/py/tests/unit/extensions/test_fastapi.py`, `src/py/tests/unit/extensions/test_starlette.py` |
| R5 (`Flask` & `Django` `async def` views, cached `MsgspecView`, `wrap_asgi`, WSGI `send_raw_response`) | `t05` | `behavior_tdd` | `src/py/tests/unit/extensions/test_flask.py`, `src/py/tests/unit/extensions/test_django.py` |
| R6 & R7 (Auto-attached prefix-aware `FastPath` stacking across `App.mount` + cross-framework shared native `DatabaseRegistry.default()` pool & `LeaseBroker`) | `t06` | `behavior_tdd` | `src/py/tests/unit/extensions/test_fastapi.py`, `src/py/tests/unit/extensions/test_django.py`, `src/py/tests/unit/server/test_asgi_capi_runner.py` |

---

## 4. Implementation Plan

### Phase 1: Event Loop Backends, C-API Coroutine Stepper & Rust Completion Ring
- [ ] t01 — Add unified `LoopBackend` (`uvloop`, `winloop`, `rloop`, `asyncio`) selector and remove deprecated `EventLoopPolicy` APIs (tasks/t01-loop-backend-and-gil-audit.md)
- [ ] t02 — Add C-API `drive_native_batch` (`PyIter_Send`), `AsgiScope.body_bytes()`, `AsgiProtocol.send_raw_response`, `AsgiSend.send_raw_response`, and `_WsgiStartResponse.send_raw_response` in `vertebra-py` (tasks/t02-capi-native-async-stepper-and-raw-send.md)
- [ ] t03 — Replace per-op `call_soon_threadsafe` with `RustCompletionRing` + `Notifier` in `vertebra-storage` and batched `SyncPool.submit_async` wakeups (tasks/t03-rust-completion-ring-storage-and-db.md)
- **Phase 1 Recovery Checkpoint**: `make dev && uv run --no-sync pytest src/py/tests/unit/server/test_runtime_tuning.py src/py/tests/unit/server/test_asgi_capi_runner.py src/py/tests/unit/server/test_wsgi_batch_runner.py src/py/tests/unit/storage/test_gcs_token_refresh.py src/py/tests/unit/db/test_native_db_drivers.py -q`

### Phase 2: Unified 6-Framework Zero-Copy `FastPath`, Multi-Framework Stacking (`App.mount`) & Shared Native Database Pool
- [ ] t04 — Unify zero-closure `wrap_asgi`, `send_raw_response`, lazy `Request`, and inline sync execution across Litestar, FastAPI, and Starlette `FastPath` (tasks/t04-asgi-fastpath-litestar-fastapi-starlette.md)
- [ ] t05 — Add `async def` view execution, cached `MsgspecView`, `wrap_asgi`, and `_WsgiStartResponse.send_raw_response` to Flask and Django `FastPath` (tasks/t05-flask-and-django-async-and-wsgi-fastpath.md)
- [ ] t06 — Auto-attach stacked `FastPath` dispatchers across `App.mount()` and share native database pools & SHM `LeaseBroker` across all 6 frameworks (tasks/t06-multi-framework-stacked-mount-fastpath.md)
- **Phase 2 Recovery Checkpoint**: `make lint && uv run --no-sync pytest src/py/tests/unit/server/test_litestar_direct_fastpath.py src/py/tests/unit/extensions/test_fastapi.py src/py/tests/unit/extensions/test_starlette.py src/py/tests/unit/extensions/test_flask.py src/py/tests/unit/extensions/test_django.py src/py/tests/unit/server/test_asgi_capi_runner.py -q`

---

## 5. Continuity Snapshot
- **Flow ID**: `async-loop-and-framework-fastpath` (`state: planned`, `plan_revision: 2`, `plan_commit: 34429ff8c50d4622d0cacbd1288525f8a1211dc8`)
- **Current Task**: `null`
- **Last Verified Checkpoint**: `34429ff8c50d4622d0cacbd1288525f8a1211dc8`
- **Key Decisions**:
  - Adopt unified `LoopBackend` (`auto`, `uvloop>=0.23.0`, `winloop>=0.8.1`, `rloop>=0.5.0`, `asyncio`) and remove deprecated `EventLoopPolicy` calls.
  - Drive native `async def` handlers via C-API `PyIter_Send` (`drive_native_batch`), expose `AsgiScope.body_bytes()` and `AsgiProtocol.__call__`, and add `send_raw_response` on `AsgiProtocol`, `AsgiSend`, and `_WsgiStartResponse`.
  - Replace per-op Tokio `Python::attach` + `call_soon_threadsafe` in `vertebra-storage` with a per-loop `RustCompletionRing` + `Notifier` (`eventfd`/`kqueue`) and coalesce `SyncPool.submit_async` wakeups.
  - Unify and stack `FastPath` across all 6 frameworks (`Vertebra Native`, `Litestar`, `FastAPI`, `Starlette`, `Flask`, `Django`), both standalone and mounted via `app.mount("/prefix", sub_app)`, and share native database pools, Cloud SQL/AlloyDB dialers, and SHM `LeaseBroker` across frameworks via `DatabaseRegistry.default()`.
- **Recent Discoveries**:
  - `uvloop v0.23.0` adds `eager_start` support in `create_task()` and `3.15`/`3.15t` wheels.
  - `Litestar`, `FastAPI`, and `Starlette` `wrap_asgi` previously allocated 2 closures + 1 list before checking route match and constructed 2 ASGI dicts per response; `Flask` and `Django` `FastPath` previously bailed out on `async def` views.
  - Native database adapters declared across stacked `Django`, `Litestar` (`SQLSpec` / `Alchemy`), and `FastAPI` apps can share the exact same underlying connection pool and SHM `LeaseBroker` by keying `DatabaseRegistry.default()` on a normalized endpoint fingerprint.
