---
type: Guide
title: Vertebra Product and Engineering Guidelines
description: Engineering invariants, domain naming rules, deep-module seam design, zero-copy hot-path standards, and code quality conventions across Python, Rust, and TypeScript.
status: stable
tags: [guidelines, conventions, performance, naming, architecture]
sources:
  - resource: src/py/vertebra/__init__.py
  - resource: src/py/vertebra/server/_dispatch.py
  - resource: src/py/vertebra/server/_asgi/_runner.py
  - resource: src/py/vertebra/framework/_router.py
  - resource: src/py/vertebra/utils/serializers/_json.py
  - resource: src/py/vertebra/typing.py
  - resource: src/py/vertebra/_typing.py
  - resource: src/rs/vertebra-core/src/channel.rs
  - resource: src/rs/vertebra-http/src/lib.rs
  - resource: src/ts/vertebra-client/src/index.ts
---

# Vertebra Product and Engineering Guidelines

<!-- truth: start -->
- **Clean-Break Domain Naming (`PY-07`)**: Name symbols across Python, Rust, and TypeScript after their domain role (`App`, `Controller`, `Router`, `Route`, `Procedure`, `Request`, `Response`, `LeaseBroker`, `SyncObjectStore` / `AsyncObjectStore`, `HttpClient`) without `Vertebra*` or `Native*` prefixes (`VertebraError`, host-framework `VertebraPlugin`/`VertebraConfig`/`VertebraAppConfig`, and CLI `VertebraEnv`/`VertebraGroup`/`VertebraExtensionGroup` are the sole exceptions) and with zero backward-compatibility `X = Y` aliases.
- **Deep Modules & Seam Classification**: Expose compact public interfaces that hide internal IPC, FFI, and wire-framing complexity. Classify dependencies into four categories: `In-process` (test directly), `Local-substitutable` (use in-memory/SHM/SQLite/Floci fakes), `Remote-owned` (isolate behind Ports & Adapters), and `External boundary` (mock only at the outer edge).
- **Zero-Copy Hot Paths & Value Objects (`PY-04`, `PY-05`)**: Use `msgspec.Struct(..., frozen=True, gc=False)` (`array_like=True` only on internal ring frames), route JSON through `vertebra.utils.serializers`, release the GIL via `py.detach(|| ...)`, and hydrate DTOs via C-API `msgspec_bridge.rs`.
- **Python Layout & Zero Inline Comments (`PY-01`, `PY-02`, `PY-06`, `PY-08`)**: Zero inline `#` comments in Python files (use Google-style docstrings); top-level grouped imports; public `vertebra.typing` facade; cached `vertebra.utils.module_loader` (`import_optional`, `ensure_module`); PEP 810 `__lazy_modules__` + PEP 562 `__getattr__`.
- **Public-Seam Vertical-Slice TDD & Cold-Reader Docs**: Deliver vertical slices combining production code and observable public-seam tests in canonical test files; keep `docs/` conceptual (`4–6` node Mermaid diagrams, `pymdownx.tabbed` install blocks, zero contributor `make` commands in user docs).
- **Polyglot Router Parity & Git Discipline (`HTTP-10`)**: Keep FNV-1a route hashing identical between Rust (`routing.rs`) and Python (`_router.py`), and deliver all changes on dedicated branches via signed conventional commits (`commit.gpgsign=true`).
<!-- truth: end -->

## 1. Domain Naming Without Aliases

Public and internal symbols across Python, Rust, and TypeScript name *what* the domain concept is. They carry no `Vertebra*` brand prefix and no implementation-mechanism suffix:

- **Canonical Domain Names**: `App`, `Controller`, `Router`, `Route`, `Procedure`, `ProcedureRouter`, `Provide`, `Inject`, `Container`, `DependencyGraph`, `HandlerInfo`, `Request`, `Response`, `Headers`, `HeadersIterator`, `RequestBatch`, `RequestBatchIterator`, `AsgiScope`, `AsgiScopeIterator`, `WsgiEnviron`, `WsgiEnvironIterator`, `EventLoopPool`, `BatchDispatchPlan`, `HandlerSpec`, `AsgiRunner`, `AsgiReceive`, `AsgiSend`, `RsgiAdapter`, `WsgiAdapter`, `Bundler`, `SourceWatcher`, `SSRPool`, `SyncEmbeddedSSRClient`, `AsyncEmbeddedSSRClient`, `SyncSidecarSSRClient`, `AsyncSidecarSSRClient`, `SyncInertiaMiddleware`, `AsyncInertiaMiddleware`, `BuildOrchestrator`, `DevServerProcess`, `TypeExtractor`, `ManifestGenerator`, `RequestBatcher`, `BatchLimit`, `InFlightRequests`, `PendingRequests`, `RouteTable`, `ResponseBody`, `HttpResponse`, `JSONSerializer`, `MsgPackSerializer`, `OptionalDependency`, `Console`, `SyncObjectStore`, `AsyncObjectStore`, `SQLSpecPlugin`, `SyncAdapterConfig`, `AsyncAdapterConfig`, `PostgreSQLSyncConfig`, `PostgreSQLAsyncConfig`, `MySQLSyncConfig`, `MySQLAsyncConfig`, `SQLiteSyncConfig`, `SQLiteAsyncConfig`, `OracleSyncConfig`, `OracleAsyncConfig`, `MSSQLSyncConfig`, `MSSQLAsyncConfig`, `DuckDBSyncConfig`, `DuckDBAsyncConfig`, `BigQuerySyncConfig`, `BigQueryAsyncConfig`, `SpannerSyncConfig`, `SpannerAsyncConfig`, `AlchemyPlugin`, `BrokerPlugin`, `LeaseBroker`, `LeaseGrant`, `BrokerStats`, `StatementCache`, `StatementCacheStats`, `StatementPlan`, `ArrowCursor`, `SyncCloudSQLDialer`, `AsyncCloudSQLDialer`, `SyncAlloyDBDialer`, `AsyncAlloyDBDialer`, `SyncPostgreSQLDialect`, `AsyncPostgreSQLDialect`, `SyncMySQLDialect`, `AsyncMySQLDialect`, `SyncSQLiteDialect`, `AsyncSQLiteDialect`, `SyncOracleDialect`, `AsyncOracleDialect`, `SyncMSSQLDialect`, `AsyncMSSQLDialect`, `SyncDuckDBDialect`, `AsyncDuckDBDialect`, `SyncBigQueryDialect`, `AsyncBigQueryDialect`, `SyncSpannerDialect`, `AsyncSpannerDialect`, `SyncSpangresDialect`, `AsyncSpangresDialect`, `VertebraPlugin`, `VertebraConfig`, `VertebraAppConfig`, `RouteCompiler`, `OffloadPlan`, `FastPath`, `ChannelsBackend`, `MemoryStore`, `WorkerRunner`, `LaneConfig`, `LiveState`, `LogSink`, `LogHandler`, `JwtGuard`, `RateLimiter`, `TraceContext`, `CgroupQuota`, `LameDuckController`, `ZPagesRegistry`, and on the TypeScript side `HttpClient`, `ClientConfig`, `ClientError`, `RealtimeChannel`, `SseClient`, `WebSocketClient`, `SessionStore`, `HotReloadClient`, `LiveStateClient`, and `TraceContext`.
- **No Backward-Compatibility Shims**: Do not reintroduce removed aliases (`VertebraApp`, `CallbackScheduler`, `LocalExchange`, `BatchCollector`, `CorrelationSlab`, `DrainPolicy`, `NativeRouter`, `VertebraBody`, `VertebraResponse`, `VertebraClient`, `VertebraStore`, `VertebraHmrClient`, `VertebraSQLSpecPlugin`, `VertebraAlchemyPlugin`, `LitestarPlugin`, `LitestarConfig`, `StarlettePlugin`, `StarletteConfig`, `FastAPIPlugin`, `FastAPIConfig`, `FlaskPlugin`, `FlaskConfig`, `DjangoPlugin`, `DjangoConfig`, `DjangoAppConfig`, `VertebraChannelsBackend`, `VertebraMemoryStore`, `MultiLaneWorkerRunner`, `VertebraSseClient`, `VertebraWebSocketClient`, `VertebraArrowCursor`, `VertebraDbapiCursor`, `VertebraDbapiConnection`, `VertebraSqlSpecSyncConfig`, `VertebraSqlSpecAsyncConfig`, `VertebraPostgresSyncConfig`, `VertebraPostgresAsyncConfig`, `VertebraMysqlSyncConfig`, `VertebraMysqlAsyncConfig`, `VertebraSqliteSyncConfig`, `VertebraSqliteAsyncConfig`, `VertebraOracleSyncConfig`, `VertebraOracleAsyncConfig`, `VertebraMssqlSyncConfig`, `VertebraMssqlAsyncConfig`, `VertebraDuckdbSyncConfig`, `VertebraDuckdbAsyncConfig`, `PostgresVtbDialect`, `MysqlVtbDialect`, `SqliteVtbDialect`, `OracleVtbDialect`, `MssqlVtbDialect`, `DuckdbVtbDialect`, `NativeLeaseBroker`, `NativeStatementCache`, `NativeObjectStore`, `NativeSsrClient`, `NativeEmbeddedSsrClient`). Classes split across execution modes use explicit leading `Sync*` and `Async*` prefixes with clean, un-suffixed method names (`put`, `get`, `execute`, `render`, `connect`), with zero mixed `*_async` methods on dual-mode classes (except upstream `sqlspec` `<Engine>SyncConfig`/`<Engine>AsyncConfig`/`<Engine>SyncDriver`/`<Engine>AsyncDriver`/`*SyncDataDictionary`/`*AsyncDataDictionary`/`SQLSpecSyncProvider`/`SQLSpecAsyncProvider` and upstream AA/SQLSpec `ObjectStoreBackend` protocol adapters). `src/py/tests/unit/extensions/test_drop_in_names_and_generics.py` and `tools/scripts/review_setup.py` (`PY-07`) enforce that these prefixes, suffixes, and aliases are absent.
- **Exception Root & Host-Framework Plugin Names**: [`VertebraError`](../../../src/py/vertebra/_typing.py) is the package's base exception class, `VertebraPlugin` / `VertebraConfig` / `VertebraAppConfig` name the Vertebra integration inside host-framework extension packages (`vertebra.extensions.{litestar,starlette,fastapi,flask,django}`), and `VertebraEnv` / `VertebraGroup` / `VertebraExtensionGroup` name the `vtb` CLI extension environment and Click group classes.

## 2. Zero-Copy and Allocation-Minimal Hot Paths

Per-request and per-batch code paths minimize heap allocations, copies, and cross-thread wakeups:

- **Rust Shared Memory & Bridge**: Write serialized responses directly into ring slots through closure-based `SpscRing::try_send_with` / `MpmcRing::try_send_with`, and read through zero-copy `SlotView` (`SpscRing::try_recv_view`) or `MpmcRing::try_recv_with` ([`channel.rs`](../../../src/rs/vertebra-core/src/channel.rs)). Recycle `TransportArena` and `DataSlab` allocations through `BufferPool`.
- **PyO3 FFI & C-API `msgspec` Bridge**: Release the Python GIL with `py.detach(|| ...)` on every blocking poll (`get_batch`), batch response submission (`submit_results`, `submit_response`), RSGI file I/O, Oxc bundle build (`Bundler::build`), QuickJS SSR render (`SSRPool::render`), and native object-store calls in `_storage`. Decode request bodies and hydrate database/ORM rows directly over borrowed `Py_buffer` slices via `Request.decode_msgspec`, `WsgiInput.decode_msgspec`, and `hydrate_rows_vectorcall` (`src/rs/vertebra-py/src/msgspec_bridge.rs`).
- **Python Dispatch**: Partition each batch once into [`BatchDispatchPlan`](../../../src/py/vertebra/server/_dispatch.py) (`sync_native`, `sync_thread`, `async_native`, `mounts`, `unmatched`), pre-compile handler signatures at startup with `compile_handler_spec` / `compile_handler_table` (`HandlerSpec`), execute default synchronous routes inline in a single `submit_batch_raw` vector flush, wake `EventLoopPool` (`create_event_loop`) once per loop shard through `spawn_batch`/`schedule_batch`, and drive ASGI, RSGI, and WSGI batches directly in Rust via `drive_asgi_batch` / `drive_rsgi_batch` (`PyIter_Send` with `_SteppedCoroutine` task promotion on suspension) and `drive_wsgi_batch` (zero-copy `WsgiInput::from_snapshot` + binary `_WsgiStartResponse`).
- **Lazy Package Startup**: Keep [`vertebra/__init__.py`](../../../src/py/vertebra/__init__.py) and [`vertebra/framework/__init__.py`](../../../src/py/vertebra/framework/__init__.py) free of eager submodule imports, combining PEP 810 `__lazy_modules__` with PEP 562 `__getattr__` so `import vertebra` does not import the framework, server, or native extensions.

## 3. Python Module Organization

- **Python `>=3.12` Native Typing**: Use PEP 695 generic syntax (`def schedule[T]`, `class SQLSpecPlugin[ConfigT]`), PEP 604 unions (`X | Y`), and PEP 585 built-in collection types (`list`, `dict`, `tuple`, `set`). Do not add `typing_extensions` shims.
- **Value Objects & Base Structs**: Config, metadata, spec, and frame value objects are `msgspec.Struct(..., frozen=True, gc=False)` (or extend [`BaseStruct`](../../../src/py/vertebra/typing.py), [`CamelizedBaseStruct`](../../../src/py/vertebra/typing.py), [`FrozenBaseStruct`](../../../src/py/vertebra/typing.py), or [`FrozenCamelizedStruct`](../../../src/py/vertebra/typing.py)). Enable `cache_hash=True` on hashable lookup/key structs, `forbid_unknown_fields=True` on inbound request schemas, and `array_like=True` strictly on internal positional wire/ring-buffer frames ([`ChannelMessage`](../../../src/py/vertebra/extensions/broker/transport.py), [`StateDeltaFrame`](../../../src/py/vertebra/framework/live.py), [`TaskEnvelope`](../../../src/py/vertebra/framework/queues/base.py)) — never on HTTP/SSE/JSON response models.
- **JSON Serialization**: Route JSON through [`vertebra.utils.serializers`](../../../src/py/vertebra/utils/serializers/_json.py) (`encode_json`, `encode_json_bytes`, `encode_json_into`, `decode_json`, `format_json`, `JSONSerializer`, `serialize_zero_copy`) rather than calling `msgspec.json` or `json` directly in other modules.
- **No Inline `#` Comments**: Do not place inline `#` comments in Python implementation or test files. Document intent, invariants, and edge cases in Google-style docstrings.
- **Module & Class Layout**:
  1. Module docstring at the top.
  2. Top-level imports grouped by standard library, third-party (`msgspec`, `anyio`, `rich`), and first-party (`vertebra.*`), with `if TYPE_CHECKING:` blocks at module top level for type-only or optional imports.
  3. Explicit `__all__` tuple.
  4. Public classes and functions before private `_...` module-level helpers.
  5. Within classes: `__slots__` / class attributes, then `__init__`, then `@property`, then public methods, then private `_...` helper methods.

## 4. Deep Modules, Seam Classification & Documentation Standards

- **Deep Modules & Public Seams**: Design modules with narrow, domain-level public interfaces (`App`, `Controller`, `Router`, `SyncObjectStore` / `AsyncObjectStore`, `LeaseBroker`, `ChannelBus`, `HttpClient`) that encapsulate wire protocols, ring buffer layouts, and C-API slot offsets.
- **Dependency Seam Classification**:
  - `In-process`: Pure computation, `msgspec` codecs, route compilation, and wire parsers — exercise directly in unit tests without doubles.
  - `Local-substitutable`: Shared-memory rings, SQLite, in-memory `SyncObjectStore` / `AsyncObjectStore` (`MemoryConfig`), and Floci emulators — test against real local backends.
  - `Remote-owned`: External cloud control planes and database servers — isolate behind explicit adapter/driver protocols (`SyncAdapterConfig`, `AsyncAdapterConfig`, `QueueBackend`).
  - `External boundary`: Third-party webhook/HTTP endpoints — mock only at the outer transport edge and assert observable request/response bytes, headers, and state transitions.
- **Documentation & Prose Standards**:
  - User-facing documentation under `docs/` explains concepts for a cold reader without leaking internal contributor commands (`make dev`, `make build-pgo`, `make test`).
  - Keep `docs/` Mermaid diagrams conceptual (`4–6` nodes) and place detailed implementation diagrams in `.agents/bundles/knowledge/architecture/`.
  - Format all package installation instructions in `docs/` using linked `pymdownx.tabbed` blocks (`=== "uv"`, `=== "pip"`, `=== "pdm"`, `=== "poetry"`).

## 5. Polyglot Boundary & Git Discipline

- **Single FNV-1a Route Identity**: Route matching happens in Rust ([`Router`](../../../src/rs/vertebra-http/src/routing.rs) / `matchit 0.9`), which passes a `u64` handler id to Python workers. Any change to path normalization or FNV-1a hashing must keep [`src/rs/vertebra-http/src/routing.rs`](../../../src/rs/vertebra-http/src/routing.rs) and [`src/py/vertebra/framework/_router.py`](../../../src/py/vertebra/framework/_router.py) synchronized.
- **Dependency Hygiene**: Keep benchmark-only packages (`duckdb`, `uvicorn`, `httptools`, `uvloop`, `granian`, `spikard`) in `[dependency-groups].bench` and documentation-only packages (`zensical`, `mkdocstrings-python`, `pymdown-extensions`) in `[dependency-groups].doc` in [`pyproject.toml`](../../../pyproject.toml), never in `dev`.
- **Branch & PR Discipline**: Never commit or push directly to `main`. Develop on a dedicated branch with signed commits, verify with `make lint-all` and focused test targets, and deliver through a pull request.

## Related Context

- [Product Definition](product.md)
- [Tech Stack](tech-stack.md)
- [Python Module & Class Layout](../knowledge/patterns/python/module-and-class-layout.md)
- [PyO3 FFI & GIL Discipline](../knowledge/patterns/rust/ffi-and-gil-discipline.md)
