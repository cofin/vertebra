---
type: Research
flow_id: stack-alignment
title: Full-stack review of the merged tree at 5a833832
description: Verified defects, wrong seams, typing gaps, missing upstream features, and knowledge drift across the data integrations, framework extensions, Rust runtime, TypeScript client, packaging, and CI, with the settled target design for each area.
status: stable
state: promoted
promoted_to: stack-alignment
created_at: '2026-11-16T17:45:00Z'
updated_at: '2026-11-16T17:45:00Z'
generated:
  by: agent:review
  at: '2026-11-16T17:45:00Z'
verified:
  - by: agent:review
    at: '2026-11-16T17:45:00Z'
    commit: 5a833832
sources:
  - id: alchemy-cache
    resource: src/py/vertebra/extensions/alchemy/cache.py
    title: Advanced Alchemy cache backends, dogpile adapters, and cache manager subclass
  - id: sqlalchemy-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects/_base.py
    title: Shared SQLAlchemy dialect mixins
  - id: sqlspec-driver
    resource: src/py/vertebra/extensions/sqlspec/_driver.py
    title: Shared SQLSpec adapter configs and base drivers
  - id: broker-transport
    resource: src/py/vertebra/extensions/broker/transport.py
    title: Broker transports and connection leases
  - id: core-channel
    resource: src/rs/vertebra-core/src/channel.rs
    title: Shared-memory ring channels
  - id: http-server
    resource: src/rs/vertebra-http/src/server.rs
    title: HTTP listener, connection serving, and shutdown
  - id: client-router
    resource: src/ts/vertebra-client/src/router.ts
    title: Client route helpers
  - id: ci-workflow
    resource: .github/workflows/ci.yml
    title: Pull request CI lanes
---

# Full-stack review of the merged tree at `5a833832`

<!-- truth: start -->
- `mypy` passes on every package, yet `pyright` reports several hundred errors and the tree carries thousands of `Any` annotations; `disallow_subclassing_any = false` overrides and the absence of `pyright` and `tsc --noEmit` from `make lint-all` hide them [^ci-workflow].
- The Advanced Alchemy cache was built against `dogpile.cache` backends. Advanced Alchemy resolves `CacheConfig.region_factory` before it checks for `dogpile.cache`, so a region implementing `SyncCacheRegionProtocol` needs no `dogpile.cache` install. The shipped region fails that protocol (`invalidate(key)` versus `invalidate()`), and the dogpile adapters fail on the first cache miss [^alchemy-cache].
- Oracle and SQL Server dialects fail in `initialize()` on first connect, MySQL `is_disconnect()` raises `NotImplementedError`, and `create_engine("...?broker=true")` fails; dialect tests only run real engine round-trips on SQLite [^sqlalchemy-dialects].
- SQLSpec adapter configs hand every session one cached driver instance, and the proxy drivers return `int` where the SQLSpec driver contract returns `SQLResult` [^sqlspec-driver].
- Shared-memory readers copy peer-controlled lengths without a bound check, and HTTP/1.1 and HTTP/2 connections are not drained on shutdown [^core-channel] [^http-server].
- Generated `routes.ts` does not match the shape `routeUrl` accepts, and the release workflow can publish to PyPI from a manual dispatch [^client-router].
<!-- truth: end -->

Upstream baselines: Advanced Alchemy `1.12.0` plus `main`, SQLSpec `0.71.1` plus unreleased `0.72.0` on `main`, SQLAlchemy `2.1.8`, Alembic `1.20.1`, Litestar `2.25.0`, litestar-queues `0.14.0`, litestar-vite `0.33.0`, Django `6.1.3`.

## 1. Settled design decisions

These apply across every area and are not open questions.

- **Upstream protocols first.** Every integration implements or subclasses the host library's own protocol or base class. Shadow reimplementations and virtual `ABC.register()` registrations are replaced.
- **No `dogpile.cache` dependency.** Cache integration targets `advanced_alchemy.cache` region protocols through `CacheConfig(region_factory=...)`. The `dogpile` extra, test dependency, mypy override, and adapters are removed.
- **No cross-process compiled statement cache.** SQLAlchemy `Compiled` objects hold live dialect and DBAPI references and cannot be serialized. SQLAlchemy's own `query_cache_size` covers compilation caching.
- **Optional dependencies fail loud.** Missing optional packages raise `MissingDependencyError` through `ensure_module`; no fallback to `DefaultDialect`, in-memory substitutes, or JSON in place of Arrow.
- **Adapter isolation.** Each SQLSpec adapter owns its helpers in its own `core.py` with no cross-adapter imports, matching the upstream adapter layout so `core.py` can be compiled.
- **Names state what a class is.** A class named for shared memory uses shared memory. `SHMQueueBackend` gains a real shared-memory ring and cross-worker wakeup.
- **Framework extensions do not import each other.** Shared pieces move to framework-neutral modules under `vertebra.server`, `vertebra.framework`, or a new `vertebra.cache` package.
- **Production code matches the real contract.** Branches that exist only to satisfy test doubles are removed and the doubles are corrected.
- **Type gate.** `pyright` and `tsc --noEmit` join `make lint` and `make lint-all`; `disallow_subclassing_any = false` overrides are removed as each area is fixed.
- **SQLSpec target.** Adapters implement the `0.72.0` contracts on `main` and the dependency floor moves when that release is published.

## 2. Advanced Alchemy cache, storage, and config factories

Current state:

- `alchemy/cache.py` holds `SyncCacheBackend`, `AsyncCacheBackend`, `SQLAlchemyCacheBackend`, five `Dogpile*Backend` classes, `register_dogpile_backends`, `AsyncCacheManager`, `ZeroCopyServiceMixin`, and `create_cache_config(use_dogpile=...)`.
- `CacheManager(create_cache_config()).invalidate_all_sync()` raises `TypeError` because `SyncCacheBackend.invalidate` requires a key. `SyncCacheBackend.get` takes `default` where the protocol passes `expiration_time`.
- `CacheManager(create_cache_config(use_dogpile=True)).region.get("missing")` raises `AttributeError`: the adapters return `None` instead of dogpile's `NO_VALUE`, and msgpack flattens `CachedValue` into a list.
- `AsyncCacheManager` writes private `CacheManager._region` and `_model_versions`, calls `_make_key`, and only takes its async path for `SQLAlchemyCacheBackend`, which `create_cache_config` never builds, so async calls run through `asyncio.to_thread`.
- `sqlalchemy/cache.py` `CompiledCache` and `ResultCache` have no SQLAlchemy hook for results and cannot store compiled statements out of process.
- Importing `alchemy.cache` requires `advanced_alchemy` and `sqlalchemy`; `django/cache.py` imports from it, so Django-only installs fail.
- `create_fastapi_alchemy`, `create_starlette_alchemy`, and `create_flask_alchemy` fail with configs from `create_*_alchemy_config`, which always build Litestar config classes.
- `create_*_alchemy_config` require `connection_string`, blocking `routing_config` and `engine_instance`; `_prepare_cache_config` calls the global `setup_cache_listeners()`; explicit `pool_pre_ping`, `pool_recycle`, and `poolclass` are overwritten.
- `ObjectStoreBackend.save_object` drops `content_type` and `metadata`; `storage_key` is ignored when a backend instance is passed.
- `ZeroCopyServiceMixin.to_schema` returns a bare list where `ResultConverter.to_schema` returns `OffsetPagination`.
- `alchemy/broker.py` redefines untyped transport protocols, keeps an in-process counter that never calls the transport, and stores each lease twice.
- Not exposed from Advanced Alchemy `main`: `ServiceGroup`, `FilterSet`, `RelationshipFilter`, `get_or_none`, `SchemaDumpConfig`, `upsert_many`, `SQLAlchemyStore.delete_expired`, routing session maker listeners, Spanner and CockroachDB `IntegrityError` classification.

Target:

- `vertebra.cache`: framework-neutral `SyncCacheStore` and `AsyncCacheStore` over `shm://`, `memory://`, `valkey://`, `redis://`, and object stores, with no ORM imports. Django, Flask, and Litestar cache integrations build on it.
- `alchemy/cache.py`: `SyncCacheRegion` (`SyncCacheRegionProtocol`) and `AsyncCacheRegion` (`AsyncCacheRegionProtocol`) returning Advanced Alchemy `NO_VALUE` on miss, msgspec model serializer pair, and `create_cache_config(...) -> CacheConfig`. Upstream `CacheManager` is used unmodified.
- Delete `SQLAlchemyCacheBackend`, all `Dogpile*Backend` classes, `register_dogpile_backends`, `use_dogpile`, `AsyncCacheManager`, `sqlalchemy/cache.py`, and their exports.
- Config factories select the config class for the host framework, accept `connection_string: str | None`, forward `routing_config` and `engine_instance`, and are generic over the config type.
- `alchemy/__init__.py` loads submodules lazily.

## 3. SQLAlchemy dialects, DB-API, pool, types, and Alembic

Current state:

- Oracle and SQL Server dialects lack `_get_server_version_info`; `initialize()` compares `None` with a tuple. Oracle `RETURNING ... INTO` has no out-parameter support. SQL Server `OUTPUT inserted.*` is classified as returning no rows, and `cursor.nextset()` is missing.
- MySQL dialect leaves `_extract_error_code` unimplemented. Disconnect detection matches message substrings only, without SQLSTATE or vendor codes.
- DuckDB, BigQuery, and Spanner dialects fall back to `DefaultDialect` when their base packages are absent, and keep statement helpers used only by tests.
- PostgreSQL dialect lacks `AUTOCOMMIT`, sets `RENDER_CASTS` without cast-rendering types, and shadows `insert_executemany_returning`. Spangres inherits two-phase commit. SQLite pool selection disagrees with SQLAlchemy's file-database check.
- `BrokerPool` hides `pool_size` and `max_overflow`, disables `reset_on_return` and rolls back by hand without greenlet guards, closes drivers on every check-in, and disposes checked-out connections.
- Async adapter: `setinputsizes` awaits a sync method; no `AsyncAdapt_terminate`, `get_driver_connection`, or `async_creator` support; `SyncEngineAdapter` is unwired; `attach_serverless_disconnect_guard` does nothing.
- PEP 249 module drops `connect_args`, leaks native errors from connection construction, keeps only the last batch for `executemany` with `RETURNING`, slices backwards on `fetchmany(-1)`, lacks type objects and constructors, and reports a SQLite version on every engine.
- `Vector` types have no `comparator_factory`; distance helpers return a string builder. `MsgspecJSON` maps SQLite to text JSON and skips encoding `str`. `BinaryJSON` is a compatibility subclass. `SelectInLoadSpec`, `InsertSentinelSpec`, `DeleteUsingClause`, and `ParameterizedSQL` duplicate SQLAlchemy 2.1 features.
- Alembic: comparator registered under a key Alembic never dispatches; custom operations are plain structs outside `Operations.register_operation`; Spanner DDL batching is never invoked; `register_all` writes the private implementation registry.

Target: correct each dialect against its SQLAlchemy base, subclass `AsyncAdapt_dbapi_*` with terminate support, make `BrokerPool` a conforming `Pool`, complete the PEP 249 surface behind typed driver protocols, give vector types real comparators and SQLAlchemy expressions, register Alembic operations and comparators through public APIs, require the base dialect packages through `ensure_module` and extras, and add per-dialect `create_engine` and Alembic `MigrationContext` tests.

## 4. SQLSpec adapters, proxy, and stores

Current state:

- `SyncAdapterConfig.provide_session` and `AsyncAdapterConfig.provide_session` yield one cached driver; `provide_connection` takes no lease; args are discarded; open row streams are not closed on exit.
- `ProxySyncDriver` and `ProxyAsyncDriver` override `execute` and `execute_many` to return row counts, wrap all remote errors in one exception type, and load full result sets for streaming selects. `Proxy*Config.provide_connection` bypasses the broker.
- Configs are parameterized `[Any, Any, DriverT]`; no adapter has `_typing.py`, typed connection/pool/feature dictionaries, or `__slots__`.
- Every adapter `core.py` imports from the shared `_driver.py`. Eight `adk.py` files of 2,100 to 2,500 lines repeat sync and async SQL and copy fallback memory and artifact stores whose config keys have drifted apart. Seven data dictionaries reimplement upstream dialect dictionaries; MySQL subclasses upstream.
- `storage.py` writes the private registry alias map. BigQuery and Spanner drivers import `pyarrow` at module import. `provider.py` imports stdlib `sqlite3`.
- Not implemented from SQLSpec `main`: read replicas and routing sessions, Oracle 26ai session workflows, ADK state deltas and owner-scoped deletes, `adk_artifact_store_class` on six configs, extension schema reconcilers, `reset_session_on_release`, storage `overwrite` and `recursive`, streaming `select_to_storage`.
- Memory ranking for BigQuery and Spanner ADK stores runs in Python.

Target: per-adapter `_typing.py` session and connection contexts that lease on entry and build a driver per session; concrete generics and typed dictionaries; helpers moved into each `core.py`; ADK stores subclass upstream base stores with adapter SQL only; proxy drivers implement dispatch hooks and map errors to SQLSpec exceptions with chunked streaming; `0.72.0` contracts implemented; behavioral store tests on SQLite and DuckDB with statement-shape tests for the server engines.

## 5. Native drivers and broker

Current state:

- `AsyncConnectionLease.release_sync` marks the lease released, then closes the unawaited coroutine, leaking the slot. Leases are marked released before the native release succeeds. `AsyncSHMBrokerTransport` returns a blocking stream and carries sync methods. `BrokerConfig` has paired timeout fields where one silently overrides the other.
- Oracle, DuckDB, SQL Server, and SQLite adapters carry a statement-cache probe that catches `TypeError` to satisfy test doubles whose signatures differ from the native `StatementCache`.
- BigQuery and Spanner `close()` drop the client without closing it. SQLite `execute_tuples` opens a transaction it never commits. Redis `slot_map` returns `{}`. Sentinel rediscovery reconnects without TLS, tracking, or IAM settings.
- `DataFusionEngine` ignores partition and memory settings and never registers tables with the session context.
- `db/arrow.py` returns `self` or JSON when `pyarrow` is absent. `db/cdc.py` increments its id without a lock.
- `_redis.pyi` returns `Any` from async methods; `_oracle.pyi` imports the wrong `ArrowCursor`; BigQuery and Spanner stubs return untyped dictionaries.
- SQLite, Redis, and Valkey lack the `_sync.py` / `_async.py` split; pipeline exit behavior differs per engine; Redis and Valkey duplicate protocol helpers; `src/py/vertebra/drivers/` is empty.

Target: lease state changes only after native release; `AsyncBrokerStream`; one broker lease and statement-cache path in the driver base; real client close; real slot map; rediscovery preserving connection settings; DataFusion configured and registering sources; fail-loud Arrow paths; precise stubs; uniform adapter layout and one pipeline exit contract.

## 6. Framework extensions, queues, agent protocols, and Inertia

Current state:

- `VertebraAppConfig` is not a Django `AppConfig`, so `ready()` never runs. Django `EmailBackend` is not a `BaseEmailBackend` and suppresses sending by default. Flask cache backend is not a `flask_caching` `BaseCache`. Starlette and FastAPI plugins never register lifespan or middleware.
- Starlette, FastAPI, Flask, and Django extensions import Litestar extension modules at import time; Django imports Flask and Advanced Alchemy modules; several Litestar and Django modules import their host unguarded.
- Both `SHMQueueBackend` classes are process-local. `StoreQueueBackend.enqueue_many` renames the upstream parameter and drops fields; `cleanup_terminal` and `has_identity` are placeholders; lookups assume the `default` queue.
- Django MySQL and Oracle backends insert substitute driver modules into `sys.modules`; MySQL reports a fixed server version.
- `MCPCore._handle_completion` returns an empty result; `MCPTaskStore` ignores its queue service and TTL; a stale `tasks/result` handler remains registered (the `2025-*` lifecycle methods stay behind dual-mode negotiation by design). A2A streaming does not release broker subscriptions on disconnect, and webhook validation does not resolve host names. The gRPC registry answers protobuf content types while speaking MessagePack. APEX has its own confirmation gate.
- Route compilers, fast paths, storage registries, channel backends, rate-limit guards, and config classes are repeated across the framework extensions; Redis and Valkey queue modules are near copies.
- Inertia: async middleware never awaits async props; `DeferredProp.once` is ignored; reset handling is non-standard; header lookups are case-sensitive; templates with only the SSR body marker lose the page data script.
- Flask cache falls back to `flushdb()`; `BaseNativeKVStore` first-level cache is unbounded; `Embedder.__new__` double-initializes; the embedding broker sleeps per chunk; worker batch timeout cancels the future and not the task.

Target: each integration subclasses the host protocol; neutral shared modules replace cross-extension imports; shared-memory queue over a native ring; queue backend matches `BaseQueueBackend`; protocol handlers completed; Inertia aligned with the litestar-vite contract; tests exercise host entry points such as `AppConfig.create`.

## 7. Rust runtime, PyO3 bridge, and HTTP ingress

Current state:

- `SpscRing::read_slot_to_vec` copies `header.len` bytes without checking payload capacity; ring `attach` paths skip the region size check; lease-table and statement-cache offsets lack checked arithmetic and alignment checks; `ShmStatementCache::insert` spins forever on an odd sequence left by a crashed writer; overflow envelope decoding recurses without a depth cap and panics on corrupt slabs.
- `transport.rs` passes slices of writable Python buffers across `py.detach`; decoder caches key on raw type pointers; `SteppedCoroutine` holds a reference cycle without traverse and clear slots; `read_ipc_from_shm` maps and copies while attached.
- Shutdown drains HTTP/3, SSE, and WebSocket but not HTTP/1.1 or HTTP/2 tasks; the TLS path lacks the limits set on the plain path; no header list or header read limits; the cleartext protocol probe result is discarded.
- `AccessLogger` accumulates lines that nothing drains; `ProfileEndpoint` returns fixed JSON; pub/sub, WebSocket mount, multipart, and upgrade helpers have no request-path callers.
- Worker exit reclaims rings while the reader task may still read; many `unsafe` blocks lack safety comments.
- `_dispatch.py` creates thread pools per batch without shutdown; one slow stream stalls a worker's response ring.
- `_vertebra.pyi` declares constructors on classes that cannot be constructed and narrows buffer parameters.
- Storage and database crates each carry a completion-queue awaitable bridge.

Target: bounds, size, alignment, and depth checks with error returns; bounded seqlock recovery; immutable or copied buffers off the interpreter lock; weak-reference cache keys; GC slots; graceful connection drain with unified limits; streaming access log; wired or removed scaffolding; per-stream backpressure; accurate stubs; negative tests for corrupt segments.

## 8. TypeScript client, codegen, packaging, CI, tests, and knowledge

Current state:

- `routes.ts` emits `{ method, path }` objects while `routeUrl` takes path strings; `types.d.ts` receives runtime exports.
- React `useQuery` refetches without end at the default `staleTime`; Svelte adapter uses runes in a plain `.ts` file; the Vite virtual module returns declarations as runtime code; HTMX adapter forces JSON content type; WebMCP bridge cannot read event-stream responses and ignores `requiresConfirmation`.
- `tsconfig.json` lacks `noUncheckedIndexedAccess`; Biome allows explicit `any`; `package.json` lacks `sideEffects` and the Svelte peer dependency.
- Release workflow omits the Valkey source distribution and has no release-event guard on the PyPI upload. CI lint skips `stubtest` because extensions are not built, runs no `tsc`, runs macOS Rust tests only on manual dispatch, leaves Oracle conformance non-blocking, and checks five of thirteen wheels.
- Driver wheels do not pin the core distribution; several optional imports have no extra; the `adk` extra is unused; onefile builds can require `zstandard` on the target; import map version is stale.
- `WASMSandboxPolicy` fields are not enforced. Telemetry recorders and serializer caches mutate shared state without locks under free-threaded builds.
- Tests: one stage-named file, eight files over 2,000 lines mixing subsystems, case loops in place of `parametrize`, wall-clock sleeps for synchronization, doubles whose signatures differ from the native classes, no adapter tests for the client frameworks.
- Knowledge chapters describe removed types, wrong defaults, wrong wheel counts, and features that are not wired; several limitation entries are stale or describe shortcuts.

Target: one route shape shared by codegen and client; working adapters with tests; strict TypeScript settings; guarded release; CI running `pyright`, `tsc`, `stubtest` with built extensions, all wheel checks, and macOS on Rust changes; correct extras and pins; enforced sandbox policy; locked shared state; split and parametrized tests; knowledge chapters and limitation entries rewritten to match the code after each area lands.

## 9. Deferred backlog recorded in archived specs

Never resumed and to be scheduled: AWS RDS and Aurora DSQL IAM authentication, Azure Entra ID authentication, Redis queue store, CockroachDB and AlloyDB extensions, WebTransport over HTTP/3, workload identity federation, inbound JWKS verification, cross-worker MCP session and task stores, Arrow C stream export, atomic rename and `fsync` in object storage, presigned URL verification for GCS, Azure, and OCI.

[^alchemy-cache]: `src/py/vertebra/extensions/alchemy/cache.py` — Advanced Alchemy cache backends, dogpile adapters, and cache manager subclass.
[^sqlalchemy-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects/_base.py` — Shared SQLAlchemy dialect mixins.
[^sqlspec-driver]: `src/py/vertebra/extensions/sqlspec/_driver.py` — Shared SQLSpec adapter configs and base drivers.
[^broker-transport]: `src/py/vertebra/extensions/broker/transport.py` — Broker transports and connection leases.
[^core-channel]: `src/rs/vertebra-core/src/channel.rs` — Shared-memory ring channels.
[^http-server]: `src/rs/vertebra-http/src/server.rs` — HTTP listener, connection serving, and shutdown.
[^client-router]: `src/ts/vertebra-client/src/router.ts` — Client route helpers.
[^ci-workflow]: `.github/workflows/ci.yml` — Pull request CI lanes.
