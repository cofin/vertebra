---
type: Guide
title: Vertebra Product Definition
description: Product purpose, runtime topologies, and the capabilities that exist in the Python, Rust, and TypeScript packages of the Vertebra application backbone.
status: stable
tags: [product, runtime, python, rust, typescript]
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-review
    at: "2026-11-06T06:10:00Z"
    commit: 7657e8b7c2c96bdda6821aec70a2b603f438a127
sources:
  - id: src-vertebra-bigquery-lib-rs
    resource: src/rs/vertebra-bigquery/src/lib.rs
    title: "Rust PyO3 extension module (vertebra._bigquery)"
  - id: src-vertebra-spanner-lib-rs
    resource: src/rs/vertebra-spanner/src/lib.rs
    title: "Rust PyO3 extension module (vertebra._spanner)"
  - id: src-tenant-rls-py
    resource: src/py/vertebra/db/rls.py
    title: "Engine-neutral tenant configuration and ANSI predicates"
  - id: src-tenant-sqlspec-py
    resource: src/py/vertebra/extensions/sqlspec/adapters.py
    title: "Engine-owned session enforcement and unsupported-adapter refusal"
  - id: src-bigquery-tools-py
    resource: src/py/vertebra/db/adapters/bigquery/tools.py
    title: "Adapter-owned BigQuery tool provider"
  - id: src-spanner-tools-py
    resource: src/py/vertebra/db/adapters/spanner/tools.py
    title: "Adapter-owned Spanner tool provider"
  - id: src-agent-tools-py
    resource: src/py/vertebra/agent/tools/__init__.py
    title: "Generic tool-provider bridge"
  - id: src-readme-md
    resource: README.md
    title: "Repository overview and quickstart documentation"
  - id: src-pyproject-toml
    resource: pyproject.toml
    title: "Python package metadata, dependencies, and tool configuration"
  - id: src-init-py
    resource: src/py/vertebra/__init__.py
    title: "Python implementation module (src/py/vertebra/__init__.py)"
  - id: src-framework-init-py
    resource: src/py/vertebra/framework/__init__.py
    title: "Python implementation module (src/py/vertebra/framework/__init__.py)"
  - id: src-storage-init-py
    resource: src/py/vertebra/storage/__init__.py
    title: "Python implementation module (src/py/vertebra/storage/__init__.py)"
  - id: src-db-init-py
    resource: src/py/vertebra/db/__init__.py
    title: "Python implementation module (src/py/vertebra/db/__init__.py)"
  - id: src-telemetry-init-py
    resource: src/py/vertebra/telemetry/__init__.py
    title: "Python implementation module (src/py/vertebra/telemetry/__init__.py)"
  - id: src-server-platform-py
    resource: src/py/vertebra/server/platform.py
    title: "Python implementation module (src/py/vertebra/server/platform.py)"
  - id: src-wasm-init-py
    resource: src/py/vertebra/wasm/__init__.py
    title: "Python implementation module (src/py/vertebra/wasm/__init__.py)"
  - id: src-cli-init-py
    resource: src/py/vertebra/cli/__init__.py
    title: "Python implementation module (src/py/vertebra/cli/__init__.py)"
  - id: src-cargo-toml
    resource: src/rs/Cargo.toml
    title: "Rust crate manifest (src/rs/Cargo.toml)"
  - id: src-package-json
    resource: src/ts/vertebra-client/package.json
    title: "TypeScript package manifest (src/ts/vertebra-client/package.json)"
  - id: src-src-index-ts
    resource: src/ts/vertebra-client/src/index.ts
    title: "TypeScript client module (src/ts/vertebra-client/src/index.ts)"
---

# Vertebra Product Definition

<!-- truth: start -->
## Project Purpose

Vertebra is one unified runtime for Python and TypeScript — backend, frontend, web, gRPC, workers, and standalone apps. A native HTTP ingress and IPC runtime hosts Python handlers (native, ASGI, RSGI, WSGI) either in one process or across shared-memory worker processes. Around that runtime sit a high-performance object-storage extension, Python packages for data access, telemetry, platform integration, live state, and framework extensions, a `vtb` CLI, and the `@vertebra/client` TypeScript SDK.

[`vertebra/__init__.py`](../../../src/py/vertebra/__init__.py)[^src-init-py] and [`vertebra/framework/__init__.py`](../../../src/py/vertebra/framework/__init__.py)[^src-framework-init-py] declare PEP 810 `__lazy_modules__` alongside PEP 562 `__getattr__`, so `import vertebra` loads the framework, server, and native extension modules only on first attribute access across Python `3.12`–`3.15`. CI runs on Linux and macOS; `vertebra-core` also carries a Windows platform module ([`platform/windows.rs`](../../../src/rs/vertebra-core/src/platform/windows.rs)).

## Core Product Capabilities

- **Python Framework (`vertebra.framework`)**: [`App`](../../../src/py/vertebra/framework/_app.py), [`Controller`](../../../src/py/vertebra/framework/_router.py), [`Router`](../../../src/py/vertebra/framework/_router.py), [`Route`](../../../src/py/vertebra/framework/_router.py), and [`Procedure`](../../../src/py/vertebra/framework/procedures.py) with typed path/query/header/cookie parameters, hierarchical dependency injection (`Provide`, `Inject`, `Container`, `DependencyGraph`, `RequestScope` in [`di.py`](../../../src/py/vertebra/framework/di.py)), opt-in `sync_to_thread=True` route offloading, `App.mount(prefix, sub_app, protocol=...)`, and `on_startup`/`on_shutdown` hooks; Inertia.js middleware ([`SyncInertiaMiddleware`, `AsyncInertiaMiddleware`](../../../src/py/vertebra/framework/inertia/__init__.py)); SSR; configurable OpenAPI/AsyncAPI docs portals (`ScalarDocsConfig`, `SwaggerDocsConfig`, `AsyncAPIDocsConfig`); and subdomain packages `a2a`, `asyncapi`, `autowire`, `channels`, `di`, `email`, `htmx`, `inertia`, `live`, `mcp`, `openapi`, `procedures`, `queues`, `security`, `ssr`, and `stores` ([`framework/__init__.py`](../../../src/py/vertebra/framework/__init__.py)).
- **Build, Codegen & Packaging (`vertebra.build`)**: [`BuildOrchestrator`](../../../src/py/vertebra/build/_orchestrator.py) and [`TypeExtractor`](../../../src/py/vertebra/build/codegen/_extractor.py) drive Python-to-client code generation for TypeScript, Zod, Valibot, OpenAPI, bidirectional Protobuf v3 (`ProtobufParser`, `routes_to_proto`, `proto_to_python_routes`), and typed procedures (`build/codegen/`). `build/packaging/` assembles standalone directories and self-extracting single-file executables (`vtb build --standalone|--onefile`).
- **Multi-Protocol Execution**: native handlers (`RequestBatch`, `Request` with anchored zero-copy buffer-protocol `body_view`, `Request.decode_msgspec`, `Request.decode_pydantic_json`, `Response`, `Headers`); ASGI 3.0 through [`AsgiRunner`](../../../src/py/vertebra/server/_asgi/_runner.py) and Rust `drive_asgi_batch` / `AsgiProtocol` / [`AsgiScope`](../../../src/rs/vertebra-py/src/asgi.rs); RSGI 1.6 through [`RsgiAdapter`](../../../src/py/vertebra/server/_rsgi/_adapter.py) and Rust `drive_rsgi_batch` / `RsgiHttpProtocol` / [`rsgi/scope.rs`](../../../src/rs/vertebra-py/src/rsgi/scope.rs); PEP 3333 WSGI through [`WsgiAdapter`](../../../src/py/vertebra/server/_wsgi/_adapter.py) and Rust `drive_wsgi_batch` / `WsgiInput::from_snapshot` (`WsgiInput.decode_msgspec`) / [`WsgiEnviron`](../../../src/rs/vertebra-py/src/wsgi/environ.rs). `vtb run --interface auto|asgi|rsgi|wsgi` selects the gateway.
- **Runtime Topologies** (`vtb run --topology auto|embedded|shm [--jit/--no-jit]`):
  - **Embedded** (default for single-worker or free-threaded Python; multi-worker `SO_REUSEPORT` on Unix when explicitly `--topology embedded` with `workers > 1`): the `vertebra-server` binary embeds Python through `append_to_inittab!(pymodule_init)` in [`main.rs`](../../../src/rs/vertebra-http/src/main.rs), hands zero-copy `InProcessRequest` batches to [`worker.py`](../../../src/py/vertebra/server/worker.py) (`run_worker`) through [`BatchBridge`](../../../src/rs/vertebra-core/src/bridge.rs) (`ResponseSink` direct completion), and supervises `W` symmetric `SO_REUSEPORT` worker processes sharing pre-initialized SHM broker FDs when `workers > 1`.
  - **Shared Memory** (`--topology shm`, auto-selected on POSIX GIL Python when `--workers > 1`): Python worker subprocesses spawned by [`RingFactory`](../../../src/rs/vertebra-http/src/workers/factory.rs) read requests zero-copy from [`MpmcRing`](../../../src/rs/vertebra-core/src/channel.rs) and write responses to the small/medium/large/xlarge `SpscRing` tiers of [`TieredRingSet`](../../../src/rs/vertebra-core/src/transport/ring_pair.rs) using Dekker `SeqCst` `consumer_sleeping` wakeups (`ShmHeader` `ABI_VERSION = 3`). Container-aware [`RingPairConfig::for_environment`](../../../src/rs/vertebra-core/src/transport/ring_pair.rs) sizes primary rings (`46 MiB/worker` default, `12 MiB/worker` `compact` profile) via anonymous `memfd_create` on Linux and `shm_open` elsewhere, with automatic `/tmp/vtb-*` file-backed fallback for `VQOV`/`VROV` overflow payloads when `/dev/shm` is constrained.
  - **Free-threaded Python (`3.14t`, `3.15t`)**: `_vertebra`, `_bundler`, `_storage`, `_bigquery`, and `_spanner` declare `#[pymodule(gil_used = false)]`, disable global-mutex PyO3 freelists under `Py_GIL_DISABLED`, freeze non-subclassed `#[pyclass]` types (`immutable_type`), and spawn multi-thread embedded Python worker pools; CI and `cibuildwheel` cover `3.12`–`3.15` and `3.14t`/`3.15t`.
- **Native HTTP (`vertebra-http`)**: Hyper HTTP/1.1 (`max_buf_size(65536)`, `pipeline_flush(false)`) and HTTP/2, opt-in h2c detection, `rustls` TLS with ALPN and mTLS (`tls` feature), optional HTTP/3 over QUIC (`http3` feature), lock-free atomic [`BackpressureController`](../../../src/rs/vertebra-http/src/backpressure.rs), cached `HealthEndpoint`, offloaded CORS, response compression, static asset mounts, OpenAPI cache, and HMR WebSockets, plus gRPC framing (`application/grpc`, `+json`, `+msgpack`), an SSE hub, WebSocket mounts, a shared-memory pub/sub broker, access logs, multipart parsing, `.well-known` discovery (`/.well-known/mcp.json`, `/.well-known/agent-card.json`), and an MCP stdio bridge.
- **Bundling & SSR (`vertebra-bundler`)**: Oxc TS/JSX transforms ([`Bundler`](../../../src/rs/vertebra-bundler/src/bundler.rs)), `rolldown-notify` `SourceWatcher` for HMR, and QuickJS SSR context pooling ([`SSRPool`](../../../src/rs/vertebra-bundler/src/ssr.rs)).
- **Object Storage (`vertebra-storage`, `vertebra.storage`)**: the `_storage` extension (`SyncObjectStore`, `AsyncObjectStore`, `StorageBuffer`, `RustStorageAwaitable`) wraps `object_store` (S3, GCS with zero-config ADC, Azure, HTTP) and adds OCI request signing ([`oci.rs`](../../../src/rs/vertebra-storage/src/oci.rs)) and S3/GCS/Azure SAS presigning ([`signing.rs`](../../../src/rs/vertebra-storage/src/signing.rs)). Python exposes [`SyncObjectStore`, `AsyncObjectStore`](../../../src/py/vertebra/storage/_store.py) with `LocalConfig`, `S3Config`, `GCSConfig`, `AzureConfig`, `OCIConfig`, `R2Config`, `HTTPConfig`, `MemoryConfig`, and `SigningConfig`, plus the Arrow `SyncReadableFile` / `AsyncReadableFile` bridge.
- **Data Access (`vertebra.db`, `vertebra-db`)**: two-tier shared-memory database and cache plane across PostgreSQL, MySQL, SQLite, Oracle (Thin TNS/TTC + Autonomous Database Wallet + 26ai `VECTOR`/`OSON`), SQL Server / MSSQL (TDS + `@P1` parameter rewriting), DuckDB (single-writer SHM broker host with Arrow IPC), Google BigQuery (REST v2 + Storage Read/Write v1 `VTBARROW` + `ObjectTableConfig`), Google Cloud Spanner (GoogleSQL + `Spangres`, cached `multiplexed = true` sessions, wound-wait `Aborted` retry, `BatchWrite`, Data Boost `PartitionQuery` `VTBARROW`), Redis, and Valkey (RESP3 + `CLIENT TRACKING` + Sentinel + Cluster CRC16 slot routing); direct TCP/TLS (`:5432`, `:3306`) IAM OAuth2 token password resolution plus opt-in `SyncCloudSQLDialer` / `AsyncCloudSQLDialer` (`:3307`) and `SyncAlloyDBDialer` / `AsyncAlloyDBDialer` (`:5433` `MetadataExchangeRequest`); zero-copy `ArrowCursor` over `ShmRegion` Arrow IPC; `vertebra.server._channels.ChannelBus` (re-exported from `vertebra.framework.channels`); `SyncPipelineCapable` / `AsyncPipelineCapable` (`driver.pipeline()` native batching and ordered results), `ChangeFeed` & `SyncSpannerChangeStreamSource` / `AsyncSpannerChangeStreamSource` (CDC), `TenantPredicate` (ANSI tenant filtering with adapter-owned session builders), `DataFusionEngine`, adapter-owned `BigQueryToolProvider` / `SpannerToolProvider` descriptors consumed by generic `vertebra.agent.build_tools`, and `DatabaseRegistry`.
- **Telemetry (`vertebra.telemetry`)**: `LogSink`, `LogHandler`, `LoggingConfig`, `TraceContext`, `SpanExporter`, `GenAiRecorder`, and `SqlTracePropagator`. `vertebra-core` provides `AsyncLogCore` ([`logger.rs`](../../../src/rs/vertebra-core/src/logger.rs)), `W3cTraceContext`, and `sanitize_sql_statement` ([`telemetry.rs`](../../../src/rs/vertebra-core/src/telemetry.rs)).
- **Platform Runtime (`vertebra.server`)**: [`platform.py`](../../../src/py/vertebra/server/platform.py)[^src-server-platform-py] (`CgroupQuota` cgroup v2 sizing, `LameDuckController` drain, `ZPagesRegistry`, `PlatformAdapter`), `GilAuditReport` (free-threaded GIL re-enablement audit), `ShmAllocationPolicy` (`memfd_create` and `SCM_RIGHTS` fallback for constrained `/dev/shm`), and `EventLoopPool`.
- **Live State (`vertebra.framework.live`)**: `LiveState`, `LiveStateRoom`, `StateDeltaFrame`, and `HybridLogicalClock` sync MessagePack deltas with last-write-wins ordering ([`live.py`](../../../src/py/vertebra/framework/live.py)).
- **WebAssembly Plugins (`vertebra.wasm`)**: `WASMPluginHost`, `WASMModuleConfig`, and `WASMSandboxPolicy` validate Wasm binary headers, register modules, and enforce a fuel budget before calling host-registered exports. No Wasm engine is bundled.
- **Framework Extensions (`vertebra.extensions`)**: `litestar` (`VertebraPlugin`, `VertebraConfig`, `RouteCompiler`, `OffloadPlan`, `offload_middlewares`, `FastPath`, `ChannelsBackend`, `MemoryStore`, and optional ecosystem integrations `queues`, `security`, `autowire`, `email`, `mcp`, `vite`, `asyncapi`, `htmx`, `saq`), `starlette` (`VertebraPlugin`, `VertebraConfig`, fastpath route compiler), `fastapi` (`VertebraPlugin`, `VertebraConfig`, topological `DependencyPlan` compiler), `flask` (`VertebraPlugin`, `VertebraConfig`, zero-copy WSGI batch bridge), `django` (`VertebraPlugin`, `VertebraConfig`, `VertebraAppConfig`, `@vertebra_view`, `MsgspecView`, `ArrowQuerySet`, and 8 native `vertebra.extensions.django.db.backends.{postgresql,mysql,sqlite3,oracle,mssql,duckdb,bigquery,spanner}` engines), `sqlspec` (`create_sqlspec_plugin`, `ProxySyncConfig`, `ProxyAsyncConfig`, `SQLSpecSyncProvider`, `SQLSpecAsyncProvider`, `SyncAdapterConfig`, `AsyncAdapterConfig`, `{PostgreSQL,MySQL,SQLite,Oracle,MSSQL,DuckDB,BigQuery,Spanner}{Sync,Async}Config`, `ObjectStoreBackend`), `alchemy` (`create_alchemy_plugin`, `normalize_alchemy_url`, `AsyncCacheManager`, `SQLAlchemyCacheBackend`, `ZeroCopyServiceMixin`, `ObjectStoreBackend`), `sqlalchemy` (`+vertebra`/`+vtb` dialects `Sync{PostgreSQL,MySQL,SQLite,Oracle,MSSQL,DuckDB,BigQuery,Spanner,Spangres}Dialect` / `Async{PostgreSQL,MySQL,SQLite,Oracle,MSSQL,DuckDB,BigQuery,Spanner,Spangres}Dialect`, `AlembicImpl`, `BigQueryAlembicImpl`, `SpannerAlembicImpl`, `Vector`, `BinaryJSON`), `grpc` (`GrpcServiceRegistry`, `ProtobufSchemaGenerator`), `apex` (Oracle APEX sessions, auth handoff, reactive islands, offline HLC sync, federated queries, ORDS/26ai Duality Views, and `ApexGateway`), and `worker` (`WorkerRunner`, `LaneConfig`).
- **CLI (`vtb`)**: `run`, `dev`, `init`, `js`, `build`, `ai models`, `ai mcp`, and `workers run`, plus pluggable commands registered through `VertebraExtensionGroup`, `CLIPluginProtocol` (`on_cli_init`), `VertebraEnv`, and package entry points ([`cli/__init__.py`](../../../src/py/vertebra/cli/__init__.py)[^src-cli-init-py], [`cli/plugin_registry.py`](../../../src/py/vertebra/cli/plugin_registry.py)).
- **TypeScript Client (`@vertebra/client`)**: [`HttpClient`](../../../src/ts/vertebra-client/src/client/client.ts) with retry/backoff, [`WebSocketClient`](../../../src/ts/vertebra-client/src/websocket.ts), `RealtimeChannel`, `SseClient`, [`SessionStore`](../../../src/ts/vertebra-client/src/store/index.ts), [`HotReloadClient`](../../../src/ts/vertebra-client/src/hmr/index.ts), `LiveStateClient`, `ArrowStreamClient`, `WASMClient`, `ApexRegion`, `TraceContext`, `routeUrl`/`wsUrl` (`/router`), `registerWebMCPTools` (`/webmcp`), and `McpClient`/`A2aClient` (`/agent`), with subpath adapters for React, Vue, Svelte, Angular, HTMX, and Vite.
<!-- truth: end -->

Tenant configuration is engine-neutral (`setting_name`, optional parameter cast, caller-selected placeholder). PostgreSQL/Oracle cores own session SQL; unsupported SQLSpec adapters refuse session enforcement, and BigQuery/Spanner reject configured predicates before initialization. Oracle context support currently has helper/SQLite-stand-in acceptance; trusted-package provisioning, session reset, and live Oracle acceptance belong to native Oracle driver work.

## Architectural Overview

| Layer | Package / Crate | Primary Responsibility |
| --- | --- | --- |
| Python Framework & Server | [`src/py/vertebra/`](../../../src/py/vertebra/__init__.py) | `App`, `Controller`, `Router`, `Route`, `Procedure`, `Provide`/`Inject`, framework subdomains, `BuildOrchestrator`, `AsgiRunner`, `RsgiAdapter`, `WsgiAdapter`, `EventLoopPool`, `BatchDispatchPlan`, `vtb` CLI, and the `run_supervisor` launcher |
| Python Data, Storage & Telemetry | [`src/py/vertebra/storage/`](../../../src/py/vertebra/storage/__init__.py)[^src-storage-init-py], [`db/`](../../../src/py/vertebra/db/__init__.py)[^src-db-init-py], [`telemetry/`](../../../src/py/vertebra/telemetry/__init__.py)[^src-telemetry-init-py] | `SyncObjectStore` / `AsyncObjectStore`, database configs/dialers/drivers, `ArrowCursor`, `SyncPipelineCapable` / `AsyncPipelineCapable`, `ChangeFeed`, `LogSink`, `TraceContext` |
| Python Extensions | [`src/py/vertebra/extensions/`](../../../src/py/vertebra/extensions/__init__.py) | Litestar, Starlette, FastAPI, Flask, Django (plus 8 native DB backends), SQLSpec (`SQLSpecSyncProvider`, `SQLSpecAsyncProvider`, `Proxy*Config`, `{PostgreSQL,MySQL,SQLite,Oracle,MSSQL,DuckDB,BigQuery,Spanner}{Sync,Async}Config`), Advanced Alchemy, SQLAlchemy `+vtb` dialects, gRPC, Oracle APEX, and worker lanes |
| Rust Core IPC & Wire Protocol | [`src/rs/vertebra-core/`](../../../src/rs/vertebra-core/src/lib.rs) | `ShmRegion`, `SpscRing` (`SlotView`), `MpmcRing`, `BatchBridge`, `RingPair`, `TieredRingSet`, `ShmLeaseTable` + co-located `ShmStatementCache` + `ShmChannelSlot`, `CompressionCodec` (LZ4/Zstd), OS wakeups (`eventfd`, `ulock`, pipes), plus `AsyncLogCore`, `W3cTraceContext`, `validate_jwt`/`RateLimiter`, and `SharedMutex`/`SharedRwLock`/`SharedCondvar` |
| Rust HTTP Ingress & Dispatch | [`src/rs/vertebra-http/`](../../../src/rs/vertebra-http/src/lib.rs) | `run_with_listener` accept loop (`ServerState`), `Router` (`matchit 0.9`), `RouteTable`, `RequestBatcher` shards (`BatchLimit`), `Dispatcher`, `ShmDispatcher` with sharded `InFlightRequests`, `BackpressureController`, `RingFactory`, and the `vertebra-server` binary |
| Rust Python FFI Extension | [`src/rs/vertebra-py/`](../../../src/rs/vertebra-py/src/lib.rs) | PyO3 `vertebra._vertebra` classes (`Request`, `Response`, `Headers`, `RequestBatch`, `AsgiScope`, `AsgiProtocol`, `PyEmptyAwaitable`, `PyValueAwaitable`, `RsgiScope`, `RsgiHttpProtocol`, `WsgiEnviron`, `WsgiInput`, `_WsgiStartResponse`, `LeaseBroker`, `StatementCache`, `CloudSQLDialer`, `AlloyDBDialer`, `SyncGcpTokenCache`, `AsyncGcpTokenCache`, `ArrowCursor`, `LogSink`, `JWTGuard`, `RateLimiter`), C-API `msgspec_bridge` (`decode_msgspec`, `decode_pydantic_json`, `hydrate_rows_vectorcall`, `orm_to_struct_vectorcall`), and batch API (`init_transport`, `get_batch`, `try_get_batch`, `prepare_request_sleep`, `cancel_request_sleep`, `drive_asgi_batch`, `drive_native_batch`, `drive_rsgi_batch`, `drive_wsgi_batch`, `submit_response_raw`, `submit_batch_raw`) |
| Rust Native Bundler & SSR | [`src/rs/vertebra-bundler/`](../../../src/rs/vertebra-bundler/src/lib.rs) | PyO3 `vertebra._bundler`: Oxc transforms, `SourceWatcher`, `SSRPool` |
| Rust Object Storage | [`src/rs/vertebra-storage/`](../../../src/rs/vertebra-storage/src/lib.rs) | PyO3 `vertebra._storage`: `SyncObjectStore`, `AsyncObjectStore`, `StorageBuffer`, `RustStorageAwaitable`, `ObjectStoreEngine`, OCI signing, S3/GCS/Azure presigning |
| Rust BigQuery Extension | [`src/rs/vertebra-bigquery/`](../../../src/rs/vertebra-bigquery/src/lib.rs)[^src-vertebra-bigquery-lib-rs] | PyO3 `vertebra._bigquery`: `SyncBigQueryClient`, `AsyncBigQueryClient`, typed DB-API exceptions, REST v2 + Storage Read/Write v1 `VTBARROW` SHM transport |
| Rust Spanner Extension | [`src/rs/vertebra-spanner/`](../../../src/rs/vertebra-spanner/src/lib.rs)[^src-vertebra-spanner-lib-rs] | PyO3 `vertebra._spanner`: `SyncSpannerClient`, `AsyncSpannerClient`, typed DB-API exceptions, multiplexed gRPC sessions, wound-wait retry, Data Boost `VTBARROW` SHM transport |
| Rust Database & Wire Engine | [`src/rs/vertebra-db/`](../../../src/rs/vertebra-db/src/lib.rs) | `CloudSQLInstanceURI`, `AlloyDBClusterURI`, `PoolerMode`/`compute_worker_pool_budget`, `parse_pgcopy_binary`, `encode_com_stmt_execute`, `SQLitePragmaPlan`, `parse_tnsnames_ora`, `OracleConnectDescriptor`, `extract_wallet_files`, `encode_oracle_vector`, `rewrite_tds_positional_params`, `DuckDBPragmaPlan`, `parse_resp3_frame`, `crc16_redis_slot`, `ShmRowBatch`, `ArrowShmDescriptor` |
| TypeScript Client SDK | [`src/ts/vertebra-client/`](../../../src/ts/vertebra-client/src/index.ts)[^src-src-index-ts] | `@vertebra/client` multi-entry package and framework adapters |

## Related Context

- [Product Guidelines](product-guidelines.md)
- [Tech Stack](tech-stack.md)
- [Knowledge Index](../knowledge/index.md)

Provenance sources: [^src-readme-md], [^src-pyproject-toml], [^src-wasm-init-py], [^src-cargo-toml], [^src-package-json].

[^src-readme-md]: `README.md` — Repository overview and quickstart documentation.
[^src-pyproject-toml]: `pyproject.toml` — Python package metadata, dependencies, and tool configuration.
[^src-init-py]: `src/py/vertebra/__init__.py` — Python implementation module (src/py/vertebra/__init__.py).
[^src-framework-init-py]: `src/py/vertebra/framework/__init__.py` — Python implementation module (src/py/vertebra/framework/__init__.py).
[^src-storage-init-py]: `src/py/vertebra/storage/__init__.py` — Python implementation module (src/py/vertebra/storage/__init__.py).
[^src-db-init-py]: `src/py/vertebra/db/__init__.py` — Python implementation module (src/py/vertebra/db/__init__.py).
[^src-telemetry-init-py]: `src/py/vertebra/telemetry/__init__.py` — Python implementation module (src/py/vertebra/telemetry/__init__.py).
[^src-server-platform-py]: `src/py/vertebra/server/platform.py` — Python implementation module (src/py/vertebra/server/platform.py).
[^src-wasm-init-py]: `src/py/vertebra/wasm/__init__.py` — Python implementation module (src/py/vertebra/wasm/__init__.py).
[^src-cli-init-py]: `src/py/vertebra/cli/__init__.py` — Python implementation module (src/py/vertebra/cli/__init__.py).
[^src-cargo-toml]: `src/rs/Cargo.toml` — Rust crate manifest (src/rs/Cargo.toml).
[^src-package-json]: `src/ts/vertebra-client/package.json` — TypeScript package manifest (src/ts/vertebra-client/package.json).
[^src-src-index-ts]: `src/ts/vertebra-client/src/index.ts` — TypeScript client module (src/ts/vertebra-client/src/index.ts).

[^src-tenant-rls-py]: `src/py/vertebra/db/rls.py` — Engine-neutral tenant configuration and ANSI predicates.

[^src-tenant-sqlspec-py]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Engine-owned session enforcement and unsupported-adapter refusal.

[^src-bigquery-tools-py]: `src/py/vertebra/db/adapters/bigquery/tools.py` — Adapter-owned BigQuery tool provider.

[^src-spanner-tools-py]: `src/py/vertebra/db/adapters/spanner/tools.py` — Adapter-owned Spanner tool provider.

[^src-agent-tools-py]: `src/py/vertebra/agent/tools/__init__.py` — Generic tool-provider bridge.
[^src-vertebra-bigquery-lib-rs]: `src/rs/vertebra-bigquery/src/lib.rs` — Rust PyO3 extension module (vertebra._bigquery).
[^src-vertebra-spanner-lib-rs]: `src/rs/vertebra-spanner/src/lib.rs` — Rust PyO3 extension module (vertebra._spanner).
