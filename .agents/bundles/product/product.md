---
type: Guide
title: Vertebra Product Definition
description: Product purpose, runtime topologies, and the capabilities that exist in the Python, Rust, and TypeScript packages of the Vertebra application backbone.
status: stable
tags: [product, runtime, python, rust, typescript]
sources:
  - resource: README.md
  - resource: pyproject.toml
  - resource: src/py/vertebra/__init__.py
  - resource: src/py/vertebra/framework/__init__.py
  - resource: src/py/vertebra/storage/__init__.py
  - resource: src/py/vertebra/db/__init__.py
  - resource: src/py/vertebra/telemetry/__init__.py
  - resource: src/py/vertebra/server/platform.py
  - resource: src/py/vertebra/wasm/__init__.py
  - resource: src/py/vertebra/cli/__init__.py
  - resource: src/rs/Cargo.toml
  - resource: src/ts/vertebra-client/package.json
  - resource: src/ts/vertebra-client/src/index.ts
---

# Vertebra Product Definition

<!-- truth: start -->
## Project Purpose

Vertebra is a Python-first application backbone. A Rust HTTP ingress and IPC runtime hosts Python handlers (native, ASGI, RSGI, WSGI) either in one process or across shared-memory worker processes. Around that runtime sit a Rust object-storage extension, Python packages for data access, telemetry, platform integration, live state, and framework extensions, a `vtb` CLI, and the `@vertebra/client` TypeScript SDK.

[`vertebra/__init__.py`](../../../src/py/vertebra/__init__.py) uses a PEP 562 `__getattr__`, so `import vertebra` loads the framework, server, and native extension modules only on first attribute access. CI runs on Linux and macOS; `vertebra-core` also carries a Windows platform module ([`platform/windows.rs`](../../../src/rs/vertebra-core/src/platform/windows.rs)).

## Core Product Capabilities

- **Python Framework (`vertebra.framework`)**: [`App`](../../../src/py/vertebra/framework/_app.py) and [`Router`](../../../src/py/vertebra/framework/_router.py) with typed path parameters, `App.mount(prefix, sub_app, protocol=...)`, and `on_startup`/`on_shutdown` hooks; Inertia.js middleware ([`InertiaMiddleware`](../../../src/py/vertebra/framework/inertia/__init__.py)); SSR; and subdomain packages `a2a`, `asyncapi`, `autowire`, `channels`, `di`, `email`, `htmx`, `inertia`, `live`, `mcp`, `openapi`, `procedures`, `queues`, `security`, `ssr`, and `stores` ([`framework/__init__.py`](../../../src/py/vertebra/framework/__init__.py)).
- **Build, Codegen & Packaging (`vertebra.build`)**: [`BuildOrchestrator`](../../../src/py/vertebra/build/_orchestrator.py) and [`TypeExtractor`](../../../src/py/vertebra/build/codegen/_extractor.py) drive Python-to-client code generation for TypeScript, Zod, Valibot, OpenAPI, Protobuf, and typed procedures (`build/codegen/`). `build/packaging/` assembles standalone directories and self-extracting single-file executables (`vtb build --standalone|--onefile [--pgo]`).
- **Multi-Protocol Execution**: native handlers (`RequestBatch`, `Request`, `Response`, `Headers`); ASGI 3.0 through [`AsgiRunner`](../../../src/py/vertebra/server/_asgi/_runner.py) (`_drive_coro`) and [`AsgiScope`](../../../src/rs/vertebra-py/src/asgi.rs); RSGI 1.6 through [`RsgiAdapter`](../../../src/py/vertebra/server/_rsgi/_adapter.py) and [`rsgi/scope.rs`](../../../src/rs/vertebra-py/src/rsgi/scope.rs); PEP 3333 WSGI through [`WsgiAdapter`](../../../src/py/vertebra/server/_wsgi/_adapter.py) and [`WsgiEnviron`](../../../src/rs/vertebra-py/src/wsgi/environ.rs). `vtb run --interface auto|asgi|rsgi|wsgi` selects the gateway.
- **Runtime Topologies** (`vtb run --topology embedded|shm`):
  - **Embedded** (default): the `vertebra-server` binary embeds Python through `append_to_inittab!(pymodule_init)` in [`main.rs`](../../../src/rs/vertebra-http/src/main.rs) and hands batches to Python worker threads through [`BatchBridge`](../../../src/rs/vertebra-core/src/bridge.rs).
  - **Shared Memory**: Python worker subprocesses spawned by [`RingFactory`](../../../src/rs/vertebra-http/src/workers/factory.rs) read requests from [`MpmcRing`](../../../src/rs/vertebra-core/src/channel.rs) and write responses to the small/medium/large/xlarge `SpscRing` tiers of [`TieredRingSet`](../../../src/rs/vertebra-core/src/transport/ring_pair.rs). Regions use `memfd_create` on Linux and `shm_open` elsewhere, with `eventfd`, `ulock`, or pipe wakeups.
  - **Free-threaded Python**: `_vertebra`, `_bundler`, and `_storage` declare `#[pymodule(gil_used = false)]`; CI tests Python `3.14t`.
- **Native HTTP (`vertebra-http`)**: Hyper HTTP/1.1 (`pipeline_flush`) and HTTP/2, h2c detection, `rustls` TLS with ALPN and mTLS (`tls` feature), optional HTTP/3 over QUIC (`http3` feature), [`BackpressureController`](../../../src/rs/vertebra-http/src/backpressure.rs), a health endpoint, and HMR WebSockets, plus gRPC framing, an SSE hub, WebSocket mounts, a shared-memory pub/sub broker, CORS, compression, static assets, access logs, multipart parsing, `.well-known` discovery, and an MCP stdio bridge.
- **Bundling & SSR (`vertebra-bundler`)**: Oxc TS/JSX transforms ([`Bundler`](../../../src/rs/vertebra-bundler/src/bundler.rs)), `rolldown-notify` `SourceWatcher` for HMR, and QuickJS SSR context pooling ([`SsrPool`](../../../src/rs/vertebra-bundler/src/ssr.rs)).
- **Object Storage (`vertebra-storage`, `vertebra.storage`)**: the `_storage` extension (`ObjectStore`, `StorageBuffer`, `RustTaskAwaitable`) wraps `object_store` (S3, GCS, Azure, HTTP) and adds OCI request signing ([`oci.rs`](../../../src/rs/vertebra-storage/src/oci.rs)) and S3/GCS/Azure SAS presigning ([`signing.rs`](../../../src/rs/vertebra-storage/src/signing.rs)). Python exposes [`ObjectStore`, `SyncStore`, `AsyncStore`](../../../src/py/vertebra/storage/_store.py) with `LocalConfig`, `S3Config`, `GcsConfig`, `AzureConfig`, `OciConfig`, `R2Config`, `HttpConfig`, `MemoryConfig`, and `SigningConfig`, plus `StorageRegistry`, `UploadTracker`, and the Arrow `ReadableFile` bridge.
- **Data Access (`vertebra.db`, `vertebra-db`)**: two-tier shared-memory database and cache plane across PostgreSQL, MySQL, SQLite, Oracle (Thin TNS/TTC + Autonomous Database Wallet + 26ai `VECTOR`/`OSON`), SQL Server / MSSQL (TDS + `@P1` parameter rewriting), DuckDB (single-writer SHM broker host with Arrow IPC), Redis, and Valkey (RESP3 + `CLIENT TRACKING` + Sentinel + Cluster CRC16 slot routing); `CloudSqlDialer` (`:3307`) and `AlloyDbDialer` (`:5433` `MetadataExchangeRequest`); zero-copy `ArrowCursor` over `ShmRegion` Arrow IPC; `QueryPipeline` (pipelined/CTE-batched PostgreSQL queries), `ChangeFeed` (CDC), `TenantPredicate` (row-level security), `DataFusionEngine` (federation plan descriptors), and `DatabaseRegistry`.
- **Telemetry (`vertebra.telemetry`)**: `LogSink`, `LogHandler`, `LoggingConfig`, `TraceContext`, `SpanExporter`, `GenAiRecorder`, and `SqlTracePropagator`. `vertebra-core` provides `AsyncLogCore` ([`logger.rs`](../../../src/rs/vertebra-core/src/logger.rs)), `W3cTraceContext`, and `sanitize_sql_statement` ([`telemetry.rs`](../../../src/rs/vertebra-core/src/telemetry.rs)).
- **Platform Runtime (`vertebra.server`)**: [`platform.py`](../../../src/py/vertebra/server/platform.py) (`CgroupQuota` cgroup v2 sizing, `LameDuckController` drain, `ZPagesRegistry`, `PlatformAdapter`), `GilAuditReport` (free-threaded GIL re-enablement audit), `ShmAllocationPolicy` (`memfd_create` and `SCM_RIGHTS` fallback for constrained `/dev/shm`), and `EventLoopPool`.
- **Live State (`vertebra.framework.live`)**: `LiveState`, `LiveStateRoom`, `StateDeltaFrame`, and `HybridLogicalClock` sync MessagePack deltas with last-write-wins ordering ([`live.py`](../../../src/py/vertebra/framework/live.py)).
- **WebAssembly Plugins (`vertebra.wasm`)**: `WasmPluginHost`, `WasmModuleConfig`, and `WasmSandboxPolicy` validate Wasm binary headers, register modules, and enforce a fuel budget before calling host-registered exports. No Wasm engine is bundled.
- **Framework Extensions (`vertebra.extensions`)**: `litestar` (`LitestarPlugin`, `LitestarRouteCompiler`, `MiddlewareOffloadCompiler`, `ChannelsBackend`, `MemoryStore`), `sqlspec` (`SQLSpecPlugin`, `ProxySyncConfig`, `ProxyAsyncConfig`, `SqlSpecProvider`, `ProxySyncDriver`, `ProxyAsyncDriver`, `SyncAdapterConfig`, `AsyncAdapterConfig`, `{Postgres,Mysql,Sqlite,Oracle,Mssql,Duckdb}{Sync,Async}Config`), `alchemy` (`AlchemyPlugin`, `BrokerPlugin`), `sqlalchemy` (`+vertebra`/`+vtb` dialects `{Postgres,Mysql,Sqlite,Oracle,Mssql,Duckdb}Dialect`, `AlembicImpl`, `Vector`, `BinaryJson`), `grpc` (`GrpcServiceRegistry`, `ProtobufSchemaGenerator`), `apex` (Oracle APEX sessions, auth handoff, reactive islands, offline HLC sync, federated queries, ORDS/26ai Duality Views, and `ApexGateway`), and `worker` (`WorkerRunner`, `LaneConfig`).
- **CLI (`vtb`)**: `run`, `dev`, `init`, `js`, `build`, `mcp bridge`, and `workers run`, plus commands registered through entry points ([`cli/__init__.py`](../../../src/py/vertebra/cli/__init__.py)).
- **TypeScript Client (`@vertebra/client`)**: [`HttpClient`](../../../src/ts/vertebra-client/src/client/client.ts) with retry/backoff, [`WebSocketClient`](../../../src/ts/vertebra-client/src/websocket.ts), `RealtimeChannel`, `SseClient`, [`SessionStore`](../../../src/ts/vertebra-client/src/store/index.ts), [`HotReloadClient`](../../../src/ts/vertebra-client/src/hmr/index.ts), `LiveStateClient`, `ArrowStreamClient`, `WasmClient`, `ApexRegion`, and `TraceContext`, with subpath adapters for React, Vue, Svelte, Angular, HTMX, and Vite.
<!-- truth: end -->

## Architectural Overview

| Layer | Package / Crate | Primary Responsibility |
| --- | --- | --- |
| Python Framework & Server | [`src/py/vertebra/`](../../../src/py/vertebra/__init__.py) | `App`, `Router`, framework subdomains, `BuildOrchestrator`, `AsgiRunner`, `RsgiAdapter`, `WsgiAdapter`, `AsyncRuntime`, `BatchDispatchPlan`, `vtb` CLI, and the `run_supervisor` launcher |
| Python Data, Storage & Telemetry | [`src/py/vertebra/storage/`](../../../src/py/vertebra/storage/__init__.py), [`db/`](../../../src/py/vertebra/db/__init__.py), [`telemetry/`](../../../src/py/vertebra/telemetry/__init__.py) | `ObjectStore`, database configs/dialers/drivers, `ArrowCursor`, `QueryPipeline`, `ChangeFeed`, `LogSink`, `TraceContext` |
| Python Extensions | [`src/py/vertebra/extensions/`](../../../src/py/vertebra/extensions/__init__.py) | Litestar, SQLSpec (`SqlSpecProvider`, `Proxy*Config`, `{Postgres,Mysql,Sqlite,Oracle,Mssql,Duckdb}*Config`), Advanced Alchemy, SQLAlchemy `+vtb` dialects, gRPC, Oracle APEX, and worker lanes |
| Rust Core IPC & Wire Protocol | [`src/rs/vertebra-core/`](../../../src/rs/vertebra-core/src/lib.rs) | `ShmRegion`, `SpscRing` (`SlotView`), `MpmcRing`, `BatchBridge`, `RingPair`, `TieredRingSet`, `ShmLeaseTable` + co-located `ShmStatementCache` + `ShmChannelSlot`, `CompressionCodec` (LZ4/Zstd), OS wakeups (`eventfd`, `ulock`, pipes), plus `AsyncLogCore`, `W3cTraceContext`, `validate_jwt`/`RateLimiter`, and `SharedMutex`/`SharedRwLock`/`SharedCondvar` |
| Rust HTTP Ingress & Dispatch | [`src/rs/vertebra-http/`](../../../src/rs/vertebra-http/src/lib.rs) | `run_with_listener` accept loop (`ServerState`), `Router` (`matchit 0.9`), `RouteTable`, `RequestBatcher` shards (`BatchPolicy`), `Dispatcher`, `ShmDispatcher` with sharded `InFlightRequests`, `BackpressureController`, `RingFactory`, and the `vertebra-server` binary |
| Rust Python FFI Extension | [`src/rs/vertebra-py/`](../../../src/rs/vertebra-py/src/lib.rs) | PyO3 `vertebra._vertebra` classes (`Request`, `Response`, `Headers`, `RequestBatch`, `AsgiScope`, `RsgiScope`, `WsgiEnviron`, `LeaseBroker`, `StatementCache`, `CloudSqlDialer`, `AlloyDbDialer`, `ArrowCursor`, `LogSink`, `JwtGuard`, `RateLimiter`) and batch API (`init_transport`, `get_batch`, `try_get_batch`, `submit_response`, `submit_results`) |
| Rust Native Bundler & SSR | [`src/rs/vertebra-bundler/`](../../../src/rs/vertebra-bundler/src/lib.rs) | PyO3 `vertebra._bundler`: Oxc transforms, `SourceWatcher`, `SsrPool` |
| Rust Object Storage | [`src/rs/vertebra-storage/`](../../../src/rs/vertebra-storage/src/lib.rs) | PyO3 `vertebra._storage`: `ObjectStore`, `StorageBuffer`, `RustTaskAwaitable`, `ObjectStoreEngine`, OCI signing, S3/GCS/Azure presigning |
| Rust Database & Wire Engine | [`src/rs/vertebra-db/`](../../../src/rs/vertebra-db/src/lib.rs) | `CloudSqlInstanceUri`, `AlloyDbClusterUri`, `PoolerMode`/`compute_worker_pool_budget`, `parse_pgcopy_binary`, `encode_com_stmt_execute`, `SqlitePragmaPlan`, `parse_tnsnames_ora`, `OracleConnectDescriptor`, `extract_wallet_files`, `encode_oracle_vector`, `rewrite_tds_positional_params`, `DuckdbPragmaPlan`, `parse_resp3_frame`, `crc16_redis_slot`, `ShmRowBatch`, `ArrowShmDescriptor` |
| TypeScript Client SDK | [`src/ts/vertebra-client/`](../../../src/ts/vertebra-client/src/index.ts) | `@vertebra/client` multi-entry package and framework adapters |

## Related Context

- [Product Guidelines](product-guidelines.md)
- [Tech Stack](tech-stack.md)
- [Knowledge Index](../knowledge/index.md)
