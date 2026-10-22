---
title: "Domain Vocabulary Index"
description: "Index of Vertebra's nested domain vocabulary and bounded-context chapters across Python, Rust, and TypeScript."
status: stable
tags:
  - domains
  - index
  - vocabulary
  - architecture
---

# Domain Vocabulary Index

Vertebra uses one domain naming model across Python, Rust, and TypeScript: each class, struct, and function names the domain concept directly, without brand prefixes (`Vertebra*`, except for the root `VertebraError` exception hierarchy in `vertebra._typing`), implementation-mechanism suffixes, or compatibility aliases.

## Framework (`framework/`)

- [Python Framework, Routing & Serialization Domain](framework/routing-and-app.md) — `App`, `Router`, `RouteDef`, `ParamSpec`, `HandlerInfo`, `Request`, `Response`, `InertiaMiddleware`, `InertiaConfig`, `InertiaResponse`, `JSONSerializer`, `module_loader`, and `OptionalDependency`.
- [Framework Subdomains](framework/subdomains.md) — `di` (`Container`, `Provide`), `security` (`SecurityConfig`, `Guard`), `autowire` (`AutowireLoader`), `procedures` (`ProcedureRouter`), `htmx`, `ssr`, `openapi`, `asyncapi`, `mcp` (`McpServer`, `McpStdioBridge`), `a2a` (`A2AServer`, `WellKnownDiscoveryCache`), `queues`, `channels`, `stores`, and `email`.
- [Live State Synchronization](framework/live-state.md) — `LiveState`, `LiveStateRoom`, `StateDeltaFrame`, `StateOpcode`, `HybridLogicalClock`, `@live_action`, `LwwStateRegister`, and TypeScript `LiveStateClient` / `OfflineDeltaQueue`.
- [`vtb` Command-Line Interface](framework/cli.md) — `vtb run`, `vtb dev`, `vtb build`, `vtb js`, `vtb init`, `vtb mcp bridge`, `vtb workers run`, `discover_entry_point_commands`, `Console`, and `run_dev_tui`.

## Server & Worker Runtime (`server/`)

- [Python Server, Worker Loop & Protocol Adapters Domain](server/worker-and-protocols.md) — `AsyncRuntime`, `AsgiEventLoopPool`, `GilAuditReport`, `BatchDispatchPlan`, `run_worker`, `AsgiRunner`, `AsgiScope`, `AsgiReceive`, `AsgiSend`, `RsgiAdapter`, `WsgiAdapter`, `WsgiEnviron`, `CgroupQuota`, `LameDuckController`, `ZPagesRegistry`, `ShmAllocationPolicy`, and `run_supervisor`.
- [Multi-Lane Worker Runner](server/worker-lanes.md) — `LaneConfig`, `WorkerRunner` (`submit`, `stop`, `parse_lanes`, `resolve_job`, `run_oneshot`), and `vtb workers run`.

## IPC & HTTP (`ipc/`, `http/`)

- [Rust IPC, Shared Memory & Ring Transport Domain](ipc/transport-and-rings.md) — `ShmRegion`, `ShmHeader`, `SpscRing`, `SlotView`, `MpmcRing`, `RingPair`, `TieredRingSet`, `BatchBridge`, `TransportArena`, `DataSlab`, `TransportNode`, `BatchSnapshot`, and `CompressionCodec`.
- [Rust HTTP Ingress, Batching & Dispatch Domain](http/server-and-dispatch.md) — `ServerState`, `Router`, `RouteTable`, `RequestBatcher`, `BatchPolicy`, `Dispatcher`, `ShmDispatcher`, `InFlightRequests`, `PendingRequests`, `BackpressureController`, and `RingFactory`.

## Frontend, Packaging & WASM (`frontend/`)

- [Frontend Bundler, Codegen & Client SDK Domain](frontend/bundler-and-client-sdk.md) — `Bundler`, `SourceWatcher`, `SsrPool`, `BuildOrchestrator`, `DevServerProcess`, `TypeExtractor`, `HttpClient`, `RealtimeChannel`, `SseClient`, `WebSocketClient`, `SessionStore`, `HotReloadClient`, and `Console`.
- [Standalone & Onefile Packaging and PGO Tooling](frontend/packaging-and-pgo.md) — `BuildTarget`, `StandaloneBuilder`, `OnefileBuilder` (`ONEFILE_MAGIC = b"VTB1FILE"`), `RustPgoConfig` (`tools/pgo/rust_pgo.py`), and `PythonBuildInfo` (`tools/pgo/python_pgo.py`).
- [WASM Plugin Host & Browser Bridge](frontend/wasm.md) — `WasmPluginHost`, `WasmSandboxPolicy`, `WasmModuleConfig`, `WasmExecutionError`, and `@vertebra/client/wasm` `WasmClient`.

## Storage, Data, Telemetry & Extensions (`storage/`, `data/`, `telemetry/`, `extensions/`)

- [Object Storage, Presigning & Upload Tracking](storage/object-storage.md) — `vertebra-storage` (`vertebra._storage`: `NativeObjectStore`, `StorageBuffer`, `RustTaskAwaitable`) and `vertebra.storage` (`ObjectStore`, `SyncStore`, `AsyncStore`, `StorageRegistry`, `UploadTracker`, `ReadableFile`, S3/GCS/Azure/OCI/R2 signing).
- [Database Drivers, Cloud Dialers & Oracle Wallets](data/database-drivers-and-dialers.md) — `vertebra-db` (`CloudSqlInstanceUri`, `AlloyDbClusterUri`, `OracleConnectDescriptor`, RESP3 helpers) and `vertebra.db` (`SyncPostgresDriver`, `AsyncPostgresDriver`, MySQL/SQLite/Redis/Valkey/Oracle drivers, `CloudSqlDialer`, `AlloyDbDialer`, `DatabaseRegistry`).
- [Query Pipelines, Change Feeds, Tenant Predicates & DataFusion](data/query-pipeline-cdc-and-tenancy.md) — `QueryPipeline`, `CteStep`, `RelOverlay`, `ChangeFeed`, `TenantPredicate`, `DataFusionEngine`, and `@vertebra/client/arrow` `ArrowStreamClient`.
- [Logging, Trace Context, SQL Trace Propagation & GenAI Metrics](telemetry/logging-tracing-and-genai.md) — `AsyncLogCore` / `BoundedLogQueue`, `W3cTraceContext`, `LogSink`, `LogHandler`, `TraceContext`, `SpanExporter`, `SqlTracePropagator`, `GenAiRecorder`, and `@vertebra/client/telemetry`.
- [Litestar Integration: Route Compiler, Middleware Offload, and Fast Path](extensions/litestar.md) — `LitestarPlugin`, `LitestarRouteCompiler`, `MiddlewareOffloadCompiler`, `CompiledLitestarRoute`, `ChannelsBackend`, `MemoryStore`, and `LitestarEcosystemBridge`.
- [SQLSpec, Advanced Alchemy & SQLAlchemy Dialect Extensions](extensions/sqlspec-alchemy-and-dialects.md) — `SQLSpecPlugin`, `ProxySyncConfig`, `ProxyAsyncConfig`, `AlchemyPlugin`, `BrokerPlugin`, `+vtb` `DialectDescriptor` entry points, `Vector`, `BinaryJson`, and `AlembicImpl`.
- [Oracle APEX, ORDS & E-Business Suite Gateway](extensions/oracle-apex-and-ebs.md) — `ApexSessionContext`, `OracleSessionManager`, `ApexOpenAPIGenerator`, `ApexLangRunner`, `EbsGateway`, `ConfirmationGate`, and `@vertebra/client/apex` `ApexRegion`.
- [gRPC Services & Wire Framing](extensions/grpc.md) — `GrpcFrame` and `GrpcStreamBody` in `vertebra-http`, `@grpc_method`, `GrpcServiceRegistry`, `GrpcStatus`, and `ProtobufSchemaGenerator`.
