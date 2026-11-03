# API Reference

Complete reference documentation for Vertebra's Python packages (`vertebra.*`), PEP 561 PyO3 native extension stubs (`_vertebra`, `_bundler`, `_storage`), the `@vertebra/client` TypeScript SDK, and the `vtb` CLI.

## Framework & Routing

| Module / Page | Summary |
| --- | --- |
| **[vertebra](vertebra.md)** | Top-level application (`App`), routing (`Controller`, `Router`, `Route`, HTTP method decorators), base `msgspec.Struct` schemas (`BaseStruct`, `FrozenBaseStruct`), JSON/Msgpack serializers, and `vertebra.utils` (`text`, `humanize`, `uuids`, `dispatch`, `schema`, `config_tools`, `deprecation`, `correlation`). |
| **[Framework](framework.md)** | Dependency injection (`Container`, `Provide`, `Inject`), security (`Guard`, `SecurityConfig`, `Principal`), OpenAPI & AsyncAPI builders, `LiveState`, procedures (`Procedure`, `ProcedureRouter`), channels, email, and response cache stores. |
| **[Task Queues](queues.md)** | Background task queues (`QueueService`, `QueueConfig`, `TaskRetryPolicy`), cron scheduling (`CronScheduler`), and Memory, PostgreSQL, Redis, Valkey, and Oracle backends. |

## Server & Runtime

| Module / Page | Summary |
| --- | --- |
| **[Server & Workers](server.md)** | Worker execution loop (`Worker`, `run_worker`), worker lanes (`LaneConfig`, `WorkerRunner`), `EventLoopPool`, ASGI/RSGI/WSGI adapters, free-threaded GIL audit (`GilAuditReport`), and cgroup platform utilities (`CgroupQuota`, `ShmAllocationPolicy`). |
| **[Telemetry & Logging](telemetry.md)** | OpenTelemetry spans (`TraceContext`, `SpanExporter`), GenAI token/cost metrics (`GenAiRecorder`), DB pool metrics (`DbPoolRecorder`), SQL trace propagation (`SqlTracePropagator`), and structured logging (`LoggingConfig`). |
| **[Build, Wasm & Supervisor](build-and-wasm.md)** | Build orchestration (`BuildConfig`, `BuildOrchestrator`), TypeScript/Zod/Valibot/OpenAPI/Protobuf codegen, standalone/onefile packaging, PGO, Wasm sandboxes (`WasmPluginHost`), and process supervision. |
| **[CLI Reference](cli.md)** | Generated reference for all `vtb` CLI commands (`run`, `dev`, `init`, `build`, `js`, `mcp`, `workers`), arguments, and options. |

## Databases

| Module / Page | Summary |
| --- | --- |
| **[Database Drivers & Pools](db.md)** | Single-RTT `QueryPipeline` / `RelOverlay`, native Rust-backed database drivers and connection configs across PostgreSQL, MySQL, SQLite, Oracle 26ai, SQL Server, DuckDB, DataFusion, Redis, Valkey, and Cloud SQL / AlloyDB dialers. |
| **[SQLSpec, Alchemy & Extensions](extensions.md)** | Integrations for Litestar (`LitestarPlugin`), SQLSpec (`create_sqlspec_plugin`), SQLAlchemy / Advanced Alchemy (`create_alchemy_plugin`), Oracle APEX (`ApexGateway`), gRPC (`GrpcServiceRegistry`), and SHM session brokers (`BrokerPlugin`). |

## Object Storage

| Module / Page | Summary |
| --- | --- |
| **[Object Storage](storage.md)** | Multicloud and local object storage (`ObjectStore`, `LocalConfig`, `S3Config`, `GcsConfig`, `AzureConfig`, `R2Config`, `OciConfig`, `MemoryConfig`), presigning (`SigningConfig`), and random-access `ReadableFile`. |

## Frontend & Client SDK

| Module / Page | Summary |
| --- | --- |
| **[Inertia, HTMX & SSR](inertia.md)** | Inertia v3 response protocol (`InertiaConfig`, `InertiaResponse`, `DeferredProp`, `MergeProp`, `ScrollProp`), HTMX helpers, and pooled QuickJS SSR (`SsrClient`, `SsrConfig`). |
| **[@vertebra/client (TypeScript)](typescript-client.md)** | Full-stack TypeScript client exports (`HttpClient`, `SseClient`, `WebSocketClient`, `LiveStateClient`, `ApexRegion`, `WasmClient`, `ArrowStreamClient`, `McpClient`, `A2aClient`) and 14 subpath entry points. |

## Agents & Native SDKs

| Module / Page | Summary |
| --- | --- |
| **[Agent Runtime (MCP & A2A)](agent.md)** | Model Context Protocol (`MCPRegistry`, `MCPConfig`, `MCPSkillsConfig`, `MCPTaskConfig`), Agent-to-Agent (`A2AService`, `A2AConfig`), `AgentRuntime`, and `WorkflowEngine`. |
| **[PyO3 Native Stubs](native-stubs.md)** | Low-level Rust/PyO3 extension classes and functions declared in `vertebra._vertebra`, `vertebra._bundler`, and `vertebra._storage`. |
