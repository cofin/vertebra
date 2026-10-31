# API Reference

Complete reference documentation for Vertebra's Python packages (`vertebra.*`), PEP 561 PyO3 native extension stubs (`_vertebra`, `_bundler`, `_storage`), the `@vertebra/client` TypeScript SDK, and the `vtb` CLI.

## Core & Framework

| Module / Page | Summary |
| --- | --- |
| **[vertebra](vertebra.md)** | Top-level application (`App`), routing (`Router`, `RouteDef`, HTTP method decorators), base `msgspec.Struct` schemas (`BaseStruct`, `FrozenBaseStruct`), and JSON/Msgpack serializers. |
| **[Framework](framework.md)** | Dependency injection (`Container`, `Provide`), security (`Guard`, `SecurityConfig`, `Principal`), OpenAPI & AsyncAPI builders, `LiveState`, procedures, channels, email, and response cache stores. |
| **[Task Queues](queues.md)** | Background task queues (`QueueService`, `QueueConfig`, `TaskRetryPolicy`), cron scheduling (`CronScheduler`), and Memory, PostgreSQL, Redis, Valkey, and Oracle backends. |
| **[Inertia & SSR](inertia.md)** | Inertia v3 response protocol (`InertiaConfig`, `InertiaResponse`, `DeferredProp`, `MergeProp`, `ScrollProp`), HTMX helpers, and pooled QuickJS SSR (`SsrClient`, `SsrConfig`). |
| **[Server & Workers](server.md)** | Worker execution loop (`Worker`, `run_worker`), worker lanes (`LaneConfig`, `WorkerRunner`), `EventLoopPool`, ASGI/RSGI/WSGI adapters, and cgroup platform utilities (`CgroupQuota`, `ShmAllocationPolicy`). |

## Data, Storage & Telemetry

| Module / Page | Summary |
| --- | --- |
| **[Object Storage](storage.md)** | Multi-cloud and local object storage (`ObjectStore`, `LocalConfig`, `S3Config`, `GcsConfig`, `AzureConfig`, `R2Config`, `OciConfig`, `MemoryConfig`), presigning (`SigningConfig`), and `UploadTracker`. |
| **[Database Pools](db.md)** | Native Rust-backed database drivers and connection configs across PostgreSQL, MySQL, SQLite, Oracle, SQL Server, DuckDB, DataFusion, Redis, Valkey, and Cloud SQL / AlloyDB dialers. |
| **[Telemetry & Logging](telemetry.md)** | OpenTelemetry spans (`TraceContext`, `SpanExporter`), GenAI token/cost metrics (`GenAiRecorder`), DB pool metrics (`DbPoolRecorder`), SQL trace propagation (`SqlTracePropagator`), and structured logging (`LoggingConfig`). |

## Agents, Extensions & Build

| Module / Page | Summary |
| --- | --- |
| **[Agent Runtime (MCP & A2A)](agent.md)** | Model Context Protocol (`MCPRegistry`, `MCPConfig`, `MCPSkillsConfig`, `MCPTaskConfig`), Agent-to-Agent (`A2AService`, `A2AConfig`), `AgentRuntime`, and `WorkflowEngine`. |
| **[Framework Extensions](extensions.md)** | Integrations for Litestar (`LitestarPlugin`), SQLSpec (`SQLSpecPlugin`), SQLAlchemy (`AlchemyPlugin`), Oracle APEX (`ApexGateway`), gRPC (`GrpcServiceRegistry`), and SHM session brokers (`BrokerPlugin`). |
| **[Build, Wasm & Supervisor](build-and-wasm.md)** | Build orchestration (`BuildConfig`, `BuildOrchestrator`), TypeScript/Zod/Valibot/OpenAPI/Protobuf codegen, standalone/onefile packaging, PGO, Wasm sandboxes (`WasmPluginHost`), and process supervision. |
| **[CLI Reference](cli.md)** | Generated reference for all `vtb` CLI commands (`run`, `dev`, `init`, `build`, `js`, `mcp`, `workers`), arguments, and options. |

## Native & Polyglot SDKs

| Module / Page | Summary |
| --- | --- |
| **[PyO3 Native Stubs](native-stubs.md)** | Low-level Rust/PyO3 extension classes and functions declared in `vertebra._vertebra`, `vertebra._bundler`, and `vertebra._storage`. |
| **[@vertebra/client (TypeScript)](typescript-client.md)** | Full-stack TypeScript client exports (`HttpClient`, `SseClient`, `WebSocketClient`, `LiveStateClient`, `ApexRegion`, `WasmClient`, `ArrowStreamClient`, `McpClient`, `A2aClient`) and 14 subpath entry points. |
