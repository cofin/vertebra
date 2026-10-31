# Usage Guides

Practical guides for building, securing, testing, and deploying polyglot applications with Vertebra.

## Guides in This Section

| Guide | Topics Covered |
| --- | --- |
| **[Routing & Handlers](routing-and-handlers.md)** | `App`, `Router`, `RouteDef`, path parameter extraction, `msgspec.Struct` request/response DTOs, and typed RPC `ProcedureRouter`. |
| **[Framework Features](framework-features.md)** | Dependency injection (`Container`, `Provide`), authentication and guards (`Guard`, `SecurityConfig`, `Principal`), OpenAPI & Scalar UI, `QueueService`, and `LiveState`. |
| **[Server & Workers](server-and-workers.md)** | Embedded vs. SHM topologies, worker lanes (`fast`, `async`, `cpu`, `blocking`), `EventLoopPool`, ASGI/WSGI/RSGI adapters, and free-threaded `3.14t`/`3.15t` execution. |
| **[Frontend & SSR](frontend-and-ssr.md)** | Embedded Rolldown/Oxc bundler (`Bundler`, `BundleConfig`), pooled QuickJS SSR (`EmbeddedSsrClient`, `SsrConfig`), Inertia v3 props (`DeferredProp`, `MergeProp`), HTMX helpers, and `@vertebra/client`. |
| **[Databases & Storage](databases-and-storage.md)** | Native database drivers (`PostgresConfig`, `MysqlConfig`, `SqliteConfig`, `SyncSqliteDriver`) and multi-cloud object storage (`ObjectStore`, `UploadTracker`, presigned URLs). |
| **[Framework Extensions](extensions.md)** | `LitestarPlugin`, `SQLSpecPlugin`, `AlchemyPlugin`, `ApexGateway`, `ApexIslandRegistry`, `GrpcServiceRegistry`, and `BrokerPlugin`. |
| **[Agent Protocols (MCP & A2A)](agent-protocols.md)** | Model Context Protocol (`MCPRegistry`, `@tool`, `MCPSkillsConfig`, `MCPTaskConfig`, stdio bridge), Agent-to-Agent (`A2AService`), and `AgentRuntime`. |
| **[Packaging & Deployment](packaging-and-deployment.md)** | Standalone and single-file executables (`vtb build`, `StandaloneBuilder`, `OnefileBuilder`), container cgroup tuning, WebAssembly sandboxes (`WasmModuleConfig`), and process supervision. |
| **[Testing](testing.md)** | In-process testing patterns for `App` and `Router` handlers, dependency injection containers, in-memory `ObjectStore`, `SyncSqliteDriver`, and `MemoryQueueBackend`. |
