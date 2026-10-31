# Usage Guides

Practical guides for building, testing, and deploying applications with Vertebra.

## Guides in This Section

| Guide | Topics Covered |
| --- | --- |
| **[Routing & Handlers](routing-and-handlers.md)** | `App`, `Controller`, `Router`, `Route`, query/header/cookie markers, `msgspec.Struct` models, RPC `Procedure` / `ProcedureRouter`, lifecycle hooks, and auto-wiring. |
| **[Framework Features](framework-features.md)** | Dependency injection (`Provide`, `Inject`, `Container`), security guards, OpenAPI & AsyncAPI, background queues & cron, `LiveState`, pub/sub channels, caching, email, and telemetry. |
| **[Server & Workers](server-and-workers.md)** | Embedded vs. shared-memory modes, worker lanes (`fast`, `async`, `cpu`, `blocking`), `vtb workers run`, ASGI/WSGI/RSGI adapters, z-pages, and free-threaded `3.14t`/`3.15t` execution. |
| **[Frontend & SSR](frontend-and-ssr.md)** | JavaScript bundler (`Bundler`, `BundleConfig`), QuickJS SSR (`EmbeddedSsrClient`), Inertia v3 (`inertia`, `DeferredProp`, `MergeProp`), HTMX helpers, and `@vertebra/client` adapters. |
| **[Databases & Storage](databases-and-storage.md)** | CTE statement stacking (`QueryPipeline`), 9 native database drivers, Cloud SQL & AlloyDB dialers, Row-Level Security, CDC, DataFusion/Arrow, and cloud object storage (`ObjectStore`, `UploadTracker`). |
| **[Framework Extensions](extensions.md)** | `SQLSpecPlugin` (statement stacks & SHM proxy), `AlchemyPlugin` & `+vtb` dialects, `LitestarPlugin`, `GrpcServiceRegistry`, `ApexGateway` & 26ai Duality Views, and `BrokerPlugin`. |
| **[Agent Protocols (MCP & A2A)](agent-protocols.md)** | Model Context Protocol (`@tool`, `@prompt`, `@resource`, skills, tasks, stdio bridge), Agent-to-Agent (`A2AService`, `AgentCard`), `SpecTree`, `WorkflowEngine`, and `AgentRuntime`. |
| **[Packaging & Deployment](packaging-and-deployment.md)** | Standalone and single-file binaries (`vtb build`, `StandaloneBuilder`, `OnefileBuilder`), TypeScript/Zod/OpenAPI/Protobuf codegen, Rust PGO, container cgroups, and WebAssembly sandboxes. |
| **[Testing](testing.md)** | In-process testing patterns for `App`, `Controller`, and `Router` handlers, dependency overrides, in-memory `ObjectStore`, `SyncSqliteDriver`, and `MemoryQueueBackend`. |
