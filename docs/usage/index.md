# Usage Guides

Practical guides for building, securing, testing, and deploying polyglot applications with Vertebra.

## Guides in This Section

| Guide | Topics Covered |
| --- | --- |
| **[Routing & Handlers](routing-and-handlers.md)** | `App`, `Router`, `Controller`, path/query parameters, `msgspec.Struct` request bodies, `DTO` field projection, and `Request`/`Response` objects. |
| **[Framework Features](framework-features.md)** | Dependency injection (`Container`, `Provide`), authentication and guards (`Guard`, `SecurityConfig`, `Principal`), OpenAPI & Scalar UI, `QueueService`, and `LiveState`. |
| **[Server & Workers](server-and-workers.md)** | Embedded vs. SHM topologies, worker lanes (`fast`, `async`, `cpu`, `blocking`), `EventLoopPool`, ASGI/WSGI/RSGI adapters, and free-threaded `3.14t`/`3.15t` execution. |
| **[Frontend & SSR](frontend-and-ssr.md)** | Embedded Rolldown/Oxc bundler (`Bundler`), pooled QuickJS SSR (`SsrClient`), Inertia v3 props (`DeferProp`, `MergeProp`), HTMX helpers, and `@vertebra/client`. |
| **[Databases & Storage](databases-and-storage.md)** | Native Rust database pools (`PostgresPool`, `MySqlPool`, `SqlitePool`) and multi-cloud object storage (`Store`, `StoragePipeline`, presigned URLs). |
| **[Framework Extensions](extensions.md)** | `VertebraLitestarPlugin`, `SQLSpecExtension`, `AlchemyExtension`, `ApexExtension`, `GrpcBridge`, `SessionExtension`, and `AutodiscoveryConfig`. |
| **[Agent Protocols (MCP & A2A)](agent-protocols.md)** | Model Context Protocol (`MCPRegistry`, `@mcp.tool`, `@mcp.resource`, `@mcp.prompt`, skills, tasks, stdio bridge), Agent-to-Agent (`A2AService`), and Google ADK. |
| **[Packaging & Deployment](packaging-and-deployment.md)** | Standalone PyApp single-file executables (`vtb build`), container cgroup tuning, WebAssembly sandboxes (`WasmModuleConfig`), and process supervision. |
| **[Testing](testing.md)** | Synchronous `TestClient` and asynchronous `AsyncTestClient` patterns for routes, WebSockets, SSE streams, and dependency overrides. |
