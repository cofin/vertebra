# Usage Guides

Practical guides for building, scaling, testing, and deploying applications with Vertebra, organized by architectural subsystem.

## Framework & Routing

| Guide | Topics Covered |
| --- | --- |
| **[Routing & Handlers](routing-and-handlers.md)** | `App`, `Controller`, `Router`, `Route`, query/header/cookie markers, `msgspec.Struct` models, RPC `Procedure` / `ProcedureRouter`, lifecycle hooks, and auto-wiring. |
| **[Framework Features](framework-features.md)** | Dependency injection (`Provide`, `Inject`, `Container`), security guards, OpenAPI & AsyncAPI, background queues & cron, `LiveState`, pub/sub channels, caching, email, and telemetry. |

## Server & Runtime

| Guide | Topics Covered |
| --- | --- |
| **[Server & Workers](server-and-workers.md)** | Embedded vs. shared-memory topologies, worker lanes (`fast`, `async`, `cpu`, `blocking`), `vtb workers run`, ASGI/WSGI/RSGI adapters, z-pages, and free-threaded `3.14t`/`3.15t` execution. |
| **[Packaging & Deployment](packaging-and-deployment.md)** | Standalone and single-file binaries (`vtb build`, `StandaloneBuilder`, `OnefileBuilder`), TypeScript/Zod/OpenAPI/Protobuf codegen, Rust PGO, container cgroups, and WebAssembly sandboxes. |

## Databases

| Guide | Topics Covered |
| --- | --- |
| **[Databases & Analytics](databases.md)** | Single-RTT CTE statement stacking (`QueryPipeline`, `RelOverlay`), 9 native database drivers, zero-sidecar Cloud SQL & AlloyDB dialers, Row-Level Security (`TenantPredicate`), CDC (`ChangeFeed`), and DataFusion/Arrow SHM. |
| **[SQLSpec, SQLAlchemy & Extensions](extensions.md)** | `SQLSpecPlugin` (statement stacks & SHM proxy), `AlchemyPlugin` & `+vtb` dialects, `LitestarPlugin`, `GrpcServiceRegistry`, `ApexGateway` & 26ai Duality Views, and `BrokerPlugin`. |

## Object Storage

| Guide | Topics Covered |
| --- | --- |
| **[Object Storage](storage.md)** | Multi-cloud and local object storage (`ObjectStore`, `StorageRegistry`) across S3, GCS, Azure, R2, OCI, HTTP, Local, and Memory backends, presigned URLs (`SigningConfig`), PyArrow `ReadableFile`, and transactional `UploadTracker`. |

## Frontend, Agents & Testing

| Guide | Topics Covered |
| --- | --- |
| **[Frontend, Inertia & SSR](frontend-and-ssr.md)** | JavaScript bundler (`Bundler`, `BundleConfig`), QuickJS SSR (`EmbeddedSsrClient`), Inertia v3 (`inertia`, `DeferredProp`, `MergeProp`), HTMX helpers, and `@vertebra/client` adapters. |
| **[Agent Protocols (MCP & A2A)](agent-protocols.md)** | Model Context Protocol (`@tool`, `@prompt`, `@resource`, skills, tasks, stdio bridge), Agent-to-Agent (`A2AService`, `AgentCard`), `SpecTree`, `WorkflowEngine`, and `AgentRuntime`. |
| **[Testing](testing.md)** | In-process testing patterns for `App`, `Controller`, and `Router` handlers, dependency overrides, in-memory `ObjectStore`, `SyncSqliteDriver`, and `MemoryQueueBackend`. |
