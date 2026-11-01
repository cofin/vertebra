# Usage Guides

Practical guides for building, scaling, testing, and deploying applications with Vertebra, organized by architectural subsystem.

## Framework & Routing

| Guide | Topics Covered |
| --- | --- |
| **[Routing, Controllers & Procedures](routing-and-handlers.md)** | `App`, `Controller`, `Router`, `Route`, query/header/cookie markers, `msgspec.Struct` models, RPC `Procedure` / `ProcedureRouter`, lifecycle hooks, and auto-wiring. |
| **[DI, Security & OpenAPI](framework-features.md)** | Scoped dependency injection (`Provide`, `Inject`, `Container`), security guards (`Principal`, `CsrfConfig`, `RateLimitConfig`), OpenAPI 3.1 / Scalar UI, and AsyncAPI 3.0. |
| **[Queues, LiveState, Email & Telemetry](queues-and-realtime.md)** | Background task queues (`QueueService`, `CronScheduler`), real-time `LiveState` & `ChannelBus`, `KeyValueStore`, `EmailService`, and OpenTelemetry / GenAI metrics. |

## Server & Runtime

| Guide | Topics Covered |
| --- | --- |
| **[Server & Workers](server-and-workers.md)** | Embedded vs. shared-memory topologies, worker lanes (`fast`, `async`, `cpu`, `blocking`), `vtb run --with-workers`, `vtb workers run`, ASGI/WSGI/RSGI adapters, and `3.14t`/`3.15t` execution. |
| **[Packaging & Deployment](packaging-and-deployment.md)** | Standalone and single-file binaries (`vtb build`, `StandaloneBuilder`, `OnefileBuilder`), TypeScript/Zod/OpenAPI/Protobuf codegen, Rust PGO, container cgroups, and WebAssembly sandboxes. |
| **[Testing](testing.md)** | In-process testing patterns for `App`, `Controller`, and `Router` handlers, dependency overrides, in-memory `ObjectStore`, `SyncSqliteDriver`, and `MemoryQueueBackend`. |

## Databases & Analytics

| Guide | Topics Covered |
| --- | --- |
| **[Overview & Drivers](databases.md)** | First-class SQLSpec & Litestar integration (`create_sqlspec_plugin`) and the 9 native Rust shared-memory database and cache drivers. |
| **[Query Pipelines & CTE Stacking](query-pipelines.md)** | Single-RTT CTE statement stacking (`QueryPipeline`, `RelOverlay`), `build_jsonb_object`, and PgBouncer/PgCat/pgdog pooler-safe statement caching. |
| **[Cloud Dialers (Cloud SQL & AlloyDB)](cloud-dialers.md)** | Zero-sidecar Cloud SQL (`:3307`) and AlloyDB (`:5433`) ephemeral mTLS certificate dialers and IAM database authentication. |
| **[RLS, CDC & Arrow Analytics](analytics-and-cdc.md)** | Multi-tenant Row-Level Security (`TenantPredicate`), logical replication CDC (`ChangeFeed`), zero-copy `ArrowCursor`, DuckDB `httpfs` ADC rotation, and `DataFusionEngine`. |
| **[SQLSpec, SQLAlchemy & Extensions](extensions.md)** | `LitestarPlugin`, `create_sqlspec_plugin`, `create_alchemy_plugin`, SQLAlchemy 2.1 `+vtb` dialects, `Vector` & `BinaryJSON` column types, `GrpcServiceRegistry`, `ApexGateway`, and `BrokerPlugin`. |

## Object Storage

| Guide | Topics Covered |
| --- | --- |
| **[Overview & Backends](storage.md)** | Multicloud and local object storage (`ObjectStore`) across S3, GCS, Azure, R2, OCI, HTTP, Local, and Memory backends with GIL-free sync and `RustTaskAwaitable` async I/O. |
| **[GCS, Active ADC & Presigning](storage-auth-and-signing.md)** | Active `300s`-skew GCP ADC token caching (`RefreshingGcpCredentialProvider`, `GcpTokenCache`), keyless IAM `signBlob` V4 signing, and `SigningConfig` presigned URLs. |
| **[Range Reads, Parquet & Attachments](storage-parquet-and-orm.md)** | Seekable HTTP `Range` reads (`ReadableFile`) for PyArrow/Polars Parquet inspection and transactional `ObjectStoreBackend` model attachments. |

## AI, Agents & Embeddings

| Guide | Topics Covered |
| --- | --- |
| **[MCP, Skills & Stdio Bridge](agent-protocols.md)** | Model Context Protocol (`@tool`, `@prompt`, `@resource`, `MCPRegistry`), `SKILL.md` bundles (`MCPSkillsConfig`), async tasks, and `vtb ai mcp serve|bridge`. |
| **[A2A, WebMCP & Workflows](a2a-and-workflows.md)** | Agent-to-Agent (`A2AService`, `AgentCard` with automatic `AgentInterface` and dynamic URL resolution), browser WebMCP (`@vertebra/client/webmcp`), `SpecTree`, `DynamicWorkflow`, and `AgentRuntime`. |
| **[Vector Embeddings & Slabs](embeddings-and-vectors.md)** | Shared-memory `VectorSlab` and `VectorCache`, PEP 3118 `VectorView` / `VectorBatchView`, `Embedder`, `VertexEmbedder`, `GeminiEmbedder`, `AlloyDbEmbedder`, `OracleEmbedder`, `GemmaEmbedder`, `FastEmbedder`, `StaticEmbedder`, `RemoteEmbedder`, and `vtb ai models`. |

## Frontend & Client SDK

| Guide | Topics Covered |
| --- | --- |
| **[Bundler, Inertia v3 & QuickJS SSR](frontend-and-ssr.md)** | Built-in Rust JavaScript bundler (`Bundler`, `BundleConfig`), Inertia v3 (`@inertia`, `DeferredProp`, `MergeProp`), QuickJS SSR (`EmbeddedSSRClient`), and HTMX helpers. |
| **[TypeScript SDK & Framework Adapters](typescript-sdk.md)** | Wheel-native `@vertebra/client` distribution (`/__vertebra/client/*`), `vtb js install`, and React, Vue, Svelte, Angular, HTMX, and Vite adapters. |
