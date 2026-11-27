# Usage Guides

Practical guides for building, scaling, testing, and deploying applications with Vertebra.

## Framework & Routing

| Guide | Topics Covered |
| --- | --- |
| **[Routing, Controllers & Procedures](routing-and-handlers.md)** | Apps, controllers, routers, parameter extractors, msgspec models, RPC procedures, lifecycle hooks, and auto-wiring. |
| **[DI, Security & OpenAPI](framework-features.md)** | Scoped dependency injection, security guards, CSRF and rate limiting, OpenAPI 3.1 with Scalar UI, and AsyncAPI 3.0. |
| **[Queues, LiveState, Email & Telemetry](queues-and-realtime.md)** | Background job queues, cron schedules, real-time state sync and pub/sub channels, key-value stores, email delivery, and OpenTelemetry metrics. |

## Server & Runtime

| Guide | Topics Covered |
| --- | --- |
| **[Server & Workers](server-and-workers.md)** | Embedded and shared-memory topologies, worker lanes, CLI runner commands, ASGI/WSGI/RSGI adapters, and free-threaded Python execution. |
| **[Packaging & Deployment](packaging-and-deployment.md)** | Standalone and single-file builds, TypeScript/Zod/OpenAPI/Protobuf codegen, container cgroups, and WebAssembly sandboxes. |
| **[Testing](testing.md)** | In-process handler testing, dependency overrides, and in-memory object store, SQLite, and queue test backends. |

## Databases & Analytics

| Guide | Topics Covered |
| --- | --- |
| **[Overview & Drivers](databases.md)** | Native database drivers, SQLSpec and Litestar integration, pipelining, and error handling across relational, analytical, cloud, and cache engines. |
| **[Cloud Dialers (Cloud SQL & AlloyDB)](cloud-dialers.md)** | Direct mTLS and IAM authentication for Cloud SQL, AlloyDB, AWS RDS, Aurora DSQL, and Azure Entra ID without proxy sidecars. |
| **[RLS, CDC & Arrow Analytics](analytics-and-cdc.md)** | Multi-tenant row-level security, logical replication and change streams, shared-memory Arrow cursors, DuckDB secret rotation, and DataFusion queries. |
| **[SQLSpec, SQLAlchemy & Extensions](extensions.md)** | Integrations for Litestar, SQLSpec, SQLAlchemy, Advanced Alchemy, Django, gRPC, Oracle APEX, and shared-memory session brokers. |

## Object Storage

| Guide | Topics Covered |
| --- | --- |
| **[Overview & Backends](storage.md)** | Sync and async object storage across local files, S3, GCS, Azure, R2, OCI, HTTP, and memory backends, plus streaming, checksums, and Arrow/Parquet I/O. |
| **[GCS, Active ADC & Presigning](storage-auth-and-signing.md)** | Shared GCP token caches, audience-bound ID tokens, keyless IAM signing, and presigned URLs across GCS, S3, R2, Azure, and OCI. |
| **[Range Reads, Parquet & Attachments](storage-parquet-and-orm.md)** | Seekable HTTP Range readers for PyArrow and Polars, Advanced Alchemy model attachments, and SQLSpec dataset exports. |

## AI, Agents & Embeddings

| Guide | Topics Covered |
| --- | --- |
| **[MCP, Skills & Stdio Bridge](agent-protocols.md)** | Model Context Protocol tools, prompts, and resources, skill bundles, async tasks, and the stdio MCP bridge. |
| **[A2A, WebMCP & Workflows](a2a-and-workflows.md)** | Agent-to-Agent discovery and services, browser WebMCP integration, workflow trees, and the agent runtime. |
| **[Vector Embeddings & Slabs](embeddings-and-vectors.md)** | Shared-memory vector slabs and caches, buffer views, local and remote embedders, and in-database embedding pushdown. |

## Frontend & Client SDK

| Guide | Topics Covered |
| --- | --- |
| **[Bundler, Inertia v3 & QuickJS SSR](frontend-and-ssr.md)** | Built-in Rust JavaScript bundler, Inertia v3 deferred and merge props, embedded QuickJS server rendering, and HTMX helpers. |
| **[TypeScript SDK & Framework Adapters](typescript-sdk.md)** | Bundled TypeScript client distribution, CLI installation, and adapters for React, Vue, Svelte, Angular, HTMX, and Vite. |
