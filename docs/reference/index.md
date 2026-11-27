# API Reference

Reference documentation for Vertebra's Python packages, PyO3 extension stubs, the `@vertebra/client` TypeScript SDK, and the `vtb` CLI.

## Framework & Routing

| Module / Page | Summary |
| --- | --- |
| **[vertebra](vertebra.md)** | Top-level application setup, routing primitives, base `msgspec` schemas, JSON and Msgpack serializers, and core utilities. |
| **[Framework](framework.md)** | Dependency injection, security guards, OpenAPI and AsyncAPI builders, live state, RPC procedures, channels, email, and cache stores. |
| **[Task Queues](queues.md)** | Background task queues, retry and dead-letter policies, distributed cron scheduling, and database or cache queue backends. |

## Server & Runtime

| Module / Page | Summary |
| --- | --- |
| **[Server & Workers](server.md)** | Worker poll loops, isolated execution lanes, sharded event loops, ASGI/RSGI/WSGI adapters, GIL audits, and cgroup introspection. |
| **[Telemetry & Logging](telemetry.md)** | OpenTelemetry span export, GenAI token and cost metrics, database pool and event loop telemetry, SQL trace propagation, and structured logging. |
| **[Build, Wasm & Supervisor](build-and-wasm.md)** | Build orchestration, schema and client code generation, standalone and onefile packaging, Wasm sandboxes, and process supervision. |
| **[CLI Reference](cli.md)** | Generated reference for all `vtb` CLI commands, arguments, and options. |

## Databases

| Module / Page | Summary |
| --- | --- |
| **[Database Drivers & Pools](db.md)** | Connection configs, sync and async drivers, query pipelining, standalone DB-API adapters, DataFusion analytics, and Cloud SQL or AlloyDB dialers. |
| **[Database Capabilities](database-capabilities.md)** | Engine-owned agent tool providers, tenant connection scopes, change streams, notifications, and database-backed queue stores. |
| **[SQLSpec, Alchemy & Extensions](extensions.md)** | Integrations for Litestar, SQLSpec, SQLAlchemy, Advanced Alchemy, Django, Oracle APEX, gRPC services, and shared-memory session brokers. |

## Object Storage

| Module / Page | Summary |
| --- | --- |
| **[Object Storage](storage.md)** | Local and cloud object stores, presigned URLs, random-access readers and writers, GCP token caches, and HTTP byte-range streaming. |

## Frontend & Client SDK

| Module / Page | Summary |
| --- | --- |
| **[Inertia, HTMX & SSR](inertia.md)** | Inertia v3 responses and prop descriptors, HTMX request and response helpers, and pooled QuickJS or sidecar server-side rendering. |
| **[@vertebra/client (TypeScript)](typescript-client.md)** | TypeScript client SDK exports for HTTP, SSE, WebSockets, live state, Arrow, Wasm, APEX, agent protocols, and framework adapters. |

## Agents & Native SDKs

| Module / Page | Summary |
| --- | --- |
| **[Agent Runtime (MCP & A2A)](agent.md)** | Agent plugin and runtime loop, confirmation gates, MCP and A2A protocol services, workflow DAG execution, and session stores. |
| **[OKF & Knowledge Catalog](okf-and-catalog.md)** | Pure-Python OKF v0.2 bundle parsing, validation, and ZIP archiving alongside Google Cloud Dataplex Knowledge Catalog sync clients. |
| **[PyO3 Native Stubs](native-stubs.md)** | Low-level Rust extension classes and functions exposed through PEP 561 `.pyi` stubs across Vertebra's compiled modules. |
