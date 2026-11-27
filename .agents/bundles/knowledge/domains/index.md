# Domain Vocabulary Index

Vertebra names classes, structs, and functions directly after their domain role across Python, Rust, and TypeScript, without brand prefixes (except `VertebraError`, host-framework plugin classes, and CLI groups) or compatibility aliases.

## Framework (`framework/`)

- [Python Framework, Routing & Serialization Domain](framework/routing-and-app.md) — Core application, router, controller, request/response models, Inertia middleware, serializers, and optional dependency loading.
- [Framework Subdomains](framework/subdomains.md) — Dependency injection, security, autowire, typed procedures, HTMX, SSR, OpenAPI/AsyncAPI, channels, stores, caching, and host integrations.
- [Distributed Queues, Cron Leader Election & Transactional Email](framework/distributed-queues-and-email.md) — Pluggable queue backends, leader-elected cron scheduling, multi-lane workers, and transactional email transports.
- [Live State Synchronization](framework/live-state.md) — MessagePack state deltas, hybrid logical clocks, last-write-wins registers, and offline client synchronization.
- [Framework Extension Limitations](framework/framework-extension-limitations.md) — Operational boundaries for Django backends, Flask caching, Starlette/FastAPI initialization, and SHM queue naming.
- [`vtb` Command-Line Interface](framework/cli.md) — Core `vtb` subcommands, plugin extension groups, entry-point discovery, console formatting, and dev TUI.

## Agent Protocols & Harness (`agent/`)

- [Agentic Harness, MCP 2026-07-28 & A2A 1.0 Protocols](agent/mcp-and-a2a-protocol-harness.md) — Dual-mode MCP and A2A 1.0 protocol support, session/task/stream stores, identity verification, webhooks, and GenAI telemetry.
- [MCP Implementation Limitations](agent/mcp-limitations.md) — Per-process default store bounds, live-stream push limits, client protocol scope, mTLS header constraints, and host shims.
- [Shared-Memory Vector Slabs, Cloud & SQL Embeddings, and Local Broker](agent/embeddings-and-vectors.md) — POSIX shared-memory vector slabs, zero-copy buffer views, cloud and SQL pushdown embedders, and the local embedding broker.
- [Embeddings Limitations](agent/embeddings-limitations.md) — ADC OIDC audience limits, rejected credential modes, inbound vs. outbound auth boundaries, and loopback test coverage.

## Server & Worker Runtime (`server/`)

- [Python Server, Worker Loop & Protocol Adapters Domain](server/worker-and-protocols.md) — Worker lifecycle, event-loop pools, dispatch planning, ASGI/RSGI/WSGI adapters, cgroup sizing, and ZPages diagnostics.
- [Multi-Lane Worker Runner](server/worker-lanes.md) — Priority queue lane configuration, job resolution, oneshot execution, and `vtb workers run`.
- [Server Runtime Limitations](server/server-runtime-limitations.md) — Mid-stream error termination, slow-client SHM backpressure, disconnect visibility, and shared `libpython` checks.

## IPC & HTTP (`ipc/`, `http/`)

- [Rust IPC, Shared Memory & Ring Transport Domain](ipc/transport-and-rings.md) — Shared-memory regions, SPSC/MPMC rings, tiered ring pairs, in-process bridge, transport arenas, and compression codecs.
- [Rust HTTP Ingress, Batching & Dispatch Domain](http/server-and-dispatch.md) — Server state, radix routing, request batching, SHM/bridge dispatchers, stream registries, and admission control.

## Frontend, Packaging & WASM (`frontend/`)

- [Frontend Bundler, Codegen & Client SDK Domain](frontend/bundler-and-client-sdk.md) — Oxc bundling, HMR watching, SSR pooling, client codegen, and the `@vertebra/client` browser SDK.
- [Standalone & Onefile Packaging and PGO Tooling](frontend/packaging-and-pgo.md) — Relocatable standalone staging, self-extracting onefile executables, wheel isolation, and contributor Rust/CPython PGO pipelines.
- [WASM Plugin Host & Browser Bridge](frontend/wasm.md) — WebAssembly module validation, sandbox fuel policies, and the browser WASM client bridge.
- [Client, Bundler, WASM, and Packaging Limitations](frontend/client-and-packaging-limitations.md) — Bundler entry and watch bounds, optional zstd compression, WASI network restrictions, WebMCP confirmation, and release manifest guards.

## Storage, Data, Telemetry & Extensions (`storage/`, `data/`, `telemetry/`, `extensions/`)

- [Object Storage, Streaming & Presigning](storage/object-storage.md) — Multicloud Rust object-store bindings, shared GCP token caches, conditional writes, streaming readers/writers, presigning, and Arrow/Parquet codecs.
- [Object Storage Implementation Limitations](storage/object-storage-limitations.md) — Provider, multipart, compose, timestamp, and emulator boundaries across Rust and Python storage layers.
- [Shared-Memory Connection Broker & Statement Cache](data/broker-and-shm-leases.md) — Cross-process SHM connection lease tables, seqlock statement caches, crash quarantine, and Python broker transports.
- [Shared-Memory Broker & Cross-Process Limitations](data/shm-broker-limitations.md) — Native capsule exclusions, Unix attach preconditions, macOS SHM name length limits, and partition/cleanup caps.
- [Database Drivers & Cloud Dialers](data/database-drivers-and-dialers.md) — Per-engine adapter contracts, standalone driver wheels, Arrow SHM bridges, cloud IAM token providers, and cloud dialers.
- [PostgreSQL Native Extension (`vertebra-postgres`)](data/postgres-native-driver.md) — Standalone PostgreSQL wire driver covering authentication, cloud dialers, pooling, pipelines, binary COPY, Arrow SHM, and logical replication.
- [PostgreSQL Native Driver Limitations](data/postgres-limitations.md) — Operational boundaries across queue executors, external poolers, replication slots, Arrow conversion, and SQLSpec alignment.
- [SQLite Native Driver Limitations](data/sqlite-limitations.md) — Single-writer WAL serialization, in-memory worker affinity, dynamic Arrow schema inference, and extension loading rules.
- [Redis Native Driver Limitations](data/redis-limitations.md) — Dedicated pub/sub sockets, Cluster hash-tag rules, RESP3 client-tracking invalidations, and Unix socket constraints.
- [Valkey Native Driver Limitations](data/valkey-limitations.md) — Dedicated pub/sub sockets, Valkey 9.0 hash expiration, Cluster hash-tag rules, and Unix socket constraints.
- [Database Driver Limitations](data/database-driver-limitations.md) — Companion wheel loading, DuckDB benchmark packaging, queue store contracts, cloud IAM caveats, and Arrow IPC/adapter benchmarks.
- [Google Cloud BigQuery & Spanner Native Drivers](data/gcp-bigquery-and-spanner-drivers.md) — Standalone free-threaded BigQuery and Spanner wheels, Arrow SHM framing, object-store staging, and agent tool providers.
- [Native Pipelines, Change Feeds, Tenant Predicates & DataFusion](data/query-pipeline-cdc-and-tenancy.md) — Pipelined execution protocols, CDC change feeds, ANSI tenant predicates, and embedded DataFusion queries.
- [Logging, Trace Context, SQL Trace Propagation & GenAI Metrics](telemetry/logging-tracing-and-genai.md) — Async Rust log core, W3C trace context, span export, SQL trace comments, GenAI metrics, and client telemetry.
- [Framework Extensions: Litestar, Starlette, FastAPI, Flask, and Django](extensions/litestar.md) — Host-framework plugins, route compilers, middleware offloads, fastpaths, channels/stores, and native Django database backends.
- [SQLSpec, Advanced Alchemy & SQLAlchemy Dialect Extensions](extensions/sqlspec-alchemy-and-dialects.md) — Native SHM SQLSpec adapters, universal broker proxies, cache regions, SQLAlchemy `+vtb` dialects, column types, and Alembic hooks.
- [SQLSpec Adapter, Proxy & Store Limitations](extensions/sqlspec-limitations.md) — Cached-statement executor rules, nested schema defaults, data dictionary bases, Oracle sessionless gaps, session resets, and ADK/storage caveats.
- [SQLAlchemy Dialects, DB-API & Alembic Limitations](extensions/sqlalchemy-limitations.md) — Sync adapter boundaries, non-transactional cloud DDL rejections, Spangres two-phase commit limits, and online DDL batching.
- [Advanced Alchemy & Cache Limitations](extensions/alchemy-limitations.md) — Process-local query caching, SHM cache key/value size bounds, missing invalidation rings, and Advanced Alchemy compatibility caveats.
- [Oracle APEX, ORDS & 26ai Duality View Integration](extensions/oracle-apex.md) — APEX session auth handoff, reactive islands, offline HLC sync, federated queries, ORDS/Duality Views, and client integration.
- [gRPC Services & Wire Framing](extensions/grpc.md) — Rust gRPC wire framing, service registration, status codes, and Protobuf schema generation.
