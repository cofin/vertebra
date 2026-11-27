# Vertebra Knowledge Base Index

Curated current-state engineering documentation for Vertebra (`src/py/vertebra/`, `src/rs/`, `src/ts/vertebra-client/`).

## Workflow & Quality Gates

- **[Project Workflow & Commands](workflow.md)** — Canonical Makefile targets, polyglot extension rebuild rules, CI matrix tiers, and Flow chapter-batch loop.

## Architecture (`architecture/`)

- **[Architecture Index](architecture/index.md)**
- **Runtime (`architecture/runtime/`)**:
  - [Runtime Topologies & Python Embedding](architecture/runtime/topologies-and-embedding.md) — Automatic, embedded, and shared-memory runtime modes, multi-process supervision, and free-threaded worker pools.
  - [SHM Worker Spawn, Shutdown & Recovery Hooks](architecture/runtime/shm-supervision-and-recovery.md) — File-descriptor inheritance, graceful worker shutdown, crash recovery, and stale segment cleanup.
  - [Server Platform & Process Lifecycle](architecture/runtime/platform-and-lifecycle.md) — Binary discovery, launcher, event loop pools, GIL auditing, cgroup sizing, lame-duck drain, and ZPages diagnostics.
- **IPC & Shared Memory (`architecture/ipc/`)**:
  - [Lock-Free Ring Buffers & Shared Memory](architecture/ipc/ring-buffers-and-shm.md) — Cache-line-aligned ring headers, SPSC and MPMC rings, payload compression, and OS wake primitives.
  - [Tiered Rings & In-Process Batch Bridge](architecture/ipc/tiered-rings-and-batch-bridge.md) — Size-tiered response rings, overflow fallbacks, streaming frames, and the lock-free in-process request bridge.
- **HTTP Ingress & Dispatch (`architecture/http/`)**:
  - [HTTP Ingress, Request Pipeline & Native Routing](architecture/http/ingress-and-routing.md) — Accept loop, TLS/HTTP/3 termination, ordered ingress offloads, and Rust radix routing.
  - [Request Batching, Dispatch & Backpressure](architecture/http/batching-dispatch-and-backpressure.md) — Sharded request batchers, bridge and SHM dispatchers, correlation slabs, stream pumps, and atomic admission control.
  - [Protocol and Offload Modules in vertebra-http](architecture/http/protocol-and-offload-modules.md) — Inventory of wired HTTP offloads, wire protocols, realtime bridges, and crate helpers.
- **Python Worker & Protocols (`architecture/worker/`)**:
  - [PyO3 FFI & Vectorized Batch Dispatch](architecture/worker/pyo3-ffi-and-batch-dispatch.md) — Zero-copy request and response bindings, inline sync batching, mount routing, and event-loop wakeups.
  - [ASGI, RSGI & WSGI Runners](architecture/worker/asgi-and-wsgi-runners.md) — Rust-driven ASGI 3.0, RSGI 1.6, and PEP 3333 WSGI batch execution, lazy scopes, and framework fastpaths.
- **Frontend, SSR & Codegen (`architecture/frontend/`)**:
  - [Oxc Bundler, HMR Watcher & QuickJS SSR Pool](architecture/frontend/bundler-hmr-and-ssr.md) — Native TypeScript/JSX bundling, debounced file watching, SSR context pooling, and build orchestration.
  - [Route Codegen Targets & Inertia v3 Middleware](architecture/frontend/codegen-and-inertia.md) — Type extraction, multi-target client codegen, import maps, and sync/async Inertia middleware.

## Domains (`domains/`)

- **[Domain Vocabulary Index](domains/index.md)**
- **Framework (`domains/framework/`)**:
  - [Python Framework, Routing & Serialization Domain](domains/framework/routing-and-app.md)
  - [Framework Subdomains](domains/framework/subdomains.md)
  - [Distributed Queues, Cron Leader Election & Transactional Email](domains/framework/distributed-queues-and-email.md)
  - [Live State Synchronization](domains/framework/live-state.md)
  - [Framework Extension Limitations](domains/framework/framework-extension-limitations.md)
  - [`vtb` Command-Line Interface](domains/framework/cli.md)
- **Agent Protocols & Harness (`domains/agent/`)**:
  - [Agentic Harness, MCP 2026-07-28 & A2A 1.0 Protocols](domains/agent/mcp-and-a2a-protocol-harness.md)
  - [MCP Implementation Limitations](domains/agent/mcp-limitations.md)
  - [Shared-Memory Vector Slabs, Cloud & SQL Embeddings, and Local Broker](domains/agent/embeddings-and-vectors.md)
  - [Embeddings Limitations](domains/agent/embeddings-limitations.md)
- **Server & Worker Runtime (`domains/server/`)**:
  - [Python Server, Worker Loop & Protocol Adapters Domain](domains/server/worker-and-protocols.md)
  - [Multi-Lane Worker Runner](domains/server/worker-lanes.md)
  - [Server Runtime Limitations](domains/server/server-runtime-limitations.md)
- **IPC & HTTP (`domains/ipc/`, `domains/http/`)**:
  - [Rust IPC, Shared Memory & Ring Transport Domain](domains/ipc/transport-and-rings.md)
  - [Rust HTTP Ingress, Batching & Dispatch Domain](domains/http/server-and-dispatch.md)
- **Frontend, Packaging & WASM (`domains/frontend/`)**:
  - [Frontend Bundler, Codegen & Client SDK Domain](domains/frontend/bundler-and-client-sdk.md)
  - [Standalone & Onefile Packaging and PGO Tooling](domains/frontend/packaging-and-pgo.md)
  - [WASM Plugin Host & Browser Bridge](domains/frontend/wasm.md)
  - [Client, Bundler, WASM, and Packaging Limitations](domains/frontend/client-and-packaging-limitations.md)
- **Storage, Data, Telemetry & Extensions (`domains/storage/`, `domains/data/`, `domains/telemetry/`, `domains/extensions/`)**:
  - [Object Storage, Streaming & Presigning](domains/storage/object-storage.md)
  - [Object Storage Implementation Limitations](domains/storage/object-storage-limitations.md)
  - [Shared-Memory Connection Broker & Statement Cache](domains/data/broker-and-shm-leases.md)
  - [Shared-Memory Broker & Cross-Process Limitations](domains/data/shm-broker-limitations.md)
  - [Database Drivers & Cloud Dialers](domains/data/database-drivers-and-dialers.md)
  - [PostgreSQL Native Extension (`vertebra-postgres`)](domains/data/postgres-native-driver.md)
  - [PostgreSQL Native Driver Limitations](domains/data/postgres-limitations.md)
  - [SQLite Native Driver Limitations](domains/data/sqlite-limitations.md)
  - [Redis Native Driver Limitations](domains/data/redis-limitations.md)
  - [Valkey Native Driver Limitations](domains/data/valkey-limitations.md)
  - [Database Driver Limitations](domains/data/database-driver-limitations.md)
  - [Google Cloud BigQuery & Spanner Native Drivers](domains/data/gcp-bigquery-and-spanner-drivers.md)
  - [Native Pipelines, Change Feeds, Tenant Predicates & DataFusion](domains/data/query-pipeline-cdc-and-tenancy.md)
  - [Logging, Trace Context, SQL Trace Propagation & GenAI Metrics](domains/telemetry/logging-tracing-and-genai.md)
  - [Framework Extensions: Litestar, Starlette, FastAPI, Flask, and Django](domains/extensions/litestar.md)
  - [SQLSpec, Advanced Alchemy & SQLAlchemy Dialect Extensions](domains/extensions/sqlspec-alchemy-and-dialects.md)
  - [SQLSpec Adapter, Proxy & Store Limitations](domains/extensions/sqlspec-limitations.md)
  - [SQLAlchemy Dialects, DB-API & Alembic Limitations](domains/extensions/sqlalchemy-limitations.md)
  - [Advanced Alchemy & Cache Limitations](domains/extensions/alchemy-limitations.md)
  - [Oracle APEX, ORDS & 26ai Duality View Integration](domains/extensions/oracle-apex.md)
  - [gRPC Services & Wire Framing](domains/extensions/grpc.md)

## Patterns & Conventions (`patterns/`)

- **[Implementation Patterns Index](patterns/index.md)**
  - [Python Module Structure, Class Layout & PEP 695 Typing](patterns/python/module-and-class-layout.md)
  - [Hot-Path Caching & Single-Wake Async Dispatch](patterns/python/hot-path-and-async-dispatch.md)
  - [Serialization & Python Utility Conventions](patterns/python/serialization-and-utils.md)
  - [PyO3 0.30.1 FFI & GIL Discipline](patterns/rust/ffi-and-gil-discipline.md)
  - [Lock-Free Shared Memory & Sharded Concurrency](patterns/rust/lock-free-shm-and-sharding.md)
  - [Client SDK Root Exports, Subpaths & Build Entries](patterns/typescript/client-and-adapter-subpaths.md)

## Testing & Benchmarks (`testing/`)

- **[Testing & Verification Index](testing/index.md)**
  - [Polyglot Test Matrix & Environment Setup](testing/polyglot-test-matrix.md)
  - [Benchmark Suite & Free-Threaded Python](testing/benchmarks-and-free-threaded.md)
  - [Agentic Review & Benchmark Gates](testing/agentic-review.md)

## Gotchas (`gotchas/`)

- **[Gotchas & Non-Obvious Footguns Index](gotchas/index.md)**
  - [PyO3 FFI, Embedded Globals & Python Runtime Gotchas](gotchas/pyo3-and-python-runtime.md)
  - [Shared-Memory Rings & Cross-Platform IPC Gotchas](gotchas/shm-and-cross-platform-ipc.md)

## Decisions (`decisions/`)

- **[Architectural Decision Records Index](decisions/index.md)**
  - [ADR-001: Dual-Topology Embedded and Shared-Memory Runtime](decisions/adr-001-dual-topology-embedded-and-shm-runtime.md)
  - [ADR-002: Shared-Memory Database Broker and Arrow IPC Data Plane](decisions/adr-002-two-tier-shm-database-and-arrow-data-plane.md)
  - [ADR-003: PyPI `vertebra-core` Distribution and Wheel-Bundled TypeScript Client](decisions/adr-003-pypi-vertebra-core-distribution-and-wheel-bundled-client.md)
  - [ADR-004: Optional Driver Wheels and Shared-Memory Ownership](decisions/adr-004-optional-driver-wheels-and-shm-ownership.md)
  - [ADR-005: Persistent Object-Store TTL Envelopes](decisions/adr-005-persistent-object-store-ttl-envelopes.md)
  - [ADR-006: Native Cursor and Connection-Scoped Leases](decisions/adr-006-native-cursor-and-connection-scoped-leases.md)
