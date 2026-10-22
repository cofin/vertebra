---
title: "Vertebra Knowledge Base Index"
description: "Curated map of Vertebra's nested architecture, domain taxonomy, implementation patterns, verification gates, and platform gotchas."
status: stable
tags:
  - knowledge
  - index
  - architecture
  - patterns
---

# Vertebra Knowledge Base Index

Curated current-state engineering documentation for **Vertebra** (`src/py/vertebra/`, `src/rs/`, `src/ts/vertebra-client/`).

---

## Workflow & Quality Gates

- **[Project Workflow & Commands](workflow.md)** — Canonical `Makefile` targets (`make setup-env`, `make install`, `make dev`, `make rs-test`, `make py-test`, `make ts-test`, `make lint-all`, `make review-check`, `make check`, `make build`, `make bench`, `make bench-ft`, `make bench-baseline`, `make bench-check`, `make upgrade`), polyglot rebuild discipline across `_vertebra`, `_bundler`, and `_storage`, and Flow OKF v0.2 execution rules.

## Architecture (`architecture/`)

- **[Architecture Index](architecture/index.md)**
- **Runtime (`architecture/runtime/`)**:
  - [Runtime Topologies & Python Embedding](architecture/runtime/topologies-and-embedding.md) — `Topology::Embedded` (`append_to_inittab!(pymodule_init)`, `BatchBridge`), `Topology::Shm` (`RingFactory`), and `3.14t` free-threaded execution.
  - [SHM Worker Spawn, Shutdown & Recovery Hooks](architecture/runtime/shm-supervision-and-recovery.md) — `RingFactory` FD inheritance, `RingPairFds` env handoff, `shutdown_workers`, and library-level recovery hooks (`WorkerLifecycle`, `RingPair::reclaim`, `cleanup_stale_shm`).
  - [Server Platform & Process Lifecycle](architecture/runtime/platform-and-lifecycle.md) — `run_supervisor`, `EventLoopPool`, `GilAuditReport`, `ShmAllocationPolicy`, `CgroupQuota`, `LameDuckController`, and `ZPagesRegistry`.
- **IPC & Shared Memory (`architecture/ipc/`)**:
  - [Lock-Free Ring Buffers & Shared Memory](architecture/ipc/ring-buffers-and-shm.md) — `ShmHeader` cache-line layout, `SpscRing` + `SlotView` zero-copy borrow/release, Vyukov `MpmcRing`, LZ4/Zstd compression, and Linux/macOS wake primitives.
  - [Tiered Rings & In-Process Batch Bridge](architecture/ipc/tiered-rings-and-batch-bridge.md) — `RingPair`, `TieredRingSet` (`small`/`medium`/`large`/`xlarge` response tiers, `VRES` wire frame header), `ResponseSlot`, and `BatchBridge` (`TransportArena`, `DataSlab`, `BufferPool`).
- **HTTP Ingress & Dispatch (`architecture/http/`)**:
  - [HTTP Ingress, Request Pipeline & Native Routing](architecture/http/ingress-and-routing.md) — `run_with_listener` accept loop (`ServerState`), HTTP/1.1 `pipeline_flush`, ALPN `TlsAcceptor` (`h2` + mTLS), `Router` (`normalize_typed_path` + FNV-1a `HandlerId`), and `RouteTable`.
  - [Request Batching, Dispatch & Backpressure](architecture/http/batching-dispatch-and-backpressure.md) — `RequestBatcher` (`BatchPolicy::{Fixed, Adaptive}`), `Dispatcher` (`DispatchMode::{Direct, Bridge, SharedMemory}`), `ShmDispatcher` 8-shard `InFlightRequests` slabs, `PendingRequests`, and `BackpressureController`.
  - [Protocol and Offload Modules in vertebra-http](architecture/http/protocol-and-offload-modules.md) — `h2c`, `http3`, `grpc`, `sse`, `ws_mount`, `pubsub`, `assets`, `compression`, `cors`, `health`, `payload`, `multipart`, `access_log`, `profile`, `discovery`, and `dispatcher/direct`.
- **Python Worker & Protocols (`architecture/worker/`)**:
  - [PyO3 FFI & Vectorized Batch Dispatch](architecture/worker/pyo3-ffi-and-batch-dispatch.md) — `BatchSnapshot`-backed `Request`/`Headers`/`Response`/`RequestBatch`, `compile_handler_spec`, `BatchDispatchPlan` (`sync_native`, `async_native`, `mounts`, `unmatched`), and `EventLoopPool`.
  - [ASGI, RSGI & WSGI Runners](architecture/worker/asgi-and-wsgi-runners.md) — `AsgiRunner` (`_drive_coro`, `_resume_coro_in_task`), `AsgiScope`, `AsgiReceive`/`AsgiSend`, RSGI 1.6 (`RsgiAdapter`, `RsgiScope`, `RsgiHttpProtocol`), and `WsgiAdapter` (`WsgiEnviron`).
- **Frontend, SSR & Codegen (`architecture/frontend/`)**:
  - [Oxc Bundler, HMR Watcher & QuickJS SSR Pool](architecture/frontend/bundler-hmr-and-ssr.md) — Oxc `Bundler`, `rolldown-notify-debouncer-mini` `SourceWatcher`, `rquickjs` `SsrPool` (`ArrayQueue` + `DashMap` 60 s TTL cache), and `BuildOrchestrator`.
  - [Route Codegen Targets & Inertia v3 Middleware](architecture/frontend/codegen-and-inertia.md) — `RouteCollector`, `TypeExtractor` (`TypeNode` IR), `TypeScriptGenerator`, `ZodGenerator`, `ValibotGenerator`, `OpenApiGenerator`, `ProtobufSchemaGenerator`, `ProceduresGenerator`, `ManifestGenerator`, and `InertiaMiddleware`.

## Domains (`domains/`)

- **[Domain Vocabulary Index](domains/index.md)**
- **Framework (`domains/framework/`)**:
  - [Python Framework, Routing & Serialization Domain](domains/framework/routing-and-app.md)
  - [Framework Subdomains](domains/framework/subdomains.md)
  - [Live State Synchronization](domains/framework/live-state.md)
  - [`vtb` Command-Line Interface](domains/framework/cli.md)
- **Server & Worker Runtime (`domains/server/`)**:
  - [Python Server, Worker Loop & Protocol Adapters Domain](domains/server/worker-and-protocols.md)
  - [Multi-Lane Worker Runner](domains/server/worker-lanes.md)
- **IPC & HTTP (`domains/ipc/`, `domains/http/`)**:
  - [Rust IPC, Shared Memory & Ring Transport Domain](domains/ipc/transport-and-rings.md)
  - [Rust HTTP Ingress, Batching & Dispatch Domain](domains/http/server-and-dispatch.md)
- **Frontend, Packaging & WASM (`domains/frontend/`)**:
  - [Frontend Bundler, Codegen & Client SDK Domain](domains/frontend/bundler-and-client-sdk.md)
  - [Standalone & Onefile Packaging and PGO Tooling](domains/frontend/packaging-and-pgo.md)
  - [WASM Plugin Host & Browser Bridge](domains/frontend/wasm.md)
- **Storage, Data, Telemetry & Extensions (`domains/storage/`, `domains/data/`, `domains/telemetry/`, `domains/extensions/`)**:
  - [Object Storage, Presigning & Upload Tracking](domains/storage/object-storage.md)
  - [Database Drivers, Cloud Dialers & Oracle Wallets](domains/data/database-drivers-and-dialers.md)
  - [Query Pipelines, Change Feeds, Tenant Predicates & DataFusion](domains/data/query-pipeline-cdc-and-tenancy.md)
  - [Logging, Trace Context, SQL Trace Propagation & GenAI Metrics](domains/telemetry/logging-tracing-and-genai.md)
  - [Litestar Integration: Route Compiler, Middleware Offload, and Fast Path](domains/extensions/litestar.md)
  - [SQLSpec, Advanced Alchemy & SQLAlchemy Dialect Extensions](domains/extensions/sqlspec-alchemy-and-dialects.md)
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
  - [Benchmark Suite & Free-Threaded Python 3.14t](testing/benchmarks-and-free-threaded.md)
  - [Agentic Review & Benchmark Gates](testing/agentic-review.md)

## Gotchas (`gotchas/`)

- **[Gotchas & Non-Obvious Footguns Index](gotchas/index.md)**
  - [PyO3 FFI, Embedded Globals & Python Runtime Gotchas](gotchas/pyo3-and-python-runtime.md)
  - [Shared-Memory Rings & Cross-Platform IPC Gotchas](gotchas/shm-and-cross-platform-ipc.md)
