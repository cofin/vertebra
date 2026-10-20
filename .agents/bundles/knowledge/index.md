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

- **[Project Workflow & Commands](workflow.md)** — Canonical `Makefile` targets (`make setup-env`, `make install`, `make dev`, `make rs-test`, `make py-test`, `make ts-test`, `make lint-all`, `make check`, `make bench`, `make bench-ft`), polyglot rebuild discipline, and Flow OKF v0.2 execution rules.

## Architecture (`architecture/`)

- **[Architecture Index](architecture/index.md)**
- **Runtime (`architecture/runtime/`)**:
  - [Runtime Topologies & Python Embedding](architecture/runtime/topologies-and-embedding.md) — `Topology::Embedded` (`append_to_inittab!`, `BatchBridge`), `Topology::SharedMemory`, and `3.14t` free-threaded execution.
  - [SHM Worker Supervision & Crash Recovery](architecture/runtime/shm-supervision-and-recovery.md) — `RingFactory` FD inheritance, `pidfd_open`/`waitid`, `RingPair::reclaim`, exponential backoff, and `/dev/shm` sweeping.
- **IPC & Shared Memory (`architecture/ipc/`)**:
  - [Lock-Free Ring Buffers & Shared Memory](architecture/ipc/ring-buffers-and-shm.md) — `ShmHeader` cache-line layout, `SpscRing` + `SlotView` zero-copy borrow/release, Vyukov `MpmcRing`, LZ4/Zstd compression, and Linux/macOS wake primitives.
  - [Tiered Rings & In-Process Batch Bridge](architecture/ipc/tiered-rings-and-batch-bridge.md) — `RingPair`, 4-tier `TieredRingSet` (`20B` `VRES` wire header), and `BatchBridge` (`TransportArena`, `DataSlab`, `BufferPool`).
- **HTTP Ingress & Dispatch (`architecture/http/`)**:
  - [HTTP/1.1 & HTTP/2 Ingress & Native Routing](architecture/http/ingress-and-routing.md) — `Server` listener loop, HTTP/1.1 `pipeline_flush`, HTTP/2 `TlsAcceptor`, `Router` (`normalize_typed_path` + FNV-1a `HandlerId`), `RouteTable`, and static assets.
  - [Request Batching, Dispatch & Backpressure](architecture/http/batching-dispatch-and-backpressure.md) — 8-shard `RequestBatcher` (`BatchPolicy`), `Dispatcher` (`Direct`, `Bridge`, `SharedMemory`), 256-shard `InFlightRequests`, and `BackpressureController`.
- **Python Worker & Protocols (`architecture/worker/`)**:
  - [PyO3 Zero-Copy FFI & Vectorized Batch Dispatch](architecture/worker/pyo3-ffi-and-batch-dispatch.md) — Slab-backed `Request`/`Headers`/`Response`/`RequestBatch`, `_handler_dispatch_spec`, 4-way `BatchDispatchPlan`, and single-wake `AsyncRuntime`.
  - [Task-Free ASGI & Sync WSGI Runners](architecture/worker/asgi-and-wsgi-runners.md) — `AsgiRunner` (`_drive_coro`, `_CoroResumer`), `AsgiScope`, `AsgiReceive`/`AsgiSend`, and `WsgiAdapter` (`WsgiEnviron`).
- **Frontend, SSR & Codegen (`architecture/frontend/`)**:
  - [Oxc Bundler, HMR & QuickJS SSR Pool](architecture/frontend/bundler-hmr-and-ssr.md) — Oxc `Bundler`, `rolldown-notify` `SourceWatcher`, `rquickjs` `SsrPool` (`ArrayQueue` + `DashMap` TTL cache), and `BuildOrchestrator`.
  - [TypeScript/Zod Codegen & Inertia v3 Protocol](architecture/frontend/codegen-and-inertia.md) — `TypeExtractor` (`TypeNode` IR), `TypeScriptGenerator`, `ZodGenerator`, `ManifestGenerator`, import maps, and `InertiaMiddleware`.

## Domains (`domains/`)

- **[Domain Vocabulary Index](domains/index.md)**
  - [Python Framework, Routing & Serialization (`domains/framework/`)](domains/framework/routing-and-app.md)
  - [Python Server, Worker Loop & Protocol Adapters (`domains/server/`)](domains/server/worker-and-protocols.md)
  - [Rust IPC, Shared Memory & Ring Transport (`domains/ipc/`)](domains/ipc/transport-and-rings.md)
  - [Rust HTTP Ingress, Batching & Dispatch (`domains/http/`)](domains/http/server-and-dispatch.md)
  - [Frontend Bundler, Codegen & TypeScript SDK (`domains/frontend/`)](domains/frontend/bundler-and-client-sdk.md)

## Patterns & Conventions (`patterns/`)

- **[Implementation Patterns Index](patterns/index.md)**
  - [Python Module Structure, Class Layout & PEP 695 Typing (`patterns/python/`)](patterns/python/module-and-class-layout.md)
  - [Python Hot-Path Caching & Single-Wake Async Dispatch (`patterns/python/`)](patterns/python/hot-path-and-async-dispatch.md)
  - [Rust PyO3 0.30.1 FFI & GIL Discipline (`patterns/rust/`)](patterns/rust/ffi-and-gil-discipline.md)
  - [Rust Lock-Free Shared Memory & Sharded Concurrency (`patterns/rust/`)](patterns/rust/lock-free-shm-and-sharding.md)
  - [TypeScript Client SDK & Framework Adapter Subpaths (`patterns/typescript/`)](patterns/typescript/client-and-adapter-subpaths.md)

## Testing & Benchmarks (`testing/`)

- **[Testing & Verification Index](testing/index.md)**
  - [Polyglot Test Matrix & Environment Setup](testing/polyglot-test-matrix.md)
  - [Benchmark Suite & Free-Threaded Python 3.14t](testing/benchmarks-and-free-threaded.md)

## Gotchas (`gotchas/`)

- **[Gotchas & Non-Obvious Footguns Index](gotchas/index.md)**
  - [PyO3 FFI, Embedded Globals & Python Runtime Gotchas](gotchas/pyo3-and-python-runtime.md)
  - [Shared-Memory Rings & Cross-Platform IPC Gotchas](gotchas/shm-and-cross-platform-ipc.md)
