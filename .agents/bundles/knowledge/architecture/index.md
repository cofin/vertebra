---
name: architecture-index
description: Index of Vertebra's nested architecture chapters across runtime topologies, IPC transport, HTTP ingress, Python workers, and frontend tooling.
---

# Architecture Index

## Runtime (`runtime/`)

- [Runtime Topologies & Python Embedding](runtime/topologies-and-embedding.md) — Embedded (`append_to_inittab!`, `BatchBridge`), Multi-Process (`WorkerSupervisor`, `RingFactory`), and Free-Threaded Python `3.14t` execution modes.
- [SHM Worker Supervision & Crash Recovery](runtime/shm-supervision-and-recovery.md) — FD inheritance (`VERTEBRA_*_FD`), `pidfd_open` / `WaitId` crash detection, `RingPair::reclaim`, exponential backoff, and orphaned `/dev/shm` cleanup.

## IPC & Shared Memory (`ipc/`)

- [Lock-Free Ring Buffers & Shared Memory](ipc/ring-buffers-and-shm.md) — `ShmHeader` 4-cache-line layout, `SpscRing` + `SlotView` zero-copy borrow/release, Vyukov `MpmcRing`, LZ4/Zstd payload compression, and Linux/macOS wake primitives.
- [Tiered Rings & In-Process Batch Bridge](ipc/tiered-rings-and-batch-bridge.md) — `RingPair`, 4-tier `TieredRingSet` with 20-byte `VRES` wire envelopes and spillover, and in-process `BatchBridge` (`TransportArena`, `DataSlab`, `BufferPool`).

## HTTP Ingress & Dispatch (`http/`)

- [HTTP/1.1 & HTTP/2 Ingress & Native Routing](http/ingress-and-routing.md) — `Server` listener loop, HTTP/1.1 `pipeline_flush`, HTTP/2 `TlsAcceptor`, `Router` (`normalize_typed_path` + FNV-1a `HandlerId`), `RouteTable`, and static assets.
- [Request Batching, Dispatch & Backpressure](http/batching-dispatch-and-backpressure.md) — 8-shard `RequestBatcher` (`BatchPolicy`), `Dispatcher` modes (`Direct`, `Bridge`, `Shm`, `TieredShm`), 256-shard `InFlightRequests`, and `BackpressureController`.

## Python Worker & Protocols (`worker/`)

- [PyO3 Zero-Copy FFI & Vectorized Batch Dispatch](worker/pyo3-ffi-and-batch-dispatch.md) — `vertebra._vertebra` slab-backed `Request`/`Headers`/`Response`/`RequestBatch`, 4-way `BatchDispatchPlan`, startup `_handler_dispatch_spec`, and single-wake `AsyncRuntime`.
- [Task-Free ASGI & Sync WSGI Runners](worker/asgi-and-wsgi-runners.md) — `AsgiRunner` direct coroutine driver (`_drive_coro`, `_CoroResumer`), `AsgiScope`, `AsgiReceive`/`AsgiSend`, and `WsgiAdapter` (`WsgiEnviron`).

## Frontend, SSR & Codegen (`frontend/`)

- [Oxc Bundler, HMR & QuickJS SSR Pool](frontend/bundler-hmr-and-ssr.md) — Oxc `Bundler`, `rolldown-notify` `SourceWatcher`, `rquickjs` `SsrPool` with `ArrayQueue` context leasing and `DashMap` TTL cache, and `BuildOrchestrator`.
- [TypeScript/Zod Codegen & Inertia v3 Protocol](frontend/codegen-and-inertia.md) — `TypeExtractor` (`TypeNode` IR), `TypeScriptGenerator`, `ZodGenerator`, `ManifestGenerator`, import-map vendor bundling, and `InertiaMiddleware`.
