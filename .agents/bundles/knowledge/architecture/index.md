---
title: "Architecture Index"
description: "Index of Vertebra's nested architecture chapters across runtime topologies, IPC transport, HTTP ingress, Python workers, and frontend tooling."
status: stable
tags:
  - architecture
  - index
  - runtime
  - ipc
  - http
---

# Architecture Index

## Runtime (`runtime/`)

- [Runtime Topologies & Python Embedding](runtime/topologies-and-embedding.md) — `Topology::Embedded` (`append_to_inittab!(pymodule_init)`, `BatchBridge`), `Topology::Shm` (`RingFactory`, `RingPairFds`), and Free-Threaded Python `3.14t` execution modes (`#[pymodule(gil_used = false)]` across `_vertebra`, `_bundler`, and `_storage`).
- [SHM Worker Spawn, Shutdown & Recovery Hooks](runtime/shm-supervision-and-recovery.md) — `RingFactory` FD inheritance (`VERTEBRA_*_FD`), `shutdown_workers` (`SIGTERM` then `SIGKILL`), and library-level recovery primitives (`WorkerLifecycle` with `pidfd_open` + `Child::try_wait` and 1 s–32 s exponential backoff, `RingPair::reclaim`, `cleanup_stale_shm`).
- [Server Platform & Process Lifecycle](runtime/platform-and-lifecycle.md) — `run_supervisor` binary launcher, `EventLoopPool`, `GilAuditReport`, `LoopShardedResource`, `SyncPool`, `ShmAllocationPolicy`, and container platform primitives (`CgroupQuota`, `LameDuckController`, `ZPagesRegistry`).

## IPC & Shared Memory (`ipc/`)

- [Lock-Free Ring Buffers & Shared Memory](ipc/ring-buffers-and-shm.md) — `ShmHeader` layout (metadata plus six cache-line-isolated head/wait/waiter counters), `SpscRing` + `SlotView` zero-copy borrow/release, Vyukov `MpmcRing`, LZ4/Zstd payload compression, and Linux (`futex`/`eventfd`) / macOS (`pipe`/`__ulock_wait`) wake primitives.
- [Tiered Rings & In-Process Batch Bridge](ipc/tiered-rings-and-batch-bridge.md) — `RingPair`, `TieredRingSet` (`small`/`medium`/`large`/`xlarge` response tiers with 20-byte `VRES` wire headers and spillover), `ResponseSlot` streaming frames, and in-process `BatchBridge` (`TransportArena`, `DataSlab`, `BufferPool`).

## HTTP Ingress & Dispatch (`http/`)

- [HTTP Ingress, Request Pipeline & Native Routing](http/ingress-and-routing.md) — `run_with_listener` accept loop (`ServerState`), HTTP/1.1 `pipeline_flush`, ALPN `TlsAcceptor` (`h2` + mTLS), `Router` (`normalize_typed_path` + FNV-1a `HandlerId`), and `RouteTable`.
- [Request Batching, Dispatch & Backpressure](http/batching-dispatch-and-backpressure.md) — `RequestBatcher` shard pool (`BatchPolicy::{Fixed, Adaptive}`), `Dispatcher` (`DispatchMode::{Direct, Bridge, SharedMemory}`), `ShmDispatcher` 8-shard `Mutex<InFlightRequests>` slabs, `PendingRequests` streaming `DashMap`, and `BackpressureController`.
- [Protocol and Offload Modules in vertebra-http](http/protocol-and-offload-modules.md) — Inventory of `vertebra-http` wire protocols (`h2c`, `http3`, `grpc`), realtime brokers (`sse`, `ws_mount`, `pubsub`), and offload helpers (`assets`, `compression`, `cors`, `health`, `payload`, `multipart`, `access_log`, `profile`, `discovery`, `dispatcher/direct`).

## Python Worker & Protocols (`worker/`)

- [PyO3 FFI & Vectorized Batch Dispatch](worker/pyo3-ffi-and-batch-dispatch.md) — `vertebra._vertebra` `BatchSnapshot`-backed `Request`/`Headers`/`Response`/`RequestBatch`, `compile_handler_spec` (`HandlerSpec`), `BatchDispatchPlan` (`sync_native`, `async_native`, `mounts`, `unmatched`), and `EventLoopPool` wakeups.
- [ASGI, RSGI & WSGI Runners](worker/asgi-and-wsgi-runners.md) — `AsgiRunner` (`_drive_coro`, `_step_coro`, `_resume_coro_in_task`, `_BatchProcessor`), 20-slot `AsgiScope`, `AsgiReceive`/`AsgiSend`, RSGI 1.6 (`RsgiAdapter`, `RsgiScope`, `RsgiHttpProtocol`), and `WsgiAdapter` (`WsgiEnviron`).

## Frontend, SSR & Codegen (`frontend/`)

- [Oxc Bundler, HMR Watcher & QuickJS SSR Pool](frontend/bundler-hmr-and-ssr.md) — Oxc `Bundler` (`vertebra._bundler`), `rolldown-notify-debouncer-mini` `SourceWatcher` (`HmrEvent`), `rquickjs` `SsrPool` with `ArrayQueue` slot leasing and `DashMap` 60 s TTL cache, and `BuildOrchestrator`.
- [Route Codegen Targets & Inertia v3 Middleware](frontend/codegen-and-inertia.md) — `RouteCollector` and `TypeExtractor` (`TypeNode` IR), `TypeScriptGenerator`, `ZodGenerator`, `ValibotGenerator`, `OpenApiGenerator`, `ProtobufSchemaGenerator`, `ProceduresGenerator`, `ManifestGenerator`, import maps, and `InertiaMiddleware`.
