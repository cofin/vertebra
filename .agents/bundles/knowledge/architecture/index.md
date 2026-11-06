---
title: "Architecture Index"
description: "Index of Vertebra's nested architecture chapters across runtime topologies, IPC transport, HTTP ingress, Python workers, and frontend tooling."
tags:
  - architecture
  - index
  - runtime
  - ipc
  - http
---

# Architecture Index

## Runtime (`runtime/`)

- [Runtime Topologies & Python Embedding](runtime/topologies-and-embedding.md) — `Topology::{Auto, Embedded, Shm}`, explicit `tokio::runtime::Builder`, `EmbeddedSupervisor` `SO_REUSEPORT` multi-process supervision, `BatchBridge`, `RingFactory`, and Free-Threaded Python `3.14t`/`3.15t` execution modes (`#[pymodule(gil_used = false)]` across `_vertebra`, `_bundler`, `_storage`, `_bigquery`, and `_spanner`).
- [SHM Worker Spawn, Shutdown & Recovery Hooks](runtime/shm-supervision-and-recovery.md) — `RingFactory` FD inheritance (`VERTEBRA_*_FD`), `shutdown_workers` (`SIGTERM` then `SIGKILL`), and library-level recovery primitives (`WorkerLifecycle` with `pidfd_open` + `Child::try_wait` and 1 s–32 s exponential backoff, `RingPair::reclaim`, `cleanup_stale_shm`).
- [Server Platform & Process Lifecycle](runtime/platform-and-lifecycle.md) — `run_supervisor` binary launcher, `find_server_binary`, `EventLoopPool` (`uvloop` + inline `add_reader`), `GilAuditReport`, `SyncLoopShardedResource` / `AsyncLoopShardedResource`, `SyncPool`, `ShmAllocationPolicy`, and container platform primitives (`CgroupQuota`, `LameDuckController`, `ZPagesRegistry`).

## IPC & Shared Memory (`ipc/`)

- [Lock-Free Ring Buffers & Shared Memory](ipc/ring-buffers-and-shm.md) — `ShmHeader` (`ABI_VERSION = 3`) layout with Dekker `consumer_sleeping` and `active_flags` cache lines, cached-head `SpscRing` + `SlotView` zero-copy borrow/release, cache-line-padded Vyukov `MpmcRing` + `MpmcSlotView`, LZ4/Zstd payload compression, and Linux (`futex`/`eventfd`) / macOS (`pipe`/`__ulock_wait`) wake primitives.
- [Tiered Rings & In-Process Batch Bridge](ipc/tiered-rings-and-batch-bridge.md) — `RingPair`, right-sized `TieredRingSet` (`small` 4 KiB×512, `medium` 32 KiB×128, `large` 256 KiB×32, `xlarge` 2 MiB×8 with 20-byte `VRES` wire headers and `active_flags` bitmask), `ResponseSlot` streaming frames, and lock-free `BatchBridge` (`InProcessRequest`, `ResponseSink`, Dekker request `Notifier`).

## HTTP Ingress & Dispatch (`http/`)

- [HTTP Ingress, Request Pipeline & Native Routing](http/ingress-and-routing.md) — `run_with_listener` `SO_REUSEPORT` accept loop (`ServerState`), tuned `http1::Builder`, ALPN `TlsAcceptor` (`h2` + mTLS), `Router` (`normalize_typed_path` + FNV-1a `HandlerId`), and `RouteTable` (`MOUNT_HANDLER_BASE`).
- [Request Batching, Dispatch & Backpressure](http/batching-dispatch-and-backpressure.md) — Double-buffered `RequestBatcher` shard pool (`BatchLimit`, compact binary headers), `Dispatcher` (`DispatchBackend::{Direct, Bridge, Shm}`), lock-free batch-affinitized `ShmDispatcher` 8-shard `Mutex<InFlightRequests>` slabs, `PendingRequests` streaming `DashMap`, and lock-free atomic `BackpressureController`.
- [Protocol and Offload Modules in vertebra-http](http/protocol-and-offload-modules.md) — Inventory of wired `vertebra-http` offloads (`cors`, `compression`, `assets`, `openapi_cache`, `health` via `VERTEBRA_OFFLOAD_CONFIG`), wire protocols (`h2c`, `http3`, `grpc`), realtime brokers (`sse`, `ws_mount`, `pubsub`), and library helpers (`payload`, `multipart`, `access_log`, `profile`, `discovery`, `dispatcher/direct`).

## Python Worker & Protocols (`worker/`)

- [PyO3 FFI & Vectorized Batch Dispatch](worker/pyo3-ffi-and-batch-dispatch.md) — `vertebra._vertebra` zero-copy `BatchSnapshot`-backed `Request` (`body_view` buffer protocol)/`Headers`/`Response`/`RequestBatch`, Python 3.15/3.15t `immutable_type` and conditional `freelist`, inline vectorized sync dispatch (`sync_to_thread`), `O(1)` mount routing, and `EventLoopPool` wakeups.
- [ASGI, RSGI & WSGI Runners](worker/asgi-and-wsgi-runners.md) — `AsgiRunner` + Rust `AsgiProtocol` / `PyEmptyAwaitable` / `drive_asgi_batch` (`PyIter_Send` + `_SteppedCoroutine`), `FastPath`, 20-slot `AsgiScope`, RSGI 1.6 (`RsgiAdapter`, lazy `RsgiScope`, direct-submitting `RsgiHttpProtocol`, `drive_rsgi_batch`), and `WsgiAdapter` (`WsgiEnviron`).

## Frontend, SSR & Codegen (`frontend/`)

- [Oxc Bundler, HMR Watcher & QuickJS SSR Pool](frontend/bundler-hmr-and-ssr.md) — Oxc `Bundler` (`vertebra._bundler`), `rolldown-notify-debouncer-mini` `SourceWatcher` (`HmrEvent`), `rquickjs` `SSRPool` with `ArrayQueue` slot leasing and `DashMap` 60 s TTL cache, and `BuildOrchestrator`.
- [Route Codegen Targets & Inertia v3 Middleware](frontend/codegen-and-inertia.md) — `RouteCollector` and `TypeExtractor` (`TypeNode` IR), `TypeScriptGenerator`, `ZodGenerator`, `ValibotGenerator`, `OpenAPIGenerator`, `ProtobufGenerator`, `ProcedureClientGenerator`, `ManifestGenerator`, import maps, and `SyncInertiaMiddleware` / `AsyncInertiaMiddleware`.
