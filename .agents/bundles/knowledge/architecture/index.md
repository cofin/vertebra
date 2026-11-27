# Architecture Index

## Runtime (`runtime/`)

- [Runtime Topologies & Python Embedding](runtime/topologies-and-embedding.md) — Automatic, embedded, and shared-memory topologies, Tokio runtime sizing, multi-process supervision, and free-threaded worker pools.
- [SHM Worker Spawn, Shutdown & Recovery Hooks](runtime/shm-supervision-and-recovery.md) — Shared-memory FD inheritance, graceful worker termination, crash recovery backoff, and stale segment cleanup.
- [Server Platform & Process Lifecycle](runtime/platform-and-lifecycle.md) — Binary discovery, supervisor launcher, event-loop pools, GIL auditing, sync thread pools, cgroup sizing, lame-duck drain, and ZPages diagnostics.

## IPC & Shared Memory (`ipc/`)

- [Lock-Free Ring Buffers & Shared Memory](ipc/ring-buffers-and-shm.md) — Cache-line-aligned ring headers, Dekker wakeups, SPSC and MPMC ring transports, payload compression, and OS notification primitives.
- [Tiered Rings & In-Process Batch Bridge](ipc/tiered-rings-and-batch-bridge.md) — Size-tiered response rings, file-backed overflow fallbacks, streaming response frames, and the lock-free in-process batch bridge.

## HTTP Ingress & Dispatch (`http/`)

- [HTTP Ingress, Request Pipeline & Native Routing](http/ingress-and-routing.md) — TCP/TLS/HTTP/3 accept loop, fixed-order ingress offloads, environment configuration, and Rust radix routing.
- [Request Batching, Dispatch & Backpressure](http/batching-dispatch-and-backpressure.md) — Sharded request batchers, bridge and SHM dispatchers, sharded correlation slabs, isolated stream pumps, and lock-free admission control.
- [Protocol and Offload Modules in vertebra-http](http/protocol-and-offload-modules.md) — Inventory of wired HTTP offloads, wire protocols, realtime bridges, and auxiliary crate helpers.

## Python Worker & Protocols (`worker/`)

- [PyO3 FFI & Vectorized Batch Dispatch](worker/pyo3-ffi-and-batch-dispatch.md) — Zero-copy request and response wrappers, free-threaded class flags, inline sync batching, mount dispatch, and loop wakeups.
- [ASGI, RSGI & WSGI Runners](worker/asgi-and-wsgi-runners.md) — Rust-driven ASGI 3.0, RSGI 1.6, and PEP 3333 WSGI batch runners, lazy scopes, lifespan handling, and framework fastpaths.

## Frontend, SSR & Codegen (`frontend/`)

- [Oxc Bundler, HMR Watcher & QuickJS SSR Pool](frontend/bundler-hmr-and-ssr.md) — Native TypeScript/JSX transforms, debounced file watching, QuickJS SSR pooling with TTL caching, and build orchestration.
- [Route Codegen Targets & Inertia v3 Middleware](frontend/codegen-and-inertia.md) — Route and type extraction, TypeScript/schema/OpenAPI/Protobuf codegen, import maps, and sync/async Inertia middleware.
