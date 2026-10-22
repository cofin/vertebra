---
type: Guide
title: "Python Server, Worker Loop & Protocol Adapters Domain"
description: "Canonical domain types for Vertebra's Python worker runtime, AsgiEventLoopPool, BatchDispatchPlan, ASGI/RSGI/WSGI adapters, server platform, and process supervision."
status: stable
tags: [domains, server, worker, asgi, wsgi]
sources:
  - resource: src/py/vertebra/server/_native.py
  - resource: src/py/vertebra/server/_runtime_tuning.py
  - resource: src/py/vertebra/server/_gil_audit.py
  - resource: src/py/vertebra/server/_shm_hardening.py
  - resource: src/py/vertebra/server/platform.py
  - resource: src/py/vertebra/server/_asgi/_runner.py
  - resource: src/py/vertebra/server/_asgi/_queues.py
  - resource: src/py/vertebra/server/_rsgi/_adapter.py
  - resource: src/py/vertebra/server/_wsgi/_adapter.py
  - resource: src/py/vertebra/supervisor/_supervisor.py
  - resource: src/py/vertebra/supervisor/_worker.py
  - resource: src/rs/vertebra-py/src/asgi_scope.rs
  - resource: src/rs/vertebra-py/src/rsgi_scope.rs
  - resource: src/rs/vertebra-py/src/rsgi_protocol.rs
  - resource: src/rs/vertebra-py/src/wsgi_environ.rs
---

# Python Server, Worker Loop & Protocol Adapters Domain

[`vertebra.server`](../../../../../src/py/vertebra/server/__init__.py) and [`vertebra.supervisor`](../../../../../src/py/vertebra/supervisor/__init__.py) host the embedded and multi-process Python worker loops, free-threaded event-loop pooling, batch partitioning, ASGI 3.0 / RSGI 1.6 / PEP 3333 WSGI adapters, container platform primitives, and process launch utilities.

## 1. Worker Loop, Runtime Tuning & Batch Dispatch (`_native.py`, `_runtime_tuning.py`, `_gil_audit.py`, `supervisor/_worker.py`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`AsyncRuntime`](../../../../../src/py/vertebra/server/_native.py) | `vertebra.server._native` | Dedicated background `asyncio` event loop thread (`spawn_batch`, `schedule_batch`, `spawn`, `schedule`) used in-process on GIL builds when `worker_count == 1`. |
| [`AsgiEventLoopPool`](../../../../../src/py/vertebra/server/_runtime_tuning.py) / [`configure_worker_affinity`](../../../../../src/py/vertebra/server/_runtime_tuning.py) / [`is_free_threaded`](../../../../../src/py/vertebra/server/_runtime_tuning.py) | `vertebra.server._runtime_tuning` | Sharded pool of `size` persistent event-loop threads (`vertebra-event-loop-{i}`, default `os.cpu_count()`), selected when `worker_count > 1` or on `3.14t`, plus opt-in `sched_setaffinity` pinning via `VERTEBRA_CPU_AFFINITY=1`. |
| [`GilAuditReport`](../../../../../src/py/vertebra/server/_gil_audit.py) | `vertebra.server._gil_audit` | Frozen `msgspec.Struct` returned by `audit()` at worker startup; detects extensions that re-enabled the GIL on `3.14t` and raises when `VERTEBRA_STRICT_FREE_THREADING=1`. |
| [`BatchDispatchPlan`](../../../../../src/py/vertebra/server/_native.py) | `vertebra.server._native` | Frozen `msgspec.Struct` partitioning a `RequestBatch` via `_partition_batch()` into `sync_native`, `async_native`, `mounts`, and `unmatched`. |
| [`run_worker`](../../../../../src/py/vertebra/server/_native.py) | `vertebra.server._native` | Unified batch runner supporting both in-process `BatchBridge` (`get_batch` / `submit_response`) and multi-process SHM (`_shm_worker_loop`, `try_get_batch`). Honors `VERTEBRA_TRANSPORT_MODE`, `VERTEBRA_RING_NAME`, `VERTEBRA_SLOT_SIZE`, `VERTEBRA_SLOTS`, `VERTEBRA_BATCH_SIZE`, `VERTEBRA_THREADS`, `VERTEBRA_WORKERS`, and `VERTEBRA_WORKER_ID`. |
| [`main`](../../../../../src/py/vertebra/supervisor/_worker.py) | `vertebra.supervisor._worker` | Subprocess worker entry point: imports `VERTEBRA_APP`, wraps raw ASGI apps when `VERTEBRA_NATIVE=0`, and calls `run_worker`, which attaches to inherited SHM FDs via `init_transport(mode="shm")`. |

## 2. ASGI 3.0, RSGI 1.6 & WSGI Protocol Adapters (`_asgi/`, `_rsgi/`, `_wsgi/`, `vertebra-py`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`AsgiRunner`](../../../../../src/py/vertebra/server/_asgi/_runner.py) | `vertebra.server._asgi._runner` | Executes ASGI 3.0 applications over single requests or batches (`__call__`, `spawn_batch`, `_BatchProcessor`) using single-step `_drive_coro` driving and promoting to an `asyncio.Task` (`_resume_coro_in_task`) on first suspension. |
| [`AsgiScope`](../../../../../src/rs/vertebra-py/src/asgi_scope.rs) / [`AsgiScopeIterator`](../../../../../src/rs/vertebra-py/src/asgi_scope.rs) | `vertebra._vertebra` | Lazy PyO3 ASGI 3.0 HTTP scope mapping backed by `Arc<BatchSnapshot>` with interned keys, a 20-slot inline cache, `deleted_known` bitmask, and a Rust `extras` map for writes. |
| [`AsgiReceive`](../../../../../src/py/vertebra/server/_asgi/_queues.py) / [`AsgiSend`](../../../../../src/py/vertebra/server/_asgi/_queues.py) | `vertebra.server._asgi._queues` | `async def` receive/send callables that return without awaiting so unary `http.request` / `http.response.*` exchanges complete without suspending. |
| [`RsgiAdapter`](../../../../../src/py/vertebra/server/_rsgi/_adapter.py) / [`RsgiScope`](../../../../../src/rs/vertebra-py/src/rsgi_scope.rs) / [`RsgiHttpProtocol`](../../../../../src/rs/vertebra-py/src/rsgi_protocol.rs) | `vertebra.server._rsgi._adapter`, `vertebra._vertebra` | RSGI 1.6 adapter (`is_rsgi_app`, `detect_interface`, `handle_request`, `run_batch`) paired with PyO3 `RsgiScope`, `RsgiHeaders`, `RsgiHttpProtocol`, and `RsgiWebsocketProtocol`. |
| [`WsgiAdapter`](../../../../../src/py/vertebra/server/_wsgi/_adapter.py) | `vertebra.server._wsgi._adapter` | Synchronous PEP 3333 WSGI adapter dispatched by `_native` onto the shared `vertebra-wsgi` `ThreadPoolExecutor` (`max(128, cpu_count * 8)` threads). |
| [`WsgiEnviron`](../../../../../src/rs/vertebra-py/src/wsgi_environ.rs) / [`WsgiEnvironIterator`](../../../../../src/rs/vertebra-py/src/wsgi_environ.rs) | `vertebra._vertebra` | Lazy PyO3 PEP 3333 `environ` mapping (`WsgiInput`, `WsgiFileWrapper`, `extras` map) built in Rust. |

## 3. Platform Lifecycle, SHM Hardening & Process Launch (`platform.py`, `_shm_hardening.py`, `_supervisor.py`)

- **[`CgroupQuota`](../../../../../src/py/vertebra/server/platform.py), [`LameDuckController`](../../../../../src/py/vertebra/server/platform.py), [`ZPagesRegistry`](../../../../../src/py/vertebra/server/platform.py)**: Container cgroup v2 CPU/memory quota detection (`effective_workers`), lame-duck readiness draining (`503 {"status": "lame_duck"}`), and `/healthz`, `/statusz`, `/varz`, `/rpcz` diagnostics ( detailed in [Server Platform: Cgroup Quotas, Lame-Duck Shutdown, and zPages](../../architecture/runtime/platform-and-lifecycle.md)).
- **[`ShmAllocationPolicy`](../../../../../src/py/vertebra/server/_shm_hardening.py)**: Anonymous `memfd_create` / `/dev/shm` capacity validator (`min_free_bytes=16 MiB`) and `SCM_RIGHTS` descriptor passing (`send_fd` / `recv_fd`).
- **[`run_supervisor`](../../../../../src/py/vertebra/supervisor/_supervisor.py)**: Resolves and launches the Rust `vertebra-server` binary (`find_server_binary`, `build_server_command`), exports `VERTEBRA_DRAIN_TIMEOUT_MS` and `VERTEBRA_SHUTDOWN_TIMEOUT_MS`, optionally drives the Rich dev TUI, and escalates to `SIGTERM` on shutdown. Background job lanes live in [Multi-Lane Worker Runner](worker-lanes.md).
