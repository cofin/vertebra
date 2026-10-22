---
type: Guide
title: "Python Server, Worker Loop & Protocol Adapters Domain"
description: "Canonical domain types for Vertebra's Python worker runtime, EventLoopPool, BatchDispatchPlan, ASGI/RSGI/WSGI adapters, server platform, and process supervision."
status: stable
tags: [domains, server, worker, asgi, wsgi]
sources:
  - resource: src/py/vertebra/server/worker.py
  - resource: src/py/vertebra/server/_dispatch.py
  - resource: src/py/vertebra/server/_loops.py
  - resource: src/py/vertebra/server/_gil_audit.py
  - resource: src/py/vertebra/server/platform.py
  - resource: src/py/vertebra/server/_asgi/_runner.py
  - resource: src/py/vertebra/server/_asgi/_queues.py
  - resource: src/py/vertebra/server/_rsgi/_adapter.py
  - resource: src/py/vertebra/server/_wsgi/_adapter.py
  - resource: src/py/vertebra/supervisor/_supervisor.py
  - resource: src/py/vertebra/supervisor/_worker.py
  - resource: src/rs/vertebra-py/src/asgi.rs
  - resource: src/rs/vertebra-py/src/rsgi/scope.rs
  - resource: src/rs/vertebra-py/src/rsgi/http.rs
  - resource: src/rs/vertebra-py/src/wsgi/environ.rs
---

# Python Server, Worker Loop & Protocol Adapters Domain

[`vertebra.server`](../../../../../src/py/vertebra/server/__init__.py) and [`vertebra.supervisor`](../../../../../src/py/vertebra/supervisor/__init__.py) host the embedded and multi-process Python worker loops, free-threaded event-loop pooling, batch partitioning, ASGI 3.0 / RSGI 1.6 / PEP 3333 WSGI adapters, container platform primitives, and process launch utilities.

## 1. Worker Loop, Event Loops & Batch Dispatch (`worker.py`, `_dispatch.py`, `_loops.py`, `_gil_audit.py`, `supervisor/_worker.py`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`EventLoopPool`](../../../../../src/py/vertebra/server/_loops.py) / [`configure_worker_affinity`](../../../../../src/py/vertebra/server/_loops.py) / [`resolve_loop_shards`](../../../../../src/py/vertebra/server/_loops.py) | `vertebra.server._loops` | Unified pool of `size` persistent event-loop threads (`vertebra-event-loop-{i}`, default `1` on GIL builds or `os.cpu_count()` on `3.14t`), supporting both single-loop (`size=1`) and sharded (`size > 1`) modes (`spawn`, `schedule`, `spawn_batch`, `schedule_batch`), plus opt-in `sched_setaffinity` pinning via `VERTEBRA_CPU_AFFINITY=1`. |
| [`GilAuditReport`](../../../../../src/py/vertebra/server/_gil_audit.py) | `vertebra.server._gil_audit` | Frozen `msgspec.Struct` returned by `GilAuditReport.audit()` at worker startup; detects extensions that re-enabled the GIL on `3.14t` and raises when `VERTEBRA_STRICT_FREE_THREADING=1`. |
| [`BatchDispatchPlan`](../../../../../src/py/vertebra/server/_dispatch.py) / [`HandlerSpec`](../../../../../src/py/vertebra/server/_dispatch.py) | `vertebra.server._dispatch` | Frozen `msgspec.Struct` partitioning a `RequestBatch` via `partition_batch()` into `sync_native`, `async_native`, `mounts`, and `unmatched`. |
| [`Worker`](../../../../../src/py/vertebra/server/worker.py) / [`run_worker`](../../../../../src/py/vertebra/server/worker.py) | `vertebra.server.worker` | Unified batch runner supporting both in-process `BatchBridge` (`get_batch` / `submit_response`) and multi-process SHM (`_shm_worker_loop`, `try_get_batch`). Honors `VERTEBRA_TRANSPORT_MODE`, `VERTEBRA_RING_NAME`, `VERTEBRA_SLOT_SIZE`, `VERTEBRA_SLOTS`, `VERTEBRA_BATCH_SIZE`, `VERTEBRA_THREADS`, `VERTEBRA_WORKERS`, `VERTEBRA_LOOP_SHARDS`, and `VERTEBRA_WORKER_ID`. |
| [`main`](../../../../../src/py/vertebra/supervisor/_worker.py) | `vertebra.supervisor._worker` | Subprocess worker entry point: imports `VERTEBRA_APP`, wraps raw ASGI apps when `VERTEBRA_NATIVE=0`, and calls `run_worker`, which attaches to inherited SHM FDs via `init_transport(mode="shm")`. |

## 2. ASGI 3.0, RSGI 1.6 & WSGI Protocol Adapters (`_asgi/`, `_rsgi/`, `_wsgi/`, `vertebra-py`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`AsgiRunner`](../../../../../src/py/vertebra/server/_asgi/_runner.py) | `vertebra.server._asgi._runner` | Executes ASGI 3.0 applications over single requests or batches (`__call__`, `spawn_batch`, `_BatchProcessor`) using single-step `_drive_coro` driving and promoting to an `asyncio.Task` (`_resume_coro_in_task`) on first suspension. |
| [`AsgiScope`](../../../../../src/rs/vertebra-py/src/asgi.rs) / [`AsgiScopeIterator`](../../../../../src/rs/vertebra-py/src/asgi.rs) | `vertebra._vertebra` | Lazy PyO3 ASGI 3.0 HTTP scope mapping backed by `Arc<BatchSnapshot>` with interned keys, a 20-slot inline cache, `deleted_known` bitmask, and a Rust `extras` map for writes. |
| [`AsgiReceive`](../../../../../src/py/vertebra/server/_asgi/_queues.py) / [`AsgiSend`](../../../../../src/py/vertebra/server/_asgi/_queues.py) | `vertebra.server._asgi._queues` | `async def` receive/send callables that return without awaiting so unary `http.request` / `http.response.*` exchanges complete without suspending. |
| [`RsgiAdapter`](../../../../../src/py/vertebra/server/_rsgi/_adapter.py) / [`RsgiScope`](../../../../../src/rs/vertebra-py/src/rsgi/scope.rs) / [`RsgiHttpProtocol`](../../../../../src/rs/vertebra-py/src/rsgi/http.rs) | `vertebra.server._rsgi._adapter`, `vertebra._vertebra` | RSGI 1.6 adapter (`is_rsgi_app`, `detect_interface`, `handle_request`, `run_batch`) paired with PyO3 `RsgiScope`, `RsgiHeaders`, `RsgiHttpProtocol`, and `RsgiWebsocketProtocol`. |
| [`WsgiAdapter`](../../../../../src/py/vertebra/server/_wsgi/_adapter.py) | `vertebra.server._wsgi._adapter` | Synchronous PEP 3333 WSGI adapter dispatched by `_dispatch` onto the shared `vertebra-wsgi` `ThreadPoolExecutor` (`max(128, cpu_count * 8)` threads). |
| [`WsgiEnviron`](../../../../../src/rs/vertebra-py/src/wsgi/environ.rs) / [`WsgiEnvironIterator`](../../../../../src/rs/vertebra-py/src/wsgi/environ.rs) | `vertebra._vertebra` | Lazy PyO3 PEP 3333 `environ` mapping (`WsgiInput`, `WsgiFileWrapper`, `extras` map) built in Rust. |

## 3. Platform Lifecycle, SHM Hardening & Process Launch (`platform.py`, `_supervisor.py`)

- **[`CgroupQuota`](../../../../../src/py/vertebra/server/platform.py), [`LameDuckController`](../../../../../src/py/vertebra/server/platform.py), [`ZPagesRegistry`](../../../../../src/py/vertebra/server/platform.py), [`ShmAllocationPolicy`](../../../../../src/py/vertebra/server/platform.py)**: Container cgroup v2 CPU/memory quota detection (`effective_workers`), lame-duck readiness draining (`503 {"status": "lame_duck"}`), `/healthz`, `/statusz`, `/varz`, `/rpcz` diagnostics, anonymous `memfd_create` / `/dev/shm` capacity validator (`min_free_bytes=16 MiB`), and `SCM_RIGHTS` descriptor passing (`send_fd` / `recv_fd`) (detailed in [Server Platform: Cgroup Quotas, Lame-Duck Shutdown, and zPages](../../architecture/runtime/platform-and-lifecycle.md)).
- **[`run_supervisor`](../../../../../src/py/vertebra/supervisor/_supervisor.py)**: Resolves and launches the Rust `vertebra-server` binary (`find_server_binary`, `build_server_command`), exports `VERTEBRA_DRAIN_TIMEOUT_MS` and `VERTEBRA_SHUTDOWN_TIMEOUT_MS`, optionally drives the Rich dev TUI, and escalates to `SIGTERM` on shutdown. Background job lanes live in [Multi-Lane Worker Runner](worker-lanes.md).
