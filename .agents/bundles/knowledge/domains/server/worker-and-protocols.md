---
type: Guide
title: "Python Server, Worker Loop & Protocol Adapters Domain"
description: "Canonical domain types for Vertebra's Python worker runtime, uvloop EventLoopPool, BatchDispatchPlan, Rust ASGI/RSGI drivers, WSGI adapter, server platform, and process supervision."
status: stable
tags: [domains, server, worker, asgi, rsgi, wsgi]
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
  - resource: src/rs/vertebra-py/src/rsgi/mod.rs
  - resource: src/rs/vertebra-py/src/rsgi/scope.rs
  - resource: src/rs/vertebra-py/src/rsgi/http.rs
  - resource: src/rs/vertebra-py/src/wsgi/environ.rs
---

# Python Server, Worker Loop & Protocol Adapters Domain

[`vertebra.server`](../../../../../src/py/vertebra/server/__init__.py) and [`vertebra.supervisor`](../../../../../src/py/vertebra/supervisor/__init__.py) host the embedded and multi-process Python worker loops, `uvloop` event-loop management, inline vectorized batch execution, Rust C-API `PyIter_Send` ASGI 3.0 and RSGI 1.6 drivers, PEP 3333 WSGI adapters, container platform primitives, and process launch utilities.

## 1. Worker Loop, Event Loops & Batch Dispatch (`worker.py`, `_dispatch.py`, `_loops.py`, `_gil_audit.py`, `supervisor/_worker.py`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`EventLoopPool`](../../../../../src/py/vertebra/server/_loops.py) / [`create_event_loop`](../../../../../src/py/vertebra/server/_loops.py) / [`configure_worker_affinity`](../../../../../src/py/vertebra/server/_loops.py) / [`resolve_loop_shards`](../../../../../src/py/vertebra/server/_loops.py) | `vertebra.server._loops` | Automatically creates `uvloop` event loops on POSIX (`winloop` on Windows, `asyncio` fallback), coalesces multi-loop scheduling in `spawn_batch` / `schedule_batch` (`1` loop on GIL builds, sharded on `3.14t`/`3.15t`), and supports `VERTEBRA_CPU_AFFINITY=1`. |
| [`GilAuditReport`](../../../../../src/py/vertebra/server/_gil_audit.py) | `vertebra.server._gil_audit` | Frozen `msgspec.Struct` returned by `GilAuditReport.audit()` at worker startup; detects extensions that re-enabled the GIL on `3.14t`/`3.15t` and raises when `VERTEBRA_STRICT_FREE_THREADING=1`. |
| [`BatchDispatchPlan`](../../../../../src/py/vertebra/server/_dispatch.py) / [`HandlerSpec`](../../../../../src/py/vertebra/server/_dispatch.py) | `vertebra.server._dispatch` | Frozen `msgspec.Struct` compiling route signatures (`sync_to_thread: bool = False`), executing default sync handlers inline via `submit_batch_raw`, offloading opt-in `sync_to_thread=True` routes once to `ThreadPoolExecutor`, accepting zero-copy `bytes \| bytearray \| memoryview` results, and unpinning `Request` before `await` via `_AsyncErrorStub`. |
| [`Worker`](../../../../../src/py/vertebra/server/worker.py) / [`run_worker`](../../../../../src/py/vertebra/server/worker.py) | `vertebra.server.worker` | Unified batch runner supporting direct `get_batch` blocking loops for pure-sync Embedded apps and single-thread `uvloop` + `loop.add_reader(notify_fd, ...)` Dekker `prepare_request_sleep` / `cancel_request_sleep` polling for SHM and async/ASGI/RSGI single-consumer Embedded workers, plus `O(1)` `MOUNT_HANDLER_BASE` mount resolution (`single_root_asgi.dispatch_app`). |
| [`main`](../../../../../src/py/vertebra/supervisor/_worker.py) | `vertebra.supervisor._worker` | Subprocess SHM worker entry point: imports `VERTEBRA_APP`, skips native route registration when `VERTEBRA_NATIVE=0` (`--asgi`) so `App` instances mount once as ASGI without double-registration, and calls `run_worker`. |

## 2. ASGI 3.0, RSGI 1.6 & WSGI Protocol Adapters (`_asgi/`, `_rsgi/`, `_wsgi/`, `vertebra-py`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`AsgiRunner`](../../../../../src/py/vertebra/server/_asgi/_runner.py) / [`drive_asgi_batch`](../../../../../src/rs/vertebra-py/src/asgi.rs) / [`AsgiProtocol`](../../../../../src/rs/vertebra-py/src/asgi.rs) | `vertebra.server._asgi._runner`, `vertebra._vertebra` | Finalizes `LitestarPlugin.ensure_finalized`, exposes `dispatch_app` (`__vertebra_fastpath__.dispatch` bypass), and drives unary HTTP batches in Rust via `PyIter_Send` (`0` `asyncio.Task` allocations on single-step handlers; `_SteppedCoroutine` promotion onto `uvloop` on suspension). |
| [`AsgiScope`](../../../../../src/rs/vertebra-py/src/asgi.rs) / `PyEmptyAwaitable` / `PyValueAwaitable` | `vertebra._vertebra` | Lazy 20-slot PyO3 ASGI 3.0 HTTP scope (`immutable_type`, conditional `freelist = 128`, PEP 814 frozen extension dicts on 3.15) and zero-suspension Rust awaitables. |
| [`AsgiReceive`](../../../../../src/py/vertebra/server/_asgi/_queues.py) / [`AsgiSend`](../../../../../src/py/vertebra/server/_asgi/_queues.py) / `AsgiStreamQueuePair` | `vertebra.server._asgi._queues` | Python ASGI helpers for `run_single`, WebSocket upgrades, and bounded full-duplex streaming queues. |
| [`RsgiAdapter`](../../../../../src/py/vertebra/server/_rsgi/_adapter.py) / [`drive_rsgi_batch`](../../../../../src/rs/vertebra-py/src/rsgi/mod.rs) / [`RsgiScope`](../../../../../src/rs/vertebra-py/src/rsgi/scope.rs) / [`RsgiHttpProtocol`](../../../../../src/rs/vertebra-py/src/rsgi/http.rs) | `vertebra.server._rsgi._adapter`, `vertebra._vertebra` | RSGI 1.6 adapter delegating batches to `drive_rsgi_batch` over zero-alloc lazy `RsgiScope` (`OnceLock<Py<Headers>>`, `root_path`, `pyo3::intern!`), frozen `RsgiReadyAwaitable` (`_RsgiReadyAwaitable`, `&self`), and direct-submitting `RsgiHttpProtocol`. |
| [`WsgiAdapter`](../../../../../src/py/vertebra/server/_wsgi/_adapter.py) / [`drive_wsgi_batch`](../../../../../src/rs/vertebra-py/src/wsgi/runner.rs) / [`WsgiEnviron`](../../../../../src/rs/vertebra-py/src/wsgi/environ.rs) | `vertebra.server._wsgi._adapter`, `vertebra._vertebra` | Synchronous PEP 3333 WSGI adapter delegating batches to `drive_wsgi_batch` over zero-copy `WsgiInput::from_snapshot` and lazy PyO3 `WsgiEnviron` mappings (`WsgiFileWrapper`, static `pyo3::intern!` keys, single-call `submit_batch_raw_inner` flush). |

## 3. Platform Lifecycle, SHM Hardening & Process Launch (`platform.py`, `_supervisor.py`)

- **[`CgroupQuota`](../../../../../src/py/vertebra/server/platform.py), [`LameDuckController`](../../../../../src/py/vertebra/server/platform.py), [`ZPagesRegistry`](../../../../../src/py/vertebra/server/platform.py), [`ShmAllocationPolicy`](../../../../../src/py/vertebra/server/platform.py)**: Container cgroup v2 CPU/memory quota detection (`effective_workers`), lame-duck readiness draining (`503 {"status": "lame_duck"}`), `/healthz`, `/statusz`, `/varz`, `/rpcz` diagnostics, anonymous `memfd_create` / `/dev/shm` capacity validator (`min_free_bytes=16 MiB`), and `SCM_RIGHTS` descriptor passing (`send_fd` / `recv_fd`).
- **[`run_supervisor`](../../../../../src/py/vertebra/supervisor/_supervisor.py) & [`find_server_binary`](../../../../../src/py/vertebra/supervisor/_supervisor.py)**: Honors `CARGO_TARGET_DIR` and compares `st_mtime_ns` across candidate target directories (`CARGO_TARGET_DIR`, `target`, `src/rs/target`, `/tmp/vtb-cargo-target`) so `vtb run` always launches the freshest compiled `vertebra-server` binary. Background job lanes live in [Multi-Lane Worker Runner](worker-lanes.md).
