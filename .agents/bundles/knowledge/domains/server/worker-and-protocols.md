---
name: domain-server-worker-and-protocols
description: Canonical domain types for Vertebra's Python worker runtime, vectorized batch dispatch plan, ASGI/WSGI adapters, and process supervision.
sources:
  - src/py/vertebra/server/_native.py
  - src/py/vertebra/server/_asgi/_runner.py
  - src/py/vertebra/server/_asgi/_queues.py
  - src/py/vertebra/server/_wsgi/_adapter.py
  - src/py/vertebra/supervisor/_supervisor.py
  - src/py/vertebra/supervisor/_worker.py
  - src/rs/vertebra-py/src/asgi_scope.rs
  - src/rs/vertebra-py/src/wsgi_environ.rs
---

# Python Server, Worker Loop & Protocol Adapters Domain

[`vertebra.server`](../../../../../src/py/vertebra/server/__init__.py) and [`vertebra.supervisor`](../../../../../src/py/vertebra/supervisor/__init__.py) host the embedded and multi-process Python worker loops, vectorized batch partitioning, task-free ASGI 3.0 runner, synchronous PEP 3333 WSGI adapter, and process supervision utilities.

## 1. Worker Loop & Vectorized Batch Dispatch (`_native.py`, `supervisor/_worker.py`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`AsyncRuntime`](../../../../../src/py/vertebra/server/_native.py) | `vertebra.server._native` | Dedicated background `asyncio` event loop thread supporting single-wakeup vectorized batch execution (`spawn_batch`, `schedule_batch`, `spawn`, `schedule`). |
| [`BatchDispatchPlan`](../../../../../src/py/vertebra/server/_native.py) | `vertebra.server._native` | Partitions an incoming `RequestBatch` via `_partition_batch()` into `sync_native`, `async_native`, `mounts`, and `unmatched`. |
| [`run_worker`](../../../../../src/py/vertebra/server/_native.py) | `vertebra.server._native` | Unified batch runner loop supporting both in-process `BatchBridge` (`get_batch` / `submit_response`) and multi-process SHM (`_shm_worker_loop`, `try_get_batch`). |
| [`WorkerProcess`](../../../../../src/py/vertebra/supervisor/_worker.py) | `vertebra.supervisor._worker` | Subprocess worker entry point attaching to inherited SHM file descriptors and invoking `run_worker`. |

## 2. ASGI 3.0 & WSGI Protocol Adapters (`_asgi/`, `_wsgi/`, `asgi_scope.rs`, `wsgi_environ.rs`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`AsgiRunner`](../../../../../src/py/vertebra/server/_asgi/_runner.py) | `vertebra.server._asgi._runner` | Executes ASGI 3.0 applications over single requests or batches (`__call__`, `spawn_batch`) using task-free `_drive_coro` + `_CoroResumer` stepping by default. |
| [`AsgiScope`](../../../../../src/rs/vertebra-py/src/asgi_scope.rs) / [`AsgiScopeIterator`](../../../../../src/rs/vertebra-py/src/asgi_scope.rs) | `vertebra._vertebra` | Lazy PyO3 ASGI 3.0 HTTP scope mapping backed by Rust request fields with interned keys and copy-on-write overrides. |
| [`AsgiReceive`](../../../../../src/py/vertebra/server/_asgi/_queues.py) / [`AsgiSend`](../../../../../src/py/vertebra/server/_asgi/_queues.py) | `vertebra.server._asgi._queues` | Zero-allocation ASGI receive/send callables using `_CompletedAwaitable` to complete standard `http.request`, `http.response.start`, and `http.response.body` exchanges in 0 suspension steps. |
| [`WsgiAdapter`](../../../../../src/py/vertebra/server/_wsgi/_adapter.py) | `vertebra.server._wsgi._adapter` | Synchronous PEP 3333 WSGI runner executed across the shared `vertebra-wsgi` `ThreadPoolExecutor`. |
| [`WsgiEnviron`](../../../../../src/rs/vertebra-py/src/wsgi_environ.rs) / [`WsgiEnvironIterator`](../../../../../src/rs/vertebra-py/src/wsgi_environ.rs) | `vertebra._vertebra` | Lazy PyO3 PEP 3333 `environ` mapping built directly in Rust. |

## 3. Python Process Supervision (`supervisor/_supervisor.py`)

- **[`WorkerSupervisor`](../../../../../src/py/vertebra/supervisor/_supervisor.py)**: Multi-worker process manager handling SHM ring creation, subprocess spawning, health checks, exponential backoff, and crash-rate circuit breaking.
