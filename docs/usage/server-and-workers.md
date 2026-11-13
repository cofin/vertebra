# Server & Workers

`vertebra.server` connects the Rust HTTP server (`vertebra-http`) and shared-memory transport (`vertebra-core`) to Python handlers, background queue workers, and ASGI, RSGI, and WSGI applications. See [`vertebra.server` API Reference](../reference/server.md) for class and method signatures.

## Runtime Topologies (`embedded` vs. `shm`)

Vertebra supports two execution topologies selectable via `--topology` (or `VERTEBRA_TOPOLOGY`):

| Topology | Flag | When Selected | How Requests Cross Rust & Python |
| --- | --- | --- | --- |
| **Embedded** | `--topology embedded` | Default when `--workers 1` or on free-threaded Python `3.14t`/`3.15t` | Zero-IPC in-process `EmbeddedTransport` inside a single OS process |
| **Shared Memory** | `--topology shm` | Default when `--workers > 1` on standard GIL-enabled Python | Lock-free POSIX shared-memory SPSC ring (`ShmTransport`) with futex/ulock wakeups |

## Worker Lanes & Task Runners (`vertebra.server.lanes`)

To keep CPU-heavy or blocking I/O work from slowing down fast routes, Vertebra separates work into isolated execution lanes:

| Lane | Purpose | How It Runs |
| --- | --- | --- |
| `fast` | Synchronous, non-blocking handlers (`msgspec` models, cached lookups) | Runs directly on the worker poll thread |
| `async` | `async def` handlers, SSE streams, and WebSockets | Runs on the `EventLoopPool` (`resolve_loop_shards`) |
| `cpu` | Compute-heavy work and background CPU tasks | Runs on a bounded CPU worker pool (`LaneConfig`) |
| `blocking` | Synchronous file/network I/O and WSGI apps | Runs on a dedicated blocking `ThreadPoolExecutor` |

For background queue workers (`vtb workers run`, or `vtb run --worker-only` when paired with health probes), `WorkerRunner` (`vertebra.server.lanes`) calls `WorkerRunner.resolve_queue_service(app)` to resolve a `QueueService` directly or from `app.state["vertebra_queue_service"]` / `app.state.vertebra_queue_service` inside app lifespan and runs configured `cpu`, `io`, and `agent` lanes until shutdown:

```bash
vtb workers run app:queue --queues io,cpu,agent --concurrency 8
vtb run app:queue --worker-only --host 0.0.0.0 --port 8080
```

When `--worker-only` is set on `vtb run`, Vertebra skips launching the Rust HTTP ingress server and serves `HealthProbeServer` (`/healthz`, `/readyz`, `/startupz`) alongside `WorkerRunner`. When an ASGI mount lifespan or RSGI `__rsgi_init__` hook fails at worker startup, `Worker` raises `WorkerStartupError` and exits with code `78` (`EXIT_STARTUP_FAILURE`) instead of serving degraded mounts.

## Multi-Protocol Adapters (ASGI, RSGI & WSGI)

In addition to native `App` routes and controllers returning `(status, headers, AsyncIterable | Iterable)` streams, `vertebra.server` runs standard Python web applications directly:

- **ASGI 3.0 (`AsgiRunner`, `AsgiLifespan`, `AsgiProtocol`)**: Runs Litestar, FastAPI, or Starlette apps with startup/shutdown lifespan state machines, Server-Sent Events (`SseEvent`, `SseResponse`), WebSockets (`WebSocketSession`), progressive HTTP/1.1 & HTTP/2 streaming over `VRSF` frames (`submit_stream_frame` / `max_stream_frame_payload`), and file responses (`ZeroCopySendResponse`, `PathSendResponse`). Buffered `AsgiRunner.__call__` remains collecting by contract while live HTTP mounts flush response headers on the first non-final chunk (mid-stream exceptions emit `STREAM_FLAG_ERROR` to terminate the body early, and empty byte chunks are skipped).
- **RSGI 1.6 (`RsgiAdapter`, `detect_interface`)**: Runs RSGI HTTP (including progressive `RsgiHttpProtocol.response_stream` streaming) and WebSocket applications directly over batched Rust-to-Python calls.
- **WSGI (`WsgiAdapter`, `FlaskWsgiApp`, `DjangoWsgiApp`)**: Runs synchronous PEP 3333 applications (`Flask`, `Django`) inside the `blocking` thread pool.

```python
from vertebra.server._asgi import AsgiLifespan, AsgiRunner
from vertebra.server._wsgi import WsgiAdapter

asgi_runner = AsgiRunner(app=my_asgi_app)
wsgi_adapter = WsgiAdapter(app=my_wsgi_app)
```

## Diagnostic Z-Pages & Graceful Drain (`vertebra.server.platform`)

`ZPagesRegistry` and `LameDuckController` provide built-in operational endpoints (`/healthz`, `/statusz`, `/varz`, `/rpcz`, `/brokerz`) and graceful connection draining during rollouts:

```python
from vertebra.server.platform import LameDuckController, ZPagesRegistry

lame_duck = LameDuckController(drain_grace_seconds=10.0)
zpages = ZPagesRegistry(service_name="orders-api", lame_duck=lame_duck)
zpages.set_var("version", "1.0.0")
zpages.record_rpc("GET /orders", ok=True)

status, body = zpages.handle_zpage("/healthz")
```

When `lame_duck.enter_lame_duck()` is called on `SIGTERM`, `/healthz` immediately returns `503` so load balancers stop sending new traffic while in-flight requests finish draining.

## ASGI Mount Timeouts

When an ASGI mount is served through the buffered batch path, each request waits up to 30 seconds by default. Set `VERTEBRA_ASGI_BATCH_TIMEOUT` to another number of seconds, or to `0` or `none` to disable the limit. A request that exceeds it receives `504 Gateway Timeout` and its task is cancelled. Streaming responses use a separate path and are not affected.

## Free-Threaded Python & GIL Auditing (`3.14t` / `3.15t`)

When compiled for free-threaded Python (`Py_GIL_DISABLED`), `vertebra._vertebra.FREE_THREADED` is `True`. Under `--topology embedded`, multiple worker threads run Python handlers in parallel inside a single process.

Use `GilAuditReport` (`vertebra.server._gil_audit`) or set `VERTEBRA_STRICT_FREE_THREADING=1` to verify that imported C extensions do not re-enable the GIL at runtime:

```python
from vertebra.server._gil_audit import GilAuditReport, is_free_threaded

report = GilAuditReport.capture()
assert isinstance(is_free_threaded(), bool)
```
