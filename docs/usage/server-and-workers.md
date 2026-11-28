# Server & Workers

Vertebra connects its Rust HTTP server and transport layer to Python handlers, background queue workers, and ASGI, RSGI, and WSGI applications. See the [Server API Reference](../reference/server.md) for class and method signatures.

## Runtime Topologies

Choose between two execution topologies with `--topology` or `VERTEBRA_TOPOLOGY`:

| Topology | Flag | When Selected | Request Path |
| --- | --- | --- | --- |
| **Embedded** | `--topology embedded` | Default with `--workers 1` or on free-threaded Python | In-process calls inside a single OS process |
| **Shared Memory** | `--topology shm` | Default with `--workers > 1` on GIL-enabled Python | Lock-free POSIX shared-memory rings with futex/ulock wakeups |

## Worker Lanes & Task Runners

To keep CPU-heavy or blocking I/O work from stalling fast routes, Vertebra isolates execution into four lanes:

| Lane | Purpose | How It Runs |
| --- | --- | --- |
| `fast` | Sync, non-blocking handlers such as cached lookups | Directly on the worker poll thread |
| `async` | Async handlers, SSE streams, and WebSockets | Managed event-loop pool |
| `cpu` | Compute-heavy handlers and background CPU tasks | Bounded CPU worker pool |
| `blocking` | Sync file or network I/O and WSGI apps | Dedicated thread pool |

For background queue workers, pass a `QueueService` or an app that exposes one on `app.state.vertebra_queue_service`. Workers process configured io, cpu, and agent lanes until shutdown:

```bash
vtb workers run app:queue --queues io,cpu,agent --concurrency 8
vtb run app:queue --worker-only --host 0.0.0.0 --port 8080
```

With `--worker-only`, Vertebra skips the HTTP ingress server and serves health probes (`/healthz`, `/readyz`, `/startupz`) alongside the queue worker. If an ASGI lifespan or RSGI init hook fails at startup, the worker exits with code 78 instead of serving broken mounts.

## Multi-Protocol Adapters

Alongside native `App` routes and streaming iterables, the server runs standard Python web applications directly:

- **ASGI 3.0**: Runs Litestar, FastAPI, and Starlette apps with startup/shutdown lifespans, SSE, WebSockets, file responses, and progressive HTTP/1.1 and HTTP/2 streaming. Live mounts flush response headers on the first non-final chunk and terminate cleanly if a stream raises mid-response.
- **RSGI 1.6**: Runs RSGI HTTP (including streaming responses) and WebSocket apps directly.
- **WSGI**: Runs synchronous Flask and Django applications inside the blocking thread pool.

```python
from vertebra.server._asgi import AsgiLifespan, AsgiRunner
from vertebra.server._wsgi import WsgiAdapter

asgi_runner = AsgiRunner(app=my_asgi_app)
wsgi_adapter = WsgiAdapter(app=my_wsgi_app)
```

## Diagnostic Z-Pages & Graceful Drain

`ZPagesRegistry` and `LameDuckController` expose operational endpoints (`/healthz`, `/statusz`, `/varz`, `/rpcz`, `/brokerz`, `/taskz`) and coordinate graceful connection draining during rollouts:

```python
from vertebra.server.platform import LameDuckController, ZPagesRegistry

lame_duck = LameDuckController(drain_grace_seconds=10.0)
zpages = ZPagesRegistry(service_name="orders-api", lame_duck=lame_duck)
zpages.set_var("version", "1.0.0")
zpages.record_rpc("GET /orders", ok=True)

status, body = zpages.handle_zpage("/healthz")
```

Calling `lame_duck.enter_lame_duck()` on SIGTERM makes `/healthz` return 503 immediately so load balancers stop sending new traffic while in-flight requests drain.

### Enabling HTTP Z-Pages on vtb run

HTTP Z-Pages are disabled by default so user routes stay untouched. Enable them with `--zpages` or `VERTEBRA_ZPAGES_ENABLED=1`:

```bash
vtb run app:app --zpages
vtb run app:app --host 0.0.0.0 --zpages --zpages-token "<token>"
```

- **Configuration**: Enable with `--zpages` or `VERTEBRA_ZPAGES_ENABLED=1`. Set a Bearer token with `--zpages-token` or `VERTEBRA_ZPAGES_TOKEN`.
- **Paths and methods**: Serves `/taskz`, `/statusz`, `/varz`, `/rpcz`, `/brokerz`, and `/debug/zpages/{tasks,broker}` on GET and HEAD. Other methods return 405 Method Not Allowed. Health probes stay on `/healthz` and `/readyz`.
- **Authorization**: When a token is set, requests must send an `Authorization: Bearer <token>` header. Without a token, loopback binds (127.0.0.1, ::1) are allowed and non-loopback binds fail closed with 401 Unauthorized.
- **Topologies**: Embedded mode shares one registry across worker threads. Shared-memory mode accepts `?worker=<id>` to inspect a single worker or aggregates all healthy workers.

## Event Loops & Task Diagnostics

Pick the event-loop backend with `--loop` or `VERTEBRA_LOOP` (auto, uvloop, winloop, rloop, zuvloop, or asyncio). Eager task execution is enabled on every backend.

Register an `EventLoopPool` with `ZPagesRegistry` to list active tasks on `/taskz`:

```python
from vertebra.server._loops import EventLoopPool
from vertebra.server.platform import ZPagesRegistry

pool = EventLoopPool(size=2)
zpages = ZPagesRegistry(service_name="orders-api")
zpages.register_loop_pool("http", pool)

status, body = zpages.handle_zpage("/taskz")
```

Each task entry reports its name, shard, coroutine, and call graph (using `asyncio.capture_call_graph` on Python 3.14+ and stack frames on Python 3.12–3.13). Queue workers register their loop pool automatically, and `vtb run --worker-only` serves `/taskz` on its probe port.

Set `VERTEBRA_LOOP_TELEMETRY=1` to sample loop scheduling lag and active task counts. Configure the sampling period in seconds with `VERTEBRA_LOOP_TELEMETRY_INTERVAL` (default `1.0`). Samples appear under `loop_telemetry` in `/varz` and export to OpenTelemetry when `[opentelemetry]` is installed. See the [telemetry reference](../reference/telemetry.md).

## ASGI Mount Timeouts

Buffered ASGI mounts time out after 30 seconds by default. Set `VERTEBRA_ASGI_BATCH_TIMEOUT` to another number of seconds, or to 0 or none to disable the limit. Timed-out requests return 504 Gateway Timeout and cancel the handler task. Streaming responses use a separate path and are unaffected.

## Free-Threaded Python & GIL Auditing

On free-threaded Python builds, `--topology embedded` runs multiple worker threads in parallel inside a single process.

Use `GilAuditReport` or set `VERTEBRA_STRICT_FREE_THREADING=1` to verify that imported C extensions do not re-enable the GIL at runtime:

```python
from vertebra.server._gil_audit import GilAuditReport, is_free_threaded

report = GilAuditReport.capture()
print(f"Free-threaded interpreter: {is_free_threaded()}")
```
