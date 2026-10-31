# Server & Workers

`vertebra.server` bridges the Rust HTTP ingress (`vertebra-http`) and shared-memory transport (`vertebra-core`) to Python execution across native handlers, ASGI 3.0, RSGI 1.6, and PEP 3333 WSGI applications.

## Worker Lanes

To prevent CPU-heavy or blocking I/O handlers from starving low-latency endpoints, Vertebra classifies handlers into four isolated execution lanes (`vertebra.server.lanes`):

| Lane | Purpose | Execution Mechanism |
| --- | --- | --- |
| `fast` | Synchronous, non-blocking handlers (`msgspec` DTOs, cached lookups) | Direct vectorized dispatch on the worker poll thread |
| `async` | Coroutine (`async def`) handlers, SSE, and WebSockets | Sharded `asyncio` `EventLoopPool` (`resolve_loop_shards`) |
| `cpu` | Compute-bound transformations | Bounded CPU worker pool (`LaneConfig`) |
| `blocking` | Synchronous file/network I/O and PEP 3333 WSGI | Dedicated blocking `ThreadPoolExecutor` |

## Multi-Protocol Adapters

In addition to native `App` handlers, `vertebra.server` runs standard Python web protocols without an external proxy:

- **ASGI 3.0 (`AsgiRunner`, `AsgiLifespan`)**: Drives Litestar, FastAPI, or Starlette applications over batched PyO3 `drive_asgi_batch` bridges, including WebSocket upgrade and SSE streaming.
- **RSGI 1.6 (`RsgiAdapter`)**: High-throughput HTTP and WebSocket protocol bridge via `drive_rsgi_batch`.
- **WSGI (`WsgiAdapter`)**: Runs synchronous PEP 3333 applications (`Flask`, `Django`) inside the `blocking` lane via `drive_wsgi_batch`.

## Free-Threaded Python (`3.14t` / `3.15t`)

When compiled against free-threaded CPython (`Py_GIL_DISABLED`), `vertebra._vertebra.FREE_THREADED` is `True`. Under `--topology embedded`, multiple worker threads execute Python handlers concurrently in a single address space with zero cross-process IPC serialization. Use `GilAuditReport` (`vertebra.server._gil_audit`) to verify that loaded C extensions do not re-enable the GIL at runtime.
