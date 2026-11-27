# Server Runtime, Execution Lanes & Protocol Adapters

`vertebra.server` manages the worker lifecycle, pre-compiled batch dispatch, isolated execution lanes, sharded `asyncio` event loops, ASGI/RSGI/WSGI adapters, and container cgroup introspection. See the [Server & Workers Usage Guide](../usage/server-and-workers.md) for topology and tuning guidance.

| Subsystem | Responsibility |
| --- | --- |
| **Worker & Batch Dispatch** | Poll request batches from `vertebra-core`, partition them by handler lane, and submit responses. |
| **Lanes & Event Loops** | Isolate `io`, `cpu`, and `agent` work and shard `asyncio` loops across worker threads. |
| **Protocol Runners** | Run ASGI 3.0, RSGI 1.6, and PEP 3333 WSGI applications such as Litestar, FastAPI, Flask, and Django. |
| **Platform & GIL Audit** | Read cgroup v2 CPU and shared-memory quotas, serve `/healthz` and `/statusz`, drain traffic on `SIGTERM`, and audit GIL state on free-threaded Python. |

## Worker Lifecycle & Batch Dispatch

`Worker` drives the per-process poll loop and compiles route handlers into `HandlerSpec` entries partitioned per batch via `BatchDispatchPlan`.

::: vertebra.server.worker.Worker
    options:
      heading_level: 3

::: vertebra.server._dispatch.HandlerSpec
    options:
      heading_level: 4
      members: false

::: vertebra.server._dispatch.BatchDispatchPlan
    options:
      heading_level: 4
      members: false

## Execution Lanes & Event Loop Shards

::: vertebra.server.lanes.LaneConfig
    options:
      heading_level: 4
      members: false

::: vertebra.server.lanes.LaneMetrics
    options:
      heading_level: 4
      members: false

::: vertebra.server._loops.EventLoopPool
    options:
      heading_level: 3

## Protocol Runners

::: vertebra.server._asgi._runner.AsgiRunner
    options:
      heading_level: 3

::: vertebra.server._asgi.AsgiLifespan
    options:
      heading_level: 4
      members: false

::: vertebra.server._asgi.SseResponse
    options:
      heading_level: 4
      members: false

::: vertebra.server._asgi.WebSocketSession
    options:
      heading_level: 4
      members: false

::: vertebra.server._rsgi._adapter.RsgiAdapter
    options:
      heading_level: 3

::: vertebra.server._wsgi._adapter.WsgiAdapter
    options:
      heading_level: 3

::: vertebra.server._wsgi.FlaskWsgiApp
    options:
      heading_level: 4
      members: false

::: vertebra.server._wsgi.DjangoWsgiApp
    options:
      heading_level: 4
      members: false

## Cgroups, Z-Pages, Graceful Drain & GIL Audit

::: vertebra.server.platform.CgroupQuota
    options:
      heading_level: 3

::: vertebra.server.platform.SHMAllocationPolicy
    options:
      heading_level: 3

::: vertebra.server.platform.LameDuckController
    options:
      heading_level: 4
      members: false

::: vertebra.server.platform.ZPagesRegistry
    options:
      heading_level: 4
      members: false

::: vertebra.server._gil_audit.GilAuditReport
    options:
      heading_level: 4
      members: false
