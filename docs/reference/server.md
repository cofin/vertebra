# `vertebra.server` — Worker Runtime, Execution Lanes & Protocol Adapters

Multi-protocol worker lifecycle (`Worker`), pre-compiled batch dispatch (`HandlerSpec`, `BatchDispatchPlan`), isolated execution lanes (`LaneConfig`, `LaneMetrics`), sharded `asyncio` event loops (`EventLoopPool`), ASGI/RSGI/WSGI bridges, and container cgroup platform introspection. See the [Server & Workers Usage Guide](../usage/server-and-workers.md) for topology and tuning guidance.

| Subsystem | Key Classes | Responsibility |
| --- | --- | --- |
| **Worker & Batch Dispatch** | `Worker`, `HandlerSpec`, `BatchDispatchPlan` | Poll request batches from `vertebra-core` transport, partition by execution mode, and submit responses |
| **Lanes & Event Loops** | `LaneConfig`, `LaneMetrics`, `EventLoopPool` | Isolate `io`, `cpu`, and `agent` concurrency and shard `asyncio` loops across worker threads |
| **Protocol Runners** | `AsgiRunner`, `RsgiAdapter`, `WsgiAdapter` | Run ASGI 3.0, RSGI 1.6, and PEP 3333 WSGI applications (`Litestar`, `FastAPI`, `Flask`, `Django`) |
| **Platform & GIL Audit** | `CgroupQuota`, `SHMAllocationPolicy`, `LameDuckController`, `ZPagesRegistry`, `GilAuditReport` | Inspect cgroup v2 CPU/SHM limits, serve `/healthz` and `/statusz`, drain traffic on `SIGTERM`, and audit 3.14t GIL state |

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

## Protocol Runners (ASGI 3.0, RSGI 1.6 & PEP 3333 WSGI)

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

## Container Cgroups, Z-Pages, Graceful Drain & Free-Threaded Audit

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
