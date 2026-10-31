# `vertebra.server` — Worker Runtime, Execution Lanes & Protocol Adapters

Multi-protocol worker lifecycle (`Worker`), isolated execution lanes (`LaneConfig`, `WorkerRunner`), sharded `asyncio` event loops (`EventLoopPool`), ASGI/RSGI/WSGI bridges, and container cgroup platform introspection.

## Worker Lifecycle & Batch Dispatch

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

::: vertebra.server.platform.ShmAllocationPolicy
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
