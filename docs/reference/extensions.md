# Framework & Data Extensions

`vertebra.extensions` connects Vertebra's HTTP and IPC runtime to Litestar, Starlette, FastAPI, Flask, Django, SQLSpec, SQLAlchemy, Advanced Alchemy, Oracle APEX, gRPC services, and shared-memory database session brokers. See [Framework Extensions](../usage/extensions.md) for integration guides.

| Extension | Purpose |
| --- | --- |
| **Litestar** | Mount Litestar apps on Vertebra, replace `litestar run`, and compile static routes into the Rust router. |
| **Litestar Ecosystem** | Optional plugins under `vertebra.extensions.litestar.*` for queues, security, autowire, email, MCP, and Vite. |
| **SQLSpec** | Per-engine `sqlspec>=0.74.1` adapters, data dictionaries, event and session stores, statement stacks, and object-store Arrow streaming. |
| **SQLAlchemy & Alchemy** | Sync and async `+vtb` dialects, broker-backed pools, multi-tier cache regions, request-scoped sessions, and file attachment storage. |
| **Django 6.0+** | Database backends for PostgreSQL, MySQL, Oracle, SQLite, and Spanner, plus cache, object storage, and Arrow query support. |
| **Oracle APEX** | Reverse-proxy ORDS and APEX sessions, verify session state, and hydrate TypeScript UI islands. |
| **gRPC & SHM Broker** | Dispatch HTTP/2 gRPC methods and share connection pools and statement caches across workers over shared memory. |

## Litestar Integration

::: vertebra.extensions.litestar.config.VertebraConfig
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.extensions.litestar.plugin.VertebraPlugin
    options:
      heading_level: 3

::: vertebra.extensions.litestar.router.RouteCompiler
    options:
      heading_level: 3

## SQLSpec Integration

::: vertebra.extensions.sqlspec._driver.SyncAdapterConfig
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.extensions.sqlspec._driver.AsyncAdapterConfig
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.extensions.sqlspec.storage.ObjectStoreBackend
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.extensions.sqlspec.plugin.ProxySyncConfig
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.extensions.sqlspec.plugin.ProxyAsyncConfig
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.extensions.sqlspec.provider.SQLSpecSyncProvider
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.extensions.sqlspec.provider.SQLSpecAsyncProvider
    options:
      heading_level: 4
      members: false
      summary: false

## SQLAlchemy & Advanced Alchemy

::: vertebra.extensions.alchemy.storage.ObjectStoreBackend
    options:
      heading_level: 3
      members:
        - save_object
        - save_object_async
        - save_arrow
        - save_arrow_async
        - get_content
        - get_content_async
        - delete_object
        - delete_object_async

::: vertebra.extensions.sqlalchemy.pool.BrokerPool
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.extensions.sqlalchemy.pool.AsyncBrokerPool
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.extensions.sqlalchemy.pool.broker_creator
    options:
      heading_level: 4
      summary: false

::: vertebra.extensions.alchemy.cache.SyncCacheRegion
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.extensions.alchemy.cache.AsyncCacheRegion
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.extensions.django.cache.CacheBackend
    options:
      heading_level: 4
      members: false
      summary: false

## Oracle APEX Gateway & Island Bridge

::: vertebra.extensions.apex.bridge.ApexIslandRegistry
    options:
      heading_level: 3

::: vertebra.extensions.apex.gateway.ApexGateway
    options:
      heading_level: 3

## gRPC Wire Framing & SHM Broker

::: vertebra.extensions.grpc.GrpcServiceRegistry
    options:
      heading_level: 3

::: vertebra.extensions.broker.plugin.BrokerPlugin
    options:
      heading_level: 3
      members: false
      summary: false

::: vertebra.extensions.broker.host.BrokerHost
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.extensions.broker.engines.BrokerEngineDiscovery
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.extensions.broker.transport.SyncSHMBrokerTransport
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.extensions.broker.errors.LeaseQuarantinedError
    options:
      heading_level: 4
      members: false
      summary: false
