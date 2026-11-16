# `vertebra.extensions` — Litestar, SQLSpec, SQLAlchemy, APEX, gRPC & Broker

First-party framework extensions connecting Vertebra's native HTTP/IPC runtime to Litestar (`VertebraPlugin`), Starlette, FastAPI, Flask, Django (`VertebraPlugin` / `VertebraConfig` / `VertebraAppConfig`), SQLSpec (`create_sqlspec_plugin`), SQLAlchemy / Advanced Alchemy (`create_alchemy_plugin`), Oracle APEX (`ApexGateway`), gRPC services (`GrpcServiceRegistry`), and shared-memory database session brokers (`BrokerPlugin`). See [Framework Extensions](../usage/extensions.md) for integration guides.

| Extension | Key Objects | Purpose |
| --- | --- | --- |
| **Litestar** | `VertebraConfig`, `VertebraPlugin`, `RouteCompiler` | Mount Litestar applications on Vertebra, override `litestar run`, and compile static Litestar routes directly into the native router |
| **Litestar Ecosystem** | `queues`, `security`, `autowire`, `email`, `mcp`, `vite` | First-party decoupled plugins under `vertebra.extensions.litestar.*` |
| **SQLSpec** | `create_sqlspec_plugin`, `ProxySyncConfig`, `ProxyAsyncConfig` | Bind SQLSpec database sessions, `StatementStack` pipelines, and shared-memory statement caches to request lifecycles |
| **SQLAlchemy & Alchemy** | `create_alchemy_plugin`, `SyncCacheBackend`, `AsyncCacheBackend`, `ObjectStoreBackend` | Manage SQLAlchemy async/sync engines (`+vtb` dialects), multi-tier `ShmCache`/Redis/Valkey/ObjectStore caching, request-scoped sessions, and transactional file attachments |
| **Django 6.0+** | `VertebraPlugin`, `CacheBackend`, `DjangoStorage`, `ArrowQuerySet` | Native Django `6.0+` database backends (`postgresql`, `mysql`, `oracle`, `sqlite3`, `spanner`), native `CacheBackend`, object storage, and fastpath routing |
| **Oracle APEX** | `ApexIslandRegistry`, `ApexGateway` | Reverse-proxy Oracle ORDS/APEX sessions, verify ECID/session state, and hydrate TypeScript UI islands |
| **gRPC & SHM Broker** | `GrpcServiceRegistry`, `BrokerHost`, `SyncSHMBrokerTransport`, `BrokerPlugin`, `BrokerPool`, `LeaseQuarantinedError` | Dispatch HTTP/2 gRPC methods and share cross-worker SHM broker connection pools and statement caches |

## Litestar Integration (`vertebra.extensions.litestar`)

::: vertebra.extensions.litestar.config.VertebraConfig
    options:
      heading_level: 4
      members: false

::: vertebra.extensions.litestar.plugin.VertebraPlugin
    options:
      heading_level: 3

::: vertebra.extensions.litestar.router.RouteCompiler
    options:
      heading_level: 3

## SQLSpec Integration (`vertebra.extensions.sqlspec`)

::: vertebra.extensions.sqlspec.plugin.ProxySyncConfig
    options:
      heading_level: 4
      members: false

::: vertebra.extensions.sqlspec.plugin.ProxyAsyncConfig
    options:
      heading_level: 4
      members: false

::: vertebra.extensions.sqlspec.provider.SQLSpecSyncProvider
    options:
      heading_level: 4
      members: false

::: vertebra.extensions.sqlspec.provider.SQLSpecAsyncProvider
    options:
      heading_level: 4
      members: false

## SQLAlchemy & Advanced Alchemy (`vertebra.extensions.alchemy`, `vertebra.extensions.sqlalchemy`)

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

::: vertebra.extensions.alchemy.cache.SyncCacheBackend
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.extensions.alchemy.cache.AsyncCacheBackend
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.extensions.sqlalchemy.cache.CompiledCache
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.extensions.sqlalchemy.cache.ResultCache
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.extensions.django.cache.CacheBackend
    options:
      heading_level: 4
      members: false
      summary: false

## Oracle APEX Gateway & Island Bridge (`vertebra.extensions.apex`)

::: vertebra.extensions.apex.bridge.ApexIslandRegistry
    options:
      heading_level: 3

::: vertebra.extensions.apex.gateway.ApexGateway
    options:
      heading_level: 3

## gRPC Wire Framing & SHM Broker (`vertebra.extensions.grpc`, `vertebra.extensions.broker`)

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
