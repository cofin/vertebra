# `vertebra.extensions` — Litestar, SQLSpec, SQLAlchemy, APEX, gRPC & Broker

First-party framework extensions connecting Vertebra's native HTTP/IPC runtime to Litestar (`LitestarPlugin`), SQLSpec (`create_sqlspec_plugin`), SQLAlchemy / Advanced Alchemy (`create_alchemy_plugin`), Oracle APEX (`ApexGateway`), gRPC services (`GrpcServiceRegistry`), and shared-memory database session brokers (`BrokerPlugin`). See [Framework Extensions](../usage/extensions.md) for integration guides.

| Extension | Key Objects | Purpose |
| --- | --- | --- |
| **Litestar** | `LitestarConfig`, `LitestarPlugin`, `RouteCompiler` | Mount Litestar applications on Vertebra and compile static Litestar routes directly into the native router |
| **Litestar Ecosystem** | `queues`, `security`, `autowire`, `email`, `mcp`, `vite` | First-party decoupled plugins under `vertebra.extensions.litestar.*` |
| **SQLSpec** | `create_sqlspec_plugin`, `ProxySyncConfig`, `ProxyAsyncConfig` | Bind SQLSpec database sessions, `StatementStack` pipelines, and shared-memory statement caches to request lifecycles |
| **SQLAlchemy & Alchemy** | `create_alchemy_plugin`, `ObjectStoreBackend` | Manage SQLAlchemy async/sync engines (`+vtb` dialects), request-scoped sessions, and transactional file attachments |
| **Oracle APEX** | `ApexIslandRegistry`, `ApexGateway` | Reverse-proxy Oracle ORDS/APEX sessions, verify ECID/session state, and hydrate TypeScript UI islands |
| **gRPC & SHM Broker** | `GrpcServiceRegistry`, `BrokerPlugin` | Dispatch HTTP/2 gRPC methods and attach cross-worker `DatabaseBroker` connection/statement leasing |

## Litestar Integration (`vertebra.extensions.litestar`)

::: vertebra.extensions.litestar.config.LitestarConfig
    options:
      heading_level: 4
      members: false

::: vertebra.extensions.litestar.plugin.LitestarPlugin
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

::: vertebra.extensions.sqlspec.provider.SQLSpecProvider
    options:
      heading_level: 3

## SQLAlchemy & Advanced Alchemy (`vertebra.extensions.alchemy`, `vertebra.extensions.sqlalchemy`)

::: vertebra.extensions.alchemy.storage.ObjectStoreBackend
    options:
      heading_level: 3

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
