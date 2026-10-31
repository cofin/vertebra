# `vertebra.extensions` — Litestar, SQLSpec, SQLAlchemy, APEX, gRPC & Broker

First-party framework extensions connecting Vertebra's Rust HTTP/IPC runtime to Litestar (`LitestarPlugin`), SQLSpec (`SQLSpecPlugin`), SQLAlchemy (`AlchemyPlugin`), Oracle APEX (`ApexGateway`), gRPC services (`GrpcServiceRegistry`), and shared-memory database session brokers (`BrokerPlugin`). See [SQLSpec, SQLAlchemy & Extensions](../usage/extensions.md) for integration examples.

| Extension | Classes | Purpose |
| --- | --- | --- |
| **Litestar** | `LitestarConfig`, `LitestarPlugin`, `LitestarRouteCompiler` | Mount Litestar applications on Vertebra and compile static Litestar routes directly into the Rust radix router |
| **SQLSpec** | `SQLSpecConfig`, `SQLSpecPlugin` | Bind SQLSpec database sessions, `StatementStack` pipelines, and shared-memory statement caches to request lifecycles |
| **SQLAlchemy & Alembic** | `AlchemyPlugin`, `AlembicRunner` | Manage SQLAlchemy async/sync engines (`+vtb` dialects), request-scoped sessions, and programmatic Alembic migrations |
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

::: vertebra.extensions.litestar.router.LitestarRouteCompiler
    options:
      heading_level: 3

## SQLSpec Integration (`vertebra.extensions.sqlspec`)

::: vertebra.extensions.sqlspec.plugin.SQLSpecConfig
    options:
      heading_level: 4
      members: false

::: vertebra.extensions.sqlspec.plugin.SQLSpecPlugin
    options:
      heading_level: 3

## SQLAlchemy & Alembic (`vertebra.extensions.alchemy`, `vertebra.extensions.sqlalchemy`)

::: vertebra.extensions.alchemy.plugin.AlchemyPlugin
    options:
      heading_level: 3

::: vertebra.extensions.sqlalchemy.alembic.AlembicRunner
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
