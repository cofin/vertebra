# `vertebra.extensions` — Litestar, SQLSpec, SQLAlchemy, APEX, gRPC & Broker

First-party framework extensions connecting Vertebra's Rust HTTP/IPC runtime to Litestar, SQLSpec, SQLAlchemy, Oracle APEX, gRPC services, and shared-memory database session brokers.

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
