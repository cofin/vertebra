# Framework Extensions

`vertebra.extensions` integrates Vertebra's Rust runtime, shared-memory transport, and frontend bundler with Litestar, SQLSpec, SQLAlchemy, Oracle APEX, gRPC, and shared-memory database session brokers.

## Litestar Integration (`vertebra.extensions.litestar`)

Use `LitestarPlugin` and `LitestarConfig` to run a Litestar application on Vertebra's Rust HTTP ingress while coordinating Litestar lifespan hooks (`enter_litestar_server_lifespan`) and Agent/MCP plugins:

```python
from litestar import Litestar, get
from vertebra.extensions.litestar import LitestarConfig, LitestarPlugin


@get("/hello")
async def hello() -> dict[str, str]:
    return {"hello": "litestar"}


app = Litestar(
    route_handlers=[hello],
    plugins=[LitestarPlugin(LitestarConfig())],
)
```

## SQLSpec & SQLAlchemy (`vertebra.extensions.sqlspec`, `vertebra.extensions.alchemy`)

Attach `SQLSpecPlugin` (`SQLSpecConfig`) or `AlchemyPlugin` (`AlembicRunner`) to manage database session lifecycles, request-scoped transactions, and OpenTelemetry SQL trace propagation automatically across routes.

## Oracle APEX Gateway & Island Bridge (`vertebra.extensions.apex`)

`ApexGateway`, `ApexIslandRegistry`, and `@vertebra/client/apex` (`ApexRegion`, `ApexOfflineOutbox`) allow incremental modernization of Oracle APEX pages by mounting reactive Vertebra islands and offline-capable mutation queues inside existing APEX regions.

## gRPC Wire Framing & SHM Broker (`vertebra.extensions.grpc`, `vertebra.extensions.broker`)

- **`GrpcServiceRegistry`**: Maps length-prefixed gRPC HTTP/2 frames from `vertebra-http` directly to typed Python service methods (`GrpcStatus`, unary and server-streaming RPCs) without requiring a separate `grpcio` C++ server port.
- **`BrokerPlugin`**: Coordinates shared-memory database connection leasing and statement caching across multi-worker processes.
