# Framework Extensions

`vertebra.extensions` integrates Vertebra's Rust runtime, shared-memory transport, and frontend bundler with Litestar, SQLSpec, Advanced Alchemy, Oracle APEX, gRPC, and signed cookie/store sessions.

## Litestar Integration (`vertebra.extensions.litestar`)

Use `VertebraLitestarPlugin` to run a Litestar application on Vertebra's Rust HTTP ingress while coordinating Litestar lifespan hooks (`enter_litestar_server_lifespan`) and Agent/MCP plugins:

```python
from litestar import Litestar, get
from vertebra.extensions.litestar import VertebraLitestarPlugin


@get("/hello")
async def hello() -> dict[str, str]:
    return {"hello": "litestar"}


app = Litestar(route_handlers=[hello], plugins=[VertebraLitestarPlugin()])
```

## SQLSpec & Advanced Alchemy (`vertebra.extensions.sqlspec`, `vertebra.extensions.alchemy`)

Attach `SQLSpecExtension` or `AlchemyExtension` to manage database session lifecycles, request-scoped transactions, and OpenTelemetry SQL trace propagation automatically across routes.

## Oracle APEX Island Bridge (`vertebra.extensions.apex`)

`ApexExtension` and `@vertebra/client/apex` (`ApexRegion`, `ApexOfflineOutbox`) allow incremental modernization of Oracle APEX pages by mounting reactive Vertebra islands and offline-capable mutation queues inside existing APEX regions.

## gRPC Wire Bridge (`vertebra.extensions.grpc`)

`GrpcBridge` maps length-prefixed gRPC HTTP/2 frames from `vertebra-http` directly to typed Python service methods (`GrpcStatus`, unary and server-streaming RPCs) without requiring a separate `grpcio` C++ server port.

## Sessions & Autodiscovery (`vertebra.extensions.sessions`, `vertebra.extensions.autodiscovery`)

- **`SessionExtension`**: Provides signed cookie and server-side session storage (`SessionConfig`).
- **`AutodiscoveryConfig`**: Scans configured domain packages at startup to register controllers, routers, task queues, and MCP tools automatically.
