# Routing & Handlers

Vertebra routes incoming HTTP requests through its Rust router (`vertebra-http`) directly to typed Python functions, `Controller` classes, modular `Router` groups, or RPC `Procedure` definitions.

## Route Decorators, `Controller`, and `Router`

Register routes directly on `App` with `@app.get`, `@app.post`, `@app.put`, `@app.patch`, and `@app.delete`, or group related endpoints on a `Controller` and `Router`:

```python
from vertebra import App, Controller, Provide, Router, get, post


class UserService:
    def get_name(self, user_id: int) -> str:
        return f"user-{user_id}"


class UserController(Controller):
    path = "/users"
    dependencies = {"users": Provide(UserService, scope="singleton")}
    tags = ("users",)

    @get("/{user_id:int}")
    def get_user(self, user_id: int, users: UserService) -> dict[str, int | str]:
        return {"id": user_id, "username": users.get_name(user_id)}

    @post("/", status_code=201)
    def create_user(self, payload: dict[str, str]) -> dict[str, str]:
        return {"status": "created", **payload}


api_router = Router(prefix="/api/v1", route_handlers=[UserController])
app = App(route_handlers=[api_router])
```

You can also instantiate explicit `Route` descriptors when building route tables programmatically:

```python
from vertebra import Route


def ping() -> dict[str, str]:
    return {"ping": "pong"}


ping_route = Route(path="/ping", fn=ping, methods=("GET",))
```

## Path, Query, Header & Cookie Parameters

Path parameters are extracted from `{name}` or typed `{name:int}` / `{name:float}` / `{name:uuid}` / `{name:path}` placeholders. Use `FromQuery`, `FromHeader`, `FromCookie`, or `Parameter` (`vertebra.build.codegen`) to bind query strings, HTTP headers, and cookies with optional default values and wire-name aliases:

```python
from typing import Annotated

from vertebra import App
from vertebra.build.codegen import FromCookie, FromHeader, FromQuery, Parameter

app = App()


@app.get("/search")
def search_items(
    q: FromQuery[str],
    limit: Annotated[int, Parameter(query="limit", default=20)] = 20,
    request_id: Annotated[
        str, Parameter(header="X-Request-Id", default="none")
    ] = "none",
    session_id: FromCookie[str | None] = None,
) -> dict[str, str | int | None]:
    return {
        "q": q,
        "limit": limit,
        "request_id": request_id,
        "session_id": session_id,
    }
```

## Typed `msgspec.Struct` Request & Response Models

Vertebra uses `msgspec.Struct` (or `FrozenBaseStruct` from `vertebra.typing`) for fast JSON and MessagePack (`MsgPackBody`) validation and response serialization:

```python
import msgspec
from vertebra import App


class CreateOrderInput(msgspec.Struct):
    sku: str
    quantity: int = 1


class OrderSummary(msgspec.Struct):
    order_id: int
    sku: str
    quantity: int


app = App()


@app.post("/orders", status_code=201)
def create_order(body: CreateOrderInput) -> OrderSummary:
    return OrderSummary(order_id=101, sku=body.sku, quantity=body.quantity)
```

## Typed RPC Procedures (`Procedure` & `ProcedureRouter`)

For RPC-style `@query`, `@mutation`, and `@subscription` endpoints with automatic TypeScript client generation, declare procedures on a `Controller` or `ProcedureRouter` (`vertebra.framework.procedures`):

```python
from vertebra import App, ProcedureRouter

procedures = ProcedureRouter(prefix="/_rpc")


@procedures.query()
def server_time() -> dict[str, str]:
    return {"iso": "2026-10-31T00:00:00Z"}


@procedures.mutation()
def archive_order(order_id: int) -> dict[str, int | bool]:
    return {"order_id": order_id, "archived": True}


app = App(route_handlers=[procedures])
```

Invoke registered procedures directly in Python tests or internal services with `await app.call_procedure("archive_order", {"order_id": 42})`.

## Lifecycle Hooks, Sub-Apps & Auto-Wiring

Register startup/shutdown hooks, mount sub-applications under a path prefix, enable interactive OpenAPI + Scalar docs, or auto-discover controllers and routers across domain packages using `AutowireLoader`:

```python
from vertebra import App
from vertebra.framework.autowire import AutowireConfig, AutowireLoader

app = App()
app.enable_docs(title="Orders Service", version="1.0.0", docs_path="/docs")


@app.on_startup
async def connect_resources() -> None:
    pass


@app.on_shutdown
async def close_resources() -> None:
    pass


loader = AutowireLoader(AutowireConfig(domain_packages=("myapp.domains",)))
```
