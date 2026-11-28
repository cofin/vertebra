# Routing & Handlers

Vertebra routes incoming HTTP requests through its Rust router directly to typed Python functions, controllers, modular routers, or RPC procedures.

## Compose an application

Declare handlers with `get`, `post`, `put`, `patch`, and `delete`. Group related
handlers on a controller or nest routers to apply a common URL prefix. Save
the controller and its dependencies in a domain module such as `inventory.py`.

```python
from vertebra import Controller, Provide, get


class InventoryService:
    def fetch(self, item_id: int) -> dict[str, int | str]:
        return {"id": item_id, "sku": "VTB-100"}


class ItemController(Controller):
    path = "/items"
    dependencies = {"inventory": Provide(InventoryService, scope="singleton")}

    @get("/{item_id:int}")
    async def read_item(
        self, item_id: int, inventory: InventoryService
    ) -> dict[str, int | str]:
        return inventory.fetch(item_id)
```

Import the controller where you assemble the app. Create routers in the factory
so each app gets fresh routing groups.

```python
from inventory import ItemController

from vertebra import App, Router, get


@get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


def create_app() -> App:
    version = Router(prefix="/v1", route_handlers=[ItemController])
    api = Router(prefix="/api", route_handlers=[version])
    return App(route_handlers=[health, api])


app = create_app()
```

With both files in the same directory, run `vtb run app:app` and request
`GET /api/v1/items/42`. The response is `{"id":42,"sku":"VTB-100"}`.

Use `app.register(target)` to add handlers, controllers, and routers during
assembly. `@app.get` and the other app-bound decorators register immediately.

## Apply guards

Guards run before the handler in this order.

| Layer | Configuration |
| --- | --- |
| Application | `App(guards=[...])` |
| Routers, outer to inner | `Router(guards=[...])` |
| Controller | `Controller.guards` |
| Handler | `@get(..., guards=[...])` |

A guard returning `False` or raising `PermissionDeniedError` stops dispatch
and returns HTTP 403. Native role and scope guards evaluate the authenticated
`Principal` supplied by the request or by a dependency. The guarded inventory
example registers a request-scoped principal provider that verifies a bearer
credential. [Inventory app](../examples/index.md#controllers-routers-guards)

## Construct a route directly

Use `Route` for a programmatically defined method, path, and handler.

```python
from vertebra import App, Route


async def ping() -> dict[str, str]:
    return {"ping": "pong"}


ping_route = Route(method="GET", path="/ping", handler=ping)
app = App(route_handlers=[ping_route])
```

## Path, Query, Header & Cookie Parameters

Extract path parameters from placeholders like `{name}` or typed variants (int, float, uuid, path). Use `FromQuery`, `FromHeader`, `FromCookie`, or `Parameter` to bind query strings, headers, and cookies with defaults and wire-name aliases:

```python
from typing import Annotated

from vertebra import App, get
from vertebra.build.codegen import FromCookie, FromHeader, FromQuery, Parameter

@get("/search")
async def search_items(
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


app = App(route_handlers=[search_items])
```

## Request & Response Models

Use `msgspec.Struct` models for fast JSON and MessagePack request validation and response serialization:

```python
import msgspec

from vertebra import App, post


class CreateOrderInput(msgspec.Struct):
    sku: str
    quantity: int = 1


class OrderSummary(msgspec.Struct):
    order_id: int
    sku: str
    quantity: int


@post("/orders", status_code=201)
async def create_order(body: CreateOrderInput) -> OrderSummary:
    return OrderSummary(order_id=101, sku=body.sku, quantity=body.quantity)


app = App(route_handlers=[create_order])
```

## RPC Procedures

Declare query, mutation, and subscription endpoints on a `Controller` or `ProcedureRouter` to expose typed RPC APIs with automatic TypeScript client generation:

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

In tests or internal services, call registered procedures directly with `await app.call_procedure("archive_order", {"order_id": 42})`.

## Application setup

Configure OpenAPI after registering routes. Use `app.on_startup` and
`app.on_shutdown` to manage process-local resources. Use `App.mount` to add
an ASGI, RSGI, or WSGI app under a URL prefix.

```python
from vertebra import App, get
from vertebra.framework.openapi import OpenAPIConfig, ScalarDocsConfig


@get("/orders")
async def list_orders() -> list[dict[str, int]]:
    return [{"id": 101}]


app = App(route_handlers=[list_orders])
app.enable_docs(
    OpenAPIConfig(
        title="Orders Service",
        version="1.0.0",
        scalar=ScalarDocsConfig(path="/docs"),
    )
)
```

`AutowireLoader` can discover controllers and routers across domain packages.
See [DI, Security & OpenAPI](framework-features.md) for provider scopes and
policy configuration.
