# Routing & Handlers

Vertebra routes HTTP requests through a compiled radix router in `vertebra-http` directly to typed Python functions registered on `App`, modular `Router` instances, or typed RPC `ProcedureRouter` definitions.

## Route Decorators on `App` and `Router`

Register routes directly on `App` with `@app.get`, `@app.post`, `@app.put`, `@app.patch`, and `@app.delete`, or attach standalone `@get`, `@post`, `@put`, `@patch`, and `@delete` route definitions to a modular `Router`:

```python
from vertebra import App, Router, get, post

users_router = Router(prefix="/users", tags=["users"])


@get("/{user_id:int}")
def get_user(user_id: int) -> dict[str, int | str]:
    return {"id": user_id, "username": f"user-{user_id}"}


@post("")
def create_user(payload: dict[str, str]) -> dict[str, str]:
    return {"status": "created", **payload}


users_router.register(get_user)
users_router.register(create_user)

app = App()
app.register(users_router)
```

## Typed `msgspec.Struct` Request & Response Models

Vertebra uses `msgspec.Struct(..., frozen=True, gc=False)` (or `FrozenBaseStruct` from `vertebra.typing`) for zero-copy JSON/Msgpack request validation and response serialization without cyclic garbage-collection overhead:

```python
import msgspec
from vertebra import App


class CreateOrderInput(msgspec.Struct, frozen=True, gc=False):
    sku: str
    quantity: int = 1


class OrderSummary(msgspec.Struct, frozen=True, gc=False):
    order_id: int
    sku: str
    quantity: int


app = App()


@app.post("/orders")
def create_order(body: CreateOrderInput) -> OrderSummary:
    return OrderSummary(order_id=101, sku=body.sku, quantity=body.quantity)
```

## Typed RPC Procedures (`ProcedureRouter`)

For RPC-style query and mutation endpoints with automatic TypeScript client generation, use `ProcedureRouter` (`vertebra.framework.procedures`):

```python
from vertebra.framework.procedures import ProcedureRouter

procedures = ProcedureRouter(prefix="/rpc")


@procedures.query()
def server_time() -> dict[str, str]:
    return {"iso": "2026-10-31T00:00:00Z"}


@procedures.mutation()
def archive_order(order_id: int) -> dict[str, int | bool]:
    return {"order_id": order_id, "archived": True}
```
