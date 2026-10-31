# Routing & Handlers

Vertebra routes HTTP requests through a compiled radix router in `vertebra-http` directly to typed Python functions, `Router` instances, or class-based `Controller` definitions.

## Route Decorators on `App` and `Router`

Register routes using `@app.get`, `@app.post`, `@app.put`, `@app.patch`, and `@app.delete` (or standalone `@get`, `@post`, `@put`, `@patch`, `@delete` decorators on a `Router`):

```python
from vertebra import App, Router

users_router = Router(prefix="/users")


@users_router.get("/{user_id:int}")
def get_user(user_id: int) -> dict[str, int | str]:
    return {"id": user_id, "username": f"user-{user_id}"}


app = App()
app.include_router(users_router)
```

## Class-Based Controllers

Group related endpoints on a `Controller` subclass with shared path prefixes, guards, or dependencies:

```python
from vertebra.framework import Controller, get, post


class OrderController(Controller):
    path = "/orders"

    @get("/")
    def list_orders(self) -> list[dict[str, int]]:
        return [{"id": 101}]

    @post("/")
    def create_order(self, payload: dict[str, str]) -> dict[str, str]:
        return {"status": "created", **payload}
```

## Typed Models & `DTO` Projection

Vertebra uses `msgspec.Struct(..., frozen=True, gc=False)` for high-throughput request validation and response serialization without cyclic garbage-collection overhead. Use `DTO` (`vertebra.framework.dto.DTO`) to include, exclude, or rename fields per route:

```python
import msgspec
from vertebra.framework.dto import DTO, DTOConfig


class UserRecord(msgspec.Struct, frozen=True, gc=False):
    id: int
    email: str
    password_hash: str


PublicUserDTO = DTO(UserRecord, DTOConfig(exclude={"password_hash"}))
```

## Request, Response & Streaming

Access raw request metadata via `vertebra.framework.Request` and return custom headers, status codes, or streaming payloads via `Response`, `StreamingResponse`, or `EventSourceResponse`:

```python
from collections.abc import AsyncIterator
from vertebra.framework import EventSourceResponse, Request, Response


async def stream_events(request: Request) -> EventSourceResponse:
    async def generate() -> AsyncIterator[str]:
        yield "data: connected\n\n"

    return EventSourceResponse(generate())
```
