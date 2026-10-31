# Testing

`vertebra.framework.testing` provides synchronous (`TestClient`) and asynchronous (`AsyncTestClient`) test clients that execute routes, middleware, guards, dependency injection, WebSockets, and SSE streams directly in memory without binding a TCP port.

## Synchronous Route Testing (`TestClient`)

```python
from vertebra import App
from vertebra.framework.testing import TestClient


def test_health_and_headers() -> None:
    app = App()

    @app.get("/ping")
    def ping() -> dict[str, str]:
        return {"pong": "true"}

    client = TestClient(app)
    response = client.get("/ping", headers={"x-request-id": "req-1"})

    assert response.status == 200
    assert response.json() == {"pong": "true"}
```

## Asynchronous Testing (`AsyncTestClient`)

Use `AsyncTestClient` with `pytest` and `anyio` when testing async lifecycle hooks, streaming responses, or background task queues:

```python
import pytest
from vertebra import App
from vertebra.framework.testing import AsyncTestClient


@pytest.mark.anyio
async def test_async_route() -> None:
    app = App()

    @app.get("/items/{item_id:int}")
    async def get_item(item_id: int) -> dict[str, int]:
        return {"item_id": item_id}

    async with AsyncTestClient(app) as client:
        response = await client.get("/items/7")
        assert response.status == 200
        assert response.json() == {"item_id": 7}
```

## Dependency Overrides & In-Memory Subsystems

- **Dependency Overrides**: Replace external services on `app.container` during tests so handlers receive deterministic fakes.
- **In-Memory Object Storage**: Pass `MemoryConfig()` to `vertebra.storage.Store` for zero-disk object storage tests.
- **In-Memory SQLite Pools**: Use `SqlitePool.connect("sqlite::memory:")` for fast relational schema and query verification.
