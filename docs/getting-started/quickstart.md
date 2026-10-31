# Quickstart

Build a typed Vertebra application with `App`, `msgspec.Struct` models, and in-process testing.

## 1. Create an Application

Create `app.py` with a health endpoint and a typed `POST` route:

```python
import msgspec
from vertebra import App


class CreateItemRequest(msgspec.Struct, frozen=True, gc=False):
    name: str
    quantity: int = 1


class ItemResponse(msgspec.Struct, frozen=True, gc=False):
    id: int
    name: str
    quantity: int


app = App()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/items")
def create_item(body: CreateItemRequest) -> ItemResponse:
    return ItemResponse(id=1, name=body.name, quantity=body.quantity)
```

## 2. Run the Development Server

Start the application with live reload using `vtb dev`:

```bash
uv run vtb dev app:app --port 8000
```

Or run in production mode with `vtb run`:

```bash
uv run vtb run app:app --host 0.0.0.0 --port 8000 --topology embedded
```

## 3. Test with `TestClient`

Verify routes synchronously in `pytest` without opening a network socket using `vertebra.framework.testing.TestClient`:

```python
from vertebra.framework.testing import TestClient

from app import app


def test_create_item() -> None:
    client = TestClient(app)

    health_res = client.get("/health")
    assert health_res.status == 200
    assert health_res.json() == {"status": "ok"}

    item_res = client.post("/items", json={"name": "widget", "quantity": 3})
    assert item_res.status == 200
    assert item_res.json() == {"id": 1, "name": "widget", "quantity": 3}
```

## Next Steps

- Configure runtime topologies and environment variables in [Configuration](configuration.md).
- Explore controllers, dependency injection, guards, and DTOs in [Routing & Handlers](../usage/routing-and-handlers.md).
- Browse runnable scripts in [Examples](../examples/index.md).
