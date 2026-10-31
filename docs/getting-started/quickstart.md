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

## 3. Verify Routes in `pytest`

Verify route registration, deterministic FNV-1a handler IDs (`handler_id`), and path parameter extraction (`extract_path_params`) directly in `pytest` without opening a network socket:

```python
from vertebra import App, extract_path_params, handler_id


def test_routes_and_handlers() -> None:
    app = App()

    @app.get("/items/{item_id:int}")
    def get_item(item_id: int) -> dict[str, int]:
        return {"id": item_id}

    params = extract_path_params("/items/{item_id:int}", "/items/42")
    assert params == {"item_id": "42"}

    hid = handler_id("GET", "/items/{item_id:int}")
    info = app.get_handler_by_id(hid)
    assert info is not None
    assert info.fn(int(params["item_id"])) == {"id": 42}
```

## Next Steps

- Configure runtime topologies and environment variables in [Configuration](configuration.md).
- Explore modular routers, dependency injection, guards, and `msgspec.Struct` models in [Routing & Handlers](../usage/routing-and-handlers.md).
- Browse runnable scripts in [Examples](../examples/index.md).
