# Quickstart

Build a typed Vertebra app, run it locally, and test its routes in `pytest`.

## 1. Create an Application

Create `app.py` with a health check and a `POST` route:

```python
import msgspec
from vertebra import App


class CreateItemRequest(msgspec.Struct):
    name: str
    quantity: int = 1


class ItemResponse(msgspec.Struct):
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

## 2. Run the Server

Start the dev server with live reload:

```bash
vtb dev app:app --port 8000
```

Run in production across multiple workers:

```bash
vtb run app:app --host 0.0.0.0 --port 8000 --workers 4
```

## 3. Test Routes in `pytest`

Test route lookup and path parameter extraction directly in memory without opening a socket:

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

- Tune worker modes and environment variables in [Configuration](configuration.md).
- Learn about routers, controllers, dependency injection, and guards in [Routing & Handlers](../usage/routing-and-handlers.md).
- Browse runnable scripts in [Examples](../examples/index.md).
