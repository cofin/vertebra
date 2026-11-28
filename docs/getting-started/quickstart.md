# Quickstart

Define typed HTTP routes, assemble an application, and run it locally.

## Create an application

Save this example as `app.py`.

```python
import msgspec

from vertebra import App, get, post


class CreateItemRequest(msgspec.Struct):
    name: str
    quantity: int = 1


class ItemResponse(msgspec.Struct):
    id: int
    name: str
    quantity: int


@get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@post("/items", status_code=201)
async def create_item(body: CreateItemRequest) -> ItemResponse:
    return ItemResponse(id=1, name=body.name, quantity=body.quantity)


app = App(route_handlers=[health, create_item])
app.enable_docs()
```

`get` and `post` declare handlers. The `App` call registers them and supplies the
routing configuration. The `msgspec.Struct` types describe the JSON request and response.

## Run the server

```bash
vtb run app:app --host 127.0.0.1 --port 8000
```

For local changes, use `vtb dev app:app --port 8000` to reload on edits.

## Make a request

```bash
curl -i http://127.0.0.1:8000/health
curl -i http://127.0.0.1:8000/items \
  -H 'Content-Type: application/json' \
  -d '{"name":"Notebook","quantity":2}'
```

The health check returns HTTP 200 with `{"status":"ok"}`. Creating the item returns
HTTP 201 with `{"id":1,"name":"Notebook","quantity":2}`. Open
`http://127.0.0.1:8000/docs` to view the generated API documentation.

## Next steps

- Group handlers and configure guards in [Routing & Handlers](../usage/routing-and-handlers.md).
- Set up worker modes and environment variables in [Configuration](configuration.md).
- Exercise application routes from pytest in [Testing](../usage/testing.md).
