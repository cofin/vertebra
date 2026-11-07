# Testing

Test Vertebra applications in `pytest` without opening network sockets using in-process route and controller dispatch, `Container` / `Provide` dependency overrides, in-memory `SyncObjectStore` / `AsyncObjectStore` namespaces, `:memory:` SQLite drivers, and `MemoryQueueBackend`.

## Route, Controller & Dependency Override Testing

Test route registration, `Controller` endpoints, path parameter extraction (`extract_path_params`), and `Provide` dependency overrides directly in memory:

```python
from vertebra import App, Controller, Provide, extract_path_params, get, handler_id


class OrderRepo:
    def fetch_status(self, order_id: int) -> str:
        return "paid"


class OrderController(Controller):
    path = "/orders"
    dependencies = {"repo": Provide(OrderRepo, scope="singleton")}

    @get("/{order_id:int}")
    def get_order(self, order_id: int, repo: OrderRepo) -> dict[str, int | str]:
        return {"order_id": order_id, "status": repo.fetch_status(order_id)}


def test_controller_and_params() -> None:
    app = App(route_handlers=[OrderController])

    params = extract_path_params("/orders/{order_id:int}", "/orders/7")
    assert params == {"order_id": "7"}

    hid = handler_id("GET", "/orders/{order_id:int}")
    info = app.get_handler_by_id(hid)
    assert info is not None
```

## Asynchronous Queue & Storage Testing

Use `pytest` and `anyio` when testing async handlers, `QueueService` with `memory://`, and `SyncObjectStore.from_memory()`:

```python
import pytest
from vertebra.framework.queues import QueueConfig, QueueService
from vertebra.storage import SyncObjectStore


@pytest.mark.anyio
async def test_memory_queue_and_storage() -> None:
    store = SyncObjectStore.from_memory(namespace="test-uploads")
    store.put("items/7.json", b'{"item_id": 7}')
    assert store.get("items/7.json") == b'{"item_id": 7}'

    queue = QueueService(QueueConfig(backend_url="memory://"))
    env = await queue.enqueue("jobs.process", item_id=7)
    assert env.task_name == "jobs.process"
```

## In-Memory Subsystems for Fast Tests

- **Dependency Overrides**: Pass overridden `Provide(...)` entries on `App(dependencies=...)` or `container.create_child({...})` so handlers receive test doubles.
- **In-Memory Object Storage**: Call `SyncObjectStore.from_memory()` (or `AsyncObjectStore.from_memory()`) or pass `MemoryConfig()` to `SyncObjectStore.from_config()` for zero-disk storage tests.
- **In-Memory SQLite Drivers**: Use `SyncSQLiteDriver(SQLiteConfig(path=":memory:"))` for fast SQL schema and query tests.
