# Testing

Test Vertebra applications in `pytest` without opening network sockets. Use in-process route lookup, dependency overrides, in-memory object stores, `:memory:` SQLite databases, and the in-memory queue backend.

## Testing Routes, Controllers & Dependencies

Verify route registration, path parameter extraction, and dependency overrides directly in memory:

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
    assert params == {"order_id": 7}

    hid = handler_id("GET", "/orders/{order_id:int}")
    info = app.handler_index[hid]
    assert info.path == "/orders/{order_id:int}"
```

## Testing Async Queues & Storage

Use `pytest` and `anyio` to test async handlers, `memory://` queues, and in-memory object storage:

```python
import pytest
from vertebra.framework.queues import QueueConfig, QueueService, TaskDef
from vertebra.storage import SyncObjectStore


async def process_item(item_id: int) -> int:
    return item_id


@pytest.mark.anyio
async def test_memory_queue_and_storage() -> None:
    store = SyncObjectStore.from_memory(namespace="test-uploads")
    store.put("items/7.json", b'{"item_id": 7}')
    assert store.get("items/7.json") == b'{"item_id": 7}'

    queue = QueueService(QueueConfig(backend_url="memory://"))
    queue.register(TaskDef(name="jobs.process", fn=process_item))
    try:
        env = await queue.enqueue("jobs.process", item_id=7)
        assert env.task_name == "jobs.process"
    finally:
        await queue.close()
```

## In-Memory Backends for Fast Tests

- **Dependency overrides**: Pass test doubles via `App(dependencies=...)` or `container.create_child({...})`.
- **Object storage**: Call `SyncObjectStore.from_memory()` or `AsyncObjectStore.from_memory()` for zero-disk storage tests.
- **SQLite**: Use `SyncSQLiteDriver(SQLiteConfig(path=":memory:"))` for isolated SQL schema and query tests.
