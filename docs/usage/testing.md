# Testing

Vertebra applications and subsystems are designed for deterministic, zero-network unit testing in `pytest` using in-process route inspection, `DependencyGraph` overrides, in-memory `ObjectStore` namespaces, `:memory:` SQLite drivers, and `MemoryQueueBackend`.

## Synchronous Route & Handler Testing

Verify route registration, deterministic FNV-1a handler IDs (`handler_id`), and path parameter extraction (`extract_path_params`) directly in memory without binding a TCP port:

```python
from vertebra import App, extract_path_params, handler_id


def test_health_and_params() -> None:
    app = App()

    @app.get("/orders/{order_id:int}")
    def get_order(order_id: int) -> dict[str, int | str]:
        return {"order_id": order_id, "status": "paid"}

    params = extract_path_params("/orders/{order_id:int}", "/orders/7")
    assert params == {"order_id": "7"}

    hid = handler_id("GET", "/orders/{order_id:int}")
    info = app.get_handler_by_id(hid)
    assert info is not None
    assert info.fn(int(params["order_id"])) == {
        "order_id": 7,
        "status": "paid",
    }
```

## Asynchronous Queue & Storage Testing

Use `pytest` and `anyio` when testing async handlers, `QueueService` with `memory://`, and `ObjectStore.from_memory()`:

```python
import pytest
from vertebra.framework.queues import QueueConfig, QueueService
from vertebra.storage import ObjectStore


@pytest.mark.anyio
async def test_memory_queue_and_storage() -> None:
    store = ObjectStore.from_memory(namespace="test-uploads")
    store.put("items/7.json", b'{"item_id": 7}')
    assert store.get("items/7.json") == b'{"item_id": 7}'

    queue = QueueService(QueueConfig(backend_url="memory://"))
    env = await queue.enqueue("jobs.process", item_id=7)
    assert env.task_name == "jobs.process"
```

## Dependency Overrides & In-Memory Subsystems

- **Dependency Graph Overrides**: Register test providers on `DependencyGraph` (`vertebra.framework.di`) so handlers receive deterministic fakes.
- **In-Memory Object Storage**: Call `ObjectStore.from_memory()` or pass `MemoryConfig()` to `ObjectStore.from_config()` for zero-disk object storage tests.
- **In-Memory SQLite Drivers**: Use `SyncSqliteDriver(SqliteConfig(path=":memory:"))` for fast relational schema and query verification.
