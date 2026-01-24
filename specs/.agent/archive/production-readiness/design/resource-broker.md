# Design: Resource Broker & Pooling (Phase 2)

**Focus**: Implementing the Bun broker as a universal resource router with shared-memory transport.

**Update (2026-01-19)**: Socket-based broker IPC is superseded by
`specs/active/shared-memory-broker/`.

## 1. Broker Process Structure

**Binary**: Bun broker process (TypeScript/Bun)
**Location**: `src/ts/src/broker/` (shared-memory transport via bun:ffi)

**Components**:
1.  **Config**: Loaded from broker TOML (or env) at startup; Python passes the path via `VERTEBRA_BROKER_CONFIG`.
2.  **Shared-Memory Loop**: Async loop polling ring buffers.
3.  **Router**: Maps `pool_name` to a `ResourceProvider`.
4.  **Scheduler**: WFQ/DRR-style fairness for request ordering.

**Startup Flow**:
```python
# app.py - Python is the entry point
from vertebra import Vertebra
from vertebra.config import BrokerConfig, PoolConfig

app = Vertebra(
    broker=BrokerConfig(
        name="vertebra-broker",
        arrow_threshold_bytes=1_048_576,
    ),
)

# Internal pools registered via Python
app.register_pool(PoolConfig(
    name="postgres_main",
    type="internal",
    driver="bun-postgres",
    connection_string="postgres://...",
))
```

## 2. Resource Provider Architecture

To support "any database adapter", the Broker supports two types of providers:

### Type A: Internal Provider (Bun)
-   **Host**: Inside the Bun broker process.
-   **Implementation**: Bun SQL for Postgres/MySQL/SQLite; native Redis; storage adapters.
-   **Performance**: Maximum (Zero-copy, no GIL).
-   **Supported**: Postgres, MySQL, Redis, SQLite, storage backends.
-   **Registration**: Via Python config at app startup.

```python
# Register internal providers via Python
app.register_pool(PoolConfig(
    name="postgres_main",
    type="internal",
    driver="bun-postgres",  # or "rust-mysql", "rust-redis"
    connection_string="postgres://user:pass@localhost/db",
    max_connections=20,
))
```

### Type B: External Provider (Python Workers)
-   **Host**: A dedicated Python process (SQLSpec/SQLAlchemy adapter or storage backend).
-   **Registration**: Dynamic via shared-memory `REGISTER_PROVIDER` message.
-   **Mechanism**:
    1.  Python worker starts with SQLAlchemy engine or SQLSpec config.
2.  Connects to Broker shared memory transport.
    3.  Sends `REGISTER_PROVIDER(name="oracle_pool", dialect="oracle", capacity=10)`.
    4.  Broker adds "oracle_pool" to routing table.

```python
# oracle_worker.py - External provider
from vertebra.extensions.sqlalchemy import DriverWorker
from sqlalchemy import create_engine

engine = create_engine("oracle+cx_oracle://user:pass@host/xe")
worker = DriverWorker(pool_name="oracle_main", engine=engine, concurrency=10)
worker.run()  # Registers with broker and starts handling requests
```

**Query Flow**:
1.  Client sends `EXECUTE("oracle_pool", sql)`.
2.  Broker accepts request, applies Fairness Scheduler.
3.  Broker forwards request to the registered External Provider (via IPC).
4.  External Provider executes SQL via native driver.
5.  Result streams back to Client (via Broker or direct Shared Memory).

## 3. Protocol Extensions

**Registry Messages**:
-   `REGISTER_PROVIDER`: `{ name: string, type: "external", dialect: string, capacity: int, features: string[] }`
-   `HEARTBEAT`: Keep-alive for external providers.
-   `UNREGISTER_PROVIDER`: Graceful deregistration.

**Configuration Messages** (future, optional):
-   `CONFIGURE_POOL`: `{ name: string, type: "internal", driver: string, connection_string: string, max_connections: int }`
-   `BROKER_CONFIG`: `{ name: string, arrow_threshold_bytes: int }`

Current implementation reads config from TOML on broker start; dynamic config messages are deferred.

**Routing Logic**:
```rust
enum Provider {
    Internal(Box<dyn ManagedPool>),  // Rust/Bun driver
    External(ConnectionHandle),       // Python worker
}

async fn handle_request(req: Request) {
    let provider = router.get(req.pool_name)?;
    match provider {
        Internal(pool) => pool.execute(req).await,
        External(worker) => worker.forward(req).await,
    }
}
```

## 4. Scheduler & Fairness

The **Scheduler** works exactly the same for both provider types.
-   It queues requests based on `worker_id`.
-   It releases requests to the Provider when capacity is available.
-   This ensures **Global Fairness** even for Python-backed drivers.

## 5. Python-First Integration

**Key Principle**: Python is always the entry point and configuration source.

```python
# Full example: app.py
from vertebra import Vertebra
from vertebra.config import BrokerConfig, PoolConfig
from litestar import Litestar, get

app = Vertebra(
    broker=BrokerConfig(name="vertebra-broker"),
)

# Internal pool (Bun postgres)
app.register_pool(PoolConfig(
    name="main",
    type="internal",
    driver="bun-postgres",
    connection_string="postgres://localhost/app",
))

@get("/")
async def index() -> dict:
    return {"status": "ok"}

litestar_app = Litestar([index], plugins=[app.litestar_plugin])
```

**External workers started separately**:
```bash
# In another terminal
python oracle_worker.py  # Registers oracle_main pool
python snowflake_worker.py  # Registers snowflake_dw pool
```

**Future**: TOML/YAML config file support may be added as convenience layer on top.
