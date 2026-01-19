# Vertebra Examples

Vertebra is a Python-first application backbone that combines:
- **Python** for business logic with type-safe, tRPC-like developer experience
- **Bun/TypeScript** for high-performance HTTP and WebSocket handling
- **Rust** for zero-copy shared-memory IPC

## Quick Start

### 1. Basic Application

```python
# app.py
from vertebra import Vertebra, procedure
from pydantic import BaseModel

# Define your data models
class User(BaseModel):
    id: int
    name: str
    email: str

# Create the Vertebra app
app = Vertebra()

# Define procedures with automatic type extraction
@procedure(name="users.get")
async def get_user(user_id: int) -> User | None:
    # Your business logic here
    return User(id=user_id, name="Alice", email="alice@example.com")

@procedure(name="users.create")
async def create_user(user: User) -> User:
    # Save user to database
    return user
```

### 2. Running the Server

#### Option A: Python CLI (Recommended)

The Python CLI launches both the broker and server via a supervisor:

```bash
# Start with 4 workers
uv run python -m vertebra serve app:app --host 0.0.0.0 --port 8000 --workers 4

# With custom broker config
uv run python -m vertebra serve app:app --broker-config vertebra-broker.toml
```

#### Option B: Direct Bun Execution

For development or custom setups, run the TypeScript components directly:

```bash
# Terminal 1: Start the broker (database pools, fair scheduling)
cd src/ts
bun src/broker/serve.ts --config ../../vertebra-broker.toml

# Terminal 2: Start the ASGI server (HTTP routing)
cd src/ts
bun src/server/serve.ts
```

This gives you more control over each component independently.

---

## Database Drivers

Vertebra provides broker-backed drivers that communicate via shared memory for zero-copy performance.

### Async PostgreSQL

```python
from vertebra.drivers import AsyncPostgresDriver, AsyncPostgresDriverConfig
from vertebra.resources import BrokerClientConfig

# Configure the driver
config = AsyncPostgresDriverConfig(
    pool="postgres",  # Pool name from broker config
    client=BrokerClientConfig(name="vertebra-broker")
)
driver = AsyncPostgresDriver(config)

# Use the connection
conn = await driver.connect()
rows = await conn.fetch("SELECT * FROM users WHERE id = $1", [1])
await conn.close()
```

### Async SQLite

```python
from vertebra.drivers import AsyncSqliteDriver, AsyncSqliteDriverConfig

config = AsyncSqliteDriverConfig(pool="sqlite")
driver = AsyncSqliteDriver(config)

conn = await driver.connect()
await conn.execute("INSERT INTO items (name) VALUES (?)", ["widget"])
await conn.commit()
await conn.close()
```

### Sync Drivers

```python
from vertebra.drivers import SyncPostgresDriver, SyncPostgresDriverConfig

config = SyncPostgresDriverConfig(pool="postgres")
driver = SyncPostgresDriver(config)

conn = driver.connect()
cursor = conn.cursor()
cursor.execute("SELECT * FROM users")
rows = cursor.fetchall()
conn.close()
```

---

## Litestar Integration

```python
# app.py
from litestar import Litestar, get
from vertebra.extensions.litestar import VertebraPlugin
from vertebra import procedure
from pydantic import BaseModel

class Item(BaseModel):
    id: int
    name: str
    price: float

@procedure(name="items.list")
async def list_items() -> list[Item]:
    return [Item(id=1, name="Widget", price=9.99)]

@get("/items")
async def get_items() -> list[Item]:
    return await list_items()

app = Litestar(
    route_handlers=[get_items],
    plugins=[VertebraPlugin(log_level="info")],
)
```

Run with:
```bash
litestar run vertebra serve app:app --workers 4
```

---

## Broker Configuration

Create `vertebra-broker.toml` to configure database pools:

```toml
[broker]
name = "vertebra-broker"
control_name = "/vertebra-broker.control"
slot_size = 65536
slots = 1024
timeout_ms = 5000

[[pools]]
name = "postgres"
driver = "bun-postgres"
url = "postgresql://user:pass@localhost:5432/mydb"
max_connections = 10

[[pools]]
name = "sqlite"
driver = "bun-sqlite"
path = "./data/app.db"

[[pools]]
name = "redis"
driver = "bun-redis"
url = "redis://localhost:6379"

# Storage backends
[[pools]]
name = "local-storage"
driver = "storage-local"

[[pools]]
name = "s3-storage"
driver = "storage-s3"
bucket = "my-bucket"
```

---

## Complete Example: REST API with Database

```python
# app.py
from vertebra import Vertebra, procedure
from vertebra.drivers import AsyncPostgresDriver, AsyncPostgresDriverConfig
from vertebra.resources import BrokerClientConfig
from pydantic import BaseModel
from typing import Optional

# Models
class CreateUserRequest(BaseModel):
    name: str
    email: str

class User(BaseModel):
    id: int
    name: str
    email: str

# App
app = Vertebra()

# Shared driver configuration
driver_config = AsyncPostgresDriverConfig(
    pool="postgres",
    client=BrokerClientConfig(name="vertebra-broker")
)

@procedure(name="users.create")
async def create_user(request: CreateUserRequest) -> User:
    driver = AsyncPostgresDriver(driver_config)
    conn = await driver.connect()

    row = await conn.fetchone(
        """
        INSERT INTO users (name, email)
        VALUES ($1, $2)
        RETURNING id, name, email
        """,
        [request.name, request.email]
    )
    await conn.commit()
    await conn.close()

    return User(id=row[0], name=row[1], email=row[2])

@procedure(name="users.get")
async def get_user(user_id: int) -> Optional[User]:
    driver = AsyncPostgresDriver(driver_config)
    conn = await driver.connect()

    row = await conn.fetchone(
        "SELECT id, name, email FROM users WHERE id = $1",
        [user_id]
    )
    await conn.close()

    if row:
        return User(id=row[0], name=row[1], email=row[2])
    return None

@procedure(name="users.list")
async def list_users(limit: int = 10, offset: int = 0) -> list[User]:
    driver = AsyncPostgresDriver(driver_config)
    conn = await driver.connect()

    rows = await conn.fetch(
        "SELECT id, name, email FROM users LIMIT $1 OFFSET $2",
        [limit, offset]
    )
    await conn.close()

    return [User(id=r[0], name=r[1], email=r[2]) for r in rows]

@procedure(name="users.delete")
async def delete_user(user_id: int) -> bool:
    driver = AsyncPostgresDriver(driver_config)
    conn = await driver.connect()

    count = await conn.execute(
        "DELETE FROM users WHERE id = $1",
        [user_id]
    )
    await conn.commit()
    await conn.close()

    return count > 0
```

---

## Type Bridge (TypeScript Generation)

Procedures decorated with `@procedure` automatically extract type information. This enables TypeScript code generation for a tRPC-like experience:

```python
# Python
@procedure(name="items.search")
async def search_items(query: str, category: Optional[str] = None) -> list[Item]:
    ...
```

Generates TypeScript:
```typescript
// Auto-generated
interface Item {
  id: number;
  name: string;
  price: number;
}

async function searchItems(query: string, category?: string | null): Promise<Item[]>;
```

---

## Architecture Overview

```
HTTP Client
    │
    ▼
┌─────────────────────────────────────┐
│ Bun Server (TypeScript)             │
│  - HTTP/2, WebSocket, JSON-RPC      │
│  - ASGI request proxying            │
│  [bun src/server/serve.ts]          │
└─────────────────┬───────────────────┘
                  │ Shared Memory IPC
                  ▼
┌─────────────────────────────────────┐
│ Python ASGI App                     │
│  - Business logic                   │
│  - @procedure handlers              │
│  - Driver facades                   │
└─────────────────┬───────────────────┘
                  │ Shared Memory IPC
                  ▼
┌─────────────────────────────────────┐
│ Bun Broker (TypeScript)             │
│  - Connection pooling               │
│  - Fair scheduling                  │
│  - Database backends                │
│  [bun src/broker/serve.ts]          │
└─────────────────┬───────────────────┘
                  │
                  ▼
            PostgreSQL / MySQL / SQLite / Redis
```

---

## CLI Reference

### Python CLI

```bash
# Start server (launches broker + ASGI server)
vertebra serve <app> [OPTIONS]

Options:
  --host TEXT           Server host [default: 127.0.0.1]
  --port INTEGER        Server port [default: 8000]
  --workers INTEGER     Number of worker processes [default: 1]
  --config PATH         Path to config file
  --log-level TEXT      Logging level [default: info]
  --broker-config PATH  Path to broker config [default: vertebra-broker.toml]
```

### Direct Bun Commands

```bash
# Broker (connection pooling, database backends)
bun src/ts/src/broker/serve.ts [OPTIONS]

Options:
  --config PATH    Path to vertebra-broker.toml

# ASGI Server (HTTP routing to Python)
bun src/ts/src/server/serve.ts
```

---

## Development Setup

```bash
# Install all dependencies
make install

# Run tests
make test          # Python tests
make js-test       # TypeScript tests
make rs-test       # Rust tests

# Type checking
make lint          # Python linting
make js-type-check # TypeScript type check

# Build Rust components
make rs-build
```
