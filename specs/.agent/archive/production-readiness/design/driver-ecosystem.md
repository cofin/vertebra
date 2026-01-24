# Design: ORM & Driver Ecosystem (Phase 3)

**Focus**: Bidirectional driver architecture—expose Vertebra's internal drivers (Bun postgres) to Python clients while allowing any SQLSpec/SQLAlchemy adapter to register with the broker.

**Transport**: Shared-memory C ABI (CFFI in Python, bun:ffi in Bun). **No sockets** in the data plane.

## 1. Architecture Overview

The driver ecosystem supports **two directions**:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Vertebra Broker (Router)                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────────────┐      ┌─────────────────────────────────┐   │
│  │   Internal Providers    │      │      External Providers          │   │
│  │   (Bun Built-in)        │      │      (Python Workers)            │   │
│  ├─────────────────────────┤      ├─────────────────────────────────┤   │
│  │ • Bun Postgres (Bun.SQL)│      │ • SQLAlchemy Oracle (cx_Oracle) │   │
│  │ • Bun MySQL (Bun.SQL)   │      │ • SQLAlchemy Snowflake          │   │
│  │ • Bun SQLite (bun:sqlite│      │ • Any asyncpg/psycopg pool      │   │
│  │ • Bun Redis (RedisClient│      │ • Custom Python adapters        │   │
│  └─────────────────────────┘      └─────────────────────────────────┘   │
│                                                                          │
│                          ▲                    ▲                          │
│                          │                    │                          │
│                     IPC Execute          IPC Forward                     │
│                          │                    │                          │
└──────────────────────────┼────────────────────┼──────────────────────────┘
                           │                    │
            ┌──────────────┴────────────────────┴──────────────┐
            │            Python Application                     │
            ├──────────────────────────────────────────────────┤
            │  SQLAlchemy          │  SQLSpec         │  AA    │
            │  vertebra+postgres:// │  IPCConfig       │  Repo  │
            │  vertebra+oracle://   │  IPCDriver       │  Svc   │
            └──────────────────────────────────────────────────┘
```

**Key Principle**: The client doesn't know (or care) if the backend is Rust, Bun, or Python—it just sends shared-memory requests to a pool name.

## 2. Internal Providers (Exposing Bun/Rust Drivers)

Internal providers run inside the broker process or as tightly-coupled sidecars.

### 2.1 Bun Postgres Driver

The Bun runtime's high-performance postgres driver becomes accessible via SQLAlchemy/SQLSpec.

**How it works**:
1. App configures internal pool via Python config
2. SQLAlchemy uses `vertebra+postgres://main` dialect (or SQLSpec `IPCConfig`)
3. Driver sends shared-memory `EXECUTE` to broker
4. Broker routes to Bun postgres driver
5. Result returns via shared memory (Arrow) or MsgPack

**Configuration (Python)**:
```python
from vertebra import Vertebra
from vertebra.config import PoolConfig

app = Vertebra()

# Register Bun postgres as internal provider
app.register_pool(
    PoolConfig(
        name="postgres_main",
        type="internal",
        driver="bun-postgres",
        connection_string="postgres://user:pass@localhost:5432/db",
        max_connections=20,
    )
)
```

**Client Usage (SQLAlchemy)**:
```python
from sqlalchemy import create_engine

# Uses Bun postgres driver under the hood
engine = create_engine("vertebra+postgres://postgres_main")

with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM users"))
```

**Client Usage (SQLSpec)**:
```python
from vertebra.extensions.sqlspec import IPCConfig

config = IPCConfig(pool_name="postgres_main")

async with config.provide_session() as session:
    result = await session.execute("SELECT * FROM users")
```

### 2.2 Internal Driver Capabilities

All internal drivers use **Bun's built-in database clients** and expose the full Bun API surface.

**Reference**: [Bun SQL Docs](https://bun.com/docs/runtime/sql), [bun:sqlite Docs](https://bun.com/docs/runtime/sqlite), [Bun Redis Docs](https://bun.com/docs/runtime/redis)

#### Postgres/MySQL (Bun.SQL)

| Feature | Method | Description |
|---------|--------|-------------|
| **Transactions** | `begin`, `commit`, `rollback` | Scoped transaction with auto-rollback on error |
| **Distributed Txn** | `beginDistributed` | Two-phase commit (PREPARE TRANSACTION) |
| **Savepoints** | Nested `begin` calls | Automatic savepoint creation |
| **Prepared Stmts** | Automatic | Enabled by default, improves repeated query perf |
| **Pipelining** | Automatic | Commands batched for network efficiency |
| **Pool Stats** | `active`, `idle`, `waiting` | Connection pool observability |
| **Result Metadata** | `lastrowid`, `rowcount` | INSERT/UPDATE/DELETE metadata |

```python
# Register Bun MySQL
app.register_pool(
    PoolConfig(
        name="mysql_main",
        type="internal",
        driver="bun-mysql",
        connection_string="mysql://user:pass@localhost:3306/db",
    )
)
```

#### SQLite (bun:sqlite)

| Feature | Method | Description |
|---------|--------|-------------|
| **Transactions** | `transaction` | Callable wrapper with auto-commit/rollback |
| **Transaction Modes** | `deferred`, `immediate`, `exclusive` | Lock acquisition strategy |
| **Query Methods** | `get`, `all`, `values`, `run` | Single row, all rows, arrays, no-result |
| **Result Metadata** | `lastInsertRowid`, `changes` | From `run()` method |
| **Backup/Restore** | `serialize`, `deserialize` | Full database serialization |
| **Extensions** | `loadExtension` | Load SQLite extensions |
| **Pragmas** | via raw SQL | WAL mode, etc. |

```python
# Register Bun SQLite
app.register_pool(
    PoolConfig(
        name="sqlite_db",
        type="internal",
        driver="bun-sqlite",
        connection_string="file:./data.db",
    )
)
```

#### Redis (Bun.RedisClient)

| Feature | Method | Description |
|---------|--------|-------------|
| **Pub/Sub** | `subscribe`, `unsubscribe`, `publish` | Real-time messaging |
| **Subscriber Conn** | `duplicate` | Dedicated connection for subscribers |
| **Pipelining** | Automatic | Commands batched by default |
| **Commands** | `send(cmd, args)` | All 66+ Redis commands |
| **Auto-Reconnect** | `autoReconnect` option | Handles network failures |
| **Offline Queue** | `enableOfflineQueue` | Queue commands during disconnect |
| **TLS** | `tls` options | Secure connections |

```python
# Register Bun Redis
app.register_pool(
    PoolConfig(
        name="redis_cache",
        type="internal",
        driver="bun-redis",
        connection_string="redis://localhost:6379",
    )
)
```

## 3. External Providers (SQLSpec Configs)

Any SQLSpec config can register with the broker. This is the **primary** way to add external database support.

### 3.1 Registration Pattern

```python
from sqlspec.adapters.oracledb import OracleAsyncConfig
from vertebra.extensions.sqlspec.provider import SqlSpecProvider
from vertebra.resources import ProviderConfig

oracle_config = OracleAsyncConfig(
    dsn="user/pass@host:1521/xe",
    pool_size=10,
)

# Provider process registers with broker
provider = SqlSpecProvider(
    oracle_config,
    ProviderConfig(pool="oracle_main", driver="oracledb", max_size=10),
)
await provider.register()
await provider.serve()
```

**Client side**: wrap the same SQLSpec config with `BrokerProxySyncConfig` / `BrokerProxyAsyncConfig`
to route queries to the broker-managed provider pool.

### 3.2 Supported Adapters

All 16+ SQLSpec adapters work as external providers:

| Adapter | Config Class |
|---------|--------------|
| asyncpg | `AsyncpgConfig` |
| psycopg (async) | `PsycopgAsyncConfig` |
| psycopg (sync) | `PsycopgSyncConfig` |
| psqlpy | `PsqlpyConfig` |
| aiosqlite | `AiosqliteConfig` |
| sqlite3 | `SqliteConfig` |
| asyncmy | `AsyncmyConfig` |
| mysql-connector | `MysqlConnectorConfig` |
| DuckDB | `DuckDBConfig` |
| Oracle (oracledb) | `OracleAsyncConfig` |
| BigQuery | `BigQueryConfig` |
| ADBC | `ADBCConfig` |
| Spanner | `SpannerConfig` |

## 4. Exposed Drivers (Plain SQLAlchemy)

For plain SQLAlchemy users, Vertebra exposes its **internal drivers only** (Bun postgres, Rust mysql).

**Important Limitation**: Plain SQLAlchemy users **cannot** register their own adapters with the broker. They can only use the drivers Vertebra exposes. To plug custom adapters into the broker fabric, use Advanced Alchemy.

### 4.1 Available Exposed Drivers

| Dialect URL | Internal Driver | Bun API |
|-------------|-----------------|---------|
| `vertebra+postgres://` | Bun Postgres | `Bun.SQL` with `adapter: "postgres"` |
| `vertebra+mysql://` | Bun MySQL | `Bun.SQL` with `adapter: "mysql"` |
| `vertebra+sqlite://` | Bun SQLite | `bun:sqlite` Database |

### 4.2 Usage

```python
from sqlalchemy import create_engine, text

# Uses Vertebra's Bun postgres driver
engine = create_engine("vertebra+postgres://localhost/db")

with engine.connect() as conn:
    result = conn.execute(text("SELECT 1"))
```

### 4.3 For Custom Adapters

If you need to use a driver not exposed by Vertebra (Oracle, Snowflake, etc.) and want it to go through the broker fabric:

**Use Advanced Alchemy provider plugins** (registered via `plugins=[...]`) - this gives full IPC integration with any SQLAlchemy-compatible driver.

## 5. SQLSpec Integration

SQLSpec configs are hosted by provider plugins and accessed via proxy configs. Register a provider plugin with the broker for pooling, fairness, and routing.

### 5.1 Provider Plugin (Simple)

Use any SQLSpec config as-is, wrap it in a provider plugin:

```python
from vertebra import Vertebra
from vertebra.resources import ProviderConfig
from vertebra.extensions.sqlspec.provider import SqlSpecProvider
from sqlspec.adapters.asyncpg import AsyncpgConfig

config = AsyncpgConfig(
    host="localhost",
    port=5432,
    database="mydb",
    user="user",
    password="pass",
    pool_size=20,
)

provider = SqlSpecProvider(
    config,
    ProviderConfig(pool="postgres_main", driver="asyncpg"),
)

app = Vertebra(plugins=[provider])
await app.serve()

# Client-side usage via SQLSpec proxy config
async with config.provide_session() as session:
    result = await session.execute("SELECT * FROM users")
    df = await result.to_pandas()
```

### 5.2 How It Works

When you run a SQLSpec provider plugin with Vertebra:

1. **Broker intercepts** `provide_session()` calls
2. **Routes through IPC** to the pool manager
3. **Applies fairness** scheduling across all workers
4. **Executes via SQLSpec** driver (the real one you registered)
5. **Returns results** via shared memory (Arrow) or MsgPack

```
┌─────────────────────────────────────────────────────────┐
│                    Your Python App                       │
│                                                          │
│  config = AsyncpgConfig(...)                            │
│  provider = SqlSpecProvider(config, ProviderConfig(...))│
│  app = Vertebra(plugins=[provider])                     │
│                                                          │
│  async with config.provide_session() as session:        │
│      result = await session.execute(sql)  ───────────┐  │
│                                                       │  │
└───────────────────────────────────────────────────────┼──┘
                                                        │
                                                        ▼
┌─────────────────────────────────────────────────────────┐
│                   Vertebra Broker                        │
│                                                          │
│  • Applies fairness scheduling                          │
│  • Manages connection pool                              │
│  • Routes to registered SQLSpec driver                  │
│  • Returns results via Arrow/MsgPack                    │
└─────────────────────────────────────────────────────────┘
```

### 5.3 All 16+ SQLSpec Adapters Supported

Any SQLSpec config works:

```python
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.psycopg import PsycopgAsyncConfig
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.asyncmy import AsyncmyConfig
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.oracledb import OracleAsyncConfig
from sqlspec.adapters.bigquery import BigQueryConfig
# ... etc.

app = Vertebra()

# Register multiple pools
app = Vertebra(
    plugins=[
        SqlSpecProvider(AsyncpgConfig(...), ProviderConfig(pool="postgres", driver="asyncpg")),
        SqlSpecProvider(AsyncmyConfig(...), ProviderConfig(pool="mysql", driver="asyncmy")),
        SqlSpecProvider(DuckDBConfig(...), ProviderConfig(pool="analytics", driver="duckdb")),
        SqlSpecProvider(OracleAsyncConfig(...), ProviderConfig(pool="oracle", driver="oracledb")),
    ],
)
```

### 5.4 With Litestar Plugin

For Litestar apps, add the Litestar plugin and run providers alongside:

```python
from litestar import Litestar
from vertebra.extensions.litestar import VertebraPlugin
from sqlspec.adapters.asyncpg import AsyncpgConfig

config = AsyncpgConfig(
    host="localhost",
    database="mydb",
    pool_size=20,
)

plugin = VertebraPlugin()

app = Litestar(plugins=[plugin])

# In route handlers, use SQLSpec normally
@get("/users")
async def get_users(db: AsyncpgConfig) -> list[User]:
    async with db.provide_session() as session:
        return await session.execute("SELECT * FROM users")
```

### 5.5 Zero-Copy Arrow Results

SQLSpec's Arrow support works through Vertebra:

```python
async with config.provide_session() as session:
    # Returns Arrow table via shared memory (zero-copy)
    table = await session.select_to_arrow("SELECT * FROM big_table")

    # Convert to pandas/polars as needed
    df = table.to_pandas()
    pl_df = table.to_polars()
```

### 5.6 Storage Integration

SQLSpec's storage methods work transparently:

```python
async with config.provide_session() as session:
    result = await session.execute("SELECT * FROM data")

    # Write to storage (local, S3, GCS)
    await result.write_to_storage_async("s3://bucket/export.parquet")
```

### 5.7 Tracing & Correlation

SQLSpec includes a correlation ID for tracing. Vertebra should propagate this value into
the unified trace context:

- Map SQLSpec correlation IDs into the same `trace_id` used by HTTP/MCP logs.
- Preserve existing `X-Request-ID` / `traceparent` when present; SQLSpec fills gaps.

## 6. Advanced Alchemy Integration

Advanced Alchemy configs work through provider plugins (last feature). Register the provider, use AA normally.

### 6.1 Direct Registration

```python
from vertebra import Vertebra
from vertebra.resources import ProviderConfig
from advanced_alchemy.providers import AdvancedAlchemyProvider
from advanced_alchemy.config import SQLAlchemyAsyncConfig

config = SQLAlchemyAsyncConfig(
    connection_string="postgresql+asyncpg://user:pass@localhost/db",
)

provider = AdvancedAlchemyProvider(
    config,
    ProviderConfig(pool="main", driver="advanced-alchemy"),
)

app = Vertebra(plugins=[provider])

# Use AA normally - Repository/Service patterns just work
class UserRepository(SQLAlchemyAsyncRepository[User]):
    model_type = User

class UserService(SQLAlchemyAsyncRepositoryService[User]):
    repository_type = UserRepository
```

### 6.2 With Litestar Plugin

```python
from litestar import Litestar
from vertebra.extensions.litestar import VertebraPlugin
from advanced_alchemy.config import SQLAlchemyAsyncConfig
from advanced_alchemy.extensions.litestar import SQLAlchemyPlugin

aa_config = SQLAlchemyAsyncConfig(
    connection_string="postgresql+asyncpg://localhost/db",
)

plugin = VertebraPlugin()

app = Litestar(plugins=[plugin, SQLAlchemyPlugin(config=aa_config)])

# Route handlers use AA dependency injection as normal
@get("/users/{user_id:int}")
async def get_user(user_id: int, service: UserService) -> User:
    return await service.get(user_id)
```

### 6.3 File Object Storage

AA's File Object Storage routes through Vertebra automatically:

```python
from advanced_alchemy.types import FileObject

class Document(Base):
    __tablename__ = "documents"
    id: Mapped[int] = mapped_column(primary_key=True)
    file: Mapped[FileObject] = mapped_column(FileObject)

# Files stored via Vertebra's storage layer
doc = Document(file=uploaded_file)
session.add(doc)
await session.commit()  # File saved to configured backend
```

## 7. Result Transport

All providers (internal and external) return results via:

### 7.1 MsgPack (Default)

For small results (< 1MB by default):
- Serialize rows + metadata to MsgPack
- Send through IPC channel
- Client deserializes

### 7.2 Shared Memory Arrow (Large Results)

For large results:
1. Provider writes Arrow RecordBatch to shared memory
2. Sends buffer handle through IPC
3. Client reconstructs Table from shared memory (zero-copy)

```python
# Client requesting Arrow result
result = await session.execute("SELECT * FROM large_table", want_arrow=True)
table = result.to_arrow()  # Zero-copy from shared memory
df = table.to_pandas()     # Can optionally convert
```

## 8. Broker Protocol Extensions

### 8.1 Provider Registration

```
REGISTER_PROVIDER {
    name: string,           # Pool name
    type: "external",       # Always "external" for Python workers
    dialect: string,        # postgres, oracle, mysql, etc.
    capacity: int,          # Max concurrent requests
    features: string[],     # ["arrow", "streaming", "transactions"]
}

REGISTERED {
    provider_id: string,
    config: {...},
}
```

### 8.2 Execute Protocol

```
EXECUTE {
    pool: string,
    sql: string,
    params: object | array,
    flags: {
        want_arrow: bool,       # Return Arrow buffer handle
        want_description: bool, # Include column metadata
        timeout_ms: int,
    },
    transaction_id: string | null,
}

RESULT {
    request_id: string,
    status: "ok" | "error",
    data: [...] | null,          # MsgPack rows if not Arrow
    arrow_handle: string | null, # Shared memory handle if Arrow
    description: [...] | null,   # Column metadata
    rowcount: int,
    error: string | null,
}
```

## 9. Configuration (Python-First)

All configuration is done via Python. TOML/YAML config files may be added later as convenience.

### 9.1 Broker Configuration

```python
# app.py
from vertebra import Vertebra
from vertebra.config import BrokerConfig, PoolConfig

app = Vertebra(
    broker=BrokerConfig(
        name="vertebra-broker",
        arrow_threshold_bytes=1_048_576,  # 1MB
    ),
)

# Register internal provider (Bun postgres)
app.register_pool(
    PoolConfig(
        name="postgres_main",
        type="internal",
        driver="bun-postgres",
        connection_string="postgres://user:pass@localhost:5432/db",
        max_connections=20,
    )
)
```

### 9.2 Registering SQLSpec Providers

```python
from vertebra.resources import ProviderConfig
from vertebra.extensions.sqlspec.provider import SqlSpecProvider
from sqlspec.adapters.asyncpg import AsyncpgConfig

config = AsyncpgConfig(
    host="localhost",
    port=5432,
    database="mydb",
    pool_size=20,
)

app.register_plugin(SqlSpecProvider(config, ProviderConfig(pool="postgres", driver="asyncpg")))
```

### 9.3 Registering Advanced Alchemy Providers

```python
from advanced_alchemy.config import SQLAlchemyAsyncConfig
from advanced_alchemy.providers import AdvancedAlchemyProvider
from vertebra.resources import ProviderConfig

config = SQLAlchemyAsyncConfig(
    connection_string="postgresql+asyncpg://localhost/db",
)

app.register_plugin(AdvancedAlchemyProvider(config, ProviderConfig(pool="main", driver="advanced-alchemy")))
```

### 9.4 Future: TOML Configuration

TOML support may be added later for static pool definitions:

```toml
# vertebra.toml (future)
[pools.postgres_main]
driver = "bun-postgres"
connection_string = "postgres://..."
```

## 10. fsspec Integration (Local-first)

**Goal**: Expose `vertebra.storage` through an `fsspec`-compatible filesystem.

**Adapter**:
- Implement a `VertebraFileSystem` that subclasses `fsspec.AbstractFileSystem`.
- Map core methods (`open`, `ls`, `info`, `exists`, `mkdir`, `rm`) to storage APIs.

**Testing**:
- Local backend required for MVP.
- Optional S3/GCS coverage should be gated by env vars and skipped/xfail when not configured.
