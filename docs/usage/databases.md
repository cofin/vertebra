# Databases & Native Drivers

Vertebra pairs native database drivers in `vertebra.db` with SQLSpec adapters in `vertebra.extensions.sqlspec`. Each engine lives in its own subpackage, so importing the database package stays fast until you request a specific driver.

See the focused guides for [Cloud SQL & AlloyDB Dialers](cloud-dialers.md), [RLS, CDC & Arrow Analytics](analytics-and-cdc.md), and [SQLSpec & SQLAlchemy Extensions](extensions.md).

## Optional Engine Extras

Core database configuration contracts ship in `vertebra-core`. Install only the engine extras you need:

```bash
uv add "vertebra-core[postgres]"
```

Available extras cover PostgreSQL (`postgres`), MySQL (`mysql`), SQL Server (`mssql`), Oracle (`oracle`), SQLite (`sqlite`), DuckDB (`duckdb`), BigQuery (`bigquery`), Spanner (`spanner`), Redis (`redis`), and Valkey (`valkey`), plus optional SQLAlchemy dialect add-ons (`sqlalchemy-duckdb` and `sqlalchemy-gcp`).

All native drivers work on standard and free-threaded Python without external C client libraries like libpq or Oracle Instant Client. SQLite bundles SQLite 3.45+. Requesting a driver whose extra is not installed raises `MissingDependencyError`.

## Driver Interface

All SQL and analytical drivers pair a configuration struct with sync and async drivers that share one execution interface:

- **Statements and tuples**: Execute DML statements, batch parameter sets, or fetch raw tuple rows with optional column metadata.
- **PEP 249 cursors**: Calling `driver.cursor()` returns a standard database cursor returning tuple rows; async drivers expose awaitable cursor methods.
- **Typed rows and scalars**: Query methods decode rows directly into a target `schema_type` model. Single-row queries raise `NotFoundError` when zero rows match; scalar queries return None for a single SQL NULL.
- **Transactions, cancellation, and Arrow**: Use `driver.transaction()` as a context manager, cancel running queries with `cancel()`, and move bulk data with Arrow streaming methods.
- **Capability protocols**: Use `isinstance` with runtime-checkable protocols on `vertebra.db` (such as `SyncPipelineCapable` and `AsyncPipelineCapable`) to detect optional features like pipelining, notifications, change streams, queue stores, and embedding pushdown.

```python
from vertebra.db import (
    DatabaseRegistry,
    SyncPipelineCapable,
    provide_driver,
)

registry = DatabaseRegistry()
registry.register("warehouse", "sqlite:///:memory:")
db = provide_driver("warehouse", sync=True, registry=registry)

with db.transaction():
    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
    db.execute("INSERT INTO items (id, name) VALUES (?, ?)", (1, "bolt"))

row = db.select_one("SELECT id, name FROM items WHERE id = ?", (1,))
supports_pipelining = isinstance(db, SyncPipelineCapable)
db.close()
```

### DB-API 2.0

`vertebra.db.dbapi` exposes PEP 249 sync and async connections backed by Vertebra's native drivers without importing SQLAlchemy or extension modules. Calling `cursor()` returns the engine's native cursor, where a None description indicates the statement returned no rows.

DB-API exception names alias the driver exception classes directly, so specific errors such as `QueryCancelledError` propagate unchanged. Note one PEP 249 difference: `InterfaceError` subclasses `DatabaseError`.

BigQuery, Spanner, and Spangres manage transactions at statement or RPC boundaries. Passing `autocommit=False` to a DB-API connection for those engines raises `NotSupportedError`.

## SQLSpec & Litestar Integration

Pass an adapter config to `create_sqlspec_plugin` to wire connection pooling, statement caching, and dependency injection into Litestar:

```python
from litestar import Litestar, get
from vertebra.extensions.sqlspec import (
    PostgresAsyncConfig,
    PostgresAsyncDriver,
    create_sqlspec_plugin,
)

db_config = PostgresAsyncConfig(
    dsn="postgresql://app_user:secret@localhost:5432/production_db",
    min_size=5,
    max_size=20,
)

plugin = create_sqlspec_plugin(db_config)


@get("/orders/{order_id:int}")
async def get_order(
    order_id: int,
    db_session: PostgresAsyncDriver,
) -> dict:
    """Fetch order record using injected SQLSpec driver session."""
    rows = await db_session.select(
        "SELECT id, customer_id, total_cents FROM orders WHERE id = :id",
        {"id": order_id},
    )
    return rows[0] if rows else {}


app = Litestar(
    route_handlers=[get_order],
    plugins=[plugin],
)
```

## Supported Engines

| Engine | Extra | Native Config | Key Capabilities |
| --- | --- | --- | --- |
| **PostgreSQL** | `postgres` | `PostgreSQLConfig` | Pipelining, binary COPY, pgvector, JSONB, logical replication CDC, embedding pushdown, PgBouncer modes, IAM and Cloud SQL / AlloyDB dialers |
| **MySQL / MariaDB** | `mysql` | `MySQLConfig` | Binary prepared statements, pipelining, Arrow streaming, out-of-band query cancellation, IAM and Cloud SQL mTLS dialers |
| **SQLite / libSQL** | `sqlite` | `SQLiteConfig` | Bundled SQLite 3.45+, WAL reader pool, binary JSONB, delayed-commit pipelines, online backup/restore, Arrow streaming |
| **Oracle 26ai** | `oracle` | `OracleConfig` | Thin driver (no Instant Client), native pooling with DRCP, Cloud Wallet mTLS and IAM auth, 26ai VECTOR and OSON, VPD tenancy, TxEventQ notifications, Arrow streaming |
| **SQL Server** | `mssql` | `MSSQLConfig` | TDS 7.4, automatic parameter rewriting, bulk load, query cancellation, pipelining, Arrow streaming, Cloud SQL mTLS |
| **DuckDB** | `duckdb` | `DuckDBConfig` | Single-writer/multi-reader OLAP, savepoints, pipelining, rotating GCS httpfs secrets, Arrow streaming and Appender |
| **Google BigQuery** | `bigquery` | `BigQueryConfig` | Storage Read/Write API with Arrow streaming, Object Tables, dry-run cost ceilings, ADK and Litestar stores |
| **Spanner** | `spanner` | `SpannerConfig` | GoogleSQL and Spangres, multiplexed sessions, automatic Aborted retry, BatchWrite, Data Boost partitioned Arrow reads, change streams |
| **Redis** | `redis` | `RedisConfig` | RESP3, atomic pipelines, client-side tracking cache invalidation, Sentinel, Cluster |
| **Valkey** | `valkey` | `ValkeyConfig` | RESP3 client-side caching, Valkey 9.0 hash-field expiration, redirect capability, Sentinel, Cluster, Memorystore IAM auth |
| **DataFusion** | — | `DataFusionConfig` | Federated SQL across SQL engines and Parquet/CSV object storage |

Note that `memory://` is not a database engine URL:

- MySQL configs and `PostgreSQLConfig.from_url("memory://")` raise `ImproperConfigurationError` immediately.
- Direct DSN construction on PostgreSQL, Redis, and Valkey accepts the string, then raises `ImproperConfigurationError` when initializing a driver or store factory.
- Parsing it through `DatabaseRegistry.parse_url` raises `ValueError`.
- In-process components (queues, object storage, and cache backends) continue to accept `memory://`.

### Spanner and BigQuery

The Spanner and BigQuery drivers support native Arrow streaming and local emulator workflows:

- **Partitioned Arrow reads and writes**: Spanner executes Data Boost partitioned queries in `select_to_arrow` and falls back to a single stream when a query is not root-partitionable. BigQuery streams Arrow reads and writes through the Storage Read and Write APIs.
- **Spanner DDL endpoint**: Admin DDL runs over REST. When `admin_endpoint` is omitted, Vertebra targets the standard Google endpoint for Google hosts and automatically maps the emulator port 9010 to REST port 9020. Other custom endpoints must set `admin_endpoint` explicitly.
- **Custom CA certificates**: `SpannerConfig.ca_pem` is for Spanner Omni or private HTTPS endpoints only; passing it with a Google or plain HTTP endpoint raises an error. `BigQueryConfig.ca_pem` applies to REST API calls only, while Storage Read and Write gRPC streams use system and Mozilla trust roots.
- **GoogleSQL placeholders**: In GoogleSQL mode, positional `$1..$N` placeholders outside strings, identifiers, and comments are rewritten to `@p1..@pN`.

```python
from vertebra.db.adapters.spanner import SpannerConfig, SyncSpannerDriver

emulator_config = SpannerConfig(
    project_id="local-project",
    instance_id="test-instance",
    database_id="app-db",
    endpoint="http://localhost:9010",
)
driver = SyncSpannerDriver(emulator_config)
```

### PostgreSQL

Install the `postgres` extra for native PostgreSQL support, including pipelining, typed positional parameter wrappers, logical replication CDC, and vector embedding pushdown:

```python
import msgspec
from vertebra.db.adapters.postgres import (
    AsyncPostgreSQLDriver,
    PgJson,
    PgParameter,
    PgVector,
    PostgreSQLConfig,
)
from vertebra.db.adapters.postgres.embeddings import PostgresEmbeddingPushdown
from vertebra.embeddings import PushdownEmbedder


class ArticleRow(msgspec.Struct):
    id: int
    metadata: dict[str, str]


config = PostgreSQLConfig(
    dsn="postgresql://app_user:secret@localhost:5432/app",
    pooler_mode="transaction",
    min_size=2,
    max_size=16,
)
async with AsyncPostgreSQLDriver(config) as driver:
    async with driver.pipeline() as pipe:
        pipe.execute(
            "INSERT INTO articles (id, metadata, embedding) VALUES ($1, $2, $3)",
            (
                1,
                PgJson({"tier": "prod"}),
                PgVector((0.12, 0.48, 0.87), kind="vector"),
            ),
        )
        pipe.select(
            "SELECT id, metadata FROM articles WHERE id = $1",
            (PgParameter(1, type_oid=23),),
        )
        _, piped_rows = await pipe.results()

    articles = await driver.select(
        "SELECT id, metadata FROM articles WHERE id = $1",
        (1,),
        schema_type=ArticleRow,
    )
    embedder = PushdownEmbedder(
        PostgresEmbeddingPushdown(driver, dimensions=768),
        dimensions=768,
    )
```

### Oracle 26ai

Install the `oracle` extra to connect to Oracle without Instant Client:

- **Target resolution and auth**: `OracleConfig` checks explicit connect descriptors, Cloud Wallet aliases, RAC/Data Guard topology, explicit DSNs, and host/port/service settings in order. Wallet zip archives extract in memory without temporary files. Set `auth_mode="token"` for IAM or OAuth2 bearer authentication.
- **Reset relay**: Enabled by default (`enable_reset_relay=True`) as a break fallback path. Set it to False to connect directly to the resolved address.
- **Pooling and DRCP**: Native pools run each physical connection on a dedicated worker thread, support out-of-band cancellation (`QueryCancelledError`), configure DRCP pooling, and piggyback client identifier, module, action, and ECID session metadata without extra round trips.
- **26ai types and Arrow**: Decodes binary OSON JSON and VECTOR embeddings (FLOAT32, FLOAT64, INT8, BINARY), supports single-round-trip DML RETURNING out-binds, and streams Arrow batches directly.
- **Pipelines, VPD tenancy, and notifications**: Pipelines hold an ambient transaction or nested savepoint and commit only when the block exits cleanly. Tenant scopes apply trusted VPD procedures or client identifiers and evict the pooled session if reset fails. Notification sources stream TxEventQ and AQ events with reconnect backoff and liveness probes.

```python
import msgspec
from vertebra.db.adapters.oracle import AsyncOraclePool, OracleConfig
from vertebra.db.adapters.oracle.config import OracleDrcpConfig
from vertebra.db.adapters.oracle.notify import OracleNotifyConfig
from vertebra.db.rls import AsyncTenantConnectionScope, TenantScopeConfig


class DocumentChunk(msgspec.Struct):
    id: int
    title: str
    metadata: dict[str, str]


async with AsyncOraclePool(
    OracleConfig(
        wallet_zip=wallet_bytes,
        tns_alias="appdb_high",
        user="app_user",
        password="secret",
        drcp=OracleDrcpConfig(cclass="APP_POOL", purity="SELF"),
        context_namespace="APP_CTX",
        context_package_proc="APP_SEC.CTX_PKG.SET_TENANT",
        context_clear_proc="APP_SEC.CTX_PKG.CLEAR_TENANT",
    ),
    min_size=2,
    max_size=16,
) as pool:
    async with (
        pool.lease() as driver,
        AsyncTenantConnectionScope(
            driver,
            "tenant-acme",
            TenantScopeConfig(setting_name="tenant_id"),
        ),
    ):
        async with driver.pipeline() as pipe:
            pipe.execute(
                "INSERT INTO doc_chunks (id, title, metadata, embedding) "
                "VALUES (:id, :title, :meta, :vec)",
                {
                    "id": 1,
                    "title": "Vector Guide",
                    "meta": {"tier": "prod"},
                    "vec": [0.12, 0.48, 0.87],
                },
            )
            pipe.select("SELECT COUNT(*) AS total FROM doc_chunks")
            _, totals = await pipe.results()

        top_chunks = await driver.select(
            "SELECT id, title, metadata FROM doc_chunks "
            "ORDER BY VECTOR_DISTANCE(embedding, :query_vec, COSINE) "
            "FETCH FIRST 5 ROWS ONLY",
            {"query_vec": [0.10, 0.50, 0.85]},
            schema_type=DocumentChunk,
        )
        notifications = driver.notification_source(
            OracleNotifyConfig(queue_prefix="VTB_AQ_", wait_timeout_seconds=5),
        )
        await notifications.subscribe(["doc_events"])
        await notifications.publish("doc_events", b'{"doc_id": 1}')
        await notifications.close()
```

### SQLite

Install the `sqlite` extra for a bundled SQLite 3.45+ engine safe on free-threaded Python:

- **WAL reader pool**: Runs in WAL mode by default with one dedicated writer thread and configurable concurrent readers. Write transactions and in-memory databases always run on the writer thread.
- **Pipelines**: `driver.pipeline()` queues operations and executes them in one batch inside an ambient transaction or nested savepoint.
- **Online backup and restore**: `backup_to` and `restore_from` step through pages (256 pages per step by default) without blocking concurrent readers.
- **Arrow streaming**: `select_to_arrow()` writes query results straight to a shared-memory Arrow cursor.

```python
import msgspec
from vertebra.db.adapters.sqlite import AsyncSQLiteDriver, SQLiteConfig


class UserRow(msgspec.Struct):
    id: int
    name: str


config = SQLiteConfig(
    path="app.db",
    wal=True,
    busy_timeout_ms=5000,
    max_readers=4,
)
async with AsyncSQLiteDriver(config) as driver:
    async with driver.pipeline() as pipe:
        pipe.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        pipe.execute("INSERT INTO users (id, name) VALUES (?, ?)", (1, "Ada"))
        pipe.select("SELECT id, name FROM users WHERE id = ?", (1,))
        _, _, rows = await pipe.results()

    user = await driver.select_one(
        "SELECT id, name FROM users WHERE id = ?",
        (1,),
        schema_type=UserRow,
    )
    await driver.backup_to("backup.db")
```

### Redis and Valkey

Install the `redis` or `valkey` extra for native RESP3 drivers with pipelining, client-side cache tracking invalidation, Sentinel, and Cluster routing.

Redis example:

```python
from vertebra.db.adapters.redis import AsyncRedisDriver, RedisConfig

config = RedisConfig.from_url("redis://localhost:6379/0")
async with AsyncRedisDriver(config) as driver:
    await driver.set("vtb:user:1", b'{"name": "Alice"}', ex=300)
    user_bytes = await driver.get("vtb:user:1")
    notifications = driver.notification_source()
    await notifications.subscribe(["user_events"])
```

Valkey adds support for Valkey 9.0 hash-field expiration, client redirect capability, and Google Cloud Memorystore IAM auth:

```python
from vertebra.db.adapters.valkey import AsyncValkeyDriver, ValkeyConfig

config = ValkeyConfig.from_url("valkey://localhost:6379/0")
async with AsyncValkeyDriver(config) as driver:
    await driver.set("vtb:user:1", b'{"name": "Alice"}', ex=300)
    user_bytes = await driver.get("vtb:user:1")
    await driver.execute_command(
        "HSET", "vtb:session:1", "token", "xyz", "ip", "127.0.0.1"
    )
    await driver.hexpire("vtb:session:1", 3600, "token")
    notifications = driver.notification_source()
    await notifications.subscribe(["user_events"])
```

### Errors and Cancellation

All database drivers raise exceptions rooted at `DatabaseError`, carrying SQLSTATE, vendor code, and an `is_transient` retry flag:

- **SQL errors**: Common subclasses include `NotFoundError`, `IntegrityError`, `OperationalError`, `ConnectionFailure`, `PoolExhaustedError`, `QueryCancelledError`, and `SerializationFailure`. See the [database API reference](../reference/db.md) for the full hierarchy.
- **Google Cloud errors**: `GoogleCloudError` and `GoogleCloudAuthError` expose gRPC status codes, HTTP status, and server retry delays.
- **Cloud DB dual inheritance**: `SpannerError` and `BigQueryError` inherit from both `GoogleCloudError` and `DatabaseError`. `SpannerAbortedError` also subclasses `SerializationFailure`, and `BigQueryRowError` subclasses `DataError` with per-row failure details on streaming writes.

```python
from vertebra._vertebra import SpannerAbortedError, SpannerError
from vertebra.db import DatabaseError, NotFoundError

try:
    driver.execute(
        "UPDATE singers SET active = TRUE WHERE singer_id = @id",
        {"id": 1},
    )
except SpannerAbortedError as exc:
    retry_ms = exc.retry_delay_ms or 10
except (SpannerError, NotFoundError, DatabaseError) as exc:
    is_retryable = exc.is_transient
```

Cancelling an awaited async driver call aborts its background task at the next suspension point. Requests already sent on the wire may still finish on the server.

## Statement Pipelining

Drivers that implement pipelining expose `driver.pipeline()`:

```python
with driver.pipeline() as pipe:
    pipe.select("SELECT id FROM orders WHERE account_id = $1", (account_id,))
    pipe.select(
        "SELECT COUNT(*) AS total FROM orders WHERE account_id = $1", (account_id,)
    )
    order_rows, total_rows = pipe.results()
```

Async drivers use an async context manager and await `pipe.results()`. Results come back in queue order, one entry per statement.

SQLSpec `execute_stack` automatically uses native pipelining when a stack has multiple execute operations and `continue_on_error=False`. Other stacks run sequentially inside a transaction. When `continue_on_error=True`, each operation commits or rolls back independently so later operations still run. Spanner keeps native batch DML for all-DML stacks, and BigQuery keeps its session transaction behavior.

## Topic Guides

| Guide | What You Will Learn |
| --- | --- |
| **[Cloud SQL & AlloyDB Dialers](cloud-dialers.md)** | Direct mTLS and IAM auth for Cloud SQL, AlloyDB, AWS RDS, Aurora DSQL, and Azure Entra ID. |
| **[RLS, CDC & Arrow Analytics](analytics-and-cdc.md)** | Multi-tenant row-level security, logical replication change feeds, and shared-memory Arrow queries. |
| **[SQLSpec, SQLAlchemy & Extensions](extensions.md)** | Integrations for Litestar, SQLSpec, SQLAlchemy, Advanced Alchemy, Django, gRPC, Oracle APEX, and session brokers. |
