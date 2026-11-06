# Databases & Native Drivers

Vertebra integrates first-class **SQLSpec** adapters (`vertebra.extensions.sqlspec`) with native Rust shared-memory database drivers (`vertebra.db` / `vertebra-db`). Each engine lives in its own adapter package under `vertebra.db.adapters.<engine>`, keeping `import vertebra.db` lightweight until a concrete driver is requested. See the focused guides for [Cloud SQL & AlloyDB Dialers](cloud-dialers.md), [RLS, CDC & Arrow Analytics](analytics-and-cdc.md), and [SQLSpec & SQLAlchemy Extensions](extensions.md).

```mermaid
flowchart LR
    A["Route Handlers"] --> B["SQLSpec Plugin & provide_driver"]
    B --> C["DatabaseRegistry & Adapter Configs"]
    C --> D["Vertebra Native Drivers (Rust SHM)"]
    D --> E[("PostgreSQL / Oracle / MySQL / MSSQL / SQLite / DuckDB / BigQuery / Spanner")]
```

## Optional Engine Extras

Core SQL and RESP3 configuration contracts ship in `vertebra-core`, while standalone native cloud wheels and federated engines install via optional extras:

=== "uv"

    ```bash
    uv add "vertebra-core[bigquery]"
    uv add "vertebra-core[spanner]"
    ```

=== "pip"

    ```bash
    pip install "vertebra-core[bigquery]"
    pip install "vertebra-core[spanner]"
    ```

=== "pdm"

    ```bash
    pdm add "vertebra-core[bigquery]"
    pdm add "vertebra-core[spanner]"
    ```

=== "poetry"

    ```bash
    poetry add "vertebra-core[bigquery]"
    poetry add "vertebra-core[spanner]"
    ```

## Unified Driver Contract & `provide_driver`

The PostgreSQL, MySQL, SQLite, Oracle, MSSQL, DuckDB, BigQuery, and Spanner adapters pair a `BaseDatabaseConfig` struct (`url_schemes`, `from_url`, `pool_key`) with `SyncDatabaseDriver` and `AsyncDatabaseDriver` implementations sharing a uniform execution contract:

- **DML & Tuple Execution**: `execute(sql, parameters) -> int`, `execute_many(sql, parameter_sets) -> int`, `execute_tuples(sql, parameters) -> list[tuple]`, and `execute_with_description(sql, parameters) -> tuple[list[tuple], tuple[str, ...]]`.
- **Typed Row & Scalar Queries**: `select(sql, parameters, *, schema_type=None)`, `select_one(...)` (raises `NotFoundError` when `0` rows match), `select_one_or_none(...) -> Any | None`, and `select_value(sql, parameters, *, value_type=None)` (raises `NotFoundError` on `0` rows and returns `None` for a single-row SQL `NULL`).
- **Transactions, Cancellation & Arrow SHM**: `with driver.transaction():` / `async with driver.transaction():`, `cancel()`, `select_to_arrow(...) -> ArrowCursor`, `stream_arrow_batches(...)`, and `load_from_arrow(table_name, data) -> int`.
- **Capability Protocols**: Engine-specific extensions are exposed as `@runtime_checkable` protocols on `vertebra.db` (`SyncPipelineCapable`, `AsyncPipelineCapable`, `NotificationSource`, `SyncChangeSource`, `AsyncChangeSource`, `QueueStore`, and `EmbeddingPushdown`).

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

## SQLSpec & Litestar Integration

Use `create_sqlspec_plugin` with an adapter configuration (such as `PostgreSQLAsyncConfig`, `MySQLAsyncConfig`, or `SQLiteAsyncConfig`) to manage connection leasing, shared-memory statement caches, and dependency injection:

```python
from litestar import Litestar, get
from vertebra.extensions.sqlspec import (
    PostgreSQLAsyncConfig,
    PostgreSQLAsyncDriver,
    create_sqlspec_plugin,
)

db_config = PostgreSQLAsyncConfig(
    dsn="postgresql://app_user:secret@localhost:5432/production_db",
    min_size=5,
    max_size=20,
)

plugin = create_sqlspec_plugin(db_config)


@get("/orders/{order_id:int}")
async def get_order(
    order_id: int,
    db: PostgreSQLAsyncDriver,
) -> dict:
    """Fetch order record using injected SQLSpec driver session."""
    rows = await db.select(
        "SELECT id, customer_id, total_cents FROM orders WHERE id = :id",
        {"id": order_id},
    )
    return rows[0] if rows else {}


app = Litestar(
    route_handlers=[get_order],
    plugins=[plugin],
)
```

## Native Database Drivers & Adapters Matrix

`vertebra.db.adapters` and `vertebra.extensions.sqlspec` provide synchronous and asynchronous drivers across 11 relational, analytical, cloud-native, and key-value engines:

| Engine | SQLSpec Adapter Config | Native Config Class | Sync / Async Drivers | Key Capabilities |
| --- | --- | --- | --- | --- |
| **PostgreSQL** | `PostgreSQLAsyncConfig`, `PostgreSQLSyncConfig` | `PostgreSQLConfig` | `SyncPostgreSQLDriver`, `AsyncPostgreSQLDriver` | Binary `PGCOPY`, binary `pgvector` (`vector`/`halfvec`/`bit`), zero-copy `JSONB`, PgBouncer/PgCat/pgdog pooler modes, direct `:5432` IAM OAuth2 password or `:5433` AlloyDB / `:3307` Cloud SQL dialers |
| **MySQL / MariaDB** | `MySQLAsyncConfig`, `MySQLSyncConfig` | `MySQLConfig` | `SyncMySQLDriver`, `AsyncMySQLDriver` | Bounded connection pools, binary prepared statements, direct `:3306` IAM OAuth2 password or Cloud SQL `:3307` dialer integration |
| **SQLite / libSQL** | `SQLiteAsyncConfig`, `SQLiteSyncConfig` | `SQLiteConfig` | `SyncSQLiteDriver`, `AsyncSQLiteDriver` | WAL mode, `mmap_size`, SQLite 3.45+ binary `JSONB`, SHM single-writer/multi-reader leasing |
| **Oracle 26ai** | `OracleAsyncConfig`, `OracleSyncConfig` | `OracleConfig` | `SyncOracleDriver`, `AsyncOracleDriver` | Pure-Rust Thin TNS/TTC wire framing, zero-copy `PyBuffer` 26ai `VECTOR` (`0xDB`), `OSON` binary JSON, `OracleWallet` |
| **SQL Server** | `MSSQLAsyncConfig`, `MSSQLSyncConfig` | `MSSQLConfig` | `SyncMSSQLDriver`, `AsyncMSSQLDriver` | TDS 7.4/8.0 framing, `?` / `$1` to `@P1` parameter rewriter, `ArrowCursor` |
| **DuckDB** | `DuckDBAsyncConfig`, `DuckDBSyncConfig` | `DuckDBConfig` | `SyncDuckDBDriver`, `AsyncDuckDBDriver` | Single-writer/multi-reader OLAP, active ADC `httpfs` secret rotation, zero-copy Arrow SHM |
| **Google BigQuery** | `BigQueryAsyncConfig`, `BigQuerySyncConfig` | `BigQueryConfig` | `SyncBigQueryDriver`, `AsyncBigQueryDriver` | Pure-Rust REST v2 + Storage Read/Write v1 (`VTBARROW` zero-copy Arrow SHM via `select_to_arrow` / `load_from_arrow`), `ObjectTableConfig` (`ObjectRef`), `dry_run` cost ceiling, ADK/Litestar/Events/Artifact stores |
| **Google Cloud Spanner** | `SpannerAsyncConfig`, `SpannerSyncConfig` | `SpannerConfig` | `SyncSpannerDriver`, `AsyncSpannerDriver` | Pure-Rust GoogleSQL + `Spangres`, cached `multiplexed = true` sessions, wound-wait `Aborted` retry, `BatchWrite`, Data Boost `PartitionQuery` (`select_to_arrow`), `SyncSpannerChangeStreamSource` / `AsyncSpannerChangeStreamSource` |
| **Redis** | — | `RedisConfig` | `SyncRedisDriver`, `AsyncRedisDriver` | RESP3 (`HELLO 3`), `CLIENT TRACKING` local cache invalidation, Sentinel, Cluster (`CRC16-XMODEM`) |
| **Valkey** | — | `ValkeyConfig` | `SyncValkeyDriver`, `AsyncValkeyDriver` | Valkey 8.0+/9.0+ RESP3 client-side caching, Cluster/Sentinel, GCP Memorystore IAM auth |
| **DataFusion** | — | `DataFusionConfig` | `DataFusionEngine` | Federated SQL across PostgreSQL, MySQL, SQLite, DuckDB, BigQuery, Cloud Spanner, and Parquet/CSV object storage |

### Google Cloud Spanner and BigQuery

The native Spanner (`vertebra-core[spanner]`) and BigQuery (`vertebra-core[bigquery]`) drivers delegate gRPC Data, Database Admin, Storage Read, and Storage Write calls to the official Google Cloud Rust clients (`google-cloud-spanner` and `google-cloud-bigquery`):

- **Partitioned Arrow Reads & Writes (`select_to_arrow`, `load_from_arrow`)**: `SyncSpannerDriver.select_to_arrow` and `AsyncSpannerDriver.select_to_arrow` execute a Data Boost partitioned query (`partitioned_query`), automatically falling back to a single-stream query when the statement is not root-partitionable, and materialize the rows into a shared-memory `ArrowCursor` (`VTBARROW`). `SyncBigQueryDriver.select_to_arrow` and `load_from_arrow` stream directly through BigQuery Storage Read/Write v1.
- **Spanner DDL Endpoint (`SpannerConfig.admin_endpoint`)**: Database Admin DDL (`update_database_ddl`) runs over REST. When `admin_endpoint` is omitted, Vertebra derives `https://spanner.googleapis.com` for `*.googleapis.com` endpoints and maps the Cloud Spanner Emulator's `:9010` gRPC port to its `:9020` REST port (`http://localhost:9010` → `http://localhost:9020`). Any other non-Google endpoint requires an explicit `admin_endpoint` before running DDL.
- **Custom CA Certificates (`ca_pem`)**:
  - `SpannerConfig.ca_pem` configures TLS trust for Spanner Omni or private HTTPS endpoints only; passing `ca_pem` with a `*.googleapis.com` or `http://` endpoint raises an error.
  - `BigQueryConfig.ca_pem` applies to the BigQuery REST v2 API only; Storage Read and Storage Write gRPC streams always use the system and bundled Mozilla trust roots.
- **GoogleSQL Positional Placeholders**: In GoogleSQL mode (`dialect="googlesql"`), positional `$1..$N` placeholders outside single-, double-, triple-quoted, raw, or bytes literals, backtick identifiers, and `--`, `#`, or `/* */` comments are rewritten to `@p1..@pN`.

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

### Typed `DatabaseError` Hierarchy & Google Cloud Exceptions

All relational, analytical, and cloud drivers raise typed exceptions rooted at `vertebra.db.DatabaseError(VertebraError)` with `sqlstate`, `vendor_code`, and `is_transient` metadata:

- **Core SQL Exceptions (`vertebra.db`)**: `DatabaseError`, `InterfaceError`, `DataError`, `NotFoundError(DataError, ValueError)`, `OperationalError`, `ConnectionFailure`, `PoolExhaustedError`, `QueryCancelledError`, `SerializationFailure`, `IntegrityError`, `InternalError`, `ProgrammingError`, and `NotSupportedError`.
- **Multi-Base Google Cloud Exceptions (`vertebra._vertebra`)**:
  - `GoogleCloudError(VertebraError)` and `GoogleCloudAuthError(GoogleCloudError)` carry `code` (`int`, canonical gRPC status `0..16`), `code_name`, `message`, `http_status`, `retry_delay_ms`, and `reason`.
  - `SpannerError(GoogleCloudError, DatabaseError)` and `SpannerAbortedError(SpannerError, SerializationFailure)` (`code == 10`, `ABORTED`) match both `except GoogleCloudError:` and `except DatabaseError:` handlers.
  - `BigQueryError(GoogleCloudError, DatabaseError)` and `BigQueryRowError(BigQueryError, DataError)` expose `row_errors` (`list[dict[str, Any]]`, each shaped `{"index": int, "code": int, "message": str}`) on streaming write failures.

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

Cancelling an awaited async driver call aborts its background Rust task at the next suspension point; RPCs already transmitted on the wire may still complete server-side. In debug builds, an unexpected Rust task panic surfaces on the awaiting coroutine as `vertebra._vertebra.TaskPanicError` rather than leaving the future pending.

## Native Statement Pipelining

Drivers implementing `SyncPipelineCapable` or `AsyncPipelineCapable` expose `driver.pipeline()`:

```python
with driver.pipeline() as pipe:
    pipe.select("SELECT id FROM orders WHERE account_id = $1", (account_id,))
    pipe.select(
        "SELECT COUNT(*) AS total FROM orders WHERE account_id = $1", (account_id,)
    )
    order_rows, total_rows = pipe.results()
```

The async form uses `async with driver.pipeline()` and `await pipe.results()`. Results follow queue order. Statements are independent SQL operations; each result contains its own rows or affected count.

SQLSpec `execute_stack` uses the capability when the stack contains multiple `execute` operations and `continue_on_error=False`. SQLSpec prepares each statement and its binds once, and its processed row metadata selects the native `select` or `execute` operation. Other stacks execute sequentially through the corresponding adapter methods in a transaction. With `continue_on_error=True`, each operation commits or rolls back separately, captures its error, and permits later operations to run. Spanner retains native batch DML for raw all-DML stacks; BigQuery retains its session transaction behavior.

## Topic Guides

| Guide | What You Will Learn |
| --- | --- |
| **[Cloud SQL & AlloyDB Dialers](cloud-dialers.md)** | Connect directly to Cloud SQL (`:3307`) and AlloyDB (`:5433`) with ephemeral mTLS and IAM auth without sidecars. |
| **[RLS, CDC & Arrow Analytics](analytics-and-cdc.md)** | Enforce `TenantPredicate` Row-Level Security, stream `ChangeFeed` events, and query zero-copy `ArrowCursor` tables. |
| **[SQLSpec, SQLAlchemy & Extensions](extensions.md)** | Configure `SQLSpecPlugin`, Advanced Alchemy, SQLAlchemy 2.1 `+vtb` dialects, `Vector`, and `BinaryJSON` column types. |
