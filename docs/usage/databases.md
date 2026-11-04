# Databases & Native Drivers

Vertebra integrates first-class **SQLSpec** adapters (`vertebra.extensions.sqlspec`) with native Rust shared-memory database drivers (`vertebra.db` / `vertebra-db`). See the focused guides for [Query Pipelines & CTE Stacking](query-pipelines.md), [Cloud SQL & AlloyDB Dialers](cloud-dialers.md), [RLS, CDC & Arrow Analytics](analytics-and-cdc.md), and [SQLSpec & SQLAlchemy Extensions](extensions.md).

```mermaid
flowchart LR
    A["Route Handlers"] --> B["SQLSpec Plugin & DI"]
    B --> C["SQLSpec Adapters"]
    C --> D["Vertebra Native Drivers (Rust SHM)"]
    D --> E[("PostgreSQL / Oracle / MySQL / SQLite / DuckDB")]
```

## SQLSpec & Litestar Integration

Use `create_sqlspec_plugin` with an adapter configuration (such as `PostgreSQLAsyncConfig`, `MySQLAsyncConfig`, or `SQLiteAsyncConfig`) to manage connection leasing, shared-memory statement caches, and dependency injection:

```python
from litestar import Litestar, get
from vertebra.extensions.sqlspec import create_sqlspec_plugin
from vertebra.extensions.sqlspec.adapters import PostgreSQLAsyncConfig

db_config = PostgreSQLAsyncConfig(
    url="postgresql://app_user:secret@localhost:5432/production_db",
    pool_min_size=5,
    pool_max_size=20,
)

plugin = create_sqlspec_plugin(db_config)


@get("/orders/{order_id:int}")
async def get_order(
    order_id: int,
    db: PostgreSQLAsyncConfig.driver_type,
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

`vertebra.db.adapters` and `vertebra.extensions.sqlspec.adapters` provide synchronous and asynchronous drivers across 11 relational, analytical, cloud-native, and key-value engines:

| Engine | SQLSpec Adapter Config | Native Config Class | Sync / Async Drivers | Key Capabilities |
| --- | --- | --- | --- | --- |
| **PostgreSQL** | `PostgreSQLAsyncConfig`, `PostgreSQLSyncConfig` | `PostgreSQLConfig` | `SyncPostgreSQLDriver`, `AsyncPostgreSQLDriver` | Binary `PGCOPY`, binary `pgvector` (`vector`/`halfvec`/`bit`), zero-copy `JSONB`, PgBouncer/PgCat/pgdog pooler modes, direct `:5432` IAM OAuth2 password or `:5433` AlloyDB / `:3307` Cloud SQL dialers |
| **MySQL / MariaDB** | `MySQLAsyncConfig`, `MySQLSyncConfig` | `MySQLConfig` | `SyncMySQLDriver`, `AsyncMySQLDriver` | Bounded connection pools, binary prepared statements, direct `:3306` IAM OAuth2 password or Cloud SQL `:3307` dialer integration |
| **SQLite / libSQL** | `SQLiteAsyncConfig`, `SQLiteSyncConfig` | `SQLiteConfig` | `SyncSQLiteDriver`, `AsyncSQLiteDriver` | WAL mode, `mmap_size`, SQLite 3.45+ binary `JSONB`, SHM single-writer/multi-reader leasing |
| **Oracle 26ai** | `OracleAsyncConfig`, `OracleSyncConfig` | `OracleConfig` | `SyncOracleDriver`, `AsyncOracleDriver` | Pure-Rust Thin TNS/TTC wire framing, zero-copy `PyBuffer` 26ai `VECTOR` (`0xDB`), `OSON` binary JSON, `OracleWallet` |
| **SQL Server** | `MSSQLAsyncConfig`, `MSSQLSyncConfig` | `MSSQLConfig` | `SyncMSSQLDriver`, `AsyncMSSQLDriver` | TDS 7.4/8.0 framing, `?` / `$1` to `@P1` parameter rewriter, `ArrowCursor` |
| **DuckDB** | `DuckDBAsyncConfig`, `DuckDBSyncConfig` | `DuckDBConfig` | `SyncDuckDBDriver`, `AsyncDuckDBDriver` | Single-writer/multi-reader OLAP, active ADC `httpfs` secret rotation, zero-copy Arrow SHM |
| **Google BigQuery** | `BigQueryAsyncConfig`, `BigQuerySyncConfig` | `BigQueryConfig` | `SyncBigQueryDriver`, `AsyncBigQueryDriver` | Pure-Rust REST v2 + Storage Read/Write v1 (`VTBARROW` zero-copy Arrow SHM), `ObjectTableConfig` (`ObjectRef`), `dry_run` cost ceiling, ADK/Litestar/Events/Artifact stores |
| **Google Cloud Spanner** | `SpannerAsyncConfig`, `SpannerSyncConfig` | `SpannerConfig` | `SyncSpannerDriver`, `AsyncSpannerDriver` | Pure-Rust GoogleSQL + `Spangres`, cached `multiplexed = true` sessions, wound-wait `Aborted` retry, `BatchWrite`, Data Boost `PartitionQuery` (`VTBARROW`), `SyncSpannerChangeStreamSource` / `AsyncSpannerChangeStreamSource` |
| **Redis** | — | `RedisConfig` | `SyncRedisDriver`, `AsyncRedisDriver` | RESP3 (`HELLO 3`), `CLIENT TRACKING` local cache invalidation, Sentinel, Cluster (`CRC16-XMODEM`) |
| **Valkey** | — | `ValkeyConfig` | `SyncValkeyDriver`, `AsyncValkeyDriver` | Valkey 8.0+/9.0+ RESP3 client-side caching, Cluster/Sentinel, GCP Memorystore IAM auth |
| **DataFusion** | — | `DataFusionConfig` | `DataFusionEngine` | Federated SQL across PostgreSQL, MySQL, SQLite, DuckDB, BigQuery, Cloud Spanner, and Parquet/CSV object storage |

### Google Cloud Spanner and BigQuery

The native Spanner and BigQuery drivers delegate gRPC Data, Database Admin, Storage Read, and Storage Write calls to the official Google Cloud Rust clients (`google-cloud-spanner` and `google-cloud-bigquery`):

- **Partitioned Arrow Reads (`fetch_arrow`)**: `SyncSpannerDriver.fetch_arrow` and `AsyncSpannerDriver.fetch_arrow` execute a Data Boost partitioned query (`partitioned_query`), automatically falling back to a single-stream query when the statement is not root-partitionable, and materialize the rows into a shared-memory `ArrowCursor` (`VTBARROW`).
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

### Error Handling for Google Cloud Drivers

Both synchronous and asynchronous Spanner, BigQuery, and `SyncGcpTokenCache` / `AsyncGcpTokenCache` methods raise a unified exception hierarchy rooted at `vertebra.typing.VertebraError` and exposed on `vertebra._vertebra`:

- `GoogleCloudError(VertebraError)`: Base exception carrying `code` (`int`, canonical gRPC status `0..16`), `code_name` (`str`), `message` (`str`), `http_status` (`int | None`), `retry_delay_ms` (`int | None`), and `reason` (`str | None`).
- `GoogleCloudAuthError(GoogleCloudError)`: Raised when credentials cannot be obtained or are rejected (`code == 16`, `UNAUTHENTICATED`).
- `SpannerError(GoogleCloudError)` and `SpannerAbortedError(SpannerError)`: Raised by Cloud Spanner operations; `SpannerAbortedError` (`code == 10`, `ABORTED`) signals an aborted transaction and is the only exception retried by `run_in_transaction`.
- `BigQueryError(GoogleCloudError)` and `BigQueryRowError(BigQueryError)`: Raised by BigQuery REST and Storage Read/Write operations; `BigQueryRowError` carries per-row errors in `row_errors` (`list[tuple[int, int, str]]`).

```python
from vertebra._vertebra import SpannerAbortedError, SpannerError

try:
    driver.execute(
        "UPDATE singers SET active = TRUE WHERE singer_id = @id",
        {"id": 1},
    )
except SpannerAbortedError as exc:
    retry_ms = exc.retry_delay_ms or 10
except SpannerError as exc:
    status_code = exc.code
```

Cancelling an awaited async driver call aborts its background Rust task at the next suspension point; RPCs already transmitted on the wire may still complete server-side. In debug builds, an unexpected Rust task panic surfaces on the awaiting coroutine as `vertebra._vertebra.TaskPanicError` rather than leaving the future pending.

## Topic Guides

| Guide | What You Will Learn |
| --- | --- |
| **[Query Pipelines & CTE Stacking](query-pipelines.md)** | Stack multi-step reads and mutations into a single PostgreSQL CTE round-trip with `SyncQueryPipeline` / `AsyncQueryPipeline` and `RelOverlay`. |
| **[Cloud SQL & AlloyDB Dialers](cloud-dialers.md)** | Connect directly to Cloud SQL (`:3307`) and AlloyDB (`:5433`) with ephemeral mTLS and IAM auth without sidecars. |
| **[RLS, CDC & Arrow Analytics](analytics-and-cdc.md)** | Enforce `TenantPredicate` Row-Level Security, stream `ChangeFeed` events, and query zero-copy `ArrowCursor` tables. |
| **[SQLSpec, SQLAlchemy & Extensions](extensions.md)** | Configure `SQLSpecPlugin`, Advanced Alchemy, SQLAlchemy 2.1 `+vtb` dialects, `Vector`, and `BinaryJSON` column types. |
