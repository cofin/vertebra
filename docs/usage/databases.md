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

`vertebra.db` and `vertebra.extensions.sqlspec.adapters` provide synchronous and asynchronous drivers across 11 relational, analytical, cloud-native, and key-value engines:

| Engine | SQLSpec Adapter Config | Native Config Class | Sync / Async Drivers | Key Capabilities |
| --- | --- | --- | --- | --- |
| **PostgreSQL** | `PostgresAsyncConfig`, `PostgresSyncConfig` | `PostgresConfig` | `SyncPostgresDriver`, `AsyncPostgresDriver` | Binary `PGCOPY`, binary `pgvector` (`vector`/`halfvec`/`bit`), zero-copy `JSONB`, PgBouncer/PgCat/pgdog pooler modes, direct `:5432` IAM OAuth2 password or `:5433` AlloyDB / `:3307` Cloud SQL dialers |
| **MySQL / MariaDB** | `MysqlAsyncConfig`, `MysqlSyncConfig` | `MysqlConfig` | `SyncMysqlDriver`, `AsyncMysqlDriver` | Bounded connection pools, binary prepared statements, direct `:3306` IAM OAuth2 password or Cloud SQL `:3307` dialer integration |
| **SQLite / libSQL** | `SqliteAsyncConfig`, `SqliteSyncConfig` | `SqliteConfig` | `SyncSqliteDriver`, `AsyncSqliteDriver` | WAL mode, `mmap_size`, SQLite 3.45+ binary `JSONB`, SHM single-writer/multi-reader leasing |
| **Oracle 26ai** | `OracleAsyncConfig`, `OracleSyncConfig` | `OracleConfig` | `SyncOracleDriver`, `AsyncOracleDriver` | Pure-Rust Thin TNS/TTC wire framing, zero-copy `PyBuffer` 26ai `VECTOR` (`0xDB`), `OSON` binary JSON, `OracleWallet` |
| **SQL Server** | `MssqlAsyncConfig`, `MssqlSyncConfig` | `MssqlConfig` | `SyncMssqlDriver`, `AsyncMssqlDriver` | TDS 7.4/8.0 framing, `?` / `$1` to `@P1` parameter rewriter, `ArrowCursor` |
| **DuckDB** | `DuckdbAsyncConfig`, `DuckdbSyncConfig` | `DuckdbConfig` | `SyncDuckdbDriver`, `AsyncDuckdbDriver` | Single-writer/multi-reader OLAP, active ADC `httpfs` secret rotation, zero-copy Arrow SHM |
| **Google BigQuery** | `BigQueryAsyncConfig`, `BigQuerySyncConfig` | `BigQueryConfig` | `SyncBigQueryDriver`, `AsyncBigQueryDriver` | Pure-Rust REST v2 + Storage Read/Write v1 (`VTBARROW` zero-copy Arrow SHM), `ObjectTableConfig` (`ObjectRef`), `dry_run` cost ceiling, ADK/Litestar/Events/Artifact stores |
| **Google Cloud Spanner** | `SpannerAsyncConfig`, `SpannerSyncConfig` | `SpannerConfig` | `SyncSpannerDriver`, `AsyncSpannerDriver` | Pure-Rust GoogleSQL + `Spangres`, cached `multiplexed = true` sessions, wound-wait `Aborted` retry, `BatchWrite`, Data Boost `PartitionQuery` (`VTBARROW`), `SpannerChangeStreamSource` |
| **Redis** | — | `RedisConfig` | `SyncRedisDriver`, `AsyncRedisDriver` | RESP3 (`HELLO 3`), `CLIENT TRACKING` local cache invalidation, Sentinel, Cluster (`CRC16-XMODEM`) |
| **Valkey** | — | `ValkeyConfig` | `SyncValkeyDriver`, `AsyncValkeyDriver` | Valkey 8.0+/9.0+ RESP3 client-side caching, Cluster/Sentinel, GCP Memorystore IAM auth |
| **DataFusion** | — | `DataFusionConfig` | `DataFusionEngine` | Federated SQL across PostgreSQL, MySQL, SQLite, DuckDB, BigQuery, Cloud Spanner, and Parquet/CSV object storage |

## Topic Guides

| Guide | What You Will Learn |
| --- | --- |
| **[Query Pipelines & CTE Stacking](query-pipelines.md)** | Stack multi-step reads and mutations into a single PostgreSQL CTE round-trip with `QueryPipeline` and `RelOverlay`. |
| **[Cloud SQL & AlloyDB Dialers](cloud-dialers.md)** | Connect directly to Cloud SQL (`:3307`) and AlloyDB (`:5433`) with ephemeral mTLS and IAM auth without sidecars. |
| **[RLS, CDC & Arrow Analytics](analytics-and-cdc.md)** | Enforce `TenantPredicate` Row-Level Security, stream `ChangeFeed` events, and query zero-copy `ArrowCursor` tables. |
| **[SQLSpec, SQLAlchemy & Extensions](extensions.md)** | Configure `SQLSpecPlugin`, Advanced Alchemy, SQLAlchemy 2.1 `+vtb` dialects, `Vector`, and `BinaryJSON` column types. |
