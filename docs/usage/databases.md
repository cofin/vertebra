# Databases & Analytics

Vertebra's database architecture is built around first-class integration with **SQLSpec** and **Litestar**. Rather than introducing a custom ORM or proprietary data access pattern, Vertebra leverages SQLSpec (`vertebra.extensions.sqlspec`) as the canonical adapter interface for application data access, session lifecycle, and dependency injection, powered under the hood by native Rust drivers (`vertebra.db` / `vertebra-db`), single-RTT CTE statement stacking (`QueryPipeline`), zero-sidecar Cloud SQL and AlloyDB dialers, and zero-copy Apache Arrow IPC streaming (`ArrowCursor`).

```mermaid
flowchart LR
    A["Litestar Route Handlers"] --> B["SQLSpec Plugin & DI"]
    B --> C["SQLSpec Adapters\n(Postgres, MySQL, SQLite...)"]
    C --> D["Vertebra Native Drivers\n(Rust SHM & Broker)"]
    D --> E[("Databases & Cloud Dialers\n(Cloud SQL, AlloyDB, OLAP)")]
```

## SQLSpec & Litestar Integration

Vertebra requires SQLSpec adapters for database components. Use `SQLSpecPlugin` with an engine adapter configuration (e.g. `PostgreSQLAsyncConfig`, `MySQLAsyncConfig`, `SQLiteAsyncConfig`) to register databases on your Litestar application. The plugin manages connection leasing, shared-memory statement caches, and provides typed sessions directly to route handlers:

```python
from litestar import Litestar, get
from vertebra.extensions.sqlspec import SQLSpecPlugin
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

### Native Framework Leverage

All Vertebra framework subsystems leverage SQLSpec adapters directly:

- **AI Agent Sessions (`AgentSessionStore`)**: Persist conversation trajectories and tool calls using `SQLSpecSessionStore` over your configured SQLSpec database.
- **Model File Attachments (`ObjectStoreBackend`)**: Synchronize object storage attachments with database model transactions via Advanced Alchemy's `FileObject` / `StoredObject` integration so aborted transactions never leave orphaned blobs.
- **Replication & Events (`ChangeFeed`, Event Queues)**: Stream change feeds and coordinate worker queues using SQLSpec event store implementations.

## Architectural Invariants

Beneath the SQLSpec adapter layer, Vertebra's native database engine enforces three latency and concurrency invariants:

1. **Single-RTT Statement Stacking (`QueryPipeline`)**: Network round-trips dominate handler latency when code issues sequential reads and writes. `QueryPipeline` and `RelOverlay` compile multi-step mutations and dependent queries into a single parameterized PostgreSQL CTE statement executed in one round-trip.
2. **Pooler-Safe Connection & Statement Leasing (`DatabaseBroker`, `StatementCache`)**: Across multi-worker deployments (`--topology shm`), workers coordinate connection budgets (`PostgresConfig.compute_worker_pool_budget`) and share compiled query plans in lock-free shared memory (`StatementCache`). When running behind transaction-mode poolers (`PgBouncer`, `PgCat`, `pgdog`), `PostgresConfig.effective_statement_cache_size` automatically disables connection-pinned prepared statements unless `protocol_level_pooler_cache=True` is set.
3. **Zero-Copy Analytical Streaming (`ArrowCursor`)**: Result sets exceeding `ARROW_SHM_THRESHOLD_ROWS` (`256` rows or `64 KiB`) bypass per-row Python tuple allocation and stream directly as Apache Arrow IPC buffers in shared memory (`write_arrow_shm`, `read_arrow_shm`).

## Statement Stacking & Query Pipelines (`QueryPipeline`)

`QueryPipeline`, `CteStep`, and `RelOverlay` (`vertebra.db.pipeline`) stack multiple read and write steps into a single PostgreSQL CTE query executed in one network round-trip, with intra-statement read-your-own-writes over uncommitted CTE inserts and deletes:

```python
from vertebra.db import QueryPipeline, RelOverlay

pipeline = (
    QueryPipeline()
    .add_step(
        "inserted_order",
        "INSERT INTO orders (id, customer_id, total_cents) "
        "VALUES (:id, :customer_id, :total) RETURNING *",
        params={"id": 501, "customer_id": 9, "total": 4500},
    )
    .add_overlay(
        "orders_with_new",
        RelOverlay(
            base_table="orders",
            inserted_cte="inserted_order",
            id_column="id",
        ),
    )
    .add_step(
        "customer_orders",
        "SELECT id, total_cents FROM orders_with_new "
        "WHERE customer_id = :customer_id",
        params={"customer_id": 9},
    )
)

sql, params = pipeline.compile()
```

- **`RelOverlay`**: Generates a `UNION ALL` / `NOT IN` CTE view over a physical `base_table` and earlier `INSERT ... RETURNING *` or `DELETE ... RETURNING id` steps so subsequent CTE steps in the same statement observe uncommitted mutations immediately.
- **`QueryPipeline.build_jsonb_object`**: Automatically chunks wide column projections into concatenated `jsonb_build_object(...) || jsonb_build_object(...)` expressions so queries never trip PostgreSQL's 100-argument function limit (`max_pairs_per_call=50`).
- **SQLSpec Statement Stacking**: When using [`SQLSpecPlugin`](extensions.md) (`vertebra.extensions.sqlspec`), SQLSpec `StatementStack` pipelines and `execute_many` batches run over shared-memory leases and cached prepared statement plans (`StatementCache`).

## Native Database Drivers & Adapters Matrix

`vertebra.db` and `vertebra.extensions.sqlspec.adapters` provide synchronous and asynchronous drivers across 9 relational, analytical, and key-value engines:

| Engine | SQLSpec Adapter Config | Native Config Class | Sync / Async Drivers | Key Capabilities |
| --- | --- | --- | --- | --- |
| **PostgreSQL** | `PostgresAsyncConfig`, `PostgresSyncConfig` | `PostgresConfig` | `SyncPostgresDriver`, `AsyncPostgresDriver` | Binary `PGCOPY`, zero-copy `JSONB` (`strip_pg_jsonb_header`), PgBouncer/PgCat/pgdog pooler modes, `QueryPipeline` |
| **MySQL / MariaDB** | `MysqlAsyncConfig`, `MysqlSyncConfig` | `MysqlConfig` | `SyncMysqlDriver`, `AsyncMysqlDriver` | Bounded connection pools, binary prepared statements, Cloud SQL `:3307` dialer integration |
| **SQLite / libSQL** | `SqliteAsyncConfig`, `SqliteSyncConfig` | `SqliteConfig` | `SyncSqliteDriver`, `AsyncSqliteDriver` | WAL mode, `mmap_size`, SQLite 3.45+ binary `JSONB`, SHM single-writer/multi-reader leasing |
| **Oracle 26ai** | `OracleAsyncConfig`, `OracleSyncConfig` | `OracleConfig` | `SyncOracleDriver`, `AsyncOracleDriver` | Pure-Rust Thin TNS/TTC wire framing, 26ai `VECTOR` (`encode_vector_f32`), `OSON` binary JSON, `OracleWallet` |
| **SQL Server** | `MssqlAsyncConfig`, `MssqlSyncConfig` | `MssqlConfig` | `SyncMssqlDriver`, `AsyncMssqlDriver` | TDS 7.4 framing, state-machine `?` / `$1` to `@P1` parameter rewriter, `ArrowCursor` |
| **DuckDB** | `DuckdbAsyncConfig`, `DuckdbSyncConfig` | `DuckdbConfig` | `SyncDuckdbDriver`, `AsyncDuckdbDriver` | Embedded OLAP, Parquet/HTTP metadata caching, zero-copy Arrow IPC via shared memory |
| **Redis** | — | `RedisConfig` | `SyncRedisDriver`, `AsyncRedisDriver` | RESP3 (`HELLO 3`), `CLIENT TRACKING` local cache invalidation, Sentinel, Cluster (`CRC16-XMODEM`) |
| **Valkey** | — | `ValkeyConfig` | `SyncValkeyDriver`, `AsyncValkeyDriver` | Valkey 8.0+/9.0+ RESP3 client-side caching, Cluster/Sentinel, GCP Memorystore IAM auth |
| **DataFusion** | — | `DataFusionConfig` | `DataFusionEngine` | Federated SQL across PostgreSQL, MySQL, SQLite, DuckDB, and Parquet/CSV object storage |

## Cloud SQL & AlloyDB Dialers (`vertebra.db.dialer`)

Connect directly to Google Cloud SQL (`:3307` ephemeral mTLS certificates) and Google Cloud AlloyDB (`:5433` `MetadataExchangeRequest` protobuf handshake) from Rust and Python without running an external Cloud SQL Auth Proxy sidecar container:

```python
from vertebra.db import (
    AlloyDBConfig,
    AlloyDBDialer,
    CloudSQLConfig,
    CloudSQLDialer,
)

cloudsql = CloudSQLDialer(
    CloudSQLConfig(
        instance_connection_name="my-project:us-central1:orders-db",
        enable_iam_auth=True,
    )
)
alloydb = AlloyDBDialer(
    AlloyDBConfig(
        instance_uri=(
            "projects/my-project/locations/us-central1/"
            "clusters/prod/instances/primary"
        ),
        enable_iam_auth=True,
    )
)
```

## Row-Level Security, CDC & Federated Analytics

- **Multi-Tenant Row-Level Security (`vertebra.db.rls`)**: `TenantPredicate` and `TenantScopeConfig` enforce dual-layer PostgreSQL tenant isolation by combining transaction-local session GUCs (`SELECT set_config('app.tenant_id', $1, true)`) with planner-unfenced predicates (`alias.tenant_id = $1::uuid`, casting the parameter rather than the column so B-tree indexes remain usable).
- **Change Data Capture (`vertebra.db.cdc`)**: `ChangeFeed` (`CdcConfig`, `ChangeEvent`) streams logical replication events with LSN checkpointing, `replay_since()`, and batch Parquet archival (`flush_to_parquet()`).
- **Federated Queries & Arrow SHM (`DataFusionEngine`, `ArrowCursor`)**: Register external tables (`TableRegistration`) on `DataFusionEngine` and exchange zero-copy Arrow record batches through shared memory (`write_arrow_shm`, `read_arrow_shm`).
- **API Reference**: See [`vertebra.db` Reference](../reference/db.md) and [`vertebra.extensions` Reference](../reference/extensions.md) for full configuration fields and driver methods.
