# Databases & Native Drivers

Vertebra integrates first-class **SQLSpec** adapters (`vertebra.extensions.sqlspec`) on top of `vertebra.db` engine drivers. Each engine lives in its own package under `vertebra.db.adapters.<engine>`, keeping `import vertebra.db` lightweight until a concrete driver is requested. PostgreSQL, MySQL, Oracle 26ai, SQL Server, DuckDB, BigQuery, and Spanner run on standalone optional PyO3 native wheels, while SQLite, Redis, Valkey, and DataFusion run in core or protocol modules. Meanwhile, the Rust `vertebra-db` crate supplies shared wire codecs, Cloud SQL / AlloyDB dialers, and shared-memory Arrow transport. See the focused guides for [Cloud SQL & AlloyDB Dialers](cloud-dialers.md), [RLS, CDC & Arrow Analytics](analytics-and-cdc.md), and [SQLSpec & SQLAlchemy Extensions](extensions.md).

```mermaid
flowchart LR
    A["Route Handlers"] --> B["SQLSpec Plugin & provide_driver"]
    B --> C["DatabaseRegistry & Adapter Configs"]
    C --> D["Engine Drivers & vertebra-db Codecs"]
    D --> E[("PostgreSQL / MySQL / SQLite / DuckDB / BigQuery / Spanner")]
```

## Optional Engine Extras

Core SQL and RESP3 configuration contracts ship in `vertebra-core`, while standalone native cloud wheels and federated engines install via optional extras:

=== "uv"

    ```bash
    uv add "vertebra-core[postgres]"
    uv add "vertebra-core[mysql]"
    uv add "vertebra-core[mssql]"
    uv add "vertebra-core[bigquery]"
    uv add "vertebra-core[duckdb]"
    uv add "vertebra-core[oracle]"
    uv add "vertebra-core[spanner]"
    uv add "vertebra-core[sqlite]"
    uv add "vertebra-core[redis]"
    uv add "vertebra-core[valkey]"
    ```

=== "pip"

    ```bash
    pip install "vertebra-core[postgres]"
    pip install "vertebra-core[mysql]"
    pip install "vertebra-core[mssql]"
    pip install "vertebra-core[bigquery]"
    pip install "vertebra-core[duckdb]"
    pip install "vertebra-core[oracle]"
    pip install "vertebra-core[spanner]"
    pip install "vertebra-core[sqlite]"
    pip install "vertebra-core[redis]"
    pip install "vertebra-core[valkey]"
    ```

=== "pdm"

    ```bash
    pdm add "vertebra-core[postgres]"
    pdm add "vertebra-core[mysql]"
    pdm add "vertebra-core[mssql]"
    pdm add "vertebra-core[bigquery]"
    pdm add "vertebra-core[duckdb]"
    pdm add "vertebra-core[oracle]"
    pdm add "vertebra-core[spanner]"
    pdm add "vertebra-core[sqlite]"
    pdm add "vertebra-core[redis]"
    pdm add "vertebra-core[valkey]"
    ```

=== "poetry"

    ```bash
    poetry add "vertebra-core[postgres]"
    poetry add "vertebra-core[mysql]"
    poetry add "vertebra-core[mssql]"
    poetry add "vertebra-core[bigquery]"
    poetry add "vertebra-core[duckdb]"
    poetry add "vertebra-core[oracle]"
    poetry add "vertebra-core[spanner]"
    poetry add "vertebra-core[sqlite]"
    poetry add "vertebra-core[redis]"
    poetry add "vertebra-core[valkey]"
    ```

Installing `vertebra-core[postgres]` provides the standalone pure-Rust `vertebra-postgres` (`vertebra._postgres`) wheel with zero `libpq` or `libecpg` C dependency and `GIL_USED = False` compatibility across Python `3.12`–`3.15` (`3.14t`/`3.15t`); without it, `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` raise `MissingDependencyError`. Installing `vertebra-core[duckdb]` provides `vertebra-duckdb` (`vertebra._duckdb`). Installing `vertebra-core[sqlite]` provides the standalone pure-Rust `vertebra-sqlite` (`vertebra._sqlite`) wheel with bundled SQLite 3.45+, `GIL_USED = False`, and WAL pooling. Redis (`vertebra-core[redis]`, backed by `vertebra._redis`), Valkey (`vertebra-core[valkey]`, backed by `vertebra._valkey`), Oracle (`vertebra-core[oracle]`), MySQL (`vertebra-core[mysql]`), SQL Server (`vertebra-core[mssql]`), BigQuery (`vertebra-core[bigquery]`), and Spanner (`vertebra-core[spanner]`) drivers execute through their standalone native wheels and raise `MissingDependencyError` when absent.

## Unified Driver Contract & `provide_driver`

The PostgreSQL, MySQL, SQLite, Oracle, MSSQL, DuckDB, BigQuery, and Spanner adapters pair a `BaseDatabaseConfig` struct (`url_schemes`, `from_url`, `pool_key`) with `SyncDatabaseDriver` and `AsyncDatabaseDriver` implementations sharing a uniform execution contract:

- **DML & Tuple Execution**: `execute(sql, parameters) -> int`, `execute_many(sql, parameter_sets) -> int`, `execute_tuples(sql, parameters) -> list[tuple]`, and `execute_with_description(sql, parameters) -> tuple[list[tuple], tuple[str, ...]]`.
- **Typed Row & Scalar Queries**: `select(sql, parameters, *, schema_type=None)`, `select_one(...)` (raises `NotFoundError` when `0` rows match), `select_one_or_none(...) -> Any | None`, and `select_value(sql, parameters, *, value_type=None)` (raises `NotFoundError` on `0` rows and returns `None` for a single-row SQL `NULL`).
- **Transactions, Cancellation & Arrow SHM**: `with driver.transaction():` / `async with driver.transaction():`, `cancel()`, `select_to_arrow(...) -> ArrowCursor`, `stream_arrow_batches(...)`, and `load_from_arrow(table_name, data) -> int`.
- **Capability Protocols**: Engine-specific extensions are exposed as `@runtime_checkable` protocols on `vertebra.db` (`SyncPipelineCapable`, `AsyncPipelineCapable`, `SyncNotificationSource`, `AsyncNotificationSource`, `SyncChangeSource`, `AsyncChangeSource`, `SyncQueueStore`, `AsyncQueueStore`, and `EmbeddingPushdown`).

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

### Standalone DB-API 2.0 (`vertebra.db.dbapi`) & Broker Capabilities

`vertebra.db.dbapi` provides standalone PEP 249 synchronous and asynchronous connections (`SyncDBAPIConnection`, `SyncDBAPICursor`, `SyncDBAPIModule`, `AsyncDBAPIConnection`, `AsyncDBAPICursor`, `AsyncDBAPIModule`, `connect`, `is_row_returning_sql`, and standard PEP 249 exception types) backed by `vertebra.db` drivers without importing `sqlalchemy`, `vertebra.extensions`, or adapter subpackages eagerly:

- **Statement Autocommit Databases**: Setting `autocommit=False` on `bigquery`, `spanner`, or `spangres` DB-API connections raises PEP 249 `NotSupportedError` because those backends manage transactions at statement or RPC boundaries.
- **Native Broker Engine Capsules**: `vertebra._postgres`, `vertebra._mysql`, `vertebra._oracle`, `vertebra._mssql`, `vertebra._duckdb`, and `vertebra._sqlite` export C-ABI `BrokerEngineCapsule` (`broker_engine_capsule()`) hooks discovered by `BrokerEngineDiscovery`; `vertebra._bigquery` and `vertebra._spanner` use multiplexed HTTP/2 REST/gRPC channels and are excluded from in-process SHM broker capsule hosting.

## SQLSpec & Litestar Integration

Use `create_sqlspec_plugin` with an adapter configuration (such as `PostgresAsyncConfig`, `MySQLAsyncConfig`, or `SQLiteAsyncConfig`) to manage connection leasing, shared-memory statement caches, and dependency injection:

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

## Native Database Drivers & Adapters Matrix

`vertebra.db.adapters` and `vertebra.extensions.sqlspec` provide synchronous and asynchronous drivers across 11 relational, analytical, cloud-native, and key-value engines:

| Engine | SQLSpec Adapter Config | Native Config Class | Sync / Async Drivers | Key Capabilities |
| --- | --- | --- | --- | --- |
| **PostgreSQL** | `PostgresAsyncConfig`, `PostgresSyncConfig` | `PostgreSQLConfig` | `SyncPostgreSQLDriver`, `AsyncPostgreSQLDriver` | Pure-Rust Protocol 3.0 (`vertebra._postgres`) with `$1..$N` binds, single-`Sync` `pipeline()`, binary `PGCOPY`, `pgvector`, `JSONB`, typed `Pg*` wrappers, `PostgresChangeSource`, `PostgresEmbeddingPushdown`, PgBouncer/PgCat/pgdog modes, direct `:5432` IAM OAuth2 or `:5433` AlloyDB / `:3307` Cloud SQL dialers |
| **MySQL / MariaDB** | `MySQLAsyncConfig`, `MySQLSyncConfig` | `MySQLConfig` | `SyncMySQLDriver`, `AsyncMySQLDriver` | Pure-Rust `vertebra._mysql` binary prepared statements (`COM_STMT_PREPARE` / `COM_STMT_EXECUTE`), `SyncMySQLPipeline` / `AsyncMySQLPipeline`, `VTBARROW` `ArrowCursor`, out-of-band `KILL QUERY` cancellation, direct `:3306` IAM OAuth2 token or Cloud SQL `:3307` mTLS dialer |
| **SQLite / libSQL** | `SQLiteAsyncConfig`, `SQLiteSyncConfig` | `SQLiteConfig` | `SyncSQLiteDriver`, `AsyncSQLiteDriver` | Pure-Rust `vertebra-sqlite` (`vertebra._sqlite`), WAL mode, `mmap_size`, SQLite 3.45+ binary `JSONB`, delayed-commit `SyncSQLitePipeline` / `AsyncSQLitePipeline`, online `backup_to` / `restore_from`, and `VTBARROW` `ArrowCursor` streaming |
| **Oracle 26ai** | `OracleAsyncConfig`, `OracleSyncConfig` | `OracleConfig` | `SyncOracleDriver`, `AsyncOracleDriver` | Pure-Rust Thin TNS/TTC (`vertebra._oracle`) with zero Instant Client dependency, `SyncOraclePool` / `AsyncOraclePool` + DRCP (`cclass`, `purity`, `connection_tag`), Autonomous Wallet mTLS & IAM token auth, 26ai `VECTOR` (`0xDB`) & `OSON` binary JSON, `VTBARROW` `ArrowCursor` (`select_to_arrow` / `load_from_arrow`), delayed-commit `pipeline()`, VPD tenancy, `SyncOracleNotificationSource` / `AsyncOracleNotificationSource` |
| **SQL Server** | `MSSQLAsyncConfig`, `MSSQLSyncConfig` | `MSSQLConfig` | `SyncMSSQLDriver`, `AsyncMSSQLDriver` | Pure-Rust TDS 7.4/8.0 (`vertebra._mssql`), `?` / `$1` to `@P1` `sp_executesql` RPC rewriter, `BulkLoad`, `ATTENTION` cancellation, `VTBARROW` `ArrowCursor`, `SyncMSSQLPipeline` / `AsyncMSSQLPipeline`, Cloud SQL `:3307` mTLS |
| **DuckDB** | `DuckDBAsyncConfig`, `DuckDBSyncConfig` | `DuckDBConfig` | `SyncDuckDBDriver`, `AsyncDuckDBDriver` | Native `vertebra._duckdb` (`vertebra-core[duckdb]`), single-writer/multi-reader OLAP, savepoints, `SyncDuckDBPipeline` / `AsyncDuckDBPipeline`, `SyncGcsBearerSecretProvider` / `AsyncGcsBearerSecretProvider` `httpfs` secret rotation, zero-copy Arrow SHM + `Appender` |
| **Google BigQuery** | `BigQueryAsyncConfig`, `BigQuerySyncConfig` | `BigQueryConfig` | `SyncBigQueryDriver`, `AsyncBigQueryDriver` | Pure-Rust REST v2 + Storage Read/Write v1 (`VTBARROW` zero-copy Arrow SHM via `select_to_arrow` / `load_from_arrow`), `ObjectTableConfig` (`ObjectRef`), `dry_run` cost ceiling, ADK/Litestar/Events/Artifact stores |
| **Spanner** | `SpannerAsyncConfig`, `SpannerSyncConfig` | `SpannerConfig` | `SyncSpannerDriver`, `AsyncSpannerDriver` | Pure-Rust GoogleSQL + `Spangres`, cached `multiplexed = true` sessions, wound-wait `Aborted` retry, `BatchWrite`, Data Boost `PartitionQuery` (`select_to_arrow`), `SyncSpannerChangeStreamSource` / `AsyncSpannerChangeStreamSource` |
| **Redis** | — | `RedisConfig` | `SyncRedisDriver`, `AsyncRedisDriver` | Standalone native `vertebra._redis` (`vertebra-core[redis]`), RESP3 (`HELLO 3`), atomic pipelines, `CLIENT TRACKING` local cache invalidation, Sentinel, and Cluster (`CRC16-XMODEM`) |
| **Valkey** | — | `ValkeyConfig` | `SyncValkeyDriver`, `AsyncValkeyDriver` | Standalone native `vertebra._valkey` (`vertebra-core[valkey]`), Valkey 8.0+/9.0+ RESP3 client-side caching, Valkey 9.0 hash-field expiration (`hexpire`, `httl`), `CLIENT CAPA redirect`, Cluster/Sentinel, Google Cloud Memorystore IAM auth |
| **DataFusion** | — | `DataFusionConfig` | `DataFusionEngine` | Federated SQL across PostgreSQL, MySQL, SQLite, DuckDB, BigQuery, Spanner, and Parquet/CSV object storage |

`memory://` is not an engine URL. `MySQLConfig` and `PostgreSQLConfig.from_url("memory://")` raise `ImproperConfigurationError` immediately during config construction; `PostgreSQLConfig(dsn="memory://")`, `RedisConfig`, and `ValkeyConfig` construct from it, while their drivers raise `ImproperConfigurationError` on initialization, and so do the `from_valkey` / `from_redis` factories of `KeyValueStore`, the Litestar `MemoryStore`, the Starlette `CacheStore`, and `SQLAlchemyCacheBackend`. `DatabaseRegistry.parse_url("memory://")` raises `ValueError`. `QueueConfig(backend_url="memory://")`, `SyncObjectStore("memory://")`, and `SQLAlchemyCacheBackend.from_url("memory://")` keep their in-process backends.

### Spanner and BigQuery

The native Spanner (`vertebra-core[spanner]`) and BigQuery (`vertebra-core[bigquery]`) drivers delegate gRPC Data, Database Admin, Storage Read, and Storage Write calls to the official Google Cloud Rust clients (`google-cloud-spanner` and `google-cloud-bigquery`):

- **Partitioned Arrow Reads & Writes (`select_to_arrow`, `load_from_arrow`)**: `SyncSpannerDriver.select_to_arrow` and `AsyncSpannerDriver.select_to_arrow` execute a Data Boost partitioned query (`partitioned_query`), automatically falling back to a single-stream query when the statement is not root-partitionable, and materialize the rows into a shared-memory `ArrowCursor` (`VTBARROW`). `SyncBigQueryDriver.select_to_arrow` and `load_from_arrow` stream directly through BigQuery Storage Read/Write v1.
- **Spanner DDL Endpoint (`SpannerConfig.admin_endpoint`)**: Database Admin DDL (`update_database_ddl`) runs over REST. When `admin_endpoint` is omitted, Vertebra derives `https://spanner.googleapis.com` for `*.googleapis.com` endpoints and maps the Spanner Emulator's `:9010` gRPC port to its `:9020` REST port (`http://localhost:9010` → `http://localhost:9020`). Any other non-Google endpoint requires an explicit `admin_endpoint` before running DDL.
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

### PostgreSQL Native Driver

The native PostgreSQL driver (`vertebra-core[postgres]`, backed by `vertebra._postgres`) supports Protocol 3.0 pipelining, `$1..$N` typed parameters (`PgJson`, `PgParameter`, `PgVector`), logical replication CDC, and pushdown vector embeddings:

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

### Oracle 26ai Native Driver

The native Oracle 26ai driver (`vertebra-core[oracle]`, backed by `vertebra._oracle`) speaks Oracle Thin TNS/TTC directly from Rust with zero Oracle Instant Client (`libclntsh` / ODPI-C) dependency and `GIL_USED == False` compatibility on free-threaded Python:

```mermaid
flowchart LR
    A["OracleConfig & Wallet"] --> B["SyncOracleDriver / AsyncOracleDriver"]
    B --> C["SyncOraclePool / AsyncOraclePool"]
    C --> D["DRCP, TTC Piggyback & VPD Scope"]
    D --> E[("Oracle Database 26ai")]
```

- **Connect Descriptor Precedence, Wallet mTLS & IAM Tokens**: `OracleConfig.connect_descriptor()` resolves targets in priority order: explicit `connect_descriptor_str`, Autonomous Database Cloud Wallet aliases, multi-host RAC/Data Guard `OracleTopologyConfig`, explicit `dsn`, and `host`/`port`/`service_name` (or `sid`). Cloud Wallet archives (`wallet_zip` bytes/path or `wallet_path` + `tns_alias`) extract in memory via `OracleWallet.from_zip_bytes` without temporary files. Set `auth_mode="token"` with `access_token` or `token_auth_options` for IAM/OAuth2 bearer authentication.
- **Native Pools, DRCP & Zero-RTT Session Piggyback**: `SyncOraclePool` and `AsyncOraclePool` run each physical connection on a dedicated Rust worker thread with out-of-band `cancel()` (`ORA-01013` → `QueryCancelledError`), Database Resident Connection Pooling (`OracleDrcpConfig` with `cclass`, `purity="SELF"` or `"NEW"`, and `connection_tag`), and Two-Task Common (`TTC`) piggybacking for `client_identifier`, `module`, `action`, and `ecid`.
- **Oracle 26ai `VECTOR`, `OSON`, `RETURNING` & Arrow SHM**: Decodes binary `OSON` JSON into Python objects and binary `0xDB` `VECTOR` (`FLOAT32`, `FLOAT64`, `INT8`, `BINARY`) embeddings alongside `OracleEmbeddingPushdown`, supports single-round-trip `DML ... RETURNING ... INTO` out-binds, streams large queries into shared-memory `ArrowCursor` (`VTBARROW`) via `select_to_arrow()`, and ingests Arrow batches via `load_from_arrow()`.
- **Delayed-Commit Pipelines, VPD Tenancy & TxEventQ Notifications**: `driver.pipeline()` holds an ambient transaction (or nested `SAVEPOINT`) across `results()` and commits only when the `with` / `async with` block exits cleanly so post-fetch checks roll back the batch. `TenantConnectionScope` and `AsyncTenantConnectionScope` apply trusted VPD procedures (`context_package_proc` / `context_clear_proc`) or `DBMS_SESSION.SET_IDENTIFIER` and evict the pooled session if lease-exit reset fails. `SyncOracleNotificationSource` and `AsyncOracleNotificationSource` (`OracleNotifyConfig`) publish and stream Oracle 26ai TxEventQ / `DBMS_AQ` events with exponential reconnect backoff, `wait_timeout_seconds` idle session liveness probing (`SELECT 1 FROM DUAL`), and optional `ChangeFeed` envelopes.

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

### SQLite Native Driver

The native SQLite driver (`vertebra-core[sqlite]`, backed by `vertebra._sqlite`) runs on a pure-Rust worker engine bundling SQLite 3.45+ with `GIL_USED = False` compatibility on free-threaded Python:

- **Single-Writer / Multi-Reader WAL Pool**: By default, SQLite databases run in WAL mode with a 1-writer thread plus `max_readers` read worker pool. Write transactions and `:memory:` databases are automatically pinned to the dedicated writer worker connection.
- **Delayed-Commit Pipelines**: `SyncSQLitePipeline` and `AsyncSQLitePipeline` implement `SyncPipeline` and `AsyncPipeline` over `driver.pipeline()`, queueing operations and executing them in a single batch within an ambient transaction or nested `SAVEPOINT`.
- **Online Backup and Restore**: `driver.backup_to(target_path)` and `driver.restore_from(source_path)` invoke SQLite's online backup API in stepped pages (`pages_per_step=256`) without blocking concurrent reader threads.
- **Zero-Copy Arrow Streaming**: `driver.select_to_arrow()` streams query results directly into shared-memory `ArrowCursor` (`VTBARROW`) without intermediate Python object allocation.

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

### Redis Native Driver

The native Redis driver (`vertebra-core[redis]`, backed by `vertebra._redis`) provides pure-Rust RESP3 execution, pipeline batching, `CLIENT TRACKING` local cache invalidation, Sentinel, and Cluster topology routing:

```python
from vertebra.db.adapters.redis import AsyncRedisDriver, RedisConfig

config = RedisConfig.from_url("redis://localhost:6379/0")
async with AsyncRedisDriver(config) as driver:
    await driver.set("vtb:user:1", b'{"name": "Alice"}', ex=300)
    user_bytes = await driver.get("vtb:user:1")
    notifications = driver.notification_source()
    await notifications.subscribe(["user_events"])
```

### Valkey Native Driver

The native Valkey driver (`vertebra-core[valkey]`, backed by `vertebra._valkey`) provides pure-Rust RESP3 execution, Valkey 9.0 hash-field expiration (`hexpire`, `hpexpire`, `httl`, `hpersist`), `CLIENT CAPA redirect`, pipeline batching, `CLIENT TRACKING` local cache invalidation, Sentinel, and Cluster topology routing:

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
| **[SQLSpec, SQLAlchemy & Extensions](extensions.md)** | Configure `SQLSpecPlugin`, Advanced Alchemy, SQLAlchemy 2.0+ `+vtb` dialects, `Vector`, and `BinaryJSON` column types. |
