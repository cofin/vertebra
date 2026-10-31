# Databases & Object Storage

Vertebra ships native database drivers (`vertebra.db` / `vertebra-db`), CTE statement stacking (`QueryPipeline`), Cloud SQL and AlloyDB dialers, Arrow/DataFusion analytics, and cloud object storage (`vertebra.storage` / `vertebra-storage`) that release the Python GIL during network and disk I/O.

## Statement Stacking & Query Pipelines (`QueryPipeline`)

Network round-trips dominate database latency when a handler executes multiple sequential queries or mutations. `QueryPipeline`, `CteStep`, and `RelOverlay` (`vertebra.db.pipeline`) stack multiple read and write statements into a single PostgreSQL CTE query executed in one round-trip, with intra-statement read-your-own-writes over uncommitted CTE inserts and deletes:

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

- **`RelOverlay`**: Generates a `UNION ALL` / `NOT IN` CTE view over a base table and earlier `INSERT ... RETURNING *` or `DELETE ... RETURNING id` steps so subsequent steps in the same statement see uncommitted rows immediately.
- **`QueryPipeline.build_jsonb_object`**: Automatically chunks wide result sets into concatenated `jsonb_build_object(...) || jsonb_build_object(...)` calls so queries never trip PostgreSQL's 100-argument function limit.
- **SQLSpec Statement Stacking & Batching**: When using `SQLSpecPlugin` (`vertebra.extensions.sqlspec`), SQLSpec `StatementStack` pipelines and `execute_many` batches run over shared-memory leases and cached prepared statement plans (`StatementCache`).

## Native Database Drivers & Registry (`vertebra.db`)

`vertebra.db` provides synchronous and asynchronous drivers across 9 relational, analytical, and key-value engines managed through `DatabaseRegistry`:

| Engine | Config Class | Sync / Async Drivers | Key Capabilities |
| --- | --- | --- | --- |
| **PostgreSQL** | `PostgresConfig` | `SyncPostgresDriver`, `AsyncPostgresDriver` | Binary wire protocol, prepared statement cache, `QueryPipeline` CTE stacking |
| **MySQL / MariaDB** | `MysqlConfig` | `SyncMysqlDriver`, `AsyncMysqlDriver` | Pooled connections, binary prepared statements |
| **SQLite / libSQL** | `SqliteConfig` | `SyncSqliteDriver`, `AsyncSqliteDriver` | WAL mode, `:memory:` testing, zero-dependency local persistence |
| **Oracle 26ai** | `OracleConfig` | `SyncOracleDriver`, `AsyncOracleDriver` | Native `VECTOR` embeddings (`encode_vector_f32`), `OSON` binary JSON, `OracleWallet` mTLS |
| **SQL Server** | `MssqlConfig` | `SyncMssqlDriver`, `AsyncMssqlDriver` | TDS wire protocol, automatic `?` to `@P1, @P2` parameter rewriting, `TVP` table-valued params |
| **DuckDB** | `DuckdbConfig` | `SyncDuckdbDriver`, `AsyncDuckdbDriver` | Zero-copy Apache Arrow record batches in shared memory (`write_arrow_shm`, `read_arrow_shm`) |
| **Redis** | `RedisConfig` | `SyncRedisDriver`, `AsyncRedisDriver` | RESP3 pipelining, client-side caching (`TrackingBroadcast`), Redis Cluster (`CRC16` routing) |
| **Valkey** | `ValkeyConfig` | `SyncValkeyDriver`, `AsyncValkeyDriver` | RESP3 commands, GCP Memorystore IAM token refresh (`GcpMemorystoreIamConfig`) |
| **DataFusion** | `DataFusionConfig` | `DataFusionEngine` | Federated SQL across PostgreSQL, MySQL, SQLite, DuckDB, and Parquet/CSV object storage |

```python
from vertebra.db import DatabaseRegistry, SqliteConfig, SyncSqliteDriver

registry = DatabaseRegistry()
registry.register("default", SqliteConfig(path=":memory:"))

db = SyncSqliteDriver(SqliteConfig(path=":memory:"))
db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
db.execute("INSERT INTO items (name) VALUES (?)", ("bolt",))
rows = db.select("SELECT id, name FROM items")
db.close()
```

## Cloud SQL & AlloyDB Dialers (`vertebra.db.dialer`)

Connect directly to Google Cloud SQL (`:3307` ephemeral mTLS certificates) and Google Cloud AlloyDB (`:5433` `MetadataExchange` protobuf handshake) from Rust and Python without running an external Cloud SQL Auth Proxy sidecar container:

```python
from vertebra.db import AlloyDbConfig, AlloyDbDialer, CloudSqlConfig, CloudSqlDialer

cloudsql = CloudSqlDialer(
    CloudSqlConfig(
        instance_connection_name="my-project:us-central1:orders-db",
        enable_iam_auth=True,
    )
)
alloydb = AlloyDbDialer(
    AlloyDbConfig(
        instance_uri=(
            "projects/my-project/locations/us-central1/"
            "clusters/prod/instances/primary"
        ),
        enable_iam_auth=True,
    )
)
```

## Row-Level Security, CDC & Federated Analytics

- **Multi-Tenant Row-Level Security (`vertebra.db.rls`)**: `TenantPredicate` and `TenantScopeConfig` enforce dual-layer tenant isolation by injecting session context variables (`SET LOCAL app.tenant_id = ...`) alongside parameterized `WHERE tenant_id = ...` predicates.
- **Change Data Capture (`vertebra.db.cdc`)**: `ChangeFeed` (`CdcConfig`, `ChangeEvent`) streams logical replication changes with LSN checkpointing, `replay_since()`, and batch Parquet archival (`flush_to_parquet()`).
- **Federated Queries & Arrow SHM (`DataFusionEngine`, `ArrowCursor`)**: Register external tables (`TableRegistration`) on `DataFusionEngine` and exchange zero-copy Arrow record batches through shared memory (`write_arrow_shm`, `read_arrow_shm`).

## Multi-Cloud & Local Object Storage (`vertebra.storage`)

`vertebra.storage.ObjectStore` wraps the Rust `object_store` crate with both synchronous (`put`, `get`, `head`, `delete`, `list`, `copy`) and asynchronous (`put_async`, `get_async`, `head_async`, `delete_async`, `list_async`, `copy_async`) methods across 8 backends:

- **Cloud Backends**: AWS S3 (`S3Config`), Google Cloud Storage (`GcsConfig`), Azure Blob Storage (`AzureConfig`), Cloudflare R2 (`R2Config`), and Oracle Cloud Infrastructure (`OciConfig`).
- **Local, Memory & HTTP**: Local filesystem (`LocalConfig`), in-memory testing (`MemoryConfig`), and HTTP/WebDAV origins (`HttpConfig`), managed centrally via `StorageRegistry`.

```python
from vertebra.storage import MemoryConfig, ObjectStore, StorageRegistry

store = ObjectStore.from_config(MemoryConfig(namespace="uploads"))
store.put("reports/q1.json", b'{"revenue": 125000}')
payload = store.get("reports/q1.json")
meta = store.head("reports/q1.json")

registry = StorageRegistry()
registry.register("uploads", store)
```

## Presigned URLs, PyArrow & Transactional Uploads

Generate HMAC, S3 SigV4, GCS V4, Azure SAS, or OCI Cavage signed URLs (`presign_get`, `presign_put`, `verify_presigned_url`), read Parquet/Arrow byte ranges without downloading full files (`ReadableFile`), or stage uploads that commit and roll back alongside database transactions (`UploadTracker`):

```python
from vertebra.storage import ObjectStore, SigningConfig, UploadTracker

store = ObjectStore.from_memory(
    namespace="uploads",
    signing=SigningConfig(
        secret="hmac-signing-key",
        issuer="https://cdn.example.com",
    ),
)
download_url = store.presign_get("avatars/user-1.png", expires_in_secs=900)

tracker = UploadTracker(default_store=store)
tracker.stage_upload("avatars/user-1.png", b"\x89PNG\r\n\x1a\n...")
tracker.before_flush()
tracker.after_commit()

with store.open_readable("avatars/user-1.png") as reader:
    header_magic = reader.read_at(0, 8)
```
