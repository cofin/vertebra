# Databases & Object Storage

Vertebra ships Rust-backed object storage (`vertebra.storage` / `vertebra-storage`) and native database drivers (`vertebra.db` / `vertebra-db`) that release the Python GIL across all network and disk I/O operations.

## Multi-Cloud & Local Object Storage (`vertebra.storage`)

`vertebra.storage.ObjectStore` wraps the Rust `object_store` crate with typed configuration structs for local filesystems, in-memory testing, HTTP endpoints, AWS S3, Google Cloud Storage, Azure Blob Storage, Cloudflare R2, and Oracle Cloud Infrastructure (OCI):

```python
from vertebra.storage import MemoryConfig, ObjectStore

store = ObjectStore.from_config(MemoryConfig(namespace="uploads"))
put_result = store.put("reports/q1.json", b'{"revenue": 125000}')
payload = store.get("reports/q1.json")
meta = store.head("reports/q1.json")
```

### Presigned URLs & Transactional Upload Staging

Generate HMAC, S3 SigV4, GCS V4, Azure SAS, or OCI Cavage signed URLs with `SigningConfig`, or coordinate staged object uploads with database commits and rollbacks via `UploadTracker`:

```python
from vertebra.storage import ObjectStore, UploadTracker

store = ObjectStore.from_memory(namespace="uploads")
tracker = UploadTracker(default_store=store)
tracker.stage_upload("avatars/user-1.png", b"\x89PNG\r\n\x1a\n...")
tracker.before_flush()
tracker.after_commit()
```

## Native Database Drivers (`vertebra.db`)

`vertebra.db` provides synchronous and asynchronous database drivers for PostgreSQL (`PostgresConfig`), MySQL (`MysqlConfig`), and SQLite (`SqliteConfig`, `SyncSqliteDriver`, `AsyncSqliteDriver`) alongside automatic OpenTelemetry `sqlcommenter` trace injection:

```python
from vertebra.db.sqlite import SqliteConfig, SyncSqliteDriver

db = SyncSqliteDriver(SqliteConfig(path=":memory:"))
db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
db.execute("INSERT INTO items (name) VALUES (?)", ("bolt",))
rows = db.select("SELECT id, name FROM items")
db.close()
```

For full SQL query building, named `.sql` file loading, and 20+ database adapters, combine Vertebra with `SQLSpecPlugin` (`vertebra.extensions.sqlspec`).
