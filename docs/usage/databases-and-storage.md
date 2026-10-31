# Databases & Object Storage

Vertebra ships Rust-backed object storage (`vertebra.storage` / `vertebra-storage`) and native database connection pools (`vertebra.db` / `vertebra-db`) that release the Python GIL across all network and disk I/O operations.

## Multi-Cloud & Local Object Storage (`vertebra.storage`)

`vertebra.storage.Store` wraps the Rust `object_store` crate with typed configuration structs for local filesystems, in-memory testing, HTTP endpoints, AWS S3, Google Cloud Storage, Azure Blob Storage, Cloudflare R2, and Oracle Cloud Infrastructure (OCI):

```python
from vertebra.storage import MemoryConfig, Store

store = Store(MemoryConfig(namespace="uploads"))
put_result = store.put("reports/q1.json", b'{"revenue": 125000}')
payload = store.get("reports/q1.json")
meta = store.head("reports/q1.json")
```

### Presigned URLs & Staged Upload Pipelines

Generate HMAC presigned `GET`/`PUT` URLs with `SigningConfig`, or validate MIME types, byte size limits, and SHA-256 digests before committing objects with `StoragePipeline`:

```python
from vertebra.storage import MemoryConfig, StoragePipeline, Store

store = Store(MemoryConfig())
pipeline = StoragePipeline(store, max_bytes=5 * 1024 * 1024, allowed_prefixes=("image/",))
staged = pipeline.stage("avatars/user-1.png", b"\x89PNG\r\n\x1a\n...", content_type="image/png")
committed = pipeline.commit(staged)
```

## Native Database Pools (`vertebra.db`)

`vertebra.db` provides Rust-managed connection pools for PostgreSQL (`PostgresPool`), MySQL (`MySqlPool`), and SQLite (`SqlitePool`) with structured health checks (`DatabaseHealthCheck`) and automatic OpenTelemetry `sqlcommenter` trace injection:

```python
from vertebra.db import SqlitePool

pool = SqlitePool.connect("sqlite::memory:")
pool.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
pool.execute("INSERT INTO items (name) VALUES (?)", ["bolt"])
rows = pool.query("SELECT id, name FROM items")
```

For full SQL query building, named `.sql` file loading, and 20+ database adapters, combine Vertebra with `SQLSpecExtension` (`vertebra.extensions.sqlspec`).
