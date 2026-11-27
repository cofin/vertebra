# Range reads, Parquet, and ORM attachments

Vertebra object stores connect to PyArrow through seekable range readers, and to Advanced Alchemy and SQLSpec through `ObjectStoreBackend` adapters. The examples below use in-memory stores; swap in any backend from the [object storage guide](storage.md).

## Range reads with PyArrow

`store.open_reader(key)` returns a seekable file object accepted directly by PyArrow and Polars. Parquet readers fetch only the footer and needed row groups instead of downloading the whole file:

```python
import pyarrow as pa
import pyarrow.parquet as pq
from vertebra.storage import SyncObjectStore

store = SyncObjectStore("memory://analytics")
table = pa.table({"day": ["2026-10-01", "2026-10-02"], "events": [120, 98]})
store.write_parquet("events/2026-10.parquet", table, use_dictionary=["day"])

with store.open_reader("events/2026-10.parquet", read_ahead=1_048_576) as reader:
    parquet = pq.ParquetFile(reader)
    print(parquet.metadata.num_rows, parquet.schema_arrow.names)
```

Output: `2 ['day', 'events']`.

`read_ahead` sets the cached range window (default 4 MiB) so small sequential footer and page reads coalesce into fewer requests. Set it to 0 for one range request per read, or call `read_at` to read a slice without moving the cursor.

For encoding options and streaming batch reads, see [Arrow IPC and Parquet batches](storage.md#arrow-ipc-and-parquet-batches).

## Advanced Alchemy file attachments

`register_storage_backend` registers an object-store backend in Advanced Alchemy's storage registry so file attachment model fields read and write through Vertebra:

```python
from advanced_alchemy.types.file_object import FileObject
from vertebra.extensions.alchemy.storage import register_storage_backend
from vertebra.storage import SyncObjectStore

store = SyncObjectStore("memory://documents")
backend = register_storage_backend(store, key="documents")

contract = FileObject(backend=backend, filename="contract.pdf")
backend.save_object(contract, b"%PDF-1.4...")
print(contract.size)
```

Output: 11. Saves populate file size and ETag automatically and forward checksum and expected-digest options.

Use `save_arrow` or `save_arrow_async` to stream Arrow tables, record batches, or iterators directly and get back the write result:

```python
import pyarrow as pa

res = backend.save_arrow(
    "datasets/events.parquet",
    pa.table({"id": [1, 2]}),
    checksum=True,
)
print(res.sha256)
```

To sign URLs or share a GCS token cache, pass a config instead of a pre-built store so you can forward `signing` and `token_cache`:

```python
import os
from vertebra.extensions.alchemy.storage import register_storage_backend
from vertebra.storage import GCSConfig

config = GCSConfig(
    bucket="app-documents",
    service_account_email=os.environ["GCS_SIGNING_SERVICE_ACCOUNT"],
)
backend = register_storage_backend(
    config, key="contracts", token_cache=config.sync_token_cache()
)
url = backend.sign("contract.pdf", expires_in=900)
```

A GCS config with a service-account identity derives signing automatically; pass an explicit `SigningConfig` to override it. Passing signing or token-cache options alongside an already-built store raises `ImproperConfigurationError`.

## SQLSpec storage backend

`vertebra.extensions.sqlspec.storage.ObjectStoreBackend` implements SQLSpec's storage protocol for dataset staging and Arrow or Parquet exports:

```python
import pyarrow as pa
from vertebra.extensions.sqlspec.storage import ObjectStoreBackend
from vertebra.storage import SyncObjectStore

lake = ObjectStoreBackend(SyncObjectStore("memory://lake"), base_path="exports")
lake.write_arrow_sync("daily.parquet", pa.table({"events": [120, 98]}))
print(lake.read_arrow_sync("daily.parquet").num_rows)
```

Output: 2.

- **Direct Arrow streaming**: Supports Parquet, Arrow IPC, and CSV (`file_format="csv"`), forwarding compression, writer options, and checksum checks. SQLSpec storage pipelines stream through these writers so peak memory stays bounded by upload part size. Errors or cancellations abort the upload without overwriting existing objects, and `overwrite=False` refuses to replace an existing key.
- **Reads**: Sync and async table reads default to pre-buffering enabled, while pattern streams yield batches lazily across every matching key.
- **Django integrity hooks**: In Django apps, `DjangoStorage` forwards optional SHA-256, CRC32C, and MD5 attributes on uploaded files and raises `vertebra.typing.IntegrityError` on mismatch.

Pass a URI or storage config with signing or token-cache options to share a GCS token cache across SQLSpec exports and your other stores:

```python
from vertebra.extensions.sqlspec.storage import ObjectStoreBackend
from vertebra.storage import GCSConfig

config = GCSConfig(bucket="app-exports")
lake = ObjectStoreBackend(
    config,
    base_path="daily",
    token_cache=config.sync_token_cache(),
)
```

See the [storage API reference](../reference/storage.md) for the full method list.
