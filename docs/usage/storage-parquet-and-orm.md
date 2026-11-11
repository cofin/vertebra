# Range reads, Parquet, and ORM attachments

`SyncObjectStore` and `AsyncObjectStore` plug into PyArrow through seekable range readers, and into Advanced Alchemy and SQLSpec through `ObjectStoreBackend` adapters. The examples use in-memory stores with explicit namespaces; swap in any backend configuration from the [object storage guide](storage.md).

## Range reads with PyArrow

`store.open_reader(key)` returns a seekable `SyncReadableFile` (or `AsyncReadableFile`). PyArrow and Polars accept it as a file, so a Parquet reader fetches the footer and the row groups it needs instead of downloading the whole object:

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

`read_ahead` sets the cached range window (default `4_194_304` bytes). Reads shorter than the window are served from it, which keeps the many small footer and page reads a Parquet reader issues down to a few requests. Set `read_ahead=0` when every read should map to exactly one range request. `read_at(offset, length)` reads a slice without moving the cursor.

`write_parquet` takes `use_dictionary` and `use_byte_stream_split` as a boolean or a list of column names, plus `options` forwarded to `pyarrow.parquet.ParquetWriter`. `iter_parquet_batches` reads row groups on demand; see [Arrow IPC and Parquet batches](storage.md#arrow-ipc-and-parquet-batches).

## Advanced Alchemy file attachments

`register_storage_backend` registers an `ObjectStoreBackend` (`vertebra.extensions.alchemy.storage`) in Advanced Alchemy's storage registry. `FileObject` and `StoredObject` columns then save and load through the native store:

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

Output: `11`. Saves use native `put`, `put_file`, or streaming `put_stream`, and set the file's size and ETag from the result.

To sign attachment URLs or share a GCS token cache, pass a configuration instead of a built store and forward `signing` and `token_cache`:

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

A GCS configuration with a service-account identity derives `SigningConfig` automatically; pass `signing=SigningConfig(...)` to override it. Passing `signing` or `token_cache` together with an already-built store raises `ImproperConfigurationError`, because that store's authentication is fixed.

## SQLSpec storage backend

`vertebra.extensions.sqlspec.storage.ObjectStoreBackend` implements SQLSpec's `ObjectStoreProtocol` for data pipelines, dataset staging, and Arrow or Parquet export:

```python
import pyarrow as pa
from vertebra.extensions.sqlspec.storage import ObjectStoreBackend
from vertebra.storage import SyncObjectStore

lake = ObjectStoreBackend(SyncObjectStore("memory://lake"), base_path="exports")
lake.write_arrow_sync("daily.parquet", pa.table({"events": [120, 98]}))
print(lake.read_arrow_sync("daily.parquet").num_rows)
```

Output: `2`. `base_path` prefixes every key. `read_arrow_*` returns a complete Table and defaults to `pre_buffer=True`; `stream_arrow_sync` and `stream_arrow_async` yield batches on demand across every object matching a pattern.

The constructor also accepts a URI or `StorageConfig` with `signing=` and `token_cache=`, so SQLSpec exports to GCS can share the token cache used by your other stores:

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

See the [storage API reference](../reference/storage.md) for every store method.
