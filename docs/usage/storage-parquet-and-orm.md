# Range Reads, Parquet & ORM Attachments

Vertebra's `SyncObjectStore` and `AsyncObjectStore` integrate with Apache Arrow / Polars via random-access HTTP `Range` reads (`SyncReadableFile` / `AsyncReadableFile`) and with Advanced Alchemy / SQLSpec via `ObjectStoreBackend`.

## Random-Access Range Reads (`SyncReadableFile` / `AsyncReadableFile`) & PyArrow

`store.open_reader(key)` returns a seekable `SyncReadableFile` (or `AsyncReadableFile`) supporting `read_at(offset, length)` (`Range: bytes=start-end`). Pass `SyncReadableFile` directly to PyArrow or Polars Parquet readers to inspect metadata footers and row groups without downloading entire multi-gigabyte objects:

```python
from vertebra.storage import SyncObjectStore

store = SyncObjectStore.from_memory(namespace="analytics")
store.put("events/2026-10.parquet", b"PAR1...footer...PAR1")

with store.open_reader("events/2026-10.parquet") as reader:
    magic_header = reader.read_at(0, 4)
    magic_footer = reader.read_at(reader.size - 4, 4)
```

## Advanced Alchemy Model File Attachments

Synchronize object storage attachments with database model transactions using Advanced Alchemy's `FileObject` and Vertebra's `ObjectStoreBackend` (`vertebra.extensions.alchemy.storage`):

```python
from advanced_alchemy.types.file_object import FileObject
from vertebra.extensions.alchemy.storage import (
    ObjectStoreBackend,
    register_storage_backend,
)
from vertebra.storage import SyncObjectStore

store = SyncObjectStore("memory://")
backend = register_storage_backend("documents", store=store)

file_obj = FileObject(backend=backend, filename="contract.pdf")
backend.save_object(file_obj, b"%PDF-1.4...")
```

## SQLSpec Storage Protocol Adapter

Use `vertebra.extensions.sqlspec.storage.ObjectStoreBackend` as a drop-in `sqlspec.protocols.ObjectStoreProtocol` backend for SQLSpec data pipelines, dataset staging, and Parquet export workflows.
