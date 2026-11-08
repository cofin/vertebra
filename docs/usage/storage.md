# Object storage

Use one object-store interface for local files, cloud objects, and explicit in-memory tests:

```python
from vertebra.storage import MemoryConfig, SyncObjectStore

store = SyncObjectStore(MemoryConfig(namespace="uploads"))
store.put_stream("reports/day.txt", [b"hello ", b"world"], if_not_exists=True)
store.rename("reports/day.txt", "reports/latest.txt", overwrite=False)
print(store.get("reports/latest.txt"))
```

Output: `b'hello world'`.

`SyncObjectStore` releases the GIL during native I/O. `AsyncObjectStore` uses native `RustTaskAwaitable` completion on the event loop. Pair handles with `store.to_async()` or `store.to_sync()` to share their native engine, including an in-memory store.

## Choose an explicit backend

Install `vertebra-core[storage]` and pass a URI or configuration as the constructor's required positional target. Empty, invalid, or explicit `None` targets raise `ImproperConfigurationError`; omitting the argument raises `TypeError`. A missing native storage extension raises `MissingDependencyError`.

| Backend | Configuration | URI |
| --- | --- | --- |
| Local filesystem | `LocalConfig(root="./objects")` | `file:///absolute/root` |
| AWS S3 / compatible services | `S3Config(bucket="assets", region="us-east-1")` | `s3://assets` |
| Google Cloud Storage | `GCSConfig(bucket="assets")` | `gs://assets` |
| Azure Blob / ADLS | `AzureConfig(account="account", container="assets")` | `az://assets` |
| Cloudflare R2 | `R2Config(bucket="assets", account_id="account")` | `r2://assets` |
| Oracle Cloud | `OCIConfig(bucket="assets", namespace="tenancy")` | `oci://assets@tenancy` |
| HTTP / WebDAV | `HTTPConfig(url="https://objects.example.com")` | `https://objects.example.com` |
| Ephemeral tests | `MemoryConfig(namespace="tests")` | `memory://tests` |

Import configurations from `vertebra.storage`. `from_config(config)` accepts the same configurations. Memory stores require explicit configuration and retain data only in their native engine; use local or cloud storage for persistence. See [authentication and signing](storage-auth-and-signing.md) for provider credentials.

## Stream bytes and update objects

`get()` returns complete bytes; `get_view()` returns a whole-object `memoryview` retaining its native `StorageBuffer` owner. Use `get_stream(key, chunk_size=65536)` for incremental `StorageBuffer` chunks and `put_stream(key, chunks)` for incremental uploads. Async stores accept sync or async upload iterables and expose async download iterators:

```python
import asyncio
from contextlib import aclosing
from vertebra.storage import AsyncObjectStore, MemoryConfig

async def main() -> None:
    store = AsyncObjectStore(MemoryConfig(namespace="streams"))
    await store.put_stream("greeting.txt", [b"hello ", b"world"])
    async with aclosing(store.get_stream("greeting.txt", chunk_size=6)) as chunks:
        async for chunk in chunks:
            print(bytes(chunk))

asyncio.run(main())
```

The chunks contain `b'hello '` and `b'world'`. Close a stream explicitly when stopping early. Producer errors abort the active multipart upload. S3/B2, R2, and OCI accumulate bounded 8 MiB parts, so callers can supply smaller chunks; the final part may be shorter.

`put()` and `put_stream()` accept `content_type`, string `attributes`, and write conditions: `if_not_exists=True` creates only a missing object; `if_match=etag` requires the current ETag; `if_none_match=etag` rejects a matching ETag. Writes return `PutResult` metadata. Create conflicts and failed preconditions raise exceptions.

`head(key)` returns `ObjectMeta` without fetching its body. `list_page(prefix, cursor=..., limit=1000)` returns `(objects, next_cursor)`; `None` ends pagination. `list_with_delimiter(prefix)` returns direct objects and common child prefixes. `copy(src, dst)` copies an object; `rename(src, dst, overwrite=False)` rejects an existing destination. Async versions use the same names and are awaited.

`get_range(key, offset, length)` reads a byte slice. `open_reader(key)` provides seekable reads; the async reader has async `read` and context management, with synchronous nonblocking `seek`, `tell`, and `close`.

## Arrow IPC and Parquet batches

Install the `arrow` extra to use PyArrow. The same methods exist on both stores; async writes are awaited and async readers return async iterators directly:

```python
import asyncio
from contextlib import aclosing
import pyarrow as pa
from vertebra.storage import AsyncObjectStore, MemoryConfig

async def main() -> None:
    store = AsyncObjectStore(MemoryConfig(namespace="tables"))
    table = pa.table({"id": range(250), "label": ["row"] * 250})
    await store.write_parquet("rows.parquet", table, row_group_size=100)
    sizes = []
    iterator = store.iter_parquet_batches(
        "rows.parquet", batch_size=100, columns=["id"]
    )
    async with aclosing(iterator) as batches:
        async for batch in batches:
            sizes.append(batch.num_rows)
    print(sizes)

asyncio.run(main())
```

Output: `[100, 100, 50]`.

`write_arrow(key, data, batch_size=65536)` writes IPC streams; `iter_arrow_batches(key)` reads them. `write_parquet` accepts `schema`, `compression="zstd"`, and `row_group_size=65536`. Writers accept a Table, RecordBatch, ArrowCursor, or synchronous batch iterable; empty iterables require a schema. Writes return `PutResult`. `ArrowCursor.from_rows` requires real PyArrow IPC and propagates required native named-SHM allocation failures.

Batch codecs advance with consumer demand. Async codec work runs off-loop while native range, stream, and multipart I/O stays on the caller loop. Cancellation waits for codec and I/O cleanup before releasing owners. Parquet batch readers default to `pre_buffer=False`; a fixed footer read may overlap a small file's entire contents. SQLSpec's explicit `pre_buffer=True` opts into prefetch. Its `read_arrow_*` methods intentionally return a complete Table, while `stream_arrow_*` preserves batch demand and format options.

## Litestar stores, static files, and Range responses

`ObjectStoreLitestarStore` implements Litestar's `Store` over an explicit async store, configuration, or URI. `set(key, value, expires_in=60)` persists bytes and expiration together; TTL survives backend reconstruction. `with_namespace("sessions")` creates a child namespace. `delete_all()` removes that namespace and descendants while preserving siblings; renewal applies to entries that already expire.

Mount an async filesystem adapter for static GET/HEAD:

```python
from litestar import Litestar
from litestar.static_files import create_static_files_router
from vertebra.extensions.litestar import ObjectStoreFileSystemAdapter
from vertebra.storage import LocalConfig

files = ObjectStoreFileSystemAdapter(
    LocalConfig(root="./objects"), base_path="public"
)
assets = create_static_files_router(
    path="/assets", directories=["/"], file_system=files, resolve_symlinks=False
)
app = Litestar(route_handlers=[assets])
```

The adapter confines paths beneath `public/`; populate that prefix before requesting an asset. For HTTP Range delivery, upload `report.pdf` to the configured root and use an explicit streaming route:

```python
from collections.abc import AsyncIterator
from contextlib import aclosing
from litestar import Litestar, Request, get
from litestar.response import Stream
from vertebra.storage import AsyncObjectStore, LocalConfig, StreamingStorageResponse

store = AsyncObjectStore(LocalConfig(root="./objects"))

@get("/download")
async def download(request: Request) -> Stream:
    response = await StreamingStorageResponse.from_async_store(
        store, "report.pdf", range_header=request.headers.get("range")
    )
    async def body() -> AsyncIterator[bytes]:
        async with aclosing(response.aiter_chunks()) as chunks:
            async for chunk in chunks:
                yield bytes(chunk)
    headers = dict(response.headers)
    if response.content_range is not None:
        headers["content-range"] = response.content_range
    if response.etag is not None:
        headers["etag"] = response.etag
    return Stream(body(), status_code=response.status_code, headers=headers,
                  media_type=response.content_type or response.media_type)

app = Litestar(route_handlers=[download])
```

Single closed, open-ended, and suffix ranges produce `206`; malformed or unsatisfiable byte ranges produce an empty `416` with `Content-Range: bytes */<size>`. No range produces `200`. Static mounting alone serves GET/HEAD. Native response handlers can explicitly await `to_async_fastpath_tuple()` before submitting its resolved body and byte header pairs; that conversion reads the selected body, while `aiter_chunks()` streams it.

## Framework storage contracts

SQLSpec `ObjectStoreBackend` supplies protocol `*_sync` / `*_async` methods, native stream/page/reader extensions, and lazy Arrow pattern streams. Advanced Alchemy's `register_storage_backend(store, key="media")` registers its concrete backend through `StorageRegistry`; resolve it with `get_backend`. `DjangoStorage` subclasses Django `Storage`, is deconstructible for migrations, and returns seekable Django `File` handles. It supports async save/open operations; returned Django file reads follow Django's synchronous interface. Remote `path()` and unavailable creation/access timestamps raise `NotImplementedError`.

See [Parquet, SQLSpec, and ORM examples](storage-parquet-and-orm.md), [authentication and signing](storage-auth-and-signing.md), and the [storage API reference](../reference/storage.md).
