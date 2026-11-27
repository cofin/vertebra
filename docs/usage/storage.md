# Object storage

Use one object-store interface for local files, cloud objects, and in-memory tests:

```python
from vertebra.storage import MemoryConfig, SyncObjectStore

store = SyncObjectStore(MemoryConfig(namespace="uploads"))
store.put_stream("reports/day.txt", [b"hello ", b"world"], if_not_exists=True)
store.rename("reports/day.txt", "reports/latest.txt", overwrite=False)
print(store.get("reports/latest.txt"))
```

Output: `b'hello world'`.

`SyncObjectStore` releases the GIL during I/O, and `AsyncObjectStore` runs operations asynchronously on your event loop. Call `to_async()` or `to_sync()` on an existing store to create a paired handle sharing the same underlying backend, including for in-memory stores.

## Install

```bash
uv add "vertebra-core[storage]"
```

Add the arrow extra (`vertebra-core[storage,arrow]`) if you use the Arrow IPC or Parquet methods.

## Choose an explicit backend

Pass a URI or config object as the constructor's first positional argument. Empty, invalid, or None targets raise `ImproperConfigurationError`; omitting the argument raises `TypeError`. If the storage extra is not installed, construction raises `MissingDependencyError`.

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

Import configs from `vertebra.storage`; `from_config` accepts the same config types. Each memory store owns private storage—two stores created from the same memory URI or config do not share data unless one was derived from the other via `to_async()` or `to_sync()`.

See [authentication and signing](storage-auth-and-signing.md) for provider credentials, and [RLS, CDC & Arrow Analytics](analytics-and-cdc.md) for rotating GCS bearer tokens in DuckDB httpfs queries.

Backend addressing options:

- **S3**: Set `virtual_hosted_style=True` to put the bucket in the host subdomain for data requests and presigned URLs. Use `endpoint` for S3-compatible services, and add `allow_http=True` for local HTTP endpoints.
- **R2**: `custom_domain` records your public hostname, while presigned URLs always target the R2 S3 API host because Cloudflare rejects presigned URLs on custom domains.
- **HTTP/WebDAV**: Pass static `headers` on `HTTPConfig` to send them on every request.

## Object keys

Keys are slash-delimited strings. Leading and trailing slashes and surrounding whitespace are trimmed. Everything else is stored as written—spaces, percent signs, and non-ASCII characters are not percent-encoded—so keys returned by listings match what you wrote. Keys with a `..` segment or empty `//` segment raise `ValueError`.

## Stream bytes and update objects

- `get` returns complete bytes; `get_view` returns a zero-copy memoryview.
- `get_stream` yields chunks as they arrive (pass `chunk_size` to cap chunk sizes).
- `put_stream` uploads incrementally and accepts both sync and async iterables on async stores.
- `head` fetches object metadata without the body, and `exists` returns a boolean.

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

Output chunks: `b'hello '` and `b'world'`. Close streams explicitly if you stop reading early. On S3/B2, R2, and OCI, uploads buffer bounded 8 MiB parts so you can yield smaller chunks. If the producer raises an error, the multipart upload aborts cleanly.

`put_multipart` accepts a synchronous iterable on both sync and async stores, consuming parts in order without materializing the iterable in memory.

## Conditional writes

Write methods accept content type, string attributes, and one write condition, returning a `PutResult`:

| Condition | Succeeds when | Failure |
| --- | --- | --- |
| `if_not_exists=True` | Key does not exist | `FileExistsError` |
| `if_none_match="*"` | Key does not exist | `FileExistsError` |
| `if_match=etag` | Current ETag equals `etag` | `PreconditionError` |
| `if_none_match=etag` | Current ETag differs from `etag`, or key is missing | `PreconditionError` |
| `copy(src, dst, overwrite=False)` | `dst` does not exist | `FileExistsError` |
| `rename(src, dst, overwrite=False)` | `dst` does not exist | `FileExistsError` |

Copy and rename overwrite by default. Cloud rename copies then deletes the source, so a failed delete can leave both keys present. `PreconditionError` subclasses `OSError` and is exported from `vertebra.storage` and `vertebra.typing`.

```python
from vertebra.storage import MemoryConfig, PreconditionError, SyncObjectStore

store = SyncObjectStore(MemoryConfig(namespace="config"))
current = store.put("settings.json", b'{"v": 1}')
store.put("settings.json", b'{"v": 2}', if_match=current.etag)
try:
    store.put("settings.json", b'{"v": 3}', if_match=current.etag)
except PreconditionError:
    print("stale write rejected")
```

The second conditional write fails because the first write changed the ETag. On GCS, `if_match` also accepts a numeric object generation from metadata version.

## Content integrity and verified reads

Pass `checksum=True` to write methods to compute lowercase hex SHA-256, CRC32C, and MD5 digests on the returned result. Metadata lookups and listings leave digest fields as None.

Pass `expected_sha256`, `expected_crc32c`, or `expected_md5` on writes to verify payloads before commit. Mismatches raise `vertebra.typing.IntegrityError` (distinct from database integrity errors) and abort multipart uploads without overwriting existing objects.

```python
import hashlib
from vertebra.storage import MemoryConfig, SyncObjectStore
from vertebra.typing import IntegrityError

store = SyncObjectStore(MemoryConfig(namespace="integrity"))
payload = b"verified-payload"
digest = hashlib.sha256(payload).hexdigest()

result = store.put("artifacts/data.bin", payload, checksum=True)
assert result.sha256 is not None
assert result.crc32c is not None and result.md5 is not None

store.put_stream(
    "artifacts/stream.bin",
    [b"verified-", b"payload"],
    expected_sha256=digest,
)
data = store.get("artifacts/stream.bin", expected_sha256=digest)

try:
    store.get("artifacts/stream.bin", expected_sha256="0" * 64)
except IntegrityError:
    print("checksum mismatch detected")
```

Read methods also accept `expected_sha256`:

- Malformed hex digests raise `ValueError` before I/O starts.
- Streaming reads verify incrementally and raise `IntegrityError` at EOF on mismatch.
- File downloads delete their temporary part file on mismatch before renaming.

## Files and bulk transfers

- **Single files**: `put_file` streams files larger than 8 MiB via multipart upload. `get_file` creates parent directories, downloads to a sibling temporary file, and renames it into place once complete.
- **Batch transfers**: `upload_many` and `download_many` run with bounded concurrency (16 by default). Duplicate target filenames raise `ValueError` before any transfer starts.
- **Staged scratch files**: `staged_download` downloads objects into a temporary directory (preferring `/dev/shm` when writable) and deletes them when exiting the context manager. Passing a destination directory keeps that directory and removes only the staged files.

```python
from vertebra.storage import MemoryConfig, SyncObjectStore

store = SyncObjectStore(MemoryConfig(namespace="batch"))
store.put("inputs/a.csv", b"id\n1\n")
store.put("inputs/b.csv", b"id\n2\n")
with store.staged_download(["inputs/a.csv", "inputs/b.csv"]) as files:
    print(sum(len(path.read_bytes()) for path in files))
```

Output: 10.

## List and match keys

- `list` returns every object under a prefix.
- `list_page` returns a page of objects and the next cursor (None when done), defaulting to 1,000 items.
- `list_with_delimiter` returns direct objects and immediate child prefixes.
- `glob` matches shell wildcards against full keys. Wildcards match slashes as well, so `runs/*` matches `runs/a/b.json`. Add literal path segments to narrow a pattern, or use `list_with_delimiter` for a single directory level.

## Compose objects

`compose` concatenates source objects in order into a destination key and returns the write result:

```python
from vertebra.storage import MemoryConfig, SyncObjectStore

store = SyncObjectStore(MemoryConfig(namespace="parts"))
store.put("upload/part-0", b"hello ")
store.put("upload/part-1", b"world")
store.compose(["upload/part-0", "upload/part-1"], "upload/final.txt")
print(store.get("upload/final.txt"))
```

Output: `b'hello world'`. GCS uses server-side compose and folds more than 32 sources through temporary objects that are always cleaned up. Other backends stream sources into one multipart upload. Passing an empty source list raises `ValueError`.

## Range reads and file-like access

- `get_range` reads a single byte slice by offset and length.
- `open_reader` returns a seekable file object with a `read_ahead` cache window (default 4 MiB; set to 0 for one range request per read). Async readers expose awaitable reads with synchronous seek, tell, and close.
- `open_writer` buffers writes into 8 MiB slices by default and uploads full slices as multipart parts.

```python
import gzip
from vertebra.storage import MemoryConfig, SyncObjectStore

store = SyncObjectStore(MemoryConfig(namespace="logs"))
with store.open_writer("logs/day.gz", content_type="application/gzip") as raw:
    with gzip.GzipFile(fileobj=raw, mode="wb") as compressed:
        compressed.write(b"line\n" * 1000)
print(raw.result.size > 0)
```

Closing the writer commits the object and populates its result attribute. Calling abort or raising inside the block discards the upload. Sync writers subclass `io.RawIOBase` so standard wrappers like `gzip.GzipFile` work directly. On async writers, `await writer.aclose()` returns the write result (or None after abort), and unclosed writers emit a `ResourceWarning`.

## Arrow IPC and Parquet batches

With the arrow extra installed, both sync and async stores read and write Arrow IPC and Parquet streams:

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

- `write_arrow` and `iter_arrow_batches` handle Arrow IPC streams. Writers accept a Table, RecordBatch, Arrow cursor, or sync batch iterable. Empty iterables raise `ValueError` on Arrow writes, and require an explicit schema on Parquet writes.
- `write_parquet` defaults to Zstd compression and 65,536-row groups, supports dictionary and byte-stream-split encoding per column, and forwards extra `options` directly to PyArrow's ParquetWriter.
- `iter_parquet_batches` reads row groups lazily with pre-buffering disabled; pass `options={"pre_buffer": True}` to coalesce column-chunk reads.

```python
import pyarrow as pa
from vertebra.storage import MemoryConfig, SyncObjectStore

store = SyncObjectStore(MemoryConfig(namespace="metrics"))
table = pa.table({"label": ["a", "b"], "latency_ms": [1.5, 2.5]})
store.write_parquet(
    "metrics.parquet",
    table,
    use_dictionary=["label"],
    use_byte_stream_split=["latency_ms"],
    options={"data_page_size": 1 << 20},
)
```

## Multiple processes

Storage clients run I/O on a process-wide native runtime. Forked child processes do not inherit those threads, so never reuse a store created before forking. Use the spawn start method and construct stores inside each worker:

```python
import multiprocessing
from vertebra.storage import LocalConfig, SyncObjectStore

def count(prefix: str) -> int:
    store = SyncObjectStore(LocalConfig(root="./objects"))
    return len(store.list(prefix))

if __name__ == "__main__":
    with multiprocessing.get_context("spawn").Pool(4) as pool:
        print(pool.map(count, ["a/", "b/", "c/", "d/"]))
```

Threads within one process can safely share a single store.

## Litestar stores, static files, and Range responses

`ObjectStoreLitestarStore` implements Litestar's Store interface with persisted TTLs, child namespaces, and scoped deletion.

Serve static GET and HEAD assets with `ObjectStoreFileSystemAdapter`:

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

For HTTP Range streaming, use `StreamingStorageResponse`:

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

Valid byte ranges return HTTP 206, missing Range headers return 200, and malformed or unsatisfiable ranges return an empty 416 with a full-length Content-Range header.

## Framework storage contracts

- **SQLSpec**: `ObjectStoreBackend` implements SQLSpec's storage protocol with sync and async methods, streaming helpers, and lazy Arrow pattern reads.
- **Advanced Alchemy**: `register_storage_backend` registers a backend in Advanced Alchemy's storage registry.
- **Django**: `DjangoStorage` subclasses Django's Storage, supports migrations, returns seekable synchronous File handles, and supports async save and open. Calling `path()` on remote backends or reading unsupported timestamps raises `NotImplementedError`.

See [Parquet, SQLSpec, and ORM examples](storage-parquet-and-orm.md), [authentication and signing](storage-auth-and-signing.md), and the [storage API reference](../reference/storage.md).
