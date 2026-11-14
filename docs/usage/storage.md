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

## Install

=== "uv"

    ```bash
    uv add "vertebra-core[storage]"
    ```

=== "pip"

    ```bash
    pip install "vertebra-core[storage]"
    ```

=== "pdm"

    ```bash
    pdm add "vertebra-core[storage]"
    ```

=== "poetry"

    ```bash
    poetry add "vertebra-core[storage]"
    ```

Add the `arrow` extra (`vertebra-core[storage,arrow]`) for the Arrow IPC and Parquet methods.

## Choose an explicit backend

Pass a URI or configuration as the constructor's required positional target. Empty, invalid, or explicit `None` targets raise `ImproperConfigurationError`; omitting the argument raises `TypeError`. A missing native storage extension raises `MissingDependencyError`.

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

Import configurations from `vertebra.storage`. `from_config(config)` accepts the same configurations. Each memory store owns a private in-memory backend: the namespace is a label, and two stores built from the same `memory://` URI or `MemoryConfig` do not see each other's objects. Only handles derived with `to_async()` or `to_sync()` share data. Use local or cloud storage for persistence or cross-store sharing. See [authentication and signing](storage-auth-and-signing.md) for provider credentials.

For DuckDB `httpfs` `gs://` queries, `SyncGcsBearerSecretProvider` and `AsyncGcsBearerSecretProvider` in `vertebra.storage` wrap `SyncGcpTokenCache` and `AsyncGcpTokenCache` (`from_config(GCSConfig(...), allow_ambient_adc=True, scopes=GCS_DEFAULT_SCOPES)`) to supply rotating `{"bearer_token": <token>}` dictionaries to `DuckDBSecretConfig(secret_type="gcs", value_provider=...)`. Dynamic `value_provider` secrets require `persistent=False` and a positive finite `refresh_interval_seconds`; when DuckDB surfaces an `httpfs` `(HTTP 401)` or `(HTTP 403)` response, the driver calls `provider.invalidate(used_generation)` and re-raises without retrying the failed statement so the next query rotates `CREATE OR REPLACE SECRET` automatically:

```python
from vertebra.db.adapters.duckdb import DuckDBConfig, DuckDBSecretConfig
from vertebra.storage import GCSConfig, SyncGcsBearerSecretProvider

duck_config = DuckDBConfig(
    database=":memory:",
    secrets=(
        DuckDBSecretConfig(
            name="gcs_lake",
            secret_type="gcs",
            value_provider=SyncGcsBearerSecretProvider.from_config(
                GCSConfig(bucket="lake"),
            ),
            scope="gs://lake/",
        ),
    ),
)
```

A few backend options change addressing rather than credentials:

- `S3Config(virtual_hosted_style=True)` addresses the bucket as a host subdomain (`<bucket>.s3.<region>.amazonaws.com`) for data requests and SigV4 presigned URLs. `endpoint` targets an S3-compatible service; local `http://` endpoints also need `allow_http=True`.
- `R2Config(custom_domain="files.example.com")` records a public custom domain as a store option. Presigned URLs always use the R2 S3 API host, because Cloudflare rejects presigned URLs on custom domains. Serve public objects from the custom domain with plain URLs.
- `HTTPConfig(headers={"authorization": "Bearer ..."})` sends static headers on every request to the HTTP/WebDAV origin.

## Object keys

Keys are `/`-delimited strings. Leading and trailing `/` and surrounding whitespace are trimmed. Everything else is stored as written: spaces, `%`, and non-ASCII characters are not percent-encoded, so a key read back from `list()` matches the key you wrote. Keys containing a `..` segment or an empty `//` segment raise `ValueError`.

## Stream bytes and update objects

`get()` returns complete bytes; `get_view()` returns a whole-object `memoryview` retaining its native `StorageBuffer` owner. Use `get_stream(key)` for incremental `StorageBuffer` chunks (backend chunks pass through unchanged unless you set `chunk_size` to cap each chunk) and `put_stream(key, chunks)` for incremental uploads. Async stores accept sync or async upload iterables and expose async download iterators:

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

`put_multipart(key, parts)` accepts a synchronous iterable on both sync and async stores and uses the same incremental upload path. Parts are consumed in order without collecting the whole iterable. A producer exception aborts the upload and preserves an existing destination.

`head(key)` returns `ObjectMeta` without fetching its body, and `exists(key)` returns a boolean. Async versions of every method use the same names and are awaited.

## Conditional writes

`put()`, `put_stream()`, and `open_writer()` accept `content_type`, string `attributes`, and one write condition. Writes return `PutResult` metadata.

| Condition | Succeeds when | Failure |
| --- | --- | --- |
| `if_not_exists=True` | The key does not exist | `FileExistsError` |
| `if_none_match="*"` | The key does not exist (same as `if_not_exists=True`) | `FileExistsError` |
| `if_match=etag` | The current ETag equals `etag` | `PreconditionError` |
| `if_none_match=etag` | The current ETag differs from `etag`, or the key is missing | `PreconditionError` |
| `copy(src, dst, overwrite=False)` | `dst` does not exist | `FileExistsError` |
| `rename(src, dst, overwrite=False)` | `dst` does not exist | `FileExistsError` |

S3/B2 enforce create-only copy with conditional multipart completion; R2 uses its conditional copy header. Providers that reject these operations surface a backend error. Cloud rename copies before deleting the source, so it is not atomic; a delete failure can leave both keys present.

`copy` and `rename` overwrite by default. `PreconditionError` subclasses `OSError` and is importable from `vertebra.storage` and `vertebra.typing`:

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

The second conditional write fails because the first one changed the ETag. On GCS, `if_match` also accepts a numeric object generation from `ObjectMeta.version`.

## Content integrity and verified reads

`put()`, `put_stream()`, `put_multipart()`, `put_file()`, and `open_writer()` accept `checksum=True` to compute SHA-256, Castagnoli CRC32C, and MD5 lowercase hexadecimal receipts in `PutResult` (`PutResult.sha256`, `PutResult.crc32c`, `PutResult.md5`; `head()` and `list()` leave digest fields `None` and never copy unverified user metadata into receipt fields). Supply `expected_sha256`, `expected_crc32c`, or `expected_md5` on writes to verify payloads prior to commit; mismatches raise `vertebra.typing.IntegrityError` (distinct from `vertebra.db.IntegrityError`) and abort multipart uploads without overwriting existing destinations:

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

Reads (`get()`, `get_file()`, `get_stream()`, and `open_reader()`) accept `expected_sha256`. Malformed hex strings fail upfront with `ValueError` before I/O starts. Streaming reads (`get_stream()` and `open_reader()`) verify incrementally and raise `IntegrityError` on the final EOF poll if the downloaded stream does not match `expected_sha256`; `get_file()` removes the temporary `.part` file on mismatch before renaming.

## Files and bulk transfers

`put_file(key, path)` uploads a local file and `get_file(key, path)` downloads one. Files larger than 8 MiB stream through multipart parts without loading the whole file. `get_file` writes to a temporary sibling file and renames it into place after the last byte arrives, so readers never see a partial file; parent directories are created.

`upload_many(paths, destination_prefix)` and `download_many(keys, destination)` transfer many files with bounded concurrency (`concurrency=16` by default). Uploaded keys join the prefix and each file name; downloaded files are named after each key's last segment. Duplicate names raise `ValueError` before any transfer starts.

`staged_download(keys)` downloads into a scratch directory for the duration of a `with` block and deletes the files on exit. Without `destination`, files stage under `/dev/shm` when it is writable, otherwise under the system temporary directory:

```python
from vertebra.storage import MemoryConfig, SyncObjectStore

store = SyncObjectStore(MemoryConfig(namespace="batch"))
store.put("inputs/a.csv", b"id\n1\n")
store.put("inputs/b.csv", b"id\n2\n")
with store.staged_download(["inputs/a.csv", "inputs/b.csv"]) as files:
    print(sum(len(path.read_bytes()) for path in files))
```

Output: `10`. On `AsyncObjectStore`, `staged_download` is an async context manager (`async with`). Passing `destination` keeps that directory and removes only the staged files.

## List and match keys

`list(prefix)` returns every object under a prefix. `list_page(prefix, cursor=..., limit=1000)` returns `(objects, next_cursor)`; `None` ends pagination. `list_with_delimiter(prefix)` returns direct objects and common child prefixes.

`glob(pattern)` returns objects matching a shell-style `*`, `?`, or `[...]` pattern. It lists the literal prefix before the first wildcard and filters full keys. Matching is not segment-aware: `*` and `?` also match `/`, so `runs/*` matches `runs/a/b.json`. Add literal segments to narrow a match (`runs/2024/part-*.json`), and use `list_with_delimiter` when you need exactly one level.

## Compose objects

`compose(sources, dst)` concatenates objects in order into `dst` and returns `PutResult`:

```python
from vertebra.storage import MemoryConfig, SyncObjectStore

store = SyncObjectStore(MemoryConfig(namespace="parts"))
store.put("upload/part-0", b"hello ")
store.put("upload/part-1", b"world")
store.compose(["upload/part-0", "upload/part-1"], "upload/final.txt")
print(store.get("upload/final.txt"))
```

Output: `b'hello world'`. GCS stores use the server-side compose API, folding more than 32 sources through temporary objects that are deleted on success, failure, or cancellation. Other backends stream each source into one multipart upload. An empty `sources` list raises `ValueError`.

## Range reads and file-like access

`get_range(key, offset, length)` reads a byte slice. `open_reader(key)` returns a seekable `SyncReadableFile` (or `AsyncReadableFile`) with `read`, `read_at`, `seek`, and `tell`. Reads shorter than `read_ahead` (default `4_194_304` bytes) are served from one cached range window, so small sequential reads cost one request per window; `read_ahead=0` issues one range request per read. The async reader has async `read` and `read_at`, with synchronous `seek`, `tell`, and `close`.

`open_writer(key)` returns a write-only `SyncWritableFile` (or `AsyncWritableFile`) that buffers writes into `part_size` slices (default `8_388_608` bytes) and uploads each full slice as a multipart part:

```python
import gzip
from vertebra.storage import MemoryConfig, SyncObjectStore

store = SyncObjectStore(MemoryConfig(namespace="logs"))
with store.open_writer("logs/day.gz", content_type="application/gzip") as raw:
    with gzip.GzipFile(fileobj=raw, mode="wb") as compressed:
        compressed.write(b"line\n" * 1000)
print(raw.result.size > 0)
```

Closing the writer uploads the tail and commits the object; `result` holds the `PutResult`. An exception escaping the `with` block, or `abort()`, discards the upload. `SyncWritableFile` subclasses `io.RawIOBase`, so `gzip.GzipFile` and `io.BufferedWriter` wrap it directly. On the async writer, `await writer.aclose()` returns `PutResult | None` (`None` after an abort), and an unclosed writer emits a `ResourceWarning` when collected.

## Arrow IPC and Parquet batches

With the `arrow` extra installed, both stores read and write Arrow IPC and Parquet. Async writes are awaited and async readers return async iterators directly:

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

`write_arrow(key, data, batch_size=65536)` writes IPC streams; `iter_arrow_batches(key)` reads them. Writers accept a Table, RecordBatch, ArrowCursor, or synchronous batch iterable and return `PutResult`. `write_arrow` raises `ValueError` on an empty iterable; `write_parquet` accepts one only together with `schema=...`.

`write_parquet` accepts `schema`, `compression="zstd"`, `row_group_size=65536`, and two encoding controls:

- `use_dictionary=True` dictionary-encodes every column; pass `False` or a list of column names.
- `use_byte_stream_split=False` leaves floating-point columns plain; pass `True` or a list of column names to apply byte-stream-split encoding, which usually compresses numeric data better.

`options` is forwarded to `pyarrow.parquet.ParquetWriter` and overrides the keyword arguments:

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

Batch readers advance with consumer demand. `iter_parquet_batches` fetches each row group only when the consumer asks for it, with `pre_buffer=False` and no read-ahead; pass `options={"pre_buffer": True}` to coalesce column-chunk reads instead. Whole-table reads through the SQLSpec adapter's `read_arrow_*` default to `pre_buffer=True`. Async codec work runs off-loop while native range, stream, and multipart I/O stays on the caller loop, and cancellation waits for codec and I/O cleanup before releasing owners.

## Multiple processes

Native stores run I/O on a process-wide Tokio runtime with its own threads. A child created with `fork` does not inherit those threads, so a store created before the fork is unsafe to use in the child. Use the `spawn` start method and construct stores inside each worker:

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

Threads within one process can share a single store.

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
