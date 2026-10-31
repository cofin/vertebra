# PyO3 Bridge, Worker Lanes & Native Storage/DB (`vertebra-py`, `vertebra-db`, `vertebra-storage`)

`vertebra-py` compiles to the `vertebra._vertebra` native module, connecting Rust `RequestBatch` buffers, database pools (`vertebra-db`), and object storage (`vertebra-storage`) to Python's native, ASGI, RSGI, and WSGI workers.

## Worker Modes & Free-Threaded Python

Vertebra supports two worker modes (`embedded` and `shm`) across both standard GIL-enabled Python (`3.12`–`3.15`) and free-threaded Python (`3.14t`/`3.15t`). When `--topology` is omitted (or set to `auto`), `--workers > 1` automatically uses `shm` on standard GIL builds and stays `embedded` on free-threaded builds:

| Build Flavor | Workers | Default Mode (`auto`) | How It Runs |
| --- | --- | --- | --- |
| Standard GIL Python | `1` (`0`) | `embedded` | Single process running both the Rust HTTP server and Python |
| Standard GIL Python | `N > 1` | `shm` (override with `--topology embedded`) | Rust parent process + `N` Python worker processes over shared memory |
| Free-Threaded Python (`3.14t`/`3.15t`) | `N` (`0`) | `embedded` | Single process with multiple Python worker threads running in parallel without a GIL (`FREE_THREADED = True`) |
| Free-Threaded Python (`3.14t`/`3.15t`) | `N` | `--topology shm` (explicit) | Multi-process shared memory combined with free-threaded worker pools per process |

### Compile-Time `FREE_THREADED` Flag

`vertebra-py` exposes a boolean constant indicating whether the extension was compiled for free-threaded Python (`Py_GIL_DISABLED`):

```rust
#[cfg(Py_GIL_DISABLED)]
pub const FREE_THREADED: bool = true;
#[cfg(not(Py_GIL_DISABLED))]
pub const FREE_THREADED: bool = false;
```

All blocking Rust work in `vertebra-py`, `vertebra-bundler`, `vertebra-db`, and `vertebra-storage` releases the Python GIL (`py.detach(|| ...)`) so Rust I/O and bundling never block Python threads.

## Batch Dispatch & Worker Lanes

When a `RequestBatch` arrives in `vertebra.server.worker.Worker`:

1. **`partition_batch`**: Uses pre-compiled `HandlerSpec` metadata (`compile_handler_table`) to group requests by target lane (`fast`, `async`, `cpu`, `blocking`).
2. **Direct Buffer Views**: Request headers and bodies are exposed via `Request` and `RequestBatch` views backed by Rust memory, avoiding extra byte copies when decoding `msgspec.Struct` models.
3. **Sharded `EventLoopPool`**: Async handlers run on a pool of `asyncio` event loops (`vertebra.server._loops.EventLoopPool`) with optional CPU core pinning (`configure_worker_affinity`), returning completed batches via `submit_batch_raw`.

## Native Database Pools & Object Storage (`vertebra-db`, `vertebra-storage`)

- **`vertebra-db`**: Implements Rust connection pools, prepared statement caches (`StatementCache`), cross-worker connection leases (`LeaseBroker`), Arrow shared-memory cursors (`ArrowCursor`), and Cloud SQL (`:3307`) / AlloyDB (`:5433`) dialers exposed through `vertebra._vertebra`.
- **`vertebra-storage`**: Compiles to `vertebra._storage`, wrapping the Rust `object_store` crate with zero-copy `StorageBuffer` views, `ReadableFile` random-access readers for PyArrow/Parquet, and cloud URL presigning (`S3`, `GCS`, `Azure`, `R2`, `OCI`, `HMAC`).
