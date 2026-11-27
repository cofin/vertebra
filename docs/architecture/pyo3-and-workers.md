# PyO3 Bridge, Worker Lanes & Data Plane

`vertebra-py` compiles to the `vertebra._vertebra` extension module. It bridges Rust request batches, `vertebra-db` connection pools, and `vertebra-storage` object stores into Python workers running native, ASGI, RSGI, and WSGI apps.

## Worker Modes & Free-Threaded Python

Vertebra supports `embedded` and `shm` topologies on both standard GIL Python and free-threaded Python. When `--topology` is omitted or set to `auto`, running with `--workers > 1` selects `shm` on standard GIL builds and stays `embedded` on free-threaded builds:

| Build Flavor | Workers | Default Mode (`auto`) | How It Runs |
| --- | --- | --- | --- |
| Standard GIL Python | `1` or `0` | `embedded` | Single process running both the Rust HTTP server and Python |
| Standard GIL Python | `N > 1` | `shm` (override with `--topology embedded`) | Rust parent process plus `N` Python worker processes over shared memory |
| Free-Threaded Python | `N` or `0` | `embedded` | Single process with parallel Python worker threads and no GIL |
| Free-Threaded Python | `N` | `--topology shm` (explicit) | Multi-process shared memory with free-threaded worker pools per process |

### Compile-Time Free-Threaded Flag

`vertebra-py` exports a `FREE_THREADED` boolean indicating whether the extension was built with `Py_GIL_DISABLED`:

```rust
#[cfg(Py_GIL_DISABLED)]
pub const FREE_THREADED: bool = true;
#[cfg(not(Py_GIL_DISABLED))]
pub const FREE_THREADED: bool = false;
```

Blocking Rust work in `vertebra-py`, `vertebra-bundler`, `vertebra-db`, and `vertebra-storage` detaches from the Python GIL (`py.detach`) so network I/O and bundling never stall Python threads.

## Batch Dispatch & Worker Lanes

When a request batch arrives in a Python worker:

1. **Lane Partitioning**: Pre-compiled handler metadata splits the batch into `fast`, `async`, `cpu`, and `blocking` lanes before execution.
2. **Direct Buffer Views**: Request headers and bodies are exposed as views over Rust-owned memory, avoiding extra copies when decoding `msgspec.Struct` payloads.
3. **Sharded Event Loops**: Async handlers run across a sharded `asyncio` loop pool with optional CPU core pinning, then submit completed response batches back to Rust.

## Database Pools & Object Storage

- **`vertebra-db`**: Implements Rust connection pools, prepared statement caches, cross-worker connection leases, Apache Arrow shared-memory cursors, and Cloud SQL (`:3307`) and AlloyDB (`:5433`) dialers exposed via `vertebra._vertebra`.
- **`vertebra-storage`**: Compiles to `vertebra._storage`, wrapping the Rust `object_store` crate with zero-copy buffer views, random-access readers for PyArrow and Parquet, and presigned URL support for S3, GCS, Azure, R2, OCI, and HMAC schemes.
