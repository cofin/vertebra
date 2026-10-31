# PyO3 Bridge & Worker Lanes (`vertebra-py`)

`vertebra-py` compiles to the `vertebra._vertebra` native module, bridging Rust `RequestBatch` buffers to Python's native, ASGI, RSGI, and WSGI execution loops.

## Parallelism Strategies

Vertebra supports two runtime topologies (`embedded` and `shm`) across both standard GIL-enabled CPython (`3.12`–`3.15`) and free-threaded CPython (`3.14t`/`3.15t`):

| Build Flavor | Workers | Topology | Execution Strategy |
| --- | --- | --- | --- |
| Standard GIL Python | `1` (`0`) | `embedded` | Single-process Tokio ingress + Python event loop |
| Standard GIL Python | `N` | `shm` | Rust parent process + `N` isolated Python worker processes over SPSC shared-memory rings |
| Free-Threaded Python (`3.14t`/`3.15t`) | `N` (`0`) | `embedded` | Single-process multi-threaded Python workers executing in parallel without a GIL (`FREE_THREADED = True`) |
| Free-Threaded Python (`3.14t`/`3.15t`) | `N` | `shm` | Multi-process SHM isolation combined with free-threaded worker pools per process |

### Compile-Time `FREE_THREADED` Gate

`vertebra-py` exposes a single compile-time boolean constant indicating whether the extension was compiled with `Py_GIL_DISABLED`:

```rust
#[cfg(Py_GIL_DISABLED)]
pub const FREE_THREADED: bool = true;
#[cfg(not(Py_GIL_DISABLED))]
pub const FREE_THREADED: bool = false;
```

All blocking Rust operations in `vertebra-py`, `vertebra-bundler`, and `vertebra-storage` detach from the Python runtime via `py.detach(|| ...)` so Rust I/O and bundling never block Python threads.

## Vectorized Batch Dispatch & Worker Lanes

When a `RequestBatch` arrives in `vertebra.server.worker.Worker`:

1. **`partition_batch`**: Inspects pre-compiled `HandlerSpec` metadata (`compile_handler_table`) to partition requests in the batch by target execution lane (`fast`, `async`, `cpu`, `blocking`).
2. **Zero-Copy Buffer Views**: Request headers and bodies are exposed via `Request` and `RequestBatch` views backed by Rust memory, avoiding redundant `bytes` copies when decoding `msgspec.Struct` payloads.
3. **Sharded `EventLoopPool`**: Coroutine handlers run on a sharded pool of `asyncio` event loops (`vertebra.server._loops.EventLoopPool`) with optional CPU core affinity (`configure_worker_affinity`), returning completed batches via `submit_batch_raw`.
