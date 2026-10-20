---
name: worker-pyo3-ffi-and-batch-dispatch
description: Slab-backed PyO3 Request/Headers/Response/RequestBatch wrappers, 4-way BatchDispatchPlan, and single-wake AsyncRuntime.
sources:
  - src/rs/vertebra-py/src/api.rs
  - src/rs/vertebra-py/src/batch.rs
  - src/rs/vertebra-py/src/request.rs
  - src/rs/vertebra-py/src/headers.rs
  - src/rs/vertebra-py/src/response.rs
  - src/py/vertebra/server/_native.py
---

# PyO3 Zero-Copy FFI & Vectorized Batch Dispatch

[`vertebra-py`](../../../../../src/rs/vertebra-py/src/lib.rs) (`vertebra._vertebra`) exposes slab-backed Python types and vectorized batch completion so Python workers in [`_native.py`](../../../../../src/py/vertebra/server/_native.py) process HTTP batches with minimal allocation and FFI transitions.

## 1. Slab-Backed Lazy `#[pyclass]` Types (`request.rs`, `headers.rs`, `response.rs`, `batch.rs`)

When [`get_batch`](../../../../../src/rs/vertebra-py/src/api.rs) or [`try_get_batch`](../../../../../src/rs/vertebra-py/src/api.rs) drains a batch, it wraps the underlying `BatchSnapshot` inside a [`RequestBatch`](../../../../../src/rs/vertebra-py/src/batch.rs) (`RequestBatchIterator`) without copying headers or bodies into Python heap objects:

- **[`Request`](../../../../../src/rs/vertebra-py/src/request.rs)**: Lazily reads `method`, `path`, `query`, `body`, and `headers` from the underlying `DataSlab` on property access, caching materialized Python objects only for attributes the handler actually reads.
- **[`Headers`](../../../../../src/rs/vertebra-py/src/headers.rs) & [`HeadersIterator`](../../../../../src/rs/vertebra-py/src/headers.rs)**: Case-insensitive mapping backed by Rust header slices (`get`, `get_all`, `keys`, `values`, `items`, `__contains__`, `__len__`, `__iter__`).
- **[`Response`](../../../../../src/rs/vertebra-py/src/response.rs)**: Holds `status: u16`, `headers: Vec<(String, String)>`, `body: Bytes`, and `request_id: u64`.
- **Vectorized & Streaming Submission ([`api.rs`](../../../../../src/rs/vertebra-py/src/api.rs))**: `submit_results(responses)` submits an entire batch while releasing the GIL (`py.detach`), and `submit_response(resp)` completes individual responses in `async_mode=True`.

## 2. Startup Dispatch Caching (`_handler_dispatch_spec`)

At worker startup (`run_worker` in [`_native.py`](../../../../../src/py/vertebra/server/_native.py)), [`_handler_dispatch_spec(handler)`](../../../../../src/py/vertebra/server/_native.py) (`@functools.lru_cache(maxsize=512)`) inspects `inspect.signature(handler)` once and compiles a specialized invoker closure `(has_params, is_async, params, invoker)`. Request-time dispatch never calls `inspect.signature`.

## 3. 4-Way `BatchDispatchPlan` & Single-Wake `AsyncRuntime` (`_native.py`)

[`_partition_batch(batch, handlers, mount_runners)`](../../../../../src/py/vertebra/server/_native.py) splits each incoming `RequestBatch` into a 4-way [`BatchDispatchPlan`](../../../../../src/py/vertebra/server/_native.py):

1. **`sync_native`**: Synchronous native handlers executed on the worker thread (or submitted to the shared `ThreadPoolExecutor` in `async_mode=True`).
2. **`async_native`**: Async native coroutines scheduled onto [`AsyncRuntime`](../../../../../src/py/vertebra/server/_native.py) in a single cross-thread `loop.call_soon_threadsafe` wakeup via `scheduler.spawn_batch(async_coros)` or `scheduler.schedule_batch(coros)`.
3. **`mounts`**: Requests grouped by mount prefix (`dict[str, list[tuple[int, Request]]]`) and dispatched as a sub-batch to [`AsgiRunner.spawn_batch`](../../../../../src/py/vertebra/server/_asgi/_runner.py) or [`WsgiAdapter`](../../../../../src/py/vertebra/server/_wsgi/_adapter.py).
4. **`unmatched`**: Unmatched requests completed immediately with `404 Not Found`.
