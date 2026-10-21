---
type: Guide
title: "PyO3 Zero-Copy FFI & Vectorized Batch Dispatch"
description: "Slab-backed PyO3 Request/Headers/Response/RequestBatch wrappers, zero-copy Py_buffer views, PyOnceLock header interning, and 4-way BatchDispatchPlan."
status: stable
tags: [architecture, worker, pyo3, ffi, zero-copy, batch-dispatch]
sources:
  - resource: src/rs/vertebra-py/src/api.rs
  - resource: src/rs/vertebra-py/src/batch.rs
  - resource: src/rs/vertebra-py/src/request.rs
  - resource: src/rs/vertebra-py/src/headers.rs
  - resource: src/rs/vertebra-py/src/response.rs
  - resource: src/py/vertebra/server/_native.py
---

# PyO3 Zero-Copy FFI & Vectorized Batch Dispatch

[`vertebra-py`](../../../../../src/rs/vertebra-py/src/lib.rs) (`vertebra._vertebra`) exposes slab-backed Python types, zero-copy `Py_buffer` views, and vectorized batch completion so Python workers in [`_native.py`](../../../../../src/py/vertebra/server/_native.py) process HTTP batches with minimal allocation and FFI transitions.

## 1. Slab-Backed Lazy `#[pyclass]` Types & Zero-Copy `Py_buffer` (`request.rs`, `headers.rs`, `response.rs`, `batch.rs`)

When [`get_batch`](../../../../../src/rs/vertebra-py/src/api.rs) or [`try_get_batch`](../../../../../src/rs/vertebra-py/src/api.rs) drains a batch, it wraps the underlying `BatchSnapshot` inside a [`RequestBatch`](../../../../../src/rs/vertebra-py/src/batch.rs) (`RequestBatchIterator`) without copying headers or bodies into Python heap objects:

- **[`Request`](../../../../../src/rs/vertebra-py/src/request.rs) & `Request.body_view`**: Lazily reads `method`, `path`, `query`, `body`, and `headers` from the underlying `DataSlab` on property access. `request.body_view` returns a zero-copy read-only Python `memoryview` (`PyMemoryView_FromMemory` with an attached `_SlabAnchor` keeping `Arc<DataSlab>` alive) so `msgspec.json.decode(request.body_view)` parses directly from the Rust slab without allocating a `PyBytes` copy. `Request.from_parts(...)` constructs standalone slab-backed requests in unit tests.
- **[`Headers`](../../../../../src/rs/vertebra-py/src/headers.rs) & `PyOnceLock` Interning**: Case-insensitive mapping backed by Rust header slices (`get`, `get_all`, `keys`, `values`, `items`, `__contains__`, `__len__`, `__iter__`). Common lowercase HTTP header keys (`content-type`, `authorization`, `accept`, `user-agent`, `host`, `cookie`, etc.) are interned via `PyOnceLock<Py<PyString>>` and `pyo3::intern!`.
- **[`Response`](../../../../../src/rs/vertebra-py/src/response.rs) & `Response.from_buffer`**: Holds `status: u16`, `headers: Vec<(String, String)>`, `body: Bytes`, and `request_id: u64`. `Response.from_buffer(...)` and `RequestBatch.submit_buffers(...)` extract payload bytes from any Python buffer-protocol object (`bytes`, `bytearray`, `memoryview`) via `PyBuffer::<u8>::get` and copy directly into Rust `Bytes` inside `py.detach`.
- **Direct Raw & Vectorized Submission ([`api.rs`](../../../../../src/rs/vertebra-py/src/api.rs))**: `submit_raw_response(request_id, status, headers, body)` writes status, header tuples, and body bytes directly into `BatchBridge` or `TieredRingSet::send_response_bytes` without constructing a `#[pyclass] Response`. `submit_results(responses)` and `submit_response(resp)` complete batch or individual responses while releasing the GIL (`py.detach`).

## 2. Startup Dispatch Caching (`_handler_dispatch_spec`)

At worker startup (`run_worker` in [`_native.py`](../../../../../src/py/vertebra/server/_native.py)), [`_handler_dispatch_spec(handler)`](../../../../../src/py/vertebra/server/_native.py) (`@functools.lru_cache(maxsize=512)`) inspects `inspect.signature(handler)` once and compiles a specialized invoker closure `(has_params, is_async, params, invoker)`. Request-time dispatch never calls `inspect.signature`.

## 3. 4-Way `BatchDispatchPlan` & Single-Wake `AsyncRuntime` (`_native.py`)

[`_partition_batch(batch, handlers, mount_runners)`](../../../../../src/py/vertebra/server/_native.py) splits each incoming `RequestBatch` into a 4-way [`BatchDispatchPlan`](../../../../../src/py/vertebra/server/_native.py):

1. **`sync_native`**: Synchronous native handlers executed on the worker thread (or submitted to the shared `ThreadPoolExecutor` in `async_mode=True`).
2. **`async_native`**: Async native coroutines scheduled onto [`AsyncRuntime`](../../../../../src/py/vertebra/server/_native.py) in a single cross-thread `loop.call_soon_threadsafe` wakeup via `scheduler.spawn_batch(async_coros)` or `scheduler.schedule_batch(coros)`.
3. **`mounts`**: Requests grouped by mount prefix (`dict[str, list[tuple[int, Request]]]`) and dispatched as a sub-batch to [`AsgiRunner.spawn_batch`](../../../../../src/py/vertebra/server/_asgi/_runner.py) or [`WsgiAdapter`](../../../../../src/py/vertebra/server/_wsgi/_adapter.py).
4. **`unmatched`**: Unmatched requests completed immediately with `404 Not Found`.
