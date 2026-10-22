---
type: Guide
title: "PyO3 FFI & Vectorized Batch Dispatch"
description: "The vertebra._vertebra module surface, snapshot-backed Request/Headers/Response/RequestBatch types, and how run_worker partitions and schedules each batch."
status: stable
tags: [architecture, worker, pyo3, ffi, batch-dispatch]
sources:
  - resource: src/rs/vertebra-py/src/lib.rs
  - resource: src/rs/vertebra-py/src/api.rs
  - resource: src/rs/vertebra-py/src/batch.rs
  - resource: src/rs/vertebra-py/src/request.rs
  - resource: src/rs/vertebra-py/src/headers.rs
  - resource: src/rs/vertebra-py/src/response.rs
  - resource: src/py/vertebra/_vertebra.pyi
  - resource: src/py/vertebra/server/_native.py
  - resource: src/py/vertebra/server/_runtime_tuning.py
---

# PyO3 FFI & Vectorized Batch Dispatch

[`vertebra-py`](../../../../../src/rs/vertebra-py/src/lib.rs) builds the `vertebra._vertebra` extension (`#[pymodule(gil_used = false)]`, stub in [`_vertebra.pyi`](../../../../../src/py/vertebra/_vertebra.pyi)). [`run_worker`](../../../../../src/py/vertebra/server/_native.py) pulls request batches through it, runs handlers, and submits native `Response` objects back.

## Module surface (`lib.rs`, `api.rs`)

- Constants: `__version__`, `FREE_THREADED` (`cfg(Py_GIL_DISABLED)`), `GIL_USED` (`False`).
- Request-path classes: `Request`, `Headers`, `HeadersIterator`, `Response`, `RequestBatch`, `RequestBatchIterator`.
- Protocol classes: `AsgiScope`, `AsgiScopeIterator`, `WsgiEnviron`, `WsgiEnvironIterator`, `WsgiInput`, `WsgiFileWrapper`, `_WsgiStartResponse`, `RsgiHeaders`, `RsgiScope`, `RsgiHttpProtocol`, `RsgiTransport`, `RsgiWebsocketProtocol` (see [ASGI, RSGI & WSGI Runners](asgi-and-wsgi-runners.md)).
- `init_transport(mode="inprocess", ring_name="default", slot_size=4096, slots=64, batch_size=64)`: `inprocess` uses the process-wide `BatchBridge`; `shm` attaches the inherited ring FDs with `RingPair::from_env` and stores them in the `SHM_TRANSPORT` `OnceLock`.
- `get_batch(timeout_ms=100)` and `try_get_batch()` return a `RequestBatch` or `None`; `get_batch` releases the GIL while polling. `get_request_notify_fd()` and `clear_request_notification()` support the SHM `asyncio` reader.
- `submit_response(resp)` and `submit_results(responses)` release the GIL around the send. When an SHM response ring is full, `send_shm_response_with_backpressure` waits with the GIL released.
- `submit_raw_response(request_id, status, headers, body, flags=FLAG_FINAL, slot_capacity=16384)` encodes `ResponseSlot` frames and splits bodies larger than `slot_capacity` into `FLAG_STREAMING` chunks. It sends only when `SHM_TRANSPORT` is set and always returns the frames as `list[bytes]`. In-process mode sends nothing.
- `stream_file_chunks` and `run_wsgi_callable` are protocol helpers.

## Snapshot-backed types (`request.rs`, `headers.rs`, `response.rs`, `batch.rs`)

- [`RequestBatch`](../../../../../src/rs/vertebra-py/src/batch.rs) wraps `Arc<BatchSnapshot>`; iteration yields `Request` objects without copying slab bytes.
- [`Request`](../../../../../src/rs/vertebra-py/src/request.rs) (`frozen`, `freelist = 128`) holds `Arc<BatchSnapshot>` plus an index. Properties (`method`, `path`, `raw_path`, `query`, `raw_query`, `body`, `http_version`, `handler_id`, `request_id`, `flags`) read the slab on access. `headers` copies the header block once into a cached `Headers`. `asgi_scope`, `wsgi_environ`, `rsgi_scope`, and `rsgi_protocol` build the protocol objects. `Request.from_parts` builds a standalone request for tests.
- `Request.body_view` is a read-only `memoryview` over the slab (`PyMemoryView_FromMemory`, `PyBUF_READ`). Nothing keeps the snapshot alive, so the view is valid only while the handler runs; call `bytes(request.body_view)` to keep the body.
- [`Headers`](../../../../../src/rs/vertebra-py/src/headers.rs) (`frozen`, `freelist = 128`) is a case-insensitive mapping: `__getitem__`, `get`, `keys`, `values`, `items`, `asgi_items`, `as_dict`, `__contains__`, `__len__`, `__iter__`, and `Headers.from_pairs`. `content-type`, `content-length`, `authorization`, `accept`, `host`, `user-agent`, `cookie`, `upgrade`, and `connection` keys are interned with `pyo3::intern!`; other keys allocate a new `PyString`.
- [`Response`](../../../../../src/rs/vertebra-py/src/response.rs) (`freelist = 128`) holds `status: u16`, `body: Bytes`, `headers: Vec<(Vec<u8>, Vec<u8>)>`, and `request_id: u64`. The constructor and `Response.from_buffer` accept any buffer-protocol body and copy it into `Bytes` while holding the GIL (non-contiguous buffers fall back to `PyBuffer::to_vec`).
- `RequestBatch.submit_buffers(items)` returns `list[Response]` aligned to the batch; it does not submit.

`body_view`, `submit_buffers`, and `submit_raw_response` have no callers in `src/py/vertebra`; the runtime path uses `Response(...)` plus `submit_response`.

## Startup handler specs (`_native.py`)

`run_worker` calls `_handler_dispatch_spec(handler)` for every handler at startup. The function is wrapped in `functools.lru_cache(maxsize=512)` and returns `(has_params, is_async, params, invoker)` from one `inspect.signature` call. `_partition_batch` calls it per request, so apps with more than 512 handlers re-run `inspect.signature` after cache eviction.

## `BatchDispatchPlan` partitioning and scheduling

[`_partition_batch(batch, handlers, mount_runners)`](../../../../../src/py/vertebra/server/_native.py) fills a [`BatchDispatchPlan`](../../../../../src/py/vertebra/server/_native.py) (`msgspec.Struct, gc=False`) with four groups. `run_worker` and `_shm_worker_loop` always call `_dispatch_batch(..., async_mode=True)`:

1. `sync_native`: each handler is submitted to the shared `vertebra-wsgi` `ThreadPoolExecutor` (`max(128, cpu_count * 8)` threads) and completed with `submit_response`.
2. `async_native`: coroutines go to `scheduler.spawn_batch`. `AsyncRuntime` wakes its loop once per batch. `AsgiEventLoopPool` wakes once per non-empty shard. In SHM mode without a pool, `_shm_worker_loop` calls `loop.create_task` on its own running loop.
3. `mounts` (`dict[str, list[tuple[int, Request]]]`): ASGI mounts use `AsgiRunner.spawn_batch` (or `_AsgiMountRunner` across pool loops), RSGI mounts schedule `RsgiAdapter.handle_request` coroutines, and WSGI mounts go to the executor.
4. `unmatched`: answered with 404 `not found`.

Handler errors produce 500 `internal server error`. `_coerce_response` passes `Response` objects through (filling `request_id` when it is 0), encodes `str` as UTF-8, passes `bytes` through, and encodes anything else with `encode_json_bytes`.

The blocking `async_mode=False` path (direct callers and tests) runs sync handlers inline and uses `schedule_batch`.

## Scheduler selection (`run_worker`)

- `pool_size` is `worker_count` (passed by the embedded server and SHM workers from `VERTEBRA_WORKERS`), else `VERTEBRA_THREADS`, else `os.cpu_count()`.
- `use_pool = pool_size > 1 or is_free_threaded()` selects [`AsgiEventLoopPool(size=pool_size)`](../../../../../src/py/vertebra/server/_runtime_tuning.py). Otherwise in-process mode uses `AsyncRuntime` and SHM mode uses the `asyncio.run` loop.
- In-process mode loops on `get_batch(timeout_ms=batch_timeout_ms)`. SHM mode runs `_shm_worker_loop`, which registers the request notify FD with `loop.add_reader` and drains with `try_get_batch`.
