---
type: Guide
title: "PyO3 FFI & Vectorized Batch Dispatch"
description: "The vertebra._vertebra module surface, anchored zero-copy Request/Headers/Response/RequestBatch types, and how Worker and run_worker partition and execute batches."
status: stable
tags: [architecture, worker, pyo3, ffi, batch-dispatch]
sources:
  - resource: src/rs/vertebra-py/src/lib.rs
  - resource: src/rs/vertebra-py/src/transport.rs
  - resource: src/rs/vertebra-py/src/request.rs
  - resource: src/rs/vertebra-py/src/headers.rs
  - resource: src/rs/vertebra-py/src/response.rs
  - resource: src/py/vertebra/_vertebra.pyi
  - resource: src/py/vertebra/server/worker.py
  - resource: src/py/vertebra/server/_dispatch.py
  - resource: src/py/vertebra/server/_loops.py
---

# PyO3 FFI & Vectorized Batch Dispatch

[`vertebra-py`](../../../../../src/rs/vertebra-py/src/lib.rs) builds the `vertebra._vertebra` extension (`#[pymodule(gil_used = false)]`, typed in [`_vertebra.pyi`](../../../../../src/py/vertebra/_vertebra.pyi)), alongside `_bundler` (`vertebra-bundler`) and `_storage` (`vertebra-storage`). [`Worker`](../../../../../src/py/vertebra/server/worker.py) (`run_worker`) pulls `RequestBatch` snapshots across FFI, executes inline vectorized sync batches and single-thread `uvloop` async/ASGI/RSGI batches, and flushes raw responses back to Rust.

## Module surface (`lib.rs`, `transport.rs`)

- **Constants**: `__version__`, `FREE_THREADED` (`cfg(Py_GIL_DISABLED)`), `GIL_USED` (`False`).
- **Request-path classes**: `Request`, `Headers`, `HeadersIterator`, `Response`, `RequestBatch`, `RequestBatchIterator`. All non-subclassed `#[pyclass]` types declare `immutable_type` (`Py_TPFLAGS_IMMUTABLETYPE` for CPython 3.14/3.15 inline-cache specialization) and gate `freelist = 128` behind `#[cfg_attr(not(Py_GIL_DISABLED), ...)]` so free-threaded `3.14t`/`3.15t` builds use thread-local `mimalloc` without global `Mutex` freelist contention.
- **Protocol & batch-driver surface**: `AsgiScope`, `AsgiScopeIterator`, `AsgiProtocol`, `PyEmptyAwaitable`, `PyValueAwaitable`, `drive_asgi_batch`, `RsgiScope`, `RsgiHttpProtocol`, `RsgiTransport`, `_RsgiReadyAwaitable`, `drive_rsgi_batch`, `WsgiEnviron`, `WsgiEnvironIterator`, `WsgiInput`, `WsgiFileWrapper`, `_WsgiStartResponse`, `run_wsgi_callable`, and `stream_file_chunks` (see [ASGI, RSGI & WSGI Runners](asgi-and-wsgi-runners.md)).
- **Transport & Dekker sleep FFI**:
  - `init_transport(mode="inprocess", ...)`: attaches either the process-wide `BatchBridge` (`inprocess`) or inherited `RingPair::from_env` FDs (`shm`).
  - `get_batch(timeout_ms=100)` and `try_get_batch()`: return a `RequestBatch` wrapping `Arc<BatchSnapshot>` (`Backing::InProcess` or zero-copy `Backing::ShmSlot(SlotView)`), checking `poll_batch_async()` / `try_poll_batch()` before `py.detach` so ready batches return without releasing the GIL, and spinning briefly in user-space before arming Dekker sleep.
  - `get_request_notify_fd()`, `clear_request_notification()`, `prepare_request_sleep()`, and `cancel_request_sleep()`: drive lost-wakeup-free Dekker `SeqCst` `eventfd`/pipe polling on both `RingPair` (`shm`) and single-consumer `BatchBridge` (`inprocess` when `consumer_count <= 1`).
  - `submit_response_raw(request_id, status, headers_raw, body)` and `submit_batch_raw(items)`: accept any contiguous buffer-protocol object (`bytes`, `bytearray`, `memoryview`, including `req.body_view`), write directly into the SHM response ring or `BatchBridge` inline without `py.detach` for payloads `<= 64 KiB`, and coalesce `notify_if_sleeping` to once per batch.
  - `submit_response(resp)`, `submit_results(responses)`, and `submit_raw_response(...)`: object and multi-frame `ResponseSlot` streaming submission helpers.

## Snapshot-backed types (`request.rs`, `headers.rs`, `response.rs`)

- [`RequestBatch`](../../../../../src/rs/vertebra-py/src/request.rs) wraps `Arc<BatchSnapshot>` and yields `Request` instances without copying request headers or bodies.
- [`Request`](../../../../../src/rs/vertebra-py/src/request.rs) (`frozen`, `immutable_type`, conditional `freelist = 128`) holds `Arc<BatchSnapshot>` + item index and implements CPython's buffer protocol (`__getbuffer__` / `__releasebuffer__`, typed with PEP 688 `def __buffer__(self, flags: int, /) -> memoryview: ...` in `_vertebra.pyi`). `__getbuffer__` passes borrowed `slf.as_ptr()` to `PyBuffer_FillInfo` (which calls `Py_XNewRef` internally), so `memoryview(req)` and `req.body_view` return an anchored zero-copy read-only `memoryview` pinning `Request` -> `Arc<BatchSnapshot>` safely even after `del req`, and flowing zero-copy into `submit_response_raw` / `submit_batch_raw`.
- [`Headers`](../../../../../src/rs/vertebra-py/src/headers.rs) and [`Response`](../../../../../src/rs/vertebra-py/src/response.rs) allocate byte outputs via `PyBytes::new_with` (PEP 782 `PyBytesWriter` zero-copy initialization on Python 3.15+) and intern common HTTP header names via `pyo3::intern!`.

## Startup handler compilation & batch execution (`worker.py`, `_dispatch.py`, `_loops.py`)

- [`compile_handler_table`](../../../../../src/py/vertebra/server/_dispatch.py) inspects every registered route once at startup into a frozen [`HandlerSpec`](../../../../../src/py/vertebra/server/_dispatch.py) (`msgspec.Struct, frozen=True, gc=False, cache_hash=True`) carrying `(has_params, is_async, param_names, invoker, sync_to_thread)`. Route decorators (`@app.get(..., sync_to_thread=False)`) default `sync_to_thread` to `False`.
- **Inline vectorized sync execution**: default synchronous native routes (`sync_to_thread=False`) run inline in the batch loop and flush together via a single `submit_batch_raw(raw_responses)` call (`0` `ThreadPoolExecutor` submissions and `0` `Future` allocations). Routes opting into `sync_to_thread=True` execute once on `ThreadPoolExecutor` without duplicate inline execution.
- **Zero-copy buffer serialization & async snapshot release**: `_serialize_result` accepts `bytes`, `bytearray`, and `memoryview` (`_RAW_BUFFER_TYPES`) directly so `return req.body_view` incurs zero payload copies, and encodes structured objects via `encode_json_bytes`. For `async def` native handlers, `_await_async_item` extracts `_AsyncErrorStub(request_id)` and drops the local `Request` reference *before* `await coro` so suspending handlers (`io10 c=256`) never pin `Arc<BatchSnapshot>` (`SlotView`) across I/O waits.
- **`O(1)` mount resolution**: `vertebra-http` stamps mounted requests with `MOUNT_HANDLER_BASE | mount_idx` (`0xFFFF_0000_0000_0000 | idx`), allowing `Worker._process_batch` and `partition_batch` to index directly into `mounts[mount_idx]` in `O(1)` without decoding `req.path`. When an app has a single root ASGI mount, `Worker._process_batch` passes `single_root_asgi.dispatch_app` directly to `drive_asgi_batch` so attached `__vertebra_fastpath__` handlers (`LitestarFastPathBypass`) run on the batch hot path.
- **`uvloop` & same-thread `add_reader` polling**: [`_loops.py`](../../../../../src/py/vertebra/server/_loops.py) automatically uses `uvloop.new_event_loop` on POSIX via `import_optional("uvloop")` (falling back to `winloop` on Windows and `asyncio` otherwise) and coalesces multi-loop scheduling in `EventLoopPool.spawn_batch`. When `loop_shards == 1` and a request notify FD is active (SHM mode or single-consumer Embedded mode with async/ASGI/RSGI routes; pure-sync Embedded native apps stay on direct blocking `get_batch`), `Worker._run_inline_event_loop` registers `loop.add_reader(notify_fd, ...)` with Dekker `prepare_request_sleep()` / `cancel_request_sleep()` so polling and coroutine execution share one OS thread with **zero** cross-thread `call_soon_threadsafe` wakeups.
