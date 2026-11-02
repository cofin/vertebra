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
  - resource: src/rs/vertebra-py/src/msgspec_bridge.rs
  - resource: src/py/vertebra/_vertebra.pyi
  - resource: src/py/vertebra/server/worker.py
  - resource: src/py/vertebra/server/_dispatch.py
  - resource: src/py/vertebra/server/_loops.py
  - resource: src/py/vertebra/server/_fastpath.py
---

# PyO3 FFI & Vectorized Batch Dispatch

[`vertebra-py`](../../../../../src/rs/vertebra-py/src/lib.rs) builds the `vertebra._vertebra` extension (`#[pymodule(gil_used = false)]`, typed in [`_vertebra.pyi`](../../../../../src/py/vertebra/_vertebra.pyi)), alongside `_bundler` (`vertebra-bundler`) and `_storage` (`vertebra-storage`). [`Worker`](../../../../../src/py/vertebra/server/worker.py) (`run_worker`) pulls `RequestBatch` snapshots across FFI, executes inline vectorized sync, WSGI (`drive_wsgi_batch`), and single-thread `uvloop`/`winloop`/`rloop` async/ASGI/RSGI batches (`drive_native_batch`, `drive_asgi_batch`, `drive_rsgi_batch`), and flushes raw responses back to Rust.

## Module surface (`lib.rs`, `transport.rs`)

- **Constants**: `__version__`, `FREE_THREADED` (`cfg(Py_GIL_DISABLED)`), `GIL_USED` (`False`).
- **Request-path classes**: `Request`, `Headers`, `HeadersIterator`, `Response`, `RequestBatch`, `RequestBatchIterator`. All non-subclassed `#[pyclass]` types declare `immutable_type` (`Py_TPFLAGS_IMMUTABLETYPE` for CPython 3.14/3.15 inline-cache specialization) and gate `freelist = 128` behind `#[cfg_attr(not(Py_GIL_DISABLED), ...)]` so free-threaded `3.14t`/`3.15t` builds use thread-local `mimalloc` without global `Mutex` freelist contention.
- **Protocol & batch-driver surface**: `AsgiScope` (including zero-awaitable `body_bytes()`), `AsgiScopeIterator`, `AsgiProtocol` (including `__call__`, `body_bytes()`, and single-call `send_raw_response`), `PyEmptyAwaitable`, `PyValueAwaitable`, `drive_asgi_batch`, `drive_native_batch`, `RsgiScope`, `RsgiHttpProtocol`, `RsgiTransport`, `_RsgiReadyAwaitable`, `drive_rsgi_batch`, `WsgiEnviron`, `WsgiEnvironIterator`, `WsgiInput`, `WsgiFileWrapper`, `_WsgiStartResponse` (including `send_raw_response`), `run_wsgi_callable`, `drive_wsgi_batch`, and `stream_file_chunks` (see [ASGI, RSGI & WSGI Runners](asgi-and-wsgi-runners.md)).
- **Transport & Dekker sleep FFI**:
  - `init_transport(mode="inprocess", ...)`: attaches either the process-wide `BatchBridge` (`inprocess`) or inherited `RingPair::from_env` FDs (`shm`).
  - `get_batch(timeout_ms=100)` and `try_get_batch()`: return a `RequestBatch` wrapping `Arc<BatchSnapshot>` (`Backing::InProcess` or zero-copy `Backing::ShmSlot(SlotView)`), checking `poll_batch_async()` / `try_poll_batch()` before `py.detach` so ready batches return without releasing the GIL, and spinning briefly in user-space before arming Dekker sleep.
  - `get_request_notify_fd()`, `clear_request_notification()`, `prepare_request_sleep()`, and `cancel_request_sleep()`: drive lost-wakeup-free Dekker `SeqCst` `eventfd`/pipe polling on both `RingPair` (`shm`) and single-consumer `BatchBridge` (`inprocess` when `consumer_count <= 1`).
  - `submit_response_raw(request_id, status, headers_raw, body)` and `submit_batch_raw(items)`: accept any contiguous buffer-protocol object (`bytes`, `bytearray`, `memoryview`, including `req.body_view`), write directly into the SHM response ring or `BatchBridge` inline without `py.detach` for payloads `<= 64 KiB`, and coalesce `notify_if_sleeping` to once per batch.
  - `submit_response(resp)`, `submit_results(responses)`, and `submit_raw_response(...)`: object and multi-frame `ResponseSlot` streaming submission helpers.

## Snapshot-backed types & C-API zero-copy bridge (`request.rs`, `headers.rs`, `response.rs`, `msgspec_bridge.rs`)

- [`RequestBatch`](../../../../../src/rs/vertebra-py/src/request.rs) wraps `Arc<BatchSnapshot>` and yields `Request` instances without copying request headers or bodies.
- [`Request`](../../../../../src/rs/vertebra-py/src/request.rs) (`frozen`, `immutable_type`, conditional `freelist = 128`) holds `Arc<BatchSnapshot>` + item index and implements CPython's buffer protocol (`__getbuffer__` / `__releasebuffer__`, typed with PEP 688 `def __buffer__(self, flags: int, /) -> memoryview: ...` in `_vertebra.pyi`). `__getbuffer__` passes borrowed `slf.as_ptr()` to `PyBuffer_FillInfo` (which calls `Py_XNewRef` internally), so `memoryview(req)` and `req.body_view` return an anchored zero-copy read-only `memoryview` pinning `Request` -> `Arc<BatchSnapshot>` safely even after `del req`, and flowing zero-copy into `submit_response_raw` / `submit_batch_raw`.
- [`msgspec_bridge.rs`](../../../../../src/rs/vertebra-py/src/msgspec_bridge.rs) exposes C-API zero-copy decode, vectorcall row hydration, and gRPC LPM framing primitives on `Request`, `WsgiInput`, `AsgiScope`, and `vertebra._vertebra`:
  - `Request.decode_msgspec(target_type=None, format="json")`, `WsgiInput.decode_msgspec(...)`, and `AsgiScope.decode_msgspec(body_view, target_type=None, format="json")` invoke cached `msgspec.json.Decoder(ty).decode` or `msgspec.msgpack.Decoder(ty).decode` via `PyObject_CallOneArg(decoder_fn, slf.as_ptr())` so `msgspec._core` reads the zero-copy `__getbuffer__` pointer in-place.
  - `Request.decode_pydantic_json(adapter_or_model)` invokes `TypeAdapter.validate_json` / `BaseModel.model_validate_json` directly over the `Request` buffer protocol view.
  - `hydrate_one_vectorcall`, `hydrate_rows_vectorcall`, `instantiate_struct_vectorcall`, and `orm_to_struct_vectorcall` invoke `PyObject_Vectorcall` with stack-allocated/scratch `[*mut PyObject]` argument slices (`PY_VECTORCALL_ARGUMENTS_OFFSET`) to construct `msgspec.Struct`, `@dataclass`, `@attrs.define`, `NamedTuple`, `TypedDict`, or Pydantic v2 instances without intermediate `dict` or `zip()` allocations.
  - `pack_grpc_lpm_frame(buf, compressed=False)` and `decode_grpc_lpm_frames_zero_copy(payload)` prepend and slice 5-byte gRPC Length-Prefixed-Message headers (`[u8 compressed, u32be length]`) in place over `bytearray` and `PyMemoryView_FromMemory` views.
- [`Headers`](../../../../../src/rs/vertebra-py/src/headers.rs) and [`Response`](../../../../../src/rs/vertebra-py/src/response.rs) allocate byte outputs via `PyBytes::new_with` (PEP 782 `PyBytesWriter` zero-copy initialization on Python 3.15+) and intern common HTTP header names via `pyo3::intern!`.

## Startup handler compilation & batch execution (`worker.py`, `_dispatch.py`, `_loops.py`)

- [`compile_handler_table`](../../../../../src/py/vertebra/server/_dispatch.py) inspects every registered route once at startup into a frozen [`HandlerSpec`](../../../../../src/py/vertebra/server/_dispatch.py) (`msgspec.Struct, frozen=True, gc=False, cache_hash=True`) carrying `(has_params, is_async, param_names, invoker, sync_to_thread)`. Route decorators (`@app.get(..., sync_to_thread=False)`) default `sync_to_thread` to `False`.
- **Inline vectorized sync execution**: default synchronous native routes (`sync_to_thread=False`) run inline in the batch loop and flush together via a single `submit_batch_raw(raw_responses)` call (`0` `ThreadPoolExecutor` submissions and `0` `Future` allocations). Routes opting into `sync_to_thread=True` execute once on `ThreadPoolExecutor` without duplicate inline execution.
- **Zero-copy buffer serialization, C-API `drive_native_batch` & async snapshot release**: `_serialize_result` accepts `bytes`, `bytearray`, and `memoryview` (`_RAW_BUFFER_TYPES`) directly so `return req.body_view` incurs zero payload copies, and encodes structured objects via `encode_json_bytes`. When an `async def` native handler has only synchronous guards and dependencies, `compile_handler_spec` compiles `_invoke_compiled_sync_to_coro` so parameter extraction executes synchronously and returns the handler's direct coroutine without an outer wrapper frame. On the worker event loop, `dispatch_batch` steps `partitioned.async_native` coroutines in C via `drive_native_batch` (`PyIter_Send`): coroutines returning on the first step (`PYGEN_RETURN`) serialize and flush immediately with **zero** `asyncio.Task` allocations, while suspending coroutines (`PYGEN_NEXT`) promote via `SteppedCoroutine` (`ProtocolFinalizer::Native`) after dropping the local `Request` reference so I/O waits never pin `Arc<BatchSnapshot>`.
- **`O(1)` mount resolution & auto-attached `FastPath` stacking**: `vertebra-http` stamps mounted requests with `MOUNT_HANDLER_BASE | mount_idx` (`0xFFFF_0000_0000_0000 | idx`), allowing `Worker._process_batch` and `partition_batch` to index directly into `mounts[mount_idx]` in `O(1)` without decoding `req.path`. `_instantiate_mount_runners` calls [`attach_framework_fastpath`](../../../../../src/py/vertebra/server/_fastpath.py) on every mounted `Litestar`, `FastAPI`, `Starlette`, `Flask`, or `Django` sub-application so stacked `app.mount("/prefix", sub_app)` trees automatically execute through each framework's C-API `FastPath` with `mount_prefix` / `root_path` stripping.
- **Unified `LoopBackend` (`auto`, `uvloop`, `winloop`, `rloop`, `asyncio`) & same-thread `add_reader` polling**: [`_loops.py`](../../../../../src/py/vertebra/server/_loops.py) exposes `LoopBackend(StrEnum)` and `resolve_loop_backend`, selecting `uvloop` (`>=0.23.0`) on POSIX, `winloop` (`>=0.8.1`) on Windows, opt-in `rloop` (`>=0.5.0`), or stdlib `asyncio` via `create_event_loop` without deprecated `asyncio.AbstractEventLoopPolicy` APIs, and honoring `--loop` / `VERTEBRA_LOOP` and `GilAuditReport` `quarantined_modules`. When `loop_shards == 1` and a request notify FD is active, `Worker._run_inline_event_loop` registers `loop.add_reader(notify_fd, ...)` with Dekker `prepare_request_sleep()` / `cancel_request_sleep()` so polling and coroutine execution share one OS thread with **zero** cross-thread `call_soon_threadsafe` wakeups.
