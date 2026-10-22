---
type: Spec
flow_id: bridge-and-server-consolidation
title: "Chapter 4: PyO3 bridge, Python server, and telemetry consolidation"
state: closed
plan_revision: 2
plan_commit: ""
state_revision: 1
current_task: null
last_operation: finish
operation_targets: [4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 4.9, 4.10]
last_verified_checkpoint: 4.10
created_at: 2026-10-22T03:30:00Z
updated_at: 2026-10-22T16:50:00Z
description: "Consolidate vertebra-py, vertebra.server, vertebra.supervisor, and vertebra.telemetry into deep modules with stubtest-verified type stubs, explicit file bodies, a single WSGI/ASGI/RSGI runtime path, and Rust-backed telemetry."
tags: [refactor, pyo3, python, server]
parent_prd: vertebra-design-quality
research: [../vertebra-design-quality/findings.md]
---

# Flow: Chapter 4: PyO3 bridge, Python server, and telemetry consolidation

**Flow ID:** `bridge-and-server-consolidation`

## Specification

### Parent Context

This chapter is Chapter 4 of the [`vertebra-design-quality`](../vertebra-design-quality/spec.md) master roadmap. It executes on branch `refactor/bridge-and-server-consolidation` branched from the tip of Chapter 3 (`http-ingress-consolidation`). It owns the PyO3 FFI bridge (`src/rs/vertebra-py/**`), Python server and protocol adapters (`src/py/vertebra/server/**`, `src/py/vertebra/supervisor/**`), telemetry (`src/py/vertebra/telemetry/**`, `src/rs/vertebra-core/src/{logger,telemetry}.rs`), and native extension module naming (`vertebra._vertebra`, `vertebra._bundler`, `vertebra._storage`) for roadmap requirements:

- **R4 (Correctness defects fixed test-first):** Propagate transport errors in `submit_raw_response` under `py.detach` (`PYB-03`), replace leaked `x-vertebra-sendfile` headers with explicit `ResponseBody::File` (`PYB-01`, `PYB-04`, `PYB-32`, `PYB-42`), combine duplicate WSGI headers per PEP 3333 (`PYB-29`), size event-loop shards from `VERTEBRA_LOOP_SHARDS` instead of `VERTEBRA_WORKERS` (`PYB-02`), stop advertising `tls` on plaintext ASGI scopes (`PYB-25`), make RSGI `client_disconnect()` await actual disconnect/shutdown (`PYB-37`), and fix UTF-8 and `JOIN (` corruption in `sanitize_sql_statement` (`CORE-34`).
- **R5 (Dead code and duplicate removal):** Delete `src/py/vertebra/_bundler.py` (`PYB-58`, `FE-12`), `AsyncRuntime` (`PYB-08`), Python `AsgiScope` fallback (`PYB-26`), Python `WsgiEnviron`/`BodyStream`/`WsgiRunner` (`PYB-31`), blocking `_dispatch_batch(async_mode=False)` and unused handler introspection helpers (`PYB-15`, `PYB-16`, `PYB-48`), placeholder `_ipc`/`_js` packages (`PYB-59`), `RsgiHeaders` (`PYB-34`), and dead Rust functions/dependencies (`PYB-18`, `PYB-40`, `PYB-41`, `PYB-46`, `PYB-60`).
- **R6 (Honest capabilities and wired modules):** Stream `LargeBodyBypass` directly from the request slab (`PYB-33`), advertise only delivered ASGI extensions and replace the custom coroutine stepper with `asyncio.eager_task_factory` (`PYB-11`, `PYB-24`, `PYB-25`), move the in-memory `RsgiWebsocketProtocol` fake to test fixtures (`PYB-38`), bind Rust `NativeLogSink` (wrapping `AsyncLogCore` with `"stdout"`, `"stderr"`, `"memory"`, or file targets, background off-GIL flush thread, `emit()`, `flush()`, and `metrics()` returning `(emitted, flushed, dropped)`), `parse_traceparent`, `format_traceparent`, `sanitize_sql`, `NativeJwtGuard`, and `NativeRateLimiter` into `vertebra.telemetry` and `_vertebra.pyi` per Decision D4 (`PYB-55`, `PYB-56`, `CORE-13`), and wire `CgroupQuota`, `LameDuckController`, `ZPagesRegistry`, and `ShmAllocationPolicy` into the worker and supervisor (`PYB-53`, `PYB-54`).
- **R7 (Deep module reorganization):** Reorganize `src/rs/vertebra-py/src/` into `transport.rs`, `request.rs`, `synthetic.rs`, `headers.rs`, `response.rs`, `asgi.rs`, `wsgi/{mod,environ,input,runner}.rs`, `rsgi/{mod,scope,http}.rs`, `telemetry.rs`, and `file_io.rs`; reorganize `src/py/vertebra/server/` into `worker.py`, `_dispatch.py`, `_loops.py`, `_protocol.py`, `_gil_audit.py`, `platform.py`, `_asgi/`, `_wsgi/`, and `_rsgi/`.
- **R8 (Native static gates):** Synchronize `_vertebra.pyi` and add `_bundler.pyi` and `_storage.pyi`, enforced by `python -m mypy.stubtest` in `make lint` and `make lint-all` (`PYB-47`, `PYB-58`, `FE-12`), and eliminate function-scoped imports, `global` statements, and re-export shims across `server/`, `supervisor/`, and `telemetry/` (`PYB-22`, `PYB-62`).

### Code Analysis Summary

All 12 Rust source files in `src/rs/vertebra-py/src/` (3,660 lines) and 28 Python files across `src/py/vertebra/{server,supervisor,telemetry}/` and `src/py/vertebra/_vertebra.pyi` (4,580 lines at commit `edceea6`) were inspected:

- **Stubs, packaging, and dead FFI code (`_vertebra.pyi` 332 lines, `_bundler.py` 15 lines, `asgi_bridge.rs` 95 lines, `batch.rs` 86 lines, `lib.rs` 93 lines):**
  - `_vertebra.pyi` declares `AsgiScope.pop`, `WsgiEnviron.pop`, and `RequestBatchIterator.submit_buffers` (none exist in Rust), mistypes `raw_query`, `wsgi_environ`, and `Response` attributes, and omits `__version__` (`PYB-47`).
  - `_bundler` and `_storage` are built as top-level modules rather than `vertebra._bundler` and `vertebra._storage`, requiring the re-export shim `src/py/vertebra/_bundler.py` and `ignore_missing_imports` in `pyproject.toml` (`PYB-58`, `FE-12`).
  - `asgi_bridge.rs:12-41` `read_file_range_chunks` has zero callers and is the sole user of `tokio`, `tokio-util`, and `futures-util` in `vertebra-py` (`PYB-41`). `RequestBatch.submit_buffers` (`batch.rs:44-61`), `_assert_py_string` (`rsgi_protocol.rs:378-381`), `ring_name` in `init_transport` (`api.rs:34-41,71`), `Response.from_buffer`/`get_headers`/`get_body` (`response.rs:117-135,206-213`), and `send_shm_response_with_backpressure` (`api.rs:466-486`) are dead or redundant (`PYB-18`, `PYB-40`, `PYB-46`, `PYB-60`).
- **Transport submit and file responses (`api.rs` 573 lines, `response.rs` 221 lines):**
  - `submit_results` and `submit_response` (`api.rs:110-252`) duplicate SHM prep, error mapping, and bridge fallback (`PYB-05`); `submit_raw_response` (`api.rs:346-363,414-419`) runs transport I/O while attached to the GIL and swallows errors (`let _ =`) (`PYB-03`).
  - Four paths (`rsgi_protocol.rs:156-221`, `wsgi_worker.rs:117-120`, `_asgi/_queues.py:169-180`, `_wsgi/_adapter.py:103-104`) emit `x-vertebra-sendfile` headers that no Rust HTTP code reads, sending empty bodies on RSGI file responses and leaking internal filesystem paths to clients (`PYB-01`). `Response.body` (`response.rs:142-168`) performs blocking file I/O under the GIL (`PYB-04`), WSGI file wrappers stat files up to 3 times (`PYB-32`), and `stream_file_chunks` (`asgi_bridge.rs:44-95`) copies file bytes 3 times (`PYB-42`).
- **Request primitives (`request.rs` 274, `headers.rs` 210, `asgi_scope.rs` 611, `rsgi_scope.rs` 283):**
  - `from_parts` builders in `asgi_scope.rs:251-299`, `request.rs:62-108`, and `wsgi_environ.rs:438-501` duplicate method-tag tables and slab construction (`PYB-43`).
  - `RsgiHeaders` (`rsgi_scope.rs:11-123`) duplicates `Headers` (`headers.rs:63-176`) with eager `String` allocations (`PYB-34`), `HeadersIterator::__next__` (`headers.rs:178-210`) re-implements compact header parsing (`PYB-44`), `RsgiScope::from_batch` (`rsgi_scope.rs:142-171`) eagerly allocates 8 strings per request (`PYB-35`), and `Request.headers` (`request.rs:47-54`) and `AsgiReceive` (`_queues.py:58-68`) copy header/body bytes on every read (`PYB-28`, `PYB-45`).
- **WSGI, ASGI, RSGI, loops, and worker dispatch (`wsgi_environ.rs` 715, `wsgi_worker.rs` 128, `rsgi_protocol.rs` 381, `_native.py` 849, `_runtime_tuning.py` 246, `_gil_audit.py` 132, `_asgi/*` 1,362, `_wsgi/*` 572, `_rsgi/*` 162):**
  - `WsgiEnviron::ensure_headers` (`wsgi_environ.rs:286-298,338-385`) overwrites duplicate headers instead of combining with `", "` per PEP 3333 (`PYB-29`), formats every key on `len()`/`__iter__` (`PYB-30`), captures `sys.stderr` once per process (`PYB-50`), holds a `std::sync::Mutex` across Python C-API calls (`PYB-49`), and coexists with duplicate Python `WsgiEnviron`/`BodyStream`/`WsgiRunner` (`PYB-31`) and a no-op `LargeBodyBypass` (`PYB-33`).
  - `supervisor/_worker.py:50` passes `VERTEBRA_WORKERS` (process count) to `run_worker` as `worker_count` -> `AsgiEventLoopPool(size=pool_size)`, spawning `N` GIL-contending event loops inside each of `N` worker processes (`PYB-02`). `AsyncRuntime` (`_native.py:57-167`) duplicates `AsgiEventLoopPool` (`PYB-08`), `.loop` mutates state on read (`PYB-09`), `schedule` double-wraps coroutines (`PYB-10`), and `_gil_audit.py` duplicates `is_free_threaded` and logs quarantine without changing the loop policy (`PYB-51`, `PYB-52`).
  - `_native.py` calls `_handler_dispatch_spec` twice per request (`PYB-13`), allocates closures per request (`PYB-12`), repeats 14 inline error `Response` constructions while ignoring `error_handler` (`PYB-14`, `PYB-17`), keeps dead `async_mode=False` and `_handler_params` code (`PYB-15`, `PYB-16`, `PYB-48`), checks adapter types by `__name__` string (`PYB-19`), duplicates `detect_interface` (`PYB-21`), and contains 16 function-scoped imports, 5 `global` statements, and re-export shims (`PYB-22`, `PYB-62`).
  - `_asgi/_runner.py` hand-rolls a 90-line coroutine stepper instead of `asyncio.eager_task_factory` (`PYB-11`), splits `AsgiAdapter` and `AsgiRunner` (`PYB-27`), keeps a dead Python `AsgiScope` fallback (`PYB-26`), imports `vertebra.extensions.litestar` at module top in `_asgi/_extensions.py` with an unwired `FastApiExtension` (`PYB-23`, `PYB-24`), and advertises `tls` on plaintext connections while dropping `trailers`/`early_hints`/`debug` (`PYB-25`).
  - `rsgi_protocol.rs` resolves `client_disconnect()` immediately (`PYB-37`), copies bodies twice in `RsgiAdapter.run_single` (`PYB-36`), ships a test-only `RsgiWebsocketProtocol` in production (`PYB-38`), allocates `_RsgiReadyAwaitable` on every send (`PYB-39`), and ignores mutex poisoning (`PYB-49`).
- **Telemetry and platform controls (`telemetry/*` 481 lines, `server/platform.py` 166 lines, `server/_shm_hardening.py` 67 lines, `vertebra-core/src/{logger,telemetry}.rs` 402 lines):**
  - Python `LogSink`, `TraceContext`, and `SqlTracePropagator.sanitize_sql` duplicate unbound Rust `AsyncLogCore`, `W3cTraceContext`, and `sanitize_sql_statement` (`PYB-55`, `PYB-56`, `CORE-13`), while `sanitize_sql_statement` in `vertebra-core/src/telemetry.rs:100-133` corrupts multi-byte UTF-8, rewrites `JOIN (SELECT ...)` as `JOIN (?)`, and omits `VALUES` (`CORE-34`).
  - `GenAiRecorder._samples` (`telemetry/otel.py:139-168`) grows without bound (`PYB-57`); `CgroupQuota`, `LameDuckController`, `ZPagesRegistry`, and `ShmAllocationPolicy` are unwired (`PYB-53`); `platform.py:111,148` computes uptime from `time.time()` instead of `time.monotonic()` (`PYB-54`); and `server/_ipc` and `server/_js` are empty placeholder packages (`PYB-59`).

#### Findings from `pybridge.md` not carried in this chapter

- **`PYB-06` (Zero-copy `PyBackedBytes` response bodies and pre-encoded headers):** Assigned to Chapter 8 (`hot-path-performance`) Task 8.5.
- **`PYB-07` (Per-tick response coalescing in async mode):** Assigned to Chapter 8 (`hot-path-performance`) Task 8.6.
- **`PYB-20` (Reader-callback SHM drain replacing `wait_for(ready_event.wait())`):** Assigned to Chapter 8 (`hot-path-performance`) Task 8.7.
- **`PYB-61` (Weak signature/callable/constant test assertions and stage-named test files):** Assigned to Chapter 9 (`test-suite-consolidation`) Tasks 9.1 and 9.2.

### Relevant Patterns

- [Product and Engineering Guidelines](../../product/product-guidelines.md) — Clean-break domain naming, `msgspec.Struct(frozen=True, gc=False)`, module/class layout, zero inline `#` comments, no nested imports, no re-export shims.
- [PyO3 FFI & GIL Discipline](../../knowledge/patterns/rust/ffi-and-gil-discipline.md) — `py.detach` on all blocking/transport/file operations, `MutexExt::lock_py_attached` to prevent ABBA deadlocks between the GIL and Rust mutexes.
- [Python Module Structure, Class Layout & PEP 695 Typing](../../knowledge/patterns/python/module-and-class-layout.md) and [Python Hot-Path Caching & Single-Wake Async Dispatch](../../knowledge/patterns/python/hot-path-and-async-dispatch.md).
- [PyO3 FFI, Embedded Globals & Python Runtime Gotchas](../../knowledge/gotchas/pyo3-and-python-runtime.md).

### Requirements

| ID | Requirement |
| --- | --- |
| C4-R1 | Build native extensions directly as `vertebra._vertebra`, `vertebra._bundler`, and `vertebra._storage` with synchronized `.pyi` stubs verified by `mypy.stubtest` in `make lint`/`lint-all`, and remove `src/py/vertebra/_bundler.py` and dead FFI code/dependencies. |
| C4-R2 | Consolidate `api.rs` into `transport.rs` with a single `ResponseSubmitter`, run all transport I/O inside `py.detach` with `RuntimeError` propagation on failure, and provide `Response.error(status, request_id)` in Rust. |
| C4-R3 | Replace `x-vertebra-sendfile` headers across RSGI, WSGI, and ASGI with explicit file body handling (`ResponseBody::File` / detached chunked file reads), eliminating empty RSGI file bodies and path-leaking headers. |
| C4-R4 | Deduplicate `from_parts` constructors onto `SyntheticRequest::build`, merge `batch.rs` into `request.rs`, unify `Headers` (with `get_all`) across ASGI/WSGI/RSGI (deleting `RsgiHeaders`), make `RsgiScope` lazy over `Arc<BatchSnapshot>`, and cache `Request.body` and `Request.headers` in `OnceLock`. |
| C4-R5 | Split `wsgi_environ.rs` and `wsgi_worker.rs` into `wsgi/{mod,environ,input,runner}.rs`, combine duplicate headers with `", "` per PEP 3333, stream `LargeBodyBypass` from the slab, look up `sys.stderr` dynamically, fix mutex locking under the GIL, and delete the duplicate Python `_wsgi/_environ.py` and `_wsgi/_runner.py`. |
| C4-R6 | Replace `AsyncRuntime` and `AsgiEventLoopPool` with a single `EventLoopPool` in `server/_loops.py`, size loop shards from `VERTEBRA_LOOP_SHARDS` (default 1 on GIL builds, `CgroupQuota.effective_workers` on 3.14t) instead of `VERTEBRA_WORKERS`, and make `GilAuditReport.quarantine_unsafe_modules` configure the loop policy. |
| C4-R7 | Split `server/_native.py` into `server/worker.py` (`Worker` class without module globals) and `server/_dispatch.py` (`HandlerSpec` table, module-level runners, `Response.error`, wired `error_handler`), and remove dead blocking dispatch, unused helpers, function-scoped imports, and re-export shims. |
| C4-R8 | Merge `AsgiAdapter` into `AsgiRunner` using `asyncio.eager_task_factory`, delete `_asgi/_scope.py`, move Litestar integrations out of `server/_asgi/_extensions.py` into `vertebra.extensions.litestar`, advertise `tls` only on TLS connections, and carry ASGI `trailers` via `FrameFlags::TRAILERS`. |
| C4-R9 | Split `rsgi_scope.rs` and `rsgi_protocol.rs` into `rsgi/{mod,scope,http}.rs`, wire `client_disconnect()` to a real cancellation future, stream `response_stream` chunks as `FrameFlags::STREAMING` frames, use a singleton `RsgiDone` awaitable, and move `RsgiWebsocketProtocol` to test fixtures. |
| C4-R10 | Bind Rust `NativeLogSink` (wrapping `AsyncLogCore` with stdout/stderr/memory/file targets, off-GIL thread, emit/flush/metrics), `parse_traceparent`, `format_traceparent`, fixed `sanitize_sql` (UTF-8 safe, word-boundary `IN`, `VALUES`), `NativeJwtGuard`, and `NativeRateLimiter` through `vertebra-py/src/telemetry.rs` and `_vertebra.pyi`, bound `GenAiRecorder`, wire `CgroupQuota`, `LameDuckController`, `ZPagesRegistry`, and `ShmAllocationPolicy`, use `time.monotonic()` for uptime, and delete `server/_ipc` and `server/_js`. |

### Target Layout

```text
src/rs/vertebra-py/
  Cargo.toml                 tokio, tokio-util, futures-util removed (PYB-41)
  src/
    lib.rs                   pymodule registration + __version__ (PYB-47, PYB-60)
    transport.rs             renamed from api.rs: init_transport, poll, single ResponseSubmitter, detached I/O (PYB-03, PYB-05, PYB-18)
    request.rs               Request (cached body/headers OnceLock) + RequestBatch + RequestBatchIterator (batch.rs merged) (PYB-28, PYB-45, PYB-46)
    synthetic.rs             SyntheticRequest::build shared by Request, AsgiScope, WsgiEnviron, RsgiScope from_parts (PYB-43)
    headers.rs               Headers (with get_all) + HeadersIterator over iter_raw_headers (PYB-34, PYB-44)
    response.rs              Response { status, headers, body, file_range }, Response.error(status, request_id) (PYB-01, PYB-04, PYB-14, PYB-46)
    asgi.rs                  renamed from asgi_scope.rs: 20-slot AsgiScope with pop() and accurate extensions (PYB-25, PYB-43, PYB-47)
    wsgi/
      mod.rs                 WSGI module exports
      environ.rs             WsgiEnviron with slab-indexed header cache, PEP 3333 duplicate combining, pop(), dynamic sys.stderr (PYB-29, PYB-30, PYB-47, PYB-50)
      input.rs               WsgiInput (zero-copy slab cursor) and WsgiFileWrapper (single detached stat) (PYB-32, PYB-33)
      runner.rs              renamed from wsgi_worker.rs: run_wsgi_callable with lock_py_attached (PYB-31, PYB-49)
    rsgi/
      mod.rs                 RSGI module exports
      scope.rs               lazy RsgiScope over Arc<BatchSnapshot> returning Headers (PYB-34, PYB-35)
      http.rs                RsgiHttpProtocol (real client_disconnect future, streaming frames, explicit file body, RsgiDone singleton, poison error) (PYB-01, PYB-37, PYB-38, PYB-39, PYB-40, PYB-49)
    telemetry.rs             PyO3 bindings for NativeLogSink (AsyncLogCore), parse/format_traceparent, sanitize_sql, NativeJwtGuard, NativeRateLimiter (PYB-55, PYB-56, CORE-13, CORE-34)
    file_io.rs               renamed from asgi_bridge.rs: single-copy detached stream_file_chunks (PYB-41, PYB-42)

src/py/vertebra/
  _vertebra.pyi              synchronized with vertebra._vertebra via mypy.stubtest (PYB-47)
  _bundler.pyi               type stub for native vertebra._bundler (PYB-58, FE-12)
  _storage.pyi               type stub for native vertebra._storage (FE-12)
  server/
    __init__.py              clean server exports
    worker.py                Worker class, run_worker, signal/lame-duck handlers, SHM bootstrap with ShmAllocationPolicy (PYB-19, PYB-22, PYB-53, PYB-62)
    _dispatch.py             HandlerSpec table, BatchDispatchPlan, module-level runners, Response.error + error_handler (PYB-12, PYB-13, PYB-14, PYB-15, PYB-16, PYB-17, PYB-48)
    _loops.py                renamed from _runtime_tuning.py: EventLoopPool (with primary_loop/next_loop, eager_task_factory), loop_shards (PYB-02, PYB-08, PYB-09, PYB-10, PYB-52)
    _protocol.py             detect_interface (single protocol detector) (PYB-21)
    _gil_audit.py            GilAuditReport with working quarantine policy and single before_extensions param (PYB-51, PYB-52)
    platform.py              CgroupQuota, LameDuckController, ZPagesRegistry with monotonic uptime (PYB-53, PYB-54)
    _asgi/
      __init__.py
      _runner.py             AsgiRunner (merged with AsgiAdapter) using eager_task_factory (PYB-11, PYB-26, PYB-27)
      _queues.py             AsgiReceive (cached body), AsgiSend (trailers, explicit file body, zero-copy view) (PYB-01, PYB-25, PYB-28)
      _wire.py               renamed from _extensions.py: ASGI wire response helpers only (Litestar moved to extensions/litestar) (PYB-22, PYB-23, PYB-24)
      _sse.py                ASGI SSE helper
      _websocket.py          ASGI WebSocket helper
    _wsgi/
      __init__.py
      _adapter.py            WsgiAdapter delegating to native run_wsgi_callable (PYB-01, PYB-31, PYB-32)
      _frameworks.py         renamed from _extensions.py: Django/Flask helpers + slab-streaming LargeBodyBypass (PYB-33)
    _rsgi/
      __init__.py
      _adapter.py            RsgiAdapter using request.rsgi_scope() and request.rsgi_protocol() (PYB-36)
  supervisor/
    __init__.py
    _supervisor.py           Rust server subprocess supervisor
    _worker.py               worker entrypoint passing loop_shards and pre-import extension snapshot (PYB-02, PYB-22, PYB-51, PYB-53)
  telemetry/
    __init__.py
    logger.py                LogSink and LogHandler backed by Rust AsyncLogCore (PYB-55, CORE-13)
    otel.py                  TraceContext backed by Rust W3cTraceContext, bounded GenAiRecorder (PYB-56, PYB-57, CORE-13)
    db_propagation.py        SqlTracePropagator backed by Rust sanitize_sql_statement (PYB-56, CORE-13, CORE-34)
```

Deleted files and directories: `src/py/vertebra/_bundler.py`, `src/rs/vertebra-py/src/batch.rs`, `src/py/vertebra/server/_native.py`, `src/py/vertebra/server/_runtime_tuning.py`, `src/py/vertebra/server/_shm_hardening.py`, `src/py/vertebra/server/_asgi/_adapter.py`, `src/py/vertebra/server/_asgi/_scope.py`, `src/py/vertebra/server/_asgi/_extensions.py`, `src/py/vertebra/server/_wsgi/_environ.py`, `src/py/vertebra/server/_wsgi/_runner.py`, `src/py/vertebra/server/_wsgi/_extensions.py`, `src/py/vertebra/server/_ipc/`, `src/py/vertebra/server/_js/`.

### Risks

| Risk | Mitigation |
| --- | --- |
| Changing maturin `module-name` for `_bundler` and `_storage` (`PYB-58`, `FE-12`) affects `Makefile` build targets and dynamic imports across `vertebra`. | Configure `module-name = "vertebra._bundler"` and `module-name = "vertebra._storage"` in each crate's `Cargo.toml`/`pyproject.toml`, update `Makefile` `dev`/`build`, add `.pyi` stubs, and verify with `mypy.stubtest` in Task 4.1 before touching other modules. |
| Holding a `std::sync::Mutex` across PyO3 C-API calls (`PYB-49`) can deadlock under free-threaded Python 3.14t when another thread triggers GC or acquires the GIL. | Extract all Python arguments into owned Rust values before locking, or use `pyo3::sync::MutexExt::lock_py_attached(py)`, and turn mutex poison into `PyRuntimeError`. |
| Replacing the custom ASGI coroutine stepper with `asyncio.eager_task_factory` (`PYB-11`) changes task scheduling on Python 3.12+. | Install `loop.set_task_factory(asyncio.eager_task_factory)` on every loop in `EventLoopPool` so synchronous-completing ASGI coroutines still finish inline without event-loop suspension while preserving standard `contextvars` and `anyio` task semantics; verify with `test_asgi.py` and `make bench TARGETS=vtb-shm-asgi-4w`. |

## Implementation Plan

### Phase 1: FFI Surface, Transport, File Bodies, Request Primitives & WSGI
- [x] 4.1 Accurate stubs, dead bridge code, native module names ([worksheet](tasks/4.1.md))
- [x] 4.2 One response submit path ([worksheet](tasks/4.2.md))
- [x] 4.3 Explicit file bodies ([worksheet](tasks/4.3.md))
- [x] 4.4 Shared request primitives ([worksheet](tasks/4.4.md))
- [x] 4.5 Single WSGI path ([worksheet](tasks/4.5.md))
- [x] Checkpoint: `make dev && make lint-all && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py && uv run pytest src/py/tests/unit/ffi src/py/tests/unit/server/test_wsgi.py src/py/tests/unit/server/test_wsgi_file_wrapper.py -q`

### Phase 2: Loop Pool, Worker Dispatch, ASGI, RSGI & Telemetry/Platform Wiring
- [x] 4.6 Event loop pool ([worksheet](tasks/4.6.md))
- [x] 4.7 Worker and dispatch split ([worksheet](tasks/4.7.md))
- [x] 4.8 ASGI runner debloat ([worksheet](tasks/4.8.md))
- [x] 4.9 RSGI protocol ([worksheet](tasks/4.9.md))
- [x] 4.10 Rust-backed telemetry and wired platform controls ([worksheet](tasks/4.10.md))
- [x] Chapter gate: `make lint-all && make rs-test && make py-test && make ts-test` (+ `make bench-check BASELINE=pre-refactor`), then area review per Global Constraint 12.

## Requirement-to-task trace

| Requirement | Tasks | Verification |
| --- | --- | --- |
| C4-R1 | 4.1 | `make dev && uv run python -m mypy.stubtest vertebra._vertebra vertebra._bundler vertebra._storage && make lint-all` |
| C4-R2 | 4.2 | `make dev && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py && uv run pytest src/py/tests/unit/ffi/test_ffi.py src/py/tests/unit/server/test_zero_copy_buffer.py -q` |
| C4-R3 | 4.3 | `make dev && uv run pytest src/py/tests/unit/server/test_rsgi_protocol.py src/py/tests/unit/server/test_wsgi_file_wrapper.py src/py/tests/unit/server/test_e2e.py -q` |
| C4-R4 | 4.4 | `make dev && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py && uv run pytest src/py/tests/unit/server/test_rsgi_protocol.py src/py/tests/unit/ffi/test_ffi.py -q && make bench-micro` |
| C4-R5 | 4.5 | `make dev && uv run pytest src/py/tests/unit/server/test_wsgi.py src/py/tests/unit/server/test_wsgi_file_wrapper.py -q` |
| C4-R6 | 4.6 | `uv run pytest src/py/tests/unit/server/test_runtime_tuning.py -q && make bench TARGETS=vtb-shm-asgi-4w` |
| C4-R7 | 4.7 | `uv run pytest src/py/tests/unit/server/test_runner.py -q && make bench-micro` |
| C4-R8 | 4.8 | `make dev && uv run pytest src/py/tests/unit/server/test_asgi.py src/py/tests/unit/server/test_asgi_scope_and_extensions.py src/py/tests/unit/server/test_realtime_backpressure.py -q` |
| C4-R9 | 4.9 | `make dev && uv run pytest src/py/tests/unit/server/test_rsgi_protocol.py -q` |
| C4-R10 | 4.10 | `make dev && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core && uv run pytest src/py/tests/unit/telemetry/test_telemetry_and_platform.py -q` |

## Finding-to-task trace

| Finding | Task |
| --- | --- |
| PYB-01 | 4.3 |
| PYB-02 | 4.6 |
| PYB-03 | 4.2 |
| PYB-04 | 4.3 |
| PYB-05 | 4.2 |
| PYB-08 | 4.6 |
| PYB-09 | 4.6 |
| PYB-10 | 4.6 |
| PYB-11 | 4.8 |
| PYB-12 | 4.7 |
| PYB-13 | 4.7 |
| PYB-14 | 4.7 |
| PYB-15 | 4.7 |
| PYB-16 | 4.7 |
| PYB-17 | 4.7 |
| PYB-18 | 4.1 |
| PYB-19 | 4.7 |
| PYB-21 | 4.7 |
| PYB-22 | 4.7 |
| PYB-23 | 4.8 |
| PYB-24 | 4.8 |
| PYB-25 | 4.8 |
| PYB-26 | 4.8 |
| PYB-27 | 4.8 |
| PYB-28 | 4.4 |
| PYB-29 | 4.5 |
| PYB-30 | 4.5 |
| PYB-31 | 4.5 |
| PYB-32 | 4.3 |
| PYB-33 | 4.5 |
| PYB-34 | 4.4 |
| PYB-35 | 4.4 |
| PYB-36 | 4.9 |
| PYB-37 | 4.9 |
| PYB-38 | 4.9 |
| PYB-39 | 4.9 |
| PYB-40 | 4.1 |
| PYB-41 | 4.1 |
| PYB-42 | 4.3 |
| PYB-43 | 4.4 |
| PYB-44 | 4.4 |
| PYB-45 | 4.4 |
| PYB-46 | 4.1 |
| PYB-47 | 4.1 |
| PYB-48 | 4.7 |
| PYB-49 | 4.5 |
| PYB-50 | 4.5 |
| PYB-51 | 4.6 |
| PYB-52 | 4.6 |
| PYB-53 | 4.10 |
| PYB-54 | 4.10 |
| PYB-55 | 4.10 |
| PYB-56 | 4.10 |
| PYB-57 | 4.10 |
| PYB-58 | 4.1 |
| PYB-59 | 4.10 |
| PYB-60 | 4.1 |
| PYB-62 | 4.7 |
| CORE-13 | 4.10 |
| CORE-34 | 4.10 |
| FE-12 | 4.1 |

## Continuity Snapshot

- Active flow: `bridge-and-server-consolidation`, state `closed`. Current task: none. Next step: `/flow:archive bridge-and-server-consolidation`.
- Plan identity: plan_revision 2, plan_commit "". State identity: state_revision 1.
- Blockers: none. Decisions: inherited D1–D4 from the roadmap (D4 governs binding Rust `NativeLogSink` (`AsyncLogCore`), `parse/format_traceparent`, `sanitize_sql`, `NativeJwtGuard`, and `NativeRateLimiter` in Task 4.10; stub policy from Global Constraint 6 governs `LargeBodyBypass` in Task 4.5, ASGI extensions in Task 4.8, and `RsgiWebsocketProtocol` in Task 4.9).
- Knowledge paths: `.agents/bundles/product/product-guidelines.md`, `.agents/bundles/knowledge/workflow.md`, `.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md`, `.agents/bundles/knowledge/architecture/worker/asgi-and-wsgi-runners.md`, `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`, `.agents/bundles/knowledge/patterns/python/module-and-class-layout.md`, `.agents/bundles/knowledge/patterns/python/hot-path-and-async-dispatch.md`, `.agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md`.
