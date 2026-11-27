---
type: Task
id: "runtime-perf-remediation:ch03:t07"
chapter_id: ch03-ingress-and-worker-hotpaths
title: Eventfd-split drain, native_only mixed dispatch, and 1-arg extractors
description: Split _on_notify_readable from _drain_batches with a 1-turn grace yield, add single-pass native_only mixed dispatch, and specialize 1-arg simple extractors.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
worksheet_format: compact
depends_on: []
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/chapters/ch03-ingress-and-worker-hotpaths/spec.md
  - src/py/vertebra/server/worker.py
  - src/py/vertebra/server/_dispatch.py
files:
  - src/py/vertebra/server/worker.py
  - src/py/vertebra/server/_dispatch.py
tests:
  - src/py/tests/unit/server/test_hotpath_dispatch_and_freethreaded.py
  - src/py/tests/unit/server/test_runtime_tuning.py
estimated_loc: 170
plan_revision: 2
plan_commit: "86527cf1"
commit: "6e72abac"
sources:
  - id: src-server-worker
    resource: src/py/vertebra/server/worker.py::Worker
    title: Worker inline event loop and batch processor
  - id: src-server-dispatch
    resource: src/py/vertebra/server/_dispatch.py::compile_handler_spec
    title: HandlerSpec compilation and simple extractors
tags:
  - python-worker
  - event-loop
  - dispatch
  - performance
---

# Eventfd-Split Drain, `native_only` Mixed Dispatch, and 1-Arg Extractors

## Objective & Scope Boundary

Eliminate redundant `clear_request_notification()` `eventfd` syscalls on `call_soon`, enable single-pass inline dispatch on mixed sync+async native apps, and specialize 1-arg simple extractors without generator expressions or empty dict allocations [^src-server-worker] [^src-server-dispatch].

Out of scope: Rust transport/bridge code (`t01`–`t06`) and benchmark scripts (`ch04`).

## Bounded Read Manifest

- Chapter spec: `.agents/bundles/specs/runtime-perf-remediation/chapters/ch03-ingress-and-worker-hotpaths/spec.md`
- Source files: `src/py/vertebra/server/worker.py`, `src/py/vertebra/server/_dispatch.py`

## Contracts & Edge Cases

- **Eventfd-split inline drain (`src/py/vertebra/server/worker.py::Worker._run_inline_event_loop`)**:
  - Register `loop.add_reader(notify_fd, _on_notify_readable)`. `_on_notify_readable()` cancels any pending scheduled drain handle, calls `clear_request_notification()`, and delegates directly to `_drain_batches(from_grace=False)`.
  - `_drain_batches(*, from_grace: bool = False)` calls `cancel_request_sleep()` (never `clear_request_notification()`) and drains up to `_INLINE_DRAIN_BUDGET` batches.
  - When `try_get_batch()` returns empty:
    - If `drained > 0` and not `from_grace` and not `self._should_stop(batches_processed)`: schedule one grace check via `loop.call_soon(_drain_grace)` without calling `prepare_request_sleep()`.
    - Otherwise (`drained == 0` or `from_grace`): arm sleep via `prepare_request_sleep()`; if `prepare_request_sleep()` returns `False`, call `cancel_request_sleep()` and reschedule `_drain_batches`, else stop the loop if `self._should_stop(batches_processed)`.
  - When the drain budget is reached (`drained == _INLINE_DRAIN_BUDGET`), schedule `_drain_batches` (not `_on_notify_readable`) via `loop.call_soon`. Update `_watchdog` and initial startup `call_soon` to call `_drain_batches`.
- **Single-pass `native_only` dispatch (`src/py/vertebra/server/worker.py::Worker._process_batch`)**:
  - In `Worker.run`, compute `native_only = not has_mounts and not has_sync_thread` and pass it to `_run_inline_event_loop` and `_process_batch`. Keep `all_sync_native: bool` in both signatures and default `native_only: bool = False` for backward compatibility with direct unit tests.
  - In `_process_batch`, when `all_sync_native or native_only`: iterate `batch` once. Handle `_ZPAGES_HANDLER_ID` via `_handle_zpage_request`, unknown handler IDs as `404 b"not found"`, sync specs inline (collecting `raw_items` for `submit_batch_raw` and handling `Response` / `_is_potential_special_return` / `encode_json_bytes` / exceptions identically to `all_sync_native`), and async specs (`spec.is_async`) by invoking `spec.invoker(req)` and lazily accumulating into `drive_native_batch` tuples (when `self._error_handler is None and loop_pool.size == 1`) or `_await_async_item` coroutines scheduled via `_schedule_coros` (otherwise). Flush `raw_items` before returning.
- **Specialized 1-arg simple extractors (`src/py/vertebra/server/_dispatch.py::compile_handler_spec`)**:
  - Inside `if all_simple_extractors and not needs_path_params and not needs_query_map and not has_di_params:`:
    - When `len(params) == 1`: specialize `"body"`, `"headers"`, `"method"`, `"path"`, and `"query"` with dedicated 1-arg invoker and `arg_resolver` closures reading directly from `request` (zero generator expression, zero empty `{}` dict, preserving exact `_extract_body` coercion for `None`/`bytes`/`bytearray`/`memoryview` and default values on mock requests).
    - When `len(params) > 1`: build 1-arg `raw_extractors = tuple(_PARAM_EXTRACTORS.get(p.name, _extract_request) for p in param_plans)` so multi-arg simple handlers also skip empty `{}` dict allocations.
  - Follow project Python style: PEP 585/604 types and zero inline `#` comments.

## Test Matrix

| Case | Verification |
| :--- | :--- |
| `_on_notify_readable` vs `_drain_batches` syscall separation | Unit test with mocked `clear_request_notification`, `prepare_request_sleep`, and `try_get_batch` asserts `clear_request_notification` runs only on reader callbacks, never on `call_soon` continuations or grace turns, and `prepare_request_sleep` runs after the 1-turn grace miss. |
| Mixed sync+async `native_only` batch | App with sync `/hello`, sync `/json`, and `async def /io` executes sync items inline via `submit_batch_raw` and async items via `drive_native_batch` without calling `partition_batch`; custom `error_handler`, 404s, special returns, and z-pages behave identically. |
| Specialized 1-arg simple extractors | `compile_handler_spec` for `body`, `headers`, `method`, `path`, `query` (sync and async, native `Request` and mock objects, `None`/`bytes`/`bytearray` body) matches exact return values and `arg_resolver` tuples. |
| Existing hotpath and runtime tuning tests | `test_hotpath_dispatch_and_freethreaded.py` and `test_runtime_tuning.py` pass cleanly. |

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/server/test_hotpath_dispatch_and_freethreaded.py src/py/tests/unit/server/test_runtime_tuning.py
uv run --no-sync ruff check src/py/vertebra/server/worker.py src/py/vertebra/server/_dispatch.py
```

## Acceptance Criteria

- `clear_request_notification()` is invoked only inside `_on_notify_readable`, never during `call_soon` drain continuations or grace turns.
- Mixed sync+async native apps without mounts or `sync_to_thread` handlers bypass `partition_batch` and `dispatch_batch` in `_process_batch`.
- 1-arg simple extractors (`body`, `headers`, `method`, `path`, `query`) allocate zero generator expressions and zero empty `{}` dicts per call.
- Ruff lint and targeted Python unit tests pass with zero inline `#` comments.

[^src-server-worker]: `src/py/vertebra/server/worker.py::Worker` — Worker inline event loop and batch processor.
[^src-server-dispatch]: `src/py/vertebra/server/_dispatch.py::compile_handler_spec` — HandlerSpec compilation and simple extractors.
