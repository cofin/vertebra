---
type: Task
id: "runtime-perf-remediation:ch01:t05"
chapter_id: ch01-wsgi-adaptive-pool
title: Add WSGI adaptive pool concurrency and regression tests
description: Verify blocking WSGI concurrency, inline zero-I/O fastpath, hysteresis, 500 error isolation, and GIL-safe shutdown in pytest.
status: stable
state: completed
priority: P0
verification_strategy: regression_tdd
worksheet_format: compact
depends_on:
  - "runtime-perf-remediation:ch01:t03"
  - "runtime-perf-remediation:ch01:t04"
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/chapters/ch01-wsgi-adaptive-pool/spec.md
  - src/rs/vertebra-py/src/transport.rs
  - src/py/vertebra/server/_wsgi/_adapter.py
  - src/py/vertebra/server/worker.py
files:
  - src/py/tests/unit/server/test_wsgi_adaptive_pool.py
tests:
  - src/py/tests/unit/server/test_wsgi_adaptive_pool.py
estimated_loc: 220
plan_revision: 2
plan_commit: "86527cf1"
commit: "b110db7d"
sources:
  - id: ch01-spec
    resource: .agents/bundles/specs/runtime-perf-remediation/chapters/ch01-wsgi-adaptive-pool/spec.md
    title: Chapter 01 adaptive WSGI pool specification
  - id: src-wsgi-adapter
    resource: src/py/vertebra/server/_wsgi/_adapter.py::WsgiAdapter
    title: Python WSGI adapter
  - id: src-server-worker
    resource: src/py/vertebra/server/worker.py::Worker::run
    title: Worker lifecycle and shutdown cleanup
tags:
  - wsgi
  - testing
  - concurrency
  - regression
---

# Add WSGI adaptive pool concurrency and regression tests

## Objective & Scope Boundary

Add `src/py/tests/unit/server/test_wsgi_adaptive_pool.py` exercising the live `_vertebra` extension (`drive_wsgi_batch`, `shutdown_wsgi_executor`), `WsgiAdapter.spawn_batch`, and `Worker.run` shutdown cleanup [^ch01-spec] [^src-wsgi-adapter] [^src-server-worker]. Inspect `src/py/tests/unit/server/test_wsgi.py` first to reuse the repo's established transport response-capture pattern. Do not edit production Rust or Python code in this task.

## Bounded Read Manifest

- `.agents/bundles/specs/runtime-perf-remediation/chapters/ch01-wsgi-adaptive-pool/spec.md` (Sections 2.1, 2.4)
- `src/rs/vertebra-py/src/transport.rs` (for `init_transport` in-process response inspection semantics)
- `src/py/vertebra/server/_wsgi/_adapter.py`
- `src/py/vertebra/server/worker.py`
- `src/py/tests/unit/server/test_wsgi.py` (reuse response verification helpers)

## Contracts & Edge Cases

- Reset pool state in an `autouse` fixture calling `shutdown_wsgi_executor()` before and after each test so `monkeypatch.setenv("VERTEBRA_WSGI_MODE", ...)` and `VERTEBRA_WSGI_STALL_US` / `VERTEBRA_WSGI_THREADS` take effect deterministically.
- Avoid brittle timing-only checks: combine a real `threading.Barrier` proof of simultaneous execution with the wall-clock ceiling (`16 x 30 ms < 150 ms`).
- On `FREE_THREADED` (`_vertebra.FREE_THREADED == True`), assert default mode is `concurrent`; on GIL builds, assert default `auto` runs zero-I/O batches inline on the caller thread.
- Follow project Python style: zero inline `#` comments, PEP 585/604 types, and clear failure messages.

## Test Matrix

| Test Case | Observable Assertion |
| :--- | :--- |
| Blocking I/O in `auto` mode (`16 x 30 ms`) | `drive_wsgi_batch` / `WsgiAdapter.spawn_batch` completes all 16 requests in `< 150 ms` (vs `480 ms` serial), with all 16 `200` responses submitted and distinct request IDs. |
| Concurrency barrier proof (`auto` & `concurrent`) | `threading.Barrier(4, timeout=1.0)` inside WSGI handler releases without `BrokenBarrierError`, proving >= 4 requests execute concurrently. |
| Zero-I/O `auto` inline fastpath (GIL builds) | Fast `return [b"ok"]` batch executes all items on `threading.get_ident() == caller_tid` and preserves `SCRIPT_NAME`, `PATH_INFO`, `SERVER_NAME`, `SERVER_PORT`, and `wsgi.url_scheme`. |
| `inline` mode override | `VERTEBRA_WSGI_MODE=inline` runs every request on the caller thread synchronously before `drive_wsgi_batch` returns. |
| 2 s concurrent hysteresis | Batch submitted immediately after a stalled batch runs on non-caller pool worker threads. |
| Exception & PEP 3333 violation isolation | Mixed batch (raising `RuntimeError`, missing `start_response`, and healthy `200` handlers) submits `500` for failures and `200` for healthy items; a follow-up batch still succeeds. |
| GIL-safe `shutdown_wsgi_executor()` + re-init | Calling `shutdown_wsgi_executor()` while blocking handlers are in flight drains all responses without deadlocking; a subsequent batch initializes a new pool and succeeds. |
| `Worker.run` shutdown integration | `Worker.run()` calls `shutdown_wsgi_executor()` in its `finally:` block on both clean stop and early startup failure. |

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py wsgi
make dev-ext EXT=core
uv run --no-sync pytest src/py/tests/unit/server/test_wsgi_adaptive_pool.py -v
uv run --no-sync ruff check src/py/tests/unit/server/test_wsgi_adaptive_pool.py
```

## Acceptance Criteria

- All tests in `src/py/tests/unit/server/test_wsgi_adaptive_pool.py` pass deterministically without flakes.
- Blocking concurrency test proves `< 150 ms` wall clock for `16 x 30 ms` sleeps and passes the `threading.Barrier` check.
- Fast Chapter Gate (`cargo test`, rebuild, `pytest`, `ruff`, `stubtest`) completes in `< 60s`.

[^ch01-spec]: `.agents/bundles/specs/runtime-perf-remediation/chapters/ch01-wsgi-adaptive-pool/spec.md` — Chapter 01 adaptive WSGI pool specification.
[^src-wsgi-adapter]: `src/py/vertebra/server/_wsgi/_adapter.py::WsgiAdapter` — Python WSGI adapter.
[^src-server-worker]: `src/py/vertebra/server/worker.py::Worker::run` — Worker lifecycle and shutdown cleanup.
