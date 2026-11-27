---
type: Task
id: "runtime-perf-remediation:ch01:t04"
chapter_id: ch01-wsgi-adaptive-pool
title: Wire Python stubs and Worker WSGI shutdown
description: Declare shutdown_wsgi_executor in _vertebra.pyi, invoke it in Worker.run finally block, and update WsgiAdapter batch docstring.
status: stable
state: completed
priority: P1
verification_strategy: regression_tdd
worksheet_format: compact
depends_on:
  - "runtime-perf-remediation:ch01:t03"
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/chapters/ch01-wsgi-adaptive-pool/spec.md
  - src/py/vertebra/_vertebra.pyi
  - src/py/vertebra/server/_wsgi/_adapter.py
  - src/py/vertebra/server/worker.py
files:
  - src/py/vertebra/_vertebra.pyi
  - src/py/vertebra/server/_wsgi/_adapter.py
  - src/py/vertebra/server/worker.py
tests:
  - src/py/tests/unit/server/test_wsgi_adaptive_pool.py
estimated_loc: 35
plan_revision: 2
plan_commit: "86527cf1"
commit: "b110db7d"
sources:
  - id: ch01-spec
    resource: .agents/bundles/specs/runtime-perf-remediation/chapters/ch01-wsgi-adaptive-pool/spec.md
    title: Chapter 01 adaptive WSGI pool specification
  - id: src-py-stub
    resource: src/py/vertebra/_vertebra.pyi
    title: Type stubs for _vertebra
  - id: src-wsgi-adapter
    resource: src/py/vertebra/server/_wsgi/_adapter.py::WsgiAdapter
    title: Python WSGI adapter
  - id: src-server-worker
    resource: src/py/vertebra/server/worker.py::Worker::run
    title: Worker lifecycle and shutdown cleanup
tags:
  - wsgi
  - python
  - typing
  - worker
---

# Wire Python stubs and Worker WSGI shutdown

## Objective & Scope Boundary

Add `shutdown_wsgi_executor` to `src/py/vertebra/_vertebra.pyi`, call it from `Worker.run`'s `finally:` block in `src/py/vertebra/server/worker.py`, and update `WsgiAdapter.spawn_batch`'s docstring in `src/py/vertebra/server/_wsgi/_adapter.py` to match adaptive inline/concurrent execution [^ch01-spec] [^src-py-stub] [^src-wsgi-adapter] [^src-server-worker]. Follow project Python rules: zero inline `#` comments, PEP 585/604 types, and Google-style docstrings.

## Bounded Read Manifest

- `.agents/bundles/specs/runtime-perf-remediation/chapters/ch01-wsgi-adaptive-pool/spec.md` (Section 2.2)
- `src/py/vertebra/_vertebra.pyi` (around `__all__` and `drive_wsgi_batch`)
- `src/py/vertebra/server/_wsgi/_adapter.py`
- `src/py/vertebra/server/worker.py` (imports and `Worker.run` `finally:` block)

## Contracts & Edge Cases

- `_vertebra.pyi`: add `"shutdown_wsgi_executor"` to `__all__` in sorted order and declare `def shutdown_wsgi_executor() -> None: ...` next to `drive_wsgi_batch` so `mypy.stubtest vertebra._vertebra` passes with zero mismatches.
- `worker.py`: import `shutdown_wsgi_executor` from `vertebra._vertebra` and call `shutdown_wsgi_executor()` in `Worker.run`'s `finally:` block alongside `shutdown_worker_executor(wait=True)` before `loop_pool.stop()`, wrapped so an unexpected shutdown error is logged without preventing `loop_pool.stop()`.
- `_adapter.py`: keep `WsgiAdapter.spawn_batch` delegating `RequestBatch` and `Sequence[Request]` directly to `drive_wsgi_batch` and update its docstring to state adaptive inline/concurrent Rust execution.

## Test Matrix

| Check | Assertion |
| :--- | :--- |
| Stub fidelity | `uv run --no-sync python -m mypy.stubtest vertebra._vertebra` exits 0 with zero missing or mismatched symbols. |
| Static typing & lint | `ruff check`, `mypy`, and `pyright` pass on all three touched Python files. |
| Worker shutdown hook | Verified by `t05` test confirming `Worker.run` invokes `shutdown_wsgi_executor` in `finally:`. |

## Verification

```bash
make dev-ext EXT=core
uv run --no-sync python -m mypy.stubtest vertebra._vertebra
uv run --no-sync ruff check src/py/vertebra/_vertebra.pyi src/py/vertebra/server/_wsgi/_adapter.py src/py/vertebra/server/worker.py
uv run --no-sync mypy src/py/vertebra/server/_wsgi/_adapter.py src/py/vertebra/server/worker.py
```

## Acceptance Criteria

- `_vertebra.pyi` passes `mypy.stubtest` against the rebuilt `_vertebra` extension.
- `Worker.run` always drains the native WSGI pool via `shutdown_wsgi_executor()` on exit.
- Touched files pass `ruff`, `mypy`, and contain zero inline `#` comments.

[^ch01-spec]: `.agents/bundles/specs/runtime-perf-remediation/chapters/ch01-wsgi-adaptive-pool/spec.md` — Chapter 01 adaptive WSGI pool specification.
[^src-py-stub]: `src/py/vertebra/_vertebra.pyi` — Type stubs for _vertebra.
[^src-wsgi-adapter]: `src/py/vertebra/server/_wsgi/_adapter.py::WsgiAdapter` — Python WSGI adapter.
[^src-server-worker]: `src/py/vertebra/server/worker.py::Worker::run` — Worker lifecycle and shutdown cleanup.
