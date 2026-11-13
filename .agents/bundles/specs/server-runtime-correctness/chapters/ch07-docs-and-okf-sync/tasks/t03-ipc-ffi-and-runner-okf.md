---
type: Task
id: "server-runtime-correctness:ch07:t03"
flow_id: server-runtime-correctness
chapter_id: ch07-docs-and-okf-sync
title: "Sync IPC, Runner, and PyO3 FFI OKF Chapters (<= 200 Lines)"
description: "Update tiered-rings-and-batch-bridge.md and asgi-and-wsgi-runners.md, and compact pyo3-ffi-and-batch-dispatch.md from 212 to <= 200 lines."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on: []
files:
  - .agents/bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md
  - .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md
  - .agents/bundles/knowledge/architecture/worker/asgi-and-wsgi-runners.md
tests: []
read_set:
  - .agents/bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md
  - .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md
  - .agents/bundles/knowledge/architecture/worker/asgi-and-wsgi-runners.md
  - src/rs/vertebra-py/src/transport.rs
  - src/py/vertebra/server/_dispatch.py
  - src/py/vertebra/server/_fastpath_core.py
estimated_loc: 145
tags:
  - okf
  - ipc
  - pyo3
  - asgi
sources:
  - id: bridge-okf
    resource: .agents/bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md
    title: Tiered rings and batch bridge OKF chapter
  - id: pyo3-ffi-okf
    resource: .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md
    title: PyO3 FFI and batch dispatch OKF chapter
  - id: runners-okf
    resource: .agents/bundles/knowledge/architecture/worker/asgi-and-wsgi-runners.md
    title: "ASGI, RSGI, and WSGI runners OKF chapter"
---

# Task `t03`: Sync IPC, Runner, and PyO3 FFI OKF Chapters (<= 200 Lines)

## Objective & Scope Boundary

Synchronize the three IPC/FFI/runner architecture OKF chapters and compact `pyo3-ffi-and-batch-dispatch.md` (`212L -> <= 200L`) per [`../spec.md` §2](../spec.md) [^bridge-okf] [^pyo3-ffi-okf] [^runners-okf].

## Bounded Read Manifest

- `.agents/bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md`.
- `.agents/bundles/knowledge/architecture/worker/{pyo3-ffi-and-batch-dispatch,asgi-and-wsgi-runners}.md`.
- `src/rs/vertebra-py/src/transport.rs`, `src/py/vertebra/server/{_dispatch,_fastpath_core}.py`.

## Contracts & Edge Cases

- Document `submit_stream_frame`, `max_stream_frame_payload`, native `_dispatch.py` `(status, headers, AsyncIterable | Iterable)` streaming, direct `_zero_copy` bindings, `CompiledRouteBase`, and single `__vertebra_fastpath__` resolution.
- Keep all three OKF chapters `<= 200` total lines and `<= 40` truth-block lines with valid `verified.commit` and matching `sources:` footnotes.

## Test Matrix

- Verified via `make review-check` and line-count check (`<= 200` lines per chapter).

## Verification

```bash
python3 -c 'import pathlib; [assert_ok := (len(pathlib.Path(p).read_text().splitlines()) <= 200) or (_ for _ in ()).throw(AssertionError(p)) for p in [".agents/bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md", ".agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md", ".agents/bundles/knowledge/architecture/worker/asgi-and-wsgi-runners.md"]]' && make review-check
```

## Acceptance Criteria

- [ ] `pyo3-ffi-and-batch-dispatch.md` and sibling architecture chapters are each `<= 200` lines.
- [ ] `make review-check` exits `0`.

[^bridge-okf]: `.agents/bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md` — Tiered rings and batch bridge OKF chapter.
[^pyo3-ffi-okf]: `.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` — PyO3 FFI and batch dispatch OKF chapter.
[^runners-okf]: `.agents/bundles/knowledge/architecture/worker/asgi-and-wsgi-runners.md` — ASGI, RSGI, and WSGI runners OKF chapter.
