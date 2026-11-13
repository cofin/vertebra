---
type: Task
id: "server-runtime-correctness:ch05:t02"
flow_id: server-runtime-correctness
chapter_id: ch05-fastpath-consolidation
title: "Consolidate Litestar, Starlette, and FastAPI Plugins onto set_attached_fastpath"
description: Replace vertebra_fastpath_bypass state writes and Litestar class-property mutation with set_attached_fastpath / get_attached_fastpath.
status: stable
state: completed
commit: 780338a4
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "server-runtime-correctness:ch05:t01"
files:
  - src/py/vertebra/extensions/litestar/plugin.py
  - src/py/vertebra/extensions/starlette/plugin.py
  - src/py/vertebra/extensions/fastapi/plugin.py
tests:
  - src/py/tests/unit/extensions/test_litestar_plugin.py
  - src/py/tests/unit/extensions/test_starlette_plugin.py
  - src/py/tests/unit/extensions/test_fastapi_plugin.py
read_set:
  - src/py/vertebra/server/_fastpath_core.py
  - src/py/vertebra/extensions/litestar/plugin.py
  - src/py/vertebra/extensions/starlette/plugin.py
  - src/py/vertebra/extensions/fastapi/plugin.py
estimated_loc: 95
tags:
  - fastpath
  - litestar
  - starlette
  - fastapi
sources:
  - id: litestar-plugin-py
    resource: "src/py/vertebra/extensions/litestar/plugin.py::VertebraPlugin"
    title: Litestar plugin mutating type(app).__vertebra_fastpath__
  - id: starlette-plugin-py
    resource: "src/py/vertebra/extensions/starlette/plugin.py::VertebraPlugin"
    title: Starlette plugin writing state vertebra_fastpath_bypass
---

# Task `t02`: Consolidate Litestar, Starlette, and FastAPI Plugins onto set_attached_fastpath

## Objective & Scope Boundary

Switch Litestar, Starlette, and FastAPI `VertebraPlugin` implementations to `get_attached_fastpath` / `set_attached_fastpath` per [`../spec.md` §2](../spec.md) [^litestar-plugin-py] [^starlette-plugin-py].

## Bounded Read Manifest

- `src/py/vertebra/extensions/{litestar,starlette,fastapi}/plugin.py` — fastpath attachment hooks.
- `src/py/vertebra/server/_fastpath_core.py` — `FASTPATH_ATTR`, `get_attached_fastpath`, `set_attached_fastpath`.

## Contracts & Edge Cases

- Remove all `'vertebra_fastpath_bypass'` state reads/writes across all three plugins.
- Remove `type(app).__vertebra_fastpath__ = property(...)` from `litestar/plugin.py` so `Litestar` class attributes are never mutated across instances; store on `app.state['__vertebra_fastpath__']` via `set_attached_fastpath`.

## Test Matrix

- Assertions in `test_litestar_plugin.py`, `test_starlette_plugin.py`, and `test_fastapi_plugin.py` verifying `get_attached_fastpath(app)` returns the compiled `FastPath`, `'vertebra_fastpath_bypass'` is absent from `app.state`, and a second plain `Litestar()` instance has no `__vertebra_fastpath__` attribute.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/{litestar,starlette,fastapi}/plugin.py && uv run --no-sync mypy src/py/vertebra/extensions/{litestar,starlette,fastapi}/plugin.py && uv run --no-sync pytest src/py/tests/unit/extensions/test_litestar_plugin.py src/py/tests/unit/extensions/test_starlette_plugin.py src/py/tests/unit/extensions/test_fastapi_plugin.py -q
```

## Acceptance Criteria

- [ ] Zero `'vertebra_fastpath_bypass'` references or `type(app)` mutations remain in ASGI plugins.
- [ ] Litestar, Starlette, and FastAPI plugin suites pass.

[^litestar-plugin-py]: `src/py/vertebra/extensions/litestar/plugin.py::VertebraPlugin` — Litestar plugin mutating type(app).__vertebra_fastpath__.
[^starlette-plugin-py]: `src/py/vertebra/extensions/starlette/plugin.py::VertebraPlugin` — Starlette plugin writing state vertebra_fastpath_bypass.
