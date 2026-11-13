---
type: Task
id: "server-runtime-correctness:ch05:t01"
flow_id: server-runtime-correctness
chapter_id: ch05-fastpath-consolidation
title: Add CompiledRouteBase and Single __vertebra_fastpath__ Helpers in _fastpath_core
description: "Define FASTPATH_ATTR, get_attached_fastpath, set_attached_fastpath, has_custom_exception_handlers, and CompiledRouteBase, removing alias probes in _fastpath.py and _runner.py."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/py/vertebra/server/_fastpath_core.py
  - src/py/vertebra/server/_fastpath.py
  - src/py/vertebra/server/_asgi/_runner.py
tests:
  - src/py/tests/unit/server/test_asgi.py
read_set:
  - src/py/vertebra/server/_fastpath_core.py
  - src/py/vertebra/server/_fastpath.py
  - src/py/vertebra/server/_asgi/_runner.py
  - src/py/tests/unit/server/test_asgi.py
estimated_loc: 125
tags:
  - fastpath
  - server
  - asgi
  - cleanup
sources:
  - id: fastpath-core-py
    resource: src/py/vertebra/server/_fastpath_core.py
    title: Shared FastPath core primitives
  - id: fastpath-attach-py
    resource: "src/py/vertebra/server/_fastpath.py::attach_framework_fastpath"
    title: Multi-alias FastPath resolver
---

# Task `t01`: Add CompiledRouteBase and Single __vertebra_fastpath__ Helpers in _fastpath_core

## Objective & Scope Boundary

Introduce `CompiledRouteBase`, `FASTPATH_ATTR`, `get_attached_fastpath`, `set_attached_fastpath`, and `has_custom_exception_handlers` in `_fastpath_core.py` and drop alias probes in `_fastpath.py` and `_asgi/_runner.py` per [`../spec.md` §2](../spec.md) [^fastpath-core-py] [^fastpath-attach-py].

## Bounded Read Manifest

- `src/py/vertebra/server/_fastpath_core.py` & `_fastpath.py` — provider registry and attachment.
- `src/py/vertebra/server/_asgi/_runner.py` — `AsgiRunner` fastpath lookup.

## Contracts & Edge Cases

- `get_attached_fastpath(app)` checks `getattr(app, '__vertebra_fastpath__', None)` then `app.state['__vertebra_fastpath__']` (mapping or attribute) and no other alias.
- `set_attached_fastpath(app, fastpath)` writes `app.__vertebra_fastpath__` or falls back on `AttributeError` (slotted apps) to `app.state['__vertebra_fastpath__']`, raising `TypeError` when neither exists.
- `CompiledRouteBase(msgspec.Struct, frozen=True, gc=False, kw_only=True)` defines shared `handler_id: int`, `is_async: bool = False`, `status_code: int = 200`, and `param_converters: tuple[tuple[str, str], ...] = ()`.

## Test Matrix

- Parameterized tests in `test_asgi.py` verifying normal object attribute storage, slotted object `state['__vertebra_fastpath__']` fallback, `TypeError` on slotted stateless objects, and rejection of legacy `fastpath` / `vertebra_fastpath_bypass` keys.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/server && uv run --no-sync mypy src/py/vertebra/server && uv run --no-sync pytest src/py/tests/unit/server/test_asgi.py -q
```

## Acceptance Criteria

- [ ] Only `__vertebra_fastpath__` is read or written by `_fastpath.py` and `_asgi/_runner.py`.
- [ ] `CompiledRouteBase` and `has_custom_exception_handlers` are exported from `_fastpath_core.py`.

[^fastpath-core-py]: `src/py/vertebra/server/_fastpath_core.py` — Shared FastPath core primitives.
[^fastpath-attach-py]: `src/py/vertebra/server/_fastpath.py::attach_framework_fastpath` — Multi-alias FastPath resolver.
