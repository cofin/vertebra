---
type: Spec
flow_id: server-runtime-correctness
chapter_id: ch05-fastpath-consolidation
title: "Chapter 05: FastPath Route Struct and Attribute Consolidation"
description: "Unify the five framework CompiledRoute structs onto CompiledRouteBase, resolve FastPath solely through __vertebra_fastpath__, deduplicate exception-handler checks, and serialize Starlette/FastAPI responses copy-free by exact type."
status: stable
state: active
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters: []
replan_trigger: null
plan_commit: 18baadcd
last_reviewed_commit: null
updated_at: "2026-11-13T05:37:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - fastpath
  - litestar
  - starlette
  - fastapi
  - wsgi
sources:
  - id: fastpath-core-py
    resource: src/py/vertebra/server/_fastpath_core.py
    title: Shared FastPath core primitives
  - id: fastpath-attach-py
    resource: "src/py/vertebra/server/_fastpath.py::attach_framework_fastpath"
    title: FastPath attachment probing four alias locations
  - id: litestar-fp-py
    resource: "src/py/vertebra/extensions/litestar/fastpath.py::CompiledRoute"
    title: Litestar CompiledRoute and _has_custom_exception_handlers
  - id: starlette-fp-py
    resource: "src/py/vertebra/extensions/starlette/fastpath.py::CompiledRoute"
    title: Starlette CompiledRoute and response body copy
  - id: flask-fp-py
    resource: "src/py/vertebra/extensions/flask/fastpath.py::CompiledRoute"
    title: Flask WSGI CompiledRoute
---

# Chapter 05: FastPath Route Struct and Attribute Consolidation

<!-- truth: start -->
- `_fastpath_core.py` defines `FASTPATH_ATTR = '__vertebra_fastpath__'`, `get_attached_fastpath`, `set_attached_fastpath`, `has_custom_exception_handlers`, and `CompiledRouteBase(msgspec.Struct, frozen=True, gc=False, kw_only=True)` [^fastpath-core-py].
- `attach_framework_fastpath`, `AsgiRunner`, and all host `VertebraPlugin` implementations read and write only `__vertebra_fastpath__` (attribute, or `app.state['__vertebra_fastpath__']` for slotted `Litestar` instances without mutating `type(app)`) [^fastpath-attach-py] [^litestar-fp-py].
- Litestar, Starlette, FastAPI, Flask, and Django `CompiledRoute` classes inherit `CompiledRouteBase`, and Starlette/FastAPI serialize `BUFFERED_RESPONSE_TYPES` by exact `type(result)` without `bytes(resp_body)` copying [^starlette-fp-py] [^flask-fp-py].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Host framework extensions currently duplicate five independent `CompiledRoute` structs (`litestar`, `starlette`, `fastapi`, `flask`, `django`), three `_has_custom_exception_handlers` helpers, and four FastPath lookup aliases (`__vertebra_fastpath__`, `_vertebra_fastpath`, `fastpath`, `state['vertebra_fastpath_bypass']`, `extensions['vertebra'].fastpath`), while `litestar/plugin.py` mutates `type(app).__vertebra_fastpath__` globally and Starlette/FastAPI copy response buffers via `bytes(resp_body)` [^fastpath-core-py] [^fastpath-attach-py] [^litestar-fp-py] [^starlette-fp-py] [^flask-fp-py].

## 2. Requirements, Concrete Contracts & Architecture

- **`R8a` (`t01`)**: Add `FASTPATH_ATTR = "__vertebra_fastpath__"`, `get_attached_fastpath(app)`, `set_attached_fastpath(app, fastpath)` (raising `TypeError` when neither attribute assignment nor `app.state` mapping exists), `has_custom_exception_handlers(handlers, builtin_prefixes)`, and `CompiledRouteBase(msgspec.Struct, frozen=True, gc=False, kw_only=True)` (`handler_id: int`, `is_async: bool = False`, `status_code: int = 200`, `param_converters: tuple[tuple[str, str], ...] = ()`) in `_fastpath_core.py`; update `_fastpath.py` and `_asgi/_runner.py` to use `get_attached_fastpath` / `set_attached_fastpath` exclusively.
- **`R8b` (`t02`)**: Update `extensions/{litestar,starlette,fastapi}/plugin.py` to attach via `set_attached_fastpath`, removing `vertebra_fastpath_bypass` state writes and removing `type(app).__vertebra_fastpath__` property injection in `litestar/plugin.py`.
- **`R8c` (`t03`)**: Subclass `CompiledRoute(CompiledRouteBase)` across `extensions/{litestar,starlette,fastapi}/fastpath.py`, replace the three local `_has_custom_exception_handlers` functions with `_fastpath_core.has_custom_exception_handlers`, and dispatch Starlette/FastAPI result serialization via exact `type(result) in BUFFERED_RESPONSE_TYPES` returning `bytes` or `memoryview` without copying (falling back to ASGI when `result.background is not None` or for subclasses).
- **`R8d` (`t04`)**: Subclass `CompiledRoute(CompiledRouteBase)` in `extensions/{flask,django}/fastpath.py` (`kw_only=True` or inherited base fields) and call `set_attached_fastpath(app, self._fastpath)` in `extensions/flask/plugin.py`.

## 3. Documentation & OKF Knowledge Plan

`asgi-and-wsgi-runners.md` is updated in `ch07:t03` to document `CompiledRouteBase` and single `__vertebra_fastpath__` resolution.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Files | Verification |
| --- | --- | --- | --- |
| `R8a` | `t01` | `server/_fastpath_core.py`, `server/_fastpath.py`, `server/_asgi/_runner.py` | `pytest test_asgi.py` |
| `R8b` | `t02` | `extensions/{litestar,starlette,fastapi}/plugin.py` | `pytest test_litestar_plugin.py test_starlette_plugin.py test_fastapi_plugin.py` |
| `R8c` | `t03` | `extensions/{litestar,starlette,fastapi}/fastpath.py` | `pytest test_litestar_direct_fastpath.py test_starlette_plugin.py test_fastapi_plugin.py` |
| `R8d` | `t04` | `extensions/flask/{plugin,fastpath}.py`, `extensions/django/fastpath.py` | `pytest test_flask_plugin.py test_django_plugin.py` |

## 5. Implementation Plan

- [x] [780338a4] [t01-fastpath-core-route-base-and-attr-helpers.md](tasks/t01-fastpath-core-route-base-and-attr-helpers.md) — Add `CompiledRouteBase`, `get_attached_fastpath` / `set_attached_fastpath`, and `has_custom_exception_handlers` in `_fastpath_core.py`
- [x] [780338a4] [t02-asgi-plugin-fastpath-attachment.md](tasks/t02-asgi-plugin-fastpath-attachment.md) — Consolidate Litestar, Starlette, and FastAPI plugins onto `set_attached_fastpath`
- [x] [780338a4] [t03-asgi-compiled-route-and-response-types.md](tasks/t03-asgi-compiled-route-and-response-types.md) — Unify ASGI `CompiledRoute(CompiledRouteBase)`, exception checks, and copy-free `BUFFERED_RESPONSE_TYPES`
- [x] [780338a4] [t04-wsgi-compiled-route-and-flask-attachment.md](tasks/t04-wsgi-compiled-route-and-flask-attachment.md) — Unify Flask and Django `CompiledRoute(CompiledRouteBase)` and Flask `set_attached_fastpath`

## 6. Current State & Remaining Plan Snapshot

Completed (`4/4` tasks at `780338a4`).

- **Fast Chapter Gate (`< 25s`)**: `uv run --no-sync ruff check src/py/vertebra/server src/py/vertebra/extensions/{litestar,starlette,fastapi,flask,django} && uv run --no-sync mypy src/py/vertebra/server src/py/vertebra/extensions/{litestar,starlette,fastapi,flask,django} && uv run --no-sync pytest src/py/tests/unit/server/test_asgi.py src/py/tests/unit/server/test_litestar_direct_fastpath.py src/py/tests/unit/extensions/test_litestar_plugin.py src/py/tests/unit/extensions/test_starlette_plugin.py src/py/tests/unit/extensions/test_fastapi_plugin.py src/py/tests/unit/extensions/test_flask_plugin.py src/py/tests/unit/extensions/test_django_plugin.py -q`.

[^fastpath-core-py]: `src/py/vertebra/server/_fastpath_core.py` — Shared FastPath core primitives.
[^fastpath-attach-py]: `src/py/vertebra/server/_fastpath.py::attach_framework_fastpath` — FastPath attachment probing four alias locations.
[^litestar-fp-py]: `src/py/vertebra/extensions/litestar/fastpath.py::CompiledRoute` — Litestar CompiledRoute and _has_custom_exception_handlers.
[^starlette-fp-py]: `src/py/vertebra/extensions/starlette/fastpath.py::CompiledRoute` — Starlette CompiledRoute and response body copy.
[^flask-fp-py]: `src/py/vertebra/extensions/flask/fastpath.py::CompiledRoute` — Flask WSGI CompiledRoute.
