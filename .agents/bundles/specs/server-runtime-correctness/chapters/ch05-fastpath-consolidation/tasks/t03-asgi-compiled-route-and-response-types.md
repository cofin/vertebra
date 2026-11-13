---
type: Task
id: "server-runtime-correctness:ch05:t03"
flow_id: server-runtime-correctness
chapter_id: ch05-fastpath-consolidation
title: Unify ASGI CompiledRoute Structs and Copy-Free Exact-Type Response Serialization
description: "Subclass CompiledRouteBase in Litestar, Starlette, and FastAPI fastpath modules, share has_custom_exception_handlers, and serialize BUFFERED_RESPONSE_TYPES without copying."
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
  - src/py/vertebra/extensions/litestar/fastpath.py
  - src/py/vertebra/extensions/starlette/fastpath.py
  - src/py/vertebra/extensions/fastapi/fastpath.py
tests:
  - src/py/tests/unit/server/test_litestar_direct_fastpath.py
  - src/py/tests/unit/extensions/test_starlette_plugin.py
  - src/py/tests/unit/extensions/test_fastapi_plugin.py
read_set:
  - src/py/vertebra/server/_fastpath_core.py
  - src/py/vertebra/extensions/litestar/fastpath.py
  - src/py/vertebra/extensions/starlette/fastpath.py
  - src/py/vertebra/extensions/fastapi/fastpath.py
estimated_loc: 145
tags:
  - fastpath
  - serialization
  - zero-copy
  - asgi
sources:
  - id: litestar-fp-py
    resource: "src/py/vertebra/extensions/litestar/fastpath.py::CompiledRoute"
    title: Litestar CompiledRoute and _has_custom_exception_handlers
  - id: starlette-fp-py
    resource: "src/py/vertebra/extensions/starlette/fastpath.py::CompiledRoute"
    title: Starlette CompiledRoute and response body copy
  - id: fastapi-fp-py
    resource: "src/py/vertebra/extensions/fastapi/fastpath.py::CompiledRoute"
    title: FastAPI CompiledRoute and response serializer
---

# Task `t03`: Unify ASGI CompiledRoute Structs and Copy-Free Exact-Type Response Serialization

## Objective & Scope Boundary

Consolidate ASGI `CompiledRoute` classes onto `CompiledRouteBase`, replace duplicate `_has_custom_exception_handlers` helpers, and dispatch Starlette/FastAPI responses by exact `BUFFERED_RESPONSE_TYPES` without copying body buffers per [`../spec.md` §2](../spec.md) [^litestar-fp-py] [^starlette-fp-py] [^fastapi-fp-py].

## Bounded Read Manifest

- `src/py/vertebra/server/_fastpath_core.py` — `CompiledRouteBase` and `has_custom_exception_handlers`.
- `src/py/vertebra/extensions/{litestar,starlette,fastapi}/fastpath.py` — `CompiledRoute` and result serializers.

## Contracts & Edge Cases

- `CompiledRoute` in all three modules inherits `CompiledRouteBase` (`issubclass(CompiledRoute, CompiledRouteBase)`).
- Delete the three module-private `_has_custom_exception_handlers` functions in favor of `_fastpath_core.has_custom_exception_handlers`.
- Define `BUFFERED_RESPONSE_TYPES: frozenset[type]` (`Response`, `HTMLResponse`, `PlainTextResponse`, `JSONResponse`, `RedirectResponse`, plus FastAPI/Vertebra `MsgspecJSONResponse`, `ORJSONResponse`, `UJSONResponse` when imported); check `type(result) in BUFFERED_RESPONSE_TYPES` and `result.background is None`, returning `bytes` or `memoryview` directly without `bytes(...)` copy; subclasses fall back to ASGI.

## Test Matrix

- Parameterized tests verifying `issubclass(CompiledRoute, CompiledRouteBase)`, custom `JSONResponse` subclass falling back to ASGI `__call__`, `Response(content=memoryview(...))` preserving zero-copy `memoryview` identity, and `background` task fallback.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/{litestar,starlette,fastapi}/fastpath.py && uv run --no-sync mypy src/py/vertebra/extensions/{litestar,starlette,fastapi}/fastpath.py && uv run --no-sync pytest src/py/tests/unit/server/test_litestar_direct_fastpath.py src/py/tests/unit/extensions/test_starlette_plugin.py src/py/tests/unit/extensions/test_fastapi_plugin.py -q
```

## Acceptance Criteria

- [ ] Litestar, Starlette, and FastAPI `CompiledRoute` inherit `CompiledRouteBase` and share `has_custom_exception_handlers`.
- [ ] Starlette and FastAPI serialize `BUFFERED_RESPONSE_TYPES` by exact type with zero `bytes(...)` copy.

[^litestar-fp-py]: `src/py/vertebra/extensions/litestar/fastpath.py::CompiledRoute` — Litestar CompiledRoute and _has_custom_exception_handlers.
[^starlette-fp-py]: `src/py/vertebra/extensions/starlette/fastpath.py::CompiledRoute` — Starlette CompiledRoute and response body copy.
[^fastapi-fp-py]: `src/py/vertebra/extensions/fastapi/fastpath.py::CompiledRoute` — FastAPI CompiledRoute and response serializer.
