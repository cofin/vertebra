---
type: Task
id: server-runtime-correctness:ch08:t02
title: Share One Custom-Exception-Handler Check and Serialize Starlette/FastAPI Responses by Exact Type
description: Move the three _has_custom_exception_handlers copies into _fastpath_core.has_custom_exception_handlers, and replace the type(result).__name__ string-set exclusion and bytes(resp_body) copy in the Starlette and FastAPI FastPath result serializers with exact-type membership in BUFFERED_RESPONSE_TYPES and a pass-through body.
status: draft
state: pending
priority: P2
verification_strategy: behavior_tdd
depends_on:
  - "server-runtime-correctness:ch08:t01"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch08-fastpath-cleanup/spec.md
  - src/py/vertebra/server/_fastpath_core.py
  - src/py/vertebra/extensions/starlette/fastpath.py
  - src/py/vertebra/extensions/fastapi/fastpath.py
  - src/py/vertebra/extensions/litestar/fastpath.py
  - src/py/tests/unit/extensions/test_starlette_plugin.py
files:
  - src/py/vertebra/server/_fastpath_core.py
  - src/py/vertebra/extensions/starlette/fastpath.py
  - src/py/vertebra/extensions/fastapi/fastpath.py
  - src/py/vertebra/extensions/litestar/fastpath.py
tests:
  - src/py/tests/unit/extensions/test_starlette_plugin.py
  - src/py/tests/unit/extensions/test_fastapi_plugin.py
  - src/py/tests/unit/extensions/test_litestar_plugin.py
estimated_loc: 120
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T01:30:00Z"
updated_at: "2026-11-12T01:30:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T01:30:00Z"
sources:
  - id: fastpath-core-py
    resource: src/py/vertebra/server/_fastpath_core.py
    title: Shared FastPath helpers module
  - id: starlette-fastpath-py
    resource: src/py/vertebra/extensions/starlette/fastpath.py::_has_custom_exception_handlers
    title: Starlette copy of the handler check and class-name response dispatch
  - id: fastapi-fastpath-py
    resource: src/py/vertebra/extensions/fastapi/fastpath.py::_has_custom_exception_handlers
    title: FastAPI copy of the handler check and class-name response dispatch
  - id: litestar-fastpath-py
    resource: src/py/vertebra/extensions/litestar/fastpath.py::_has_custom_exception_handlers
    title: Litestar copy of the handler check over an exception map
tags:
  - fastpath
  - cleanup
  - zero-copy
---

# Task t02: Share One Custom-Exception-Handler Check and Serialize Starlette/FastAPI Responses by Exact Type

## Objective & Scope Boundary

Replace the three private `_has_custom_exception_handlers` functions with one `has_custom_exception_handlers(handlers, builtin_prefixes)` in `_fastpath_core.py`, and make the Starlette and FastAPI result serializers decide the buffered fast path by `type(result) in BUFFERED_RESPONSE_TYPES` and return `resp_body` as-is. This task owns four files because the Litestar caller must move in the same commit that deletes its private copy. Out of scope: FastPath attribute resolution (`t01`) and `serialize_fastpath_special_result`.

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts.
- `src/py/vertebra/server/_fastpath_core.py` — destination module [^fastpath-core-py].
- `src/py/vertebra/extensions/starlette/fastpath.py` — `_has_custom_exception_handlers(app)` and the response branch building `(status_code, list(raw_headers), bytes(resp_body))` [^starlette-fastpath-py].
- `src/py/vertebra/extensions/fastapi/fastpath.py` — identical copy and the same response branch in its result serializer [^fastapi-fastpath-py].
- `src/py/vertebra/extensions/litestar/fastpath.py` — `_has_custom_exception_handlers(exc_map)` with `_is_empty_sentinel` and prefix `"litestar."` [^litestar-fastpath-py].
- `src/py/tests/unit/extensions/test_starlette_plugin.py` — existing FastPath coverage.

## Symbol & Signature Contracts

```python
# src/py/vertebra/server/_fastpath_core.py
def has_custom_exception_handlers(handlers: Any, builtin_prefixes: tuple[str, ...]) -> bool:
    """Return True when ``handlers`` is a non-empty dict holding any handler defined outside ``builtin_prefixes``."""

# src/py/vertebra/extensions/starlette/fastpath.py
BUFFERED_RESPONSE_TYPES: frozenset[type]
```

- `BUFFERED_RESPONSE_TYPES = frozenset({Response, JSONResponse, HTMLResponse, PlainTextResponse, RedirectResponse})` from `starlette.responses`, imported at module top level (the module already requires Starlette).
- `fastapi/fastpath.py` imports `BUFFERED_RESPONSE_TYPES` from the Starlette module and adds FastAPI's `ORJSONResponse` and `UJSONResponse` only when present in `fastapi.responses` at import time; a missing optional class is not an error because FastAPI defines both unconditionally and only their `render` requires the optional package.
- Callers: Starlette and FastAPI pass `(getattr(app, "exception_handlers", None), ("starlette.", "fastapi."))`; Litestar passes `(exc_map, ("litestar.",))` after its own `_is_empty_sentinel(exc_map)` check returns `False`.
- The fast branch condition becomes `type(result) in BUFFERED_RESPONSE_TYPES and isinstance(raw_headers, list) and isinstance(resp_body, (bytes, memoryview)) and background is None`, returning `(status, list(raw_headers), resp_body)`.

## Edge Cases & Error Semantics

- `StreamingResponse`, `FileResponse`, and any subclass of a buffered type take the ASGI-call path unchanged.
- A `memoryview` body is returned as the identical object; downstream `AsgiSend.send_raw_response` already accepts buffer objects.
- An empty or non-dict handler map returns `False`.
- `git grep -n "_has_custom_exception_handlers\|__name__ not in\|resp_cls_name" -- src/py/vertebra/extensions` returns nothing.

## Public-Seam Test Blueprint

- **Canonical test files**: `test_starlette_plugin.py`, `test_fastapi_plugin.py`, `test_litestar_plugin.py`; dependency category `unit`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Handler check, parametrized | `({}, ("starlette.",))`; `({ValueError: starlette.exceptions.HTTPException}, ("starlette.",))`; `({ValueError: user_handler}, ("starlette.", "fastapi."))`; `(None, ("litestar.",))` | `False`; `False`; `True`; `False` |
| Buffered JSON | Route returning `JSONResponse({"a": 1})` | Fast-path tuple `(200, [...], b'{"a":1}')`, body identity equals `response.body` |
| memoryview body | Route returning `Response(content=b"x")` whose `body` is replaced by `memoryview(b"xyz")` | Returned body `is` that memoryview |
| Streaming excluded | Route returning `StreamingResponse(iter([b"a"]))` | ASGI-call path; client receives `b"a"` |
| Subclass excluded | `class Custom(JSONResponse): ...` returned from a route | ASGI-call path; identical client body |
| Litestar custom handler | Litestar app with `exception_handlers={ValueError: user_handler}` | Compiled route marks custom handlers present, matching current behaviour |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/extensions/test_starlette_plugin.py -q -k "custom_exception or buffered"`
- Final Green + lint:
  ```bash
  uv run --no-sync pytest src/py/tests/unit/extensions -q
  uv run --no-sync ruff check src/py/vertebra/server/_fastpath_core.py src/py/vertebra/extensions
  uv run --no-sync mypy src/py/vertebra/server src/py/vertebra/extensions
  ```
- Planned local commit: `refactor(extensions): share exception-handler check and serialize responses by exact type`

## Acceptance Criteria

- [ ] One `has_custom_exception_handlers` exists, in `_fastpath_core.py` (R19).
- [ ] Starlette and FastAPI serializers dispatch on exact type with no `__name__` comparison and no `bytes(resp_body)` copy (R19).

## Notes & Discoveries

- The Starlette serializer has the same class-name dispatch and copy as FastAPI; both change here so FastAPI does not keep importing a divergent helper.

[^fastpath-core-py]: `src/py/vertebra/server/_fastpath_core.py` — Shared FastPath helpers module.
[^starlette-fastpath-py]: `src/py/vertebra/extensions/starlette/fastpath.py::_has_custom_exception_handlers` — Starlette copy of the handler check and class-name response dispatch.
[^fastapi-fastpath-py]: `src/py/vertebra/extensions/fastapi/fastpath.py::_has_custom_exception_handlers` — FastAPI copy of the handler check and class-name response dispatch.
[^litestar-fastpath-py]: `src/py/vertebra/extensions/litestar/fastpath.py::_has_custom_exception_handlers` — Litestar copy of the handler check over an exception map.
