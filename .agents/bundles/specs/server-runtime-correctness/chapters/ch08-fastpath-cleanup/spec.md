---
type: Spec
parent_flow: server-runtime-correctness
chapter_id: ch08-fastpath-cleanup
title: "Chapter 08: FastPath Attribute Consolidation and Exact-Type Response Serialization"
description: Resolve framework FastPath objects through the single __vertebra_fastpath__ name, share one custom-exception-handler check in _fastpath_core, and serialize Starlette/FastAPI responses by exact type without copying the body.
status: draft
state: planned
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: null
plan_revision: 1
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
created_at: "2026-11-12T01:30:00Z"
updated_at: "2026-11-12T01:30:00Z"
generated:
  by: agent:plan-generator
  at: "2026-11-12T01:30:00Z"
sources:
  - id: fastpath-attach-py
    resource: src/py/vertebra/server/_fastpath.py::attach_framework_fastpath
    title: FastPath resolver that probes four alias locations
  - id: fastpath-core-py
    resource: src/py/vertebra/server/_fastpath_core.py
    title: Shared FastPath helpers module
  - id: starlette-fastpath-py
    resource: src/py/vertebra/extensions/starlette/fastpath.py
    title: Starlette FastPath with class-name response dispatch and bytes copy
  - id: fastapi-fastpath-py
    resource: src/py/vertebra/extensions/fastapi/fastpath.py
    title: FastAPI FastPath with class-name response dispatch and bytes copy
tags:
  - fastpath
  - cleanup
  - serialization
---

# Chapter 08: FastPath Attribute Consolidation and Exact-Type Response Serialization

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`attach_framework_fastpath` looks for an existing `FastPath` on `__vertebra_fastpath__`, then `fastpath`, then `state["vertebra_fastpath_bypass"]`, then `extensions["vertebra"].fastpath`, and writes both `state["vertebra_fastpath_bypass"]` and `__vertebra_fastpath__` [^fastpath-attach-py]. `AsgiRunner` repeats the `state["vertebra_fastpath_bypass"]` probe, the Litestar, FastAPI, and Starlette plugins write that state key, and the Litestar plugin installs a class-level `__vertebra_fastpath__` property on `type(app)` that reads it back. `_has_custom_exception_handlers` exists three times (`starlette/fastpath.py`, `fastapi/fastpath.py`, `litestar/fastpath.py`) with only the built-in module prefixes differing [^fastpath-core-py]. The Starlette and FastAPI result serializers exclude streaming responses by comparing `type(result).__name__` against `{"StreamingResponse", "FileResponse", "_StreamingResponse"}` and copy every buffered body through `bytes(resp_body)` [^starlette-fastpath-py] [^fastapi-fastpath-py].

### 1.2 Non-Goals

- Changing `FastPath` dispatch semantics, route compilation, or the `__vertebra_fastpath_attached__` idempotency marker.
- Native-module, entropy, Spanner, and embedding cleanups (owned by `ch09-runtime-native-cleanup`).

### 1.3 Upstream Baseline & Code Analysis

- `litestar.Litestar` defines `__slots__` without `__dict__`, so `app.__vertebra_fastpath__ = bypass` raises `AttributeError`; Litestar apps carry the `FastPath` in `app.state`. The single name therefore lives on the app object when it accepts attributes, and under the `app.state["__vertebra_fastpath__"]` key for slotted apps; no other name is read or written.
- `vertebra.extensions.flask.plugin.VertebraPlugin` registers itself in `app.extensions["vertebra"]` and exposes a `fastpath` property; after this chapter the Flask plugin sets `app.__vertebra_fastpath__` when it builds the `FastPath`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R19** (`t01`, `t02`): FastPath resolution uses only `__vertebra_fastpath__` (attribute, or `state["__vertebra_fastpath__"]` for slotted apps); one `has_custom_exception_handlers` lives in `_fastpath_core.py`; Starlette and FastAPI result serialization dispatches on exact response type and returns the body without copying.

### 2.2 Concrete Type & Signature Contracts

```python
# src/py/vertebra/server/_fastpath_core.py
FASTPATH_ATTR: Final = "__vertebra_fastpath__"

def get_attached_fastpath(app: Any) -> Any | None: ...
def set_attached_fastpath(app: Any, fastpath: Any) -> None: ...
def has_custom_exception_handlers(handlers: Any, builtin_prefixes: tuple[str, ...]) -> bool: ...

# src/py/vertebra/extensions/starlette/fastpath.py
BUFFERED_RESPONSE_TYPES: frozenset[type]

```

### 2.3 Caller → Seam → Collaborator Map

| Caller | Seam | Collaborator |
| --- | --- | --- |
| `Worker._instantiate_mount_runners`, `AsgiRunner.__init__`, `WsgiAdapter.__init__` | `attach_framework_fastpath` | `get_attached_fastpath` / `set_attached_fastpath` |
| `AsgiRunner.dispatch_app` resolution | `get_attached_fastpath` | framework `FastPath.dispatch` |
| Litestar/FastAPI/Starlette/Flask `VertebraPlugin` finalization | `set_attached_fastpath` | app attribute or `app.state` |
| `compile_fastpath_routes` in Starlette, FastAPI, Litestar | `has_custom_exception_handlers` | `app.exception_handlers` / Litestar exception map |

### 2.4 Edge-Case & Failure-Mode Matrix

| Case | Behaviour |
| --- | --- |
| App rejects attribute assignment and has no `state` mapping | `set_attached_fastpath` raises `TypeError` naming the app type |
| Response subclass of `JSONResponse` not in `BUFFERED_RESPONSE_TYPES` | Takes the ASGI-call path; output identical, no fast serialization |
| `Response.body` is a `memoryview` | Returned as the same `memoryview` object |
| `Response.background` is set | Takes the ASGI-call path |

### 2.5 Mandatory Alternatives Comparison

| Decision | Chosen | Rejected and why |
| --- | --- | --- |
| FastPath location for slotted apps | `app.state["__vertebra_fastpath__"]` under the same name | Class-level property on `type(app)` (mutates the shared `Litestar` class for every instance); plugin-instance lookup (a second name to probe). |
| Response exclusion | Exact-type membership in `BUFFERED_RESPONSE_TYPES` | `isinstance` exclusion of `StreamingResponse`/`FileResponse` (any user subclass that overrides `__call__` would be serialized from `.body`). |

## 3. Documentation & OKF Knowledge Plan

The FastPath resolver description in `.agents/bundles/knowledge/architecture/worker/asgi-and-wsgi-runners.md` lists the removed alias locations; `ch10:t03` rewrites it. No public `docs/**` page documents the removed aliases.

## 4. Task Plan

- [ ] [t01](tasks/t01-fastpath-single-attribute.md) — Single `__vertebra_fastpath__` resolution across the resolver, runner, and four plugins.
- [ ] [t02](tasks/t02-fastpath-shared-helpers-and-response-serialization.md) — Shared `has_custom_exception_handlers` and exact-type, copy-free response serialization.

**Chapter Checkpoint:** `make lint-all && make py-test` exits `0`.

[^fastpath-attach-py]: `src/py/vertebra/server/_fastpath.py::attach_framework_fastpath` — FastPath resolver that probes four alias locations.
[^fastpath-core-py]: `src/py/vertebra/server/_fastpath_core.py` — Shared FastPath helpers module.
[^starlette-fastpath-py]: `src/py/vertebra/extensions/starlette/fastpath.py` — Starlette FastPath with class-name response dispatch and bytes copy.
[^fastapi-fastpath-py]: `src/py/vertebra/extensions/fastapi/fastpath.py` — FastAPI FastPath with class-name response dispatch and bytes copy.
