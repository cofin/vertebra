---
type: Task
id: server-runtime-correctness:ch08:t01
title: Resolve Framework FastPath Objects Through the Single __vertebra_fastpath__ Name
description: Add get_attached_fastpath and set_attached_fastpath to _fastpath_core, make attach_framework_fastpath and AsgiRunner use only them, and make the Litestar, FastAPI, Starlette, and Flask plugins publish the FastPath only under __vertebra_fastpath__, removing the fastpath attribute probe, the vertebra_fastpath_bypass state key, the extensions["vertebra"].fastpath probe, and the Litestar class-level property.
status: draft
state: pending
priority: P2
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch08-fastpath-cleanup/spec.md
  - src/py/vertebra/server/_fastpath.py
  - src/py/vertebra/server/_fastpath_core.py
  - src/py/vertebra/server/_asgi/_runner.py
  - src/py/vertebra/extensions/litestar/plugin.py
  - src/py/vertebra/extensions/flask/plugin.py
files:
  - src/py/vertebra/server/_fastpath.py
  - src/py/vertebra/server/_fastpath_core.py
  - src/py/vertebra/server/_asgi/_runner.py
  - src/py/vertebra/extensions/litestar/plugin.py
  - src/py/vertebra/extensions/fastapi/plugin.py
  - src/py/vertebra/extensions/starlette/plugin.py
  - src/py/vertebra/extensions/flask/plugin.py
tests:
  - src/py/tests/unit/extensions/test_litestar_plugin.py
  - src/py/tests/unit/extensions/test_fastapi_plugin.py
  - src/py/tests/unit/extensions/test_starlette_plugin.py
estimated_loc: 160
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
  - id: fastpath-attach-py
    resource: src/py/vertebra/server/_fastpath.py::attach_framework_fastpath
    title: FastPath resolver that probes four alias locations
  - id: asgi-runner-py
    resource: src/py/vertebra/server/_asgi/_runner.py::AsgiRunner
    title: Runner FastPath lookup that also probes the bypass state key
  - id: litestar-plugin-py
    resource: src/py/vertebra/extensions/litestar/plugin.py::VertebraPlugin
    title: Litestar plugin writing vertebra_fastpath_bypass and a class-level property
  - id: flask-plugin-py
    resource: src/py/vertebra/extensions/flask/plugin.py::VertebraPlugin
    title: Flask plugin exposing FastPath through extensions["vertebra"].fastpath
tags:
  - fastpath
  - cleanup
---

# Task t01: Resolve Framework FastPath Objects Through the Single __vertebra_fastpath__ Name

## Objective & Scope Boundary

Publish and resolve framework `FastPath` objects only under `__vertebra_fastpath__`: as an attribute on apps that accept attributes, and as `app.state["__vertebra_fastpath__"]` on slotted apps (Litestar). Remove the `getattr(app, "fastpath")` probes, every `"vertebra_fastpath_bypass"` read and write, the `extensions["vertebra"].fastpath` probe, and the `type(app).__vertebra_fastpath__ = property(...)` fallback. This task exceeds the three-owned-file guideline because the resolver and all four publishers must switch in one commit; splitting them would leave a commit where a plugin publishes under a name the resolver no longer reads. Out of scope: `_has_custom_exception_handlers` and response serialization (`t02`); the `@vertebra_route`/`@vertebra_view` decorator flag `__vertebra_fastpath__ = True` on view callables, which is unrelated to app-level resolution.

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts and edge cases.
- `src/py/vertebra/server/_fastpath.py::attach_framework_fastpath` [^fastpath-attach-py].
- `src/py/vertebra/server/_fastpath_core.py` — destination for the accessors.
- `src/py/vertebra/server/_asgi/_runner.py::AsgiRunner` — FastPath lookup that reads `state.get("vertebra_fastpath_bypass")` [^asgi-runner-py].
- `src/py/vertebra/extensions/litestar/plugin.py::VertebraPlugin` — `on_app_init` writes `app_config.state["vertebra_fastpath_bypass"]`; finalization reads it, rebinds, and installs the class property [^litestar-plugin-py].
- `src/py/vertebra/extensions/flask/plugin.py::VertebraPlugin` — `extensions["vertebra"] = self` and the `fastpath` property [^flask-plugin-py].

## Symbol & Signature Contracts

```python
# src/py/vertebra/server/_fastpath_core.py
FASTPATH_ATTR: Final = "__vertebra_fastpath__"

def get_attached_fastpath(app: Any) -> Any | None:
    """Return the FastPath stored on ``app.__vertebra_fastpath__`` or ``app.state["__vertebra_fastpath__"]``."""

def set_attached_fastpath(app: Any, fastpath: Any) -> None:
    """Store ``fastpath`` as ``app.__vertebra_fastpath__``, or in ``app.state`` when the app rejects attributes.

    Raises:
        TypeError: The app accepts neither an attribute nor a ``state`` item.
    """
```

- `get_attached_fastpath` reads the attribute first; when absent it reads `state["__vertebra_fastpath__"]` from a `state` exposing `get`, or `getattr(state, "__vertebra_fastpath__", None)` for attribute-style state (Starlette `State`).
- `attach_framework_fastpath` replaces both alias chains with `get_attached_fastpath(app)` and writes newly built objects through `set_attached_fastpath(app, existing_fp)`.
- `AsgiRunner` returns `get_attached_fastpath(app)` when it exposes a callable `dispatch`, else `None`.
- The Litestar plugin stores the pre-built `FastPath` in `app_config.state["__vertebra_fastpath__"]` during `on_app_init` and rebinds the same object during finalization; it no longer touches `type(app)`.
- The FastAPI and Starlette plugins call `set_attached_fastpath(app, bypass)` in place of `_set_state_item(state, "vertebra_fastpath_bypass", bypass)` plus the attribute write.
- The Flask plugin calls `set_attached_fastpath(app, self._fastpath)` when it builds the FastPath; `extensions["vertebra"]` keeps the plugin instance for Flask extension conventions but is never probed for a FastPath.

## Edge Cases & Error Semantics

- An app whose `__vertebra_fastpath_attached__` is set keeps the idempotent early return; prefix rebinding uses `get_attached_fastpath`.
- A slotted app without `state` raises `TypeError("cannot attach FastPath to <type name>: no attribute slot or state mapping")` from `set_attached_fastpath`.
- An app carrying only the old `state["vertebra_fastpath_bypass"]` key resolves to `None` and gets a freshly built FastPath from its provider.
- `git grep -n -e '"vertebra_fastpath_bypass"' -e 'getattr(app, "fastpath"' -e '\.fastpath\b' -- src/py/vertebra/server` returns nothing.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/extensions/test_litestar_plugin.py`, `test_fastapi_plugin.py`, `test_starlette_plugin.py`; dependency category `unit`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Litestar slotted app | `Litestar(route_handlers=[...], plugins=[VertebraPlugin()])` | `app.state["__vertebra_fastpath__"]` is the `FastPath`; `"vertebra_fastpath_bypass" not in app.state`; `"__vertebra_fastpath__" not in vars(Litestar)` |
| Litestar resolution | `attach_framework_fastpath(app)` on the app above | Returns `FastPath.wrap_asgi(...)` built from `app.state["__vertebra_fastpath__"]`; provider `build` not called |
| FastAPI attribute | `FastAPI()` with `VertebraPlugin` finalized | `app.__vertebra_fastpath__` is the `FastPath`; `getattr(app.state, "vertebra_fastpath_bypass", None) is None` |
| Starlette attribute | `Starlette(routes=[...])` with `VertebraPlugin` finalized | `get_attached_fastpath(app) is app.__vertebra_fastpath__` |
| Stale alias ignored (parametrized over Starlette and FastAPI) | App with only `app.state.vertebra_fastpath_bypass = sentinel` | `get_attached_fastpath(app) is None` |
| Unattachable app | `set_attached_fastpath(object(), fp)` | `TypeError` containing `cannot attach FastPath to object` |
| Runner lookup | `AsgiRunner(app)` on the Litestar app | Runner dispatch target is `app.state["__vertebra_fastpath__"]` |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/extensions/test_litestar_plugin.py src/py/tests/unit/extensions/test_fastapi_plugin.py src/py/tests/unit/extensions/test_starlette_plugin.py -q -k fastpath`
- Final Green + lint:
  ```bash
  uv run --no-sync pytest src/py/tests/unit/extensions src/py/tests/unit/server -q
  uv run --no-sync ruff check src/py/vertebra/server src/py/vertebra/extensions
  uv run --no-sync mypy src/py/vertebra/server src/py/vertebra/extensions
  git grep -n -e 'vertebra_fastpath_bypass' -e 'getattr(app, "fastpath"' -- src/py/vertebra
  ```
- Planned local commit: `refactor(server): resolve FastPath only through __vertebra_fastpath__`

## Acceptance Criteria

- [ ] `vertebra_fastpath_bypass`, the `fastpath` attribute probe, and the `extensions["vertebra"].fastpath` probe are absent from `src/py/vertebra` (R19).
- [ ] Litestar apps carry the FastPath in `app.state["__vertebra_fastpath__"]` and `Litestar` gains no class attribute (R19).
- [ ] Attribute-capable apps carry it on `app.__vertebra_fastpath__` (R19).

## Notes & Discoveries

- Litestar's `__slots__` excludes `__dict__`; this is why slotted apps use the state mapping under the same name.

[^fastpath-attach-py]: `src/py/vertebra/server/_fastpath.py::attach_framework_fastpath` — FastPath resolver that probes four alias locations.
[^asgi-runner-py]: `src/py/vertebra/server/_asgi/_runner.py::AsgiRunner` — Runner FastPath lookup that also probes the bypass state key.
[^litestar-plugin-py]: `src/py/vertebra/extensions/litestar/plugin.py::VertebraPlugin` — Litestar plugin writing `vertebra_fastpath_bypass` and a class-level property.
[^flask-plugin-py]: `src/py/vertebra/extensions/flask/plugin.py::VertebraPlugin` — Flask plugin exposing FastPath through `extensions["vertebra"].fastpath`.
