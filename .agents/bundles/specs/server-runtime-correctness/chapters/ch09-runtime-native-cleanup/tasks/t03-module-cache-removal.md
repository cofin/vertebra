---
type: Task
id: server-runtime-correctness:ch09:t03
title: Resolve Optional Imports Through sys.modules and Delete _module_cache
description: Make import_optional return sys.modules entries or import on demand without writing a private cache, delete vertebra._typing._module_cache and its re-export, and limit reset_dependency_cache to the availability cache.
status: draft
state: pending
priority: P3
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch09-runtime-native-cleanup/spec.md
  - src/py/vertebra/utils/module_loader.py
  - src/py/vertebra/_typing.py
  - src/py/vertebra/typing.py
  - src/py/tests/unit/framework/test_typing.py
files:
  - src/py/vertebra/utils/module_loader.py
  - src/py/vertebra/_typing.py
  - src/py/vertebra/typing.py
tests:
  - src/py/tests/unit/framework/test_typing.py
estimated_loc: 50
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
created_at: "2026-11-12T01:35:00Z"
updated_at: "2026-11-12T01:35:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T01:35:00Z"
sources:
  - id: module-loader-py
    resource: src/py/vertebra/utils/module_loader.py::import_optional
    title: Optional import helper writing _module_cache
  - id: typing-py
    resource: src/py/vertebra/_typing.py::reset_dependency_cache
    title: Dependency cache reset that also clears _module_cache
tags:
  - imports
  - cleanup
---

# Task t03: Resolve Optional Imports Through sys.modules and Delete _module_cache

## Objective & Scope Boundary

`import_optional` reads and writes `vertebra._typing._module_cache`, a second copy of `sys.modules` [^module-loader-py]. Delete the cache, its re-export from `vertebra.typing`, and its clearing in `reset_dependency_cache` [^typing-py]. Out of scope: `_dependency_cache` and `module_available`.

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts.
- `src/py/vertebra/utils/module_loader.py::import_optional` — current lookup order.
- `src/py/vertebra/_typing.py` — `_module_cache`, `_dependency_cache`, `reset_dependency_cache`.
- `src/py/vertebra/typing.py` — imports and `__all__` entry `"_module_cache"`.
- `src/py/tests/unit/framework/test_typing.py` — assertions on `_module_cache`.

## Symbol & Signature Contracts

```python
def import_optional(module_name: str) -> Any | None:
    """Return ``sys.modules[module_name]`` or import it when available, else None."""
    loaded = sys.modules.get(module_name)
    if loaded is not None:
        return loaded
    if module_name in sys.modules:
        return None
    if not module_available(module_name):
        return None
    try:
        return importlib.import_module(module_name)
    except ImportError:
        return None
```

- `reset_dependency_cache(module_name: str | None = None) -> None` clears or pops only `_dependency_cache`; its docstring drops "imported module instances".
- `module_loader.py` no longer imports `_module_cache`; `typing.py` removes it from its import list and `__all__`.

## Edge Cases & Error Semantics

- `sys.modules[name] = None` (import blocked) returns `None`.
- A module that raises `ImportError` during import returns `None` and is not cached.
- `git grep -n _module_cache -- src/py` returns nothing after the test update.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/framework/test_typing.py`, dependency category `unit`. Replace the existing `_module_cache` assertions.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Loaded module | `import_optional("json")` | `is sys.modules["json"]` |
| Missing module | `import_optional("nonexistent_pkg_xyz")` | `None` |
| Blocked import | `monkeypatch.setitem(sys.modules, "blocked_mod_xyz", None)`; `import_optional("blocked_mod_xyz")` | `None` |
| Cache removed | `hasattr(vertebra._typing, "_module_cache")`, `hasattr(vertebra.typing, "_module_cache")` | `False`, `False` |
| Reset scope | `module_available("json")`; `reset_dependency_cache("json")` | `"json" not in vertebra._typing._dependency_cache` |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/framework/test_typing.py -q`
- Final Green + lint:
  ```bash
  uv run --no-sync pytest src/py/tests/unit/framework -q
  uv run --no-sync ruff check src/py/vertebra/utils/module_loader.py src/py/vertebra/_typing.py src/py/vertebra/typing.py
  uv run --no-sync mypy src/py/vertebra/utils/module_loader.py src/py/vertebra/_typing.py src/py/vertebra/typing.py
  git grep -n _module_cache -- src/py
  ```
- Planned local commit: `refactor(utils): resolve optional imports through sys.modules`

## Acceptance Criteria

- [ ] `_module_cache` is absent from `src/py` (R21).
- [ ] `import_optional` returns `sys.modules` entries and imports on demand without a private cache (R21).

## Notes & Discoveries

[^module-loader-py]: `src/py/vertebra/utils/module_loader.py::import_optional` — Optional import helper writing `_module_cache`.
[^typing-py]: `src/py/vertebra/_typing.py::reset_dependency_cache` — Dependency cache reset that also clears `_module_cache`.
