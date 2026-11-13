---
type: Task
id: "server-runtime-correctness:ch06:t02"
flow_id: server-runtime-correctness
chapter_id: ch06-runtime-native-cleanup
title: Remove Duplicate _module_cache from module_loader and typing
description: "Resolve import_optional directly against sys.modules and remove _module_cache from _typing.py, typing.py, and module_loader.py."
status: stable
state: completed
commit: db02a0b6
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/py/vertebra/utils/module_loader.py
  - src/py/vertebra/_typing.py
  - src/py/vertebra/typing.py
tests:
  - src/py/tests/unit/framework/test_typing.py
read_set:
  - src/py/vertebra/utils/module_loader.py
  - src/py/vertebra/_typing.py
  - src/py/vertebra/typing.py
  - src/py/tests/unit/framework/test_typing.py
estimated_loc: 55
tags:
  - python
  - typing
  - cleanup
sources:
  - id: module-loader-py
    resource: "src/py/vertebra/utils/module_loader.py::import_optional"
    title: Optional module import helper
  - id: typing-py
    resource: src/py/vertebra/typing.py
    title: Public typing shim re-exporting _module_cache
---

# Task `t02`: Remove Duplicate _module_cache from module_loader and typing

## Objective & Scope Boundary

Delete `_module_cache` and resolve `import_optional` via `sys.modules` per [`../spec.md` §2](../spec.md) [^module-loader-py] [^typing-py].

## Bounded Read Manifest

- `src/py/vertebra/utils/module_loader.py` — `import_optional` and `module_available`.
- `src/py/vertebra/_typing.py` & `typing.py` — `_module_cache` definition and `reset_dependency_cache`.

## Contracts & Edge Cases

- Remove `_module_cache` from `_typing.py`, `typing.py`, and `module_loader.py` (retaining `_dependency_cache` for negative availability checks).
- `import_optional(module_name)` checks `sys.modules` first (returning `None` when `sys.modules[module_name] is None`) before delegating to `importlib.import_module`.

## Test Matrix

- Unit tests in `test_typing.py` verifying `import_optional` returns loaded modules, returns `None` when `sys.modules[name] = None` or missing, and `hasattr(vertebra.typing, '_module_cache')` is `False`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/utils/module_loader.py src/py/vertebra/_typing.py src/py/vertebra/typing.py && uv run --no-sync mypy src/py/vertebra/utils/module_loader.py src/py/vertebra/typing.py && uv run --no-sync pytest src/py/tests/unit/framework/test_typing.py -q
```

## Acceptance Criteria

- [ ] `_module_cache` is completely removed across `src/py/vertebra`.
- [ ] `import_optional` and `test_typing.py` pass strict `mypy` and `pytest`.

[^module-loader-py]: `src/py/vertebra/utils/module_loader.py::import_optional` — Optional module import helper.
[^typing-py]: `src/py/vertebra/typing.py` — Public typing shim re-exporting _module_cache.
