---
type: Task
id: "server-runtime-correctness:ch05:t01"
chapter_id: ch05-supervisor-exports-and-worker-lanes
title: Lazily export the vertebra.supervisor launcher API
description: Replace the docstring-only vertebra.supervisor package with lazy_exports for run_supervisor, build_server_command, find_server_binary, find_repo_root, and worker_main, without importing the worker module at package import time.
status: draft
state: pending
priority: P2
verification_strategy: behavior_tdd
depends_on:
  - "server-runtime-correctness:ch04:t02"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch05-supervisor-exports-and-worker-lanes/spec.md
  - src/py/vertebra/supervisor/__init__.py
  - src/py/vertebra/server/__init__.py
  - src/py/vertebra/utils/module_loader.py
  - src/py/vertebra/supervisor/_supervisor.py
  - src/py/tests/unit/cli/test_cli_commands.py
files:
  - src/py/vertebra/supervisor/__init__.py
tests:
  - src/py/tests/unit/cli/test_cli_commands.py
estimated_loc: 60
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
created_at: "2026-11-12T00:45:00Z"
updated_at: "2026-11-12T00:45:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:45:00Z"
sources:
  - id: supervisor-init-py
    resource: src/py/vertebra/supervisor/__init__.py
    title: Docstring-only supervisor package
  - id: lazy-exports-py
    resource: src/py/vertebra/utils/module_loader.py::lazy_exports
    title: PEP 562 lazy export helper
tags:
  - supervisor
  - exports
---

# Task t01: Lazily Export the vertebra.supervisor Launcher API

## Objective & Scope Boundary

Rewrite `src/py/vertebra/supervisor/__init__.py` following the `vertebra.server` pattern: a module docstring describing the package, a `TYPE_CHECKING` import block, `__all__`, `__lazy_modules__`, `_EXPORT_MAP`, and `__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)`. Out of scope: changes to `_supervisor.py` (`ch06:t02`) and `_worker.py` (`ch04:t02`).

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts.
- `src/py/vertebra/supervisor/__init__.py` [^supervisor-init-py].
- `src/py/vertebra/server/__init__.py` — reference layout.
- `src/py/vertebra/utils/module_loader.py::lazy_exports` [^lazy-exports-py].
- `src/py/vertebra/supervisor/_supervisor.py::__all__` — `build_server_command`, `find_repo_root`, `find_server_binary`, `run_supervisor`.
- `src/py/tests/unit/cli/test_cli_commands.py` — supervisor test location.

## Symbol & Signature Contracts

- `__all__ = ("build_server_command", "find_repo_root", "find_server_binary", "run_supervisor", "worker_main")`.
- `_EXPORT_MAP = {"build_server_command": ("vertebra.supervisor._supervisor", "build_server_command"), "find_repo_root": (..., "find_repo_root"), "find_server_binary": (..., "find_server_binary"), "run_supervisor": (..., "run_supervisor"), "worker_main": ("vertebra.supervisor._worker", "main")}`.
- `__lazy_modules__ = ["vertebra.supervisor._supervisor", "vertebra.supervisor._worker"]`.

## Edge Cases & Error Semantics

- `import vertebra.supervisor` does not import `vertebra.supervisor._worker`.
- Unknown attributes raise `AttributeError` from `lazy_exports`.
- `python -m vertebra.supervisor._worker` emits no `RuntimeWarning` about the module already being in `sys.modules`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/cli/test_cli_commands.py`, dependency category `unit`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Exports resolve | `from vertebra.supervisor import build_server_command, find_repo_root, find_server_binary, run_supervisor, worker_main` | Each is the same object as the attribute in `_supervisor` / `_worker.main` |
| Lazy worker import | Subprocess `python -c "import sys, vertebra.supervisor; print('vertebra.supervisor._worker' in sys.modules)"` | Prints `False` |
| No runpy warning | Subprocess `python -W error::RuntimeWarning -c "import runpy, vertebra.supervisor; runpy.run_module('vertebra.supervisor._worker', run_name='not_main')"` | Exit code `0` |
| Unknown attribute | `vertebra.supervisor.missing` | `AttributeError` |
| `dir()` | `sorted(dir(vertebra.supervisor))` | Contains every `__all__` name |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/cli/test_cli_commands.py -q -k supervisor_exports`
- Final Green + lint:
  ```bash
  uv run --no-sync pytest src/py/tests/unit/cli/test_cli_commands.py -q
  uv run --no-sync ruff check src/py/vertebra/supervisor
  uv run --no-sync mypy src/py/vertebra/supervisor
  ```
- Planned local commit: `fix(supervisor): export launcher API from vertebra.supervisor`

## Acceptance Criteria

- [ ] `vertebra.supervisor` exports the five names lazily (R11).
- [ ] Importing the package never imports `_worker` (R11).

## Notes & Discoveries

[^supervisor-init-py]: `src/py/vertebra/supervisor/__init__.py` — Docstring-only supervisor package.
[^lazy-exports-py]: `src/py/vertebra/utils/module_loader.py::lazy_exports` — PEP 562 lazy export helper.
