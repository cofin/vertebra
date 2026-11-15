---
type: Task
id: "strict-typing-hygiene:t01"
flow_id: strict-typing-hygiene
title: "Prune Redundant Mypy Overrides, Fix Masked Imports, and Add Benchmark Lint Paths"
description: "Remove 43+ PEP 561 typed packages from ignore_missing_imports in pyproject.toml, fix broken imports in extensions/litestar/queues/backend.py, and add tools/benchmarks to pyproject.toml and Makefile."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - pyproject.toml
  - Makefile
  - src/py/vertebra/extensions/litestar/queues/backend.py
tests:
  - src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py
read_set:
  - pyproject.toml
  - Makefile
  - src/py/vertebra/extensions/litestar/queues/backend.py
  - src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py
estimated_loc: 90
tags:
  - typing
  - mypy
  - pyproject
  - litestar
sources:
  - id: pyproject-mypy
    resource: pyproject.toml
    title: "[tool.mypy] files and [[tool.mypy.overrides]] ignore_missing_imports list"
  - id: makefile-lint
    resource: Makefile
    title: "lint and lint-all Ruff and Mypy target paths"
  - id: litestar-queues-backend
    resource: src/py/vertebra/extensions/litestar/queues/backend.py::SHMQueueBackend
    title: "Broken litestar_queues.records TYPE_CHECKING import at line 23"
---

# Task `t01`: Prune Redundant Mypy Overrides, Fix Masked Imports, and Add Benchmark Lint Paths

## Objective & Scope Boundary

Prune the `40+` redundant `ignore_missing_imports = true` entries from `pyproject.toml` (`52` patterns currently configured) and verify/fix any masked broken imports in `src/py/vertebra/extensions/litestar/queues/backend.py` (or its post-`queues-tasks-and-channels` `StoreQueueBackend` layout) per [`../spec.md` §2](../spec.md) [^pyproject-mypy] [^makefile-lint] [^litestar-queues-backend]. Out of scope: benchmark unit regression tests (`t02`) and enabling `src/py/tests` in `mypy` (`t03`).

## Bounded Read Manifest

- `pyproject.toml` — `[tool.mypy]` (`lines 306–368`) [^pyproject-mypy].
- `Makefile` — `lint` and `lint-all` Ruff and Mypy invocations (`lines 358–393`) [^makefile-lint].
- `src/py/vertebra/extensions/litestar/queues/backend.py` — `TYPE_CHECKING` imports [^litestar-queues-backend].
- `src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py` — Litestar queues unit suite.

## Contracts & Edge Cases

- Remove `pydantic.*`, `attrs`, `uvloop`, `starlette*`, `fastapi*`, `flask*`, `litestar*`, `litestar_queues*`, `litestar_security*`, `litestar_autowire*`, `litestar_email*`, `litestar_mcp*`, `litestar_vite*`, `litestar_asyncapi*`, `litestar_htmx*`, `advanced_alchemy*`, `sqlalchemy*`, `alembic*`, `sqlspec*`, `django*`, `pytest_databases*`, and `duckdb` from `[[tool.mypy.overrides]]`.
- Retain only genuinely untyped C or uninstalled optional modules (`pyarrow`, `pyarrow.*`, `polars`, `polars.*`, `numpy`, `numpy.*`, `rloop`, `winloop`, `zuvloop`, `spikard`, `spikard.*`, `concurrent.interpreters`).
- Ensure zero `litestar_queues.records` imports or `# type: ignore` suppressions exist across `src/py/vertebra/extensions/litestar/queues/`, and confirm `tools/benchmarks` stays registered in `[tool.mypy].files` and `Makefile`.

## Test Matrix

- Verify `uv run --no-sync mypy src/py/vertebra tools/scripts tools/pgo tools/docs docs/examples` exits `0` against real installed PEP 561 stubs; run `test_litestar_ecosystem_queues.py`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/litestar/queues/backend.py && uv run --no-sync mypy src/py/vertebra tools/scripts tools/pgo tools/docs docs/examples && uv run --no-sync pytest src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py -q
```

## Acceptance Criteria

- [ ] `pyproject.toml` retains only genuinely untyped/uninstalled optional packages in `ignore_missing_imports`, `src/py/vertebra` passes `mypy --strict` with zero masked broken imports, and `tools/benchmarks` is registered in `pyproject.toml` and `Makefile` (`R1`).
- [ ] Zero regressions in `test_litestar_ecosystem_queues.py`.

[^pyproject-mypy]: `pyproject.toml` — `[tool.mypy]` `files` and `[[tool.mypy.overrides]]` `ignore_missing_imports` list.
[^makefile-lint]: `Makefile` — `lint` and `lint-all` Ruff and Mypy target paths.
[^litestar-queues-backend]: `src/py/vertebra/extensions/litestar/queues/backend.py::SHMQueueBackend` — Broken `litestar_queues.records` `TYPE_CHECKING` import at line 23.
