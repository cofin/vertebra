---
type: Task
id: "strict-typing-hygiene:t03"
flow_id: strict-typing-hygiene
title: "Enable Mypy Verification Across Unit and Integration Test Suites"
description: "Add structural Spanner client Protocols, clean up test annotations across src/py/tests/unit/** and src/py/tests/integration/**, and enable test-suite mypy in pyproject.toml and Makefile."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 18baadcd
depends_on:
  - "strict-typing-hygiene:t02"
files:
  - pyproject.toml
  - Makefile
  - src/py/vertebra/db/adapters/spanner/driver.py
tests:
  - src/py/tests/conftest.py
  - src/py/tests/unit/conftest.py
  - src/py/tests/unit/db/test_spanner.py
  - src/py/tests/integration/db/test_driver_conformance.py
read_set:
  - pyproject.toml
  - Makefile
  - src/py/vertebra/db/adapters/spanner/driver.py
  - src/py/tests/conftest.py
  - src/py/tests/unit/db/test_spanner.py
  - src/py/tests/integration/db/test_driver_conformance.py
estimated_loc: 245
tags:
  - typing
  - mypy
  - testing
  - spanner
sources:
  - id: spanner-driver
    resource: src/py/vertebra/db/adapters/spanner/driver.py::SyncSpannerDriver
    title: "SyncSpannerDriver and AsyncSpannerDriver client parameter annotations"
  - id: makefile-lint
    resource: Makefile
    title: "lint and lint-all mypy commands"
  - id: tests-conftest
    resource: src/py/tests/conftest.py
    title: "Root pytest fixtures and shared test typing surface"
---

# Task `t03`: Enable Mypy Verification Across Unit and Integration Test Suites

## Objective & Scope Boundary

Add structural `SyncSpannerClientProtocol` / `AsyncSpannerClientProtocol` (`typing.Protocol`) annotations to `src/py/vertebra/db/adapters/spanner/driver.py`, resolve typing issues across `src/py/tests/unit/**` and `src/py/tests/integration/**`, and wire both test trees into `pyproject.toml` (`[tool.mypy].files`) and `Makefile` (`lint` and `lint-all`) per [`../spec.md` §2](../spec.md) [^spanner-driver] [^makefile-lint] [^tests-conftest]. Out of scope: OKF documentation sync (`t04`).

## Bounded Read Manifest

- `pyproject.toml` — `[tool.mypy]` configuration and `[[tool.mypy.overrides]]`.
- `Makefile` — `lint` and `lint-all` `mypy` commands [^makefile-lint].
- `src/py/vertebra/db/adapters/spanner/driver.py` — `client: SyncSpannerClient | None` / `AsyncSpannerClient | None` [^spanner-driver].
- `src/py/tests/conftest.py`, `src/py/tests/unit/db/test_spanner.py`, and `src/py/tests/integration/db/test_driver_conformance.py` — representative test double and fixture patterns [^tests-conftest].

## Contracts & Edge Cases

- `src/py/vertebra/db/adapters/spanner/driver.py`: define `SyncSpannerClientProtocol` and `AsyncSpannerClientProtocol` matching `vertebra._spanner.{SyncSpannerClient,AsyncSpannerClient}` methods so `@final` PyO3 client classes and deterministic test ports in `test_spanner.py` / `test_sqlspec_spanner.py` both type-check without `cast` or `# type: ignore`.
- Across `src/py/tests/unit/**` and `src/py/tests/integration/**`: narrow `import_optional(...)` returns (`assert mod is not None` or `pytest.skip(...)`), replace `object()` dummy arguments passed to `LeaseBroker | None` with real `LeaseBroker(4)` or `None`, add missing fixture/helper PEP 585/604 annotations, and introduce zero `# type: ignore` or inline `#` comments.
- Add `"src/py/tests/unit"` and `"src/py/tests/integration"` to `[tool.mypy].files` in `pyproject.toml` and to the `uv run mypy` lines in `Makefile` (`lint` and `lint-all`).

## Test Matrix

- Run `uv run --no-sync mypy` (with zero CLI args, exercising `[tool.mypy].files`) and `uv run --no-sync mypy src/py/vertebra src/py/tests/unit src/py/tests/integration tools/benchmarks tools/scripts tools/pgo tools/docs docs/examples`; run `test_spanner.py` to confirm zero runtime regressions.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/db/adapters/spanner/driver.py src/py/tests && uv run --no-sync mypy src/py/vertebra src/py/tests/unit src/py/tests/integration tools/benchmarks tools/scripts tools/pgo tools/docs docs/examples && uv run --no-sync pytest src/py/tests/unit/db/test_spanner.py -q
```

## Acceptance Criteria

- [ ] `src/py/tests/unit/**` and `src/py/tests/integration/**` are enforced by `mypy` in `pyproject.toml` and `Makefile` (`lint` / `lint-all`) with `0` errors and zero `# type: ignore` additions (`R3`).
- [ ] Zero regressions in `test_spanner.py` and Ruff.

[^spanner-driver]: `src/py/vertebra/db/adapters/spanner/driver.py::SyncSpannerDriver` — `SyncSpannerDriver` and `AsyncSpannerDriver` `client` parameter annotations.
[^makefile-lint]: `Makefile` — `lint` and `lint-all` `mypy` commands.
[^tests-conftest]: `src/py/tests/conftest.py` — Root pytest fixtures and shared test typing surface.
