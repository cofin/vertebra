---
type: Spec
flow_id: strict-typing-hygiene
title: "Strict Python Typing Hygiene AcrossOverrides, Benchmarks, and Test Suites"
description: "Prunes redundant ignore_missing_imports overrides in pyproject.toml, fixes masked broken imports and tools/benchmarks subprocess typing bugs, enables mypy verification across unit and integration test suites, and synchronizes typing OKF chapters."
status: stable
state: planned
planning_quality: standard
planning_degraded_reason: null
has_chapters: false
current_task: "strict-typing-hygiene:t01"
parent_prd: integration-remediation
depends_on_flows:
  - queues-tasks-and-channels
  - sqlspec-adapter-parity
  - sqlalchemy-and-django-integration
plan_revision: 1
plan_commit: 18baadcd
created_at: "2026-11-12T23:58:00Z"
updated_at: "2026-11-12T23:58:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:58:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:58:00Z"
    commit: 18baadcd
tags:
  - typing
  - mypy
  - benchmarks
  - testing
  - hygiene
sources:
  - id: pyproject-mypy
    resource: pyproject.toml
    title: "[tool.mypy] files and 50-pattern ignore_missing_imports override block"
  - id: makefile-lint
    resource: Makefile
    title: "lint and lint-all Ruff and Mypy invocations"
  - id: litestar-queues-backend
    resource: src/py/vertebra/extensions/litestar/queues/backend.py::SHMQueueBackend
    title: "Masked broken litestar_queues.records import under TYPE_CHECKING"
  - id: bench-runner
    resource: tools/benchmarks/runner.py::ServerRunner
    title: "ServerRunner Popen[str] annotation mismatch and str.decode bug at line 170"
  - id: bench-asgi
    resource: tools/benchmarks/apps/asgi.py
    title: "Benchmark ASGI app Any return annotation drift"
  - id: bench-spikard
    resource: tools/benchmarks/apps/spikard_app.py
    title: "Benchmark Spikard app untyped decorators and Response argument mismatch"
  - id: spanner-driver
    resource: src/py/vertebra/db/adapters/spanner/driver.py::SyncSpannerDriver
    title: "SyncSpannerDriver and AsyncSpannerDriver @final client parameter annotations"
  - id: okf-tech-stack
    resource: .agents/bundles/product/tech-stack.md
    title: "Technology stack Mypy Overrides and verification toolchain documentation"
  - id: okf-test-matrix
    resource: .agents/bundles/knowledge/testing/polyglot-test-matrix.md
    title: "Polyglot test matrix OKF chapter (currently 243 lines)"
  - id: okf-workflow
    resource: .agents/bundles/knowledge/workflow.md
    title: "Project workflow Makefile lint and type-check commands"
---

# Specification: Strict Python Typing Hygiene Across Overrides, Benchmarks, and Test Suites

<!-- truth: start -->
- **Baseline (`334dc98d`)**: `pyproject.toml` carries `52` `ignore_missing_imports = true` patterns — `43+` of which suppress installed PEP 561 / `django-stubs` packages (`duckdb`, `spikard`, `spikard.*`, `litestar*`, etc.) and mask a broken `litestar_queues.records` import in `extensions/litestar/queues/backend.py` [^pyproject-mypy] [^litestar-queues-backend]; `tools/benchmarks` paths are already added in `pyproject.toml`/`Makefile` and strict `mypy`/`ruff` errors across `runner.py`, `asgi.py`, and `spikard_app.py` are fixed (`t02` regression test pending) [^bench-runner]; and `src/py/tests/{unit,integration}` remains unchecked by `mypy` in `Makefile` ([#59](https://github.com/cofin/vertebra/issues/59)) [^makefile-lint].
- **Target Architecture**: `[tool.mypy.overrides]` retains only genuinely untyped C or uninstalled optional packages (`pyarrow*`, `polars*`, `numpy*`, `rloop`, `winloop`, `fastembed*`, `datafusion*`, `wasmtime*`, `zstandard*`, `spikard*`, `concurrent.interpreters`); `tools/benchmarks/**`, `src/py/tests/unit/**`, and `src/py/tests/integration/**` pass `mypy` and Ruff in `Makefile` (`lint` / `lint-all`); `SpannerDriver` accepts structural client `Protocol`s for typed test doubles [^spanner-driver]; and `tech-stack.md`, `polyglot-test-matrix.md` (already `196L <= 200L`), and `workflow.md` stay `<= 200` lines [^okf-tech-stack] [^okf-test-matrix] [^okf-workflow].
- **Scope & Boundaries**: Chapterless flow (`4` compact tasks) under [integration-remediation](../integration-remediation/spec.md), sequenced after `queues-tasks-and-channels`, `sqlspec-adapter-parity`, and `sqlalchemy-and-django-integration` so extension and test files are edited once.
<!-- truth: end -->

## 1. Specification, Context & Provenance

At `HEAD @ 334dc98d` ([#59](https://github.com/cofin/vertebra/issues/59)), two core gaps remain while `tools/benchmarks` strict compliance is partially landed:
1. `pyproject.toml` (`[[tool.mypy.overrides]]`) still suppresses missing imports across `litestar*`, `sqlalchemy*`, `advanced_alchemy*`, `alembic*`, `sqlspec*`, `django*`, `starlette*`, `fastapi*`, `flask*`, `pydantic*`, `attrs`, `uvloop`, and `pytest_databases*` (`52` patterns total), all of which ship PEP 561 `py.typed` markers (or `django-stubs` in `[dependency-groups].lint`) [^pyproject-mypy]. Removing those `43+` redundant entries immediately surfaces a broken `from litestar_queues.records import QueuedTaskRecord` import in `src/py/vertebra/extensions/litestar/queues/backend.py:23` [^litestar-queues-backend].
2. `tools/benchmarks` was registered in `[tool.mypy].files` and `Makefile` `lint` / `lint-all` (`334dc98d`), and `ServerRunner._process: subprocess.Popen[bytes] | None`, `tools/benchmarks/apps/asgi.py`, and `tools/benchmarks/apps/spikard_app.py` now pass `mypy --strict tools/benchmarks` and `ruff check tools/benchmarks`; only the `ServerRunner` UTF-8/non-UTF-8 stderr regression test in `test_bench_infrastructure.py` (`t02`) remains [^makefile-lint] [^bench-runner] [^bench-asgi] [^bench-spikard].
3. `src/py/tests/unit/**` and `src/py/tests/integration/**` are excluded from `mypy` in `Makefile` and `pyproject.toml`, allowing un-narrowed `import_optional(...)` (`Any | None`) calls, untyped `object()` broker stubs, and `@final` `SyncSpannerClient` / `AsyncSpannerClient` test-double mismatches [^spanner-driver]. Relevant OKF: [tech-stack.md](../../product/tech-stack.md), [polyglot-test-matrix.md](../../knowledge/testing/polyglot-test-matrix.md), and [workflow.md](../../knowledge/workflow.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R1 (Minimal `ignore_missing_imports` Surface & Masked Import Fix)**: Remove all PEP 561 / `django-stubs`-typed packages from `[[tool.mypy.overrides]]` in `pyproject.toml`, retaining strictly `pyarrow`, `pyarrow.*`, `polars`, `polars.*`, `numpy`, `numpy.*`, `rloop`, `winloop`, `fastembed`, `fastembed.*`, `datafusion`, `datafusion.*`, `wasmtime`, `wasmtime.*`, `zstandard`, `zstandard.*`, `spikard`, `spikard.*`, and `concurrent.interpreters` (and keeping `duckdb` out unless optional). Remove/replace the non-existent `litestar_queues.records` import in `src/py/vertebra/extensions/litestar/queues/backend.py` (or its successor after `queues-tasks-and-channels`) with zero `# type: ignore` comments (`tools/benchmarks` is already registered in `[tool.mypy].files` and `Makefile` at `334dc98d`).
- **R2 (`tools/benchmarks` Strict Compliance & `ServerRunner` Regression Test)**: `ServerRunner._process: subprocess.Popen[bytes] | None`, `tools/benchmarks/apps/asgi.py`, and `tools/benchmarks/apps/spikard_app.py` already pass `mypy --strict tools/benchmarks` and `ruff check tools/benchmarks` (`334dc98d`); keep those invariants and add the `ServerRunner` UTF-8/non-UTF-8 binary stderr regression test in `src/py/tests/unit/test_bench_infrastructure.py`.
- **R3 (Test-Suite `mypy` Gate in `Makefile` & Structural Spanner Client Protocols)**: Define `SyncSpannerClientProtocol` and `AsyncSpannerClientProtocol` (`typing.Protocol`) in `src/py/vertebra/db/adapters/spanner/driver.py` for `SyncSpannerDriver(client=...)` / `AsyncSpannerDriver(client=...)`, narrow `import_optional(...)` returns and fixture/stub annotations across `src/py/tests/unit/**` and `src/py/tests/integration/**` (zero `# type: ignore` additions), and add `src/py/tests/unit` and `src/py/tests/integration` to `[tool.mypy].files` in `pyproject.toml` and `Makefile` (`lint` and `lint-all`).
- **R4 (OKF & Product Tech-Stack Sync `<= 200L`)**: Update `.agents/bundles/product/tech-stack.md` (`Mypy Overrides` and `[tool.mypy].files`, `<= 200L`), synchronize `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` (already `196L <= 200L`), and synchronize `.agents/bundles/knowledge/workflow.md` (`<= 200L`).

## 3. Documentation & OKF Knowledge Plan

- `.agents/bundles/product/tech-stack.md` (`<= 200L`), `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` (`<= 200L`), and `.agents/bundles/knowledge/workflow.md` (`<= 200L`) in `t04`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1` (Prune `43+` `ignore_missing_imports`, fix `litestar_queues` import; `tools/benchmarks` paths landed in `334dc98d`) | `t01` | `pyproject.toml`, `src/py/vertebra/extensions/litestar/queues/backend.py` | `uv run --no-sync mypy src/py/vertebra` & `pytest test_litestar_ecosystem_queues.py` |
| `R2` (Benchmark `Popen[bytes]` / strict typing landed in `334dc98d`; add `ServerRunner` stderr regression test) | `t02` | `src/py/tests/unit/test_bench_infrastructure.py` (`tools/benchmarks/{runner.py,apps/*}` verified) | `uv run --no-sync mypy --strict tools/benchmarks` & `pytest test_bench_infrastructure.py` |
| `R3` (Type-check `src/py/tests/{unit,integration}/**` in `Makefile` `lint`/`lint-all`) | `t03` | `pyproject.toml`, `Makefile`, `src/py/vertebra/db/adapters/spanner/driver.py` | `uv run --no-sync mypy src/py/tests/unit src/py/tests/integration` |
| `R4` (Sync `tech-stack.md`, `polyglot-test-matrix.md <= 200L`, and `workflow.md <= 200L`) | `t04` | `.agents/bundles/{product/tech-stack,knowledge/testing/polyglot-test-matrix,knowledge/workflow}.md` | `make review-check` |

## 5. Implementation Plan

- [ ] [t01-prune-mypy-overrides-and-fix-masked-imports.md](tasks/t01-prune-mypy-overrides-and-fix-masked-imports.md) — Prune `43+` redundant `ignore_missing_imports` entries in `pyproject.toml` and fix masked broken imports in `extensions/litestar/queues/backend.py` (`tools/benchmarks` paths already registered in `334dc98d`).
- [ ] [t02-strict-type-benchmarks-and-runner-fix.md](tasks/t02-strict-type-benchmarks-and-runner-fix.md) — Add `ServerRunner` binary-stderr regression test in `test_bench_infrastructure.py` (benchmark source fixes already landed in `334dc98d`).
- [ ] [t03-strict-type-unit-and-integration-test-suites.md](tasks/t03-strict-type-unit-and-integration-test-suites.md) — Add Spanner client `Protocol`s and bring `src/py/tests/unit/**` and `src/py/tests/integration/**` under `mypy` verification in `pyproject.toml` and `Makefile` (`lint` / `lint-all`).
- [ ] [t04-sync-typing-docs-and-okf.md](tasks/t04-sync-typing-docs-and-okf.md) — Synchronize `product/tech-stack.md` (`<= 200L`), `knowledge/testing/polyglot-test-matrix.md` (`<= 200L`), and `knowledge/workflow.md` (`<= 200L`).

## 6. Current State & Remaining Plan Snapshot

- **Active Frontier**: `state: planned` (`t01` and `t02` partial at `334dc98d`: `tools/benchmarks` paths, `ServerRunner` `Popen[bytes]`, `asgi.py`, and `spikard_app.py` typing fixes landed; remaining `t01`/`t02`/`t03`/`t04` gated on `queues-tasks-and-channels`, `sqlspec-adapter-parity`, and `sqlalchemy-and-django-integration`).
- **Release Gate (`/flow:finish`)**: `make lint-all && make py-test && make review-check`.

[^pyproject-mypy]: `pyproject.toml` — `[tool.mypy]` `files` and 52-pattern `ignore_missing_imports` override block.
[^makefile-lint]: `Makefile` — `lint` and `lint-all` Ruff and Mypy invocations.
[^litestar-queues-backend]: `src/py/vertebra/extensions/litestar/queues/backend.py::SHMQueueBackend` — Masked broken `litestar_queues.records` import under `TYPE_CHECKING`.
[^bench-runner]: `tools/benchmarks/runner.py::ServerRunner` — `ServerRunner` binary `Popen[bytes]` and `.decode()` implementation (`t02` code landed at `334dc98d`).
[^bench-asgi]: `tools/benchmarks/apps/asgi.py` — Benchmark ASGI app JSON encoding helper (`t02` code landed at `334dc98d`).
[^bench-spikard]: `tools/benchmarks/apps/spikard_app.py` — Benchmark Spikard app typed route wrappers (`t02` code landed at `334dc98d`).
[^spanner-driver]: `src/py/vertebra/db/adapters/spanner/driver.py::SyncSpannerDriver` — `SyncSpannerDriver` and `AsyncSpannerDriver` `@final` client parameter annotations.
[^okf-tech-stack]: `.agents/bundles/product/tech-stack.md` — Technology stack `Mypy Overrides` and verification toolchain documentation.
[^okf-test-matrix]: `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — Polyglot test matrix OKF chapter (`196` lines on `HEAD`).
[^okf-workflow]: `.agents/bundles/knowledge/workflow.md` — Project workflow `Makefile` lint and type-check commands.
