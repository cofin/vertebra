---
type: Spec
flow_id: strict-typing-hygiene
title: Strict Python Typing Hygiene Across Overrides, Benchmarks, and Test Suites
description: Prunes redundant ignore_missing_imports overrides in pyproject.toml, fixes masked broken imports and tools/benchmarks subprocess typing bugs, enables mypy verification across unit and integration test suites, and synchronizes typing OKF chapters.
status: stable
state: planned
planning_quality: standard
planning_degraded_reason: null
has_chapters: false
current_task: "strict-typing-hygiene:t01"
parent_prd: integration-remediation
depends_on_flows: []
plan_revision: 2
plan_commit: 473e1f2e
created_at: '2026-11-12T23:58:00Z'
updated_at: '2026-11-15T23:30:00Z'
generated:
  by: flow-plan
  at: '2026-11-12T23:58:00Z'
verified:
  - by: agent:spec-sync
    at: '2026-11-15T23:30:00Z'
    commit: 473e1f2e
  - by: agent:plan-generator
    at: '2026-11-12T23:58:00Z'
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
    title: '[tool.mypy] files and 52-pattern ignore_missing_imports override block'
  - id: makefile-lint
    resource: Makefile
    title: lint and lint-all Ruff and Mypy invocations
  - id: litestar-queues-backend
    resource: src/py/vertebra/extensions/litestar/queues/backend.py::SHMQueueBackend
    title: Masked broken litestar_queues.records import under TYPE_CHECKING
  - id: bench-runner
    resource: tools/benchmarks/runner.py::ServerRunner
    title: ServerRunner Popen[bytes] and decode(errors="replace") stderr handling (lines 135, 170)
  - id: bench-asgi
    resource: tools/benchmarks/apps/asgi.py
    title: Benchmark ASGI app typed send/receive helpers
  - id: bench-spikard
    resource: tools/benchmarks/apps/spikard_app.py
    title: Benchmark Spikard app typed route wrappers
  - id: spanner-driver
    resource: src/py/vertebra/db/adapters/spanner/_sync.py::SyncSpannerDriver
    title: SyncSpannerDriver and _async.py::AsyncSpannerDriver @final client parameter annotations
  - id: okf-tech-stack
    resource: .agents/bundles/product/tech-stack.md
    title: Technology stack Mypy Overrides and verification toolchain documentation
  - id: okf-test-matrix
    resource: .agents/bundles/knowledge/testing/polyglot-test-matrix.md
    title: Polyglot test matrix OKF chapter (198 lines on HEAD)
  - id: okf-workflow
    resource: .agents/bundles/knowledge/workflow.md
    title: Project workflow Makefile lint and type-check commands
---

# Specification: Strict Python Typing Hygiene Across Overrides, Benchmarks, and Test Suites

<!-- truth: start -->
- **Baseline (`473e1f2e`)**: `pyproject.toml` carries `52` `ignore_missing_imports = true` patterns — `40` of which suppress installed PEP 561 / `django-stubs` packages (`duckdb`, `litestar*`, `sqlalchemy*`, `sqlspec*`, `django*`, etc.) and mask broken `litestar_queues.records` imports in `extensions/litestar/queues/backend.py` [^pyproject-mypy] [^litestar-queues-backend]; `tools/benchmarks` paths are registered in `pyproject.toml`/`Makefile` (`ServerRunner._process: subprocess.Popen[bytes] | None`, `asgi.py`, and `spikard_app.py` pass strict `mypy`/`ruff`, with `t02` regression test pending in `test_bench_infrastructure.py`) [^bench-runner]; and `src/py/tests/{unit,integration}` remains unchecked by `mypy` in `Makefile` ([#59](https://github.com/cofin/vertebra/issues/59)) [^makefile-lint].
- **Target Architecture**: `[tool.mypy.overrides]` retains only untyped, platform-conditional, or optional-extra modules (`pyarrow*`, `polars*`, `numpy*`, `rloop`, `winloop`, `zuvloop`, `spikard*`, `concurrent.interpreters`); `tools/benchmarks/**`, `src/py/tests/unit/**`, and `src/py/tests/integration/**` pass `mypy` and Ruff in `Makefile` (`lint` / `lint-all`); `SyncSpannerDriver` (`_sync.py`) and `AsyncSpannerDriver` (`_async.py`) accept structural client `Protocol`s for typed test doubles [^spanner-driver]; and `tech-stack.md`, `polyglot-test-matrix.md`, and `workflow.md` stay `<= 200` lines [^okf-tech-stack] [^okf-test-matrix] [^okf-workflow].
- **Scope & Boundaries**: Chapterless flow (`4` compact tasks) under [integration-remediation](../integration-remediation/spec.md), sequenced after `queues-tasks-and-channels`, `sqlspec-adapter-parity`, `sqlalchemy-and-django-integration`, and `spanner-queue-and-rw-transactions` (which adds `begin_read_write_transaction` to the Spanner clients the new `Protocol`s mirror) so extension and test files are edited once.
<!-- truth: end -->

## 1. Specification, Context & Provenance

At `HEAD @ 473e1f2e` ([#59](https://github.com/cofin/vertebra/issues/59)), three hygiene gaps remain across typing overrides, benchmark regression tests, and test-suite verification:
1. `pyproject.toml` (`[[tool.mypy.overrides]]`, `52` patterns total) suppresses missing imports across `pydantic.*`, `attrs`, `uvloop`, `starlette*`, `fastapi*`, `flask*`, `litestar*`, `litestar_queues*`, `litestar_security*`, `litestar_autowire*`, `litestar_email*`, `litestar_mcp*`, `litestar_vite*`, `litestar_asyncapi*`, `litestar_htmx*`, `advanced_alchemy*`, `sqlalchemy*`, `alembic*`, `sqlspec*`, `django*`, `pytest_databases*`, and `duckdb`, all of which ship PEP 561 `py.typed` markers (or `django-stubs` in `[dependency-groups].lint`) [^pyproject-mypy]. Removing those redundant entries surfaces any broken `litestar_queues.records` import in `src/py/vertebra/extensions/litestar/queues/backend.py` [^litestar-queues-backend].
2. `tools/benchmarks` is registered in `[tool.mypy].files` and `Makefile` `lint` / `lint-all`, and `ServerRunner._process: subprocess.Popen[bytes] | None`, `tools/benchmarks/apps/asgi.py`, and `tools/benchmarks/apps/spikard_app.py` pass `mypy --strict tools/benchmarks` and `ruff check tools/benchmarks`; the `ServerRunner` UTF-8/non-UTF-8 binary stderr regression test in `test_bench_infrastructure.py` (`t02`) remains [^makefile-lint] [^bench-runner] [^bench-asgi] [^bench-spikard].
3. `src/py/tests/unit/**` and `src/py/tests/integration/**` are excluded from `mypy` in `Makefile` and `pyproject.toml`, allowing un-narrowed `import_optional(...)` (`Any | None`) calls, untyped `object()` broker stubs, and `@final` `SyncSpannerClient` / `AsyncSpannerClient` test-double mismatches in `_sync.py` and `_async.py` [^spanner-driver]. Relevant OKF: [tech-stack.md](../../product/tech-stack.md), [polyglot-test-matrix.md](../../knowledge/testing/polyglot-test-matrix.md), and [workflow.md](../../knowledge/workflow.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R1 (Minimal `ignore_missing_imports` Surface & Masked Import Fix)**: Remove all `40` PEP 561 / `django-stubs`-typed packages (`pydantic.*`, `attrs`, `uvloop`, `starlette*`, `fastapi*`, `flask*`, `litestar*`, `litestar_queues*`, `litestar_security*`, `litestar_autowire*`, `litestar_email*`, `litestar_mcp*`, `litestar_vite*`, `litestar_asyncapi*`, `litestar_htmx*`, `advanced_alchemy*`, `sqlalchemy*`, `alembic*`, `sqlspec*`, `django*`, `pytest_databases*`, `duckdb`) from `[[tool.mypy.overrides]]` in `pyproject.toml`, retaining strictly genuinely untyped C or uninstalled optional modules (`pyarrow`, `pyarrow.*`, `polars`, `polars.*`, `numpy`, `numpy.*`, `rloop`, `winloop`, `zuvloop`, `spikard`, `spikard.*`, `concurrent.interpreters`). Ensure zero broken `litestar_queues.records` imports remain across `src/py/vertebra/extensions/litestar/queues/` with zero `# type: ignore` suppressions, and verify `tools/benchmarks` stays registered in `[tool.mypy].files` and `Makefile`.
- **R2 (`tools/benchmarks` Strict Compliance & `ServerRunner` Regression Test)**: Keep `ServerRunner._process: subprocess.Popen[bytes] | None`, `tools/benchmarks/apps/asgi.py`, and `tools/benchmarks/apps/spikard_app.py` `mypy --strict` and Ruff clean, and add the `ServerRunner` UTF-8/non-UTF-8 binary stderr regression test in `src/py/tests/unit/test_bench_infrastructure.py`.
- **R3 (Test-Suite `mypy` Gate in `Makefile` & Structural Spanner Client Protocols)**: Define `SyncSpannerClientProtocol` (`src/py/vertebra/db/adapters/spanner/_sync.py`) and `AsyncSpannerClientProtocol` (`src/py/vertebra/db/adapters/spanner/_async.py`, or shared in `spanner/core.py`) matching `vertebra._spanner` client methods for `SyncSpannerDriver(client=...)` / `AsyncSpannerDriver(client=...)`, narrow `import_optional(...)` returns and fixture/stub annotations across `src/py/tests/unit/**` and `src/py/tests/integration/**` (zero `# type: ignore` additions), and add `src/py/tests/unit` and `src/py/tests/integration` to `[tool.mypy].files` in `pyproject.toml` and `Makefile` (`lint` and `lint-all`).
- **R4 (OKF & Product Tech-Stack Sync `<= 200L`)**: Update `.agents/bundles/product/tech-stack.md` (`Mypy Overrides` and `[tool.mypy].files`, `<= 200L`), synchronize `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` (`198L` -> `<= 200L`), and synchronize `.agents/bundles/knowledge/workflow.md` (`<= 200L`).

## 3. Documentation & OKF Knowledge Plan

- `.agents/bundles/product/tech-stack.md` (`<= 200L`), `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` (`<= 200L`), and `.agents/bundles/knowledge/workflow.md` (`<= 200L`) in `t04`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1` (Prune `40` `ignore_missing_imports`, verify `litestar/queues/` imports & `tools/benchmarks` paths) | `t01` | `pyproject.toml`, `src/py/vertebra/extensions/litestar/queues/backend.py` | `uv run --no-sync mypy src/py/vertebra` & `pytest test_litestar_ecosystem_queues.py` |
| `R2` (Benchmark `Popen[bytes]` / strict typing verified; add `ServerRunner` stderr regression test) | `t02` | `src/py/tests/unit/test_bench_infrastructure.py` (`tools/benchmarks/{runner.py,apps/*}` verified) | `uv run --no-sync mypy --strict tools/benchmarks` & `pytest test_bench_infrastructure.py` |
| `R3` (Type-check `src/py/tests/{unit,integration}/**` in `Makefile` `lint`/`lint-all`) | `t03` | `pyproject.toml`, `Makefile`, `src/py/vertebra/db/adapters/spanner/{_sync,_async}.py` | `uv run --no-sync mypy src/py/tests/unit src/py/tests/integration` |
| `R4` (Sync `tech-stack.md`, `polyglot-test-matrix.md <= 200L`, and `workflow.md <= 200L`) | `t04` | `.agents/bundles/{product/tech-stack,knowledge/testing/polyglot-test-matrix,knowledge/workflow}.md` | `make review-check` |

## 5. Implementation Plan

- [ ] [t01-prune-mypy-overrides-and-fix-masked-imports.md](tasks/t01-prune-mypy-overrides-and-fix-masked-imports.md) — Prune `40` redundant `ignore_missing_imports` entries in `pyproject.toml` and verify zero masked broken imports in `extensions/litestar/queues/`.
- [ ] [t02-strict-type-benchmarks-and-runner-fix.md](tasks/t02-strict-type-benchmarks-and-runner-fix.md) — Add `ServerRunner` binary-stderr regression test in `test_bench_infrastructure.py` and keep `tools/benchmarks/**` strictly typed.
- [ ] [t03-strict-type-unit-and-integration-test-suites.md](tasks/t03-strict-type-unit-and-integration-test-suites.md) — Add Spanner client `Protocol`s in `_sync.py`/`_async.py` and bring `src/py/tests/unit/**` and `src/py/tests/integration/**` under `mypy` verification in `pyproject.toml` and `Makefile` (`lint` / `lint-all`).
- [ ] [t04-sync-typing-docs-and-okf.md](tasks/t04-sync-typing-docs-and-okf.md) — Synchronize `product/tech-stack.md` (`<= 200L`), `knowledge/testing/polyglot-test-matrix.md` (`<= 200L`), and `knowledge/workflow.md` (`<= 200L`).

## 6. Current State & Remaining Plan Snapshot

- **Active Frontier**: `state: planned` (`tools/benchmarks` paths, `ServerRunner` `Popen[bytes]`, `asgi.py`, and `spikard_app.py` typing fixes landed; `t01`–`t04` gated on `queues-tasks-and-channels`, `sqlspec-adapter-parity`, and `sqlalchemy-and-django-integration`).
- **Release Gate (`/flow:finish`)**: `make lint-all && make py-test && make review-check`.

[^pyproject-mypy]: `pyproject.toml` — `[tool.mypy]` `files` and 52-pattern `ignore_missing_imports` override block.
[^makefile-lint]: `Makefile` — `lint` and `lint-all` Ruff and Mypy invocations.
[^litestar-queues-backend]: `src/py/vertebra/extensions/litestar/queues/backend.py::SHMQueueBackend` — Masked broken `litestar_queues.records` import under `TYPE_CHECKING`.
[^bench-runner]: `tools/benchmarks/runner.py::ServerRunner` — `ServerRunner` binary `Popen[bytes]` and `.decode()` implementation.
[^bench-asgi]: `tools/benchmarks/apps/asgi.py` — Benchmark ASGI app JSON encoding helper.
[^bench-spikard]: `tools/benchmarks/apps/spikard_app.py` — Benchmark Spikard app typed route wrappers.
[^spanner-driver]: `src/py/vertebra/db/adapters/spanner/_sync.py::SyncSpannerDriver` — `SyncSpannerDriver` (`_sync.py`) and `AsyncSpannerDriver` (`_async.py`) `@final` client parameter annotations.
[^okf-tech-stack]: `.agents/bundles/product/tech-stack.md` — Technology stack `Mypy Overrides` and verification toolchain documentation.
[^okf-test-matrix]: `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — Polyglot test matrix OKF chapter (`198` lines on `HEAD`).
[^okf-workflow]: `.agents/bundles/knowledge/workflow.md` — Project workflow `Makefile` lint and type-check commands.
