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
- **Baseline (`18baadcd`)**: `pyproject.toml` carries `50` `ignore_missing_imports = true` patterns — `43+` of which suppress installed PEP 561 / `django-stubs` packages and mask a broken `litestar_queues.records` import in `extensions/litestar/queues/backend.py` [^pyproject-mypy] [^litestar-queues-backend]; `tools/benchmarks` is excluded from Ruff and Mypy (`16` strict errors including a `Popen[bytes]` / `str.decode` bug in `runner.py:170`) [^bench-runner]; and `src/py/tests/{unit,integration}` is unchecked by `mypy` in `Makefile` ([#59](https://github.com/cofin/vertebra/issues/59)) [^makefile-lint].
- **Target Architecture**: `[tool.mypy.overrides]` retains only genuinely untyped C or uninstalled optional packages (`pyarrow*`, `polars*`, `numpy*`, `rloop`, `winloop`, `fastembed*`, `datafusion*`, `wasmtime*`, `zstandard*`, `concurrent.interpreters`); `tools/benchmarks/**`, `src/py/tests/unit/**`, and `src/py/tests/integration/**` pass `mypy` and Ruff in `Makefile` (`lint` / `lint-all`); `SpannerDriver` accepts structural client `Protocol`s for typed test doubles [^spanner-driver]; and `tech-stack.md`, `polyglot-test-matrix.md` (`243L -> <= 200L`), and `workflow.md` stay `<= 200` lines [^okf-tech-stack] [^okf-test-matrix] [^okf-workflow].
- **Scope & Boundaries**: Chapterless flow (`4` compact tasks) under [integration-remediation](../integration-remediation/spec.md), sequenced after `queues-tasks-and-channels`, `sqlspec-adapter-parity`, and `sqlalchemy-and-django-integration` so extension and test files are edited once.
<!-- truth: end -->

## 1. Specification, Context & Provenance

At baseline `18baadcd` ([#59](https://github.com/cofin/vertebra/issues/59)), three gaps weaken `mypy --strict` coverage across the Python codebase:
1. `pyproject.toml` (`[[tool.mypy.overrides]]`) suppresses missing imports across `litestar*`, `sqlalchemy*`, `advanced_alchemy*`, `alembic*`, `sqlspec*`, `django*`, `starlette*`, `fastapi*`, `flask*`, `pydantic*`, `attrs`, `uvloop`, and `pytest_databases*`, all of which ship PEP 561 `py.typed` markers (or `django-stubs` in `[dependency-groups].lint`) [^pyproject-mypy]. Removing those `43+` redundant entries immediately surfaces a broken `from litestar_queues.records import QueuedTaskRecord` import in `src/py/vertebra/extensions/litestar/queues/backend.py:23` [^litestar-queues-backend].
2. `tools/benchmarks` is omitted from `[tool.mypy].files` and from `Makefile` `lint` / `lint-all` Ruff and Mypy commands [^makefile-lint], hiding `16` strict errors across `tools/benchmarks/runner.py` (`self._process: subprocess.Popen[str] | None` assigned binary `Popen[bytes]` at line `149` and calling `.read().decode(...)` on `str` at line `170`), `tools/benchmarks/apps/asgi.py`, and `tools/benchmarks/apps/spikard_app.py` [^bench-runner] [^bench-asgi] [^bench-spikard].
3. `src/py/tests/unit/**` and `src/py/tests/integration/**` are excluded from `mypy` in `Makefile` and `pyproject.toml`, allowing un-narrowed `import_optional(...)` (`Any | None`) calls, untyped `object()` broker stubs, and `@final` `SyncSpannerClient` / `AsyncSpannerClient` test-double mismatches [^spanner-driver]. Relevant OKF: [tech-stack.md](../../product/tech-stack.md), [polyglot-test-matrix.md](../../knowledge/testing/polyglot-test-matrix.md), and [workflow.md](../../knowledge/workflow.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R1 (Minimal `ignore_missing_imports` Surface & Masked Import Fix)**: Remove all PEP 561 / `django-stubs`-typed packages from `[[tool.mypy.overrides]]` in `pyproject.toml`, retaining strictly `pyarrow`, `pyarrow.*`, `polars`, `polars.*`, `numpy`, `numpy.*`, `rloop`, `winloop`, `fastembed`, `fastembed.*`, `datafusion`, `datafusion.*`, `wasmtime`, `wasmtime.*`, `zstandard`, `zstandard.*`, `spikard`, `spikard.*`, and `concurrent.interpreters`. Remove/replace the non-existent `litestar_queues.records` import in `src/py/vertebra/extensions/litestar/queues/backend.py` (or its successor after `queues-tasks-and-channels`) with zero `# type: ignore` comments, and add `tools/benchmarks` to `[tool.mypy].files` and `Makefile` Ruff/Mypy paths.
- **R2 (`tools/benchmarks` Strict Compliance & `ServerRunner` Fix)**: Annotate `ServerRunner._process: subprocess.Popen[bytes] | None = None` in `tools/benchmarks/runner.py` so binary `stderr=subprocess.PIPE` `.read().decode("utf-8", errors="replace")` is both runtime-safe and type-sound; type `tools/benchmarks/apps/asgi.py` (`encode_json_bytes` -> `bytes`) and `tools/benchmarks/apps/spikard_app.py` so `uv run --no-sync mypy --strict tools/benchmarks` and `ruff check tools/benchmarks` exit `0` with `0` errors.
- **R3 (Test-Suite `mypy` Gate in `Makefile` & Structural Spanner Client Protocols)**: Define `SyncSpannerClientProtocol` and `AsyncSpannerClientProtocol` (`typing.Protocol`) in `src/py/vertebra/db/adapters/spanner/driver.py` for `SyncSpannerDriver(client=...)` / `AsyncSpannerDriver(client=...)`, narrow `import_optional(...)` returns and fixture/stub annotations across `src/py/tests/unit/**` and `src/py/tests/integration/**` (zero `# type: ignore` additions), and add `src/py/tests/unit` and `src/py/tests/integration` to `[tool.mypy].files` in `pyproject.toml` and `Makefile` (`lint` and `lint-all`).
- **R4 (OKF & Product Tech-Stack Sync `<= 200L`)**: Update `.agents/bundles/product/tech-stack.md` (`Mypy Overrides` and `[tool.mypy].files`, `<= 200L`), trim and synchronize `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` (`243L -> <= 200L`), and synchronize `.agents/bundles/knowledge/workflow.md` (`<= 200L`).

## 3. Documentation & OKF Knowledge Plan

- `.agents/bundles/product/tech-stack.md` (`<= 200L`), `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` (`243L -> <= 200L`), and `.agents/bundles/knowledge/workflow.md` (`<= 200L`) in `t04`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1` (Prune `43+` `ignore_missing_imports`, fix `litestar_queues` import, add `tools/benchmarks` paths) | `t01` | `pyproject.toml`, `Makefile`, `src/py/vertebra/extensions/litestar/queues/backend.py` | `uv run --no-sync mypy src/py/vertebra` & `pytest test_litestar_ecosystem_queues.py` |
| `R2` (`runner.py:170` `Popen[bytes]` fix & `100%` `mypy --strict` on `tools/benchmarks/**`) | `t02` | `tools/benchmarks/{runner.py,apps/asgi.py,apps/spikard_app.py}` | `uv run --no-sync mypy --strict tools/benchmarks` & `pytest test_bench_infrastructure.py` |
| `R3` (Type-check `src/py/tests/{unit,integration}/**` in `Makefile` `lint`/`lint-all`) | `t03` | `pyproject.toml`, `Makefile`, `src/py/vertebra/db/adapters/spanner/driver.py` | `uv run --no-sync mypy src/py/tests/unit src/py/tests/integration` |
| `R4` (Sync `tech-stack.md`, `polyglot-test-matrix.md <= 200L`, and `workflow.md <= 200L`) | `t04` | `.agents/bundles/{product/tech-stack,knowledge/testing/polyglot-test-matrix,knowledge/workflow}.md` | `make review-check` |

## 5. Implementation Plan

- [ ] [t01-prune-mypy-overrides-and-fix-masked-imports.md](tasks/t01-prune-mypy-overrides-and-fix-masked-imports.md) — Prune `43+` redundant `ignore_missing_imports` entries in `pyproject.toml`, fix masked broken imports in `extensions/litestar/queues/backend.py`, and add `tools/benchmarks` to `[tool.mypy].files` and `Makefile` Ruff/Mypy paths.
- [ ] [t02-strict-type-benchmarks-and-runner-fix.md](tasks/t02-strict-type-benchmarks-and-runner-fix.md) — Fix `tools/benchmarks/runner.py` `Popen[bytes]` / `str.decode` mismatch and bring `tools/benchmarks/**` to `100%` `mypy --strict` compliance.
- [ ] [t03-strict-type-unit-and-integration-test-suites.md](tasks/t03-strict-type-unit-and-integration-test-suites.md) — Add Spanner client `Protocol`s and bring `src/py/tests/unit/**` and `src/py/tests/integration/**` under `mypy` verification in `pyproject.toml` and `Makefile` (`lint` / `lint-all`).
- [ ] [t04-sync-typing-docs-and-okf.md](tasks/t04-sync-typing-docs-and-okf.md) — Synchronize `product/tech-stack.md` (`<= 200L`), `knowledge/testing/polyglot-test-matrix.md` (`243L -> <= 200L`), and `knowledge/workflow.md` (`<= 200L`).

## 6. Current State & Remaining Plan Snapshot

- **Active Frontier**: `state: planned` at `18baadcd`, gated on `queues-tasks-and-channels`, `sqlspec-adapter-parity`, and `sqlalchemy-and-django-integration`; next action `/flow:implement strict-typing-hygiene` starting at `strict-typing-hygiene:t01` once upstream flows merge.
- **Release Gate (`/flow:finish`)**: `make lint-all && make py-test && make review-check`.

[^pyproject-mypy]: `pyproject.toml` — `[tool.mypy]` `files` and 50-pattern `ignore_missing_imports` override block.
[^makefile-lint]: `Makefile` — `lint` and `lint-all` Ruff and Mypy invocations.
[^litestar-queues-backend]: `src/py/vertebra/extensions/litestar/queues/backend.py::SHMQueueBackend` — Masked broken `litestar_queues.records` import under `TYPE_CHECKING`.
[^bench-runner]: `tools/benchmarks/runner.py::ServerRunner` — `ServerRunner` `Popen[str]` annotation mismatch and `str.decode` bug at line 170.
[^bench-asgi]: `tools/benchmarks/apps/asgi.py` — Benchmark ASGI app `Any` return annotation drift.
[^bench-spikard]: `tools/benchmarks/apps/spikard_app.py` — Benchmark Spikard app untyped decorators and `Response` argument mismatch.
[^spanner-driver]: `src/py/vertebra/db/adapters/spanner/driver.py::SyncSpannerDriver` — `SyncSpannerDriver` and `AsyncSpannerDriver` `@final` client parameter annotations.
[^okf-tech-stack]: `.agents/bundles/product/tech-stack.md` — Technology stack `Mypy Overrides` and verification toolchain documentation.
[^okf-test-matrix]: `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — Polyglot test matrix OKF chapter (currently 243 lines).
[^okf-workflow]: `.agents/bundles/knowledge/workflow.md` — Project workflow `Makefile` lint and type-check commands.
