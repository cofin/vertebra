---
type: Spec
flow_id: driver-fail-loud
parent_flow: driver-fail-loud
chapter_id: ch09-conformance-outcomes
title: "Chapter 09: Conformance Outcomes for Missing Native Modules"
description: Make driver conformance skip a case whose native module is absent, fail it under Docker conformance only for non-pending engines, apply strict xfail to pending engines only when their module imports, and drop the integration conftest future import and unused Oracle and MSSQL driver fixtures.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch04-redis-valkey-memory-removal
replan_trigger: null
current_task: "driver-fail-loud:ch09:t01"
plan_revision: 1
plan_commit: 1e1f028a
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-08T00:00:00Z"
updated_at: "2026-11-08T00:00:00Z"
generated:
  by: flow/0.28.0
  at: "2026-11-08T00:00:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-08T00:00:00Z"
    commit: 0acdf576
sources:
  - id: conformance
    resource: src/py/tests/integration/db/test_driver_conformance.py::_resolve_live_config
    title: Conformance live config resolution
  - id: conformance-params
    resource: src/py/tests/integration/db/test_driver_conformance.py::_build_live_engine_params
    title: Pending-engine strict xfail parameter builder
  - id: integration-conftest
    resource: src/py/tests/integration/conftest.py
    title: pytest-databases Docker fixtures for integration tests
  - id: module-loader
    resource: src/py/vertebra/utils/module_loader.py::import_optional
    title: Optional module import returning None when unavailable
  - id: missing-dep
    resource: src/py/vertebra/_typing.py::MissingDependencyError
    title: Missing dependency error raised by ensure_module
tags:
  - conformance
  - testing
  - pytest
---

# Chapter 09: Conformance Outcomes for Missing Native Modules

<!-- truth: start -->
- Conformance resolves the native module before any credential skip.[^conformance]
- A missing native module skips the case in local and `VERTEBRA_CONFORMANCE_DOCKER=1` runs alike when the spec has `pending_flow_id` set.[^conformance]
- A missing native module skips a non-pending case (`bigquery`, `spanner`) locally and fails it under `VERTEBRA_CONFORMANCE_DOCKER=1`.[^conformance]
- Pending engines carry strict `xfail` only when their native module imports, so a missing-module skip or fail is never converted to `xfailed`.[^conformance-params]
- `src/py/tests/integration/conftest.py` has no `from __future__ import annotations`; `TYPE_CHECKING`-only service types are quoted; the unused `vtb_oracle_driver` and `vtb_mssql_driver` fixtures are gone.[^integration-conftest]
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

The conformance suite calls `ensure_module` for each spec's native module at the top of `_resolve_live_config` and lets `MissingDependencyError` escape.[^conformance][^missing-dep] Non-pending engines (BigQuery, Spanner) report a setup ERROR; pending engines report `xfailed` under their strict `xfail` mark for the wrong reason.[^conformance-params] `src/py/tests/integration/conftest.py` keeps `vtb_oracle_driver` and `vtb_mssql_driver` fixtures that no test requests and that construct pure-Python drivers ch01 makes fail loud.[^integration-conftest]

This chapter maps a missing native module to an explicit skip, or to an explicit failure in Docker conformance mode for engines that are not pending a native flow, and removes the dead conftest fixtures.

**Non-goals**: install hints and the extras gate (owned by [ch08-install-hints-and-extras-gate](../ch08-install-hints-and-extras-gate/spec.md)); `test_engine_driver_resolution_and_provide_driver` Oracle/MSSQL assertions (owned by ch01 and ch02); `CACHE_SPECS` and cache fixtures (owned by ch04); removing `from __future__ import annotations` from `test_driver_conformance.py`; OKF and user docs for conformance outcomes (owned by [ch11-docs-and-okf](../ch11-docs-and-okf/spec.md)).

### 1.2 Upstream Execution Baseline

- Branch `feat/storage-updates` at `0acdf576`; parent roadmap [spec.md](../../spec.md); catalog [chapters/index.md](../index.md).
- Depends on [ch04-redis-valkey-memory-removal](../ch04-redis-valkey-memory-removal/spec.md), which reaches ch01, ch02, and ch03 transitively. ch01 `t01` / `t02`, ch02 `t04`, and ch04 `t01` each edit `test_driver_conformance.py` (driver-resolution assertions, DuckDB spec, cache helpers); `t01` edits `_resolve_live_config` and `_build_live_engine_params` in the same file afterward. ch01 makes the Oracle and MSSQL pure-Python drivers fail loud, which is why their conftest fixtures are deleted here.
- No file overlap with [ch08-install-hints-and-extras-gate](../ch08-install-hints-and-extras-gate/spec.md), ch05, ch06, ch07, or [ch10-queue-backend-fail-loud](../ch10-queue-backend-fail-loud/spec.md); this chapter runs in parallel with them.
- Research: [change-surface](../../research/change-surface_20261107.md) section "Conformance".

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Conformance | `src/py/tests/integration/db/test_driver_conformance.py::EngineConformanceSpec`, `ENGINE_SPECS`, `_build_live_engine_params`, `_resolve_live_config`, `sync_sql_driver`, `async_sql_driver`[^conformance][^conformance-params] | Test seam; fixtures parametrized by `_build_live_engine_params()` call `_resolve_live_config` first |
| Integration fixtures | `src/py/tests/integration/conftest.py`[^integration-conftest] | pytest-databases service fixtures annotated with `TYPE_CHECKING`-only types; `vtb_oracle_driver` and `vtb_mssql_driver` have no consumers under `src/py/tests` |
| Loader | `src/py/vertebra/utils/module_loader.py::import_optional`, `ensure_module`[^module-loader] | In-process; `import_optional` returns `None`, `ensure_module` raises `MissingDependencyError`[^missing-dep] |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Polyglot test matrix](../../../../knowledge/testing/polyglot-test-matrix.md)
- [Database drivers and dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md)
- [Project workflow](../../../../knowledge/workflow.md)

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R12 (`t01`)**: Conformance resolves the native module before credential skips; a case whose native module is missing skips when its spec has `pending_flow_id` set (local and `VERTEBRA_CONFORMANCE_DOCKER=1` alike), skips a non-pending spec locally, and fails a non-pending spec when `VERTEBRA_CONFORMANCE_DOCKER=1`; strict `xfail` applies only when a pending engine's module imports.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

Conformance helpers in `src/py/tests/integration/db/test_driver_conformance.py`:

```python
_CONFORMANCE_DOCKER_ENV = "VERTEBRA_CONFORMANCE_DOCKER"


def _build_live_engine_params(specs: Sequence[EngineConformanceSpec] = ENGINE_SPECS) -> list[Any]:
    """Build fixture parameters, marking pending engines strict ``xfail`` only when their native module imports."""


def _native_module_missing(spec: EngineConformanceSpec) -> bool:
    """Return True when ``spec`` names a native module that does not import."""


def _require_native_module(spec: EngineConformanceSpec) -> None:
    """Skip the case, or fail a non-pending case under Docker conformance, when the native module is missing.

    Raises:
        pytest.skip.Exception: When the module is missing and either the spec is pending or
            ``VERTEBRA_CONFORMANCE_DOCKER`` is not ``"1"``.
        pytest.fail.Exception: When the module is missing, the spec is not pending, and
            ``VERTEBRA_CONFORMANCE_DOCKER`` is ``"1"``.
    """
```

`_resolve_live_config(spec, tmp_path, request) -> BaseDatabaseConfig` keeps its signature; its first statement becomes `_require_native_module(spec)`.

### 2.3 Caller -> Seam -> Collaborator Integration Map

- pytest collection → `_build_live_engine_params()` → `_native_module_missing(spec)` → `import_optional(spec.native_extension_module)` → `pytest.param(spec, marks=...)`.[^conformance-params][^module-loader]
- `sync_sql_driver` / `async_sql_driver` → `_resolve_live_config(spec, tmp_path, request)` → `_require_native_module(spec)` → `pytest.skip(reason)`, or `pytest.fail(reason, pytrace=False)` when `spec.pending_flow_id is None` and `VERTEBRA_CONFORMANCE_DOCKER == "1"` → BigQuery / Spanner credential checks → env DSN → Docker fixture → `sample_url`.[^conformance]

### 2.4 Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Pending module missing, local | `vertebra._postgres` absent, env unset | Case reports SKIPPED with `postgres conformance requires the vertebra._postgres native module` | No setup ERROR, no `xfailed` |
| Pending module missing, Docker | same, `VERTEBRA_CONFORMANCE_DOCKER=1` | Case reports SKIPPED with the same reason | Docker runs stay green until each native flow ships and clears `pending_flow_id` |
| DuckDB case | `duckdb` package installed, `vertebra._duckdb` absent | SKIPPED with `duckdb conformance requires the vertebra._duckdb native module` in both modes | DuckDB conformance stays pending on the native module |
| Non-pending module missing, local | `vertebra._bigquery` absent, env unset | SKIPPED with `bigquery conformance requires the vertebra._bigquery native module` | No setup ERROR |
| Non-pending module missing, Docker | same, `VERTEBRA_CONFORMANCE_DOCKER=1` | Case reports ERROR at setup with the same reason; exit code non-zero | No silent pass for shipped native modules |
| Native module present, pending | `vertebra._postgres` imports, `pending_flow_id="native-postgres-driver"` | Strict `xfail` with `Owned by native-postgres-driver` | XPASS still fails |
| Cloud engine without credentials | `vertebra._bigquery` absent, no DSN | Native-module skip, raised before the `VERTEBRA_CONFORMANCE_BIGQUERY_DSN` skip | Module resolution first |
| Docker env other than `"1"` | `VERTEBRA_CONFORMANCE_DOCKER=0` or `true` | Skip | Exact-match `"1"` like the Docker fixture branch |
| Spec without native module | SQLite (`native_extension_module=None`) | No check, no mark | SQLite runs unchanged |
| Unused driver fixtures | `vtb_oracle_driver`, `vtb_mssql_driver` in the integration conftest | Deleted with the `SyncOracleDriver` / `SyncMSSQLDriver` imports; collection exits `0` | No fixture constructs a fail-loud driver |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given `vertebra._bigquery` absent and no BigQuery DSN, when `_resolve_live_config` runs for the BigQuery spec, then `pytest.skip` fires with `bigquery conformance requires the vertebra._bigquery native module`.
- Given the same with `VERTEBRA_CONFORMANCE_DOCKER=1`, then `pytest.fail` fires with the same reason.
- Given `vertebra._postgres` absent and `VERTEBRA_CONFORMANCE_DOCKER=1`, when `_resolve_live_config` runs for the pending PostgreSQL spec, then `pytest.skip` fires with `postgres conformance requires the vertebra._postgres native module`.
- Given a pending spec whose module imports, when `_build_live_engine_params` builds its parameter, then the parameter carries `pytest.mark.xfail(strict=True, reason="Owned by <pending_flow_id>")`.

### 2.6 Alternatives Considered

| Alternative | Depth / Locality / Seam | Outcome |
| --- | --- | --- |
| Fail every engine, pending or not, under `VERTEBRA_CONFORMANCE_DOCKER=1` | Docker conformance fails on every host until all five native flows ship | Rejected: pending engines skip; the failure applies once a native flow clears `pending_flow_id` |
| Catch `MissingDependencyError` and call `pytest.fail` while keeping strict `xfail` on pending specs | pytest reports a setup failure under an `xfail` mark as `xfailed`, so Docker mode would silently pass | Rejected: apply strict `xfail` only when the native module imports |
| Collection-time `pytest.mark.skip` for missing modules | Skip is decided at collection; Docker mode cannot turn it into a failure | Rejected: runtime `_require_native_module` decides skip or fail |
| Helper unit tests in a new `src/py/tests/unit/` module | Must import private helpers from an integration test module | Rejected: tests live beside the private helpers in `test_driver_conformance.py`, which `make py-test` already collects |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch09]`
- Conformance-outcome prose in `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` and `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` is owned by [ch11-docs-and-okf](../ch11-docs-and-okf/spec.md); this chapter changes no user docs. Python docstrings on touched helpers follow Google style.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R12 | [t01](tasks/t01-conformance-outcomes.md) | `behavior_tdd` | `src/py/tests/integration/db/test_driver_conformance.py` |

## 5. Implementation Plan

- [ ] driver-fail-loud:ch09:t01 — Conformance skips or fails explicitly on a missing native module; integration conftest drops `from __future__` and the unused Oracle / MSSQL driver fixtures (tasks/t01-conformance-outcomes.md)
- **Chapter Recovery Checkpoint**: `uv run --no-sync ruff check src/py/tests/integration/db/test_driver_conformance.py src/py/tests/integration/conftest.py && uv run --no-sync pytest src/py/tests/integration/db/test_driver_conformance.py -q -rs && uv run --no-sync pytest src/py/tests/integration --collect-only -q` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline (`HEAD`)**: `0acdf576` — chapter planned; `0/1` tasks completed; waits on `ch04-redis-valkey-memory-removal`.
- **Remaining Tasks**: `t01`.
- **Documentation & OKF Sync**: none owned; `ch11-docs-and-okf` documents conformance outcomes.
- **Active Invariants**: strict `xfail` remains for pending engines whose native module imports; pending engines skip a missing module in Docker mode; `test_engine_driver_resolution_and_provide_driver` stays owned by ch01 / ch02.

[^conformance]: `src/py/tests/integration/db/test_driver_conformance.py::_resolve_live_config` — Conformance live config resolution.
[^conformance-params]: `src/py/tests/integration/db/test_driver_conformance.py::_build_live_engine_params` — Pending-engine strict xfail parameter builder.
[^integration-conftest]: `src/py/tests/integration/conftest.py` — pytest-databases Docker fixtures for integration tests.
[^module-loader]: `src/py/vertebra/utils/module_loader.py::import_optional` — Optional module import returning None when unavailable.
[^missing-dep]: `src/py/vertebra/_typing.py::MissingDependencyError` — Missing dependency error raised by ensure_module.
