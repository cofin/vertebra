---
type: Task
id: "driver-conformance-ci:ch02:t02"
flow_id: driver-conformance-ci
chapter_id: ch02-conformance-harness-and-live-suites
title: "Add MariaDB conformance spec, clean dead matrix code, and create test_duckdb_native.py"
description: "Add mariadb to ENGINE_SPECS, delete dead branches in test_driver_conformance.py, and verify DuckDB native Arrow/pipeline/savepoint and loopback GCS bearer rotation."
status: stable
state: completed
commit: 38e02b16
last_verified_commit: 38e02b16
last_verified_at: "2026-11-14T17:42:00Z"
verification_evidence: "VERTEBRA_CONFORMANCE_ENGINES=duckdb pytest test_driver_conformance.py test_duckdb_native.py (42 passed, 0 skipped) + ruff + mypy clean"
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_revision: 4
plan_commit: 37972f2a
depends_on:
  - "driver-conformance-ci:ch02:t01"
  - "driver-conformance-ci:ch01:t02"
files:
  - src/py/tests/integration/db/test_driver_conformance.py
  - src/py/tests/integration/db/test_duckdb_native.py
tests:
  - src/py/tests/unit/db/test_conformance_gate.py
read_set:
  - src/py/tests/integration/db/test_driver_conformance.py
  - src/py/tests/integration/conformance_gate.py
  - src/py/vertebra/storage/_secrets.py
  - src/py/vertebra/db/adapters/duckdb/driver.py
estimated_loc: 240
tags:
  - conformance
  - mariadb
  - duckdb
  - gcs
sources:
  - id: conformance-suite
    resource: src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS
    title: "ENGINE_SPECS and _resolve_live_config in test_driver_conformance.py"
  - id: duckdb-driver
    resource: src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver
    title: "Native DuckDB driver Arrow, pipeline, savepoint, and secret rotation APIs"
---

# Task `t02`: Add MariaDB Conformance Spec, Clean Dead Matrix Code, and Create `test_duckdb_native.py`

## Objective & Scope Boundary

Update `src/py/tests/integration/db/test_driver_conformance.py` (`mariadb` `EngineConformanceSpec` + dead-surface cleanup) and create `src/py/tests/integration/db/test_duckdb_native.py` per [`../spec.md` §2](../spec.md) [^conformance-suite] [^duckdb-driver]. Out of scope: MySQL/MariaDB, SQL Server, and Oracle native edge suites (`t03`–`t04`).

## Bounded Read Manifest

- `src/py/tests/integration/db/test_driver_conformance.py` and `src/py/tests/integration/conformance_gate.py` — Conformance matrix and `driver_engine("duckdb")` tagging.
- `src/py/vertebra/storage/_secrets.py` and `src/py/vertebra/db/adapters/duckdb/driver.py` — GCS bearer secret providers and DuckDB native capabilities.

## Contracts & Edge Cases

- **`test_driver_conformance.py`**: Add a `mariadb` `EngineConformanceSpec` resolving `VERTEBRA_CONFORMANCE_MARIADB_DSN` or `vtb_mariadb_config` (skipping offline with `"MariaDB conformance requires VERTEBRA_CONFORMANCE_MARIADB_DSN or VERTEBRA_CONFORMANCE_DOCKER=1"`), attach `pytest.mark.driver_engine(spec.engine)` in `_build_live_engine_params`, and delete unused `_DUCKDB_SKIP_REASON` and `VERTEBRA_CONFORMANCE_DUCKDB_DSN` from `test_driver_conformance.py`.
- **`test_duckdb_native.py` (`pytestmark = [pytest.mark.driver_engine("duckdb")]`)**:
  - Offline embedded `:memory:` tests (always executed, zero skips): parameterized sync/async `select_to_arrow` (`VTBARROW` SHM above threshold + `/dev/shm` cleanup), `stream_arrow_batches`, `load_from_arrow`, `pipeline()` rollback on mid-batch error, savepoints, and loopback token-server + `SyncGcsBearerSecretProvider` / `AsyncGcsBearerSecretProvider` `(HTTP 401)` invalidation & generation rotation.
  - Opt-in live `httpfs` test gated on `VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS == "1"` reading `gs://` against a loopback GCS server across token invalidation (`Bearer tok-1` -> `Bearer tok-2`).

## Test Matrix

- Offline run with `VERTEBRA_CONFORMANCE_ENGINES=duckdb` (and `VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS` unset: `httpfs` test separated or deselected cleanly unless `HTTPFS=1` so offline `VERTEBRA_CONFORMANCE_ENGINES=duckdb` executes embedded + loopback rotation tests with `0` skips; when `VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1`, live `httpfs` test also runs with `0` skips), plus offline `mariadb` skip verification.

## Verification

```bash
VERTEBRA_CONFORMANCE_ENGINES=duckdb uv run --no-sync pytest src/py/tests/integration/db/test_driver_conformance.py src/py/tests/integration/db/test_duckdb_native.py -q && uv run --no-sync pytest src/py/tests/integration/db/test_driver_conformance.py -k mariadb -q && uv run --no-sync ruff check src/py/tests/integration/db/test_driver_conformance.py src/py/tests/integration/db/test_duckdb_native.py && uv run --no-sync mypy src/py/tests/integration/db/test_driver_conformance.py src/py/tests/integration/db/test_duckdb_native.py
```

## Acceptance Criteria

- [x] `ENGINE_SPECS` includes `mariadb` with zero dead conformance constants or duplicate branches.
- [x] `VERTEBRA_CONFORMANCE_ENGINES=duckdb` passes offline in `< 15s` with zero skipped items.

[^conformance-suite]: `src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS` — `ENGINE_SPECS` and `_resolve_live_config` in `test_driver_conformance.py`.
[^duckdb-driver]: `src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver` — Native DuckDB driver Arrow, pipeline, savepoint, and secret rotation APIs.
