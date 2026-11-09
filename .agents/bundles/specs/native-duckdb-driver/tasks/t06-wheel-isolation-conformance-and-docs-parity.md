---
type: Task
id: "native-duckdb-driver:t06"
title: "Activate DuckDB conformance matrix, wheel isolation targets, unit tests, and documentation sync"
description: "Remove PyPI duckdb from pyproject.toml, add duckdb-test and duckdb-wheel-check to Makefile, activate duckdb (pending_flow_id=None, supports_pipeline=True) in test_driver_conformance.py, add test_duckdb_driver.py and test_optional_driver_wheels.py, update test_enterprise_olap_drivers.py and test_gcs_token_refresh.py, and synchronize docs/ and OKF knowledge."
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-duckdb-driver:t05"
read_set:
  - pyproject.toml
  - Makefile
  - src/py/tests/integration/db/test_driver_conformance.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
  - docs/usage/databases.md
  - docs/reference/database-capabilities.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
files:
  - pyproject.toml
  - Makefile
  - src/py/tests/integration/db/test_driver_conformance.py
  - src/py/tests/unit/db/test_duckdb_driver.py
  - src/py/tests/unit/db/test_optional_driver_wheels.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
  - docs/usage/databases.md
  - docs/reference/database-capabilities.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
tests:
  - src/py/tests/integration/db/test_driver_conformance.py
  - src/py/tests/unit/db/test_duckdb_driver.py
  - src/py/tests/unit/db/test_optional_driver_wheels.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
estimated_loc: 550
plan_revision: 4
plan_commit: d7a629b1
state_revision: 1
claimed_by: agent
claimed_at: "2026-11-09T16:00:00Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T16:34:00Z"
last_verified_commit: 47f54de9
verification_evidence: "pytest src/py/tests/unit/db/test_duckdb_driver.py src/py/tests/unit/db/test_optional_driver_wheels.py src/py/tests/integration/db/test_driver_conformance.py -k duckdb && python -m tools.scripts.review_setup validate"
created_at: "2026-11-09T15:46:00Z"
updated_at: "2026-11-09T16:34:00Z"
commit: null
sources:
  - id: conformance-py
    resource: src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS
    title: Cross-engine database driver conformance matrix
  - id: docs-db
    resource: docs/usage/databases.md
    title: Public database drivers usage documentation
  - id: okf-drivers
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: OKF knowledge chapter for database drivers and dialers
tags:
  - duckdb
  - conformance
  - wheel
  - docs
  - okf
---

# Task t06: Activate DuckDB Conformance Matrix, Wheel Isolation Targets, Unit Tests, and Documentation Sync

## Objective & Scope Boundary

Remove the temporary PyPI `duckdb` dependency from `pyproject.toml`, add `duckdb-test` and `duckdb-wheel-check` targets to `Makefile`, activate `duckdb` (`pending_flow_id=None`, `supports_pipeline=True`, `_DUCKDB_ITEMS_DDL`, `_DUCKDB_TYPES_DDL`) in `src/py/tests/integration/db/test_driver_conformance.py`, add unit tests in `src/py/tests/unit/db/test_duckdb_driver.py` and `src/py/tests/unit/db/test_optional_driver_wheels.py`, update `src/py/tests/unit/db/test_enterprise_olap_drivers.py` and `src/py/tests/unit/storage/test_gcs_token_refresh.py`, and synchronize `docs/usage/databases.md`, `docs/reference/database-capabilities.md`, and `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` [^conformance-py] [^docs-db] [^okf-drivers].

## Symbol & Signature Contracts

- `src/py/tests/integration/db/test_driver_conformance.py`:
  - `ENGINE_SPECS["duckdb"]`: `pending_flow_id=None`, `supports_pipeline=True`, `items_ddl=_DUCKDB_ITEMS_DDL`, `types_ddl=_DUCKDB_TYPES_DDL`.
- `Makefile`:
  - `duckdb-test`: runs `cargo test` for `vertebra-duckdb` and pytest for DuckDB unit and conformance suites.
  - `duckdb-wheel-check`: builds `vertebra-duckdb` wheel via `maturin build` and asserts wheel archive contains only `_duckdb` and `.dist-info/` entries.

## Acceptance Criteria

- [x] `src/py/tests/integration/db/test_driver_conformance.py` passes all sync and async conformance cases for `duckdb` (`pending_flow_id=None`, `supports_pipeline=True`).
- [x] `src/py/tests/unit/db/test_duckdb_driver.py` and `src/py/tests/unit/db/test_optional_driver_wheels.py` pass.
- [x] `pyproject.toml` has no PyPI `duckdb` dependency and `Makefile` defines `duckdb-test` and `duckdb-wheel-check`.
- [x] `docs/usage/databases.md`, `docs/reference/database-capabilities.md`, and `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` pass `tools.scripts.review_setup validate`.

[^conformance-py]: `src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS` — Cross-engine database driver conformance matrix.
[^docs-db]: `docs/usage/databases.md` — Public database drivers usage documentation.
[^okf-drivers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — OKF knowledge chapter for database drivers and dialers.
