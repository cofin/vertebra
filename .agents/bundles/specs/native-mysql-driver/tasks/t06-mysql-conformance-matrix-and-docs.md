---
type: Task
flow_id: native-mysql-driver
task_id: t06
title: "Activate MySQL conformance matrix, unit suites, and documentation"
description: "Set pending_flow_id=None and supports_pipeline=True for mysql in test_driver_conformance.py, update test_mysql_driver.py, test_native_db_drivers.py, test_sqlalchemy_and_sqlspec_vtb.py, and wire_conformance.rs, and update docs and OKF knowledge chapters."
status: stable
state: completed
verification_strategy: tdd
Verification-Exempt-Reason: null
dependencies:
  - t05
files_to_modify:
  - src/py/tests/integration/db/test_driver_conformance.py
  - src/py/tests/unit/db/test_mysql_driver.py
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
  - src/rs/vertebra-mysql/tests/wire_conformance.rs
  - docs/usage/databases.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
read_set:
  - .agents/bundles/specs/native-mysql-driver/spec.md
  - src/py/tests/integration/db/test_driver_conformance.py
  - src/py/tests/unit/db/test_mysql_driver.py
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
  - docs/usage/databases.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
commands_to_run:
  - "cargo test --manifest-path src/rs/Cargo.toml -p vertebra-mysql --locked"
  - "uv run pytest src/py/tests/unit/db/test_mysql_driver.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/integration/db/test_driver_conformance.py -k mysql -q"
  - "uv run python -m tools.scripts.review_setup validate"
commit_sha: 3700e5d0
created_at: "2026-11-09T16:50:00Z"
updated_at: "2026-11-09T16:50:00Z"
sources:
  - id: mysql-spec
    resource: .agents/bundles/specs/native-mysql-driver/spec.md
    title: Native MySQL driver specification
  - id: conformance-py
    resource: src/py/tests/integration/db/test_driver_conformance.py
    title: Cross-engine driver conformance suite
  - id: db-knowledge
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: Database Drivers and Cloud Dialers OKF knowledge chapter
---

# Task t06: Activate MySQL conformance matrix, unit suites, and documentation

<!-- truth: start -->
- `src/py/tests/integration/db/test_driver_conformance.py` sets `pending_flow_id=None` and `supports_pipeline=True` for `mysql` with `_MYSQL_ITEMS_DDL` and `_MYSQL_TYPES_DDL`, passing all 36 sync and async conformance tests [^mysql-spec] [^conformance-py].
- `test_mysql_driver.py`, `test_native_db_drivers.py`, `test_sqlalchemy_and_sqlspec_vtb.py`, and `wire_conformance.rs` exercise native `vertebra._mysql` execution, and `docs/usage/databases.md` and `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` document `vertebra-core[mysql]` (`vertebra._mysql`) [^db-knowledge].
<!-- truth: end -->

## Context

Task `t06` enables the full MySQL conformance matrix (`pending_flow_id=None`, `supports_pipeline=True`) in `test_driver_conformance.py`, updates unit and extension test suites to run against `vertebra._mysql`, and updates user and OKF documentation [^mysql-spec] [^conformance-py] [^db-knowledge].

## Public Seam & TDD Plan

1. **Red**:
   - Set `pending_flow_id=None` and `supports_pipeline=True` for `mysql` in `src/py/tests/integration/db/test_driver_conformance.py` with `_MYSQL_ITEMS_DDL` and `_MYSQL_TYPES_DDL`.
   - Rewrite `src/py/tests/unit/db/test_mysql_driver.py` and update `test_native_db_drivers.py` and `test_sqlalchemy_and_sqlspec_vtb.py` for native `SyncMySQLDriver` and `AsyncMySQLDriver`.
2. **Green**:
   - Wire `MySQLWireTestServer` fallback in `test_driver_conformance.py` and `test_native_db_drivers.py`, add `src/rs/vertebra-mysql/tests/wire_conformance.rs`, and update `docs/usage/databases.md` and `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`.
3. **Refactor & Verify**:
   - Run `cargo test`, `pytest`, `ruff check`, and `review_setup validate`.

## Acceptance Criteria

- [x] All 36 MySQL sync and async tests in `src/py/tests/integration/db/test_driver_conformance.py` pass with `pending_flow_id=None` and `supports_pipeline=True`.
- [x] `src/py/tests/unit/db/test_mysql_driver.py`, `src/py/tests/unit/db/test_native_db_drivers.py`, `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`, and `src/rs/vertebra-mysql/tests/wire_conformance.rs` pass.
- [x] `docs/usage/databases.md` and `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` reflect `vertebra-core[mysql]` (`vertebra._mysql`) and pass `review_setup validate`.

[^mysql-spec]: `.agents/bundles/specs/native-mysql-driver/spec.md` — Native MySQL driver specification.
[^conformance-py]: `src/py/tests/integration/db/test_driver_conformance.py` — Cross-engine driver conformance suite.
[^db-knowledge]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Database Drivers and Cloud Dialers OKF knowledge chapter.
