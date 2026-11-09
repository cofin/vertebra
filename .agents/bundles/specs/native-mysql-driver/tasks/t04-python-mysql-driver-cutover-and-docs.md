---
type: Task
flow_id: native-mysql-driver
task_id: t04
title: "Cut Python SyncMySQLDriver and AsyncMySQLDriver over to vertebra._mysql and update conformance and docs"
description: "Cut src/py/vertebra/db/adapters/mysql/driver.py and config.py over to vertebra._mysql (SyncMySQLClient, AsyncMySQLClient), remove all pymysql, asyncmy, and memory:// execution paths, activate mysql in test_driver_conformance.py, and update documentation and OKF chapters."
status: stable
state: in_progress
verification_strategy: tdd
Verification-Exempt-Reason: null
dependencies:
  - t03
files_to_modify:
  - src/py/vertebra/db/adapters/mysql/config.py
  - src/py/vertebra/db/adapters/mysql/core.py
  - src/py/vertebra/db/adapters/mysql/driver.py
  - src/py/tests/unit/db/test_mysql_driver.py
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
  - src/py/tests/integration/db/test_driver_conformance.py
  - src/rs/vertebra-mysql/tests/wire_conformance.rs
  - docs/usage/databases.md
  - docs/reference/database-capabilities.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
read_set:
  - .agents/bundles/specs/native-mysql-driver/spec.md
  - src/py/vertebra/db/adapters/mysql/config.py
  - src/py/vertebra/db/adapters/mysql/core.py
  - src/py/vertebra/db/adapters/mysql/driver.py
  - src/py/tests/unit/db/test_mysql_driver.py
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
  - src/py/tests/integration/db/test_driver_conformance.py
  - docs/usage/databases.md
  - docs/reference/database-capabilities.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
commands_to_run:
  - "cargo test --manifest-path src/rs/Cargo.toml -p vertebra-mysql --locked"
  - "uv run ruff check src/py/vertebra/db/adapters/mysql src/py/tests/unit/db/test_mysql_driver.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/integration/db/test_driver_conformance.py"
  - "uv run mypy src/py/vertebra/db/adapters/mysql src/py/vertebra/_mysql.pyi"
  - "uv run pytest src/py/tests/unit/db/test_mysql_driver.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/integration/db/test_driver_conformance.py -k mysql -q"
  - "uv run python -m tools.scripts.review_setup validate"
commit_sha: null
created_at: "2026-11-09T03:25:00Z"
updated_at: "2026-11-09T03:25:00Z"
sources:
  - id: mysql-spec
    resource: .agents/bundles/specs/native-mysql-driver/spec.md
    title: Native MySQL driver specification
  - id: mysql-py
    resource: src/py/vertebra/db/adapters/mysql/driver.py
    title: Python SyncMySQLDriver and AsyncMySQLDriver target cutover module
  - id: conformance-py
    resource: src/py/tests/integration/db/test_driver_conformance.py
    title: Cross-engine 7-category driver conformance suite
---

# Task t04: Cut Python `SyncMySQLDriver` and `AsyncMySQLDriver` over to `vertebra._mysql` and update conformance and docs

<!-- truth: start -->
- `SyncMySQLDriver` and `AsyncMySQLDriver` load `vertebra._mysql` via `ensure_module("vertebra._mysql", package="vertebra-core[mysql]", extra="mysql")` and delegate all operations to `SyncMySQLClient` and `AsyncMySQLClient` with zero `pymysql`, `asyncmy`, or `memory://` fallback paths [^mysql-spec] [^mysql-py].
- `src/py/tests/integration/db/test_driver_conformance.py` sets `pending_flow_id=None` for `mysql` and passes all 7 conformance categories (`crud_and_Select`, `arrow_roundtrip`, `transactions_and_savepoints`, `error_mapping`, `cancellation`, `pool_concurrency`, `schema_hydration`) across sync and async modes [^conformance-py].
<!-- truth: end -->

## Context

Task `t04` completes the Python cutover of `src/py/vertebra/db/adapters/mysql/` onto `vertebra._mysql`, deletes all `pymysql`, `asyncmy`, and `memory://` execution paths, activates `mysql` in `test_driver_conformance.py`, and updates user and architectural documentation [^mysql-spec] [^mysql-py] [^conformance-py].

## Public Seam & TDD Plan

1. **Red**:
   - Update `src/py/tests/unit/db/test_mysql_driver.py`, `src/py/tests/unit/db/test_native_db_drivers.py`, and `src/py/tests/integration/db/test_driver_conformance.py` (`pending_flow_id=None` for `mysql`) to assert:
     - `mysql://memory` and `mysql:///:memory:` raise `ImproperConfigurationError`.
     - `SyncMySQLDriver` and `AsyncMySQLDriver` resolve `vertebra._mysql` (`SyncMySQLClient`, `AsyncMySQLClient`) via `ensure_module("vertebra._mysql", package="vertebra-core[mysql]", extra="mysql")` and never import `pymysql` or `asyncmy`.
     - All 7 conformance categories in `test_driver_conformance.py` pass for `mysql` in both `sync` and `async` modes, including `has_shm_capsule is True` on `select_to_arrow`.
2. **Green**:
   - Cut `src/py/vertebra/db/adapters/mysql/config.py`, `src/py/vertebra/db/adapters/mysql/core.py`, and `src/py/vertebra/db/adapters/mysql/driver.py` over to `vertebra._mysql`.
   - Add `src/rs/vertebra-mysql/tests/wire_conformance.rs` integration tests and update `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`, `docs/usage/databases.md`, `docs/reference/database-capabilities.md`, and `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`.
3. **Refactor & Verify**:
   - Run `cargo test`, `ruff check`, `mypy`, `mypy.stubtest`, `pytest`, and `review_setup validate`.

## Acceptance Criteria

- [ ] `src/py/vertebra/db/adapters/mysql/` contains zero `pymysql`, `asyncmy`, or `memory://` execution paths; `memory://` DSNs raise `ImproperConfigurationError`.
- [ ] `pytest src/py/tests/integration/db/test_driver_conformance.py -k mysql` passes all 14 sync+async conformance tests with `pending_flow_id=None`.
- [ ] `pytest src/py/tests/unit/db/test_mysql_driver.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` passes.
- [ ] `docs/usage/databases.md`, `docs/reference/database-capabilities.md`, and `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` document `vertebra._mysql` (`vertebra-core[mysql]`) accurately without referencing contributor `Makefile` targets in public `docs/`.

[^mysql-spec]: `.agents/bundles/specs/native-mysql-driver/spec.md` — Native MySQL driver specification.
[^mysql-py]: `src/py/vertebra/db/adapters/mysql/driver.py` — Python SyncMySQLDriver and AsyncMySQLDriver target cutover module.
[^conformance-py]: `src/py/tests/integration/db/test_driver_conformance.py` — Cross-engine 7-category driver conformance suite.
