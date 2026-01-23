# Implementation Plan - Test Suite Reorganization

## Phase 1: Setup and Unit Test Migration [checkpoint: 6593b6f]
Establish the new directory structure, configure scoped `conftest.py` files, and move purely functional/unit tests.

- [x] Task: Create Root Directory Structure 2972dbb
    - [ ] Create `src/py/tests/unit`
    - [ ] Create `src/py/tests/integration`
- [x] Task: Configure Test Fixtures (Conftest Split) 2d69979
    - [ ] Analyze root `src/py/tests/conftest.py`.
    - [ ] Retain truly global fixtures in `src/py/tests/conftest.py`.
    - [ ] Create `src/py/tests/integration/conftest.py` and move database/service-related fixtures there.
    - [ ] Create `src/py/tests/unit/conftest.py` and move unit-test specific mocks there.
    - [ ] Move/Refactor `database.py` and `_test_app.py` to `src/py/tests/integration/helpers/` if they are only used for integration.
- [x] Task: Migrate Core Logic Unit Tests 97a50b6
    - [ ] Move `test_cli_init.py` to `src/py/tests/unit/cli/test_init.py` (creating `unit/cli/`)
    - [ ] Move `test_module_loader.py` to `src/py/tests/unit/core/test_module_loader.py`
    - [ ] Move `test_serialization_codec.py` to `src/py/tests/unit/extensions/serialization/test_codec.py`
    - [ ] Move `test_build.py` to `src/py/tests/unit/tools/test_build.py`
    - [ ] Update imports in migrated files to ensure they can locate helpers/fixtures.
- [x] Task: Verify Unit Tests e1ca2ff
    - [ ] Run `pytest src/py/tests/unit` to ensure unit tests pass with the new fixture setup.
- [ ] Task: Checkpoint - Verify Phase 1 complete and create recovery point
- [ ] Task: Flow - User Manual Verification 'Phase 1: Setup and Unit Test Migration' (Protocol in workflow.md)

## Phase 2: Integration Test Migration & Consolidation (SQLAlchemy)
Move and consolidate the heavy SQLAlchemy and DBAPI related tests into the integration suite.

- [x] Task: Migrate SQLAlchemy Tests
    - [x] Create `src/py/tests/integration/extensions/sqlalchemy/`
    - [x] Consolidate `test_sqlalchemy_dialect.py` and `test_sqlalchemy_dbapi.py` into `src/py/tests/integration/extensions/sqlalchemy/test_dialect.py` and `test_dbapi.py`.
    - [x] Move `test_sqlalchemy_integration.py` to `src/py/tests/integration/extensions/sqlalchemy/test_integration.py`.
- [x] Task: Migrate Advanced Alchemy Tests
    - [x] Move `test_advanced_alchemy.py` to `src/py/tests/integration/extensions/advanced_alchemy/test_plugin.py`.
- [x] Task: Verify SQLAlchemy Integration Tests 1aa3765
    - [ ] Run `pytest src/py/tests/integration/extensions/sqlalchemy` to confirm DB tests pass with the new `integration/conftest.py`.
- [ ] Task: Checkpoint - Verify Phase 2 complete and create recovery point
- [ ] Task: Flow - User Manual Verification 'Phase 2: Integration Test Migration & Consolidation (SQLAlchemy)' (Protocol in workflow.md)

## Phase 3: Integration Test Migration & Consolidation (SQLSpec & Litestar)
Move the remaining complex integration tests for SQLSpec and the Litestar plugin.

- [ ] Task: Migrate SQLSpec Tests
    - [ ] Create `src/py/tests/integration/extensions/sqlspec/`
    - [ ] Consolidate `test_sqlspec_arrow.py`, `test_sqlspec_integration.py`, and `test_sqlspec_storage.py` into appropriate files within this directory.
- [ ] Task: Migrate Litestar & Serialization Tests
    - [ ] Move `test_litestar_plugin.py` to `src/py/tests/integration/extensions/litestar/test_plugin.py`.
    - [ ] Move `test_serialization_litestar.py` to `src/py/tests/integration/extensions/serialization/test_litestar_adapter.py`.
- [ ] Task: Migrate Remaining Tests
    - [ ] Move `test_supervisor_eventloop.py` to `src/py/tests/integration/core/test_supervisor.py` (or unit if appropriate).
    - [ ] Move `test_standalone.py` to `src/py/tests/integration/core/test_standalone.py`.
- [ ] Task: Cleanup
    - [ ] Remove old empty directories in `src/py/tests/`.
- [ ] Task: Final Verification
    - [ ] Run `pytest src/py/tests/` (full suite) to ensure comprehensive passing.
- [ ] Task: Checkpoint - Verify Phase 3 complete and create recovery point
- [ ] Task: Flow - User Manual Verification 'Phase 3: Integration Test Migration & Consolidation (SQLSpec & Litestar)' (Protocol in workflow.md)
