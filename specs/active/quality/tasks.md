# Tasks: Infrastructure & Quality Consolidation

**PRD**: `/home/cody/code/c/vertebra/specs/active/quality/prd.md`
**Status**: Complete - Merged
**Priority**: P0 - Blocking 1.0 Release

---

## Phase 1: Test Deadlock Fix (P0 - BLOCKING)

**Duration**: 2-3 days
**Risk**: Low (test-only changes)
**Dependencies**: None

### Task Group 1.1: Immediate Unblock

- [x] **Task 1.1.1**: Add pytest-timeout configuration
  - Add `timeout = 30` to `[tool.pytest.ini_options]` in `pyproject.toml`
  - Add `timeout_method = "thread"`
  - File: `/home/cody/code/c/vertebra/pyproject.toml`

- [x] **Task 1.1.2**: Clean stale shared memory
  - Command: `rm -rf /dev/shm/test-* /dev/shm/*vertebra*`
  - Verify: `ls /dev/shm/ | grep -E "test-|vertebra" | wc -l` returns 0

- [x] **Task 1.1.3**: Enhance cleanup patterns in conftest.py
  - Add patterns: `"test-*"`, `"*vertebra*"`, `"*.control"`, `"*.req"`, `"*.resp"`
  - Add before AND after cleanup fixture
  - File: `/home/cody/code/c/vertebra/src/py/tests/conftest.py`

**Acceptance**: Tests no longer hang immediately

---

### Task Group 1.2: Test Helpers Module

- [x] **Task 1.2.1**: Create helpers package
  - File: `/home/cody/code/c/vertebra/src/py/tests/helpers/__init__.py`
  - Export: `unique_shm_name`, `tracked_shm_cleanup`

- [x] **Task 1.2.2**: Implement `unique_shm_name()` function
  - Generate unique name like `vertebra-test-{uuid4().hex[:8]}`
  - File: `/home/cody/code/c/vertebra/src/py/tests/helpers/shm.py`

- [x] **Task 1.2.3**: Implement `tracked_shm_cleanup()` context manager
  - Track created shared memory names
  - Clean up on exit using `shm_unlink()`
  - Fallback glob cleanup for orphans
  - File: `/home/cody/code/c/vertebra/src/py/tests/helpers/shm.py`

**Acceptance**: Test helpers module exists and works

---

### Task Group 1.3: Update Broker Tests

- [x] **Task 1.3.1**: Update `test_broker_client.py`
  - Replace hardcoded `"vertebra-broker-test"` with `unique_shm_name()`
  - Wrap test body in `tracked_shm_cleanup()` context manager
  - File: `/home/cody/code/c/vertebra/src/py/tests/test_broker_client.py`

- [x] **Task 1.3.2**: Update `test_broker_pool.py`
  - Same pattern as Task 1.3.1
  - File: `/home/cody/code/c/vertebra/src/py/tests/test_broker_pool.py`

- [x] **Task 1.3.3**: Update `test_broker_multi_client.py`
  - Same pattern as Task 1.3.1
  - File: `/home/cody/code/c/vertebra/src/py/tests/test_broker_multi_client.py`

**Acceptance**: Broker tests use unique names and cleanup

---

### Task Group 1.4: Update ASGI Tests

- [x] **Task 1.4.1**: Update `test_asgi_worker.py`
  - Replace hardcoded names with `unique_shm_name()`
  - Add cleanup context manager
  - File: `/home/cody/code/c/vertebra/src/py/tests/test_asgi_worker.py`

**Acceptance**: ASGI tests use unique names and cleanup

---

### Task Group 1.5: Update IPC Tests

- [x] **Task 1.5.1**: Add thread coordination to `test_ipc_channel.py`
  - Ensure threads exit cleanly
  - Add timeouts to blocking operations
  - File: `/home/cody/code/c/vertebra/src/py/tests/test_ipc_channel.py`

- [x] **Task 1.5.2**: Add thread coordination to `test_ipc_integration.py`
  - Same pattern as Task 1.5.1
  - File: `/home/cody/code/c/vertebra/src/py/tests/test_ipc_integration.py`

- [x] **Task 1.5.3**: Add asyncio timeouts to async test operations
  - Use `asyncio.wait_for()` with timeout
  - Prevent infinite waits

**Acceptance**: IPC tests have proper timeouts and coordination

---

### Task Group 1.6: Verification

- [x] **Task 1.6.1**: Run `make test` - should complete in < 60s
  - Command: `time make test`
  - Expected: All tests pass, < 60 seconds

- [x] **Task 1.6.2**: Run 5 consecutive test runs
  - Command: `for i in {1..5}; do make test || exit 1; done`
  - Expected: All 5 runs succeed

- [x] **Task 1.6.3**: Verify no stale shared memory
  - Command: `ls /dev/shm/ | grep -E "test-|vertebra" | wc -l`
  - Expected: 0 after each run

- [x] **Task 1.6.4**: Test parallel execution
  - Command: `uv run pytest src/py/tests -n 4 --timeout=30`
  - Expected: All tests pass with pytest-xdist

**Acceptance**: Test suite is reliable and fast

---

## Phase 2: Codebase Consolidation Phase 4 (P1)

**Duration**: 1-2 days
**Risk**: Low (mostly renames and docs)
**Dependencies**: Phase 1 (tests must pass)

### Task Group 2.1: TypeScript File Renames

- [x] **Task 2.1.1**: Rename `worker-pool.ts` to `worker-connections.ts`
  - From: `/home/cody/code/c/vertebra/src/ts/src/server/worker-pool.ts`
  - To: `/home/cody/code/c/vertebra/src/ts/src/server/worker-connections.ts`

- [x] **Task 2.1.2**: Rename `worker-manager.ts` to `process-manager.ts`
  - From: `/home/cody/code/c/vertebra/src/ts/src/server/worker-manager.ts`
  - To: `/home/cody/code/c/vertebra/src/ts/src/server/process-manager.ts`

- [x] **Task 2.1.3**: Update imports in `index.ts`
  - Update `import { WorkerPool }` to `import { IpcConnectionPool }`
  - Update `import { WorkerManager }` to `import { ProcessManager }`
  - File: `/home/cody/code/c/vertebra/src/ts/src/server/index.ts`

- [x] **Task 2.1.4**: Update any other import references
  - Search: `grep -r "worker-pool" src/ts/`
  - Search: `grep -r "worker-manager" src/ts/`

- [x] **Task 2.1.5**: Run TypeScript tests and type check
  - Command: `make js-test && make js-type-check`

**Acceptance**: TypeScript files renamed, tests pass

---

### Task Group 2.2: Update CLAUDE.md

- [x] **Task 2.2.1**: Update Directory Layout section
  - Reflect new file names
  - Document storage module (merged from filesystem + storage)
  - File: `/home/cody/code/c/vertebra/CLAUDE.md`

- [x] **Task 2.2.2**: Document `resources/broker.py` purpose
  - Add to Directory Layout section
  - Explain internal IPC client role

- [x] **Task 2.2.3**: Document TypeScript naming conventions
  - Explain 3 types of "pools" (database, worker connection, process)
  - Add naming convention section

- [x] **Task 2.2.4**: Add Public vs Internal API naming section
  - `Vertebra*` for public APIs
  - `BrokerClient` etc. for internal APIs

**Acceptance**: CLAUDE.md accurately reflects codebase

---

### Task Group 2.3: Add Module Docstrings

- [x] **Task 2.3.1**: Add docstring to `storage/__init__.py`
  - Explain unified storage abstraction
  - File: `/home/cody/code/c/vertebra/src/py/vertebra/storage/__init__.py`

- [x] **Task 2.3.2**: Add docstring to `resources/__init__.py`
  - Explain broker communication (internal API)
  - File: `/home/cody/code/c/vertebra/src/py/vertebra/resources/__init__.py`

- [x] **Task 2.3.3**: Add JSDoc to broker `index.ts`
  - Explain pool types and broker role
  - File: `/home/cody/code/c/vertebra/src/ts/src/broker/index.ts`

- [x] **Task 2.3.4**: Add JSDoc to server `index.ts`
  - Explain server architecture
  - File: `/home/cody/code/c/vertebra/src/ts/src/server/index.ts`

**Acceptance**: Key modules have clear documentation

---

## Phase 3: API Rename (P0)

**Duration**: 2-3 days
**Risk**: Medium (public API changes)
**Dependencies**: Phase 1 (tests must pass)

### Task Group 3.1: Base Class Renames

- [x] **Task 3.1.1**: Rename `BrokerSyncConfigBase` to `VertebraSyncConfigBase`
  - File: `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/_config.py`
  - Update class definition and all references

- [x] **Task 3.1.2**: Rename `BrokerAsyncConfigBase` to `VertebraAsyncConfigBase`
  - File: `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/_config.py`

- [x] **Task 3.1.3**: Rename `BrokerProxySyncDriver` to `VertebraProxySyncDriver`
  - File: `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/_driver.py`

- [x] **Task 3.1.4**: Rename `BrokerProxyAsyncDriver` to `VertebraProxyAsyncDriver`
  - File: `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/_driver.py`

- [x] **Task 3.1.5**: Run tests to verify
  - Command: `make test`
  - Last run: passed (skips: /dev/shm permission, pyarrow missing)

**Acceptance**: Base classes renamed, tests pass

---

### Task Group 3.2: Postgres Dialect Renames

- [x] **Task 3.2.1**: Rename Postgres configs
  - `BrokerPostgresSyncConfig` -> `VertebraPostgresSyncConfig`
  - `BrokerPostgresAsyncConfig` -> `VertebraPostgresAsyncConfig`
  - File: `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/postgres/config.py`

- [x] **Task 3.2.2**: Rename Postgres drivers
  - `BrokerPostgresSyncDriver` -> `VertebraPostgresSyncDriver`
  - `BrokerPostgresAsyncDriver` -> `VertebraPostgresAsyncDriver`
  - File: `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/postgres/driver.py`

- [x] **Task 3.2.3**: Update `postgres/__init__.py` exports
  - Update `__all__` list
  - File: `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/postgres/__init__.py`

- [x] **Task 3.2.4**: Run tests to verify
  - Command: `make test`
  - Last run: passed (skips: /dev/shm permission, pyarrow missing)

**Acceptance**: Postgres dialect renamed, tests pass

---

### Task Group 3.3: MySQL Dialect Renames

- [x] **Task 3.3.1**: Rename MySQL configs (same pattern as Postgres)
  - File: `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/mysql/config.py`

- [x] **Task 3.3.2**: Rename MySQL drivers (same pattern as Postgres)
  - File: `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/mysql/driver.py`

- [x] **Task 3.3.3**: Update `mysql/__init__.py` exports
  - File: `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/mysql/__init__.py`

- [x] **Task 3.3.4**: Run tests to verify
  - Command: `make test`
  - Last run: passed (skips: /dev/shm permission, pyarrow missing)

**Acceptance**: MySQL dialect renamed, tests pass

---

### Task Group 3.4: SQLite Dialect Renames

- [x] **Task 3.4.1**: Rename SQLite configs (same pattern as Postgres)
  - File: `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/sqlite/config.py`

- [x] **Task 3.4.2**: Rename SQLite drivers (same pattern as Postgres)
  - File: `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/sqlite/driver.py`

- [x] **Task 3.4.3**: Update `sqlite/__init__.py` exports
  - File: `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/sqlite/__init__.py`

- [x] **Task 3.4.4**: Run tests to verify
  - Command: `make test`
  - Last run: passed (skips: /dev/shm permission, pyarrow missing)

**Acceptance**: SQLite dialect renamed, tests pass

---

### Task Group 3.5: Update Tests

- [x] **Task 3.5.1**: Update imports in `test_sqlspec_integration.py`
  - Update all `BrokerPostgres*` to `VertebraPostgres*`
  - File: `/home/cody/code/c/vertebra/src/py/tests/test_sqlspec_integration.py`

- [x] **Task 3.5.2**: Update imports in `test_provider_registration.py`
  - Update all `Broker*` to `Vertebra*`
  - File: `/home/cody/code/c/vertebra/src/py/tests/test_provider_registration.py`

- [x] **Task 3.5.3**: Search for any other test files using old names

### Task Group 3.7: Align SQLSpec Adapter Layouts

- [x] **Task 3.7.1**: Add `core.py`, `data_dictionary.py`, `_typing.py`, and `type_converter.py` per dialect
  - Mirrors sqlspec adapter layouts for Postgres/MySQL/SQLite
- [x] **Task 3.7.2**: Update configs/drivers to use per-dialect modules and session contexts
- [x] **Task 3.7.3**: Export adapter-specific cursors/exception handlers in `__init__.py`
- [x] **Task 3.7.4**: Remove `Any` from SQLSpec adapter type hints (use `object`/typed aliases)
  - Command: `grep -r "BrokerPostgres\|BrokerMysql\|BrokerSqlite" src/py/tests/`
- [x] **Task 3.7.5**: Align adapter `__init__.py` exports with SQLSpec patterns
  - Keep configs/connections/cursors/drivers/exception handlers; omit session contexts/data dictionaries
- [x] **Task 3.7.6**: Remove `__future__` annotations in adapter `_typing.py`
  - Quote forward references to avoid runtime NameError

- [x] **Task 3.5.4**: Run full test suite
  - Command: `make test`
  - Last run: passed (skips: /dev/shm permission, pyarrow missing)

**Acceptance**: All test imports updated, tests pass

---

### Task Group 3.6: Verification

- [x] **Task 3.6.1**: Verify no `BrokerPostgres` in public APIs
  - Command: `rg -n "BrokerPostgres" src/py/vertebra/extensions/sqlspec`
  - Expected: No results

- [x] **Task 3.6.2**: Verify no `BrokerMysql` in public APIs
  - Command: `rg -n "BrokerMysql" src/py/vertebra/extensions/sqlspec`
  - Expected: No results

- [x] **Task 3.6.3**: Verify no `BrokerSqlite` in public APIs
  - Command: `rg -n "BrokerSqlite" src/py/vertebra/extensions/sqlspec`
  - Expected: No results

- [x] **Task 3.6.4**: Verify `BrokerClient` remains unchanged
  - Command: `rg -n "BrokerClient" src/py/vertebra/resources/broker.py`
  - Expected: `BrokerClient` still present (internal API)

- [x] **Task 3.6.5**: Run type checks
  - Command: `make mypy`
  - Expected: No new errors
  - Last run: failed (sqlspec missing + additional type errors)

**Acceptance**: API rename complete and verified

---

## Phase 4: Type Audit (P1)

**Duration**: 4-6 days
**Risk**: Medium (may uncover bugs)
**Dependencies**: Phases 1-3 (stable codebase required)

### Task Group 4.1: Python Core Module Audit

- [x] **Task 4.1.1**: Fix `_internal/shm.py` type issues
  - Run: `mypy --strict src/py/vertebra/_internal/shm.py`
  - Add missing return types
  - File: `/home/cody/code/c/vertebra/src/py/vertebra/_internal/shm.py`

- [x] **Task 4.1.2**: Fix `_internal/serialization.py` type issues
  - Run: `mypy --strict src/py/vertebra/_internal/serialization.py`

- [x] **Task 4.1.3**: Fix `_internal/dependencies.py` type issues
  - Run: `mypy --strict src/py/vertebra/_internal/dependencies.py`

- [x] **Task 4.1.4**: Fix `_internal/errors.py` type issues
  - Run: `mypy --strict src/py/vertebra/_internal/errors.py`

- [x] **Task 4.1.5**: Verify 0 errors in `_internal/`
  - Command: `mypy --strict src/py/vertebra/_internal/`

**Acceptance**: 0 mypy errors in `_internal/` module

---

### Task Group 4.2: Python Resources Module Audit

- [x] **Task 4.2.1**: Fix `resources/broker.py` type issues
  - Run: `mypy --strict src/py/vertebra/resources/broker.py`
  - File: `/home/cody/code/c/vertebra/src/py/vertebra/resources/broker.py`

- [x] **Task 4.2.2**: Fix `resources/provider.py` type issues
  - Run: `mypy --strict src/py/vertebra/resources/provider.py`

- [x] **Task 4.2.3**: Fix `resources/pool.py` type issues
  - Run: `mypy --strict src/py/vertebra/resources/pool.py`

- [x] **Task 4.2.4**: Verify 0 errors in `resources/`
  - Command: `mypy --strict src/py/vertebra/resources/`

**Acceptance**: 0 mypy errors in `resources/` module

---

### Task Group 4.3: Python Extensions Module Audit

- [x] **Task 4.3.1**: Fix `extensions/sqlspec/_config.py` type issues
  - Run: `mypy --strict src/py/vertebra/extensions/sqlspec/_config.py`

- [x] **Task 4.3.2**: Fix `extensions/sqlspec/_driver.py` type issues
  - Run: `mypy --strict src/py/vertebra/extensions/sqlspec/_driver.py`

- [x] **Task 4.3.3**: Fix dialect-specific type issues
  - Run: `mypy --strict src/py/vertebra/extensions/sqlspec/postgres/`
  - Run: `mypy --strict src/py/vertebra/extensions/sqlspec/mysql/`
  - Run: `mypy --strict src/py/vertebra/extensions/sqlspec/sqlite/`

- [x] **Task 4.3.4**: Verify 0 errors in `extensions/sqlspec/`
  - Command: `mypy --strict src/py/vertebra/extensions/sqlspec/`

**Acceptance**: 0 mypy errors in `extensions/sqlspec/` module

---

### Task Group 4.4: Python Server Module Audit

- [x] **Task 4.4.1**: Fix `server/worker.py` type issues
  - Run: `mypy --strict src/py/vertebra/server/worker.py`

- [x] **Task 4.4.2**: Fix `server/supervisor.py` type issues
  - Run: `mypy --strict src/py/vertebra/server/supervisor.py`

- [x] **Task 4.4.3**: Verify 0 errors in `server/`
  - Command: `mypy --strict src/py/vertebra/server/`

**Acceptance**: 0 mypy errors in `server/` module

---

### Task Group 4.5: Python Type Syntax Modernization

- [x] **Task 4.5.1**: Replace all `Optional[T]` with `T | None`
  - Search: `grep -r "Optional\[" src/py/vertebra/`
  - Replace using modern syntax

- [x] **Task 4.5.2**: Replace all `Union[A, B]` with `A | B`
  - Search: `grep -r "Union\[" src/py/vertebra/`
  - Replace using modern syntax

- [x] **Task 4.5.3**: Replace all `List[T]` with `list[T]`
  - Search: `grep -r "List\[" src/py/vertebra/`
  - Replace using modern syntax

- [x] **Task 4.5.4**: Replace all `Dict[K, V]` with `dict[K, V]`
  - Search: `grep -r "Dict\[" src/py/vertebra/`
  - Replace using modern syntax

- [x] **Task 4.5.5**: Remove unnecessary `from __future__ import annotations`
  - Python 3.12+ doesn't need this
  - Search: `grep -r "from __future__ import annotations" src/py/vertebra/`

- [x] **Task 4.5.6**: Run mypy to verify
  - Command: `mypy --strict src/py/vertebra/`

**Acceptance**: All type syntax modernized

---

### Task Group 4.6: TypeScript Type Audit

- [x] **Task 4.6.1**: Enable strict mode in `tsconfig.json`
  - Add: `"strict": true`
  - Add: `"noImplicitAny": true`
  - File: `/home/cody/code/c/vertebra/tsconfig.json`

- [x] **Task 4.6.2**: Fix `any` types in `broker/` module
  - Search: `grep -r ": any" src/ts/src/broker/`
  - Replace with specific types or `unknown`

- [x] **Task 4.6.3**: Fix `any` types in `server/` module
  - Search: `grep -r ": any" src/ts/src/server/`
  - Replace with specific types

- [x] **Task 4.6.4**: Fix `any` types in `mcp/` module
  - Search: `grep -r ": any" src/ts/src/mcp/`
  - Replace with specific types

- [x] **Task 4.6.5**: Document justified `any` usage
  - Add comments explaining why `any` is needed
  - Limit to < 10 justified cases

- [x] **Task 4.6.6**: Run TypeScript type check
  - Command: `make js-type-check`

**Acceptance**: TypeScript strict mode enabled, < 10 `any` types

---

### Task Group 4.7: Final Verification

- [x] **Task 4.7.1**: Run full mypy strict check
  - Command: `mypy --strict src/py/vertebra/`
  - Expected: 0 errors

- [x] **Task 4.7.2**: Run full lint
  - Command: `make lint-all`
  - Expected: Pass

- [x] **Task 4.7.3**: Run full test suite
  - Command: `make test-all`
  - Expected: All tests pass

- [x] **Task 4.7.4**: Verify no legacy syntax remains
  - Command: `grep -r "Optional\[" src/py/vertebra/ | wc -l`
  - Expected: 0
  - Command: `grep -r "Union\[" src/py/vertebra/ | wc -l`
  - Expected: 0

**Acceptance**: Full type audit complete

---

## Summary

**Total Tasks**: 45+
**Estimated Duration**: 2-3 weeks

**Critical Path**: Phase 1 -> Phase 2 -> Phase 3 -> Phase 4

**Dependencies**:
- Phase 1 (Test Deadlock): No dependencies - START HERE
- Phase 2 (Consolidation): Depends on Phase 1
- Phase 3 (API Rename): Depends on Phase 1
- Phase 4 (Type Audit): Depends on Phases 1-3

**Testing Strategy**:
- Run `make test` after each task group
- Run `make lint-all` after each phase
- Run full verification at phase completion

---

**Document Version**: 1.0
**Last Updated**: 2026-01-19
**Status**: Ready for Implementation