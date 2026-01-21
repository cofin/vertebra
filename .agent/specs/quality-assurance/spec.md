# PRD: Vertebra Infrastructure & Quality Consolidation

**PRD ID:** QUALITY-001
**Version:** 1.0
**Date:** 2026-01-19
**Status:** Ready for Implementation
**Priority:** P0 - Critical (Blocking 1.0 Release)
**Complexity:** High
**Estimated Duration:** 2-3 weeks

---

## Executive Summary

This PRD consolidates **all Infrastructure & Quality work** required to unblock Vertebra 1.0 development and achieve production-ready code quality. The work is divided into four major areas that must be completed sequentially:

1. **Test Deadlock Fix** (P0) - Unblock development by fixing hanging test suite
2. **Codebase Consolidation Phase 4** (P1) - Complete file renames and documentation
3. **API Rename** (P0) - Stabilize public APIs with `Vertebra*` prefix
4. **Type Audit** (P1) - Fix 120+ mypy errors and modernize type syntax

**Current State:**
- Test suite hangs at ~2% progress, blocking CI/CD and development
- Codebase consolidation is 75% complete (Phases 1-3 done, Phase 4 pending)
- Public SQLSpec APIs use `Broker*` prefix (implementation detail leakage)
- 120+ mypy errors across codebase
- TypeScript has multiple `any` type leaks

**Business Impact:**
- Developers cannot run tests to verify changes
- CI/CD pipeline fails with timeout
- Public API naming is inconsistent with branding
- Type safety gaps increase bug risk
- Codebase maintainability suffers

**Success Criteria:**
- `make test` completes within 60 seconds without hanging
- All public APIs use `Vertebra*` prefix
- 0 mypy errors in strict mode
- TypeScript strict mode enabled with minimal `any` types
- Documentation reflects actual codebase structure

---

## 1. Intelligence Context

### 1.1 Complexity Assessment

This is a **HIGH COMPLEXITY** multi-phase refactoring spanning:

- **Scope:** 65+ Python files, 36+ TypeScript files, test infrastructure, documentation
- **Impact Areas:** Test framework, SQLSpec public API, type system, naming conventions
- **Risk Level:** Medium-High (test fixes are critical; API renames affect public interface)
- **Phases:** 4 major phases with 45+ individual tasks
- **Dependencies:** Sequential execution required (tests must pass before type audit)

### 1.2 Similar Patterns Referenced

| Pattern | Location | Usage |
|---------|----------|-------|
| Test cleanup fixtures | `src/py/tests/conftest.py:55-76` | Session-scoped cleanup with pattern matching |
| Unique name generation | Python stdlib `uuid.uuid4()` | Per-test isolation |
| pytest-timeout | Common pytest pattern | Prevent hanging tests |
| Type modernization | Python 3.12+ PEP 604 | `T \| None` instead of `Optional[T]` |
| API consistency | SQLSpec base classes | Config/Driver naming patterns |

### 1.3 Detected Stack and Commands

**Languages:** Python 3.12+, TypeScript (Bun), Rust (supporting role)

**Test Commands:**
```bash
make test             # Python tests (currently hangs)
make js-test          # Bun/TypeScript tests
make test-all         # All tests (blocked by Python)
uv run pytest src/py/tests/test_foo.py::test_bar -v
```

**Lint Commands:**
```bash
make lint             # Format all (ruff + biome + rustfmt)
make lint-all         # Full lint + type checks + pre-commit
mypy src/py/vertebra  # Python type checking (120+ errors)
```

**Build System:** Makefile with uv (Python), Bun (TypeScript), Cargo (Rust)

---

## 2. Problem Statement

### 2.1 User Problems Being Solved

**Problem 1: Test Deadlock (P0 - BLOCKING)**

The Python test suite hangs indefinitely when running `make test`, preventing:
- Developers from verifying changes before commits
- CI/CD pipelines from completing
- Confident refactoring and feature development

**Root Causes:**
1. Stale shared memory from previous test runs with corrupt state
2. Non-unique shared memory names (`vertebra-broker-test` hardcoded in multiple tests)
3. Missing timeouts on blocking IPC operations (futex waits can hang forever)
4. Incomplete cleanup patterns (don't catch all naming conventions)

**Symptoms:**
- Full test suite hangs at ~2% progress with no output
- Individual test files pass when run in isolation
- Process requires SIGKILL (exit code 137) to terminate
- Stale files in `/dev/shm/` after test runs

**Problem 2: Incomplete Consolidation (P1)**

Codebase consolidation is 75% complete but Phase 4 remains:
- TypeScript worker files have confusing names (`worker-pool.ts` vs actual pools)
- CLAUDE.md doesn't reflect completed structural changes
- Module purposes not documented

**Problem 3: Public API Inconsistency (P0)**

SQLSpec extension uses `Broker*` prefix for public APIs:
- `BrokerPostgresSyncConfig` exposes implementation detail
- Brand inconsistency (should be `Vertebra*`)
- User confusion about what "Broker" means
- Affects public-facing documentation and examples

**Problem 4: Type Safety Gaps (P1)**

120+ mypy errors across codebase:
- Missing return type annotations
- `Any` type leaks
- Legacy type syntax (`Optional[T]`, `Union[A, B]`)
- Unnecessary `from __future__ import annotations` in Python 3.12+ code
- TypeScript `any` types without justification

### 2.2 Business Value

**Developer Productivity:**
- Unblocked test suite enables rapid iteration
- Clear naming reduces onboarding time
- Type safety catches bugs at development time

**Code Quality:**
- 0 mypy errors = production-ready type safety
- Modern syntax improves maintainability
- Consistent APIs reduce cognitive load

**1.0 Release Readiness:**
- Stable public APIs prevent breaking changes post-1.0
- Professional naming aligns with brand
- Test reliability enables confident releases

### 2.3 Success Criteria

| Criterion | Target | Measurement |
|-----------|--------|-------------|
| Test completion | < 60 seconds | `time make test` |
| Test pass rate | 100% | All 44+ tests passing |
| Stale shared memory | 0 files after run | `ls /dev/shm/ \| grep vertebra \| wc -l` |
| Consecutive test runs | 5 runs without failure | Loop test script |
| API consistency | 100% `Vertebra*` | `grep -r "BrokerPostgres" src/py/` |
| mypy errors | 0 in strict mode | `mypy --strict src/py/vertebra/` |
| TypeScript strict | Enabled with <10 `any` | `tsc --strict` |
| Documentation accuracy | 100% match | Manual review of CLAUDE.md |

---

## 3. Acceptance Criteria

### 3.1 Phase 1: Test Deadlock Fix (P0)

**Functional Requirements:**
- [ ] **AC-TEST-1:** `make test` completes within 60 seconds without hanging
- [ ] **AC-TEST-2:** All existing tests pass (44+ tests)
- [ ] **AC-TEST-3:** No stale `/dev/shm/` files after test run (0 matching `test-*` or `*vertebra*`)
- [ ] **AC-TEST-4:** Running tests 5 times consecutively completes without failure
- [ ] **AC-TEST-5:** Tests work with `pytest-xdist -n 4` parallel execution

**Non-Functional Requirements:**
- [ ] **AC-TEST-6:** No changes to production code (only `src/py/tests/**`)
- [ ] **AC-TEST-7:** Backward compatible - tests still work when run individually
- [ ] **AC-TEST-8:** Minimal runtime overhead (< 5% increase)

**Edge Cases:**
- [ ] **AC-TEST-9:** Cleanup works even when tests fail mid-execution
- [ ] **AC-TEST-10:** Works when shared memory bindings unavailable (graceful skip)
- [ ] **AC-TEST-11:** Works on systems without `/dev/shm` (macOS, Windows)

### 3.2 Phase 2: Codebase Consolidation Phase 4 (P1)

**File Renames:**
- [ ] **AC-CONS-1:** `src/ts/src/server/worker-pool.ts` renamed to `worker-connections.ts`
- [ ] **AC-CONS-2:** `src/ts/src/server/worker-manager.ts` renamed to `process-manager.ts`
- [ ] **AC-CONS-3:** All import statements updated in `src/ts/src/server/index.ts`

**Documentation Updates:**
- [ ] **AC-CONS-4:** CLAUDE.md Directory Layout reflects merged `storage/` module
- [ ] **AC-CONS-5:** CLAUDE.md documents `resources/broker.py` purpose
- [ ] **AC-CONS-6:** CLAUDE.md documents TypeScript naming conventions
- [ ] **AC-CONS-7:** Module docstrings added to key modules

**Testing:**
- [ ] **AC-CONS-8:** `make js-test` passes after renames
- [ ] **AC-CONS-9:** `make js-type-check` passes
- [ ] **AC-CONS-10:** No broken imports in TypeScript

### 3.3 Phase 3: API Rename (P0)

**Python API Changes:**
- [ ] **AC-API-1:** All `BrokerSyncConfigBase` renamed to `VertebraSyncConfigBase`
- [ ] **AC-API-2:** All `BrokerAsyncConfigBase` renamed to `VertebraAsyncConfigBase`
- [ ] **AC-API-3:** All `BrokerProxySyncDriver` renamed to `VertebraProxySyncDriver`
- [ ] **AC-API-4:** All `BrokerProxyAsyncDriver` renamed to `VertebraProxyAsyncDriver`
- [ ] **AC-API-5:** Postgres configs use `VertebraPostgres*` prefix
- [ ] **AC-API-6:** MySQL configs use `VertebraMysql*` prefix
- [ ] **AC-API-7:** SQLite configs use `VertebraSqlite*` prefix

**Internal vs Public:**
- [ ] **AC-API-8:** Internal `BrokerClient` remains unchanged (in `resources/broker.py`)
- [ ] **AC-API-9:** Only public SQLSpec APIs renamed

**Testing:**
- [ ] **AC-API-10:** All tests pass with new names
- [ ] **AC-API-11:** No mypy/pyright errors introduced
- [ ] **AC-API-12:** Example usage works with new names

### 3.4 Phase 4: Type Audit (P1)

**Python Type Safety:**
- [ ] **AC-TYPE-1:** 0 mypy errors in strict mode for `src/py/vertebra/`
- [ ] **AC-TYPE-2:** All functions have return type annotations
- [ ] **AC-TYPE-3:** All `Optional[T]` replaced with `T | None`
- [ ] **AC-TYPE-4:** All `Union[A, B]` replaced with `A | B`
- [ ] **AC-TYPE-5:** All `List[T]` replaced with `list[T]`
- [ ] **AC-TYPE-6:** All `Dict[K, V]` replaced with `dict[K, V]`
- [ ] **AC-TYPE-7:** No `from __future__ import annotations` in Python 3.12+ files

**TypeScript Type Safety:**
- [ ] **AC-TYPE-8:** `tsc --strict` enabled and passing
- [ ] **AC-TYPE-9:** All `any` types documented with justification or replaced
- [ ] **AC-TYPE-10:** No implicit `any` in broker module
- [ ] **AC-TYPE-11:** No implicit `any` in server module

---

## 4. Technical Architecture

### 4.1 Test Isolation Strategy

**Three complementary approaches for robustness:**

1. **Isolation** - Give each test unique shared memory names
2. **Timeouts** - Fail tests that exceed time limits instead of hanging
3. **Cleanup** - Comprehensively track and clean created shared memory

**Architecture:**

```
+-------------------------------------------------------------+
|                     Test Execution                           |
+-------------------------------------------------------------+
|                                                              |
|  +-------------+    +-------------+    +-------------+       |
|  |  conftest   |    |   unique    |    |   pytest    |       |
|  |  fixtures   |    |    names    |    |   timeout   |       |
|  +------+------+    +------+------+    +------+------+       |
|         |                  |                  |              |
|         v                  v                  v              |
|  +---------------------------------------------+             |
|  |              Per-Test Shared Memory          |            |
|  |     vertebra-test-abc12345.control           |            |
|  |     vertebra-test-def67890.client.test.req   |            |
|  +---------------------------------------------+             |
|                          |                                   |
|                          v                                   |
|  +---------------------------------------------+             |
|  |              Cleanup on Exit                 |            |
|  |     - Track created names during test        |            |
|  |     - shm_unlink() all tracked names         |            |
|  |     - Fallback glob cleanup for orphans      |            |
|  +---------------------------------------------+             |
|                                                              |
+-------------------------------------------------------------+
```

### 4.2 API Rename Mapping

**Complete Rename Table:**

| Before (Broker*) | After (Vertebra*) | File |
|------------------|-------------------|------|
| `BrokerSyncConfigBase` | `VertebraSyncConfigBase` | `_config.py` |
| `BrokerAsyncConfigBase` | `VertebraAsyncConfigBase` | `_config.py` |
| `BrokerProxySyncDriver` | `VertebraProxySyncDriver` | `_driver.py` |
| `BrokerProxyAsyncDriver` | `VertebraProxyAsyncDriver` | `_driver.py` |
| `BrokerPostgresSyncConfig` | `VertebraPostgresSyncConfig` | `postgres/config.py` |
| `BrokerPostgresAsyncConfig` | `VertebraPostgresAsyncConfig` | `postgres/config.py` |
| `BrokerPostgresSyncDriver` | `VertebraPostgresSyncDriver` | `postgres/driver.py` |
| `BrokerPostgresAsyncDriver` | `VertebraPostgresAsyncDriver` | `postgres/driver.py` |
| `BrokerMysqlSyncConfig` | `VertebraMysqlSyncConfig` | `mysql/config.py` |
| `BrokerMysqlAsyncConfig` | `VertebraMysqlAsyncConfig` | `mysql/config.py` |
| `BrokerMysqlSyncDriver` | `VertebraMysqlSyncDriver` | `mysql/driver.py` |
| `BrokerMysqlAsyncDriver` | `VertebraMysqlAsyncDriver` | `mysql/driver.py` |
| `BrokerSqliteSyncConfig` | `VertebraSqliteSyncConfig` | `sqlite/config.py` |
| `BrokerSqliteAsyncConfig` | `VertebraSqliteAsyncConfig` | `sqlite/config.py` |
| `BrokerSqliteSyncDriver` | `VertebraSqliteSyncDriver` | `sqlite/driver.py` |
| `BrokerSqliteAsyncDriver` | `VertebraSqliteAsyncDriver` | `sqlite/driver.py` |

**Unchanged (Internal APIs):**

| Name | Location | Reason |
|------|----------|--------|
| `BrokerClient` | `resources/broker.py` | Internal IPC client, not public API |
| `BrokerClientConfig` | `resources/broker.py` | Internal configuration |

### 4.3 Type Modernization Pattern

**Python Type Modernization:**

```python
# Before (Python 3.9 syntax):
from typing import Optional, Union, List, Dict
from __future__ import annotations

def foo(x: Optional[str] = None) -> Optional[int]:
    items: List[str] = []
    mapping: Dict[str, int] = {}
    result: Union[int, str] = 42
    return result

# After (Python 3.12 syntax):
def foo(x: str | None = None) -> int | None:
    items: list[str] = []
    mapping: dict[str, int] = {}
    result: int | str = 42
    return result
```

---

## 5. Implementation Phases

### Phase 1: Test Deadlock Fix (P0 - BLOCKING)

**Duration:** 2-3 days
**Risk:** Low (test-only changes)
**Dependencies:** None

See `tasks.md` for detailed task breakdown.

### Phase 2: Codebase Consolidation Phase 4 (P1)

**Duration:** 1-2 days
**Risk:** Low (mostly renames and docs)
**Dependencies:** Phase 1 (tests must pass)

See `tasks.md` for detailed task breakdown.

### Phase 3: API Rename (P0)

**Duration:** 2-3 days
**Risk:** Medium (public API changes)
**Dependencies:** Phase 1 (tests must pass)

See `tasks.md` for detailed task breakdown.

### Phase 4: Type Audit (P1)

**Duration:** 4-6 days
**Risk:** Medium (may uncover bugs)
**Dependencies:** Phases 1-3 (stable codebase required)

See `tasks.md` for detailed task breakdown.

---

## 6. Testing Strategy

### 6.1 Verification Commands

**Phase 1 Verification:**
```bash
# Test completion time
time make test  # Should be < 60 seconds

# Verify no stale shared memory
ls /dev/shm/ | grep -E "test-|vertebra" | wc -l  # Should be 0

# Repeated runs
for i in {1..5}; do
    rm -rf /dev/shm/test-* /dev/shm/*vertebra*
    make test || exit 1
done

# Parallel execution
uv run pytest src/py/tests -n 4 --timeout=30
```

**Phase 3 Verification:**
```bash
# Search for old names (should find nothing)
grep -r "BrokerPostgres" src/py/vertebra/extensions/sqlspec/
grep -r "BrokerMysql" src/py/vertebra/extensions/sqlspec/
grep -r "BrokerSqlite" src/py/vertebra/extensions/sqlspec/

# Verify internal API unchanged
grep -r "BrokerClient" src/py/vertebra/resources/broker.py

# All tests
make test
make lint-all
```

**Phase 4 Verification:**
```bash
# Strict type checking
mypy --strict src/py/vertebra/

# Search for legacy syntax (should find nothing)
grep -r "Optional\[" src/py/vertebra/
grep -r "Union\[" src/py/vertebra/

# TypeScript strict
cd src/ts && bun run type-check

# All linting
make lint-all
```

---

## 7. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Test fixes break existing tests | Low | Medium | Incremental changes with verification after each file |
| API rename breaks user code | High | High | Pre-1.0, no users yet; document in migration guide |
| Type audit uncovers runtime bugs | Medium | High | Fix bugs discovered; consider it a quality win |
| TypeScript strict mode too restrictive | Low | Low | Document justified `any` usage with comments |
| Consolidation breaks imports | Low | Medium | Update all imports atomically; verify with tests |

---

## 8. Migration Guide

### 8.1 API Changes (Phase 3)

```python
# Before:
from vertebra.extensions.sqlspec.postgres import BrokerPostgresSyncConfig

config = BrokerPostgresSyncConfig(pool="main")

# After:
from vertebra.extensions.sqlspec.postgres import VertebraPostgresSyncConfig

config = VertebraPostgresSyncConfig(pool="main")
```

All `Broker*` public APIs renamed to `Vertebra*`:
- `BrokerPostgresSyncConfig` -> `VertebraPostgresSyncConfig`
- `BrokerPostgresAsyncConfig` -> `VertebraPostgresAsyncConfig`
- `BrokerMysqlSyncConfig` -> `VertebraMysqlSyncConfig`
- `BrokerMysqlAsyncConfig` -> `VertebraMysqlAsyncConfig`
- `BrokerSqliteSyncConfig` -> `VertebraSqliteSyncConfig`
- `BrokerSqliteAsyncConfig` -> `VertebraSqliteAsyncConfig`

Internal `BrokerClient` remains unchanged.

---

## 9. Rollback Plan

If issues arise during or after implementation:

**Phase 1 (Test Deadlock Fix):**
1. Revert `pyproject.toml` pytest-timeout config
2. Revert `conftest.py` changes
3. Delete `tests/helpers/` directory
4. Revert individual test file changes

**Phase 2 (Consolidation):**
1. Rename files back to original names
2. Revert import changes
3. Revert CLAUDE.md changes

**Phase 3 (API Rename):**
1. Revert all `Vertebra*` -> `Broker*` changes
2. Revert test import changes
3. All changes are in SQLSpec extension only (localized)

**Phase 4 (Type Audit):**
1. Disable strict mode in mypy config
2. Revert type annotation changes
3. Re-enable legacy syntax if needed

All changes are tracked in version control and can be reverted commit-by-commit.

---

## 10. File Paths Reference

### Files Modified in Phase 1:
- `/home/cody/code/c/vertebra/pyproject.toml`
- `/home/cody/code/c/vertebra/src/py/tests/conftest.py`
- `/home/cody/code/c/vertebra/src/py/tests/helpers/__init__.py` (NEW)
- `/home/cody/code/c/vertebra/src/py/tests/helpers/shm.py` (NEW)
- `/home/cody/code/c/vertebra/src/py/tests/test_broker_client.py`
- `/home/cody/code/c/vertebra/src/py/tests/test_broker_pool.py`
- `/home/cody/code/c/vertebra/src/py/tests/test_broker_multi_client.py`
- `/home/cody/code/c/vertebra/src/py/tests/test_asgi_worker.py`
- `/home/cody/code/c/vertebra/src/py/tests/test_ipc_channel.py`
- `/home/cody/code/c/vertebra/src/py/tests/test_ipc_integration.py`

### Files Modified in Phase 2:
- `/home/cody/code/c/vertebra/src/ts/src/server/worker-pool.ts` -> `worker-connections.ts`
- `/home/cody/code/c/vertebra/src/ts/src/server/worker-manager.ts` -> `process-manager.ts`
- `/home/cody/code/c/vertebra/src/ts/src/server/index.ts`
- `/home/cody/code/c/vertebra/CLAUDE.md`
- `/home/cody/code/c/vertebra/src/py/vertebra/storage/__init__.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/resources/__init__.py`
- `/home/cody/code/c/vertebra/src/ts/src/broker/index.ts`

### Files Modified in Phase 3:
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/_config.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/_driver.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/postgres/config.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/postgres/driver.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/postgres/__init__.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/mysql/config.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/mysql/driver.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/mysql/__init__.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/sqlite/config.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/sqlite/driver.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/sqlite/__init__.py`
- `/home/cody/code/c/vertebra/src/py/tests/test_sqlspec_integration.py`
- `/home/cody/code/c/vertebra/src/py/tests/test_provider_registration.py`

### Files Modified in Phase 4:
- ~50+ Python files across all modules (type annotations)
- ~30+ TypeScript files in broker/server/mcp (type annotations)
- `/home/cody/code/c/vertebra/tsconfig.json`
- `/home/cody/code/c/vertebra/pyproject.toml` (mypy config)

---

## 11. Related Specifications

**Active Specifications:**
- `/home/cody/code/c/vertebra/specs/active/database/` - Database & ORM Integration
- `/home/cody/code/c/vertebra/specs/active/server/` - Server Readiness
- `/home/cody/code/c/vertebra/specs/active/distribution/` - Build & Distribution

**Archived References:**
- `/home/cody/code/c/vertebra/specs/archive/test-deadlock-fix/` - Original test deadlock analysis
- `/home/cody/code/c/vertebra/specs/archive/codebase-consolidation/` - Original consolidation spec

---

## 12. Approval

**Author:** Claude Code
**Date:** 2026-01-19

**Pending Reviews:**
- [ ] Engineering Lead - Technical review
- [ ] QA Review - Test strategy verification
- [ ] Documentation Review - CLAUDE.md accuracy
- [ ] Ready for Implementation

---

**Document Version:** 1.0
**Last Updated:** 2026-01-19
**Word Count:** ~5,000 words
**Status:** Completed

**Supporting Documents:**
- `tasks.md` - Detailed task breakdown
- `recovery.md` - Session recovery guide
- `research/plan.md` - Research and analysis
- `index.md` - Specification overview
