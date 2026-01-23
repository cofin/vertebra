# Test Suite Reorganization and Refactoring

## Overview
Reorganize the current flat test structure in `src/py/tests` into a hierarchical layout that mirrors the `vertebra` package structure while strictly separating unit and integration tests. This aims to reduce "test sprawl," improve maintainability, and clarify the scope of each test module.

## Research Reference
No prior research conducted

## Functional Requirements
### Must Have
- [ ] Split all existing tests into `unit/` and `integration/` directories.
- [ ] Mirror the `src/py/vertebra/` package structure within both test directories.
- [ ] Consolidate related tests into logical modules (e.g., merging multiple SQLAlchemy-related test files).
- [ ] Simplify filenames by removing redundant prefixes where the directory structure provides context.
- [ ] Ensure `pytest` discovery continues to work correctly after the move.
- [ ] Create `conftest.py` in `unit/` and `integration/` as well.
- [ ] Move globally shared fixtures to top-level `conftest.py`, move database fixtures to `integration/conftest.py`, and unit mocks to `unit/conftest.py`.

### Should Have
- [ ] Standardize test class and function naming conventions across the refactored files.

### Nice to Have
- [ ] Identify and tag tests that require specific external dependencies (Postgres, etc.) using pytest markers.

## Non-Functional Requirements
- **Maintainability:** The test structure should make it obvious where to add a new test for any given module.
- **Clarity:** Test file names should be concise and avoid redundancy with their parent directories.

## Technical Approach
### Recommended Implementation
1. **Directory Creation:** Create `src/py/tests/unit/` and `src/py/tests/integration/`.
2. **Structural Mapping:**
   - `vertebra/extensions/advanced_alchemy` -> `tests/[unit|integration]/extensions/advanced_alchemy/`
   - `vertebra/extensions/litestar` -> `tests/[unit|integration]/extensions/litestar/`
   - `vertebra/extensions/sqlspec` -> `tests/[unit|integration]/extensions/sqlspec/`
   - `vertebra/supervisor.py` -> `tests/[unit|integration]/test_supervisor.py` (or similar)
3. **Consolidation Strategy:**
   - `test_sqlalchemy_dialect.py`, `test_sqlalchemy_dbapi.py`, `test_sqlalchemy_integration.py` -> Consolidate into `integration/extensions/sqlalchemy/` or similar.
   - `test_sqlspec_arrow.py`, `test_sqlspec_integration.py`, `test_sqlspec_storage.py` -> Consolidate into `integration/extensions/sqlspec/`.
4. **Shared Assets:** Keep `conftest.py` in `src/py/tests/` to serve as a shared root for both suites.

### Files to Modify
- All files in `src/py/tests/*.py` (moving and refactoring).

## Acceptance Criteria
- [ ] `pytest src/py/tests/unit` runs all unit tests and passes.
- [ ] `pytest src/py/tests/integration` runs all integration tests and passes.
- [ ] The directory structure under `unit/` and `integration/` accurately reflects the `vertebra/` module layout.
- [ ] No redundant "sqlspec" or "sqlalchemy" prefixes remain in filenames where folders already identify the module.

## Out of Scope
- Adding new test coverage (this is strictly a refactor).
- Modifying the actual `vertebra` source code.

## Risk Assessment
### Identified Risks
| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Broken imports in tests | High | High | Update all relative/absolute imports within test files during the move. |
| pytest discovery failure | Medium | Medium | Verify discovery using `pytest --collect-only` before and after. |

### Recovery Strategy
**Rollback Trigger:** Test suite fails to run significantly or cannot be fixed within a single session.
**Rollback Steps:**
1. Revert moves and deletions via git.
2. Clean up any empty directories created.

**Safe Checkpoints:**
- After Phase 1: Unit tests moved and passing.
- After Phase 2: Integration tests moved and passing.
