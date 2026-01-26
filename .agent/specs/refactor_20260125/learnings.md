# Learnings - Python Package Restructuring

> Patterns and discoveries from the refactoring effort.

---

## [2026-01-26 12:30] - Phase 1 Tasks 1-6: Create vertebra.common

- **Implemented:** Created `vertebra.common` package with submodules for exceptions, protocols, utils, and serializers
- **Files created:**
  - `src/py/vertebra/common/__init__.py`
  - `src/py/vertebra/common/exceptions.py`
  - `src/py/vertebra/common/protocols.py`
  - `src/py/vertebra/common/utils/*.py`
  - `src/py/vertebra/common/serializers/*.py`
- **Commit:** 7d01447
- **Learnings:**
  - **Pattern: Backwards-Compatible Re-exports**: When moving modules, keep the original location as a re-export shim. This allows existing code to work without changes while enabling gradual migration.
    ```python
    # vertebra/exceptions.py (old location)
    from vertebra.common.exceptions import MissingDependencyError, ...
    __all__ = (...)  # re-export everything
    ```
  - **Gotcha: TypeBridge is a Feature, Not a Utility**: The `types/` package contains TypeBridge (TypeScript codegen) which is a feature, not base types. Decision: rename to `vertebra.codegen` rather than putting in `common.types`.
  - **Gotcha: PyO3 Native Bindings Location**: `_typing.py` and `_vertebra.py` must stay at root level for PyO3/maturin compatibility. Native extension modules can't be in subdirectories.
  - **Pattern: Import Update Order**: When moving modules with internal dependencies:
    1. Copy files to new location
    2. Update imports WITHIN the copied files first
    3. Create backwards-compatible re-exports at old locations
    4. Verify tests pass
    5. Update imports in rest of codebase (can be done gradually)

---

## [2026-01-26 13:00] - Phase 1 Refinement: Remove Re-exports

- **Decision:** User decided to skip backwards-compatible re-exports and migrate all imports at once
- **Files deleted:**
  - `src/py/vertebra/exceptions.py` (re-export shim)
  - `src/py/vertebra/protocols.py` (re-export shim)
  - `src/py/vertebra/serializers/` (entire directory)
- **Files updated:** 41 files with import path changes
- **Commit:** c972930
- **Learnings:**
  - **Pattern: Atomic Migration Over Gradual**: For internal codebases, a single atomic migration (update all imports at once) is cleaner than maintaining re-export shims. Use re-exports only for public APIs where external consumers need deprecation periods.
  - **Pattern: Keep utils at top level**: `utils/`, `typing.py`, `_typing.py` remain at package root for convenience and PyO3 compatibility. Only "foundational" modules like exceptions/protocols/serializers go into `common/`.

---
