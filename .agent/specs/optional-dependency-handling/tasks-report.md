# Task Checklist Report: optional-dependency-handling

- Generated: 2026-01-24
- Source: `specs/active/optional-dependency-handling/tasks.md`
- Total tasks: 22
- Done: 22
- Pending: 0

## Checklist

# Tasks: Optional Dependency Handling Standardization

## Phase 1: Foundation (Dependency Flags)

- [x] 1. Add ADVANCED_ALCHEMY_INSTALLED and SQLALCHEMY_INSTALLED to `vertebra/utils/dependencies.py`
- [x] 2. Update `vertebra/_typing.py` to re-export framework dependency flags
- [x] 3. Write unit tests for ensure_dependency and dependency_flag functions (currently none exist for `module_loader.py`)

## Phase 2: Litestar Extension Refactoring

- [x] 4. Refactor `vertebra/extensions/litestar/_plugin.py` - add ensure_dependency("litestar") at top (currently has direct imports with NO guards)
- [x] 5. Delete `vertebra/extensions/litestar/plugin.py` (unused duplicate implementation)
- [x] 6. Refactor `vertebra/extensions/litestar/advanced_alchemy.py` - remove object fallback, use ensure_dependency("advanced_alchemy")
- [x] 7. Refactor `vertebra/extensions/litestar/sqlspec.py` - remove object fallback, use ensure_dependency("sqlspec")
- [x] 8. Refactor `vertebra/extensions/litestar/sqlalchemy.py` - remove object fallback, use ensure_dependency("advanced_alchemy") (inherits from AA)
- [x] 9. Update `test_litestar_plugin.py` for import behavior tests

## Phase 3: Advanced Alchemy Extension Refactoring

- [x] 10. Refactor `vertebra/extensions/advanced_alchemy/_config.py` - remove object fallback, use ensure_dependency
- [x] 11. Refactor `vertebra/extensions/advanced_alchemy/_storage.py` - same pattern, remove try/except registration
- [x] 12. Update `test_advanced_alchemy.py` for import behavior tests

## Phase 4: SQLSpec Compound Dependencies

- [x] 13. Add ensure_dependency("litestar") to `vertebra/extensions/sqlspec/postgres/litestar/__init__.py`
- [x] 14. Add ensure_dependency("litestar") to `vertebra/extensions/sqlspec/mysql/litestar/__init__.py`
- [x] 15. Add ensure_dependency("litestar") to `vertebra/extensions/sqlspec/sqlite/litestar/__init__.py`
- [x] 16. Review store.py files - they have direct imports from `sqlspec.extensions.litestar.store` (ensure_dependency at __init__ level should suffice)
- [x] 17. Update sqlspec tests for compound dependency import behavior

## Phase 4b: SQLAlchemy Extension (Optional - Natural Failure OK)

- [x] 18. (Optional) Add ensure_dependency("sqlalchemy") to `vertebra/extensions/sqlalchemy/__init__.py` for better error message
- [x] 19. Review `_base.py` and `_async_dbapi.py` - confirm natural import failure is acceptable (both have direct sqlalchemy imports)

## Phase 5: Validation & Documentation

- [x] 20. Run full test suite (`make test`) and fix any failures
- [x] 21. Run linting and type checks (`make lint-all`) and fix any issues
- [x] 22. Update extension docstrings to document required dependencies

## Files Discovered in Rescan

### NEW files using object fallback anti-pattern (added since initial PRD):

### Files with direct imports needing guards:

### Test Files to Update:

## Task Estimates


## Dependencies Between Tasks


## Critical Path
