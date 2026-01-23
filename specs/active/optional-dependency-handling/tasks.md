# Tasks: Optional Dependency Handling Standardization

## Phase 1: Foundation (Dependency Flags)

- [ ] 1. Add ADVANCED_ALCHEMY_INSTALLED and SQLALCHEMY_INSTALLED to `vertebra/utils/dependencies.py`
- [ ] 2. Update `vertebra/_typing.py` to re-export framework dependency flags
- [ ] 3. Write unit tests for ensure_dependency and dependency_flag functions (currently none exist for `module_loader.py`)

## Phase 2: Litestar Extension Refactoring

- [ ] 4. Refactor `vertebra/extensions/litestar/_plugin.py` - add ensure_dependency("litestar") at top (currently has direct imports with NO guards)
- [ ] 5. Delete `vertebra/extensions/litestar/plugin.py` (unused duplicate implementation)
- [ ] 6. Refactor `vertebra/extensions/litestar/advanced_alchemy.py` - remove object fallback, use ensure_dependency("advanced_alchemy")
- [ ] 7. Refactor `vertebra/extensions/litestar/sqlspec.py` - remove object fallback, use ensure_dependency("sqlspec")
- [ ] 8. Refactor `vertebra/extensions/litestar/sqlalchemy.py` - remove object fallback, use ensure_dependency("advanced_alchemy") (inherits from AA)
- [ ] 9. Update `test_litestar_plugin.py` for import behavior tests

## Phase 3: Advanced Alchemy Extension Refactoring

- [ ] 10. Refactor `vertebra/extensions/advanced_alchemy/_config.py` - remove object fallback, use ensure_dependency
- [ ] 11. Refactor `vertebra/extensions/advanced_alchemy/_storage.py` - same pattern, remove try/except registration
- [ ] 12. Update `test_advanced_alchemy.py` for import behavior tests

## Phase 4: SQLSpec Compound Dependencies

- [ ] 13. Add ensure_dependency("litestar") to `vertebra/extensions/sqlspec/postgres/litestar/__init__.py`
- [ ] 14. Add ensure_dependency("litestar") to `vertebra/extensions/sqlspec/mysql/litestar/__init__.py`
- [ ] 15. Add ensure_dependency("litestar") to `vertebra/extensions/sqlspec/sqlite/litestar/__init__.py`
- [ ] 16. Review store.py files - they have direct imports from `sqlspec.extensions.litestar.store` (ensure_dependency at __init__ level should suffice)
- [ ] 17. Update sqlspec tests for compound dependency import behavior

## Phase 4b: SQLAlchemy Extension (Optional - Natural Failure OK)

- [ ] 18. (Optional) Add ensure_dependency("sqlalchemy") to `vertebra/extensions/sqlalchemy/__init__.py` for better error message
- [ ] 19. Review `_base.py` and `_async_dbapi.py` - confirm natural import failure is acceptable (both have direct sqlalchemy imports)

## Phase 5: Validation & Documentation

- [ ] 20. Run full test suite (`make test`) and fix any failures
- [ ] 21. Run linting and type checks (`make lint-all`) and fix any issues
- [ ] 22. Update extension docstrings to document required dependencies

## Files Discovered in Rescan

### NEW files using object fallback anti-pattern (added since initial PRD):
- `vertebra/extensions/litestar/advanced_alchemy.py` - `SQLAlchemyPlugin = object`
- `vertebra/extensions/litestar/sqlspec.py` - `SQLSpecPlugin = object`, `LitestarConfig = object`
- `vertebra/extensions/litestar/sqlalchemy.py` - `SQLAlchemyPlugin = object`

### Files with direct imports needing guards:
- `vertebra/extensions/litestar/_plugin.py` - Direct `from litestar.plugins import ...`
- `vertebra/extensions/sqlspec/*/litestar/store.py` - Direct `from sqlspec.extensions.litestar.store import ...`
- `vertebra/extensions/sqlalchemy/_base.py` - Direct `from sqlalchemy.engine import ...`
- `vertebra/extensions/sqlalchemy/_async_dbapi.py` - No SQLAlchemy imports (internal only)

### Test Files to Update:
- `src/py/tests/test_litestar_plugin.py` - Tests litestar plugin integration
- `src/py/tests/test_advanced_alchemy.py` - Tests AA integration
- `src/py/tests/test_sqlspec_integration.py` - Tests SQLSpec integration
- `src/py/tests/test_sqlspec_storage.py` - Tests SQLSpec storage
- (NEW) Need to create `test_module_loader.py` for ensure_dependency tests

## Task Estimates

| Phase | Tasks | Risk Level |
|-------|-------|------------|
| Phase 1 | 1-3 | Low |
| Phase 2 | 4-9 | Medium |
| Phase 3 | 10-12 | Medium |
| Phase 4 | 13-17 | Low |
| Phase 4b | 18-19 | Low |
| Phase 5 | 20-22 | Low |

## Dependencies Between Tasks

```
Task 1 → Task 2 → Task 3
                    ↓
         ┌─────────┴─────────────────┐
         ↓                           ↓
      Tasks 4-8                   Tasks 10-11
         ↓                           ↓
      Task 9                      Task 12
         ↓                           ↓
         └───────────┬───────────────┘
                     ↓
              Tasks 13-17
                     ↓
              Tasks 18-19
                     ↓
              Tasks 20-22
```

## Critical Path

1. Task 1 (Add flags to dependencies.py)
2. Task 2 (Re-export in _typing.py)
3. Task 3 (Unit tests for module_loader)
4. Tasks 4-8, 10-11 (Core refactoring - can be parallelized)
5. Tasks 13-15 (SQLSpec litestar guards)
6. Task 20 (Test suite validation)
