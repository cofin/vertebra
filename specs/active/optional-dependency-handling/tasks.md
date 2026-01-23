# Tasks: Optional Dependency Handling Standardization

## Phase 1: Foundation (Dependency Flags)

- [ ] 1. Add ADVANCED_ALCHEMY_INSTALLED and SQLALCHEMY_INSTALLED to `vertebra/utils/dependencies.py`
- [ ] 2. Update `vertebra/_typing.py` to re-export framework dependency flags
- [ ] 3. Write unit tests for ensure_dependency and dependency_flag functions

## Phase 2: Litestar Extension Refactoring

- [ ] 4. Refactor `vertebra/extensions/litestar/_plugin.py` - add ensure_dependency("litestar") at top (currently has direct imports with NO guards)
- [ ] 5. Delete `vertebra/extensions/litestar/plugin.py` (unused duplicate implementation)
- [ ] 6. Update tests for litestar extension import behavior

## Phase 3: Advanced Alchemy Extension Refactoring

- [ ] 7. Refactor `vertebra/extensions/advanced_alchemy/_config.py` - remove object fallback, use ensure_dependency
- [ ] 8. Refactor `vertebra/extensions/advanced_alchemy/_storage.py` - same pattern, remove try/except registration
- [ ] 9. Update tests for advanced_alchemy extension import behavior

## Phase 4: SQLSpec Compound Dependencies

- [ ] 10. Add ensure_dependency("litestar") to `vertebra/extensions/sqlspec/postgres/litestar/__init__.py`
- [ ] 11. Add ensure_dependency("litestar") to `vertebra/extensions/sqlspec/mysql/litestar/__init__.py`
- [ ] 12. Add ensure_dependency("litestar") to `vertebra/extensions/sqlspec/sqlite/litestar/__init__.py`
- [ ] 13. Review ADK submodules - determine if ADK is external dependency requiring guard
- [ ] 14. Write tests for compound dependency import behavior

## Phase 4b: SQLAlchemy Extension (Optional - Natural Failure OK)

- [ ] 15. (Optional) Add ensure_dependency("sqlalchemy") to `vertebra/extensions/sqlalchemy/__init__.py` for better error message
- [ ] 16. Review `_base.py` and `_async_dbapi.py` - confirm natural import failure is acceptable

## Phase 5: Validation & Documentation

- [ ] 17. Run full test suite (`make test`) and fix any failures
- [ ] 18. Run linting and type checks (`make lint-all`) and fix any issues
- [ ] 19. Update extension docstrings to document required dependencies

## Task Estimates

| Phase | Tasks | Risk Level |
|-------|-------|------------|
| Phase 1 | 1-3 | Low |
| Phase 2 | 4-6 | Medium |
| Phase 3 | 7-9 | Medium |
| Phase 4 | 10-14 | Low |
| Phase 5 | 15-17 | Low |

## Dependencies Between Tasks

```
Task 1 → Task 2 → Task 3
                    ↓
         ┌─────────┴─────────┐
         ↓                   ↓
      Task 4              Task 7
         ↓                   ↓
      Task 5              Task 8
         ↓                   ↓
      Task 6              Task 9
         ↓                   ↓
         └─────────┬─────────┘
                   ↓
            Tasks 10-14
                   ↓
            Tasks 15-17
```

## Critical Path

1. Task 1 (Add flags to dependencies.py)
2. Task 2 (Re-export in _typing.py)
3. Tasks 4, 5, 7, 8 (Core refactoring - can be parallelized)
4. Tasks 10-12 (SQLSpec litestar guards)
5. Task 15 (Test suite validation)
