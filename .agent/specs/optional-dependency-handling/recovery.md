# Recovery Guide: Optional Dependency Handling

## Quick Resume

**Last phase**: Implementation Complete
**Current status**: Ready for Review
**PRD location**: `specs/active/optional-dependency-handling/`

## Intelligence Context

**Complexity**: Medium (22 tasks across 6 phases)

**Stack**:
- Language: Python 3.12+
- Framework: Vertebra (Python-first backbone with Rust IPC and Bun server)
- Test: `make test` or `uv run pytest src/py/tests/`
- Lint: `make lint`
- Full check: `make lint-all`

## Implementation Summary

### Files Modified

#### Phase 1: Foundation
- `src/py/vertebra/utils/dependencies.py` - Added `ADVANCED_ALCHEMY_INSTALLED`, `SQLALCHEMY_INSTALLED`
- `src/py/vertebra/_typing.py` - Re-exported framework dependency flags
- `src/py/tests/test_module_loader.py` - **NEW** - 17 unit tests for ensure_dependency

#### Phase 2: Litestar Extension
- `src/py/vertebra/extensions/litestar/_plugin.py` - Added ensure_dependency("litestar")
- `src/py/vertebra/extensions/litestar/plugin.py` - **DELETED** (unused duplicate)
- `src/py/vertebra/extensions/litestar/advanced_alchemy.py` - Removed object fallback
- `src/py/vertebra/extensions/litestar/sqlspec.py` - Removed object fallback
- `src/py/vertebra/extensions/litestar/sqlalchemy.py` - Removed object fallback

#### Phase 3: Advanced Alchemy Extension
- `src/py/vertebra/extensions/advanced_alchemy/_config.py` - Removed object fallback, use ensure_dependency
- `src/py/vertebra/extensions/advanced_alchemy/_storage.py` - Removed object fallback, use ensure_dependency

#### Phase 4: SQLSpec Compound Dependencies
- `src/py/vertebra/extensions/sqlspec/postgres/litestar/__init__.py` - Added ensure_dependency("litestar")
- `src/py/vertebra/extensions/sqlspec/mysql/litestar/__init__.py` - Added ensure_dependency("litestar")
- `src/py/vertebra/extensions/sqlspec/sqlite/litestar/__init__.py` - Added ensure_dependency("litestar")

#### Phase 5: SQLAlchemy Extension
- `src/py/vertebra/extensions/sqlalchemy/__init__.py` - Added ensure_dependency("sqlalchemy")

### Test Results

- `test_module_loader.py`: 17 tests pass
- `test_litestar_plugin.py::test_plugin_registration`: Pass
- `test_advanced_alchemy.py::test_aa_postgres_config`: Pass

### Known Issues (Pre-existing)

1. `test_aa_storage_backend` - AA `StorageBackend` interface changed; test needs update
2. `test_sqlspec_plugin_registration` - sqlspec `SQLSpecPlugin` constructor changed; test needs update
3. Various IPC tests failing - Unrelated to this PR

## Patterns Established

### ensure_dependency Pattern
```python
from vertebra.utils.module_loader import ensure_dependency

ensure_dependency("library", install_package="vertebra[library]")

from library import SomeClass  # noqa: E402
```

### Centralized Dependency Flags
- Framework flags in `vertebra/utils/dependencies.py`
- Data library flags in `vertebra/_typing.py`
- Re-export through `_typing.py` for convenience

## Test Commands

```bash
# Run module_loader tests
uv run pytest src/py/tests/test_module_loader.py -v

# Run all key tests
uv run pytest src/py/tests/test_module_loader.py src/py/tests/test_litestar_plugin.py::test_plugin_registration src/py/tests/test_advanced_alchemy.py::test_aa_postgres_config -v

# Run linting
make lint

# Verify imports work
uv run python -c "
from vertebra.extensions.litestar import VertebraPlugin
from vertebra.extensions.advanced_alchemy._config import VertebraAsyncAlchemyConfig
from vertebra.extensions.sqlspec.postgres.litestar import PostgresLitestarStore
print('All imports successful!')
"
```

## Next Steps

1. Run `/test optional-dependency-handling` for comprehensive testing
2. Run `/review optional-dependency-handling` for quality gate enforcement
3. Update failing tests for AA storage and sqlspec plugin (separate PR)

## Rollback Strategy

If issues arise:
1. Git revert the commits from this implementation
2. All changes are isolated to the files listed above
3. No database changes or API changes to roll back
