# Recovery Guide: Optional Dependency Handling

## Quick Resume

**Last phase**: Planning Complete
**Current status**: Ready for Implementation
**PRD location**: `specs/active/optional-dependency-handling/`

## Intelligence Context

**Complexity**: Medium (8 checkpoints)

**Stack**:
- Language: Python 3.12+
- Framework: Vertebra (Python-first backbone with Rust IPC and Bun server)
- Test: `make test` or `uv run pytest src/py/tests/`
- Lint: `make lint`
- Full check: `make lint-all`

**Documentation**:
- System: None (CLAUDE.md only)
- Skills: N/A

**Similar implementations**:
- `vertebra/extensions/litestar/plugin.py` - Best current pattern
- `vertebra/extensions/advanced_alchemy/_config.py` - Pattern to replace
- `vertebra/serializers/litestar.py` - Correct conditional enhancement
- `vertebra/utils/module_loader.py` - Core infrastructure

**Patterns being followed**:
- `ensure_dependency()` at extension entry points
- `dependency_flag()` for conditional feature enhancement
- Empty `__all__` at extension roots
- `TYPE_CHECKING` for type-only imports

## State Summary

### Completed
- Project discovery and structure analysis
- Pattern recognition across 70+ extension files
- Complexity assessment (Medium, 8 checkpoints)
- Research document (2058 words)
- PRD document (3355 words)
- Task breakdown (17 tasks across 5 phases)

### Pending
- Implementation of all tasks
- No source code modified during planning

## Key Files to Modify

### Phase 1: Foundation
```
src/py/vertebra/utils/dependencies.py  # Add ADVANCED_ALCHEMY_INSTALLED, SQLALCHEMY_INSTALLED
src/py/vertebra/_typing.py             # Re-export framework flags
```

### Phase 2: Litestar Extension
```
src/py/vertebra/extensions/litestar/plugin.py   # Remove try/except + stubs
src/py/vertebra/extensions/litestar/_plugin.py  # Same refactoring
```

### Phase 3: Advanced Alchemy Extension
```
src/py/vertebra/extensions/advanced_alchemy/_config.py   # Remove object fallback
src/py/vertebra/extensions/advanced_alchemy/_storage.py  # Same refactoring
```

### Phase 4: SQLSpec Compound Dependencies
```
src/py/vertebra/extensions/sqlspec/postgres/litestar/__init__.py  # Add ensure_dependency
src/py/vertebra/extensions/sqlspec/mysql/litestar/__init__.py     # Add ensure_dependency
src/py/vertebra/extensions/sqlspec/sqlite/litestar/__init__.py    # Add ensure_dependency
```

## Key Decisions Made

1. **Use ensure_dependency() instead of try/except**: Provides better error messages and fail-fast behavior
2. **Remove object fallback pattern**: Causes confusing runtime errors
3. **Centralize dependency flags**: Single source of truth in `utils/dependencies.py`
4. **Keep natural ImportError for deep files**: `sqlspec/_driver.py` doesn't need guards since user explicitly imported the extension

## Implementation Pattern

Replace this:
```python
try:
    from some_library import SomeClass
    _AVAILABLE = True
except ImportError as exc:
    _AVAILABLE = False
    class SomeClass: ...

# Later:
if not _AVAILABLE:
    raise MissingDependencyError(...)
```

With this:
```python
from vertebra.utils.module_loader import ensure_dependency

ensure_dependency("some_library", install_package="vertebra[some-library]")

from some_library import SomeClass
# No runtime check needed
```

## Test Commands

```bash
# Run all tests
make test

# Run specific test file
uv run pytest src/py/tests/test_module_loader.py -v

# Run linting
make lint

# Run full checks (lint + type check)
make lint-all
```

## Next Steps

Run `/implement optional-dependency-handling` to begin implementation.

Or manually start with:
1. Open `src/py/vertebra/utils/dependencies.py`
2. Add `ADVANCED_ALCHEMY_INSTALLED = dependency_flag("advanced_alchemy")`
3. Add `SQLALCHEMY_INSTALLED = dependency_flag("sqlalchemy")`
4. Update `__all__` tuple
5. Run `make test` to verify

## Rollback Strategy

If issues arise:
1. All changes are additive to dependencies.py (safe)
2. Extension refactoring can be reverted file-by-file
3. Keep the old tests until new patterns are verified
4. Git history provides full rollback capability
