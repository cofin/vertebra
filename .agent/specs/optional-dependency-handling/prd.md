# PRD: Optional Dependency Handling Standardization

## Intelligence Context

### Complexity Assessment
- **Classification**: Medium
- **Estimated Checkpoints**: 8
- **Risk Level**: Low-Medium
- **Breaking Changes**: None (improved error messages only)

### Similar Features Referenced
1. `vertebra/extensions/litestar/_plugin.py` - Active implementation with direct imports (needs ensure_dependency)
2. `vertebra/extensions/advanced_alchemy/_config.py` - Object fallback pattern (needs refactoring)
3. `vertebra/serializers/litestar.py` - Correct conditional feature enhancement (keep as-is)
4. `vertebra/utils/module_loader.py` - Core dependency infrastructure
5. `vertebra/extensions/sqlalchemy/_base.py` - Direct SQLAlchemy imports (natural failure OK)
6. `vertebra/extensions/sqlalchemy/_async_dbapi.py` - Async DBAPI implementation

### Patterns to Follow
- **ensure_dependency()**: Use at extension entry points for clear error messages
- **dependency_flag()**: Use for conditional feature enhancement only
- **Empty __all__**: Keep at extension roots to force explicit imports
- **TYPE_CHECKING**: Use for type-only imports from optional dependencies

### Detected Stack and Commands
- **Language**: Python 3.12+
- **Test Command**: `make test` or `uv run pytest src/py/tests/`
- **Lint Command**: `make lint`
- **Type Check**: Included in `make lint-all`

---

## Problem Statement

### User Problem Being Solved

Vertebra's extension modules currently handle optional dependencies inconsistently, leading to:

1. **Confusing Error Messages**: When users import an extension without its dependency, they may get generic `ImportError: No module named 'X'` instead of actionable guidance like `MissingDependencyError: Missing dependency 'X'. Install with 'pip install vertebra[X]'`.

2. **Deferred Failures**: Some patterns (object fallback) allow modules to import successfully but fail at runtime with confusing errors about missing methods rather than missing dependencies.

3. **Inconsistent Patterns**: Different extensions use different approaches, making the codebase harder to maintain and reason about.

4. **Duplicated Code**: Dependency flags like `ADVANCED_ALCHEMY_INSTALLED` are defined multiple times across files.

### Business Value

1. **Improved Developer Experience**: Clear error messages reduce debugging time and frustration
2. **Reduced Support Burden**: Self-explanatory errors mean fewer support requests
3. **Maintainable Codebase**: Consistent patterns reduce cognitive load for contributors
4. **Professional Quality**: Framework-quality error handling builds trust

### Success Criteria

1. All extension imports that fail due to missing dependencies raise `MissingDependencyError` with pip install instructions
2. No extensions use the object fallback pattern
3. All dependency flags are centralized in `vertebra.utils.dependencies`
4. Test coverage for import scenarios with missing dependencies
5. Zero regressions in existing functionality when dependencies are installed

---

## Acceptance Criteria

### Functional Requirements

#### FR-1: Centralized Dependency Flags
- [ ] `vertebra/utils/dependencies.py` contains all framework dependency flags
- [ ] New flags added: `ADVANCED_ALCHEMY_INSTALLED`, `SQLALCHEMY_INSTALLED`
- [ ] All flags use lazy evaluation via `dependency_flag()`
- [ ] Flags are re-exported through `vertebra._typing.py`

#### FR-2: Litestar Extension Refactoring
- [ ] `vertebra/extensions/litestar/_plugin.py` - Add `ensure_dependency("litestar")` at module top (currently has direct imports with NO guards!)
- [ ] MissingDependencyError includes install instructions: `pip install vertebra[litestar]`
- [ ] TYPE_CHECKING blocks used for type-only imports where needed
- [ ] Delete `vertebra/extensions/litestar/plugin.py` (unused duplicate implementation)

#### FR-3: Advanced Alchemy Extension Refactoring
- [ ] `vertebra/extensions/advanced_alchemy/_config.py` uses `ensure_dependency()` for both dependencies
- [ ] Object fallback pattern removed
- [ ] `vertebra/extensions/advanced_alchemy/_storage.py` follows same pattern
- [ ] Error messages reference both `vertebra[advanced-alchemy]` and `vertebra[sqlalchemy]` as needed

#### FR-4: SQLSpec Compound Dependencies
- [ ] `vertebra/extensions/sqlspec/*/litestar/__init__.py` adds `ensure_dependency("litestar")`
- [ ] `vertebra/extensions/sqlspec/*/adk/__init__.py` adds appropriate dependency check (if ADK is external)
- [ ] Core sqlspec files (`_driver.py`, `_config.py`) remain unchanged (natural import failure is acceptable)

#### FR-4b: SQLAlchemy Extension (NEW)
- [ ] `vertebra/extensions/sqlalchemy/_base.py` - Direct imports from sqlalchemy (acceptable - natural failure)
- [ ] `vertebra/extensions/sqlalchemy/_async_dbapi.py` - Direct imports (acceptable - natural failure)
- [ ] `vertebra/extensions/sqlalchemy/{postgres,mysql,sqlite}/__init__.py` - Register dialects on import (keep as-is)
- [ ] Consider: Add `ensure_dependency("sqlalchemy")` to `__init__.py` files for better error messages

#### FR-5: Type Checking Compatibility
- [ ] All refactored files pass type checking with `make lint-all`
- [ ] TYPE_CHECKING blocks used where needed for type annotations
- [ ] No stub classes needed after refactoring

### Edge Cases Handled

#### EC-1: Compound Dependencies
When `vertebra.extensions.sqlspec.postgres.litestar` is imported:
- If sqlspec is missing: ImportError from parent chain (acceptable)
- If litestar is missing: MissingDependencyError with clear message (required)
- If both installed: Normal operation

#### EC-2: Partial Installation
User installs `vertebra[sqlspec]` but not `vertebra[litestar]`:
- Importing `from vertebra.extensions.sqlspec.postgres import *` works
- Importing `from vertebra.extensions.sqlspec.postgres.litestar import *` fails with MissingDependencyError

#### EC-3: Development Environment
During development with all dependencies:
- All imports work normally
- No performance impact from ensure_dependency() checks
- Type checking works correctly

### Error Scenarios

#### ES-1: Missing Primary Dependency
```python
# User runs without litestar installed
from vertebra.extensions.litestar import VertebraPlugin

# Expected: MissingDependencyError
# Message: "Missing dependency 'litestar'. Install with 'pip install vertebra[litestar]' to enable this feature."
```

#### ES-2: Missing Secondary Dependency
```python
# User runs without litestar but with sqlspec
from vertebra.extensions.sqlspec.postgres.litestar import PostgresLitestarStore

# Expected: MissingDependencyError
# Message: "Missing dependency 'litestar'. Install with 'pip install vertebra[litestar]' to enable this feature."
```

#### ES-3: All Dependencies Present
```python
# User runs with all dependencies
from vertebra.extensions.litestar import VertebraPlugin
from vertebra.extensions.sqlspec.postgres.litestar import PostgresLitestarStore

# Expected: Normal operation, no errors
```

---

## Technical Approach

### Pattern References from Codebase

The recommended pattern is already partially implemented in `vertebra/utils/module_loader.py`:

```python
def ensure_dependency(module_name: str, install_package: str | None = None, feature: str | None = None) -> "ModuleType":
    """Ensure an optional dependency is installed, raising a clear error if not."""
    if not module_available(module_name):
        install = install_package or module_name
        raise MissingDependencyError(
            package=module_name,
            install_package=f"vertebra[{install}]" if install != module_name else install
        )
    import importlib
    return importlib.import_module(module_name)
```

### File Structure Plan

```
src/py/vertebra/
├── utils/
│   ├── dependencies.py      # ADD: ADVANCED_ALCHEMY_INSTALLED, SQLALCHEMY_INSTALLED
│   └── module_loader.py     # NO CHANGE (already correct)
├── _typing.py               # ADD: Re-export dependency flags
├── typing.py                # CONSIDER: Re-export public flags
└── extensions/
    ├── __init__.py          # NO CHANGE (empty __all__)
    ├── litestar/
    │   ├── __init__.py      # NO CHANGE
    │   └── plugin.py        # REFACTOR: Use ensure_dependency()
    │   └── _plugin.py       # REFACTOR: Remove try/except + stubs
    ├── advanced_alchemy/
    │   ├── __init__.py      # NO CHANGE (empty)
    │   ├── _config.py       # REFACTOR: Remove object fallback
    │   └── _storage.py      # REFACTOR: Remove object fallback
    ├── sqlalchemy/
    │   └── *                 # NO CHANGE (natural import failure OK)
    └── sqlspec/
        ├── __init__.py      # NO CHANGE (empty)
        ├── _driver.py       # NO CHANGE (natural import failure OK)
        ├── _config.py       # NO CHANGE (natural import failure OK)
        ├── postgres/
        │   ├── __init__.py  # NO CHANGE
        │   ├── litestar/
        │   │   └── __init__.py  # ADD: ensure_dependency("litestar")
        │   └── adk/
        │       └── __init__.py  # ADD: appropriate check
        ├── mysql/           # Same as postgres
        └── sqlite/          # Same as postgres
```

### Database Changes
None required.

### API Endpoints
None required.

### Frontend Changes
None required.

### Documentation Updates Required

1. **CLAUDE.md**: Add section on optional dependency patterns
2. **Extension Docstrings**: Document required dependencies in each extension's docstring
3. **pyproject.toml Comments**: Ensure extras are clearly documented

---

## Testing Strategy

### Unit Tests (90%+ coverage target)

#### Test File: `test_optional_dependencies.py`

```python
# Test ensure_dependency raises correct error
def test_ensure_dependency_missing():
    """Test that ensure_dependency raises MissingDependencyError."""
    # Mock module_available to return False
    with mock.patch('vertebra.utils.module_loader.module_available', return_value=False):
        with pytest.raises(MissingDependencyError) as exc_info:
            ensure_dependency("fake_module", install_package="vertebra[fake]")
        assert "fake_module" in str(exc_info.value)
        assert "vertebra[fake]" in str(exc_info.value)

# Test ensure_dependency succeeds when installed
def test_ensure_dependency_installed():
    """Test that ensure_dependency returns module when installed."""
    # Use a known-installed module
    result = ensure_dependency("json")  # stdlib, always available
    assert result is not None

# Test dependency_flag lazy evaluation
def test_dependency_flag_lazy():
    """Test that dependency_flag doesn't import module immediately."""
    flag = dependency_flag("nonexistent_module_12345")
    # Creating the flag should not raise
    # Using it in boolean context should return False
    assert bool(flag) is False
```

#### Test File: `test_extension_imports.py`

```python
# Test litestar extension import behavior
def test_litestar_extension_missing_dep():
    """Test that litestar extension fails with MissingDependencyError."""
    with mock.patch.dict('sys.modules', {'litestar': None}):
        # ... test import behavior

# Test compound dependency behavior
def test_sqlspec_litestar_missing_litestar():
    """Test sqlspec/postgres/litestar fails if litestar missing but sqlspec present."""
    # ... test import behavior
```

### Integration Tests

#### Test File: `test_extension_integration.py`

```python
@pytest.mark.integration
def test_litestar_plugin_full_lifecycle():
    """Test VertebraPlugin works correctly when litestar is installed."""
    from vertebra.extensions.litestar import VertebraPlugin
    plugin = VertebraPlugin()
    # Test plugin methods work

@pytest.mark.integration
def test_sqlspec_postgres_litestar_store():
    """Test PostgresLitestarStore works when all deps installed."""
    from vertebra.extensions.sqlspec.postgres.litestar import PostgresLitestarStore
    # Test store creation
```

### E2E Tests
Not applicable for this feature.

---

## Implementation Notes

### Pattern Deviations (with rationale)

1. **Keeping natural ImportError for core sqlspec files**: Files like `_driver.py` that are deep in the extension hierarchy don't need `ensure_dependency()` guards. By the time a user reaches these files, they've already explicitly imported from `vertebra.extensions.sqlspec`, so sqlspec being required is expected. The natural `ImportError: No module named 'sqlspec'` is acceptable, though we COULD add guards for consistency.

2. **TYPE_CHECKING blocks**: After removing stub classes, we need TYPE_CHECKING blocks for imports used only in type annotations. This is Python best practice and already used extensively in the codebase.

### Dependencies

- No new external dependencies
- No changes to pyproject.toml extras (they should already be correct)

### Migration Considerations

This change is backwards compatible:
- Users with correct dependencies installed see no change
- Users with missing dependencies see BETTER error messages
- No API changes, no breaking changes

### Performance Considerations

`ensure_dependency()` uses `find_spec()` which is very fast and doesn't actually import the module. The check is cached, so subsequent imports don't re-check. Performance impact is negligible.

---

## Appendix: Code Transformation Examples

### Before/After: litestar/plugin.py

**Before:**
```python
from vertebra.exceptions import MissingDependencyError

try:
    from litestar.plugins import CLIPluginProtocol, InitPluginProtocol
    _LITESTAR_AVAILABLE = True
    _LITESTAR_IMPORT_ERROR: ImportError | None = None
except ImportError as exc:
    _LITESTAR_AVAILABLE = False
    _LITESTAR_IMPORT_ERROR = exc

    class CLIPluginProtocol:
        """Stub protocol for type checking."""

    class InitPluginProtocol:
        """Stub protocol for type checking."""

@dataclass(slots=True)
class VertebraPlugin(InitPluginProtocol, CLIPluginProtocol):
    def __post_init__(self) -> None:
        if not _LITESTAR_AVAILABLE:
            raise MissingDependencyError(package="litestar") from _LITESTAR_IMPORT_ERROR
```

**After:**
```python
from vertebra.utils.module_loader import ensure_dependency

ensure_dependency("litestar", install_package="vertebra[litestar]")

from litestar.plugins import CLIPluginProtocol, InitPluginProtocol

@dataclass(slots=True)
class VertebraPlugin(InitPluginProtocol, CLIPluginProtocol):
    # No __post_init__ check needed
    pass
```

### Before/After: advanced_alchemy/_config.py

**Before:**
```python
if TYPE_CHECKING:
    from advanced_alchemy.config import AsyncAlchemyConfig, SyncAlchemyConfig
else:
    try:
        from advanced_alchemy.config import AsyncAlchemyConfig, SyncAlchemyConfig
    except ImportError:
        AsyncAlchemyConfig = object
        SyncAlchemyConfig = object

ADVANCED_ALCHEMY_INSTALLED = dependency_flag("advanced_alchemy")

class VertebraAsyncAlchemyConfig(AsyncAlchemyConfig):
    def __init__(self, ...):
        if not ADVANCED_ALCHEMY_INSTALLED:
            msg = "advanced-alchemy is not installed."
            raise ImportError(msg)
```

**After:**
```python
from vertebra.utils.module_loader import ensure_dependency

ensure_dependency("advanced_alchemy", install_package="vertebra[advanced-alchemy]")
ensure_dependency("sqlalchemy", install_package="vertebra[sqlalchemy]")

from advanced_alchemy.config import AsyncAlchemyConfig, SyncAlchemyConfig
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

class VertebraAsyncAlchemyConfig(AsyncAlchemyConfig):
    def __init__(self, ...):
        # No runtime check needed - import already validated
```

### Before/After: sqlspec/postgres/litestar/__init__.py

**Before:**
```python
"""PostgreSQL Litestar store module."""
from vertebra.extensions.sqlspec.postgres.litestar.store import PostgresLitestarStore

__all__ = ("PostgresLitestarStore",)
```

**After:**
```python
"""PostgreSQL Litestar store module."""
from vertebra.utils.module_loader import ensure_dependency

# sqlspec is implicitly required via parent imports
# Explicitly check for litestar which is a compound dependency
ensure_dependency("litestar", install_package="vertebra[litestar]")

from vertebra.extensions.sqlspec.postgres.litestar.store import PostgresLitestarStore

__all__ = ("PostgresLitestarStore",)
```

---

## Appendix: Dependency Flag Consolidation

### Current State (scattered definitions)

```python
# In vertebra/utils/dependencies.py:
FSSPEC_INSTALLED = dependency_flag("fsspec")
LITESTAR_INSTALLED = dependency_flag("litestar")
SQLSPEC_INSTALLED = dependency_flag("sqlspec")
WATCHDOG_INSTALLED = dependency_flag("watchdog")

# In vertebra/_typing.py:
PYARROW_INSTALLED = dependency_flag("pyarrow")
NUMPY_INSTALLED = dependency_flag("numpy")
PANDAS_INSTALLED = dependency_flag("pandas")
POLARS_INSTALLED = dependency_flag("polars")
MSGSPEC_INSTALLED = dependency_flag("msgspec")
PYDANTIC_INSTALLED = dependency_flag("pydantic")
ATTRS_INSTALLED = dependency_flag("attrs")

# In vertebra/extensions/advanced_alchemy/_config.py:
ADVANCED_ALCHEMY_INSTALLED = dependency_flag("advanced_alchemy")  # DUPLICATED

# In vertebra/extensions/advanced_alchemy/_storage.py:
ADVANCED_ALCHEMY_INSTALLED = dependency_flag("advanced_alchemy")  # DUPLICATED
```

### Target State (centralized)

```python
# In vertebra/utils/dependencies.py (ALL framework/extension flags):
ADVANCED_ALCHEMY_INSTALLED = dependency_flag("advanced_alchemy")
FSSPEC_INSTALLED = dependency_flag("fsspec")
LITESTAR_INSTALLED = dependency_flag("litestar")
SQLALCHEMY_INSTALLED = dependency_flag("sqlalchemy")
SQLSPEC_INSTALLED = dependency_flag("sqlspec")
WATCHDOG_INSTALLED = dependency_flag("watchdog")

# In vertebra/_typing.py (data library flags + re-exports):
from vertebra.utils.dependencies import (
    ADVANCED_ALCHEMY_INSTALLED,
    LITESTAR_INSTALLED,
    SQLALCHEMY_INSTALLED,
    SQLSPEC_INSTALLED,
    # ... others as needed
)

PYARROW_INSTALLED = dependency_flag("pyarrow")
NUMPY_INSTALLED = dependency_flag("numpy")
# ... other data library flags
```

---

---

## Detailed Implementation Guide

### Phase 1: Foundation Setup (Dependencies Module)

#### Step 1.1: Update vertebra/utils/dependencies.py

Add the missing framework dependency flags:

```python
"""Optional dependency availability flags."""

from vertebra.utils.module_loader import dependency_flag

# Framework/Extension Dependencies
ADVANCED_ALCHEMY_INSTALLED = dependency_flag("advanced_alchemy")
FSSPEC_INSTALLED = dependency_flag("fsspec")
LITESTAR_INSTALLED = dependency_flag("litestar")
SQLALCHEMY_INSTALLED = dependency_flag("sqlalchemy")
SQLSPEC_INSTALLED = dependency_flag("sqlspec")
WATCHDOG_INSTALLED = dependency_flag("watchdog")

__all__ = (
    "ADVANCED_ALCHEMY_INSTALLED",
    "FSSPEC_INSTALLED",
    "LITESTAR_INSTALLED",
    "SQLALCHEMY_INSTALLED",
    "SQLSPEC_INSTALLED",
    "WATCHDOG_INSTALLED",
)
```

**Rationale**: Centralizing these flags ensures single source of truth and prevents the duplicate definitions we currently have in `advanced_alchemy/_config.py` and `_storage.py`.

#### Step 1.2: Update vertebra/_typing.py

Re-export the framework flags for modules that need them:

```python
# At the end of the existing imports section, add:
from vertebra.utils.dependencies import (
    ADVANCED_ALCHEMY_INSTALLED,
    LITESTAR_INSTALLED,
    SQLALCHEMY_INSTALLED,
    SQLSPEC_INSTALLED,
)

# Add to __all__:
__all__ = (
    # ... existing exports ...
    "ADVANCED_ALCHEMY_INSTALLED",
    "LITESTAR_INSTALLED",
    "SQLALCHEMY_INSTALLED",
    "SQLSPEC_INSTALLED",
)
```

**Rationale**: `_typing.py` is the internal module for type-related utilities. Re-exporting here allows other internal modules to import from a single location.

### Phase 2: Litestar Extension Refactoring

#### Step 2.1: Refactor litestar/plugin.py

This file currently has the most complex pattern with try/except, stub classes, and deferred error checking. The refactoring simplifies this significantly:

**Key Changes:**
1. Remove the try/except block at module level
2. Remove the stub classes `CLIPluginProtocol` and `InitPluginProtocol`
3. Remove the `_LITESTAR_AVAILABLE` flag and `_LITESTAR_IMPORT_ERROR` variable
4. Add `ensure_dependency()` call at module top
5. Remove the `__post_init__` dependency check

**Import Section Changes:**
```python
# OLD:
try:
    from litestar.plugins import CLIPluginProtocol, InitPluginProtocol
    _LITESTAR_AVAILABLE = True
    _LITESTAR_IMPORT_ERROR: ImportError | None = None
except ImportError as exc:
    _LITESTAR_AVAILABLE = False
    _LITESTAR_IMPORT_ERROR = exc
    class CLIPluginProtocol: ...
    class InitPluginProtocol: ...

# NEW:
from vertebra.utils.module_loader import ensure_dependency
ensure_dependency("litestar", install_package="vertebra[litestar]")
from litestar.plugins import CLIPluginProtocol, InitPluginProtocol
```

**Class Changes:**
```python
# OLD:
def __post_init__(self) -> None:
    if not _LITESTAR_AVAILABLE:
        raise MissingDependencyError(package="litestar") from _LITESTAR_IMPORT_ERROR
    # ... rest of init

# NEW:
def __post_init__(self) -> None:
    # Dependency already validated at import time
    # ... rest of init (no check needed)
```

#### Step 2.2: Handle litestar/_plugin.py

This is the actual implementation file. Similar refactoring applies:

```python
# At top of file, after standard imports:
from vertebra.utils.module_loader import ensure_dependency

ensure_dependency("litestar", install_package="vertebra[litestar]")

# Then import from litestar:
from litestar.plugins import CLIPluginProtocol, InitPluginProtocol
```

### Phase 3: Advanced Alchemy Extension Refactoring

#### Step 3.1: Refactor advanced_alchemy/_config.py

The current pattern uses object fallback which is problematic:

**Current Pattern (Problematic):**
```python
if TYPE_CHECKING:
    from advanced_alchemy.config import AsyncAlchemyConfig, SyncAlchemyConfig
else:
    try:
        from advanced_alchemy.config import AsyncAlchemyConfig, SyncAlchemyConfig
    except ImportError:
        AsyncAlchemyConfig = object
        SyncAlchemyConfig = object

ADVANCED_ALCHEMY_INSTALLED = dependency_flag("advanced_alchemy")

class VertebraAsyncAlchemyConfig(AsyncAlchemyConfig):
    def __init__(self, pool: str, driver: str = "postgresql+vertebra_async", **kwargs: Any) -> None:
        if not ADVANCED_ALCHEMY_INSTALLED:
            msg = "advanced-alchemy is not installed."
            raise ImportError(msg)
```

**New Pattern (Clean):**
```python
from vertebra.utils.module_loader import ensure_dependency

# Validate dependencies immediately
ensure_dependency("advanced_alchemy", install_package="vertebra[advanced-alchemy]")
ensure_dependency("sqlalchemy", install_package="vertebra[sqlalchemy]")

# Now safe to import
from advanced_alchemy.config import AsyncAlchemyConfig, SyncAlchemyConfig
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

class VertebraAsyncAlchemyConfig(AsyncAlchemyConfig):
    def __init__(self, pool: str, driver: str = "postgresql+vertebra_async", **kwargs: Any) -> None:
        # No runtime check needed - dependencies validated at import
        self.pool = pool
        engine = create_async_engine(f"{driver}://{pool}", poolclass=NullPool)
        super().__init__(engine=engine, **kwargs)
```

**Benefits:**
- Error at import time, not runtime
- Clear error message with install instructions
- Simpler code without object fallback
- No duplicate flag definition

#### Step 3.2: Refactor advanced_alchemy/_storage.py

Same pattern as `_config.py`:

```python
from vertebra.utils.module_loader import ensure_dependency

ensure_dependency("advanced_alchemy", install_package="vertebra[advanced-alchemy]")

from advanced_alchemy.types.file_object import Backend, storages
from vertebra.resources.broker import AsyncBrokerClient

class VertebraStorageBackend(Backend):
    # ... implementation unchanged ...

# Registration is now safe since we know advanced_alchemy is installed
storages.register_backend(VertebraStorageBackend())
```

**Note:** The try/except for registration at the bottom of the file can also be removed since we've already validated the import.

### Phase 4: SQLSpec Compound Dependencies

#### Step 4.1: Add Guards to Litestar Sub-Extensions

For each database (postgres, mysql, sqlite), the litestar submodule needs a guard:

**File: vertebra/extensions/sqlspec/postgres/litestar/__init__.py**
```python
"""PostgreSQL Litestar store module."""
from vertebra.utils.module_loader import ensure_dependency

# The parent imports handle sqlspec
# We need to explicitly check for litestar
ensure_dependency("litestar", install_package="vertebra[litestar]")

from vertebra.extensions.sqlspec.postgres.litestar.store import PostgresLitestarStore

__all__ = ("PostgresLitestarStore",)
```

**File: vertebra/extensions/sqlspec/mysql/litestar/__init__.py**
```python
"""MySQL Litestar store module."""
from vertebra.utils.module_loader import ensure_dependency

ensure_dependency("litestar", install_package="vertebra[litestar]")

from vertebra.extensions.sqlspec.mysql.litestar.store import MySQLLitestarStore

__all__ = ("MySQLLitestarStore",)
```

**File: vertebra/extensions/sqlspec/sqlite/litestar/__init__.py**
```python
"""SQLite Litestar store module."""
from vertebra.utils.module_loader import ensure_dependency

ensure_dependency("litestar", install_package="vertebra[litestar]")

from vertebra.extensions.sqlspec.sqlite.litestar.store import SQLiteLitestarStore

__all__ = ("SQLiteLitestarStore",)
```

#### Step 4.2: Review ADK Sub-Extensions

The ADK (Google Agent Development Kit) modules need similar treatment. First, we need to determine:
1. What is the pip package name for ADK?
2. Is it a separate install or bundled with sqlspec?

Based on the imports in `sqlspec/postgres/adk/store.py`:
```python
from sqlspec.extensions.adk import BaseAsyncADKStore, BaseSyncADKStore, ...
```

This appears to be part of sqlspec's extensions, so if sqlspec is installed, ADK support should be available. However, we should verify this and add appropriate guards if ADK requires additional dependencies.

### Phase 5: Testing Implementation

#### Test Cases to Implement

**1. Unit Tests for ensure_dependency**

```python
# tests/test_module_loader.py

import pytest
from unittest import mock

from vertebra.utils.module_loader import ensure_dependency, dependency_flag
from vertebra.exceptions import MissingDependencyError


class TestEnsureDependency:
    """Tests for ensure_dependency function."""

    def test_raises_missing_dependency_error(self):
        """ensure_dependency raises MissingDependencyError for missing modules."""
        with mock.patch('vertebra.utils.module_loader.module_available', return_value=False):
            with pytest.raises(MissingDependencyError) as exc_info:
                ensure_dependency("nonexistent_module")

            error = exc_info.value
            assert error.package == "nonexistent_module"
            assert "pip install" in str(error)

    def test_includes_custom_install_package(self):
        """ensure_dependency uses custom install package in error message."""
        with mock.patch('vertebra.utils.module_loader.module_available', return_value=False):
            with pytest.raises(MissingDependencyError) as exc_info:
                ensure_dependency("fake", install_package="vertebra[fake]")

            assert "vertebra[fake]" in str(exc_info.value)

    def test_returns_module_when_available(self):
        """ensure_dependency returns the module when it's installed."""
        # json is always available in Python stdlib
        result = ensure_dependency("json")
        import json
        assert result is json

    def test_caches_module_availability(self):
        """ensure_dependency caches the availability check."""
        with mock.patch('vertebra.utils.module_loader.find_spec') as mock_find:
            mock_find.return_value = mock.MagicMock()  # Module found

            # Call twice
            ensure_dependency("json")
            ensure_dependency("json")

            # Should only call find_spec once due to caching
            # (Actually calls through module_available which caches)


class TestDependencyFlag:
    """Tests for dependency_flag function."""

    def test_returns_true_for_installed_module(self):
        """dependency_flag returns True for installed modules."""
        flag = dependency_flag("json")  # stdlib, always available
        assert bool(flag) is True

    def test_returns_false_for_missing_module(self):
        """dependency_flag returns False for missing modules."""
        flag = dependency_flag("definitely_not_a_real_module_12345")
        assert bool(flag) is False

    def test_lazy_evaluation(self):
        """dependency_flag doesn't check availability until used."""
        # Creating the flag should not raise or check anything
        flag = dependency_flag("any_module_name")
        # The type should be OptionalDependencyFlag
        from vertebra.utils.module_loader import OptionalDependencyFlag
        assert isinstance(flag, OptionalDependencyFlag)
```

**2. Import Behavior Tests**

```python
# tests/test_extension_imports.py

import pytest
from unittest import mock
import sys


class TestLitestarExtensionImport:
    """Tests for litestar extension import behavior."""

    def test_import_fails_without_litestar(self):
        """Importing litestar extension fails with MissingDependencyError when litestar not installed."""
        # Remove litestar from available modules
        with mock.patch.dict(sys.modules, {'litestar': None, 'litestar.plugins': None}):
            with mock.patch('vertebra.utils.module_loader.module_available', return_value=False):
                from vertebra.exceptions import MissingDependencyError

                with pytest.raises(MissingDependencyError) as exc_info:
                    # Force reimport
                    import importlib
                    import vertebra.extensions.litestar.plugin
                    importlib.reload(vertebra.extensions.litestar.plugin)

                assert "litestar" in str(exc_info.value)


class TestAdvancedAlchemyExtensionImport:
    """Tests for advanced_alchemy extension import behavior."""

    def test_import_fails_without_advanced_alchemy(self):
        """Importing advanced_alchemy extension fails with MissingDependencyError."""
        # Similar pattern to above


class TestSQLSpecLitestarCompoundImport:
    """Tests for sqlspec/*/litestar compound dependency."""

    def test_import_fails_without_litestar(self):
        """Importing sqlspec.postgres.litestar fails if litestar missing but sqlspec present."""
        # Mock sqlspec as available but litestar as missing
        # Verify MissingDependencyError for litestar
```

---

## Risk Mitigation Strategies

### Risk 1: Type Checker Compatibility

**Risk:** After removing stub classes, type checkers may report errors for modules that import from litestar or other optional dependencies.

**Mitigation:**
1. Use `TYPE_CHECKING` blocks for type-only imports
2. Use string annotations for forward references
3. Run `make lint-all` after each change to catch issues early
4. Consider `# type: ignore` comments as last resort

**Example:**
```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from litestar.config.app import AppConfig  # Only for type hints

def on_app_init(self, app_config: "AppConfig") -> "AppConfig":
    # String annotation avoids runtime import
    ...
```

### Risk 2: Existing Tests May Fail

**Risk:** Tests that mock or manipulate optional dependencies may need updates.

**Mitigation:**
1. Audit existing tests before implementation
2. Update tests to use the new patterns
3. Ensure test isolation (each test should have clean import state)

### Risk 3: Third-Party Integration

**Risk:** Third-party tools or libraries that import from our extensions may see changed behavior.

**Mitigation:**
1. Document the change clearly in release notes
2. The behavior change is strictly an improvement (better errors)
3. No API changes - just import-time behavior

---

## Success Metrics

### Quantitative Metrics

1. **Code Reduction**: Remove approximately 50 lines of try/except + stub class boilerplate
2. **Flag Centralization**: Reduce dependency flag definitions from 6+ scattered locations to 1
3. **Test Coverage**: Maintain or improve 90%+ test coverage on affected modules

### Qualitative Metrics

1. **Developer Feedback**: Positive feedback on improved error messages
2. **Support Requests**: Reduction in "how do I install X" questions
3. **Code Review**: Faster reviews due to consistent patterns

---

## Glossary

| Term | Definition |
|------|------------|
| **dependency_flag** | Lazy-evaluated boolean flag that checks if a module is available |
| **ensure_dependency** | Function that validates a dependency is installed, raising MissingDependencyError if not |
| **MissingDependencyError** | Custom exception with pip install instructions for missing optional dependencies |
| **Compound Dependency** | Module that requires multiple optional dependencies (e.g., sqlspec + litestar) |
| **Object Fallback** | Anti-pattern where missing base class is replaced with `object` |
| **Stub Class** | Empty class definition used for type checking when real class unavailable |
| **TYPE_CHECKING** | Python constant that is True during type checking, False at runtime |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01-22 | PRD Agent | Initial PRD creation |
