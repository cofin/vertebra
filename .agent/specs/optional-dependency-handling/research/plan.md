# Optional Dependency Handling Research

## Executive Summary

This research analyzes how Vertebra's extension modules handle optional dependencies and proposes a unified strategy. The current implementation mixes multiple patterns inconsistently, leading to confusing error messages and potential runtime surprises. The recommended approach leverages Python's natural import semantics combined with strategic use of `ensure_dependency()` for enhanced developer experience.

## Current State Analysis

### Extension Categories in Vertebra

Vertebra provides four primary extension categories, each with different dependency requirements:

1. **Litestar Extension** (`vertebra.extensions.litestar/`)
   - Primary dependency: `litestar`
   - Purpose: Framework integration plugin, CLI commands, logging configuration

2. **SQLSpec Extension** (`vertebra.extensions.sqlspec/`)
   - Primary dependency: `sqlspec`
   - Sub-extensions: `postgres/`, `mysql/`, `sqlite/`
   - Compound dependencies: `{db}/litestar/` requires BOTH sqlspec AND litestar
   - Additional: `{db}/adk/` for Google's Agent Development Kit, `{db}/events/` for event stores

3. **SQLAlchemy Extension** (`vertebra.extensions.sqlalchemy/`)
   - Primary dependency: `sqlalchemy`
   - Purpose: DBAPI dialects for broker-backed connections

4. **Advanced Alchemy Extension** (`vertebra.extensions.advanced_alchemy/`)
   - Primary dependencies: `advanced_alchemy`, `sqlalchemy`
   - Purpose: ORM configuration helpers and storage backends

### Existing Pattern Analysis

#### Pattern 1: Try/Except with Object Fallback (PROBLEMATIC)

Found in `advanced_alchemy/_config.py`:
```python
if TYPE_CHECKING:
    from advanced_alchemy.config import AsyncAlchemyConfig
else:
    try:
        from advanced_alchemy.config import AsyncAlchemyConfig
    except ImportError:
        AsyncAlchemyConfig = object
```

**Problems:**
- Class inherits from `object` silently when dependency missing
- No immediate error feedback - user discovers problem at runtime
- Confusing errors about missing methods rather than missing dependencies
- Violates fail-fast principle

#### Pattern 2: Try/Except with Stub Classes (CURRENT BEST)

Found in `litestar/plugin.py`:
```python
try:
    from litestar.plugins import CLIPluginProtocol, InitPluginProtocol
    _LITESTAR_AVAILABLE = True
    _LITESTAR_IMPORT_ERROR: ImportError | None = None
except ImportError as exc:
    _LITESTAR_AVAILABLE = False
    _LITESTAR_IMPORT_ERROR = exc

    class CLIPluginProtocol: ...
    class InitPluginProtocol: ...

# Later in __post_init__:
if not _LITESTAR_AVAILABLE:
    raise MissingDependencyError(package="litestar") from _LITESTAR_IMPORT_ERROR
```

**Advantages:**
- Clear error message via MissingDependencyError
- Preserves original exception chain
- Module is importable for type checking purposes

**Disadvantages:**
- Verbose boilerplate
- Error deferred to instantiation rather than import
- Stub classes can leak into type hints

#### Pattern 3: Direct Import Without Guards (INCOMPLETE)

Found in `sqlspec/_driver.py`:
```python
from sqlspec.driver import AsyncDriverAdapterBase, ExecutionResult, ...
```

**Advantages:**
- Clean, simple code
- Natural Python import semantics

**Disadvantages:**
- Generic ImportError message: "No module named 'sqlspec'"
- User must deduce they need to install sqlspec

#### Pattern 4: Conditional Feature Enhancement (CORRECT)

Found in `serializers/litestar.py`:
```python
if dependency_flag("pydantic"):
    from pydantic import BaseModel
    VERTEBRA_TYPE_ENCODERS[BaseModel] = default_encoder
```

**This is the RIGHT pattern when:**
- The module works without the dependency
- The dependency adds optional functionality
- Not requiring the dependency is a feature, not a limitation

### Dependency Flag Infrastructure

Vertebra already has solid infrastructure for dependency checking:

**`vertebra.utils.module_loader`:**
- `dependency_flag(module_name)` - Returns lazy-evaluated flag
- `module_available(module_name)` - Checks if module can be imported (cached)
- `ensure_dependency(module_name, install_package)` - Raises MissingDependencyError if missing
- `MissingDependencyError` - Clear error with install instructions

**`vertebra.utils.dependencies`:**
- Centralized flags for common optional dependencies
- Currently includes: FSSPEC_INSTALLED, LITESTAR_INSTALLED, SQLSPEC_INSTALLED, WATCHDOG_INSTALLED
- Missing: ADVANCED_ALCHEMY_INSTALLED, SQLALCHEMY_INSTALLED

### Current __init__.py Structure

The extension packages correctly use empty exports:

```python
# vertebra/extensions/__init__.py
"""Framework integrations for Vertebra."""
__all__ = ()

# vertebra/extensions/sqlspec/__init__.py
"""SQLSpec integrations for Vertebra."""
# (empty file)

# vertebra/extensions/litestar/__init__.py
from ._plugin import VertebraPlugin
__all__ = ("VertebraPlugin",)
```

This forces users to explicitly import from submodules, which is the key insight for optional dependency handling.

## User's Key Insight

> "if we don't expose the extension into __all__, the user has to explicitly import from it. this is how we can make sure it's optional and doesn't crash when it's not there."

This means:
1. The top-level package never crashes on import
2. Users must explicitly `from vertebra.extensions.sqlspec.postgres import ...`
3. Only then does Python attempt to load the sqlspec dependency
4. This is Python's NATURAL guard against optional dependencies

The anti-pattern is adding try/except everywhere - that's defensive coding that obscures the real issue.

## Recommended Strategy

### Principle 1: Fail Fast at Import Time

When a user explicitly imports an extension, they WANT to use it. Let the import fail immediately with a helpful error message rather than deferring the error.

### Principle 2: Use ensure_dependency() at Entry Points

Replace try/except patterns with:
```python
from vertebra.utils.module_loader import ensure_dependency

ensure_dependency("litestar", install_package="vertebra[litestar]")

from litestar.plugins import CLIPluginProtocol, InitPluginProtocol
```

Benefits:
- Clear, actionable error message
- Consistent pattern across all extensions
- No stub classes needed
- Module fails to import = user knows immediately what's missing

### Principle 3: Centralize Dependency Flags

All dependency flags should be defined in `vertebra.utils.dependencies` and optionally re-exported through `vertebra._typing.py` for consistency.

### Principle 4: Handle Compound Dependencies

For modules like `sqlspec/postgres/litestar/`, the parent chain handles sqlspec, but litestar needs explicit checking:

```python
# vertebra/extensions/sqlspec/postgres/litestar/__init__.py
from vertebra.utils.module_loader import ensure_dependency

# sqlspec is implicitly required by parent imports
ensure_dependency("litestar", install_package="vertebra[litestar]")

from .store import PostgresLitestarStore
__all__ = ("PostgresLitestarStore",)
```

### Principle 5: Keep dependency_flag() for Conditional Enhancement

When a module WORKS without a dependency but BENEFITS from it:
```python
if dependency_flag("pydantic"):
    # Add pydantic support
```

This is different from required dependencies where the whole module is useless without them.

## File-by-File Change Plan

### New Constants Required

**vertebra/utils/dependencies.py:**
```python
ADVANCED_ALCHEMY_INSTALLED = dependency_flag("advanced_alchemy")
SQLALCHEMY_INSTALLED = dependency_flag("sqlalchemy")
```

### Pattern Updates Required

**1. vertebra/extensions/litestar/_plugin.py**
- Remove: try/except + stub classes + _LITESTAR_AVAILABLE check
- Add: `ensure_dependency("litestar", install_package="vertebra[litestar]")` at top

**2. vertebra/extensions/advanced_alchemy/_config.py**
- Remove: try/except + object fallback
- Add: `ensure_dependency("advanced_alchemy")` and `ensure_dependency("sqlalchemy")`

**3. vertebra/extensions/advanced_alchemy/_storage.py**
- Same changes as _config.py

**4. vertebra/extensions/sqlspec/{postgres,mysql,sqlite}/litestar/__init__.py**
- Add: `ensure_dependency("litestar", install_package="vertebra[litestar]")`

**5. vertebra/extensions/sqlspec/{postgres,mysql,sqlite}/adk/__init__.py**
- Research: Determine ADK's package name and add appropriate check

### Files to Keep As-Is

**vertebra/extensions/sqlspec/_driver.py**
- Direct imports from sqlspec are fine
- User explicitly imported the sqlspec extension, so sqlspec is expected
- Natural ImportError is acceptable (or could add ensure_dependency for better message)

**vertebra/serializers/litestar.py**
- Uses dependency_flag correctly for optional enhancement
- Module works without pydantic/pyarrow but gains features with them

## Error Message Format

The MissingDependencyError should produce clear messages:

```
MissingDependencyError: Missing dependency 'litestar'. Install with 'pip install vertebra[litestar]' to enable this feature.
```

This requires updating ensure_dependency() calls to pass the correct extras:
```python
ensure_dependency("litestar", install_package="vertebra[litestar]")
ensure_dependency("advanced_alchemy", install_package="vertebra[advanced-alchemy]")  # Note hyphen
```

## Type Checking Considerations

After removing stub classes, type checkers may see issues. Solutions:

1. **TYPE_CHECKING blocks for imports used only in annotations:**
```python
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from litestar.config.app import AppConfig
```

2. **String annotations for forward references:**
```python
def on_app_init(self, app_config: "AppConfig") -> "AppConfig":
```

This is already used extensively in the codebase.

## Testing Strategy

### Unit Tests

1. Test `ensure_dependency()` raises MissingDependencyError with correct message
2. Test `dependency_flag()` returns correct boolean
3. Test flag caching behavior

### Import Tests

1. Test that importing extension without deps raises MissingDependencyError
2. Test that importing with deps succeeds
3. Test compound dependencies (both must be present)

### Integration Tests

1. Full functionality tests with all deps installed
2. Verify no runtime errors from missing method calls

## Backwards Compatibility

The changes are backwards compatible in behavior (extensions still require their dependencies) but may change the exact error users see:

**Before:** `ImportError: No module named 'litestar'`
**After:** `MissingDependencyError: Missing dependency 'litestar'. Install with 'pip install vertebra[litestar]'`

This is an improvement, not a breaking change.

## Conclusion

The recommended approach simplifies the codebase by:
1. Removing verbose try/except boilerplate
2. Leveraging Python's natural import semantics
3. Adding strategic `ensure_dependency()` calls for better error messages
4. Centralizing dependency flags for consistency

The result is cleaner code, better developer experience, and consistent behavior across all extensions.

## Appendix A: Full Dependency Matrix

| Extension | Primary Deps | Secondary Deps |
|-----------|--------------|----------------|
| litestar | litestar | - |
| sqlspec | sqlspec | - |
| sqlspec.postgres | sqlspec | - |
| sqlspec.postgres.litestar | sqlspec | litestar |
| sqlspec.postgres.adk | sqlspec | adk (TBD) |
| sqlspec.postgres.events | sqlspec | - |
| sqlalchemy | sqlalchemy | - |
| advanced_alchemy | advanced_alchemy | sqlalchemy |

## Appendix B: Detailed Code Examples

### Example 1: Refactoring litestar/plugin.py

**Before (Current Implementation):**
```python
"""Vertebra plugin for Litestar framework."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

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

if TYPE_CHECKING:
    from litestar.config.app import AppConfig

@dataclass(slots=True)
class VertebraPlugin(InitPluginProtocol, CLIPluginProtocol):
    config: VertebraPluginConfig = field(default_factory=VertebraPluginConfig)

    def __post_init__(self) -> None:
        if not _LITESTAR_AVAILABLE:
            raise MissingDependencyError(package="litestar") from _LITESTAR_IMPORT_ERROR
```

**After (Recommended Implementation):**
```python
"""Vertebra plugin for Litestar framework."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from vertebra.utils.module_loader import ensure_dependency

# Validate litestar is available immediately on import
ensure_dependency("litestar", install_package="vertebra[litestar]")

from litestar.plugins import CLIPluginProtocol, InitPluginProtocol

if TYPE_CHECKING:
    from litestar.config.app import AppConfig

@dataclass(slots=True)
class VertebraPlugin(InitPluginProtocol, CLIPluginProtocol):
    config: VertebraPluginConfig = field(default_factory=VertebraPluginConfig)
    # No __post_init__ check needed - import already validated
```

The refactored version is:
- 15 lines shorter
- No stub classes to maintain
- Error occurs at import time, not instantiation
- Cleaner inheritance (no object fallback risk)

### Example 2: Handling Compound Dependencies

For `sqlspec/postgres/litestar/__init__.py`, we need to handle the fact that this module requires BOTH sqlspec (via parent imports) and litestar (not in parent chain):

**Current (Incomplete):**
```python
"""PostgreSQL Litestar store module."""
from vertebra.extensions.sqlspec.postgres.litestar.store import PostgresLitestarStore

__all__ = ("PostgresLitestarStore",)
```

**Recommended (Complete):**
```python
"""PostgreSQL Litestar store module."""
from vertebra.utils.module_loader import ensure_dependency

# sqlspec is implicitly required - parent modules import from it
# But litestar is an additional requirement we must check explicitly
ensure_dependency("litestar", install_package="vertebra[litestar]")

from vertebra.extensions.sqlspec.postgres.litestar.store import PostgresLitestarStore

__all__ = ("PostgresLitestarStore",)
```

### Example 3: The store.py File

The actual store implementation (`postgres/litestar/store.py`) imports from `sqlspec.extensions.litestar.store`. This is interesting because:

1. It requires sqlspec (parent chain handles this)
2. It requires sqlspec's litestar extension (which itself requires litestar)
3. The import `from sqlspec.extensions.litestar.store import BaseSQLSpecStore` will fail if litestar is not installed

This means sqlspec itself handles the litestar check for its own modules. Our `__init__.py` guard is still valuable because:
- It provides a Vertebra-specific error message mentioning `vertebra[litestar]`
- It fails faster (at __init__.py import, not at store.py import)
- It documents the dependency explicitly in our code

## Appendix C: Python Import Mechanics

Understanding Python's import system helps clarify why this strategy works:

### Import Resolution Order

When you execute `from vertebra.extensions.sqlspec.postgres.litestar import PostgresLitestarStore`:

1. Python loads `vertebra/__init__.py`
2. Python loads `vertebra/extensions/__init__.py` (empty __all__, nothing executed)
3. Python loads `vertebra/extensions/sqlspec/__init__.py` (empty, nothing executed)
4. Python loads `vertebra/extensions/sqlspec/postgres/__init__.py` (imports from config.py, driver.py)
5. Those imports trigger sqlspec imports - FAILS HERE if sqlspec missing
6. Python loads `vertebra/extensions/sqlspec/postgres/litestar/__init__.py`
7. With our guard, this FAILS HERE if litestar missing with MissingDependencyError
8. Finally loads the store.py module

### Why Empty __all__ is Important

If `vertebra/extensions/__init__.py` contained:
```python
from .litestar import VertebraPlugin  # WRONG
__all__ = ("VertebraPlugin",)
```

Then ANY import from vertebra.extensions would trigger litestar loading:
```python
from vertebra.extensions.sqlspec.postgres import VertebraPostgresAsyncConfig
# ^^^ This would fail if litestar not installed, even though user wants sqlspec!
```

By keeping __all__ empty and not importing submodules, each extension is truly independent.

## Appendix D: Comparison with Other Python Frameworks

### How SQLSpec Handles Optional Dependencies

SQLSpec (our upstream library) uses a similar pattern. Looking at `sqlspec/extensions/litestar/__init__.py`:
```python
from sqlspec.extensions.litestar.store import BaseSQLSpecStore
```

It imports directly, relying on:
1. User explicitly importing the litestar extension
2. Natural ImportError if litestar not installed

### How Litestar Handles Optional Dependencies

Litestar uses extras and optional imports extensively. For example, Litestar's SQLAlchemy plugin checks for sqlalchemy availability. Their pattern is similar to what we're recommending - fail fast with clear errors.

### How Advanced-Alchemy Handles It

Advanced-Alchemy typically requires its dependencies but provides graceful errors. The pattern aligns with using the parent framework's (Litestar's) plugin system.

## Appendix E: Migration Considerations

### Impact on Existing Users

Users who have the correct dependencies installed will see no change in behavior. Users who are missing dependencies will see BETTER error messages that guide them to the correct pip install command.

### Documentation Updates Needed

1. **README.md** - Add section on optional dependencies and extras
2. **Extension docstrings** - Each extension should document its requirements
3. **Error messages** - Ensure all MissingDependencyError instances reference correct extras

### Release Notes Guidance

When releasing this change:
- Mention improved error messages for missing optional dependencies
- No breaking changes for correctly configured environments
- Recommend using extras: `pip install vertebra[litestar,sqlspec]`
