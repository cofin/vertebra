# PRD: Adopt SQLSpec Typing Patterns for Optional Dependencies

## Overview

**Previous approach (abandoned):** Transition from pyarrow to nanoarrow as core dependency.

**New approach:** Adopt SQLSpec's proven `_typing.py` + `typing.py` facade pattern for handling optional dependencies (pyarrow, pgvector, numpy, etc.) with proper lazy evaluation, Protocol-based stubs, and graceful error handling.

This change improves type safety, provides clearer error messages, and maintains compatibility with SQLSpec's extension system for Vertebra's broker-backed adapters.

## Research Reference

Analysis of SQLSpec's typing implementation:
- `sqlspec/_typing.py` - Internal engine (700+ lines)
- `sqlspec/typing.py` - Public facade (396 lines)
- `sqlspec/utils/module_loader.py` - Lazy dependency evaluation
- `sqlspec/adapters/asyncpg/core.py` - Extension registration (pgvector, JSON codecs)
- `sqlspec/adapters/asyncpg/config.py` - Driver features and connection initialization

## Functional Requirements

### Must Have

- [ ] Create `vertebra/_internal/arrow.py` implementing SQLSpec's typing pattern:
  - `PYARROW_INSTALLED` lazy flag via `OptionalDependencyFlag`
  - Protocol-based `ArrowTableProtocol` for type checking without importing
  - Conditional import with stub fallback
  - `ensure_pyarrow()` function for clear error messages

- [ ] Create `vertebra/_internal/typing.py` consolidating optional dependency handling:
  - Re-export from `_internal/arrow.py`
  - Add flags: `NUMPY_INSTALLED`, `PGVECTOR_INSTALLED`, `MSGSPEC_INSTALLED`
  - Protocol stubs for numpy arrays, pgvector types

- [ ] Update `vertebra/resources/broker.py` to use new typing pattern:
  - Replace inline `try/except ImportError` with `PYARROW_INSTALLED` check
  - Use `ensure_pyarrow()` for clear install instructions

- [ ] Update `vertebra/drivers/broker.py` to use new typing pattern:
  - Type annotations using Protocols (works without pyarrow installed)
  - Runtime checks using availability flags

### Should Have

- [ ] Extension support in Vertebra's SQLSpec adapters:
  - `VertebraDriverFeatures` TypedDict for adapter configuration
  - Support for `enable_pgvector`, `enable_json_codecs` features
  - Connection initialization hooks for extension registration

- [ ] Formalize `vertebra[arrow]` optional dependency group in `pyproject.toml`

### Nice to Have

- [ ] Metrics/observability integration hooks (following SQLSpec's pattern)
- [ ] Event backend support for broker-backed adapters

## Non-Functional Requirements

- **Type Safety:** Pyright/mypy should pass without optional dependencies installed
- **Clear Errors:** Users get actionable messages like `pip install vertebra[arrow]`
- **Zero Runtime Overhead:** Flags evaluate lazily, cached per session
- **SQLSpec Compatibility:** Patterns align with upstream for maintainability

## Technical Approach

### Pattern 1: Lazy Dependency Flags

```python
# vertebra/_internal/module_loader.py (or reuse from sqlspec)
from importlib.util import find_spec

_dependency_cache: dict[str, bool] = {}

def module_available(module_name: str) -> bool:
    cached = _dependency_cache.get(module_name)
    if cached is not None:
        return cached
    try:
        is_available = find_spec(module_name) is not None
    except ModuleNotFoundError:
        is_available = False
    _dependency_cache[module_name] = is_available
    return is_available

class OptionalDependencyFlag:
    __slots__ = ("module_name",)

    def __init__(self, module_name: str) -> None:
        self.module_name = module_name

    def __bool__(self) -> bool:
        return module_available(self.module_name)

def dependency_flag(module_name: str) -> OptionalDependencyFlag:
    return OptionalDependencyFlag(module_name)
```

### Pattern 2: Protocol Stubs + Conditional Import

```python
# vertebra/_internal/arrow.py
from typing import Any, Protocol, runtime_checkable
from vertebra._internal.module_loader import dependency_flag

PYARROW_INSTALLED = dependency_flag("pyarrow")

@runtime_checkable
class ArrowTableProtocol(Protocol):
    """Typed shim for pyarrow.Table - works without pyarrow installed."""

    @property
    def column_names(self) -> list[str]: ...

    @property
    def num_rows(self) -> int: ...

    def to_pylist(self) -> list[dict[str, Any]]: ...

    def to_pydict(self) -> dict[str, Any]: ...

# Conditional import at module level
try:
    from pyarrow import Table as ArrowTable
    from pyarrow import RecordBatch as ArrowRecordBatch
    from pyarrow import Schema as ArrowSchema
except ImportError:
    ArrowTable = ArrowTableProtocol  # type: ignore[assignment,misc]
    ArrowRecordBatch = ArrowTableProtocol  # type: ignore[assignment,misc]
    ArrowSchema = ArrowTableProtocol  # type: ignore[assignment,misc]
```

### Pattern 3: Ensure Functions for Clear Errors

```python
# vertebra/_internal/module_loader.py
from vertebra._internal.errors import MissingDependencyError

def ensure_pyarrow() -> None:
    """Raise MissingDependencyError if pyarrow is not installed."""
    if not module_available("pyarrow"):
        raise MissingDependencyError(
            package="pyarrow",
            install_package="arrow",  # maps to vertebra[arrow]
            message="Arrow IPC results require pyarrow. Install with: pip install vertebra[arrow]"
        )
```

### Pattern 4: Extension Support in Adapters

```python
# vertebra/extensions/sqlspec/postgres/config.py
from typing import TypedDict, NotRequired, Callable, Any

class VertebraPostgresDriverFeatures(TypedDict):
    """Vertebra PostgreSQL driver feature flags."""

    # JSON handling
    json_serializer: NotRequired[Callable[[Any], str]]
    json_deserializer: NotRequired[Callable[[str], Any]]
    enable_json_codecs: NotRequired[bool]

    # Extensions
    enable_pgvector: NotRequired[bool]

    # Broker-specific
    arrow_threshold_rows: NotRequired[int]
    arrow_threshold_bytes: NotRequired[int]

async def _init_connection(connection: Any, features: VertebraPostgresDriverFeatures) -> None:
    """Initialize connection with codecs and extensions."""

    if features.get("enable_json_codecs", True):
        await register_json_codecs(connection, ...)

    if features.get("enable_pgvector", False):
        if not PGVECTOR_INSTALLED:
            logger.debug("pgvector not installed - skipping")
            return
        # Check if extension is enabled in database
        result = await connection.fetchval(
            "SELECT 1 FROM pg_extension WHERE extname = 'vector'"
        )
        if result:
            await register_pgvector_support(connection)
```

### Pattern 5: Updated Broker Client

```python
# vertebra/resources/broker.py (updated _decode_result)
from vertebra._internal.arrow import PYARROW_INSTALLED, ArrowTable
from vertebra._internal.module_loader import ensure_pyarrow

def _decode_result(self, response: dict[str, Any]) -> Any:
    data = _assert_ok(response)

    if "arrow" not in data:
        return data.get("result")

    # Clear, actionable error message
    ensure_pyarrow()

    import pyarrow as pa

    arrow_meta = data["arrow"]
    name = arrow_meta.get("name")
    size = int(arrow_meta.get("size", 0))

    region = ShmRegion.open_named(name, size)
    try:
        with pa.ipc.open_stream(memoryview(region)) as reader:
            table = reader.read_all()
    finally:
        region.close()
        self._release_arrow(str(name))

    return {"arrow": table}
```

## Files to Create/Modify

### New Files
- `src/py/vertebra/_internal/module_loader.py` - Dependency checking utilities
- `src/py/vertebra/_internal/arrow.py` - Arrow typing and protocols
- `src/py/vertebra/_internal/typing.py` - Public facade re-exporting all types

### Modified Files
- `pyproject.toml` - Add `[arrow]` optional dependency group
- `src/py/vertebra/resources/broker.py` - Use new typing pattern
- `src/py/vertebra/drivers/broker.py` - Use Protocol types
- `src/py/vertebra/extensions/sqlspec/provider.py` - Use ensure_pyarrow()
- `src/py/vertebra/extensions/sqlspec/postgres/config.py` - Add DriverFeatures

## Acceptance Criteria

- [ ] `pyright src/py/vertebra` passes without pyarrow installed
- [ ] `mypy src/py/vertebra` passes without pyarrow installed
- [ ] `from vertebra._internal.arrow import PYARROW_INSTALLED` works
- [ ] `if PYARROW_INSTALLED:` evaluates correctly at runtime
- [ ] Missing pyarrow raises `MissingDependencyError` with install instructions
- [ ] `pip install vertebra[arrow]` installs pyarrow
- [ ] Existing tests pass

## Out of Scope

- Adding nanoarrow as an alternative Arrow implementation
- Changing the Arrow IPC wire format
- Modifying SQLSpec's upstream code

## Dependencies

### Internal Dependencies
- `vertebra._internal.errors.MissingDependencyError` (existing)

### External Dependencies
- `pyarrow` (Optional, via `vertebra[arrow]`)
- `pgvector` (Optional, for vector support)
- `numpy` (Optional, for array support)

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Type checker compatibility | Low | Low | Test with pyright and mypy |
| Import cycle issues | Low | Medium | Keep `_internal/` imports minimal |
| Breaking existing code | Low | Medium | Maintain backward-compatible API |

## Comparison: Old vs New Approach

| Aspect | Old (nanoarrow) | New (SQLSpec pattern) |
|--------|-----------------|----------------------|
| New dependencies | +nanoarrow | None |
| Code paths | Dual engine | Single engine (pyarrow) |
| Type safety | Partial | Full (Protocols) |
| Error messages | Generic | Actionable |
| SQLSpec alignment | Divergent | Aligned |
| Maintenance burden | High | Low |
