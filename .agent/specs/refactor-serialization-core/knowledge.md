# Knowledge Capture: Refactor Serialization to Core Package

## Patterns Discovered

### 1. Protocol Consolidation Pattern
Consolidating protocols from 8 files into one `protocols.py` improves discoverability and reduces import complexity. However, storage protocols with complex dependencies on domain types should remain with their implementations to avoid circular imports.

### 2. Public/Private Typing Facade
Using `typing.py` as public facade that re-exports from `_typing.py` (internal stubs) provides:
- Clean public API: `from vertebra.typing import ShmRing, PYARROW_INSTALLED`
- Internal flexibility to change stub implementations
- Graceful degradation when native extensions unavailable

### 3. Utils Directory Mirroring
Mirroring sqlspec's `utils/` structure creates consistency across the codebase:
- `utils/module_loader.py` - Lazy dependency detection with `OptionalDependencyFlag`
- `utils/dependencies.py` - Application-level dependency flags (FSSPEC, LITESTAR, etc.)
- `utils/encoders.py` - Shared encoding utilities for msgspec

## Lessons Learned

### What Worked Well
1. **Phased migration** - Moving files in phases (create new → update imports → delete old) prevented broken states
2. **Grep-based impact analysis** - Using grep to find all import references before making changes
3. **Running lint after each phase** - Caught E402 and other issues early

### What Didn't Work
1. **Circular import with storage protocols** - `SyncObjectStore`/`AsyncObjectStore` depend on types (`ByteRange`, `PutOptions`) defined in `storage/store.py`. Moving them to `protocols.py` would create circular imports. Left them in place.

## Decisions Made

| Decision | Rationale |
|----------|-----------|
| Merge json.py + msgpack.py → codecs.py | Both use msgspec, share encoding logic, simpler API |
| Keep storage protocols in storage/store.py | Avoid circular imports with domain types |
| No deprecation period for _internal/ | Clean break, all code is internal |
| Move async_transport to ipc/ | Better alignment with module purpose |

## Reusable Components

### OptionalDependencyFlag
```python
from vertebra.typing import dependency_flag

POLARS_INSTALLED = dependency_flag("polars")

if POLARS_INSTALLED:  # Evaluated lazily on first access
    import polars as pl
```

### ensure_* Functions
```python
from vertebra.typing import ensure_pyarrow

pa = ensure_pyarrow()  # Raises MissingDependencyError with install instructions
```

## Gotchas & Warnings

1. **E402 lint errors** - Imports after try/except blocks trigger E402. Put standard imports first, conditional imports after.

2. **Circular imports with protocols** - Protocols that reference domain types can't be moved to a central protocols.py. Keep them with their implementations.

3. **Test import updates** - Tests often import from internal modules. Update test imports as part of the migration.

## Final Structure

```
vertebra/
├── exceptions.py      # MissingDependencyError
├── protocols.py       # 21 consolidated protocols (driver, serializer, context manager, app, dataframe, arrow)
├── typing.py          # Public typing facade (re-exports from _typing.py)
├── _typing.py         # Internal native stubs (ShmRing, AsyncRing, etc.)
├── utils/
│   ├── module_loader.py   # OptionalDependencyFlag, ensure_* functions
│   ├── dependencies.py    # FSSPEC_INSTALLED, LITESTAR_INSTALLED, etc.
│   └── encoders.py        # default_encoder for msgspec
├── serializers/
│   ├── __init__.py        # Re-exports
│   └── codecs.py          # JSONCodec, MessagePackCodec, VertebraCodec
└── ipc/
    └── async_transport.py # AsyncShmTransport, PollingTransport
```
