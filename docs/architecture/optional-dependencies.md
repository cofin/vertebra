# Optional Dependencies and Typing Patterns

Vertebra uses a specific pattern (adopted from SQLSpec) to handle optional dependencies like `pyarrow`, `numpy`, and `pgvector` while maintaining strong type safety and clean error messages.

## Key Concepts

1.  **Lazy Evaluation**: Checks for dependency availability are deferred until runtime and cached.
2.  **Protocol Stubs**: We define `Protocol` classes that mirror the public API of optional dependencies. This allows static type checkers (mypy, pyright) to validate code without the dependency needing to be installed.
3.  **Conditional Imports**: We use `try/except ImportError` blocks at the module level to conditionally assign either the real class (if installed) or the Protocol stub to a type alias.
4.  **Clear Error Messages**: Helper functions like `ensure_pyarrow()` raise actionable errors (e.g., "Install with pip install vertebra[arrow]") when a user tries to use a feature requiring a missing dependency.

## Usage Guide

### 1. Using Optional Types

Always import types from `vertebra.typing`. This module provides the consolidated public facade.

```python
from typing import Any
from vertebra.typing import (
    ArrowTable,
    PYARROW_INSTALLED,
    ensure_pyarrow,
    POLARS_INSTALLED,
    ensure_polars,
    PANDAS_INSTALLED,
    ensure_pandas
)

def process_data(table: ArrowTable) -> None:
    # Type checking works here even if pyarrow is missing!
    # ArrowTable is either pyarrow.lib.Table or ArrowTableProtocol.
    print(table.num_rows)

    if PYARROW_INSTALLED:
        # Runtime block for code that strictly requires the real library
        import pyarrow as pa
        # ... do complex arrow operations ...

def load_data() -> ArrowTable:
    # If a feature strictly requires the library, guard it:
    ensure_pyarrow()  # Raises MissingDependencyError if missing

    import pyarrow as pa
    return pa.Table.from_pylist([])
```

### 2. Adding a New Optional Dependency

To add a new optional dependency (e.g., `scipy`):

1.  **Edit `src/py/vertebra/utils/module_loader.py`**:
    - Add `ensure_scipy()` function.

2.  **Edit `src/py/vertebra/typing.py`**:
    - Add `SCIPY_INSTALLED = dependency_flag("scipy")`.
    - Export `SCIPY_INSTALLED` in `__all__`.

3.  **Edit `src/py/vertebra/protocols.py`** (if needed):
    - Define a Protocol stub:
        ```python
        @runtime_checkable
        class ScipyMatrixProtocol(Protocol):
            ...
        ```

4.  **Edit `src/py/vertebra/typing.py`**:
    - Add conditional import:
        ```python
        try:
            from scipy import Matrix as ScipyMatrix # type: ignore[import-not-found,import-untyped,unused-ignore]
        except ImportError:
            ScipyMatrix = ScipyMatrixProtocol # type: ignore[assignment,misc,unused-ignore]
        ```
    - Export `ScipyMatrix` in `__all__`.

5.  **Update `pyproject.toml`**:
    - Add the dependency to `[project.optional-dependencies]`.

## Reference

- **`vertebra.typing`**: Public typing facade (dependency flags, type aliases, ensure functions).
- **`vertebra.utils.module_loader`**: Low-level dependency checking utilities (`OptionalDependencyFlag`, `dependency_flag()`).
- **`vertebra.protocols`**: Protocol definitions for optional dependency types.
- **`vertebra.exceptions`**: `MissingDependencyError` exception.
- **Supported Optionals**:
    - `arrow` (`pyarrow`)
    - `numpy` (`numpy`)
    - `polars` (`polars`)
    - `pandas` (`pandas`)
    - `pgvector` (Flag only, integration pending)
