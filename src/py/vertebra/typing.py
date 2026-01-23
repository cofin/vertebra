"""Public typing facade for Vertebra.

This module re-exports types from _typing and provides dependency flags.
"""

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import dependency_flag

if TYPE_CHECKING:
    import pyarrow as pa  # pyright: ignore[reportMissingImports]

# Dependency flags
MSGSPEC_INSTALLED = dependency_flag("msgspec")
PYDANTIC_INSTALLED = dependency_flag("pydantic")
ATTRS_INSTALLED = dependency_flag("attrs")
PYARROW_INSTALLED = dependency_flag("pyarrow")
NUMPY_INSTALLED = dependency_flag("numpy")
PANDAS_INSTALLED = dependency_flag("pandas")
POLARS_INSTALLED = dependency_flag("polars")

# Type aliases
try:
    import pyarrow as pa  # pyright: ignore[reportMissingImports]

    ArrowTable = pa.Table
except Exception:  # pragma: no cover - optional dependency fallback
    ArrowTable = object

__all__ = (
    "ATTRS_INSTALLED",
    "MSGSPEC_INSTALLED",
    "NUMPY_INSTALLED",
    "PANDAS_INSTALLED",
    "POLARS_INSTALLED",
    "PYARROW_INSTALLED",
    "PYDANTIC_INSTALLED",
    "ArrowTable",
)
