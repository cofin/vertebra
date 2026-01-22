"""Public typing facade for Vertebra.

This module re-exports types from _typing and provides dependency flags.
"""

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import dependency_flag

if TYPE_CHECKING:
    import pyarrow as pa  # pyright: ignore[reportMissingImports]

# Dependency flags
PYARROW_INSTALLED = dependency_flag("pyarrow")
NUMPY_INSTALLED = dependency_flag("numpy")
PANDAS_INSTALLED = dependency_flag("pandas")
POLARS_INSTALLED = dependency_flag("polars")

# Type aliases
if TYPE_CHECKING:
    ArrowTable = pa.Table
else:
    ArrowTable = object

__all__ = (
    "NUMPY_INSTALLED",
    "PANDAS_INSTALLED",
    "POLARS_INSTALLED",
    "PYARROW_INSTALLED",
    "ArrowTable",
)