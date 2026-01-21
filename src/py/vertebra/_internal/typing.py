"""Consolidated typing facade for optional dependencies.

This module provides a single import point for all optional dependency
types, flags, and ensure functions. Import from here for consistent
access to optional dependency handling.

Example usage:
    from vertebra._internal.typing import (
        PYARROW_INSTALLED,
        ArrowTable,
        ensure_pyarrow,
    )

    # Type annotations work without pyarrow installed
    def process_data(table: ArrowTable) -> list[dict[str, Any]]:
        return table.to_pylist()

    # Runtime check with lazy evaluation
    if PYARROW_INSTALLED:
        import pyarrow as pa
        # ... use pyarrow

    # Clear error message if not installed
    ensure_pyarrow()  # Raises MissingDependencyError with install instructions
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from vertebra._internal.arrow import (
    PYARROW_INSTALLED,
    ArrowArray,
    ArrowArrayProtocol,
    ArrowField,
    ArrowFieldProtocol,
    ArrowRecordBatch,
    ArrowRecordBatchProtocol,
    ArrowSchema,
    ArrowSchemaProtocol,
    ArrowTable,
    ArrowTableProtocol,
    ensure_pyarrow,
)
from vertebra._internal.module_loader import (
    OptionalDependencyFlag,
    dependency_flag,
    ensure_dependency,
    ensure_numpy,
    ensure_pandas,
    ensure_polars,
)

# Additional optional dependency flags
NUMPY_INSTALLED = dependency_flag("numpy")
PANDAS_INSTALLED = dependency_flag("pandas")
POLARS_INSTALLED = dependency_flag("polars")
MSGSPEC_INSTALLED = dependency_flag("msgspec")
PYDANTIC_INSTALLED = dependency_flag("pydantic")


@runtime_checkable
class NumpyArrayProtocol(Protocol):
    """Protocol for numpy.ndarray - works without numpy installed."""

    @property
    def shape(self) -> tuple[int, ...]: ...

    @property
    def dtype(self) -> Any: ...

    @property
    def ndim(self) -> int: ...

    def tolist(self) -> list[Any]: ...

    def __len__(self) -> int: ...


@runtime_checkable
class PandasDataFrameProtocol(Protocol):
    """Protocol for pandas.DataFrame - works without pandas installed."""

    @property
    def columns(self) -> Any: ...

    @property
    def index(self) -> Any: ...

    def to_dict(self, orient: str = "records") -> list[dict[str, Any]]: ...


@runtime_checkable
class PolarsDataFrameProtocol(Protocol):
    """Protocol for polars.DataFrame - works without polars installed."""

    @property
    def columns(self) -> list[str]: ...

    @property
    def schema(self) -> dict[str, Any]: ...

    def to_arrow(self) -> Any: ...

    def to_dicts(self) -> list[dict[str, Any]]: ...


# Conditional imports for numpy
try:
    from numpy import ndarray as NumpyArray  # noqa: N812  # pyright: ignore[reportMissingImports]
except ImportError:
    NumpyArray = NumpyArrayProtocol  # type: ignore[assignment,misc,unused-ignore]

# Conditional imports for pandas
try:
    from pandas import (
        DataFrame as PandasDataFrame,  # type: ignore[import-not-found,import-untyped,unused-ignore]  # pyright: ignore[reportMissingImports]
    )
except ImportError:
    PandasDataFrame = PandasDataFrameProtocol  # type: ignore[assignment,misc,unused-ignore]

# Conditional imports for polars
try:
    from polars import (
        DataFrame as PolarsDataFrame,  # type: ignore[import-not-found,import-untyped,unused-ignore]  # pyright: ignore[reportMissingImports]
    )
except ImportError:
    PolarsDataFrame = PolarsDataFrameProtocol  # type: ignore[assignment,misc,unused-ignore]


__all__ = (
    # Other optional deps
    "MSGSPEC_INSTALLED",
    # NumPy
    "NUMPY_INSTALLED",
    # Pandas
    "PANDAS_INSTALLED",
    # Polars
    "POLARS_INSTALLED",
    # Arrow
    "PYARROW_INSTALLED",
    "PYDANTIC_INSTALLED",
    "ArrowArray",
    "ArrowArrayProtocol",
    "ArrowField",
    "ArrowFieldProtocol",
    "ArrowRecordBatch",
    "ArrowRecordBatchProtocol",
    "ArrowSchema",
    "ArrowSchemaProtocol",
    "ArrowTable",
    "ArrowTableProtocol",
    "NumpyArray",
    "NumpyArrayProtocol",
    # Core utilities
    "OptionalDependencyFlag",
    "PandasDataFrame",
    "PandasDataFrameProtocol",
    "PolarsDataFrame",
    "PolarsDataFrameProtocol",
    "dependency_flag",
    "ensure_dependency",
    "ensure_numpy",
    "ensure_pandas",
    "ensure_polars",
    "ensure_pyarrow",
)
