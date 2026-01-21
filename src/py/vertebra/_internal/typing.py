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
from vertebra._internal.module_loader import OptionalDependencyFlag, dependency_flag, ensure_dependency, ensure_numpy

# Additional optional dependency flags
NUMPY_INSTALLED = dependency_flag("numpy")
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


# Conditional imports for numpy
try:
    from numpy import ndarray as NumpyArray  # noqa: N812  # pyright: ignore[reportMissingImports]
except ImportError:
    NumpyArray = NumpyArrayProtocol  # type: ignore[assignment,misc]


__all__ = (
    # Other optional deps
    "MSGSPEC_INSTALLED",
    # NumPy
    "NUMPY_INSTALLED",
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
    "dependency_flag",
    "ensure_dependency",
    "ensure_numpy",
    "ensure_pyarrow",
)
