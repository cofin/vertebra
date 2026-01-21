"""Arrow typing support with lazy dependency checking.

This module provides Protocol-based types for pyarrow objects that work
without pyarrow installed. Type checkers can verify code correctness
while the actual imports only happen at runtime when needed.

Pattern adapted from SQLSpec's typing implementation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from vertebra._internal.module_loader import dependency_flag, ensure_pyarrow

if TYPE_CHECKING:
    from collections.abc import Iterator

PYARROW_INSTALLED = dependency_flag("pyarrow")


@runtime_checkable
class ArrowFieldProtocol(Protocol):
    """Protocol for pyarrow.Field."""

    @property
    def name(self) -> str: ...

    @property
    def type(self) -> Any: ...

    @property
    def nullable(self) -> bool: ...


@runtime_checkable
class ArrowSchemaProtocol(Protocol):
    """Protocol for pyarrow.Schema."""

    @property
    def names(self) -> list[str]: ...

    def field(self, i: int | str) -> ArrowFieldProtocol: ...

    def __len__(self) -> int: ...

    def __iter__(self) -> Iterator[ArrowFieldProtocol]: ...


@runtime_checkable
class ArrowTableProtocol(Protocol):
    """Protocol for pyarrow.Table."""

    @property
    def column_names(self) -> list[str]: ...

    @property
    def num_rows(self) -> int: ...

    @property
    def num_columns(self) -> int: ...

    @property
    def schema(self) -> ArrowSchemaProtocol: ...

    def to_pylist(self) -> list[dict[str, Any]]: ...

    def to_pydict(self) -> dict[str, Any]: ...

    def column(self, i: int | str) -> Any: ...

    def select(self, columns: list[str]) -> ArrowTableProtocol: ...


@runtime_checkable
class ArrowRecordBatchProtocol(Protocol):
    """Protocol for pyarrow.RecordBatch."""

    @property
    def column_names(self) -> list[str]: ...

    @property
    def num_rows(self) -> int: ...

    @property
    def num_columns(self) -> int: ...

    @property
    def schema(self) -> ArrowSchemaProtocol: ...

    def to_pylist(self) -> list[dict[str, Any]]: ...

    def to_pydict(self) -> dict[str, Any]: ...


@runtime_checkable
class ArrowArrayProtocol(Protocol):
    """Protocol for pyarrow.Array."""

    def to_pylist(self) -> list[Any]: ...

    @property
    def type(self) -> Any: ...

    def __len__(self) -> int: ...


# Conditional imports with stub fallbacks
# These allow code to use `ArrowTable` as a type annotation without
# importing pyarrow, while type checkers see the real type when available.
try:
    from pyarrow import Array as ArrowArray  # pyright: ignore[reportMissingImports]
    from pyarrow import Field as ArrowField  # pyright: ignore[reportMissingImports]
    from pyarrow import RecordBatch as ArrowRecordBatch  # pyright: ignore[reportMissingImports]
    from pyarrow import Schema as ArrowSchema  # pyright: ignore[reportMissingImports]
    from pyarrow import Table as ArrowTable  # pyright: ignore[reportMissingImports]
except ImportError:
    # When pyarrow is not installed, use Protocol stubs
    ArrowTable = ArrowTableProtocol  # type: ignore[assignment,misc]
    ArrowRecordBatch = ArrowRecordBatchProtocol  # type: ignore[assignment,misc]
    ArrowSchema = ArrowSchemaProtocol  # type: ignore[assignment,misc]
    ArrowField = ArrowFieldProtocol  # type: ignore[assignment,misc]
    ArrowArray = ArrowArrayProtocol  # type: ignore[assignment,misc]


__all__ = (
    # Flag
    "PYARROW_INSTALLED",
    # Concrete types (real pyarrow types or Protocol fallbacks)
    "ArrowArray",
    # Protocol types (for type hints without importing pyarrow)
    "ArrowArrayProtocol",
    "ArrowField",
    "ArrowFieldProtocol",
    "ArrowRecordBatch",
    "ArrowRecordBatchProtocol",
    "ArrowSchema",
    "ArrowSchemaProtocol",
    "ArrowTable",
    "ArrowTableProtocol",
    # Ensure function
    "ensure_pyarrow",
)
