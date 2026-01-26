"""Internal typing module with conditional imports.

This module handles all try/except import logic for optional and native dependencies.
Users should import from `vertebra.typing` (the public facade), not directly from here.
"""

from vertebra.common.protocols import (
    ArrowArrayProtocol,
    ArrowFieldProtocol,
    ArrowRecordBatchProtocol,
    ArrowSchemaProtocol,
    ArrowTableProtocol,
    AsyncContextManager,
    NumpyArrayProtocol,
    PandasDataFrameProtocol,
    PolarsDataFrameProtocol,
    SyncContextManager,
)
from vertebra.utils.dependencies import (
    ADVANCED_ALCHEMY_INSTALLED,
    LITESTAR_INSTALLED,
    SQLALCHEMY_INSTALLED,
    SQLSPEC_INSTALLED,
)
from vertebra.utils.module_loader import dependency_flag

# =============================================================================
# Native Extension Imports (Required)
# =============================================================================

try:
    from vertebra._vertebra import AsyncRing, ShmRegion, SpscRing, shm_unlink
except ImportError as exc:
    msg = (
        "vertebra requires the native extension (vertebra._vertebra).\n"
        "Install from a wheel: pip install vertebra\n"
        "Or build from source: maturin develop"
    )
    raise ImportError(msg) from exc

# =============================================================================
# Async Transport (depends on native extension + Python wrapper)
# =============================================================================

from vertebra.ipc.async_transport import AsyncShmTransport, UnsupportedPlatformError, create_transport

# =============================================================================
# Dependency Flags
# =============================================================================

PYARROW_INSTALLED = dependency_flag("pyarrow")
NUMPY_INSTALLED = dependency_flag("numpy")
PANDAS_INSTALLED = dependency_flag("pandas")
POLARS_INSTALLED = dependency_flag("polars")
MSGSPEC_INSTALLED = dependency_flag("msgspec")
PYDANTIC_INSTALLED = dependency_flag("pydantic")
ATTRS_INSTALLED = dependency_flag("attrs")

# =============================================================================
# Optional Data Library Imports
# =============================================================================

# PyArrow
try:
    from pyarrow import Array as ArrowArray  # pyright: ignore[reportMissingImports]
    from pyarrow import Field as ArrowField  # pyright: ignore[reportMissingImports]
    from pyarrow import RecordBatch as ArrowRecordBatch  # pyright: ignore[reportMissingImports]
    from pyarrow import Schema as ArrowSchema  # pyright: ignore[reportMissingImports]
    from pyarrow import Table as ArrowTable  # pyright: ignore[reportMissingImports]
except ImportError:
    ArrowArray = ArrowArrayProtocol  # type: ignore[assignment,misc]
    ArrowField = ArrowFieldProtocol  # type: ignore[assignment,misc]
    ArrowRecordBatch = ArrowRecordBatchProtocol  # type: ignore[assignment,misc]
    ArrowSchema = ArrowSchemaProtocol  # type: ignore[assignment,misc]
    ArrowTable = ArrowTableProtocol  # type: ignore[assignment,misc]

# NumPy
try:
    from numpy import ndarray as NumpyArray  # noqa: N812  # pyright: ignore[reportMissingImports]
except ImportError:
    NumpyArray = NumpyArrayProtocol  # type: ignore[assignment,misc]

# Pandas
try:
    from pandas import (
        DataFrame as PandasDataFrame,  # type: ignore[import-not-found]  # pyright: ignore[reportMissingImports]
    )
except ImportError:
    PandasDataFrame = PandasDataFrameProtocol  # type: ignore[assignment,misc]

# Polars
try:
    from polars import (
        DataFrame as PolarsDataFrame,  # type: ignore[import-not-found]  # pyright: ignore[reportMissingImports]
    )
except ImportError:
    PolarsDataFrame = PolarsDataFrameProtocol  # type: ignore[assignment,misc]

# Pydantic
try:
    from pydantic import BaseModel as PydanticBaseModel  # pyright: ignore[reportMissingImports]
except ImportError:
    from typing import Any, Protocol, runtime_checkable

    @runtime_checkable
    class PydanticBaseModel(Protocol):  # type: ignore[no-redef]
        """Protocol stub for pydantic.BaseModel when pydantic is not installed."""

        def model_dump(self) -> dict[str, Any]: ...
        def model_dump_json(self) -> str: ...


# Attrs
try:
    from attrs import AttrsInstance  # pyright: ignore[reportMissingImports]
except ImportError:
    from typing import Any, ClassVar, Protocol, runtime_checkable

    @runtime_checkable
    class AttrsInstance(Protocol):  # type: ignore[no-redef]
        """Protocol stub for attrs instances when attrs is not installed."""

        __attrs_attrs__: ClassVar[tuple[Any, ...]]


# Litestar
try:
    from litestar.response import Response as LitestarResponse  # pyright: ignore[reportMissingImports]
except ImportError:
    LitestarResponse = object  # type: ignore[assignment,misc]


__all__ = (
    # Framework/Extension dependency flags (re-exported)
    "ADVANCED_ALCHEMY_INSTALLED",
    # Data library dependency flags
    "ATTRS_INSTALLED",
    "LITESTAR_INSTALLED",
    "MSGSPEC_INSTALLED",
    "NUMPY_INSTALLED",
    "PANDAS_INSTALLED",
    "POLARS_INSTALLED",
    "PYARROW_INSTALLED",
    "PYDANTIC_INSTALLED",
    "SQLALCHEMY_INSTALLED",
    "SQLSPEC_INSTALLED",
    # Arrow types
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
    # Context manager protocols
    "AsyncContextManager",
    # Native shared memory types
    "AsyncRing",
    "AsyncShmTransport",
    # Data library types
    "AttrsInstance",
    "LitestarResponse",
    "NumpyArray",
    "NumpyArrayProtocol",
    "PandasDataFrame",
    "PandasDataFrameProtocol",
    "PolarsDataFrame",
    "PolarsDataFrameProtocol",
    "PydanticBaseModel",
    "ShmRegion",
    "SpscRing",
    "SyncContextManager",
    "UnsupportedPlatformError",
    "create_transport",
    "shm_unlink",
)
