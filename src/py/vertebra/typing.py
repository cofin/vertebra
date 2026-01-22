"""Public typing facade for vertebra.

Import types and dependency flags from here:

    from vertebra.typing import SpscRing, ArrowTable, PYARROW_INSTALLED

For ensure functions, import from utils.module_loader:

    from vertebra.utils.module_loader import ensure_pyarrow
"""

from vertebra._typing import (
    # Dependency flags
    ATTRS_INSTALLED,
    MSGSPEC_INSTALLED,
    NUMPY_INSTALLED,
    PANDAS_INSTALLED,
    POLARS_INSTALLED,
    PYARROW_INSTALLED,
    PYDANTIC_INSTALLED,
    # Arrow types
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
    # Context manager protocols
    AsyncContextManager,
    # Native shared memory types
    AsyncRing,
    AsyncShmTransport,
    # Data library types
    AttrsInstance,
    NumpyArray,
    NumpyArrayProtocol,
    PandasDataFrame,
    PandasDataFrameProtocol,
    PolarsDataFrame,
    PolarsDataFrameProtocol,
    PollingTransport,
    PydanticBaseModel,
    ShmRegion,
    SpscRing,
    SyncContextManager,
    create_transport,
    shm_unlink,
)

__all__ = (
    # Dependency flags
    "ATTRS_INSTALLED",
    "MSGSPEC_INSTALLED",
    "NUMPY_INSTALLED",
    "PANDAS_INSTALLED",
    "POLARS_INSTALLED",
    "PYARROW_INSTALLED",
    "PYDANTIC_INSTALLED",
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
    "NumpyArray",
    "NumpyArrayProtocol",
    "PandasDataFrame",
    "PandasDataFrameProtocol",
    "PolarsDataFrame",
    "PolarsDataFrameProtocol",
    "PollingTransport",
    "PydanticBaseModel",
    "ShmRegion",
    "SpscRing",
    "SyncContextManager",
    "create_transport",
    "shm_unlink",
)
