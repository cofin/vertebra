"""Backwards-compatible re-export from vertebra.common.protocols.

This module is deprecated. Import from vertebra.common.protocols instead.
"""

from vertebra.common.protocols import (
    ArrowArrayProtocol,
    ArrowFieldProtocol,
    ArrowRecordBatchProtocol,
    ArrowSchemaProtocol,
    ArrowTableProtocol,
    AsyncConnection,
    AsyncContextManager,
    AsyncCursor,
    AsyncPool,
    JSONSerializer,
    NumpyArrayProtocol,
    PandasDataFrameProtocol,
    PolarsDataFrameProtocol,
    ProviderPlugin,
    Serializer,
    SyncConnection,
    SyncContextManager,
    SyncCursor,
    SyncPool,
)

__all__ = (
    "ArrowArrayProtocol",
    "ArrowFieldProtocol",
    "ArrowRecordBatchProtocol",
    "ArrowSchemaProtocol",
    "ArrowTableProtocol",
    "AsyncConnection",
    "AsyncContextManager",
    "AsyncCursor",
    "AsyncPool",
    "JSONSerializer",
    "NumpyArrayProtocol",
    "PandasDataFrameProtocol",
    "PolarsDataFrameProtocol",
    "ProviderPlugin",
    "Serializer",
    "SyncConnection",
    "SyncContextManager",
    "SyncCursor",
    "SyncPool",
)
