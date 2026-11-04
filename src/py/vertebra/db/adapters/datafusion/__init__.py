"""Embedded Apache DataFusion analytics federation adapter."""

from __future__ import annotations

from vertebra.db.adapters.datafusion.config import (
    DataFusionConfig,
    DataFusionMode,
    TableRegistration,
)
from vertebra.db.adapters.datafusion.driver import (
    ARROW_STREAM_MEDIA_TYPE,
    DataFusionEngine,
)

__all__ = (
    "ARROW_STREAM_MEDIA_TYPE",
    "DataFusionConfig",
    "DataFusionEngine",
    "DataFusionMode",
    "TableRegistration",
)
