"""Embedded Apache DataFusion analytics federation adapter."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
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

__lazy_modules__ = [
    "vertebra.db.adapters.datafusion.config",
    "vertebra.db.adapters.datafusion.driver",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "ARROW_STREAM_MEDIA_TYPE": ("vertebra.db.adapters.datafusion.driver", "ARROW_STREAM_MEDIA_TYPE"),
    "DataFusionConfig": ("vertebra.db.adapters.datafusion.config", "DataFusionConfig"),
    "DataFusionEngine": ("vertebra.db.adapters.datafusion.driver", "DataFusionEngine"),
    "DataFusionMode": ("vertebra.db.adapters.datafusion.config", "DataFusionMode"),
    "TableRegistration": ("vertebra.db.adapters.datafusion.config", "TableRegistration"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
