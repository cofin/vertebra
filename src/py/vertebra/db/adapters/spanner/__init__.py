"""Pure-Rust Google Cloud Spanner v1 sync and async drivers across GoogleSQL and Spangres dialects."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.db.adapters.spanner.config import (
        DEFAULT_SPANNER_ENDPOINT,
        SPANGRES_DIALECT_TAG,
        SPANNER_GOOGLESQL_DIALECT_TAG,
        Mutation,
        MutationGroup,
        SpannerConfig,
        StalenessBound,
    )
    from vertebra.db.adapters.spanner.core import SpannerCore
    from vertebra.db.adapters.spanner.driver import (
        AsyncSpannerDriver,
        AsyncSpannerSnapshot,
        AsyncSpannerTransaction,
        SyncSpannerDriver,
        SyncSpannerSnapshot,
        SyncSpannerTransaction,
    )

__all__ = (
    "DEFAULT_SPANNER_ENDPOINT",
    "SPANGRES_DIALECT_TAG",
    "SPANNER_GOOGLESQL_DIALECT_TAG",
    "AsyncSpannerDriver",
    "AsyncSpannerSnapshot",
    "AsyncSpannerTransaction",
    "Mutation",
    "MutationGroup",
    "SpannerConfig",
    "SpannerCore",
    "StalenessBound",
    "SyncSpannerDriver",
    "SyncSpannerSnapshot",
    "SyncSpannerTransaction",
)

__lazy_modules__ = [
    "vertebra.db.adapters.spanner.config",
    "vertebra.db.adapters.spanner.core",
    "vertebra.db.adapters.spanner.driver",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "DEFAULT_SPANNER_ENDPOINT": ("vertebra.db.adapters.spanner.config", "DEFAULT_SPANNER_ENDPOINT"),
    "SPANGRES_DIALECT_TAG": ("vertebra.db.adapters.spanner.config", "SPANGRES_DIALECT_TAG"),
    "SPANNER_GOOGLESQL_DIALECT_TAG": ("vertebra.db.adapters.spanner.config", "SPANNER_GOOGLESQL_DIALECT_TAG"),
    "AsyncSpannerDriver": ("vertebra.db.adapters.spanner.driver", "AsyncSpannerDriver"),
    "AsyncSpannerSnapshot": ("vertebra.db.adapters.spanner.driver", "AsyncSpannerSnapshot"),
    "AsyncSpannerTransaction": ("vertebra.db.adapters.spanner.driver", "AsyncSpannerTransaction"),
    "Mutation": ("vertebra.db.adapters.spanner.config", "Mutation"),
    "MutationGroup": ("vertebra.db.adapters.spanner.config", "MutationGroup"),
    "SpannerConfig": ("vertebra.db.adapters.spanner.config", "SpannerConfig"),
    "SpannerCore": ("vertebra.db.adapters.spanner.core", "SpannerCore"),
    "StalenessBound": ("vertebra.db.adapters.spanner.config", "StalenessBound"),
    "SyncSpannerDriver": ("vertebra.db.adapters.spanner.driver", "SyncSpannerDriver"),
    "SyncSpannerSnapshot": ("vertebra.db.adapters.spanner.driver", "SyncSpannerSnapshot"),
    "SyncSpannerTransaction": ("vertebra.db.adapters.spanner.driver", "SyncSpannerTransaction"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
