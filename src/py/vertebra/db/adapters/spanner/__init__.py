"""Pure-Rust Google Cloud Spanner v1 sync and async drivers across GoogleSQL and Spangres dialects."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.db.adapters.spanner.change_stream import (
        AsyncSpannerChangeStreamSource,
        SpannerChangeStreamRecord,
        SpannerChildPartition,
        SyncSpannerChangeStreamSource,
    )
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
    from vertebra.db.adapters.spanner.embeddings import SpannerEmbeddingPushdown

__all__ = (
    "SpannerEmbeddingPushdown",
    "AsyncSpannerChangeStreamSource",
    "AsyncSpannerDriver",
    "AsyncSpannerSnapshot",
    "AsyncSpannerTransaction",
    "DEFAULT_SPANNER_ENDPOINT",
    "Mutation",
    "MutationGroup",
    "SPANGRES_DIALECT_TAG",
    "SPANNER_GOOGLESQL_DIALECT_TAG",
    "SpannerChangeStreamRecord",
    "SpannerChildPartition",
    "SpannerConfig",
    "SpannerCore",
    "StalenessBound",
    "SyncSpannerChangeStreamSource",
    "SyncSpannerDriver",
    "SyncSpannerSnapshot",
    "SyncSpannerTransaction",
)

__lazy_modules__ = [
    "vertebra.db.adapters.spanner.embeddings",
    "vertebra.db.adapters.spanner.change_stream",
    "vertebra.db.adapters.spanner.config",
    "vertebra.db.adapters.spanner.core",
    "vertebra.db.adapters.spanner.driver",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "SpannerEmbeddingPushdown": ("vertebra.db.adapters.spanner.embeddings", "SpannerEmbeddingPushdown"),
    "AsyncSpannerChangeStreamSource": ("vertebra.db.adapters.spanner.change_stream", "AsyncSpannerChangeStreamSource"),
    "AsyncSpannerDriver": ("vertebra.db.adapters.spanner.driver", "AsyncSpannerDriver"),
    "AsyncSpannerSnapshot": ("vertebra.db.adapters.spanner.driver", "AsyncSpannerSnapshot"),
    "AsyncSpannerTransaction": ("vertebra.db.adapters.spanner.driver", "AsyncSpannerTransaction"),
    "DEFAULT_SPANNER_ENDPOINT": ("vertebra.db.adapters.spanner.config", "DEFAULT_SPANNER_ENDPOINT"),
    "Mutation": ("vertebra.db.adapters.spanner.config", "Mutation"),
    "MutationGroup": ("vertebra.db.adapters.spanner.config", "MutationGroup"),
    "SPANGRES_DIALECT_TAG": ("vertebra.db.adapters.spanner.config", "SPANGRES_DIALECT_TAG"),
    "SPANNER_GOOGLESQL_DIALECT_TAG": ("vertebra.db.adapters.spanner.config", "SPANNER_GOOGLESQL_DIALECT_TAG"),
    "SpannerChangeStreamRecord": ("vertebra.db.adapters.spanner.change_stream", "SpannerChangeStreamRecord"),
    "SpannerChildPartition": ("vertebra.db.adapters.spanner.change_stream", "SpannerChildPartition"),
    "SpannerConfig": ("vertebra.db.adapters.spanner.config", "SpannerConfig"),
    "SpannerCore": ("vertebra.db.adapters.spanner.core", "SpannerCore"),
    "StalenessBound": ("vertebra.db.adapters.spanner.config", "StalenessBound"),
    "SyncSpannerChangeStreamSource": ("vertebra.db.adapters.spanner.change_stream", "SyncSpannerChangeStreamSource"),
    "SyncSpannerDriver": ("vertebra.db.adapters.spanner.driver", "SyncSpannerDriver"),
    "SyncSpannerSnapshot": ("vertebra.db.adapters.spanner.driver", "SyncSpannerSnapshot"),
    "SyncSpannerTransaction": ("vertebra.db.adapters.spanner.driver", "SyncSpannerTransaction"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
