"""Pure-Rust Google Cloud Spanner v1 sync and async drivers across GoogleSQL and Spangres dialects."""

from __future__ import annotations

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
