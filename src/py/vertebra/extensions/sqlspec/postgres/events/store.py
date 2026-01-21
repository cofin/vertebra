# ruff: noqa: S608
"""PostgreSQL event queue store for Vertebra."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlspec.extensions.events import BaseEventQueueStore

if TYPE_CHECKING:
    from vertebra.extensions.sqlspec.postgres.config import VertebraPostgresAsyncConfig


__all__ = ("PostgresEventQueueStore",)


class PostgresEventQueueStore(BaseEventQueueStore["VertebraPostgresAsyncConfig"]):
    """PostgreSQL event queue store with JSONB columns for Vertebra.

    Uses PostgreSQL-native JSONB for efficient JSON storage and querying.
    TIMESTAMPTZ ensures proper timezone handling.
    """

    __slots__ = ()

    def _column_types(self) -> tuple[str, str, str]:
        """Return PostgreSQL-native column types.

        Returns:
            Tuple of (payload_type, metadata_type, timestamp_type).
        """
        return "JSONB", "JSONB", "TIMESTAMPTZ"