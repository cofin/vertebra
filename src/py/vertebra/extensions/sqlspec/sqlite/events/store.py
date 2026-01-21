"""SQLite event queue store for Vertebra."""

from __future__ import annotations

from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("SqliteEventQueueStore",)


class SqliteEventQueueStore(BaseEventQueueStore["VertebraSqliteAsyncConfig"]):
    """SQLite event queue store for Vertebra.

    Uses TEXT for JSON fields and TIMESTAMP for temporal fields.
    """

    __slots__ = ()

    def _column_types(self) -> tuple[str, str, str]:
        """Return SQLite-compatible column types for the event queue.

        Returns:
            Tuple of (payload_type, metadata_type, timestamp_type).
        """
        return "TEXT", "TEXT", "TIMESTAMP"
