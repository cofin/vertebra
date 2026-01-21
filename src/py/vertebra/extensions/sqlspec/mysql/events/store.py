# ruff: noqa: S608
"""MySQL event queue store for Vertebra."""

from __future__ import annotations

from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("MysqlEventQueueStore",)


class MysqlEventQueueStore(BaseEventQueueStore["VertebraMysqlAsyncConfig"]):
    """MySQL event queue store for Vertebra.

    Uses JSON for payload/metadata and DATETIME(6) for microsecond timestamps.
    Handles conditional index creation for MySQL.
    """

    __slots__ = ()

    def _column_types(self) -> tuple[str, str, str]:
        return "JSON", "JSON", "DATETIME(6)"

    def _timestamp_default(self) -> str:
        return "CURRENT_TIMESTAMP(6)"

    def _build_index_sql(self) -> str | None:
        table_name = self.table_name
        segments = table_name.split(".", 1)

        if len(segments) == 2:
            schema = segments[0]
            table = segments[1]
            schema_selector = f"'{schema}'"
        else:
            table = segments[0]
            schema_selector = "DATABASE()"

        index_name = self._index_name()

        return (
            "SET @sqlspec_events_idx_exists := ("
            "SELECT COUNT(1) FROM information_schema.statistics "
            f"WHERE table_schema = {schema_selector} "
            f"AND table_name = '{table}' "
            f"AND index_name = '{index_name}');"
            "SET @sqlspec_events_idx_stmt := IF(@sqlspec_events_idx_exists = 0, "
            f"'ALTER TABLE {table_name} ADD INDEX {index_name} (channel, status, available_at)', "
            "'SELECT 1');"
            "PREPARE sqlspec_events_stmt FROM @sqlspec_events_idx_stmt;"
            "EXECUTE sqlspec_events_stmt;"
            "DEALLOCATE PREPARE sqlspec_events_stmt;"
        )
