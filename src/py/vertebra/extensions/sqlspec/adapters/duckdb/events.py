"""DuckDB event queue store over Vertebra `DuckDBSyncConfig` / `DuckDBAsyncConfig`."""

from typing import Any

from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("DuckDBEventQueueStore",)


class DuckDBEventQueueStore(BaseEventQueueStore[Any]):
    """DuckDB event queue store using serialized JSON payloads and `TIMESTAMP` ordering."""

    __slots__ = ()

    def _column_types(self) -> tuple[str, str, str]:
        return "VARCHAR", "VARCHAR", "TIMESTAMP"
