"""MySQL event queue store implementing InnoDB DDL and companion index placement."""

from typing import Any, Final, NotRequired

from sqlspec.config import EventsConfig
from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("MySQLEventQueueStore", "MySQLEventsConfig")

_SCHEMA_QUALIFIED_SEGMENTS: Final[int] = 2


class MySQLEventsConfig(EventsConfig):
    """MySQL-specific events extension settings inside `extension_config['events']`."""

    table_options: NotRequired[str]
    """Raw MySQL table options appended to DDL statements."""


class MySQLEventQueueStore(BaseEventQueueStore[Any]):
    """MySQL and MariaDB event queue store with `DATETIME(6)` precision and `ALTER TABLE ADD INDEX`."""

    __slots__ = ()

    adapter_name = "vertebra_mysql"
    extension_config_options = BaseEventQueueStore.extension_config_options | frozenset({"table_options"})

    def _column_types(self) -> tuple[str, str, str]:
        """Return `(payload_type, metadata_type, timestamp_type)` for MySQL/MariaDB DDL."""
        return "JSON", "JSON", "DATETIME(6)"

    def _timestamp_default(self) -> str:
        """Return the MySQL microsecond timestamp default."""
        return "CURRENT_TIMESTAMP(6)"

    def _index_ddl(self) -> str | None:
        """Return non-idempotent `ALTER TABLE ... ADD INDEX` DDL guarded by `get_indexes()` inspection."""
        return f"ALTER TABLE {self.table_name} ADD INDEX {self._index_name()} (channel, status, available_at)"

    def _index_existence_target(self) -> tuple[str | None, str] | None:
        """Return `(schema, table)` target tuple for `data_dictionary.get_indexes()`."""
        segments = self.table_name.split(".", 1)
        if len(segments) == _SCHEMA_QUALIFIED_SEGMENTS:
            return segments[0], segments[1]
        return None, segments[0]
