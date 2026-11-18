"""MySQL event queue store implementing InnoDB DDL and companion index placement."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Final, NotRequired

from sqlspec.config import EventsConfig
from sqlspec.extensions.events import BaseEventQueueStore

from vertebra.extensions.sqlspec.adapters.mysql.core import mysql_table_options

if TYPE_CHECKING:
    from sqlspec.migrations.schema import DropStatement, SchemaTarget

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
    dialect: ClassVar[str] = "mysql"
    extension_config_options = BaseEventQueueStore.extension_config_options | frozenset({"table_options"})

    def get_schema_targets(self) -> list[SchemaTarget]:
        """Return ordered `SchemaTarget` definitions for the MySQL event queue table."""
        return super().get_schema_targets()

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Create the MySQL companion index only when absent in `data_dictionary.get_indexes()`."""
        super().reconcile_companion_objects_sync(driver)

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Create the MySQL companion index asynchronously only when absent in metadata."""
        await super().reconcile_companion_objects_async(driver)

    def downgrade_drops(self) -> list[DropStatement]:
        """Return MySQL downgrade drop statements for the event queue table."""
        return super().downgrade_drops()

    def _column_types(self) -> tuple[str, str, str]:
        """Return `(payload_type, metadata_type, timestamp_type)` for MySQL/MariaDB DDL."""
        return "JSON", "JSON", "DATETIME(6)"

    def _timestamp_default(self) -> str:
        """Return the MySQL microsecond timestamp default."""
        return "CURRENT_TIMESTAMP(6)"

    def _table_clause(self) -> str:
        """Return optional MySQL table DDL options configured under `extension_config['events']`."""
        return mysql_table_options(self.settings)

    def _index_ddl(self) -> str | None:
        """Return non-idempotent `ALTER TABLE ... ADD INDEX` DDL guarded by `get_indexes()` inspection."""
        return f"ALTER TABLE {self.table_name} ADD INDEX {self._index_name()} (channel, status, available_at)"

    def _index_existence_target(self) -> tuple[str | None, str] | None:
        """Return `(schema, table)` target tuple for `data_dictionary.get_indexes()`."""
        segments = self.table_name.split(".", 1)
        if len(segments) == _SCHEMA_QUALIFIED_SEGMENTS:
            return segments[0].strip("`"), segments[1].strip("`")
        return None, segments[0].strip("`")
