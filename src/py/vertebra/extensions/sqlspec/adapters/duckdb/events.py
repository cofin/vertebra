"""DuckDB event queue store over Vertebra `DuckDBSyncConfig` / `DuckDBAsyncConfig`."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from sqlspec.extensions.events import BaseEventQueueStore
from sqlspec.migrations.schema import SchemaTarget

from vertebra.extensions.sqlspec.adapters.duckdb.core import split_schema_and_table

if TYPE_CHECKING:
    from sqlspec.migrations.schema import DropStatement

__all__ = ("DuckDBEventQueueStore",)


class DuckDBEventQueueStore(BaseEventQueueStore[Any]):
    """DuckDB event queue store using serialized JSON payloads and `TIMESTAMP` ordering."""

    __slots__ = ()

    adapter_name = "vertebra_duckdb"
    dialect: ClassVar[str] = "duckdb"

    def _schema_target(self) -> SchemaTarget:
        """Build a DuckDB `SchemaTarget` from the canonical queue table DDL."""
        if "." in self.table_name:
            schema_name, bare_table = split_schema_and_table(self.table_name)
            return SchemaTarget.from_ddl(bare_table, self.create_statements()[0], schema=schema_name, dialect="duckdb")
        bare_table = self.table_name.strip('"')
        return SchemaTarget.from_ddl(bare_table, self.create_statements()[0], dialect="duckdb")

    def get_schema_targets(self) -> list[SchemaTarget]:
        """Return ordered `SchemaTarget` definitions for the DuckDB event queue table."""
        return super().get_schema_targets()

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Create the DuckDB companion queue index synchronously when absent."""
        super().reconcile_companion_objects_sync(driver)

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Create the DuckDB companion queue index asynchronously when absent."""
        await super().reconcile_companion_objects_async(driver)

    def downgrade_drops(self) -> list[DropStatement]:
        """Return DuckDB downgrade drop statements for the event queue table."""
        return super().downgrade_drops()

    def _column_types(self) -> tuple[str, str, str]:
        """Return `(payload_type, metadata_type, timestamp_type)` for DuckDB DDL."""
        return "VARCHAR", "VARCHAR", "TIMESTAMP"
