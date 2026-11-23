"""DuckDB synchronous and asynchronous SQLSpec data dictionaries."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from sqlspec.data_dictionary.dialects.duckdb.dictionary import (
    DuckDBAsyncDataDictionary as _UpstreamDuckDBAsyncDataDictionary,
)
from sqlspec.data_dictionary.dialects.duckdb.dictionary import (
    DuckDBSyncDataDictionary as _UpstreamDuckDBSyncDataDictionary,
)

if TYPE_CHECKING:
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

__all__ = (
    "DuckDBAsyncDataDictionary",
    "DuckDBSyncDataDictionary",
)


class DuckDBSyncDataDictionary(_UpstreamDuckDBSyncDataDictionary):
    """Synchronous DuckDB data dictionary backed by SQLSpec's shared dialect base."""

    __slots__ = ()

    dialect: ClassVar[str] = "duckdb"

    def resolve_connection_schema(self, driver: SyncDriverAdapterBase, schema: str | None) -> str | None:
        """Resolve an explicit schema or query DuckDB's `current_schema()`."""
        if schema is not None:
            return self.resolve_schema(schema)
        current_schema = driver.select_value_or_none("SELECT current_schema()")
        return str(current_schema) if current_schema else self.get_dialect_config().default_schema

    def resolve_ddl_object_name(
        self,
        driver: SyncDriverAdapterBase,
        object_name: str,
        schema: str | None,
    ) -> tuple[str, str | None]:
        """Normalize an object identifier and resolve its target DuckDB schema."""
        return (
            self.resolve_identifier(object_name),
            self.resolve_connection_schema(driver, schema),
        )


class DuckDBAsyncDataDictionary(_UpstreamDuckDBAsyncDataDictionary):
    """Asynchronous DuckDB data dictionary backed by SQLSpec's shared dialect base."""

    __slots__ = ()

    dialect: ClassVar[str] = "duckdb"

    async def resolve_connection_schema(self, driver: AsyncDriverAdapterBase, schema: str | None) -> str | None:
        """Resolve an explicit schema or query DuckDB's `current_schema()` asynchronously."""
        if schema is not None:
            return self.resolve_schema(schema)
        current_schema = await driver.select_value_or_none("SELECT current_schema()")
        return str(current_schema) if current_schema else self.get_dialect_config().default_schema
