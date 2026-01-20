"""PostgreSQL data dictionary helpers for broker-backed adapters."""

from typing import TYPE_CHECKING, ClassVar

from sqlspec.driver import AsyncDataDictionaryBase, SyncDataDictionaryBase
from sqlspec.typing import ColumnMetadata, ForeignKeyMetadata, IndexMetadata, TableMetadata, VersionInfo

if TYPE_CHECKING:
    from vertebra.extensions.sqlspec.postgres.driver import VertebraPostgresAsyncDriver, VertebraPostgresSyncDriver

__all__ = ("VertebraPostgresAsyncDataDictionary", "VertebraPostgresSyncDataDictionary")


class VertebraPostgresSyncDataDictionary(SyncDataDictionaryBase):
    """PostgreSQL-specific sync data dictionary."""

    dialect: ClassVar[str] = "postgres"

    def __init__(self) -> None:
        super().__init__()

    def get_version(self, driver: "VertebraPostgresSyncDriver") -> "VersionInfo | None":
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)

        version_value = driver.select_value_or_none(self.get_query("version"))
        if not version_value:
            self._log_version_unavailable(type(self).dialect, "missing")
            self.cache_version(driver_id, None)
            return None

        version_info = self.parse_version_with_pattern(self.get_dialect_config().version_pattern, str(version_value))
        if version_info is None:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            self.cache_version(driver_id, None)
            return None

        self._log_version_detected(type(self).dialect, version_info)
        self.cache_version(driver_id, version_info)
        return version_info

    def get_feature_flag(self, driver: "VertebraPostgresSyncDriver", feature: str) -> bool:
        version_info = self.get_version(driver)
        return self.resolve_feature_flag(feature, version_info)

    def get_optimal_type(self, driver: "VertebraPostgresSyncDriver", type_category: str) -> str:
        config = self.get_dialect_config()
        version_info = self.get_version(driver)

        if type_category == "json":
            jsonb_version = config.get_feature_version("supports_jsonb")
            json_version = config.get_feature_version("supports_json")
            if version_info and jsonb_version and version_info >= jsonb_version:
                return "JSONB"
            if version_info and json_version and version_info >= json_version:
                return "JSON"
            return "TEXT"

        return config.get_optimal_type(type_category)

    def get_tables(self, driver: "VertebraPostgresSyncDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        return driver.select(self.get_query("tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata)

    def get_columns(
        self, driver: "VertebraPostgresSyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            return driver.select(
                self.get_query("columns_by_schema"), schema_name=schema_name, schema_type=ColumnMetadata
            )

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="columns")
        return driver.select(
            self.get_query("columns_by_table"), schema_name=schema_name, table_name=table, schema_type=ColumnMetadata
        )

    def get_indexes(
        self, driver: "VertebraPostgresSyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            return driver.select(
                self.get_query("indexes_by_schema"), schema_name=schema_name, schema_type=IndexMetadata
            )

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="indexes")
        return driver.select(
            self.get_query("indexes_by_table"), schema_name=schema_name, table_name=table, schema_type=IndexMetadata
        )

    def get_foreign_keys(
        self, driver: "VertebraPostgresSyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            return driver.select(
                self.get_query("foreign_keys_by_schema"), schema_name=schema_name, schema_type=ForeignKeyMetadata
            )
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
        return driver.select(
            self.get_query("foreign_keys_by_table"),
            schema_name=schema_name,
            table_name=table,
            schema_type=ForeignKeyMetadata,
        )


class VertebraPostgresAsyncDataDictionary(AsyncDataDictionaryBase):
    """PostgreSQL-specific async data dictionary."""

    dialect: ClassVar[str] = "postgres"

    def __init__(self) -> None:
        super().__init__()

    async def get_version(self, driver: "VertebraPostgresAsyncDriver") -> "VersionInfo | None":
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)

        version_value = await driver.select_value_or_none(self.get_query("version"))
        if not version_value:
            self._log_version_unavailable(type(self).dialect, "missing")
            self.cache_version(driver_id, None)
            return None

        version_info = self.parse_version_with_pattern(self.get_dialect_config().version_pattern, str(version_value))
        if version_info is None:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            self.cache_version(driver_id, None)
            return None

        self._log_version_detected(type(self).dialect, version_info)
        self.cache_version(driver_id, version_info)
        return version_info

    async def get_feature_flag(self, driver: "VertebraPostgresAsyncDriver", feature: str) -> bool:
        version_info = await self.get_version(driver)
        return self.resolve_feature_flag(feature, version_info)

    async def get_optimal_type(self, driver: "VertebraPostgresAsyncDriver", type_category: str) -> str:
        config = self.get_dialect_config()
        version_info = await self.get_version(driver)

        if type_category == "json":
            jsonb_version = config.get_feature_version("supports_jsonb")
            json_version = config.get_feature_version("supports_json")
            if version_info and jsonb_version and version_info >= jsonb_version:
                return "JSONB"
            if version_info and json_version and version_info >= json_version:
                return "JSON"
            return "TEXT"

        return config.get_optimal_type(type_category)

    async def get_tables(
        self, driver: "VertebraPostgresAsyncDriver", schema: "str | None" = None
    ) -> "list[TableMetadata]":
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        return await driver.select(
            self.get_query("tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata
        )

    async def get_columns(
        self, driver: "VertebraPostgresAsyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            return await driver.select(
                self.get_query("columns_by_schema"), schema_name=schema_name, schema_type=ColumnMetadata
            )

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="columns")
        return await driver.select(
            self.get_query("columns_by_table"), schema_name=schema_name, table_name=table, schema_type=ColumnMetadata
        )

    async def get_indexes(
        self, driver: "VertebraPostgresAsyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            return await driver.select(
                self.get_query("indexes_by_schema"), schema_name=schema_name, schema_type=IndexMetadata
            )

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="indexes")
        return await driver.select(
            self.get_query("indexes_by_table"), schema_name=schema_name, table_name=table, schema_type=IndexMetadata
        )

    async def get_foreign_keys(
        self, driver: "VertebraPostgresAsyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            return await driver.select(
                self.get_query("foreign_keys_by_schema"), schema_name=schema_name, schema_type=ForeignKeyMetadata
            )
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
        return await driver.select(
            self.get_query("foreign_keys_by_table"),
            schema_name=schema_name,
            table_name=table,
            schema_type=ForeignKeyMetadata,
        )
