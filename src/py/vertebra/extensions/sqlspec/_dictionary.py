"""Data dictionary factories for broker-backed SQLSpec adapters."""

from collections.abc import Callable
from typing import Any, ClassVar

from sqlspec.adapters.aiosqlite.data_dictionary import AiosqliteDataDictionary
from sqlspec.adapters.asyncmy.data_dictionary import AsyncmyDataDictionary
from sqlspec.adapters.asyncpg.data_dictionary import AsyncpgDataDictionary
from sqlspec.adapters.psycopg.data_dictionary import PsycopgSyncDataDictionary
from sqlspec.adapters.pymysql.data_dictionary import PyMysqlDataDictionary
from sqlspec.adapters.sqlite.data_dictionary import SqliteDataDictionary
from sqlspec.driver import AsyncDataDictionaryBase, SyncDataDictionaryBase
from sqlspec.exceptions import SQLFileNotFoundError
from sqlspec.typing import ColumnMetadata, ForeignKeyMetadata, IndexMetadata, TableMetadata, VersionInfo


class _GenericSyncDataDictionary(SyncDataDictionaryBase):
    dialect: ClassVar[str] = "generic"

    def __init__(self) -> None:
        super().__init__()

    def get_version(self, driver: Any) -> "VersionInfo | None":
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)

        try:
            version_value = driver.select_value_or_none(self.get_query("version"))
        except SQLFileNotFoundError:
            self.cache_version(driver_id, None)
            return None

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

    def get_feature_flag(self, driver: Any, feature: str) -> bool:
        version_info = self.get_version(driver)
        return self.resolve_feature_flag(feature, version_info)

    def get_optimal_type(self, driver: Any, type_category: str) -> str:
        _ = driver
        config = self.get_dialect_config()
        version_info = self.get_version(driver)

        if type_category == "json":
            json_version = config.get_feature_version("supports_json")
            if version_info and json_version and version_info >= json_version:
                return "JSON"
            return "TEXT"

        return config.get_optimal_type(type_category)

    def get_tables(self, driver: Any, schema: "str | None" = None) -> "list[TableMetadata]":
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        try:
            return driver.select(self.get_query("tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata)
        except SQLFileNotFoundError:
            return []

    def get_columns(
        self, driver: Any, table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            try:
                return driver.select(
                    self.get_query("columns_by_schema"), schema_name=schema_name, schema_type=ColumnMetadata
                )
            except SQLFileNotFoundError:
                return []

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="columns")
        try:
            return driver.select(
                self.get_query("columns_by_table"),
                schema_name=schema_name,
                table_name=table,
                schema_type=ColumnMetadata,
            )
        except SQLFileNotFoundError:
            return []

    def get_indexes(
        self, driver: Any, table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            try:
                return driver.select(
                    self.get_query("indexes_by_schema"), schema_name=schema_name, schema_type=IndexMetadata
                )
            except SQLFileNotFoundError:
                return []

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="indexes")
        try:
            return driver.select(
                self.get_query("indexes_by_table"), schema_name=schema_name, table_name=table, schema_type=IndexMetadata
            )
        except SQLFileNotFoundError:
            return []

    def get_foreign_keys(
        self, driver: Any, table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            try:
                return driver.select(
                    self.get_query("foreign_keys_by_schema"), schema_name=schema_name, schema_type=ForeignKeyMetadata
                )
            except SQLFileNotFoundError:
                return []

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
        try:
            return driver.select(
                self.get_query("foreign_keys_by_table"),
                schema_name=schema_name,
                table_name=table,
                schema_type=ForeignKeyMetadata,
            )
        except SQLFileNotFoundError:
            return []


class _GenericAsyncDataDictionary(AsyncDataDictionaryBase):
    dialect: ClassVar[str] = "generic"

    def __init__(self) -> None:
        super().__init__()

    async def get_version(self, driver: Any) -> "VersionInfo | None":
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)

        try:
            version_value = await driver.select_value_or_none(self.get_query("version"))
        except SQLFileNotFoundError:
            self.cache_version(driver_id, None)
            return None

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

    async def get_feature_flag(self, driver: Any, feature: str) -> bool:
        version_info = await self.get_version(driver)
        return self.resolve_feature_flag(feature, version_info)

    async def get_optimal_type(self, driver: Any, type_category: str) -> str:
        _ = driver
        config = self.get_dialect_config()
        version_info = await self.get_version(driver)

        if type_category == "json":
            json_version = config.get_feature_version("supports_json")
            if version_info and json_version and version_info >= json_version:
                return "JSON"
            return "TEXT"

        return config.get_optimal_type(type_category)

    async def get_tables(self, driver: Any, schema: "str | None" = None) -> "list[TableMetadata]":
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        try:
            return await driver.select(
                self.get_query("tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata
            )
        except SQLFileNotFoundError:
            return []

    async def get_columns(
        self, driver: Any, table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            try:
                return await driver.select(
                    self.get_query("columns_by_schema"), schema_name=schema_name, schema_type=ColumnMetadata
                )
            except SQLFileNotFoundError:
                return []

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="columns")
        try:
            return await driver.select(
                self.get_query("columns_by_table"),
                schema_name=schema_name,
                table_name=table,
                schema_type=ColumnMetadata,
            )
        except SQLFileNotFoundError:
            return []

    async def get_indexes(
        self, driver: Any, table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            try:
                return await driver.select(
                    self.get_query("indexes_by_schema"), schema_name=schema_name, schema_type=IndexMetadata
                )
            except SQLFileNotFoundError:
                return []

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="indexes")
        try:
            return await driver.select(
                self.get_query("indexes_by_table"), schema_name=schema_name, table_name=table, schema_type=IndexMetadata
            )
        except SQLFileNotFoundError:
            return []

    async def get_foreign_keys(
        self, driver: Any, table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            try:
                return await driver.select(
                    self.get_query("foreign_keys_by_schema"), schema_name=schema_name, schema_type=ForeignKeyMetadata
                )
            except SQLFileNotFoundError:
                return []

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
        try:
            return await driver.select(
                self.get_query("foreign_keys_by_table"),
                schema_name=schema_name,
                table_name=table,
                schema_type=ForeignKeyMetadata,
            )
        except SQLFileNotFoundError:
            return []


def _generic_sync_factory(dialect: str) -> Callable[[], SyncDataDictionaryBase]:
    class _DialectDataDictionary(_GenericSyncDataDictionary):
        dialect: ClassVar[str] = dialect

    return _DialectDataDictionary


def _generic_async_factory(dialect: str) -> Callable[[], AsyncDataDictionaryBase]:
    class _DialectDataDictionary(_GenericAsyncDataDictionary):
        dialect: ClassVar[str] = dialect

    return _DialectDataDictionary


def resolve_sync_data_dictionary(dialect: str) -> Callable[[], SyncDataDictionaryBase]:
    normalized = dialect.lower() if dialect else ""
    if normalized.startswith("postgres"):
        return PsycopgSyncDataDictionary
    if normalized.startswith("mysql"):
        return PyMysqlDataDictionary
    if normalized.startswith("sqlite"):
        return SqliteDataDictionary
    return _generic_sync_factory(normalized or "generic")


def resolve_async_data_dictionary(dialect: str) -> Callable[[], AsyncDataDictionaryBase]:
    normalized = dialect.lower() if dialect else ""
    if normalized.startswith("postgres"):
        return AsyncpgDataDictionary
    if normalized.startswith("mysql"):
        return AsyncmyDataDictionary
    if normalized.startswith("sqlite"):
        return AiosqliteDataDictionary
    return _generic_async_factory(normalized or "generic")


__all__ = ("resolve_async_data_dictionary", "resolve_sync_data_dictionary")
