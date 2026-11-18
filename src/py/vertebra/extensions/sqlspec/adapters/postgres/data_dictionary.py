"""PostgreSQL data dictionary executing SQLSpec `dialects/postgres/sql/*.sql` catalog queries."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, cast

from sqlspec.data_dictionary import (
    ColumnMetadata,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataCapability,
    MetadataFidelity,
    MetadataResult,
    MetadataSource,
    MetadataSupport,
    TableMetadata,
    VersionInfo,
    get_data_dictionary_loader,
)
from sqlspec.data_dictionary.dialects.postgres import (
    POSTGRES_CONFIG,
    POSTGRES_VERSION_PATTERN,
    resolve_postgres_json_type,
)
from sqlspec.driver import AsyncDataDictionaryBase, SyncDataDictionaryBase
from sqlspec.exceptions import SQLFileNotFoundError
from sqlspec.utils.serializers import from_json

from vertebra.extensions.sqlspec.adapters.postgres.core import pg_row_value

if TYPE_CHECKING:
    from sqlspec.core import SQL
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

__all__ = ("PostgresAsyncDataDictionary", "PostgresSyncDataDictionary")


def _postgres_query(domain: str, operation: str) -> SQL:
    """Resolve a required PostgreSQL catalog query via the SQLSpec dialect loader."""
    query = get_data_dictionary_loader().get_domain_query("postgres", domain, operation)
    if query.sql is None:
        msg = f"No data-dictionary query found for postgres/{domain}/{operation}"
        raise SQLFileNotFoundError(msg)
    return query.sql


def _schema_capability() -> MetadataCapability:
    return MetadataCapability(
        domain="schemas",
        support=MetadataSupport.SUPPORTED,
        fidelity=MetadataFidelity.NATIVE,
        source=MetadataSource.CATALOG,
    )


def _extract_row_str(row: Any, *keys: str) -> str | None:
    """Return the first non-empty string field found across candidate column names."""
    for idx, key in enumerate(keys):
        value = pg_row_value(row, key, idx)
        if value is not None:
            text = str(value).strip()
            if text:
                return text
    return None


def _rows_as_dicts(rows: Sequence[Any] | None) -> list[dict[str, Any]]:
    """Normalize catalog rows into plain dictionaries."""
    if not rows:
        return []
    result: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, Mapping):
            result.append(dict(row))
        else:
            result.append({"value": row})
    return result


def _parse_postgres_version(
    dictionary: SyncDataDictionaryBase | AsyncDataDictionaryBase, raw_value: Any
) -> VersionInfo | None:
    """Parse PostgreSQL version string using `POSTGRES_VERSION_PATTERN` with dotted fallback."""
    if raw_value is None:
        return None
    text = str(raw_value).strip()
    if not text:
        return None
    parsed = dictionary.parse_version_with_pattern(POSTGRES_VERSION_PATTERN, text)
    if isinstance(parsed, VersionInfo):
        return parsed
    parts = [part for part in text.split(".") if part.isdigit()]
    if not parts:
        return None
    major = int(parts[0])
    minor = int(parts[1]) if len(parts) > 1 else 0
    patch = int(parts[2]) if len(parts) > 2 else 0
    return VersionInfo(major=major, minor=minor, patch=patch)


def _resolve_optimal_postgres_type(version_info: VersionInfo | None, type_category: str) -> str:
    """Resolve the optimal PostgreSQL SQL type for `type_category`."""
    if type_category in {"json", "jsonb"}:
        return resolve_postgres_json_type(version_info)
    return POSTGRES_CONFIG.get_optimal_type(type_category)


def _decode_json_field(value: Any) -> Any:
    """Decode JSON-encoded catalog columns when returned as raw strings."""
    if isinstance(value, (dict, list)) or value is None:
        return value
    try:
        return from_json(str(value))
    except (TypeError, ValueError):
        return value


class PostgresSyncDataDictionary(SyncDataDictionaryBase):
    """Synchronous PostgreSQL data dictionary backed by SQLSpec catalog SQL queries."""

    __slots__ = ()

    dialect: ClassVar[str] = "postgres"

    def get_version(self, driver: SyncDriverAdapterBase) -> VersionInfo | None:
        """Query `SELECT version()` / PostgreSQL catalog version and cache per driver."""
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)

        row = driver.select_one_or_none(_postgres_query("version", "current"))
        if row is None:
            self._log_version_unavailable(type(self).dialect, "missing")
            self.cache_version(driver_id, None)
            return None

        raw_version = _extract_row_str(row, "version", "server_version", "version_num")
        version_info = _parse_postgres_version(self, raw_version)
        if version_info is None:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            self.cache_version(driver_id, None)
            return None

        self._log_version_detected(type(self).dialect, version_info)
        self.cache_version(driver_id, version_info)
        return version_info

    def get_feature_flag(self, driver: SyncDriverAdapterBase, feature: str) -> bool:
        """Evaluate PostgreSQL feature availability against detected server version."""
        return self.resolve_feature_flag(feature, self.get_version(driver))

    def get_optimal_type(self, driver: SyncDriverAdapterBase, type_category: str) -> str:
        """Return the optimal PostgreSQL SQL type for `type_category`."""
        return _resolve_optimal_postgres_type(self.get_version(driver), type_category)

    def get_schemas(self, driver: SyncDriverAdapterBase) -> MetadataResult:
        """List accessible non-system PostgreSQL schemas."""
        rows = driver.select(_postgres_query("schemas", "list"))
        return MetadataResult("schemas", capability=_schema_capability(), items=tuple(_rows_as_dicts(rows)))

    def get_tables(self, driver: SyncDriverAdapterBase, schema: str | None = None) -> list[TableMetadata]:
        """Return PostgreSQL tables and views in `schema` (default `public`)."""
        schema_name = self.resolve_schema(schema)
        rows = driver.select(_postgres_query("tables", "by_schema"), schema_name=schema_name, table_name=None)
        return cast("list[TableMetadata]", _rows_as_dicts(rows))

    def get_columns(
        self,
        driver: SyncDriverAdapterBase,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[ColumnMetadata]:
        """Return PostgreSQL column metadata for one table or an entire schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            rows = driver.select(_postgres_query("columns", "by_schema"), schema_name=schema_name, table_name=None)
        else:
            rows = driver.select(
                _postgres_query("columns", "by_table"),
                schema_name=schema_name,
                table_name=self.resolve_identifier(table),
            )
        return cast("list[ColumnMetadata]", _rows_as_dicts(rows))

    def get_indexes(
        self,
        driver: SyncDriverAdapterBase,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[IndexMetadata]:
        """Return PostgreSQL index definitions for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            rows = driver.select(_postgres_query("indexes", "by_schema"), schema_name=schema_name, table_name=None)
        else:
            rows = driver.select(
                _postgres_query("indexes", "by_table"),
                schema_name=schema_name,
                table_name=self.resolve_identifier(table),
            )
        results = _rows_as_dicts(rows)
        for item in results:
            if "columns" in item:
                item["columns"] = _decode_json_field(item["columns"])
        return cast("list[IndexMetadata]", results)

    def get_foreign_keys(
        self,
        driver: SyncDriverAdapterBase,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[ForeignKeyMetadata]:
        """Return PostgreSQL foreign key constraints for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            rows = driver.select(_postgres_query("foreign_keys", "by_schema"), schema_name=schema_name)
        else:
            rows = driver.select(
                _postgres_query("foreign_keys", "by_table"),
                schema_name=schema_name,
                table_name=self.resolve_identifier(table),
            )
        return cast("list[ForeignKeyMetadata]", _rows_as_dicts(rows))


class PostgresAsyncDataDictionary(AsyncDataDictionaryBase):
    """Asynchronous PostgreSQL data dictionary backed by SQLSpec catalog SQL queries."""

    __slots__ = ()

    dialect: ClassVar[str] = "postgres"

    async def get_version(self, driver: AsyncDriverAdapterBase) -> VersionInfo | None:
        """Query PostgreSQL server version asynchronously and cache per driver."""
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)

        row = await driver.select_one_or_none(_postgres_query("version", "current"))
        if row is None:
            self._log_version_unavailable(type(self).dialect, "missing")
            self.cache_version(driver_id, None)
            return None

        raw_version = _extract_row_str(row, "version", "server_version", "version_num")
        version_info = _parse_postgres_version(self, raw_version)
        if version_info is None:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            self.cache_version(driver_id, None)
            return None

        self._log_version_detected(type(self).dialect, version_info)
        self.cache_version(driver_id, version_info)
        return version_info

    async def get_feature_flag(self, driver: AsyncDriverAdapterBase, feature: str) -> bool:
        """Evaluate PostgreSQL feature availability asynchronously."""
        return self.resolve_feature_flag(feature, await self.get_version(driver))

    async def get_optimal_type(self, driver: AsyncDriverAdapterBase, type_category: str) -> str:
        """Return the optimal PostgreSQL SQL type for `type_category`."""
        return _resolve_optimal_postgres_type(await self.get_version(driver), type_category)

    async def get_schemas(self, driver: AsyncDriverAdapterBase) -> MetadataResult:
        """List accessible non-system PostgreSQL schemas asynchronously."""
        rows = await driver.select(_postgres_query("schemas", "list"))
        return MetadataResult("schemas", capability=_schema_capability(), items=tuple(_rows_as_dicts(rows)))

    async def get_tables(self, driver: AsyncDriverAdapterBase, schema: str | None = None) -> list[TableMetadata]:
        """Return PostgreSQL tables and views in `schema` asynchronously."""
        schema_name = self.resolve_schema(schema)
        rows = await driver.select(_postgres_query("tables", "by_schema"), schema_name=schema_name, table_name=None)
        return cast("list[TableMetadata]", _rows_as_dicts(rows))

    async def get_columns(
        self,
        driver: AsyncDriverAdapterBase,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[ColumnMetadata]:
        """Return PostgreSQL column metadata asynchronously."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            rows = await driver.select(
                _postgres_query("columns", "by_schema"), schema_name=schema_name, table_name=None
            )
        else:
            rows = await driver.select(
                _postgres_query("columns", "by_table"),
                schema_name=schema_name,
                table_name=self.resolve_identifier(table),
            )
        return cast("list[ColumnMetadata]", _rows_as_dicts(rows))

    async def get_indexes(
        self,
        driver: AsyncDriverAdapterBase,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[IndexMetadata]:
        """Return PostgreSQL index definitions asynchronously."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            rows = await driver.select(
                _postgres_query("indexes", "by_schema"), schema_name=schema_name, table_name=None
            )
        else:
            rows = await driver.select(
                _postgres_query("indexes", "by_table"),
                schema_name=schema_name,
                table_name=self.resolve_identifier(table),
            )
        results = _rows_as_dicts(rows)
        for item in results:
            if "columns" in item:
                item["columns"] = _decode_json_field(item["columns"])
        return cast("list[IndexMetadata]", results)

    async def get_foreign_keys(
        self,
        driver: AsyncDriverAdapterBase,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[ForeignKeyMetadata]:
        """Return PostgreSQL foreign key constraints asynchronously."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            rows = await driver.select(_postgres_query("foreign_keys", "by_schema"), schema_name=schema_name)
        else:
            rows = await driver.select(
                _postgres_query("foreign_keys", "by_table"),
                schema_name=schema_name,
                table_name=self.resolve_identifier(table),
            )
        return cast("list[ForeignKeyMetadata]", _rows_as_dicts(rows))
