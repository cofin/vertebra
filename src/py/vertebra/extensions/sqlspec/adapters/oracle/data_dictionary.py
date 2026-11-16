"""Oracle data dictionary executing SQLSpec `dialects/oracle/sql/*.sql` catalog queries."""

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
)
from sqlspec.data_dictionary.dialects.oracle import (
    ORACLE_CONFIG,
    ORACLE_VERSION_PATTERN,
    extract_oracle_version_value,
    parse_oracle_version_components,
    resolve_oracle_feature_flag,
    resolve_oracle_json_type,
)
from sqlspec.driver import AsyncDataDictionaryBase, SyncDataDictionaryBase
from sqlspec.utils.serializers import from_json

from vertebra.extensions.sqlspec.adapters.oracle.core import oracle_row_value

if TYPE_CHECKING:
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

__all__ = ("OracleAsyncDataDictionary", "OracleSyncDataDictionary")


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
        value = oracle_row_value(row, key, idx)
        if value is not None:
            text = str(value).strip()
            if text:
                return text
    return None


def _rows_as_dicts(rows: Sequence[Any] | None) -> list[dict[str, Any]]:
    """Normalize Oracle catalog rows into lowercase-keyed dictionaries."""
    if not rows:
        return []
    result: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, Mapping):
            result.append({str(k).lower(): v for k, v in row.items()})
        else:
            result.append({"value": row})
    return result


class _OracleDataDictionaryMixin:
    """Shared Oracle version parsing, 19c/23ai feature detection, and type helpers."""

    __slots__ = ()

    @staticmethod
    def _parse_version_from_row(dictionary: Any, row: Any) -> VersionInfo | None:
        raw_val = extract_oracle_version_value(row)
        if raw_val:
            parsed = dictionary.parse_version_with_pattern(ORACLE_VERSION_PATTERN, str(raw_val))
            if isinstance(parsed, VersionInfo):
                return parsed
            parts = parse_oracle_version_components(str(raw_val))
            if parts is not None:
                major, minor, patch = parts
                if major > 0:
                    return VersionInfo(major=major, minor=minor, patch=patch)
        fallback = _extract_row_str(row, "version_full", "version", "banner_full", "banner")
        if fallback is None:
            return None
        fallback_parts = parse_oracle_version_components(fallback)
        if fallback_parts is None:
            return None
        major, minor, patch = fallback_parts
        return VersionInfo(major=major, minor=minor, patch=patch) if major > 0 else None

    @staticmethod
    def _decode_json_field(value: Any) -> Any:
        if isinstance(value, (dict, list)) or value is None:
            return value
        try:
            return from_json(str(value))
        except (TypeError, ValueError):
            return value


class OracleSyncDataDictionary(_OracleDataDictionaryMixin, SyncDataDictionaryBase):
    """Synchronous Oracle data dictionary backed by SQLSpec `oracle/sql/*.sql` catalog queries."""

    dialect: ClassVar[str] = "oracle"

    def get_version(self, driver: SyncDriverAdapterBase) -> VersionInfo | None:
        """Query Oracle server version and cache per driver instance."""
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)

        row = driver.select_one_or_none(self.get_query("version", "current"))
        if row is None:
            self._log_version_unavailable(type(self).dialect, "missing")
            self.cache_version(driver_id, None)
            return None

        version_info = self._parse_version_from_row(self, row)
        if version_info is None:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            self.cache_version(driver_id, None)
            return None

        self._log_version_detected(type(self).dialect, version_info)
        self.cache_version(driver_id, version_info)
        return version_info

    def get_feature_flag(self, driver: SyncDriverAdapterBase, feature: str) -> bool:
        """Evaluate Oracle 19c/23ai feature availability against detected server version."""
        version_info = self.get_version(driver)
        return resolve_oracle_feature_flag(
            ORACLE_CONFIG,
            version_info,
            feature,
            compatible_major=None,
            is_autonomous=False,
        )

    def get_optimal_type(self, driver: SyncDriverAdapterBase, type_category: str) -> str:
        """Return optimal Oracle column type (23ai `JSON` vs 19c `BLOB CHECK (.. IS JSON)`)."""
        version_info = self.get_version(driver)
        if type_category in {"json", "jsonb"}:
            return resolve_oracle_json_type(version_info, compatible_major=None, is_autonomous=False)
        return ORACLE_CONFIG.get_optimal_type(type_category)

    def get_schemas(self, driver: SyncDriverAdapterBase, schema: str | None = None) -> MetadataResult:
        """List accessible Oracle database schemas."""
        schema_filter = schema.upper() if schema is not None else None
        rows = driver.select(self.get_query("schemas", "by_schema"), schema_name=schema_filter)
        return MetadataResult("schemas", capability=_schema_capability(), items=tuple(_rows_as_dicts(rows)))

    def get_tables(self, driver: SyncDriverAdapterBase, schema: str | None = None) -> list[TableMetadata]:
        """Return Oracle tables in `schema`."""
        schema_name = self.resolve_schema(schema)
        rows = driver.select(
            self.get_query("tables", "by_schema"),
            schema_name=schema_name.upper() if schema_name else None,
        )
        return cast("list[TableMetadata]", _rows_as_dicts(rows))

    def get_columns(
        self,
        driver: SyncDriverAdapterBase,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[ColumnMetadata]:
        """Return Oracle column metadata for a table or schema."""
        schema_name = self.resolve_schema(schema)
        upper_schema = schema_name.upper() if schema_name else None
        if table is None:
            rows = driver.select(self.get_query("columns", "by_schema"), schema_name=upper_schema)
        else:
            rows = driver.select(
                self.get_query("columns", "by_table"),
                schema_name=upper_schema,
                table_name=table.upper(),
            )
        return cast("list[ColumnMetadata]", _rows_as_dicts(rows))

    def get_indexes(
        self,
        driver: SyncDriverAdapterBase,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[IndexMetadata]:
        """Return Oracle index definitions for a table or schema."""
        schema_name = self.resolve_schema(schema)
        upper_schema = schema_name.upper() if schema_name else None
        if table is None:
            rows = driver.select(self.get_query("indexes", "by_schema"), schema_name=upper_schema)
        else:
            rows = driver.select(
                self.get_query("indexes", "by_table"),
                schema_name=upper_schema,
                table_name=table.upper(),
            )
        results = _rows_as_dicts(rows)
        for item in results:
            if "columns" in item:
                item["columns"] = self._decode_json_field(item["columns"])
        return cast("list[IndexMetadata]", results)

    def get_foreign_keys(
        self,
        driver: SyncDriverAdapterBase,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[ForeignKeyMetadata]:
        """Return Oracle foreign key constraints for a table or schema."""
        schema_name = self.resolve_schema(schema)
        upper_schema = schema_name.upper() if schema_name else None
        if table is None:
            rows = driver.select(self.get_query("foreign_keys", "by_schema"), schema_name=upper_schema)
        else:
            rows = driver.select(
                self.get_query("foreign_keys", "by_table"),
                schema_name=upper_schema,
                table_name=table.upper(),
            )
        return cast("list[ForeignKeyMetadata]", _rows_as_dicts(rows))


class OracleAsyncDataDictionary(_OracleDataDictionaryMixin, AsyncDataDictionaryBase):
    """Asynchronous Oracle data dictionary backed by SQLSpec `oracle/sql/*.sql` catalog queries."""

    dialect: ClassVar[str] = "oracle"

    async def get_version(self, driver: AsyncDriverAdapterBase) -> VersionInfo | None:
        """Asynchronously query Oracle server version and cache per driver instance."""
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)

        row = await driver.select_one_or_none(self.get_query("version", "current"))
        if row is None:
            self._log_version_unavailable(type(self).dialect, "missing")
            self.cache_version(driver_id, None)
            return None

        version_info = self._parse_version_from_row(self, row)
        if version_info is None:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            self.cache_version(driver_id, None)
            return None

        self._log_version_detected(type(self).dialect, version_info)
        self.cache_version(driver_id, version_info)
        return version_info

    async def get_feature_flag(self, driver: AsyncDriverAdapterBase, feature: str) -> bool:
        """Asynchronously evaluate Oracle 19c/23ai feature availability."""
        version_info = await self.get_version(driver)
        return resolve_oracle_feature_flag(
            ORACLE_CONFIG,
            version_info,
            feature,
            compatible_major=None,
            is_autonomous=False,
        )

    async def get_optimal_type(self, driver: AsyncDriverAdapterBase, type_category: str) -> str:
        """Asynchronously return optimal Oracle column type (23ai `JSON` vs 19c `BLOB`)."""
        version_info = await self.get_version(driver)
        if type_category in {"json", "jsonb"}:
            return resolve_oracle_json_type(version_info, compatible_major=None, is_autonomous=False)
        return ORACLE_CONFIG.get_optimal_type(type_category)

    async def get_schemas(self, driver: AsyncDriverAdapterBase, schema: str | None = None) -> MetadataResult:
        """Asynchronously list accessible Oracle schemas."""
        schema_filter = schema.upper() if schema is not None else None
        rows = await driver.select(self.get_query("schemas", "by_schema"), schema_name=schema_filter)
        return MetadataResult("schemas", capability=_schema_capability(), items=tuple(_rows_as_dicts(rows)))

    async def get_tables(self, driver: AsyncDriverAdapterBase, schema: str | None = None) -> list[TableMetadata]:
        """Asynchronously return Oracle tables in `schema`."""
        schema_name = self.resolve_schema(schema)
        rows = await driver.select(
            self.get_query("tables", "by_schema"),
            schema_name=schema_name.upper() if schema_name else None,
        )
        return cast("list[TableMetadata]", _rows_as_dicts(rows))

    async def get_columns(
        self,
        driver: AsyncDriverAdapterBase,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[ColumnMetadata]:
        """Asynchronously return Oracle column metadata."""
        schema_name = self.resolve_schema(schema)
        upper_schema = schema_name.upper() if schema_name else None
        if table is None:
            rows = await driver.select(self.get_query("columns", "by_schema"), schema_name=upper_schema)
        else:
            rows = await driver.select(
                self.get_query("columns", "by_table"),
                schema_name=upper_schema,
                table_name=table.upper(),
            )
        return cast("list[ColumnMetadata]", _rows_as_dicts(rows))

    async def get_indexes(
        self,
        driver: AsyncDriverAdapterBase,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[IndexMetadata]:
        """Asynchronously return Oracle index definitions."""
        schema_name = self.resolve_schema(schema)
        upper_schema = schema_name.upper() if schema_name else None
        if table is None:
            rows = await driver.select(self.get_query("indexes", "by_schema"), schema_name=upper_schema)
        else:
            rows = await driver.select(
                self.get_query("indexes", "by_table"),
                schema_name=upper_schema,
                table_name=table.upper(),
            )
        results = _rows_as_dicts(rows)
        for item in results:
            if "columns" in item:
                item["columns"] = self._decode_json_field(item["columns"])
        return cast("list[IndexMetadata]", results)

    async def get_foreign_keys(
        self,
        driver: AsyncDriverAdapterBase,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[ForeignKeyMetadata]:
        """Asynchronously return Oracle foreign key constraints."""
        schema_name = self.resolve_schema(schema)
        upper_schema = schema_name.upper() if schema_name else None
        if table is None:
            rows = await driver.select(self.get_query("foreign_keys", "by_schema"), schema_name=upper_schema)
        else:
            rows = await driver.select(
                self.get_query("foreign_keys", "by_table"),
                schema_name=upper_schema,
                table_name=table.upper(),
            )
        return cast("list[ForeignKeyMetadata]", _rows_as_dicts(rows))
