"""Oracle synchronous and asynchronous SQLSpec data dictionaries."""

from __future__ import annotations

import contextlib
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, cast

from sqlspec.data_dictionary.dialects.oracle.dictionary import (
    JSONStorageType,
    OracleVersionInfo,
    storage_type_from_version,
)
from sqlspec.data_dictionary.dialects.oracle.dictionary import (
    OracleAsyncDataDictionary as _UpstreamOracleAsyncDataDictionary,
)
from sqlspec.data_dictionary.dialects.oracle.dictionary import (
    OracleSyncDataDictionary as _UpstreamOracleSyncDataDictionary,
)
from sqlspec.utils.serializers import from_json

if TYPE_CHECKING:
    from sqlspec.data_dictionary import ColumnMetadata, ForeignKeyMetadata, IndexMetadata, TableMetadata
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

__all__ = (
    "JSONStorageType",
    "OracleAsyncDataDictionary",
    "OracleSyncDataDictionary",
    "OracleVersionInfo",
    "storage_type_from_version",
)


class OracleSyncDataDictionary(_UpstreamOracleSyncDataDictionary):
    """Synchronous Oracle data dictionary backed by SQLSpec's shared dialect base."""

    __slots__ = ()

    dialect: ClassVar[str] = "oracle"

    def get_optimal_type(self, driver: SyncDriverAdapterBase, type_category: str) -> str:
        """Return optimal Oracle column type for `type_category`."""
        normalized_category = "json" if type_category == "jsonb" else type_category
        return super().get_optimal_type(driver, normalized_category)

    def get_tables(self, driver: SyncDriverAdapterBase, schema: str | None = None) -> list[TableMetadata]:
        """Return Oracle tables in `schema` with normalized lowercase column keys."""
        return cast("list[TableMetadata]", _lowercase_rows(super().get_tables(driver, schema=schema)))

    def get_columns(
        self,
        driver: SyncDriverAdapterBase,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[ColumnMetadata]:
        """Return Oracle column metadata with normalized lowercase column keys."""
        return cast("list[ColumnMetadata]", _lowercase_rows(super().get_columns(driver, table=table, schema=schema)))

    def get_indexes(
        self,
        driver: SyncDriverAdapterBase,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[IndexMetadata]:
        """Return Oracle index definitions with normalized lowercase keys and decoded column lists."""
        return cast("list[IndexMetadata]", _decode_index_rows(super().get_indexes(driver, table=table, schema=schema)))

    def get_foreign_keys(
        self,
        driver: SyncDriverAdapterBase,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[ForeignKeyMetadata]:
        """Return Oracle foreign key constraints with normalized lowercase column keys."""
        return cast(
            "list[ForeignKeyMetadata]",
            _lowercase_rows(super().get_foreign_keys(driver, table=table, schema=schema)),
        )

    def get_json_duality_views(self, driver: SyncDriverAdapterBase, schema: str | None = None) -> list[dict[str, Any]]:
        """Return Oracle 26ai JSON Relational Duality View catalog rows."""
        return _lowercase_rows(super().get_json_duality_views(driver, schema=schema))


class OracleAsyncDataDictionary(_UpstreamOracleAsyncDataDictionary):
    """Asynchronous Oracle data dictionary backed by SQLSpec's shared dialect base."""

    __slots__ = ()

    dialect: ClassVar[str] = "oracle"

    async def get_optimal_type(self, driver: AsyncDriverAdapterBase, type_category: str) -> str:
        """Asynchronously return optimal Oracle column type for `type_category`."""
        normalized_category = "json" if type_category == "jsonb" else type_category
        return await super().get_optimal_type(driver, normalized_category)

    async def get_tables(self, driver: AsyncDriverAdapterBase, schema: str | None = None) -> list[TableMetadata]:
        """Asynchronously return Oracle tables in `schema` with normalized lowercase column keys."""
        return cast("list[TableMetadata]", _lowercase_rows(await super().get_tables(driver, schema=schema)))

    async def get_columns(
        self,
        driver: AsyncDriverAdapterBase,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[ColumnMetadata]:
        """Asynchronously return Oracle column metadata with normalized lowercase column keys."""
        return cast(
            "list[ColumnMetadata]",
            _lowercase_rows(await super().get_columns(driver, table=table, schema=schema)),
        )

    async def get_indexes(
        self,
        driver: AsyncDriverAdapterBase,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[IndexMetadata]:
        """Asynchronously return Oracle index definitions with normalized lowercase keys and decoded column lists."""
        return cast(
            "list[IndexMetadata]",
            _decode_index_rows(await super().get_indexes(driver, table=table, schema=schema)),
        )

    async def get_foreign_keys(
        self,
        driver: AsyncDriverAdapterBase,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[ForeignKeyMetadata]:
        """Asynchronously return Oracle foreign key constraints with normalized lowercase column keys."""
        return cast(
            "list[ForeignKeyMetadata]",
            _lowercase_rows(await super().get_foreign_keys(driver, table=table, schema=schema)),
        )

    async def get_json_duality_views(
        self, driver: AsyncDriverAdapterBase, schema: str | None = None
    ) -> list[dict[str, Any]]:
        """Asynchronously return Oracle 26ai JSON Relational Duality View catalog rows."""
        return _lowercase_rows(await super().get_json_duality_views(driver, schema=schema))


def _lowercase_rows(rows: Sequence[Any] | None) -> list[dict[str, Any]]:
    """Normalize Oracle catalog row keys to lowercase dictionaries."""
    if not rows:
        return []
    result: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, Mapping):
            result.append({str(k).lower(): v for k, v in row.items()})
        else:
            result.append({"value": row})
    return result


def _decode_index_rows(rows: Sequence[Any] | None) -> list[dict[str, Any]]:
    """Normalize Oracle index row keys and decode JSON-encoded `columns` strings."""
    normalized = _lowercase_rows(rows)
    for item in normalized:
        raw_cols = item.get("columns")
        if isinstance(raw_cols, str):
            with contextlib.suppress(TypeError, ValueError):
                item["columns"] = from_json(raw_cols)
    return normalized
