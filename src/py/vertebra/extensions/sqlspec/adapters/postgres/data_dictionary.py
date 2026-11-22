"""PostgreSQL synchronous and asynchronous SQLSpec data dictionaries."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, ClassVar, cast

from sqlspec.data_dictionary.dialects.postgres import resolve_postgres_json_type
from sqlspec.data_dictionary.dialects.postgres.dictionary import (
    PostgresAsyncDataDictionary as _UpstreamPostgresAsyncDataDictionary,
)
from sqlspec.data_dictionary.dialects.postgres.dictionary import (
    PostgresSyncDataDictionary as _UpstreamPostgresSyncDataDictionary,
)
from sqlspec.utils.serializers import from_json

if TYPE_CHECKING:
    from sqlspec.data_dictionary import IndexMetadata
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

__all__ = ("PostgresAsyncDataDictionary", "PostgresSyncDataDictionary")


class PostgresSyncDataDictionary(_UpstreamPostgresSyncDataDictionary):
    """Synchronous PostgreSQL data dictionary backed by SQLSpec's shared dialect base."""

    __slots__ = ()

    dialect: ClassVar[str] = "postgres"

    def get_optimal_type(self, driver: SyncDriverAdapterBase, type_category: str) -> str:
        """Return the optimal PostgreSQL SQL type for `type_category`."""
        if type_category == "jsonb":
            return resolve_postgres_json_type(self.get_version(driver))
        return super().get_optimal_type(driver, type_category)

    def get_indexes(
        self,
        driver: SyncDriverAdapterBase,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[IndexMetadata]:
        """Return PostgreSQL index definitions for a table or schema."""
        return _decode_index_columns(super().get_indexes(driver, table=table, schema=schema))


class PostgresAsyncDataDictionary(_UpstreamPostgresAsyncDataDictionary):
    """Asynchronous PostgreSQL data dictionary backed by SQLSpec's shared dialect base."""

    __slots__ = ()

    dialect: ClassVar[str] = "postgres"

    async def get_optimal_type(self, driver: AsyncDriverAdapterBase, type_category: str) -> str:
        """Return the optimal PostgreSQL SQL type for `type_category`."""
        if type_category == "jsonb":
            return resolve_postgres_json_type(await self.get_version(driver))
        return await super().get_optimal_type(driver, type_category)

    async def get_indexes(
        self,
        driver: AsyncDriverAdapterBase,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[IndexMetadata]:
        """Return PostgreSQL index definitions asynchronously."""
        return _decode_index_columns(await super().get_indexes(driver, table=table, schema=schema))


def _decode_index_columns(rows: list[IndexMetadata]) -> list[IndexMetadata]:
    """Decode JSON-encoded `columns` entries returned as raw strings by native drivers."""
    normalized: list[IndexMetadata] = []
    for row in rows:
        raw_cols = row.get("columns") if isinstance(row, Mapping) else None
        if isinstance(raw_cols, str):
            try:
                decoded = from_json(raw_cols)
            except (TypeError, ValueError):
                decoded = raw_cols
            updated: dict[str, Any] = dict(row)
            updated["columns"] = decoded
            normalized.append(cast("IndexMetadata", updated))
        else:
            normalized.append(row)
    return normalized
