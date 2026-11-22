"""BigQuery synchronous and asynchronous SQLSpec data dictionaries over catalog SQL."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, ClassVar, Final, cast

from sqlspec.data_dictionary import (
    ColumnMetadata,
    DDLResult,
    ForeignKeyMetadata,
    IndexMetadata,
    PartitionMetadata,
    TableMetadata,
)
from sqlspec.data_dictionary.dialects.bigquery.dictionary import (
    BigQueryAsyncDataDictionary as _UpstreamBigQueryAsyncDataDictionary,
)
from sqlspec.data_dictionary.dialects.bigquery.dictionary import (
    BigQueryMetadataScope as _UpstreamBigQueryMetadataScope,
)
from sqlspec.data_dictionary.dialects.bigquery.dictionary import (
    BigQuerySyncDataDictionary as _UpstreamBigQuerySyncDataDictionary,
)

from vertebra.extensions.sqlspec.adapters.bigquery.core import (
    resolve_dataset_id,
    resolve_project_id,
)
from vertebra.typing import ImproperConfigurationError

if TYPE_CHECKING:
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

__all__ = (
    "BigQueryAsyncDataDictionary",
    "BigQueryMetadataScope",
    "BigQuerySyncDataDictionary",
)

_BQ_PROJECT_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_BQ_DATASET_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_-]*$")
_BQ_REGION_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")


class BigQueryMetadataScope(_UpstreamBigQueryMetadataScope):
    """Validated BigQuery dataset and region scope used to format ``INFORMATION_SCHEMA`` queries."""

    __slots__ = ()

    def __init__(
        self,
        *,
        project: str | None = None,
        dataset: str | None = None,
        region: str | None = None,
    ) -> None:
        super().__init__(
            project=self._validate_scope_part(project, name="project", pattern=_BQ_PROJECT_RE),
            dataset=self._validate_scope_part(dataset, name="dataset", pattern=_BQ_DATASET_RE),
            region=self._validate_scope_part(region, name="region", pattern=_BQ_REGION_RE),
        )

    @staticmethod
    def _validate_scope_part(value: str | None, *, name: str, pattern: re.Pattern[str]) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            msg = f"BigQuery {name} scope must be a string, got {type(value).__name__}"
            raise ImproperConfigurationError(msg)
        cleaned = value.strip()
        if not cleaned or ".." in cleaned or not pattern.match(cleaned):
            msg = f"Invalid BigQuery {name} scope: {value!r}"
            raise ImproperConfigurationError(msg)
        return cleaned


class BigQuerySyncDataDictionary(_UpstreamBigQuerySyncDataDictionary):
    """BigQuery synchronous SQLSpec data dictionary backed by SQLSpec's shared dialect base."""

    __slots__ = ()

    dialect: ClassVar[str] = "bigquery"

    def get_tables(self, driver: SyncDriverAdapterBase, schema: str | None = None) -> list[TableMetadata]:
        """List base tables sorted topologically by FK dependency depth."""
        return super().get_tables(driver, schema=_resolve_effective_schema(driver, schema))

    def get_columns(
        self, driver: SyncDriverAdapterBase, table: str | None = None, schema: str | None = None
    ) -> list[ColumnMetadata]:
        """Retrieve column metadata for a specific table or dataset schema."""
        return super().get_columns(driver, table=table, schema=_resolve_effective_schema(driver, schema))

    def get_indexes(
        self, driver: SyncDriverAdapterBase, table: str | None = None, schema: str | None = None
    ) -> list[IndexMetadata]:
        """List BigQuery search and vector indexes for a table or dataset."""
        return super().get_indexes(driver, table=table, schema=_resolve_effective_schema(driver, schema))

    def get_foreign_keys(
        self, driver: SyncDriverAdapterBase, table: str | None = None, schema: str | None = None
    ) -> list[ForeignKeyMetadata]:
        """Retrieve un-enforced BigQuery ``KEY_COLUMN_USAGE`` foreign key constraints."""
        return super().get_foreign_keys(driver, table=table, schema=_resolve_effective_schema(driver, schema))

    def get_partitions(
        self, driver: SyncDriverAdapterBase, table: str | None = None, schema: str | None = None
    ) -> list[PartitionMetadata]:
        """Retrieve BigQuery table partition storage statistics."""
        scope = BigQueryMetadataScope.from_schema(_resolve_effective_schema(driver, schema))
        query_text = self.get_query_text("tables", "partitions_by_dataset").format(
            partitions_table=scope.dataset_information_schema_table("PARTITIONS")
        )
        rows = driver.select(query_text, schema_name=scope.schema_name, table_name=table)
        return cast("list[PartitionMetadata]", rows)

    def _get_native_object_ddl(
        self,
        driver: SyncDriverAdapterBase,
        object_name: str,
        *,
        schema: str | None,
        object_type: str,
        include_dependencies: bool,
        redact: bool,
    ) -> DDLResult | None:
        return super()._get_native_object_ddl(
            driver,
            object_name,
            schema=_resolve_effective_schema(driver, schema),
            object_type=object_type,
            include_dependencies=include_dependencies,
            redact=redact,
        )

    def _get_native_schema_ddl(
        self,
        driver: SyncDriverAdapterBase,
        schema: str | None,
        *,
        include_dependencies: bool,
        redact: bool,
    ) -> list[DDLResult] | None:
        return super()._get_native_schema_ddl(
            driver,
            _resolve_effective_schema(driver, schema),
            include_dependencies=include_dependencies,
            redact=redact,
        )


class BigQueryAsyncDataDictionary(_UpstreamBigQueryAsyncDataDictionary):
    """BigQuery asynchronous SQLSpec data dictionary backed by SQLSpec's shared dialect base."""

    __slots__ = ()

    dialect: ClassVar[str] = "bigquery"

    async def get_tables(self, driver: AsyncDriverAdapterBase, schema: str | None = None) -> list[TableMetadata]:
        """List base tables sorted topologically by FK dependency depth."""
        return await super().get_tables(driver, schema=_resolve_effective_schema(driver, schema))

    async def get_columns(
        self, driver: AsyncDriverAdapterBase, table: str | None = None, schema: str | None = None
    ) -> list[ColumnMetadata]:
        """Retrieve column metadata for a specific table or dataset schema."""
        return await super().get_columns(driver, table=table, schema=_resolve_effective_schema(driver, schema))

    async def get_indexes(
        self, driver: AsyncDriverAdapterBase, table: str | None = None, schema: str | None = None
    ) -> list[IndexMetadata]:
        """List BigQuery search and vector indexes for a table or dataset."""
        return await super().get_indexes(driver, table=table, schema=_resolve_effective_schema(driver, schema))

    async def get_foreign_keys(
        self, driver: AsyncDriverAdapterBase, table: str | None = None, schema: str | None = None
    ) -> list[ForeignKeyMetadata]:
        """Retrieve un-enforced BigQuery ``KEY_COLUMN_USAGE`` foreign key constraints."""
        return await super().get_foreign_keys(driver, table=table, schema=_resolve_effective_schema(driver, schema))

    async def get_partitions(
        self, driver: AsyncDriverAdapterBase, table: str | None = None, schema: str | None = None
    ) -> list[PartitionMetadata]:
        """Retrieve BigQuery table partition storage statistics."""
        scope = BigQueryMetadataScope.from_schema(_resolve_effective_schema(driver, schema))
        query_text = self.get_query_text("tables", "partitions_by_dataset").format(
            partitions_table=scope.dataset_information_schema_table("PARTITIONS")
        )
        rows = await driver.select(query_text, schema_name=scope.schema_name, table_name=table)
        return cast("list[PartitionMetadata]", rows)

    async def _get_native_object_ddl(
        self,
        driver: AsyncDriverAdapterBase,
        object_name: str,
        *,
        schema: str | None,
        object_type: str,
        include_dependencies: bool,
        redact: bool,
    ) -> DDLResult | None:
        return await super()._get_native_object_ddl(
            driver,
            object_name,
            schema=_resolve_effective_schema(driver, schema),
            object_type=object_type,
            include_dependencies=include_dependencies,
            redact=redact,
        )

    async def _get_native_schema_ddl(
        self,
        driver: AsyncDriverAdapterBase,
        schema: str | None,
        *,
        include_dependencies: bool,
        redact: bool,
    ) -> list[DDLResult] | None:
        return await super()._get_native_schema_ddl(
            driver,
            _resolve_effective_schema(driver, schema),
            include_dependencies=include_dependencies,
            redact=redact,
        )


def _resolve_effective_schema(driver: Any, schema: str | None) -> str | None:
    """Resolve an explicit dataset qualifier or fall back to the driver's project and dataset."""
    raw = schema.strip() if isinstance(schema, str) and schema.strip() else resolve_dataset_id(driver)
    if not raw:
        return None
    parts = [part.strip("`").strip() for part in raw.split(".") if part.strip("`").strip()]
    if len(parts) == 2:
        return f"{parts[0]}.{parts[1]}"
    if len(parts) == 1:
        project_id = resolve_project_id(driver)
        return f"{project_id}.{parts[0]}" if project_id else parts[0]
    return raw
