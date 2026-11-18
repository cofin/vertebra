"""BigQuery synchronous and asynchronous SQLSpec data dictionaries over catalog SQL."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Final, cast

from sqlspec.data_dictionary import (
    ColumnMetadata,
    DDLResult,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataCapability,
    MetadataCapabilityProfile,
    MetadataFidelity,
    MetadataRisk,
    MetadataSource,
    MetadataSupport,
    ObjectIdentity,
    PartitionMetadata,
    TableMetadata,
    VersionInfo,
)
from sqlspec.data_dictionary.dialects.bigquery import (
    format_bigquery_information_schema_tables,
    format_bigquery_schema_prefix,
)
from sqlspec.driver import AsyncDataDictionaryBase, SyncDataDictionaryBase

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

_BILLED_WARNINGS: Final[tuple[str, ...]] = (
    "BigQuery INFORMATION_SCHEMA queries require a dataset or region qualifier.",
    "BigQuery INFORMATION_SCHEMA query results are not cached and may be billed.",
)

_SYSTEM_WARNINGS: Final[tuple[str, ...]] = (
    "BigQuery system views are region scoped and require matching job location.",
    "BigQuery system views are not cached, may be billed, and require elevated IAM permissions.",
)

_SUPPORTED_DOMAINS: Final[tuple[str, ...]] = (
    "schemas",
    "objects",
    "tables",
    "columns",
    "constraints",
    "indexes",
    "views",
    "routines",
    "privileges",
    "dependencies",
    "ddl",
    "system",
)


_BQ_PROJECT_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_BQ_DATASET_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_-]*$")
_BQ_REGION_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")


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


class BigQueryMetadataScope:
    """Normalized BigQuery dataset and region scope used to format ``INFORMATION_SCHEMA`` queries."""

    __slots__ = ("dataset", "project", "region")

    def __init__(
        self,
        *,
        project: str | None = None,
        dataset: str | None = None,
        region: str | None = None,
    ) -> None:
        self.project = _validate_scope_part(project, name="project", pattern=_BQ_PROJECT_RE)
        self.dataset = _validate_scope_part(dataset, name="dataset", pattern=_BQ_DATASET_RE)
        self.region = _validate_scope_part(region, name="region", pattern=_BQ_REGION_RE)


def _normalize_schema_and_prefix(schema: str | None, driver: Any) -> tuple[str | None, str]:
    """Compute a dataset name and ``INFORMATION_SCHEMA`` qualifier prefix from schema or driver config."""
    dataset_name = schema.strip("`") if isinstance(schema, str) and schema.strip() else resolve_dataset_id(driver)
    project_id = resolve_project_id(driver)
    if dataset_name and "." in dataset_name:
        parts = [part.strip("`") for part in dataset_name.split(".") if part.strip("`")]
        if len(parts) == 2:
            project_id, dataset_name = parts
        elif parts:
            dataset_name = parts[-1]
    if project_id and dataset_name:
        return dataset_name, f"`{project_id}`.`{dataset_name}`."
    if dataset_name:
        try:
            return dataset_name, str(format_bigquery_schema_prefix(dataset_name))
        except ValueError as exc:
            raise ImproperConfigurationError(str(exc)) from exc
    return None, ""


def _format_catalog_query(query_text: str, schema: str | None, driver: Any) -> tuple[str | None, str]:
    """Render all BigQuery ``INFORMATION_SCHEMA`` table placeholders in a loaded catalog SQL query."""
    dataset_name, schema_prefix = _normalize_schema_and_prefix(schema, driver)
    tables_table, kcu_table, rc_table = format_bigquery_information_schema_tables(
        f"{schema_prefix.rstrip('.')}" if schema_prefix else None
    )
    if schema_prefix:
        tables_table = f"{schema_prefix}INFORMATION_SCHEMA.TABLES"
        kcu_table = f"{schema_prefix}INFORMATION_SCHEMA.KEY_COLUMN_USAGE"
        rc_table = f"{schema_prefix}INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS"
    rendered = query_text.format(
        schema_prefix=schema_prefix,
        tables_table=tables_table,
        columns_table=f"{schema_prefix}INFORMATION_SCHEMA.COLUMNS",
        table_options_table=f"{schema_prefix}INFORMATION_SCHEMA.TABLE_OPTIONS",
        partitions_table=f"{schema_prefix}INFORMATION_SCHEMA.PARTITIONS",
        table_storage_table=f"{schema_prefix}INFORMATION_SCHEMA.TABLE_STORAGE",
        kcu_table=kcu_table,
        rc_table=rc_table,
        table_constraints_table=f"{schema_prefix}INFORMATION_SCHEMA.TABLE_CONSTRAINTS",
        key_column_usage_table=kcu_table,
        referential_constraints_table=rc_table,
        constraint_column_usage_table=f"{schema_prefix}INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE",
        search_indexes_table=f"{schema_prefix}INFORMATION_SCHEMA.SEARCH_INDEXES",
        search_index_columns_table=f"{schema_prefix}INFORMATION_SCHEMA.SEARCH_INDEX_COLUMNS",
        vector_indexes_table=f"{schema_prefix}INFORMATION_SCHEMA.VECTOR_INDEXES",
        vector_index_columns_table=f"{schema_prefix}INFORMATION_SCHEMA.VECTOR_INDEX_COLUMNS",
        schemata_table=f"{schema_prefix}INFORMATION_SCHEMA.SCHEMATA",
        schemata_options_table=f"{schema_prefix}INFORMATION_SCHEMA.SCHEMATA_OPTIONS",
        routines_table=f"{schema_prefix}INFORMATION_SCHEMA.ROUTINES",
        routine_options_table=f"{schema_prefix}INFORMATION_SCHEMA.ROUTINE_OPTIONS",
        routine_parameters_table=f"{schema_prefix}INFORMATION_SCHEMA.PARAMETERS",
        views_table=f"{schema_prefix}INFORMATION_SCHEMA.VIEWS",
        materialized_views_table=f"{schema_prefix}INFORMATION_SCHEMA.MATERIALIZED_VIEWS",
        object_privileges_table=f"{schema_prefix}INFORMATION_SCHEMA.OBJECT_PRIVILEGES",
        jobs_table=f"{schema_prefix}INFORMATION_SCHEMA.JOBS_BY_PROJECT",
        reservations_table=f"{schema_prefix}INFORMATION_SCHEMA.RESERVATIONS",
        sessions_table=f"{schema_prefix}INFORMATION_SCHEMA.SESSIONS_BY_PROJECT",
    )
    return dataset_name, rendered


def _bigquery_capabilities(domains: Sequence[str] | None, adapter_cls_name: str) -> MetadataCapabilityProfile:
    """Build the SQLSpec metadata capability profile for BigQuery."""
    requested = tuple(domains) if domains is not None else _SUPPORTED_DOMAINS
    capabilities: list[MetadataCapability] = []
    for domain in requested:
        if domain == "system":
            capabilities.append(
                MetadataCapability(
                    domain=domain,
                    support=MetadataSupport.SUPPORTED,
                    fidelity=MetadataFidelity.PARTIAL,
                    source=MetadataSource.SYSTEM_VIEW,
                    risks=(MetadataRisk.BILLED, MetadataRisk.EXPENSIVE, MetadataRisk.PRIVILEGED),
                    warnings=_SYSTEM_WARNINGS,
                )
            )
        elif domain in _SUPPORTED_DOMAINS:
            capabilities.append(
                MetadataCapability(
                    domain=domain,
                    support=MetadataSupport.SUPPORTED,
                    fidelity=MetadataFidelity.NATIVE,
                    source=MetadataSource.INFORMATION_SCHEMA,
                    risks=(MetadataRisk.BILLED,),
                    warnings=_BILLED_WARNINGS,
                )
            )
        else:
            capabilities.append(MetadataCapability.unsupported(domain))
    return MetadataCapabilityProfile(
        dialect="bigquery",
        adapter=adapter_cls_name,
        capabilities=tuple(capabilities),
    )


def _table_metadata_from_rows(rows: list[dict[str, Any]], dataset_name: str | None) -> list[TableMetadata]:
    """Normalize BigQuery table catalog rows into ``TableMetadata`` records."""
    result: list[TableMetadata] = []
    for row in rows:
        schema_val = str(row.get("table_schema") or dataset_name or "")
        result.append(
            cast(
                TableMetadata,
                {
                    "schema_name": schema_val,
                    "table_schema": schema_val,
                    "table_catalog": str(row.get("table_catalog") or ""),
                    "table_name": str(row.get("table_name") or ""),
                    "table_type": str(row.get("table_type") or "BASE TABLE"),
                    "dependency_level": int(row.get("dependency_level", 0) or 0),
                    "level": int(row.get("level", row.get("dependency_level", 0)) or 0),
                },
            )
        )
    return result


def _column_metadata_from_rows(
    rows: list[dict[str, Any]],
    table: str | None,
    dataset_name: str | None,
) -> list[ColumnMetadata]:
    """Normalize BigQuery column catalog rows into ``ColumnMetadata`` records."""
    result: list[ColumnMetadata] = []
    for row in rows:
        row_table = str(row.get("table_name") or table or "")
        if table is not None and row_table and row_table != table:
            continue
        is_nullable_raw = row.get("is_nullable", "YES")
        is_nullable = (
            str(is_nullable_raw).upper() == "YES" if isinstance(is_nullable_raw, str) else bool(is_nullable_raw)
        )
        is_generated_raw = row.get("is_generated", "NEVER")
        is_generated = (
            str(is_generated_raw).upper() not in {"", "NEVER", "NO", "FALSE"}
            if isinstance(is_generated_raw, str)
            else bool(is_generated_raw)
        )
        is_hidden_raw = row.get("is_hidden", "NO")
        is_hidden = (
            str(is_hidden_raw).upper() in {"YES", "TRUE", "1"}
            if isinstance(is_hidden_raw, str)
            else bool(is_hidden_raw)
        )
        data_type = str(row.get("data_type") or "STRING")
        result.append(
            cast(
                ColumnMetadata,
                {
                    "schema_name": str(row.get("table_schema") or dataset_name or ""),
                    "table_name": row_table,
                    "column_name": str(row.get("column_name") or ""),
                    "data_type": data_type,
                    "column_type": data_type,
                    "is_nullable": is_nullable,
                    "column_default": row.get("column_default"),
                    "ordinal_position": int(row.get("ordinal_position", 0) or 0),
                    "max_length": None,
                    "numeric_precision": None,
                    "numeric_scale": None,
                    "is_primary": False,
                    "is_unique": False,
                    "extra": "",
                    "column_key": "",
                    "identity_generation": None,
                    "is_identity": False,
                    "sequence_name": None,
                    "is_generated": is_generated,
                    "generation_expression": row.get("generation_expression"),
                    "is_hidden": is_hidden,
                    "column_comment": None,
                },
            )
        )
    return result


def _index_metadata_from_rows(rows: list[dict[str, Any]], dataset_name: str | None) -> list[IndexMetadata]:
    """Normalize search and vector index rows into ``IndexMetadata`` records."""
    result: list[IndexMetadata] = []
    for row in rows:
        cols_raw = row.get("columns")
        columns = (
            [str(c) for c in cols_raw if c] if isinstance(cols_raw, Sequence) and not isinstance(cols_raw, str) else []
        )
        index_name = row.get("index_name")
        if not index_name:
            continue
        result.append(
            cast(
                IndexMetadata,
                {
                    "schema_name": str(row.get("schema_name") or dataset_name or ""),
                    "table_name": str(row.get("table_name") or ""),
                    "index_name": str(index_name),
                    "columns": columns,
                    "is_unique": bool(row.get("is_unique", False)),
                    "is_primary": bool(row.get("is_primary", False)),
                    "expression": None,
                    "expressions": None,
                },
            )
        )
    return result


def _foreign_key_metadata_from_rows(rows: list[dict[str, Any]], dataset_name: str | None) -> list[ForeignKeyMetadata]:
    """Normalize BigQuery foreign key constraint rows into ``ForeignKeyMetadata`` records."""
    result: list[ForeignKeyMetadata] = []
    for row in rows:
        result.append(
            cast(
                ForeignKeyMetadata,
                {
                    "table_name": str(row.get("table_name") or ""),
                    "column_name": str(row.get("column_name") or ""),
                    "referenced_table_name": str(row.get("referenced_table_name") or ""),
                    "referenced_column_name": str(row.get("referenced_column_name") or ""),
                    "constraint_name": str(row.get("constraint_name") or ""),
                    "table_schema": str(row.get("table_schema") or dataset_name or ""),
                    "referenced_table_schema": str(row.get("referenced_table_schema") or dataset_name or ""),
                },
            )
        )
    return result


def _partition_metadata_from_rows(rows: list[dict[str, Any]], dataset_name: str | None) -> list[PartitionMetadata]:
    """Normalize ``INFORMATION_SCHEMA.PARTITIONS`` rows into ``PartitionMetadata`` records."""
    result: list[PartitionMetadata] = []
    for row in rows:
        result.append(
            cast(
                PartitionMetadata,
                {
                    "table_catalog": str(row.get("table_catalog") or ""),
                    "table_schema": str(row.get("table_schema") or dataset_name or ""),
                    "table_name": str(row.get("table_name") or ""),
                    "partition_id": str(row.get("partition_id") or ""),
                    "total_rows": int(row.get("total_rows", 0) or 0),
                    "total_logical_bytes": int(row.get("total_logical_bytes", 0) or 0),
                    "last_modified_time": row.get("last_modified_time"),
                    "storage_tier": row.get("storage_tier"),
                },
            )
        )
    return result


def _ddl_result_from_row(
    row: Mapping[str, Any],
    *,
    default_name: str,
    default_object_type: str,
    default_schema: str | None,
) -> DDLResult:
    object_name = str(row.get("object_name") or default_name)
    object_type = str(row.get("object_type") or default_object_type)
    schema_raw = row.get("schema_name") or row.get("table_schema") or default_schema
    schema_name = str(schema_raw) if schema_raw is not None and str(schema_raw) else None
    ddl_raw = row.get("ddl")
    ddl_text = str(ddl_raw) if ddl_raw is not None else None
    identity = ObjectIdentity(
        name=object_name,
        object_type=object_type,
        schema=schema_name,
        dialect="bigquery",
        source=MetadataSource.INFORMATION_SCHEMA,
    )
    if ddl_text is None:
        return DDLResult.unsupported(identity, source=MetadataSource.INFORMATION_SCHEMA)
    return DDLResult(
        identity=identity,
        status=MetadataSupport.SUPPORTED,
        fidelity=str(row.get("fidelity") or MetadataFidelity.NATIVE.value),
        source=MetadataSource.INFORMATION_SCHEMA,
        ddl=ddl_text,
    )


class BigQuerySyncDataDictionary(SyncDataDictionaryBase):
    """BigQuery synchronous SQLSpec data dictionary over ``INFORMATION_SCHEMA`` catalog SQL."""

    __slots__ = ()

    dialect: ClassVar[str] = "bigquery"

    def get_version(self, driver: SyncDriverAdapterBase) -> VersionInfo | None:
        """Return None because BigQuery is a versionless managed cloud service."""
        _ = driver
        return None

    def get_feature_flag(self, driver: SyncDriverAdapterBase, feature: str) -> bool:
        """Return whether ``feature`` is enabled by BigQuery GoogleSQL capabilities."""
        _ = driver
        return bool(self.get_dialect_config().get_feature_flag(feature))

    def get_optimal_type(self, driver: SyncDriverAdapterBase, type_category: str) -> str:
        """Return the canonical BigQuery SQL type name for ``type_category``."""
        _ = driver
        return self.get_dialect_config().get_optimal_type(type_category)

    def get_metadata_capabilities(
        self, driver: SyncDriverAdapterBase, domains: Sequence[str] | None = None
    ) -> MetadataCapabilityProfile:
        """Describe supported BigQuery metadata domains and billing/scope warnings."""
        _ = driver
        return _bigquery_capabilities(domains, type(self).__name__)

    def get_tables(self, driver: SyncDriverAdapterBase, schema: str | None = None) -> list[TableMetadata]:
        """List base tables sorted topologically by FK dependency depth."""
        query_text = self.get_query_text("tables", "by_schema")
        dataset_name, sql = _format_catalog_query(query_text, schema, driver)
        rows = [dict(row) for row in driver.select(sql, schema_name=dataset_name)]
        return _table_metadata_from_rows(rows, dataset_name)

    def get_columns(
        self, driver: SyncDriverAdapterBase, table: str | None = None, schema: str | None = None
    ) -> list[ColumnMetadata]:
        """Retrieve column metadata for a specific table or dataset schema."""
        query_name = "by_table" if table is not None else "by_schema"
        query_text = self.get_query_text("columns", query_name)
        dataset_name, sql = _format_catalog_query(query_text, schema, driver)
        params: dict[str, Any] = {"schema_name": dataset_name}
        if table is not None:
            params["table_name"] = table
        rows = [dict(row) for row in driver.select(sql, **params)]
        return _column_metadata_from_rows(rows, table, dataset_name)

    def get_indexes(
        self, driver: SyncDriverAdapterBase, table: str | None = None, schema: str | None = None
    ) -> list[IndexMetadata]:
        """List BigQuery search and vector indexes for a table or dataset."""
        rows: list[dict[str, Any]] = []
        dataset_name: str | None = None
        for query_name in ("search_by_dataset", "vector_by_dataset"):
            query_text = self.get_query_text("indexes", query_name)
            dataset_name, sql = _format_catalog_query(query_text, schema, driver)
            rows.extend(dict(row) for row in driver.select(sql, schema_name=dataset_name, table_name=table))
        return _index_metadata_from_rows(rows, dataset_name)

    def get_foreign_keys(
        self, driver: SyncDriverAdapterBase, table: str | None = None, schema: str | None = None
    ) -> list[ForeignKeyMetadata]:
        """Retrieve un-enforced BigQuery ``KEY_COLUMN_USAGE`` foreign key constraints."""
        query_name = "by_table" if table is not None else "by_schema"
        query_text = self.get_query_text("foreign_keys", query_name)
        dataset_name, sql = _format_catalog_query(query_text, schema, driver)
        params: dict[str, Any] = {"schema_name": dataset_name}
        if table is not None:
            params["table_name"] = table
        rows = [dict(row) for row in driver.select(sql, **params)]
        return _foreign_key_metadata_from_rows(rows, dataset_name)

    def get_partitions(
        self, driver: SyncDriverAdapterBase, table: str | None = None, schema: str | None = None
    ) -> list[PartitionMetadata]:
        """Retrieve BigQuery table partition storage statistics."""
        query_text = self.get_query_text("tables", "partitions_by_dataset")
        dataset_name, sql = _format_catalog_query(query_text, schema, driver)
        rows = [dict(row) for row in driver.select(sql, schema_name=dataset_name, table_name=table)]
        return _partition_metadata_from_rows(rows, dataset_name)

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
        _ = (include_dependencies, redact)
        query_text = self.get_query_text("ddl", "by_object")
        dataset_name, sql = _format_catalog_query(query_text, schema, driver)
        rows = [dict(row) for row in driver.select(sql, schema_name=dataset_name, table_name=object_name)]
        if not rows:
            return None
        return _ddl_result_from_row(
            rows[0],
            default_name=object_name,
            default_object_type=object_type,
            default_schema=dataset_name,
        )

    def _get_native_schema_ddl(
        self,
        driver: SyncDriverAdapterBase,
        schema: str | None,
        *,
        include_dependencies: bool,
        redact: bool,
    ) -> list[DDLResult]:
        _ = (include_dependencies, redact)
        query_text = self.get_query_text("ddl", "by_schema")
        dataset_name, sql = _format_catalog_query(query_text, schema, driver)
        rows = [dict(row) for row in driver.select(sql, schema_name=dataset_name)]
        return [
            _ddl_result_from_row(
                row,
                default_name="",
                default_object_type="table",
                default_schema=dataset_name,
            )
            for row in rows
        ]


class BigQueryAsyncDataDictionary(AsyncDataDictionaryBase):
    """BigQuery asynchronous SQLSpec data dictionary over ``INFORMATION_SCHEMA`` catalog SQL."""

    __slots__ = ()

    dialect: ClassVar[str] = "bigquery"

    async def get_version(self, driver: AsyncDriverAdapterBase) -> VersionInfo | None:
        """Return None because BigQuery is a versionless managed cloud service."""
        _ = driver
        return None

    async def get_feature_flag(self, driver: AsyncDriverAdapterBase, feature: str) -> bool:
        """Return whether ``feature`` is enabled by BigQuery GoogleSQL capabilities."""
        _ = driver
        return bool(self.get_dialect_config().get_feature_flag(feature))

    async def get_optimal_type(self, driver: AsyncDriverAdapterBase, type_category: str) -> str:
        """Return the canonical BigQuery SQL type name for ``type_category``."""
        _ = driver
        return self.get_dialect_config().get_optimal_type(type_category)

    async def get_metadata_capabilities(
        self, driver: AsyncDriverAdapterBase, domains: Sequence[str] | None = None
    ) -> MetadataCapabilityProfile:
        """Describe supported BigQuery metadata domains and billing/scope warnings."""
        _ = driver
        return _bigquery_capabilities(domains, type(self).__name__)

    async def get_tables(self, driver: AsyncDriverAdapterBase, schema: str | None = None) -> list[TableMetadata]:
        """List base tables sorted topologically by FK dependency depth."""
        query_text = self.get_query_text("tables", "by_schema")
        dataset_name, sql = _format_catalog_query(query_text, schema, driver)
        raw_rows = await driver.select(sql, schema_name=dataset_name)
        rows = [dict(row) for row in raw_rows]
        return _table_metadata_from_rows(rows, dataset_name)

    async def get_columns(
        self, driver: AsyncDriverAdapterBase, table: str | None = None, schema: str | None = None
    ) -> list[ColumnMetadata]:
        """Retrieve column metadata for a specific table or dataset schema."""
        query_name = "by_table" if table is not None else "by_schema"
        query_text = self.get_query_text("columns", query_name)
        dataset_name, sql = _format_catalog_query(query_text, schema, driver)
        params: dict[str, Any] = {"schema_name": dataset_name}
        if table is not None:
            params["table_name"] = table
        raw_rows = await driver.select(sql, **params)
        rows = [dict(row) for row in raw_rows]
        return _column_metadata_from_rows(rows, table, dataset_name)

    async def get_indexes(
        self, driver: AsyncDriverAdapterBase, table: str | None = None, schema: str | None = None
    ) -> list[IndexMetadata]:
        """List BigQuery search and vector indexes for a table or dataset."""
        rows: list[dict[str, Any]] = []
        dataset_name: str | None = None
        for query_name in ("search_by_dataset", "vector_by_dataset"):
            query_text = self.get_query_text("indexes", query_name)
            dataset_name, sql = _format_catalog_query(query_text, schema, driver)
            raw_rows = await driver.select(sql, schema_name=dataset_name, table_name=table)
            rows.extend(dict(row) for row in raw_rows)
        return _index_metadata_from_rows(rows, dataset_name)

    async def get_foreign_keys(
        self, driver: AsyncDriverAdapterBase, table: str | None = None, schema: str | None = None
    ) -> list[ForeignKeyMetadata]:
        """Retrieve un-enforced BigQuery ``KEY_COLUMN_USAGE`` foreign key constraints."""
        query_name = "by_table" if table is not None else "by_schema"
        query_text = self.get_query_text("foreign_keys", query_name)
        dataset_name, sql = _format_catalog_query(query_text, schema, driver)
        params: dict[str, Any] = {"schema_name": dataset_name}
        if table is not None:
            params["table_name"] = table
        raw_rows = await driver.select(sql, **params)
        rows = [dict(row) for row in raw_rows]
        return _foreign_key_metadata_from_rows(rows, dataset_name)

    async def get_partitions(
        self, driver: AsyncDriverAdapterBase, table: str | None = None, schema: str | None = None
    ) -> list[PartitionMetadata]:
        """Retrieve BigQuery table partition storage statistics."""
        query_text = self.get_query_text("tables", "partitions_by_dataset")
        dataset_name, sql = _format_catalog_query(query_text, schema, driver)
        raw_rows = await driver.select(sql, schema_name=dataset_name, table_name=table)
        rows = [dict(row) for row in raw_rows]
        return _partition_metadata_from_rows(rows, dataset_name)

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
        _ = (include_dependencies, redact)
        query_text = self.get_query_text("ddl", "by_object")
        dataset_name, sql = _format_catalog_query(query_text, schema, driver)
        raw_rows = await driver.select(sql, schema_name=dataset_name, table_name=object_name)
        rows = [dict(row) for row in raw_rows]
        if not rows:
            return None
        return _ddl_result_from_row(
            rows[0],
            default_name=object_name,
            default_object_type=object_type,
            default_schema=dataset_name,
        )

    async def _get_native_schema_ddl(
        self,
        driver: AsyncDriverAdapterBase,
        schema: str | None,
        *,
        include_dependencies: bool,
        redact: bool,
    ) -> list[DDLResult]:
        _ = (include_dependencies, redact)
        query_text = self.get_query_text("ddl", "by_schema")
        dataset_name, sql = _format_catalog_query(query_text, schema, driver)
        raw_rows = await driver.select(sql, schema_name=dataset_name)
        rows = [dict(row) for row in raw_rows]
        return [
            _ddl_result_from_row(
                row,
                default_name="",
                default_object_type="table",
                default_schema=dataset_name,
            )
            for row in rows
        ]
