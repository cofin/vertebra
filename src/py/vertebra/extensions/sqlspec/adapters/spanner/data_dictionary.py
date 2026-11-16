"""Spanner synchronous and asynchronous SQLSpec data dictionaries over GoogleSQL and Spangres catalog SQL."""

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, cast

from sqlspec.data_dictionary import (
    ColumnMetadata,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataCapability,
    MetadataCapabilityProfile,
    MetadataFidelity,
    MetadataRisk,
    MetadataSource,
    MetadataSupport,
    TableMetadata,
    VersionInfo,
    get_data_dictionary_loader,
)
from sqlspec.data_dictionary.dialects.spanner import SPANNER_CONFIG
from sqlspec.driver import AsyncDataDictionaryBase, SyncDataDictionaryBase

from vertebra.extensions.sqlspec.adapters.spanner.core import (
    normalize_spanner_dialect,
    sqlspec_dialect_name,
)

if TYPE_CHECKING:
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

__all__ = (
    "SpannerAsyncDataDictionary",
    "SpannerSyncDataDictionary",
)

_GOOGLE_SQL_DOMAINS: Final[tuple[str, ...]] = (
    "version",
    "tables",
    "columns",
    "indexes",
    "foreign_keys",
    "constraints",
    "objects",
    "sequences",
    "views",
    "routines",
    "privileges",
    "change_streams",
    "property_graphs",
    "placements",
    "system",
)

_SPANGRES_DOMAINS: Final[tuple[str, ...]] = (
    "version",
    "tables",
    "columns",
    "indexes",
    "foreign_keys",
    "constraints",
    "objects",
    "sequences",
    "change_streams",
    "system",
)

_SPANGRES_TYPE_OVERRIDES: Final[dict[str, str]] = {
    "json": "JSONB",
    "uuid": "BYTEA",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMPTZ",
    "text": "TEXT",
    "blob": "BYTEA",
    "numeric": "NUMERIC",
    "bignumeric": "NUMERIC",
    "integer": "BIGINT",
    "bigint": "BIGINT",
    "float": "DOUBLE PRECISION",
    "float32": "REAL",
    "varchar": "VARCHAR({length})",
}


def _resolve_driver_mode(driver: Any, configured: str | None) -> Literal["googlesql", "postgresql"]:
    if configured is not None:
        return "postgresql" if normalize_spanner_dialect(configured) == "spangres" else "googlesql"
    for candidate in (
        getattr(driver, "dialect", None),
        getattr(driver, "spanner_dialect", None),
        getattr(getattr(driver, "statement_config", None), "dialect", None),
        getattr(getattr(getattr(driver, "connection", None), "config", None), "dialect", None),
    ):
        if candidate and normalize_spanner_dialect(candidate) == "spangres":
            return "postgresql"
    return "googlesql"


def _require_catalog_sql(domain: str, name: str, mode: Literal["googlesql", "postgresql"]) -> str:
    loader = get_data_dictionary_loader()
    query_text = loader.get_domain_query_text("spanner", domain, name, mode=mode)
    if query_text is None and mode != "googlesql":
        query_text = loader.get_domain_query_text("spanner", domain, name)
    if query_text is None:
        msg = f"Missing Spanner catalog query {mode}/{domain}/{name}"
        raise RuntimeError(msg)
    return query_text


def _table_metadata_from_rows(
    rows: list[dict[str, Any]],
    default_schema: str,
) -> list[TableMetadata]:
    result: list[TableMetadata] = []
    for row in rows:
        schema_val = str(row.get("schema_name") or row.get("table_schema") or default_schema)
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
    default_schema: str,
) -> list[ColumnMetadata]:
    result: list[ColumnMetadata] = []
    for row in rows:
        row_table = str(row.get("table_name") or table or "")
        if table is not None and row_table and row_table != table:
            continue
        is_nullable_raw = row.get("is_nullable", "YES")
        is_nullable = (
            str(is_nullable_raw).upper() == "YES" if isinstance(is_nullable_raw, str) else bool(is_nullable_raw)
        )
        is_generated_raw = row.get("is_generated", "NO")
        is_generated = (
            str(is_generated_raw).upper() in {"ALWAYS", "YES", "TRUE", "1"}
            if isinstance(is_generated_raw, str)
            else bool(is_generated_raw)
        )
        data_type = str(row.get("data_type") or row.get("spanner_type") or "STRING")
        result.append(
            cast(
                ColumnMetadata,
                {
                    "schema_name": str(row.get("schema_name") or row.get("table_schema") or default_schema),
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
                    "is_primary": bool(row.get("is_primary", False)),
                    "is_unique": bool(row.get("is_unique", False)),
                    "extra": "",
                    "column_key": "PRI" if row.get("is_primary") else "",
                    "identity_generation": None,
                    "is_identity": bool(row.get("is_identity", False)),
                    "sequence_name": None,
                    "is_generated": is_generated,
                    "generation_expression": row.get("generation_expression"),
                    "is_hidden": False,
                    "column_comment": None,
                },
            )
        )
    return result


def _index_metadata_from_rows(
    rows: list[dict[str, Any]],
    default_schema: str,
) -> list[IndexMetadata]:
    result: list[IndexMetadata] = []
    for row in rows:
        cols_raw = row.get("columns")
        columns = (
            [str(c) for c in cols_raw if c]
            if isinstance(cols_raw, Sequence) and not isinstance(cols_raw, (str, bytes))
            else []
        )
        index_name = row.get("index_name")
        if not index_name:
            continue
        result.append(
            cast(
                IndexMetadata,
                {
                    "schema_name": str(row.get("schema_name") or row.get("table_schema") or default_schema),
                    "table_name": str(row.get("table_name") or ""),
                    "index_name": str(index_name),
                    "columns": columns,
                    "is_unique": bool(row.get("is_unique", False)),
                    "is_primary": bool(row.get("is_primary", str(index_name) == "PRIMARY_KEY")),
                    "expression": None,
                    "expressions": None,
                },
            )
        )
    return result


def _foreign_key_metadata_from_rows(
    rows: list[dict[str, Any]],
    default_schema: str,
) -> list[ForeignKeyMetadata]:
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
                    "table_schema": str(row.get("table_schema") or row.get("schema_name") or default_schema),
                    "referenced_table_schema": str(row.get("referenced_table_schema") or default_schema),
                },
            )
        )
    return result


def _spanner_capabilities(
    domains: Sequence[str] | None,
    mode: Literal["googlesql", "postgresql"],
    adapter_cls_name: str,
) -> MetadataCapabilityProfile:
    supported = _SPANGRES_DOMAINS if mode == "postgresql" else _GOOGLE_SQL_DOMAINS
    requested = tuple(domains) if domains is not None else supported
    capabilities: list[MetadataCapability] = []
    for domain in requested:
        if domain == "system":
            capabilities.append(
                MetadataCapability(
                    domain=domain,
                    support=MetadataSupport.SUPPORTED,
                    fidelity=MetadataFidelity.PARTIAL,
                    source=MetadataSource.SYSTEM_VIEW,
                    risks=(MetadataRisk.EXPENSIVE, MetadataRisk.PRIVILEGED),
                )
            )
        elif domain in supported:
            capabilities.append(
                MetadataCapability(
                    domain=domain,
                    support=MetadataSupport.SUPPORTED,
                    fidelity=MetadataFidelity.NATIVE,
                    source=MetadataSource.INFORMATION_SCHEMA,
                )
            )
        else:
            capabilities.append(MetadataCapability.unsupported(domain))
    dialect_name = "spangres" if mode == "postgresql" else "spanner"
    return MetadataCapabilityProfile(
        dialect=dialect_name,
        adapter=adapter_cls_name,
        capabilities=tuple(capabilities),
    )


class SpannerSyncDataDictionary(SyncDataDictionaryBase):
    """Synchronous SQLSpec data dictionary for Cloud Spanner (GoogleSQL and Spangres)."""

    dialect: ClassVar[str] = "spanner"

    def __init__(self, *, dialect: str | None = None) -> None:
        super().__init__()
        self._configured_dialect: str | None = dialect
        object.__setattr__(self, "dialect", sqlspec_dialect_name(dialect or "googlesql"))

    @property
    def configured_dialect(self) -> Literal["spanner", "spangres"]:
        """Return the configured SQLSpec dialect identifier."""
        return sqlspec_dialect_name(self._configured_dialect or "googlesql")

    def get_version(self, driver: "SyncDriverAdapterBase") -> VersionInfo | None:
        """Return None because Cloud Spanner is a versionless managed cloud database."""
        _ = driver
        return None

    def get_feature_flag(self, driver: "SyncDriverAdapterBase", feature: str) -> bool:
        """Return whether ``feature`` is supported by the active Spanner dialect."""
        mode = _resolve_driver_mode(driver, self._configured_dialect)
        if mode == "postgresql" and feature == "supports_property_graphs":
            return False
        return bool(SPANNER_CONFIG.feature_flags.get(feature, False))

    def get_optimal_type(self, driver: "SyncDriverAdapterBase", type_category: str) -> str:
        """Return the canonical Spanner GoogleSQL or Spangres SQL type name for ``type_category``."""
        mode = _resolve_driver_mode(driver, self._configured_dialect)
        if mode == "postgresql" and type_category in _SPANGRES_TYPE_OVERRIDES:
            return _SPANGRES_TYPE_OVERRIDES[type_category]
        return str(SPANNER_CONFIG.type_mappings.get(type_category, "STRING(MAX)"))

    def get_metadata_capabilities(
        self,
        driver: "SyncDriverAdapterBase",
        domains: Sequence[str] | None = None,
    ) -> MetadataCapabilityProfile:
        """Describe supported Cloud Spanner catalog metadata domains for the active dialect."""
        mode = _resolve_driver_mode(driver, self._configured_dialect)
        return _spanner_capabilities(domains, mode, type(self).__name__)

    def get_tables(
        self,
        driver: "SyncDriverAdapterBase",
        schema: str | None = None,
    ) -> list[TableMetadata]:
        """List Cloud Spanner tables including interleaved parent metadata when available."""
        mode = _resolve_driver_mode(driver, self._configured_dialect)
        default_schema = "public" if mode == "postgresql" else ""
        sql = _require_catalog_sql("tables", "by_schema", mode)
        rows = [dict(row) for row in driver.select(sql, {"schema_name": schema})]
        return _table_metadata_from_rows(rows, default_schema)

    def get_columns(
        self,
        driver: "SyncDriverAdapterBase",
        table: str | None = None,
        schema: str | None = None,
    ) -> list[ColumnMetadata]:
        """Retrieve column metadata for a Cloud Spanner table or schema."""
        mode = _resolve_driver_mode(driver, self._configured_dialect)
        default_schema = "public" if mode == "postgresql" else ""
        query_name = "by_table" if table is not None else "by_schema"
        sql = _require_catalog_sql("columns", query_name, mode)
        params: dict[str, Any] = {"schema_name": schema}
        if table is not None:
            params["table_name"] = table
        rows = [dict(row) for row in driver.select(sql, params)]
        return _column_metadata_from_rows(rows, table, default_schema)

    def get_indexes(
        self,
        driver: "SyncDriverAdapterBase",
        table: str | None = None,
        schema: str | None = None,
    ) -> list[IndexMetadata]:
        """Retrieve secondary, null-filtered, vector, and search indexes."""
        mode = _resolve_driver_mode(driver, self._configured_dialect)
        default_schema = "public" if mode == "postgresql" else ""
        query_name = "by_table" if table is not None else "by_schema"
        sql = _require_catalog_sql("indexes", query_name, mode)
        params: dict[str, Any] = {"schema_name": schema}
        if table is not None:
            params["table_name"] = table
        rows = [dict(row) for row in driver.select(sql, params)]
        return _index_metadata_from_rows(rows, default_schema)

    def get_foreign_keys(
        self,
        driver: "SyncDriverAdapterBase",
        table: str | None = None,
        schema: str | None = None,
    ) -> list[ForeignKeyMetadata]:
        """Retrieve referential foreign key constraints."""
        mode = _resolve_driver_mode(driver, self._configured_dialect)
        default_schema = "public" if mode == "postgresql" else ""
        query_name = "by_table" if table is not None else "by_schema"
        sql = _require_catalog_sql("foreign_keys", query_name, mode)
        params: dict[str, Any] = {"schema_name": schema}
        if table is not None:
            params["table_name"] = table
        rows = [dict(row) for row in driver.select(sql, params)]
        return _foreign_key_metadata_from_rows(rows, default_schema)

    def get_interleaved_tables(
        self,
        driver: "SyncDriverAdapterBase",
        parent_table: str | None = None,
        schema: str | None = None,
    ) -> list[dict[str, Any]]:
        """List interleaved child tables and their ``on_delete_action``."""
        mode = _resolve_driver_mode(driver, self._configured_dialect)
        sql = _require_catalog_sql("tables", "by_schema", mode)
        rows = [dict(row) for row in driver.select(sql, {"schema_name": schema})]
        filtered: list[dict[str, Any]] = []
        for row in rows:
            parent_val = row.get("parent_table_name")
            if not parent_val:
                continue
            if parent_table is not None and str(parent_val) != parent_table:
                continue
            filtered.append(row)
        return filtered

    def get_change_streams(
        self,
        driver: "SyncDriverAdapterBase",
        change_stream_name: str | None = None,
        schema: str | None = None,
    ) -> list[dict[str, Any]]:
        """List Cloud Spanner change streams registered in ``INFORMATION_SCHEMA``."""
        mode = _resolve_driver_mode(driver, self._configured_dialect)
        sql = _require_catalog_sql("change_streams", "list", mode)
        return [
            dict(row)
            for row in driver.select(
                sql,
                {"schema_name": schema, "change_stream_name": change_stream_name},
            )
        ]

    def get_property_graphs(
        self,
        driver: "SyncDriverAdapterBase",
        property_graph_name: str | None = None,
        schema: str | None = None,
    ) -> list[dict[str, Any]]:
        """List Spanner Graph property graph definitions in GoogleSQL databases."""
        mode = _resolve_driver_mode(driver, self._configured_dialect)
        if mode == "postgresql":
            return []
        sql = _require_catalog_sql("property_graphs", "list", mode)
        return [
            dict(row)
            for row in driver.select(
                sql,
                {"schema_name": schema, "property_graph_name": property_graph_name},
            )
        ]

    def list_available_features(self) -> list[str]:
        """Return sorted capability feature names declared for Cloud Spanner."""
        return sorted(SPANNER_CONFIG.feature_flags.keys())


class SpannerAsyncDataDictionary(AsyncDataDictionaryBase):
    """Asynchronous SQLSpec data dictionary for Cloud Spanner (GoogleSQL and Spangres)."""

    dialect: ClassVar[str] = "spanner"

    def __init__(self, *, dialect: str | None = None) -> None:
        super().__init__()
        self._configured_dialect: str | None = dialect
        object.__setattr__(self, "dialect", sqlspec_dialect_name(dialect or "googlesql"))

    @property
    def configured_dialect(self) -> Literal["spanner", "spangres"]:
        """Return the configured SQLSpec dialect identifier."""
        return sqlspec_dialect_name(self._configured_dialect or "googlesql")

    async def get_version(self, driver: "AsyncDriverAdapterBase") -> VersionInfo | None:
        """Return None because Cloud Spanner is a versionless managed cloud database."""
        _ = driver
        return None

    async def get_feature_flag(self, driver: "AsyncDriverAdapterBase", feature: str) -> bool:
        """Return whether ``feature`` is supported by the active Spanner dialect."""
        mode = _resolve_driver_mode(driver, self._configured_dialect)
        if mode == "postgresql" and feature == "supports_property_graphs":
            return False
        return bool(SPANNER_CONFIG.feature_flags.get(feature, False))

    async def get_optimal_type(self, driver: "AsyncDriverAdapterBase", type_category: str) -> str:
        """Return the canonical Spanner GoogleSQL or Spangres SQL type name for ``type_category``."""
        mode = _resolve_driver_mode(driver, self._configured_dialect)
        if mode == "postgresql" and type_category in _SPANGRES_TYPE_OVERRIDES:
            return _SPANGRES_TYPE_OVERRIDES[type_category]
        return str(SPANNER_CONFIG.type_mappings.get(type_category, "STRING(MAX)"))

    async def get_metadata_capabilities(
        self,
        driver: "AsyncDriverAdapterBase",
        domains: Sequence[str] | None = None,
    ) -> MetadataCapabilityProfile:
        """Describe supported Cloud Spanner catalog metadata domains for the active dialect."""
        mode = _resolve_driver_mode(driver, self._configured_dialect)
        return _spanner_capabilities(domains, mode, type(self).__name__)

    async def get_tables(
        self,
        driver: "AsyncDriverAdapterBase",
        schema: str | None = None,
    ) -> list[TableMetadata]:
        """Asynchronously list Cloud Spanner tables."""
        mode = _resolve_driver_mode(driver, self._configured_dialect)
        default_schema = "public" if mode == "postgresql" else ""
        sql = _require_catalog_sql("tables", "by_schema", mode)
        rows = [dict(row) for row in await driver.select(sql, {"schema_name": schema})]
        return _table_metadata_from_rows(rows, default_schema)

    async def get_columns(
        self,
        driver: "AsyncDriverAdapterBase",
        table: str | None = None,
        schema: str | None = None,
    ) -> list[ColumnMetadata]:
        """Asynchronously retrieve column metadata for a Cloud Spanner table or schema."""
        mode = _resolve_driver_mode(driver, self._configured_dialect)
        default_schema = "public" if mode == "postgresql" else ""
        query_name = "by_table" if table is not None else "by_schema"
        sql = _require_catalog_sql("columns", query_name, mode)
        params: dict[str, Any] = {"schema_name": schema}
        if table is not None:
            params["table_name"] = table
        rows = [dict(row) for row in await driver.select(sql, params)]
        return _column_metadata_from_rows(rows, table, default_schema)

    async def get_indexes(
        self,
        driver: "AsyncDriverAdapterBase",
        table: str | None = None,
        schema: str | None = None,
    ) -> list[IndexMetadata]:
        """Asynchronously retrieve secondary, null-filtered, vector, and search indexes."""
        mode = _resolve_driver_mode(driver, self._configured_dialect)
        default_schema = "public" if mode == "postgresql" else ""
        query_name = "by_table" if table is not None else "by_schema"
        sql = _require_catalog_sql("indexes", query_name, mode)
        params: dict[str, Any] = {"schema_name": schema}
        if table is not None:
            params["table_name"] = table
        rows = [dict(row) for row in await driver.select(sql, params)]
        return _index_metadata_from_rows(rows, default_schema)

    async def get_foreign_keys(
        self,
        driver: "AsyncDriverAdapterBase",
        table: str | None = None,
        schema: str | None = None,
    ) -> list[ForeignKeyMetadata]:
        """Asynchronously retrieve referential foreign key constraints."""
        mode = _resolve_driver_mode(driver, self._configured_dialect)
        default_schema = "public" if mode == "postgresql" else ""
        query_name = "by_table" if table is not None else "by_schema"
        sql = _require_catalog_sql("foreign_keys", query_name, mode)
        params: dict[str, Any] = {"schema_name": schema}
        if table is not None:
            params["table_name"] = table
        rows = [dict(row) for row in await driver.select(sql, params)]
        return _foreign_key_metadata_from_rows(rows, default_schema)

    async def get_interleaved_tables(
        self,
        driver: "AsyncDriverAdapterBase",
        parent_table: str | None = None,
        schema: str | None = None,
    ) -> list[dict[str, Any]]:
        """Asynchronously list interleaved child tables and their ``on_delete_action``."""
        mode = _resolve_driver_mode(driver, self._configured_dialect)
        sql = _require_catalog_sql("tables", "by_schema", mode)
        rows = [dict(row) for row in await driver.select(sql, {"schema_name": schema})]
        filtered: list[dict[str, Any]] = []
        for row in rows:
            parent_val = row.get("parent_table_name")
            if not parent_val:
                continue
            if parent_table is not None and str(parent_val) != parent_table:
                continue
            filtered.append(row)
        return filtered

    async def get_change_streams(
        self,
        driver: "AsyncDriverAdapterBase",
        change_stream_name: str | None = None,
        schema: str | None = None,
    ) -> list[dict[str, Any]]:
        """Asynchronously list Cloud Spanner change streams registered in ``INFORMATION_SCHEMA``."""
        mode = _resolve_driver_mode(driver, self._configured_dialect)
        sql = _require_catalog_sql("change_streams", "list", mode)
        return [
            dict(row)
            for row in await driver.select(
                sql,
                {"schema_name": schema, "change_stream_name": change_stream_name},
            )
        ]

    async def get_property_graphs(
        self,
        driver: "AsyncDriverAdapterBase",
        property_graph_name: str | None = None,
        schema: str | None = None,
    ) -> list[dict[str, Any]]:
        """Asynchronously list Spanner Graph property graph definitions in GoogleSQL databases."""
        mode = _resolve_driver_mode(driver, self._configured_dialect)
        if mode == "postgresql":
            return []
        sql = _require_catalog_sql("property_graphs", "list", mode)
        return [
            dict(row)
            for row in await driver.select(
                sql,
                {"schema_name": schema, "property_graph_name": property_graph_name},
            )
        ]

    def list_available_features(self) -> list[str]:
        """Return sorted capability feature names declared for Cloud Spanner."""
        return sorted(SPANNER_CONFIG.feature_flags.keys())
