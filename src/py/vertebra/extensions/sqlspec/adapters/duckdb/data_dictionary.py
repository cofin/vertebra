"""DuckDB synchronous and asynchronous SQLSpec data dictionaries."""

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Final, cast

from sqlspec.data_dictionary import (
    DDL_PARENT_TABLE_CONTEXT_KEY,
    ColumnMetadata,
    DDLResult,
    DependencyEdge,
    DependencyEdgeKind,
    DependencyMetadata,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataCapability,
    MetadataCapabilityProfile,
    MetadataFidelity,
    MetadataResult,
    MetadataSource,
    MetadataSupport,
    ObjectIdentity,
    SystemMetadataCapability,
    SystemMetadataRequest,
    SystemMetadataResult,
    TableMetadata,
    VersionInfo,
    ensure_system_metadata_request,
    get_data_dictionary_loader,
    system_metadata_gated_result,
    unsupported_system_metadata_capability,
)
from sqlspec.driver import AsyncDataDictionaryBase, SyncDataDictionaryBase
from sqlspec.utils.text import normalize_identifier

if TYPE_CHECKING:
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

__all__ = (
    "DuckDBAsyncDataDictionary",
    "DuckDBSyncDataDictionary",
)

_DUCKDB_METADATA_DOMAINS: Final[tuple[str, ...]] = (
    "schemas",
    "objects",
    "tables",
    "columns",
    "constraints",
    "indexes",
    "views",
    "routines",
    "databases",
    "extensions",
    "system",
    "dependencies",
    "types",
    "sequences",
    "ddl",
    "privileges",
)

_DUCKDB_SUPPORTED_DOMAINS: Final[frozenset[str]] = frozenset(d for d in _DUCKDB_METADATA_DOMAINS if d != "privileges")

_DUCKDB_SYSTEM_METADATA_QUERIES: Final[dict[str, str]] = {
    "logs": "logs",
    "memory": "memory",
    "settings": "settings",
}

_DUCKDB_TABLE_CONSTRAINT_TYPES: Final[frozenset[str]] = frozenset({"PRIMARY KEY", "UNIQUE", "CHECK"})
_DUCKDB_FOREIGN_KEY_WARNING: Final[str] = (
    "DuckDB catalog SQL omits self-referencing foreign keys and leaves referenced table names unquoted; "
    "table DDL was synthesized from catalog metadata"
)
_DUCKDB_REFERENCE_EDGE_KINDS: Final[dict[str, DependencyEdgeKind]] = {
    "table": DependencyEdgeKind.VIEW_REFERENCE,
    "temporary_table": DependencyEdgeKind.VIEW_REFERENCE,
    "view": DependencyEdgeKind.VIEW_REFERENCE,
    "sequence": DependencyEdgeKind.SEQUENCE_USE,
    "type": DependencyEdgeKind.DEFAULT_EXPRESSION,
}


def _duckdb_capability_for_domain(domain: str) -> MetadataCapability:
    if domain in _DUCKDB_SUPPORTED_DOMAINS:
        return MetadataCapability(
            domain=domain,
            support=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.NATIVE,
            source=MetadataSource.CATALOG,
        )
    return MetadataCapability.unsupported(domain)


def _duckdb_system_metadata_capability(domain: str) -> SystemMetadataCapability:
    if domain not in _DUCKDB_SYSTEM_METADATA_QUERIES:
        return unsupported_system_metadata_capability(domain)
    return SystemMetadataCapability(
        domain,
        MetadataSupport.SUPPORTED,
        fidelity=MetadataFidelity.NATIVE,
        source=MetadataSource.SYSTEM_VIEW,
    )


def _rows_as_mappings(rows: Sequence[Any]) -> tuple[Mapping[str, object], ...]:
    return tuple(dict(row) if not isinstance(row, Mapping) else row for row in rows)


def _duckdb_index_columns(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value]
    if not isinstance(value, str):
        return [str(value)]
    cleaned = value.strip()
    if cleaned.startswith("[") and cleaned.endswith("]"):
        cleaned = cleaned[1:-1].strip()
    if not cleaned:
        return []
    return [part.strip().strip('"') for part in cleaned.split(",")]


def _duckdb_index_rows_to_metadata(rows: Sequence[Mapping[str, Any]]) -> list[IndexMetadata]:
    indexes: list[IndexMetadata] = []
    for row in rows:
        index_name = row.get("index_name")
        table_name = row.get("table_name")
        if index_name is None or table_name is None:
            continue
        index_metadata: dict[str, Any] = {
            "index_name": str(index_name),
            "table_name": str(table_name),
            "columns": _duckdb_index_columns(row.get("columns")),
            "is_unique": bool(row.get("is_unique")),
            "is_primary": bool(row.get("is_primary")),
        }
        schema_name = row.get("schema_name")
        if schema_name is not None:
            index_metadata["schema_name"] = str(schema_name)
        native_sql = row.get("native_sql")
        if native_sql is not None:
            index_metadata["native_sql"] = native_sql
        indexes.append(cast("IndexMetadata", index_metadata))
    return indexes


def _duckdb_constraint_items(rows: Sequence[Mapping[str, Any]]) -> MetadataResult:
    items: list[dict[str, Any]] = []
    for row in rows:
        constraint_type = str(row.get("constraint_type") or "").upper()
        if constraint_type not in _DUCKDB_TABLE_CONSTRAINT_TYPES:
            continue
        item: dict[str, Any] = {
            "schema_name": row.get("schema_name"),
            "table_name": row.get("table_name"),
            "constraint_name": row.get("constraint_name"),
            "constraint_type": constraint_type,
            "columns": [str(column) for column in row.get("constraint_column_names") or ()],
        }
        if constraint_type == "CHECK":
            item["check_clause"] = row.get("expression") or row.get("constraint_text")
        items.append(item)
    return MetadataResult("constraints", items=tuple(items))


def _duckdb_dependency_items(
    rows: Sequence[Mapping[str, Any]],
    *,
    object_name: str | None,
    schema_name: str | None,
    dialect: str,
) -> MetadataResult:
    items: list[DependencyMetadata] = []
    for row in rows:
        if (
            object_name is not None
            and row.get("object_name") != object_name
            and row.get("referenced_object_name") != object_name
        ):
            continue
        name = str(row.get("object_name") or row.get("objid") or "dependency")
        object_type = str(row.get("object_type") or "dependency")
        identity = ObjectIdentity(
            name=name,
            object_type=object_type,
            schema=cast("str | None", row.get("schema_name", schema_name)),
            dialect=dialect,
            source=MetadataSource.CATALOG,
        )
        items.append(DependencyMetadata(identity, attributes=dict(row)))
    return MetadataResult("dependencies", items=tuple(items))


class _DuckDBDataDictionaryMixin:
    dialect: ClassVar[str] = "duckdb"

    def _get_domain_query_text(self, domain: str, query_name: str) -> str:
        query_text = get_data_dictionary_loader().get_domain_query_text(type(self).dialect, domain, query_name)
        if query_text is None:
            msg = f"Missing DuckDB data-dictionary query: {domain}/{query_name}"
            raise RuntimeError(msg)
        return query_text

    def _ddl_result_from_row(self, row: Mapping[str, Any]) -> DDLResult:
        object_type = str(row.get("object_type") or "table").lower()
        row_schema = row.get("schema_name")
        identity = ObjectIdentity(
            name=str(row.get("object_name") or ""),
            object_type=object_type,
            schema=str(row_schema) if row_schema is not None else None,
            dialect=type(self).dialect,
            source=MetadataSource.CATALOG,
        )
        parent = row.get("table_name")
        context: dict[str, object] = (
            {DDL_PARENT_TABLE_CONTEXT_KEY: str(parent)} if object_type == "index" and parent is not None else {}
        )
        ddl_value = row.get("ddl")
        if ddl_value is None:
            return DDLResult.unsupported(
                identity,
                source=MetadataSource.CATALOG,
                context=context,
                warnings=(f"DuckDB does not expose reconstructable DDL for {object_type} {identity.name}",),
            )
        if object_type in {"table", "temporary_table"} and row.get("has_foreign_keys"):
            return DDLResult.unsupported(
                identity,
                source=MetadataSource.CATALOG,
                warnings=(_DUCKDB_FOREIGN_KEY_WARNING,),
            )
        return DDLResult(
            identity=identity,
            status=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.NATIVE,
            source=MetadataSource.CATALOG,
            ddl=str(ddl_value),
            context=context,
        )

    def _link_schema_ddl(
        self,
        rows: Sequence[Mapping[str, Any]],
        references_identifier: Any,
    ) -> list[DDLResult]:
        results = [self._ddl_result_from_row(row) for row in rows]
        raw_sql = {
            (str(row.get("object_type") or "").lower(), str(row.get("object_name") or "")): str(row.get("ddl") or "")
            for row in rows
        }
        relations = {
            item.identity.name.lower(): item.identity
            for item in results
            if item.identity.object_type in {"table", "temporary_table", "view"}
        }
        referenced = {
            item.identity.name.lower(): item.identity
            for item in results
            if item.identity.object_type in {"sequence", "type"}
        }
        linked: list[DDLResult] = []
        for item in results:
            edges: list[DependencyEdge] = []
            ddl_text = raw_sql.get((item.identity.object_type, item.identity.name), "")
            object_type = item.identity.object_type
            parent = item.context.get(DDL_PARENT_TABLE_CONTEXT_KEY)
            if object_type == "index" and parent is not None:
                parent_identity = relations.get(str(parent).lower())
                if parent_identity is not None:
                    edges.append(
                        DependencyEdge(
                            item.identity,
                            parent_identity,
                            DependencyEdgeKind.INDEX_EXPRESSION,
                            source=MetadataSource.CATALOG,
                        )
                    )
            elif object_type in {"table", "temporary_table", "view"} and ddl_text:
                candidates = referenced if object_type != "view" else {**relations, **referenced}
                edges.extend(
                    DependencyEdge(
                        item.identity,
                        ref_identity,
                        _DUCKDB_REFERENCE_EDGE_KINDS[ref_identity.object_type],
                        source=MetadataSource.CATALOG,
                    )
                    for ref_key, ref_identity in candidates.items()
                    if ref_key != item.identity.name.lower() and references_identifier(ddl_text, ref_identity.name)
                )
            linked.append(
                DDLResult(
                    identity=item.identity,
                    status=item.status,
                    fidelity=item.fidelity,
                    source=item.source,
                    ddl=item.ddl,
                    dependencies=tuple(edges),
                    context=item.context,
                    warnings=item.warnings,
                )
            )
        return linked


class DuckDBSyncDataDictionary(_DuckDBDataDictionaryMixin, SyncDataDictionaryBase):
    """Synchronous DuckDB data dictionary executing bundled SQLSpec `duckdb/sql/*.sql` catalog queries."""

    dialect: ClassVar[str] = "duckdb"

    def __init__(self) -> None:
        super().__init__()

    def resolve_connection_schema(self, driver: "SyncDriverAdapterBase", schema: str | None) -> str | None:
        """Resolve an explicit schema or query DuckDB's `current_schema()`."""
        if schema is not None:
            return self.resolve_schema(schema)
        current_schema = driver.select_value_or_none("SELECT current_schema()")
        return str(current_schema) if current_schema else self.get_dialect_config().default_schema

    def resolve_ddl_object_name(
        self,
        driver: "SyncDriverAdapterBase",
        object_name: str,
        schema: str | None,
    ) -> tuple[str, str | None]:
        """Normalize an object identifier and resolve its target DuckDB schema."""
        return (
            normalize_identifier(object_name, self.get_dialect_config().name),
            self.resolve_connection_schema(driver, schema),
        )

    def get_metadata_capabilities(
        self,
        driver: "SyncDriverAdapterBase",
        domains: Sequence[str] | None = None,
    ) -> MetadataCapabilityProfile:
        """Return DuckDB metadata capability disclosures."""
        _ = driver
        requested_domains = tuple(domains) if domains is not None else _DUCKDB_METADATA_DOMAINS
        capabilities = tuple(_duckdb_capability_for_domain(domain) for domain in requested_domains)
        return MetadataCapabilityProfile(self.dialect, adapter=type(self).__name__, capabilities=capabilities)

    def get_system_metadata_capabilities(
        self,
        driver: "SyncDriverAdapterBase",
        domains: Sequence[str] | None = None,
    ) -> tuple[SystemMetadataCapability, ...]:
        """Return DuckDB system metadata capability disclosures."""
        _ = driver
        requested_domains = tuple(_DUCKDB_SYSTEM_METADATA_QUERIES) if domains is None else tuple(domains)
        return tuple(_duckdb_system_metadata_capability(domain) for domain in requested_domains)

    def get_version(self, driver: "SyncDriverAdapterBase") -> VersionInfo | None:
        """Query and cache DuckDB version information via `version.current`."""
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)
        version_value = driver.select_value_or_none(self.get_query("version", "current"))
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

    def get_feature_flag(self, driver: "SyncDriverAdapterBase", feature: str) -> bool:
        """Evaluate whether the connected DuckDB instance supports `feature`."""
        version_info = self.get_version(driver)
        return self.resolve_feature_flag(feature, version_info)

    def get_optimal_type(self, driver: "SyncDriverAdapterBase", type_category: str) -> str:
        """Return optimal DuckDB SQL type name for `type_category`."""
        _ = driver
        return self.get_dialect_config().get_optimal_type(type_category)

    def get_schemas(self, driver: "SyncDriverAdapterBase") -> MetadataResult:
        """List accessible DuckDB database schemas."""
        rows = driver.select(self.get_query("schemas", "list"))
        return MetadataResult("schemas", capability=_duckdb_capability_for_domain("schemas"), items=tuple(rows))

    def get_objects(self, driver: "SyncDriverAdapterBase", schema: str | None = None) -> MetadataResult:
        """List tables and views in `schema`."""
        schema_name = self.resolve_schema(schema)
        rows = driver.select(self.get_query("objects", "by_schema"), schema_name=schema_name)
        return MetadataResult("objects", capability=_duckdb_capability_for_domain("objects"), items=tuple(rows))

    def get_routines(self, driver: "SyncDriverAdapterBase", schema: str | None = None) -> MetadataResult:
        """List user-defined and scalar/table functions in `schema`."""
        schema_name = self.resolve_schema(schema)
        rows = driver.select(self.get_query("functions", "by_schema"), schema_name=schema_name)
        return MetadataResult("routines", capability=_duckdb_capability_for_domain("routines"), items=tuple(rows))

    def get_databases(self, driver: "SyncDriverAdapterBase") -> MetadataResult:
        """List attached DuckDB databases."""
        rows = driver.select(self.get_query("databases", "list"))
        return MetadataResult("databases", capability=_duckdb_capability_for_domain("databases"), items=tuple(rows))

    def get_extensions(self, driver: "SyncDriverAdapterBase") -> MetadataResult:
        """List installed and loaded DuckDB extensions."""
        rows = driver.select(self.get_query("extensions", "list"))
        return MetadataResult("extensions", capability=_duckdb_capability_for_domain("extensions"), items=tuple(rows))

    def get_table_details(
        self, driver: "SyncDriverAdapterBase", table: str, schema: str | None = None
    ) -> MetadataResult:
        """Return detailed metadata for `table`."""
        schema_name = self.resolve_schema(schema)
        rows = driver.select(self.get_query("tables", "by_schema"), schema_name=schema_name, table_name=table)
        return MetadataResult("tables", capability=_duckdb_capability_for_domain("tables"), items=tuple(rows))

    def get_tables(self, driver: "SyncDriverAdapterBase", schema: str | None = None) -> list[TableMetadata]:
        """List DuckDB tables ordered topologically by foreign-key dependencies."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        return cast(
            "list[TableMetadata]",
            driver.select(
                self.get_query("tables", "by_schema"),
                schema_name=schema_name,
                table_name=None,
                schema_type=TableMetadata,
            ),
        )

    def get_columns(
        self,
        driver: "SyncDriverAdapterBase",
        table: str | None = None,
        schema: str | None = None,
    ) -> list[ColumnMetadata]:
        """Retrieve DuckDB column metadata for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            return cast(
                "list[ColumnMetadata]",
                driver.select(
                    self._get_domain_query_text("columns", "by_schema"),
                    schema_name=schema_name,
                    schema_type=ColumnMetadata,
                ),
            )
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="columns")
        return cast(
            "list[ColumnMetadata]",
            driver.select(
                self._get_domain_query_text("columns", "by_table"),
                table_name=table,
                schema_name=schema_name,
                schema_type=ColumnMetadata,
            ),
        )

    def get_indexes(
        self,
        driver: "SyncDriverAdapterBase",
        table: str | None = None,
        schema: str | None = None,
    ) -> list[IndexMetadata]:
        """Retrieve DuckDB index metadata for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            rows = driver.select(self._get_domain_query_text("indexes", "by_schema"), schema_name=schema_name)
            return _duckdb_index_rows_to_metadata(rows)
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="indexes")
        rows = driver.select(
            self._get_domain_query_text("indexes", "by_table"),
            table_name=table,
            schema_name=schema_name,
        )
        return _duckdb_index_rows_to_metadata(rows)

    def get_foreign_keys(
        self,
        driver: "SyncDriverAdapterBase",
        table: str | None = None,
        schema: str | None = None,
    ) -> list[ForeignKeyMetadata]:
        """Retrieve DuckDB foreign key metadata for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            rows = driver.select(self.get_query("foreign_keys", "by_schema"), schema_name=schema_name)
        else:
            self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
            rows = driver.select(
                self.get_query("foreign_keys", "by_table"),
                table_name=table,
                schema_name=schema_name,
            )
        return [ForeignKeyMetadata.from_row(row) for row in rows]

    def get_constraints(
        self,
        driver: "SyncDriverAdapterBase",
        table: str | None = None,
        schema: str | None = None,
    ) -> MetadataResult:
        """Return PRIMARY KEY, UNIQUE, and CHECK constraint metadata from `duckdb_constraints()`."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            rows = driver.select(self._get_domain_query_text("constraints", "by_schema"), schema_name=schema_name)
        else:
            rows = driver.select(
                self._get_domain_query_text("constraints", "by_table"),
                schema_name=schema_name,
                table_name=table,
            )
        return _duckdb_constraint_items(rows)

    def get_views(self, driver: "SyncDriverAdapterBase", schema: str | None = None) -> MetadataResult:
        """List DuckDB views in `schema`."""
        schema_name = self.resolve_schema(schema)
        rows = driver.select(self._get_domain_query_text("views", "by_schema"), schema_name=schema_name)
        return MetadataResult("views", items=tuple(rows))

    def get_sequences(self, driver: "SyncDriverAdapterBase", schema: str | None = None) -> MetadataResult:
        """List DuckDB sequences in `schema`."""
        schema_name = self.resolve_schema(schema)
        rows = driver.select(self._get_domain_query_text("sequences", "by_schema"), schema_name=schema_name)
        return MetadataResult("sequences", items=tuple(rows))

    def get_types(self, driver: "SyncDriverAdapterBase", schema: str | None = None) -> MetadataResult:
        """List user-defined DuckDB types in `schema`."""
        schema_name = self.resolve_schema(schema)
        rows = driver.select(self._get_domain_query_text("types", "by_schema"), schema_name=schema_name)
        return MetadataResult("types", items=tuple(rows))

    def get_dependencies(
        self,
        driver: "SyncDriverAdapterBase",
        object_name: str | None = None,
        schema: str | None = None,
    ) -> MetadataResult:
        """List object dependency relationships from `duckdb_dependencies()`."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=object_name, operation="dependencies")
        rows = driver.select(self._get_domain_query_text("dependencies", "by_schema"), schema_name=schema_name)
        return _duckdb_dependency_items(
            rows,
            object_name=object_name,
            schema_name=schema_name,
            dialect=type(self).dialect,
        )

    def get_system_metadata(
        self,
        driver: "SyncDriverAdapterBase",
        request: SystemMetadataRequest | str | None = None,
        **kwargs: Any,
    ) -> SystemMetadataResult:
        """Return opt-in DuckDB logs, memory, or settings system metadata."""
        metadata_request = ensure_system_metadata_request(request, **kwargs)
        capability = _duckdb_system_metadata_capability(metadata_request.domain)
        gate_result = system_metadata_gated_result(metadata_request, capability)
        if gate_result.capability.support != MetadataSupport.SUPPORTED:
            return gate_result
        query_name = _DUCKDB_SYSTEM_METADATA_QUERIES.get(metadata_request.domain)
        if query_name is None:
            return gate_result
        rows = driver.select(self.get_query("system", query_name))
        return SystemMetadataResult.from_rows(
            metadata_request,
            capability,
            rows=_rows_as_mappings(rows),
            source=MetadataSource.SYSTEM_VIEW,
        )

    def _get_native_object_ddl(
        self,
        driver: "SyncDriverAdapterBase",
        object_name: str,
        *,
        schema: str | None,
        object_type: str,
        include_dependencies: bool,
        redact: bool,
    ) -> DDLResult | None:
        _ = include_dependencies, redact
        rows = driver.select(
            self._get_domain_query_text("ddl", "by_object"),
            schema_name=schema,
            object_name=object_name,
            object_type=object_type,
        )
        if not rows:
            return DDLResult.unsupported(
                ObjectIdentity(object_name, object_type, schema=schema, dialect=type(self).dialect),
                source=MetadataSource.CATALOG,
            )
        return self._ddl_result_from_row(rows[0])

    def _get_native_schema_ddl(
        self,
        driver: "SyncDriverAdapterBase",
        schema: str | None,
        *,
        include_dependencies: bool,
        redact: bool,
    ) -> list[DDLResult] | None:
        _ = include_dependencies, redact
        rows = driver.select(
            self._get_domain_query_text("ddl", "by_schema"),
            schema_name=self.resolve_schema(schema),
        )
        return self._link_schema_ddl(rows, self._references_identifier)


class DuckDBAsyncDataDictionary(_DuckDBDataDictionaryMixin, AsyncDataDictionaryBase):
    """Asynchronous DuckDB data dictionary executing bundled SQLSpec `duckdb/sql/*.sql` catalog queries."""

    dialect: ClassVar[str] = "duckdb"

    def __init__(self) -> None:
        super().__init__()

    async def resolve_connection_schema(self, driver: "AsyncDriverAdapterBase", schema: str | None) -> str | None:
        """Resolve an explicit schema or query DuckDB's `current_schema()` asynchronously."""
        if schema is not None:
            return self.resolve_schema(schema)
        current_schema = await driver.select_value_or_none("SELECT current_schema()")
        return str(current_schema) if current_schema else self.get_dialect_config().default_schema

    def resolve_ddl_object_name(
        self,
        driver: "AsyncDriverAdapterBase",
        object_name: str,
        schema: str | None,
    ) -> tuple[str, str | None]:
        """Normalize an object identifier and resolve its target DuckDB schema."""
        _ = driver
        return (
            normalize_identifier(object_name, self.get_dialect_config().name),
            self.resolve_schema(schema),
        )

    async def get_metadata_capabilities(
        self,
        driver: "AsyncDriverAdapterBase",
        domains: Sequence[str] | None = None,
    ) -> MetadataCapabilityProfile:
        """Return DuckDB metadata capability disclosures asynchronously."""
        _ = driver
        requested_domains = tuple(domains) if domains is not None else _DUCKDB_METADATA_DOMAINS
        capabilities = tuple(_duckdb_capability_for_domain(domain) for domain in requested_domains)
        return MetadataCapabilityProfile(self.dialect, adapter=type(self).__name__, capabilities=capabilities)

    async def get_system_metadata_capabilities(
        self,
        driver: "AsyncDriverAdapterBase",
        domains: Sequence[str] | None = None,
    ) -> tuple[SystemMetadataCapability, ...]:
        """Return DuckDB system metadata capability disclosures asynchronously."""
        _ = driver
        requested_domains = tuple(_DUCKDB_SYSTEM_METADATA_QUERIES) if domains is None else tuple(domains)
        return tuple(_duckdb_system_metadata_capability(domain) for domain in requested_domains)

    async def get_version(self, driver: "AsyncDriverAdapterBase") -> VersionInfo | None:
        """Asynchronously query and cache DuckDB version information via `version.current`."""
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)
        version_value = await driver.select_value_or_none(self.get_query("version", "current"))
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

    async def get_feature_flag(self, driver: "AsyncDriverAdapterBase", feature: str) -> bool:
        """Asynchronously evaluate whether the connected DuckDB instance supports `feature`."""
        version_info = await self.get_version(driver)
        return self.resolve_feature_flag(feature, version_info)

    async def get_optimal_type(self, driver: "AsyncDriverAdapterBase", type_category: str) -> str:
        """Return optimal DuckDB SQL type name for `type_category` asynchronously."""
        _ = driver
        return self.get_dialect_config().get_optimal_type(type_category)

    async def get_schemas(self, driver: "AsyncDriverAdapterBase") -> MetadataResult:
        """Asynchronously list accessible DuckDB database schemas."""
        rows = await driver.select(self.get_query("schemas", "list"))
        return MetadataResult("schemas", capability=_duckdb_capability_for_domain("schemas"), items=tuple(rows))

    async def get_objects(self, driver: "AsyncDriverAdapterBase", schema: str | None = None) -> MetadataResult:
        """Asynchronously list tables and views in `schema`."""
        schema_name = self.resolve_schema(schema)
        rows = await driver.select(self.get_query("objects", "by_schema"), schema_name=schema_name)
        return MetadataResult("objects", capability=_duckdb_capability_for_domain("objects"), items=tuple(rows))

    async def get_routines(self, driver: "AsyncDriverAdapterBase", schema: str | None = None) -> MetadataResult:
        """Asynchronously list user-defined functions in `schema`."""
        schema_name = self.resolve_schema(schema)
        rows = await driver.select(self.get_query("functions", "by_schema"), schema_name=schema_name)
        return MetadataResult("routines", capability=_duckdb_capability_for_domain("routines"), items=tuple(rows))

    async def get_databases(self, driver: "AsyncDriverAdapterBase") -> MetadataResult:
        """Asynchronously list attached DuckDB databases."""
        rows = await driver.select(self.get_query("databases", "list"))
        return MetadataResult("databases", capability=_duckdb_capability_for_domain("databases"), items=tuple(rows))

    async def get_extensions(self, driver: "AsyncDriverAdapterBase") -> MetadataResult:
        """Asynchronously list installed and loaded DuckDB extensions."""
        rows = await driver.select(self.get_query("extensions", "list"))
        return MetadataResult("extensions", capability=_duckdb_capability_for_domain("extensions"), items=tuple(rows))

    async def get_table_details(
        self,
        driver: "AsyncDriverAdapterBase",
        table: str,
        schema: str | None = None,
    ) -> MetadataResult:
        """Asynchronously return detailed metadata for `table`."""
        schema_name = self.resolve_schema(schema)
        rows = await driver.select(self.get_query("tables", "by_schema"), schema_name=schema_name, table_name=table)
        return MetadataResult("tables", capability=_duckdb_capability_for_domain("tables"), items=tuple(rows))

    async def get_tables(self, driver: "AsyncDriverAdapterBase", schema: str | None = None) -> list[TableMetadata]:
        """Asynchronously list DuckDB tables ordered topologically by foreign-key dependencies."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        return cast(
            "list[TableMetadata]",
            await driver.select(
                self.get_query("tables", "by_schema"),
                schema_name=schema_name,
                table_name=None,
                schema_type=TableMetadata,
            ),
        )

    async def get_columns(
        self,
        driver: "AsyncDriverAdapterBase",
        table: str | None = None,
        schema: str | None = None,
    ) -> list[ColumnMetadata]:
        """Asynchronously retrieve DuckDB column metadata for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            return cast(
                "list[ColumnMetadata]",
                await driver.select(
                    self._get_domain_query_text("columns", "by_schema"),
                    schema_name=schema_name,
                    schema_type=ColumnMetadata,
                ),
            )
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="columns")
        return cast(
            "list[ColumnMetadata]",
            await driver.select(
                self._get_domain_query_text("columns", "by_table"),
                table_name=table,
                schema_name=schema_name,
                schema_type=ColumnMetadata,
            ),
        )

    async def get_indexes(
        self,
        driver: "AsyncDriverAdapterBase",
        table: str | None = None,
        schema: str | None = None,
    ) -> list[IndexMetadata]:
        """Asynchronously retrieve DuckDB index metadata for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            rows = await driver.select(self._get_domain_query_text("indexes", "by_schema"), schema_name=schema_name)
            return _duckdb_index_rows_to_metadata(rows)
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="indexes")
        rows = await driver.select(
            self._get_domain_query_text("indexes", "by_table"),
            table_name=table,
            schema_name=schema_name,
        )
        return _duckdb_index_rows_to_metadata(rows)

    async def get_foreign_keys(
        self,
        driver: "AsyncDriverAdapterBase",
        table: str | None = None,
        schema: str | None = None,
    ) -> list[ForeignKeyMetadata]:
        """Asynchronously retrieve DuckDB foreign key metadata for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            rows = await driver.select(self.get_query("foreign_keys", "by_schema"), schema_name=schema_name)
        else:
            self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
            rows = await driver.select(
                self.get_query("foreign_keys", "by_table"),
                table_name=table,
                schema_name=schema_name,
            )
        return [ForeignKeyMetadata.from_row(row) for row in rows]

    async def get_constraints(
        self,
        driver: "AsyncDriverAdapterBase",
        table: str | None = None,
        schema: str | None = None,
    ) -> MetadataResult:
        """Asynchronously return PRIMARY KEY, UNIQUE, and CHECK constraints."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            rows = await driver.select(self._get_domain_query_text("constraints", "by_schema"), schema_name=schema_name)
        else:
            rows = await driver.select(
                self._get_domain_query_text("constraints", "by_table"),
                schema_name=schema_name,
                table_name=table,
            )
        return _duckdb_constraint_items(rows)

    async def get_views(self, driver: "AsyncDriverAdapterBase", schema: str | None = None) -> MetadataResult:
        """Asynchronously list DuckDB views in `schema`."""
        schema_name = self.resolve_schema(schema)
        rows = await driver.select(self._get_domain_query_text("views", "by_schema"), schema_name=schema_name)
        return MetadataResult("views", items=tuple(rows))

    async def get_sequences(self, driver: "AsyncDriverAdapterBase", schema: str | None = None) -> MetadataResult:
        """Asynchronously list DuckDB sequences in `schema`."""
        schema_name = self.resolve_schema(schema)
        rows = await driver.select(self._get_domain_query_text("sequences", "by_schema"), schema_name=schema_name)
        return MetadataResult("sequences", items=tuple(rows))

    async def get_types(self, driver: "AsyncDriverAdapterBase", schema: str | None = None) -> MetadataResult:
        """Asynchronously list user-defined DuckDB types in `schema`."""
        schema_name = self.resolve_schema(schema)
        rows = await driver.select(self._get_domain_query_text("types", "by_schema"), schema_name=schema_name)
        return MetadataResult("types", items=tuple(rows))

    async def get_dependencies(
        self,
        driver: "AsyncDriverAdapterBase",
        object_name: str | None = None,
        schema: str | None = None,
    ) -> MetadataResult:
        """Asynchronously list object dependency relationships from `duckdb_dependencies()`."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=object_name, operation="dependencies")
        rows = await driver.select(self._get_domain_query_text("dependencies", "by_schema"), schema_name=schema_name)
        return _duckdb_dependency_items(
            rows,
            object_name=object_name,
            schema_name=schema_name,
            dialect=type(self).dialect,
        )

    async def get_system_metadata(
        self,
        driver: "AsyncDriverAdapterBase",
        request: SystemMetadataRequest | str | None = None,
        **kwargs: Any,
    ) -> SystemMetadataResult:
        """Asynchronously return opt-in DuckDB logs, memory, or settings system metadata."""
        metadata_request = ensure_system_metadata_request(request, **kwargs)
        capability = _duckdb_system_metadata_capability(metadata_request.domain)
        gate_result = system_metadata_gated_result(metadata_request, capability)
        if gate_result.capability.support != MetadataSupport.SUPPORTED:
            return gate_result
        query_name = _DUCKDB_SYSTEM_METADATA_QUERIES.get(metadata_request.domain)
        if query_name is None:
            return gate_result
        rows = await driver.select(self.get_query("system", query_name))
        return SystemMetadataResult.from_rows(
            metadata_request,
            capability,
            rows=_rows_as_mappings(rows),
            source=MetadataSource.SYSTEM_VIEW,
        )

    async def _get_native_object_ddl(
        self,
        driver: "AsyncDriverAdapterBase",
        object_name: str,
        *,
        schema: str | None,
        object_type: str,
        include_dependencies: bool,
        redact: bool,
    ) -> DDLResult | None:
        _ = include_dependencies, redact
        rows = await driver.select(
            self._get_domain_query_text("ddl", "by_object"),
            schema_name=schema,
            object_name=object_name,
            object_type=object_type,
        )
        if not rows:
            return DDLResult.unsupported(
                ObjectIdentity(object_name, object_type, schema=schema, dialect=type(self).dialect),
                source=MetadataSource.CATALOG,
            )
        return self._ddl_result_from_row(rows[0])

    async def _get_native_schema_ddl(
        self,
        driver: "AsyncDriverAdapterBase",
        schema: str | None,
        *,
        include_dependencies: bool,
        redact: bool,
    ) -> list[DDLResult] | None:
        _ = include_dependencies, redact
        rows = await driver.select(
            self._get_domain_query_text("ddl", "by_schema"),
            schema_name=self.resolve_schema(schema),
        )
        return self._link_schema_ddl(rows, self._references_identifier)
