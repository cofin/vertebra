"""SQL Server (MSSQL) synchronous and asynchronous data dictionaries."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, cast

from sqlspec.data_dictionary import (
    ColumnMetadata,
    DDLResult,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataCapabilityProfile,
    MetadataResult,
    MetadataSupport,
    SystemMetadataCapability,
    SystemMetadataRequest,
    SystemMetadataResult,
    TableMetadata,
    ensure_system_metadata_request,
    system_metadata_gated_result,
)
from sqlspec.data_dictionary.dialects.mssql import (
    MssqlVersionInfo,
    build_mssql_metadata_capability_profile,
    build_mssql_system_metadata_capability,
    build_mssql_system_metadata_result,
    build_mssql_table_ddl_result,
    extract_mssql_version_value,
    get_mssql_data_dictionary_options,
    list_mssql_available_features,
    merge_mssql_table_lists,
    parse_mssql_engine_edition,
    parse_mssql_version_components,
    resolve_mssql_feature_flag,
    validate_mssql_system_metadata_options,
)
from sqlspec.data_dictionary.dialects.mssql.dictionary import (
    MssqlSyncDataDictionary as _UpstreamMssqlSyncDataDictionary,
)
from sqlspec.driver import AsyncDataDictionaryBase
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.data_dictionary import DialectConfig
    from sqlspec.driver import AsyncDriverAdapterBase

__all__ = (
    "MSSQLAsyncDataDictionary",
    "MSSQLSyncDataDictionary",
    "MssqlVersionInfo",
)

logger = get_logger("vertebra.extensions.sqlspec.adapters.mssql.data_dictionary")


def _extract_row_field(row: object, *names: str) -> Any:
    if isinstance(row, Mapping):
        for name in names:
            if name in row:
                return row[name]
            upper_name = name.upper()
            if upper_name in row:
                return row[upper_name]
            lower_name = name.lower()
            for k, v in row.items():
                if str(k).lower() == lower_name:
                    return v
        return None
    for name in names:
        if hasattr(row, name):
            return getattr(row, name)
    return None


class _MSSQLDataDictionaryMixin:
    """Shared T-SQL catalog helpers, version parsing, and capability configuration."""

    __slots__ = ()

    dialect: ClassVar[str] = "mssql"

    if TYPE_CHECKING:

        def get_dialect_config(self) -> DialectConfig: ...

    def list_available_features(self) -> list[str]:
        """List static and dynamic feature flags supported by SQL Server."""
        return list_mssql_available_features(self.get_dialect_config())

    def _build_version_info(
        self,
        version_value: str | None,
        edition: str | None,
        engine_edition_value: Any,
    ) -> MssqlVersionInfo | None:
        if not version_value:
            return None
        major, minor, build, revision = parse_mssql_version_components(version_value)
        return MssqlVersionInfo(
            major,
            minor,
            build,
            revision,
            edition=edition,
            engine_edition=parse_mssql_engine_edition(engine_edition_value),
        )

    def _get_optimal_type_from_version(self, version_info: MssqlVersionInfo | None, type_category: str) -> str:
        if type_category in {"json", "jsonb"} and version_info is not None and version_info.supports_native_json():
            return "JSON"
        return self.get_dialect_config().get_optimal_type(type_category)


class MSSQLSyncDataDictionary(_UpstreamMssqlSyncDataDictionary):
    """Synchronous SQL Server data dictionary backed by SQLSpec's shared dialect base."""

    __slots__ = ()

    dialect: ClassVar[str] = "mssql"


class MSSQLAsyncDataDictionary(_MSSQLDataDictionaryMixin, AsyncDataDictionaryBase):
    """Asynchronous SQL Server data dictionary executing bundled SQLSpec `mssql/sql/*.sql` catalog queries."""

    __slots__ = ()

    dialect: ClassVar[str] = "mssql"

    async def resolve_connection_schema(self, driver: AsyncDriverAdapterBase, schema: str | None) -> str | None:
        """Resolve explicit schema name or asynchronously query SQL Server's `SCHEMA_NAME()`."""
        if schema is not None:
            return schema
        current_schema = await driver.select_value_or_none(self.get_query("schemas", "current"))
        return str(current_schema) if current_schema else self.get_dialect_config().default_schema

    async def get_metadata_capabilities(
        self,
        driver: AsyncDriverAdapterBase,
        domains: Sequence[str] | None = None,
    ) -> MetadataCapabilityProfile:
        """Return SQL Server data dictionary metadata capability disclosures asynchronously."""
        _ = driver
        return build_mssql_metadata_capability_profile(type(self).__name__, domains)

    async def get_system_metadata_capabilities(
        self,
        driver: AsyncDriverAdapterBase,
        domains: Sequence[str] | None = None,
    ) -> tuple[SystemMetadataCapability, ...]:
        """Return SQL Server opt-in DMV and Query Store system metadata disclosures asynchronously."""
        _ = driver
        requested_domains = ("dmv_exec_requests", "query_store_runtime") if domains is None else tuple(domains)
        return tuple(build_mssql_system_metadata_capability(domain) for domain in requested_domains)

    async def get_version(self, driver: AsyncDriverAdapterBase) -> MssqlVersionInfo | None:
        """Asynchronously query and cache SQL Server version and edition metadata for `driver`."""
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return cast("MssqlVersionInfo | None", self._version_cache.get(driver_id))

        row = await driver.select_one_or_none(self.get_query("version", "current"))
        if not row:
            self._log_version_unavailable(type(self).dialect, "missing")
            self.cache_version(driver_id, None)
            return None

        version_value = extract_mssql_version_value(
            _extract_row_field(row, "product_version") or _extract_row_field(row, "version_string", "version")
        )
        edition_raw = _extract_row_field(row, "edition")
        edition = str(edition_raw) if edition_raw is not None else None
        version_info = self._build_version_info(version_value, edition, _extract_row_field(row, "engine_edition"))
        if version_info is None:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            self.cache_version(driver_id, None)
            return None

        self._log_version_detected(type(self).dialect, version_info)
        self.cache_version(driver_id, version_info)
        return version_info

    async def get_feature_flag(self, driver: AsyncDriverAdapterBase, feature: str) -> bool:
        """Evaluate asynchronously whether the connected SQL Server instance supports `feature`."""
        version_info = await self.get_version(driver)
        return resolve_mssql_feature_flag(
            feature,
            major=version_info.major if version_info is not None else 0,
            is_azure_sql=bool(version_info and version_info.is_azure_sql),
            config=self.get_dialect_config(),
            version_info=version_info,
        )

    async def get_optimal_type(self, driver: AsyncDriverAdapterBase, type_category: str) -> str:
        """Return the optimal T-SQL type name for `type_category` asynchronously."""
        return self._get_optimal_type_from_version(await self.get_version(driver), type_category)

    async def get_tables(self, driver: AsyncDriverAdapterBase, schema: str | None = None) -> list[TableMetadata]:
        """Asynchronously list SQL Server tables ordered topologically by foreign keys."""
        schema_name = await self.resolve_connection_schema(driver, schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        ordered = cast(
            "list[TableMetadata]",
            await driver.select(
                self.get_query("tables", "by_schema"),
                schema_name=schema_name,
                schema_type=TableMetadata,
            ),
        )
        all_rows = cast(
            "list[TableMetadata]",
            await driver.select(
                self.get_query("tables", "all_by_schema"),
                schema_name=schema_name,
                schema_type=TableMetadata,
            ),
        )
        return merge_mssql_table_lists(ordered, all_rows)

    async def get_columns(
        self,
        driver: AsyncDriverAdapterBase,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[ColumnMetadata]:
        """Asynchronously retrieve column metadata for a table or schema."""
        schema_name = await self.resolve_connection_schema(driver, schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            return cast(
                "list[ColumnMetadata]",
                await driver.select(
                    self.get_query("columns", "by_schema"),
                    schema_name=schema_name,
                    schema_type=ColumnMetadata,
                ),
            )
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="columns")
        return cast(
            "list[ColumnMetadata]",
            await driver.select(
                self.get_query("columns", "by_table"),
                schema_name=schema_name,
                table_name=table,
                schema_type=ColumnMetadata,
            ),
        )

    async def get_indexes(
        self,
        driver: AsyncDriverAdapterBase,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[IndexMetadata]:
        """Asynchronously retrieve index metadata for a table or schema."""
        schema_name = await self.resolve_connection_schema(driver, schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            return cast(
                "list[IndexMetadata]",
                await driver.select(
                    self.get_query("indexes", "by_schema"),
                    schema_name=schema_name,
                    schema_type=IndexMetadata,
                ),
            )
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="indexes")
        return cast(
            "list[IndexMetadata]",
            await driver.select(
                self.get_query("indexes", "by_table"),
                schema_name=schema_name,
                table_name=table,
                schema_type=IndexMetadata,
            ),
        )

    async def get_foreign_keys(
        self,
        driver: AsyncDriverAdapterBase,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[ForeignKeyMetadata]:
        """Asynchronously retrieve foreign key metadata for a table or schema."""
        schema_name = await self.resolve_connection_schema(driver, schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            return cast(
                "list[ForeignKeyMetadata]",
                await driver.select(
                    self.get_query("foreign_keys", "by_schema"),
                    schema_name=schema_name,
                    schema_type=ForeignKeyMetadata,
                ),
            )
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
        return cast(
            "list[ForeignKeyMetadata]",
            await driver.select(
                self.get_query("foreign_keys", "by_table"),
                schema_name=schema_name,
                table_name=table,
                schema_type=ForeignKeyMetadata,
            ),
        )

    async def get_constraints(
        self,
        driver: AsyncDriverAdapterBase,
        table: str | None = None,
        schema: str | None = None,
    ) -> MetadataResult:
        """Asynchronously return primary key, unique, check, and default constraint metadata."""
        schema_name = await self.resolve_connection_schema(driver, schema)
        capability = (await self.get_metadata_capabilities(driver, ("constraints",))).get("constraints")
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="constraints")
            rows = await driver.select(self.get_query("constraints", "by_schema"), schema_name=schema_name)
        else:
            self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="constraints")
            rows = await driver.select(
                self.get_query("constraints", "by_table"),
                schema_name=schema_name,
                table_name=table,
            )
        return MetadataResult("constraints", capability=capability, items=tuple(rows))

    async def get_schemas(self, driver: AsyncDriverAdapterBase) -> MetadataResult:
        """Asynchronously list accessible SQL Server database schemas."""
        capability = (await self.get_metadata_capabilities(driver, ("schemas",))).get("schemas")
        self._log_schema_introspect(driver, schema_name=None, table_name=None, operation="schemas")
        rows = await driver.select(self.get_query("schemas", "list"))
        return MetadataResult("schemas", capability=capability, items=tuple(rows))

    async def get_objects(self, driver: AsyncDriverAdapterBase, schema: str | None = None) -> MetadataResult:
        """Asynchronously list database objects in `schema`."""
        schema_name = await self.resolve_connection_schema(driver, schema)
        capability = (await self.get_metadata_capabilities(driver, ("objects",))).get("objects")
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="objects")
        rows = await driver.select(self.get_query("objects", "by_schema"), schema_name=schema_name)
        return MetadataResult("objects", capability=capability, items=tuple(rows))

    async def get_table_details(
        self,
        driver: AsyncDriverAdapterBase,
        table: str,
        schema: str | None = None,
    ) -> MetadataResult:
        """Asynchronously return detailed table properties and statistics."""
        schema_name = await self.resolve_connection_schema(driver, schema)
        capability = (await self.get_metadata_capabilities(driver, ("tables",))).get("tables")
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=table, operation="table_details")
        rows = await driver.select(
            self.get_query("tables", "details_by_table"),
            schema_name=schema_name,
            table_name=table,
        )
        return MetadataResult("tables", capability=capability, items=tuple(rows))

    async def get_routines(self, driver: AsyncDriverAdapterBase, schema: str | None = None) -> MetadataResult:
        """Asynchronously list stored procedures and user-defined functions in `schema`."""
        schema_name = await self.resolve_connection_schema(driver, schema)
        capability = (await self.get_metadata_capabilities(driver, ("routines",))).get("routines")
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="routines")
        rows = await driver.select(self.get_query("routines", "by_schema"), schema_name=schema_name)
        return MetadataResult("routines", capability=capability, items=tuple(rows))

    async def get_privileges(
        self,
        driver: AsyncDriverAdapterBase,
        object_name: str | None = None,
        schema: str | None = None,
    ) -> MetadataResult:
        """Asynchronously list schema or object-level permissions."""
        schema_name = await self.resolve_connection_schema(driver, schema)
        capability = (await self.get_metadata_capabilities(driver, ("privileges",))).get("privileges")
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=object_name, operation="privileges")
        rows = await driver.select(self.get_query("permissions", "by_schema"), schema_name=schema_name)
        if object_name is not None:
            rows = [r for r in rows if _extract_row_field(r, "object_name") == object_name]
        return MetadataResult("privileges", capability=capability, items=tuple(rows))

    async def get_views(self, driver: AsyncDriverAdapterBase, schema: str | None = None) -> MetadataResult:
        """Asynchronously list SQL Server views in `schema`."""
        schema_name = await self.resolve_connection_schema(driver, schema)
        capability = (await self.get_metadata_capabilities(driver, ("views",))).get("views")
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="views")
        rows = await driver.select(self.get_query("views", "by_schema"), schema_name=schema_name)
        return MetadataResult("views", capability=capability, items=tuple(rows))

    async def get_sequences(self, driver: AsyncDriverAdapterBase, schema: str | None = None) -> MetadataResult:
        """Asynchronously list SQL Server sequences in `schema`."""
        schema_name = await self.resolve_connection_schema(driver, schema)
        capability = (await self.get_metadata_capabilities(driver, ("sequences",))).get("sequences")
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="sequences")
        rows = await driver.select(self.get_query("sequences", "by_schema"), schema_name=schema_name)
        return MetadataResult("sequences", capability=capability, items=tuple(rows))

    async def get_dependencies(
        self,
        driver: AsyncDriverAdapterBase,
        object_name: str | None = None,
        schema: str | None = None,
    ) -> MetadataResult:
        """Asynchronously list SQL expression dependency relationships in `schema`."""
        schema_name = await self.resolve_connection_schema(driver, schema)
        capability = (await self.get_metadata_capabilities(driver, ("dependencies",))).get("dependencies")
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=object_name, operation="dependencies")
        rows = await driver.select(self.get_query("dependencies", "by_schema"), schema_name=schema_name)
        if object_name is not None:
            rows = [r for r in rows if _extract_row_field(r, "referencing_object_name") == object_name]
        return MetadataResult("dependencies", capability=capability, items=tuple(rows))

    async def get_system_metadata(
        self,
        driver: AsyncDriverAdapterBase,
        request: SystemMetadataRequest | str | None = None,
        **kwargs: Any,
    ) -> SystemMetadataResult:
        """Return redacted opt-in DMV/Query Store metadata asynchronously when enabled."""
        metadata_request = ensure_system_metadata_request(request, **kwargs)
        capability = build_mssql_system_metadata_capability(metadata_request.domain)
        gate_result = system_metadata_gated_result(metadata_request, capability)
        if gate_result.capability.support != MetadataSupport.SUPPORTED:
            return gate_result
        options = get_mssql_data_dictionary_options(driver)
        denied = validate_mssql_system_metadata_options(metadata_request, options)
        if denied is not None:
            return denied
        query = self.get_query("system", metadata_request.domain)
        rows = await driver.select(query)
        return build_mssql_system_metadata_result(metadata_request, rows)

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
        _ = include_dependencies, redact
        if object_type.lower() != "table":
            return None
        columns = await driver.select(
            self.get_query("ddl", "table_inputs_by_table"),
            schema_name=schema,
            table_name=object_name,
        )
        if not columns:
            return build_mssql_table_ddl_result(schema, object_name, columns, object_type=object_type)
        indexes = await driver.select(
            self.get_query("ddl", "index_inputs_by_table"),
            schema_name=schema,
            table_name=object_name,
        )
        constraints = await driver.select(
            self.get_query("constraints", "by_table"),
            schema_name=schema,
            table_name=object_name,
        )
        foreign_keys = await self.get_foreign_keys(driver, table=object_name, schema=schema)
        return build_mssql_table_ddl_result(
            schema,
            object_name,
            columns,
            indexes,
            constraint_rows=constraints,
            foreign_key_rows=foreign_keys,
            object_type=object_type,
        )
