"""SQLite synchronous and asynchronous SQLSpec data dictionaries."""

from typing import Any, ClassVar, cast

from sqlspec.data_dictionary import (
    ColumnMetadata,
    DDLResult,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataResult,
    TableMetadata,
    VersionInfo,
    get_data_dictionary_loader,
    get_dialect_config,
)
from sqlspec.driver import AsyncDataDictionaryBase, SyncDataDictionaryBase
from sqlspec.exceptions import SQLFileNotFoundError

from vertebra.extensions.sqlspec.adapters.sqlite.core import (
    format_sqlite_schema_prefix,
    list_sqlite_available_features,
    normalize_sqlite_schema_name,
    resolve_sqlite_json_type,
    sqlite_check_constraints,
    sqlite_constraint_metadata,
    sqlite_generated_expressions,
    sqlite_index_rows_to_metadata,
    sqlite_object_ddl_result,
    sqlite_schema_ddl_results,
)

__all__ = ("SQLiteAsyncDataDictionary", "SQLiteSyncDataDictionary")


def _apply_sqlite_generated_expressions(
    rows: list[dict[str, Any]],
    table_sql: dict[str, str],
) -> list[ColumnMetadata]:
    """Attach parsed generated-column expressions to column metadata rows."""
    if any(str(row.get("extra") or "").endswith("generated") for row in rows):
        parsed_expressions: dict[str, dict[str, str]] = {}
        for row in rows:
            if str(row.get("extra") or "").endswith("generated"):
                table_name = str(row.get("table_name"))
                if table_name not in parsed_expressions:
                    parsed_expressions[table_name] = sqlite_generated_expressions(table_sql.get(table_name))
                expr = parsed_expressions[table_name].get(str(row.get("column_name")))
                if expr is not None:
                    row["generation_expression"] = expr
    return cast("list[ColumnMetadata]", rows)


def _domain_query_text(dialect: str, domain: str, query_name: str) -> str:
    """Resolve a required SQLite catalog query text or raise `SQLFileNotFoundError`."""
    query_text = get_data_dictionary_loader().get_domain_query_text(dialect, domain, query_name)
    if query_text is None:
        msg = f"Missing SQLite data-dictionary query: {domain}/{query_name}"
        raise SQLFileNotFoundError(msg)
    return query_text


class SQLiteSyncDataDictionary(SyncDataDictionaryBase):
    """SQLite-specific synchronous data dictionary backed by Vertebra `SQLiteSyncDriver`."""

    dialect: ClassVar[str] = "sqlite"

    def resolve_schema(self, schema: str | None) -> str | None:
        """Return a validated schema name defaulting to the SQLite dialect default (`'main'`)."""
        if schema is not None:
            return normalize_sqlite_schema_name(schema)
        return get_dialect_config(type(self).dialect).default_schema

    def get_version(self, driver: Any) -> VersionInfo | None:
        """Detect and cache SQLite engine version by executing `SELECT sqlite_version()`."""
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)

        version_value = driver.select_value_or_none(self.get_query("version", "current"))
        if not version_value:
            self._log_version_unavailable(type(self).dialect, "missing")
            self.cache_version(driver_id, None)
            return None

        config = get_dialect_config(type(self).dialect)
        version_info = self.parse_version_with_pattern(config.version_pattern, str(version_value))
        if version_info is None:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            self.cache_version(driver_id, None)
            return None

        self._log_version_detected(type(self).dialect, version_info)
        self.cache_version(driver_id, version_info)
        return version_info

    def get_feature_flag(self, driver: Any, feature: str) -> bool:
        """Return whether the detected SQLite engine version supports `feature`."""
        version_info = self.get_version(driver)
        return self.resolve_feature_flag(feature, version_info)

    def get_optimal_type(self, driver: Any, type_category: str) -> str:
        """Return the optimal SQLite column type for `type_category`."""
        config = get_dialect_config(type(self).dialect)
        version_info = self.get_version(driver)
        if type_category == "json":
            return resolve_sqlite_json_type(version_info)
        return config.get_optimal_type(type_category)

    def list_available_features(self) -> list[str]:
        """List supported SQLite dialect feature flags."""
        return list_sqlite_available_features()

    def get_tables(self, driver: Any, schema: str | None = None) -> list[TableMetadata]:
        """List SQLite tables in topological dependency order."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        query_text = _domain_query_text(type(self).dialect, "tables", "by_schema")
        return cast("list[TableMetadata]", driver.select(query_text, schema_name=schema_name))

    def get_columns(self, driver: Any, table: str | None = None, schema: str | None = None) -> list[ColumnMetadata]:
        """List column metadata for a table or entire SQLite schema."""
        schema_name = self.resolve_schema(schema)
        schema_prefix = format_sqlite_schema_prefix(schema_name)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            query_text = _domain_query_text(type(self).dialect, "columns", "by_schema").format(
                schema_prefix=schema_prefix
            )
            rows = driver.select(query_text, schema_name=schema_name)
        else:
            self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="columns")
            query_text = _domain_query_text(type(self).dialect, "columns", "by_table").format(
                schema_prefix=schema_prefix
            )
            rows = driver.select(query_text, table_name=table, schema_name=schema_name)
        table_sql = (
            self._get_table_sql(driver, schema_name, table)
            if any(str(row.get("extra") or "").endswith("generated") for row in rows)
            else {}
        )
        return _apply_sqlite_generated_expressions(rows, table_sql)

    def get_indexes(self, driver: Any, table: str | None = None, schema: str | None = None) -> list[IndexMetadata]:
        """List index metadata for a table or SQLite schema."""
        schema_name = self.resolve_schema(schema)
        schema_prefix = format_sqlite_schema_prefix(schema_name)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            query_text = _domain_query_text(type(self).dialect, "indexes", "by_schema").format(
                schema_prefix=schema_prefix
            )
            rows = driver.select(query_text, schema_name=schema_name)
            return sqlite_index_rows_to_metadata(rows, schema_name)
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="indexes")
        query_text = _domain_query_text(type(self).dialect, "indexes", "by_table").format(schema_prefix=schema_prefix)
        rows = driver.select(query_text, table_name=table, schema_name=schema_name)
        return sqlite_index_rows_to_metadata(rows, schema_name)

    def get_foreign_keys(
        self,
        driver: Any,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[ForeignKeyMetadata]:
        """List foreign-key relationships including referential actions."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            rows = driver.select(
                _domain_query_text(type(self).dialect, "foreign_keys", "by_schema"), schema_name=schema_name
            )
        else:
            self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
            rows = driver.select(
                _domain_query_text(type(self).dialect, "foreign_keys", "by_table"),
                table_name=table,
                schema_name=schema_name,
            )
        return [ForeignKeyMetadata.from_row(row) for row in rows]

    def get_constraints(
        self,
        driver: Any,
        table: str | None = None,
        schema: str | None = None,
    ) -> MetadataResult:
        """List PRIMARY KEY, UNIQUE, and parsed CHECK constraint metadata."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            rows = driver.select(
                _domain_query_text(type(self).dialect, "constraints", "by_schema"),
                schema_name=schema_name,
                table_name=None,
            )
        else:
            rows = driver.select(
                _domain_query_text(type(self).dialect, "constraints", "by_table"),
                schema_name=schema_name,
                table_name=table,
            )
        items: list[dict[str, Any]] = sqlite_constraint_metadata(rows)
        warnings: list[str] = []
        for table_name, create_sql in self._get_table_sql(driver, schema_name, table).items():
            checks, check_warnings = sqlite_check_constraints(table_name, create_sql, schema_name=schema_name)
            items.extend(checks)
            warnings.extend(check_warnings)
        return MetadataResult("constraints", items=tuple(items), warnings=tuple(warnings))

    def get_views(self, driver: Any, schema: str | None = None) -> MetadataResult:
        """List views and their stored CREATE VIEW DDL."""
        schema_name = self.resolve_schema(schema)
        schema_prefix = format_sqlite_schema_prefix(schema_name)
        query_text = _domain_query_text(type(self).dialect, "views", "by_schema").format(schema_prefix=schema_prefix)
        return MetadataResult("views", items=tuple(driver.select(query_text, schema_name=schema_name)))

    def get_sequences(self, driver: Any, schema: str | None = None) -> MetadataResult:
        """Return empty sequences metadata because SQLite does not support standalone sequences."""
        _ = (driver, schema)
        return MetadataResult("sequences", items=())

    def get_types(self, driver: Any, schema: str | None = None) -> MetadataResult:
        """Return empty custom type metadata because SQLite has no user-defined types."""
        _ = (driver, schema)
        return MetadataResult("types", items=())

    def _get_native_object_ddl(
        self,
        driver: Any,
        object_name: str,
        *,
        schema: str | None,
        object_type: str,
        include_dependencies: bool,
        redact: bool,
    ) -> DDLResult | None:
        _ = (include_dependencies, redact)
        schema_name = self.resolve_schema(schema)
        schema_prefix = format_sqlite_schema_prefix(schema_name)
        query_text = _domain_query_text(type(self).dialect, "ddl", "by_object").format(schema_prefix=schema_prefix)
        rows = driver.select(query_text, schema_name=schema_name, object_name=object_name, object_type=object_type)
        return sqlite_object_ddl_result(rows, object_name=object_name, object_type=object_type, schema=schema_name)

    def _get_native_schema_ddl(
        self,
        driver: Any,
        schema: str | None,
        *,
        include_dependencies: bool,
        redact: bool,
    ) -> list[DDLResult] | None:
        _ = (include_dependencies, redact)
        schema_name = self.resolve_schema(schema)
        schema_prefix = format_sqlite_schema_prefix(schema_name)
        query_text = _domain_query_text(type(self).dialect, "ddl", "by_schema").format(schema_prefix=schema_prefix)
        rows = driver.select(query_text, schema_name=schema_name)
        return sqlite_schema_ddl_results(rows, schema=schema_name)

    def _get_table_sql(self, driver: Any, schema_name: str | None, table: str | None) -> dict[str, str]:
        schema_prefix = format_sqlite_schema_prefix(schema_name)
        if table is None:
            query_text = _domain_query_text(type(self).dialect, "ddl", "by_schema").format(schema_prefix=schema_prefix)
            rows = driver.select(query_text, schema_name=schema_name)
        else:
            query_text = _domain_query_text(type(self).dialect, "ddl", "by_object").format(schema_prefix=schema_prefix)
            rows = driver.select(query_text, schema_name=schema_name, object_name=table, object_type="table")
        return {
            str(row["object_name"]): str(row["ddl"])
            for row in rows
            if str(row.get("object_type") or "").lower() == "table" and row.get("ddl") is not None
        }


class SQLiteAsyncDataDictionary(AsyncDataDictionaryBase):
    """SQLite-specific asynchronous data dictionary backed by Vertebra `SQLiteAsyncDriver`."""

    dialect: ClassVar[str] = "sqlite"

    def resolve_schema(self, schema: str | None) -> str | None:
        """Return a validated schema name defaulting to the SQLite dialect default (`'main'`)."""
        if schema is not None:
            return normalize_sqlite_schema_name(schema)
        return get_dialect_config(type(self).dialect).default_schema

    def list_available_features(self) -> list[str]:
        """List supported SQLite dialect feature flags."""
        return list_sqlite_available_features()

    async def get_version(self, driver: Any) -> VersionInfo | None:
        """Asynchronously detect and cache SQLite engine version."""
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)

        version_value = await driver.select_value_or_none(self.get_query("version", "current"))
        if not version_value:
            self._log_version_unavailable(type(self).dialect, "missing")
            self.cache_version(driver_id, None)
            return None

        config = get_dialect_config(type(self).dialect)
        version_info = self.parse_version_with_pattern(config.version_pattern, str(version_value))
        if version_info is None:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            self.cache_version(driver_id, None)
            return None

        self._log_version_detected(type(self).dialect, version_info)
        self.cache_version(driver_id, version_info)
        return version_info

    async def get_feature_flag(self, driver: Any, feature: str) -> bool:
        """Return asynchronously whether the SQLite engine supports `feature`."""
        version_info = await self.get_version(driver)
        return self.resolve_feature_flag(feature, version_info)

    async def get_optimal_type(self, driver: Any, type_category: str) -> str:
        """Return asynchronously the optimal SQLite column type for `type_category`."""
        config = get_dialect_config(type(self).dialect)
        version_info = await self.get_version(driver)
        if type_category == "json":
            return resolve_sqlite_json_type(version_info)
        return config.get_optimal_type(type_category)

    async def get_tables(self, driver: Any, schema: str | None = None) -> list[TableMetadata]:
        """List SQLite tables in topological dependency order asynchronously."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        query_text = _domain_query_text(type(self).dialect, "tables", "by_schema")
        return cast("list[TableMetadata]", await driver.select(query_text, schema_name=schema_name))

    async def get_columns(
        self,
        driver: Any,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[ColumnMetadata]:
        """List column metadata for a table or schema asynchronously."""
        schema_name = self.resolve_schema(schema)
        schema_prefix = format_sqlite_schema_prefix(schema_name)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            query_text = _domain_query_text(type(self).dialect, "columns", "by_schema").format(
                schema_prefix=schema_prefix
            )
            rows = await driver.select(query_text, schema_name=schema_name)
        else:
            self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="columns")
            query_text = _domain_query_text(type(self).dialect, "columns", "by_table").format(
                schema_prefix=schema_prefix
            )
            rows = await driver.select(query_text, table_name=table, schema_name=schema_name)
        table_sql = (
            await self._get_table_sql(driver, schema_name, table)
            if any(str(row.get("extra") or "").endswith("generated") for row in rows)
            else {}
        )
        return _apply_sqlite_generated_expressions(rows, table_sql)

    async def get_indexes(
        self,
        driver: Any,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[IndexMetadata]:
        """List index metadata for a table or schema asynchronously."""
        schema_name = self.resolve_schema(schema)
        schema_prefix = format_sqlite_schema_prefix(schema_name)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            query_text = _domain_query_text(type(self).dialect, "indexes", "by_schema").format(
                schema_prefix=schema_prefix
            )
            rows = await driver.select(query_text, schema_name=schema_name)
            return sqlite_index_rows_to_metadata(rows, schema_name)
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="indexes")
        query_text = _domain_query_text(type(self).dialect, "indexes", "by_table").format(schema_prefix=schema_prefix)
        rows = await driver.select(query_text, table_name=table, schema_name=schema_name)
        return sqlite_index_rows_to_metadata(rows, schema_name)

    async def get_foreign_keys(
        self,
        driver: Any,
        table: str | None = None,
        schema: str | None = None,
    ) -> list[ForeignKeyMetadata]:
        """List foreign-key relationships asynchronously."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            rows = await driver.select(
                _domain_query_text(type(self).dialect, "foreign_keys", "by_schema"),
                schema_name=schema_name,
            )
        else:
            self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
            rows = await driver.select(
                _domain_query_text(type(self).dialect, "foreign_keys", "by_table"),
                table_name=table,
                schema_name=schema_name,
            )
        return [ForeignKeyMetadata.from_row(row) for row in rows]

    async def get_constraints(
        self,
        driver: Any,
        table: str | None = None,
        schema: str | None = None,
    ) -> MetadataResult:
        """List PRIMARY KEY, UNIQUE, and CHECK constraint metadata asynchronously."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            rows = await driver.select(
                _domain_query_text(type(self).dialect, "constraints", "by_schema"),
                schema_name=schema_name,
                table_name=None,
            )
        else:
            rows = await driver.select(
                _domain_query_text(type(self).dialect, "constraints", "by_table"),
                schema_name=schema_name,
                table_name=table,
            )
        items: list[dict[str, Any]] = sqlite_constraint_metadata(rows)
        warnings: list[str] = []
        table_sql = await self._get_table_sql(driver, schema_name, table)
        for table_name, create_sql in table_sql.items():
            checks, check_warnings = sqlite_check_constraints(table_name, create_sql, schema_name=schema_name)
            items.extend(checks)
            warnings.extend(check_warnings)
        return MetadataResult("constraints", items=tuple(items), warnings=tuple(warnings))

    async def get_views(self, driver: Any, schema: str | None = None) -> MetadataResult:
        """List view definitions asynchronously."""
        schema_name = self.resolve_schema(schema)
        schema_prefix = format_sqlite_schema_prefix(schema_name)
        query_text = _domain_query_text(type(self).dialect, "views", "by_schema").format(schema_prefix=schema_prefix)
        return MetadataResult("views", items=tuple(await driver.select(query_text, schema_name=schema_name)))

    async def get_sequences(self, driver: Any, schema: str | None = None) -> MetadataResult:
        """Return empty sequence metadata asynchronously."""
        _ = (driver, schema)
        return MetadataResult("sequences", items=())

    async def get_types(self, driver: Any, schema: str | None = None) -> MetadataResult:
        """Return empty custom type metadata asynchronously."""
        _ = (driver, schema)
        return MetadataResult("types", items=())

    async def _get_native_object_ddl(
        self,
        driver: Any,
        object_name: str,
        *,
        schema: str | None,
        object_type: str,
        include_dependencies: bool,
        redact: bool,
    ) -> DDLResult | None:
        _ = (include_dependencies, redact)
        schema_name = self.resolve_schema(schema)
        schema_prefix = format_sqlite_schema_prefix(schema_name)
        query_text = _domain_query_text(type(self).dialect, "ddl", "by_object").format(schema_prefix=schema_prefix)
        rows = await driver.select(
            query_text,
            schema_name=schema_name,
            object_name=object_name,
            object_type=object_type,
        )
        return sqlite_object_ddl_result(rows, object_name=object_name, object_type=object_type, schema=schema_name)

    async def _get_native_schema_ddl(
        self,
        driver: Any,
        schema: str | None,
        *,
        include_dependencies: bool,
        redact: bool,
    ) -> list[DDLResult] | None:
        _ = (include_dependencies, redact)
        schema_name = self.resolve_schema(schema)
        schema_prefix = format_sqlite_schema_prefix(schema_name)
        query_text = _domain_query_text(type(self).dialect, "ddl", "by_schema").format(schema_prefix=schema_prefix)
        rows = await driver.select(query_text, schema_name=schema_name)
        return sqlite_schema_ddl_results(rows, schema=schema_name)

    async def _get_table_sql(self, driver: Any, schema_name: str | None, table: str | None) -> dict[str, str]:
        schema_prefix = format_sqlite_schema_prefix(schema_name)
        if table is None:
            query_text = _domain_query_text(type(self).dialect, "ddl", "by_schema").format(schema_prefix=schema_prefix)
            rows = await driver.select(query_text, schema_name=schema_name)
        else:
            query_text = _domain_query_text(type(self).dialect, "ddl", "by_object").format(schema_prefix=schema_prefix)
            rows = await driver.select(query_text, schema_name=schema_name, object_name=table, object_type="table")
        return {
            str(row["object_name"]): str(row["ddl"])
            for row in rows
            if str(row.get("object_type") or "").lower() == "table" and row.get("ddl") is not None
        }
