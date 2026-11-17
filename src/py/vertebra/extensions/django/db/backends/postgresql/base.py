"""Django PostgreSQL DatabaseWrapper over vertebra.db.dbapi with Cloud Dialers and LeaseBroker."""

from __future__ import annotations

import re
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast

from vertebra.db.adapters.postgres import PoolerMode, PostgreSQLConfig
from vertebra.db.dbapi import SyncDBAPIConnection, SyncDBAPIModule
from vertebra.extensions.django.db.backends import DatabaseWrapperMixin
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from collections.abc import Mapping

    from django.db.backends.base.base import BaseDatabaseWrapper
    from django.db.backends.base.creation import BaseDatabaseCreation
    from django.db.backends.base.features import BaseDatabaseFeatures
    from django.db.backends.base.introspection import BaseDatabaseIntrospection, TableInfo
    from django.db.backends.base.operations import BaseDatabaseOperations
    from django.db.backends.base.schema import BaseDatabaseSchemaEditor
    from django.db.backends.base.validation import BaseDatabaseValidation
    from django.db.backends.postgresql.client import DatabaseClient as PostgreSQLDatabaseClient

    from vertebra.db.dbapi import SyncDBAPICursor
else:
    BaseDatabaseWrapper = ensure_module(
        "django.db.backends.base.base", package="django", extra="django"
    ).BaseDatabaseWrapper
    BaseDatabaseCreation = ensure_module(
        "django.db.backends.base.creation", package="django", extra="django"
    ).BaseDatabaseCreation
    BaseDatabaseFeatures = ensure_module(
        "django.db.backends.base.features", package="django", extra="django"
    ).BaseDatabaseFeatures
    _introspection_mod = ensure_module("django.db.backends.base.introspection", package="django", extra="django")
    BaseDatabaseIntrospection = _introspection_mod.BaseDatabaseIntrospection
    TableInfo = _introspection_mod.TableInfo
    BaseDatabaseOperations = ensure_module(
        "django.db.backends.base.operations", package="django", extra="django"
    ).BaseDatabaseOperations
    BaseDatabaseSchemaEditor = ensure_module(
        "django.db.backends.base.schema", package="django", extra="django"
    ).BaseDatabaseSchemaEditor
    BaseDatabaseValidation = ensure_module(
        "django.db.backends.base.validation", package="django", extra="django"
    ).BaseDatabaseValidation
    PostgreSQLDatabaseClient = ensure_module(
        "django.db.backends.postgresql.client", package="django", extra="django"
    ).DatabaseClient

__all__ = ("DatabaseWrapper",)

_PG_VERSION_RE = re.compile(r"(\d+)\.(\d+)(?:\.(\d+))?")


def _get_varchar_column(data: Mapping[str, Any]) -> str:
    if data.get("max_length") is None:
        return "varchar"
    return f"varchar({data['max_length']})"


def _get_decimal_column(data: Mapping[str, Any]) -> str:
    if data.get("max_digits") is None and data.get("decimal_places") is None:
        return "numeric"
    return f"numeric({data['max_digits']}, {data['decimal_places']})"


class _PostgreSQLFeatures(BaseDatabaseFeatures):
    """Driver-free PostgreSQL database capability flags."""

    minimum_database_version: tuple[int, ...] = (14,)
    allows_group_by_selected_pks: bool = True
    can_return_columns_from_insert: bool = True
    can_return_rows_from_bulk_insert: bool = True
    has_real_datatype: bool = True
    supports_transactions: bool = True
    has_native_uuid_field: bool = True
    has_native_duration_field: bool = True
    has_native_json_field: bool = True
    can_defer_constraint_checks: bool = True
    uses_savepoints: bool = True
    can_release_savepoints: bool = True
    supports_tablespaces: bool = True
    supports_paramstyle_pyformat: bool = True


class _PostgreSQLOperations(BaseDatabaseOperations):
    """Driver-free PostgreSQL SQL compilation operations."""

    compiler_module: str = "django.db.backends.postgresql.compiler"
    cast_char_field_without_max_length: str = "varchar"
    explain_prefix: str = "EXPLAIN"

    def max_name_length(self) -> int:
        """Return PostgreSQL NAMEDATALEN identifier limit (63 bytes)."""
        return 63

    def quote_name(self, name: str) -> str:
        """Double-quote a PostgreSQL identifier while escaping embedded quotes."""
        if name.startswith('"') and name.endswith('"'):
            return name
        escaped = name.replace('"', '""')
        return f'"{escaped}"'

    def no_limit_value(self) -> None:
        """Return None so unbounded PostgreSQL OFFSET queries omit LIMIT."""
        return None

    def deferrable_sql(self) -> str:
        """Return DEFERRABLE INITIALLY DEFERRED constraint suffix."""
        return " DEFERRABLE INITIALLY DEFERRED"

    def return_insert_columns(self, fields: Any) -> tuple[str, tuple[Any, ...]]:
        """Render RETURNING clause for PostgreSQL INSERT statements."""
        if not fields:
            return "", ()
        columns = [f"{self.quote_name(field.model._meta.db_table)}.{self.quote_name(field.column)}" for field in fields]
        return f"RETURNING {', '.join(columns)}", ()


class _PostgreSQLIntrospection(BaseDatabaseIntrospection):
    """Driver-free PostgreSQL schema introspection over SyncDBAPI cursors."""

    data_types_reverse: dict[Any, str] = {}

    def get_table_list(self, cursor: Any) -> list[TableInfo]:
        """Return tables, partitioned tables, views, and materialized views in the search path."""
        cursor.execute(
            "SELECT c.relname, "
            "CASE WHEN c.relkind IN ('m', 'v') THEN 'v' "
            "WHEN c.relkind = 'p' THEN 'p' ELSE 't' END "
            "FROM pg_catalog.pg_class c "
            "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relkind IN ('f', 'm', 'p', 'r', 'v') "
            "AND n.nspname NOT IN ('pg_catalog', 'pg_toast') "
            "AND pg_catalog.pg_table_is_visible(c.oid)"
        )
        return [TableInfo(row[0], row[1]) for row in cursor.fetchall()]


class _PostgreSQLSchemaEditor(BaseDatabaseSchemaEditor):
    """Driver-free PostgreSQL DDL schema editor."""

    sql_create_sequence: str = "CREATE SEQUENCE %(sequence)s"
    sql_delete_sequence: str = "DROP SEQUENCE IF EXISTS %(sequence)s CASCADE"
    sql_create_index: str = "CREATE INDEX %(name)s ON %(table)s%(using)s (%(columns)s)%(include)s%(extra)s%(condition)s"
    sql_create_index_concurrently: str = (
        "CREATE INDEX CONCURRENTLY %(name)s ON %(table)s%(using)s (%(columns)s)%(include)s%(extra)s%(condition)s"
    )
    sql_delete_index: str = "DROP INDEX IF EXISTS %(name)s"
    sql_delete_index_concurrently: str = "DROP INDEX CONCURRENTLY IF EXISTS %(name)s"


class DatabaseWrapper(DatabaseWrapperMixin, BaseDatabaseWrapper):
    """Django PostgreSQL database wrapper backed by Vertebra native driver and LeaseBroker."""

    vendor: str = "postgresql"
    display_name: str = "PostgreSQL (Vertebra)"
    Database: Any = SyncDBAPIModule(paramstyle="pyformat")
    SchemaEditorClass = _PostgreSQLSchemaEditor
    client_class = PostgreSQLDatabaseClient
    creation_class = BaseDatabaseCreation
    features_class = _PostgreSQLFeatures
    introspection_class = _PostgreSQLIntrospection
    ops_class = _PostgreSQLOperations
    validation_class = BaseDatabaseValidation

    data_types: dict[str, Any] = {
        "AutoField": "integer",
        "BigAutoField": "bigint",
        "BinaryField": "bytea",
        "BooleanField": "boolean",
        "CharField": _get_varchar_column,
        "DateField": "date",
        "DateTimeField": "timestamp with time zone",
        "DecimalField": _get_decimal_column,
        "DurationField": "interval",
        "FileField": "varchar(%(max_length)s)",
        "FilePathField": "varchar(%(max_length)s)",
        "FloatField": "double precision",
        "IntegerField": "integer",
        "BigIntegerField": "bigint",
        "IPAddressField": "inet",
        "GenericIPAddressField": "inet",
        "JSONField": "jsonb",
        "PositiveBigIntegerField": "bigint",
        "PositiveIntegerField": "integer",
        "PositiveSmallIntegerField": "smallint",
        "SlugField": "varchar(%(max_length)s)",
        "SmallAutoField": "smallint",
        "SmallIntegerField": "smallint",
        "TextField": "text",
        "TimeField": "time",
        "UUIDField": "uuid",
    }
    data_type_check_constraints: dict[str, str] = {
        "PositiveBigIntegerField": '"%(column)s" >= 0',
        "PositiveIntegerField": '"%(column)s" >= 0',
        "PositiveSmallIntegerField": '"%(column)s" >= 0',
    }
    data_types_suffix: dict[str, str] = {
        "AutoField": "GENERATED BY DEFAULT AS IDENTITY",
        "BigAutoField": "GENERATED BY DEFAULT AS IDENTITY",
        "SmallAutoField": "GENERATED BY DEFAULT AS IDENTITY",
    }
    operators: dict[str, str] = {
        "exact": "= %s",
        "iexact": "= UPPER(%s)",
        "contains": "LIKE %s",
        "icontains": "LIKE UPPER(%s)",
        "regex": "~ %s",
        "iregex": "~* %s",
        "gt": "> %s",
        "gte": ">= %s",
        "lt": "< %s",
        "lte": "<= %s",
        "startswith": "LIKE %s",
        "endswith": "LIKE %s",
        "istartswith": "LIKE UPPER(%s)",
        "iendswith": "LIKE UPPER(%s)",
    }
    pattern_esc: str = r"REPLACE(REPLACE(REPLACE({}, E'\\', E'\\\\'), E'%%', E'\\%%'), E'_', E'\\_')"
    pattern_ops: dict[str, str] = {
        "contains": "LIKE '%%' || {} || '%%'",
        "icontains": "LIKE '%%' || UPPER({}) || '%%'",
        "startswith": "LIKE {} || '%%'",
        "istartswith": "LIKE UPPER({}) || '%%'",
        "endswith": "LIKE '%%' || {}",
        "iendswith": "LIKE '%%' || UPPER({})",
    }

    def __init__(self, settings_dict: dict[str, Any], alias: str = "default") -> None:
        """Initialize PostgreSQL DatabaseWrapper with settings dictionary and alias."""
        BaseDatabaseWrapper.__init__(self, settings_dict, alias=alias)
        DatabaseWrapperMixin.__init__(self, settings_dict, alias=alias)

    @cached_property
    def pg_version(self) -> int:
        """Return PostgreSQL server_version_num integer (e.g. 160000)."""
        major, minor, *rest = self.get_database_version()
        patch = rest[0] if rest else 0
        return major * 10000 + (minor * 100 if major < 10 else minor) + (patch if major < 10 else 0)

    def get_database_version(self) -> tuple[int, ...]:
        """Query the connected PostgreSQL server version and return ``(major, minor)``."""
        with self.temporary_connection() as cursor:
            cursor.execute("SHOW server_version")
            row = cursor.fetchone()
        version_str = str(row[0]) if row and row[0] is not None else "16.0"
        match = _PG_VERSION_RE.search(version_str)
        if match is None:
            return (16, 0)
        parts = [int(g) for g in match.groups() if g is not None]
        return tuple(parts)

    def get_connection_params(self) -> dict[str, Any]:
        """Build PostgreSQLConfig locally from Django DATABASES settings."""
        host = str(self.settings_dict.get("HOST") or "localhost")
        port = int(self.settings_dict.get("PORT") or 5432)
        name = str(self.settings_dict.get("NAME") or "postgres")
        user = str(self.settings_dict.get("USER") or "")
        password = str(self.settings_dict.get("PASSWORD") or "")
        auth = f"{user}:{password}@" if (user and password) else (f"{user}@" if user else "")
        dsn = f"postgresql://{auth}{host}:{port}/{name}"

        cfg = PostgreSQLConfig(
            dsn=dsn,
            direct_dsn=self.pooler_config.direct_dsn,
            pooler_mode=cast("PoolerMode", self.pooler_config.pooler_mode),
            pgbouncer=self.pooler_config.pgbouncer,
            cloud_sql_instance=self.pooler_config.cloud_sql_instance,
            alloydb_instance_uri=self.pooler_config.alloydb_instance_uri,
            broker=self._broker if self._share_pool else None,
        )
        return {"config": cfg}

    def get_new_connection(self, conn_params: Mapping[str, Any]) -> SyncDBAPIConnection:
        """Open a native SyncDBAPIConnection using PostgreSQLConfig."""
        cfg = conn_params["config"]
        conn = SyncDBAPIConnection(cfg)
        conn.autocommit = True
        return conn

    def create_cursor(self, name: str | None = None) -> SyncDBAPICursor:
        """Create and return a DBAPI cursor from the active connection."""
        _ = name
        return cast("SyncDBAPICursor", self.connection.cursor())

    def _set_autocommit(self, autocommit: bool) -> None:
        """Set autocommit on the underlying connection."""
        with self.wrap_database_errors:
            self.connection.autocommit = autocommit

    def is_usable(self) -> bool:
        """Verify connection liveness with a ping."""
        if self.connection is None or getattr(self.connection, "closed", True):
            return False
        try:
            with self.connection.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        except Exception:
            return False
