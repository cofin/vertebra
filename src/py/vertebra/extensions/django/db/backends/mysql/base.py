"""Django MySQL DatabaseWrapper over vertebra.db.dbapi with CloudSQLDialer and LeaseBroker."""

from __future__ import annotations

import re
from functools import cached_property
from typing import TYPE_CHECKING, Any, Self

from vertebra.db.adapters.mysql import MySQLConfig
from vertebra.db.dbapi import SyncDBAPIConnection, SyncDBAPIModule
from vertebra.extensions.django.db.backends import DatabaseWrapperMixin
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from collections.abc import Mapping

    from django.db.backends.base.base import BaseDatabaseWrapper
    from django.db.backends.base.introspection import BaseDatabaseIntrospection, TableInfo
    from django.db.backends.mysql.client import DatabaseClient as MySQLDatabaseClient
    from django.db.backends.mysql.creation import DatabaseCreation as MySQLDatabaseCreation
    from django.db.backends.mysql.features import DatabaseFeatures as MySQLDatabaseFeatures
    from django.db.backends.mysql.operations import DatabaseOperations as MySQLDatabaseOperations
    from django.db.backends.mysql.schema import DatabaseSchemaEditor as MySQLDatabaseSchemaEditor
    from django.db.backends.mysql.validation import DatabaseValidation as MySQLDatabaseValidation
else:
    BaseDatabaseWrapper = ensure_module(
        "django.db.backends.base.base", package="django", extra="django"
    ).BaseDatabaseWrapper
    _introspection_mod = ensure_module("django.db.backends.base.introspection", package="django", extra="django")
    BaseDatabaseIntrospection = _introspection_mod.BaseDatabaseIntrospection
    TableInfo = _introspection_mod.TableInfo
    MySQLDatabaseClient = ensure_module(
        "django.db.backends.mysql.client", package="django", extra="django"
    ).DatabaseClient
    MySQLDatabaseCreation = ensure_module(
        "django.db.backends.mysql.creation", package="django", extra="django"
    ).DatabaseCreation
    MySQLDatabaseFeatures = ensure_module(
        "django.db.backends.mysql.features", package="django", extra="django"
    ).DatabaseFeatures
    MySQLDatabaseOperations = ensure_module(
        "django.db.backends.mysql.operations", package="django", extra="django"
    ).DatabaseOperations
    MySQLDatabaseSchemaEditor = ensure_module(
        "django.db.backends.mysql.schema", package="django", extra="django"
    ).DatabaseSchemaEditor
    MySQLDatabaseValidation = ensure_module(
        "django.db.backends.mysql.validation", package="django", extra="django"
    ).DatabaseValidation

__all__ = ("DatabaseWrapper",)

_SERVER_VERSION_RE = re.compile(r"(\d+)\.(\d+)\.(\d+)")


class _MySQLIntrospection(BaseDatabaseIntrospection):
    """Driver-free MySQL schema introspection over SyncDBAPI cursors."""

    data_types_reverse: dict[Any, str] = {}

    def get_table_list(self, cursor: Any) -> list[TableInfo]:
        """Return a list of table and view entries in the active MySQL schema."""
        cursor.execute("SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = DATABASE()")
        type_map = {"BASE TABLE": "t", "VIEW": "v"}
        return [TableInfo(row[0], type_map.get(row[1], "t")) for row in cursor.fetchall()]

    def get_storage_engine(self, cursor: Any, table_name: str) -> str:
        """Return the storage engine name for ``table_name``."""
        cursor.execute(
            "SELECT engine FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = %s",
            [table_name],
        )
        row = cursor.fetchone()
        return str(row[0]) if row and row[0] else "InnoDB"


def _format_to_qmark(sql: str) -> str:
    """Translate unquoted %s placeholders to ? placeholders for vertebra._mysql."""
    out: list[str] = []
    length = len(sql)
    idx = 0
    while idx < length:
        ch = sql[idx]
        if ch in ("'", '"', "`"):
            quote = ch
            out.append(quote)
            idx += 1
            while idx < length:
                c = sql[idx]
                out.append(c)
                if c == "\\" and idx + 1 < length:
                    idx += 1
                    out.append(sql[idx])
                elif c == quote:
                    if idx + 1 < length and sql[idx + 1] == quote:
                        idx += 1
                        out.append(quote)
                    else:
                        break
                idx += 1
            idx += 1
            continue
        if ch == "%" and idx + 1 < length and sql[idx + 1] == "s":
            out.append("?")
            idx += 2
            continue
        out.append(ch)
        idx += 1
    return "".join(out)


class MySQLCursorWrapper:
    """Cursor wrapper translating Django %s placeholders to ? outside string literals."""

    def __init__(self, cursor: Any) -> None:
        """Initialize cursor wrapper with underlying DBAPI cursor."""
        self.cursor = cursor

    def execute(self, query: str, args: Any = None) -> Any:
        """Execute query with translated placeholders."""
        translated = _format_to_qmark(query)
        return self.cursor.execute(translated, args or ())

    def executemany(self, query: str, args: Any) -> Any:
        """Execute multiple parameter sets with translated placeholders."""
        translated = _format_to_qmark(query)
        return self.cursor.executemany(translated, args)

    def __getattr__(self, name: str) -> Any:
        """Delegate attribute lookup to underlying cursor."""
        return getattr(self.cursor, name)

    def __iter__(self) -> Any:
        """Iterate over result rows."""
        return iter(self.cursor)

    def __enter__(self) -> Self:
        """Enter cursor context."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit cursor context and close cursor."""
        self.cursor.close()


class DatabaseWrapper(DatabaseWrapperMixin, BaseDatabaseWrapper):
    """Django MySQL database wrapper backed by Vertebra native driver and LeaseBroker."""

    vendor: str = "mysql"
    display_name: str = "MySQL (Vertebra)"
    Database: Any = SyncDBAPIModule(paramstyle="format")
    SchemaEditorClass = MySQLDatabaseSchemaEditor
    client_class = MySQLDatabaseClient
    creation_class = MySQLDatabaseCreation
    features_class = MySQLDatabaseFeatures
    introspection_class = _MySQLIntrospection
    ops_class = MySQLDatabaseOperations
    validation_class = MySQLDatabaseValidation

    data_types: dict[str, str] = {
        "AutoField": "integer AUTO_INCREMENT",
        "BigAutoField": "bigint AUTO_INCREMENT",
        "BinaryField": "longblob",
        "BooleanField": "bool",
        "CharField": "varchar(%(max_length)s)",
        "DateField": "date",
        "DateTimeField": "datetime(6)",
        "DecimalField": "numeric(%(max_digits)s, %(decimal_places)s)",
        "DurationField": "bigint",
        "FileField": "varchar(%(max_length)s)",
        "FilePathField": "varchar(%(max_length)s)",
        "FloatField": "double precision",
        "IntegerField": "integer",
        "BigIntegerField": "bigint",
        "IPAddressField": "char(15)",
        "GenericIPAddressField": "char(39)",
        "JSONField": "json",
        "PositiveBigIntegerField": "bigint UNSIGNED",
        "PositiveIntegerField": "integer UNSIGNED",
        "PositiveSmallIntegerField": "smallint UNSIGNED",
        "SlugField": "varchar(%(max_length)s)",
        "SmallAutoField": "smallint AUTO_INCREMENT",
        "SmallIntegerField": "smallint",
        "TextField": "longtext",
        "TimeField": "time(6)",
        "UUIDField": "char(32)",
    }

    _limited_data_types: tuple[str, ...] = (
        "tinyblob",
        "blob",
        "mediumblob",
        "longblob",
        "tinytext",
        "text",
        "mediumtext",
        "longtext",
        "json",
    )

    operators: dict[str, str] = {
        "exact": "= %s",
        "iexact": "LIKE %s",
        "contains": "LIKE BINARY %s",
        "icontains": "LIKE %s",
        "gt": "> %s",
        "gte": ">= %s",
        "lt": "< %s",
        "lte": "<= %s",
        "startswith": "LIKE BINARY %s",
        "endswith": "LIKE BINARY %s",
        "istartswith": "LIKE %s",
        "iendswith": "LIKE %s",
    }

    pattern_esc: str = r"REPLACE(REPLACE(REPLACE({}, '\\', '\\\\'), '%%', '\%%'), '_', '\_')"
    pattern_ops: dict[str, str] = {
        "contains": "LIKE BINARY CONCAT('%%', {}, '%%')",
        "icontains": "LIKE CONCAT('%%', {}, '%%')",
        "startswith": "LIKE BINARY CONCAT({}, '%%')",
        "istartswith": "LIKE CONCAT({}, '%%')",
        "endswith": "LIKE BINARY CONCAT('%%', {})",
        "iendswith": "LIKE CONCAT('%%', {})",
    }

    def __init__(self, settings_dict: dict[str, Any], alias: str = "default") -> None:
        """Initialize MySQL DatabaseWrapper with settings dictionary and alias."""
        BaseDatabaseWrapper.__init__(self, settings_dict, alias=alias)
        DatabaseWrapperMixin.__init__(self, settings_dict, alias=alias)

    def get_connection_params(self) -> dict[str, Any]:
        """Build MySQLConfig locally from Django DATABASES settings."""
        host = str(self.settings_dict.get("HOST") or "localhost")
        port = int(self.settings_dict.get("PORT") or 3306)
        name = str(self.settings_dict.get("NAME") or "")
        user = str(self.settings_dict.get("USER") or "")
        password = str(self.settings_dict.get("PASSWORD") or "")
        auth = f"{user}:{password}@" if (user and password) else (f"{user}@" if user else "")
        dsn = f"mysql://{auth}{host}:{port}/{name}"

        cfg = MySQLConfig(
            dsn=dsn,
            cloud_sql_instance=self.pooler_config.cloud_sql_instance,
            broker=self._broker if self._share_pool else None,
        )
        return {"config": cfg}

    def get_new_connection(self, conn_params: Mapping[str, Any]) -> SyncDBAPIConnection:
        """Open a native SyncDBAPIConnection using MySQLConfig."""
        cfg = conn_params["config"]
        conn = SyncDBAPIConnection(cfg)
        conn.autocommit = True
        return conn

    def create_cursor(self, name: str | None = None) -> Any:
        """Create and return a DBAPI cursor translating %s to ? placeholders."""
        _ = name
        return MySQLCursorWrapper(self.connection.cursor())

    def get_database_version(self) -> tuple[int, ...]:
        """Query the connected MySQL/MariaDB server version and parse ``(major, minor, patch)``."""
        with self.temporary_connection() as cursor:
            cursor.execute("SELECT VERSION()")
            row = cursor.fetchone()
        version_str = str(row[0]) if row and row[0] is not None else ""
        match = _SERVER_VERSION_RE.search(version_str)
        if match is None:
            msg = f"Unable to determine MySQL version from version string {version_str!r}"
            raise ValueError(msg)
        return tuple(int(part) for part in match.groups())

    @cached_property
    def mysql_server_data(self) -> dict[str, Any]:
        """Fetch MySQL server session variables used by Django feature flags."""
        with self.temporary_connection() as cursor:
            cursor.execute(
                "SELECT VERSION(), @@sql_mode, @@default_storage_engine, "
                "@@sql_auto_is_null, @@lower_case_table_names, "
                "CONVERT_TZ('2001-01-01 01:00:00', 'UTC', 'UTC') IS NOT NULL"
            )
            row = cursor.fetchone()
        if not row:
            return {
                "version": "",
                "sql_mode": "",
                "default_storage_engine": "InnoDB",
                "sql_auto_is_null": False,
                "lower_case_table_names": False,
                "has_zoneinfo_database": False,
            }
        return {
            "version": row[0],
            "sql_mode": row[1],
            "default_storage_engine": row[2],
            "sql_auto_is_null": bool(row[3]),
            "lower_case_table_names": bool(row[4]),
            "has_zoneinfo_database": bool(row[5]),
        }

    @cached_property
    def mysql_server_info(self) -> str:
        """Return the raw server version string from ``mysql_server_data``."""
        return str(self.mysql_server_data["version"])

    @cached_property
    def mysql_version(self) -> tuple[int, ...]:
        """Return the parsed ``(major, minor, patch)`` server version tuple."""
        return self.get_database_version()

    @cached_property
    def mysql_is_mariadb(self) -> bool:
        """Return True when the connected server reports MariaDB."""
        return "mariadb" in self.mysql_server_info.lower()

    @cached_property
    def sql_mode(self) -> set[str]:
        """Return active SQL modes reported by the MySQL server."""
        raw_mode = self.mysql_server_data["sql_mode"]
        return set(str(raw_mode).split(",") if raw_mode else ())

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
