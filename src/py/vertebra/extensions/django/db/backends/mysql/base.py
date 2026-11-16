"""Django MySQL DatabaseWrapper over vertebra.db.dbapi with CloudSQLDialer and LeaseBroker."""

from __future__ import annotations

import sys
import types
from typing import TYPE_CHECKING, Any, Self

from vertebra.db.adapters.mysql import MySQLConfig
from vertebra.db.dbapi import SyncDBAPIConnection, SyncDBAPIModule
from vertebra.extensions.django.db.backends import DatabaseWrapperMixin


def _ensure_mysqldb() -> None:
    """Inject a minimal MySQLdb shim into sys.modules if mysqlclient is not installed."""
    if "MySQLdb" in sys.modules:
        return
    m: Any = SyncDBAPIModule(paramstyle="format")
    m.version_info = (2, 2, 2)
    m.__version__ = "2.2.2"
    constants_mod: Any = types.ModuleType("MySQLdb.constants")
    client_mod: Any = types.ModuleType("MySQLdb.constants.CLIENT")
    client_mod.MULTI_STATEMENTS = 0
    client_mod.MULTI_RESULTS = 0
    field_type_mod: Any = types.ModuleType("MySQLdb.constants.FIELD_TYPE")
    for name in (
        "BLOB",
        "CHAR",
        "DECIMAL",
        "NEWDECIMAL",
        "DATE",
        "DATETIME",
        "DOUBLE",
        "FLOAT",
        "INT24",
        "JSON",
        "LONG",
        "LONGLONG",
        "SHORT",
        "STRING",
        "TIME",
        "TIMESTAMP",
        "TINY",
        "TINY_BLOB",
        "MEDIUM_BLOB",
        "LONG_BLOB",
        "VAR_STRING",
    ):
        setattr(field_type_mod, name, object())
    constants_mod.CLIENT = client_mod
    constants_mod.FIELD_TYPE = field_type_mod
    converters_mod: Any = types.ModuleType("MySQLdb.converters")
    converters_mod.conversions = {}
    sys.modules["MySQLdb"] = m
    sys.modules["MySQLdb.constants"] = constants_mod
    sys.modules["MySQLdb.constants.CLIENT"] = client_mod
    sys.modules["MySQLdb.constants.FIELD_TYPE"] = field_type_mod
    sys.modules["MySQLdb.converters"] = converters_mod


_ensure_mysqldb()

from django.db.backends.mysql.base import (  # noqa: E402
    DatabaseWrapper as DjangoMySQLDatabaseWrapper,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from vertebra.db.dbapi import SyncDBAPICursor

__all__ = ("DatabaseWrapper",)


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

    def __init__(self, cursor: SyncDBAPICursor) -> None:
        """Initialize cursor wrapper with underlying SyncDBAPICursor."""
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


class DatabaseWrapper(DatabaseWrapperMixin, DjangoMySQLDatabaseWrapper):
    """Django MySQL database wrapper backed by Vertebra native driver and LeaseBroker."""

    vendor: str = "mysql"
    display_name: str = "MySQL (Vertebra)"
    Database: Any = SyncDBAPIModule(paramstyle="format")

    def __init__(self, settings_dict: dict[str, Any], alias: str = "default") -> None:
        """Initialize MySQL DatabaseWrapper with settings dictionary and alias."""
        DjangoMySQLDatabaseWrapper.__init__(self, settings_dict, alias=alias)
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
        """Return MySQL/MariaDB server version tuple."""
        return (8, 0, 36)

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
