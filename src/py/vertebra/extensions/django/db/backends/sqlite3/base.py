"""Django SQLite3 DatabaseWrapper over vertebra.db.dbapi with WAL pragmas and LeaseBroker."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra.db.adapters.sqlite import SQLiteConfig
from vertebra.db.dbapi import SyncDBAPIConnection, SyncDBAPIModule
from vertebra.extensions.django.db.backends import DatabaseWrapperMixin
from vertebra.utils.module_loader import ensure_module, import_optional_attr

if TYPE_CHECKING:
    from collections.abc import Mapping

    from django.db.backends.sqlite3.base import (
        DatabaseWrapper as DjangoSQLiteDatabaseWrapper,
    )
else:
    DjangoSQLiteDatabaseWrapper = ensure_module(
        "django.db.backends.sqlite3.base", package="django", extra="django"
    ).DatabaseWrapper

__all__ = ("DatabaseWrapper",)


def __getattr__(name: str) -> Any:
    """Lazily resolve optional Django SQLite3 backend classes on demand."""
    if name == "DjangoSQLiteDatabaseWrapper":
        return ensure_module("django.db.backends.sqlite3.base", package="django", extra="django").DatabaseWrapper
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


class DatabaseWrapper(DatabaseWrapperMixin, DjangoSQLiteDatabaseWrapper):
    """Django SQLite3 database wrapper backed by Vertebra native driver and LeaseBroker."""

    vendor: str = "sqlite"
    display_name: str = "SQLite (Vertebra)"
    Database: Any = SyncDBAPIModule(paramstyle="qmark", dialect="sqlite")

    def __init__(self, settings_dict: dict[str, Any], alias: str = "default") -> None:
        """Initialize SQLite DatabaseWrapper with settings dictionary and alias."""
        DjangoSQLiteDatabaseWrapper.__init__(self, settings_dict, alias=alias)
        DatabaseWrapperMixin.__init__(self, settings_dict, alias=alias)

    def get_connection_params(self) -> dict[str, Any]:
        """Build SQLiteConfig locally from Django DATABASES settings."""
        db_path = str(self.settings_dict.get("NAME") or ":memory:")
        init_command = str(dict(self.settings_dict.get("OPTIONS") or {}).get("init_command", ""))
        self.init_commands = [c.strip() for c in init_command.split(";") if c.strip()]
        cfg = SQLiteConfig(
            path=db_path,
            database=db_path,
            broker=self._broker if self._share_pool else None,
        )
        return {"config": cfg}

    def get_new_connection(self, conn_params: Mapping[str, Any]) -> SyncDBAPIConnection:
        """Open a native SyncDBAPIConnection using SQLiteConfig with registered functions and pragmas."""
        cfg = conn_params["config"]
        conn = SyncDBAPIConnection(cfg)
        conn.autocommit = True
        register_functions = import_optional_attr("django.db.backends.sqlite3._functions", "register")
        if callable(register_functions):
            register_functions(conn)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA legacy_alter_table = OFF")
        for init_command in self.init_commands:
            if cmd := init_command.strip():
                conn.execute(cmd)
        return conn

    def create_cursor(self, name: str | None = None) -> Any:
        """Create and return a DBAPI cursor from the active connection."""
        _ = name
        return self.connection.cursor()

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
