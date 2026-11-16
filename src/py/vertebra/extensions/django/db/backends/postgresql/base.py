"""Django PostgreSQL DatabaseWrapper over vertebra.db.dbapi with Cloud Dialers and LeaseBroker."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from django.db.backends.postgresql.base import (
    DatabaseWrapper as DjangoPostgreSQLDatabaseWrapper,
)

from vertebra.db.adapters.postgres import PoolerMode, PostgreSQLConfig
from vertebra.db.dbapi import SyncDBAPIConnection, SyncDBAPIModule
from vertebra.extensions.django.db.backends import DatabaseWrapperMixin

if TYPE_CHECKING:
    from collections.abc import Mapping

    from vertebra.db.dbapi import SyncDBAPICursor

__all__ = ("DatabaseWrapper",)


class DatabaseWrapper(DatabaseWrapperMixin, DjangoPostgreSQLDatabaseWrapper):
    """Django PostgreSQL database wrapper backed by Vertebra native driver and LeaseBroker."""

    vendor: str = "postgresql"
    display_name: str = "PostgreSQL (Vertebra)"
    Database: Any = SyncDBAPIModule(paramstyle="pyformat")

    def __init__(self, settings_dict: dict[str, Any], alias: str = "default") -> None:
        """Initialize PostgreSQL DatabaseWrapper with settings dictionary and alias."""
        DjangoPostgreSQLDatabaseWrapper.__init__(self, settings_dict, alias=alias)
        DatabaseWrapperMixin.__init__(self, settings_dict, alias=alias)

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
