"""Django Oracle DatabaseWrapper over vertebra.db.dbapi with mTLS Wallet and LeaseBroker."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any, cast

from vertebra.db.adapters.oracle import OracleConfig
from vertebra.db.dbapi import SyncDBAPIConnection, SyncDBAPIModule
from vertebra.extensions.django.db.backends import DatabaseWrapperMixin
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from collections.abc import Mapping

    from django.db.backends.oracle.base import (
        DatabaseWrapper as DjangoOracleDatabaseWrapper,
    )

    from vertebra.db.dbapi import SyncDBAPICursor

__all__ = ("DatabaseWrapper",)


def _ensure_oracledb() -> None:
    """Inject a minimal oracledb shim into sys.modules if oracledb is not installed."""
    if "oracledb" in sys.modules:
        return
    ora: Any = SyncDBAPIModule(paramstyle="named")
    ora.version = "2.0.0"
    ora.__version__ = "2.0.0"
    for attr in (
        "DB_TYPE_BINARY_DOUBLE",
        "DB_TYPE_BLOB",
        "DB_TYPE_BOOLEAN",
        "DB_TYPE_CHAR",
        "DB_TYPE_CLOB",
        "DB_TYPE_DATE",
        "DB_TYPE_INTERVAL_DS",
        "DB_TYPE_NCHAR",
        "DB_TYPE_NCLOB",
        "DB_TYPE_NUMBER",
        "DB_TYPE_NVARCHAR",
        "DB_TYPE_TIMESTAMP",
        "DB_TYPE_VARCHAR",
        "LOB",
        "NUMBER",
        "STRING",
    ):
        setattr(ora, attr, object())
    sys.modules["oracledb"] = ora


def _load_django_oracle_wrapper() -> type[Any]:
    """Ensure oracledb shim is registered and return Django's Oracle DatabaseWrapper base."""
    _ensure_oracledb()
    mod = ensure_module("django.db.backends.oracle.base", extra="django")
    wrapper_cls: type[Any] = mod.DatabaseWrapper
    return wrapper_cls


if not TYPE_CHECKING:
    DjangoOracleDatabaseWrapper = _load_django_oracle_wrapper()


class DatabaseWrapper(DatabaseWrapperMixin, DjangoOracleDatabaseWrapper):
    """Django Oracle database wrapper backed by Vertebra native driver and LeaseBroker."""

    vendor: str = "oracle"
    display_name: str = "Oracle (Vertebra)"
    Database: Any = SyncDBAPIModule(paramstyle="named")

    def __init__(self, settings_dict: dict[str, Any], alias: str = "default") -> None:
        """Initialize Oracle DatabaseWrapper with settings dictionary and alias."""
        DjangoOracleDatabaseWrapper.__init__(self, settings_dict, alias=alias)
        DatabaseWrapperMixin.__init__(self, settings_dict, alias=alias)

    def get_connection_params(self) -> dict[str, Any]:
        """Build OracleConfig locally from Django DATABASES settings."""
        host = str(self.settings_dict.get("HOST") or "localhost")
        port = int(self.settings_dict.get("PORT") or 1521)
        name = str(self.settings_dict.get("NAME") or "FREEPDB1")
        user = str(self.settings_dict.get("USER") or "system")
        password = str(self.settings_dict.get("PASSWORD") or "")
        auth = f"{user}:{password}@" if (user and password) else (f"{user}@" if user else "")
        dsn = f"oracle://{auth}{host}:{port}/{name}"

        cfg = OracleConfig(
            dsn=dsn,
            host=host,
            port=port,
            service_name=name,
            user=user,
            password=password,
            wallet_zip=self.pooler_config.wallet_zip,
            broker=self._broker if self._share_pool else None,
        )
        return {"config": cfg}

    def get_new_connection(self, conn_params: Mapping[str, Any]) -> SyncDBAPIConnection:
        """Open a native SyncDBAPIConnection using OracleConfig."""
        cfg = conn_params["config"]
        conn = SyncDBAPIConnection(cfg)
        conn.autocommit = True
        return conn

    def create_cursor(self, name: str | None = None) -> SyncDBAPICursor:
        """Create and return a DBAPI cursor adapting Django :arg0 and out-binds."""
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
                cur.execute("SELECT 1 FROM DUAL")
            return True
        except Exception:
            return False
