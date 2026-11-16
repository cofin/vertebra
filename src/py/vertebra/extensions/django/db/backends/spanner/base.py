"""Django Cloud Spanner DatabaseWrapper over vertebra.db.dbapi with LeaseBroker."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from vertebra.db.adapters.spanner import SpannerConfig
from vertebra.db.dbapi import SyncDBAPIConnection, SyncDBAPIModule
from vertebra.extensions.django.db.backends import DatabaseWrapperMixin
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    _BaseSpannerDatabaseWrapper: Any = object
else:
    try:
        from django_spanner.base import (
            DatabaseWrapper as _BaseSpannerDatabaseWrapper,
        )
    except ImportError:
        _BaseSpannerDatabaseWrapper = object

if TYPE_CHECKING:
    from collections.abc import Mapping

    from vertebra.db.dbapi import SyncDBAPICursor

__all__ = ("DatabaseWrapper",)


class DatabaseWrapper(DatabaseWrapperMixin, _BaseSpannerDatabaseWrapper):
    """Django Cloud Spanner database wrapper backed by Vertebra native driver and LeaseBroker."""

    vendor: str = "spanner"
    display_name: str = "Cloud Spanner (Vertebra)"
    driver_scheme: str = "spanner"
    Database: Any = SyncDBAPIModule(paramstyle="named")

    def __init__(self, settings_dict: dict[str, Any], alias: str = "default") -> None:
        """Initialize Cloud Spanner DatabaseWrapper with settings dictionary and alias."""
        ensure_module("django_spanner", package="vertebra-core[django-spanner]", extra="django-spanner")
        if _BaseSpannerDatabaseWrapper is not object:
            _BaseSpannerDatabaseWrapper.__init__(self, settings_dict, alias=alias)
        DatabaseWrapperMixin.__init__(self, settings_dict, alias=alias)

    def get_connection_params(self) -> dict[str, Any]:
        """Build SpannerConfig locally from Django DATABASES settings."""
        options = dict(self.settings_dict.get("OPTIONS") or {})
        dsn = str(options.get("dsn") or "")
        name = str(self.settings_dict.get("NAME") or "")
        if dsn:
            cfg = SpannerConfig.from_dsn(dsn)
        elif name.startswith("projects/"):
            cfg = SpannerConfig.from_dsn(f"spanner://{name}")
        else:
            project_id = str(options.get("project_id") or self.settings_dict.get("PROJECT") or "test-project")
            instance_id = str(options.get("instance_id") or self.settings_dict.get("INSTANCE") or "test-instance")
            db_name = name or str(options.get("database_id") or "test-db")
            dialect_raw = str(options.get("dialect") or self.settings_dict.get("DIALECT") or "googlesql").lower()
            database_role = options.get("database_role")
            endpoint = str(
                options.get("endpoint")
                or (
                    f"http://{self.settings_dict.get('HOST', 'localhost')}:{self.settings_dict.get('PORT', 9010)}"
                    if self.settings_dict.get("HOST") not in {"localhost", "", None} and self.settings_dict.get("PORT")
                    else "https://spanner.googleapis.com"
                )
            )
            cfg = SpannerConfig(
                project_id=project_id,
                instance_id=instance_id,
                database_id=db_name,
                dialect="spangres" if dialect_raw in {"spangres", "postgresql", "pg"} else "googlesql",
                database_role=str(database_role) if database_role is not None else None,
                endpoint=endpoint,
                broker=self._broker if self._share_pool else None,
            )
        return {"config": cfg}

    def get_new_connection(self, conn_params: Mapping[str, Any]) -> SyncDBAPIConnection:
        """Open a native SyncDBAPIConnection using SpannerConfig."""
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
