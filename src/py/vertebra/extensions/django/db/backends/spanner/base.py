"""Django Cloud Spanner DatabaseWrapper over vertebra.db.dbapi with LeaseBroker."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra.db.adapters.spanner import SpannerConfig
from vertebra.db.dbapi import SyncDBAPIConnection, SyncDBAPIModule
from vertebra.extensions.django.db.backends import DatabaseWrapperMixin
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from collections.abc import Mapping

    from django.db.backends.base.base import BaseDatabaseWrapper
    from django.db.backends.base.client import BaseDatabaseClient
    from django.db.backends.base.creation import BaseDatabaseCreation
    from django.db.backends.base.features import BaseDatabaseFeatures
    from django.db.backends.base.introspection import BaseDatabaseIntrospection
    from django.db.backends.base.operations import BaseDatabaseOperations
    from django.db.backends.base.schema import BaseDatabaseSchemaEditor
else:
    BaseDatabaseWrapper = ensure_module(
        "django.db.backends.base.base", package="django", extra="django"
    ).BaseDatabaseWrapper
    BaseDatabaseClient = ensure_module(
        "django.db.backends.base.client", package="django", extra="django"
    ).BaseDatabaseClient
    BaseDatabaseCreation = ensure_module(
        "django.db.backends.base.creation", package="django", extra="django"
    ).BaseDatabaseCreation
    BaseDatabaseFeatures = ensure_module(
        "django.db.backends.base.features", package="django", extra="django"
    ).BaseDatabaseFeatures
    BaseDatabaseIntrospection = ensure_module(
        "django.db.backends.base.introspection", package="django", extra="django"
    ).BaseDatabaseIntrospection
    BaseDatabaseOperations = ensure_module(
        "django.db.backends.base.operations", package="django", extra="django"
    ).BaseDatabaseOperations
    BaseDatabaseSchemaEditor = ensure_module(
        "django.db.backends.base.schema", package="django", extra="django"
    ).BaseDatabaseSchemaEditor

__all__ = ("DatabaseWrapper",)


class DatabaseWrapper(DatabaseWrapperMixin, BaseDatabaseWrapper):
    """Django Cloud Spanner database wrapper backed by Vertebra native driver and LeaseBroker."""

    vendor: str = "spanner"
    display_name: str = "Cloud Spanner (Vertebra)"
    driver_scheme: str = "spanner"
    Database: Any = SyncDBAPIModule(paramstyle="named")
    SchemaEditorClass = BaseDatabaseSchemaEditor
    client_class = BaseDatabaseClient
    creation_class = BaseDatabaseCreation
    features_class = BaseDatabaseFeatures
    introspection_class = BaseDatabaseIntrospection
    ops_class = BaseDatabaseOperations

    def __init__(self, settings_dict: dict[str, Any], alias: str = "default") -> None:
        """Initialize Cloud Spanner DatabaseWrapper with settings dictionary and alias."""
        ensure_module("django_spanner.base", package="django-google-spanner", extra="django-spanner")
        BaseDatabaseWrapper.__init__(self, settings_dict, alias=alias)
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
