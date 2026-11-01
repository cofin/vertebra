"""MSSQL DatabaseWrapper with CloudSQLDialer and SHM LeaseBroker."""

from __future__ import annotations

from vertebra.extensions.django.db.backends import DatabaseWrapperMixin

__all__ = ("DatabaseWrapper",)


class DatabaseWrapper(DatabaseWrapperMixin):
    """Django MSSQL database wrapper backed by Vertebra's LeaseBroker."""

    vendor: str = "mssql"
    display_name: str = "SQL Server (Vertebra)"
