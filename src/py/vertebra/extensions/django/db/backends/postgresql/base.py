"""PostgreSQL DatabaseWrapper with CloudSQLDialer, AlloyDBDialer, PgBouncer, and SHM LeaseBroker."""

from __future__ import annotations

from vertebra.extensions.django.db.backends import DatabaseWrapperMixin

__all__ = ("DatabaseWrapper",)


class DatabaseWrapper(DatabaseWrapperMixin):
    """Django PostgreSQL database wrapper backed by Vertebra's LeaseBroker and Cloud Dialers."""

    vendor: str = "postgresql"
    display_name: str = "PostgreSQL (Vertebra)"
