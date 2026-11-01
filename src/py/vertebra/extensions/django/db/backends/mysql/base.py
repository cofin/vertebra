"""MySQL DatabaseWrapper with CloudSQLDialer and SHM LeaseBroker."""

from __future__ import annotations

from vertebra.extensions.django.db.backends import DatabaseWrapperMixin

__all__ = ("DatabaseWrapper",)


class DatabaseWrapper(DatabaseWrapperMixin):
    """Django MySQL database wrapper backed by Vertebra's LeaseBroker and CloudSQLDialer."""

    vendor: str = "mysql"
    display_name: str = "MySQL (Vertebra)"
