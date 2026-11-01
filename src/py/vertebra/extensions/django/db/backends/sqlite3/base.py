"""SQLite3 DatabaseWrapper with WAL pragmas and SHM LeaseBroker."""

from __future__ import annotations

from vertebra.extensions.django.db.backends import DatabaseWrapperMixin

__all__ = ("DatabaseWrapper",)


class DatabaseWrapper(DatabaseWrapperMixin):
    """Django SQLite3 database wrapper backed by Vertebra's LeaseBroker."""

    vendor: str = "sqlite"
    display_name: str = "SQLite (Vertebra)"
