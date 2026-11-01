"""DuckDB DatabaseWrapper with native Arrow zero-copy handoff and SHM LeaseBroker."""

from __future__ import annotations

from vertebra.extensions.django.db.backends import DatabaseWrapperMixin

__all__ = ("DatabaseWrapper",)


class DatabaseWrapper(DatabaseWrapperMixin):
    """Django DuckDB analytical database wrapper backed by Vertebra's LeaseBroker."""

    vendor: str = "duckdb"
    display_name: str = "DuckDB (Vertebra)"
