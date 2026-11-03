"""BigQuery DatabaseWrapper with Storage Read API Arrow streams and SHM LeaseBroker."""

from __future__ import annotations

from vertebra.extensions.django.db.backends import DatabaseWrapperMixin

__all__ = ("DatabaseWrapper",)


class DatabaseWrapper(DatabaseWrapperMixin):
    """Django BigQuery analytical database wrapper backed by Vertebra's LeaseBroker."""

    vendor: str = "bigquery"
    display_name: str = "BigQuery (Vertebra)"
    driver_scheme: str = "bigquery"
