"""Cloud Spanner DatabaseWrapper with multiplexed gRPC sessions and SHM LeaseBroker."""

from __future__ import annotations

from vertebra.extensions.django.db.backends import DatabaseWrapperMixin

__all__ = ("DatabaseWrapper",)


class DatabaseWrapper(DatabaseWrapperMixin):
    """Django Cloud Spanner database wrapper backed by Vertebra's LeaseBroker."""

    vendor: str = "spanner"
    display_name: str = "Cloud Spanner (Vertebra)"
    driver_scheme: str = "spanner"
