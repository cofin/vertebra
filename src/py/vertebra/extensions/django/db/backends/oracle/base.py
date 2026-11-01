"""Oracle DatabaseWrapper with OracleWallet, DB_TYPE_VECTOR NumPy handlers, and SHM LeaseBroker."""

from __future__ import annotations

from vertebra.extensions.django.db.backends import DatabaseWrapperMixin

__all__ = ("DatabaseWrapper",)


class DatabaseWrapper(DatabaseWrapperMixin):
    """Django Oracle database wrapper with mTLS Wallet, Vector, and Arrow PyCapsule support."""

    vendor: str = "oracle"
    display_name: str = "Oracle (Vertebra)"
