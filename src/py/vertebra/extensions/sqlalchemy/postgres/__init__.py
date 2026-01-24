"""Vertebra Postgres SQLAlchemy integration.

Dialect naming:
- postgresql+vertebra -> async dialect
- postgresql+vertebra_sync -> sync dialect
"""

from sqlalchemy.dialects import registry

from .dialect import VertebraPostgresAsyncDialect, VertebraPostgresDialect

registry.register(
    "postgresql.vertebra", "vertebra.extensions.sqlalchemy.postgres.dialect", "VertebraPostgresAsyncDialect"
)
registry.register(
    "postgresql.vertebra_sync", "vertebra.extensions.sqlalchemy.postgres.dialect", "VertebraPostgresDialect"
)

__all__ = ("VertebraPostgresAsyncDialect", "VertebraPostgresDialect")
