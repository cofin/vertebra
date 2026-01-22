"""Vertebra Postgres SQLAlchemy integration."""

from sqlalchemy.dialects import registry

from .dialect import VertebraPostgresAsyncDialect, VertebraPostgresDialect

registry.register("postgresql.vertebra", "vertebra.extensions.sqlalchemy.postgres", "VertebraPostgresDialect")
registry.register(
    "postgresql.vertebra_async", "vertebra.extensions.sqlalchemy.postgres", "VertebraPostgresAsyncDialect"
)

__all__ = ("VertebraPostgresAsyncDialect", "VertebraPostgresDialect")
