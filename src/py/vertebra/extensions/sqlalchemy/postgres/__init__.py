"""Vertebra Postgres SQLAlchemy integration."""

from sqlalchemy.dialects import registry

# Register dialects
# For create_engine("postgresql+vertebra://")
registry.register("postgresql.vertebra", "vertebra.extensions.sqlalchemy.postgres.dialect", "VertebraPostgresDialect")

# For create_async_engine("postgresql+vertebra://")
# Wait, SQLAlchemy uses the same name for both often if the driver is polymorphic,
# but usually it's driver-specific.
# If we want create_async_engine to work with +vertebra, we might need a separate name
# OR make the dialect smart.
# Actually, SQLAlchemy's registry.register for a name like 'postgresql.vertebra'
# can only point to ONE class.

# Let's use standard naming:
# postgresql+vertebra -> async
# postgresql+vertebra_sync -> sync
registry.register(
    "postgresql.vertebra", "vertebra.extensions.sqlalchemy.postgres.dialect", "VertebraPostgresAsyncDialect"
)
registry.register(
    "postgresql.vertebra_sync", "vertebra.extensions.sqlalchemy.postgres.dialect", "VertebraPostgresDialect"
)

from .dialect import VertebraPostgresAsyncDialect, VertebraPostgresDialect

__all__ = ("VertebraPostgresAsyncDialect", "VertebraPostgresDialect")
