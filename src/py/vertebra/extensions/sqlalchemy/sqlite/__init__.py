"""Vertebra SQLite SQLAlchemy integration."""

from sqlalchemy.dialects import registry

# Register dialects
registry.register("sqlite.vertebra", "vertebra.extensions.sqlalchemy.sqlite.dialect", "VertebraSQLiteAsyncDialect")
registry.register("sqlite.vertebra_sync", "vertebra.extensions.sqlalchemy.sqlite.dialect", "VertebraSQLiteDialect")

from .dialect import VertebraSQLiteAsyncDialect, VertebraSQLiteDialect

__all__ = ("VertebraSQLiteAsyncDialect", "VertebraSQLiteDialect")