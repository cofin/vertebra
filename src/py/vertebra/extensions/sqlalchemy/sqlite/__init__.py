"""Vertebra SQLite SQLAlchemy integration."""

from sqlalchemy.dialects import registry

from .dialect import VertebraSQLiteAsyncDialect, VertebraSQLiteDialect

registry.register("sqlite.vertebra", "vertebra.extensions.sqlalchemy.sqlite.dialect", "VertebraSQLiteAsyncDialect")
registry.register("sqlite.vertebra_sync", "vertebra.extensions.sqlalchemy.sqlite.dialect", "VertebraSQLiteDialect")

__all__ = ("VertebraSQLiteAsyncDialect", "VertebraSQLiteDialect")
