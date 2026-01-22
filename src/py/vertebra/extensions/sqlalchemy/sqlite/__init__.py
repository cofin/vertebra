"""Vertebra SQLite SQLAlchemy integration."""

from sqlalchemy.dialects import registry

from .dialect import VertebraSqliteAsyncDialect, VertebraSqliteDialect

registry.register("sqlite.vertebra", "vertebra.extensions.sqlalchemy.sqlite", "VertebraSqliteDialect")
registry.register("sqlite.vertebra_async", "vertebra.extensions.sqlalchemy.sqlite", "VertebraSqliteAsyncDialect")

__all__ = ("VertebraSqliteAsyncDialect", "VertebraSqliteDialect")
