"""Vertebra MySQL SQLAlchemy integration."""

from sqlalchemy.dialects import registry

# Register dialects
registry.register("mysql.vertebra", "vertebra.extensions.sqlalchemy.mysql.dialect", "VertebraMySQLAsyncDialect")
registry.register("mysql.vertebra_sync", "vertebra.extensions.sqlalchemy.mysql.dialect", "VertebraMySQLDialect")

from .dialect import VertebraMySQLAsyncDialect, VertebraMySQLDialect

__all__ = ("VertebraMySQLAsyncDialect", "VertebraMySQLDialect")