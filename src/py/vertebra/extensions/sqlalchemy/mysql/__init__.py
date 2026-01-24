"""Vertebra MySQL SQLAlchemy integration."""

from sqlalchemy.dialects import registry

from .dialect import VertebraMySQLAsyncDialect, VertebraMySQLDialect

registry.register("mysql.vertebra", "vertebra.extensions.sqlalchemy.mysql.dialect", "VertebraMySQLAsyncDialect")
registry.register("mysql.vertebra_sync", "vertebra.extensions.sqlalchemy.mysql.dialect", "VertebraMySQLDialect")

__all__ = ("VertebraMySQLAsyncDialect", "VertebraMySQLDialect")
