"""Vertebra MySQL SQLAlchemy integration."""

from sqlalchemy.dialects import registry

from .dialect import VertebraMysqlAsyncDialect, VertebraMysqlDialect

registry.register("mysql.vertebra", "vertebra.extensions.sqlalchemy.mysql", "VertebraMysqlDialect")
registry.register("mysql.vertebra_async", "vertebra.extensions.sqlalchemy.mysql", "VertebraMysqlAsyncDialect")

__all__ = ("VertebraMysqlAsyncDialect", "VertebraMysqlDialect")
