"""SQLAlchemy 2.1 synchronous and asynchronous dialects for Oracle (+vtb / +vertebra)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from sqlalchemy.dialects.oracle.base import OracleDialect as BaseOracleDialect

from vertebra.extensions.sqlalchemy.dialects._base import _AsyncDialectMixin, _SyncDialectMixin
from vertebra.extensions.sqlalchemy.types import MsgspecJSON, OracleVector

if TYPE_CHECKING:
    from vertebra.extensions.sqlalchemy.types import (
        DeleteUsingClause,
        InsertSentinelSpec,
    )

__all__ = (
    "AsyncOracleDialect",
    "SyncOracleDialect",
)


class SyncOracleDialect(_SyncDialectMixin, BaseOracleDialect):
    """SQLAlchemy 2.1 synchronous dialect for Oracle (+vtb / +vertebra)."""

    driver = "vtb"
    supports_statement_cache = True
    supports_native_boolean = True
    default_paramstyle = "named"
    paramstyle = "named"

    ischema_names = dict(BaseOracleDialect.ischema_names)
    ischema_names.update(
        {
            "JSON": MsgspecJSON,
            "VECTOR": OracleVector,
        }
    )

    @classmethod
    def compile_insert_returning_sentinel(
        cls,
        table_name: str,
        columns: Sequence[str],
        spec: InsertSentinelSpec,
        *,
        pk_column: str = "id",
    ) -> str:
        """Compile a bulk INSERT ... RETURNING query with an insert sentinel for Oracle."""
        cols_str = ", ".join(columns)
        vals_str = ", ".join(f":{c}" for c in columns)
        sentinel_col = spec.sentinel_column
        return f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str}) RETURNING {pk_column}, {sentinel_col} INTO :ret_{pk_column}, :ret_{sentinel_col}"

    @classmethod
    def compile_delete_using(cls, clause: DeleteUsingClause) -> str:
        """Compile a multi-table DELETE ... USING statement for Oracle."""
        return clause.to_sql()

    def type_descriptor(self, typeobj: Any) -> Any:
        """Adapt SQLAlchemy and custom column types using Oracle colspecs."""
        parent_descriptor: Any = super().type_descriptor
        return parent_descriptor(typeobj)


class AsyncOracleDialect(_AsyncDialectMixin, BaseOracleDialect):
    """SQLAlchemy 2.1 asynchronous dialect for Oracle (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True
    supports_native_boolean = True
    default_paramstyle = "named"
    paramstyle = "named"

    ischema_names = dict(BaseOracleDialect.ischema_names)
    ischema_names.update(
        {
            "JSON": MsgspecJSON,
            "VECTOR": OracleVector,
        }
    )

    @classmethod
    def compile_insert_returning_sentinel(
        cls,
        table_name: str,
        columns: Sequence[str],
        spec: InsertSentinelSpec,
        *,
        pk_column: str = "id",
    ) -> str:
        """Compile a bulk INSERT ... RETURNING query with an insert sentinel for Oracle."""
        cols_str = ", ".join(columns)
        vals_str = ", ".join(f":{c}" for c in columns)
        sentinel_col = spec.sentinel_column
        return f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str}) RETURNING {pk_column}, {sentinel_col} INTO :ret_{pk_column}, :ret_{sentinel_col}"

    @classmethod
    def compile_delete_using(cls, clause: DeleteUsingClause) -> str:
        """Compile a multi-table DELETE ... USING statement for Oracle."""
        return clause.to_sql()

    def type_descriptor(self, typeobj: Any) -> Any:
        """Adapt SQLAlchemy and custom column types using Oracle colspecs."""
        parent_descriptor: Any = super().type_descriptor
        return parent_descriptor(typeobj)
