"""SQLAlchemy 2.1 synchronous and asynchronous dialects for PostgreSQL (+vtb / +vertebra)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.dialects.postgresql.base import PGDialect

from vertebra.extensions.sqlalchemy.dialects._base import _AsyncDialectMixin, _SyncDialectMixin
from vertebra.extensions.sqlalchemy.types import HalfVector, PgVector, SparseVector

if TYPE_CHECKING:
    from vertebra.extensions.sqlalchemy.types import (
        DeleteUsingClause,
        InsertSentinelSpec,
    )

__all__ = (
    "AsyncPostgreSQLDialect",
    "SyncPostgreSQLDialect",
)


class SyncPostgreSQLDialect(_SyncDialectMixin, PGDialect):
    """SQLAlchemy 2.1 synchronous dialect for PostgreSQL (+vtb / +vertebra)."""

    driver = "vtb"
    supports_statement_cache = True
    default_paramstyle = "numeric_dollar"
    paramstyle = "numeric_dollar"
    use_insertmanyvalues = True
    insert_executemany_returning = True
    supports_native_jsonb = True

    ischema_names = dict(PGDialect.ischema_names)
    ischema_names.update(
        {
            "array": ARRAY,
            "halfvec": HalfVector,
            "jsonb": JSONB,
            "sparsevec": SparseVector,
            "uuid": UUID,
            "vector": PgVector,
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
        """Compile a bulk INSERT ... RETURNING query with an insert sentinel."""
        cols_str = ", ".join(columns)
        vals_str = ", ".join(f":{c}" for c in columns)
        sentinel_col = spec.sentinel_column
        return f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str}) RETURNING {pk_column}, {sentinel_col}"

    @classmethod
    def compile_delete_using(cls, clause: DeleteUsingClause) -> str:
        """Compile a multi-table DELETE ... USING statement."""
        return clause.to_sql()

    def type_descriptor(self, typeobj: Any) -> Any:
        """Adapt SQLAlchemy and custom column types using PostgreSQL colspecs."""
        parent_descriptor: Any = super().type_descriptor
        return parent_descriptor(typeobj)


class AsyncPostgreSQLDialect(_AsyncDialectMixin, PGDialect):
    """SQLAlchemy 2.1 asynchronous dialect for PostgreSQL (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True
    default_paramstyle = "numeric_dollar"
    paramstyle = "numeric_dollar"
    use_insertmanyvalues = True
    insert_executemany_returning = True
    supports_native_jsonb = True

    ischema_names = dict(PGDialect.ischema_names)
    ischema_names.update(
        {
            "array": ARRAY,
            "halfvec": HalfVector,
            "jsonb": JSONB,
            "sparsevec": SparseVector,
            "uuid": UUID,
            "vector": PgVector,
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
        """Compile a bulk INSERT ... RETURNING query with an insert sentinel."""
        cols_str = ", ".join(columns)
        vals_str = ", ".join(f":{c}" for c in columns)
        sentinel_col = spec.sentinel_column
        return f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str}) RETURNING {pk_column}, {sentinel_col}"

    @classmethod
    def compile_delete_using(cls, clause: DeleteUsingClause) -> str:
        """Compile a multi-table DELETE ... USING statement."""
        return clause.to_sql()

    def type_descriptor(self, typeobj: Any) -> Any:
        """Adapt SQLAlchemy and custom column types using PostgreSQL colspecs."""
        parent_descriptor: Any = super().type_descriptor
        return parent_descriptor(typeobj)
