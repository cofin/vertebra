"""SQLAlchemy 2.1 ORM and Core query optimizations (`selectinload`, `orm_insert_sentinel`, `Delete.using`, `tstring`)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import msgspec

__all__ = (
    "DeleteUsingClause",
    "InsertSentinelSpec",
    "ParameterizedSql",
    "SelectInLoadSpec",
)


class SelectInLoadSpec(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """SQLAlchemy 2.1 ``selectinload(chunksize=..., omit_join=...)`` configuration."""

    relationship_name: str
    chunksize: int = 500
    omit_join: bool = True
    recursion_depth: int | None = None

    @property
    def batch_size(self) -> int:
        """Return the batch chunk size."""
        return self.chunksize

    def num_batches(self, total_keys: int) -> int:
        """Return the number of ``IN (...)`` batches required for ``total_keys``."""
        if total_keys <= 0:
            return 0
        return (total_keys + self.chunksize - 1) // self.chunksize

    @classmethod
    def create(
        cls,
        relationship_name: str | Any,
        *,
        recursion_depth: int | None = None,
        batch_size: int | None = None,
        chunksize: int = 500,
        omit_join: bool = True,
    ) -> SelectInLoadSpec:
        """Configure SQLAlchemy 2.1 ``selectinload`` with Oracle-safe ``chunksize`` and ``omit_join=True``.

        Prevents Oracle ``ORA-01795: maximum number of expressions in a list is 1000`` while
        skipping redundant parent table joins introduced in SQLAlchemy 2.1.
        """
        effective_chunksize = batch_size if batch_size is not None else chunksize
        if effective_chunksize <= 0 or effective_chunksize > 1000:
            msg = f"selectinload chunksize must be between 1 and 1000 (got {effective_chunksize})"
            raise ValueError(msg)
        name = (
            relationship_name
            if isinstance(relationship_name, str)
            else getattr(relationship_name, "key", str(relationship_name))
        )
        return cls(
            relationship_name=name,
            chunksize=effective_chunksize,
            omit_join=omit_join,
            recursion_depth=recursion_depth,
        )


class InsertSentinelSpec(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """SQLAlchemy 2.1 ``orm_insert_sentinel()`` descriptor for deterministic bulk ``INSERT ... RETURNING``."""

    column_name: str = "_insert_sentinel"
    table_name: str = ""
    page_size: int = 1000
    use_insertmanyvalues: bool = True
    insert_executemany_returning: bool = True

    @property
    def sentinel_column(self) -> str:
        """Return the sentinel column name."""
        return self.column_name

    @classmethod
    def create(
        cls,
        table_name: str = "",
        *,
        sentinel_column: str = "_insert_sentinel",
        column_name: str | None = None,
        page_size: int = 1000,
    ) -> InsertSentinelSpec:
        """Configure SQLAlchemy 2.1 ``orm_insert_sentinel()`` for bulk ``INSERT ... RETURNING`` correlation."""
        resolved_col = column_name if column_name is not None else sentinel_column
        return cls(
            column_name=resolved_col,
            table_name=table_name,
            page_size=page_size,
        )


class DeleteUsingClause(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """SQLAlchemy 2.1 ``Delete.using()`` multi-table delete descriptor."""

    target_table: str
    using_tables: tuple[str, ...]
    where_sql: str | None = None

    def to_sql(self) -> str:
        """Render the ``DELETE FROM <target> USING <tables> [WHERE ...]`` statement."""
        using_part = ", ".join(self.using_tables)
        stmt = f"DELETE FROM {self.target_table} USING {using_part}"
        if self.where_sql:
            stmt = f"{stmt} WHERE {self.where_sql}"
        return stmt

    @classmethod
    def create(
        cls,
        target_table: str,
        *using_tables: str,
        join_condition: str | None = None,
        where_condition: str | None = None,
        where_sql: str | None = None,
    ) -> DeleteUsingClause:
        """Build a SQLAlchemy 2.1 ``Delete.using()`` multi-table delete statement."""
        if not using_tables:
            msg = "Delete.using() requires at least one USING table"
            raise ValueError(msg)
        resolved_tables = list(using_tables)
        inferred_join: str | None = None
        if len(resolved_tables) >= 2 and ("=" in resolved_tables[-1] or " " in resolved_tables[-1].strip()):
            inferred_join = resolved_tables.pop()
        conditions = [c for c in (inferred_join, join_condition, where_condition, where_sql) if c]
        combined_where = " AND ".join(conditions) if conditions else None
        return cls(
            target_table=target_table,
            using_tables=tuple(resolved_tables),
            where_sql=combined_where,
        )


class ParameterizedSql(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Compiled parameterized SQL statement produced from a PEP 750 / SQLAlchemy 2.1 ``tstring``."""

    sql: str
    params: dict[str, Any]

    @classmethod
    def from_template(
        cls,
        template: Any,
        parts: Sequence[Any] | None = None,
        *,
        param_prefix: str = "p",
    ) -> ParameterizedSql:
        """Compile static template parts and interpolated values into a bind-parameterized SQL query."""
        if hasattr(template, "strings") and hasattr(template, "interpolations"):
            static_parts: Sequence[str] = tuple(template.strings)
            interpolated_values: Sequence[Any] = tuple(
                getattr(interp, "value", interp) for interp in template.interpolations
            )
        elif isinstance(template, str):
            raw_pairs = list(parts or ())
            return cls(sql=template, params=dict(raw_pairs))
        else:
            static_parts = tuple(template)
            interpolated_values = tuple(parts or ())

        if len(static_parts) != len(interpolated_values) + 1:
            msg = "static_parts must have length len(interpolated_values) + 1"
            raise ValueError(msg)
        fragments: list[str] = [static_parts[0]]
        params: dict[str, Any] = {}
        for idx, val in enumerate(interpolated_values, start=1):
            pname = f"{param_prefix}_{idx}"
            params[pname] = val
            fragments.append(f":{pname}")
            fragments.append(static_parts[idx])
        return cls(sql="".join(fragments), params=params)
