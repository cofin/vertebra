"""SQLAlchemy 2.1 ORM and Core query optimizations (`selectinload`, `orm_insert_sentinel`, `Delete.using`, `tstring`)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = (
    "DeleteUsingClause",
    "InsertSentinelSpec",
    "ParameterizedSql",
    "SelectInLoadSpec",
    "build_delete_using",
    "chunked_selectinload",
    "compile_tstring_sql",
    "configure_insertmanyvalues_sentinel",
)


@dataclass(frozen=True, slots=True)
class SelectInLoadSpec:
    """SQLAlchemy 2.1 ``selectinload(chunksize=..., omit_join=...)`` configuration."""

    relationship_name: str
    chunksize: int = 500
    omit_join: bool = True

    def num_batches(self, total_keys: int) -> int:
        """Return the number of ``IN (...)`` batches required for ``total_keys``."""
        if total_keys <= 0:
            return 0
        return (total_keys + self.chunksize - 1) // self.chunksize


def chunked_selectinload(
    relationship_attr: str | Any,
    *,
    chunksize: int = 500,
    omit_join: bool = True,
) -> SelectInLoadSpec:
    """Configure SQLAlchemy 2.1 ``selectinload`` with Oracle-safe ``chunksize=500`` and ``omit_join=True``.

    Prevents Oracle ``ORA-01795: maximum number of expressions in a list is 1000`` while
    skipping redundant parent table joins introduced in SQLAlchemy 2.1.
    """
    if chunksize <= 0 or chunksize > 1000:
        msg = f"selectinload chunksize must be between 1 and 1000 (got {chunksize})"
        raise ValueError(msg)
    name = (
        relationship_attr
        if isinstance(relationship_attr, str)
        else getattr(relationship_attr, "key", str(relationship_attr))
    )
    return SelectInLoadSpec(relationship_name=name, chunksize=chunksize, omit_join=omit_join)


@dataclass(frozen=True, slots=True)
class InsertSentinelSpec:
    """SQLAlchemy 2.1 ``orm_insert_sentinel()`` descriptor for deterministic bulk ``INSERT ... RETURNING``."""

    column_name: str = "_insert_sentinel"
    use_insertmanyvalues: bool = True
    insert_executemany_returning: bool = True


def configure_insertmanyvalues_sentinel(column_name: str = "_insert_sentinel") -> InsertSentinelSpec:
    """Configure SQLAlchemy 2.1 ``orm_insert_sentinel()`` for bulk ``INSERT ... RETURNING`` correlation."""
    return InsertSentinelSpec(column_name=column_name)


@dataclass(frozen=True, slots=True)
class DeleteUsingClause:
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


def build_delete_using(
    target_table: str,
    *using_tables: str,
    where_sql: str | None = None,
) -> DeleteUsingClause:
    """Build a SQLAlchemy 2.1 ``Delete.using()`` multi-table delete statement."""
    if not using_tables:
        msg = "Delete.using() requires at least one USING table"
        raise ValueError(msg)
    return DeleteUsingClause(
        target_table=target_table,
        using_tables=tuple(using_tables),
        where_sql=where_sql,
    )


@dataclass(frozen=True, slots=True)
class ParameterizedSql:
    """Compiled parameterized SQL statement produced from a PEP 750 / SQLAlchemy 2.1 ``tstring``."""

    sql: str
    params: dict[str, Any]


def compile_tstring_sql(
    static_parts: Sequence[str],
    interpolated_values: Sequence[Any],
    *,
    param_prefix: str = "p",
) -> ParameterizedSql:
    """Compile static template parts and interpolated values into a bind-parameterized SQL query."""
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
    return ParameterizedSql(sql="".join(fragments), params=params)
