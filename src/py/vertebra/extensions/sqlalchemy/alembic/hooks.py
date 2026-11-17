"""Alembic 1.18+ autogenerate column comparator and item rendering hooks for Vertebra types."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from alembic.autogenerate import comparators
from alembic.util import DispatchPriority, PriorityDispatchResult
from sqlalchemy import types as sqltypes

from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from alembic.autogenerate.api import AutogenContext
    from alembic.operations.ops import AlterColumnOp
    from sqlalchemy.sql.elements import quoted_name
    from sqlalchemy.sql.schema import Column

__all__ = (
    "compare_column_types",
    "compare_table_options",
    "register_alembic_hooks",
    "render_item",
)


def _vector_type_classes(sa_mod: Any) -> tuple[type[Any], ...]:
    return (
        sa_mod.Vector,
        sa_mod.PgVector,
        sa_mod.HalfVector,
        sa_mod.SparseVector,
        sa_mod.OracleVector,
        sa_mod.SpannerVector,
    )


def _compare_vector_types(
    autogen_context: AutogenContext,
    alter_column_op: AlterColumnOp,
    conn_type: Any,
    metadata_type: Any,
    vector_types: tuple[type[Any], ...],
) -> PriorityDispatchResult:
    if isinstance(conn_type, vector_types):
        meta_dim = getattr(metadata_type, "dimensions", None)
        insp_dim = getattr(conn_type, "dimensions", None)
        meta_prec = getattr(metadata_type, "precision", None)
        insp_prec = getattr(conn_type, "precision", None)
        if insp_dim != meta_dim or insp_prec != meta_prec:
            alter_column_op.modify_type = metadata_type
        return PriorityDispatchResult.STOP
    inspected_str = str(conn_type).upper()
    dialect_name = getattr(getattr(autogen_context, "dialect", None), "name", "").lower()
    if "VECTOR" in inspected_str:
        meta_dim = getattr(metadata_type, "dimensions", None)
        if meta_dim is not None and str(meta_dim) not in inspected_str:
            alter_column_op.modify_type = metadata_type
        return PriorityDispatchResult.STOP
    if inspected_str == "BLOB" and dialect_name == "sqlite":
        return PriorityDispatchResult.STOP
    if inspected_str.startswith("ARRAY") and dialect_name in ("spanner", "bigquery"):
        return PriorityDispatchResult.STOP
    alter_column_op.modify_type = metadata_type
    return PriorityDispatchResult.STOP


def _compare_json_types(
    autogen_context: AutogenContext,
    alter_column_op: AlterColumnOp,
    conn_type: Any,
    metadata_type: Any,
    msgspec_json_cls: type[Any],
) -> PriorityDispatchResult:
    if isinstance(conn_type, msgspec_json_cls):
        return PriorityDispatchResult.STOP
    inspected_str = str(conn_type).upper()
    dialect = getattr(autogen_context, "dialect", None)
    dialect_name = getattr(dialect, "name", "").lower()
    if inspected_str in ("JSON", "JSONB", "OSON") or "JSON" in inspected_str:
        return PriorityDispatchResult.STOP
    if dialect_name == "mssql" and inspected_str in ("NVARCHAR", "NVARCHAR(MAX)", "TEXT"):
        return PriorityDispatchResult.STOP
    if dialect_name == "oracle" and isinstance(conn_type, sqltypes.BLOB):
        sv = getattr(dialect, "server_version_info", None)
        if sv is not None and sv[0] < 21:
            return PriorityDispatchResult.STOP
    alter_column_op.modify_type = metadata_type
    return PriorityDispatchResult.STOP


def compare_column_types(
    autogen_context: AutogenContext,
    alter_column_op: AlterColumnOp,
    schema: str | None,
    tname: quoted_name | str,
    cname: quoted_name | str,
    conn_col: Column[Any],
    metadata_col: Column[Any],
) -> PriorityDispatchResult:
    """Compare column types for Vertebra Vector and MsgspecJSON using Alembic 1.18+ column dispatch."""
    _ = (schema, tname, cname)
    sa_mod = ensure_module("vertebra.extensions.sqlalchemy")
    vector_types = _vector_type_classes(sa_mod)
    conn_type = conn_col.type
    metadata_type = metadata_col.type

    if isinstance(metadata_type, vector_types):
        alter_column_op.existing_type = conn_type
        return _compare_vector_types(autogen_context, alter_column_op, conn_type, metadata_type, vector_types)

    if isinstance(metadata_type, sa_mod.MsgspecJSON):
        alter_column_op.existing_type = conn_type
        return _compare_json_types(autogen_context, alter_column_op, conn_type, metadata_type, sa_mod.MsgspecJSON)

    if isinstance(conn_type, (*vector_types, sa_mod.MsgspecJSON)):
        alter_column_op.existing_type = conn_type
        alter_column_op.modify_type = metadata_type
        return PriorityDispatchResult.STOP

    return PriorityDispatchResult.CONTINUE


def compare_table_options(
    context: Any,
    modify_table_ops: Any,
    schema: str | None,
    tablename: str,
    conn_table: Any,
    metadata_table: Any,
) -> PriorityDispatchResult:
    """Compare table-level dialect options for Spanner and BigQuery without false positives."""
    _ = (context, modify_table_ops, schema, tablename, conn_table, metadata_table)
    return PriorityDispatchResult.CONTINUE


def render_item(
    type_: str,
    obj: Any,
    autogen_context: Any,
) -> str | Literal[False]:
    """Alembic render_item hook rendering Vector and MsgspecJSON column types."""
    if type_ != "type":
        return False
    sa_mod = ensure_module("vertebra.extensions.sqlalchemy")
    imports = getattr(autogen_context, "imports", None)
    if isinstance(obj, sa_mod.Vector):
        if isinstance(imports, set):
            imports.add("from vertebra.extensions.sqlalchemy import Vector")
        dim = obj.dimensions
        if dim is None:
            return f"Vector(storage_format={obj.precision!r})"
        return f"Vector({dim}, storage_format={obj.precision!r})"
    for cls_name in ("PgVector", "HalfVector", "SparseVector", "OracleVector", "SpannerVector"):
        vec_cls = getattr(sa_mod, cls_name)
        if isinstance(obj, vec_cls):
            if isinstance(imports, set):
                imports.add(f"from vertebra.extensions.sqlalchemy import {cls_name}")
            dim = getattr(obj, "dimensions", None)
            return f"{cls_name}({dim!r})" if dim is not None else f"{cls_name}()"
    if isinstance(obj, sa_mod.MsgspecJSON):
        if isinstance(imports, set):
            imports.add("from vertebra.extensions.sqlalchemy import MsgspecJSON")
        return "MsgspecJSON()"
    return False


def register_alembic_hooks() -> None:
    """Register Alembic 1.18+ column and table comparator hooks."""
    comparators.dispatch_for("column", subgroup="types", priority=DispatchPriority.FIRST)(compare_column_types)
    comparators.dispatch_for("table")(compare_table_options)


register_alembic_hooks()
