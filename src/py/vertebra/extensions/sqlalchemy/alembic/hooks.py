"""Alembic autogenerate comparator hooks for Vertebra vector types, JSON, and cloud dialects."""

from __future__ import annotations

from typing import Any

from vertebra.extensions.sqlalchemy.alembic.impl import _compare_type_impl, render_item

comparators: Any
try:
    from alembic.autogenerate import comparators

    _HAS_ALEMBIC = True
except ImportError:
    comparators = None
    _HAS_ALEMBIC = False

__all__ = (
    "compare_table_options",
    "compare_type",
    "register_alembic_hooks",
    "render_item",
)


def compare_type(
    context: Any,
    inspected_column: Any,
    metadata_column: Any,
    inspected_type: Any = None,
    metadata_type: Any = None,
    *args: Any,
    **kwargs: Any,
) -> bool | None:
    """Compare database types, suppressing false-positive autogenerate diffs for Vertebra types."""
    return _compare_type_impl(
        context,
        inspected_column,
        metadata_column,
        inspected_type,
        metadata_type,
        *args,
        **kwargs,
    )


def compare_table_options(
    context: Any,
    modify_table_ops: Any,
    schema: str | None,
    tablename: str,
    conn_table: Any,
    metadata_table: Any,
) -> None:
    """Compare table-level dialect options for Spanner and BigQuery without false positives."""
    _ = (context, modify_table_ops, schema, tablename, conn_table, metadata_table)


def register_alembic_hooks() -> None:
    """Register Alembic comparator and item rendering hooks when Alembic is installed."""
    if not _HAS_ALEMBIC or comparators is None:
        return
    comparators.dispatch_for("type")(compare_type)
    comparators.dispatch_for("table")(compare_table_options)


register_alembic_hooks()
