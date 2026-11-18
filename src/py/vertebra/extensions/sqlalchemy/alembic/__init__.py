"""Alembic DefaultImpl subclasses, Vector/MsgspecJSON autogenerate hooks, and view/index DDL operations."""

from __future__ import annotations

from vertebra.extensions.sqlalchemy.alembic.hooks import (
    compare_column_types,
    compare_table_options,
    register_alembic_hooks,
    render_item,
)
from vertebra.extensions.sqlalchemy.alembic.impl import (
    BigQueryAlembicImpl,
    DuckDBAlembicImpl,
    SpangresAlembicImpl,
    SpannerAlembicImpl,
)
from vertebra.extensions.sqlalchemy.alembic.ops import (
    CreateVectorIndexOp,
    CreateViewOp,
    DropVectorIndexOp,
    DropViewOp,
)

__all__ = (
    "BigQueryAlembicImpl",
    "CreateVectorIndexOp",
    "CreateViewOp",
    "DropVectorIndexOp",
    "DropViewOp",
    "DuckDBAlembicImpl",
    "SpangresAlembicImpl",
    "SpannerAlembicImpl",
    "compare_column_types",
    "compare_table_options",
    "register_alembic_hooks",
    "render_item",
)
