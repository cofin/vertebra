"""Alembic DefaultImpl subclasses, Vector/BinaryJSON autogenerate hooks, and view/index DDL operations."""

from __future__ import annotations

from vertebra.extensions.sqlalchemy.alembic.hooks import (
    compare_table_options,
    compare_type,
    register_alembic_hooks,
    render_item,
)
from vertebra.extensions.sqlalchemy.alembic.impl import (
    AlembicImpl,
    BigQueryAlembicImpl,
    DuckDBAlembicImpl,
    MSSQLAlembicImpl,
    MySQLAlembicImpl,
    OracleAlembicImpl,
    PostgreSQLAlembicImpl,
    SpangresAlembicImpl,
    SpannerAlembicImpl,
    SQLiteAlembicImpl,
)
from vertebra.extensions.sqlalchemy.alembic.ops import (
    CreateVectorIndexOp,
    CreateViewOp,
    DropVectorIndexOp,
    DropViewOp,
)

__all__ = (
    "AlembicImpl",
    "BigQueryAlembicImpl",
    "CreateVectorIndexOp",
    "CreateViewOp",
    "DropVectorIndexOp",
    "DropViewOp",
    "DuckDBAlembicImpl",
    "MSSQLAlembicImpl",
    "MySQLAlembicImpl",
    "OracleAlembicImpl",
    "PostgreSQLAlembicImpl",
    "SQLiteAlembicImpl",
    "SpangresAlembicImpl",
    "SpannerAlembicImpl",
    "compare_table_options",
    "compare_type",
    "register_alembic_hooks",
    "render_item",
)
