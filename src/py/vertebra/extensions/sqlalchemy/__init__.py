"""SQLAlchemy 2.1 +vertebra / +vtb dialects, Vector/BinaryJson types, Alembic DefaultImpl, and ORM optimizations."""

from __future__ import annotations

from vertebra.extensions.sqlalchemy.alembic import (
    AlembicImpl,
    CreateViewOp,
    DropViewOp,
)
from vertebra.extensions.sqlalchemy.dialects import (
    DIALECT_SCHEMES,
    DBAPIConnection,
    DBAPICursor,
    DBAPIModule,
    DialectDescriptor,
    MysqlDialect,
    OracleDialect,
    PostgresDialect,
    SqliteDialect,
    SyncEngineAdapter,
)
from vertebra.extensions.sqlalchemy.optimizations import (
    DeleteUsingClause,
    InsertSentinelSpec,
    ParameterizedSql,
    SelectInLoadSpec,
)
from vertebra.extensions.sqlalchemy.types import (
    BinaryJson,
    Vector,
    VectorDistanceExpr,
)

__all__ = (
    "DIALECT_SCHEMES",
    "AlembicImpl",
    "BinaryJson",
    "CreateViewOp",
    "DBAPIConnection",
    "DBAPICursor",
    "DBAPIModule",
    "DeleteUsingClause",
    "DialectDescriptor",
    "DropViewOp",
    "InsertSentinelSpec",
    "MysqlDialect",
    "OracleDialect",
    "ParameterizedSql",
    "PostgresDialect",
    "SelectInLoadSpec",
    "SqliteDialect",
    "SyncEngineAdapter",
    "Vector",
    "VectorDistanceExpr",
)
