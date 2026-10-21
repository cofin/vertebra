"""SQLAlchemy 2.1 +vertebra / +vtb dialects, Vector/BinaryJson types, Alembic DefaultImpl, and ORM optimizations."""

from __future__ import annotations

from vertebra.extensions.sqlalchemy.alembic import (
    ALEMBIC_DIALECT_IMPLS,
    CreateViewOp,
    DropViewOp,
    VertebraAlembicImpl,
    compare_vertebra_type,
    register_alembic_impls,
    render_vertebra_item,
    resolve_alembic_migration_url,
)
from vertebra.extensions.sqlalchemy.dialects import (
    DIALECT_SCHEMES,
    FreeThreadedEngineAdapter,
    MysqlVertebraDialect,
    OracleVertebraDialect,
    PostgresVertebraDialect,
    SqliteVertebraDialect,
    VertebraDBAPIConnection,
    VertebraDBAPICursor,
    VertebraDBAPIModule,
    VertebraDialectDescriptor,
    register_sqlalchemy_dialects,
)
from vertebra.extensions.sqlalchemy.optimizations import (
    DeleteUsingClause,
    InsertSentinelSpec,
    ParameterizedSql,
    SelectInLoadSpec,
    build_delete_using,
    chunked_selectinload,
    compile_tstring_sql,
    configure_insertmanyvalues_sentinel,
)
from vertebra.extensions.sqlalchemy.types import (
    BinaryJson,
    Vector,
    VectorDistanceExpr,
)

__all__ = (
    "ALEMBIC_DIALECT_IMPLS",
    "DIALECT_SCHEMES",
    "BinaryJson",
    "CreateViewOp",
    "DeleteUsingClause",
    "DropViewOp",
    "FreeThreadedEngineAdapter",
    "InsertSentinelSpec",
    "MysqlVertebraDialect",
    "OracleVertebraDialect",
    "ParameterizedSql",
    "PostgresVertebraDialect",
    "SelectInLoadSpec",
    "SqliteVertebraDialect",
    "Vector",
    "VectorDistanceExpr",
    "VertebraAlembicImpl",
    "VertebraDBAPIConnection",
    "VertebraDBAPICursor",
    "VertebraDBAPIModule",
    "VertebraDialectDescriptor",
    "build_delete_using",
    "chunked_selectinload",
    "compare_vertebra_type",
    "compile_tstring_sql",
    "configure_insertmanyvalues_sentinel",
    "register_alembic_impls",
    "register_sqlalchemy_dialects",
    "render_vertebra_item",
    "resolve_alembic_migration_url",
)
