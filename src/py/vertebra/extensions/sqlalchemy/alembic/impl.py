"""Alembic DefaultImpl subclasses and registration for Vertebra database dialects."""

from __future__ import annotations

import inspect
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal

import alembic.ddl.impl as alembic_impl_mod
from alembic.ddl.impl import DefaultImpl
from alembic.ddl.mssql import MSSQLImpl
from alembic.ddl.mysql import MySQLImpl
from alembic.ddl.oracle import OracleImpl
from alembic.ddl.postgresql import PostgresqlImpl
from alembic.ddl.sqlite import SQLiteImpl

from vertebra.db.driver._common import BaseDatabaseConfig
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from vertebra.db.dbapi import SyncDBAPIConnection
    from vertebra.extensions.sqlalchemy.alembic.ops import (
        CreateVectorIndexOp,
        CreateViewOp,
        DropVectorIndexOp,
        DropViewOp,
    )

__all__ = (
    "AlembicImpl",
    "BigQueryAlembicImpl",
    "DuckDBAlembicImpl",
    "MSSQLAlembicImpl",
    "MySQLAlembicImpl",
    "OracleAlembicImpl",
    "PostgreSQLAlembicImpl",
    "SQLiteAlembicImpl",
    "SpangresAlembicImpl",
    "SpannerAlembicImpl",
    "render_item",
)


class PostgreSQLAlembicImpl(PostgresqlImpl):
    """Alembic DefaultImpl for PostgreSQL +vtb and +vertebra dialects."""

    __dialect__ = "postgresql+vtb"
    transactional_ddl: bool = True


class MySQLAlembicImpl(MySQLImpl):
    """Alembic DefaultImpl for MySQL +vtb and +vertebra dialects."""

    __dialect__ = "mysql+vtb"
    transactional_ddl: bool = False


class SQLiteAlembicImpl(SQLiteImpl):
    """Alembic DefaultImpl for SQLite +vtb and +vertebra dialects."""

    __dialect__ = "sqlite+vtb"
    transactional_ddl: bool = True


class OracleAlembicImpl(OracleImpl):
    """Alembic DefaultImpl for Oracle +vtb and +vertebra dialects."""

    __dialect__ = "oracle+vtb"
    transactional_ddl: bool = False


class MSSQLAlembicImpl(MSSQLImpl):
    """Alembic DefaultImpl for MSSQL +vtb and +vertebra dialects."""

    __dialect__ = "mssql+vtb"
    transactional_ddl: bool = True


class DuckDBAlembicImpl(DefaultImpl):
    """Alembic DefaultImpl for DuckDB +vtb and +vertebra dialects."""

    __dialect__ = "duckdb+vtb"
    transactional_ddl: bool = True


class BigQueryAlembicImpl(DefaultImpl):
    """Alembic DefaultImpl for Google BigQuery +vtb and +vertebra dialects."""

    __dialect__ = "bigquery+vtb"
    transactional_ddl: bool = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize BigQueryAlembicImpl."""
        if args or kwargs:
            super().__init__(*args, **kwargs)


class SpannerAlembicImpl(DefaultImpl):
    """DDL-batching Alembic DefaultImpl for Cloud Spanner GoogleSQL +vtb and +vertebra dialects."""

    __dialect__ = "spanner+vtb"
    transactional_ddl: bool = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize SpannerAlembicImpl with an empty DDL batch buffer."""
        if args or kwargs:
            super().__init__(*args, **kwargs)
        self._ddl_buffer: list[str] = []

    @property
    def buffered_ddl(self) -> tuple[str, ...]:
        """Return the currently buffered DDL statements awaiting a batched UpdateDatabaseDdl LRO."""
        return tuple(self._ddl_buffer)

    def buffer_ddl(self, sql: str | Sequence[str]) -> None:
        """Append one or more DDL statements to the single-LRO migration batch buffer."""
        if isinstance(sql, str):
            stmt = sql.strip().rstrip(";")
            if stmt:
                self._ddl_buffer.append(stmt)
            return
        for item in sql:
            stmt = str(item).strip().rstrip(";")
            if stmt:
                self._ddl_buffer.append(stmt)

    def flush_ddl_batch_sync(self, driver: Any) -> Any:
        """Flush all buffered DDL statements in a single driver.update_ddl(statements) LRO call."""
        if not self._ddl_buffer:
            return None
        statements = list(self._ddl_buffer)
        self._ddl_buffer.clear()
        return driver.update_ddl(statements)

    async def flush_ddl_batch_async(self, driver: Any) -> Any:
        """Asynchronously flush all buffered DDL statements in a single driver.update_ddl(statements) LRO call."""
        if not self._ddl_buffer:
            return None
        statements = list(self._ddl_buffer)
        self._ddl_buffer.clear()
        res = driver.update_ddl(statements)
        if inspect.isawaitable(res):
            return await res
        return res


class SpangresAlembicImpl(SpannerAlembicImpl):
    """DDL-batching Alembic DefaultImpl for Cloud Spanner Spangres +vtb and +vertebra dialects."""

    __dialect__ = "spangres+vtb"
    transactional_ddl: bool = False


def _compare_type_impl(
    context: Any,
    inspected_column: Any,
    metadata_column: Any,
    inspected_type: Any = None,
    metadata_type: Any = None,
    *args: Any,
    **kwargs: Any,
) -> bool | None:
    """Alembic compare_type hook preventing false-positive diffs on Vector and BinaryJSON."""
    _ = (context, inspected_column, metadata_column, args, kwargs)
    sa_mod = ensure_module("vertebra.extensions.sqlalchemy")
    if isinstance(metadata_type, (sa_mod.Vector, sa_mod.PgVector, sa_mod.OracleVector, sa_mod.SpannerVector)):
        if isinstance(inspected_type, (sa_mod.Vector, sa_mod.PgVector, sa_mod.OracleVector, sa_mod.SpannerVector)):
            meta_dim = getattr(metadata_type, "dimensions", None)
            insp_dim = getattr(inspected_type, "dimensions", None)
            meta_prec = getattr(metadata_type, "precision", None)
            insp_prec = getattr(inspected_type, "precision", None)
            return bool((insp_dim != meta_dim) or (insp_prec != meta_prec))
        inspected_str = str(inspected_type).upper()
        if "VECTOR" in inspected_str:
            meta_dim = getattr(metadata_type, "dimensions", None)
            return bool(meta_dim is not None and str(meta_dim) not in inspected_str)
    if isinstance(metadata_type, (sa_mod.BinaryJSON, sa_mod.MsgspecJSON)):
        if isinstance(inspected_type, (sa_mod.BinaryJSON, sa_mod.MsgspecJSON)):
            return False
        inspected_str = str(inspected_type).upper()
        if inspected_str in ("JSON", "JSONB", "OSON"):
            return False
    return None


class AlembicImpl(DefaultImpl):
    """Base Alembic implementation and registration coordinator for Vertebra dialects."""

    __dialect__ = "vertebra"
    transactional_ddl: bool = True

    @classmethod
    def register_all(cls) -> dict[str, type[DefaultImpl]]:
        """Register all +vtb and +vertebra dialect implementations with Alembic."""
        impl_map: dict[str, type[DefaultImpl]] = {
            "postgresql+vertebra": PostgreSQLAlembicImpl,
            "postgresql+vtb": PostgreSQLAlembicImpl,
            "postgresql+vertebra_async": PostgreSQLAlembicImpl,
            "postgresql+vtb_async": PostgreSQLAlembicImpl,
            "oracle+vertebra": OracleAlembicImpl,
            "oracle+vtb": OracleAlembicImpl,
            "oracle+vertebra_async": OracleAlembicImpl,
            "oracle+vtb_async": OracleAlembicImpl,
            "mysql+vertebra": MySQLAlembicImpl,
            "mysql+vtb": MySQLAlembicImpl,
            "mysql+vertebra_async": MySQLAlembicImpl,
            "mysql+vtb_async": MySQLAlembicImpl,
            "sqlite+vertebra": SQLiteAlembicImpl,
            "sqlite+vtb": SQLiteAlembicImpl,
            "sqlite+vertebra_async": SQLiteAlembicImpl,
            "sqlite+vtb_async": SQLiteAlembicImpl,
            "mssql+vertebra": MSSQLAlembicImpl,
            "mssql+vtb": MSSQLAlembicImpl,
            "mssql+vertebra_async": MSSQLAlembicImpl,
            "mssql+vtb_async": MSSQLAlembicImpl,
            "duckdb": DuckDBAlembicImpl,
            "duckdb+vertebra": DuckDBAlembicImpl,
            "duckdb+vtb": DuckDBAlembicImpl,
            "duckdb+vertebra_async": DuckDBAlembicImpl,
            "duckdb+vtb_async": DuckDBAlembicImpl,
            "bigquery": BigQueryAlembicImpl,
            "bigquery+vertebra": BigQueryAlembicImpl,
            "bigquery+vtb": BigQueryAlembicImpl,
            "bigquery+vertebra_async": BigQueryAlembicImpl,
            "bigquery+vtb_async": BigQueryAlembicImpl,
            "spanner": SpannerAlembicImpl,
            "spanner+vertebra": SpannerAlembicImpl,
            "spanner+vtb": SpannerAlembicImpl,
            "spanner+vertebra_async": SpannerAlembicImpl,
            "spanner+vtb_async": SpannerAlembicImpl,
            "spangres": SpangresAlembicImpl,
            "spangres+vertebra": SpangresAlembicImpl,
            "spangres+vtb": SpangresAlembicImpl,
            "spangres+vertebra_async": SpangresAlembicImpl,
            "spangres+vtb_async": SpangresAlembicImpl,
        }
        for name, impl_cls in impl_map.items():
            alembic_impl_mod._impls[name] = impl_cls
        return impl_map

    @staticmethod
    def render_item(
        type_: str,
        obj: Any,
        autogen_context: Any,
    ) -> str | Literal[False]:
        """Alembic render_item hook rendering Vector and BinaryJSON column types."""
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
        if isinstance(obj, sa_mod.MsgspecJSON):
            if isinstance(imports, set):
                imports.add("from vertebra.extensions.sqlalchemy import MsgspecJSON")
            return "MsgspecJSON()"
        if isinstance(obj, sa_mod.BinaryJSON):
            if isinstance(imports, set):
                imports.add("from vertebra.extensions.sqlalchemy import BinaryJSON")
            return "BinaryJSON()"
        return False

    compare_type: Any = staticmethod(_compare_type_impl)

    @staticmethod
    def resolve_migration_url(url: BaseDatabaseConfig | str) -> str:
        """Return the direct migration DSN, bypassing transaction-mode poolers."""
        if isinstance(url, BaseDatabaseConfig):
            dsn = url.effective_direct_dsn
            if dsn:
                return dsn
        return str(url)

    @staticmethod
    def execute_ddl(
        connection: SyncDBAPIConnection | Any,
        op: CreateViewOp | DropViewOp | CreateVectorIndexOp | DropVectorIndexOp,
        *,
        dialect_name: str = "postgresql",
    ) -> None:
        """Compile an Alembic DDL operation and execute it on the connection."""
        sql = op.to_sql(dialect_name)
        if hasattr(connection, "cursor"):
            cur = connection.cursor()
            try:
                cur.execute(sql)
            finally:
                if hasattr(cur, "close"):
                    cur.close()
        elif hasattr(connection, "execute"):
            connection.execute(sql)


def render_item(
    type_: str,
    obj: Any,
    autogen_context: Any,
) -> str | Literal[False]:
    """Alembic render_item hook rendering Vector and BinaryJSON column types."""
    return AlembicImpl.render_item(type_, obj, autogen_context)


AlembicImpl.register_all()
