"""Alembic DefaultImpl subclasses, Vector/BinaryJSON autogenerate hooks, and view/index DDL operations."""

from __future__ import annotations

import inspect
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal

import alembic.ddl.impl as alembic_impl_mod
import msgspec
from alembic.ddl.impl import DefaultImpl
from alembic.ddl.mssql import MSSQLImpl
from alembic.ddl.mysql import MySQLImpl
from alembic.ddl.oracle import OracleImpl
from alembic.ddl.postgresql import PostgresqlImpl
from alembic.ddl.sqlite import SQLiteImpl

from vertebra.db.adapters.mysql import MySQLConfig
from vertebra.db.adapters.postgres import PostgreSQLConfig
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from vertebra.extensions.sqlalchemy.dialects import SyncDBAPIConnection

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
    "SpannerAlembicImpl",
)


class CreateVectorIndexOp(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """SQLAlchemy 2.1 CreateVectorIndex DDL operation for Alembic migrations."""

    index_name: str
    """Identifier of the vector index to create."""
    table_name: str
    """Target table containing the vector column."""
    column_name: str
    """Target vector column to index."""
    metric: str = "cosine"
    """Distance metric (`cosine`, `l2`, or `inner_product`)."""
    storing: tuple[str, ...] = ()
    """Optional `STORING (...)` columns included in Cloud Spanner ScaNN indexes."""
    not_null_filtered: bool = True
    """Whether to include `WHERE <column> IS NOT NULL` on Cloud Spanner ScaNN indexes."""

    def to_sql(self, dialect_name: str = "postgresql") -> str:
        """Render the dialect-aware CREATE VECTOR INDEX statement."""
        base = dialect_name.split("+", 1)[0].lower()
        if base == "spanner":
            sp_metric = {
                "cosine": "COSINE",
                "l2": "EUCLIDEAN",
                "inner_product": "DOT_PRODUCT",
            }.get(self.metric.lower(), self.metric.upper())
            storing_clause = f" STORING ({', '.join(self.storing)})" if self.storing else ""
            where_clause = f" WHERE {self.column_name} IS NOT NULL" if self.not_null_filtered else ""
            return (
                f"CREATE VECTOR INDEX {self.index_name} ON {self.table_name} "
                f"({self.column_name}){storing_clause}{where_clause} "
                f"OPTIONS (tree_type = 'SCANN', distance_type = '{sp_metric}')"
            )
        if base == "bigquery":
            bq_metric = {
                "cosine": "COSINE",
                "l2": "EUCLIDEAN",
                "inner_product": "DOT_PRODUCT",
            }.get(self.metric.lower(), self.metric.upper())
            return (
                f"CREATE VECTOR INDEX {self.index_name} ON {self.table_name}({self.column_name}) "
                f"OPTIONS(index_type = 'IVF', distance_type = '{bq_metric}')"
            )
        if base == "oracle":
            metric_kw = {
                "cosine": "DISTANCE COSINE",
                "l2": "DISTANCE EUCLIDEAN",
                "inner_product": "DISTANCE DOT",
            }.get(self.metric.lower(), "DISTANCE COSINE")
            return (
                f"CREATE VECTOR INDEX {self.index_name} ON {self.table_name} "
                f"({self.column_name}) ORGANIZATION INMEMORY NEIGHBOR GRAPH {metric_kw}"
            )
        ops = {
            "cosine": "vector_cosine_ops",
            "l2": "vector_l2_ops",
            "inner_product": "vector_ip_ops",
        }.get(self.metric.lower(), "vector_cosine_ops")
        return f"CREATE INDEX {self.index_name} ON {self.table_name} USING hnsw ({self.column_name} {ops})"

    def reverse(self) -> DropVectorIndexOp:
        """Return the inverse DropVectorIndexOp."""
        return DropVectorIndexOp(
            index_name=self.index_name,
            table_name=self.table_name,
        )


class DropVectorIndexOp(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """SQLAlchemy 2.1 DropVectorIndex DDL operation for Alembic migrations."""

    index_name: str
    """Identifier of the vector index to drop."""
    table_name: str = ""
    """Optional table name when required by the target dialect."""
    if_exists: bool = True
    """Whether to emit `IF EXISTS` in the `DROP INDEX` statement."""

    def to_sql(self, dialect_name: str = "postgresql") -> str:
        """Render the dialect-aware DROP INDEX statement."""
        base = dialect_name.split("+", 1)[0].lower()
        if base == "oracle":
            return f"DROP INDEX {self.index_name}"
        if base == "bigquery" and self.table_name:
            exists_bq = "IF EXISTS " if self.if_exists else ""
            return f"DROP VECTOR INDEX {exists_bq}{self.index_name} ON {self.table_name}"
        if base == "spanner":
            return f"DROP VECTOR INDEX {self.index_name}"
        exists_clause = "IF EXISTS " if self.if_exists else ""
        return f"DROP INDEX {exists_clause}{self.index_name}"


class CreateViewOp(msgspec.Struct, frozen=True, gc=False):
    """SQLAlchemy 2.1 CreateView DDL operation for Alembic migrations."""

    view_name: str
    """Identifier of the view or materialized view to create."""
    select_sql: str
    """Defining `SELECT` query for the view body."""
    or_replace: bool = False
    """Whether to emit `CREATE OR REPLACE VIEW`."""
    materialized: bool = False
    """Whether to create a `MATERIALIZED VIEW`."""

    def to_sql(self, dialect_name: str = "postgresql") -> str:
        """Render the dialect-aware CREATE VIEW DDL statement."""
        base = dialect_name.split("+", 1)[0].lower()
        if self.materialized:
            return f"CREATE MATERIALIZED VIEW {self.view_name} AS {self.select_sql}"
        replace_clause = "OR REPLACE " if self.or_replace and base != "sqlite" else ""
        return f"CREATE {replace_clause}VIEW {self.view_name} AS {self.select_sql}"

    def reverse(self) -> DropViewOp:
        """Return the inverse DropViewOp for Alembic downgrade migrations."""
        return DropViewOp(view_name=self.view_name, materialized=self.materialized)


class DropViewOp(msgspec.Struct, frozen=True, gc=False):
    """SQLAlchemy 2.1 DropView DDL operation for Alembic migrations."""

    view_name: str
    """Identifier of the view or materialized view to drop."""
    if_exists: bool = True
    """Whether to emit `IF EXISTS` in the `DROP VIEW` statement."""
    materialized: bool = False
    """Whether the target is a `MATERIALIZED VIEW`."""

    def to_sql(self, dialect_name: str = "postgresql") -> str:
        """Render the dialect-aware DROP VIEW DDL statement."""
        base = dialect_name.split("+", 1)[0].lower()
        mat = "MATERIALIZED " if self.materialized else ""
        exists_clause = "IF EXISTS " if self.if_exists and base != "oracle" else ""
        return f"DROP {mat}VIEW {exists_clause}{self.view_name}"


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
    """DDL-batching Alembic DefaultImpl for Cloud Spanner +vtb and +vertebra dialects."""

    __dialect__ = "spanner+vtb"
    transactional_ddl: bool = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize SpannerAlembicImpl with an empty DDL batch buffer."""
        if args or kwargs:
            super().__init__(*args, **kwargs)
        self._ddl_buffer: list[str] = []

    @property
    def buffered_ddl(self) -> tuple[str, ...]:
        """Return the currently buffered DDL statements awaiting a batched `UpdateDatabaseDdl` LRO."""
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
        """Flush all buffered DDL statements in a single `driver.update_ddl(statements)` LRO call."""
        if not self._ddl_buffer:
            return None
        statements = list(self._ddl_buffer)
        self._ddl_buffer.clear()
        return driver.update_ddl(statements)

    async def flush_ddl_batch_async(self, driver: Any) -> Any:
        """Asynchronously flush all buffered DDL statements in a single `driver.update_ddl(statements)` LRO call."""
        if not self._ddl_buffer:
            return None
        statements = list(self._ddl_buffer)
        self._ddl_buffer.clear()
        res = driver.update_ddl(statements)
        if inspect.isawaitable(res):
            return await res
        return res


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
    if isinstance(metadata_type, sa_mod.Vector):
        if isinstance(inspected_type, sa_mod.Vector):
            return bool(
                (inspected_type.dimensions != metadata_type.dimensions)
                or (inspected_type.precision != metadata_type.precision)
            )
        inspected_str = str(inspected_type).upper()
        if "VECTOR" in inspected_str:
            return bool(metadata_type.dimensions is not None and str(metadata_type.dimensions) not in inspected_str)
    if isinstance(metadata_type, sa_mod.BinaryJSON):
        if isinstance(inspected_type, sa_mod.BinaryJSON):
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
            "duckdb+vertebra": DuckDBAlembicImpl,
            "duckdb+vtb": DuckDBAlembicImpl,
            "duckdb+vertebra_async": DuckDBAlembicImpl,
            "duckdb+vtb_async": DuckDBAlembicImpl,
            "bigquery+vertebra": BigQueryAlembicImpl,
            "bigquery+vtb": BigQueryAlembicImpl,
            "bigquery+vertebra_async": BigQueryAlembicImpl,
            "bigquery+vtb_async": BigQueryAlembicImpl,
            "spanner+vertebra": SpannerAlembicImpl,
            "spanner+vtb": SpannerAlembicImpl,
            "spanner+vertebra_async": SpannerAlembicImpl,
            "spanner+vtb_async": SpannerAlembicImpl,
            "spangres+vertebra": SpannerAlembicImpl,
            "spangres+vtb": SpannerAlembicImpl,
            "spangres+vertebra_async": SpannerAlembicImpl,
            "spangres+vtb_async": SpannerAlembicImpl,
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
        if isinstance(obj, sa_mod.BinaryJSON):
            if isinstance(imports, set):
                imports.add("from vertebra.extensions.sqlalchemy import BinaryJSON")
            return "BinaryJSON()"
        return False

    compare_type: Any = staticmethod(_compare_type_impl)

    @staticmethod
    def resolve_migration_url(url: PostgreSQLConfig | MySQLConfig | str) -> str:
        """Return the direct migration DSN, bypassing transaction-mode poolers."""
        if isinstance(url, PostgreSQLConfig):
            return url.effective_direct_dsn
        if isinstance(url, MySQLConfig):
            dsn = url.dsn
            if dsn.startswith("mysql://"):
                return "mysql+vtb://" + dsn[len("mysql://") :]
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


AlembicImpl.register_all()
