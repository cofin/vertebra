"""Alembic DefaultImpl registrations, Vector/BinaryJson autogenerate hooks, and CreateView DDL."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any, Literal

import msgspec

from vertebra.db.mysql import MysqlConfig
from vertebra.db.oracle import OracleConfig
from vertebra.db.postgres import PostgresConfig

if TYPE_CHECKING:
    from vertebra.extensions.sqlalchemy.dialects import DBAPIConnection

__all__ = (
    "AlembicImpl",
    "AlembicRunner",
    "CreateVectorIndexOp",
    "CreateViewOp",
    "DropVectorIndexOp",
    "DropViewOp",
)


class AlembicRunner:
    """Wrapper around alembic.command for executing programmatic migrations."""

    __slots__ = ("_config",)

    def __init__(self, config: Any = None) -> None:
        """Initialize runner with optional Alembic configuration."""
        self._config = config

    def upgrade(self, revision: str = "head") -> None:
        """Run alembic upgrade to revision."""
        try:
            cmd = importlib.import_module("alembic.command")
            cfg_mod = importlib.import_module("alembic.config")

            cfg = self._config if isinstance(self._config, cfg_mod.Config) else cfg_mod.Config()
            cmd.upgrade(cfg, revision)
        except (ImportError, AttributeError):
            pass

    def downgrade(self, revision: str = "-1") -> None:
        """Run alembic downgrade to revision."""
        try:
            cmd = importlib.import_module("alembic.command")
            cfg_mod = importlib.import_module("alembic.config")

            cfg = self._config if isinstance(self._config, cfg_mod.Config) else cfg_mod.Config()
            cmd.downgrade(cfg, revision)
        except (ImportError, AttributeError):
            pass


class AlembicImpl(msgspec.Struct, frozen=True, gc=False):
    """Alembic ``DefaultImpl`` descriptor for ``+vertebra`` and ``+vtb`` dialects."""

    __dialect__: str
    transactional_ddl: bool = True
    type_synonyms: tuple[frozenset[str], ...] = (
        frozenset({"JSON", "JSONB"}),
        frozenset({"VECTOR"}),
    )

    @classmethod
    def register_all(cls) -> dict[str, AlembicImpl]:
        """Register ``+vertebra`` and ``+vtb`` dialect implementations with Alembic when installed."""
        impls: dict[str, AlembicImpl] = {
            "postgresql+vertebra": cls(__dialect__="postgresql", transactional_ddl=True),
            "postgresql+vtb": cls(__dialect__="postgresql", transactional_ddl=True),
            "oracle+vertebra": cls(__dialect__="oracle", transactional_ddl=False),
            "oracle+vtb": cls(__dialect__="oracle", transactional_ddl=False),
            "mysql+vertebra": cls(__dialect__="mysql", transactional_ddl=False),
            "mysql+vtb": cls(__dialect__="mysql", transactional_ddl=False),
            "sqlite+vertebra": cls(__dialect__="sqlite", transactional_ddl=True),
            "sqlite+vtb": cls(__dialect__="sqlite", transactional_ddl=True),
        }
        try:
            impl_mod: Any = importlib.import_module("alembic.ddl.impl")
            default_impl = getattr(impl_mod, "DefaultImpl", None)
            i_map = getattr(default_impl, "_impls", None) if default_impl is not None else None
            if isinstance(i_map, dict):
                for key, desc in impls.items():
                    base_impl = i_map.get(desc.__dialect__)
                    if base_impl is not None:
                        i_map.setdefault(key, base_impl)
        except ImportError:
            pass
        return impls

    @staticmethod
    def render_item(
        type_: str,
        obj: Any,
        autogen_context: Any,
    ) -> str | Literal[False]:
        """Alembic ``render_item`` hook rendering ``Vector`` and ``BinaryJson`` column types."""
        if type_ != "type":
            return False
        sa_mod = importlib.import_module("vertebra.extensions.sqlalchemy")
        imports = getattr(autogen_context, "imports", None)
        if isinstance(obj, sa_mod.Vector):
            if isinstance(imports, set):
                imports.add("from vertebra.extensions.sqlalchemy import Vector")
            if obj.dim is None:
                return f"Vector(storage_format={obj.storage_format!r})"
            return f"Vector({obj.dim}, storage_format={obj.storage_format!r})"
        if isinstance(obj, sa_mod.BinaryJson):
            if isinstance(imports, set):
                imports.add("from vertebra.extensions.sqlalchemy import BinaryJson")
            return "BinaryJson()"
        return False

    @staticmethod
    def compare_type(
        context: Any,
        inspected_column: Any,
        metadata_column: Any,
        inspected_type: Any,
        metadata_type: Any,
    ) -> bool | None:
        """Alembic ``compare_type`` hook preventing false-positive diffs on ``Vector`` and ``BinaryJson``."""
        _ = (context, inspected_column, metadata_column)
        sa_mod = importlib.import_module("vertebra.extensions.sqlalchemy")
        if isinstance(metadata_type, sa_mod.Vector):
            if isinstance(inspected_type, sa_mod.Vector):
                return bool(
                    (inspected_type.dim != metadata_type.dim)
                    or (inspected_type.storage_format != metadata_type.storage_format)
                )
            inspected_str = str(inspected_type).upper()
            if "VECTOR" in inspected_str:
                return bool(metadata_type.dim is not None and str(metadata_type.dim) not in inspected_str)
        if isinstance(metadata_type, sa_mod.BinaryJson):
            if isinstance(inspected_type, sa_mod.BinaryJson):
                return False
            inspected_str = str(inspected_type).upper()
            if inspected_str in ("JSON", "JSONB", "OSON"):
                return False
        return None

    @staticmethod
    def resolve_migration_url(url: PostgresConfig | OracleConfig | MysqlConfig | str) -> str:
        """Return the direct migration DSN, bypassing transaction-mode poolers (PgBouncer/PgCat/pgdog)."""
        if isinstance(url, PostgresConfig):
            return url.effective_direct_dsn
        if isinstance(url, OracleConfig):
            auth = f"{url.user}:{url.password}"
            return f"oracle+vtb://{auth}@{url.host}:{url.port}/{url.service_name}"
        if isinstance(url, MysqlConfig):
            dsn = url.dsn
            if dsn.startswith("mysql://"):
                return "mysql+vtb://" + dsn[len("mysql://") :]
            return dsn
        return str(url)

    @staticmethod
    def execute_ddl(
        connection: DBAPIConnection | Any,
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


class CreateVectorIndexOp(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """SQLAlchemy 2.1 CreateVectorIndex DDL operation for Alembic migrations."""

    index_name: str
    table_name: str
    column_name: str
    metric: str = "cosine"

    def to_sql(self, dialect_name: str = "postgresql") -> str:
        """Render the dialect-aware CREATE VECTOR INDEX statement."""
        base = dialect_name.split("+", 1)[0].lower()
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
    table_name: str = ""
    if_exists: bool = True

    def to_sql(self, dialect_name: str = "postgresql") -> str:
        """Render the dialect-aware DROP INDEX statement."""
        base = dialect_name.split("+", 1)[0].lower()
        if base == "oracle":
            return f"DROP INDEX {self.index_name}"
        exists_clause = "IF EXISTS " if self.if_exists else ""
        return f"DROP INDEX {exists_clause}{self.index_name}"


class CreateViewOp(msgspec.Struct, frozen=True, gc=False):
    """SQLAlchemy 2.1 ``CreateView`` DDL operation for Alembic migrations."""

    view_name: str
    select_sql: str
    or_replace: bool = False
    materialized: bool = False

    def to_sql(self, dialect_name: str = "postgresql") -> str:
        """Render the dialect-aware ``CREATE VIEW`` DDL statement."""
        base = dialect_name.split("+", 1)[0].lower()
        if self.materialized:
            return f"CREATE MATERIALIZED VIEW {self.view_name} AS {self.select_sql}"
        replace_clause = "OR REPLACE " if self.or_replace and base != "sqlite" else ""
        return f"CREATE {replace_clause}VIEW {self.view_name} AS {self.select_sql}"

    def reverse(self) -> DropViewOp:
        """Return the inverse ``DropViewOp`` for Alembic downgrade migrations."""
        return DropViewOp(view_name=self.view_name, materialized=self.materialized)


class DropViewOp(msgspec.Struct, frozen=True, gc=False):
    """SQLAlchemy 2.1 ``DropView`` DDL operation for Alembic migrations."""

    view_name: str
    if_exists: bool = True
    materialized: bool = False

    def to_sql(self, dialect_name: str = "postgresql") -> str:
        """Render the dialect-aware ``DROP VIEW`` DDL statement."""
        base = dialect_name.split("+", 1)[0].lower()
        mat = "MATERIALIZED " if self.materialized else ""
        exists_clause = "IF EXISTS " if self.if_exists and base != "oracle" else ""
        return f"DROP {mat}VIEW {exists_clause}{self.view_name}"
