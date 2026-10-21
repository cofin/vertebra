"""Alembic DefaultImpl registrations, Vector/BinaryJson autogenerate hooks, and CreateView DDL."""

from __future__ import annotations

import importlib
from typing import Any, Literal

import msgspec

from vertebra.db.postgres import PostgresConfig
from vertebra.extensions.sqlalchemy.types import BinaryJson, Vector

__all__ = (
    "AlembicImpl",
    "CreateViewOp",
    "DropViewOp",
)


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
        imports = getattr(autogen_context, "imports", None)
        if isinstance(obj, Vector):
            if isinstance(imports, set):
                imports.add("from vertebra.extensions.sqlalchemy import Vector")
            if obj.dim is None:
                return f"Vector(storage_format={obj.storage_format!r})"
            return f"Vector({obj.dim}, storage_format={obj.storage_format!r})"
        if isinstance(obj, BinaryJson):
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
        if isinstance(metadata_type, Vector):
            if isinstance(inspected_type, Vector):
                return (inspected_type.dim != metadata_type.dim) or (
                    inspected_type.storage_format != metadata_type.storage_format
                )
            inspected_str = str(inspected_type).upper()
            if "VECTOR" in inspected_str:
                return bool(metadata_type.dim is not None and str(metadata_type.dim) not in inspected_str)
        if isinstance(metadata_type, BinaryJson):
            if isinstance(inspected_type, BinaryJson):
                return False
            inspected_str = str(inspected_type).upper()
            if inspected_str in ("JSON", "JSONB", "OSON"):
                return False
        return None

    @staticmethod
    def resolve_migration_url(url: PostgresConfig | str) -> str:
        """Return the direct migration DSN, bypassing transaction-mode poolers (PgBouncer/PgCat/pgdog)."""
        if isinstance(url, PostgresConfig):
            return url.effective_direct_dsn
        return url


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
