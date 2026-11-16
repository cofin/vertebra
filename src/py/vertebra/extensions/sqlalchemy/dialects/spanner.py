"""SQLAlchemy 2.1 synchronous and asynchronous dialects for Cloud Spanner GoogleSQL and Spangres (+vtb / +vertebra)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.pool import NullPool

from vertebra.extensions.sqlalchemy.dialects._base import _AsyncDialectMixin, _SyncDialectMixin

try:
    from google.cloud.sqlalchemy_spanner import SpannerDialect as _BaseSpannerDialect
except ImportError:
    _BaseSpannerDialect = DefaultDialect

if TYPE_CHECKING:
    from sqlalchemy.engine.url import URL
    from sqlalchemy.pool import Pool

__all__ = (
    "AsyncSpangresDialect",
    "AsyncSpannerDialect",
    "SyncSpangresDialect",
    "SyncSpannerDialect",
)


class SyncSpannerDialect(_SyncDialectMixin, _BaseSpannerDialect):
    """SQLAlchemy 2.1 synchronous dialect for Cloud Spanner GoogleSQL (+vtb / +vertebra)."""

    name = "spanner"
    driver = "vtb"
    paramstyle = "named"
    default_paramstyle = "named"
    supports_statement_cache = True
    supports_native_jsonb = False
    positional = False

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return super().import_dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool so Cloud Spanner multiplexes over DatabaseRegistry gRPC channels."""
        _ = url
        return NullPool

    @classmethod
    def compile_insert_or_update(
        cls,
        table_name: str,
        columns: Sequence[str],
    ) -> str:
        """Compile a GoogleSQL `INSERT OR UPDATE INTO` statement with `@param` bindings."""
        if not columns:
            msg = "Spanner INSERT OR UPDATE requires at least one column"
            raise ValueError(msg)
        cols_str = ", ".join(columns)
        vals_str = ", ".join(f"@{c}" for c in columns)
        return f"INSERT OR UPDATE INTO {table_name} ({cols_str}) VALUES ({vals_str})"

    @classmethod
    def extract_interleave_options(cls, table_kwargs: Mapping[str, Any]) -> tuple[str | None, str]:
        """Extract `(parent_table, on_delete)` from SQLAlchemy table dialect options."""
        parent = table_kwargs.get("spanner_interleave_in_parent")
        on_delete = str(table_kwargs.get("spanner_interleave_on_delete") or "CASCADE").upper()
        if on_delete not in {"CASCADE", "NO ACTION"}:
            msg = f"Invalid spanner_interleave_on_delete option: {on_delete!r}"
            raise ValueError(msg)
        return (str(parent) if parent else None, on_delete)

    @classmethod
    def compile_interleave_clause(
        cls,
        parent_table: str,
        *,
        on_delete: str = "CASCADE",
    ) -> str:
        """Render a Cloud Spanner `INTERLEAVE IN PARENT` table suffix clause."""
        norm_delete = on_delete.upper()
        if norm_delete not in {"CASCADE", "NO ACTION"}:
            msg = f"Unsupported INTERLEAVE ON DELETE action: {on_delete!r}"
            raise ValueError(msg)
        return f", INTERLEAVE IN PARENT {parent_table} ON DELETE {norm_delete}"


class AsyncSpannerDialect(_AsyncDialectMixin, _BaseSpannerDialect):
    """SQLAlchemy 2.1 asynchronous dialect for Cloud Spanner GoogleSQL (+vtb_async / +vertebra_async)."""

    name = "spanner"
    driver = "vtb_async"
    is_async = True
    paramstyle = "named"
    default_paramstyle = "named"
    supports_statement_cache = True
    supports_native_jsonb = False
    positional = False

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return super().import_dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool so Cloud Spanner multiplexes over DatabaseRegistry gRPC channels."""
        _ = url
        return NullPool

    @classmethod
    def compile_insert_or_update(
        cls,
        table_name: str,
        columns: Sequence[str],
    ) -> str:
        """Compile a GoogleSQL `INSERT OR UPDATE INTO` statement with `@param` bindings."""
        return SyncSpannerDialect.compile_insert_or_update(table_name, columns)

    @classmethod
    def extract_interleave_options(cls, table_kwargs: Mapping[str, Any]) -> tuple[str | None, str]:
        """Extract `(parent_table, on_delete)` from SQLAlchemy table dialect options."""
        return SyncSpannerDialect.extract_interleave_options(table_kwargs)

    @classmethod
    def compile_interleave_clause(
        cls,
        parent_table: str,
        *,
        on_delete: str = "CASCADE",
    ) -> str:
        """Render a Cloud Spanner `INTERLEAVE IN PARENT` table suffix clause."""
        return SyncSpannerDialect.compile_interleave_clause(parent_table, on_delete=on_delete)


class SyncSpangresDialect(_SyncDialectMixin, PGDialect):
    """SQLAlchemy 2.1 synchronous dialect for Cloud Spanner Spangres (+vtb / +vertebra)."""

    name = "spangres"
    driver = "vtb"
    paramstyle = "pyformat"
    supports_statement_cache = True
    supports_native_jsonb = True

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return super().import_dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool so Cloud Spanner Spangres multiplexes over DatabaseRegistry gRPC channels."""
        _ = url
        return NullPool


class AsyncSpangresDialect(_AsyncDialectMixin, PGDialect):
    """SQLAlchemy 2.1 asynchronous dialect for Cloud Spanner Spangres (+vtb_async / +vertebra_async)."""

    name = "spangres"
    driver = "vtb_async"
    is_async = True
    paramstyle = "pyformat"
    supports_statement_cache = True
    supports_native_jsonb = True

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return super().import_dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool so Cloud Spanner Spangres multiplexes over DatabaseRegistry gRPC channels."""
        _ = url
        return NullPool
