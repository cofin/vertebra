"""SQLAlchemy 2.1 +vertebra / +vtb sync and async dialects, PEP 249 DBAPI bridges, and 3.14t pool adapters."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING, Any

from sqlalchemy.dialects import registry
from sqlalchemy.pool import NullPool

from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from vertebra.extensions.sqlalchemy.dialects._base import (
        AsyncExecutionContext,
        SyncExecutionContext,
        _AsyncDialectMixin,
        _SyncDialectMixin,
    )
    from vertebra.extensions.sqlalchemy.dialects.bigquery import (
        AsyncBigQueryDialect,
        SyncBigQueryDialect,
    )
    from vertebra.extensions.sqlalchemy.dialects.duckdb import (
        AsyncDuckDBDialect,
        SyncDuckDBDialect,
    )
    from vertebra.extensions.sqlalchemy.dialects.mssql import (
        AsyncMSSQLDialect,
        SyncMSSQLDialect,
    )
    from vertebra.extensions.sqlalchemy.dialects.mysql import (
        AsyncMySQLDialect,
        SyncMySQLDialect,
    )
    from vertebra.extensions.sqlalchemy.dialects.oracle import (
        AsyncOracleDialect,
        SyncOracleDialect,
    )
    from vertebra.extensions.sqlalchemy.dialects.postgresql import (
        AsyncPostgreSQLDialect,
        SyncPostgreSQLDialect,
    )
    from vertebra.extensions.sqlalchemy.dialects.spanner import (
        AsyncSpangresDialect,
        AsyncSpannerDialect,
        SyncSpangresDialect,
        SyncSpannerDialect,
    )
    from vertebra.extensions.sqlalchemy.dialects.sqlite import (
        AsyncSQLiteDialect,
        SyncSQLiteDialect,
    )

__all__ = (
    "ALL_DIALECT_SCHEMES",
    "DIALECT_DESCRIPTORS",
    "DIALECT_SCHEMES",
    "AsyncBigQueryDialect",
    "AsyncDuckDBDialect",
    "AsyncExecutionContext",
    "AsyncMSSQLDialect",
    "AsyncMySQLDialect",
    "AsyncOracleDialect",
    "AsyncPostgreSQLDialect",
    "AsyncSQLiteDialect",
    "AsyncSpangresDialect",
    "AsyncSpannerDialect",
    "NullPool",
    "SyncBigQueryDialect",
    "SyncDuckDBDialect",
    "SyncExecutionContext",
    "SyncMSSQLDialect",
    "SyncMySQLDialect",
    "SyncOracleDialect",
    "SyncPostgreSQLDialect",
    "SyncSQLiteDialect",
    "SyncSpangresDialect",
    "SyncSpannerDialect",
    "_AsyncDialectMixin",
    "_SyncDialectMixin",
    "register_sqlalchemy_dialects",
)

DIALECT_SCHEMES: tuple[str, ...] = (
    "postgresql+vertebra",
    "postgresql+vtb",
    "oracle+vertebra",
    "oracle+vtb",
    "mysql+vertebra",
    "mysql+vtb",
    "sqlite+vertebra",
    "sqlite+vtb",
)

ALL_DIALECT_SCHEMES: tuple[str, ...] = (
    *DIALECT_SCHEMES,
    "postgresql+vertebra_async",
    "postgresql+vtb_async",
    "oracle+vertebra_async",
    "oracle+vtb_async",
    "mysql+vertebra_async",
    "mysql+vtb_async",
    "sqlite+vertebra_async",
    "sqlite+vtb_async",
    "mssql+vertebra",
    "mssql+vtb",
    "mssql+vertebra_async",
    "mssql+vtb_async",
    "duckdb+vertebra",
    "duckdb+vtb",
    "duckdb+vertebra_async",
    "duckdb+vtb_async",
    "bigquery+vertebra",
    "bigquery+vtb",
    "bigquery+vertebra_async",
    "bigquery+vtb_async",
    "spanner+vertebra",
    "spanner+vtb",
    "spanner+vertebra_async",
    "spanner+vtb_async",
    "spangres+vertebra",
    "spangres+vtb",
    "spangres+vertebra_async",
    "spangres+vtb_async",
)

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "AsyncBigQueryDialect": ("vertebra.extensions.sqlalchemy.dialects.bigquery", "AsyncBigQueryDialect"),
    "AsyncDuckDBDialect": ("vertebra.extensions.sqlalchemy.dialects.duckdb", "AsyncDuckDBDialect"),
    "AsyncExecutionContext": ("vertebra.extensions.sqlalchemy.dialects._base", "AsyncExecutionContext"),
    "AsyncMSSQLDialect": ("vertebra.extensions.sqlalchemy.dialects.mssql", "AsyncMSSQLDialect"),
    "AsyncMySQLDialect": ("vertebra.extensions.sqlalchemy.dialects.mysql", "AsyncMySQLDialect"),
    "AsyncOracleDialect": ("vertebra.extensions.sqlalchemy.dialects.oracle", "AsyncOracleDialect"),
    "AsyncPostgreSQLDialect": ("vertebra.extensions.sqlalchemy.dialects.postgresql", "AsyncPostgreSQLDialect"),
    "AsyncSQLiteDialect": ("vertebra.extensions.sqlalchemy.dialects.sqlite", "AsyncSQLiteDialect"),
    "AsyncSpangresDialect": ("vertebra.extensions.sqlalchemy.dialects.spanner", "AsyncSpangresDialect"),
    "AsyncSpannerDialect": ("vertebra.extensions.sqlalchemy.dialects.spanner", "AsyncSpannerDialect"),
    "SyncBigQueryDialect": ("vertebra.extensions.sqlalchemy.dialects.bigquery", "SyncBigQueryDialect"),
    "SyncDuckDBDialect": ("vertebra.extensions.sqlalchemy.dialects.duckdb", "SyncDuckDBDialect"),
    "SyncExecutionContext": ("vertebra.extensions.sqlalchemy.dialects._base", "SyncExecutionContext"),
    "SyncMSSQLDialect": ("vertebra.extensions.sqlalchemy.dialects.mssql", "SyncMSSQLDialect"),
    "SyncMySQLDialect": ("vertebra.extensions.sqlalchemy.dialects.mysql", "SyncMySQLDialect"),
    "SyncOracleDialect": ("vertebra.extensions.sqlalchemy.dialects.oracle", "SyncOracleDialect"),
    "SyncPostgreSQLDialect": ("vertebra.extensions.sqlalchemy.dialects.postgresql", "SyncPostgreSQLDialect"),
    "SyncSQLiteDialect": ("vertebra.extensions.sqlalchemy.dialects.sqlite", "SyncSQLiteDialect"),
    "SyncSpangresDialect": ("vertebra.extensions.sqlalchemy.dialects.spanner", "SyncSpangresDialect"),
    "SyncSpannerDialect": ("vertebra.extensions.sqlalchemy.dialects.spanner", "SyncSpannerDialect"),
    "_AsyncDialectMixin": ("vertebra.extensions.sqlalchemy.dialects._base", "_AsyncDialectMixin"),
    "_SyncDialectMixin": ("vertebra.extensions.sqlalchemy.dialects._base", "_SyncDialectMixin"),
}

_DIALECT_REGISTRATIONS: tuple[tuple[str, str, str], ...] = (
    ("postgresql.vtb", "vertebra.extensions.sqlalchemy.dialects.postgresql", "SyncPostgreSQLDialect"),
    ("postgresql.vertebra", "vertebra.extensions.sqlalchemy.dialects.postgresql", "SyncPostgreSQLDialect"),
    ("postgresql.vtb_async", "vertebra.extensions.sqlalchemy.dialects.postgresql", "AsyncPostgreSQLDialect"),
    ("postgresql.vertebra_async", "vertebra.extensions.sqlalchemy.dialects.postgresql", "AsyncPostgreSQLDialect"),
    ("mysql.vtb", "vertebra.extensions.sqlalchemy.dialects.mysql", "SyncMySQLDialect"),
    ("mysql.vertebra", "vertebra.extensions.sqlalchemy.dialects.mysql", "SyncMySQLDialect"),
    ("mysql.vtb_async", "vertebra.extensions.sqlalchemy.dialects.mysql", "AsyncMySQLDialect"),
    ("mysql.vertebra_async", "vertebra.extensions.sqlalchemy.dialects.mysql", "AsyncMySQLDialect"),
    ("sqlite.vtb", "vertebra.extensions.sqlalchemy.dialects.sqlite", "SyncSQLiteDialect"),
    ("sqlite.vertebra", "vertebra.extensions.sqlalchemy.dialects.sqlite", "SyncSQLiteDialect"),
    ("sqlite.vtb_async", "vertebra.extensions.sqlalchemy.dialects.sqlite", "AsyncSQLiteDialect"),
    ("sqlite.vertebra_async", "vertebra.extensions.sqlalchemy.dialects.sqlite", "AsyncSQLiteDialect"),
    ("oracle.vtb", "vertebra.extensions.sqlalchemy.dialects.oracle", "SyncOracleDialect"),
    ("oracle.vertebra", "vertebra.extensions.sqlalchemy.dialects.oracle", "SyncOracleDialect"),
    ("oracle.vtb_async", "vertebra.extensions.sqlalchemy.dialects.oracle", "AsyncOracleDialect"),
    ("oracle.vertebra_async", "vertebra.extensions.sqlalchemy.dialects.oracle", "AsyncOracleDialect"),
    ("mssql.vtb", "vertebra.extensions.sqlalchemy.dialects.mssql", "SyncMSSQLDialect"),
    ("mssql.vertebra", "vertebra.extensions.sqlalchemy.dialects.mssql", "SyncMSSQLDialect"),
    ("mssql.vtb_async", "vertebra.extensions.sqlalchemy.dialects.mssql", "AsyncMSSQLDialect"),
    ("mssql.vertebra_async", "vertebra.extensions.sqlalchemy.dialects.mssql", "AsyncMSSQLDialect"),
    ("duckdb.vtb", "vertebra.extensions.sqlalchemy.dialects.duckdb", "SyncDuckDBDialect"),
    ("duckdb.vertebra", "vertebra.extensions.sqlalchemy.dialects.duckdb", "SyncDuckDBDialect"),
    ("duckdb.vtb_async", "vertebra.extensions.sqlalchemy.dialects.duckdb", "AsyncDuckDBDialect"),
    ("duckdb.vertebra_async", "vertebra.extensions.sqlalchemy.dialects.duckdb", "AsyncDuckDBDialect"),
    ("bigquery.vtb", "vertebra.extensions.sqlalchemy.dialects.bigquery", "SyncBigQueryDialect"),
    ("bigquery.vertebra", "vertebra.extensions.sqlalchemy.dialects.bigquery", "SyncBigQueryDialect"),
    ("bigquery.vtb_async", "vertebra.extensions.sqlalchemy.dialects.bigquery", "AsyncBigQueryDialect"),
    ("bigquery.vertebra_async", "vertebra.extensions.sqlalchemy.dialects.bigquery", "AsyncBigQueryDialect"),
    ("spanner.vtb", "vertebra.extensions.sqlalchemy.dialects.spanner", "SyncSpannerDialect"),
    ("spanner.vertebra", "vertebra.extensions.sqlalchemy.dialects.spanner", "SyncSpannerDialect"),
    ("spanner.vtb_async", "vertebra.extensions.sqlalchemy.dialects.spanner", "AsyncSpannerDialect"),
    ("spanner.vertebra_async", "vertebra.extensions.sqlalchemy.dialects.spanner", "AsyncSpannerDialect"),
    ("spangres.vtb", "vertebra.extensions.sqlalchemy.dialects.spanner", "SyncSpangresDialect"),
    ("spangres.vertebra", "vertebra.extensions.sqlalchemy.dialects.spanner", "SyncSpangresDialect"),
    ("spangres.vtb_async", "vertebra.extensions.sqlalchemy.dialects.spanner", "AsyncSpangresDialect"),
    ("spangres.vertebra_async", "vertebra.extensions.sqlalchemy.dialects.spanner", "AsyncSpangresDialect"),
)


def register_sqlalchemy_dialects() -> None:
    """Register all 36 Vertebra dialect variants into SQLAlchemy's dialect registry."""
    for name, mod, cls_name in _DIALECT_REGISTRATIONS:
        registry.register(name, mod, cls_name)


def __getattr__(name: str) -> Any:
    """Lazily import and resolve dialect symbols on demand."""
    target = _LAZY_EXPORTS.get(name)
    if target is not None:
        mod_name, attr_name = target
        mod = ensure_module(mod_name)
        val = getattr(mod, attr_name)
        globals()[name] = val
        return val
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


class _DialectDescriptorRegistry(Mapping[str, type[Any]]):
    """Lazy mapping of SQLAlchemy scheme strings to dialect classes without eager third-party imports."""

    __slots__ = ("_cache", "_targets")

    def __init__(self, registrations: tuple[tuple[str, str, str], ...]) -> None:
        self._targets: dict[str, tuple[str, str]] = {
            reg_name.replace(".", "+"): (mod_name, cls_name) for reg_name, mod_name, cls_name in registrations
        }
        self._cache: dict[str, type[Any]] = {}

    def __getitem__(self, key: str) -> type[Any]:
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        target = self._targets.get(key)
        if target is None:
            raise KeyError(key)
        mod_name, cls_name = target
        mod = ensure_module(mod_name)
        resolved: type[Any] = getattr(mod, cls_name)
        self._cache[key] = resolved
        return resolved

    def __iter__(self) -> Iterator[str]:
        return iter(ALL_DIALECT_SCHEMES)

    def __len__(self) -> int:
        return len(ALL_DIALECT_SCHEMES)

    def __contains__(self, key: object) -> bool:
        return isinstance(key, str) and key in self._targets


DIALECT_DESCRIPTORS: Mapping[str, type[Any]] = _DialectDescriptorRegistry(_DIALECT_REGISTRATIONS)


register_sqlalchemy_dialects()
