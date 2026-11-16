"""SQLAlchemy 2.1 synchronous and asynchronous dialects for Google BigQuery (+vtb / +vertebra)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.pool import NullPool

from vertebra.extensions.sqlalchemy.dialects._base import _AsyncDialectMixin, _SyncDialectMixin

try:
    from sqlalchemy_bigquery import BigQueryDialect as _BaseBigQueryDialect
except ImportError:
    _BaseBigQueryDialect = DefaultDialect

if TYPE_CHECKING:
    from sqlalchemy.engine.url import URL
    from sqlalchemy.pool import Pool

__all__ = (
    "AsyncBigQueryDialect",
    "SyncBigQueryDialect",
)


class SyncBigQueryDialect(_SyncDialectMixin, _BaseBigQueryDialect):
    """SQLAlchemy 2.1 synchronous dialect for Google BigQuery (+vtb / +vertebra)."""

    name = "bigquery"
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
        """Return NullPool so BigQuery delegates connection pooling to DatabaseRegistry."""
        _ = url
        return NullPool

    @classmethod
    def compile_merge_upsert(
        cls,
        target_table: str,
        source_table: str,
        on_keys: Sequence[str],
        update_columns: Sequence[str],
        insert_columns: Sequence[str] | None = None,
    ) -> str:
        """Compile a GoogleSQL `MERGE INTO` upsert statement for BigQuery."""
        if not on_keys:
            msg = "BigQuery MERGE upsert requires at least one key column in on_keys"
            raise ValueError(msg)
        resolved_insert = list(insert_columns) if insert_columns is not None else [*on_keys, *update_columns]
        on_clause = " AND ".join(f"T.{k} = S.{k}" for k in on_keys)
        set_clause = ", ".join(f"T.{c} = S.{c}" for c in update_columns)
        ins_cols = ", ".join(resolved_insert)
        ins_vals = ", ".join(f"S.{c}" for c in resolved_insert)
        return (
            f"MERGE INTO {target_table} AS T "
            f"USING {source_table} AS S "
            f"ON {on_clause} "
            f"WHEN MATCHED THEN UPDATE SET {set_clause} "
            f"WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals})"
        )


class AsyncBigQueryDialect(_AsyncDialectMixin, _BaseBigQueryDialect):
    """SQLAlchemy 2.1 asynchronous dialect for Google BigQuery (+vtb_async / +vertebra_async)."""

    name = "bigquery"
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
        """Return NullPool so BigQuery delegates connection pooling to DatabaseRegistry."""
        _ = url
        return NullPool

    @classmethod
    def compile_merge_upsert(
        cls,
        target_table: str,
        source_table: str,
        on_keys: Sequence[str],
        update_columns: Sequence[str],
        insert_columns: Sequence[str] | None = None,
    ) -> str:
        """Compile a GoogleSQL `MERGE INTO` upsert statement for BigQuery."""
        return SyncBigQueryDialect.compile_merge_upsert(
            target_table,
            source_table,
            on_keys,
            update_columns,
            insert_columns,
        )
