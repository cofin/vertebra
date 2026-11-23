"""DuckDB Litestar session store supporting both sync and async Vertebra DuckDB configs."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, ClassVar, Final

from sqlspec.extensions.litestar.store import MAX_TABLE_NAME_LENGTH, VALID_TABLE_NAME_PATTERN
from sqlspec.migrations.schema import SchemaTarget
from sqlspec.utils.text import split_qualified_identifier

from vertebra.extensions.sqlspec._litestar import AdapterLitestarStore
from vertebra.extensions.sqlspec.adapters.duckdb.core import format_duckdb_identifier, split_schema_and_table

__all__ = ("DuckDBLitestarStore",)

_MAX_TABLE_NAME_PARTS: Final[int] = 2


class DuckDBLitestarStore(AdapterLitestarStore):
    """DuckDB Litestar session store using `ON CONFLICT DO UPDATE` and `provide_session()`."""

    __slots__ = ()

    adapter_name = "vertebra_duckdb"
    _dialect: ClassVar[str] = "duckdb"
    _reconcile_after_create_table: ClassVar[bool] = True
    _delete_expired_uses_select: ClassVar[bool] = True
    _uses_select_value: ClassVar[bool] = True

    @staticmethod
    def _ensure_table_name(table_name: str) -> None:
        cleaned = table_name.strip()
        if not cleaned or cleaned != table_name:
            msg = f"Invalid table name: {table_name!r}"
            raise ValueError(msg)
        parts = split_qualified_identifier(cleaned, quote_chars='"')
        if not parts or len(parts) > _MAX_TABLE_NAME_PARTS:
            msg = f"Invalid table name: {table_name!r}"
            raise ValueError(msg)
        for segment in parts:
            if len(segment) > MAX_TABLE_NAME_LENGTH or not VALID_TABLE_NAME_PATTERN.fullmatch(segment):
                msg = f"Invalid table name: {table_name!r}"
                raise ValueError(msg)

    def _table_ref(self) -> str:
        return format_duckdb_identifier(self._table_name)

    def _table_ddl(self) -> str:
        table_ref = self._table_ref()
        idx_name = format_duckdb_identifier(f"idx_{self._table_name.replace('.', '_')}_expires_at")
        return (
            f"CREATE TABLE IF NOT EXISTS {table_ref} (\n"
            "    session_id VARCHAR PRIMARY KEY,\n"
            "    data BLOB NOT NULL,\n"
            "    expires_at TIMESTAMPTZ,\n"
            "    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,\n"
            "    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP\n"
            ");\n"
            f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_ref}(expires_at);"
        )

    def _drop_table_sql(self) -> list[str]:
        idx_name = format_duckdb_identifier(f"idx_{self._table_name.replace('.', '_')}_expires_at")
        return [f"DROP INDEX IF EXISTS {idx_name}", f"DROP TABLE IF EXISTS {self._table_ref()}"]

    def _schema_target(self) -> SchemaTarget:
        if "." in self._table_name:
            schema_name, bare_table = split_schema_and_table(self._table_name)
            return SchemaTarget.from_ddl(bare_table, self._table_ddl(), schema=schema_name, dialect="duckdb")
        bare_table = self._table_name.strip('"')
        return SchemaTarget.from_ddl(bare_table, self._table_ddl(), dialect="duckdb")

    def _encode_expires_at(self, expires_at: datetime | None) -> str | None:
        return None if expires_at is None else expires_at.astimezone(UTC).isoformat()

    def _get_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        sql = (
            f"SELECT data, expires_at FROM {self._table_ref()} "
            "WHERE session_id = ? AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)"
        )
        return sql, (key,)

    def _renew_query(self, key: str, encoded_expires_at: Any) -> tuple[str, tuple[Any, ...]]:
        return (
            f"UPDATE {self._table_ref()} SET expires_at = ?, updated_at = NOW() WHERE session_id = ?",
            (encoded_expires_at, key),
        )

    def _set_query(self, key: str, data: bytes, encoded_expires_at: Any) -> tuple[str, tuple[Any, ...]]:
        sql = (
            f"INSERT INTO {self._table_ref()} (session_id, data, expires_at) VALUES (?, ?, ?) "
            "ON CONFLICT (session_id) DO UPDATE SET "
            "data = EXCLUDED.data, expires_at = EXCLUDED.expires_at, updated_at = NOW()"
        )
        return sql, (key, data, encoded_expires_at)

    def _delete_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        return f"DELETE FROM {self._table_ref()} WHERE session_id = ?", (key,)

    def _delete_all_query(self) -> tuple[str, tuple[Any, ...]]:
        return f"DELETE FROM {self._table_ref()}", ()

    def _exists_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        sql = (
            f"SELECT 1 AS present FROM {self._table_ref()} "
            "WHERE session_id = ? AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)"
        )
        return sql, (key,)

    def _expires_in_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        return f"SELECT expires_at FROM {self._table_ref()} WHERE session_id = ?", (key,)

    def _delete_expired_query(self, now: datetime) -> tuple[str, tuple[Any, ...]]:
        _ = now
        return f"DELETE FROM {self._table_ref()} WHERE expires_at <= CURRENT_TIMESTAMP RETURNING 1 AS deleted", ()
