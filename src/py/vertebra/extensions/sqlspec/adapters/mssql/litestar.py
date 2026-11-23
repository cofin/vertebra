"""SQL Server (MSSQL) Litestar session store backed by `VARBINARY(MAX)` and `MERGE ... WITH (HOLDLOCK)`."""

from __future__ import annotations

from datetime import datetime
from typing import Any, ClassVar, Final

from sqlspec.extensions.litestar.store import MAX_TABLE_NAME_LENGTH, VALID_TABLE_NAME_PATTERN, CompanionIndex
from sqlspec.migrations.schema import SchemaTarget
from sqlspec.utils.text import split_qualified_identifier

from vertebra.extensions.sqlspec._litestar import AdapterLitestarStore
from vertebra.extensions.sqlspec.adapters.mssql.core import (
    escape_sql_literal,
    is_mssql_table_missing,
    mssql_ensure_utc,
    mssql_row_value,
    mssql_table_ref,
    quote_bracket_identifier,
    split_schema_and_table,
)

__all__ = ("MSSQLLitestarStore",)

_MAX_TABLE_NAME_PARTS: Final[int] = 2
_MIN_QUOTED_SEGMENT_LENGTH: Final[int] = 2


class MSSQLLitestarStore(AdapterLitestarStore):
    """SQL Server Litestar session store supporting both sync and async Vertebra TDS drivers."""

    __slots__ = ()

    adapter_name = "vertebra_mssql"
    _dialect: ClassVar[str] = "tsql"
    _reconcile_after_create_table: ClassVar[bool] = True

    @staticmethod
    def _ensure_table_name(table_name: str) -> None:
        if not table_name or not table_name.strip():
            msg = "Table name must not be empty"
            raise ValueError(msg)
        if table_name != table_name.strip():
            msg = f"Invalid table name: {table_name!r}"
            raise ValueError(msg)
        raw_segments = table_name.split(".")
        if len(raw_segments) > _MAX_TABLE_NAME_PARTS:
            msg = f"Invalid table name: {table_name!r}"
            raise ValueError(msg)
        unwrapped: list[str] = []
        for raw_segment in raw_segments:
            segment = _unwrap_tsql_segment(raw_segment)
            if (
                segment is None
                or len(segment) > MAX_TABLE_NAME_LENGTH
                or not VALID_TABLE_NAME_PATTERN.fullmatch(segment)
            ):
                msg = f"Invalid table name: {table_name!r}"
                raise ValueError(msg)
            unwrapped.append(segment)
        parts = split_qualified_identifier(table_name, quote_chars='"', allow_bracket_quotes=True)
        if parts != tuple(unwrapped):
            msg = f"Invalid table name: {table_name!r}"
            raise ValueError(msg)

    def _table_ddl(self) -> str:
        schema_name, table_name = split_schema_and_table(self._table_name)
        table_ref = mssql_table_ref(self._table_name)
        index_name = quote_bracket_identifier(f"IX_{table_name}_expires_at")
        return (
            "IF NOT EXISTS ("
            "SELECT 1 FROM sys.tables "
            f"WHERE name = N'{escape_sql_literal(table_name)}' "
            f"AND schema_id = SCHEMA_ID(N'{escape_sql_literal(schema_name)}')"
            ") BEGIN "
            f"CREATE TABLE {table_ref} ("
            "session_id NVARCHAR(255) PRIMARY KEY, "
            "data VARBINARY(MAX) NOT NULL, "
            "expires_at DATETIMEOFFSET(6) NULL, "
            "created_at DATETIMEOFFSET(6) NOT NULL DEFAULT SYSDATETIMEOFFSET(), "
            "updated_at DATETIMEOFFSET(6) NOT NULL DEFAULT SYSDATETIMEOFFSET()"
            "); "
            f"CREATE INDEX {index_name} ON {table_ref}(expires_at) WHERE expires_at IS NOT NULL; "
            "END;"
        )

    def _drop_table_sql(self) -> list[str]:
        table_ref = mssql_table_ref(self._table_name)
        return [f"IF OBJECT_ID(N'{escape_sql_literal(table_ref)}', N'U') IS NOT NULL DROP TABLE {table_ref};"]

    def _schema_target(self) -> SchemaTarget:
        schema_name, bare_table = split_schema_and_table(self._table_name)
        return SchemaTarget.from_ddl(bare_table, self._table_ddl(), schema=schema_name, dialect="tsql")

    def get_companion_indexes(self) -> list[CompanionIndex]:
        """Return the filtered SQL Server session expiry index metadata and `IF NOT EXISTS` T-SQL DDL."""
        schema_name, bare_table = split_schema_and_table(self._table_name)
        table_ref = mssql_table_ref(self._table_name)
        raw_index_name = f"IX_{bare_table}_expires_at"
        quoted_index_name = quote_bracket_identifier(raw_index_name)
        ddl = (
            "IF NOT EXISTS ("
            "SELECT 1 FROM sys.indexes "
            f"WHERE name = N'{escape_sql_literal(raw_index_name)}' "
            f"AND object_id = OBJECT_ID(N'{escape_sql_literal(table_ref)}')"
            ") BEGIN "
            f"CREATE INDEX {quoted_index_name} ON {table_ref}(expires_at) WHERE expires_at IS NOT NULL; "
            "END;"
        )
        return [CompanionIndex(name=raw_index_name, table=bare_table, schema=schema_name, ddl=ddl)]

    def _is_table_missing(self, exc: Exception) -> bool:
        return is_mssql_table_missing(exc)

    def _row_value(self, row: Any, column: str, index: int) -> Any:
        return mssql_row_value(row, column, index)

    def _encode_expires_at(self, expires_at: datetime | None) -> datetime | None:
        return None if expires_at is None else mssql_ensure_utc(expires_at)

    def _decode_expires_at(self, raw: Any) -> datetime | None:
        return None if raw is None else mssql_ensure_utc(raw)

    def _get_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        table_ref = mssql_table_ref(self._table_name)
        sql = (
            f"SELECT data, expires_at FROM {table_ref} "
            "WHERE session_id = ? AND (expires_at IS NULL OR expires_at > SYSDATETIMEOFFSET())"
        )
        return sql, (key,)

    def _renew_query(self, key: str, encoded_expires_at: Any) -> tuple[str, tuple[Any, ...]]:
        table_ref = mssql_table_ref(self._table_name)
        return (
            f"UPDATE {table_ref} SET expires_at = ?, updated_at = SYSDATETIMEOFFSET() WHERE session_id = ?",
            (encoded_expires_at, key),
        )

    def _set_query(self, key: str, data: bytes, encoded_expires_at: Any) -> tuple[str, tuple[Any, ...]]:
        table_ref = mssql_table_ref(self._table_name)
        sql = (
            f"MERGE INTO {table_ref} WITH (HOLDLOCK) AS target "
            "USING (SELECT ? AS session_id, CONVERT(VARBINARY(MAX), ?) AS data, ? AS expires_at) AS src "
            "ON target.session_id = src.session_id "
            "WHEN MATCHED THEN UPDATE SET data = src.data, expires_at = src.expires_at, updated_at = SYSDATETIMEOFFSET() "
            "WHEN NOT MATCHED THEN INSERT (session_id, data, expires_at) VALUES (src.session_id, src.data, src.expires_at);"
        )
        return sql, (key, data, encoded_expires_at)

    def _delete_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        return f"DELETE FROM {mssql_table_ref(self._table_name)} WHERE session_id = ?", (key,)

    def _delete_all_query(self) -> tuple[str, tuple[Any, ...]]:
        return f"TRUNCATE TABLE {mssql_table_ref(self._table_name)}", ()

    def _exists_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        table_ref = mssql_table_ref(self._table_name)
        sql = (
            f"SELECT TOP (1) 1 AS present FROM {table_ref} "
            "WHERE session_id = ? AND (expires_at IS NULL OR expires_at > SYSDATETIMEOFFSET())"
        )
        return sql, (key,)

    def _expires_in_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        return f"SELECT TOP (1) expires_at FROM {mssql_table_ref(self._table_name)} WHERE session_id = ?", (key,)

    def _delete_expired_query(self, now: datetime) -> tuple[str, tuple[Any, ...]]:
        _ = now
        table_ref = mssql_table_ref(self._table_name)
        return f"DELETE FROM {table_ref} WHERE expires_at IS NOT NULL AND expires_at <= SYSDATETIMEOFFSET()", ()


def _unwrap_tsql_segment(segment: str) -> str | None:
    if not segment or segment != segment.strip():
        return None
    if segment.startswith("["):
        if len(segment) < _MIN_QUOTED_SEGMENT_LENGTH or not segment.endswith("]"):
            return None
        inner = segment[1:-1]
        if "[" in inner or "]" in inner.replace("]]", ""):
            return None
        return inner.replace("]]", "]")
    if segment.startswith('"'):
        if len(segment) < _MIN_QUOTED_SEGMENT_LENGTH or not segment.endswith('"'):
            return None
        inner = segment[1:-1]
        if '"' in inner.replace('""', ""):
            return None
        return inner.replace('""', '"')
    return segment
