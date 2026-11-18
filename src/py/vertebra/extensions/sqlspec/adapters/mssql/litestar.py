"""SQL Server (MSSQL) Litestar session store backed by `VARBINARY(MAX)` and `MERGE ... WITH (HOLDLOCK)`."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Final

from sqlspec.extensions.litestar.store import (
    MAX_TABLE_NAME_LENGTH,
    VALID_TABLE_NAME_PATTERN,
    BaseSQLSpecStore,
    CompanionIndex,
)
from sqlspec.migrations.schema import SchemaTarget
from sqlspec.utils.sync_tools import async_
from sqlspec.utils.text import split_qualified_identifier

from vertebra.extensions.sqlspec.adapters.mssql.core import (
    escape_sql_literal,
    is_mssql_table_missing,
    mssql_ensure_utc,
    mssql_row_value,
    mssql_table_ref,
    quote_bracket_identifier,
    split_schema_and_table,
)

if TYPE_CHECKING:
    from sqlspec.migrations.schema import DropStatement

__all__ = ("MSSQLLitestarStore",)

_MAX_TABLE_NAME_PARTS: Final[int] = 2
_MIN_QUOTED_SEGMENT_LENGTH: Final[int] = 2


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


def _validate_tsql_table_name(table_name: str) -> None:
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
        if segment is None or len(segment) > MAX_TABLE_NAME_LENGTH or not VALID_TABLE_NAME_PATTERN.fullmatch(segment):
            msg = f"Invalid table name: {table_name!r}"
            raise ValueError(msg)
        unwrapped.append(segment)
    parts = split_qualified_identifier(table_name, quote_chars='"', allow_bracket_quotes=True)
    if parts != tuple(unwrapped):
        msg = f"Invalid table name: {table_name!r}"
        raise ValueError(msg)


def _coerce_bytes(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, str):
        return value.encode("utf-8")
    return bytes(value)


class MSSQLLitestarStore(BaseSQLSpecStore[Any]):
    """SQL Server Litestar session store supporting both sync and async Vertebra TDS drivers."""

    __slots__ = ()

    adapter_name = "vertebra_mssql"
    _dialect: ClassVar[str] = "tsql"

    @staticmethod
    def _ensure_table_name(table_name: str) -> None:
        """Validate a 1- or 2-part T-SQL session table identifier."""
        _validate_tsql_table_name(table_name)

    def _is_async_config(self) -> bool:
        return bool(getattr(self._config, "is_async", False))

    def _table_ddl(self) -> str:
        """Build idempotent T-SQL DDL for the Litestar session table and filtered expiry index."""
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
        """Return idempotent T-SQL `DROP TABLE` statement list."""
        table_ref = mssql_table_ref(self._table_name)
        return [f"IF OBJECT_ID(N'{escape_sql_literal(table_ref)}', N'U') IS NOT NULL DROP TABLE {table_ref};"]

    def _schema_target(self) -> SchemaTarget:
        """Build the SQL Server session `SchemaTarget` scoped to `dbo` or an explicit schema."""
        schema_name, bare_table = split_schema_and_table(self._table_name)
        return SchemaTarget.from_ddl(bare_table, self._table_ddl(), schema=schema_name, dialect="tsql")

    def get_schema_targets(self) -> list[SchemaTarget]:
        """Return ordered `SchemaTarget` definitions for the SQL Server session table."""
        return super().get_schema_targets()

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

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Create missing SQL Server companion indexes synchronously after `get_indexes()` checks."""
        super().reconcile_companion_objects_sync(driver)

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Create missing SQL Server companion indexes asynchronously after `get_indexes()` checks."""
        await super().reconcile_companion_objects_async(driver)

    def downgrade_drops(self) -> list[DropStatement]:
        """Return guarded T-SQL downgrade drop statements for the session table."""
        return super().downgrade_drops()

    def _get_sql(self) -> str:
        table_ref = mssql_table_ref(self._table_name)
        return (
            f"SELECT data, expires_at FROM {table_ref} "
            "WHERE session_id = ? AND (expires_at IS NULL OR expires_at > SYSDATETIMEOFFSET())"
        )

    def _update_expiry_sql(self) -> str:
        table_ref = mssql_table_ref(self._table_name)
        return f"UPDATE {table_ref} SET expires_at = ?, updated_at = SYSDATETIMEOFFSET() WHERE session_id = ?"

    def _merge_set_sql(self) -> str:
        table_ref = mssql_table_ref(self._table_name)
        return (
            f"MERGE INTO {table_ref} WITH (HOLDLOCK) AS target "
            "USING (SELECT ? AS session_id, CONVERT(VARBINARY(MAX), ?) AS data, ? AS expires_at) AS src "
            "ON target.session_id = src.session_id "
            "WHEN MATCHED THEN UPDATE SET data = src.data, expires_at = src.expires_at, updated_at = SYSDATETIMEOFFSET() "
            "WHEN NOT MATCHED THEN INSERT (session_id, data, expires_at) VALUES (src.session_id, src.data, src.expires_at);"
        )

    def _exists_sql(self) -> str:
        table_ref = mssql_table_ref(self._table_name)
        return (
            f"SELECT TOP (1) 1 AS present FROM {table_ref} "
            "WHERE session_id = ? AND (expires_at IS NULL OR expires_at > SYSDATETIMEOFFSET())"
        )

    def _create_table_sync(self) -> None:
        with self._config.provide_session() as driver:
            driver.execute_script(self._table_ddl())
            driver.commit()

    def _get_sync(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(self._get_sql(), key)
                if row is None:
                    return None
                expires_raw = mssql_row_value(row, "expires_at", 1)
                if renew_for is not None and expires_raw is not None:
                    new_expires_at = self._calculate_expires_at(renew_for)
                    if new_expires_at is not None:
                        driver.execute(self._update_expiry_sql(), mssql_ensure_utc(new_expires_at), key)
                        driver.commit()
                return _coerce_bytes(mssql_row_value(row, "data", 0))
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return None
            raise

    def _set_sync(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None:
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)
        with self._config.provide_session() as driver:
            driver.execute(self._merge_set_sql(), key, data, expires_at)
            driver.commit()

    def _delete_sync(self, key: str) -> None:
        table_ref = mssql_table_ref(self._table_name)
        with self._config.provide_session() as driver:
            driver.execute(f"DELETE FROM {table_ref} WHERE session_id = ?", key)
            driver.commit()

    def _delete_all_sync(self) -> None:
        table_ref = mssql_table_ref(self._table_name)
        with self._config.provide_session() as driver:
            driver.execute(f"TRUNCATE TABLE {table_ref}")
            driver.commit()

    def _exists_sync(self, key: str) -> bool:
        try:
            with self._config.provide_session() as driver:
                return driver.select_one_or_none(self._exists_sql(), key) is not None
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return False
            raise

    def _expires_in_sync(self, key: str) -> int | None:
        table_ref = mssql_table_ref(self._table_name)
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(
                    f"SELECT TOP (1) expires_at FROM {table_ref} WHERE session_id = ?",
                    key,
                )
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return None
            raise
        if row is None:
            return None
        expires_raw = mssql_row_value(row, "expires_at", 0)
        if expires_raw is None:
            return None
        remaining = mssql_ensure_utc(expires_raw) - datetime.now(UTC)
        return max(0, int(remaining.total_seconds()))

    def _delete_expired_sync(self) -> int:
        table_ref = mssql_table_ref(self._table_name)
        sql = f"DELETE FROM {table_ref} WHERE expires_at IS NOT NULL AND expires_at < SYSDATETIMEOFFSET()"
        try:
            with self._config.provide_session() as driver:
                res = driver.execute(sql)
                driver.commit()
                return int(res.rows_affected or 0)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return 0
            raise

    async def create_table(self) -> None:
        """Create the SQL Server session table and filtered index idempotently."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        if self._is_async_config():
            async with self._config.provide_session() as driver:
                await driver.execute_script(self._table_ddl())
                await driver.commit()
        else:
            await async_(self._create_table_sync)()
        self._log_table_created()
        await self.reconcile_schema(assume_existing=True)

    async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        """Retrieve session payload bytes by `key`, extending expiry when `renew_for` is provided."""
        if not self._is_async_config():
            return await async_(self._get_sync)(key, renew_for)
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(self._get_sql(), key)
                if row is None:
                    return None
                expires_raw = mssql_row_value(row, "expires_at", 1)
                if renew_for is not None and expires_raw is not None:
                    new_expires_at = self._calculate_expires_at(renew_for)
                    if new_expires_at is not None:
                        await driver.execute(self._update_expiry_sql(), mssql_ensure_utc(new_expires_at), key)
                        await driver.commit()
                return _coerce_bytes(mssql_row_value(row, "data", 0))
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return None
            raise

    async def set(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None:
        """Insert or update a session record using `MERGE ... WITH (HOLDLOCK)`."""
        if not self._is_async_config():
            await async_(self._set_sync)(key, value, expires_in)
            return
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)
        async with self._config.provide_session() as driver:
            await driver.execute(self._merge_set_sql(), key, data, expires_at)
            await driver.commit()

    async def delete(self, key: str) -> None:
        """Delete a stored session by `key`."""
        if not self._is_async_config():
            await async_(self._delete_sync)(key)
            return
        table_ref = mssql_table_ref(self._table_name)
        async with self._config.provide_session() as driver:
            await driver.execute(f"DELETE FROM {table_ref} WHERE session_id = ?", key)
            await driver.commit()

    async def delete_all(self) -> None:
        """Truncate all session rows from the backing SQL Server table."""
        if not self._is_async_config():
            await async_(self._delete_all_sync)()
        else:
            table_ref = mssql_table_ref(self._table_name)
            async with self._config.provide_session() as driver:
                await driver.execute(f"TRUNCATE TABLE {table_ref}")
                await driver.commit()
        self._log_delete_all()

    async def exists(self, key: str) -> bool:
        """Return whether an unexpired session entry exists for `key`."""
        if not self._is_async_config():
            return await async_(self._exists_sync)(key)
        try:
            async with self._config.provide_session() as driver:
                return await driver.select_one_or_none(self._exists_sql(), key) is not None
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return False
            raise

    async def expires_in(self, key: str) -> int | None:
        """Return remaining TTL in seconds for `key`, or `None` if absent or non-expiring."""
        if not self._is_async_config():
            return await async_(self._expires_in_sync)(key)
        table_ref = mssql_table_ref(self._table_name)
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(
                    f"SELECT TOP (1) expires_at FROM {table_ref} WHERE session_id = ?",
                    key,
                )
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return None
            raise
        if row is None:
            return None
        expires_raw = mssql_row_value(row, "expires_at", 0)
        if expires_raw is None:
            return None
        remaining = mssql_ensure_utc(expires_raw) - datetime.now(UTC)
        return max(0, int(remaining.total_seconds()))

    async def delete_expired(self) -> int:
        """Delete expired session rows and return the affected row count."""
        if not self._is_async_config():
            count = await async_(self._delete_expired_sync)()
        else:
            table_ref = mssql_table_ref(self._table_name)
            sql = f"DELETE FROM {table_ref} WHERE expires_at IS NOT NULL AND expires_at < SYSDATETIMEOFFSET()"
            try:
                async with self._config.provide_session() as driver:
                    res = await driver.execute(sql)
                    await driver.commit()
                    count = int(res.rows_affected or 0)
            except Exception as exc:
                if is_mssql_table_missing(exc):
                    return 0
                raise
        if count > 0:
            self._log_delete_expired(count)
        return count
