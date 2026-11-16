"""Oracle Litestar session store backed by `OracleSyncConfig` / `OracleAsyncConfig`."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from sqlspec.extensions.litestar.store import BaseSQLSpecStore

from vertebra.extensions.sqlspec.adapters.oracle.core import (
    escape_oracle_literal,
    is_oracle_table_missing,
    oracle_ensure_utc,
    oracle_index_name,
    oracle_row_value,
    oracle_table_feature_clause,
    oracle_table_ref,
)

__all__ = ("OracleLitestarStore",)


def _coerce_bytes(value: Any) -> bytes:
    """Convert raw BLOB return values into immutable `bytes`."""
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    return str(value).encode("utf-8")


class OracleLitestarStore(BaseSQLSpecStore[Any]):
    """Litestar session store persisting BLOB sessions in Oracle via `MERGE INTO` and `provide_session()`."""

    __slots__ = ()

    extension_config_options = BaseSQLSpecStore.extension_config_options | frozenset(
        {"compression", "in_memory", "table_options"}
    )

    def _is_async_config(self) -> bool:
        return bool(getattr(self._config, "is_async", False))

    def _table_ddl(self) -> str:
        table_ref = oracle_table_ref(self._table_name)
        idx_name = oracle_index_name("idx", self._table_name, "exp")
        ext = getattr(self._config, "extension_config", None) or {}
        litestar_cfg = ext.get("litestar", {}) if isinstance(ext, dict) else {}
        feature_clause = oracle_table_feature_clause(litestar_cfg)
        create_table_sql = escape_oracle_literal(
            f"CREATE TABLE {table_ref} ("
            "session_id VARCHAR2(128) PRIMARY KEY, "
            "data BLOB NOT NULL, "
            "expires_at TIMESTAMP WITH TIME ZONE, "
            "created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL, "
            "updated_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL"
            f"){feature_clause}"
        )
        create_idx_sql = escape_oracle_literal(f"CREATE INDEX {idx_name} ON {table_ref} (expires_at)")
        return (
            "BEGIN\n"
            f"    EXECUTE IMMEDIATE '{create_table_sql}';\n"
            "EXCEPTION\n"
            "    WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF;\n"
            "END;\n"
            "BEGIN\n"
            f"    EXECUTE IMMEDIATE '{create_idx_sql}';\n"
            "EXCEPTION\n"
            "    WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF;\n"
            "END;"
        )

    def _drop_table_sql(self) -> list[str]:
        table_ref = escape_oracle_literal(oracle_table_ref(self._table_name))
        idx_name = escape_oracle_literal(oracle_index_name("idx", self._table_name, "exp"))
        return [
            (
                "BEGIN\n"
                f"    EXECUTE IMMEDIATE 'DROP INDEX {idx_name}';\n"
                "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -1418 THEN RAISE; END IF;\n"
                "END;"
            ),
            (
                "BEGIN\n"
                f"    EXECUTE IMMEDIATE 'DROP TABLE {table_ref}';\n"
                "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;\n"
                "END;"
            ),
        ]

    def _merge_set_sql(self) -> str:
        table_ref = oracle_table_ref(self._table_name)
        return (
            f"MERGE INTO {table_ref} target "
            "USING (SELECT :session_id AS session_id, :data AS data, :expires_at AS expires_at FROM DUAL) src "
            "ON (target.session_id = src.session_id) "
            "WHEN MATCHED THEN UPDATE SET target.data = src.data, target.expires_at = src.expires_at, "
            "target.updated_at = SYSTIMESTAMP "
            "WHEN NOT MATCHED THEN INSERT (session_id, data, expires_at, created_at, updated_at) "
            "VALUES (src.session_id, src.data, src.expires_at, SYSTIMESTAMP, SYSTIMESTAMP)"
        )

    def _create_table_sync(self) -> None:
        with self._config.provide_session() as driver:
            driver.execute_script(self._table_ddl())
            driver.commit()
        self._log_table_created()

    def _get_sync(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        table_ref = oracle_table_ref(self._table_name)
        sql = (
            f"SELECT data, expires_at FROM {table_ref} "
            "WHERE session_id = :session_id AND (expires_at IS NULL OR expires_at > SYSTIMESTAMP)"
        )
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(sql, {"session_id": key})
                if row is None:
                    return None
                expires_raw = oracle_row_value(row, "expires_at", 1)
                if renew_for is not None and expires_raw is not None:
                    new_expires_at = self._calculate_expires_at(renew_for)
                    if new_expires_at is not None:
                        driver.execute(
                            f"UPDATE {table_ref} SET expires_at = :expires_at, updated_at = SYSTIMESTAMP "
                            "WHERE session_id = :session_id",
                            {"expires_at": new_expires_at, "session_id": key},
                        )
                        driver.commit()
                return _coerce_bytes(oracle_row_value(row, "data", 0))
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise

    def _set_sync(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None:
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)
        with self._config.provide_session() as driver:
            driver.execute(
                self._merge_set_sql(),
                {"session_id": key, "data": data, "expires_at": expires_at},
            )
            driver.commit()

    def _delete_sync(self, key: str) -> None:
        table_ref = oracle_table_ref(self._table_name)
        try:
            with self._config.provide_session() as driver:
                driver.execute(f"DELETE FROM {table_ref} WHERE session_id = :session_id", {"session_id": key})
                driver.commit()
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return
            raise

    def _delete_all_sync(self) -> None:
        table_ref = oracle_table_ref(self._table_name)
        try:
            with self._config.provide_session() as driver:
                driver.execute(f"DELETE FROM {table_ref}")
                driver.commit()
            self._log_delete_all()
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return
            raise

    def _exists_sync(self, key: str) -> bool:
        table_ref = oracle_table_ref(self._table_name)
        sql = (
            f"SELECT 1 FROM {table_ref} "
            "WHERE session_id = :session_id AND (expires_at IS NULL OR expires_at > SYSTIMESTAMP)"
        )
        try:
            with self._config.provide_session() as driver:
                return driver.select_one_or_none(sql, {"session_id": key}) is not None
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return False
            raise

    def _expires_in_sync(self, key: str) -> int | None:
        table_ref = oracle_table_ref(self._table_name)
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(
                    f"SELECT expires_at FROM {table_ref} WHERE session_id = :session_id",
                    {"session_id": key},
                )
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise
        if row is None:
            return None
        expires_raw = oracle_row_value(row, "expires_at", 0)
        if expires_raw is None:
            return None
        remaining = oracle_ensure_utc(expires_raw) - datetime.now(UTC)
        return max(0, int(remaining.total_seconds()))

    def _delete_expired_sync(self) -> int:
        table_ref = oracle_table_ref(self._table_name)
        try:
            with self._config.provide_session() as driver:
                result = driver.execute(
                    f"DELETE FROM {table_ref} WHERE expires_at IS NOT NULL AND expires_at <= SYSTIMESTAMP"
                )
                driver.commit()
                count = int(result.rows_affected or 0)
                if count > 0:
                    self._log_delete_expired(count)
                return count
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return 0
            raise

    async def create_table(self) -> None:
        """Create the Oracle Litestar session table idempotently via PL/SQL."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        if not self._is_async_config():
            self._create_table_sync()
            return
        async with self._config.provide_session() as driver:
            await driver.execute_script(self._table_ddl())
            await driver.commit()
        self._log_table_created()

    async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        """Fetch session bytes from Oracle, renewing expiration when configured."""
        if not self._is_async_config():
            return self._get_sync(key, renew_for=renew_for)
        table_ref = oracle_table_ref(self._table_name)
        sql = (
            f"SELECT data, expires_at FROM {table_ref} "
            "WHERE session_id = :session_id AND (expires_at IS NULL OR expires_at > SYSTIMESTAMP)"
        )
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(sql, {"session_id": key})
                if row is None:
                    return None
                expires_raw = oracle_row_value(row, "expires_at", 1)
                if renew_for is not None and expires_raw is not None:
                    new_expires_at = self._calculate_expires_at(renew_for)
                    if new_expires_at is not None:
                        await driver.execute(
                            f"UPDATE {table_ref} SET expires_at = :expires_at, updated_at = SYSTIMESTAMP "
                            "WHERE session_id = :session_id",
                            {"expires_at": new_expires_at, "session_id": key},
                        )
                        await driver.commit()
                return _coerce_bytes(oracle_row_value(row, "data", 0))
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise

    async def set(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None:
        """Upsert a session entry into Oracle using `MERGE INTO`."""
        if not self._is_async_config():
            self._set_sync(key, value, expires_in=expires_in)
            return
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)
        async with self._config.provide_session() as driver:
            await driver.execute(
                self._merge_set_sql(),
                {"session_id": key, "data": data, "expires_at": expires_at},
            )
            await driver.commit()

    async def delete(self, key: str) -> None:
        """Delete a single Oracle session row."""
        if not self._is_async_config():
            self._delete_sync(key)
            return
        table_ref = oracle_table_ref(self._table_name)
        try:
            async with self._config.provide_session() as driver:
                await driver.execute(f"DELETE FROM {table_ref} WHERE session_id = :session_id", {"session_id": key})
                await driver.commit()
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return
            raise

    async def delete_all(self) -> None:
        """Delete all Oracle Litestar session rows."""
        if not self._is_async_config():
            self._delete_all_sync()
            return
        table_ref = oracle_table_ref(self._table_name)
        try:
            async with self._config.provide_session() as driver:
                await driver.execute(f"DELETE FROM {table_ref}")
                await driver.commit()
            self._log_delete_all()
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return
            raise

    async def exists(self, key: str) -> bool:
        """Return True when an unexpired session exists for `key`."""
        if not self._is_async_config():
            return self._exists_sync(key)
        table_ref = oracle_table_ref(self._table_name)
        sql = (
            f"SELECT 1 FROM {table_ref} "
            "WHERE session_id = :session_id AND (expires_at IS NULL OR expires_at > SYSTIMESTAMP)"
        )
        try:
            async with self._config.provide_session() as driver:
                return await driver.select_one_or_none(sql, {"session_id": key}) is not None
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return False
            raise

    async def expires_in(self, key: str) -> int | None:
        """Return remaining TTL in seconds for `key`, or `None` when persistent/missing."""
        if not self._is_async_config():
            return self._expires_in_sync(key)
        table_ref = oracle_table_ref(self._table_name)
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(
                    f"SELECT expires_at FROM {table_ref} WHERE session_id = :session_id",
                    {"session_id": key},
                )
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise
        if row is None:
            return None
        expires_raw = oracle_row_value(row, "expires_at", 0)
        if expires_raw is None:
            return None
        remaining = oracle_ensure_utc(expires_raw) - datetime.now(UTC)
        return max(0, int(remaining.total_seconds()))

    async def delete_expired(self) -> int:
        """Delete expired Oracle session rows and return the deleted count."""
        if not self._is_async_config():
            return self._delete_expired_sync()
        table_ref = oracle_table_ref(self._table_name)
        try:
            async with self._config.provide_session() as driver:
                result = await driver.execute(
                    f"DELETE FROM {table_ref} WHERE expires_at IS NOT NULL AND expires_at <= SYSTIMESTAMP"
                )
                await driver.commit()
                count = int(result.rows_affected or 0)
                if count > 0:
                    self._log_delete_expired(count)
                return count
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return 0
            raise
