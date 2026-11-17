"""PostgreSQL Litestar session store backed by Vertebra `PostgresSyncConfig` / `PostgresAsyncConfig`."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from sqlspec.extensions.litestar.store import BaseSQLSpecStore, CompanionIndex
from sqlspec.migrations.schema import DropStatement, SchemaTarget, metadata_has_index

from vertebra.extensions.sqlspec.adapters.postgres.core import (
    is_pg_table_missing,
    pg_ensure_utc,
    pg_index_name,
    pg_row_value,
    pg_table_ref,
    postgres_storage_params_sql,
    validate_pg_table_name,
)

__all__ = ("PostgresLitestarStore",)


def _coerce_bytes(value: Any) -> bytes:
    """Convert raw BYTEA return values into immutable `bytes`."""
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    return str(value).encode("utf-8")


class PostgresLitestarStore(BaseSQLSpecStore[Any]):
    """Litestar session store persisting BYTEA sessions in PostgreSQL via `provide_session()`."""

    __slots__ = ()

    extension_config_options = BaseSQLSpecStore.extension_config_options | frozenset(
        {"autovacuum_analyze_scale_factor", "autovacuum_vacuum_scale_factor", "fillfactor"}
    )

    @staticmethod
    def _ensure_table_name(table_name: str) -> None:
        validate_pg_table_name(table_name)

    def _split_table_name(self) -> tuple[str | None, str]:
        parts = validate_pg_table_name(self._table_name)
        if len(parts) == 2:
            return parts[0], parts[1]
        return None, parts[0]

    def _is_async_config(self) -> bool:
        return bool(getattr(self._config, "is_async", False))

    def _companion_index(self) -> CompanionIndex:
        schema_name, bare_table = self._split_table_name()
        table_ref = pg_table_ref(self._table_name)
        idx_name = pg_index_name("idx", self._table_name, "expires_at")
        return CompanionIndex(
            name=idx_name,
            table=bare_table,
            schema=schema_name,
            ddl=f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_ref} (expires_at) WHERE expires_at IS NOT NULL;",
        )

    def _table_ddl(self) -> str:
        table_ref = pg_table_ref(self._table_name)
        ext = getattr(self._config, "extension_config", None) or {}
        litestar_cfg = ext.get("litestar", {}) if isinstance(ext, dict) else {}
        tuning = postgres_storage_params_sql(
            litestar_cfg,
            default_fillfactor=80,
            default_vacuum_scale=0.05,
            default_analyze_scale=0.02,
            include_autovacuum=True,
        )
        return (
            f"CREATE TABLE IF NOT EXISTS {table_ref} ("
            "session_id TEXT PRIMARY KEY, "
            "data BYTEA NOT NULL, "
            "expires_at TIMESTAMPTZ, "
            "created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, "
            "updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP"
            f"){tuning};\n"
            f"{self._companion_index().ddl}"
        )

    def _schema_target(self) -> SchemaTarget:
        schema_name, bare_table = self._split_table_name()
        return SchemaTarget.from_ddl(
            bare_table,
            self._table_ddl(),
            schema=schema_name,
            dialect="postgres",
        )

    def get_schema_targets(self) -> list[SchemaTarget]:
        """Return ordered `SchemaTarget` definitions for the PostgreSQL session store."""
        return [self._schema_target()]

    def get_companion_indexes(self) -> list[CompanionIndex]:
        """Return the partial PostgreSQL session expiration companion index."""
        return [self._companion_index()]

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Create missing companion partial index on a synchronous driver without committing."""
        for index in self.get_companion_indexes():
            rows = driver.data_dictionary.get_indexes(driver, table=index.table, schema=index.schema)
            if not metadata_has_index(rows, index.table, index.name, schema_name=index.schema):
                driver.execute_script(index.ddl)

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Create missing companion partial index on an asynchronous driver without committing."""
        for index in self.get_companion_indexes():
            rows = await driver.data_dictionary.get_indexes(driver, table=index.table, schema=index.schema)
            if not metadata_has_index(rows, index.table, index.name, schema_name=index.schema):
                await driver.execute_script(index.ddl)

    def _drop_table_sql(self) -> list[str]:
        table_ref = pg_table_ref(self._table_name)
        idx_name = pg_index_name("idx", self._table_name, "expires_at")
        return [
            f"DROP INDEX IF EXISTS {idx_name};",
            f"DROP TABLE IF EXISTS {table_ref};",
        ]

    def downgrade_drops(self) -> list[DropStatement]:
        """Return downgrade drop statements for the PostgreSQL session index and table."""
        return [DropStatement(statement) for statement in self._drop_table_sql()]

    def _create_table_sync(self) -> None:
        with self._config.provide_session() as driver:
            driver.execute_script(self._table_ddl())
            driver.commit()
        self._log_table_created()

    def _get_sync(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        table_ref = pg_table_ref(self._table_name)
        sql = (
            f"SELECT data, expires_at FROM {table_ref} "
            "WHERE session_id = $1 AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)"
        )
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(sql, key)
                if row is None:
                    return None
                expires_raw = pg_row_value(row, "expires_at", 1)
                if renew_for is not None and expires_raw is not None:
                    new_expires_at = self._calculate_expires_at(renew_for)
                    if new_expires_at is not None:
                        driver.execute(
                            f"UPDATE {table_ref} SET expires_at = $1, updated_at = CURRENT_TIMESTAMP WHERE session_id = $2",
                            new_expires_at,
                            key,
                        )
                        driver.commit()
                return _coerce_bytes(pg_row_value(row, "data", 0))
        except Exception as exc:
            if is_pg_table_missing(exc):
                return None
            raise

    def _set_sync(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None:
        table_ref = pg_table_ref(self._table_name)
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)
        sql = (
            f"INSERT INTO {table_ref} (session_id, data, expires_at) VALUES ($1, $2, $3) "
            "ON CONFLICT (session_id) DO UPDATE SET "
            "data = EXCLUDED.data, expires_at = EXCLUDED.expires_at, updated_at = CURRENT_TIMESTAMP"
        )
        with self._config.provide_session() as driver:
            driver.execute(sql, key, data, expires_at)
            driver.commit()

    def _delete_sync(self, key: str) -> None:
        table_ref = pg_table_ref(self._table_name)
        try:
            with self._config.provide_session() as driver:
                driver.execute(f"DELETE FROM {table_ref} WHERE session_id = $1", key)
                driver.commit()
        except Exception as exc:
            if is_pg_table_missing(exc):
                return
            raise

    def _delete_all_sync(self) -> None:
        table_ref = pg_table_ref(self._table_name)
        try:
            with self._config.provide_session() as driver:
                driver.execute(f"DELETE FROM {table_ref}")
                driver.commit()
            self._log_delete_all()
        except Exception as exc:
            if is_pg_table_missing(exc):
                return
            raise

    def _exists_sync(self, key: str) -> bool:
        table_ref = pg_table_ref(self._table_name)
        sql = (
            f"SELECT 1 FROM {table_ref} "
            "WHERE session_id = $1 AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)"
        )
        try:
            with self._config.provide_session() as driver:
                return driver.select_one_or_none(sql, key) is not None
        except Exception as exc:
            if is_pg_table_missing(exc):
                return False
            raise

    def _expires_in_sync(self, key: str) -> int | None:
        table_ref = pg_table_ref(self._table_name)
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(f"SELECT expires_at FROM {table_ref} WHERE session_id = $1", key)
        except Exception as exc:
            if is_pg_table_missing(exc):
                return None
            raise
        if row is None:
            return None
        expires_raw = pg_row_value(row, "expires_at", 0)
        if expires_raw is None:
            return None
        remaining = pg_ensure_utc(expires_raw) - datetime.now(UTC)
        return max(0, int(remaining.total_seconds()))

    def _delete_expired_sync(self) -> int:
        table_ref = pg_table_ref(self._table_name)
        try:
            with self._config.provide_session() as driver:
                result = driver.execute(
                    f"DELETE FROM {table_ref} WHERE expires_at IS NOT NULL AND expires_at <= CURRENT_TIMESTAMP"
                )
                driver.commit()
                count = int(result.rows_affected or 0)
                if count > 0:
                    self._log_delete_expired(count)
                return count
        except Exception as exc:
            if is_pg_table_missing(exc):
                return 0
            raise

    async def create_table(self) -> None:
        """Create the PostgreSQL Litestar session table and expiration index."""
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
        """Fetch session payload bytes by key, optionally sliding its expiration timestamp."""
        if not self._is_async_config():
            return self._get_sync(key, renew_for=renew_for)
        table_ref = pg_table_ref(self._table_name)
        sql = (
            f"SELECT data, expires_at FROM {table_ref} "
            "WHERE session_id = $1 AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)"
        )
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(sql, key)
                if row is None:
                    return None
                expires_raw = pg_row_value(row, "expires_at", 1)
                if renew_for is not None and expires_raw is not None:
                    new_expires_at = self._calculate_expires_at(renew_for)
                    if new_expires_at is not None:
                        await driver.execute(
                            f"UPDATE {table_ref} SET expires_at = $1, updated_at = CURRENT_TIMESTAMP WHERE session_id = $2",
                            new_expires_at,
                            key,
                        )
                        await driver.commit()
                return _coerce_bytes(pg_row_value(row, "data", 0))
        except Exception as exc:
            if is_pg_table_missing(exc):
                return None
            raise

    async def set(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None:
        """Upsert a session entry via PostgreSQL `ON CONFLICT (session_id) DO UPDATE`."""
        if not self._is_async_config():
            self._set_sync(key, value, expires_in=expires_in)
            return
        table_ref = pg_table_ref(self._table_name)
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)
        sql = (
            f"INSERT INTO {table_ref} (session_id, data, expires_at) VALUES ($1, $2, $3) "
            "ON CONFLICT (session_id) DO UPDATE SET "
            "data = EXCLUDED.data, expires_at = EXCLUDED.expires_at, updated_at = CURRENT_TIMESTAMP"
        )
        async with self._config.provide_session() as driver:
            await driver.execute(sql, key, data, expires_at)
            await driver.commit()

    async def delete(self, key: str) -> None:
        """Delete a single session row by `session_id`."""
        if not self._is_async_config():
            self._delete_sync(key)
            return
        table_ref = pg_table_ref(self._table_name)
        try:
            async with self._config.provide_session() as driver:
                await driver.execute(f"DELETE FROM {table_ref} WHERE session_id = $1", key)
                await driver.commit()
        except Exception as exc:
            if is_pg_table_missing(exc):
                return
            raise

    async def delete_all(self) -> None:
        """Delete all rows from the PostgreSQL Litestar session table."""
        if not self._is_async_config():
            self._delete_all_sync()
            return
        table_ref = pg_table_ref(self._table_name)
        try:
            async with self._config.provide_session() as driver:
                await driver.execute(f"DELETE FROM {table_ref}")
                await driver.commit()
            self._log_delete_all()
        except Exception as exc:
            if is_pg_table_missing(exc):
                return
            raise

    async def exists(self, key: str) -> bool:
        """Return True when an unexpired session exists for `key`."""
        if not self._is_async_config():
            return self._exists_sync(key)
        table_ref = pg_table_ref(self._table_name)
        sql = (
            f"SELECT 1 FROM {table_ref} "
            "WHERE session_id = $1 AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)"
        )
        try:
            async with self._config.provide_session() as driver:
                return await driver.select_one_or_none(sql, key) is not None
        except Exception as exc:
            if is_pg_table_missing(exc):
                return False
            raise

    async def expires_in(self, key: str) -> int | None:
        """Return remaining TTL in seconds for `key`, or `None` when persistent/missing."""
        if not self._is_async_config():
            return self._expires_in_sync(key)
        table_ref = pg_table_ref(self._table_name)
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(f"SELECT expires_at FROM {table_ref} WHERE session_id = $1", key)
        except Exception as exc:
            if is_pg_table_missing(exc):
                return None
            raise
        if row is None:
            return None
        expires_raw = pg_row_value(row, "expires_at", 0)
        if expires_raw is None:
            return None
        remaining = pg_ensure_utc(expires_raw) - datetime.now(UTC)
        return max(0, int(remaining.total_seconds()))

    async def delete_expired(self) -> int:
        """Delete expired session rows and return the deleted count."""
        if not self._is_async_config():
            return self._delete_expired_sync()
        table_ref = pg_table_ref(self._table_name)
        try:
            async with self._config.provide_session() as driver:
                result = await driver.execute(
                    f"DELETE FROM {table_ref} WHERE expires_at IS NOT NULL AND expires_at <= CURRENT_TIMESTAMP"
                )
                await driver.commit()
                count = int(result.rows_affected or 0)
                if count > 0:
                    self._log_delete_expired(count)
                return count
        except Exception as exc:
            if is_pg_table_missing(exc):
                return 0
            raise
