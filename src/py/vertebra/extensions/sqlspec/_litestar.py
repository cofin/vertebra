"""Shared SQLSpec Litestar session store base for Vertebra adapters."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar

from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.migrations.schema import DropStatement, metadata_has_index
from sqlspec.utils.sync_tools import async_

__all__ = ("AdapterLitestarStore",)


class AdapterLitestarStore(BaseSQLSpecStore[Any]):
    """Uniform Litestar session store executing sync and async configs via ``provide_session()``."""

    __slots__ = ()

    _use_threadpool_for_sync: ClassVar[bool] = True
    _commit_after_write: ClassVar[bool] = True
    _create_uses_script: ClassVar[bool] = True
    _check_create_schema_enabled: ClassVar[bool] = True
    _reconcile_after_create_table: ClassVar[bool] = False
    _check_expiry_in_python: ClassVar[bool] = False
    _separate_write_session_on_get: ClassVar[bool] = False
    _delete_expired_uses_select: ClassVar[bool] = False
    _uses_select_value: ClassVar[bool] = False
    _log_writes: ClassVar[bool] = True

    def _is_async_config(self) -> bool:
        return bool(getattr(self._config, "is_async", False))

    def _ddl_session_kwargs(self) -> dict[str, Any]:
        return {}

    def _read_session_kwargs(self) -> dict[str, Any]:
        return {}

    def _write_session_kwargs(self) -> dict[str, Any]:
        return {}

    def _prepare_driver_sync(self, driver: Any) -> None:
        _ = driver

    async def _prepare_driver_async(self, driver: Any) -> None:
        _ = driver

    def _is_table_missing(self, exc: Exception) -> bool:
        _ = exc
        return False

    def _coerce_bytes(self, value: Any) -> bytes:
        if value is None:
            return b""
        if isinstance(value, bytes):
            return value
        if isinstance(value, (bytearray, memoryview)):
            return bytes(value)
        if isinstance(value, str):
            return value.encode("utf-8")
        return bytes(value)

    def _row_value(self, row: Any, column: str, index: int) -> Any:
        if isinstance(row, dict):
            return row.get(column)
        return row[index]

    def _encode_expires_at(self, expires_at: datetime | None) -> Any:
        return expires_at

    def _decode_expires_at(self, raw: Any) -> datetime | None:
        if raw is None:
            return None
        if isinstance(raw, datetime):
            return raw.replace(tzinfo=UTC) if raw.tzinfo is None else raw.astimezone(UTC)
        dt = datetime.fromisoformat(str(raw))
        return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)

    def _create_table_statements(self) -> list[str]:
        return [self._table_ddl()]

    def _get_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        raise NotImplementedError

    def _renew_query(self, key: str, encoded_expires_at: Any) -> tuple[str, tuple[Any, ...]]:
        raise NotImplementedError

    def _set_query(self, key: str, data: bytes, encoded_expires_at: Any) -> tuple[str, tuple[Any, ...]]:
        raise NotImplementedError

    def _delete_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        raise NotImplementedError

    def _delete_all_query(self) -> tuple[str, tuple[Any, ...]]:
        raise NotImplementedError

    def _exists_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        raise NotImplementedError

    def _expires_in_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        raise NotImplementedError

    def _delete_expired_query(self, now: datetime) -> tuple[str, tuple[Any, ...]]:
        _ = now
        raise NotImplementedError

    def _run_companion_reconcile_sync(self, driver: Any) -> None:
        for index in self.get_companion_indexes():
            rows = driver.data_dictionary.get_indexes(driver, table=index.table, schema=index.schema)
            if not metadata_has_index(rows, index.table, index.name, schema_name=index.schema):
                driver.execute_script(index.ddl)

    async def _run_companion_reconcile_async(self, driver: Any) -> None:
        for index in self.get_companion_indexes():
            rows = await driver.data_dictionary.get_indexes(driver, table=index.table, schema=index.schema)
            if not metadata_has_index(rows, index.table, index.name, schema_name=index.schema):
                await driver.execute_script(index.ddl)

    def downgrade_drops(self) -> list[DropStatement]:
        """Return ordered downgrade drop statements for the session store."""
        return [DropStatement(statement) for statement in self._drop_table_sql()]

    async def _dispatch_sync[T](self, fn: Callable[..., T], *args: Any) -> T:
        if self._use_threadpool_for_sync:
            return await async_(fn)(*args)
        return fn(*args)

    def _create_table_sync(self) -> None:
        statements = self._create_table_statements()
        with self._config.provide_session(**self._ddl_session_kwargs()) as driver:
            self._prepare_driver_sync(driver)
            if self._create_uses_script:
                for stmt in statements:
                    driver.execute_script(stmt)
            else:
                for stmt in statements:
                    driver.execute(stmt)
            if self._commit_after_write:
                driver.commit()
        if self._log_writes:
            self._log_table_created()

    async def create_table(self) -> None:
        """Create the Litestar session table when schema creation is enabled."""
        if self._check_create_schema_enabled and not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        if self._is_async_config():
            statements = self._create_table_statements()
            async with self._config.provide_session(**self._ddl_session_kwargs()) as driver:
                await self._prepare_driver_async(driver)
                if self._create_uses_script:
                    for stmt in statements:
                        await driver.execute_script(stmt)
                else:
                    for stmt in statements:
                        await driver.execute(stmt)
                if self._commit_after_write:
                    await driver.commit()
            if self._log_writes:
                self._log_table_created()
        else:
            await self._dispatch_sync(self._create_table_sync)
        if self._reconcile_after_create_table:
            await self.reconcile_schema(assume_existing=True)

    def _get_sync(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        now = datetime.now(UTC)
        select_sql, select_params = self._get_query(key)
        try:
            if self._separate_write_session_on_get:
                with self._config.provide_session(**self._read_session_kwargs()) as driver:
                    self._prepare_driver_sync(driver)
                    row = driver.select_one_or_none(select_sql, *select_params)
                if row is None:
                    return None
                expires_raw = self._row_value(row, "expires_at", 1)
                if self._check_expiry_in_python and expires_raw is not None:
                    expires_dt = self._decode_expires_at(expires_raw)
                    if expires_dt is not None and expires_dt <= now:
                        del_sql, del_params = self._delete_query(key)
                        with self._config.provide_session(**self._write_session_kwargs()) as w_driver:
                            w_driver.execute(del_sql, *del_params)
                            if self._commit_after_write:
                                w_driver.commit()
                        return None
                if renew_for is not None and (self._check_expiry_in_python or expires_raw is not None):
                    new_exp = self._calculate_expires_at(renew_for)
                    if new_exp is not None:
                        upd_sql, upd_params = self._renew_query(key, self._encode_expires_at(new_exp))
                        with self._config.provide_session(**self._write_session_kwargs()) as w_driver:
                            w_driver.execute(upd_sql, *upd_params)
                            if self._commit_after_write:
                                w_driver.commit()
                return self._coerce_bytes(self._row_value(row, "data", 0))

            with self._config.provide_session(**self._read_session_kwargs()) as driver:
                self._prepare_driver_sync(driver)
                row = driver.select_one_or_none(select_sql, *select_params)
                if row is None:
                    return None
                expires_raw = self._row_value(row, "expires_at", 1)
                if self._check_expiry_in_python and expires_raw is not None:
                    expires_dt = self._decode_expires_at(expires_raw)
                    if expires_dt is not None and expires_dt <= now:
                        del_sql, del_params = self._delete_query(key)
                        driver.execute(del_sql, *del_params)
                        if self._commit_after_write:
                            driver.commit()
                        return None
                if renew_for is not None and (self._check_expiry_in_python or expires_raw is not None):
                    new_exp = self._calculate_expires_at(renew_for)
                    if new_exp is not None:
                        upd_sql, upd_params = self._renew_query(key, self._encode_expires_at(new_exp))
                        driver.execute(upd_sql, *upd_params)
                        if self._commit_after_write:
                            driver.commit()
                return self._coerce_bytes(self._row_value(row, "data", 0))
        except Exception as exc:
            if self._is_table_missing(exc):
                return None
            raise

    async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        """Retrieve session payload bytes by key, optionally sliding its expiration timestamp."""
        if not self._is_async_config():
            return await self._dispatch_sync(self._get_sync, key, renew_for)
        now = datetime.now(UTC)
        select_sql, select_params = self._get_query(key)
        try:
            if self._separate_write_session_on_get:
                async with self._config.provide_session(**self._read_session_kwargs()) as driver:
                    await self._prepare_driver_async(driver)
                    row = await driver.select_one_or_none(select_sql, *select_params)
                if row is None:
                    return None
                expires_raw = self._row_value(row, "expires_at", 1)
                if self._check_expiry_in_python and expires_raw is not None:
                    expires_dt = self._decode_expires_at(expires_raw)
                    if expires_dt is not None and expires_dt <= now:
                        del_sql, del_params = self._delete_query(key)
                        async with self._config.provide_session(**self._write_session_kwargs()) as w_driver:
                            await w_driver.execute(del_sql, *del_params)
                            if self._commit_after_write:
                                await w_driver.commit()
                        return None
                if renew_for is not None and (self._check_expiry_in_python or expires_raw is not None):
                    new_exp = self._calculate_expires_at(renew_for)
                    if new_exp is not None:
                        upd_sql, upd_params = self._renew_query(key, self._encode_expires_at(new_exp))
                        async with self._config.provide_session(**self._write_session_kwargs()) as w_driver:
                            await w_driver.execute(upd_sql, *upd_params)
                            if self._commit_after_write:
                                await w_driver.commit()
                return self._coerce_bytes(self._row_value(row, "data", 0))

            async with self._config.provide_session(**self._read_session_kwargs()) as driver:
                await self._prepare_driver_async(driver)
                row = await driver.select_one_or_none(select_sql, *select_params)
                if row is None:
                    return None
                expires_raw = self._row_value(row, "expires_at", 1)
                if self._check_expiry_in_python and expires_raw is not None:
                    expires_dt = self._decode_expires_at(expires_raw)
                    if expires_dt is not None and expires_dt <= now:
                        del_sql, del_params = self._delete_query(key)
                        await driver.execute(del_sql, *del_params)
                        if self._commit_after_write:
                            await driver.commit()
                        return None
                if renew_for is not None and (self._check_expiry_in_python or expires_raw is not None):
                    new_exp = self._calculate_expires_at(renew_for)
                    if new_exp is not None:
                        upd_sql, upd_params = self._renew_query(key, self._encode_expires_at(new_exp))
                        await driver.execute(upd_sql, *upd_params)
                        if self._commit_after_write:
                            await driver.commit()
                return self._coerce_bytes(self._row_value(row, "data", 0))
        except Exception as exc:
            if self._is_table_missing(exc):
                return None
            raise

    def _set_sync(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None:
        data = self._value_to_bytes(value)
        encoded_exp = self._encode_expires_at(self._calculate_expires_at(expires_in))
        sql, params = self._set_query(key, data, encoded_exp)
        with self._config.provide_session(**self._write_session_kwargs()) as driver:
            self._prepare_driver_sync(driver)
            driver.execute(sql, *params)
            if self._commit_after_write:
                driver.commit()

    async def set(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None:
        """Insert or update a session record."""
        if not self._is_async_config():
            await self._dispatch_sync(self._set_sync, key, value, expires_in)
            return
        data = self._value_to_bytes(value)
        encoded_exp = self._encode_expires_at(self._calculate_expires_at(expires_in))
        sql, params = self._set_query(key, data, encoded_exp)
        async with self._config.provide_session(**self._write_session_kwargs()) as driver:
            await self._prepare_driver_async(driver)
            await driver.execute(sql, *params)
            if self._commit_after_write:
                await driver.commit()

    def _delete_sync(self, key: str) -> None:
        sql, params = self._delete_query(key)
        try:
            with self._config.provide_session(**self._write_session_kwargs()) as driver:
                self._prepare_driver_sync(driver)
                driver.execute(sql, *params)
                if self._commit_after_write:
                    driver.commit()
        except Exception as exc:
            if self._is_table_missing(exc):
                return
            raise

    async def delete(self, key: str) -> None:
        """Delete a single session row by key."""
        if not self._is_async_config():
            await self._dispatch_sync(self._delete_sync, key)
            return
        sql, params = self._delete_query(key)
        try:
            async with self._config.provide_session(**self._write_session_kwargs()) as driver:
                await self._prepare_driver_async(driver)
                await driver.execute(sql, *params)
                if self._commit_after_write:
                    await driver.commit()
        except Exception as exc:
            if self._is_table_missing(exc):
                return
            raise

    def _delete_all_sync(self) -> None:
        sql, params = self._delete_all_query()
        try:
            with self._config.provide_session(**self._write_session_kwargs()) as driver:
                self._prepare_driver_sync(driver)
                driver.execute(sql, *params)
                if self._commit_after_write:
                    driver.commit()
            if self._log_writes:
                self._log_delete_all()
        except Exception as exc:
            if self._is_table_missing(exc):
                return
            raise

    async def delete_all(self) -> None:
        """Delete all stored session rows."""
        if not self._is_async_config():
            await self._dispatch_sync(self._delete_all_sync)
            return
        sql, params = self._delete_all_query()
        try:
            async with self._config.provide_session(**self._write_session_kwargs()) as driver:
                await self._prepare_driver_async(driver)
                await driver.execute(sql, *params)
                if self._commit_after_write:
                    await driver.commit()
            if self._log_writes:
                self._log_delete_all()
        except Exception as exc:
            if self._is_table_missing(exc):
                return
            raise

    def _exists_sync(self, key: str) -> bool:
        sql, params = self._exists_query(key)
        now = datetime.now(UTC)
        try:
            with self._config.provide_session(**self._read_session_kwargs()) as driver:
                self._prepare_driver_sync(driver)
                if self._uses_select_value:
                    return driver.select_value_or_none(sql, *params) is not None
                row = driver.select_one_or_none(sql, *params)
            if row is None:
                return False
            if self._check_expiry_in_python:
                expires_raw = self._row_value(row, "expires_at", 0)
                if expires_raw is None:
                    return True
                expires_dt = self._decode_expires_at(expires_raw)
                return expires_dt is None or expires_dt > now
            return True
        except Exception as exc:
            if self._is_table_missing(exc):
                return False
            raise

    async def exists(self, key: str) -> bool:
        """Return True when an unexpired session exists for key."""
        if not self._is_async_config():
            return await self._dispatch_sync(self._exists_sync, key)
        sql, params = self._exists_query(key)
        now = datetime.now(UTC)
        try:
            async with self._config.provide_session(**self._read_session_kwargs()) as driver:
                await self._prepare_driver_async(driver)
                if self._uses_select_value:
                    return await driver.select_value_or_none(sql, *params) is not None
                row = await driver.select_one_or_none(sql, *params)
            if row is None:
                return False
            if self._check_expiry_in_python:
                expires_raw = self._row_value(row, "expires_at", 0)
                if expires_raw is None:
                    return True
                expires_dt = self._decode_expires_at(expires_raw)
                return expires_dt is None or expires_dt > now
            return True
        except Exception as exc:
            if self._is_table_missing(exc):
                return False
            raise

    def _expires_in_sync(self, key: str) -> int | None:
        sql, params = self._expires_in_query(key)
        try:
            with self._config.provide_session(**self._read_session_kwargs()) as driver:
                self._prepare_driver_sync(driver)
                if self._uses_select_value:
                    raw = driver.select_value_or_none(sql, *params)
                else:
                    row = driver.select_one_or_none(sql, *params)
                    raw = None if row is None else self._row_value(row, "expires_at", 0)
        except Exception as exc:
            if self._is_table_missing(exc):
                return None
            raise
        expires_dt = self._decode_expires_at(raw)
        if expires_dt is None:
            return None
        return max(0, int((expires_dt - datetime.now(UTC)).total_seconds()))

    async def expires_in(self, key: str) -> int | None:
        """Return remaining lifetime in seconds for key, or None when absent or permanent."""
        if not self._is_async_config():
            return await self._dispatch_sync(self._expires_in_sync, key)
        sql, params = self._expires_in_query(key)
        try:
            async with self._config.provide_session(**self._read_session_kwargs()) as driver:
                await self._prepare_driver_async(driver)
                if self._uses_select_value:
                    raw = await driver.select_value_or_none(sql, *params)
                else:
                    row = await driver.select_one_or_none(sql, *params)
                    raw = None if row is None else self._row_value(row, "expires_at", 0)
        except Exception as exc:
            if self._is_table_missing(exc):
                return None
            raise
        expires_dt = self._decode_expires_at(raw)
        if expires_dt is None:
            return None
        return max(0, int((expires_dt - datetime.now(UTC)).total_seconds()))

    def _delete_expired_sync(self) -> int:
        sql, params = self._delete_expired_query(datetime.now(UTC))
        try:
            with self._config.provide_session(**self._write_session_kwargs()) as driver:
                self._prepare_driver_sync(driver)
                if self._delete_expired_uses_select:
                    rows = driver.select(sql, *params)
                    count = len(rows)
                else:
                    result = driver.execute(sql, *params)
                    raw_count = getattr(result, "rows_affected", 0) or getattr(result, "num_rows", 0) or 0
                    count = int(raw_count) if raw_count > 0 else 0
                if self._commit_after_write:
                    driver.commit()
            if count > 0 and self._log_writes:
                self._log_delete_expired(count)
            return count
        except Exception as exc:
            if self._is_table_missing(exc):
                return 0
            raise

    async def delete_expired(self) -> int:
        """Delete all expired session rows and return the affected row count."""
        if not self._is_async_config():
            return await self._dispatch_sync(self._delete_expired_sync)
        sql, params = self._delete_expired_query(datetime.now(UTC))
        try:
            async with self._config.provide_session(**self._write_session_kwargs()) as driver:
                await self._prepare_driver_async(driver)
                if self._delete_expired_uses_select:
                    rows = await driver.select(sql, *params)
                    count = len(rows)
                else:
                    result = await driver.execute(sql, *params)
                    raw_count = getattr(result, "rows_affected", 0) or getattr(result, "num_rows", 0) or 0
                    count = int(raw_count) if raw_count > 0 else 0
                if self._commit_after_write:
                    await driver.commit()
            if count > 0 and self._log_writes:
                self._log_delete_expired(count)
            return count
        except Exception as exc:
            if self._is_table_missing(exc):
                return 0
            raise
