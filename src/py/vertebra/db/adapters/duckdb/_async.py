"""Asynchronous DuckDB analytical driver backed by `vertebra._duckdb`."""

from __future__ import annotations

import inspect
import itertools
import os
import re
import time
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Self

from anyio import CancelScope

from vertebra._vertebra import InterfaceError
from vertebra.db.adapters.duckdb.config import DuckDBConfig, DuckDBSecretConfig
from vertebra.db.adapters.duckdb.core import DUCKDB_DIALECT_TAG, DuckDBCore
from vertebra.db.adapters.duckdb.pipeline import AsyncDuckDBPipeline
from vertebra.db.arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    _table_to_rows_and_cols,
)
from vertebra.db.driver import AsyncDatabaseDriver
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from vertebra._duckdb import AsyncDuckDBClient

__all__ = (
    "AsyncDuckDBDriver",
    "_AsyncDuckDBCursor",
)

_TX_SAVEPOINT_COUNTER = itertools.count(1)
_CREDENTIAL_REJECTION_PATTERN = re.compile(r"\(HTTP (?:401|403)\)")


class AsyncDuckDBDriver(AsyncDatabaseDriver[DuckDBConfig]):
    """Asynchronous DuckDB analytical driver backed by `vertebra._duckdb.AsyncDuckDBClient`."""

    dialect: ClassVar[str] = "duckdb"
    dialect_tag: ClassVar[int] = DUCKDB_DIALECT_TAG

    __slots__ = (
        "_client",
        "_dynamic_secrets",
        "_executed_bootstrap_statements",
        "_initial_secrets_applied",
        "_secret_generations",
        "_secret_refresh_times",
    )

    def __init__(
        self,
        config: DuckDBConfig | None = None,
        *,
        broker: Any = None,
        _client: AsyncDuckDBClient | None = None,
    ) -> None:
        resolved = config or DuckDBConfig()
        if int(resolved.pool_max_size) <= 0:
            msg = "DuckDB pool_max_size must be at least 1"
            raise InterfaceError(msg)
        super().__init__(resolved, broker=broker)
        normalized_secrets = self.config.normalized_secrets()
        self._dynamic_secrets: tuple[DuckDBSecretConfig, ...] = tuple(
            s for s in normalized_secrets if s.value_provider is not None
        )
        self._initial_secrets_applied: bool = False
        self._secret_refresh_times: dict[str, float] = {}
        self._secret_generations: dict[str, int] = {}
        self._executed_bootstrap_statements: tuple[str, ...] = tuple(self.config.bootstrap_statements())
        if _client is not None:
            self._client: AsyncDuckDBClient | None = _client
        else:
            native_mod = ensure_module(
                "vertebra._duckdb",
                package="vertebra-core[duckdb]",
                extra="duckdb",
            )
            init_stmts = [s.to_create_secret_sql() for s in normalized_secrets if s.value_provider is None]
            init_stmts.extend(self.config.init_statements)
            self._client = native_mod.AsyncDuckDBClient(
                self.config.resolved_path(),
                read_only=self.config.read_only,
                max_connections=int(self.config.pool_max_size),
                max_memory_bytes=max(1, int(self.config.memory_limit_mb)) * 1024 * 1024,
                threads=max(1, int(self.config.threads)),
                temp_directory=self.config.temp_directory,
                parquet_metadata_cache=bool(self.config.parquet_metadata_cache),
                httpfs_connection_caching=bool(self.config.httpfs_connection_caching),
                enable_http_metadata_cache=bool(self.config.enable_http_metadata_cache),
                config=self.config.config or None,
                extensions=list(self.config.extensions) or None,
                init_statements=init_stmts or None,
            )

    @property
    def executed_bootstrap_statements(self) -> tuple[str, ...]:
        """Return the post-initialization `SET` statements executed on startup."""
        return self._executed_bootstrap_statements

    @property
    def in_transaction(self) -> bool:
        """Return `True` when an explicit DuckDB transaction is active on the writer session."""
        client = self._client
        return bool(client.in_transaction) if client is not None else False

    async def connect(self) -> Self:
        """Apply initial dynamic secrets when opening the asynchronous driver."""
        await super().connect()
        await self._refresh_dynamic_secrets_async(force=True)
        return self

    def cursor(self) -> _AsyncDuckDBCursor:
        """Create an asynchronous cursor bound to this driver."""
        return _AsyncDuckDBCursor(self)

    def pipeline(self, *, transaction: bool = True) -> AsyncDuckDBPipeline:
        """Create an asynchronous delayed-commit query pipeline bound to this driver."""
        return AsyncDuckDBPipeline(self, transaction=transaction)

    async def begin(self) -> None:
        """Begin an explicit DuckDB transaction asynchronously."""
        if not self._initial_secrets_applied:
            await self._refresh_dynamic_secrets_async()
        client = self._require_client()
        if not client.in_transaction:
            await client.begin()

    async def commit(self) -> None:
        """Commit the active DuckDB transaction asynchronously when one is open."""
        client = self._require_client()
        if client.in_transaction:
            await client.commit()

    async def rollback(self) -> None:
        """Roll back the active DuckDB transaction asynchronously when one is open."""
        client = self._require_client()
        if client.in_transaction:
            await client.rollback()

    async def savepoint(self, name: str) -> None:
        """Create a transactional savepoint asynchronously on the active DuckDB transaction."""
        await self._require_client().savepoint(name)

    async def rollback_to_savepoint(self, name: str) -> None:
        """Roll back the active DuckDB transaction asynchronously to savepoint `name`."""
        await self._require_client().rollback_to_savepoint(name)

    async def release_savepoint(self, name: str) -> None:
        """Release savepoint `name` asynchronously on the active DuckDB transaction."""
        await self._require_client().release_savepoint(name)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Self]:
        """Execute a block inside an async transaction or nested savepoint when already in a transaction."""
        if self.in_transaction:
            sp_name = f"_vtb_sp_{next(_TX_SAVEPOINT_COUNTER)}"
            await self.savepoint(sp_name)
            try:
                yield self
            except BaseException:
                with CancelScope(shield=True):
                    await self.rollback_to_savepoint(sp_name)
                raise
            else:
                await self.release_savepoint(sp_name)
            return

        await self.begin()
        try:
            yield self
        except BaseException:
            with CancelScope(shield=True):
                await self.rollback()
            raise
        else:
            await self.commit()

    async def cancel(self) -> None:
        """Interrupt any in-flight DuckDB query via `duckdb::InterruptHandle`."""
        client = self._client
        if not self._closed and client is not None:
            client.cancel()

    async def close(self) -> None:
        """Close the underlying DuckDB worker pool asynchronously."""
        self._closed = True
        client = self._client
        self._client = None
        if client is not None:
            await client.close()

    async def refresh_secrets(self, *, force: bool = True) -> None:
        """Evaluate configured sync or async `value_provider` secrets and apply `CREATE OR REPLACE SECRET`."""
        await self._refresh_dynamic_secrets_async(force=force)

    async def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DDL or DML statement asynchronously via the native completion ring."""
        _ = kwargs
        await self._refresh_dynamic_secrets_async()
        client = self._require_client()
        bound = DuckDBCore.normalize_parameters(parameters)
        grant = self._acquire_duckdb_lease(sql, DuckDBCore.param_len(bound))
        try:
            return int(await client.execute(sql, bound))
        except Exception as exc:
            self._invalidate_rejected_credentials(exc)
            raise
        finally:
            self._release_duckdb_lease(grant)

    async def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a batch statement asynchronously via the native completion ring."""
        _ = kwargs
        if not parameter_sets:
            return 0
        await self._refresh_dynamic_secrets_async()
        client = self._require_client()
        normalized_sets = [DuckDBCore.normalize_parameters(item) or () for item in parameter_sets]
        grant = self._acquire_duckdb_lease(sql, DuckDBCore.param_len(normalized_sets[0]))
        try:
            return int(await client.execute_many(sql, normalized_sets))
        except Exception as exc:
            self._invalidate_rejected_credentials(exc)
            raise
        finally:
            self._release_duckdb_lease(grant)

    async def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query returning tuple rows asynchronously."""
        _ = kwargs
        await self._refresh_dynamic_secrets_async()
        client = self._require_client()
        bound = DuckDBCore.normalize_parameters(parameters)
        grant = self._acquire_duckdb_lease(sql, DuckDBCore.param_len(bound))
        try:
            rows = await client.execute_tuples(sql, bound)
            return list(rows)
        except Exception as exc:
            self._invalidate_rejected_credentials(exc)
            raise
        finally:
            self._release_duckdb_lease(grant)

    async def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute SQL and return `(rows, column_names)` asynchronously preserving column names on empty result sets."""
        _ = kwargs
        await self._refresh_dynamic_secrets_async()
        client = self._require_client()
        bound = DuckDBCore.normalize_parameters(parameters)
        grant = self._acquire_duckdb_lease(sql, DuckDBCore.param_len(bound))
        try:
            rows, cols = await client.execute_with_description(sql, bound)
            return list(rows), tuple(str(c) for c in cols)
        except Exception as exc:
            self._invalidate_rejected_credentials(exc)
            raise
        finally:
            self._release_duckdb_lease(grant)

    async def select_with_total(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[Any], int]:
        """Execute an analytical `SELECT` query asynchronously and return `(rows, total_count)`."""
        rows = await self.select(sql, parameters, schema_type=schema_type, **kwargs)
        return rows, len(rows)

    async def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a query asynchronously and return an `ArrowCursor`, spilling to `VTBARROW` SHM at `threshold_rows`."""
        _ = kwargs
        await self._refresh_dynamic_secrets_async()
        client = self._require_client()
        bound = DuckDBCore.normalize_parameters(parameters)
        grant = self._acquire_duckdb_lease(sql, DuckDBCore.param_len(bound))
        try:
            native_cursor = await client.select_to_arrow(
                sql,
                bound,
                batch_size=max(1, int(batch_size)),
                threshold_rows=max(0, int(threshold_rows)),
            )
            return native_cursor if isinstance(native_cursor, ArrowCursor) else ArrowCursor(native_cursor)
        except Exception as exc:
            self._invalidate_rejected_credentials(exc)
            raise
        finally:
            self._release_duckdb_lease(grant)

    def stream_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> AsyncIterator[Any]:
        """Stream Arrow record batches asynchronously."""
        return self.stream_arrow_batches(sql, parameters, batch_size=batch_size, **kwargs)

    async def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> AsyncIterator[Any]:
        """Stream Arrow record batches asynchronously via `vertebra._duckdb`."""
        _ = kwargs
        chunk = max(1, int(batch_size))
        await self._refresh_dynamic_secrets_async()
        client = self._require_client()
        bound = DuckDBCore.normalize_parameters(parameters)
        grant = self._acquire_duckdb_lease(sql, DuckDBCore.param_len(bound))
        try:
            native_cursors = await client.stream_arrow_batches(
                sql,
                bound,
                batch_size=chunk,
            )
        except Exception as exc:
            self._invalidate_rejected_credentials(exc)
            raise
        finally:
            self._release_duckdb_lease(grant)

        idx_cur = 0
        try:
            while idx_cur < len(native_cursors):
                native_cursor = native_cursors[idx_cur]
                idx_cur += 1
                with ArrowCursor(native_cursor) as cursor:
                    if cursor.row_count == 0:
                        continue
                    table = cursor.to_arrow_table()
                    if hasattr(table, "to_batches"):
                        for batch in table.to_batches(max_chunksize=chunk):
                            yield batch
                    else:
                        rows = cursor.to_pylist()
                        for idx in range(0, len(rows), chunk):
                            yield rows[idx : idx + chunk]
        except Exception as exc:
            self._invalidate_rejected_credentials(exc)
            raise
        finally:
            for remaining in native_cursors[idx_cur:]:
                release_fn = getattr(remaining, "release", None)
                if callable(release_fn):
                    release_fn()

    async def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        create_if_missing: bool = False,
        **kwargs: Any,
    ) -> int:
        """Asynchronously bulk-ingest an `ArrowCursor`, PyArrow Table, or row sequence via DuckDB `Appender`."""
        _ = kwargs
        validate_identifier(table_name, "table_name", allow_dots=True)
        col_list: list[str] | None = None
        if columns is not None:
            col_list = []
            for col in columns:
                col_str = str(col)
                validate_identifier(col_str, "column", allow_dots=False)
                col_list.append(col_str)

        await self._refresh_dynamic_secrets_async()
        client = self._require_client()
        if isinstance(data, ArrowCursor) or hasattr(data, "schema") or isinstance(data, (bytes, bytearray, memoryview)):
            if hasattr(data, "schema") and col_list is None:
                for field_name in getattr(data.schema, "names", ()):
                    validate_identifier(str(field_name), "column", allow_dots=False)
            payload = data.buffer_view() if isinstance(data, ArrowCursor) and data.used_shm else data
            grant = self._acquire_duckdb_lease(f"APPENDER {table_name}", len(col_list or ()))
            try:
                return int(
                    await client.load_from_arrow(
                        table_name,
                        payload,
                        columns=col_list,
                        create_if_missing=create_if_missing,
                    )
                )
            except Exception as exc:
                self._invalidate_rejected_credentials(exc)
                raise
            finally:
                self._release_duckdb_lease(grant)

        rows, cols = _table_to_rows_and_cols(data, columns)
        for col in cols:
            validate_identifier(str(col), "column", allow_dots=False)
        if not rows or not cols:
            return 0
        dict_rows = [{str(col): DuckDBCore.normalize_param(row[idx]) for idx, col in enumerate(cols)} for row in rows]
        grant = self._acquire_duckdb_lease(f"APPENDER {table_name}", len(cols))
        try:
            return int(
                await client.load_from_arrow(
                    table_name,
                    dict_rows,
                    columns=[str(c) for c in cols],
                    create_if_missing=create_if_missing,
                )
            )
        except Exception as exc:
            self._invalidate_rejected_credentials(exc)
            raise
        finally:
            self._release_duckdb_lease(grant)

    def _require_client(self) -> AsyncDuckDBClient:
        client = self._client
        if client is None:
            msg = "DuckDB driver is closed"
            raise InterfaceError(msg)
        return client

    async def _cleanup_failed_startup_client(self) -> None:
        client = self._client
        self._client = None
        self._closed = True
        if client is not None:
            await client.close()

    async def _refresh_dynamic_secrets_async(self, *, force: bool = False) -> None:
        if not self._dynamic_secrets or self._closed:
            self._initial_secrets_applied = True
            return
        client = self._require_client()
        is_initial = not self._initial_secrets_applied
        now_monotonic = time.monotonic()
        try:
            for secret in self._dynamic_secrets:
                provider = secret.value_provider
                if provider is None:
                    continue
                key = secret.name.lower()
                provider_gen = self._probe_provider_generation(provider)
                provider_needs_refresh = self._probe_provider_needs_refresh(provider)
                last_refreshed = self._secret_refresh_times.get(key)
                applied_gen = self._secret_generations.get(key)
                interval_due = (
                    last_refreshed is None or (now_monotonic - last_refreshed) >= secret.effective_refresh_interval()
                )
                gen_changed = provider_gen is not None and provider_gen != applied_gen
                if not (force or interval_due or provider_needs_refresh or gen_changed):
                    continue
                try:
                    candidate = provider()
                    resolved = await candidate if inspect.isawaitable(candidate) else candidate
                    ddl = secret.to_create_secret_sql(resolved)
                    await client.execute(ddl, None)
                except Exception:
                    if secret.required:
                        raise
                else:
                    self._secret_refresh_times[key] = now_monotonic
                    updated_gen = self._probe_provider_generation(provider)
                    if updated_gen is not None:
                        self._secret_generations[key] = updated_gen
        except BaseException:
            if is_initial:
                await self._cleanup_failed_startup_client()
            raise
        self._initial_secrets_applied = True

    def _acquire_duckdb_lease(self, sql: str, param_count: int) -> Any:
        if self._broker is None:
            return None
        if hasattr(self._broker, "statement_cache"):
            sc_attr = self._broker.statement_cache
            sc = sc_attr() if callable(sc_attr) else sc_attr
            if sc is not None and hasattr(sc, "lookup") and hasattr(sc, "insert"):
                sql_bytes = sql.encode("utf-8")
                try:
                    cached = sc.lookup(DUCKDB_DIALECT_TAG, sql)
                    if cached is None:
                        sc.insert(
                            DUCKDB_DIALECT_TAG,
                            sql,
                            param_count,
                            sql_bytes,
                            b"",
                        )
                except TypeError:
                    cached = sc.lookup(DUCKDB_DIALECT_TAG, sql_bytes)
                    if cached is None:
                        sc.insert(
                            DUCKDB_DIALECT_TAG,
                            sql_bytes,
                            param_count,
                            sql_bytes,
                            b"",
                        )
        if hasattr(self._broker, "lease_table") and self._broker.lease_table is not None:
            return self._broker.lease_table.acquire(os.getpid())
        if hasattr(self._broker, "acquire"):
            try:
                return self._broker.acquire(os.getpid())
            except TypeError:
                return self._broker.acquire(bind_key="duckdb")
        return None

    def _release_duckdb_lease(self, grant: Any) -> None:
        if grant is not None and self._broker is not None:
            if hasattr(self._broker, "lease_table") and self._broker.lease_table is not None:
                self._broker.lease_table.release(grant.slot_index, grant.generation)
            elif hasattr(self._broker, "release") and hasattr(grant, "slot_index"):
                self._broker.release(grant.slot_index, grant.generation)

    def _invalidate_rejected_credentials(self, exc: BaseException) -> None:
        if not _CREDENTIAL_REJECTION_PATTERN.search(str(exc)):
            return
        for secret in self._dynamic_secrets:
            provider = secret.value_provider
            if provider is None:
                continue
            key = secret.name.lower()
            applied_gen = self._secret_generations.get(key)
            if applied_gen is None:
                continue
            target = getattr(provider, "__self__", provider)
            invalidate_fn = getattr(target, "invalidate", None)
            if callable(invalidate_fn):
                invalidate_fn(applied_gen)
                self._secret_generations.pop(key, None)

    @staticmethod
    def _probe_provider_generation(provider: Any) -> int | None:
        target = getattr(provider, "__self__", provider)
        gen = getattr(target, "generation", None)
        if isinstance(gen, int) and not isinstance(gen, bool):
            return gen
        return None

    @staticmethod
    def _probe_provider_needs_refresh(provider: Any) -> bool:
        target = getattr(provider, "__self__", provider)
        fn = getattr(target, "needs_refresh", None)
        if callable(fn):
            return bool(fn())
        return False


class _AsyncDuckDBCursor:
    """Asynchronous cursor delegating to `AsyncDuckDBDriver`."""

    __slots__ = ("_driver", "_pos", "_rows", "arraysize", "description", "rowcount")

    def __init__(self, driver: AsyncDuckDBDriver) -> None:
        self._driver = driver
        self._rows: list[tuple[Any, ...]] = []
        self._pos: int = 0
        self.arraysize: int = 1
        self.description: tuple[tuple[str, None, None, None, None, None, None], ...] | None = None
        self.rowcount: int = -1

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()

    async def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> Self:
        """Execute a statement or query asynchronously and populate cursor state."""
        if DuckDBCore.is_write_statement(sql):
            self.rowcount = await self._driver.execute(sql, parameters)
            self._rows = []
            self._pos = 0
            self.description = None
            return self
        rows, cols = await self._driver.execute_with_description(sql, parameters)
        self._rows = rows
        self._pos = 0
        self.rowcount = len(rows)
        self.description = tuple((col, None, None, None, None, None, None) for col in cols) if cols else None
        return self

    async def executemany(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> Self:
        """Execute a statement across multiple parameter sets asynchronously."""
        self.rowcount = await self._driver.execute_many(sql, parameter_sets)
        self._rows = []
        self._pos = 0
        self.description = None
        return self

    async def fetchone(self) -> tuple[Any, ...] | None:
        """Return the next row tuple or `None` when exhausted."""
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    async def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Return up to `size` remaining row tuples."""
        limit = self.arraysize if size is None else max(0, int(size))
        batch = self._rows[self._pos : self._pos + limit]
        self._pos += len(batch)
        return batch

    async def fetchall(self) -> list[tuple[Any, ...]]:
        """Return all remaining row tuples."""
        rest = self._rows[self._pos :]
        self._pos = len(self._rows)
        return rest

    async def close(self) -> None:
        """Clear buffered cursor rows."""
        self._rows = []
        self._pos = 0
