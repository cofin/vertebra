"""Asynchronous DuckDB analytical driver backed by `vertebra._duckdb`."""

from __future__ import annotations

import inspect
import itertools
import time
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Self

from anyio import CancelScope

from vertebra._vertebra import InterfaceError, NotFoundError
from vertebra.db.adapters.duckdb._sync import _raise_closed
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
from vertebra.utils.schema import to_value_type
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from vertebra._duckdb import AsyncDuckDBClient, AsyncDuckDBCursor

__all__ = ("AsyncDuckDBDriver",)

_TX_SAVEPOINT_COUNTER = itertools.count(1)


class AsyncDuckDBDriver(AsyncDatabaseDriver[DuckDBConfig]):
    """Asynchronous DuckDB analytical driver backed by `vertebra._duckdb.AsyncDuckDBClient`."""

    dialect: ClassVar[str] = "duckdb"
    dialect_tag: ClassVar[int] = DUCKDB_DIALECT_TAG

    __slots__ = (
        "_client",
        "_dynamic_secrets",
        "_executed_bootstrap_statements",
        "_fetch_dicts",
        "_fetch_rows",
        "_fetch_value",
        "_initial_secrets_applied",
        "_run_dml",
        "_secret_generations",
        "_secret_refresh_times",
    )

    _client: AsyncDuckDBClient | None
    _fetch_dicts: Callable[..., Any]
    _fetch_rows: Callable[..., Any]
    _fetch_value: Callable[..., Any]
    _run_dml: Callable[..., Any]

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
        self._initial_secrets_applied: bool = not self._dynamic_secrets
        self._secret_refresh_times: dict[str, float] = {}
        self._secret_generations: dict[str, int] = {}
        self._executed_bootstrap_statements: tuple[str, ...] = tuple(self.config.bootstrap_statements())
        if _client is not None:
            self._bind_client(_client)
        else:
            native_mod = ensure_module(
                "vertebra._duckdb",
                package="vertebra-core[duckdb]",
                extra="duckdb",
            )
            init_stmts = [s.to_create_secret_sql() for s in normalized_secrets if s.value_provider is None]
            init_stmts.extend(self.config.init_statements)
            self._bind_client(
                native_mod.AsyncDuckDBClient(
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
            )

    def _bind_client(self, client: AsyncDuckDBClient | None) -> None:
        """Set the client and bind its single-call methods, or the closed-driver error when there is none."""
        self._client = client
        if client is None:
            self._fetch_dicts = self._fetch_rows = self._fetch_value = self._run_dml = _raise_closed
            return
        self._fetch_dicts = client.fetch_dicts
        self._fetch_rows = client.fetch_rows
        self._fetch_value = client.fetch_value
        self._run_dml = client.execute_dml

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

    def cursor(self, *, lazy_begin: bool = False) -> AsyncDuckDBCursor:
        """Create a native asynchronous DBAPI-style cursor on the driver's client."""
        return self._require_client().cursor(lazy_begin=lazy_begin)

    def pipeline(self, *, transaction: bool = True) -> AsyncDuckDBPipeline:
        """Create an asynchronous delayed-commit query pipeline bound to this driver."""
        return AsyncDuckDBPipeline(self, transaction=transaction)

    async def begin(self) -> None:
        """Begin an explicit DuckDB transaction asynchronously."""
        if not self._initial_secrets_applied:
            await self._refresh_dynamic_secrets_async(force=True)
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
        self._bind_client(None)
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
        """Execute a DDL or DML statement asynchronously and return the affected row count."""
        _ = kwargs
        if not self._initial_secrets_applied:
            await self._refresh_dynamic_secrets_async(force=True)
        try:
            rowcount: int = (await self._run_dml(sql, parameters))[0]
        except Exception as exc:
            await self._on_native_error(exc)
            raise
        return rowcount

    async def execute_dml(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[int, int | None]:
        """Execute a DML or DDL statement asynchronously and return `(rowcount, None)`."""
        _ = kwargs
        if not self._initial_secrets_applied:
            await self._refresh_dynamic_secrets_async(force=True)
        try:
            result: tuple[int, int | None] = await self._run_dml(sql, parameters)
        except Exception as exc:
            await self._on_native_error(exc)
            raise
        return result

    async def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a batch statement asynchronously and return the affected row count."""
        _ = kwargs
        if not parameter_sets:
            return 0
        if not self._initial_secrets_applied:
            await self._refresh_dynamic_secrets_async(force=True)
        cur = self._require_client().cursor()
        try:
            await cur.executemany(sql, parameter_sets)
            return int(cur.rowcount)
        except Exception as exc:
            await self._on_native_error(exc)
            raise

    async def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute SQL and return `(rows, column_names)` asynchronously preserving column names on empty result sets."""
        _ = kwargs
        if not self._initial_secrets_applied:
            await self._refresh_dynamic_secrets_async(force=True)
        try:
            result: tuple[list[tuple[Any, ...]], tuple[str, ...]] = await self._fetch_rows(sql, parameters)
        except Exception as exc:
            await self._on_native_error(exc)
            raise
        return result

    async def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query asynchronously and return unboxed tuple rows."""
        _ = kwargs
        if not self._initial_secrets_applied:
            await self._refresh_dynamic_secrets_async(force=True)
        try:
            rows: list[tuple[Any, ...]] = (await self._fetch_rows(sql, parameters))[0]
        except Exception as exc:
            await self._on_native_error(exc)
            raise
        return rows

    async def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `SELECT` query asynchronously and hydrate rows into dictionaries or `schema_type` instances."""
        _ = kwargs
        if not self._initial_secrets_applied:
            await self._refresh_dynamic_secrets_async(force=True)
        if schema_type is None or schema_type is dict:
            try:
                dict_rows: list[Any] = await self._fetch_dicts(sql, parameters)
            except Exception as exc:
                await self._on_native_error(exc)
                raise
            return dict_rows
        try:
            rows, cols = await self._fetch_rows(sql, parameters)
        except Exception as exc:
            await self._on_native_error(exc)
            raise
        return self.hydrate_rows(rows, cols, schema_type)

    async def select_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query asynchronously and return the first row, raising `NotFoundError` when 0 rows match."""
        _ = kwargs
        if not self._initial_secrets_applied:
            await self._refresh_dynamic_secrets_async(force=True)
        if schema_type is None or schema_type is dict:
            try:
                rows = await self._fetch_dicts(sql, parameters)
            except Exception as exc:
                await self._on_native_error(exc)
                raise
        else:
            try:
                raw_rows, cols = await self._fetch_rows(sql, parameters)
            except Exception as exc:
                await self._on_native_error(exc)
                raise
            rows = self.hydrate_rows(raw_rows, cols, schema_type)
        if not rows:
            msg = "Query returned no rows"
            raise NotFoundError(msg)
        return rows[0]

    async def select_one_or_none(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any | None:
        """Execute a query asynchronously and return the first row or `None` when empty."""
        _ = kwargs
        if not self._initial_secrets_applied:
            await self._refresh_dynamic_secrets_async(force=True)
        if schema_type is None or schema_type is dict:
            try:
                rows = await self._fetch_dicts(sql, parameters)
            except Exception as exc:
                await self._on_native_error(exc)
                raise
        else:
            try:
                raw_rows, cols = await self._fetch_rows(sql, parameters)
            except Exception as exc:
                await self._on_native_error(exc)
                raise
            rows = self.hydrate_rows(raw_rows, cols, schema_type)
        return rows[0] if rows else None

    async def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query asynchronously and return the first scalar column, raising `NotFoundError` when 0 rows match."""
        _ = kwargs
        if not self._initial_secrets_applied:
            await self._refresh_dynamic_secrets_async(force=True)
        try:
            val = await self._fetch_value(sql, parameters)
        except Exception as exc:
            await self._on_native_error(exc)
            raise
        if value_type is not None and val is not None:
            return to_value_type(val, value_type)
        return val

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
        if not self._initial_secrets_applied:
            await self._refresh_dynamic_secrets_async(force=True)
        client = self._require_client()
        try:
            native_cursor = await client.select_to_arrow(
                sql,
                parameters,
                batch_size=max(1, int(batch_size)),
                threshold_rows=max(0, int(threshold_rows)),
            )
            return native_cursor if isinstance(native_cursor, ArrowCursor) else ArrowCursor(native_cursor)
        except Exception as exc:
            await self._on_native_error(exc)
            raise

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
    ) -> AsyncGenerator[Any, None]:
        """Stream Arrow record batches asynchronously via `vertebra._duckdb`."""
        _ = kwargs
        chunk = max(1, int(batch_size))
        if not self._initial_secrets_applied:
            await self._refresh_dynamic_secrets_async(force=True)
        client = self._require_client()
        try:
            native_cursors = await client.stream_arrow_batches(
                sql,
                parameters,
                batch_size=chunk,
            )
        except Exception as exc:
            await self._on_native_error(exc)
            raise

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
            await self._on_native_error(exc)
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

        if not self._initial_secrets_applied:
            await self._refresh_dynamic_secrets_async(force=True)
        client = self._require_client()
        schema = getattr(data, "schema", None)
        if isinstance(data, ArrowCursor) or schema is not None or isinstance(data, (bytes, bytearray, memoryview)):
            if schema is not None and col_list is None:
                for field_name in getattr(schema, "names", ()):
                    validate_identifier(str(field_name), "column", allow_dots=False)
            payload = data.buffer_view() if isinstance(data, ArrowCursor) and data.used_shm else data
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
                await self._on_native_error(exc)
                raise

        rows, cols = _table_to_rows_and_cols(data, columns)
        for col in cols:
            validate_identifier(str(col), "column", allow_dots=False)
        if not rows or not cols:
            return 0
        dict_rows = [{str(col): DuckDBCore.normalize_param(row[idx]) for idx, col in enumerate(cols)} for row in rows]
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
            await self._on_native_error(exc)
            raise

    def _require_client(self) -> AsyncDuckDBClient:
        client = self._client
        if client is None:
            msg = "DuckDB driver is closed"
            raise InterfaceError(msg)
        return client

    async def _on_native_error(self, exc: BaseException) -> None:
        """Invalidate rejected dynamic secrets and refresh them before the error propagates."""
        if not self._dynamic_secrets:
            return
        tracked = len(self._secret_generations)
        DuckDBCore.invalidate_rejected_credentials(exc, self._dynamic_secrets, self._secret_generations)
        if len(self._secret_generations) >= tracked:
            return
        try:
            await self._refresh_dynamic_secrets_async(force=True)
        except Exception as refresh_exc:
            exc.add_note(f"DuckDB secret refresh after credential rejection failed: {refresh_exc}")

    async def _cleanup_failed_startup_client(self) -> None:
        client = self._client
        self._bind_client(None)
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
                provider_gen = DuckDBCore.probe_provider_generation(provider)
                provider_needs_refresh = DuckDBCore.probe_provider_needs_refresh(provider)
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
                    updated_gen = DuckDBCore.probe_provider_generation(provider)
                    if updated_gen is not None:
                        self._secret_generations[key] = updated_gen
        except BaseException:
            if is_initial:
                await self._cleanup_failed_startup_client()
            raise
        self._initial_secrets_applied = True
