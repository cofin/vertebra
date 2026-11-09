"""Synchronous and asynchronous DuckDB analytical drivers backed by `vertebra._duckdb`."""

from __future__ import annotations

import datetime
import inspect
import itertools
import os
from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from contextlib import asynccontextmanager, contextmanager
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar, Self
from uuid import UUID

from anyio import CancelScope

from vertebra._vertebra import InterfaceError
from vertebra.db.adapters.duckdb.config import DuckDBConfig
from vertebra.db.adapters.duckdb.core import DUCKDB_DIALECT_TAG, DuckDBCore
from vertebra.db.adapters.duckdb.pipeline import (
    AsyncDuckDBPipeline,
    SyncDuckDBPipeline,
)
from vertebra.db.arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    _table_to_rows_and_cols,
)
from vertebra.db.driver import AsyncDatabaseDriver, SyncDatabaseDriver
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import encode_json
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from vertebra._duckdb import AsyncDuckDBClient, SyncDuckDBClient

__all__ = (
    "AsyncDuckDBDriver",
    "SyncDuckDBDriver",
)

_TX_SAVEPOINT_COUNTER = itertools.count(1)


class SyncDuckDBDriver(SyncDatabaseDriver[DuckDBConfig]):
    """Synchronous DuckDB analytical driver backed by `vertebra._duckdb.SyncDuckDBClient`."""

    dialect: ClassVar[str] = "duckdb"
    dialect_tag: ClassVar[int] = DUCKDB_DIALECT_TAG

    def __init__(
        self,
        config: DuckDBConfig | None = None,
        *,
        broker: Any = None,
        _client: SyncDuckDBClient | None = None,
    ) -> None:
        resolved = config or DuckDBConfig()
        if int(resolved.pool_max_size) <= 0:
            msg = "DuckDB pool_max_size must be at least 1"
            raise InterfaceError(msg)
        super().__init__(resolved, broker=broker)
        self._cached_gcs_snapshot: Any = None
        self.executed_bootstrap_statements: tuple[str, ...] = tuple(self.config.bootstrap_statements())

        if _client is not None:
            self._client: SyncDuckDBClient = _client
        else:
            native_mod = ensure_module(
                "vertebra._duckdb",
                package="vertebra-core[duckdb]",
                extra="duckdb",
            )
            init_stmts = [secret.to_create_secret_sql() for secret in self.config.secrets]
            init_stmts.extend(self.config.init_statements)
            self._client = native_mod.SyncDuckDBClient(
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
    def applied_token_generation(self) -> int:
        """Return the GCS OAuth2 token generation currently applied to this DuckDB client."""
        return int(self._client.applied_token_generation)

    @property
    def last_gcs_secret_sql(self) -> str:
        """Return the last `CREATE OR REPLACE SECRET` SQL statement applied for GCS ADC auth."""
        return str(self._client.last_gcs_secret_sql or "")

    @property
    def in_transaction(self) -> bool:
        """Return `True` when an explicit DuckDB transaction is active on the writer session."""
        return bool(self._client.in_transaction)

    def cursor(self) -> _SyncDuckDBCursor:
        """Create a synchronous DBAPI-style cursor bound to this driver."""
        return _SyncDuckDBCursor(self)

    def pipeline(self, *, transaction: bool = True) -> SyncDuckDBPipeline:
        """Create a synchronous delayed-commit query pipeline bound to this driver."""
        return SyncDuckDBPipeline(self, transaction=transaction)

    def begin(self) -> None:
        """Begin an explicit DuckDB transaction on the writer session."""
        if not self._client.in_transaction:
            self._client.begin()

    def commit(self) -> None:
        """Commit the active DuckDB transaction when one is open."""
        if self._client.in_transaction:
            self._client.commit()

    def rollback(self) -> None:
        """Roll back the active DuckDB transaction when one is open."""
        if self._client.in_transaction:
            self._client.rollback()

    def savepoint(self, name: str) -> None:
        """Create a transactional savepoint on the active DuckDB transaction."""
        self._client.savepoint(name)

    def rollback_to_savepoint(self, name: str) -> None:
        """Roll back the active DuckDB transaction to savepoint `name`."""
        self._client.rollback_to_savepoint(name)

    def release_savepoint(self, name: str) -> None:
        """Release savepoint `name` on the active DuckDB transaction."""
        self._client.release_savepoint(name)

    @contextmanager
    def transaction(self) -> Iterator[Self]:
        """Execute a block inside a transaction or nested savepoint when already in a transaction."""
        if self.in_transaction:
            sp_name = f"_vtb_sp_{next(_TX_SAVEPOINT_COUNTER)}"
            self.savepoint(sp_name)
            try:
                yield self
            except BaseException:
                self.rollback_to_savepoint(sp_name)
                raise
            else:
                self.release_savepoint(sp_name)
            return

        self.begin()
        try:
            yield self
        except BaseException:
            self.rollback()
            raise
        else:
            self.commit()

    def cancel(self) -> None:
        """Interrupt any in-flight DuckDB query via `duckdb::InterruptHandle`."""
        if not self._closed:
            self._client.cancel()

    def close(self) -> None:
        """Close the underlying `SyncDuckDBClient` worker pool."""
        self._closed = True
        self._client.close()

    def apply_gcs_secret(
        self,
        *,
        secret_name: str = "vertebra_gcs_adc",
        token: str | None = None,
        generation: int | None = None,
        token_cache: Any = None,
        scope: str | None = None,
    ) -> tuple[bool, int]:
        """Rotate or apply a DuckDB GCS `BEARER_TOKEN` secret across pooled sessions."""
        cache = token_cache if token_cache is not None else self.config.gcs_token_cache
        rotated, applied_gen = self._client.apply_gcs_secret(
            secret_name=secret_name,
            token=token,
            generation=generation,
            token_cache=cache,
            scope=scope,
        )
        return bool(rotated), int(applied_gen)

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DDL or DML statement under single-writer coordination."""
        _ = kwargs
        self._sync_gcs_token_secret()
        bound = _normalize_duckdb_parameters(parameters)
        grant = self._acquire_duckdb_lease(sql, _param_len(bound))
        try:
            return int(self._client.execute(sql, bound))
        finally:
            self._release_duckdb_lease(grant)

    def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a statement across multiple parameter sets under single-writer coordination."""
        _ = kwargs
        if not parameter_sets:
            return 0
        self._sync_gcs_token_secret()
        normalized_sets = [_normalize_duckdb_parameters(item) or () for item in parameter_sets]
        grant = self._acquire_duckdb_lease(sql, _param_len(normalized_sets[0]))
        try:
            return int(self._client.execute_many(sql, normalized_sets))
        finally:
            self._release_duckdb_lease(grant)

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return positional row tuples."""
        _ = kwargs
        self._sync_gcs_token_secret()
        bound = _normalize_duckdb_parameters(parameters)
        grant = self._acquire_duckdb_lease(sql, _param_len(bound))
        try:
            return list(self._client.execute_tuples(sql, bound))
        finally:
            self._release_duckdb_lease(grant)

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute SQL and return `(tuple_rows, column_names)` preserving column names on empty result sets."""
        _ = kwargs
        self._sync_gcs_token_secret()
        bound = _normalize_duckdb_parameters(parameters)
        grant = self._acquire_duckdb_lease(sql, _param_len(bound))
        try:
            rows, cols = self._client.execute_with_description(sql, bound)
            return list(rows), tuple(str(c) for c in cols)
        finally:
            self._release_duckdb_lease(grant)

    def select_with_total(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[Any], int]:
        """Execute a `SELECT` query and return `(rows, total_count)`."""
        rows = self.select(sql, parameters, schema_type=schema_type, **kwargs)
        return rows, len(rows)

    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Stream analytical query results into a zero-copy `VTBARROW` `ShmRegion`-backed `ArrowCursor`."""
        _ = (threshold_rows, kwargs)
        self._sync_gcs_token_secret()
        bound = _normalize_duckdb_parameters(parameters)
        grant = self._acquire_duckdb_lease(sql, _param_len(bound))
        try:
            native_cursor = self._client.select_to_arrow(
                sql,
                bound,
                batch_size=max(1, int(batch_size)),
            )
            return ArrowCursor(native_cursor)
        finally:
            self._release_duckdb_lease(grant)

    def stream_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> Iterator[Any]:
        """Yield Arrow record batches of at most `batch_size` rows."""
        return self.stream_arrow_batches(sql, parameters, batch_size=batch_size, **kwargs)

    def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> Iterator[Any]:
        """Stream Arrow record batches from `vertebra._duckdb` shared-memory cursors."""
        _ = kwargs
        chunk = max(1, int(batch_size))
        self._sync_gcs_token_secret()
        bound = _normalize_duckdb_parameters(parameters)
        grant = self._acquire_duckdb_lease(sql, _param_len(bound))
        try:
            native_cursors = self._client.stream_arrow_batches(
                sql,
                bound,
                batch_size=chunk,
            )
        finally:
            self._release_duckdb_lease(grant)

        for native_cursor in native_cursors:
            with ArrowCursor(native_cursor) as cursor:
                if cursor.row_count == 0:
                    continue
                table = cursor.to_arrow_table()
                if hasattr(table, "to_batches"):
                    yield from table.to_batches(max_chunksize=chunk)
                else:
                    rows = cursor.to_pylist()
                    for idx in range(0, len(rows), chunk):
                        yield rows[idx : idx + chunk]

    def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        create_if_missing: bool = False,
        **kwargs: Any,
    ) -> int:
        """Bulk-ingest an `ArrowCursor`, PyArrow Table/RecordBatch, or row sequence via DuckDB `Appender`."""
        _ = kwargs
        validate_identifier(table_name, "table_name", allow_dots=True)
        col_list: list[str] | None = None
        if columns is not None:
            col_list = []
            for col in columns:
                col_str = str(col)
                validate_identifier(col_str, "column", allow_dots=False)
                col_list.append(col_str)

        self._sync_gcs_token_secret()
        if isinstance(data, ArrowCursor) or hasattr(data, "schema") or isinstance(data, (bytes, bytearray, memoryview)):
            if hasattr(data, "schema") and col_list is None:
                for field_name in getattr(data.schema, "names", ()):
                    validate_identifier(str(field_name), "column", allow_dots=False)
            payload = data.buffer_view() if isinstance(data, ArrowCursor) and data.used_shm else data
            grant = self._acquire_duckdb_lease(f"APPENDER {table_name}", len(col_list or ()))
            try:
                return int(
                    self._client.load_from_arrow(
                        table_name,
                        payload,
                        columns=col_list,
                        create_if_missing=create_if_missing,
                    )
                )
            finally:
                self._release_duckdb_lease(grant)

        rows, cols = _table_to_rows_and_cols(data, columns)
        for col in cols:
            validate_identifier(str(col), "column", allow_dots=False)
        if not rows or not cols:
            return 0
        dict_rows = [{str(col): _normalize_duckdb_param(row[idx]) for idx, col in enumerate(cols)} for row in rows]
        grant = self._acquire_duckdb_lease(f"APPENDER {table_name}", len(cols))
        try:
            return int(
                self._client.load_from_arrow(
                    table_name,
                    dict_rows,
                    columns=[str(c) for c in cols],
                    create_if_missing=create_if_missing,
                )
            )
        finally:
            self._release_duckdb_lease(grant)

    def _sync_gcs_token_secret(self) -> None:
        """Ensure this DuckDB client's GCS `BEARER_TOKEN` secret matches the latest `GcpTokenCache` generation."""
        cache = self.config.gcs_token_cache
        if cache is None:
            return
        cache_gen = int(getattr(cache, "generation", 0) or 0)
        needs_refresh = bool(hasattr(cache, "needs_refresh") and cache.needs_refresh())
        if self.applied_token_generation == 0 or needs_refresh or cache_gen != self.applied_token_generation:
            snapshot: Any = None
            if (
                self._cached_gcs_snapshot is not None
                and hasattr(self._cached_gcs_snapshot, "generation")
                and int(self._cached_gcs_snapshot.generation) != self.applied_token_generation
            ):
                snapshot = self._cached_gcs_snapshot
            else:
                res = cache.get_token()
                if inspect.isawaitable(res):
                    close_fn = getattr(res, "close", None)
                    if callable(close_fn):
                        close_fn()
                    snapshot = self._cached_gcs_snapshot
                else:
                    snapshot = res
                    self._cached_gcs_snapshot = snapshot
            if snapshot is not None and int(snapshot.generation) != self.applied_token_generation:
                self._client.apply_gcs_secret(
                    secret_name="vertebra_gcs_adc",
                    token=str(snapshot.access_token),
                    generation=int(snapshot.generation),
                )

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


class AsyncDuckDBDriver(AsyncDatabaseDriver[DuckDBConfig]):
    """Asynchronous DuckDB analytical driver backed by `vertebra._duckdb.AsyncDuckDBClient`."""

    dialect: ClassVar[str] = "duckdb"
    dialect_tag: ClassVar[int] = DUCKDB_DIALECT_TAG

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
        if _client is not None:
            self._client: AsyncDuckDBClient = _client
        else:
            native_mod = ensure_module(
                "vertebra._duckdb",
                package="vertebra-core[duckdb]",
                extra="duckdb",
            )
            init_stmts = [secret.to_create_secret_sql() for secret in self.config.secrets]
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
        self._sync = SyncDuckDBDriver(
            resolved,
            broker=self._broker,
            _client=self._client.to_sync(),
        )

    @property
    def sync_driver(self) -> SyncDuckDBDriver:
        """Return a synchronous driver sharing the same underlying DuckDB session pool."""
        return self._sync

    @property
    def executed_bootstrap_statements(self) -> tuple[str, ...]:
        """Return the post-initialization `SET` statements executed on startup."""
        return self._sync.executed_bootstrap_statements

    @property
    def applied_token_generation(self) -> int:
        """Return the token generation currently applied to this DuckDB session."""
        return int(self._client.applied_token_generation)

    @property
    def last_gcs_secret_sql(self) -> str:
        """Return the last `CREATE OR REPLACE SECRET` SQL statement applied for GCS ADC auth."""
        return str(self._client.last_gcs_secret_sql or "")

    @property
    def in_transaction(self) -> bool:
        """Return `True` when an explicit DuckDB transaction is active on the writer session."""
        return bool(self._client.in_transaction)

    def cursor(self) -> _AsyncDuckDBCursor:
        """Create an asynchronous cursor bound to this driver."""
        return _AsyncDuckDBCursor(self)

    def pipeline(self, *, transaction: bool = True) -> AsyncDuckDBPipeline:
        """Create an asynchronous delayed-commit query pipeline bound to this driver."""
        return AsyncDuckDBPipeline(self, transaction=transaction)

    async def begin(self) -> None:
        """Begin an explicit DuckDB transaction asynchronously."""
        if not self._client.in_transaction:
            await self._client.begin()

    async def commit(self) -> None:
        """Commit the active DuckDB transaction asynchronously when one is open."""
        if self._client.in_transaction:
            await self._client.commit()

    async def rollback(self) -> None:
        """Roll back the active DuckDB transaction asynchronously when one is open."""
        if self._client.in_transaction:
            await self._client.rollback()

    async def savepoint(self, name: str) -> None:
        """Create a transactional savepoint asynchronously on the active DuckDB transaction."""
        await self._client.savepoint(name)

    async def rollback_to_savepoint(self, name: str) -> None:
        """Roll back the active DuckDB transaction asynchronously to savepoint `name`."""
        await self._client.rollback_to_savepoint(name)

    async def release_savepoint(self, name: str) -> None:
        """Release savepoint `name` asynchronously on the active DuckDB transaction."""
        await self._client.release_savepoint(name)

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
        if not self._closed:
            self._client.cancel()

    async def close(self) -> None:
        """Close the underlying DuckDB worker pool asynchronously."""
        self._closed = True
        self._sync._closed = True
        await self._client.close()

    async def apply_gcs_secret(
        self,
        *,
        secret_name: str = "vertebra_gcs_adc",
        token: str | None = None,
        generation: int | None = None,
        token_cache: Any = None,
        scope: str | None = None,
    ) -> tuple[bool, int]:
        """Asynchronously rotate or apply a DuckDB GCS `BEARER_TOKEN` secret across pooled sessions."""
        cache = token_cache if token_cache is not None else self.config.gcs_token_cache
        if token is None and cache is not None:
            get_fn = getattr(cache, "get_token", None) or getattr(cache, "get_token_async", None)
            if callable(get_fn):
                res = get_fn()
                snapshot = await res if inspect.isawaitable(res) else res
                if snapshot is not None:
                    token = str(snapshot.access_token)
                    if generation is None:
                        generation = int(snapshot.generation)
        rotated, applied_gen = await self._client.apply_gcs_secret(
            secret_name=secret_name,
            token=token,
            generation=generation,
            token_cache=cache if token is None else None,
            scope=scope,
        )
        return bool(rotated), int(applied_gen)

    async def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DDL or DML statement asynchronously via the native completion ring."""
        _ = kwargs
        await self._ensure_gcs_token_async()
        bound = _normalize_duckdb_parameters(parameters)
        grant = self._sync._acquire_duckdb_lease(sql, _param_len(bound))
        try:
            return int(await self._client.execute(sql, bound))
        finally:
            self._sync._release_duckdb_lease(grant)

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
        await self._ensure_gcs_token_async()
        normalized_sets = [_normalize_duckdb_parameters(item) or () for item in parameter_sets]
        grant = self._sync._acquire_duckdb_lease(sql, _param_len(normalized_sets[0]))
        try:
            return int(await self._client.execute_many(sql, normalized_sets))
        finally:
            self._sync._release_duckdb_lease(grant)

    async def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query returning tuple rows asynchronously."""
        _ = kwargs
        await self._ensure_gcs_token_async()
        bound = _normalize_duckdb_parameters(parameters)
        grant = self._sync._acquire_duckdb_lease(sql, _param_len(bound))
        try:
            rows = await self._client.execute_tuples(sql, bound)
            return list(rows)
        finally:
            self._sync._release_duckdb_lease(grant)

    async def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute SQL and return `(rows, column_names)` asynchronously preserving column names on empty result sets."""
        _ = kwargs
        await self._ensure_gcs_token_async()
        bound = _normalize_duckdb_parameters(parameters)
        grant = self._sync._acquire_duckdb_lease(sql, _param_len(bound))
        try:
            rows, cols = await self._client.execute_with_description(sql, bound)
            return list(rows), tuple(str(c) for c in cols)
        finally:
            self._sync._release_duckdb_lease(grant)

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
        """Execute a query asynchronously and return a zero-copy `VTBARROW` `ArrowCursor`."""
        _ = (threshold_rows, kwargs)
        await self._ensure_gcs_token_async()
        bound = _normalize_duckdb_parameters(parameters)
        grant = self._sync._acquire_duckdb_lease(sql, _param_len(bound))
        try:
            native_cursor = await self._client.select_to_arrow(
                sql,
                bound,
                batch_size=max(1, int(batch_size)),
            )
            return ArrowCursor(native_cursor)
        finally:
            self._sync._release_duckdb_lease(grant)

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
        await self._ensure_gcs_token_async()
        bound = _normalize_duckdb_parameters(parameters)
        grant = self._sync._acquire_duckdb_lease(sql, _param_len(bound))
        try:
            native_cursors = await self._client.stream_arrow_batches(
                sql,
                bound,
                batch_size=chunk,
            )
        finally:
            self._sync._release_duckdb_lease(grant)

        for native_cursor in native_cursors:
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

        await self._ensure_gcs_token_async()
        if isinstance(data, ArrowCursor) or hasattr(data, "schema") or isinstance(data, (bytes, bytearray, memoryview)):
            if hasattr(data, "schema") and col_list is None:
                for field_name in getattr(data.schema, "names", ()):
                    validate_identifier(str(field_name), "column", allow_dots=False)
            payload = data.buffer_view() if isinstance(data, ArrowCursor) and data.used_shm else data
            grant = self._sync._acquire_duckdb_lease(f"APPENDER {table_name}", len(col_list or ()))
            try:
                return int(
                    await self._client.load_from_arrow(
                        table_name,
                        payload,
                        columns=col_list,
                        create_if_missing=create_if_missing,
                    )
                )
            finally:
                self._sync._release_duckdb_lease(grant)

        rows, cols = _table_to_rows_and_cols(data, columns)
        for col in cols:
            validate_identifier(str(col), "column", allow_dots=False)
        if not rows or not cols:
            return 0
        dict_rows = [{str(col): _normalize_duckdb_param(row[idx]) for idx, col in enumerate(cols)} for row in rows]
        grant = self._sync._acquire_duckdb_lease(f"APPENDER {table_name}", len(cols))
        try:
            return int(
                await self._client.load_from_arrow(
                    table_name,
                    dict_rows,
                    columns=[str(c) for c in cols],
                    create_if_missing=create_if_missing,
                )
            )
        finally:
            self._sync._release_duckdb_lease(grant)

    async def _ensure_gcs_token_async(self) -> None:
        """Refresh and apply the GCS ADC token secret asynchronously when generation advances."""
        cache = self.config.gcs_token_cache
        if cache is None:
            return
        needs_refresh = bool(hasattr(cache, "needs_refresh") and cache.needs_refresh())
        cache_gen = int(getattr(cache, "generation", 0) or 0)
        if (
            self.applied_token_generation == 0
            or needs_refresh
            or cache_gen != self.applied_token_generation
            or self._sync._cached_gcs_snapshot is None
        ):
            get_fn = getattr(cache, "get_token", None) or getattr(cache, "get_token_async", None)
            if callable(get_fn):
                res = get_fn()
                snapshot = await res if inspect.isawaitable(res) else res
                self._sync._cached_gcs_snapshot = snapshot
                if snapshot is not None and int(snapshot.generation) != self.applied_token_generation:
                    await self._client.apply_gcs_secret(
                        secret_name="vertebra_gcs_adc",
                        token=str(snapshot.access_token),
                        generation=int(snapshot.generation),
                    )


class _SyncDuckDBCursor:
    """Synchronous DBAPI-style cursor delegating to `SyncDuckDBDriver`."""

    __slots__ = ("_driver", "_pos", "_rows", "arraysize", "description", "rowcount")

    def __init__(self, driver: SyncDuckDBDriver) -> None:
        self._driver = driver
        self._rows: list[tuple[Any, ...]] = []
        self._pos: int = 0
        self.arraysize: int = 1
        self.description: tuple[tuple[str, None, None, None, None, None, None], ...] | None = None
        self.rowcount: int = -1

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> Self:
        """Execute a statement or query and populate cursor state."""
        if DuckDBCore.is_write_statement(sql):
            self.rowcount = self._driver.execute(sql, parameters)
            self._rows = []
            self._pos = 0
            self.description = None
            return self
        rows, cols = self._driver.execute_with_description(sql, parameters)
        self._rows = rows
        self._pos = 0
        self.rowcount = len(rows)
        self.description = tuple((col, None, None, None, None, None, None) for col in cols) if cols else None
        return self

    def executemany(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> Self:
        """Execute a statement across multiple parameter sets."""
        self.rowcount = self._driver.execute_many(sql, parameter_sets)
        self._rows = []
        self._pos = 0
        self.description = None
        return self

    def fetchone(self) -> tuple[Any, ...] | None:
        """Return the next row tuple or `None` when exhausted."""
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Return up to `size` remaining row tuples."""
        limit = self.arraysize if size is None else max(0, int(size))
        batch = self._rows[self._pos : self._pos + limit]
        self._pos += len(batch)
        return batch

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Return all remaining row tuples."""
        rest = self._rows[self._pos :]
        self._pos = len(self._rows)
        return rest

    def close(self) -> None:
        """Clear buffered cursor rows."""
        self._rows = []
        self._pos = 0


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


def _normalize_duckdb_param(value: Any) -> Any:
    """Normalize rich Python bind values into DuckDB-compatible scalar types."""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (dict, list)):
        return encode_json(value)
    return value


def _normalize_duckdb_parameters(
    parameters: Sequence[Any] | Mapping[str, Any] | None,
) -> tuple[Any, ...] | dict[str, Any] | None:
    """Normalize positional or named bind parameters for `vertebra._duckdb`."""
    if parameters is None:
        return None
    if isinstance(parameters, Mapping):
        if not parameters:
            return None
        return {str(key): _normalize_duckdb_param(val) for key, val in parameters.items()}
    if not parameters:
        return None
    return tuple(_normalize_duckdb_param(val) for val in parameters)


def _param_len(bound: tuple[Any, ...] | dict[str, Any] | None) -> int:
    """Return the parameter count for statement cache registration."""
    if bound is None:
        return 0
    return len(bound)
