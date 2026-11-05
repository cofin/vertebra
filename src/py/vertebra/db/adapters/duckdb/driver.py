"""Synchronous and asynchronous DuckDB analytical drivers."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import os
import sqlite3
import threading
from typing import TYPE_CHECKING, Any, ClassVar

from vertebra.db.adapters.duckdb.config import DuckDBConfig
from vertebra.db.adapters.duckdb.core import DUCKDB_DIALECT_TAG, DuckDBCore
from vertebra.db.arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    _hydrate_rows_with_schema,
    _table_to_rows_and_cols,
)
from vertebra.db.driver import SyncDatabaseDriver, ThreadOffloadAsyncDatabaseDriver
from vertebra.utils.module_loader import import_optional

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Mapping, Sequence

__all__ = (
    "AsyncDuckDBDriver",
    "SyncDuckDBDriver",
)


class SyncDuckDBDriver(SyncDatabaseDriver[DuckDBConfig]):
    """Synchronous DuckDB analytical driver with single-writer coordination and SHM Arrow streaming."""

    dialect: ClassVar[str] = "duckdb"
    dialect_tag: ClassVar[int] = DUCKDB_DIALECT_TAG

    def __init__(self, config: DuckDBConfig | None = None, *, broker: Any = None) -> None:
        resolved = config or DuckDBConfig()
        super().__init__(resolved, broker=broker)
        self._writer_lock = threading.RLock()
        self._in_transaction: bool = False
        self.applied_token_generation: int = 0
        self.last_gcs_secret_sql: str = ""
        self._cached_gcs_snapshot: Any = None
        self.executed_bootstrap_statements: tuple[str, ...] = tuple(self.config.bootstrap_statements())

        duckdb_mod = import_optional("duckdb")
        self._using_native_duckdb = duckdb_mod is not None
        if duckdb_mod is not None:
            self._conn = duckdb_mod.connect(self.config.resolved_path(), read_only=self.config.read_only)
            for stmt in self.executed_bootstrap_statements:
                with contextlib.suppress(RuntimeError, ValueError, OSError):
                    self._conn.execute(stmt)
        else:
            db_path = self.config.resolved_path()
            if db_path.startswith("duckdb://"):
                db_path = db_path[len("duckdb://") :] or ":memory:"
            self._conn = sqlite3.connect(db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row

    def begin(self) -> None:
        """Begin an explicit DuckDB transaction."""
        with self._writer_lock:
            if self._using_native_duckdb:
                self._conn.execute("BEGIN TRANSACTION")
            elif not self._conn.in_transaction:
                self._conn.execute("BEGIN")
            self._in_transaction = True

    def commit(self) -> None:
        """Commit the active DuckDB transaction."""
        with self._writer_lock:
            try:
                if self._using_native_duckdb and not self._in_transaction:
                    return
                self._conn.commit()
            finally:
                self._in_transaction = False

    def rollback(self) -> None:
        """Roll back the active DuckDB transaction."""
        with self._writer_lock:
            try:
                if self._using_native_duckdb and not self._in_transaction:
                    return
                self._conn.rollback()
            finally:
                self._in_transaction = False

    def cancel(self) -> None:
        """Interrupt an active DuckDB query when supported, or act as an idempotent no-op."""
        interrupt_fn = getattr(self._conn, "interrupt", None)
        if not self._closed and callable(interrupt_fn):
            interrupt_fn()

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._closed = True
        self._in_transaction = False
        self._conn.close()

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DDL or DML statement under single-writer coordination."""
        _ = kwargs
        self._sync_gcs_token_secret()
        bound = tuple(parameters) if isinstance(parameters, (list, tuple)) else (parameters or ())
        grant = self._acquire_duckdb_lease(sql, len(bound) if isinstance(bound, tuple) else 0)
        try:
            with self._writer_lock:
                cur = self._conn.execute(sql, bound) if bound else self._conn.execute(sql)
                if not self._using_native_duckdb and not self._in_transaction:
                    self._conn.commit()
                if self._using_native_duckdb and cur.description and cur.description[0][0] == "Count":
                    count = cur.fetchone()
                    return int(count[0]) if count else 0
                rowcount = getattr(cur, "rowcount", 0)
                return max(int(rowcount or 0), 0)
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
        grant = self._acquire_duckdb_lease(sql, len(parameter_sets[0]))
        try:
            with self._writer_lock:
                if self._using_native_duckdb:
                    affected = 0
                    for parameters in parameter_sets:
                        cur = self._conn.execute(sql, parameters)
                        if cur.description and cur.description[0][0] == "Count":
                            count = cur.fetchone()
                            affected += int(count[0]) if count else 0
                    return affected
                cur = self._conn.executemany(sql, list(parameter_sets))
                if not self._in_transaction:
                    self._conn.commit()
                return max(int(cur.rowcount or 0), 0)
        finally:
            self._release_duckdb_lease(grant)

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return positional tuples."""
        _ = kwargs
        self._sync_gcs_token_secret()
        bound = tuple(parameters) if isinstance(parameters, (list, tuple)) else (parameters or ())
        grant = self._acquire_duckdb_lease(sql, len(bound) if isinstance(bound, tuple) else 0)
        try:
            with self._writer_lock:
                cur = self._conn.execute(sql, bound) if bound else self._conn.execute(sql)
                rows = [tuple(row) for row in cur.fetchall()]
                if not self._using_native_duckdb and not self._in_transaction and cur.description is None:
                    self._conn.commit()
                return rows
        finally:
            self._release_duckdb_lease(grant)

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute SQL and return `(rows, column_names)`."""
        _ = kwargs
        self._sync_gcs_token_secret()
        bound = tuple(parameters) if isinstance(parameters, (list, tuple)) else (parameters or ())
        grant = self._acquire_duckdb_lease(sql, len(bound) if isinstance(bound, tuple) else 0)
        try:
            with self._writer_lock:
                cur = self._conn.execute(sql, bound) if bound else self._conn.execute(sql)
                if cur.description is not None:
                    rows = [tuple(row) for row in cur.fetchall()]
                    cols = tuple(str(col[0]) for col in cur.description)
                    return rows, cols
                if not self._using_native_duckdb and not self._in_transaction:
                    self._conn.commit()
                return [], ()
        finally:
            self._release_duckdb_lease(grant)

    def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute an analytical `SELECT` query and return rows as dictionaries or `schema_type` instances."""
        _ = kwargs
        self._sync_gcs_token_secret()
        bound = tuple(parameters) if isinstance(parameters, (list, tuple)) else (parameters or ())
        grant = self._acquire_duckdb_lease(sql, len(bound) if isinstance(bound, tuple) else 0)
        try:
            if DuckDBCore.is_write_statement(sql):
                with self._writer_lock:
                    cur = self._conn.execute(sql, bound) if bound else self._conn.execute(sql)
                    rows = cur.fetchall()
                    cols = [str(col[0]) for col in (cur.description or ())]
            else:
                cur = self._conn.execute(sql, bound) if bound else self._conn.execute(sql)
                rows = cur.fetchall()
                cols = [str(col[0]) for col in (cur.description or ())]
            if not rows:
                return []
            tuple_rows = [tuple(row) for row in rows]
            return _hydrate_rows_with_schema(tuple_rows, cols, schema_type)
        finally:
            self._release_duckdb_lease(grant)

    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Stream analytical query results into a zero-copy `ShmRegion`-backed `ArrowCursor`."""
        rows, cols = self.execute_with_description(sql, parameters, **kwargs)
        return ArrowCursor.from_rows(rows, cols, threshold_rows=threshold_rows)

    def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> int:
        """Bulk-ingest an Arrow Table, `ArrowCursor`, or row sequence into `table_name`."""
        _ = kwargs
        self._sync_gcs_token_secret()
        rows, cols = _table_to_rows_and_cols(data, columns)
        if not rows or not cols:
            return 0
        placeholders = ", ".join("?" for _ in cols)
        col_list = ", ".join(cols)
        sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"
        grant = self._acquire_duckdb_lease(sql, len(cols))
        try:
            with self._writer_lock:
                self._conn.executemany(sql, rows)
                if not self._using_native_duckdb and not self._in_transaction:
                    self._conn.commit()
                return len(rows)
        finally:
            self._release_duckdb_lease(grant)

    def _sync_gcs_token_secret(self) -> None:
        """Ensure this DuckDB session's GCS `BEARER_TOKEN` secret matches the latest `GcpTokenCache` generation."""
        cache = self.config.gcs_token_cache
        if cache is None:
            return
        cache_gen = int(getattr(cache, "generation", 0) or 0)
        if (
            self.applied_token_generation == 0
            or (hasattr(cache, "needs_refresh") and cache.needs_refresh())
            or cache_gen != self.applied_token_generation
        ):
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
                escaped_token = str(snapshot.access_token).replace("'", "''")
                secret_sql = f"CREATE OR REPLACE SECRET vertebra_gcs_adc (TYPE GCS, BEARER_TOKEN '{escaped_token}')"
                self.last_gcs_secret_sql = secret_sql
                self.applied_token_generation = int(snapshot.generation)
                if self._using_native_duckdb:
                    with self._writer_lock, contextlib.suppress(RuntimeError, ValueError, OSError):
                        self._conn.execute(secret_sql)

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


class AsyncDuckDBDriver(ThreadOffloadAsyncDatabaseDriver[DuckDBConfig, SyncDuckDBDriver]):
    """Asynchronous DuckDB analytical driver with single-writer coordination and SHM Arrow streaming."""

    dialect: ClassVar[str] = "duckdb"
    dialect_tag: ClassVar[int] = DUCKDB_DIALECT_TAG

    def __init__(self, config: DuckDBConfig | None = None, *, broker: Any = None) -> None:
        resolved = config or DuckDBConfig()
        sync_driver = SyncDuckDBDriver(resolved, broker=broker)
        super().__init__(resolved, sync_driver, broker=sync_driver.broker)

    @property
    def executed_bootstrap_statements(self) -> tuple[str, ...]:
        """Return the post-initialization `SET` statements executed on startup."""
        return self._sync.executed_bootstrap_statements

    @property
    def applied_token_generation(self) -> int:
        """Return the token generation currently applied to this DuckDB session."""
        return self._sync.applied_token_generation

    @property
    def last_gcs_secret_sql(self) -> str:
        """Return the last `CREATE OR REPLACE SECRET` SQL statement applied for GCS ADC auth."""
        return self._sync.last_gcs_secret_sql

    async def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DDL or DML statement asynchronously."""
        await self._ensure_gcs_token_async()
        return await super().execute(sql, parameters, **kwargs)

    async def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a batch statement asynchronously."""
        await self._ensure_gcs_token_async()
        return await super().execute_many(sql, parameter_sets, **kwargs)

    async def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query returning tuple rows asynchronously."""
        await self._ensure_gcs_token_async()
        return await super().execute_tuples(sql, parameters, **kwargs)

    async def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute SQL and return `(rows, column_names)` asynchronously."""
        await self._ensure_gcs_token_async()
        return await super().execute_with_description(sql, parameters, **kwargs)

    async def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute an analytical `SELECT` query asynchronously."""
        await self._ensure_gcs_token_async()
        return await super().select(sql, parameters, schema_type=schema_type, **kwargs)

    async def select_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a `SELECT` query asynchronously and return the first row or raise `NotFoundError`."""
        await self._ensure_gcs_token_async()
        return await super().select_one(sql, parameters, schema_type=schema_type, **kwargs)

    async def select_one_or_none(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any | None:
        """Execute a `SELECT` query asynchronously and return the first row or `None`."""
        await self._ensure_gcs_token_async()
        return await super().select_one_or_none(sql, parameters, schema_type=schema_type, **kwargs)

    async def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a `SELECT` query asynchronously and return the first scalar column."""
        await self._ensure_gcs_token_async()
        return await super().select_value(sql, parameters, value_type=value_type, **kwargs)

    async def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a query asynchronously and return a zero-copy `ArrowCursor`."""
        await self._ensure_gcs_token_async()
        return await super().select_to_arrow(sql, parameters, threshold_rows=threshold_rows, **kwargs)

    async def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> AsyncIterator[Any]:
        """Stream Arrow record batches asynchronously."""
        await self._ensure_gcs_token_async()
        async for batch in super().stream_arrow_batches(sql, parameters, batch_size=batch_size, **kwargs):
            yield batch

    async def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> int:
        """Asynchronously bulk-ingest an Arrow Table or `ArrowCursor` into `table_name`."""
        await self._ensure_gcs_token_async()
        return await super().load_from_arrow(table_name, data, columns=columns, **kwargs)

    async def close(self) -> None:
        """Close the underlying DuckDB connection."""
        await asyncio.to_thread(self._sync.close)
        self._closed = True

    async def _ensure_gcs_token_async(self) -> None:
        """Pre-refresh the GCS ADC token on the Rust Tokio runtime before dispatching DuckDB work."""
        cache = self.config.gcs_token_cache
        if cache is None:
            return
        needs_refresh = bool(hasattr(cache, "needs_refresh") and cache.needs_refresh())
        cache_gen = int(getattr(cache, "generation", 0) or 0)
        if (
            self._sync.applied_token_generation == 0
            or needs_refresh
            or cache_gen != self._sync.applied_token_generation
            or self._sync._cached_gcs_snapshot is None
        ):
            get_fn = getattr(cache, "get_token", None) or getattr(cache, "get_token_async", None)
            if callable(get_fn):
                res = get_fn()
                snapshot = await res if inspect.isawaitable(res) else res
                self._sync._cached_gcs_snapshot = snapshot
