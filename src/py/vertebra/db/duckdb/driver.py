"""Synchronous and asynchronous DuckDB analytical drivers."""

from __future__ import annotations

import asyncio
import contextlib
import os
import sqlite3
import threading
from typing import TYPE_CHECKING, Any, ClassVar

from vertebra.db._arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    _hydrate_rows_with_schema,
    _table_to_rows_and_cols,
)
from vertebra.db.driver import SyncDatabaseDriver, ThreadOffloadAsyncDatabaseDriver
from vertebra.db.duckdb.config import DuckDBConfig
from vertebra.db.duckdb.core import DUCKDB_DIALECT_TAG, DuckDBCore
from vertebra.utils.module_loader import import_optional

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

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
        self.applied_token_generation: int = 0
        self.last_gcs_secret_sql: str = ""
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

    def execute(  # type: ignore[override]
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> int:
        """Execute a DDL or DML statement under single-writer coordination."""
        self._sync_gcs_token_secret()
        bound = tuple(params) if isinstance(params, (list, tuple)) else (params or ())
        grant = self._acquire_duckdb_lease(sql, len(bound) if isinstance(bound, tuple) else 0)
        try:
            with self._writer_lock:
                cur = self._conn.execute(sql, bound) if bound else self._conn.execute(sql)
                if not self._using_native_duckdb:
                    self._conn.commit()
                rowcount = getattr(cur, "rowcount", 0)
                return max(int(rowcount or 0), 0)
        finally:
            self._release_duckdb_lease(grant)

    def select(  # type: ignore[override]
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute an analytical `SELECT` query and return rows as dictionaries or `schema_type` instances."""
        self._sync_gcs_token_secret()
        bound = tuple(params) if isinstance(params, (list, tuple)) else (params or ())
        grant = self._acquire_duckdb_lease(sql, len(bound) if isinstance(bound, tuple) else 0)
        try:
            if DuckDBCore.is_write_statement(sql):
                with self._writer_lock:
                    cur = self._conn.execute(sql, bound) if bound else self._conn.execute(sql)
                    rows = cur.fetchall()
                    cols = [col[0] for col in (cur.description or ())]
            else:
                cur = self._conn.execute(sql, bound) if bound else self._conn.execute(sql)
                rows = cur.fetchall()
                cols = [col[0] for col in (cur.description or ())]
            if not rows:
                return []
            tuple_rows = [tuple(row) for row in rows]
            return _hydrate_rows_with_schema(tuple_rows, cols, schema_type)
        finally:
            self._release_duckdb_lease(grant)

    def select_one(  # type: ignore[override]
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any:
        """Execute a `SELECT` query and return the first row or raise `ValueError`."""
        rows = self.select(sql, params, schema_type=schema_type)
        if not rows:
            msg = "No row found for select_one"
            raise ValueError(msg)
        return rows[0]

    def select_one_or_none(  # type: ignore[override]
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Execute a `SELECT` query and return the first row or `None`."""
        rows = self.select(sql, params, schema_type=schema_type)
        return rows[0] if rows else None

    def load_from_arrow(  # type: ignore[override]
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
    ) -> int:
        """Bulk-ingest an Arrow Table, `ArrowCursor`, or row sequence into `table_name`."""
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
                if not self._using_native_duckdb:
                    self._conn.commit()
                return len(rows)
        finally:
            self._release_duckdb_lease(grant)

    def execute_tuples(  # type: ignore[override]
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return positional tuples."""
        self._sync_gcs_token_secret()
        bound = tuple(params) if isinstance(params, (list, tuple)) else (params or ())
        grant = self._acquire_duckdb_lease(sql, len(bound) if isinstance(bound, tuple) else 0)
        try:
            with self._writer_lock:
                cur = self._conn.execute(sql, bound) if bound else self._conn.execute(sql)
                rows = [tuple(row) for row in cur.fetchall()]
                if not self._using_native_duckdb:
                    self._conn.commit()
                return rows
        finally:
            self._release_duckdb_lease(grant)

    def execute_with_description(  # type: ignore[override]
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[Any, ...], ...] | None, int]:
        """Execute SQL and return `(rows, description, rowcount)` for DBAPI compatibility."""
        self._sync_gcs_token_secret()
        bound = tuple(params) if isinstance(params, (list, tuple)) else (params or ())
        grant = self._acquire_duckdb_lease(sql, len(bound) if isinstance(bound, tuple) else 0)
        try:
            with self._writer_lock:
                cur = self._conn.execute(sql, bound) if bound else self._conn.execute(sql)
                if cur.description is not None:
                    rows = [tuple(row) for row in cur.fetchall()]
                    desc = tuple((col[0], None, None, None, None, None, None) for col in cur.description)
                    return rows, desc, len(rows)
                if not self._using_native_duckdb:
                    self._conn.commit()
                return [], None, max(int(getattr(cur, "rowcount", 0) or 0), 0)
        finally:
            self._release_duckdb_lease(grant)

    def select_to_arrow(  # type: ignore[override]
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        shm_threshold_rows: int = 1,
    ) -> ArrowCursor:
        """Stream analytical query results into a zero-copy `ShmRegion`-backed `ArrowCursor`."""
        rows = self.select(sql, params)
        return ArrowCursor.from_rows(rows, threshold_rows=shm_threshold_rows)

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._closed = True
        self._conn.close()

    def _sync_gcs_token_secret(self) -> None:
        """Ensure this DuckDB session's GCS `BEARER_TOKEN` secret matches the latest `GcpTokenCache` generation."""
        cache = self.config.gcs_token_cache
        if cache is None:
            return
        if (
            self.applied_token_generation == 0
            or cache.needs_refresh()
            or cache.generation != self.applied_token_generation
        ):
            snapshot = cache.get_token()
            if int(snapshot.generation) != self.applied_token_generation:
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

    async def execute(  # type: ignore[override]
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> int:
        """Execute a DDL or DML statement asynchronously."""
        await self._ensure_gcs_token_async()
        return await asyncio.to_thread(self._sync.execute, sql, params)

    async def select(  # type: ignore[override]
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute an analytical `SELECT` query asynchronously."""
        await self._ensure_gcs_token_async()
        return await asyncio.to_thread(self._sync.select, sql, params, schema_type=schema_type)

    async def select_one(  # type: ignore[override]
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any:
        """Execute a `SELECT` query asynchronously and return the first row or raise `ValueError`."""
        await self._ensure_gcs_token_async()
        return await asyncio.to_thread(self._sync.select_one, sql, params, schema_type=schema_type)

    async def select_one_or_none(  # type: ignore[override]
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Execute a `SELECT` query asynchronously and return the first row or `None`."""
        await self._ensure_gcs_token_async()
        return await asyncio.to_thread(self._sync.select_one_or_none, sql, params, schema_type=schema_type)

    async def load_from_arrow(  # type: ignore[override]
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
    ) -> int:
        """Asynchronously bulk-ingest an Arrow Table or `ArrowCursor` into `table_name`."""
        await self._ensure_gcs_token_async()
        return await asyncio.to_thread(self._sync.load_from_arrow, table_name, data, columns=columns)

    async def select_to_arrow(  # type: ignore[override]
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        shm_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
    ) -> ArrowCursor:
        """Execute a query asynchronously and return a zero-copy `ArrowCursor`."""
        await self._ensure_gcs_token_async()
        return await asyncio.to_thread(
            self._sync.select_to_arrow,
            sql,
            params,
            shm_threshold_rows=shm_threshold_rows,
        )

    async def close(self) -> None:
        """Close the underlying DuckDB connection."""
        await asyncio.to_thread(self._sync.close)

    async def _ensure_gcs_token_async(self) -> None:
        """Pre-refresh the GCS ADC token on the Rust Tokio runtime before dispatching DuckDB work."""
        cache = self.config.gcs_token_cache
        if cache is not None and cache.needs_refresh() and hasattr(cache, "get_token_async"):
            await cache.get_token_async()
