"""Embedded DuckDB OLAP driver with SHM single-writer/multi-reader coordination and Arrow IPC streaming."""

from __future__ import annotations

import asyncio
import contextlib
import os
import sqlite3
import threading
from typing import TYPE_CHECKING, Any

from vertebra.db.postgres import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    read_arrow_shm,
    write_arrow_shm,
)
from vertebra.typing import FrozenBaseStruct
from vertebra.utils.module_loader import import_optional

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

__all__ = (
    "AsyncDuckDBDriver",
    "DUCKDB_DIALECT_TAG",
    "DuckDBConfig",
    "SyncDuckDBDriver",
    "duckdb_bootstrap_statements",
    "read_arrow_shm",
    "write_arrow_shm",
)

DUCKDB_DIALECT_TAG = 6


class DuckDBConfig(FrozenBaseStruct, frozen=True, gc=False):
    """Configuration for the embedded DuckDB analytical engine."""

    path: str = ":memory:"
    """Filesystem path to the DuckDB database file or `":memory:"`."""
    database: str = ":memory:"
    """Fallback database path alias used when `path` is left at `":memory:"`."""
    memory_limit_mb: int = 512
    """Maximum buffer manager memory allowance in MiB (`SET memory_limit`)."""
    threads: int = 4
    """Number of DuckDB execution threads (`SET threads`)."""
    parquet_metadata_cache: bool = True
    """When True, enable `SET parquet_metadata_cache = true` after connection bootstrap."""
    httpfs_connection_caching: bool = True
    """When True, keep remote HTTP/S3 object storage connections warm (`SET http_keep_alive = true`)."""
    enable_http_metadata_cache: bool = True
    """When True, cache remote HTTP/Parquet `HEAD` metadata (`SET enable_http_metadata_cache = true`)."""
    read_only: bool = False
    """When True, open the DuckDB file in read-only mode allowing multi-process readers."""
    pool_min_size: int = 1
    """Minimum number of pooled DuckDB cursors per worker."""
    pool_max_size: int = 10
    """Maximum number of pooled DuckDB cursors per worker."""
    statement_cache_size: int = 256
    """Maximum number of prepared analytical statements cached per worker."""
    broker: Any = None
    """Optional shared-memory `DatabaseBroker` coordinating single-writer/multi-reader leases."""

    def resolved_path(self) -> str:
        """Resolve the effective database path from `path` or `database`."""
        if self.path != ":memory:":
            return self.path
        return self.database

    def bootstrap_statements(self) -> list[str]:
        """Return post-initialization `SET` statements (`parquet_metadata_cache` is always post-init SQL)."""
        native = import_optional("vertebra._vertebra")
        if native is not None and hasattr(native, "duckdb_bootstrap_statements"):
            raw_stmts = native.duckdb_bootstrap_statements(
                self.resolved_path(),
                self.memory_limit_mb,
                self.threads,
                self.parquet_metadata_cache,
                self.httpfs_connection_caching,
                self.enable_http_metadata_cache,
            )
            return [s.replace("SET httpfs_connection_caching = ", "SET http_keep_alive = ") for s in raw_stmts]
        parquet_flag = "true" if self.parquet_metadata_cache else "false"
        keep_alive_flag = "true" if self.httpfs_connection_caching else "false"
        http_meta_flag = "true" if self.enable_http_metadata_cache else "false"
        return [
            f"SET memory_limit = '{self.memory_limit_mb}MB'",
            f"SET threads = {self.threads}",
            f"SET parquet_metadata_cache = {parquet_flag}",
            f"SET http_keep_alive = {keep_alive_flag}",
            f"SET enable_http_metadata_cache = {http_meta_flag}",
        ]


def duckdb_bootstrap_statements(config: DuckDBConfig | None = None) -> list[str]:
    """Generate post-init DuckDB `SET` statements for the given `DuckDBConfig`."""
    cfg = config or DuckDBConfig()
    return cfg.bootstrap_statements()


def _is_write_statement(sql: str) -> bool:
    """Return True if the SQL statement mutates state and requires the single-writer lock."""
    head = sql.lstrip().split(None, 1)
    if not head:
        return False
    verb = head[0].upper()
    return verb in {"INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "COPY", "MERGE", "TRUNCATE", "ATTACH"}


class SyncDuckDBDriver:
    """Synchronous DuckDB analytical driver with single-writer coordination and SHM Arrow streaming."""

    def __init__(self, config: DuckDBConfig | None = None, *, broker: Any = None) -> None:
        self.config = config or DuckDBConfig()
        self._broker = broker if broker is not None else self.config.broker
        self._writer_lock = threading.RLock()
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

    def _acquire_broker_lease(self, sql: str, param_count: int) -> Any:
        if self._broker is None:
            return None
        if hasattr(self._broker, "statement_cache"):
            cached = self._broker.statement_cache.lookup(DUCKDB_DIALECT_TAG, sql)
            if cached is None:
                self._broker.statement_cache.insert(
                    DUCKDB_DIALECT_TAG,
                    sql,
                    param_count,
                    sql.encode("utf-8"),
                    b"",
                )
        if hasattr(self._broker, "lease_table"):
            return self._broker.lease_table.acquire(os.getpid())
        return None

    def _release_broker_lease(self, grant: Any) -> None:
        if grant is not None and self._broker is not None and hasattr(self._broker, "lease_table"):
            self._broker.lease_table.release(grant.slot_index, grant.generation)

    def execute(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> int:
        """Execute a DDL or DML statement under single-writer coordination."""
        bound = tuple(params) if isinstance(params, (list, tuple)) else (params or ())
        grant = self._acquire_broker_lease(sql, len(bound) if isinstance(bound, tuple) else 0)
        try:
            with self._writer_lock:
                cur = self._conn.execute(sql, bound) if bound else self._conn.execute(sql)
                if not self._using_native_duckdb:
                    self._conn.commit()
                rowcount = getattr(cur, "rowcount", 0)
                return max(int(rowcount or 0), 0)
        finally:
            self._release_broker_lease(grant)

    def select(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute an analytical `SELECT` query and return rows as dictionaries."""
        bound = tuple(params) if isinstance(params, (list, tuple)) else (params or ())
        grant = self._acquire_broker_lease(sql, len(bound) if isinstance(bound, tuple) else 0)
        try:
            if _is_write_statement(sql):
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
            return [dict(zip(cols, tuple(row), strict=False)) for row in rows]
        finally:
            self._release_broker_lease(grant)

    def execute_tuples(
        self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return positional tuples."""
        bound = tuple(params) if isinstance(params, (list, tuple)) else (params or ())
        grant = self._acquire_broker_lease(sql, len(bound) if isinstance(bound, tuple) else 0)
        try:
            with self._writer_lock:
                cur = self._conn.execute(sql, bound) if bound else self._conn.execute(sql)
                rows = [tuple(row) for row in cur.fetchall()]
                if not self._using_native_duckdb:
                    self._conn.commit()
                return rows
        finally:
            self._release_broker_lease(grant)

    def execute_with_description(
        self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[Any, ...], ...] | None, int]:
        """Execute SQL and return `(rows, description, rowcount)` for DBAPI compatibility."""
        bound = tuple(params) if isinstance(params, (list, tuple)) else (params or ())
        grant = self._acquire_broker_lease(sql, len(bound) if isinstance(bound, tuple) else 0)
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
            self._release_broker_lease(grant)

    def select_to_arrow(
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
        self._conn.close()


class AsyncDuckDBDriver:
    """Asynchronous DuckDB analytical driver with single-writer coordination and SHM Arrow streaming."""

    def __init__(self, config: DuckDBConfig | None = None, *, broker: Any = None) -> None:
        self.config = config or DuckDBConfig()
        self._sync = SyncDuckDBDriver(self.config, broker=broker)

    @property
    def executed_bootstrap_statements(self) -> tuple[str, ...]:
        """Return the post-initialization `SET` statements executed on startup."""
        return self._sync.executed_bootstrap_statements

    async def execute(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> int:
        """Execute a DDL or DML statement asynchronously."""
        return await asyncio.to_thread(self._sync.execute, sql, params)

    async def select(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute an analytical `SELECT` query asynchronously."""
        return await asyncio.to_thread(self._sync.select, sql, params)

    async def select_to_arrow(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        shm_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
    ) -> ArrowCursor:
        """Execute a query asynchronously and return a zero-copy `ArrowCursor`."""
        return await asyncio.to_thread(
            self._sync.select_to_arrow,
            sql,
            params,
            shm_threshold_rows=shm_threshold_rows,
        )

    async def close(self) -> None:
        """Close the underlying DuckDB connection."""
        await asyncio.to_thread(self._sync.close)
