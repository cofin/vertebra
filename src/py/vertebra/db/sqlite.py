"""SQLite WAL driver with SQLite 3.45+ binary JSONB, SHM broker leasing, and Arrow IPC support."""

from __future__ import annotations

import asyncio
import contextlib
import os
import sqlite3
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import msgspec

from vertebra.db.postgres import (
    ArrowCursor,
    _normalize_driver_params,
    write_arrow_shm,
)
from vertebra.typing import FrozenBaseStruct
from vertebra.utils.module_loader import import_optional

__all__ = (
    "AsyncSQLiteDriver",
    "SQLiteConfig",
    "SyncSQLiteDriver",
)

_VALID_SYNCHRONOUS = {"OFF", "NORMAL", "FULL", "EXTRA"}


class SQLiteConfig(FrozenBaseStruct, frozen=True, gc=False):
    """Configuration for SQLite 3.45+ with WAL mode, memory-mapped I/O, and SHM broker leasing."""

    path: str = ":memory:"
    """Filesystem path to the SQLite database file or `":memory:"`."""
    wal_mode: bool = True
    """When True, enable `PRAGMA journal_mode = WAL` for concurrent readers alongside a single writer."""
    synchronous: str = "NORMAL"
    """SQLite `PRAGMA synchronous` durability level (`"OFF"`, `"NORMAL"`, `"FULL"`, or `"EXTRA"`)."""
    mmap_size: int = 268_435_456
    """Maximum memory-mapped I/O window in bytes (`PRAGMA mmap_size`, default 256 MiB)."""
    busy_timeout_ms: int = 5000
    """Lock contention wait timeout in milliseconds (`PRAGMA busy_timeout`)."""
    max_readers: int = 8
    """Maximum number of concurrent read connections leased across workers."""
    foreign_keys: bool = True
    """When True, enforce referential integrity via `PRAGMA foreign_keys = ON`."""
    broker: Any = None
    """Optional shared-memory `DatabaseBroker` coordinating single-writer/multi-reader leases."""
    database: str | Path | None = None
    """Optional alias for `path` accepting a `pathlib.Path` or string."""
    enable_jsonb: bool = True
    """When True, use SQLite 3.45+ binary `JSONB` functions when available."""

    def __post_init__(self) -> None:
        if self.database is not None and self.path == ":memory:":
            msgspec.structs.force_setattr(self, "path", str(self.database))
        if self.database is None:
            msgspec.structs.force_setattr(self, "database", self.path)
        if self.busy_timeout_ms < 0:
            msg = f"busy_timeout_ms must be non-negative (got {self.busy_timeout_ms})"
            raise ValueError(msg)
        sync_upper = self.synchronous.upper()
        if sync_upper not in _VALID_SYNCHRONOUS:
            msg = f"Invalid synchronous mode: {self.synchronous!r}"
            raise ValueError(msg)
        msgspec.structs.force_setattr(self, "synchronous", sync_upper)

    @property
    def is_in_memory(self) -> bool:
        """Return True if the target database is in-memory."""
        raw = str(self.path)
        return raw == ":memory:" or "mode=memory" in raw

    def initialization_pragmas(self) -> list[str]:
        """Return the list of PRAGMA statements executed upon connection initialization."""
        native_mod = import_optional("vertebra._vertebra")
        if native_mod is not None:
            native_fn = getattr(native_mod, "sqlite_initialization_pragmas", None)
            if callable(native_fn):
                with contextlib.suppress(TypeError, ValueError, RuntimeError):
                    res = native_fn(
                        self.wal_mode,
                        self.synchronous,
                        self.mmap_size,
                        self.busy_timeout_ms,
                        self.foreign_keys,
                    )
                    if isinstance(res, list) and all(isinstance(p, str) for p in res):
                        return list(res)

        pragmas = [f"PRAGMA busy_timeout = {self.busy_timeout_ms}"]
        if self.wal_mode:
            pragmas.append("PRAGMA journal_mode = WAL")
        pragmas.append(f"PRAGMA synchronous = {self.synchronous}")
        if self.mmap_size > 0:
            pragmas.append(f"PRAGMA mmap_size = {self.mmap_size}")
        pragmas.append(f"PRAGMA foreign_keys = {'ON' if self.foreign_keys else 'OFF'}")
        return pragmas

    def startup_pragmas(self) -> list[str]:
        """Return startup PRAGMA statements for connection initialization."""
        return self.initialization_pragmas()


class SyncSQLiteDriver:
    """Synchronous SQLite driver returning plain Python tuples with SHM broker and Arrow IPC support."""

    __slots__ = ("_broker", "_closed", "_config", "_conn")

    def __init__(self, config: SQLiteConfig | None = None, *, broker: Any = None) -> None:
        self._config = config or SQLiteConfig()
        if self._config.synchronous not in _VALID_SYNCHRONOUS:
            msg = f"Invalid synchronous mode: {self._config.synchronous!r}"
            raise ValueError(msg)
        self._broker = broker if broker is not None else self._config.broker
        self._closed = False
        self._conn = sqlite3.connect(str(self._config.path), check_same_thread=False)
        for pragma in self._config.initialization_pragmas():
            self._conn.execute(pragma)

    @property
    def config(self) -> SQLiteConfig:
        """Return the active SQLiteConfig."""
        return self._config

    @property
    def broker(self) -> Any:
        """Return the attached SHM connection broker transport if present."""
        return self._broker

    @property
    def closed(self) -> bool:
        """Return True when the SQLite connection has been closed."""
        return self._closed

    @property
    def raw_connection(self) -> sqlite3.Connection:
        """Return the underlying sqlite3.Connection."""
        return self._conn

    def _acquire_broker_lease(self, sql: str) -> Any:
        """Acquire a lease and statement cache plan from the attached broker if present."""
        if self._broker is None:
            return None
        if hasattr(self._broker, "get_or_compile_statement"):
            self._broker.get_or_compile_statement("sqlite", sql, lambda s: (s, ()))
        elif hasattr(self._broker, "statement_cache") and self._broker.statement_cache is not None:
            sc = self._broker.statement_cache
            if hasattr(sc, "lookup") and hasattr(sc, "insert") and sc.lookup(3, sql) is None:
                sc.insert(3, sql, 0, sql.encode("utf-8"), b"")
        if hasattr(self._broker, "acquire"):
            return self._broker.acquire(bind_key="sqlite")
        if hasattr(self._broker, "lease_table") and self._broker.lease_table is not None:
            return self._broker.lease_table.acquire(os.getpid())
        return None

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[str, Any, None, None, None, None, None], ...] | None]:
        """Execute a SQL statement and return plain tuples and column descriptions."""
        lease = self._acquire_broker_lease(sql)
        try:
            cur = self._conn.execute(sql, parameters)
            rows = [tuple(row) for row in cur.fetchall()]
            desc = (
                tuple((d[0], d[1], None, None, None, None, None) for d in cur.description) if cur.description else None
            )
            return rows, desc
        finally:
            if lease is not None:
                if hasattr(lease, "release"):
                    lease.release()
                elif (
                    self._broker is not None
                    and hasattr(self._broker, "lease_table")
                    and self._broker.lease_table is not None
                ):
                    self._broker.lease_table.release(lease.slot_index, lease.generation)

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Execute a SQL statement and return plain Python tuples (`list[tuple[Any, ...]]`)."""
        rows, _ = self.execute_with_description(sql, parameters)
        return rows

    def execute(
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Execute a SQL statement with variadic positional or explicit parameters."""
        resolved = _normalize_driver_params(params, parameters)
        return self.execute_tuples(sql, resolved)

    def select(
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[dict[str, Any]]:
        """Execute a SELECT statement and return rows as dictionaries."""
        resolved = _normalize_driver_params(params, parameters)
        rows, desc = self.execute_with_description(sql, resolved)
        if not rows:
            return []
        cols = [d[0] for d in desc] if desc else [f"col_{i}" for i in range(len(rows[0]))]
        return [{col: (r[i] if i < len(r) else None) for i, col in enumerate(cols)} for r in rows]

    def select_to_arrow(
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
        shm_threshold_rows: int = 0,
    ) -> ArrowCursor:
        """Execute a query and return an ArrowCursor via zero-copy ShmRegion Arrow IPC."""
        resolved = _normalize_driver_params(params, parameters)
        rows, desc = self.execute_with_description(sql, resolved)
        cols = [d[0] for d in desc] if desc else [f"col_{i}" for i in range(len(rows[0]) if rows else 0)]
        return ArrowCursor.from_rows(rows, cols, threshold_rows=shm_threshold_rows)

    def write_arrow_shm(self, table: Any) -> memoryview:
        """Write an Arrow Table into a shared-memory Arrow IPC buffer and return a memoryview."""
        return write_arrow_shm(table)

    def commit(self) -> None:
        """Commit the current SQLite transaction."""
        self._conn.commit()

    def rollback(self) -> None:
        """Roll back the current SQLite transaction."""
        self._conn.rollback()

    def close(self) -> None:
        """Close the underlying SQLite connection."""
        self._closed = True
        self._conn.close()


class AsyncSQLiteDriver:
    """Asynchronous SQLite driver facade compatible with Python 3.12+ and free-threaded 3.14t."""

    __slots__ = ("_sync",)

    def __init__(self, config: SQLiteConfig | None = None, *, broker: Any = None) -> None:
        self._sync = SyncSQLiteDriver(config, broker=broker)

    @property
    def config(self) -> SQLiteConfig:
        """Return the active SQLiteConfig."""
        return self._sync.config

    @property
    def broker(self) -> Any:
        """Return the attached SHM connection broker transport if present."""
        return self._sync.broker

    @property
    def closed(self) -> bool:
        """Return True when the SQLite connection has been closed."""
        return self._sync.closed

    async def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[str, Any, None, None, None, None, None], ...] | None]:
        """Asynchronously execute a SQL statement returning rows and column descriptions."""
        return await asyncio.to_thread(self._sync.execute_with_description, sql, parameters)

    async def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Asynchronously execute a SQL statement and return plain Python tuples."""
        return await asyncio.to_thread(self._sync.execute_tuples, sql, parameters)

    async def execute(
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Asynchronously execute a SQL statement."""
        resolved = _normalize_driver_params(params, parameters)
        return await asyncio.to_thread(self._sync.execute_tuples, sql, resolved)

    async def select(
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[dict[str, Any]]:
        """Asynchronously execute a SELECT statement and return rows as dictionaries."""
        resolved = _normalize_driver_params(params, parameters)
        return await asyncio.to_thread(self._sync.select, sql, parameters=resolved)

    async def select_to_arrow(
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
        shm_threshold_rows: int = 0,
    ) -> ArrowCursor:
        """Asynchronously execute a query and return an ArrowCursor via zero-copy ShmRegion Arrow IPC."""
        resolved = _normalize_driver_params(params, parameters)
        return await asyncio.to_thread(
            self._sync.select_to_arrow,
            sql,
            parameters=resolved,
            shm_threshold_rows=shm_threshold_rows,
        )

    def write_arrow_shm(self, table: Any) -> memoryview:
        """Write an Arrow Table into a shared-memory Arrow IPC buffer and return a memoryview."""
        return self._sync.write_arrow_shm(table)

    async def commit(self) -> None:
        """Commit the current SQLite transaction."""
        await asyncio.to_thread(self._sync.commit)

    async def rollback(self) -> None:
        """Roll back the current SQLite transaction."""
        await asyncio.to_thread(self._sync.rollback)

    async def close(self) -> None:
        """Close the underlying SQLite connection."""
        await asyncio.to_thread(self._sync.close)
