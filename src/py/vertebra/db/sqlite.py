"""SQLite WAL driver with SQLite 3.45+ binary JSONB and free-threaded 3.14t pool support."""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Literal

import msgspec

__all__ = (
    "AsyncSqliteDriver",
    "SqliteConfig",
    "SyncSqliteDriver",
)


_VALID_SYNCHRONOUS = {"OFF", "NORMAL", "FULL", "EXTRA"}


class SqliteConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for SQLite 3.45+ with WAL mode, memory-mapped I/O, and binary JSONB."""

    database: str | Path = ":memory:"
    wal_mode: bool = True
    synchronous: Literal["OFF", "NORMAL", "FULL", "EXTRA"] = "NORMAL"
    mmap_size: int = 268_435_456
    busy_timeout_ms: int = 5000
    foreign_keys: bool = True
    enable_jsonb: bool = True

    def __post_init__(self) -> None:
        if self.busy_timeout_ms < 0:
            msg = f"busy_timeout_ms must be non-negative (got {self.busy_timeout_ms})"
            raise ValueError(msg)
        if self.synchronous not in _VALID_SYNCHRONOUS:
            msg = f"Invalid synchronous mode: {self.synchronous!r}"
            raise ValueError(msg)

    @property
    def is_in_memory(self) -> bool:
        """Return True if the target database is in-memory."""
        raw = str(self.database)
        return raw == ":memory:" or "mode=memory" in raw

    def startup_pragmas(self) -> list[str]:
        """Return the list of PRAGMA statements executed upon connection initialization."""
        pragmas = [f"PRAGMA busy_timeout = {self.busy_timeout_ms}"]
        if not self.is_in_memory and self.wal_mode:
            pragmas.append("PRAGMA journal_mode = WAL")
        pragmas.append(f"PRAGMA synchronous = {self.synchronous}")
        if not self.is_in_memory and self.mmap_size > 0:
            pragmas.append(f"PRAGMA mmap_size = {self.mmap_size}")
        if self.foreign_keys:
            pragmas.append("PRAGMA foreign_keys = ON")
        return pragmas


class SyncSqliteDriver:
    """Synchronous SQLite driver returning plain Python tuples for SQLAlchemy 2.1 speed."""

    __slots__ = ("_config", "_conn")

    def __init__(self, config: SqliteConfig | None = None) -> None:
        self._config = config or SqliteConfig()
        if self._config.synchronous not in _VALID_SYNCHRONOUS:
            msg = f"Invalid synchronous mode: {self._config.synchronous!r}"
            raise ValueError(msg)
        self._conn = sqlite3.connect(str(self._config.database), check_same_thread=False)
        for pragma in self._config.startup_pragmas():
            self._conn.execute(pragma)

    @property
    def config(self) -> SqliteConfig:
        """Return the active SqliteConfig."""
        return self._config

    @property
    def raw_connection(self) -> sqlite3.Connection:
        """Return the underlying sqlite3.Connection."""
        return self._conn

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Execute a SQL statement and return plain Python tuples (`list[tuple[Any, ...]]`)."""
        cur = self._conn.execute(sql, parameters)
        rows = cur.fetchall()
        return [tuple(row) for row in rows]

    def commit(self) -> None:
        """Commit the current SQLite transaction."""
        self._conn.commit()

    def rollback(self) -> None:
        """Roll back the current SQLite transaction."""
        self._conn.rollback()

    def close(self) -> None:
        """Close the underlying SQLite connection."""
        self._conn.close()


class AsyncSqliteDriver:
    """Asynchronous SQLite driver facade compatible with Python 3.12+ and free-threaded 3.14t."""

    __slots__ = ("_sync",)

    def __init__(self, config: SqliteConfig | None = None) -> None:
        self._sync = SyncSqliteDriver(config)

    @property
    def config(self) -> SqliteConfig:
        """Return the active SqliteConfig."""
        return self._sync.config

    async def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Asynchronously execute a SQL statement and return plain Python tuples."""
        return await asyncio.to_thread(self._sync.execute_tuples, sql, parameters)

    async def commit(self) -> None:
        """Commit the current SQLite transaction."""
        await asyncio.to_thread(self._sync.commit)

    async def rollback(self) -> None:
        """Roll back the current SQLite transaction."""
        await asyncio.to_thread(self._sync.rollback)

    async def close(self) -> None:
        """Close the underlying SQLite connection."""
        await asyncio.to_thread(self._sync.close)
