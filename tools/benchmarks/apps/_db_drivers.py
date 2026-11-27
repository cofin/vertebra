"""Pluggable SQLite and PostgreSQL ``WorldDriver`` implementations for benchmark apps.

Provides ``native`` (Vertebra Rust drivers) and ``stdlib`` (``sqlite3``/``aiosqlite``,
``psycopg``/``asyncpg``) database backends, shared broker transport resolution,
idempotent TechEmpower ``world`` table seeding, and lazy routing to ORM adapters.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import sqlite3
import threading
import weakref
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Any, Final, Protocol
from urllib.parse import urlsplit

from anyio import CancelScope

from tools.benchmarks.apps._data_backend import (
    WORLD_ROW_COUNT,
    DataBackendConfig,
    WorldRow,
)
from vertebra.db.adapters.postgres import (
    AsyncPostgreSQLDriver,
    PostgreSQLConfig,
    SyncPostgreSQLDriver,
)
from vertebra.db.adapters.sqlite import (
    AsyncSQLiteDriver,
    SQLiteConfig,
    SyncSQLiteDriver,
)
from vertebra.db.registry import DatabaseRegistry
from vertebra.extensions.broker import (
    AsyncSHMBrokerTransport,
    BrokerConfig,
    SyncSHMBrokerTransport,
)
from vertebra.utils.module_loader import import_optional

__all__ = (
    "POSTGRES_BENCH_INSTALL_HINT",
    "WorldDriver",
    "build_world_driver",
    "ensure_task_context",
    "is_postgres_dsn",
    "normalize_sqlite_target",
    "record_to_world_row",
    "require_postgres_dsn",
    "resolve_async_broker_transport",
    "resolve_benchmark_broker",
    "resolve_broker_transports",
    "resolve_sync_broker_transport",
    "seed_postgres_world_async",
    "seed_postgres_world_sync",
    "seed_sqlite_world_sync",
    "world_seed_rows",
)

POSTGRES_BENCH_INSTALL_HINT: Final[str] = "uv sync --group postgres-bench"
_POSTGRES_SCHEMES: Final[tuple[str, ...]] = ("postgres://", "postgresql://", "postgresql+")
_PG_SEED_ADVISORY_LOCK_KEY: Final[int] = 745_018_293
_PG_ADVISORY_LOCK_SQL: Final[str] = f"SELECT pg_advisory_xact_lock({_PG_SEED_ADVISORY_LOCK_KEY})"

_SQLITE_CREATE_WORLD_SQL: Final[str] = (
    "CREATE TABLE IF NOT EXISTS world (id INTEGER PRIMARY KEY, randomnumber INTEGER NOT NULL)"
)
_SQLITE_COUNT_WORLD_SQL: Final[str] = "SELECT COUNT(*) FROM world"
_SQLITE_INSERT_WORLD_SQL: Final[str] = "INSERT OR IGNORE INTO world (id, randomnumber) VALUES (?, ?)"
_SQLITE_SELECT_WORLD_SQL: Final[str] = "SELECT id, randomnumber FROM world WHERE id = ?"
_SQLITE_UPDATE_WORLD_SQL: Final[str] = "UPDATE world SET randomnumber = ? WHERE id = ?"

_PG_CREATE_WORLD_SQL: Final[str] = (
    "CREATE TABLE IF NOT EXISTS world (id INTEGER PRIMARY KEY, randomnumber INTEGER NOT NULL)"
)
_PG_SEED_WORLD_SQL: Final[str] = (
    "INSERT INTO world (id, randomnumber) "
    "SELECT g, ((g::BIGINT * 1103515245 + 12345) % 10000)::INT + 1 "
    "FROM generate_series(1, 10000) AS g "
    "ON CONFLICT (id) DO NOTHING"
)
_PG_SELECT_WORLD_DOLLAR_SQL: Final[str] = "SELECT id, randomnumber FROM world WHERE id = $1"
_PG_UPDATE_WORLD_DOLLAR_SQL: Final[str] = "UPDATE world SET randomnumber = $1 WHERE id = $2"
_PG_SELECT_WORLD_PERCENT_SQL: Final[str] = "SELECT id, randomnumber FROM world WHERE id = %s"
_PG_UPDATE_WORLD_PERCENT_SQL: Final[str] = "UPDATE world SET randomnumber = %s WHERE id = %s"


class WorldDriver(Protocol):
    """Protocol implemented by pluggable benchmark database drivers."""

    def fetch_world_sync(self, row_id: int) -> WorldRow:
        """Synchronously read a single World row by primary key."""
        ...

    def fetch_worlds_sync(self, row_ids: list[int]) -> list[WorldRow]:
        """Synchronously read multiple World rows in requested order."""
        ...

    def update_worlds_sync(self, updates: list[tuple[int, int]]) -> list[WorldRow]:
        """Synchronously read and update multiple World rows."""
        ...

    async def fetch_world_async(self, row_id: int) -> WorldRow:
        """Asynchronously read a single World row by primary key."""
        ...

    async def fetch_worlds_async(self, row_ids: list[int]) -> list[WorldRow]:
        """Asynchronously read multiple World rows in requested order."""
        ...

    async def update_worlds_async(self, updates: list[tuple[int, int]]) -> list[WorldRow]:
        """Asynchronously read and update multiple World rows."""
        ...

    def close(self) -> None:
        """Release synchronous resources and perform best-effort async cleanup."""
        ...

    async def aclose(self) -> None:
        """Asynchronously release pooled connections and synchronous handles."""
        ...


async def ensure_task_context() -> None:
    """Yield once when stepped synchronously outside an ``asyncio.Task`` (e.g. ``PyIter_Send``).

    Required before awaiting libraries such as SQLAlchemy ``AsyncAdaptedQueuePool`` or
    ``asyncpg`` that invoke ``asyncio.timeout()`` internally.
    """
    if asyncio.current_task() is None:
        await asyncio.sleep(0)


def is_postgres_dsn(dsn: str) -> bool:
    """Return True when ``dsn`` uses a PostgreSQL URI scheme."""
    lowered = dsn.strip().lower()
    return lowered.startswith(_POSTGRES_SCHEMES)


def require_postgres_dsn(dsn: str) -> str:
    """Validate and return a non-empty PostgreSQL DSN or raise actionable ``ValueError``."""
    cleaned = dsn.strip()
    if not cleaned:
        msg = (
            "PostgreSQL benchmark backend requires a non-empty VTB_BENCH_DB_DSN "
            "(launch local postgres:17-alpine via tools.benchmarks.fixtures.FixtureLauncher "
            "or export VTB_BENCH_DB_DSN=postgresql://postgres:bench@127.0.0.1:<port>/bench)"
        )
        raise ValueError(msg)
    return cleaned


def normalize_sqlite_target(raw_dsn: str) -> str:
    """Normalize a SQLite path or ``sqlite://`` URI while stripping accidental non-URI query params."""
    dsn = raw_dsn.strip()
    if not dsn or dsn == ":memory:":
        return ":memory:"
    if dsn.startswith("file:"):
        return dsn
    if "://" in dsn:
        parsed = urlsplit(dsn)
        raw_path = parsed.path
        if parsed.netloc == ":memory:" or (not parsed.netloc and raw_path in ("", "/", "/:memory:", ":memory:")):
            return ":memory:"
        if raw_path.startswith("//"):
            return raw_path[1:]
        if parsed.netloc:
            return f"{parsed.netloc}{raw_path}"
        return raw_path
    if "?" in dsn:
        return dsn.split("?", 1)[0]
    return dsn


def world_seed_rows() -> list[tuple[int, int]]:
    """Return the deterministic 10,000-row TechEmpower ``world`` seed dataset."""
    return [(idx, ((idx * 1_103_515_245 + 12_345) % WORLD_ROW_COUNT) + 1) for idx in range(1, WORLD_ROW_COUNT + 1)]


def record_to_world_row(record: Any, fallback_id: int) -> WorldRow:
    """Convert a driver row mapping, sequence, or ORM model to ``WorldRow``."""
    if record is None:
        return WorldRow(id=fallback_id, randomNumber=1)
    if isinstance(record, WorldRow):
        return record
    if isinstance(record, Mapping):
        raw_id = record.get("id", fallback_id)
        raw_num = record.get("randomnumber", record.get("randomNumber", 1))
        return WorldRow(id=int(raw_id), randomNumber=int(raw_num))
    if isinstance(record, Sequence) and not isinstance(record, (str, bytes, bytearray)):
        if len(record) >= 2:
            return WorldRow(id=int(record[0]), randomNumber=int(record[1]))
        return WorldRow(id=fallback_id, randomNumber=int(record[0]))
    return WorldRow(
        id=int(getattr(record, "id", fallback_id)),
        randomNumber=int(getattr(record, "randomnumber", getattr(record, "randomNumber", 1))),
    )


def _worker_broker_config(shm_name: str, *, pool_size: int) -> BrokerConfig:
    """Build a worker-role ``BrokerConfig`` aligned with ``BrokerHostHook`` env vars."""
    normalized_name = shm_name if shm_name.startswith("/") else f"/{shm_name}"
    raw_overflow = os.environ.get("VERTEBRA_BROKER_MAX_OVERFLOW", "0").strip()
    try:
        max_overflow = max(0, int(raw_overflow))
    except ValueError:
        max_overflow = 0
    return BrokerConfig(
        shm_name=normalized_name,
        role="worker",
        pool_size=max(1, pool_size),
        max_overflow=max_overflow,
        socket_path=os.environ.get("VERTEBRA_BROKER_SOCKET") or None,
        attach_secret=os.environ.get("VERTEBRA_BROKER_SECRET") or None,
    )


def resolve_sync_broker_transport(
    broker_mode: str,
    *,
    pool_size: int = 40,
) -> SyncSHMBrokerTransport | None:
    """Resolve a single sync SHM broker transport when ``broker_mode == 'shm_broker'``."""
    if broker_mode.strip().lower() != "shm_broker":
        return None
    shm_name = os.environ.get("VERTEBRA_BROKER_SHM", "").strip()
    shm_fd = os.environ.get("VERTEBRA_BROKER_SHM_FD", "").strip()
    if shm_name and not shm_fd:
        return SyncSHMBrokerTransport(_worker_broker_config(shm_name, pool_size=pool_size))
    native_broker = DatabaseRegistry.default_broker(pool_size=max(1, pool_size))
    return SyncSHMBrokerTransport(native=native_broker)


def resolve_async_broker_transport(
    broker_mode: str,
    *,
    pool_size: int = 40,
) -> AsyncSHMBrokerTransport | None:
    """Resolve a single async SHM broker transport when ``broker_mode == 'shm_broker'``."""
    if broker_mode.strip().lower() != "shm_broker":
        return None
    shm_name = os.environ.get("VERTEBRA_BROKER_SHM", "").strip()
    shm_fd = os.environ.get("VERTEBRA_BROKER_SHM_FD", "").strip()
    if shm_name and not shm_fd:
        return AsyncSHMBrokerTransport(_worker_broker_config(shm_name, pool_size=pool_size))
    native_broker = DatabaseRegistry.default_broker(pool_size=max(1, pool_size))
    return AsyncSHMBrokerTransport(native=native_broker)


def resolve_benchmark_broker(broker_mode: str, *, pool_size: int = 40) -> Any:
    """Resolve a sync-capable SHM broker handle when ``broker_mode == 'shm_broker'``."""
    if broker_mode.strip().lower() != "shm_broker":
        return None
    shm_name = os.environ.get("VERTEBRA_BROKER_SHM", "").strip()
    shm_fd = os.environ.get("VERTEBRA_BROKER_SHM_FD", "").strip()
    if shm_name and not shm_fd:
        return SyncSHMBrokerTransport(_worker_broker_config(shm_name, pool_size=pool_size))
    return DatabaseRegistry.default_broker(pool_size=max(1, pool_size))


def resolve_broker_transports(
    broker_mode: str,
    *,
    pool_size: int = 40,
) -> tuple[SyncSHMBrokerTransport | None, AsyncSHMBrokerTransport | None]:
    """Resolve paired sync and async SHM broker transports for SQLAlchemy ``SyncBrokerPool``."""
    if broker_mode.strip().lower() != "shm_broker":
        return None, None
    shm_name = os.environ.get("VERTEBRA_BROKER_SHM", "").strip()
    shm_fd = os.environ.get("VERTEBRA_BROKER_SHM_FD", "").strip()
    if shm_name and not shm_fd:
        broker_cfg = _worker_broker_config(shm_name, pool_size=pool_size)
        return SyncSHMBrokerTransport(broker_cfg), AsyncSHMBrokerTransport(broker_cfg)
    native_broker = DatabaseRegistry.default_broker(pool_size=max(1, pool_size))
    return (
        SyncSHMBrokerTransport(native=native_broker),
        AsyncSHMBrokerTransport(native=native_broker),
    )


def seed_sqlite_world_sync(db_target: str) -> None:
    """Idempotently configure WAL pragmas and seed 10,000 ``world`` rows in SQLite."""
    is_uri = db_target.startswith("file:")
    conn = sqlite3.connect(db_target, uri=is_uri, timeout=5.0)
    try:
        if db_target != ":memory:" and "mode=memory" not in db_target:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA busy_timeout = 5000")
        conn.execute(_SQLITE_CREATE_WORLD_SQL)
        existing = int(conn.execute(_SQLITE_COUNT_WORLD_SQL).fetchone()[0])
        if existing < WORLD_ROW_COUNT:
            conn.executemany(_SQLITE_INSERT_WORLD_SQL, world_seed_rows())
            conn.commit()
    finally:
        conn.close()


def _seed_sqlite_connection_inline(conn: sqlite3.Connection, *, is_memory: bool) -> None:
    """Idempotently configure pragmas and seed 10,000 ``world`` rows on an open ``sqlite3.Connection``."""
    if not is_memory:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute(_SQLITE_CREATE_WORLD_SQL)
    existing = int(conn.execute(_SQLITE_COUNT_WORLD_SQL).fetchone()[0])
    if existing < WORLD_ROW_COUNT:
        conn.executemany(_SQLITE_INSERT_WORLD_SQL, world_seed_rows())
        conn.commit()


def _seed_pg_native_sync(driver: SyncPostgreSQLDriver) -> None:
    """Create and seed the PostgreSQL ``world`` table under a transaction-scoped advisory lock."""
    driver.begin()
    try:
        driver.execute(_PG_ADVISORY_LOCK_SQL)
        driver.execute(_PG_CREATE_WORLD_SQL)
        driver.execute(_PG_SEED_WORLD_SQL)
        driver.commit()
    except BaseException:
        with contextlib.suppress(Exception):
            driver.rollback()
        raise


async def _seed_pg_native_async(driver: AsyncPostgreSQLDriver) -> None:
    """Asynchronously create and seed the PostgreSQL ``world`` table under an advisory lock."""
    await driver.begin()
    try:
        await driver.execute(_PG_ADVISORY_LOCK_SQL)
        await driver.execute(_PG_CREATE_WORLD_SQL)
        await driver.execute(_PG_SEED_WORLD_SQL)
        await driver.commit()
    except BaseException:
        with contextlib.suppress(Exception):
            await driver.rollback()
        raise


def seed_postgres_world_sync(dsn: str) -> None:
    """Idempotently create and seed the 10,000-row PostgreSQL ``world`` table via ``SyncPostgreSQLDriver``."""
    cfg = PostgreSQLConfig.from_url(dsn, min_size=1, max_size=2)
    with SyncPostgreSQLDriver(cfg) as drv:
        _seed_pg_native_sync(drv)


async def seed_postgres_world_async(dsn: str) -> None:
    """Idempotently create and seed the 10,000-row PostgreSQL ``world`` table via ``AsyncPostgreSQLDriver``."""
    cfg = PostgreSQLConfig.from_url(dsn, min_size=1, max_size=2)
    async with AsyncPostgreSQLDriver(cfg) as drv:
        await _seed_pg_native_async(drv)


class NativeSQLiteWorldDriver:
    """Vertebra native Rust SQLite ``WorldDriver`` using ``SyncSQLiteDriver`` and lazy ``AsyncSQLiteDriver``."""

    def __init__(self, config: DataBackendConfig, *, pool_size: int) -> None:
        self.db_target = normalize_sqlite_target(config.db_dsn)
        self.is_memory = self.db_target == ":memory:" or "mode=memory" in self.db_target
        self.sqlite_config = SQLiteConfig(
            path=self.db_target,
            wal_mode=not self.is_memory,
            synchronous="NORMAL",
            busy_timeout_ms=5000,
            max_readers=max(1, pool_size),
        )
        self._sync_lock = threading.Lock()
        self._sync_driver = SyncSQLiteDriver(self.sqlite_config)
        self._seed_native_sync(self._sync_driver)
        self._async_drivers: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, AsyncSQLiteDriver] = (
            weakref.WeakKeyDictionary()
        )
        self._async_locks: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = (
            weakref.WeakKeyDictionary()
        )
        self._loop_lock = threading.Lock()

    def _seed_native_sync(self, driver: SyncSQLiteDriver) -> None:
        driver.execute(_SQLITE_CREATE_WORLD_SQL)
        count_row = driver.select_one(_SQLITE_COUNT_WORLD_SQL)
        existing = int(next(iter(count_row.values()))) if isinstance(count_row, Mapping) else int(count_row[0])
        if existing < WORLD_ROW_COUNT:
            driver.execute("BEGIN")
            try:
                for row_id, random_num in world_seed_rows():
                    driver.execute(_SQLITE_INSERT_WORLD_SQL, (row_id, random_num))
                driver.execute("COMMIT")
            except BaseException:
                with contextlib.suppress(Exception):
                    driver.execute("ROLLBACK")
                raise

    async def _seed_native_async(self, driver: AsyncSQLiteDriver) -> None:
        await driver.execute(_SQLITE_CREATE_WORLD_SQL)
        count_row = await driver.select_one(_SQLITE_COUNT_WORLD_SQL)
        existing = int(next(iter(count_row.values()))) if isinstance(count_row, Mapping) else int(count_row[0])
        if existing < WORLD_ROW_COUNT:
            await driver.execute("BEGIN")
            try:
                for row_id, random_num in world_seed_rows():
                    await driver.execute(_SQLITE_INSERT_WORLD_SQL, (row_id, random_num))
                await driver.execute("COMMIT")
            except BaseException:
                with contextlib.suppress(Exception):
                    await driver.execute("ROLLBACK")
                raise

    def _loop_init_lock(self, loop: asyncio.AbstractEventLoop) -> asyncio.Lock:
        with self._loop_lock:
            lock = self._async_locks.get(loop)
            if lock is None:
                lock = asyncio.Lock()
                self._async_locks[loop] = lock
            return lock

    async def _ensure_async_driver(self) -> AsyncSQLiteDriver:
        loop = asyncio.get_running_loop()
        existing = self._async_drivers.get(loop)
        if existing is not None:
            return existing
        async with self._loop_init_lock(loop):
            existing = self._async_drivers.get(loop)
            if existing is not None:
                return existing
            driver = AsyncSQLiteDriver(self.sqlite_config)
            if self.is_memory:
                await self._seed_native_async(driver)
            self._async_drivers[loop] = driver
            return driver

    def fetch_world_sync(self, row_id: int) -> WorldRow:
        with self._sync_lock:
            rec = self._sync_driver.select_one(_SQLITE_SELECT_WORLD_SQL, (row_id,))
        return record_to_world_row(rec, row_id)

    def fetch_worlds_sync(self, row_ids: list[int]) -> list[WorldRow]:
        with self._sync_lock:
            return [
                record_to_world_row(self._sync_driver.select_one(_SQLITE_SELECT_WORLD_SQL, (row_id,)), row_id)
                for row_id in row_ids
            ]

    def update_worlds_sync(self, updates: list[tuple[int, int]]) -> list[WorldRow]:
        updated: list[WorldRow] = []
        with self._sync_lock:
            self._sync_driver.execute("BEGIN IMMEDIATE")
            try:
                for row_id, new_value in updates:
                    _ = self._sync_driver.select_one(_SQLITE_SELECT_WORLD_SQL, (row_id,))
                    self._sync_driver.execute(_SQLITE_UPDATE_WORLD_SQL, (new_value, row_id))
                    updated.append(WorldRow(id=row_id, randomNumber=new_value))
                self._sync_driver.execute("COMMIT")
            except BaseException:
                with contextlib.suppress(Exception):
                    self._sync_driver.execute("ROLLBACK")
                raise
        return updated

    async def fetch_world_async(self, row_id: int) -> WorldRow:
        driver = await self._ensure_async_driver()
        rec = await driver.select_one(_SQLITE_SELECT_WORLD_SQL, (row_id,))
        return record_to_world_row(rec, row_id)

    async def fetch_worlds_async(self, row_ids: list[int]) -> list[WorldRow]:
        driver = await self._ensure_async_driver()
        rows: list[WorldRow] = []
        for row_id in row_ids:
            rec = await driver.select_one(_SQLITE_SELECT_WORLD_SQL, (row_id,))
            rows.append(record_to_world_row(rec, row_id))
        return rows

    async def update_worlds_async(self, updates: list[tuple[int, int]]) -> list[WorldRow]:
        driver = await self._ensure_async_driver()
        updated: list[WorldRow] = []
        for row_id, new_value in updates:
            _ = await driver.select_one(_SQLITE_SELECT_WORLD_SQL, (row_id,))
            await driver.execute(_SQLITE_UPDATE_WORLD_SQL, (new_value, row_id))
            updated.append(WorldRow(id=row_id, randomNumber=new_value))
        return updated

    def close(self) -> None:
        with self._sync_lock, contextlib.suppress(Exception):
            self._sync_driver.close()
        with self._loop_lock:
            drivers = list(self._async_drivers.values())
            self._async_drivers.clear()
        for drv in drivers:
            close_fn = getattr(drv, "close", None)
            if callable(close_fn) and not asyncio.iscoroutinefunction(close_fn):
                with contextlib.suppress(Exception):
                    close_fn()

    async def aclose(self) -> None:
        with self._loop_lock:
            drivers = list(self._async_drivers.values())
            self._async_drivers.clear()
        for drv in drivers:
            with contextlib.suppress(Exception):
                await drv.close()
        self.close()


class StdlibSQLiteWorldDriver:
    """Stdlib ``sqlite3`` (sync) and lazy ``aiosqlite`` (async) ``WorldDriver``."""

    def __init__(self, config: DataBackendConfig) -> None:
        self.db_target = normalize_sqlite_target(config.db_dsn)
        self.is_memory = self.db_target == ":memory:" or "mode=memory" in self.db_target
        self._sync_lock = threading.Lock()
        self._sync_conn = sqlite3.connect(
            self.db_target,
            uri=self.db_target.startswith("file:"),
            check_same_thread=False,
            timeout=5.0,
        )
        _seed_sqlite_connection_inline(self._sync_conn, is_memory=self.is_memory)
        self._async_conns: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, Any] = weakref.WeakKeyDictionary()
        self._async_locks: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = (
            weakref.WeakKeyDictionary()
        )
        self._loop_lock = threading.Lock()

    def _loop_init_lock(self, loop: asyncio.AbstractEventLoop) -> asyncio.Lock:
        with self._loop_lock:
            lock = self._async_locks.get(loop)
            if lock is None:
                lock = asyncio.Lock()
                self._async_locks[loop] = lock
            return lock

    async def _ensure_async_conn(self) -> Any:
        loop = asyncio.get_running_loop()
        existing = self._async_conns.get(loop)
        if existing is not None:
            return existing
        async with self._loop_init_lock(loop):
            existing = self._async_conns.get(loop)
            if existing is not None:
                return existing
            aiosqlite_mod = import_optional("aiosqlite")
            if aiosqlite_mod is None:
                msg = "aiosqlite is required for async stdlib SQLite benchmarks (install bench-comparators)"
                raise RuntimeError(msg)
            conn = await aiosqlite_mod.connect(
                self.db_target,
                uri=self.db_target.startswith("file:"),
                timeout=5.0,
            )
            if not self.is_memory:
                await conn.execute("PRAGMA journal_mode = WAL")
                await conn.execute("PRAGMA synchronous = NORMAL")
                await conn.execute("PRAGMA busy_timeout = 5000")
            else:
                await conn.execute(_SQLITE_CREATE_WORLD_SQL)
                async with conn.execute(_SQLITE_COUNT_WORLD_SQL) as cur:
                    row = await cur.fetchone()
                existing_count = int(row[0]) if row is not None else 0
                if existing_count < WORLD_ROW_COUNT:
                    await conn.executemany(_SQLITE_INSERT_WORLD_SQL, world_seed_rows())
                    await conn.commit()
            self._async_conns[loop] = conn
            return conn

    def fetch_world_sync(self, row_id: int) -> WorldRow:
        with self._sync_lock:
            rec = self._sync_conn.execute(_SQLITE_SELECT_WORLD_SQL, (row_id,)).fetchone()
        return record_to_world_row(rec, row_id)

    def fetch_worlds_sync(self, row_ids: list[int]) -> list[WorldRow]:
        with self._sync_lock:
            return [
                record_to_world_row(self._sync_conn.execute(_SQLITE_SELECT_WORLD_SQL, (row_id,)).fetchone(), row_id)
                for row_id in row_ids
            ]

    def update_worlds_sync(self, updates: list[tuple[int, int]]) -> list[WorldRow]:
        updated: list[WorldRow] = []
        with self._sync_lock:
            for row_id, new_value in updates:
                _ = self._sync_conn.execute(_SQLITE_SELECT_WORLD_SQL, (row_id,)).fetchone()
                self._sync_conn.execute(_SQLITE_UPDATE_WORLD_SQL, (new_value, row_id))
                updated.append(WorldRow(id=row_id, randomNumber=new_value))
            self._sync_conn.commit()
        return updated

    async def fetch_world_async(self, row_id: int) -> WorldRow:
        conn = await self._ensure_async_conn()
        async with conn.execute(_SQLITE_SELECT_WORLD_SQL, (row_id,)) as cur:
            rec = await cur.fetchone()
        return record_to_world_row(rec, row_id)

    async def fetch_worlds_async(self, row_ids: list[int]) -> list[WorldRow]:
        conn = await self._ensure_async_conn()
        rows: list[WorldRow] = []
        for row_id in row_ids:
            async with conn.execute(_SQLITE_SELECT_WORLD_SQL, (row_id,)) as cur:
                rec = await cur.fetchone()
            rows.append(record_to_world_row(rec, row_id))
        return rows

    async def update_worlds_async(self, updates: list[tuple[int, int]]) -> list[WorldRow]:
        conn = await self._ensure_async_conn()
        updated: list[WorldRow] = []
        for row_id, new_value in updates:
            async with conn.execute(_SQLITE_SELECT_WORLD_SQL, (row_id,)) as cur:
                _ = await cur.fetchone()
            await conn.execute(_SQLITE_UPDATE_WORLD_SQL, (new_value, row_id))
            updated.append(WorldRow(id=row_id, randomNumber=new_value))
        await conn.commit()
        return updated

    def close(self) -> None:
        with self._sync_lock, contextlib.suppress(Exception):
            self._sync_conn.close()
        with self._loop_lock:
            self._async_conns.clear()

    async def aclose(self) -> None:
        with self._loop_lock:
            conns = list(self._async_conns.values())
            self._async_conns.clear()
        for conn in conns:
            with contextlib.suppress(Exception):
                await conn.close()
        self.close()


class NativePostgresWorldDriver:
    """Vertebra native Rust PostgreSQL ``WorldDriver`` using ``SyncPostgreSQLDriver`` and ``AsyncPostgreSQLDriver``."""

    def __init__(self, config: DataBackendConfig, *, pool_size: int) -> None:
        self.dsn = require_postgres_dsn(config.db_dsn)
        self.pool_size = max(1, pool_size)
        sync_broker, async_broker = resolve_broker_transports(config.broker_mode, pool_size=self.pool_size)
        self.broker = sync_broker
        self.async_broker = async_broker
        self.pg_config = PostgreSQLConfig.from_url(
            self.dsn,
            min_size=1,
            max_size=self.pool_size,
            broker=self.broker,
        )
        self.async_pg_config = PostgreSQLConfig.from_url(
            self.dsn,
            min_size=1,
            max_size=self.pool_size,
            broker=self.async_broker,
        )
        self._sync_lock = threading.Lock()
        self._sync_driver: SyncPostgreSQLDriver | None = None
        self._seeded = False
        self._seed_lock = threading.Lock()
        self._async_drivers: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, AsyncPostgreSQLDriver] = (
            weakref.WeakKeyDictionary()
        )
        self._async_locks: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = (
            weakref.WeakKeyDictionary()
        )
        self._loop_lock = threading.Lock()

    def _ensure_sync_driver(self) -> SyncPostgreSQLDriver:
        with self._sync_lock:
            if self._sync_driver is None:
                self._sync_driver = SyncPostgreSQLDriver(self.pg_config)
            if not self._seeded:
                with self._seed_lock:
                    if not self._seeded:
                        _seed_pg_native_sync(self._sync_driver)
                        self._seeded = True
            return self._sync_driver

    def _loop_init_lock(self, loop: asyncio.AbstractEventLoop) -> asyncio.Lock:
        with self._loop_lock:
            lock = self._async_locks.get(loop)
            if lock is None:
                lock = asyncio.Lock()
                self._async_locks[loop] = lock
            return lock

    async def _ensure_async_driver(self) -> AsyncPostgreSQLDriver:
        loop = asyncio.get_running_loop()
        existing = self._async_drivers.get(loop)
        if existing is not None:
            return existing
        async with self._loop_init_lock(loop):
            existing = self._async_drivers.get(loop)
            if existing is not None:
                return existing
            driver = AsyncPostgreSQLDriver(self.async_pg_config)
            await driver.connect()
            if not self._seeded:
                await _seed_pg_native_async(driver)
                self._seeded = True
            self._async_drivers[loop] = driver
            return driver

    @contextlib.asynccontextmanager
    async def _checkout_async_client(self, driver: AsyncPostgreSQLDriver) -> AsyncIterator[Any]:
        """Acquire one physical pooled client and at most one SHM broker lease for a multi-query request."""
        lease: Any = None
        if self.async_broker is not None:
            lease = await self.async_broker.acquire(bind_key="default")
        client: Any = None
        try:
            client = await driver.pool.acquire()
            yield client
        finally:
            if client is not None:
                with CancelScope(shield=True), contextlib.suppress(Exception):
                    await client.close()
            if lease is not None:
                with CancelScope(shield=True), contextlib.suppress(Exception):
                    release_sync = getattr(lease, "release_sync", None)
                    if callable(release_sync):
                        release_sync()
                    else:
                        await lease.release()

    def fetch_world_sync(self, row_id: int) -> WorldRow:
        driver = self._ensure_sync_driver()
        rec = driver.select_one(_PG_SELECT_WORLD_DOLLAR_SQL, (row_id,))
        return record_to_world_row(rec, row_id)

    def fetch_worlds_sync(self, row_ids: list[int]) -> list[WorldRow]:
        driver = self._ensure_sync_driver()
        return [
            record_to_world_row(driver.select_one(_PG_SELECT_WORLD_DOLLAR_SQL, (row_id,)), row_id) for row_id in row_ids
        ]

    def update_worlds_sync(self, updates: list[tuple[int, int]]) -> list[WorldRow]:
        driver = self._ensure_sync_driver()
        updated: list[WorldRow] = []
        for row_id, new_value in updates:
            _ = driver.select_one(_PG_SELECT_WORLD_DOLLAR_SQL, (row_id,))
            driver.execute(_PG_UPDATE_WORLD_DOLLAR_SQL, (new_value, row_id))
            updated.append(WorldRow(id=row_id, randomNumber=new_value))
        return updated

    async def fetch_world_async(self, row_id: int) -> WorldRow:
        driver = await self._ensure_async_driver()
        rec = await driver.select_one(_PG_SELECT_WORLD_DOLLAR_SQL, (row_id,))
        return record_to_world_row(rec, row_id)

    async def fetch_worlds_async(self, row_ids: list[int]) -> list[WorldRow]:
        driver = await self._ensure_async_driver()
        rows: list[WorldRow] = []
        async with self._checkout_async_client(driver) as client:
            for row_id in row_ids:
                recs = await client.fetch_dicts(_PG_SELECT_WORLD_DOLLAR_SQL, (row_id,))
                rows.append(record_to_world_row(recs[0] if recs else None, row_id))
        return rows

    async def update_worlds_async(self, updates: list[tuple[int, int]]) -> list[WorldRow]:
        driver = await self._ensure_async_driver()
        sorted_updates = sorted(updates, key=lambda pair: pair[0])
        async with self._checkout_async_client(driver) as client:
            await client.begin()
            try:
                for row_id, new_value in sorted_updates:
                    _ = await client.fetch_dicts(_PG_SELECT_WORLD_DOLLAR_SQL, (row_id,))
                    await client.execute_dml(_PG_UPDATE_WORLD_DOLLAR_SQL, (new_value, row_id))
                await client.commit()
            except BaseException:
                with CancelScope(shield=True), contextlib.suppress(Exception):
                    await client.rollback()
                raise
        return [WorldRow(id=row_id, randomNumber=new_value) for row_id, new_value in updates]

    def close(self) -> None:
        with self._sync_lock:
            if self._sync_driver is not None:
                with contextlib.suppress(Exception):
                    self._sync_driver.close()
                self._sync_driver = None
        with self._loop_lock:
            self._async_drivers.clear()

    async def aclose(self) -> None:
        with self._loop_lock:
            drivers = list(self._async_drivers.values())
            self._async_drivers.clear()
        for drv in drivers:
            with contextlib.suppress(Exception):
                await drv.close()
        self.close()


class StdlibPostgresWorldDriver:
    """Upstream ``psycopg`` / ``psycopg_pool`` (sync) and lazy ``asyncpg`` (async) ``WorldDriver``."""

    def __init__(self, config: DataBackendConfig, *, pool_size: int) -> None:
        self.dsn = require_postgres_dsn(config.db_dsn)
        self.pool_size = max(1, pool_size)
        self._sync_lock = threading.Lock()
        self._sync_pool: Any = None
        self._sync_conn: Any = None
        self._seeded = False
        self._seed_lock = threading.Lock()
        self._async_pools: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, Any] = weakref.WeakKeyDictionary()
        self._async_locks: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = (
            weakref.WeakKeyDictionary()
        )
        self._loop_lock = threading.Lock()

    def _ensure_sync_resources(self) -> tuple[Any, Any]:
        with self._sync_lock:
            if self._sync_pool is None and self._sync_conn is None:
                psycopg_mod = import_optional("psycopg")
                if psycopg_mod is None:
                    msg = f"psycopg is required for stdlib sync PostgreSQL benchmarks ({POSTGRES_BENCH_INSTALL_HINT})"
                    raise RuntimeError(msg)
                psycopg_pool_mod = import_optional("psycopg_pool")
                if psycopg_pool_mod is not None and hasattr(psycopg_pool_mod, "ConnectionPool"):
                    self._sync_pool = psycopg_pool_mod.ConnectionPool(
                        self.dsn,
                        min_size=1,
                        max_size=self.pool_size,
                        open=True,
                    )
                else:
                    self._sync_conn = psycopg_mod.connect(self.dsn, autocommit=True)
            if not self._seeded:
                with self._seed_lock:
                    if not self._seeded:
                        if self._sync_pool is not None:
                            with self._sync_pool.connection() as conn:
                                conn.execute(_PG_ADVISORY_LOCK_SQL)
                                conn.execute(_PG_CREATE_WORLD_SQL)
                                conn.execute(_PG_SEED_WORLD_SQL)
                                conn.commit()
                        elif self._sync_conn is not None:
                            with self._sync_conn.transaction():
                                self._sync_conn.execute(_PG_ADVISORY_LOCK_SQL)
                                self._sync_conn.execute(_PG_CREATE_WORLD_SQL)
                                self._sync_conn.execute(_PG_SEED_WORLD_SQL)
                        self._seeded = True
            return self._sync_pool, self._sync_conn

    def _loop_init_lock(self, loop: asyncio.AbstractEventLoop) -> asyncio.Lock:
        with self._loop_lock:
            lock = self._async_locks.get(loop)
            if lock is None:
                lock = asyncio.Lock()
                self._async_locks[loop] = lock
            return lock

    async def _ensure_async_pool(self) -> Any:
        loop = asyncio.get_running_loop()
        existing = self._async_pools.get(loop)
        if existing is not None:
            return existing
        async with self._loop_init_lock(loop):
            existing = self._async_pools.get(loop)
            if existing is not None:
                return existing
            asyncpg_mod = import_optional("asyncpg")
            if asyncpg_mod is None:
                msg = f"asyncpg is required for stdlib async PostgreSQL benchmarks ({POSTGRES_BENCH_INSTALL_HINT})"
                raise RuntimeError(msg)
            pool = await asyncpg_mod.create_pool(self.dsn, min_size=1, max_size=self.pool_size)
            if not self._seeded:
                async with pool.acquire() as conn, conn.transaction():
                    await conn.execute(_PG_ADVISORY_LOCK_SQL)
                    await conn.execute(_PG_CREATE_WORLD_SQL)
                    await conn.execute(_PG_SEED_WORLD_SQL)
                self._seeded = True
            self._async_pools[loop] = pool
            return pool

    def fetch_world_sync(self, row_id: int) -> WorldRow:
        pool, conn = self._ensure_sync_resources()
        if pool is not None:
            with pool.connection() as pooled_conn:
                rec = pooled_conn.execute(_PG_SELECT_WORLD_PERCENT_SQL, (row_id,)).fetchone()
            return record_to_world_row(rec, row_id)
        with self._sync_lock:
            rec = conn.execute(_PG_SELECT_WORLD_PERCENT_SQL, (row_id,)).fetchone()
        return record_to_world_row(rec, row_id)

    def fetch_worlds_sync(self, row_ids: list[int]) -> list[WorldRow]:
        pool, conn = self._ensure_sync_resources()
        if pool is not None:
            with pool.connection() as pooled_conn:
                return [
                    record_to_world_row(pooled_conn.execute(_PG_SELECT_WORLD_PERCENT_SQL, (row_id,)).fetchone(), row_id)
                    for row_id in row_ids
                ]
        with self._sync_lock:
            return [
                record_to_world_row(conn.execute(_PG_SELECT_WORLD_PERCENT_SQL, (row_id,)).fetchone(), row_id)
                for row_id in row_ids
            ]

    def update_worlds_sync(self, updates: list[tuple[int, int]]) -> list[WorldRow]:
        pool, conn = self._ensure_sync_resources()
        updated: list[WorldRow] = []
        if pool is not None:
            with pool.connection() as pooled_conn:
                for row_id, new_value in updates:
                    _ = pooled_conn.execute(_PG_SELECT_WORLD_PERCENT_SQL, (row_id,)).fetchone()
                    pooled_conn.execute(_PG_UPDATE_WORLD_PERCENT_SQL, (new_value, row_id))
                    updated.append(WorldRow(id=row_id, randomNumber=new_value))
                pooled_conn.commit()
            return updated
        with self._sync_lock:
            for row_id, new_value in updates:
                _ = conn.execute(_PG_SELECT_WORLD_PERCENT_SQL, (row_id,)).fetchone()
                conn.execute(_PG_UPDATE_WORLD_PERCENT_SQL, (new_value, row_id))
                updated.append(WorldRow(id=row_id, randomNumber=new_value))
        return updated

    async def fetch_world_async(self, row_id: int) -> WorldRow:
        await ensure_task_context()
        pool = await self._ensure_async_pool()
        async with pool.acquire() as conn:
            rec = await conn.fetchrow(_PG_SELECT_WORLD_DOLLAR_SQL, row_id)
        return record_to_world_row(rec, row_id)

    async def fetch_worlds_async(self, row_ids: list[int]) -> list[WorldRow]:
        await ensure_task_context()
        pool = await self._ensure_async_pool()
        rows: list[WorldRow] = []
        async with pool.acquire() as conn:
            for row_id in row_ids:
                rec = await conn.fetchrow(_PG_SELECT_WORLD_DOLLAR_SQL, row_id)
                rows.append(record_to_world_row(rec, row_id))
        return rows

    async def update_worlds_async(self, updates: list[tuple[int, int]]) -> list[WorldRow]:
        await ensure_task_context()
        pool = await self._ensure_async_pool()
        sorted_updates = sorted(updates, key=lambda pair: pair[0])
        async with pool.acquire() as conn, conn.transaction():
            for row_id, new_value in sorted_updates:
                _ = await conn.fetchrow(_PG_SELECT_WORLD_DOLLAR_SQL, row_id)
                await conn.execute(_PG_UPDATE_WORLD_DOLLAR_SQL, new_value, row_id)
        return [WorldRow(id=row_id, randomNumber=new_value) for row_id, new_value in updates]

    def close(self) -> None:
        with self._sync_lock:
            if self._sync_pool is not None:
                with contextlib.suppress(Exception):
                    self._sync_pool.close()
                self._sync_pool = None
            if self._sync_conn is not None:
                with contextlib.suppress(Exception):
                    self._sync_conn.close()
                self._sync_conn = None
        with self._loop_lock:
            self._async_pools.clear()

    async def aclose(self) -> None:
        with self._loop_lock:
            pools = list(self._async_pools.values())
            self._async_pools.clear()
        for pool in pools:
            with contextlib.suppress(Exception):
                await pool.close()
        self.close()


def build_world_driver(config: DataBackendConfig, *, pool_size: int) -> WorldDriver:
    """Construct the ``WorldDriver`` matching ``config.db_backend`` and ``config.db_adapter``."""
    backend = config.db_backend.strip().lower()
    adapter = config.db_adapter.strip().lower()

    if adapter in {"sqlspec", "advanced_alchemy"}:
        from tools.benchmarks.apps._db_orm_drivers import build_orm_world_driver

        return build_orm_world_driver(config, pool_size=pool_size)

    if backend == "sqlite":
        if adapter == "native":
            return NativeSQLiteWorldDriver(config, pool_size=pool_size)
        if adapter == "stdlib":
            return StdlibSQLiteWorldDriver(config)
    elif backend == "postgres":
        if adapter == "native":
            return NativePostgresWorldDriver(config, pool_size=pool_size)
        if adapter == "stdlib":
            return StdlibPostgresWorldDriver(config, pool_size=pool_size)

    msg = f"Unsupported db_backend/db_adapter combination: ({config.db_backend!r}, {config.db_adapter!r})"
    raise ValueError(msg)
