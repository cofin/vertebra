"""SQLite statement completion, affected counts, and transaction behavior."""

from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import MappingProxyType
from typing import Any, NamedTuple, cast

import msgspec
import pytest

import vertebra.db.dbapi as dbapi_pkg
from vertebra._sqlite import AsyncSQLiteClient, SyncSQLiteClient
from vertebra._vertebra import (
    DatabaseError,
    IntegrityError,
    InterfaceError,
    LeaseBroker,
    NotFoundError,
    ProgrammingError,
    ReadyAwaitable,
)
from vertebra.db.adapters import sqlite as sqlite_pkg
from vertebra.db.adapters.sqlite import AsyncSQLiteDriver, SQLiteConfig, SyncSQLiteDriver
from vertebra.db.adapters.sqlite import _async as sqlite_async_mod
from vertebra.db.adapters.sqlite import _sync as sqlite_sync_mod
from vertebra.db.adapters.sqlite import driver as sqlite_driver_mod


def test_execute_returning_commits_affected_rows(tmp_path: Path) -> None:
    """Complete RETURNING statements before committing and report their affected counts."""
    config = SQLiteConfig(database=tmp_path / "returning.db")
    driver = SyncSQLiteDriver(config)
    observer = SyncSQLiteDriver(config)
    try:
        driver.execute("CREATE TABLE items (value INTEGER)")
        assert driver.execute("INSERT INTO items VALUES (?), (?) RETURNING value", (1, 2)) == 2
        assert observer.execute_tuples("SELECT value FROM items ORDER BY value") == [(1,), (2,)]
        assert driver.execute("UPDATE items SET value = value + ? RETURNING value", (10,)) == 2
        assert observer.execute_tuples("SELECT value FROM items ORDER BY value") == [(11,), (12,)]
        assert driver.execute("UPDATE items SET value = ? WHERE value = ? RETURNING value", (0, -1)) == 0
        assert driver.execute("DELETE FROM items WHERE value = ? RETURNING value", (-1,)) == 0
        assert driver.execute("DELETE FROM items RETURNING value") == 2
        assert observer.execute_tuples("SELECT value FROM items") == []
    finally:
        observer.close()
        driver.close()


@pytest.mark.anyio
async def test_async_execute_returning_commits_affected_rows(tmp_path: Path) -> None:
    """Async RETURNING calls finish their statements and persist the affected rows."""
    config = SQLiteConfig(database=tmp_path / "returning.db")
    driver = AsyncSQLiteDriver(config)
    observer = AsyncSQLiteDriver(config)
    try:
        await driver.execute("CREATE TABLE items (value INTEGER)")
        assert await driver.execute("INSERT INTO items VALUES (?), (?) RETURNING value", (1, 2)) == 2
        assert await observer.execute_tuples("SELECT value FROM items ORDER BY value") == [(1,), (2,)]
        assert await driver.execute("UPDATE items SET value = value + ? RETURNING value", (10,)) == 2
        assert await observer.execute_tuples("SELECT value FROM items ORDER BY value") == [(11,), (12,)]
        assert await driver.execute("UPDATE items SET value = ? WHERE value = ? RETURNING value", (0, -1)) == 0
        assert await driver.execute("DELETE FROM items WHERE value = ? RETURNING value", (-1,)) == 0
        assert await driver.execute("DELETE FROM items RETURNING value") == 2
        assert await observer.execute_tuples("SELECT value FROM items") == []
    finally:
        await observer.close()
        await driver.close()


@pytest.mark.parametrize("commit_transaction", [False, True], ids=["rollback", "commit"])
def test_execute_returning_respects_transaction(tmp_path: Path, commit_transaction: bool) -> None:
    """RETURNING reports counts while the transaction retains control of persistence."""
    config = SQLiteConfig(database=tmp_path / "returning.db")
    driver = SyncSQLiteDriver(config)
    observer = SyncSQLiteDriver(config)
    try:
        driver.execute("CREATE TABLE items (value INTEGER)")
        driver.begin()
        assert driver.execute("INSERT INTO items VALUES (?), (?) RETURNING value", (1, 2)) == 2
        assert driver.execute("UPDATE items SET value = value + ? RETURNING value", (10,)) == 2
        assert driver.execute("DELETE FROM items WHERE value = ? RETURNING value", (11,)) == 1
        assert driver.execute("UPDATE items SET value = ? WHERE value = ? RETURNING value", (0, -1)) == 0
        assert driver.execute_tuples("SELECT value FROM items") == [(12,)]
        assert observer.execute_tuples("SELECT value FROM items") == []
        if commit_transaction:
            driver.commit()
        else:
            driver.rollback()
        expected = [(12,)] if commit_transaction else []
        assert observer.execute_tuples("SELECT value FROM items") == expected
        assert driver.execute("INSERT INTO items VALUES (?) RETURNING value", (20,)) == 1
        assert observer.execute_tuples("SELECT value FROM items ORDER BY value") == [*expected, (20,)]
    finally:
        observer.close()
        driver.close()


@pytest.mark.anyio
@pytest.mark.parametrize("commit_transaction", [False, True], ids=["rollback", "commit"])
async def test_async_execute_returning_respects_transaction(tmp_path: Path, commit_transaction: bool) -> None:
    """Async RETURNING counts remain accurate through explicit commit and rollback."""
    config = SQLiteConfig(database=tmp_path / "returning.db")
    driver = AsyncSQLiteDriver(config)
    observer = AsyncSQLiteDriver(config)
    try:
        await driver.execute("CREATE TABLE items (value INTEGER)")
        await driver.begin()
        assert await driver.execute("INSERT INTO items VALUES (?), (?) RETURNING value", (1, 2)) == 2
        assert await driver.execute("UPDATE items SET value = value + ? RETURNING value", (10,)) == 2
        assert await driver.execute("DELETE FROM items WHERE value = ? RETURNING value", (11,)) == 1
        assert await driver.execute("UPDATE items SET value = ? WHERE value = ? RETURNING value", (0, -1)) == 0
        assert await driver.execute_tuples("SELECT value FROM items") == [(12,)]
        assert await observer.execute_tuples("SELECT value FROM items") == []
        if commit_transaction:
            await driver.commit()
        else:
            await driver.rollback()
        expected = [(12,)] if commit_transaction else []
        assert await observer.execute_tuples("SELECT value FROM items") == expected
        assert await driver.execute("INSERT INTO items VALUES (?) RETURNING value", (20,)) == 1
        assert await observer.execute_tuples("SELECT value FROM items ORDER BY value") == [*expected, (20,)]
    finally:
        await observer.close()
        await driver.close()


def test_sync_sqlite_driver_connect_reopens_closed_connection_and_reapplies_pragmas(tmp_path: Path) -> None:
    """Reopening a closed SyncSQLiteDriver restores connectivity, persisted rows, and initialization pragmas."""
    db_file = tmp_path / "reopen.db"
    config = SQLiteConfig(path=str(db_file), busy_timeout_ms=2500, foreign_keys=True)
    driver = SyncSQLiteDriver(config)
    try:
        driver.execute("CREATE TABLE items (value INTEGER)")
        assert driver.execute("INSERT INTO items VALUES (?)", (42,)) == 1
        driver.close()
        assert driver.connect() is driver
        assert driver.execute_tuples("SELECT value FROM items") == [(42,)]
        assert driver.execute_tuples("PRAGMA busy_timeout") == [(2500,)]
        assert driver.execute_tuples("PRAGMA foreign_keys") == [(1,)]
        assert driver.execute("INSERT INTO items VALUES (?)", (99,)) == 1
        assert driver.execute_tuples("SELECT value FROM items ORDER BY value") == [(42,), (99,)]
    finally:
        driver.close()


def test_sync_sqlite_driver_serializes_concurrent_multithreaded_operations(tmp_path: Path) -> None:
    """Concurrent multi-threaded SyncSQLiteDriver writes and reads serialize cleanly without corruption."""
    config = SQLiteConfig(path=str(tmp_path / "concurrent.db"), busy_timeout_ms=5000)
    driver = SyncSQLiteDriver(config)
    try:
        driver.execute("CREATE TABLE concurrent_items (worker_id INTEGER, seq INTEGER)")

        def run_worker(worker_id: int) -> int:
            inserted = sum(
                driver.execute("INSERT INTO concurrent_items (worker_id, seq) VALUES (?, ?)", (worker_id, seq))
                for seq in range(10)
            )
            count_rows = driver.execute_tuples("SELECT COUNT(*) FROM concurrent_items")
            assert count_rows[0][0] >= inserted
            return inserted

        with ThreadPoolExecutor(max_workers=6) as pool:
            counts = list(pool.map(run_worker, range(6)))

        assert counts == [10, 10, 10, 10, 10, 10]
        assert driver.execute_tuples("SELECT COUNT(*) FROM concurrent_items") == [(60,)]
    finally:
        driver.close()


@pytest.mark.parametrize("driver_cls", [SyncSQLiteDriver, AsyncSQLiteDriver], ids=["sync", "async"])
@pytest.mark.parametrize(
    "helper_name",
    ["_acquire_sqlite_broker_lease", "_release_sqlite_broker_lease"],
)
def test_sqlite_driver_probe_helpers_removed(driver_cls: type[Any], helper_name: str) -> None:
    """SyncSQLiteDriver and AsyncSQLiteDriver no longer expose legacy SQLite broker probe helpers."""
    assert not hasattr(driver_cls, helper_name)


def test_sync_sqlite_driver_queries_take_no_lease_or_statement_cache_entry(tmp_path: Path) -> None:
    """SyncSQLiteDriver query methods leave the broker lease and statement-cache counters untouched."""
    broker = LeaseBroker(pool_size=2, max_overflow=0, stmt_capacity=16)
    driver = SyncSQLiteDriver(SQLiteConfig(path=str(tmp_path / "broker_sync.db")), broker=broker)
    assert driver.broker is broker
    try:
        driver.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        insert_sql = "INSERT INTO items (id, label) VALUES (?, ?)"
        assert driver.execute(insert_sql, (1, "first")) == 1
        assert driver.execute(insert_sql, (2, "second")) == 1
        assert driver.execute_tuples("SELECT id FROM items ORDER BY id") == [(1,), (2,)]
        assert driver.execute_many(insert_sql, [(3, "third")]) == 1
        assert driver.select_to_arrow("SELECT id FROM items").to_arrow_table().num_rows == 3

        with pytest.raises(DatabaseError):
            driver.execute_tuples("SELECT * FROM nonexistent_sqlite_table")

        broker_stats = broker.stats()
        assert broker_stats.total_acquired == 0
        assert broker_stats.active_leases == 0
        sc = broker.statement_cache()
        assert sc is not None
        cache_stats = sc.stats()
        assert cache_stats.inserts == 0
        assert cache_stats.hits == 0
    finally:
        driver.close()


@pytest.mark.anyio
async def test_async_sqlite_driver_queries_take_no_lease_or_statement_cache_entry(tmp_path: Path) -> None:
    """AsyncSQLiteDriver query methods leave the broker lease and statement-cache counters untouched."""
    broker = LeaseBroker(pool_size=2, max_overflow=0, stmt_capacity=16)
    driver = AsyncSQLiteDriver(SQLiteConfig(path=str(tmp_path / "broker_async.db")), broker=broker)
    assert driver.broker is broker
    try:
        await driver.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        insert_sql = "INSERT INTO items (id, label) VALUES (?, ?)"
        assert await driver.execute(insert_sql, (1, "first")) == 1
        assert await driver.execute(insert_sql, (2, "second")) == 1
        assert await driver.execute_tuples("SELECT id FROM items ORDER BY id") == [(1,), (2,)]
        assert await driver.execute_many(insert_sql, [(3, "third")]) == 1
        assert (await driver.select_to_arrow("SELECT id FROM items")).to_arrow_table().num_rows == 3

        with pytest.raises(DatabaseError):
            await driver.execute_tuples("SELECT * FROM nonexistent_sqlite_table")

        broker_stats = broker.stats()
        assert broker_stats.total_acquired == 0
        assert broker_stats.active_leases == 0
        sc = broker.statement_cache()
        assert sc is not None
        cache_stats = sc.stats()
        assert cache_stats.inserts == 0
        assert cache_stats.hits == 0
    finally:
        await driver.close()


@pytest.mark.parametrize(
    ("module", "name"),
    [
        (sqlite_sync_mod, "_sqlite_param_count"),
        (sqlite_sync_mod, "classify_sql_statement"),
        (sqlite_async_mod, "_sqlite_param_count"),
        (sqlite_async_mod, "classify_sql_statement"),
    ],
    ids=["sync_param_count", "sync_classify", "async_param_count", "async_classify"],
)
def test_sqlite_driver_modules_drop_statement_classification_names(module: Any, name: str) -> None:
    """The SQLite driver modules no longer carry SQL classification or parameter-count helpers."""
    assert not hasattr(module, name)


def test_sync_sqlite_driver_cursor_backed_query_results() -> None:
    """Sync driver query methods report rows, columns, counts, and rowids from one native cursor call."""
    driver = SyncSQLiteDriver()
    try:
        assert driver.execute_with_description("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)") == ([], ())
        assert driver.lastrowid is None

        assert driver.execute_dml("INSERT INTO items (label) VALUES (?)", ("a",)) == (1, 1)
        assert driver.lastrowid == 1
        assert driver.execute_dml("UPDATE items SET label = ?", ("b",)) == (1, None)
        assert driver.lastrowid is None

        assert driver.execute_tuples("INSERT INTO items (label) VALUES (?) RETURNING id", ("c",)) == [(2,)]
        assert driver.lastrowid == 2
        assert driver.execute_with_description("SELECT id, label FROM items ORDER BY id") == (
            [(1, "b"), (2, "c")],
            ("id", "label"),
        )
        assert driver.lastrowid is None

        assert driver.execute("SELECT id FROM items") == 0
        assert driver.execute_dml("UPDATE items SET label = 'z' WHERE id = 99") == (0, None)
        assert driver.select("CREATE TABLE other (id INTEGER)") == []
        assert driver.select_one("SELECT 1 AS a") == {"a": 1}
        assert driver.select_value("SELECT count(*) FROM items") == 2

        many_sql = "INSERT INTO items (label) VALUES (?)"
        assert driver.execute_many(many_sql, [("d",), ("e",), ("f",)]) == 3
        assert driver.execute_many(many_sql, []) == 0
        assert driver.select_value("SELECT count(*) FROM items") == 5

        with pytest.raises(IntegrityError):
            driver.execute("INSERT INTO items (id, label) VALUES (?, ?)", (1, "dup"))
        with pytest.raises(ProgrammingError):
            driver.execute_tuples("SELECT * FROM missing_items")
    finally:
        driver.close()


@pytest.mark.anyio
async def test_async_sqlite_driver_cursor_backed_query_results() -> None:
    """Async driver query methods report rows, columns, counts, and rowids from one native cursor call."""
    driver = AsyncSQLiteDriver()
    try:
        created = await driver.execute_with_description("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        assert created == ([], ())
        assert driver.lastrowid is None

        assert await driver.execute_dml("INSERT INTO items (label) VALUES (?)", ("a",)) == (1, 1)
        assert driver.lastrowid == 1
        assert await driver.execute_dml("UPDATE items SET label = ?", ("b",)) == (1, None)
        assert driver.lastrowid is None

        assert await driver.execute_tuples("INSERT INTO items (label) VALUES (?) RETURNING id", ("c",)) == [(2,)]
        assert driver.lastrowid == 2
        assert await driver.execute_with_description("SELECT id, label FROM items ORDER BY id") == (
            [(1, "b"), (2, "c")],
            ("id", "label"),
        )
        assert driver.lastrowid is None

        assert await driver.execute("SELECT id FROM items") == 0
        assert await driver.execute_dml("UPDATE items SET label = 'z' WHERE id = 99") == (0, None)
        assert await driver.select("CREATE TABLE other (id INTEGER)") == []
        assert await driver.select_one("SELECT 1 AS a") == {"a": 1}
        assert await driver.select_value("SELECT count(*) FROM items") == 2

        many_sql = "INSERT INTO items (label) VALUES (?)"
        assert await driver.execute_many(many_sql, [("d",), ("e",), ("f",)]) == 3
        assert await driver.execute_many(many_sql, []) == 0
        assert await driver.select_value("SELECT count(*) FROM items") == 5

        with pytest.raises(IntegrityError):
            await driver.execute("INSERT INTO items (id, label) VALUES (?, ?)", (1, "dup"))
        with pytest.raises(ProgrammingError):
            await driver.execute_tuples("SELECT * FROM missing_items")
    finally:
        await driver.close()


def test_sync_sqlite_driver_cursor_returns_new_native_cursor() -> None:
    """SyncSQLiteDriver.cursor hands out a distinct native cursor per call and rejects a closed driver."""
    driver = SyncSQLiteDriver()
    try:
        first = driver.cursor()
        second = driver.cursor()
        assert first is not second
        assert type(first).__name__ == "SyncSQLiteCursor"
        assert type(second).__name__ == "SyncSQLiteCursor"
        first.execute("SELECT 1")
        assert first.fetchall() == [(1,)]
    finally:
        driver.close()
    with pytest.raises(InterfaceError, match="SQLite driver is closed"):
        driver.cursor()
    with pytest.raises(InterfaceError, match="SQLite driver is closed"):
        driver.execute_tuples("SELECT 1")


@pytest.mark.anyio
async def test_async_sqlite_driver_cursor_returns_new_native_cursor() -> None:
    """AsyncSQLiteDriver.cursor is a plain call that hands out a distinct native cursor each time."""
    driver = AsyncSQLiteDriver()
    try:
        first = driver.cursor()
        second = driver.cursor()
        assert first is not second
        assert type(first).__name__ == "AsyncSQLiteCursor"
        assert type(second).__name__ == "AsyncSQLiteCursor"
        await first.execute("SELECT 1")
        assert await first.fetchall() == [(1,)]
    finally:
        await driver.close()
    with pytest.raises(InterfaceError, match="SQLite driver is closed"):
        driver.cursor()
    with pytest.raises(InterfaceError, match="SQLite driver is closed"):
        await driver.execute_tuples("SELECT 1")


def test_sync_sqlite_driver_cursor_lazy_begin_opens_transaction() -> None:
    """A lazy-begin cursor from the sync driver opens a transaction the driver can roll back."""
    driver = SyncSQLiteDriver()
    try:
        driver.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        assert driver.in_transaction is False
        driver.cursor(lazy_begin=True).execute("INSERT INTO items VALUES (?, ?)", (1, "a"))
        assert driver.in_transaction is True
        driver.rollback()
        assert driver.in_transaction is False
        assert driver.execute_tuples("SELECT count(*) FROM items") == [(0,)]

        driver.execute("INSERT INTO items VALUES (?, ?)", (2, "b"))
        assert driver.in_transaction is False
        assert driver.execute_tuples("SELECT id FROM items") == [(2,)]
    finally:
        driver.close()


@pytest.mark.anyio
async def test_async_sqlite_driver_cursor_lazy_begin_opens_transaction() -> None:
    """A lazy-begin cursor from the async driver opens a transaction the driver can roll back."""
    driver = AsyncSQLiteDriver()
    try:
        await driver.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        assert driver.in_transaction is False
        await driver.cursor(lazy_begin=True).execute("INSERT INTO items VALUES (?, ?)", (1, "a"))
        assert driver.in_transaction is True
        await driver.rollback()
        assert driver.in_transaction is False
        assert await driver.execute_tuples("SELECT count(*) FROM items") == [(0,)]

        await driver.execute("INSERT INTO items VALUES (?, ?)", (2, "b"))
        assert driver.in_transaction is False
        assert await driver.execute_tuples("SELECT id FROM items") == [(2,)]
    finally:
        await driver.close()


def test_execute_tuples_returning_autocommits_without_implicit_transaction(tmp_path: Path) -> None:
    """Sync execute_tuples with RETURNING runs in autocommit outside an explicit transaction."""
    config = SQLiteConfig(database=tmp_path / "tuples_autocommit.db")
    driver = SyncSQLiteDriver(config)
    observer = SyncSQLiteDriver(config)
    try:
        driver.execute("CREATE TABLE items (value INTEGER)")
        assert driver.in_transaction is False

        inserted = driver.execute_tuples("INSERT INTO items VALUES (?) RETURNING value", (1,))
        assert inserted == [(1,)]
        assert driver.in_transaction is False
        assert observer.execute_tuples("SELECT value FROM items ORDER BY value") == [(1,)]

        updated = driver.execute_tuples("UPDATE items SET value = value + ? RETURNING value", (10,))
        assert updated == [(11,)]
        assert driver.in_transaction is False
        assert observer.execute_tuples("SELECT value FROM items ORDER BY value") == [(11,)]

        deleted = driver.execute_tuples("DELETE FROM items WHERE value = ? RETURNING value", (11,))
        assert deleted == [(11,)]
        assert driver.in_transaction is False
        assert observer.execute_tuples("SELECT value FROM items") == []
    finally:
        observer.close()
        driver.close()


@pytest.mark.anyio
async def test_async_execute_tuples_returning_autocommits_without_implicit_transaction(tmp_path: Path) -> None:
    """Async execute_tuples with RETURNING runs in autocommit outside an explicit transaction."""
    config = SQLiteConfig(database=tmp_path / "async_tuples_autocommit.db")
    driver = AsyncSQLiteDriver(config)
    observer = AsyncSQLiteDriver(config)
    try:
        await driver.execute("CREATE TABLE items (value INTEGER)")
        assert driver.in_transaction is False

        inserted = await driver.execute_tuples("INSERT INTO items VALUES (?) RETURNING value", (1,))
        assert inserted == [(1,)]
        assert driver.in_transaction is False
        assert await observer.execute_tuples("SELECT value FROM items ORDER BY value") == [(1,)]

        updated = await driver.execute_tuples("UPDATE items SET value = value + ? RETURNING value", (10,))
        assert updated == [(11,)]
        assert driver.in_transaction is False
        assert await observer.execute_tuples("SELECT value FROM items ORDER BY value") == [(11,)]

        deleted = await driver.execute_tuples("DELETE FROM items WHERE value = ? RETURNING value", (11,))
        assert deleted == [(11,)]
        assert driver.in_transaction is False
        assert await observer.execute_tuples("SELECT value FROM items") == []
    finally:
        await observer.close()
        await driver.close()


@pytest.mark.parametrize("commit_transaction", [False, True], ids=["rollback", "commit"])
def test_execute_tuples_returning_respects_explicit_transaction(tmp_path: Path, commit_transaction: bool) -> None:
    """Sync execute_tuples preserves active caller transaction scope until commit or rollback."""
    config = SQLiteConfig(database=tmp_path / "tuples_tx.db")
    driver = SyncSQLiteDriver(config)
    observer = SyncSQLiteDriver(config)
    try:
        driver.execute("CREATE TABLE items (value INTEGER)")
        driver.begin()
        assert driver.in_transaction is True
        assert driver.execute_tuples("INSERT INTO items VALUES (?) RETURNING value", (7,)) == [(7,)]
        assert driver.in_transaction is True
        assert observer.execute_tuples("SELECT value FROM items") == []
        if commit_transaction:
            driver.commit()
        else:
            driver.rollback()
        assert driver.in_transaction is False
        expected = [(7,)] if commit_transaction else []
        assert observer.execute_tuples("SELECT value FROM items") == expected
    finally:
        observer.close()
        driver.close()


@pytest.mark.anyio
@pytest.mark.parametrize("commit_transaction", [False, True], ids=["rollback", "commit"])
async def test_async_execute_tuples_returning_respects_explicit_transaction(
    tmp_path: Path,
    commit_transaction: bool,
) -> None:
    """Async execute_tuples preserves active caller transaction scope until commit or rollback."""
    config = SQLiteConfig(database=tmp_path / "async_tuples_tx.db")
    driver = AsyncSQLiteDriver(config)
    observer = AsyncSQLiteDriver(config)
    try:
        await driver.execute("CREATE TABLE items (value INTEGER)")
        await driver.begin()
        assert driver.in_transaction is True
        assert await driver.execute_tuples("INSERT INTO items VALUES (?) RETURNING value", (7,)) == [(7,)]
        assert driver.in_transaction is True
        assert await observer.execute_tuples("SELECT value FROM items") == []
        if commit_transaction:
            await driver.commit()
        else:
            await driver.rollback()
        assert driver.in_transaction is False
        expected = [(7,)] if commit_transaction else []
        assert await observer.execute_tuples("SELECT value FROM items") == expected
    finally:
        await observer.close()
        await driver.close()


def test_sqlite_driver_module_split_import_parity() -> None:
    """SQLite package, driver barrel, _sync, and _async expose identical driver classes."""
    assert sqlite_pkg.SyncSQLiteDriver is sqlite_driver_mod.SyncSQLiteDriver
    assert sqlite_driver_mod.SyncSQLiteDriver is sqlite_sync_mod.SyncSQLiteDriver
    assert sqlite_pkg.AsyncSQLiteDriver is sqlite_driver_mod.AsyncSQLiteDriver
    assert sqlite_driver_mod.AsyncSQLiteDriver is sqlite_async_mod.AsyncSQLiteDriver


@pytest.mark.parametrize(
    "scenario",
    ["auto_flush", "tail_flush", "caller_tx_rollback", "exception_discard", "caller_tx_exception"],
)
def test_sync_sqlite_pipeline_exit_contract(tmp_path: Path, scenario: str) -> None:
    """SyncSQLitePipeline honors unified R1.14 flush, commit, discard, and caller tx ownership."""
    config = SQLiteConfig(database=tmp_path / f"pipe_{scenario}.db")
    driver = SyncSQLiteDriver(config)
    observer = SyncSQLiteDriver(config)
    try:
        driver.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        if scenario == "auto_flush":
            with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO items VALUES (?, ?)", (1, "auto"))
                pipe.select("SELECT id, label FROM items ORDER BY id")
            assert driver.in_transaction is False
            assert observer.execute_tuples("SELECT id, label FROM items ORDER BY id") == [(1, "auto")]
        elif scenario == "tail_flush":
            with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO items VALUES (?, ?)", (1, "first"))
                assert pipe.results() == [1]
                assert observer.execute_tuples("SELECT id, label FROM items") == []
                pipe.execute("INSERT INTO items VALUES (?, ?)", (2, "tail"))
            assert driver.in_transaction is False
            assert observer.execute_tuples("SELECT id, label FROM items ORDER BY id") == [
                (1, "first"),
                (2, "tail"),
            ]
        elif scenario == "caller_tx_rollback":
            driver.begin()
            driver.execute("INSERT INTO items VALUES (?, ?)", (1, "outer"))
            with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO items VALUES (?, ?)", (2, "inner"))
            assert driver.in_transaction is True
            assert driver.execute_tuples("SELECT id, label FROM items ORDER BY id") == [
                (1, "outer"),
                (2, "inner"),
            ]
            assert observer.execute_tuples("SELECT id, label FROM items") == []
            driver.rollback()
            assert observer.execute_tuples("SELECT id, label FROM items") == []
        elif scenario == "exception_discard":
            with pytest.raises(RuntimeError, match="abort"), driver.pipeline() as pipe:
                pipe.execute("INSERT INTO items VALUES (?, ?)", (1, "first"))
                assert pipe.results() == [1]
                pipe.execute("INSERT INTO items VALUES (?, ?)", (2, "discarded"))
                raise RuntimeError("abort")
            assert driver.in_transaction is False
            assert observer.execute_tuples("SELECT id, label FROM items") == []
        else:
            driver.begin()
            driver.execute("INSERT INTO items VALUES (?, ?)", (1, "outer_kept"))
            with pytest.raises(RuntimeError, match="abort_inner"), driver.pipeline() as pipe:
                pipe.execute("INSERT INTO items VALUES (?, ?)", (2, "inner_flushed"))
                assert pipe.results() == [1]
                pipe.execute("INSERT INTO items VALUES (?, ?)", (3, "inner_discarded"))
                raise RuntimeError("abort_inner")
            assert driver.in_transaction is True
            driver.commit()
            assert observer.execute_tuples("SELECT id, label FROM items ORDER BY id") == [(1, "outer_kept")]
    finally:
        observer.close()
        driver.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "scenario",
    ["auto_flush", "tail_flush", "caller_tx_rollback", "exception_discard", "caller_tx_exception"],
)
async def test_async_sqlite_pipeline_exit_contract(tmp_path: Path, scenario: str) -> None:
    """AsyncSQLitePipeline honors unified R1.14 flush, commit, discard, and caller tx ownership."""
    config = SQLiteConfig(database=tmp_path / f"apipe_{scenario}.db")
    driver = AsyncSQLiteDriver(config)
    observer = AsyncSQLiteDriver(config)
    try:
        await driver.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        if scenario == "auto_flush":
            async with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO items VALUES (?, ?)", (1, "auto"))
                pipe.select("SELECT id, label FROM items ORDER BY id")
            assert driver.in_transaction is False
            assert await observer.execute_tuples("SELECT id, label FROM items ORDER BY id") == [(1, "auto")]
        elif scenario == "tail_flush":
            async with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO items VALUES (?, ?)", (1, "first"))
                assert await pipe.results() == [1]
                assert await observer.execute_tuples("SELECT id, label FROM items") == []
                pipe.execute("INSERT INTO items VALUES (?, ?)", (2, "tail"))
            assert driver.in_transaction is False
            assert await observer.execute_tuples("SELECT id, label FROM items ORDER BY id") == [
                (1, "first"),
                (2, "tail"),
            ]
        elif scenario == "caller_tx_rollback":
            await driver.begin()
            await driver.execute("INSERT INTO items VALUES (?, ?)", (1, "outer"))
            async with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO items VALUES (?, ?)", (2, "inner"))
            assert driver.in_transaction is True
            assert await driver.execute_tuples("SELECT id, label FROM items ORDER BY id") == [
                (1, "outer"),
                (2, "inner"),
            ]
            assert await observer.execute_tuples("SELECT id, label FROM items") == []
            await driver.rollback()
            assert await observer.execute_tuples("SELECT id, label FROM items") == []
        elif scenario == "exception_discard":
            with pytest.raises(RuntimeError, match="abort"):
                async with driver.pipeline() as pipe:
                    pipe.execute("INSERT INTO items VALUES (?, ?)", (1, "first"))
                    assert await pipe.results() == [1]
                    pipe.execute("INSERT INTO items VALUES (?, ?)", (2, "discarded"))
                    raise RuntimeError("abort")
            assert driver.in_transaction is False
            assert await observer.execute_tuples("SELECT id, label FROM items") == []
        else:
            await driver.begin()
            await driver.execute("INSERT INTO items VALUES (?, ?)", (1, "outer_kept"))
            with pytest.raises(RuntimeError, match="abort_inner"):
                async with driver.pipeline() as pipe:
                    pipe.execute("INSERT INTO items VALUES (?, ?)", (2, "inner_flushed"))
                    assert await pipe.results() == [1]
                    pipe.execute("INSERT INTO items VALUES (?, ?)", (3, "inner_discarded"))
                    raise RuntimeError("abort_inner")
            assert driver.in_transaction is True
            await driver.commit()
            assert await observer.execute_tuples("SELECT id, label FROM items ORDER BY id") == [(1, "outer_kept")]
    finally:
        await observer.close()
        await driver.close()


def _seeded_cursor_client() -> SyncSQLiteClient:
    """Return an in-memory client holding an empty `items (id, label)` table."""
    client = SyncSQLiteClient(":memory:")
    client.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
    return client


def test_sync_cursor_fresh_state() -> None:
    """Report PEP 249 defaults before any statement runs."""
    client = SyncSQLiteClient(":memory:")
    try:
        cursor = client.cursor()
        assert cursor.description is None
        assert cursor.rowcount == -1
        assert cursor.lastrowid is None
        assert cursor.arraysize == 1
    finally:
        client.close()


@pytest.mark.parametrize(
    ("sql", "params", "names", "rows", "rowcount", "lastrowid"),
    [
        pytest.param("SELECT 1 AS a, 'x' AS b", None, ["a", "b"], [(1, "x")], -1, None, id="query"),
        pytest.param("CREATE TABLE other (id INTEGER)", (), None, None, 0, None, id="ddl"),
        pytest.param("INSERT INTO items VALUES (?, ?)", (1, "a"), None, None, 1, 1, id="insert"),
        pytest.param("SELECT :v AS v", {"v": 5}, ["v"], [(5,)], -1, None, id="named_parameters"),
        pytest.param("INSERT INTO items VALUES (?, ?) RETURNING id", (7, "r"), ["id"], [(7,)], 1, 7, id="returning"),
        pytest.param("SELECT id FROM items WHERE 1 = 0", (), ["id"], [], -1, None, id="empty_result"),
    ],
)
def test_sync_cursor_execute_outcome(
    sql: str,
    params: Any,
    names: list[str] | None,
    rows: list[tuple[Any, ...]] | None,
    rowcount: int,
    lastrowid: int | None,
) -> None:
    """Leave the description, rows, changed-row count, and rowid of one statement on the cursor."""
    client = _seeded_cursor_client()
    try:
        cursor = client.cursor()
        assert cursor.execute(sql, params) is cursor
        description = cursor.description
        if names is None:
            assert description is None
            assert cursor.fetchone() is None
        else:
            assert description is not None
            assert [entry[0] for entry in description] == names
            assert all(len(entry) == 7 and entry[1:] == (None,) * 6 for entry in description)
            assert cursor.description is description
            assert cursor.fetchall() == rows
        assert cursor.rowcount == rowcount
        assert cursor.lastrowid == lastrowid
    finally:
        client.close()


def test_sync_cursor_fetch_walk_and_sizes() -> None:
    """Walk a result set with every fetch method and enforce `arraysize` and size limits."""
    client = _seeded_cursor_client()
    try:
        cursor = client.cursor()
        assert cursor.executemany("INSERT INTO items VALUES (?, ?)", [(1, "a"), (2, "b"), (3, "c")]) is cursor
        assert cursor.rowcount == 3
        assert cursor.description is None
        assert cursor.executemany("INSERT INTO items VALUES (?, ?)", []).rowcount == 0
        assert cursor.description is None

        cursor.execute("SELECT id FROM items ORDER BY id")
        assert cursor.fetchone() == (1,)
        assert cursor.fetchmany(1) == [(2,)]
        assert cursor.fetchall() == [(3,)]
        assert cursor.fetchone() is None
        assert cursor.fetchall() == []

        cursor.execute("SELECT id FROM items ORDER BY id")
        cursor.arraysize = 2
        assert cursor.fetchmany() == [(1,), (2,)]
        for invalid in (0, -1):
            with pytest.raises(ProgrammingError, match="arraysize must be at least 1"):
                cursor.arraysize = invalid
            assert cursor.arraysize == 2
        assert cursor.fetchmany(0) == []
        with pytest.raises(ProgrammingError, match="fetchmany size must not be negative"):
            cursor.fetchmany(-1)
        assert cursor.fetchmany(5) == [(3,)]

        cursor.execute("SELECT id FROM items ORDER BY id")
        cursor.execute("SELECT label FROM items WHERE id = ?", (2,))
        assert cursor.fetchall() == [("b",)]
    finally:
        client.close()


def test_sync_cursor_lazy_begin_opens_transactions() -> None:
    """Open a transaction before each statement when none is active, and only when asked."""
    client = _seeded_cursor_client()
    try:
        lazy = client.cursor(lazy_begin=True)
        lazy.execute("INSERT INTO items VALUES (?, ?)", (1, "a"))
        assert client.in_transaction is True
        client.rollback()
        assert client.cursor().execute("SELECT count(*) FROM items").fetchall() == [(0,)]

        lazy.execute("INSERT INTO items VALUES (?, ?)", (1, "a"))
        client.commit()
        lazy.executemany("INSERT INTO items VALUES (?, ?)", [(2, "b")])
        assert client.in_transaction is True
        client.rollback()
        assert client.cursor().execute("SELECT id FROM items").fetchall() == [(1,)]

        lazy.executemany("INSERT INTO items VALUES (?, ?)", [])
        assert client.in_transaction is False

        default = client.cursor()
        default.execute("INSERT INTO items VALUES (?, ?)", (3, "c"))
        assert client.in_transaction is False
        assert default.execute("SELECT id FROM items ORDER BY id").fetchall() == [(1,), (3,)]
    finally:
        client.close()


def test_sync_cursor_serializes_two_threads() -> None:
    """Run concurrent `execute` calls on one cursor one after the other."""
    client = _seeded_cursor_client()
    try:
        cursor = client.cursor()
        barrier = threading.Barrier(2)

        def insert(row_id: int) -> object:
            barrier.wait(timeout=10)
            return cursor.execute("INSERT INTO items VALUES (?, ?)", (row_id, "t"))

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(insert, (1, 2)))
        assert results == [cursor, cursor]
        assert cursor.rowcount == 1
        assert cursor.lastrowid in {1, 2}
        assert cursor.description is None
        assert cursor.execute("SELECT id FROM items ORDER BY id").fetchall() == [(1,), (2,)]
    finally:
        client.close()


@pytest.mark.parametrize(
    ("method", "sql", "params", "error"),
    [
        pytest.param("execute", "SELEC 1", (), ProgrammingError, id="syntax"),
        pytest.param("execute", "SELECT ?", (object(),), InterfaceError, id="unsupported_bind"),
        pytest.param("execute", "INSERT INTO items VALUES (?, ?)", (1, "dup"), IntegrityError, id="constraint"),
        pytest.param(
            "executemany",
            "INSERT INTO items VALUES (?, ?)",
            [(2, "b"), (2, "dup")],
            IntegrityError,
            id="executemany_constraint",
        ),
        pytest.param(
            "executemany", "INSERT INTO items VALUES (?, ?)", [(object(), "x")], InterfaceError, id="many_bind"
        ),
    ],
)
@pytest.mark.parametrize(
    "prior",
    [
        pytest.param(("SELECT id FROM items", ()), id="after_query"),
        pytest.param(("INSERT INTO items VALUES (?, ?)", (9, "prior")), id="after_insert"),
    ],
)
def test_sync_cursor_failed_statement_clears_result(
    method: str, sql: str, params: Any, error: type[Exception], prior: tuple[str, Any]
) -> None:
    """Raise the classified error and clear the previous result when a statement fails."""
    client = _seeded_cursor_client()
    try:
        client.execute("INSERT INTO items VALUES (?, ?)", (1, "a"))
        cursor = client.cursor()
        cursor.execute(*prior)
        with pytest.raises(error) as exc_info:
            getattr(cursor, method)(sql, params)
        assert cursor.description is None
        assert cursor.rowcount == -1
        assert cursor.lastrowid is None
        assert cursor.fetchall() == []
        if error is IntegrityError:
            assert error is dbapi_pkg.IntegrityError
            assert isinstance(exc_info.value, DatabaseError)
            assert exc_info.value.sqlstate
    finally:
        client.close()


@pytest.mark.parametrize(("fetch", "expected"), [("fetchone", None), ("fetchmany", []), ("fetchall", [])])
@pytest.mark.parametrize("after_dml", [False, True], ids=["before_execute", "after_dml"])
def test_sync_cursor_fetch_without_result_set(fetch: str, expected: Any, after_dml: bool) -> None:
    """Return empty results before any statement and after a statement with no result set."""
    client = _seeded_cursor_client()
    try:
        cursor = client.cursor()
        if after_dml:
            cursor.execute("INSERT INTO items VALUES (?, ?)", (1, "a"))
        assert getattr(cursor, fetch)() == expected
    finally:
        client.close()


@pytest.mark.parametrize(
    ("method", "args"),
    [
        ("execute", ("SELECT 1",)),
        ("executemany", ("INSERT INTO items VALUES (?, ?)", [(1, "a")])),
        ("fetchone", ()),
        ("fetchmany", ()),
        ("fetchall", ()),
    ],
)
def test_sync_cursor_closed_rejects_calls(method: str, args: tuple[Any, ...]) -> None:
    """Close silently twice and reject every later call with `InterfaceError`."""
    client = _seeded_cursor_client()
    try:
        cursor = client.cursor()
        cursor.execute("SELECT 1 AS a")
        cursor.close()
        cursor.close()
        assert cursor.description is None
        assert cursor.rowcount == -1
        with pytest.raises(InterfaceError, match="cursor is closed"):
            getattr(cursor, method)(*args)
        assert client.cursor().execute("SELECT count(*) FROM items").fetchall() == [(0,)]
    finally:
        client.close()


async def _seeded_async_cursor_client(path: str = ":memory:") -> AsyncSQLiteClient:
    """Return an async client holding an empty `items (id, label)` table."""
    client = AsyncSQLiteClient(path)
    await client.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
    return client


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("sql", "params", "names", "rows", "rowcount", "lastrowid"),
    [
        pytest.param("SELECT 1 AS a", None, ["a"], [(1,)], -1, None, id="query"),
        pytest.param("INSERT INTO items VALUES (?, ?)", (1, "a"), None, None, 1, 1, id="insert"),
        pytest.param("SELECT :v AS v", {"v": 5}, ["v"], [(5,)], -1, None, id="named_parameters"),
        pytest.param("SELECT id FROM items WHERE 1 = 0", (), ["id"], [], -1, None, id="empty_result"),
    ],
)
async def test_async_cursor_execute_outcome(
    sql: str,
    params: Any,
    names: list[str] | None,
    rows: list[tuple[Any, ...]] | None,
    rowcount: int,
    lastrowid: int | None,
) -> None:
    """Resolve `execute` to the cursor and leave one statement outcome on it."""
    client = await _seeded_async_cursor_client()
    try:
        cursor = client.cursor()
        assert cursor.description is None
        assert (cursor.rowcount, cursor.lastrowid, cursor.arraysize) == (-1, None, 1)
        assert await cursor.execute(sql, params) is cursor
        description = cursor.description
        if names is None:
            assert description is None
            assert await cursor.fetchone() is None
        else:
            assert description is not None
            assert [entry[0] for entry in description] == names
            assert all(len(entry) == 7 and entry[1:] == (None,) * 6 for entry in description)
            assert cursor.description is description
            assert await cursor.fetchall() == rows
        assert cursor.rowcount == rowcount
        assert cursor.lastrowid == lastrowid
    finally:
        await client.close()


@pytest.mark.anyio
async def test_async_cursor_fetch_walk_and_sizes() -> None:
    """Walk one buffered result through every awaitable fetch method and size rule."""
    client = await _seeded_async_cursor_client()
    try:
        cursor = client.cursor()
        rows = [(1, "a"), (2, "b"), (3, "c")]
        assert await cursor.executemany("INSERT INTO items VALUES (?, ?)", rows) is cursor
        assert cursor.rowcount == 3
        assert cursor.description is None
        assert (await cursor.executemany("INSERT INTO items VALUES (?, ?)", [])).rowcount == 0

        await cursor.execute("SELECT id FROM items ORDER BY id")
        assert await cursor.fetchone() == (1,)
        assert await cursor.fetchmany(1) == [(2,)]
        assert await cursor.fetchall() == [(3,)]
        assert await cursor.fetchone() is None
        assert await cursor.fetchall() == []

        await cursor.execute("SELECT id FROM items ORDER BY id")
        cursor.arraysize = 2
        assert await cursor.fetchmany() == [(1,), (2,)]
        for invalid in (0, -1):
            with pytest.raises(ProgrammingError, match="arraysize"):
                cursor.arraysize = invalid
            assert cursor.arraysize == 2
        with pytest.raises(ProgrammingError, match="negative"):
            cursor.fetchmany(-1)
        assert await cursor.fetchmany(0) == []
        assert await cursor.fetchmany(5) == [(3,)]
    finally:
        await client.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("method", "args"),
    [
        ("execute", ("SELECT 1",)),
        ("executemany", ("INSERT INTO items VALUES (?, ?)", [(1, "a")])),
        ("fetchone", ()),
        ("fetchmany", ()),
        ("fetchall", ()),
    ],
)
async def test_async_cursor_state_errors_raise_at_the_call(method: str, args: tuple[Any, ...]) -> None:
    """Raise state errors from the method call itself, before any awaitable exists."""
    client = await _seeded_async_cursor_client()
    try:
        cursor = client.cursor()
        if method.startswith("fetch"):
            expected: Any = None if method == "fetchone" else []
            assert await getattr(cursor, method)(*args) == expected
        await cursor.execute("SELECT 1 AS a")
        await cursor.close()
        await cursor.close()
        assert cursor.description is None
        assert cursor.rowcount == -1
        with pytest.raises(InterfaceError, match="cursor is closed"):
            getattr(cursor, method)(*args)
        assert await (await client.cursor().execute("SELECT count(*) FROM items")).fetchall() == [(0,)]
    finally:
        await client.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("method", "sql", "params", "error", "at_call"),
    [
        pytest.param("execute", "SELEC 1", (), ProgrammingError, False, id="syntax"),
        pytest.param("execute", "SELECT ?", (object(),), InterfaceError, True, id="unsupported_bind"),
        pytest.param("execute", "INSERT INTO items VALUES (?, ?)", (1, "dup"), IntegrityError, False, id="constraint"),
        pytest.param(
            "executemany",
            "INSERT INTO items VALUES (?, ?)",
            [(2, "b"), (2, "dup")],
            IntegrityError,
            False,
            id="executemany_constraint",
        ),
        pytest.param(
            "executemany", "INSERT INTO items VALUES (?, ?)", [(object(), "x")], InterfaceError, True, id="many_bind"
        ),
    ],
)
async def test_async_cursor_failed_statement_clears_result(
    method: str, sql: str, params: Any, error: type[Exception], at_call: bool
) -> None:
    """Raise bind errors at the call and engine errors at the await, clearing the prior result."""
    client = await _seeded_async_cursor_client()
    try:
        await client.execute("INSERT INTO items VALUES (?, ?)", (1, "a"))
        cursor = client.cursor()
        await cursor.execute("SELECT id FROM items")
        assert cursor.description is not None
        if at_call:
            with pytest.raises(error):
                getattr(cursor, method)(sql, params)
        else:
            awaitable = getattr(cursor, method)(sql, params)
            with pytest.raises(error) as exc_info:
                await awaitable
            if error is IntegrityError:
                assert getattr(exc_info.value, "sqlstate", None)
        assert cursor.description is None
        assert cursor.rowcount == -1
        assert cursor.lastrowid is None
        assert await cursor.fetchall() == []
        assert await (await cursor.execute("SELECT 2 AS b")).fetchall() == [(2,)]
    finally:
        await client.close()


@pytest.mark.anyio
async def test_async_cursor_lazy_begin_opens_transactions() -> None:
    """Open a transaction before each statement when none is active, and only when asked."""
    client = await _seeded_async_cursor_client()
    try:
        lazy = client.cursor(lazy_begin=True)
        await lazy.execute("INSERT INTO items VALUES (?, ?)", (1, "a"))
        assert client.in_transaction is True
        await client.rollback()
        assert await (await client.cursor().execute("SELECT count(*) FROM items")).fetchall() == [(0,)]

        await lazy.execute("INSERT INTO items VALUES (?, ?)", (1, "a"))
        await client.commit()
        await lazy.executemany("INSERT INTO items VALUES (?, ?)", [(2, "b")])
        assert client.in_transaction is True
        await client.rollback()
        assert await (await client.cursor().execute("SELECT count(*) FROM items")).fetchall() == [(1,)]

        await lazy.executemany("INSERT INTO items VALUES (?, ?)", [])
        assert client.in_transaction is False

        default = client.cursor()
        await default.execute("INSERT INTO items VALUES (?, ?)", (3, "c"))
        assert client.in_transaction is False
    finally:
        await client.close()


@pytest.mark.anyio
async def test_async_cursor_rejects_execute_in_flight() -> None:
    """Reject a second statement while one is in flight and leave the first untouched."""
    client = await _seeded_async_cursor_client()
    try:
        cursor = client.cursor()
        first = cursor.execute("SELECT 1 AS a")
        for method, args in (("execute", ("SELECT 2 AS b",)), ("executemany", ("SELECT ?", [(1,)]))):
            with pytest.raises(InterfaceError, match="cursor is already executing"):
                getattr(cursor, method)(*args)
        assert await first is cursor
        assert await cursor.fetchall() == [(1,)]
        assert await (await cursor.execute("SELECT 2 AS b")).fetchall() == [(2,)]
    finally:
        await client.close()


@pytest.mark.anyio
async def test_async_cursor_cancelled_execute_clears_result_and_releases_cursor() -> None:
    """Clear the prior result when an awaited statement is cancelled and accept a later statement."""
    client = await _seeded_async_cursor_client()
    try:
        cursor = client.cursor()
        await cursor.execute("SELECT 1 AS a")
        endless = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c"
        task = asyncio.ensure_future(cursor.execute(endless))
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert cursor.description is None
        assert cursor.rowcount == -1

        rows: list[tuple[Any, ...]] | None = None
        for _ in range(500):
            try:
                await cursor.execute("SELECT 2 AS b")
            except InterfaceError:
                await asyncio.sleep(0.01)
                continue
            rows = await cursor.fetchall()
            break
        assert rows == [(2,)]
    finally:
        await client.close()


@pytest.mark.anyio
async def test_async_cursor_shares_pool_with_sync_cursor(tmp_path: Path) -> None:
    """Read through a sync cursor a row that the async cursor inserted on the same pool."""
    client = await _seeded_async_cursor_client(str(tmp_path / "shared.db"))
    try:
        await client.cursor().execute("INSERT INTO items VALUES (?, ?)", (1, "a"))
        sync_cursor = client.to_sync().cursor()
        assert sync_cursor.execute("SELECT id, label FROM items").fetchall() == [(1, "a")]
    finally:
        await client.close()


class _ParamPair(NamedTuple):
    first: int
    second: int


@pytest.mark.parametrize(
    ("sql", "params", "expected"),
    [
        pytest.param("SELECT ?, ?", (1, "a"), [(1, "a")], id="tuple"),
        pytest.param("SELECT ?, ?", [1, "a"], [(1, "a")], id="list"),
        pytest.param("SELECT :v", {"v": 5}, [(5,)], id="dict"),
        pytest.param("SELECT 1", (), [(1,)], id="empty_tuple"),
        pytest.param("SELECT 1", [], [(1,)], id="empty_list"),
        pytest.param("SELECT 1", {}, [(1,)], id="empty_dict"),
        pytest.param("SELECT 1", None, [(1,)], id="none"),
        pytest.param("SELECT ?, ?", _ParamPair(1, 2), [(1, 2)], id="named_tuple"),
        pytest.param("SELECT :v", MappingProxyType({"v": 5}), [(5,)], id="mapping_proxy"),
    ],
)
def test_sync_cursor_bind_fast_path_and_fallbacks(
    sql: str,
    params: Any,
    expected: list[tuple[Any, ...]],
) -> None:
    """Bind exact containers, empty containers, subclasses, and non-dict mappings on sync cursor."""
    client = SyncSQLiteClient(":memory:")
    try:
        cursor = client.cursor()
        assert cursor.execute(sql, params).fetchall() == expected
    finally:
        client.close()


def test_sync_cursor_rejects_generator_params_and_clears_result() -> None:
    """Reject generator parameters with InterfaceError and clear the prior cursor result."""
    client = SyncSQLiteClient(":memory:")
    try:
        cursor = client.cursor()
        cursor.execute("SELECT 1 AS a")
        assert cursor.description is not None
        bad_params: Any = (x for x in [1])
        with pytest.raises(InterfaceError, match="parameters must be a sequence or mapping"):
            cursor.execute("SELECT ?", bad_params)
        assert cursor.description is None
        assert cursor.rowcount == -1
        assert cursor.fetchone() is None
    finally:
        client.close()


def test_sync_cursor_reuses_same_sql_string() -> None:
    """Execute the same borrowed SQL string object repeatedly with varying parameters."""
    client = SyncSQLiteClient(":memory:")
    try:
        cursor = client.cursor()
        sql = "SELECT ?"
        for i in range(100):
            assert cursor.execute(sql, (i,)).fetchall() == [(i,)]
    finally:
        client.close()


def _seeded_single_call_client() -> SyncSQLiteClient:
    """Return an in-memory client seeded with two rows in `items (id, label)`."""
    client = _seeded_cursor_client()
    client.execute("INSERT INTO items VALUES (?, ?)", (1, "a"))
    client.execute("INSERT INTO items VALUES (?, ?)", (2, "b"))
    return client


def test_sync_single_call_select_and_dicts_match_cursor() -> None:
    """Return rows, column names, and dicts matching the cursor on an equally seeded client."""
    client = _seeded_single_call_client()
    peer = _seeded_single_call_client()
    try:
        sql = "SELECT id, label FROM items ORDER BY id"
        rows, names = client.fetch_rows(sql)
        cursor = peer.cursor()
        cursor.execute(sql)
        cursor_rows = cursor.fetchall()
        assert cursor.description is not None
        cursor_names = tuple(entry[0] for entry in cursor.description)

        assert (rows, names) == (cursor_rows, cursor_names)
        assert isinstance(rows, list)
        assert all(isinstance(row, tuple) for row in rows)
        assert isinstance(names, tuple)
        assert all(isinstance(name, str) for name in names)
        assert client.fetch_dicts(sql) == [{"id": 1, "label": "a"}, {"id": 2, "label": "b"}]
    finally:
        peer.close()
        client.close()


def test_sync_single_call_empty_result_and_no_result_set() -> None:
    """Handle empty result sets, statements with no result set, values, and duplicate names."""
    client = _seeded_single_call_client()
    try:
        empty_sql = "SELECT id, label FROM items WHERE 1 = 0"
        assert client.fetch_rows(empty_sql) == ([], ("id", "label"))
        assert client.fetch_dicts(empty_sql) == []
        with pytest.raises(NotFoundError):
            client.fetch_value(empty_sql)

        assert client.fetch_rows("CREATE TABLE other_rows (x INTEGER)") == ([], ())
        assert client.fetch_dicts("CREATE TABLE other_dicts (x INTEGER)") == []
        with pytest.raises(NotFoundError):
            client.fetch_value("CREATE TABLE other_value (x INTEGER)")
        assert (
            client.fetch_value("SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name LIKE 'other_%'") == 3
        )

        assert client.fetch_value("SELECT count(*) FROM items") == 2
        assert client.fetch_value("SELECT NULL") is None
        assert client.fetch_dicts("SELECT 1 AS a, 2 AS a") == [{"a": 2}]
    finally:
        client.close()


def test_sync_single_call_dml_and_returning() -> None:
    """Report affected rows and rowids across DML, RETURNING, and read statements."""
    client = _seeded_single_call_client()
    try:
        assert client.execute_dml("INSERT INTO items (label) VALUES (?)", ("c",)) == (1, 3)
        assert client.execute_dml("UPDATE items SET label = ? WHERE id <= ?", ("z", 2)) == (2, None)
        assert client.execute_dml("UPDATE items SET label = ? WHERE id = ?", ("miss", 999)) == (0, None)
        assert client.execute_dml("SELECT id FROM items") == (0, None)

        rows, names = client.fetch_rows("INSERT INTO items (label) VALUES (?) RETURNING id", ("d",))
        assert rows == [(4,)]
        assert names == ("id",)
        assert client.fetch_dicts("INSERT INTO items (label) VALUES (?) RETURNING id", ("e",)) == [{"id": 5}]
        assert client.execute_dml("INSERT INTO items (label) VALUES (?) RETURNING id", ("f",)) == (1, 6)
    finally:
        client.close()


def test_sync_last_statement_rowid_lifecycle_and_isolation() -> None:
    """Track only single-call statements in `last_statement_rowid` and preserve it across failures and cursor calls."""
    client = _seeded_cursor_client()
    try:
        assert client.last_statement_rowid is None
        async_view = client.to_async()
        sync_view = async_view.to_sync()
        assert sync_view.last_statement_rowid is None

        changes, first_rowid = client.execute_dml("INSERT INTO items (label) VALUES (?)", ("first",))
        assert changes == 1
        assert first_rowid == 1
        assert client.last_statement_rowid == first_rowid

        assert client.fetch_rows("SELECT id FROM items") == ([(1,)], ("id",))
        assert client.last_statement_rowid is None

        rows, names = client.fetch_rows("INSERT INTO items (label) VALUES (?) RETURNING id", ("second",))
        assert rows == [(2,)]
        assert names == ("id",)
        assert client.last_statement_rowid == 2

        with pytest.raises(ProgrammingError):
            client.fetch_rows("SELEC 1")
        assert client.last_statement_rowid == 2

        with pytest.raises(InterfaceError):
            client.execute_dml("INSERT INTO items (label) VALUES (?)", (object(),))
        assert client.last_statement_rowid == 2

        cursor = client.cursor()
        cursor.execute("INSERT INTO items (label) VALUES (?)", ("via_cursor",))
        assert cursor.lastrowid == 3
        assert client.last_statement_rowid == 2

        client.execute("INSERT INTO items (label) VALUES (?)", ("via_execute",))
        client.execute_many("INSERT INTO items (label) VALUES (?)", [("via_many",)])
        client.execute_pipeline([(False, "INSERT INTO items (label) VALUES (?)", ("via_pipe",))])
        assert client.last_statement_rowid == 2
    finally:
        client.close()


@pytest.mark.parametrize(
    ("select_sql", "dml_sql", "params"),
    [
        pytest.param(
            "SELECT id, label FROM items WHERE id = ?", "UPDATE items SET label = 'u' WHERE id = ?", (1,), id="tuple"
        ),
        pytest.param(
            "SELECT id, label FROM items WHERE id = ?", "UPDATE items SET label = 'u' WHERE id = ?", [1], id="list"
        ),
        pytest.param(
            "SELECT id, label FROM items WHERE id = :v",
            "UPDATE items SET label = 'u' WHERE id = :v",
            {"v": 1},
            id="dict",
        ),
        pytest.param(
            "SELECT id, label FROM items WHERE id = 1",
            "UPDATE items SET label = 'u' WHERE id = 1",
            (),
            id="empty_tuple",
        ),
        pytest.param(
            "SELECT id, label FROM items WHERE id = 1", "UPDATE items SET label = 'u' WHERE id = 1", None, id="none"
        ),
    ],
)
def test_sync_single_call_parameters_match_cursor(
    select_sql: str,
    dml_sql: str,
    params: Any,
) -> None:
    """Bind tuple, list, dict, empty tuple, and None parameters identically to the cursor across all four methods."""
    client = _seeded_single_call_client()
    peer = _seeded_single_call_client()
    try:
        cursor = peer.cursor()
        cursor.execute(select_sql, params)
        cursor_rows = cursor.fetchall()
        assert cursor.description is not None
        cursor_names = tuple(entry[0] for entry in cursor.description)
        cursor_dicts = [dict(zip(cursor_names, row, strict=True)) for row in cursor_rows]

        assert client.fetch_rows(select_sql, params) == (cursor_rows, cursor_names)
        assert client.fetch_dicts(select_sql, params) == cursor_dicts
        assert client.fetch_value(select_sql, params) == cursor_rows[0][0]

        cursor.execute(dml_sql, params)
        assert client.execute_dml(dml_sql, params) == (cursor.rowcount, cursor.lastrowid)
    finally:
        peer.close()
        client.close()


@pytest.mark.parametrize("method_name", ["fetch_rows", "fetch_dicts", "fetch_value", "execute_dml"])
@pytest.mark.parametrize(
    ("sql", "params", "expected_error", "expect_sqlstate"),
    [
        pytest.param("INSERT INTO items VALUES (?, ?)", (1, "dup"), IntegrityError, True, id="duplicate_key"),
        pytest.param("SELEC 1", (), ProgrammingError, False, id="syntax"),
        pytest.param("SELECT ?", (object(),), InterfaceError, False, id="unsupported_bind"),
    ],
)
def test_sync_single_call_error_classes_match_cursor(
    method_name: str,
    sql: str,
    params: Any,
    expected_error: type[DatabaseError],
    expect_sqlstate: bool,
) -> None:
    """Raise the same exception class and SQLSTATE metadata as the cursor across all four methods."""
    client = _seeded_single_call_client()
    peer = _seeded_single_call_client()
    try:
        with pytest.raises(expected_error) as cursor_exc:
            peer.cursor().execute(sql, params)
        with pytest.raises(expected_error) as single_exc:
            getattr(client, method_name)(sql, params)

        assert type(single_exc.value) is type(cursor_exc.value)
        if expect_sqlstate:
            assert getattr(single_exc.value, "sqlstate", None) is not None
            assert getattr(single_exc.value, "sqlstate", None) == getattr(cursor_exc.value, "sqlstate", None)
    finally:
        peer.close()
        client.close()


@pytest.mark.parametrize("method_name", ["fetch_rows", "fetch_dicts", "fetch_value", "execute_dml"])
def test_sync_single_call_closed_client_matches_cursor(method_name: str) -> None:
    """Raise the same closed-client exception class as `cursor().execute` on every single-call method."""
    client = _seeded_cursor_client()
    client.close()
    with pytest.raises(InterfaceError) as cursor_exc:
        client.cursor().execute("SELECT 1")
    with pytest.raises(InterfaceError) as single_exc:
        getattr(client, method_name)("SELECT 1")
    assert type(single_exc.value) is type(cursor_exc.value)


def test_sync_single_call_dml_opens_no_transaction(tmp_path: Path) -> None:
    """Commit `execute_dml` immediately without opening an explicit transaction."""
    db_path = str(tmp_path / "single_call_autocommit.db")
    client = SyncSQLiteClient(db_path)
    observer = SyncSQLiteClient(db_path)
    try:
        client.execute_dml("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        assert client.execute_dml("INSERT INTO items VALUES (?, ?)", (1, "visible")) == (1, 1)
        assert client.in_transaction is False
        assert observer.fetch_rows("SELECT id, label FROM items") == ([(1, "visible")], ("id", "label"))
    finally:
        observer.close()
        client.close()


async def _seeded_async_single_call_client() -> AsyncSQLiteClient:
    """Return an async in-memory client seeded with two rows in `items (id, label)`."""
    client = await _seeded_async_cursor_client()
    await client.execute("INSERT INTO items VALUES (?, ?)", (1, "a"))
    await client.execute("INSERT INTO items VALUES (?, ?)", (2, "b"))
    return client


@pytest.mark.anyio
async def test_async_single_call_results_equal_sync() -> None:
    """Return results identical to SyncSQLiteClient across all single-call queries and DML shapes."""
    async_client = await _seeded_async_single_call_client()
    sync_client = _seeded_single_call_client()
    try:
        select_sql = "SELECT id, label FROM items ORDER BY id"
        assert await async_client.fetch_rows(select_sql) == sync_client.fetch_rows(select_sql)
        assert await async_client.fetch_dicts(select_sql) == sync_client.fetch_dicts(select_sql)
        assert await async_client.fetch_dicts("SELECT 1 AS a, 2 AS a") == sync_client.fetch_dicts(
            "SELECT 1 AS a, 2 AS a"
        )

        empty_sql = "SELECT id, label FROM items WHERE 1 = 0"
        assert await async_client.fetch_rows(empty_sql) == sync_client.fetch_rows(empty_sql)
        assert await async_client.fetch_dicts(empty_sql) == sync_client.fetch_dicts(empty_sql)
        with pytest.raises(NotFoundError):
            await async_client.fetch_value(empty_sql)

        assert await async_client.fetch_rows("CREATE TABLE other_rows (x INTEGER)") == sync_client.fetch_rows(
            "CREATE TABLE other_rows (x INTEGER)"
        )
        assert await async_client.fetch_dicts("CREATE TABLE other_dicts (x INTEGER)") == sync_client.fetch_dicts(
            "CREATE TABLE other_dicts (x INTEGER)"
        )
        with pytest.raises(NotFoundError):
            await async_client.fetch_value("CREATE TABLE other_value (x INTEGER)")
        with pytest.raises(NotFoundError):
            sync_client.fetch_value("CREATE TABLE other_value (x INTEGER)")
        ddl_check = "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name LIKE 'other_%'"
        assert await async_client.fetch_value(ddl_check) == sync_client.fetch_value(ddl_check) == 3

        assert await async_client.fetch_value("SELECT count(*) FROM items") == sync_client.fetch_value(
            "SELECT count(*) FROM items"
        )
        assert await async_client.fetch_value("SELECT NULL") is None
        assert sync_client.fetch_value("SELECT NULL") is None

        assert await async_client.execute_dml(
            "INSERT INTO items (label) VALUES (?)", ("c",)
        ) == sync_client.execute_dml("INSERT INTO items (label) VALUES (?)", ("c",))
        assert await async_client.execute_dml(
            "UPDATE items SET label = ? WHERE id <= ?", ("z", 2)
        ) == sync_client.execute_dml("UPDATE items SET label = ? WHERE id <= ?", ("z", 2))
        assert await async_client.execute_dml(
            "UPDATE items SET label = ? WHERE id = ?", ("miss", 999)
        ) == sync_client.execute_dml("UPDATE items SET label = ? WHERE id = ?", ("miss", 999))
        assert await async_client.execute_dml("SELECT id FROM items") == sync_client.execute_dml("SELECT id FROM items")

        ret_sql = "INSERT INTO items (label) VALUES (?) RETURNING id"
        assert await async_client.fetch_rows(ret_sql, ("d",)) == sync_client.fetch_rows(ret_sql, ("d",))
        assert await async_client.fetch_dicts(ret_sql, ("e",)) == sync_client.fetch_dicts(ret_sql, ("e",))
        assert await async_client.execute_dml(ret_sql, ("f",)) == sync_client.execute_dml(ret_sql, ("f",))
    finally:
        sync_client.close()
        await async_client.close()


@pytest.mark.anyio
async def test_async_single_call_one_awaitable() -> None:
    """Return one awaitable from fetch_rows whose awaited value matches the expected rows and names."""
    client = AsyncSQLiteClient(":memory:")
    try:
        aw = client.fetch_rows("SELECT 1 AS a")
        assert await aw == ([(1,)], ("a",))
    finally:
        await client.close()


@pytest.mark.anyio
@pytest.mark.parametrize("method_name", ["fetch_rows", "fetch_dicts", "fetch_value", "execute_dml"])
async def test_async_single_call_error_timing(method_name: str) -> None:
    """Raise bind errors at the call and engine or NotFound errors on await."""
    client = await _seeded_async_single_call_client()
    try:
        with pytest.raises(InterfaceError):
            getattr(client, method_name)("SELECT ?", (object(),))

        syntax_aw = getattr(client, method_name)("SELEC 1")
        with pytest.raises(ProgrammingError):
            await syntax_aw

        constraint_aw = getattr(client, method_name)("INSERT INTO items VALUES (?, ?)", (1, "dup"))
        with pytest.raises(IntegrityError):
            await constraint_aw

        if method_name == "fetch_value":
            missing_aw = client.fetch_value("SELECT 1 WHERE 0")
            with pytest.raises(NotFoundError):
                await missing_aw
    finally:
        await client.close()


@pytest.mark.anyio
async def test_async_last_statement_rowid_lifecycle_and_isolation() -> None:
    """Update last_statement_rowid on completion, isolate it from sync/cursor calls, and preserve on failure."""
    client = await _seeded_async_cursor_client()
    try:
        assert client.last_statement_rowid is None

        sync_peer = _seeded_cursor_client()
        try:
            sync_peer.execute_dml("INSERT INTO items (label) VALUES (?)", ("from_sync",))
            assert sync_peer.last_statement_rowid == 1
            async_view = sync_peer.to_async()
            assert async_view.last_statement_rowid is None
        finally:
            sync_peer.close()

        changes, first_rowid = await client.execute_dml("INSERT INTO items (label) VALUES (?)", ("first",))
        assert changes == 1
        assert first_rowid == 1
        assert client.last_statement_rowid == first_rowid

        assert await client.fetch_rows("SELECT id FROM items") == ([(1,)], ("id",))
        assert client.last_statement_rowid is None

        rows, names = await client.fetch_rows("INSERT INTO items (label) VALUES (?) RETURNING id", ("second",))
        assert rows == [(2,)]
        assert names == ("id",)
        assert client.last_statement_rowid == 2

        with pytest.raises(ProgrammingError):
            await client.fetch_rows("SELEC 1")
        assert client.last_statement_rowid == 2

        with pytest.raises(InterfaceError):
            client.execute_dml("INSERT INTO items (label) VALUES (?)", (object(),))
        assert client.last_statement_rowid == 2

        cursor = client.cursor()
        await cursor.execute("INSERT INTO items (label) VALUES (?)", ("via_cursor",))
        assert cursor.lastrowid == 3
        assert client.last_statement_rowid == 2

        await client.execute("INSERT INTO items (label) VALUES (?)", ("via_execute",))
        assert client.last_statement_rowid == 2
    finally:
        await client.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("method_name", "expected"),
    [
        pytest.param("fetchall", [(1,), (2,)], id="fetchall"),
        pytest.param("fetchone", (1,), id="fetchone"),
        pytest.param("fetchmany", [(1,)], id="fetchmany"),
        pytest.param("close", None, id="close"),
    ],
)
async def test_async_cursor_ready_awaitable_does_not_iterate_loop(method_name: str, expected: Any) -> None:
    """Resolve async cursor fetch and close methods via ReadyAwaitable without yielding to the event loop."""
    client = await _seeded_async_single_call_client()
    try:
        cursor = client.cursor()
        await cursor.execute("SELECT id FROM items ORDER BY id")
        ran: list[int] = []
        loop = asyncio.get_running_loop()
        loop.call_soon(ran.append, 1)

        aw = getattr(cursor, method_name)()
        assert type(aw).__name__ == "ReadyAwaitable"
        assert not isinstance(aw, ReadyAwaitable)
        result = await aw

        assert result == expected
        assert ran == []
        await asyncio.sleep(0)
        assert ran == [1]
    finally:
        await client.close()


def test_async_cursor_ready_awaitable_no_loop_reuse_and_create_task_rejected() -> None:
    """Allow creating ReadyAwaitable without a running loop, re-awaiting it, and reject create_task."""
    client = AsyncSQLiteClient(":memory:")
    try:
        cursor = client.cursor()
        aw_no_loop = cursor.fetchall()
        assert type(aw_no_loop).__name__ == "ReadyAwaitable"

        async def _verify() -> None:
            first = await aw_no_loop
            second = await aw_no_loop
            assert first == []
            assert first is second

            await cursor.execute("SELECT 1 UNION ALL SELECT 2")
            rows_aw = cursor.fetchall()
            rows_first = await rows_aw
            rows_second = await rows_aw
            assert rows_first == [(1,), (2,)]
            assert rows_first is rows_second

            bad_coro: Any = cursor.fetchall()
            with pytest.raises(TypeError):
                asyncio.create_task(bad_coro)

        asyncio.run(_verify())
    finally:
        client.to_sync().close()


@pytest.mark.anyio
async def test_async_single_call_cancel_recovers() -> None:
    """Cancel an in-flight single-call query, keep last_statement_rowid intact, and serve subsequent queries."""
    client = await _seeded_async_cursor_client()
    try:
        assert await client.execute_dml("INSERT INTO items (label) VALUES (?)", ("before_cancel",)) == (1, 1)
        assert client.last_statement_rowid == 1

        endless = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c"
        task = asyncio.ensure_future(client.fetch_rows(endless))
        await asyncio.sleep(0.02)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert client.last_statement_rowid == 1
        assert await client.fetch_value("SELECT 1") == 1
    finally:
        await client.close()


class _CountingClient:
    """Forward to a real client and record the name of each method call."""

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.calls: list[str] = []

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self.inner, name)
        if not callable(attr):
            return attr

        def counted(*args: Any, **kwargs: Any) -> Any:
            self.calls.append(name)
            return attr(*args, **kwargs)

        return counted


class _ItemSchema(msgspec.Struct, frozen=True, gc=False):
    """Typed row schema for SQLite single-call hydration tests."""

    id: int
    label: str


def test_sync_sqlite_driver_single_call_dispatch_and_hydration() -> None:
    """Each SyncSQLiteDriver query method dispatches through one bound native client call."""
    counter = _CountingClient(SyncSQLiteClient(":memory:"))
    driver = SyncSQLiteDriver(_client=cast("SyncSQLiteClient", counter))
    try:
        counter.calls.clear()
        assert driver.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)") == 0
        assert counter.calls == ["execute_dml"]

        counter.calls.clear()
        assert driver.execute_dml("INSERT INTO items (label) VALUES (?)", ("alpha",)) == (1, 1)
        assert counter.calls == ["execute_dml"]

        counter.calls.clear()
        assert driver.select("SELECT id, label FROM items ORDER BY id") == [{"id": 1, "label": "alpha"}]
        assert counter.calls == ["fetch_dicts"]

        counter.calls.clear()
        assert driver.select_one("SELECT id, label FROM items WHERE id = ?", (1,)) == {"id": 1, "label": "alpha"}
        assert counter.calls == ["fetch_dicts"]

        counter.calls.clear()
        assert driver.select_one_or_none("SELECT id, label FROM items WHERE id = ?", (1,)) == {
            "id": 1,
            "label": "alpha",
        }
        assert counter.calls == ["fetch_dicts"]

        counter.calls.clear()
        assert driver.select_value("SELECT label FROM items WHERE id = ?", (1,)) == "alpha"
        assert counter.calls == ["fetch_value"]

        counter.calls.clear()
        assert driver.execute_with_description("SELECT id, label FROM items ORDER BY id") == (
            [(1, "alpha")],
            ("id", "label"),
        )
        assert counter.calls == ["fetch_rows"]

        counter.calls.clear()
        assert driver.execute_tuples("SELECT id, label FROM items ORDER BY id") == [(1, "alpha")]
        assert counter.calls == ["fetch_rows"]

        counter.calls.clear()
        assert driver.select("SELECT 1 AS a", schema_type=dict) == [{"a": 1}]
        assert counter.calls == ["fetch_dicts"]

        counter.calls.clear()
        assert driver.select("SELECT id, label FROM items ORDER BY id", schema_type=_ItemSchema) == [
            _ItemSchema(id=1, label="alpha"),
        ]
        assert counter.calls == ["fetch_rows"]

        counter.calls.clear()
        assert driver.select_one("SELECT id, label FROM items WHERE id = ?", (1,), schema_type=_ItemSchema) == (
            _ItemSchema(id=1, label="alpha")
        )
        assert counter.calls == ["fetch_rows"]

        counter.calls.clear()
        assert driver.select_one_or_none(
            "SELECT id, label FROM items WHERE id = ?",
            (1,),
            schema_type=_ItemSchema,
        ) == _ItemSchema(id=1, label="alpha")
        assert counter.calls == ["fetch_rows"]
    finally:
        driver.close()


@pytest.mark.anyio
async def test_async_sqlite_driver_single_call_dispatch_and_hydration() -> None:
    """Each AsyncSQLiteDriver query method dispatches through one bound native client call."""
    counter = _CountingClient(AsyncSQLiteClient(":memory:"))
    driver = AsyncSQLiteDriver(_client=cast("AsyncSQLiteClient", counter))
    try:
        counter.calls.clear()
        assert await driver.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)") == 0
        assert counter.calls == ["execute_dml"]

        counter.calls.clear()
        assert await driver.execute_dml("INSERT INTO items (label) VALUES (?)", ("alpha",)) == (1, 1)
        assert counter.calls == ["execute_dml"]

        counter.calls.clear()
        assert await driver.select("SELECT id, label FROM items ORDER BY id") == [{"id": 1, "label": "alpha"}]
        assert counter.calls == ["fetch_dicts"]

        counter.calls.clear()
        assert await driver.select_one("SELECT id, label FROM items WHERE id = ?", (1,)) == {
            "id": 1,
            "label": "alpha",
        }
        assert counter.calls == ["fetch_dicts"]

        counter.calls.clear()
        assert await driver.select_one_or_none("SELECT id, label FROM items WHERE id = ?", (1,)) == {
            "id": 1,
            "label": "alpha",
        }
        assert counter.calls == ["fetch_dicts"]

        counter.calls.clear()
        assert await driver.select_value("SELECT label FROM items WHERE id = ?", (1,)) == "alpha"
        assert counter.calls == ["fetch_value"]

        counter.calls.clear()
        assert await driver.execute_with_description("SELECT id, label FROM items ORDER BY id") == (
            [(1, "alpha")],
            ("id", "label"),
        )
        assert counter.calls == ["fetch_rows"]

        counter.calls.clear()
        assert await driver.execute_tuples("SELECT id, label FROM items ORDER BY id") == [(1, "alpha")]
        assert counter.calls == ["fetch_rows"]

        counter.calls.clear()
        assert await driver.select("SELECT 1 AS a", schema_type=dict) == [{"a": 1}]
        assert counter.calls == ["fetch_dicts"]

        counter.calls.clear()
        assert await driver.select("SELECT id, label FROM items ORDER BY id", schema_type=_ItemSchema) == [
            _ItemSchema(id=1, label="alpha"),
        ]
        assert counter.calls == ["fetch_rows"]

        counter.calls.clear()
        assert await driver.select_one("SELECT id, label FROM items WHERE id = ?", (1,), schema_type=_ItemSchema) == (
            _ItemSchema(id=1, label="alpha")
        )
        assert counter.calls == ["fetch_rows"]

        counter.calls.clear()
        assert await driver.select_one_or_none(
            "SELECT id, label FROM items WHERE id = ?",
            (1,),
            schema_type=_ItemSchema,
        ) == _ItemSchema(id=1, label="alpha")
        assert counter.calls == ["fetch_rows"]
    finally:
        await driver.close()


def test_sync_sqlite_driver_value_type_not_found_and_lastrowid() -> None:
    """Sync driver coerces select_value, raises NotFoundError on empty sets, and reads lastrowid from the client."""
    driver = SyncSQLiteDriver()
    try:
        assert driver.lastrowid is None
        assert driver.select("SELECT 1 AS a", schema_type=dict) == [{"a": 1}]
        assert driver.select_value("SELECT 1", value_type=str) == "1"

        with pytest.raises(NotFoundError, match="Query returned no rows"):
            driver.select_one("SELECT 1 WHERE 1 = 0")
        with pytest.raises(NotFoundError, match="Query returned no rows"):
            driver.select_value("SELECT 1 WHERE 1 = 0")
        assert driver.select_one_or_none("SELECT 1 WHERE 1 = 0") is None

        assert driver.select("CREATE TABLE ddl_empty_a (id INTEGER)") == []
        assert driver.execute_with_description("CREATE TABLE ddl_empty_b (id INTEGER)") == ([], ())
        with pytest.raises(NotFoundError, match="Query returned no rows"):
            driver.select_one("CREATE TABLE ddl_empty_c (id INTEGER)")
        with pytest.raises(NotFoundError, match="Query returned no rows"):
            driver.select_value("CREATE TABLE ddl_empty_d (id INTEGER)")
        assert driver.select_one_or_none("CREATE TABLE ddl_empty_e (id INTEGER)") is None

        driver.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        assert driver.execute("INSERT INTO items (label) VALUES (?)", ("first",)) == 1
        assert driver.lastrowid == 1
        assert driver.execute_many("INSERT INTO items (label) VALUES (?)", [("second",), ("third",)]) == 2
        assert driver.lastrowid == 1
        driver.cursor().execute("INSERT INTO items (label) VALUES (?)", ("via_cursor",))
        assert driver.lastrowid == 1
        with pytest.raises(ProgrammingError):
            driver.execute("SELEC 1")
        assert driver.lastrowid == 1

        driver.close()
        assert driver.lastrowid is None
        assert driver.connect() is driver
        assert driver.select_value("SELECT 1") == 1
        assert driver.lastrowid is None
    finally:
        driver.close()


@pytest.mark.anyio
async def test_async_sqlite_driver_value_type_not_found_and_lastrowid() -> None:
    """Async driver coerces select_value, raises NotFoundError on empty sets, and reads lastrowid from the client."""
    driver = AsyncSQLiteDriver()
    try:
        assert driver.lastrowid is None
        assert await driver.select("SELECT 1 AS a", schema_type=dict) == [{"a": 1}]
        assert await driver.select_value("SELECT 1", value_type=str) == "1"

        with pytest.raises(NotFoundError, match="Query returned no rows"):
            await driver.select_one("SELECT 1 WHERE 1 = 0")
        with pytest.raises(NotFoundError, match="Query returned no rows"):
            await driver.select_value("SELECT 1 WHERE 1 = 0")
        assert await driver.select_one_or_none("SELECT 1 WHERE 1 = 0") is None

        assert await driver.select("CREATE TABLE ddl_empty_a (id INTEGER)") == []
        assert await driver.execute_with_description("CREATE TABLE ddl_empty_b (id INTEGER)") == ([], ())
        with pytest.raises(NotFoundError, match="Query returned no rows"):
            await driver.select_one("CREATE TABLE ddl_empty_c (id INTEGER)")
        with pytest.raises(NotFoundError, match="Query returned no rows"):
            await driver.select_value("CREATE TABLE ddl_empty_d (id INTEGER)")
        assert await driver.select_one_or_none("CREATE TABLE ddl_empty_e (id INTEGER)") is None

        await driver.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        assert await driver.execute("INSERT INTO items (label) VALUES (?)", ("first",)) == 1
        assert driver.lastrowid == 1
        assert await driver.execute_many("INSERT INTO items (label) VALUES (?)", [("second",), ("third",)]) == 2
        assert driver.lastrowid == 1
        await driver.cursor().execute("INSERT INTO items (label) VALUES (?)", ("via_cursor",))
        assert driver.lastrowid == 1
        with pytest.raises(ProgrammingError):
            await driver.execute("SELEC 1")
        assert driver.lastrowid == 1

        await driver.close()
        assert driver.lastrowid is None
        assert await driver.connect() is driver
        assert await driver.select_value("SELECT 1") == 1
        assert driver.lastrowid is None
    finally:
        await driver.close()


@pytest.mark.parametrize(
    ("method_name", "sql"),
    [
        pytest.param("select", "SELECT 1", id="select"),
        pytest.param("select_one", "SELECT 1", id="select_one"),
        pytest.param("select_one_or_none", "SELECT 1", id="select_one_or_none"),
        pytest.param("select_value", "SELECT 1", id="select_value"),
        pytest.param("execute_with_description", "SELECT 1", id="execute_with_description"),
        pytest.param("execute_tuples", "SELECT 1", id="execute_tuples"),
        pytest.param("execute", "SELECT 1", id="execute"),
        pytest.param("execute_dml", "SELECT 1", id="execute_dml"),
    ],
)
def test_sync_sqlite_driver_closed_query_methods_raise_interface_error(method_name: str, sql: str) -> None:
    """Every query method on a closed SyncSQLiteDriver raises InterfaceError through the closed binding."""
    driver = SyncSQLiteDriver()
    driver.close()
    with pytest.raises(InterfaceError, match="SQLite driver is closed"):
        getattr(driver, method_name)(sql)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("method_name", "sql"),
    [
        pytest.param("select", "SELECT 1", id="select"),
        pytest.param("select_one", "SELECT 1", id="select_one"),
        pytest.param("select_one_or_none", "SELECT 1", id="select_one_or_none"),
        pytest.param("select_value", "SELECT 1", id="select_value"),
        pytest.param("execute_with_description", "SELECT 1", id="execute_with_description"),
        pytest.param("execute_tuples", "SELECT 1", id="execute_tuples"),
        pytest.param("execute", "SELECT 1", id="execute"),
        pytest.param("execute_dml", "SELECT 1", id="execute_dml"),
    ],
)
async def test_async_sqlite_driver_closed_query_methods_raise_interface_error(method_name: str, sql: str) -> None:
    """Every query method on a closed AsyncSQLiteDriver raises InterfaceError when awaited."""
    driver = AsyncSQLiteDriver()
    await driver.close()
    coro = getattr(driver, method_name)(sql)
    with pytest.raises(InterfaceError, match="SQLite driver is closed"):
        await coro


@pytest.mark.parametrize("driver_cls", [SyncSQLiteDriver, AsyncSQLiteDriver], ids=["sync", "async"])
def test_sqlite_driver_slots_drop_lastrowid(driver_cls: type[Any]) -> None:
    """Neither SQLite driver retains a private _lastrowid attribute."""
    driver = driver_cls()
    try:
        assert not hasattr(driver, "_lastrowid")
    finally:
        if isinstance(driver, SyncSQLiteDriver):
            driver.close()
        else:
            asyncio.run(driver.close())


@pytest.mark.anyio
async def test_sync_and_async_cursor_lazy_begin_skips_read_only_select() -> None:
    """Verify lazy_begin cursors leave in_transaction False on read-only SELECTs including audit columns."""
    sync_driver = SyncSQLiteDriver()
    try:
        sync_driver.execute(
            "CREATE TABLE audit_items (id INTEGER PRIMARY KEY, created_at TEXT, updated_at TEXT, deleted_at TEXT)"
        )
        sync_driver.execute("INSERT INTO audit_items VALUES (1, '2026-01-01', '2026-01-02', NULL)")
        sync_cur = sync_driver.cursor(lazy_begin=True)
        sync_cur.execute("SELECT id, created_at, updated_at, deleted_at FROM audit_items WHERE id = ?", (1,))
        assert sync_cur.fetchall() == [(1, "2026-01-01", "2026-01-02", None)]
        assert sync_driver.in_transaction is False
        sync_cur.execute("/* trace */ SELECT id FROM audit_items WHERE id = ?", (1,))
        assert sync_cur.fetchall() == [(1,)]
        assert sync_driver.in_transaction is False
        sync_driver.begin()
        sync_cur.execute("SELECT id FROM audit_items WHERE id = ?", (1,))
        assert sync_cur.fetchall() == [(1,)]
        assert sync_driver.in_transaction is True
        sync_driver.rollback()
        assert sync_driver.in_transaction is False
    finally:
        sync_driver.close()

    async_driver = AsyncSQLiteDriver()
    try:
        await async_driver.execute(
            "CREATE TABLE audit_items (id INTEGER PRIMARY KEY, created_at TEXT, updated_at TEXT, deleted_at TEXT)"
        )
        await async_driver.execute("INSERT INTO audit_items VALUES (1, '2026-01-01', '2026-01-02', NULL)")
        async_cur = async_driver.cursor(lazy_begin=True)
        await async_cur.execute("SELECT id, created_at, updated_at, deleted_at FROM audit_items WHERE id = ?", (1,))
        assert await async_cur.fetchall() == [(1, "2026-01-01", "2026-01-02", None)]
        assert async_driver.in_transaction is False
        await async_cur.execute("/* trace */ SELECT id FROM audit_items WHERE id = ?", (1,))
        assert await async_cur.fetchall() == [(1,)]
        assert async_driver.in_transaction is False
    finally:
        await async_driver.close()


@pytest.mark.anyio
async def test_lazy_begin_read_then_write_opens_rollbackable_tx() -> None:
    """Verify SELECT followed by DML or WITH REPLACE on lazy_begin cursor opens a rollbackable transaction."""
    driver = AsyncSQLiteDriver()
    try:
        await driver.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, val INTEGER)")
        await driver.execute("INSERT INTO items VALUES (1, 10)")
        cur = driver.cursor(lazy_begin=True)
        await cur.execute("SELECT val FROM items WHERE id = ?", (1,))
        assert await cur.fetchall() == [(10,)]
        assert driver.in_transaction is False

        await cur.execute("UPDATE items SET val = ? WHERE id = ?", (20, 1))
        assert driver.in_transaction is True
        await driver.rollback()
        assert driver.in_transaction is False
        assert await driver.execute_tuples("SELECT val FROM items WHERE id = ?", (1,)) == [(10,)]

        await cur.execute("WITH src AS (SELECT 1 AS id, 30 AS val) REPLACE INTO items SELECT id, val FROM src")
        assert driver.in_transaction is True
        await driver.rollback()
        assert driver.in_transaction is False
        assert await driver.execute_tuples("SELECT val FROM items WHERE id = ?", (1,)) == [(10,)]
    finally:
        await driver.close()
