"""SQLite statement completion, affected counts, and transaction behavior."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from vertebra._vertebra import DatabaseError, LeaseBroker
from vertebra.db.adapters import sqlite as sqlite_pkg
from vertebra.db.adapters.sqlite import AsyncSQLiteDriver, SQLiteConfig, SyncSQLiteDriver
from vertebra.db.adapters.sqlite import _async as sqlite_async_mod
from vertebra.db.adapters.sqlite import _sync as sqlite_sync_mod
from vertebra.db.adapters.sqlite import driver as sqlite_driver_mod
from vertebra.db.driver._common import SQLITE_DIALECT_TAG


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


def test_sync_sqlite_driver_uses_common_mixin_broker_and_releases_on_error(tmp_path: Path) -> None:
    """SyncSQLiteDriver routes through CommonDriverMixin bytes StatementCache and releases leases on error."""
    broker = LeaseBroker(pool_size=2, max_overflow=0, stmt_capacity=16)
    driver = SyncSQLiteDriver(SQLiteConfig(path=str(tmp_path / "broker_sync.db")), broker=broker)
    assert driver.broker is broker
    try:
        driver.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        insert_sql = "INSERT INTO items (id, label) VALUES (?, ?)"
        assert driver.execute(insert_sql, (1, "first")) == 1
        assert driver.execute(insert_sql, (2, "second")) == 1

        sc = broker.statement_cache()
        assert sc is not None
        assert sc.stats().hits >= 1
        plan = sc.lookup(SQLITE_DIALECT_TAG, insert_sql.encode("utf-8"))
        assert plan is not None
        assert plan.param_count == 2
        assert plan.compiled_sql == insert_sql.encode("utf-8")

        with pytest.raises(DatabaseError):
            driver.execute_tuples("SELECT * FROM nonexistent_sqlite_table")
        broker_stats = broker.stats()
        assert broker_stats.active_leases == 0
        assert broker_stats.total_acquired == 4
        assert broker_stats.total_released == 4
    finally:
        driver.close()


@pytest.mark.anyio
async def test_async_sqlite_driver_uses_common_mixin_broker_and_releases_on_error(tmp_path: Path) -> None:
    """AsyncSQLiteDriver routes through CommonDriverMixin bytes StatementCache and releases leases on error."""
    broker = LeaseBroker(pool_size=2, max_overflow=0, stmt_capacity=16)
    driver = AsyncSQLiteDriver(SQLiteConfig(path=str(tmp_path / "broker_async.db")), broker=broker)
    assert driver.broker is broker
    try:
        await driver.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        insert_sql = "INSERT INTO items (id, label) VALUES (?, ?)"
        assert await driver.execute(insert_sql, (1, "first")) == 1
        assert await driver.execute(insert_sql, (2, "second")) == 1

        sc = broker.statement_cache()
        assert sc is not None
        assert sc.stats().hits >= 1
        plan = sc.lookup(SQLITE_DIALECT_TAG, insert_sql.encode("utf-8"))
        assert plan is not None
        assert plan.param_count == 2
        assert plan.compiled_sql == insert_sql.encode("utf-8")

        with pytest.raises(DatabaseError):
            await driver.execute_tuples("SELECT * FROM nonexistent_sqlite_table")
        broker_stats = broker.stats()
        assert broker_stats.active_leases == 0
        assert broker_stats.total_acquired == 4
        assert broker_stats.total_released == 4
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
