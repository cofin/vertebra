"""SQLite statement completion, affected counts, and transaction behavior."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from vertebra.db.adapters.sqlite import AsyncSQLiteDriver, SQLiteConfig, SyncSQLiteDriver


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
