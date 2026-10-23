"""Unit tests for SyncPostgresDriver, AsyncPostgresDriver, and Arrow binary COPY streaming."""

from __future__ import annotations

import struct
from collections.abc import Sequence
from typing import Any

import pytest

from vertebra.db import AlloyDbDialer, CloudSqlDialer, DatabaseRegistry
from vertebra.db.postgres import (
    PGCOPY_BINARY_SIGNATURE,
    AsyncPostgresDriver,
    PostgresConfig,
    SyncPostgresDriver,
)
from vertebra.typing import MissingDependencyError


def _build_test_pgcopy_payload(
    rows: Sequence[Sequence[bytes | None]],
    flags: int = 0,
    ext_bytes: bytes = b"",
) -> bytes:
    """Build a valid PostgreSQL binary COPY stream payload for test validation."""
    buf = bytearray(PGCOPY_BINARY_SIGNATURE)
    buf.extend(struct.pack(">II", flags, len(ext_bytes)))
    buf.extend(ext_bytes)
    for row in rows:
        buf.extend(struct.pack(">h", len(row)))
        for field in row:
            if field is None:
                buf.extend(struct.pack(">i", -1))
            else:
                buf.extend(struct.pack(">i", len(field)))
                buf.extend(field)
    buf.extend(struct.pack(">h", -1))
    return bytes(buf)


class MockCursor:
    """Mock database cursor recording queries and returning configured rows."""

    __slots__ = ("_rows", "description")

    def __init__(self, rows: list[tuple[Any, ...]] | None = None) -> None:
        self._rows = rows or []
        self.description = (
            ("id", 23, None, None, None, None, None),
            ("name", 1043, None, None, None, None, None),
        )

    def execute(self, sql: str, parameters: Any = ()) -> None:
        """Record execution of SQL."""
        _ = (sql, parameters)

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Return buffered rows."""
        return list(self._rows)

    def __enter__(self) -> MockCursor:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass


class MockConnection:
    """Mock database connection providing context management and transaction hooks."""

    __slots__ = ("closed", "committed", "cursor_instance", "rolled_back")

    def __init__(self, cursor_instance: MockCursor | None = None) -> None:
        self.cursor_instance = cursor_instance or MockCursor()
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self) -> MockCursor:
        """Return the mock cursor."""
        return self.cursor_instance

    def __enter__(self) -> MockConnection:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass


class MockConnectionPool:
    """Mock connection pool providing sync connection management and copy streaming."""

    __slots__ = ("_binary_payload", "_conn", "closed")

    def __init__(
        self,
        rows: list[tuple[Any, ...]] | None = None,
        binary_payload: bytes | None = None,
    ) -> None:
        self._conn = MockConnection(MockCursor(rows))
        self._binary_payload = binary_payload
        self.closed = False

    def connection(self) -> MockConnection:
        """Return the mock connection context."""
        return self._conn

    def copy_binary_stream(self, sql: str) -> bytes:
        """Return binary COPY stream payload."""
        _ = sql
        return self._binary_payload or b""

    def commit(self) -> None:
        """Record transaction commit."""
        self._conn.committed = True

    def rollback(self) -> None:
        """Record transaction rollback."""
        self._conn.rolled_back = True

    def close(self) -> None:
        """Close the mock pool."""
        self.closed = True


def test_sync_postgres_driver_pool_gucs_and_prepared_statements() -> None:
    """Verify worker pool budget calculation and sync driver pool execution."""
    budget = PostgresConfig.compute_worker_pool_budget(
        100,
        replica_count=3,
        workers_per_replica=3,
        reserved_admin_connections=10,
    )
    assert budget == 10

    with pytest.raises(TypeError):
        PostgresConfig.compute_worker_pool_budget(
            100,
            replica_count=3,
            workers_per_replica=3,
            reserved_super_connections=10,
        )

    cfg = PostgresConfig(
        dsn="postgresql://user:pass@127.0.0.1:5432/appdb",
        statement_timeout_ms=5000,
        idle_in_transaction_session_timeout_ms=10000,
        session_gucs={"work_mem": "64MB"},
    )
    gucs = cfg.build_session_gucs()
    assert gucs["statement_timeout"] == "5000"
    assert gucs["idle_in_transaction_session_timeout"] == "10000"
    assert gucs["work_mem"] == "64MB"
    assert gucs["application_name"] == "vertebra"
    assert cfg.effective_statement_cache_size == 256

    mock_pool = MockConnectionPool(rows=[(1, "alpha"), (2, "beta")])
    driver = SyncPostgresDriver(cfg, pool=mock_pool)
    rows, desc = driver.execute_with_description("SELECT id, name FROM items WHERE id > %s", (0,))
    assert rows == [(1, "alpha"), (2, "beta")]
    assert desc is not None
    assert desc[0][0] == "id"
    assert desc[1][0] == "name"

    driver.commit()
    assert mock_pool._conn.committed is True
    driver.rollback()
    assert mock_pool._conn.rolled_back is True
    driver.close()
    assert mock_pool.closed is True
    assert driver.closed is True


def test_postgres_missing_dependency_raises_on_unconnected_dsn() -> None:
    """Verify unmocked live connection attempts raise MissingDependencyError when psycopg is absent."""
    cfg = PostgresConfig(dsn="postgresql://unreachable-host:5432/db")
    driver = SyncPostgresDriver(cfg)
    with pytest.raises(MissingDependencyError) as exc_info:
        driver.execute_tuples("SELECT 1")
    assert "psycopg" in exc_info.value.package or "psycopg_pool" in exc_info.value.package


def test_pgcopy_binary_parser_success_and_error_handling() -> None:
    """Verify parsing valid and malformed PGCOPY binary stream framing."""
    valid_rows: list[list[bytes | None]] = [
        [b"first", None],
        [b"second", b"val2"],
    ]
    payload = _build_test_pgcopy_payload(valid_rows)
    parsed = SyncPostgresDriver.parse_pgcopy_binary_rows(payload)
    assert parsed == [(b"first", None), (b"second", b"val2")]

    empty_payload = _build_test_pgcopy_payload([])
    assert SyncPostgresDriver.parse_pgcopy_binary_rows(empty_payload) == []

    with pytest.raises(ValueError, match="Invalid PGCOPY binary signature"):
        SyncPostgresDriver.parse_pgcopy_binary_rows(b"BAD_SIGNATURE_EXTRA")

    with pytest.raises(ValueError, match="Truncated PGCOPY header"):
        SyncPostgresDriver.parse_pgcopy_binary_rows(PGCOPY_BINARY_SIGNATURE + b"\x00\x00")

    with pytest.raises(ValueError, match="Unexpected EOF before PGCOPY trailer"):
        bad_trailer = _build_test_pgcopy_payload([[b"v"]]).rstrip(b"\xff\xff")
        SyncPostgresDriver.parse_pgcopy_binary_rows(bad_trailer)


@pytest.mark.anyio
async def test_async_postgres_driver_and_arrow_copy_streaming() -> None:
    """Verify AsyncPostgresDriver and stream_arrow_batches chunking."""
    rows_data: list[list[bytes | None]] = [
        [b"key1", b"val1"],
        [b"key2", b"val2"],
        [b"key3", b"val3"],
    ]
    binary_payload = _build_test_pgcopy_payload(rows_data)
    mock_pool = MockConnectionPool(
        rows=[(1, "x"), (2, "y")],
        binary_payload=binary_payload,
    )
    async_driver = AsyncPostgresDriver(PostgresConfig(), pool=mock_pool)

    tuples = await async_driver.execute_tuples("SELECT id, name FROM tbl")
    assert tuples == [(1, "x"), (2, "y")]

    batches = list(
        await async_driver.stream_arrow_batches(
            "SELECT k, v FROM kv",
            columns=["k", "v"],
            batch_size=2,
        )
    )
    assert len(batches) == 2
    assert len(batches[0]) == 2
    assert len(batches[1]) == 1

    await async_driver.commit()
    await async_driver.rollback()
    await async_driver.close()
    assert async_driver.closed is True


def test_database_registry_attaches_cloudsql_and_alloydb_dialers_to_postgres_drivers() -> None:
    """Verify DatabaseRegistry attaches CloudSqlDialer and AlloyDbDialer to sync and async drivers."""
    registry = DatabaseRegistry()
    registry.register(
        "csql_pg",
        PostgresConfig(
            dsn="postgresql://app@127.0.0.1:5432/db",
            cloud_sql_instance="proj:us-central1:pg1",
            enable_iam_auth=True,
        ),
    )
    registry.register(
        "alloy_pg",
        PostgresConfig(
            dsn="postgresql://app@127.0.0.1:5432/db",
            alloydb_instance_uri="projects/p/locations/l/clusters/c/instances/i",
        ),
    )

    csql_async = registry.get_driver("csql_pg")
    assert isinstance(csql_async, AsyncPostgresDriver)
    assert isinstance(csql_async.dialer, CloudSqlDialer)

    csql_sync = registry.get_sync_driver("csql_pg")
    assert isinstance(csql_sync, SyncPostgresDriver)
    assert isinstance(csql_sync.dialer, CloudSqlDialer)

    alloy_async = registry.get_driver("alloy_pg")
    assert isinstance(alloy_async, AsyncPostgresDriver)
    assert isinstance(alloy_async.dialer, AlloyDbDialer)
    assert alloy_async.config.build_session_gucs().get("alloydb.enable_columnar_engine") == "on"

    alloy_sync = registry.get_sync_driver("alloy_pg")
    assert isinstance(alloy_sync, SyncPostgresDriver)
    assert isinstance(alloy_sync.dialer, AlloyDbDialer)
    assert alloy_sync.config.build_session_gucs().get("alloydb.enable_columnar_engine") == "on"
