"""Unit tests for zero-copy Arrow selection, streaming, and binary COPY ingestion."""

from __future__ import annotations

import contextlib

import pyarrow as pa
import pytest

from tests.unit.db.postgres_ports import (
    FakeAsyncPostgresPool,
    FakePostgresCluster,
    FakeSyncPostgresPool,
)
from vertebra.db.adapters.postgres._async import AsyncPostgreSQLDriver
from vertebra.db.adapters.postgres._sync import SyncPostgreSQLDriver
from vertebra.db.adapters.postgres.config import PostgreSQLConfig
from vertebra.db.arrow import ArrowCursor


@pytest.fixture
def anyio_backend() -> str:
    """Restrict async Arrow tests cleanly to asyncio."""
    return "asyncio"


def _seed_metrics_table(cluster: FakePostgresCluster, rows: int = 10) -> None:
    """Create and populate a numeric metrics table in the simulated cluster."""
    with cluster._lock:
        cluster._conn.execute("CREATE TABLE metrics (id INTEGER PRIMARY KEY, label TEXT)")
        cluster._conn.executemany(
            "INSERT INTO metrics (id, label) VALUES (?, ?)",
            [(i, f"metric-{i}") for i in range(rows)],
        )


def test_sync_arrow_select_stream_and_load() -> None:
    """Sync driver exposes inline/SHM ``ArrowCursor``, bounded portal streams, and Arrow COPY loads."""
    cluster = FakePostgresCluster()
    _seed_metrics_table(cluster, rows=10)
    pool = FakeSyncPostgresPool(cluster, max_size=2)
    drv = SyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool)

    inline_cursor = drv.select_to_arrow(
        "SELECT id, label FROM metrics WHERE id < $1 ORDER BY id", (3,), threshold_rows=256
    )
    assert inline_cursor.used_shm is False
    assert inline_cursor.to_pylist() == [
        {"id": 0, "label": "metric-0"},
        {"id": 1, "label": "metric-1"},
        {"id": 2, "label": "metric-2"},
    ]

    shm_cursor = drv.select_to_arrow("SELECT id, label FROM metrics ORDER BY id", threshold_rows=4)
    assert shm_cursor.used_shm is True
    assert len(shm_cursor) == 10
    assert shm_cursor.to_arrow_table().num_rows == 10
    assert shm_cursor.release() is True

    streamed = drv.stream_arrow_batches("SELECT id, label FROM metrics ORDER BY id", batch_size=4)
    first_chunk = next(streamed)
    assert isinstance(first_chunk, ArrowCursor)
    assert len(first_chunk) == 4
    streamed.close()
    assert len(pool.active) == 0

    incoming = pa.table({"id": [100, 101], "label": ["bulk-100", "bulk-101"]})
    assert drv.load_from_arrow("metrics", incoming) == 2
    assert drv.execute_tuples("SELECT COUNT(*) FROM metrics") == [(12,)]

    with pytest.raises(ValueError):
        drv.load_from_arrow("metrics; DROP TABLE metrics", incoming)

    drv.close()


@pytest.mark.anyio
async def test_async_arrow_select_stream_and_load() -> None:
    """Async driver streams Arrow IPC/SHM cursors, honors early stream exit, and COPY-ingests Tables."""
    cluster = FakePostgresCluster()
    _seed_metrics_table(cluster, rows=9)
    pool = FakeAsyncPostgresPool(cluster, max_size=2)
    drv = AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool)

    inline_cursor = await drv.select_to_arrow(
        "SELECT id, label FROM metrics WHERE id < $1 ORDER BY id", (2,), threshold_rows=100
    )
    assert inline_cursor.used_shm is False
    assert inline_cursor.to_pylist() == [{"id": 0, "label": "metric-0"}, {"id": 1, "label": "metric-1"}]

    shm_cursor = await drv.select_to_arrow("SELECT id, label FROM metrics ORDER BY id", threshold_rows=5)
    assert shm_cursor.used_shm is True
    assert len(shm_cursor) == 9
    assert shm_cursor.release() is True

    batches: list[ArrowCursor] = []
    async with contextlib.aclosing(
        drv.stream_arrow_batches("SELECT id, label FROM metrics ORDER BY id", batch_size=4)
    ) as stream:
        async for chunk in stream:
            batches.append(chunk)
            break
    assert len(batches) == 1
    assert len(pool.active) == 0

    incoming = ArrowCursor.from_rows([(200, "seq-200"), (201, "seq-201")], columns=["id", "label"])
    assert await drv.load_from_arrow("metrics", incoming) == 2
    assert await drv.execute_tuples("SELECT id, label FROM metrics WHERE id >= 200 ORDER BY id") == [
        (200, "seq-200"),
        (201, "seq-201"),
    ]
    await drv.close()
