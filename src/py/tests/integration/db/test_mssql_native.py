"""Live SQL Server (TDS 7.4) native protocol, LOB, Arrow BulkLoad, pipeline, savepoint, and ATTENTION suite."""

from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from vertebra.db.adapters.mssql import AsyncMSSQLDriver, MSSQLConfig, SyncMSSQLDriver
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS
from vertebra.db.driver import IntegrityError, QueryCancelledError
from vertebra.utils.module_loader import ensure_module

pytestmark = [pytest.mark.driver_engine("mssql")]


class _MSSQLPipelineAbortError(RuntimeError):
    """Sentinel error used to verify SQL Server pipeline rollback."""


def _shm_arrow_segments() -> set[str]:
    """Return active ``/dev/shm`` segment names matching Vertebra Arrow shared-memory prefixes."""
    shm_dir = Path("/dev/shm")
    if not shm_dir.is_dir():
        return set()
    return {entry.name for entry in shm_dir.iterdir() if "vtb" in entry.name.lower() or "arrow" in entry.name.lower()}


@pytest.fixture
def mssql_config(request: pytest.FixtureRequest) -> MSSQLConfig:
    """Resolve a live ``MSSQLConfig`` from ``VERTEBRA_CONFORMANCE_MSSQL_DSN`` or ``vtb_mssql_config``."""
    ensure_module("vertebra._mssql", package="vertebra-core[mssql]", extra="mssql")
    dsn = os.environ.get("VERTEBRA_CONFORMANCE_MSSQL_DSN", "").strip()
    if dsn:
        return MSSQLConfig.from_url(dsn)
    selected_engines = {
        token.strip().lower()
        for token in os.environ.get("VERTEBRA_CONFORMANCE_ENGINES", "").split(",")
        if token.strip()
    }
    if os.environ.get("VERTEBRA_CONFORMANCE_DOCKER") == "1" or "mssql" in selected_engines:
        fixture_cfg = request.getfixturevalue("vtb_mssql_config")
        assert isinstance(fixture_cfg, MSSQLConfig)
        return fixture_cfg
    pytest.skip("MSSQL live integration suite requires VERTEBRA_CONFORMANCE_MSSQL_DSN or VERTEBRA_CONFORMANCE_DOCKER=1")


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_mssql_tds_types_lobs_and_duplicate_key(
    mssql_config: MSSQLConfig,
    mode: str,
) -> None:
    """Verify TDS ``@P1`` ``sp_executesql``, ``DATETIMEOFFSET``, ``DECIMAL(38,10)``, ``> 64 KiB`` LOBs, and ``2627``."""
    table = f"vtb_live_mssql_types_{mode}"
    ddl = (
        f"CREATE TABLE {table} ("
        "id BIGINT PRIMARY KEY, "
        "dto DATETIMEOFFSET NOT NULL, "
        "dec_val DECIMAL(38, 10) NOT NULL, "
        "large_text NVARCHAR(MAX) NOT NULL, "
        "large_bin VARBINARY(MAX) NOT NULL"
        ")"
    )
    ist_offset = timezone(timedelta(hours=5, minutes=30))
    sample_dto = datetime(2026, 11, 14, 18, 45, 12, 123456, tzinfo=ist_offset)
    sample_dec = Decimal("1234567890123456789012345678.0123456789")
    large_text = "脊椎-MSSQL-" * 9000
    large_bin = b"\x00\xff\xfe\x01vtb-lob-" * 9000
    assert len(large_text.encode("utf-8")) > 65_536
    assert len(large_bin) > 65_536

    if mode == "sync":
        with SyncMSSQLDriver(mssql_config) as driver:
            driver.execute(f"DROP TABLE IF EXISTS {table}")
            driver.execute(ddl)
            try:
                inserted = driver.execute(
                    f"INSERT INTO {table} (id, dto, dec_val, large_text, large_bin) VALUES (@P1, @P2, @P3, @P4, @P5)",
                    (1, sample_dto, sample_dec, large_text, large_bin),
                )
                assert inserted == 1

                row = driver.select_one(
                    f"SELECT id, dto, dec_val, large_text, large_bin FROM {table} WHERE id = @P1",
                    (1,),
                )
                assert int(row["id"]) == 1
                assert row["dto"] == sample_dto
                assert Decimal(str(row["dec_val"])) == sample_dec
                assert row["large_text"] == large_text
                assert bytes(row["large_bin"]) == large_bin

                with pytest.raises(IntegrityError) as exc_info:
                    driver.execute(
                        f"INSERT INTO {table} (id, dto, dec_val, large_text, large_bin) VALUES (@P1, @P2, @P3, @P4, @P5)",
                        (1, sample_dto, sample_dec, "dup", b"dup"),
                    )
                assert exc_info.value.vendor_code == 2627
            finally:
                driver.execute(f"DROP TABLE IF EXISTS {table}")
    else:
        async with AsyncMSSQLDriver(mssql_config) as driver:
            await driver.execute(f"DROP TABLE IF EXISTS {table}")
            await driver.execute(ddl)
            try:
                inserted = await driver.execute(
                    f"INSERT INTO {table} (id, dto, dec_val, large_text, large_bin) VALUES (@P1, @P2, @P3, @P4, @P5)",
                    (1, sample_dto, sample_dec, large_text, large_bin),
                )
                assert inserted == 1

                row = await driver.select_one(
                    f"SELECT id, dto, dec_val, large_text, large_bin FROM {table} WHERE id = @P1",
                    (1,),
                )
                assert int(row["id"]) == 1
                assert row["dto"] == sample_dto
                assert Decimal(str(row["dec_val"])) == sample_dec
                assert row["large_text"] == large_text
                assert bytes(row["large_bin"]) == large_bin

                with pytest.raises(IntegrityError) as exc_info:
                    await driver.execute(
                        f"INSERT INTO {table} (id, dto, dec_val, large_text, large_bin) VALUES (@P1, @P2, @P3, @P4, @P5)",
                        (1, sample_dto, sample_dec, "dup", b"dup"),
                    )
                assert exc_info.value.vendor_code == 2627
            finally:
                await driver.execute(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_mssql_bulk_load_arrow_shm_pipeline_and_savepoints(
    mssql_config: MSSQLConfig,
    mode: str,
) -> None:
    """Verify TDS ``BulkLoad`` ``load_from_arrow``, ``VTBARROW`` SHM handoff, ``pipeline()``, and savepoints."""
    table = f"vtb_live_mssql_arrow_{mode}"
    target_rows = max(ARROW_SHM_THRESHOLD_ROWS, 512) + 16
    before_shm = _shm_arrow_segments()
    source_table = pa.Table.from_pydict(
        {
            "id": list(range(1, target_rows + 1)),
            "label": [f"mssql-{idx}" for idx in range(1, target_rows + 1)],
        }
    )

    if mode == "sync":
        with SyncMSSQLDriver(mssql_config) as driver:
            driver.execute(f"DROP TABLE IF EXISTS {table}")
            driver.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, label NVARCHAR(128) NOT NULL)")
            try:
                loaded = driver.load_from_arrow(table, source_table)
                assert loaded == target_rows

                with driver.select_to_arrow(f"SELECT id, label FROM {table} ORDER BY id") as cursor:
                    assert cursor.row_count == target_rows
                    assert cursor.used_shm is True
                    assert cursor.to_pyarrow().num_rows == target_rows

                batches = list(
                    driver.stream_arrow_batches(
                        f"SELECT id, label FROM {table} ORDER BY id",
                        batch_size=200,
                    )
                )
                assert len(batches) == 3

                with pytest.raises(_MSSQLPipelineAbortError), driver.pipeline() as pipe:
                    pipe.execute(f"INSERT INTO {table} (id, label) VALUES (@P1, @P2)", (99991, "pipe-rollback"))
                    _ = pipe.results()
                    raise _MSSQLPipelineAbortError("abort sync mssql pipeline")

                assert driver.select_one_or_none(f"SELECT id FROM {table} WHERE id = @P1", (99991,)) is None

                driver.begin()
                try:
                    driver.execute(f"INSERT INTO {table} (id, label) VALUES (@P1, @P2)", (99992, "kept"))
                    driver.savepoint("vtb_sp1")
                    driver.execute(f"INSERT INTO {table} (id, label) VALUES (@P1, @P2)", (99993, "reverted"))
                    driver.rollback_to_savepoint("vtb_sp1")
                    driver.release_savepoint("vtb_sp1")
                    driver.commit()
                except BaseException:
                    driver.rollback()
                    raise

                assert driver.select_one_or_none(f"SELECT id FROM {table} WHERE id = @P1", (99992,)) is not None
                assert driver.select_one_or_none(f"SELECT id FROM {table} WHERE id = @P1", (99993,)) is None
            finally:
                driver.execute(f"DROP TABLE IF EXISTS {table}")
    else:
        async with AsyncMSSQLDriver(mssql_config) as driver:
            await driver.execute(f"DROP TABLE IF EXISTS {table}")
            await driver.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, label NVARCHAR(128) NOT NULL)")
            try:
                loaded = await driver.load_from_arrow(table, source_table)
                assert loaded == target_rows

                with await driver.select_to_arrow(f"SELECT id, label FROM {table} ORDER BY id") as cursor:
                    assert cursor.row_count == target_rows
                    assert cursor.used_shm is True
                    assert cursor.to_pyarrow().num_rows == target_rows

                batches = [
                    batch
                    async for batch in driver.stream_arrow_batches(
                        f"SELECT id, label FROM {table} ORDER BY id",
                        batch_size=200,
                    )
                ]
                assert len(batches) == 3

                with pytest.raises(_MSSQLPipelineAbortError):
                    async with driver.pipeline() as pipe:
                        pipe.execute(f"INSERT INTO {table} (id, label) VALUES (@P1, @P2)", (99991, "pipe-rollback"))
                        _ = await pipe.results()
                        raise _MSSQLPipelineAbortError("abort async mssql pipeline")

                assert await driver.select_one_or_none(f"SELECT id FROM {table} WHERE id = @P1", (99991,)) is None

                await driver.begin()
                try:
                    await driver.execute(f"INSERT INTO {table} (id, label) VALUES (@P1, @P2)", (99992, "kept"))
                    await driver.savepoint("vtb_sp1")
                    await driver.execute(f"INSERT INTO {table} (id, label) VALUES (@P1, @P2)", (99993, "reverted"))
                    await driver.rollback_to_savepoint("vtb_sp1")
                    await driver.release_savepoint("vtb_sp1")
                    await driver.commit()
                except BaseException:
                    await driver.rollback()
                    raise

                assert await driver.select_one_or_none(f"SELECT id FROM {table} WHERE id = @P1", (99992,)) is not None
                assert await driver.select_one_or_none(f"SELECT id FROM {table} WHERE id = @P1", (99993,)) is None
            finally:
                await driver.execute(f"DROP TABLE IF EXISTS {table}")

    after_shm = _shm_arrow_segments()
    assert after_shm - before_shm == set()


def test_mssql_attention_cancellation_aborts_waitfor_delay(
    mssql_config: MSSQLConfig,
) -> None:
    """Verify TDS ``ATTENTION`` packet interrupts ``WAITFOR DELAY '00:00:30'`` with ``QueryCancelledError`` in ``< 10s``."""
    with SyncMSSQLDriver(mssql_config) as driver:
        assert driver.select_value("SELECT 1", value_type=int) == 1
        start = time.monotonic()
        timer = threading.Timer(0.3, driver.cancel)
        timer.start()
        try:
            with pytest.raises(QueryCancelledError):
                driver.execute("WAITFOR DELAY '00:00:30'")
            elapsed = time.monotonic() - start
            assert elapsed < 10.0
        finally:
            timer.cancel()
        assert driver.select_value("SELECT 1", value_type=int) == 1
