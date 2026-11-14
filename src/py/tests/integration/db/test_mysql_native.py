"""Live MySQL and MariaDB native binary protocol, Arrow SHM, pipeline, and cancellation integration suite."""

from __future__ import annotations

import contextlib
import os
import threading
import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from vertebra.db.adapters.mysql import AsyncMySQLDriver, MySQLConfig, SyncMySQLDriver
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS
from vertebra.db.driver import DatabaseError, IntegrityError
from vertebra.utils.module_loader import ensure_module


class _PipelineAbortError(RuntimeError):
    """Sentinel exception used to verify MySQL/MariaDB pipeline rollback."""


def _shm_arrow_segments() -> set[str]:
    """Return active ``/dev/shm`` segment names matching Vertebra Arrow shared-memory prefixes."""
    shm_dir = Path("/dev/shm")
    if not shm_dir.is_dir():
        return set()
    return {entry.name for entry in shm_dir.iterdir() if "vtb" in entry.name.lower() or "arrow" in entry.name.lower()}


@pytest.fixture(
    params=[
        pytest.param("mysql", marks=pytest.mark.driver_engine("mysql"), id="mysql"),
        pytest.param("mariadb", marks=pytest.mark.driver_engine("mariadb"), id="mariadb"),
    ]
)
def mysql_family_config(request: pytest.FixtureRequest) -> tuple[str, MySQLConfig]:
    """Resolve a live ``MySQLConfig`` for ``mysql`` or ``mariadb`` from DSN env or ``pytest-databases``."""
    family = str(request.param)
    ensure_module("vertebra._mysql", package="vertebra-core[mysql]", extra="mysql")
    env_var = f"VERTEBRA_CONFORMANCE_{family.upper()}_DSN"
    dsn = os.environ.get(env_var, "").strip()
    if dsn:
        return family, MySQLConfig.from_url(dsn)
    selected_engines = {
        token.strip().lower()
        for token in os.environ.get("VERTEBRA_CONFORMANCE_ENGINES", "").split(",")
        if token.strip()
    }
    if os.environ.get("VERTEBRA_CONFORMANCE_DOCKER") == "1" or family in selected_engines:
        fixture_cfg = request.getfixturevalue(f"vtb_{family}_config")
        assert isinstance(fixture_cfg, MySQLConfig)
        return family, fixture_cfg
    pytest.skip(f"{family} live integration suite requires {env_var} or VERTEBRA_CONFORMANCE_DOCKER=1")


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_mysql_family_binary_prepared_types_lastrowid_and_duplicate_key(
    mysql_family_config: tuple[str, MySQLConfig],
    mode: str,
) -> None:
    """Verify binary ``COM_STMT_*`` prepared roundtrips, ``utf8mb4`` emoji, ``DECIMAL(65,30)``, ``lastrowid``, and ``1062``."""
    family, config = mysql_family_config
    table = f"vtb_live_{family}_types_{mode}"
    ddl = (
        f"CREATE TABLE {table} ("
        "id BIGINT AUTO_INCREMENT PRIMARY KEY, "
        "emoji_text VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL, "
        "high_dec DECIMAL(65, 30) NOT NULL, "
        "ts_micro DATETIME(6) NOT NULL, "
        "payload_json JSON NOT NULL"
        ")"
    )
    high_decimal = Decimal("12345678901234567890123456789012345.012345678901234567890123456789")
    sample_ts = datetime(2026, 11, 14, 15, 20, 25, 654321, tzinfo=UTC)
    sample_emoji = "Vertebra 脊椎 🚀🧪🔥"

    if mode == "sync":
        with SyncMySQLDriver(config) as driver:
            driver.execute(f"DROP TABLE IF EXISTS {table}")
            driver.execute(ddl)
            try:
                affected = driver.execute(
                    f"INSERT INTO {table} (emoji_text, high_dec, ts_micro, payload_json) VALUES (?, ?, ?, ?)",
                    (sample_emoji, high_decimal, sample_ts, '{"engine": "mysql"}'),
                )
                assert affected == 1
                assert driver.lastrowid == 1
                assert driver.statement_cache_len() >= 1

                row = driver.select_one(
                    f"SELECT id, emoji_text, high_dec, ts_micro FROM {table} WHERE id = ?",
                    (1,),
                )
                assert int(row["id"]) == 1
                assert row["emoji_text"] == sample_emoji
                assert Decimal(str(row["high_dec"])) == high_decimal

                with pytest.raises(IntegrityError) as exc_info:
                    driver.execute(
                        f"INSERT INTO {table} (id, emoji_text, high_dec, ts_micro, payload_json) VALUES (?, ?, ?, ?, ?)",
                        (1, "dup", high_decimal, sample_ts, "{}"),
                    )
                assert exc_info.value.vendor_code == 1062
            finally:
                driver.execute(f"DROP TABLE IF EXISTS {table}")
    else:
        async with AsyncMySQLDriver(config) as driver:
            await driver.execute(f"DROP TABLE IF EXISTS {table}")
            await driver.execute(ddl)
            try:
                affected = await driver.execute(
                    f"INSERT INTO {table} (emoji_text, high_dec, ts_micro, payload_json) VALUES (?, ?, ?, ?)",
                    (sample_emoji, high_decimal, sample_ts, '{"engine": "mysql"}'),
                )
                assert affected == 1
                assert driver.lastrowid == 1
                assert driver.statement_cache_len() >= 1

                row = await driver.select_one(
                    f"SELECT id, emoji_text, high_dec, ts_micro FROM {table} WHERE id = ?",
                    (1,),
                )
                assert int(row["id"]) == 1
                assert row["emoji_text"] == sample_emoji
                assert Decimal(str(row["high_dec"])) == high_decimal

                with pytest.raises(IntegrityError) as exc_info:
                    await driver.execute(
                        f"INSERT INTO {table} (id, emoji_text, high_dec, ts_micro, payload_json) VALUES (?, ?, ?, ?, ?)",
                        (1, "dup", high_decimal, sample_ts, "{}"),
                    )
                assert exc_info.value.vendor_code == 1062
            finally:
                await driver.execute(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_mysql_family_multi_result_call_arrow_shm_and_pipeline(
    mysql_family_config: tuple[str, MySQLConfig],
    mode: str,
) -> None:
    """Verify ``CALL`` multi-result sets via ``fetch_all_sets``, ``VTBARROW`` SHM handoff, and ``pipeline()``."""
    family, config = mysql_family_config
    table = f"vtb_live_{family}_arrow_{mode}"
    proc = f"vtb_live_{family}_proc_{mode}"
    target_rows = max(ARROW_SHM_THRESHOLD_ROWS, 512) + 16
    before_shm = _shm_arrow_segments()
    source_table = pa.Table.from_pydict(
        {
            "id": list(range(1, target_rows + 1)),
            "label": [f"item-{idx}" for idx in range(1, target_rows + 1)],
        }
    )

    if mode == "sync":
        with SyncMySQLDriver(config) as driver:
            driver.execute(f"DROP PROCEDURE IF EXISTS {proc}")
            driver.execute(f"DROP TABLE IF EXISTS {table}")
            driver.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, label VARCHAR(128) NOT NULL)")
            driver.execute(f"CREATE PROCEDURE {proc}() BEGIN SELECT 1 AS first_col; SELECT 'two' AS second_col; END")
            try:
                result_sets = driver.fetch_all_sets(f"CALL {proc}()")
                assert len(result_sets) >= 2
                assert int(result_sets[0][0]["first_col"]) == 1
                assert result_sets[1][0]["second_col"] == "two"

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

                with pytest.raises(_PipelineAbortError), driver.pipeline() as pipe:
                    pipe.execute(f"INSERT INTO {table} (id, label) VALUES (?, ?)", (99999, "rolled-back"))
                    _ = pipe.results()
                    raise _PipelineAbortError("abort sync mysql pipeline")

                assert driver.select_one_or_none(f"SELECT id FROM {table} WHERE id = ?", (99999,)) is None
            finally:
                driver.execute(f"DROP PROCEDURE IF EXISTS {proc}")
                driver.execute(f"DROP TABLE IF EXISTS {table}")
    else:
        async with AsyncMySQLDriver(config) as driver:
            await driver.execute(f"DROP PROCEDURE IF EXISTS {proc}")
            await driver.execute(f"DROP TABLE IF EXISTS {table}")
            await driver.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, label VARCHAR(128) NOT NULL)")
            await driver.execute(
                f"CREATE PROCEDURE {proc}() BEGIN SELECT 1 AS first_col; SELECT 'two' AS second_col; END"
            )
            try:
                result_sets = await driver.fetch_all_sets(f"CALL {proc}()")
                assert len(result_sets) >= 2
                assert int(result_sets[0][0]["first_col"]) == 1
                assert result_sets[1][0]["second_col"] == "two"

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

                with pytest.raises(_PipelineAbortError):
                    async with driver.pipeline() as pipe:
                        pipe.execute(f"INSERT INTO {table} (id, label) VALUES (?, ?)", (99999, "rolled-back"))
                        _ = await pipe.results()
                        raise _PipelineAbortError("abort async mysql pipeline")

                assert await driver.select_one_or_none(f"SELECT id FROM {table} WHERE id = ?", (99999,)) is None
            finally:
                await driver.execute(f"DROP PROCEDURE IF EXISTS {proc}")
                await driver.execute(f"DROP TABLE IF EXISTS {table}")

    after_shm = _shm_arrow_segments()
    assert after_shm - before_shm == set()


def test_mysql_family_active_query_cancellation_under_ten_seconds(
    mysql_family_config: tuple[str, MySQLConfig],
) -> None:
    """Verify out-of-band ``KILL QUERY`` interrupts an active ``SELECT SLEEP(30)`` in ``< 10s``."""
    _family, config = mysql_family_config
    with SyncMySQLDriver(config) as driver:
        assert driver.select_value("SELECT 1", value_type=int) == 1
        conn_id = driver.connection_id
        start = time.monotonic()
        timer = threading.Timer(0.3, lambda: driver.cancel(connection_id=conn_id))
        timer.start()
        try:
            with contextlib.suppress(DatabaseError):
                _ = driver.select("SELECT SLEEP(30) AS slept")
            elapsed = time.monotonic() - start
            assert elapsed < 10.0
        finally:
            timer.cancel()
        assert driver.select_value("SELECT 1", value_type=int) == 1
