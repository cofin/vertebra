"""Live PostgreSQL acceptance tests for binary COPY, VTBARROW SHM, LISTEN/NOTIFY, pgvector, and PgBouncer."""

from __future__ import annotations

import asyncio
import contextlib
import os
import struct
import uuid
from collections.abc import AsyncIterator

import pyarrow as pa
import pytest

from vertebra.db.adapters.postgres import (
    ARROW_SHM_THRESHOLD_ROWS,
    AsyncPostgreSQLDriver,
    PgBitString,
    PgVector,
    PostgresChangeConfig,
    PostgresNotificationSource,
    PostgresNotifyConfig,
    PostgreSQLConfig,
    SyncPostgreSQLDriver,
)
from vertebra.db.arrow import ArrowCursor
from vertebra.db.driver import InterfaceError, NotificationEvent
from vertebra.typing import ImproperConfigurationError, MissingDependencyError
from vertebra.utils.module_loader import ensure_module

_DOCKER_ENV = "VERTEBRA_CONFORMANCE_DOCKER"
_PGVECTOR_DSN_ENV = "VERTEBRA_CONFORMANCE_PGVECTOR_DSN"
_PGBOUNCER_DSN_ENV = "VERTEBRA_CONFORMANCE_PGBOUNCER_DSN"


def _ensure_postgres_extension_for_live() -> None:
    """Require compiled vertebra._postgres, failing under Docker conformance and skipping locally."""
    try:
        ensure_module("vertebra._postgres", package="vertebra-core[postgres]", extra="postgres")
    except MissingDependencyError:
        reason = "PostgreSQL live feature tests require the vertebra._postgres native module"
        if os.environ.get(_DOCKER_ENV) == "1":
            pytest.fail(reason, pytrace=False)
        pytest.skip(reason)


@pytest.fixture
def pgvector_config(request: pytest.FixtureRequest) -> PostgreSQLConfig:
    """Resolve a live pgvector PostgreSQL config from env or Docker fixture."""
    _ensure_postgres_extension_for_live()
    env_dsn = os.environ.get(_PGVECTOR_DSN_ENV, "").strip()
    if env_dsn:
        return PostgreSQLConfig.from_url(env_dsn)
    if os.environ.get(_DOCKER_ENV) == "1":
        cfg = request.getfixturevalue("vtb_pgvector_config")
        assert isinstance(cfg, PostgreSQLConfig)
        return cfg
    pytest.skip(f"Live pgvector tests require {_PGVECTOR_DSN_ENV} or {_DOCKER_ENV}=1")


@pytest.fixture
def pgbouncer_config_pair(request: pytest.FixtureRequest) -> tuple[PostgreSQLConfig, PostgreSQLConfig | None]:
    """Resolve live PgBouncer pooled and optional direct PostgreSQL configs."""
    _ensure_postgres_extension_for_live()
    env_dsn = os.environ.get(_PGBOUNCER_DSN_ENV, "").strip()
    if env_dsn:
        return PostgreSQLConfig(dsn=env_dsn, pooler_mode="transaction", pgbouncer=True), None
    if os.environ.get(_DOCKER_ENV) == "1":
        pair = request.getfixturevalue("vtb_pgbouncer_config")
        assert isinstance(pair, tuple) and len(pair) == 2
        return pair
    pytest.skip(f"Live PgBouncer tests require {_PGBOUNCER_DSN_ENV} or {_DOCKER_ENV}=1")


@pytest.mark.parametrize("vector_kind", ["vector", "halfvec"])
def test_live_pgvector_copy_and_arrow_roundtrip(
    pgvector_config: PostgreSQLConfig,
    vector_kind: str,
) -> None:
    """Verify pgvector vector/halfvec, bitstrings, binary COPY, and VTBARROW SHM against live PostgreSQL."""
    table_name = f"vtb_feat_live_{uuid.uuid4().hex[:8]}"
    copy_table = f"vtb_copy_live_{uuid.uuid4().hex[:8]}"
    with SyncPostgreSQLDriver(pgvector_config) as driver:
        driver.execute("CREATE EXTENSION IF NOT EXISTS vector")
        col_type = "vector(3)" if vector_kind == "vector" else "halfvec(3)"
        driver.execute(
            f"CREATE TABLE {table_name} (id INT4 PRIMARY KEY, emb {col_type} NOT NULL, flags BIT(6) NOT NULL)"
        )
        driver.execute(f"CREATE TABLE {copy_table} (id INT4 PRIMARY KEY, label TEXT NOT NULL)")
        try:
            vec = PgVector(values=(1.25, -0.5, 2.0), kind="vector" if vector_kind == "vector" else "halfvec")
            bits = PgBitString(data=b"\xa4", bit_length=6)
            driver.execute(f"INSERT INTO {table_name} (id, emb, flags) VALUES ($1, $2, $3)", (1, vec, bits))
            row = driver.select_one(f"SELECT id, emb, flags FROM {table_name} WHERE id = $1", (1,))
            assert isinstance(row["emb"], PgVector)
            assert row["emb"].kind == vector_kind
            assert list(row["emb"].values) == pytest.approx([1.25, -0.5, 2.0], rel=1e-3)
            assert row["flags"] == bits

            loaded = driver.copy_from(
                f"COPY {copy_table} (id, label) FROM STDIN WITH (FORMAT binary)",
                [(1, "alpha"), (2, "beta"), (3, "gamma")],
                (23, 25),
            )
            assert loaded == 3
            with driver.copy_to(
                f"COPY (SELECT id, label FROM {copy_table} ORDER BY id) TO STDOUT WITH (FORMAT binary)"
            ) as stream:
                copied_rows = list(stream)
            assert copied_rows == [
                (struct.pack("!i", 1), b"alpha"),
                (struct.pack("!i", 2), b"beta"),
                (struct.pack("!i", 3), b"gamma"),
            ]

            arrow_table = pa.table({"id": [10, 11], "label": ["arrow_x", "arrow_y"]})
            assert driver.load_from_arrow(copy_table, arrow_table) == 2
            with driver.select_to_arrow(
                f"SELECT id, label FROM {copy_table} WHERE id >= $1 ORDER BY id",
                (10,),
                threshold_rows=min(1, ARROW_SHM_THRESHOLD_ROWS),
            ) as cursor:
                assert isinstance(cursor, ArrowCursor)
                assert cursor.used_shm is True
                assert cursor.shm_name
                assert cursor.row_count == 2
                assert cursor.to_pylist() == [
                    {"id": 10, "label": "arrow_x"},
                    {"id": 11, "label": "arrow_y"},
                ]
        finally:
            with contextlib.suppress(Exception):
                driver.execute(f"DROP TABLE IF EXISTS {table_name}")
            with contextlib.suppress(Exception):
                driver.execute(f"DROP TABLE IF EXISTS {copy_table}")


async def _wait_for_listener_ready(
    notify_source: PostgresNotificationSource,
    listener_iter: AsyncIterator[NotificationEvent],
    channel: str,
    *,
    timeout: float = 10.0,
) -> bytes:
    """Publish readiness probes deterministically until the active LISTEN stream consumes one."""
    probe_token = f"vtb_ready_{uuid.uuid4().hex[:8]}".encode("ascii")

    async def _pull_until_probe() -> None:
        while True:
            event = await anext(listener_iter)
            if event.channel == channel and event.payload == probe_token:
                return

    wait_task = asyncio.create_task(_pull_until_probe())
    try:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while not wait_task.done():
            if loop.time() >= deadline:
                raise TimeoutError(f"Listener did not become ready on channel {channel!r}")
            await notify_source.publish(channel, probe_token)
            done, _ = await asyncio.wait({wait_task}, timeout=0.2)
            if done:
                await wait_task
                return probe_token
        await wait_task
        return probe_token
    finally:
        if not wait_task.done():
            wait_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await wait_task


@pytest.mark.anyio
async def test_live_async_listen_notify_delivery(pgvector_config: PostgreSQLConfig) -> None:
    """Verify asynchronous LISTEN/NOTIFY subscription, publish, and binary payload hydration."""
    channel = f"vtb_live_ch_{uuid.uuid4().hex[:8]}"
    async with AsyncPostgreSQLDriver(pgvector_config) as driver:
        notify_source = driver.listen_notifications(PostgresNotifyConfig(channel=channel))
        async with notify_source:
            await notify_source.subscribe([channel])
            listener_iter = notify_source.listen()
            probe_token = await _wait_for_listener_ready(notify_source, listener_iter, channel)
            await notify_source.publish(channel, b"binary\x00frame")

            async def _pull_next() -> NotificationEvent:
                while True:
                    next_event = await anext(listener_iter)
                    if next_event.payload != probe_token:
                        return next_event

            event = await asyncio.wait_for(_pull_next(), timeout=10.0)
            assert event.channel == channel
            assert event.payload == b"binary\x00frame"


@pytest.mark.anyio
async def test_live_pgbouncer_unnamed_statement_and_capability_guards(
    pgbouncer_config_pair: tuple[PostgreSQLConfig, PostgreSQLConfig | None],
) -> None:
    """Verify PgBouncer transaction/statement mode unnamed statement execution and event/tx refusals."""
    mode_cfg, _ = pgbouncer_config_pair
    assert mode_cfg.effective_statement_cache_size == 0

    async with AsyncPostgreSQLDriver(mode_cfg) as driver:
        results = [await driver.select_value("SELECT $1::int4 + 1", (idx,)) for idx in range(1, 6)]
        assert results == [2, 3, 4, 5, 6]

        no_direct_cfg = PostgreSQLConfig(
            dsn=mode_cfg.dsn,
            direct_dsn=None,
            pooler_mode=mode_cfg.pooler_mode,
            pgbouncer=mode_cfg.pgbouncer,
        )
        async with AsyncPostgreSQLDriver(no_direct_cfg) as restricted_driver:
            with pytest.raises(ImproperConfigurationError, match="LISTEN/NOTIFY"):
                restricted_driver.listen_notifications()
            with pytest.raises(ImproperConfigurationError, match="logical replication"):
                restricted_driver.open_replication(
                    PostgresChangeConfig(slot="vtb_pooler_slot", publication="vtb_pooler_pub"),
                )

        if mode_cfg.pooler_mode == "statement":
            with pytest.raises(InterfaceError, match="Statement"):
                async with driver.transaction():
                    await driver.select_value("SELECT 1")
