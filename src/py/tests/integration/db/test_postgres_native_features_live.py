"""Live PostgreSQL acceptance tests for binary COPY, VTBARROW SHM, LISTEN/NOTIFY, pgvector, and PgBouncer."""

from __future__ import annotations

import asyncio
import contextlib
import os
import uuid
from typing import Any

import pyarrow as pa
import pytest

from vertebra.db.adapters.postgres import (
    ARROW_SHM_THRESHOLD_ROWS,
    AsyncPostgreSQLDriver,
    PgBitString,
    PgVector,
    PoolerMode,
    PostgresNotifyConfig,
    PostgreSQLConfig,
    SyncPostgreSQLDriver,
)
from vertebra.db.arrow import ArrowCursor
from vertebra.db.driver import InterfaceError
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
    """Verify pgvector vector/halfvec, bitstrings, binary COPY, and Arrow SHM against live PostgreSQL."""
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
            assert len(copied_rows) == 3

            arrow_table = pa.table({"id": [10, 11], "label": ["arrow_x", "arrow_y"]})
            assert driver.load_from_arrow(copy_table, arrow_table) == 2
            with driver.select_to_arrow(
                f"SELECT id, label FROM {copy_table} WHERE id >= $1 ORDER BY id",
                (10,),
                threshold_rows=min(1, ARROW_SHM_THRESHOLD_ROWS),
            ) as cursor:
                assert isinstance(cursor, ArrowCursor)
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


@pytest.mark.anyio
async def test_live_async_listen_notify_delivery(pgvector_config: PostgreSQLConfig) -> None:
    """Verify asynchronous LISTEN/NOTIFY subscription, publish, and payload hydration."""
    channel = f"vtb_live_ch_{uuid.uuid4().hex[:8]}"
    async with AsyncPostgreSQLDriver(pgvector_config) as driver:
        notify_source = driver.listen_notifications(PostgresNotifyConfig(channel=channel))
        async with notify_source:
            await notify_source.subscribe([channel])
            listener_iter = notify_source.listen()

            async def _pull_next() -> Any:
                return await anext(listener_iter)

            next_event_task = asyncio.create_task(_pull_next())
            await asyncio.sleep(0.05)
            await notify_source.publish(channel, b"binary\x00frame")
            event = await asyncio.wait_for(next_event_task, timeout=10.0)
            assert event.channel == channel
            assert event.payload == b"binary\x00frame"


@pytest.mark.anyio
@pytest.mark.parametrize("pooler_mode", ["transaction", "statement"])
async def test_live_pgbouncer_unnamed_statement_and_capability_guards(
    pgbouncer_config_pair: tuple[PostgreSQLConfig, PostgreSQLConfig | None],
    pooler_mode: PoolerMode,
) -> None:
    """Verify PgBouncer transaction/statement mode unnamed statement execution and statement refusals."""
    base_cfg, _ = pgbouncer_config_pair
    mode_cfg = PostgreSQLConfig(
        dsn=base_cfg.dsn,
        direct_dsn=base_cfg.direct_dsn,
        pooler_mode=pooler_mode,
        pgbouncer=pooler_mode == "transaction",
        protocol_level_pooler_cache=False,
    )
    assert mode_cfg.effective_statement_cache_size == 0

    async with AsyncPostgreSQLDriver(mode_cfg) as driver:
        results = [await driver.select_value("SELECT $1::int4 + 1", (idx,)) for idx in range(1, 6)]
        assert results == [2, 3, 4, 5, 6]

        if pooler_mode == "statement":
            no_direct_cfg = PostgreSQLConfig(
                dsn=base_cfg.dsn,
                direct_dsn=None,
                pooler_mode="statement",
                pgbouncer=False,
            )
            async with AsyncPostgreSQLDriver(no_direct_cfg) as restricted_driver:
                with pytest.raises(ImproperConfigurationError, match="LISTEN/NOTIFY"):
                    restricted_driver.listen_notifications()
            with pytest.raises(InterfaceError, match="Statement"):
                async with driver.transaction():
                    await driver.select_value("SELECT 1")
