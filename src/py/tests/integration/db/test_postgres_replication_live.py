"""Live PostgreSQL wal_level=logical CDC acceptance tests for PostgresChangeSource."""

from __future__ import annotations

import asyncio
import contextlib
import os
import uuid
from collections.abc import AsyncIterator

import pytest

from vertebra.db.adapters.postgres import (
    PG_UNCHANGED_TOAST,
    AsyncPostgreSQLDriver,
    MemoryPostgresCheckpointStore,
    PostgresChangeConfig,
    PostgreSQLConfig,
    PostgresReplicationGapError,
)
from vertebra.db.cdc import ChangeEvent
from vertebra.db.driver import DatabaseError, OperationalError
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module

_DOCKER_ENV = "VERTEBRA_CONFORMANCE_DOCKER"
_LOGICAL_DSN_ENV = "VERTEBRA_CONFORMANCE_POSTGRES_LOGICAL_DSN"


def _resolve_logical_postgres_config(request: pytest.FixtureRequest) -> PostgreSQLConfig:
    """Resolve a live ``wal_level=logical`` PostgreSQL config or skip cleanly offline."""
    try:
        ensure_module("vertebra._postgres", package="vertebra-core[postgres]", extra="postgres")
    except MissingDependencyError:
        reason = "PostgreSQL replication acceptance tests require the vertebra._postgres native module"
        if os.environ.get(_DOCKER_ENV) == "1":
            pytest.fail(reason, pytrace=False)
        pytest.skip(reason)

    env_dsn = os.environ.get(_LOGICAL_DSN_ENV, "").strip()
    if env_dsn:
        return PostgreSQLConfig.from_url(env_dsn)
    if os.environ.get(_DOCKER_ENV) == "1":
        cfg = request.getfixturevalue("vtb_postgres_logical_config")
        assert isinstance(cfg, PostgreSQLConfig)
        return cfg
    pytest.skip(f"Live logical replication acceptance tests require {_LOGICAL_DSN_ENV} or {_DOCKER_ENV}=1")


@pytest.fixture
def logical_pg_config(request: pytest.FixtureRequest) -> PostgreSQLConfig:
    """Return resolved ``wal_level=logical`` PostgreSQL configuration."""
    return _resolve_logical_postgres_config(request)


async def _next_non_control_event(stream_iter: AsyncIterator[ChangeEvent], *, timeout: float = 10.0) -> ChangeEvent:
    """Pull the next replication ChangeEvent skipping internal control notifications."""
    while True:
        event = await asyncio.wait_for(anext(stream_iter), timeout=timeout)
        if event.operation != "control":
            return event


@pytest.mark.anyio
async def test_live_pgoutput_toast_bytea_checkpoint_resume_and_slot_contention(
    logical_pg_config: PostgreSQLConfig,
) -> None:
    """Verify pgoutput insert/update/delete, PG_UNCHANGED_TOAST, raw bytea, backend kill resume, and 55006 contention."""
    suffix = uuid.uuid4().hex[:8]
    table_name = f"vtb_cdc_live_{suffix}"
    pub_name = f"vtb_pub_live_{suffix}"
    slot_name = f"vtb_slot_live_{suffix}"
    checkpoint_store = MemoryPostgresCheckpointStore()
    admin = AsyncPostgreSQLDriver(logical_pg_config)
    await admin.connect()

    try:
        await admin.execute(
            f"CREATE TABLE {table_name} ("
            "id INT4 PRIMARY KEY, "
            "label TEXT NOT NULL, "
            "toast_body TEXT NOT NULL, "
            "raw_payload BYTEA NOT NULL"
            ")"
        )
        await admin.execute(f"ALTER TABLE {table_name} ALTER COLUMN toast_body SET STORAGE EXTERNAL")
        await admin.execute(f"CREATE PUBLICATION {pub_name} FOR TABLE {table_name}")
        await admin.execute(f"SELECT pg_create_logical_replication_slot('{slot_name}', 'pgoutput')")

        raw_bytes = b"\xff\xfe\x00\x80vtb_bin"
        large_toast = ("toast_segment_" * 400).strip()
        change_cfg = PostgresChangeConfig(slot=slot_name, publication=pub_name, reconnect=True)

        async with (
            AsyncPostgreSQLDriver(logical_pg_config) as stream_driver,
            stream_driver.open_replication(change_cfg, checkpoint_store=checkpoint_store) as source,
        ):
            events_iter = source.stream()
            await admin.execute(
                f"INSERT INTO {table_name} (id, label, toast_body, raw_payload) VALUES ($1, $2, $3, $4)",
                (1, "initial", large_toast, raw_bytes),
            )
            begin_evt = await _next_non_control_event(events_iter)
            insert_evt = await _next_non_control_event(events_iter)
            commit_evt = await _next_non_control_event(events_iter)
            assert begin_evt.operation == "begin"
            assert insert_evt.operation == "insert"
            assert insert_evt.payload["new_tuple"]["label"] == "initial"
            assert insert_evt.payload["new_tuple"]["toast_body"] == large_toast
            assert insert_evt.payload["new_tuple"]["raw_payload"] == raw_bytes
            assert commit_evt.operation == "commit"
            first_lsn = int(commit_evt.payload["commit_end_lsn"])
            saved_cp = await source.acknowledge(first_lsn)
            assert saved_cp.commit_end_lsn == first_lsn

            await admin.execute(f"UPDATE {table_name} SET label = $1 WHERE id = $2", ("updated", 1))
            begin_upd = await _next_non_control_event(events_iter)
            upd_evt = await _next_non_control_event(events_iter)
            commit_upd = await _next_non_control_event(events_iter)
            assert begin_upd.operation == "begin"
            assert upd_evt.operation == "update"
            assert upd_evt.payload["new_tuple"]["label"] == "updated"
            assert upd_evt.payload["new_tuple"]["toast_body"] is PG_UNCHANGED_TOAST
            assert commit_upd.operation == "commit"
            second_lsn = int(commit_upd.payload["commit_end_lsn"])
            assert second_lsn > first_lsn
            saved_cp = await source.acknowledge(second_lsn)
            assert saved_cp.commit_end_lsn == second_lsn

            await admin.execute(
                "SELECT pg_terminate_backend(active_pid) "
                "FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL",
                (slot_name,),
            )

            await admin.execute(f"DELETE FROM {table_name} WHERE id = $1", (1,))
            resumed_kinds: list[str] = []
            while len(resumed_kinds) < 3:
                evt = await _next_non_control_event(events_iter)
                resumed_kinds.append(evt.operation)
                if evt.operation == "commit":
                    assert int(evt.payload["commit_end_lsn"]) > second_lsn
            assert resumed_kinds == ["begin", "delete", "commit"]

            with pytest.raises((PostgresReplicationGapError, OperationalError, DatabaseError)):
                async with (
                    AsyncPostgreSQLDriver(logical_pg_config) as contender_driver,
                    contender_driver.open_replication(
                        PostgresChangeConfig(slot=slot_name, publication=pub_name, reconnect=False),
                    ) as contender_source,
                ):
                    await _next_non_control_event(contender_source.stream(), timeout=5.0)
    finally:
        with contextlib.suppress(Exception):
            await admin.execute(
                "SELECT pg_terminate_backend(active_pid) "
                "FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL",
                (slot_name,),
            )
        with contextlib.suppress(Exception):
            await admin.execute(f"SELECT pg_drop_replication_slot('{slot_name}')")
        with contextlib.suppress(Exception):
            await admin.execute(f"DROP PUBLICATION IF EXISTS {pub_name}")
        with contextlib.suppress(Exception):
            await admin.execute(f"DROP TABLE IF EXISTS {table_name}")
        await admin.close()
