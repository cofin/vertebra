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
)
from vertebra.db.cdc import ChangeEvent
from vertebra.db.driver import OperationalError
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


async def _collect_transaction_events(
    stream_iter: AsyncIterator[ChangeEvent],
    *,
    expected_dml_kind: str,
    timeout: float = 10.0,
) -> tuple[list[ChangeEvent], list[ChangeEvent]]:
    """Collect a begin/dml/commit transaction sequence alongside any observed control frames."""
    dml_events: list[ChangeEvent] = []
    control_events: list[ChangeEvent] = []
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise TimeoutError(f"Timed out waiting for {expected_dml_kind} replication transaction")
        event = await asyncio.wait_for(anext(stream_iter), timeout=remaining)
        if event.operation == "control":
            control_events.append(event)
            continue
        dml_events.append(event)
        if len(dml_events) >= 3 and event.operation == "commit":
            assert dml_events[0].operation == "begin"
            assert dml_events[1].operation == expected_dml_kind
            return dml_events, control_events


async def _wait_for_slot_state(
    admin: AsyncPostgreSQLDriver,
    slot_name: str,
    *,
    expect_active: bool,
    previous_pid: int | None = None,
    accept_pid_replacement: bool = False,
    timeout: float = 10.0,
) -> int | None:
    """Poll pg_replication_slots until slot activity matches ``expect_active`` or PID rotates."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        row = await admin.select_one_or_none(
            "SELECT active, active_pid FROM pg_replication_slots WHERE slot_name = $1",
            (slot_name,),
        )
        if row is not None:
            active = bool(row["active"])
            active_pid = row["active_pid"]
            pid_changed = previous_pid is not None and active_pid is not None and int(active_pid) != previous_pid
            if active is expect_active and (previous_pid is None or active_pid != previous_pid):
                return int(active_pid) if active_pid is not None else None
            if accept_pid_replacement and pid_changed:
                return int(active_pid)
        await asyncio.sleep(0)
    raise AssertionError(f"Slot {slot_name!r} did not reach active={expect_active!r} within {timeout}s")


@pytest.mark.anyio
async def test_live_pgoutput_toast_bytea_checkpoint_resume_and_slot_contention(
    logical_pg_config: PostgreSQLConfig,
) -> None:
    """Verify pgoutput DML, PG_UNCHANGED_TOAST, raw bytea, kill resume, fresh source resume, and contention."""
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
            insert_tx, _ = await _collect_transaction_events(events_iter, expected_dml_kind="insert")
            insert_evt = insert_tx[1]
            commit_evt = insert_tx[2]
            assert insert_evt.payload["new_tuple"]["label"] == "initial"
            assert insert_evt.payload["new_tuple"]["toast_body"] == large_toast
            assert insert_evt.payload["new_tuple"]["raw_payload"] == raw_bytes
            first_lsn = int(commit_evt.payload["commit_end_lsn"])
            saved_cp = await source.acknowledge(first_lsn)
            assert saved_cp.commit_end_lsn == first_lsn

            await admin.execute(f"UPDATE {table_name} SET label = $1 WHERE id = $2", ("updated", 1))
            update_tx, _ = await _collect_transaction_events(events_iter, expected_dml_kind="update")
            upd_evt = update_tx[1]
            commit_upd = update_tx[2]
            assert upd_evt.payload["new_tuple"]["label"] == "updated"
            assert upd_evt.payload["new_tuple"]["toast_body"] is PG_UNCHANGED_TOAST
            second_lsn = int(commit_upd.payload["commit_end_lsn"])
            assert second_lsn > first_lsn
            saved_cp = await source.acknowledge(second_lsn)
            assert saved_cp.commit_end_lsn == second_lsn
            first_generation = int(commit_upd.payload["generation"])

            initial_pid = await _wait_for_slot_state(admin, slot_name, expect_active=True)
            assert initial_pid is not None
            terminated = await admin.select_value(
                "SELECT pg_terminate_backend($1::int4)",
                (initial_pid,),
                value_type=bool,
            )
            assert terminated is True
            await _wait_for_slot_state(
                admin,
                slot_name,
                expect_active=False,
                previous_pid=initial_pid,
                accept_pid_replacement=True,
            )

            await admin.execute(f"DELETE FROM {table_name} WHERE id = $1", (1,))
            delete_tx, reconnect_controls = await _collect_transaction_events(
                events_iter,
                expected_dml_kind="delete",
            )
            del_evt = delete_tx[1]
            commit_del = delete_tx[2]
            assert del_evt.payload["old_tuple"]["id"] == 1
            third_lsn = int(commit_del.payload["commit_end_lsn"])
            assert third_lsn > second_lsn
            assert reconnect_controls or int(commit_del.payload["generation"]) > first_generation
            await source.acknowledge(third_lsn)

            with pytest.raises(OperationalError, match=r"(already active|55006|in use)"):
                async with (
                    AsyncPostgreSQLDriver(logical_pg_config) as contender_driver,
                    contender_driver.open_replication(
                        PostgresChangeConfig(slot=slot_name, publication=pub_name, reconnect=False),
                    ) as contender_source,
                ):
                    contender_iter = contender_source.stream()

                    async def _pull_contender() -> ChangeEvent:
                        return await anext(contender_iter)

                    await asyncio.wait_for(_pull_contender(), timeout=5.0)

        await _wait_for_slot_state(admin, slot_name, expect_active=False)
        await admin.execute(
            f"INSERT INTO {table_name} (id, label, toast_body, raw_payload) VALUES ($1, $2, $3, $4)",
            (2, "resumed", "short_toast", b"\x00\x01"),
        )
        async with (
            AsyncPostgreSQLDriver(logical_pg_config) as resume_driver,
            resume_driver.open_replication(change_cfg, checkpoint_store=checkpoint_store) as resumed_source,
        ):
            resumed_iter = resumed_source.stream()
            resumed_tx, _ = await _collect_transaction_events(resumed_iter, expected_dml_kind="insert")
            assert resumed_tx[1].payload["new_tuple"]["id"] == 2
            assert resumed_tx[1].payload["new_tuple"]["label"] == "resumed"
            assert resumed_source.checkpoint is not None
            assert resumed_source.checkpoint.commit_end_lsn == third_lsn
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
