"""Unit tests for PostgreSQL EventSessionRegistry and PostgresChangeSource."""

from __future__ import annotations

import asyncio
import re
from typing import Any

import pytest

from vertebra.db.adapters.postgres import SqlPostgresCheckpointStore as ExportedSqlPostgresCheckpointStore
from vertebra.db.adapters.postgres.change_stream import (
    MemoryPostgresCheckpointStore,
    PostgresChangeConfig,
    PostgresChangeSource,
    PostgresCheckpoint,
    PostgresCheckpointConflictError,
    PostgresCheckpointStore,
    PostgresReplicationGapError,
    SqlPostgresCheckpointStore,
    _decode_identity_key,
    _encode_identity_key,
)
from vertebra.db.adapters.postgres.config import PostgreSQLConfig
from vertebra.db.adapters.postgres.events import (
    EventSessionRegistry,
    PostgresReplicationDriver,
)
from vertebra.db.adapters.postgres.types import (
    PG_UNCHANGED_TOAST,
    PostgresReplicationIdentity,
    PostgresReplicationRecord,
)
from vertebra.db.cdc import CDCConfig, ChangeEvent, ChangeFeed
from vertebra.db.driver import AsyncChangeSource, OperationalError, SerializationFailure
from vertebra.storage import MemoryConfig, SyncObjectStore


class _TrackedHandle:
    """Minimal closeable stream/listener handle for EventSessionRegistry tests."""

    def __init__(self, *, closed: bool = False, fail_aclose: Exception | None = None) -> None:
        self.closed = closed
        self.fail_aclose = fail_aclose
        self.aclose_calls = 0

    async def aclose(self) -> None:
        self.aclose_calls += 1
        self.closed = True
        if self.fail_aclose is not None:
            raise self.fail_aclose


class _TrackedClient:
    """Minimal closeable client handle paired with a session handle."""

    def __init__(self, *, fail_close: Exception | None = None) -> None:
        self.fail_close = fail_close
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1
        if self.fail_close is not None:
            raise self.fail_close


class _FakeNativeStream:
    """In-memory scriptable replication stream mimicking AsyncPostgresReplication."""

    def __init__(
        self,
        items: list[PostgresReplicationRecord | BaseException | None],
        *,
        fail_aclose_exc: BaseException | None = None,
    ) -> None:
        self._items = list(items)
        self.closed = False
        self.acks: list[int] = []
        self.ack_called = asyncio.Event()
        self.aclose_called = asyncio.Event()
        self.next_started = asyncio.Event()
        self.block_next_gate: asyncio.Event | None = None
        self.fail_ack_exc: BaseException | None = None
        self.fail_aclose_exc = fail_aclose_exc

    async def next(self) -> PostgresReplicationRecord | None:
        self.next_started.set()
        if self.block_next_gate is not None:
            await self.block_next_gate.wait()
        if not self._items:
            return None
        item = self._items.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    def acknowledge(self, commit_end_lsn: int) -> int:
        if self.closed:
            raise ConnectionError("replication stream closed")
        if self.fail_ack_exc is not None:
            raise self.fail_ack_exc
        self.acks.append(commit_end_lsn)
        self.ack_called.set()
        return commit_end_lsn

    async def aclose(self) -> None:
        self.closed = True
        self.aclose_called.set()
        if self.fail_aclose_exc is not None:
            raise self.fail_aclose_exc


class _FakeReplicationDriver:
    """In-memory PostgreSQL replication driver serving scripted stream generations."""

    def __init__(
        self,
        identity: PostgresReplicationIdentity,
        streams: list[_FakeNativeStream | BaseException],
        *,
        log: list[str] | None = None,
    ) -> None:
        self._config = PostgreSQLConfig(dsn="postgresql://postgres@127.0.0.1:5432/app")
        self._identity = identity
        self._streams = list(streams)
        self._log = log
        self.calls: list[str] = []
        self.opened_args: list[tuple[int | None, bool]] = []
        self.open_count = 0

    @property
    def config(self) -> PostgreSQLConfig:
        return self._config

    async def _replication_identity(
        self,
        slot: str,
        publication: str,
    ) -> PostgresReplicationIdentity:
        self.calls.append(f"identity:{slot}:{publication}")
        if self._log is not None:
            self._log.append("driver.identity")
        return self._identity

    async def _open_replication_stream(
        self,
        config: PostgresChangeConfig,
        *,
        start_lsn: int | None = None,
        reconnect: bool = False,
    ) -> Any:
        self.calls.append(f"open:{start_lsn}:{reconnect}")
        if self._log is not None:
            self._log.append("driver.open")
        self.opened_args.append((start_lsn, reconnect))
        self.open_count += 1
        entry = self._streams.pop(0)
        if isinstance(entry, BaseException):
            raise entry
        return entry


class _OrderRecordingCheckpointStore:
    """Wrapper store recording load/save order and injecting controlled failures."""

    def __init__(
        self,
        inner: MemoryPostgresCheckpointStore,
        log: list[str] | None = None,
    ) -> None:
        self._inner = inner
        self._log = log if log is not None else []
        self.load_started = asyncio.Event()
        self.load_gate: asyncio.Event | None = None
        self.save_started = asyncio.Event()
        self.save_gate: asyncio.Event | None = None
        self.fail_next_save: BaseException | None = None
        self.fail_after_commit = False
        self.load_count = 0
        self.save_count = 0

    async def load(self, identity_key: tuple[str, str, str, str]) -> PostgresCheckpoint | None:
        self.load_count += 1
        self._log.append("store.load")
        self.load_started.set()
        if self.load_gate is not None:
            await self.load_gate.wait()
        return await self._inner.load(identity_key)

    async def save(
        self,
        checkpoint: PostgresCheckpoint,
        *,
        expected: PostgresCheckpoint | None,
    ) -> PostgresCheckpoint:
        self.save_count += 1
        self._log.append(f"store.save:{checkpoint.commit_end_lsn}:v{checkpoint.version}")
        self.save_started.set()
        if self.save_gate is not None:
            await self.save_gate.wait()
        if self.fail_next_save is not None:
            exc = self.fail_next_save
            self.fail_next_save = None
            if self.fail_after_commit:
                self.fail_after_commit = False
                await self._inner.save(checkpoint, expected=expected)
            raise exc
        return await self._inner.save(checkpoint, expected=expected)


def _identity(
    *,
    system_id: str = "sys-1234",
    database: str = "appdb",
    slot: str = "cdc_slot",
    fingerprint: str = "pub-v1",
) -> PostgresReplicationIdentity:
    return PostgresReplicationIdentity(
        system_identifier=system_id,
        database=database,
        slot=slot,
        publication_fingerprint=fingerprint,
    )


@pytest.fixture
def anyio_backend() -> str:
    """Pin anyio tests in this module to asyncio."""
    return "asyncio"


@pytest.mark.anyio
async def test_event_session_registry_prunes_closed_and_preserves_primary_error() -> None:
    """Verify EventSessionRegistry prunes closed pairs and closes all without masking errors."""
    registry = EventSessionRegistry()
    closed_handle = _TrackedHandle(closed=True)
    closed_client = _TrackedClient()
    failing_handle = _TrackedHandle(fail_aclose=RuntimeError("handle boom"))
    failing_client = _TrackedClient(fail_close=RuntimeError("client secondary boom"))
    healthy_handle = _TrackedHandle()
    healthy_client = _TrackedClient()

    registry.register(closed_handle, closed_client)
    registry.register(failing_handle, failing_client)
    registry.register(healthy_handle, healthy_client)

    assert len(registry) == 2
    assert closed_handle.aclose_calls == 0

    with pytest.raises(RuntimeError, match="handle boom"):
        await registry.close_all()

    assert healthy_handle.aclose_calls == 1
    assert healthy_client.close_calls == 1
    assert failing_handle.aclose_calls == 1
    assert failing_client.close_calls == 1
    assert len(registry) == 0


@pytest.mark.anyio
async def test_checkpoint_store_cas_conflict_and_monotonicity() -> None:
    """Verify MemoryPostgresCheckpointStore enforces CAS fencing and monotone cursors."""
    store = MemoryPostgresCheckpointStore()
    ident = _identity()
    first = PostgresCheckpoint(
        identity_key=ident.identity_key,
        commit_end_lsn=100,
        next_event_id=2,
        version=1,
    )
    saved_first = await store.save(first, expected=None)
    assert saved_first.version == 1

    with pytest.raises(PostgresCheckpointConflictError) as conflict_info:
        await store.save(first, expected=None)
    assert isinstance(conflict_info.value, SerializationFailure)
    assert isinstance(conflict_info.value, OperationalError)
    assert conflict_info.value.sqlstate == "40001"

    second = saved_first.advance(commit_end_lsn=200, next_event_id=3)
    saved_second = await store.save(second, expected=saved_first)
    assert saved_second.version == 2

    with pytest.raises(PostgresCheckpointConflictError):
        await store.save(saved_first.advance(commit_end_lsn=150, next_event_id=3), expected=saved_first)

    with pytest.raises(ValueError, match="commit_end_lsn regression"):
        saved_second.advance(commit_end_lsn=199, next_event_id=4)


@pytest.mark.anyio
async def test_change_source_identity_before_load_and_no_ack_on_yield_or_close() -> None:
    """Verify identity resolution precedes store load, reconnect=False, and yield/close never ack."""
    ident = _identity()
    inner_store = MemoryPostgresCheckpointStore()
    timeline: list[str] = []
    store = _OrderRecordingCheckpointStore(inner_store, timeline)
    stream = _FakeNativeStream(
        [
            PostgresReplicationRecord(
                kind="insert",
                payload={"table": "orders", "toast": PG_UNCHANGED_TOAST},
                commit_end_lsn=128,
            ),
        ]
    )
    driver = _FakeReplicationDriver(ident, [stream], log=timeline)
    assert isinstance(driver, PostgresReplicationDriver)
    config = PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=False)
    source = PostgresChangeSource(driver, config, store)
    assert isinstance(source, AsyncChangeSource)

    event = await source.__anext__()
    assert event.operation == "insert"
    assert event.event_id == 1
    assert driver.opened_args == [(None, False)]
    assert stream.acks == []
    assert timeline == ["driver.identity", "store.load", "driver.open"]

    await source.aclose()
    assert stream.acks == []
    assert stream.closed is True
    assert await store.load(ident.identity_key) is None


@pytest.mark.anyio
async def test_change_source_shielded_save_before_wire_ack_preserves_stream() -> None:
    """Verify save-before-wire-ack survives caller cancellation and keeps stream open on success."""
    ident = _identity()
    inner_store = MemoryPostgresCheckpointStore()
    log: list[str] = []
    store = _OrderRecordingCheckpointStore(inner_store, log)
    store.save_gate = asyncio.Event()
    stream = _FakeNativeStream(
        [
            PostgresReplicationRecord(kind="insert", payload={"id": 1}, commit_end_lsn=100),
            PostgresReplicationRecord(kind="update", payload={"id": 2}, commit_end_lsn=200),
        ]
    )
    driver = _FakeReplicationDriver(ident, [stream])
    source = PostgresChangeSource(
        driver,
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=False),
        store,
    )

    first_ev = await source.__anext__()
    assert first_ev.event_id == 1

    ack_task = asyncio.create_task(source.acknowledge(100))
    await store.save_started.wait()
    assert stream.acks == []

    ack_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await ack_task

    store.save_gate.set()
    await asyncio.wait_for(stream.ack_called.wait(), timeout=1.0)

    saved = await inner_store.load(ident.identity_key)
    assert saved is not None
    assert saved.commit_end_lsn == 100
    assert saved.next_event_id == 2
    assert stream.acks == [100]
    assert driver.open_count == 1
    assert [entry for entry in log if entry.startswith("store.save:")][0] == "store.save:100:v1"

    second_ev = await source.__anext__()
    assert second_ev.event_id == 2
    assert driver.open_count == 1
    await source.aclose()


@pytest.mark.anyio
async def test_change_source_uncertain_failure_reconciles_and_discards_stale_pending() -> None:
    """Verify uncertain ack closes old generation, discards gated _pending_record, and reconciles."""
    ident = _identity()
    inner_store = MemoryPostgresCheckpointStore()
    seed = PostgresCheckpoint(
        identity_key=ident.identity_key,
        commit_end_lsn=50,
        next_event_id=10,
        version=1,
    )
    await inner_store.save(seed, expected=None)

    store = _OrderRecordingCheckpointStore(inner_store)
    stream1 = _FakeNativeStream(
        [
            PostgresReplicationRecord(kind="insert", payload={"n": 1}, commit_end_lsn=100),
            PostgresReplicationRecord(kind="insert", payload={"n": "stale"}, commit_end_lsn=125),
        ]
    )
    stream2 = _FakeNativeStream(
        [
            PostgresReplicationRecord(kind="insert", payload={"n": 2}, commit_end_lsn=150),
        ]
    )
    driver = _FakeReplicationDriver(ident, [stream1, stream2])
    source = PostgresChangeSource(
        driver,
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=True),
        store,
    )

    ev1 = await source.__anext__()
    assert ev1.event_id == 10

    stream1.next_started.clear()
    stream1.block_next_gate = asyncio.Event()
    gated_next_task: asyncio.Task[ChangeEvent] = asyncio.create_task(source.__anext__())
    await stream1.next_started.wait()

    store.save_gate = asyncio.Event()
    ack_task = asyncio.create_task(source.acknowledge(100))
    await store.save_started.wait()

    stream1.block_next_gate.set()
    await asyncio.sleep(0)

    stream1.fail_ack_exc = ConnectionError("wire ack lost")
    store.save_gate.set()
    with pytest.raises(PostgresReplicationGapError) as gap_info:
        await ack_task
    assert gap_info.value.reason == "checkpoint_uncertain"
    assert stream1.closed is True

    ev2 = await asyncio.wait_for(gated_next_task, timeout=1.0)
    assert ev2.event_id == 11
    assert ev2.payload["n"] == 2
    assert driver.open_count == 2
    assert driver.opened_args[1] == (100, False)
    await source.aclose()


@pytest.mark.anyio
async def test_change_source_cas_conflict_terminates_ownership() -> None:
    """Verify CAS checkpoint conflict closes active stream and terminates future yields/acks."""
    ident = _identity()
    inner_store = MemoryPostgresCheckpointStore()
    seed = PostgresCheckpoint(
        identity_key=ident.identity_key,
        commit_end_lsn=50,
        next_event_id=5,
        version=1,
    )
    await inner_store.save(seed, expected=None)

    stream = _FakeNativeStream(
        [
            PostgresReplicationRecord(kind="insert", payload={"n": 1}, commit_end_lsn=100),
        ]
    )
    driver = _FakeReplicationDriver(ident, [stream])
    source = PostgresChangeSource(
        driver,
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=False),
        inner_store,
    )

    ev1 = await source.__anext__()
    assert ev1.event_id == 5

    await inner_store.save(seed.advance(commit_end_lsn=80, next_event_id=6), expected=seed)

    with pytest.raises(PostgresCheckpointConflictError):
        await source.acknowledge(100)
    assert stream.closed is True

    with pytest.raises(PostgresCheckpointConflictError):
        await source.__anext__()
    with pytest.raises(PostgresCheckpointConflictError):
        await source.acknowledge(120)
    await source.aclose()


@pytest.mark.anyio
async def test_change_source_spill_and_replay_across_feed() -> None:
    """Verify PostgresChangeSource preserves ChangeFeed object spill, Parquet flush, and replay continuity."""
    ident = _identity()
    object_store = SyncObjectStore(MemoryConfig())
    cdc_config = CDCConfig(
        spill_threshold_bytes=16,
        max_history_size=8,
    )
    feed = ChangeFeed(config=cdc_config, store=object_store)
    stream = _FakeNativeStream(
        [
            PostgresReplicationRecord(
                kind="insert",
                payload={"row": 1, "blob": "x" * 64},
                commit_end_lsn=100,
            ),
            PostgresReplicationRecord(
                kind="update",
                payload={"row": 2, "blob": "y" * 64},
                commit_end_lsn=200,
            ),
        ]
    )
    driver = _FakeReplicationDriver(ident, [stream])
    source = PostgresChangeSource(
        driver,
        PostgresChangeConfig(
            slot="cdc_slot",
            publication="pub_orders",
            reconnect=False,
            cdc_config=cdc_config,
        ),
        feed=feed,
    )

    events: list[Any] = []
    async for ev in source.stream():
        events.append(ev)
        if len(events) == 2:
            break
    assert [ev.event_id for ev in events] == [1, 2]
    assert events[0].spilled is True
    assert events[1].spilled is True
    put_result = feed.flush_to_parquet("cdc/events.parquet")
    assert put_result.size > 0
    assert len(object_store.get("cdc/events.parquet")) > 0
    replayed = source.replay_since(0)
    assert [ev.event_id for ev in replayed] == [1, 2]
    await source.aclose()
    await source.aclose()
    with pytest.raises(PostgresReplicationGapError) as closed_ack_err:
        await source.acknowledge(100)
    assert closed_ack_err.value.reason == "closed"


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("error", "expected_reason"),
    [
        (RuntimeError("wal_unavailable: requested LSN was removed"), "wal_unavailable"),
        (RuntimeError("identity_mismatch: server changed"), "identity_mismatch"),
    ],
)
async def test_change_source_fail_closed_gap_errors(
    error: BaseException,
    expected_reason: str,
) -> None:
    """Verify wal_unavailable and identity_mismatch fail closed without reconnect retries."""
    ident = _identity()
    stream = _FakeNativeStream([error])
    driver = _FakeReplicationDriver(ident, [stream])
    source = PostgresChangeSource(
        driver,
        PostgresChangeConfig(
            slot="cdc_slot",
            publication="pub_orders",
            reconnect=True,
            max_reconnect_attempts=3,
            initial_backoff_seconds=0.0,
            max_backoff_seconds=0.0,
        ),
    )

    with pytest.raises(PostgresReplicationGapError) as gap_info:
        await source.__anext__()
    assert gap_info.value.reason == expected_reason
    assert driver.open_count == 1
    await source.aclose()


@pytest.mark.anyio
async def test_change_source_retries_exhausted_without_chaining_outer_exception() -> None:
    """Verify connect/EOF retry exhaustion raises retries_exhausted with clean exception chaining."""
    ident = _identity()
    driver = _FakeReplicationDriver(
        ident,
        [ConnectionError("drop 1"), ConnectionError("drop 2")],
    )
    source = PostgresChangeSource(
        driver,
        PostgresChangeConfig(
            slot="cdc_slot",
            publication="pub_orders",
            reconnect=True,
            max_reconnect_attempts=1,
            initial_backoff_seconds=0.0,
            max_backoff_seconds=0.0,
        ),
    )
    with pytest.raises(PostgresReplicationGapError) as gap_info:
        await source.__anext__()
    assert gap_info.value.reason == "retries_exhausted"
    await source.aclose()

    eof_source = PostgresChangeSource(
        _FakeReplicationDriver(ident, [_FakeNativeStream([])]),
        PostgresChangeConfig(
            slot="cdc_slot",
            publication="pub_orders",
            reconnect=True,
            max_reconnect_attempts=0,
            initial_backoff_seconds=0.0,
            max_backoff_seconds=0.0,
        ),
    )
    try:
        try:
            raise RuntimeError("unrelated outer failure")
        except RuntimeError:
            with pytest.raises(PostgresReplicationGapError) as eof_exc:
                await anext(eof_source.stream())
            assert eof_exc.value.reason == "retries_exhausted"
            assert eof_exc.value.__cause__ is None
            assert eof_exc.value.__suppress_context__ is True
    finally:
        await eof_source.aclose()


@pytest.mark.anyio
async def test_change_source_bounded_aclose_raises_when_in_flight_ack_stalls() -> None:
    """Verify bounded aclose fails closed with checkpoint_uncertain when save stalls past deadline."""
    ident = _identity()
    store = _OrderRecordingCheckpointStore(MemoryPostgresCheckpointStore())
    store.save_gate = asyncio.Event()
    stream = _FakeNativeStream(
        [PostgresReplicationRecord(kind="insert", payload={"k": 1}, commit_end_lsn=100)],
        fail_aclose_exc=OperationalError("terminating connection due to administrator command"),
    )
    source = PostgresChangeSource(
        _FakeReplicationDriver(ident, [stream]),
        PostgresChangeConfig(
            slot="cdc_slot",
            publication="pub_orders",
            reconnect=False,
            checkpoint_timeout_seconds=0.05,
        ),
        store,
    )
    await source.__anext__()
    hung_ack = asyncio.create_task(source.acknowledge(100))
    await store.save_started.wait()
    store.load_gate = asyncio.Event()
    with pytest.raises(PostgresReplicationGapError) as close_gap:
        await source.aclose()
    assert close_gap.value.reason == "checkpoint_uncertain"
    assert stream.closed is True
    store.save_gate.set()
    store.load_gate.set()
    with pytest.raises(PostgresReplicationGapError):
        await hung_ack


@pytest.mark.anyio
async def test_change_source_reconnects_when_dead_stream_aclose_raises() -> None:
    """Verify failed cleanup on a dead native stream does not prevent reconnect recovery."""
    ident = _identity()
    term_err = OperationalError("terminating connection due to administrator command")
    broken_gen = _FakeNativeStream([term_err], fail_aclose_exc=term_err)
    recovered_gen = _FakeNativeStream(
        [PostgresReplicationRecord(kind="insert", payload={"ok": True}, commit_end_lsn=200)]
    )
    driver = _FakeReplicationDriver(ident, [broken_gen, recovered_gen])
    source = PostgresChangeSource(
        driver,
        PostgresChangeConfig(
            slot="cdc_slot",
            publication="pub_orders",
            reconnect=True,
            initial_backoff_seconds=0.0,
            max_backoff_seconds=0.0,
        ),
    )
    event = await anext(source.stream())
    assert event.payload == {"kind": "insert", "generation": 0, "commit_end_lsn": 200, "ok": True}
    assert broken_gen.closed is True
    assert driver.open_count == 2
    await source.aclose()


@pytest.mark.anyio
async def test_change_source_no_reconnect_propagates_terminal_stream_error() -> None:
    """Verify reconnect=False propagates terminal native stream errors directly."""
    ident = _identity()
    term_err = OperationalError("terminating connection due to administrator command")
    source = PostgresChangeSource(
        _FakeReplicationDriver(ident, [_FakeNativeStream([term_err], fail_aclose_exc=term_err)]),
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=False),
    )
    async with source:
        with pytest.raises(OperationalError, match="administrator command"):
            await anext(source.stream())


@pytest.mark.anyio
async def test_change_source_wal_unavailable_closes_dead_stream_without_retry() -> None:
    """Verify removed WAL error fails closed with wal_unavailable and closes dead generation."""
    ident = _identity()
    term_err = OperationalError("terminating connection due to administrator command")
    gap_stream = _FakeNativeStream(
        [OperationalError("requested WAL segment has already been removed")],
        fail_aclose_exc=term_err,
    )
    driver = _FakeReplicationDriver(ident, [gap_stream])
    source = PostgresChangeSource(
        driver,
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=True),
    )
    async with source:
        with pytest.raises(PostgresReplicationGapError) as gap_err:
            await anext(source.stream())
        assert gap_err.value.reason == "wal_unavailable"
    assert gap_stream.closed is True
    assert driver.open_count == 1


@pytest.mark.anyio
async def test_change_source_wire_ack_failure_recovers_on_next_generation() -> None:
    """Verify wire ack loss marks checkpoint_uncertain and resumes cleanly on next generation."""
    ident = _identity()
    term_err = OperationalError("terminating connection due to administrator command")
    dead_stream = _FakeNativeStream(
        [PostgresReplicationRecord(kind="insert", payload={"n": 1}, commit_end_lsn=100)],
        fail_aclose_exc=term_err,
    )
    dead_stream.fail_ack_exc = ConnectionError("wire ack lost")
    recovery_stream = _FakeNativeStream(
        [PostgresReplicationRecord(kind="insert", payload={"n": 2}, commit_end_lsn=200)]
    )
    source = PostgresChangeSource(
        _FakeReplicationDriver(ident, [dead_stream, recovery_stream]),
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=True),
        MemoryPostgresCheckpointStore(),
    )
    async with source:
        await anext(source.stream())
        with pytest.raises(PostgresReplicationGapError) as ack_gap:
            await source.acknowledge(100)
        assert ack_gap.value.reason == "checkpoint_uncertain"
        assert dead_stream.closed is True
        resumed = await asyncio.wait_for(anext(source.stream()), timeout=1.0)
        assert resumed.payload == {"kind": "insert", "generation": 0, "commit_end_lsn": 200, "n": 2}


@pytest.mark.anyio
async def test_change_source_concurrent_uncertain_ack_reconciles_on_stream_eof() -> None:
    """Verify concurrent stream EOF reconciles after wire ack failure even with reconnect=False."""
    ident = _identity()
    stream1 = _FakeNativeStream([PostgresReplicationRecord(kind="insert", payload={"n": 1}, commit_end_lsn=100)])
    stream2 = _FakeNativeStream([PostgresReplicationRecord(kind="insert", payload={"n": 2}, commit_end_lsn=200)])
    source = PostgresChangeSource(
        _FakeReplicationDriver(ident, [stream1, stream2]),
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=False),
        MemoryPostgresCheckpointStore(),
    )
    await source.__anext__()
    stream1.next_started.clear()
    stream1.block_next_gate = asyncio.Event()
    stream1.fail_ack_exc = ConnectionError("ack failed")

    next_task: asyncio.Task[ChangeEvent] = asyncio.create_task(source.__anext__())
    await stream1.next_started.wait()
    with pytest.raises(PostgresReplicationGapError) as ack_err:
        await source.acknowledge(100)
    assert ack_err.value.reason == "checkpoint_uncertain"
    stream1.block_next_gate.set()
    resumed = await asyncio.wait_for(next_task, timeout=1.0)
    assert resumed.payload == {"kind": "insert", "generation": 0, "commit_end_lsn": 200, "n": 2}
    await source.aclose()


@pytest.mark.anyio
async def test_change_source_concurrent_close_during_open() -> None:
    """Verify concurrent aclose aborts pending stream open cleanly and closes late-arriving stream."""
    ident = _identity()

    class _BlockedOpenDriver(_FakeReplicationDriver):
        def __init__(self, stream: _FakeNativeStream) -> None:
            super().__init__(ident, [stream])
            self.open_started = asyncio.Event()
            self.open_gate = asyncio.Event()

        async def _open_replication_stream(
            self,
            config: PostgresChangeConfig,
            *,
            start_lsn: int | None = None,
            reconnect: bool = False,
        ) -> Any:
            self.open_started.set()
            await self.open_gate.wait()
            return await super()._open_replication_stream(
                config,
                start_lsn=start_lsn,
                reconnect=reconnect,
            )

    late_stream = _FakeNativeStream([])
    driver = _BlockedOpenDriver(late_stream)
    source = PostgresChangeSource(
        driver,
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=True, max_reconnect_attempts=0),
        MemoryPostgresCheckpointStore(),
    )
    open_task: asyncio.Task[ChangeEvent] = asyncio.create_task(source.__anext__())
    await driver.open_started.wait()
    await source.aclose()
    driver.open_gate.set()
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(open_task, timeout=1.0)
    assert late_stream.closed is True
    assert driver.open_count == 1


@pytest.mark.anyio
async def test_change_source_concurrent_close_during_next() -> None:
    """Verify concurrent aclose terminates an awaited idle stream without triggering reconnect."""
    ident = _identity()
    stream = _FakeNativeStream([])
    stream.block_next_gate = asyncio.Event()
    driver = _FakeReplicationDriver(ident, [stream])
    source = PostgresChangeSource(
        driver,
        PostgresChangeConfig(
            slot="cdc_slot",
            publication="pub_orders",
            reconnect=True,
            max_reconnect_attempts=0,
            initial_backoff_seconds=5.0,
            max_backoff_seconds=5.0,
        ),
        MemoryPostgresCheckpointStore(),
    )
    awaiting_task: asyncio.Task[ChangeEvent] = asyncio.create_task(source.__anext__())
    await stream.next_started.wait()
    await source.aclose()
    stream.block_next_gate.set()
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(awaiting_task, timeout=1.0)
    assert stream.closed is True
    assert driver.open_count == 1


@pytest.mark.anyio
async def test_change_source_cas_conflict_interrupts_blocked_next() -> None:
    """Verify CAS fencing mid-ack terminates an awaited stream iteration with conflict error."""
    ident = _identity()
    store = MemoryPostgresCheckpointStore()
    seed = PostgresCheckpoint(
        identity_key=ident.identity_key,
        commit_end_lsn=50,
        next_event_id=5,
        version=1,
    )
    await store.save(seed, expected=None)
    stream = _FakeNativeStream(
        [
            PostgresReplicationRecord(kind="insert", payload={"n": 1}, commit_end_lsn=100),
            ConnectionError("closed during CAS fence"),
        ]
    )
    source = PostgresChangeSource(
        _FakeReplicationDriver(ident, [stream]),
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=True, max_reconnect_attempts=0),
        store,
    )
    await source.__anext__()
    stream.next_started.clear()
    stream.block_next_gate = asyncio.Event()
    next_task: asyncio.Task[ChangeEvent] = asyncio.create_task(source.__anext__())
    await stream.next_started.wait()
    await store.save(seed.advance(commit_end_lsn=80, next_event_id=6), expected=seed)
    with pytest.raises(PostgresCheckpointConflictError):
        await source.acknowledge(100)
    stream.block_next_gate.set()
    with pytest.raises(PostgresCheckpointConflictError):
        await asyncio.wait_for(next_task, timeout=1.0)
    await source.aclose()


@pytest.mark.anyio
async def test_change_source_aclose_external_timeout_stops_anext_and_shields_ack() -> None:
    """Verify external wait_for cancellation of aclose shields in-flight ack and halts __anext__."""
    ident = _identity()
    store = _OrderRecordingCheckpointStore(MemoryPostgresCheckpointStore())
    store.save_gate = asyncio.Event()
    stream = _FakeNativeStream([PostgresReplicationRecord(kind="insert", payload={"id": 1}, commit_end_lsn=100)])
    source = PostgresChangeSource(
        _FakeReplicationDriver(ident, [stream]),
        PostgresChangeConfig(
            slot="cdc_slot",
            publication="pub_orders",
            checkpoint_timeout_seconds=1.0,
            reconnect=False,
        ),
        store,
    )
    await source.__anext__()
    ack_task = asyncio.create_task(source.acknowledge(100))
    await store.save_started.wait()
    blocked_next: asyncio.Task[ChangeEvent] = asyncio.create_task(source.__anext__())
    await asyncio.sleep(0)
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(source.aclose(), timeout=0.02)
    assert not ack_task.done()
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(blocked_next, timeout=1.0)
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(source.__anext__(), timeout=1.0)
    store.save_gate.set()
    saved_after_close = await asyncio.wait_for(ack_task, timeout=1.0)
    assert saved_after_close.commit_end_lsn == 100


@pytest.mark.anyio
async def test_change_source_aclose_reconciles_after_post_commit_timeout() -> None:
    """Verify aclose reconciles uncertain state after a committed-before-timeout save failure."""
    ident = _identity()
    store = _OrderRecordingCheckpointStore(MemoryPostgresCheckpointStore())
    stream = _FakeNativeStream([PostgresReplicationRecord(kind="insert", payload={"id": 1}, commit_end_lsn=100)])
    source = PostgresChangeSource(
        _FakeReplicationDriver(ident, [stream]),
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=False),
        store,
    )
    await source.__anext__()
    store.fail_after_commit = True
    store.fail_next_save = TimeoutError("post-commit ack timeout")
    with pytest.raises(PostgresReplicationGapError):
        await source.acknowledge(100)
    await source.aclose()
    assert store.load_count == 2
    assert source.checkpoint is not None
    assert source.checkpoint.commit_end_lsn == 100
    assert source.checkpoint.version == 1


@pytest.mark.anyio
async def test_change_source_aclose_reconciles_after_in_flight_ack_cancellation() -> None:
    """Verify aclose handles a cancelled pending ack task and reconciles durable checkpoint state."""
    ident = _identity()
    store = _OrderRecordingCheckpointStore(MemoryPostgresCheckpointStore())
    store.fail_next_save = asyncio.CancelledError("cancelled mid save")
    store.save_gate = asyncio.Event()
    source = PostgresChangeSource(
        _FakeReplicationDriver(
            ident,
            [_FakeNativeStream([PostgresReplicationRecord(kind="insert", payload={"id": 2}, commit_end_lsn=150)])],
        ),
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=False),
        store,
    )
    await source.__anext__()
    pending_ack = asyncio.create_task(source.acknowledge(150))
    await store.save_started.wait()
    close_task = asyncio.create_task(source.aclose())
    await asyncio.sleep(0)
    store.save_gate.set()
    await asyncio.wait_for(close_task, timeout=1.0)
    with pytest.raises(asyncio.CancelledError):
        await pending_ack
    assert store.load_count == 2


@pytest.mark.anyio
async def test_change_source_aclose_reconciles_after_pending_gap_and_conflict_errors() -> None:
    """Verify aclose reconciles uncertain pending GapError and finishes cleanly on CAS conflict."""
    ident = _identity()
    gap_store = _OrderRecordingCheckpointStore(MemoryPostgresCheckpointStore())
    gap_store.save_gate = asyncio.Event()
    gap_store.fail_after_commit = True
    gap_store.fail_next_save = TimeoutError("in-flight uncertain save")
    gap_source = PostgresChangeSource(
        _FakeReplicationDriver(
            ident,
            [_FakeNativeStream([PostgresReplicationRecord(kind="insert", payload={"id": 1}, commit_end_lsn=100)])],
        ),
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=False),
        gap_store,
    )
    await gap_source.__anext__()
    gap_ack = asyncio.create_task(gap_source.acknowledge(100))
    await gap_store.save_started.wait()
    gap_close = asyncio.create_task(gap_source.aclose())
    await asyncio.sleep(0)
    gap_store.save_gate.set()
    await asyncio.wait_for(gap_close, timeout=1.0)
    with pytest.raises(PostgresReplicationGapError):
        await gap_ack
    assert gap_store.load_count == 2
    assert gap_source.checkpoint is not None
    assert gap_source.checkpoint.commit_end_lsn == 100

    inner_conflict = MemoryPostgresCheckpointStore()
    seed = PostgresCheckpoint(identity_key=ident.identity_key, commit_end_lsn=50, next_event_id=2, version=1)
    await inner_conflict.save(seed, expected=None)
    conflict_store = _OrderRecordingCheckpointStore(inner_conflict)
    conflict_store.save_gate = asyncio.Event()
    conflict_source = PostgresChangeSource(
        _FakeReplicationDriver(
            ident,
            [_FakeNativeStream([PostgresReplicationRecord(kind="insert", payload={"id": 2}, commit_end_lsn=100)])],
        ),
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=False),
        conflict_store,
    )
    await conflict_source.__anext__()
    conflict_ack = asyncio.create_task(conflict_source.acknowledge(100))
    await conflict_store.save_started.wait()
    await inner_conflict.save(seed.advance(commit_end_lsn=75, next_event_id=3), expected=seed)
    conflict_close = asyncio.create_task(conflict_source.aclose())
    await asyncio.sleep(0)
    conflict_store.save_gate.set()
    await asyncio.wait_for(conflict_close, timeout=1.0)
    with pytest.raises(PostgresCheckpointConflictError):
        await conflict_ack
    assert conflict_store.load_count == 1


@pytest.mark.anyio
async def test_change_source_aclose_enforces_deadline_across_contended_reconcile() -> None:
    """Verify aclose fails closed within checkpoint_timeout_seconds even when _reconcile_lock is contended."""
    ident = _identity()
    store = _OrderRecordingCheckpointStore(MemoryPostgresCheckpointStore())
    source = PostgresChangeSource(
        _FakeReplicationDriver(
            ident,
            [_FakeNativeStream([PostgresReplicationRecord(kind="insert", payload={"id": 1}, commit_end_lsn=100)])],
        ),
        PostgresChangeConfig(
            slot="cdc_slot",
            publication="pub_orders",
            checkpoint_timeout_seconds=0.05,
            reconnect=False,
        ),
        store,
    )
    await source.__anext__()
    store.fail_next_save = TimeoutError("initial failure")
    with pytest.raises(PostgresReplicationGapError):
        await source.acknowledge(100)

    store.load_started.clear()
    store.load_gate = asyncio.Event()
    contending_ack = asyncio.create_task(source.acknowledge(100))
    await store.load_started.wait()
    started = asyncio.get_running_loop().time()
    with pytest.raises(PostgresReplicationGapError) as close_timeout:
        await source.aclose()
    elapsed = asyncio.get_running_loop().time() - started
    assert close_timeout.value.reason == "checkpoint_uncertain"
    assert elapsed < 0.25
    store.load_gate.set()
    with pytest.raises(TimeoutError):
        await contending_ack


@pytest.mark.anyio
async def test_change_source_aclose_preserves_caller_cancellation_on_cancelled_ack_tick() -> None:
    """Verify cancelling the task calling aclose is not masked if pending ack also cancelled."""
    ident = _identity()
    store = _OrderRecordingCheckpointStore(MemoryPostgresCheckpointStore())
    store.save_gate = asyncio.Event()
    store.fail_next_save = asyncio.CancelledError("pending ack cancelled")
    source = PostgresChangeSource(
        _FakeReplicationDriver(
            ident,
            [_FakeNativeStream([PostgresReplicationRecord(kind="insert", payload={"id": 1}, commit_end_lsn=100)])],
        ),
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=False),
        store,
    )
    await source.__anext__()
    ack_task = asyncio.create_task(source.acknowledge(100))
    await store.save_started.wait()
    close_task = asyncio.create_task(source.aclose())
    await asyncio.sleep(0)
    close_task.cancel()
    store.save_gate.set()
    with pytest.raises(asyncio.CancelledError):
        await close_task
    with pytest.raises(asyncio.CancelledError):
        await ack_task


@pytest.mark.anyio
async def test_change_source_reconcile_after_commit_preserves_monotonic_event_ids() -> None:
    """Verify post-commit timeout reconciliation preserves ChangeFeed event_id and replay continuity."""
    ident = _identity()
    store = _OrderRecordingCheckpointStore(MemoryPostgresCheckpointStore())
    s1 = _FakeNativeStream(
        [
            PostgresReplicationRecord(kind="insert", payload={"id": 1}, commit_end_lsn=100),
            PostgresReplicationRecord(kind="insert", payload={"id": 2}, commit_end_lsn=200),
        ]
    )
    s2 = _FakeNativeStream([PostgresReplicationRecord(kind="insert", payload={"id": 3}, commit_end_lsn=300)])
    source = PostgresChangeSource(
        _FakeReplicationDriver(ident, [s1, s2]),
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=True),
        store,
    )
    ev1 = await source.__anext__()
    ev2 = await source.__anext__()
    assert (ev1.event_id, ev2.event_id) == (1, 2)

    store.fail_after_commit = True
    store.fail_next_save = TimeoutError("uncertain post-commit timeout")
    with pytest.raises(PostgresReplicationGapError) as uncertain_gap:
        await source.acknowledge(100)
    assert uncertain_gap.value.reason == "checkpoint_uncertain"

    reconciled_ack = await source.acknowledge(200)
    assert reconciled_ack.version == 2
    assert reconciled_ack.commit_end_lsn == 200
    assert reconciled_ack.next_event_id == 3

    ev3 = await source.__anext__()
    assert ev3.event_id == 3
    assert [ev.event_id for ev in source.replay_since(2)] == [3]
    assert [ev.event_id for ev in source.replay_since(1)] == [2, 3]
    await source.aclose()


@pytest.mark.anyio
async def test_change_source_retry_ack_without_commit_preserves_monotonic_event_ids() -> None:
    """Verify failed pre-commit ack retry does not reset ChangeFeed event_ids backwards."""
    ident = _identity()
    store = _OrderRecordingCheckpointStore(MemoryPostgresCheckpointStore())
    s1 = _FakeNativeStream(
        [
            PostgresReplicationRecord(kind="insert", payload={"n": 1}, commit_end_lsn=100),
            PostgresReplicationRecord(kind="insert", payload={"n": 2}, commit_end_lsn=200),
        ]
    )
    s2 = _FakeNativeStream([PostgresReplicationRecord(kind="insert", payload={"n": 3}, commit_end_lsn=300)])
    source = PostgresChangeSource(
        _FakeReplicationDriver(ident, [s1, s2]),
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=True),
        store,
    )
    ev1 = await source.__anext__()
    ev2 = await source.__anext__()
    assert (ev1.event_id, ev2.event_id) == (1, 2)

    store.fail_next_save = TimeoutError("uncertain pre-commit save")
    with pytest.raises(PostgresReplicationGapError):
        await source.acknowledge(100)

    retried_ack = await source.acknowledge(200)
    assert retried_ack.version == 1
    assert retried_ack.next_event_id == 3

    ev3 = await source.__anext__()
    assert ev3.event_id == 3
    assert [ev.event_id for ev in source.replay_since(1)] == [2, 3]
    await source.aclose()


@pytest.mark.anyio
async def test_change_source_concurrent_ack_serializes_single_flight() -> None:
    """Verify concurrent uncertain acknowledge callers serialize onto a single reconcile and save."""
    ident = _identity()
    conc_log: list[str] = []
    store = _OrderRecordingCheckpointStore(MemoryPostgresCheckpointStore(), conc_log)
    s1 = _FakeNativeStream([PostgresReplicationRecord(kind="insert", payload={"id": 1}, commit_end_lsn=100)])
    source = PostgresChangeSource(
        _FakeReplicationDriver(ident, [s1]),
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=False),
        store,
    )
    await source.__anext__()
    store.fail_next_save = TimeoutError("uncertain first save")
    with pytest.raises(PostgresReplicationGapError):
        await source.acknowledge(100)

    store.load_gate = asyncio.Event()
    ack_first = asyncio.create_task(source.acknowledge(100))
    await store.load_started.wait()
    ack_second = asyncio.create_task(source.acknowledge(100))
    await asyncio.sleep(0)
    store.load_gate.set()
    cp_first, cp_second = await asyncio.wait_for(asyncio.gather(ack_first, ack_second), timeout=1.0)
    assert cp_first == cp_second
    assert cp_first.version == 1
    assert store.load_count == 2
    assert store.save_count == 2
    assert conc_log == ["store.load", "store.save:100:v1", "store.load", "store.save:100:v1"]
    await source.aclose()


@pytest.mark.anyio
async def test_change_source_acknowledge_does_not_block_on_idle_stream_next() -> None:
    """Verify acknowledge completes immediately even while __anext__ is parked on an idle stream."""
    ident = _identity()
    stream = _FakeNativeStream([PostgresReplicationRecord(kind="insert", payload={"id": 2}, commit_end_lsn=200)])
    stream.block_next_gate = asyncio.Event()
    source = PostgresChangeSource(
        _FakeReplicationDriver(ident, [stream]),
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=False),
        MemoryPostgresCheckpointStore(),
    )
    idle_next_task: asyncio.Task[ChangeEvent] = asyncio.create_task(source.__anext__())
    await stream.next_started.wait()
    idle_ack = await asyncio.wait_for(source.acknowledge(150), timeout=1.0)
    assert idle_ack.commit_end_lsn == 150
    stream.block_next_gate.set()
    idle_event = await asyncio.wait_for(idle_next_task, timeout=1.0)
    assert idle_event.payload["commit_end_lsn"] == 200
    assert idle_event.payload["id"] == 2
    await source.aclose()


@pytest.mark.anyio
async def test_change_source_concurrent_initialize_serializes_checkpoint_load() -> None:
    """Verify _init_lock prevents concurrent callers from observing uninitialized checkpoint state."""
    ident = _identity()
    seed_store = MemoryPostgresCheckpointStore()
    seeded = PostgresCheckpoint(
        identity_key=ident.identity_key,
        commit_end_lsn=100,
        next_event_id=5,
        version=1,
    )
    await seed_store.save(seeded, expected=None)

    init_store = _OrderRecordingCheckpointStore(seed_store)
    init_store.load_gate = asyncio.Event()
    s1 = _FakeNativeStream([PostgresReplicationRecord(kind="insert", payload={"id": 5}, commit_end_lsn=200)])
    init_driver = _FakeReplicationDriver(ident, [s1])
    init_source = PostgresChangeSource(
        init_driver,
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=False),
        init_store,
    )

    ack_race = asyncio.create_task(init_source.acknowledge(150))
    await init_store.load_started.wait()
    next_race: asyncio.Task[ChangeEvent] = asyncio.create_task(init_source.__anext__())
    await asyncio.sleep(0)
    init_store.load_gate.set()
    ack_cp, ev5 = await asyncio.wait_for(asyncio.gather(ack_race, next_race), timeout=1.0)

    assert init_store.load_count == 1
    assert ack_cp.version == 2
    assert ack_cp.commit_end_lsn == 150
    assert init_driver.opened_args in ([(100, False)], [(150, False)])
    assert ev5.event_id == 5
    await init_source.aclose()


@pytest.mark.anyio
async def test_change_source_initialize_bounds_stalled_store_load() -> None:
    """Verify initial checkpoint store load under _init_lock obeys checkpoint_timeout_seconds."""
    ident = _identity()
    stalled_store = _OrderRecordingCheckpointStore(MemoryPostgresCheckpointStore())
    stalled_store.load_gate = asyncio.Event()
    stalled_driver = _FakeReplicationDriver(ident, [])
    stalled_source = PostgresChangeSource(
        stalled_driver,
        PostgresChangeConfig(
            slot="cdc_slot",
            publication="pub_orders",
            checkpoint_timeout_seconds=0.05,
            reconnect=False,
        ),
        stalled_store,
    )
    with pytest.raises(TimeoutError):
        await stalled_source.__anext__()
    stalled_store.load_gate.set()
    assert stalled_driver.open_count == 0
    await stalled_source.aclose()


@pytest.mark.anyio
async def test_change_source_open_fresh_stream_reconciles_mid_connect_ack_failure() -> None:
    """Verify _open_fresh_stream discards a stream opened with stale LSN after concurrent ack failure."""
    ident = _identity()
    seed_store = MemoryPostgresCheckpointStore()
    seed = PostgresCheckpoint(
        identity_key=ident.identity_key,
        commit_end_lsn=50,
        next_event_id=3,
        version=1,
    )
    await seed_store.save(seed, expected=None)

    class _MidConnectDriver(_FakeReplicationDriver):
        def __init__(self, streams: list[_FakeNativeStream | BaseException]) -> None:
            super().__init__(ident, streams)
            self.first_open_started = asyncio.Event()
            self.first_open_gate = asyncio.Event()

        async def _open_replication_stream(
            self,
            config: PostgresChangeConfig,
            *,
            start_lsn: int | None = None,
            reconnect: bool = False,
        ) -> Any:
            if self.open_count == 0:
                self.first_open_started.set()
                await self.first_open_gate.wait()
            return await super()._open_replication_stream(
                config,
                start_lsn=start_lsn,
                reconnect=reconnect,
            )

    stale_stream = _FakeNativeStream([])
    fresh_stream = _FakeNativeStream([PostgresReplicationRecord(kind="insert", payload={"id": 3}, commit_end_lsn=120)])
    store = _OrderRecordingCheckpointStore(seed_store)
    driver = _MidConnectDriver([stale_stream, fresh_stream])
    source = PostgresChangeSource(
        driver,
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=False),
        store,
    )

    next_task: asyncio.Task[ChangeEvent] = asyncio.create_task(source.__anext__())
    await driver.first_open_started.wait()
    store.fail_after_commit = True
    store.fail_next_save = TimeoutError("concurrent ack uncertain after commit")
    with pytest.raises(PostgresReplicationGapError):
        await source.acknowledge(90)

    driver.first_open_gate.set()
    ev = await asyncio.wait_for(next_task, timeout=1.0)
    assert stale_stream.closed is True
    assert driver.opened_args == [(50, False), (90, False)]
    assert ev.event_id == 3
    assert ev.payload["commit_end_lsn"] == 120
    await source.aclose()


@pytest.mark.anyio
async def test_change_source_cancelled_ack_marks_reconcile() -> None:
    """Verify BaseException cancellation inside checkpoint save triggers reconciliation on retry."""
    ident = _identity()
    store = _OrderRecordingCheckpointStore(MemoryPostgresCheckpointStore())
    stream = _FakeNativeStream([PostgresReplicationRecord(kind="insert", payload={"id": 1}, commit_end_lsn=100)])
    source = PostgresChangeSource(
        _FakeReplicationDriver(ident, [stream]),
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=False),
        store,
    )
    await source.__anext__()
    store.fail_next_save = asyncio.CancelledError("cancelled during CAS save")
    with pytest.raises(asyncio.CancelledError):
        await source.acknowledge(100)
    reconciled = await source.acknowledge(100)
    assert store.load_count == 2
    assert reconciled.version == 1
    assert reconciled.commit_end_lsn == 100
    await source.aclose()


@pytest.mark.anyio
async def test_change_source_interruptible_backoff_wakes_immediately_on_aclose() -> None:
    """Verify long reconnect backoff terminates immediately when aclose() is called."""
    ident = _identity()
    backoff_stream = _FakeNativeStream([ConnectionError("transient drop")])
    backoff_driver = _FakeReplicationDriver(ident, [backoff_stream])
    backoff_source = PostgresChangeSource(
        backoff_driver,
        PostgresChangeConfig(
            slot="cdc_slot",
            publication="pub_orders",
            reconnect=True,
            max_reconnect_attempts=3,
            initial_backoff_seconds=30.0,
            max_backoff_seconds=30.0,
        ),
    )
    backoff_task: asyncio.Task[ChangeEvent] = asyncio.create_task(backoff_source.__anext__())
    await asyncio.wait_for(backoff_stream.aclose_called.wait(), timeout=1.0)
    await asyncio.wait_for(backoff_source.aclose(), timeout=1.0)
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(backoff_task, timeout=1.0)
    assert backoff_driver.open_count == 1


class _FakeSqlCheckpointDriver:
    """In-memory PostgreSQL SQL driver double evaluating checkpoint DDL and CAS queries."""

    def __init__(self) -> None:
        self.tables: dict[str, dict[str, tuple[int, int, int]]] = {}
        self.ddl_statements: list[str] = []
        self.queries: list[tuple[str, tuple[Any, ...] | None]] = []

    async def execute(self, sql: str, parameters: tuple[Any, ...] | None = None) -> int:
        self.queries.append((sql, parameters))
        match = re.match(r"^\s*CREATE TABLE IF NOT EXISTS ([A-Za-z0-9_.]+)\s*\(", sql)
        if match is None:
            raise AssertionError(f"Unexpected execute SQL: {sql}")
        table_name = match.group(1)
        self.ddl_statements.append(sql)
        self.tables.setdefault(table_name, {})
        return 0

    async def select_one_or_none(
        self,
        sql: str,
        parameters: tuple[Any, ...] | None = None,
    ) -> dict[str, Any] | None:
        self.queries.append((sql, parameters))
        params = parameters or ()

        select_match = re.match(
            r"^\s*SELECT commit_end_lsn, next_event_id, version FROM ([A-Za-z0-9_.]+) WHERE identity_key = \$1\s*$",
            sql,
        )
        if select_match is not None:
            table = self.tables[select_match.group(1)]
            row = table.get(str(params[0]))
            if row is None:
                return None
            return {
                "commit_end_lsn": row[0],
                "next_event_id": row[1],
                "version": row[2],
            }

        insert_match = re.match(
            r"^\s*INSERT INTO ([A-Za-z0-9_.]+) "
            r"\(identity_key, commit_end_lsn, next_event_id, version\) "
            r"VALUES \(\$1, \$2, \$3, 1\) "
            r"ON CONFLICT \(identity_key\) DO NOTHING "
            r"RETURNING version\s*$",
            sql,
        )
        if insert_match is not None:
            table = self.tables[insert_match.group(1)]
            key = str(params[0])
            if key in table:
                return None
            table[key] = (int(params[1]), int(params[2]), 1)
            return {"version": 1}

        update_match = re.match(
            r"^\s*UPDATE ([A-Za-z0-9_.]+) "
            r"SET commit_end_lsn = \$2, next_event_id = \$3, version = version \+ 1 "
            r"WHERE identity_key = \$1 "
            r"AND version = \$4 "
            r"AND commit_end_lsn <= \$2 "
            r"AND next_event_id <= \$3 "
            r"RETURNING version\s*$",
            sql,
        )
        if update_match is not None:
            table = self.tables[update_match.group(1)]
            key = str(params[0])
            existing = table.get(key)
            if existing is None:
                return None
            cur_lsn, cur_event_id, cur_version = existing
            new_lsn = int(params[1])
            new_event_id = int(params[2])
            expected_version = int(params[3])
            if cur_version != expected_version or cur_lsn > new_lsn or cur_event_id > new_event_id:
                return None
            next_version = cur_version + 1
            table[key] = (new_lsn, new_event_id, next_version)
            return {"version": next_version}

        raise AssertionError(f"Unexpected select_one_or_none SQL: {sql}")


@pytest.mark.anyio
async def test_sql_checkpoint_store_export_protocol_and_ddl() -> None:
    """Verify SqlPostgresCheckpointStore export, protocol conformance, DDL, and single-flight ensure_table."""
    assert ExportedSqlPostgresCheckpointStore is SqlPostgresCheckpointStore
    driver = _FakeSqlCheckpointDriver()
    store = SqlPostgresCheckpointStore(driver, table_name="custom_cdc_checkpoints")
    assert isinstance(store, PostgresCheckpointStore)
    ddl = store.ddl()
    assert ddl == (
        "CREATE TABLE IF NOT EXISTS custom_cdc_checkpoints ("
        "identity_key TEXT PRIMARY KEY, "
        "commit_end_lsn BIGINT NOT NULL, "
        "next_event_id BIGINT NOT NULL, "
        "version BIGINT NOT NULL DEFAULT 1)"
    )

    await asyncio.gather(store.ensure_table(), store.ensure_table(), store.ensure_table())
    assert len(driver.ddl_statements) == 1
    assert "custom_cdc_checkpoints" in driver.tables


@pytest.mark.parametrize(
    "bad_table",
    [
        "",
        "   ",
        "checkpoints; DROP TABLE users",
        "bad-table",
        "1invalid_start",
        "public..checkpoints",
    ],
)
def test_sql_checkpoint_store_rejects_invalid_table_name(bad_table: str) -> None:
    """Verify invalid or SQL-injection table names fail loudly at construction."""
    driver = _FakeSqlCheckpointDriver()
    with pytest.raises(ValueError):
        SqlPostgresCheckpointStore(driver, table_name=bad_table)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "bad_key",
    [
        ("sys", "", "cdc_slot", "pub"),
        ("sys", "appdb", "bad.slot", "pub"),
        ("sys", "appdb", "slot; DROP TABLE x", "pub"),
        ("sys", "appdb", "cdc_slot"),
    ],
)
async def test_sql_checkpoint_store_rejects_malformed_identity_keys(bad_key: Any) -> None:
    """Verify malformed IdentityKey tuples and invalid slot names are rejected cleanly."""
    store = SqlPostgresCheckpointStore(_FakeSqlCheckpointDriver())
    with pytest.raises(ValueError):
        await store.load(bad_key)


@pytest.mark.parametrize(
    "bad_raw",
    [
        "not-json",
        '{"a": 1}',
        '["sys", "appdb", "cdc_slot"]',
        '["sys", "", "cdc_slot", "pub"]',
        '["sys", "appdb", "bad.slot", "pub"]',
    ],
)
def test_decode_identity_key_rejects_malformed_payloads(bad_raw: str) -> None:
    """Verify _decode_identity_key rejects non-array, short, empty, or unsafe payloads."""
    with pytest.raises(ValueError):
        _decode_identity_key(bad_raw)


@pytest.mark.anyio
async def test_sql_checkpoint_store_identity_key_punctuation_roundtrip() -> None:
    """Verify canonical JSON array encoding keeps punctuated IdentityKeys collision-free."""
    key_a = ("sys:1|prod", "app.db/v2,main", "cdc_slot_a", "pub=orders:v1,2")
    key_b = ("sys:1|prod,app.db/v2", "main", "cdc_slot_a", "pub=orders:v1,2")
    encoded_a = _encode_identity_key(key_a)
    encoded_b = _encode_identity_key(key_b)
    assert encoded_a != encoded_b
    assert _decode_identity_key(encoded_a) == key_a
    assert _decode_identity_key(encoded_b) == key_b

    store = SqlPostgresCheckpointStore(_FakeSqlCheckpointDriver())
    cp_a = await store.save(PostgresCheckpoint(identity_key=key_a, commit_end_lsn=10, next_event_id=2))
    cp_b = await store.save(PostgresCheckpoint(identity_key=key_b, commit_end_lsn=20, next_event_id=5))
    assert await store.load(key_a) == cp_a
    assert await store.load(key_b) == cp_b


@pytest.mark.anyio
async def test_sql_checkpoint_store_cas_insert_update_and_conflicts() -> None:
    """Verify SqlPostgresCheckpointStore enforces CAS insert, update, version, and monotonicity."""
    driver = _FakeSqlCheckpointDriver()
    store = SqlPostgresCheckpointStore(driver)
    ident = _identity()

    assert await store.load(ident.identity_key) is None

    bad_initial = PostgresCheckpoint(
        identity_key=ident.identity_key,
        commit_end_lsn=100,
        next_event_id=2,
        version=2,
    )
    with pytest.raises(PostgresCheckpointConflictError):
        await store.save(bad_initial, expected=None)

    first = PostgresCheckpoint(
        identity_key=ident.identity_key,
        commit_end_lsn=100,
        next_event_id=2,
        version=1,
    )
    saved_first = await store.save(first, expected=None)
    assert saved_first == first
    assert await store.load(ident.identity_key) == saved_first

    with pytest.raises(PostgresCheckpointConflictError) as dup_exc:
        await store.save(first, expected=None)
    assert dup_exc.value.sqlstate == "40001"

    second = saved_first.advance(commit_end_lsn=200, next_event_id=3)
    saved_second = await store.save(second, expected=saved_first)
    assert saved_second.version == 2
    assert saved_second.commit_end_lsn == 200
    assert saved_second.next_event_id == 3
    assert await store.load(ident.identity_key) == saved_second

    with pytest.raises(PostgresCheckpointConflictError):
        await store.save(saved_first.advance(commit_end_lsn=250, next_event_id=4), expected=saved_first)

    other_key = _identity(slot="other_slot").identity_key
    with pytest.raises(PostgresCheckpointConflictError):
        await store.save(
            PostgresCheckpoint(identity_key=other_key, commit_end_lsn=300, next_event_id=4, version=3),
            expected=saved_second,
        )

    with pytest.raises(PostgresCheckpointConflictError):
        await store.save(
            PostgresCheckpoint(identity_key=ident.identity_key, commit_end_lsn=250, next_event_id=4, version=4),
            expected=saved_second,
        )

    with pytest.raises(PostgresCheckpointConflictError):
        await store.save(
            PostgresCheckpoint(identity_key=ident.identity_key, commit_end_lsn=150, next_event_id=4, version=3),
            expected=saved_second,
        )

    with pytest.raises(PostgresCheckpointConflictError):
        await store.save(
            PostgresCheckpoint(identity_key=ident.identity_key, commit_end_lsn=250, next_event_id=2, version=3),
            expected=saved_second,
        )

    stale_expected = PostgresCheckpoint(
        identity_key=ident.identity_key,
        commit_end_lsn=100,
        next_event_id=2,
        version=2,
    )
    with pytest.raises(PostgresCheckpointConflictError):
        await store.save(
            PostgresCheckpoint(identity_key=ident.identity_key, commit_end_lsn=180, next_event_id=3, version=3),
            expected=stale_expected,
        )


@pytest.mark.anyio
async def test_change_source_resumes_from_sql_checkpoint_store() -> None:
    """Verify PostgresChangeSource persists and resumes next_event_id continuity via SqlPostgresCheckpointStore."""
    sql_driver = _FakeSqlCheckpointDriver()
    store_a = SqlPostgresCheckpointStore(sql_driver)
    ident = _identity()

    stream_a = _FakeNativeStream(
        [
            PostgresReplicationRecord(kind="insert", payload={"id": 1}, commit_end_lsn=100),
            PostgresReplicationRecord(kind="update", payload={"id": 2}, commit_end_lsn=200),
        ]
    )
    source_a = PostgresChangeSource(
        _FakeReplicationDriver(ident, [stream_a]),
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=False),
        store_a,
    )
    ev1 = await source_a.__anext__()
    ev2 = await source_a.__anext__()
    assert (ev1.event_id, ev2.event_id) == (1, 2)
    await source_a.acknowledge(200)
    await source_a.aclose()

    persisted = await store_a.load(ident.identity_key)
    assert persisted == PostgresCheckpoint(
        identity_key=ident.identity_key,
        commit_end_lsn=200,
        next_event_id=3,
        version=1,
    )

    store_b = SqlPostgresCheckpointStore(sql_driver)
    stream_b = _FakeNativeStream(
        [
            PostgresReplicationRecord(kind="delete", payload={"id": 3}, commit_end_lsn=350),
        ]
    )
    driver_b = _FakeReplicationDriver(ident, [stream_b])
    source_b = PostgresChangeSource(
        driver_b,
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=False),
        store_b,
    )
    ev3 = await source_b.__anext__()
    assert driver_b.opened_args == [(200, False)]
    assert ev3.event_id == 3
    assert ev3.operation == "delete"
    await source_b.acknowledge(350)
    await source_b.aclose()

    resumed = await store_b.load(ident.identity_key)
    assert resumed == PostgresCheckpoint(
        identity_key=ident.identity_key,
        commit_end_lsn=350,
        next_event_id=4,
        version=2,
    )
