"""Unit tests for PostgreSQL EventSessionRegistry and PostgresChangeSource."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from vertebra.db.adapters.postgres.change_stream import (
    MemoryPostgresCheckpointStore,
    PostgresChangeConfig,
    PostgresChangeSource,
    PostgresCheckpoint,
    PostgresCheckpointConflictError,
    PostgresReplicationGapError,
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
        self.aclose_calls = 0
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
        self.aclose_calls += 1
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
        identity_error: BaseException | None = None,
    ) -> None:
        self._config = PostgreSQLConfig(dsn="postgresql://postgres@127.0.0.1:5432/app")
        self._identity = identity
        self._streams = list(streams)
        self._identity_error = identity_error
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
        if self._identity_error is not None:
            raise self._identity_error
        return self._identity

    async def _open_replication_stream(
        self,
        config: PostgresChangeConfig,
        *,
        start_lsn: int | None = None,
        reconnect: bool = False,
    ) -> Any:
        self.calls.append(f"open:{start_lsn}:{reconnect}")
        self.opened_args.append((start_lsn, reconnect))
        self.open_count += 1
        entry = self._streams.pop(0)
        if isinstance(entry, BaseException):
            raise entry
        return entry


class _OrderRecordingCheckpointStore:
    """Wrapper store recording load/save order and injecting controlled failures."""

    def __init__(self, inner: MemoryPostgresCheckpointStore, log: list[str]) -> None:
        self._inner = inner
        self._log = log
        self.save_started = asyncio.Event()
        self.save_gate: asyncio.Event | None = None
        self.fail_next_save: BaseException | None = None
        self.fail_load: BaseException | None = None

    async def load(self, identity_key: tuple[str, str, str, str]) -> PostgresCheckpoint | None:
        self._log.append("store.load")
        if self.fail_load is not None:
            raise self.fail_load
        return await self._inner.load(identity_key)

    async def save(
        self,
        checkpoint: PostgresCheckpoint,
        *,
        expected: PostgresCheckpoint | None,
    ) -> PostgresCheckpoint:
        self._log.append(f"store.save:{checkpoint.commit_end_lsn}:v{checkpoint.version}")
        self.save_started.set()
        if self.save_gate is not None:
            await self.save_gate.wait()
        if self.fail_next_save is not None:
            exc = self.fail_next_save
            self.fail_next_save = None
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
    log: list[str] = []
    store = _OrderRecordingCheckpointStore(inner_store, log)
    stream = _FakeNativeStream(
        [
            PostgresReplicationRecord(
                kind="insert",
                payload={"table": "orders", "toast": PG_UNCHANGED_TOAST},
                commit_end_lsn=128,
            ),
        ]
    )
    driver = _FakeReplicationDriver(ident, [stream])
    assert isinstance(driver, PostgresReplicationDriver)
    config = PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=False)
    source = PostgresChangeSource(driver, config, store)
    assert isinstance(source, AsyncChangeSource)

    event = await source.__anext__()
    assert event.operation == "insert"
    assert event.event_id == 1
    assert driver.opened_args == [(None, False)]
    assert stream.acks == []
    assert driver.calls[0].startswith("identity:")
    assert "store.load" in log
    assert log.index("store.load") == 0

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
    """Verify uncertain ack/save closes old stream generation and reconciles from durable store."""
    ident = _identity()
    inner_store = MemoryPostgresCheckpointStore()
    seed = PostgresCheckpoint(
        identity_key=ident.identity_key,
        commit_end_lsn=50,
        next_event_id=10,
        version=1,
    )
    await inner_store.save(seed, expected=None)

    log: list[str] = []
    store = _OrderRecordingCheckpointStore(inner_store, log)
    stream1 = _FakeNativeStream(
        [
            PostgresReplicationRecord(kind="insert", payload={"n": 1}, commit_end_lsn=100),
        ]
    )
    stream1.fail_ack_exc = ConnectionError("wire ack lost")
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

    with pytest.raises(PostgresReplicationGapError) as gap_info:
        await source.acknowledge(100)
    assert gap_info.value.reason == "checkpoint_uncertain"
    assert stream1.closed is True

    ev2 = await source.__anext__()
    assert ev2.event_id == 11
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
async def test_change_source_retries_exhausted_and_bounded_uncertain_aclose() -> None:
    """Verify reconnect exhaustion raises retries_exhausted and bounded aclose raises checkpoint_uncertain."""
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

    inner_store = MemoryPostgresCheckpointStore()
    store = _OrderRecordingCheckpointStore(inner_store, [])
    store.save_gate = asyncio.Event()
    stream = _FakeNativeStream([PostgresReplicationRecord(kind="insert", payload={"k": 1}, commit_end_lsn=100)])
    driver2 = _FakeReplicationDriver(ident, [stream])
    source2 = PostgresChangeSource(
        driver2,
        PostgresChangeConfig(
            slot="cdc_slot",
            publication="pub_orders",
            reconnect=False,
            checkpoint_timeout_seconds=0.05,
        ),
        store,
    )
    await source2.__anext__()
    hung_ack = asyncio.create_task(source2.acknowledge(100))
    await store.save_started.wait()
    with pytest.raises(PostgresReplicationGapError) as close_gap:
        await source2.aclose()
    assert close_gap.value.reason == "checkpoint_uncertain"
    assert stream.closed is True
    store.save_gate.set()
    with pytest.raises(PostgresReplicationGapError):
        await hung_ack


@pytest.mark.anyio
async def test_change_source_reconnects_and_closes_cleanly_when_dead_stream_aclose_raises() -> None:
    """Verify terminal stream errors stored on dead native generations do not block reconnect or close."""
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

    failing_driver = _FakeReplicationDriver(ident, [_FakeNativeStream([term_err], fail_aclose_exc=term_err)])
    no_retry_source = PostgresChangeSource(
        failing_driver,
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=False),
    )
    async with no_retry_source:
        with pytest.raises(OperationalError, match="administrator command"):
            await anext(no_retry_source.stream())

    gap_stream = _FakeNativeStream(
        [OperationalError("requested WAL segment has already been removed")],
        fail_aclose_exc=term_err,
    )
    gap_source = PostgresChangeSource(
        _FakeReplicationDriver(ident, [gap_stream]),
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=True),
    )
    async with gap_source:
        with pytest.raises(PostgresReplicationGapError) as gap_err:
            await anext(gap_source.stream())
        assert gap_err.value.reason == "wal_unavailable"
    assert gap_stream.closed is True

    ack_dead_stream = _FakeNativeStream(
        [PostgresReplicationRecord(kind="insert", payload={"n": 1}, commit_end_lsn=100)],
        fail_aclose_exc=term_err,
    )
    ack_dead_stream.fail_ack_exc = ConnectionError("wire ack lost")
    ack_recovery_stream = _FakeNativeStream(
        [PostgresReplicationRecord(kind="insert", payload={"n": 2}, commit_end_lsn=200)]
    )
    ack_source = PostgresChangeSource(
        _FakeReplicationDriver(ident, [ack_dead_stream, ack_recovery_stream]),
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=True),
        MemoryPostgresCheckpointStore(),
    )
    async with ack_source:
        await anext(ack_source.stream())
        with pytest.raises(PostgresReplicationGapError) as ack_gap:
            await ack_source.acknowledge(100)
        assert ack_gap.value.reason == "checkpoint_uncertain"
        assert ack_dead_stream.closed is True
        resumed = await asyncio.wait_for(anext(ack_source.stream()), timeout=1.0)
        assert resumed.payload == {"kind": "insert", "generation": 0, "commit_end_lsn": 200, "n": 2}


@pytest.mark.anyio
async def test_change_source_eof_retries_reconcile_and_aclose_timeout_error_preservation() -> None:
    """Verify None EOF retry cap, concurrent ack uncertainty before EOF, and aclose error preservation."""
    ident = _identity()

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
    async with eof_source:
        with pytest.raises(PostgresReplicationGapError) as eof_gap:
            await anext(eof_source.stream())
        assert eof_gap.value.reason == "retries_exhausted"

    store = _OrderRecordingCheckpointStore(MemoryPostgresCheckpointStore(), [])
    reconcile_stream1 = _FakeNativeStream(
        [PostgresReplicationRecord(kind="insert", payload={"n": 1}, commit_end_lsn=100)]
    )
    reconcile_stream2 = _FakeNativeStream(
        [PostgresReplicationRecord(kind="insert", payload={"n": 2}, commit_end_lsn=200)]
    )
    conc_source = PostgresChangeSource(
        _FakeReplicationDriver(ident, [reconcile_stream1, reconcile_stream2]),
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", reconnect=False),
        store,
    )
    await conc_source.__anext__()
    reconcile_stream1.next_started.clear()
    reconcile_stream1.block_next_gate = asyncio.Event()
    reconcile_stream1.fail_ack_exc = ConnectionError("ack failed")

    next_task: asyncio.Task[ChangeEvent] = asyncio.create_task(conc_source.__anext__())
    await reconcile_stream1.next_started.wait()
    with pytest.raises(PostgresReplicationGapError) as conc_ack_err:
        await conc_source.acknowledge(100)
    assert conc_ack_err.value.reason == "checkpoint_uncertain"
    reconcile_stream1.block_next_gate.set()
    resumed_after_eof = await asyncio.wait_for(next_task, timeout=1.0)
    assert resumed_after_eof.payload == {"kind": "insert", "generation": 0, "commit_end_lsn": 200, "n": 2}
    await conc_source.aclose()

    hung_store = _OrderRecordingCheckpointStore(MemoryPostgresCheckpointStore(), [])
    hung_store.save_gate = asyncio.Event()
    dead_acl_stream = _FakeNativeStream(
        [PostgresReplicationRecord(kind="insert", payload={"n": 3}, commit_end_lsn=300)],
        fail_aclose_exc=OperationalError("terminating connection due to administrator command"),
    )
    timeout_source = PostgresChangeSource(
        _FakeReplicationDriver(ident, [dead_acl_stream]),
        PostgresChangeConfig(slot="cdc_slot", publication="pub_orders", checkpoint_timeout_seconds=0.05),
        hung_store,
    )
    await anext(timeout_source.stream())
    hung_ack = asyncio.create_task(timeout_source.acknowledge(300))
    await hung_store.save_started.wait()
    with pytest.raises(PostgresReplicationGapError) as close_gap:
        await timeout_source.aclose()
    assert close_gap.value.reason == "checkpoint_uncertain"
    assert dead_acl_stream.closed is True
    hung_store.save_gate.set()
    with pytest.raises(PostgresReplicationGapError):
        await hung_ack


@pytest.mark.anyio
async def test_change_source_concurrent_close_and_fencing_during_open_and_next() -> None:
    """Verify concurrent aclose or CAS fencing interrupts pending open and next waits cleanly."""
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
    blocked_driver = _BlockedOpenDriver(late_stream)
    open_close_source = PostgresChangeSource(
        blocked_driver,
        PostgresChangeConfig(
            slot="cdc_slot",
            publication="pub_orders",
            reconnect=True,
            max_reconnect_attempts=0,
        ),
        MemoryPostgresCheckpointStore(),
    )
    open_task: asyncio.Task[ChangeEvent] = asyncio.create_task(open_close_source.__anext__())
    await blocked_driver.open_started.wait()
    await open_close_source.aclose()
    blocked_driver.open_gate.set()
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(open_task, timeout=1.0)
    assert late_stream.closed is True
    assert blocked_driver.open_count == 1

    blocking_stream = _FakeNativeStream([])
    blocking_stream.block_next_gate = asyncio.Event()
    next_close_driver = _FakeReplicationDriver(ident, [blocking_stream])
    next_close_source = PostgresChangeSource(
        next_close_driver,
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
    awaiting_next_task: asyncio.Task[ChangeEvent] = asyncio.create_task(next_close_source.__anext__())
    await blocking_stream.next_started.wait()
    await next_close_source.aclose()
    blocking_stream.block_next_gate.set()
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(awaiting_next_task, timeout=1.0)
    assert blocking_stream.closed is True
    assert next_close_driver.open_count == 1

    store = MemoryPostgresCheckpointStore()
    seed = PostgresCheckpoint(
        identity_key=ident.identity_key,
        commit_end_lsn=50,
        next_event_id=5,
        version=1,
    )
    await store.save(seed, expected=None)
    fenced_stream = _FakeNativeStream(
        [
            PostgresReplicationRecord(kind="insert", payload={"n": 1}, commit_end_lsn=100),
            ConnectionError("closed during CAS fence"),
        ]
    )
    fenced_source = PostgresChangeSource(
        _FakeReplicationDriver(ident, [fenced_stream]),
        PostgresChangeConfig(
            slot="cdc_slot",
            publication="pub_orders",
            reconnect=True,
            max_reconnect_attempts=0,
        ),
        store,
    )
    await fenced_source.__anext__()
    fenced_stream.next_started.clear()
    fenced_stream.block_next_gate = asyncio.Event()
    fenced_next_task: asyncio.Task[ChangeEvent] = asyncio.create_task(fenced_source.__anext__())
    await fenced_stream.next_started.wait()
    await store.save(seed.advance(commit_end_lsn=80, next_event_id=6), expected=seed)
    with pytest.raises(PostgresCheckpointConflictError):
        await fenced_source.acknowledge(100)
    fenced_stream.block_next_gate.set()
    with pytest.raises(PostgresCheckpointConflictError):
        await asyncio.wait_for(fenced_next_task, timeout=1.0)
    await fenced_source.aclose()

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
            assert eof_exc.value.__cause__ is None
            assert eof_exc.value.__suppress_context__ is True
    finally:
        await eof_source.aclose()


@pytest.mark.anyio
async def test_change_source_anext_after_aclose_timeout_and_interruptible_backoff() -> None:
    """Verify timed-out aclose never spins __anext__ and backoff wakes immediately on stop."""
    ident = _identity()
    store = _OrderRecordingCheckpointStore(MemoryPostgresCheckpointStore(), [])
    store.save_gate = asyncio.Event()
    stream = _FakeNativeStream([PostgresReplicationRecord(kind="insert", payload={"id": 1}, commit_end_lsn=100)])
    source = PostgresChangeSource(
        _FakeReplicationDriver(ident, [stream]),
        PostgresChangeConfig(
            slot="cdc_slot",
            publication="pub_orders",
            checkpoint_timeout_seconds=0.02,
            reconnect=False,
        ),
        store,
    )
    await source.__anext__()
    ack_task = asyncio.create_task(source.acknowledge(100))
    await store.save_started.wait()
    blocked_next: asyncio.Task[ChangeEvent] = asyncio.create_task(source.__anext__())
    await asyncio.sleep(0)
    with pytest.raises(PostgresReplicationGapError) as close_err:
        await source.aclose()
    assert close_err.value.reason == "checkpoint_uncertain"
    assert not ack_task.done()
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(blocked_next, timeout=1.0)
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(source.__anext__(), timeout=1.0)
    store.save_gate.set()
    with pytest.raises(PostgresReplicationGapError):
        await ack_task

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
