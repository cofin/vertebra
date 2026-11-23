"""PostgreSQL event queue store and LISTEN/NOTIFY event backends over native notifications."""

from __future__ import annotations

import asyncio
import collections
import contextlib
import threading
import time
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, ClassVar

from sqlspec.extensions.events import (
    MAX_NOTIFY_BYTES,
    AsyncEventBackendProtocol,
    BaseEventQueueStore,
    EventMessage,
    SyncEventBackendProtocol,
    decode_notify_payload,
    encode_notify_payload,
    fits_notify_payload,
    normalize_event_channel_name,
)
from sqlspec.migrations.schema import DropStatement, SchemaTarget, metadata_has_index

from vertebra.db.adapters.postgres.notify import AsyncPostgresNotificationSource, SyncPostgresNotificationSource
from vertebra.extensions.sqlspec._driver import _build_companion_index
from vertebra.extensions.sqlspec.adapters.postgres.core import (
    pg_ensure_utc,
    pg_index_name,
    pg_json_dumps,
    pg_json_loads,
    pg_row_value,
    pg_table_ref,
    postgres_storage_params_sql,
    validate_pg_table_name,
)
from vertebra.utils.serializers import decode_json, encode_json

if TYPE_CHECKING:
    from sqlspec.extensions.litestar.store import CompanionIndex

    from vertebra.extensions.sqlspec.adapters.postgres.config import PostgresAsyncConfig, PostgresSyncConfig

__all__ = (
    "PostgresAsyncEventsBackend",
    "PostgresEventQueueStore",
    "PostgresSyncEventsBackend",
)

_SPILL_KEY = "__vtb_spilled__"


def _encode_spill_wakeup(event_id: str) -> str:
    """Encode a compact LISTEN/NOTIFY wakeup pointer referencing a spilled queue row."""
    raw = encode_json({"event_id": event_id, _SPILL_KEY: True})
    return raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)


def _decode_spill_marker(raw_payload: Any) -> str | None:
    """Return spilled `event_id` if `raw_payload` is a compact wakeup marker."""
    if isinstance(raw_payload, dict) and raw_payload.get(_SPILL_KEY):
        ev_id = raw_payload.get("event_id")
        return str(ev_id) if ev_id else None
    if isinstance(raw_payload, (str, bytes, bytearray)):
        try:
            decoded = decode_json(bytes(raw_payload) if isinstance(raw_payload, bytearray) else raw_payload)
        except (TypeError, ValueError):
            return None
        if isinstance(decoded, dict) and decoded.get(_SPILL_KEY):
            ev_id = decoded.get("event_id")
            return str(ev_id) if ev_id else None
    return None


class PostgresEventQueueStore(BaseEventQueueStore["PostgresSyncConfig | PostgresAsyncConfig"]):
    """PostgreSQL durable event queue store using JSONB and TIMESTAMPTZ columns."""

    __slots__ = ()

    extension_config_options = BaseEventQueueStore.extension_config_options | frozenset(
        {
            "autovacuum_analyze_scale_factor",
            "autovacuum_vacuum_scale_factor",
            "fillfactor",
            "listener_queue_capacity",
        }
    )

    def _split_table_name(self) -> tuple[str | None, str]:
        parts = validate_pg_table_name(self.table_name)
        if len(parts) == 2:
            return parts[0], parts[1]
        return None, parts[0]

    def _column_types(self) -> tuple[str, str, str]:
        return "JSONB", "JSONB", "TIMESTAMPTZ"

    def _string_type(self, length: int) -> str:
        return f"VARCHAR({length})"

    def _index_name(self) -> str:
        return pg_index_name("idx", self.table_name, "channel_status")

    def _index_ddl(self) -> str:
        table_ref = pg_table_ref(self.table_name)
        idx_name = self._index_name()
        return (
            f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_ref} (channel, status, available_at) "
            "WHERE status = 'pending';"
        )

    def _index_existence_target(self) -> tuple[str | None, str]:
        return self._split_table_name()

    def _wrap_create_statement(self, statement: str, object_type: str) -> str:
        if object_type != "table":
            return statement
        tuning = postgres_storage_params_sql(
            self.settings,
            default_fillfactor=80,
            default_vacuum_scale=0.05,
            default_analyze_scale=0.02,
            include_autovacuum=True,
        )
        if not tuning:
            return statement
        return f"{statement.rstrip(';')}{tuning};"

    def create_statements(self) -> list[str]:
        """Build idempotent PostgreSQL DDL for the event queue table and partial pending index."""
        table_ref = pg_table_ref(self.table_name)
        table_sql = self._wrap_create_statement(
            f"CREATE TABLE IF NOT EXISTS {table_ref} ("
            "event_id VARCHAR(64) PRIMARY KEY, "
            "channel VARCHAR(128) NOT NULL, "
            "payload_json JSONB NOT NULL, "
            "metadata_json JSONB, "
            "status VARCHAR(32) NOT NULL DEFAULT 'pending', "
            "available_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, "
            "lease_expires_at TIMESTAMPTZ, "
            "attempts INTEGER NOT NULL DEFAULT 0, "
            "created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP"
            ");",
            "table",
        )
        return [table_sql, self._index_ddl()]

    def drop_statements(self) -> list[str]:
        """Return PostgreSQL DROP statements for queue index and table."""
        table_ref = pg_table_ref(self.table_name)
        idx_name = self._index_name()
        return [f"DROP INDEX IF EXISTS {idx_name};", f"DROP TABLE IF EXISTS {table_ref};"]

    def _schema_target(self) -> SchemaTarget:
        schema_name, bare_table = self._split_table_name()
        return SchemaTarget.from_ddl(
            bare_table,
            self.create_statements()[0],
            schema=schema_name,
            dialect="postgres",
        )

    def get_schema_targets(self) -> list[SchemaTarget]:
        """Return ordered `SchemaTarget` definitions for the PostgreSQL event queue table."""
        return [self._schema_target()]

    def companion_index_statement(self) -> str:
        """Return PostgreSQL partial pending-index DDL for the event queue table."""
        return self._index_ddl()

    def get_companion_indexes(self) -> list[CompanionIndex]:
        """Return companion partial index descriptors for the PostgreSQL event queue table."""
        schema_name, bare_table = self._split_table_name()
        return [
            _build_companion_index(
                name=self._index_name(),
                table=bare_table,
                schema=schema_name,
                ddl=self.companion_index_statement(),
            )
        ]

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Create missing companion partial index on a synchronous driver without committing."""
        for index in self.get_companion_indexes():
            rows = driver.data_dictionary.get_indexes(driver, table=index.table, schema=index.schema)
            if not metadata_has_index(rows, index.table, index.name, schema_name=index.schema):
                driver.execute_script(index.ddl)

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Create missing companion partial index on an asynchronous driver without committing."""
        for index in self.get_companion_indexes():
            rows = await driver.data_dictionary.get_indexes(driver, table=index.table, schema=index.schema)
            if not metadata_has_index(rows, index.table, index.name, schema_name=index.schema):
                await driver.execute_script(index.ddl)

    def downgrade_drops(self) -> list[DropStatement]:
        """Return downgrade drop statements for the PostgreSQL event queue index and table."""
        return [DropStatement(statement) for statement in self.drop_statements()]


class PostgresSyncEventsBackend(SyncEventBackendProtocol):
    """Synchronous PostgreSQL event backend combining LISTEN/NOTIFY with `EventQueueStore` spill."""

    __slots__ = (
        "_closed",
        "_cond",
        "_config",
        "_owns_source",
        "_pump_done",
        "_pump_error",
        "_pump_thread",
        "_queue",
        "_raw_queues",
        "_source",
        "_spilled_buffers",
        "_subscribed",
        "_unacked",
    )

    supports_sync: ClassVar[bool] = True
    backend_name: ClassVar[str] = "postgres_listen_notify"

    def __init__(
        self,
        config: PostgresSyncConfig,
        *,
        source: SyncPostgresNotificationSource | None = None,
        queue: PostgresEventQueueStore | None = None,
    ) -> None:
        """Initialize the synchronous PostgreSQL LISTEN/NOTIFY + table-spill backend."""
        self._config = config
        self._source = source
        self._owns_source = False
        self._queue = queue or PostgresEventQueueStore(config)
        self._subscribed: set[str] = set()
        self._cond = threading.Condition()
        self._raw_queues: dict[str, collections.deque[Any]] = collections.defaultdict(collections.deque)
        self._pump_thread: threading.Thread | None = None
        self._pump_done = False
        self._pump_error: BaseException | None = None
        self._spilled_buffers: dict[str, collections.deque[EventMessage]] = collections.defaultdict(collections.deque)
        self._unacked: dict[str, tuple[str, EventMessage]] = {}
        self._closed = False

    def _ensure_source(self) -> SyncPostgresNotificationSource:
        if self._source is not None:
            return self._source
        direct_cfg = self._config.derive_direct_config()
        driver_cls = direct_cfg._driver_cls
        native_driver = driver_cls(direct_cfg.driver_config)
        self._source = SyncPostgresNotificationSource(native_driver)
        self._owns_source = True
        return self._source

    def _persist_spilled_row(
        self, event_id: str, channel: str, payload: dict[str, Any], metadata: dict[str, Any] | None
    ) -> None:
        table_ref = pg_table_ref(self._queue.table_name)
        sql = (
            f"INSERT INTO {table_ref} (event_id, channel, payload_json, metadata_json, status, available_at) "
            "VALUES ($1, $2, $3::jsonb, $4::jsonb, 'pending', CURRENT_TIMESTAMP) "
            "ON CONFLICT (event_id) DO NOTHING"
        )
        with self._config.provide_session() as driver:
            driver.execute(
                sql,
                event_id,
                channel,
                pg_json_dumps(payload),
                pg_json_dumps(metadata) if metadata is not None else None,
            )
            driver.commit()

    def _hydrate_spilled_row(self, event_id: str, channel: str) -> EventMessage | None:
        table_ref = pg_table_ref(self._queue.table_name)
        with self._config.provide_session() as driver:
            row = driver.select_one_or_none(
                f"SELECT event_id, channel, payload_json, metadata_json, attempts, available_at, created_at "
                f"FROM {table_ref} WHERE event_id = $1",
                event_id,
            )
            if row is not None:
                driver.execute(
                    f"UPDATE {table_ref} SET status = 'leased', attempts = attempts + 1 WHERE event_id = $1",
                    event_id,
                )
                driver.commit()
        if row is None:
            return None
        payload = pg_json_loads(pg_row_value(row, "payload_json", 2))
        metadata = pg_json_loads(pg_row_value(row, "metadata_json", 3))
        return EventMessage(
            event_id=str(pg_row_value(row, "event_id", 0)),
            channel=str(pg_row_value(row, "channel", 1) or channel),
            payload=payload if isinstance(payload, dict) else {"value": payload},
            metadata=metadata if isinstance(metadata, dict) else None,
            attempts=int(pg_row_value(row, "attempts", 4) or 0) + 1,
            available_at=pg_ensure_utc(pg_row_value(row, "available_at", 5)),
            lease_expires_at=None,
            created_at=pg_ensure_utc(pg_row_value(row, "created_at", 6)),
        )

    def publish(self, channel: str, payload: dict[str, Any], metadata: dict[str, Any] | None = None) -> str:
        """Publish an event over `pg_notify`, spilling frames over `MAX_NOTIFY_BYTES` to `EventQueueStore`."""
        if self._closed:
            msg = "PostgresSyncEventsBackend has been shut down"
            raise RuntimeError(msg)
        norm_channel = normalize_event_channel_name(channel)
        event_id = uuid.uuid4().hex
        if (
            fits_notify_payload(payload, metadata, event_id=event_id)
            and len(encode_notify_payload(event_id, payload, metadata)) <= MAX_NOTIFY_BYTES
        ):
            encoded = encode_notify_payload(event_id, payload, metadata)
        else:
            self._persist_spilled_row(event_id, norm_channel, payload, metadata)
            encoded = _encode_spill_wakeup(event_id)

        if self._source is not None:
            self._source.publish(norm_channel, encoded)
        else:
            with self._config.provide_session() as driver:
                driver.execute("SELECT pg_notify($1, $2)", norm_channel, encoded)
                driver.commit()
        return event_id

    def publish_many(self, events: Sequence[tuple[str, dict[str, Any], dict[str, Any] | None]]) -> list[str]:
        """Publish multiple channel payloads preserving input event order."""
        return [self.publish(channel, payload, metadata) for channel, payload, metadata in events]

    def _pump_sync_stream(self, source: SyncPostgresNotificationSource) -> None:
        try:
            for event in source.listen():
                if self._closed:
                    break
                ev_ch = normalize_event_channel_name(event.channel) if getattr(event, "channel", None) else ""
                with self._cond:
                    self._raw_queues[ev_ch].append(event)
                    self._cond.notify_all()
        except BaseException as exc:
            with self._cond:
                self._pump_error = exc
                self._cond.notify_all()
        finally:
            with self._cond:
                self._pump_done = True
                self._cond.notify_all()

    def dequeue(self, channel: str, poll_interval: float) -> EventMessage | None:
        """Listen for notifications on `channel`, hydrating any table-spilled payloads."""
        if self._closed:
            return None
        norm_channel = normalize_event_channel_name(channel)
        buffered = self._spilled_buffers.get(norm_channel)
        if buffered:
            msg = buffered.popleft()
            self._unacked[msg.event_id] = (norm_channel, msg)
            return msg

        source = self._ensure_source()
        if norm_channel not in self._subscribed:
            source.subscribe([norm_channel])
            self._subscribed.add(norm_channel)

        event: Any = None
        with self._cond:
            raw_q = self._raw_queues[norm_channel]
            if raw_q:
                event = raw_q.popleft()
            else:
                if self._pump_thread is None or not self._pump_thread.is_alive():
                    self._pump_done = False
                    self._pump_error = None
                    self._pump_thread = threading.Thread(
                        target=self._pump_sync_stream,
                        args=(source,),
                        daemon=True,
                    )
                    self._pump_thread.start()
                if poll_interval <= 0:
                    if not raw_q and not self._pump_done:
                        self._cond.wait(timeout=0.01)
                else:
                    deadline = time.monotonic() + float(poll_interval)
                    while not raw_q and not self._pump_done and not self._closed:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            break
                        self._cond.wait(timeout=remaining)
                if self._pump_error is not None:
                    err = self._pump_error
                    self._pump_error = None
                    raise err
                if raw_q:
                    event = raw_q.popleft()
        if event is None:
            return None

        raw_payload = event.payload
        spill_id = _decode_spill_marker(raw_payload)
        if spill_id is not None:
            hydrated = self._hydrate_spilled_row(spill_id, norm_channel)
            if hydrated is None:
                return None
            self._unacked[hydrated.event_id] = (norm_channel, hydrated)
            return hydrated

        if isinstance(raw_payload, dict) and "event_id" in raw_payload and "payload" in raw_payload:
            ev_id = str(raw_payload["event_id"])
            body = raw_payload["payload"]
            meta = raw_payload.get("metadata")
            now = datetime.now(UTC)
            msg = EventMessage(
                event_id=ev_id,
                channel=event.channel or norm_channel,
                payload=body if isinstance(body, dict) else {"value": body},
                metadata=meta if isinstance(meta, dict) else None,
                attempts=1,
                available_at=now,
                lease_expires_at=None,
                created_at=now,
            )
            self._unacked[msg.event_id] = (norm_channel, msg)
            return msg

        text = raw_payload if isinstance(raw_payload, str) else pg_json_dumps(raw_payload)
        decoded = decode_notify_payload(event.channel or norm_channel, text)
        now = datetime.now(UTC)
        msg = EventMessage(
            event_id=decoded.event_id,
            channel=event.channel or norm_channel,
            payload=decoded.payload if isinstance(decoded.payload, dict) else {"value": decoded.payload},
            metadata=decoded.metadata if isinstance(decoded.metadata, dict) else None,
            attempts=1,
            available_at=now,
            lease_expires_at=None,
            created_at=now,
        )
        self._unacked[msg.event_id] = (norm_channel, msg)
        return msg

    def ack(self, event_id: str) -> None:
        """Acknowledge delivery of `event_id` and mark any spilled queue row acknowledged."""
        self._unacked.pop(event_id, None)
        table_ref = pg_table_ref(self._queue.table_name)
        with contextlib.suppress(Exception), self._config.provide_session() as driver:
            driver.execute(f"UPDATE {table_ref} SET status = 'acked' WHERE event_id = $1", event_id)
            driver.commit()

    def nack(self, event_id: str) -> None:
        """Return an unacknowledged message to the channel queue for immediate redelivery."""
        entry = self._unacked.pop(event_id, None)
        if entry is None:
            return
        channel, msg = entry
        msg.attempts += 1
        self._spilled_buffers[channel].append(msg)

    def shutdown(self) -> None:
        """Close owned PostgreSQL notification listeners and clear buffered deliveries."""
        with self._cond:
            self._closed = True
            self._cond.notify_all()
        if self._source is not None and self._owns_source:
            with contextlib.suppress(Exception):
                self._source.close()
        if self._pump_thread is not None and self._pump_thread.is_alive():
            self._pump_thread.join(timeout=0.2)
        with self._cond:
            self._pump_thread = None
            self._raw_queues.clear()
            self._unacked.clear()
            self._spilled_buffers.clear()


class PostgresAsyncEventsBackend(AsyncEventBackendProtocol):
    """Asynchronous PostgreSQL event backend combining LISTEN/NOTIFY with `EventQueueStore` spill."""

    __slots__ = (
        "_channel_queues",
        "_closed",
        "_config",
        "_owns_source",
        "_pump_error",
        "_pump_task",
        "_queue",
        "_source",
        "_spilled_buffers",
        "_subscribed",
        "_unacked",
    )

    supports_async: ClassVar[bool] = True
    backend_name: ClassVar[str] = "postgres_listen_notify"

    def __init__(
        self,
        config: PostgresAsyncConfig,
        *,
        source: AsyncPostgresNotificationSource | None = None,
        queue: PostgresEventQueueStore | None = None,
    ) -> None:
        """Initialize the asynchronous PostgreSQL LISTEN/NOTIFY + table-spill backend."""
        self._config = config
        self._source = source
        self._owns_source = False
        self._queue = queue or PostgresEventQueueStore(config)
        self._subscribed: set[str] = set()
        self._channel_queues: dict[str, asyncio.Queue[Any]] = {}
        self._pump_task: asyncio.Task[None] | None = None
        self._pump_error: BaseException | None = None
        self._spilled_buffers: dict[str, collections.deque[EventMessage]] = collections.defaultdict(collections.deque)
        self._unacked: dict[str, tuple[str, EventMessage]] = {}
        self._closed = False

    def _ensure_source(self) -> AsyncPostgresNotificationSource:
        if self._source is not None:
            return self._source
        direct_cfg = self._config.derive_direct_config()
        driver_cls = direct_cfg._driver_cls
        native_driver = driver_cls(direct_cfg.driver_config)
        self._source = AsyncPostgresNotificationSource(native_driver)
        self._owns_source = True
        return self._source

    async def _persist_spilled_row(
        self, event_id: str, channel: str, payload: dict[str, Any], metadata: dict[str, Any] | None
    ) -> None:
        table_ref = pg_table_ref(self._queue.table_name)
        sql = (
            f"INSERT INTO {table_ref} (event_id, channel, payload_json, metadata_json, status, available_at) "
            "VALUES ($1, $2, $3::jsonb, $4::jsonb, 'pending', CURRENT_TIMESTAMP) "
            "ON CONFLICT (event_id) DO NOTHING"
        )
        async with self._config.provide_session() as driver:
            await driver.execute(
                sql,
                event_id,
                channel,
                pg_json_dumps(payload),
                pg_json_dumps(metadata) if metadata is not None else None,
            )
            await driver.commit()

    async def _hydrate_spilled_row(self, event_id: str, channel: str) -> EventMessage | None:
        table_ref = pg_table_ref(self._queue.table_name)
        async with self._config.provide_session() as driver:
            row = await driver.select_one_or_none(
                f"SELECT event_id, channel, payload_json, metadata_json, attempts, available_at, created_at "
                f"FROM {table_ref} WHERE event_id = $1",
                event_id,
            )
            if row is not None:
                await driver.execute(
                    f"UPDATE {table_ref} SET status = 'leased', attempts = attempts + 1 WHERE event_id = $1",
                    event_id,
                )
                await driver.commit()
        if row is None:
            return None
        payload = pg_json_loads(pg_row_value(row, "payload_json", 2))
        metadata = pg_json_loads(pg_row_value(row, "metadata_json", 3))
        return EventMessage(
            event_id=str(pg_row_value(row, "event_id", 0)),
            channel=str(pg_row_value(row, "channel", 1) or channel),
            payload=payload if isinstance(payload, dict) else {"value": payload},
            metadata=metadata if isinstance(metadata, dict) else None,
            attempts=int(pg_row_value(row, "attempts", 4) or 0) + 1,
            available_at=pg_ensure_utc(pg_row_value(row, "available_at", 5)),
            lease_expires_at=None,
            created_at=pg_ensure_utc(pg_row_value(row, "created_at", 6)),
        )

    async def publish(self, channel: str, payload: dict[str, Any], metadata: dict[str, Any] | None = None) -> str:
        """Asynchronously publish an event via `pg_notify`, spilling oversized payloads."""
        if self._closed:
            msg = "PostgresAsyncEventsBackend has been shut down"
            raise RuntimeError(msg)
        norm_channel = normalize_event_channel_name(channel)
        event_id = uuid.uuid4().hex
        if (
            fits_notify_payload(payload, metadata, event_id=event_id)
            and len(encode_notify_payload(event_id, payload, metadata)) <= MAX_NOTIFY_BYTES
        ):
            encoded = encode_notify_payload(event_id, payload, metadata)
        else:
            await self._persist_spilled_row(event_id, norm_channel, payload, metadata)
            encoded = _encode_spill_wakeup(event_id)

        if self._source is not None:
            await self._source.publish(norm_channel, encoded)
        else:
            async with self._config.provide_session() as driver:
                await driver.execute("SELECT pg_notify($1, $2)", norm_channel, encoded)
                await driver.commit()
        return event_id

    async def publish_many(self, events: Sequence[tuple[str, dict[str, Any], dict[str, Any] | None]]) -> list[str]:
        """Asynchronously publish multiple events preserving input order."""
        ids: list[str] = []
        for channel, payload, metadata in events:
            ids.append(await self.publish(channel, payload, metadata))
        return ids

    async def _pump_stream(self, source: AsyncPostgresNotificationSource) -> None:
        try:
            async for event in source.listen():
                if self._closed:
                    break
                ev_ch = normalize_event_channel_name(event.channel) if getattr(event, "channel", None) else ""
                q = self._channel_queues.setdefault(ev_ch, asyncio.Queue())
                q.put_nowait(event)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            self._pump_error = exc

    async def dequeue(self, channel: str, poll_interval: float) -> EventMessage | None:
        """Asynchronously dequeue a notification event on `channel`, hydrating spilled rows."""
        if self._closed:
            return None
        norm_channel = normalize_event_channel_name(channel)
        buffered = self._spilled_buffers.get(norm_channel)
        if buffered:
            msg = buffered.popleft()
            self._unacked[msg.event_id] = (norm_channel, msg)
            return msg

        source = self._ensure_source()
        if norm_channel not in self._subscribed:
            await source.subscribe([norm_channel])
            self._subscribed.add(norm_channel)
        q = self._channel_queues.setdefault(norm_channel, asyncio.Queue())

        event: Any = None
        if not q.empty():
            event = q.get_nowait()
        else:
            if self._pump_task is None or self._pump_task.done():
                if self._pump_error is not None:
                    err = self._pump_error
                    self._pump_error = None
                    raise err
                self._pump_task = asyncio.create_task(self._pump_stream(source))
            if poll_interval <= 0:
                await asyncio.sleep(0)
                if self._pump_error is not None:
                    err = self._pump_error
                    self._pump_error = None
                    raise err
                if not q.empty():
                    event = q.get_nowait()
            else:
                get_task = asyncio.create_task(q.get())
                done, _ = await asyncio.wait(
                    {get_task, self._pump_task},
                    timeout=poll_interval,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if get_task in done:
                    event = get_task.result()
                else:
                    get_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await get_task
                    if self._pump_error is not None:
                        err = self._pump_error
                        self._pump_error = None
                        raise err
                    if not q.empty():
                        event = q.get_nowait()
        if event is None:
            return None

        raw_payload = event.payload
        spill_id = _decode_spill_marker(raw_payload)
        if spill_id is not None:
            hydrated = await self._hydrate_spilled_row(spill_id, norm_channel)
            if hydrated is None:
                return None
            self._unacked[hydrated.event_id] = (norm_channel, hydrated)
            return hydrated

        if isinstance(raw_payload, dict) and "event_id" in raw_payload and "payload" in raw_payload:
            ev_id = str(raw_payload["event_id"])
            body = raw_payload["payload"]
            meta = raw_payload.get("metadata")
            now = datetime.now(UTC)
            msg = EventMessage(
                event_id=ev_id,
                channel=event.channel or norm_channel,
                payload=body if isinstance(body, dict) else {"value": body},
                metadata=meta if isinstance(meta, dict) else None,
                attempts=1,
                available_at=now,
                lease_expires_at=None,
                created_at=now,
            )
            self._unacked[msg.event_id] = (norm_channel, msg)
            return msg

        text = raw_payload if isinstance(raw_payload, str) else pg_json_dumps(raw_payload)
        decoded = decode_notify_payload(event.channel or norm_channel, text)
        now = datetime.now(UTC)
        msg = EventMessage(
            event_id=decoded.event_id,
            channel=event.channel or norm_channel,
            payload=decoded.payload if isinstance(decoded.payload, dict) else {"value": decoded.payload},
            metadata=decoded.metadata if isinstance(decoded.metadata, dict) else None,
            attempts=1,
            available_at=now,
            lease_expires_at=None,
            created_at=now,
        )
        self._unacked[msg.event_id] = (norm_channel, msg)
        return msg

    async def ack(self, event_id: str) -> None:
        """Asynchronously acknowledge event delivery."""
        self._unacked.pop(event_id, None)
        table_ref = pg_table_ref(self._queue.table_name)
        with contextlib.suppress(Exception):
            async with self._config.provide_session() as driver:
                await driver.execute(f"UPDATE {table_ref} SET status = 'acked' WHERE event_id = $1", event_id)
                await driver.commit()

    async def nack(self, event_id: str) -> None:
        """Asynchronously return an unacknowledged message for redelivery."""
        entry = self._unacked.pop(event_id, None)
        if entry is None:
            return
        channel, msg = entry
        msg.attempts += 1
        self._spilled_buffers[channel].append(msg)

    async def shutdown(self) -> None:
        """Close owned async PostgreSQL listeners and clear buffered events."""
        self._closed = True
        if self._pump_task is not None:
            self._pump_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._pump_task
            self._pump_task = None
        self._channel_queues.clear()
        self._unacked.clear()
        self._spilled_buffers.clear()
        if self._source is not None and self._owns_source:
            with contextlib.suppress(Exception):
                await self._source.aclose()
