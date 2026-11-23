"""Shared store-backed helpers for SQL task queue backends."""

from __future__ import annotations

import asyncio
import inspect
import time
from typing import TYPE_CHECKING, Any, Protocol, cast, runtime_checkable

import msgspec

from vertebra.db.driver.capabilities import QueueEntry
from vertebra.framework.queues.base import (
    CronLease,
    DeadLetterEntry,
    QueueBackendSettings,
    QueueConfig,
    TaskEnvelope,
    TaskLane,
)
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.serializers import decode_json, encode_json_bytes

if TYPE_CHECKING:
    from collections.abc import Awaitable, Sequence

    from vertebra.db.driver.capabilities import AsyncQueueStore


@runtime_checkable
class _DeadLetterPopCapable(Protocol):
    """Protocol for queue stores providing atomic dead-letter pop operations."""

    def pop_dead_letter(self, job_id: str) -> QueueEntry | Awaitable[QueueEntry | None] | None:
        """Atomically remove and return a dead-lettered queue entry."""
        ...


def envelope_to_queue_entry(envelope: TaskEnvelope) -> QueueEntry:
    """Convert a ``TaskEnvelope`` into a driver-level ``QueueEntry``."""
    headers: dict[str, str] = {}
    if envelope.idempotency_key:
        headers["idempotency_key"] = envelope.idempotency_key
    if envelope.traceparent:
        headers["traceparent"] = envelope.traceparent
    sched_ms = int(envelope.scheduled_at * 1000) if envelope.scheduled_at is not None else None
    return QueueEntry(
        job_id=envelope.task_id,
        queue=envelope.lane,
        task_name=envelope.task_name,
        payload=encode_json_bytes(envelope),
        attempt=envelope.attempt,
        max_attempts=envelope.max_retries,
        scheduled_at_ms=sched_ms,
        headers=headers,
    )


def decode_envelope_bytes(payload: Any) -> TaskEnvelope:
    """Decode a JSON `str` or `bytes` column payload into a `TaskEnvelope`."""
    payload_data = payload.encode("utf-8") if isinstance(payload, str) else payload
    return msgspec.convert(decode_json(payload_data), type=TaskEnvelope)


def decode_entry_envelope(
    entry: QueueEntry, *, receipt_handle: str | None = None, attempt_override: int | None = None
) -> TaskEnvelope:
    """Decode a `QueueEntry` payload into a `TaskEnvelope`, falling back on invalid JSON."""
    attempt = entry.attempt if attempt_override is None else attempt_override
    try:
        env = msgspec.convert(decode_json(entry.payload), type=TaskEnvelope)
        if receipt_handle is not None:
            return msgspec.structs.replace(env, attempt=attempt, receipt_handle=receipt_handle)
        return env
    except Exception:
        return TaskEnvelope(
            task_id=entry.job_id,
            task_name=entry.task_name,
            lane=cast("TaskLane", entry.queue),
            args=(),
            kwargs={},
            attempt=attempt,
            max_retries=entry.max_attempts,
            receipt_handle=receipt_handle,
        )


def rows_to_envelopes(rows: Sequence[dict[str, Any]]) -> list[TaskEnvelope]:
    """Hydrate claimed executor row mappings into `TaskEnvelope` instances."""
    out: list[TaskEnvelope] = []
    for row in rows:
        env = decode_envelope_bytes(row["payload"])
        if row.get("receipt_handle"):
            env = msgspec.structs.replace(
                env,
                receipt_handle=row["receipt_handle"],
                attempt=row.get("attempt", env.attempt),
            )
        out.append(env)
    return out


def rows_to_dlq_entries(rows: Sequence[dict[str, Any]]) -> list[DeadLetterEntry]:
    """Hydrate executor DLQ query rows into `DeadLetterEntry` instances."""
    return [
        DeadLetterEntry(
            dlq_id=row["dlq_id"],
            envelope=decode_envelope_bytes(row["payload"]),
            error_type=row["error_type"],
            error_message=row["error_message"],
            failed_at=row["failed_at"],
        )
        for row in rows
    ]


def error_type_and_message(error: Exception | str) -> tuple[str, str]:
    """Split an exception or string into ``(error_type, error_message)``."""
    return type(error).__name__ if isinstance(error, Exception) else "TaskExecutionError", str(error)


def unpack_queue_settings(
    config: QueueConfig | QueueBackendSettings | None,
    store: Any,
    sync_store: Any,
    driver: Any,
    sync_driver: Any,
) -> tuple[Any, Any, Any, Any]:
    """Overlay store/driver fields from `QueueBackendSettings` when not explicitly passed."""
    if isinstance(config, QueueBackendSettings):
        if store is None and config.store is not None:
            store = config.store
        if sync_store is None and config.sync_store is not None:
            sync_store = config.sync_store
        if driver is None and config.driver is not None:
            driver = config.driver
        if sync_driver is None and config.sync_driver is not None:
            sync_driver = config.sync_driver
    return store, sync_store, driver, sync_driver


async def pop_dead_letter_entry(store: AsyncQueueStore, dlq_id: str) -> QueueEntry | None:
    """Invoke ``pop_dead_letter`` when supported by ``store``, otherwise fall back to ``get_entry``."""
    if isinstance(store, _DeadLetterPopCapable):
        result = store.pop_dead_letter(dlq_id)
        if inspect.isawaitable(result):
            return await result
        return result
    return await store.get_entry(dlq_id)


class _SqlQueueBackendBase:
    """Shared store-backed operations across PostgreSQL and Oracle task queue backends."""

    __slots__ = ()

    _config: QueueConfig | QueueBackendSettings
    _store: Any
    _sync_store: Any
    _executor: Any
    _sync_store_label: str = "SyncQueueStore"

    def enqueue_sync(self, envelope: TaskEnvelope) -> TaskEnvelope:
        """Enqueue a task envelope synchronously via the bound `SyncQueueStore`."""
        if self._sync_store is None:
            msg = f"Synchronous enqueue requires a configured {self._sync_store_label}"
            raise ImproperConfigurationError(msg)
        self._sync_store.enqueue(envelope_to_queue_entry(envelope))
        return envelope

    async def _store_enqueue(self, envelope: TaskEnvelope) -> TaskEnvelope:
        await self._store.enqueue(envelope_to_queue_entry(envelope))
        return envelope

    async def _store_dequeue(
        self, lanes: Sequence[TaskLane], *, batch_size: int, timeout_seconds: float
    ) -> list[TaskEnvelope]:
        lane_names = [str(lane) for lane in lanes]
        entries = await self._store.claim(
            lane_names,
            limit=batch_size,
            worker_id=self._config.consumer_name,
            lease_seconds=self._config.visibility_timeout_seconds,
        )
        if not entries and timeout_seconds > 0:
            await asyncio.sleep(min(timeout_seconds, self._config.poll_interval_seconds))
            entries = await self._store.claim(
                lane_names,
                limit=batch_size,
                worker_id=self._config.consumer_name,
                lease_seconds=self._config.visibility_timeout_seconds,
            )
        return [decode_entry_envelope(entry, receipt_handle=f"{entry.job_id}:{entry.attempt}") for entry in entries]

    async def _store_nack(
        self,
        envelope: TaskEnvelope,
        *,
        error: Exception | str,
        delay_seconds: float | None,
        send_to_dlq: bool,
    ) -> DeadLetterEntry | None:
        err_type, err_msg = error_type_and_message(error)
        if send_to_dlq or envelope.attempt >= envelope.max_retries:
            await self._store.nack(
                envelope.task_id,
                attempt=envelope.attempt,
                error=f"{err_type}: {err_msg}",
                retry_delay_s=None,
            )
            return DeadLetterEntry(
                dlq_id=envelope.task_id,
                envelope=envelope,
                error_type=err_type,
                error_message=err_msg,
                failed_at=time.time(),
            )
        await self._store.nack(
            envelope.task_id,
            attempt=envelope.attempt,
            error=f"{err_type}: {err_msg}",
            retry_delay_s=delay_seconds or 1.0,
        )
        return None

    async def _store_list_dlq(self, limit: int) -> list[DeadLetterEntry]:
        default_lane = str(getattr(self._config, "default_lane", "io"))
        lanes_to_scan = tuple(dict.fromkeys(("cpu", "io", "agent", default_lane)))
        dead_entries: list[QueueEntry] = []
        for lane_name in lanes_to_scan:
            remaining = limit - len(dead_entries)
            if remaining <= 0:
                break
            dead_entries.extend(await self._store.list_entries(lane_name, status="dead_letter", limit=remaining))
        now = time.time()
        return [
            DeadLetterEntry(
                dlq_id=de.job_id,
                envelope=decode_entry_envelope(de),
                error_type="DeadLetterError",
                error_message="Job exceeded max retries",
                failed_at=now,
            )
            for de in dead_entries
        ]

    async def _store_replay_dlq(self, dlq_id: str) -> TaskEnvelope | None:
        entry = await pop_dead_letter_entry(self._store, dlq_id)
        if entry is None:
            return None
        env = decode_entry_envelope(entry, attempt_override=1)
        idem_key = env.idempotency_key or entry.headers.get("idempotency_key")
        if idem_key:
            await self._store.release_unique_key(idem_key)
        replayed = msgspec.structs.replace(env, attempt=1, scheduled_at=None, receipt_handle=None)
        return cast("TaskEnvelope | None", await cast("Any", self).enqueue(replayed))

    async def _store_acquire_lease(
        self, lease_key: str, owner_id: str, ttl_seconds: float, *, slot: str | None = None
    ) -> CronLease | None:
        if await self._store.acquire_lease(lease_key, owner_id=owner_id, ttl_seconds=ttl_seconds):
            return CronLease(
                lease_key=lease_key,
                owner_id=owner_id,
                expires_at=time.time() + ttl_seconds,
                last_slot=slot,
            )
        return None

    async def renew_lease(
        self, lease_key: str, owner_id: str, ttl_seconds: float, *, slot: str | None = None
    ) -> CronLease | None:
        """Renew lease matching active owner."""
        return cast(
            "CronLease | None", await cast("Any", self).acquire_lease(lease_key, owner_id, ttl_seconds, slot=slot)
        )

    async def close(self) -> None:
        """Close backing stores and executor."""
        if self._store is not None:
            await self._store.aclose()
        if self._sync_store is not None:
            self._sync_store.close()
        if self._executor is not None:
            await self._executor.close()
