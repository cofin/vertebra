"""Aggregated queue backend exports and first-party shared-memory queue implementation."""

from __future__ import annotations

import asyncio
import time
import urllib.parse
import uuid
from typing import TYPE_CHECKING, Any, cast

from vertebra._vertebra import ShmQueueRing
from vertebra.framework.queues.base import (
    CronLease,
    DeadLetterEntry,
    MemoryQueueBackend,
    QueueBackend,
    QueueBackendSettings,
    QueueConfig,
    TaskEnvelope,
    TaskLane,
    TaskResultRecord,
)
from vertebra.framework.queues.oracle import OracleQueueBackend, OracleQueueExecutor
from vertebra.framework.queues.postgres import PostgreSQLQueueBackend, PostgreSQLQueueExecutor
from vertebra.framework.queues.redis import RedisQueueBackend
from vertebra.framework.queues.spanner import SpannerQueueBackend, SpannerQueueExecutor
from vertebra.framework.queues.valkey import ValkeyQueueBackend
from vertebra.utils.serializers import decode_json, encode_json_bytes

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = (
    "MemoryQueueBackend",
    "OracleQueueBackend",
    "OracleQueueExecutor",
    "PostgreSQLQueueBackend",
    "PostgreSQLQueueExecutor",
    "QueueBackend",
    "RedisQueueBackend",
    "SHMQueueBackend",
    "SpannerQueueBackend",
    "SpannerQueueExecutor",
    "ValkeyQueueBackend",
)

_VALID_LANES: frozenset[str] = frozenset({"cpu", "io", "agent"})


def _resolve_ring_name(config: QueueConfig | QueueBackendSettings, ring_name: str | None) -> str:
    if ring_name:
        return ring_name
    if isinstance(config, QueueBackendSettings):
        if config.dsn:
            parsed = urllib.parse.urlparse(config.dsn)
            segment = f"{parsed.netloc}{parsed.path}".strip("/").replace("/", "-")
            if segment:
                return segment
        return "vertebra-framework-queue"
    if config.backend_url.startswith("shm://"):
        segment = config.backend_url.removeprefix("shm://").strip("/").replace("/", "-")
        if segment:
            return segment
    return config.stream_prefix or "vertebra-framework-queue"


def _envelope_to_dict(
    envelope: TaskEnvelope,
    *,
    status: str,
    seq: int,
    locked_until: float | None = None,
    expires_at: float | None = None,
) -> dict[str, Any]:
    return {
        "task_id": envelope.task_id,
        "task_name": envelope.task_name,
        "lane": envelope.lane,
        "args": list(envelope.args),
        "kwargs": dict(envelope.kwargs),
        "attempt": envelope.attempt,
        "max_retries": envelope.max_retries,
        "enqueued_at": envelope.enqueued_at,
        "scheduled_at": envelope.scheduled_at,
        "idempotency_key": envelope.idempotency_key,
        "traceparent": envelope.traceparent,
        "receipt_handle": envelope.receipt_handle,
        "status": status,
        "seq": seq,
        "locked_until": locked_until,
        "expires_at": expires_at,
    }


def _dict_to_envelope(
    data: dict[str, Any], *, receipt_handle: str | None = None, attempt: int | None = None
) -> TaskEnvelope:
    lane_raw = str(data.get("lane", "io"))
    lane: TaskLane = cast("TaskLane", lane_raw if lane_raw in _VALID_LANES else "io")
    return TaskEnvelope(
        task_id=str(data["task_id"]),
        task_name=str(data["task_name"]),
        lane=lane,
        args=tuple(data.get("args", ())),
        kwargs=dict(data.get("kwargs", {})),
        attempt=int(data.get("attempt", 1) if attempt is None else attempt),
        max_retries=int(data.get("max_retries", 3)),
        enqueued_at=float(data.get("enqueued_at", 0.0)),
        scheduled_at=data.get("scheduled_at"),
        idempotency_key=data.get("idempotency_key"),
        traceparent=data.get("traceparent"),
        receipt_handle=data.get("receipt_handle") if receipt_handle is None else receipt_handle,
    )


class SHMQueueBackend:
    """First-party shared-memory queue backend backed by Rust ShmQueueRing."""

    __slots__ = (
        "_capacity",
        "_config",
        "_ring",
        "_ring_name",
        "_slot_bytes",
        "_visibility_timeout_seconds",
    )

    def __init__(
        self,
        config: QueueConfig | QueueBackendSettings | None = None,
        *,
        ring_name: str | None = None,
        capacity: int = 4096,
        slot_bytes: int = 8192,
    ) -> None:
        """Initialize the shared-memory queue backend with cross-process ring storage."""
        self._config = config or QueueBackendSettings(kind="shm")
        self._ring_name = _resolve_ring_name(self._config, ring_name)
        self._capacity = capacity
        self._slot_bytes = slot_bytes
        self._ring: ShmQueueRing | None = ShmQueueRing(
            self._ring_name,
            capacity=self._capacity,
            slot_bytes=self._slot_bytes,
        )
        self._visibility_timeout_seconds = self._config.visibility_timeout_seconds

    @property
    def ring_name(self) -> str:
        """Return the logical shared-memory ring name."""
        return self._ring_name

    @property
    def ring(self) -> ShmQueueRing:
        """Return the active ShmQueueRing instance, reopening if closed."""
        if self._ring is None:
            self._ring = ShmQueueRing(
                self._ring_name,
                capacity=self._capacity,
                slot_bytes=self._slot_bytes,
            )
        return self._ring

    @property
    def visibility_timeout_seconds(self) -> float:
        """Return the visibility timeout in seconds."""
        return self._visibility_timeout_seconds

    def _emit_wakeup(self) -> None:
        ring = self.ring
        try:
            ring.push(b"\x01")
        except OverflowError:
            ring.wake(False)

    def _drain_wakeups(self) -> None:
        ring = self.ring
        while ring.pop() is not None:
            pass

    async def enqueue(self, envelope: TaskEnvelope) -> TaskEnvelope:
        """Enqueue an envelope into cross-process SHM storage, waking waiters when due."""
        ring = self.ring
        if envelope.idempotency_key:
            reserved = ring.cas_record(
                f"idem:{envelope.idempotency_key}",
                None,
                envelope.task_id.encode("utf-8"),
            )
            if not reserved:
                return envelope

        now = time.time()
        is_scheduled = envelope.scheduled_at is not None and envelope.scheduled_at > now
        status = "scheduled" if is_scheduled else "ready"
        payload = _envelope_to_dict(envelope, status=status, seq=time.time_ns())
        ring.put_record(f"task:{envelope.task_id}", encode_json_bytes(payload))
        if not is_scheduled:
            self._emit_wakeup()
        return envelope

    async def dequeue(
        self,
        lanes: Sequence[TaskLane],
        *,
        batch_size: int = 1,
        timeout_seconds: float = 0.0,
    ) -> list[TaskEnvelope]:
        """Claim ready tasks across lanes with atomic cross-process CAS fencing."""
        await self.promote_scheduled()
        ring = self.ring
        now = time.time()
        claimed: list[TaskEnvelope] = []
        target_lanes = set(lanes) if lanes else set(_VALID_LANES)

        raw_records = ring.list_records("task:")
        parsed: list[tuple[str, bytes, dict[str, Any]]] = []
        for rec_key, raw in raw_records:
            decoded = decode_json(raw)
            if isinstance(decoded, dict):
                parsed.append((rec_key, raw, decoded))

        expired = [
            (rec_key, raw, data)
            for rec_key, raw, data in parsed
            if data.get("status") == "in_flight"
            and (data.get("locked_until") or 0.0) <= now
            and str(data.get("lane", "io")) in target_lanes
        ]
        expired.sort(
            key=lambda item: (float(item[2].get("locked_until") or 0.0), int(item[2].get("seq") or 0), item[0])
        )
        for rec_key, raw, data in expired:
            if len(claimed) >= batch_size:
                break
            new_handle = uuid.uuid4().hex
            new_attempt = int(data.get("attempt", 1)) + 1
            reclaimed_env = _dict_to_envelope(data, receipt_handle=new_handle, attempt=new_attempt)
            updated = _envelope_to_dict(
                reclaimed_env,
                status="in_flight",
                seq=int(data.get("seq") or 0),
                locked_until=now + self._visibility_timeout_seconds,
                expires_at=data.get("expires_at"),
            )
            if ring.cas_record(rec_key, raw, encode_json_bytes(updated)):
                claimed.append(reclaimed_env)

        for lane in lanes:
            if len(claimed) >= batch_size:
                break
            lane_ready = [
                (rec_key, raw, data)
                for rec_key, raw, data in parsed
                if data.get("status") == "ready" and str(data.get("lane", "io")) == lane
            ]
            lane_ready.sort(key=lambda item: (int(item[2].get("seq") or 0), item[0]))
            for rec_key, raw, data in lane_ready:
                if len(claimed) >= batch_size:
                    break
                handle = uuid.uuid4().hex
                active_env = _dict_to_envelope(data, receipt_handle=handle)
                updated = _envelope_to_dict(
                    active_env,
                    status="in_flight",
                    seq=int(data.get("seq") or 0),
                    locked_until=now + self._visibility_timeout_seconds,
                    expires_at=data.get("expires_at"),
                )
                if ring.cas_record(rec_key, raw, encode_json_bytes(updated)):
                    claimed.append(active_env)

        if not claimed and timeout_seconds > 0.0:
            deadline = time.monotonic() + timeout_seconds
            while True:
                remaining_ms = int((deadline - time.monotonic()) * 1000)
                if remaining_ms <= 0:
                    return []
                slice_ms = min(remaining_ms, 50)
                woke = await asyncio.to_thread(ring.wait, slice_ms)
                if woke:
                    self._drain_wakeups()
                    retried = await self.dequeue(lanes, batch_size=batch_size, timeout_seconds=0.0)
                    if retried:
                        return retried
                if time.monotonic() >= deadline:
                    return await self.dequeue(lanes, batch_size=batch_size, timeout_seconds=0.0)

        return claimed

    async def heartbeat(
        self,
        envelope: TaskEnvelope,
        *,
        lease_seconds: float | None = None,
    ) -> bool:
        """Extend visibility lease in SHM when receipt_handle still owns the task."""
        if not envelope.receipt_handle:
            return False
        ring = self.ring
        rec_key = f"task:{envelope.task_id}"
        for _ in range(4):
            raw = ring.get_record(rec_key)
            if raw is None:
                return False
            data = decode_json(raw)
            if not isinstance(data, dict):
                return False
            if data.get("status") != "in_flight" or data.get("receipt_handle") != envelope.receipt_handle:
                return False
            ttl = self._visibility_timeout_seconds if lease_seconds is None else lease_seconds
            data["locked_until"] = time.time() + ttl
            if ring.cas_record(rec_key, raw, encode_json_bytes(data)):
                return True
        return False

    async def ack(self, envelope: TaskEnvelope) -> None:
        """Acknowledge completed task by fenced CAS deletion of in-flight SHM record."""
        if not envelope.receipt_handle:
            return
        ring = self.ring
        rec_key = f"task:{envelope.task_id}"
        raw = ring.get_record(rec_key)
        if raw is None:
            return
        data = decode_json(raw)
        if isinstance(data, dict) and data.get("receipt_handle") == envelope.receipt_handle:
            ring.cas_record(rec_key, raw, None)

    async def nack(
        self,
        envelope: TaskEnvelope,
        *,
        error: Exception | str,
        delay_seconds: float | None = None,
        send_to_dlq: bool = False,
    ) -> DeadLetterEntry | None:
        """Negatively acknowledge task with fenced CAS, routing to DLQ or retry."""
        ring = self.ring
        rec_key = f"task:{envelope.task_id}"
        raw = ring.get_record(rec_key)
        if raw is not None:
            data = decode_json(raw)
            if (
                isinstance(data, dict)
                and envelope.receipt_handle
                and data.get("receipt_handle") != envelope.receipt_handle
            ):
                return None

        err_type = type(error).__name__ if isinstance(error, Exception) else "TaskExecutionError"
        err_msg = str(error)

        if send_to_dlq or envelope.attempt >= envelope.max_retries:
            if raw is not None and not ring.cas_record(rec_key, raw, None):
                return None
            dlq_id = uuid.uuid4().hex
            entry = DeadLetterEntry(
                dlq_id=dlq_id,
                envelope=envelope,
                error_type=err_type,
                error_message=err_msg,
                failed_at=time.time(),
            )
            dlq_payload = {
                "dlq_id": dlq_id,
                "envelope": _envelope_to_dict(envelope, status="dlq", seq=0),
                "error_type": err_type,
                "error_message": err_msg,
                "failed_at": entry.failed_at,
            }
            ring.put_record(f"dlq:{dlq_id}", encode_json_bytes(dlq_payload))
            return entry

        retry_attempt = envelope.attempt + 1
        now = time.time()
        has_delay = delay_seconds is not None and delay_seconds > 0.0
        sched_at = now + delay_seconds if has_delay and delay_seconds is not None else None
        rescheduled_env = TaskEnvelope(
            task_id=envelope.task_id,
            task_name=envelope.task_name,
            lane=envelope.lane,
            args=envelope.args,
            kwargs=envelope.kwargs,
            attempt=retry_attempt,
            max_retries=envelope.max_retries,
            enqueued_at=envelope.enqueued_at,
            scheduled_at=sched_at,
            idempotency_key=envelope.idempotency_key,
            traceparent=envelope.traceparent,
            receipt_handle=None,
        )
        new_status = "scheduled" if has_delay else "ready"
        new_payload = _envelope_to_dict(rescheduled_env, status=new_status, seq=time.time_ns())
        new_bytes = encode_json_bytes(new_payload)
        if raw is not None:
            if not ring.cas_record(rec_key, raw, new_bytes):
                return None
        else:
            ring.put_record(rec_key, new_bytes)
        if not has_delay:
            self._emit_wakeup()
        return None

    async def promote_scheduled(self, *, now: float | None = None, batch_size: int = 100) -> int:
        """Promote due scheduled envelopes to ready status in SHM."""
        current_time = now if now is not None else time.time()
        ring = self.ring
        promoted = 0
        candidates: list[tuple[str, bytes, dict[str, Any]]] = []
        for rec_key, raw in ring.list_records("task:"):
            data = decode_json(raw)
            if not isinstance(data, dict):
                continue
            if data.get("status") == "scheduled":
                sched = data.get("scheduled_at")
                if sched is not None and float(sched) <= current_time:
                    candidates.append((rec_key, raw, data))
        candidates.sort(key=lambda item: (float(item[2].get("scheduled_at") or 0.0), item[0]))
        for rec_key, raw, data in candidates[:batch_size]:
            data["status"] = "ready"
            if ring.cas_record(rec_key, raw, encode_json_bytes(data)):
                promoted += 1
        if promoted > 0:
            self._emit_wakeup()
        return promoted

    async def expire_overdue(self, *, now: float | None = None) -> int:
        """Delete overdue unstarted tasks whose expires_at timestamp has passed."""
        current_time = now if now is not None else time.time()
        ring = self.ring
        expired = 0
        for rec_key, raw in ring.list_records("task:"):
            data = decode_json(raw)
            if not isinstance(data, dict):
                continue
            if data.get("status") not in {"ready", "scheduled"}:
                continue
            exp = data.get("expires_at")
            if exp is not None and float(exp) <= current_time and ring.cas_record(rec_key, raw, None):
                expired += 1
        return expired

    async def list_dlq(self, *, limit: int = 100) -> list[DeadLetterEntry]:
        """List dead-letter entries stored in SHM up to limit."""
        entries: list[DeadLetterEntry] = []
        for _, raw in self.ring.list_records("dlq:"):
            if len(entries) >= limit:
                break
            data = decode_json(raw)
            if not isinstance(data, dict):
                continue
            env_dict = data.get("envelope")
            if not isinstance(env_dict, dict):
                continue
            entries.append(
                DeadLetterEntry(
                    dlq_id=str(data["dlq_id"]),
                    envelope=_dict_to_envelope(env_dict),
                    error_type=str(data.get("error_type", "TaskExecutionError")),
                    error_message=str(data.get("error_message", "")),
                    failed_at=float(data.get("failed_at", 0.0)),
                )
            )
        return entries

    async def replay_dlq(self, dlq_id: str) -> TaskEnvelope | None:
        """Replay a dead-letter entry by re-enqueuing into SHM with reset attempt count."""
        ring = self.ring
        dlq_key = f"dlq:{dlq_id}"
        raw = ring.get_record(dlq_key)
        if raw is None:
            return None
        if not ring.cas_record(dlq_key, raw, None):
            return None
        data = decode_json(raw)
        if not isinstance(data, dict) or not isinstance(data.get("envelope"), dict):
            return None
        env_dict = data["envelope"]
        replayed_env = TaskEnvelope(
            task_id=str(env_dict["task_id"]),
            task_name=str(env_dict["task_name"]),
            lane=_dict_to_envelope(env_dict).lane,
            args=tuple(env_dict.get("args", ())),
            kwargs=dict(env_dict.get("kwargs", {})),
            attempt=1,
            max_retries=int(env_dict.get("max_retries", 3)),
            enqueued_at=time.time(),
            scheduled_at=None,
            idempotency_key=env_dict.get("idempotency_key"),
            traceparent=env_dict.get("traceparent"),
            receipt_handle=None,
        )
        payload = _envelope_to_dict(replayed_env, status="ready", seq=time.time_ns())
        ring.put_record(f"task:{replayed_env.task_id}", encode_json_bytes(payload))
        self._emit_wakeup()
        return replayed_env

    async def acquire_lease(
        self,
        lease_key: str,
        owner_id: str,
        ttl_seconds: float,
        *,
        slot: str | None = None,
    ) -> CronLease | None:
        """Atomically acquire a cross-process lease if free, expired, or owned by caller."""
        ring = self.ring
        rec_key = f"lease:{lease_key}"
        for _ in range(4):
            now = time.time()
            raw = ring.get_record(rec_key)
            if raw is None:
                lease = CronLease(
                    lease_key=lease_key,
                    owner_id=owner_id,
                    expires_at=now + ttl_seconds,
                    last_slot=slot,
                )
                payload = {
                    "lease_key": lease.lease_key,
                    "owner_id": lease.owner_id,
                    "expires_at": lease.expires_at,
                    "last_slot": lease.last_slot,
                }
                if ring.cas_record(rec_key, None, encode_json_bytes(payload)):
                    return lease
                continue
            existing = decode_json(raw)
            if not isinstance(existing, dict):
                return None
            if float(existing.get("expires_at", 0.0)) > now and existing.get("owner_id") != owner_id:
                return None
            lease = CronLease(
                lease_key=lease_key,
                owner_id=owner_id,
                expires_at=now + ttl_seconds,
                last_slot=slot or existing.get("last_slot"),
            )
            payload = {
                "lease_key": lease.lease_key,
                "owner_id": lease.owner_id,
                "expires_at": lease.expires_at,
                "last_slot": lease.last_slot,
            }
            if ring.cas_record(rec_key, raw, encode_json_bytes(payload)):
                return lease
        return None

    async def renew_lease(
        self,
        lease_key: str,
        owner_id: str,
        ttl_seconds: float,
        *,
        slot: str | None = None,
    ) -> CronLease | None:
        """Atomically renew a held lease when owner_id matches the current holder."""
        ring = self.ring
        rec_key = f"lease:{lease_key}"
        for _ in range(4):
            now = time.time()
            raw = ring.get_record(rec_key)
            if raw is None:
                return None
            existing = decode_json(raw)
            if not isinstance(existing, dict) or existing.get("owner_id") != owner_id:
                return None
            lease = CronLease(
                lease_key=lease_key,
                owner_id=owner_id,
                expires_at=now + ttl_seconds,
                last_slot=slot or existing.get("last_slot"),
            )
            payload = {
                "lease_key": lease.lease_key,
                "owner_id": lease.owner_id,
                "expires_at": lease.expires_at,
                "last_slot": lease.last_slot,
            }
            if ring.cas_record(rec_key, raw, encode_json_bytes(payload)):
                return lease
        return None

    async def reserve_identity(self, key: str, owner_id: str, *, ttl_seconds: float | None = None) -> bool:
        """Atomically reserve an identity key in SHM across OS processes."""
        ring = self.ring
        rec_key = f"ident:{key}"
        for _ in range(4):
            now = time.time()
            raw = ring.get_record(rec_key)
            if raw is not None:
                existing = decode_json(raw)
                if not isinstance(existing, dict):
                    return False
                exp = existing.get("expires_at")
                if exp is None or float(exp) > now:
                    return existing.get("owner_id") == owner_id
            payload = {
                "owner_id": owner_id,
                "expires_at": (now + ttl_seconds) if ttl_seconds is not None else None,
            }
            if ring.cas_record(rec_key, raw, encode_json_bytes(payload)):
                return True
        return False

    async def release_identity(self, key: str, *, owner_id: str | None = None) -> bool:
        """Release a held identity reservation in SHM, optionally fenced by owner_id."""
        ring = self.ring
        rec_key = f"ident:{key}"
        raw = ring.get_record(rec_key)
        if raw is None:
            return False
        if owner_id is not None:
            existing = decode_json(raw)
            if not isinstance(existing, dict) or existing.get("owner_id") != owner_id:
                return False
        return ring.cas_record(rec_key, raw, None)

    def store_result(self, record: TaskResultRecord) -> None:
        """Store a task execution result synchronously in SHM."""
        payload = {
            "task_id": record.task_id,
            "status": record.status,
            "result": record.result,
            "error": record.error,
            "completed_at": record.completed_at,
        }
        self.ring.put_record(f"res:{record.task_id}", encode_json_bytes(payload))

    async def astore_result(self, record: TaskResultRecord) -> None:
        """Store a task execution result asynchronously in SHM."""
        self.store_result(record)

    def get_result(self, task_id: str) -> TaskResultRecord | None:
        """Retrieve a task execution result synchronously from SHM."""
        raw = self.ring.get_record(f"res:{task_id}")
        if raw is None:
            return None
        data = decode_json(raw)
        if not isinstance(data, dict):
            return None
        return TaskResultRecord(
            task_id=str(data["task_id"]),
            status=str(data["status"]),
            result=data.get("result"),
            error=data.get("error"),
            completed_at=float(data.get("completed_at", 0.0)),
        )

    async def aget_result(self, task_id: str) -> TaskResultRecord | None:
        """Retrieve a task execution result asynchronously from SHM."""
        return self.get_result(task_id)

    def unlink(self) -> None:
        """Unlink the underlying named POSIX SHM segment."""
        ShmQueueRing.unlink(self._ring_name)

    async def close(self) -> None:
        """Close the local ShmQueueRing mapping without unlinking the shared segment."""
        if self._ring is not None:
            self._ring.close()
            self._ring = None
