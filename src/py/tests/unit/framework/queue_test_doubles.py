"""Shared in-memory test doubles for distributed queue and notification unit suites."""

from __future__ import annotations

import asyncio
import collections
import time
import uuid
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Any

import msgspec

import vertebra.db.driver._resp_queue as resp_queue_mod
from vertebra.db.driver import (
    AsyncNotificationSource,
    AsyncQueueStore,
    NotificationEvent,
    QueueEntry,
    QueueReservation,
)
from vertebra.framework.queues import (
    MemoryQueueBackend,
    OracleQueueBackend,
    PostgreSQLQueueBackend,
    QueueConfig,
    RedisQueueBackend,
    ValkeyQueueBackend,
)
from vertebra.utils.serializers import decode_json, encode_json

__all__ = (
    "FakeOracleQueueExecutor",
    "FakePostgreSQLQueueExecutor",
    "FakeRedisStreamClient",
    "InMemoryAsyncNotificationSource",
    "InMemoryAsyncRESPQueueStoreDriver",
    "InMemoryRESPQueueStoreDriver",
    "StatefulAsyncQueueStore",
    "create_backend_by_url",
)

_JOB_COLUMNS = (
    "task_id",
    "task_name",
    "lane",
    "payload",
    "attempt",
    "max_retries",
    "status",
    "enqueued_at",
    "scheduled_at",
    "locked_until",
    "consumer_name",
    "receipt_handle",
    "idempotency_key",
    "traceparent",
)
_DLQ_COLUMNS = ("dlq_id", "task_id", "task_name", "lane", "payload", "error_type", "error_message", "failed_at")


class _BaseFakeSQLQueueExecutor:
    """Shared transactional SQL simulation for fake Oracle and PostgreSQL queue executors."""

    __slots__ = ("_dlq", "_jobs", "_leases", "_lock")

    def __init__(self) -> None:
        self._jobs: dict[str, dict[str, Any]] = {}
        self._dlq: dict[str, dict[str, Any]] = {}
        self._leases: dict[str, dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    def _execute_common(self, q: str, params: tuple[Any, ...]) -> int | None:
        if "INSERT INTO VERTEBRA_QUEUE_JOBS" in q:
            row = dict(zip(_JOB_COLUMNS, params, strict=True))
            ikey = row["idempotency_key"]
            if ikey and any(j.get("idempotency_key") == ikey for j in self._jobs.values()):
                return 0
            self._jobs[row["task_id"]] = row
            return 1
        if "DELETE FROM VERTEBRA_QUEUE_JOBS" in q:
            return 1 if self._jobs.pop(params[0], None) is not None else 0
        if "DELETE FROM VERTEBRA_QUEUE_DLQ" in q:
            return 1 if self._dlq.pop(params[0], None) is not None else 0
        if "INSERT INTO VERTEBRA_QUEUE_DLQ" in q:
            row = dict(zip(_DLQ_COLUMNS, params, strict=True))
            self._dlq[row["dlq_id"]] = row
            return 1
        if "SET SCHEDULED_AT = NULL" in q:
            promoted = 0
            for job in self._jobs.values():
                if (
                    job.get("status") == "pending"
                    and job.get("scheduled_at") is not None
                    and job["scheduled_at"] <= params[0]
                ):
                    job["scheduled_at"] = None
                    promoted += 1
            return promoted
        if "UPDATE VERTEBRA_QUEUE_JOBS" in q and "CONSUMER_NAME" not in q:
            status, attempt, sched, locked, handle, task_id = params
            if task_id not in self._jobs:
                return 0
            self._jobs[task_id].update(
                {
                    "status": status,
                    "attempt": attempt,
                    "scheduled_at": sched,
                    "locked_until": locked,
                    "receipt_handle": handle,
                }
            )
            return 1
        return None

    def _upsert_lease(self, params: tuple[Any, ...]) -> dict[str, Any] | None:
        lease_key, owner_id, expires_at, last_slot, check_now = params
        existing = self._leases.get(lease_key)
        if existing is None or existing["expires_at"] <= check_now or existing["owner_id"] == owner_id:
            record = {
                "lease_key": lease_key,
                "owner_id": owner_id,
                "expires_at": expires_at,
                "last_slot": last_slot or (existing.get("last_slot") if existing else None),
            }
            self._leases[lease_key] = record
            return dict(record)
        return None

    def _select_claimable(self, target_lanes: set[str], now_ts: float, limit: int) -> list[tuple[dict[str, Any], bool]]:
        sorted_jobs = sorted(
            self._jobs.values(),
            key=lambda j: (j.get("scheduled_at") or j.get("enqueued_at") or 0.0, j.get("enqueued_at") or 0.0),
        )
        matched: list[tuple[dict[str, Any], bool]] = []
        for job in sorted_jobs:
            if len(matched) >= limit:
                break
            if job.get("lane") not in target_lanes:
                continue
            status, sched, locked = job.get("status"), job.get("scheduled_at"), job.get("locked_until")
            is_ready = status == "pending" and (sched is None or sched <= now_ts)
            is_expired = status == "running" and locked is not None and locked <= now_ts
            if is_ready or is_expired:
                matched.append((job, is_expired))
        return matched

    def _fetch_dlq_all(self, q: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        if "SELECT * FROM VERTEBRA_QUEUE_DLQ" in q:
            return [dict(e) for e in list(self._dlq.values())[: (params[0] if params else 100)]]
        return []

    def _fetch_dlq_row(self, q: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
        if "SELECT * FROM VERTEBRA_QUEUE_DLQ WHERE DLQ_ID" in q:
            dlq = self._dlq.get(params[0])
            return dict(dlq) if dlq is not None else None
        return None


class FakeOracleQueueExecutor(_BaseFakeSQLQueueExecutor):
    """In-memory transactional SQL simulation of Oracle FOR UPDATE SKIP LOCKED semantics."""

    __slots__ = ("commit_count",)

    def __init__(self) -> None:
        super().__init__()
        self.commit_count = 0

    async def execute(self, query: str, *params: Any) -> int:
        """Execute mock DML query."""
        async with self._lock:
            q = query.strip().upper()
            common = self._execute_common(q, params)
            if common is not None:
                return common
            if "UPDATE VERTEBRA_QUEUE_JOBS" in q and "CONSUMER_NAME" in q:
                status, attempt, locked, cname, handle, task_id = params
                if task_id not in self._jobs:
                    return 0
                self._jobs[task_id].update(
                    {
                        "status": status,
                        "attempt": attempt,
                        "locked_until": locked,
                        "consumer_name": cname,
                        "receipt_handle": handle,
                    }
                )
                return 1
            if "MERGE INTO VERTEBRA_QUEUE_LEASES" in q:
                return 1 if self._upsert_lease(params) is not None else 0
            return 0

    async def fetch_all(self, query: str, *params: Any) -> list[dict[str, Any]]:
        """Fetch all matching rows."""
        async with self._lock:
            q = query.strip().upper()
            if "FOR UPDATE SKIP LOCKED" in q:
                *lane_binds, now_ts, limit = params
                assert all(isinstance(item, str) for item in lane_binds)
                return [dict(job) for job, _ in self._select_claimable(set(lane_binds), now_ts, limit)]
            return self._fetch_dlq_all(q, params)

    async def fetch_one(self, query: str, *params: Any) -> dict[str, Any] | None:
        """Fetch single matching row."""
        async with self._lock:
            q = query.strip().upper()
            assert "MERGE INTO" not in q
            if "FROM VERTEBRA_QUEUE_LEASES WHERE LEASE_KEY" in q:
                lease = self._leases.get(params[0])
                return dict(lease) if lease is not None else None
            return self._fetch_dlq_row(q, params)

    async def commit(self) -> None:
        """Commit active mock transaction."""
        self.commit_count += 1

    async def close(self) -> None:
        """Clear state."""
        async with self._lock:
            self._jobs.clear()
            self._dlq.clear()
            self._leases.clear()


class FakePostgreSQLQueueExecutor(_BaseFakeSQLQueueExecutor):
    """In-memory PostgreSQL queue executor simulating SKIP LOCKED claims and LISTEN/NOTIFY."""

    __slots__ = ("_waiters", "waiter_ready")

    def __init__(self) -> None:
        super().__init__()
        self._waiters: dict[str, list[asyncio.Event]] = collections.defaultdict(list)
        self.waiter_ready: asyncio.Event | None = None

    async def execute(self, query: str, *params: Any) -> int:
        """Execute mock query."""
        async with self._lock:
            common = self._execute_common(query.strip().upper(), params)
            return common if common is not None else 0

    async def fetch_all(self, query: str, *params: Any) -> list[dict[str, Any]]:
        """Fetch all matching rows."""
        async with self._lock:
            q = query.strip().upper()
            if "WITH CLAIMED AS" in q or "FOR UPDATE SKIP LOCKED" in q:
                lanes, now_ts, limit, locked_until, consumer_name, receipt_handle = params
                claimed: list[dict[str, Any]] = []
                for job, is_expired in self._select_claimable(set(lanes), now_ts, limit):
                    job.update(
                        {
                            "status": "running",
                            "locked_until": locked_until,
                            "consumer_name": consumer_name,
                            "receipt_handle": receipt_handle or uuid.uuid4().hex,
                        }
                    )
                    if is_expired:
                        job["attempt"] = int(job.get("attempt", 1)) + 1
                    claimed.append(dict(job))
                return claimed
            return self._fetch_dlq_all(q, params)

    async def fetch_one(self, query: str, *params: Any) -> dict[str, Any] | None:
        """Fetch a single matching row."""
        async with self._lock:
            q = query.strip().upper()
            if "SELECT MIN(SCHEDULED_AT)" in q:
                lanes, now_ts = params
                target_lanes = set(lanes)
                scheds = [
                    j["scheduled_at"]
                    for j in self._jobs.values()
                    if j.get("lane") in target_lanes
                    and j.get("status") == "pending"
                    and j.get("scheduled_at") is not None
                    and j["scheduled_at"] > now_ts
                ]
                return {"min_scheduled": min(scheds) if scheds else None}
            if "INSERT INTO VERTEBRA_QUEUE_LEASES" in q:
                return self._upsert_lease(params)
            return self._fetch_dlq_row(q, params)

    async def notify(self, channel: str, payload: str) -> None:
        """Wake up any tasks waiting on channel."""
        del payload
        async with self._lock:
            for evt in self._waiters.get(channel, []):
                evt.set()

    async def wait_for_notify(self, channel: str, timeout_seconds: float) -> bool:
        """Wait for notification on channel until timeout."""
        evt = asyncio.Event()
        async with self._lock:
            self._waiters[channel].append(evt)
            if self.waiter_ready is not None:
                self.waiter_ready.set()
        try:
            await asyncio.wait_for(evt.wait(), timeout=timeout_seconds)
            return True
        except TimeoutError:
            return False
        finally:
            async with self._lock:
                if evt in self._waiters[channel]:
                    self._waiters[channel].remove(evt)

    async def close(self) -> None:
        """Clear state."""
        async with self._lock:
            self._jobs.clear()
            self._dlq.clear()
            self._leases.clear()
            self._waiters.clear()


class FakeRedisStreamClient:
    """In-memory Redis Streams client emulating consumer groups, sorted sets, and queue Lua scripts."""

    __slots__ = ("_groups", "_kv", "_pel", "_streams", "_zsets")

    def __init__(self) -> None:
        self._streams: dict[str, list[tuple[str, dict[str, Any]]]] = collections.defaultdict(list)
        self._groups: dict[str, set[str]] = collections.defaultdict(set)
        self._pel: dict[str, dict[str, tuple[str, float, dict[str, Any]]]] = collections.defaultdict(dict)
        self._zsets: dict[str, dict[str, float]] = collections.defaultdict(dict)
        self._kv: dict[str, tuple[str, float | None]] = {}

    async def xgroup_create(self, stream: str, group: str, id: str = "$", mkstream: bool = False) -> bool:
        """Register consumer group for stream."""
        del id
        if mkstream and stream not in self._streams:
            self._streams[stream] = []
        self._groups[stream].add(group)
        return True

    async def xadd(
        self, stream: str, fields: Mapping[str, Any], id: str = "*", maxlen: int | None = None, approximate: bool = True
    ) -> str:
        """Append stream message."""
        del id, approximate
        entry_id = f"{int(time.time() * 1000)}-{len(self._streams[stream])}"
        self._streams[stream].append(
            (entry_id, {str(k): (v.decode("utf-8") if isinstance(v, bytes) else str(v)) for k, v in fields.items()})
        )
        if maxlen and len(self._streams[stream]) > maxlen:
            del self._streams[stream][: len(self._streams[stream]) - maxlen]
        return entry_id

    async def xreadgroup(
        self, group: str, consumer: str, streams: Mapping[str, str], count: int | None = None, block: int | None = None
    ) -> list[Any]:
        """Read unclaimed entries for consumer group."""
        del block
        limit, results, now = count or 10, [], time.time()
        for stream_name in streams:
            pel_key = f"{stream_name}:{group}"
            claimed_in_pel = set(self._pel[pel_key].keys())
            matched = []
            for entry_id, fields in self._streams.get(stream_name, []):
                if entry_id not in claimed_in_pel:
                    self._pel[pel_key][entry_id] = (consumer, now, fields)
                    matched.append((entry_id, fields))
                    if len(matched) >= limit:
                        break
            if matched:
                results.append((stream_name, matched))
        return results

    async def xack(self, stream: str, group: str, *ids: str) -> int:
        """Remove entries from group PEL."""
        pel_key = f"{stream}:{group}"
        return sum(1 for entry_id in ids if self._pel[pel_key].pop(entry_id, None) is not None)

    async def xautoclaim(
        self,
        stream: str,
        group: str,
        consumer: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
    ) -> tuple[str, list[tuple[str, dict[str, Any]]], list[str]]:
        """Reclaim pending entries older than min_idle_time milliseconds."""
        del start_id
        limit, pel_key, now, idle_s = count or 10, f"{stream}:{group}", time.time(), min_idle_time / 1000.0
        reclaimed = []
        for entry_id, (_, last_delivered, fields) in list(self._pel[pel_key].items()):
            if now - last_delivered >= idle_s:
                self._pel[pel_key][entry_id] = (consumer, now, fields)
                reclaimed.append((entry_id, fields))
                if len(reclaimed) >= limit:
                    break
        return ("0-0", reclaimed, [])

    async def xrange(
        self, stream: str, min: str = "-", max: str = "+", count: int | None = None
    ) -> list[tuple[str, dict[str, Any]]]:
        """List stream entries in FIFO order."""
        del min, max
        entries = self._streams.get(stream, [])
        return entries[:count] if count else entries

    async def xdel(self, stream: str, *ids: str) -> int:
        """Remove matching entries from stream."""
        id_set = set(ids)
        before = len(self._streams.get(stream, []))
        self._streams[stream] = [item for item in self._streams.get(stream, []) if item[0] not in id_set]
        return before - len(self._streams[stream])

    async def zadd(self, key: str, mapping: Mapping[str, float]) -> int:
        """Add elements to simulated sorted set."""
        added = sum(1 for m in mapping if m not in self._zsets[key])
        for member, score in mapping.items():
            self._zsets[key][member] = float(score)
        return added

    async def eval(self, script: str, numkeys: int, *keys_and_args: Any) -> Any:
        """Simulate execution of queue promotion and lease renewal scripts."""
        del numkeys
        if "ZRANGEBYSCORE" in script:
            scheduled_key, now_ts, prefix, limit = (
                str(keys_and_args[0]),
                float(keys_and_args[1]),
                str(keys_and_args[2]),
                int(keys_and_args[3]),
            )
            zset, promoted = self._zsets[scheduled_key], 0
            for raw_json, _ in [(m, s) for m, s in sorted(zset.items(), key=lambda p: p[1]) if s <= now_ts][:limit]:
                decoded = decode_json(raw_json.encode("utf-8") if isinstance(raw_json, str) else raw_json)
                if isinstance(decoded, dict):
                    lane, task_name = decoded.get("lane"), str(decoded.get("task_name") or "")
                elif isinstance(decoded, list) and len(decoded) >= 3:
                    lane, task_name = decoded[2], str(decoded[1] or "")
                else:
                    lane, task_name = None, ""
                if lane:
                    await self.xadd(
                        f"{prefix}:lane:{lane}", {"payload": raw_json, "lane": lane, "task_name": task_name}
                    )
                    zset.pop(raw_json, None)
                    promoted += 1
            return promoted
        if "current_owner == owner_id" in script:
            lease_key, owner_id, ttl_ms = str(keys_and_args[0]), str(keys_and_args[1]), float(keys_and_args[2])
            new_slot = str(keys_and_args[3]) if len(keys_and_args) > 3 else ""
            existing = self._kv.get(lease_key)
            if existing and (existing[1] is None or existing[1] > time.time()):
                parts = existing[0].split(":", 1)
                if parts[0] == owner_id:
                    slot = new_slot or (parts[1] if len(parts) > 1 else "")
                    self._kv[lease_key] = (f"{owner_id}:{slot}" if slot else owner_id, time.time() + (ttl_ms / 1000.0))
                    return 1
            return 0
        return None

    async def set(self, key: str, value: str, nx: bool = False, px: int | None = None) -> bool:
        """Store key value pair."""
        now, existing = time.time(), self._kv.get(key)
        if existing and existing[1] is not None and existing[1] <= now:
            self._kv.pop(key, None)
            existing = None
        if nx and existing is not None:
            return False
        self._kv[key] = (value, now + (px / 1000.0) if px else None)
        return True

    async def get(self, key: str) -> str | None:
        """Fetch value if not expired."""
        existing = self._kv.get(key)
        if not existing:
            return None
        if existing[1] is not None and existing[1] <= time.time():
            self._kv.pop(key, None)
            return None
        return existing[0]

    async def aclose(self) -> None:
        """Clear memory structures."""
        for store in (self._streams, self._groups, self._pel, self._zsets, self._kv):
            store.clear()


def create_backend_by_url(url: str, config: QueueConfig) -> Any:
    """Instantiate queue backends, injecting the Redis client and Oracle/PostgreSQL executor fakes."""
    cfg = QueueConfig(
        backend_url=url,
        visibility_timeout_seconds=config.visibility_timeout_seconds,
        poll_interval_seconds=config.poll_interval_seconds,
        stream_prefix=config.stream_prefix,
        consumer_group=config.consumer_group,
        consumer_name=config.consumer_name,
    )
    if url.startswith("memory"):
        return MemoryQueueBackend(cfg)
    if url.startswith("redis"):
        return RedisQueueBackend(cfg, client=FakeRedisStreamClient())
    if url.startswith("valkey"):
        return ValkeyQueueBackend(cfg, client=FakeRedisStreamClient())
    if url.startswith("postgres"):
        return PostgreSQLQueueBackend(cfg, executor=FakePostgreSQLQueueExecutor())
    if url.startswith("oracle"):
        return OracleQueueBackend(cfg, executor=FakeOracleQueueExecutor())
    msg = f"Unknown backend url: {url}"
    raise ValueError(msg)


class StatefulAsyncQueueStore(AsyncQueueStore):
    """In-memory AsyncQueueStore implementing fenced attempts, reap_stale, claim_job, and pop_dead_letter."""

    def __init__(self) -> None:
        self._rows: dict[str, tuple[QueueEntry, str, str | None]] = {}
        self._unique_keys: set[str] = set()
        self._reservations: dict[str, tuple[QueueReservation, float | None]] = {}
        self._leases: dict[str, tuple[str, float]] = {}
        self.reap_calls = 0

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    async def ensure_schema(self) -> None:
        return None

    async def enqueue(self, entry: QueueEntry) -> str:
        jid = entry.job_id or uuid.uuid4().hex
        if jid in self._rows:
            msg = f"Duplicate job_id {jid!r}"
            raise ValueError(msg)
        ikey = entry.headers.get("idempotency_key")
        if ikey:
            if ikey in self._unique_keys:
                return jid
            self._unique_keys.add(ikey)
        self._rows[jid] = (msgspec.structs.replace(entry, job_id=jid), "ready", None)
        return jid

    async def enqueue_many(self, entries: Sequence[QueueEntry]) -> list[str]:
        return [await self.enqueue(e) for e in entries]

    def _try_claim_entry(
        self,
        jid: str,
        entry: QueueEntry,
        status: str,
        *,
        now_ms: int,
        worker_id: str,
        lease_seconds: float,
        expected_attempt: int | None = None,
    ) -> QueueEntry | None:
        due = status == "ready" and (entry.scheduled_at_ms is None or entry.scheduled_at_ms <= now_ms)
        exp = status == "claimed" and entry.lease_until_ms <= now_ms and entry.attempt < entry.max_attempts
        if not (due or exp):
            return None
        next_attempt = entry.attempt + 1 if status == "claimed" else entry.attempt
        if expected_attempt is not None and next_attempt != expected_attempt:
            return None
        updated = msgspec.structs.replace(
            entry, attempt=next_attempt, lease_until_ms=now_ms + int(lease_seconds * 1000), worker_id=worker_id
        )
        self._rows[jid] = (updated, "claimed", worker_id)
        return updated

    async def claim(
        self, queues: Sequence[str], *, limit: int = 1, worker_id: str, lease_seconds: float = 30.0
    ) -> list[QueueEntry]:
        now_ms = self._now_ms()
        claimed: list[QueueEntry] = []
        for jid, (entry, status, _) in list(self._rows.items()):
            if len(claimed) >= limit:
                break
            if entry.queue in queues:
                updated = self._try_claim_entry(
                    jid, entry, status, now_ms=now_ms, worker_id=worker_id, lease_seconds=lease_seconds
                )
                if updated is not None:
                    claimed.append(updated)
        return claimed

    async def claim_job(
        self, job_id: str, *, worker_id: str, lease_seconds: float = 30.0, expected_attempt: int | None = None
    ) -> QueueEntry | None:
        item = self._rows.get(job_id)
        return (
            self._try_claim_entry(
                job_id,
                item[0],
                item[1],
                now_ms=self._now_ms(),
                worker_id=worker_id,
                lease_seconds=lease_seconds,
                expected_attempt=expected_attempt,
            )
            if item
            else None
        )

    async def ack(self, job_id: str, *, attempt: int) -> bool:
        item = self._rows.get(job_id)
        if item is None or item[1] != "claimed" or item[0].attempt != attempt:
            return False
        comp_ms = item[0].completed_at_ms if item[0].completed_at_ms is not None else self._now_ms()
        self._rows[job_id] = (
            msgspec.structs.replace(item[0], lease_until_ms=0, completed_at_ms=comp_ms),
            "completed",
            None,
        )
        return True

    async def nack(self, job_id: str, *, attempt: int, error: str, retry_delay_s: float | None = None) -> bool:
        item = self._rows.get(job_id)
        if item is None or item[1] != "claimed" or item[0].attempt != attempt:
            return False
        entry, now_ms = item[0], self._now_ms()
        if retry_delay_s is None or entry.attempt >= entry.max_attempts:
            comp_ms = entry.completed_at_ms if entry.completed_at_ms is not None else now_ms
            self._rows[job_id] = (
                msgspec.structs.replace(entry, lease_until_ms=0, completed_at_ms=comp_ms, error=error),
                "dead_letter",
                None,
            )
            return True
        sched_ms = now_ms + int(retry_delay_s * 1000) if retry_delay_s > 0 else None
        self._rows[job_id] = (
            msgspec.structs.replace(
                entry,
                attempt=entry.attempt + 1,
                lease_until_ms=0,
                scheduled_at_ms=sched_ms,
                worker_id=None,
                error=error,
            ),
            "ready",
            None,
        )
        return True

    async def heartbeat(self, job_ids: Sequence[str], *, worker_id: str, lease_seconds: float) -> int:
        new_lease, touched = self._now_ms() + int(lease_seconds * 1000), 0
        for jid in job_ids:
            item = self._rows.get(jid)
            if item is not None and item[1] == "claimed" and item[2] == worker_id:
                self._rows[jid] = (msgspec.structs.replace(item[0], lease_until_ms=new_lease), "claimed", worker_id)
                touched += 1
        return touched

    async def reap_stale(self, *, lease_seconds: float = 300.0) -> int:
        _ = lease_seconds
        self.reap_calls += 1
        now_ms, reaped = self._now_ms(), 0
        for jid, (entry, status, _) in list(self._rows.items()):
            if status == "claimed" and entry.lease_until_ms <= now_ms:
                if entry.attempt >= entry.max_attempts:
                    self._rows[jid] = (msgspec.structs.replace(entry, lease_until_ms=0), "dead_letter", None)
                else:
                    self._rows[jid] = (
                        msgspec.structs.replace(entry, attempt=entry.attempt + 1, lease_until_ms=0),
                        "ready",
                        None,
                    )
                reaped += 1
        return reaped

    async def promote_scheduled(self, *, limit: int = 100) -> int:
        now_ms, promoted = self._now_ms(), 0
        for jid, (entry, status, owner) in list(self._rows.items()):
            if promoted >= limit:
                break
            if status == "ready" and entry.scheduled_at_ms is not None and entry.scheduled_at_ms <= now_ms:
                self._rows[jid] = (msgspec.structs.replace(entry, scheduled_at_ms=None), status, owner)
                promoted += 1
        return promoted

    async def cancel(self, job_id: str) -> bool:
        item = self._rows.get(job_id)
        if item is None or item[1] not in ("ready", "claimed"):
            return False
        comp_ms = item[0].completed_at_ms if item[0].completed_at_ms is not None else self._now_ms()
        self._rows[job_id] = (
            msgspec.structs.replace(item[0], lease_until_ms=0, completed_at_ms=comp_ms),
            "cancelled",
            None,
        )
        return True

    async def get_entry(self, job_id: str) -> QueueEntry | None:
        item = self._rows.get(job_id)
        return item[0] if item else None

    async def get_entry_status(self, job_id: str) -> str | None:
        item = self._rows.get(job_id)
        return item[1] if item else None

    async def pop_dead_letter(self, job_id: str) -> QueueEntry | None:
        item = self._rows.get(job_id)
        if item is None or item[1] != "dead_letter":
            return None
        del self._rows[job_id]
        return item[0]

    async def list_entries(
        self, queue: str | None = None, *, status: str | None = None, limit: int = 100
    ) -> list[QueueEntry]:
        return [
            e
            for e, st, _ in self._rows.values()
            if (queue is None or e.queue == queue) and (status is None or st == status)
        ][:limit]

    async def cleanup_terminal(self, *, before_ms: int, limit: int | None = None) -> int:
        terminal, deleted = {"completed", "dead_letter", "cancelled", "expired", "interrupted"}, 0
        for jid, (entry, st, _) in list(self._rows.items()):
            if limit is not None and deleted >= limit:
                break
            if st in terminal and entry.completed_at_ms is not None and entry.completed_at_ms < before_ms:
                del self._rows[jid]
                deleted += 1
        return deleted

    async def get_entry_by_key(self, key: str, *, queue: str | None = None) -> QueueEntry | None:
        for entry, _, _ in self._rows.values():
            if (queue is None or entry.queue == queue) and (
                entry.headers.get("key") == key or entry.headers.get("idempotency_key") == key
            ):
                return entry
        return None

    def _prune_expired_reservation(self, key: str, now_s: float) -> QueueReservation | None:
        existing = self._reservations.get(key)
        if existing is None:
            return None
        if existing[1] is not None and existing[1] <= now_s:
            del self._reservations[key]
            return None
        return existing[0]

    async def reserve_identity(
        self, key: str, *, job_id: str, task_name: str, ttl_seconds: float | None = None
    ) -> QueueReservation | None:
        now_s = time.time()
        existing = self._prune_expired_reservation(key, now_s)
        if existing is not None:
            return existing
        res = QueueReservation(key=key, job_id=job_id, task_name=task_name, created_at_ms=int(now_s * 1000))
        self._reservations[key] = (res, now_s + ttl_seconds if ttl_seconds is not None else None)
        return None

    async def get_reservation(self, key: str) -> QueueReservation | None:
        return self._prune_expired_reservation(key, time.time())

    async def reset_identity(self, key: str, *, expected_job_id: str | None = None) -> bool:
        existing = self._prune_expired_reservation(key, time.time())
        if existing is None or (expected_job_id is not None and existing.job_id != expected_job_id):
            return False
        del self._reservations[key]
        return True

    async def assign_worker(self, job_id: str, *, worker_id: str, attempt: int) -> bool:
        item = self._rows.get(job_id)
        if item is None or item[1] != "claimed" or item[0].attempt != attempt:
            return False
        self._rows[job_id] = (msgspec.structs.replace(item[0], worker_id=worker_id), item[1], worker_id)
        return True

    async def touch_heartbeats(self, job_ids: Sequence[str], *, heartbeat_at_ms: int) -> int:
        touched = 0
        for jid in job_ids:
            item = self._rows.get(jid)
            if item is not None and item[1] == "claimed":
                self._rows[jid] = (msgspec.structs.replace(item[0], heartbeat_at_ms=heartbeat_at_ms), item[1], item[2])
                touched += 1
        return touched

    async def null_heartbeats(self, job_ids: Sequence[str]) -> int:
        cleared = 0
        for jid in job_ids:
            item = self._rows.get(jid)
            if item is not None and item[0].heartbeat_at_ms is not None:
                self._rows[jid] = (msgspec.structs.replace(item[0], heartbeat_at_ms=None), item[1], item[2])
                cleared += 1
        return cleared

    async def interrupt_task(self, job_id: str, *, error: str | None = None) -> bool:
        item = self._rows.get(job_id)
        if item is None or item[1] not in ("ready", "claimed"):
            return False
        self._rows[job_id] = (
            msgspec.structs.replace(item[0], lease_until_ms=0, completed_at_ms=self._now_ms(), error=error),
            "interrupted",
            None,
        )
        return True

    async def expire_overdue(self, *, now_ms: int, limit: int | None = None) -> int:
        expired = 0
        for jid, (entry, st, _) in list(self._rows.items()):
            if limit is not None and expired >= limit:
                break
            if st == "ready" and entry.expires_at_ms is not None and entry.expires_at_ms <= now_ms:
                self._rows[jid] = (
                    msgspec.structs.replace(entry, lease_until_ms=0, completed_at_ms=now_ms),
                    "expired",
                    None,
                )
                expired += 1
        return expired

    async def next_due_delay(self, queues: Sequence[str]) -> float | None:
        now_ms = self._now_ms()
        scheds = [
            e.scheduled_at_ms
            for e, st, _ in self._rows.values()
            if st == "ready" and e.queue in queues and e.scheduled_at_ms is not None and e.scheduled_at_ms > now_ms
        ]
        return max(0.0, (min(scheds) - now_ms) / 1000.0) if scheds else None

    async def reserve_unique_key(self, key: str, *, ttl_seconds: float) -> bool:
        _ = ttl_seconds
        if key in self._unique_keys:
            return False
        self._unique_keys.add(key)
        return True

    async def release_unique_key(self, key: str) -> bool:
        if key in self._unique_keys:
            self._unique_keys.remove(key)
            return True
        return False

    async def acquire_lease(self, lease_key: str, *, owner_id: str, ttl_seconds: float) -> bool:
        now, existing = time.time(), self._leases.get(lease_key)
        if existing is None or existing[1] <= now or existing[0] == owner_id:
            self._leases[lease_key] = (owner_id, now + ttl_seconds)
            return True
        return False

    async def release_lease(self, lease_key: str, *, owner_id: str) -> bool:
        existing = self._leases.get(lease_key)
        if existing is not None and existing[0] == owner_id:
            del self._leases[lease_key]
            return True
        return False

    async def aclose(self) -> None:
        return None


class InMemoryAsyncNotificationSource(AsyncNotificationSource):
    """In-memory AsyncNotificationSource for wakeup and channel tests."""

    def __init__(self) -> None:
        self.subscribed: set[str] = set()
        self._queues: set[asyncio.Queue[NotificationEvent]] = set()
        self.listener_ready: asyncio.Event | None = None

    async def publish(self, channel: str, payload: bytes | str) -> None:
        raw = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
        evt = NotificationEvent(channel=channel, payload=raw, sender_id=None, metadata={})
        for q in tuple(self._queues):
            q.put_nowait(evt)

    async def subscribe(self, channels: Sequence[str]) -> None:
        self.subscribed.update(channels)

    async def unsubscribe(self, channels: Sequence[str]) -> None:
        for ch in channels:
            self.subscribed.discard(ch)

    async def listen(self) -> AsyncIterator[NotificationEvent]:
        q: asyncio.Queue[NotificationEvent] = asyncio.Queue()
        self._queues.add(q)
        if self.listener_ready is not None:
            self.listener_ready.set()
        try:
            while True:
                evt = await q.get()
                if not self.subscribed or evt.channel in self.subscribed:
                    yield evt
        finally:
            self._queues.discard(q)


class InMemoryRESPQueueStoreDriver:
    """In-memory RESP command and Lua script test double for RESPQueueStoreBase."""

    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = collections.defaultdict(dict)
        self.zsets: dict[str, dict[str, float]] = collections.defaultdict(dict)
        self.kv: dict[str, tuple[str, float | None]] = {}
        self.seq: int = 0
        self.closed: bool = False

    def _get_kv(self, key: str) -> str | None:
        item = self.kv.get(key)
        if item is None:
            return None
        if item[1] is not None and item[1] <= time.time():
            del self.kv[key]
            return None
        return item[0]

    def _load_job(self, jobs_key: str, jid: str) -> dict[str, Any] | None:
        raw = self.hashes[jobs_key].get(jid)
        return decode_json(raw.encode("utf-8")) if raw else None

    def _save_job(self, jobs_key: str, jid: str, data: dict[str, Any]) -> str:
        encoded = encode_json(data)
        self.hashes[jobs_key][jid] = encoded
        return encoded

    @staticmethod
    def _ready_score(data: dict[str, Any]) -> float:
        return float((-int(data.get("priority") or 0) * 1_000_000) + int(data.get("seq") or 0))

    def _mark_terminal(
        self,
        jobs_key: str,
        jid: str,
        data: dict[str, Any],
        status: str,
        now_ms: int,
        terminal_key: str,
        zkeys_to_pop: tuple[str, ...],
    ) -> None:
        comp_ms = data.get("completed_at_ms") or now_ms
        data.update(
            {
                "status": status,
                "lease_until_ms": 0,
                "worker_id": None,
                "completed_at_ms": comp_ms,
                "updated_at_ms": now_ms,
            }
        )
        self._save_job(jobs_key, jid, data)
        for zkey in zkeys_to_pop:
            self.zsets[zkey].pop(jid, None)
        self.zsets[terminal_key][jid] = float(comp_ms)

    def execute_command(self, *parts: Any) -> Any:
        """Execute a simulated synchronous RESP command."""
        cmd = str(parts[0]).upper()
        if cmd == "HGET":
            return self.hashes[str(parts[1])].get(str(parts[2]))
        if cmd == "HMGET":
            return [self.hashes[str(parts[1])].get(str(f)) for f in parts[2:]]
        if cmd == "GET":
            return self._get_kv(str(parts[1]))
        if cmd == "SET":
            key, val, nx = str(parts[1]), str(parts[2]), "NX" in parts[3:]
            px = next(
                (
                    int(parts[i + 1])
                    for i, t in enumerate(parts[3:], start=3)
                    if str(t).upper() == "PX" and i + 1 < len(parts)
                ),
                None,
            )
            if nx and self._get_kv(key) is not None:
                return False
            self.kv[key] = (val, time.time() + (px / 1000.0) if px is not None else None)
            return True
        if cmd == "DEL":
            return 1 if self.kv.pop(str(parts[1]), None) is not None else 0
        if cmd == "ZRANGE":
            ordered = sorted(self.zsets[str(parts[1])].items(), key=lambda pair: (pair[1], pair[0]))
            return [x for m, s in ordered for x in (m, s)] if "WITHSCORES" in parts else [m for m, _ in ordered]
        msg = f"Unsupported command {cmd}"
        raise NotImplementedError(msg)

    def _eval_enqueue(self, keys: Sequence[str], args: Sequence[Any]) -> str:
        jobs_key, order_key, _seq_key, sched_key, exp_key, by_key_hash, ready_key = keys
        jid, queue, record_json, now_ms_s, sched_s, prio_s, exp_s, idem_key = (str(a) for a in args)
        self.seq += 1
        data = decode_json(record_json.encode("utf-8"))
        data["seq"] = self.seq
        self._save_job(jobs_key, jid, data)
        self.zsets[order_key][jid] = float(self.seq)
        if sched_s and int(sched_s) > int(now_ms_s):
            self.zsets[sched_key][jid] = float(sched_s)
        else:
            self.zsets[ready_key][jid] = float((-int(prio_s) * 1_000_000) + self.seq)
        if exp_s:
            self.zsets[exp_key][jid] = float(exp_s)
        if idem_key:
            self.hashes[by_key_hash][f"any:{idem_key}"] = jid
            self.hashes[by_key_hash][f"q:{queue}:{idem_key}"] = jid
        return jid

    def _eval_claim(self, keys: Sequence[str], args: Sequence[Any]) -> list[str]:
        jobs_key, claimed_key, *ready_keys = keys
        limit, worker_id, lease_until_s, now_ms_val = int(args[0]), str(args[1]), int(args[2]), int(args[3])
        claimed: list[str] = []
        for rkey in ready_keys:
            if len(claimed) >= limit:
                break
            for jid, _ in sorted(self.zsets[rkey].items(), key=lambda p: (p[1], p[0]))[: limit - len(claimed)]:
                self.zsets[rkey].pop(jid, None)
                data = self._load_job(jobs_key, jid)
                if data is not None and data.get("status") == "ready":
                    data.update(
                        {
                            "status": "claimed",
                            "worker_id": worker_id,
                            "lease_until_ms": lease_until_s,
                            "updated_at_ms": now_ms_val,
                        }
                    )
                    claimed.append(self._save_job(jobs_key, jid, data))
                    self.zsets[claimed_key][jid] = float(lease_until_s)
        return claimed

    def _eval_nack(self, keys: Sequence[str], args: Sequence[Any]) -> int:
        jobs_key, claimed_key, sched_key, terminal_key, exp_key = keys
        jid, attempt, error_msg, delay_ms_s, now_ms, tag = (
            str(args[0]),
            int(args[1]),
            str(args[2]),
            str(args[3]),
            int(args[4]),
            str(args[5]),
        )
        data = self._load_job(jobs_key, jid)
        if data is None or data.get("status") != "claimed" or int(data["attempt"]) != attempt:
            return 0
        self.zsets[claimed_key].pop(jid, None)
        data["error"] = error_msg
        if int(data["attempt"]) >= int(data["max_attempts"]) or delay_ms_s == "":
            self._mark_terminal(jobs_key, jid, data, "dead_letter", now_ms, terminal_key, (exp_key,))
            return 1
        data.update(
            {
                "attempt": int(data["attempt"]) + 1,
                "status": "ready",
                "lease_until_ms": 0,
                "worker_id": None,
                "updated_at_ms": now_ms,
            }
        )
        delay_val = int(delay_ms_s)
        if delay_val > 0:
            data["scheduled_at_ms"] = now_ms + delay_val
            self.zsets[sched_key][jid] = float(now_ms + delay_val)
        else:
            data["scheduled_at_ms"] = None
            self.zsets[f"{tag}:ready:{data['queue']}"][jid] = self._ready_score(data)
        self._save_job(jobs_key, jid, data)
        return 1

    def _eval_reap_stale(self, keys: Sequence[str], args: Sequence[Any]) -> int:
        jobs_key, claimed_key, terminal_key, exp_key = keys
        now_ms, tag, reaped = int(args[0]), str(args[1]), 0
        for jid in [j for j, score in self.zsets[claimed_key].items() if score <= now_ms]:
            self.zsets[claimed_key].pop(jid, None)
            data = self._load_job(jobs_key, jid)
            if data is not None and data.get("status") == "claimed":
                if int(data["attempt"]) >= int(data["max_attempts"]):
                    data["error"] = data.get("error") or "Visibility lease expired"
                    self._mark_terminal(jobs_key, jid, data, "dead_letter", now_ms, terminal_key, (exp_key,))
                else:
                    data.update(
                        {
                            "status": "ready",
                            "attempt": int(data["attempt"]) + 1,
                            "worker_id": None,
                            "lease_until_ms": 0,
                            "updated_at_ms": now_ms,
                        }
                    )
                    self.zsets[f"{tag}:ready:{data['queue']}"][jid] = self._ready_score(data)
                    self._save_job(jobs_key, jid, data)
                reaped += 1
        return reaped

    def _eval_cleanup_terminal(self, keys: Sequence[str], args: Sequence[Any]) -> int:
        jobs_key, order_key, terminal_key, exp_key, claimed_key, by_key_hash = keys
        before_ms, limit = int(args[0]), int(args[1])
        eligible = [j for j, s in sorted(self.zsets[terminal_key].items(), key=lambda p: p[1]) if s < before_ms]
        eligible = eligible[:limit] if limit >= 0 else eligible
        for jid in eligible:
            raw = self.hashes[jobs_key].pop(jid, None)
            if raw:
                data = decode_json(raw.encode("utf-8"))
                headers = data.get("headers") if isinstance(data.get("headers"), dict) else {}
                for hk in (headers.get("key"), headers.get("idempotency_key")):
                    if hk:
                        for prefix_k in (f"any:{hk}", f"q:{data['queue']}:{hk}"):
                            if self.hashes[by_key_hash].get(prefix_k) == jid:
                                self.hashes[by_key_hash].pop(prefix_k, None)
            for zkey in (order_key, terminal_key, exp_key, claimed_key):
                self.zsets[zkey].pop(jid, None)
        return len(eligible)

    def eval_script(self, script: str, keys: Sequence[str] = (), args: Sequence[Any] = ()) -> Any:
        """Execute a simulated Lua queue-store script atomically."""
        if script is resp_queue_mod._STORE_ENQUEUE_LUA:
            return self._eval_enqueue(keys, args)
        if script is resp_queue_mod._STORE_CLAIM_LUA:
            return self._eval_claim(keys, args)
        if script is resp_queue_mod._STORE_NACK_LUA:
            return self._eval_nack(keys, args)
        if script is resp_queue_mod._STORE_REAP_STALE_LUA:
            return self._eval_reap_stale(keys, args)
        if script is resp_queue_mod._STORE_CLEANUP_TERMINAL_LUA:
            return self._eval_cleanup_terminal(keys, args)
        if script is resp_queue_mod._STORE_ACK_LUA:
            jobs_key, claimed_key, terminal_key, exp_key = keys
            data = self._load_job(jobs_key, str(args[0]))
            if data is None or data.get("status") != "claimed" or int(data["attempt"]) != int(args[1]):
                return 0
            self._mark_terminal(
                jobs_key, str(args[0]), data, "completed", int(args[2]), terminal_key, (claimed_key, exp_key)
            )
            return 1
        if script is resp_queue_mod._STORE_HEARTBEAT_LUA:
            jobs_key, claimed_key = keys
            worker_id, lease_until_ms, now_ms, touched = str(args[0]), int(args[1]), int(args[2]), 0
            for jid in (str(a) for a in args[3:]):
                data = self._load_job(jobs_key, jid)
                if data is not None and data.get("status") == "claimed" and data.get("worker_id") == worker_id:
                    data.update({"lease_until_ms": lease_until_ms, "updated_at_ms": now_ms})
                    self._save_job(jobs_key, jid, data)
                    self.zsets[claimed_key][jid] = float(lease_until_ms)
                    touched += 1
            return touched
        if script is resp_queue_mod._STORE_PROMOTE_SCHEDULED_LUA:
            jobs_key, sched_key = keys
            now_ms, limit, tag, promoted = int(args[0]), int(args[1]), str(args[2]), 0
            for jid in [j for j, s in sorted(self.zsets[sched_key].items(), key=lambda p: p[1]) if s <= now_ms][:limit]:
                self.zsets[sched_key].pop(jid, None)
                data = self._load_job(jobs_key, jid)
                if data is not None and data.get("status") == "ready":
                    data.update({"scheduled_at_ms": None, "updated_at_ms": now_ms})
                    self.zsets[f"{tag}:ready:{data['queue']}"][jid] = self._ready_score(data)
                    self._save_job(jobs_key, jid, data)
                    promoted += 1
            return promoted
        if script is resp_queue_mod._STORE_CANCEL_LUA:
            jobs_key, sched_key, terminal_key, exp_key = keys
            jid, now_ms, tag = str(args[0]), int(args[1]), str(args[2])
            data = self._load_job(jobs_key, jid)
            if data is None or data.get("status") != "ready":
                return 0
            self._mark_terminal(
                jobs_key,
                jid,
                data,
                "cancelled",
                now_ms,
                terminal_key,
                (f"{tag}:ready:{data['queue']}", sched_key, exp_key),
            )
            return 1
        if script is resp_queue_mod._STORE_RESERVE_IDENTITY_LUA:
            res_key, key_name, jid, task_name, now_ms, ttl_ms = (
                keys[0],
                str(args[0]),
                str(args[1]),
                str(args[2]),
                int(args[3]),
                int(args[4]),
            )
            existing = self._get_kv(res_key)
            if existing is not None:
                exp = decode_json(existing.encode("utf-8")).get("expires_at_ms")
                if exp is None or int(exp) > now_ms:
                    return existing
            payload = encode_json(
                {
                    "key": key_name,
                    "job_id": jid,
                    "task_name": task_name,
                    "created_at_ms": now_ms,
                    "expires_at_ms": now_ms + ttl_ms if ttl_ms >= 0 else None,
                }
            )
            if ttl_ms != 0:
                self.kv[res_key] = (payload, time.time() + (ttl_ms / 1000.0) if ttl_ms > 0 else None)
            return None
        if script is resp_queue_mod._STORE_RESET_IDENTITY_LUA:
            res_key, expected = keys[0], str(args[0])
            existing = self._get_kv(res_key)
            if existing is None or (expected and decode_json(existing.encode("utf-8")).get("job_id") != expected):
                return 0
            self.kv.pop(res_key, None)
            return 1
        if script is resp_queue_mod._STORE_ASSIGN_WORKER_LUA:
            jid, worker_id, attempt, now_ms = str(args[0]), str(args[1]), int(args[2]), int(args[3])
            data = self._load_job(keys[0], jid)
            if data is None or data.get("status") != "claimed" or int(data["attempt"]) != attempt:
                return 0
            data.update({"worker_id": worker_id, "updated_at_ms": now_ms})
            self._save_job(keys[0], jid, data)
            return 1
        if script is resp_queue_mod._STORE_TOUCH_HEARTBEATS_LUA:
            hb_ms, now_ms, touched = int(args[0]), int(args[1]), 0
            for jid in (str(a) for a in args[2:]):
                data = self._load_job(keys[0], jid)
                if data is not None and data.get("status") == "claimed":
                    data.update({"heartbeat_at_ms": hb_ms, "updated_at_ms": now_ms})
                    self._save_job(keys[0], jid, data)
                    touched += 1
            return touched
        if script is resp_queue_mod._STORE_NULL_HEARTBEATS_LUA:
            now_ms, cleared = int(args[0]), 0
            for jid in (str(a) for a in args[1:]):
                data = self._load_job(keys[0], jid)
                if data is not None and data.get("heartbeat_at_ms") is not None:
                    data.update({"heartbeat_at_ms": None, "updated_at_ms": now_ms})
                    self._save_job(keys[0], jid, data)
                    cleared += 1
            return cleared
        if script is resp_queue_mod._STORE_INTERRUPT_TASK_LUA:
            jobs_key, claimed_key, sched_key, terminal_key, exp_key = keys
            jid, has_err, err_msg, now_ms, tag = str(args[0]), str(args[1]), str(args[2]), int(args[3]), str(args[4])
            data = self._load_job(jobs_key, jid)
            if data is None or data.get("status") not in ("ready", "claimed"):
                return 0
            if has_err == "1":
                data["error"] = err_msg
            self._mark_terminal(
                jobs_key,
                jid,
                data,
                "interrupted",
                now_ms,
                terminal_key,
                (f"{tag}:ready:{data['queue']}", claimed_key, sched_key, exp_key),
            )
            return 1
        if script is resp_queue_mod._STORE_EXPIRE_OVERDUE_LUA:
            jobs_key, exp_key, sched_key, terminal_key = keys
            now_ms, limit, tag, expired = int(args[0]), int(args[1]), str(args[2]), 0
            eligible = [j for j, s in sorted(self.zsets[exp_key].items(), key=lambda p: p[1]) if s <= now_ms]
            for jid in eligible[:limit] if limit >= 0 else eligible:
                data = self._load_job(jobs_key, jid)
                if (
                    data is not None
                    and data.get("status") == "ready"
                    and data.get("expires_at_ms") is not None
                    and int(data["expires_at_ms"]) <= now_ms
                ):
                    self._mark_terminal(
                        jobs_key,
                        jid,
                        data,
                        "expired",
                        now_ms,
                        terminal_key,
                        (f"{tag}:ready:{data['queue']}", sched_key, exp_key),
                    )
                    expired += 1
            return expired
        if script is resp_queue_mod._STORE_ACQUIRE_LEASE_LUA:
            cur = self._get_kv(keys[0])
            if cur is None or cur == str(args[0]):
                self.kv[keys[0]] = (str(args[0]), time.time() + (int(args[1]) / 1000.0))
                return 1
            return 0
        if script is resp_queue_mod._STORE_RELEASE_LEASE_LUA:
            if self._get_kv(keys[0]) == str(args[0]):
                self.kv.pop(keys[0], None)
                return 1
            return 0
        msg = "Unknown Lua script"
        raise NotImplementedError(msg)

    def close(self) -> None:
        """Mark driver closed."""
        self.closed = True


class InMemoryAsyncRESPQueueStoreDriver:
    """Async adapter over InMemoryRESPQueueStoreDriver for AsyncQueueStore tests."""

    def __init__(self) -> None:
        self._sync = InMemoryRESPQueueStoreDriver()

    async def execute_command(self, *parts: Any) -> Any:
        """Execute a simulated async RESP command."""
        return self._sync.execute_command(*parts)

    async def eval_script(self, script: str, keys: Sequence[str] = (), args: Sequence[Any] = ()) -> Any:
        """Execute a simulated async Lua script."""
        return self._sync.eval_script(script, keys=keys, args=args)

    async def close(self) -> None:
        """Close the async driver."""
        self._sync.close()
