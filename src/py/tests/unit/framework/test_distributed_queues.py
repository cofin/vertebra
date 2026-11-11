"""Unit and contract tests for distributed queue backends, cron scheduling, and worker lanes."""

from __future__ import annotations

import asyncio
import collections
import sys
import time
import types
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import msgspec
import pytest

from vertebra.db.adapters.postgres import PostgreSQLConfig
from vertebra.framework.queues import (
    CronSchedule,
    MemoryQueueBackend,
    OracleQueueBackend,
    OracleQueueExecutor,
    PostgreSQLQueueBackend,
    PostgreSQLQueueExecutor,
    QueueConfig,
    QueueService,
    RedisQueueBackend,
    RedisStreamClient,
    TaskEnvelope,
    TaskRetryPolicy,
    ValkeyQueueBackend,
    task,
)
from vertebra.framework.queues.scheduler import CronScheduler
from vertebra.server.lanes import LaneConfig, WorkerRunner
from vertebra.server.platform import LameDuckController
from vertebra.telemetry.otel import SpanExporter, TraceContext
from vertebra.typing import (
    ImproperConfigurationError,
    MissingDependencyError,
    reset_dependency_cache,
)
from vertebra.utils.serializers import decode_json

if TYPE_CHECKING:
    from collections.abc import Mapping


class _FakeOracleQueueExecutor:
    """In-memory transactional SQL simulation of Oracle FOR UPDATE SKIP LOCKED semantics."""

    __slots__ = ("_dlq", "_jobs", "_leases", "_lock")

    def __init__(self) -> None:
        self._jobs: dict[str, dict[str, Any]] = {}
        self._dlq: dict[str, dict[str, Any]] = {}
        self._leases: dict[str, dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def execute(self, query: str, *params: Any) -> int:
        """Execute mock DML query."""
        async with self._lock:
            q = query.strip().upper()
            if "INSERT INTO VERTEBRA_QUEUE_JOBS" in q:
                (
                    task_id,
                    task_name,
                    lane,
                    payload,
                    attempt,
                    max_retries,
                    status,
                    enq,
                    sched,
                    locked,
                    cname,
                    handle,
                    ikey,
                    tp,
                ) = params
                if ikey and any(j.get("idempotency_key") == ikey for j in self._jobs.values()):
                    return 0
                self._jobs[task_id] = {
                    "task_id": task_id,
                    "task_name": task_name,
                    "lane": lane,
                    "payload": payload,
                    "attempt": attempt,
                    "max_retries": max_retries,
                    "status": status,
                    "enqueued_at": enq,
                    "scheduled_at": sched,
                    "locked_until": locked,
                    "consumer_name": cname,
                    "receipt_handle": handle,
                    "idempotency_key": ikey,
                    "traceparent": tp,
                }
                return 1

            if "DELETE FROM VERTEBRA_QUEUE_JOBS" in q:
                task_id = params[0]
                if task_id in self._jobs:
                    del self._jobs[task_id]
                    return 1
                return 0

            if "INSERT INTO VERTEBRA_QUEUE_DLQ" in q:
                dlq_id, task_id, task_name, lane, payload, error_type, error_msg, failed_at = params
                self._dlq[dlq_id] = {
                    "dlq_id": dlq_id,
                    "task_id": task_id,
                    "task_name": task_name,
                    "lane": lane,
                    "payload": payload,
                    "error_type": error_type,
                    "error_message": error_msg,
                    "failed_at": failed_at,
                }
                return 1

            if "SET SCHEDULED_AT = NULL" in q:
                current_time = params[0]
                promoted = 0
                for job in self._jobs.values():
                    if (
                        job.get("status") == "pending"
                        and job.get("scheduled_at") is not None
                        and job["scheduled_at"] <= current_time
                    ):
                        job["scheduled_at"] = None
                        promoted += 1
                return promoted

            if "UPDATE VERTEBRA_QUEUE_JOBS" in q:
                status, attempt, sched, locked, handle, task_id = params
                if task_id in self._jobs:
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
                return 0

            return 0

    async def fetch_all(self, query: str, *params: Any) -> list[dict[str, Any]]:
        """Fetch all matching rows."""
        async with self._lock:
            q = query.strip().upper()
            if "FOR UPDATE SKIP LOCKED" in q:
                lanes, now_ts, limit, locked_until, consumer_name, receipt_handle = params
                target_lanes = set(lanes)
                matched: list[dict[str, Any]] = []
                sorted_jobs = sorted(
                    self._jobs.values(),
                    key=lambda j: (j.get("scheduled_at") or j.get("enqueued_at") or 0.0, j.get("enqueued_at") or 0.0),
                )
                for job in sorted_jobs:
                    if len(matched) >= limit:
                        break
                    if job.get("lane") not in target_lanes:
                        continue
                    status = job.get("status")
                    sched = job.get("scheduled_at")
                    locked = job.get("locked_until")
                    is_ready_pending = status == "pending" and (sched is None or sched <= now_ts)
                    is_expired_running = status == "running" and locked is not None and locked <= now_ts
                    if is_ready_pending or is_expired_running:
                        job_handle = receipt_handle or uuid.uuid4().hex
                        job["status"] = "running"
                        job["locked_until"] = locked_until
                        job["consumer_name"] = consumer_name
                        job["receipt_handle"] = job_handle
                        if is_expired_running:
                            job["attempt"] = int(job.get("attempt", 1)) + 1
                        matched.append(dict(job))
                return matched

            if "SELECT * FROM VERTEBRA_QUEUE_DLQ" in q:
                limit = params[0] if params else 100
                return [dict(e) for e in list(self._dlq.values())[:limit]]

            return []

    async def fetch_one(self, query: str, *params: Any) -> dict[str, Any] | None:
        """Fetch single matching row."""
        async with self._lock:
            q = query.strip().upper()
            if "MERGE INTO VERTEBRA_QUEUE_LEASES" in q:
                lease_key, owner_id, expires_at, last_slot, check_now = params
                existing = self._leases.get(lease_key)
                if existing is None or existing["expires_at"] <= check_now or existing["owner_id"] == owner_id:
                    new_slot = last_slot or (existing.get("last_slot") if existing else None)
                    record = {
                        "lease_key": lease_key,
                        "owner_id": owner_id,
                        "expires_at": expires_at,
                        "last_slot": new_slot,
                    }
                    self._leases[lease_key] = record
                    return dict(record)
                return None

            if "SELECT * FROM VERTEBRA_QUEUE_DLQ WHERE DLQ_ID" in q:
                dlq_id = params[0]
                if dlq_id in self._dlq:
                    return dict(self._dlq[dlq_id])
                return None

            return None

    async def commit(self) -> None:
        """Commit active mock transaction."""
        return None

    async def close(self) -> None:
        """Clear state."""
        async with self._lock:
            self._jobs.clear()
            self._dlq.clear()
            self._leases.clear()


class _FakePostgreSQLQueueExecutor:
    """In-memory PostgreSQL queue executor simulating SKIP LOCKED claims and LISTEN/NOTIFY."""

    __slots__ = ("_dlq", "_jobs", "_leases", "_lock", "_waiters")

    def __init__(self) -> None:
        self._jobs: dict[str, dict[str, Any]] = {}
        self._dlq: dict[str, dict[str, Any]] = {}
        self._leases: dict[str, dict[str, Any]] = {}
        self._waiters: dict[str, list[asyncio.Event]] = collections.defaultdict(list)
        self._lock = asyncio.Lock()

    async def execute(self, query: str, *params: Any) -> int:
        """Execute mock query."""
        async with self._lock:
            q = query.strip().upper()
            if "INSERT INTO VERTEBRA_QUEUE_JOBS" in q:
                (
                    task_id,
                    task_name,
                    lane,
                    payload,
                    attempt,
                    max_retries,
                    status,
                    enq,
                    sched,
                    locked,
                    cname,
                    handle,
                    ikey,
                    tp,
                ) = params
                if ikey and any(j.get("idempotency_key") == ikey for j in self._jobs.values()):
                    return 0
                self._jobs[task_id] = {
                    "task_id": task_id,
                    "task_name": task_name,
                    "lane": lane,
                    "payload": payload,
                    "attempt": attempt,
                    "max_retries": max_retries,
                    "status": status,
                    "enqueued_at": enq,
                    "scheduled_at": sched,
                    "locked_until": locked,
                    "consumer_name": cname,
                    "receipt_handle": handle,
                    "idempotency_key": ikey,
                    "traceparent": tp,
                }
                return 1

            if "DELETE FROM VERTEBRA_QUEUE_JOBS" in q:
                task_id = params[0]
                if task_id in self._jobs:
                    del self._jobs[task_id]
                    return 1
                return 0

            if "INSERT INTO VERTEBRA_QUEUE_DLQ" in q:
                dlq_id, task_id, task_name, lane, payload, error_type, error_msg, failed_at = params
                self._dlq[dlq_id] = {
                    "dlq_id": dlq_id,
                    "task_id": task_id,
                    "task_name": task_name,
                    "lane": lane,
                    "payload": payload,
                    "error_type": error_type,
                    "error_message": error_msg,
                    "failed_at": failed_at,
                }
                return 1

            if "SET SCHEDULED_AT = NULL" in q:
                current_time = params[0]
                promoted = 0
                for job in self._jobs.values():
                    if (
                        job.get("status") == "pending"
                        and job.get("scheduled_at") is not None
                        and job["scheduled_at"] <= current_time
                    ):
                        job["scheduled_at"] = None
                        promoted += 1
                return promoted

            if "UPDATE VERTEBRA_QUEUE_JOBS" in q:
                status, attempt, sched, locked, handle, task_id = params
                if task_id in self._jobs:
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
                return 0

            return 0

    async def fetch_all(self, query: str, *params: Any) -> list[dict[str, Any]]:
        """Fetch all matching rows."""
        async with self._lock:
            q = query.strip().upper()
            if "WITH CLAIMED AS" in q or "FOR UPDATE SKIP LOCKED" in q:
                lanes, now_ts, limit, locked_until, consumer_name, receipt_handle = params
                target_lanes = set(lanes)
                matched: list[dict[str, Any]] = []
                sorted_jobs = sorted(
                    self._jobs.values(),
                    key=lambda j: (j.get("scheduled_at") or j.get("enqueued_at") or 0.0, j.get("enqueued_at") or 0.0),
                )
                for job in sorted_jobs:
                    if len(matched) >= limit:
                        break
                    if job.get("lane") not in target_lanes:
                        continue
                    status = job.get("status")
                    sched = job.get("scheduled_at")
                    locked = job.get("locked_until")
                    is_ready_pending = status == "pending" and (sched is None or sched <= now_ts)
                    is_expired_running = status == "running" and locked is not None and locked <= now_ts
                    if is_ready_pending or is_expired_running:
                        job_handle = receipt_handle or uuid.uuid4().hex
                        job["status"] = "running"
                        job["locked_until"] = locked_until
                        job["consumer_name"] = consumer_name
                        job["receipt_handle"] = job_handle
                        if is_expired_running:
                            job["attempt"] = int(job.get("attempt", 1)) + 1
                        matched.append(dict(job))
                return matched

            if "SELECT * FROM VERTEBRA_QUEUE_DLQ" in q:
                limit = params[0] if params else 100
                return [dict(e) for e in list(self._dlq.values())[:limit]]

            return []

    async def fetch_one(self, query: str, *params: Any) -> dict[str, Any] | None:
        """Fetch a single matching row."""
        async with self._lock:
            q = query.strip().upper()
            if "SELECT MIN(SCHEDULED_AT)" in q:
                lanes, now_ts = params
                target_lanes = set(lanes)
                min_sched: float | None = None
                for job in self._jobs.values():
                    if job.get("lane") in target_lanes and job.get("status") == "pending":
                        sched = job.get("scheduled_at")
                        if sched is not None and sched > now_ts and (min_sched is None or sched < min_sched):
                            min_sched = sched
                return {"min_scheduled": min_sched}

            if "INSERT INTO VERTEBRA_QUEUE_LEASES" in q:
                lease_key, owner_id, expires_at, last_slot, check_now = params
                existing = self._leases.get(lease_key)
                if existing is None or existing["expires_at"] <= check_now or existing["owner_id"] == owner_id:
                    new_slot = last_slot or (existing.get("last_slot") if existing else None)
                    record = {
                        "lease_key": lease_key,
                        "owner_id": owner_id,
                        "expires_at": expires_at,
                        "last_slot": new_slot,
                    }
                    self._leases[lease_key] = record
                    return dict(record)
                return None

            if "SELECT * FROM VERTEBRA_QUEUE_DLQ WHERE DLQ_ID" in q:
                dlq_id = params[0]
                if dlq_id in self._dlq:
                    return dict(self._dlq[dlq_id])
                return None

            return None

    async def notify(self, channel: str, payload: str) -> None:
        """Wake up any tasks waiting on channel."""
        async with self._lock:
            waiters = self._waiters.get(channel, [])
            for evt in waiters:
                evt.set()

    async def wait_for_notify(self, channel: str, timeout_seconds: float) -> bool:
        """Wait for notification on channel until timeout."""
        evt = asyncio.Event()
        async with self._lock:
            self._waiters[channel].append(evt)
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
    """In-memory Redis Streams client emulating consumer groups, sorted sets, and the queue Lua scripts."""

    __slots__ = ("_groups", "_kv", "_pel", "_streams", "_zsets")

    def __init__(self) -> None:
        self._streams: dict[str, list[tuple[str, dict[str, Any]]]] = collections.defaultdict(list)
        self._groups: dict[str, set[str]] = collections.defaultdict(set)
        self._pel: dict[str, dict[str, tuple[str, float, dict[str, Any]]]] = collections.defaultdict(dict)
        self._zsets: dict[str, dict[str, float]] = collections.defaultdict(dict)
        self._kv: dict[str, tuple[str, float | None]] = {}

    async def xgroup_create(
        self,
        stream: str,
        group: str,
        id: str = "$",
        mkstream: bool = False,
    ) -> bool:
        """Register consumer group for stream."""
        if mkstream and stream not in self._streams:
            self._streams[stream] = []
        self._groups[stream].add(group)
        return True

    async def xadd(
        self,
        stream: str,
        fields: Mapping[str, Any],
        id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
    ) -> str:
        """Append stream message."""
        entry_id = f"{int(time.time() * 1000)}-{len(self._streams[stream])}"
        converted = {str(k): (v.decode("utf-8") if isinstance(v, bytes) else str(v)) for k, v in fields.items()}
        self._streams[stream].append((entry_id, converted))
        if maxlen and len(self._streams[stream]) > maxlen:
            trim_count = len(self._streams[stream]) - maxlen
            del self._streams[stream][:trim_count]
        return entry_id

    async def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: Mapping[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> list[Any]:
        """Read unclaimed entries for consumer group."""
        limit = count or 10
        results: list[tuple[str, list[tuple[str, dict[str, Any]]]]] = []
        now = time.time()
        for stream_name, _ in streams.items():
            pel_key = f"{stream_name}:{group}"
            stream_entries = self._streams.get(stream_name, [])
            claimed_in_pel = set(self._pel[pel_key].keys())
            matched: list[tuple[str, dict[str, Any]]] = []
            for entry_id, fields in stream_entries:
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
        acked = 0
        for entry_id in ids:
            if entry_id in self._pel[pel_key]:
                del self._pel[pel_key][entry_id]
                acked += 1
        return acked

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
        limit = count or 10
        pel_key = f"{stream}:{group}"
        now = time.time()
        idle_threshold_s = min_idle_time / 1000.0
        reclaimed: list[tuple[str, dict[str, Any]]] = []
        for entry_id, (_, last_delivered, fields) in list(self._pel[pel_key].items()):
            if now - last_delivered >= idle_threshold_s:
                self._pel[pel_key][entry_id] = (consumer, now, fields)
                reclaimed.append((entry_id, fields))
                if len(reclaimed) >= limit:
                    break
        return ("0-0", reclaimed, [])

    async def xrange(
        self,
        stream: str,
        min: str = "-",
        max: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """List stream entries in FIFO order."""
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
        added = 0
        for member, score in mapping.items():
            if member not in self._zsets[key]:
                added += 1
            self._zsets[key][member] = float(score)
        return added

    async def eval(self, script: str, numkeys: int, *keys_and_args: Any) -> Any:
        """Simulate execution of queue promotion and lease renewal scripts."""
        if "ZRANGEBYSCORE" in script:
            scheduled_key = str(keys_and_args[0])
            now_ts = float(keys_and_args[1])
            prefix = str(keys_and_args[2])
            limit = int(keys_and_args[3])
            zset = self._zsets[scheduled_key]
            eligible = [
                (member, score) for member, score in sorted(zset.items(), key=lambda pair: pair[1]) if score <= now_ts
            ][:limit]
            promoted = 0
            for raw_json, _ in eligible:
                raw_bytes = raw_json.encode("utf-8") if isinstance(raw_json, str) else raw_json
                env = msgspec.convert(decode_json(raw_bytes), type=TaskEnvelope)
                lane = env.lane
                stream_key = f"{prefix}:lane:{lane}"
                await self.xadd(
                    stream_key,
                    {
                        "payload": raw_json,
                        "lane": lane,
                        "task_name": env.task_name,
                    },
                )
                zset.pop(raw_json, None)
                promoted += 1
            return promoted

        if "current_owner == owner_id" in script:
            lease_key = str(keys_and_args[0])
            owner_id = str(keys_and_args[1])
            ttl_ms = float(keys_and_args[2])
            new_slot = str(keys_and_args[3]) if len(keys_and_args) > 3 else ""
            existing_tuple = self._kv.get(lease_key)
            if existing_tuple:
                current_val, exp = existing_tuple
                now = time.time()
                if exp is None or exp > now:
                    parts = current_val.split(":", 1)
                    current_owner = parts[0]
                    if current_owner == owner_id:
                        slot_to_save = new_slot if new_slot else (parts[1] if len(parts) > 1 else "")
                        val_to_save = f"{owner_id}:{slot_to_save}" if slot_to_save else owner_id
                        self._kv[lease_key] = (val_to_save, now + (ttl_ms / 1000.0))
                        return 1
            return 0

        return None

    async def set(
        self,
        key: str,
        value: str,
        nx: bool = False,
        px: int | None = None,
    ) -> bool:
        """Store key value pair."""
        now = time.time()
        existing = self._kv.get(key)
        if existing:
            _, exp = existing
            if exp is not None and exp <= now:
                self._kv.pop(key, None)
                existing = None
        if nx and existing is not None:
            return False
        expires_at = now + (px / 1000.0) if px else None
        self._kv[key] = (value, expires_at)
        return True

    async def get(self, key: str) -> str | None:
        """Fetch value if not expired."""
        existing = self._kv.get(key)
        if not existing:
            return None
        val, exp = existing
        if exp is not None and exp <= time.time():
            self._kv.pop(key, None)
            return None
        return val

    async def aclose(self) -> None:
        """Clear memory structures."""
        self._streams.clear()
        self._groups.clear()
        self._pel.clear()
        self._zsets.clear()
        self._kv.clear()


def _create_backend_by_url(url: str, config: QueueConfig) -> Any:
    """Instantiate queue backends, injecting the Redis client and the Oracle and PostgreSQL executor fakes."""
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
        return PostgreSQLQueueBackend(cfg, executor=_FakePostgreSQLQueueExecutor())
    if url.startswith("oracle"):
        return OracleQueueBackend(cfg, executor=_FakeOracleQueueExecutor())
    msg = f"Unknown backend url: {url}"
    raise ValueError(msg)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "backend_url",
    [
        "memory://",
        "redis://localhost:6379/0",
        "valkey://localhost:6379/0",
        "postgresql://localhost:5432/vtb",
        "oracle://localhost:1521/FREEPDB1",
    ],
)
async def test_queue_backends_fifo_and_lane_isolation(backend_url: str) -> None:
    """Verify FIFO ordering and lane isolation across all queue backends."""
    config = QueueConfig(backend_url=backend_url)
    backend = _create_backend_by_url(backend_url, config)

    env_cpu = TaskEnvelope(
        task_id="cpu-1",
        task_name="compute",
        lane="cpu",
        args=(10,),
    )
    env_io1 = TaskEnvelope(
        task_id="io-1",
        task_name="fetch",
        lane="io",
        args=("https://example.com/1",),
    )
    env_io2 = TaskEnvelope(
        task_id="io-2",
        task_name="fetch",
        lane="io",
        args=("https://example.com/2",),
    )

    await backend.enqueue(env_cpu)
    await backend.enqueue(env_io1)
    await backend.enqueue(env_io2)

    claimed_cpu = await backend.dequeue(["cpu"], batch_size=10)
    assert len(claimed_cpu) == 1
    assert claimed_cpu[0].task_id == "cpu-1"
    await backend.ack(claimed_cpu[0])

    claimed_io = await backend.dequeue(["io"], batch_size=10)
    assert len(claimed_io) == 2
    assert [e.task_id for e in claimed_io] == ["io-1", "io-2"]
    for e in claimed_io:
        await backend.ack(e)

    empty_check = await backend.dequeue(["cpu", "io"], batch_size=10)
    assert len(empty_check) == 0
    await backend.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "backend_url",
    [
        "redis://localhost:6379/0",
        "valkey://localhost:6379/0",
        "postgresql://localhost:5432/vtb",
        "oracle://localhost:1521/FREEPDB1",
    ],
)
async def test_queue_backends_concurrent_workers_no_duplicate_delivery(backend_url: str) -> None:
    """Verify concurrent workers never receive duplicate task deliveries."""
    config = QueueConfig(backend_url=backend_url)
    backend = _create_backend_by_url(backend_url, config)

    total_tasks = 40
    for i in range(total_tasks):
        env = TaskEnvelope(
            task_id=f"task-{i}",
            task_name="process_item",
            lane="io",
            args=(i,),
        )
        await backend.enqueue(env)

    delivered_ids: list[str] = []
    delivered_lock = asyncio.Lock()

    async def worker_loop(w_idx: int) -> None:
        while True:
            async with delivered_lock:
                if len(delivered_ids) >= total_tasks:
                    break
            batch = await backend.dequeue(["io"], batch_size=5)
            if not batch:
                await asyncio.sleep(0.005)
                continue
            for item in batch:
                async with delivered_lock:
                    delivered_ids.append(item.task_id)
                await backend.ack(item)

    workers = [asyncio.create_task(worker_loop(i)) for i in range(4)]
    await asyncio.gather(*workers)

    assert len(delivered_ids) == total_tasks
    assert len(set(delivered_ids)) == total_tasks
    await backend.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "backend_url",
    [
        "memory://",
        "redis://localhost:6379/0",
        "valkey://localhost:6379/0",
        "postgresql://localhost:5432/vtb",
        "oracle://localhost:1521/FREEPDB1",
    ],
)
async def test_queue_backends_visibility_timeout_reclaims_unacked_tasks(backend_url: str) -> None:
    """Verify unacknowledged tasks are reclaimed after visibility timeout elapses."""
    config = QueueConfig(
        backend_url=backend_url,
        visibility_timeout_seconds=0.05,
        consumer_name="worker-a",
    )
    backend = _create_backend_by_url(backend_url, config)

    env = TaskEnvelope(
        task_id="crash-task",
        task_name="flaky_op",
        lane="io",
        args=("val",),
    )
    await backend.enqueue(env)

    claimed_a = await backend.dequeue(["io"], batch_size=1)
    assert len(claimed_a) == 1
    assert claimed_a[0].task_id == "crash-task"
    assert claimed_a[0].attempt == 1

    await asyncio.sleep(0.06)

    if backend_url.startswith("memory"):
        backend_b = backend
    else:
        config_b = QueueConfig(
            backend_url=backend_url,
            visibility_timeout_seconds=0.05,
            consumer_name="worker-b",
        )
        backend_b = _create_backend_by_url(backend_url, config_b)
        if hasattr(backend, "client") and hasattr(backend_b, "_client"):
            backend_b._client = backend.client
        if hasattr(backend, "_executor") and hasattr(backend_b, "_executor"):
            backend_b._executor = backend._executor

    claimed_b = await backend_b.dequeue(["io"], batch_size=1)
    assert len(claimed_b) == 1
    assert claimed_b[0].task_id == "crash-task"
    assert claimed_b[0].attempt == 2
    await backend_b.ack(claimed_b[0])

    await backend.close()
    if backend_b is not backend:
        await backend_b.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "backend_url",
    [
        "memory://",
        "redis://localhost:6379/0",
        "valkey://localhost:6379/0",
        "postgresql://localhost:5432/vtb",
        "oracle://localhost:1521/FREEPDB1",
    ],
)
async def test_queue_backends_delayed_schedule_promotion(backend_url: str) -> None:
    """Verify delayed jobs are not visible until scheduled_at and then promoted."""
    config = QueueConfig(backend_url=backend_url)
    backend = _create_backend_by_url(backend_url, config)

    t0 = time.time()
    env = TaskEnvelope(
        task_id="delayed-1",
        task_name="scheduled_job",
        lane="io",
        scheduled_at=t0 + 10.0,
    )
    await backend.enqueue(env)

    immediate = await backend.dequeue(["io"], batch_size=1)
    assert len(immediate) == 0

    promoted_count = await backend.promote_scheduled(now=t0 + 11.0)
    if "redis" in backend_url or "valkey" in backend_url or "memory" in backend_url:
        assert promoted_count >= 1

    promoted = await backend.dequeue(["io"], batch_size=1)
    assert len(promoted) == 1
    assert promoted[0].task_id == "delayed-1"
    await backend.ack(promoted[0])
    await backend.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "backend_url",
    [
        "memory://",
        "redis://localhost:6379/0",
        "valkey://localhost:6379/0",
        "postgresql://localhost:5432/vtb",
        "oracle://localhost:1521/FREEPDB1",
    ],
)
async def test_queue_backends_retry_backoff_and_dlq_replay(backend_url: str) -> None:
    """Verify failing tasks retry with backoff, move to DLQ when exhausted, and can be replayed."""
    config = QueueConfig(backend_url=backend_url)
    backend = _create_backend_by_url(backend_url, config)

    qsvc = QueueService(config, backend=backend)
    attempts_recorded: list[int] = []

    @task(lane="io", retry_policy=TaskRetryPolicy(max_retries=2, initial_backoff_seconds=0.01))
    async def failing_operation(x: int) -> int:
        attempts_recorded.append(len(attempts_recorded) + 1)
        if len(attempts_recorded) <= 2:
            msg = "transient failure"
            raise RuntimeError(msg)
        return x * 2

    qsvc.register(failing_operation)
    await qsvc.enqueue("failing_operation", 42)

    claimed1 = (await backend.dequeue(["io"], batch_size=1))[0]
    assert claimed1.attempt == 1

    with pytest.raises(RuntimeError, match="transient failure"):
        await qsvc.process_envelope(claimed1)

    await asyncio.sleep(0.03)

    claimed2 = (await backend.dequeue(["io"], batch_size=1))[0]
    assert claimed2.attempt == 2

    with pytest.raises(RuntimeError, match="transient failure"):
        await qsvc.process_envelope(claimed2)

    dlq_entries = await backend.list_dlq()
    assert len(dlq_entries) == 1
    assert dlq_entries[0].envelope.task_name == "failing_operation"
    assert dlq_entries[0].error_type == "RuntimeError"

    replayed = await backend.replay_dlq(dlq_entries[0].dlq_id)
    assert replayed is not None
    assert replayed.attempt == 1

    res = await qsvc.process_envelope(replayed)
    assert res == 84
    await backend.close()


@pytest.mark.anyio
async def test_postgres_listen_notify_zero_poll_wakeup_uses_direct_dsn() -> None:
    """Verify PostgreSQLQueueBackend binds to direct_dsn and wakes on notification in < 50ms."""
    pg_config = PostgreSQLConfig(
        dsn="postgresql://pooler:6432/db",
        direct_dsn="postgresql://primary:5432/db",
        pooler_mode="transaction",
    )
    backend = PostgreSQLQueueBackend(postgres_config=pg_config, executor=_FakePostgreSQLQueueExecutor())
    assert backend.direct_dsn == "postgresql://primary:5432/db"

    dequeued_result: list[TaskEnvelope] = []

    async def waiter() -> None:
        claimed = await backend.dequeue(["io"], batch_size=1, timeout_seconds=2.0)
        dequeued_result.extend(claimed)

    task_coro = asyncio.create_task(waiter())
    await asyncio.sleep(0.01)

    t_before = time.time()
    env = TaskEnvelope(task_id="fast-wakeup", task_name="ping", lane="io")
    await backend.enqueue(env)

    await task_coro
    elapsed_ms = (time.time() - t_before) * 1000.0

    assert len(dequeued_result) == 1
    assert dequeued_result[0].task_id == "fast-wakeup"
    assert elapsed_ms < 50.0
    await backend.close()


@pytest.mark.anyio
async def test_distributed_cron_scheduler_leader_election_and_failover() -> None:
    """Verify exactly one node enqueues cron tasks and failover is seamless without duplicates."""
    backend = MemoryQueueBackend()
    qsvc = QueueService(backend=backend)

    @task(lane="io", cron=CronSchedule(expression="* * * * *", timezone="America/New_York"))
    def minutely_heartbeat() -> str:
        return "heartbeat"

    qsvc.register(minutely_heartbeat)

    sched1 = CronScheduler(qsvc, node_id="node-1", lease_ttl_seconds=0.1)
    sched2 = CronScheduler(qsvc, node_id="node-2", lease_ttl_seconds=0.1)
    sched3 = CronScheduler(qsvc, node_id="node-3", lease_ttl_seconds=0.1)

    t0 = datetime(2026, 10, 22, 12, 0, 0, tzinfo=UTC)
    res1 = await sched1.tick(now=t0)
    res2 = await sched2.tick(now=t0)
    res3 = await sched3.tick(now=t0)

    total_t0 = len(res1) + len(res2) + len(res3)
    assert total_t0 == 1
    assert len(res1) == 1
    assert len(res2) == 0
    assert len(res3) == 0

    await asyncio.sleep(0.12)

    t1 = datetime(2026, 10, 22, 12, 1, 0, tzinfo=UTC)
    res_failover_2 = await sched2.tick(now=t1)
    res_failover_3 = await sched3.tick(now=t1)

    total_t1 = len(res_failover_2) + len(res_failover_3)
    assert total_t1 == 1
    assert len(res_failover_2) == 1
    assert len(res_failover_3) == 0

    claimed = await backend.dequeue(["io"], batch_size=10)
    assert len(claimed) == 2
    assert claimed[0].idempotency_key == "cron:minutely_heartbeat:2026-10-22T12:00"
    assert claimed[1].idempotency_key == "cron:minutely_heartbeat:2026-10-22T12:01"
    await backend.close()


@pytest.mark.anyio
async def test_multi_lane_worker_runner_lame_duck_drain_and_otel_spans() -> None:
    """Verify WorkerRunner drains in-flight jobs on lame-duck shutdown and exports spans."""
    backend = MemoryQueueBackend()
    qsvc = QueueService(backend=backend)
    lame_duck = LameDuckController()
    exporter = SpanExporter()

    completed_jobs: list[str] = []

    @task(lane="cpu")
    async def slow_cpu_job() -> str:
        await asyncio.sleep(0.04)
        completed_jobs.append("cpu_done")
        return "cpu_done"

    qsvc.register(slow_cpu_job)

    root_trace = TraceContext.generate(sampled=True)
    token = TraceContext.set_current(root_trace)
    try:
        env = await qsvc.enqueue("slow_cpu_job")
        assert env.task_name == "slow_cpu_job"
    finally:
        TraceContext.set_current(token.old_value if hasattr(token, "old_value") else None)

    runner = WorkerRunner(
        [LaneConfig(queue_name="cpu", concurrency=2, drain_timeout_s=2.0, lane_kind="cpu")],
        queue_service=qsvc,
        lame_duck=lame_duck,
        span_exporter=exporter,
    )

    runner_task = runner.submit("cpu", lambda: runner.process_once())
    await asyncio.sleep(0.01)

    lame_duck.enter_lame_duck()
    status_code, status_payload = lame_duck.readiness_status()
    assert status_code == 503
    assert status_payload["status"] == "lame_duck"

    await runner.stop()
    await runner_task

    assert completed_jobs == ["cpu_done"]

    spans = exporter.drain_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "messaging.process slow_cpu_job"
    assert span.trace_id == root_trace.trace_id
    assert span.attributes["messaging.worker.lane"] == "cpu"
    assert span.attributes["status"] == "ok"
    assert span.status_code == 200
    await backend.close()


@pytest.mark.anyio
async def test_oracle_queue_backend_requires_oracledb_and_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OracleQueueBackend raises MissingDependencyError when oracledb is absent and ImproperConfigurationError without executor."""
    monkeypatch.setitem(sys.modules, "oracledb", None)
    reset_dependency_cache("oracledb")
    try:
        with pytest.raises(MissingDependencyError) as exc_info:
            OracleQueueBackend(QueueConfig(backend_url="oracle://localhost:1521/FREEPDB1"))
        assert exc_info.value.package == "oracledb"
        assert exc_info.value.install_package == "oracledb"

        with pytest.raises(MissingDependencyError) as svc_exc:
            QueueService(QueueConfig(backend_url="oracle://localhost:1521/FREEPDB1"))
        assert svc_exc.value.package == "oracledb"

        fake = _FakeOracleQueueExecutor()
        assert isinstance(fake, OracleQueueExecutor)
        cfg = QueueConfig(backend_url="oracle://localhost:1521/FREEPDB1")
        backend = OracleQueueBackend(cfg, executor=fake)
        service = QueueService(cfg, backend=backend)

        @task(name="oracle_ping", lane="io")
        def oracle_ping(val: int) -> int:
            return val + 1

        service.register(oracle_ping)
        await service.enqueue("oracle_ping", 41)
        assert await service.drain_once(("io",)) == [42]
        await service.close()

        monkeypatch.setitem(sys.modules, "oracledb", types.ModuleType("oracledb"))
        reset_dependency_cache("oracledb")
        with pytest.raises(ImproperConfigurationError, match="requires an OracleQueueExecutor"):
            OracleQueueBackend(QueueConfig())
    finally:
        reset_dependency_cache("oracledb")


@pytest.mark.anyio
async def test_postgres_queue_backend_requires_psycopg_and_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PostgreSQLQueueBackend raises MissingDependencyError when psycopg is absent and ImproperConfigurationError without executor."""
    monkeypatch.setitem(sys.modules, "psycopg", None)
    reset_dependency_cache("psycopg")
    try:
        with pytest.raises(MissingDependencyError) as exc_info:
            PostgreSQLQueueBackend(QueueConfig(backend_url="postgresql://localhost:5432/vtb"))
        assert exc_info.value.package == "psycopg"
        assert exc_info.value.install_package == "psycopg[binary,pool]"
        assert exc_info.value.hint == "Install it with: uv add psycopg[binary,pool]"

        with pytest.raises(MissingDependencyError) as svc_exc:
            QueueService(QueueConfig(backend_url="postgres://localhost:5432/vtb"))
        assert svc_exc.value.package == "psycopg"

        fake = _FakePostgreSQLQueueExecutor()
        assert isinstance(fake, PostgreSQLQueueExecutor)
        pg_config = PostgreSQLConfig(
            dsn="postgresql://pooler:6432/db",
            direct_dsn="postgresql://primary:5432/db",
        )
        qcfg = QueueConfig(backend_url="postgresql://pooler:6432/db")
        backend = PostgreSQLQueueBackend(qcfg, postgres_config=pg_config, executor=fake)
        assert backend.direct_dsn == "postgresql://primary:5432/db"
        service = QueueService(qcfg, backend=backend)

        @task(name="pg_ping", lane="io")
        def pg_ping(val: int) -> int:
            return val * 2

        service.register(pg_ping)
        await service.enqueue("pg_ping", 21)
        assert await service.drain_once(("io",)) == [42]
        await service.close()

        monkeypatch.setitem(sys.modules, "psycopg", types.ModuleType("psycopg"))
        reset_dependency_cache("psycopg")
        with pytest.raises(ImproperConfigurationError, match="requires a PostgreSQLQueueExecutor"):
            PostgreSQLQueueBackend(QueueConfig(backend_url="postgresql://localhost:5432/vtb"))
    finally:
        reset_dependency_cache("psycopg")


def test_redis_queue_backend_requires_redis_and_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RedisQueueBackend and ValkeyQueueBackend require redis.asyncio and a hosted URL unless client is injected."""
    monkeypatch.setitem(sys.modules, "redis.asyncio", None)
    reset_dependency_cache("redis.asyncio")
    try:
        with pytest.raises(MissingDependencyError) as exc_info:
            RedisQueueBackend(QueueConfig(backend_url="redis://localhost:6379/0"))
        assert exc_info.value.package == "redis"
        assert exc_info.value.install_package == "redis"
        assert exc_info.value.hint == "Install it with: uv add redis"

        with pytest.raises(MissingDependencyError) as svc_exc:
            QueueService(QueueConfig(backend_url="valkey://localhost:6379/0"))
        assert svc_exc.value.package == "redis"

        fake = FakeRedisStreamClient()
        assert isinstance(fake, RedisStreamClient)
        injected = ValkeyQueueBackend(client=fake)
        assert injected.client is fake

        captured: list[str] = []
        live = FakeRedisStreamClient()

        def _from_url(url: str) -> FakeRedisStreamClient:
            captured.append(url)
            return live

        stub = types.ModuleType("redis.asyncio")
        monkeypatch.setattr(stub, "from_url", _from_url, raising=False)
        monkeypatch.setitem(sys.modules, "redis.asyncio", stub)
        reset_dependency_cache("redis.asyncio")

        backend = RedisQueueBackend(QueueConfig(backend_url="redis://cache.internal:6380/2"))
        assert backend.client is live
        assert captured == ["redis://cache.internal:6380/2"]

        with pytest.raises(ImproperConfigurationError, match="has no host"):
            RedisQueueBackend(QueueConfig(backend_url="redis:///0"))
        assert captured == ["redis://cache.internal:6380/2"]

        with pytest.raises(ImproperConfigurationError, match="has no host"):
            ValkeyQueueBackend()
    finally:
        reset_dependency_cache("redis.asyncio")
