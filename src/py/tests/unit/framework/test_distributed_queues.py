"""Unit and contract tests for distributed queue backends, cron scheduling, and worker lanes."""

from __future__ import annotations

import ast
import asyncio
import collections
import contextlib
import inspect
import sys
import time
import types
import uuid
from collections.abc import AsyncIterator, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import msgspec
import pytest

import vertebra.framework.channels as channels_mod
import vertebra.framework.queues.redis as redis_queue_mod
import vertebra.framework.queues.valkey as valkey_queue_mod
from vertebra.db.adapters.oracle.queue import AsyncOracleQueueStore, SyncOracleQueueStore
from vertebra.db.adapters.postgres import PostgreSQLConfig
from vertebra.db.adapters.postgres.queue import AsyncPostgresQueueStore, SyncPostgresQueueStore
from vertebra.db.driver import AsyncNotificationSource, AsyncQueueStore, NotificationEvent, QueueEntry
from vertebra.extensions.django.management.commands.vertebra_worker import (
    Command as DjangoVertebraWorkerCommand,
)
from vertebra.extensions.django.tasks import TaskBackend as DjangoTaskBackend
from vertebra.extensions.litestar.queues.backend import StoreQueueBackend, StoreQueueBackendConfig
from vertebra.extensions.litestar.queues.shm_backend import SHMQueueBackendConfig
from vertebra.framework.di import Container, Provide
from vertebra.framework.queues import (
    CronSchedule,
    MemoryQueueBackend,
    OracleQueueBackend,
    OracleQueueExecutor,
    PostgreSQLQueueBackend,
    PostgreSQLQueueExecutor,
    QueueBackendSettings,
    QueueConfig,
    QueueService,
    RedisQueueBackend,
    TaskEnvelope,
    TaskRetryPolicy,
    ValkeyQueueBackend,
    ValkeyStreamClient,
    task,
)
from vertebra.framework.queues.backends import (
    ValkeyQueueBackend as BackendsValkeyQueueBackend,
)
from vertebra.framework.queues.scheduler import CronScheduler
from vertebra.framework.queues.valkey import (
    ValkeyQueueBackend as DirectValkeyQueueBackend,
)
from vertebra.framework.queues.valkey import (
    ValkeyStreamClient as DirectValkeyStreamClient,
)
from vertebra.server.lanes import LaneConfig, WorkerRunner
from vertebra.server.platform import LameDuckController
from vertebra.telemetry.otel import SpanExporter, TraceContext
from vertebra.typing import (
    ImproperConfigurationError,
    MissingDependencyError,
    reset_dependency_cache,
)
from vertebra.utils.serializers import decode_json, encode_json_bytes


class _FakeOracleQueueExecutor:
    """In-memory transactional SQL simulation of Oracle FOR UPDATE SKIP LOCKED semantics."""

    __slots__ = ("_dlq", "_jobs", "_leases", "_lock", "commit_count")

    def __init__(self) -> None:
        self._jobs: dict[str, dict[str, Any]] = {}
        self._dlq: dict[str, dict[str, Any]] = {}
        self._leases: dict[str, dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self.commit_count = 0

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

            if "DELETE FROM VERTEBRA_QUEUE_DLQ" in q:
                dlq_id = params[0]
                if dlq_id in self._dlq:
                    del self._dlq[dlq_id]
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
                if "CONSUMER_NAME" in q:
                    status, attempt, locked, cname, handle, task_id = params
                    if task_id in self._jobs:
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
                    return 0
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

            if "MERGE INTO VERTEBRA_QUEUE_LEASES" in q:
                lease_key, owner_id, expires_at, last_slot, check_now = params
                existing = self._leases.get(lease_key)
                if existing is None or existing["expires_at"] <= check_now or existing["owner_id"] == owner_id:
                    new_slot = last_slot or (existing.get("last_slot") if existing else None)
                    self._leases[lease_key] = {
                        "lease_key": lease_key,
                        "owner_id": owner_id,
                        "expires_at": expires_at,
                        "last_slot": new_slot,
                    }
                    return 1
                return 0

            return 0

    async def fetch_all(self, query: str, *params: Any) -> list[dict[str, Any]]:
        """Fetch all matching rows."""
        async with self._lock:
            q = query.strip().upper()
            if "FOR UPDATE SKIP LOCKED" in q:
                *lane_binds, now_ts, limit = params
                assert all(isinstance(item, str) for item in lane_binds)
                target_lanes = set(lane_binds)
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
            assert "MERGE INTO" not in q
            if "FROM VERTEBRA_QUEUE_LEASES WHERE LEASE_KEY" in q:
                lease_key = params[0]
                if lease_key in self._leases:
                    return dict(self._leases[lease_key])
                return None

            if "SELECT * FROM VERTEBRA_QUEUE_DLQ WHERE DLQ_ID" in q:
                dlq_id = params[0]
                if dlq_id in self._dlq:
                    return dict(self._dlq[dlq_id])
                return None

            return None

    async def commit(self) -> None:
        """Commit active mock transaction."""
        self.commit_count += 1

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
                decoded = decode_json(raw_bytes)
                if isinstance(decoded, dict):
                    lane = decoded.get("lane")
                    task_name = str(decoded.get("task_name") or "")
                elif isinstance(decoded, list) and len(decoded) >= 3:
                    lane = decoded[2]
                    task_name = str(decoded[1] or "")
                else:
                    lane = None
                    task_name = ""
                if not lane:
                    continue
                stream_key = f"{prefix}:lane:{lane}"
                await self.xadd(
                    stream_key,
                    {
                        "payload": raw_json,
                        "lane": lane,
                        "task_name": task_name,
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

    async def failing_operation(x: int) -> int:
        attempts_recorded.append(len(attempts_recorded) + 1)
        if len(attempts_recorded) <= 2:
            msg = "transient failure"
            raise RuntimeError(msg)
        return x * 2

    qsvc.register(
        task(lane="io", retry_policy=TaskRetryPolicy(max_retries=2, initial_backoff_seconds=0.01))(failing_operation)
    )
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

    def minutely_heartbeat() -> str:
        return "heartbeat"

    qsvc.register(
        task(lane="io", cron=CronSchedule(expression="* * * * *", timezone="America/New_York"))(minutely_heartbeat)
    )

    sched1 = CronScheduler(qsvc, node_id="node-1", lease_ttl_seconds=0.1)
    sched2 = CronScheduler(qsvc, node_id="node-2", lease_ttl_seconds=0.1)
    sched3 = CronScheduler(qsvc, node_id="node-3", lease_ttl_seconds=0.1)

    t0 = datetime(2026, 10, 22, 12, 0, 0, tzinfo=UTC)
    res1 = await sched1.tick(now=t0)
    res2 = await sched2.tick(now=t0)
    res3 = await sched3.tick(now=t0)

    total_t0 = res1 + res2 + res3
    assert total_t0 == 1
    assert res1 == 1
    assert res2 == 0
    assert res3 == 0

    await asyncio.sleep(0.12)

    t1 = datetime(2026, 10, 22, 12, 1, 0, tzinfo=UTC)
    res_failover_2 = await sched2.tick(now=t1)
    res_failover_3 = await sched3.tick(now=t1)

    total_t1 = res_failover_2 + res_failover_3
    assert total_t1 == 1
    assert res_failover_2 == 1
    assert res_failover_3 == 0

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

    async def slow_cpu_job() -> str:
        await asyncio.sleep(0.04)
        completed_jobs.append("cpu_done")
        return "cpu_done"

    qsvc.register(task(lane="cpu")(slow_cpu_job))

    root_trace = TraceContext.generate(sampled=True)
    token = TraceContext.set_current(root_trace)
    try:
        env = await qsvc.enqueue("slow_cpu_job")
        assert env.task_name == "slow_cpu_job"
    finally:
        old_ctx = getattr(token, "old_value", None)
        TraceContext.set_current(old_ctx if isinstance(old_ctx, TraceContext) else None)

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
    """OracleQueueBackend raises MissingDependencyError when vertebra._oracle is absent and ImproperConfigurationError without executor."""
    monkeypatch.setitem(sys.modules, "vertebra._oracle", None)
    reset_dependency_cache("vertebra._oracle")
    try:
        with pytest.raises(MissingDependencyError) as exc_info:
            OracleQueueBackend(QueueConfig(backend_url="oracle://localhost:1521/FREEPDB1"))
        assert exc_info.value.package == "vertebra-core[oracle]"
        assert exc_info.value.install_package == "vertebra-core[oracle]"

        with pytest.raises(MissingDependencyError) as svc_exc:
            QueueService(QueueConfig(backend_url="oracle://localhost:1521/FREEPDB1"))
        assert svc_exc.value.package == "vertebra-core[oracle]"

        fake = _FakeOracleQueueExecutor()
        assert isinstance(fake, OracleQueueExecutor)
        cfg = QueueConfig(backend_url="oracle://localhost:1521/FREEPDB1")
        backend = OracleQueueBackend(cfg, executor=fake)
        service = QueueService(cfg, backend=backend)

        def oracle_ping(val: int) -> int:
            return val + 1

        service.register(task(name="oracle_ping", lane="io")(oracle_ping))
        await service.enqueue("oracle_ping", 41)
        assert await service.drain_once(("io",)) == [42]
        await service.close()

        monkeypatch.setitem(sys.modules, "vertebra._oracle", types.ModuleType("vertebra._oracle"))
        reset_dependency_cache("vertebra._oracle")
        with pytest.raises(ImproperConfigurationError, match="is unsupported"):
            OracleQueueBackend(QueueConfig())
    finally:
        reset_dependency_cache("vertebra._oracle")


@pytest.mark.anyio
async def test_postgres_queue_backend_requires_native_postgres_and_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PostgreSQLQueueBackend raises MissingDependencyError when vertebra._postgres is absent and ImproperConfigurationError without executor."""
    monkeypatch.setitem(sys.modules, "vertebra._postgres", None)
    reset_dependency_cache("vertebra._postgres")
    try:
        with pytest.raises(MissingDependencyError) as exc_info:
            PostgreSQLQueueBackend(QueueConfig(backend_url="postgresql://localhost:5432/vtb"))
        assert exc_info.value.package == "vertebra-core[postgres]"
        assert "vertebra-core[postgres]" in exc_info.value.hint

        with pytest.raises(MissingDependencyError) as svc_exc:
            QueueService(QueueConfig(backend_url="postgres://localhost:5432/vtb"))
        assert svc_exc.value.package == "vertebra-core[postgres]"

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

        def pg_ping(val: int) -> int:
            return val * 2

        service.register(task(name="pg_ping", lane="io")(pg_ping))
        await service.enqueue("pg_ping", 21)
        assert await service.drain_once(("io",)) == [42]
        await service.close()

        monkeypatch.setitem(sys.modules, "vertebra._postgres", types.ModuleType("vertebra._postgres"))
        reset_dependency_cache("vertebra._postgres")
        with pytest.raises(ImproperConfigurationError, match="do not support memory:// DSNs"):
            PostgreSQLQueueBackend(QueueConfig())
    finally:
        reset_dependency_cache("vertebra._postgres")


def test_redis_queue_backend_requires_redis_and_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RedisQueueBackend requires vertebra._redis and a valid URL, while ValkeyQueueBackend requires vertebra._valkey."""
    monkeypatch.setitem(sys.modules, "vertebra._redis", None)
    reset_dependency_cache("vertebra._redis")
    try:
        with pytest.raises(MissingDependencyError) as exc_info:
            RedisQueueBackend(QueueConfig(backend_url="redis://localhost:6379/0"))
        assert exc_info.value.package == "vertebra-core[redis]"
        assert exc_info.value.install_package == "vertebra-core[redis]"
    finally:
        monkeypatch.delitem(sys.modules, "vertebra._redis", raising=False)
        reset_dependency_cache("vertebra._redis")

    monkeypatch.setitem(sys.modules, "vertebra._valkey", None)
    reset_dependency_cache("vertebra._valkey")
    try:
        with pytest.raises(MissingDependencyError) as svc_exc:
            QueueService(QueueConfig(backend_url="valkey://localhost:6379/0"))
        assert svc_exc.value.package == "vertebra-core[valkey]"

        fake = FakeRedisStreamClient()
        assert isinstance(fake, ValkeyStreamClient)
        injected = ValkeyQueueBackend(client=fake)
        assert injected.client is fake
    finally:
        monkeypatch.delitem(sys.modules, "vertebra._valkey", raising=False)
        reset_dependency_cache("vertebra._valkey")

    with pytest.raises(ImproperConfigurationError, match="has no host"):
        ValkeyQueueBackend(QueueConfig(backend_url="valkey:///0"))

    fake = FakeRedisStreamClient()
    injected_redis = RedisQueueBackend(client=fake)
    assert injected_redis.client is fake

    with pytest.raises(ImproperConfigurationError, match="has no host"):
        RedisQueueBackend(QueueConfig(backend_url="redis:///0"))


@pytest.mark.parametrize(
    ("url", "valid", "expected_err"),
    [
        ("redis://127.0.0.1:6379/0", True, None),
        ("rediss://127.0.0.1:6380/0", True, None),
        ("unix:///tmp/redis.sock", True, None),
        ("memory://", False, "does not support memory://"),
        ("valkey://localhost:6379/0", False, "valkey:// URLs are not supported by RedisQueueBackend"),
        ("valkeys://localhost:6380/0", False, "valkey:// URLs are not supported by RedisQueueBackend"),
    ],
)
def test_redis_queue_backend_url_schemes(url: str, valid: bool, expected_err: str | None) -> None:
    """RedisQueueBackend accepts redis://, rediss://, and unix:// while rejecting memory:// and valkey://."""
    if valid:
        backend = RedisQueueBackend(QueueConfig(backend_url=url), client=FakeRedisStreamClient())
        assert backend is not None
    else:
        with pytest.raises(ImproperConfigurationError, match=expected_err or ""):
            RedisQueueBackend(QueueConfig(backend_url=url))


@pytest.mark.parametrize(
    ("url", "valid", "expected_err"),
    [
        ("valkey://127.0.0.1:6379/0", True, None),
        ("valkeys://127.0.0.1:6380/0", True, None),
        ("unix:///tmp/valkey.sock", True, None),
        ("memory://", False, "does not support memory://"),
        ("redis://localhost:6379/0", False, "redis:// URLs are not supported by ValkeyQueueBackend"),
        ("rediss://localhost:6380/0", False, "redis:// URLs are not supported by ValkeyQueueBackend"),
    ],
)
def test_valkey_queue_backend_url_schemes(url: str, valid: bool, expected_err: str | None) -> None:
    """ValkeyQueueBackend accepts valkey://, valkeys://, and unix:// while rejecting memory:// and redis://."""
    if valid:
        backend = ValkeyQueueBackend(QueueConfig(backend_url=url), client=FakeRedisStreamClient())
        assert backend is not None
    else:
        with pytest.raises(ImproperConfigurationError, match=expected_err or ""):
            ValkeyQueueBackend(QueueConfig(backend_url=url))


def test_valkey_queue_backend_isolation(monkeypatch: pytest.MonkeyPatch) -> None:
    """ValkeyQueueBackend and ValkeyStreamClient are isolated from framework.queues.redis."""
    assert ValkeyQueueBackend is DirectValkeyQueueBackend
    assert BackendsValkeyQueueBackend is DirectValkeyQueueBackend
    assert ValkeyStreamClient is DirectValkeyStreamClient

    source = inspect.getsource(valkey_queue_mod)
    assert "vertebra.framework.queues.redis" not in source
    assert "from .redis" not in source
    assert "redis.asyncio" not in source

    fake = FakeRedisStreamClient()
    assert isinstance(fake, DirectValkeyStreamClient)

    monkeypatch.setattr(DirectValkeyQueueBackend, "_build_client", staticmethod(lambda cfg: fake))
    backend = QueueService._build_backend(QueueConfig(backend_url="valkey://localhost:6379/0", stream_prefix="test"))
    assert isinstance(backend, DirectValkeyQueueBackend)
    assert backend.client is fake


@pytest.mark.anyio
async def test_worker_runner_scoped_di_and_cron_scheduler_register() -> None:
    """Verify WorkerRunner per-task Container.scope() DI, generator teardown, and CronScheduler.register."""
    cleaned_up: list[str] = []

    async def get_db_session() -> AsyncIterator[dict[str, str]]:
        session = {"id": "session-1"}
        try:
            yield session
        finally:
            cleaned_up.append("session-closed")

    container = Container()
    container.graph.register("db_session", Provide(dependency=get_db_session, scope="request"))
    container.graph.compile()

    backend = MemoryQueueBackend()
    qsvc = QueueService(backend=backend)

    invoked_with: list[dict[str, str]] = []

    async def task_with_di(db_session: dict[str, str]) -> str:
        invoked_with.append(db_session)
        return "done"

    qsvc.register(task(lane="io")(task_with_di))
    await qsvc.enqueue("task_with_di")

    runner = WorkerRunner(
        [LaneConfig(queue_name="io", concurrency=2, lane_kind="io")],
        queue_service=qsvc,
        container=container,
    )

    processed = await runner.process_once()
    assert processed == 1
    assert len(invoked_with) == 1
    assert invoked_with[0] == {"id": "session-1"}
    assert cleaned_up == ["session-closed"]

    sched = CronScheduler(qsvc, node_id="sched-test", lease_ttl_seconds=10.0)
    sched.register("daily_sync", "0 0 * * *", "task_with_di")

    t_midnight = datetime(2026, 5, 1, 0, 0, 0, tzinfo=UTC)
    count = await sched.tick(now=t_midnight)
    assert count == 1

    t_other = datetime(2026, 5, 1, 0, 1, 0, tzinfo=UTC)
    count_other = await sched.tick(now=t_other)
    assert count_other == 0

    await backend.close()


class _StatefulAsyncQueueStore(AsyncQueueStore):
    """In-memory AsyncQueueStore implementing fenced attempts, reap_stale, claim_job, and pop_dead_letter."""

    def __init__(self) -> None:
        self._rows: dict[str, tuple[QueueEntry, str, str | None]] = {}
        self._unique_keys: set[str] = set()
        self._leases: dict[str, tuple[str, float]] = {}
        self.reap_calls = 0

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
        stored = msgspec.structs.replace(entry, job_id=jid)
        self._rows[jid] = (stored, "ready", None)
        return jid

    async def enqueue_many(self, entries: Sequence[QueueEntry]) -> list[str]:
        return [await self.enqueue(e) for e in entries]

    async def claim(
        self,
        queues: Sequence[str],
        *,
        limit: int = 1,
        worker_id: str,
        lease_seconds: float = 30.0,
    ) -> list[QueueEntry]:
        now_ms = int(time.time() * 1000)
        lease_ms = now_ms + int(lease_seconds * 1000)
        claimed: list[QueueEntry] = []
        for jid, (entry, status, _owner) in list(self._rows.items()):
            if len(claimed) >= limit:
                break
            if entry.queue not in queues:
                continue
            due_ready = status == "ready" and (entry.scheduled_at_ms is None or entry.scheduled_at_ms <= now_ms)
            expired_claim = (
                status == "claimed" and entry.lease_until_ms <= now_ms and entry.attempt < entry.max_attempts
            )
            if due_ready or expired_claim:
                next_attempt = entry.attempt + 1 if status == "claimed" else entry.attempt
                updated = msgspec.structs.replace(entry, attempt=next_attempt, lease_until_ms=lease_ms)
                self._rows[jid] = (updated, "claimed", worker_id)
                claimed.append(updated)
        return claimed

    async def claim_job(
        self,
        job_id: str,
        *,
        worker_id: str,
        lease_seconds: float = 30.0,
        expected_attempt: int | None = None,
    ) -> QueueEntry | None:
        item = self._rows.get(job_id)
        if item is None:
            return None
        entry, status, _owner = item
        now_ms = int(time.time() * 1000)
        due_ready = status == "ready" and (entry.scheduled_at_ms is None or entry.scheduled_at_ms <= now_ms)
        expired_claim = status == "claimed" and entry.lease_until_ms <= now_ms and entry.attempt < entry.max_attempts
        if not (due_ready or expired_claim):
            return None
        next_attempt = entry.attempt + 1 if status == "claimed" else entry.attempt
        if expected_attempt is not None and next_attempt != expected_attempt:
            return None
        updated = msgspec.structs.replace(
            entry,
            attempt=next_attempt,
            lease_until_ms=now_ms + int(lease_seconds * 1000),
        )
        self._rows[job_id] = (updated, "claimed", worker_id)
        return updated

    async def ack(self, job_id: str, *, attempt: int) -> bool:
        item = self._rows.get(job_id)
        if item is None:
            return False
        entry, status, _owner = item
        if status != "claimed" or entry.attempt != attempt:
            return False
        updated = msgspec.structs.replace(entry, lease_until_ms=0)
        self._rows[job_id] = (updated, "completed", None)
        return True

    async def nack(
        self,
        job_id: str,
        *,
        attempt: int,
        error: str,
        retry_delay_s: float | None = None,
    ) -> bool:
        _ = error
        item = self._rows.get(job_id)
        if item is None:
            return False
        entry, status, _owner = item
        if status != "claimed" or entry.attempt != attempt:
            return False
        if retry_delay_s is None or entry.attempt >= entry.max_attempts:
            updated = msgspec.structs.replace(entry, lease_until_ms=0)
            self._rows[job_id] = (updated, "dead_letter", None)
            return True
        now_ms = int(time.time() * 1000)
        sched_ms = now_ms + int(retry_delay_s * 1000) if retry_delay_s > 0 else None
        updated = msgspec.structs.replace(
            entry,
            attempt=entry.attempt + 1,
            lease_until_ms=0,
            scheduled_at_ms=sched_ms,
        )
        self._rows[job_id] = (updated, "ready", None)
        return True

    async def heartbeat(
        self,
        job_ids: Sequence[str],
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> int:
        now_ms = int(time.time() * 1000)
        new_lease = now_ms + int(lease_seconds * 1000)
        touched = 0
        for jid in job_ids:
            item = self._rows.get(jid)
            if item is not None and item[1] == "claimed" and item[2] == worker_id:
                self._rows[jid] = (msgspec.structs.replace(item[0], lease_until_ms=new_lease), "claimed", worker_id)
                touched += 1
        return touched

    async def reap_stale(self, *, lease_seconds: float = 300.0) -> int:
        _ = lease_seconds
        self.reap_calls += 1
        now_ms = int(time.time() * 1000)
        reaped = 0
        for jid, (entry, status, _owner) in list(self._rows.items()):
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
        now_ms = int(time.time() * 1000)
        promoted = 0
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
        self._rows[job_id] = (msgspec.structs.replace(item[0], lease_until_ms=0), "cancelled", None)
        return True

    async def get_entry(self, job_id: str) -> QueueEntry | None:
        item = self._rows.get(job_id)
        return item[0] if item is not None else None

    async def get_entry_status(self, job_id: str) -> str | None:
        item = self._rows.get(job_id)
        return item[1] if item is not None else None

    async def pop_dead_letter(self, job_id: str) -> QueueEntry | None:
        item = self._rows.get(job_id)
        if item is None or item[1] != "dead_letter":
            return None
        del self._rows[job_id]
        return item[0]

    async def list_entries(
        self,
        queue: str,
        *,
        status: str | None = None,
        limit: int = 100,
    ) -> list[QueueEntry]:
        matched = [
            entry
            for entry, row_status, _owner in self._rows.values()
            if entry.queue == queue and (status is None or row_status == status)
        ]
        return matched[:limit]

    async def next_due_delay(self, queues: Sequence[str]) -> float | None:
        now_ms = int(time.time() * 1000)
        scheds = [
            entry.scheduled_at_ms
            for entry, status, _owner in self._rows.values()
            if status == "ready"
            and entry.queue in queues
            and entry.scheduled_at_ms is not None
            and entry.scheduled_at_ms > now_ms
        ]
        if not scheds:
            return None
        return max(0.0, (min(scheds) - now_ms) / 1000.0)

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
        now = time.time()
        existing = self._leases.get(lease_key)
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


class _InMemoryAsyncNotificationSource(AsyncNotificationSource):
    """In-memory AsyncNotificationSource for wakeup and channel tests."""

    def __init__(self) -> None:
        self.subscribed: set[str] = set()
        self._queues: set[asyncio.Queue[NotificationEvent]] = set()

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
        try:
            while True:
                evt = await q.get()
                if not self.subscribed or evt.channel in self.subscribed:
                    yield evt
        finally:
            self._queues.discard(q)


@pytest.mark.anyio
async def test_redis_and_valkey_lua_promote_array_like_envelope_and_nack() -> None:
    """Redis and Valkey promotion Lua scripts extract array_like TaskEnvelope fields and nack has no dead tail."""
    env = TaskEnvelope(
        task_id="arr-1",
        task_name="dispatch_webhook",
        lane="agent",
        scheduled_at=10.0,
        max_retries=3,
    )
    raw = encode_json_bytes(env)
    decoded = decode_json(raw)
    assert isinstance(decoded, list)
    assert decoded[1] == "dispatch_webhook"
    assert decoded[2] == "agent"

    for lua_src in (redis_queue_mod._PROMOTE_SCHEDULED_LUA, valkey_queue_mod._PROMOTE_SCHEDULED_LUA):
        assert "data.lane or data[3]" in lua_src
        assert "data.task_name or data[2]" in lua_src
        assert "if ok and data and lane then" in lua_src

    nack_src = inspect.getsource(RedisQueueBackend.nack)
    tree = ast.parse(inspect.cleandoc(nack_src))
    func_node = tree.body[0]
    assert isinstance(func_node, ast.AsyncFunctionDef)
    assert isinstance(func_node.body[-1], ast.Return)
    assert sum(1 for stmt in func_node.body if isinstance(stmt, ast.Return)) == 1

    client = FakeRedisStreamClient()
    backend = RedisQueueBackend(QueueConfig(backend_url="redis://127.0.0.1:6379/0"), client=client)
    await backend.enqueue(msgspec.structs.replace(env, scheduled_at=None))
    claimed = await backend.dequeue(["agent"], batch_size=1)
    assert len(claimed) == 1
    dlq_res = await backend.nack(claimed[0], error="retry me", delay_seconds=5.0)
    assert dlq_res is None
    assert len(client._zsets["vtb:queue:scheduled"]) == 1
    promoted = await backend.promote_scheduled(now=time.time() + 10.0)
    assert promoted == 1
    reclaimed = await backend.dequeue(["agent"], batch_size=1)
    assert len(reclaimed) == 1
    assert reclaimed[0].attempt == 2
    await backend.close()


@pytest.mark.anyio
async def test_oracle_executor_multilane_binds_update_commit_and_merge_lease() -> None:
    """OracleQueueBackend executor mode expands multi-lane binds, persists running state, and avoids fetch_one on MERGE."""
    fake = _FakeOracleQueueExecutor()
    backend = OracleQueueBackend(
        QueueConfig(backend_url="oracle://localhost:1521/FREEPDB1", consumer_name="ora-worker-1"),
        executor=fake,
    )
    await backend.enqueue(TaskEnvelope(task_id="ora-cpu", task_name="c", lane="cpu"))
    await backend.enqueue(TaskEnvelope(task_id="ora-agent", task_name="a", lane="agent"))

    commits_before = fake.commit_count
    claimed = await backend.dequeue(["cpu", "agent"], batch_size=5)
    assert [c.task_id for c in claimed] == ["ora-cpu", "ora-agent"]
    assert fake._jobs["ora-cpu"]["status"] == "running"
    assert fake._jobs["ora-cpu"]["consumer_name"] == "ora-worker-1"
    assert fake._jobs["ora-cpu"]["receipt_handle"] is not None
    assert fake.commit_count > commits_before

    lease1 = await backend.acquire_lease("cron:hourly", "leader-a", 30.0, slot="2026-11-16T08:00")
    assert lease1 is not None
    assert lease1.owner_id == "leader-a"
    assert lease1.last_slot == "2026-11-16T08:00"

    competing = await backend.acquire_lease("cron:hourly", "leader-b", 30.0, slot="2026-11-16T08:00")
    assert competing is None

    renewed = await backend.renew_lease("cron:hourly", "leader-a", 30.0, slot="2026-11-16T09:00")
    assert renewed is not None
    assert renewed.last_slot == "2026-11-16T09:00"
    await backend.close()


@pytest.mark.anyio
async def test_oracle_and_postgres_queue_stores_reap_fencing_settings_and_dlq_replay() -> None:
    """Oracle and Postgres queue stores validate table_name, fence zombie workers in reap_stale, and replay DLQ cleanly."""
    dummy_drv: Any = object()
    with pytest.raises(ValueError, match="table_name"):
        SyncOracleQueueStore(dummy_drv, table_name="bad;table")
    with pytest.raises(ValueError, match="table_name"):
        AsyncOracleQueueStore(dummy_drv, table_name="bad;table")
    with pytest.raises(ValueError, match="table_name"):
        SyncPostgresQueueStore(dummy_drv, table_name="bad;table")
    with pytest.raises(ValueError, match="table_name"):
        AsyncPostgresQueueStore(dummy_drv, table_name="bad;table")

    executed_oracle_sql: list[str] = []

    class _RecordingOracleDriver:
        def execute(self, sql: str, _params: Any = None) -> int:
            executed_oracle_sql.append(sql)
            return 1

        @contextlib.contextmanager
        def transaction(self) -> Any:
            yield

        def select_one_or_none(self, sql: str, _params: Any = None) -> dict[str, Any] | None:
            executed_oracle_sql.append(sql)
            return {
                "job_id": "ora-dlq-1",
                "queue": "io",
                "task_name": "sync_task",
                "payload": b"{}",
                "attempt": 2,
                "max_attempts": 2,
                "lease_until_ms": 0,
                "scheduled_at_ms": None,
                "headers": None,
            }

    ora_drv_any: Any = _RecordingOracleDriver()
    ora_sync_sql_store = SyncOracleQueueStore(ora_drv_any, table_name="custom_ora_jobs")
    assert ora_sync_sql_store.reap_stale() == 1
    assert "attempt >= max_attempts THEN 'dead_letter'" in executed_oracle_sql[0]
    assert "ELSE attempt + 1" in executed_oracle_sql[0]
    popped_ora = ora_sync_sql_store.pop_dead_letter("ora-dlq-1")
    assert popped_ora is not None
    assert popped_ora.job_id == "ora-dlq-1"
    assert any("DELETE FROM custom_ora_jobs" in s for s in executed_oracle_sql)

    store = _StatefulAsyncQueueStore()
    await store.enqueue(
        QueueEntry(
            job_id="zombie-job",
            queue="io",
            task_name="work",
            payload=b"{}",
            attempt=1,
            max_attempts=2,
        )
    )
    first_claim = await store.claim(["io"], limit=1, worker_id="worker-zombie", lease_seconds=0.0)
    assert len(first_claim) == 1
    assert first_claim[0].attempt == 1

    assert await store.reap_stale() == 1
    assert await store.ack("zombie-job", attempt=1) is False
    assert await store.nack("zombie-job", attempt=1, error="late crash") is False

    second_claim = await store.claim(["io"], limit=1, worker_id="worker-healthy", lease_seconds=0.0)
    assert len(second_claim) == 1
    assert second_claim[0].attempt == 2

    assert await store.reap_stale() == 1
    assert await store.get_entry_status("zombie-job") == "dead_letter"
    assert await store.claim(["io"], limit=1, worker_id="worker-3", lease_seconds=30.0) == []

    pg_store = _StatefulAsyncQueueStore()
    pg_sync_marker: Any = object()
    pg_backend = PostgreSQLQueueBackend(
        QueueBackendSettings(kind="postgres", store=pg_store, sync_store=pg_sync_marker)
    )
    resolved_pg_store: Any = pg_backend.store
    assert resolved_pg_store is pg_store
    assert pg_backend.sync_store is pg_sync_marker

    ora_store = _StatefulAsyncQueueStore()
    ora_sync_marker: Any = object()
    ora_backend = OracleQueueBackend(QueueBackendSettings(kind="oracle", store=ora_store, sync_store=ora_sync_marker))
    resolved_ora_store: Any = ora_backend.store
    assert resolved_ora_store is ora_store
    assert ora_backend.sync_store is ora_sync_marker

    for native_backend, backing_store in ((pg_backend, pg_store), (ora_backend, ora_store)):
        await backing_store.reserve_unique_key("dedup-key-1", ttl_seconds=60.0)
        await backing_store.release_unique_key("dedup-key-1")
        env = TaskEnvelope(
            task_id="dlq-replay-task",
            task_name="sync_invoice",
            lane="io",
            max_retries=1,
            idempotency_key="dedup-key-1",
        )
        await native_backend.enqueue(env)
        claimed_batch = await native_backend.dequeue(["io"], batch_size=1)
        assert len(claimed_batch) == 1
        dlq_item = await native_backend.nack(claimed_batch[0], error="boom", send_to_dlq=True)
        assert dlq_item is not None
        dlq_listed = await native_backend.list_dlq()
        assert len(dlq_listed) == 1
        assert dlq_listed[0].dlq_id == "dlq-replay-task"

        replayed_env = await native_backend.replay_dlq(dlq_listed[0].dlq_id)
        assert replayed_env is not None
        assert replayed_env.attempt == 1
        reclaimed_after_replay = await native_backend.dequeue(["io"], batch_size=1)
        assert len(reclaimed_after_replay) == 1
        assert reclaimed_after_replay[0].task_id == "dlq-replay-task"
        await native_backend.ack(reclaimed_after_replay[0])


@pytest.mark.anyio
async def test_litestar_store_and_shm_queue_backend_configs_and_fenced_lifecycle() -> None:
    """StoreQueueBackendConfig and SHMQueueBackendConfig are frozen and StoreQueueBackend enforces retry fencing."""
    store = _StatefulAsyncQueueStore()
    notif = _InMemoryAsyncNotificationSource()
    store_cfg = StoreQueueBackendConfig(store=store, notification_source=notif)
    shm_cfg = SHMQueueBackendConfig()

    store_cfg_any: Any = store_cfg
    shm_cfg_any: Any = shm_cfg
    with pytest.raises(AttributeError):
        store_cfg_any.terminal_ttl = timedelta(seconds=10)
    with pytest.raises(AttributeError):
        shm_cfg_any.max_concurrency = 99

    backend: StoreQueueBackend = store_cfg.create_backend()
    rec1 = await backend.enqueue("compute_report", args=(7,), max_retries=2)
    claimed1 = await backend.claim_task(rec1.id)
    assert claimed1 is not None
    assert claimed1.status == "running"
    assert claimed1.retry_count == 0
    assert await backend.claim_task(rec1.id) is None

    failed_retry = await backend.fail_task(rec1.id, "transient", retry=True, expected_retry_count=0)
    assert failed_retry is not None
    assert failed_retry.retry_count == 1
    assert failed_retry.status == "pending"

    claimed2 = await backend.claim_next()
    assert claimed2 is not None
    assert claimed2.id == rec1.id
    assert claimed2.retry_count == 1
    assert claimed2.status == "running"

    running_ext = await backend.list_running_external()
    assert [r.id for r in running_ext] == [rec1.id]
    assert running_ext[0].status == "running"

    completed1 = await backend.complete_task(rec1.id, expected_retry_count=claimed2.retry_count)
    assert completed1 is not None
    assert completed1.status == "completed"

    rec_fatal = await backend.enqueue("fatal_job", max_retries=3)
    claimed_fatal = await backend.claim_task(rec_fatal.id)
    assert claimed_fatal is not None
    failed_fatal = await backend.fail_task(rec_fatal.id, "unrecoverable", retry=False)
    assert failed_fatal is not None
    assert failed_fatal.status == "failed"

    rec_pending = await backend.enqueue("pending_job")
    rec_sched = await backend.enqueue("sched_job", scheduled_at=datetime.now(UTC) + timedelta(minutes=10))
    rec_cancel = await backend.enqueue("cancel_job")
    assert await backend.cancel_task(rec_cancel.id) is True

    stats = await backend.get_statistics(queue="default")
    assert stats.pending == 1
    assert stats.scheduled == 1
    assert stats.running == 0
    assert stats.completed == 1
    assert stats.failed == 1
    assert stats.cancelled == 1
    _ = (rec_pending, rec_sched)

    assert await backend.wait_for_wakeups(timeout=0.02) is False

    async def _emit_wakeup() -> None:
        await asyncio.sleep(0.01)
        await notif.publish("vertebra_queue_wakeup", "default")

    emitter = asyncio.create_task(_emit_wakeup())
    assert await backend.wait_for_wakeups(timeout=1.0) is True
    await emitter
    await backend.close()


def test_create_channels_backend_postgres_and_oracle_wiring(monkeypatch: pytest.MonkeyPatch) -> None:
    """create_channels_backend passes dsn=url to PostgreSQLConfig and OracleConfig with canonical driver names."""
    recorded: list[tuple[str, Any, Any]] = []

    class _FakePgConfig:
        def __init__(self, *, dsn: str = "", **kwargs: Any) -> None:
            self.dsn = dsn
            self.kwargs = kwargs

    class _FakeOraConfig:
        def __init__(self, *, dsn: str = "", **kwargs: Any) -> None:
            self.dsn = dsn
            self.kwargs = kwargs

    class _FakePgMod:
        PostgreSQLConfig = _FakePgConfig
        AsyncPostgreSQLDriver = staticmethod(lambda cfg: ("pg_async_drv", cfg))
        SyncPostgreSQLDriver = staticmethod(lambda cfg: ("pg_sync_drv", cfg))
        AsyncPostgresNotificationSource = staticmethod(lambda drv: ("pg_async_src", drv))
        SyncPostgresNotificationSource = staticmethod(lambda drv: ("pg_sync_src", drv))

    class _FakeOraMod:
        OracleConfig = _FakeOraConfig
        AsyncOracleDriver = staticmethod(lambda cfg: ("ora_async_drv", cfg))
        SyncOracleDriver = staticmethod(lambda cfg: ("ora_sync_drv", cfg))
        AsyncOracleNotificationSource = staticmethod(lambda drv: ("ora_async_src", drv))
        SyncOracleNotificationSource = staticmethod(lambda drv: ("ora_sync_src", drv))

    def _fake_import_optional(name: str) -> Any:
        if name == "vertebra.db.adapters.postgres":
            return _FakePgMod
        if name == "vertebra.db.adapters.oracle":
            return _FakeOraMod
        return None

    monkeypatch.setattr(channels_mod, "import_optional", _fake_import_optional)

    pg_bus = channels_mod.create_channels_backend("postgresql", url="postgresql://localhost:5432/vtb")
    pg_async_src: Any = pg_bus.notification_source
    pg_sync_src: Any = pg_bus.sync_notification_source
    recorded.append(("pg", pg_async_src, pg_sync_src))
    assert pg_async_src[0] == "pg_async_src"
    assert pg_async_src[1][0] == "pg_async_drv"
    assert pg_async_src[1][1].dsn == "postgresql://localhost:5432/vtb"
    assert pg_sync_src[1][0] == "pg_sync_drv"

    ora_bus = channels_mod.create_channels_backend("oracle", url="oracle://localhost:1521/FREEPDB1")
    ora_async_src: Any = ora_bus.notification_source
    ora_sync_src: Any = ora_bus.sync_notification_source
    recorded.append(("ora", ora_async_src, ora_sync_src))
    assert ora_async_src[0] == "ora_async_src"
    assert ora_async_src[1][0] == "ora_async_drv"
    assert ora_async_src[1][1].dsn == "oracle://localhost:1521/FREEPDB1"
    assert ora_sync_src[1][0] == "ora_sync_drv"
    assert len(recorded) == 2


@pytest.mark.anyio
async def test_worker_runner_concurrent_batch_lane_alignment_and_periodic_reap() -> None:
    """WorkerRunner dispatches claimed tasks concurrently up to concurrency, aligns queue_name/lane metrics, and reaps periodically."""
    store = _StatefulAsyncQueueStore()
    pg_backend = PostgreSQLQueueBackend(QueueBackendSettings(kind="postgres", store=store))
    qsvc = QueueService(backend=pg_backend)

    active_count = 0
    all_started = asyncio.Event()
    release_gate = asyncio.Event()

    async def barrier_task(idx: int) -> int:
        nonlocal active_count
        active_count += 1
        if active_count == 3:
            all_started.set()
        await release_gate.wait()
        return idx

    qsvc.register(task(name="barrier_task", lane="io")(barrier_task))
    for i in range(3):
        await qsvc.enqueue("barrier_task", i)

    runner = WorkerRunner(
        [LaneConfig(queue_name="default", concurrency=3, drain_timeout_s=2.0, lane_kind="io")],
        queue_service=qsvc,
        reap_interval_seconds=60.0,
    )

    batch_task = asyncio.create_task(runner.process_once())
    await asyncio.wait_for(all_started.wait(), timeout=1.0)
    assert runner.metrics["default"].in_flight == 3
    assert len(runner._in_flight) == 3

    stop_task = asyncio.create_task(runner.stop())
    await asyncio.sleep(0.01)
    assert not stop_task.done()
    release_gate.set()

    await stop_task
    processed = await batch_task
    assert processed == 3
    assert runner.metrics["default"].in_flight == 0
    assert runner.metrics["default"].processed_total == 3
    assert store.reap_calls == 1
    await pg_backend.close()


@pytest.mark.anyio
async def test_django_task_backend_list_arg_queues_filter_and_bad_import_dlq() -> None:
    """Django TaskBackend preserves list/tuple user args and vertebra_worker honors --queues and DLQs unimportable tasks."""
    backend = DjangoTaskBackend(
        alias="vtb_django_test",
        params={"QUEUES": ["default", "cpu", "agent"], "OPTIONS": {"BACKEND_URL": "memory://"}},
    )

    @backend.task("join_items")
    def join_items(items: list[str] | tuple[str, ...]) -> str:
        return ",".join(items)

    res_list = backend.enqueue(join_items, ["alpha", "beta", "gamma"])
    assert res_list.args == [["alpha", "beta", "gamma"]]
    assert await backend.arun_next() == "alpha,beta,gamma"

    res_tuple = await backend.aenqueue(join_items, ("x", "y"))
    assert len(res_tuple.args) == 1
    assert list(res_tuple.args[0]) == ["x", "y"]
    assert await backend.arun_next() == "x,y"

    await backend.queue_service.backend.enqueue(
        TaskEnvelope(
            task_id="bad-import-1",
            task_name="nonexistent_django_module_xyz.missing_callable",
            lane="io",
            max_retries=3,
        )
    )
    cmd = DjangoVertebraWorkerCommand()
    await cmd._run_async(
        backend=backend,
        queues=["default"],
        explicit_queues=True,
        concurrency=1,
        once=True,
        burst=False,
        container=Container(),
    )
    dlq_entries = await backend.queue_service.backend.list_dlq()
    assert len(dlq_entries) == 1
    assert dlq_entries[0].envelope.task_id == "bad-import-1"

    executed_lanes: list[str] = []

    def cpu_only_task() -> str:
        executed_lanes.append("cpu")
        return "cpu"

    def agent_only_task() -> str:
        executed_lanes.append("agent")
        return "agent"

    backend.queue_service.register(task(name="cpu_only_task", lane="cpu")(cpu_only_task))
    backend.queue_service.register(task(name="agent_only_task", lane="agent")(agent_only_task))
    await backend.queue_service.enqueue("cpu_only_task", lane="cpu")
    await backend.queue_service.enqueue("agent_only_task", lane="agent")

    await cmd._run_async(
        backend=backend,
        queues=["cpu"],
        explicit_queues=True,
        concurrency=1,
        once=False,
        burst=True,
        container=Container(),
    )
    assert executed_lanes == ["cpu"]
    remaining_agent = await backend.queue_service.backend.dequeue(["agent"], batch_size=5)
    assert len(remaining_agent) == 1
    assert remaining_agent[0].task_name == "agent_only_task"
    await backend.queue_service.close()
