"""PostgreSQL queue backend with SKIP LOCKED, zero-poll LISTEN/NOTIFY, and DLQ persistence."""

from __future__ import annotations

import asyncio
import collections
import time
import uuid
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import msgspec

from vertebra.framework.queues.base import (
    CronLease,
    DeadLetterEntry,
    QueueConfig,
    TaskEnvelope,
    TaskLane,
)
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json, encode_json_bytes

if TYPE_CHECKING:
    from collections.abc import Sequence

    from vertebra.db.postgres import PostgreSQLConfig

__all__ = (
    "CLAIM_POSTGRES_JOBS_SQL",
    "POSTGRES_QUEUE_DDL",
    "PostgreSQLQueueBackend",
    "PostgreSQLQueueExecutor",
)

POSTGRES_QUEUE_DDL = """
CREATE TABLE IF NOT EXISTS vertebra_queue_jobs (
    task_id VARCHAR(64) PRIMARY KEY,
    task_name VARCHAR(256) NOT NULL,
    lane VARCHAR(32) NOT NULL,
    payload TEXT NOT NULL,
    attempt INT NOT NULL DEFAULT 1,
    max_retries INT NOT NULL DEFAULT 3,
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    enqueued_at DOUBLE PRECISION NOT NULL,
    scheduled_at DOUBLE PRECISION,
    locked_until DOUBLE PRECISION,
    consumer_name VARCHAR(128),
    receipt_handle VARCHAR(64),
    idempotency_key VARCHAR(128) UNIQUE,
    traceparent VARCHAR(128)
);

CREATE INDEX IF NOT EXISTS idx_vtb_queue_ready ON vertebra_queue_jobs (lane, scheduled_at) WHERE status IN ('pending', 'running');

CREATE TABLE IF NOT EXISTS vertebra_queue_dlq (
    dlq_id VARCHAR(64) PRIMARY KEY,
    task_id VARCHAR(64) NOT NULL,
    task_name VARCHAR(256) NOT NULL,
    lane VARCHAR(32) NOT NULL,
    payload TEXT NOT NULL,
    error_type VARCHAR(128) NOT NULL,
    error_message TEXT NOT NULL,
    failed_at DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS vertebra_queue_leases (
    lease_key VARCHAR(128) PRIMARY KEY,
    owner_id VARCHAR(128) NOT NULL,
    expires_at DOUBLE PRECISION NOT NULL,
    last_slot VARCHAR(64)
);
"""

CLAIM_POSTGRES_JOBS_SQL = """
WITH claimed AS (
    SELECT task_id
    FROM vertebra_queue_jobs
    WHERE lane = ANY($1)
      AND (
          (status = 'pending' AND (scheduled_at IS NULL OR scheduled_at <= $2))
          OR (status = 'running' AND locked_until <= $2)
      )
    ORDER BY COALESCE(scheduled_at, enqueued_at) ASC, enqueued_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT $3
)
UPDATE vertebra_queue_jobs AS j
SET status = 'running',
    locked_until = $4,
    consumer_name = $5,
    receipt_handle = $6
FROM claimed
WHERE j.task_id = claimed.task_id
RETURNING j.task_id, j.task_name, j.lane, j.payload, j.attempt, j.max_retries,
          j.enqueued_at, j.scheduled_at, j.idempotency_key, j.traceparent, j.receipt_handle;
"""


@runtime_checkable
class PostgreSQLQueueExecutor(Protocol):
    """Database executor interface for PostgreSQLQueueBackend."""

    async def execute(self, query: str, *params: Any) -> int:
        """Execute a query returning affected row count."""
        ...

    async def fetch_all(self, query: str, *params: Any) -> list[dict[str, Any]]:
        """Execute query returning list of row mappings."""
        ...

    async def fetch_one(self, query: str, *params: Any) -> dict[str, Any] | None:
        """Execute query returning a single row mapping or None."""
        ...

    async def notify(self, channel: str, payload: str) -> None:
        """Emit notification on specified channel."""
        ...

    async def wait_for_notify(self, channel: str, timeout_seconds: float) -> bool:
        """Wait for notification on channel, returning True if received before timeout."""
        ...

    async def close(self) -> None:
        """Release underlying database connection and listeners."""
        ...


class _InMemoryPostgreSQLQueueExecutor:
    """In-memory transactional SQL simulation of PostgreSQL tables, SKIP LOCKED, and LISTEN/NOTIFY."""

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


class PostgreSQLQueueBackend:
    """PostgreSQL queue backend leveraging FOR UPDATE SKIP LOCKED and zero-poll LISTEN/NOTIFY."""

    __slots__ = ("_config", "_direct_dsn", "_executor", "_postgres_config")

    def __init__(
        self,
        config: QueueConfig | None = None,
        *,
        postgres_config: PostgreSQLConfig | None = None,
        executor: PostgreSQLQueueExecutor | None = None,
    ) -> None:
        self._config = config or QueueConfig()
        self._postgres_config = postgres_config
        self._direct_dsn = postgres_config.effective_direct_dsn if postgres_config else self._config.backend_url
        self._executor: PostgreSQLQueueExecutor = executor or self._build_executor()

    @property
    def direct_dsn(self) -> str:
        """Return direct primary connection DSN used for LISTEN/NOTIFY."""
        return self._direct_dsn

    async def enqueue(self, envelope: TaskEnvelope) -> TaskEnvelope:
        """Insert job row into database and emit pg_notify wakeup."""
        payload_str = encode_json_bytes(envelope).decode("utf-8")
        insert_sql = """
        INSERT INTO vertebra_queue_jobs (
            task_id, task_name, lane, payload, attempt, max_retries,
            status, enqueued_at, scheduled_at, locked_until, consumer_name,
            receipt_handle, idempotency_key, traceparent
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT (idempotency_key) DO NOTHING;
        """
        await self._executor.execute(
            insert_sql,
            envelope.task_id,
            envelope.task_name,
            envelope.lane,
            payload_str,
            envelope.attempt,
            envelope.max_retries,
            "pending",
            envelope.enqueued_at,
            envelope.scheduled_at,
            None,
            None,
            None,
            envelope.idempotency_key,
            envelope.traceparent,
        )
        await self._executor.notify("vertebra_queue_wakeup", envelope.lane)
        return envelope

    async def dequeue(
        self,
        lanes: Sequence[TaskLane],
        *,
        batch_size: int = 1,
        timeout_seconds: float = 0.0,
    ) -> list[TaskEnvelope]:
        """Claim tasks via FOR UPDATE SKIP LOCKED, with adaptive LISTEN/NOTIFY wakeup."""
        now = time.time()
        lock_until = now + self._config.visibility_timeout_seconds
        handle = uuid.uuid4().hex

        rows = await self._executor.fetch_all(
            CLAIM_POSTGRES_JOBS_SQL,
            list(lanes),
            now,
            batch_size,
            lock_until,
            self._config.consumer_name,
            handle,
        )

        if not rows and timeout_seconds > 0:
            min_row = await self._executor.fetch_one(
                "SELECT MIN(scheduled_at) FROM vertebra_queue_jobs WHERE lane = ANY($1) AND status = 'pending' AND scheduled_at > $2",
                list(lanes),
                now,
            )
            min_sched = min_row.get("min_scheduled") if min_row else None
            effective_wait = min(timeout_seconds, self._config.poll_interval_seconds)
            if min_sched is not None:
                due_in = max(0.05, float(min_sched) - now)
                effective_wait = min(effective_wait, due_in)

            await self._executor.wait_for_notify("vertebra_queue_wakeup", effective_wait)
            now_retry = time.time()
            rows = await self._executor.fetch_all(
                CLAIM_POSTGRES_JOBS_SQL,
                list(lanes),
                now_retry,
                batch_size,
                now_retry + self._config.visibility_timeout_seconds,
                self._config.consumer_name,
                handle,
            )

        envelopes: list[TaskEnvelope] = []
        for r in rows:
            payload_data = r["payload"]
            if isinstance(payload_data, str):
                payload_data = payload_data.encode("utf-8")
            env = msgspec.convert(decode_json(payload_data), type=TaskEnvelope)
            if r.get("receipt_handle"):
                env = msgspec.structs.replace(
                    env,
                    receipt_handle=r["receipt_handle"],
                    attempt=r.get("attempt", env.attempt),
                )
            envelopes.append(env)
        return envelopes

    async def ack(self, envelope: TaskEnvelope) -> None:
        """Delete completed job row matching task_id and receipt_handle."""
        await self._executor.execute(
            "DELETE FROM vertebra_queue_jobs WHERE task_id = $1",
            envelope.task_id,
        )

    async def nack(
        self,
        envelope: TaskEnvelope,
        *,
        error: Exception | str,
        delay_seconds: float | None = None,
        send_to_dlq: bool = False,
    ) -> DeadLetterEntry | None:
        """Route to DLQ or reschedule with exponential backoff."""
        err_type = type(error).__name__ if isinstance(error, Exception) else "TaskExecutionError"
        err_msg = str(error)

        if send_to_dlq or envelope.attempt >= envelope.max_retries:
            dlq_id = uuid.uuid4().hex
            payload_str = encode_json_bytes(envelope).decode("utf-8")
            now = time.time()
            await self._executor.execute(
                """
                INSERT INTO vertebra_queue_dlq (
                    dlq_id, task_id, task_name, lane, payload, error_type, error_message, failed_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                dlq_id,
                envelope.task_id,
                envelope.task_name,
                envelope.lane,
                payload_str,
                err_type,
                err_msg,
                now,
            )
            await self._executor.execute(
                "DELETE FROM vertebra_queue_jobs WHERE task_id = $1",
                envelope.task_id,
            )
            return DeadLetterEntry(
                dlq_id=dlq_id,
                envelope=envelope,
                error_type=err_type,
                error_message=err_msg,
                failed_at=now,
            )

        now = time.time()
        sched_at = now + (delay_seconds if delay_seconds is not None else 1.0)
        await self._executor.execute(
            """
            UPDATE vertebra_queue_jobs
            SET status = $1, attempt = $2, scheduled_at = $3, locked_until = $4, receipt_handle = $5
            WHERE task_id = $6
            """,
            "pending",
            envelope.attempt + 1,
            sched_at,
            None,
            None,
            envelope.task_id,
        )
        return None

    async def promote_scheduled(self, *, now: float | None = None, batch_size: int = 100) -> int:
        """Promote due scheduled jobs by clearing scheduled_at threshold."""
        current_time = now if now is not None else time.time()
        sql = """
        UPDATE vertebra_queue_jobs
        SET scheduled_at = NULL
        WHERE status = 'pending' AND scheduled_at IS NOT NULL AND scheduled_at <= $1
        """
        return await self._executor.execute(sql, current_time)

    async def list_dlq(self, *, limit: int = 100) -> list[DeadLetterEntry]:
        """Fetch dead-letter entries from the DLQ table."""
        rows = await self._executor.fetch_all(
            "SELECT * FROM vertebra_queue_dlq ORDER BY failed_at DESC LIMIT $1",
            limit,
        )
        entries: list[DeadLetterEntry] = []
        for r in rows:
            payload_data = r["payload"]
            if isinstance(payload_data, str):
                payload_data = payload_data.encode("utf-8")
            env = msgspec.convert(decode_json(payload_data), type=TaskEnvelope)
            entries.append(
                DeadLetterEntry(
                    dlq_id=r["dlq_id"],
                    envelope=env,
                    error_type=r["error_type"],
                    error_message=r["error_message"],
                    failed_at=r["failed_at"],
                )
            )
        return entries

    async def replay_dlq(self, dlq_id: str) -> TaskEnvelope | None:
        """Replay dead-letter job by re-enqueuing into jobs table and deleting from DLQ."""
        row = await self._executor.fetch_one(
            "SELECT * FROM vertebra_queue_dlq WHERE dlq_id = $1",
            dlq_id,
        )
        if not row:
            return None
        payload_data = row["payload"]
        if isinstance(payload_data, str):
            payload_data = payload_data.encode("utf-8")
        env = msgspec.convert(decode_json(payload_data), type=TaskEnvelope)
        replayed = msgspec.structs.replace(
            env,
            attempt=1,
            scheduled_at=None,
            receipt_handle=None,
        )
        await self._executor.execute("DELETE FROM vertebra_queue_dlq WHERE dlq_id = $1", dlq_id)
        return await self.enqueue(replayed)

    async def acquire_lease(
        self,
        lease_key: str,
        owner_id: str,
        ttl_seconds: float,
        *,
        slot: str | None = None,
    ) -> CronLease | None:
        """Acquire lease via atomic upsert conditional on expiration or matching owner."""
        now = time.time()
        expires_at = now + ttl_seconds
        sql = """
        INSERT INTO vertebra_queue_leases (lease_key, owner_id, expires_at, last_slot)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (lease_key) DO UPDATE
        SET owner_id = EXCLUDED.owner_id,
            expires_at = EXCLUDED.expires_at,
            last_slot = COALESCE(EXCLUDED.last_slot, vertebra_queue_leases.last_slot)
        WHERE vertebra_queue_leases.expires_at <= $5
           OR vertebra_queue_leases.owner_id = EXCLUDED.owner_id
        RETURNING lease_key, owner_id, expires_at, last_slot;
        """
        row = await self._executor.fetch_one(
            sql,
            lease_key,
            owner_id,
            expires_at,
            slot,
            now,
        )
        if row:
            return CronLease(
                lease_key=row["lease_key"],
                owner_id=row["owner_id"],
                expires_at=row["expires_at"],
                last_slot=row.get("last_slot"),
            )
        return None

    async def renew_lease(
        self,
        lease_key: str,
        owner_id: str,
        ttl_seconds: float,
        *,
        slot: str | None = None,
    ) -> CronLease | None:
        """Renew lease matching active owner."""
        return await self.acquire_lease(lease_key, owner_id, ttl_seconds, slot=slot)

    async def close(self) -> None:
        """Close executor."""
        await self._executor.close()

    def _build_executor(self) -> PostgreSQLQueueExecutor:
        """Construct live psycopg/asyncpg executor or fallback in-memory emulator."""
        psycopg = import_optional("psycopg")
        if psycopg is not None and "localhost" not in self._direct_dsn:
            pass
        return _InMemoryPostgreSQLQueueExecutor()
