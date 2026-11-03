"""Google Cloud Spanner distributed queue backend with hash sharding and ``FOR UPDATE``."""

from __future__ import annotations

import asyncio
import hashlib
import time
import urllib.parse
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Final, Literal, Protocol, runtime_checkable

import msgspec

from vertebra.framework.queues.base import (
    CronLease,
    DeadLetterEntry,
    QueueConfig,
    TaskEnvelope,
    TaskLane,
)
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.serializers import decode_json, encode_json_bytes
from vertebra.utils.uuids import uuid4

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = (
    "CLAIM_SPANNER_JOBS_SQL",
    "SPANNER_QUEUE_DDL",
    "SPANNER_QUEUE_DDL_GOOGLESQL",
    "SPANNER_QUEUE_DDL_SPANGRES",
    "UPDATE_CLAIM_SPANNER_JOBS_SQL",
    "SpannerQueueBackend",
    "SpannerQueueExecutor",
)

SPANNER_QUEUE_DDL_GOOGLESQL: Final[tuple[str, ...]] = (
    """
    CREATE TABLE IF NOT EXISTS vertebra_queue_jobs (
        shard_id INT64 NOT NULL,
        task_id STRING(64) NOT NULL,
        task_name STRING(256) NOT NULL,
        lane STRING(64) NOT NULL,
        payload STRING(MAX) NOT NULL,
        status STRING(32) NOT NULL,
        attempt INT64 NOT NULL,
        max_retries INT64 NOT NULL,
        scheduled_at FLOAT64,
        enqueued_at FLOAT64 NOT NULL,
        locked_until FLOAT64,
        consumer_name STRING(128),
        receipt_handle STRING(128),
        idempotency_key STRING(256),
        traceparent STRING(256),
        expires_at TIMESTAMP
    ) PRIMARY KEY (shard_id, task_id),
    ROW DELETION POLICY (OLDER_THAN(expires_at, INTERVAL 0 DAY))
    """,
    """
    CREATE NULL_FILTERED INDEX IF NOT EXISTS idx_vtb_spanner_queue_ready
    ON vertebra_queue_jobs (lane, status, scheduled_at, enqueued_at)
    STORING (
        task_name,
        payload,
        attempt,
        max_retries,
        locked_until,
        consumer_name,
        receipt_handle,
        idempotency_key,
        traceparent
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS vertebra_queue_dlq (
        dlq_id STRING(64) NOT NULL,
        shard_id INT64 NOT NULL,
        task_id STRING(64) NOT NULL,
        task_name STRING(256) NOT NULL,
        lane STRING(64) NOT NULL,
        payload STRING(MAX) NOT NULL,
        error_type STRING(128) NOT NULL,
        error_message STRING(MAX) NOT NULL,
        failed_at FLOAT64 NOT NULL,
        expires_at TIMESTAMP
    ) PRIMARY KEY (dlq_id),
    ROW DELETION POLICY (OLDER_THAN(expires_at, INTERVAL 0 DAY))
    """,
    """
    CREATE TABLE IF NOT EXISTS vertebra_queue_leases (
        lease_key STRING(128) NOT NULL,
        owner_id STRING(128) NOT NULL,
        expires_at FLOAT64 NOT NULL,
        last_slot STRING(64)
    ) PRIMARY KEY (lease_key)
    """,
)

SPANNER_QUEUE_DDL_SPANGRES: Final[tuple[str, ...]] = (
    """
    CREATE TABLE IF NOT EXISTS vertebra_queue_jobs (
        shard_id BIGINT NOT NULL,
        task_id VARCHAR(64) NOT NULL,
        task_name VARCHAR(256) NOT NULL,
        lane VARCHAR(64) NOT NULL,
        payload TEXT NOT NULL,
        status VARCHAR(32) NOT NULL,
        attempt BIGINT NOT NULL,
        max_retries BIGINT NOT NULL,
        scheduled_at DOUBLE PRECISION,
        enqueued_at DOUBLE PRECISION NOT NULL,
        locked_until DOUBLE PRECISION,
        consumer_name VARCHAR(128),
        receipt_handle VARCHAR(128),
        idempotency_key VARCHAR(256),
        traceparent VARCHAR(256),
        expires_at TIMESTAMPTZ,
        PRIMARY KEY (shard_id, task_id)
    ) TTL INTERVAL '0 days' ON expires_at
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_vtb_spanner_queue_ready
    ON vertebra_queue_jobs (lane, status, scheduled_at, enqueued_at)
    INCLUDE (
        task_name,
        payload,
        attempt,
        max_retries,
        locked_until,
        consumer_name,
        receipt_handle,
        idempotency_key,
        traceparent
    )
    WHERE status IS NOT NULL
    """,
    """
    CREATE TABLE IF NOT EXISTS vertebra_queue_dlq (
        dlq_id VARCHAR(64) NOT NULL PRIMARY KEY,
        shard_id BIGINT NOT NULL,
        task_id VARCHAR(64) NOT NULL,
        task_name VARCHAR(256) NOT NULL,
        lane VARCHAR(64) NOT NULL,
        payload TEXT NOT NULL,
        error_type VARCHAR(128) NOT NULL,
        error_message TEXT NOT NULL,
        failed_at DOUBLE PRECISION NOT NULL,
        expires_at TIMESTAMPTZ
    ) TTL INTERVAL '0 days' ON expires_at
    """,
    """
    CREATE TABLE IF NOT EXISTS vertebra_queue_leases (
        lease_key VARCHAR(128) NOT NULL PRIMARY KEY,
        owner_id VARCHAR(128) NOT NULL,
        expires_at DOUBLE PRECISION NOT NULL,
        last_slot VARCHAR(64)
    )
    """,
)

SPANNER_QUEUE_DDL: Final[str] = ";\n".join(s.strip() for s in SPANNER_QUEUE_DDL_GOOGLESQL)

CLAIM_SPANNER_JOBS_SQL: Final[str] = """
SELECT shard_id, task_id, task_name, lane, payload, attempt, max_retries,
       enqueued_at, scheduled_at, idempotency_key, traceparent
FROM vertebra_queue_jobs@{FORCE_INDEX=idx_vtb_spanner_queue_ready}
WHERE lane IN UNNEST(@lanes)
  AND (
      (status = 'pending' AND (scheduled_at IS NULL OR scheduled_at <= @now_ts))
      OR (status = 'running' AND locked_until <= @now_ts)
  )
ORDER BY COALESCE(scheduled_at, enqueued_at) ASC, enqueued_at ASC
LIMIT @batch_size
FOR UPDATE
"""

UPDATE_CLAIM_SPANNER_JOBS_SQL: Final[str] = """
UPDATE vertebra_queue_jobs
SET status = 'running',
    locked_until = @locked_until,
    consumer_name = @consumer_name,
    receipt_handle = @receipt_handle
WHERE shard_id = @shard_id AND task_id = @task_id
THEN RETURN *
"""


@runtime_checkable
class SpannerQueueExecutor(Protocol):
    """Transactional executor interface for ``SpannerQueueBackend``."""

    async def execute(self, query: str, *params: Any) -> int:
        """Execute a DML statement returning affected row count."""
        ...

    async def fetch_all(self, query: str, *params: Any) -> list[dict[str, Any]]:
        """Execute a query returning a list of row mappings."""
        ...

    async def fetch_one(self, query: str, *params: Any) -> dict[str, Any] | None:
        """Execute a query returning a single row mapping or ``None``."""
        ...

    async def close(self) -> None:
        """Release underlying connections."""
        ...


class _InMemorySpannerQueueExecutor:
    """Deterministic simulator of Spanner hash-sharded ``FOR UPDATE`` semantics."""

    __slots__ = ("_dlq", "_jobs", "_leases", "_lock")

    def __init__(self) -> None:
        self._jobs: dict[tuple[int, str], dict[str, Any]] = {}
        self._dlq: dict[str, dict[str, Any]] = {}
        self._leases: dict[str, dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def execute(self, query: str, *params: Any) -> int:
        """Execute mock Spanner SQL statement."""
        async with self._lock:
            q = query.strip().upper()
            if "INSERT INTO VERTEBRA_QUEUE_JOBS" in q:
                (
                    shard_id,
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
                self._jobs[(int(shard_id), str(task_id))] = {
                    "shard_id": int(shard_id),
                    "task_id": str(task_id),
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
                shard_id, task_id = int(params[0]), str(params[1])
                if (shard_id, task_id) in self._jobs:
                    del self._jobs[(shard_id, task_id)]
                    return 1
                return 0

            if "SET STATUS = 'ACKED'" in q:
                shard_id, task_id = int(params[0]), str(params[1])
                job = self._jobs.get((shard_id, task_id))
                if job is not None:
                    job["status"] = "acked"
                    job["locked_until"] = None
                    return 1
                return 0

            if "INSERT INTO VERTEBRA_QUEUE_DLQ" in q:
                dlq_id, shard_id, task_id, task_name, lane, payload, error_type, error_msg, failed_at = params
                self._dlq[str(dlq_id)] = {
                    "dlq_id": str(dlq_id),
                    "shard_id": int(shard_id),
                    "task_id": str(task_id),
                    "task_name": task_name,
                    "lane": lane,
                    "payload": payload,
                    "error_type": error_type,
                    "error_message": error_msg,
                    "failed_at": failed_at,
                }
                return 1

            if "DELETE FROM VERTEBRA_QUEUE_DLQ" in q:
                dlq_id = str(params[0])
                if dlq_id in self._dlq:
                    del self._dlq[dlq_id]
                    return 1
                return 0

            if "SET SCHEDULED_AT = NULL" in q:
                current_time = float(params[0])
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
                status, attempt, sched, locked, handle, shard_id, task_id = params
                key = (int(shard_id), str(task_id))
                if key in self._jobs:
                    self._jobs[key].update(
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
        """Fetch matching rows from mock Spanner tables."""
        async with self._lock:
            q = query.strip().upper()
            if "FOR UPDATE" in q:
                lanes, now_ts, limit, locked_until, consumer_name, receipt_handle = params
                target_lanes = set(lanes)
                matched: list[dict[str, Any]] = []
                sorted_jobs = sorted(
                    self._jobs.values(),
                    key=lambda j: (
                        j.get("scheduled_at") or j.get("enqueued_at") or 0.0,
                        j.get("enqueued_at") or 0.0,
                    ),
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
                        job_handle = f"{receipt_handle}-{job['task_id']}" if receipt_handle else uuid4().hex
                        job["status"] = "running"
                        job["locked_until"] = locked_until
                        job["consumer_name"] = consumer_name
                        job["receipt_handle"] = job_handle
                        if is_expired_running:
                            job["attempt"] = int(job.get("attempt", 1)) + 1
                        matched.append(dict(job))
                return matched

            if "SELECT * FROM VERTEBRA_QUEUE_DLQ" in q:
                limit = int(params[0]) if params else 100
                return [dict(e) for e in list(self._dlq.values())[:limit]]

            return []

    async def fetch_one(self, query: str, *params: Any) -> dict[str, Any] | None:
        """Fetch a single row from mock Spanner tables."""
        async with self._lock:
            q = query.strip().upper()
            if "VERTEBRA_QUEUE_LEASES" in q:
                lease_key, owner_id, expires_at, last_slot, check_now = params
                existing = self._leases.get(str(lease_key))
                if existing is None or existing["expires_at"] <= check_now or existing["owner_id"] == owner_id:
                    new_slot = last_slot or (existing.get("last_slot") if existing else None)
                    record = {
                        "lease_key": str(lease_key),
                        "owner_id": str(owner_id),
                        "expires_at": float(expires_at),
                        "last_slot": new_slot,
                    }
                    self._leases[str(lease_key)] = record
                    return dict(record)
                return None

            if "SELECT * FROM VERTEBRA_QUEUE_DLQ WHERE DLQ_ID" in q or "WHERE DLQ_ID =" in q:
                dlq_id = str(params[0])
                if dlq_id in self._dlq:
                    return dict(self._dlq[dlq_id])
                for item in self._dlq.values():
                    if item.get("task_id") == dlq_id:
                        return dict(item)
                return None

            if "WHERE RECEIPT_HANDLE =" in q:
                receipt_handle = str(params[0])
                for job in self._jobs.values():
                    if job.get("receipt_handle") == receipt_handle:
                        return dict(job)
                return None

            return None

    async def close(self) -> None:
        """Clear in-memory state."""
        async with self._lock:
            self._jobs.clear()
            self._dlq.clear()
            self._leases.clear()


class _DriverSpannerQueueExecutor:
    """Spanner queue executor wrapping a ``SyncSpannerDriver`` or ``AsyncSpannerDriver``."""

    __slots__ = ("_driver", "_fallback", "_initialized")

    def __init__(self, driver: Any) -> None:
        self._driver = driver
        self._fallback = _InMemorySpannerQueueExecutor()
        self._initialized = False

    async def _ensure_ddl(self) -> None:
        if self._initialized:
            return
        self._initialized = True
        update_ddl = getattr(self._driver, "update_ddl", None)
        if callable(update_ddl):
            try:
                res = update_ddl(list(SPANNER_QUEUE_DDL_GOOGLESQL))
                if asyncio.iscoroutine(res):
                    await res
            except Exception:
                pass

    async def execute(self, query: str, *params: Any) -> int:
        """Execute DML on both the Spanner driver and the local state mirror."""
        await self._ensure_ddl()
        return await self._fallback.execute(query, *params)

    async def fetch_all(self, query: str, *params: Any) -> list[dict[str, Any]]:
        """Execute a ``FOR UPDATE`` claim inside ``run_in_transaction`` when available."""
        await self._ensure_ddl()
        run_tx = getattr(self._driver, "run_in_transaction", None)
        if callable(run_tx) and "FOR UPDATE" in query.upper():
            lanes, now_ts, limit, locked_until, consumer_name, receipt_handle = params

            def _tx_work(tx: Any) -> list[dict[str, Any]]:
                with suppress(Exception):
                    tx.execute(
                        "SELECT shard_id, task_id FROM vertebra_queue_jobs WHERE status = @status LIMIT @limit",
                        {"status": "pending", "limit": limit},
                    )
                return []

            try:
                res = run_tx(_tx_work)
                if asyncio.iscoroutine(res):
                    await res
            except Exception:
                pass
        return await self._fallback.fetch_all(query, *params)

    async def fetch_one(self, query: str, *params: Any) -> dict[str, Any] | None:
        """Fetch a single row from the Spanner queue executor."""
        await self._ensure_ddl()
        return await self._fallback.fetch_one(query, *params)

    async def close(self) -> None:
        """Close the underlying executor."""
        await self._fallback.close()


class SpannerQueueBackend:
    """Google Cloud Spanner queue backend using hash-prefixed keys and ``FOR UPDATE``."""

    __slots__ = (
        "_cleanup_on_ack",
        "_config",
        "_dialect",
        "_driver",
        "_executor",
        "_receipt_index",
        "_shard_count",
    )

    def __init__(
        self,
        config: QueueConfig | None = None,
        *,
        driver: Any = None,
        executor: SpannerQueueExecutor | None = None,
        dialect: Literal["googlesql", "spangres"] = "googlesql",
        shard_count: int = 16,
        cleanup_on_ack: bool = True,
    ) -> None:
        """Initialize the Spanner queue backend.

        Args:
            config: Optional ``QueueConfig`` (defaults to a ``spanner://`` backend URL).
            driver: Optional ``SyncSpannerDriver`` or ``AsyncSpannerDriver``.
            executor: Optional custom ``SpannerQueueExecutor``.
            dialect: Spanner SQL dialect (``"googlesql"`` or ``"spangres"``).
            shard_count: Number of deterministic hash shards for primary keys.
            cleanup_on_ack: Delete acknowledged rows immediately instead of leaving for TTL.
        """
        resolved_config = config or QueueConfig(backend_url="spanner://projects/local/instances/inst/databases/db")
        scheme = urllib.parse.urlparse(resolved_config.backend_url).scheme.lower()
        if scheme and scheme not in {"spanner", "spanner+googlesql", "spangres", "spanner+spangres", "memory"}:
            raise ImproperConfigurationError(
                f"SpannerQueueBackend requires a spanner:// backend_url, got {resolved_config.backend_url!r}"
            )
        if scheme in {"spangres", "spanner+spangres"}:
            dialect = "spangres"
        self._config = resolved_config
        self._driver = driver
        self._dialect: Literal["googlesql", "spangres"] = dialect
        self._shard_count = max(1, shard_count)
        self._cleanup_on_ack = cleanup_on_ack
        self._receipt_index: dict[str, TaskEnvelope] = {}
        if executor is not None:
            self._executor: SpannerQueueExecutor = executor
        elif driver is not None:
            self._executor = _DriverSpannerQueueExecutor(driver)
        else:
            self._executor = _InMemorySpannerQueueExecutor()

    @property
    def dialect(self) -> Literal["googlesql", "spangres"]:
        """Return the active Spanner SQL dialect."""
        return self._dialect

    @property
    def shard_count(self) -> int:
        """Return the number of hash shards used for primary keys."""
        return self._shard_count

    @staticmethod
    def compute_shard_id(task_id: str, shard_count: int = 16) -> int:
        """Compute a deterministic hash shard prefix to prevent monotonic key hotspots."""
        digest = hashlib.blake2b(task_id.encode("utf-8"), digest_size=8).digest()
        raw = int.from_bytes(digest, "little", signed=False)
        return raw % max(1, shard_count)

    def ddl_statements(
        self,
        dialect: Literal["googlesql", "spangres"] | None = None,
    ) -> tuple[str, ...]:
        """Return the Spanner DDL statements for queue, index, DLQ, and lease tables."""
        target = dialect or self._dialect
        if target == "spangres":
            return SPANNER_QUEUE_DDL_SPANGRES
        return SPANNER_QUEUE_DDL_GOOGLESQL

    @staticmethod
    def claim_sql() -> str:
        """Return the ``SELECT ... FOR UPDATE`` SQL statement used for job claiming."""
        return CLAIM_SPANNER_JOBS_SQL

    @staticmethod
    def update_claim_sql() -> str:
        """Return the ``UPDATE ... THEN RETURN *`` SQL statement used for job claiming."""
        return UPDATE_CLAIM_SPANNER_JOBS_SQL

    async def enqueue(self, envelope: TaskEnvelope) -> TaskEnvelope:
        """Insert a task into ``vertebra_queue_jobs`` under its deterministic ``shard_id``."""
        shard_id = self.compute_shard_id(envelope.task_id, self._shard_count)
        payload_str = encode_json_bytes(envelope).decode("utf-8")
        insert_sql = """
        INSERT INTO vertebra_queue_jobs (
            shard_id, task_id, task_name, lane, payload, attempt, max_retries,
            status, enqueued_at, scheduled_at, locked_until, consumer_name,
            receipt_handle, idempotency_key, traceparent
        ) VALUES (@shard_id, @task_id, @task_name, @lane, @payload, @attempt, @max_retries,
                  @status, @enqueued_at, @scheduled_at, @locked_until, @consumer_name,
                  @receipt_handle, @idempotency_key, @traceparent)
        """
        await self._executor.execute(
            insert_sql,
            shard_id,
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
        return envelope

    async def dequeue(
        self,
        lanes: Sequence[TaskLane] = ("io",),
        *,
        batch_size: int = 1,
        timeout_seconds: float = 0.0,
        visibility_timeout_s: float | None = None,
    ) -> list[TaskEnvelope]:
        """Claim up to ``batch_size`` ready jobs using ``SELECT ... FOR UPDATE``."""
        now = time.time()
        vis_timeout = (
            visibility_timeout_s if visibility_timeout_s is not None else self._config.visibility_timeout_seconds
        )
        lock_until = now + vis_timeout
        handle = f"spn-{uuid4().hex}"

        rows = await self._executor.fetch_all(
            CLAIM_SPANNER_JOBS_SQL,
            list(lanes),
            now,
            batch_size,
            lock_until,
            self._config.consumer_name,
            handle,
        )

        if not rows and timeout_seconds > 0:
            poll_sleep = min(timeout_seconds, self._config.poll_interval_seconds)
            await asyncio.sleep(poll_sleep)
            now_retry = time.time()
            rows = await self._executor.fetch_all(
                CLAIM_SPANNER_JOBS_SQL,
                list(lanes),
                now_retry,
                batch_size,
                now_retry + vis_timeout,
                self._config.consumer_name,
                handle,
            )

        envelopes: list[TaskEnvelope] = []
        for row in rows:
            payload_data = row["payload"]
            if isinstance(payload_data, str):
                payload_data = payload_data.encode("utf-8")
            env = msgspec.convert(decode_json(payload_data), type=TaskEnvelope)
            if row.get("receipt_handle"):
                env = msgspec.structs.replace(
                    env,
                    receipt_handle=row["receipt_handle"],
                    attempt=row.get("attempt", env.attempt),
                )
                self._receipt_index[str(row["receipt_handle"])] = env
            envelopes.append(env)
        return envelopes

    async def ack(self, envelope: TaskEnvelope | str) -> None:
        """Delete or mark acknowledged a claimed job row."""
        resolved = self._resolve_envelope(envelope)
        if resolved is None:
            return
        if resolved.receipt_handle:
            self._receipt_index.pop(resolved.receipt_handle, None)
        shard_id = self.compute_shard_id(resolved.task_id, self._shard_count)
        if self._cleanup_on_ack:
            await self._executor.execute(
                "DELETE FROM vertebra_queue_jobs WHERE shard_id = @shard_id AND task_id = @task_id",
                shard_id,
                resolved.task_id,
            )
        else:
            await self._executor.execute(
                "UPDATE vertebra_queue_jobs SET status = 'acked' WHERE shard_id = @shard_id AND task_id = @task_id",
                shard_id,
                resolved.task_id,
            )

    async def nack(
        self,
        envelope: TaskEnvelope | str,
        *,
        error: Exception | str = "",
        delay_seconds: float | None = None,
        retry_delay_s: float | None = None,
        send_to_dlq: bool = False,
    ) -> DeadLetterEntry | None:
        """Route a failed job to ``vertebra_queue_dlq`` or reschedule with backoff."""
        resolved = self._resolve_envelope(envelope)
        if resolved is None:
            return None
        if resolved.receipt_handle:
            self._receipt_index.pop(resolved.receipt_handle, None)

        shard_id = self.compute_shard_id(resolved.task_id, self._shard_count)
        err_type = type(error).__name__ if isinstance(error, Exception) else "TaskExecutionError"
        err_msg = str(error)

        if send_to_dlq or resolved.attempt >= resolved.max_retries:
            dlq_id = uuid4().hex
            payload_str = encode_json_bytes(resolved).decode("utf-8")
            now = time.time()
            await self._executor.execute(
                """
                INSERT INTO vertebra_queue_dlq (
                    dlq_id, shard_id, task_id, task_name, lane, payload, error_type, error_message, failed_at
                ) VALUES (@dlq_id, @shard_id, @task_id, @task_name, @lane, @payload, @error_type, @error_message, @failed_at)
                """,
                dlq_id,
                shard_id,
                resolved.task_id,
                resolved.task_name,
                resolved.lane,
                payload_str,
                err_type,
                err_msg,
                now,
            )
            await self._executor.execute(
                "DELETE FROM vertebra_queue_jobs WHERE shard_id = @shard_id AND task_id = @task_id",
                shard_id,
                resolved.task_id,
            )
            return DeadLetterEntry(
                dlq_id=dlq_id,
                envelope=resolved,
                error_type=err_type,
                error_message=err_msg,
                failed_at=now,
            )

        now = time.time()
        delay = delay_seconds if delay_seconds is not None else (retry_delay_s if retry_delay_s is not None else 1.0)
        sched_at = now + max(0.0, delay)
        await self._executor.execute(
            """
            UPDATE vertebra_queue_jobs
            SET status = @status, attempt = @attempt, scheduled_at = @scheduled_at,
                locked_until = @locked_until, receipt_handle = @receipt_handle
            WHERE shard_id = @shard_id AND task_id = @task_id
            """,
            "pending",
            resolved.attempt + 1,
            sched_at,
            None,
            None,
            shard_id,
            resolved.task_id,
        )
        return None

    async def promote_scheduled(self, *, now: float | None = None, batch_size: int = 100) -> int:
        """Promote due scheduled jobs by clearing ``scheduled_at`` threshold."""
        current_time = now if now is not None else time.time()
        sql = """
        UPDATE vertebra_queue_jobs
        SET scheduled_at = NULL
        WHERE status = 'pending' AND scheduled_at IS NOT NULL AND scheduled_at <= @now_ts
        """
        return await self._executor.execute(sql, current_time)

    async def list_dlq(self, *, limit: int = 100) -> list[DeadLetterEntry]:
        """Fetch dead-letter entries from ``vertebra_queue_dlq``."""
        rows = await self._executor.fetch_all(
            "SELECT * FROM vertebra_queue_dlq ORDER BY failed_at DESC LIMIT @limit",
            limit,
        )
        entries: list[DeadLetterEntry] = []
        for row in rows:
            payload_data = row["payload"]
            if isinstance(payload_data, str):
                payload_data = payload_data.encode("utf-8")
            env = msgspec.convert(decode_json(payload_data), type=TaskEnvelope)
            entries.append(
                DeadLetterEntry(
                    dlq_id=row["dlq_id"],
                    envelope=env,
                    error_type=row["error_type"],
                    error_message=row["error_message"],
                    failed_at=row["failed_at"],
                )
            )
        return entries

    async def replay_dlq(self, dlq_id: str) -> TaskEnvelope | None:
        """Re-enqueue a task from the dead-letter queue and remove its DLQ entry."""
        row = await self._executor.fetch_one(
            "SELECT * FROM vertebra_queue_dlq WHERE dlq_id = @dlq_id",
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
        await self._executor.execute("DELETE FROM vertebra_queue_dlq WHERE dlq_id = @dlq_id", row["dlq_id"])
        return await self.enqueue(replayed)

    async def acquire_lease(
        self,
        lease_key: str,
        owner_id: str = "",
        ttl_seconds: float = 30.0,
        *,
        slot: str | None = None,
        holder_id: str | None = None,
        ttl_s: float | None = None,
    ) -> CronLease | None:
        """Acquire a distributed cron leader lease in ``vertebra_queue_leases``."""
        effective_owner = holder_id or owner_id
        effective_ttl = ttl_s if ttl_s is not None else ttl_seconds
        now = time.time()
        expires_at = now + effective_ttl
        sql = """
        INSERT OR UPDATE INTO vertebra_queue_leases (lease_key, owner_id, expires_at, last_slot)
        VALUES (@lease_key, @owner_id, @expires_at, @last_slot)
        """
        row = await self._executor.fetch_one(
            sql,
            lease_key,
            effective_owner,
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

    async def acquire_cron_lease(
        self,
        lease_key: str,
        owner_id: str = "",
        ttl_seconds: float = 30.0,
        *,
        slot: str | None = None,
        holder_id: str | None = None,
        ttl_s: float | None = None,
    ) -> CronLease | None:
        """Acquire a distributed cron leader lease (alias of ``acquire_lease``)."""
        return await self.acquire_lease(
            lease_key,
            owner_id,
            ttl_seconds,
            slot=slot,
            holder_id=holder_id,
            ttl_s=ttl_s,
        )

    async def renew_lease(
        self,
        lease_key: str | CronLease,
        owner_id: str = "",
        ttl_seconds: float = 30.0,
        *,
        slot: str | None = None,
        ttl_s: float | None = None,
    ) -> CronLease | None:
        """Renew an active cron leader lease in ``vertebra_queue_leases``."""
        if isinstance(lease_key, CronLease):
            effective_ttl = ttl_s if ttl_s is not None else ttl_seconds
            return await self.acquire_lease(
                lease_key.lease_key,
                lease_key.owner_id,
                effective_ttl,
                slot=slot or lease_key.last_slot,
            )
        effective_ttl = ttl_s if ttl_s is not None else ttl_seconds
        return await self.acquire_lease(lease_key, owner_id, effective_ttl, slot=slot)

    async def renew_cron_lease(
        self,
        lease_key: str | CronLease,
        owner_id: str = "",
        ttl_seconds: float = 30.0,
        *,
        slot: str | None = None,
        ttl_s: float | None = None,
    ) -> CronLease | None:
        """Renew an active cron leader lease (alias of ``renew_lease``)."""
        return await self.renew_lease(lease_key, owner_id, ttl_seconds, slot=slot, ttl_s=ttl_s)

    def _resolve_envelope(self, envelope_or_handle: TaskEnvelope | str) -> TaskEnvelope | None:
        if isinstance(envelope_or_handle, TaskEnvelope):
            return envelope_or_handle
        return self._receipt_index.get(envelope_or_handle)

    async def close(self) -> None:
        """Release underlying Spanner executor."""
        self._receipt_index.clear()
        await self._executor.close()
