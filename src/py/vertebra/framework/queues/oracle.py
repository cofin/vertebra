"""Oracle Database task queue backend using FOR UPDATE SKIP LOCKED and DLQ persistence."""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import msgspec

from vertebra.db.adapters.oracle._async import AsyncOracleDriver
from vertebra.db.adapters.oracle._sync import SyncOracleDriver
from vertebra.db.adapters.oracle.config import OracleConfig
from vertebra.db.adapters.oracle.notify import (
    AsyncOracleNotificationSource,
    SyncOracleNotificationSource,
)
from vertebra.db.adapters.oracle.queue import (
    AsyncOracleQueueStore,
    SyncOracleQueueStore,
)
from vertebra.framework.queues._sql import (
    _SqlQueueBackendBase,
    decode_envelope_bytes,
    error_type_and_message,
    rows_to_dlq_entries,
    rows_to_envelopes,
    unpack_queue_settings,
)
from vertebra.framework.queues.base import (
    CronLease,
    DeadLetterEntry,
    QueueBackendSettings,
    QueueConfig,
    TaskEnvelope,
    TaskLane,
)
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import encode_json_bytes

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = (
    "CLAIM_ORACLE_JOBS_SQL",
    "ORACLE_QUEUE_DDL",
    "OracleQueueBackend",
    "OracleQueueExecutor",
)

ORACLE_QUEUE_DDL = """
CREATE TABLE vertebra_queue_jobs (
    task_id VARCHAR2(64) PRIMARY KEY,
    task_name VARCHAR2(256) NOT NULL,
    lane VARCHAR2(32) NOT NULL,
    payload CLOB NOT NULL,
    attempt NUMBER(10) DEFAULT 1 NOT NULL,
    max_retries NUMBER(10) DEFAULT 3 NOT NULL,
    status VARCHAR2(32) DEFAULT 'pending' NOT NULL,
    enqueued_at NUMBER(19, 4) NOT NULL,
    scheduled_at NUMBER(19, 4),
    locked_until NUMBER(19, 4),
    consumer_name VARCHAR2(128),
    receipt_handle VARCHAR2(64),
    idempotency_key VARCHAR2(128) UNIQUE,
    traceparent VARCHAR2(128)
);

CREATE TABLE vertebra_queue_dlq (
    dlq_id VARCHAR2(64) PRIMARY KEY,
    task_id VARCHAR2(64) NOT NULL,
    task_name VARCHAR2(256) NOT NULL,
    lane VARCHAR2(32) NOT NULL,
    payload CLOB NOT NULL,
    error_type VARCHAR2(128) NOT NULL,
    error_message VARCHAR2(4000) NOT NULL,
    failed_at NUMBER(19, 4) NOT NULL
);

CREATE TABLE vertebra_queue_leases (
    lease_key VARCHAR2(128) PRIMARY KEY,
    owner_id VARCHAR2(128) NOT NULL,
    expires_at NUMBER(19, 4) NOT NULL,
    last_slot VARCHAR2(64)
);
"""

CLAIM_ORACLE_JOBS_SQL = """
SELECT task_id, task_name, lane, payload, status, attempt, max_retries,
       enqueued_at, scheduled_at, idempotency_key, traceparent
FROM vertebra_queue_jobs
WHERE lane IN (:1)
  AND (
      (status = 'pending' AND (scheduled_at IS NULL OR scheduled_at <= :2))
      OR (status = 'running' AND locked_until <= :2)
  )
ORDER BY COALESCE(scheduled_at, enqueued_at) ASC, enqueued_at ASC
FETCH FIRST :3 ROWS ONLY
FOR UPDATE SKIP LOCKED
"""


@runtime_checkable
class OracleQueueExecutor(Protocol):
    """Transactional cursor executor contract for OracleQueueBackend."""

    async def execute(self, query: str, *params: Any) -> int:
        """Execute DML statement returning affected row count."""
        ...

    async def fetch_all(self, query: str, *params: Any) -> list[dict[str, Any]]:
        """Fetch rows matching query."""
        ...

    async def fetch_one(self, query: str, *params: Any) -> dict[str, Any] | None:
        """Fetch single matching row."""
        ...

    async def commit(self) -> None:
        """Commit active transaction."""
        ...

    async def close(self) -> None:
        """Close connection or pool resources."""
        ...


class OracleQueueBackend(_SqlQueueBackendBase):
    """Oracle Database queue backend using FOR UPDATE SKIP LOCKED over native stores or an executor."""

    __slots__ = ("_config", "_executor", "_oracle_config", "_store", "_sync_store")

    _sync_store_label = "SyncOracleQueueStore"

    def __init__(
        self,
        config: QueueConfig | QueueBackendSettings | None = None,
        *,
        oracle_config: OracleConfig | None = None,
        driver: AsyncOracleDriver | None = None,
        sync_driver: SyncOracleDriver | None = None,
        executor: OracleQueueExecutor | None = None,
        store: AsyncOracleQueueStore | None = None,
        sync_store: SyncOracleQueueStore | None = None,
    ) -> None:
        """Initialize the Oracle Database queue backend."""
        store, sync_store, driver, sync_driver = unpack_queue_settings(config, store, sync_store, driver, sync_driver)
        self._config = config or QueueConfig()
        self._oracle_config = oracle_config
        if executor is not None:
            self._executor: OracleQueueExecutor | None = executor
            self._store: AsyncOracleQueueStore | None = store
            self._sync_store: SyncOracleQueueStore | None = sync_store
        elif store is not None or sync_store is not None:
            self._store = store
            self._sync_store = sync_store
            self._executor = None
        elif driver is not None or sync_driver is not None:
            self._store = (
                AsyncOracleQueueStore(driver, notification_source=AsyncOracleNotificationSource(driver))
                if driver is not None
                else None
            )
            self._sync_store = (
                SyncOracleQueueStore(sync_driver, notification_source=SyncOracleNotificationSource(sync_driver))
                if sync_driver is not None
                else None
            )
            self._executor = None
        else:
            ensure_module("vertebra._oracle", package="vertebra-core[oracle]", extra="oracle")
            dsn = self._config.backend_url
            if not dsn or dsn in ("oracle://",):
                raise ImproperConfigurationError(
                    "OracleQueueBackend requires a configured Oracle DSN, store, or executor"
                )
            resolved_ora_cfg = self._oracle_config or OracleConfig(dsn=dsn)
            async_drv = AsyncOracleDriver(resolved_ora_cfg)
            sync_drv = SyncOracleDriver(resolved_ora_cfg)
            self._store = AsyncOracleQueueStore(async_drv, notification_source=AsyncOracleNotificationSource(async_drv))
            self._sync_store = SyncOracleQueueStore(
                sync_drv, notification_source=SyncOracleNotificationSource(sync_drv)
            )
            self._executor = None

    @property
    def store(self) -> AsyncOracleQueueStore | None:
        """Return the active asynchronous native queue store, if configured."""
        return self._store

    @property
    def sync_store(self) -> SyncOracleQueueStore | None:
        """Return the active synchronous native queue store, if configured."""
        return self._sync_store

    async def enqueue(self, envelope: TaskEnvelope) -> TaskEnvelope:
        """Insert task into vertebra_queue_jobs."""
        if self._store is not None:
            return await self._store_enqueue(envelope)
        assert self._executor is not None
        payload_str = encode_json_bytes(envelope).decode("utf-8")
        await self._executor.execute(
            """
            INSERT INTO vertebra_queue_jobs (
                task_id, task_name, lane, payload, attempt, max_retries,
                status, enqueued_at, scheduled_at, locked_until, consumer_name,
                receipt_handle, idempotency_key, traceparent
            ) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14)
            """,
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
        await self._executor.commit()
        return envelope

    async def dequeue(
        self, lanes: Sequence[TaskLane], *, batch_size: int = 1, timeout_seconds: float = 0.0
    ) -> list[TaskEnvelope]:
        """Claim tasks with FOR UPDATE SKIP LOCKED."""
        if self._store is not None:
            return await self._store_dequeue(lanes, batch_size=batch_size, timeout_seconds=timeout_seconds)
        assert self._executor is not None
        rows = await self._claim_via_executor(lanes, batch_size=batch_size)
        if not rows and timeout_seconds > 0:
            await asyncio.sleep(min(timeout_seconds, self._config.poll_interval_seconds))
            rows = await self._claim_via_executor(lanes, batch_size=batch_size)
        return rows_to_envelopes(rows)

    async def ack(self, envelope: TaskEnvelope) -> None:
        """Delete acknowledged task and commit transaction."""
        if self._store is not None:
            await self._store.ack(envelope.task_id, attempt=envelope.attempt)
            return
        assert self._executor is not None
        await self._executor.execute("DELETE FROM vertebra_queue_jobs WHERE task_id = :1", envelope.task_id)
        await self._executor.commit()

    async def nack(
        self,
        envelope: TaskEnvelope,
        *,
        error: Exception | str,
        delay_seconds: float | None = None,
        send_to_dlq: bool = False,
    ) -> DeadLetterEntry | None:
        """Route to DLQ or reschedule with backoff."""
        if self._store is not None:
            return await self._store_nack(envelope, error=error, delay_seconds=delay_seconds, send_to_dlq=send_to_dlq)
        assert self._executor is not None
        err_type, err_msg = error_type_and_message(error)
        now = time.time()
        if send_to_dlq or envelope.attempt >= envelope.max_retries:
            dlq_id = uuid.uuid4().hex
            payload_str = encode_json_bytes(envelope).decode("utf-8")
            await self._executor.execute(
                """
                INSERT INTO vertebra_queue_dlq (
                    dlq_id, task_id, task_name, lane, payload, error_type, error_message, failed_at
                ) VALUES (:1, :2, :3, :4, :5, :6, :7, :8)
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
            await self._executor.execute("DELETE FROM vertebra_queue_jobs WHERE task_id = :1", envelope.task_id)
            await self._executor.commit()
            return DeadLetterEntry(
                dlq_id=dlq_id, envelope=envelope, error_type=err_type, error_message=err_msg, failed_at=now
            )
        sched_at = now + (delay_seconds if delay_seconds is not None else 1.0)
        await self._executor.execute(
            "UPDATE vertebra_queue_jobs SET status = :1, attempt = :2, scheduled_at = :3, "
            "locked_until = :4, receipt_handle = :5 WHERE task_id = :6",
            "pending",
            envelope.attempt + 1,
            sched_at,
            None,
            None,
            envelope.task_id,
        )
        await self._executor.commit()
        return None

    async def promote_scheduled(self, *, now: float | None = None, batch_size: int = 100) -> int:
        """Promote due scheduled jobs by clearing scheduled_at threshold."""
        if self._store is not None:
            return await self._store.promote_scheduled(limit=batch_size)
        assert self._executor is not None
        res = await self._executor.execute(
            "UPDATE vertebra_queue_jobs SET scheduled_at = NULL "
            "WHERE status = 'pending' AND scheduled_at IS NOT NULL AND scheduled_at <= :1",
            now if now is not None else time.time(),
        )
        await self._executor.commit()
        return res

    async def list_dlq(self, *, limit: int = 100) -> list[DeadLetterEntry]:
        """Fetch dead-letter entries from the DLQ table."""
        if self._store is not None:
            return await self._store_list_dlq(limit)
        assert self._executor is not None
        rows = await self._executor.fetch_all(
            "SELECT * FROM vertebra_queue_dlq ORDER BY failed_at DESC FETCH FIRST :1 ROWS ONLY", limit
        )
        return rows_to_dlq_entries(rows)

    async def replay_dlq(self, dlq_id: str) -> TaskEnvelope | None:
        """Re-enqueue task from dead-letter queue and commit."""
        if self._store is not None:
            return await self._store_replay_dlq(dlq_id)
        assert self._executor is not None
        row = await self._executor.fetch_one("SELECT * FROM vertebra_queue_dlq WHERE dlq_id = :1", dlq_id)
        if not row:
            return None
        replayed = msgspec.structs.replace(
            decode_envelope_bytes(row["payload"]), attempt=1, scheduled_at=None, receipt_handle=None
        )
        await self._executor.execute("DELETE FROM vertebra_queue_dlq WHERE dlq_id = :1", dlq_id)
        await self._executor.commit()
        return await self.enqueue(replayed)

    async def acquire_lease(
        self, lease_key: str, owner_id: str, ttl_seconds: float, *, slot: str | None = None
    ) -> CronLease | None:
        """Acquire lease via MERGE INTO vertebra_queue_leases."""
        if self._store is not None:
            return await self._store_acquire_lease(lease_key, owner_id, ttl_seconds, slot=slot)
        assert self._executor is not None
        now = time.time()
        expires_at = now + ttl_seconds
        sql = """
        MERGE INTO vertebra_queue_leases l
        USING (SELECT :1 AS lease_key, :2 AS owner_id, :3 AS expires_at, :4 AS last_slot FROM DUAL) src
        ON (l.lease_key = src.lease_key)
        WHEN MATCHED THEN
          UPDATE SET l.owner_id = src.owner_id, l.expires_at = src.expires_at, l.last_slot = NVL(src.last_slot, l.last_slot)
          WHERE l.expires_at <= :5 OR l.owner_id = src.owner_id
        WHEN NOT MATCHED THEN
          INSERT (lease_key, owner_id, expires_at, last_slot)
          VALUES (src.lease_key, src.owner_id, src.expires_at, src.last_slot)
        """
        affected = await self._executor.execute(sql, lease_key, owner_id, expires_at, slot, now)
        if affected <= 0:
            await self._executor.commit()
            return None
        row = await self._executor.fetch_one(
            "SELECT lease_key, owner_id, expires_at, last_slot FROM vertebra_queue_leases WHERE lease_key = :1",
            lease_key,
        )
        await self._executor.commit()
        if row is None:
            return CronLease(lease_key=lease_key, owner_id=owner_id, expires_at=expires_at, last_slot=slot)
        lowered = {k.lower(): v for k, v in row.items()}
        if str(lowered.get("owner_id", "")) != owner_id:
            return None
        return CronLease(
            lease_key=str(lowered["lease_key"]),
            owner_id=str(lowered["owner_id"]),
            expires_at=float(lowered["expires_at"]),
            last_slot=lowered.get("last_slot"),
        )

    async def _claim_via_executor(self, lanes: Sequence[TaskLane], *, batch_size: int) -> list[dict[str, Any]]:
        """Select ready rows via dynamic positional binds, mark running, and commit."""
        assert self._executor is not None
        if not lanes or batch_size <= 0:
            return []
        lane_vals = [str(lane) for lane in lanes]
        now = time.time()
        lock_until = now + self._config.visibility_timeout_seconds
        candidates = await self._executor.fetch_all(
            _build_oracle_claim_sql(len(lane_vals)), *lane_vals, now, batch_size
        )
        if not candidates:
            await self._executor.commit()
            return []
        claimed_rows: list[dict[str, Any]] = []
        for cand in candidates:
            lowered = {k.lower(): v for k, v in cand.items()}
            task_id = str(lowered["task_id"])
            curr_attempt = int(lowered.get("attempt", 1))
            next_attempt = curr_attempt + 1 if str(lowered.get("status", "pending")) == "running" else curr_attempt
            row_handle = uuid.uuid4().hex
            await self._executor.execute(
                "UPDATE vertebra_queue_jobs SET status = :1, attempt = :2, locked_until = :3, "
                "consumer_name = :4, receipt_handle = :5 WHERE task_id = :6",
                "running",
                next_attempt,
                lock_until,
                self._config.consumer_name,
                row_handle,
                task_id,
            )
            lowered.update(
                status="running",
                attempt=next_attempt,
                locked_until=lock_until,
                consumer_name=self._config.consumer_name,
                receipt_handle=row_handle,
            )
            claimed_rows.append(lowered)
        await self._executor.commit()
        return claimed_rows


def _build_oracle_claim_sql(num_lanes: int) -> str:
    """Build Oracle SELECT ... FOR UPDATE SKIP LOCKED SQL with dynamic :1..:N lane binds."""
    lane_binds = ", ".join(f":{idx}" for idx in range(1, num_lanes + 1))
    now_idx = num_lanes + 1
    limit_idx = num_lanes + 2
    return f"""
    SELECT task_id, task_name, lane, payload, status, attempt, max_retries,
           enqueued_at, scheduled_at, idempotency_key, traceparent
    FROM vertebra_queue_jobs
    WHERE lane IN ({lane_binds})
      AND (
          (status = 'pending' AND (scheduled_at IS NULL OR scheduled_at <= :{now_idx}))
          OR (status = 'running' AND locked_until <= :{now_idx})
      )
    ORDER BY COALESCE(scheduled_at, enqueued_at) ASC, enqueued_at ASC
    FETCH FIRST :{limit_idx} ROWS ONLY
    FOR UPDATE SKIP LOCKED
    """
