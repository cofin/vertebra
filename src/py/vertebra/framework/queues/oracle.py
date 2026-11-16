"""Oracle Database task queue backend using FOR UPDATE SKIP LOCKED and DLQ persistence."""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import TYPE_CHECKING, Any, Protocol, cast, runtime_checkable

import msgspec

from vertebra.db.driver.capabilities import QueueEntry
from vertebra.framework.queues.base import (
    CronLease,
    DeadLetterEntry,
    QueueConfig,
    TaskEnvelope,
    TaskLane,
)
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import decode_json, encode_json_bytes

if TYPE_CHECKING:
    from collections.abc import Sequence

    from vertebra.db.adapters.oracle.queue import (
        AsyncOracleQueueStore,
        SyncOracleQueueStore,
    )
    from vertebra.framework.queues.base import QueueBackendSettings

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
SELECT task_id, task_name, lane, payload, attempt, max_retries,
       enqueued_at, scheduled_at, idempotency_key, traceparent
FROM vertebra_queue_jobs
WHERE lane IN (:lanes)
  AND (
      (status = 'pending' AND (scheduled_at IS NULL OR scheduled_at <= :now_ts))
      OR (status = 'running' AND locked_until <= :now_ts)
  )
ORDER BY COALESCE(scheduled_at, enqueued_at) ASC, enqueued_at ASC
FETCH FIRST :batch_size ROWS ONLY
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


class OracleQueueBackend:
    """Oracle Database queue backend using FOR UPDATE SKIP LOCKED over native stores or an executor."""

    __slots__ = ("_config", "_executor", "_oracle_config", "_store", "_sync_store")

    def __init__(
        self,
        config: QueueConfig | QueueBackendSettings | None = None,
        *,
        oracle_config: Any = None,
        executor: OracleQueueExecutor | None = None,
        store: AsyncOracleQueueStore | None = None,
        sync_store: SyncOracleQueueStore | None = None,
    ) -> None:
        """Initialize the Oracle Database queue backend.

        Args:
            config: Optional queue configuration settings or QueueBackendSettings.
            oracle_config: Optional Oracle connection configuration.
            executor: Optional OracleQueueExecutor binding a transactional cursor executor.
            store: Optional pre-constructed AsyncOracleQueueStore.
            sync_store: Optional pre-constructed SyncOracleQueueStore.

        Raises:
            MissingDependencyError: When neither executor nor store is passed and ``vertebra._oracle`` is not installed.
            ImproperConfigurationError: When neither executor nor store is passed and no valid Oracle DSN is configured.
        """
        self._config = config or QueueConfig()
        self._oracle_config = oracle_config
        if executor is not None:
            self._executor: OracleQueueExecutor | None = executor
            self._store: AsyncOracleQueueStore | None = store
            self._sync_store: SyncOracleQueueStore | None = sync_store
        elif store is not None:
            self._store = store
            self._sync_store = sync_store
            self._executor = None
        else:
            ensure_module("vertebra._oracle", package="vertebra-core[oracle]", extra="oracle")
            dsn = self._config.backend_url
            if not dsn or dsn in ("oracle://",):
                msg = "OracleQueueBackend requires a configured Oracle DSN, store, or executor"
                raise ImproperConfigurationError(msg)
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

            resolved_ora_cfg = self._oracle_config or OracleConfig(dsn=dsn)
            async_drv = AsyncOracleDriver(resolved_ora_cfg)
            sync_drv = SyncOracleDriver(resolved_ora_cfg)
            self._store = AsyncOracleQueueStore(
                async_drv,
                notification_source=AsyncOracleNotificationSource(async_drv),
            )
            self._sync_store = SyncOracleQueueStore(
                sync_drv,
                notification_source=SyncOracleNotificationSource(sync_drv),
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

    def enqueue_sync(self, envelope: TaskEnvelope) -> TaskEnvelope:
        """Enqueue task envelope synchronously via SyncQueueStore."""
        if self._sync_store is None:
            msg = "Synchronous enqueue requires a configured SyncOracleQueueStore"
            raise ImproperConfigurationError(msg)
        payload_bytes = encode_json_bytes(envelope)
        sched_ms = int(envelope.scheduled_at * 1000) if envelope.scheduled_at is not None else None
        headers: dict[str, str] = {}
        if envelope.idempotency_key:
            headers["idempotency_key"] = envelope.idempotency_key
        if envelope.traceparent:
            headers["traceparent"] = envelope.traceparent
        entry = QueueEntry(
            job_id=envelope.task_id,
            queue=envelope.lane,
            task_name=envelope.task_name,
            payload=payload_bytes,
            attempt=envelope.attempt,
            max_attempts=envelope.max_retries,
            scheduled_at_ms=sched_ms,
            headers=headers,
        )
        self._sync_store.enqueue(entry)
        return envelope

    async def enqueue(self, envelope: TaskEnvelope) -> TaskEnvelope:
        """Insert task into vertebra_queue_jobs."""
        if self._store is not None:
            payload_bytes = encode_json_bytes(envelope)
            sched_ms = int(envelope.scheduled_at * 1000) if envelope.scheduled_at is not None else None
            headers: dict[str, str] = {}
            if envelope.idempotency_key:
                headers["idempotency_key"] = envelope.idempotency_key
            if envelope.traceparent:
                headers["traceparent"] = envelope.traceparent
            entry = QueueEntry(
                job_id=envelope.task_id,
                queue=envelope.lane,
                task_name=envelope.task_name,
                payload=payload_bytes,
                attempt=envelope.attempt,
                max_attempts=envelope.max_retries,
                scheduled_at_ms=sched_ms,
                headers=headers,
            )
            await self._store.enqueue(entry)
            return envelope
        assert self._executor is not None
        payload_str = encode_json_bytes(envelope).decode("utf-8")
        insert_sql = """
        INSERT INTO vertebra_queue_jobs (
            task_id, task_name, lane, payload, attempt, max_retries,
            status, enqueued_at, scheduled_at, locked_until, consumer_name,
            receipt_handle, idempotency_key, traceparent
        ) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14)
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
        await self._executor.commit()
        return envelope

    async def dequeue(
        self,
        lanes: Sequence[TaskLane],
        *,
        batch_size: int = 1,
        timeout_seconds: float = 0.0,
    ) -> list[TaskEnvelope]:
        """Claim tasks with FOR UPDATE SKIP LOCKED."""
        if self._store is not None:
            entries = await self._store.claim(
                [str(lane) for lane in lanes],
                limit=batch_size,
                worker_id=self._config.consumer_name,
                lease_seconds=self._config.visibility_timeout_seconds,
            )
            if not entries and timeout_seconds > 0:
                await asyncio.sleep(min(timeout_seconds, self._config.poll_interval_seconds))
                entries = await self._store.claim(
                    [str(lane) for lane in lanes],
                    limit=batch_size,
                    worker_id=self._config.consumer_name,
                    lease_seconds=self._config.visibility_timeout_seconds,
                )
            envelopes: list[TaskEnvelope] = []
            for entry in entries:
                try:
                    env = msgspec.convert(decode_json(entry.payload), type=TaskEnvelope)
                    env = msgspec.structs.replace(
                        env,
                        attempt=entry.attempt,
                        receipt_handle=f"{entry.job_id}:{entry.attempt}",
                    )
                except Exception:
                    env = TaskEnvelope(
                        task_id=entry.job_id,
                        task_name=entry.task_name,
                        lane=cast("TaskLane", entry.queue),
                        args=(),
                        kwargs={},
                        attempt=entry.attempt,
                        max_retries=entry.max_attempts,
                        receipt_handle=f"{entry.job_id}:{entry.attempt}",
                    )
                envelopes.append(env)
            return envelopes
        assert self._executor is not None
        now = time.time()
        lock_until = now + self._config.visibility_timeout_seconds
        handle = uuid.uuid4().hex

        rows = await self._executor.fetch_all(
            CLAIM_ORACLE_JOBS_SQL,
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
                CLAIM_ORACLE_JOBS_SQL,
                list(lanes),
                now_retry,
                batch_size,
                now_retry + self._config.visibility_timeout_seconds,
                self._config.consumer_name,
                handle,
            )

        envelopes_ex: list[TaskEnvelope] = []
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
            envelopes_ex.append(env)
        return envelopes_ex

    async def ack(self, envelope: TaskEnvelope) -> None:
        """Delete acknowledged task and commit transaction."""
        if self._store is not None:
            await self._store.ack(envelope.task_id, attempt=envelope.attempt)
            return
        assert self._executor is not None
        await self._executor.execute(
            "DELETE FROM vertebra_queue_jobs WHERE task_id = :1",
            envelope.task_id,
        )
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
            err_type = type(error).__name__ if isinstance(error, Exception) else "TaskExecutionError"
            err_msg = str(error)
            now = time.time()
            if send_to_dlq or envelope.attempt >= envelope.max_retries:
                await self._store.nack(
                    envelope.task_id,
                    attempt=envelope.attempt,
                    error=f"{err_type}: {err_msg}",
                    retry_delay_s=None,
                )
                return DeadLetterEntry(
                    dlq_id=uuid.uuid4().hex,
                    envelope=envelope,
                    error_type=err_type,
                    error_message=err_msg,
                    failed_at=now,
                )
            await self._store.nack(
                envelope.task_id,
                attempt=envelope.attempt,
                error=f"{err_type}: {err_msg}",
                retry_delay_s=delay_seconds or 1.0,
            )
            return None
        assert self._executor is not None
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
            await self._executor.execute(
                "DELETE FROM vertebra_queue_jobs WHERE task_id = :1",
                envelope.task_id,
            )
            await self._executor.commit()
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
            SET status = :1, attempt = :2, scheduled_at = :3, locked_until = :4, receipt_handle = :5
            WHERE task_id = :6
            """,
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
        current_time = now if now is not None else time.time()
        sql = """
        UPDATE vertebra_queue_jobs
        SET scheduled_at = NULL
        WHERE status = 'pending' AND scheduled_at IS NOT NULL AND scheduled_at <= :1
        """
        res = await self._executor.execute(sql, current_time)
        await self._executor.commit()
        return res

    async def list_dlq(self, *, limit: int = 100) -> list[DeadLetterEntry]:
        """Fetch dead-letter entries from the DLQ table."""
        if self._store is not None:
            default_lane = getattr(self._config, "default_lane", "default")
            dead_entries = await self._store.list_entries(
                str(default_lane),
                status="dead_letter",
                limit=limit,
            )
            dlq_list: list[DeadLetterEntry] = []
            for de in dead_entries:
                try:
                    env = msgspec.convert(decode_json(de.payload), type=TaskEnvelope)
                except Exception:
                    env = TaskEnvelope(
                        task_id=de.job_id,
                        task_name=de.task_name,
                        lane=cast("TaskLane", de.queue),
                        attempt=de.attempt,
                        max_retries=de.max_attempts,
                    )
                dlq_list.append(
                    DeadLetterEntry(
                        dlq_id=de.job_id,
                        envelope=env,
                        error_type="DeadLetterError",
                        error_message="Job exceeded max retries",
                        failed_at=time.time(),
                    )
                )
            return dlq_list
        assert self._executor is not None
        rows = await self._executor.fetch_all(
            "SELECT * FROM vertebra_queue_dlq ORDER BY failed_at DESC FETCH FIRST :1 ROWS ONLY",
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
        """Re-enqueue task from dead-letter queue and commit."""
        if self._store is not None:
            entry = await self._store.get_entry(dlq_id)
            if entry is None:
                return None
            try:
                env = msgspec.convert(decode_json(entry.payload), type=TaskEnvelope)
            except Exception:
                env = TaskEnvelope(
                    task_id=entry.job_id,
                    task_name=entry.task_name,
                    lane=cast("TaskLane", entry.queue),
                    attempt=1,
                    max_retries=entry.max_attempts,
                )
            replayed = msgspec.structs.replace(
                env,
                attempt=1,
                scheduled_at=None,
                receipt_handle=None,
            )
            return await self.enqueue(replayed)
        assert self._executor is not None
        row = await self._executor.fetch_one(
            "SELECT * FROM vertebra_queue_dlq WHERE dlq_id = :1",
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
        await self._executor.execute("DELETE FROM vertebra_queue_dlq WHERE dlq_id = :1", dlq_id)
        await self._executor.commit()
        return await self.enqueue(replayed)

    async def acquire_lease(
        self,
        lease_key: str,
        owner_id: str,
        ttl_seconds: float,
        *,
        slot: str | None = None,
    ) -> CronLease | None:
        """Acquire lease via MERGE INTO vertebra_queue_leases."""
        if self._store is not None:
            ok = await self._store.acquire_lease(lease_key, owner_id=owner_id, ttl_seconds=ttl_seconds)
            if ok:
                return CronLease(
                    lease_key=lease_key,
                    owner_id=owner_id,
                    expires_at=time.time() + ttl_seconds,
                    last_slot=slot,
                )
            return None
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
        row = await self._executor.fetch_one(
            sql,
            lease_key,
            owner_id,
            expires_at,
            slot,
            now,
        )
        await self._executor.commit()
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
        """Release underlying executor and stores."""
        if self._store is not None:
            await self._store.aclose()
        if self._sync_store is not None:
            self._sync_store.close()
        if self._executor is not None:
            await self._executor.close()
