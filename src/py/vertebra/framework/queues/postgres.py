"""PostgreSQL queue backend with SKIP LOCKED, zero-poll LISTEN/NOTIFY, and DLQ persistence."""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import msgspec

from vertebra.db.adapters.postgres._async import AsyncPostgreSQLDriver
from vertebra.db.adapters.postgres._sync import SyncPostgreSQLDriver
from vertebra.db.adapters.postgres.config import PostgreSQLConfig
from vertebra.db.adapters.postgres.notify import (
    AsyncPostgresNotificationSource,
    SyncPostgresNotificationSource,
)
from vertebra.db.adapters.postgres.queue import (
    AsyncPostgresQueueStore,
    SyncPostgresQueueStore,
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


class PostgreSQLQueueBackend(_SqlQueueBackendBase):
    """PostgreSQL queue backend using FOR UPDATE SKIP LOCKED and LISTEN/NOTIFY over native stores or an executor."""

    __slots__ = ("_config", "_direct_dsn", "_executor", "_postgres_config", "_store", "_sync_store")

    _sync_store_label = "SyncPostgresQueueStore"

    def __init__(
        self,
        config: QueueConfig | QueueBackendSettings | None = None,
        *,
        postgres_config: PostgreSQLConfig | None = None,
        driver: AsyncPostgreSQLDriver | None = None,
        sync_driver: SyncPostgreSQLDriver | None = None,
        executor: PostgreSQLQueueExecutor | None = None,
        store: AsyncPostgresQueueStore | None = None,
        sync_store: SyncPostgresQueueStore | None = None,
    ) -> None:
        """Bind the queue backend to native queue stores, drivers, or an injected PostgreSQLQueueExecutor."""
        store, sync_store, driver, sync_driver = unpack_queue_settings(config, store, sync_store, driver, sync_driver)
        self._config = config or QueueConfig()
        self._postgres_config = postgres_config
        self._direct_dsn = postgres_config.effective_direct_dsn if postgres_config else self._config.backend_url
        if executor is not None:
            self._executor: PostgreSQLQueueExecutor | None = executor
            self._store: AsyncPostgresQueueStore | None = store
            self._sync_store: SyncPostgresQueueStore | None = sync_store
        elif store is not None or sync_store is not None:
            self._store = store
            self._sync_store = sync_store
            self._executor = None
        elif driver is not None or sync_driver is not None:
            self._store = (
                AsyncPostgresQueueStore(driver, notification_source=AsyncPostgresNotificationSource(driver))
                if driver is not None
                else None
            )
            self._sync_store = (
                SyncPostgresQueueStore(sync_driver, notification_source=SyncPostgresNotificationSource(sync_driver))
                if sync_driver is not None
                else None
            )
            self._executor = None
        else:
            ensure_module("vertebra._postgres", package="vertebra-core[postgres]", extra="postgres")
            dsn = self._direct_dsn
            if not dsn or dsn in ("postgres://", "postgresql://"):
                raise ImproperConfigurationError(
                    "PostgreSQLQueueBackend requires a configured PostgreSQL DSN, store, or executor"
                )
            resolved_cfg = self._postgres_config or PostgreSQLConfig(dsn=dsn)
            async_drv = AsyncPostgreSQLDriver(resolved_cfg)
            sync_drv = SyncPostgreSQLDriver(resolved_cfg)
            self._store = AsyncPostgresQueueStore(
                async_drv, notification_source=AsyncPostgresNotificationSource(async_drv)
            )
            self._sync_store = SyncPostgresQueueStore(
                sync_drv, notification_source=SyncPostgresNotificationSource(sync_drv)
            )
            self._executor = None

    @property
    def direct_dsn(self) -> str:
        """Return direct primary connection DSN used for LISTEN/NOTIFY."""
        return self._direct_dsn

    @property
    def store(self) -> AsyncPostgresQueueStore | None:
        """Return the active asynchronous native queue store, if configured."""
        return self._store

    @property
    def sync_store(self) -> SyncPostgresQueueStore | None:
        """Return the active synchronous native queue store, if configured."""
        return self._sync_store

    async def enqueue(self, envelope: TaskEnvelope) -> TaskEnvelope:
        """Insert job row into database and emit pg_notify wakeup."""
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
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (idempotency_key) DO NOTHING;
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
        await self._executor.notify("vertebra_queue_wakeup", envelope.lane)
        return envelope

    async def dequeue(
        self, lanes: Sequence[TaskLane], *, batch_size: int = 1, timeout_seconds: float = 0.0
    ) -> list[TaskEnvelope]:
        """Claim tasks via FOR UPDATE SKIP LOCKED, with adaptive LISTEN/NOTIFY wakeup."""
        if self._store is not None:
            return await self._store_dequeue(lanes, batch_size=batch_size, timeout_seconds=timeout_seconds)
        assert self._executor is not None
        now = time.time()
        handle = uuid.uuid4().hex
        lane_list = list(lanes)
        rows = await self._executor.fetch_all(
            CLAIM_POSTGRES_JOBS_SQL,
            lane_list,
            now,
            batch_size,
            now + self._config.visibility_timeout_seconds,
            self._config.consumer_name,
            handle,
        )
        if not rows and timeout_seconds > 0:
            min_row = await self._executor.fetch_one(
                "SELECT MIN(scheduled_at) AS min_scheduled FROM vertebra_queue_jobs WHERE lane = ANY($1) AND status = 'pending' AND scheduled_at > $2",
                lane_list,
                now,
            )
            min_sched = min_row.get("min_scheduled") if min_row else None
            effective_wait = min(timeout_seconds, self._config.poll_interval_seconds)
            if min_sched is not None:
                effective_wait = min(effective_wait, max(0.05, float(min_sched) - now))
            await self._executor.wait_for_notify("vertebra_queue_wakeup", effective_wait)
            now_retry = time.time()
            rows = await self._executor.fetch_all(
                CLAIM_POSTGRES_JOBS_SQL,
                lane_list,
                now_retry,
                batch_size,
                now_retry + self._config.visibility_timeout_seconds,
                self._config.consumer_name,
                handle,
            )
        return rows_to_envelopes(rows)

    async def ack(self, envelope: TaskEnvelope) -> None:
        """Delete completed job row matching task_id and receipt_handle."""
        if self._store is not None:
            await self._store.ack(envelope.task_id, attempt=envelope.attempt)
            return
        assert self._executor is not None
        await self._executor.execute("DELETE FROM vertebra_queue_jobs WHERE task_id = $1", envelope.task_id)

    async def nack(
        self,
        envelope: TaskEnvelope,
        *,
        error: Exception | str,
        delay_seconds: float | None = None,
        send_to_dlq: bool = False,
    ) -> DeadLetterEntry | None:
        """Route to DLQ or reschedule with exponential backoff."""
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
            await self._executor.execute("DELETE FROM vertebra_queue_jobs WHERE task_id = $1", envelope.task_id)
            return DeadLetterEntry(
                dlq_id=dlq_id, envelope=envelope, error_type=err_type, error_message=err_msg, failed_at=now
            )
        sched_at = now + (delay_seconds if delay_seconds is not None else 1.0)
        await self._executor.execute(
            "UPDATE vertebra_queue_jobs SET status = $1, attempt = $2, scheduled_at = $3, "
            "locked_until = $4, receipt_handle = $5 WHERE task_id = $6",
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
        if self._store is not None:
            return await self._store.promote_scheduled(limit=batch_size)
        assert self._executor is not None
        return await self._executor.execute(
            "UPDATE vertebra_queue_jobs SET scheduled_at = NULL "
            "WHERE status = 'pending' AND scheduled_at IS NOT NULL AND scheduled_at <= $1",
            now if now is not None else time.time(),
        )

    async def list_dlq(self, *, limit: int = 100) -> list[DeadLetterEntry]:
        """Fetch dead-letter entries from the DLQ table."""
        if self._store is not None:
            return await self._store_list_dlq(limit)
        assert self._executor is not None
        rows = await self._executor.fetch_all(
            "SELECT * FROM vertebra_queue_dlq ORDER BY failed_at DESC LIMIT $1", limit
        )
        return rows_to_dlq_entries(rows)

    async def replay_dlq(self, dlq_id: str) -> TaskEnvelope | None:
        """Replay dead-letter job by re-enqueuing into jobs table and deleting from DLQ."""
        if self._store is not None:
            return await self._store_replay_dlq(dlq_id)
        assert self._executor is not None
        row = await self._executor.fetch_one("SELECT * FROM vertebra_queue_dlq WHERE dlq_id = $1", dlq_id)
        if not row:
            return None
        replayed = msgspec.structs.replace(
            decode_envelope_bytes(row["payload"]), attempt=1, scheduled_at=None, receipt_handle=None
        )
        await self._executor.execute("DELETE FROM vertebra_queue_dlq WHERE dlq_id = $1", dlq_id)
        return await self.enqueue(replayed)

    async def acquire_lease(
        self, lease_key: str, owner_id: str, ttl_seconds: float, *, slot: str | None = None
    ) -> CronLease | None:
        """Acquire lease via atomic upsert conditional on expiration or matching owner."""
        if self._store is not None:
            return await self._store_acquire_lease(lease_key, owner_id, ttl_seconds, slot=slot)
        assert self._executor is not None
        now = time.time()
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
        row = await self._executor.fetch_one(sql, lease_key, owner_id, now + ttl_seconds, slot, now)
        if not row:
            return None
        return CronLease(
            lease_key=row["lease_key"],
            owner_id=row["owner_id"],
            expires_at=row["expires_at"],
            last_slot=row.get("last_slot"),
        )
