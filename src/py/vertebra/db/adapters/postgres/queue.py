"""PostgreSQL-backed synchronous and asynchronous queue stores using SKIP LOCKED."""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING, Any, cast

from vertebra.db.driver.capabilities import AsyncQueueStore, QueueEntry, SyncQueueStore
from vertebra.utils.serializers import decode_json, encode_json
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from collections.abc import Sequence

    from vertebra.db.adapters.postgres._async import AsyncPostgreSQLDriver
    from vertebra.db.adapters.postgres._sync import SyncPostgreSQLDriver
    from vertebra.db.adapters.postgres.notify import (
        AsyncPostgresNotificationSource,
        SyncPostgresNotificationSource,
    )

__all__ = (
    "AsyncPostgresQueueStore",
    "SyncPostgresQueueStore",
)


def _serialize_headers(headers: dict[str, str] | None) -> str | None:
    """Encode dictionary headers to JSON string."""
    if not headers:
        return None
    return encode_json(headers)


def _parse_headers(val: Any) -> dict[str, str]:
    """Decode raw database headers value into a string-keyed dictionary."""
    if val is None:
        return {}
    if isinstance(val, dict):
        return {str(k): str(v) for k, v in val.items()}
    if isinstance(val, (bytes, bytearray, memoryview)):
        return cast("dict[str, str]", decode_json(bytes(val)))
    if isinstance(val, str):
        return cast("dict[str, str]", decode_json(val))
    return {}


def _row_to_entry(row: dict[str, Any]) -> QueueEntry:
    """Convert a database result dictionary into a QueueEntry."""
    payload = row["payload"]
    if isinstance(payload, (bytes, bytearray, memoryview)):
        payload_bytes = bytes(payload)
    elif isinstance(payload, str):
        payload_bytes = payload.encode("utf-8")
    else:
        payload_bytes = bytes(payload)

    sched = row.get("scheduled_at_ms")
    return QueueEntry(
        job_id=str(row["job_id"]),
        queue=str(row["queue"]),
        task_name=str(row["task_name"]),
        payload=payload_bytes,
        attempt=int(row["attempt"]),
        max_attempts=int(row["max_attempts"]),
        lease_until_ms=int(row.get("lease_until_ms", 0)),
        scheduled_at_ms=int(sched) if sched is not None else None,
        headers=_parse_headers(row.get("headers")),
    )


class SyncPostgresQueueStore(SyncQueueStore):
    """Synchronous PostgreSQL queue store backed by SKIP LOCKED and atomic CTE updates."""

    __slots__ = ("_driver", "_notification_source", "_table_name")

    def __init__(
        self,
        driver: SyncPostgreSQLDriver,
        *,
        notification_source: SyncPostgresNotificationSource | None = None,
        table_name: str = "vertebra_queue_jobs",
    ) -> None:
        """Initialize the queue store with a synchronous driver and optional notification publisher."""
        self._driver = driver
        self._notification_source = notification_source
        self._table_name = validate_identifier(table_name, "table_name")

    def ensure_schema(self) -> None:
        """Idempotently create queue, lease, and uniqueness tables and indexes."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            job_id VARCHAR(64) PRIMARY KEY,
            queue VARCHAR(128) NOT NULL,
            task_name VARCHAR(256) NOT NULL,
            payload BYTEA NOT NULL,
            attempt INT NOT NULL DEFAULT 1,
            max_attempts INT NOT NULL DEFAULT 3,
            status VARCHAR(32) NOT NULL DEFAULT 'ready',
            scheduled_at_ms BIGINT,
            lease_until_ms BIGINT NOT NULL DEFAULT 0,
            worker_id VARCHAR(128),
            headers TEXT,
            last_error TEXT,
            created_at_ms BIGINT NOT NULL,
            updated_at_ms BIGINT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_{self._table_name}_claim
            ON {self._table_name} (queue, status, scheduled_at_ms, lease_until_ms);
        CREATE INDEX IF NOT EXISTS idx_{self._table_name}_stale
            ON {self._table_name} (status, lease_until_ms);
        CREATE TABLE IF NOT EXISTS vertebra_queue_leases (
            lease_key VARCHAR(256) PRIMARY KEY,
            owner_id VARCHAR(128) NOT NULL,
            expires_at_ms BIGINT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS vertebra_queue_unique_keys (
            unique_key VARCHAR(256) PRIMARY KEY,
            expires_at_ms BIGINT NOT NULL
        );
        """
        self._driver.execute(ddl)

    def enqueue(self, entry: QueueEntry) -> str:
        """Enqueue a single queue entry and emit a compact notification wakeup."""
        jid = entry.job_id or uuid.uuid4().hex
        now_ms = int(time.time() * 1000)
        sched_ms = entry.scheduled_at_ms
        sql = f"""
        INSERT INTO {self._table_name} (
            job_id, queue, task_name, payload, attempt, max_attempts,
            status, scheduled_at_ms, lease_until_ms, headers,
            created_at_ms, updated_at_ms
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        """
        self._driver.execute(
            sql,
            (
                jid,
                entry.queue,
                entry.task_name,
                entry.payload,
                entry.attempt,
                entry.max_attempts,
                "ready",
                sched_ms,
                entry.lease_until_ms,
                _serialize_headers(entry.headers),
                now_ms,
                now_ms,
            ),
        )
        if self._notification_source is not None:
            self._notification_source.publish(f"vtb:queue:{entry.queue}", f"wakeup:{jid}")
        return jid

    def enqueue_many(self, entries: Sequence[QueueEntry]) -> list[str]:
        """Bulk-enqueue job entries and return assigned identifiers."""
        return [self.enqueue(e) for e in entries]

    def claim(
        self,
        queues: Sequence[str],
        *,
        limit: int = 1,
        worker_id: str,
        lease_seconds: float = 30.0,
    ) -> list[QueueEntry]:
        """Atomically claim up to limit ready jobs under a visibility lease via CTE."""
        if not queues:
            return []
        now_ms = int(time.time() * 1000)
        lease_until_ms = now_ms + int(lease_seconds * 1000)
        sql = f"""
        WITH next_jobs AS (
            SELECT q.job_id
            FROM {self._table_name} q
            WHERE q.queue = ANY($1)
              AND (
                  (q.status = 'ready' AND (q.scheduled_at_ms IS NULL OR q.scheduled_at_ms <= $2))
                  OR (q.status = 'claimed' AND q.lease_until_ms <= $2 AND q.attempt < q.max_attempts)
              )
            ORDER BY q.created_at_ms ASC
            LIMIT $3
            FOR UPDATE OF q SKIP LOCKED
        )
        UPDATE {self._table_name} u
        SET status = 'claimed',
            worker_id = $4,
            lease_until_ms = $5,
            attempt = CASE WHEN u.status = 'claimed' THEN u.attempt + 1 ELSE u.attempt END,
            updated_at_ms = $2
        FROM next_jobs
        WHERE u.job_id = next_jobs.job_id
        RETURNING u.job_id, u.queue, u.task_name, u.payload, u.attempt, u.max_attempts,
                  u.lease_until_ms, u.scheduled_at_ms, u.headers
        """
        rows = self._driver.select(sql, (list(queues), now_ms, limit, worker_id, lease_until_ms))
        return [_row_to_entry(r) for r in rows]

    def claim_job(
        self,
        job_id: str,
        *,
        worker_id: str,
        lease_seconds: float = 30.0,
        expected_attempt: int | None = None,
    ) -> QueueEntry | None:
        """Atomically claim a specific ready job by job_id under a visibility lease."""
        now_ms = int(time.time() * 1000)
        lease_until_ms = now_ms + int(lease_seconds * 1000)
        sql = f"""
        UPDATE {self._table_name} u
        SET status = 'claimed',
            worker_id = $3,
            lease_until_ms = $4,
            attempt = CASE WHEN u.status = 'claimed' THEN u.attempt + 1 ELSE u.attempt END,
            updated_at_ms = $2
        WHERE u.job_id = $1
          AND (
              (u.status = 'ready' AND (u.scheduled_at_ms IS NULL OR u.scheduled_at_ms <= $2))
              OR (u.status = 'claimed' AND u.lease_until_ms <= $2 AND u.attempt < u.max_attempts)
          )
          AND (
              $5::INT IS NULL
              OR (CASE WHEN u.status = 'claimed' THEN u.attempt + 1 ELSE u.attempt END) = $5
          )
        RETURNING u.job_id, u.queue, u.task_name, u.payload, u.attempt, u.max_attempts,
                  u.lease_until_ms, u.scheduled_at_ms, u.headers
        """
        rows = self._driver.select(sql, (job_id, now_ms, worker_id, lease_until_ms, expected_attempt))
        return _row_to_entry(rows[0]) if rows else None

    def ack(self, job_id: str, *, attempt: int) -> bool:
        """Mark a fenced (job_id, attempt) claim as completed."""
        now_ms = int(time.time() * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET status = 'completed', lease_until_ms = 0, worker_id = NULL, updated_at_ms = $3
        WHERE job_id = $1 AND attempt = $2 AND status = 'claimed'
        """
        affected = self._driver.execute(sql, (job_id, attempt, now_ms))
        return affected > 0

    def nack(
        self,
        job_id: str,
        *,
        attempt: int,
        error: str,
        retry_delay_s: float | None = None,
    ) -> bool:
        """Record a failed (job_id, attempt) execution and schedule retry or dead-letter transition."""
        now_ms = int(time.time() * 1000)
        entry = self.get_entry(job_id)
        if entry is None or entry.attempt != attempt:
            return False

        if entry.attempt >= entry.max_attempts or retry_delay_s is None:
            sql = f"""
            UPDATE {self._table_name}
            SET status = 'dead_letter', last_error = $3, lease_until_ms = 0, worker_id = NULL, updated_at_ms = $4
            WHERE job_id = $1 AND attempt = $2 AND status = 'claimed'
            """
            affected = self._driver.execute(sql, (job_id, attempt, error, now_ms))
            return affected > 0

        retry_sched_ms = now_ms + int(retry_delay_s * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET status = 'ready',
            attempt = attempt + 1,
            scheduled_at_ms = $3,
            last_error = $4,
            lease_until_ms = 0,
            worker_id = NULL,
            updated_at_ms = $5
        WHERE job_id = $1 AND attempt = $2 AND status = 'claimed'
        """
        affected = self._driver.execute(sql, (job_id, attempt, retry_sched_ms, error, now_ms))
        return affected > 0

    def heartbeat(
        self,
        job_ids: Sequence[str],
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> int:
        """Extend the visibility lease for active (job_ids, worker_id) claims."""
        if not job_ids:
            return 0
        now_ms = int(time.time() * 1000)
        new_lease_ms = now_ms + int(lease_seconds * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET lease_until_ms = $1, updated_at_ms = $2
        WHERE job_id = ANY($3) AND worker_id = $4 AND status = 'claimed'
        """
        return self._driver.execute(sql, (new_lease_ms, now_ms, list(job_ids), worker_id))

    def reap_stale(self, *, lease_seconds: float = 300.0) -> int:
        """Reap abandoned in-flight claims by bumping attempt or transitioning exhausted jobs to dead_letter."""
        _ = lease_seconds
        now_ms = int(time.time() * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET status = CASE WHEN attempt >= max_attempts THEN 'dead_letter' ELSE 'ready' END,
            attempt = CASE WHEN attempt >= max_attempts THEN attempt ELSE attempt + 1 END,
            last_error = CASE
                WHEN attempt >= max_attempts THEN COALESCE(last_error, 'Visibility lease expired')
                ELSE last_error
            END,
            worker_id = NULL,
            lease_until_ms = 0,
            updated_at_ms = $1
        WHERE status = 'claimed' AND lease_until_ms <= $1
        """
        return self._driver.execute(sql, (now_ms,))

    def promote_scheduled(self, *, limit: int = 100) -> int:
        """Promote due scheduled jobs into ready status."""
        now_ms = int(time.time() * 1000)
        sql = f"""
        WITH due_jobs AS (
            SELECT job_id
            FROM {self._table_name}
            WHERE status = 'ready' AND scheduled_at_ms IS NOT NULL AND scheduled_at_ms <= $1
            LIMIT $2
            FOR UPDATE SKIP LOCKED
        )
        UPDATE {self._table_name} u
        SET scheduled_at_ms = NULL, updated_at_ms = $1
        FROM due_jobs
        WHERE u.job_id = due_jobs.job_id
        """
        return self._driver.execute(sql, (now_ms, limit))

    def cancel(self, job_id: str) -> bool:
        """Cancel an enqueued or scheduled job."""
        now_ms = int(time.time() * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET status = 'cancelled', lease_until_ms = 0, worker_id = NULL, updated_at_ms = $2
        WHERE job_id = $1 AND status IN ('ready', 'claimed')
        """
        affected = self._driver.execute(sql, (job_id, now_ms))
        return affected > 0

    def get_entry(self, job_id: str) -> QueueEntry | None:
        """Retrieve a specific queue entry by job_id."""
        sql = f"""
        SELECT job_id, queue, task_name, payload, attempt, max_attempts,
               lease_until_ms, scheduled_at_ms, headers
        FROM {self._table_name}
        WHERE job_id = $1
        """
        row = self._driver.select_one_or_none(sql, (job_id,))
        return _row_to_entry(row) if row is not None else None

    def get_entry_status(self, job_id: str) -> str | None:
        """Retrieve the current status string of job_id, or None if absent."""
        sql = f"SELECT status FROM {self._table_name} WHERE job_id = $1"
        row = self._driver.select_one_or_none(sql, (job_id,))
        if row is None or row.get("status") is None:
            return None
        return str(row["status"])

    def pop_dead_letter(self, job_id: str) -> QueueEntry | None:
        """Atomically remove and return a dead-lettered queue entry by job_id."""
        sql = f"""
        DELETE FROM {self._table_name}
        WHERE job_id = $1 AND status = 'dead_letter'
        RETURNING job_id, queue, task_name, payload, attempt, max_attempts,
                  lease_until_ms, scheduled_at_ms, headers
        """
        rows = self._driver.select(sql, (job_id,))
        return _row_to_entry(rows[0]) if rows else None

    def list_entries(
        self,
        queue: str,
        *,
        status: str | None = None,
        limit: int = 100,
    ) -> list[QueueEntry]:
        """List queue entries matching optional status up to limit."""
        if status is not None:
            sql = f"""
            SELECT job_id, queue, task_name, payload, attempt, max_attempts,
                   lease_until_ms, scheduled_at_ms, headers
            FROM {self._table_name}
            WHERE queue = $1 AND status = $2
            ORDER BY created_at_ms ASC
            LIMIT $3
            """
            rows = self._driver.select(sql, (queue, status, limit))
        else:
            sql = f"""
            SELECT job_id, queue, task_name, payload, attempt, max_attempts,
                   lease_until_ms, scheduled_at_ms, headers
            FROM {self._table_name}
            WHERE queue = $1
            ORDER BY created_at_ms ASC
            LIMIT $2
            """
            rows = self._driver.select(sql, (queue, limit))
        return [_row_to_entry(r) for r in rows]

    def next_due_delay(self, queues: Sequence[str]) -> float | None:
        """Return the delay in seconds until the next scheduled job is due, or None."""
        if not queues:
            return None
        now_ms = int(time.time() * 1000)
        sql = f"""
        SELECT MIN(scheduled_at_ms) AS min_sched
        FROM {self._table_name}
        WHERE queue = ANY($1) AND status = 'ready' AND scheduled_at_ms > $2
        """
        row = self._driver.select_one_or_none(sql, (list(queues), now_ms))
        if row is None or row.get("min_sched") is None:
            return None
        min_sched = int(row["min_sched"])
        return max(0.0, (min_sched - now_ms) / 1000.0)

    def reserve_unique_key(self, key: str, *, ttl_seconds: float) -> bool:
        """Reserve an idempotency key for ttl_seconds."""
        now_ms = int(time.time() * 1000)
        expires_at_ms = now_ms + int(ttl_seconds * 1000)
        sql = """
        INSERT INTO vertebra_queue_unique_keys (unique_key, expires_at_ms)
        VALUES ($1, $2)
        ON CONFLICT (unique_key) DO UPDATE
        SET expires_at_ms = EXCLUDED.expires_at_ms
        WHERE vertebra_queue_unique_keys.expires_at_ms <= $3
        RETURNING unique_key;
        """
        rows = self._driver.select(sql, (key, expires_at_ms, now_ms))
        return len(rows) > 0

    def release_unique_key(self, key: str) -> bool:
        """Release an idempotency key reservation."""
        sql = "DELETE FROM vertebra_queue_unique_keys WHERE unique_key = $1"
        return self._driver.execute(sql, (key,)) > 0

    def acquire_lease(self, lease_key: str, *, owner_id: str, ttl_seconds: float) -> bool:
        """Attempt to acquire a distributed leader lease for a cron or coordination slot."""
        now_ms = int(time.time() * 1000)
        expires_at_ms = now_ms + int(ttl_seconds * 1000)
        sql = """
        INSERT INTO vertebra_queue_leases (lease_key, owner_id, expires_at_ms)
        VALUES ($1, $2, $3)
        ON CONFLICT (lease_key) DO UPDATE
        SET owner_id = EXCLUDED.owner_id, expires_at_ms = EXCLUDED.expires_at_ms
        WHERE vertebra_queue_leases.expires_at_ms <= $4 OR vertebra_queue_leases.owner_id = EXCLUDED.owner_id
        RETURNING lease_key;
        """
        rows = self._driver.select(sql, (lease_key, owner_id, expires_at_ms, now_ms))
        return len(rows) > 0

    def release_lease(self, lease_key: str, *, owner_id: str) -> bool:
        """Release a held distributed leader lease."""
        sql = "DELETE FROM vertebra_queue_leases WHERE lease_key = $1 AND owner_id = $2"
        return self._driver.execute(sql, (lease_key, owner_id)) > 0

    def close(self) -> None:
        """Close backing driver resources if owned."""
        self._driver.close()


class AsyncPostgresQueueStore(AsyncQueueStore):
    """Asynchronous PostgreSQL queue store backed by SKIP LOCKED and atomic CTE updates."""

    __slots__ = ("_driver", "_notification_source", "_table_name")

    def __init__(
        self,
        driver: AsyncPostgreSQLDriver,
        *,
        notification_source: AsyncPostgresNotificationSource | None = None,
        table_name: str = "vertebra_queue_jobs",
    ) -> None:
        """Initialize the asynchronous queue store with a driver and optional notification publisher."""
        self._driver = driver
        self._notification_source = notification_source
        self._table_name = validate_identifier(table_name, "table_name")

    async def ensure_schema(self) -> None:
        """Idempotently create queue, lease, and uniqueness tables and indexes."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            job_id VARCHAR(64) PRIMARY KEY,
            queue VARCHAR(128) NOT NULL,
            task_name VARCHAR(256) NOT NULL,
            payload BYTEA NOT NULL,
            attempt INT NOT NULL DEFAULT 1,
            max_attempts INT NOT NULL DEFAULT 3,
            status VARCHAR(32) NOT NULL DEFAULT 'ready',
            scheduled_at_ms BIGINT,
            lease_until_ms BIGINT NOT NULL DEFAULT 0,
            worker_id VARCHAR(128),
            headers TEXT,
            last_error TEXT,
            created_at_ms BIGINT NOT NULL,
            updated_at_ms BIGINT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_{self._table_name}_claim
            ON {self._table_name} (queue, status, scheduled_at_ms, lease_until_ms);
        CREATE INDEX IF NOT EXISTS idx_{self._table_name}_stale
            ON {self._table_name} (status, lease_until_ms);
        CREATE TABLE IF NOT EXISTS vertebra_queue_leases (
            lease_key VARCHAR(256) PRIMARY KEY,
            owner_id VARCHAR(128) NOT NULL,
            expires_at_ms BIGINT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS vertebra_queue_unique_keys (
            unique_key VARCHAR(256) PRIMARY KEY,
            expires_at_ms BIGINT NOT NULL
        );
        """
        await self._driver.execute(ddl)

    async def enqueue(self, entry: QueueEntry) -> str:
        """Enqueue a single queue entry asynchronously and emit a compact notification wakeup."""
        jid = entry.job_id or uuid.uuid4().hex
        now_ms = int(time.time() * 1000)
        sched_ms = entry.scheduled_at_ms
        sql = f"""
        INSERT INTO {self._table_name} (
            job_id, queue, task_name, payload, attempt, max_attempts,
            status, scheduled_at_ms, lease_until_ms, headers,
            created_at_ms, updated_at_ms
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        """
        await self._driver.execute(
            sql,
            (
                jid,
                entry.queue,
                entry.task_name,
                entry.payload,
                entry.attempt,
                entry.max_attempts,
                "ready",
                sched_ms,
                entry.lease_until_ms,
                _serialize_headers(entry.headers),
                now_ms,
                now_ms,
            ),
        )
        if self._notification_source is not None:
            await self._notification_source.publish(f"vtb:queue:{entry.queue}", f"wakeup:{jid}")
        return jid

    async def enqueue_many(self, entries: Sequence[QueueEntry]) -> list[str]:
        """Bulk-enqueue job entries asynchronously and return assigned identifiers."""
        result: list[str] = []
        for e in entries:
            res = await self.enqueue(e)
            result.append(res)
        return result

    async def claim(
        self,
        queues: Sequence[str],
        *,
        limit: int = 1,
        worker_id: str,
        lease_seconds: float = 30.0,
    ) -> list[QueueEntry]:
        """Atomically claim up to limit ready jobs asynchronously under a visibility lease via CTE."""
        if not queues:
            return []
        now_ms = int(time.time() * 1000)
        lease_until_ms = now_ms + int(lease_seconds * 1000)
        sql = f"""
        WITH next_jobs AS (
            SELECT q.job_id
            FROM {self._table_name} q
            WHERE q.queue = ANY($1)
              AND (
                  (q.status = 'ready' AND (q.scheduled_at_ms IS NULL OR q.scheduled_at_ms <= $2))
                  OR (q.status = 'claimed' AND q.lease_until_ms <= $2 AND q.attempt < q.max_attempts)
              )
            ORDER BY q.created_at_ms ASC
            LIMIT $3
            FOR UPDATE OF q SKIP LOCKED
        )
        UPDATE {self._table_name} u
        SET status = 'claimed',
            worker_id = $4,
            lease_until_ms = $5,
            attempt = CASE WHEN u.status = 'claimed' THEN u.attempt + 1 ELSE u.attempt END,
            updated_at_ms = $2
        FROM next_jobs
        WHERE u.job_id = next_jobs.job_id
        RETURNING u.job_id, u.queue, u.task_name, u.payload, u.attempt, u.max_attempts,
                  u.lease_until_ms, u.scheduled_at_ms, u.headers
        """
        rows = await self._driver.select(sql, (list(queues), now_ms, limit, worker_id, lease_until_ms))
        return [_row_to_entry(r) for r in rows]

    async def claim_job(
        self,
        job_id: str,
        *,
        worker_id: str,
        lease_seconds: float = 30.0,
        expected_attempt: int | None = None,
    ) -> QueueEntry | None:
        """Atomically claim a specific ready job asynchronously by job_id under a visibility lease."""
        now_ms = int(time.time() * 1000)
        lease_until_ms = now_ms + int(lease_seconds * 1000)
        sql = f"""
        UPDATE {self._table_name} u
        SET status = 'claimed',
            worker_id = $3,
            lease_until_ms = $4,
            attempt = CASE WHEN u.status = 'claimed' THEN u.attempt + 1 ELSE u.attempt END,
            updated_at_ms = $2
        WHERE u.job_id = $1
          AND (
              (u.status = 'ready' AND (u.scheduled_at_ms IS NULL OR u.scheduled_at_ms <= $2))
              OR (u.status = 'claimed' AND u.lease_until_ms <= $2 AND u.attempt < u.max_attempts)
          )
          AND (
              $5::INT IS NULL
              OR (CASE WHEN u.status = 'claimed' THEN u.attempt + 1 ELSE u.attempt END) = $5
          )
        RETURNING u.job_id, u.queue, u.task_name, u.payload, u.attempt, u.max_attempts,
                  u.lease_until_ms, u.scheduled_at_ms, u.headers
        """
        rows = await self._driver.select(sql, (job_id, now_ms, worker_id, lease_until_ms, expected_attempt))
        return _row_to_entry(rows[0]) if rows else None

    async def ack(self, job_id: str, *, attempt: int) -> bool:
        """Mark a fenced (job_id, attempt) claim as completed asynchronously."""
        now_ms = int(time.time() * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET status = 'completed', lease_until_ms = 0, worker_id = NULL, updated_at_ms = $3
        WHERE job_id = $1 AND attempt = $2 AND status = 'claimed'
        """
        affected = await self._driver.execute(sql, (job_id, attempt, now_ms))
        return affected > 0

    async def nack(
        self,
        job_id: str,
        *,
        attempt: int,
        error: str,
        retry_delay_s: float | None = None,
    ) -> bool:
        """Record a failed (job_id, attempt) execution asynchronously and schedule retry or dead-lettering."""
        now_ms = int(time.time() * 1000)
        entry = await self.get_entry(job_id)
        if entry is None or entry.attempt != attempt:
            return False

        if entry.attempt >= entry.max_attempts or retry_delay_s is None:
            sql = f"""
            UPDATE {self._table_name}
            SET status = 'dead_letter', last_error = $3, lease_until_ms = 0, worker_id = NULL, updated_at_ms = $4
            WHERE job_id = $1 AND attempt = $2 AND status = 'claimed'
            """
            affected = await self._driver.execute(sql, (job_id, attempt, error, now_ms))
            return affected > 0

        retry_sched_ms = now_ms + int(retry_delay_s * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET status = 'ready',
            attempt = attempt + 1,
            scheduled_at_ms = $3,
            last_error = $4,
            lease_until_ms = 0,
            worker_id = NULL,
            updated_at_ms = $5
        WHERE job_id = $1 AND attempt = $2 AND status = 'claimed'
        """
        affected = await self._driver.execute(sql, (job_id, attempt, retry_sched_ms, error, now_ms))
        return affected > 0

    async def heartbeat(
        self,
        job_ids: Sequence[str],
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> int:
        """Extend the visibility lease for active (job_ids, worker_id) claims asynchronously."""
        if not job_ids:
            return 0
        now_ms = int(time.time() * 1000)
        new_lease_ms = now_ms + int(lease_seconds * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET lease_until_ms = $1, updated_at_ms = $2
        WHERE job_id = ANY($3) AND worker_id = $4 AND status = 'claimed'
        """
        return await self._driver.execute(sql, (new_lease_ms, now_ms, list(job_ids), worker_id))

    async def reap_stale(self, *, lease_seconds: float = 300.0) -> int:
        """Reap abandoned in-flight claims asynchronously by bumping attempt or dead-lettering."""
        _ = lease_seconds
        now_ms = int(time.time() * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET status = CASE WHEN attempt >= max_attempts THEN 'dead_letter' ELSE 'ready' END,
            attempt = CASE WHEN attempt >= max_attempts THEN attempt ELSE attempt + 1 END,
            last_error = CASE
                WHEN attempt >= max_attempts THEN COALESCE(last_error, 'Visibility lease expired')
                ELSE last_error
            END,
            worker_id = NULL,
            lease_until_ms = 0,
            updated_at_ms = $1
        WHERE status = 'claimed' AND lease_until_ms <= $1
        """
        return await self._driver.execute(sql, (now_ms,))

    async def promote_scheduled(self, *, limit: int = 100) -> int:
        """Promote due scheduled jobs into ready status asynchronously."""
        now_ms = int(time.time() * 1000)
        sql = f"""
        WITH due_jobs AS (
            SELECT job_id
            FROM {self._table_name}
            WHERE status = 'ready' AND scheduled_at_ms IS NOT NULL AND scheduled_at_ms <= $1
            LIMIT $2
            FOR UPDATE SKIP LOCKED
        )
        UPDATE {self._table_name} u
        SET scheduled_at_ms = NULL, updated_at_ms = $1
        FROM due_jobs
        WHERE u.job_id = due_jobs.job_id
        """
        return await self._driver.execute(sql, (now_ms, limit))

    async def cancel(self, job_id: str) -> bool:
        """Cancel an enqueued or scheduled job asynchronously."""
        now_ms = int(time.time() * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET status = 'cancelled', lease_until_ms = 0, worker_id = NULL, updated_at_ms = $2
        WHERE job_id = $1 AND status IN ('ready', 'claimed')
        """
        affected = await self._driver.execute(sql, (job_id, now_ms))
        return affected > 0

    async def get_entry(self, job_id: str) -> QueueEntry | None:
        """Retrieve a specific queue entry by job_id asynchronously."""
        sql = f"""
        SELECT job_id, queue, task_name, payload, attempt, max_attempts,
               lease_until_ms, scheduled_at_ms, headers
        FROM {self._table_name}
        WHERE job_id = $1
        """
        row = await self._driver.select_one_or_none(sql, (job_id,))
        return _row_to_entry(row) if row is not None else None

    async def get_entry_status(self, job_id: str) -> str | None:
        """Retrieve the current status string of job_id asynchronously, or None if absent."""
        sql = f"SELECT status FROM {self._table_name} WHERE job_id = $1"
        row = await self._driver.select_one_or_none(sql, (job_id,))
        if row is None or row.get("status") is None:
            return None
        return str(row["status"])

    async def pop_dead_letter(self, job_id: str) -> QueueEntry | None:
        """Atomically remove and return a dead-lettered queue entry asynchronously by job_id."""
        sql = f"""
        DELETE FROM {self._table_name}
        WHERE job_id = $1 AND status = 'dead_letter'
        RETURNING job_id, queue, task_name, payload, attempt, max_attempts,
                  lease_until_ms, scheduled_at_ms, headers
        """
        rows = await self._driver.select(sql, (job_id,))
        return _row_to_entry(rows[0]) if rows else None

    async def list_entries(
        self,
        queue: str,
        *,
        status: str | None = None,
        limit: int = 100,
    ) -> list[QueueEntry]:
        """List queue entries matching optional status up to limit asynchronously."""
        if status is not None:
            sql = f"""
            SELECT job_id, queue, task_name, payload, attempt, max_attempts,
                   lease_until_ms, scheduled_at_ms, headers
            FROM {self._table_name}
            WHERE queue = $1 AND status = $2
            ORDER BY created_at_ms ASC
            LIMIT $3
            """
            rows = await self._driver.select(sql, (queue, status, limit))
        else:
            sql = f"""
            SELECT job_id, queue, task_name, payload, attempt, max_attempts,
                   lease_until_ms, scheduled_at_ms, headers
            FROM {self._table_name}
            WHERE queue = $1
            ORDER BY created_at_ms ASC
            LIMIT $2
            """
            rows = await self._driver.select(sql, (queue, limit))
        return [_row_to_entry(r) for r in rows]

    async def next_due_delay(self, queues: Sequence[str]) -> float | None:
        """Return the delay in seconds until the next scheduled job is due, or None."""
        if not queues:
            return None
        now_ms = int(time.time() * 1000)
        sql = f"""
        SELECT MIN(scheduled_at_ms) AS min_sched
        FROM {self._table_name}
        WHERE queue = ANY($1) AND status = 'ready' AND scheduled_at_ms > $2
        """
        row = await self._driver.select_one_or_none(sql, (list(queues), now_ms))
        if row is None or row.get("min_sched") is None:
            return None
        min_sched = int(row["min_sched"])
        return max(0.0, (min_sched - now_ms) / 1000.0)

    async def reserve_unique_key(self, key: str, *, ttl_seconds: float) -> bool:
        """Reserve an idempotency key asynchronously for ttl_seconds."""
        now_ms = int(time.time() * 1000)
        expires_at_ms = now_ms + int(ttl_seconds * 1000)
        sql = """
        INSERT INTO vertebra_queue_unique_keys (unique_key, expires_at_ms)
        VALUES ($1, $2)
        ON CONFLICT (unique_key) DO UPDATE
        SET expires_at_ms = EXCLUDED.expires_at_ms
        WHERE vertebra_queue_unique_keys.expires_at_ms <= $3
        RETURNING unique_key;
        """
        rows = await self._driver.select(sql, (key, expires_at_ms, now_ms))
        return len(rows) > 0

    async def release_unique_key(self, key: str) -> bool:
        """Release an idempotency key reservation asynchronously."""
        sql = "DELETE FROM vertebra_queue_unique_keys WHERE unique_key = $1"
        return await self._driver.execute(sql, (key,)) > 0

    async def acquire_lease(self, lease_key: str, *, owner_id: str, ttl_seconds: float) -> bool:
        """Attempt to acquire a distributed leader lease asynchronously."""
        now_ms = int(time.time() * 1000)
        expires_at_ms = now_ms + int(ttl_seconds * 1000)
        sql = """
        INSERT INTO vertebra_queue_leases (lease_key, owner_id, expires_at_ms)
        VALUES ($1, $2, $3)
        ON CONFLICT (lease_key) DO UPDATE
        SET owner_id = EXCLUDED.owner_id, expires_at_ms = EXCLUDED.expires_at_ms
        WHERE vertebra_queue_leases.expires_at_ms <= $4 OR vertebra_queue_leases.owner_id = EXCLUDED.owner_id
        RETURNING lease_key;
        """
        rows = await self._driver.select(sql, (lease_key, owner_id, expires_at_ms, now_ms))
        return len(rows) > 0

    async def release_lease(self, lease_key: str, *, owner_id: str) -> bool:
        """Release a held distributed leader lease asynchronously."""
        sql = "DELETE FROM vertebra_queue_leases WHERE lease_key = $1 AND owner_id = $2"
        return await self._driver.execute(sql, (lease_key, owner_id)) > 0

    async def aclose(self) -> None:
        """Close backing driver resources asynchronously."""
        await self._driver.close()
