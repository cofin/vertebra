"""PostgreSQL-backed synchronous and asynchronous queue stores using SKIP LOCKED."""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING, Any, cast

from vertebra.db.driver.capabilities import AsyncQueueStore, QueueEntry, QueueReservation, SyncQueueStore
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

_ENTRY_SELECT_COLUMNS = (
    "job_id, queue, task_name, payload, attempt, max_attempts, "
    "lease_until_ms, scheduled_at_ms, headers, priority, expires_at_ms, "
    "completed_at_ms, worker_id, heartbeat_at_ms, result, error, last_error"
)

_QUALIFIED_ENTRY_SELECT_COLUMNS = (
    "u.job_id, u.queue, u.task_name, u.payload, u.attempt, u.max_attempts, "
    "u.lease_until_ms, u.scheduled_at_ms, u.headers, u.priority, u.expires_at_ms, "
    "u.completed_at_ms, u.worker_id, u.heartbeat_at_ms, u.result, u.error, u.last_error"
)

_JOINED_ENTRY_SELECT_COLUMNS = (
    "j.job_id, j.queue, j.task_name, j.payload, j.attempt, j.max_attempts, "
    "j.lease_until_ms, j.scheduled_at_ms, j.headers, j.priority, j.expires_at_ms, "
    "j.completed_at_ms, j.worker_id, j.heartbeat_at_ms, j.result, j.error, j.last_error"
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


def _to_optional_bytes(val: Any) -> bytes | None:
    """Convert raw BYTEA or string value to optional bytes."""
    if val is None:
        return None
    if isinstance(val, (bytes, bytearray, memoryview)):
        return bytes(val)
    if isinstance(val, str):
        return val.encode("utf-8")
    return bytes(val)


def _to_optional_int(val: Any) -> int | None:
    """Convert raw numeric database value to optional int."""
    return int(val) if val is not None else None


def _validate_positive_limit(limit: int | None) -> None:
    """Reject zero or negative limit arguments before issuing SQL."""
    if limit is not None and limit <= 0:
        msg = f"limit must be a positive integer, got {limit!r}"
        raise ValueError(msg)


def _row_to_entry(row: dict[str, Any]) -> QueueEntry:
    """Convert a database result dictionary into a QueueEntry."""
    payload_bytes = _to_optional_bytes(row["payload"])
    if payload_bytes is None:
        payload_bytes = b""
    error_val = row.get("error")
    if error_val is None:
        error_val = row.get("last_error")
    return QueueEntry(
        job_id=str(row["job_id"]),
        queue=str(row["queue"]),
        task_name=str(row["task_name"]),
        payload=payload_bytes,
        attempt=int(row["attempt"]),
        max_attempts=int(row["max_attempts"]),
        lease_until_ms=int(row.get("lease_until_ms") or 0),
        scheduled_at_ms=_to_optional_int(row.get("scheduled_at_ms")),
        headers=_parse_headers(row.get("headers")),
        priority=int(row.get("priority") or 0),
        expires_at_ms=_to_optional_int(row.get("expires_at_ms")),
        completed_at_ms=_to_optional_int(row.get("completed_at_ms")),
        worker_id=str(row["worker_id"]) if row.get("worker_id") is not None else None,
        heartbeat_at_ms=_to_optional_int(row.get("heartbeat_at_ms")),
        result=_to_optional_bytes(row.get("result")),
        error=str(error_val) if error_val is not None else None,
    )


def _row_to_reservation(row: dict[str, Any]) -> QueueReservation:
    """Convert a database result dictionary into a QueueReservation."""
    return QueueReservation(
        key=str(row["unique_key"]),
        job_id=str(row.get("job_id") or ""),
        task_name=str(row.get("task_name") or ""),
        created_at_ms=int(row.get("created_at_ms") or 0),
    )


def _build_schema_ddl(table_name: str) -> str:
    """Build idempotent DDL statements creating and migrating PostgreSQL queue tables."""
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
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
        priority INT NOT NULL DEFAULT 0,
        expires_at_ms BIGINT,
        completed_at_ms BIGINT,
        heartbeat_at_ms BIGINT,
        result BYTEA,
        error TEXT,
        created_at_ms BIGINT NOT NULL,
        updated_at_ms BIGINT NOT NULL
    );
    ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS priority INT NOT NULL DEFAULT 0;
    ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS expires_at_ms BIGINT;
    ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS completed_at_ms BIGINT;
    ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS heartbeat_at_ms BIGINT;
    ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS result BYTEA;
    ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS error TEXT;
    CREATE INDEX IF NOT EXISTS idx_{table_name}_claim
        ON {table_name} (queue, status, scheduled_at_ms, lease_until_ms);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_stale
        ON {table_name} (status, lease_until_ms);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_expires
        ON {table_name} (status, expires_at_ms);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_terminal
        ON {table_name} (status, completed_at_ms);
    CREATE TABLE IF NOT EXISTS vertebra_queue_leases (
        lease_key VARCHAR(256) PRIMARY KEY,
        owner_id VARCHAR(128) NOT NULL,
        expires_at_ms BIGINT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS vertebra_queue_unique_keys (
        unique_key VARCHAR(256) PRIMARY KEY,
        expires_at_ms BIGINT,
        job_id VARCHAR(64),
        task_name VARCHAR(256),
        created_at_ms BIGINT
    );
    ALTER TABLE vertebra_queue_unique_keys ALTER COLUMN expires_at_ms DROP NOT NULL;
    ALTER TABLE vertebra_queue_unique_keys ADD COLUMN IF NOT EXISTS job_id VARCHAR(64);
    ALTER TABLE vertebra_queue_unique_keys ADD COLUMN IF NOT EXISTS task_name VARCHAR(256);
    ALTER TABLE vertebra_queue_unique_keys ADD COLUMN IF NOT EXISTS created_at_ms BIGINT;
    """


def _build_enqueue_sql_and_params(
    table_name: str,
    entry: QueueEntry,
    job_id: str,
    now_ms: int,
) -> tuple[str, tuple[Any, ...]]:
    """Build INSERT SQL and positional parameters for enqueueing a QueueEntry."""
    sql = f"""
    INSERT INTO {table_name} (
        job_id, queue, task_name, payload, attempt, max_attempts,
        status, scheduled_at_ms, lease_until_ms, headers, priority,
        expires_at_ms, completed_at_ms, worker_id, heartbeat_at_ms,
        result, error, last_error, created_at_ms, updated_at_ms
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
    """
    params = (
        job_id,
        entry.queue,
        entry.task_name,
        entry.payload,
        entry.attempt,
        entry.max_attempts,
        "ready",
        entry.scheduled_at_ms,
        entry.lease_until_ms,
        _serialize_headers(entry.headers),
        entry.priority,
        entry.expires_at_ms,
        entry.completed_at_ms,
        entry.worker_id,
        entry.heartbeat_at_ms,
        entry.result,
        entry.error,
        entry.error,
        now_ms,
        now_ms,
    )
    return sql, params


def _build_list_entries_sql_and_params(
    table_name: str,
    queue: str | None,
    status: str | None,
    limit: int,
) -> tuple[str, tuple[Any, ...]]:
    """Build parameterized SELECT SQL for listing entries across optional queue and status."""
    _validate_positive_limit(limit)
    conditions: list[str] = []
    params: list[Any] = []
    if queue is not None:
        params.append(queue)
        conditions.append(f"queue = ${len(params)}")
    if status is not None:
        params.append(status)
        conditions.append(f"status = ${len(params)}")
    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params.append(limit)
    sql = f"""
    SELECT {_ENTRY_SELECT_COLUMNS}
    FROM {table_name}
    {where_clause}
    ORDER BY created_at_ms ASC
    LIMIT ${len(params)}
    """
    return sql, tuple(params)


def _build_cleanup_terminal_sql_and_params(
    table_name: str,
    before_ms: int,
    limit: int | None,
) -> tuple[str, tuple[Any, ...]]:
    """Build DELETE SQL for pruning oldest terminal job records completed before before_ms."""
    _validate_positive_limit(limit)
    if limit is not None:
        sql = f"""
        WITH doomed AS (
            SELECT job_id
            FROM {table_name}
            WHERE status IN ('completed', 'cancelled', 'dead_letter', 'expired')
              AND COALESCE(completed_at_ms, updated_at_ms) < $1
            ORDER BY COALESCE(completed_at_ms, updated_at_ms) ASC
            LIMIT $2
        )
        DELETE FROM {table_name} q
        USING doomed
        WHERE q.job_id = doomed.job_id
        """
        return sql, (before_ms, limit)
    sql = f"""
    DELETE FROM {table_name}
    WHERE status IN ('completed', 'cancelled', 'dead_letter', 'expired')
      AND COALESCE(completed_at_ms, updated_at_ms) < $1
    """
    return sql, (before_ms,)


def _build_get_entry_by_key_sql_and_params(
    table_name: str,
    key: str,
    queue: str | None,
) -> tuple[str, tuple[Any, ...]]:
    """Build explicit JOIN query locating a job by reservation key or idempotency header."""
    params: list[Any] = [key]
    queue_filter = ""
    if queue is not None:
        params.append(queue)
        queue_filter = f"AND j.queue = ${len(params)}"
    sql = f"""
    SELECT {_JOINED_ENTRY_SELECT_COLUMNS}
    FROM {table_name} j
    LEFT JOIN vertebra_queue_unique_keys u ON u.job_id = j.job_id
    WHERE (
        u.unique_key = $1
        OR (j.headers IS NOT NULL AND (j.headers::jsonb ->> 'key' = $1 OR j.headers::jsonb ->> 'idempotency_key' = $1))
    )
    {queue_filter}
    ORDER BY j.created_at_ms DESC
    LIMIT 1
    """
    return sql, tuple(params)


def _build_reserve_identity_sql_and_params(
    key: str,
    job_id: str,
    task_name: str,
    ttl_seconds: float | None,
    now_ms: int,
) -> tuple[str, tuple[Any, ...]]:
    """Build atomic UPSERT statement acquiring an identity reservation when absent or expired."""
    expires_at_ms = now_ms + int(ttl_seconds * 1000) if ttl_seconds is not None else None
    sql = """
    INSERT INTO vertebra_queue_unique_keys (unique_key, job_id, task_name, expires_at_ms, created_at_ms)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (unique_key) DO UPDATE
    SET job_id = EXCLUDED.job_id,
        task_name = EXCLUDED.task_name,
        expires_at_ms = EXCLUDED.expires_at_ms,
        created_at_ms = EXCLUDED.created_at_ms
    WHERE vertebra_queue_unique_keys.expires_at_ms IS NOT NULL
      AND vertebra_queue_unique_keys.expires_at_ms <= $5
    RETURNING unique_key
    """
    return sql, (key, job_id, task_name, expires_at_ms, now_ms)


def _build_reset_identity_sql_and_params(
    key: str,
    expected_job_id: str | None,
) -> tuple[str, tuple[Any, ...]]:
    """Build DELETE SQL for releasing an identity reservation with optional job fencing."""
    if expected_job_id is not None:
        return "DELETE FROM vertebra_queue_unique_keys WHERE unique_key = $1 AND job_id = $2", (
            key,
            expected_job_id,
        )
    return "DELETE FROM vertebra_queue_unique_keys WHERE unique_key = $1", (key,)


def _build_interrupt_task_sql_and_params(
    table_name: str,
    job_id: str,
    error: str | None,
    now_ms: int,
) -> tuple[str, tuple[Any, ...]]:
    """Build UPDATE SQL for interrupting a ready or claimed job with optional error detail."""
    if error is not None:
        sql = f"""
        UPDATE {table_name}
        SET status = 'cancelled',
            lease_until_ms = 0,
            worker_id = NULL,
            completed_at_ms = $2,
            updated_at_ms = $2,
            error = $3,
            last_error = $3
        WHERE job_id = $1 AND status IN ('ready', 'claimed')
        """
        return sql, (job_id, now_ms, error)
    sql = f"""
    UPDATE {table_name}
    SET status = 'cancelled',
        lease_until_ms = 0,
        worker_id = NULL,
        completed_at_ms = $2,
        updated_at_ms = $2
    WHERE job_id = $1 AND status IN ('ready', 'claimed')
    """
    return sql, (job_id, now_ms)


def _build_expire_overdue_sql_and_params(
    table_name: str,
    now_ms: int,
    limit: int | None,
) -> tuple[str, tuple[Any, ...]]:
    """Build UPDATE SQL for transitioning overdue jobs to expired status."""
    _validate_positive_limit(limit)
    if limit is not None:
        sql = f"""
        WITH overdue AS (
            SELECT job_id
            FROM {table_name}
            WHERE status IN ('ready', 'claimed')
              AND expires_at_ms IS NOT NULL
              AND expires_at_ms <= $1
            ORDER BY expires_at_ms ASC
            LIMIT $2
        )
        UPDATE {table_name} u
        SET status = 'expired',
            lease_until_ms = 0,
            worker_id = NULL,
            completed_at_ms = $1,
            updated_at_ms = $1
        FROM overdue
        WHERE u.job_id = overdue.job_id
        """
        return sql, (now_ms, limit)
    sql = f"""
    UPDATE {table_name}
    SET status = 'expired',
        lease_until_ms = 0,
        worker_id = NULL,
        completed_at_ms = $1,
        updated_at_ms = $1
    WHERE status IN ('ready', 'claimed')
      AND expires_at_ms IS NOT NULL
      AND expires_at_ms <= $1
    """
    return sql, (now_ms,)


class _BasePostgresQueueStore:
    """Shared SQL and parameter builders for synchronous and asynchronous PostgreSQL queue stores."""

    __slots__ = ("_driver", "_notification_source", "_table_name")

    _driver: Any
    _notification_source: Any
    _table_name: str

    def _init_store(self, driver: Any, notification_source: Any, table_name: str) -> None:
        self._driver = driver
        self._notification_source = notification_source
        self._table_name = validate_identifier(table_name, "table_name")

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    def _claim_sql(
        self, queues: Sequence[str], *, limit: int, worker_id: str, lease_seconds: float
    ) -> tuple[str, tuple[Any, ...]]:
        now_ms = self._now_ms()
        sql = f"""
        WITH next_jobs AS (
            SELECT q.job_id
            FROM {self._table_name} q
            WHERE q.queue = ANY($1)
              AND (
                  (q.status = 'ready' AND (q.scheduled_at_ms IS NULL OR q.scheduled_at_ms <= $2))
                  OR (q.status = 'claimed' AND q.lease_until_ms <= $2 AND q.attempt < q.max_attempts)
              )
            ORDER BY q.priority DESC, q.created_at_ms ASC
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
        RETURNING {_QUALIFIED_ENTRY_SELECT_COLUMNS}
        """
        return sql, (list(queues), now_ms, limit, worker_id, now_ms + int(lease_seconds * 1000))

    def _claim_job_sql(
        self, job_id: str, *, worker_id: str, lease_seconds: float, expected_attempt: int | None
    ) -> tuple[str, tuple[Any, ...]]:
        now_ms = self._now_ms()
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
        RETURNING {_QUALIFIED_ENTRY_SELECT_COLUMNS}
        """
        return sql, (job_id, now_ms, worker_id, now_ms + int(lease_seconds * 1000), expected_attempt)

    def _ack_sql(self, job_id: str, attempt: int) -> tuple[str, tuple[Any, ...]]:
        sql = (
            f"UPDATE {self._table_name} SET status = 'completed', lease_until_ms = 0, "
            "worker_id = NULL, completed_at_ms = $3, updated_at_ms = $3 "
            "WHERE job_id = $1 AND attempt = $2 AND status = 'claimed'"
        )
        return sql, (job_id, attempt, self._now_ms())

    def _nack_sql(
        self, entry: QueueEntry, *, job_id: str, attempt: int, error: str, retry_delay_s: float | None, now_ms: int
    ) -> tuple[str, tuple[Any, ...]]:
        if entry.attempt >= entry.max_attempts or retry_delay_s is None:
            sql = (
                f"UPDATE {self._table_name} SET status = 'dead_letter', error = $3, last_error = $3, "
                "lease_until_ms = 0, worker_id = NULL, completed_at_ms = $4, updated_at_ms = $4 "
                "WHERE job_id = $1 AND attempt = $2 AND status = 'claimed'"
            )
            return sql, (job_id, attempt, error, now_ms)
        sql = (
            f"UPDATE {self._table_name} SET status = 'ready', attempt = attempt + 1, scheduled_at_ms = $3, "
            "error = $4, last_error = $4, lease_until_ms = 0, worker_id = NULL, updated_at_ms = $5 "
            "WHERE job_id = $1 AND attempt = $2 AND status = 'claimed'"
        )
        return sql, (job_id, attempt, now_ms + int(retry_delay_s * 1000), error, now_ms)

    def _heartbeat_sql(
        self, job_ids: Sequence[str], *, worker_id: str, lease_seconds: float
    ) -> tuple[str, tuple[Any, ...]]:
        now_ms = self._now_ms()
        sql = (
            f"UPDATE {self._table_name} SET lease_until_ms = $1, heartbeat_at_ms = $2, updated_at_ms = $2 "
            "WHERE job_id = ANY($3) AND worker_id = $4 AND status = 'claimed'"
        )
        return sql, (now_ms + int(lease_seconds * 1000), now_ms, list(job_ids), worker_id)

    def _reap_stale_sql(self) -> tuple[str, tuple[int]]:
        sql = f"""
        UPDATE {self._table_name}
        SET status = CASE WHEN attempt >= max_attempts THEN 'dead_letter' ELSE 'ready' END,
            attempt = CASE WHEN attempt >= max_attempts THEN attempt ELSE attempt + 1 END,
            last_error = CASE
                WHEN attempt >= max_attempts THEN COALESCE(last_error, 'Visibility lease expired')
                ELSE last_error
            END,
            error = CASE
                WHEN attempt >= max_attempts THEN COALESCE(error, last_error, 'Visibility lease expired')
                ELSE error
            END,
            completed_at_ms = CASE WHEN attempt >= max_attempts THEN $1 ELSE completed_at_ms END,
            worker_id = NULL,
            lease_until_ms = 0,
            updated_at_ms = $1
        WHERE status = 'claimed' AND lease_until_ms <= $1
        """
        return sql, (self._now_ms(),)

    def _promote_scheduled_sql(self, limit: int) -> tuple[str, tuple[int, int]]:
        sql = f"""
        WITH due_jobs AS (
            SELECT job_id FROM {self._table_name}
            WHERE status = 'ready' AND scheduled_at_ms IS NOT NULL AND scheduled_at_ms <= $1
            LIMIT $2 FOR UPDATE SKIP LOCKED
        )
        UPDATE {self._table_name} u SET scheduled_at_ms = NULL, updated_at_ms = $1
        FROM due_jobs WHERE u.job_id = due_jobs.job_id
        """
        return sql, (self._now_ms(), limit)

    def _cancel_sql(self, job_id: str) -> tuple[str, tuple[str, int]]:
        sql = (
            f"UPDATE {self._table_name} SET status = 'cancelled', lease_until_ms = 0, worker_id = NULL, "
            "completed_at_ms = $2, updated_at_ms = $2 WHERE job_id = $1 AND status IN ('ready', 'claimed')"
        )
        return sql, (job_id, self._now_ms())

    def _get_entry_sql(self, job_id: str) -> tuple[str, tuple[str]]:
        return f"SELECT {_ENTRY_SELECT_COLUMNS} FROM {self._table_name} WHERE job_id = $1", (job_id,)

    def _pop_dead_letter_sql(self, job_id: str) -> tuple[str, tuple[str]]:
        return (
            f"DELETE FROM {self._table_name} WHERE job_id = $1 AND status = 'dead_letter' "
            f"RETURNING {_ENTRY_SELECT_COLUMNS}",
            (job_id,),
        )

    @staticmethod
    def _reserve_unique_key_sql(key: str, ttl_seconds: float) -> tuple[str, tuple[Any, ...]]:
        now_ms = int(time.time() * 1000)
        sql = """
        INSERT INTO vertebra_queue_unique_keys (unique_key, expires_at_ms) VALUES ($1, $2)
        ON CONFLICT (unique_key) DO UPDATE SET expires_at_ms = EXCLUDED.expires_at_ms
        WHERE vertebra_queue_unique_keys.expires_at_ms IS NOT NULL
          AND vertebra_queue_unique_keys.expires_at_ms <= $3
        RETURNING unique_key;
        """
        return sql, (key, now_ms + int(ttl_seconds * 1000), now_ms)

    @staticmethod
    def _acquire_lease_sql(lease_key: str, owner_id: str, ttl_seconds: float) -> tuple[str, tuple[Any, ...]]:
        now_ms = int(time.time() * 1000)
        sql = """
        INSERT INTO vertebra_queue_leases (lease_key, owner_id, expires_at_ms) VALUES ($1, $2, $3)
        ON CONFLICT (lease_key) DO UPDATE
        SET owner_id = EXCLUDED.owner_id, expires_at_ms = EXCLUDED.expires_at_ms
        WHERE vertebra_queue_leases.expires_at_ms <= $4 OR vertebra_queue_leases.owner_id = EXCLUDED.owner_id
        RETURNING lease_key;
        """
        return sql, (lease_key, owner_id, now_ms + int(ttl_seconds * 1000), now_ms)

    def _get_reservation_sql(self, key: str) -> tuple[str, tuple[str, int]]:
        sql = (
            "SELECT unique_key, job_id, task_name, created_at_ms FROM vertebra_queue_unique_keys "
            "WHERE unique_key = $1 AND (expires_at_ms IS NULL OR expires_at_ms > $2)"
        )
        return sql, (key, self._now_ms())


class SyncPostgresQueueStore(_BasePostgresQueueStore, SyncQueueStore):
    """Synchronous PostgreSQL queue store backed by SKIP LOCKED and atomic CTE updates."""

    __slots__ = ()

    def __init__(
        self,
        driver: SyncPostgreSQLDriver,
        *,
        notification_source: SyncPostgresNotificationSource | None = None,
        table_name: str = "vertebra_queue_jobs",
    ) -> None:
        """Initialize the queue store with a synchronous driver and optional notification publisher."""
        self._init_store(driver, notification_source, table_name)

    def ensure_schema(self) -> None:
        """Idempotently create queue, lease, and uniqueness tables and indexes."""
        self._driver.execute(_build_schema_ddl(self._table_name))

    def enqueue(self, entry: QueueEntry) -> str:
        """Enqueue a single queue entry and emit a compact notification wakeup."""
        jid = entry.job_id or uuid.uuid4().hex
        self._driver.execute(*_build_enqueue_sql_and_params(self._table_name, entry, jid, self._now_ms()))
        if self._notification_source is not None:
            self._notification_source.publish(f"vtb:queue:{entry.queue}", f"wakeup:{jid}")
        return jid

    def enqueue_many(self, entries: Sequence[QueueEntry]) -> list[str]:
        """Bulk-enqueue job entries and return assigned identifiers."""
        return [self.enqueue(e) for e in entries]

    def claim(
        self, queues: Sequence[str], *, limit: int = 1, worker_id: str, lease_seconds: float = 30.0
    ) -> list[QueueEntry]:
        """Atomically claim up to limit ready jobs under a visibility lease via CTE."""
        _validate_positive_limit(limit)
        if not queues:
            return []
        rows = self._driver.select(
            *self._claim_sql(queues, limit=limit, worker_id=worker_id, lease_seconds=lease_seconds)
        )
        return [_row_to_entry(r) for r in rows]

    def claim_job(
        self, job_id: str, *, worker_id: str, lease_seconds: float = 30.0, expected_attempt: int | None = None
    ) -> QueueEntry | None:
        """Atomically claim a specific ready job by job_id under a visibility lease."""
        rows = self._driver.select(
            *self._claim_job_sql(
                job_id, worker_id=worker_id, lease_seconds=lease_seconds, expected_attempt=expected_attempt
            )
        )
        return _row_to_entry(rows[0]) if rows else None

    def ack(self, job_id: str, *, attempt: int) -> bool:
        """Mark a fenced (job_id, attempt) claim as completed."""
        return bool(self._driver.execute(*self._ack_sql(job_id, attempt)) > 0)

    def nack(self, job_id: str, *, attempt: int, error: str, retry_delay_s: float | None = None) -> bool:
        """Record a failed (job_id, attempt) execution and schedule retry or dead-letter transition."""
        now_ms = self._now_ms()
        entry = self.get_entry(job_id)
        if entry is None or entry.attempt != attempt:
            return False
        sql, params = self._nack_sql(
            entry, job_id=job_id, attempt=attempt, error=error, retry_delay_s=retry_delay_s, now_ms=now_ms
        )
        return bool(self._driver.execute(sql, params) > 0)

    def heartbeat(self, job_ids: Sequence[str], *, worker_id: str, lease_seconds: float) -> int:
        """Extend the visibility lease for active (job_ids, worker_id) claims."""
        if not job_ids:
            return 0
        return int(
            self._driver.execute(*self._heartbeat_sql(job_ids, worker_id=worker_id, lease_seconds=lease_seconds))
        )

    def reap_stale(self, *, lease_seconds: float = 300.0) -> int:
        """Reap abandoned in-flight claims by bumping attempt or transitioning exhausted jobs to dead_letter."""
        _ = lease_seconds
        return int(self._driver.execute(*self._reap_stale_sql()))

    def promote_scheduled(self, *, limit: int = 100) -> int:
        """Promote due scheduled jobs into ready status."""
        _validate_positive_limit(limit)
        return int(self._driver.execute(*self._promote_scheduled_sql(limit)))

    def cancel(self, job_id: str) -> bool:
        """Cancel an enqueued or scheduled job."""
        return bool(self._driver.execute(*self._cancel_sql(job_id)) > 0)

    def get_entry(self, job_id: str) -> QueueEntry | None:
        """Retrieve a specific queue entry by job_id."""
        row = self._driver.select_one_or_none(*self._get_entry_sql(job_id))
        return _row_to_entry(row) if row is not None else None

    def get_entry_status(self, job_id: str) -> str | None:
        """Retrieve the current status string of job_id, or None if absent."""
        row = self._driver.select_one_or_none(f"SELECT status FROM {self._table_name} WHERE job_id = $1", (job_id,))
        return str(row["status"]) if row is not None and row.get("status") is not None else None

    def pop_dead_letter(self, job_id: str) -> QueueEntry | None:
        """Atomically remove and return a dead-lettered queue entry by job_id."""
        rows = self._driver.select(*self._pop_dead_letter_sql(job_id))
        return _row_to_entry(rows[0]) if rows else None

    def list_entries(
        self, queue: str | None = None, *, status: str | None = None, limit: int = 100
    ) -> list[QueueEntry]:
        """List queue entries matching optional queue and status up to limit."""
        return [
            _row_to_entry(r)
            for r in self._driver.select(*_build_list_entries_sql_and_params(self._table_name, queue, status, limit))
        ]

    def cleanup_terminal(self, *, before_ms: int, limit: int | None = None) -> int:
        """Delete oldest terminal queue records before before_ms while preserving forever reservations."""
        deleted = int(self._driver.execute(*_build_cleanup_terminal_sql_and_params(self._table_name, before_ms, limit)))
        self._driver.execute(
            "DELETE FROM vertebra_queue_unique_keys WHERE expires_at_ms IS NOT NULL AND expires_at_ms <= $1",
            (before_ms,),
        )
        return deleted

    def get_entry_by_key(self, key: str, *, queue: str | None = None) -> QueueEntry | None:
        """Retrieve a queue entry matching idempotency key and optional queue."""
        row = self._driver.select_one_or_none(*_build_get_entry_by_key_sql_and_params(self._table_name, key, queue))
        return _row_to_entry(row) if row is not None else None

    def reserve_identity(
        self, key: str, *, job_id: str, task_name: str, ttl_seconds: float | None = None
    ) -> QueueReservation | None:
        """Acquire a single-winner reservation, returning None when acquired or the existing owner."""
        rows = self._driver.select(
            *_build_reserve_identity_sql_and_params(key, job_id, task_name, ttl_seconds, self._now_ms())
        )
        return None if rows else self.get_reservation(key)

    def get_reservation(self, key: str) -> QueueReservation | None:
        """Retrieve an active identity reservation for key, or None."""
        row = self._driver.select_one_or_none(*self._get_reservation_sql(key))
        return _row_to_reservation(row) if row is not None else None

    def reset_identity(self, key: str, *, expected_job_id: str | None = None) -> bool:
        """Release an identity reservation, optionally fenced by expected_job_id."""
        return bool(self._driver.execute(*_build_reset_identity_sql_and_params(key, expected_job_id)) > 0)

    def assign_worker(self, job_id: str, *, worker_id: str, attempt: int) -> bool:
        """Record worker_id assignment for an active fenced claim."""
        sql = f"UPDATE {self._table_name} SET worker_id = $2, updated_at_ms = $4 WHERE job_id = $1 AND attempt = $3 AND status = 'claimed'"
        return bool(self._driver.execute(sql, (job_id, worker_id, attempt, self._now_ms())) > 0)

    def touch_heartbeats(self, job_ids: Sequence[str], *, heartbeat_at_ms: int) -> int:
        """Update heartbeat_at_ms for active claimed jobs and return updated count."""
        if not job_ids:
            return 0
        sql = f"UPDATE {self._table_name} SET heartbeat_at_ms = $1, updated_at_ms = $2 WHERE status = 'claimed' AND job_id = ANY($3)"
        return int(self._driver.execute(sql, (heartbeat_at_ms, self._now_ms(), list(job_ids))))

    def null_heartbeats(self, job_ids: Sequence[str]) -> int:
        """Clear heartbeat_at_ms for the given job_ids and return updated count."""
        if not job_ids:
            return 0
        sql = f"UPDATE {self._table_name} SET heartbeat_at_ms = NULL, updated_at_ms = $1 WHERE job_id = ANY($2)"
        return int(self._driver.execute(sql, (self._now_ms(), list(job_ids))))

    def interrupt_task(self, job_id: str, *, error: str | None = None) -> bool:
        """Interrupt a pending or running job with optional error detail."""
        return bool(
            self._driver.execute(*_build_interrupt_task_sql_and_params(self._table_name, job_id, error, self._now_ms()))
            > 0
        )

    def expire_overdue(self, *, now_ms: int, limit: int | None = None) -> int:
        """Mark overdue jobs with expires_at_ms <= now_ms as expired up to limit."""
        return int(self._driver.execute(*_build_expire_overdue_sql_and_params(self._table_name, now_ms, limit)))

    def next_due_delay(self, queues: Sequence[str]) -> float | None:
        """Return the delay in seconds until the next scheduled job is due, or None."""
        if not queues:
            return None
        now_ms = self._now_ms()
        sql = f"SELECT MIN(scheduled_at_ms) AS min_sched FROM {self._table_name} WHERE queue = ANY($1) AND status = 'ready' AND scheduled_at_ms > $2"
        row = self._driver.select_one_or_none(sql, (list(queues), now_ms))
        return (
            None if row is None or row.get("min_sched") is None else max(0.0, (int(row["min_sched"]) - now_ms) / 1000.0)
        )

    def reserve_unique_key(self, key: str, *, ttl_seconds: float) -> bool:
        """Reserve an idempotency key for ttl_seconds."""
        return len(self._driver.select(*self._reserve_unique_key_sql(key, ttl_seconds))) > 0

    def release_unique_key(self, key: str) -> bool:
        """Release an idempotency key reservation."""
        return bool(self._driver.execute("DELETE FROM vertebra_queue_unique_keys WHERE unique_key = $1", (key,)) > 0)

    def acquire_lease(self, lease_key: str, *, owner_id: str, ttl_seconds: float) -> bool:
        """Attempt to acquire a distributed leader lease for a cron or coordination slot."""
        return len(self._driver.select(*self._acquire_lease_sql(lease_key, owner_id, ttl_seconds))) > 0

    def release_lease(self, lease_key: str, *, owner_id: str) -> bool:
        """Release a held distributed leader lease."""
        return bool(
            self._driver.execute(
                "DELETE FROM vertebra_queue_leases WHERE lease_key = $1 AND owner_id = $2", (lease_key, owner_id)
            )
            > 0
        )

    def close(self) -> None:
        """Close backing driver resources if owned."""
        self._driver.close()


class AsyncPostgresQueueStore(_BasePostgresQueueStore, AsyncQueueStore):
    """Asynchronous PostgreSQL queue store backed by SKIP LOCKED and atomic CTE updates."""

    __slots__ = ()

    def __init__(
        self,
        driver: AsyncPostgreSQLDriver,
        *,
        notification_source: AsyncPostgresNotificationSource | None = None,
        table_name: str = "vertebra_queue_jobs",
    ) -> None:
        """Initialize the queue store with an asynchronous driver and optional notification publisher."""
        self._init_store(driver, notification_source, table_name)

    async def ensure_schema(self) -> None:
        """Idempotently create queue, lease, and uniqueness tables and indexes."""
        await self._driver.execute(_build_schema_ddl(self._table_name))

    async def enqueue(self, entry: QueueEntry) -> str:
        """Enqueue a single queue entry and emit a compact notification wakeup."""
        jid = entry.job_id or uuid.uuid4().hex
        await self._driver.execute(*_build_enqueue_sql_and_params(self._table_name, entry, jid, self._now_ms()))
        if self._notification_source is not None:
            await self._notification_source.publish(f"vtb:queue:{entry.queue}", f"wakeup:{jid}")
        return jid

    async def enqueue_many(self, entries: Sequence[QueueEntry]) -> list[str]:
        """Bulk-enqueue job entries and return assigned identifiers."""
        return [await self.enqueue(e) for e in entries]

    async def claim(
        self, queues: Sequence[str], *, limit: int = 1, worker_id: str, lease_seconds: float = 30.0
    ) -> list[QueueEntry]:
        """Atomically claim up to limit ready jobs under a visibility lease via CTE."""
        _validate_positive_limit(limit)
        if not queues:
            return []
        rows = await self._driver.select(
            *self._claim_sql(queues, limit=limit, worker_id=worker_id, lease_seconds=lease_seconds)
        )
        return [_row_to_entry(r) for r in rows]

    async def claim_job(
        self, job_id: str, *, worker_id: str, lease_seconds: float = 30.0, expected_attempt: int | None = None
    ) -> QueueEntry | None:
        """Atomically claim a specific ready job by job_id under a visibility lease."""
        rows = await self._driver.select(
            *self._claim_job_sql(
                job_id, worker_id=worker_id, lease_seconds=lease_seconds, expected_attempt=expected_attempt
            )
        )
        return _row_to_entry(rows[0]) if rows else None

    async def ack(self, job_id: str, *, attempt: int) -> bool:
        """Mark a fenced (job_id, attempt) claim as completed."""
        return bool((await self._driver.execute(*self._ack_sql(job_id, attempt))) > 0)

    async def nack(self, job_id: str, *, attempt: int, error: str, retry_delay_s: float | None = None) -> bool:
        """Record a failed (job_id, attempt) execution and schedule retry or dead-letter transition."""
        now_ms = self._now_ms()
        entry = await self.get_entry(job_id)
        if entry is None or entry.attempt != attempt:
            return False
        sql, params = self._nack_sql(
            entry, job_id=job_id, attempt=attempt, error=error, retry_delay_s=retry_delay_s, now_ms=now_ms
        )
        return bool((await self._driver.execute(sql, params)) > 0)

    async def heartbeat(self, job_ids: Sequence[str], *, worker_id: str, lease_seconds: float) -> int:
        """Extend the visibility lease for active (job_ids, worker_id) claims."""
        if not job_ids:
            return 0
        return int(
            await self._driver.execute(*self._heartbeat_sql(job_ids, worker_id=worker_id, lease_seconds=lease_seconds))
        )

    async def reap_stale(self, *, lease_seconds: float = 300.0) -> int:
        """Reap abandoned in-flight claims by bumping attempt or transitioning exhausted jobs to dead_letter."""
        _ = lease_seconds
        return int(await self._driver.execute(*self._reap_stale_sql()))

    async def promote_scheduled(self, *, limit: int = 100) -> int:
        """Promote due scheduled jobs into ready status."""
        _validate_positive_limit(limit)
        return int(await self._driver.execute(*self._promote_scheduled_sql(limit)))

    async def cancel(self, job_id: str) -> bool:
        """Cancel an enqueued or scheduled job."""
        return bool((await self._driver.execute(*self._cancel_sql(job_id))) > 0)

    async def get_entry(self, job_id: str) -> QueueEntry | None:
        """Retrieve a specific queue entry by job_id."""
        row = await self._driver.select_one_or_none(*self._get_entry_sql(job_id))
        return _row_to_entry(row) if row is not None else None

    async def get_entry_status(self, job_id: str) -> str | None:
        """Retrieve the current status string of job_id, or None if absent."""
        row = await self._driver.select_one_or_none(
            f"SELECT status FROM {self._table_name} WHERE job_id = $1", (job_id,)
        )
        return str(row["status"]) if row is not None and row.get("status") is not None else None

    async def pop_dead_letter(self, job_id: str) -> QueueEntry | None:
        """Atomically remove and return a dead-lettered queue entry by job_id."""
        rows = await self._driver.select(*self._pop_dead_letter_sql(job_id))
        return _row_to_entry(rows[0]) if rows else None

    async def list_entries(
        self, queue: str | None = None, *, status: str | None = None, limit: int = 100
    ) -> list[QueueEntry]:
        """List queue entries matching optional queue and status up to limit."""
        rows = await self._driver.select(*_build_list_entries_sql_and_params(self._table_name, queue, status, limit))
        return [_row_to_entry(r) for r in rows]

    async def cleanup_terminal(self, *, before_ms: int, limit: int | None = None) -> int:
        """Delete oldest terminal queue records before before_ms while preserving forever reservations."""
        deleted = int(
            await self._driver.execute(*_build_cleanup_terminal_sql_and_params(self._table_name, before_ms, limit))
        )
        await self._driver.execute(
            "DELETE FROM vertebra_queue_unique_keys WHERE expires_at_ms IS NOT NULL AND expires_at_ms <= $1",
            (before_ms,),
        )
        return deleted

    async def get_entry_by_key(self, key: str, *, queue: str | None = None) -> QueueEntry | None:
        """Retrieve a queue entry matching idempotency key and optional queue."""
        row = await self._driver.select_one_or_none(
            *_build_get_entry_by_key_sql_and_params(self._table_name, key, queue)
        )
        return _row_to_entry(row) if row is not None else None

    async def reserve_identity(
        self, key: str, *, job_id: str, task_name: str, ttl_seconds: float | None = None
    ) -> QueueReservation | None:
        """Acquire a single-winner reservation, returning None when acquired or the existing owner."""
        rows = await self._driver.select(
            *_build_reserve_identity_sql_and_params(key, job_id, task_name, ttl_seconds, self._now_ms())
        )
        return None if rows else await self.get_reservation(key)

    async def get_reservation(self, key: str) -> QueueReservation | None:
        """Retrieve an active identity reservation for key, or None."""
        row = await self._driver.select_one_or_none(*self._get_reservation_sql(key))
        return _row_to_reservation(row) if row is not None else None

    async def reset_identity(self, key: str, *, expected_job_id: str | None = None) -> bool:
        """Release an identity reservation, optionally fenced by expected_job_id."""
        return bool((await self._driver.execute(*_build_reset_identity_sql_and_params(key, expected_job_id))) > 0)

    async def assign_worker(self, job_id: str, *, worker_id: str, attempt: int) -> bool:
        """Record worker_id assignment for an active fenced claim."""
        sql = f"UPDATE {self._table_name} SET worker_id = $2, updated_at_ms = $4 WHERE job_id = $1 AND attempt = $3 AND status = 'claimed'"
        return bool((await self._driver.execute(sql, (job_id, worker_id, attempt, self._now_ms()))) > 0)

    async def touch_heartbeats(self, job_ids: Sequence[str], *, heartbeat_at_ms: int) -> int:
        """Update heartbeat_at_ms for active claimed jobs and return updated count."""
        if not job_ids:
            return 0
        sql = f"UPDATE {self._table_name} SET heartbeat_at_ms = $1, updated_at_ms = $2 WHERE status = 'claimed' AND job_id = ANY($3)"
        return int(await self._driver.execute(sql, (heartbeat_at_ms, self._now_ms(), list(job_ids))))

    async def null_heartbeats(self, job_ids: Sequence[str]) -> int:
        """Clear heartbeat_at_ms for the given job_ids and return updated count."""
        if not job_ids:
            return 0
        sql = f"UPDATE {self._table_name} SET heartbeat_at_ms = NULL, updated_at_ms = $1 WHERE job_id = ANY($2)"
        return int(await self._driver.execute(sql, (self._now_ms(), list(job_ids))))

    async def interrupt_task(self, job_id: str, *, error: str | None = None) -> bool:
        """Interrupt a pending or running job with optional error detail."""
        return bool(
            (
                await self._driver.execute(
                    *_build_interrupt_task_sql_and_params(self._table_name, job_id, error, self._now_ms())
                )
            )
            > 0
        )

    async def expire_overdue(self, *, now_ms: int, limit: int | None = None) -> int:
        """Mark overdue jobs with expires_at_ms <= now_ms as expired up to limit."""
        return int(await self._driver.execute(*_build_expire_overdue_sql_and_params(self._table_name, now_ms, limit)))

    async def next_due_delay(self, queues: Sequence[str]) -> float | None:
        """Return the delay in seconds until the next scheduled job is due, or None."""
        if not queues:
            return None
        now_ms = self._now_ms()
        sql = f"SELECT MIN(scheduled_at_ms) AS min_sched FROM {self._table_name} WHERE queue = ANY($1) AND status = 'ready' AND scheduled_at_ms > $2"
        row = await self._driver.select_one_or_none(sql, (list(queues), now_ms))
        return (
            None if row is None or row.get("min_sched") is None else max(0.0, (int(row["min_sched"]) - now_ms) / 1000.0)
        )

    async def reserve_unique_key(self, key: str, *, ttl_seconds: float) -> bool:
        """Reserve an idempotency key for ttl_seconds."""
        return len(await self._driver.select(*self._reserve_unique_key_sql(key, ttl_seconds))) > 0

    async def release_unique_key(self, key: str) -> bool:
        """Release an idempotency key reservation."""
        return bool(
            (await self._driver.execute("DELETE FROM vertebra_queue_unique_keys WHERE unique_key = $1", (key,))) > 0
        )

    async def acquire_lease(self, lease_key: str, *, owner_id: str, ttl_seconds: float) -> bool:
        """Attempt to acquire a distributed leader lease for a cron or coordination slot."""
        return len(await self._driver.select(*self._acquire_lease_sql(lease_key, owner_id, ttl_seconds))) > 0

    async def release_lease(self, lease_key: str, *, owner_id: str) -> bool:
        """Release a held distributed leader lease."""
        return bool(
            (
                await self._driver.execute(
                    "DELETE FROM vertebra_queue_leases WHERE lease_key = $1 AND owner_id = $2", (lease_key, owner_id)
                )
            )
            > 0
        )

    async def aclose(self) -> None:
        """Close backing driver resources if owned."""
        await self._driver.aclose()
