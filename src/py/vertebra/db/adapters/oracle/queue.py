"""Oracle 26ai-backed synchronous and asynchronous queue stores using FOR UPDATE SKIP LOCKED."""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING, Any, cast

from vertebra.db.driver.capabilities import AsyncQueueStore, QueueEntry, QueueReservation, SyncQueueStore
from vertebra.utils.serializers import decode_json, encode_json
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from collections.abc import Sequence

    from vertebra.db.adapters.oracle._async import AsyncOracleDriver
    from vertebra.db.adapters.oracle._sync import SyncOracleDriver
    from vertebra.db.adapters.oracle.notify import (
        AsyncOracleNotificationSource,
        SyncOracleNotificationSource,
    )

__all__ = (
    "AsyncOracleQueueStore",
    "SyncOracleQueueStore",
)

_ENTRY_SELECT_COLUMNS = (
    "job_id, queue, task_name, payload, attempt, max_attempts, status, "
    "lease_until_ms, scheduled_at_ms, headers, priority, expires_at_ms, "
    "completed_at_ms, worker_id, heartbeat_at_ms, result, error, last_error"
)

_JOINED_ENTRY_SELECT_COLUMNS = (
    "j.job_id, j.queue, j.task_name, j.payload, j.attempt, j.max_attempts, j.status, "
    "j.lease_until_ms, j.scheduled_at_ms, j.headers, j.priority, j.expires_at_ms, "
    "j.completed_at_ms, j.worker_id, j.heartbeat_at_ms, j.result, j.error, j.last_error"
)

_IGNORED_DDL_CODES = (
    "ORA-00955",
    "00955",
    "ORA-01408",
    "01408",
    "ORA-01430",
    "01430",
    "ORA-01442",
    "01442",
    "ORA-01451",
    "01451",
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
    """Convert raw BLOB or string value to optional bytes."""
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


def _row_to_entry(row: dict[str, Any]) -> QueueEntry:
    """Convert a database result dictionary into a QueueEntry."""
    lowered = {k.lower(): v for k, v in row.items()}
    payload_bytes = _to_optional_bytes(lowered["payload"])
    if payload_bytes is None:
        payload_bytes = b""
    error_val = lowered.get("error")
    if error_val is None:
        error_val = lowered.get("last_error")
    return QueueEntry(
        job_id=str(lowered["job_id"]),
        queue=str(lowered["queue"]),
        task_name=str(lowered["task_name"]),
        payload=payload_bytes,
        attempt=int(lowered["attempt"]),
        max_attempts=int(lowered["max_attempts"]),
        lease_until_ms=int(lowered.get("lease_until_ms") or 0),
        scheduled_at_ms=_to_optional_int(lowered.get("scheduled_at_ms")),
        headers=_parse_headers(lowered.get("headers")),
        priority=int(lowered.get("priority") or 0),
        expires_at_ms=_to_optional_int(lowered.get("expires_at_ms")),
        completed_at_ms=_to_optional_int(lowered.get("completed_at_ms")),
        worker_id=str(lowered["worker_id"]) if lowered.get("worker_id") is not None else None,
        heartbeat_at_ms=_to_optional_int(lowered.get("heartbeat_at_ms")),
        result=_to_optional_bytes(lowered.get("result")),
        error=str(error_val) if error_val is not None else None,
    )


def _row_to_reservation(row: dict[str, Any]) -> QueueReservation:
    """Convert a database result dictionary into a QueueReservation."""
    lowered = {k.lower(): v for k, v in row.items()}
    return QueueReservation(
        key=str(lowered["unique_key"]),
        job_id=str(lowered.get("job_id") or ""),
        task_name=str(lowered.get("task_name") or ""),
        created_at_ms=int(lowered.get("created_at_ms") or 0),
    )


def _schema_statements(table_name: str) -> tuple[str, ...]:
    """Build idempotent DDL creation and migration statements for Oracle queue storage."""
    return (
        f"""
        CREATE TABLE {table_name} (
            job_id VARCHAR2(64) PRIMARY KEY,
            queue VARCHAR2(128) NOT NULL,
            task_name VARCHAR2(256) NOT NULL,
            payload BLOB NOT NULL,
            attempt NUMBER(5) DEFAULT 1 NOT NULL,
            max_attempts NUMBER(5) DEFAULT 3 NOT NULL,
            status VARCHAR2(32) DEFAULT 'ready' NOT NULL,
            scheduled_at_ms NUMBER(19),
            lease_until_ms NUMBER(19) DEFAULT 0 NOT NULL,
            worker_id VARCHAR2(128),
            headers CLOB,
            last_error CLOB,
            priority NUMBER(10) DEFAULT 0 NOT NULL,
            expires_at_ms NUMBER(19),
            completed_at_ms NUMBER(19),
            heartbeat_at_ms NUMBER(19),
            result BLOB,
            error CLOB,
            created_at_ms NUMBER(19) NOT NULL,
            updated_at_ms NUMBER(19) NOT NULL
        )
        """,
        f"ALTER TABLE {table_name} ADD (priority NUMBER(10) DEFAULT 0 NOT NULL)",
        f"ALTER TABLE {table_name} ADD (expires_at_ms NUMBER(19))",
        f"ALTER TABLE {table_name} ADD (completed_at_ms NUMBER(19))",
        f"ALTER TABLE {table_name} ADD (heartbeat_at_ms NUMBER(19))",
        f"ALTER TABLE {table_name} ADD (result BLOB)",
        f"ALTER TABLE {table_name} ADD (error CLOB)",
        f"CREATE INDEX idx_{table_name}_claim ON {table_name} (queue, status, scheduled_at_ms, lease_until_ms)",
        f"CREATE INDEX idx_{table_name}_stale ON {table_name} (status, lease_until_ms)",
        f"CREATE INDEX idx_{table_name}_expires ON {table_name} (status, expires_at_ms)",
        f"CREATE INDEX idx_{table_name}_terminal ON {table_name} (status, completed_at_ms)",
        """
        CREATE TABLE vertebra_queue_leases (
            lease_key VARCHAR2(256) PRIMARY KEY,
            owner_id VARCHAR2(128) NOT NULL,
            expires_at_ms NUMBER(19) NOT NULL
        )
        """,
        """
        CREATE TABLE vertebra_queue_unique_keys (
            unique_key VARCHAR2(256) PRIMARY KEY,
            expires_at_ms NUMBER(19),
            job_id VARCHAR2(64),
            task_name VARCHAR2(256),
            created_at_ms NUMBER(19)
        )
        """,
        "ALTER TABLE vertebra_queue_unique_keys MODIFY (expires_at_ms NUMBER(19) NULL)",
        "ALTER TABLE vertebra_queue_unique_keys ADD (job_id VARCHAR2(64))",
        "ALTER TABLE vertebra_queue_unique_keys ADD (task_name VARCHAR2(256))",
        "ALTER TABLE vertebra_queue_unique_keys ADD (created_at_ms NUMBER(19))",
    )


def _is_ignored_ddl_error(exc: Exception) -> bool:
    """Return True when an Oracle DDL exception represents an already-applied schema state."""
    err_msg = str(exc)
    return any(code in err_msg for code in _IGNORED_DDL_CODES)


def _enqueue_sql_and_params(
    table_name: str,
    entry: QueueEntry,
    job_id: str,
    now_ms: int,
) -> tuple[str, dict[str, Any]]:
    """Build INSERT SQL and named bind parameters for enqueueing a QueueEntry."""
    sql = f"""
    INSERT INTO {table_name} (
        job_id, queue, task_name, payload, attempt, max_attempts,
        status, scheduled_at_ms, lease_until_ms, headers, priority,
        expires_at_ms, completed_at_ms, worker_id, heartbeat_at_ms,
        result, error, last_error, created_at_ms, updated_at_ms
    ) VALUES (
        :job_id, :queue, :task_name, :payload, :attempt, :max_attempts,
        :status, :scheduled_at_ms, :lease_until_ms, :headers, :priority,
        :expires_at_ms, :completed_at_ms, :worker_id, :heartbeat_at_ms,
        :result, :error, :last_error, :created_at_ms, :updated_at_ms
    )
    """
    params: dict[str, Any] = {
        "job_id": job_id,
        "queue": entry.queue,
        "task_name": entry.task_name,
        "payload": entry.payload,
        "attempt": entry.attempt,
        "max_attempts": entry.max_attempts,
        "status": "ready",
        "scheduled_at_ms": entry.scheduled_at_ms,
        "lease_until_ms": entry.lease_until_ms,
        "headers": _serialize_headers(entry.headers),
        "priority": entry.priority,
        "expires_at_ms": entry.expires_at_ms,
        "completed_at_ms": entry.completed_at_ms,
        "worker_id": entry.worker_id,
        "heartbeat_at_ms": entry.heartbeat_at_ms,
        "result": entry.result,
        "error": entry.error,
        "last_error": entry.error,
        "created_at_ms": now_ms,
        "updated_at_ms": now_ms,
    }
    return sql, params


def _list_entries_sql_and_params(
    table_name: str,
    queue: str | None,
    status: str | None,
    limit: int,
) -> tuple[str, dict[str, Any]]:
    """Build parameterized SELECT SQL for listing entries across optional queue and status."""
    conditions: list[str] = []
    params: dict[str, Any] = {}
    if queue is not None:
        conditions.append("queue = :queue")
        params["queue"] = queue
    if status is not None:
        conditions.append("status = :status")
        params["status"] = status
    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params["limit_val"] = limit
    sql = f"""
    SELECT {_ENTRY_SELECT_COLUMNS}
    FROM {table_name}
    {where_clause}
    ORDER BY created_at_ms ASC
    FETCH FIRST :limit_val ROWS ONLY
    """
    return sql, params


def _cleanup_terminal_sql_and_params(
    table_name: str,
    before_ms: int,
    limit: int | None,
) -> tuple[str, dict[str, Any]]:
    """Build DELETE SQL for pruning terminal job records completed before before_ms."""
    if limit is not None:
        sql = f"""
        DELETE FROM {table_name}
        WHERE job_id IN (
            SELECT job_id FROM {table_name}
            WHERE status IN ('completed', 'cancelled', 'dead_letter', 'expired')
              AND COALESCE(completed_at_ms, updated_at_ms) < :before_ms
            ORDER BY COALESCE(completed_at_ms, updated_at_ms) ASC
            FETCH FIRST :limit_val ROWS ONLY
        )
        """
        return sql, {"before_ms": before_ms, "limit_val": limit}
    sql = f"""
    DELETE FROM {table_name}
    WHERE status IN ('completed', 'cancelled', 'dead_letter', 'expired')
      AND COALESCE(completed_at_ms, updated_at_ms) < :before_ms
    """
    return sql, {"before_ms": before_ms}


def _get_entry_by_key_sql_and_params(
    table_name: str,
    key: str,
    queue: str | None,
) -> tuple[str, dict[str, Any]]:
    """Build explicit JOIN query locating a job by reservation key or idempotency header."""
    params: dict[str, Any] = {"key": key}
    queue_filter = ""
    if queue is not None:
        queue_filter = "AND j.queue = :queue"
        params["queue"] = queue
    sql = f"""
    SELECT {_JOINED_ENTRY_SELECT_COLUMNS}
    FROM {table_name} j
    LEFT JOIN vertebra_queue_unique_keys u ON u.job_id = j.job_id
    WHERE (
        u.unique_key = :key
        OR JSON_VALUE(j.headers, '$.key') = :key
        OR JSON_VALUE(j.headers, '$.idempotency_key') = :key
    )
    {queue_filter}
    ORDER BY j.created_at_ms DESC
    FETCH FIRST 1 ROWS ONLY
    """
    return sql, params


def _reserve_identity_sql_and_params(
    key: str,
    job_id: str,
    task_name: str,
    ttl_seconds: float | None,
    now_ms: int,
) -> tuple[str, dict[str, Any]]:
    """Build atomic MERGE statement acquiring an identity reservation when absent or expired."""
    expires_at_ms = now_ms + int(ttl_seconds * 1000) if ttl_seconds is not None else None
    sql = """
    MERGE INTO vertebra_queue_unique_keys u
    USING (
        SELECT :key AS k, :job_id AS jid, :task_name AS tname,
               :expires_at_ms AS exp, :now_ms AS created_ms
        FROM dual
    ) src
    ON (u.unique_key = src.k)
    WHEN MATCHED THEN
      UPDATE SET u.job_id = src.jid,
                 u.task_name = src.tname,
                 u.expires_at_ms = src.exp,
                 u.created_at_ms = src.created_ms
      WHERE u.expires_at_ms IS NOT NULL AND u.expires_at_ms <= :now_ms
    WHEN NOT MATCHED THEN
      INSERT (unique_key, job_id, task_name, expires_at_ms, created_at_ms)
      VALUES (src.k, src.jid, src.tname, src.exp, src.created_ms)
    """
    params: dict[str, Any] = {
        "key": key,
        "job_id": job_id,
        "task_name": task_name,
        "expires_at_ms": expires_at_ms,
        "now_ms": now_ms,
    }
    return sql, params


def _reset_identity_sql_and_params(
    key: str,
    expected_job_id: str | None,
) -> tuple[str, dict[str, Any]]:
    """Build DELETE SQL for releasing an identity reservation with optional job fencing."""
    if expected_job_id is not None:
        sql = "DELETE FROM vertebra_queue_unique_keys WHERE unique_key = :key AND job_id = :expected_job_id"
        return sql, {"key": key, "expected_job_id": expected_job_id}
    sql = "DELETE FROM vertebra_queue_unique_keys WHERE unique_key = :key"
    return sql, {"key": key}


def _heartbeat_update_sql_and_params(
    table_name: str,
    job_ids: Sequence[str],
    now_ms: int,
    *,
    heartbeat_at_ms: int | None,
) -> tuple[str, dict[str, Any]]:
    """Build UPDATE SQL for touching or clearing heartbeat_at_ms across job_ids."""
    in_clause = ", ".join(f":j{i}" for i in range(len(job_ids)))
    params: dict[str, Any] = {f"j{i}": jid for i, jid in enumerate(job_ids)}
    params["now_ms"] = now_ms
    if heartbeat_at_ms is not None:
        params["heartbeat_at_ms"] = heartbeat_at_ms
        sql = f"""
        UPDATE {table_name}
        SET heartbeat_at_ms = :heartbeat_at_ms, updated_at_ms = :now_ms
        WHERE status = 'claimed' AND job_id IN ({in_clause})
        """
    else:
        sql = f"""
        UPDATE {table_name}
        SET heartbeat_at_ms = NULL, updated_at_ms = :now_ms
        WHERE job_id IN ({in_clause})
        """
    return sql, params


def _interrupt_task_sql_and_params(
    table_name: str,
    job_id: str,
    error: str | None,
    now_ms: int,
) -> tuple[str, dict[str, Any]]:
    """Build UPDATE SQL for interrupting a ready or claimed job with optional error detail."""
    params: dict[str, Any] = {"job_id": job_id, "now_ms": now_ms}
    if error is not None:
        params["error"] = error
        error_set = ", error = :error, last_error = :error"
    else:
        error_set = ""
    sql = f"""
    UPDATE {table_name}
    SET status = 'cancelled',
        lease_until_ms = 0,
        worker_id = NULL,
        completed_at_ms = :now_ms,
        updated_at_ms = :now_ms{error_set}
    WHERE job_id = :job_id AND status IN ('ready', 'claimed')
    """
    return sql, params


def _expire_overdue_sql_and_params(
    table_name: str,
    now_ms: int,
    limit: int | None,
) -> tuple[str, dict[str, Any]]:
    """Build UPDATE SQL for transitioning overdue jobs to expired status."""
    if limit is not None:
        sql = f"""
        UPDATE {table_name}
        SET status = 'expired',
            lease_until_ms = 0,
            worker_id = NULL,
            completed_at_ms = :now_ms,
            updated_at_ms = :now_ms
        WHERE job_id IN (
            SELECT job_id FROM {table_name}
            WHERE status IN ('ready', 'claimed')
              AND expires_at_ms IS NOT NULL
              AND expires_at_ms <= :now_ms
            ORDER BY expires_at_ms ASC
            FETCH FIRST :limit_val ROWS ONLY
        )
        """
        return sql, {"now_ms": now_ms, "limit_val": limit}
    sql = f"""
    UPDATE {table_name}
    SET status = 'expired',
        lease_until_ms = 0,
        worker_id = NULL,
        completed_at_ms = :now_ms,
        updated_at_ms = :now_ms
    WHERE status IN ('ready', 'claimed')
      AND expires_at_ms IS NOT NULL
      AND expires_at_ms <= :now_ms
    """
    return sql, {"now_ms": now_ms}


class SyncOracleQueueStore(SyncQueueStore):
    """Synchronous Oracle queue store backed by FOR UPDATE SKIP LOCKED and MERGE leases."""

    __slots__ = ("_driver", "_notification_source", "_table_name")

    def __init__(
        self,
        driver: SyncOracleDriver,
        *,
        notification_source: SyncOracleNotificationSource | None = None,
        table_name: str = "vertebra_queue_jobs",
    ) -> None:
        """Initialize the queue store with a synchronous Oracle driver."""
        self._driver = driver
        self._notification_source = notification_source
        self._table_name = validate_identifier(table_name, "table_name")

    def ensure_schema(self) -> None:
        """Idempotently create queue, lease, and uniqueness tables and indexes."""
        for stmt in _schema_statements(self._table_name):
            try:
                self._driver.execute(stmt)
            except Exception as exc:
                if _is_ignored_ddl_error(exc):
                    continue
                raise

    def enqueue(self, entry: QueueEntry) -> str:
        """Enqueue a single queue entry and emit an optional TxEventQ notification wakeup."""
        jid = entry.job_id or uuid.uuid4().hex
        now_ms = int(time.time() * 1000)
        sql, params = _enqueue_sql_and_params(self._table_name, entry, jid, now_ms)
        self._driver.execute(sql, params)
        if self._notification_source is not None:
            self._notification_source.publish(entry.queue, f"wakeup:{jid}")
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
        """Atomically claim up to limit ready jobs under a visibility lease via FOR UPDATE SKIP LOCKED."""
        if not queues:
            return []
        now_ms = int(time.time() * 1000)
        lease_until_ms = now_ms + int(lease_seconds * 1000)

        in_clause = ", ".join(f":q{i}" for i in range(len(queues)))
        select_params: dict[str, Any] = {f"q{i}": q for i, q in enumerate(queues)}
        select_params["now_ms"] = now_ms
        select_params["limit_val"] = limit

        select_sql = f"""
        SELECT {_ENTRY_SELECT_COLUMNS}
        FROM {self._table_name}
        WHERE queue IN ({in_clause})
          AND (
              (status = 'ready' AND (scheduled_at_ms IS NULL OR scheduled_at_ms <= :now_ms))
              OR (status = 'claimed' AND lease_until_ms <= :now_ms AND attempt < max_attempts)
          )
        ORDER BY priority DESC, created_at_ms ASC
        FETCH FIRST :limit_val ROWS ONLY
        FOR UPDATE SKIP LOCKED
        """

        results: list[QueueEntry] = []
        with self._driver.transaction():
            rows = self._driver.select(select_sql, select_params)
            for row in rows:
                lowered = {k.lower(): v for k, v in row.items()}
                jid = str(lowered["job_id"])
                prev_status = str(lowered["status"])
                curr_attempt = int(lowered["attempt"])
                next_attempt = curr_attempt + 1 if prev_status == "claimed" else curr_attempt

                update_sql = f"""
                UPDATE {self._table_name}
                SET status = 'claimed',
                    worker_id = :worker_id,
                    lease_until_ms = :lease_until_ms,
                    attempt = :next_attempt,
                    updated_at_ms = :now_ms
                WHERE job_id = :job_id
                """
                self._driver.execute(
                    update_sql,
                    {
                        "worker_id": worker_id,
                        "lease_until_ms": lease_until_ms,
                        "next_attempt": next_attempt,
                        "now_ms": now_ms,
                        "job_id": jid,
                    },
                )
                lowered["attempt"] = next_attempt
                lowered["lease_until_ms"] = lease_until_ms
                lowered["worker_id"] = worker_id
                results.append(_row_to_entry(lowered))
        return results

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
        select_sql = f"""
        SELECT {_ENTRY_SELECT_COLUMNS}
        FROM {self._table_name}
        WHERE job_id = :job_id
          AND (
              (status = 'ready' AND (scheduled_at_ms IS NULL OR scheduled_at_ms <= :now_ms))
              OR (status = 'claimed' AND lease_until_ms <= :now_ms AND attempt < max_attempts)
          )
        FOR UPDATE SKIP LOCKED
        """
        with self._driver.transaction():
            row = self._driver.select_one_or_none(select_sql, {"job_id": job_id, "now_ms": now_ms})
            if row is None:
                return None
            lowered = {k.lower(): v for k, v in row.items()}
            prev_status = str(lowered["status"])
            curr_attempt = int(lowered["attempt"])
            next_attempt = curr_attempt + 1 if prev_status == "claimed" else curr_attempt
            if expected_attempt is not None and next_attempt != expected_attempt:
                return None
            update_sql = f"""
            UPDATE {self._table_name}
            SET status = 'claimed',
                worker_id = :worker_id,
                lease_until_ms = :lease_until_ms,
                attempt = :next_attempt,
                updated_at_ms = :now_ms
            WHERE job_id = :job_id
            """
            self._driver.execute(
                update_sql,
                {
                    "worker_id": worker_id,
                    "lease_until_ms": lease_until_ms,
                    "next_attempt": next_attempt,
                    "now_ms": now_ms,
                    "job_id": job_id,
                },
            )
            lowered["attempt"] = next_attempt
            lowered["lease_until_ms"] = lease_until_ms
            lowered["worker_id"] = worker_id
            return _row_to_entry(lowered)

    def ack(self, job_id: str, *, attempt: int) -> bool:
        """Mark a fenced (job_id, attempt) claim as completed."""
        now_ms = int(time.time() * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET status = 'completed',
            lease_until_ms = 0,
            worker_id = NULL,
            completed_at_ms = :now_ms,
            updated_at_ms = :now_ms
        WHERE job_id = :job_id AND attempt = :attempt AND status = 'claimed'
        """
        affected = self._driver.execute(sql, {"job_id": job_id, "attempt": attempt, "now_ms": now_ms})
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
            SET status = 'dead_letter',
                error = :error,
                last_error = :error,
                lease_until_ms = 0,
                worker_id = NULL,
                completed_at_ms = :now_ms,
                updated_at_ms = :now_ms
            WHERE job_id = :job_id AND attempt = :attempt AND status = 'claimed'
            """
            affected = self._driver.execute(
                sql,
                {"job_id": job_id, "attempt": attempt, "error": error, "now_ms": now_ms},
            )
            return affected > 0

        retry_sched_ms = now_ms + int(retry_delay_s * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET status = 'ready',
            attempt = attempt + 1,
            scheduled_at_ms = :retry_sched_ms,
            error = :error,
            last_error = :error,
            lease_until_ms = 0,
            worker_id = NULL,
            updated_at_ms = :now_ms
        WHERE job_id = :job_id AND attempt = :attempt AND status = 'claimed'
        """
        affected = self._driver.execute(
            sql,
            {
                "job_id": job_id,
                "attempt": attempt,
                "retry_sched_ms": retry_sched_ms,
                "error": error,
                "now_ms": now_ms,
            },
        )
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
        in_clause = ", ".join(f":j{i}" for i in range(len(job_ids)))
        params: dict[str, Any] = {f"j{i}": jid for i, jid in enumerate(job_ids)}
        params["lease_until_ms"] = new_lease_ms
        params["now_ms"] = now_ms
        params["worker_id"] = worker_id
        sql = f"""
        UPDATE {self._table_name}
        SET lease_until_ms = :lease_until_ms, updated_at_ms = :now_ms
        WHERE worker_id = :worker_id AND status = 'claimed' AND job_id IN ({in_clause})
        """
        return self._driver.execute(sql, params)

    def reap_stale(self, *, lease_seconds: float = 300.0) -> int:
        """Reap abandoned in-flight claims by bumping attempt or transitioning exhausted jobs to dead_letter."""
        _ = lease_seconds
        now_ms = int(time.time() * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET status = CASE WHEN attempt >= max_attempts THEN 'dead_letter' ELSE 'ready' END,
            attempt = CASE WHEN attempt >= max_attempts THEN attempt ELSE attempt + 1 END,
            last_error = CASE
                WHEN attempt >= max_attempts THEN COALESCE(last_error, TO_CLOB('Visibility lease expired'))
                ELSE last_error
            END,
            worker_id = NULL,
            lease_until_ms = 0,
            updated_at_ms = :now_ms
        WHERE status = 'claimed' AND lease_until_ms <= :now_ms
        """
        return self._driver.execute(sql, {"now_ms": now_ms})

    def promote_scheduled(self, *, limit: int = 100) -> int:
        """Promote due scheduled jobs into ready status."""
        now_ms = int(time.time() * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET scheduled_at_ms = NULL, updated_at_ms = :now_ms
        WHERE job_id IN (
            SELECT job_id FROM {self._table_name}
            WHERE status = 'ready' AND scheduled_at_ms IS NOT NULL AND scheduled_at_ms <= :now_ms
            FETCH FIRST :limit_val ROWS ONLY
        )
        """
        return self._driver.execute(sql, {"now_ms": now_ms, "limit_val": limit})

    def cancel(self, job_id: str) -> bool:
        """Cancel an enqueued or scheduled job."""
        now_ms = int(time.time() * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET status = 'cancelled',
            lease_until_ms = 0,
            worker_id = NULL,
            completed_at_ms = :now_ms,
            updated_at_ms = :now_ms
        WHERE job_id = :job_id AND status IN ('ready', 'claimed')
        """
        affected = self._driver.execute(sql, {"job_id": job_id, "now_ms": now_ms})
        return affected > 0

    def get_entry(self, job_id: str) -> QueueEntry | None:
        """Retrieve a specific queue entry by job_id."""
        sql = f"""
        SELECT {_ENTRY_SELECT_COLUMNS}
        FROM {self._table_name}
        WHERE job_id = :job_id
        """
        row = self._driver.select_one_or_none(sql, {"job_id": job_id})
        return _row_to_entry(row) if row is not None else None

    def get_entry_status(self, job_id: str) -> str | None:
        """Retrieve the current status string of job_id, or None if absent."""
        sql = f"SELECT status FROM {self._table_name} WHERE job_id = :job_id"
        row = self._driver.select_one_or_none(sql, {"job_id": job_id})
        if row is None:
            return None
        lowered = {k.lower(): v for k, v in row.items()}
        return str(lowered["status"]) if lowered.get("status") is not None else None

    def pop_dead_letter(self, job_id: str) -> QueueEntry | None:
        """Atomically remove and return a dead-lettered queue entry by job_id."""
        select_sql = f"""
        SELECT {_ENTRY_SELECT_COLUMNS}
        FROM {self._table_name}
        WHERE job_id = :job_id AND status = 'dead_letter'
        FOR UPDATE
        """
        with self._driver.transaction():
            row = self._driver.select_one_or_none(select_sql, {"job_id": job_id})
            if row is None:
                return None
            self._driver.execute(
                f"DELETE FROM {self._table_name} WHERE job_id = :job_id AND status = 'dead_letter'",
                {"job_id": job_id},
            )
            return _row_to_entry(row)

    def list_entries(
        self,
        queue: str | None = None,
        *,
        status: str | None = None,
        limit: int = 100,
    ) -> list[QueueEntry]:
        """List queue entries matching optional queue and status up to limit."""
        sql, params = _list_entries_sql_and_params(self._table_name, queue, status, limit)
        rows = self._driver.select(sql, params)
        return [_row_to_entry(r) for r in rows]

    def cleanup_terminal(self, *, before_ms: int, limit: int | None = None) -> int:
        """Delete terminal queue records completed before before_ms while preserving forever reservations."""
        sql, params = _cleanup_terminal_sql_and_params(self._table_name, before_ms, limit)
        deleted = self._driver.execute(sql, params)
        self._driver.execute(
            "DELETE FROM vertebra_queue_unique_keys WHERE expires_at_ms IS NOT NULL AND expires_at_ms <= :before_ms",
            {"before_ms": before_ms},
        )
        return deleted

    def get_entry_by_key(self, key: str, *, queue: str | None = None) -> QueueEntry | None:
        """Retrieve a queue entry matching idempotency key and optional queue."""
        sql, params = _get_entry_by_key_sql_and_params(self._table_name, key, queue)
        row = self._driver.select_one_or_none(sql, params)
        return _row_to_entry(row) if row is not None else None

    def reserve_identity(
        self,
        key: str,
        *,
        job_id: str,
        task_name: str,
        ttl_seconds: float | None = None,
    ) -> QueueReservation | None:
        """Acquire a single-winner reservation, returning None when acquired or the existing owner."""
        now_ms = int(time.time() * 1000)
        sql, params = _reserve_identity_sql_and_params(key, job_id, task_name, ttl_seconds, now_ms)
        affected = self._driver.execute(sql, params)
        if affected > 0:
            return None
        return self.get_reservation(key)

    def get_reservation(self, key: str) -> QueueReservation | None:
        """Retrieve an active identity reservation for key, or None."""
        now_ms = int(time.time() * 1000)
        sql = """
        SELECT unique_key, job_id, task_name, created_at_ms
        FROM vertebra_queue_unique_keys
        WHERE unique_key = :key
          AND (expires_at_ms IS NULL OR expires_at_ms > :now_ms)
        """
        row = self._driver.select_one_or_none(sql, {"key": key, "now_ms": now_ms})
        return _row_to_reservation(row) if row is not None else None

    def reset_identity(self, key: str, *, expected_job_id: str | None = None) -> bool:
        """Release an identity reservation, optionally fenced by expected_job_id."""
        sql, params = _reset_identity_sql_and_params(key, expected_job_id)
        return self._driver.execute(sql, params) > 0

    def assign_worker(self, job_id: str, *, worker_id: str, attempt: int) -> bool:
        """Record worker_id assignment for an active fenced claim."""
        now_ms = int(time.time() * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET worker_id = :worker_id, updated_at_ms = :now_ms
        WHERE job_id = :job_id AND attempt = :attempt AND status = 'claimed'
        """
        return (
            self._driver.execute(
                sql,
                {"job_id": job_id, "worker_id": worker_id, "attempt": attempt, "now_ms": now_ms},
            )
            > 0
        )

    def touch_heartbeats(self, job_ids: Sequence[str], *, heartbeat_at_ms: int) -> int:
        """Update heartbeat_at_ms for active claimed jobs and return updated count."""
        if not job_ids:
            return 0
        now_ms = int(time.time() * 1000)
        sql, params = _heartbeat_update_sql_and_params(
            self._table_name,
            job_ids,
            now_ms,
            heartbeat_at_ms=heartbeat_at_ms,
        )
        return self._driver.execute(sql, params)

    def null_heartbeats(self, job_ids: Sequence[str]) -> int:
        """Clear heartbeat_at_ms for the given job_ids and return updated count."""
        if not job_ids:
            return 0
        now_ms = int(time.time() * 1000)
        sql, params = _heartbeat_update_sql_and_params(
            self._table_name,
            job_ids,
            now_ms,
            heartbeat_at_ms=None,
        )
        return self._driver.execute(sql, params)

    def interrupt_task(self, job_id: str, *, error: str | None = None) -> bool:
        """Interrupt a pending or running job with optional error detail."""
        now_ms = int(time.time() * 1000)
        sql, params = _interrupt_task_sql_and_params(self._table_name, job_id, error, now_ms)
        return self._driver.execute(sql, params) > 0

    def expire_overdue(self, *, now_ms: int, limit: int | None = None) -> int:
        """Mark overdue jobs with expires_at_ms <= now_ms as expired up to limit."""
        sql, params = _expire_overdue_sql_and_params(self._table_name, now_ms, limit)
        return self._driver.execute(sql, params)

    def next_due_delay(self, queues: Sequence[str]) -> float | None:
        """Return the delay in seconds until the next scheduled job is due, or None."""
        if not queues:
            return None
        now_ms = int(time.time() * 1000)
        in_clause = ", ".join(f":q{i}" for i in range(len(queues)))
        params: dict[str, Any] = {f"q{i}": q for i, q in enumerate(queues)}
        params["now_ms"] = now_ms
        sql = f"""
        SELECT MIN(scheduled_at_ms) AS min_sched
        FROM {self._table_name}
        WHERE status = 'ready' AND scheduled_at_ms > :now_ms AND queue IN ({in_clause})
        """
        row = self._driver.select_one_or_none(sql, params)
        if row is None:
            return None
        lowered = {k.lower(): v for k, v in row.items()}
        val = lowered.get("min_sched")
        if val is None:
            return None
        min_sched = int(val)
        return max(0.0, (min_sched - now_ms) / 1000.0)

    def reserve_unique_key(self, key: str, *, ttl_seconds: float) -> bool:
        """Reserve an idempotency key for ttl_seconds."""
        now_ms = int(time.time() * 1000)
        expires_at_ms = now_ms + int(ttl_seconds * 1000)
        sql = """
        MERGE INTO vertebra_queue_unique_keys u
        USING (SELECT :key AS k, :expires_at_ms AS exp FROM dual) src
        ON (u.unique_key = src.k)
        WHEN MATCHED THEN
          UPDATE SET u.expires_at_ms = src.exp
          WHERE u.expires_at_ms IS NOT NULL AND u.expires_at_ms <= :now_ms
        WHEN NOT MATCHED THEN
          INSERT (unique_key, expires_at_ms)
          VALUES (src.k, src.exp)
        """
        affected = self._driver.execute(
            sql,
            {"key": key, "expires_at_ms": expires_at_ms, "now_ms": now_ms},
        )
        return affected > 0

    def release_unique_key(self, key: str) -> bool:
        """Release an idempotency key reservation."""
        sql = "DELETE FROM vertebra_queue_unique_keys WHERE unique_key = :key"
        return self._driver.execute(sql, {"key": key}) > 0

    def acquire_lease(self, lease_key: str, *, owner_id: str, ttl_seconds: float) -> bool:
        """Attempt to acquire a distributed leader lease for a cron or coordination slot."""
        now_ms = int(time.time() * 1000)
        expires_at_ms = now_ms + int(ttl_seconds * 1000)
        sql = """
        MERGE INTO vertebra_queue_leases l
        USING (SELECT :lease_key AS k, :owner_id AS o, :expires_at_ms AS exp FROM dual) src
        ON (l.lease_key = src.k)
        WHEN MATCHED THEN
          UPDATE SET l.owner_id = src.o, l.expires_at_ms = src.exp
          WHERE l.expires_at_ms <= :now_ms OR l.owner_id = src.o
        WHEN NOT MATCHED THEN
          INSERT (lease_key, owner_id, expires_at_ms)
          VALUES (src.k, src.o, src.exp)
        """
        affected = self._driver.execute(
            sql,
            {
                "lease_key": lease_key,
                "owner_id": owner_id,
                "expires_at_ms": expires_at_ms,
                "now_ms": now_ms,
            },
        )
        return affected > 0

    def release_lease(self, lease_key: str, *, owner_id: str) -> bool:
        """Release a held distributed leader lease."""
        sql = "DELETE FROM vertebra_queue_leases WHERE lease_key = :lease_key AND owner_id = :owner_id"
        return self._driver.execute(sql, {"lease_key": lease_key, "owner_id": owner_id}) > 0

    def close(self) -> None:
        """Close backing driver resources if owned."""
        self._driver.close()


class AsyncOracleQueueStore(AsyncQueueStore):
    """Asynchronous Oracle queue store backed by FOR UPDATE SKIP LOCKED and MERGE leases."""

    __slots__ = ("_driver", "_notification_source", "_table_name")

    def __init__(
        self,
        driver: AsyncOracleDriver,
        *,
        notification_source: AsyncOracleNotificationSource | None = None,
        table_name: str = "vertebra_queue_jobs",
    ) -> None:
        """Initialize the asynchronous queue store with an Oracle driver."""
        self._driver = driver
        self._notification_source = notification_source
        self._table_name = validate_identifier(table_name, "table_name")

    async def ensure_schema(self) -> None:
        """Idempotently create queue, lease, and uniqueness tables and indexes."""
        for stmt in _schema_statements(self._table_name):
            try:
                await self._driver.execute(stmt)
            except Exception as exc:
                if _is_ignored_ddl_error(exc):
                    continue
                raise

    async def enqueue(self, entry: QueueEntry) -> str:
        """Enqueue a single queue entry asynchronously and emit an optional notification wakeup."""
        jid = entry.job_id or uuid.uuid4().hex
        now_ms = int(time.time() * 1000)
        sql, params = _enqueue_sql_and_params(self._table_name, entry, jid, now_ms)
        await self._driver.execute(sql, params)
        if self._notification_source is not None:
            await self._notification_source.publish(entry.queue, f"wakeup:{jid}")
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
        """Atomically claim up to limit ready jobs asynchronously via FOR UPDATE SKIP LOCKED."""
        if not queues:
            return []
        now_ms = int(time.time() * 1000)
        lease_until_ms = now_ms + int(lease_seconds * 1000)

        in_clause = ", ".join(f":q{i}" for i in range(len(queues)))
        select_params: dict[str, Any] = {f"q{i}": q for i, q in enumerate(queues)}
        select_params["now_ms"] = now_ms
        select_params["limit_val"] = limit

        select_sql = f"""
        SELECT {_ENTRY_SELECT_COLUMNS}
        FROM {self._table_name}
        WHERE queue IN ({in_clause})
          AND (
              (status = 'ready' AND (scheduled_at_ms IS NULL OR scheduled_at_ms <= :now_ms))
              OR (status = 'claimed' AND lease_until_ms <= :now_ms AND attempt < max_attempts)
          )
        ORDER BY priority DESC, created_at_ms ASC
        FETCH FIRST :limit_val ROWS ONLY
        FOR UPDATE SKIP LOCKED
        """

        results: list[QueueEntry] = []
        async with self._driver.transaction():
            rows = await self._driver.select(select_sql, select_params)
            for row in rows:
                lowered = {k.lower(): v for k, v in row.items()}
                jid = str(lowered["job_id"])
                prev_status = str(lowered["status"])
                curr_attempt = int(lowered["attempt"])
                next_attempt = curr_attempt + 1 if prev_status == "claimed" else curr_attempt

                update_sql = f"""
                UPDATE {self._table_name}
                SET status = 'claimed',
                    worker_id = :worker_id,
                    lease_until_ms = :lease_until_ms,
                    attempt = :next_attempt,
                    updated_at_ms = :now_ms
                WHERE job_id = :job_id
                """
                await self._driver.execute(
                    update_sql,
                    {
                        "worker_id": worker_id,
                        "lease_until_ms": lease_until_ms,
                        "next_attempt": next_attempt,
                        "now_ms": now_ms,
                        "job_id": jid,
                    },
                )
                lowered["attempt"] = next_attempt
                lowered["lease_until_ms"] = lease_until_ms
                lowered["worker_id"] = worker_id
                results.append(_row_to_entry(lowered))
        return results

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
        select_sql = f"""
        SELECT {_ENTRY_SELECT_COLUMNS}
        FROM {self._table_name}
        WHERE job_id = :job_id
          AND (
              (status = 'ready' AND (scheduled_at_ms IS NULL OR scheduled_at_ms <= :now_ms))
              OR (status = 'claimed' AND lease_until_ms <= :now_ms AND attempt < max_attempts)
          )
        FOR UPDATE SKIP LOCKED
        """
        async with self._driver.transaction():
            row = await self._driver.select_one_or_none(select_sql, {"job_id": job_id, "now_ms": now_ms})
            if row is None:
                return None
            lowered = {k.lower(): v for k, v in row.items()}
            prev_status = str(lowered["status"])
            curr_attempt = int(lowered["attempt"])
            next_attempt = curr_attempt + 1 if prev_status == "claimed" else curr_attempt
            if expected_attempt is not None and next_attempt != expected_attempt:
                return None
            update_sql = f"""
            UPDATE {self._table_name}
            SET status = 'claimed',
                worker_id = :worker_id,
                lease_until_ms = :lease_until_ms,
                attempt = :next_attempt,
                updated_at_ms = :now_ms
            WHERE job_id = :job_id
            """
            await self._driver.execute(
                update_sql,
                {
                    "worker_id": worker_id,
                    "lease_until_ms": lease_until_ms,
                    "next_attempt": next_attempt,
                    "now_ms": now_ms,
                    "job_id": job_id,
                },
            )
            lowered["attempt"] = next_attempt
            lowered["lease_until_ms"] = lease_until_ms
            lowered["worker_id"] = worker_id
            return _row_to_entry(lowered)

    async def ack(self, job_id: str, *, attempt: int) -> bool:
        """Mark a fenced (job_id, attempt) claim as completed asynchronously."""
        now_ms = int(time.time() * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET status = 'completed',
            lease_until_ms = 0,
            worker_id = NULL,
            completed_at_ms = :now_ms,
            updated_at_ms = :now_ms
        WHERE job_id = :job_id AND attempt = :attempt AND status = 'claimed'
        """
        affected = await self._driver.execute(sql, {"job_id": job_id, "attempt": attempt, "now_ms": now_ms})
        return affected > 0

    async def nack(
        self,
        job_id: str,
        *,
        attempt: int,
        error: str,
        retry_delay_s: float | None = None,
    ) -> bool:
        """Record a failed (job_id, attempt) execution asynchronously and schedule retry or DLQ."""
        now_ms = int(time.time() * 1000)
        entry = await self.get_entry(job_id)
        if entry is None or entry.attempt != attempt:
            return False

        if entry.attempt >= entry.max_attempts or retry_delay_s is None:
            sql = f"""
            UPDATE {self._table_name}
            SET status = 'dead_letter',
                error = :error,
                last_error = :error,
                lease_until_ms = 0,
                worker_id = NULL,
                completed_at_ms = :now_ms,
                updated_at_ms = :now_ms
            WHERE job_id = :job_id AND attempt = :attempt AND status = 'claimed'
            """
            affected = await self._driver.execute(
                sql,
                {"job_id": job_id, "attempt": attempt, "error": error, "now_ms": now_ms},
            )
            return affected > 0

        retry_sched_ms = now_ms + int(retry_delay_s * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET status = 'ready',
            attempt = attempt + 1,
            scheduled_at_ms = :retry_sched_ms,
            error = :error,
            last_error = :error,
            lease_until_ms = 0,
            worker_id = NULL,
            updated_at_ms = :now_ms
        WHERE job_id = :job_id AND attempt = :attempt AND status = 'claimed'
        """
        affected = await self._driver.execute(
            sql,
            {
                "job_id": job_id,
                "attempt": attempt,
                "retry_sched_ms": retry_sched_ms,
                "error": error,
                "now_ms": now_ms,
            },
        )
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
        in_clause = ", ".join(f":j{i}" for i in range(len(job_ids)))
        params: dict[str, Any] = {f"j{i}": jid for i, jid in enumerate(job_ids)}
        params["lease_until_ms"] = new_lease_ms
        params["now_ms"] = now_ms
        params["worker_id"] = worker_id
        sql = f"""
        UPDATE {self._table_name}
        SET lease_until_ms = :lease_until_ms, updated_at_ms = :now_ms
        WHERE worker_id = :worker_id AND status = 'claimed' AND job_id IN ({in_clause})
        """
        return await self._driver.execute(sql, params)

    async def reap_stale(self, *, lease_seconds: float = 300.0) -> int:
        """Reap abandoned in-flight claims asynchronously by bumping attempt or dead-lettering."""
        _ = lease_seconds
        now_ms = int(time.time() * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET status = CASE WHEN attempt >= max_attempts THEN 'dead_letter' ELSE 'ready' END,
            attempt = CASE WHEN attempt >= max_attempts THEN attempt ELSE attempt + 1 END,
            last_error = CASE
                WHEN attempt >= max_attempts THEN COALESCE(last_error, TO_CLOB('Visibility lease expired'))
                ELSE last_error
            END,
            worker_id = NULL,
            lease_until_ms = 0,
            updated_at_ms = :now_ms
        WHERE status = 'claimed' AND lease_until_ms <= :now_ms
        """
        return await self._driver.execute(sql, {"now_ms": now_ms})

    async def promote_scheduled(self, *, limit: int = 100) -> int:
        """Promote due scheduled jobs into ready status asynchronously."""
        now_ms = int(time.time() * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET scheduled_at_ms = NULL, updated_at_ms = :now_ms
        WHERE job_id IN (
            SELECT job_id FROM {self._table_name}
            WHERE status = 'ready' AND scheduled_at_ms IS NOT NULL AND scheduled_at_ms <= :now_ms
            FETCH FIRST :limit_val ROWS ONLY
        )
        """
        return await self._driver.execute(sql, {"now_ms": now_ms, "limit_val": limit})

    async def cancel(self, job_id: str) -> bool:
        """Cancel an enqueued or scheduled job asynchronously."""
        now_ms = int(time.time() * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET status = 'cancelled',
            lease_until_ms = 0,
            worker_id = NULL,
            completed_at_ms = :now_ms,
            updated_at_ms = :now_ms
        WHERE job_id = :job_id AND status IN ('ready', 'claimed')
        """
        affected = await self._driver.execute(sql, {"job_id": job_id, "now_ms": now_ms})
        return affected > 0

    async def get_entry(self, job_id: str) -> QueueEntry | None:
        """Retrieve a specific queue entry by job_id asynchronously."""
        sql = f"""
        SELECT {_ENTRY_SELECT_COLUMNS}
        FROM {self._table_name}
        WHERE job_id = :job_id
        """
        row = await self._driver.select_one_or_none(sql, {"job_id": job_id})
        return _row_to_entry(row) if row is not None else None

    async def get_entry_status(self, job_id: str) -> str | None:
        """Retrieve the current status string of job_id asynchronously, or None if absent."""
        sql = f"SELECT status FROM {self._table_name} WHERE job_id = :job_id"
        row = await self._driver.select_one_or_none(sql, {"job_id": job_id})
        if row is None:
            return None
        lowered = {k.lower(): v for k, v in row.items()}
        return str(lowered["status"]) if lowered.get("status") is not None else None

    async def pop_dead_letter(self, job_id: str) -> QueueEntry | None:
        """Atomically remove and return a dead-lettered queue entry asynchronously by job_id."""
        select_sql = f"""
        SELECT {_ENTRY_SELECT_COLUMNS}
        FROM {self._table_name}
        WHERE job_id = :job_id AND status = 'dead_letter'
        FOR UPDATE
        """
        async with self._driver.transaction():
            row = await self._driver.select_one_or_none(select_sql, {"job_id": job_id})
            if row is None:
                return None
            await self._driver.execute(
                f"DELETE FROM {self._table_name} WHERE job_id = :job_id AND status = 'dead_letter'",
                {"job_id": job_id},
            )
            return _row_to_entry(row)

    async def list_entries(
        self,
        queue: str | None = None,
        *,
        status: str | None = None,
        limit: int = 100,
    ) -> list[QueueEntry]:
        """List queue entries matching optional queue and status up to limit asynchronously."""
        sql, params = _list_entries_sql_and_params(self._table_name, queue, status, limit)
        rows = await self._driver.select(sql, params)
        return [_row_to_entry(r) for r in rows]

    async def cleanup_terminal(self, *, before_ms: int, limit: int | None = None) -> int:
        """Delete terminal queue records completed before before_ms while preserving forever reservations."""
        sql, params = _cleanup_terminal_sql_and_params(self._table_name, before_ms, limit)
        deleted = await self._driver.execute(sql, params)
        await self._driver.execute(
            "DELETE FROM vertebra_queue_unique_keys WHERE expires_at_ms IS NOT NULL AND expires_at_ms <= :before_ms",
            {"before_ms": before_ms},
        )
        return deleted

    async def get_entry_by_key(self, key: str, *, queue: str | None = None) -> QueueEntry | None:
        """Retrieve a queue entry matching idempotency key and optional queue asynchronously."""
        sql, params = _get_entry_by_key_sql_and_params(self._table_name, key, queue)
        row = await self._driver.select_one_or_none(sql, params)
        return _row_to_entry(row) if row is not None else None

    async def reserve_identity(
        self,
        key: str,
        *,
        job_id: str,
        task_name: str,
        ttl_seconds: float | None = None,
    ) -> QueueReservation | None:
        """Acquire a single-winner reservation asynchronously, returning None when acquired or existing owner."""
        now_ms = int(time.time() * 1000)
        sql, params = _reserve_identity_sql_and_params(key, job_id, task_name, ttl_seconds, now_ms)
        affected = await self._driver.execute(sql, params)
        if affected > 0:
            return None
        return await self.get_reservation(key)

    async def get_reservation(self, key: str) -> QueueReservation | None:
        """Retrieve an active identity reservation for key asynchronously, or None."""
        now_ms = int(time.time() * 1000)
        sql = """
        SELECT unique_key, job_id, task_name, created_at_ms
        FROM vertebra_queue_unique_keys
        WHERE unique_key = :key
          AND (expires_at_ms IS NULL OR expires_at_ms > :now_ms)
        """
        row = await self._driver.select_one_or_none(sql, {"key": key, "now_ms": now_ms})
        return _row_to_reservation(row) if row is not None else None

    async def reset_identity(self, key: str, *, expected_job_id: str | None = None) -> bool:
        """Release an identity reservation asynchronously, optionally fenced by expected_job_id."""
        sql, params = _reset_identity_sql_and_params(key, expected_job_id)
        return await self._driver.execute(sql, params) > 0

    async def assign_worker(self, job_id: str, *, worker_id: str, attempt: int) -> bool:
        """Record worker_id assignment for an active fenced claim asynchronously."""
        now_ms = int(time.time() * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET worker_id = :worker_id, updated_at_ms = :now_ms
        WHERE job_id = :job_id AND attempt = :attempt AND status = 'claimed'
        """
        return (
            await self._driver.execute(
                sql,
                {"job_id": job_id, "worker_id": worker_id, "attempt": attempt, "now_ms": now_ms},
            )
            > 0
        )

    async def touch_heartbeats(self, job_ids: Sequence[str], *, heartbeat_at_ms: int) -> int:
        """Update heartbeat_at_ms for active claimed jobs asynchronously and return updated count."""
        if not job_ids:
            return 0
        now_ms = int(time.time() * 1000)
        sql, params = _heartbeat_update_sql_and_params(
            self._table_name,
            job_ids,
            now_ms,
            heartbeat_at_ms=heartbeat_at_ms,
        )
        return await self._driver.execute(sql, params)

    async def null_heartbeats(self, job_ids: Sequence[str]) -> int:
        """Clear heartbeat_at_ms for the given job_ids asynchronously and return updated count."""
        if not job_ids:
            return 0
        now_ms = int(time.time() * 1000)
        sql, params = _heartbeat_update_sql_and_params(
            self._table_name,
            job_ids,
            now_ms,
            heartbeat_at_ms=None,
        )
        return await self._driver.execute(sql, params)

    async def interrupt_task(self, job_id: str, *, error: str | None = None) -> bool:
        """Interrupt a pending or running job asynchronously with optional error detail."""
        now_ms = int(time.time() * 1000)
        sql, params = _interrupt_task_sql_and_params(self._table_name, job_id, error, now_ms)
        return await self._driver.execute(sql, params) > 0

    async def expire_overdue(self, *, now_ms: int, limit: int | None = None) -> int:
        """Mark overdue jobs with expires_at_ms <= now_ms as expired asynchronously up to limit."""
        sql, params = _expire_overdue_sql_and_params(self._table_name, now_ms, limit)
        return await self._driver.execute(sql, params)

    async def next_due_delay(self, queues: Sequence[str]) -> float | None:
        """Return the delay in seconds until the next scheduled job is due, or None."""
        if not queues:
            return None
        now_ms = int(time.time() * 1000)
        in_clause = ", ".join(f":q{i}" for i in range(len(queues)))
        params: dict[str, Any] = {f"q{i}": q for i, q in enumerate(queues)}
        params["now_ms"] = now_ms
        sql = f"""
        SELECT MIN(scheduled_at_ms) AS min_sched
        FROM {self._table_name}
        WHERE status = 'ready' AND scheduled_at_ms > :now_ms AND queue IN ({in_clause})
        """
        row = await self._driver.select_one_or_none(sql, params)
        if row is None:
            return None
        lowered = {k.lower(): v for k, v in row.items()}
        val = lowered.get("min_sched")
        if val is None:
            return None
        min_sched = int(val)
        return max(0.0, (min_sched - now_ms) / 1000.0)

    async def reserve_unique_key(self, key: str, *, ttl_seconds: float) -> bool:
        """Reserve an idempotency key asynchronously for ttl_seconds."""
        now_ms = int(time.time() * 1000)
        expires_at_ms = now_ms + int(ttl_seconds * 1000)
        sql = """
        MERGE INTO vertebra_queue_unique_keys u
        USING (SELECT :key AS k, :expires_at_ms AS exp FROM dual) src
        ON (u.unique_key = src.k)
        WHEN MATCHED THEN
          UPDATE SET u.expires_at_ms = src.exp
          WHERE u.expires_at_ms IS NOT NULL AND u.expires_at_ms <= :now_ms
        WHEN NOT MATCHED THEN
          INSERT (unique_key, expires_at_ms)
          VALUES (src.k, src.exp)
        """
        affected = await self._driver.execute(
            sql,
            {"key": key, "expires_at_ms": expires_at_ms, "now_ms": now_ms},
        )
        return affected > 0

    async def release_unique_key(self, key: str) -> bool:
        """Release an idempotency key reservation asynchronously."""
        sql = "DELETE FROM vertebra_queue_unique_keys WHERE unique_key = :key"
        return await self._driver.execute(sql, {"key": key}) > 0

    async def acquire_lease(self, lease_key: str, *, owner_id: str, ttl_seconds: float) -> bool:
        """Attempt to acquire a distributed leader lease asynchronously."""
        now_ms = int(time.time() * 1000)
        expires_at_ms = now_ms + int(ttl_seconds * 1000)
        sql = """
        MERGE INTO vertebra_queue_leases l
        USING (SELECT :lease_key AS k, :owner_id AS o, :expires_at_ms AS exp FROM dual) src
        ON (l.lease_key = src.k)
        WHEN MATCHED THEN
          UPDATE SET l.owner_id = src.o, l.expires_at_ms = src.exp
          WHERE l.expires_at_ms <= :now_ms OR l.owner_id = src.o
        WHEN NOT MATCHED THEN
          INSERT (lease_key, owner_id, expires_at_ms)
          VALUES (src.k, src.o, src.exp)
        """
        affected = await self._driver.execute(
            sql,
            {
                "lease_key": lease_key,
                "owner_id": owner_id,
                "expires_at_ms": expires_at_ms,
                "now_ms": now_ms,
            },
        )
        return affected > 0

    async def release_lease(self, lease_key: str, *, owner_id: str) -> bool:
        """Release a held distributed leader lease asynchronously."""
        sql = "DELETE FROM vertebra_queue_leases WHERE lease_key = :lease_key AND owner_id = :owner_id"
        return await self._driver.execute(sql, {"lease_key": lease_key, "owner_id": owner_id}) > 0

    async def aclose(self) -> None:
        """Close backing driver resources asynchronously."""
        await self._driver.close()
