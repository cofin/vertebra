"""Oracle 26ai-backed synchronous and asynchronous queue stores using FOR UPDATE SKIP LOCKED."""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING, Any, cast

from vertebra.db.driver.capabilities import AsyncQueueStore, QueueEntry, SyncQueueStore
from vertebra.utils.serializers import decode_json, encode_json

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
    lowered = {k.lower(): v for k, v in row.items()}
    payload = lowered["payload"]
    if isinstance(payload, (bytes, bytearray, memoryview)):
        payload_bytes = bytes(payload)
    elif isinstance(payload, str):
        payload_bytes = payload.encode("utf-8")
    else:
        payload_bytes = bytes(payload)

    sched = lowered.get("scheduled_at_ms")
    return QueueEntry(
        job_id=str(lowered["job_id"]),
        queue=str(lowered["queue"]),
        task_name=str(lowered["task_name"]),
        payload=payload_bytes,
        attempt=int(lowered["attempt"]),
        max_attempts=int(lowered["max_attempts"]),
        lease_until_ms=int(lowered.get("lease_until_ms", 0)),
        scheduled_at_ms=int(sched) if sched is not None else None,
        headers=_parse_headers(lowered.get("headers")),
    )


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
        self._table_name = table_name

    def ensure_schema(self) -> None:
        """Idempotently create queue, lease, and uniqueness tables and indexes."""
        statements = [
            f"""
            CREATE TABLE {self._table_name} (
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
                created_at_ms NUMBER(19) NOT NULL,
                updated_at_ms NUMBER(19) NOT NULL
            )
            """,
            f"CREATE INDEX idx_{self._table_name}_claim ON {self._table_name} (queue, status, scheduled_at_ms, lease_until_ms)",
            f"CREATE INDEX idx_{self._table_name}_stale ON {self._table_name} (status, lease_until_ms)",
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
                expires_at_ms NUMBER(19) NOT NULL
            )
            """,
        ]
        for stmt in statements:
            try:
                self._driver.execute(stmt)
            except Exception as exc:
                err_msg = str(exc)
                if "00955" in err_msg or "01408" in err_msg or "ORA-00955" in err_msg:
                    continue
                raise

    def enqueue(self, entry: QueueEntry) -> str:
        """Enqueue a single queue entry and emit an optional TxEventQ notification wakeup."""
        jid = entry.job_id or uuid.uuid4().hex
        now_ms = int(time.time() * 1000)
        sched_ms = entry.scheduled_at_ms
        sql = f"""
        INSERT INTO {self._table_name} (
            job_id, queue, task_name, payload, attempt, max_attempts,
            status, scheduled_at_ms, lease_until_ms, headers,
            created_at_ms, updated_at_ms
        ) VALUES (
            :job_id, :queue, :task_name, :payload, :attempt, :max_attempts,
            :status, :scheduled_at_ms, :lease_until_ms, :headers,
            :created_at_ms, :updated_at_ms
        )
        """
        self._driver.execute(
            sql,
            {
                "job_id": jid,
                "queue": entry.queue,
                "task_name": entry.task_name,
                "payload": entry.payload,
                "attempt": entry.attempt,
                "max_attempts": entry.max_attempts,
                "status": "ready",
                "scheduled_at_ms": sched_ms,
                "lease_until_ms": entry.lease_until_ms,
                "headers": _serialize_headers(entry.headers),
                "created_at_ms": now_ms,
                "updated_at_ms": now_ms,
            },
        )
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
        SELECT job_id, queue, task_name, payload, attempt, max_attempts, status,
               lease_until_ms, scheduled_at_ms, headers
        FROM {self._table_name}
        WHERE queue IN ({in_clause})
          AND (
              (status = 'ready' AND (scheduled_at_ms IS NULL OR scheduled_at_ms <= :now_ms))
              OR (status = 'claimed' AND lease_until_ms <= :now_ms)
          )
        ORDER BY created_at_ms ASC
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
                results.append(_row_to_entry(lowered))
        return results

    def ack(self, job_id: str, *, attempt: int) -> bool:
        """Mark a fenced (job_id, attempt) claim as completed."""
        now_ms = int(time.time() * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET status = 'completed', updated_at_ms = :now_ms
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
            SET status = 'dead_letter', last_error = :error, updated_at_ms = :now_ms
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
        """Reap abandoned in-flight claims back into ready status."""
        now_ms = int(time.time() * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET status = 'ready', worker_id = NULL, lease_until_ms = 0, updated_at_ms = :now_ms
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
        SET status = 'cancelled', updated_at_ms = :now_ms
        WHERE job_id = :job_id AND status IN ('ready', 'claimed')
        """
        affected = self._driver.execute(sql, {"job_id": job_id, "now_ms": now_ms})
        return affected > 0

    def get_entry(self, job_id: str) -> QueueEntry | None:
        """Retrieve a specific queue entry by job_id."""
        sql = f"""
        SELECT job_id, queue, task_name, payload, attempt, max_attempts,
               lease_until_ms, scheduled_at_ms, headers
        FROM {self._table_name}
        WHERE job_id = :job_id
        """
        row = self._driver.select_one_or_none(sql, {"job_id": job_id})
        return _row_to_entry(row) if row is not None else None

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
            WHERE queue = :queue AND status = :status
            ORDER BY created_at_ms ASC
            FETCH FIRST :limit_val ROWS ONLY
            """
            rows = self._driver.select(sql, {"queue": queue, "status": status, "limit_val": limit})
        else:
            sql = f"""
            SELECT job_id, queue, task_name, payload, attempt, max_attempts,
                   lease_until_ms, scheduled_at_ms, headers
            FROM {self._table_name}
            WHERE queue = :queue
            ORDER BY created_at_ms ASC
            FETCH FIRST :limit_val ROWS ONLY
            """
            rows = self._driver.select(sql, {"queue": queue, "limit_val": limit})
        return [_row_to_entry(r) for r in rows]

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
          WHERE u.expires_at_ms <= :now_ms
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
        self._table_name = table_name

    async def ensure_schema(self) -> None:
        """Idempotently create queue, lease, and uniqueness tables and indexes."""
        statements = [
            f"""
            CREATE TABLE {self._table_name} (
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
                created_at_ms NUMBER(19) NOT NULL,
                updated_at_ms NUMBER(19) NOT NULL
            )
            """,
            f"CREATE INDEX idx_{self._table_name}_claim ON {self._table_name} (queue, status, scheduled_at_ms, lease_until_ms)",
            f"CREATE INDEX idx_{self._table_name}_stale ON {self._table_name} (status, lease_until_ms)",
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
                expires_at_ms NUMBER(19) NOT NULL
            )
            """,
        ]
        for stmt in statements:
            try:
                await self._driver.execute(stmt)
            except Exception as exc:
                err_msg = str(exc)
                if "00955" in err_msg or "01408" in err_msg or "ORA-00955" in err_msg:
                    continue
                raise

    async def enqueue(self, entry: QueueEntry) -> str:
        """Enqueue a single queue entry asynchronously and emit an optional notification wakeup."""
        jid = entry.job_id or uuid.uuid4().hex
        now_ms = int(time.time() * 1000)
        sched_ms = entry.scheduled_at_ms
        sql = f"""
        INSERT INTO {self._table_name} (
            job_id, queue, task_name, payload, attempt, max_attempts,
            status, scheduled_at_ms, lease_until_ms, headers,
            created_at_ms, updated_at_ms
        ) VALUES (
            :job_id, :queue, :task_name, :payload, :attempt, :max_attempts,
            :status, :scheduled_at_ms, :lease_until_ms, :headers,
            :created_at_ms, :updated_at_ms
        )
        """
        await self._driver.execute(
            sql,
            {
                "job_id": jid,
                "queue": entry.queue,
                "task_name": entry.task_name,
                "payload": entry.payload,
                "attempt": entry.attempt,
                "max_attempts": entry.max_attempts,
                "status": "ready",
                "scheduled_at_ms": sched_ms,
                "lease_until_ms": entry.lease_until_ms,
                "headers": _serialize_headers(entry.headers),
                "created_at_ms": now_ms,
                "updated_at_ms": now_ms,
            },
        )
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
        SELECT job_id, queue, task_name, payload, attempt, max_attempts, status,
               lease_until_ms, scheduled_at_ms, headers
        FROM {self._table_name}
        WHERE queue IN ({in_clause})
          AND (
              (status = 'ready' AND (scheduled_at_ms IS NULL OR scheduled_at_ms <= :now_ms))
              OR (status = 'claimed' AND lease_until_ms <= :now_ms)
          )
        ORDER BY created_at_ms ASC
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
                results.append(_row_to_entry(lowered))
        return results

    async def ack(self, job_id: str, *, attempt: int) -> bool:
        """Mark a fenced (job_id, attempt) claim as completed asynchronously."""
        now_ms = int(time.time() * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET status = 'completed', updated_at_ms = :now_ms
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
            SET status = 'dead_letter', last_error = :error, updated_at_ms = :now_ms
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
        """Reap abandoned in-flight claims back into ready status asynchronously."""
        now_ms = int(time.time() * 1000)
        sql = f"""
        UPDATE {self._table_name}
        SET status = 'ready', worker_id = NULL, lease_until_ms = 0, updated_at_ms = :now_ms
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
        SET status = 'cancelled', updated_at_ms = :now_ms
        WHERE job_id = :job_id AND status IN ('ready', 'claimed')
        """
        affected = await self._driver.execute(sql, {"job_id": job_id, "now_ms": now_ms})
        return affected > 0

    async def get_entry(self, job_id: str) -> QueueEntry | None:
        """Retrieve a specific queue entry by job_id asynchronously."""
        sql = f"""
        SELECT job_id, queue, task_name, payload, attempt, max_attempts,
               lease_until_ms, scheduled_at_ms, headers
        FROM {self._table_name}
        WHERE job_id = :job_id
        """
        row = await self._driver.select_one_or_none(sql, {"job_id": job_id})
        return _row_to_entry(row) if row is not None else None

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
            WHERE queue = :queue AND status = :status
            ORDER BY created_at_ms ASC
            FETCH FIRST :limit_val ROWS ONLY
            """
            rows = await self._driver.select(sql, {"queue": queue, "status": status, "limit_val": limit})
        else:
            sql = f"""
            SELECT job_id, queue, task_name, payload, attempt, max_attempts,
                   lease_until_ms, scheduled_at_ms, headers
            FROM {self._table_name}
            WHERE queue = :queue
            ORDER BY created_at_ms ASC
            FETCH FIRST :limit_val ROWS ONLY
            """
            rows = await self._driver.select(sql, {"queue": queue, "limit_val": limit})
        return [_row_to_entry(r) for r in rows]

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
          WHERE u.expires_at_ms <= :now_ms
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
