"""Cloud Spanner synchronous and asynchronous queue stores across GoogleSQL and Spangres."""

from __future__ import annotations

import base64
import hashlib
import time
from typing import TYPE_CHECKING, Any, Final, Literal, cast

from vertebra.db.driver.capabilities import AsyncQueueStore, QueueEntry, SyncQueueStore
from vertebra.utils.serializers import decode_json, encode_json
from vertebra.utils.text import validate_identifier
from vertebra.utils.uuids import uuid4

if TYPE_CHECKING:
    from collections.abc import Sequence

    from vertebra.db.adapters.spanner._async import AsyncSpannerDriver
    from vertebra.db.adapters.spanner._sync import SyncSpannerDriver
    from vertebra.db.adapters.spanner.transactions import (
        AsyncSpannerTransaction,
        SyncSpannerTransaction,
    )

__all__ = (
    "AsyncSpannerQueueStore",
    "SPANNER_QUEUE_STORE_DDL_GOOGLESQL",
    "SPANNER_QUEUE_STORE_DDL_SPANGRES",
    "SyncSpannerQueueStore",
    "compute_spanner_shard_id",
    "format_spanner_receipt_handle",
    "parse_spanner_receipt_handle",
)

_BINARY_PAYLOAD_PREFIX: Final[str] = "vtb-b64:"

SPANNER_QUEUE_STORE_DDL_GOOGLESQL: Final[tuple[str, ...]] = (
    """
    CREATE TABLE IF NOT EXISTS vertebra_queue_jobs (
        shard_id INT64 NOT NULL,
        task_id STRING(64) NOT NULL,
        task_name STRING(256) NOT NULL,
        lane STRING(128) NOT NULL,
        payload STRING(MAX) NOT NULL,
        status STRING(32) NOT NULL,
        attempt INT64 NOT NULL,
        max_retries INT64 NOT NULL,
        scheduled_at FLOAT64,
        enqueued_at FLOAT64 NOT NULL,
        locked_until FLOAT64,
        consumer_name STRING(128),
        receipt_handle STRING(192),
        idempotency_key STRING(256),
        traceparent STRING(256),
        headers STRING(MAX),
        last_error STRING(MAX),
        expires_at TIMESTAMP
    ) PRIMARY KEY (shard_id, task_id),
    ROW DELETION POLICY (OLDER_THAN(expires_at, INTERVAL 0 DAY))
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_vtb_spanner_queue_ready
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
        traceparent,
        headers,
        last_error
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS vertebra_queue_dlq (
        dlq_id STRING(64) NOT NULL,
        shard_id INT64 NOT NULL,
        task_id STRING(64) NOT NULL,
        task_name STRING(256) NOT NULL,
        lane STRING(128) NOT NULL,
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
        lease_key STRING(256) NOT NULL,
        owner_id STRING(128) NOT NULL,
        expires_at FLOAT64 NOT NULL,
        last_slot STRING(64)
    ) PRIMARY KEY (lease_key)
    """,
    """
    CREATE TABLE IF NOT EXISTS vertebra_queue_unique_keys (
        unique_key STRING(256) NOT NULL,
        expires_at FLOAT64 NOT NULL
    ) PRIMARY KEY (unique_key)
    """,
)

SPANNER_QUEUE_STORE_DDL_SPANGRES: Final[tuple[str, ...]] = (
    """
    CREATE TABLE IF NOT EXISTS vertebra_queue_jobs (
        shard_id BIGINT NOT NULL,
        task_id VARCHAR(64) NOT NULL,
        task_name VARCHAR(256) NOT NULL,
        lane VARCHAR(128) NOT NULL,
        payload TEXT NOT NULL,
        status VARCHAR(32) NOT NULL,
        attempt BIGINT NOT NULL,
        max_retries BIGINT NOT NULL,
        scheduled_at DOUBLE PRECISION,
        enqueued_at DOUBLE PRECISION NOT NULL,
        locked_until DOUBLE PRECISION,
        consumer_name VARCHAR(128),
        receipt_handle VARCHAR(192),
        idempotency_key VARCHAR(256),
        traceparent VARCHAR(256),
        headers TEXT,
        last_error TEXT,
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
        traceparent,
        headers,
        last_error
    )
    WHERE status IS NOT NULL
    """,
    """
    CREATE TABLE IF NOT EXISTS vertebra_queue_dlq (
        dlq_id VARCHAR(64) NOT NULL PRIMARY KEY,
        shard_id BIGINT NOT NULL,
        task_id VARCHAR(64) NOT NULL,
        task_name VARCHAR(256) NOT NULL,
        lane VARCHAR(128) NOT NULL,
        payload TEXT NOT NULL,
        error_type VARCHAR(128) NOT NULL,
        error_message TEXT NOT NULL,
        failed_at DOUBLE PRECISION NOT NULL,
        expires_at TIMESTAMPTZ
    ) TTL INTERVAL '0 days' ON expires_at
    """,
    """
    CREATE TABLE IF NOT EXISTS vertebra_queue_leases (
        lease_key VARCHAR(256) NOT NULL PRIMARY KEY,
        owner_id VARCHAR(128) NOT NULL,
        expires_at DOUBLE PRECISION NOT NULL,
        last_slot VARCHAR(64)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS vertebra_queue_unique_keys (
        unique_key VARCHAR(256) NOT NULL PRIMARY KEY,
        expires_at DOUBLE PRECISION NOT NULL
    )
    """,
)


def compute_spanner_shard_id(task_id: str, shard_count: int = 16) -> int:
    """Compute a deterministic hash shard prefix to prevent monotonic primary-key hotspots."""
    digest = hashlib.blake2b(task_id.encode("utf-8"), digest_size=8).digest()
    raw = int.from_bytes(digest, "little", signed=False)
    return raw % max(1, shard_count)


def format_spanner_receipt_handle(shard_id: int, task_id: str, attempt: int) -> str:
    """Encode a deterministic stateless receipt token carrying ``(shard_id, task_id, attempt)``."""
    return f"spn:{shard_id}:{task_id}:{attempt}"


def parse_spanner_receipt_handle(
    receipt_handle: str,
    shard_count: int = 16,
) -> tuple[int, str, int] | None:
    """Decode a deterministic Spanner receipt token into ``(shard_id, task_id, attempt)``."""
    if not receipt_handle.startswith("spn:"):
        return None
    rest = receipt_handle[4:]
    shard_part, sep_first, tail = rest.partition(":")
    if not sep_first:
        return None
    task_part, sep_last, attempt_part = tail.rpartition(":")
    if not sep_last or not task_part:
        return None
    try:
        parsed_shard = int(shard_part)
        parsed_attempt = int(attempt_part)
    except ValueError:
        return None
    if parsed_shard < 0:
        parsed_shard = compute_spanner_shard_id(task_part, shard_count)
    return parsed_shard, task_part, max(1, parsed_attempt)


class SyncSpannerQueueStore(SyncQueueStore):
    """Synchronous Cloud Spanner queue store backed by interactive ``ReadWrite`` transactions."""

    __slots__ = ("_cleanup_on_ack", "_dialect", "_driver", "_shard_count", "_table_name")

    def __init__(
        self,
        driver: SyncSpannerDriver,
        *,
        dialect: Literal["googlesql", "spangres"] | None = None,
        shard_count: int = 16,
        cleanup_on_ack: bool = True,
        table_name: str = "vertebra_queue_jobs",
    ) -> None:
        """Initialize the synchronous Spanner queue store."""
        self._driver = driver
        resolved_dialect = dialect or getattr(driver, "dialect", "googlesql")
        self._dialect: Literal["googlesql", "spangres"] = (
            "spangres" if str(resolved_dialect).lower() in {"spangres", "postgres", "postgresql", "pg"} else "googlesql"
        )
        self._shard_count = max(1, int(shard_count))
        self._cleanup_on_ack = cleanup_on_ack
        self._table_name = validate_identifier(table_name, "table_name")

    @property
    def dialect(self) -> Literal["googlesql", "spangres"]:
        """Return the active Spanner SQL dialect."""
        return self._dialect

    @property
    def shard_count(self) -> int:
        """Return the configured primary-key hash shard count."""
        return self._shard_count

    def ensure_schema(self) -> None:
        """Idempotently create queue, index, DLQ, lease, and unique-key tables."""
        statements = (
            SPANNER_QUEUE_STORE_DDL_SPANGRES if self._dialect == "spangres" else SPANNER_QUEUE_STORE_DDL_GOOGLESQL
        )
        for ddl in statements:
            rendered = ddl.replace("vertebra_queue_jobs", self._table_name)
            self._driver.execute(rendered)

    def enqueue(self, entry: QueueEntry) -> str:
        """Enqueue a single job entry inside a Spanner transaction and return its ``job_id``."""
        jid = entry.job_id or uuid4().hex
        shard_id = compute_spanner_shard_id(jid, self._shard_count)
        now = time.time()
        sched_s = entry.scheduled_at_ms / 1000.0 if entry.scheduled_at_ms is not None else None
        payload_str = _encode_payload(entry.payload)
        headers_str = _serialize_headers(entry.headers)
        idem_key = entry.headers.get("idempotency_key") or entry.headers.get("key")
        traceparent = entry.headers.get("traceparent")
        initial_attempt = max(0, int(entry.attempt) - 1)
        max_retries = max(1, int(entry.max_attempts))
        is_pg = self._dialect == "spangres"

        def _tx_enqueue(tx: SyncSpannerTransaction) -> str:
            if idem_key:
                dup_sql, dup_params = _build_idempotency_lookup(self._table_name, idem_key, is_pg=is_pg)
                existing = tx.select_one_or_none(dup_sql, dup_params)
                if existing is not None:
                    return str(existing["task_id"])
            ins_sql, ins_params = _build_insert_job(
                self._table_name,
                shard_id=shard_id,
                task_id=jid,
                task_name=entry.task_name,
                lane=entry.queue,
                payload=payload_str,
                attempt=initial_attempt,
                max_retries=max_retries,
                scheduled_at=sched_s,
                enqueued_at=now,
                idempotency_key=idem_key,
                traceparent=traceparent,
                headers=headers_str,
                is_pg=is_pg,
            )
            tx.execute(ins_sql, ins_params)
            return jid

        return cast("str", self._driver.run_in_transaction(_tx_enqueue))

    def enqueue_many(self, entries: Sequence[QueueEntry]) -> list[str]:
        """Bulk-enqueue job entries inside one interactive ``ReadWrite`` transaction."""
        if not entries:
            return []
        now = time.time()
        is_pg = self._dialect == "spangres"

        def _tx_enqueue_many(tx: SyncSpannerTransaction) -> list[str]:
            assigned: list[str] = []
            batch_statements: list[tuple[str, Any]] = []
            for entry in entries:
                jid = entry.job_id or uuid4().hex
                shard_id = compute_spanner_shard_id(jid, self._shard_count)
                sched_s = entry.scheduled_at_ms / 1000.0 if entry.scheduled_at_ms is not None else None
                idem_key = entry.headers.get("idempotency_key") or entry.headers.get("key")
                if idem_key:
                    dup_sql, dup_params = _build_idempotency_lookup(self._table_name, idem_key, is_pg=is_pg)
                    existing = tx.select_one_or_none(dup_sql, dup_params)
                    if existing is not None:
                        assigned.append(str(existing["task_id"]))
                        continue
                ins_sql, ins_params = _build_insert_job(
                    self._table_name,
                    shard_id=shard_id,
                    task_id=jid,
                    task_name=entry.task_name,
                    lane=entry.queue,
                    payload=_encode_payload(entry.payload),
                    attempt=max(0, int(entry.attempt) - 1),
                    max_retries=max(1, int(entry.max_attempts)),
                    scheduled_at=sched_s,
                    enqueued_at=now,
                    idempotency_key=idem_key,
                    traceparent=entry.headers.get("traceparent"),
                    headers=_serialize_headers(entry.headers),
                    is_pg=is_pg,
                )
                batch_statements.append((ins_sql, ins_params))
                assigned.append(jid)
            if batch_statements:
                tx.execute_batch_dml(batch_statements)
            return assigned

        return cast("list[str]", self._driver.run_in_transaction(_tx_enqueue_many))

    def claim(
        self,
        queues: Sequence[str],
        *,
        limit: int = 1,
        worker_id: str,
        lease_seconds: float = 30.0,
    ) -> list[QueueEntry]:
        """Atomically lock and claim ready jobs inside an interactive ``ReadWrite`` transaction."""
        if not queues or limit <= 0:
            return []
        is_pg = self._dialect == "spangres"
        lane_list = [str(q) for q in queues]
        batch_limit = int(limit)

        def _tx_claim(tx: SyncSpannerTransaction) -> list[QueueEntry]:
            now = time.time()
            locked_until = now + float(lease_seconds)
            sel_sql, sel_params = _build_claim_select(
                self._table_name,
                lanes=lane_list,
                now_ts=now,
                batch_size=batch_limit,
                is_pg=is_pg,
            )
            candidates = tx.select(sel_sql, sel_params)
            if not candidates:
                return []
            claimed: list[QueueEntry] = []
            for cand in candidates:
                c_shard = int(cand["shard_id"])
                c_task_id = str(cand["task_id"])
                next_attempt = int(cand.get("attempt", 0)) + 1
                handle = format_spanner_receipt_handle(c_shard, c_task_id, next_attempt)
                upd_sql, upd_params = _build_claim_update(
                    self._table_name,
                    shard_id=c_shard,
                    task_id=c_task_id,
                    locked_until=locked_until,
                    consumer_name=worker_id,
                    receipt_handle=handle,
                    is_pg=is_pg,
                )
                updated_rows = tx.select(upd_sql, upd_params)
                if updated_rows:
                    claimed.append(_row_to_entry(updated_rows[0]))
            return claimed

        return cast("list[QueueEntry]", self._driver.run_in_transaction(_tx_claim))

    def claim_job(
        self,
        job_id: str,
        *,
        worker_id: str,
        lease_seconds: float = 30.0,
        expected_attempt: int | None = None,
    ) -> QueueEntry | None:
        """Atomically lock and claim a specific ready job by ``job_id``."""
        shard_id = compute_spanner_shard_id(job_id, self._shard_count)
        is_pg = self._dialect == "spangres"

        def _tx_claim_job(tx: SyncSpannerTransaction) -> QueueEntry | None:
            sel_sql, sel_params = _build_job_for_update(
                self._table_name,
                shard_id=shard_id,
                task_id=job_id,
                is_pg=is_pg,
            )
            row = tx.select_one_or_none(sel_sql, sel_params)
            if row is None:
                return None
            now = time.time()
            status = str(row.get("status", ""))
            sched = row.get("scheduled_at")
            locked = row.get("locked_until")
            is_ready = status == "pending" and (sched is None or float(sched) <= now)
            is_expired = (
                status == "running"
                and locked is not None
                and float(locked) <= now
                and int(row.get("attempt", 0)) < int(row.get("max_retries", 1))
            )
            if not (is_ready or is_expired):
                return None
            next_attempt = int(row.get("attempt", 0)) + 1
            if expected_attempt is not None and next_attempt != expected_attempt:
                return None
            locked_until = now + float(lease_seconds)
            handle = format_spanner_receipt_handle(shard_id, job_id, next_attempt)
            upd_sql, upd_params = _build_claim_update(
                self._table_name,
                shard_id=shard_id,
                task_id=job_id,
                locked_until=locked_until,
                consumer_name=worker_id,
                receipt_handle=handle,
                is_pg=is_pg,
            )
            updated_rows = tx.select(upd_sql, upd_params)
            return _row_to_entry(updated_rows[0]) if updated_rows else None

        return cast("QueueEntry | None", self._driver.run_in_transaction(_tx_claim_job))

    def ack(self, job_id: str, *, attempt: int) -> bool:
        """Acknowledge a fenced ``(job_id, attempt)`` claim."""
        shard_id = compute_spanner_shard_id(job_id, self._shard_count)
        sql, params = _build_ack_dml(
            self._table_name,
            shard_id=shard_id,
            task_id=job_id,
            attempt=attempt,
            cleanup=self._cleanup_on_ack,
            is_pg=self._dialect == "spangres",
        )
        return self._driver.execute(sql, params) > 0

    def nack(
        self,
        job_id: str,
        *,
        attempt: int,
        error: str,
        retry_delay_s: float | None = None,
    ) -> bool:
        """Record a failed ``(job_id, attempt)`` execution and schedule retry or dead-lettering."""
        outcome = self.nack_with_dlq(
            job_id,
            attempt=attempt,
            error_type="TaskExecutionError",
            error_message=error,
            retry_delay_s=retry_delay_s,
            force_dlq=retry_delay_s is None,
        )
        return outcome[0]

    def nack_with_dlq(
        self,
        job_id: str,
        *,
        attempt: int,
        error_type: str,
        error_message: str,
        retry_delay_s: float | None = None,
        force_dlq: bool = False,
    ) -> tuple[bool, dict[str, Any] | None]:
        """Execute attempt-fenced retry or DLQ routing inside one ``ReadWrite`` transaction."""
        shard_id = compute_spanner_shard_id(job_id, self._shard_count)
        is_pg = self._dialect == "spangres"
        combined_error = f"{error_type}: {error_message}" if error_type else error_message

        def _tx_nack(tx: SyncSpannerTransaction) -> tuple[bool, dict[str, Any] | None]:
            sel_sql, sel_params = _build_job_for_update(
                self._table_name,
                shard_id=shard_id,
                task_id=job_id,
                is_pg=is_pg,
            )
            row = tx.select_one_or_none(sel_sql, sel_params)
            if row is None or str(row.get("status")) != "running" or int(row.get("attempt", 0)) != attempt:
                return False, None
            now = time.time()
            max_retries = int(row.get("max_retries", 1))
            if force_dlq or retry_delay_s is None or attempt >= max_retries:
                dlq_id = uuid4().hex
                dlq_sql, dlq_params = _build_insert_dlq(
                    dlq_id=dlq_id,
                    shard_id=shard_id,
                    task_id=job_id,
                    task_name=str(row["task_name"]),
                    lane=str(row["lane"]),
                    payload=str(row["payload"]),
                    error_type=error_type,
                    error_message=error_message,
                    failed_at=now,
                    is_pg=is_pg,
                )
                tx.execute(dlq_sql, dlq_params)
                dead_sql, dead_params = _build_mark_dead_letter(
                    self._table_name,
                    shard_id=shard_id,
                    task_id=job_id,
                    attempt=attempt,
                    last_error=combined_error,
                    is_pg=is_pg,
                )
                tx.execute(dead_sql, dead_params)
                return True, {
                    "dlq_id": dlq_id,
                    "shard_id": shard_id,
                    "task_id": job_id,
                    "task_name": str(row["task_name"]),
                    "lane": str(row["lane"]),
                    "payload": str(row["payload"]),
                    "error_type": error_type,
                    "error_message": error_message,
                    "failed_at": now,
                }
            sched_at = now + max(0.0, float(retry_delay_s))
            retry_sql, retry_params = _build_reschedule_job(
                self._table_name,
                shard_id=shard_id,
                task_id=job_id,
                attempt=attempt,
                scheduled_at=sched_at,
                last_error=combined_error,
                is_pg=is_pg,
            )
            tx.execute(retry_sql, retry_params)
            return True, None

        return cast("tuple[bool, dict[str, Any] | None]", self._driver.run_in_transaction(_tx_nack))

    def heartbeat(
        self,
        job_ids: Sequence[str],
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> int:
        """Extend visibility leases for active ``(job_ids, worker_id)`` claims."""
        if not job_ids:
            return 0
        now = time.time()
        locked_until = now + float(lease_seconds)
        is_pg = self._dialect == "spangres"
        statements = [
            _build_heartbeat_dml(
                self._table_name,
                shard_id=compute_spanner_shard_id(jid, self._shard_count),
                task_id=jid,
                worker_id=worker_id,
                locked_until=locked_until,
                is_pg=is_pg,
            )
            for jid in job_ids
        ]
        counts = self._driver.execute_batch_dml(statements)
        return sum(counts)

    def reap_stale(self, *, lease_seconds: float = 300.0) -> int:
        """Reap expired ``running`` claims back into ``pending`` status."""
        _ = lease_seconds
        now = time.time()
        sql, params = _build_reap_stale_dml(self._table_name, now_ts=now, is_pg=self._dialect == "spangres")
        return self._driver.execute(sql, params)

    def promote_scheduled(self, *, limit: int = 100) -> int:
        """Clear ``scheduled_at`` on up to ``limit`` due ``pending`` jobs."""
        is_pg = self._dialect == "spangres"
        max_rows = max(1, int(limit))

        def _tx_promote(tx: SyncSpannerTransaction) -> int:
            now = time.time()
            sel_sql, sel_params = _build_due_scheduled_select(
                self._table_name,
                now_ts=now,
                limit=max_rows,
                is_pg=is_pg,
            )
            due = tx.select(sel_sql, sel_params)
            if not due:
                return 0
            stmts = [
                _build_clear_scheduled_dml(
                    self._table_name,
                    shard_id=int(r["shard_id"]),
                    task_id=str(r["task_id"]),
                    is_pg=is_pg,
                )
                for r in due
            ]
            return sum(tx.execute_batch_dml(stmts))

        return int(self._driver.run_in_transaction(_tx_promote))

    def cancel(self, job_id: str) -> bool:
        """Cancel a ``pending`` or ``running`` job."""
        shard_id = compute_spanner_shard_id(job_id, self._shard_count)
        sql, params = _build_cancel_dml(
            self._table_name,
            shard_id=shard_id,
            task_id=job_id,
            is_pg=self._dialect == "spangres",
        )
        return self._driver.execute(sql, params) > 0

    def get_entry(self, job_id: str) -> QueueEntry | None:
        """Retrieve a queue entry by ``job_id``."""
        shard_id = compute_spanner_shard_id(job_id, self._shard_count)
        sql, params = _build_get_job(
            self._table_name,
            shard_id=shard_id,
            task_id=job_id,
            is_pg=self._dialect == "spangres",
        )
        row = self._driver.select_one_or_none(sql, params)
        return _row_to_entry(row) if row is not None else None

    def get_entry_status(self, job_id: str) -> str | None:
        """Retrieve the current status string of ``job_id``, or ``None`` if absent."""
        shard_id = compute_spanner_shard_id(job_id, self._shard_count)
        sql, params = _build_get_job(
            self._table_name,
            shard_id=shard_id,
            task_id=job_id,
            is_pg=self._dialect == "spangres",
        )
        row = self._driver.select_one_or_none(sql, params)
        if row is None or row.get("status") is None:
            return None
        return str(row["status"])

    def list_entries(
        self,
        queue: str,
        *,
        status: str | None = None,
        limit: int = 100,
    ) -> list[QueueEntry]:
        """List queue entries in ``queue`` matching optional ``status``."""
        sql, params = _build_list_jobs(
            self._table_name,
            lane=queue,
            status=_normalize_status_filter(status),
            limit=limit,
            is_pg=self._dialect == "spangres",
        )
        rows = self._driver.select(sql, params)
        return [_row_to_entry(r) for r in rows]

    def next_due_delay(self, queues: Sequence[str]) -> float | None:
        """Return seconds until the earliest scheduled job in ``queues`` is due, or ``None``."""
        if not queues:
            return None
        now = time.time()
        sql, params = _build_next_due_query(
            self._table_name,
            lanes=[str(q) for q in queues],
            now_ts=now,
            is_pg=self._dialect == "spangres",
        )
        row = self._driver.select_one_or_none(sql, params)
        if row is None or row.get("min_sched") is None:
            return None
        return max(0.0, float(row["min_sched"]) - now)

    def reserve_unique_key(self, key: str, *, ttl_seconds: float) -> bool:
        """Conditionally reserve ``key`` for ``ttl_seconds`` without overwriting unexpired keys."""
        is_pg = self._dialect == "spangres"
        if is_pg:
            now = time.time()
            expires_at = now + float(ttl_seconds)
            sql = """
            INSERT INTO vertebra_queue_unique_keys (unique_key, expires_at)
            VALUES ($1, $2)
            ON CONFLICT (unique_key) DO UPDATE
            SET expires_at = EXCLUDED.expires_at
            WHERE vertebra_queue_unique_keys.expires_at <= $3
            RETURNING unique_key
            """
            rows = self._driver.select(sql, (key, expires_at, now))
            return len(rows) > 0

        def _tx_reserve(tx: SyncSpannerTransaction) -> bool:
            now_tx = time.time()
            expires_tx = now_tx + float(ttl_seconds)
            existing = tx.select_one_or_none(
                "SELECT unique_key, expires_at FROM vertebra_queue_unique_keys WHERE unique_key = @key FOR UPDATE",
                {"key": key},
            )
            if existing is not None and float(existing["expires_at"]) > now_tx:
                return False
            if existing is None:
                tx.execute(
                    "INSERT INTO vertebra_queue_unique_keys (unique_key, expires_at) VALUES (@key, @expires_at)",
                    {"key": key, "expires_at": expires_tx},
                )
            else:
                tx.execute(
                    "UPDATE vertebra_queue_unique_keys SET expires_at = @expires_at WHERE unique_key = @key",
                    {"key": key, "expires_at": expires_tx},
                )
            return True

        return bool(self._driver.run_in_transaction(_tx_reserve))

    def release_unique_key(self, key: str) -> bool:
        """Release an idempotency key reservation."""
        if self._dialect == "spangres":
            return self._driver.execute("DELETE FROM vertebra_queue_unique_keys WHERE unique_key = $1", (key,)) > 0
        return self._driver.execute("DELETE FROM vertebra_queue_unique_keys WHERE unique_key = @key", {"key": key}) > 0

    def acquire_lease(self, lease_key: str, *, owner_id: str, ttl_seconds: float) -> bool:
        """Acquire or renew ``lease_key`` only when expired or already owned by ``owner_id``."""
        return (
            self.acquire_lease_record(
                lease_key,
                owner_id=owner_id,
                ttl_seconds=ttl_seconds,
            )
            is not None
        )

    def acquire_lease_record(
        self,
        lease_key: str,
        *,
        owner_id: str,
        ttl_seconds: float,
        slot: str | None = None,
    ) -> dict[str, Any] | None:
        """Acquire or renew ``lease_key`` without stealing active foreign leases and return lease metadata."""
        is_pg = self._dialect == "spangres"
        if is_pg:
            now = time.time()
            expires_at = now + float(ttl_seconds)
            sql = """
            INSERT INTO vertebra_queue_leases (lease_key, owner_id, expires_at, last_slot)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (lease_key) DO UPDATE
            SET owner_id = EXCLUDED.owner_id,
                expires_at = EXCLUDED.expires_at,
                last_slot = COALESCE(EXCLUDED.last_slot, vertebra_queue_leases.last_slot)
            WHERE vertebra_queue_leases.expires_at <= $5
               OR vertebra_queue_leases.owner_id = EXCLUDED.owner_id
            RETURNING lease_key, owner_id, expires_at, last_slot
            """
            rows = self._driver.select(sql, (lease_key, owner_id, expires_at, slot, now))
            return dict(rows[0]) if rows else None

        def _tx_lease(tx: SyncSpannerTransaction) -> dict[str, Any] | None:
            now_tx = time.time()
            expires_tx = now_tx + float(ttl_seconds)
            existing = tx.select_one_or_none(
                "SELECT lease_key, owner_id, expires_at, last_slot "
                "FROM vertebra_queue_leases WHERE lease_key = @lease_key FOR UPDATE",
                {"lease_key": lease_key},
            )
            if (
                existing is not None
                and float(existing["expires_at"]) > now_tx
                and str(existing["owner_id"]) != owner_id
            ):
                return None
            resolved_slot = slot if slot is not None else (existing.get("last_slot") if existing else None)
            if existing is None:
                tx.execute(
                    "INSERT INTO vertebra_queue_leases (lease_key, owner_id, expires_at, last_slot) "
                    "VALUES (@lease_key, @owner_id, @expires_at, @last_slot)",
                    {
                        "lease_key": lease_key,
                        "owner_id": owner_id,
                        "expires_at": expires_tx,
                        "last_slot": resolved_slot,
                    },
                )
            else:
                tx.execute(
                    "UPDATE vertebra_queue_leases "
                    "SET owner_id = @owner_id, expires_at = @expires_at, last_slot = @last_slot "
                    "WHERE lease_key = @lease_key",
                    {
                        "lease_key": lease_key,
                        "owner_id": owner_id,
                        "expires_at": expires_tx,
                        "last_slot": resolved_slot,
                    },
                )
            return {
                "lease_key": lease_key,
                "owner_id": owner_id,
                "expires_at": expires_tx,
                "last_slot": resolved_slot,
            }

        return cast("dict[str, Any] | None", self._driver.run_in_transaction(_tx_lease))

    def release_lease(self, lease_key: str, *, owner_id: str) -> bool:
        """Release a held distributed leader lease matching ``owner_id``."""
        if self._dialect == "spangres":
            sql = "DELETE FROM vertebra_queue_leases WHERE lease_key = $1 AND owner_id = $2"
            return self._driver.execute(sql, (lease_key, owner_id)) > 0
        sql = "DELETE FROM vertebra_queue_leases WHERE lease_key = @lease_key AND owner_id = @owner_id"
        return self._driver.execute(sql, {"lease_key": lease_key, "owner_id": owner_id}) > 0

    def list_dlq_rows(self, *, limit: int = 100) -> list[dict[str, Any]]:
        """List dead-letter rows ordered by ``failed_at DESC``."""
        if self._dialect == "spangres":
            return self._driver.select(
                "SELECT * FROM vertebra_queue_dlq ORDER BY failed_at DESC LIMIT $1",
                (limit,),
            )
        return self._driver.select(
            "SELECT * FROM vertebra_queue_dlq ORDER BY failed_at DESC LIMIT @limit",
            {"limit": limit},
        )

    def pop_dlq_row(self, dlq_id: str) -> dict[str, Any] | None:
        """Atomically fetch and remove a dead-letter entry by ``dlq_id``."""
        is_pg = self._dialect == "spangres"

        def _tx_pop(tx: SyncSpannerTransaction) -> dict[str, Any] | None:
            if is_pg:
                row = tx.select_one_or_none(
                    "SELECT * FROM vertebra_queue_dlq WHERE dlq_id = $1 FOR UPDATE",
                    (dlq_id,),
                )
                if row is None:
                    return None
                tx.execute("DELETE FROM vertebra_queue_dlq WHERE dlq_id = $1", (dlq_id,))
                tx.execute(
                    f"DELETE FROM {self._table_name} WHERE shard_id = $1 AND task_id = $2 AND status = 'dead_letter'",
                    (int(row["shard_id"]), str(row["task_id"])),
                )
                return dict(row)
            row = tx.select_one_or_none(
                "SELECT * FROM vertebra_queue_dlq WHERE dlq_id = @dlq_id FOR UPDATE",
                {"dlq_id": dlq_id},
            )
            if row is None:
                return None
            tx.execute("DELETE FROM vertebra_queue_dlq WHERE dlq_id = @dlq_id", {"dlq_id": dlq_id})
            tx.execute(
                f"DELETE FROM {self._table_name} "
                "WHERE shard_id = @shard_id AND task_id = @task_id AND status = 'dead_letter'",
                {"shard_id": int(row["shard_id"]), "task_id": str(row["task_id"])},
            )
            return dict(row)

        return cast("dict[str, Any] | None", self._driver.run_in_transaction(_tx_pop))

    def close(self) -> None:
        """Close the underlying synchronous Spanner driver."""
        self._driver.close()


class AsyncSpannerQueueStore(AsyncQueueStore):
    """Asynchronous Cloud Spanner queue store backed by interactive ``ReadWrite`` transactions."""

    __slots__ = ("_cleanup_on_ack", "_dialect", "_driver", "_shard_count", "_table_name")

    def __init__(
        self,
        driver: AsyncSpannerDriver,
        *,
        dialect: Literal["googlesql", "spangres"] | None = None,
        shard_count: int = 16,
        cleanup_on_ack: bool = True,
        table_name: str = "vertebra_queue_jobs",
    ) -> None:
        """Initialize the asynchronous Spanner queue store."""
        self._driver = driver
        resolved_dialect = dialect or getattr(driver, "dialect", "googlesql")
        self._dialect: Literal["googlesql", "spangres"] = (
            "spangres" if str(resolved_dialect).lower() in {"spangres", "postgres", "postgresql", "pg"} else "googlesql"
        )
        self._shard_count = max(1, int(shard_count))
        self._cleanup_on_ack = cleanup_on_ack
        self._table_name = validate_identifier(table_name, "table_name")

    @property
    def dialect(self) -> Literal["googlesql", "spangres"]:
        """Return the active Spanner SQL dialect."""
        return self._dialect

    @property
    def shard_count(self) -> int:
        """Return the configured primary-key hash shard count."""
        return self._shard_count

    async def ensure_schema(self) -> None:
        """Idempotently create queue, index, DLQ, lease, and unique-key tables."""
        statements = (
            SPANNER_QUEUE_STORE_DDL_SPANGRES if self._dialect == "spangres" else SPANNER_QUEUE_STORE_DDL_GOOGLESQL
        )
        for ddl in statements:
            rendered = ddl.replace("vertebra_queue_jobs", self._table_name)
            await self._driver.execute(rendered)

    async def enqueue(self, entry: QueueEntry) -> str:
        """Enqueue a single job entry inside an async Spanner transaction and return its ``job_id``."""
        jid = entry.job_id or uuid4().hex
        shard_id = compute_spanner_shard_id(jid, self._shard_count)
        now = time.time()
        sched_s = entry.scheduled_at_ms / 1000.0 if entry.scheduled_at_ms is not None else None
        payload_str = _encode_payload(entry.payload)
        headers_str = _serialize_headers(entry.headers)
        idem_key = entry.headers.get("idempotency_key") or entry.headers.get("key")
        traceparent = entry.headers.get("traceparent")
        initial_attempt = max(0, int(entry.attempt) - 1)
        max_retries = max(1, int(entry.max_attempts))
        is_pg = self._dialect == "spangres"

        async def _tx_enqueue(tx: AsyncSpannerTransaction) -> str:
            if idem_key:
                dup_sql, dup_params = _build_idempotency_lookup(self._table_name, idem_key, is_pg=is_pg)
                existing = await tx.select_one_or_none(dup_sql, dup_params)
                if existing is not None:
                    return str(existing["task_id"])
            ins_sql, ins_params = _build_insert_job(
                self._table_name,
                shard_id=shard_id,
                task_id=jid,
                task_name=entry.task_name,
                lane=entry.queue,
                payload=payload_str,
                attempt=initial_attempt,
                max_retries=max_retries,
                scheduled_at=sched_s,
                enqueued_at=now,
                idempotency_key=idem_key,
                traceparent=traceparent,
                headers=headers_str,
                is_pg=is_pg,
            )
            await tx.execute(ins_sql, ins_params)
            return jid

        return cast("str", await self._driver.run_in_transaction(_tx_enqueue))

    async def enqueue_many(self, entries: Sequence[QueueEntry]) -> list[str]:
        """Bulk-enqueue job entries inside one interactive ``ReadWrite`` transaction."""
        if not entries:
            return []
        now = time.time()
        is_pg = self._dialect == "spangres"

        async def _tx_enqueue_many(tx: AsyncSpannerTransaction) -> list[str]:
            assigned: list[str] = []
            batch_statements: list[tuple[str, Any]] = []
            for entry in entries:
                jid = entry.job_id or uuid4().hex
                shard_id = compute_spanner_shard_id(jid, self._shard_count)
                sched_s = entry.scheduled_at_ms / 1000.0 if entry.scheduled_at_ms is not None else None
                idem_key = entry.headers.get("idempotency_key") or entry.headers.get("key")
                if idem_key:
                    dup_sql, dup_params = _build_idempotency_lookup(self._table_name, idem_key, is_pg=is_pg)
                    existing = await tx.select_one_or_none(dup_sql, dup_params)
                    if existing is not None:
                        assigned.append(str(existing["task_id"]))
                        continue
                ins_sql, ins_params = _build_insert_job(
                    self._table_name,
                    shard_id=shard_id,
                    task_id=jid,
                    task_name=entry.task_name,
                    lane=entry.queue,
                    payload=_encode_payload(entry.payload),
                    attempt=max(0, int(entry.attempt) - 1),
                    max_retries=max(1, int(entry.max_attempts)),
                    scheduled_at=sched_s,
                    enqueued_at=now,
                    idempotency_key=idem_key,
                    traceparent=entry.headers.get("traceparent"),
                    headers=_serialize_headers(entry.headers),
                    is_pg=is_pg,
                )
                batch_statements.append((ins_sql, ins_params))
                assigned.append(jid)
            if batch_statements:
                await tx.execute_batch_dml(batch_statements)
            return assigned

        return cast("list[str]", await self._driver.run_in_transaction(_tx_enqueue_many))

    async def claim(
        self,
        queues: Sequence[str],
        *,
        limit: int = 1,
        worker_id: str,
        lease_seconds: float = 30.0,
    ) -> list[QueueEntry]:
        """Atomically lock and claim ready jobs inside an async interactive ``ReadWrite`` transaction."""
        if not queues or limit <= 0:
            return []
        is_pg = self._dialect == "spangres"
        lane_list = [str(q) for q in queues]
        batch_limit = int(limit)

        async def _tx_claim(tx: AsyncSpannerTransaction) -> list[QueueEntry]:
            now = time.time()
            locked_until = now + float(lease_seconds)
            sel_sql, sel_params = _build_claim_select(
                self._table_name,
                lanes=lane_list,
                now_ts=now,
                batch_size=batch_limit,
                is_pg=is_pg,
            )
            candidates = await tx.select(sel_sql, sel_params)
            if not candidates:
                return []
            claimed: list[QueueEntry] = []
            for cand in candidates:
                c_shard = int(cand["shard_id"])
                c_task_id = str(cand["task_id"])
                next_attempt = int(cand.get("attempt", 0)) + 1
                handle = format_spanner_receipt_handle(c_shard, c_task_id, next_attempt)
                upd_sql, upd_params = _build_claim_update(
                    self._table_name,
                    shard_id=c_shard,
                    task_id=c_task_id,
                    locked_until=locked_until,
                    consumer_name=worker_id,
                    receipt_handle=handle,
                    is_pg=is_pg,
                )
                updated_rows = await tx.select(upd_sql, upd_params)
                if updated_rows:
                    claimed.append(_row_to_entry(updated_rows[0]))
            return claimed

        return cast("list[QueueEntry]", await self._driver.run_in_transaction(_tx_claim))

    async def claim_job(
        self,
        job_id: str,
        *,
        worker_id: str,
        lease_seconds: float = 30.0,
        expected_attempt: int | None = None,
    ) -> QueueEntry | None:
        """Atomically lock and claim a specific ready job asynchronously by ``job_id``."""
        shard_id = compute_spanner_shard_id(job_id, self._shard_count)
        is_pg = self._dialect == "spangres"

        async def _tx_claim_job(tx: AsyncSpannerTransaction) -> QueueEntry | None:
            sel_sql, sel_params = _build_job_for_update(
                self._table_name,
                shard_id=shard_id,
                task_id=job_id,
                is_pg=is_pg,
            )
            row = await tx.select_one_or_none(sel_sql, sel_params)
            if row is None:
                return None
            now = time.time()
            status = str(row.get("status", ""))
            sched = row.get("scheduled_at")
            locked = row.get("locked_until")
            is_ready = status == "pending" and (sched is None or float(sched) <= now)
            is_expired = (
                status == "running"
                and locked is not None
                and float(locked) <= now
                and int(row.get("attempt", 0)) < int(row.get("max_retries", 1))
            )
            if not (is_ready or is_expired):
                return None
            next_attempt = int(row.get("attempt", 0)) + 1
            if expected_attempt is not None and next_attempt != expected_attempt:
                return None
            locked_until = now + float(lease_seconds)
            handle = format_spanner_receipt_handle(shard_id, job_id, next_attempt)
            upd_sql, upd_params = _build_claim_update(
                self._table_name,
                shard_id=shard_id,
                task_id=job_id,
                locked_until=locked_until,
                consumer_name=worker_id,
                receipt_handle=handle,
                is_pg=is_pg,
            )
            updated_rows = await tx.select(upd_sql, upd_params)
            return _row_to_entry(updated_rows[0]) if updated_rows else None

        return cast("QueueEntry | None", await self._driver.run_in_transaction(_tx_claim_job))

    async def ack(self, job_id: str, *, attempt: int) -> bool:
        """Acknowledge a fenced ``(job_id, attempt)`` claim asynchronously."""
        shard_id = compute_spanner_shard_id(job_id, self._shard_count)
        sql, params = _build_ack_dml(
            self._table_name,
            shard_id=shard_id,
            task_id=job_id,
            attempt=attempt,
            cleanup=self._cleanup_on_ack,
            is_pg=self._dialect == "spangres",
        )
        return (await self._driver.execute(sql, params)) > 0

    async def nack(
        self,
        job_id: str,
        *,
        attempt: int,
        error: str,
        retry_delay_s: float | None = None,
    ) -> bool:
        """Record a failed ``(job_id, attempt)`` execution and schedule retry or dead-lettering."""
        outcome = await self.nack_with_dlq(
            job_id,
            attempt=attempt,
            error_type="TaskExecutionError",
            error_message=error,
            retry_delay_s=retry_delay_s,
            force_dlq=retry_delay_s is None,
        )
        return outcome[0]

    async def nack_with_dlq(
        self,
        job_id: str,
        *,
        attempt: int,
        error_type: str,
        error_message: str,
        retry_delay_s: float | None = None,
        force_dlq: bool = False,
    ) -> tuple[bool, dict[str, Any] | None]:
        """Execute attempt-fenced retry or DLQ routing inside one async ``ReadWrite`` transaction."""
        shard_id = compute_spanner_shard_id(job_id, self._shard_count)
        is_pg = self._dialect == "spangres"
        combined_error = f"{error_type}: {error_message}" if error_type else error_message

        async def _tx_nack(tx: AsyncSpannerTransaction) -> tuple[bool, dict[str, Any] | None]:
            sel_sql, sel_params = _build_job_for_update(
                self._table_name,
                shard_id=shard_id,
                task_id=job_id,
                is_pg=is_pg,
            )
            row = await tx.select_one_or_none(sel_sql, sel_params)
            if row is None or str(row.get("status")) != "running" or int(row.get("attempt", 0)) != attempt:
                return False, None
            now = time.time()
            max_retries = int(row.get("max_retries", 1))
            if force_dlq or retry_delay_s is None or attempt >= max_retries:
                dlq_id = uuid4().hex
                dlq_sql, dlq_params = _build_insert_dlq(
                    dlq_id=dlq_id,
                    shard_id=shard_id,
                    task_id=job_id,
                    task_name=str(row["task_name"]),
                    lane=str(row["lane"]),
                    payload=str(row["payload"]),
                    error_type=error_type,
                    error_message=error_message,
                    failed_at=now,
                    is_pg=is_pg,
                )
                await tx.execute(dlq_sql, dlq_params)
                dead_sql, dead_params = _build_mark_dead_letter(
                    self._table_name,
                    shard_id=shard_id,
                    task_id=job_id,
                    attempt=attempt,
                    last_error=combined_error,
                    is_pg=is_pg,
                )
                await tx.execute(dead_sql, dead_params)
                return True, {
                    "dlq_id": dlq_id,
                    "shard_id": shard_id,
                    "task_id": job_id,
                    "task_name": str(row["task_name"]),
                    "lane": str(row["lane"]),
                    "payload": str(row["payload"]),
                    "error_type": error_type,
                    "error_message": error_message,
                    "failed_at": now,
                }
            sched_at = now + max(0.0, float(retry_delay_s))
            retry_sql, retry_params = _build_reschedule_job(
                self._table_name,
                shard_id=shard_id,
                task_id=job_id,
                attempt=attempt,
                scheduled_at=sched_at,
                last_error=combined_error,
                is_pg=is_pg,
            )
            await tx.execute(retry_sql, retry_params)
            return True, None

        return cast("tuple[bool, dict[str, Any] | None]", await self._driver.run_in_transaction(_tx_nack))

    async def heartbeat(
        self,
        job_ids: Sequence[str],
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> int:
        """Extend visibility leases asynchronously for active ``(job_ids, worker_id)`` claims."""
        if not job_ids:
            return 0
        now = time.time()
        locked_until = now + float(lease_seconds)
        is_pg = self._dialect == "spangres"
        statements = [
            _build_heartbeat_dml(
                self._table_name,
                shard_id=compute_spanner_shard_id(jid, self._shard_count),
                task_id=jid,
                worker_id=worker_id,
                locked_until=locked_until,
                is_pg=is_pg,
            )
            for jid in job_ids
        ]
        counts = await self._driver.execute_batch_dml(statements)
        return sum(counts)

    async def reap_stale(self, *, lease_seconds: float = 300.0) -> int:
        """Reap expired ``running`` claims back into ``pending`` status."""
        _ = lease_seconds
        now = time.time()
        sql, params = _build_reap_stale_dml(self._table_name, now_ts=now, is_pg=self._dialect == "spangres")
        return await self._driver.execute(sql, params)

    async def promote_scheduled(self, *, limit: int = 100) -> int:
        """Clear ``scheduled_at`` asynchronously on up to ``limit`` due ``pending`` jobs."""
        is_pg = self._dialect == "spangres"
        max_rows = max(1, int(limit))

        async def _tx_promote(tx: AsyncSpannerTransaction) -> int:
            now = time.time()
            sel_sql, sel_params = _build_due_scheduled_select(
                self._table_name,
                now_ts=now,
                limit=max_rows,
                is_pg=is_pg,
            )
            due = await tx.select(sel_sql, sel_params)
            if not due:
                return 0
            stmts = [
                _build_clear_scheduled_dml(
                    self._table_name,
                    shard_id=int(r["shard_id"]),
                    task_id=str(r["task_id"]),
                    is_pg=is_pg,
                )
                for r in due
            ]
            return sum(await tx.execute_batch_dml(stmts))

        return int(await self._driver.run_in_transaction(_tx_promote))

    async def cancel(self, job_id: str) -> bool:
        """Cancel a ``pending`` or ``running`` job asynchronously."""
        shard_id = compute_spanner_shard_id(job_id, self._shard_count)
        sql, params = _build_cancel_dml(
            self._table_name,
            shard_id=shard_id,
            task_id=job_id,
            is_pg=self._dialect == "spangres",
        )
        return (await self._driver.execute(sql, params)) > 0

    async def get_entry(self, job_id: str) -> QueueEntry | None:
        """Retrieve a queue entry asynchronously by ``job_id``."""
        shard_id = compute_spanner_shard_id(job_id, self._shard_count)
        sql, params = _build_get_job(
            self._table_name,
            shard_id=shard_id,
            task_id=job_id,
            is_pg=self._dialect == "spangres",
        )
        row = await self._driver.select_one_or_none(sql, params)
        return _row_to_entry(row) if row is not None else None

    async def get_entry_status(self, job_id: str) -> str | None:
        """Retrieve the current status string of ``job_id`` asynchronously, or ``None`` if absent."""
        shard_id = compute_spanner_shard_id(job_id, self._shard_count)
        sql, params = _build_get_job(
            self._table_name,
            shard_id=shard_id,
            task_id=job_id,
            is_pg=self._dialect == "spangres",
        )
        row = await self._driver.select_one_or_none(sql, params)
        if row is None or row.get("status") is None:
            return None
        return str(row["status"])

    async def list_entries(
        self,
        queue: str,
        *,
        status: str | None = None,
        limit: int = 100,
    ) -> list[QueueEntry]:
        """List queue entries asynchronously in ``queue`` matching optional ``status``."""
        sql, params = _build_list_jobs(
            self._table_name,
            lane=queue,
            status=_normalize_status_filter(status),
            limit=limit,
            is_pg=self._dialect == "spangres",
        )
        rows = await self._driver.select(sql, params)
        return [_row_to_entry(r) for r in rows]

    async def next_due_delay(self, queues: Sequence[str]) -> float | None:
        """Return seconds until the earliest scheduled job in ``queues`` is due, or ``None``."""
        if not queues:
            return None
        now = time.time()
        sql, params = _build_next_due_query(
            self._table_name,
            lanes=[str(q) for q in queues],
            now_ts=now,
            is_pg=self._dialect == "spangres",
        )
        row = await self._driver.select_one_or_none(sql, params)
        if row is None or row.get("min_sched") is None:
            return None
        return max(0.0, float(row["min_sched"]) - now)

    async def reserve_unique_key(self, key: str, *, ttl_seconds: float) -> bool:
        """Conditionally reserve ``key`` asynchronously for ``ttl_seconds``."""
        is_pg = self._dialect == "spangres"
        if is_pg:
            now = time.time()
            expires_at = now + float(ttl_seconds)
            sql = """
            INSERT INTO vertebra_queue_unique_keys (unique_key, expires_at)
            VALUES ($1, $2)
            ON CONFLICT (unique_key) DO UPDATE
            SET expires_at = EXCLUDED.expires_at
            WHERE vertebra_queue_unique_keys.expires_at <= $3
            RETURNING unique_key
            """
            rows = await self._driver.select(sql, (key, expires_at, now))
            return len(rows) > 0

        async def _tx_reserve(tx: AsyncSpannerTransaction) -> bool:
            now_tx = time.time()
            expires_tx = now_tx + float(ttl_seconds)
            existing = await tx.select_one_or_none(
                "SELECT unique_key, expires_at FROM vertebra_queue_unique_keys WHERE unique_key = @key FOR UPDATE",
                {"key": key},
            )
            if existing is not None and float(existing["expires_at"]) > now_tx:
                return False
            if existing is None:
                await tx.execute(
                    "INSERT INTO vertebra_queue_unique_keys (unique_key, expires_at) VALUES (@key, @expires_at)",
                    {"key": key, "expires_at": expires_tx},
                )
            else:
                await tx.execute(
                    "UPDATE vertebra_queue_unique_keys SET expires_at = @expires_at WHERE unique_key = @key",
                    {"key": key, "expires_at": expires_tx},
                )
            return True

        return bool(await self._driver.run_in_transaction(_tx_reserve))

    async def release_unique_key(self, key: str) -> bool:
        """Release an idempotency key reservation asynchronously."""
        if self._dialect == "spangres":
            return (
                await self._driver.execute("DELETE FROM vertebra_queue_unique_keys WHERE unique_key = $1", (key,))
            ) > 0
        return (
            await self._driver.execute("DELETE FROM vertebra_queue_unique_keys WHERE unique_key = @key", {"key": key})
        ) > 0

    async def acquire_lease(self, lease_key: str, *, owner_id: str, ttl_seconds: float) -> bool:
        """Acquire or renew ``lease_key`` asynchronously without stealing active foreign leases."""
        return (
            await self.acquire_lease_record(
                lease_key,
                owner_id=owner_id,
                ttl_seconds=ttl_seconds,
            )
            is not None
        )

    async def acquire_lease_record(
        self,
        lease_key: str,
        *,
        owner_id: str,
        ttl_seconds: float,
        slot: str | None = None,
    ) -> dict[str, Any] | None:
        """Acquire or renew ``lease_key`` asynchronously and return the resulting lease row."""
        is_pg = self._dialect == "spangres"
        if is_pg:
            now = time.time()
            expires_at = now + float(ttl_seconds)
            sql = """
            INSERT INTO vertebra_queue_leases (lease_key, owner_id, expires_at, last_slot)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (lease_key) DO UPDATE
            SET owner_id = EXCLUDED.owner_id,
                expires_at = EXCLUDED.expires_at,
                last_slot = COALESCE(EXCLUDED.last_slot, vertebra_queue_leases.last_slot)
            WHERE vertebra_queue_leases.expires_at <= $5
               OR vertebra_queue_leases.owner_id = EXCLUDED.owner_id
            RETURNING lease_key, owner_id, expires_at, last_slot
            """
            rows = await self._driver.select(sql, (lease_key, owner_id, expires_at, slot, now))
            return dict(rows[0]) if rows else None

        async def _tx_lease(tx: AsyncSpannerTransaction) -> dict[str, Any] | None:
            now_tx = time.time()
            expires_tx = now_tx + float(ttl_seconds)
            existing = await tx.select_one_or_none(
                "SELECT lease_key, owner_id, expires_at, last_slot "
                "FROM vertebra_queue_leases WHERE lease_key = @lease_key FOR UPDATE",
                {"lease_key": lease_key},
            )
            if (
                existing is not None
                and float(existing["expires_at"]) > now_tx
                and str(existing["owner_id"]) != owner_id
            ):
                return None
            resolved_slot = slot if slot is not None else (existing.get("last_slot") if existing else None)
            if existing is None:
                await tx.execute(
                    "INSERT INTO vertebra_queue_leases (lease_key, owner_id, expires_at, last_slot) "
                    "VALUES (@lease_key, @owner_id, @expires_at, @last_slot)",
                    {
                        "lease_key": lease_key,
                        "owner_id": owner_id,
                        "expires_at": expires_tx,
                        "last_slot": resolved_slot,
                    },
                )
            else:
                await tx.execute(
                    "UPDATE vertebra_queue_leases "
                    "SET owner_id = @owner_id, expires_at = @expires_at, last_slot = @last_slot "
                    "WHERE lease_key = @lease_key",
                    {
                        "lease_key": lease_key,
                        "owner_id": owner_id,
                        "expires_at": expires_tx,
                        "last_slot": resolved_slot,
                    },
                )
            return {
                "lease_key": lease_key,
                "owner_id": owner_id,
                "expires_at": expires_tx,
                "last_slot": resolved_slot,
            }

        return cast("dict[str, Any] | None", await self._driver.run_in_transaction(_tx_lease))

    async def release_lease(self, lease_key: str, *, owner_id: str) -> bool:
        """Release a held distributed leader lease matching ``owner_id`` asynchronously."""
        if self._dialect == "spangres":
            sql = "DELETE FROM vertebra_queue_leases WHERE lease_key = $1 AND owner_id = $2"
            return (await self._driver.execute(sql, (lease_key, owner_id))) > 0
        sql = "DELETE FROM vertebra_queue_leases WHERE lease_key = @lease_key AND owner_id = @owner_id"
        return (await self._driver.execute(sql, {"lease_key": lease_key, "owner_id": owner_id})) > 0

    async def list_dlq_rows(self, *, limit: int = 100) -> list[dict[str, Any]]:
        """List dead-letter rows asynchronously ordered by ``failed_at DESC``."""
        if self._dialect == "spangres":
            return await self._driver.select(
                "SELECT * FROM vertebra_queue_dlq ORDER BY failed_at DESC LIMIT $1",
                (limit,),
            )
        return await self._driver.select(
            "SELECT * FROM vertebra_queue_dlq ORDER BY failed_at DESC LIMIT @limit",
            {"limit": limit},
        )

    async def pop_dlq_row(self, dlq_id: str) -> dict[str, Any] | None:
        """Atomically fetch and remove a dead-letter entry asynchronously by ``dlq_id``."""
        is_pg = self._dialect == "spangres"

        async def _tx_pop(tx: AsyncSpannerTransaction) -> dict[str, Any] | None:
            if is_pg:
                row = await tx.select_one_or_none(
                    "SELECT * FROM vertebra_queue_dlq WHERE dlq_id = $1 FOR UPDATE",
                    (dlq_id,),
                )
                if row is None:
                    return None
                await tx.execute("DELETE FROM vertebra_queue_dlq WHERE dlq_id = $1", (dlq_id,))
                await tx.execute(
                    f"DELETE FROM {self._table_name} WHERE shard_id = $1 AND task_id = $2 AND status = 'dead_letter'",
                    (int(row["shard_id"]), str(row["task_id"])),
                )
                return dict(row)
            row = await tx.select_one_or_none(
                "SELECT * FROM vertebra_queue_dlq WHERE dlq_id = @dlq_id FOR UPDATE",
                {"dlq_id": dlq_id},
            )
            if row is None:
                return None
            await tx.execute("DELETE FROM vertebra_queue_dlq WHERE dlq_id = @dlq_id", {"dlq_id": dlq_id})
            await tx.execute(
                f"DELETE FROM {self._table_name} "
                "WHERE shard_id = @shard_id AND task_id = @task_id AND status = 'dead_letter'",
                {"shard_id": int(row["shard_id"]), "task_id": str(row["task_id"])},
            )
            return dict(row)

        return cast("dict[str, Any] | None", await self._driver.run_in_transaction(_tx_pop))

    async def aclose(self) -> None:
        """Close the underlying asynchronous Spanner driver."""
        await self._driver.close()


def _encode_payload(payload: bytes) -> str:
    try:
        decoded = payload.decode("utf-8")
    except UnicodeDecodeError:
        return f"{_BINARY_PAYLOAD_PREFIX}{base64.b64encode(payload).decode('ascii')}"
    if decoded.startswith(_BINARY_PAYLOAD_PREFIX):
        return f"{_BINARY_PAYLOAD_PREFIX}{base64.b64encode(payload).decode('ascii')}"
    return decoded


def _decode_payload(raw: Any) -> bytes:
    if isinstance(raw, (bytes, bytearray, memoryview)):
        return bytes(raw)
    text = str(raw)
    if text.startswith(_BINARY_PAYLOAD_PREFIX):
        return base64.b64decode(text[len(_BINARY_PAYLOAD_PREFIX) :])
    return text.encode("utf-8")


def _serialize_headers(headers: dict[str, str] | None) -> str | None:
    if not headers:
        return None
    return encode_json(headers)


def _parse_headers(val: Any) -> dict[str, str]:
    if val is None:
        return {}
    if isinstance(val, dict):
        return {str(k): str(v) for k, v in val.items()}
    if isinstance(val, (bytes, bytearray, memoryview)):
        return cast("dict[str, str]", decode_json(bytes(val)))
    if isinstance(val, str):
        return cast("dict[str, str]", decode_json(val))
    return {}


def _normalize_status_filter(status: str | None) -> str | None:
    if status is None:
        return None
    normalized = status.strip().lower()
    if normalized == "ready":
        return "pending"
    if normalized == "claimed":
        return "running"
    if normalized == "completed":
        return "acked"
    return normalized


def _row_to_entry(row: dict[str, Any]) -> QueueEntry:
    raw_status = str(row.get("status", "running"))
    raw_attempt = int(row.get("attempt", 1))
    effective_attempt = max(1, raw_attempt + 1) if raw_status == "pending" else max(1, raw_attempt)
    locked_s = row.get("locked_until")
    sched_s = row.get("scheduled_at")
    headers = _parse_headers(row.get("headers"))
    if row.get("idempotency_key") and "idempotency_key" not in headers:
        headers["idempotency_key"] = str(row["idempotency_key"])
    if row.get("traceparent") and "traceparent" not in headers:
        headers["traceparent"] = str(row["traceparent"])
    return QueueEntry(
        job_id=str(row["task_id"]),
        queue=str(row["lane"]),
        task_name=str(row["task_name"]),
        payload=_decode_payload(row["payload"]),
        attempt=effective_attempt,
        max_attempts=max(1, int(row.get("max_retries", 1))),
        lease_until_ms=int(float(locked_s) * 1000) if locked_s is not None else 0,
        scheduled_at_ms=int(float(sched_s) * 1000) if sched_s is not None else None,
        headers=headers,
    )


def _build_idempotency_lookup(table_name: str, idem_key: str, *, is_pg: bool) -> tuple[str, Any]:
    if is_pg:
        sql = (
            f"SELECT task_id FROM {table_name} "
            "WHERE idempotency_key = $1 AND status IN ('pending', 'running') LIMIT 1 FOR UPDATE"
        )
        return sql, (idem_key,)
    sql = (
        f"SELECT task_id FROM {table_name} "
        "WHERE idempotency_key = @idempotency_key AND status IN ('pending', 'running') LIMIT 1 FOR UPDATE"
    )
    return sql, {"idempotency_key": idem_key}


def _build_insert_job(
    table_name: str,
    *,
    shard_id: int,
    task_id: str,
    task_name: str,
    lane: str,
    payload: str,
    attempt: int,
    max_retries: int,
    scheduled_at: float | None,
    enqueued_at: float,
    idempotency_key: str | None,
    traceparent: str | None,
    headers: str | None,
    is_pg: bool,
) -> tuple[str, Any]:
    if is_pg:
        sql = f"""
        INSERT INTO {table_name} (
            shard_id, task_id, task_name, lane, payload, status,
            attempt, max_retries, scheduled_at, enqueued_at, locked_until,
            consumer_name, receipt_handle, idempotency_key, traceparent, headers
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
        """
        return sql, (
            shard_id,
            task_id,
            task_name,
            lane,
            payload,
            "pending",
            attempt,
            max_retries,
            scheduled_at,
            enqueued_at,
            None,
            None,
            None,
            idempotency_key,
            traceparent,
            headers,
        )
    sql = f"""
    INSERT INTO {table_name} (
        shard_id, task_id, task_name, lane, payload, status,
        attempt, max_retries, scheduled_at, enqueued_at, locked_until,
        consumer_name, receipt_handle, idempotency_key, traceparent, headers
    ) VALUES (
        @shard_id, @task_id, @task_name, @lane, @payload, @status,
        @attempt, @max_retries, @scheduled_at, @enqueued_at, @locked_until,
        @consumer_name, @receipt_handle, @idempotency_key, @traceparent, @headers
    )
    """
    return sql, {
        "shard_id": shard_id,
        "task_id": task_id,
        "task_name": task_name,
        "lane": lane,
        "payload": payload,
        "status": "pending",
        "attempt": attempt,
        "max_retries": max_retries,
        "scheduled_at": scheduled_at,
        "enqueued_at": enqueued_at,
        "locked_until": None,
        "consumer_name": None,
        "receipt_handle": None,
        "idempotency_key": idempotency_key,
        "traceparent": traceparent,
        "headers": headers,
    }


def _build_claim_select(
    table_name: str,
    *,
    lanes: list[str],
    now_ts: float,
    batch_size: int,
    is_pg: bool,
) -> tuple[str, Any]:
    if is_pg:
        sql = f"""
        SELECT shard_id, task_id, attempt
        FROM {table_name}
        WHERE lane = ANY($1)
          AND (
              (status = 'pending' AND (scheduled_at IS NULL OR scheduled_at <= $2))
              OR (status = 'running' AND locked_until <= $2)
          )
        ORDER BY COALESCE(scheduled_at, enqueued_at) ASC, enqueued_at ASC
        LIMIT $3
        FOR UPDATE
        """
        return sql, (lanes, now_ts, batch_size)
    table_expr = (
        f"{table_name}@{{FORCE_INDEX=idx_vtb_spanner_queue_ready}}"
        if table_name == "vertebra_queue_jobs"
        else table_name
    )
    sql = f"""
    SELECT shard_id, task_id, attempt
    FROM {table_expr}
    WHERE lane IN UNNEST(@lanes)
      AND (
          (status = 'pending' AND (scheduled_at IS NULL OR scheduled_at <= @now_ts))
          OR (status = 'running' AND locked_until <= @now_ts)
      )
    ORDER BY COALESCE(scheduled_at, enqueued_at) ASC, enqueued_at ASC
    LIMIT @batch_size
    FOR UPDATE
    """
    return sql, {"lanes": lanes, "now_ts": now_ts, "batch_size": batch_size}


def _build_claim_update(
    table_name: str,
    *,
    shard_id: int,
    task_id: str,
    locked_until: float,
    consumer_name: str,
    receipt_handle: str,
    is_pg: bool,
) -> tuple[str, Any]:
    if is_pg:
        sql = f"""
        UPDATE {table_name}
        SET status = 'running',
            attempt = attempt + 1,
            locked_until = $1,
            consumer_name = $2,
            receipt_handle = $3
        WHERE shard_id = $4 AND task_id = $5
        RETURNING *
        """
        return sql, (locked_until, consumer_name, receipt_handle, shard_id, task_id)
    sql = f"""
    UPDATE {table_name}
    SET status = 'running',
        attempt = attempt + 1,
        locked_until = @locked_until,
        consumer_name = @consumer_name,
        receipt_handle = @receipt_handle
    WHERE shard_id = @shard_id AND task_id = @task_id
    THEN RETURN *
    """
    return sql, {
        "locked_until": locked_until,
        "consumer_name": consumer_name,
        "receipt_handle": receipt_handle,
        "shard_id": shard_id,
        "task_id": task_id,
    }


def _build_ack_dml(
    table_name: str,
    *,
    shard_id: int,
    task_id: str,
    attempt: int,
    cleanup: bool,
    is_pg: bool,
) -> tuple[str, Any]:
    if is_pg:
        if cleanup:
            sql = (
                f"DELETE FROM {table_name} WHERE shard_id = $1 AND task_id = $2 AND attempt = $3 AND status = 'running'"
            )
        else:
            sql = (
                f"UPDATE {table_name} SET status = 'acked', locked_until = NULL "
                "WHERE shard_id = $1 AND task_id = $2 AND attempt = $3 AND status = 'running'"
            )
        return sql, (shard_id, task_id, attempt)
    if cleanup:
        sql = (
            f"DELETE FROM {table_name} "
            "WHERE shard_id = @shard_id AND task_id = @task_id AND attempt = @attempt AND status = @status"
        )
    else:
        sql = (
            f"UPDATE {table_name} SET status = 'acked', locked_until = NULL "
            "WHERE shard_id = @shard_id AND task_id = @task_id AND attempt = @attempt AND status = @status"
        )
    return sql, {"shard_id": shard_id, "task_id": task_id, "attempt": attempt, "status": "running"}


def _build_job_for_update(table_name: str, *, shard_id: int, task_id: str, is_pg: bool) -> tuple[str, Any]:
    if is_pg:
        return (
            f"SELECT * FROM {table_name} WHERE shard_id = $1 AND task_id = $2 FOR UPDATE",
            (shard_id, task_id),
        )
    return (
        f"SELECT * FROM {table_name} WHERE shard_id = @shard_id AND task_id = @task_id FOR UPDATE",
        {"shard_id": shard_id, "task_id": task_id},
    )


def _build_insert_dlq(
    *,
    dlq_id: str,
    shard_id: int,
    task_id: str,
    task_name: str,
    lane: str,
    payload: str,
    error_type: str,
    error_message: str,
    failed_at: float,
    is_pg: bool,
) -> tuple[str, Any]:
    if is_pg:
        sql = """
        INSERT INTO vertebra_queue_dlq (
            dlq_id, shard_id, task_id, task_name, lane, payload, error_type, error_message, failed_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """
        return sql, (dlq_id, shard_id, task_id, task_name, lane, payload, error_type, error_message, failed_at)
    sql = """
    INSERT INTO vertebra_queue_dlq (
        dlq_id, shard_id, task_id, task_name, lane, payload, error_type, error_message, failed_at
    ) VALUES (@dlq_id, @shard_id, @task_id, @task_name, @lane, @payload, @error_type, @error_message, @failed_at)
    """
    return sql, {
        "dlq_id": dlq_id,
        "shard_id": shard_id,
        "task_id": task_id,
        "task_name": task_name,
        "lane": lane,
        "payload": payload,
        "error_type": error_type,
        "error_message": error_message,
        "failed_at": failed_at,
    }


def _build_mark_dead_letter(
    table_name: str,
    *,
    shard_id: int,
    task_id: str,
    attempt: int,
    last_error: str,
    is_pg: bool,
) -> tuple[str, Any]:
    if is_pg:
        sql = f"""
        UPDATE {table_name}
        SET status = 'dead_letter', locked_until = NULL, receipt_handle = NULL, last_error = $1
        WHERE shard_id = $2 AND task_id = $3 AND attempt = $4
        """
        return sql, (last_error, shard_id, task_id, attempt)
    sql = f"""
    UPDATE {table_name}
    SET status = 'dead_letter', locked_until = NULL, receipt_handle = NULL, last_error = @last_error
    WHERE shard_id = @shard_id AND task_id = @task_id AND attempt = @attempt
    """
    return sql, {"last_error": last_error, "shard_id": shard_id, "task_id": task_id, "attempt": attempt}


def _build_reschedule_job(
    table_name: str,
    *,
    shard_id: int,
    task_id: str,
    attempt: int,
    scheduled_at: float,
    last_error: str,
    is_pg: bool,
) -> tuple[str, Any]:
    if is_pg:
        sql = f"""
        UPDATE {table_name}
        SET status = 'pending',
            scheduled_at = $1,
            locked_until = NULL,
            consumer_name = NULL,
            receipt_handle = NULL,
            last_error = $2
        WHERE shard_id = $3 AND task_id = $4 AND attempt = $5
        """
        return sql, (scheduled_at, last_error, shard_id, task_id, attempt)
    sql = f"""
    UPDATE {table_name}
    SET status = 'pending',
        scheduled_at = @scheduled_at,
        locked_until = NULL,
        consumer_name = NULL,
        receipt_handle = NULL,
        last_error = @last_error
    WHERE shard_id = @shard_id AND task_id = @task_id AND attempt = @attempt
    """
    return sql, {
        "scheduled_at": scheduled_at,
        "last_error": last_error,
        "shard_id": shard_id,
        "task_id": task_id,
        "attempt": attempt,
    }


def _build_heartbeat_dml(
    table_name: str,
    *,
    shard_id: int,
    task_id: str,
    worker_id: str,
    locked_until: float,
    is_pg: bool,
) -> tuple[str, Any]:
    if is_pg:
        sql = f"""
        UPDATE {table_name}
        SET locked_until = $1
        WHERE shard_id = $2 AND task_id = $3 AND consumer_name = $4 AND status = 'running'
        """
        return sql, (locked_until, shard_id, task_id, worker_id)
    sql = f"""
    UPDATE {table_name}
    SET locked_until = @locked_until
    WHERE shard_id = @shard_id AND task_id = @task_id AND consumer_name = @worker_id AND status = 'running'
    """
    return sql, {
        "locked_until": locked_until,
        "shard_id": shard_id,
        "task_id": task_id,
        "worker_id": worker_id,
    }


def _build_reap_stale_dml(table_name: str, *, now_ts: float, is_pg: bool) -> tuple[str, Any]:
    if is_pg:
        sql = f"""
        UPDATE {table_name}
        SET status = 'pending', locked_until = NULL, consumer_name = NULL, receipt_handle = NULL
        WHERE status = 'running' AND locked_until <= $1
        """
        return sql, (now_ts,)
    sql = f"""
    UPDATE {table_name}
    SET status = 'pending', locked_until = NULL, consumer_name = NULL, receipt_handle = NULL
    WHERE status = 'running' AND locked_until <= @now_ts
    """
    return sql, {"now_ts": now_ts}


def _build_due_scheduled_select(
    table_name: str,
    *,
    now_ts: float,
    limit: int,
    is_pg: bool,
) -> tuple[str, Any]:
    if is_pg:
        sql = f"""
        SELECT shard_id, task_id
        FROM {table_name}
        WHERE status = 'pending' AND scheduled_at IS NOT NULL AND scheduled_at <= $1
        ORDER BY scheduled_at ASC
        LIMIT $2
        FOR UPDATE
        """
        return sql, (now_ts, limit)
    sql = f"""
    SELECT shard_id, task_id
    FROM {table_name}
    WHERE status = 'pending' AND scheduled_at IS NOT NULL AND scheduled_at <= @now_ts
    ORDER BY scheduled_at ASC
    LIMIT @limit
    FOR UPDATE
    """
    return sql, {"now_ts": now_ts, "limit": limit}


def _build_clear_scheduled_dml(
    table_name: str,
    *,
    shard_id: int,
    task_id: str,
    is_pg: bool,
) -> tuple[str, Any]:
    if is_pg:
        return (
            f"UPDATE {table_name} SET scheduled_at = NULL WHERE shard_id = $1 AND task_id = $2",
            (shard_id, task_id),
        )
    return (
        f"UPDATE {table_name} SET scheduled_at = NULL WHERE shard_id = @shard_id AND task_id = @task_id",
        {"shard_id": shard_id, "task_id": task_id},
    )


def _build_cancel_dml(
    table_name: str,
    *,
    shard_id: int,
    task_id: str,
    is_pg: bool,
) -> tuple[str, Any]:
    if is_pg:
        sql = (
            f"UPDATE {table_name} SET status = 'cancelled', locked_until = NULL "
            "WHERE shard_id = $1 AND task_id = $2 AND status IN ('pending', 'running')"
        )
        return sql, (shard_id, task_id)
    sql = (
        f"UPDATE {table_name} SET status = 'cancelled', locked_until = NULL "
        "WHERE shard_id = @shard_id AND task_id = @task_id AND status IN ('pending', 'running')"
    )
    return sql, {"shard_id": shard_id, "task_id": task_id}


def _build_get_job(
    table_name: str,
    *,
    shard_id: int,
    task_id: str,
    is_pg: bool,
) -> tuple[str, Any]:
    if is_pg:
        return f"SELECT * FROM {table_name} WHERE shard_id = $1 AND task_id = $2", (shard_id, task_id)
    return (
        f"SELECT * FROM {table_name} WHERE shard_id = @shard_id AND task_id = @task_id",
        {"shard_id": shard_id, "task_id": task_id},
    )


def _build_list_jobs(
    table_name: str,
    *,
    lane: str,
    status: str | None,
    limit: int,
    is_pg: bool,
) -> tuple[str, Any]:
    if is_pg:
        if status is not None:
            sql = f"SELECT * FROM {table_name} WHERE lane = $1 AND status = $2 ORDER BY enqueued_at ASC LIMIT $3"
            return sql, (lane, status, limit)
        sql = f"SELECT * FROM {table_name} WHERE lane = $1 ORDER BY enqueued_at ASC LIMIT $2"
        return sql, (lane, limit)
    if status is not None:
        sql = (
            f"SELECT * FROM {table_name} WHERE lane = @lane AND status = @status ORDER BY enqueued_at ASC LIMIT @limit"
        )
        return sql, {"lane": lane, "status": status, "limit": limit}
    sql = f"SELECT * FROM {table_name} WHERE lane = @lane ORDER BY enqueued_at ASC LIMIT @limit"
    return sql, {"lane": lane, "limit": limit}


def _build_next_due_query(
    table_name: str,
    *,
    lanes: list[str],
    now_ts: float,
    is_pg: bool,
) -> tuple[str, Any]:
    if is_pg:
        sql = f"""
        SELECT MIN(scheduled_at) AS min_sched
        FROM {table_name}
        WHERE lane = ANY($1) AND status = 'pending' AND scheduled_at > $2
        """
        return sql, (lanes, now_ts)
    sql = f"""
    SELECT MIN(scheduled_at) AS min_sched
    FROM {table_name}
    WHERE lane IN UNNEST(@lanes) AND status = 'pending' AND scheduled_at > @now_ts
    """
    return sql, {"lanes": lanes, "now_ts": now_ts}
