"""Google Cloud Spanner distributed queue backend over SyncSpannerQueueStore and AsyncSpannerQueueStore."""

from __future__ import annotations

import asyncio
import re
import time
import urllib.parse
from typing import TYPE_CHECKING, Any, Final, Literal, Protocol, runtime_checkable

import msgspec

from vertebra.db.adapters.spanner._async import AsyncSpannerDriver
from vertebra.db.adapters.spanner._sync import SyncSpannerDriver
from vertebra.db.adapters.spanner.config import SpannerConfig
from vertebra.db.adapters.spanner.queue import (
    SPANNER_QUEUE_STORE_DDL_GOOGLESQL,
    SPANNER_QUEUE_STORE_DDL_SPANGRES,
    AsyncSpannerQueueStore,
    SyncSpannerQueueStore,
    compute_spanner_shard_id,
    format_spanner_receipt_handle,
    parse_spanner_receipt_handle,
)
from vertebra.framework.queues._sql import (
    _SqlQueueBackendBase,
    decode_entry_envelope,
    decode_envelope_bytes,
    error_type_and_message,
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
from vertebra.utils.uuids import uuid4

if TYPE_CHECKING:
    from collections.abc import Sequence

    from vertebra.db.driver.capabilities import QueueEntry

__all__ = (
    "CLAIM_SPANNER_JOBS_SPANGRES_SQL",
    "CLAIM_SPANNER_JOBS_SQL",
    "SPANNER_QUEUE_DDL",
    "SPANNER_QUEUE_DDL_GOOGLESQL",
    "SPANNER_QUEUE_DDL_SPANGRES",
    "UPDATE_CLAIM_SPANNER_JOBS_SPANGRES_SQL",
    "UPDATE_CLAIM_SPANNER_JOBS_SQL",
    "SpannerQueueBackend",
    "SpannerQueueExecutor",
)

SPANNER_QUEUE_DDL_GOOGLESQL: Final[tuple[str, ...]] = SPANNER_QUEUE_STORE_DDL_GOOGLESQL
SPANNER_QUEUE_DDL_SPANGRES: Final[tuple[str, ...]] = SPANNER_QUEUE_STORE_DDL_SPANGRES
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

CLAIM_SPANNER_JOBS_SPANGRES_SQL: Final[str] = """
SELECT shard_id, task_id, task_name, lane, payload, attempt, max_retries,
       enqueued_at, scheduled_at, idempotency_key, traceparent
FROM vertebra_queue_jobs
WHERE lane = ANY($1)
  AND (
      (status = 'pending' AND (scheduled_at IS NULL OR scheduled_at <= $2))
      OR (status = 'running' AND locked_until <= $2)
  )
ORDER BY COALESCE(scheduled_at, enqueued_at) ASC, enqueued_at ASC
LIMIT $3
FOR UPDATE
"""

UPDATE_CLAIM_SPANNER_JOBS_SQL: Final[str] = """
UPDATE vertebra_queue_jobs
SET status = 'running',
    attempt = @attempt,
    locked_until = @locked_until,
    consumer_name = @consumer_name,
    receipt_handle = @receipt_handle
WHERE shard_id = @shard_id AND task_id = @task_id
THEN RETURN *
"""

UPDATE_CLAIM_SPANNER_JOBS_SPANGRES_SQL: Final[str] = """
UPDATE vertebra_queue_jobs
SET status = 'running',
    attempt = $1,
    locked_until = $2,
    consumer_name = $3,
    receipt_handle = $4
WHERE shard_id = $5 AND task_id = $6
RETURNING *
"""

_SPANNER_SCHEMES: Final[frozenset[str]] = frozenset({"spanner", "spanner+googlesql", "spangres", "spanner+spangres"})
_NAMED_PARAM_RE: Final[re.Pattern[str]] = re.compile(r"@([a-zA-Z_][a-zA-Z0-9_]*)")


def _to_spangres_params(sql: str) -> str:
    """Replace left-to-right ``@name`` placeholders with sequential ``$1..$N`` slots."""
    index = 0

    def _replace(_match: re.Match[str]) -> str:
        nonlocal index
        index += 1
        return f"${index}"

    return _NAMED_PARAM_RE.sub(_replace, sql)


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


class SpannerQueueBackend(_SqlQueueBackendBase):
    """Google Cloud Spanner queue backend over ``SyncSpannerQueueStore``, ``AsyncSpannerQueueStore``, or an executor."""

    __slots__ = (
        "_cleanup_on_ack",
        "_config",
        "_dialect",
        "_executor",
        "_shard_count",
        "_store",
        "_sync_store",
    )

    _sync_store_label = "SyncSpannerQueueStore"

    def __init__(
        self,
        config: QueueConfig | QueueBackendSettings | None = None,
        *,
        spanner_config: SpannerConfig | None = None,
        driver: AsyncSpannerDriver | None = None,
        sync_driver: SyncSpannerDriver | None = None,
        store: AsyncSpannerQueueStore | None = None,
        sync_store: SyncSpannerQueueStore | None = None,
        executor: SpannerQueueExecutor | None = None,
        dialect: Literal["googlesql", "spangres"] = "googlesql",
        shard_count: int = 16,
        cleanup_on_ack: bool = True,
    ) -> None:
        """Initialize the Spanner queue backend with native queue stores, drivers, DSN, or an executor."""
        store, sync_store, driver, sync_driver = unpack_queue_settings(config, store, sync_store, driver, sync_driver)

        resolved_config = config or QueueConfig(backend_url="")
        raw_url = resolved_config.backend_url
        parsed = urllib.parse.urlparse(raw_url)
        scheme = parsed.scheme.lower()
        if scheme and scheme not in _SPANNER_SCHEMES:
            msg = f"SpannerQueueBackend requires a spanner:// backend_url, got {raw_url!r}"
            raise ImproperConfigurationError(msg)

        if (
            scheme in {"spangres", "spanner+spangres"}
            or spanner_config is not None
            and spanner_config.dialect == "spangres"
            or store is not None
            and getattr(store, "dialect", None) == "spangres"
            or driver is not None
            and getattr(driver, "dialect", None) == "spangres"
        ):
            dialect = "spangres"

        self._config = resolved_config
        self._dialect: Literal["googlesql", "spangres"] = dialect
        self._shard_count = max(1, int(shard_count))
        self._cleanup_on_ack = cleanup_on_ack

        if executor is not None:
            self._executor: SpannerQueueExecutor | None = executor
            self._store: AsyncSpannerQueueStore | None = store
            self._sync_store: SyncSpannerQueueStore | None = sync_store
            return

        if store is not None or sync_store is not None:
            self._store = store
            self._sync_store = sync_store
            self._executor = None
            return

        if driver is not None or sync_driver is not None:
            self._store = (
                AsyncSpannerQueueStore(
                    driver,
                    dialect=self._dialect,
                    shard_count=self._shard_count,
                    cleanup_on_ack=cleanup_on_ack,
                )
                if driver is not None
                else None
            )
            self._sync_store = (
                SyncSpannerQueueStore(
                    sync_driver,
                    dialect=self._dialect,
                    shard_count=self._shard_count,
                    cleanup_on_ack=cleanup_on_ack,
                )
                if sync_driver is not None
                else None
            )
            self._executor = None
            return

        ensure_module("vertebra._spanner", package="vertebra-core[spanner]", extra="spanner")
        if spanner_config is None:
            if not raw_url or raw_url in {"spanner://", "spangres://", "spanner+googlesql://", "spanner+spangres://"}:
                msg = (
                    "SpannerQueueBackend requires a configured spanner:// or spangres:// DSN, "
                    "driver, store, or SpannerQueueExecutor"
                )
                raise ImproperConfigurationError(msg)
            spanner_config = SpannerConfig.from_dsn(raw_url)
            if not (spanner_config.project_id or spanner_config.instance_id or spanner_config.database_id):
                msg = f"SpannerQueueBackend requires project, instance, and database in {raw_url!r}"
                raise ImproperConfigurationError(msg)

        async_drv = AsyncSpannerDriver(spanner_config)
        sync_drv = SyncSpannerDriver(spanner_config)
        self._dialect = spanner_config.dialect
        self._store = AsyncSpannerQueueStore(
            async_drv,
            dialect=self._dialect,
            shard_count=self._shard_count,
            cleanup_on_ack=cleanup_on_ack,
        )
        self._sync_store = SyncSpannerQueueStore(
            sync_drv,
            dialect=self._dialect,
            shard_count=self._shard_count,
            cleanup_on_ack=cleanup_on_ack,
        )
        self._executor = None

    @property
    def dialect(self) -> Literal["googlesql", "spangres"]:
        """Return the active Spanner SQL dialect."""
        return self._dialect

    @property
    def shard_count(self) -> int:
        """Return the number of hash shards used for primary keys."""
        return self._shard_count

    @property
    def store(self) -> AsyncSpannerQueueStore | None:
        """Return the bound asynchronous Spanner queue store, if configured."""
        return self._store

    @property
    def sync_store(self) -> SyncSpannerQueueStore | None:
        """Return the bound synchronous Spanner queue store, if configured."""
        return self._sync_store

    @staticmethod
    def compute_shard_id(task_id: str, shard_count: int = 16) -> int:
        """Compute a deterministic hash shard prefix to prevent monotonic key hotspots."""
        return compute_spanner_shard_id(task_id, shard_count)

    def ddl_statements(
        self,
        dialect: Literal["googlesql", "spangres"] | None = None,
    ) -> tuple[str, ...]:
        """Return the Spanner DDL statements for queue, index, DLQ, and lease tables."""
        target = dialect or self._dialect
        if target == "spangres":
            return SPANNER_QUEUE_DDL_SPANGRES
        return SPANNER_QUEUE_DDL_GOOGLESQL

    def claim_sql(self, dialect: Literal["googlesql", "spangres"] | None = None) -> str:
        """Return the ``SELECT ... FOR UPDATE`` SQL statement for ``dialect``."""
        target = dialect or self._dialect
        if target == "spangres":
            return CLAIM_SPANNER_JOBS_SPANGRES_SQL
        return CLAIM_SPANNER_JOBS_SQL

    def update_claim_sql(self, dialect: Literal["googlesql", "spangres"] | None = None) -> str:
        """Return the ``UPDATE ... THEN RETURN *`` / ``RETURNING *`` claim statement for ``dialect``."""
        target = dialect or self._dialect
        if target == "spangres":
            return UPDATE_CLAIM_SPANNER_JOBS_SPANGRES_SQL
        return UPDATE_CLAIM_SPANNER_JOBS_SQL

    def _render_sql(self, gsql: str) -> str:
        return _to_spangres_params(gsql) if self._dialect == "spangres" else gsql

    async def enqueue(self, envelope: TaskEnvelope) -> TaskEnvelope:
        """Insert a task into ``vertebra_queue_jobs`` under its deterministic ``shard_id``."""
        if self._store is not None:
            return await self._store_enqueue(envelope)
        assert self._executor is not None
        shard_id = self.compute_shard_id(envelope.task_id, self._shard_count)
        payload_str = encode_json_bytes(envelope).decode("utf-8")
        initial_attempt = max(0, int(envelope.attempt) - 1)
        insert_sql = self._render_sql(
            """
            INSERT INTO vertebra_queue_jobs (
                shard_id, task_id, task_name, lane, payload, attempt, max_retries,
                status, enqueued_at, scheduled_at, locked_until, consumer_name,
                receipt_handle, idempotency_key, traceparent
            ) VALUES (@shard_id, @task_id, @task_name, @lane, @payload, @attempt, @max_retries,
                      @status, @enqueued_at, @scheduled_at, @locked_until, @consumer_name,
                      @receipt_handle, @idempotency_key, @traceparent)
            """
        )
        await self._executor.execute(
            insert_sql,
            shard_id,
            envelope.task_id,
            envelope.task_name,
            envelope.lane,
            payload_str,
            initial_attempt,
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
        """Claim up to ``batch_size`` ready jobs atomically via ``SELECT ... FOR UPDATE`` and ``UPDATE``."""
        vis_timeout = (
            visibility_timeout_s if visibility_timeout_s is not None else self._config.visibility_timeout_seconds
        )
        lane_names = [str(lane) for lane in lanes]
        if self._store is not None:
            entries = await self._store.claim(
                lane_names,
                limit=batch_size,
                worker_id=self._config.consumer_name,
                lease_seconds=vis_timeout,
            )
            if not entries and timeout_seconds > 0:
                due_delay = await self._store.next_due_delay(lane_names)
                poll_sleep = min(timeout_seconds, self._config.poll_interval_seconds)
                if due_delay is not None:
                    poll_sleep = min(poll_sleep, max(0.01, due_delay))
                await asyncio.sleep(poll_sleep)
                entries = await self._store.claim(
                    lane_names,
                    limit=batch_size,
                    worker_id=self._config.consumer_name,
                    lease_seconds=vis_timeout,
                )
            return [_entry_to_envelope(e, self._shard_count) for e in entries]

        assert self._executor is not None
        rows = await self._claim_via_executor(lane_names, batch_size=batch_size, vis_timeout=vis_timeout)
        if not rows and timeout_seconds > 0:
            await asyncio.sleep(min(timeout_seconds, self._config.poll_interval_seconds))
            rows = await self._claim_via_executor(lane_names, batch_size=batch_size, vis_timeout=vis_timeout)

        envelopes: list[TaskEnvelope] = []
        for row in rows:
            env = decode_envelope_bytes(row["payload"])
            shard_id = int(row.get("shard_id", self.compute_shard_id(env.task_id, self._shard_count)))
            attempt = int(row.get("attempt", env.attempt))
            handle = str(row.get("receipt_handle") or format_spanner_receipt_handle(shard_id, env.task_id, attempt))
            envelopes.append(msgspec.structs.replace(env, attempt=attempt, receipt_handle=handle))
        return envelopes

    async def ack(self, envelope: TaskEnvelope | str) -> None:
        """Delete or mark acknowledged a claimed job row with attempt fencing."""
        shard_id, task_id, attempt = self._resolve_target(envelope)
        if not task_id:
            return
        if self._store is not None:
            await self._store.ack(task_id, attempt=attempt)
            return
        assert self._executor is not None
        stmt = (
            "DELETE FROM vertebra_queue_jobs WHERE shard_id = @shard_id AND task_id = @task_id AND attempt = @attempt"
            if self._cleanup_on_ack
            else "UPDATE vertebra_queue_jobs SET status = 'acked' WHERE shard_id = @shard_id AND task_id = @task_id AND attempt = @attempt"
        )
        await self._executor.execute(self._render_sql(stmt), shard_id, task_id, attempt)

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
        err_type, err_msg = error_type_and_message(error)
        delay = delay_seconds if delay_seconds is not None else (retry_delay_s if retry_delay_s is not None else 1.0)

        if self._store is not None:
            shard_id, task_id, attempt = self._resolve_target(envelope)
            if not task_id:
                return None
            ok, dlq_row = await self._store.nack_with_dlq(
                task_id,
                attempt=attempt,
                error_type=err_type,
                error_message=err_msg,
                retry_delay_s=None if send_to_dlq else delay,
                force_dlq=send_to_dlq,
            )
            if not ok or dlq_row is None:
                return None
            resolved_env = envelope if isinstance(envelope, TaskEnvelope) else decode_envelope_bytes(dlq_row["payload"])
            return DeadLetterEntry(
                dlq_id=str(dlq_row["dlq_id"]),
                envelope=resolved_env,
                error_type=err_type,
                error_message=err_msg,
                failed_at=float(dlq_row["failed_at"]),
            )

        assert self._executor is not None
        resolved = await self._resolve_executor_envelope(envelope)
        if resolved is None:
            return None
        shard_id = self.compute_shard_id(resolved.task_id, self._shard_count)
        now = time.time()

        if send_to_dlq or resolved.attempt >= resolved.max_retries:
            dlq_id = uuid4().hex
            payload_str = encode_json_bytes(resolved).decode("utf-8")
            dlq_sql = self._render_sql(
                """
                INSERT INTO vertebra_queue_dlq (
                    dlq_id, shard_id, task_id, task_name, lane, payload, error_type, error_message, failed_at
                ) VALUES (@dlq_id, @shard_id, @task_id, @task_name, @lane, @payload, @error_type, @error_message, @failed_at)
                """
            )
            await self._executor.execute(
                dlq_sql,
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
            del_sql = self._render_sql(
                "DELETE FROM vertebra_queue_jobs WHERE shard_id = @shard_id AND task_id = @task_id AND attempt = @attempt"
            )
            await self._executor.execute(del_sql, shard_id, resolved.task_id, resolved.attempt)
            return DeadLetterEntry(
                dlq_id=dlq_id,
                envelope=resolved,
                error_type=err_type,
                error_message=err_msg,
                failed_at=now,
            )

        sched_at = now + max(0.0, delay)
        upd_sql = self._render_sql(
            """
            UPDATE vertebra_queue_jobs
            SET status = @status, attempt = @attempt, scheduled_at = @scheduled_at,
                locked_until = @locked_until, receipt_handle = @receipt_handle
            WHERE shard_id = @shard_id AND task_id = @task_id
            """
        )
        await self._executor.execute(
            upd_sql,
            "pending",
            resolved.attempt,
            sched_at,
            None,
            None,
            shard_id,
            resolved.task_id,
        )
        return None

    async def promote_scheduled(self, *, now: float | None = None, batch_size: int = 100) -> int:
        """Promote due scheduled jobs by clearing ``scheduled_at`` threshold."""
        if self._store is not None:
            return await self._store.promote_scheduled(limit=batch_size)
        assert self._executor is not None
        current_time = now if now is not None else time.time()
        sql = self._render_sql(
            """
            UPDATE vertebra_queue_jobs
            SET scheduled_at = NULL
            WHERE status = 'pending' AND scheduled_at IS NOT NULL AND scheduled_at <= @now_ts
            """
        )
        return await self._executor.execute(sql, current_time)

    async def list_dlq(self, *, limit: int = 100) -> list[DeadLetterEntry]:
        """Fetch dead-letter entries from ``vertebra_queue_dlq``."""
        if self._store is not None:
            rows = await self._store.list_dlq_rows(limit=limit)
        else:
            assert self._executor is not None
            dlq_sql = self._render_sql("SELECT * FROM vertebra_queue_dlq ORDER BY failed_at DESC LIMIT @limit")
            rows = await self._executor.fetch_all(dlq_sql, limit)
        return [
            DeadLetterEntry(
                dlq_id=str(row["dlq_id"]),
                envelope=decode_envelope_bytes(row["payload"]),
                error_type=str(row["error_type"]),
                error_message=str(row["error_message"]),
                failed_at=float(row["failed_at"]),
            )
            for row in rows
        ]

    async def replay_dlq(self, dlq_id: str) -> TaskEnvelope | None:
        """Re-enqueue a task from the dead-letter queue and remove its DLQ entry."""
        if self._store is not None:
            row = await self._store.pop_dlq_row(dlq_id)
            if row is None:
                return None
            env = decode_envelope_bytes(row["payload"])
            if env.idempotency_key:
                await self._store.release_unique_key(env.idempotency_key)
            replayed = msgspec.structs.replace(env, attempt=1, scheduled_at=None, receipt_handle=None)
            return await self.enqueue(replayed)

        assert self._executor is not None
        row = await self._executor.fetch_one(
            self._render_sql("SELECT * FROM vertebra_queue_dlq WHERE dlq_id = @dlq_id"), dlq_id
        )
        if not row:
            return None
        env = decode_envelope_bytes(row["payload"])
        replayed = msgspec.structs.replace(env, attempt=1, scheduled_at=None, receipt_handle=None)
        await self._executor.execute(
            self._render_sql("DELETE FROM vertebra_queue_dlq WHERE dlq_id = @dlq_id"), row["dlq_id"]
        )
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
        """Conditionally acquire or renew ``lease_key`` without stealing active foreign leases."""
        effective_owner = holder_id or owner_id
        effective_ttl = ttl_s if ttl_s is not None else ttl_seconds
        if self._store is not None:
            rec = await self._store.acquire_lease_record(
                lease_key,
                owner_id=effective_owner,
                ttl_seconds=effective_ttl,
                slot=slot,
            )
            if rec is None:
                return None
            return CronLease(
                lease_key=str(rec["lease_key"]),
                owner_id=str(rec["owner_id"]),
                expires_at=float(rec["expires_at"]),
                last_slot=rec.get("last_slot"),
            )

        assert self._executor is not None
        now = time.time()
        expires_at = now + effective_ttl
        sql = (
            """
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
            if self._dialect == "spangres"
            else """
            SELECT lease_key, owner_id, expires_at, last_slot
            FROM vertebra_queue_leases
            WHERE lease_key = @lease_key
            FOR UPDATE
            """
        )
        if self._dialect == "spangres":
            row = await self._executor.fetch_one(sql, lease_key, effective_owner, expires_at, slot, now)
            if not row:
                return None
            return CronLease(
                lease_key=str(row["lease_key"]),
                owner_id=str(row["owner_id"]),
                expires_at=float(row["expires_at"]),
                last_slot=row.get("last_slot"),
            )

        existing = await self._executor.fetch_one(sql, lease_key)
        if (
            existing is not None
            and float(existing["expires_at"]) > now
            and str(existing["owner_id"]) != effective_owner
        ):
            return None
        resolved_slot = slot if slot is not None else (existing.get("last_slot") if existing else None)
        if existing is None:
            await self._executor.execute(
                "INSERT INTO vertebra_queue_leases (lease_key, owner_id, expires_at, last_slot) "
                "VALUES (@lease_key, @owner_id, @expires_at, @last_slot)",
                lease_key,
                effective_owner,
                expires_at,
                resolved_slot,
            )
        else:
            await self._executor.execute(
                "UPDATE vertebra_queue_leases "
                "SET owner_id = @owner_id, expires_at = @expires_at, last_slot = @last_slot "
                "WHERE lease_key = @lease_key "
                "AND (expires_at <= @now_ts OR owner_id = @owner_id)",
                effective_owner,
                expires_at,
                resolved_slot,
                lease_key,
                now,
            )
        return CronLease(
            lease_key=lease_key,
            owner_id=effective_owner,
            expires_at=expires_at,
            last_slot=resolved_slot,
        )

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
        """Acquire a distributed cron leader lease."""
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
        """Renew an active cron leader lease."""
        return await self.renew_lease(lease_key, owner_id, ttl_seconds, slot=slot, ttl_s=ttl_s)

    async def _claim_via_executor(
        self,
        lanes: list[str],
        *,
        batch_size: int,
        vis_timeout: float,
    ) -> list[dict[str, Any]]:
        assert self._executor is not None
        now = time.time()
        locked_until = now + vis_timeout
        candidates = await self._executor.fetch_all(
            self.claim_sql(),
            lanes,
            now,
            batch_size,
        )
        claimed: list[dict[str, Any]] = []
        for cand in candidates:
            shard_id = int(cand["shard_id"])
            task_id = str(cand["task_id"])
            next_attempt = int(cand.get("attempt", 0)) + 1
            handle = format_spanner_receipt_handle(shard_id, task_id, next_attempt)
            updated = await self._executor.fetch_one(
                self.update_claim_sql(),
                next_attempt,
                locked_until,
                self._config.consumer_name,
                handle,
                shard_id,
                task_id,
            )
            if updated is not None:
                claimed.append(updated)
        return claimed

    def _resolve_target(self, envelope_or_handle: TaskEnvelope | str) -> tuple[int, str, int]:
        if isinstance(envelope_or_handle, TaskEnvelope):
            if envelope_or_handle.receipt_handle:
                parsed = parse_spanner_receipt_handle(envelope_or_handle.receipt_handle, self._shard_count)
                if parsed is not None:
                    return parsed
            shard_id = self.compute_shard_id(envelope_or_handle.task_id, self._shard_count)
            return shard_id, envelope_or_handle.task_id, max(1, int(envelope_or_handle.attempt))
        parsed = parse_spanner_receipt_handle(envelope_or_handle, self._shard_count)
        if parsed is not None:
            return parsed
        return 0, "", 1

    async def _resolve_executor_envelope(self, envelope_or_handle: TaskEnvelope | str) -> TaskEnvelope | None:
        if isinstance(envelope_or_handle, TaskEnvelope):
            return envelope_or_handle
        shard_id, task_id, attempt = self._resolve_target(envelope_or_handle)
        if not task_id or self._executor is None:
            return None
        row = await self._executor.fetch_one(
            self._render_sql("SELECT * FROM vertebra_queue_jobs WHERE shard_id = @shard_id AND task_id = @task_id"),
            shard_id,
            task_id,
        )
        if row is None:
            return None
        env = decode_envelope_bytes(row["payload"])
        return msgspec.structs.replace(env, attempt=attempt, receipt_handle=envelope_or_handle)


def _entry_to_envelope(entry: QueueEntry, shard_count: int) -> TaskEnvelope:
    shard_id = compute_spanner_shard_id(entry.job_id, shard_count)
    handle = format_spanner_receipt_handle(shard_id, entry.job_id, entry.attempt)
    return decode_entry_envelope(entry, receipt_handle=handle)
