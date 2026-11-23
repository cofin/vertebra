"""Cloud Spanner synchronous and asynchronous queue stores across GoogleSQL and Spangres."""

from __future__ import annotations

import base64
import re
import time
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Final, Literal, cast

from vertebra.db.driver.capabilities import AsyncQueueStore, QueueEntry, QueueReservation, SyncQueueStore
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
    "farm_fingerprint64",
    "format_spanner_receipt_handle",
    "parse_spanner_receipt_handle",
)

_BINARY_PAYLOAD_PREFIX: Final[str] = "vtb-b64:"
_FARMHASH_MASK64: Final[int] = 0xFFFFFFFFFFFFFFFF
_FARMHASH_K0: Final[int] = 0xC3A5C85C97CB3127
_FARMHASH_K1: Final[int] = 0xB492B66FBE98F273
_FARMHASH_K2: Final[int] = 0x9AE16A3B2F90404F

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
        priority INT64,
        expires_at_ms INT64,
        completed_at_ms INT64,
        worker_id STRING(128),
        heartbeat_at_ms INT64,
        result STRING(MAX),
        error STRING(MAX),
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
        last_error,
        priority,
        expires_at_ms,
        completed_at_ms,
        worker_id,
        heartbeat_at_ms,
        result,
        error
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
        expires_at FLOAT64,
        job_id STRING(64),
        task_name STRING(256),
        created_at_ms INT64
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
        priority BIGINT,
        expires_at_ms BIGINT,
        completed_at_ms BIGINT,
        worker_id VARCHAR(128),
        heartbeat_at_ms BIGINT,
        result TEXT,
        error TEXT,
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
        last_error,
        priority,
        expires_at_ms,
        completed_at_ms,
        worker_id,
        heartbeat_at_ms,
        result,
        error
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
        expires_at DOUBLE PRECISION,
        job_id VARCHAR(64),
        task_name VARCHAR(256),
        created_at_ms BIGINT
    )
    """,
)


def _farmhash_rot64(val: int, shift: int) -> int:
    val &= _FARMHASH_MASK64
    return val if shift == 0 else (((val >> shift) | (val << (64 - shift))) & _FARMHASH_MASK64)


def _farmhash_shift_mix(val: int) -> int:
    val &= _FARMHASH_MASK64
    return (val ^ (val >> 47)) & _FARMHASH_MASK64


def _farmhash_len_16(u: int, v: int, mul: int) -> int:
    a = ((u ^ v) * mul) & _FARMHASH_MASK64
    a ^= a >> 47
    b = ((v ^ a) * mul) & _FARMHASH_MASK64
    b ^= b >> 47
    return (b * mul) & _FARMHASH_MASK64


def _farmhash_fetch64(buf: bytes, offset: int) -> int:
    return int.from_bytes(buf[offset : offset + 8], "little", signed=False)


def _farmhash_fetch32(buf: bytes, offset: int) -> int:
    return int.from_bytes(buf[offset : offset + 4], "little", signed=False)


def _farmhash_weak_len_32(buf: bytes, offset: int, a: int, b: int) -> tuple[int, int]:
    w = _farmhash_fetch64(buf, offset)
    x = _farmhash_fetch64(buf, offset + 8)
    y = _farmhash_fetch64(buf, offset + 16)
    z = _farmhash_fetch64(buf, offset + 24)
    a = (a + w) & _FARMHASH_MASK64
    b = _farmhash_rot64((b + a + z) & _FARMHASH_MASK64, 21)
    c = a
    a = (a + x) & _FARMHASH_MASK64
    a = (a + y) & _FARMHASH_MASK64
    b = (b + _farmhash_rot64(a, 44)) & _FARMHASH_MASK64
    return (a + z) & _FARMHASH_MASK64, (b + c) & _FARMHASH_MASK64


def _farmhash_len_0_to_16(buf: bytes) -> int:
    length = len(buf)
    if length >= 8:
        mul = (_FARMHASH_K2 + length * 2) & _FARMHASH_MASK64
        a = (_farmhash_fetch64(buf, 0) + _FARMHASH_K2) & _FARMHASH_MASK64
        b = _farmhash_fetch64(buf, length - 8)
        c = (_farmhash_rot64(b, 37) * mul + a) & _FARMHASH_MASK64
        d = ((_farmhash_rot64(a, 25) + b) * mul) & _FARMHASH_MASK64
        return _farmhash_len_16(c, d, mul)
    if length >= 4:
        mul = (_FARMHASH_K2 + length * 2) & _FARMHASH_MASK64
        a = _farmhash_fetch32(buf, 0)
        return _farmhash_len_16((length + (a << 3)) & _FARMHASH_MASK64, _farmhash_fetch32(buf, length - 4), mul)
    if length > 0:
        a = buf[0]
        b = buf[length >> 1]
        c = buf[length - 1]
        y = (a + (b << 8)) & 0xFFFFFFFF
        z = (length + (c << 2)) & 0xFFFFFFFF
        return (_farmhash_shift_mix(((y * _FARMHASH_K2) ^ (z * _FARMHASH_K0)) & _FARMHASH_MASK64) * _FARMHASH_K2) & (
            _FARMHASH_MASK64
        )
    return _FARMHASH_K2


def _farmhash_len_17_to_32(buf: bytes) -> int:
    length = len(buf)
    mul = (_FARMHASH_K2 + length * 2) & _FARMHASH_MASK64
    a = (_farmhash_fetch64(buf, 0) * _FARMHASH_K1) & _FARMHASH_MASK64
    b = _farmhash_fetch64(buf, 8)
    c = (_farmhash_fetch64(buf, length - 8) * mul) & _FARMHASH_MASK64
    d = (_farmhash_fetch64(buf, length - 16) * _FARMHASH_K2) & _FARMHASH_MASK64
    return _farmhash_len_16(
        (_farmhash_rot64((a + b) & _FARMHASH_MASK64, 43) + _farmhash_rot64(c, 30) + d) & _FARMHASH_MASK64,
        (a + _farmhash_rot64((b + _FARMHASH_K2) & _FARMHASH_MASK64, 18) + c) & _FARMHASH_MASK64,
        mul,
    )


def _farmhash_len_33_to_64(buf: bytes) -> int:
    length = len(buf)
    mul = (_FARMHASH_K2 + length * 2) & _FARMHASH_MASK64
    a = (_farmhash_fetch64(buf, 0) * _FARMHASH_K2) & _FARMHASH_MASK64
    b = _farmhash_fetch64(buf, 8)
    c = (_farmhash_fetch64(buf, length - 8) * mul) & _FARMHASH_MASK64
    d = (_farmhash_fetch64(buf, length - 16) * _FARMHASH_K2) & _FARMHASH_MASK64
    y = (_farmhash_rot64((a + b) & _FARMHASH_MASK64, 43) + _farmhash_rot64(c, 30) + d) & _FARMHASH_MASK64
    z = _farmhash_len_16(
        y, (a + _farmhash_rot64((b + _FARMHASH_K2) & _FARMHASH_MASK64, 18) + c) & _FARMHASH_MASK64, mul
    )
    e = (_farmhash_fetch64(buf, 16) * mul) & _FARMHASH_MASK64
    f = _farmhash_fetch64(buf, 24)
    g = ((y + _farmhash_fetch64(buf, length - 32)) * mul) & _FARMHASH_MASK64
    h = ((z + _farmhash_fetch64(buf, length - 24)) * mul) & _FARMHASH_MASK64
    return _farmhash_len_16(
        (_farmhash_rot64((e + f) & _FARMHASH_MASK64, 43) + _farmhash_rot64(g, 30) + h) & _FARMHASH_MASK64,
        (e + _farmhash_rot64((f + a) & _FARMHASH_MASK64, 18) + g) & _FARMHASH_MASK64,
        mul,
    )


def farm_fingerprint64(data: bytes | str) -> int:
    """Compute Google FarmHash ``Fingerprint64`` as a signed 64-bit integer matching Spanner ``FARM_FINGERPRINT``."""
    buf = data.encode("utf-8") if isinstance(data, str) else bytes(data)
    length = len(buf)
    if length <= 16:
        u64 = _farmhash_len_0_to_16(buf)
    elif length <= 32:
        u64 = _farmhash_len_17_to_32(buf)
    elif length <= 64:
        u64 = _farmhash_len_33_to_64(buf)
    else:
        seed = 81
        x = seed
        y = (seed * _FARMHASH_K1 + 113) & _FARMHASH_MASK64
        z = (_farmhash_shift_mix((y * _FARMHASH_K2 + 113) & _FARMHASH_MASK64) * _FARMHASH_K2) & _FARMHASH_MASK64
        v0, v1 = 0, 0
        w0, w1 = 0, 0
        x = (x * _FARMHASH_K2 + _farmhash_fetch64(buf, 0)) & _FARMHASH_MASK64
        end = ((length - 1) // 64) * 64
        last64 = end + ((length - 1) & 63) - 63
        offset = 0
        while True:
            x = (
                _farmhash_rot64((x + y + v0 + _farmhash_fetch64(buf, offset + 8)) & _FARMHASH_MASK64, 37) * _FARMHASH_K1
            ) & _FARMHASH_MASK64
            y = (
                _farmhash_rot64((y + v1 + _farmhash_fetch64(buf, offset + 48)) & _FARMHASH_MASK64, 42) * _FARMHASH_K1
            ) & _FARMHASH_MASK64
            x ^= w1
            y = (y + v0 + _farmhash_fetch64(buf, offset + 40)) & _FARMHASH_MASK64
            z = (_farmhash_rot64((z + w0) & _FARMHASH_MASK64, 33) * _FARMHASH_K1) & _FARMHASH_MASK64
            v0, v1 = _farmhash_weak_len_32(
                buf,
                offset,
                (v1 * _FARMHASH_K1) & _FARMHASH_MASK64,
                (x + w0) & _FARMHASH_MASK64,
            )
            w0, w1 = _farmhash_weak_len_32(
                buf,
                offset + 32,
                (z + w1) & _FARMHASH_MASK64,
                (y + _farmhash_fetch64(buf, offset + 16)) & _FARMHASH_MASK64,
            )
            z, x = x, z
            offset += 64
            if offset == end:
                break
        mul = (_FARMHASH_K1 + ((z & 0xFF) << 1)) & _FARMHASH_MASK64
        offset = last64
        w0 = (w0 + ((length - 1) & 63)) & _FARMHASH_MASK64
        v0 = (v0 + w0) & _FARMHASH_MASK64
        w0 = (w0 + v0) & _FARMHASH_MASK64
        x = (
            _farmhash_rot64((x + y + v0 + _farmhash_fetch64(buf, offset + 8)) & _FARMHASH_MASK64, 37) * mul
        ) & _FARMHASH_MASK64
        y = (
            _farmhash_rot64((y + v1 + _farmhash_fetch64(buf, offset + 48)) & _FARMHASH_MASK64, 42) * mul
        ) & _FARMHASH_MASK64
        x ^= w1
        y = (y + v0 + _farmhash_fetch64(buf, offset + 40)) & _FARMHASH_MASK64
        z = (_farmhash_rot64((z + w0) & _FARMHASH_MASK64, 33) * mul) & _FARMHASH_MASK64
        v0, v1 = _farmhash_weak_len_32(buf, offset, (v1 * mul) & _FARMHASH_MASK64, (x + w0) & _FARMHASH_MASK64)
        w0, w1 = _farmhash_weak_len_32(
            buf,
            offset + 32,
            (z + w1) & _FARMHASH_MASK64,
            (y + _farmhash_fetch64(buf, offset + 16)) & _FARMHASH_MASK64,
        )
        z, x = x, z
        u64 = _farmhash_len_16(
            (_farmhash_len_16(v0, w0, mul) + _farmhash_shift_mix(y) * _FARMHASH_K0 + z) & _FARMHASH_MASK64,
            (_farmhash_len_16(v1, w1, mul) + x) & _FARMHASH_MASK64,
            mul,
        )
    return int.from_bytes(u64.to_bytes(8, "little", signed=False), "little", signed=True)


def compute_spanner_shard_id(task_id: str, shard_count: int = 16) -> int:
    """Compute a deterministic FarmHash shard prefix matching ``MOD(ABS(FARM_FINGERPRINT(task_id)), shard_count)``."""
    return abs(farm_fingerprint64(task_id)) % max(1, shard_count)


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


class _BaseSpannerQueueStore:
    """Shared non-I/O query planning and row parsing for synchronous and asynchronous Spanner queue stores."""

    __slots__ = ("_cleanup_on_ack", "_dialect", "_driver", "_shard_count", "_table_name")

    _driver: Any
    _dialect: Literal["googlesql", "spangres"]
    _shard_count: int
    _cleanup_on_ack: bool
    _table_name: str

    def _init_store(
        self,
        driver: Any,
        *,
        dialect: Literal["googlesql", "spangres"] | None = None,
        shard_count: int = 16,
        cleanup_on_ack: bool = True,
        table_name: str = "vertebra_queue_jobs",
    ) -> None:
        self._driver = driver
        resolved_dialect = dialect or getattr(driver, "dialect", "googlesql")
        self._dialect = (
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

    @property
    def _is_pg(self) -> bool:
        return self._dialect == "spangres"

    def _schema_ddls(self) -> list[str]:
        statements = SPANNER_QUEUE_STORE_DDL_SPANGRES if self._is_pg else SPANNER_QUEUE_STORE_DDL_GOOGLESQL
        return [ddl.replace("vertebra_queue_jobs", self._table_name) for ddl in statements]

    def _shard_for(self, job_id: str) -> int:
        return compute_spanner_shard_id(job_id, self._shard_count)

    def _prepare_insert_entry(self, entry: QueueEntry, now: float) -> tuple[str, str | None, tuple[str, Any]]:
        jid = entry.job_id or uuid4().hex
        idem_key = entry.headers.get("idempotency_key") or entry.headers.get("key")
        stmt = _build_insert_job(
            self._table_name,
            shard_id=self._shard_for(jid),
            task_id=jid,
            task_name=entry.task_name,
            lane=entry.queue,
            payload=_encode_payload(entry.payload),
            attempt=max(0, int(entry.attempt) - 1),
            max_retries=max(1, int(entry.max_attempts)),
            scheduled_at=entry.scheduled_at_ms / 1000.0 if entry.scheduled_at_ms is not None else None,
            enqueued_at=now,
            idempotency_key=idem_key,
            traceparent=entry.headers.get("traceparent"),
            headers=_serialize_headers(entry.headers),
            priority=int(entry.priority),
            expires_at_ms=entry.expires_at_ms,
            completed_at_ms=entry.completed_at_ms,
            worker_id=entry.worker_id,
            heartbeat_at_ms=entry.heartbeat_at_ms,
            result=_encode_payload(entry.result) if entry.result is not None else None,
            error=entry.error,
            is_pg=self._is_pg,
        )
        return jid, idem_key, stmt

    def _plan_claim_job_update(
        self,
        row: dict[str, Any] | None,
        *,
        shard_id: int,
        job_id: str,
        worker_id: str,
        lease_seconds: float,
        expected_attempt: int | None,
    ) -> tuple[str, Any] | None:
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
        return _build_claim_update(
            self._table_name,
            shard_id=shard_id,
            task_id=job_id,
            locked_until=now + float(lease_seconds),
            consumer_name=worker_id,
            receipt_handle=format_spanner_receipt_handle(shard_id, job_id, next_attempt),
            is_pg=self._is_pg,
        )

    def _plan_nack(
        self,
        row: dict[str, Any] | None,
        *,
        shard_id: int,
        job_id: str,
        attempt: int,
        error_type: str,
        error_message: str,
        retry_delay_s: float | None,
        force_dlq: bool,
    ) -> tuple[list[tuple[str, Any]], dict[str, Any] | None] | None:
        if row is None or str(row.get("status")) != "running" or int(row.get("attempt", 0)) != attempt:
            return None
        now = time.time()
        is_pg = self._is_pg
        combined_error = f"{error_type}: {error_message}" if error_type else error_message
        max_retries = int(row.get("max_retries", 1))
        if force_dlq or retry_delay_s is None or attempt >= max_retries:
            dlq_id = uuid4().hex
            task_name = str(row["task_name"])
            lane = str(row["lane"])
            payload = str(row["payload"])
            stmts = [
                _build_insert_dlq(
                    dlq_id=dlq_id,
                    shard_id=shard_id,
                    task_id=job_id,
                    task_name=task_name,
                    lane=lane,
                    payload=payload,
                    error_type=error_type,
                    error_message=error_message,
                    failed_at=now,
                    is_pg=is_pg,
                ),
                _build_mark_dead_letter(
                    self._table_name,
                    shard_id=shard_id,
                    task_id=job_id,
                    attempt=attempt,
                    last_error=combined_error,
                    completed_at_ms=int(now * 1000),
                    is_pg=is_pg,
                ),
            ]
            return stmts, {
                "dlq_id": dlq_id,
                "shard_id": shard_id,
                "task_id": job_id,
                "task_name": task_name,
                "lane": lane,
                "payload": payload,
                "error_type": error_type,
                "error_message": error_message,
                "failed_at": now,
            }
        return [
            _build_reschedule_job(
                self._table_name,
                shard_id=shard_id,
                task_id=job_id,
                attempt=attempt,
                scheduled_at=now + max(0.0, float(retry_delay_s)),
                last_error=combined_error,
                is_pg=is_pg,
            )
        ], None

    def _plan_heartbeat_batch(
        self, job_ids: Sequence[str], *, worker_id: str, lease_seconds: float
    ) -> list[tuple[str, Any]]:
        locked_until = time.time() + float(lease_seconds)
        is_pg = self._is_pg
        return [
            _build_heartbeat_dml(
                self._table_name,
                shard_id=self._shard_for(jid),
                task_id=jid,
                worker_id=worker_id,
                locked_until=locked_until,
                is_pg=is_pg,
            )
            for jid in job_ids
        ]

    def _plan_touch_heartbeat_batch(self, job_ids: Sequence[str], *, heartbeat_at_ms: int) -> list[tuple[str, Any]]:
        is_pg = self._is_pg
        return [
            _build_touch_heartbeat_dml(
                self._table_name,
                shard_id=self._shard_for(jid),
                task_id=jid,
                heartbeat_at_ms=heartbeat_at_ms,
                is_pg=is_pg,
            )
            for jid in job_ids
        ]

    def _plan_null_heartbeat_batch(self, job_ids: Sequence[str]) -> list[tuple[str, Any]]:
        is_pg = self._is_pg
        return [
            _build_null_heartbeat_dml(
                self._table_name,
                shard_id=self._shard_for(jid),
                task_id=jid,
                is_pg=is_pg,
            )
            for jid in job_ids
        ]

    def _render(
        self, template: str, params: dict[str, Any], *, pg_order: tuple[str, ...] | None = None
    ) -> tuple[str, Any]:
        return _render_spanner_sql(template, params, is_pg=self._is_pg, pg_order=pg_order)

    @staticmethod
    def _reserve_unique_pg(key: str, ttl_seconds: float) -> tuple[str, tuple[Any, ...]]:
        now = time.time()
        sql = """
        INSERT INTO vertebra_queue_unique_keys (unique_key, expires_at)
        VALUES ($1, $2)
        ON CONFLICT (unique_key) DO UPDATE
        SET expires_at = EXCLUDED.expires_at
        WHERE vertebra_queue_unique_keys.expires_at <= $3
        RETURNING unique_key
        """
        return sql, (key, now + float(ttl_seconds), now)

    @staticmethod
    def _acquire_lease_pg(
        lease_key: str, *, owner_id: str, ttl_seconds: float, slot: str | None
    ) -> tuple[str, tuple[Any, ...]]:
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
        RETURNING lease_key, owner_id, expires_at, last_slot
        """
        return sql, (lease_key, owner_id, now + float(ttl_seconds), slot, now)


class SyncSpannerQueueStore(_BaseSpannerQueueStore, SyncQueueStore):
    """Synchronous Cloud Spanner queue store backed by interactive ``ReadWrite`` transactions."""

    __slots__ = ()

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
        self._init_store(
            driver, dialect=dialect, shard_count=shard_count, cleanup_on_ack=cleanup_on_ack, table_name=table_name
        )

    def ensure_schema(self) -> None:
        """Idempotently create queue, index, DLQ, lease, and unique-key tables."""
        for ddl in self._schema_ddls():
            self._driver.execute(ddl)

    def enqueue(self, entry: QueueEntry) -> str:
        """Enqueue a single job entry inside a Spanner transaction and return its ``job_id``."""
        jid, idem_key, (ins_sql, ins_params) = self._prepare_insert_entry(entry, time.time())

        def _tx_enqueue(tx: SyncSpannerTransaction) -> str:
            if idem_key:
                existing = tx.select_one_or_none(
                    *_build_idempotency_lookup(self._table_name, idem_key, is_pg=self._is_pg)
                )
                if existing is not None:
                    return str(existing["task_id"])
            tx.execute(ins_sql, ins_params)
            return jid

        return cast("str", self._driver.run_in_transaction(_tx_enqueue))

    def enqueue_many(self, entries: Sequence[QueueEntry]) -> list[str]:
        """Bulk-enqueue job entries inside one interactive ``ReadWrite`` transaction."""
        if not entries:
            return []
        now = time.time()

        def _tx_enqueue_many(tx: SyncSpannerTransaction) -> list[str]:
            assigned: list[str] = []
            batch_statements: list[tuple[str, Any]] = []
            for entry in entries:
                jid, idem_key, stmt = self._prepare_insert_entry(entry, now)
                if idem_key:
                    existing = tx.select_one_or_none(
                        *_build_idempotency_lookup(self._table_name, idem_key, is_pg=self._is_pg)
                    )
                    if existing is not None:
                        assigned.append(str(existing["task_id"]))
                        continue
                batch_statements.append(stmt)
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
        lane_list = [str(q) for q in queues]
        batch_limit = int(limit)

        def _tx_claim(tx: SyncSpannerTransaction) -> list[QueueEntry]:
            now = time.time()
            locked_until = now + float(lease_seconds)
            candidates = tx.select(
                *_build_claim_select(
                    self._table_name, lanes=lane_list, now_ts=now, batch_size=batch_limit, is_pg=self._is_pg
                )
            )
            claimed: list[QueueEntry] = []
            for cand in candidates:
                c_shard = int(cand["shard_id"])
                c_task_id = str(cand["task_id"])
                next_attempt = int(cand.get("attempt", 0)) + 1
                updated_rows = tx.select(
                    *_build_claim_update(
                        self._table_name,
                        shard_id=c_shard,
                        task_id=c_task_id,
                        locked_until=locked_until,
                        consumer_name=worker_id,
                        receipt_handle=format_spanner_receipt_handle(c_shard, c_task_id, next_attempt),
                        is_pg=self._is_pg,
                    )
                )
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
        shard_id = self._shard_for(job_id)

        def _tx_claim_job(tx: SyncSpannerTransaction) -> QueueEntry | None:
            row = tx.select_one_or_none(
                *_build_job_for_update(self._table_name, shard_id=shard_id, task_id=job_id, is_pg=self._is_pg)
            )
            upd = self._plan_claim_job_update(
                row,
                shard_id=shard_id,
                job_id=job_id,
                worker_id=worker_id,
                lease_seconds=lease_seconds,
                expected_attempt=expected_attempt,
            )
            if upd is None:
                return None
            updated_rows = tx.select(*upd)
            return _row_to_entry(updated_rows[0]) if updated_rows else None

        return cast("QueueEntry | None", self._driver.run_in_transaction(_tx_claim_job))

    def ack(self, job_id: str, *, attempt: int) -> bool:
        """Acknowledge a fenced ``(job_id, attempt)`` claim."""
        return bool(
            self._driver.execute(
                *_build_ack_dml(
                    self._table_name,
                    shard_id=self._shard_for(job_id),
                    task_id=job_id,
                    attempt=attempt,
                    cleanup=self._cleanup_on_ack,
                    completed_at_ms=int(time.time() * 1000),
                    is_pg=self._is_pg,
                )
            )
            > 0
        )

    def nack(
        self,
        job_id: str,
        *,
        attempt: int,
        error: str,
        retry_delay_s: float | None = None,
    ) -> bool:
        """Record a failed ``(job_id, attempt)`` execution and schedule retry or dead-lettering."""
        return self.nack_with_dlq(
            job_id,
            attempt=attempt,
            error_type="TaskExecutionError",
            error_message=error,
            retry_delay_s=retry_delay_s,
            force_dlq=retry_delay_s is None,
        )[0]

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
        shard_id = self._shard_for(job_id)

        def _tx_nack(tx: SyncSpannerTransaction) -> tuple[bool, dict[str, Any] | None]:
            row = tx.select_one_or_none(
                *_build_job_for_update(self._table_name, shard_id=shard_id, task_id=job_id, is_pg=self._is_pg)
            )
            plan = self._plan_nack(
                row,
                shard_id=shard_id,
                job_id=job_id,
                attempt=attempt,
                error_type=error_type,
                error_message=error_message,
                retry_delay_s=retry_delay_s,
                force_dlq=force_dlq,
            )
            if plan is None:
                return False, None
            stmts, dlq_record = plan
            for sql, params in stmts:
                tx.execute(sql, params)
            return True, dlq_record

        return cast("tuple[bool, dict[str, Any] | None]", self._driver.run_in_transaction(_tx_nack))

    def heartbeat(self, job_ids: Sequence[str], *, worker_id: str, lease_seconds: float) -> int:
        """Extend visibility leases for active ``(job_ids, worker_id)`` claims."""
        if not job_ids:
            return 0
        return int(
            sum(
                self._driver.execute_batch_dml(
                    self._plan_heartbeat_batch(job_ids, worker_id=worker_id, lease_seconds=lease_seconds)
                )
            )
        )

    def reap_stale(self, *, lease_seconds: float = 300.0) -> int:
        """Reap expired ``running`` claims back into ``pending`` status."""
        _ = lease_seconds
        return int(
            self._driver.execute(*_build_reap_stale_dml(self._table_name, now_ts=time.time(), is_pg=self._is_pg))
        )

    def promote_scheduled(self, *, limit: int = 100) -> int:
        """Clear ``scheduled_at`` on up to ``limit`` due ``pending`` jobs."""
        max_rows = max(1, int(limit))

        def _tx_promote(tx: SyncSpannerTransaction) -> int:
            due = tx.select(
                *_build_due_scheduled_select(self._table_name, now_ts=time.time(), limit=max_rows, is_pg=self._is_pg)
            )
            if not due:
                return 0
            return sum(
                tx.execute_batch_dml(
                    [
                        _build_clear_scheduled_dml(
                            self._table_name, shard_id=int(r["shard_id"]), task_id=str(r["task_id"]), is_pg=self._is_pg
                        )
                        for r in due
                    ]
                )
            )

        return int(self._driver.run_in_transaction(_tx_promote))

    def cancel(self, job_id: str) -> bool:
        """Cancel a ``pending`` or ``running`` job."""
        return bool(
            self._driver.execute(
                *_build_cancel_dml(
                    self._table_name,
                    shard_id=self._shard_for(job_id),
                    task_id=job_id,
                    completed_at_ms=int(time.time() * 1000),
                    is_pg=self._is_pg,
                )
            )
            > 0
        )

    def get_entry(self, job_id: str) -> QueueEntry | None:
        """Retrieve a queue entry by ``job_id``."""
        row = self._driver.select_one_or_none(
            *_build_get_job(self._table_name, shard_id=self._shard_for(job_id), task_id=job_id, is_pg=self._is_pg)
        )
        return _row_to_entry(row) if row is not None else None

    def get_entry_status(self, job_id: str) -> str | None:
        """Retrieve the current status string of ``job_id``, or ``None`` if absent."""
        row = self._driver.select_one_or_none(
            *_build_get_job(self._table_name, shard_id=self._shard_for(job_id), task_id=job_id, is_pg=self._is_pg)
        )
        return str(row["status"]) if row is not None and row.get("status") is not None else None

    def list_entries(
        self, queue: str | None = None, *, status: str | None = None, limit: int = 100
    ) -> list[QueueEntry]:
        """List queue entries matching optional ``queue`` and ``status`` up to ``limit``."""
        rows = self._driver.select(
            *_build_list_jobs(
                self._table_name, lane=queue, status=_normalize_status_filter(status), limit=limit, is_pg=self._is_pg
            )
        )
        return [_row_to_entry(r) for r in rows]

    def cleanup_terminal(self, *, before_ms: int, limit: int | None = None) -> int:
        """Delete terminal queue records completed before ``before_ms`` while preserving forever reservations."""
        if limit is not None and limit <= 0:
            return 0

        def _tx_cleanup(tx: SyncSpannerTransaction) -> int:
            if limit is not None:
                rows = tx.select(
                    *_build_cleanup_terminal_select(
                        self._table_name, before_ms=before_ms, limit=limit, is_pg=self._is_pg
                    )
                )
                deleted = (
                    sum(
                        tx.execute_batch_dml(
                            [
                                _build_delete_by_pk_dml(
                                    self._table_name,
                                    shard_id=int(r["shard_id"]),
                                    task_id=str(r["task_id"]),
                                    is_pg=self._is_pg,
                                )
                                for r in rows
                            ]
                        )
                    )
                    if rows
                    else 0
                )
            else:
                deleted = tx.execute(
                    *_build_cleanup_terminal_dml(self._table_name, before_ms=before_ms, is_pg=self._is_pg)
                )
            tx.execute(*_build_cleanup_expired_unique_keys(before_ms=before_ms, is_pg=self._is_pg))
            return deleted

        return int(self._driver.run_in_transaction(_tx_cleanup))

    def get_entry_by_key(self, key: str, *, queue: str | None = None) -> QueueEntry | None:
        """Retrieve a queue entry matching idempotency or reservation ``key`` and optional ``queue``."""
        row = self._driver.select_one_or_none(
            *_build_get_entry_by_key(self._table_name, key=key, lane=queue, is_pg=self._is_pg)
        )
        return _row_to_entry(row) if row is not None else None

    def reserve_identity(
        self,
        key: str,
        *,
        job_id: str,
        task_name: str,
        ttl_seconds: float | None = None,
    ) -> QueueReservation | None:
        """Acquire a single-winner reservation, returning ``None`` when acquired or the existing owner."""

        def _tx_reserve_identity(tx: SyncSpannerTransaction) -> QueueReservation | None:
            now_tx = time.time()
            expires_tx = (now_tx + float(ttl_seconds)) if ttl_seconds is not None else None
            existing = tx.select_one_or_none(*_build_reservation_for_update(key=key, is_pg=self._is_pg))
            if existing is not None:
                exp = existing.get("expires_at")
                if exp is None or float(exp) > now_tx:
                    return _row_to_reservation(existing)
                tx.execute(
                    *_build_update_reservation(
                        key=key,
                        expires_at=expires_tx,
                        job_id=job_id,
                        task_name=task_name,
                        created_at_ms=int(now_tx * 1000),
                        is_pg=self._is_pg,
                    )
                )
            else:
                tx.execute(
                    *_build_insert_reservation(
                        key=key,
                        expires_at=expires_tx,
                        job_id=job_id,
                        task_name=task_name,
                        created_at_ms=int(now_tx * 1000),
                        is_pg=self._is_pg,
                    )
                )
            return None

        return cast("QueueReservation | None", self._driver.run_in_transaction(_tx_reserve_identity))

    def get_reservation(self, key: str) -> QueueReservation | None:
        """Retrieve an active identity reservation for ``key``, or ``None``."""
        row = self._driver.select_one_or_none(*_build_get_reservation(key=key, now_ts=time.time(), is_pg=self._is_pg))
        return _row_to_reservation(row) if row is not None else None

    def reset_identity(self, key: str, *, expected_job_id: str | None = None) -> bool:
        """Release an identity reservation, optionally fenced by ``expected_job_id``."""
        return bool(
            self._driver.execute(*_build_reset_identity(key=key, expected_job_id=expected_job_id, is_pg=self._is_pg))
            > 0
        )

    def assign_worker(self, job_id: str, *, worker_id: str, attempt: int) -> bool:
        """Record ``worker_id`` assignment for an active fenced claim."""
        return bool(
            self._driver.execute(
                *_build_assign_worker_dml(
                    self._table_name,
                    shard_id=self._shard_for(job_id),
                    task_id=job_id,
                    worker_id=worker_id,
                    attempt=attempt,
                    is_pg=self._is_pg,
                )
            )
            > 0
        )

    def touch_heartbeats(self, job_ids: Sequence[str], *, heartbeat_at_ms: int) -> int:
        """Update ``heartbeat_at_ms`` for active claimed jobs and return updated count."""
        return (
            int(
                sum(
                    self._driver.execute_batch_dml(
                        self._plan_touch_heartbeat_batch(job_ids, heartbeat_at_ms=heartbeat_at_ms)
                    )
                )
            )
            if job_ids
            else 0
        )

    def null_heartbeats(self, job_ids: Sequence[str]) -> int:
        """Clear ``heartbeat_at_ms`` for the given ``job_ids`` and return updated count."""
        return int(sum(self._driver.execute_batch_dml(self._plan_null_heartbeat_batch(job_ids)))) if job_ids else 0

    def interrupt_task(self, job_id: str, *, error: str | None = None) -> bool:
        """Interrupt a pending or running job with optional error detail."""
        return bool(
            self._driver.execute(
                *_build_interrupt_task_dml(
                    self._table_name,
                    shard_id=self._shard_for(job_id),
                    task_id=job_id,
                    error=error,
                    completed_at_ms=int(time.time() * 1000),
                    is_pg=self._is_pg,
                )
            )
            > 0
        )

    def expire_overdue(self, *, now_ms: int, limit: int | None = None) -> int:
        """Mark overdue jobs with ``expires_at_ms <= now_ms`` as expired up to ``limit``."""
        if limit is not None and limit <= 0:
            return 0

        def _tx_expire(tx: SyncSpannerTransaction) -> int:
            if limit is not None:
                rows = tx.select(
                    *_build_overdue_select(self._table_name, now_ms=now_ms, limit=limit, is_pg=self._is_pg)
                )
                if not rows:
                    return 0
                return sum(
                    tx.execute_batch_dml(
                        [
                            _build_expire_by_pk_dml(
                                self._table_name,
                                shard_id=int(r["shard_id"]),
                                task_id=str(r["task_id"]),
                                now_ms=now_ms,
                                is_pg=self._is_pg,
                            )
                            for r in rows
                        ]
                    )
                )
            return tx.execute(*_build_expire_overdue_dml(self._table_name, now_ms=now_ms, is_pg=self._is_pg))

        return int(self._driver.run_in_transaction(_tx_expire))

    def next_due_delay(self, queues: Sequence[str]) -> float | None:
        """Return seconds until the earliest scheduled job in ``queues`` is due, or ``None``."""
        if not queues:
            return None
        now = time.time()
        row = self._driver.select_one_or_none(
            *_build_next_due_query(self._table_name, lanes=[str(q) for q in queues], now_ts=now, is_pg=self._is_pg)
        )
        if row is None or row.get("min_sched") is None:
            return None
        return max(0.0, float(row["min_sched"]) - now)

    def reserve_unique_key(self, key: str, *, ttl_seconds: float) -> bool:
        """Conditionally reserve ``key`` for ``ttl_seconds`` without overwriting unexpired keys."""
        if self._is_pg:
            return len(self._driver.select(*self._reserve_unique_pg(key, ttl_seconds))) > 0

        def _tx_reserve(tx: SyncSpannerTransaction) -> bool:
            now_tx = time.time()
            expires_tx = now_tx + float(ttl_seconds)
            existing = tx.select_one_or_none(
                "SELECT unique_key, expires_at FROM vertebra_queue_unique_keys WHERE unique_key = @key FOR UPDATE",
                {"key": key},
            )
            if existing is not None and float(existing["expires_at"]) > now_tx:
                return False
            sql = (
                "INSERT INTO vertebra_queue_unique_keys (unique_key, expires_at) VALUES (@key, @expires_at)"
                if existing is None
                else "UPDATE vertebra_queue_unique_keys SET expires_at = @expires_at WHERE unique_key = @key"
            )
            tx.execute(sql, {"key": key, "expires_at": expires_tx})
            return True

        return bool(self._driver.run_in_transaction(_tx_reserve))

    def release_unique_key(self, key: str) -> bool:
        """Release an idempotency key reservation."""
        sql, params = self._render("DELETE FROM vertebra_queue_unique_keys WHERE unique_key = @key", {"key": key})
        return bool(self._driver.execute(sql, params) > 0)

    def acquire_lease(self, lease_key: str, *, owner_id: str, ttl_seconds: float) -> bool:
        """Acquire or renew ``lease_key`` only when expired or already owned by ``owner_id``."""
        return self.acquire_lease_record(lease_key, owner_id=owner_id, ttl_seconds=ttl_seconds) is not None

    def acquire_lease_record(
        self,
        lease_key: str,
        *,
        owner_id: str,
        ttl_seconds: float,
        slot: str | None = None,
    ) -> dict[str, Any] | None:
        """Acquire or renew ``lease_key`` without stealing active foreign leases and return lease metadata."""
        if self._is_pg:
            rows = self._driver.select(
                *self._acquire_lease_pg(lease_key, owner_id=owner_id, ttl_seconds=ttl_seconds, slot=slot)
            )
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
            record = {
                "lease_key": lease_key,
                "owner_id": owner_id,
                "expires_at": expires_tx,
                "last_slot": resolved_slot,
            }
            sql = (
                "INSERT INTO vertebra_queue_leases (lease_key, owner_id, expires_at, last_slot) "
                "VALUES (@lease_key, @owner_id, @expires_at, @last_slot)"
                if existing is None
                else "UPDATE vertebra_queue_leases SET owner_id = @owner_id, expires_at = @expires_at, last_slot = @last_slot WHERE lease_key = @lease_key"
            )
            tx.execute(sql, record)
            return record

        return cast("dict[str, Any] | None", self._driver.run_in_transaction(_tx_lease))

    def release_lease(self, lease_key: str, *, owner_id: str) -> bool:
        """Release a held distributed leader lease matching ``owner_id``."""
        sql, params = self._render(
            "DELETE FROM vertebra_queue_leases WHERE lease_key = @lease_key AND owner_id = @owner_id",
            {"lease_key": lease_key, "owner_id": owner_id},
        )
        return bool(self._driver.execute(sql, params) > 0)

    def list_dlq_rows(self, *, limit: int = 100) -> list[dict[str, Any]]:
        """List dead-letter rows ordered by ``failed_at DESC``."""
        sql, params = self._render(
            "SELECT * FROM vertebra_queue_dlq ORDER BY failed_at DESC LIMIT @limit", {"limit": limit}
        )
        return cast("list[dict[str, Any]]", self._driver.select(sql, params))

    def pop_dlq_row(self, dlq_id: str) -> dict[str, Any] | None:
        """Atomically fetch and remove a dead-letter entry by ``dlq_id``."""

        def _tx_pop(tx: SyncSpannerTransaction) -> dict[str, Any] | None:
            row = tx.select_one_or_none(
                *self._render("SELECT * FROM vertebra_queue_dlq WHERE dlq_id = @dlq_id FOR UPDATE", {"dlq_id": dlq_id})
            )
            if row is None:
                return None
            tx.execute(*self._render("DELETE FROM vertebra_queue_dlq WHERE dlq_id = @dlq_id", {"dlq_id": dlq_id}))
            tx.execute(
                *self._render(
                    f"DELETE FROM {self._table_name} WHERE shard_id = @shard_id AND task_id = @task_id AND status = 'dead_letter'",
                    {"shard_id": int(row["shard_id"]), "task_id": str(row["task_id"])},
                )
            )
            return dict(row)

        return cast("dict[str, Any] | None", self._driver.run_in_transaction(_tx_pop))

    def close(self) -> None:
        """Close the underlying synchronous Spanner driver."""
        self._driver.close()


class AsyncSpannerQueueStore(_BaseSpannerQueueStore, AsyncQueueStore):
    """Asynchronous Cloud Spanner queue store backed by interactive ``ReadWrite`` transactions."""

    __slots__ = ()

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
        self._init_store(
            driver, dialect=dialect, shard_count=shard_count, cleanup_on_ack=cleanup_on_ack, table_name=table_name
        )

    async def ensure_schema(self) -> None:
        """Idempotently create queue, index, DLQ, lease, and unique-key tables."""
        for ddl in self._schema_ddls():
            await self._driver.execute(ddl)

    async def enqueue(self, entry: QueueEntry) -> str:
        """Enqueue a single job entry inside an async Spanner transaction and return its ``job_id``."""
        jid, idem_key, (ins_sql, ins_params) = self._prepare_insert_entry(entry, time.time())

        async def _tx_enqueue(tx: AsyncSpannerTransaction) -> str:
            if idem_key:
                existing = await tx.select_one_or_none(
                    *_build_idempotency_lookup(self._table_name, idem_key, is_pg=self._is_pg)
                )
                if existing is not None:
                    return str(existing["task_id"])
            await tx.execute(ins_sql, ins_params)
            return jid

        return cast("str", await self._driver.run_in_transaction(_tx_enqueue))

    async def enqueue_many(self, entries: Sequence[QueueEntry]) -> list[str]:
        """Bulk-enqueue job entries inside one async interactive ``ReadWrite`` transaction."""
        if not entries:
            return []
        now = time.time()

        async def _tx_enqueue_many(tx: AsyncSpannerTransaction) -> list[str]:
            assigned: list[str] = []
            batch_statements: list[tuple[str, Any]] = []
            for entry in entries:
                jid, idem_key, stmt = self._prepare_insert_entry(entry, now)
                if idem_key:
                    existing = await tx.select_one_or_none(
                        *_build_idempotency_lookup(self._table_name, idem_key, is_pg=self._is_pg)
                    )
                    if existing is not None:
                        assigned.append(str(existing["task_id"]))
                        continue
                batch_statements.append(stmt)
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
        lane_list = [str(q) for q in queues]
        batch_limit = int(limit)

        async def _tx_claim(tx: AsyncSpannerTransaction) -> list[QueueEntry]:
            now = time.time()
            locked_until = now + float(lease_seconds)
            candidates = await tx.select(
                *_build_claim_select(
                    self._table_name, lanes=lane_list, now_ts=now, batch_size=batch_limit, is_pg=self._is_pg
                )
            )
            claimed: list[QueueEntry] = []
            for cand in candidates:
                c_shard = int(cand["shard_id"])
                c_task_id = str(cand["task_id"])
                next_attempt = int(cand.get("attempt", 0)) + 1
                updated_rows = await tx.select(
                    *_build_claim_update(
                        self._table_name,
                        shard_id=c_shard,
                        task_id=c_task_id,
                        locked_until=locked_until,
                        consumer_name=worker_id,
                        receipt_handle=format_spanner_receipt_handle(c_shard, c_task_id, next_attempt),
                        is_pg=self._is_pg,
                    )
                )
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
        shard_id = self._shard_for(job_id)

        async def _tx_claim_job(tx: AsyncSpannerTransaction) -> QueueEntry | None:
            row = await tx.select_one_or_none(
                *_build_job_for_update(self._table_name, shard_id=shard_id, task_id=job_id, is_pg=self._is_pg)
            )
            upd = self._plan_claim_job_update(
                row,
                shard_id=shard_id,
                job_id=job_id,
                worker_id=worker_id,
                lease_seconds=lease_seconds,
                expected_attempt=expected_attempt,
            )
            if upd is None:
                return None
            updated_rows = await tx.select(*upd)
            return _row_to_entry(updated_rows[0]) if updated_rows else None

        return cast("QueueEntry | None", await self._driver.run_in_transaction(_tx_claim_job))

    async def ack(self, job_id: str, *, attempt: int) -> bool:
        """Acknowledge a fenced ``(job_id, attempt)`` claim asynchronously."""
        return bool(
            (
                await self._driver.execute(
                    *_build_ack_dml(
                        self._table_name,
                        shard_id=self._shard_for(job_id),
                        task_id=job_id,
                        attempt=attempt,
                        cleanup=self._cleanup_on_ack,
                        completed_at_ms=int(time.time() * 1000),
                        is_pg=self._is_pg,
                    )
                )
            )
            > 0
        )

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
        shard_id = self._shard_for(job_id)

        async def _tx_nack(tx: AsyncSpannerTransaction) -> tuple[bool, dict[str, Any] | None]:
            row = await tx.select_one_or_none(
                *_build_job_for_update(self._table_name, shard_id=shard_id, task_id=job_id, is_pg=self._is_pg)
            )
            plan = self._plan_nack(
                row,
                shard_id=shard_id,
                job_id=job_id,
                attempt=attempt,
                error_type=error_type,
                error_message=error_message,
                retry_delay_s=retry_delay_s,
                force_dlq=force_dlq,
            )
            if plan is None:
                return False, None
            stmts, dlq_record = plan
            for sql, params in stmts:
                await tx.execute(sql, params)
            return True, dlq_record

        return cast("tuple[bool, dict[str, Any] | None]", await self._driver.run_in_transaction(_tx_nack))

    async def heartbeat(self, job_ids: Sequence[str], *, worker_id: str, lease_seconds: float) -> int:
        """Extend visibility leases asynchronously for active ``(job_ids, worker_id)`` claims."""
        if not job_ids:
            return 0
        counts = await self._driver.execute_batch_dml(
            self._plan_heartbeat_batch(job_ids, worker_id=worker_id, lease_seconds=lease_seconds)
        )
        return int(sum(counts))

    async def reap_stale(self, *, lease_seconds: float = 300.0) -> int:
        """Reap expired ``running`` claims back into ``pending`` status asynchronously."""
        _ = lease_seconds
        return int(
            await self._driver.execute(*_build_reap_stale_dml(self._table_name, now_ts=time.time(), is_pg=self._is_pg))
        )

    async def promote_scheduled(self, *, limit: int = 100) -> int:
        """Clear ``scheduled_at`` asynchronously on up to ``limit`` due ``pending`` jobs."""
        max_rows = max(1, int(limit))

        async def _tx_promote(tx: AsyncSpannerTransaction) -> int:
            due = await tx.select(
                *_build_due_scheduled_select(self._table_name, now_ts=time.time(), limit=max_rows, is_pg=self._is_pg)
            )
            if not due:
                return 0
            return sum(
                await tx.execute_batch_dml(
                    [
                        _build_clear_scheduled_dml(
                            self._table_name, shard_id=int(r["shard_id"]), task_id=str(r["task_id"]), is_pg=self._is_pg
                        )
                        for r in due
                    ]
                )
            )

        return int(await self._driver.run_in_transaction(_tx_promote))

    async def cancel(self, job_id: str) -> bool:
        """Cancel a ``pending`` or ``running`` job asynchronously."""
        return bool(
            (
                await self._driver.execute(
                    *_build_cancel_dml(
                        self._table_name,
                        shard_id=self._shard_for(job_id),
                        task_id=job_id,
                        completed_at_ms=int(time.time() * 1000),
                        is_pg=self._is_pg,
                    )
                )
            )
            > 0
        )

    async def get_entry(self, job_id: str) -> QueueEntry | None:
        """Retrieve a queue entry asynchronously by ``job_id``."""
        row = await self._driver.select_one_or_none(
            *_build_get_job(self._table_name, shard_id=self._shard_for(job_id), task_id=job_id, is_pg=self._is_pg)
        )
        return _row_to_entry(row) if row is not None else None

    async def get_entry_status(self, job_id: str) -> str | None:
        """Retrieve the current status string of ``job_id`` asynchronously, or ``None`` if absent."""
        row = await self._driver.select_one_or_none(
            *_build_get_job(self._table_name, shard_id=self._shard_for(job_id), task_id=job_id, is_pg=self._is_pg)
        )
        return str(row["status"]) if row is not None and row.get("status") is not None else None

    async def list_entries(
        self, queue: str | None = None, *, status: str | None = None, limit: int = 100
    ) -> list[QueueEntry]:
        """List queue entries matching optional ``queue`` and ``status`` up to ``limit``."""
        rows = await self._driver.select(
            *_build_list_jobs(
                self._table_name, lane=queue, status=_normalize_status_filter(status), limit=limit, is_pg=self._is_pg
            )
        )
        return [_row_to_entry(r) for r in rows]

    async def cleanup_terminal(self, *, before_ms: int, limit: int | None = None) -> int:
        """Delete terminal queue records completed before ``before_ms`` while preserving forever reservations."""
        if limit is not None and limit <= 0:
            return 0

        async def _tx_cleanup(tx: AsyncSpannerTransaction) -> int:
            if limit is not None:
                rows = await tx.select(
                    *_build_cleanup_terminal_select(
                        self._table_name, before_ms=before_ms, limit=limit, is_pg=self._is_pg
                    )
                )
                deleted = (
                    sum(
                        await tx.execute_batch_dml(
                            [
                                _build_delete_by_pk_dml(
                                    self._table_name,
                                    shard_id=int(r["shard_id"]),
                                    task_id=str(r["task_id"]),
                                    is_pg=self._is_pg,
                                )
                                for r in rows
                            ]
                        )
                    )
                    if rows
                    else 0
                )
            else:
                deleted = await tx.execute(
                    *_build_cleanup_terminal_dml(self._table_name, before_ms=before_ms, is_pg=self._is_pg)
                )
            await tx.execute(*_build_cleanup_expired_unique_keys(before_ms=before_ms, is_pg=self._is_pg))
            return deleted

        return int(await self._driver.run_in_transaction(_tx_cleanup))

    async def get_entry_by_key(self, key: str, *, queue: str | None = None) -> QueueEntry | None:
        """Retrieve a queue entry matching idempotency or reservation ``key`` and optional ``queue``."""
        row = await self._driver.select_one_or_none(
            *_build_get_entry_by_key(self._table_name, key=key, lane=queue, is_pg=self._is_pg)
        )
        return _row_to_entry(row) if row is not None else None

    async def reserve_identity(
        self,
        key: str,
        *,
        job_id: str,
        task_name: str,
        ttl_seconds: float | None = None,
    ) -> QueueReservation | None:
        """Acquire a single-winner reservation asynchronously, returning ``None`` or the existing owner."""

        async def _tx_reserve_identity(tx: AsyncSpannerTransaction) -> QueueReservation | None:
            now_tx = time.time()
            expires_tx = (now_tx + float(ttl_seconds)) if ttl_seconds is not None else None
            existing = await tx.select_one_or_none(*_build_reservation_for_update(key=key, is_pg=self._is_pg))
            if existing is not None:
                exp = existing.get("expires_at")
                if exp is None or float(exp) > now_tx:
                    return _row_to_reservation(existing)
                await tx.execute(
                    *_build_update_reservation(
                        key=key,
                        expires_at=expires_tx,
                        job_id=job_id,
                        task_name=task_name,
                        created_at_ms=int(now_tx * 1000),
                        is_pg=self._is_pg,
                    )
                )
            else:
                await tx.execute(
                    *_build_insert_reservation(
                        key=key,
                        expires_at=expires_tx,
                        job_id=job_id,
                        task_name=task_name,
                        created_at_ms=int(now_tx * 1000),
                        is_pg=self._is_pg,
                    )
                )
            return None

        return cast("QueueReservation | None", await self._driver.run_in_transaction(_tx_reserve_identity))

    async def get_reservation(self, key: str) -> QueueReservation | None:
        """Retrieve an active identity reservation asynchronously for ``key``, or ``None``."""
        row = await self._driver.select_one_or_none(
            *_build_get_reservation(key=key, now_ts=time.time(), is_pg=self._is_pg)
        )
        return _row_to_reservation(row) if row is not None else None

    async def reset_identity(self, key: str, *, expected_job_id: str | None = None) -> bool:
        """Release an identity reservation asynchronously, optionally fenced by ``expected_job_id``."""
        return bool(
            (
                await self._driver.execute(
                    *_build_reset_identity(key=key, expected_job_id=expected_job_id, is_pg=self._is_pg)
                )
            )
            > 0
        )

    async def assign_worker(self, job_id: str, *, worker_id: str, attempt: int) -> bool:
        """Record ``worker_id`` assignment asynchronously for an active fenced claim."""
        return bool(
            (
                await self._driver.execute(
                    *_build_assign_worker_dml(
                        self._table_name,
                        shard_id=self._shard_for(job_id),
                        task_id=job_id,
                        worker_id=worker_id,
                        attempt=attempt,
                        is_pg=self._is_pg,
                    )
                )
            )
            > 0
        )

    async def touch_heartbeats(self, job_ids: Sequence[str], *, heartbeat_at_ms: int) -> int:
        """Update ``heartbeat_at_ms`` asynchronously for active claimed jobs and return updated count."""
        if not job_ids:
            return 0
        return int(
            sum(
                await self._driver.execute_batch_dml(
                    self._plan_touch_heartbeat_batch(job_ids, heartbeat_at_ms=heartbeat_at_ms)
                )
            )
        )

    async def null_heartbeats(self, job_ids: Sequence[str]) -> int:
        """Clear ``heartbeat_at_ms`` asynchronously for the given ``job_ids`` and return updated count."""
        if not job_ids:
            return 0
        return int(sum(await self._driver.execute_batch_dml(self._plan_null_heartbeat_batch(job_ids))))

    async def interrupt_task(self, job_id: str, *, error: str | None = None) -> bool:
        """Interrupt a pending or running job asynchronously with optional error detail."""
        return bool(
            (
                await self._driver.execute(
                    *_build_interrupt_task_dml(
                        self._table_name,
                        shard_id=self._shard_for(job_id),
                        task_id=job_id,
                        error=error,
                        completed_at_ms=int(time.time() * 1000),
                        is_pg=self._is_pg,
                    )
                )
            )
            > 0
        )

    async def expire_overdue(self, *, now_ms: int, limit: int | None = None) -> int:
        """Mark overdue jobs with ``expires_at_ms <= now_ms`` as expired asynchronously."""
        if limit is not None and limit <= 0:
            return 0

        async def _tx_expire(tx: AsyncSpannerTransaction) -> int:
            if limit is not None:
                rows = await tx.select(
                    *_build_overdue_select(self._table_name, now_ms=now_ms, limit=limit, is_pg=self._is_pg)
                )
                if not rows:
                    return 0
                return sum(
                    await tx.execute_batch_dml(
                        [
                            _build_expire_by_pk_dml(
                                self._table_name,
                                shard_id=int(r["shard_id"]),
                                task_id=str(r["task_id"]),
                                now_ms=now_ms,
                                is_pg=self._is_pg,
                            )
                            for r in rows
                        ]
                    )
                )
            return await tx.execute(*_build_expire_overdue_dml(self._table_name, now_ms=now_ms, is_pg=self._is_pg))

        return int(await self._driver.run_in_transaction(_tx_expire))

    async def next_due_delay(self, queues: Sequence[str]) -> float | None:
        """Return seconds until the earliest scheduled job in ``queues`` is due, or ``None``."""
        if not queues:
            return None
        now = time.time()
        row = await self._driver.select_one_or_none(
            *_build_next_due_query(self._table_name, lanes=[str(q) for q in queues], now_ts=now, is_pg=self._is_pg)
        )
        if row is None or row.get("min_sched") is None:
            return None
        return max(0.0, float(row["min_sched"]) - now)

    async def reserve_unique_key(self, key: str, *, ttl_seconds: float) -> bool:
        """Conditionally reserve ``key`` for ``ttl_seconds`` without overwriting unexpired keys."""
        if self._is_pg:
            return len(await self._driver.select(*self._reserve_unique_pg(key, ttl_seconds))) > 0

        async def _tx_reserve(tx: AsyncSpannerTransaction) -> bool:
            now_tx = time.time()
            expires_tx = now_tx + float(ttl_seconds)
            existing = await tx.select_one_or_none(
                "SELECT unique_key, expires_at FROM vertebra_queue_unique_keys WHERE unique_key = @key FOR UPDATE",
                {"key": key},
            )
            if existing is not None and float(existing["expires_at"]) > now_tx:
                return False
            sql = (
                "INSERT INTO vertebra_queue_unique_keys (unique_key, expires_at) VALUES (@key, @expires_at)"
                if existing is None
                else "UPDATE vertebra_queue_unique_keys SET expires_at = @expires_at WHERE unique_key = @key"
            )
            await tx.execute(sql, {"key": key, "expires_at": expires_tx})
            return True

        return bool(await self._driver.run_in_transaction(_tx_reserve))

    async def release_unique_key(self, key: str) -> bool:
        """Release an idempotency key reservation asynchronously."""
        sql, params = self._render("DELETE FROM vertebra_queue_unique_keys WHERE unique_key = @key", {"key": key})
        return bool((await self._driver.execute(sql, params)) > 0)

    async def acquire_lease(self, lease_key: str, *, owner_id: str, ttl_seconds: float) -> bool:
        """Acquire or renew ``lease_key`` only when expired or already owned by ``owner_id``."""
        return (await self.acquire_lease_record(lease_key, owner_id=owner_id, ttl_seconds=ttl_seconds)) is not None

    async def acquire_lease_record(
        self,
        lease_key: str,
        *,
        owner_id: str,
        ttl_seconds: float,
        slot: str | None = None,
    ) -> dict[str, Any] | None:
        """Acquire or renew ``lease_key`` without stealing active foreign leases and return lease metadata."""
        if self._is_pg:
            rows = await self._driver.select(
                *self._acquire_lease_pg(lease_key, owner_id=owner_id, ttl_seconds=ttl_seconds, slot=slot)
            )
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
            record = {
                "lease_key": lease_key,
                "owner_id": owner_id,
                "expires_at": expires_tx,
                "last_slot": resolved_slot,
            }
            sql = (
                "INSERT INTO vertebra_queue_leases (lease_key, owner_id, expires_at, last_slot) "
                "VALUES (@lease_key, @owner_id, @expires_at, @last_slot)"
                if existing is None
                else "UPDATE vertebra_queue_leases SET owner_id = @owner_id, expires_at = @expires_at, last_slot = @last_slot WHERE lease_key = @lease_key"
            )
            await tx.execute(sql, record)
            return record

        return cast("dict[str, Any] | None", await self._driver.run_in_transaction(_tx_lease))

    async def release_lease(self, lease_key: str, *, owner_id: str) -> bool:
        """Release a held distributed leader lease asynchronously matching ``owner_id``."""
        sql, params = self._render(
            "DELETE FROM vertebra_queue_leases WHERE lease_key = @lease_key AND owner_id = @owner_id",
            {"lease_key": lease_key, "owner_id": owner_id},
        )
        return bool((await self._driver.execute(sql, params)) > 0)

    async def list_dlq_rows(self, *, limit: int = 100) -> list[dict[str, Any]]:
        """List dead-letter rows ordered by ``failed_at DESC``."""
        sql, params = self._render(
            "SELECT * FROM vertebra_queue_dlq ORDER BY failed_at DESC LIMIT @limit", {"limit": limit}
        )
        return cast("list[dict[str, Any]]", await self._driver.select(sql, params))

    async def pop_dlq_row(self, dlq_id: str) -> dict[str, Any] | None:
        """Atomically fetch and remove a dead-letter entry asynchronously by ``dlq_id``."""

        async def _tx_pop(tx: AsyncSpannerTransaction) -> dict[str, Any] | None:
            row = await tx.select_one_or_none(
                *self._render("SELECT * FROM vertebra_queue_dlq WHERE dlq_id = @dlq_id FOR UPDATE", {"dlq_id": dlq_id})
            )
            if row is None:
                return None
            await tx.execute(*self._render("DELETE FROM vertebra_queue_dlq WHERE dlq_id = @dlq_id", {"dlq_id": dlq_id}))
            await tx.execute(
                *self._render(
                    f"DELETE FROM {self._table_name} WHERE shard_id = @shard_id AND task_id = @task_id AND status = 'dead_letter'",
                    {"shard_id": int(row["shard_id"]), "task_id": str(row["task_id"])},
                )
            )
            return dict(row)

        return cast("dict[str, Any] | None", await self._driver.run_in_transaction(_tx_pop))

    async def aclose(self) -> None:
        """Close the underlying asynchronous Spanner driver."""
        await self._driver.close()


_PARAM_RE = re.compile(r"@([A-Za-z_][A-Za-z0-9_]*)")
_UNNEST_RE = re.compile(r"\bIN\s+UNNEST\((@[A-Za-z_][A-Za-z0-9_]*)\)")
_FORCE_INDEX_RE = re.compile(r"@\{FORCE_INDEX=[^}]+\}")


@lru_cache(maxsize=128)
def _compile_spanner_pg_template(template: str, pg_order: tuple[str, ...] | None = None) -> tuple[str, tuple[str, ...]]:
    """Compile a GoogleSQL query template into PostgreSQL-dialect Spanner SQL and parameter key order."""
    sql = _FORCE_INDEX_RE.sub("", template)
    sql = _UNNEST_RE.sub(r"= ANY(\1)", sql)
    sql = sql.replace("THEN RETURN", "RETURNING").replace("AS INT64)", "AS BIGINT)")
    mapping: dict[str, int] = {}
    keys: list[str] = []
    if pg_order is not None:
        for idx, name in enumerate(pg_order, start=1):
            mapping[name] = idx
            keys.append(name)

    def _replace(match: re.Match[str]) -> str:
        name = match.group(1)
        pos = mapping.get(name)
        if pos is None:
            pos = len(keys) + 1
            mapping[name] = pos
            keys.append(name)
        return f"${pos}"

    rendered = _PARAM_RE.sub(_replace, sql)
    return rendered, tuple(keys)


def _render_spanner_sql(
    template: str,
    params: dict[str, Any],
    *,
    is_pg: bool,
    pg_order: tuple[str, ...] | None = None,
) -> tuple[str, Any]:
    """Return dialect-appropriate ``(sql, params)`` from a single GoogleSQL template."""
    if not is_pg:
        return template, params
    pg_sql, key_order = _compile_spanner_pg_template(template, pg_order)
    return pg_sql, tuple(params[k] for k in key_order)


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
    return encode_json(headers) if headers else None


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
    return {"ready": "pending", "claimed": "running", "completed": "acked"}.get(normalized, normalized)


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
    raw_worker = row.get("worker_id")
    if raw_worker is None:
        raw_worker = row.get("consumer_name")
    raw_error = row.get("error")
    if raw_error is None:
        raw_error = row.get("last_error")
    raw_result = row.get("result")
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
        priority=int(row.get("priority") or 0),
        expires_at_ms=int(row["expires_at_ms"]) if row.get("expires_at_ms") is not None else None,
        completed_at_ms=int(row["completed_at_ms"]) if row.get("completed_at_ms") is not None else None,
        worker_id=str(raw_worker) if raw_worker is not None else None,
        heartbeat_at_ms=int(row["heartbeat_at_ms"]) if row.get("heartbeat_at_ms") is not None else None,
        result=_decode_payload(raw_result) if raw_result is not None else None,
        error=str(raw_error) if raw_error is not None else None,
    )


def _row_to_reservation(row: dict[str, Any]) -> QueueReservation:
    return QueueReservation(
        key=str(row["unique_key"]),
        job_id=str(row.get("job_id") or ""),
        task_name=str(row.get("task_name") or ""),
        created_at_ms=int(row.get("created_at_ms") or 0),
    )


def _build_idempotency_lookup(table_name: str, idem_key: str, *, is_pg: bool) -> tuple[str, Any]:
    return _render_spanner_sql(
        f"SELECT task_id FROM {table_name} "
        "WHERE idempotency_key = @idempotency_key AND status IN ('pending', 'running') LIMIT 1 FOR UPDATE",
        {"idempotency_key": idem_key},
        is_pg=is_pg,
    )


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
    priority: int,
    expires_at_ms: int | None,
    completed_at_ms: int | None,
    worker_id: str | None,
    heartbeat_at_ms: int | None,
    result: str | None,
    error: str | None,
    is_pg: bool,
) -> tuple[str, Any]:
    cols = (
        "shard_id, task_id, task_name, lane, payload, status, "
        "attempt, max_retries, scheduled_at, enqueued_at, locked_until, "
        "consumer_name, receipt_handle, idempotency_key, traceparent, headers, "
        "priority, expires_at_ms, completed_at_ms, worker_id, heartbeat_at_ms, result, error"
    )
    vals = (
        "@shard_id, @task_id, @task_name, @lane, @payload, @status, "
        "@attempt, @max_retries, @scheduled_at, @enqueued_at, @locked_until, "
        "@consumer_name, @receipt_handle, @idempotency_key, @traceparent, @headers, "
        "@priority, @expires_at_ms, @completed_at_ms, @worker_id, @heartbeat_at_ms, @result, @error"
    )
    return _render_spanner_sql(
        f"INSERT INTO {table_name} ({cols}) VALUES ({vals})",
        {
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
            "consumer_name": worker_id,
            "receipt_handle": None,
            "idempotency_key": idempotency_key,
            "traceparent": traceparent,
            "headers": headers,
            "priority": priority,
            "expires_at_ms": expires_at_ms,
            "completed_at_ms": completed_at_ms,
            "worker_id": worker_id,
            "heartbeat_at_ms": heartbeat_at_ms,
            "result": result,
            "error": error,
        },
        is_pg=is_pg,
    )


def _build_claim_select(
    table_name: str, *, lanes: list[str], now_ts: float, batch_size: int, is_pg: bool
) -> tuple[str, Any]:
    table_expr = (
        f"{table_name}@{{FORCE_INDEX=idx_vtb_spanner_queue_ready}}"
        if table_name == "vertebra_queue_jobs"
        else table_name
    )
    template = f"""
    SELECT shard_id, task_id, attempt
    FROM {table_expr}
    WHERE lane IN UNNEST(@lanes)
      AND (
          (status = 'pending' AND (scheduled_at IS NULL OR scheduled_at <= @now_ts))
          OR (status = 'running' AND locked_until <= @now_ts)
      )
    ORDER BY COALESCE(priority, 0) DESC, COALESCE(scheduled_at, enqueued_at) ASC, enqueued_at ASC
    LIMIT @batch_size
    FOR UPDATE
    """
    return _render_spanner_sql(template, {"lanes": lanes, "now_ts": now_ts, "batch_size": batch_size}, is_pg=is_pg)


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
    template = f"""
    UPDATE {table_name}
    SET status = 'running',
        attempt = attempt + 1,
        locked_until = @locked_until,
        consumer_name = @consumer_name,
        worker_id = @consumer_name,
        receipt_handle = @receipt_handle
    WHERE shard_id = @shard_id AND task_id = @task_id
    THEN RETURN *
    """
    return _render_spanner_sql(
        template,
        {
            "locked_until": locked_until,
            "consumer_name": consumer_name,
            "receipt_handle": receipt_handle,
            "shard_id": shard_id,
            "task_id": task_id,
        },
        is_pg=is_pg,
    )


def _build_ack_dml(
    table_name: str,
    *,
    shard_id: int,
    task_id: str,
    attempt: int,
    cleanup: bool,
    completed_at_ms: int,
    is_pg: bool,
) -> tuple[str, Any]:
    if cleanup:
        return _render_spanner_sql(
            f"DELETE FROM {table_name} "
            "WHERE shard_id = @shard_id AND task_id = @task_id AND attempt = @attempt AND status = 'running'",
            {"shard_id": shard_id, "task_id": task_id, "attempt": attempt},
            is_pg=is_pg,
            pg_order=("shard_id", "task_id", "attempt"),
        )
    return _render_spanner_sql(
        f"UPDATE {table_name} SET status = 'acked', locked_until = NULL, "
        "worker_id = NULL, consumer_name = NULL, completed_at_ms = @completed_at_ms "
        "WHERE shard_id = @shard_id AND task_id = @task_id AND attempt = @attempt AND status = 'running'",
        {"shard_id": shard_id, "task_id": task_id, "attempt": attempt, "completed_at_ms": completed_at_ms},
        is_pg=is_pg,
        pg_order=("shard_id", "task_id", "attempt", "completed_at_ms"),
    )


def _build_job_for_update(table_name: str, *, shard_id: int, task_id: str, is_pg: bool) -> tuple[str, Any]:
    return _render_spanner_sql(
        f"SELECT * FROM {table_name} WHERE shard_id = @shard_id AND task_id = @task_id FOR UPDATE",
        {"shard_id": shard_id, "task_id": task_id},
        is_pg=is_pg,
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
    return _render_spanner_sql(
        "INSERT INTO vertebra_queue_dlq ("
        "dlq_id, shard_id, task_id, task_name, lane, payload, error_type, error_message, failed_at"
        ") VALUES (@dlq_id, @shard_id, @task_id, @task_name, @lane, @payload, @error_type, @error_message, @failed_at)",
        {
            "dlq_id": dlq_id,
            "shard_id": shard_id,
            "task_id": task_id,
            "task_name": task_name,
            "lane": lane,
            "payload": payload,
            "error_type": error_type,
            "error_message": error_message,
            "failed_at": failed_at,
        },
        is_pg=is_pg,
    )


def _build_mark_dead_letter(
    table_name: str,
    *,
    shard_id: int,
    task_id: str,
    attempt: int,
    last_error: str,
    completed_at_ms: int,
    is_pg: bool,
) -> tuple[str, Any]:
    return _render_spanner_sql(
        f"UPDATE {table_name} SET status = 'dead_letter', locked_until = NULL, worker_id = NULL, "
        "consumer_name = NULL, receipt_handle = NULL, last_error = @last_error, error = @last_error, "
        "completed_at_ms = @completed_at_ms "
        "WHERE shard_id = @shard_id AND task_id = @task_id AND attempt = @attempt",
        {
            "last_error": last_error,
            "shard_id": shard_id,
            "task_id": task_id,
            "attempt": attempt,
            "completed_at_ms": completed_at_ms,
        },
        is_pg=is_pg,
        pg_order=("last_error", "shard_id", "task_id", "attempt", "completed_at_ms"),
    )


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
    return _render_spanner_sql(
        f"UPDATE {table_name} SET status = 'pending', scheduled_at = @scheduled_at, "
        "locked_until = NULL, consumer_name = NULL, worker_id = NULL, receipt_handle = NULL, "
        "last_error = @last_error, error = @last_error "
        "WHERE shard_id = @shard_id AND task_id = @task_id AND attempt = @attempt",
        {
            "scheduled_at": scheduled_at,
            "last_error": last_error,
            "shard_id": shard_id,
            "task_id": task_id,
            "attempt": attempt,
        },
        is_pg=is_pg,
    )


def _build_heartbeat_dml(
    table_name: str,
    *,
    shard_id: int,
    task_id: str,
    worker_id: str,
    locked_until: float,
    is_pg: bool,
) -> tuple[str, Any]:
    return _render_spanner_sql(
        f"UPDATE {table_name} SET locked_until = @locked_until "
        "WHERE shard_id = @shard_id AND task_id = @task_id AND consumer_name = @worker_id AND status = 'running'",
        {"locked_until": locked_until, "shard_id": shard_id, "task_id": task_id, "worker_id": worker_id},
        is_pg=is_pg,
    )


def _build_reap_stale_dml(table_name: str, *, now_ts: float, is_pg: bool) -> tuple[str, Any]:
    return _render_spanner_sql(
        f"UPDATE {table_name} SET status = 'pending', locked_until = NULL, consumer_name = NULL, "
        "worker_id = NULL, receipt_handle = NULL WHERE status = 'running' AND locked_until <= @now_ts",
        {"now_ts": now_ts},
        is_pg=is_pg,
    )


def _build_due_scheduled_select(table_name: str, *, now_ts: float, limit: int, is_pg: bool) -> tuple[str, Any]:
    return _render_spanner_sql(
        f"SELECT shard_id, task_id FROM {table_name} "
        "WHERE status = 'pending' AND scheduled_at IS NOT NULL AND scheduled_at <= @now_ts "
        "ORDER BY scheduled_at ASC LIMIT @limit FOR UPDATE",
        {"now_ts": now_ts, "limit": limit},
        is_pg=is_pg,
    )


def _build_clear_scheduled_dml(table_name: str, *, shard_id: int, task_id: str, is_pg: bool) -> tuple[str, Any]:
    return _render_spanner_sql(
        f"UPDATE {table_name} SET scheduled_at = NULL WHERE shard_id = @shard_id AND task_id = @task_id",
        {"shard_id": shard_id, "task_id": task_id},
        is_pg=is_pg,
    )


def _build_cancel_dml(
    table_name: str, *, shard_id: int, task_id: str, completed_at_ms: int, is_pg: bool
) -> tuple[str, Any]:
    return _render_spanner_sql(
        f"UPDATE {table_name} SET status = 'cancelled', locked_until = NULL, worker_id = NULL, "
        "consumer_name = NULL, completed_at_ms = @completed_at_ms "
        "WHERE shard_id = @shard_id AND task_id = @task_id AND status IN ('pending', 'running')",
        {"shard_id": shard_id, "task_id": task_id, "completed_at_ms": completed_at_ms},
        is_pg=is_pg,
        pg_order=("shard_id", "task_id", "completed_at_ms"),
    )


def _build_get_job(table_name: str, *, shard_id: int, task_id: str, is_pg: bool) -> tuple[str, Any]:
    return _render_spanner_sql(
        f"SELECT * FROM {table_name} WHERE shard_id = @shard_id AND task_id = @task_id",
        {"shard_id": shard_id, "task_id": task_id},
        is_pg=is_pg,
    )


def _build_list_jobs(
    table_name: str, *, lane: str | None, status: str | None, limit: int, is_pg: bool
) -> tuple[str, Any]:
    conditions: list[str] = []
    params: dict[str, Any] = {"limit": limit}
    if lane is not None:
        conditions.append("lane = @lane")
        params["lane"] = lane
    if status is not None:
        conditions.append("status = @status")
        params["status"] = status
    where_clause = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    return _render_spanner_sql(
        f"SELECT * FROM {table_name}{where_clause} ORDER BY enqueued_at ASC LIMIT @limit",
        params,
        is_pg=is_pg,
    )


def _build_cleanup_terminal_select(table_name: str, *, before_ms: int, limit: int, is_pg: bool) -> tuple[str, Any]:
    return _render_spanner_sql(
        f"SELECT shard_id, task_id FROM {table_name} "
        "WHERE status IN ('acked', 'completed', 'cancelled', 'dead_letter', 'expired') "
        "AND COALESCE(completed_at_ms, CAST(ROUND(enqueued_at * 1000) AS INT64)) < @before_ms "
        "ORDER BY COALESCE(completed_at_ms, CAST(ROUND(enqueued_at * 1000) AS INT64)) ASC "
        "LIMIT @limit FOR UPDATE",
        {"before_ms": before_ms, "limit": limit},
        is_pg=is_pg,
    )


def _build_delete_by_pk_dml(table_name: str, *, shard_id: int, task_id: str, is_pg: bool) -> tuple[str, Any]:
    return _render_spanner_sql(
        f"DELETE FROM {table_name} WHERE shard_id = @shard_id AND task_id = @task_id",
        {"shard_id": shard_id, "task_id": task_id},
        is_pg=is_pg,
    )


def _build_cleanup_terminal_dml(table_name: str, *, before_ms: int, is_pg: bool) -> tuple[str, Any]:
    return _render_spanner_sql(
        f"DELETE FROM {table_name} "
        "WHERE status IN ('acked', 'completed', 'cancelled', 'dead_letter', 'expired') "
        "AND COALESCE(completed_at_ms, CAST(ROUND(enqueued_at * 1000) AS INT64)) < @before_ms",
        {"before_ms": before_ms},
        is_pg=is_pg,
    )


def _build_cleanup_expired_unique_keys(*, before_ms: int, is_pg: bool) -> tuple[str, Any]:
    return _render_spanner_sql(
        "DELETE FROM vertebra_queue_unique_keys WHERE expires_at IS NOT NULL AND expires_at <= @before_s",
        {"before_s": before_ms / 1000.0},
        is_pg=is_pg,
    )


def _build_get_entry_by_key(table_name: str, *, key: str, lane: str | None, is_pg: bool) -> tuple[str, Any]:
    params: dict[str, Any] = {"key": key}
    lane_filter = ""
    if lane is not None:
        lane_filter = " AND j.lane = @lane"
        params["lane"] = lane
    return _render_spanner_sql(
        f"SELECT j.* FROM {table_name} j "
        "LEFT JOIN vertebra_queue_unique_keys u ON u.job_id = j.task_id "
        f"WHERE (u.unique_key = @key OR j.idempotency_key = @key){lane_filter} "
        "ORDER BY j.enqueued_at DESC LIMIT 1",
        params,
        is_pg=is_pg,
    )


def _build_reservation_for_update(*, key: str, is_pg: bool) -> tuple[str, Any]:
    return _render_spanner_sql(
        "SELECT unique_key, expires_at, job_id, task_name, created_at_ms "
        "FROM vertebra_queue_unique_keys WHERE unique_key = @key FOR UPDATE",
        {"key": key},
        is_pg=is_pg,
    )


def _build_insert_reservation(
    *, key: str, expires_at: float | None, job_id: str, task_name: str, created_at_ms: int, is_pg: bool
) -> tuple[str, Any]:
    return _render_spanner_sql(
        "INSERT INTO vertebra_queue_unique_keys (unique_key, expires_at, job_id, task_name, created_at_ms) "
        "VALUES (@key, @expires_at, @job_id, @task_name, @created_at_ms)",
        {
            "key": key,
            "expires_at": expires_at,
            "job_id": job_id,
            "task_name": task_name,
            "created_at_ms": created_at_ms,
        },
        is_pg=is_pg,
    )


def _build_update_reservation(
    *, key: str, expires_at: float | None, job_id: str, task_name: str, created_at_ms: int, is_pg: bool
) -> tuple[str, Any]:
    return _render_spanner_sql(
        "UPDATE vertebra_queue_unique_keys "
        "SET expires_at = @expires_at, job_id = @job_id, task_name = @task_name, created_at_ms = @created_at_ms "
        "WHERE unique_key = @key",
        {
            "key": key,
            "expires_at": expires_at,
            "job_id": job_id,
            "task_name": task_name,
            "created_at_ms": created_at_ms,
        },
        is_pg=is_pg,
        pg_order=("key", "expires_at", "job_id", "task_name", "created_at_ms"),
    )


def _build_get_reservation(*, key: str, now_ts: float, is_pg: bool) -> tuple[str, Any]:
    return _render_spanner_sql(
        "SELECT unique_key, job_id, task_name, created_at_ms "
        "FROM vertebra_queue_unique_keys "
        "WHERE unique_key = @key AND (expires_at IS NULL OR expires_at > @now_ts)",
        {"key": key, "now_ts": now_ts},
        is_pg=is_pg,
    )


def _build_reset_identity(*, key: str, expected_job_id: str | None, is_pg: bool) -> tuple[str, Any]:
    if expected_job_id is not None:
        return _render_spanner_sql(
            "DELETE FROM vertebra_queue_unique_keys WHERE unique_key = @key AND job_id = @expected_job_id",
            {"key": key, "expected_job_id": expected_job_id},
            is_pg=is_pg,
        )
    return _render_spanner_sql(
        "DELETE FROM vertebra_queue_unique_keys WHERE unique_key = @key",
        {"key": key},
        is_pg=is_pg,
    )


def _build_assign_worker_dml(
    table_name: str, *, shard_id: int, task_id: str, worker_id: str, attempt: int, is_pg: bool
) -> tuple[str, Any]:
    return _render_spanner_sql(
        f"UPDATE {table_name} SET worker_id = @worker_id, consumer_name = @worker_id "
        "WHERE shard_id = @shard_id AND task_id = @task_id AND attempt = @attempt AND status = 'running'",
        {"worker_id": worker_id, "shard_id": shard_id, "task_id": task_id, "attempt": attempt},
        is_pg=is_pg,
    )


def _build_touch_heartbeat_dml(
    table_name: str, *, shard_id: int, task_id: str, heartbeat_at_ms: int, is_pg: bool
) -> tuple[str, Any]:
    return _render_spanner_sql(
        f"UPDATE {table_name} SET heartbeat_at_ms = @heartbeat_at_ms "
        "WHERE shard_id = @shard_id AND task_id = @task_id AND status = 'running'",
        {"heartbeat_at_ms": heartbeat_at_ms, "shard_id": shard_id, "task_id": task_id},
        is_pg=is_pg,
    )


def _build_null_heartbeat_dml(table_name: str, *, shard_id: int, task_id: str, is_pg: bool) -> tuple[str, Any]:
    return _render_spanner_sql(
        f"UPDATE {table_name} SET heartbeat_at_ms = NULL WHERE shard_id = @shard_id AND task_id = @task_id",
        {"shard_id": shard_id, "task_id": task_id},
        is_pg=is_pg,
    )


def _build_interrupt_task_dml(
    table_name: str,
    *,
    shard_id: int,
    task_id: str,
    error: str | None,
    completed_at_ms: int,
    is_pg: bool,
) -> tuple[str, Any]:
    params: dict[str, Any] = {"shard_id": shard_id, "task_id": task_id, "completed_at_ms": completed_at_ms}
    if error is not None:
        params["error"] = error
        return _render_spanner_sql(
            f"UPDATE {table_name} SET status = 'cancelled', locked_until = NULL, worker_id = NULL, "
            "consumer_name = NULL, completed_at_ms = @completed_at_ms, error = @error, last_error = @error "
            "WHERE shard_id = @shard_id AND task_id = @task_id AND status IN ('pending', 'running')",
            params,
            is_pg=is_pg,
            pg_order=("shard_id", "task_id", "completed_at_ms", "error"),
        )
    return _render_spanner_sql(
        f"UPDATE {table_name} SET status = 'cancelled', locked_until = NULL, worker_id = NULL, "
        "consumer_name = NULL, completed_at_ms = @completed_at_ms "
        "WHERE shard_id = @shard_id AND task_id = @task_id AND status IN ('pending', 'running')",
        params,
        is_pg=is_pg,
        pg_order=("shard_id", "task_id", "completed_at_ms"),
    )


def _build_overdue_select(table_name: str, *, now_ms: int, limit: int, is_pg: bool) -> tuple[str, Any]:
    return _render_spanner_sql(
        f"SELECT shard_id, task_id FROM {table_name} "
        "WHERE status IN ('pending', 'running') AND expires_at_ms IS NOT NULL AND expires_at_ms <= @now_ms "
        "ORDER BY expires_at_ms ASC LIMIT @limit FOR UPDATE",
        {"now_ms": now_ms, "limit": limit},
        is_pg=is_pg,
    )


def _build_expire_by_pk_dml(
    table_name: str, *, shard_id: int, task_id: str, now_ms: int, is_pg: bool
) -> tuple[str, Any]:
    return _render_spanner_sql(
        f"UPDATE {table_name} SET status = 'expired', locked_until = NULL, worker_id = NULL, "
        "consumer_name = NULL, completed_at_ms = @now_ms WHERE shard_id = @shard_id AND task_id = @task_id",
        {"shard_id": shard_id, "task_id": task_id, "now_ms": now_ms},
        is_pg=is_pg,
        pg_order=("shard_id", "task_id", "now_ms"),
    )


def _build_expire_overdue_dml(table_name: str, *, now_ms: int, is_pg: bool) -> tuple[str, Any]:
    return _render_spanner_sql(
        f"UPDATE {table_name} SET status = 'expired', locked_until = NULL, worker_id = NULL, "
        "consumer_name = NULL, completed_at_ms = @now_ms "
        "WHERE status IN ('pending', 'running') AND expires_at_ms IS NOT NULL AND expires_at_ms <= @now_ms",
        {"now_ms": now_ms},
        is_pg=is_pg,
    )


def _build_next_due_query(table_name: str, *, lanes: list[str], now_ts: float, is_pg: bool) -> tuple[str, Any]:
    return _render_spanner_sql(
        f"SELECT MIN(scheduled_at) AS min_sched FROM {table_name} "
        "WHERE lane IN UNNEST(@lanes) AND status = 'pending' AND scheduled_at > @now_ts",
        {"lanes": lanes, "now_ts": now_ts},
        is_pg=is_pg,
    )
