"""Shared RESP3 Lua scripts and durable queue store base implementations."""

from __future__ import annotations

import base64
import contextlib
import time
import uuid
from typing import TYPE_CHECKING, Any

from vertebra.db.driver.capabilities import (
    AsyncQueueStore,
    QueueEntry,
    QueueReservation,
    SyncQueueStore,
)
from vertebra.utils.serializers import decode_json, encode_json

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = (
    "AsyncRESPQueueStore",
    "RESPQueueStoreBase",
    "SyncRESPQueueStore",
)

_STORE_ENQUEUE_LUA = """
local jobs_key = KEYS[1]
local order_key = KEYS[2]
local seq_key = KEYS[3]
local scheduled_key = KEYS[4]
local expires_key = KEYS[5]
local by_key_hash = KEYS[6]
local ready_key = KEYS[7]
local job_id = ARGV[1]
local queue = ARGV[2]
local record_json = ARGV[3]
local now_ms = tonumber(ARGV[4])
local scheduled_at_ms = ARGV[5]
local priority = tonumber(ARGV[6])
local expires_at_ms = ARGV[7]
local idem_key = ARGV[8]
local seq = redis.call('INCR', seq_key)
local data = cjson.decode(record_json)
data.seq = seq
redis.call('HSET', jobs_key, job_id, cjson.encode(data))
redis.call('ZADD', order_key, seq, job_id)
if scheduled_at_ms ~= '' and tonumber(scheduled_at_ms) > now_ms then
    redis.call('ZADD', scheduled_key, tonumber(scheduled_at_ms), job_id)
else
    local score = (-priority * 1000000) + seq
    redis.call('ZADD', ready_key, score, job_id)
end
if expires_at_ms ~= '' then
    redis.call('ZADD', expires_key, tonumber(expires_at_ms), job_id)
end
if idem_key ~= '' then
    redis.call('HSET', by_key_hash, 'any:' .. idem_key, job_id)
    redis.call('HSET', by_key_hash, 'q:' .. queue .. ':' .. idem_key, job_id)
end
return job_id
"""

_STORE_CLAIM_LUA = """
local jobs_key = KEYS[1]
local claimed_key = KEYS[2]
local limit = tonumber(ARGV[1])
local worker_id = ARGV[2]
local lease_until_ms = tonumber(ARGV[3])
local now_ms = tonumber(ARGV[4])
local claimed = {}
for i = 3, #KEYS do
    local remaining = limit - #claimed
    if remaining <= 0 then
        break
    end
    local ready_key = KEYS[i]
    local candidates = redis.call('ZRANGE', ready_key, 0, remaining - 1)
    for _, job_id in ipairs(candidates) do
        redis.call('ZREM', ready_key, job_id)
        local raw = redis.call('HGET', jobs_key, job_id)
        if raw then
            local data = cjson.decode(raw)
            if data.status == 'ready' then
                data.status = 'claimed'
                data.worker_id = worker_id
                data.lease_until_ms = lease_until_ms
                data.updated_at_ms = now_ms
                local updated = cjson.encode(data)
                redis.call('HSET', jobs_key, job_id, updated)
                redis.call('ZADD', claimed_key, lease_until_ms, job_id)
                table.insert(claimed, updated)
            end
        end
    end
end
return claimed
"""

_STORE_ACK_LUA = """
local jobs_key = KEYS[1]
local claimed_key = KEYS[2]
local terminal_key = KEYS[3]
local expires_key = KEYS[4]
local job_id = ARGV[1]
local attempt = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local raw = redis.call('HGET', jobs_key, job_id)
if not raw then
    return 0
end
local data = cjson.decode(raw)
if data.status ~= 'claimed' or tonumber(data.attempt) ~= attempt then
    return 0
end
local comp_ms = data.completed_at_ms
if comp_ms == nil or comp_ms == cjson.null then
    comp_ms = now_ms
end
data.status = 'completed'
data.lease_until_ms = 0
data.completed_at_ms = comp_ms
data.updated_at_ms = now_ms
redis.call('HSET', jobs_key, job_id, cjson.encode(data))
redis.call('ZREM', claimed_key, job_id)
redis.call('ZREM', expires_key, job_id)
redis.call('ZADD', terminal_key, tonumber(comp_ms), job_id)
return 1
"""

_STORE_NACK_LUA = """
local jobs_key = KEYS[1]
local claimed_key = KEYS[2]
local scheduled_key = KEYS[3]
local terminal_key = KEYS[4]
local expires_key = KEYS[5]
local job_id = ARGV[1]
local attempt = tonumber(ARGV[2])
local error_msg = ARGV[3]
local retry_delay_ms = ARGV[4]
local now_ms = tonumber(ARGV[5])
local tag_prefix = ARGV[6]
local raw = redis.call('HGET', jobs_key, job_id)
if not raw then
    return 0
end
local data = cjson.decode(raw)
if data.status ~= 'claimed' or tonumber(data.attempt) ~= attempt then
    return 0
end
redis.call('ZREM', claimed_key, job_id)
data.error = error_msg
data.lease_until_ms = 0
data.worker_id = cjson.null
data.updated_at_ms = now_ms
if tonumber(data.attempt) >= tonumber(data.max_attempts) or retry_delay_ms == '' then
    local comp_ms = data.completed_at_ms
    if comp_ms == nil or comp_ms == cjson.null then
        comp_ms = now_ms
    end
    data.status = 'dead_letter'
    data.completed_at_ms = comp_ms
    redis.call('ZREM', expires_key, job_id)
    redis.call('ZADD', terminal_key, tonumber(comp_ms), job_id)
else
    data.attempt = tonumber(data.attempt) + 1
    data.status = 'ready'
    local delay_val = tonumber(retry_delay_ms)
    if delay_val and delay_val > 0 then
        local sched = now_ms + delay_val
        data.scheduled_at_ms = sched
        redis.call('ZADD', scheduled_key, sched, job_id)
    else
        data.scheduled_at_ms = cjson.null
        local prio = tonumber(data.priority or 0)
        local seq = tonumber(data.seq or 0)
        local ready_key = tag_prefix .. ':ready:' .. data.queue
        redis.call('ZADD', ready_key, (-prio * 1000000) + seq, job_id)
    end
end
redis.call('HSET', jobs_key, job_id, cjson.encode(data))
return 1
"""

_STORE_HEARTBEAT_LUA = """
local jobs_key = KEYS[1]
local claimed_key = KEYS[2]
local worker_id = ARGV[1]
local lease_until_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local touched = 0
for i = 4, #ARGV do
    local job_id = ARGV[i]
    local raw = redis.call('HGET', jobs_key, job_id)
    if raw then
        local data = cjson.decode(raw)
        if data.status == 'claimed' and data.worker_id == worker_id then
            data.lease_until_ms = lease_until_ms
            data.updated_at_ms = now_ms
            redis.call('HSET', jobs_key, job_id, cjson.encode(data))
            redis.call('ZADD', claimed_key, lease_until_ms, job_id)
            touched = touched + 1
        end
    end
end
return touched
"""

_STORE_REAP_STALE_LUA = """
local jobs_key = KEYS[1]
local claimed_key = KEYS[2]
local terminal_key = KEYS[3]
local expires_key = KEYS[4]
local now_ms = tonumber(ARGV[1])
local tag_prefix = ARGV[2]
local stale = redis.call('ZRANGEBYSCORE', claimed_key, '-inf', now_ms)
local reaped = 0
for _, job_id in ipairs(stale) do
    redis.call('ZREM', claimed_key, job_id)
    local raw = redis.call('HGET', jobs_key, job_id)
    if raw then
        local data = cjson.decode(raw)
        if data.status == 'claimed' then
            data.worker_id = cjson.null
            data.lease_until_ms = 0
            data.updated_at_ms = now_ms
            if tonumber(data.attempt) >= tonumber(data.max_attempts) then
                local comp_ms = data.completed_at_ms
                if comp_ms == nil or comp_ms == cjson.null then
                    comp_ms = now_ms
                end
                data.status = 'dead_letter'
                data.completed_at_ms = comp_ms
                if data.error == nil or data.error == cjson.null then
                    data.error = 'Visibility lease expired'
                end
                redis.call('ZREM', expires_key, job_id)
                redis.call('ZADD', terminal_key, tonumber(comp_ms), job_id)
            else
                data.status = 'ready'
                data.attempt = tonumber(data.attempt) + 1
                local prio = tonumber(data.priority or 0)
                local seq = tonumber(data.seq or 0)
                local ready_key = tag_prefix .. ':ready:' .. data.queue
                redis.call('ZADD', ready_key, (-prio * 1000000) + seq, job_id)
            end
            redis.call('HSET', jobs_key, job_id, cjson.encode(data))
            reaped = reaped + 1
        end
    end
end
return reaped
"""

_STORE_PROMOTE_SCHEDULED_LUA = """
local jobs_key = KEYS[1]
local scheduled_key = KEYS[2]
local now_ms = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local tag_prefix = ARGV[3]
local items = redis.call('ZRANGEBYSCORE', scheduled_key, '-inf', now_ms, 'LIMIT', 0, limit)
local promoted = 0
for _, job_id in ipairs(items) do
    redis.call('ZREM', scheduled_key, job_id)
    local raw = redis.call('HGET', jobs_key, job_id)
    if raw then
        local data = cjson.decode(raw)
        if data.status == 'ready' then
            data.scheduled_at_ms = cjson.null
            data.updated_at_ms = now_ms
            local prio = tonumber(data.priority or 0)
            local seq = tonumber(data.seq or 0)
            local ready_key = tag_prefix .. ':ready:' .. data.queue
            redis.call('ZADD', ready_key, (-prio * 1000000) + seq, job_id)
            redis.call('HSET', jobs_key, job_id, cjson.encode(data))
            promoted = promoted + 1
        end
    end
end
return promoted
"""

_STORE_CANCEL_LUA = """
local jobs_key = KEYS[1]
local scheduled_key = KEYS[2]
local terminal_key = KEYS[3]
local expires_key = KEYS[4]
local job_id = ARGV[1]
local now_ms = tonumber(ARGV[2])
local tag_prefix = ARGV[3]
local raw = redis.call('HGET', jobs_key, job_id)
if not raw then
    return 0
end
local data = cjson.decode(raw)
if data.status ~= 'ready' then
    return 0
end
local comp_ms = data.completed_at_ms
if comp_ms == nil or comp_ms == cjson.null then
    comp_ms = now_ms
end
data.status = 'cancelled'
data.lease_until_ms = 0
data.completed_at_ms = comp_ms
data.updated_at_ms = now_ms
redis.call('HSET', jobs_key, job_id, cjson.encode(data))
redis.call('ZREM', tag_prefix .. ':ready:' .. data.queue, job_id)
redis.call('ZREM', scheduled_key, job_id)
redis.call('ZREM', expires_key, job_id)
redis.call('ZADD', terminal_key, tonumber(comp_ms), job_id)
return 1
"""

_STORE_CLEANUP_TERMINAL_LUA = """
local jobs_key = KEYS[1]
local order_key = KEYS[2]
local terminal_key = KEYS[3]
local expires_key = KEYS[4]
local claimed_key = KEYS[5]
local by_key_hash = KEYS[6]
local before_ms = ARGV[1]
local limit = tonumber(ARGV[2])
local items
if limit >= 0 then
    items = redis.call('ZRANGEBYSCORE', terminal_key, '-inf', '(' .. before_ms, 'LIMIT', 0, limit)
else
    items = redis.call('ZRANGEBYSCORE', terminal_key, '-inf', '(' .. before_ms)
end
local deleted = 0
for _, job_id in ipairs(items) do
    local raw = redis.call('HGET', jobs_key, job_id)
    if raw then
        local data = cjson.decode(raw)
        if type(data.headers) == 'table' then
            local k1 = data.headers.key
            local k2 = data.headers.idempotency_key
            if k1 and k1 ~= '' then
                if redis.call('HGET', by_key_hash, 'any:' .. k1) == job_id then
                    redis.call('HDEL', by_key_hash, 'any:' .. k1)
                end
                if redis.call('HGET', by_key_hash, 'q:' .. data.queue .. ':' .. k1) == job_id then
                    redis.call('HDEL', by_key_hash, 'q:' .. data.queue .. ':' .. k1)
                end
            end
            if k2 and k2 ~= '' then
                if redis.call('HGET', by_key_hash, 'any:' .. k2) == job_id then
                    redis.call('HDEL', by_key_hash, 'any:' .. k2)
                end
                if redis.call('HGET', by_key_hash, 'q:' .. data.queue .. ':' .. k2) == job_id then
                    redis.call('HDEL', by_key_hash, 'q:' .. data.queue .. ':' .. k2)
                end
            end
        end
    end
    redis.call('HDEL', jobs_key, job_id)
    redis.call('ZREM', order_key, job_id)
    redis.call('ZREM', terminal_key, job_id)
    redis.call('ZREM', expires_key, job_id)
    redis.call('ZREM', claimed_key, job_id)
    deleted = deleted + 1
end
return deleted
"""

_STORE_RESERVE_IDENTITY_LUA = """
local res_key = KEYS[1]
local key_name = ARGV[1]
local job_id = ARGV[2]
local task_name = ARGV[3]
local now_ms = tonumber(ARGV[4])
local ttl_ms = tonumber(ARGV[5])
local existing = redis.call('GET', res_key)
if existing then
    local ok, data = pcall(cjson.decode, existing)
    if ok and data then
        local exp = data.expires_at_ms
        if exp == nil or exp == cjson.null or tonumber(exp) > now_ms then
            return existing
        end
    end
end
local expires_at_ms = cjson.null
if ttl_ms >= 0 then
    expires_at_ms = now_ms + ttl_ms
end
local payload = cjson.encode({
    key = key_name,
    job_id = job_id,
    task_name = task_name,
    created_at_ms = now_ms,
    expires_at_ms = expires_at_ms
})
if ttl_ms > 0 then
    redis.call('SET', res_key, payload, 'PX', ttl_ms)
elseif ttl_ms < 0 then
    redis.call('SET', res_key, payload)
end
return nil
"""

_STORE_RESET_IDENTITY_LUA = """
local res_key = KEYS[1]
local expected_job_id = ARGV[1]
local existing = redis.call('GET', res_key)
if not existing then
    return 0
end
if expected_job_id ~= '' then
    local ok, data = pcall(cjson.decode, existing)
    if not ok or not data or data.job_id ~= expected_job_id then
        return 0
    end
end
redis.call('DEL', res_key)
return 1
"""

_STORE_ASSIGN_WORKER_LUA = """
local jobs_key = KEYS[1]
local job_id = ARGV[1]
local worker_id = ARGV[2]
local attempt = tonumber(ARGV[3])
local now_ms = tonumber(ARGV[4])
local raw = redis.call('HGET', jobs_key, job_id)
if not raw then
    return 0
end
local data = cjson.decode(raw)
if data.status ~= 'claimed' or tonumber(data.attempt) ~= attempt then
    return 0
end
data.worker_id = worker_id
data.updated_at_ms = now_ms
redis.call('HSET', jobs_key, job_id, cjson.encode(data))
return 1
"""

_STORE_TOUCH_HEARTBEATS_LUA = """
local jobs_key = KEYS[1]
local heartbeat_at_ms = tonumber(ARGV[1])
local now_ms = tonumber(ARGV[2])
local touched = 0
for i = 3, #ARGV do
    local raw = redis.call('HGET', jobs_key, ARGV[i])
    if raw then
        local data = cjson.decode(raw)
        if data.status == 'claimed' then
            data.heartbeat_at_ms = heartbeat_at_ms
            data.updated_at_ms = now_ms
            redis.call('HSET', jobs_key, ARGV[i], cjson.encode(data))
            touched = touched + 1
        end
    end
end
return touched
"""

_STORE_NULL_HEARTBEATS_LUA = """
local jobs_key = KEYS[1]
local now_ms = tonumber(ARGV[1])
local cleared = 0
for i = 2, #ARGV do
    local raw = redis.call('HGET', jobs_key, ARGV[i])
    if raw then
        local data = cjson.decode(raw)
        if data.heartbeat_at_ms ~= nil and data.heartbeat_at_ms ~= cjson.null then
            data.heartbeat_at_ms = cjson.null
            data.updated_at_ms = now_ms
            redis.call('HSET', jobs_key, ARGV[i], cjson.encode(data))
            cleared = cleared + 1
        end
    end
end
return cleared
"""

_STORE_INTERRUPT_TASK_LUA = """
local jobs_key = KEYS[1]
local claimed_key = KEYS[2]
local scheduled_key = KEYS[3]
local terminal_key = KEYS[4]
local expires_key = KEYS[5]
local job_id = ARGV[1]
local has_error = ARGV[2]
local error_msg = ARGV[3]
local now_ms = tonumber(ARGV[4])
local tag_prefix = ARGV[5]
local raw = redis.call('HGET', jobs_key, job_id)
if not raw then
    return 0
end
local data = cjson.decode(raw)
if data.status ~= 'ready' and data.status ~= 'claimed' then
    return 0
end
local comp_ms = data.completed_at_ms
if comp_ms == nil or comp_ms == cjson.null then
    comp_ms = now_ms
end
data.status = 'interrupted'
data.lease_until_ms = 0
data.worker_id = cjson.null
data.completed_at_ms = comp_ms
data.updated_at_ms = now_ms
if has_error == '1' then
    data.error = error_msg
end
redis.call('HSET', jobs_key, job_id, cjson.encode(data))
redis.call('ZREM', tag_prefix .. ':ready:' .. data.queue, job_id)
redis.call('ZREM', claimed_key, job_id)
redis.call('ZREM', scheduled_key, job_id)
redis.call('ZREM', expires_key, job_id)
redis.call('ZADD', terminal_key, tonumber(comp_ms), job_id)
return 1
"""

_STORE_EXPIRE_OVERDUE_LUA = """
local jobs_key = KEYS[1]
local expires_key = KEYS[2]
local scheduled_key = KEYS[3]
local terminal_key = KEYS[4]
local now_ms = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local tag_prefix = ARGV[3]
local items
if limit >= 0 then
    items = redis.call('ZRANGEBYSCORE', expires_key, '-inf', now_ms, 'LIMIT', 0, limit)
else
    items = redis.call('ZRANGEBYSCORE', expires_key, '-inf', now_ms)
end
local expired = 0
for _, job_id in ipairs(items) do
    local raw = redis.call('HGET', jobs_key, job_id)
    if raw then
        local data = cjson.decode(raw)
        if data.status == 'ready' and data.expires_at_ms ~= nil and data.expires_at_ms ~= cjson.null and tonumber(data.expires_at_ms) <= now_ms then
            local comp_ms = data.completed_at_ms
            if comp_ms == nil or comp_ms == cjson.null then
                comp_ms = now_ms
            end
            data.status = 'expired'
            data.lease_until_ms = 0
            data.worker_id = cjson.null
            data.completed_at_ms = comp_ms
            data.updated_at_ms = now_ms
            redis.call('HSET', jobs_key, job_id, cjson.encode(data))
            redis.call('ZREM', tag_prefix .. ':ready:' .. data.queue, job_id)
            redis.call('ZREM', scheduled_key, job_id)
            redis.call('ZREM', expires_key, job_id)
            redis.call('ZADD', terminal_key, tonumber(comp_ms), job_id)
            expired = expired + 1
        end
    end
end
return expired
"""

_STORE_ACQUIRE_LEASE_LUA = """
local lease_key = KEYS[1]
local owner_id = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local current = redis.call('GET', lease_key)
if not current or current == owner_id then
    redis.call('SET', lease_key, owner_id, 'PX', ttl_ms)
    return 1
end
return 0
"""

_STORE_RELEASE_LEASE_LUA = """
local lease_key = KEYS[1]
local owner_id = ARGV[1]
if redis.call('GET', lease_key) == owner_id then
    redis.call('DEL', lease_key)
    return 1
end
return 0
"""


def _decode_str(raw: Any) -> str:
    if isinstance(raw, bytes):
        return raw.decode("utf-8")
    return str(raw)


def _decode_json_value(raw: Any) -> Any:
    if raw is None:
        return None
    raw_bytes = raw.encode("utf-8") if isinstance(raw, str) else bytes(raw)
    return decode_json(raw_bytes)


def _parse_headers_map(raw: Any) -> dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    return {str(k): str(v) for k, v in raw.items()}


def _record_to_entry(rec: dict[str, Any]) -> QueueEntry:
    payload_b64 = rec.get("payload_b64")
    payload = base64.b64decode(payload_b64) if isinstance(payload_b64, str) else b""
    result_b64 = rec.get("result_b64")
    result = base64.b64decode(result_b64) if isinstance(result_b64, str) else None
    sched = rec.get("scheduled_at_ms")
    exp = rec.get("expires_at_ms")
    comp = rec.get("completed_at_ms")
    hb = rec.get("heartbeat_at_ms")
    return QueueEntry(
        job_id=str(rec["job_id"]),
        queue=str(rec["queue"]),
        task_name=str(rec["task_name"]),
        payload=payload,
        attempt=int(rec.get("attempt", 1)),
        max_attempts=int(rec.get("max_attempts", 3)),
        lease_until_ms=int(rec.get("lease_until_ms", 0)),
        scheduled_at_ms=int(sched) if sched is not None else None,
        headers=_parse_headers_map(rec.get("headers")),
        priority=int(rec.get("priority", 0)),
        expires_at_ms=int(exp) if exp is not None else None,
        completed_at_ms=int(comp) if comp is not None else None,
        worker_id=str(rec["worker_id"]) if rec.get("worker_id") is not None else None,
        heartbeat_at_ms=int(hb) if hb is not None else None,
        result=result,
        error=str(rec["error"]) if rec.get("error") is not None else None,
    )


def _record_to_reservation(rec: dict[str, Any]) -> QueueReservation:
    return QueueReservation(
        key=str(rec["key"]),
        job_id=str(rec["job_id"]),
        task_name=str(rec["task_name"]),
        created_at_ms=int(rec["created_at_ms"]),
    )


def _parse_zrange_withscores(raw: Any) -> list[tuple[str, float]]:
    if not raw:
        return []
    pairs: list[tuple[str, float]] = []
    if isinstance(raw[0], (list, tuple)) and len(raw[0]) == 2:
        for item in raw:
            pairs.append((_decode_str(item[0]), float(item[1])))
        return pairs
    for idx in range(0, len(raw) - 1, 2):
        pairs.append((_decode_str(raw[idx]), float(raw[idx + 1])))
    return pairs


class RESPQueueStoreBase:
    """Shared RESP key-space, Lua script preparation, and record serialization for Redis and Valkey queue stores."""

    __slots__ = ("_driver", "_notification_source", "_prefix", "_tag")

    def __init__(
        self,
        driver: Any,
        *,
        notification_source: Any = None,
        key_prefix: str = "vtb:queue",
    ) -> None:
        """Initialize shared RESP queue store state."""
        self._driver = driver
        self._notification_source = notification_source
        self._prefix = key_prefix.strip(":") or "vtb:queue"
        self._tag = f"{{{self._prefix}}}"

    @property
    def _jobs_key(self) -> str:
        return f"{self._tag}:jobs"

    @property
    def _order_key(self) -> str:
        return f"{self._tag}:order"

    @property
    def _seq_key(self) -> str:
        return f"{self._tag}:seq"

    @property
    def _scheduled_key(self) -> str:
        return f"{self._tag}:scheduled"

    @property
    def _claimed_key(self) -> str:
        return f"{self._tag}:claimed"

    @property
    def _expires_key(self) -> str:
        return f"{self._tag}:expires"

    @property
    def _terminal_key(self) -> str:
        return f"{self._tag}:terminal"

    @property
    def _by_key_hash(self) -> str:
        return f"{self._tag}:by_key"

    def _ready_key(self, queue: str) -> str:
        return f"{self._tag}:ready:{queue}"

    def _res_key(self, key: str) -> str:
        return f"{self._tag}:res:{key}"

    def _uniq_key(self, key: str) -> str:
        return f"{self._tag}:uniq:{key}"

    def _lease_key(self, lease_key: str) -> str:
        return f"{self._tag}:lease:{lease_key}"

    def _build_enqueue_args(self, entry: QueueEntry, now_ms: int) -> tuple[list[str], list[Any], str]:
        jid = entry.job_id or uuid.uuid4().hex
        record = {
            "job_id": jid,
            "queue": entry.queue,
            "task_name": entry.task_name,
            "payload_b64": base64.b64encode(entry.payload).decode("ascii"),
            "attempt": entry.attempt,
            "max_attempts": entry.max_attempts,
            "status": "ready",
            "lease_until_ms": entry.lease_until_ms,
            "scheduled_at_ms": entry.scheduled_at_ms,
            "headers": dict(entry.headers) if entry.headers else {},
            "priority": entry.priority,
            "expires_at_ms": entry.expires_at_ms,
            "completed_at_ms": entry.completed_at_ms,
            "worker_id": entry.worker_id,
            "heartbeat_at_ms": entry.heartbeat_at_ms,
            "result_b64": base64.b64encode(entry.result).decode("ascii") if entry.result is not None else None,
            "error": entry.error,
            "created_at_ms": now_ms,
            "updated_at_ms": now_ms,
        }
        idem_key = ""
        if entry.headers:
            idem_key = str(entry.headers.get("key") or entry.headers.get("idempotency_key") or "")
        keys = [
            self._jobs_key,
            self._order_key,
            self._seq_key,
            self._scheduled_key,
            self._expires_key,
            self._by_key_hash,
            self._ready_key(entry.queue),
        ]
        args: list[Any] = [
            jid,
            entry.queue,
            encode_json(record),
            str(now_ms),
            str(entry.scheduled_at_ms) if entry.scheduled_at_ms is not None else "",
            str(entry.priority),
            str(entry.expires_at_ms) if entry.expires_at_ms is not None else "",
            idem_key,
        ]
        return keys, args, jid

    def _decode_claimed_records(self, raw_items: Any) -> list[QueueEntry]:
        if not raw_items:
            return []
        entries: list[QueueEntry] = []
        for raw in raw_items:
            decoded = _decode_json_value(raw)
            if isinstance(decoded, dict):
                entries.append(_record_to_entry(decoded))
        return entries

    def _filter_entries(
        self,
        raw_records: Any,
        *,
        queue: str | None,
        status: str | None,
        limit: int,
    ) -> list[QueueEntry]:
        if not raw_records:
            return []
        matched: list[QueueEntry] = []
        for raw in raw_records:
            if raw is None:
                continue
            decoded = _decode_json_value(raw)
            if not isinstance(decoded, dict):
                continue
            if queue is not None and decoded.get("queue") != queue:
                continue
            if status is not None and decoded.get("status") != status:
                continue
            matched.append(_record_to_entry(decoded))
            if len(matched) >= limit:
                break
        return matched

    def _decode_reservation_if_active(self, raw: Any, now_ms: int) -> QueueReservation | None:
        decoded = _decode_json_value(raw)
        if not isinstance(decoded, dict):
            return None
        exp = decoded.get("expires_at_ms")
        if exp is not None and int(exp) <= now_ms:
            return None
        return _record_to_reservation(decoded)

    def _compute_next_due_delay(
        self,
        zrange_raw: Any,
        raw_records: Any,
        queues: Sequence[str],
        now_ms: int,
    ) -> float | None:
        pairs = _parse_zrange_withscores(zrange_raw)
        if not pairs or not raw_records:
            return None
        queue_set = set(queues)
        min_sched: int | None = None
        for (_, score), raw in zip(pairs, raw_records, strict=False):
            if raw is None:
                continue
            decoded = _decode_json_value(raw)
            if not isinstance(decoded, dict):
                continue
            if decoded.get("status") != "ready" or decoded.get("queue") not in queue_set:
                continue
            sched_ms = int(score)
            if sched_ms > now_ms and (min_sched is None or sched_ms < min_sched):
                min_sched = sched_ms
        if min_sched is None:
            return None
        return max(0.0, (min_sched - now_ms) / 1000.0)


class SyncRESPQueueStore(RESPQueueStoreBase, SyncQueueStore):
    """Synchronous RESP queue store implementing the full SyncQueueStore capability protocol."""

    __slots__ = ()

    def ensure_schema(self) -> None:
        """Schema-less RESP store initialization no-op."""
        return None

    def enqueue(self, entry: QueueEntry) -> str:
        """Enqueue a single job entry and return its assigned job_id."""
        now_ms = int(time.time() * 1000)
        keys, args, jid = self._build_enqueue_args(entry, now_ms)
        self._driver.eval_script(_STORE_ENQUEUE_LUA, keys=keys, args=args)
        if self._notification_source is not None:
            with contextlib.suppress(Exception):
                self._notification_source.publish(f"vtb:queue:{entry.queue}", f"wakeup:{jid}")
        return jid

    def enqueue_many(self, entries: Sequence[QueueEntry]) -> list[str]:
        """Bulk-enqueue job entries and return their assigned job_id values."""
        return [self.enqueue(entry) for entry in entries]

    def claim(
        self,
        queues: Sequence[str],
        *,
        limit: int = 1,
        worker_id: str,
        lease_seconds: float = 30.0,
    ) -> list[QueueEntry]:
        """Atomically claim up to limit ready jobs under a visibility lease."""
        if not queues or limit <= 0:
            return []
        now_ms = int(time.time() * 1000)
        lease_until_ms = now_ms + int(lease_seconds * 1000)
        keys = [self._jobs_key, self._claimed_key, *(self._ready_key(q) for q in queues)]
        raw_items = self._driver.eval_script(
            _STORE_CLAIM_LUA,
            keys=keys,
            args=[str(limit), worker_id, str(lease_until_ms), str(now_ms)],
        )
        return self._decode_claimed_records(raw_items)

    def ack(self, job_id: str, *, attempt: int) -> bool:
        """Mark a fenced (job_id, attempt) claim as completed."""
        now_ms = int(time.time() * 1000)
        res = self._driver.eval_script(
            _STORE_ACK_LUA,
            keys=[self._jobs_key, self._claimed_key, self._terminal_key, self._expires_key],
            args=[job_id, str(attempt), str(now_ms)],
        )
        return int(res or 0) > 0

    def nack(
        self,
        job_id: str,
        *,
        attempt: int,
        error: str,
        retry_delay_s: float | None = None,
    ) -> bool:
        """Record a failed (job_id, attempt) execution and schedule retry or dead-lettering."""
        now_ms = int(time.time() * 1000)
        delay_ms_str = str(int(retry_delay_s * 1000)) if retry_delay_s is not None else ""
        res = self._driver.eval_script(
            _STORE_NACK_LUA,
            keys=[
                self._jobs_key,
                self._claimed_key,
                self._scheduled_key,
                self._terminal_key,
                self._expires_key,
            ],
            args=[job_id, str(attempt), error, delay_ms_str, str(now_ms), self._tag],
        )
        return int(res or 0) > 0

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
        lease_until_ms = now_ms + int(lease_seconds * 1000)
        res = self._driver.eval_script(
            _STORE_HEARTBEAT_LUA,
            keys=[self._jobs_key, self._claimed_key],
            args=[worker_id, str(lease_until_ms), str(now_ms), *job_ids],
        )
        return int(res or 0)

    def reap_stale(self, *, lease_seconds: float = 300.0) -> int:
        """Reap abandoned in-flight claims back into ready status."""
        _ = lease_seconds
        now_ms = int(time.time() * 1000)
        res = self._driver.eval_script(
            _STORE_REAP_STALE_LUA,
            keys=[self._jobs_key, self._claimed_key, self._terminal_key, self._expires_key],
            args=[str(now_ms), self._tag],
        )
        return int(res or 0)

    def promote_scheduled(self, *, limit: int = 100) -> int:
        """Promote due scheduled jobs into ready status."""
        now_ms = int(time.time() * 1000)
        res = self._driver.eval_script(
            _STORE_PROMOTE_SCHEDULED_LUA,
            keys=[self._jobs_key, self._scheduled_key],
            args=[str(now_ms), str(limit), self._tag],
        )
        return int(res or 0)

    def cancel(self, job_id: str) -> bool:
        """Cancel an enqueued or scheduled job."""
        now_ms = int(time.time() * 1000)
        res = self._driver.eval_script(
            _STORE_CANCEL_LUA,
            keys=[self._jobs_key, self._scheduled_key, self._terminal_key, self._expires_key],
            args=[job_id, str(now_ms), self._tag],
        )
        return int(res or 0) > 0

    def get_entry(self, job_id: str) -> QueueEntry | None:
        """Retrieve a specific queue entry by job_id."""
        raw = self._driver.execute_command("HGET", self._jobs_key, job_id)
        decoded = _decode_json_value(raw)
        if not isinstance(decoded, dict):
            return None
        return _record_to_entry(decoded)

    def list_entries(
        self,
        queue: str | None = None,
        *,
        status: str | None = None,
        limit: int = 100,
    ) -> list[QueueEntry]:
        """List queue entries matching optional queue and status up to limit."""
        if limit <= 0:
            return []
        ordered_ids = self._driver.execute_command("ZRANGE", self._order_key, 0, -1)
        if not ordered_ids:
            return []
        str_ids = [_decode_str(jid) for jid in ordered_ids]
        raw_records = self._driver.execute_command("HMGET", self._jobs_key, *str_ids)
        return self._filter_entries(raw_records, queue=queue, status=status, limit=limit)

    def cleanup_terminal(self, *, before_ms: int, limit: int | None = None) -> int:
        """Delete terminal queue records completed before before_ms up to limit."""
        res = self._driver.eval_script(
            _STORE_CLEANUP_TERMINAL_LUA,
            keys=[
                self._jobs_key,
                self._order_key,
                self._terminal_key,
                self._expires_key,
                self._claimed_key,
                self._by_key_hash,
            ],
            args=[str(before_ms), str(limit) if limit is not None else "-1"],
        )
        return int(res or 0)

    def get_entry_by_key(self, key: str, *, queue: str | None = None) -> QueueEntry | None:
        """Retrieve a queue entry matching idempotency key and optional queue."""
        field = f"q:{queue}:{key}" if queue is not None else f"any:{key}"
        jid_raw = self._driver.execute_command("HGET", self._by_key_hash, field)
        if jid_raw is not None:
            return self.get_entry(_decode_str(jid_raw))
        return None

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
        ttl_ms = int(ttl_seconds * 1000) if ttl_seconds is not None else -1
        existing_raw = self._driver.eval_script(
            _STORE_RESERVE_IDENTITY_LUA,
            keys=[self._res_key(key)],
            args=[key, job_id, task_name, str(now_ms), str(ttl_ms)],
        )
        if existing_raw is None:
            return None
        decoded = _decode_json_value(existing_raw)
        return _record_to_reservation(decoded) if isinstance(decoded, dict) else None

    def get_reservation(self, key: str) -> QueueReservation | None:
        """Retrieve an active identity reservation for key, or None."""
        now_ms = int(time.time() * 1000)
        raw = self._driver.execute_command("GET", self._res_key(key))
        return self._decode_reservation_if_active(raw, now_ms)

    def reset_identity(self, key: str, *, expected_job_id: str | None = None) -> bool:
        """Release an identity reservation, optionally fenced by expected_job_id."""
        res = self._driver.eval_script(
            _STORE_RESET_IDENTITY_LUA,
            keys=[self._res_key(key)],
            args=[expected_job_id or ""],
        )
        return int(res or 0) > 0

    def assign_worker(self, job_id: str, *, worker_id: str, attempt: int) -> bool:
        """Record worker_id assignment for an active fenced claim."""
        now_ms = int(time.time() * 1000)
        res = self._driver.eval_script(
            _STORE_ASSIGN_WORKER_LUA,
            keys=[self._jobs_key],
            args=[job_id, worker_id, str(attempt), str(now_ms)],
        )
        return int(res or 0) > 0

    def touch_heartbeats(self, job_ids: Sequence[str], *, heartbeat_at_ms: int) -> int:
        """Update heartbeat_at_ms for active claimed jobs and return updated count."""
        if not job_ids:
            return 0
        now_ms = int(time.time() * 1000)
        res = self._driver.eval_script(
            _STORE_TOUCH_HEARTBEATS_LUA,
            keys=[self._jobs_key],
            args=[str(heartbeat_at_ms), str(now_ms), *job_ids],
        )
        return int(res or 0)

    def null_heartbeats(self, job_ids: Sequence[str]) -> int:
        """Clear heartbeat_at_ms for the given job_ids and return updated count."""
        if not job_ids:
            return 0
        now_ms = int(time.time() * 1000)
        res = self._driver.eval_script(
            _STORE_NULL_HEARTBEATS_LUA,
            keys=[self._jobs_key],
            args=[str(now_ms), *job_ids],
        )
        return int(res or 0)

    def interrupt_task(self, job_id: str, *, error: str | None = None) -> bool:
        """Interrupt a pending or running job with optional error detail."""
        now_ms = int(time.time() * 1000)
        res = self._driver.eval_script(
            _STORE_INTERRUPT_TASK_LUA,
            keys=[
                self._jobs_key,
                self._claimed_key,
                self._scheduled_key,
                self._terminal_key,
                self._expires_key,
            ],
            args=[job_id, "1" if error is not None else "0", error or "", str(now_ms), self._tag],
        )
        return int(res or 0) > 0

    def expire_overdue(self, *, now_ms: int, limit: int | None = None) -> int:
        """Mark overdue jobs with expires_at_ms <= now_ms as expired up to limit."""
        res = self._driver.eval_script(
            _STORE_EXPIRE_OVERDUE_LUA,
            keys=[self._jobs_key, self._expires_key, self._scheduled_key, self._terminal_key],
            args=[str(now_ms), str(limit) if limit is not None else "-1", self._tag],
        )
        return int(res or 0)

    def next_due_delay(self, queues: Sequence[str]) -> float | None:
        """Return the delay in seconds until the next scheduled job is due, or None."""
        if not queues:
            return None
        now_ms = int(time.time() * 1000)
        zrange_raw = self._driver.execute_command("ZRANGE", self._scheduled_key, 0, -1, "WITHSCORES")
        pairs = _parse_zrange_withscores(zrange_raw)
        if not pairs:
            return None
        jids = [jid for jid, _ in pairs]
        raw_records = self._driver.execute_command("HMGET", self._jobs_key, *jids)
        return self._compute_next_due_delay(zrange_raw, raw_records, queues, now_ms)

    def reserve_unique_key(self, key: str, *, ttl_seconds: float) -> bool:
        """Reserve an idempotency key for ttl_seconds."""
        ttl_ms = max(1, int(ttl_seconds * 1000))
        res = self._driver.execute_command("SET", self._uniq_key(key), "1", "NX", "PX", ttl_ms)
        return bool(res)

    def release_unique_key(self, key: str) -> bool:
        """Release an idempotency key reservation."""
        res = self._driver.execute_command("DEL", self._uniq_key(key))
        return int(res or 0) > 0

    def acquire_lease(self, lease_key: str, *, owner_id: str, ttl_seconds: float) -> bool:
        """Attempt to acquire a distributed leader lease for a cron or coordination slot."""
        ttl_ms = max(1, int(ttl_seconds * 1000))
        res = self._driver.eval_script(
            _STORE_ACQUIRE_LEASE_LUA,
            keys=[self._lease_key(lease_key)],
            args=[owner_id, str(ttl_ms)],
        )
        return int(res or 0) == 1

    def release_lease(self, lease_key: str, *, owner_id: str) -> bool:
        """Release a held distributed leader lease."""
        res = self._driver.eval_script(
            _STORE_RELEASE_LEASE_LUA,
            keys=[self._lease_key(lease_key)],
            args=[owner_id],
        )
        return int(res or 0) == 1

    def close(self) -> None:
        """Close backing store connections and release resources."""
        self._driver.close()


class AsyncRESPQueueStore(RESPQueueStoreBase, AsyncQueueStore):
    """Asynchronous RESP queue store implementing the full AsyncQueueStore capability protocol."""

    __slots__ = ()

    async def ensure_schema(self) -> None:
        """Schema-less RESP store initialization no-op."""
        return None

    async def enqueue(self, entry: QueueEntry) -> str:
        """Enqueue a single job entry and return its assigned job_id."""
        now_ms = int(time.time() * 1000)
        keys, args, jid = self._build_enqueue_args(entry, now_ms)
        await self._driver.eval_script(_STORE_ENQUEUE_LUA, keys=keys, args=args)
        if self._notification_source is not None:
            with contextlib.suppress(Exception):
                await self._notification_source.publish(f"vtb:queue:{entry.queue}", f"wakeup:{jid}")
        return jid

    async def enqueue_many(self, entries: Sequence[QueueEntry]) -> list[str]:
        """Bulk-enqueue job entries and return their assigned job_id values."""
        return [await self.enqueue(entry) for entry in entries]

    async def claim(
        self,
        queues: Sequence[str],
        *,
        limit: int = 1,
        worker_id: str,
        lease_seconds: float = 30.0,
    ) -> list[QueueEntry]:
        """Atomically claim up to limit ready jobs under a visibility lease."""
        if not queues or limit <= 0:
            return []
        now_ms = int(time.time() * 1000)
        lease_until_ms = now_ms + int(lease_seconds * 1000)
        keys = [self._jobs_key, self._claimed_key, *(self._ready_key(q) for q in queues)]
        raw_items = await self._driver.eval_script(
            _STORE_CLAIM_LUA,
            keys=keys,
            args=[str(limit), worker_id, str(lease_until_ms), str(now_ms)],
        )
        return self._decode_claimed_records(raw_items)

    async def ack(self, job_id: str, *, attempt: int) -> bool:
        """Mark a fenced (job_id, attempt) claim as completed."""
        now_ms = int(time.time() * 1000)
        res = await self._driver.eval_script(
            _STORE_ACK_LUA,
            keys=[self._jobs_key, self._claimed_key, self._terminal_key, self._expires_key],
            args=[job_id, str(attempt), str(now_ms)],
        )
        return int(res or 0) > 0

    async def nack(
        self,
        job_id: str,
        *,
        attempt: int,
        error: str,
        retry_delay_s: float | None = None,
    ) -> bool:
        """Record a failed (job_id, attempt) execution and schedule retry or dead-lettering."""
        now_ms = int(time.time() * 1000)
        delay_ms_str = str(int(retry_delay_s * 1000)) if retry_delay_s is not None else ""
        res = await self._driver.eval_script(
            _STORE_NACK_LUA,
            keys=[
                self._jobs_key,
                self._claimed_key,
                self._scheduled_key,
                self._terminal_key,
                self._expires_key,
            ],
            args=[job_id, str(attempt), error, delay_ms_str, str(now_ms), self._tag],
        )
        return int(res or 0) > 0

    async def heartbeat(
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
        lease_until_ms = now_ms + int(lease_seconds * 1000)
        res = await self._driver.eval_script(
            _STORE_HEARTBEAT_LUA,
            keys=[self._jobs_key, self._claimed_key],
            args=[worker_id, str(lease_until_ms), str(now_ms), *job_ids],
        )
        return int(res or 0)

    async def reap_stale(self, *, lease_seconds: float = 300.0) -> int:
        """Reap abandoned in-flight claims back into ready status."""
        _ = lease_seconds
        now_ms = int(time.time() * 1000)
        res = await self._driver.eval_script(
            _STORE_REAP_STALE_LUA,
            keys=[self._jobs_key, self._claimed_key, self._terminal_key, self._expires_key],
            args=[str(now_ms), self._tag],
        )
        return int(res or 0)

    async def promote_scheduled(self, *, limit: int = 100) -> int:
        """Promote due scheduled jobs into ready status."""
        now_ms = int(time.time() * 1000)
        res = await self._driver.eval_script(
            _STORE_PROMOTE_SCHEDULED_LUA,
            keys=[self._jobs_key, self._scheduled_key],
            args=[str(now_ms), str(limit), self._tag],
        )
        return int(res or 0)

    async def cancel(self, job_id: str) -> bool:
        """Cancel an enqueued or scheduled job."""
        now_ms = int(time.time() * 1000)
        res = await self._driver.eval_script(
            _STORE_CANCEL_LUA,
            keys=[self._jobs_key, self._scheduled_key, self._terminal_key, self._expires_key],
            args=[job_id, str(now_ms), self._tag],
        )
        return int(res or 0) > 0

    async def get_entry(self, job_id: str) -> QueueEntry | None:
        """Retrieve a specific queue entry by job_id."""
        raw = await self._driver.execute_command("HGET", self._jobs_key, job_id)
        decoded = _decode_json_value(raw)
        if not isinstance(decoded, dict):
            return None
        return _record_to_entry(decoded)

    async def list_entries(
        self,
        queue: str | None = None,
        *,
        status: str | None = None,
        limit: int = 100,
    ) -> list[QueueEntry]:
        """List queue entries matching optional queue and status up to limit."""
        if limit <= 0:
            return []
        ordered_ids = await self._driver.execute_command("ZRANGE", self._order_key, 0, -1)
        if not ordered_ids:
            return []
        str_ids = [_decode_str(jid) for jid in ordered_ids]
        raw_records = await self._driver.execute_command("HMGET", self._jobs_key, *str_ids)
        return self._filter_entries(raw_records, queue=queue, status=status, limit=limit)

    async def cleanup_terminal(self, *, before_ms: int, limit: int | None = None) -> int:
        """Delete terminal queue records completed before before_ms up to limit."""
        res = await self._driver.eval_script(
            _STORE_CLEANUP_TERMINAL_LUA,
            keys=[
                self._jobs_key,
                self._order_key,
                self._terminal_key,
                self._expires_key,
                self._claimed_key,
                self._by_key_hash,
            ],
            args=[str(before_ms), str(limit) if limit is not None else "-1"],
        )
        return int(res or 0)

    async def get_entry_by_key(self, key: str, *, queue: str | None = None) -> QueueEntry | None:
        """Retrieve a queue entry matching idempotency key and optional queue."""
        field = f"q:{queue}:{key}" if queue is not None else f"any:{key}"
        jid_raw = await self._driver.execute_command("HGET", self._by_key_hash, field)
        if jid_raw is not None:
            return await self.get_entry(_decode_str(jid_raw))
        return None

    async def reserve_identity(
        self,
        key: str,
        *,
        job_id: str,
        task_name: str,
        ttl_seconds: float | None = None,
    ) -> QueueReservation | None:
        """Acquire a single-winner reservation, returning None when acquired or the existing owner."""
        now_ms = int(time.time() * 1000)
        ttl_ms = int(ttl_seconds * 1000) if ttl_seconds is not None else -1
        existing_raw = await self._driver.eval_script(
            _STORE_RESERVE_IDENTITY_LUA,
            keys=[self._res_key(key)],
            args=[key, job_id, task_name, str(now_ms), str(ttl_ms)],
        )
        if existing_raw is None:
            return None
        decoded = _decode_json_value(existing_raw)
        return _record_to_reservation(decoded) if isinstance(decoded, dict) else None

    async def get_reservation(self, key: str) -> QueueReservation | None:
        """Retrieve an active identity reservation for key, or None."""
        now_ms = int(time.time() * 1000)
        raw = await self._driver.execute_command("GET", self._res_key(key))
        return self._decode_reservation_if_active(raw, now_ms)

    async def reset_identity(self, key: str, *, expected_job_id: str | None = None) -> bool:
        """Release an identity reservation, optionally fenced by expected_job_id."""
        res = await self._driver.eval_script(
            _STORE_RESET_IDENTITY_LUA,
            keys=[self._res_key(key)],
            args=[expected_job_id or ""],
        )
        return int(res or 0) > 0

    async def assign_worker(self, job_id: str, *, worker_id: str, attempt: int) -> bool:
        """Record worker_id assignment for an active fenced claim."""
        now_ms = int(time.time() * 1000)
        res = await self._driver.eval_script(
            _STORE_ASSIGN_WORKER_LUA,
            keys=[self._jobs_key],
            args=[job_id, worker_id, str(attempt), str(now_ms)],
        )
        return int(res or 0) > 0

    async def touch_heartbeats(self, job_ids: Sequence[str], *, heartbeat_at_ms: int) -> int:
        """Update heartbeat_at_ms for active claimed jobs and return updated count."""
        if not job_ids:
            return 0
        now_ms = int(time.time() * 1000)
        res = await self._driver.eval_script(
            _STORE_TOUCH_HEARTBEATS_LUA,
            keys=[self._jobs_key],
            args=[str(heartbeat_at_ms), str(now_ms), *job_ids],
        )
        return int(res or 0)

    async def null_heartbeats(self, job_ids: Sequence[str]) -> int:
        """Clear heartbeat_at_ms for the given job_ids and return updated count."""
        if not job_ids:
            return 0
        now_ms = int(time.time() * 1000)
        res = await self._driver.eval_script(
            _STORE_NULL_HEARTBEATS_LUA,
            keys=[self._jobs_key],
            args=[str(now_ms), *job_ids],
        )
        return int(res or 0)

    async def interrupt_task(self, job_id: str, *, error: str | None = None) -> bool:
        """Interrupt a pending or running job with optional error detail."""
        now_ms = int(time.time() * 1000)
        res = await self._driver.eval_script(
            _STORE_INTERRUPT_TASK_LUA,
            keys=[
                self._jobs_key,
                self._claimed_key,
                self._scheduled_key,
                self._terminal_key,
                self._expires_key,
            ],
            args=[job_id, "1" if error is not None else "0", error or "", str(now_ms), self._tag],
        )
        return int(res or 0) > 0

    async def expire_overdue(self, *, now_ms: int, limit: int | None = None) -> int:
        """Mark overdue jobs with expires_at_ms <= now_ms as expired up to limit."""
        res = await self._driver.eval_script(
            _STORE_EXPIRE_OVERDUE_LUA,
            keys=[self._jobs_key, self._expires_key, self._scheduled_key, self._terminal_key],
            args=[str(now_ms), str(limit) if limit is not None else "-1", self._tag],
        )
        return int(res or 0)

    async def next_due_delay(self, queues: Sequence[str]) -> float | None:
        """Return the delay in seconds until the next scheduled job is due, or None."""
        if not queues:
            return None
        now_ms = int(time.time() * 1000)
        zrange_raw = await self._driver.execute_command("ZRANGE", self._scheduled_key, 0, -1, "WITHSCORES")
        pairs = _parse_zrange_withscores(zrange_raw)
        if not pairs:
            return None
        jids = [jid for jid, _ in pairs]
        raw_records = await self._driver.execute_command("HMGET", self._jobs_key, *jids)
        return self._compute_next_due_delay(zrange_raw, raw_records, queues, now_ms)

    async def reserve_unique_key(self, key: str, *, ttl_seconds: float) -> bool:
        """Reserve an idempotency key for ttl_seconds."""
        ttl_ms = max(1, int(ttl_seconds * 1000))
        res = await self._driver.execute_command("SET", self._uniq_key(key), "1", "NX", "PX", ttl_ms)
        return bool(res)

    async def release_unique_key(self, key: str) -> bool:
        """Release an idempotency key reservation."""
        res = await self._driver.execute_command("DEL", self._uniq_key(key))
        return int(res or 0) > 0

    async def acquire_lease(self, lease_key: str, *, owner_id: str, ttl_seconds: float) -> bool:
        """Attempt to acquire a distributed leader lease for a cron or coordination slot."""
        ttl_ms = max(1, int(ttl_seconds * 1000))
        res = await self._driver.eval_script(
            _STORE_ACQUIRE_LEASE_LUA,
            keys=[self._lease_key(lease_key)],
            args=[owner_id, str(ttl_ms)],
        )
        return int(res or 0) == 1

    async def release_lease(self, lease_key: str, *, owner_id: str) -> bool:
        """Release a held distributed leader lease."""
        res = await self._driver.eval_script(
            _STORE_RELEASE_LEASE_LUA,
            keys=[self._lease_key(lease_key)],
            args=[owner_id],
        )
        return int(res or 0) == 1

    async def aclose(self) -> None:
        """Close backing store connections and release resources."""
        await self._driver.close()
