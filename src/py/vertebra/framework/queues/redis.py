"""Redis and Valkey Streams queue backends with visibility timeout and DLQ routing."""

from __future__ import annotations

import collections
import contextlib
import time
import urllib.parse
import uuid
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import msgspec

from vertebra.framework.queues.base import (
    CronLease,
    DeadLetterEntry,
    QueueConfig,
    TaskEnvelope,
    TaskLane,
)
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json, encode_json_bytes

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

__all__ = (
    "RedisQueueBackend",
    "RedisStreamClient",
    "ValkeyQueueBackend",
)

_PROMOTE_SCHEDULED_LUA = """
local scheduled_key = KEYS[1]
local now_ts = tonumber(ARGV[1])
local prefix = ARGV[2]
local limit = tonumber(ARGV[3])
local items = redis.call('ZRANGEBYSCORE', scheduled_key, '-inf', now_ts, 'LIMIT', 0, limit)
local promoted = 0
for _, raw in ipairs(items) do
    local ok, data = pcall(cjson.decode, raw)
    if ok and data and data.lane then
        local stream_key = prefix .. ':lane:' .. data.lane
        redis.call('XADD', stream_key, '*', 'payload', raw, 'lane', data.lane, 'task_name', data.task_name or '')
        redis.call('ZREM', scheduled_key, raw)
        promoted = promoted + 1
    end
end
return promoted
"""

_RENEW_LEASE_LUA = """
local lease_key = KEYS[1]
local owner_id = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local new_slot = ARGV[3]
local current = redis.call('GET', lease_key)
if current then
    local sep = string.find(current, ':')
    local current_owner = current
    if sep then
        current_owner = string.sub(current, 1, sep - 1)
    end
    if current_owner == owner_id then
        local value = owner_id
        if new_slot and new_slot ~= '' then
            value = owner_id .. ':' .. new_slot
        else
            value = current
        end
        redis.call('SET', lease_key, value, 'PX', ttl_ms)
        return 1
    end
end
return 0
"""


@runtime_checkable
class RedisStreamClient(Protocol):
    """Protocol covering the subset of Redis/Valkey commands required by RedisQueueBackend."""

    async def xgroup_create(
        self,
        stream: str,
        group: str,
        id: str = "$",
        mkstream: bool = False,
    ) -> Any:
        """Create consumer group on stream."""
        ...

    async def xadd(
        self,
        stream: str,
        fields: Mapping[str, Any],
        id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
    ) -> str:
        """Append entry to stream."""
        ...

    async def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: Mapping[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> list[Any]:
        """Read entries from streams via consumer group."""
        ...

    async def xack(self, stream: str, group: str, *ids: str) -> int:
        """Acknowledge processed stream entries."""
        ...

    async def xautoclaim(
        self,
        stream: str,
        group: str,
        consumer: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
    ) -> tuple[str, list[tuple[str, dict[str, Any]]], list[str]]:
        """Reclaim pending entries exceeding idle time."""
        ...

    async def xrange(
        self,
        stream: str,
        min: str = "-",
        max: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Read range of stream entries."""
        ...

    async def xdel(self, stream: str, *ids: str) -> int:
        """Delete stream entries by ID."""
        ...

    async def zadd(self, key: str, mapping: Mapping[str, float]) -> int:
        """Add members to sorted set."""
        ...

    async def eval(self, script: str, numkeys: int, *keys_and_args: Any) -> Any:
        """Evaluate Lua script."""
        ...

    async def set(
        self,
        key: str,
        value: str,
        nx: bool = False,
        px: int | None = None,
    ) -> Any:
        """Set key with optional NX and PX options."""
        ...

    async def get(self, key: str) -> str | None:
        """Get string value by key."""
        ...

    async def aclose(self) -> None:
        """Close client connection."""
        ...


class _InMemoryRedisStreamClient:
    """In-memory emulator of Redis/Valkey Streams, ZSET, and Lua execution."""

    __slots__ = ("_groups", "_kv", "_pel", "_streams", "_zsets")

    def __init__(self) -> None:
        self._streams: dict[str, list[tuple[str, dict[str, Any]]]] = collections.defaultdict(list)
        self._groups: dict[str, set[str]] = collections.defaultdict(set)
        self._pel: dict[str, dict[str, tuple[str, float, dict[str, Any]]]] = collections.defaultdict(dict)
        self._zsets: dict[str, dict[str, float]] = collections.defaultdict(dict)
        self._kv: dict[str, tuple[str, float | None]] = {}

    async def xgroup_create(
        self,
        stream: str,
        group: str,
        id: str = "$",
        mkstream: bool = False,
    ) -> bool:
        """Register consumer group for stream."""
        if mkstream and stream not in self._streams:
            self._streams[stream] = []
        self._groups[stream].add(group)
        return True

    async def xadd(
        self,
        stream: str,
        fields: Mapping[str, Any],
        id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
    ) -> str:
        """Append stream message."""
        entry_id = f"{int(time.time() * 1000)}-{len(self._streams[stream])}"
        converted = {str(k): (v.decode("utf-8") if isinstance(v, bytes) else str(v)) for k, v in fields.items()}
        self._streams[stream].append((entry_id, converted))
        if maxlen and len(self._streams[stream]) > maxlen:
            trim_count = len(self._streams[stream]) - maxlen
            del self._streams[stream][:trim_count]
        return entry_id

    async def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: Mapping[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> list[Any]:
        """Read unclaimed entries for consumer group."""
        limit = count or 10
        results: list[tuple[str, list[tuple[str, dict[str, Any]]]]] = []
        now = time.time()
        for stream_name, _ in streams.items():
            pel_key = f"{stream_name}:{group}"
            stream_entries = self._streams.get(stream_name, [])
            claimed_in_pel = set(self._pel[pel_key].keys())
            matched: list[tuple[str, dict[str, Any]]] = []
            for entry_id, fields in stream_entries:
                if entry_id not in claimed_in_pel:
                    self._pel[pel_key][entry_id] = (consumer, now, fields)
                    matched.append((entry_id, fields))
                    if len(matched) >= limit:
                        break
            if matched:
                results.append((stream_name, matched))
        return results

    async def xack(self, stream: str, group: str, *ids: str) -> int:
        """Remove entries from group PEL."""
        pel_key = f"{stream}:{group}"
        acked = 0
        for entry_id in ids:
            if entry_id in self._pel[pel_key]:
                del self._pel[pel_key][entry_id]
                acked += 1
        return acked

    async def xautoclaim(
        self,
        stream: str,
        group: str,
        consumer: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
    ) -> tuple[str, list[tuple[str, dict[str, Any]]], list[str]]:
        """Reclaim pending entries older than min_idle_time milliseconds."""
        limit = count or 10
        pel_key = f"{stream}:{group}"
        now = time.time()
        idle_threshold_s = min_idle_time / 1000.0
        reclaimed: list[tuple[str, dict[str, Any]]] = []
        for entry_id, (_, last_delivered, fields) in list(self._pel[pel_key].items()):
            if now - last_delivered >= idle_threshold_s:
                self._pel[pel_key][entry_id] = (consumer, now, fields)
                reclaimed.append((entry_id, fields))
                if len(reclaimed) >= limit:
                    break
        return ("0-0", reclaimed, [])

    async def xrange(
        self,
        stream: str,
        min: str = "-",
        max: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """List stream entries in FIFO order."""
        entries = self._streams.get(stream, [])
        return entries[:count] if count else entries

    async def xdel(self, stream: str, *ids: str) -> int:
        """Remove matching entries from stream."""
        id_set = set(ids)
        before = len(self._streams.get(stream, []))
        self._streams[stream] = [item for item in self._streams.get(stream, []) if item[0] not in id_set]
        return before - len(self._streams[stream])

    async def zadd(self, key: str, mapping: Mapping[str, float]) -> int:
        """Add elements to simulated sorted set."""
        added = 0
        for member, score in mapping.items():
            if member not in self._zsets[key]:
                added += 1
            self._zsets[key][member] = float(score)
        return added

    async def eval(self, script: str, numkeys: int, *keys_and_args: Any) -> Any:
        """Simulate execution of queue promotion and lease renewal scripts."""
        if "ZRANGEBYSCORE" in script:
            scheduled_key = str(keys_and_args[0])
            now_ts = float(keys_and_args[1])
            prefix = str(keys_and_args[2])
            limit = int(keys_and_args[3])
            zset = self._zsets[scheduled_key]
            eligible = [
                (member, score) for member, score in sorted(zset.items(), key=lambda pair: pair[1]) if score <= now_ts
            ][:limit]
            promoted = 0
            for raw_json, _ in eligible:
                raw_bytes = raw_json.encode("utf-8") if isinstance(raw_json, str) else raw_json
                env = msgspec.convert(decode_json(raw_bytes), type=TaskEnvelope)
                lane = env.lane
                stream_key = f"{prefix}:lane:{lane}"
                await self.xadd(
                    stream_key,
                    {
                        "payload": raw_json,
                        "lane": lane,
                        "task_name": env.task_name,
                    },
                )
                zset.pop(raw_json, None)
                promoted += 1
            return promoted

        if "current_owner == owner_id" in script:
            lease_key = str(keys_and_args[0])
            owner_id = str(keys_and_args[1])
            ttl_ms = float(keys_and_args[2])
            new_slot = str(keys_and_args[3]) if len(keys_and_args) > 3 else ""
            existing_tuple = self._kv.get(lease_key)
            if existing_tuple:
                current_val, exp = existing_tuple
                now = time.time()
                if exp is None or exp > now:
                    parts = current_val.split(":", 1)
                    current_owner = parts[0]
                    if current_owner == owner_id:
                        slot_to_save = new_slot if new_slot else (parts[1] if len(parts) > 1 else "")
                        val_to_save = f"{owner_id}:{slot_to_save}" if slot_to_save else owner_id
                        self._kv[lease_key] = (val_to_save, now + (ttl_ms / 1000.0))
                        return 1
            return 0

        return None

    async def set(
        self,
        key: str,
        value: str,
        nx: bool = False,
        px: int | None = None,
    ) -> bool:
        """Store key value pair."""
        now = time.time()
        existing = self._kv.get(key)
        if existing:
            _, exp = existing
            if exp is not None and exp <= now:
                self._kv.pop(key, None)
                existing = None
        if nx and existing is not None:
            return False
        expires_at = now + (px / 1000.0) if px else None
        self._kv[key] = (value, expires_at)
        return True

    async def get(self, key: str) -> str | None:
        """Fetch value if not expired."""
        existing = self._kv.get(key)
        if not existing:
            return None
        val, exp = existing
        if exp is not None and exp <= time.time():
            self._kv.pop(key, None)
            return None
        return val

    async def aclose(self) -> None:
        """Clear memory structures."""
        self._streams.clear()
        self._groups.clear()
        self._pel.clear()
        self._zsets.clear()
        self._kv.clear()


class RedisQueueBackend:
    """Distributed task queue backend powered by Redis Streams and Sorted Sets."""

    __slots__ = ("_client", "_config", "_prefix")

    def __init__(
        self,
        config: QueueConfig | None = None,
        *,
        client: RedisStreamClient | None = None,
    ) -> None:
        self._config = config or QueueConfig()
        self._prefix = self._config.stream_prefix
        self._client: RedisStreamClient = client or self._build_client(self._config)

    @property
    def client(self) -> RedisStreamClient:
        """Return the active RedisStreamClient."""
        return self._client

    async def enqueue(self, envelope: TaskEnvelope) -> TaskEnvelope:
        """Enqueue task into delayed ZSET or target lane stream."""
        now = time.time()
        encoded_bytes = encode_json_bytes(envelope)
        encoded_str = encoded_bytes.decode("utf-8")

        if envelope.scheduled_at is not None and envelope.scheduled_at > now:
            scheduled_key = f"{self._prefix}:scheduled"
            await self._client.zadd(scheduled_key, {encoded_str: envelope.scheduled_at})
            return envelope

        stream_key = f"{self._prefix}:lane:{envelope.lane}"
        entry_id = await self._client.xadd(
            stream_key,
            {
                "payload": encoded_str,
                "lane": envelope.lane,
                "task_name": envelope.task_name,
            },
        )
        return TaskEnvelope(
            task_id=envelope.task_id,
            task_name=envelope.task_name,
            lane=envelope.lane,
            args=envelope.args,
            kwargs=envelope.kwargs,
            attempt=envelope.attempt,
            max_retries=envelope.max_retries,
            enqueued_at=envelope.enqueued_at,
            scheduled_at=envelope.scheduled_at,
            idempotency_key=envelope.idempotency_key,
            traceparent=envelope.traceparent,
            receipt_handle=entry_id,
        )

    async def dequeue(
        self,
        lanes: Sequence[TaskLane],
        *,
        batch_size: int = 1,
        timeout_seconds: float = 0.0,
    ) -> list[TaskEnvelope]:
        """Claim tasks across lanes using XAUTOCLAIM visibility recovery then XREADGROUP."""
        await self.promote_scheduled()
        min_idle_ms = int(self._config.visibility_timeout_seconds * 1000)
        claimed: list[TaskEnvelope] = []

        for lane in lanes:
            if len(claimed) >= batch_size:
                break
            stream_key = f"{self._prefix}:lane:{lane}"
            with contextlib.suppress(Exception):
                await self._client.xgroup_create(
                    stream_key,
                    self._config.consumer_group,
                    id="0",
                    mkstream=True,
                )

            remaining = batch_size - len(claimed)
            _, recovered_entries, _ = await self._client.xautoclaim(
                stream_key,
                self._config.consumer_group,
                self._config.consumer_name,
                min_idle_ms,
                count=remaining,
            )
            for entry_id, fields in recovered_entries:
                envelope = self._decode_envelope(fields, entry_id)
                reclaimed_env = TaskEnvelope(
                    task_id=envelope.task_id,
                    task_name=envelope.task_name,
                    lane=envelope.lane,
                    args=envelope.args,
                    kwargs=envelope.kwargs,
                    attempt=envelope.attempt + 1,
                    max_retries=envelope.max_retries,
                    enqueued_at=envelope.enqueued_at,
                    scheduled_at=envelope.scheduled_at,
                    idempotency_key=envelope.idempotency_key,
                    traceparent=envelope.traceparent,
                    receipt_handle=entry_id,
                )
                claimed.append(reclaimed_env)

        if len(claimed) < batch_size:
            read_streams = {f"{self._prefix}:lane:{lane}": ">" for lane in lanes}
            block_ms = int(timeout_seconds * 1000) if timeout_seconds > 0 else None
            remaining = batch_size - len(claimed)
            read_results = await self._client.xreadgroup(
                self._config.consumer_group,
                self._config.consumer_name,
                read_streams,
                count=remaining,
                block=block_ms,
            )
            for _, entries in read_results:
                for entry_id, fields in entries:
                    claimed.append(self._decode_envelope(fields, entry_id))
                    if len(claimed) >= batch_size:
                        break
                if len(claimed) >= batch_size:
                    break

        return claimed

    async def ack(self, envelope: TaskEnvelope) -> None:
        """Acknowledge completed stream entry via XACK and XDEL."""
        if not envelope.receipt_handle:
            return
        stream_key = f"{self._prefix}:lane:{envelope.lane}"
        await self._client.xack(
            stream_key,
            self._config.consumer_group,
            envelope.receipt_handle,
        )
        await self._client.xdel(stream_key, envelope.receipt_handle)

    async def nack(
        self,
        envelope: TaskEnvelope,
        *,
        error: Exception | str,
        delay_seconds: float | None = None,
        send_to_dlq: bool = False,
    ) -> DeadLetterEntry | None:
        """Nack task envelope, routing to DLQ stream or re-scheduling in ZSET."""
        if envelope.receipt_handle:
            stream_key = f"{self._prefix}:lane:{envelope.lane}"
            await self._client.xack(
                stream_key,
                self._config.consumer_group,
                envelope.receipt_handle,
            )
            await self._client.xdel(stream_key, envelope.receipt_handle)

        err_type = type(error).__name__ if isinstance(error, Exception) else "TaskExecutionError"
        err_msg = str(error)

        if send_to_dlq or envelope.attempt >= envelope.max_retries:
            dlq_id = uuid.uuid4().hex
            dlq_entry = DeadLetterEntry(
                dlq_id=dlq_id,
                envelope=envelope,
                error_type=err_type,
                error_message=err_msg,
                failed_at=time.time(),
            )
            dlq_payload = encode_json_bytes(dlq_entry).decode("utf-8")
            dlq_key = f"{self._prefix}:dlq"
            await self._client.xadd(
                dlq_key,
                {"payload": dlq_payload, "dlq_id": dlq_id},
                maxlen=self._config.dlq_max_len,
            )
            return dlq_entry

        now = time.time()
        delay = delay_seconds if delay_seconds is not None else 1.0
        retry_env = TaskEnvelope(
            task_id=envelope.task_id,
            task_name=envelope.task_name,
            lane=envelope.lane,
            args=envelope.args,
            kwargs=envelope.kwargs,
            attempt=envelope.attempt + 1,
            max_retries=envelope.max_retries,
            enqueued_at=envelope.enqueued_at,
            scheduled_at=now + delay,
            idempotency_key=envelope.idempotency_key,
            traceparent=envelope.traceparent,
            receipt_handle=None,
        )
        retry_payload = encode_json_bytes(retry_env).decode("utf-8")
        scheduled_key = f"{self._prefix}:scheduled"
        await self._client.zadd(scheduled_key, {retry_payload: now + delay})
        return None

    async def promote_scheduled(self, *, now: float | None = None, batch_size: int = 100) -> int:
        """Promote due items from ZSET into respective lane streams via atomic Lua."""
        current_time = now if now is not None else time.time()
        scheduled_key = f"{self._prefix}:scheduled"
        result = await self._client.eval(
            _PROMOTE_SCHEDULED_LUA,
            1,
            scheduled_key,
            current_time,
            self._prefix,
            batch_size,
        )
        return int(result or 0)

    async def list_dlq(self, *, limit: int = 100) -> list[DeadLetterEntry]:
        """Fetch dead-letter entries from the DLQ stream."""
        dlq_key = f"{self._prefix}:dlq"
        raw_entries = await self._client.xrange(dlq_key, count=limit)
        results: list[DeadLetterEntry] = []
        for _, fields in raw_entries:
            payload = fields.get("payload")
            if payload:
                raw_bytes = payload.encode("utf-8") if isinstance(payload, str) else payload
                entry = msgspec.convert(decode_json(raw_bytes), type=DeadLetterEntry)
                results.append(entry)
        return results

    async def replay_dlq(self, dlq_id: str) -> TaskEnvelope | None:
        """Locate entry in DLQ stream, delete it, and re-enqueue into its lane stream."""
        dlq_key = f"{self._prefix}:dlq"
        raw_entries = await self._client.xrange(dlq_key)
        for entry_id, fields in raw_entries:
            if fields.get("dlq_id") == dlq_id:
                payload = fields.get("payload")
                if payload:
                    raw_bytes = payload.encode("utf-8") if isinstance(payload, str) else payload
                    entry = msgspec.convert(decode_json(raw_bytes), type=DeadLetterEntry)
                    replayed_env = msgspec.structs.replace(
                        entry.envelope,
                        attempt=1,
                        scheduled_at=None,
                        receipt_handle=None,
                    )
                    await self._client.xdel(dlq_key, entry_id)
                    return await self.enqueue(replayed_env)
        return None

    async def acquire_lease(
        self,
        lease_key: str,
        owner_id: str,
        ttl_seconds: float,
        *,
        slot: str | None = None,
    ) -> CronLease | None:
        """Acquire lease key with owner_id and optional slot."""
        ttl_ms = int(ttl_seconds * 1000)
        full_key = f"{self._prefix}:lease:{lease_key}"
        payload = f"{owner_id}:{slot}" if slot else owner_id
        acquired = await self._client.set(full_key, payload, nx=True, px=ttl_ms)
        if acquired:
            return CronLease(
                lease_key=lease_key,
                owner_id=owner_id,
                expires_at=time.time() + ttl_seconds,
                last_slot=slot,
            )
        renewed = await self.renew_lease(lease_key, owner_id, ttl_seconds, slot=slot)
        return renewed

    async def renew_lease(
        self,
        lease_key: str,
        owner_id: str,
        ttl_seconds: float,
        *,
        slot: str | None = None,
    ) -> CronLease | None:
        """Renew lease via atomic Lua script verifying ownership."""
        ttl_ms = int(ttl_seconds * 1000)
        full_key = f"{self._prefix}:lease:{lease_key}"
        result = await self._client.eval(
            _RENEW_LEASE_LUA,
            1,
            full_key,
            owner_id,
            ttl_ms,
            slot or "",
        )
        if result == 1:
            return CronLease(
                lease_key=lease_key,
                owner_id=owner_id,
                expires_at=time.time() + ttl_seconds,
                last_slot=slot,
            )
        return None

    async def close(self) -> None:
        """Close client connection."""
        await self._client.aclose()

    def _decode_envelope(self, fields: Mapping[str, Any], receipt_handle: str) -> TaskEnvelope:
        """Decode TaskEnvelope from stream entry fields."""
        payload_data = fields.get("payload")
        if isinstance(payload_data, str):
            payload_data = payload_data.encode("utf-8")
        if not payload_data:
            msg = "Missing payload in stream entry"
            raise ValueError(msg)
        env = msgspec.convert(decode_json(payload_data), type=TaskEnvelope)
        if receipt_handle:
            env = msgspec.structs.replace(env, receipt_handle=receipt_handle)
        return env

    @staticmethod
    def _build_client(config: QueueConfig) -> RedisStreamClient:
        """Construct live Redis client or in-memory fallback emulator."""
        aioredis = import_optional("redis.asyncio")
        if aioredis is not None:
            parsed = urllib.parse.urlparse(config.backend_url)
            if parsed.hostname:
                client: RedisStreamClient = aioredis.from_url(config.backend_url)
                return client
        return _InMemoryRedisStreamClient()


class ValkeyQueueBackend(RedisQueueBackend):
    """Valkey-optimized distributed task queue backend."""

    def __init__(
        self,
        config: QueueConfig | None = None,
        *,
        client: RedisStreamClient | None = None,
    ) -> None:
        cfg = config or QueueConfig(stream_prefix="vtb:valkey:queue")
        super().__init__(cfg, client=client)
