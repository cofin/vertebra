"""Redis Streams queue backend with visibility timeout and DLQ routing."""

from __future__ import annotations

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
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import decode_json, encode_json_bytes

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from vertebra._redis import AsyncRedisClient

__all__ = (
    "RedisQueueBackend",
    "RedisStreamClient",
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


def _normalize_stream_fields(fields: Any) -> dict[str, Any]:
    """Convert stream entry fields into a normalized string-keyed dictionary."""
    if isinstance(fields, dict):
        return {k.decode("utf-8") if isinstance(k, bytes) else str(k): v for k, v in fields.items()}
    if isinstance(fields, (list, tuple)):
        if fields and isinstance(fields[0], (list, tuple)) and len(fields[0]) == 2:
            return {(k.decode("utf-8") if isinstance(k, bytes) else str(k)): v for k, v in fields}
        res: dict[str, Any] = {}
        for i in range(0, len(fields) - 1, 2):
            k = fields[i].decode("utf-8") if isinstance(fields[i], bytes) else str(fields[i])
            res[k] = fields[i + 1]
        return res
    return {}


@runtime_checkable
class RedisStreamClient(Protocol):
    """Protocol covering the subset of Redis commands required by RedisQueueBackend."""

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


class _RedisStreamAdapter:
    """Adapter bridging AsyncRedisClient to the RedisStreamClient protocol."""

    __slots__ = ("_client",)

    def __init__(self, client: AsyncRedisClient) -> None:
        """Initialize adapter wrapping a native AsyncRedisClient."""
        self._client = client

    async def xgroup_create(
        self,
        stream: str,
        group: str,
        id: str = "$",
        mkstream: bool = False,
    ) -> Any:
        """Create consumer group on stream."""
        return await self._client.xgroup_create(stream, group, id=id, mkstream=mkstream)

    async def xadd(
        self,
        stream: str,
        fields: Mapping[str, Any],
        id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
    ) -> str:
        """Append entry to stream."""
        res = await self._client.xadd(stream, dict(fields), id=id, maxlen=maxlen, approximate=approximate)
        return res.decode("utf-8") if isinstance(res, bytes) else str(res)

    async def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: Mapping[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> list[Any]:
        """Read entries from streams via consumer group."""
        res = await self._client.xreadgroup(group, consumer, dict(streams), count=count, block_ms=block)
        return res if res is not None else []

    async def xack(self, stream: str, group: str, *ids: str) -> int:
        """Acknowledge processed stream entries."""
        return int(await self._client.xack(stream, group, list(ids)))

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
        res = await self._client.xautoclaim(
            stream,
            group,
            consumer,
            min_idle_time,
            start_id=start_id,
            count=count,
        )
        if not res or len(res) < 3:
            return "0-0", [], []
        next_id, raw_entries, deleted = res
        next_id_str = next_id.decode("utf-8") if isinstance(next_id, bytes) else str(next_id)
        entries: list[tuple[str, dict[str, Any]]] = []
        for item in raw_entries:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                eid = item[0].decode("utf-8") if isinstance(item[0], bytes) else str(item[0])
                entries.append((eid, _normalize_stream_fields(item[1])))
        deleted_list = [d.decode("utf-8") if isinstance(d, bytes) else str(d) for d in deleted]
        return next_id_str, entries, deleted_list

    async def xrange(
        self,
        stream: str,
        min: str = "-",
        max: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Read range of stream entries."""
        res = await self._client.xrange(stream, start=min, end=max, count=count)
        if not res:
            return []
        entries: list[tuple[str, dict[str, Any]]] = []
        for item in res:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                eid = item[0].decode("utf-8") if isinstance(item[0], bytes) else str(item[0])
                entries.append((eid, _normalize_stream_fields(item[1])))
        return entries

    async def xdel(self, stream: str, *ids: str) -> int:
        """Delete stream entries by ID."""
        return int(await self._client.xdel(stream, list(ids)))

    async def zadd(self, key: str, mapping: Mapping[str, float]) -> int:
        """Add members to sorted set."""
        return int(await self._client.zadd(key, dict(mapping)))

    async def eval(self, script: str, numkeys: int, *keys_and_args: Any) -> Any:
        """Evaluate Lua script."""
        keys = list(keys_and_args[:numkeys])
        args = list(keys_and_args[numkeys:])
        return await self._client.eval_script(script, keys, args)

    async def set(
        self,
        key: str,
        value: str,
        nx: bool = False,
        px: int | None = None,
    ) -> Any:
        """Set key with optional NX and PX options."""
        return await self._client.set(key, value, nx=nx, px=px)

    async def get(self, key: str) -> str | None:
        """Get string value by key."""
        raw = await self._client.get(key)
        if raw is None:
            return None
        return raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)

    async def aclose(self) -> None:
        """Close client connection."""
        self._client.close()


class RedisQueueBackend:
    """Distributed task queue backend powered by Redis Streams and Sorted Sets."""

    __slots__ = ("_client", "_config", "_prefix")

    def __init__(
        self,
        config: QueueConfig | None = None,
        *,
        client: RedisStreamClient | None = None,
    ) -> None:
        """Bind the queue backend to a Redis Streams client.

        Args:
            config: Queue configuration.
            client: Pre-built RedisStreamClient bound as given.

        Raises:
            MissingDependencyError: When client is None and vertebra-core[redis] is not installed.
            ImproperConfigurationError: When client is None and config.backend_url is invalid.
        """
        ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")
        self._config = config or QueueConfig()
        self._prefix = self._config.stream_prefix
        self._client: RedisStreamClient = client if client is not None else self._build_client(self._config)

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
                for item in entries:
                    if isinstance(item, (list, tuple)) and len(item) >= 2:
                        entry_id = item[0].decode("utf-8") if isinstance(item[0], bytes) else str(item[0])
                        fields = item[1]
                    else:
                        continue
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
            norm_fields = _normalize_stream_fields(fields)
            payload = norm_fields.get("payload")
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
            norm_fields = _normalize_stream_fields(fields)
            if norm_fields.get("dlq_id") == dlq_id:
                payload = norm_fields.get("payload")
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

    def _decode_envelope(self, fields: Any, receipt_handle: str) -> TaskEnvelope:
        """Decode TaskEnvelope from stream entry fields."""
        norm_fields = _normalize_stream_fields(fields)
        payload_data = norm_fields.get("payload")
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
        """Build an AsyncRedisClient wrapped in _RedisStreamAdapter from config.backend_url."""
        url = config.backend_url
        if url.startswith("memory://"):
            msg = "RedisQueueBackend does not support memory:// URLs"
            raise ImproperConfigurationError(msg)
        if url.startswith("valkey://") or url.startswith("valkeys://"):
            msg = "valkey:// URLs are not supported by RedisQueueBackend; use ValkeyQueueBackend instead"
            raise ImproperConfigurationError(msg)
        if not (url.startswith("redis://") or url.startswith("rediss://") or url.startswith("unix://")):
            msg = f"Unsupported Redis URL scheme in {url!r}; expected redis://, rediss://, or unix://"
            raise ImproperConfigurationError(msg)
        parsed = urllib.parse.urlparse(url)
        if url.startswith("unix://"):
            path = url.removeprefix("unix://")
            if not path or not path.strip("/"):
                msg = f"Redis queue backend_url {url!r} has no socket path"
                raise ImproperConfigurationError(msg)
        elif not parsed.hostname:
            msg = f"Redis queue backend_url {url!r} has no host; pass a redis://host:port URL or client= to bind a RedisStreamClient"
            raise ImproperConfigurationError(msg)

        mod = ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")
        client = mod.AsyncRedisClient(url=url)
        return _RedisStreamAdapter(client)
