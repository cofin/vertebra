"""Shared RESP3 Streams and Lua script queue backend and durable queue store primitives."""

from __future__ import annotations

import contextlib
import time
import urllib.parse
import uuid
from typing import TYPE_CHECKING, Any, ClassVar, Protocol, cast, runtime_checkable

import msgspec

from vertebra.db.driver._resp_queue import (
    AsyncRESPQueueStore,
    RESPQueueStoreBase,
    SyncRESPQueueStore,
)
from vertebra.framework.queues.base import (
    CronLease,
    DeadLetterEntry,
    QueueBackendSettings,
    QueueConfig,
    TaskEnvelope,
    TaskLane,
    TaskResultRecord,
)
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import decode_json, encode_json_bytes

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

__all__ = (
    "REDIS_QUEUE_DIALECT",
    "VALKEY_QUEUE_DIALECT",
    "AsyncRESPQueueStore",
    "RESPQueueDialect",
    "RESPQueueStoreBase",
    "AsyncRESPStreamClient",
    "RESPStreamQueueBackend",
    "SyncRESPQueueStore",
    "SyncRESPStreamClient",
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
    local lane = ok and type(data) == 'table' and (data.lane or data[3]) or nil
    local task_name = ok and type(data) == 'table' and (data.task_name or data[2] or '') or ''
    if ok and data and lane then
        local stream_key = prefix .. ':lane:' .. lane
        redis.call('XADD', stream_key, '*', 'payload', raw, 'lane', lane, 'task_name', task_name)
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


class RESPQueueDialect(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Dialect descriptor parameterizing RESP stream queue URL validation and client loading."""

    name: str
    backend_name: str
    alternative_backend: str
    allowed_schemes: tuple[str, ...]
    rejected_schemes: tuple[str, ...]
    default_key_prefix: str = "vtb:queue"
    driver_module: str
    extra_package: str
    extra_name: str
    async_client_attr: str
    sync_client_attr: str


REDIS_QUEUE_DIALECT = RESPQueueDialect(
    name="redis",
    backend_name="RedisQueueBackend",
    alternative_backend="ValkeyQueueBackend",
    allowed_schemes=("redis", "rediss", "unix"),
    rejected_schemes=("valkey", "valkeys"),
    default_key_prefix="vtb:queue",
    driver_module="vertebra._redis",
    extra_package="vertebra-core[redis]",
    extra_name="redis",
    async_client_attr="AsyncRedisClient",
    sync_client_attr="SyncRedisClient",
)

VALKEY_QUEUE_DIALECT = RESPQueueDialect(
    name="valkey",
    backend_name="ValkeyQueueBackend",
    alternative_backend="RedisQueueBackend",
    allowed_schemes=("valkey", "valkeys", "unix"),
    rejected_schemes=("redis", "rediss"),
    default_key_prefix="vtb:queue",
    driver_module="vertebra._valkey",
    extra_package="vertebra-core[valkey]",
    extra_name="valkey",
    async_client_attr="AsyncValkeyClient",
    sync_client_attr="SyncValkeyClient",
)


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


def _validate_resp_url(dialect: RESPQueueDialect, url: str) -> None:
    """Validate backend URL schemes and host/socket constraints for a RESP queue dialect."""
    if url.startswith("memory://"):
        msg = f"{dialect.backend_name} does not support memory:// URLs"
        raise ImproperConfigurationError(msg)
    for scheme in dialect.rejected_schemes:
        if url.startswith(f"{scheme}://"):
            primary_rejected = dialect.rejected_schemes[0]
            msg = (
                f"{primary_rejected}:// URLs are not supported by {dialect.backend_name}; "
                f"use {dialect.alternative_backend} instead"
            )
            raise ImproperConfigurationError(msg)
    if not any(url.startswith(f"{scheme}://") for scheme in dialect.allowed_schemes):
        expected = ", ".join(f"{s}://" for s in dialect.allowed_schemes[:-1]) + f", or {dialect.allowed_schemes[-1]}://"
        msg = f"Unsupported {dialect.name.title()} URL scheme in {url!r}; expected {expected}"
        raise ImproperConfigurationError(msg)
    if url.startswith("unix://"):
        path = url.removeprefix("unix://")
        if not path or not path.strip("/"):
            msg = f"{dialect.name.title()} queue backend_url {url!r} has no socket path"
            raise ImproperConfigurationError(msg)
        return
    parsed = urllib.parse.urlparse(url)
    if not parsed.hostname:
        primary_scheme = dialect.allowed_schemes[0]
        msg = (
            f"{dialect.name.title()} queue backend_url {url!r} has no host; "
            f"pass a {primary_scheme}://host:port URL or client= to bind a stream client"
        )
        raise ImproperConfigurationError(msg)


@runtime_checkable
class AsyncRESPStreamClient(Protocol):
    """Protocol covering the subset of async RESP commands required by RESPStreamQueueBackend."""

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


@runtime_checkable
class SyncRESPStreamClient(Protocol):
    """Protocol covering the synchronous subset of RESP commands required by RESPStreamQueueBackend."""

    def xadd(
        self,
        stream: str,
        fields: Mapping[str, Any],
        id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
    ) -> str:
        """Append entry to stream."""
        ...

    def zadd(self, key: str, mapping: Mapping[str, float]) -> int:
        """Add members to sorted set."""
        ...

    def set(
        self,
        key: str,
        value: str,
        nx: bool = False,
        px: int | None = None,
    ) -> Any:
        """Set key with optional NX and PX options."""
        ...

    def get(self, key: str) -> str | None:
        """Get string value by key."""
        ...

    def publish(self, channel: str, message: Any) -> int:
        """Publish message to channel."""
        ...

    def close(self) -> None:
        """Close client connection."""
        ...


class RESPStreamQueueBackend:
    """Shared distributed task queue backend powered by RESP3 Streams and Sorted Sets."""

    _DIALECT: ClassVar[RESPQueueDialect]

    __slots__ = (
        "_client",
        "_config",
        "_notification_source",
        "_prefix",
        "_sync_client",
        "_sync_notification_source",
    )

    def __init__(
        self,
        config: QueueConfig | QueueBackendSettings | None = None,
        *,
        client: AsyncRESPStreamClient | None = None,
        sync_client: SyncRESPStreamClient | None = None,
        notification_source: Any = None,
        sync_notification_source: Any = None,
    ) -> None:
        """Bind the queue backend to RESP Streams clients and notification sources."""
        if client is None:
            ensure_module(
                self._DIALECT.driver_module,
                package=self._DIALECT.extra_package,
                extra=self._DIALECT.extra_name,
            )
        self._config = config or QueueConfig()
        self._prefix = getattr(self._config, "stream_prefix", self._DIALECT.default_key_prefix)
        self._notification_source = notification_source
        self._sync_notification_source = sync_notification_source
        self._client: AsyncRESPStreamClient = client if client is not None else self._build_client(self._config)
        self._sync_client: SyncRESPStreamClient | None = sync_client

    @property
    def client(self) -> AsyncRESPStreamClient:
        """Return the active async RESP stream client."""
        return self._client

    @property
    def sync_client(self) -> SyncRESPStreamClient:
        """Return the active synchronous RESP stream client."""
        return self._get_sync_client()

    @property
    def notification_source(self) -> Any:
        """Return the active async notification source."""
        return self._notification_source

    @property
    def sync_notification_source(self) -> Any:
        """Return the active synchronous notification source."""
        return self._sync_notification_source

    def _get_sync_client(self) -> SyncRESPStreamClient:
        if self._sync_client is not None:
            return self._sync_client
        sync_cli = getattr(self._client, "sync_client", None)
        if sync_cli is not None:
            return cast("SyncRESPStreamClient", sync_cli)
        if isinstance(self._client, SyncRESPStreamClient):
            return self._client
        self._sync_client = self._build_sync_client(self._config)
        return self._sync_client

    def enqueue_sync(self, envelope: TaskEnvelope) -> TaskEnvelope:
        """Synchronously enqueue task into delayed ZSET or target lane stream."""
        now = time.time()
        encoded_str = encode_json_bytes(envelope).decode("utf-8")
        sync_cli = self._get_sync_client()

        if envelope.scheduled_at is not None and envelope.scheduled_at > now:
            scheduled_key = f"{self._prefix}:scheduled"
            sync_cli.zadd(scheduled_key, {encoded_str: envelope.scheduled_at})
            return envelope

        stream_key = f"{self._prefix}:lane:{envelope.lane}"
        entry_id = sync_cli.xadd(
            stream_key,
            {
                "payload": encoded_str,
                "lane": envelope.lane,
                "task_name": envelope.task_name,
            },
        )
        if self._sync_notification_source is not None:
            with contextlib.suppress(Exception):
                self._sync_notification_source.publish("vertebra_queue_wakeup", envelope.lane)

        return msgspec.structs.replace(envelope, receipt_handle=entry_id)

    async def enqueue(self, envelope: TaskEnvelope) -> TaskEnvelope:
        """Enqueue task into delayed ZSET or target lane stream."""
        now = time.time()
        encoded_str = encode_json_bytes(envelope).decode("utf-8")

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
        if self._notification_source is not None:
            with contextlib.suppress(Exception):
                await self._notification_source.publish("vertebra_queue_wakeup", envelope.lane)

        return msgspec.structs.replace(envelope, receipt_handle=entry_id)

    def store_result(self, record: TaskResultRecord, *, ttl_seconds: float | None = 86400.0) -> None:
        """Synchronously persist task execution result."""
        key = f"{self._prefix}:result:{record.task_id}"
        payload = encode_json_bytes(record).decode("utf-8")
        px = int(ttl_seconds * 1000) if ttl_seconds is not None else None
        self._get_sync_client().set(key, payload, px=px)

    async def astore_result(self, record: TaskResultRecord, *, ttl_seconds: float | None = 86400.0) -> None:
        """Asynchronously persist task execution result."""
        key = f"{self._prefix}:result:{record.task_id}"
        payload = encode_json_bytes(record).decode("utf-8")
        px = int(ttl_seconds * 1000) if ttl_seconds is not None else None
        await self._client.set(key, payload, px=px)

    def get_result(self, task_id: str) -> TaskResultRecord | None:
        """Synchronously retrieve task execution result."""
        key = f"{self._prefix}:result:{task_id}"
        raw = self._get_sync_client().get(key)
        if raw is None:
            return None
        raw_bytes = raw.encode("utf-8") if isinstance(raw, str) else raw
        return msgspec.convert(decode_json(raw_bytes), type=TaskResultRecord)

    async def aget_result(self, task_id: str) -> TaskResultRecord | None:
        """Asynchronously retrieve task execution result."""
        key = f"{self._prefix}:result:{task_id}"
        raw = await self._client.get(key)
        if raw is None:
            return None
        raw_bytes = raw.encode("utf-8") if isinstance(raw, str) else raw
        return msgspec.convert(decode_json(raw_bytes), type=TaskResultRecord)

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
        consumer_group = getattr(self._config, "consumer_group", "vertebra-workers")
        claimed: list[TaskEnvelope] = []

        for lane in lanes:
            if len(claimed) >= batch_size:
                break
            stream_key = f"{self._prefix}:lane:{lane}"
            with contextlib.suppress(Exception):
                await self._client.xgroup_create(
                    stream_key,
                    consumer_group,
                    id="0",
                    mkstream=True,
                )

            remaining = batch_size - len(claimed)
            _, recovered_entries, _ = await self._client.xautoclaim(
                stream_key,
                consumer_group,
                self._config.consumer_name,
                min_idle_ms,
                count=remaining,
            )
            for entry_id, fields in recovered_entries:
                envelope = self._decode_envelope(fields, entry_id)
                reclaimed_env = msgspec.structs.replace(
                    envelope,
                    attempt=envelope.attempt + 1,
                    receipt_handle=entry_id,
                )
                claimed.append(reclaimed_env)

        if len(claimed) < batch_size:
            read_streams = {f"{self._prefix}:lane:{lane}": ">" for lane in lanes}
            block_ms = int(timeout_seconds * 1000) if timeout_seconds > 0 else None
            remaining = batch_size - len(claimed)
            read_results = await self._client.xreadgroup(
                consumer_group,
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
        consumer_group = getattr(self._config, "consumer_group", "vertebra-workers")
        stream_key = f"{self._prefix}:lane:{envelope.lane}"
        await self._client.xack(
            stream_key,
            consumer_group,
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
        consumer_group = getattr(self._config, "consumer_group", "vertebra-workers")
        if envelope.receipt_handle:
            stream_key = f"{self._prefix}:lane:{envelope.lane}"
            await self._client.xack(
                stream_key,
                consumer_group,
                envelope.receipt_handle,
            )
            await self._client.xdel(stream_key, envelope.receipt_handle)

        err_type = type(error).__name__ if isinstance(error, Exception) else "TaskExecutionError"
        err_msg = str(error)
        result: DeadLetterEntry | None = None

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
            dlq_max_len = getattr(self._config, "dlq_max_len", 10_000)
            await self._client.xadd(
                dlq_key,
                {"payload": dlq_payload, "dlq_id": dlq_id},
                maxlen=dlq_max_len,
            )
            result = dlq_entry
        else:
            now = time.time()
            delay = delay_seconds if delay_seconds is not None else 1.0
            retry_env = msgspec.structs.replace(
                envelope,
                attempt=envelope.attempt + 1,
                scheduled_at=now + delay,
                receipt_handle=None,
            )
            retry_payload = encode_json_bytes(retry_env).decode("utf-8")
            scheduled_key = f"{self._prefix}:scheduled"
            await self._client.zadd(scheduled_key, {retry_payload: now + delay})

        return result

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
        return await self.renew_lease(lease_key, owner_id, ttl_seconds, slot=slot)

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
        """Close client connections."""
        await self._client.aclose()
        if self._sync_client is not None:
            self._sync_client.close()

    def _decode_envelope(self, fields: Any, receipt_handle: str) -> TaskEnvelope:
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

    @classmethod
    def _wrap_async_client(cls, client: Any) -> AsyncRESPStreamClient:
        raise NotImplementedError

    @classmethod
    def _wrap_sync_client(cls, client: Any) -> SyncRESPStreamClient:
        raise NotImplementedError

    @classmethod
    def _build_client(cls, config: QueueConfig | QueueBackendSettings) -> AsyncRESPStreamClient:
        _validate_resp_url(cls._DIALECT, config.backend_url)
        mod = ensure_module(
            cls._DIALECT.driver_module,
            package=cls._DIALECT.extra_package,
            extra=cls._DIALECT.extra_name,
        )
        client_cls = getattr(mod, cls._DIALECT.async_client_attr)
        return cls._wrap_async_client(client_cls(config.backend_url))

    @classmethod
    def _build_sync_client(cls, config: QueueConfig | QueueBackendSettings) -> SyncRESPStreamClient:
        _validate_resp_url(cls._DIALECT, config.backend_url)
        mod = ensure_module(
            cls._DIALECT.driver_module,
            package=cls._DIALECT.extra_package,
            extra=cls._DIALECT.extra_name,
        )
        client_cls = getattr(mod, cls._DIALECT.sync_client_attr)
        return cls._wrap_sync_client(client_cls(config.backend_url))


def _decode_str(val: Any) -> str:
    return val.decode("utf-8") if isinstance(val, bytes) else str(val)


def _decode_stream_entries(raw: Any) -> list[tuple[str, dict[str, Any]]]:
    entries: list[tuple[str, dict[str, Any]]] = []
    if not raw:
        return entries
    for item in raw:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            entries.append((_decode_str(item[0]), _normalize_stream_fields(item[1])))
    return entries


class _BaseRESPStreamAdapter:
    """Shared async adapter bridging a native Redis/Valkey client to `AsyncRESPStreamClient`."""

    __slots__ = ("_client",)

    def __init__(self, client: Any) -> None:
        self._client = client

    async def xgroup_create(self, stream: str, group: str, id: str = "$", mkstream: bool = False) -> Any:
        """Create consumer group on stream."""
        return await self._client.xgroup_create(stream, group, id=id, mkstream=mkstream)

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
        res = await self._client.xautoclaim(stream, group, consumer, min_idle_time, start_id=start_id, count=count)
        if not res or len(res) < 3:
            return "0-0", [], []
        next_id, raw_entries, deleted = res
        return _decode_str(next_id), _decode_stream_entries(raw_entries), [_decode_str(d) for d in deleted]

    async def xdel(self, stream: str, *ids: str) -> int:
        """Delete stream entries by ID."""
        return int(await self._client.xdel(stream, list(ids)))

    async def set(self, key: str, value: str, nx: bool = False, px: int | None = None) -> Any:
        """Set key with optional NX and PX options."""
        return await self._client.set(key, value, nx=nx, px=px)

    async def get(self, key: str) -> str | None:
        """Get string value by key."""
        raw = await self._client.get(key)
        return None if raw is None else _decode_str(raw)


class _BaseSyncRESPStreamAdapter:
    """Shared sync adapter bridging a native Redis/Valkey client to `SyncRESPStreamClient`."""

    __slots__ = ("_client",)

    def __init__(self, client: Any) -> None:
        self._client = client

    def set(self, key: str, value: str, nx: bool = False, px: int | None = None) -> Any:
        """Set key with optional NX and PX options."""
        return self._client.set(key, value, nx=nx, px=px)

    def get(self, key: str) -> str | None:
        """Get string value by key."""
        raw = self._client.get(key)
        return None if raw is None else _decode_str(raw)

    def publish(self, channel: str, message: Any) -> int:
        """Publish message to channel."""
        return int(self._client.publish(channel, message))

    def close(self) -> None:
        """Close client connection."""
        self._client.close()
