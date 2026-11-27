"""Agent-to-Agent (A2A) 1.0 JSON-RPC protocol service and task store.

This module provides A2AService, A2ATaskStore, and PushNotificationDispatcher
implementing all 11 core A2A 1.0 JSON-RPC methods, SSE streaming events,
and caller-isolated durable task management.
"""

from __future__ import annotations

import builtins
import contextlib
import ipaddress
import secrets
import socket
import time
import urllib.parse
from collections.abc import AsyncIterable, Callable, Coroutine, Mapping
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

import anyio
import msgspec

from vertebra.agent.a2a.models import (
    TERMINAL_A2A_STATES,
    AgentCard,
    AgentSkill,
    Artifact,
    Message,
    PushNotificationConfig,
    Task,
    TaskArtifactUpdateEvent,
    TaskStatus,
    TaskStatusUpdateEvent,
    TextPart,
)
from vertebra.agent.a2a.signing import AgentCardSigner
from vertebra.agent.core.context import AgentContext
from vertebra.agent.core.cursor import paginate_sequence
from vertebra.agent.core.jsonrpc import (
    INVALID_PARAMS,
    JSONRPCError,
    JSONRPCException,
    JSONRPCRequest,
    JSONRPCRouter,
)
from vertebra.agent.core.sse import CacheSseStreamBroker, SseFrame, SseStreamBroker
from vertebra.agent.telemetry import AgentTelemetryRecorder
from vertebra.cache import SyncCacheStore
from vertebra.cache._sync import _parse_url_scheme
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import decode_json, encode_json, encode_json_bytes

if TYPE_CHECKING:
    import httpx

__all__ = (
    "CONTENT_TYPE_NOT_SUPPORTED",
    "EXTENDED_CARD_NOT_CONFIGURED",
    "PUSH_NOTIFICATION_NOT_SUPPORTED",
    "TASK_NOT_CANCELABLE",
    "TASK_NOT_FOUND",
    "UNSUPPORTED_OPERATION",
    "A2AConfig",
    "A2AService",
    "A2ATaskStore",
    "CacheA2ATaskStore",
    "PushNotificationDispatcher",
    "get_owner_key",
)

TASK_NOT_FOUND: int = -32001
TASK_NOT_CANCELABLE: int = -32002
PUSH_NOTIFICATION_NOT_SUPPORTED: int = -32003
UNSUPPORTED_OPERATION: int = -32004
CONTENT_TYPE_NOT_SUPPORTED: int = -32005
EXTENDED_CARD_NOT_CONFIGURED: int = -32006


def get_owner_key(context: AgentContext | None) -> str:
    """Derive caller ownership key from AgentContext identity."""
    if context is not None and context.identity is not None:
        key = context.identity.owner_key()
        if key:
            return key
    return "default:anonymous"


class A2AConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration options for the A2A 1.0 service."""

    path: str = "/a2a"
    """HTTP route path where the A2A JSON-RPC endpoint is mounted."""
    base_url: str | None = None
    """Optional public origin (`https://agents.example.com`) for `AgentCard` interfaces; derived dynamically from request headers when `None`."""
    agent_card_path: str = "/.well-known/agent-card.json"
    """Well-known discovery route serving the public `AgentCard` JSON."""
    agent_card: AgentCard | None = None
    """Public `AgentCard` advertised at `agent_card_path`."""
    card_secret: str | None = None
    """Optional signing secret or key material used to verify/sign the `AgentCard`."""
    extended_card: AgentCard | None = None
    """Authenticated extended `AgentCard` returned by `agent/authenticatedExtendedCard`."""
    enable_streaming: bool = True
    """Allow `message/stream` and `tasks/resubscribe` SSE event streams."""
    enable_push_notifications: bool = True
    """Enable webhook registration and delivery via `PushNotificationDispatcher`."""
    allow_insecure_webhooks: bool = False
    """Allow plain HTTP or loopback/private-network webhook URLs (for local testing only)."""
    max_tasks: int = 10000
    """Maximum number of tasks retained in `A2ATaskStore` before LRU eviction."""
    task_ttl_seconds: int = 86400
    """Time-to-live in seconds (`24 hours` default) for retained task records."""
    cache_url: str | None = None
    """Optional `vertebra.cache` backend URL for distributed A2A tasks, push configs, and SSE replay."""
    task_store: Any | None = None
    """Optional pre-configured A2A task store overriding `cache_url` and in-memory defaults."""
    push_dispatcher: Any | None = None
    """Optional pre-configured push notification dispatcher overriding `cache_url` and defaults."""
    stream_broker: Any | None = None
    """Optional pre-configured SSE stream broker overriding `cache_url` and in-memory defaults."""

    def __post_init__(self) -> None:
        if self.cache_url is not None:
            _parse_url_scheme(self.cache_url)
        if self.task_ttl_seconds <= 0:
            raise ValueError("A2AConfig.task_ttl_seconds must be positive")


class A2ATaskStore:
    """Owner-isolated durable task store with bounded capacity and TTL eviction."""

    def __init__(self, max_tasks: int = 10000, ttl_seconds: int = 86400) -> None:
        self.max_tasks = max_tasks
        self.ttl_seconds = ttl_seconds
        self._tasks: dict[str, tuple[Task, str, float]] = {}

    def save(self, task: Task, owner_id: str) -> None:
        """Store or update a task with caller ownership."""
        self._evict_expired()
        self._tasks[task.task_id] = (task, owner_id, self._now())

    def get(self, task_id: str, owner_id: str) -> Task:
        """Retrieve a task under caller ownership, raising TASK_NOT_FOUND on mismatch."""
        entry = self._tasks.get(task_id)
        if entry is None:
            raise JSONRPCException(JSONRPCError(code=TASK_NOT_FOUND, message=f"Task '{task_id}' not found"))
        task, stored_owner, _ = entry
        if stored_owner != owner_id:
            raise JSONRPCException(JSONRPCError(code=TASK_NOT_FOUND, message=f"Task '{task_id}' not found"))
        return task

    def list(
        self,
        owner_id: str,
        limit: int = 50,
        cursor: str | None = None,
    ) -> tuple[tuple[Task, ...], str | None]:
        """List tasks belonging to owner_id with cursor pagination."""
        matching_tasks = [task for task, stored_owner, _ in self._tasks.values() if stored_owner == owner_id]
        matching_tasks.sort(key=lambda t: t.task_id)
        page = paginate_sequence(matching_tasks, cursor=cursor, page_size=limit)
        return page.items, page.next_cursor

    def cancel(self, task_id: str, owner_id: str) -> Task:
        """Cancel a non-terminal task under caller ownership."""
        task = self.get(task_id, owner_id)
        if task.status.state in TERMINAL_A2A_STATES:
            raise JSONRPCException(
                JSONRPCError(
                    code=TASK_NOT_CANCELABLE,
                    message=f"Task '{task_id}' cannot be canceled from terminal state '{task.status.state}'",
                )
            )

        cancel_msg = Message(
            message_id=f"msg-cancel-{secrets.token_hex(4)}",
            role="agent",
            parts=(TextPart(text="Task canceled by caller"),),
            task_id=task_id,
        )
        updated_status = TaskStatus(
            state="canceled",
            message=cancel_msg,
            timestamp=datetime.now(UTC).isoformat(),
        )
        updated_task = Task(
            task_id=task.task_id,
            status=updated_status,
            context_id=task.context_id,
            artifacts=task.artifacts,
            metadata=task.metadata,
            owner_id=task.owner_id,
        )
        self.save(updated_task, owner_id)
        return updated_task

    def _now(self) -> float:
        return time.time()

    def _evict_expired(self) -> None:
        current_time = self._now()
        expired_keys = [
            task_id
            for task_id, (_, _, updated_at) in self._tasks.items()
            if (current_time - updated_at) > self.ttl_seconds
        ]
        for key in expired_keys:
            self._tasks.pop(key, None)

        if len(self._tasks) > self.max_tasks:
            sorted_entries = sorted(self._tasks.items(), key=lambda item: item[1][2])
            overflow_count = len(self._tasks) - self.max_tasks
            for idx in range(overflow_count):
                self._tasks.pop(sorted_entries[idx][0], None)


class CacheA2ATaskStore(A2ATaskStore):
    """Distributed owner-isolated A2A task store backed by SyncCacheStore."""

    def __init__(
        self,
        store: SyncCacheStore | None = None,
        *,
        cache_url: str = "memory://",
        key_prefix: str = "vtb:a2a:task:",
        max_tasks: int = 10000,
        ttl_seconds: int = 86400,
    ) -> None:
        super().__init__(max_tasks=max_tasks, ttl_seconds=ttl_seconds)
        self.key_prefix = key_prefix
        self._owns_store = store is None
        self._store: SyncCacheStore = (
            store
            if store is not None
            else SyncCacheStore(cache_url, key_prefix=key_prefix, default_ttl=float(ttl_seconds))
        )

    def save(self, task: Task, owner_id: str) -> None:
        """Store or update a task with caller ownership, enforcing monotonic terminal states."""
        existing = self._load_entry(task.task_id)
        if existing is not None:
            existing_task, existing_owner = existing
            if existing_owner != owner_id:
                raise JSONRPCException(JSONRPCError(code=TASK_NOT_FOUND, message=f"Task '{task.task_id}' not found"))
            if existing_task.status.state in TERMINAL_A2A_STATES:
                return
        payload = {
            "task": _serialize_struct(task),
            "owner_id": owner_id,
            "updated_at": self._now(),
        }
        self._store.set(self._item_key(task.task_id), encode_json_bytes(payload), ttl=float(self.ttl_seconds))
        owner_key = self._owner_key(owner_id)
        task_ids = self._load_id_list(owner_key)
        if task.task_id not in task_ids:
            task_ids.append(task.task_id)
            self._save_id_list(owner_key, task_ids)

    def get(self, task_id: str, owner_id: str) -> Task:
        """Retrieve a task under caller ownership, raising TASK_NOT_FOUND on mismatch."""
        entry = self._load_entry(task_id)
        if entry is None:
            raise JSONRPCException(JSONRPCError(code=TASK_NOT_FOUND, message=f"Task '{task_id}' not found"))
        task, stored_owner = entry
        if stored_owner != owner_id:
            raise JSONRPCException(JSONRPCError(code=TASK_NOT_FOUND, message=f"Task '{task_id}' not found"))
        return task

    def list(
        self,
        owner_id: str,
        limit: int = 50,
        cursor: str | None = None,
    ) -> tuple[tuple[Task, ...], str | None]:
        """List tasks belonging to owner_id with cursor pagination."""
        owner_key = self._owner_key(owner_id)
        task_ids = self._load_id_list(owner_key)
        matching_tasks: builtins.list[Task] = []
        kept_ids: builtins.list[str] = []
        dirty = False
        for task_id in task_ids:
            entry = self._load_entry(task_id)
            if entry is not None and entry[1] == owner_id:
                matching_tasks.append(entry[0])
                kept_ids.append(task_id)
            else:
                dirty = True
        if dirty:
            self._save_id_list(owner_key, kept_ids)
        matching_tasks.sort(key=lambda t: t.task_id)
        page = paginate_sequence(matching_tasks, cursor=cursor, page_size=limit)
        return page.items, page.next_cursor

    def close(self) -> None:
        """Release the backing SyncCacheStore if owned by this task store."""
        if self._owns_store:
            self._store.close()

    def _item_key(self, task_id: str) -> str:
        return f"{self.key_prefix}item:{task_id}"

    def _owner_key(self, owner_id: str) -> str:
        return f"{self.key_prefix}owner:{owner_id}"

    def _load_entry(self, task_id: str) -> tuple[Task, str] | None:
        raw = self._store.get(self._item_key(task_id))
        if raw is None:
            return None
        parsed = decode_json(raw)
        if not isinstance(parsed, dict) or "task" not in parsed:
            return None
        task = msgspec.convert(parsed["task"], Task)
        stored_owner = str(parsed.get("owner_id", ""))
        return task, stored_owner

    def _load_id_list(self, key: str) -> builtins.list[str]:
        raw = self._store.get(key)
        if raw is None:
            return []
        parsed = decode_json(raw)
        if not isinstance(parsed, builtins.list):
            return []
        return [str(item) for item in parsed]

    def _save_id_list(self, key: str, ids: builtins.list[str]) -> None:
        if not ids:
            self._store.delete(key)
            return
        unique_ids = builtins.list(dict.fromkeys(ids))
        self._store.set(key, encode_json_bytes(unique_ids), ttl=float(self.ttl_seconds))


class PushNotificationDispatcher:
    """Manages and delivers signed push notification webhooks with strict SSRF validation."""

    def __init__(
        self,
        allow_insecure_webhooks: bool = False,
        *,
        http_client: httpx.AsyncClient | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        store: SyncCacheStore | None = None,
        cache_url: str | None = None,
        key_prefix: str = "vtb:a2a:push:",
        ttl_seconds: float | None = 86400.0,
    ) -> None:
        self.allow_insecure_webhooks = allow_insecure_webhooks
        self._http_client = http_client
        self._transport = transport
        self.key_prefix = key_prefix
        self.ttl_seconds = ttl_seconds
        self._owns_store = store is None and cache_url is not None
        if store is not None:
            self._store: SyncCacheStore | None = store
        elif cache_url is not None:
            self._store = SyncCacheStore(cache_url, key_prefix=key_prefix, default_ttl=ttl_seconds)
        else:
            self._store = None
        self._configs: dict[str, tuple[PushNotificationConfig, str]] = {}

    @staticmethod
    def _is_disallowed_ip(
        ip: ipaddress.IPv4Address | ipaddress.IPv6Address,
        *,
        allow_insecure: bool,
    ) -> bool:
        candidate: ipaddress.IPv4Address | ipaddress.IPv6Address = ip
        if isinstance(candidate, ipaddress.IPv6Address) and candidate.ipv4_mapped is not None:
            candidate = candidate.ipv4_mapped
        if candidate.is_link_local or candidate.is_multicast or candidate.is_unspecified:
            return True
        return bool(not allow_insecure and (candidate.is_private or candidate.is_loopback or candidate.is_reserved))

    def validate_webhook_url(
        self,
        url: str,
        *,
        allow_insecure_webhooks: bool | None = None,
    ) -> tuple[str, str, int, str, str]:
        """Resolve and validate webhook URL against SSRF, returning (scheme, host, port, path, resolved_ip)."""
        effective_allow_insecure = (
            self.allow_insecure_webhooks if allow_insecure_webhooks is None else allow_insecure_webhooks
        )
        parsed = urllib.parse.urlparse(url)
        scheme = parsed.scheme.lower()
        if scheme not in ("http", "https"):
            msg = f"Unsupported webhook scheme: {parsed.scheme}"
            raise ValueError(msg)
        if not effective_allow_insecure and scheme != "https":
            msg = "Insecure HTTP webhooks are disallowed; use HTTPS"
            raise ValueError(msg)

        hostname = (parsed.hostname or "").lower()
        if not hostname:
            msg = "Missing hostname in webhook URL"
            raise ValueError(msg)

        if hostname in ("169.254.169.254", "metadata.google.internal", "instance-data"):
            msg = f"Webhook URL targets cloud metadata: {hostname}"
            raise ValueError(msg)

        if not effective_allow_insecure and (
            hostname == "localhost" or hostname.endswith(".local") or hostname.endswith(".internal")
        ):
            msg = f"Webhook URL targets internal hostname: {hostname}"
            raise ValueError(msg)

        try:
            port = parsed.port if parsed.port is not None else (443 if scheme == "https" else 80)
        except ValueError as exc:
            msg = f"Invalid port in webhook URL: {exc}"
            raise ValueError(msg) from exc

        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"

        try:
            addr_infos = socket.getaddrinfo(hostname, port, type=socket.SOCK_STREAM)
        except OSError as exc:
            msg = f"Failed to resolve webhook hostname {hostname!r}: {exc}"
            raise ValueError(msg) from exc

        if not addr_infos:
            msg = f"Webhook hostname {hostname!r} resolved to no addresses"
            raise ValueError(msg)

        validated_ips: list[str] = []
        for info in addr_infos:
            sockaddr = info[4]
            if not sockaddr:
                continue
            raw_ip = str(sockaddr[0]).split("%", 1)[0]
            try:
                ip = ipaddress.ip_address(raw_ip)
            except ValueError as exc:
                msg = f"Invalid resolved IP address {raw_ip!r} for webhook hostname {hostname!r}"
                raise ValueError(msg) from exc
            if self._is_disallowed_ip(ip, allow_insecure=effective_allow_insecure):
                msg = f"Webhook URL targets disallowed IP address: {ip}"
                raise ValueError(msg)
            validated_ips.append(str(ip))

        if not validated_ips:
            msg = f"Webhook hostname {hostname!r} resolved to no usable addresses"
            raise ValueError(msg)

        return scheme, hostname, port, path, validated_ips[0]

    def _config_key(self, task_id: str) -> str:
        return f"{self.key_prefix}item:{task_id}"

    def _load_config_entry(self, task_id: str) -> tuple[PushNotificationConfig, str] | None:
        if self._store is None:
            return self._configs.get(task_id)
        raw = self._store.get(self._config_key(task_id))
        if raw is None:
            return None
        parsed = decode_json(raw)
        if not isinstance(parsed, dict) or "config" not in parsed:
            return None
        config = msgspec.convert(parsed["config"], PushNotificationConfig)
        stored_owner = str(parsed.get("owner_id", ""))
        return config, stored_owner

    def close(self) -> None:
        """Release the backing SyncCacheStore if owned by this dispatcher."""
        if self._owns_store and self._store is not None:
            self._store.close()

    def set_config(self, task_id: str, config: PushNotificationConfig, owner_id: str) -> None:
        """Register or update a push notification config for a task."""
        try:
            self.validate_webhook_url(config.url)
        except ValueError as exc:
            raise JSONRPCException(JSONRPCError(code=INVALID_PARAMS, message=str(exc))) from exc
        if self._store is not None:
            payload = {"config": _serialize_struct(config), "owner_id": owner_id}
            self._store.set(self._config_key(task_id), encode_json_bytes(payload), ttl=self.ttl_seconds)
            return
        self._configs[task_id] = (config, owner_id)

    def get_config(self, task_id: str, owner_id: str) -> PushNotificationConfig:
        """Retrieve push notification config for a task."""
        entry = self._load_config_entry(task_id)
        if entry is None or entry[1] != owner_id:
            raise JSONRPCException(
                JSONRPCError(code=TASK_NOT_FOUND, message=f"Push notification config for '{task_id}' not found")
            )
        return entry[0]

    def list_configs(self, task_id: str, owner_id: str) -> tuple[PushNotificationConfig, ...]:
        """List push notification configs for a task."""
        entry = self._load_config_entry(task_id)
        if entry is None or entry[1] != owner_id:
            return ()
        return (entry[0],)

    def delete_config(self, task_id: str, owner_id: str) -> bool:
        """Delete push notification config for a task."""
        entry = self._load_config_entry(task_id)
        if entry is None or entry[1] != owner_id:
            return False
        if self._store is not None:
            return bool(self._store.delete(self._config_key(task_id)))
        self._configs.pop(task_id, None)
        return True

    async def dispatch(self, task_id: str, event: TaskStatusUpdateEvent | TaskArtifactUpdateEvent) -> None:
        """Deliver status or artifact update event to configured webhook via DNS-pinned IP connection."""
        entry = self._load_config_entry(task_id)
        if entry is None:
            return
        config, _ = entry
        scheme, host, port, path, resolved_ip = self.validate_webhook_url(config.url)
        ip_obj = ipaddress.ip_address(resolved_ip)
        connect_host = f"[{resolved_ip}]" if ip_obj.version == 6 else resolved_ip
        default_port = 443 if scheme == "https" else 80
        target_url = (
            f"{scheme}://{connect_host}:{port}{path}" if port != default_port else f"{scheme}://{connect_host}{path}"
        )
        host_header = f"{host}:{port}" if port != default_port else host
        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "Host": host_header,
        }
        if config.token:
            headers["Authorization"] = f"Bearer {config.token}"
        extensions: dict[str, Any] = {}
        if scheme == "https":
            extensions["sni_hostname"] = host

        payload_bytes = encode_json_bytes(_serialize_struct(event))
        httpx_mod = ensure_module("httpx")

        if self._http_client is not None:
            with contextlib.suppress(httpx_mod.HTTPError):
                await self._http_client.post(
                    target_url,
                    content=payload_bytes,
                    headers=headers,
                    extensions=extensions,
                    follow_redirects=False,
                    timeout=10.0,
                )
            return

        async with httpx_mod.AsyncClient(
            transport=self._transport,
            timeout=10.0,
            follow_redirects=False,
        ) as client:
            with contextlib.suppress(httpx_mod.HTTPError):
                await client.post(
                    target_url,
                    content=payload_bytes,
                    headers=headers,
                    extensions=extensions,
                )


type A2AExecutorCallable = Callable[
    [Message, str | None, str],
    Coroutine[Any, Any, tuple[TaskStatus, tuple[Artifact, ...]] | Task],
]


class A2AService:
    """Coordinates JSON-RPC dispatch, SSE event streaming, and durable execution for A2A 1.0."""

    def __init__(
        self,
        config: A2AConfig | None = None,
        task_store: A2ATaskStore | None = None,
        push_dispatcher: PushNotificationDispatcher | None = None,
        stream_broker: SseStreamBroker | None = None,
        telemetry: AgentTelemetryRecorder | None = None,
        executor: A2AExecutorCallable | None = None,
        *,
        tool_registry: Any | None = None,
    ) -> None:
        self.config = config if config is not None else A2AConfig()
        self._owned_sync_cache: SyncCacheStore | None = None
        self._owns_task_store = False
        self._owns_push_dispatcher = False
        self._owns_stream_broker = False

        resolved_task_store = task_store if task_store is not None else self.config.task_store
        if resolved_task_store is None:
            if self.config.cache_url is not None:
                sync_cache = self._ensure_owned_sync_cache()
                resolved_task_store = CacheA2ATaskStore(
                    store=sync_cache,
                    key_prefix="vtb:a2a:task:",
                    max_tasks=self.config.max_tasks,
                    ttl_seconds=self.config.task_ttl_seconds,
                )
                self._owns_task_store = True
            else:
                resolved_task_store = A2ATaskStore(
                    max_tasks=self.config.max_tasks,
                    ttl_seconds=self.config.task_ttl_seconds,
                )
        self.task_store: A2ATaskStore = resolved_task_store

        resolved_push = push_dispatcher if push_dispatcher is not None else self.config.push_dispatcher
        if resolved_push is None:
            if self.config.cache_url is not None:
                sync_cache = self._ensure_owned_sync_cache()
                resolved_push = PushNotificationDispatcher(
                    allow_insecure_webhooks=self.config.allow_insecure_webhooks,
                    store=sync_cache,
                    key_prefix="vtb:a2a:push:",
                    ttl_seconds=float(self.config.task_ttl_seconds),
                )
                self._owns_push_dispatcher = True
            else:
                resolved_push = PushNotificationDispatcher(
                    allow_insecure_webhooks=self.config.allow_insecure_webhooks,
                )
        self.push_dispatcher: PushNotificationDispatcher = resolved_push

        resolved_broker = stream_broker if stream_broker is not None else self.config.stream_broker
        if resolved_broker is None:
            if self.config.cache_url is not None:
                sync_cache = self._ensure_owned_sync_cache()
                resolved_broker = CacheSseStreamBroker(
                    store=sync_cache,
                    key_prefix="vtb:a2a:sse:",
                    ttl_seconds=float(self.config.task_ttl_seconds),
                )
                self._owns_stream_broker = True
            else:
                resolved_broker = SseStreamBroker()
        self.stream_broker: SseStreamBroker = resolved_broker

        self.telemetry = telemetry if telemetry is not None else AgentTelemetryRecorder()
        self.executor = executor
        self.tool_registry: Any | None = tool_registry
        self.router = JSONRPCRouter()
        self._event_counter: int = 0
        self._register_routes()

    def _ensure_owned_sync_cache(self) -> SyncCacheStore:
        if self._owned_sync_cache is None:
            assert self.config.cache_url is not None
            self._owned_sync_cache = SyncCacheStore(self.config.cache_url)
        return self._owned_sync_cache

    def close(self) -> None:
        """Release any cache stores owned by this A2AService instance."""
        if self._owns_task_store:
            close_fn = getattr(self.task_store, "close", None)
            if callable(close_fn):
                close_fn()
        if self._owns_push_dispatcher:
            close_fn = getattr(self.push_dispatcher, "close", None)
            if callable(close_fn):
                close_fn()
        if self._owns_stream_broker:
            close_fn = getattr(self.stream_broker, "close", None)
            if callable(close_fn):
                close_fn()
        if self._owned_sync_cache is not None:
            self._owned_sync_cache.close()
            self._owned_sync_cache = None

    async def aclose(self) -> None:
        """Asynchronously release any cache stores owned by this A2AService instance."""
        self.close()

    async def handle_payload(self, body: bytes, context: AgentContext | None = None) -> bytes:
        """Process incoming raw JSON-RPC body and return response bytes."""
        try:
            req_data = decode_json(body)
        except Exception as exc:
            err_resp = {
                "jsonrpc": "2.0",
                "id": None,
                "error": {"code": -32700, "message": f"Parse error: {exc}"},
            }
            return encode_json_bytes(err_resp)

        if isinstance(req_data, list):
            results: list[dict[str, Any]] = []
            for item in req_data:
                if isinstance(item, dict):
                    req = JSONRPCRequest(
                        method=item.get("method", ""),
                        id=item.get("id"),
                        params=item.get("params"),
                    )
                    resp = await self.router.dispatch(req, context)
                    if resp is not None:
                        results.append(resp)
            return encode_json_bytes(results)

        if isinstance(req_data, dict):
            req = JSONRPCRequest(
                method=req_data.get("method", ""),
                id=req_data.get("id"),
                params=req_data.get("params"),
            )
            resp = await self.router.dispatch(req, context)
            if resp is None:
                return b""
            return encode_json_bytes(resp)

        return encode_json_bytes(
            {
                "jsonrpc": "2.0",
                "id": None,
                "error": {"code": -32600, "message": "Invalid Request: Expected object or list"},
            }
        )

    async def execute_task_flow(
        self,
        message: Message,
        context_id: str | None,
        task_id: str,
        owner_id: str,
    ) -> Task:
        """Run task execution lifecycle, transitioning states and publishing SSE events."""
        initial_status = TaskStatus(
            state="working",
            message=message,
            timestamp=datetime.now(UTC).isoformat(),
        )
        task = Task(
            task_id=task_id,
            status=initial_status,
            context_id=context_id,
            owner_id=owner_id,
        )
        self.task_store.save(task, owner_id)
        with self.telemetry.span_a2a_task(task_id, "working"):
            await self._emit_and_dispatch(
                task_id,
                TaskStatusUpdateEvent(task_id=task_id, status=initial_status, final=False),
            )

        artifacts: tuple[Artifact, ...] = ()
        if self.executor is not None:
            exec_result = await self.executor(message, context_id, task_id)
            if isinstance(exec_result, Task):
                final_status = exec_result.status
                artifacts = exec_result.artifacts
            else:
                final_status, artifacts = exec_result
        else:
            reply_text = f"Processed: {message.message_id}"
            reply_msg = Message(
                message_id=f"msg-reply-{secrets.token_hex(4)}",
                role="agent",
                parts=(TextPart(text=reply_text),),
                task_id=task_id,
                context_id=context_id,
            )
            final_status = TaskStatus(
                state="completed",
                message=reply_msg,
                timestamp=datetime.now(UTC).isoformat(),
            )

        for art in artifacts:
            await self._emit_and_dispatch(
                task_id,
                TaskArtifactUpdateEvent(task_id=task_id, artifact=art),
            )

        completed_task = Task(
            task_id=task_id,
            status=final_status,
            context_id=context_id,
            artifacts=artifacts,
            owner_id=owner_id,
        )
        self.task_store.save(completed_task, owner_id)

        with self.telemetry.span_a2a_task(task_id, final_status.state):
            await self._emit_and_dispatch(
                task_id,
                TaskStatusUpdateEvent(
                    task_id=task_id,
                    status=final_status,
                    final=final_status.state in TERMINAL_A2A_STATES,
                ),
            )

        return completed_task

    async def handle_message_send(
        self,
        params: dict[str, Any],
        context: AgentContext | None = None,
    ) -> dict[str, Any]:
        """A2A 1.0 message/send RPC handler."""
        with self.telemetry.span_rpc("a2a", "message/send", params=params):
            if "message" not in params:
                raise JSONRPCException(JSONRPCError(code=INVALID_PARAMS, message="Missing 'message' in params"))
            message = self._parse_message(params["message"])
            context_id = params.get("contextId") or message.context_id
            task_id = params.get("taskId") or message.task_id or f"task-{secrets.token_hex(6)}"
            owner_id = get_owner_key(context)

            completed_task = await self.execute_task_flow(message, context_id, task_id, owner_id)
            return _serialize_struct(completed_task)

    async def handle_message_stream(
        self,
        params: dict[str, Any],
        context: AgentContext | None = None,
    ) -> dict[str, Any]:
        """A2A 1.0 message/stream RPC handler for initial task creation."""
        with self.telemetry.span_rpc("a2a", "message/stream", params=params):
            if "message" not in params:
                raise JSONRPCException(JSONRPCError(code=INVALID_PARAMS, message="Missing 'message' in params"))
            message = self._parse_message(params["message"])
            context_id = params.get("contextId") or message.context_id
            task_id = params.get("taskId") or message.task_id or f"task-{secrets.token_hex(6)}"
            owner_id = get_owner_key(context)

            initial_status = TaskStatus(
                state="working",
                message=message,
                timestamp=datetime.now(UTC).isoformat(),
            )
            task = Task(
                task_id=task_id,
                status=initial_status,
                context_id=context_id,
                owner_id=owner_id,
            )
            self.task_store.save(task, owner_id)
            self._publish_event(
                task_id,
                TaskStatusUpdateEvent(task_id=task_id, status=initial_status, final=False),
            )
            return _serialize_struct(task)

    async def stream_message(
        self,
        params: dict[str, Any],
        context: AgentContext | None = None,
    ) -> AsyncIterable[SseFrame]:
        """Stream SSE frames for a message execution until completion."""
        if "message" not in params:
            raise JSONRPCException(JSONRPCError(code=INVALID_PARAMS, message="Missing 'message' in params"))
        message = self._parse_message(params["message"])
        context_id = params.get("contextId") or message.context_id
        task_id = params.get("taskId") or message.task_id or f"task-{secrets.token_hex(6)}"
        owner_id = get_owner_key(context)

        recv_stream, _ = self.stream_broker.subscribe(task_id)

        async def _run_in_background() -> None:
            await self.execute_task_flow(message, context_id, task_id, owner_id)

        closed_early = False
        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(_run_in_background)

                try:
                    async for frame in recv_stream:
                        yield frame
                        try:
                            payload = decode_json(frame.data)
                            if isinstance(payload, dict) and payload.get("final") is True:
                                break
                        except Exception:
                            pass
                except GeneratorExit:
                    closed_early = True
                    tg.cancel_scope.cancel()
                except BaseException:
                    tg.cancel_scope.cancel()
                    raise
        finally:
            with contextlib.suppress(Exception):
                recv_stream.close()
            self.stream_broker.disconnect(task_id)
        if closed_early:
            return

    async def handle_tasks_get(
        self,
        params: dict[str, Any],
        context: AgentContext | None = None,
    ) -> dict[str, Any]:
        """A2A 1.0 tasks/get RPC handler."""
        with self.telemetry.span_rpc("a2a", "tasks/get", params=params):
            task_id = params.get("taskId") or params.get("id")
            if not task_id:
                raise JSONRPCException(JSONRPCError(code=INVALID_PARAMS, message="Missing 'taskId' in params"))
            owner_id = get_owner_key(context)
            task = self.task_store.get(task_id, owner_id)
            return _serialize_struct(task)

    async def handle_tasks_list(
        self,
        params: dict[str, Any],
        context: AgentContext | None = None,
    ) -> dict[str, Any]:
        """A2A 1.0 tasks/list RPC handler."""
        with self.telemetry.span_rpc("a2a", "tasks/list", params=params):
            limit = int(params.get("limit", 50))
            cursor = params.get("cursor")
            owner_id = get_owner_key(context)
            tasks, next_cursor = self.task_store.list(owner_id, limit=limit, cursor=cursor)
            return {
                "tasks": [_serialize_struct(t) for t in tasks],
                "nextCursor": next_cursor,
            }

    async def handle_tasks_cancel(
        self,
        params: dict[str, Any],
        context: AgentContext | None = None,
    ) -> dict[str, Any]:
        """A2A 1.0 tasks/cancel RPC handler."""
        with self.telemetry.span_rpc("a2a", "tasks/cancel", params=params):
            task_id = params.get("taskId") or params.get("id")
            if not task_id:
                raise JSONRPCException(JSONRPCError(code=INVALID_PARAMS, message="Missing 'taskId' in params"))
            owner_id = get_owner_key(context)
            updated_task = self.task_store.cancel(task_id, owner_id)
            await self._emit_and_dispatch(
                task_id,
                TaskStatusUpdateEvent(task_id=task_id, status=updated_task.status, final=True),
            )
            return _serialize_struct(updated_task)

    async def handle_tasks_resubscribe(
        self,
        params: dict[str, Any],
        context: AgentContext | None = None,
    ) -> dict[str, Any]:
        """A2A 1.0 tasks/resubscribe RPC handler."""
        with self.telemetry.span_rpc("a2a", "tasks/resubscribe", params=params):
            task_id = params.get("taskId") or params.get("id")
            if not task_id:
                raise JSONRPCException(JSONRPCError(code=INVALID_PARAMS, message="Missing 'taskId' in params"))
            owner_id = get_owner_key(context)
            task = self.task_store.get(task_id, owner_id)
            return _serialize_struct(task)

    async def stream_resubscribe(
        self,
        params: dict[str, Any],
        context: AgentContext | None = None,
    ) -> AsyncIterable[SseFrame]:
        """Stream replayed and live SSE frames for tasks/resubscribe."""
        task_id = params.get("taskId") or params.get("id")
        if not task_id:
            raise JSONRPCException(JSONRPCError(code=INVALID_PARAMS, message="Missing 'taskId' in params"))
        owner_id = get_owner_key(context)
        task = self.task_store.get(task_id, owner_id)
        last_event_id = params.get("lastEventId")

        recv_stream, _ = self.stream_broker.subscribe(task_id, last_event_id=last_event_id)

        try:
            if task.status.state in TERMINAL_A2A_STATES:
                while True:
                    try:
                        frame = recv_stream.receive_nowait()
                        yield frame
                    except (anyio.WouldBlock, anyio.ClosedResourceError):
                        break
                return

            async for frame in recv_stream:
                yield frame
                try:
                    payload = decode_json(frame.data)
                    if isinstance(payload, dict) and payload.get("final") is True:
                        break
                except Exception:
                    pass
        finally:
            with contextlib.suppress(Exception):
                recv_stream.close()
            self.stream_broker.disconnect(task_id)

    async def handle_push_set(
        self,
        params: dict[str, Any],
        context: AgentContext | None = None,
    ) -> dict[str, Any]:
        """A2A 1.0 tasks/pushNotificationConfig/set RPC handler."""
        with self.telemetry.span_rpc("a2a", "tasks/pushNotificationConfig/set", params=params):
            if not self.config.enable_push_notifications:
                raise JSONRPCException(
                    JSONRPCError(
                        code=PUSH_NOTIFICATION_NOT_SUPPORTED,
                        message="Push notifications are disabled",
                    )
                )
            task_id = params.get("taskId")
            config_data = params.get("config")
            if not task_id or not config_data:
                raise JSONRPCException(
                    JSONRPCError(code=INVALID_PARAMS, message="Missing 'taskId' or 'config' in params")
                )
            owner_id = get_owner_key(context)
            self.task_store.get(task_id, owner_id)

            try:
                config = msgspec.convert(config_data, PushNotificationConfig)
            except Exception as exc:
                raise JSONRPCException(
                    JSONRPCError(code=INVALID_PARAMS, message=f"Invalid PushNotificationConfig: {exc}")
                ) from exc

            self.push_dispatcher.set_config(task_id, config, owner_id)
            return {"taskId": task_id, "status": "configured"}

    async def handle_push_get(
        self,
        params: dict[str, Any],
        context: AgentContext | None = None,
    ) -> dict[str, Any]:
        """A2A 1.0 tasks/pushNotificationConfig/get RPC handler."""
        with self.telemetry.span_rpc("a2a", "tasks/pushNotificationConfig/get", params=params):
            task_id = params.get("taskId")
            if not task_id:
                raise JSONRPCException(JSONRPCError(code=INVALID_PARAMS, message="Missing 'taskId' in params"))
            owner_id = get_owner_key(context)
            self.task_store.get(task_id, owner_id)
            config = self.push_dispatcher.get_config(task_id, owner_id)
            return _serialize_struct(config)

    async def handle_push_list(
        self,
        params: dict[str, Any],
        context: AgentContext | None = None,
    ) -> dict[str, Any]:
        """A2A 1.0 tasks/pushNotificationConfig/list RPC handler."""
        with self.telemetry.span_rpc("a2a", "tasks/pushNotificationConfig/list", params=params):
            task_id = params.get("taskId")
            if not task_id:
                raise JSONRPCException(JSONRPCError(code=INVALID_PARAMS, message="Missing 'taskId' in params"))
            owner_id = get_owner_key(context)
            self.task_store.get(task_id, owner_id)
            configs = self.push_dispatcher.list_configs(task_id, owner_id)
            return {"configs": [_serialize_struct(c) for c in configs]}

    async def handle_push_delete(
        self,
        params: dict[str, Any],
        context: AgentContext | None = None,
    ) -> dict[str, Any]:
        """A2A 1.0 tasks/pushNotificationConfig/delete RPC handler."""
        with self.telemetry.span_rpc("a2a", "tasks/pushNotificationConfig/delete", params=params):
            task_id = params.get("taskId")
            if not task_id:
                raise JSONRPCException(JSONRPCError(code=INVALID_PARAMS, message="Missing 'taskId' in params"))
            owner_id = get_owner_key(context)
            self.task_store.get(task_id, owner_id)
            deleted = self.push_dispatcher.delete_config(task_id, owner_id)
            return {"deleted": deleted}

    def resolve_agent_card(
        self,
        headers: Mapping[str, str] | None = None,
        *,
        base_url: str | None = None,
        extended: bool = False,
        registry: Any | None = None,
    ) -> AgentCard | None:
        """Resolve the public or extended AgentCard with auto-populated interface, URL, and ToolSpec skills."""
        card = self.config.extended_card if extended else self.config.agent_card
        if card is None:
            return None
        resolved = card.resolve_for_request(
            route_path=self.config.path,
            base_url=base_url if base_url is not None else self.config.base_url,
            headers=headers,
        )
        effective_registry = registry if registry is not None else self.tool_registry
        if effective_registry is not None and hasattr(effective_registry, "list_specs"):
            existing_ids = {s.id for s in resolved.skills}
            projected_skills = list(resolved.skills)
            for spec in effective_registry.list_specs(exposure="a2a"):
                if spec.name not in existing_ids:
                    existing_ids.add(spec.name)
                    projected_skills.append(
                        AgentSkill(
                            id=spec.name,
                            name=spec.title or spec.name,
                            description=spec.description,
                        )
                    )
            if len(projected_skills) != len(resolved.skills):
                resolved = msgspec.structs.replace(resolved, skills=tuple(projected_skills))
        if self.config.card_secret and not resolved.signatures:
            resolved = AgentCardSigner.sign(resolved, self.config.card_secret)
        return resolved

    async def handle_extended_card(
        self,
        params: dict[str, Any] | None = None,
        context: AgentContext | None = None,
    ) -> dict[str, Any]:
        """A2A 1.0 agent/authenticatedExtendedCard RPC handler."""
        with self.telemetry.span_rpc("a2a", "agent/authenticatedExtendedCard", params=params):
            if context is None or context.identity is None or not context.identity.is_authenticated:
                raise JSONRPCException(JSONRPCError(code=-32000, message="Authentication required for extended card"))
            req_headers = context.metadata.get("headers") if isinstance(context.metadata, Mapping) else None
            resolved_card = self.resolve_agent_card(
                headers=req_headers if isinstance(req_headers, Mapping) else None,
                extended=True,
            )
            if resolved_card is None:
                raise JSONRPCException(
                    JSONRPCError(
                        code=EXTENDED_CARD_NOT_CONFIGURED,
                        message="Authenticated extended card not configured",
                    )
                )
            return resolved_card.to_dict()

    def _next_event_id(self) -> str:
        self._event_counter += 1
        return f"evt-{self._event_counter}"

    def _register_routes(self) -> None:
        self.router.register("message/send", self.handle_message_send)
        self.router.register("message/stream", self.handle_message_stream)
        self.router.register("tasks/get", self.handle_tasks_get)
        self.router.register("tasks/list", self.handle_tasks_list)
        self.router.register("tasks/cancel", self.handle_tasks_cancel)
        self.router.register("tasks/resubscribe", self.handle_tasks_resubscribe)
        self.router.register("tasks/pushNotificationConfig/set", self.handle_push_set)
        self.router.register("tasks/pushNotificationConfig/get", self.handle_push_get)
        self.router.register("tasks/pushNotificationConfig/list", self.handle_push_list)
        self.router.register("tasks/pushNotificationConfig/delete", self.handle_push_delete)
        self.router.register("agent/authenticatedExtendedCard", self.handle_extended_card)

    def _publish_event(
        self,
        task_id: str,
        event: TaskStatusUpdateEvent | TaskArtifactUpdateEvent,
    ) -> SseFrame:
        event_id = self._next_event_id()
        data_str = encode_json(_serialize_struct(event))
        frame = SseFrame(id=event_id, event=event.kind, data=data_str)
        self.stream_broker.publish(task_id, frame)
        return frame

    async def _emit_and_dispatch(
        self,
        task_id: str,
        event: TaskStatusUpdateEvent | TaskArtifactUpdateEvent,
    ) -> SseFrame:
        frame = self._publish_event(task_id, event)
        if self.config.enable_push_notifications:
            await self.push_dispatcher.dispatch(task_id, event)
        return frame

    def _parse_message(self, message_data: dict[str, Any]) -> Message:
        try:
            return msgspec.convert(message_data, Message)
        except Exception as exc:
            raise JSONRPCException(
                JSONRPCError(code=INVALID_PARAMS, message=f"Invalid Message payload: {exc}")
            ) from exc


def _serialize_struct(obj: Any) -> dict[str, Any]:
    """Serialize msgspec Struct to a standard Python dictionary."""
    return cast(dict[str, Any], decode_json(encode_json_bytes(obj)))
