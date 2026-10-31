"""Agent-to-Agent (A2A) 1.0 JSON-RPC protocol service and task store.

This module provides A2AService, A2ATaskStore, and PushNotificationDispatcher
implementing all 11 core A2A 1.0 JSON-RPC methods, SSE streaming events,
and caller-isolated durable task management.
"""

import ipaddress
import secrets
import time
import urllib.parse
import urllib.request
from collections.abc import AsyncIterable, Callable, Coroutine
from datetime import UTC, datetime
from typing import Any, cast

import anyio
import msgspec

from vertebra.agent.a2a.models import (
    TERMINAL_A2A_STATES,
    AgentCard,
    Artifact,
    Message,
    PushNotificationConfig,
    Task,
    TaskArtifactUpdateEvent,
    TaskStatus,
    TaskStatusUpdateEvent,
    TextPart,
)
from vertebra.agent.core.context import AgentContext
from vertebra.agent.core.cursor import paginate_sequence
from vertebra.agent.core.jsonrpc import (
    INVALID_PARAMS,
    JsonRpcError,
    JsonRpcErrorException,
    JsonRpcRequest,
    JsonRpcRouter,
)
from vertebra.agent.core.sse import SseFrame, SseStreamBroker
from vertebra.agent.telemetry import AgentTelemetryRecorder
from vertebra.utils.serializers import decode_json, encode_json, encode_json_bytes

TASK_NOT_FOUND: int = -32001
TASK_NOT_CANCELABLE: int = -32002
PUSH_NOTIFICATION_NOT_SUPPORTED: int = -32003
UNSUPPORTED_OPERATION: int = -32004
CONTENT_TYPE_NOT_SUPPORTED: int = -32005
EXTENDED_CARD_NOT_CONFIGURED: int = -32006


def _serialize_struct(obj: Any) -> dict[str, Any]:
    """Serialize msgspec Struct to a standard Python dictionary."""
    return cast(dict[str, Any], decode_json(encode_json_bytes(obj)))


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


class A2ATaskStore:
    """Owner-isolated durable task store with bounded capacity and TTL eviction."""

    def __init__(self, max_tasks: int = 10000, ttl_seconds: int = 86400) -> None:
        self.max_tasks = max_tasks
        self.ttl_seconds = ttl_seconds
        self._tasks: dict[str, tuple[Task, str, float]] = {}

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

    def save(self, task: Task, owner_id: str) -> None:
        """Store or update a task with caller ownership."""
        self._evict_expired()
        self._tasks[task.task_id] = (task, owner_id, self._now())

    def get(self, task_id: str, owner_id: str) -> Task:
        """Retrieve a task under caller ownership, raising TASK_NOT_FOUND on mismatch."""
        entry = self._tasks.get(task_id)
        if entry is None:
            raise JsonRpcErrorException(JsonRpcError(code=TASK_NOT_FOUND, message=f"Task '{task_id}' not found"))
        task, stored_owner, _ = entry
        if stored_owner != owner_id:
            raise JsonRpcErrorException(JsonRpcError(code=TASK_NOT_FOUND, message=f"Task '{task_id}' not found"))
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
            raise JsonRpcErrorException(
                JsonRpcError(
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


class PushNotificationDispatcher:
    """Manages and delivers signed push notification webhooks with strict SSRF validation."""

    def __init__(self, allow_insecure_webhooks: bool = False) -> None:
        self.allow_insecure_webhooks = allow_insecure_webhooks
        self._configs: dict[str, tuple[PushNotificationConfig, str]] = {}

    def validate_webhook_url(self, url: str) -> None:
        """Validate webhook URL against SSRF and unsupported targets."""
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in ("http", "https"):
            raise JsonRpcErrorException(
                JsonRpcError(code=INVALID_PARAMS, message=f"Unsupported webhook scheme: {parsed.scheme}")
            )
        if not self.allow_insecure_webhooks and parsed.scheme != "https":
            raise JsonRpcErrorException(
                JsonRpcError(code=INVALID_PARAMS, message="Insecure HTTP webhooks are disallowed; use HTTPS")
            )

        hostname = (parsed.hostname or "").lower()
        if not hostname:
            raise JsonRpcErrorException(JsonRpcError(code=INVALID_PARAMS, message="Missing hostname in webhook URL"))

        if hostname in ("169.254.169.254", "metadata.google.internal", "instance-data"):
            raise JsonRpcErrorException(
                JsonRpcError(code=INVALID_PARAMS, message=f"Webhook URL targets cloud metadata: {hostname}")
            )

        try:
            ip = ipaddress.ip_address(hostname)
            if ip.is_link_local:
                raise JsonRpcErrorException(
                    JsonRpcError(code=INVALID_PARAMS, message=f"Webhook URL targets link-local address: {ip}")
                )
            if not self.allow_insecure_webhooks and (ip.is_private or ip.is_loopback or ip.is_reserved):
                raise JsonRpcErrorException(
                    JsonRpcError(code=INVALID_PARAMS, message=f"Webhook URL targets private/loopback IP: {ip}")
                )
        except ValueError as err:
            if not self.allow_insecure_webhooks and (
                hostname == "localhost" or hostname.endswith(".local") or hostname.endswith(".internal")
            ):
                raise JsonRpcErrorException(
                    JsonRpcError(code=INVALID_PARAMS, message=f"Webhook URL targets internal hostname: {hostname}")
                ) from err

    def set_config(self, task_id: str, config: PushNotificationConfig, owner_id: str) -> None:
        """Register or update a push notification config for a task."""
        self.validate_webhook_url(config.url)
        self._configs[task_id] = (config, owner_id)

    def get_config(self, task_id: str, owner_id: str) -> PushNotificationConfig:
        """Retrieve push notification config for a task."""
        entry = self._configs.get(task_id)
        if entry is None or entry[1] != owner_id:
            raise JsonRpcErrorException(
                JsonRpcError(code=TASK_NOT_FOUND, message=f"Push notification config for '{task_id}' not found")
            )
        return entry[0]

    def list_configs(self, task_id: str, owner_id: str) -> tuple[PushNotificationConfig, ...]:
        """List push notification configs for a task."""
        entry = self._configs.get(task_id)
        if entry is None or entry[1] != owner_id:
            return ()
        return (entry[0],)

    def delete_config(self, task_id: str, owner_id: str) -> bool:
        """Delete push notification config for a task."""
        entry = self._configs.get(task_id)
        if entry is None or entry[1] != owner_id:
            return False
        self._configs.pop(task_id, None)
        return True

    async def dispatch(self, task_id: str, event: TaskStatusUpdateEvent | TaskArtifactUpdateEvent) -> None:
        """Deliver status or artifact update event to configured webhook."""
        entry = self._configs.get(task_id)
        if entry is None:
            return
        config, _ = entry
        headers = {"Content-Type": "application/json"}
        if config.token:
            headers["Authorization"] = f"Bearer {config.token}"

        payload_bytes = encode_json_bytes(_serialize_struct(event))

        def _send() -> None:
            req = urllib.request.Request(config.url, data=payload_bytes, headers=headers, method="POST")
            try:
                with urllib.request.urlopen(req, timeout=10.0) as resp:
                    resp.read()
            except Exception:
                pass

        await anyio.to_thread.run_sync(_send)


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
    ) -> None:
        self.config = config if config is not None else A2AConfig()
        self.task_store = (
            task_store
            if task_store is not None
            else A2ATaskStore(
                max_tasks=self.config.max_tasks,
                ttl_seconds=self.config.task_ttl_seconds,
            )
        )
        self.push_dispatcher = (
            push_dispatcher
            if push_dispatcher is not None
            else PushNotificationDispatcher(
                allow_insecure_webhooks=self.config.allow_insecure_webhooks,
            )
        )
        self.stream_broker = stream_broker if stream_broker is not None else SseStreamBroker()
        self.telemetry = telemetry if telemetry is not None else AgentTelemetryRecorder()
        self.executor = executor
        self.router = JsonRpcRouter()
        self._event_counter: int = 0
        self._register_routes()

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
                    req = JsonRpcRequest(
                        method=item.get("method", ""),
                        id=item.get("id"),
                        params=item.get("params"),
                    )
                    resp = await self.router.dispatch(req, context)
                    if resp is not None:
                        results.append(resp)
            return encode_json_bytes(results)

        if isinstance(req_data, dict):
            req = JsonRpcRequest(
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
            raise JsonRpcErrorException(
                JsonRpcError(code=INVALID_PARAMS, message=f"Invalid Message payload: {exc}")
            ) from exc

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
                raise JsonRpcErrorException(JsonRpcError(code=INVALID_PARAMS, message="Missing 'message' in params"))
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
                raise JsonRpcErrorException(JsonRpcError(code=INVALID_PARAMS, message="Missing 'message' in params"))
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
            raise JsonRpcErrorException(JsonRpcError(code=INVALID_PARAMS, message="Missing 'message' in params"))
        message = self._parse_message(params["message"])
        context_id = params.get("contextId") or message.context_id
        task_id = params.get("taskId") or message.task_id or f"task-{secrets.token_hex(6)}"
        owner_id = get_owner_key(context)

        recv_stream, _ = self.stream_broker.subscribe(task_id)

        async def _run_in_background() -> None:
            await self.execute_task_flow(message, context_id, task_id, owner_id)

        async with anyio.create_task_group() as tg:
            tg.start_soon(_run_in_background)

            async for frame in recv_stream:
                yield frame
                try:
                    payload = decode_json(frame.data)
                    if isinstance(payload, dict) and payload.get("final") is True:
                        break
                except Exception:
                    pass

        self.stream_broker.disconnect(task_id)

    async def handle_tasks_get(
        self,
        params: dict[str, Any],
        context: AgentContext | None = None,
    ) -> dict[str, Any]:
        """A2A 1.0 tasks/get RPC handler."""
        with self.telemetry.span_rpc("a2a", "tasks/get", params=params):
            task_id = params.get("taskId") or params.get("id")
            if not task_id:
                raise JsonRpcErrorException(JsonRpcError(code=INVALID_PARAMS, message="Missing 'taskId' in params"))
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
                raise JsonRpcErrorException(JsonRpcError(code=INVALID_PARAMS, message="Missing 'taskId' in params"))
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
                raise JsonRpcErrorException(JsonRpcError(code=INVALID_PARAMS, message="Missing 'taskId' in params"))
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
            raise JsonRpcErrorException(JsonRpcError(code=INVALID_PARAMS, message="Missing 'taskId' in params"))
        owner_id = get_owner_key(context)
        task = self.task_store.get(task_id, owner_id)
        last_event_id = params.get("lastEventId")

        recv_stream, _ = self.stream_broker.subscribe(task_id, last_event_id=last_event_id)

        if task.status.state in TERMINAL_A2A_STATES:
            while True:
                try:
                    frame = recv_stream.receive_nowait()
                    yield frame
                except (anyio.WouldBlock, anyio.ClosedResourceError):
                    break
            self.stream_broker.disconnect(task_id)
            return

        async for frame in recv_stream:
            yield frame
            try:
                payload = decode_json(frame.data)
                if isinstance(payload, dict) and payload.get("final") is True:
                    break
            except Exception:
                pass

        self.stream_broker.disconnect(task_id)

    async def handle_push_set(
        self,
        params: dict[str, Any],
        context: AgentContext | None = None,
    ) -> dict[str, Any]:
        """A2A 1.0 tasks/pushNotificationConfig/set RPC handler."""
        with self.telemetry.span_rpc("a2a", "tasks/pushNotificationConfig/set", params=params):
            if not self.config.enable_push_notifications:
                raise JsonRpcErrorException(
                    JsonRpcError(
                        code=PUSH_NOTIFICATION_NOT_SUPPORTED,
                        message="Push notifications are disabled",
                    )
                )
            task_id = params.get("taskId")
            config_data = params.get("config")
            if not task_id or not config_data:
                raise JsonRpcErrorException(
                    JsonRpcError(code=INVALID_PARAMS, message="Missing 'taskId' or 'config' in params")
                )
            owner_id = get_owner_key(context)
            self.task_store.get(task_id, owner_id)

            try:
                config = msgspec.convert(config_data, PushNotificationConfig)
            except Exception as exc:
                raise JsonRpcErrorException(
                    JsonRpcError(code=INVALID_PARAMS, message=f"Invalid PushNotificationConfig: {exc}")
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
                raise JsonRpcErrorException(JsonRpcError(code=INVALID_PARAMS, message="Missing 'taskId' in params"))
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
                raise JsonRpcErrorException(JsonRpcError(code=INVALID_PARAMS, message="Missing 'taskId' in params"))
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
                raise JsonRpcErrorException(JsonRpcError(code=INVALID_PARAMS, message="Missing 'taskId' in params"))
            owner_id = get_owner_key(context)
            self.task_store.get(task_id, owner_id)
            deleted = self.push_dispatcher.delete_config(task_id, owner_id)
            return {"deleted": deleted}

    async def handle_extended_card(
        self,
        params: dict[str, Any] | None = None,
        context: AgentContext | None = None,
    ) -> dict[str, Any]:
        """A2A 1.0 agent/authenticatedExtendedCard RPC handler."""
        with self.telemetry.span_rpc("a2a", "agent/authenticatedExtendedCard", params=params):
            if context is None or context.identity is None or not context.identity.is_authenticated:
                raise JsonRpcErrorException(
                    JsonRpcError(code=-32000, message="Authentication required for extended card")
                )
            if self.config.extended_card is None:
                raise JsonRpcErrorException(
                    JsonRpcError(
                        code=EXTENDED_CARD_NOT_CONFIGURED,
                        message="Authenticated extended card not configured",
                    )
                )
            return self.config.extended_card.to_dict()


__all__ = [
    "CONTENT_TYPE_NOT_SUPPORTED",
    "EXTENDED_CARD_NOT_CONFIGURED",
    "PUSH_NOTIFICATION_NOT_SUPPORTED",
    "TASK_NOT_CANCELABLE",
    "TASK_NOT_FOUND",
    "UNSUPPORTED_OPERATION",
    "A2AConfig",
    "A2AService",
    "A2ATaskStore",
    "PushNotificationDispatcher",
    "get_owner_key",
]
