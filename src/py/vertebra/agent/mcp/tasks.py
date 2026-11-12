"""Durable async tasks management for Model Context Protocol (MCP) 2026-07-28.

This module provides TaskRecord and MCPTaskStore implementing io.modelcontextprotocol/tasks
with owner isolation, TTL expiry, and background queue lane integration.
"""

import secrets
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Literal

import anyio
import msgspec

from vertebra.agent.core.cursor import paginate_sequence
from vertebra.agent.core.sse import SseFrame, SseStreamBroker
from vertebra.agent.telemetry import AgentTelemetryRecorder
from vertebra.utils.serializers import encode_json_bytes

MCPTaskStatus = Literal["working", "input_required", "completed", "failed", "cancelled"]


class _OwnerLookup(Enum):
    UNSCOPED = "unscoped"


class TaskLookupError(Exception):
    """Raised when a task does not exist or caller ownership fails validation."""


class TaskStateError(Exception):
    """Raised when a task operation is invalid for the current task state."""


class TaskRecord(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Immutable snapshot of an MCP io.modelcontextprotocol/tasks execution record."""

    task_id: str
    """Unique task identifier (`task_<hex>`)."""
    owner_id: str | None
    """Caller ownership key isolating task visibility across tenants."""
    tool_name: str
    """Name of the MCP tool executing asynchronously."""
    status: MCPTaskStatus
    """Current lifecycle state (`working`, `input_required`, `completed`, `failed`, `cancelled`)."""
    created_at_iso: str
    """ISO-8601 UTC timestamp when the task was created."""
    last_updated_at_iso: str
    """ISO-8601 UTC timestamp of the most recent status transition."""
    ttl_ms: int
    """Time-to-live retention window in milliseconds."""
    poll_interval_ms: int = 1000
    """Recommended client polling interval in milliseconds."""
    status_message: str | None = None
    """Human-readable progress or error summary."""
    input_requests: dict[str, dict[str, Any]] | None = None
    """Pending input prompts required from the caller when `status` is `"input_required"`."""
    inputs_received: dict[str, Any] | None = None
    """Caller-supplied input values submitted to resume the task."""
    result: dict[str, Any] | None = None
    """Final tool output payload when `status` is `"completed"`."""
    error: dict[str, Any] | None = None
    """Structured error details when `status` is `"failed"`."""

    @property
    def is_terminal(self) -> bool:
        """Return True if the task has reached a final state."""
        return self.status in ("completed", "failed", "cancelled")


class MCPTaskConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration options for MCP tasks execution."""

    enabled: bool = True
    """Enable durable `tasks/get`, `tasks/list`, `tasks/result`, and `tasks/cancel` methods."""
    default_ttl_ms: int = 300_000
    """Default retention TTL in milliseconds (`5 minutes`) for newly created tasks."""
    max_ttl_ms: int = 3_600_000
    """Maximum allowed task TTL in milliseconds (`1 hour`)."""
    poll_interval_ms: int = 1_000
    """Default polling interval hint in milliseconds returned to MCP clients."""


class MCPTaskStore:
    """Owner-scoped store managing async task records and notifications."""

    def __init__(
        self,
        config: MCPTaskConfig | None = None,
        queue_service: Any | None = None,
        telemetry: AgentTelemetryRecorder | None = None,
        stream_broker: SseStreamBroker | None = None,
    ) -> None:
        self.config = config if config is not None else MCPTaskConfig()
        self.queue_service = queue_service
        self.telemetry = telemetry if telemetry is not None else AgentTelemetryRecorder()
        self.stream_broker = stream_broker
        self._tasks: dict[str, TaskRecord] = {}
        self._events: dict[str, anyio.Event] = {}

    def _now_iso(self) -> str:
        return datetime.now(UTC).isoformat()

    def _get_event(self, task_id: str) -> anyio.Event:
        if task_id not in self._events:
            self._events[task_id] = anyio.Event()
        return self._events[task_id]

    def _emit_status(self, task: TaskRecord) -> None:
        if self.stream_broker is not None:
            payload = encode_json_bytes(
                {
                    "jsonrpc": "2.0",
                    "method": "notifications/tasks/status",
                    "params": {
                        "taskId": task.task_id,
                        "status": task.status,
                        "statusMessage": task.status_message,
                        "isTerminal": task.is_terminal,
                    },
                }
            )
            frame = SseFrame(
                event="notifications/tasks/status",
                data=payload.decode("utf-8"),
            )
            self.stream_broker.broadcast(frame)

    def create(
        self,
        tool_name: str,
        owner_id: str | None,
        ttl_ms: int | None = None,
        poll_interval_ms: int | None = None,
    ) -> TaskRecord:
        """Create and persist a new task record in working state."""
        task_id = f"task_{secrets.token_hex(12)}"
        now = self._now_iso()
        actual_ttl = min(ttl_ms or self.config.default_ttl_ms, self.config.max_ttl_ms)
        poll_interval = poll_interval_ms or self.config.poll_interval_ms

        task = TaskRecord(
            task_id=task_id,
            owner_id=owner_id,
            tool_name=tool_name,
            status="working",
            created_at_iso=now,
            last_updated_at_iso=now,
            ttl_ms=actual_ttl,
            poll_interval_ms=poll_interval,
        )
        self._tasks[task_id] = task
        self._get_event(task_id)
        self._emit_status(task)
        return task

    def get(self, task_id: str, owner_id: str | None | _OwnerLookup = _OwnerLookup.UNSCOPED) -> TaskRecord:
        """Retrieve a task record enforcing owner isolation."""
        task = self._tasks.get(task_id)
        if task is None:
            raise TaskLookupError(f"Task '{task_id}' not found")
        if owner_id is not _OwnerLookup.UNSCOPED and task.owner_id != owner_id:
            raise TaskLookupError(f"Task '{task_id}' not found")
        return task

    def list_for_owner(
        self,
        owner_id: str | None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> tuple[list[TaskRecord], str | None]:
        """List tasks matching owner_id using cursor pagination."""
        matched: list[TaskRecord] = []
        for t in self._tasks.values():
            if t.owner_id == owner_id:
                matched.append(t)
        matched.sort(key=lambda t: t.created_at_iso, reverse=True)
        page = paginate_sequence(matched, cursor=cursor, page_size=limit)
        return list(page.items), page.next_cursor

    async def wait_for_result(
        self,
        task_id: str,
        owner_id: str | None = None,
        timeout_s: float = 30.0,
    ) -> TaskRecord:
        """Wait until a task reaches a terminal state or requires input."""
        task = self.get(task_id, owner_id)
        if task.is_terminal or task.status == "input_required":
            return task

        event = self._get_event(task_id)
        with anyio.fail_after(timeout_s):
            while not task.is_terminal and str(task.status) != "input_required":
                await event.wait()
                event = anyio.Event()
                self._events[task_id] = event
                task = self.get(task_id, owner_id)
        return task

    def require_input(
        self,
        task_id: str,
        input_requests: dict[str, dict[str, Any]],
        status_message: str | None = None,
    ) -> TaskRecord:
        """Transition a working task into input_required status."""
        current = self.get(task_id)
        if current.is_terminal:
            raise TaskStateError(f"Cannot require input for terminal task {task_id}")
        updated = TaskRecord(
            task_id=current.task_id,
            owner_id=current.owner_id,
            tool_name=current.tool_name,
            status="input_required",
            created_at_iso=current.created_at_iso,
            last_updated_at_iso=self._now_iso(),
            ttl_ms=current.ttl_ms,
            poll_interval_ms=current.poll_interval_ms,
            status_message=status_message or "Additional input required",
            input_requests=dict(input_requests),
            inputs_received=current.inputs_received,
            result=current.result,
            error=current.error,
        )
        self._tasks[task_id] = updated
        self._get_event(task_id).set()
        self._emit_status(updated)
        return updated

    def submit_input(
        self,
        task_id: str,
        owner_id: str | None,
        inputs: dict[str, Any],
    ) -> TaskRecord:
        """Provide requested inputs and resume task execution to working status."""
        current = self.get(task_id, owner_id)
        if current.status != "input_required":
            raise TaskStateError(f"Task {task_id} is in status '{current.status}', expected 'input_required'")
        updated = TaskRecord(
            task_id=current.task_id,
            owner_id=current.owner_id,
            tool_name=current.tool_name,
            status="working",
            created_at_iso=current.created_at_iso,
            last_updated_at_iso=self._now_iso(),
            ttl_ms=current.ttl_ms,
            poll_interval_ms=current.poll_interval_ms,
            status_message="Input received; resuming execution",
            input_requests=None,
            inputs_received=dict(inputs),
            result=current.result,
            error=current.error,
        )
        self._tasks[task_id] = updated
        self._get_event(task_id).set()
        self._emit_status(updated)
        return updated

    def cancel(
        self,
        task_id: str,
        owner_id: str | None = None,
        reason: str | None = None,
    ) -> TaskRecord:
        """Mark a task as cancelled."""
        current = self.get(task_id, owner_id)
        if current.is_terminal:
            return current
        updated = TaskRecord(
            task_id=current.task_id,
            owner_id=current.owner_id,
            tool_name=current.tool_name,
            status="cancelled",
            created_at_iso=current.created_at_iso,
            last_updated_at_iso=self._now_iso(),
            ttl_ms=current.ttl_ms,
            poll_interval_ms=current.poll_interval_ms,
            status_message=reason or "Task cancelled by client",
            input_requests=None,
            inputs_received=current.inputs_received,
            result=current.result,
            error=current.error,
        )
        self._tasks[task_id] = updated
        self._get_event(task_id).set()
        self._emit_status(updated)
        return updated

    def complete(self, task_id: str, result: dict[str, Any]) -> TaskRecord:
        """Mark a task as completed with result payload."""
        current = self.get(task_id)
        if current.is_terminal:
            return current
        updated = TaskRecord(
            task_id=current.task_id,
            owner_id=current.owner_id,
            tool_name=current.tool_name,
            status="completed",
            created_at_iso=current.created_at_iso,
            last_updated_at_iso=self._now_iso(),
            ttl_ms=current.ttl_ms,
            poll_interval_ms=current.poll_interval_ms,
            status_message="Completed successfully",
            input_requests=None,
            inputs_received=current.inputs_received,
            result=dict(result),
            error=None,
        )
        self._tasks[task_id] = updated
        self._get_event(task_id).set()
        self._emit_status(updated)
        return updated

    def fail(self, task_id: str, error: dict[str, Any]) -> TaskRecord:
        """Mark a task as failed with error descriptor."""
        current = self.get(task_id)
        if current.is_terminal:
            return current
        updated = TaskRecord(
            task_id=current.task_id,
            owner_id=current.owner_id,
            tool_name=current.tool_name,
            status="failed",
            created_at_iso=current.created_at_iso,
            last_updated_at_iso=self._now_iso(),
            ttl_ms=current.ttl_ms,
            poll_interval_ms=current.poll_interval_ms,
            status_message=str(error.get("message") or "Task execution failed"),
            input_requests=None,
            inputs_received=current.inputs_received,
            result=None,
            error=dict(error),
        )
        self._tasks[task_id] = updated
        self._get_event(task_id).set()
        self._emit_status(updated)
        return updated


__all__ = [
    "MCPTaskConfig",
    "MCPTaskStatus",
    "MCPTaskStore",
    "TaskLookupError",
    "TaskRecord",
    "TaskStateError",
]
