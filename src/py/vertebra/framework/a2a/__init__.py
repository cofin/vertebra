"""Agent-to-Agent (A2A) protocol server, `/.well-known/agent-card.json`, and task lifecycle."""

from __future__ import annotations

import inspect
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = (
    "A2AServer",
    "A2ATask",
    "A2ATaskState",
    "AgentCard",
    "AgentSkill",
)

type A2ATaskState = Literal["submitted", "working", "input-required", "completed", "failed", "canceled"]


@dataclass(frozen=True, slots=True)
class AgentSkill:
    """Skill advertised in an A2A ``AgentCard``."""

    id: str
    name: str
    description: str
    tags: tuple[str, ...] = ()
    examples: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class AgentCard:
    """A2A discovery document served at ``/.well-known/agent-card.json``."""

    name: str
    description: str
    url: str
    version: str = "0.1.0"
    skills: tuple[AgentSkill, ...] = ()
    streaming: bool = True
    push_notifications: bool = False
    authentication_schemes: tuple[str, ...] = ("Bearer",)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the ``AgentCard`` to its canonical JSON dictionary."""
        return {
            "name": self.name,
            "description": self.description,
            "url": self.url,
            "version": self.version,
            "capabilities": {
                "streaming": self.streaming,
                "pushNotifications": self.push_notifications,
            },
            "authentication": {"schemes": list(self.authentication_schemes)},
            "skills": [
                {
                    "id": s.id,
                    "name": s.name,
                    "description": s.description,
                    "tags": list(s.tags),
                    "examples": list(s.examples),
                }
                for s in self.skills
            ],
        }


@dataclass(slots=True)
class A2ATask:
    """Stateful A2A task execution record."""

    id: str
    state: A2ATaskState = "submitted"
    messages: list[dict[str, Any]] = field(default_factory=list)
    artifacts: list[dict[str, Any]] = field(default_factory=list)


class A2AServer:
    """Agent-to-Agent (A2A) JSON-RPC 2.0 and discovery endpoint server."""

    def __init__(
        self,
        card: AgentCard,
        *,
        handler: Callable[[A2ATask, dict[str, Any]], Any] | None = None,
    ) -> None:
        self._card = card
        self._handler = handler
        self._tasks: dict[str, A2ATask] = {}

    @property
    def card(self) -> AgentCard:
        """Return the ``AgentCard``."""
        return self._card

    def agent_card_json(self) -> dict[str, Any]:
        """Return the ``/.well-known/agent-card.json`` payload."""
        return self._card.to_dict()

    async def handle_jsonrpc(self, request: dict[str, Any]) -> dict[str, Any]:
        """Handle A2A ``tasks/send``, ``tasks/get``, and ``tasks/cancel`` JSON-RPC calls."""
        req_id = request.get("id")
        method = request.get("method", "")
        params = request.get("params") or {}

        if method in ("tasks/send", "message/send"):
            task_id = params.get("id") or uuid.uuid4().hex
            task_obj = self._tasks.get(task_id) or A2ATask(id=task_id)
            msg = params.get("message")
            if isinstance(msg, dict):
                task_obj.messages.append(msg)
            task_obj.state = "working"
            if self._handler is not None:
                res = self._handler(task_obj, params)
                if inspect.isawaitable(res):
                    res = await res
                if isinstance(res, str):
                    task_obj.artifacts.append({"parts": [{"type": "text", "text": res}]})
            if task_obj.state == "working":
                task_obj.state = "completed"
            self._tasks[task_id] = task_obj
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "id": task_obj.id,
                    "status": {"state": task_obj.state},
                    "artifacts": list(task_obj.artifacts),
                },
            }
        if method == "tasks/get":
            task_id = params.get("id", "")
            if task_id not in self._tasks:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {"code": -32001, "message": f"Task not found: {task_id}"},
                }
            task_obj = self._tasks[task_id]
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "id": task_obj.id,
                    "status": {"state": task_obj.state},
                    "artifacts": list(task_obj.artifacts),
                },
            }
        if method == "tasks/cancel":
            task_id = params.get("id", "")
            if task_id in self._tasks:
                self._tasks[task_id].state = "canceled"
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"id": task_id, "status": {"state": "canceled"}},
            }
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32601, "message": f"Method not found: {method}"},
        }
