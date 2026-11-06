"""Full Model Context Protocol (MCP) 2026-07-28 JSON-RPC service.

This module provides the complete MCPService implementing initialize, tools/list,
tools/call, resources/*, prompts/*, skills/*, tasks/*, completions, and logging with OpenTelemetry observability.
"""

import asyncio
import dataclasses
import inspect
import logging
from collections.abc import Mapping
from typing import Any

import msgspec

from vertebra.agent.core.cursor import paginate_sequence
from vertebra.agent.core.jsonrpc import (
    INVALID_PARAMS,
    JSONRPCError,
    JSONRPCErrorException,
    JSONRPCRouter,
)
from vertebra.agent.core.schema import ToolArgumentError
from vertebra.agent.core.sse import SseFrame, SseStreamBroker
from vertebra.agent.mcp.models import (
    MCP_PROTOCOL_VERSION,
    BlobContent,
    MCPConfig,
    PromptMessage,
    ResourceLinkContent,
    TextContent,
    ToolCallOutput,
)
from vertebra.agent.mcp.registry import MCPRegistry
from vertebra.agent.mcp.tasks import MCPTaskStore, TaskLookupError, TaskRecord, TaskStateError
from vertebra.agent.runtime import ConfirmationGate
from vertebra.agent.telemetry import AgentTelemetryRecorder
from vertebra.utils.serializers import decode_json, encode_json_bytes

logger = logging.getLogger(__name__)

__all__ = [
    "CONFIRMATION_REQUIRED",
    "CONFIRMATION_TOKEN_META_KEY",
    "MCPService",
]

CONFIRMATION_REQUIRED: int = -32003
CONFIRMATION_TOKEN_META_KEY: str = "vertebra/confirmationToken"


class MCPService:
    """Dispatches and coordinates all MCP 2026-07-28 protocol methods."""

    def __init__(
        self,
        config: MCPConfig | None = None,
        registry: MCPRegistry | None = None,
        telemetry: AgentTelemetryRecorder | None = None,
        stream_broker: SseStreamBroker | None = None,
        task_store: MCPTaskStore | None = None,
        skill_catalog: Any | None = None,
        confirmation_gate: ConfirmationGate | None = None,
    ) -> None:
        self.config = config if config is not None else MCPConfig()
        self.registry = registry if registry is not None else MCPRegistry()
        self.telemetry = telemetry if telemetry is not None else AgentTelemetryRecorder()
        self.stream_broker = stream_broker if stream_broker is not None else SseStreamBroker()
        self.task_store = task_store if task_store is not None else MCPTaskStore(stream_broker=self.stream_broker)
        self.skill_catalog = skill_catalog
        self.confirmation_gate = confirmation_gate if confirmation_gate is not None else ConfirmationGate()
        self.router = JSONRPCRouter()
        self._resource_subscriptions: dict[str, set[str]] = {}

        self._register_handlers()

    async def handle_tool_call(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Execute a tool invocation with parameter validation and structured outputs."""
        return await self._handle_tools_call(params, context=context)

    def notify_resource_updated(self, uri: str) -> int:
        """Push a notifications/resources/updated SSE frame to all subscribed sessions."""
        sessions = self._resource_subscriptions.get(uri, set())
        if not sessions:
            return 0

        payload = {"jsonrpc": "2.0", "method": "notifications/resources/updated", "params": {"uri": uri}}
        frame = SseFrame(event="message", data=encode_json_bytes(payload).decode("utf-8"))
        return self.stream_broker.broadcast(frame, list(sessions))

    async def handle_payload(self, raw_bytes: bytes, context: Any = None) -> bytes | None:
        """Dispatch a raw JSON-RPC payload byte sequence through the router."""
        return await self.router.dispatch_payload(raw_bytes, context)

    @staticmethod
    def _task_to_dict(task: TaskRecord) -> dict[str, Any]:
        """Convert a TaskRecord into an MCP-compliant JSON dictionary."""
        data: dict[str, Any] = {
            "taskId": task.task_id,
            "status": task.status,
            "createdAt": task.created_at_iso,
            "lastUpdatedAt": task.last_updated_at_iso,
            "ttl": task.ttl_ms,
            "pollInterval": task.poll_interval_ms,
            "isTerminal": task.is_terminal,
        }
        if task.status_message is not None:
            data["statusMessage"] = task.status_message
        if task.input_requests is not None:
            data["inputRequests"] = task.input_requests
        if task.result is not None:
            data["result"] = task.result
        if task.error is not None:
            data["error"] = task.error
        return data

    def _register_handlers(self) -> None:
        """Register all MCP 2026-07-28 protocol handlers on the JSON-RPC router."""
        self.router.register("initialize", self._handle_initialize)
        self.router.register("notifications/initialized", self._handle_initialized_notif)
        self.router.register("ping", self._handle_ping)
        self.router.register("server/discover", self._handle_discover)
        self.router.register("tools/list", self._handle_tools_list)
        self.router.register("tools/call", self._handle_tools_call)
        self.router.register("resources/list", self._handle_resources_list)
        self.router.register("resources/read", self._handle_resources_read)
        self.router.register("resources/templates/list", self._handle_resources_templates_list)
        self.router.register("resources/subscribe", self._handle_resources_subscribe)
        self.router.register("resources/unsubscribe", self._handle_resources_unsubscribe)
        self.router.register("prompts/list", self._handle_prompts_list)
        self.router.register("prompts/get", self._handle_prompts_get)
        self.router.register("skills/list", self._handle_skills_list)
        self.router.register("skills/get", self._handle_skills_get)
        self.router.register("resources/directory/read", self._handle_resources_directory_read)
        self.router.register("tasks/get", self._handle_tasks_get)
        self.router.register("tasks/result", self._handle_tasks_result)
        self.router.register("tasks/update", self._handle_tasks_update)
        self.router.register("tasks/cancel", self._handle_tasks_cancel)
        self.router.register("tasks/list", self._handle_tasks_list)
        self.router.register("completion/complete", self._handle_completion)
        self.router.register("logging/setLevel", self._handle_set_level)

    async def _handle_initialize(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Handle initialize handshake advertising protocol version and capabilities."""
        return {
            "protocolVersion": MCP_PROTOCOL_VERSION,
            "capabilities": {
                "tools": {"listChanged": True},
                "resources": {"subscribe": True, "listChanged": True},
                "prompts": {"listChanged": True},
                "logging": {},
                "tasks": {"listChanged": True},
                "skills": {"listChanged": True},
            },
            "serverInfo": {
                "name": self.config.server_name,
                "version": self.config.server_version,
            },
            "instructions": self.config.instructions,
        }

    async def _handle_initialized_notif(self, params: Any = None, context: Any = None) -> None:
        """Process notifications/initialized confirmation."""
        return None

    async def _handle_ping(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Return empty pong response."""
        return {}

    async def _handle_discover(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Return server capability manifest and registered count summaries."""
        tools_list = self.registry.list_tools()
        resources_list = self.registry.list_resources()
        prompts_list = self.registry.list_prompts()
        return {
            "protocolVersion": MCP_PROTOCOL_VERSION,
            "serverInfo": {"name": self.config.server_name, "version": self.config.server_version},
            "counts": {
                "tools": len(tools_list),
                "resources": len(resources_list),
                "prompts": len(prompts_list),
            },
        }

    async def _handle_tools_list(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """List registered tools with JSON-RPC cursor pagination."""
        cursor = params.get("cursor") if isinstance(params, Mapping) else None
        tools = self.registry.list_tools()
        page = paginate_sequence(tools, cursor=cursor, page_size=50)

        tool_dicts: list[dict[str, Any]] = []
        for t in page.items:
            tool_dicts.append(
                {
                    "name": t.name,
                    "description": t.description,
                    "inputSchema": t.input_schema,
                }
            )

        res: dict[str, Any] = {"tools": tool_dicts}
        if page.next_cursor is not None:
            res["nextCursor"] = page.next_cursor
        return res

    async def _handle_tools_call(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Execute a tool invocation with parameter validation and structured outputs."""
        if not isinstance(params, Mapping) or "name" not in params:
            msg = "Missing required 'name' in tools/call parameters"
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message=msg))

        tool_name = str(params["name"])
        arguments = params.get("arguments", {})
        if not isinstance(arguments, dict):
            arguments = {}

        target_tool = self.registry.get_tool(tool_name)
        if target_tool is None:
            msg = f"Unknown tool: {tool_name}"
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message=msg))

        meta = params.get("_meta", {})
        call_id = str(params.get("callId", "call_0"))
        if target_tool.requires_confirmation:
            self._require_confirmation(tool_name, call_id, arguments, meta, context)

        if isinstance(meta, dict) and meta.get("io.modelcontextprotocol/task") and self.task_store is not None:
            owner_id = context.identity.owner_key() if context and getattr(context, "identity", None) else None
            task = self.task_store.create(tool_name=tool_name, owner_id=owner_id)

            async def _run_async_tool() -> None:
                try:
                    res = await target_tool.invoke(arguments, context=context)
                    formatted = self._format_tool_output(res, is_error=False)
                    self.task_store.complete(task.task_id, formatted)
                except Exception as exc:
                    self.task_store.fail(task.task_id, {"message": str(exc)})

            asyncio.create_task(_run_async_tool())
            return {"task": {"taskId": task.task_id, "status": task.status, "pollInterval": task.poll_interval_ms}}

        with self.telemetry.span_tool(tool_name, call_id):
            try:
                raw_result = await target_tool.invoke(arguments, context=context)
                return self._format_tool_output(raw_result, is_error=False)
            except ToolArgumentError as exc:
                msg = f"Invalid arguments for tool '{tool_name}': {exc.message}"
                raise JSONRPCErrorException(
                    JSONRPCError(code=INVALID_PARAMS, message=msg, data={"pointer": exc.pointer})
                ) from exc
            except Exception as exc:
                logger.exception("Tool '%s' execution raised exception", tool_name)
                return {
                    "content": [{"type": "text", "text": f"Error: {exc}"}],
                    "isError": True,
                }

    def _format_tool_output(self, raw_result: Any, is_error: bool = False) -> dict[str, Any]:
        """Format arbitrary Python tool return values into MCP ToolCallOutput dictionaries."""
        content: list[dict[str, Any]] = []
        structured: dict[str, Any] | None = None

        if isinstance(raw_result, ToolCallOutput):
            for c in raw_result.content:
                if isinstance(c, TextContent):
                    content.append({"type": "text", "text": c.text})
                elif isinstance(c, BlobContent):
                    content.append({"type": c.type, "data": c.data, "mimeType": c.mimeType})
                elif isinstance(c, ResourceLinkContent):
                    content.append({"type": "resource_link", "uri": c.uri, "name": c.name})
            return {
                "content": content,
                "structuredContent": raw_result.structuredContent,
                "isError": raw_result.isError,
            }

        if isinstance(raw_result, (TextContent, BlobContent, ResourceLinkContent)):
            if isinstance(raw_result, TextContent):
                content.append({"type": "text", "text": raw_result.text})
            elif isinstance(raw_result, BlobContent):
                content.append({"type": raw_result.type, "data": raw_result.data, "mimeType": raw_result.mimeType})
            elif isinstance(raw_result, ResourceLinkContent):
                content.append({"type": "resource_link", "uri": raw_result.uri, "name": raw_result.name})
            return {"content": content, "isError": is_error}

        if isinstance(raw_result, msgspec.Struct):
            structured = decode_json(encode_json_bytes(raw_result))
            content.append({"type": "text", "text": encode_json_bytes(raw_result).decode("utf-8")})
        elif dataclasses.is_dataclass(raw_result) and not isinstance(raw_result, type):
            structured = dataclasses.asdict(raw_result)
            content.append({"type": "text", "text": encode_json_bytes(structured).decode("utf-8")})
        elif isinstance(raw_result, dict):
            structured = raw_result
            content.append({"type": "text", "text": encode_json_bytes(raw_result).decode("utf-8")})
        elif isinstance(raw_result, (str, int, float, bool)):
            content.append({"type": "text", "text": str(raw_result)})
        elif raw_result is None:
            content.append({"type": "text", "text": ""})
        else:
            content.append({"type": "text", "text": str(raw_result)})

        payload: dict[str, Any] = {"content": content, "isError": is_error}
        if structured is not None:
            payload["structuredContent"] = structured
        return payload

    async def _handle_resources_list(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """List registered static resources with cursor pagination."""
        cursor = params.get("cursor") if isinstance(params, Mapping) else None
        resources = self.registry.list_resources()
        page = paginate_sequence(resources, cursor=cursor, page_size=50)

        res_dicts: list[dict[str, Any]] = []
        for r in page.items:
            res_dicts.append(
                {
                    "uri": r.uri,
                    "name": r.name,
                    "description": r.description,
                    "mimeType": r.mimeType,
                }
            )

        res: dict[str, Any] = {"resources": res_dicts}
        if page.next_cursor is not None:
            res["nextCursor"] = page.next_cursor
        return res

    async def _handle_resources_read(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Read content of a static resource or matched template or skill."""
        if not isinstance(params, Mapping) or "uri" not in params:
            msg = "Missing required 'uri' in resources/read parameters"
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message=msg))

        uri = str(params["uri"])

        if uri.startswith("skill://") and self.skill_catalog is not None:
            try:
                file_bytes = await self.skill_catalog.read_file(uri)
                return {
                    "contents": [
                        {
                            "uri": uri,
                            "mimeType": "text/markdown",
                            "text": file_bytes.decode("utf-8", errors="replace"),
                        }
                    ]
                }
            except Exception as exc:
                msg = f"Failed to read skill resource '{uri}': {exc}"
                raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message=msg)) from exc

        with self.telemetry.span_resource(uri):
            res_entry = self.registry.get_resource(uri)
            if res_entry is not None:
                content_text = ""
                if res_entry.reader is not None:
                    res_val = res_entry.reader()
                    if inspect.isawaitable(res_val):
                        res_val = await res_val
                    content_text = res_val if isinstance(res_val, str) else str(res_val)
                return {
                    "contents": [
                        {
                            "uri": res_entry.uri,
                            "mimeType": res_entry.mimeType,
                            "text": content_text,
                        }
                    ]
                }

            matched = self.registry.match_resource_template(uri)
            if matched is not None:
                tmpl, variables = matched
                content_text = ""
                if tmpl.reader is not None:
                    res_val = tmpl.reader(**variables)
                    if inspect.isawaitable(res_val):
                        res_val = await res_val
                    content_text = res_val if isinstance(res_val, str) else str(res_val)
                return {
                    "contents": [
                        {
                            "uri": uri,
                            "mimeType": tmpl.mimeType,
                            "text": content_text,
                        }
                    ]
                }

        msg = f"Resource not found: {uri}"
        raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message=msg))

    async def _handle_resources_templates_list(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """List registered resource URI templates with cursor pagination."""
        cursor = params.get("cursor") if isinstance(params, Mapping) else None
        templates = self.registry.list_resource_templates()
        page = paginate_sequence(templates, cursor=cursor, page_size=50)

        tmpl_dicts: list[dict[str, Any]] = []
        for t in page.items:
            tmpl_dicts.append(
                {
                    "uriTemplate": t.uriTemplate,
                    "name": t.name,
                    "description": t.description,
                    "mimeType": t.mimeType,
                }
            )

        res: dict[str, Any] = {"resourceTemplates": tmpl_dicts}
        if page.next_cursor is not None:
            res["nextCursor"] = page.next_cursor
        return res

    async def _handle_resources_subscribe(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Subscribe caller session to resource change notifications."""
        if not isinstance(params, Mapping) or "uri" not in params:
            msg = "Missing required 'uri' in resources/subscribe parameters"
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message=msg))

        uri = str(params["uri"])
        session_id = getattr(context, "session_id", "default")
        self._resource_subscriptions.setdefault(uri, set()).add(session_id)
        return {}

    async def _handle_resources_unsubscribe(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Unsubscribe caller session from resource change notifications."""
        if not isinstance(params, Mapping) or "uri" not in params:
            msg = "Missing required 'uri' in resources/unsubscribe parameters"
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message=msg))

        uri = str(params["uri"])
        session_id = getattr(context, "session_id", "default")
        if uri in self._resource_subscriptions:
            self._resource_subscriptions[uri].discard(session_id)
        return {}

    async def _handle_prompts_list(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """List registered prompt templates with cursor pagination."""
        cursor = params.get("cursor") if isinstance(params, Mapping) else None
        prompts = self.registry.list_prompts()
        page = paginate_sequence(prompts, cursor=cursor, page_size=50)

        prompt_dicts: list[dict[str, Any]] = []
        for p in page.items:
            prompt_dicts.append(
                {
                    "name": p.name,
                    "description": p.description,
                    "arguments": [
                        {"name": a.name, "description": a.description, "required": a.required} for a in p.arguments
                    ],
                }
            )

        res: dict[str, Any] = {"prompts": prompt_dicts}
        if page.next_cursor is not None:
            res["nextCursor"] = page.next_cursor
        return res

    async def _handle_prompts_get(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Retrieve and instantiate an MCP prompt template."""
        if not isinstance(params, Mapping) or "name" not in params:
            msg = "Missing required 'name' in prompts/get parameters"
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message=msg))

        prompt_name = str(params["name"])
        arguments = params.get("arguments", {})
        if not isinstance(arguments, dict):
            arguments = {}

        with self.telemetry.span_prompt(prompt_name):
            prompt_entry = self.registry.get_prompt(prompt_name)
            if prompt_entry is None:
                msg = f"Prompt not found: {prompt_name}"
                raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message=msg))

            messages: list[dict[str, Any]] = []
            if prompt_entry.getter is not None:
                raw_messages = prompt_entry.getter(**arguments)
                if inspect.isawaitable(raw_messages):
                    raw_messages = await raw_messages
                for m in raw_messages:
                    if isinstance(m, PromptMessage):
                        content_dict = {"type": m.content.type, "text": getattr(m.content, "text", "")}
                        messages.append({"role": m.role, "content": content_dict})
                    elif isinstance(m, dict):
                        messages.append(m)

            return {
                "description": prompt_entry.description,
                "messages": messages,
            }

    async def _handle_skills_list(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """List discovered skills from the configured skill catalog."""
        if self.skill_catalog is None:
            return {"skills": []}
        skills = self.skill_catalog.list_skills()
        return {
            "skills": [
                {
                    "name": s.name,
                    "description": s.description,
                    "files": sorted(s.files.keys()),
                    "frontmatter": s.frontmatter,
                }
                for s in skills
            ]
        }

    async def _handle_skills_get(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Retrieve complete skill package details by name or uri."""
        if not isinstance(params, Mapping):
            msg = "Missing required parameters in skills/get"
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message=msg))
        skill_name = params.get("name") or params.get("uri")
        if not skill_name:
            msg = "Missing required 'name' or 'uri' in skills/get parameters"
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message=msg))
        if self.skill_catalog is None:
            msg = "Skill catalog not configured"
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message=msg))
        skill = self.skill_catalog.get_skill(str(skill_name))
        if skill is None:
            msg = f"Skill not found: {skill_name}"
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message=msg))
        files_summary = {
            k: {"digest": f.digest, "sizeBytes": f.size_bytes, "mimeType": f.mime_type} for k, f in skill.files.items()
        }
        return {
            "skill": {
                "name": skill.name,
                "description": skill.description,
                "body": skill.body,
                "files": files_summary,
                "frontmatter": skill.frontmatter,
            }
        }

    async def _handle_resources_directory_read(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Read directory listing for a skill package URI."""
        if not isinstance(params, Mapping) or "uri" not in params:
            msg = "Missing required 'uri' in resources/directory/read parameters"
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message=msg))
        if self.skill_catalog is None:
            return {"files": []}
        uri = str(params["uri"])
        files = self.skill_catalog.read_directory(uri)
        return {"files": files}

    def _caller_owner(self, context: Any) -> str | None:
        if context is not None and getattr(context, "identity", None) is not None:
            key = context.identity.owner_key()
            return str(key) if key is not None else None
        return None

    def _require_confirmation(
        self,
        tool_name: str,
        call_id: str,
        arguments: dict[str, Any],
        meta: Any,
        context: Any,
    ) -> None:
        """Raise unless the call carries a ticket approved for this tool, caller, and arguments.

        The token is read from ``_meta["vertebra/confirmationToken"]``. It must name an existing
        ticket with ``status == "approved"``, issued for ``tool_name`` with identical
        ``arguments``, and owned by the caller (or by no owner).

        Raises:
            JSONRPCErrorException: With code ``CONFIRMATION_REQUIRED`` and a newly issued ticket
                token in ``data`` when the token is missing, unknown, unapproved, owned by another
                caller, or issued for another tool or other arguments.
        """
        owner_id = self._caller_owner(context)
        token = meta.get(CONFIRMATION_TOKEN_META_KEY) if isinstance(meta, Mapping) else None
        if self.confirmation_gate.is_call_approved(
            token if isinstance(token, str) else None,
            tool_name=tool_name,
            arguments=arguments,
            owner_id=owner_id,
        ):
            return
        issued = self.confirmation_gate.request_confirmation(
            tool_name,
            call_id,
            dict(arguments),
            owner_id=owner_id,
            reason=f"Tool '{tool_name}' requires confirmation.",
        )
        raise JSONRPCErrorException(
            JSONRPCError(
                code=CONFIRMATION_REQUIRED,
                message=f"Tool '{tool_name}' requires confirmation",
                data={"confirmationToken": issued.token, "tool": tool_name},
            )
        )

    async def _handle_tasks_get(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Retrieve task execution record by task ID."""
        if not isinstance(params, Mapping):
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message="Invalid params"))
        task_id = params.get("taskId") or params.get("task_id")
        if not task_id:
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message="Missing 'taskId'"))
        try:
            task = self.task_store.get(str(task_id), self._caller_owner(context))
            return {"task": self._task_to_dict(task)}
        except TaskLookupError as exc:
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message=str(exc))) from exc

    async def _handle_tasks_result(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Wait for task result until completion or required input."""
        if not isinstance(params, Mapping):
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message="Invalid params"))
        task_id = params.get("taskId") or params.get("task_id")
        if not task_id:
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message="Missing 'taskId'"))
        timeout = float(params.get("timeoutSeconds", 30.0))
        try:
            task = await self.task_store.wait_for_result(str(task_id), self._caller_owner(context), timeout_s=timeout)
            return {"task": self._task_to_dict(task)}
        except TaskLookupError as exc:
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message=str(exc))) from exc

    async def _handle_tasks_update(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Submit required inputs to resume a waiting task."""
        if not isinstance(params, Mapping):
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message="Invalid params"))
        task_id = params.get("taskId") or params.get("task_id")
        inputs = params.get("inputs") or {}
        if not task_id:
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message="Missing 'taskId'"))
        try:
            task = self.task_store.submit_input(str(task_id), self._caller_owner(context), inputs)
            return {"task": self._task_to_dict(task)}
        except (TaskLookupError, TaskStateError) as exc:
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message=str(exc))) from exc

    async def _handle_tasks_cancel(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Cancel an in-flight task."""
        if not isinstance(params, Mapping):
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message="Invalid params"))
        task_id = params.get("taskId") or params.get("task_id")
        reason = params.get("reason")
        if not task_id:
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message="Missing 'taskId'"))
        try:
            task = self.task_store.cancel(str(task_id), self._caller_owner(context), reason)
            return {"task": self._task_to_dict(task)}
        except TaskLookupError as exc:
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message=str(exc))) from exc

    async def _handle_tasks_list(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """List tasks owned by caller."""
        cursor = params.get("cursor") if isinstance(params, Mapping) else None
        limit = int(params.get("limit", 50)) if isinstance(params, Mapping) else 50
        tasks, next_cursor = self.task_store.list_for_owner(self._caller_owner(context), cursor=cursor, limit=limit)
        res: dict[str, Any] = {"tasks": [self._task_to_dict(t) for t in tasks]}
        if next_cursor is not None:
            res["nextCursor"] = next_cursor
        return res

    async def _handle_completion(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Handle argument completion request."""
        return {"completion": {"values": [], "total": 0, "hasMore": False}}

    async def _handle_set_level(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Handle logging/setLevel configuration."""
        return {}
