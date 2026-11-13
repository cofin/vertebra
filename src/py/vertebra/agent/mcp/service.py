"""Full Model Context Protocol (MCP) 2026-07-28 JSON-RPC and HTTP core engine.

This module provides MCPCore implementing transport-agnostic
MCP 2026-07-28 HTTP handling, server/discover, tools/list, tools/call, resources/*,
prompts/*, skills/*, tasks/*, and completions with OpenTelemetry observability.
"""

import asyncio
import dataclasses
import inspect
import logging
from collections.abc import Callable, Mapping
from typing import Any, NoReturn

import msgspec

from vertebra.agent.core.context import AgentContext, bind_agent_context, reset_agent_context
from vertebra.agent.core.cursor import paginate_sequence
from vertebra.agent.core.guards import BudgetExceededError, PermissionDeniedError, RateLimitExceededError
from vertebra.agent.core.jsonrpc import (
    INVALID_PARAMS,
    INVALID_REQUEST,
    JSONRPCError,
    JSONRPCErrorException,
    JSONRPCRouter,
)
from vertebra.agent.core.schema import ToolArgumentError
from vertebra.agent.core.sse import SseFrame, SseStreamBroker
from vertebra.agent.mcp.client import MCP_NAME_FIELDS, decode_mcp_header_value
from vertebra.agent.mcp.models import (
    HEADER_MISMATCH,
    MCP_PROTOCOL_VERSION,
    UNSUPPORTED_PROTOCOL_VERSION,
    BlobContent,
    MCPConfig,
    McpHttpRequest,
    McpHttpResponse,
    PromptMessage,
    ResourceLinkContent,
    TextContent,
    ToolCallOutput,
)
from vertebra.agent.mcp.registry import ToolRegistry
from vertebra.agent.mcp.tasks import MCPTaskStore, TaskLookupError, TaskRecord, TaskStateError
from vertebra.agent.runtime import ConfirmationGate
from vertebra.agent.telemetry import AgentTelemetryRecorder
from vertebra.utils.serializers import decode_json, encode_json_bytes

_native_classify_jsonrpc_frame: Callable[[bytes], tuple[int, str | None, str | None]] | None = None
try:
    from vertebra._vertebra import classify_jsonrpc_frame as _native_classify_jsonrpc_frame
except ImportError:
    _native_classify_jsonrpc_frame = None

logger = logging.getLogger(__name__)

__all__ = [
    "CACHEABLE_METHODS",
    "CONFIRMATION_REQUIRED",
    "CONFIRMATION_TOKEN_META_KEY",
    "MCPCore",
    "PERMISSION_DENIED",
    "RATE_LIMITED",
]

CONFIRMATION_REQUIRED: int = -32003
PERMISSION_DENIED: int = -32004
RATE_LIMITED: int = -32005
CONFIRMATION_TOKEN_META_KEY: str = "vertebra/confirmationToken"
CACHEABLE_METHODS: frozenset[str] = frozenset(
    {
        "server/discover",
        "tools/list",
        "resources/list",
        "resources/templates/list",
        "prompts/list",
        "skills/list",
    }
)


class MCPCore:
    """Transport-agnostic MCP 2026-07-28 HTTP and JSON-RPC protocol engine."""

    def __init__(
        self,
        config: MCPConfig | None = None,
        registry: ToolRegistry | None = None,
        telemetry: AgentTelemetryRecorder | None = None,
        stream_broker: SseStreamBroker | None = None,
        task_store: MCPTaskStore | None = None,
        skill_catalog: Any | None = None,
        confirmation_gate: ConfirmationGate | None = None,
    ) -> None:
        self.config = config if config is not None else MCPConfig()
        self.confirmation_gate = confirmation_gate if confirmation_gate is not None else ConfirmationGate()
        self.registry = registry if registry is not None else ToolRegistry(confirmation_gate=self.confirmation_gate)
        self.telemetry = telemetry if telemetry is not None else AgentTelemetryRecorder()
        self.stream_broker = stream_broker if stream_broker is not None else SseStreamBroker()
        self.task_store = task_store if task_store is not None else MCPTaskStore(stream_broker=self.stream_broker)
        self.skill_catalog = skill_catalog
        self.router = JSONRPCRouter()
        self._resource_subscriptions: dict[str, set[str]] = {}
        self._task_runs: dict[str, tuple[asyncio.Task[None], AgentContext]] = {}

        self._register_handlers()

    async def handle(self, request: McpHttpRequest) -> McpHttpResponse:
        """Process an HTTP request under its caller context and restore the prior binding."""
        context = request.context if isinstance(request.context, AgentContext) else AgentContext()
        token = bind_agent_context(context)
        try:
            return await self._handle_http(request, context)
        finally:
            reset_agent_context(token)

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

    async def handle_payload(self, raw_bytes: bytes, context: AgentContext | None = None) -> bytes | None:
        """Dispatch JSON-RPC under its caller context and restore the prior binding."""
        context = context if isinstance(context, AgentContext) else AgentContext()
        token = bind_agent_context(context)
        try:
            return await self.router.dispatch_payload(raw_bytes, context)
        finally:
            reset_agent_context(token)

    async def _handle_http(self, request: McpHttpRequest, context: AgentContext) -> McpHttpResponse:
        """Validate HTTP metadata and dispatch with the bound caller context."""
        lower_headers = {k.lower(): v for k, v in request.headers.items()}
        origin = lower_headers.get("origin")
        host = lower_headers.get("host")

        if not self._is_origin_allowed(origin, host):
            return McpHttpResponse(
                status=403,
                headers={"Content-Type": "text/plain"},
                body=b"Forbidden: Disallowed Origin",
            )

        method = request.method.upper()
        if method == "OPTIONS":
            cors_headers = {
                "Access-Control-Allow-Origin": origin or "*",
                "Access-Control-Allow-Methods": "POST, OPTIONS",
                "Access-Control-Allow-Headers": (
                    "Content-Type, MCP-Protocol-Version, Mcp-Method, Mcp-Name, "
                    "Mcp-Session-Id, If-None-Match, Authorization"
                ),
            }
            return McpHttpResponse(status=204, headers=cors_headers, body=b"")

        if method != "POST":
            return McpHttpResponse(
                status=405,
                headers={"Content-Type": "text/plain", "Allow": "POST, OPTIONS"},
                body=b"Method Not Allowed",
            )

        proto_ver = lower_headers.get("mcp-protocol-version")
        if proto_ver is not None and proto_ver != MCP_PROTOCOL_VERSION:
            return self._jsonrpc_http_error(
                status=400,
                code=UNSUPPORTED_PROTOCOL_VERSION,
                message=f"Unsupported MCP protocol version: {proto_ver}",
            )

        try:
            frame_kind, frame_method, frame_id = _classify_jsonrpc_frame(request.body)
        except Exception as exc:
            return self._jsonrpc_http_error(
                status=400,
                code=-32700,
                message=f"Parse error: {exc}",
            )

        mcp_method_header = lower_headers.get("mcp-method")
        mcp_name_header = lower_headers.get("mcp-name")
        has_mcp_param_header = any(key.startswith("mcp-param-") for key in lower_headers)
        if frame_kind == 2:
            if mcp_method_header is not None or mcp_name_header is not None or has_mcp_param_header:
                return self._jsonrpc_http_error(
                    status=400,
                    code=HEADER_MISMATCH,
                    message="Mcp-Method, Mcp-Name, and Mcp-Param-* headers cannot be used with JSON-RPC batch requests",
                )
            return self._jsonrpc_http_error(
                status=400,
                code=INVALID_REQUEST,
                message="JSON-RPC batch arrays are not supported in MCP 2026-07-28",
            )

        if mcp_method_header is not None and frame_method is not None and mcp_method_header != frame_method:
            return self._jsonrpc_http_error(
                status=400,
                code=HEADER_MISMATCH,
                message=f"Header Mcp-Method '{mcp_method_header}' does not match body method '{frame_method}'",
                req_id=frame_id,
            )

        try:
            body_dict = decode_json(request.body) if request.body else {}
        except Exception as exc:
            return self._jsonrpc_http_error(
                status=400,
                code=-32700,
                message=f"Parse error: {exc}",
                req_id=frame_id,
            )
        raw_params = body_dict.get("params") if isinstance(body_dict, dict) else None
        params = raw_params if isinstance(raw_params, dict) else {}
        raw_meta = params.get("_meta")
        if isinstance(raw_meta, Mapping):
            meta_version = raw_meta.get("io.modelcontextprotocol/protocolVersion")
            if meta_version is not None and meta_version != MCP_PROTOCOL_VERSION:
                return self._jsonrpc_http_error(
                    status=400,
                    code=UNSUPPORTED_PROTOCOL_VERSION,
                    message=f"Unsupported MCP _meta protocol version: {meta_version}",
                    req_id=frame_id,
                )

        if mcp_name_header is not None and frame_method in MCP_NAME_FIELDS:
            decoded_name = decode_mcp_header_value(mcp_name_header)
            field_name = MCP_NAME_FIELDS[frame_method]
            target_val = params.get(field_name)
            if target_val is None and field_name == "taskId":
                target_val = params.get("task_id")
            if target_val is None or str(target_val) != decoded_name:
                return self._jsonrpc_http_error(
                    status=400,
                    code=HEADER_MISMATCH,
                    message=f"Header Mcp-Name '{decoded_name}' does not match body parameter {field_name!r}",
                    req_id=frame_id,
                )

        param_mismatch = self._validate_mcp_param_headers(
            request.headers,
            frame_method=frame_method,
            params=params,
            req_id=frame_id,
        )
        if param_mismatch is not None:
            return param_mismatch

        current_etag = self.registry.etag()
        if frame_method in CACHEABLE_METHODS and self._match_etag(lower_headers.get("if-none-match"), current_etag):
            return McpHttpResponse(
                status=304,
                headers={"ETag": current_etag, "MCP-Protocol-Version": MCP_PROTOCOL_VERSION},
                body=b"",
            )

        resp_bytes = await self.router.dispatch_payload(request.body, context)
        resp_headers: dict[str, str] = {
            "Content-Type": "application/json",
            "MCP-Protocol-Version": MCP_PROTOCOL_VERSION,
        }
        if frame_method in CACHEABLE_METHODS:
            resp_headers["ETag"] = current_etag

        session_id = lower_headers.get("mcp-session-id")
        if session_id:
            resp_headers["Mcp-Session-Id"] = session_id

        if frame_kind == 0 or resp_bytes is None:
            return McpHttpResponse(status=202, headers=resp_headers, body=b"")

        return McpHttpResponse(status=200, headers=resp_headers, body=resp_bytes)

    def _validate_mcp_param_headers(
        self,
        headers: Mapping[str, str],
        *,
        frame_method: str | None,
        params: Mapping[str, Any],
        req_id: Any = None,
    ) -> McpHttpResponse | None:
        """Validate every Mcp-Param-<name> header against decoded JSON-RPC body parameters."""
        param_headers = [
            (raw_key, raw_val) for raw_key, raw_val in headers.items() if raw_key.lower().startswith("mcp-param-")
        ]
        if not param_headers:
            return None

        if frame_method in ("tools/call", "prompts/get"):
            raw_args = params.get("arguments")
            target_params: Mapping[str, Any] = raw_args if isinstance(raw_args, Mapping) else {}
        else:
            target_params = {k: v for k, v in params.items() if k != "_meta"}

        for raw_key, raw_val in param_headers:
            param_slug = raw_key[len("mcp-param-") :]
            if not param_slug:
                return self._jsonrpc_http_error(
                    status=400,
                    code=HEADER_MISMATCH,
                    message="Empty parameter name in Mcp-Param-* header",
                    req_id=req_id,
                )
            decoded_expected = decode_mcp_header_value(raw_val)
            norm_slug = param_slug.lower().replace("-", "_")
            matched_values = [
                val
                for key, val in target_params.items()
                if isinstance(key, str) and key.lower().replace("-", "_") == norm_slug
            ]
            if not matched_values or not all(
                self._mcp_param_value_matches(val, decoded_expected) for val in matched_values
            ):
                return self._jsonrpc_http_error(
                    status=400,
                    code=HEADER_MISMATCH,
                    message=f"Header {raw_key} '{decoded_expected}' does not match body parameter {param_slug!r}",
                    req_id=req_id,
                )
        return None

    @staticmethod
    def _mcp_param_value_matches(actual_val: Any, decoded_expected: str) -> bool:
        if actual_val is None:
            return False
        if isinstance(actual_val, str):
            return actual_val == decoded_expected
        if isinstance(actual_val, bool):
            return decoded_expected.lower() in ("true", "false") and (
                ("true" if actual_val else "false") == decoded_expected.lower()
            )
        if isinstance(actual_val, (int, float)):
            return str(actual_val) == decoded_expected
        if isinstance(actual_val, (Mapping, list)):
            try:
                return bool(decode_json(decoded_expected.encode("utf-8")) == actual_val)
            except Exception:
                return False
        return str(actual_val) == decoded_expected

    @staticmethod
    def _raise_guard_error(
        tool_name: str,
        exc: PermissionDeniedError | RateLimitExceededError | BudgetExceededError,
    ) -> NoReturn:
        if isinstance(exc, PermissionDeniedError):
            reason = str(exc) or "Permission denied"
            raise JSONRPCErrorException(
                JSONRPCError(
                    code=PERMISSION_DENIED,
                    message=reason,
                    data={"tool": tool_name, "reason": reason},
                )
            ) from exc
        reason = str(exc) or "Rate limit exceeded"
        raise JSONRPCErrorException(
            JSONRPCError(
                code=RATE_LIMITED,
                message=reason,
                data={"tool": tool_name, "reason": reason},
            )
        ) from exc

    def _is_origin_allowed(self, origin: str | None, host: str | None) -> bool:
        if not origin:
            return True
        allowed = self.config.allowed_origins
        if "*" in allowed or origin in allowed:
            return True
        return bool(host and (origin == f"http://{host}" or origin == f"https://{host}"))

    @staticmethod
    def _match_etag(if_none_match: str | None, current_etag: str) -> bool:
        if not if_none_match:
            return False
        tokens = [t.strip() for t in if_none_match.split(",")]
        normalized_current = current_etag.strip('"').removeprefix("W/").removeprefix("w/").strip('"')
        for token in tokens:
            if token == "*":
                return True
            raw = token.strip('"').removeprefix("W/").removeprefix("w/").strip('"')
            if raw == normalized_current:
                return True
        return False

    @staticmethod
    def _jsonrpc_http_error(
        *,
        status: int,
        code: int,
        message: str,
        req_id: Any = None,
    ) -> McpHttpResponse:
        err = JSONRPCError(code=code, message=message)
        err_body = encode_json_bytes(
            {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": err.code, "message": err.message},
            }
        )
        return McpHttpResponse(status=status, headers={"Content-Type": "application/json"}, body=err_body)

    @staticmethod
    def _with_meta(method: str, payload: dict[str, Any], *, cacheable: bool = False) -> dict[str, Any]:
        meta: dict[str, Any] = {
            "io.modelcontextprotocol/protocolVersion": MCP_PROTOCOL_VERSION,
            "resultType": method,
        }
        if cacheable:
            meta["ttlMs"] = 60000
            meta["cacheScope"] = "public"
        payload["_meta"] = meta
        return payload

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
        self.router.register("notifications/initialized", self._handle_initialized_notif)
        self.router.register("server/discover", self._handle_discover)
        self.router.register("tools/list", self._handle_tools_list)
        self.router.register("tools/call", self._handle_tools_call)
        self.router.register("resources/list", self._handle_resources_list)
        self.router.register("resources/read", self._handle_resources_read)
        self.router.register("resources/templates/list", self._handle_resources_templates_list)
        self.router.register("prompts/list", self._handle_prompts_list)
        self.router.register("prompts/get", self._handle_prompts_get)
        self.router.register("skills/list", self._handle_skills_list)
        self.router.register("skills/get", self._handle_skills_get)
        self.router.register("resources/directory/read", self._handle_resources_directory_read)
        self.router.register("tasks/get", self._handle_tasks_get)
        self.router.register("tasks/update", self._handle_tasks_update)
        self.router.register("tasks/cancel", self._handle_tasks_cancel)
        self.router.register("tasks/list", self._handle_tasks_list)
        self.router.register("completion/complete", self._handle_completion)

    async def _handle_initialized_notif(self, params: Any = None, context: Any = None) -> None:
        """Process notifications/initialized confirmation."""
        return None

    async def _handle_discover(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Return server capability manifest and registered count summaries."""
        tools_list = self.registry.list_specs(exposure="mcp")
        resources_list = self.registry.list_resources()
        prompts_list = self.registry.list_prompts()
        skills_list = self.skill_catalog.list_skills() if self.skill_catalog is not None else ()
        return self._with_meta(
            "server/discover",
            {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "serverInfo": {"name": self.config.server_name, "version": self.config.server_version},
                "instructions": self.config.instructions,
                "capabilities": {
                    "tools": {"listChanged": True} if self.config.tools_enabled else {},
                    "resources": {"listChanged": True} if self.config.resources_enabled else {},
                    "prompts": {"listChanged": True} if self.config.prompts_enabled else {},
                    "tasks": {"listChanged": True} if self.config.tasks_enabled else {},
                    "skills": {"listChanged": True} if self.config.skills_enabled else {},
                },
                "counts": {
                    "tools": len(tools_list),
                    "resources": len(resources_list),
                    "prompts": len(prompts_list),
                    "skills": len(skills_list),
                },
            },
            cacheable=True,
        )

    async def _handle_tools_list(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """List registered tools with JSON-RPC cursor pagination."""
        cursor = params.get("cursor") if isinstance(params, Mapping) else None
        specs = self.registry.list_specs(exposure="mcp")
        page = paginate_sequence(specs, cursor=cursor, page_size=50)

        tool_dicts: list[dict[str, Any]] = []
        for s in page.items:
            item: dict[str, Any] = {
                "name": s.name,
                "title": s.title,
                "description": s.description,
                "inputSchema": s.input_schema,
            }
            if s.output_schema is not None:
                item["outputSchema"] = s.output_schema
            annotations_dict = s.annotations.to_mcp_dict()
            if annotations_dict:
                item["annotations"] = annotations_dict
            tool_dicts.append(item)

        res: dict[str, Any] = {"tools": tool_dicts}
        if page.next_cursor is not None:
            res["nextCursor"] = page.next_cursor
        return self._with_meta("tools/list", res, cacheable=True)

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
        spec = self.registry.get_spec(tool_name)
        if target_tool is None or (spec is not None and "mcp" not in spec.exposure):
            msg = f"Unknown tool: {tool_name}"
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message=msg))

        try:
            await target_tool.authorize(context)
        except (PermissionDeniedError, RateLimitExceededError, BudgetExceededError) as exc:
            self._raise_guard_error(tool_name, exc)

        meta = params.get("_meta", {})
        call_id = str(params.get("callId", "call_0"))
        if target_tool.requires_confirmation:
            self._require_confirmation(tool_name, call_id, arguments, meta, context)

        if isinstance(meta, dict) and meta.get("io.modelcontextprotocol/task") and self.task_store is not None:
            owner_id = context.identity.owner_key() if context and getattr(context, "identity", None) else None
            task = self.task_store.create(tool_name=tool_name, owner_id=owner_id)

            task_context = (context if isinstance(context, AgentContext) else AgentContext()).fork()

            async def _run_async_tool() -> None:
                token = bind_agent_context(task_context)
                try:
                    res = await target_tool.call(arguments, context=task_context)
                    formatted = self._format_tool_output(res, is_error=False)
                    self.task_store.complete(task.task_id, formatted)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.task_store.fail(task.task_id, {"message": str(exc)})
                finally:
                    reset_agent_context(token)

            bg_task = asyncio.create_task(_run_async_tool())
            self._task_runs[task.task_id] = (bg_task, task_context)
            bg_task.add_done_callback(lambda _run: self._task_runs.pop(task.task_id, None))
            return self._with_meta(
                "tools/call",
                {"task": {"taskId": task.task_id, "status": task.status, "pollInterval": task.poll_interval_ms}},
            )

        with self.telemetry.span_tool(tool_name, call_id):
            try:
                raw_result = await target_tool.call(arguments, context=context)
                return self._with_meta("tools/call", self._format_tool_output(raw_result, is_error=False))
            except ToolArgumentError as exc:
                msg = f"Invalid arguments for tool '{tool_name}': {exc.message}"
                raise JSONRPCErrorException(
                    JSONRPCError(code=INVALID_PARAMS, message=msg, data={"pointer": exc.pointer})
                ) from exc
            except (PermissionDeniedError, RateLimitExceededError, BudgetExceededError) as exc:
                self._raise_guard_error(tool_name, exc)
            except Exception as exc:
                logger.exception("Tool '%s' execution raised exception", tool_name)
                return self._with_meta(
                    "tools/call",
                    {
                        "content": [{"type": "text", "text": f"Error: {exc}"}],
                        "isError": True,
                    },
                )

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
        return self._with_meta("resources/list", res, cacheable=True)

    async def _handle_resources_read(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Read content of a static resource or matched template or skill."""
        if not isinstance(params, Mapping) or "uri" not in params:
            msg = "Missing required 'uri' in resources/read parameters"
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message=msg))

        uri = str(params["uri"])

        if uri.startswith("skill://") and self.skill_catalog is not None:
            try:
                file_bytes = await self.skill_catalog.read_file(uri)
                return self._with_meta(
                    "resources/read",
                    {
                        "contents": [
                            {
                                "uri": uri,
                                "mimeType": "text/markdown",
                                "text": file_bytes.decode("utf-8", errors="replace"),
                            }
                        ]
                    },
                )
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
                return self._with_meta(
                    "resources/read",
                    {
                        "contents": [
                            {
                                "uri": res_entry.uri,
                                "mimeType": res_entry.mimeType,
                                "text": content_text,
                            }
                        ]
                    },
                )

            matched = self.registry.match_resource_template(uri)
            if matched is not None:
                tmpl, variables = matched
                content_text = ""
                if tmpl.reader is not None:
                    res_val = tmpl.reader(**variables)
                    if inspect.isawaitable(res_val):
                        res_val = await res_val
                    content_text = res_val if isinstance(res_val, str) else str(res_val)
                return self._with_meta(
                    "resources/read",
                    {
                        "contents": [
                            {
                                "uri": uri,
                                "mimeType": tmpl.mimeType,
                                "text": content_text,
                            }
                        ]
                    },
                )

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
        return self._with_meta("resources/templates/list", res, cacheable=True)

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
        return self._with_meta("prompts/list", res, cacheable=True)

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

            return self._with_meta(
                "prompts/get",
                {
                    "description": prompt_entry.description,
                    "messages": messages,
                },
            )

    async def _handle_skills_list(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """List discovered skills from the configured skill catalog."""
        if self.skill_catalog is None:
            return self._with_meta("skills/list", {"skills": []}, cacheable=True)
        skills = self.skill_catalog.list_skills()
        return self._with_meta(
            "skills/list",
            {
                "skills": [
                    {
                        "name": s.name,
                        "description": s.description,
                        "files": sorted(s.files.keys()),
                        "frontmatter": s.frontmatter,
                    }
                    for s in skills
                ]
            },
            cacheable=True,
        )

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
        return self._with_meta(
            "skills/get",
            {
                "skill": {
                    "name": skill.name,
                    "description": skill.description,
                    "body": skill.body,
                    "files": files_summary,
                    "frontmatter": skill.frontmatter,
                }
            },
        )

    async def _handle_resources_directory_read(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Read directory listing for a skill package URI."""
        if not isinstance(params, Mapping) or "uri" not in params:
            msg = "Missing required 'uri' in resources/directory/read parameters"
            raise JSONRPCErrorException(JSONRPCError(code=INVALID_PARAMS, message=msg))
        if self.skill_catalog is None:
            return self._with_meta("resources/directory/read", {"files": []})
        uri = str(params["uri"])
        files = self.skill_catalog.read_directory(uri)
        return self._with_meta("resources/directory/read", {"files": files})

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
        """Raise unless the call carries a ticket approved for this tool, caller, and arguments."""
        owner_id = self._caller_owner(context)
        token = meta.get(CONFIRMATION_TOKEN_META_KEY) if isinstance(meta, Mapping) else None
        if self.confirmation_gate.consume_call_approval(
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
            return self._with_meta("tasks/get", {"task": self._task_to_dict(task)})
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
            return self._with_meta("tasks/update", {"task": self._task_to_dict(task)})
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
            if task.status == "cancelled" and str(task_id) in self._task_runs:
                run, task_context = self._task_runs[str(task_id)]
                task_context.cancellation_token.cancel(reason or "Task cancelled by client")
                run.cancel()
            return self._with_meta("tasks/cancel", {"task": self._task_to_dict(task)})
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
        return self._with_meta("tasks/list", res)

    async def _handle_completion(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Handle argument completion request."""
        return self._with_meta("completion/complete", {"completion": {"values": [], "total": 0, "hasMore": False}})


def _classify_jsonrpc_frame(payload: bytes) -> tuple[int, str | None, str | None]:
    if _native_classify_jsonrpc_frame is not None:
        return _native_classify_jsonrpc_frame(payload)
    data = decode_json(payload)
    if isinstance(data, list):
        return 2, None, None
    if not isinstance(data, dict):
        raise ValueError("Expected object or list")
    method = data.get("method")
    req_id = data.get("id")
    id_str = str(req_id) if req_id is not None else None
    if method is not None:
        if req_id is not None:
            return 1, method, id_str
        return 0, method, None
    return 3, None, id_str
