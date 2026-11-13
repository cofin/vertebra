"""Full Model Context Protocol (MCP) JSON-RPC and Streamable HTTP core engine.

This module provides MCPCore implementing transport-agnostic MCP 2026-07-28
and legacy Streamable HTTP handling, version negotiation, session lifecycle,
server/discover, tools/*, resources/*, prompts/*, skills/*, tasks/*, logging,
and completions with OpenTelemetry observability.
"""

import asyncio
import contextlib
import dataclasses
import inspect
import logging
import secrets
from collections.abc import AsyncIterator, Callable, Mapping
from typing import Any, NoReturn

import anyio
import msgspec

from vertebra.agent.core.context import (
    AgentContext,
    ProgressReporter,
    bind_agent_context,
    current_agent_context,
    reset_agent_context,
)
from vertebra.agent.core.cursor import paginate_sequence
from vertebra.agent.core.guards import BudgetExceededError, PermissionDeniedError, RateLimitExceededError
from vertebra.agent.core.jsonrpc import (
    INVALID_PARAMS,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
    PARSE_ERROR,
    JSONRPCError,
    JSONRPCErrorException,
    JSONRPCRequest,
    JSONRPCRouter,
)
from vertebra.agent.core.schema import ToolArgumentError
from vertebra.agent.core.sse import SseFrame, SseStreamBroker, StreamLimitExceededError
from vertebra.agent.mcp.client import MCP_NAME_FIELDS, decode_mcp_header_value
from vertebra.agent.mcp.models import (
    HEADER_MISMATCH,
    LEGACY_ONLY_METHODS,
    LEGACY_PROTOCOL_VERSIONS,
    MCP_FALLBACK_SESSION_VERSION,
    MCP_LOG_LEVELS,
    MCP_PROTOCOL_VERSION,
    SESSION_CAPACITY_EXHAUSTED,
    SUPPORTED_PROTOCOL_VERSIONS,
    UNSUPPORTED_PROTOCOL_VERSION,
    BlobContent,
    MCPConfig,
    McpHttpRequest,
    McpHttpResponse,
    McpLogLevel,
    NegotiatedProtocol,
    PromptMessage,
    ResourceLinkContent,
    TextContent,
    ToolCallOutput,
)
from vertebra.agent.mcp.registry import RegistryChangeKind, ToolRegistry
from vertebra.agent.mcp.sessions import MCPSession, MCPSessionStore, SessionLimitExceededError
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
MCP_LOG_LEVEL_INDEX: dict[McpLogLevel, int] = {lvl: idx for idx, lvl in enumerate(MCP_LOG_LEVELS)}
_REGISTRY_CHANGE_KINDS: tuple[RegistryChangeKind, ...] = ("tools", "resources", "prompts")


class _ListenRegistration(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Active 2026-07-28 stateless subscriptions/listen SSE registration."""

    subscription_id: str
    stream_id: str
    resource_uris: frozenset[str]
    list_changed_kinds: frozenset[RegistryChangeKind]
    owner_id: str | None = None


class _ListenStreamWrapper(AsyncIterator[bytes]):
    """Wrap an SSE stream to unregister subscriptions/listen state on close."""

    def __init__(
        self,
        core: "MCPCore",
        subscription_id: str,
        stream_id: str,
        inner: AsyncIterator[bytes],
    ) -> None:
        self._core = core
        self._subscription_id = subscription_id
        self._stream_id = stream_id
        self._inner = inner
        self._closed = False

    def __aiter__(self) -> "_ListenStreamWrapper":
        return self

    async def __anext__(self) -> bytes:
        if self._closed:
            self._cleanup()
            raise StopAsyncIteration
        try:
            return await self._inner.__anext__()
        except StopAsyncIteration:
            await self.aclose()
            raise
        except BaseException:
            await self.aclose()
            raise

    async def aclose(self) -> None:
        """Close the underlying stream and remove the listen registration."""
        if self._closed:
            return
        self._closed = True
        try:
            aclose = getattr(self._inner, "aclose", None)
            if callable(aclose):
                with contextlib.suppress(BaseException):
                    await aclose()
        finally:
            self._cleanup()

    def _cleanup(self) -> None:
        self._core._listen_registrations.pop(self._subscription_id, None)
        self._core.stream_broker.close_session(self._stream_id)


class MCPCore:
    """Transport-agnostic MCP HTTP and JSON-RPC protocol engine."""

    def __init__(
        self,
        config: MCPConfig | None = None,
        registry: ToolRegistry | None = None,
        telemetry: AgentTelemetryRecorder | None = None,
        stream_broker: SseStreamBroker | None = None,
        task_store: MCPTaskStore | None = None,
        skill_catalog: Any | None = None,
        confirmation_gate: ConfirmationGate | None = None,
        session_store: MCPSessionStore | None = None,
    ) -> None:
        self.config = config if config is not None else MCPConfig()
        self.confirmation_gate = confirmation_gate if confirmation_gate is not None else ConfirmationGate()
        self.registry = registry if registry is not None else ToolRegistry(confirmation_gate=self.confirmation_gate)
        self.telemetry = telemetry if telemetry is not None else AgentTelemetryRecorder()
        self.stream_broker = stream_broker if stream_broker is not None else SseStreamBroker()
        self.task_store = task_store if task_store is not None else MCPTaskStore(stream_broker=self.stream_broker)
        self.skill_catalog = skill_catalog
        self.session_store = (
            session_store
            if session_store is not None
            else MCPSessionStore(
                ttl_seconds=self.config.session_ttl_seconds,
                max_sessions=self.config.max_sessions,
            )
        )
        self.router = JSONRPCRouter()
        self._extensions: dict[str, dict[str, Any]] = {}
        self._listen_registrations: dict[str, _ListenRegistration] = {}
        self._event_counter: int = 0
        self._task_runs: dict[str, tuple[asyncio.Task[None], AgentContext]] = {}

        self._register_handlers()
        self.registry.add_listener(self._on_registry_changed)

    def _next_event_id(self) -> str:
        self._event_counter += 1
        return f"mcp-evt-{self._event_counter}"

    def _make_sse_frame(self, payload: Mapping[str, Any], *, event: str = "message") -> SseFrame:
        return SseFrame(
            id=self._next_event_id(),
            event=event,
            data=encode_json_bytes(payload).decode("utf-8"),
        )

    def _is_family_enabled(self, kind: RegistryChangeKind) -> bool:
        if kind == "tools":
            return self.config.tools_enabled
        if kind == "resources":
            return self.config.resources_enabled
        return self.config.prompts_enabled

    def _on_registry_changed(self, kind: RegistryChangeKind) -> None:
        if not self._is_family_enabled(kind):
            return
        method = f"notifications/{kind}/list_changed"
        legacy_frame = self._make_sse_frame({"jsonrpc": "2.0", "method": method})
        task_ids = self.task_store._tasks if self.task_store is not None else {}
        listen_stream_ids = {reg.stream_id for reg in self._listen_registrations.values()}
        legacy_targets = [
            sid
            for sid in self.stream_broker.active_stream_ids()
            if sid not in listen_stream_ids and sid not in task_ids
        ]
        if legacy_targets:
            self.stream_broker.broadcast(legacy_frame, legacy_targets)
        for reg in list(self._listen_registrations.values()):
            if kind in reg.list_changed_kinds:
                stamped = self._make_sse_frame(
                    {
                        "jsonrpc": "2.0",
                        "method": method,
                        "params": {"_meta": {"io.modelcontextprotocol/subscriptionId": reg.subscription_id}},
                    }
                )
                self.stream_broker.publish(reg.stream_id, stamped)

    def register_extension(self, name: str, payload: Mapping[str, Any] | None = None) -> None:
        """Register an MCP capability extension under ``capabilities.extensions``."""
        self._extensions[name] = dict(payload) if payload is not None else {}

    def _server_capabilities(self, *, protocol_version: str = MCP_PROTOCOL_VERSION) -> dict[str, Any]:
        """Build the unified MCP capability map shared by server/discover and initialize."""
        _ = protocol_version
        capabilities: dict[str, Any] = {}
        if self.config.tools_enabled:
            capabilities["tools"] = {"listChanged": True}
        if self.config.resources_enabled:
            capabilities["resources"] = {
                "subscribe": self.config.resource_subscriptions,
                "listChanged": True,
            }
        if self.config.prompts_enabled:
            capabilities["prompts"] = {"listChanged": True}
        if self.config.tasks_enabled:
            capabilities["tasks"] = {
                "list": {},
                "cancel": {},
                "requests": {"tools": {"call": {}}},
            }
        if self.config.logging:
            capabilities["logging"] = {}
        if self.config.skills_enabled:
            capabilities["skills"] = {"listChanged": True}
        if self._extensions:
            capabilities["extensions"] = {name: dict(payload) for name, payload in self._extensions.items()}
        return capabilities

    def _is_method_enabled(self, method: str) -> bool:
        """Return True when configuration enables the capability family owning method."""
        if method in ("resources/subscribe", "resources/unsubscribe"):
            return self.config.resources_enabled and self.config.resource_subscriptions
        if method.startswith("tools/"):
            return self.config.tools_enabled
        if method.startswith(("resources/", "subscriptions/")):
            return self.config.resources_enabled
        if method.startswith("prompts/"):
            return self.config.prompts_enabled
        if method.startswith("tasks/"):
            return self.config.tasks_enabled
        if method.startswith("skills/"):
            return self.config.skills_enabled
        if method.startswith("logging/"):
            return self.config.logging
        return True

    def _unsupported_version_response(
        self,
        requested_version: str,
        *,
        req_id: Any = None,
    ) -> McpHttpResponse:
        return self._jsonrpc_http_error(
            status=400,
            code=UNSUPPORTED_PROTOCOL_VERSION,
            message=f"Unsupported MCP protocol version: {requested_version}",
            req_id=req_id,
            data={
                "supported": list(SUPPORTED_PROTOCOL_VERSIONS),
                "supportedVersions": list(SUPPORTED_PROTOCOL_VERSIONS),
                "requested": requested_version,
            },
        )

    def _negotiate_protocol(
        self,
        *,
        header_version: str | None,
        meta_version: str | None,
        session_id: str | None,
        frame_method: str | None,
        session_version: str | None = None,
        req_id: Any = None,
    ) -> NegotiatedProtocol | McpHttpResponse:
        """Validate protocol version headers and metadata and resolve modern vs legacy mode."""
        if header_version is not None and header_version not in SUPPORTED_PROTOCOL_VERSIONS:
            return self._unsupported_version_response(header_version, req_id=req_id)
        if meta_version is not None and meta_version not in SUPPORTED_PROTOCOL_VERSIONS:
            return self._unsupported_version_response(meta_version, req_id=req_id)
        if header_version is not None and meta_version is not None and header_version != meta_version:
            return self._jsonrpc_http_error(
                status=400,
                code=HEADER_MISMATCH,
                message=(
                    f"Header MCP-Protocol-Version '{header_version}' "
                    f"does not match _meta protocolVersion '{meta_version}'"
                ),
                req_id=req_id,
            )

        explicit_version = header_version or meta_version
        if explicit_version == MCP_PROTOCOL_VERSION:
            return NegotiatedProtocol(version=MCP_PROTOCOL_VERSION, modern=True)
        if explicit_version is not None:
            return NegotiatedProtocol(version=explicit_version, modern=False)
        if session_version is not None:
            return NegotiatedProtocol(version=session_version, modern=False)
        if session_id is not None or frame_method in ("initialize", "ping", "notifications/initialized"):
            return NegotiatedProtocol(version=MCP_FALLBACK_SESSION_VERSION, modern=False)
        return NegotiatedProtocol(version=MCP_PROTOCOL_VERSION, modern=True)

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

    async def log(self, level: McpLogLevel, data: Any, *, logger: str | None = None) -> int:
        """Broadcast a level-gated notifications/message SSE frame to active legacy streams."""
        if level not in MCP_LOG_LEVEL_INDEX:
            raise ValueError(f"Invalid MCP log level {level!r}; expected one of {', '.join(MCP_LOG_LEVELS)}")
        if not self.config.logging:
            return 0
        level_rank = MCP_LOG_LEVEL_INDEX[level]
        log_params: dict[str, Any] = {"level": level, "data": data}
        if logger is not None:
            log_params["logger"] = logger
        frame = self._make_sse_frame({"jsonrpc": "2.0", "method": "notifications/message", "params": log_params})
        delivered = 0
        seen_sessions: set[str] = set()
        for session in await self.session_store.active_sessions():
            seen_sessions.add(session.session_id)
            if level_rank >= MCP_LOG_LEVEL_INDEX[session.log_level] and self.stream_broker.publish(
                session.session_id, frame
            ):
                delivered += 1
        default_rank = MCP_LOG_LEVEL_INDEX[self.config.default_log_level]
        if level_rank >= default_rank:
            for sid in self.stream_broker.active_stream_ids():
                if sid not in seen_sessions and sid.startswith("anon-") and self.stream_broker.publish(sid, frame):
                    delivered += 1
        return delivered

    async def notify_resource_updated(self, uri: str) -> int:
        """Push notifications/resources/updated to subscribed legacy sessions and listen streams."""
        delivered = 0
        session_ids = await self.session_store.subscribed_session_ids(uri)
        if session_ids:
            payload = {"jsonrpc": "2.0", "method": "notifications/resources/updated", "params": {"uri": uri}}
            frame = self._make_sse_frame(payload)
            delivered += self.stream_broker.broadcast(frame, list(session_ids))

        for reg in list(self._listen_registrations.values()):
            if uri in reg.resource_uris:
                listen_payload = {
                    "jsonrpc": "2.0",
                    "method": "notifications/resources/updated",
                    "params": {
                        "uri": uri,
                        "_meta": {"io.modelcontextprotocol/subscriptionId": reg.subscription_id},
                    },
                }
                if self.stream_broker.publish(reg.stream_id, self._make_sse_frame(listen_payload)):
                    delivered += 1
        return delivered

    async def _dispatch_request(
        self,
        req: JSONRPCRequest,
        context: AgentContext,
        *,
        modern: bool,
        in_batch: bool = False,
    ) -> dict[str, Any] | None:
        if in_batch and req.method == "initialize":
            if req.is_notification:
                return None
            return {
                "jsonrpc": "2.0",
                "id": req.id,
                "error": {
                    "code": INVALID_REQUEST,
                    "message": "initialize cannot be called inside a JSON-RPC batch",
                },
            }
        if (
            (modern and req.method in LEGACY_ONLY_METHODS)
            or (not modern and req.method == "subscriptions/listen")
            or not self._is_method_enabled(req.method)
        ):
            if req.is_notification:
                return None
            return {
                "jsonrpc": "2.0",
                "id": req.id,
                "error": {
                    "code": METHOD_NOT_FOUND,
                    "message": f"Method not found: {req.method}",
                },
            }
        req_meta = req.params.get("_meta") if isinstance(req.params, Mapping) else None
        req_progress_token = req_meta.get("progressToken") if isinstance(req_meta, Mapping) else None
        if (
            not modern
            and isinstance(req_progress_token, (str, int))
            and not isinstance(req_progress_token, bool)
            and context.progress_reporter.callback is None
            and isinstance(context.session_id, str)
            and context.session_id
        ):
            target_sid = context.session_id
            prior_reporter = context.progress_reporter

            async def _emit_session_progress(progress_payload: dict[str, Any]) -> None:
                frame = self._make_sse_frame(
                    {
                        "jsonrpc": "2.0",
                        "method": "notifications/progress",
                        "params": progress_payload,
                    }
                )
                self.stream_broker.publish(target_sid, frame)

            context.progress_reporter = ProgressReporter(
                callback=_emit_session_progress,
                progress_token=req_progress_token,
            )
            try:
                return await self.router.dispatch(req, context)
            finally:
                context.progress_reporter = prior_reporter
        return await self.router.dispatch(req, context)

    async def _dispatch_decoded(
        self,
        parsed: Any,
        context: AgentContext,
        *,
        modern: bool,
    ) -> bytes | None:
        if isinstance(parsed, list):
            if not parsed:
                return encode_json_bytes(
                    {
                        "jsonrpc": "2.0",
                        "id": None,
                        "error": {"code": INVALID_REQUEST, "message": "Invalid request: empty batch"},
                    }
                )
            batch_results: list[dict[str, Any] | None] = [None] * len(parsed)

            async def _process_entry(index: int, item: Any) -> None:
                if not isinstance(item, dict) or item.get("jsonrpc") != "2.0" or "method" not in item:
                    item_id = item.get("id") if isinstance(item, dict) else None
                    batch_results[index] = {
                        "jsonrpc": "2.0",
                        "id": item_id,
                        "error": {"code": INVALID_REQUEST, "message": "Invalid request envelope"},
                    }
                    return
                req = JSONRPCRequest(
                    method=str(item["method"]),
                    jsonrpc=str(item.get("jsonrpc", "2.0")),
                    params=item.get("params"),
                    id=item.get("id"),
                )
                batch_results[index] = await self._dispatch_request(req, context, modern=modern, in_batch=True)

            async with anyio.create_task_group() as tg:
                for idx, entry in enumerate(parsed):
                    tg.start_soon(_process_entry, idx, entry)

            valid_responses = [res for res in batch_results if res is not None]
            if not valid_responses:
                return None
            return encode_json_bytes(valid_responses)

        if isinstance(parsed, dict):
            if parsed.get("jsonrpc") != "2.0" or "method" not in parsed:
                return encode_json_bytes(
                    {
                        "jsonrpc": "2.0",
                        "id": parsed.get("id"),
                        "error": {"code": INVALID_REQUEST, "message": "Invalid request envelope"},
                    }
                )
            req = JSONRPCRequest(
                method=str(parsed["method"]),
                jsonrpc=str(parsed.get("jsonrpc", "2.0")),
                params=parsed.get("params"),
                id=parsed.get("id"),
            )
            resp = await self._dispatch_request(req, context, modern=modern, in_batch=False)
            if resp is None:
                return None
            return encode_json_bytes(resp)

        return encode_json_bytes(
            {
                "jsonrpc": "2.0",
                "id": None,
                "error": {"code": INVALID_REQUEST, "message": "Invalid request: expected object or array"},
            }
        )

    async def handle_payload(self, raw_bytes: bytes, context: AgentContext | None = None) -> bytes | None:
        """Dispatch JSON-RPC under its caller context and restore the prior binding."""
        context = context if isinstance(context, AgentContext) else AgentContext()
        token = bind_agent_context(context)
        try:
            if not raw_bytes or not raw_bytes.strip():
                return encode_json_bytes(
                    {
                        "jsonrpc": "2.0",
                        "id": None,
                        "error": {"code": PARSE_ERROR, "message": "Parse error: empty payload"},
                    }
                )
            try:
                parsed = decode_json(raw_bytes)
            except Exception:
                return encode_json_bytes(
                    {
                        "jsonrpc": "2.0",
                        "id": None,
                        "error": {"code": PARSE_ERROR, "message": "Parse error: invalid JSON"},
                    }
                )
            modern = bool(context.metadata.get("mcp_modern_mode", False))
            return await self._dispatch_decoded(parsed, context, modern=modern)
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
                "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
                "Access-Control-Allow-Headers": (
                    "Content-Type, MCP-Protocol-Version, Mcp-Method, Mcp-Name, "
                    "Mcp-Session-Id, If-None-Match, Authorization"
                ),
                "Allow": "GET, POST, DELETE, OPTIONS",
            }
            return McpHttpResponse(status=204, headers=cors_headers, body=b"")

        if method == "DELETE":
            header_version = lower_headers.get("mcp-protocol-version")
            if header_version is not None and header_version not in SUPPORTED_PROTOCOL_VERSIONS:
                return self._unsupported_version_response(header_version)
            session_id = lower_headers.get("mcp-session-id")
            if not session_id:
                return self._jsonrpc_http_error(
                    status=400,
                    code=INVALID_PARAMS,
                    message="Missing Mcp-Session-Id header",
                )
            deleted = await self.session_store.delete(session_id, self._caller_owner(context))
            if deleted is None:
                return self._jsonrpc_http_error(
                    status=404,
                    code=INVALID_PARAMS,
                    message="Session not found or expired",
                )
            self.stream_broker.close_session(session_id)
            return McpHttpResponse(
                status=204,
                headers={"MCP-Protocol-Version": deleted.protocol_version},
                body=b"",
            )

        if method == "GET":
            return await self._handle_get(lower_headers, context)

        if method != "POST":
            return McpHttpResponse(
                status=405,
                headers={"Content-Type": "text/plain", "Allow": "POST, DELETE, OPTIONS"},
                body=b"Method Not Allowed",
            )

        try:
            frame_kind, frame_method, frame_id = _classify_jsonrpc_frame(request.body)
        except Exception as exc:
            return self._jsonrpc_http_error(
                status=400,
                code=PARSE_ERROR,
                message=f"Parse error: {exc}",
            )

        try:
            decoded_body = decode_json(request.body) if request.body else {}
        except Exception as exc:
            return self._jsonrpc_http_error(
                status=400,
                code=PARSE_ERROR,
                message=f"Parse error: {exc}",
                req_id=frame_id,
            )

        raw_params = decoded_body.get("params") if isinstance(decoded_body, dict) else None
        params = raw_params if isinstance(raw_params, dict) else {}
        raw_meta = params.get("_meta")
        meta_version_raw = (
            raw_meta.get("io.modelcontextprotocol/protocolVersion") if isinstance(raw_meta, Mapping) else None
        )
        meta_version = str(meta_version_raw) if meta_version_raw is not None else None
        header_version = lower_headers.get("mcp-protocol-version")
        session_id = lower_headers.get("mcp-session-id")

        prelim = self._negotiate_protocol(
            header_version=header_version,
            meta_version=meta_version,
            session_id=session_id,
            frame_method=frame_method,
            req_id=frame_id,
        )
        if isinstance(prelim, McpHttpResponse):
            return prelim

        mcp_method_header = lower_headers.get("mcp-method")
        mcp_name_header = lower_headers.get("mcp-name")
        has_mcp_param_header = any(key.startswith("mcp-param-") for key in lower_headers)
        if frame_kind == 2 and (mcp_method_header is not None or mcp_name_header is not None or has_mcp_param_header):
            return self._jsonrpc_http_error(
                status=400,
                code=HEADER_MISMATCH,
                message="Mcp-Method, Mcp-Name, and Mcp-Param-* headers cannot be used with JSON-RPC batch requests",
            )

        session: MCPSession | None = None
        if not prelim.modern and session_id is not None and frame_method != "initialize":
            session = await self.session_store.get(session_id, self._caller_owner(context))
            if session is None:
                return self._jsonrpc_http_error(
                    status=404,
                    code=INVALID_PARAMS,
                    message="Session not found or expired",
                    req_id=frame_id,
                )

        negotiated = self._negotiate_protocol(
            header_version=header_version,
            meta_version=meta_version,
            session_id=session_id,
            frame_method=frame_method,
            session_version=session.protocol_version if session is not None else None,
            req_id=frame_id,
        )
        if isinstance(negotiated, McpHttpResponse):
            return negotiated

        if frame_kind == 2:
            if negotiated.version != "2025-03-26":
                return self._jsonrpc_http_error(
                    status=400,
                    code=INVALID_REQUEST,
                    message=f"JSON-RPC batch arrays are not supported in MCP {negotiated.version}",
                )
            if isinstance(decoded_body, list) and not decoded_body:
                return self._jsonrpc_http_error(
                    status=400,
                    code=INVALID_REQUEST,
                    message="Invalid request: empty batch",
                )

        if mcp_method_header is not None and frame_method is not None and mcp_method_header != frame_method:
            return self._jsonrpc_http_error(
                status=400,
                code=HEADER_MISMATCH,
                message=f"Header Mcp-Method '{mcp_method_header}' does not match body method '{frame_method}'",
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

        context.metadata["mcp_protocol_version"] = negotiated.version
        context.metadata["mcp_modern_mode"] = negotiated.modern
        explicit_version = header_version or meta_version
        if explicit_version is not None:
            context.metadata["mcp_explicit_protocol_version"] = explicit_version
        if isinstance(raw_meta, Mapping):
            client_caps = raw_meta.get("io.modelcontextprotocol/clientCapabilities")
            if isinstance(client_caps, Mapping):
                context.metadata["mcp_client_capabilities"] = dict(client_caps)
        if session is not None:
            context.session_id = session.session_id
            context.metadata["mcp_session_id"] = session.session_id
            context.metadata["mcp_log_level"] = session.log_level
            context.metadata.setdefault("mcp_client_capabilities", dict(session.client_capabilities))
            context.metadata["mcp_client_info"] = dict(session.client_info)

        current_etag = self.registry.etag()
        if (
            frame_method in CACHEABLE_METHODS
            and self._is_method_enabled(frame_method)
            and self._match_etag(lower_headers.get("if-none-match"), current_etag)
        ):
            not_mod_headers: dict[str, str] = {
                "ETag": current_etag,
                "MCP-Protocol-Version": negotiated.version,
            }
            if not negotiated.modern and session is not None:
                not_mod_headers["Mcp-Session-Id"] = session.session_id
            return McpHttpResponse(status=304, headers=not_mod_headers, body=b"")

        accept_types = self._parse_accept_header(lower_headers.get("accept"))
        if (
            frame_kind == 1
            and frame_method == "subscriptions/listen"
            and negotiated.modern
            and self._is_method_enabled("subscriptions/listen")
        ):
            return await self._handle_subscriptions_listen(
                decoded_body=decoded_body,
                lower_headers=lower_headers,
                accept_types=accept_types,
                context=context,
                negotiated=negotiated,
                frame_id=frame_id,
            )

        progress_token = raw_meta.get("progressToken") if isinstance(raw_meta, Mapping) else None
        if (
            frame_kind == 1
            and frame_method != "initialize"
            and isinstance(progress_token, (str, int))
            and not isinstance(progress_token, bool)
            and "text/event-stream" in accept_types
        ):
            return self._open_post_progress_stream(
                decoded_body=decoded_body,
                context=context,
                negotiated=negotiated,
                progress_token=progress_token,
                session=session,
                frame_method=frame_method,
                current_etag=current_etag,
            )

        resp_bytes = await self._dispatch_decoded(decoded_body, context, modern=negotiated.modern)
        final_version = str(context.metadata.get("mcp_negotiated_protocol_version", negotiated.version))
        resp_headers: dict[str, str] = {
            "Content-Type": "application/json",
            "MCP-Protocol-Version": final_version,
        }
        if frame_method in CACHEABLE_METHODS and self._is_method_enabled(frame_method):
            resp_headers["ETag"] = current_etag

        if not negotiated.modern:
            created_sid = context.metadata.get("mcp_created_session_id")
            if isinstance(created_sid, str) and created_sid:
                resp_headers["Mcp-Session-Id"] = created_sid
            elif session is not None:
                resp_headers["Mcp-Session-Id"] = session.session_id

        if frame_kind == 0 or resp_bytes is None:
            return McpHttpResponse(status=202, headers=resp_headers, body=b"")

        status = int(context.metadata.get("mcp_http_status", 200))
        return McpHttpResponse(status=status, headers=resp_headers, body=resp_bytes)

    @staticmethod
    def _parse_accept_header(accept_header: str | None) -> list[str]:
        if not accept_header:
            return []
        accepted: list[str] = []
        for raw_part in accept_header.split(","):
            part = raw_part.strip()
            if not part:
                continue
            segments = [seg.strip().lower() for seg in part.split(";")]
            media_type = segments[0]
            if not media_type:
                continue
            excluded = False
            for param in segments[1:]:
                if param.startswith("q="):
                    with contextlib.suppress(ValueError):
                        if float(param[2:]) <= 0.0:
                            excluded = True
            if not excluded:
                accepted.append(media_type)
        return accepted

    async def _handle_get(self, lower_headers: Mapping[str, str], context: AgentContext) -> McpHttpResponse:
        header_version = lower_headers.get("mcp-protocol-version")
        if header_version is not None and header_version not in SUPPORTED_PROTOCOL_VERSIONS:
            return self._unsupported_version_response(header_version)

        session_id = lower_headers.get("mcp-session-id")
        negotiated = self._negotiate_protocol(
            header_version=header_version,
            meta_version=None,
            session_id=session_id,
            frame_method=None,
        )
        if isinstance(negotiated, McpHttpResponse):
            return negotiated
        if negotiated.modern or not self.config.enable_get_sse:
            return McpHttpResponse(
                status=405,
                headers={"Content-Type": "text/plain", "Allow": "POST, DELETE, OPTIONS"},
                body=b"Method Not Allowed",
            )

        accept_types = self._parse_accept_header(lower_headers.get("accept"))
        if "text/event-stream" not in accept_types:
            if not accept_types or "*/*" in accept_types:
                return McpHttpResponse(
                    status=405,
                    headers={"Content-Type": "text/plain", "Allow": "POST, DELETE, OPTIONS"},
                    body=b"Method Not Allowed",
                )
            return McpHttpResponse(
                status=406,
                headers={"Content-Type": "text/plain"},
                body=b"Not Acceptable: text/event-stream required",
            )

        owner_id = self._caller_owner(context)
        session: MCPSession | None = None
        if session_id is not None:
            session = await self.session_store.get(session_id, owner_id)
            if session is None:
                return self._jsonrpc_http_error(
                    status=404,
                    code=INVALID_PARAMS,
                    message="Session not found or expired",
                )
            stream_session_id = session.session_id
            stream_version = header_version if header_version is not None else session.protocol_version
        else:
            stream_session_id = f"anon-{secrets.token_hex(8)}"
            stream_version = negotiated.version

        last_event_id = lower_headers.get("last-event-id") or None
        try:
            stream = self.stream_broker.open_stream(
                stream_session_id,
                owner_id=owner_id,
                last_event_id=last_event_id,
                heartbeat_seconds=self.config.sse_heartbeat_seconds,
                max_streams_per_owner=self.config.max_streams_per_owner,
            )
        except StreamLimitExceededError as exc:
            return self._jsonrpc_http_error(
                status=503,
                code=SESSION_CAPACITY_EXHAUSTED,
                message=str(exc),
            )

        sse_headers: dict[str, str] = {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "MCP-Protocol-Version": stream_version,
        }
        if session is not None:
            sse_headers["Mcp-Session-Id"] = session.session_id
        return McpHttpResponse(status=200, headers=sse_headers, body=b"", stream=stream)

    def _open_post_progress_stream(
        self,
        *,
        decoded_body: Any,
        context: AgentContext,
        negotiated: NegotiatedProtocol,
        progress_token: str | int,
        session: MCPSession | None,
        frame_method: str | None,
        current_etag: str,
    ) -> McpHttpResponse:
        send_chan, recv_chan = anyio.create_memory_object_stream[bytes](128)

        async def _emit_post_progress(progress_payload: dict[str, Any]) -> None:
            frame = self._make_sse_frame(
                {
                    "jsonrpc": "2.0",
                    "method": "notifications/progress",
                    "params": progress_payload,
                }
            )
            with contextlib.suppress(anyio.ClosedResourceError, anyio.BrokenResourceError):
                await send_chan.send(frame.encode())

        context.progress_reporter = ProgressReporter(
            callback=_emit_post_progress,
            progress_token=progress_token,
        )

        async def _post_sse_stream() -> AsyncIterator[bytes]:
            async def _run_dispatch() -> None:
                try:
                    token = bind_agent_context(context)
                    try:
                        resp_bytes = await self._dispatch_decoded(decoded_body, context, modern=negotiated.modern)
                    finally:
                        reset_agent_context(token)
                    if resp_bytes is not None:
                        final_frame = SseFrame(
                            id=self._next_event_id(),
                            event="message",
                            data=resp_bytes.decode("utf-8"),
                        )
                        with contextlib.suppress(anyio.ClosedResourceError, anyio.BrokenResourceError):
                            await send_chan.send(final_frame.encode())
                finally:
                    with contextlib.suppress(Exception):
                        await send_chan.aclose()

            try:
                async with anyio.create_task_group() as tg:
                    tg.start_soon(_run_dispatch)
                    async for chunk in recv_chan:
                        yield chunk
            finally:
                with contextlib.suppress(Exception):
                    await send_chan.aclose()
                with contextlib.suppress(Exception):
                    await recv_chan.aclose()

        sse_headers: dict[str, str] = {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "MCP-Protocol-Version": negotiated.version,
        }
        if frame_method in CACHEABLE_METHODS and self._is_method_enabled(frame_method):
            sse_headers["ETag"] = current_etag
        if not negotiated.modern and session is not None:
            sse_headers["Mcp-Session-Id"] = session.session_id
        return McpHttpResponse(status=200, headers=sse_headers, body=b"", stream=_post_sse_stream())

    def _is_resolvable_resource_uri(self, uri: str) -> bool:
        if self.registry.get_resource(uri) is not None:
            return True
        if self.registry.match_resource_template(uri) is not None:
            return True
        if uri.startswith("skill://") and self.skill_catalog is not None:
            raw = uri.removeprefix("skill://").strip("/")
            if not raw:
                return False
            parts = raw.split("/", 1)
            skill = self.skill_catalog.get_skill(parts[0])
            if skill is None:
                return False
            if len(parts) == 1:
                return True
            subpath = parts[1]
            return subpath in skill.files or any(file_path.startswith(f"{subpath}/") for file_path in skill.files)
        return False

    async def _authorize_subscription(self, uri: str, context: Any) -> None:
        authorizer = self.config.subscription_authorizer
        if authorizer is None:
            return
        try:
            sig = inspect.signature(authorizer)
            positional = [
                param
                for param in sig.parameters.values()
                if param.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
            ]
            has_varargs = any(param.kind == inspect.Parameter.VAR_POSITIONAL for param in sig.parameters.values())
            if has_varargs or len(positional) >= 2:
                if positional and positional[0].name in ("ctx", "context"):
                    outcome = authorizer(context, uri)
                else:
                    outcome = authorizer(uri, context)
            elif len(positional) == 1:
                outcome = authorizer(uri)
            else:
                outcome = authorizer()
            if inspect.isawaitable(outcome):
                outcome = await outcome
        except Exception as exc:
            raise JSONRPCErrorException(
                JSONRPCError(
                    code=PERMISSION_DENIED,
                    message=f"Subscription unauthorized for resource '{uri}'",
                    data={"uri": uri, "reason": str(exc)},
                )
            ) from exc
        if not bool(outcome):
            raise JSONRPCErrorException(
                JSONRPCError(
                    code=PERMISSION_DENIED,
                    message=f"Subscription denied for resource '{uri}'",
                    data={"uri": uri},
                )
            )

    async def _handle_subscriptions_listen(
        self,
        *,
        decoded_body: Any,
        lower_headers: Mapping[str, str],
        accept_types: list[str],
        context: AgentContext,
        negotiated: NegotiatedProtocol,
        frame_id: str | None,
    ) -> McpHttpResponse:
        if "text/event-stream" not in accept_types:
            return McpHttpResponse(
                status=406,
                headers={"Content-Type": "text/plain"},
                body=b"Not Acceptable: text/event-stream required",
            )
        raw_req_id = decoded_body.get("id") if isinstance(decoded_body, Mapping) else frame_id
        raw_params = decoded_body.get("params") if isinstance(decoded_body, Mapping) else None
        if raw_params is not None and not isinstance(raw_params, Mapping):
            return self._jsonrpc_http_error(
                status=200,
                code=INVALID_PARAMS,
                message="Invalid params in subscriptions/listen",
                req_id=raw_req_id,
            )
        params: Mapping[str, Any] = raw_params if isinstance(raw_params, Mapping) else {}

        resource_uris: list[str] = []
        raw_subs = params.get("resourceSubscriptions")
        if raw_subs is not None:
            if not isinstance(raw_subs, (list, tuple)) or isinstance(raw_subs, (str, bytes, Mapping)):
                return self._jsonrpc_http_error(
                    status=200,
                    code=INVALID_PARAMS,
                    message="resourceSubscriptions must be a list of URIs",
                    req_id=raw_req_id,
                )
            if raw_subs and not self.config.resource_subscriptions:
                return self._jsonrpc_http_error(
                    status=200,
                    code=METHOD_NOT_FOUND,
                    message="Resource subscriptions are disabled",
                    req_id=raw_req_id,
                )
            for item in raw_subs:
                uri = item.get("uri") if isinstance(item, Mapping) else item
                if not isinstance(uri, str) or not uri:
                    return self._jsonrpc_http_error(
                        status=200,
                        code=INVALID_PARAMS,
                        message="Each resourceSubscriptions entry must be a non-empty URI string",
                        req_id=raw_req_id,
                    )
                if not self._is_resolvable_resource_uri(uri):
                    return self._jsonrpc_http_error(
                        status=200,
                        code=INVALID_PARAMS,
                        message=f"Unknown or unresolvable resource URI: {uri}",
                        req_id=raw_req_id,
                    )
                try:
                    await self._authorize_subscription(uri, context)
                except JSONRPCErrorException as exc:
                    return self._jsonrpc_http_error(
                        status=200,
                        code=exc.error.code,
                        message=exc.error.message,
                        req_id=raw_req_id,
                        data=exc.error.data,
                    )
                resource_uris.append(uri)

        list_changed_kinds: set[RegistryChangeKind] = set()
        if "listChanged" not in params:
            if not resource_uris:
                list_changed_kinds = {kind for kind in _REGISTRY_CHANGE_KINDS if self._is_family_enabled(kind)}
        else:
            raw_lc = params["listChanged"]
            if raw_lc is True:
                list_changed_kinds = {kind for kind in _REGISTRY_CHANGE_KINDS if self._is_family_enabled(kind)}
            elif raw_lc is False or raw_lc is None:
                list_changed_kinds = set()
            elif isinstance(raw_lc, (list, tuple, set, frozenset)):
                for item in raw_lc:
                    if item not in _REGISTRY_CHANGE_KINDS:
                        return self._jsonrpc_http_error(
                            status=200,
                            code=INVALID_PARAMS,
                            message=f"Invalid listChanged family {item!r}",
                            req_id=raw_req_id,
                        )
                    if self._is_family_enabled(item):
                        list_changed_kinds.add(item)
            elif isinstance(raw_lc, Mapping):
                for key, enabled in raw_lc.items():
                    if key not in _REGISTRY_CHANGE_KINDS:
                        return self._jsonrpc_http_error(
                            status=200,
                            code=INVALID_PARAMS,
                            message=f"Invalid listChanged family {key!r}",
                            req_id=raw_req_id,
                        )
                    if enabled and self._is_family_enabled(key):
                        list_changed_kinds.add(key)
            else:
                return self._jsonrpc_http_error(
                    status=200,
                    code=INVALID_PARAMS,
                    message="Invalid listChanged parameter in subscriptions/listen",
                    req_id=raw_req_id,
                )

        sub_id_raw = params.get("subscriptionId")
        if sub_id_raw is not None and (not isinstance(sub_id_raw, str) or not sub_id_raw):
            return self._jsonrpc_http_error(
                status=200,
                code=INVALID_PARAMS,
                message="subscriptionId must be a non-empty string",
                req_id=raw_req_id,
            )
        subscription_id = sub_id_raw if isinstance(sub_id_raw, str) else f"sub_{secrets.token_hex(12)}"
        stream_id = f"listen-{subscription_id}-{secrets.token_hex(6)}"
        owner_id = self._caller_owner(context)
        try:
            raw_stream = self.stream_broker.open_stream(
                stream_id,
                owner_id=owner_id,
                last_event_id=lower_headers.get("last-event-id") or None,
                heartbeat_seconds=self.config.sse_heartbeat_seconds,
                max_streams_per_owner=self.config.max_streams_per_owner,
            )
        except StreamLimitExceededError as exc:
            return self._jsonrpc_http_error(
                status=503,
                code=SESSION_CAPACITY_EXHAUSTED,
                message=str(exc),
                req_id=raw_req_id,
            )

        self._listen_registrations[stream_id] = _ListenRegistration(
            subscription_id=subscription_id,
            stream_id=stream_id,
            resource_uris=frozenset(resource_uris),
            list_changed_kinds=frozenset(list_changed_kinds),
            owner_id=owner_id,
        )
        if raw_req_id is not None:
            init_payload = {
                "jsonrpc": "2.0",
                "id": raw_req_id,
                "result": self._with_meta(
                    "subscriptions/listen",
                    {"subscriptionId": subscription_id},
                    context=context,
                ),
            }
            self.stream_broker.publish(stream_id, self._make_sse_frame(init_payload))

        wrapped_stream = _ListenStreamWrapper(self, stream_id, stream_id, raw_stream)
        return McpHttpResponse(
            status=200,
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "MCP-Protocol-Version": negotiated.version,
            },
            body=b"",
            stream=wrapped_stream,
        )

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
                decoded_header_val = decode_json(decoded_expected.encode("utf-8"))
                normalized_actual = dict(actual_val) if isinstance(actual_val, Mapping) else actual_val
                return ConfirmationGate.arguments_equal(decoded_header_val, normalized_actual)
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
        data: Any = None,
    ) -> McpHttpResponse:
        err = JSONRPCError(code=code, message=message, data=data)
        err_body = encode_json_bytes(
            {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": err.to_dict(),
            }
        )
        return McpHttpResponse(status=status, headers={"Content-Type": "application/json"}, body=err_body)

    @staticmethod
    def _with_meta(
        method: str,
        payload: dict[str, Any],
        *,
        cacheable: bool = False,
        context: Any = None,
    ) -> dict[str, Any]:
        resolved_ctx = context if isinstance(context, AgentContext) else current_agent_context()
        proto_version = (
            str(resolved_ctx.metadata.get("mcp_protocol_version", MCP_PROTOCOL_VERSION))
            if resolved_ctx is not None
            else MCP_PROTOCOL_VERSION
        )
        meta: dict[str, Any] = {
            "io.modelcontextprotocol/protocolVersion": proto_version,
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
        """Register all MCP protocol handlers on the JSON-RPC router."""
        self.router.register("initialize", self._handle_initialize)
        self.router.register("ping", self._handle_ping)
        self.router.register("logging/setLevel", self._handle_logging_set_level)
        self.router.register("notifications/initialized", self._handle_initialized_notif)
        self.router.register("server/discover", self._handle_discover)
        self.router.register("tools/list", self._handle_tools_list)
        self.router.register("tools/call", self._handle_tools_call)
        self.router.register("resources/list", self._handle_resources_list)
        self.router.register("resources/read", self._handle_resources_read)
        self.router.register("resources/subscribe", self._handle_resources_subscribe)
        self.router.register("resources/unsubscribe", self._handle_resources_unsubscribe)
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

    async def _handle_initialize(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Negotiate a legacy MCP protocol revision and issue an owner-scoped session."""
        requested = params.get("protocolVersion") if isinstance(params, Mapping) else None
        explicit_header = (
            context.metadata.get("mcp_explicit_protocol_version") if isinstance(context, AgentContext) else None
        )
        if isinstance(requested, str) and requested in LEGACY_PROTOCOL_VERSIONS:
            negotiated_version = requested
        elif isinstance(explicit_header, str) and explicit_header in LEGACY_PROTOCOL_VERSIONS:
            negotiated_version = explicit_header
        else:
            negotiated_version = MCP_FALLBACK_SESSION_VERSION

        raw_caps = params.get("capabilities") if isinstance(params, Mapping) else None
        client_caps = dict(raw_caps) if isinstance(raw_caps, Mapping) else {}
        raw_info = params.get("clientInfo") if isinstance(params, Mapping) else None
        client_info = dict(raw_info) if isinstance(raw_info, Mapping) else {}

        try:
            session = await self.session_store.create(
                owner_id=self._caller_owner(context),
                protocol_version=negotiated_version,
                client_capabilities=client_caps,
                client_info=client_info,
                log_level=self.config.default_log_level,
            )
        except SessionLimitExceededError as exc:
            if isinstance(context, AgentContext):
                context.metadata["mcp_http_status"] = 503
            raise JSONRPCErrorException(JSONRPCError(code=SESSION_CAPACITY_EXHAUSTED, message=str(exc))) from exc

        if isinstance(context, AgentContext):
            context.session_id = session.session_id
            context.metadata["mcp_created_session_id"] = session.session_id
            context.metadata["mcp_negotiated_protocol_version"] = session.protocol_version
            context.metadata["mcp_client_capabilities"] = dict(session.client_capabilities)
            context.metadata["mcp_client_info"] = dict(session.client_info)
            context.metadata["mcp_log_level"] = session.log_level

        result: dict[str, Any] = {
            "protocolVersion": negotiated_version,
            "capabilities": self._server_capabilities(protocol_version=negotiated_version),
            "serverInfo": {"name": self.config.server_name, "version": self.config.server_version},
        }
        if self.config.instructions:
            result["instructions"] = self.config.instructions
        return result

    async def _handle_ping(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Respond to legacy MCP ping liveness probes."""
        _ = (params, context)
        return {}

    async def _handle_logging_set_level(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Validate and persist a legacy session minimum log severity level."""
        level_map: dict[str, McpLogLevel] = {lvl: lvl for lvl in MCP_LOG_LEVELS}
        raw_level = params.get("level") if isinstance(params, Mapping) else None
        typed_level = level_map.get(raw_level) if isinstance(raw_level, str) else None
        if typed_level is None:
            raise JSONRPCErrorException(
                JSONRPCError(
                    code=INVALID_PARAMS,
                    message=f"Invalid log level {raw_level!r}; expected one of {', '.join(MCP_LOG_LEVELS)}",
                )
            )
        if isinstance(context, AgentContext):
            context.metadata["mcp_log_level"] = typed_level
            session_id = context.metadata.get("mcp_session_id") or context.session_id
            if isinstance(session_id, str) and session_id:
                await self.session_store.update(
                    session_id,
                    self._caller_owner(context),
                    log_level=typed_level,
                )
        return {}

    async def _handle_initialized_notif(self, params: Any = None, context: Any = None) -> None:
        """Process notifications/initialized confirmation."""
        return None

    async def _handle_discover(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Return server capability manifest and registered count summaries."""
        resolved_ctx = context if isinstance(context, AgentContext) else current_agent_context()
        proto_version = (
            str(resolved_ctx.metadata.get("mcp_protocol_version", MCP_PROTOCOL_VERSION))
            if resolved_ctx is not None
            else MCP_PROTOCOL_VERSION
        )
        tools_list = self.registry.list_specs(exposure="mcp")
        resources_list = self.registry.list_resources()
        prompts_list = self.registry.list_prompts()
        skills_list = self.skill_catalog.list_skills() if self.skill_catalog is not None else ()
        return self._with_meta(
            "server/discover",
            {
                "protocolVersion": proto_version,
                "serverInfo": {"name": self.config.server_name, "version": self.config.server_version},
                "instructions": self.config.instructions,
                "capabilities": self._server_capabilities(protocol_version=proto_version),
                "counts": {
                    "tools": len(tools_list) if self.config.tools_enabled else 0,
                    "resources": len(resources_list) if self.config.resources_enabled else 0,
                    "prompts": len(prompts_list) if self.config.prompts_enabled else 0,
                    "skills": len(skills_list) if self.config.skills_enabled else 0,
                },
            },
            cacheable=True,
            context=resolved_ctx,
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

    async def _require_legacy_session(self, context: Any) -> tuple[MCPSession, str | None]:
        owner_id = self._caller_owner(context)
        session_id = None
        if isinstance(context, AgentContext):
            raw_sid = context.metadata.get("mcp_session_id") or context.session_id
            if isinstance(raw_sid, str) and raw_sid:
                session_id = raw_sid
        if not session_id:
            raise JSONRPCErrorException(
                JSONRPCError(
                    code=INVALID_PARAMS,
                    message="Resource subscriptions require an initialized legacy MCP session",
                )
            )
        session = await self.session_store.get(session_id, owner_id)
        if session is None:
            raise JSONRPCErrorException(
                JSONRPCError(
                    code=INVALID_PARAMS,
                    message="Session not found or expired",
                )
            )
        return session, owner_id

    async def _handle_resources_subscribe(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Subscribe an initialized legacy session to updates for a resolvable, authorized resource URI."""
        session, owner_id = await self._require_legacy_session(context)
        if not isinstance(params, Mapping) or not isinstance(params.get("uri"), str) or not params["uri"]:
            raise JSONRPCErrorException(
                JSONRPCError(code=INVALID_PARAMS, message="Missing required 'uri' in resources/subscribe parameters")
            )
        uri = str(params["uri"])
        if not self._is_resolvable_resource_uri(uri):
            raise JSONRPCErrorException(
                JSONRPCError(code=INVALID_PARAMS, message=f"Unknown or unresolvable resource URI: {uri}")
            )
        await self._authorize_subscription(uri, context)
        if uri not in session.subscriptions and len(session.subscriptions) >= self.config.max_subscriptions_per_session:
            raise JSONRPCErrorException(
                JSONRPCError(
                    code=SESSION_CAPACITY_EXHAUSTED,
                    message=f"Maximum resource subscriptions ({self.config.max_subscriptions_per_session}) exceeded for session",
                )
            )
        if uri not in session.subscriptions:
            await self.session_store.update(
                session.session_id,
                owner_id,
                subscriptions=session.subscriptions | {uri},
            )
        return {}

    async def _handle_resources_unsubscribe(self, params: Any = None, context: Any = None) -> dict[str, Any]:
        """Unsubscribe a legacy session from resource updates idempotently."""
        session, owner_id = await self._require_legacy_session(context)
        if not isinstance(params, Mapping) or not isinstance(params.get("uri"), str) or not params["uri"]:
            raise JSONRPCErrorException(
                JSONRPCError(code=INVALID_PARAMS, message="Missing required 'uri' in resources/unsubscribe parameters")
            )
        uri = str(params["uri"])
        if uri in session.subscriptions:
            await self.session_store.update(
                session.session_id,
                owner_id,
                subscriptions=session.subscriptions - {uri},
            )
        return {}

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
