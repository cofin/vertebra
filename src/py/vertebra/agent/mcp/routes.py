"""Streamable HTTP and SSE route handlers for Model Context Protocol (MCP) 2026-07-28.

This module provides the MCPHttpHandler responsible for validating MCP protocol headers,
enforcing Origin allowlists, serving ETag 304 Not Modified cache responses,
and managing unary JSON or text/event-stream SSE responses.
"""

import base64
import email.header
from collections.abc import AsyncIterable, Mapping

from vertebra.agent.core.context import AgentContext
from vertebra.agent.core.jsonrpc import JSONRPCError
from vertebra.agent.mcp.models import (
    HEADER_MISMATCH,
    MCP_PROTOCOL_VERSION,
    UNSUPPORTED_PROTOCOL_VERSION,
    MCPConfig,
)
from vertebra.agent.mcp.service import MCPService
from vertebra.utils.serializers import decode_json, encode_json_bytes

try:
    from vertebra._vertebra import classify_jsonrpc_frame
except ImportError:

    def classify_jsonrpc_frame(payload: bytes) -> tuple[int, str | None, str | None]:
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


MCP_NAME_FIELDS: dict[str, str] = {
    "tools/call": "name",
    "prompts/get": "name",
    "skills/get": "name",
    "resources/read": "uri",
    "resources/subscribe": "uri",
    "resources/unsubscribe": "uri",
    "tasks/get": "taskId",
    "tasks/result": "taskId",
    "tasks/update": "taskId",
    "tasks/cancel": "taskId",
}

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


def decode_mcp_header_value(value: str) -> str:
    """Decode an RFC 2047 or Base64-encoded MCP header value."""
    if not (value.startswith("=?") and value.endswith("?=")):
        return value
    inner = value[2:-2]
    parts = inner.split("?")
    if len(parts) == 2 and parts[0].lower() in ("base64", "b"):
        try:
            return base64.b64decode(parts[1]).decode("utf-8")
        except Exception:
            return value
    if len(parts) == 3 and parts[1].upper() == "B":
        charset = parts[0]
        try:
            return base64.b64decode(parts[2]).decode(charset)
        except Exception:
            return value
    try:
        decoded_parts = email.header.decode_header(value)
        out: list[str] = []
        for piece, encoding in decoded_parts:
            if isinstance(piece, bytes):
                out.append(piece.decode(encoding or "utf-8", errors="replace"))
            else:
                out.append(piece)
        return "".join(out)
    except Exception:
        return value


class MCPHttpHandler:
    """Streamable HTTP route handler coordinating validation, caching, and dispatch."""

    def __init__(self, service: MCPService) -> None:
        self.service = service
        self.config: MCPConfig = service.config

    def _is_origin_allowed(self, origin: str | None, host: str | None) -> bool:
        if not origin:
            return True
        allowed = self.config.allowed_origins
        if "*" in allowed:
            return True
        if origin in allowed:
            return True
        return bool(host and (origin == f"http://{host}" or origin == f"https://{host}"))

    def _match_etag(self, if_none_match: str | None, current_etag: str) -> bool:
        if not if_none_match:
            return False
        tokens = [t.strip() for t in if_none_match.split(",")]
        normalized_current = current_etag.strip('"')
        for token in tokens:
            if token == "*":
                return True
            raw = token.removeprefix("W/").removeprefix("w/").strip('"')
            if raw == normalized_current:
                return True
        return False

    async def handle_request(
        self,
        method: str,
        path: str,
        headers: Mapping[str, str],
        body: bytes,
        context: AgentContext | None = None,
    ) -> tuple[int, dict[str, str], bytes | AsyncIterable[bytes]]:
        """Process an incoming HTTP request, enforcing headers, origin, and caching."""
        lower_headers = {k.lower(): v for k, v in headers.items()}
        origin = lower_headers.get("origin")
        host = lower_headers.get("host")

        if not self._is_origin_allowed(origin, host):
            return 403, {"Content-Type": "text/plain"}, b"Forbidden: Disallowed Origin"

        if method == "OPTIONS":
            cors_headers = {
                "Access-Control-Allow-Origin": origin or "*",
                "Access-Control-Allow-Methods": "POST, GET, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type, MCP-Protocol-Version, Mcp-Method, Mcp-Name, Mcp-Session-Id, If-None-Match, Authorization",
            }
            return 204, cors_headers, b""

        if method == "GET":
            req_payload = encode_json_bytes(
                {
                    "jsonrpc": "2.0",
                    "id": "discover",
                    "method": "server/discover",
                    "params": {},
                }
            )
            resp_bytes = await self.service.handle_payload(req_payload, context)
            current_etag = self.service.registry.schema_cache.combined_etag()
            if self._match_etag(lower_headers.get("if-none-match"), current_etag):
                return 304, {"ETag": f'"{current_etag}"'}, b""
            resp_headers = {
                "Content-Type": "application/json",
                "ETag": f'"{current_etag}"',
                "MCP-Protocol-Version": MCP_PROTOCOL_VERSION,
            }
            return 200, resp_headers, resp_bytes or b""

        if method != "POST":
            return 405, {"Content-Type": "text/plain"}, b"Method Not Allowed"

        proto_ver = lower_headers.get("mcp-protocol-version")
        if proto_ver is not None and proto_ver != MCP_PROTOCOL_VERSION:
            err = JSONRPCError(
                code=UNSUPPORTED_PROTOCOL_VERSION,
                message=f"Unsupported MCP protocol version: {proto_ver}",
            )
            err_body = encode_json_bytes(
                {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": err.code, "message": err.message},
                }
            )
            return 200, {"Content-Type": "application/json"}, err_body

        try:
            frame_kind, frame_method, frame_id = classify_jsonrpc_frame(body)
        except Exception as exc:
            err = JSONRPCError(code=-32700, message=f"Parse error: {exc}")
            err_body = encode_json_bytes(
                {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": err.code, "message": err.message},
                }
            )
            return 200, {"Content-Type": "application/json"}, err_body

        mcp_method_header = lower_headers.get("mcp-method")
        mcp_name_header = lower_headers.get("mcp-name")
        if frame_kind == 2 and (mcp_method_header is not None or mcp_name_header is not None):
            err = JSONRPCError(
                code=HEADER_MISMATCH,
                message="Mcp-Method and Mcp-Name headers cannot be used with JSON-RPC batch requests",
            )
            err_body = encode_json_bytes(
                {"jsonrpc": "2.0", "id": None, "error": {"code": err.code, "message": err.message}}
            )
            return 200, {"Content-Type": "application/json"}, err_body

        if mcp_method_header is not None and frame_method is not None and mcp_method_header != frame_method:
            err = JSONRPCError(
                code=HEADER_MISMATCH,
                message=f"Header Mcp-Method '{mcp_method_header}' does not match body method '{frame_method}'",
            )
            err_body = encode_json_bytes(
                {
                    "jsonrpc": "2.0",
                    "id": frame_id,
                    "error": {"code": err.code, "message": err.message},
                }
            )
            return 200, {"Content-Type": "application/json"}, err_body

        if mcp_name_header is not None and frame_method in MCP_NAME_FIELDS:
            decoded_name = decode_mcp_header_value(mcp_name_header)
            field_name = MCP_NAME_FIELDS[frame_method]
            body_dict = decode_json(body)
            raw_params = body_dict.get("params") if isinstance(body_dict, dict) else None
            params = raw_params if isinstance(raw_params, dict) else {}
            target_val = params.get(field_name)
            if target_val is None and field_name == "taskId":
                target_val = params.get("task_id")
            if target_val is None or str(target_val) != decoded_name:
                err = JSONRPCError(
                    code=HEADER_MISMATCH,
                    message=f"Header Mcp-Name '{decoded_name}' does not match body parameter {field_name!r}",
                )
                err_body = encode_json_bytes(
                    {
                        "jsonrpc": "2.0",
                        "id": frame_id,
                        "error": {"code": err.code, "message": err.message},
                    }
                )
                return 200, {"Content-Type": "application/json"}, err_body

        current_etag = self.service.registry.schema_cache.combined_etag()
        if frame_method in CACHEABLE_METHODS and self._match_etag(lower_headers.get("if-none-match"), current_etag):
            return 304, {"ETag": f'"{current_etag}"'}, b""

        resp_bytes = await self.service.handle_payload(body, context)
        resp_headers = {
            "Content-Type": "application/json",
            "MCP-Protocol-Version": MCP_PROTOCOL_VERSION,
        }
        if frame_method in CACHEABLE_METHODS:
            resp_headers["ETag"] = f'"{current_etag}"'

        session_id = lower_headers.get("mcp-session-id")
        if session_id:
            resp_headers["Mcp-Session-Id"] = session_id

        if resp_bytes is None:
            return 204, resp_headers, b""

        return 200, resp_headers, resp_bytes
