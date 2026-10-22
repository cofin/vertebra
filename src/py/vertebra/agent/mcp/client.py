"""MCP HTTP client and header derivation utilities for Model Context Protocol 2026-07-28.

This module provides encode/decode helpers for MCP headers, mcp_request_headers derivation,
and MCPStreamableHttpClient for querying remote MCP servers over HTTP/SSE.
"""

import base64
import email.header
import secrets
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from typing import Any

import anyio
import anyio.to_thread

from vertebra.agent.core.context import current_agent_context
from vertebra.agent.mcp.models import MCP_PROTOCOL_VERSION
from vertebra.telemetry.genai import inject_w3c_trace_context
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


def encode_mcp_header_value(value: str) -> str:
    """Encode an MCP header value using RFC 2047 Base64 format if non-ASCII."""
    if value.isascii():
        return value
    b64 = base64.b64encode(value.encode("utf-8")).decode("ascii")
    return f"=?utf-8?B?{b64}?="


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


def mcp_request_headers(
    payload: bytes | Mapping[str, Any],
    *,
    protocol_version: str = MCP_PROTOCOL_VERSION,
    session_id: str | None = None,
    bearer_token: str | None = None,
    trace_context: Any | None = None,
) -> dict[str, str]:
    """Derive standard MCP 2026-07-28 request headers from a JSON-RPC frame."""
    headers: dict[str, str] = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": protocol_version,
    }
    if session_id:
        headers["Mcp-Session-Id"] = session_id
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"

    data: dict[str, Any] | None = None
    if isinstance(payload, bytes):
        try:
            raw_decoded = decode_json(payload)
            if isinstance(raw_decoded, dict):
                data = raw_decoded
        except Exception:
            pass
    elif isinstance(payload, Mapping):
        data = dict(payload)

    if data is not None:
        method = data.get("method")
        if isinstance(method, str) and method:
            headers["Mcp-Method"] = method
            target_field = MCP_NAME_FIELDS.get(method)
            if target_field:
                params = data.get("params")
                if isinstance(params, Mapping):
                    val = params.get(target_field)
                    if val is None and target_field == "taskId":
                        val = params.get("task_id")
                    if val is not None:
                        headers["Mcp-Name"] = encode_mcp_header_value(str(val))

    if trace_context is not None and hasattr(trace_context, "format"):
        inject_w3c_trace_context(trace_context, headers=headers)
    else:
        curr = current_agent_context()
        if curr is not None and getattr(curr, "trace_context", None) is not None:
            inject_w3c_trace_context(curr.trace_context, headers=headers)

    return headers


def _default_http_request(
    url: str,
    headers: dict[str, str],
    body: bytes,
    timeout_s: float,
) -> tuple[int, dict[str, str], bytes]:
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            resp_headers = dict(resp.headers)
            resp_bytes = resp.read()
            return resp.status, resp_headers, resp_bytes
    except urllib.error.HTTPError as exc:
        resp_headers = dict(exc.headers)
        resp_bytes = exc.read()
        return exc.code, resp_headers, resp_bytes


class MCPStreamableHttpClient:
    """Async client for interacting with remote MCP servers."""

    def __init__(
        self,
        base_url: str,
        protocol_version: str = MCP_PROTOCOL_VERSION,
        bearer_token: str | None = None,
        timeout_s: float = 30.0,
        transport: Callable[..., Any] | None = None,
    ) -> None:
        self.base_url = base_url
        self.protocol_version = protocol_version
        self.bearer_token = bearer_token
        self.timeout_s = timeout_s
        self.transport = transport
        self.session_id: str | None = None

    async def call(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        req_id: Any = None,
    ) -> dict[str, Any]:
        """Send a JSON-RPC 2.0 request frame to the MCP server."""
        actual_id = req_id if req_id is not None else secrets.token_hex(8)
        payload_dict: dict[str, Any] = {
            "jsonrpc": "2.0",
            "id": actual_id,
            "method": method,
            "params": params or {},
        }
        body_bytes = encode_json_bytes(payload_dict)
        headers = mcp_request_headers(
            payload_dict,
            protocol_version=self.protocol_version,
            session_id=self.session_id,
            bearer_token=self.bearer_token,
        )

        if self.transport is not None:
            resp = self.transport(self.base_url, headers, body_bytes)
            if hasattr(resp, "__await__"):
                resp = await resp
            if isinstance(resp, tuple):
                _, resp_headers, resp_body = resp
            else:
                resp_headers, resp_body = {}, resp
        else:
            _, resp_headers, resp_body = await anyio.to_thread.run_sync(
                _default_http_request,
                self.base_url,
                headers,
                body_bytes,
                self.timeout_s,
            )

        sid = resp_headers.get("Mcp-Session-Id") or resp_headers.get("mcp-session-id")
        if sid:
            self.session_id = sid

        parsed = decode_json(resp_body)
        if isinstance(parsed, dict) and "error" in parsed:
            raise RuntimeError(f"MCP error {parsed['error'].get('code')}: {parsed['error'].get('message')}")
        if isinstance(parsed, dict) and "result" in parsed:
            res_val = parsed["result"]
            return dict(res_val) if isinstance(res_val, dict) else {"value": res_val}
        return dict(parsed) if isinstance(parsed, dict) else {}

    async def initialize(self) -> dict[str, Any]:
        """Perform initialize protocol handshake."""
        return await self.call("initialize", {"protocolVersion": self.protocol_version})

    async def list_tools(self, cursor: str | None = None) -> dict[str, Any]:
        """Retrieve list of registered tools."""
        params = {"cursor": cursor} if cursor else {}
        return await self.call("tools/list", params)

    async def call_tool(
        self,
        name: str,
        arguments: dict[str, Any] | None = None,
        _meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a tool call on the remote server."""
        params: dict[str, Any] = {"name": name, "arguments": arguments or {}}
        if _meta:
            params["_meta"] = _meta
        return await self.call("tools/call", params)

    async def list_resources(self, cursor: str | None = None) -> dict[str, Any]:
        """Retrieve list of static resources."""
        params = {"cursor": cursor} if cursor else {}
        return await self.call("resources/list", params)

    async def read_resource(self, uri: str) -> dict[str, Any]:
        """Read content of a resource URI."""
        return await self.call("resources/read", {"uri": uri})

    async def list_prompts(self, cursor: str | None = None) -> dict[str, Any]:
        """Retrieve list of prompt templates."""
        params = {"cursor": cursor} if cursor else {}
        return await self.call("prompts/list", params)

    async def get_prompt(self, name: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
        """Instantiate a prompt template."""
        return await self.call("prompts/get", {"name": name, "arguments": arguments or {}})

    async def list_skills(self) -> dict[str, Any]:
        """Retrieve list of available progressive skills."""
        return await self.call("skills/list", {})

    async def get_skill(self, name: str) -> dict[str, Any]:
        """Retrieve full details of a specific skill."""
        return await self.call("skills/get", {"name": name})


__all__ = [
    "MCPStreamableHttpClient",
    "decode_mcp_header_value",
    "encode_mcp_header_value",
    "mcp_request_headers",
]
