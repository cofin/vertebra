"""Standard I/O to HTTP bridge for Model Context Protocol 2026-07-28 services.

This module provides run_stdio_http_bridge, bridging stdin/stdout JSON-RPC lines
to an HTTP MCP endpoint with SSE stream unwrapping and session persistence.
"""

import inspect
import os
import sys
from collections.abc import Awaitable, Callable
from typing import Any, TextIO

import anyio

from vertebra.agent.mcp.client import (
    _default_http_request,
    mcp_request_headers,
)
from vertebra.utils.serializers import encode_json_bytes


def _parse_sse_events(data: bytes) -> list[str]:
    """Parse text/event-stream bytes and extract all data payload lines."""
    lines = data.decode("utf-8", errors="replace").splitlines()
    events: list[str] = []
    current_data: list[str] = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            if current_data:
                events.append("\n".join(current_data))
                current_data = []
            continue
        if stripped.startswith("data:"):
            payload = stripped[5:].strip()
            current_data.append(payload)

    if current_data:
        events.append("\n".join(current_data))
    return events


async def _resolve_awaitable(coro: Any) -> Any:
    return await coro


def run_stdio_http_bridge(
    *,
    url: str,
    bearer_token: str | None = None,
    token_provider: Callable[[], Awaitable[str | None] | str | None] | None = None,
    timeout_s: float = 30.0,
    max_message_bytes: int = 4_194_304,
    stdin: TextIO | None = None,
    stdout: TextIO | None = None,
    transport: Callable[..., Any] | None = None,
) -> int:
    """Forward newline-delimited JSON-RPC frames to HTTP endpoint and write responses to stdout."""
    in_stream = stdin if stdin is not None else sys.stdin
    out_stream = stdout if stdout is not None else sys.stdout

    active_session_id: str | None = None
    default_token = bearer_token or os.environ.get("VERTEBRA_MCP_TOKEN")

    for raw_line in in_stream:
        stripped = raw_line.strip()
        if not stripped:
            continue
        payload = stripped.encode("utf-8")

        if len(payload) > max_message_bytes:
            err_frame = encode_json_bytes(
                {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {
                        "code": -32600,
                        "message": f"Message exceeded max size of {max_message_bytes} bytes",
                    },
                }
            ).decode("utf-8")
            out_stream.write(f"{err_frame}\n")
            out_stream.flush()
            continue

        current_token = default_token
        if token_provider is not None:
            resolved = token_provider()
            if inspect.isawaitable(resolved):
                resolved = anyio.run(_resolve_awaitable, resolved)
            if resolved:
                current_token = resolved

        headers = mcp_request_headers(
            payload,
            session_id=active_session_id,
            bearer_token=current_token,
        )

        resp_headers: dict[str, str] = {}
        if transport is not None:
            resp = transport(url, headers, payload)
            if inspect.isawaitable(resp):
                resp = anyio.run(_resolve_awaitable, resp)
            if isinstance(resp, tuple):
                _, resp_headers, resp_bytes = resp
            else:
                resp_bytes = resp
        else:
            _, resp_headers, resp_bytes = _default_http_request(url, headers, payload, timeout_s)

        for k, v in resp_headers.items():
            if k.lower() == "mcp-session-id":
                active_session_id = v
                break

        if len(resp_bytes) > max_message_bytes:
            err_frame = encode_json_bytes(
                {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {
                        "code": -32600,
                        "message": f"Response exceeded max size of {max_message_bytes} bytes",
                    },
                }
            ).decode("utf-8")
            out_stream.write(f"{err_frame}\n")
            out_stream.flush()
            continue

        c_type = ""
        for k, v in resp_headers.items():
            if k.lower() == "content-type":
                c_type = v
                break

        if "text/event-stream" in c_type:
            events = _parse_sse_events(resp_bytes)
            for ev in events:
                out_stream.write(f"{ev}\n")
            out_stream.flush()
        else:
            resp_str = resp_bytes.decode("utf-8", errors="replace").strip()
            if resp_str:
                out_stream.write(f"{resp_str}\n")
                out_stream.flush()

    return 0


__all__ = [
    "run_stdio_http_bridge",
]
