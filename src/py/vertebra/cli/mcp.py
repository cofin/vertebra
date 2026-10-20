"""MCP stdio-to-HTTP bridge CLI command (vtb mcp bridge)."""

import os
import sys
import urllib.request
from collections.abc import Callable
from typing import TextIO

import click

from vertebra.utils.serializers import decode_json

__all__ = (
    "bridge",
    "extract_mcp_headers_from_jsonrpc",
    "mcp",
    "run_mcp_stdio_bridge",
)

_MCP_PROTOCOL_VERSION = "2026-07-28"


def extract_mcp_headers_from_jsonrpc(payload: bytes) -> dict[str, str]:
    """Extract Mcp-Method, Mcp-Name, and MCP-Protocol-Version headers from a JSON-RPC 2.0 frame."""
    headers: dict[str, str] = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": _MCP_PROTOCOL_VERSION,
    }
    try:
        decoded = decode_json(payload)
    except Exception:
        return headers

    if isinstance(decoded, dict):
        method = decoded.get("method")
        if isinstance(method, str) and method:
            headers["Mcp-Method"] = method
        params = decoded.get("params")
        if isinstance(params, dict):
            name = params.get("name")
            if isinstance(name, str) and name:
                headers["Mcp-Name"] = name
    return headers


def run_mcp_stdio_bridge(
    *,
    url: str,
    bearer_token: str | None = None,
    timeout_s: float = 30.0,
    stdin: TextIO | None = None,
    stdout: TextIO | None = None,
    transport: Callable[[str, dict[str, str], bytes], bytes] | None = None,
) -> int:
    """Forward newline-delimited JSON-RPC 2.0 frames from stdin to an HTTP MCP endpoint."""
    in_stream = stdin if stdin is not None else sys.stdin
    out_stream = stdout if stdout is not None else sys.stdout
    send_fn = (
        transport
        if transport is not None
        else lambda target_url, hdrs, raw: _default_http_transport(
            target_url,
            hdrs,
            raw,
            timeout_s=timeout_s,
        )
    )

    resolved_token = bearer_token or os.environ.get("VERTEBRA_MCP_TOKEN")

    for raw_line in in_stream:
        stripped = raw_line.strip()
        if not stripped:
            continue
        payload = stripped.encode("utf-8")
        headers = extract_mcp_headers_from_jsonrpc(payload)
        if resolved_token:
            headers["Authorization"] = f"Bearer {resolved_token}"
        response_bytes = send_fn(url, headers, payload)
        if response_bytes:
            out_stream.write(response_bytes.decode("utf-8").strip() + "\n")
            out_stream.flush()
    return 0


@click.group()
def mcp() -> None:
    """Model Context Protocol (MCP) utilities and stdio bridge."""


@mcp.command()
@click.option("--url", required=True, help="Remote MCP HTTP/SSE endpoint URL")
@click.option("--token", "bearer_token", default=None, help="Bearer / OAuth2 / OIDC access token")
@click.option("--timeout", "timeout_s", type=float, default=30.0, help="Per-request HTTP timeout in seconds")
def bridge(url: str, bearer_token: str | None, timeout_s: float) -> None:
    """Bridge local newline-delimited JSON-RPC 2.0 stdio to a remote HTTP/SSE MCP endpoint."""
    exit_code = run_mcp_stdio_bridge(url=url, bearer_token=bearer_token, timeout_s=timeout_s)
    if exit_code != 0:
        sys.exit(exit_code)


def _default_http_transport(
    url: str,
    headers: dict[str, str],
    body: bytes,
    *,
    timeout_s: float = 30.0,
) -> bytes:
    """POST a JSON-RPC 2.0 message to an HTTP MCP endpoint and return the response payload."""
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return bytes(resp.read())
