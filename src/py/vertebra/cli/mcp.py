"""MCP stdio-to-HTTP bridge and server CLI commands (vtb mcp bridge, vtb mcp serve)."""

import sys
from typing import Any, TextIO

import anyio
import click

from vertebra.agent.mcp.bridge import run_stdio_http_bridge
from vertebra.agent.mcp.client import mcp_request_headers
from vertebra.agent.mcp.service import MCPService
from vertebra.agent.mcp.stdio import run_stdio_server

__all__ = (
    "bridge",
    "extract_mcp_headers_from_jsonrpc",
    "mcp",
    "run_mcp_stdio_bridge",
    "serve",
)


def extract_mcp_headers_from_jsonrpc(payload: bytes) -> dict[str, str]:
    """Extract Mcp-Method, Mcp-Name, and MCP-Protocol-Version headers from a JSON-RPC frame."""
    return mcp_request_headers(payload)


def run_mcp_stdio_bridge(
    *,
    url: str,
    bearer_token: str | None = None,
    timeout_s: float = 30.0,
    max_message_bytes: int = 4_194_304,
    stdin: TextIO | None = None,
    stdout: TextIO | None = None,
    transport: Any = None,
) -> int:
    """Forward newline-delimited JSON-RPC frames from stdin to an HTTP MCP endpoint."""
    return run_stdio_http_bridge(
        url=url,
        bearer_token=bearer_token,
        timeout_s=timeout_s,
        max_message_bytes=max_message_bytes,
        stdin=stdin,
        stdout=stdout,
        transport=transport,
    )


@click.group()
def mcp() -> None:
    """Model Context Protocol (MCP) utilities, server, and stdio bridge."""


@mcp.command()
@click.option("--url", required=True, help="Remote MCP HTTP/SSE endpoint URL")
@click.option("--token", "bearer_token", default=None, help="Bearer / OAuth2 / OIDC access token")
@click.option("--timeout", "timeout_s", type=float, default=30.0, help="Per-request HTTP timeout in seconds")
def bridge(url: str, bearer_token: str | None, timeout_s: float) -> None:
    """Bridge local newline-delimited JSON-RPC 2.0 stdio to a remote HTTP/SSE MCP endpoint."""
    exit_code = run_mcp_stdio_bridge(url=url, bearer_token=bearer_token, timeout_s=timeout_s)
    if exit_code != 0:
        sys.exit(exit_code)


@mcp.command()
@click.option("--name", default="vertebra-mcp", help="Server name")
def serve(name: str) -> None:
    """Run an in-process MCP service over standard input/output (stdio)."""
    service = MCPService()
    exit_code = anyio.run(lambda: run_stdio_server(service))
    if exit_code != 0:
        sys.exit(exit_code)
