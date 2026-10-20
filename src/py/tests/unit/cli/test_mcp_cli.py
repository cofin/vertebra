"""Unit tests for the vtb mcp bridge CLI and MCP JSON-RPC observability extraction."""

import io
from typing import Any

from click.testing import CliRunner

from vertebra.cli.mcp import (
    extract_mcp_headers_from_jsonrpc,
    mcp,
    run_mcp_stdio_bridge,
)


def test_extract_mcp_headers_from_jsonrpc() -> None:
    """Verify JSON-RPC 2.0 method and params.name are extracted into Mcp-Method and Mcp-Name."""
    line = '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_clusters"}}'
    headers = extract_mcp_headers_from_jsonrpc(line.encode("utf-8"))
    assert headers["Mcp-Method"] == "tools/call"
    assert headers["Mcp-Name"] == "list_clusters"
    assert headers["MCP-Protocol-Version"] == "2026-07-28"


def test_run_mcp_stdio_bridge_forwards_stdin_to_transport() -> None:
    """Verify run_mcp_stdio_bridge reads JSON-RPC lines from stdin and writes responses to stdout."""
    stdin_buf = io.StringIO('{"jsonrpc":"2.0","id":42,"method":"prompts/get","params":{"name":"review"}}\n')
    stdout_buf = io.StringIO()
    captured_calls: list[tuple[str, dict[str, str], bytes]] = []

    def fake_transport(url: str, headers: dict[str, str], body: bytes) -> bytes:
        captured_calls.append((url, headers, body))
        return b'{"jsonrpc":"2.0","id":42,"result":{"messages":[]}}'

    exit_code = run_mcp_stdio_bridge(
        url="https://agent.example.com/mcp",
        bearer_token="secret-token",
        stdin=stdin_buf,
        stdout=stdout_buf,
        transport=fake_transport,
    )
    assert exit_code == 0
    assert len(captured_calls) == 1
    called_url, called_headers, _called_body = captured_calls[0]
    assert called_url == "https://agent.example.com/mcp"
    assert called_headers["Authorization"] == "Bearer secret-token"
    assert called_headers["Mcp-Method"] == "prompts/get"
    assert called_headers["Mcp-Name"] == "review"
    assert stdout_buf.getvalue().strip() == '{"jsonrpc":"2.0","id":42,"result":{"messages":[]}}'


def test_vtb_mcp_bridge_cli_command(monkeypatch: Any) -> None:
    """Verify vtb mcp bridge CLI parses --url and --token and invokes the stdio bridge."""
    invoked: dict[str, Any] = {}

    def fake_bridge(*, url: str, bearer_token: str | None, timeout_s: float = 30.0) -> int:
        invoked["url"] = url
        invoked["bearer_token"] = bearer_token
        invoked["timeout_s"] = timeout_s
        return 0

    monkeypatch.setattr("vertebra.cli.mcp.run_mcp_stdio_bridge", fake_bridge)
    runner = CliRunner()
    result = runner.invoke(
        mcp,
        [
            "bridge",
            "--url",
            "https://agent.example.com/mcp",
            "--token",
            "my-jwt",
            "--timeout",
            "15",
        ],
    )
    assert result.exit_code == 0
    assert invoked == {
        "url": "https://agent.example.com/mcp",
        "bearer_token": "my-jwt",
        "timeout_s": 15.0,
    }
