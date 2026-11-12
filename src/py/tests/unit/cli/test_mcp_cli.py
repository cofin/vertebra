"""Unit tests for the vtb ai mcp bridge CLI and MCP JSON-RPC observability extraction."""

import io
from collections.abc import AsyncIterator
from typing import Any

import pytest
from click.testing import CliRunner

from vertebra.agent.core.context import AgentContext, CallerIdentity
from vertebra.agent.core.schema import tool
from vertebra.agent.mcp.service import MCPService
from vertebra.agent.mcp.stdio import run_stdio_server
from vertebra.cli.ai import (
    extract_mcp_headers_from_jsonrpc,
    mcp,
    run_mcp_stdio_bridge,
)
from vertebra.utils.serializers import decode_json, encode_json_bytes


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

    monkeypatch.setattr("vertebra.cli.ai.run_mcp_stdio_bridge", fake_bridge)
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


def test_extract_mcp_headers_resources_and_tasks() -> None:
    """Verify resources/read extracts uri and tasks/get extracts taskId into Mcp-Name."""
    res_line = b'{"jsonrpc":"2.0","id":2,"method":"resources/read","params":{"uri":"file:///data.txt"}}'
    res_headers = extract_mcp_headers_from_jsonrpc(res_line)
    assert res_headers["Mcp-Method"] == "resources/read"
    assert res_headers["Mcp-Name"] == "file:///data.txt"

    task_line = b'{"jsonrpc":"2.0","id":3,"method":"tasks/get","params":{"taskId":"task_12345"}}'
    task_headers = extract_mcp_headers_from_jsonrpc(task_line)
    assert task_headers["Mcp-Method"] == "tasks/get"
    assert task_headers["Mcp-Name"] == "task_12345"


def test_extract_mcp_headers_base64_non_ascii() -> None:
    """Verify non-ASCII parameter names are encoded using RFC 2047 format."""
    line = '{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"тест_инструмент"}}'.encode()
    headers = extract_mcp_headers_from_jsonrpc(line)
    assert headers["Mcp-Method"] == "tools/call"
    assert headers["Mcp-Name"].startswith("=?utf-8?B?")
    assert headers["Mcp-Name"].endswith("?=")


def test_run_mcp_stdio_bridge_sse_unwrapping() -> None:
    """Verify SSE text/event-stream payloads are unpacked and written line-by-line to stdout."""
    stdin_buf = io.StringIO('{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"echo"}}\n')
    stdout_buf = io.StringIO()

    def fake_transport(url: str, headers: dict[str, str], body: bytes) -> tuple[int, dict[str, str], bytes]:
        sse_payload = b'event: message\ndata: {"jsonrpc":"2.0","id":1,"result":"hello"}\n\n'
        return 200, {"Content-Type": "text/event-stream"}, sse_payload

    exit_code = run_mcp_stdio_bridge(
        url="https://agent.example.com/mcp",
        stdin=stdin_buf,
        stdout=stdout_buf,
        transport=fake_transport,
    )
    assert exit_code == 0
    assert stdout_buf.getvalue().strip() == '{"jsonrpc":"2.0","id":1,"result":"hello"}'


def test_run_mcp_stdio_bridge_session_id_propagation() -> None:
    """Verify Mcp-Session-Id from response headers is propagated to subsequent requests."""
    stdin_buf = io.StringIO(
        '{"jsonrpc":"2.0","id":1,"method":"tools/list"}\n{"jsonrpc":"2.0","id":2,"method":"prompts/list"}\n'
    )
    stdout_buf = io.StringIO()
    received_headers: list[dict[str, str]] = []

    def fake_transport(url: str, headers: dict[str, str], body: bytes) -> tuple[int, dict[str, str], bytes]:
        received_headers.append(dict(headers))
        return 200, {"Mcp-Session-Id": "sess-xyz-987"}, b'{"jsonrpc":"2.0","id":1,"result":{}}'

    exit_code = run_mcp_stdio_bridge(
        url="https://agent.example.com/mcp",
        stdin=stdin_buf,
        stdout=stdout_buf,
        transport=fake_transport,
    )
    assert exit_code == 0
    assert len(received_headers) == 2
    assert "Mcp-Session-Id" not in received_headers[0]
    assert received_headers[1]["Mcp-Session-Id"] == "sess-xyz-987"


def test_run_mcp_stdio_bridge_max_message_bytes_enforcement() -> None:
    """Verify messages exceeding max_message_bytes trigger a -32600 error response without transport call."""
    large_arg = "x" * 2000
    stdin_buf = io.StringIO(f'{{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{{"data":"{large_arg}"}}}}\n')
    stdout_buf = io.StringIO()
    transport_called = False

    def fake_transport(url: str, headers: dict[str, str], body: bytes) -> bytes:
        nonlocal transport_called
        transport_called = True
        return b"{}"

    exit_code = run_mcp_stdio_bridge(
        url="https://agent.example.com/mcp",
        max_message_bytes=500,
        stdin=stdin_buf,
        stdout=stdout_buf,
        transport=fake_transport,
    )
    assert exit_code == 0
    assert not transport_called
    output = stdout_buf.getvalue()
    assert "-32600" in output


@pytest.mark.anyio
@pytest.mark.parametrize("explicit", [False, True])
async def test_mcp_stdio_context_session(explicit: bool) -> None:
    """A stdio session reuses one caller context for every dispatched frame."""
    service = MCPService()
    seen: list[AgentContext] = []

    @tool(name="whoami")
    def whoami() -> str:
        context = AgentContext.current()
        assert context is not None
        seen.append(context)
        return str(context.identity.user_id) if explicit else str(context.session_id)

    service.registry.register_tool(whoami)
    context = AgentContext(identity=CallerIdentity(user_id="cli"))

    async def stdin() -> AsyncIterator[bytes]:
        for request_id in (1, 2):
            yield encode_json_bytes(
                {"jsonrpc": "2.0", "id": request_id, "method": "tools/call", "params": {"name": "whoami"}}
            )

    output: list[bytes] = []

    async def stdout(raw: bytes) -> None:
        output.append(raw)

    if explicit:
        result = await run_stdio_server(service, stdin=stdin(), stdout=stdout, context=context)
    else:
        result = await run_stdio_server(service, stdin=stdin(), stdout=stdout)
    assert result == 0
    assert len(seen) == 2
    assert seen[0] is seen[1]
    if explicit:
        assert seen[0] is context
    values = [decode_json(raw)["result"]["content"][0]["text"] for raw in output]
    assert values[0] == values[1]
    assert values[0] == "cli" if explicit else values[0].startswith("stdio-")
