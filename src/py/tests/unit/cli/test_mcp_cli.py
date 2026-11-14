"""Unit tests for the vtb ai mcp bridge CLI and MCP JSON-RPC observability extraction."""

import io
import json
import sys
from collections.abc import AsyncIterator
from types import ModuleType, SimpleNamespace
from typing import Any

import anyio
import pytest
from click.testing import CliRunner

from vertebra._vertebra import AsyncGcpIdTokenCache, GcpIdTokenSnapshot
from vertebra.agent.core.context import AgentContext, CallerIdentity
from vertebra.agent.core.schema import tool
from vertebra.agent.mcp.bridge import BridgeResponse, OidcTokenSource, run_stdio_http_bridge
from vertebra.agent.mcp.client import mcp_request_headers
from vertebra.agent.mcp.models import MCPConfig
from vertebra.agent.mcp.service import MCPCore
from vertebra.agent.mcp.stdio import run_stdio_server
from vertebra.agent.plugin import AgentPlugin, AgentPluginConfig
from vertebra.cli.ai import (
    extract_mcp_headers_from_jsonrpc,
    mcp,
    resolve_app_mcp_core,
    run_mcp_stdio_bridge,
)
from vertebra.framework._app import App
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
    service = MCPCore()
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


def test_vtb_mcp_serve_cli_discovers_app_and_rejects_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify vtb ai mcp serve discovers app MCPCore, removes --name, and fails loud when absent."""
    captured: dict[str, Any] = {}
    app_core = MCPCore(config=MCPConfig(server_name="discovered-app-mcp"))
    plugin = AgentPlugin(AgentPluginConfig(mcp=MCPConfig(server_name="discovered-app-mcp")))
    plugin.mcp_service = app_core
    app = App(plugins=[plugin])

    async def fake_stdio_runner(service: MCPCore) -> int:
        captured["service"] = service
        return 0

    mod_name = "vertebra_test_serve_mod"
    fake_mod = ModuleType(mod_name)
    fake_mod.app = app
    fake_mod.plain_app = App()
    monkeypatch.setitem(sys.modules, mod_name, fake_mod)
    monkeypatch.setattr("vertebra.cli.ai.run_stdio_server", fake_stdio_runner)

    runner = CliRunner()
    name_rejected = runner.invoke(mcp, ["serve", "--name", "custom-mcp"])
    assert name_rejected.exit_code == 2

    res_ok = runner.invoke(mcp, ["serve", "--app", f"{mod_name}:app"])
    assert res_ok.exit_code == 0
    assert captured["service"] is app_core

    res_missing = runner.invoke(mcp, ["serve", "--app", f"{mod_name}:plain_app"])
    assert res_missing.exit_code == 1
    assert "does not expose an MCP service" in res_missing.output


def test_resolve_app_mcp_core_variants() -> None:
    """Verify resolve_app_mcp_core discovers MCPCore across carriers and returns None when missing."""
    core = MCPCore(config=MCPConfig(server_name="carrier-mcp"))
    starlette_like = SimpleNamespace(state=SimpleNamespace(mcp_core=core))
    flask_like = SimpleNamespace(extensions={"vertebra_mcp_app": SimpleNamespace(core=core)})
    assert resolve_app_mcp_core(core) is core
    assert resolve_app_mcp_core(SimpleNamespace(core=core)) is core
    assert resolve_app_mcp_core(starlette_like) is core
    assert resolve_app_mcp_core(flask_like) is core
    assert resolve_app_mcp_core(App()) is None


def test_vtb_mcp_serve_litestar_delegation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify vtb ai mcp serve delegates Litestar apps with LitestarMCP plugin to litestar_mcp stdio."""
    called: list[Any] = []
    litestar_plugin = type("LitestarMCP", (), {})()
    litestar_plugin.__class__.__module__ = "litestar_mcp.plugin"
    fake_litestar_app = SimpleNamespace(state={"litestar_mcp_plugin": litestar_plugin}, plugins=(litestar_plugin,))

    async def fake_litestar_run_stdio(target_app: Any) -> int:
        called.append(target_app)
        return 0

    mod_name = "vertebra_test_litestar_serve_mod"
    fake_mod = ModuleType(mod_name)
    fake_mod.app = fake_litestar_app
    fake_lmcp_stdio = ModuleType("litestar_mcp.mcp.stdio")
    fake_lmcp_stdio.run_stdio = fake_litestar_run_stdio
    monkeypatch.setitem(sys.modules, mod_name, fake_mod)
    monkeypatch.setattr(
        "vertebra.cli.ai.ensure_module",
        lambda name, **_kw: fake_lmcp_stdio if name == "litestar_mcp.mcp.stdio" else None,
    )

    runner = CliRunner()
    result = runner.invoke(mcp, ["serve", "--app", f"{mod_name}:app"])
    assert result.exit_code == 0
    assert called == [fake_litestar_app]


@pytest.mark.anyio
async def test_run_stdio_server_incremental_pre_eof_and_cancellation() -> None:
    """Verify stdio streams replies before stdin EOF and cancels in-flight requests via notifications/cancelled."""
    service = MCPCore()
    slow_started = anyio.Event()
    release_stdin = anyio.Event()
    first_replied = anyio.Event()
    slow_finished = False

    @tool(name="fast_ping")
    def fast_ping() -> str:
        return "pong"

    @tool(name="slow_task")
    async def slow_task() -> str:
        nonlocal slow_finished
        slow_started.set()
        await anyio.sleep(5)
        slow_finished = True
        return "completed"

    service.registry.register_tool(fast_ping)
    service.registry.register_tool(slow_task)
    output: list[bytes] = []

    async def stdin_gen() -> AsyncIterator[bytes]:
        yield encode_json_bytes({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "fast_ping"}})
        await first_replied.wait()
        yield encode_json_bytes({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "slow_task"}})
        await slow_started.wait()
        yield encode_json_bytes({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 2}})
        yield (
            b'{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"fast_ping","pad":"'
            + b"x" * 200
            + b'"}}\n'
        )
        await release_stdin.wait()

    async def stdout_sink(raw: bytes) -> None:
        output.append(raw)
        parsed = decode_json(raw)
        if parsed.get("id") == 1:
            first_replied.set()
        if parsed.get("error", {}).get("code") == -32600:
            release_stdin.set()

    rc = await run_stdio_server(service, stdin=stdin_gen(), stdout=stdout_sink, max_message_bytes=160)
    assert rc == 0
    assert slow_finished is False
    decoded_out = [decode_json(item) for item in output]
    assert decoded_out[0]["id"] == 1
    assert all(entry.get("id") != 2 for entry in decoded_out)
    assert decoded_out[1]["error"]["code"] == -32600


@pytest.mark.anyio
async def test_run_stdio_server_legacy_notification_relay_and_eof_cleanup() -> None:
    """Verify stdio initialize relays session progress notifications to stdout and deletes session at EOF."""
    service = MCPCore()
    init_done = anyio.Event()
    notification_seen = anyio.Event()
    output: list[bytes] = []

    @tool(name="progress_job")
    async def progress_job() -> str:
        ctx = AgentContext.current()
        assert ctx is not None
        await ctx.progress_reporter.report(1, 2, "halfway")
        await notification_seen.wait()
        return "done"

    service.registry.register_tool(progress_job)

    async def stdin_gen() -> AsyncIterator[bytes]:
        yield encode_json_bytes(
            {"jsonrpc": "2.0", "id": 10, "method": "initialize", "params": {"protocolVersion": "2025-11-25"}}
        )
        await init_done.wait()
        yield encode_json_bytes(
            {
                "jsonrpc": "2.0",
                "id": 11,
                "method": "tools/call",
                "params": {"name": "progress_job", "_meta": {"progressToken": "tok-1"}},
            }
        )

    async def stdout_sink(raw: bytes) -> None:
        output.append(raw)
        parsed = decode_json(raw)
        if parsed.get("id") == 10:
            init_done.set()
        if parsed.get("method") == "notifications/progress":
            notification_seen.set()

    ctx = AgentContext(identity=CallerIdentity(user_id="stdio-user"))
    rc = await run_stdio_server(service, stdin=stdin_gen(), stdout=stdout_sink, context=ctx)
    assert rc == 0
    methods = [decode_json(item).get("method") for item in output]
    assert "notifications/progress" in methods
    created_sid = ctx.metadata["mcp_created_session_id"]
    assert await service.session_store.get(created_sid, "default:stdio-user") is None


def test_mcp_request_headers_omits_protocol_version_when_none() -> None:
    """Verify mcp_request_headers omits MCP-Protocol-Version when protocol_version is None."""
    headers = mcp_request_headers(
        b'{"jsonrpc":"2.0","id":1,"method":"initialize"}',
        protocol_version=None,
    )
    assert "MCP-Protocol-Version" not in headers
    assert headers["Mcp-Method"] == "initialize"


@pytest.mark.parametrize("stop_status", [405, 404])
def test_bridge_dual_mode_legacy_sse_reconnect_and_eof_delete(stop_status: int) -> None:
    """Verify bridge omits MCP-Protocol-Version on initialize, relays GET SSE with Last-Event-ID, stops on 404/405, and DELETEs at EOF."""
    calls: list[tuple[str, dict[str, str], bytes]] = []
    get_count = 0
    sse_relayed = anyio.Event()
    stop_seen = anyio.Event()

    class _FakeDualModeTransport:
        def request(
            self,
            method: str,
            url: str,
            headers: dict[str, str],
            body: bytes,
            timeout_s: float,
        ) -> BridgeResponse:
            nonlocal get_count
            calls.append((method, dict(headers), body))
            if method == "POST":
                decoded = decode_json(body)
                if decoded["method"] == "initialize":
                    return BridgeResponse(
                        status=200,
                        headers={"Mcp-Session-Id": "sess-legacy-1", "MCP-Protocol-Version": "2025-11-25"},
                        body=encode_json_bytes(
                            {"jsonrpc": "2.0", "id": decoded["id"], "result": {"protocolVersion": "2025-11-25"}}
                        ),
                    )
                if decoded["method"] == "tools/list":
                    return BridgeResponse(
                        status=200,
                        headers={"MCP-Protocol-Version": "2025-11-25"},
                        body=encode_json_bytes({"jsonrpc": "2.0", "id": decoded["id"], "result": {"tools": []}}),
                    )
                return BridgeResponse(status=202, headers={}, body=b"")
            if method == "GET":
                get_count += 1
                if get_count == 1:
                    return BridgeResponse(
                        status=200,
                        headers={"Content-Type": "text/event-stream"},
                        body=b'id: ev-1\ndata: {"jsonrpc":"2.0","method":"notifications/tools/list_changed"}\n\n',
                    )
                stop_seen.set()
                return BridgeResponse(status=stop_status, headers={}, body=b"")
            return BridgeResponse(status=204, headers={}, body=b"")

    out_frames: list[bytes] = []

    async def stdin_gen() -> AsyncIterator[bytes]:
        yield encode_json_bytes(
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-11-25"}}
        )
        yield b'{"jsonrpc":"2.0","method":"notifications/initialized"}\n'
        await sse_relayed.wait()
        await stop_seen.wait()
        yield b'{"jsonrpc":"2.0","id":2,"method":"tools/list"}\n'

    async def stdout_sink(raw: bytes) -> None:
        out_frames.append(raw)
        if decode_json(raw).get("method") == "notifications/tools/list_changed":
            sse_relayed.set()

    rc = run_stdio_http_bridge(
        url="https://agent.example.com/mcp",
        stdin=stdin_gen(),
        stdout=stdout_sink,
        transport=_FakeDualModeTransport(),
        reconnect_base_delay_s=0.001,
    )
    assert rc == 0
    post_calls = [c for c in calls if c[0] == "POST"]
    get_calls = [c for c in calls if c[0] == "GET"]
    del_calls = [c for c in calls if c[0] == "DELETE"]
    assert "MCP-Protocol-Version" not in post_calls[0][1]
    assert post_calls[1][1]["MCP-Protocol-Version"] == "2025-11-25"
    assert len(get_calls) == 2
    assert get_calls[1][1]["Last-Event-ID"] == "ev-1"
    assert len(del_calls) == 1
    assert del_calls[0][1]["Mcp-Session-Id"] == "sess-legacy-1"
    assert any(decode_json(item).get("method") == "notifications/tools/list_changed" for item in out_frames)


@pytest.mark.parametrize("double_401", [False, True])
def test_bridge_oidc_token_source_401_retry_and_exhaustion(double_401: bool) -> None:
    """Verify OidcTokenSource mints tokens via AsyncGcpIdTokenCache, retries once on 401, and emits -32001 on second 401."""
    invalidations: list[tuple[str, int]] = []
    seen_tokens: list[str] = []

    class _FakeIdCache:
        def __init__(self) -> None:
            self._gen = 0

        def needs_refresh(self, _aud: str) -> bool:
            return False

        def invalidate(self, audience: str, failed_generation: int) -> None:
            invalidations.append((audience, failed_generation))

        async def get_token(self, audience: str) -> GcpIdTokenSnapshot:
            self._gen += 1
            return GcpIdTokenSnapshot(
                id_token=f"oidc-tok-gen-{self._gen}",
                audience=audience,
                expires_at_unix_ms=1_900_000_000_000,
                generation=self._gen,
            )

    class _AuthTransport:
        def request(
            self,
            _method: str,
            _url: str,
            headers: dict[str, str],
            _body: bytes,
            _timeout_s: float,
        ) -> BridgeResponse:
            seen_tokens.append(headers.get("Authorization", ""))
            if len(seen_tokens) == 1 or double_401:
                return BridgeResponse(status=401, headers={}, body=b"unauthorized")
            return BridgeResponse(
                status=200,
                headers={},
                body=b'{"jsonrpc":"2.0","id":9,"result":{"ok":true}}',
            )

    source = OidcTokenSource("https://service.run.app", cache=Any if False else _FakeIdCache())
    stdin_buf = io.StringIO('{"jsonrpc":"2.0","id":9,"method":"tools/list"}\n')
    stdout_buf = io.StringIO()

    rc = run_stdio_http_bridge(
        url="https://service.run.app/mcp",
        token_source=source,
        stdin=stdin_buf,
        stdout=stdout_buf,
        transport=_AuthTransport(),
    )
    assert rc == 0
    assert seen_tokens == ["Bearer oidc-tok-gen-1", "Bearer oidc-tok-gen-2"]
    assert invalidations == [("https://service.run.app", 1)]
    out_obj = decode_json(stdout_buf.getvalue().strip().encode("utf-8"))
    if double_401:
        assert out_obj["id"] == 9
        assert out_obj["error"]["code"] == -32001
        assert out_obj["error"]["message"] == "Bridge authentication failed (HTTP 401)"
    else:
        assert out_obj["result"] == {"ok": True}


def test_vtb_mcp_bridge_audience_and_mutual_exclusion(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify --audience forwards to bridge and rejects --token + --audience with exit code 2."""
    captured: dict[str, Any] = {}

    def fake_bridge(**kwargs: Any) -> int:
        captured.update(kwargs)
        return 0

    monkeypatch.setattr("vertebra.cli.ai.run_mcp_stdio_bridge", fake_bridge)
    runner = CliRunner()

    conflict_res = runner.invoke(
        mcp,
        [
            "bridge",
            "--url",
            "https://agent.example.com/mcp",
            "--token",
            "t1",
            "--audience",
            "https://agent.example.com",
        ],
    )
    assert conflict_res.exit_code == 2
    assert "mutually exclusive" in conflict_res.output

    aud_res = runner.invoke(
        mcp,
        ["bridge", "--url", "https://agent.example.com/mcp", "--audience", "https://agent.example.com"],
    )
    assert aud_res.exit_code == 0
    assert captured["audience"] == "https://agent.example.com"
    assert captured["bearer_token"] is None


def test_oidc_token_source_real_cache_audience_validation() -> None:
    """Verify OidcTokenSource rejects invalid empty/whitespace audiences using AsyncGcpIdTokenCache."""
    cache = AsyncGcpIdTokenCache({"access_token": "ignored"}, allow_ambient_adc=False) if False else None
    _ = (json, cache)
    with pytest.raises(ValueError):
        OidcTokenSource("   ", cache=AsyncGcpIdTokenCache(allow_ambient_adc=False))
