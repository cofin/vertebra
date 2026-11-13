"""Unit and property tests for agentic core, schema builder, guards, and telemetry."""

import ast
import dataclasses
import inspect
from collections.abc import Mapping
from types import SimpleNamespace
from typing import Annotated, Any, Literal, TypedDict, cast

import anyio
import msgspec
import pytest
from starlette.requests import Request

import vertebra.agent as agent_pkg
import vertebra.agent.core.schema as schema_mod
from vertebra.agent.core.context import (
    AgentContext,
    CallerIdentity,
    bind_agent_context,
    current_agent_context,
    reset_agent_context,
)
from vertebra.agent.core.cursor import (
    decode_cursor,
    encode_cursor,
    paginate_sequence,
)
from vertebra.agent.core.guards import (
    BudgetExceededError,
    GuardChain,
    PermissionDeniedError,
    RateLimitExceededError,
    RequireAuthenticated,
    RequireRoles,
    RequireScopes,
    TokenBucketGuard,
)
from vertebra.agent.core.identity import CallableIdentitySource, MtlsIdentitySource
from vertebra.agent.core.jsonrpc import (
    FRAME_KIND_BATCH_REQUEST,
    FRAME_KIND_NOTIFICATION,
    FRAME_KIND_SINGLE_REQUEST,
    INTERNAL_ERROR,
    INVALID_PARAMS,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
    PARSE_ERROR,
    JSONRPCErrorException,
    JSONRPCRequest,
    JSONRPCRouter,
)
from vertebra.agent.core.schema import (
    SchemaBuilder,
    SchemaDigestCache,
    Tool,
    ToolAnnotations,
    ToolArgumentError,
    ToolCall,
    ToolSpec,
    execute_tool_calls,
    tool,
)
from vertebra.agent.core.sse import (
    SseFrame,
    SseStreamBroker,
)
from vertebra.agent.telemetry import (
    AgentTelemetryRecorder,
)
from vertebra.agent.tools import build_tools
from vertebra.telemetry.genai import GenAiUsage
from vertebra.telemetry.logger import LogSink
from vertebra.telemetry.otel import SpanExporter
from vertebra.utils.serializers import decode_json, encode_json_bytes

try:
    from vertebra import _vertebra
except ImportError:
    _vertebra = None


@pytest.mark.anyio
async def test_jsonrpc_single_batch_and_notification_silence() -> None:
    """Test JSON-RPC single, batch, notification silence, and sensitive error masking."""
    router = JSONRPCRouter()

    router.register("ping", lambda: "pong")
    router.register("echo", lambda message: message)

    def failing_handler() -> None:
        msg = "secret://db_password_123"
        raise RuntimeError(msg)

    router.register("fail", failing_handler)

    req = JSONRPCRequest(method="ping", id=1)
    res = await router.dispatch(req)
    assert res == {"jsonrpc": "2.0", "id": 1, "result": "pong"}

    notif = JSONRPCRequest(method="ping", id=None)
    res_notif = await router.dispatch(notif)
    assert res_notif is None

    fail_req = JSONRPCRequest(method="fail", id=2)
    res_fail = await router.dispatch(fail_req)
    assert res_fail is not None
    assert res_fail["jsonrpc"] == "2.0"
    assert res_fail["id"] == 2
    assert res_fail["error"]["code"] == INTERNAL_ERROR
    assert res_fail["error"]["message"] == "Internal error"
    assert res_fail["error"]["data"] == {"error": "RuntimeError"}
    assert "secret" not in str(res_fail)

    batch_payload = encode_json_bytes(
        [
            {"jsonrpc": "2.0", "method": "ping", "id": 10},
            {"jsonrpc": "2.0", "method": "ping"},
            {"jsonrpc": "2.0", "method": "echo", "params": {"message": "hello"}, "id": 11},
        ]
    )
    batch_res_bytes = await router.dispatch_payload(batch_payload)
    assert batch_res_bytes is not None
    batch_res = decode_json(batch_res_bytes)
    assert isinstance(batch_res, list)
    assert len(batch_res) == 2
    ids = {r["id"] for r in batch_res}
    assert ids == {10, 11}

    all_notif_payload = encode_json_bytes(
        [
            {"jsonrpc": "2.0", "method": "ping"},
            {"jsonrpc": "2.0", "method": "ping"},
        ]
    )
    all_notif_res = await router.dispatch_payload(all_notif_payload)
    assert all_notif_res is None

    malformed_res_bytes = await router.dispatch_payload(b"invalid json")
    assert malformed_res_bytes is not None
    malformed_res = decode_json(malformed_res_bytes)
    assert malformed_res["error"]["code"] == PARSE_ERROR

    invalid_res_bytes = await router.dispatch_payload(encode_json_bytes([]))
    assert invalid_res_bytes is not None
    invalid_res = decode_json(invalid_res_bytes)
    assert invalid_res["error"]["code"] == INVALID_REQUEST

    unknown_res = await router.dispatch(JSONRPCRequest(method="nonexistent", id=99))
    assert unknown_res is not None
    assert unknown_res["error"]["code"] == METHOD_NOT_FOUND

    assert JSONRPCRequest.__struct_config__.forbid_unknown_fields is True
    with pytest.raises(msgspec.ValidationError):
        msgspec.convert({"jsonrpc": "2.0", "method": "ping", "extra": True}, JSONRPCRequest)


def test_cursor_pagination_roundtrip_and_invalid_cursor() -> None:
    """Test cursor pagination round-trip and validation rejection."""
    items = list(range(105))

    page1 = paginate_sequence(items, cursor=None, page_size=50)
    assert len(page1.items) == 50
    assert page1.items[0] == 0
    assert page1.items[-1] == 49
    assert page1.next_cursor is not None

    page2 = paginate_sequence(items, cursor=page1.next_cursor, page_size=50)
    assert len(page2.items) == 50
    assert page2.items[0] == 50
    assert page2.items[-1] == 99
    assert page2.next_cursor is not None

    page3 = paginate_sequence(items, cursor=page2.next_cursor, page_size=50)
    assert len(page3.items) == 5
    assert page3.items[0] == 100
    assert page3.items[-1] == 104
    assert page3.next_cursor is None

    cursor_val = encode_cursor(42)
    assert decode_cursor(cursor_val) == 42

    with pytest.raises(JSONRPCErrorException) as exc_info:
        decode_cursor("invalid-base64-not-prefixed")
    assert exc_info.value.error.code == INVALID_PARAMS

    with pytest.raises(JSONRPCErrorException) as exc_info2:
        decode_cursor("")
    assert exc_info2.value.error.code == INVALID_PARAMS


class UserRecord(msgspec.Struct, frozen=True, gc=False):
    name: str
    age: int


@dataclasses.dataclass
class Location:
    city: str
    zip_code: str


class MetadataDict(TypedDict):
    tag: str
    active: bool


@pytest.mark.anyio
async def test_schema_builder_and_tool_descriptor_validation() -> None:
    """Test SchemaBuilder, @tool decorator, DI stripping, and execution."""
    builder = SchemaBuilder()

    def sample_func(
        query: str,
        user: UserRecord,
        location: Location,
        meta: MetadataDict,
        count: Annotated[int, msgspec.Meta(ge=1)],
        status: Literal["open", "closed"],
        context: AgentContext,
    ) -> str:
        return f"{query}:{user.name}:{location.city}:{count}:{context.identity.tenant_id}"

    schema = builder.build_callable_schema(sample_func)
    assert schema["type"] == "object"
    assert "query" in schema["properties"]
    assert "user" in schema["properties"]
    assert "location" in schema["properties"]
    assert "meta" in schema["properties"]
    assert "count" in schema["properties"]
    assert "status" in schema["properties"]
    assert "context" not in schema["properties"]
    assert "$defs" in schema
    assert "UserRecord" in schema["$defs"]
    assert "Location" in schema["$defs"]

    decorated = tool(name="sample")(sample_func)
    ctx = AgentContext(identity=CallerIdentity(tenant_id="corp-test"))

    result = await decorated.invoke(
        {
            "query": "find",
            "user": {"name": "Alice", "age": 30},
            "location": {"city": "Zurich", "zip_code": "8001"},
            "meta": {"tag": "prod", "active": True},
            "count": 5,
            "status": "open",
        },
        context=ctx,
    )
    assert result == "find:Alice:Zurich:5:corp-test"

    with pytest.raises(ToolArgumentError):
        await decorated.invoke(
            {
                "query": "find",
                "user": {"name": "Alice", "age": 30},
                "location": {"city": "Zurich", "zip_code": "8001"},
                "meta": {"tag": "prod", "active": True},
                "count": "not-an-int",
                "status": "open",
            },
            context=ctx,
        )

    class CalculatorController:
        @tool(name="add")
        def add(self, a: int, b: int) -> int:
            return a + b

    calc = CalculatorController()
    calc_tool = calc.add
    bound_res = await calc_tool.invoke({"a": 10, "b": 25})
    assert bound_res == 35

    calls = [
        ToolCall(
            id="c1",
            name="sample",
            arguments={
                "query": "find",
                "user": {"name": "Bob", "age": 25},
                "location": {"city": "Bern", "zip_code": "3000"},
                "meta": {"tag": "test", "active": False},
                "count": 2,
                "status": "closed",
            },
        ),
        ToolCall(id="c2", name="unknown", arguments={}),
    ]
    tool_results = await execute_tool_calls({"sample": decorated}, calls, context=ctx)
    assert len(tool_results) == 2
    assert not tool_results[0].is_error
    assert tool_results[0].output == "find:Bob:Bern:2:corp-test"
    assert tool_results[1].is_error


def test_sse_replay_buffer_and_stream_broker_resumption() -> None:
    """Test SSE stream broker, replay buffer, and Last-Event-ID reconnection."""
    broker = SseStreamBroker(replay_capacity=10)
    session_id = "sess-123"

    recv_stream, _ = broker.subscribe(session_id)

    broker.publish(session_id, SseFrame(id="1", event="message", data="frame-1"))
    broker.publish(session_id, SseFrame(id="2", event="message", data="frame-2"))
    broker.publish(session_id, SseFrame(id="3", event="message", data="frame-3"))
    broker.publish(session_id, SseFrame(id="4", event="message", data="frame-4"))
    broker.publish(session_id, SseFrame(id="5", event="message", data="frame-5"))

    f1 = recv_stream.receive_nowait()
    f2 = recv_stream.receive_nowait()
    assert f1.id == "1"
    assert f2.id == "2"

    recv_resumed, _ = broker.subscribe(session_id, last_event_id="2")
    replayed: list[SseFrame] = []
    while True:
        try:
            replayed.append(recv_resumed.receive_nowait())
        except anyio.WouldBlock:
            break

    assert len(replayed) == 3
    assert [f.id for f in replayed] == ["3", "4", "5"]

    sample_frame = SseFrame(id="1", event="message", data="frame-1")
    assert SseFrame.__struct_config__.array_like is False
    assert SseFrame.__struct_config__.cache_hash is True
    assert isinstance(decode_json(encode_json_bytes(sample_frame)), dict)
    assert isinstance(hash(sample_frame), int)


@pytest.mark.anyio
async def test_agent_context_identity_and_guard_chain_inheritance() -> None:
    """Test CallerIdentity resolution across headers and hierarchical GuardChain execution."""

    class FakeRequest:
        def __init__(self, headers: dict[str, str], user: Any = None) -> None:
            self.headers = headers
            self.scope = {"user": user, "headers": [(k.encode(), v.encode()) for k, v in headers.items()]}
            self.user = user

    req = FakeRequest(
        headers={
            "x-goog-authenticated-user-email": "accounts.google.com:dev@example.com",
            "x-tenant-id": "acme-corp",
        },
        user={"id": "usr_999"},
    )
    ctx = AgentContext.from_request(req)
    assert ctx.identity.user_id == "usr_999"
    assert ctx.identity.email is None
    assert ctx.identity.tenant_id == "default"
    assert ctx.identity.auth_scheme == "connection"
    assert ctx.identity.owner_key() == "default:usr_999"

    app_guard = RequireAuthenticated()
    controller_guard = RequireScopes("read:agents", "write:agents")
    tool_guard = RequireRoles("admin")

    chain = GuardChain.merge([app_guard], [controller_guard], [tool_guard])

    unauth_ctx = AgentContext(identity=CallerIdentity())
    with pytest.raises(PermissionDeniedError, match="Authentication required"):
        await chain.execute(unauth_ctx)

    partial_ctx = AgentContext(
        identity=CallerIdentity(
            user_id="u1",
            roles=("user",),
            scopes=("read:agents",),
        )
    )
    with pytest.raises(PermissionDeniedError, match="Missing required scopes"):
        await chain.execute(partial_ctx)

    valid_ctx = AgentContext(
        identity=CallerIdentity(
            user_id="u1",
            roles=("admin",),
            scopes=("read:agents", "write:agents"),
        )
    )
    await chain.execute(valid_ctx)


def test_observability_harness_emits_genai_spans_metrics_and_audit_logs() -> None:
    """Test AgentTelemetryRecorder spans, GenAiRecorder metrics, and LogSink audit events."""
    span_exporter = SpanExporter()
    log_sink = LogSink()
    recorder = AgentTelemetryRecorder(span_exporter=span_exporter, log_sink=log_sink)

    with recorder.span_turn("lead-agent", "session-42", "turn-1") as turn_ctx:
        with recorder.span_model("gemini-2.5-flash", agent_name="lead-agent") as model_box:
            model_box["usage"] = GenAiUsage(
                input_tokens=1200,
                output_tokens=400,
                thought_tokens=200,
                cached_input_tokens=300,
            )

        with recorder.span_tool("search_docs", "call-001", agent_name="lead-agent"):
            pass

        with (
            pytest.raises(ValueError, match="tool crashed"),
            recorder.span_tool("failing_tool", "call-002", agent_name="lead-agent"),
        ):
            msg = "tool crashed"
            raise ValueError(msg)

    spans = span_exporter.drain_spans()
    assert len(spans) == 4

    root_trace_id = turn_ctx.trace_id
    assert all(s.trace_id == root_trace_id for s in spans)

    turn_span = next(s for s in spans if s.name == "gen_ai.agent.turn")
    model_span = next(s for s in spans if s.name == "gen_ai.chat")
    tool_span = next(s for s in spans if s.name == "gen_ai.tool.search_docs")
    fail_span = next(s for s in spans if s.name == "gen_ai.tool.failing_tool")

    assert model_span.parent_span_id == turn_span.span_id
    assert tool_span.parent_span_id == turn_span.span_id
    assert fail_span.status_code == 500
    assert fail_span.attributes["error.type"] == "ValueError"

    assert len(recorder.genai_recorder.samples) >= 1
    sample = recorder.genai_recorder.samples[0]
    assert sample.input_tokens == 1200
    assert sample.output_tokens == 400
    assert sample.thought_tokens == 200
    assert sample.cached_input_tokens == 300
    assert sample.usd_cost > 0.0

    duration_hist = recorder.genai_recorder.duration_histogram("chat")
    assert duration_hist.count >= 1

    audit_lines = [line for chunk in log_sink.drain() for line in chunk.splitlines() if line.strip()]
    assert len(audit_lines) == 4
    for raw in audit_lines:
        event = decode_json(raw)
        assert event["trace_id"] == root_trace_id
        assert "event_type" in event
        assert "span_id" in event


def test_rust_agent_ffi_classifier_schema_cache_and_async_logger() -> None:
    """Test Rust PyO3 classify_jsonrpc_frame and SchemaDigestCache fastpaths."""
    if _vertebra is not None and hasattr(_vertebra, "classify_jsonrpc_frame"):
        kind, method, req_id = _vertebra.classify_jsonrpc_frame(
            b'{"jsonrpc":"2.0","method":"tools/call","id":"t1","params":{"name":"search"}}'
        )
        assert kind == FRAME_KIND_SINGLE_REQUEST
        assert method == "tools/call"
        assert req_id == "t1"

        kind_notif, method_notif, notif_id = _vertebra.classify_jsonrpc_frame(
            b'{"jsonrpc":"2.0","method":"notifications/initialized"}'
        )
        assert kind_notif == FRAME_KIND_NOTIFICATION
        assert method_notif == "notifications/initialized"
        assert notif_id is None

        kind_batch, _, _ = _vertebra.classify_jsonrpc_frame(b'[{"jsonrpc":"2.0","method":"ping"}]')
        assert kind_batch == FRAME_KIND_BATCH_REQUEST

        cache = _vertebra.SchemaDigestCache()
        digest, etag = cache.put("skill:search", b'{"properties":{"q":{"type":"string"}}}')
        assert digest > 0
        assert etag.startswith('"') and etag.endswith('"')
        assert cache.get_etag("skill:search") == etag
        assert bytes(cache.get_bytes("skill:search")) == b'{"properties":{"q":{"type":"string"}}}'
        assert cache.combined_etag().startswith('"W/')

    py_cache = SchemaDigestCache()
    d1, e1 = py_cache.put("tool:calc", b'{"type":"object"}')
    assert d1 > 0
    assert py_cache.get_etag("tool:calc") == e1
    assert py_cache.combined_etag().startswith('"W/')


@pytest.mark.anyio
async def test_jsonrpc_router_passes_dict_params_without_unwrapping() -> None:
    """Handlers declaring ``params`` receive the request params dict as-is, nested keys included."""
    router = JSONRPCRouter()
    received: list[object] = []

    async def capture(params: object = None, context: object = None) -> str:
        received.append(params)
        return "ok"

    router.register("capture", capture)
    nested = {"params": {"name": "calc_add"}}
    response = await router.dispatch(JSONRPCRequest(method="capture", id=1, params=nested))
    assert response == {"jsonrpc": "2.0", "id": 1, "result": "ok"}
    assert received == [nested]


@pytest.mark.anyio
async def test_jsonrpc_router_does_not_retry_handler_type_errors() -> None:
    """A TypeError raised inside a sync params handler is reported once, with the params unchanged."""
    router = JSONRPCRouter()
    attempts: list[object] = []

    def broken(params: object = None, context: object = None) -> str:
        attempts.append(params)
        raise TypeError("handler bug")

    router.register("broken", broken)
    response = await router.dispatch(JSONRPCRequest(method="broken", id=2, params={"params": {"x": 1}}))
    assert response is not None
    assert response["error"]["code"] == INTERNAL_ERROR
    assert attempts == [{"params": {"x": 1}}]


class _TypedToolProvider:
    """Engine-free operations with resolved method types and observable effects."""

    def __init__(self) -> None:
        self.policy_calls: list[dict[str, Any]] = []
        self.calls: list[dict[str, Any]] = []

    def operations(self) -> dict[str, Any]:
        return {"records": self.records}

    def confirmation_reason(self, operation_name: str, arguments: Mapping[str, Any]) -> None:
        self.policy_calls.append(dict(arguments))

    async def records(
        self, sql: "str", params: "dict[str, list[int]] | None" = None, limit: "int" = 10
    ) -> dict[str, Any]:
        """Read typed records with a bounded default."""
        result = {"sql": sql, "params": params, "limit": limit}
        self.calls.append(result)
        return result


@pytest.mark.anyio
async def test_provider_tools_resolve_schemas_and_validate_before_policy() -> None:
    """Original bound method schemas drive public validation and canonical defaults."""
    provider = _TypedToolProvider()
    tool = build_tools(provider)["records"]
    schema = tool.input_schema
    assert schema["required"] == ["sql"]
    assert set(schema["properties"]) == {"sql", "params", "limit", "confirmation_token"}
    assert schema["properties"]["sql"] == {"type": "string"}
    assert schema["properties"]["limit"]["type"] == "integer"
    assert schema["properties"]["limit"]["default"] == 10
    assert {"type": "object", "additionalProperties": {"type": "array", "items": {"type": "integer"}}} in schema[
        "properties"
    ]["params"]["anyOf"]
    for arguments, pointer in (
        ({}, "/sql"),
        ({"sql": 3}, "/sql"),
        ({"sql": "SELECT 1", "params": {"ids": ["bad"]}}, "/params"),
    ):
        with pytest.raises(ToolArgumentError) as exc:
            await tool.invoke(arguments)
        assert exc.value.pointer == pointer
    assert provider.policy_calls == []
    assert provider.calls == []
    assert await tool.invoke({"sql": "SELECT 1", "params": {"ids": (1, 2)}}) == {
        "sql": "SELECT 1",
        "params": {"ids": [1, 2]},
        "limit": 10,
    }
    assert provider.policy_calls == provider.calls


@pytest.mark.parametrize("reserved", ["context", "confirmation_token"])
def test_provider_tools_reject_reserved_parameters(reserved: str) -> None:
    """Provider control names cannot collide with bridge-owned arguments."""

    class Provider(_TypedToolProvider):
        def operations(self) -> dict[str, Any]:
            return {"collision": context_operation if reserved == "context" else token_operation}

    async def context_operation(context: str) -> str:
        return context

    async def token_operation(confirmation_token: str) -> str:
        return confirmation_token

    with pytest.raises(ValueError, match=reserved):
        build_tools(Provider())


@pytest.mark.parametrize("parameter_kind", ["positional-only", "var-positional", "var-keyword"])
def test_provider_registration_rejects_unsupported_parameter_kinds(parameter_kind: str) -> None:
    """Provider schemas require named fields that Tool.invoke can validate and inject safely."""

    async def positional_only(sql: str, /) -> str:
        return sql

    async def var_positional(*values: str) -> str:
        return ",".join(values)

    async def var_keyword(sql: str, **options: Any) -> str:
        return sql

    class Provider(_TypedToolProvider):
        def operations(self) -> dict[str, Any]:
            return {
                "unsupported": {
                    "positional-only": positional_only,
                    "var-positional": var_positional,
                    "var-keyword": var_keyword,
                }[parameter_kind]
            }

    with pytest.raises(ValueError, match="named parameters"):
        build_tools(Provider())


@pytest.mark.anyio
async def test_provider_tools_preserve_keyword_only_defaults() -> None:
    """Named keyword-only operation fields keep their defaults and validation."""

    class Provider(_TypedToolProvider):
        async def records(self, sql: str, *, limit: int = 10) -> dict[str, Any]:
            return {"sql": sql, "limit": limit}

    provider = Provider()
    tool = build_tools(provider)["records"]
    assert await tool.invoke({"sql": "SELECT 1"}) == {"sql": "SELECT 1", "limit": 10}
    assert provider.policy_calls == [{"sql": "SELECT 1", "limit": 10}]
    with pytest.raises(ToolArgumentError) as exc:
        await tool.invoke({"sql": "SELECT 1", "limit": "invalid"})
    assert exc.value.pointer == "/limit"
    assert len(provider.policy_calls) == 1


@pytest.mark.anyio
async def test_toolspec_and_msgspec_schema_components_without_eval() -> None:
    """Verify ToolSpec, ToolAnnotations, msgspec.defstruct/schema_components derivation, and zero eval() calls."""
    assert agent_pkg.ToolSpec is ToolSpec
    assert agent_pkg.ToolAnnotations is ToolAnnotations

    source_tree = ast.parse(inspect.getsource(schema_mod))
    eval_calls = [
        node
        for node in ast.walk(source_tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "eval"
    ]
    assert eval_calls == []

    def search_catalog(
        query: Annotated[str, msgspec.Meta(min_length=2, max_length=64, description="Search query")],
        limit: Annotated[int, msgspec.Meta(ge=1, le=50)] = 10,
        ctx: AgentContext | None = None,
    ) -> UserRecord:
        """Search user catalog by query."""
        _ = ctx
        return UserRecord(name=query, age=limit)

    search_catalog.__annotations__ = {
        "query": 'Annotated[str, msgspec.Meta(min_length=2, max_length=64, description="Search query")]',
        "limit": "Annotated[int, msgspec.Meta(ge=1, le=50)]",
        "ctx": "AgentContext | None",
        "return": "UserRecord",
    }

    default_tool = Tool(search_catalog, title="Catalog Search")
    spec = default_tool.spec
    assert isinstance(spec, ToolSpec)
    assert spec.name == "search_catalog"
    assert spec.title == "Catalog Search"
    assert spec.description == "Search user catalog by query."
    assert spec.exposure == frozenset({"mcp", "a2a"})
    assert "webmcp" not in spec.exposure
    assert spec.requires_confirmation is False
    assert isinstance(spec.annotations, ToolAnnotations)
    assert spec.annotations.consequential is False
    assert "ctx" not in spec.input_schema["properties"]
    assert spec.input_schema["properties"]["query"]["minLength"] == 2
    assert spec.input_schema["properties"]["query"]["maxLength"] == 64
    assert spec.input_schema["properties"]["query"]["description"] == "Search query"
    assert spec.input_schema["properties"]["limit"]["minimum"] == 1
    assert spec.input_schema["properties"]["limit"]["maximum"] == 50
    assert spec.input_schema["properties"]["limit"]["default"] == 10
    assert spec.output_schema is not None
    assert spec.output_schema["type"] == "object"
    assert "name" in spec.output_schema["properties"]
    assert "age" in spec.output_schema["properties"]

    web_tool = Tool(
        search_catalog,
        name="web_search_catalog",
        webmcp=True,
        requires_confirmation=True,
        annotations={"read_only": True, "open_world": True},
    )
    assert web_tool.spec.exposure == frozenset({"mcp", "a2a", "webmcp"})
    assert web_tool.spec.annotations.read_only is True
    assert web_tool.spec.annotations.open_world is True
    assert web_tool.spec.annotations.consequential is True

    invoked = await default_tool.invoke({"query": "Ada", "limit": 36})
    assert invoked == UserRecord(name="Ada", age=36)

    with pytest.raises(ToolArgumentError) as err_short:
        await default_tool.invoke({"query": "A", "limit": 10})
    assert err_short.value.pointer == "/query"

    with pytest.raises(ToolArgumentError) as err_range:
        await default_tool.invoke({"query": "Ada", "limit": 99})
    assert err_range.value.pointer == "/limit"


@pytest.mark.parametrize(
    "header,value",
    [
        ("x-goog-authenticated-user-email", "accounts.google.com:ana@example.com"),
        ("x-vertebra-mtls-subject", "CN=svc"),
        ("x-forwarded-client-cert", "By=spiffe://a"),
        ("x-tenant-id", "acme"),
    ],
)
def test_agent_context_ignores_untrusted_identity_headers(header: str, value: str) -> None:
    """Untrusted proxy headers cannot authenticate a caller or select a tenant."""
    assert AgentContext.from_headers({header: value}).identity == CallerIdentity()


@pytest.mark.parametrize(
    "peer,expected", [("10.0.0.5", "CN=svc"), ("203.0.113.9", None), (None, None), ("bad", None), ("::1", None)]
)
def test_agent_context_identity_sources(peer: str | None, expected: str | None) -> None:
    """Only a peer inside the configured proxy network may assert a certificate."""
    source = MtlsIdentitySource(trusted_proxies=("10.0.0.0/8",), header="X-Forwarded-Client-Cert")
    ctx = AgentContext.from_headers(
        {"X-Forwarded-Client-Cert": "CN=svc", "x-goog-authenticated-user-email": "ana@example.com"},
        identity_source=source,
        peer=peer,
    )
    assert ctx.identity.client_cert_subject == expected
    assert ctx.identity.email is None
    assert ctx.identity.auth_scheme == ("mtls" if expected else "anonymous")


def test_agent_context_constructors_and_fork() -> None:
    """Trusted users, scopes, custom sources, and forks preserve their stated boundaries."""
    source = CallableIdentitySource(
        resolver=lambda request: CallerIdentity(user_id=request.headers["x-user"], tenant_id="acme")
    )
    assert AgentContext.from_headers({"X-User": "u1"}, identity_source=source).identity.owner_key() == "acme:u1"
    assert (
        AgentContext.from_headers({}, identity_source=CallableIdentitySource(resolver=lambda request: None)).identity
        == CallerIdentity()
    )
    ctx = AgentContext.from_scope(
        {
            "headers": [(b"x-tenant-id", b"bad")],
            "user": SimpleNamespace(id=7, email="b@x.io", roles=["admin", "admin"]),
            "auth": {"scopes": ["read", "read"], "roles": ["viewer"]},
        }
    )
    assert ctx.identity == CallerIdentity(
        user_id="7", email="b@x.io", roles=("admin", "viewer"), scopes=("read",), auth_scheme="connection"
    )
    mtls = MtlsIdentitySource(trusted_proxies=("10.0.0.0/8",))
    assert (
        AgentContext.from_scope(
            {"headers": [(b"x-vertebra-mtls-subject", b"CN=svc")], "client": ("10.0.0.5", 5)}, identity_source=mtls
        ).identity.client_cert_subject
        == "CN=svc"
    )
    assert (
        AgentContext.from_wsgi_environ({"REMOTE_USER": "carol", "HTTP_X_TENANT_ID": "t9"}).identity.owner_key()
        == "default:carol"
    )
    assert (
        AgentContext.from_wsgi_environ(
            {"REMOTE_ADDR": "198.51.100.7", "HTTP_X_VERTEBRA_MTLS_SUBJECT": "CN=svc"}, identity_source=mtls
        ).identity
        == CallerIdentity()
    )
    assert AgentContext.from_headers({"Authorization": "Bearer abc"}).identity == CallerIdentity(auth_scheme="jwt")
    ctx.metadata["a"] = 1
    child = ctx.fork()
    assert child.identity is ctx.identity
    assert child.cancellation_token is not ctx.cancellation_token
    assert child.metadata == {"a": 1}
    assert child.metadata is not ctx.metadata
    token = bind_agent_context(ctx)
    try:
        assert current_agent_context() is ctx
    finally:
        reset_agent_context(token)
    assert current_agent_context() is None


def test_agent_context_starlette_without_authentication() -> None:
    """Request construction never evaluates middleware-dependent user properties."""
    request = Request({"type": "http", "method": "POST", "path": "/mcp", "headers": []})
    assert AgentContext.from_request(request).identity == CallerIdentity()


@pytest.mark.parametrize(
    "cidrs,message", [((), "at least one trusted proxy CIDR"), (("not-a-cidr",), "Invalid trusted proxy CIDR")]
)
def test_identity_source_config_errors(cidrs: tuple[str, ...], message: str) -> None:
    """Invalid proxy policies fail during construction."""
    with pytest.raises(ValueError, match=message):
        MtlsIdentitySource(trusted_proxies=cidrs)


def test_identity_source_rejects_noncallable() -> None:
    """A non-callable identity resolver cannot produce a working policy."""
    with pytest.raises(TypeError, match="must be callable"):
        CallableIdentitySource(resolver=cast("Any", "x"))


@pytest.mark.anyio
async def test_tool_authorize_call_split_and_single_guard_execution() -> None:
    """Tool.call bypasses guards, Tool.authorize runs guards once, and Tool.invoke composes both."""
    bucket = TokenBucketGuard(rate_per_second=0.0, capacity=1.0)
    invocations: list[int] = []

    @tool(name="double_val", guards=(bucket,))
    async def double_val(value: int) -> int:
        invocations.append(value)
        return value * 2

    ctx = AgentContext(identity=CallerIdentity(user_id="alice", tenant_id="acme"))

    direct_out = await double_val.call({"value": 4}, context=ctx)
    assert direct_out == 8
    assert invocations == [4]

    invoked_out = await double_val.invoke({"value": 7}, context=ctx)
    assert invoked_out == 14
    assert invocations == [4, 7]

    with pytest.raises(RateLimitExceededError, match="Rate limit exceeded"):
        await double_val.invoke({"value": 9}, context=ctx)
    assert invocations == [4, 7]


def _sync_connection_guard(connection: Any, route_handler: Any) -> None:
    connection["checked_by"] = ("sync", route_handler)


async def _async_connection_guard(connection: Any, route_handler: Any) -> None:
    connection["checked_by"] = ("async", route_handler)


class _MethodConnectionGuard:
    async def check(self, connection: Any, route_handler: Any) -> None:
        connection["checked_by"] = ("method", route_handler)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("guard_obj", "expected_tag"),
    [
        (_sync_connection_guard, "sync"),
        (_async_connection_guard, "async"),
        (_MethodConnectionGuard(), "method"),
    ],
    ids=["sync-fn", "async-fn", "check-method"],
)
async def test_two_arg_connection_guards_fail_closed_without_connection(
    guard_obj: Any,
    expected_tag: str,
) -> None:
    """Two-argument Litestar connection guards raise PermissionDeniedError when connection is None."""
    ctx = AgentContext(identity=CallerIdentity(user_id="alice", roles=("admin",)))

    @tool(name="conn_guarded", guards=(guard_obj,))
    def conn_guarded() -> str:
        return "allowed"

    with pytest.raises(PermissionDeniedError, match="Guard requires an HTTP connection"):
        await GuardChain((guard_obj,)).execute(ctx)

    with pytest.raises(PermissionDeniedError, match="Guard requires an HTTP connection"):
        await conn_guarded.authorize(ctx)

    with pytest.raises(PermissionDeniedError, match="Guard requires an HTTP connection"):
        await conn_guarded.invoke({}, context=ctx)

    fake_conn: dict[str, Any] = {}
    await conn_guarded.authorize(ctx, connection=fake_conn, route_handler="route_1")
    assert fake_conn["checked_by"] == (expected_tag, "route_1")


def test_guard_error_classes_exported() -> None:
    """Guard exception types instantiate with preserved reason strings."""
    budget_err = BudgetExceededError("step limit reached")
    assert str(budget_err) == "step limit reached"
