"""Unit tests for vertebra.extensions.fastapi, CompiledDependantPlan, and zero-copy DTO fastpath."""

from __future__ import annotations

import contextlib
import threading
from types import SimpleNamespace
from typing import Any

import msgspec
import pytest
import starlette.requests

from vertebra._vertebra import AsgiProtocol, AsgiScope
from vertebra.extensions.fastapi import (
    FastAPIConfig,
    FastAPIPlugin,
    FastPath,
    MsgspecJSONResponse,
    OpenAPIConfig,
    RouteCompiler,
    SwaggerDocsConfig,
    WebMCPConfig,
    compile_dependant_plan,
    execute_dependant_plan,
    mcp_tool,
    offload_middlewares,
)


class _OrderInput(msgspec.Struct, frozen=True, gc=False):
    sku: str
    quantity: int


class _OrderOutput(msgspec.Struct, frozen=True, gc=False):
    order_id: int
    sku: str
    quantity: int
    tenant: str


class _MockDepends:
    def __init__(self, dependency: Any, *, use_cache: bool = True) -> None:
        self.dependency = dependency
        self.use_cache = use_cache


def _make_mock_fastapi_app() -> tuple[Any, Any]:
    call_counter = {"tenant": 0}

    async def get_tenant(request: Any) -> str:
        call_counter["tenant"] += 1
        return str(request.query_params.get("tenant", "acme"))

    tenant_dep = _MockDepends(get_tenant)

    async def get_auth_context(tenant: str = tenant_dep) -> dict[str, str]:
        return {"tenant": tenant, "role": "admin"}

    auth_dep = _MockDepends(get_auth_context)

    @mcp_tool(name="create_order", description="Create an order")
    async def create_order(
        order_id: int,
        payload: _OrderInput,
        tenant: str = tenant_dep,
        ctx: dict[str, str] = auth_dep,
    ) -> _OrderOutput:
        return _OrderOutput(
            order_id=order_id,
            sku=payload.sku,
            quantity=payload.quantity,
            tenant=f"{tenant}:{ctx['role']}",
        )

    routes = [
        SimpleNamespace(
            path="/orders/{order_id:int}",
            endpoint=create_order,
            methods=["POST"],
            status_code=201,
            name="create_order",
            dependant=None,
            body_field=SimpleNamespace(name="payload", type_=_OrderInput),
            response_field=None,
        )
    ]
    app = SimpleNamespace(
        routes=routes,
        user_middleware=[],
        exception_handlers={},
        dependency_overrides={},
        openapi_url="/openapi.json",
        openapi=lambda: {"openapi": "3.1.0", "info": {"title": "Orders API", "version": "1.0.0"}, "paths": {}},
        state=SimpleNamespace(),
        router=SimpleNamespace(routes=routes, default_response_class=None),
    )
    return app, call_counter


@pytest.mark.anyio
async def test_fastapi_plugin_compiled_dependant_and_zero_copy_fastpath() -> None:
    """Verify FastAPIPlugin flattens Depends() DAG, caches sub-dependencies, and decodes msgspec zero-copy."""
    app, call_counter = _make_mock_fastapi_app()
    plugin = FastAPIPlugin(
        app,
        config=FastAPIConfig(
            enable_health_endpoint=True,
            openapi_config=OpenAPIConfig(swagger=SwaggerDocsConfig(path="/swagger")),
            webmcp_config=WebMCPConfig(enabled=True),
        ),
    )

    assert app.router.default_response_class is MsgspecJSONResponse
    assert plugin.offload_plan is not None
    assert plugin.offload_plan.openapi is not None
    assert plugin.offload_plan.openapi.schema_path == "/openapi.json"

    compiler = RouteCompiler()
    table = compiler.compile(app)
    matched = table.match("POST", "/orders/77")
    assert matched is not None

    compiled_route = plugin.compiled_routes[matched.handler_id]
    assert compiled_route.dependant_plan is not None
    assert compiled_route.dependant_plan.has_generator_dependencies is False
    assert len(compiled_route.dependant_plan.nodes) == 2

    fastpath: FastPath = app.__vertebra_fastpath__
    scope = AsgiScope.from_parts(
        method="POST",
        path="/orders/77",
        query_string="tenant=globex",
        body=b'{"sku":"GPU-H200","quantity":4}',
    )

    async def noop_receive() -> dict[str, Any]:
        raise AssertionError("receive() must not be called when AsgiScope has body bytes")

    async def noop_send(_msg: dict[str, Any]) -> None:
        return None

    res = await fastpath.dispatch(scope, noop_receive, noop_send)
    assert res is not None
    status, _headers, body = res
    assert status == 201
    decoded = msgspec.json.decode(body, type=_OrderOutput)
    assert decoded == _OrderOutput(order_id=77, sku="GPU-H200", quantity=4, tenant="globex:admin")
    assert call_counter["tenant"] == 1


@pytest.mark.anyio
async def test_compiled_dependant_generator_and_overrides() -> None:
    """Verify CompiledDependantPlan handles generator dependencies and dependency_overrides."""
    events: list[str] = []

    async def db_session() -> Any:
        events.append("open")
        try:
            yield "session_live"
        finally:
            events.append("close")

    dep_tree = SimpleNamespace(
        dependencies=(
            SimpleNamespace(
                name="db",
                call=db_session,
                cache_key=(db_session, ()),
                use_cache=True,
                dependencies=(),
                security_requirements=(),
            ),
        ),
        security_requirements=(),
    )
    plan = compile_dependant_plan(dep_tree)
    assert plan is not None
    assert plan.has_generator_dependencies is True

    async with contextlib.AsyncExitStack() as stack:
        resolved = await execute_dependant_plan(
            plan,
            request=SimpleNamespace(),
            path_params={},
            query_params={},
            exit_stack=stack,
        )
        assert resolved == {"db": "session_live"}
        assert events == ["open"]
    assert events == ["open", "close"]

    resp = MsgspecJSONResponse({"hello": "world"}, status_code=202)
    assert resp.status_code == 202
    assert resp.body == b'{"hello":"world"}'
    offloaded = offload_middlewares(_make_mock_fastapi_app()[0])
    assert offloaded.openapi is not None


@pytest.mark.anyio
async def test_fastapi_fastpath_lazy_request_inline_sync_and_send_raw_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify FastAPI FastPath skips Request instantiation when unneeded, runs sync inline by default, and uses send_raw_response."""
    caller_tid = threading.get_ident()
    observed_tids: dict[str, int] = {}

    def sync_inline_item(item_id: int) -> dict[str, int]:
        observed_tids["inline"] = threading.get_ident()
        return {"item_id": item_id}

    def sync_threaded_item(item_id: int) -> dict[str, int]:
        observed_tids["threaded"] = threading.get_ident()
        return {"item_id": item_id}

    setattr(sync_threaded_item, "__vertebra_sync_to_thread__", True)

    routes = [
        SimpleNamespace(
            path="/inline/{item_id:int}",
            endpoint=sync_inline_item,
            methods=["GET"],
            status_code=200,
            name="sync_inline_item",
            dependant=None,
            body_field=None,
            response_field=None,
        ),
        SimpleNamespace(
            path="/threaded/{item_id:int}",
            endpoint=sync_threaded_item,
            methods=["GET"],
            status_code=200,
            name="sync_threaded_item",
            dependant=None,
            body_field=None,
            response_field=None,
        ),
    ]
    app = SimpleNamespace(
        routes=routes,
        user_middleware=[],
        exception_handlers={},
        dependency_overrides={},
        openapi_url="/openapi.json",
        openapi=lambda: {"openapi": "3.1.0", "info": {"title": "Sync API", "version": "1.0.0"}, "paths": {}},
        state=SimpleNamespace(),
        router=SimpleNamespace(routes=routes, default_response_class=None),
    )
    plugin = FastAPIPlugin(app)
    fastpath: FastPath = app.__vertebra_fastpath__

    inline_route = next(r for r in plugin.compiled_routes.values() if r.path_template == "/inline/{item_id:int}")
    assert inline_route.needs_request is False

    def _forbid_request(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("starlette.requests.Request must not be instantiated when needs_request is False")

    monkeypatch.setattr(starlette.requests, "Request", _forbid_request)

    async def fallback_app(_scope: Any, _receive: Any, _send: Any) -> None:
        raise AssertionError("fallback_app must not be called")

    wrapped = fastpath.wrap_asgi(fallback_app)
    proto = AsgiProtocol(b"")
    raw_calls: list[int] = []

    class _RawSender:
        async def __call__(self, message: dict[str, Any]) -> None:
            await proto.send(message)

        async def send_raw_response(
            self,
            status: int,
            headers: list[tuple[bytes, bytes]],
            body: bytes,
        ) -> None:
            raw_calls.append(status)
            await proto.send_raw_response(status, headers, body)

    scope_inline = AsgiScope.from_parts(method="GET", path="/api/inline/12")
    scope_inline["root_path"] = "/api"
    await wrapped(scope_inline, proto.receive, _RawSender())
    assert raw_calls == [200]
    assert observed_tids["inline"] == caller_tid
    assert msgspec.json.decode(proto.take_response().body) == {"item_id": 12}

    proto_th = AsgiProtocol(b"")
    scope_th = AsgiScope.from_parts(method="GET", path="/threaded/15")
    await wrapped(scope_th, proto_th.receive, proto_th.send)
    assert observed_tids["threaded"] != caller_tid
    assert msgspec.json.decode(proto_th.take_response().body) == {"item_id": 15}

