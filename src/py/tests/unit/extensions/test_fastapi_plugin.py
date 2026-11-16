"""Unit tests for vertebra.extensions.fastapi, CompiledDependantPlan, and zero-copy DTO fastpath."""

from __future__ import annotations

import contextlib
import re
import threading
from collections.abc import Awaitable
from types import SimpleNamespace
from typing import Any, cast

import msgspec
import pytest
import starlette.requests
from starlette.responses import JSONResponse, Response

from vertebra._vertebra import AsgiProtocol, AsgiScope, Request
from vertebra.agent.mcp.routes import AsgiMcpApp
from vertebra.extensions.django import (
    MsgspecView as DjangoMsgspecView,
)
from vertebra.extensions.django import (
    VertebraPlugin as DjangoVertebraPlugin,
)
from vertebra.extensions.fastapi import (
    FastPath,
    MsgspecJSONResponse,
    OpenAPIConfig,
    RouteCompiler,
    SwaggerDocsConfig,
    VertebraConfig,
    VertebraPlugin,
    WebMCPConfig,
    compile_dependant_plan,
    execute_dependant_plan,
    mcp_tool,
    mount_mcp_routes,
    offload_middlewares,
)
from vertebra.extensions.fastapi.fastpath import (
    BUFFERED_RESPONSE_TYPES,
)
from vertebra.extensions.fastapi.fastpath import (
    CompiledRoute as FastAPICompiledRoute,
)
from vertebra.extensions.fastapi.queues import (
    QueueService as FastAPIQueueService,
)
from vertebra.extensions.fastapi.queues import (
    attach_queue_service as attach_fastapi_queue_service,
)
from vertebra.extensions.fastapi.queues import (
    get_queue_service as get_fastapi_queue_service,
)
from vertebra.extensions.fastapi.queues import (
    queue_lifespan as fastapi_queue_lifespan,
)
from vertebra.extensions.flask import VertebraPlugin as FlaskVertebraPlugin
from vertebra.extensions.flask import vertebra_route
from vertebra.extensions.litestar.fastpath import (
    FastPath as LitestarFastPath,
)
from vertebra.extensions.litestar.router import ExtractedRoute as LitestarExtractedRoute
from vertebra.extensions.litestar.router import RouterTable as LitestarRouterTable
from vertebra.extensions.starlette import VertebraPlugin as StarletteVertebraPlugin
from vertebra.extensions.starlette.fastpath import serialize_buffered_response
from vertebra.framework._app import App
from vertebra.framework.queues import MemoryQueueBackend, task
from vertebra.server._fastpath import attach_framework_fastpath
from vertebra.server._fastpath_core import CompiledRouteBase, get_attached_fastpath, resolve_fastpath_provider
from vertebra.server.worker import _instantiate_mount_runners
from vertebra.utils.serializers import decode_json


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

    tenant_dep: Any = _MockDepends(get_tenant)

    async def get_auth_context(tenant: str = tenant_dep) -> dict[str, str]:
        return {"tenant": tenant, "role": "admin"}

    auth_dep: Any = _MockDepends(get_auth_context)

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
    """Verify VertebraPlugin flattens Depends() DAG, caches sub-dependencies, and decodes msgspec zero-copy."""
    app, call_counter = _make_mock_fastapi_app()
    plugin = VertebraPlugin(
        app,
        config=VertebraConfig(
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
    decoded = decode_json(bytes(body), type=_OrderOutput)
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

    cast("Any", sync_threaded_item).__vertebra_sync_to_thread__ = True

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
    plugin = VertebraPlugin(app)
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
            await cast("Awaitable[None]", proto.send_raw_response(status, headers, body))

    scope_inline = AsgiScope.from_parts(method="GET", path="/api/inline/12")
    scope_inline["root_path"] = "/api"
    await wrapped(scope_inline, proto.receive, _RawSender())
    assert raw_calls == [200]
    assert observed_tids["inline"] == caller_tid
    assert decode_json(proto.take_response().body) == {"item_id": 12}

    proto_th = AsgiProtocol(b"")
    scope_th = AsgiScope.from_parts(method="GET", path="/threaded/15")
    await wrapped(scope_th, proto_th.receive, proto_th.send)
    assert observed_tids["threaded"] != caller_tid
    assert decode_json(proto_th.take_response().body) == {"item_id": 15}


@pytest.mark.anyio
async def test_multi_framework_stacked_app_mount_auto_attaches_fastpath() -> None:
    """Verify App.mount() and _instantiate_mount_runners auto-attach stacked FastPaths across Litestar, FastAPI, Starlette, Flask, and Django."""

    async def ls_handler(item_id: int) -> dict[str, Any]:
        return {"fw": "litestar", "id": item_id}

    ls_handler_obj = SimpleNamespace(
        fn=ls_handler,
        status_code=200,
        media_type="application/json",
        sync_to_thread=False,
        guards=(),
        middleware=(),
        exception_handlers={},
        opt={},
    )
    ls_table = LitestarRouterTable(
        {
            901: LitestarExtractedRoute(
                handler_id=901,
                method="GET",
                path_template="/items/{item_id}",
                matchit_path="/items/{item_id}",
                route=SimpleNamespace(),
                route_handler=ls_handler_obj,
                kwargs_model=None,
                param_types={"item_id": "int"},
                regex_pattern=re.compile(r"^/items/(?P<item_id>[^/]+)$"),
            )
        }
    )

    async def litestar_app(_scope: Any, _receive: Any, _send: Any) -> None:
        raise AssertionError("Litestar fallback must not be called")

    cast("Any", litestar_app).__vertebra_fastpath__ = LitestarFastPath(ls_table)

    fastapi_app, _ = _make_mock_fastapi_app()
    VertebraPlugin(fastapi_app)

    async def st_item(request: Any) -> dict[str, Any]:
        return {"fw": "starlette", "id": int(request.path_params["item_id"])}

    st_routes = [
        SimpleNamespace(
            path="/items/{item_id:int}",
            endpoint=st_item,
            methods=["GET"],
            name="st_item",
        )
    ]

    class _StarletteApp:
        def __init__(self) -> None:
            self.routes = st_routes
            self.user_middleware: list[Any] = []
            self.exception_handlers: dict[Any, Any] = {}
            self.state = SimpleNamespace()
            self.router = SimpleNamespace(routes=st_routes)

        async def __call__(self, _scope: Any, _receive: Any, _send: Any) -> None:
            raise AssertionError("Starlette fallback must not be called")

    starlette_app = _StarletteApp()
    StarletteVertebraPlugin(starlette_app)

    def fl_item(item_id: int) -> dict[str, Any]:
        return {"fw": "flask", "id": item_id}

    vertebra_route(fl_item, status_code=200)

    class _FlaskRule:
        def __init__(self) -> None:
            self.rule = "/items/<int:item_id>"
            self.endpoint = "fl_item"
            self.methods = {"GET"}

    class _FlaskURLMap:
        def iter_rules(self) -> list[_FlaskRule]:
            return [_FlaskRule()]

    class _FlaskApp:
        def __init__(self) -> None:
            self.name = "stacked_flask"
            self.url_map = _FlaskURLMap()
            self.view_functions = {"fl_item": fl_item}
            self.config: dict[str, Any] = {}
            self.extensions: dict[str, Any] = {}

        def wsgi_app(self, _environ: Any, start_response: Any) -> list[bytes]:
            start_response("404 Not Found", [("content-type", "text/plain")])
            return [b"not-found"]

        def __call__(self, environ: Any, start_response: Any) -> Any:
            return self.wsgi_app(environ, start_response)

    flask_app = _FlaskApp()
    FlaskVertebraPlugin(flask_app)

    class _DjangoItemView(DjangoMsgspecView):
        def get(self, request: Request, item_id: int) -> dict[str, Any]:
            assert request.method == "GET"
            return {"fw": "django", "id": item_id}

    class _DjangoPattern:
        def __init__(self) -> None:
            self.pattern = SimpleNamespace(_route="items/<int:item_id>/", _regex=None)
            self.callback = _DjangoItemView.as_view()
            self.name = "dj_item"

    dj_plugin = DjangoVertebraPlugin(urlpatterns=[_DjangoPattern()])

    root_app = App()
    root_app.mount("/litestar", litestar_app)
    root_app.mount("/fastapi", fastapi_app)
    root_app.mount("/starlette", starlette_app)
    root_app.mount("/flask", flask_app, protocol="wsgi")
    root_app.mount("/django", dj_plugin, protocol="wsgi")

    fastapi_provider = resolve_fastpath_provider(fastapi_app)
    assert fastapi_provider is not None and fastapi_provider.name == "fastapi"
    starlette_provider = resolve_fastpath_provider(starlette_app)
    assert starlette_provider is not None and starlette_provider.name == "starlette"

    mount_runners = dict(
        (prefix, (runner, proto)) for prefix, runner, proto in _instantiate_mount_runners(root_app.mounts)
    )
    assert all(
        getattr(mount_runners[p][0], "__vertebra_fastpath_attached__", False) is True
        and not hasattr(mount_runners[p][0], "_vertebra_fastpath")
        for p in ("/litestar", "/fastapi", "/starlette", "/flask", "/django")
    )

    wrapped_once = attach_framework_fastpath(litestar_app, mount_prefix="/litestar", protocol="asgi")
    assert attach_framework_fastpath(wrapped_once, mount_prefix="/litestar", protocol="asgi") is wrapped_once

    ls_runner = mount_runners["/litestar"][0]
    ls_resp = await ls_runner.handle_path("/litestar/items/42")
    assert ls_resp.status == 200
    assert decode_json(ls_resp.body) == {"fw": "litestar", "id": 42}

    fa_runner = mount_runners["/fastapi"][0]
    fa_resp = await fa_runner.handle_path(
        "/fastapi/orders/77",
        method="POST",
        query="tenant=acme",
        body=b'{"sku":"GPU","quantity":2}',
    )
    assert fa_resp.status == 201
    assert decode_json(fa_resp.body)["order_id"] == 77

    st_runner = mount_runners["/starlette"][0]
    st_resp = await st_runner.handle_path("/starlette/items/42")
    assert st_resp.status == 200
    assert decode_json(st_resp.body) == {"fw": "starlette", "id": 42}

    fl_runner = mount_runners["/flask"][0]
    fl_resp = fl_runner.handle_request(Request.from_parts("GET", "/flask/items/42"))
    assert fl_resp.status == 200
    assert decode_json(bytes(fl_resp.body)) == {"fw": "flask", "id": 42}

    dj_runner = mount_runners["/django"][0]
    dj_resp = dj_runner.handle_request(Request.from_parts("GET", "/django/items/42/"))
    assert dj_resp.status == 200
    assert decode_json(bytes(dj_resp.body)) == {"fw": "django", "id": 42}


@pytest.mark.anyio
async def test_fastapi_mount_mcp_routes_uses_shared_asgi_mcp_app() -> None:
    """FastAPI mount_mcp_routes shares AsgiMcpApp over MCPCore with 2026-07-28 HTTP semantics."""
    app, _ = _make_mock_fastapi_app()
    reg = mount_mcp_routes(app)
    assert reg.get_spec("create_order") is not None
    mcp_app = getattr(app.state, "mcp_app", None)
    assert isinstance(mcp_app, AsgiMcpApp)

    proto_list = AsgiProtocol(b'{"jsonrpc":"2.0","id":1,"method":"tools/list"}')
    scope_list = AsgiScope.from_parts(
        method="POST",
        path="/mcp",
        headers=[(b"content-type", b"application/json")],
        body=b'{"jsonrpc":"2.0","id":1,"method":"tools/list"}',
    )
    await mcp_app(scope_list, proto_list.receive, proto_list.send)
    resp_list = proto_list.take_response()
    assert resp_list.status == 200
    tools_payload = decode_json(resp_list.body)
    assert any(t["name"] == "create_order" for t in tools_payload["result"]["tools"])

    proto_get = AsgiProtocol(b"")
    await mcp_app(AsgiScope.from_parts(method="GET", path="/mcp"), proto_get.receive, proto_get.send)
    assert proto_get.take_response().status == 405

    proto_origin = AsgiProtocol(b'{"jsonrpc":"2.0","id":2,"method":"tools/list"}')
    scope_origin = AsgiScope.from_parts(
        method="POST",
        path="/mcp",
        headers=[
            (b"content-type", b"application/json"),
            (b"host", b"api.example.com"),
            (b"origin", b"https://attacker.example.com"),
        ],
        body=b'{"jsonrpc":"2.0","id":2,"method":"tools/list"}',
    )
    await mcp_app(scope_origin, proto_origin.receive, proto_origin.send)
    assert proto_origin.take_response().status == 403

    proto_notify = AsgiProtocol(b'{"jsonrpc":"2.0","method":"notifications/cancelled"}')
    scope_notify = AsgiScope.from_parts(
        method="POST",
        path="/mcp",
        headers=[(b"content-type", b"application/json")],
        body=b'{"jsonrpc":"2.0","method":"notifications/cancelled"}',
    )
    await mcp_app(scope_notify, proto_notify.receive, proto_notify.send)
    assert proto_notify.take_response().status == 202


def test_fastapi_compiled_route_base_attachment_and_buffered_response_types() -> None:
    """FastAPI plugin attaches __vertebra_fastpath__ without legacy state keys and supports exact BUFFERED_RESPONSE_TYPES."""
    assert issubclass(FastAPICompiledRoute, CompiledRouteBase)

    app, _ = _make_mock_fastapi_app()
    VertebraPlugin(app)
    assert get_attached_fastpath(app) is not None
    assert not hasattr(app.state, "vertebra_fastpath_bypass")

    assert MsgspecJSONResponse in BUFFERED_RESPONSE_TYPES
    assert JSONResponse in BUFFERED_RESPONSE_TYPES
    assert Response in BUFFERED_RESPONSE_TYPES

    msgspec_resp = MsgspecJSONResponse({"fast": True}, status_code=201)
    serialized = serialize_buffered_response(msgspec_resp)
    assert serialized is not None
    assert serialized[0] == 201
    assert serialized[2] is msgspec_resp.body

    class _CustomMsgspecJSONResponse(MsgspecJSONResponse):
        pass

    assert _CustomMsgspecJSONResponse not in BUFFERED_RESPONSE_TYPES
    assert serialize_buffered_response(_CustomMsgspecJSONResponse({"fast": False})) is None


@pytest.mark.anyio
async def test_fastapi_queue_extension() -> None:
    """Verify FastAPI attach_queue_service, get_queue_service, and queue_lifespan."""
    app, _ = _make_mock_fastapi_app()
    qsvc = attach_fastapi_queue_service(app, FastAPIQueueService(backend=MemoryQueueBackend()))
    assert get_fastapi_queue_service(app) is qsvc

    ran: list[int] = []

    def compute(x: int) -> int:
        ran.append(x)
        return x + 10

    qsvc.register(task(lane="io")(compute))
    await qsvc.enqueue("compute", 5)

    async with fastapi_queue_lifespan(app, run_worker=False) as svc:
        assert svc is qsvc
        assert await svc.drain_once(("io",)) == [15]
        assert ran == [5]
