"""Unit tests for vertebra.extensions.fastapi, CompiledDependantPlan, and zero-copy DTO fastpath."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import hmac
import re
import subprocess
import sys
import threading
from collections.abc import Awaitable
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import msgspec
import pytest
import starlette.requests
from advanced_alchemy.config import EngineConfig, RoutingConfig
from advanced_alchemy.extensions.fastapi import (
    SQLAlchemyAsyncConfig as FastAPISQLAlchemyAsyncConfig,
)
from advanced_alchemy.extensions.fastapi import (
    SQLAlchemySyncConfig as FastAPISQLAlchemySyncConfig,
)
from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool
from starlette.responses import JSONResponse, Response
from starlette.testclient import TestClient

from vertebra._vertebra import AsgiProtocol, AsgiScope, Request
from vertebra.agent.mcp.routes import AsgiMCPApp
from vertebra.extensions.alchemy import (
    create_async_alchemy_config,
    create_fastapi_alchemy,
    create_sync_alchemy_config,
)
from vertebra.extensions.broker import (
    AsyncSHMBrokerTransport,
    BrokerConfig,
    SyncSHMBrokerTransport,
)
from vertebra.extensions.django import (
    MsgspecView as DjangoMsgspecView,
)
from vertebra.extensions.django import (
    VertebraPlugin as DjangoVertebraPlugin,
)
from vertebra.extensions.fastapi import (
    AssetManifest,
    AuthenticatedPrincipal,
    CacheStore,
    ChannelsBackend,
    FastAPIOffloadMiddleware,
    FastPath,
    JWTAuthBackend,
    JWTAuthGuard,
    MemoryConfig,
    MsgspecJSONResponse,
    OpenAPIConfig,
    RateLimitGuard,
    RouteCompiler,
    SHMQueueBackend,
    SwaggerDocsConfig,
    SyncStorageRegistry,
    VertebraConfig,
    VertebraPlugin,
    ViteConfig,
    ViteExtension,
    WebMCPConfig,
    attach_cache_store,
    attach_channels,
    attach_storage,
    compile_dependant_plan,
    execute_dependant_plan,
    get_vertebra_client_path,
    get_vertebra_client_vite_aliases,
    mcp_tool,
    mount_mcp_routes,
    offload_middlewares,
    vite_asset,
    vite_hmr_client,
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
from vertebra.framework.vite import clear_vite_bridges
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
    """FastAPI mount_mcp_routes shares AsgiMCPApp over MCPCore with 2026-07-28 HTTP semantics."""
    app, _ = _make_mock_fastapi_app()
    reg = mount_mcp_routes(app)
    assert reg.get_spec("create_order") is not None
    mcp_app = getattr(app.state, "mcp_app", None)
    assert isinstance(mcp_app, AsgiMCPApp)

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


@pytest.mark.parametrize("is_async", [False, True])
@pytest.mark.parametrize(
    ("connection_string", "use_routing", "use_engine"),
    [
        (None, False, False),
        ("", False, False),
        ("   ", False, False),
        ("sqlite:///:memory:", True, False),
        ("sqlite:///:memory:", False, True),
        (None, True, True),
        ("sqlite:///:memory:", True, True),
    ],
)
def test_alchemy_config_rejects_missing_or_conflicting_sources(
    is_async: bool,
    connection_string: str | None,
    use_routing: bool,
    use_engine: bool,
) -> None:
    """Verify Alchemy config factories require exactly one source among URL, routing_config, or engine_instance."""
    routing_cfg = RoutingConfig(primary_connection_string="sqlite+vtb:///:memory:") if use_routing else None
    engine = (
        (create_async_engine("sqlite+vtb_async:///:memory:") if is_async else create_engine("sqlite+vtb:///:memory:"))
        if use_engine
        else None
    )
    try:
        with pytest.raises(ValueError):
            if is_async:
                create_async_alchemy_config(
                    connection_string,
                    routing_config=routing_cfg,
                    engine_instance=cast("Any", engine),
                )
            else:
                create_sync_alchemy_config(
                    connection_string,
                    routing_config=routing_cfg,
                    engine_instance=cast("Any", engine),
                )
    finally:
        if engine is not None:
            if is_async:
                asyncio.run(cast("Any", engine).dispose())
            else:
                cast("Any", engine).dispose()


@pytest.mark.parametrize("is_async", [False, True])
def test_alchemy_config_rejects_unknown_framework(is_async: bool) -> None:
    """Verify Alchemy config factories reject unsupported framework hosts with ValueError."""
    with pytest.raises(ValueError, match="Unsupported framework host"):
        if is_async:
            create_async_alchemy_config("sqlite+vtb_async:///:memory:", framework=cast("Any", "django"))
        else:
            create_sync_alchemy_config("sqlite+vtb:///:memory:", framework=cast("Any", "django"))


@pytest.mark.parametrize("is_async", [False, True])
def test_alchemy_config_preserves_explicit_pool_settings_with_broker(is_async: bool) -> None:
    """Verify explicit pool_pre_ping, pool_recycle=None, and poolclass survive even when a broker is provided."""
    broker = (
        AsyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0))
        if is_async
        else SyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0))
    )
    explicit_engine_cfg = EngineConfig(pool_pre_ping=False, pool_recycle=cast("Any", None), poolclass=NullPool)
    try:
        cfg = (
            create_async_alchemy_config(
                "postgresql+vtb_async://user:pass@localhost/appdb",
                framework="fastapi",
                broker=broker,
                engine_config=explicit_engine_cfg,
            )
            if is_async
            else create_sync_alchemy_config(
                "postgresql+vtb://user:pass@localhost/appdb",
                framework="fastapi",
                broker=broker,
                engine_config=explicit_engine_cfg,
            )
        )
        assert cfg.engine_config.pool_pre_ping is False
        assert cfg.engine_config.pool_recycle is None
        assert cfg.engine_config.poolclass is NullPool
    finally:
        if isinstance(broker, AsyncSHMBrokerTransport):
            asyncio.run(broker.close())
        else:
            broker.close()


def test_fastapi_alchemy_factory_and_app_binding() -> None:
    """Verify FastAPI Alchemy config factories build FastAPI configs and attach AdvancedAlchemy to FastAPI."""
    with pytest.raises(ValueError, match="at least one configuration"):
        create_fastapi_alchemy()

    async_cfg = create_async_alchemy_config("sqlite+vtb_async:///:memory:", framework="fastapi")
    sync_cfg = create_sync_alchemy_config("sqlite+vtb:///:memory:", framework="fastapi")
    assert isinstance(async_cfg, FastAPISQLAlchemyAsyncConfig)
    assert isinstance(sync_cfg, FastAPISQLAlchemySyncConfig)

    app = FastAPI()
    ext = create_fastapi_alchemy(async_cfg, app=app)
    assert ext.config == [async_cfg]
    assert async_cfg.app is app

    routed_cfg = create_sync_alchemy_config(
        framework="fastapi",
        routing_config=RoutingConfig(engines={"default": ["sqlite+vtb:///:memory:"]}),
    )
    assert isinstance(routed_cfg, FastAPISQLAlchemySyncConfig)
    assert routed_cfg.routing_config is not None


class _PydanticItem(BaseModel):
    name: str
    qty: int = 1


def _build_hs256_jwt(claims: dict[str, Any], secret: bytes) -> str:
    header_b64 = base64.urlsafe_b64encode(b'{"alg":"HS256","typ":"JWT"}').rstrip(b"=")
    payload_bytes = msgspec.json.encode(claims)
    payload_b64 = base64.urlsafe_b64encode(payload_bytes).rstrip(b"=")
    signing_input = header_b64 + b"." + payload_b64
    sig_b64 = base64.urlsafe_b64encode(hmac.new(secret, signing_input, hashlib.sha256).digest()).rstrip(b"=")
    return (signing_input + b"." + sig_b64).decode("ascii")


def test_fastapi_plugin_real_app_testclient_lifespan_cors_openapi_and_idempotency() -> None:
    """Verify VertebraPlugin.init_app wraps lifespan, installs ASGI offload middleware once, and preserves handlers."""
    lifecycle_events: list[str] = []

    @contextlib.asynccontextmanager
    async def custom_lifespan(_app: FastAPI) -> Any:
        lifecycle_events.append("startup")
        try:
            yield {"lifespan_marker": "active"}
        finally:
            lifecycle_events.append("shutdown")

    app = FastAPI(title="Vertebra FastAPI Test", lifespan=custom_lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["https://app.example.com"],
        allow_methods=["GET", "POST"],
        allow_headers=["Authorization", "Content-Type"],
        allow_credentials=True,
    )

    def get_tenant(tenant: str = "default") -> str:
        return tenant

    @app.post("/items/{item_id}")
    async def create_item(
        item_id: int,
        payload: _PydanticItem,
        tenant: str = Depends(get_tenant),
    ) -> dict[str, Any]:
        return {"item_id": item_id, "name": payload.name, "qty": payload.qty, "tenant": tenant}

    @app.get("/boom")
    async def raise_boom() -> dict[str, str]:
        raise ValueError("custom failure")

    @app.exception_handler(ValueError)
    async def handle_value_error(_request: Any, exc: ValueError) -> JSONResponse:
        return JSONResponse({"error": str(exc)}, status_code=418)

    plugin = VertebraPlugin()
    plugin.init_app(app)
    plugin.init_app(app)

    vertebra_middlewares = [mw for mw in app.user_middleware if mw.cls is cast("Any", FastAPIOffloadMiddleware)]
    assert len(vertebra_middlewares) == 1
    assert lifecycle_events == []

    with TestClient(app) as client:
        assert lifecycle_events == ["startup"]
        assert plugin.offload_plan is not None
        assert plugin.offload_plan.cors is not None
        assert plugin.offload_plan.openapi is not None

        preflight = client.options(
            "/items/10",
            headers={
                "Origin": "https://app.example.com",
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "Content-Type",
            },
        )
        assert preflight.status_code == 204
        assert preflight.headers["access-control-allow-origin"] == "https://app.example.com"
        assert preflight.headers["access-control-allow-credentials"] == "true"

        schema_resp = client.get("/openapi.json", headers={"Origin": "https://app.example.com"})
        assert schema_resp.status_code == 200
        etag = schema_resp.headers["etag"]
        assert "/items/{item_id}" in schema_resp.json()["paths"]
        assert schema_resp.headers["access-control-allow-origin"] == "https://app.example.com"

        cached_resp = client.get("/openapi.json", headers={"If-None-Match": etag})
        assert cached_resp.status_code == 304
        assert cached_resp.content == b""

        created = client.post("/items/42?tenant=globex", json={"name": "widget", "qty": 3})
        assert created.status_code == 200
        assert created.json() == {"item_id": 42, "name": "widget", "qty": 3, "tenant": "globex"}

        boom = client.get("/boom")
        assert boom.status_code == 418
        assert boom.json() == {"error": "custom failure"}

    assert lifecycle_events == ["startup", "shutdown"]


def test_fastapi_plugin_testclient_fastpath_direct_dispatch() -> None:
    """Verify TestClient dispatches middleware-free FastAPI routes through FastPath and OpenAPI ETag cache."""
    app = FastAPI(title="FastPath Direct App")
    plugin = VertebraPlugin()
    plugin.init_app(app)

    def resolve_scope(tag: str = "base") -> str:
        return tag

    @app.post("/orders/{order_id}")
    async def submit_order(
        order_id: int,
        payload: _PydanticItem,
        tag: str = Depends(resolve_scope),
    ) -> dict[str, Any]:
        return {"order_id": order_id, "name": payload.name, "tag": tag}

    with TestClient(app) as client:
        assert plugin.fastpath is not None
        assert plugin.router_table is not None
        matched = plugin.router_table.match("POST", "/orders/99")
        assert matched is not None
        compiled = plugin.compiled_routes[matched.handler_id]
        assert compiled.is_bypass_eligible is True

        resp = client.post("/orders/99?tag=turbo", json={"name": "ssd", "qty": 2})
        assert resp.status_code == 200
        assert resp.json() == {"order_id": 99, "name": "ssd", "tag": "turbo"}

        schema_resp = client.get("/openapi.json")
        assert schema_resp.status_code == 200
        not_modified = client.get("/openapi.json", headers={"If-None-Match": schema_resp.headers["etag"]})
        assert not_modified.status_code == 304


@pytest.mark.parametrize(
    "submodule",
    [
        "config",
        "plugin",
        "offload",
        "openapi",
        "router",
        "fastpath",
        "responses",
        "dependencies",
        "channels",
        "mcp",
        "queues",
        "security",
        "storage",
        "store",
        "vite",
    ],
)
def test_fastapi_subprocess_import_isolation_and_missing_dependency(submodule: str) -> None:
    """Verify vertebra.extensions.fastapi imports cleanly without Starlette/Litestar extensions or FastAPI installed."""
    script = (
        "import importlib, sys\n"
        "from types import SimpleNamespace\n"
        "from vertebra.typing import MissingDependencyError\n"
        "for mod in (\n"
        "    'fastapi', 'starlette', 'starlette.routing', 'starlette.responses', 'starlette.requests',\n"
        "    'litestar', 'litestar_queues', 'vertebra.extensions.starlette', 'vertebra.extensions.litestar'\n"
        "):\n"
        "    sys.modules[mod] = None\n"
        "pkg = importlib.import_module('vertebra.extensions.fastapi')\n"
        f"sub = importlib.import_module('vertebra.extensions.fastapi.{submodule}')\n"
        "assert sub is not None\n"
        "assert hasattr(pkg, 'SHMQueueBackend')\n"
        "for removed in ('SHMExecutionBackend', 'create_shm_queue_config', 'create_sqlspec_queue_config'):\n"
        "    assert not hasattr(pkg, removed), removed\n"
        "cross = [m for m in sys.modules if m.startswith(('vertebra.extensions.starlette', 'vertebra.extensions.litestar')) and sys.modules[m] is not None]\n"
        "assert not cross, f'Unexpected cross-imports: {cross}'\n"
        "try:\n"
        "    pkg.mount_mcp_routes(SimpleNamespace())\n"
        "except MissingDependencyError:\n"
        "    pass\n"
        "else:\n"
        "    raise AssertionError('Expected MissingDependencyError when fastapi is blocked')\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, f"stdout={proc.stdout}\nstderr={proc.stderr}"


@pytest.mark.anyio
async def test_fastapi_feature_adapters_neutral_roundtrips(tmp_path: Path) -> None:
    """Verify FastAPI Vite, storage, security, cache store, and channels adapters via neutral modules."""
    clear_vite_bridges()
    app = FastAPI()

    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text(
        '{"src/main.ts":{"file":"assets/main-abc123.js","isEntry":true,"css":["assets/main-def456.css"]}}',
        encoding="utf-8",
    )
    manifest = AssetManifest.from_file(manifest_file)
    vite_ext = ViteExtension(
        config=ViteConfig(asset_url="/static/"),
        manifest=manifest,
    )
    vite_ext.init_app(app)
    assert app.state.vite is vite_ext
    tags = vite_ext.asset_tags("src/main.ts")
    assert "/static/assets/main-abc123.js" in tags
    assert "/static/assets/main-def456.css" in tags
    assert vite_ext.hmr_client_tag() == ""
    assert vite_asset("src/main.ts")
    assert vite_hmr_client() == ""
    assert get_vertebra_client_path().name == "vertebra-client"
    assert "@vertebra/client" in get_vertebra_client_vite_aliases()

    storage = attach_storage(app, SyncStorageRegistry(default_store=MemoryConfig()))
    assert isinstance(storage, SyncStorageRegistry)
    store_backend = storage.get()
    store_backend.put("hello.txt", b"fastapi-storage")
    assert store_backend.get("hello.txt") == b"fastapi-storage"

    secret = b"test-token-placeholder-secret-32"
    token = _build_hs256_jwt({"sub": "user-1", "scope": "read write", "exp": 4_102_444_800}, secret)
    auth_backend = JWTAuthBackend(secret)
    auth_result = await auth_backend.authenticate(SimpleNamespace(headers={"authorization": f"Bearer {token}"}))
    assert auth_result is not None
    scopes, principal = auth_result
    assert scopes == ("read", "write")
    assert isinstance(principal, AuthenticatedPrincipal)
    assert principal.sub == "user-1"
    assert JWTAuthGuard(secret).verify_token(token).sub == "user-1"

    limiter = RateLimitGuard(capacity=1, refill_rate=0.0)
    assert limiter.allow("client-a") is True
    assert limiter.allow("client-a") is False

    cache = attach_cache_store(app, CacheStore(key_prefix="fastapi-test"))
    cache.set_sync("sync-key", "sync-val", expires_in=30)
    assert cache.get_sync("sync-key") == b"sync-val"
    await cache.set("async-key", b"async-val", expires_in=30)
    assert await cache.get("async-key") == b"async-val"
    await cache.delete_all()
    assert await cache.get("async-key") is None

    channels = attach_channels(app)
    assert isinstance(channels, ChannelsBackend)
    await channels.on_startup()
    try:
        received: list[bytes] = []

        async def _consume() -> None:
            async with channels.subscription(["events"]) as stream:
                async for msg in stream:
                    received.append(msg.data)
                    break

        consumer = asyncio.create_task(_consume())
        await asyncio.sleep(0)
        await channels.publish(b"ping", ["events"])
        await asyncio.wait_for(consumer, timeout=2.0)
        assert received == [b"ping"]
    finally:
        await channels.on_shutdown()
    assert SHMQueueBackend is not None
