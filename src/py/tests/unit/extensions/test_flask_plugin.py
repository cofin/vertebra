"""Unit tests for the standalone Flask Vertebra extension."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import msgspec
import pydantic
import pytest

from vertebra._vertebra import AsgiProtocol, AsgiScope, Request, WsgiEnviron, run_wsgi_callable
from vertebra.extensions.flask import (
    AsyncAPIDocsConfig,
    FastPath,
    FlaskCachingBackend,
    FlaskConfig,
    FlaskPlugin,
    JWTAuthGuard,
    MailBackend,
    MailConfig,
    MailMessage,
    MCPConfig,
    MemoryConfig,
    ObjectStore,
    OpenAPIConfig,
    QueueBackend,
    RateLimitGuard,
    RouteCompiler,
    SwaggerDocsConfig,
    ViteConfig,
    attach_storage,
    convert_werkzeug_rule,
    mcp_tool,
    send_storage_file,
    vertebra_route,
)
from vertebra.utils.serializers import decode_json

if TYPE_CHECKING:
    from collections.abc import Callable


class CreateOrderPayload(msgspec.Struct, frozen=True, gc=False):
    """Order creation payload."""

    sku: str
    qty: int


class PydanticOrderPayload(pydantic.BaseModel):
    """Pydantic order payload."""

    sku: str
    qty: int


class _MockRule:
    """Minimal Werkzeug Rule double."""

    def __init__(self, rule: str, endpoint: str, methods: tuple[str, ...]) -> None:
        self.rule = rule
        self.endpoint = endpoint
        self.methods = methods


class _MockUrlMap:
    """Minimal Werkzeug Map double."""

    def __init__(self, rules: list[_MockRule]) -> None:
        self._rules = rules

    def iter_rules(self) -> list[_MockRule]:
        return list(self._rules)


class _MockFlaskApp:
    """Minimal Flask application double supporting URL rules, context processors, and WSGI."""

    def __init__(self, static_folder: str | None = None) -> None:
        self._rules: list[_MockRule] = []
        self.url_map = _MockUrlMap(self._rules)
        self.view_functions: dict[str, Callable[..., Any]] = {}
        self.extensions: dict[str, Any] = {}
        self.config: dict[str, Any] = {
            "COMPRESS_REGISTER": True,
            "COMPRESS_MIN_SIZE": 256,
            "CORS_ORIGINS": ["https://app.example.com"],
            "CORS_SUPPORTS_CREDENTIALS": True,
        }
        self.static_folder = static_folder
        self.static_url_path = "/static"
        self.context_processors: list[Callable[[], dict[str, Any]]] = []

        def _base_wsgi(environ: Any, start_response: Callable[..., Any]) -> list[bytes]:
            start_response("404 Not Found", [("content-type", "text/plain")])
            return [b"fallback-404"]

        self.wsgi_app: Callable[..., Any] = _base_wsgi

    def add_url_rule(
        self,
        rule: str,
        endpoint: str | None = None,
        view_func: Callable[..., Any] | None = None,
        methods: list[str] | None = None,
    ) -> None:
        ep = endpoint or (view_func.__name__ if view_func is not None else "index")
        self._rules.append(_MockRule(rule, ep, tuple(methods or ["GET"])))
        if view_func is not None:
            self.view_functions[ep] = view_func

    def context_processor(self, fn: Callable[[], dict[str, Any]]) -> Callable[[], dict[str, Any]]:
        self.context_processors.append(fn)
        return fn


def test_convert_werkzeug_rule_and_flask_fastpath(tmp_path: Any) -> None:
    """Verify Werkzeug rule conversion, zero-copy msgspec/pydantic fastpath, OpenAPI, and MCP."""
    matchit_path, converters = convert_werkzeug_rule("/orders/<int:order_id>/<path:rest>")
    assert matchit_path == "/orders/{order_id}/{*rest}"
    assert converters == (("order_id", "int"), ("rest", "path"))

    static_dir = tmp_path / "static"
    static_dir.mkdir()
    app = _MockFlaskApp(static_folder=str(static_dir))

    @vertebra_route(status_code=201)
    @mcp_tool(name="create_order", description="Create a new order")
    def create_order(order_id: int, body: CreateOrderPayload) -> dict[str, Any]:
        return {"order_id": order_id, "sku": body.sku, "qty": body.qty}

    @vertebra_route
    def create_pydantic_order(body: PydanticOrderPayload) -> PydanticOrderPayload:
        return PydanticOrderPayload(sku=body.sku.upper(), qty=body.qty + 1)

    app.add_url_rule("/orders/<int:order_id>", endpoint="create_order", view_func=create_order, methods=["POST"])
    app.add_url_rule(
        "/pydantic-orders",
        endpoint="create_pydantic_order",
        view_func=create_pydantic_order,
        methods=["POST"],
    )

    plugin = FlaskPlugin(
        app,
        config=FlaskConfig(
            openapi=OpenAPIConfig(
                title="Orders Flask API",
                version="1.2.0",
                swagger=SwaggerDocsConfig(path="/swagger"),
                asyncapi=AsyncAPIDocsConfig(path="/asyncapi"),
                enabled_docs=("scalar", "swagger", "asyncapi"),
            ),
            mcp=MCPConfig(server_name="flask-orders-mcp"),
            vite=ViteConfig(asset_url="/static/build/"),
        ),
    )

    req = Request.from_parts(
        method="POST",
        path="/orders/42",
        headers=[(b"content-type", b"application/json")],
        body=b'{"sku":"BOOK-1","qty":3}',
    )
    resp = plugin.handle_request(req)
    assert resp.status == 201
    assert decode_json(bytes(resp.body)) == {"order_id": 42, "sku": "BOOK-1", "qty": 3}

    wsgi_env = WsgiEnviron.from_parts(
        method="POST",
        path="/pydantic-orders",
        headers=[(b"content-type", b"application/json")],
        body=b'{"sku":"pen","qty":9}',
    )
    wsgi_resp = run_wsgi_callable(app.wsgi_app, wsgi_env)
    assert wsgi_resp.status == 200
    assert decode_json(bytes(wsgi_resp.body)) == {"sku": "PEN", "qty": 10}

    assert plugin.openapi_schema is not None
    assert "/orders/{order_id}" in plugin.openapi_schema["paths"]
    assert plugin.offload_bundle is not None
    assert plugin.offload_bundle.compression is not None
    assert plugin.offload_bundle.cors is not None
    assert len(plugin.offload_bundle.static_mounts) == 1
    assert len(app.context_processors) == 1
    ctx_vars = app.context_processors[0]()
    assert "vite_hmr_client" in ctx_vars
    assert "vite_asset" in ctx_vars

    assert plugin.mcp_registry is not None
    mcp_req = Request.from_parts(
        method="POST",
        path="/mcp",
        headers=[(b"content-type", b"application/json")],
        body=b'{"jsonrpc":"2.0","id":1,"method":"tools/list"}',
    )
    mcp_resp = plugin.handle_request(mcp_req)
    assert mcp_resp.status == 200
    mcp_payload = decode_json(bytes(mcp_resp.body))
    assert any(t["name"] == "create_order" for t in mcp_payload["result"]["tools"])


@pytest.mark.anyio
async def test_flask_ecosystem_bridges() -> None:
    """Verify Flask CacheBackend (sync + async), Storage, Mail, Queue, and Security bridges."""
    app = _MockFlaskApp()
    cache = FlaskCachingBackend.factory(app, {"CACHE_DEFAULT_TIMEOUT": 60, "CACHE_KEY_PREFIX": "test:"}, [], {})
    assert cache.set("k1", {"v": 1}) is True
    assert cache.get("k1") == {"v": 1}
    await cache.aset("k2", {"v": 2})
    assert await cache.aget("k2") == {"v": 2}
    assert cache.get_many("k1", "k2") == [{"v": 1}, {"v": 2}]
    assert await cache.aget_many("k1", "k2") == [{"v": 1}, {"v": 2}]

    calls = 0

    @cache.cached(timeout=30)
    def compute(x: int) -> int:
        nonlocal calls
        calls += 1
        return x * 2

    assert compute(5) == 10
    assert compute(5) == 10
    assert calls == 1

    store = ObjectStore.from_config(MemoryConfig())
    store.put("reports/q1.txt", b"quarter-1")
    reg = attach_storage(app, store)
    file_resp = send_storage_file(reg.get("default"), "reports/q1.txt", as_attachment=True, download_name="q1.txt")
    assert file_resp.status == 200
    assert bytes(file_resp.body) == b"quarter-1"
    assert any(
        (k == b"content-disposition" or k == "content-disposition")
        and (b'filename="q1.txt"' in v if isinstance(v, bytes) else 'filename="q1.txt"' in v)
        for k, v in file_resp.headers
    )

    mail = MailBackend(MailConfig(suppress_send=True))
    mail.init_app(app)
    mail.send(MailMessage(subject="Hello", recipients=("user@example.com",), body="Welcome"))
    await mail.asend(MailMessage(subject="Async Hello", recipients=("user@example.com",), body="Async Welcome"))
    assert len(mail.outbox) == 2

    queue = QueueBackend()

    @queue.task("multiply")
    def multiply(a: int, b: int) -> int:
        return a * b

    await queue.aenqueue("multiply", 6, 7)
    assert queue.run_next() == 42

    limiter = RateLimitGuard(capacity=2, refill_rate=1.0)
    assert limiter.allow("ip-1") is True
    assert limiter.allow("ip-1") is True

    jwt = JWTAuthGuard(b"super-secret-key-at-least-32-bytes-long")
    assert jwt.verify_header(None) is None
    assert jwt.verify_header("Basic abc") is None


@pytest.mark.anyio
async def test_flask_fastpath_async_views_wrap_asgi_and_wsgi_send_raw_response() -> None:
    """Verify Flask FastPath executes async def views, wrap_asgi, and start_response.send_raw_response."""
    app = _MockFlaskApp()

    @vertebra_route(status_code=201)
    async def create_async_order(order_id: int, body: CreateOrderPayload) -> dict[str, Any]:
        return {"order_id": order_id, "sku": body.sku, "qty": body.qty, "mode": "immediate"}

    @vertebra_route(status_code=200)
    async def suspended_async_order(order_id: int, body: CreateOrderPayload) -> dict[str, Any]:
        await asyncio.sleep(0)
        return {"order_id": order_id, "sku": body.sku, "qty": body.qty, "mode": "suspended"}

    app.add_url_rule(
        "/async-orders/<int:order_id>",
        endpoint="create_async_order",
        view_func=create_async_order,
        methods=["POST"],
    )
    app.add_url_rule(
        "/suspended-orders/<int:order_id>",
        endpoint="suspended_async_order",
        view_func=suspended_async_order,
        methods=["POST"],
    )

    compiler = RouteCompiler(app)
    fastpath = FastPath(compiler)

    req = Request.from_parts(
        method="POST",
        path="/async-orders/11",
        headers=[(b"content-type", b"application/json")],
        body=b'{"sku":"A-11","qty":2}',
    )
    sync_resp = fastpath.dispatch(req)
    assert sync_resp is not None
    assert sync_resp.status == 201
    assert decode_json(bytes(sync_resp.body)) == {
        "order_id": 11,
        "sku": "A-11",
        "qty": 2,
        "mode": "immediate",
    }

    raw_wsgi_calls: list[tuple[int, Any, bytes]] = []

    class _RawWsgiStartResponse:
        def __call__(self, _status_line: str, _headers: Any) -> None:
            raise AssertionError("Standard start_response must not be called when send_raw_response is present")

        def send_raw_response(self, status: int, headers: Any, body: bytes) -> list[bytes]:
            raw_wsgi_calls.append((status, headers, body))
            return [body]

    wsgi_env = WsgiEnviron.from_parts(
        method="POST",
        path="/async-orders/12",
        headers=[(b"content-type", b"application/json")],
        body=b'{"sku":"A-12","qty":5}',
    )
    wsgi_chunks = fastpath.dispatch_wsgi(wsgi_env, _RawWsgiStartResponse())
    assert wsgi_chunks is not None
    assert len(raw_wsgi_calls) == 1
    assert raw_wsgi_calls[0][0] == 201
    assert decode_json(raw_wsgi_calls[0][2]) == {
        "order_id": 12,
        "sku": "A-12",
        "qty": 5,
        "mode": "immediate",
    }

    asgi_app = fastpath.wrap_asgi()
    proto = AsgiProtocol(b'{"sku":"S-99","qty":7}')
    scope = AsgiScope.from_parts(
        method="POST",
        path="/flask/suspended-orders/99",
        headers=[(b"content-type", b"application/json")],
        body=b'{"sku":"S-99","qty":7}',
    )
    scope["root_path"] = "/flask"
    await asgi_app(scope, proto.receive, proto.send)
    assert proto.complete is True
    assert proto.status == 200
    assert decode_json(proto.take_response().body) == {
        "order_id": 99,
        "sku": "S-99",
        "qty": 7,
        "mode": "suspended",
    }

