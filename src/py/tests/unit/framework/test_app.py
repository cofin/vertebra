"""Tests for App route registration, Controller, Router, ProcedureRouter, and handler ID stability."""

import asyncio
import importlib.resources
import sys
import time
from types import ModuleType
from typing import Annotated, Any

import httpx
import jwt as pyjwt
import msgspec
import pytest
from cryptography.hazmat.primitives.asymmetric import ec, rsa

import vertebra
import vertebra.framework as vtb_framework
from vertebra._vertebra import Response
from vertebra.build.codegen._metadata import RouteCollector
from vertebra.framework._app import App, HandlerInfo, _make_handler_id
from vertebra.framework._router import Controller, Route, Router, get, handler_id, post
from vertebra.framework.autowire import AutowireConfig, AutowireLoader
from vertebra.framework.di import Inject, Provide
from vertebra.framework.openapi import OpenAPISpecBuilder
from vertebra.framework.procedures import Procedure, ProcedureRouter, query
from vertebra.framework.security import (
    AuthenticatedPrincipal,
    JWKSConfig,
    JWKSVerifier,
    JWTAuthGuard,
    RateLimitGuard,
)
from vertebra.utils.serializers import encode_json_bytes


def test_handler_id_stability_same_method_path_same_id() -> None:
    assert _make_handler_id("GET", "/ping") == _make_handler_id("GET", "/ping")


def test_handler_id_stability_different_method_different_id() -> None:
    assert _make_handler_id("GET", "/ping") != _make_handler_id("POST", "/ping")


def test_handler_id_stability_different_path_different_id() -> None:
    assert _make_handler_id("GET", "/ping") != _make_handler_id("GET", "/pong")


def test_handler_id_stability_case_sensitive_path() -> None:
    assert _make_handler_id("GET", "/Ping") != _make_handler_id("GET", "/ping")


def test_handler_id_stability_method_normalized_to_upper() -> None:
    assert _make_handler_id("get", "/ping") == _make_handler_id("GET", "/ping")


def test_handler_id_stability_path_normalized_via_app_route() -> None:
    """Path normalization happens in App._route, not _make_handler_id."""
    app = App()

    @app.get("ping")
    def handler() -> str:
        return "ok"

    assert app.routes[0].path == "/ping"
    expected_id = _make_handler_id("GET", "/ping")
    assert expected_id in app.handler_index


def test_handler_id_stability_result_fits_uint64() -> None:
    value = _make_handler_id("GET", "/x")
    assert 0 <= value < 2**64


def test_handler_id_stability_routing_module_handler_id_matches() -> None:
    """handler_id() from routing module must produce identical values."""
    assert handler_id("GET", "/ping") == _make_handler_id("GET", "/ping")


def test_vertebra_app_route_registration_get_decorator_registers_route() -> None:
    app = App()

    @app.get("/ping")
    def ping() -> str:
        return "pong"

    assert len(app.routes) == 1
    assert app.routes[0].method == "GET"
    assert app.routes[0].path == "/ping"


def test_vertebra_app_route_registration_multiple_methods_on_same_path() -> None:
    app = App()

    @app.get("/items")
    def list_items() -> list[Any]:
        return []

    @app.post("/items")
    def create_item() -> dict[str, Any]:
        return {}

    assert len(app.routes) == 2
    methods = {r.method for r in app.routes}
    assert methods == {"GET", "POST"}


def test_vertebra_app_route_registration_handler_index_populated() -> None:
    app = App()

    @app.get("/hello")
    def hello() -> str:
        return "world"

    expected_id = _make_handler_id("GET", "/hello")
    assert expected_id in app.handler_index
    info = app.handler_index[expected_id]
    assert isinstance(info, HandlerInfo)
    assert info.method == "GET"
    assert info.path == "/hello"
    assert "hello" in info.handler_name


def test_vertebra_app_route_registration_handler_index_last_registration_wins() -> None:
    """Duplicate method+path: last registration wins in handler_index."""
    app = App()

    @app.get("/x")
    def first() -> str:
        return "first"

    @app.get("/x")
    def second() -> str:
        return "second"

    assert len(app.routes) == 2
    hid = _make_handler_id("GET", "/x")
    assert "second" in app.handler_index[hid].handler_name


def test_vertebra_app_route_registration_handlers_dict_populated() -> None:
    app = App()

    @app.get("/test")
    def test_handler() -> str:
        return "ok"

    expected_id = _make_handler_id("GET", "/test")
    assert expected_id in app._handlers
    assert app._handlers[expected_id] is test_handler


def test_vertebra_app_route_registration_mount_records_sub_app() -> None:
    app = App()
    sentinel = object()
    app.mount("/static", sentinel)
    assert len(app.mounts) == 1
    prefix, mounted, protocol = app.mounts[0]
    assert prefix == "/static"
    assert mounted is sentinel
    assert protocol == "auto"


def test_vertebra_app_route_registration_mount_with_protocol() -> None:
    app = App()
    sentinel = object()
    app.mount("/api", sentinel, protocol="wsgi")
    prefix, mounted, protocol = app.mounts[0]
    assert protocol == "wsgi"


def test_vertebra_app_route_registration_handler_info_is_msgspec_struct() -> None:
    assert issubclass(HandlerInfo, msgspec.Struct)


@pytest.mark.anyio
async def test_vertebra_app_route_registration_lifecycle_hooks_delegate_to_inner_app() -> None:
    app = App()
    calls: list[str] = []

    @app.on_startup
    async def startup() -> None:
        calls.append("startup")

    await app.startup()
    assert calls == ["startup"]


class _CreateWidgetPayload(msgspec.Struct, frozen=True, gc=False):
    """Input payload for widget creation tests."""

    title: str
    count: int = 1


class _WidgetDTO(msgspec.Struct, frozen=True, gc=False):
    """Output DTO for widget endpoints."""

    id: int
    title: str
    count: int


class _WidgetRepository:
    """In-memory repository injected into Controllers and Routes."""

    def __init__(self, prefix: str = "W") -> None:
        self.prefix = prefix

    def fetch(self, widget_id: int) -> _WidgetDTO:
        return _WidgetDTO(id=widget_id, title=f"{self.prefix}-{widget_id}", count=1)


class _WidgetController(Controller):
    """Controller testing method-level DI, routes, and RPC procedures."""

    path = "/widgets"
    dependencies = {"repo": Provide(_WidgetRepository, scope="singleton")}
    tags = ("widgets",)

    @get("/{widget_id:int}")
    def get_widget(self, widget_id: int, repo: _WidgetRepository) -> _WidgetDTO:
        return repo.fetch(widget_id)

    @post("/", status_code=201)
    def create_widget(self, body: _CreateWidgetPayload, repo: _WidgetRepository) -> _WidgetDTO:
        return _WidgetDTO(id=99, title=f"{repo.prefix}:{body.title}", count=body.count)

    @query()
    def inspect_widget(self, widget_id: int, repo: _WidgetRepository) -> _WidgetDTO:
        return repo.fetch(widget_id)


class _ConstructorInjectedController(Controller):
    """Controller testing constructor-level dependency injection."""

    path = "/ctor-widgets"

    def __init__(self, repo: _WidgetRepository) -> None:
        self.repo = repo

    @get("/{widget_id:int}")
    def get_by_id(self, widget_id: int) -> _WidgetDTO:
        return self.repo.fetch(widget_id)

    @query()
    def inspect_inherited_widget(self, widget_id: int) -> _WidgetDTO:
        return self.repo.fetch(widget_id)


def test_clean_break_domain_exports_and_absence_of_legacy_defs() -> None:
    """Verify Route, Controller, Procedure, Provide, and Inject are exported and legacy *Def names are absent."""
    assert vertebra.Route is Route
    assert vertebra.Controller is Controller
    assert vertebra.Procedure is Procedure
    assert vertebra.ProcedureRouter is ProcedureRouter
    assert vertebra.Provide is Provide
    assert vertebra.Inject is Inject
    assert vtb_framework.Route is Route
    assert vtb_framework.Controller is Controller
    assert vtb_framework.Procedure is Procedure
    assert not hasattr(vertebra, "RouteDef")
    assert not hasattr(vtb_framework, "RouteDef")
    assert not hasattr(vtb_framework.procedures, "ProcedureDef")
    assert not hasattr(vtb_framework.procedures, "procedure_query")
    assert not hasattr(vtb_framework.procedures, "procedure_mutation")
    assert not hasattr(vtb_framework.procedures, "procedure_subscription")


@pytest.mark.anyio
async def test_controller_router_procedure_registration_and_openapi_codegen_filtering() -> None:
    """Verify Controller, nested Router, ProcedureRouter, OpenAPI schema, and RouteCollector DI filtering."""
    api_router = Router(
        prefix="/api/v1",
        route_handlers=[_WidgetController, _ConstructorInjectedController],
        dependencies={"repo": _WidgetRepository("API")},
        tags=("v1",),
    )
    app = App(route_handlers=[api_router])

    paths = {r.path: r for r in app.routes}
    assert "/api/v1/widgets/{widget_id:int}" in paths
    assert "/api/v1/widgets" in paths
    assert "/api/v1/ctor-widgets/{widget_id:int}" in paths
    assert "/_rpc/inspect_widget" in paths
    assert paths["/api/v1/widgets"].status_code == 201
    assert paths["/api/v1/widgets/{widget_id:int}"].tags == ("v1", "widgets")

    rpc_res = await app.call_procedure("inspect_widget", 7)
    assert rpc_res == _WidgetDTO(id=7, title="W-7", count=1)
    inherited_rpc_res = await app.call_procedure("inspect_inherited_widget", 7)
    assert inherited_rpc_res == _WidgetDTO(id=7, title="API-7", count=1)

    oa_doc = OpenAPISpecBuilder.from_app(app).build()
    post_op = oa_doc["paths"]["/api/v1/widgets"]["post"]
    assert post_op["tags"] == ["v1", "widgets"]
    assert post_op["requestBody"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/_CreateWidgetPayload"
    }

    collected = RouteCollector().collect(app)
    by_path = {(m.method, m.path): m for m in collected}
    get_meta = by_path[("GET", "/api/v1/widgets/{widget_id:int}")]
    assert set(get_meta.params.keys()) == {"widget_id"}
    ctor_meta = by_path[("GET", "/api/v1/ctor-widgets/{widget_id:int}")]
    assert set(ctor_meta.params.keys()) == {"widget_id"}


def test_autowire_loader_registers_discovered_services_and_controllers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify AutowireLoader.register_all registers discovered Service and Controller classes on App."""
    fake_mod: Any = ModuleType("fake_domain_pkg")

    class OrderService:
        def label(self) -> str:
            return "orders-ready"

    class OrderController(Controller):
        path = "/orders"

        @get("/")
        def status(self, svc: Annotated[OrderService, Inject()]) -> str:
            return svc.label()

    OrderService.__module__ = "fake_domain_pkg"
    OrderController.__module__ = "fake_domain_pkg"
    fake_mod.OrderService = OrderService
    fake_mod.OrderController = OrderController
    monkeypatch.setitem(sys.modules, "fake_domain_pkg", fake_mod)

    loader = AutowireLoader(AutowireConfig(domain_packages=("fake_domain_pkg",)))
    app = App()
    controllers, services = loader.register_all(app)
    assert controllers == (OrderController,)
    assert services == (OrderService,)
    assert any(r.path == "/orders" for r in app.routes)
    assert app.container.resolve_sync(OrderService).label() == "orders-ready"


def test_app_debug_flag_and_ensure_dev_welcome_route_unclaimed_root(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify App(debug=True) and VERTEBRA_DEV=1 register the GET / welcome route without mutating app.routes."""
    monkeypatch.delenv("VERTEBRA_DEV", raising=False)
    prod_app = App()
    assert prod_app.debug is False
    assert prod_app.ensure_dev_welcome_route() is False
    assert _make_handler_id("GET", "/") not in prod_app.handler_index

    dev_app = App(debug=True)

    @dev_app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    assert dev_app.debug is True
    assert dev_app.ensure_dev_welcome_route() is True
    assert dev_app.ensure_dev_welcome_route() is True
    assert len(dev_app.routes) == 1
    assert dev_app.routes[0].path == "/health"

    root_id = _make_handler_id("GET", "/")
    assert root_id in dev_app.handler_index
    assert dev_app.handler_index[root_id].path == "/"
    assert root_id in dev_app._handlers

    resp = dev_app._handlers[root_id]()
    assert isinstance(resp, Response)
    assert resp.status == 200
    assert (b"content-type", b"text/html; charset=utf-8") in resp.headers
    html_text = bytes(resp.body).decode("utf-8")
    assert "<!DOCTYPE html>" in html_text
    assert "VERTEBRA" in html_text
    assert "#EDB641" in html_text
    assert "Litestar" in html_text
    assert "/health" in html_text
    assert "data-endpoint-tester" in html_text
    assert "data-theme-toggle" in html_text

    env_app = App()
    monkeypatch.setenv("VERTEBRA_DEV", "1")
    assert env_app.ensure_dev_welcome_route() is True
    assert root_id in env_app.handler_index


def test_app_ensure_dev_welcome_route_respects_user_root_route_and_mount(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify user '/' routes and root mounts preempt or evict the synthetic dev welcome route."""
    monkeypatch.delenv("VERTEBRA_DEV", raising=False)
    root_id = _make_handler_id("GET", "/")

    app_with_root = App(debug=True)

    @app_with_root.get("/")
    def custom_index() -> str:
        return "custom-home"

    assert app_with_root.ensure_dev_welcome_route() is False
    assert app_with_root._handlers[root_id]() == "custom-home"

    app_with_mount = App(debug=True)
    app_with_mount.mount("/", lambda *_args: None)
    assert app_with_mount.ensure_dev_welcome_route() is False
    assert root_id not in app_with_mount.handler_index

    app_late_route = App(debug=True)
    assert app_late_route.ensure_dev_welcome_route() is True
    assert root_id in app_late_route._handlers

    @app_late_route.get("/")
    def late_index() -> str:
        return "late-home"

    assert app_late_route._handlers[root_id]() == "late-home"
    assert app_late_route.ensure_dev_welcome_route() is False

    app_late_post = App(debug=True)
    assert app_late_post.ensure_dev_welcome_route() is True

    @app_late_post.post("/")
    def late_post() -> str:
        return "posted"

    @app_late_post.get("/health")
    def late_health() -> str:
        return "healthy"

    assert root_id in app_late_post._handlers
    assert app_late_post.ensure_dev_welcome_route() is True
    assert _make_handler_id("POST", "/") in app_late_post._handlers
    assert _make_handler_id("GET", "/health") in app_late_post._handlers

    app_late_any = App(debug=True)
    assert app_late_any.ensure_dev_welcome_route() is True
    app_late_any.add_route(Route(method="ANY", path="/", handler=lambda: "any-root"))
    assert root_id not in app_late_any._handlers
    assert app_late_any.ensure_dev_welcome_route() is False

    app_late_mount = App(debug=True)
    assert app_late_mount.ensure_dev_welcome_route() is True
    app_late_mount.mount("/", lambda *_args: None)
    assert root_id not in app_late_mount._handlers
    assert root_id not in app_late_mount.handler_index


def test_welcome_page_assets_shipped_in_package_data() -> None:
    """Verify welcome.html and welcome.css are stored as package data assets under vertebra.framework.assets."""
    assets_dir = importlib.resources.files("vertebra.framework").joinpath("assets")
    html_asset = assets_dir.joinpath("welcome.html")
    css_asset = assets_dir.joinpath("welcome.css")
    assert html_asset.is_file()
    assert css_asset.is_file()
    assert "<!DOCTYPE html>" in html_asset.read_text(encoding="utf-8")
    assert "--bg-canvas" in css_asset.read_text(encoding="utf-8")


@pytest.mark.parametrize("docs_path", ["/docs", "/swagger"], ids=["docs", "swagger"])
def test_app_enable_docs_serves_html_for_docs_routes(docs_path: str) -> None:
    """Verify App.enable_docs() registers HTML doc routes for /docs and /swagger."""
    app = App(debug=True)
    app.enable_docs()
    docs_id = _make_handler_id("GET", docs_path)
    docs_resp = app._handlers[docs_id]()
    assert isinstance(docs_resp, Response)
    assert docs_resp.status == 200
    assert (b"content-type", b"text/html; charset=utf-8") in docs_resp.headers
    assert b"<!doctype html>" in bytes(docs_resp.body).lower()


def test_welcome_page_renderer_snapshot_and_html_escaping(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify WelcomePageRenderer.collect_snapshot and HTML escaping across routes, procedures, mounts, and docs."""
    monkeypatch.setenv("VERTEBRA_TOPOLOGY", "shm")
    monkeypatch.setenv("VERTEBRA_LOOP", "uvloop")
    monkeypatch.setenv("VERTEBRA_WORKERS", "4")
    monkeypatch.setenv("VERTEBRA_DEV", "1")

    welcome_cls = vtb_framework.WelcomePageRenderer
    app = App(debug=True)

    @app.get("/items", tags=("<script>alert(1)</script>", "core"))
    def list_items() -> list[str]:
        return ["a"]

    @query()
    async def fetch_status() -> str:
        return "ready"

    app.register(fetch_status)
    app.mount("/sub", lambda *_args: None, protocol="asgi")
    app.enable_docs()

    snapshot = welcome_cls.collect_snapshot(app)
    assert isinstance(snapshot, vtb_framework.WelcomeSnapshot)
    assert snapshot.topology == "SHM"
    assert snapshot.loop_backend == "uvloop"
    assert snapshot.workers == 4
    assert snapshot.openapi_enabled is True
    assert snapshot.docs_url == "/docs"
    assert snapshot.schema_url == "/openapi.json"
    assert "fetch_status" in snapshot.procedures
    assert "/sub (asgi)" in snapshot.mounts
    assert any(isinstance(r, vtb_framework.WelcomeRouteItem) and r.path == "/items" for r in snapshot.routes)

    rendered = welcome_cls.render_html(app)
    assert "<script>alert(1)</script>" not in rendered
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in rendered
    assert "fetch_status" in rendered
    assert "/sub (asgi)" in rendered
    assert '<link rel="stylesheet"' not in rendered.lower()
    assert '<script src="http' not in rendered.lower()
    assert '<img src="http' not in rendered.lower()
    assert "@import" not in rendered.lower()


def _generate_rsa_jwk_pair(kid: str) -> tuple[rsa.RSAPrivateKey, dict[str, Any]]:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = pyjwt.algorithms.RSAAlgorithm.to_jwk(private_key.public_key(), as_dict=True)
    jwk["kid"] = kid
    jwk["use"] = "sig"
    jwk["alg"] = "RS256"
    return private_key, jwk


@pytest.mark.anyio
async def test_jwks_verifier_caches_keys_and_coalesces_kid_rotation() -> None:
    rsa_priv_1, rsa_jwk_1 = _generate_rsa_jwk_pair("kid-1")
    ec_priv_2 = ec.generate_private_key(ec.SECP256R1())
    ec_jwk_2 = pyjwt.algorithms.ECAlgorithm.to_jwk(ec_priv_2.public_key(), as_dict=True)
    ec_jwk_2["kid"] = "kid-2"
    ec_jwk_2["use"] = "sig"
    ec_jwk_2["alg"] = "ES256"

    active_keys: list[dict[str, Any]] = [rsa_jwk_1]
    fetch_count = 0
    now_value = 1000.0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal fetch_count
        fetch_count += 1
        return httpx.Response(200, content=encode_json_bytes({"keys": active_keys}))

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    verifier = JWKSVerifier(
        JWKSConfig(
            jwks_url="https://idp.example.com/.well-known/jwks.json",
            issuer="https://idp.example.com",
            audience="vertebra-api",
            cache_ttl_seconds=300.0,
            min_refresh_interval_seconds=5.0,
        ),
        async_client=client,
        clock=lambda: now_value,
    )
    try:
        future_exp = int(time.time()) + 300
        tok1 = pyjwt.encode(
            {
                "sub": "user-1",
                "iss": "https://idp.example.com",
                "aud": "vertebra-api",
                "exp": future_exp,
                "scope": "read write",
            },
            rsa_priv_1,
            algorithm="RS256",
            headers={"kid": "kid-1"},
        )
        p1 = await verifier.verify_token(tok1)
        p2 = await verifier.verify_header(f"Bearer {tok1}")
        assert isinstance(p1, AuthenticatedPrincipal)
        assert p1.sub == "user-1"
        assert p1.scopes == ("read", "write")
        assert p1.is_authenticated is True
        assert p1.display_name == "user-1"
        assert p2 == p1
        assert fetch_count == 1
        assert await verifier.verify_header(None) is None
        assert await verifier.verify_header("Basic test-token-placeholder") is None

        active_keys = [rsa_jwk_1, ec_jwk_2]
        now_value += 10.0
        tok2 = pyjwt.encode(
            {
                "sub": "user-2",
                "iss": "https://idp.example.com",
                "aud": "vertebra-api",
                "exp": future_exp,
                "scopes": ["admin"],
            },
            ec_priv_2,
            algorithm="ES256",
            headers={"kid": "kid-2"},
        )
        results = await asyncio.gather(*(verifier.verify_token(tok2) for _ in range(5)))
        assert fetch_count == 2
        assert all(item.sub == "user-2" and item.scopes == ("admin",) for item in results)
    finally:
        await verifier.aclose()
        await client.aclose()


@pytest.mark.parametrize(
    "case",
    ["unknown-kid", "tampered-sig", "wrong-iss", "wrong-aud", "expired", "premature", "alg-none"],
)
def test_jwks_verifier_rejects_invalid_tokens_and_sync_fetch_errors(case: str) -> None:
    rsa_priv, rsa_jwk = _generate_rsa_jwk_pair("kid-main")
    other_priv, _ = _generate_rsa_jwk_pair("kid-other")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=encode_json_bytes({"keys": [rsa_jwk]}))

    sync_client = httpx.Client(transport=httpx.MockTransport(handler))
    verifier = JWKSVerifier(
        JWKSConfig(
            jwks_url="https://idp.example.com/jwks.json",
            issuer="https://idp.example.com",
            audience="api-svc",
        ),
        sync_client=sync_client,
    )
    try:
        now = int(time.time())
        if case == "unknown-kid":
            token = pyjwt.encode(
                {"sub": "u", "iss": "https://idp.example.com", "aud": "api-svc", "exp": now + 60},
                rsa_priv,
                algorithm="RS256",
                headers={"kid": "missing-kid"},
            )
        elif case == "tampered-sig":
            token = pyjwt.encode(
                {"sub": "u", "iss": "https://idp.example.com", "aud": "api-svc", "exp": now + 60},
                other_priv,
                algorithm="RS256",
                headers={"kid": "kid-main"},
            )
        elif case == "wrong-iss":
            token = pyjwt.encode(
                {"sub": "u", "iss": "https://evil.example.com", "aud": "api-svc", "exp": now + 60},
                rsa_priv,
                algorithm="RS256",
                headers={"kid": "kid-main"},
            )
        elif case == "wrong-aud":
            token = pyjwt.encode(
                {"sub": "u", "iss": "https://idp.example.com", "aud": "wrong-aud", "exp": now + 60},
                rsa_priv,
                algorithm="RS256",
                headers={"kid": "kid-main"},
            )
        elif case == "expired":
            token = pyjwt.encode(
                {"sub": "u", "iss": "https://idp.example.com", "aud": "api-svc", "exp": now - 60},
                rsa_priv,
                algorithm="RS256",
                headers={"kid": "kid-main"},
            )
        elif case == "premature":
            token = pyjwt.encode(
                {"sub": "u", "iss": "https://idp.example.com", "aud": "api-svc", "exp": now + 600, "nbf": now + 300},
                rsa_priv,
                algorithm="RS256",
                headers={"kid": "kid-main"},
            )
        else:
            token = pyjwt.encode(
                {"sub": "u", "iss": "https://idp.example.com", "aud": "api-svc", "exp": now + 60},
                "",
                algorithm="none",
                headers={"kid": "kid-main"},
            )

        with pytest.raises(ValueError):
            verifier.verify_token_sync(token)
    finally:
        verifier.close()
        sync_client.close()


@pytest.mark.anyio
async def test_jwks_verifier_fetch_failures_and_guards() -> None:
    rsa_priv, _ = _generate_rsa_jwk_pair("kid-1")
    token = pyjwt.encode(
        {"sub": "u", "exp": int(time.time()) + 60},
        rsa_priv,
        algorithm="RS256",
        headers={"kid": "kid-1"},
    )

    err_client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _r: httpx.Response(503)))
    err_verifier = JWKSVerifier(JWKSConfig(jwks_url="https://idp.example.com/jwks"), async_client=err_client)
    try:
        with pytest.raises(RuntimeError):
            await err_verifier.verify_token(token)
    finally:
        await err_verifier.aclose()
        await err_client.aclose()

    bad_json_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _r: httpx.Response(200, content=b"not-json"))
    )
    bad_json_verifier = JWKSVerifier(JWKSConfig(jwks_url="https://idp.example.com/jwks"), async_client=bad_json_client)
    try:
        with pytest.raises(ValueError):
            await bad_json_verifier.verify_token(token)
    finally:
        await bad_json_verifier.aclose()
        await bad_json_client.aclose()

    limiter = RateLimitGuard(capacity=2, refill_rate=0.0)
    assert limiter.remaining("k1") == 2
    assert limiter.allow("k1") is True
    assert limiter.remaining("k1") == 1
    assert limiter.allow("k1") is True
    assert limiter.allow("k1") is False
    assert limiter.reset_after("k1") == float("inf")

    secret = b"test-token-placeholder-secret-32-bytes"
    hmac_token = pyjwt.encode(
        {"sub": "hmac-user", "scope": "items:read", "exp": int(time.time()) + 60}, secret, algorithm="HS256"
    )
    guard = JWTAuthGuard(secret)
    assert guard.verify_header(None) is None
    assert guard.verify_header("Basic test-token-placeholder") is None
    principal = guard.verify_header(f"Bearer {hmac_token}")
    assert principal is not None
    assert principal.sub == "hmac-user"
    assert principal.scopes == ("items:read",)
    with pytest.raises(ValueError):
        guard.verify_header("Bearer test-token-placeholder")
