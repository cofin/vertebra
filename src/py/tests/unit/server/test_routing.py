"""Tests for routing decorators and path parameter extraction."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Callable
from typing import Any
from uuid import UUID

import pytest

from vertebra._vertebra import Response as NativeResponse
from vertebra.db.arrow import ArrowCursor
from vertebra.framework._router import (
    Router,
    delete,
    extract_path_params,
    get,
    handler_id,
    patch,
    post,
    put,
)
from vertebra.server._fastpath_core import (
    CompiledRoute,
    RouteFastPathTable,
    serialize_buffered_response,
    serialize_fastpath_special_result,
)
from vertebra.server._offload import (
    CompressionOffloadSpec,
    CorsOffloadSpec,
    OffloadPlan,
    OpenApiOffloadSpec,
    StaticMountOffloadSpec,
    evaluate_cors_preflight,
    evaluate_cors_response_headers,
    evaluate_openapi_cache_request,
    is_rust_offload_active,
)
from vertebra.server._routing import (
    BaseRouteCompiler,
    make_handler_id,
)
from vertebra.utils.serializers import decode_json

_DECORATOR_CASES = [
    (get, "GET", "/items"),
    (post, "POST", "/items"),
    (put, "PUT", "/items/{item_id:int}"),
    (delete, "DELETE", "/items/{item_id:int}"),
    (patch, "PATCH", "/items/{item_id:int}"),
]


@pytest.mark.parametrize(
    ("decorator", "expected_method", "path"),
    _DECORATOR_CASES,
    ids=[c[1] for c in _DECORATOR_CASES],
)
def test_decorators_attaches_metadata(
    decorator: Callable[[str], Callable[[Callable[[], None]], Callable[[], None]]],
    expected_method: str,
    path: str,
) -> None:
    @decorator(path)
    def handler() -> None:
        pass

    routes = getattr(handler, "__vertebra_routes__", [])
    assert len(routes) == 1
    assert routes[0].method == expected_method
    assert routes[0].path == path


def test_decorators_stacked_decorators_add_multiple_routes() -> None:
    @get("/things")
    @post("/things")
    def things() -> list[Any]:
        return []

    routes = getattr(things, "__vertebra_routes__", [])
    methods = [r.method for r in routes]
    assert "GET" in methods
    assert "POST" in methods


def test_decorators_handler_returns_original_callable() -> None:
    @get("/noop")
    def noop() -> None:
        pass

    assert callable(noop)
    assert noop() is None


@pytest.mark.parametrize(
    ("prefix", "expected_path"),
    [
        ("/api/v1", "/api/v1/users"),
        ("/api/", "/api/users"),
    ],
    ids=["applies_prefix", "strips_trailing_slash"],
)
def test_router_prefix(prefix: str, expected_path: str) -> None:
    router = Router(prefix)

    @get("/users")
    def list_users() -> list[Any]:
        return []

    router.register(list_users)
    assert router.routes[0].path == expected_path


def test_router_empty_prefix_is_identity() -> None:
    router = Router("")

    @get("/x")
    def x() -> None:
        pass

    router.register(x)
    assert router.routes[0].path == "/x"


def test_path_param_extraction_integer_param() -> None:
    params = extract_path_params("/users/{user_id:int}", "/users/42")
    assert params == {"user_id": 42}


def test_path_param_extraction_string_param() -> None:
    params = extract_path_params("/users/{username}", "/users/alice")
    assert params == {"username": "alice"}


def test_path_param_extraction_uuid_param() -> None:
    uuid_str = "550e8400-e29b-41d4-a716-446655440000"
    params = extract_path_params("/items/{item_id:uuid}", f"/items/{uuid_str}")
    assert params == {"item_id": UUID(uuid_str)}


@pytest.mark.parametrize(
    ("path_value", "expected"),
    [
        ("true", True),
        ("false", False),
    ],
    ids=["true", "false"],
)
def test_path_param_extraction_bool_param(path_value: str, expected: bool) -> None:
    params = extract_path_params("/flag/{active:bool}", f"/flag/{path_value}")
    assert params == {"active": expected}


def test_path_param_extraction_float_param() -> None:
    params = extract_path_params("/scale/{factor:float}", "/scale/3.14")
    assert params["factor"] == pytest.approx(3.14)


def test_path_param_extraction_multiple_params() -> None:
    params = extract_path_params(
        "/users/{user_id:int}/posts/{post_id:int}",
        "/users/1/posts/99",
    )
    assert params == {"user_id": 1, "post_id": 99}


def test_path_param_extraction_path_mismatch_raises() -> None:
    with pytest.raises(ValueError):
        extract_path_params("/items/{item_id:int}", "/items/abc")


def test_path_param_extraction_too_many_segments_raises() -> None:
    with pytest.raises(ValueError):
        extract_path_params("/items/{item_id:int}", "/items/1/extra")


def test_handler_id_public_api_is_int() -> None:
    result = handler_id("GET", "/ping")
    assert isinstance(result, int)


def test_handler_id_public_api_is_stable() -> None:
    assert handler_id("GET", "/ping") == handler_id("GET", "/ping")


def test_handler_id_public_api_fits_uint64() -> None:
    assert 0 <= handler_id("DELETE", "/everything") < 2**64


def test_base_route_compiler_and_router_table_static_before_typed_lookup() -> None:
    compiler = BaseRouteCompiler()
    r_typed = compiler.compile_route("GET", "/users/{user_id:int}", name="user-detail")
    r_slug = compiler.compile_route("GET", "/users/{username}", name="user-slug")
    r_static = compiler.compile_route("GET", "/users/me", name="user-me")
    r_catchall = compiler.compile_route("GET", "/files/{*filepath}", name="files")
    assert r_typed.handler_id < r_slug.handler_id < r_static.handler_id < r_catchall.handler_id
    assert len({r_typed.handler_id, r_slug.handler_id, r_static.handler_id, r_catchall.handler_id}) == 4

    table = compiler.build_table([r_typed, r_slug, r_static, r_catchall])
    r_post = table.add_route("POST", "/users/me")
    assert r_post.handler_id > r_catchall.handler_id
    assert table.get_by_handler_id(r_static.handler_id) == r_static

    static_match = table.match_with_params("GET", "/users/me")
    assert static_match == (r_static, {})

    int_match = table.match_with_params("GET", "/users/42")
    assert int_match == (r_typed, {"user_id": 42})

    slug_match = table.match_with_params("GET", "/users/alice")
    assert slug_match == (r_slug, {"username": "alice"})

    catchall_match = table.match_with_params("GET", "/files/assets/app.js")
    assert catchall_match == (r_catchall, {"filepath": "assets/app.js"})

    assert table.allowed_methods("/users/me") == ["GET", "POST"]
    assert make_handler_id("GET", "/items", name="a") != make_handler_id("GET", "/items", name="b")


@pytest.mark.parametrize(
    ("method", "headers", "allow_origins", "allow_credentials", "expected_status", "expected_origin"),
    [
        (
            "GET",
            [("Origin", "https://app.example.com"), ("Access-Control-Request-Method", "POST")],
            ("https://app.example.com",),
            False,
            None,
            None,
        ),
        (
            "OPTIONS",
            [("Origin", "https://app.example.com")],
            ("https://app.example.com",),
            False,
            None,
            None,
        ),
        (
            "OPTIONS",
            [("Access-Control-Request-Method", "POST")],
            ("https://app.example.com",),
            False,
            None,
            None,
        ),
        (
            "OPTIONS",
            [("OrIgIn", "https://evil.example.com"), ("AcCeSs-CoNtRoL-rEqUeSt-MeThOd", "POST")],
            ("https://app.example.com",),
            False,
            403,
            None,
        ),
        (
            "OPTIONS",
            [("OrIgIn", "https://app.example.com"), ("Access-Control-Request-Method", "POST")],
            ("*",),
            False,
            204,
            "*",
        ),
        (
            "OPTIONS",
            [("Origin", "https://app.example.com"), ("Access-Control-Request-Method", "POST")],
            ("*",),
            True,
            204,
            "https://app.example.com",
        ),
        (
            "OPTIONS",
            [("Origin", "https://app.example.com"), ("Access-Control-Request-Method", "DELETE")],
            ("https://app.example.com",),
            False,
            400,
            None,
        ),
    ],
    ids=[
        "non-options-returns-none",
        "missing-acrm-returns-none",
        "missing-origin-returns-none",
        "disallowed-origin-403",
        "wildcard-no-credentials",
        "wildcard-with-credentials-echoes-origin",
        "disallowed-method-400",
    ],
)
def test_evaluate_cors_preflight_and_response_headers(
    method: str,
    headers: list[tuple[str, str]],
    allow_origins: tuple[str, ...],
    allow_credentials: bool,
    expected_status: int | None,
    expected_origin: str | None,
) -> None:
    spec = CorsOffloadSpec(
        allow_origins=allow_origins,
        allow_methods=("GET", "POST"),
        allow_headers=("Authorization",),
        allow_credentials=allow_credentials,
        expose_headers=("X-Request-Id",),
        max_age=600,
    )
    snapshot = list(headers)
    result = evaluate_cors_preflight(spec, headers, method=method)
    assert headers == snapshot
    if expected_status is None:
        assert result is None
    else:
        assert result is not None
        status, resp_headers = result
        assert status == expected_status
        header_map = dict(resp_headers)
        if expected_origin is not None:
            assert header_map["access-control-allow-origin"] == expected_origin
            if allow_credentials:
                assert header_map["access-control-allow-credentials"] == "true"

    resp_cors = evaluate_cors_response_headers(spec, headers)
    assert headers == snapshot
    if expected_status == 204 and expected_origin is not None:
        assert dict(resp_cors)["access-control-allow-origin"] == expected_origin
    elif expected_status == 403:
        assert resp_cors == []


@pytest.mark.parametrize(
    ("headers", "expected_status", "expected_body_empty"),
    [
        (None, 200, False),
        ([("If-None-Match", '"deadbeef"')], 304, True),
        ([("IF-NONE-MATCH", 'W/"deadbeef"')], 304, True),
        ([("if-none-match", 'w/"deadbeef"')], 304, True),
        ([("If-None-Match", '"other", "deadbeef"')], 304, True),
        ([("If-None-Match", "*")], 304, True),
        ([("If-None-Match", '"mismatch"')], 200, False),
    ],
    ids=["absent", "strong", "weak-upper", "weak-lower", "list-match", "wildcard", "mismatch"],
)
def test_evaluate_openapi_cache_request_etag_matching(
    headers: list[tuple[str, str]] | None,
    expected_status: int,
    expected_body_empty: bool,
) -> None:
    spec = OpenApiOffloadSpec(schema_path="/openapi.json", body=b'{"openapi":"3.1.0"}', etag='"deadbeef"')
    status, resp_headers, body = evaluate_openapi_cache_request(spec, headers)
    assert status == expected_status
    header_map = dict(resp_headers)
    assert header_map["etag"] == '"deadbeef"'
    assert "cache-control" in header_map
    assert (body == b"") is expected_body_empty


def test_offload_plan_serialization_and_rust_offload_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    plan = OffloadPlan(
        compression=CompressionOffloadSpec(
            backend="gzip",
            minimum_size=500,
            gzip_compress_level=6,
            brotli_quality=4,
            zstd_level=3,
            exclude_patterns=("^/health$",),
            exclude_opt_key=None,
            excluded_paths=("/metrics",),
        ),
        cors=CorsOffloadSpec(
            allow_origins=("https://app.example.com",),
            allow_methods=("GET",),
            allow_headers=("Authorization",),
            allow_credentials=False,
            expose_headers=(),
            max_age=60,
        ),
        static_mounts=(
            StaticMountOffloadSpec(
                path_prefix="/static",
                directories=("/tmp/static",),
                html_mode=False,
                send_as_attachment=False,
                headers={"cache-control": "max-age=3600"},
            ),
        ),
        openapi=OpenApiOffloadSpec(schema_path="/openapi.json", body=b"{}", etag='"1"'),
    )
    decoded = decode_json(plan.to_server_config_json())
    assert decoded["compression"]["backend"] == "gzip"
    assert decoded["cors"]["allow_origins"] == ["https://app.example.com"]
    assert decoded["static_mounts"][0]["path_prefix"] == "/static"
    assert decoded["openapi"]["etag"] == '"1"'

    monkeypatch.delenv("VERTEBRA_SERVER_ACTIVE", raising=False)
    monkeypatch.delenv("VERTEBRA_RUST_OFFLOAD", raising=False)
    assert is_rust_offload_active() is False
    assert is_rust_offload_active({"rust_offload_active": True}) is True
    monkeypatch.setenv("VERTEBRA_RUST_OFFLOAD", "true")
    assert is_rust_offload_active() is True


def test_fastpath_core_table_and_serializers() -> None:
    table = RouteFastPathTable()
    route = CompiledRoute(
        handler_id=make_handler_id("GET", "/items/{item_id}"), method="GET", path_template="/items/{item_id:int}"
    )
    table.register(route)
    assert table.get(route.handler_id) == route
    matched = table.match("GET", "/items/7")
    assert matched == (route, {"item_id": 7})

    cursor = ArrowCursor(bytearray(b"ARROW1\x00\x00test"), rows=[(1,)], columns=["id"])
    special = serialize_fastpath_special_result(cursor, default_status=200)
    assert special is not None and special[0] == 200

    native_resp = NativeResponse(status=201, headers=[("content-type", "text/plain")], body=b"ok")
    buffered = serialize_buffered_response(native_resp)
    assert buffered == (201, [(b"content-type", b"text/plain")], b"ok")


def test_server_neutral_modules_import_without_host_frameworks() -> None:
    script = (
        "import sys; "
        "import vertebra.server._offload, vertebra.server._routing, vertebra.server._fastpath_core; "
        "blocked = {'litestar', 'starlette', 'fastapi', 'flask', 'django'}; "
        "loaded = blocked.intersection(sys.modules); "
        "assert not loaded, f'Unexpected framework imports: {loaded}'"
    )
    proc = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr or proc.stdout
