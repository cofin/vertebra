"""Tests for routing decorators and path parameter extraction."""

from uuid import UUID

import pytest

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

# ---------------------------------------------------------------------------
# TestDecorators
# ---------------------------------------------------------------------------

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
    decorator: object,
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
    def things() -> list:
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


# ---------------------------------------------------------------------------
# TestRouter
# ---------------------------------------------------------------------------


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
    def list_users() -> list:
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


# ---------------------------------------------------------------------------
# TestPathParamExtraction
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# TestHandlerIdPublicAPI
# ---------------------------------------------------------------------------


def test_handler_id_public_api_is_int() -> None:
    result = handler_id("GET", "/ping")
    assert isinstance(result, int)


def test_handler_id_public_api_is_stable() -> None:
    assert handler_id("GET", "/ping") == handler_id("GET", "/ping")


def test_handler_id_public_api_fits_uint64() -> None:
    assert 0 <= handler_id("DELETE", "/everything") < 2**64
