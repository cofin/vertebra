from __future__ import annotations

from uuid import UUID

import pytest

from vertebra.web.routing import RouteDef, Router, delete, extract_path_params, get, patch, post


@get("/items/{item_id:int}")
@post("/items")
def items_handler() -> str:
    return "ok"


@delete("/items/{item_id:int}")
@patch("/items/{item_id:int}")
def item_handler() -> str:
    return "ok"


def test_route_decorators_attach_metadata() -> None:
    routes = getattr(items_handler, "__vertebra_routes__", [])
    assert [route.method for route in routes] == ["POST", "GET"]
    assert [route.path for route in routes] == ["/items", "/items/{item_id:int}"]


def test_router_groups_routes_with_prefix() -> None:
    router = Router("/api")
    router.register(items_handler)
    router.register(item_handler)

    paths = [route.path for route in router.routes]
    assert paths == ["/api/items", "/api/items/{item_id:int}", "/api/items/{item_id:int}", "/api/items/{item_id:int}"]

    methods = [route.method for route in router.routes]
    assert methods == ["POST", "GET", "PATCH", "DELETE"]


def test_extract_path_params_and_coercion() -> None:
    params = extract_path_params(
        "/users/{user_id:int}/tokens/{token_id:uuid}/active/{active:bool}",
        "/users/42/tokens/550e8400-e29b-41d4-a716-446655440000/active/true",
    )
    assert params == {"user_id": 42, "token_id": UUID("550e8400-e29b-41d4-a716-446655440000"), "active": True}


def test_extract_path_params_rejects_mismatch() -> None:
    with pytest.raises(ValueError):
        extract_path_params("/items/{item_id:int}", "/items/not-a-number")


def test_route_def_full_path() -> None:
    route = RouteDef(method="GET", path="/items", handler=items_handler)
    assert route.with_prefix("/api").path == "/api/items"
