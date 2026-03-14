"""Tests for routing decorators and path parameter extraction."""

from __future__ import annotations

from uuid import UUID

import pytest

from vertebra.web.routing import (
    Router,
    delete,
    extract_path_params,
    get,
    handler_id,
    patch,
    post,
    put,
)


class TestDecorators:
    def test_get_attaches_metadata(self) -> None:
        @get("/items")
        def list_items() -> list:
            return []

        routes = getattr(list_items, "__vertebra_routes__", [])
        assert len(routes) == 1
        assert routes[0].method == "GET"
        assert routes[0].path == "/items"

    def test_post_attaches_metadata(self) -> None:
        @post("/items")
        def create() -> dict:
            return {}

        routes = getattr(create, "__vertebra_routes__", [])
        assert routes[0].method == "POST"

    def test_put_attaches_metadata(self) -> None:
        @put("/items/{item_id:int}")
        def update(item_id: int) -> dict:
            return {}

        routes = getattr(update, "__vertebra_routes__", [])
        assert routes[0].method == "PUT"

    def test_delete_attaches_metadata(self) -> None:
        @delete("/items/{item_id:int}")
        def remove(item_id: int) -> None:
            pass

        routes = getattr(remove, "__vertebra_routes__", [])
        assert routes[0].method == "DELETE"

    def test_patch_attaches_metadata(self) -> None:
        @patch("/items/{item_id:int}")
        def partial_update(item_id: int) -> dict:
            return {}

        routes = getattr(partial_update, "__vertebra_routes__", [])
        assert routes[0].method == "PATCH"

    def test_stacked_decorators_add_multiple_routes(self) -> None:
        @get("/things")
        @post("/things")
        def things() -> list:
            return []

        routes = getattr(things, "__vertebra_routes__", [])
        methods = [r.method for r in routes]
        assert "GET" in methods
        assert "POST" in methods

    def test_handler_returns_original_callable(self) -> None:
        @get("/noop")
        def noop() -> None:
            pass

        assert callable(noop)
        assert noop() is None


class TestRouter:
    def test_router_applies_prefix(self) -> None:
        router = Router("/api/v1")

        @get("/users")
        def list_users() -> list:
            return []

        router.register(list_users)
        assert router.routes[0].path == "/api/v1/users"

    def test_router_strips_trailing_slash_from_prefix(self) -> None:
        router = Router("/api/")

        @get("/users")
        def list_users() -> list:
            return []

        router.register(list_users)
        assert router.routes[0].path == "/api/users"

    def test_empty_prefix_is_identity(self) -> None:
        router = Router("")

        @get("/x")
        def x() -> None:
            pass

        router.register(x)
        assert router.routes[0].path == "/x"


class TestPathParamExtraction:
    def test_integer_param(self) -> None:
        params = extract_path_params("/users/{user_id:int}", "/users/42")
        assert params == {"user_id": 42}

    def test_string_param(self) -> None:
        params = extract_path_params("/users/{username}", "/users/alice")
        assert params == {"username": "alice"}

    def test_uuid_param(self) -> None:
        uuid_str = "550e8400-e29b-41d4-a716-446655440000"
        params = extract_path_params("/items/{item_id:uuid}", f"/items/{uuid_str}")
        assert params == {"item_id": UUID(uuid_str)}

    def test_bool_param_true(self) -> None:
        params = extract_path_params("/flag/{active:bool}", "/flag/true")
        assert params == {"active": True}

    def test_bool_param_false(self) -> None:
        params = extract_path_params("/flag/{active:bool}", "/flag/false")
        assert params == {"active": False}

    def test_float_param(self) -> None:
        params = extract_path_params("/scale/{factor:float}", "/scale/3.14")
        assert params["factor"] == pytest.approx(3.14)

    def test_multiple_params(self) -> None:
        params = extract_path_params(
            "/users/{user_id:int}/posts/{post_id:int}",
            "/users/1/posts/99",
        )
        assert params == {"user_id": 1, "post_id": 99}

    def test_path_mismatch_raises(self) -> None:
        with pytest.raises(ValueError):
            extract_path_params("/items/{item_id:int}", "/items/abc")

    def test_too_many_segments_raises(self) -> None:
        with pytest.raises(ValueError):
            extract_path_params("/items/{item_id:int}", "/items/1/extra")


class TestHandlerIdPublicAPI:
    def test_handler_id_is_int(self) -> None:
        result = handler_id("GET", "/ping")
        assert isinstance(result, int)

    def test_handler_id_is_stable(self) -> None:
        assert handler_id("GET", "/ping") == handler_id("GET", "/ping")

    def test_handler_id_fits_uint64(self) -> None:
        assert 0 <= handler_id("DELETE", "/everything") < 2**64
