"""Unit tests for 20-slot AsgiScope acceleration and Litestar v2/v3 route extraction."""

import importlib
from collections.abc import Callable
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest

from vertebra._vertebra import AsgiScope
from vertebra.server._asgi._litestar_router import (
    ExtractedLitestarRoute,
    LitestarRouterTable,
    _unpack_route_handler_map_entry,
    convert_litestar_path_params,
    extract_litestar_routes,
    normalize_litestar_path_for_matchit,
)


def _load_litestar_primitives() -> tuple[Any, Any, Any]:
    """Load Litestar routing primitives or fallback stubs when optional extra is absent."""
    try:
        mod = importlib.import_module("litestar")
    except ImportError:

        def make_decorator(http_method: str) -> Callable[..., Callable[[Callable[..., Any]], Any]]:
            def decorator(path: str, **_kwargs: Any) -> Callable[[Callable[..., Any]], Any]:
                def wrap(fn: Callable[..., Any]) -> Any:
                    return SimpleNamespace(
                        paths={path},
                        http_methods={http_method},
                        fn=fn,
                    )

                return wrap

            return decorator

        class LitestarStub:
            """Minimal Litestar application stub for route extraction tests."""

            def __init__(self, route_handlers: list[Any] | None = None, **_kwargs: Any) -> None:
                by_path: dict[str, dict[str, Any]] = {}
                for handler in route_handlers or ():
                    paths = getattr(handler, "paths", ("/",))
                    methods = getattr(handler, "http_methods", ("GET",))
                    for path in paths:
                        method_map = by_path.setdefault(path, {})
                        for method in methods:
                            method_map[method] = handler
                self.routes = [
                    SimpleNamespace(path=path, route_handler_map=method_map, path_parameters=())
                    for path, method_map in by_path.items()
                ]

        return LitestarStub, make_decorator("GET"), make_decorator("POST")
    return mod.Litestar, mod.get, mod.post


Litestar, get, post = _load_litestar_primitives()


def test_asgi_scope_20_known_keys_no_extras_allocation() -> None:
    """Verify setting Litestar's 6 per-request keys uses fixed slots without allocating extras."""
    scope = AsgiScope()
    assert len(scope.known_keys()) == 20
    assert scope.has_extras() is False
    assert "route_handler" not in scope
    assert "path_params" not in scope
    assert "connection" not in scope

    marker_app = object()
    marker_handler = object()
    marker_connection = object()
    path_params = {"user_id": 42}

    scope["app"] = marker_app
    scope["litestar_app"] = marker_app
    scope["path_params"] = path_params
    scope["route_handler"] = marker_handler
    scope["path_template"] = "/users/{user_id:int}"
    scope["connection"] = marker_connection

    assert scope.has_extras() is False
    assert scope["app"] is marker_app
    assert scope["litestar_app"] is marker_app
    assert scope["path_params"] is path_params
    assert scope["route_handler"] is marker_handler
    assert scope["path_template"] == "/users/{user_id:int}"
    assert scope["connection"] is marker_connection


def test_asgi_scope_setdefault_adopts_caller_dict() -> None:
    """Verify scope.setdefault('state', d) adopts d directly without allocating a second dict."""
    scope = AsgiScope()
    caller_state: dict[str, Any] = {"seeded": True}
    returned = scope.setdefault("state", caller_state)
    assert returned is caller_state
    assert scope["state"] is caller_state

    second_dict: dict[str, Any] = {"other": True}
    assert scope.setdefault("state", second_dict) is caller_state


def test_normalize_all_10_litestar_path_parameter_types() -> None:
    """Verify all 10 Litestar path parameter types normalize to matchit 0.9 syntax."""
    cases = [
        ("/users/{name:str}", "/users/{name}"),
        ("/users/{user_id:int}", "/users/{user_id}"),
        ("/metrics/{score:float}", "/metrics/{score}"),
        ("/objects/{obj_id:uuid}", "/objects/{obj_id}"),
        ("/ledger/{amount:decimal}", "/ledger/{amount}"),
        ("/events/{day:date}", "/events/{day}"),
        ("/events/{ts:datetime}", "/events/{ts}"),
        ("/alarms/{at:time}", "/alarms/{at}"),
        ("/windows/{span:timedelta}", "/windows/{span}"),
        ("/files/{file_path:path}", "/files/{*file_path}"),
    ]
    for raw_path, expected in cases:
        assert normalize_litestar_path_for_matchit(raw_path) == expected


def test_convert_all_10_litestar_path_parameter_values() -> None:
    """Verify pre-compiled converters parse all 10 Litestar path parameter types."""
    param_types = {
        "name": "str",
        "user_id": "int",
        "score": "float",
        "obj_id": "uuid",
        "amount": "decimal",
        "day": "date",
        "ts": "datetime",
        "at": "time",
        "span": "timedelta",
        "file_path": "path",
    }
    raw = {
        "name": "alice",
        "user_id": "42",
        "score": "3.5",
        "obj_id": "12345678-1234-5678-1234-567812345678",
        "amount": "19.99",
        "day": "2026-10-20",
        "ts": "2026-10-20T12:30:00",
        "at": "12:30:00",
        "span": "PT15M",
        "file_path": "/a/b/c.txt",
    }
    parsed = convert_litestar_path_params(raw, param_types)
    assert parsed["name"] == "alice"
    assert parsed["user_id"] == 42
    assert parsed["score"] == pytest.approx(3.5)
    assert parsed["obj_id"] == UUID("12345678-1234-5678-1234-567812345678")
    assert parsed["amount"] == Decimal("19.99")
    assert parsed["day"] == date(2026, 10, 20)
    assert parsed["ts"] == datetime(2026, 10, 20, 12, 30, 0)
    assert parsed["at"] == time(12, 30, 0)
    assert parsed["span"] == timedelta(minutes=15)
    assert parsed["file_path"] == "a/b/c.txt"


def test_unpack_route_handler_map_entry_supports_v2_and_v3() -> None:
    """Verify _unpack_route_handler_map_entry handles both v2 2-tuples and v3 direct handlers."""
    handler_obj = SimpleNamespace(name="v3_handler")
    kwargs_model_obj = SimpleNamespace(name="v2_kwargs")

    unpacked_v2 = _unpack_route_handler_map_entry((handler_obj, kwargs_model_obj))
    assert unpacked_v2 == (handler_obj, kwargs_model_obj)

    unpacked_v3 = _unpack_route_handler_map_entry(handler_obj)
    assert unpacked_v3 == (handler_obj, None)


def test_extract_litestar_routes_and_405_allow_discovery() -> None:
    """Verify extract_litestar_routes inspects app.routes and discovers 405 Allow headers."""

    @get("/items/{item_id:int}", sync_to_thread=False)
    def get_item(item_id: int) -> dict[str, int]:
        return {"item_id": item_id}

    @post("/items/{item_id:int}", sync_to_thread=False)
    def update_item(item_id: int) -> dict[str, int]:
        return {"item_id": item_id}

    app = Litestar(route_handlers=[get_item, update_item])
    table: LitestarRouterTable = extract_litestar_routes(app)

    methods = {(entry.method, entry.matchit_path) for entry in table.routes.values()}
    assert ("GET", "/items/{item_id}") in methods
    assert ("POST", "/items/{item_id}") in methods

    matched_get = table.match("GET", "/items/99")
    assert isinstance(matched_get, ExtractedLitestarRoute)
    assert table.convert_params(matched_get, {"item_id": "99"}) == {"item_id": 99}

    allowed = table.allowed_methods("/items/99")
    assert "GET" in allowed
    assert "POST" in allowed
