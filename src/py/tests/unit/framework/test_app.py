"""Tests for VertebraApp route registration and handler ID stability."""

import pytest

from vertebra.framework._app import HandlerInfo, VertebraApp, _make_handler_id
from vertebra.framework._router import handler_id


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
    """Path normalization happens in VertebraApp._route, not _make_handler_id."""
    app = VertebraApp()

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
    app = VertebraApp()

    @app.get("/ping")
    def ping() -> str:
        return "pong"

    assert len(app.routes) == 1
    assert app.routes[0].method == "GET"
    assert app.routes[0].path == "/ping"


def test_vertebra_app_route_registration_multiple_methods_on_same_path() -> None:
    app = VertebraApp()

    @app.get("/items")
    def list_items() -> list:
        return []

    @app.post("/items")
    def create_item() -> dict:
        return {}

    assert len(app.routes) == 2
    methods = {r.method for r in app.routes}
    assert methods == {"GET", "POST"}


def test_vertebra_app_route_registration_handler_index_populated() -> None:
    app = VertebraApp()

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
    app = VertebraApp()

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
    app = VertebraApp()

    @app.get("/test")
    def test_handler() -> str:
        return "ok"

    expected_id = _make_handler_id("GET", "/test")
    assert expected_id in app._handlers  # noqa: SLF001
    assert app._handlers[expected_id] is test_handler  # noqa: SLF001


def test_vertebra_app_route_registration_mount_records_sub_app() -> None:
    app = VertebraApp()
    sentinel = object()
    app.mount("/static", sentinel)
    assert len(app.mounts) == 1
    prefix, mounted, protocol = app.mounts[0]
    assert prefix == "/static"
    assert mounted is sentinel
    assert protocol == "auto"


def test_vertebra_app_route_registration_mount_with_protocol() -> None:
    app = VertebraApp()
    sentinel = object()
    app.mount("/api", sentinel, protocol="wsgi")
    prefix, mounted, protocol = app.mounts[0]
    assert protocol == "wsgi"


def test_vertebra_app_route_registration_handler_info_is_msgspec_struct() -> None:
    import msgspec

    assert issubclass(HandlerInfo, msgspec.Struct)


@pytest.mark.anyio
async def test_vertebra_app_route_registration_lifecycle_hooks_delegate_to_inner_app() -> None:
    app = VertebraApp()
    calls: list[str] = []

    @app.on_startup
    async def startup() -> None:
        calls.append("startup")

    await app.startup()
    assert calls == ["startup"]
