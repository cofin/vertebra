"""Tests for VertebraApp route registration and handler ID stability."""

import pytest

from vertebra.framework._app import HandlerInfo, VertebraApp, _make_handler_id
from vertebra.framework._router import handler_id


class TestHandlerIdStability:
    def test_same_method_path_same_id(self) -> None:
        assert _make_handler_id("GET", "/ping") == _make_handler_id("GET", "/ping")

    def test_different_method_different_id(self) -> None:
        assert _make_handler_id("GET", "/ping") != _make_handler_id("POST", "/ping")

    def test_different_path_different_id(self) -> None:
        assert _make_handler_id("GET", "/ping") != _make_handler_id("GET", "/pong")

    def test_case_sensitive_path(self) -> None:
        assert _make_handler_id("GET", "/Ping") != _make_handler_id("GET", "/ping")

    def test_method_normalized_to_upper(self) -> None:
        assert _make_handler_id("get", "/ping") == _make_handler_id("GET", "/ping")

    def test_path_normalized_via_app_route(self) -> None:
        """Path normalization happens in VertebraApp._route, not _make_handler_id."""
        app = VertebraApp()

        @app.get("ping")  # without leading slash
        def handler() -> str:
            return "ok"

        # The route was normalized to /ping
        assert app.routes[0].path == "/ping"
        expected_id = _make_handler_id("GET", "/ping")
        assert expected_id in app.handler_index

    def test_result_fits_uint64(self) -> None:
        value = _make_handler_id("GET", "/x")
        assert 0 <= value < 2**64

    def test_routing_module_handler_id_matches(self) -> None:
        """handler_id() from routing module must produce identical values."""
        assert handler_id("GET", "/ping") == _make_handler_id("GET", "/ping")


class TestVertebraAppRouteRegistration:
    def test_get_decorator_registers_route(self) -> None:
        app = VertebraApp()

        @app.get("/ping")
        def ping() -> str:
            return "pong"

        assert len(app.routes) == 1
        assert app.routes[0].method == "GET"
        assert app.routes[0].path == "/ping"

    def test_multiple_methods_on_same_path(self) -> None:
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

    def test_handler_index_populated(self) -> None:
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

    def test_handler_index_last_registration_wins(self) -> None:
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

    def test_handlers_dict_populated(self) -> None:
        app = VertebraApp()

        @app.get("/test")
        def test_handler() -> str:
            return "ok"

        expected_id = _make_handler_id("GET", "/test")
        assert expected_id in app._handlers  # noqa: SLF001
        assert app._handlers[expected_id] is test_handler  # noqa: SLF001

    def test_mount_records_sub_app(self) -> None:
        app = VertebraApp()
        sentinel = object()
        app.mount("/static", sentinel)
        assert len(app.mounts) == 1
        prefix, mounted, protocol = app.mounts[0]
        assert prefix == "/static"
        assert mounted is sentinel
        assert protocol == "auto"

    def test_mount_with_protocol(self) -> None:
        app = VertebraApp()
        sentinel = object()
        app.mount("/api", sentinel, protocol="wsgi")
        prefix, mounted, protocol = app.mounts[0]
        assert protocol == "wsgi"

    def test_handler_info_is_msgspec_struct(self) -> None:
        import msgspec

        assert issubclass(HandlerInfo, msgspec.Struct)

    @pytest.mark.anyio
    async def test_lifecycle_hooks_delegate_to_inner_app(self) -> None:
        app = VertebraApp()
        calls: list[str] = []

        @app.on_startup
        async def startup() -> None:
            calls.append("startup")

        await app.startup()
        assert calls == ["startup"]
