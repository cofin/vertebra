from typing import Any

import pytest

try:
    from litestar import Litestar, get, post, websocket
    from litestar.handlers import WebsocketListener

    HAS_LITESTAR = True
except ImportError:
    HAS_LITESTAR = False


@pytest.mark.skipif(not HAS_LITESTAR, reason="Litestar not installed")
def test_reflect_routes_basic() -> None:
    from vertebra.extensions.litestar.native import reflect_routes

    @get("/hello", sync_to_thread=False)
    def hello_handler() -> str:
        return "hello"

    @post("/echo", sync_to_thread=False)
    def echo_handler(data: dict[str, Any]) -> dict[str, Any]:
        return data

    app = Litestar(route_handlers=[hello_handler, echo_handler], openapi_config=None)

    routes = reflect_routes(app)

    # We check that our handlers are present
    hello_route = next((r for r in routes if r.path == "/hello"), None)
    assert hello_route is not None
    assert "GET" in hello_route.methods
    assert hello_route.handler == hello_handler.fn
    assert isinstance(hello_route.handler_id, int)

    # Check echo_handler
    echo_route = next((r for r in routes if r.path == "/echo"), None)
    assert echo_route is not None
    assert "POST" in echo_route.methods
    assert echo_route.handler == echo_handler.fn
    assert echo_route.handler_id != hello_route.handler_id


@pytest.mark.skipif(not HAS_LITESTAR, reason="Litestar not installed")
def test_reflect_routes_nested() -> None:
    from litestar import Controller, Router

    from vertebra.extensions.litestar.native import reflect_routes

    class MyController(Controller):
        path = "/controller"

        @get("/hello", sync_to_thread=False)
        def hello(self) -> str:
            return "hello"

    @get("/hello", sync_to_thread=False)
    def router_hello() -> str:
        return "hello"

    router = Router(path="/router", route_handlers=[router_hello])
    app = Litestar(route_handlers=[MyController, router], openapi_config=None)

    routes = reflect_routes(app)

    paths = {r.path for r in routes}
    assert "/controller/hello" in paths
    assert "/router/hello" in paths


@pytest.mark.skipif(not HAS_LITESTAR, reason="Litestar not installed")
def test_reflect_routes_multiple_methods() -> None:
    from litestar import route

    from vertebra.extensions.litestar.native import reflect_routes

    @route("/multi", http_method=["GET", "POST"], sync_to_thread=False)
    def multi_handler() -> str:
        return "multi"

    app = Litestar(route_handlers=[multi_handler], openapi_config=None)
    routes = reflect_routes(app)

    multi_route = next((r for r in routes if r.path == "/multi"), None)
    assert multi_route is not None
    assert "GET" in multi_route.methods
    assert "POST" in multi_route.methods
    assert len(multi_route.methods) == 2


@pytest.mark.skipif(not HAS_LITESTAR, reason="Litestar not installed")
def test_reflect_routes_websockets() -> None:
    from vertebra.extensions.litestar.native import reflect_routes

    @websocket("/ws")
    async def ws_handler(socket: Any) -> None:
        await socket.accept()
        await socket.send_text("hello")
        await socket.close()

    app = Litestar(route_handlers=[ws_handler])
    routes = reflect_routes(app)

    ws_route = next(r for r in routes if r.path == "/ws")
    assert ws_route.is_websocket is True


@pytest.mark.skipif(not HAS_LITESTAR, reason="Litestar not installed")
def test_build_router() -> None:
    from vertebra._vertebra import Router
    from vertebra.extensions.litestar.native import build_router, reflect_routes

    @get("/hello/{name:str}", sync_to_thread=False)
    def hello_handler(name: str) -> str:
        return f"hello {name}"

    app = Litestar(route_handlers=[hello_handler], openapi_config=None)
    routes = reflect_routes(app)
    router = build_router(routes)

    assert isinstance(router, Router)
    assert router.match_path("/GET/hello/world") == id(hello_handler.fn)
