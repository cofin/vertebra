from typing import Annotated

import pytest
from litestar import Controller, Litestar, get, post
from litestar.datastructures import UploadFile
from litestar.di import Provide
from litestar.enums import RequestEncodingType
from litestar.params import Body, Dependency
from litestar.response import File
from litestar.static_files import create_static_files_router

from vertebra.extensions.litestar.native import RouteCapability, classify_route, reflect_routes


@pytest.mark.integration
async def test_native_supported_patterns() -> None:
    """Test patterns that should be classified as NATIVE."""

    def dependency_fn() -> str:
        return "dep"

    class NativeController(Controller):
        path = "/native"

        @get("/{id:uuid}")
        async def get_resource(self, resource_id: str) -> dict:
            return {"id": resource_id}

        @post("/")
        async def create_resource(self, data: dict, dep: str) -> dict:
            return {"data": data, "dep": dep}

    app = Litestar(route_handlers=[NativeController], dependencies={"dep": Provide(dependency_fn)})

    routes = reflect_routes(app)

    for route in routes:
        classification = classify_route(route)
        assert classification == RouteCapability.NATIVE, f"Route {route.path} should be NATIVE"


@pytest.mark.integration
async def test_fallback_patterns() -> None:
    """Test patterns that should be classified as ASGI_FALLBACK."""

    # 1. Multipart Upload
    class MultipartController(Controller):
        path = "/multipart"

        @post("/", media_type=RequestEncodingType.MULTI_PART)
        async def upload(self, data: Annotated[bytes, Body(media_type=RequestEncodingType.MULTI_PART)]) -> dict:
            return {"size": len(data)}

    class MultipartStringController(Controller):
        path = "/multipart-string"

        @post("/", media_type="multipart/form-data")
        async def upload(self, data: Annotated[bytes, Body(media_type="multipart/form-data")]) -> dict:
            return {"size": len(data)}

    class UploadFileController(Controller):
        path = "/uploadfile"

        @post("/")
        async def upload(self, file: UploadFile) -> dict:
            return {"filename": file.filename}

    # 2. Dynamic Dependency (simulating create_filter_dependencies)
    class DynamicDepController(Controller):
        path = "/dynamic"

        @get("/")
        async def dynamic(self, dep: Annotated[str, Dependency()]) -> str:
            return dep

    # 3. File Response
    class FileController(Controller):
        path = "/file"

        @get("/")
        async def download(self) -> File:
            return File(path="test.txt")

    app = Litestar(
        route_handlers=[
            MultipartController,
            MultipartStringController,
            UploadFileController,
            DynamicDepController,
            FileController,
            create_static_files_router(path="/static", directories=["/tmp"]),
        ],
        dependencies={"dep": Provide(lambda: "dep")},
    )

    routes = reflect_routes(app)

    # Verify classifications
    # We must check the POST route, not OPTIONS
    multipart_route = next(r for r in routes if "/multipart" in r.path and "POST" in r.methods)
    assert classify_route(multipart_route) == RouteCapability.ASGI_FALLBACK, "Multipart route should be FALLBACK"

    multipart_string_route = next(r for r in routes if "/multipart-string" in r.path and "POST" in r.methods)
    assert classify_route(multipart_string_route) == RouteCapability.ASGI_FALLBACK

    upload_file_route = next(r for r in routes if "/uploadfile" in r.path and "POST" in r.methods)
    assert classify_route(upload_file_route) == RouteCapability.ASGI_FALLBACK

    # The static route is usually an ASGIRoute
    static_route = next(r for r in routes if "/static" in r.path)
    assert classify_route(static_route) == RouteCapability.ASGI_FALLBACK, "Static route should be FALLBACK"


@pytest.mark.integration
async def test_union_return_does_not_crash() -> None:
    class UnionController(Controller):
        path = "/union"

        @get("/")
        async def handler(self) -> File | None:
            return None

    app = Litestar(route_handlers=[UnionController])
    routes = reflect_routes(app)
    route = next(r for r in routes if "/union" in r.path)

    assert classify_route(route) == RouteCapability.ASGI_FALLBACK


@pytest.mark.integration
async def test_websockets_fallback() -> None:
    from litestar import websocket
    from litestar.connection import WebSocket

    class WSController(Controller):
        path = "/ws"

        @websocket("/")
        async def handler(self, socket: WebSocket) -> None:
            await socket.accept()

    app = Litestar(route_handlers=[WSController])
    routes = reflect_routes(app)
    ws_route = next(r for r in routes if "/ws" in r.path)

    assert classify_route(ws_route) == RouteCapability.ASGI_FALLBACK
