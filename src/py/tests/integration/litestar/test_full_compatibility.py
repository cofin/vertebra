import json
from unittest.mock import MagicMock

import pytest
from litestar import Litestar, Router, get
from litestar.di import Provide
from litestar.exceptions import HTTPException
from litestar.status_codes import HTTP_400_BAD_REQUEST

from vertebra.extensions.litestar.compiler import PipelineCompiler
from vertebra.extensions.litestar.native import reflect_routes


@pytest.mark.integration
@pytest.mark.anyio
async def test_complex_routing_and_di() -> None:
    # 1. Setup complex app
    def get_dep_a() -> str:
        return "A"

    async def get_dep_b(dep_a: str) -> str:
        return f"{dep_a}B"

    @get("/deep/nested/{param:int}", sync_to_thread=False)
    def handler(param: int, dep_b: str) -> dict:
        if param < 0:
            raise HTTPException(detail="Negative", status_code=HTTP_400_BAD_REQUEST)
        return {"param": param, "dep": dep_b}

    router = Router(path="/deep", route_handlers=[handler])
    app = Litestar(
        route_handlers=[router],
        dependencies={"dep_a": Provide(get_dep_a, sync_to_thread=False), "dep_b": Provide(get_dep_b)},
        openapi_config=None,
    )

    # 2. Reflect and Compile
    routes = reflect_routes(app)
    # Ensure reflection found the route
    target_route = next(r for r in routes if "/deep/nested/" in r.path)

    compiler = PipelineCompiler()

    # We can create a mock object that mimics the handler wrapper
    # since PipelineCompiler expects something with resolve_guards/resolve_dependencies
    handler_wrapper = MagicMock()
    handler_wrapper.fn = target_route.handler_fn or target_route.handler.fn
    handler_wrapper.resolve_guards.return_value = target_route.guards
    handler_wrapper.resolve_dependencies.return_value = target_route.dependencies

    runner = compiler.compile_route(handler_wrapper)

    # 3. Execution
    # Success case
    msg_success = {
        "id": "req-1",
        "path": "/deep/nested/42",
        "method": "GET",
        "headers": [],
        "body": b"",
        "path_params": {"param": "42"},
    }

    # Mock VertebraRequest context (since runner expects it)
    from vertebra.extensions.litestar.native import VertebraRequest

    ctx = VertebraRequest(msg_success)

    result = await runner(ctx)
    if hasattr(result, "body"):
        body = result.body
        payload = json.loads(body) if isinstance(body, (bytes, bytearray, memoryview)) else json.loads(bytes(body))
        assert payload["param"] == 42
        assert payload["dep"] == "AB"
    else:
        assert result["param"] == 42
        assert result["dep"] == "AB"
