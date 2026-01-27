import pytest
from litestar import Litestar, get
from litestar.di import Provide
from vertebra.extensions.litestar.compiler import PipelineCompiler
from vertebra.extensions.litestar.native import VertebraRequest
from vertebra.security.auth.dependencies import provide_user
from vertebra.security.auth.models import VertebraUserMixin

@pytest.mark.asyncio
async def test_native_mode_protected_route() -> None:
    # 1. Define handler with dependency
    # Note: Type hint ensures DI injection type check pass if any
    @get("/", dependencies={"current_user": Provide(provide_user)})
    async def handler(current_user: VertebraUserMixin) -> dict:
        return {"user_id": current_user.id}

    # 2. Compile route
    app = Litestar(route_handlers=[handler])
    # Extract the route handler
    # Litestar route structure: app.routes -> [HTTPRoute] -> route_handler_map
    route = app.routes[0]
    # route is HTTPRoute. route_handler_map is dict[str, tuple[BaseRouteHandler, ...]]
    # We know it's a GET route
    http_handler = route.route_handler_map["GET"][0]
    
    compiler = PipelineCompiler()
    # PipelineCompiler.compile_route takes the handler object
    runner = compiler.compile_route(http_handler)

    # 3. Create Request with claims
    msg = {
        "auth_claims": {"id": "native-user", "username": "fast"},
        "method": "GET",
        "path": "/",
        "headers": []
    }
    req = VertebraRequest(msg)

    # 4. Run
    result = await runner(req)
    
    # 5. Assert
    assert result == {"user_id": "native-user"}
