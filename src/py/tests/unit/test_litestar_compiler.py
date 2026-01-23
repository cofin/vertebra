import pytest
from typing import Any, Callable
from litestar import Litestar, get
from litestar.exceptions import NotAuthorizedException

try:
    from vertebra.integration.litestar.compiler import compile_guards, compile_di, PipelineCompiler
    HAS_COMPILER = True
except ImportError:
    HAS_COMPILER = False

# ... existing tests ...

@pytest.mark.skipif(not HAS_COMPILER, reason="Compiler not implemented")
@pytest.mark.anyio
async def test_pipeline_compiler_end_to_end() -> None:
    from litestar.di import Provide
    from litestar.exceptions import NotAuthorizedException

    def guard(connection: Any, _: Any) -> None:
        if connection.headers.get("x-auth") != "secret":
            raise NotAuthorizedException()

    def get_user(connection: Any) -> str:
        return "cody"

    @get("/hello")
    def handler(user: str) -> dict[str, str]:
        return {"hello": user}

    # Setup dependencies as Litestar would
    handler.resolve_guards = lambda: [guard]
    handler.resolve_dependencies = lambda: {"user": Provide(get_user)}

    compiler = PipelineCompiler()
    runner = compiler.compile_route(handler)
    
    # Test successful run
    conn = MockConnection({"x-auth": "secret"})
    # The runner expects a connection/scope-like object
    result = await runner(conn)
    assert result == {"hello": "cody"}
    
    # Test guard failure
    conn = MockConnection({"x-auth": "wrong"})
    with pytest.raises(NotAuthorizedException):
        await runner(conn)

@pytest.mark.skipif(not HAS_COMPILER, reason="Compiler not implemented")
@pytest.mark.anyio
async def test_compile_di_basic() -> None:
    from litestar.di import Provide

    def dep_1() -> str:
        return "val1"

    async def dep_2(d1: str) -> str:
        return f"{d1}-val2"

    dependencies = {
        "d1": Provide(dep_1),
        "d2": Provide(dep_2),
    }
    
    # We need a dummy handler to know which dependencies it actually needs
    def handler(d2: str) -> None:
        pass

    compiled = compile_di(dependencies, handler)
    
    # Executing the compiled DI resolver
    ctx = {}
    resolved = await compiled(ctx)
    
    assert resolved["d2"] == "val1-val2"
    assert "d1" in resolved # even if not directly requested by handler, it's a sub-dep

def guard_1(connection: Any, _: Any) -> None:
    if connection.headers.get("x-auth") != "secret":
        raise NotAuthorizedException("Invalid auth")

def guard_2(connection: Any, _: Any) -> None:
    if connection.headers.get("x-tenant") != "litestar":
        raise NotAuthorizedException("Invalid tenant")

class MockConnection:
    def __init__(self, headers: dict[str, str]):
        self.headers = headers

@pytest.mark.skipif(not HAS_COMPILER, reason="Compiler not implemented")
@pytest.mark.anyio
async def test_compile_guards_basic() -> None:
    # Compile two guards into one runner
    compiled = compile_guards([guard_1, guard_2])
    
    # Test success
    conn = MockConnection({"x-auth": "secret", "x-tenant": "litestar"})
    await compiled(conn, None) # Should not raise
    
    # Test failure 1
    conn = MockConnection({"x-auth": "wrong", "x-tenant": "litestar"})
    with pytest.raises(NotAuthorizedException, match="Invalid auth"):
        await compiled(conn, None)
        
    # Test failure 2
    conn = MockConnection({"x-auth": "secret", "x-tenant": "wrong"})
    with pytest.raises(NotAuthorizedException, match="Invalid tenant"):
        await compiled(conn, None)

@pytest.mark.skipif(not HAS_COMPILER, reason="Compiler not implemented")
@pytest.mark.anyio
async def test_compile_guards_async() -> None:
    async def async_guard(connection: Any, _: Any) -> None:
        if connection.headers.get("x-auth") != "secret":
            raise NotAuthorizedException("Invalid auth")

    compiled = compile_guards([async_guard])
    
    conn = MockConnection({"x-auth": "secret"})
    await compiled(conn, None)
    
    conn = MockConnection({"x-auth": "wrong"})
    with pytest.raises(NotAuthorizedException):
        await compiled(conn, None)

@pytest.mark.skipif(not HAS_COMPILER, reason="Compiler not implemented")
@pytest.mark.anyio
async def test_compile_guards_empty() -> None:
    compiled = compile_guards([])
    conn = MockConnection({})
    await compiled(conn, None) # Should just pass
