import pytest
from typing import Any, Callable
from litestar import Litestar, get
from litestar.exceptions import NotAuthorizedException

try:
    from vertebra.integration.litestar.compiler import compile_guards
    HAS_COMPILER = True
except ImportError:
    HAS_COMPILER = False

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
