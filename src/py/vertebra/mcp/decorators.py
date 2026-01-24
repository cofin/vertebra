import asyncio
from collections.abc import Callable, Coroutine
from functools import wraps
from typing import Any, ParamSpec, Protocol, TypeVar, overload

from vertebra.mcp.registry import ProcedureRegistry

P = ParamSpec("P")
R = TypeVar("R")


class _McpToolDecorator(Protocol):
    """Protocol for decorator returned by mcp_tool()."""

    @overload
    def __call__(self, func: Callable[P, Coroutine[Any, Any, R]]) -> Callable[P, Coroutine[Any, Any, R]]: ...

    @overload
    def __call__(self, func: Callable[P, R]) -> Callable[P, R]: ...

    def __call__(self, func: Callable[P, R]) -> Callable[P, R]: ...


def mcp_tool(name: str | None = None, description: str | None = None) -> _McpToolDecorator:
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        tool_name = name or func.__name__
        tool_description = description or (func.__doc__.strip() if func.__doc__ else None)

        if asyncio.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                return await func(*args, **kwargs)  # type: ignore[no-any-return]

            wrapper: Callable[P, R] = async_wrapper  # type: ignore[assignment]
        else:

            @wraps(func)
            def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                return func(*args, **kwargs)

            wrapper = sync_wrapper

        registry = ProcedureRegistry.get_instance()
        registry.register_tool(tool_name, wrapper, description=tool_description)
        return wrapper

    return decorator
