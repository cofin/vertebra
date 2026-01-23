import asyncio
from collections.abc import Callable
from functools import wraps
from typing import Any

from vertebra.mcp.registry import ProcedureRegistry


def mcp_tool(
    name: str | None = None, description: str | None = None
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        tool_name = name or func.__name__
        tool_description = description or (func.__doc__.strip() if func.__doc__ else None)

        if asyncio.iscoroutinefunction(func):

            @wraps(func)
            async def wrapper(*args: Any, **kwargs: Any) -> Any:
                return await func(*args, **kwargs)

        else:

            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                return func(*args, **kwargs)

        registry = ProcedureRegistry.get_instance()
        registry.register_tool(tool_name, wrapper, description=tool_description)

        return wrapper

    return decorator
