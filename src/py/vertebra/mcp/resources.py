from collections.abc import Callable
from functools import wraps
from typing import Any

from vertebra.mcp.registry import ProcedureRegistry


def mcp_resource(
    uri_template: str, name: str | None = None, description: str | None = None, mime_type: str | None = None
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        res_name = name or func.__name__
        res_description = description or (func.__doc__.strip() if func.__doc__ else None)

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        registry = ProcedureRegistry.get_instance()
        registry.register_resource(
            uri_template, wrapper, name=res_name, description=res_description, mime_type=mime_type
        )

        return wrapper

    return decorator
