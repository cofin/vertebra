import asyncio
from typing import Callable, Optional, Any
from functools import wraps
from vertebra.mcp.registry import ProcedureRegistry

def mcp_tool(name: Optional[str] = None, description: Optional[str] = None):
    def decorator(func: Callable[..., Any]):
        tool_name = name or func.__name__
        tool_description = description or (func.__doc__.strip() if func.__doc__ else None)
        
        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                return await func(*args, **kwargs)
        else:
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            
        registry = ProcedureRegistry.get_instance()
        registry.register_tool(tool_name, wrapper, description=tool_description)
        
        return wrapper
    return decorator
