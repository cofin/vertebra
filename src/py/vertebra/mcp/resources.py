from typing import Callable, Optional, Any
from functools import wraps
from vertebra.mcp.registry import ProcedureRegistry

def mcp_resource(
    uri_template: str, 
    name: Optional[str] = None, 
    description: Optional[str] = None,
    mime_type: Optional[str] = None
):
    def decorator(func: Callable[..., Any]):
        res_name = name or func.__name__
        res_description = description or (func.__doc__.strip() if func.__doc__ else None)
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
            
        registry = ProcedureRegistry.get_instance()
        registry.register_resource(
            uri_template, 
            wrapper, 
            name=res_name, 
            description=res_description,
            mime_type=mime_type
        )
        
        return wrapper
    return decorator
