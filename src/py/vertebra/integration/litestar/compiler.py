from __future__ import annotations

import inspect
from typing import Any, Callable, Awaitable

def compile_guards(guards: list[Callable]) -> Callable[[Any, Any], Awaitable[None]]:
    """Compile a list of guards into a single, optimized async function.
    
    The generated function will execute guards in order. 
    Sync guards are called directly, async guards are awaited.
    """
    if not guards:
        async def noop_guard(_: Any, __: Any) -> None:
            return
        return noop_guard

    # Generate a unique name for the compiled function
    func_name = f"compiled_guards_{id(guards)}"
    
    # We build the function body dynamically
    body = []
    local_ns = {}
    
    for i, guard in enumerate(guards):
        guard_key = f"guard_{i}"
        local_ns[guard_key] = guard
        
        if inspect.iscoroutinefunction(guard) or (
            hasattr(guard, "__call__") and inspect.iscoroutinefunction(guard.__call__)
        ):
            body.append(f"    await {guard_key}(connection, handler)")
        else:
            body.append(f"    {guard_key}(connection, handler)")
            
    source = f"async def {func_name}(connection, handler):\n" + "\n".join(body)
    
    # Execute the source to define the function
    exec(source, local_ns)
    return local_ns[func_name]
