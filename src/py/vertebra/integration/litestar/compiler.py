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

def compile_di(dependencies: dict[str, Any], handler: Callable) -> Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]:
    """Compile a DI graph into a single, optimized async function.
    
    Returns a function that takes a context (like scope/connection) and returns 
    a dictionary of resolved dependency values.
    """
    if not dependencies:
        async def noop_di(_: dict[str, Any]) -> dict[str, Any]:
            return {}
        return noop_di

    # Basic topological sort of dependencies
    # Litestar's Provide objects wrap the callables
    resolved_order = []
    seen = set()
    
    def resolve(name: str, provider: Any):
        if name in seen:
            return
        # Inspect the provider's callable to find its dependencies
        # This is a simplification; Litestar does this more robustly
        sig = inspect.signature(provider.dependency)
        for param_name in sig.parameters:
            if param_name in dependencies:
                resolve(param_name, dependencies[param_name])
        
        resolved_order.append((name, provider))
        seen.add(name)

    # We only care about dependencies that are actually used by the handler
    handler_sig = inspect.signature(handler)
    for param_name in handler_sig.parameters:
        if param_name in dependencies:
            resolve(param_name, dependencies[param_name])

    if not resolved_order:
        async def noop_di_unused(_: dict[str, Any]) -> dict[str, Any]:
            return {}
        return noop_di_unused

    func_name = f"compiled_di_{id(dependencies)}"
    body = []
    local_ns = {}
    
    body.append("    results = {}")
    for i, (name, provider) in enumerate(resolved_order):
        dep_key = f"dep_{i}"
        local_ns[dep_key] = provider.dependency
        
        # Build arguments for this dependency call
        sig = inspect.signature(provider.dependency)
        args = []
        for param_name in sig.parameters:
            if param_name in dependencies:
                args.append(f"results['{param_name}']")
            # TODO: handle special types like 'connection', 'scope', etc.
            elif param_name in ("connection", "scope", "request"):
                args.append("ctx")
        
        args_str = ", ".join(args)
        if inspect.iscoroutinefunction(provider.dependency):
            body.append(f"    results['{name}'] = await {dep_key}({args_str})")
        else:
            body.append(f"    results['{name}'] = {dep_key}({args_str})")
            
    body.append("    return results")
    
    source = f"async def {func_name}(ctx):\n" + "\n".join(body)
    exec(source, local_ns)
    return local_ns[func_name]
