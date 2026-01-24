from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


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

        if inspect.iscoroutinefunction(guard) or (callable(guard) and inspect.iscoroutinefunction(guard.__call__)):
            body.append(f"    await {guard_key}(connection, handler)")
        else:
            body.append(f"    {guard_key}(connection, handler)")

    source = f"async def {func_name}(connection, handler):\n" + "\n".join(body)

    # Execute the source to define the function
    exec(source, local_ns)  # noqa: S102
    return local_ns[func_name]


def compile_di(
    dependencies: dict[str, Any], handler: Callable
) -> Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]:
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

    def resolve(name: str, provider: Any) -> None:
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
    exec(source, local_ns)  # noqa: S102
    return local_ns[func_name]


class PipelineCompiler:
    """Compiles Litestar route handlers into optimized runner functions."""

    def compile_route(self, handler: Any) -> Callable[[Any], Awaitable[Any]]:
        """Compile a single Litestar route handler into an optimized runner."""
        # Extract guards and dependencies from the handler
        # Litestar handlers have resolve_guards and resolve_dependencies methods
        guards = handler.resolve_guards()
        dependencies = handler.resolve_dependencies()

        compiled_guards = compile_guards(guards)
        compiled_di = compile_di(dependencies, handler.fn)

        # We build the final runner function
        func_name = f"runner_{id(handler)}"
        from uuid import UUID

        local_ns = {
            "compiled_guards": compiled_guards,
            "compiled_di": compiled_di,
            "handler_fn": handler.fn,
            "UUID": UUID,
        }

        body = []
        # 1. Run guards
        body.append("    await compiled_guards(ctx, handler_fn)")

        # 2. Resolve dependencies
        body.append("    deps = await compiled_di(ctx)")

        # 3. Call the handler with dependencies
        # Filter deps to only those needed by handler
        sig = inspect.signature(handler.fn)
        args = []
        for param_name in sig.parameters:
            if param_name in dependencies:
                args.append(f"{param_name}=deps['{param_name}']")
            elif param_name == "data":
                # Inject parsed data from request
                body.append("    data = ctx.json()")
                args.append("data=data")
            elif param_name in ("connection", "scope", "request"):
                args.append(f"{param_name}=ctx")
            else:
                # Try path params
                body.append(f"    if '{param_name}' in ctx.path_params:")

                param_type = sig.parameters[param_name].annotation
                if param_type is int:
                    body.append(f"        {param_name} = int(ctx.path_params['{param_name}'])")
                elif param_type is float:
                    body.append(f"        {param_name} = float(ctx.path_params['{param_name}'])")
                elif param_type is bool:
                    # Simple bool conversion matching common API expectations
                    body.append(f"        val = ctx.path_params['{param_name}'].lower()")
                    body.append(f"        {param_name} = val in ('true', '1', 'yes', 'on')")
                elif param_type is UUID:
                    body.append(f"        {param_name} = UUID(ctx.path_params['{param_name}'])")
                else:
                    body.append(f"        {param_name} = ctx.path_params['{param_name}']")
                args.append(f"{param_name}={param_name}")

        args_str = ", ".join(args)
        if inspect.iscoroutinefunction(handler.fn):
            body.append(f"    return await handler_fn({args_str})")
        else:
            body.append(f"    return handler_fn({args_str})")

        source = f"async def {func_name}(ctx):\n" + "\n".join(body)
        exec(source, local_ns)  # noqa: S102
        return local_ns[func_name]
