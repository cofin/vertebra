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
        from typing import Annotated, Union, get_args, get_origin
        import types

        def unwrap_annotation(annotation: Any) -> Any:
            if get_origin(annotation) is Annotated:
                args = get_args(annotation)
                if args:
                    return args[0]
            return annotation

        def resolve_union(annotation: Any) -> Any:
            origin = get_origin(annotation)
            if origin in (Union, types.UnionType):
                for arg in get_args(annotation):
                    if arg is type(None):
                        continue
                    return arg
            return annotation

        def make_converter(annotation: Any) -> tuple[Callable[[Any], Any] | None, bool]:
            annotation = resolve_union(unwrap_annotation(annotation))
            if annotation is inspect.Signature.empty:
                return None, False
            origin = get_origin(annotation)
            if origin in (list, set, tuple):
                args = get_args(annotation)
                item_type = args[0] if args else None
                converter, _ = make_converter(item_type)
                return converter, True
            if annotation is int:
                return int, False
            if annotation is float:
                return float, False
            if annotation is bool:
                def _bool(value: Any) -> bool:
                    if isinstance(value, bool):
                        return value
                    return str(value).lower() in ("true", "1", "yes", "on")
                return _bool, False
            if annotation is UUID:
                def _uuid(value: Any) -> UUID:
                    if isinstance(value, UUID):
                        return value
                    return UUID(str(value))
                return _uuid, False
            return None, False

        def coerce_value(value: Any, converter: Callable[[Any], Any] | None, is_list: bool) -> Any:
            if is_list:
                values = value if isinstance(value, list) else [value]
                if converter:
                    return [converter(item) for item in values]
                return values
            scalar = value[0] if isinstance(value, list) else value
            if converter:
                return converter(scalar)
            return scalar

        local_ns = {
            "compiled_guards": compiled_guards,
            "compiled_di": compiled_di,
            "handler_fn": handler.fn,
            "coerce_value": coerce_value,
        }

        body = []
        # 1. Run guards
        body.append("    await compiled_guards(ctx, handler_fn)")

        # 2. Resolve dependencies
        body.append("    deps = await compiled_di(ctx)")

        # 3. Call the handler with dependencies + request params
        sig = inspect.signature(handler.fn)
        if any(param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD, inspect.Parameter.POSITIONAL_ONLY)
               for param in sig.parameters.values()):
            raise ValueError("Native runner does not support positional-only or varargs parameters.")

        body.append("    params = {}")
        for param_name, param in sig.parameters.items():
            if param_name in dependencies:
                body.append(f"    params['{param_name}'] = deps['{param_name}']")
                continue
            if param_name == "data":
                body.append("    params['data'] = ctx.json()")
                continue
            if param_name in ("connection", "scope", "request"):
                body.append(f"    params['{param_name}'] = ctx")
                continue

            converter, is_list = make_converter(param.annotation)
            converter_key = f"converter_{param_name}"
            list_key = f"is_list_{param_name}"
            local_ns[converter_key] = converter
            local_ns[list_key] = is_list

            body.append(f"    if '{param_name}' in ctx.path_params:")
            body.append(f"        raw = ctx.path_params['{param_name}']")
            body.append(
                f"        params['{param_name}'] = coerce_value(raw, {converter_key}, {list_key})"
            )
            body.append(f"    elif '{param_name}' in ctx.query_params:")
            body.append(f"        raw = ctx.query_params['{param_name}']")
            body.append(
                f"        params['{param_name}'] = coerce_value(raw, {converter_key}, {list_key})"
            )

        if inspect.iscoroutinefunction(handler.fn):
            body.append("    return await handler_fn(**params)")
        else:
            body.append("    return handler_fn(**params)")

        source = f"async def {func_name}(ctx):\n" + "\n".join(body)
        exec(source, local_ns)  # noqa: S102
        return local_ns[func_name]
