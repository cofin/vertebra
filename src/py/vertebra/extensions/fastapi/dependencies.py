"""Startup dependency DAG flattening and zero-AsyncExitStack execution for FastAPI."""

from __future__ import annotations

import contextlib
import inspect
from collections.abc import Callable
from typing import Any

import anyio.to_thread
import msgspec

__all__ = (
    "CompiledDependantNode",
    "CompiledDependantPlan",
    "compile_dependant_plan",
    "execute_dependant_plan",
)


class CompiledDependantNode(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Single flattened node in a topological FastAPI dependency execution plan."""

    name: str | None
    call: Callable[..., Any]
    cache_key: tuple[Any, tuple[str, ...]]
    use_cache: bool
    is_async: bool
    is_generator: bool
    is_async_generator: bool
    sub_param_names: tuple[tuple[str, tuple[Any, tuple[str, ...]]], ...]
    request_param_name: str | None
    path_param_names: tuple[str, ...]
    query_param_names: tuple[tuple[str, Any, Any], ...]


class CompiledDependantPlan(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Topological execution plan for a FastAPI route's Depends() graph."""

    nodes: tuple[CompiledDependantNode, ...]
    root_dep_params: tuple[tuple[str, tuple[Any, tuple[str, ...]]], ...]
    has_generator_dependencies: bool
    has_security_requirements: bool


def compile_dependant_plan(dependant: Any, *, path_param_names: set[str] | None = None) -> CompiledDependantPlan | None:
    """Flatten a FastAPI Dependant tree into a topological CompiledDependantPlan."""
    if dependant is None:
        return None

    known_path_params = path_param_names or set()
    ordered_nodes: list[CompiledDependantNode] = []
    visited_keys: set[tuple[Any, tuple[str, ...]]] = set()
    has_generators = False
    has_security = bool(getattr(dependant, "security_requirements", None))

    def _visit(dep: Any) -> tuple[Any, tuple[str, ...]] | None:
        nonlocal has_generators, has_security
        call = getattr(dep, "call", None)
        if not callable(call):
            return None
        if getattr(dep, "security_requirements", None):
            has_security = True

        raw_key = getattr(dep, "cache_key", None)
        cache_key: tuple[Any, tuple[str, ...]] = (
            raw_key if (isinstance(raw_key, tuple) and len(raw_key) == 2) else (call, ())
        )
        use_cache = bool(getattr(dep, "use_cache", True))

        sub_params: list[tuple[str, tuple[Any, tuple[str, ...]]]] = []
        for sub_dep in getattr(dep, "dependencies", None) or ():
            sub_key = _visit(sub_dep)
            sub_name = getattr(sub_dep, "name", None)
            if sub_key is not None and isinstance(sub_name, str):
                sub_params.append((sub_name, sub_key))

        if use_cache and cache_key in visited_keys:
            return cache_key

        is_async_gen = inspect.isasyncgenfunction(call)
        is_sync_gen = inspect.isgeneratorfunction(call)
        if is_async_gen or is_sync_gen:
            has_generators = True

        is_async = (
            is_async_gen
            or inspect.iscoroutinefunction(call)
            or (callable(call) and inspect.iscoroutinefunction(type(call).__call__))
        )

        req_param = getattr(dep, "request_param_name", None)
        dep_path_params: list[str] = []
        dep_query_params: list[tuple[str, Any, Any]] = []
        sub_name_set = {name for name, _ in sub_params}

        try:
            sig = inspect.signature(call)
            for p_name, param in sig.parameters.items():
                if p_name in sub_name_set or p_name == req_param:
                    continue
                ann = param.annotation
                if p_name == "request" or getattr(ann, "__name__", "") == "Request":
                    req_param = p_name
                elif p_name in known_path_params:
                    dep_path_params.append(p_name)
                elif param.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY):
                    default_val = None if param.default is inspect.Parameter.empty else param.default
                    converter = _scalar_converter(ann)
                    dep_query_params.append((p_name, converter, default_val))
        except (TypeError, ValueError):
            pass

        node = CompiledDependantNode(
            name=getattr(dep, "name", None),
            call=call,
            cache_key=cache_key,
            use_cache=use_cache,
            is_async=is_async,
            is_generator=is_sync_gen,
            is_async_generator=is_async_gen,
            sub_param_names=tuple(sub_params),
            request_param_name=req_param if isinstance(req_param, str) else None,
            path_param_names=tuple(dep_path_params),
            query_param_names=tuple(dep_query_params),
        )
        ordered_nodes.append(node)
        if use_cache:
            visited_keys.add(cache_key)
        return cache_key

    root_deps: list[tuple[str, tuple[Any, tuple[str, ...]]]] = []
    for child in getattr(dependant, "dependencies", None) or ():
        key = _visit(child)
        child_name = getattr(child, "name", None)
        if key is not None and isinstance(child_name, str):
            root_deps.append((child_name, key))

    return CompiledDependantPlan(
        nodes=tuple(ordered_nodes),
        root_dep_params=tuple(root_deps),
        has_generator_dependencies=has_generators,
        has_security_requirements=has_security,
    )


async def execute_dependant_plan(
    plan: CompiledDependantPlan,
    *,
    request: Any,
    path_params: dict[str, Any],
    query_params: dict[str, str],
    dependency_overrides: dict[Callable[..., Any], Callable[..., Any]] | None = None,
    exit_stack: contextlib.AsyncExitStack | None = None,
) -> dict[str, Any]:
    """Execute a CompiledDependantPlan, eliding AsyncExitStack when no generator dependencies exist."""
    if not plan.nodes:
        return {}

    cache: dict[tuple[Any, tuple[str, ...]], Any] = {}
    overrides = dependency_overrides or {}

    for node in plan.nodes:
        if node.use_cache and node.cache_key in cache:
            continue

        target_call = overrides.get(node.call, node.call)
        kwargs: dict[str, Any] = {}
        if node.request_param_name is not None:
            kwargs[node.request_param_name] = request
        for p_name in node.path_param_names:
            if p_name in path_params:
                kwargs[p_name] = path_params[p_name]
        for q_name, converter, default_val in node.query_param_names:
            if q_name in query_params:
                kwargs[q_name] = converter(query_params[q_name])
            else:
                kwargs[q_name] = default_val
        for sub_name, sub_key in node.sub_param_names:
            kwargs[sub_name] = cache[sub_key]

        if node.is_async_generator:
            if exit_stack is None:
                msg = "AsyncExitStack required for async generator dependency"
                raise RuntimeError(msg)
            cm = contextlib.asynccontextmanager(target_call)(**kwargs)
            val = await exit_stack.enter_async_context(cm)
        elif node.is_generator:
            if exit_stack is None:
                msg = "AsyncExitStack required for generator dependency"
                raise RuntimeError(msg)
            sync_cm = contextlib.contextmanager(target_call)(**kwargs)
            val = exit_stack.enter_context(sync_cm)
        elif node.is_async or inspect.iscoroutinefunction(target_call):
            val = await target_call(**kwargs)
        else:

            def _invoke_sync(fn: Callable[..., Any] = target_call, kw: dict[str, Any] = kwargs) -> Any:
                return fn(**kw)

            val = await anyio.to_thread.run_sync(_invoke_sync)

        cache[node.cache_key] = val

    return {param_name: cache[cache_key] for param_name, cache_key in plan.root_dep_params}


def _scalar_converter(annotation: Any) -> Callable[[str], Any]:
    if annotation is int:
        return int
    if annotation is float:
        return float
    if annotation is bool:
        return lambda s: s.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
    return str
