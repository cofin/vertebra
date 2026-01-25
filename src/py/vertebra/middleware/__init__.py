"""Middleware primitives for native app routing."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
import inspect
from typing import Annotated, Any, Protocol, get_args, get_origin, get_type_hints

from vertebra.di import Inject, ProviderCompiler

MiddlewareHook = Callable[..., Any | Awaitable[Any]]


class Middleware(Protocol):
    priority: int
    before: MiddlewareHook | None
    after: MiddlewareHook | None


@dataclass(frozen=True)
class MiddlewareSpec:
    before: MiddlewareHook | None = None
    after: MiddlewareHook | None = None
    priority: int = 0


class MiddlewareError(RuntimeError):
    """Raised when middleware configuration is invalid."""


_CONTEXT_PARAM_NAMES = {"ctx", "context", "request", "scope", "connection"}
_RESULT_PARAM_NAMES = {"result", "response", "output"}
_REQUEST_CACHE_KEY = "_vertebra_request_cache"


def middleware(
    *,
    before: MiddlewareHook | None = None,
    after: MiddlewareHook | None = None,
    priority: int = 0,
) -> MiddlewareSpec:
    if before is None and after is None:
        raise MiddlewareError("middleware must define at least one hook")
    return MiddlewareSpec(before=before, after=after, priority=priority)


def compile_middleware(
    middleware_chain: Sequence[Any],
    *,
    compiler: ProviderCompiler | None = None,
) -> tuple[Callable[[Any], Awaitable[None]], Callable[[Any, Any], Awaitable[None]]]:
    specs = [_coerce_middleware(item) for item in middleware_chain]
    ordered = [spec for _, spec in sorted(enumerate(specs), key=lambda item: (item[1].priority, item[0]))]

    before_specs = [_build_hook_spec(spec.before, compiler, include_result=False) for spec in ordered if spec.before]
    after_specs = [_build_hook_spec(spec.after, compiler, include_result=True) for spec in ordered if spec.after]

    async def run_before(ctx: Any) -> None:
        request_cache = _get_request_cache(ctx)
        for spec in before_specs:
            await _invoke_hook(spec, ctx, None, request_cache)

    async def run_after(ctx: Any, result: Any) -> None:
        request_cache = _get_request_cache(ctx)
        for spec in reversed(after_specs):
            await _invoke_hook(spec, ctx, result, request_cache)

    return run_before, run_after


class _HookSpec:
    __slots__ = ("hook", "args")

    def __init__(self, hook: MiddlewareHook, args: list[tuple[str, Any]]) -> None:
        self.hook = hook
        self.args = args


def _coerce_middleware(item: Any) -> MiddlewareSpec:
    if isinstance(item, MiddlewareSpec):
        return item

    before = getattr(item, "before", None)
    after = getattr(item, "after", None)
    priority = getattr(item, "priority", 0)

    if before is None and after is None and callable(item):
        before = item

    if before is None and after is None:
        raise MiddlewareError("middleware entries must be callable or expose before/after hooks")

    return MiddlewareSpec(before=before, after=after, priority=int(priority))


def _build_hook_spec(
    hook: MiddlewareHook,
    compiler: ProviderCompiler | None,
    *,
    include_result: bool,
) -> _HookSpec:
    signature = _signature_for_hook(hook)
    hints = _safe_type_hints(hook)
    args: list[tuple[str, Any]] = []

    for name, param in signature.parameters.items():
        if name in ("self", "cls"):
            continue
        annotation = hints.get(name, param.annotation)
        dependency = _unwrap_inject(annotation)
        if dependency is not None:
            if compiler is None:
                msg = "middleware hook requires DI but no ProviderCompiler was supplied"
                raise MiddlewareError(msg)
            args.append(("dependency", compiler.compile(dependency)))
            continue
        if name in _CONTEXT_PARAM_NAMES:
            args.append(("context", None))
            continue
        if include_result and name in _RESULT_PARAM_NAMES:
            args.append(("result", None))
            continue

    return _HookSpec(hook=hook, args=args)


def _signature_for_hook(hook: MiddlewareHook) -> inspect.Signature:
    if inspect.isfunction(hook):
        return inspect.signature(hook)
    call = getattr(hook, "__call__", hook)
    return inspect.signature(call)


def _unwrap_inject(annotation: Any) -> type[Any] | None:
    if annotation is inspect.Signature.empty:
        return None
    origin = get_origin(annotation)
    if origin is Annotated:
        args = get_args(annotation)
        annotation = args[0] if args else annotation
        origin = get_origin(annotation)
    if origin is Inject:
        args = get_args(annotation)
        dependency = args[0] if args else None
        return dependency if isinstance(dependency, type) else None
    return None


def _safe_type_hints(callable_obj: Callable[..., Any]) -> dict[str, Any]:
    try:
        return get_type_hints(
            callable_obj,
            globalns=getattr(callable_obj, "__globals__", None),
            include_extras=True,
        )
    except (TypeError, NameError, AttributeError):
        return {}


def _get_request_cache(ctx: Any) -> dict[type[Any], Any]:
    state = getattr(ctx, "state", None)
    if isinstance(state, dict):
        cache = state.setdefault(_REQUEST_CACHE_KEY, {})
        return cache
    cache = getattr(ctx, _REQUEST_CACHE_KEY, None)
    if cache is None:
        cache = {}
        setattr(ctx, _REQUEST_CACHE_KEY, cache)
    return cache


async def _invoke_hook(
    spec: _HookSpec,
    ctx: Any,
    result: Any,
    request_cache: dict[type[Any], Any],
) -> None:
    args: list[Any] = []
    for kind, payload in spec.args:
        if kind == "context":
            args.append(ctx)
        elif kind == "result":
            args.append(result)
        else:
            args.append(await payload(request_cache))

    outcome = spec.hook(*args)
    if inspect.isawaitable(outcome):
        await outcome


__all__ = ("Middleware", "MiddlewareError", "MiddlewareSpec", "compile_middleware", "middleware")
