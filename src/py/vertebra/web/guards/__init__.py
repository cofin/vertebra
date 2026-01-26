"""Guard primitives for native app routing."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Sequence
from typing import Annotated, Any, Protocol, TypeVar, get_args, get_origin, get_type_hints

from vertebra.di import Inject, ProviderCompiler

GuardResult = bool
GuardCallable = Callable[..., GuardResult | Awaitable[GuardResult]]


class Guard(Protocol):
    """Protocol for guard callables."""

    def __call__(self, *args: Any, **kwargs: Any) -> GuardResult | Awaitable[GuardResult]: ...


class GuardDeniedError(RuntimeError):
    """Raised when a guard denies access."""


_CONTEXT_PARAM_NAMES = {"ctx", "context", "request", "scope", "connection"}
_CACHE_KEY = "_vertebra_guard_cache"
_REQUEST_CACHE_KEY = "_vertebra_request_cache"


def requires(*guards: GuardCallable) -> Callable[[GuardCallable], GuardCallable]:
    """Attach guards to a handler callable."""

    def decorator(handler: GuardCallable) -> GuardCallable:
        existing = list(getattr(handler, "__vertebra_guards__", ()))
        existing.extend(guards)
        setattr(handler, "__vertebra_guards__", existing)
        return handler

    return decorator


def compile_guards(
    guards: Sequence[GuardCallable], *, compiler: ProviderCompiler | None = None
) -> Callable[[Any], Awaitable[None]]:
    """Compile guards into a single async runner with per-request caching."""
    specs = [_build_guard_spec(guard, compiler) for guard in guards]

    async def runner(ctx: Any) -> None:
        cache = _get_guard_cache(ctx)
        request_cache = _get_request_cache(ctx)
        for spec in specs:
            if spec.guard in cache:
                if not cache[spec.guard]:
                    msg = "guard denied (cached)"
                    raise GuardDeniedError(msg)
                continue
            args = []
            for kind, payload in spec.args:
                if kind == "context":
                    args.append(ctx)
                else:
                    dep_runner = payload
                    args.append(await dep_runner(request_cache))
            result = spec.guard(*args)
            if _is_awaitable(result):
                result = await result
            allowed = bool(result)
            cache[spec.guard] = allowed
            if not allowed:
                msg = "guard denied"
                raise GuardDeniedError(msg)

    return runner


class _GuardSpec:
    __slots__ = ("args", "guard")

    def __init__(self, guard: GuardCallable, args: list[tuple[str, Any]]) -> None:
        self.guard = guard
        self.args = args


def _build_guard_spec(guard: GuardCallable, compiler: ProviderCompiler | None) -> _GuardSpec:
    signature = _signature_for_guard(guard)
    hints = _safe_type_hints(guard)
    args: list[tuple[str, Any]] = []

    for name, param in signature.parameters.items():
        if name in ("self", "cls"):
            continue
        annotation = hints.get(name, param.annotation)
        dependency = _unwrap_guard_inject(annotation)
        if dependency is not None:
            if compiler is None:
                msg = "guard requires DI but no ProviderCompiler was supplied"
                raise ValueError(msg)
            args.append(("dependency", compiler.compile(dependency)))
            continue
        if name in _CONTEXT_PARAM_NAMES:
            args.append(("context", None))

    return _GuardSpec(guard=guard, args=args)


def _signature_for_guard(guard: GuardCallable) -> inspect.Signature:
    if inspect.isfunction(guard):
        return inspect.signature(guard)
    # Get __call__ method for signature inspection (not testing callability)
    call = getattr(guard, "__call__", guard)  # noqa: B004
    return inspect.signature(call)


def _unwrap_guard_inject(annotation: Any) -> type[Any] | None:
    if annotation is inspect.Signature.empty:
        return None
    origin = get_origin(annotation)
    if origin is Annotated:
        args = get_args(annotation)
        annotation = args[0] if args else annotation
        origin = get_origin(annotation)
    if origin is Inject:
        args = get_args(annotation)
        dep = args[0] if args else None
        return dep if isinstance(dep, type) else None
    return None


def _safe_type_hints(callable_obj: Callable[..., Any]) -> dict[str, Any]:
    try:
        return get_type_hints(callable_obj, globalns=getattr(callable_obj, "__globals__", None), include_extras=True)
    except (TypeError, NameError, AttributeError):
        return {}


def _get_guard_cache(ctx: Any) -> dict[GuardCallable, bool]:
    state = getattr(ctx, "state", None)
    if isinstance(state, dict):
        return state.setdefault(_CACHE_KEY, {})
    cache = getattr(ctx, _CACHE_KEY, None)
    if cache is None:
        cache = {}
        setattr(ctx, _CACHE_KEY, cache)
    return cache


def _get_request_cache(ctx: Any) -> dict[type[Any], Any]:
    state = getattr(ctx, "state", None)
    if isinstance(state, dict):
        return state.setdefault(_REQUEST_CACHE_KEY, {})
    cache = getattr(ctx, _REQUEST_CACHE_KEY, None)
    if cache is None:
        cache = {}
        setattr(ctx, _REQUEST_CACHE_KEY, cache)
    return cache


def _is_awaitable(value: Any) -> bool:
    return inspect.isawaitable(value)


__all__ = ("Guard", "GuardDeniedError", "GuardResult", "compile_guards", "requires")

# Backwards compatibility alias
GuardDenied = GuardDeniedError
