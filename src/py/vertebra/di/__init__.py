"""Dependency injection primitives."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
import inspect
from typing import (
    Annotated,
    Any,
    Generic,
    Literal,
    Protocol,
    TypeVar,
    get_args,
    get_origin,
    get_type_hints,
    runtime_checkable,
)

import msgspec

T = TypeVar("T")

Scope = Literal["singleton", "request", "transient"]
SCOPES: tuple[Scope, ...] = ("singleton", "request", "transient")
ProviderFactory = Callable[..., T | Awaitable[T]]


class Inject(Generic[T]):
    """Marker type for injectable dependencies."""

    __slots__ = ()


@runtime_checkable
class Provider(Protocol[T]):
    """Protocol for dependency providers."""

    provides: type[T]
    scope: Scope

    def __call__(self, *args: Any, **kwargs: Any) -> T | Awaitable[T]:
        ...


class ProviderSpec(msgspec.Struct, frozen=True):
    """Lightweight provider definition with scope metadata."""

    provides: type[Any]
    factory: ProviderFactory[Any]
    scope: Scope = "singleton"
    dependencies: tuple["DependencySpec", ...] = ()

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.factory(*args, **kwargs)


class DependencySpec(msgspec.Struct, frozen=True):
    """Metadata about a single dependency for a provider factory."""

    name: str
    dependency: type[Any]


class ProviderRegistry:
    """Register provider factories and infer dependency metadata."""

    __slots__ = ("_providers",)

    def __init__(self) -> None:
        self._providers: dict[type[Any], ProviderSpec[Any]] = {}

    @property
    def providers(self) -> dict[type[Any], ProviderSpec[Any]]:
        return self._providers

    def provide(
        self,
        provider: type[T] | ProviderFactory[T] | None = None,
        *,
        scope: Scope = "singleton",
    ) -> ProviderSpec[T] | Callable[[ProviderFactory[T]], ProviderFactory[T]]:
        if provider is None:

            def decorator(factory: ProviderFactory[T]) -> ProviderFactory[T]:
                self._register_factory(factory, scope=scope)
                return factory

            return decorator

        if inspect.isclass(provider):
            return self._register_class(provider, scope=scope)

        if callable(provider):
            self._register_factory(provider, scope=scope)
            return provider

        raise TypeError("provider must be a class or callable")

    def _register_class(self, provider: type[T], *, scope: Scope) -> ProviderSpec[T]:
        dependencies = _collect_dependencies(provider.__init__, skip_first=True)
        spec = ProviderSpec(provides=provider, factory=provider, scope=scope, dependencies=dependencies)
        self._providers[provider] = spec
        return spec

    def _register_factory(self, factory: ProviderFactory[T], *, scope: Scope) -> ProviderSpec[T]:
        provides = _resolve_return_type(factory)
        dependencies = _collect_dependencies(factory)
        spec = ProviderSpec(provides=provides, factory=factory, scope=scope, dependencies=dependencies)
        self._providers[provides] = spec
        return spec


def _resolve_return_type(factory: ProviderFactory[Any]) -> type[Any]:
    hints = _safe_type_hints(factory)
    return_annotation = hints.get("return", inspect.Signature.empty)
    if return_annotation is inspect.Signature.empty:
        raise ValueError("provider factories must declare a return type")
    dependency = _unwrap_inject(return_annotation)
    if dependency is None:
        raise ValueError("provider factories must declare a concrete return type")
    return dependency


def _collect_dependencies(
    callable_obj: Callable[..., Any],
    *,
    skip_first: bool = False,
) -> tuple[DependencySpec, ...]:
    signature = inspect.signature(callable_obj)
    hints = _safe_type_hints(callable_obj)
    dependencies: list[DependencySpec] = []

    for name, param in signature.parameters.items():
        if skip_first and name in ("self", "cls"):
            continue
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue
        annotation = hints.get(name, param.annotation)
        dependency = _unwrap_inject(annotation)
        if dependency is None:
            continue
        dependencies.append(DependencySpec(name=name, dependency=dependency))

    return tuple(dependencies)


def _safe_type_hints(callable_obj: Callable[..., Any]) -> dict[str, Any]:
    try:
        return get_type_hints(
            callable_obj,
            globalns=getattr(callable_obj, "__globals__", None),
            include_extras=True,
        )
    except (TypeError, NameError, AttributeError):
        return {}


def _unwrap_inject(annotation: Any) -> type[Any] | None:
    if annotation is inspect.Signature.empty or annotation is Any:
        return None
    if isinstance(annotation, str):
        return None
    origin = get_origin(annotation)
    if origin is Annotated:
        args = get_args(annotation)
        annotation = args[0] if args else annotation
        origin = get_origin(annotation)
    if origin is Inject:
        args = get_args(annotation)
        annotation = args[0] if args else None
        origin = get_origin(annotation)
    return annotation if isinstance(annotation, type) else None


__all__ = (
    "Inject",
    "DependencySpec",
    "Provider",
    "ProviderFactory",
    "ProviderRegistry",
    "ProviderSpec",
    "SCOPES",
    "Scope",
)
