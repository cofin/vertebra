"""Dependency injection primitives."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
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


class Inject[T]:
    """Marker type for injectable dependencies."""

    __slots__ = ()


@runtime_checkable
class Provider(Protocol[T]):
    """Protocol for dependency providers."""

    provides: type[T]
    scope: Scope

    def __call__(self, *args: Any, **kwargs: Any) -> T | Awaitable[T]: ...


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


class DependencyError(RuntimeError):
    """Raised when the dependency graph cannot be resolved."""


class DependencyGraph:
    """Resolve provider dependencies into a topological execution order."""

    __slots__ = ("_providers",)

    def __init__(self, providers: dict[type[Any], ProviderSpec[Any]]) -> None:
        self._providers = providers

    def resolve_order(self, targets: tuple[type[Any], ...]) -> list[ProviderSpec[Any]]:
        order: list[ProviderSpec[Any]] = []
        visiting: set[type[Any]] = set()
        visited: set[type[Any]] = set()

        def resolve(target: type[Any]) -> None:
            if target in visited:
                return
            if target in visiting:
                msg = f"cycle detected at {target!r}"
                raise DependencyError(msg)
            spec = self._providers.get(target)
            if spec is None:
                msg = f"missing provider for {target!r}"
                raise DependencyError(msg)

            visiting.add(target)
            for dependency in spec.dependencies:
                resolve(dependency.dependency)
            visiting.remove(target)
            visited.add(target)
            order.append(spec)

        for target in targets:
            resolve(target)

        return order


class ProviderCompiler:
    """Compile provider graphs into linear runner functions."""

    __slots__ = ("_cache", "_providers", "_singletons")

    def __init__(self, providers: dict[type[Any], ProviderSpec[Any]]) -> None:
        self._providers = providers
        self._cache: dict[type[Any], Callable[[dict[type[Any], Any] | None], Awaitable[Any]]] = {}
        self._singletons: dict[type[Any], Any] = {}

    def compile(self, target: type[Any]) -> Callable[[dict[type[Any], Any] | None], Awaitable[Any]]:
        if target in self._cache:
            return self._cache[target]
        graph = DependencyGraph(self._providers)
        order = graph.resolve_order((target,))
        runner = _compile_runner(order, target, self._singletons)
        self._cache[target] = runner
        return runner


class ProviderRegistry:
    """Register provider factories and infer dependency metadata."""

    __slots__ = ("_providers",)

    def __init__(self) -> None:
        self._providers: dict[type[Any], ProviderSpec[Any]] = {}

    @property
    def providers(self) -> dict[type[Any], ProviderSpec[Any]]:
        return self._providers

    def provide(
        self, provider: type[T] | ProviderFactory[T] | None = None, *, scope: Scope = "singleton"
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

        msg = "provider must be a class or callable"
        raise TypeError(msg)

    def compile(self) -> ProviderCompiler:
        return ProviderCompiler(self._providers)

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
        msg = "provider factories must declare a return type"
        raise ValueError(msg)
    dependency = _unwrap_inject(return_annotation)
    if dependency is None:
        msg = "provider factories must declare a concrete return type"
        raise ValueError(msg)
    return dependency


def _collect_dependencies(callable_obj: Callable[..., Any], *, skip_first: bool = False) -> tuple[DependencySpec, ...]:
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
        return get_type_hints(callable_obj, globalns=getattr(callable_obj, "__globals__", None), include_extras=True)
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


def _compile_runner(
    order: list[ProviderSpec[Any]], target: type[Any], singleton_cache: dict[type[Any], Any]
) -> Callable[[dict[type[Any], Any] | None], Awaitable[Any]]:
    local_ns: dict[str, Any] = {"target_type": target, "singleton_cache": singleton_cache}
    body = ["    if request_cache is None:", "        request_cache = {}", "    resolved = {}"]

    for index, spec in enumerate(order):
        factory_key = f"factory_{index}"
        type_key = f"type_{index}"
        scope_key = f"scope_{index}"
        local_ns[factory_key] = spec.factory
        local_ns[type_key] = spec.provides
        local_ns[scope_key] = spec.scope

        args = []
        for dep_index, dependency in enumerate(spec.dependencies):
            dep_key = f"dep_{index}_{dep_index}"
            local_ns[dep_key] = dependency.dependency
            args.append(f"resolved[{dep_key}]")

        args_str = ", ".join(args)
        if inspect.iscoroutinefunction(spec.factory):
            call_expr = f"await {factory_key}({args_str})"
        else:
            call_expr = f"{factory_key}({args_str})"

        body.append(f"    if {scope_key} == 'singleton' and {type_key} in singleton_cache:")
        body.append(f"        resolved[{type_key}] = singleton_cache[{type_key}]")
        body.append(f"    elif {scope_key} == 'request' and {type_key} in request_cache:")
        body.append(f"        resolved[{type_key}] = request_cache[{type_key}]")
        body.append("    else:")
        body.append(f"        resolved[{type_key}] = {call_expr}")
        body.append(f"        if {scope_key} == 'singleton':")
        body.append(f"            singleton_cache[{type_key}] = resolved[{type_key}]")
        body.append(f"        elif {scope_key} == 'request':")
        body.append(f"            request_cache[{type_key}] = resolved[{type_key}]")

    body.append("    return resolved[target_type]")
    source = "async def runner(request_cache=None):\n" + "\n".join(body)
    exec(source, local_ns)  # noqa: S102
    return local_ns["runner"]


__all__ = (
    "SCOPES",
    "DependencyError",
    "DependencyGraph",
    "DependencySpec",
    "Inject",
    "Provider",
    "ProviderCompiler",
    "ProviderFactory",
    "ProviderRegistry",
    "ProviderSpec",
    "Scope",
)
