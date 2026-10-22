"""Topological dependency injection container with singleton, request, and transient scopes."""

from __future__ import annotations

import inspect
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Literal

import msgspec

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = (
    "Container",
    "DependencyGraph",
    "Provide",
    "Scope",
)

type Scope = Literal["singleton", "request", "transient"]


def _extract_params(fn: Callable[..., Any]) -> tuple[str, ...]:
    """Extract positional-or-keyword and keyword-only parameter names from a callable."""
    sig = inspect.signature(fn)
    return tuple(
        name
        for name, param in sig.parameters.items()
        if param.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    )


class Provide(msgspec.Struct, gc=False):
    """Dependency provider descriptor with explicit scope and sync-to-thread control."""

    dependency: Callable[..., Any]
    scope: Scope = "request"
    sync_to_thread: bool = False
    use_cache: bool = True
    parameter_names: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Precompute parameter names if not explicitly provided."""
        if not self.parameter_names:
            self.parameter_names = _extract_params(self.dependency)


class DependencyGraph:
    """Topologically sorted dependency resolution graph compiled at application startup."""

    __slots__ = ("_order", "_providers")

    def __init__(self, providers: dict[str, Provide | Callable[..., Any]] | None = None) -> None:
        """Initialize the dependency graph with optional initial providers."""
        self._providers: dict[str, Provide] = {}
        self._order: tuple[str, ...] = ()
        if providers:
            for key, provider in providers.items():
                self.register(key, provider)
            self.compile()

    @property
    def providers(self) -> MappingProxyType[str, Provide]:
        """Return a read-only view of registered providers."""
        return MappingProxyType(self._providers)

    @property
    def topological_order(self) -> tuple[str, ...]:
        """Return the compiled topological resolution order."""
        return self._order

    def register(self, key: str, provider: Provide | Callable[..., Any], *, scope: Scope = "request") -> Provide:
        """Register a provider under key."""
        wrapped = provider if isinstance(provider, Provide) else Provide(dependency=provider, scope=scope)
        self._providers[key] = wrapped
        return wrapped

    def compile(self) -> tuple[str, ...]:
        """Compile and validate the dependency DAG, raising ValueError on cycles."""
        visited: dict[str, int] = {}
        ordered: list[str] = []

        def _visit(node: str, trail: tuple[str, ...]) -> None:
            state = visited.get(node, 0)
            if state == 1:
                cycle = " -> ".join((*trail, node))
                msg = f"Circular dependency detected in DependencyGraph: {cycle}"
                raise ValueError(msg)
            if state == 2:
                return
            visited[node] = 1
            prov = self._providers.get(node)
            if prov is not None:
                for dep_name in prov.parameter_names:
                    if dep_name in self._providers:
                        _visit(dep_name, (*trail, node))
            visited[node] = 2
            ordered.append(node)

        for key in self._providers:
            if visited.get(key, 0) == 0:
                _visit(key, ())
        self._order = tuple(ordered)
        return self._order


class Container:
    """Scoped dependency injection container executing a compiled DependencyGraph."""

    __slots__ = ("_graph", "_singletons")

    def __init__(self, graph: DependencyGraph) -> None:
        """Initialize the container with a dependency graph."""
        self._graph = graph
        self._singletons: dict[str, Any] = {}

    @property
    def graph(self) -> DependencyGraph:
        """Return the underlying DependencyGraph."""
        return self._graph

    async def resolve(self, key: str, *, request_cache: dict[str, Any] | None = None) -> Any:
        """Resolve key and its transitive dependencies."""
        if not self._graph.topological_order:
            self._graph.compile()

        cache = request_cache if request_cache is not None else {}
        if key in cache:
            return cache[key]
        if key in self._singletons:
            return self._singletons[key]

        prov = self._graph.providers.get(key)
        if prov is None:
            msg = f"Unknown dependency key: {key!r}"
            raise KeyError(msg)

        kwargs: dict[str, Any] = {}
        for dep_name in prov.parameter_names:
            if dep_name in self._graph.providers:
                kwargs[dep_name] = await self.resolve(dep_name, request_cache=cache)
            elif dep_name in cache:
                kwargs[dep_name] = cache[dep_name]

        result = prov.dependency(**kwargs)
        if inspect.isawaitable(result):
            result = await result

        if prov.scope == "singleton":
            self._singletons[key] = result
        elif prov.scope == "request" and prov.use_cache:
            cache[key] = result
        return result
