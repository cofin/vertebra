"""Topological and type-aware dependency injection container with singleton, request, and transient scopes."""

from __future__ import annotations

import inspect
import typing
from collections.abc import (
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Generator,
    Iterable,
    Iterator,
    Mapping,
)
from contextlib import AsyncExitStack, ExitStack, asynccontextmanager, contextmanager
from types import MappingProxyType
from typing import Annotated, Any, Literal, get_args, get_origin

import msgspec

__all__ = (
    "Container",
    "DependencyGraph",
    "Inject",
    "ParamBinding",
    "Provide",
    "RequestScope",
    "Scope",
    "coerce_provider",
    "extract_inject_marker",
    "inspect_callable_bindings",
)

type Scope = Literal["singleton", "request", "transient"]

_REQUEST_CONTEXT_NAMES = frozenset({"request", "req", "headers", "query", "path", "method", "body"})
_BUILTIN_SCALAR_TYPES = frozenset({str, int, float, bool, bytes, bytearray, memoryview, dict, list, tuple, set, object})
_GENERATOR_ORIGINS = frozenset(
    {
        Iterator,
        Iterable,
        Generator,
        AsyncIterator,
        AsyncIterable,
        AsyncGenerator,
        typing.Iterator,
        typing.Iterable,
        typing.Generator,
        typing.AsyncIterator,
        typing.AsyncIterable,
        typing.AsyncGenerator,
    }
)


class Inject(msgspec.Struct, frozen=True, gc=False, cache_hash=True):
    """Dependency injection marker usable via Annotated[T, Inject(...)] or Inject[T]."""

    key: str | type[Any] | None = None

    def __class_getitem__(cls, item: Any) -> Any:
        """Construct an Annotated[T, Inject(...)] alias from Inject[T] or Inject[T, 'key']."""
        if isinstance(item, tuple) and len(item) == 2:
            return Annotated[item[0], cls(key=item[1])]
        return Annotated[item, cls()]


class ParamBinding(msgspec.Struct, frozen=True, gc=False, cache_hash=True):
    """Precomputed parameter metadata for a provider or route handler."""

    name: str
    annotation: Any = Any
    raw_annotation: Any = Any
    inject_key: str | type[Any] | None = None
    is_explicit_inject: bool = False
    has_default: bool = False
    default: Any = None


class Provide(msgspec.Struct, frozen=True, gc=False, cache_hash=True):
    """Dependency provider descriptor with scope, generator teardown, and type metadata."""

    dependency: Callable[..., Any]
    scope: Scope = "request"
    sync_to_thread: bool = False
    use_cache: bool = True
    parameter_names: tuple[str, ...] = ()
    param_bindings: tuple[ParamBinding, ...] = ()
    return_type: type[Any] | None = None
    is_async: bool = False
    is_generator: bool = False
    is_async_generator: bool = False

    def __post_init__(self) -> None:
        """Precompute parameter bindings, generator flags, and inferred return type at registration time."""
        dep = self.dependency
        bindings = self.param_bindings or inspect_callable_bindings(dep)
        if not self.param_bindings:
            msgspec.structs.force_setattr(self, "param_bindings", bindings)
        if not self.parameter_names:
            msgspec.structs.force_setattr(self, "parameter_names", tuple(b.name for b in bindings))
        if self.return_type is None:
            inferred = _infer_provider_type(dep)
            if inferred is not None:
                msgspec.structs.force_setattr(self, "return_type", inferred)

        is_gen = not inspect.isclass(dep) and inspect.isgeneratorfunction(dep)
        is_async_gen = not inspect.isclass(dep) and inspect.isasyncgenfunction(dep)
        is_coro = not inspect.isclass(dep) and (inspect.iscoroutinefunction(dep) or is_async_gen)
        msgspec.structs.force_setattr(self, "is_generator", is_gen)
        msgspec.structs.force_setattr(self, "is_async_generator", is_async_gen)
        msgspec.structs.force_setattr(self, "is_async", is_coro)

    @classmethod
    def value(cls, instance: Any, *, return_type: type[Any] | None = None) -> Provide:
        """Construct a singleton provider returning a pre-built constant value."""

        def _constant(_val: Any = instance) -> Any:
            return _val

        resolved_type = return_type if return_type is not None else type(instance)
        return cls(
            dependency=_constant,
            scope="singleton",
            use_cache=True,
            parameter_names=(),
            param_bindings=(),
            return_type=resolved_type if resolved_type not in _BUILTIN_SCALAR_TYPES else None,
        )


class DependencyGraph:
    """Topologically sorted dependency resolution graph compiled at application startup."""

    __slots__ = ("_order", "_providers", "_type_index")

    def __init__(self, providers: Mapping[str, Any] | None = None) -> None:
        """Initialize the dependency graph with optional initial providers."""
        self._providers: dict[str, Provide] = {}
        self._type_index: dict[type[Any], str] = {}
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
    def type_index(self) -> MappingProxyType[type[Any], str]:
        """Return a read-only mapping from provided types to provider keys."""
        return MappingProxyType(self._type_index)

    @property
    def topological_order(self) -> tuple[str, ...]:
        """Return the compiled topological resolution order."""
        return self._order

    def register(self, key: str, provider: Any, *, scope: Scope = "request") -> Provide:
        """Register a provider under key and index its inferred return type."""
        wrapped = coerce_provider(provider, default_scope=scope)
        self._providers[key] = wrapped
        if wrapped.return_type is not None and wrapped.return_type not in _BUILTIN_SCALAR_TYPES:
            self._type_index[wrapped.return_type] = key
        return wrapped

    def register_type(
        self,
        service_type: type[Any],
        provider: Any = None,
        *,
        key: str | None = None,
        scope: Scope = "request",
    ) -> Provide:
        """Register a provider indexed by both key and explicit service_type."""
        resolved_key = key or service_type.__qualname__
        target = provider if provider is not None else service_type
        wrapped = coerce_provider(target, default_scope=scope)
        if wrapped.return_type is None:
            msgspec.structs.force_setattr(wrapped, "return_type", service_type)
        self._providers[resolved_key] = wrapped
        self._type_index[service_type] = resolved_key
        return wrapped

    def merge(self, overrides: Mapping[str, Any] | DependencyGraph | None) -> DependencyGraph:
        """Return a new compiled DependencyGraph merging this graph with overrides."""
        merged = DependencyGraph()
        for key, prov in self._providers.items():
            merged._providers[key] = prov
        merged._type_index.update(self._type_index)
        if overrides is not None:
            items = overrides.providers.items() if isinstance(overrides, DependencyGraph) else overrides.items()
            for key, prov in items:
                merged.register(key, prov)
        merged.compile()
        return merged

    def resolve_param_key(self, binding: ParamBinding, *, auto_register_explicit: bool = False) -> str | None:
        """Resolve a parameter binding to a registered provider key by explicit key, name, or type."""
        if binding.inject_key is not None:
            if isinstance(binding.inject_key, str):
                return binding.inject_key
            if isinstance(binding.inject_key, type):
                if binding.inject_key in self._type_index:
                    return self._type_index[binding.inject_key]
                if auto_register_explicit and binding.inject_key not in _BUILTIN_SCALAR_TYPES:
                    self.register_type(binding.inject_key)
                    self.compile()
                    return self._type_index[binding.inject_key]
                return binding.inject_key.__qualname__

        if binding.name in self._providers:
            return binding.name

        ann = binding.annotation
        if inspect.isclass(ann) and ann in self._type_index:
            return self._type_index[ann]

        if (
            auto_register_explicit
            and binding.is_explicit_inject
            and inspect.isclass(ann)
            and ann not in _BUILTIN_SCALAR_TYPES
        ):
            self.register_type(ann)
            self.compile()
            return self._type_index[ann]

        return None

    def is_subgraph_async(self, key: str) -> bool:
        """Return True if key or any of its transitive dependencies is asynchronous."""
        prov = self._providers.get(key)
        if prov is None:
            return False
        if prov.is_async:
            return True
        for binding in prov.param_bindings:
            dep_key = self.resolve_param_key(binding)
            if dep_key is not None and dep_key in self._providers and self.is_subgraph_async(dep_key):
                return True
        return False

    def is_subgraph_generator(self, key: str) -> bool:
        """Return True if key or any of its transitive request/transient dependencies is a generator."""
        prov = self._providers.get(key)
        if prov is None:
            return False
        if (prov.is_generator or prov.is_async_generator) and prov.scope != "singleton":
            return True
        for binding in prov.param_bindings:
            dep_key = self.resolve_param_key(binding)
            if dep_key is not None and dep_key in self._providers and self.is_subgraph_generator(dep_key):
                return True
        return False

    def compile(self) -> tuple[str, ...]:
        """Compile and validate the dependency DAG, raising ValueError on cycles."""
        for prov in list(self._providers.values()):
            for binding in prov.param_bindings:
                if (
                    binding.is_explicit_inject
                    and inspect.isclass(binding.annotation)
                    and binding.annotation not in _BUILTIN_SCALAR_TYPES
                    and binding.annotation not in self._type_index
                    and binding.name not in self._providers
                ):
                    self.register_type(binding.annotation)

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
                for binding in prov.param_bindings:
                    dep_key = self.resolve_param_key(binding)
                    if dep_key is not None and dep_key in self._providers:
                        _visit(dep_key, (*trail, node))
            visited[node] = 2
            ordered.append(node)

        for key in list(self._providers):
            if visited.get(key, 0) == 0:
                _visit(key, ())
        self._order = tuple(ordered)
        return self._order


class RequestScope:
    """Per-request dependency cache and generator cleanup scope."""

    __slots__ = ("_async_stack", "_cache", "_container", "_extra_context", "_request", "_sync_stack")

    def __init__(
        self,
        container: Container,
        *,
        request: Any = None,
        extra_context: Mapping[str, Any] | None = None,
    ) -> None:
        """Initialize a request-scoped resolution context."""
        self._container = container
        self._request = request
        self._extra_context = extra_context
        self._cache: dict[str, Any] = {}
        self._sync_stack = ExitStack()
        self._async_stack = AsyncExitStack()

    @property
    def cache(self) -> dict[str, Any]:
        """Return the mutable request dependency cache."""
        return self._cache

    def resolve_sync(self, key_or_type: str | type[Any]) -> Any:
        """Synchronously resolve a dependency within this request scope."""
        return self._container.resolve_sync(
            key_or_type,
            request=self._request,
            request_cache=self._cache,
            exit_stack=self._sync_stack,
            extra_context=self._extra_context,
        )

    async def resolve(self, key_or_type: str | type[Any]) -> Any:
        """Asynchronously resolve a dependency within this request scope."""
        return await self._container.resolve(
            key_or_type,
            request=self._request,
            request_cache=self._cache,
            exit_stack=self._sync_stack,
            async_exit_stack=self._async_stack,
            extra_context=self._extra_context,
        )

    def close(self) -> None:
        """Close all synchronous request-scoped generator contexts."""
        self._sync_stack.close()

    async def aclose(self) -> None:
        """Close all asynchronous and synchronous request-scoped generator contexts."""
        try:
            await self._async_stack.aclose()
        finally:
            self._sync_stack.close()

    def __enter__(self) -> RequestScope:
        """Enter synchronous request scope."""
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Exit synchronous request scope and run generator cleanup."""
        self._sync_stack.__exit__(exc_type, exc, tb)

    async def __aenter__(self) -> RequestScope:
        """Enter asynchronous request scope."""
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Exit asynchronous request scope and run generator cleanup."""
        try:
            await self._async_stack.__aexit__(exc_type, exc, tb)
        finally:
            self._sync_stack.__exit__(exc_type, exc, tb)


class Container:
    """Scoped dependency injection container executing a compiled DependencyGraph."""

    __slots__ = ("_graph", "_singleton_async_stack", "_singleton_sync_stack", "_singletons")

    def __init__(
        self,
        graph: DependencyGraph | Mapping[str, Any] | None = None,
        *,
        singletons: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the container with a DependencyGraph or provider mapping."""
        if isinstance(graph, DependencyGraph):
            self._graph = graph
        else:
            self._graph = DependencyGraph(graph)
        self._singletons: dict[str, Any] = singletons if singletons is not None else {}
        self._singleton_sync_stack = ExitStack()
        self._singleton_async_stack = AsyncExitStack()

    @property
    def graph(self) -> DependencyGraph:
        """Return the underlying DependencyGraph."""
        return self._graph

    @property
    def singletons(self) -> dict[str, Any]:
        """Return the active singleton instance cache."""
        return self._singletons

    def create_child(self, overrides: Mapping[str, Any] | DependencyGraph | None = None) -> Container:
        """Create a child container inheriting un-overridden singletons while isolating layer overrides."""
        if not overrides:
            return self
        merged_graph = self._graph.merge(overrides)
        override_keys = set(overrides.providers.keys() if isinstance(overrides, DependencyGraph) else overrides.keys())
        inherited_singletons = (
            {k: v for k, v in self._singletons.items() if k not in override_keys} if override_keys else self._singletons
        )
        return Container(merged_graph, singletons=inherited_singletons)

    def scope(
        self,
        *,
        request: Any = None,
        extra_context: Mapping[str, Any] | None = None,
    ) -> RequestScope:
        """Create a synchronous or asynchronous request scope context manager."""
        return RequestScope(self, request=request, extra_context=extra_context)

    def resolve_sync(
        self,
        key_or_type: str | type[Any],
        *,
        request: Any = None,
        request_cache: dict[str, Any] | None = None,
        exit_stack: ExitStack | None = None,
        extra_context: Mapping[str, Any] | None = None,
    ) -> Any:
        """Synchronously resolve a dependency key or type and its transitive dependencies."""
        if not self._graph.topological_order and self._graph.providers:
            self._graph.compile()

        key = self._normalize_lookup_key(key_or_type)
        cache = request_cache if request_cache is not None else {}
        if key in cache:
            return cache[key]
        if key in self._singletons:
            return self._singletons[key]

        prov = self._graph.providers.get(key)
        if prov is None:
            msg = f"Unknown dependency key: {key_or_type!r}"
            raise KeyError(msg)
        if prov.is_async:
            msg = f"Cannot synchronously resolve async dependency {key!r}"
            raise RuntimeError(msg)

        kwargs = self._build_sync_kwargs(
            prov,
            request=request,
            cache=cache,
            exit_stack=exit_stack,
            extra_context=extra_context,
        )

        if prov.is_generator:
            cm = contextmanager(prov.dependency)(**kwargs)
            if prov.scope == "singleton":
                result = self._singleton_sync_stack.enter_context(cm)
            elif exit_stack is not None:
                result = exit_stack.enter_context(cm)
            else:
                result = self._singleton_sync_stack.enter_context(cm)
        else:
            result = prov.dependency(**kwargs)

        if prov.scope == "singleton":
            self._singletons[key] = result
        elif prov.scope == "request" and prov.use_cache:
            cache[key] = result
        return result

    async def resolve(
        self,
        key_or_type: str | type[Any],
        *,
        request: Any = None,
        request_cache: dict[str, Any] | None = None,
        exit_stack: ExitStack | None = None,
        async_exit_stack: AsyncExitStack | None = None,
        extra_context: Mapping[str, Any] | None = None,
    ) -> Any:
        """Resolve a dependency key or type and its transitive dependencies."""
        if not self._graph.topological_order and self._graph.providers:
            self._graph.compile()

        key = self._normalize_lookup_key(key_or_type)
        cache = request_cache if request_cache is not None else {}
        if key in cache:
            return cache[key]
        if key in self._singletons:
            return self._singletons[key]

        prov = self._graph.providers.get(key)
        if prov is None:
            msg = f"Unknown dependency key: {key_or_type!r}"
            raise KeyError(msg)

        kwargs = await self._build_async_kwargs(
            prov,
            request=request,
            cache=cache,
            exit_stack=exit_stack,
            async_exit_stack=async_exit_stack,
            extra_context=extra_context,
        )

        if prov.is_async_generator:
            acm = asynccontextmanager(prov.dependency)(**kwargs)
            if prov.scope == "singleton":
                result = await self._singleton_async_stack.enter_async_context(acm)
            elif async_exit_stack is not None:
                result = await async_exit_stack.enter_async_context(acm)
            else:
                result = await self._singleton_async_stack.enter_async_context(acm)
        elif prov.is_generator:
            cm = contextmanager(prov.dependency)(**kwargs)
            if prov.scope == "singleton":
                result = self._singleton_sync_stack.enter_context(cm)
            elif exit_stack is not None:
                result = exit_stack.enter_context(cm)
            elif async_exit_stack is not None:
                result = async_exit_stack.enter_context(cm)
            else:
                result = self._singleton_sync_stack.enter_context(cm)
        else:
            result = prov.dependency(**kwargs)
            if inspect.isawaitable(result):
                result = await result

        if prov.scope == "singleton":
            self._singletons[key] = result
        elif prov.scope == "request" and prov.use_cache:
            cache[key] = result
        return result

    def close(self) -> None:
        """Close all singleton synchronous generator resources."""
        self._singleton_sync_stack.close()

    async def aclose(self) -> None:
        """Close all singleton asynchronous and synchronous generator resources."""
        try:
            await self._singleton_async_stack.aclose()
        finally:
            self._singleton_sync_stack.close()

    def _normalize_lookup_key(self, key_or_type: str | type[Any]) -> str:
        """Convert a string key or class type into a canonical provider key."""
        if isinstance(key_or_type, str):
            return key_or_type
        if isinstance(key_or_type, type):
            mapped = self._graph.type_index.get(key_or_type)
            if mapped is not None:
                return mapped
            if key_or_type.__qualname__ in self._graph.providers:
                return key_or_type.__qualname__
            if key_or_type.__name__ in self._graph.providers:
                return key_or_type.__name__
        return str(key_or_type)

    def _build_sync_kwargs(
        self,
        prov: Provide,
        *,
        request: Any,
        cache: dict[str, Any],
        exit_stack: ExitStack | None,
        extra_context: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        """Resolve synchronous keyword arguments for a provider invocation."""
        kwargs: dict[str, Any] = {}
        for binding in prov.param_bindings:
            dep_key = self._graph.resolve_param_key(binding)
            if dep_key is not None and dep_key in self._graph.providers:
                kwargs[binding.name] = self.resolve_sync(
                    dep_key,
                    request=request,
                    request_cache=cache,
                    exit_stack=exit_stack,
                    extra_context=extra_context,
                )
            elif extra_context is not None and binding.name in extra_context:
                kwargs[binding.name] = extra_context[binding.name]
            elif binding.name in cache:
                kwargs[binding.name] = cache[binding.name]
            elif request is not None and self._can_extract_from_request(binding):
                kwargs[binding.name] = self._extract_from_request(binding, request)
            elif binding.has_default:
                kwargs[binding.name] = binding.default
        return kwargs

    async def _build_async_kwargs(
        self,
        prov: Provide,
        *,
        request: Any,
        cache: dict[str, Any],
        exit_stack: ExitStack | None,
        async_exit_stack: AsyncExitStack | None,
        extra_context: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        """Resolve asynchronous keyword arguments for a provider invocation."""
        kwargs: dict[str, Any] = {}
        for binding in prov.param_bindings:
            dep_key = self._graph.resolve_param_key(binding)
            if dep_key is not None and dep_key in self._graph.providers:
                kwargs[binding.name] = await self.resolve(
                    dep_key,
                    request=request,
                    request_cache=cache,
                    exit_stack=exit_stack,
                    async_exit_stack=async_exit_stack,
                    extra_context=extra_context,
                )
            elif extra_context is not None and binding.name in extra_context:
                kwargs[binding.name] = extra_context[binding.name]
            elif binding.name in cache:
                kwargs[binding.name] = cache[binding.name]
            elif request is not None and self._can_extract_from_request(binding):
                kwargs[binding.name] = self._extract_from_request(binding, request)
            elif binding.has_default:
                kwargs[binding.name] = binding.default
        return kwargs

    @staticmethod
    def _can_extract_from_request(binding: ParamBinding) -> bool:
        """Return True if a provider parameter requests the active Request or one of its attributes."""
        if binding.name in _REQUEST_CONTEXT_NAMES:
            return True
        return getattr(binding.annotation, "__name__", "") == "Request"

    @staticmethod
    def _extract_from_request(binding: ParamBinding, request: Any) -> Any:
        """Extract request or request metadata for a provider parameter."""
        if binding.name in ("request", "req") or getattr(binding.annotation, "__name__", "") == "Request":
            return request
        if binding.name == "headers":
            return getattr(request, "headers", None)
        if binding.name == "query":
            return getattr(request, "query", None)
        if binding.name == "path":
            return getattr(request, "path", "/")
        if binding.name == "method":
            return getattr(request, "method", "GET")
        if binding.name == "body":
            body = getattr(request, "body", b"")
            return body if isinstance(body, bytes) else bytes(body or b"")
        return request


def coerce_provider(provider: Any, *, default_scope: Scope = "request") -> Provide:
    """Coerce a Provide instance, callable, class, or constant value into a Provide descriptor."""
    if isinstance(provider, Provide):
        return provider
    if callable(provider):
        return Provide(dependency=provider, scope=default_scope)
    return Provide.value(provider)


def extract_inject_marker(annotation: Any) -> tuple[Any, Inject | None]:
    """Unwrap Annotated[T, ...] and return (base_type, Inject) if a dependency marker is present."""
    origin = get_origin(annotation)
    if origin is not Annotated:
        return annotation, None

    args = get_args(annotation)
    if not args:
        return annotation, None

    base_type = args[0]
    found_inject: Inject | None = None
    for meta in args[1:]:
        if isinstance(meta, Inject):
            found_inject = meta
            break
        if isinstance(meta, type) and issubclass(meta, Inject):
            found_inject = meta()
            break
        if type(meta).__name__ in ("NamedDependencyMarker", "DependencyKwarg"):
            found_inject = Inject(key=getattr(meta, "key", None))
            break

    inner_base, inner_inject = extract_inject_marker(base_type)
    return inner_base, (found_inject or inner_inject)


def inspect_callable_bindings(fn: Callable[..., Any]) -> tuple[ParamBinding, ...]:
    """Inspect a callable or class and return precomputed ParamBinding descriptors."""
    if isinstance(fn, type) and issubclass(fn, msgspec.Struct):
        struct_bindings: list[ParamBinding] = []
        for field in msgspec.structs.fields(fn):
            base_type, inject_marker = extract_inject_marker(field.type)
            has_def = field.default is not msgspec.NODEFAULT or field.default_factory is not msgspec.NODEFAULT
            def_val = (
                field.default
                if field.default is not msgspec.NODEFAULT
                else (field.default_factory() if field.default_factory is not msgspec.NODEFAULT else None)
            )
            struct_bindings.append(
                ParamBinding(
                    name=field.name,
                    annotation=base_type,
                    raw_annotation=field.type,
                    inject_key=inject_marker.key if inject_marker is not None else None,
                    is_explicit_inject=inject_marker is not None,
                    has_default=has_def,
                    default=def_val,
                )
            )
        return tuple(struct_bindings)

    target: Any = fn.__init__ if inspect.isclass(fn) else fn
    try:
        sig = inspect.signature(target)
    except (TypeError, ValueError):
        return ()

    hints = _safe_get_type_hints(target)
    bindings: list[ParamBinding] = []
    for name, param in sig.parameters.items():
        if name in ("self", "cls"):
            continue
        if param.kind not in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY):
            continue
        raw_ann = hints.get(name, param.annotation)
        if raw_ann is inspect.Parameter.empty:
            raw_ann = Any
        base_type, inject_marker = extract_inject_marker(raw_ann)
        has_default = param.default is not inspect.Parameter.empty
        bindings.append(
            ParamBinding(
                name=name,
                annotation=base_type,
                raw_annotation=raw_ann,
                inject_key=inject_marker.key if inject_marker is not None else None,
                is_explicit_inject=inject_marker is not None,
                has_default=has_default,
                default=param.default if has_default else None,
            )
        )
    return tuple(bindings)


def _safe_get_type_hints(target: Any) -> dict[str, Any]:
    """Extract resolved type hints with include_extras=True, falling back to per-annotation resolution."""
    unwrapped = inspect.unwrap(target)
    localns = dict(getattr(unwrapped, "__globals__", {}) or {})
    try:
        closure = inspect.getclosurevars(unwrapped)
        localns.update(closure.nonlocals)
    except Exception:
        pass
    try:
        return typing.get_type_hints(unwrapped, globalns=localns, localns=localns, include_extras=True)
    except Exception:
        frame = inspect.currentframe()
        try:
            caller = frame.f_back if frame is not None else None
            depth = 0
            frames: list[Any] = []
            while caller is not None and depth < 8:
                if caller.f_code.co_filename != __file__:
                    frames.append(caller)
                caller = caller.f_back
                depth += 1
            for f in reversed(frames):
                localns.update(f.f_locals)
        finally:
            del frame
        try:
            return typing.get_type_hints(unwrapped, globalns=localns, localns=localns, include_extras=True)
        except Exception:
            raw_annotations = dict(getattr(unwrapped, "__annotations__", getattr(target, "__annotations__", {})) or {})
            resolved: dict[str, Any] = {}
            for key, val in raw_annotations.items():
                if isinstance(val, str):
                    try:
                        resolved[key] = eval(val, localns, localns)
                    except Exception:
                        resolved[key] = val
                else:
                    resolved[key] = val
            return resolved


def _infer_provider_type(fn: Callable[..., Any]) -> type[Any] | None:
    """Infer the concrete class type produced by a provider callable or class."""
    if inspect.isclass(fn):
        return fn
    hints = _safe_get_type_hints(fn)
    ret = hints.get("return", inspect.Parameter.empty)
    if ret is inspect.Parameter.empty:
        try:
            ret = inspect.signature(fn).return_annotation
        except (TypeError, ValueError):
            return None
    if ret is inspect.Parameter.empty or ret is None or ret is Any:
        return None
    base_ret, _ = extract_inject_marker(ret)
    origin = get_origin(base_ret)
    if origin in _GENERATOR_ORIGINS:
        args = get_args(base_ret)
        if args:
            base_ret, _ = extract_inject_marker(args[0])
    if inspect.isclass(base_ret):
        return base_ret
    return None
