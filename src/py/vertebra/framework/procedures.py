"""Typed RPC procedures (@query, @mutation, @subscription) with DI and client codegen metadata."""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any, Literal

import msgspec

from vertebra.framework.di import (
    Container,
    ParamBinding,
    inspect_callable_bindings,
)
from vertebra.utils.serializers import decode_json

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

__all__ = (
    "Procedure",
    "ProcedureKind",
    "ProcedureRouter",
    "mutation",
    "query",
    "subscription",
)

type ProcedureKind = Literal["query", "mutation", "subscription"]


class Procedure(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Typed RPC procedure definition (query, mutation, or subscription)."""

    name: str
    kind: ProcedureKind
    fn: Callable[..., Any]
    input_type: type[Any] | None = None
    output_type: type[Any] | None = None
    dependencies: dict[str, Any] = msgspec.field(default_factory=dict)
    guards: tuple[Callable[..., Any], ...] = ()

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Invoke the underlying procedure function directly."""
        return self.fn(*args, **kwargs)

    def with_context(
        self,
        *,
        fn: Callable[..., Any] | None = None,
        dependencies: Mapping[str, Any] | None = None,
        guards: Sequence[Callable[..., Any]] | None = None,
    ) -> Procedure:
        """Return a copy of this Procedure with merged parent dependencies, guards, or bound callable."""
        merged_deps = dict(dependencies) if dependencies else {}
        merged_deps.update(self.dependencies)
        merged_guards = (*tuple(guards or ()), *self.guards)
        return Procedure(
            name=self.name,
            kind=self.kind,
            fn=fn if fn is not None else self.fn,
            input_type=self.input_type,
            output_type=self.output_type,
            dependencies=merged_deps,
            guards=merged_guards,
        )

    @classmethod
    def decorator(
        cls,
        kind: ProcedureKind,
        *,
        name: str | None = None,
        input_type: type[Any] | None = None,
        output_type: type[Any] | None = None,
        dependencies: Mapping[str, Any] | None = None,
        guards: Sequence[Callable[..., Any]] | None = None,
    ) -> Callable[[Callable[..., Any]], Procedure]:
        """Construct a decorator returning a typed Procedure."""

        def _dec(fn: Callable[..., Any]) -> Procedure:
            inferred_in, inferred_out = _infer_procedure_types(fn, input_type, output_type)
            return cls(
                name=name or fn.__name__,
                kind=kind,
                fn=fn,
                input_type=inferred_in,
                output_type=inferred_out,
                dependencies=dict(dependencies) if dependencies else {},
                guards=tuple(guards) if guards else (),
            )

        return _dec


class ProcedureRouter:
    """Registry and dispatcher for typed RPC procedures with hierarchical DI and guards."""

    __slots__ = ("_container", "_dependencies", "_guards", "_prefix", "_procedures")

    def __init__(
        self,
        *,
        prefix: str = "/_rpc",
        dependencies: Mapping[str, Any] | None = None,
        guards: Sequence[Callable[..., Any]] | None = None,
        container: Container | None = None,
    ) -> None:
        """Initialize router with URL prefix, optional dependencies, and guards."""
        self._prefix = prefix.rstrip("/") or "/_rpc"
        self._dependencies: dict[str, Any] = dict(dependencies) if dependencies else {}
        self._guards: tuple[Callable[..., Any], ...] = tuple(guards) if guards else ()
        self._container = container
        self._procedures: dict[str, Procedure] = {}

    @property
    def prefix(self) -> str:
        """Return the RPC URL prefix."""
        return self._prefix

    @property
    def dependencies(self) -> dict[str, Any]:
        """Return router-level dependency providers."""
        return dict(self._dependencies)

    @property
    def guards(self) -> tuple[Callable[..., Any], ...]:
        """Return router-level guard callables."""
        return self._guards

    @property
    def procedures(self) -> dict[str, Procedure]:
        """Return registered procedures keyed by name."""
        return dict(self._procedures)

    def register(self, target: Any) -> Any:
        """Register a Procedure, ProcedureRouter, or Controller with procedure methods."""
        if isinstance(target, Procedure):
            return self._register_procedure(target)
        if isinstance(target, ProcedureRouter):
            for proc in target.procedures.values():
                self._register_procedure(proc)
            return target
        get_procs = getattr(target, "get_procedures", None)
        if callable(get_procs):
            for proc in get_procs():
                self._register_procedure(proc)
            return target
        msg = f"ProcedureRouter.register expects a Procedure, ProcedureRouter, or Controller, got {type(target)!r}"
        raise TypeError(msg)

    def _register_procedure(self, proc: Procedure) -> Procedure:
        """Bind router-level dependencies and guards to a Procedure and store it."""
        bound = proc.with_context(dependencies=self._dependencies, guards=self._guards)
        self._procedures[bound.name] = bound
        return bound

    def query(
        self,
        *,
        name: str | None = None,
        input_type: type[Any] | None = None,
        output_type: type[Any] | None = None,
        dependencies: Mapping[str, Any] | None = None,
        guards: Sequence[Callable[..., Any]] | None = None,
    ) -> Callable[[Callable[..., Any]], Procedure]:
        """Decorator defining and registering a query procedure on this router."""

        def _decorator(fn: Callable[..., Any]) -> Procedure:
            proc = Procedure.decorator(
                "query",
                name=name,
                input_type=input_type,
                output_type=output_type,
                dependencies=dependencies,
                guards=guards,
            )(fn)
            return self._register_procedure(proc)

        return _decorator

    def mutation(
        self,
        *,
        name: str | None = None,
        input_type: type[Any] | None = None,
        output_type: type[Any] | None = None,
        dependencies: Mapping[str, Any] | None = None,
        guards: Sequence[Callable[..., Any]] | None = None,
    ) -> Callable[[Callable[..., Any]], Procedure]:
        """Decorator defining and registering a mutation procedure on this router."""

        def _decorator(fn: Callable[..., Any]) -> Procedure:
            proc = Procedure.decorator(
                "mutation",
                name=name,
                input_type=input_type,
                output_type=output_type,
                dependencies=dependencies,
                guards=guards,
            )(fn)
            return self._register_procedure(proc)

        return _decorator

    def subscription(
        self,
        *,
        name: str | None = None,
        input_type: type[Any] | None = None,
        output_type: type[Any] | None = None,
        dependencies: Mapping[str, Any] | None = None,
        guards: Sequence[Callable[..., Any]] | None = None,
    ) -> Callable[[Callable[..., Any]], Procedure]:
        """Decorator defining and registering a subscription procedure on this router."""

        def _decorator(fn: Callable[..., Any]) -> Procedure:
            proc = Procedure.decorator(
                "subscription",
                name=name,
                input_type=input_type,
                output_type=output_type,
                dependencies=dependencies,
                guards=guards,
            )(fn)
            return self._register_procedure(proc)

        return _decorator

    async def call(
        self,
        name: str,
        payload: Any = None,
        *,
        request: Any = None,
        container: Container | None = None,
    ) -> Any:
        """Invoke a registered procedure by name with guard enforcement and dependency injection."""
        if name not in self._procedures:
            msg = f"Unknown RPC procedure: {name!r}"
            raise KeyError(msg)
        proc = self._procedures[name]

        for guard in proc.guards:
            guard_res = guard(request) if request is not None else guard()
            if inspect.isawaitable(guard_res):
                await guard_res

        coerced_payload = _coerce_procedure_payload(payload, proc.input_type)
        base_container = container or self._container
        active_container = (
            base_container.create_child(proc.dependencies)
            if base_container is not None
            else (Container(proc.dependencies) if proc.dependencies else None)
        )

        bindings = inspect_callable_bindings(proc.fn)
        if not bindings:
            res = proc.fn() if coerced_payload is None else proc.fn(coerced_payload)
            if inspect.isawaitable(res):
                res = await res
            return res

        if active_container is None:
            res = proc.fn() if coerced_payload is None else proc.fn(coerced_payload)
            if inspect.isawaitable(res):
                res = await res
            return res

        async with active_container.scope(request=request) as req_scope:
            kwargs: dict[str, Any] = {}
            payload_bound = False
            for binding in bindings:
                dep_key = active_container.graph.resolve_param_key(binding, auto_register_explicit=True)
                if dep_key is not None and dep_key in active_container.graph.providers:
                    kwargs[binding.name] = await req_scope.resolve(dep_key)
                elif not payload_bound and coerced_payload is not None:
                    kwargs[binding.name] = coerced_payload
                    payload_bound = True
                elif request is not None and binding.name in ("request", "req"):
                    kwargs[binding.name] = request
                elif binding.has_default:
                    kwargs[binding.name] = binding.default
            res = proc.fn(**kwargs)
            if inspect.isawaitable(res):
                res = await res
            return res


def query(
    *,
    name: str | None = None,
    input_type: type[Any] | None = None,
    output_type: type[Any] | None = None,
    dependencies: Mapping[str, Any] | None = None,
    guards: Sequence[Callable[..., Any]] | None = None,
) -> Callable[[Callable[..., Any]], Procedure]:
    """Decorator registering a read-only query RPC procedure."""
    return Procedure.decorator(
        "query",
        name=name,
        input_type=input_type,
        output_type=output_type,
        dependencies=dependencies,
        guards=guards,
    )


def mutation(
    *,
    name: str | None = None,
    input_type: type[Any] | None = None,
    output_type: type[Any] | None = None,
    dependencies: Mapping[str, Any] | None = None,
    guards: Sequence[Callable[..., Any]] | None = None,
) -> Callable[[Callable[..., Any]], Procedure]:
    """Decorator registering a state-mutating mutation RPC procedure."""
    return Procedure.decorator(
        "mutation",
        name=name,
        input_type=input_type,
        output_type=output_type,
        dependencies=dependencies,
        guards=guards,
    )


def subscription(
    *,
    name: str | None = None,
    input_type: type[Any] | None = None,
    output_type: type[Any] | None = None,
    dependencies: Mapping[str, Any] | None = None,
    guards: Sequence[Callable[..., Any]] | None = None,
) -> Callable[[Callable[..., Any]], Procedure]:
    """Decorator registering a streaming subscription RPC procedure."""
    return Procedure.decorator(
        "subscription",
        name=name,
        input_type=input_type,
        output_type=output_type,
        dependencies=dependencies,
        guards=guards,
    )


def _infer_procedure_types(
    fn: Callable[..., Any],
    explicit_input: type[Any] | None,
    explicit_output: type[Any] | None,
) -> tuple[type[Any] | None, type[Any] | None]:
    """Infer input_type and output_type from function annotations when not explicitly provided."""
    inferred_in = explicit_input
    inferred_out = explicit_output
    if inferred_in is None:
        bindings: tuple[ParamBinding, ...] = inspect_callable_bindings(fn)
        for binding in bindings:
            if (
                not binding.is_explicit_inject
                and inspect.isclass(binding.annotation)
                and issubclass(binding.annotation, msgspec.Struct)
            ):
                inferred_in = binding.annotation
                break
    if inferred_out is None:
        try:
            sig = inspect.signature(fn)
            if inspect.isclass(sig.return_annotation):
                inferred_out = sig.return_annotation
        except (TypeError, ValueError):
            pass
    return inferred_in, inferred_out


def _coerce_procedure_payload(payload: Any, input_type: type[Any] | None) -> Any:
    """Coerce raw bytes, JSON string, or mapping payload into input_type when configured."""
    if payload is None or input_type is None:
        return payload
    if isinstance(payload, input_type):
        return payload
    if isinstance(payload, str | bytes):
        return decode_json(payload, type=input_type)
    if isinstance(payload, bytearray | memoryview):
        return decode_json(bytes(payload), type=input_type)
    return msgspec.convert(payload, type=input_type)
