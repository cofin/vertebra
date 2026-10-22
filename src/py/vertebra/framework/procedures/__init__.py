"""Typed RPC procedures (`@query`, `@mutation`, `@subscription`) with client codegen metadata."""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any, Literal

import msgspec

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = (
    "ProcedureDef",
    "ProcedureKind",
    "ProcedureRouter",
    "mutation",
    "query",
    "subscription",
)

type ProcedureKind = Literal["query", "mutation", "subscription"]


class ProcedureDef(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Typed RPC procedure definition (`query`, `mutation`, or `subscription`)."""

    name: str
    kind: ProcedureKind
    fn: Callable[..., Any]
    input_type: type[Any] | None = None
    output_type: type[Any] | None = None


class ProcedureRouter:
    """Registry and dispatcher for typed RPC procedures."""

    __slots__ = ("_prefix", "_procedures")

    def __init__(self, *, prefix: str = "/_rpc") -> None:
        self._prefix = prefix
        self._procedures: dict[str, ProcedureDef] = {}

    @property
    def prefix(self) -> str:
        """Return the RPC URL prefix."""
        return self._prefix

    @property
    def procedures(self) -> dict[str, ProcedureDef]:
        """Return registered procedures keyed by name."""
        return dict(self._procedures)

    def register(self, proc: ProcedureDef) -> ProcedureDef:
        """Register a ``ProcedureDef``."""
        self._procedures[proc.name] = proc
        return proc

    async def call(self, name: str, payload: Any = None) -> Any:
        """Invoke a registered procedure by ``name``."""
        if name not in self._procedures:
            msg = f"Unknown RPC procedure: {name!r}"
            raise KeyError(msg)
        proc = self._procedures[name]
        res = proc.fn() if payload is None else proc.fn(payload)
        if inspect.isawaitable(res):
            res = await res
        return res


def query(
    *,
    name: str | None = None,
    input_type: type[Any] | None = None,
    output_type: type[Any] | None = None,
) -> Callable[[Callable[..., Any]], ProcedureDef]:
    """Decorator registering a read-only ``query`` RPC procedure."""

    def _decorator(fn: Callable[..., Any]) -> ProcedureDef:
        return ProcedureDef(
            name=name or fn.__name__,
            kind="query",
            fn=fn,
            input_type=input_type,
            output_type=output_type,
        )

    return _decorator


def mutation(
    *,
    name: str | None = None,
    input_type: type[Any] | None = None,
    output_type: type[Any] | None = None,
) -> Callable[[Callable[..., Any]], ProcedureDef]:
    """Decorator registering a state-mutating ``mutation`` RPC procedure."""

    def _decorator(fn: Callable[..., Any]) -> ProcedureDef:
        return ProcedureDef(
            name=name or fn.__name__,
            kind="mutation",
            fn=fn,
            input_type=input_type,
            output_type=output_type,
        )

    return _decorator


def subscription(
    *,
    name: str | None = None,
    input_type: type[Any] | None = None,
    output_type: type[Any] | None = None,
) -> Callable[[Callable[..., Any]], ProcedureDef]:
    """Decorator registering a streaming ``subscription`` RPC procedure."""

    def _decorator(fn: Callable[..., Any]) -> ProcedureDef:
        return ProcedureDef(
            name=name or fn.__name__,
            kind="subscription",
            fn=fn,
            input_type=input_type,
            output_type=output_type,
        )

    return _decorator
