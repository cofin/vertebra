"""Dependency injection primitives."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, Generic, Literal, Protocol, TypeVar, runtime_checkable

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

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.factory(*args, **kwargs)


__all__ = (
    "Inject",
    "Provider",
    "ProviderFactory",
    "ProviderSpec",
    "SCOPES",
    "Scope",
)
