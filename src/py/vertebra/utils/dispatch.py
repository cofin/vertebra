"""O(1) MRO-cached type dispatch dictionary for serializers, parameter binders, and type converters."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Final, cast

__all__ = ("TypeDispatcher",)

_MISS: Final[object] = object()


class TypeDispatcher[T]:
    """Dictionary-like type-to-value dispatcher with O(1) cached MRO fallback lookup."""

    __slots__ = ("_cache", "_exact_map")

    def __init__(self) -> None:
        self._exact_map: dict[type[Any], T] = {}
        self._cache: dict[type[Any], T | object] = {}

    def register(self, target_type: type[Any], handler: T) -> None:
        """Register ``handler`` for ``target_type`` and invalidate cached MRO lookups."""
        self._exact_map[target_type] = handler
        self._cache.clear()

    def register_all(self, mappings: Iterable[tuple[type[Any], T]]) -> None:
        """Register multiple ``(target_type, handler)`` pairs and invalidate cached MRO lookups."""
        for target_type, handler in mappings:
            self._exact_map[target_type] = handler
        self._cache.clear()

    def get(self, obj_or_type: Any) -> T | None:
        """Resolve the handler for a type or instance using exact match followed by cached MRO lookup."""
        target_type: type[Any] = obj_or_type if isinstance(obj_or_type, type) else type(obj_or_type)
        exact = self._exact_map.get(target_type)
        if exact is not None:
            return exact

        cached = self._cache.get(target_type, _MISS)
        if cached is not _MISS:
            return None if cached is None else cast("T", cached)

        for base in getattr(target_type, "__mro__", ())[1:]:
            if base in self._exact_map:
                resolved = self._exact_map[base]
                self._cache[target_type] = resolved
                return resolved

        self._cache[target_type] = None
        return None

    def clear_cache(self) -> None:
        """Clear the internal MRO resolution cache while retaining registered types."""
        self._cache.clear()

    def __getitem__(self, obj_or_type: Any) -> T:
        """Return the handler for ``obj_or_type`` or raise ``KeyError``."""
        resolved = self.get(obj_or_type)
        if resolved is None:
            raise KeyError(obj_or_type)
        return resolved

    def __contains__(self, obj_or_type: object) -> bool:
        """Return True if ``obj_or_type`` matches a registered type directly or via MRO."""
        return self.get(obj_or_type) is not None
