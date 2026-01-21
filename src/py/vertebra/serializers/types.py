"""Shared type definitions for Vertebra serializers."""

from __future__ import annotations

from typing import Any, Protocol

__all__ = ("JSONSerializer",)


class JSONSerializer(Protocol):
    """Protocol for JSON serialization implementations."""

    def encode(self, data: Any, *, as_bytes: bool = False) -> str | bytes:
        """Encode data to JSON."""
        ...

    def decode(self, data: str | bytes) -> Any:
        """Decode from JSON."""
        ...
