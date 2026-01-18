"""Bun-backed driver interfaces."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class DriverConfig:
    """Configuration for Bun-backed drivers."""

    name: str
    options: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class DriverBackend(Protocol):
    """Driver backend contract (implemented in PRD-4)."""

    def connect(self) -> None:
        """Establish a connection to the backend."""

    def close(self) -> None:
        """Close the backend connection."""


def get_driver_backend(config: DriverConfig) -> DriverBackend:
    """Return the configured driver backend."""
    raise RuntimeError(f"Driver backend '{config.name}' not configured yet")


__all__ = ["DriverBackend", "DriverConfig", "get_driver_backend"]
