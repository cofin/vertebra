"""Vertebra core package."""

from __future__ import annotations

from vertebra.procedures import (
    MsgspecSerializer,
    PydanticSerializer,
    Serializer,
    detect_serializer,
    get_serializer,
    procedure,
    register_serializer,
)

__all__ = (
    "MsgspecSerializer",
    "PydanticSerializer",
    "Serializer",
    "__version__",
    "detect_serializer",
    "get_serializer",
    "procedure",
    "register_serializer",
)

__version__ = "0.1.0"
