"""Procedures package."""

from __future__ import annotations

from vertebra.core.procedures.decorator import procedure
from vertebra.core.procedures.registry import ProcedureMetadata, ProcedureRegistry
from vertebra.core.procedures.serialization import (
    MsgspecSerializer,
    PydanticSerializer,
    Serializer,
    detect_serializer,
    get_serializer,
    register_serializer,
)

__all__ = (
    "MsgspecSerializer",
    "ProcedureMetadata",
    "ProcedureRegistry",
    "PydanticSerializer",
    "Serializer",
    "detect_serializer",
    "get_serializer",
    "procedure",
    "register_serializer",
)
