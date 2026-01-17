"""Procedure registry for Vertebra type bridge."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from threading import RLock
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from pathlib import Path

    from vertebra.procedures.serialization import Serializer
    from vertebra.types.ir import TypeNode


@dataclass(slots=True)
class ProcedureMetadata:
    """Metadata captured for a registered procedure."""

    procedure_id: str
    handler_fn: Callable[..., Any]
    source_file: Path
    params: Mapping[str, TypeNode]
    return_type: TypeNode
    serializer: str | Serializer
    metadata: Mapping[str, Any] = field(default_factory=dict)
    route: str | None = None
    method: str | None = None
    last_updated: datetime = field(default_factory=lambda: datetime.now(tz=UTC))


class ProcedureRegistry:
    """Thread-safe registry of procedures."""

    _instance: "ProcedureRegistry | None" = None
    _instance_lock = RLock()

    def __init__(self) -> None:
        self._lock = RLock()
        self._procedures: dict[str, ProcedureMetadata] = {}

    @classmethod
    def instance(cls) -> "ProcedureRegistry":
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def register(self, metadata: ProcedureMetadata) -> None:
        with self._lock:
            self._procedures[metadata.procedure_id] = metadata

    def update(self, procedure_id: str, **updates: Any) -> ProcedureMetadata | None:
        with self._lock:
            current = self._procedures.get(procedure_id)
            if current is None:
                return None
            updated = replace(current, **updates, last_updated=datetime.now(tz=UTC))
            self._procedures[procedure_id] = updated
            return updated

    def unregister(self, procedure_id: str) -> ProcedureMetadata | None:
        with self._lock:
            return self._procedures.pop(procedure_id, None)

    def get(self, procedure_id: str) -> ProcedureMetadata | None:
        with self._lock:
            return self._procedures.get(procedure_id)

    def list(self) -> list[ProcedureMetadata]:
        with self._lock:
            return list(self._procedures.values())

    def clear(self) -> None:
        with self._lock:
            self._procedures.clear()
