"""Typed exception hierarchy for shared-memory broker attachment and lease errors."""

from __future__ import annotations

from vertebra.typing import VertebraError

__all__ = (
    "BrokerAttachError",
    "BrokerAuthenticationError",
    "BrokerSegmentInUseError",
    "LeaseQuarantinedError",
)


class BrokerAttachError(VertebraError):
    """Raised when attaching to or initializing a shared-memory broker segment fails."""


class BrokerSegmentInUseError(BrokerAttachError):
    """Raised when a live owner already holds a shared-memory segment with incompatible metadata."""

    segment_name: str | None
    owner_pid: int | None

    def __init__(
        self,
        segment_name: str | None = None,
        *,
        owner_pid: int | None = None,
        message: str | None = None,
    ) -> None:
        self.segment_name = segment_name
        self.owner_pid = owner_pid
        if message is not None:
            detail = message
        elif segment_name is not None and owner_pid is not None:
            detail = f"shared memory segment {segment_name!r} is in use by live owner pid {owner_pid}"
        elif segment_name is not None:
            detail = f"shared memory segment {segment_name!r} is already in use"
        elif owner_pid is not None:
            detail = f"shared memory segment is in use by live owner pid {owner_pid}"
        else:
            detail = "shared memory segment is already in use"
        super().__init__(detail)


class BrokerAuthenticationError(BrokerAttachError):
    """Raised when shared-memory broker handshake or credential verification fails."""


class LeaseQuarantinedError(VertebraError):
    """Raised when attempting to use or release a quarantined connection lease slot."""

    slot_index: int
    generation: int

    def __init__(
        self,
        slot_index: int,
        generation: int,
        message: str | None = None,
    ) -> None:
        self.slot_index = slot_index
        self.generation = generation
        detail = message or f"connection lease slot {slot_index} (generation {generation}) is quarantined"
        super().__init__(detail)
