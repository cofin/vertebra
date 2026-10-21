"""PostgreSQL Change Data Capture (CDC): LISTEN/NOTIFY spill bridge and zero-bloat logical replication."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import msgspec

__all__ = (
    "CdcConfig",
    "ChangeEvent",
    "ListenNotifySpillBridge",
    "build_logical_emit_message_sql",
)


class ChangeEvent(msgspec.Struct, frozen=True):
    """Change-feed event emitted via ``LISTEN``/``NOTIFY`` or ``pg_logical_emit_message``."""

    event_id: int
    channel: str
    operation: str
    payload: dict[str, Any]
    spilled: bool = False


@dataclass(frozen=True, slots=True)
class CdcConfig:
    """Configuration for PostgreSQL change-feed streaming and WAL slot safety."""

    channel: str = "vertebra_events"
    spill_threshold_bytes: int = 7500
    spill_table: str = "vertebra_notify_spill"
    replication_slot_name: str = "vertebra_cdc_slot"
    publication_name: str = "vertebra_pub"
    max_slot_wal_keep_size_mb: int = 4096


def build_logical_emit_message_sql(
    prefix: str,
    payload: bytes | str,
    *,
    transactional: bool = True,
) -> tuple[str, tuple[bool, str, bytes]]:
    """Build a zero-table-bloat ``pg_logical_emit_message`` SQL statement and bind tuple."""
    raw = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
    return "SELECT pg_logical_emit_message($1, $2, $3)", (transactional, prefix, raw)


class ListenNotifySpillBridge:
    """Manages PostgreSQL ``NOTIFY`` payloads with automatic ``>7,500``-byte spill table hydration."""

    def __init__(self, config: CdcConfig | None = None) -> None:
        self._config = config or CdcConfig()
        self._spill_store: dict[int, dict[str, Any]] = {}
        self._history: list[ChangeEvent] = []
        self._next_id: int = 1

    @property
    def config(self) -> CdcConfig:
        """Return the ``CdcConfig``."""
        return self._config

    def publish_event(self, operation: str, payload: dict[str, Any]) -> tuple[str, ChangeEvent]:
        """Encode an event for ``pg_notify``, spilling to ``_spill_store`` when exceeding 7,500 bytes."""
        event_id = self._next_id
        self._next_id += 1
        encoded = msgspec.json.encode(payload)
        spilled = len(encoded) > self._config.spill_threshold_bytes
        if spilled:
            self._spill_store[event_id] = payload
            wire_envelope = msgspec.json.encode(
                {
                    "event_id": event_id,
                    "channel": self._config.channel,
                    "operation": operation,
                    "spilled": True,
                }
            ).decode("utf-8")
        else:
            wire_envelope = msgspec.json.encode(
                {
                    "event_id": event_id,
                    "channel": self._config.channel,
                    "operation": operation,
                    "spilled": False,
                    "payload": payload,
                }
            ).decode("utf-8")
        evt = ChangeEvent(
            event_id=event_id,
            channel=self._config.channel,
            operation=operation,
            payload=payload,
            spilled=spilled,
        )
        self._history.append(evt)
        return wire_envelope, evt

    def hydrate_notification(self, wire_envelope: str | bytes) -> ChangeEvent:
        """Decode a ``NOTIFY`` wire envelope, hydrating spilled payloads by ``event_id``."""
        raw = wire_envelope.encode("utf-8") if isinstance(wire_envelope, str) else wire_envelope
        decoded = msgspec.json.decode(raw)
        event_id = int(decoded["event_id"])
        spilled = bool(decoded.get("spilled", False))
        payload = self._spill_store.get(event_id, {}) if spilled else dict(decoded.get("payload") or {})
        return ChangeEvent(
            event_id=event_id,
            channel=str(decoded["channel"]),
            operation=str(decoded["operation"]),
            payload=payload,
            spilled=spilled,
        )

    def replay_since(self, last_event_id: int) -> list[ChangeEvent]:
        """Return all buffered ``ChangeEvent`` items with ``event_id > last_event_id``."""
        return [evt for evt in self._history if evt.event_id > last_event_id]
