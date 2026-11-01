"""PostgreSQL Change Data Capture (CDC): LISTEN/NOTIFY spill feed and zero-bloat logical replication."""

from __future__ import annotations

import collections
from typing import TYPE_CHECKING, Any

import msgspec

from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import decode_json, encode_json, encode_json_bytes

if TYPE_CHECKING:
    from vertebra.storage import ObjectStore, PutResult

__all__ = (
    "CDCConfig",
    "ChangeEvent",
    "ChangeFeed",
)


class ChangeEvent(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Change-feed event emitted via ``LISTEN``/``NOTIFY`` or ``pg_logical_emit_message``."""

    event_id: int
    channel: str
    operation: str
    payload: dict[str, Any]
    spilled: bool = False


class CDCConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for PostgreSQL change-feed streaming and WAL slot safety."""

    channel: str = "vertebra_events"
    spill_threshold_bytes: int = 7500
    spill_table: str = "vertebra_notify_spill"
    spill_prefix: str = "cdc/spill"
    max_history_size: int = 1024
    replication_slot_name: str = "vertebra_cdc_slot"
    publication_name: str = "vertebra_pub"
    max_slot_wal_keep_size_mb: int = 4096


class ChangeFeed:
    """Manages PostgreSQL ``NOTIFY`` payloads, ``>7,500``-byte spill hydration, and logical WAL emission."""

    __slots__ = ("_config", "_history", "_listeners", "_next_id", "_spill_store", "_store")

    def __init__(
        self,
        config: CDCConfig | None = None,
        *,
        store: ObjectStore | None = None,
    ) -> None:
        self._config = config or CDCConfig()
        self._store = store
        self._spill_store: dict[int, dict[str, Any]] = {}
        self._history: collections.deque[ChangeEvent] = collections.deque(maxlen=self._config.max_history_size)
        self._listeners: list[Any] = []
        self._next_id: int = 1

    def add_listener(self, callback: Any) -> None:
        """Register a listener callback invoked with each published ChangeEvent."""
        self._listeners.append(callback)

    @property
    def config(self) -> CDCConfig:
        """Return the ``CDCConfig``."""
        return self._config

    @property
    def store(self) -> ObjectStore | None:
        """Return the attached ObjectStore when configured."""
        return self._store

    @staticmethod
    def build_emit_message_sql(
        prefix: str,
        payload: bytes | str,
        *,
        transactional: bool = True,
    ) -> tuple[str, tuple[bool, str, bytes]]:
        """Build a zero-table-bloat ``pg_logical_emit_message`` SQL statement and bind tuple."""
        raw = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
        return "SELECT pg_logical_emit_message($1, $2, $3)", (transactional, prefix, raw)

    def publish_event(self, operation: str, payload: dict[str, Any]) -> tuple[str, ChangeEvent]:
        """Encode an event for ``pg_notify``, spilling to ``_spill_store`` when exceeding threshold."""
        event_id = self._next_id
        self._next_id += 1
        encoded = encode_json_bytes(payload)
        spilled = len(encoded) > self._config.spill_threshold_bytes
        if spilled:
            spill_key = f"{self._config.spill_prefix}/{event_id}.json"
            if self._store is not None:
                self._store.put(spill_key, encoded)
            else:
                self._spill_store[event_id] = payload
            wire_envelope = encode_json(
                {
                    "event_id": event_id,
                    "channel": self._config.channel,
                    "operation": operation,
                    "spilled": True,
                    "spill_key": spill_key,
                }
            )
        else:
            wire_envelope = encode_json(
                {
                    "event_id": event_id,
                    "channel": self._config.channel,
                    "operation": operation,
                    "spilled": False,
                    "payload": payload,
                }
            )
        evt = ChangeEvent(
            event_id=event_id,
            channel=self._config.channel,
            operation=operation,
            payload=payload,
            spilled=spilled,
        )
        self._history.append(evt)
        for listener in tuple(self._listeners):
            listener(evt)
        return wire_envelope, evt

    def hydrate_notification(self, wire_envelope: str | bytes) -> ChangeEvent:
        """Decode a ``NOTIFY`` wire envelope, hydrating spilled payloads from memory or ObjectStore."""
        decoded = decode_json(wire_envelope)
        event_id = int(decoded["event_id"])
        spilled = bool(decoded.get("spilled", False))
        if spilled:
            payload: dict[str, Any] | None = None
            if self._store is not None:
                spill_key = str(decoded.get("spill_key") or f"{self._config.spill_prefix}/{event_id}.json")
                try:
                    raw_bytes = self._store.get(spill_key)
                    payload = dict(decode_json(raw_bytes))
                except Exception:
                    payload = None
            if payload is None:
                payload = self._spill_store.get(event_id)
            if payload is None:
                msg = f"Spilled CDC payload for event_id={event_id} is missing"
                raise KeyError(msg)
        else:
            payload = dict(decoded.get("payload") or {})

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

    def flush_to_parquet(self, path: str) -> PutResult:
        """Flush in-memory history events to a PyArrow Parquet object in the attached ObjectStore."""
        if self._store is None:
            msg = "An ObjectStore must be configured on ChangeFeed to flush to Parquet"
            raise ValueError(msg)
        pa = ensure_module("pyarrow", package="pyarrow", extra="arrow")
        pq = ensure_module("pyarrow.parquet", package="pyarrow", extra="arrow")
        schema = pa.schema(
            [
                ("event_id", pa.int64()),
                ("channel", pa.string()),
                ("operation", pa.string()),
                ("spilled", pa.bool_()),
                ("payload", pa.string()),
            ]
        )
        records = [
            {
                "event_id": evt.event_id,
                "channel": evt.channel,
                "operation": evt.operation,
                "spilled": evt.spilled,
                "payload": encode_json(evt.payload),
            }
            for evt in self._history
        ]
        table = pa.Table.from_pylist(records, schema=schema)
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink)
        return self._store.put(path, sink.getvalue().to_pybytes())
