"""Engine-neutral change-feed envelopes, bounded spill hydration, and replay."""

from __future__ import annotations

import collections
from typing import TYPE_CHECKING, Any

from vertebra.typing import FrozenBaseStruct
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import decode_json, encode_json, encode_json_bytes

if TYPE_CHECKING:
    from vertebra.storage import PutResult, SyncObjectStore

__all__ = (
    "CDCConfig",
    "ChangeEvent",
    "ChangeFeed",
)


class ChangeEvent(FrozenBaseStruct, frozen=True, gc=False, kw_only=True):
    """Immutable change-feed envelope carrying a decoded event payload."""

    event_id: int
    """Monotonically increasing event identifier."""

    channel: str
    """Logical CDC channel name."""

    operation: str
    """DML or notification operation type."""

    payload: dict[str, Any]
    """Decoded event payload dictionary."""

    spilled: bool = False
    """Whether the wire payload exceeded ``spill_threshold_bytes`` and spilled to ``ObjectStore``."""


class CDCConfig(FrozenBaseStruct, frozen=True, gc=False, kw_only=True):
    """Engine-neutral change-feed history and spill hydration configuration."""

    channel: str = "vertebra_events"
    """Logical channel name for emitted CDC events."""

    spill_threshold_bytes: int = 7500
    """Maximum inline payload size in bytes before spilling to storage or bounded memory."""

    spill_prefix: str = "cdc/spill"
    """Object key prefix for spilled CDC payloads."""

    max_history_size: int = 1024
    """Maximum number of events retained in the in-memory replay buffer."""

    max_memory_spills: int = 1024
    """Maximum number of oversized payloads retained in the in-memory LRU cache."""

    def __post_init__(self) -> None:
        if self.max_memory_spills < 1:
            msg = "max_memory_spills must be at least 1"
            raise ValueError(msg)


class ChangeFeed:
    """Publish change-feed envelopes with bounded history and spill storage."""

    __slots__ = ("_config", "_history", "_listeners", "_next_id", "_spill_store", "_store")

    def __init__(
        self,
        config: CDCConfig | None = None,
        *,
        store: SyncObjectStore | None = None,
    ) -> None:
        self._config = config or CDCConfig()
        self._store = store
        self._spill_store: collections.OrderedDict[int, dict[str, Any]] = collections.OrderedDict()
        self._history: collections.deque[ChangeEvent] = collections.deque(maxlen=self._config.max_history_size)
        self._listeners: list[Any] = []
        self._next_id: int = 1

    @property
    def config(self) -> CDCConfig:
        """Return the ``CDCConfig``."""
        return self._config

    @property
    def store(self) -> SyncObjectStore | None:
        """Return the attached SyncObjectStore when configured."""
        return self._store

    def add_listener(self, callback: Any) -> None:
        """Register a listener callback invoked with each published ChangeEvent."""
        self._listeners.append(callback)

    def publish_event(self, operation: str, payload: dict[str, Any]) -> tuple[str, ChangeEvent]:
        """Encode an event, spilling payloads that exceed the inline byte threshold."""
        event_id = self._next_id
        self._next_id += 1
        encoded = encode_json_bytes(payload)
        spilled = len(encoded) > self._config.spill_threshold_bytes
        if spilled:
            prefix = self._config.spill_prefix.rstrip("/")
            spill_key = f"{prefix}/{event_id}.json" if prefix else f"{event_id}.json"
            if self._store is not None:
                self._store.put(spill_key, encoded)
            else:
                self._spill_store[event_id] = payload
                if len(self._spill_store) > self._config.max_memory_spills:
                    self._spill_store.popitem(last=False)
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
        """Decode an envelope, hydrating spills and propagating backing-store failures."""
        decoded = decode_json(wire_envelope)
        event_id = int(decoded["event_id"])
        spilled = bool(decoded.get("spilled", False))
        if spilled:
            if self._store is not None:
                prefix = self._config.spill_prefix.rstrip("/")
                default_key = f"{prefix}/{event_id}.json" if prefix else f"{event_id}.json"
                spill_key = str(decoded.get("spill_key") or default_key)
                payload = dict(decode_json(self._store.get(spill_key)))
            else:
                if event_id not in self._spill_store:
                    msg = f"Evicted or unknown in-memory CDC spill id: {event_id}"
                    raise KeyError(msg)
                payload = self._spill_store[event_id]
                self._spill_store.move_to_end(event_id)
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
