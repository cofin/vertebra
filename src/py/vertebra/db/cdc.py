"""Database Change Data Capture (CDC): PostgreSQL LISTEN/NOTIFY spill feed and Cloud Spanner Change Streams."""

from __future__ import annotations

import collections
import inspect
from collections.abc import AsyncIterator, Iterable, Iterator, Mapping
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal

import msgspec

from vertebra.typing import FrozenBaseStruct
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import decode_json, encode_json, encode_json_bytes
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from vertebra.storage import PutResult, SyncObjectStore

__all__ = (
    "AsyncSpannerChangeStreamSource",
    "CDCConfig",
    "ChangeEvent",
    "ChangeFeed",
    "SpannerChangeStreamRecord",
    "SpannerChildPartition",
    "SyncSpannerChangeStreamSource",
)


class ChangeEvent(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Change-feed event emitted via ``LISTEN``/``NOTIFY``, logical WAL, or Spanner Change Streams."""

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


class CDCConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for database change-feed streaming, spill hydration, and WAL slot safety."""

    channel: str = "vertebra_events"
    """Logical channel name for emitted CDC events."""

    spill_threshold_bytes: int = 7500
    """Maximum inline payload size in bytes before spilling to ``ObjectStore``."""

    spill_table: str = "vertebra_notify_spill"
    """Fallback relational spill table name."""

    spill_prefix: str = "cdc/spill"
    """Object key prefix in ``ObjectStore`` for spilled CDC payloads."""

    max_history_size: int = 1024
    """Maximum number of events retained in the in-memory replay buffer."""

    replication_slot_name: str = "vertebra_cdc_slot"
    """PostgreSQL logical replication slot identifier."""

    publication_name: str = "vertebra_pub"
    """PostgreSQL publication identifier."""

    max_slot_wal_keep_size_mb: int = 4096
    """Maximum WAL retention size in megabytes for logical replication slots."""


class ChangeFeed:
    """Manages PostgreSQL ``NOTIFY`` payloads, ``>7,500``-byte spill hydration, and logical WAL emission."""

    __slots__ = ("_config", "_history", "_listeners", "_next_id", "_spill_store", "_store")

    def __init__(
        self,
        config: CDCConfig | None = None,
        *,
        store: SyncObjectStore | None = None,
    ) -> None:
        self._config = config or CDCConfig()
        self._store = store
        self._spill_store: dict[int, dict[str, Any]] = {}
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


class SpannerChildPartition(FrozenBaseStruct, frozen=True, gc=False):
    """Child partition descriptor emitted by a Cloud Spanner Change Stream ``ChildPartitionsRecord``."""

    token: str
    """Child partition token string returned by Spanner Change Streams."""

    parent_partition_tokens: tuple[str, ...] = ()
    """Parent partition tokens that must complete before polling this child partition."""

    start_timestamp: str = ""
    """ISO-8601 start timestamp from which the child partition should be queried."""

    record_sequence: str = ""
    """Record sequence string ordering the split or merge record."""


class SpannerChangeStreamRecord(FrozenBaseStruct, frozen=True, gc=False):
    """Typed Cloud Spanner Change Stream record covering data changes, heartbeats, and child partitions."""

    record_type: Literal["data_change", "heartbeat", "child_partitions"]
    """Change stream record variant discriminator."""

    commit_timestamp: str = ""
    """Commit timestamp of the ``DataChangeRecord``."""

    record_sequence: str = ""
    """Monotonic sequence string within the transaction or partition."""

    server_transaction_id: str = ""
    """Spanner server transaction identifier."""

    table_name: str = ""
    """Tracked table name for ``DataChangeRecord`` events."""

    mod_type: str = ""
    """Modification operation type (``INSERT``, ``UPDATE``, or ``DELETE``)."""

    mods: tuple[dict[str, Any], ...] = ()
    """Row modification objects with ``keys``, ``new_values``, and ``old_values``."""

    value_capture_type: str = "OLD_AND_NEW_VALUES"
    """Change stream value capture mode."""

    partition_token: str | None = None
    """Partition token from which this record was read."""

    watermark_timestamp: str = ""
    """Watermark timestamp reported by a ``HeartbeatRecord`` or ``ChildPartitionsRecord``."""

    child_partitions: tuple[SpannerChildPartition, ...] = ()
    """Child partitions emitted during split or merge topology changes."""


class _BaseSpannerChangeStreamSource:
    """Cloud Spanner Change Stream reader base with dynamic child-partition scheduling and ``ObjectStore`` spill."""

    __slots__ = (
        "_active_partitions",
        "_change_feed",
        "_completed_partitions",
        "_dialect",
        "_driver",
        "_heartbeat_milliseconds",
        "_partition_start_timestamps",
        "_pending_children",
        "_stream_name",
        "_watermarks",
    )

    def __init__(
        self,
        driver: Any,
        stream_name: str,
        *,
        dialect: Literal["googlesql", "spangres"] | None = None,
        change_feed: ChangeFeed | None = None,
        store: SyncObjectStore | None = None,
        channel: str = "spanner_cdc",
        heartbeat_milliseconds: int = 10_000,
        spill_threshold_bytes: int = 7500,
    ) -> None:
        """Initialize the Spanner Change Stream source.

        Args:
            driver: Sync or async Spanner driver instance.
            stream_name: Spanner change stream identifier.
            dialect: SQL dialect (``"googlesql"`` or ``"spangres"``).
            change_feed: Optional ``ChangeFeed`` destination.
            store: Optional ``ObjectStore`` for spilling oversized change events.
            channel: Logical channel name for emitted ``ChangeEvent`` frames.
            heartbeat_milliseconds: Heartbeat interval in milliseconds for TVF queries.
            spill_threshold_bytes: Inline payload size threshold before spilling to ``store``.
        """
        inferred_dialect = (
            dialect
            or getattr(driver, "dialect", None)
            or getattr(getattr(driver, "config", None), "dialect", "googlesql")
        )
        self._driver = driver
        self._stream_name = validate_identifier(stream_name, "stream_name")
        self._dialect: Literal["googlesql", "spangres"] = (
            "spangres" if str(inferred_dialect).lower() in {"spangres", "postgres", "postgresql"} else "googlesql"
        )
        self._heartbeat_milliseconds = heartbeat_milliseconds
        self._change_feed = change_feed or ChangeFeed(
            CDCConfig(
                channel=channel,
                spill_threshold_bytes=spill_threshold_bytes,
                spill_prefix=f"cdc/spanner/{stream_name}",
            ),
            store=store,
        )
        self._active_partitions: list[str] = []
        self._partition_start_timestamps: dict[str, str] = {}
        self._completed_partitions: set[str] = set()
        self._pending_children: list[SpannerChildPartition] = []
        self._watermarks: dict[str, str] = {}

    @property
    def stream_name(self) -> str:
        """Return the Spanner change stream name."""
        return self._stream_name

    @property
    def dialect(self) -> Literal["googlesql", "spangres"]:
        """Return the active Spanner SQL dialect."""
        return self._dialect

    @property
    def change_feed(self) -> ChangeFeed:
        """Return the ``ChangeFeed`` receiving published change events."""
        return self._change_feed

    @property
    def active_partitions(self) -> tuple[str, ...]:
        """Return currently scheduled active partition tokens."""
        return tuple(self._active_partitions)

    @property
    def completed_partitions(self) -> frozenset[str]:
        """Return partition tokens that have completed streaming."""
        return frozenset(self._completed_partitions)

    @property
    def watermarks(self) -> dict[str, str]:
        """Return the latest watermark timestamp observed per partition token."""
        return dict(self._watermarks)

    def build_read_query(self, dialect: Literal["googlesql", "spangres"] | None = None) -> str:
        """Construct the Spanner TVF read query for GoogleSQL or Spangres."""
        target = dialect or self._dialect
        if target == "spangres":
            return f"SELECT * FROM spanner.read_json_{self._stream_name}($1, $2, $3, $4, NULL)"
        return (
            f"SELECT * FROM READ_{self._stream_name}("
            "start_timestamp => @start_ts, "
            "end_timestamp => @end_ts, "
            "partition_token => @token, "
            "heartbeat_milliseconds => @heartbeat_ms)"
        )

    def schedule_partition(self, token: str, start_timestamp: str = "") -> None:
        """Schedule a partition token for change stream polling."""
        if token not in self._completed_partitions and token not in self._active_partitions:
            self._active_partitions.append(token)
        if start_timestamp:
            self._partition_start_timestamps[token] = start_timestamp

    def complete_partition(self, token: str) -> list[str]:
        """Mark a parent partition token completed and promote any ready child partitions."""
        if token in self._active_partitions:
            self._active_partitions.remove(token)
        self._completed_partitions.add(token)
        promoted: list[str] = []
        remaining: list[SpannerChildPartition] = []
        for child in self._pending_children:
            parents_done = all(p in self._completed_partitions for p in child.parent_partition_tokens)
            if parents_done:
                self.schedule_partition(child.token, child.start_timestamp)
                promoted.append(child.token)
            else:
                remaining.append(child)
        self._pending_children = remaining
        return promoted

    def parse_raw_row(
        self,
        row: Mapping[str, Any] | str,
        *,
        partition_token: str | None = None,
    ) -> list[SpannerChangeStreamRecord]:
        """Parse a raw GoogleSQL TVF row or Spangres JSON row into ``SpannerChangeStreamRecord`` items."""
        if isinstance(row, str):
            decoded = decode_json(row)
            if isinstance(decoded, dict):
                return self._parse_mapping(decoded, partition_token=partition_token)
            return []

        if "ChangeRecord" in row and isinstance(row["ChangeRecord"], list):
            out: list[SpannerChangeStreamRecord] = []
            for item in row["ChangeRecord"]:
                if isinstance(item, Mapping):
                    out.extend(self._parse_mapping(item, partition_token=partition_token))
            return out

        for col_val in row.values():
            if isinstance(col_val, str) and col_val.lstrip().startswith("{"):
                try:
                    parsed_json = decode_json(col_val)
                    if isinstance(parsed_json, dict):
                        return self._parse_mapping(parsed_json, partition_token=partition_token)
                except Exception:
                    pass

        return self._parse_mapping(row, partition_token=partition_token)

    def ingest_records(
        self,
        records: Iterable[SpannerChangeStreamRecord],
        *,
        partition_token: str | None = None,
    ) -> list[ChangeEvent]:
        """Process parsed change stream records, updating watermarks, scheduling children, and publishing ``ChangeEvent`` items."""
        emitted: list[ChangeEvent] = []
        for rec in records:
            effective_token = rec.partition_token if rec.partition_token is not None else partition_token
            if rec.record_type == "data_change":
                payload = {
                    "stream_name": self._stream_name,
                    "table_name": rec.table_name,
                    "mod_type": rec.mod_type,
                    "commit_timestamp": rec.commit_timestamp,
                    "record_sequence": rec.record_sequence,
                    "server_transaction_id": rec.server_transaction_id,
                    "value_capture_type": rec.value_capture_type,
                    "partition_token": effective_token,
                    "mods": list(rec.mods),
                }
                _, evt = self._change_feed.publish_event(rec.mod_type or "CHANGE", payload)
                emitted.append(evt)
                if effective_token and rec.commit_timestamp:
                    self._watermarks[effective_token] = rec.commit_timestamp
            elif rec.record_type == "heartbeat":
                if effective_token and rec.watermark_timestamp:
                    self._watermarks[effective_token] = rec.watermark_timestamp
            elif rec.record_type == "child_partitions":
                if effective_token and rec.watermark_timestamp:
                    self._watermarks[effective_token] = rec.watermark_timestamp
                for child in rec.child_partitions:
                    parents_done = all(p in self._completed_partitions for p in child.parent_partition_tokens)
                    if not child.parent_partition_tokens or parents_done:
                        self.schedule_partition(child.token, child.start_timestamp)
                    else:
                        self._pending_children.append(child)
        return emitted

    def _parse_mapping(
        self,
        data: Mapping[str, Any],
        *,
        partition_token: str | None = None,
    ) -> list[SpannerChangeStreamRecord]:
        records: list[SpannerChangeStreamRecord] = []

        data_changes = data.get("data_change_record") or data.get("DataChangeRecord")
        if data_changes is not None:
            items = data_changes if isinstance(data_changes, list) else [data_changes]
            for dc in items:
                if not isinstance(dc, Mapping):
                    continue
                raw_mods = dc.get("mods") or ()
                mods_tuple = tuple(dict(m) for m in raw_mods if isinstance(m, Mapping))
                records.append(
                    SpannerChangeStreamRecord(
                        record_type="data_change",
                        commit_timestamp=str(dc.get("commit_timestamp") or ""),
                        record_sequence=str(dc.get("record_sequence") or ""),
                        server_transaction_id=str(dc.get("server_transaction_id") or ""),
                        table_name=str(dc.get("table_name") or dc.get("row_type") or ""),
                        mod_type=str(dc.get("mod_type") or "CHANGE"),
                        mods=mods_tuple,
                        value_capture_type=str(dc.get("value_capture_type") or "OLD_AND_NEW_VALUES"),
                        partition_token=partition_token,
                    )
                )

        heartbeats = data.get("heartbeat_record") or data.get("HeartbeatRecord")
        if heartbeats is not None:
            items = heartbeats if isinstance(heartbeats, list) else [heartbeats]
            for hb in items:
                if not isinstance(hb, Mapping):
                    continue
                ts = str(hb.get("timestamp") or hb.get("watermark_timestamp") or "")
                if ts:
                    records.append(
                        SpannerChangeStreamRecord(
                            record_type="heartbeat",
                            watermark_timestamp=ts,
                            partition_token=partition_token,
                        )
                    )

        child_records = data.get("child_partitions_record") or data.get("ChildPartitionsRecord")
        if child_records is not None:
            items = child_records if isinstance(child_records, list) else [child_records]
            for cp_rec in items:
                if not isinstance(cp_rec, Mapping):
                    continue
                start_ts = str(cp_rec.get("start_timestamp") or "")
                seq = str(cp_rec.get("record_sequence") or "")
                raw_children = cp_rec.get("child_partitions") or ()
                children: list[SpannerChildPartition] = []
                for child in raw_children:
                    if not isinstance(child, Mapping):
                        continue
                    token = str(child.get("token") or "")
                    parents = tuple(str(p) for p in (child.get("parent_partition_tokens") or ()) if p)
                    if token:
                        children.append(
                            SpannerChildPartition(
                                token=token,
                                parent_partition_tokens=parents,
                                start_timestamp=start_ts,
                                record_sequence=seq,
                            )
                        )
                records.append(
                    SpannerChangeStreamRecord(
                        record_type="child_partitions",
                        watermark_timestamp=start_ts,
                        record_sequence=seq,
                        partition_token=partition_token,
                        child_partitions=tuple(children),
                    )
                )

        if not records and "table_name" in data and "mods" in data:
            raw_mods = data.get("mods") or ()
            mods_tuple = tuple(dict(m) for m in raw_mods if isinstance(m, Mapping))
            records.append(
                SpannerChangeStreamRecord(
                    record_type="data_change",
                    commit_timestamp=str(data.get("commit_timestamp") or ""),
                    record_sequence=str(data.get("record_sequence") or ""),
                    server_transaction_id=str(data.get("server_transaction_id") or ""),
                    table_name=str(data.get("table_name") or ""),
                    mod_type=str(data.get("mod_type") or "CHANGE"),
                    mods=mods_tuple,
                    value_capture_type=str(data.get("value_capture_type") or "OLD_AND_NEW_VALUES"),
                    partition_token=partition_token,
                )
            )

        return records

    def _build_read_params(
        self,
        partition_token: str | None,
        start_timestamp: str,
        end_timestamp: str | None,
    ) -> dict[str, Any] | list[Any]:
        """Return change stream TVF bind parameters for the active dialect.

        Spangres binds the positional ``$1..$4`` placeholders, which the Spanner driver maps to
        ``p1..p4``. GoogleSQL binds the named ``@start_ts``, ``@end_ts``, ``@token``, and
        ``@heartbeat_ms`` placeholders.
        """
        if self._dialect == "spangres":
            return [start_timestamp, end_timestamp, partition_token, self._heartbeat_milliseconds]
        return {
            "start_ts": start_timestamp,
            "end_ts": end_timestamp,
            "token": partition_token,
            "heartbeat_ms": self._heartbeat_milliseconds,
        }


class SyncSpannerChangeStreamSource(_BaseSpannerChangeStreamSource):
    """Synchronous Cloud Spanner Change Stream reader with dynamic child-partition scheduling."""

    def poll_partition(
        self,
        partition_token: str | None,
        *,
        start_timestamp: str,
        end_timestamp: str | None = None,
    ) -> list[ChangeEvent]:
        """Synchronously query a single partition token and publish resulting ``ChangeEvent`` items."""
        sql = self.build_read_query()
        params = self._build_read_params(partition_token, start_timestamp, end_timestamp)
        rows = self._driver.select(sql, params)
        parsed: list[SpannerChangeStreamRecord] = []
        for row in rows:
            parsed.extend(self.parse_raw_row(row, partition_token=partition_token))
        events = self.ingest_records(parsed, partition_token=partition_token)
        if partition_token is not None:
            self.complete_partition(partition_token)
        return events

    def stream(
        self,
        *,
        start_timestamp: str | None = None,
        end_timestamp: str | None = None,
        initial_partitions: Iterable[str] = ("",),
    ) -> Iterator[ChangeEvent]:
        """Synchronously stream change events across active and child partitions."""
        resolved_start = (
            start_timestamp if start_timestamp is not None else datetime.now(UTC).isoformat().replace("+00:00", "Z")
        )
        for token in initial_partitions:
            self.schedule_partition(token, resolved_start)
        while self._active_partitions:
            token = self._active_partitions[0]
            start_ts = self._partition_start_timestamps.get(token, resolved_start)
            events = self.poll_partition(token, start_timestamp=start_ts, end_timestamp=end_timestamp)
            yield from events


class AsyncSpannerChangeStreamSource(_BaseSpannerChangeStreamSource):
    """Asynchronous Cloud Spanner Change Stream reader with dynamic child-partition scheduling."""

    async def poll_partition(
        self,
        partition_token: str | None,
        *,
        start_timestamp: str,
        end_timestamp: str | None = None,
    ) -> list[ChangeEvent]:
        """Asynchronously query a single partition token and publish resulting ``ChangeEvent`` items."""
        sql = self.build_read_query()
        params = self._build_read_params(partition_token, start_timestamp, end_timestamp)
        res = self._driver.select(sql, params)
        rows = await res if inspect.isawaitable(res) else res
        parsed: list[SpannerChangeStreamRecord] = []
        for row in rows:
            parsed.extend(self.parse_raw_row(row, partition_token=partition_token))
        events = self.ingest_records(parsed, partition_token=partition_token)
        if partition_token is not None:
            self.complete_partition(partition_token)
        return events

    async def stream(
        self,
        *,
        start_timestamp: str | None = None,
        end_timestamp: str | None = None,
        initial_partitions: Iterable[str] = ("",),
    ) -> AsyncIterator[ChangeEvent]:
        """Asynchronously stream change events across active and child partitions."""
        resolved_start = (
            start_timestamp if start_timestamp is not None else datetime.now(UTC).isoformat().replace("+00:00", "Z")
        )
        for token in initial_partitions:
            self.schedule_partition(token, resolved_start)
        while self._active_partitions:
            token = self._active_partitions[0]
            start_ts = self._partition_start_timestamps.get(token, resolved_start)
            events = await self.poll_partition(token, start_timestamp=start_ts, end_timestamp=end_timestamp)
            for evt in events:
                yield evt
