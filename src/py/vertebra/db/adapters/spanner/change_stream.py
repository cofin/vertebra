"""Cloud Spanner change-stream readers, record parsing, and partition scheduling."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterable, Iterator, Mapping, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal, Protocol

from vertebra.db.cdc import CDCConfig, ChangeEvent, ChangeFeed
from vertebra.db.driver.capabilities import AsyncChangeSource, SyncChangeSource
from vertebra.typing import FrozenBaseStruct
from vertebra.utils.serializers import decode_json
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from vertebra.storage import SyncObjectStore

__all__ = (
    "AsyncSpannerChangeStreamSource",
    "SpannerChangeStreamRecord",
    "SpannerChildPartition",
    "SyncSpannerChangeStreamSource",
)


class _SyncSelectDriver(Protocol):
    """Synchronous row selection supported by Spanner drivers and snapshots."""

    def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> list[Any]: ...


class _AsyncSelectDriver(Protocol):
    """Asynchronous row selection supported by Spanner drivers and snapshots."""

    async def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> list[Any]: ...


class SpannerChildPartition(FrozenBaseStruct, frozen=True, gc=False, kw_only=True):
    """Child partition descriptor emitted by a Cloud Spanner Change Stream ``ChildPartitionsRecord``."""

    token: str
    """Child partition token string returned by Spanner Change Streams."""

    parent_partition_tokens: tuple[str, ...] = ()
    """Parent partition tokens that must complete before polling this child partition."""

    start_timestamp: str = ""
    """ISO-8601 start timestamp from which the child partition should be queried."""

    record_sequence: str = ""
    """Record sequence string ordering the split or merge record."""


class SpannerChangeStreamRecord(FrozenBaseStruct, frozen=True, gc=False, kw_only=True):
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


class _BaseSpannerChangeStreamSource[DriverT]:
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
        "_start_timestamp",
        "_end_timestamp",
        "_partition_token",
        "_watermarks",
    )

    def __init__(
        self,
        driver: DriverT,
        stream_name: str,
        *,
        dialect: Literal["googlesql", "spangres"] | None = None,
        change_feed: ChangeFeed | None = None,
        store: SyncObjectStore | None = None,
        channel: str = "spanner_cdc",
        heartbeat_milliseconds: int = 10_000,
        start_timestamp: str | None = None,
        end_timestamp: str | None = None,
        partition_token: str | None = None,
        spill_threshold_bytes: int = 7500,
    ) -> None:
        """Initialize the Spanner Change Stream source.

        Args:
            driver: Driver with the select contract required by the concrete reader.
            stream_name: Spanner change stream identifier.
            dialect: SQL dialect (``"googlesql"`` or ``"spangres"``).
            change_feed: Optional ``ChangeFeed`` destination.
            store: Optional ``ObjectStore`` for spilling oversized change events.
            channel: Logical channel name for emitted ``ChangeEvent`` frames.
            heartbeat_milliseconds: Heartbeat interval in milliseconds for TVF queries.
            start_timestamp: Default inclusive start timestamp for polling and streaming.
            end_timestamp: Default exclusive end timestamp for polling and streaming.
            partition_token: Default partition token, or None for the root query.
            spill_threshold_bytes: Inline payload size threshold before spilling to ``store``.
        """
        inferred_dialect = (
            dialect
            or getattr(driver, "dialect", None)
            or getattr(getattr(driver, "config", None), "dialect", "googlesql")
        )
        self._driver = driver
        self._start_timestamp = start_timestamp
        self._end_timestamp = end_timestamp
        self._partition_token = partition_token
        self._stream_name = validate_identifier(stream_name, "stream_name", allow_dots=False)
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
        self._completed_partitions.discard(token)
        if token not in self._active_partitions:
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
                if child.token not in self._completed_partitions:
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
            token_key = effective_token or ""
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
                if rec.commit_timestamp:
                    self._watermarks[token_key] = rec.commit_timestamp
                    self._partition_start_timestamps[token_key] = rec.commit_timestamp
            elif rec.record_type == "heartbeat":
                if rec.watermark_timestamp:
                    self._watermarks[token_key] = rec.watermark_timestamp
                    self._partition_start_timestamps[token_key] = rec.watermark_timestamp
            elif rec.record_type == "child_partitions":
                if rec.watermark_timestamp:
                    self._watermarks[token_key] = rec.watermark_timestamp
                    self._partition_start_timestamps[token_key] = rec.watermark_timestamp
                pending_tokens = {c.token for c in self._pending_children}
                for child in rec.child_partitions:
                    parents_done = all(p in self._completed_partitions for p in child.parent_partition_tokens)
                    if not child.parent_partition_tokens or parents_done:
                        if child.token not in self._completed_partitions:
                            self.schedule_partition(child.token, child.start_timestamp)
                    elif child.token not in pending_tokens:
                        self._pending_children.append(child)
                        pending_tokens.add(child.token)
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
        heartbeat_milliseconds: int | None = None,
    ) -> dict[str, Any] | list[Any]:
        """Return change stream TVF bind parameters for the active dialect.

        Spangres binds the positional ``$1..$4`` placeholders, which the Spanner driver maps to
        ``p1..p4``. GoogleSQL binds the named ``@start_ts``, ``@end_ts``, ``@token``, and
        ``@heartbeat_ms`` placeholders.
        """
        heartbeat = self._heartbeat_milliseconds if heartbeat_milliseconds is None else heartbeat_milliseconds
        if self._dialect == "spangres":
            return [start_timestamp, end_timestamp, partition_token, heartbeat]
        return {
            "start_ts": start_timestamp,
            "end_ts": end_timestamp,
            "token": partition_token,
            "heartbeat_ms": heartbeat,
        }

    def _resolve_start_timestamp(self, start_timestamp: str | None) -> str:
        return start_timestamp or self._start_timestamp or datetime.now(UTC).isoformat().replace("+00:00", "Z")

    def _publish_rows(self, rows: list[Any], partition_token: str | None) -> list[ChangeEvent]:
        parsed: list[SpannerChangeStreamRecord] = []
        for row in rows:
            parsed.extend(self.parse_raw_row(row, partition_token=partition_token))
        events = self.ingest_records(parsed, partition_token=partition_token)
        self.complete_partition(partition_token or "")
        return events


class SyncSpannerChangeStreamSource(_BaseSpannerChangeStreamSource[_SyncSelectDriver], SyncChangeSource):
    """Synchronous Cloud Spanner reader with constructor-bound stream defaults."""

    __slots__ = ()

    def poll_partition(
        self,
        partition_token: str | None = None,
        *,
        start_timestamp: str | None = None,
        end_timestamp: str | None = None,
        heartbeat_milliseconds: int | None = None,
    ) -> list[ChangeEvent]:
        """Query one partition synchronously and publish its change events."""
        token = self._partition_token if partition_token is None else partition_token
        params = self._build_read_params(
            token,
            self._resolve_start_timestamp(start_timestamp),
            self._end_timestamp if end_timestamp is None else end_timestamp,
            heartbeat_milliseconds,
        )
        rows = self._driver.select(self.build_read_query(), params)
        return self._publish_rows(rows, token)

    def stream(
        self,
        partition_token: str | None = None,
        *,
        start_timestamp: str | None = None,
        end_timestamp: str | None = None,
    ) -> Iterator[ChangeEvent]:
        """Stream the configured root or partition and its promoted children."""
        resolved_start = self._resolve_start_timestamp(start_timestamp)
        token = self._partition_token if partition_token is None else partition_token
        self.schedule_partition(token or "", resolved_start)
        while self._active_partitions:
            token = self._active_partitions[0]
            start_ts = self._partition_start_timestamps.get(token, resolved_start)
            yield from self.poll_partition(token or None, start_timestamp=start_ts, end_timestamp=end_timestamp)


class AsyncSpannerChangeStreamSource(_BaseSpannerChangeStreamSource[_AsyncSelectDriver], AsyncChangeSource):
    """Asynchronous Cloud Spanner reader with constructor-bound stream defaults."""

    __slots__ = ("_poll_interval_seconds",)

    def __init__(
        self,
        driver: _AsyncSelectDriver,
        stream_name: str,
        *,
        dialect: Literal["googlesql", "spangres"] | None = None,
        start_timestamp: str | None = None,
        end_timestamp: str | None = None,
        partition_token: str | None = None,
        heartbeat_milliseconds: int = 10_000,
        poll_interval_seconds: float = 0.5,
        change_feed: ChangeFeed | None = None,
        store: SyncObjectStore | None = None,
        channel: str = "spanner_cdc",
        spill_threshold_bytes: int = 7500,
    ) -> None:
        """Bind the async select driver, stream defaults, and delay between partition polls."""
        super().__init__(
            driver,
            stream_name,
            dialect=dialect,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            partition_token=partition_token,
            heartbeat_milliseconds=heartbeat_milliseconds,
            change_feed=change_feed,
            store=store,
            channel=channel,
            spill_threshold_bytes=spill_threshold_bytes,
        )
        self._poll_interval_seconds = poll_interval_seconds

    async def poll_partition(
        self,
        partition_token: str | None = None,
        *,
        start_timestamp: str | None = None,
        end_timestamp: str | None = None,
        heartbeat_milliseconds: int | None = None,
    ) -> list[ChangeEvent]:
        """Await one partition query directly and publish its change events."""
        token = self._partition_token if partition_token is None else partition_token
        params = self._build_read_params(
            token,
            self._resolve_start_timestamp(start_timestamp),
            self._end_timestamp if end_timestamp is None else end_timestamp,
            heartbeat_milliseconds,
        )
        rows = await self._driver.select(self.build_read_query(), params)
        return self._publish_rows(rows, token)

    async def stream(
        self,
        partition_token: str | None = None,
        *,
        start_timestamp: str | None = None,
        end_timestamp: str | None = None,
        poll_interval_seconds: float | None = None,
    ) -> AsyncIterator[ChangeEvent]:
        """Stream the configured root or partition and its promoted children."""
        resolved_start = self._resolve_start_timestamp(start_timestamp)
        token = self._partition_token if partition_token is None else partition_token
        interval = self._poll_interval_seconds if poll_interval_seconds is None else poll_interval_seconds
        self.schedule_partition(token or "", resolved_start)
        while self._active_partitions:
            token = self._active_partitions[0]
            start_ts = self._partition_start_timestamps.get(token, resolved_start)
            events = await self.poll_partition(token or None, start_timestamp=start_ts, end_timestamp=end_timestamp)
            for event in events:
                yield event
            if self._active_partitions and interval > 0:
                await asyncio.sleep(interval)
