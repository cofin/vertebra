"""Cloud Spanner hash-sharded event queue store with ``BYTES`` payloads and TTL deletion policy."""

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, TypedDict
from uuid import uuid4

from sqlspec.extensions.events import BaseEventQueueStore
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.sync_tools import async_

from vertebra.extensions.sqlspec.adapters.spanner.core import (
    farm_fingerprint_shard,
    normalize_spanner_dialect,
    parse_utc_timestamp,
    validate_spanner_identifier,
)

__all__ = (
    "SpannerEventQueueStore",
    "SpannerEventsConfig",
)


class SpannerEventsConfig(TypedDict, total=False):
    """Typed extension configuration keys supported by ``SpannerEventQueueStore``."""

    backend: str
    """Selected event delivery backend identifier."""
    cleanup_on_ack: bool
    """Delete acknowledged event rows immediately on acknowledgement."""
    create_schema: bool
    """Create the event queue table when it does not already exist."""
    event_poll_interval: float
    """Interval in seconds between event queue poll ticks."""
    lease_seconds: int
    """Default visibility timeout in seconds for claimed events."""
    manage_schema: bool
    """Create or migrate the event queue schema automatically on startup."""
    migrations_path: str
    """Filesystem path to custom migration scripts for the event queue."""
    poll_interval: float
    """Polling interval in seconds when waiting for available events."""
    queue_table: str
    """Cloud Spanner table name for the event queue."""
    retention_seconds: int
    """Row TTL retention window in seconds before cleanup."""
    select_for_update: bool
    """Use read-write locking hints when claiming queue rows."""
    shard_count: int
    """Number of hotspot-mitigating hash shards used for event keys."""
    skip_locked: bool
    """Skip locked rows when polling in Spangres dialect mode."""


def _encode_payload_bytes(payload: Any) -> bytes:
    if isinstance(payload, (bytes, bytearray, memoryview)):
        return bytes(payload)
    if isinstance(payload, str):
        return payload.encode("utf-8")
    return to_json(payload).encode("utf-8")


def _decode_payload_value(raw: Any) -> Any:
    if raw is None:
        return {}
    text = bytes(raw).decode("utf-8") if isinstance(raw, (bytes, bytearray, memoryview)) else str(raw)
    stripped = text.strip()
    if not stripped:
        return {}
    try:
        return from_json(stripped)
    except Exception:
        return text


def _metadata_expr(dialect: Literal["googlesql", "spangres"]) -> str:
    return "CAST(@metadata_json AS JSONB)" if dialect == "spangres" else "PARSE_JSON(@metadata_json)"


class SpannerEventQueueStore(BaseEventQueueStore[Any]):
    """Cloud Spanner event queue store using ``FARM_FINGERPRINT`` sharding, ``BYTES``, and TTL."""

    __slots__ = ("_cleanup_on_ack", "_retention_days", "_shard_count")

    adapter_name = "vertebra_spanner"

    def __init__(self, config: Any) -> None:
        super().__init__(config)
        settings = self._extension_settings
        self._cleanup_on_ack = bool(settings.get("cleanup_on_ack", False))
        self._shard_count = max(1, int(settings.get("shard_count", 16) or 16))
        retention_seconds = int(settings.get("retention_seconds", 7 * 86400) or (7 * 86400))
        self._retention_days = max(1, retention_seconds // 86400)

    def _dialect(self) -> Literal["googlesql", "spangres"]:
        return normalize_spanner_dialect(
            getattr(self._config, "spanner_dialect", None) or getattr(self._config, "dialect", "googlesql")
        )

    def _is_async_config(self) -> bool:
        return bool(getattr(self._config, "is_async", False))

    def _column_types(self) -> tuple[str, str, str]:
        """Return ``(payload_type, metadata_type, timestamp_type)`` for Spanner queue DDL."""
        if self._dialect() == "spangres":
            return "BYTEA", "JSONB", "TIMESTAMPTZ"
        return "BYTES(MAX)", "JSON", "TIMESTAMP"

    def _string_type(self, length: int) -> str:
        if self._dialect() == "spangres":
            return f"VARCHAR({length})"
        return f"STRING({length})"

    def _integer_type(self) -> str:
        return "BIGINT" if self._dialect() == "spangres" else "INT64"

    def _timestamp_default(self) -> str:
        return "CURRENT_TIMESTAMP" if self._dialect() == "spangres" else "CURRENT_TIMESTAMP()"

    def _primary_key_syntax(self) -> str:
        if self._dialect() == "spangres":
            return ", PRIMARY KEY (shard_id, event_id)"
        return ""

    def _table_ddl(self) -> str:
        table = validate_spanner_identifier(self.table_name, "queue_table")
        payload_type, metadata_type, timestamp_type = self._column_types()
        dialect = self._dialect()
        if dialect == "spangres":
            return (
                f"CREATE TABLE {table} ("
                "shard_id BIGINT NOT NULL, "
                "event_id VARCHAR(64) NOT NULL, "
                "channel VARCHAR(128) NOT NULL, "
                f"payload_json {payload_type} NOT NULL, "
                f"metadata_json {metadata_type}, "
                "status VARCHAR(32) NOT NULL DEFAULT 'PENDING', "
                f"available_at {timestamp_type} NOT NULL, "
                f"lease_expires_at {timestamp_type}, "
                "attempts BIGINT NOT NULL DEFAULT 0, "
                f"created_at {timestamp_type} NOT NULL, "
                f"expires_at {timestamp_type} NOT NULL, "
                f"acknowledged_at {timestamp_type}, "
                "PRIMARY KEY (shard_id, event_id)"
                f") TTL INTERVAL '{self._retention_days} days' ON expires_at"
            )
        return (
            f"CREATE TABLE {table} ("
            f"shard_id INT64 NOT NULL DEFAULT (MOD(ABS(FARM_FINGERPRINT(event_id)), {self._shard_count})), "
            "event_id STRING(64) NOT NULL, "
            "channel STRING(128) NOT NULL, "
            f"payload_json {payload_type} NOT NULL, "
            f"metadata_json {metadata_type}, "
            "status STRING(32) NOT NULL DEFAULT ('PENDING'), "
            f"available_at {timestamp_type} NOT NULL OPTIONS (allow_commit_timestamp = true), "
            f"lease_expires_at {timestamp_type}, "
            "attempts INT64 NOT NULL DEFAULT (0), "
            f"created_at {timestamp_type} NOT NULL OPTIONS (allow_commit_timestamp = true), "
            f"expires_at {timestamp_type} NOT NULL, "
            f"acknowledged_at {timestamp_type}"
            ") PRIMARY KEY (shard_id, event_id), "
            "ROW DELETION POLICY (OLDER_THAN(expires_at, INTERVAL 0 DAY))"
        )

    def _index_ddl(self) -> str | None:
        table = validate_spanner_identifier(self.table_name, "queue_table")
        index_name = validate_spanner_identifier(self._index_name(), "queue_index")
        if self._dialect() == "spangres":
            return (
                f"CREATE INDEX {index_name} ON {table}(channel, status, available_at) "
                "INCLUDE (payload_json, metadata_json, created_at, expires_at)"
            )
        return (
            f"CREATE INDEX {index_name} ON {table}(channel, status, available_at) "
            "STORING (payload_json, metadata_json, created_at, expires_at)"
        )

    def ddl_statements(self) -> tuple[str, ...]:
        """Return idempotent Cloud Spanner queue table and covering index DDL statements."""
        return tuple(self.create_statements())

    def compute_shard_id(self, event_id: str) -> int:
        """Return the deterministic hash shard prefix for ``event_id``."""
        return farm_fingerprint_shard(event_id, self._shard_count)

    def create_tables_sync(self) -> None:
        """Create the Spanner event queue table and covering index synchronously."""
        with self._config.provide_session(transaction=False) as driver:
            for stmt in self.create_statements():
                driver.execute(stmt)

    async def create_tables(self) -> None:
        """Create the Spanner event queue table and covering index asynchronously."""
        statements = self.create_statements()
        if self._is_async_config():
            async with self._config.provide_session(transaction=False) as driver:
                for stmt in statements:
                    await driver.execute(stmt)
            return
        await async_(self.create_tables_sync)()

    def publish_sync(
        self,
        channel: str,
        payload: Mapping[str, Any] | bytes | str,
        *,
        event_id: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Publish an event record synchronously into a hash-sharded row."""
        eid = event_id or uuid4().hex
        shard_id = self.compute_shard_id(eid)
        now = datetime.now(UTC)
        expires_at = now + timedelta(days=self._retention_days)
        table = validate_spanner_identifier(self.table_name, "queue_table")
        dialect = self._dialect()
        payload_bytes = _encode_payload_bytes(payload)
        metadata_json = to_json(dict(metadata)) if metadata is not None else None
        sql = (
            f"INSERT INTO {table} "
            "(shard_id, event_id, channel, payload_json, metadata_json, status, available_at, created_at, expires_at) "
            f"VALUES (@shard_id, @event_id, @channel, @payload_json, {_metadata_expr(dialect)}, "
            "@status, @available_at, @created_at, @expires_at)"
        )
        params = {
            "shard_id": shard_id,
            "event_id": eid,
            "channel": channel,
            "payload_json": payload_bytes,
            "metadata_json": metadata_json,
            "status": "PENDING",
            "available_at": now.isoformat(),
            "created_at": now.isoformat(),
            "expires_at": expires_at.isoformat(),
        }
        with self._config.provide_session(transaction=True) as driver:
            driver.execute(sql, params)
        return {
            "shard_id": shard_id,
            "event_id": eid,
            "channel": channel,
            "payload": _decode_payload_value(payload_bytes),
            "metadata": dict(metadata) if metadata is not None else None,
            "status": "PENDING",
            "created_at": now,
            "expires_at": expires_at,
        }

    async def publish(
        self,
        channel: str,
        payload: Mapping[str, Any] | bytes | str,
        *,
        event_id: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Publish an event record asynchronously into a hash-sharded row."""
        if not self._is_async_config():
            return await async_(self.publish_sync)(channel, payload, event_id=event_id, metadata=metadata)
        eid = event_id or uuid4().hex
        shard_id = self.compute_shard_id(eid)
        now = datetime.now(UTC)
        expires_at = now + timedelta(days=self._retention_days)
        table = validate_spanner_identifier(self.table_name, "queue_table")
        dialect = self._dialect()
        payload_bytes = _encode_payload_bytes(payload)
        metadata_json = to_json(dict(metadata)) if metadata is not None else None
        sql = (
            f"INSERT INTO {table} "
            "(shard_id, event_id, channel, payload_json, metadata_json, status, available_at, created_at, expires_at) "
            f"VALUES (@shard_id, @event_id, @channel, @payload_json, {_metadata_expr(dialect)}, "
            "@status, @available_at, @created_at, @expires_at)"
        )
        params = {
            "shard_id": shard_id,
            "event_id": eid,
            "channel": channel,
            "payload_json": payload_bytes,
            "metadata_json": metadata_json,
            "status": "PENDING",
            "available_at": now.isoformat(),
            "created_at": now.isoformat(),
            "expires_at": expires_at.isoformat(),
        }
        async with self._config.provide_session(transaction=True) as driver:
            await driver.execute(sql, params)
        return {
            "shard_id": shard_id,
            "event_id": eid,
            "channel": channel,
            "payload": _decode_payload_value(payload_bytes),
            "metadata": dict(metadata) if metadata is not None else None,
            "status": "PENDING",
            "created_at": now,
            "expires_at": expires_at,
        }

    def consume_sync(self, channel: str, *, limit: int = 50) -> list[dict[str, Any]]:
        """Consume pending events via a lock-free ``ReadOnly`` snapshot."""
        table = validate_spanner_identifier(self.table_name, "queue_table")
        sql = (
            "SELECT shard_id, event_id, channel, payload_json, metadata_json, status, created_at, expires_at "
            f"FROM {table} WHERE channel = @channel AND status = @status "
            "ORDER BY available_at ASC LIMIT @limit"
        )
        with self._config.provide_session(transaction=False, read_only=True) as driver:
            rows = driver.select(sql, {"channel": channel, "status": "PENDING", "limit": int(limit)})
        return [
            {
                "shard_id": int(row["shard_id"]),
                "event_id": str(row["event_id"]),
                "channel": str(row["channel"]),
                "payload": _decode_payload_value(row.get("payload_json")),
                "metadata": _decode_payload_value(row.get("metadata_json"))
                if row.get("metadata_json") is not None
                else None,
                "status": str(row["status"]),
                "created_at": parse_utc_timestamp(row["created_at"]) if row.get("created_at") is not None else None,
                "expires_at": parse_utc_timestamp(row["expires_at"]) if row.get("expires_at") is not None else None,
            }
            for row in rows
        ]

    async def consume(self, channel: str, *, limit: int = 50) -> list[dict[str, Any]]:
        """Consume pending events asynchronously via a lock-free ``ReadOnly`` snapshot."""
        if not self._is_async_config():
            return await async_(self.consume_sync)(channel, limit=limit)
        table = validate_spanner_identifier(self.table_name, "queue_table")
        sql = (
            "SELECT shard_id, event_id, channel, payload_json, metadata_json, status, created_at, expires_at "
            f"FROM {table} WHERE channel = @channel AND status = @status "
            "ORDER BY available_at ASC LIMIT @limit"
        )
        async with self._config.provide_session(transaction=False, read_only=True) as driver:
            rows = await driver.select(sql, {"channel": channel, "status": "PENDING", "limit": int(limit)})
        return [
            {
                "shard_id": int(row["shard_id"]),
                "event_id": str(row["event_id"]),
                "channel": str(row["channel"]),
                "payload": _decode_payload_value(row.get("payload_json")),
                "metadata": _decode_payload_value(row.get("metadata_json"))
                if row.get("metadata_json") is not None
                else None,
                "status": str(row["status"]),
                "created_at": parse_utc_timestamp(row["created_at"]) if row.get("created_at") is not None else None,
                "expires_at": parse_utc_timestamp(row["expires_at"]) if row.get("expires_at") is not None else None,
            }
            for row in rows
        ]

    def ack_sync(self, event_id: str) -> bool:
        """Acknowledge or delete ``event_id`` by point key ``(shard_id, event_id)``."""
        table = validate_spanner_identifier(self.table_name, "queue_table")
        shard_id = self.compute_shard_id(event_id)
        now_iso = datetime.now(UTC).isoformat()
        with self._config.provide_session(transaction=True) as driver:
            if self._cleanup_on_ack:
                res = driver.execute(
                    f"DELETE FROM {table} WHERE shard_id = @shard_id AND event_id = @event_id",
                    {"shard_id": shard_id, "event_id": event_id},
                )
            else:
                res = driver.execute(
                    f"UPDATE {table} SET status = @status, acknowledged_at = @acknowledged_at "
                    "WHERE shard_id = @shard_id AND event_id = @event_id",
                    {"status": "ACKED", "acknowledged_at": now_iso, "shard_id": shard_id, "event_id": event_id},
                )
        affected = int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)
        return affected > 0

    async def ack(self, event_id: str) -> bool:
        """Acknowledge or delete ``event_id`` asynchronously by point key ``(shard_id, event_id)``."""
        if not self._is_async_config():
            return await async_(self.ack_sync)(event_id)
        table = validate_spanner_identifier(self.table_name, "queue_table")
        shard_id = self.compute_shard_id(event_id)
        now_iso = datetime.now(UTC).isoformat()
        async with self._config.provide_session(transaction=True) as driver:
            if self._cleanup_on_ack:
                res = await driver.execute(
                    f"DELETE FROM {table} WHERE shard_id = @shard_id AND event_id = @event_id",
                    {"shard_id": shard_id, "event_id": event_id},
                )
            else:
                res = await driver.execute(
                    f"UPDATE {table} SET status = @status, acknowledged_at = @acknowledged_at "
                    "WHERE shard_id = @shard_id AND event_id = @event_id",
                    {"status": "ACKED", "acknowledged_at": now_iso, "shard_id": shard_id, "event_id": event_id},
                )
        affected = int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)
        return affected > 0
