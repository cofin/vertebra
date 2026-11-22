"""BigQuery ADK session, memory (VECTOR_SEARCH/SEARCH), and ObjectRef->ArtifactReference stores."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, NotRequired, TypedDict, cast

from sqlspec.config import ADKConfig
from sqlspec.extensions.adk import BaseAsyncADKStore, BaseSyncADKStore, StoredEvent, StoredSession
from sqlspec.extensions.adk.store import normalize_session_list_options
from sqlspec.utils.serializers import from_json, to_json

from vertebra.db.adapters.bigquery import ObjectRef, ObjectTableConfig
from vertebra.extensions.sqlspec.adapters.bigquery.core import (
    bigquery_partition_clauses,
    is_bigquery_table_missing,
    object_ref_to_artifact_reference,
    parse_utc_timestamp,
    positive_int_or_none,
    qualify_store_table,
    quote_bigquery_identifier,
    resolve_storage_backend,
)
from vertebra.extensions.sqlspec.storage import ObjectStoreBackend
from vertebra.storage import ArtifactReference
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from sqlspec.extensions.adk import SessionOrderBy, StoredMemory
    from sqlspec.extensions.adk.artifact import StoredArtifact
    from sqlspec.extensions.adk.artifact.store import (
        BaseAsyncADKArtifactStore as _BaseAsyncADKArtifactStore,
    )
    from sqlspec.extensions.adk.artifact.store import (
        BaseSyncADKArtifactStore as _BaseSyncADKArtifactStore,
    )
    from sqlspec.extensions.adk.converters import merge_scoped_state
    from sqlspec.extensions.adk.memory.store import (
        BaseAsyncADKMemoryStore as _BaseAsyncADKMemoryStore,
    )
    from sqlspec.extensions.adk.memory.store import (
        BaseSyncADKMemoryStore as _BaseSyncADKMemoryStore,
    )

    from vertebra.extensions.sqlspec.adapters.bigquery.config import BigQueryAsyncConfig, BigQuerySyncConfig
else:
    _memory_store_mod = ensure_module("sqlspec.extensions.adk.memory.store", package="google-adk", extra="adk")
    _artifact_store_mod = ensure_module("sqlspec.extensions.adk.artifact.store", package="google-adk", extra="adk")
    _converters_mod = ensure_module("sqlspec.extensions.adk.converters", package="google-adk", extra="adk")
    _artifact_types_mod = ensure_module("sqlspec.extensions.adk.artifact", package="google-adk", extra="adk")
    _memory_types_mod = ensure_module("sqlspec.extensions.adk.memory", package="google-adk", extra="adk")
    _BaseAsyncADKMemoryStore = _memory_store_mod.BaseAsyncADKMemoryStore
    _BaseSyncADKMemoryStore = _memory_store_mod.BaseSyncADKMemoryStore
    _BaseAsyncADKArtifactStore = _artifact_store_mod.BaseAsyncADKArtifactStore
    _BaseSyncADKArtifactStore = _artifact_store_mod.BaseSyncADKArtifactStore
    merge_scoped_state = _converters_mod.merge_scoped_state
    StoredArtifact = _artifact_types_mod.StoredArtifact
    StoredMemory = _memory_types_mod.StoredMemory

__all__ = (
    "BigQueryADKConfig",
    "BigQueryADKRetentionConfig",
    "BigQueryAsyncADKArtifactStore",
    "BigQueryAsyncADKMemoryStore",
    "BigQueryAsyncADKStore",
    "BigQuerySyncADKArtifactStore",
    "BigQuerySyncADKMemoryStore",
    "BigQuerySyncADKStore",
)

_BIGQUERY_ADK_EXTENSION_OPTIONS: Final[frozenset[str]] = frozenset(
    {
        "lookup_window_days",
        "memory_retention_days",
        "partition_by_day",
        "partition_expiration_days",
        "require_partition_filter",
        "retention",
        "session_lookup_window_days",
        "vector_dimensions",
        "vector_index_type",
        "with_partitioning",
    }
)


class BigQueryADKRetentionConfig(TypedDict, total=False):
    """BigQuery-specific ADK retention settings."""

    event_ttl_seconds: NotRequired[int]
    """Event partition retention in seconds."""


class BigQueryADKConfig(ADKConfig, total=False):
    """Typed extension configuration for BigQuery ADK stores."""

    retention: NotRequired[BigQueryADKRetentionConfig]
    """BigQuery event partition retention settings."""
    session_lookup_window_days: NotRequired[int]
    """Days of session partitions scanned by lookup and list queries."""
    memory_retention_days: NotRequired[int]
    """Retention window in days for partitioned ADK memory tables."""
    partition_by_day: NotRequired[bool]
    """Enable daily BigQuery time-based partitioning on ADK tables."""
    require_partition_filter: NotRequired[bool]
    """Require partition filter predicates in generated BigQuery table DDL and queries."""
    with_partitioning: NotRequired[bool]
    """Enable BigQuery time-based table partitioning on ADK tables."""
    partition_expiration_days: NotRequired[int]
    """Partition retention window in days when partitioning is enabled."""
    lookup_window_days: NotRequired[int]
    """Default lookback window in days when querying partitioned ADK tables."""
    vector_dimensions: NotRequired[int]
    """Embedding dimensionality for ADK memory vector similarity columns."""
    vector_index_type: NotRequired[str]
    """BigQuery vector index algorithm name for memory embeddings."""


def _extract_adk_extension(config: Any) -> dict[str, Any]:
    ext = getattr(config, "extension_config", {})
    if isinstance(ext, Mapping) and isinstance(ext.get("adk"), Mapping):
        return dict(ext["adk"])
    return {}


def _resolve_json_codecs(config: Any) -> tuple[Any, Any]:
    features = getattr(config, "driver_features", {}) or {}
    return features.get("json_serializer"), features.get("json_deserializer")


def _partition_settings(config: Any) -> tuple[bool, int | None, bool, int | None]:
    adk = _extract_adk_extension(config)
    with_partitioning = (
        bool(adk["partition_by_day"]) if "partition_by_day" in adk else bool(adk.get("with_partitioning", True))
    )
    expiration_days = positive_int_or_none(adk.get("partition_expiration_days"), "partition_expiration_days")
    if expiration_days is None:
        retention = adk.get("retention")
        if isinstance(retention, Mapping):
            ttl_seconds = retention.get("event_ttl_seconds")
            if isinstance(ttl_seconds, int) and not isinstance(ttl_seconds, bool) and ttl_seconds > 0:
                expiration_days = max(1, math.ceil(ttl_seconds / 86400))
    require_filter = bool(adk.get("require_partition_filter", False))
    raw_window = (
        adk.get("session_lookup_window_days")
        if adk.get("session_lookup_window_days") is not None
        else adk.get("lookup_window_days")
    )
    lookup_window_days = positive_int_or_none(raw_window, "session_lookup_window_days")
    return with_partitioning, expiration_days, require_filter, lookup_window_days


def _partition_filter_clause(column: str, *, require_filter: bool, alias: str | None = None) -> str:
    if not require_filter:
        return ""
    qualified = f"{alias}.{column}" if alias else column
    return f" AND {qualified} IS NOT NULL"


def _decode_json_dict(value: Any, *, deserializer: Any = None) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode("utf-8")
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return {}
        loader = deserializer if callable(deserializer) else from_json
        parsed = loader(stripped)
        return parsed if isinstance(parsed, dict) else {"value": parsed}
    return dict(value) if hasattr(value, "items") else {}


def _encode_json_text(value: Any, *, serializer: Any = None) -> str:
    if value is None:
        return "{}"
    if isinstance(value, str):
        return value
    dumper = serializer if callable(serializer) else to_json
    encoded = dumper(value)
    if isinstance(encoded, str):
        return encoded
    if isinstance(encoded, (bytes, bytearray, memoryview)):
        return bytes(encoded).decode("utf-8")
    return str(encoded)


def _build_session_ddl(config: Any, session_table: str, owner_id_ddl: str | None) -> str:
    with_partitioning, expiration_days, require_filter, _ = _partition_settings(config)
    part_sql, opt_sql = bigquery_partition_clauses(
        "create_time",
        enabled=with_partitioning,
        expiration_days=expiration_days,
        require_filter=require_filter,
        cluster_by=("app_name", "user_id", "id"),
    )
    table_ref = qualify_store_table(config, session_table)
    owner_col = f"{owner_id_ddl}, " if owner_id_ddl else ""
    return (
        f"CREATE TABLE IF NOT EXISTS {table_ref} ("
        "id STRING NOT NULL, "
        "app_name STRING NOT NULL, "
        "user_id STRING NOT NULL, "
        f"{owner_col}"
        "state JSON NOT NULL, "
        "create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(), "
        "update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()"
        f"){part_sql}{opt_sql}"
    )


def _build_events_ddl(config: Any, events_table: str) -> str:
    with_partitioning, expiration_days, require_filter, _ = _partition_settings(config)
    part_sql, opt_sql = bigquery_partition_clauses(
        "timestamp",
        enabled=with_partitioning,
        expiration_days=expiration_days,
        require_filter=require_filter,
        cluster_by=("app_name", "user_id", "session_id"),
    )
    table_ref = qualify_store_table(config, events_table)
    return (
        f"CREATE TABLE IF NOT EXISTS {table_ref} ("
        "id STRING NOT NULL, "
        "app_name STRING NOT NULL, "
        "user_id STRING NOT NULL, "
        "session_id STRING NOT NULL, "
        "invocation_id STRING NOT NULL, "
        "timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(), "
        "event_data JSON NOT NULL"
        f"){part_sql}{opt_sql}"
    )


def _build_app_state_ddl(config: Any, table_name: str) -> str:
    table_ref = qualify_store_table(config, table_name)
    return (
        f"CREATE TABLE IF NOT EXISTS {table_ref} ("
        "app_name STRING NOT NULL, "
        "state JSON NOT NULL, "
        "update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()"
        ")"
    )


def _build_user_state_ddl(config: Any, table_name: str) -> str:
    table_ref = qualify_store_table(config, table_name)
    return (
        f"CREATE TABLE IF NOT EXISTS {table_ref} ("
        "app_name STRING NOT NULL, "
        "user_id STRING NOT NULL, "
        "state JSON NOT NULL, "
        "update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()"
        ")"
    )


def _build_metadata_ddl(config: Any, table_name: str) -> str:
    table_ref = qualify_store_table(config, table_name)
    return (
        f"CREATE TABLE IF NOT EXISTS {table_ref} ("
        "`key` STRING NOT NULL, "
        "value STRING NOT NULL, "
        "update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()"
        ")"
    )


def _row_to_stored_session(
    row: dict[str, Any],
    *,
    deserializer: Any = None,
    app_state: dict[str, Any] | None = None,
    user_state: dict[str, Any] | None = None,
    merge_scopes: bool = False,
) -> StoredSession:
    session_state = _decode_json_dict(row.get("state"), deserializer=deserializer)
    resolved_state = merge_scoped_state(session_state, app_state, user_state) if merge_scopes else session_state
    return cast(
        "StoredSession",
        {
            "id": str(row.get("id") or ""),
            "app_name": str(row.get("app_name") or ""),
            "user_id": str(row.get("user_id") or ""),
            "state": resolved_state,
            "create_time": parse_utc_timestamp(row.get("create_time")),
            "update_time": parse_utc_timestamp(row.get("update_time")),
        },
    )


def _row_to_stored_event(row: dict[str, Any], *, deserializer: Any = None) -> StoredEvent:
    event_data = _decode_json_dict(row.get("event_data"), deserializer=deserializer)
    if not event_data and row.get("event_json") is not None:
        event_data = _decode_json_dict(row.get("event_json"), deserializer=deserializer)
    return cast(
        "StoredEvent",
        {
            "id": str(row.get("id") or ""),
            "app_name": str(row.get("app_name") or ""),
            "user_id": str(row.get("user_id") or ""),
            "session_id": str(row.get("session_id") or ""),
            "invocation_id": str(row.get("invocation_id") or ""),
            "timestamp": parse_utc_timestamp(row.get("timestamp")),
            "event_data": event_data,
        },
    )


def _row_to_stored_memory(row: dict[str, Any], *, deserializer: Any = None) -> StoredMemory:
    raw_embedding = row.get("embedding")
    embedding: list[float] | None = None
    if raw_embedding is not None:
        if isinstance(raw_embedding, str):
            loader = deserializer if callable(deserializer) else from_json
            parsed = loader(raw_embedding)
            if isinstance(parsed, Sequence) and not isinstance(parsed, (str, bytes, bytearray)):
                embedding = [float(x) for x in parsed]
        elif isinstance(raw_embedding, Sequence) and not isinstance(raw_embedding, (bytes, bytearray)):
            embedding = [float(x) for x in raw_embedding]
    return cast(
        "StoredMemory",
        {
            "id": str(row.get("id") or ""),
            "session_id": str(row.get("session_id") or ""),
            "app_name": str(row.get("app_name") or ""),
            "user_id": str(row.get("user_id") or ""),
            "scope": str(row.get("scope") or "user"),
            "event_id": str(row.get("event_id") or ""),
            "author": row.get("author"),
            "timestamp": parse_utc_timestamp(row.get("timestamp")),
            "content_json": _decode_json_dict(row.get("content_json"), deserializer=deserializer),
            "content_text": str(row.get("content_text") or ""),
            "metadata_json": _decode_json_dict(row.get("metadata_json"), deserializer=deserializer)
            if row.get("metadata_json") is not None
            else None,
            "inserted_at": parse_utc_timestamp(row.get("inserted_at")),
            "embedding": embedding,
        },
    )


def _row_to_stored_artifact(row: dict[str, Any], *, deserializer: Any = None) -> StoredArtifact:
    custom_raw = row.get("custom_metadata")
    return cast(
        "StoredArtifact",
        {
            "app_name": str(row.get("app_name") or ""),
            "user_id": str(row.get("user_id") or ""),
            "session_id": row.get("session_id"),
            "filename": str(row.get("filename") or ""),
            "version": int(row.get("version", 0) or 0),
            "mime_type": row.get("mime_type"),
            "canonical_uri": str(row.get("canonical_uri") or ""),
            "custom_metadata": _decode_json_dict(custom_raw, deserializer=deserializer)
            if custom_raw is not None
            else None,
            "created_at": parse_utc_timestamp(row.get("created_at")),
        },
    )


class _BigQueryADKSessionMixin:
    """Shared SQL and parameter builders for BigQuery sync and async ADK session stores."""

    __slots__ = ()

    _max_table_name_parts: ClassVar[int] = 3
    adapter_name: ClassVar[str | None] = "vertebra_bigquery"
    extension_config_options: ClassVar[frozenset[str]] = _BIGQUERY_ADK_EXTENSION_OPTIONS

    def _build_sessions_table_ddl(self) -> str:
        store = cast("Any", self)
        return _build_session_ddl(store._config, store._session_table, store._owner_id_column_ddl)

    def _build_events_table_ddl(self) -> str:
        store = cast("Any", self)
        return _build_events_ddl(store._config, store._events_table)

    def _build_app_states_table_ddl(self) -> str:
        store = cast("Any", self)
        return _build_app_state_ddl(store._config, store._app_state_table)

    def _build_user_states_table_ddl(self) -> str:
        store = cast("Any", self)
        return _build_user_state_ddl(store._config, store._user_state_table)

    def _build_metadata_table_ddl(self) -> str:
        store = cast("Any", self)
        return _build_metadata_ddl(store._config, store._metadata_table)

    def _drop_tables_sql(self) -> list[str]:
        store = cast("Any", self)
        return [
            f"DROP TABLE IF EXISTS {qualify_store_table(store._config, store._events_table)}",
            f"DROP TABLE IF EXISTS {qualify_store_table(store._config, store._session_table)}",
        ]

    def _drop_app_states_table_sql(self) -> str:
        store = cast("Any", self)
        return f"DROP TABLE IF EXISTS {qualify_store_table(store._config, store._app_state_table)}"

    def _drop_user_states_table_sql(self) -> str:
        store = cast("Any", self)
        return f"DROP TABLE IF EXISTS {qualify_store_table(store._config, store._user_state_table)}"

    def _drop_metadata_table_sql(self) -> str:
        store = cast("Any", self)
        return f"DROP TABLE IF EXISTS {qualify_store_table(store._config, store._metadata_table)}"

    @staticmethod
    def _validate_event_scope(event_record: StoredEvent, app_name: str, user_id: str, session_id: str) -> None:
        record_scope = (event_record["app_name"], event_record["user_id"], event_record["session_id"])
        expected_scope = (app_name, user_id, session_id)
        if record_scope != expected_scope:
            msg = f"event_record scope {record_scope!r} does not match {expected_scope!r}"
            raise ValueError(msg)

    def _build_create_session_sql_and_params(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
        state: dict[str, Any],
        owner_id: Any | None,
        now: datetime,
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        now_iso = now.isoformat()
        table_ref = qualify_store_table(store._config, store._session_table)
        state_json = _encode_json_text(state, serializer=store._json_serializer)
        params: dict[str, Any] = {
            "id": session_id,
            "app_name": app_name,
            "user_id": user_id,
            "state": state_json,
            "create_time": now_iso,
            "update_time": now_iso,
        }
        if store._owner_id_column_name:
            owner_col = quote_bigquery_identifier(store._owner_id_column_name)
            params["owner_id"] = owner_id
            sql = (
                f"INSERT INTO {table_ref} (id, app_name, user_id, {owner_col}, state, create_time, update_time) "
                "VALUES (@id, @app_name, @user_id, @owner_id, PARSE_JSON(@state), @create_time, @update_time)"
            )
            return sql, params
        sql = (
            f"INSERT INTO {table_ref} (id, app_name, user_id, state, create_time, update_time) "
            "VALUES (@id, @app_name, @user_id, PARSE_JSON(@state), @create_time, @update_time)"
        )
        return sql, params

    def _build_get_session_sql_and_params(
        self, app_name: str, user_id: str, session_id: str
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table_ref = qualify_store_table(store._config, store._session_table)
        _, _, require_filter, lookup_window_days = _partition_settings(store._config)
        params: dict[str, Any] = {"id": session_id, "app_name": app_name, "user_id": user_id}
        extra = _partition_filter_clause("create_time", require_filter=require_filter)
        if lookup_window_days is not None:
            params["window_start"] = (datetime.now(UTC) - timedelta(days=lookup_window_days)).isoformat()
            extra += " AND create_time >= @window_start"
        sql = (
            f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table_ref} "
            f"WHERE id = @id AND app_name = @app_name AND user_id = @user_id{extra} LIMIT 1"
        )
        return sql, params

    def _build_touch_session_sql_and_params(
        self, app_name: str, user_id: str, session_id: str
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table_ref = qualify_store_table(store._config, store._session_table)
        _, _, require_filter, _ = _partition_settings(store._config)
        part_filter = _partition_filter_clause("create_time", require_filter=require_filter)
        sql = (
            f"UPDATE {table_ref} SET update_time = @update_time "
            f"WHERE id = @id AND app_name = @app_name AND user_id = @user_id{part_filter}"
        )
        params = {
            "id": session_id,
            "app_name": app_name,
            "user_id": user_id,
            "update_time": datetime.now(UTC).isoformat(),
        }
        return sql, params

    def _build_update_session_state_sql_and_params(
        self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table_ref = qualify_store_table(store._config, store._session_table)
        _, _, require_filter, _ = _partition_settings(store._config)
        part_filter = _partition_filter_clause("create_time", require_filter=require_filter)
        sql = (
            f"UPDATE {table_ref} SET state = PARSE_JSON(@state), update_time = @update_time "
            f"WHERE id = @id AND app_name = @app_name AND user_id = @user_id{part_filter}"
        )
        params = {
            "id": session_id,
            "app_name": app_name,
            "user_id": user_id,
            "state": _encode_json_text(state, serializer=store._json_serializer),
            "update_time": datetime.now(UTC).isoformat(),
        }
        return sql, params

    def _build_list_sessions_sql_and_params(
        self,
        app_name: str,
        user_id: str | None,
        *,
        order_by: SessionOrderBy,
        descending: bool,
        limit: int | None,
        offset: int | None,
    ) -> tuple[str, dict[str, Any]] | None:
        store = cast("Any", self)
        column, direction, page_limit, page_offset = normalize_session_list_options(order_by, descending, limit, offset)
        if page_limit == 0:
            return None
        table_ref = qualify_store_table(store._config, store._session_table)
        _, _, require_filter, lookup_window_days = _partition_settings(store._config)
        where = "app_name = @app_name"
        params: dict[str, Any] = {"app_name": app_name}
        if user_id is not None:
            where += " AND user_id = @user_id"
            params["user_id"] = user_id
        where += _partition_filter_clause("create_time", require_filter=require_filter)
        if lookup_window_days is not None:
            where += " AND create_time >= @window_start"
            params["window_start"] = (datetime.now(UTC) - timedelta(days=lookup_window_days)).isoformat()
        limit_sql = ""
        if page_limit is not None:
            limit_sql = " LIMIT @limit OFFSET @offset"
            params["limit"] = page_limit
            params["offset"] = page_offset
        elif page_offset:
            limit_sql = " OFFSET @offset"
            params["offset"] = page_offset
        sql = (
            f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table_ref} "
            f"WHERE {where} ORDER BY {column} {direction}, id {direction}{limit_sql}"
        )
        return sql, params

    def _build_delete_session_sqls_and_params(
        self, app_name: str, user_id: str, session_id: str
    ) -> tuple[str, str, dict[str, Any]]:
        store = cast("Any", self)
        sessions_ref = qualify_store_table(store._config, store._session_table)
        events_ref = qualify_store_table(store._config, store._events_table)
        _, _, require_filter, _ = _partition_settings(store._config)
        ev_filter = _partition_filter_clause("timestamp", require_filter=require_filter)
        sess_filter = _partition_filter_clause("create_time", require_filter=require_filter)
        events_sql = f"DELETE FROM {events_ref} WHERE session_id = @id AND app_name = @app_name AND user_id = @user_id{ev_filter}"
        sessions_sql = (
            f"DELETE FROM {sessions_ref} WHERE id = @id AND app_name = @app_name AND user_id = @user_id{sess_filter}"
        )
        params = {"id": session_id, "app_name": app_name, "user_id": user_id}
        return events_sql, sessions_sql, params

    def _build_append_event_sql_and_params(self, event_record: StoredEvent) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        events_ref = qualify_store_table(store._config, store._events_table)
        ts = parse_utc_timestamp(event_record["timestamp"]).isoformat()
        sql = (
            f"INSERT INTO {events_ref} "
            "(id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
            "VALUES (@id, @app_name, @user_id, @session_id, @invocation_id, @timestamp, PARSE_JSON(@event_data))"
        )
        params = {
            "id": event_record["id"],
            "app_name": event_record["app_name"],
            "user_id": event_record["user_id"],
            "session_id": event_record["session_id"],
            "invocation_id": event_record["invocation_id"],
            "timestamp": ts,
            "event_data": _encode_json_text(
                event_record.get("event_data", {}),
                serializer=store._json_serializer,
            ),
        }
        return sql, params

    def _build_get_events_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None,
        limit: int | None,
    ) -> tuple[str, dict[str, Any]] | None:
        if limit is not None and (isinstance(limit, bool) or limit < 0):
            msg = "limit must be non-negative"
            raise ValueError(msg)
        if limit == 0:
            return None
        store = cast("Any", self)
        events_ref = qualify_store_table(store._config, store._events_table)
        _, _, require_filter, _ = _partition_settings(store._config)
        part_filter = _partition_filter_clause("timestamp", require_filter=require_filter)
        where = f"app_name = @app_name AND user_id = @user_id AND session_id = @session_id{part_filter}"
        params: dict[str, Any] = {"app_name": app_name, "user_id": user_id, "session_id": session_id}
        if after_timestamp is not None:
            where += " AND timestamp > @after_timestamp"
            params["after_timestamp"] = parse_utc_timestamp(after_timestamp).isoformat()
        limit_sql = " LIMIT @limit" if limit is not None else ""
        if limit is not None:
            params["limit"] = limit
        sql = (
            f"SELECT id, app_name, user_id, session_id, invocation_id, timestamp, event_data "
            f"FROM {events_ref} WHERE {where} ORDER BY timestamp ASC{limit_sql}"
        )
        return sql, params

    def _build_get_app_state_sql_and_params(self, app_name: str) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table_ref = qualify_store_table(store._config, store._app_state_table)
        return f"SELECT state FROM {table_ref} WHERE app_name = @app_name LIMIT 1", {"app_name": app_name}

    def _build_upsert_app_state_sql_and_params(
        self, app_name: str, state: dict[str, Any]
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table_ref = qualify_store_table(store._config, store._app_state_table)
        sql = (
            f"MERGE INTO {table_ref} AS target "
            "USING (SELECT @app_name AS app_name, PARSE_JSON(@state) AS state) AS source "
            "ON target.app_name = source.app_name "
            "WHEN MATCHED THEN UPDATE SET state = source.state, update_time = CURRENT_TIMESTAMP() "
            "WHEN NOT MATCHED THEN INSERT (app_name, state, update_time) "
            "VALUES (source.app_name, source.state, CURRENT_TIMESTAMP())"
        )
        return sql, {"app_name": app_name, "state": _encode_json_text(state, serializer=store._json_serializer)}

    def _build_get_user_state_sql_and_params(self, app_name: str, user_id: str) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table_ref = qualify_store_table(store._config, store._user_state_table)
        sql = f"SELECT state FROM {table_ref} WHERE app_name = @app_name AND user_id = @user_id LIMIT 1"
        return sql, {"app_name": app_name, "user_id": user_id}

    def _build_upsert_user_state_sql_and_params(
        self, app_name: str, user_id: str, state: dict[str, Any]
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table_ref = qualify_store_table(store._config, store._user_state_table)
        sql = (
            f"MERGE INTO {table_ref} AS target "
            "USING (SELECT @app_name AS app_name, @user_id AS user_id, PARSE_JSON(@state) AS state) AS source "
            "ON target.app_name = source.app_name AND target.user_id = source.user_id "
            "WHEN MATCHED THEN UPDATE SET state = source.state, update_time = CURRENT_TIMESTAMP() "
            "WHEN NOT MATCHED THEN INSERT (app_name, user_id, state, update_time) "
            "VALUES (source.app_name, source.user_id, source.state, CURRENT_TIMESTAMP())"
        )
        return sql, {
            "app_name": app_name,
            "user_id": user_id,
            "state": _encode_json_text(state, serializer=store._json_serializer),
        }

    def _build_get_metadata_sql_and_params(self, key: str) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table_ref = qualify_store_table(store._config, store._metadata_table)
        return f"SELECT value FROM {table_ref} WHERE `key` = @key LIMIT 1", {"key": key}

    def _build_set_metadata_sql_and_params(self, key: str, value: str) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table_ref = qualify_store_table(store._config, store._metadata_table)
        sql = (
            f"MERGE INTO {table_ref} AS target "
            "USING (SELECT @key AS `key`, @value AS value) AS source "
            "ON target.`key` = source.`key` "
            "WHEN MATCHED THEN UPDATE SET value = source.value, update_time = CURRENT_TIMESTAMP() "
            "WHEN NOT MATCHED THEN INSERT (`key`, value, update_time) "
            "VALUES (source.`key`, source.value, CURRENT_TIMESTAMP())"
        )
        return sql, {"key": key, "value": value}

    def _build_delete_expired_events_sql_and_params(
        self, before: datetime, app_name: str | None
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        events_ref = qualify_store_table(store._config, store._events_table)
        _, _, require_filter, _ = _partition_settings(store._config)
        part_filter = _partition_filter_clause("timestamp", require_filter=require_filter)
        where = "timestamp < @before" + (" AND app_name = @app_name" if app_name is not None else "") + part_filter
        params: dict[str, Any] = {"before": parse_utc_timestamp(before).isoformat()}
        if app_name is not None:
            params["app_name"] = app_name
        return f"DELETE FROM {events_ref} WHERE {where}", params

    def _build_delete_idle_sessions_sql_and_params(
        self, updated_before: datetime, app_name: str | None
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        sessions_ref = qualify_store_table(store._config, store._session_table)
        _, _, require_filter, _ = _partition_settings(store._config)
        part_filter = _partition_filter_clause("create_time", require_filter=require_filter)
        where = "update_time < @before" + (" AND app_name = @app_name" if app_name is not None else "") + part_filter
        params: dict[str, Any] = {"before": parse_utc_timestamp(updated_before).isoformat()}
        if app_name is not None:
            params["app_name"] = app_name
        return f"DELETE FROM {sessions_ref} WHERE {where}", params

    def _build_delete_idle_user_states_sql_and_params(
        self, updated_before: datetime, app_name: str | None
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table_ref = qualify_store_table(store._config, store._user_state_table)
        where = "update_time < @before" + (" AND app_name = @app_name" if app_name is not None else "")
        params: dict[str, Any] = {"before": parse_utc_timestamp(updated_before).isoformat()}
        if app_name is not None:
            params["app_name"] = app_name
        return f"DELETE FROM {table_ref} WHERE {where}", params


class BigQuerySyncADKStore(_BigQueryADKSessionMixin, BaseSyncADKStore["BigQuerySyncConfig"]):
    """BigQuery synchronous ADK store using partitioned/clustered tables and GoogleSQL ``MERGE``."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    def __init__(self, config: BigQuerySyncConfig, *, read_config: BigQuerySyncConfig | None = None) -> None:
        """Initialize the synchronous BigQuery ADK store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    def _sessions_table_ddl(self) -> str:
        return self._build_sessions_table_ddl()

    def _events_table_ddl(self) -> str:
        return self._build_events_table_ddl()

    def _app_states_table_ddl(self) -> str:
        return self._build_app_states_table_ddl()

    def _user_states_table_ddl(self) -> str:
        return self._build_user_states_table_ddl()

    def _metadata_table_ddl(self) -> str:
        return self._build_metadata_table_ddl()

    def create_tables(self) -> None:
        """Create all ADK session, event, app-state, user-state, and metadata tables."""
        ddls = [
            self._sessions_table_ddl(),
            self._events_table_ddl(),
            self._app_states_table_ddl(),
            self._user_states_table_ddl(),
            self._metadata_table_ddl(),
        ]
        with self._config.provide_session() as driver:
            for ddl in ddls:
                driver.execute(ddl)

    def _read_app_state(self, app_name: str, *, primary: bool) -> dict[str, Any] | None:
        sql, params = self._build_get_app_state_sql_and_params(app_name)
        try:
            with self._provide_replica_session(primary=primary) as driver:
                row = driver.select_one_or_none(sql, params)
            return (
                _decode_json_dict(row.get("state"), deserializer=self._json_deserializer) if row is not None else None
            )
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return None
            raise

    def _read_user_state(self, app_name: str, user_id: str, *, primary: bool) -> dict[str, Any] | None:
        sql, params = self._build_get_user_state_sql_and_params(app_name, user_id)
        try:
            with self._provide_replica_session(primary=primary) as driver:
                row = driver.select_one_or_none(sql, params)
            return (
                _decode_json_dict(row.get("state"), deserializer=self._json_deserializer) if row is not None else None
            )
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return None
            raise

    def _apply_app_state_delta(self, app_name: str, delta: dict[str, Any]) -> dict[str, Any]:
        existing = self._read_app_state(app_name, primary=True) or {}
        merged = {**existing, **delta}
        self.upsert_app_state(app_name, merged)
        return merged

    def _apply_user_state_delta(self, app_name: str, user_id: str, delta: dict[str, Any]) -> dict[str, Any]:
        existing = self._read_user_state(app_name, user_id, primary=True) or {}
        merged = {**existing, **delta}
        self.upsert_user_state(app_name, user_id, merged)
        return merged

    def create_session(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
        state: dict[str, Any],
        owner_id: Any | None = None,
        *,
        app_state_delta: dict[str, Any] | None = None,
        user_state_delta: dict[str, Any] | None = None,
    ) -> StoredSession:
        """Insert a new ADK session row and apply scoped state deltas."""
        now = datetime.now(UTC)
        session_state = dict(state)
        sql, params = self._build_create_session_sql_and_params(
            session_id, app_name, user_id, session_state, owner_id, now
        )
        with self._config.provide_session() as driver:
            driver.execute(sql, params)
        current_app_state = (
            self._apply_app_state_delta(app_name, app_state_delta)
            if app_state_delta
            else self._read_app_state(app_name, primary=True)
        )
        current_user_state = (
            self._apply_user_state_delta(app_name, user_id, user_state_delta)
            if user_state_delta
            else self._read_user_state(app_name, user_id, primary=True)
        )
        return cast(
            "StoredSession",
            {
                "id": session_id,
                "app_name": app_name,
                "user_id": user_id,
                "state": merge_scoped_state(session_state, current_app_state, current_user_state),
                "create_time": now,
                "update_time": now,
            },
        )

    def get_session(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        *,
        renew_for: int | timedelta | None = None,
    ) -> StoredSession | None:
        """Load an ADK session optionally refreshing its ``update_time``."""
        should_touch = self._is_renewal_requested(renew_for)
        if should_touch:
            touch_sql, touch_params = self._build_touch_session_sql_and_params(app_name, user_id, session_id)
            with self._config.provide_session() as driver:
                driver.execute(touch_sql, touch_params)
        sql, params = self._build_get_session_sql_and_params(app_name, user_id, session_id)
        try:
            with self._provide_replica_session(primary=renew_for is not None) as driver:
                row = driver.select_one_or_none(sql, params)
            return _row_to_stored_session(dict(row), deserializer=self._json_deserializer) if row is not None else None
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return None
            raise

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Update JSON state and ``update_time`` for an existing ADK session."""
        sql, params = self._build_update_session_state_sql_and_params(app_name, user_id, session_id, state)
        with self._config.provide_session() as driver:
            driver.execute(sql, params)

    def list_sessions(
        self,
        app_name: str,
        user_id: str | None = None,
        *,
        order_by: SessionOrderBy = "update_time",
        descending: bool = True,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[StoredSession]:
        """List ADK sessions filtered by application and optional user."""
        built = self._build_list_sessions_sql_and_params(
            app_name, user_id, order_by=order_by, descending=descending, limit=limit, offset=offset
        )
        if built is None:
            return []
        sql, params = built
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, params)
            return [_row_to_stored_session(dict(row), deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return []
            raise

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete a session and its associated events."""
        events_sql, sessions_sql, params = self._build_delete_session_sqls_and_params(app_name, user_id, session_id)
        with self._config.provide_session() as driver:
            driver.execute(events_sql, params)
            driver.execute(sessions_sql, params)

    def append_event(self, event_record: StoredEvent) -> None:
        """Append a single ADK event row."""
        sql, params = self._build_append_event_sql_and_params(event_record)
        with self._config.provide_session() as driver:
            driver.execute(sql, params)

    def append_event_and_update_state(
        self,
        event_record: StoredEvent,
        app_name: str,
        user_id: str,
        session_id: str,
        state: dict[str, Any],
        *,
        app_state_delta: dict[str, Any] | None = None,
        user_state_delta: dict[str, Any] | None = None,
    ) -> StoredSession:
        """Append an event and update session, app, and user state deltas."""
        self._validate_event_scope(event_record, app_name, user_id, session_id)
        self.update_session_state(app_name, user_id, session_id, state)
        get_sql, get_params = self._build_get_session_sql_and_params(app_name, user_id, session_id)
        with self._config.provide_session() as driver:
            row = driver.select_one_or_none(get_sql, get_params)
        if row is None:
            msg = f"Session {session_id} not found during append_event_and_update_state."
            raise ValueError(msg)
        self.append_event(event_record)
        merged_app = self._apply_app_state_delta(app_name, app_state_delta) if app_state_delta else None
        merged_user = self._apply_user_state_delta(app_name, user_id, user_state_delta) if user_state_delta else None
        return _row_to_stored_session(
            dict(row),
            deserializer=self._json_deserializer,
            app_state=merged_app,
            user_state=merged_user,
            merge_scopes=True,
        )

    def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> list[StoredEvent]:
        """Fetch chronological ADK events for a session."""
        built = self._build_get_events_sql_and_params(app_name, user_id, session_id, after_timestamp, limit)
        if built is None:
            return []
        sql, params = built
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, params)
            return [_row_to_stored_event(dict(row), deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return []
            raise

    def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Load shared application-scoped ADK state."""
        return self._read_app_state(app_name, primary=False)

    def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Upsert shared application-scoped ADK state using GoogleSQL ``MERGE``."""
        sql, params = self._build_upsert_app_state_sql_and_params(app_name, state)
        with self._config.provide_session() as driver:
            driver.execute(sql, params)

    def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Load user-scoped ADK state for an application."""
        return self._read_user_state(app_name, user_id, primary=False)

    def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Upsert user-scoped ADK state using GoogleSQL ``MERGE``."""
        sql, params = self._build_upsert_user_state_sql_and_params(app_name, user_id, state)
        with self._config.provide_session() as driver:
            driver.execute(sql, params)

    def get_metadata(self, key: str) -> str | None:
        """Retrieve a scalar ADK store metadata value."""
        sql, params = self._build_get_metadata_sql_and_params(key)
        try:
            with self._provide_replica_session(primary=False) as driver:
                row = driver.select_one_or_none(sql, params)
            return str(row["value"]) if row is not None and row.get("value") is not None else None
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return None
            raise

    def set_metadata(self, key: str, value: str) -> None:
        """Persist a scalar ADK store metadata value using ``MERGE``."""
        sql, params = self._build_set_metadata_sql_and_params(key, value)
        with self._config.provide_session() as driver:
            driver.execute(sql, params)

    def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Delete ADK events older than ``before``."""
        sql, params = self._build_delete_expired_events_sql_and_params(before, app_name)
        try:
            with self._config.provide_session() as driver:
                res = driver.execute(sql, params)
            return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return 0
            raise

    def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete ADK sessions inactive since ``updated_before``."""
        sql, params = self._build_delete_idle_sessions_sql_and_params(updated_before, app_name)
        try:
            with self._config.provide_session() as driver:
                res = driver.execute(sql, params)
            return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return 0
            raise

    def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete user state rows inactive since ``updated_before``."""
        sql, params = self._build_delete_idle_user_states_sql_and_params(updated_before, app_name)
        try:
            with self._config.provide_session() as driver:
                res = driver.execute(sql, params)
            return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return 0
            raise


class BigQueryAsyncADKStore(_BigQueryADKSessionMixin, BaseAsyncADKStore["BigQueryAsyncConfig"]):
    """BigQuery asynchronous ADK store using partitioned/clustered tables and GoogleSQL ``MERGE``."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    def __init__(self, config: BigQueryAsyncConfig, *, read_config: BigQueryAsyncConfig | None = None) -> None:
        """Initialize the asynchronous BigQuery ADK store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    async def _sessions_table_ddl(self) -> str:
        return self._build_sessions_table_ddl()

    async def _events_table_ddl(self) -> str:
        return self._build_events_table_ddl()

    async def _app_states_table_ddl(self) -> str:
        return self._build_app_states_table_ddl()

    async def _user_states_table_ddl(self) -> str:
        return self._build_user_states_table_ddl()

    async def _metadata_table_ddl(self) -> str:
        return self._build_metadata_table_ddl()

    async def create_tables(self) -> None:
        """Create all ADK tables asynchronously."""
        ddls = [
            await self._sessions_table_ddl(),
            await self._events_table_ddl(),
            await self._app_states_table_ddl(),
            await self._user_states_table_ddl(),
            await self._metadata_table_ddl(),
        ]
        async with self._config.provide_session() as driver:
            for ddl in ddls:
                await driver.execute(ddl)

    async def _read_app_state(self, app_name: str, *, primary: bool) -> dict[str, Any] | None:
        sql, params = self._build_get_app_state_sql_and_params(app_name)
        try:
            async with self._provide_replica_session(primary=primary) as driver:
                row = await driver.select_one_or_none(sql, params)
            return (
                _decode_json_dict(row.get("state"), deserializer=self._json_deserializer) if row is not None else None
            )
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return None
            raise

    async def _read_user_state(self, app_name: str, user_id: str, *, primary: bool) -> dict[str, Any] | None:
        sql, params = self._build_get_user_state_sql_and_params(app_name, user_id)
        try:
            async with self._provide_replica_session(primary=primary) as driver:
                row = await driver.select_one_or_none(sql, params)
            return (
                _decode_json_dict(row.get("state"), deserializer=self._json_deserializer) if row is not None else None
            )
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return None
            raise

    async def _apply_app_state_delta(self, app_name: str, delta: dict[str, Any]) -> dict[str, Any]:
        existing = await self._read_app_state(app_name, primary=True) or {}
        merged = {**existing, **delta}
        await self.upsert_app_state(app_name, merged)
        return merged

    async def _apply_user_state_delta(self, app_name: str, user_id: str, delta: dict[str, Any]) -> dict[str, Any]:
        existing = await self._read_user_state(app_name, user_id, primary=True) or {}
        merged = {**existing, **delta}
        await self.upsert_user_state(app_name, user_id, merged)
        return merged

    async def create_session(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
        state: dict[str, Any],
        owner_id: Any | None = None,
        *,
        app_state_delta: dict[str, Any] | None = None,
        user_state_delta: dict[str, Any] | None = None,
    ) -> StoredSession:
        """Insert a new ADK session row asynchronously and apply scoped state deltas."""
        now = datetime.now(UTC)
        session_state = dict(state)
        sql, params = self._build_create_session_sql_and_params(
            session_id, app_name, user_id, session_state, owner_id, now
        )
        async with self._config.provide_session() as driver:
            await driver.execute(sql, params)
        current_app_state = (
            await self._apply_app_state_delta(app_name, app_state_delta)
            if app_state_delta
            else await self._read_app_state(app_name, primary=True)
        )
        current_user_state = (
            await self._apply_user_state_delta(app_name, user_id, user_state_delta)
            if user_state_delta
            else await self._read_user_state(app_name, user_id, primary=True)
        )
        return cast(
            "StoredSession",
            {
                "id": session_id,
                "app_name": app_name,
                "user_id": user_id,
                "state": merge_scoped_state(session_state, current_app_state, current_user_state),
                "create_time": now,
                "update_time": now,
            },
        )

    async def get_session(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        *,
        renew_for: int | timedelta | None = None,
    ) -> StoredSession | None:
        """Load an ADK session asynchronously."""
        should_touch = self._is_renewal_requested(renew_for)
        if should_touch:
            touch_sql, touch_params = self._build_touch_session_sql_and_params(app_name, user_id, session_id)
            async with self._config.provide_session() as driver:
                await driver.execute(touch_sql, touch_params)
        sql, params = self._build_get_session_sql_and_params(app_name, user_id, session_id)
        try:
            async with self._provide_replica_session(primary=renew_for is not None) as driver:
                row = await driver.select_one_or_none(sql, params)
            return _row_to_stored_session(dict(row), deserializer=self._json_deserializer) if row is not None else None
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return None
            raise

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Update JSON session state asynchronously."""
        sql, params = self._build_update_session_state_sql_and_params(app_name, user_id, session_id, state)
        async with self._config.provide_session() as driver:
            await driver.execute(sql, params)

    async def list_sessions(
        self,
        app_name: str,
        user_id: str | None = None,
        *,
        order_by: SessionOrderBy = "update_time",
        descending: bool = True,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[StoredSession]:
        """List ADK sessions asynchronously."""
        built = self._build_list_sessions_sql_and_params(
            app_name, user_id, order_by=order_by, descending=descending, limit=limit, offset=offset
        )
        if built is None:
            return []
        sql, params = built
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, params)
            return [_row_to_stored_session(dict(row), deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return []
            raise

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete a session and its events asynchronously."""
        events_sql, sessions_sql, params = self._build_delete_session_sqls_and_params(app_name, user_id, session_id)
        async with self._config.provide_session() as driver:
            await driver.execute(events_sql, params)
            await driver.execute(sessions_sql, params)

    async def append_event(self, event_record: StoredEvent) -> None:
        """Append an ADK event row asynchronously."""
        sql, params = self._build_append_event_sql_and_params(event_record)
        async with self._config.provide_session() as driver:
            await driver.execute(sql, params)

    async def append_event_and_update_state(
        self,
        event_record: StoredEvent,
        app_name: str,
        user_id: str,
        session_id: str,
        state: dict[str, Any],
        *,
        app_state_delta: dict[str, Any] | None = None,
        user_state_delta: dict[str, Any] | None = None,
    ) -> StoredSession:
        """Append an event and update session, app, and user state deltas asynchronously."""
        self._validate_event_scope(event_record, app_name, user_id, session_id)
        await self.update_session_state(app_name, user_id, session_id, state)
        get_sql, get_params = self._build_get_session_sql_and_params(app_name, user_id, session_id)
        async with self._config.provide_session() as driver:
            row = await driver.select_one_or_none(get_sql, get_params)
        if row is None:
            msg = f"Session {session_id} not found during append_event_and_update_state."
            raise ValueError(msg)
        await self.append_event(event_record)
        merged_app = await self._apply_app_state_delta(app_name, app_state_delta) if app_state_delta else None
        merged_user = (
            await self._apply_user_state_delta(app_name, user_id, user_state_delta) if user_state_delta else None
        )
        return _row_to_stored_session(
            dict(row),
            deserializer=self._json_deserializer,
            app_state=merged_app,
            user_state=merged_user,
            merge_scopes=True,
        )

    async def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> list[StoredEvent]:
        """Fetch chronological ADK events asynchronously."""
        built = self._build_get_events_sql_and_params(app_name, user_id, session_id, after_timestamp, limit)
        if built is None:
            return []
        sql, params = built
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, params)
            return [_row_to_stored_event(dict(row), deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return []
            raise

    async def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Load shared application state asynchronously."""
        return await self._read_app_state(app_name, primary=False)

    async def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Upsert shared application state asynchronously using ``MERGE``."""
        sql, params = self._build_upsert_app_state_sql_and_params(app_name, state)
        async with self._config.provide_session() as driver:
            await driver.execute(sql, params)

    async def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Load user-scoped ADK state asynchronously."""
        return await self._read_user_state(app_name, user_id, primary=False)

    async def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Upsert user-scoped ADK state asynchronously using ``MERGE``."""
        sql, params = self._build_upsert_user_state_sql_and_params(app_name, user_id, state)
        async with self._config.provide_session() as driver:
            await driver.execute(sql, params)

    async def get_metadata(self, key: str) -> str | None:
        """Retrieve a scalar ADK store metadata value asynchronously."""
        sql, params = self._build_get_metadata_sql_and_params(key)
        try:
            async with self._provide_replica_session(primary=False) as driver:
                row = await driver.select_one_or_none(sql, params)
            return str(row["value"]) if row is not None and row.get("value") is not None else None
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return None
            raise

    async def set_metadata(self, key: str, value: str) -> None:
        """Persist a scalar ADK store metadata value asynchronously using ``MERGE``."""
        sql, params = self._build_set_metadata_sql_and_params(key, value)
        async with self._config.provide_session() as driver:
            await driver.execute(sql, params)

    async def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Delete expired ADK events asynchronously."""
        sql, params = self._build_delete_expired_events_sql_and_params(before, app_name)
        try:
            async with self._config.provide_session() as driver:
                res = await driver.execute(sql, params)
            return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return 0
            raise

    async def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete idle ADK sessions asynchronously."""
        sql, params = self._build_delete_idle_sessions_sql_and_params(updated_before, app_name)
        try:
            async with self._config.provide_session() as driver:
                res = await driver.execute(sql, params)
            return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return 0
            raise

    async def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete idle user state rows asynchronously."""
        sql, params = self._build_delete_idle_user_states_sql_and_params(updated_before, app_name)
        try:
            async with self._config.provide_session() as driver:
                res = await driver.execute(sql, params)
            return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return 0
            raise


def _memory_ddl_statements(config: Any, memory_table: str, owner_id_ddl: str | None, *, use_fts: bool) -> list[str]:
    with_partitioning, expiration_days, require_filter, _ = _partition_settings(config)
    if expiration_days is None:
        adk = _extract_adk_extension(config)
        expiration_days = positive_int_or_none(adk.get("memory_retention_days"), "memory_retention_days")
    part_sql, opt_sql = bigquery_partition_clauses(
        "timestamp",
        enabled=with_partitioning,
        expiration_days=expiration_days,
        require_filter=require_filter,
        cluster_by=("app_name", "user_id", "session_id"),
    )
    table_ref = qualify_store_table(config, memory_table)
    owner_col = f"{owner_id_ddl}, " if owner_id_ddl else ""
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {table_ref} ("
        "id STRING NOT NULL, "
        "session_id STRING NOT NULL, "
        "app_name STRING NOT NULL, "
        "user_id STRING NOT NULL, "
        f"{owner_col}"
        "scope STRING NOT NULL DEFAULT 'user', "
        "event_id STRING NOT NULL, "
        "author STRING, "
        "timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(), "
        "content_json JSON NOT NULL, "
        "content_text STRING NOT NULL, "
        "metadata_json JSON, "
        "inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(), "
        "embedding ARRAY<FLOAT64>"
        f"){part_sql}{opt_sql}"
    )
    statements = [ddl]
    if use_fts:
        idx_name = quote_bigquery_identifier(f"{memory_table.replace('.', '_')}_fts_idx")
        statements.append(f"CREATE SEARCH INDEX IF NOT EXISTS {idx_name} ON {table_ref}(content_text)")
    return statements


def _scope_sql_and_params(
    app_name: str,
    user_id: str,
    scope_filter: str,
    *,
    require_partition_filter: bool = False,
) -> tuple[str, dict[str, Any]]:
    params: dict[str, Any] = {"app_name": app_name, "user_id": user_id}
    if scope_filter == "user":
        where = "app_name = @app_name AND user_id = @user_id AND scope = 'user'"
    elif scope_filter == "app":
        where = "app_name = @app_name AND scope = 'app'"
    else:
        where = "app_name = @app_name AND (user_id = @user_id OR scope = 'app')"
    where += _partition_filter_clause("timestamp", require_filter=require_partition_filter)
    return where, params


class _BigQueryADKMemoryMixin:
    """Shared SQL generation for BigQuery ``VECTOR_SEARCH`` and ``SEARCH`` ADK memory stores."""

    __slots__ = ()

    _max_table_name_parts: ClassVar[int] = 3
    adapter_name: ClassVar[str | None] = "vertebra_bigquery"
    extension_config_options: ClassVar[frozenset[str]] = _BIGQUERY_ADK_EXTENSION_OPTIONS

    def _build_memory_table_ddl(self) -> list[str]:
        store = cast("Any", self)
        return _memory_ddl_statements(
            store._config,
            store._memory_table,
            store._owner_id_column_ddl,
            use_fts=store._use_fts,
        )

    def _drop_memory_table_sql(self) -> list[str]:
        store = cast("Any", self)
        return [f"DROP TABLE IF EXISTS {qualify_store_table(store._config, store._memory_table)}"]

    def build_vector_index_sql(self, index_name: str | None = None) -> str:
        """Return a GoogleSQL ``CREATE VECTOR INDEX`` statement for the memory embedding column."""
        store = cast("Any", self)
        table_name = store._memory_table
        table_ref = qualify_store_table(store._config, table_name)
        resolved = quote_bigquery_identifier(index_name or f"{table_name.replace('.', '_')}_vec_idx")
        return (
            f"CREATE VECTOR INDEX IF NOT EXISTS {resolved} ON {table_ref}(embedding) "
            "OPTIONS(index_type = 'IVF', distance_type = 'COSINE')"
        )

    def build_search_index_sql(self, index_name: str | None = None) -> str:
        """Return a GoogleSQL ``CREATE SEARCH INDEX`` statement for full-text memory content."""
        store = cast("Any", self)
        table_name = store._memory_table
        table_ref = qualify_store_table(store._config, table_name)
        resolved = quote_bigquery_identifier(index_name or f"{table_name.replace('.', '_')}_fts_idx")
        return f"CREATE SEARCH INDEX IF NOT EXISTS {resolved} ON {table_ref}(content_text)"

    def build_vector_search_sql(
        self,
        *,
        app_name: str,
        user_id: str,
        scope_filter: str = "all",
        top_k: int = 20,
    ) -> tuple[str, dict[str, Any]]:
        """Build a GoogleSQL ``VECTOR_SEARCH`` query with ``top_k`` and scope filtering."""
        store = cast("Any", self)
        table_ref = qualify_store_table(store._config, store._memory_table)
        _, _, require_filter, _ = _partition_settings(store._config)
        where, params = _scope_sql_and_params(app_name, user_id, scope_filter, require_partition_filter=require_filter)
        params["top_k"] = top_k
        sql = (
            "SELECT base.id, base.session_id, base.app_name, base.user_id, base.scope, base.event_id, "
            "base.author, base.timestamp, base.content_json, base.content_text, base.metadata_json, "
            "base.inserted_at, base.embedding, distance "
            f"FROM VECTOR_SEARCH((SELECT * FROM {table_ref} WHERE {where}), "
            "'embedding', (SELECT @query_embedding AS embedding), top_k => @top_k, distance_type => 'COSINE') "
            "ORDER BY distance ASC LIMIT @top_k"
        )
        return sql, params

    def build_fts_search_sql(
        self,
        *,
        app_name: str,
        user_id: str,
        query: str,
        scope_filter: str = "all",
        top_k: int = 20,
    ) -> tuple[str, dict[str, Any]]:
        """Build a GoogleSQL ``SEARCH`` full-text query over ``content_text``."""
        store = cast("Any", self)
        table_ref = qualify_store_table(store._config, store._memory_table)
        _, _, require_filter, _ = _partition_settings(store._config)
        where, params = _scope_sql_and_params(app_name, user_id, scope_filter, require_partition_filter=require_filter)
        params["query"] = query
        params["top_k"] = top_k
        sql = (
            "SELECT id, session_id, app_name, user_id, scope, event_id, author, timestamp, "
            "content_json, content_text, metadata_json, inserted_at, embedding "
            f"FROM {table_ref} WHERE {where} AND SEARCH(content_text, @query) "
            "ORDER BY timestamp DESC LIMIT @top_k"
        )
        return sql, params

    def build_text_search_sql(
        self,
        *,
        app_name: str,
        user_id: str,
        query: str,
        scope_filter: str = "all",
        top_k: int = 20,
    ) -> tuple[str, dict[str, Any]]:
        """Build a GoogleSQL case-insensitive ``LIKE`` query ordered by ``timestamp DESC``."""
        store = cast("Any", self)
        table_ref = qualify_store_table(store._config, store._memory_table)
        _, _, require_filter, _ = _partition_settings(store._config)
        where, params = _scope_sql_and_params(app_name, user_id, scope_filter, require_partition_filter=require_filter)
        stripped = query.strip()
        params["pattern"] = f"%{stripped}%" if stripped else "%"
        params["top_k"] = top_k
        sql = (
            "SELECT id, session_id, app_name, user_id, scope, event_id, author, timestamp, "
            "content_json, content_text, metadata_json, inserted_at, embedding "
            f"FROM {table_ref} WHERE {where} AND LOWER(content_text) LIKE LOWER(@pattern) "
            "ORDER BY timestamp DESC LIMIT @top_k"
        )
        return sql, params

    def _build_insert_memory_sql_and_params(
        self, entry: StoredMemory, owner_id: object | None
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table_ref = qualify_store_table(store._config, store._memory_table)
        owner_col = quote_bigquery_identifier(store._owner_id_column_name) if store._owner_id_column_name else None
        ts = parse_utc_timestamp(entry["timestamp"]).isoformat()
        ins_ts = parse_utc_timestamp(entry["inserted_at"]).isoformat()
        emb_raw = entry.get("embedding")
        emb = list(emb_raw) if emb_raw is not None else None
        params: dict[str, Any] = {
            "id": entry["id"],
            "session_id": entry["session_id"],
            "app_name": entry["app_name"],
            "user_id": entry["user_id"],
            "scope": entry.get("scope", "user"),
            "event_id": entry["event_id"],
            "author": entry.get("author"),
            "timestamp": ts,
            "content_json": _encode_json_text(entry.get("content_json", {}), serializer=store._json_serializer),
            "content_text": entry.get("content_text", ""),
            "metadata_json": (
                _encode_json_text(entry["metadata_json"], serializer=store._json_serializer)
                if entry.get("metadata_json") is not None
                else None
            ),
            "inserted_at": ins_ts,
            "embedding": emb,
        }
        if owner_col:
            params["owner_id"] = owner_id
            sql = (
                f"MERGE INTO {table_ref} AS target "
                "USING (SELECT @id AS id) AS source ON target.id = source.id "
                "WHEN NOT MATCHED THEN INSERT "
                f"(id, session_id, app_name, user_id, {owner_col}, scope, event_id, author, "
                "timestamp, content_json, content_text, metadata_json, inserted_at, embedding) "
                "VALUES (@id, @session_id, @app_name, @user_id, @owner_id, @scope, @event_id, @author, "
                "@timestamp, PARSE_JSON(@content_json), @content_text, "
                "PARSE_JSON(@metadata_json), @inserted_at, @embedding)"
            )
            return sql, params
        sql = (
            f"MERGE INTO {table_ref} AS target "
            "USING (SELECT @id AS id) AS source ON target.id = source.id "
            "WHEN NOT MATCHED THEN INSERT "
            "(id, session_id, app_name, user_id, scope, event_id, author, "
            "timestamp, content_json, content_text, metadata_json, inserted_at, embedding) "
            "VALUES (@id, @session_id, @app_name, @user_id, @scope, @event_id, @author, "
            "@timestamp, PARSE_JSON(@content_json), @content_text, "
            "PARSE_JSON(@metadata_json), @inserted_at, @embedding)"
        )
        return sql, params

    def _build_search_sql_and_params(
        self,
        query: str,
        app_name: str,
        user_id: str,
        limit: int | None,
        scope_filter: Literal["all", "user", "app"],
        embedding: Sequence[float] | None,
    ) -> tuple[str, dict[str, Any]] | None:
        store = cast("Any", self)
        if not store._enabled:
            return None
        has_query = bool(query and query.strip())
        if not has_query and embedding is None:
            return None
        top_k = limit if limit is not None else store._max_results
        if top_k <= 0:
            return None
        if embedding is not None:
            vec_sql, vec_params = self.build_vector_search_sql(
                app_name=app_name, user_id=user_id, scope_filter=scope_filter, top_k=top_k
            )
            vec_params["query_embedding"] = [float(x) for x in embedding]
            return vec_sql, vec_params
        if store._use_fts and has_query:
            return self.build_fts_search_sql(
                app_name=app_name, user_id=user_id, query=query, scope_filter=scope_filter, top_k=top_k
            )
        return self.build_text_search_sql(
            app_name=app_name, user_id=user_id, query=query, scope_filter=scope_filter, top_k=top_k
        )

    def _build_delete_by_session_sql_and_params(
        self, session_id: str, app_name: str, user_id: str
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table_ref = qualify_store_table(store._config, store._memory_table)
        _, _, require_filter, _ = _partition_settings(store._config)
        part_filter = _partition_filter_clause("timestamp", require_filter=require_filter)
        sql = (
            f"DELETE FROM {table_ref} "
            f"WHERE session_id = @session_id AND app_name = @app_name AND user_id = @user_id{part_filter}"
        )
        return sql, {"session_id": session_id, "app_name": app_name, "user_id": user_id}

    def _build_delete_older_than_sql_and_params(
        self, days: int, app_name: str | None, scope: str | None
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table_ref = qualify_store_table(store._config, store._memory_table)
        _, _, require_filter, _ = _partition_settings(store._config)
        cutoff = (datetime.now(UTC) - timedelta(days=days)).isoformat()
        clauses = ["inserted_at < @cutoff"]
        params: dict[str, Any] = {"cutoff": cutoff}
        if app_name is not None:
            clauses.append("app_name = @app_name")
            params["app_name"] = app_name
        if scope is not None:
            clauses.append("scope = @scope")
            params["scope"] = scope
        part_filter = _partition_filter_clause("timestamp", require_filter=require_filter)
        return f"DELETE FROM {table_ref} WHERE {' AND '.join(clauses)}{part_filter}", params


class BigQuerySyncADKMemoryStore(_BigQueryADKMemoryMixin, _BaseSyncADKMemoryStore["BigQuerySyncConfig"]):
    """BigQuery synchronous ADK memory store with ``VECTOR_SEARCH`` and ``SEARCH`` support."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    def __init__(self, config: BigQuerySyncConfig, *, read_config: BigQuerySyncConfig | None = None) -> None:
        """Initialize the synchronous BigQuery ADK memory store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    def _memory_table_ddl(self) -> list[str]:
        return self._build_memory_table_ddl()

    def create_tables(self) -> None:
        """Create the BigQuery memory table and optional search index if enabled."""
        if not self._enabled:
            return
        with self._config.provide_session() as driver:
            for stmt in self._memory_table_ddl():
                driver.execute(stmt)

    def insert_memory_entries(self, entries: list[StoredMemory], owner_id: object | None = None) -> int:
        """Insert memory records using ``MERGE`` deduplication on ``id``."""
        if not self._enabled or not entries:
            return 0
        inserted = 0
        with self._config.provide_session() as driver:
            for entry in entries:
                sql, params = self._build_insert_memory_sql_and_params(entry, owner_id)
                res = driver.execute(sql, params)
                affected = getattr(res, "rows_affected", None)
                inserted += int(affected) if isinstance(affected, int) and affected > 0 else 1
        return inserted

    def search_entries(
        self,
        query: str,
        app_name: str,
        user_id: str,
        limit: int | None = None,
        scope_filter: Literal["all", "user", "app"] = "all",
        embedding: Sequence[float] | None = None,
    ) -> list[StoredMemory]:
        """Search ADK memory entries via GoogleSQL ``VECTOR_SEARCH``, ``SEARCH``, or ``LIKE``."""
        built = self._build_search_sql_and_params(query, app_name, user_id, limit, scope_filter, embedding)
        if built is None:
            return []
        sql, params = built
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, params)
            return [_row_to_stored_memory(dict(row), deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return []
            raise

    def delete_entries_by_session(self, session_id: str, app_name: str, user_id: str) -> int:
        """Delete memory entries associated with ``session_id`` owned by ``app_name`` and ``user_id``."""
        sql, params = self._build_delete_by_session_sql_and_params(session_id, app_name, user_id)
        try:
            with self._config.provide_session() as driver:
                res = driver.execute(sql, params)
            return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return 0
            raise

    def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
        """Delete memory entries inserted more than ``days`` ago."""
        sql, params = self._build_delete_older_than_sql_and_params(days, app_name, scope)
        try:
            with self._config.provide_session() as driver:
                res = driver.execute(sql, params)
            return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return 0
            raise


class BigQueryAsyncADKMemoryStore(_BigQueryADKMemoryMixin, _BaseAsyncADKMemoryStore["BigQueryAsyncConfig"]):
    """BigQuery asynchronous ADK memory store with ``VECTOR_SEARCH`` and ``SEARCH`` support."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    def __init__(self, config: BigQueryAsyncConfig, *, read_config: BigQueryAsyncConfig | None = None) -> None:
        """Initialize the asynchronous BigQuery ADK memory store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    async def _memory_table_ddl(self) -> list[str]:
        return self._build_memory_table_ddl()

    async def create_tables(self) -> None:
        """Create the BigQuery memory table and optional search index asynchronously."""
        if not self._enabled:
            return
        ddls = await self._memory_table_ddl()
        async with self._config.provide_session() as driver:
            for stmt in ddls:
                await driver.execute(stmt)

    async def insert_memory_entries(self, entries: list[StoredMemory], owner_id: object | None = None) -> int:
        """Insert memory records asynchronously using ``MERGE`` deduplication on ``id``."""
        if not self._enabled or not entries:
            return 0
        inserted = 0
        async with self._config.provide_session() as driver:
            for entry in entries:
                sql, params = self._build_insert_memory_sql_and_params(entry, owner_id)
                res = await driver.execute(sql, params)
                affected = getattr(res, "rows_affected", None)
                inserted += int(affected) if isinstance(affected, int) and affected > 0 else 1
        return inserted

    async def search_entries(
        self,
        query: str,
        app_name: str,
        user_id: str,
        limit: int | None = None,
        scope_filter: Literal["all", "user", "app"] = "all",
        embedding: Sequence[float] | None = None,
    ) -> list[StoredMemory]:
        """Search ADK memory entries asynchronously via GoogleSQL ``VECTOR_SEARCH``, ``SEARCH``, or ``LIKE``."""
        built = self._build_search_sql_and_params(query, app_name, user_id, limit, scope_filter, embedding)
        if built is None:
            return []
        sql, params = built
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, params)
            return [_row_to_stored_memory(dict(row), deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return []
            raise

    async def delete_entries_by_session(self, session_id: str, app_name: str, user_id: str) -> int:
        """Delete memory entries associated with ``session_id`` owned by ``app_name`` and ``user_id``."""
        sql, params = self._build_delete_by_session_sql_and_params(session_id, app_name, user_id)
        try:
            async with self._config.provide_session() as driver:
                res = await driver.execute(sql, params)
            return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return 0
            raise

    async def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
        """Delete aged memory entries asynchronously."""
        sql, params = self._build_delete_older_than_sql_and_params(days, app_name, scope)
        try:
            async with self._config.provide_session() as driver:
                res = await driver.execute(sql, params)
            return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return 0
            raise


def _artifact_ddl(config: Any, artifact_table: str) -> str:
    with_partitioning, expiration_days, require_filter, _ = _partition_settings(config)
    part_sql, opt_sql = bigquery_partition_clauses(
        "created_at",
        enabled=with_partitioning,
        expiration_days=expiration_days,
        require_filter=require_filter,
        cluster_by=("app_name", "user_id", "filename"),
    )
    table_ref = qualify_store_table(config, artifact_table)
    return (
        f"CREATE TABLE IF NOT EXISTS {table_ref} ("
        "app_name STRING NOT NULL, "
        "user_id STRING NOT NULL, "
        "session_id STRING, "
        "filename STRING NOT NULL, "
        "version INT64 NOT NULL, "
        "mime_type STRING, "
        "canonical_uri STRING NOT NULL, "
        "custom_metadata JSON, "
        "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()"
        f"){part_sql}{opt_sql}"
    )


def _artifact_scope_where(
    app_name: str,
    user_id: str,
    filename: str,
    session_id: str | None = None,
    *,
    require_partition_filter: bool = False,
) -> tuple[str, dict[str, Any]]:
    params: dict[str, Any] = {"app_name": app_name, "user_id": user_id, "filename": filename}
    if session_id is None:
        where = "app_name = @app_name AND user_id = @user_id AND filename = @filename AND session_id IS NULL"
    else:
        params["session_id"] = session_id
        where = "app_name = @app_name AND user_id = @user_id AND filename = @filename AND session_id = @session_id"
    where += _partition_filter_clause("created_at", require_filter=require_partition_filter)
    return where, params


class _BigQueryADKArtifactMixin:
    """Shared ObjectRef->ArtifactReference and ObjectStoreBackend wiring for BigQuery ADK artifacts."""

    __slots__ = ()

    adapter_name: ClassVar[str | None] = "vertebra_bigquery"
    extension_config_options: ClassVar[frozenset[str]] = _BIGQUERY_ADK_EXTENSION_OPTIONS

    def _init_storage(
        self,
        config: Any,
        storage_backend: ObjectStoreBackend | str | None = None,
    ) -> ObjectStoreBackend | None:
        adk = _extract_adk_extension(config)
        uri = adk.get("artifact_storage_uri")
        return resolve_storage_backend(
            storage_backend,
            getattr(config, "driver_features", None),
            artifact_storage_uri=str(uri) if isinstance(uri, str) else None,
        )

    @staticmethod
    def to_artifact_reference(ref: ObjectRef) -> ArtifactReference:
        """Translate a native BigQuery ``ObjectRef`` to a storage-neutral ``ArtifactReference``."""
        return object_ref_to_artifact_reference(ref)

    @staticmethod
    def _stored_artifact_to_ref(record: StoredArtifact, size: int | None = None) -> ArtifactReference:
        return ArtifactReference(
            uri=record["canonical_uri"],
            version=int(record["version"]),
            content_type=record.get("mime_type"),
            size=size,
            updated=record.get("created_at"),
            signed_url=None,
        )

    @staticmethod
    def _object_key(app_name: str, user_id: str, filename: str, session_id: str | None, version: int) -> str:
        scope = session_id if session_id is not None else "user"
        return f"adk-artifacts/{app_name}/{user_id}/{scope}/{filename}/v{version}"

    def _build_insert_artifact_sql_and_params(self, record: StoredArtifact) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table_ref = qualify_store_table(store._config, store._artifact_table)
        sql = (
            f"INSERT INTO {table_ref} "
            "(app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at) "
            "VALUES (@app_name, @user_id, @session_id, @filename, @version, @mime_type, @canonical_uri, "
            "PARSE_JSON(@custom_metadata), @created_at)"
        )
        params = {
            "app_name": record["app_name"],
            "user_id": record["user_id"],
            "session_id": record.get("session_id"),
            "filename": record["filename"],
            "version": int(record["version"]),
            "mime_type": record.get("mime_type"),
            "canonical_uri": record["canonical_uri"],
            "custom_metadata": (
                _encode_json_text(record["custom_metadata"], serializer=store._json_serializer)
                if record.get("custom_metadata") is not None
                else None
            ),
            "created_at": parse_utc_timestamp(record.get("created_at")).isoformat(),
        }
        return sql, params

    def _build_get_artifact_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None,
        version: int | None,
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table_ref = qualify_store_table(store._config, store._artifact_table)
        _, _, require_filter, _ = _partition_settings(store._config)
        where, params = _artifact_scope_where(
            app_name, user_id, filename, session_id, require_partition_filter=require_filter
        )
        if version is not None:
            where += " AND version = @version"
            params["version"] = int(version)
        sql = (
            "SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
            f"custom_metadata, created_at FROM {table_ref} WHERE {where} ORDER BY version DESC LIMIT 1"
        )
        return sql, params

    def _build_list_artifact_keys_sql_and_params(
        self, app_name: str, user_id: str, session_id: str | None
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table_ref = qualify_store_table(store._config, store._artifact_table)
        _, _, require_filter, _ = _partition_settings(store._config)
        part_filter = _partition_filter_clause("created_at", require_filter=require_filter)
        params: dict[str, Any] = {"app_name": app_name, "user_id": user_id}
        if session_id is None:
            where = f"app_name = @app_name AND user_id = @user_id AND session_id IS NULL{part_filter}"
        else:
            where = (
                "app_name = @app_name AND user_id = @user_id "
                f"AND (session_id = @session_id OR session_id IS NULL){part_filter}"
            )
            params["session_id"] = session_id
        return f"SELECT DISTINCT filename FROM {table_ref} WHERE {where} ORDER BY filename ASC", params

    def _build_list_artifact_versions_sql_and_params(
        self, app_name: str, user_id: str, filename: str, session_id: str | None
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table_ref = qualify_store_table(store._config, store._artifact_table)
        _, _, require_filter, _ = _partition_settings(store._config)
        where, params = _artifact_scope_where(
            app_name, user_id, filename, session_id, require_partition_filter=require_filter
        )
        sql = (
            "SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
            f"custom_metadata, created_at FROM {table_ref} WHERE {where} ORDER BY version ASC"
        )
        return sql, params

    def _build_delete_artifact_sql_and_params(
        self, app_name: str, user_id: str, filename: str, session_id: str | None
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table_ref = qualify_store_table(store._config, store._artifact_table)
        _, _, require_filter, _ = _partition_settings(store._config)
        where, params = _artifact_scope_where(
            app_name, user_id, filename, session_id, require_partition_filter=require_filter
        )
        return f"DELETE FROM {table_ref} WHERE {where}", params

    def _build_get_next_version_sql_and_params(
        self, app_name: str, user_id: str, filename: str, session_id: str | None
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table_ref = qualify_store_table(store._config, store._artifact_table)
        _, _, require_filter, _ = _partition_settings(store._config)
        where, params = _artifact_scope_where(
            app_name, user_id, filename, session_id, require_partition_filter=require_filter
        )
        return f"SELECT COALESCE(MAX(version), -1) AS max_ver FROM {table_ref} WHERE {where}", params

    def _build_delete_older_than_artifacts_sql_and_params(
        self, before: datetime, app_name: str | None
    ) -> tuple[str, str, dict[str, Any]]:
        store = cast("Any", self)
        table_ref = qualify_store_table(store._config, store._artifact_table)
        _, _, require_filter, _ = _partition_settings(store._config)
        cutoff = parse_utc_timestamp(before).isoformat()
        part_filter = _partition_filter_clause("created_at", require_filter=require_filter)
        where = "created_at < @before" + (" AND app_name = @app_name" if app_name is not None else "") + part_filter
        params: dict[str, Any] = {"before": cutoff}
        if app_name is not None:
            params["app_name"] = app_name
        select_sql = (
            "SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
            f"custom_metadata, created_at FROM {table_ref} WHERE {where} ORDER BY created_at ASC, version ASC"
        )
        delete_sql = f"DELETE FROM {table_ref} WHERE {where}"
        return select_sql, delete_sql, params


class BigQuerySyncADKArtifactStore(_BigQueryADKArtifactMixin, _BaseSyncADKArtifactStore["BigQuerySyncConfig"]):
    """BigQuery synchronous ADK artifact metadata store bridged to ``ArtifactReference``."""

    __slots__ = ("_json_deserializer", "_json_serializer", "_storage_backend")

    def __init__(
        self,
        config: BigQuerySyncConfig,
        *,
        storage_backend: ObjectStoreBackend | str | None = None,
        read_config: BigQuerySyncConfig | None = None,
    ) -> None:
        """Initialize the synchronous BigQuery ADK artifact store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        self._storage_backend = self._init_storage(config, storage_backend)

    @property
    def storage_backend(self) -> ObjectStoreBackend | None:
        """Return the configured object storage backend, if any."""
        return self._storage_backend

    def create_table(self) -> None:
        """Create the BigQuery artifact metadata table if missing."""
        ddl = _artifact_ddl(self._config, self._artifact_table)
        with self._config.provide_session() as driver:
            driver.execute(ddl)

    def insert_artifact(self, record: StoredArtifact) -> None:
        """Persist a ``StoredArtifact`` version metadata row."""
        sql, params = self._build_insert_artifact_sql_and_params(record)
        with self._config.provide_session() as driver:
            driver.execute(sql, params)

    def get_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> StoredArtifact | None:
        """Fetch metadata for the requested or latest artifact version."""
        sql, params = self._build_get_artifact_sql_and_params(app_name, user_id, filename, session_id, version)
        try:
            with self._provide_replica_session(primary=False) as driver:
                row = driver.select_one_or_none(sql, params)
            return _row_to_stored_artifact(dict(row), deserializer=self._json_deserializer) if row is not None else None
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return None
            raise

    def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """List distinct artifact filenames within user or session+user scope."""
        sql, params = self._build_list_artifact_keys_sql_and_params(app_name, user_id, session_id)
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, params)
            return [str(row["filename"]) for row in rows if row.get("filename") is not None]
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return []
            raise

    def list_artifact_versions(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[StoredArtifact]:
        """List all artifact version rows ordered ascending."""
        sql, params = self._build_list_artifact_versions_sql_and_params(app_name, user_id, filename, session_id)
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, params)
            return [_row_to_stored_artifact(dict(row), deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return []
            raise

    def delete_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[StoredArtifact]:
        """Delete all metadata rows for an artifact and return the removed records."""
        existing = self.list_artifact_versions(app_name, user_id, filename, session_id)
        sql, params = self._build_delete_artifact_sql_and_params(app_name, user_id, filename, session_id)
        try:
            with self._config.provide_session() as driver:
                driver.execute(sql, params)
            return existing
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return []
            raise

    def get_next_version(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> int:
        """Compute the next zero-based integer artifact version."""
        sql, params = self._build_get_next_version_sql_and_params(app_name, user_id, filename, session_id)
        try:
            with self._provide_replica_session(primary=False) as driver:
                row = driver.select_one_or_none(sql, params)
            max_ver = int(row.get("max_ver", -1)) if row is not None and row.get("max_ver") is not None else -1
            return max_ver + 1
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return 0
            raise

    def delete_artifacts_older_than(
        self,
        before: datetime,
        app_name: str | None = None,
    ) -> list[StoredArtifact]:
        """Delete artifact metadata rows created before ``before`` and return them."""
        select_sql, delete_sql, params = self._build_delete_older_than_artifacts_sql_and_params(before, app_name)
        try:
            with self._config.provide_session() as driver:
                rows = [dict(row) for row in driver.select(select_sql, params)]
                driver.execute(delete_sql, params)
            return [_row_to_stored_artifact(row, deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return []
            raise

    def save_artifact_object(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        data: bytes,
        *,
        session_id: str | None = None,
        mime_type: str = "application/octet-stream",
        custom_metadata: dict[str, Any] | None = None,
    ) -> ArtifactReference:
        """Write binary payload to ``storage_backend``, record SQL metadata, and return ``ArtifactReference``."""
        version = self.get_next_version(app_name, user_id, filename, session_id)
        key = self._object_key(app_name, user_id, filename, session_id, version)
        if self._storage_backend is not None:
            self._storage_backend.write_bytes_sync(key, data)
            base_uri = str(getattr(self._storage_backend, "store_uri", "") or "").rstrip("/")
            canonical_uri = f"{base_uri}/{key}" if base_uri else key
        else:
            canonical_uri = key
        now = datetime.now(UTC)
        record = cast(
            "StoredArtifact",
            {
                "app_name": app_name,
                "user_id": user_id,
                "session_id": session_id,
                "filename": filename,
                "version": version,
                "mime_type": mime_type,
                "canonical_uri": canonical_uri,
                "custom_metadata": custom_metadata,
                "created_at": now,
            },
        )
        self.insert_artifact(record)
        return self._stored_artifact_to_ref(record, size=len(data))

    def get_artifact_reference(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> ArtifactReference | None:
        """Return a storage-neutral ``ArtifactReference`` for a persisted artifact."""
        record = self.get_artifact(app_name, user_id, filename, session_id=session_id, version=version)
        return self._stored_artifact_to_ref(record) if record is not None else None

    def list_object_table_references(
        self,
        config: ObjectTableConfig,
        *,
        store: Any | None = None,
        where: str | None = None,
        limit: int | None = None,
        presign: bool = True,
        expires_in_secs: int = 3600,
    ) -> list[ArtifactReference]:
        """List BigQuery Object Table entries translated from native ``ObjectRef`` to ``ArtifactReference``."""
        with self._config.provide_session() as driver:
            refs = driver.list_object_table_refs(
                config,
                store=store,
                where=where,
                limit=limit,
                presign=presign,
                expires_in_secs=expires_in_secs,
            )
        return [self.to_artifact_reference(ref) for ref in refs]


class BigQueryAsyncADKArtifactStore(_BigQueryADKArtifactMixin, _BaseAsyncADKArtifactStore["BigQueryAsyncConfig"]):
    """BigQuery asynchronous ADK artifact metadata store bridged to ``ArtifactReference``."""

    __slots__ = ("_json_deserializer", "_json_serializer", "_storage_backend")

    def __init__(
        self,
        config: BigQueryAsyncConfig,
        *,
        storage_backend: ObjectStoreBackend | str | None = None,
        read_config: BigQueryAsyncConfig | None = None,
    ) -> None:
        """Initialize the asynchronous BigQuery ADK artifact store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        self._storage_backend = self._init_storage(config, storage_backend)

    @property
    def storage_backend(self) -> ObjectStoreBackend | None:
        """Return the configured object storage backend, if any."""
        return self._storage_backend

    async def create_table(self) -> None:
        """Create the BigQuery artifact metadata table asynchronously."""
        ddl = _artifact_ddl(self._config, self._artifact_table)
        async with self._config.provide_session() as driver:
            await driver.execute(ddl)

    async def insert_artifact(self, record: StoredArtifact) -> None:
        """Persist a ``StoredArtifact`` version metadata row asynchronously."""
        sql, params = self._build_insert_artifact_sql_and_params(record)
        async with self._config.provide_session() as driver:
            await driver.execute(sql, params)

    async def get_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> StoredArtifact | None:
        """Fetch metadata asynchronously for the requested or latest artifact version."""
        sql, params = self._build_get_artifact_sql_and_params(app_name, user_id, filename, session_id, version)
        try:
            async with self._provide_replica_session(primary=False) as driver:
                row = await driver.select_one_or_none(sql, params)
            return _row_to_stored_artifact(dict(row), deserializer=self._json_deserializer) if row is not None else None
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return None
            raise

    async def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """List distinct artifact filenames asynchronously."""
        sql, params = self._build_list_artifact_keys_sql_and_params(app_name, user_id, session_id)
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, params)
            return [str(row["filename"]) for row in rows if row.get("filename") is not None]
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return []
            raise

    async def list_artifact_versions(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[StoredArtifact]:
        """List all artifact version rows asynchronously."""
        sql, params = self._build_list_artifact_versions_sql_and_params(app_name, user_id, filename, session_id)
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, params)
            return [_row_to_stored_artifact(dict(row), deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return []
            raise

    async def delete_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[StoredArtifact]:
        """Delete all metadata rows for an artifact asynchronously and return them."""
        existing = await self.list_artifact_versions(app_name, user_id, filename, session_id)
        sql, params = self._build_delete_artifact_sql_and_params(app_name, user_id, filename, session_id)
        try:
            async with self._config.provide_session() as driver:
                await driver.execute(sql, params)
            return existing
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return []
            raise

    async def get_next_version(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> int:
        """Compute the next zero-based integer artifact version asynchronously."""
        sql, params = self._build_get_next_version_sql_and_params(app_name, user_id, filename, session_id)
        try:
            async with self._provide_replica_session(primary=False) as driver:
                row = await driver.select_one_or_none(sql, params)
            max_ver = int(row.get("max_ver", -1)) if row is not None and row.get("max_ver") is not None else -1
            return max_ver + 1
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return 0
            raise

    async def delete_artifacts_older_than(
        self,
        before: datetime,
        app_name: str | None = None,
    ) -> list[StoredArtifact]:
        """Delete aged artifact metadata rows asynchronously and return them."""
        select_sql, delete_sql, params = self._build_delete_older_than_artifacts_sql_and_params(before, app_name)
        try:
            async with self._config.provide_session() as driver:
                raw_rows = await driver.select(select_sql, params)
                rows = [dict(row) for row in raw_rows]
                await driver.execute(delete_sql, params)
            return [_row_to_stored_artifact(row, deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return []
            raise

    async def save_artifact_object(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        data: bytes,
        *,
        session_id: str | None = None,
        mime_type: str = "application/octet-stream",
        custom_metadata: dict[str, Any] | None = None,
    ) -> ArtifactReference:
        """Write binary payload to ``storage_backend``, record metadata, and return ``ArtifactReference``."""
        version = await self.get_next_version(app_name, user_id, filename, session_id)
        key = self._object_key(app_name, user_id, filename, session_id, version)
        if self._storage_backend is not None:
            await self._storage_backend.write_bytes_async(key, data)
            base_uri = str(getattr(self._storage_backend, "store_uri", "") or "").rstrip("/")
            canonical_uri = f"{base_uri}/{key}" if base_uri else key
        else:
            canonical_uri = key
        now = datetime.now(UTC)
        record = cast(
            "StoredArtifact",
            {
                "app_name": app_name,
                "user_id": user_id,
                "session_id": session_id,
                "filename": filename,
                "version": version,
                "mime_type": mime_type,
                "canonical_uri": canonical_uri,
                "custom_metadata": custom_metadata,
                "created_at": now,
            },
        )
        await self.insert_artifact(record)
        return self._stored_artifact_to_ref(record, size=len(data))

    async def get_artifact_reference(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> ArtifactReference | None:
        """Return a storage-neutral ``ArtifactReference`` asynchronously."""
        record = await self.get_artifact(app_name, user_id, filename, session_id=session_id, version=version)
        return self._stored_artifact_to_ref(record) if record is not None else None

    async def list_object_table_references(
        self,
        config: ObjectTableConfig,
        *,
        store: Any | None = None,
        where: str | None = None,
        limit: int | None = None,
        presign: bool = True,
        expires_in_secs: int = 3600,
    ) -> list[ArtifactReference]:
        """List BigQuery Object Table entries asynchronously translated to ``ArtifactReference``."""
        async with self._config.provide_session() as driver:
            refs = await driver.list_object_table_refs(
                config,
                store=store,
                where=where,
                limit=limit,
                presign=presign,
                expires_in_secs=expires_in_secs,
            )
        return [self.to_artifact_reference(ref) for ref in refs]
