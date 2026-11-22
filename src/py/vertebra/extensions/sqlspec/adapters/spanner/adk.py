"""Cloud Spanner synchronous and asynchronous ADK session, memory, and artifact stores."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, NotRequired, TypedDict, cast

from sqlspec.config import ADKConfig
from sqlspec.extensions.adk import BaseAsyncADKStore, BaseSyncADKStore, StoredEvent, StoredSession
from sqlspec.extensions.adk.store import normalize_session_list_options
from sqlspec.utils.serializers import from_json, to_json

from vertebra.extensions.sqlspec.adapters.spanner.core import (
    is_spanner_table_missing,
    normalize_spanner_dialect,
    object_to_artifact_reference,
    parse_utc_timestamp,
    resolve_storage_backend,
    validate_spanner_identifier,
)
from vertebra.extensions.sqlspec.storage import ObjectStoreBackend
from vertebra.storage import ArtifactReference
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from sqlspec.extensions.adk import SessionOrderBy, StoredMemory
    from sqlspec.extensions.adk.artifact import StoredArtifact
    from sqlspec.extensions.adk.artifact.store import BaseAsyncADKArtifactStore as _BaseAsyncADKArtifactStore
    from sqlspec.extensions.adk.artifact.store import BaseSyncADKArtifactStore as _BaseSyncADKArtifactStore
    from sqlspec.extensions.adk.converters import merge_scoped_state
    from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore as _BaseAsyncADKMemoryStore
    from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore as _BaseSyncADKMemoryStore

    from vertebra.extensions.sqlspec.adapters.spanner.config import SpannerAsyncConfig, SpannerSyncConfig
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

type StoredSessionRecord = StoredSession
type StoredEventRecord = StoredEvent
type StoredMemoryRecord = StoredMemory
type StoredArtifactRecord = StoredArtifact

__all__ = (
    "SpannerADKConfig",
    "SpannerADKRetentionConfig",
    "SpannerAsyncADKArtifactStore",
    "SpannerAsyncADKMemoryStore",
    "SpannerAsyncADKStore",
    "SpannerSyncADKArtifactStore",
    "SpannerSyncADKMemoryStore",
    "SpannerSyncADKStore",
)

_SPANNER_ADK_EXTENSION_OPTIONS: Final[frozenset[str]] = frozenset(
    {
        "enable_hybrid_search",
        "enable_memory_graph",
        "events_table_options",
        "expires_index_options",
        "memory_graph_name",
        "memory_table_options",
        "retention",
        "scann_num_leaves",
        "scann_quantizer",
        "scann_tree_depth",
        "session_table_options",
        "shard_count",
        "use_fts",
        "vector_dimensions",
        "vector_distance_type",
        "vector_index_enabled",
        "vector_index_type",
    }
)

_SPANNER_DISTANCE_FUNCTIONS: Final[dict[str, str]] = {
    "COSINE": "APPROX_COSINE_DISTANCE",
    "EUCLIDEAN": "EUCLIDEAN_DISTANCE",
    "DOT_PRODUCT": "DOT_PRODUCT",
}


class SpannerADKRetentionConfig(TypedDict, total=False):
    """Cloud Spanner ADK row-deletion retention policy settings."""

    session_ttl_seconds: NotRequired[int]
    """Session row retention in seconds."""
    event_ttl_seconds: NotRequired[int]
    """Event row retention in seconds."""
    memory_ttl_seconds: NotRequired[int]
    """Memory row retention in seconds."""
    artifact_ttl_seconds: NotRequired[int]
    """Artifact row retention in seconds."""


class SpannerADKConfig(ADKConfig, total=False):
    """Typed ADK extension configuration keys honored by Cloud Spanner stores."""

    shard_count: NotRequired[int]
    """Generated shard count for hot-key mitigation."""
    session_table_options: NotRequired[str]
    """Raw Spanner OPTIONS clause content for the ADK session table."""
    events_table_options: NotRequired[str]
    """Raw Spanner OPTIONS clause content for the ADK events table."""
    memory_table_options: NotRequired[str]
    """Raw Spanner OPTIONS clause content for the ADK memory table."""
    expires_index_options: NotRequired[str]
    """Raw Spanner OPTIONS clause content for expiration indexes."""
    retention: NotRequired[SpannerADKRetentionConfig]
    """Cloud Spanner row-deletion retention policy settings."""
    use_fts: NotRequired[bool]
    """Enable Cloud Spanner full-text search indexes for memory retrieval."""
    vector_index_type: NotRequired[str]
    """Cloud Spanner vector index type for ADK memory embeddings."""
    vector_dimensions: NotRequired[int | None]
    """Embedding dimensionality for ADK memory vector similarity columns."""
    vector_distance_type: NotRequired[str]
    """Distance metric for the memory vector index ('COSINE', 'EUCLIDEAN', 'DOT_PRODUCT')."""
    vector_index_enabled: NotRequired[bool]
    """Whether to emit CREATE VECTOR INDEX for the memory table."""
    scann_tree_depth: NotRequired[int]
    """ScaNN tree depth for the memory vector index (2 or 3)."""
    scann_num_leaves: NotRequired[int]
    """Number of ScaNN index tree leaves for Cloud Spanner vector search."""
    scann_quantizer: NotRequired[str]
    """Quantizer configuration string for Cloud Spanner ScaNN indexes."""
    enable_hybrid_search: NotRequired[bool]
    """Enable SQL Reciprocal Rank Fusion when both embedding and FTS query are supplied."""
    enable_memory_graph: NotRequired[bool]
    """Emit CREATE OR REPLACE PROPERTY GRAPH over the ADK memory table."""
    memory_graph_name: NotRequired[str]
    """Name of the Spanner Property Graph created over the ADK memory table."""


def _config_dialect(config: Any) -> Literal["googlesql", "spangres"]:
    return normalize_spanner_dialect(
        getattr(config, "spanner_dialect", None) or getattr(config, "dialect", "googlesql")
    )


def _adk_settings(config: Any) -> dict[str, Any]:
    ext = getattr(config, "extension_config", None)
    if not isinstance(ext, Mapping):
        return {}
    adk = ext.get("adk")
    return dict(adk) if isinstance(adk, Mapping) else {}


def _driver_features(config: Any) -> dict[str, Any]:
    features = getattr(config, "driver_features", None)
    return dict(features) if isinstance(features, Mapping) else {}


def _resolve_json_codecs(config: Any) -> tuple[Callable[[Any], str], Callable[[str], Any]]:
    features = _driver_features(config)
    serializer = features.get("json_serializer")
    deserializer = features.get("json_deserializer")
    return (
        cast("Callable[[Any], str]", serializer if callable(serializer) else to_json),
        cast("Callable[[str], Any]", deserializer if callable(deserializer) else from_json),
    )


def _retention_ttl_days(adk: Mapping[str, Any], key: str) -> int | None:
    retention = adk.get("retention")
    if not isinstance(retention, Mapping):
        return None
    raw = retention.get(key)
    if not isinstance(raw, int) or isinstance(raw, bool) or raw <= 0:
        return None
    return max(1, (raw + 86_399) // 86_400)


def _event_ttl_days(config: Any) -> int:
    retention_days = _retention_ttl_days(_adk_settings(config), "event_ttl_seconds")
    if retention_days is not None:
        return retention_days
    features = _driver_features(config)
    raw = features.get("adk_event_ttl_days", 30)
    try:
        parsed = int(raw)
    except (TypeError, ValueError):
        return 30
    return parsed if parsed > 0 else 30


def _row_deletion_suffix(
    dialect: Literal["googlesql", "spangres"],
    ttl_days: int | None,
    column: str,
) -> str:
    if ttl_days is None or ttl_days < 0:
        return ""
    if dialect == "spangres":
        return f" TTL INTERVAL '{ttl_days} days' ON {column}"
    return f", ROW DELETION POLICY (OLDER_THAN({column}, INTERVAL {ttl_days} DAY))"


def _decode_json_dict(value: Any, *, deserializer: Callable[[str], Any] = from_json) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode("utf-8")
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return {}
        parsed = deserializer(stripped)
        return parsed if isinstance(parsed, dict) else {"value": parsed}
    return dict(value) if hasattr(value, "items") else {}


def _encode_json_text(value: Any, *, serializer: Callable[[Any], str] = to_json) -> str:
    if value is None:
        return "{}"
    if isinstance(value, str):
        return value
    return serializer(value)


def _json_expr(param_name: str, dialect: Literal["googlesql", "spangres"]) -> str:
    if dialect == "spangres":
        return f"CAST(@{param_name} AS JSONB)"
    return f"PARSE_JSON(@{param_name})"


def _metadata_key_col(dialect: Literal["googlesql", "spangres"]) -> str:
    return '"key"' if dialect == "spangres" else "`key`"


def _build_session_ddl(
    config: Any,
    session_table: str,
    owner_id_ddl: str | None,
    *,
    shard_count: int = 0,
    table_options: str | None = None,
    ttl_days: int | None = None,
) -> str:
    table = validate_spanner_identifier(session_table, "session_table")
    dialect = _config_dialect(config)
    owner_col = f"{owner_id_ddl}, " if owner_id_ddl else ""
    options_clause = f" OPTIONS ({table_options})" if table_options else ""
    ttl_clause = _row_deletion_suffix(dialect, ttl_days, "create_time")
    if dialect == "spangres":
        shard_col = (
            f"shard_id BIGINT GENERATED ALWAYS AS (mod(spanner.farm_fingerprint(id), {shard_count})) STORED, "
            if shard_count > 1
            else ""
        )
        pk = "PRIMARY KEY (shard_id, id)" if shard_count > 1 else "PRIMARY KEY (id)"
        return (
            f"CREATE TABLE IF NOT EXISTS {table} ("
            "id VARCHAR(128) NOT NULL, "
            "app_name VARCHAR(128) NOT NULL, "
            "user_id VARCHAR(128) NOT NULL, "
            f"{owner_col}"
            "state JSONB NOT NULL, "
            "create_time TIMESTAMPTZ NOT NULL, "
            "update_time TIMESTAMPTZ NOT NULL, "
            f"{shard_col}"
            f"{pk}"
            f"){ttl_clause}"
        )
    shard_col = f", shard_id INT64 AS (MOD(FARM_FINGERPRINT(id), {shard_count})) STORED" if shard_count > 1 else ""
    pk = "PRIMARY KEY (shard_id, id)" if shard_count > 1 else "PRIMARY KEY (id)"
    return (
        f"CREATE TABLE IF NOT EXISTS {table} ("
        "id STRING(128) NOT NULL, "
        "app_name STRING(128) NOT NULL, "
        "user_id STRING(128) NOT NULL, "
        f"{owner_col}"
        "state JSON NOT NULL, "
        "create_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true), "
        "update_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true)"
        f"{shard_col}"
        f") {pk}{options_clause}{ttl_clause}"
    )


def _build_events_ddl(
    config: Any,
    session_table: str,
    events_table: str,
    *,
    shard_count: int = 0,
    table_options: str | None = None,
) -> str:
    parent = validate_spanner_identifier(session_table, "session_table")
    table = validate_spanner_identifier(events_table, "events_table")
    dialect = _config_dialect(config)
    ttl_days = _event_ttl_days(config)
    options_clause = f" OPTIONS ({table_options})" if table_options else ""
    if dialect == "spangres":
        shard_col = (
            f"shard_id BIGINT GENERATED ALWAYS AS (mod(spanner.farm_fingerprint(session_id), {shard_count})) STORED, "
            if shard_count > 1
            else ""
        )
        pk = "PRIMARY KEY (shard_id, session_id, id)" if shard_count > 1 else "PRIMARY KEY (session_id, id)"
        return (
            f"CREATE TABLE IF NOT EXISTS {table} ("
            "id VARCHAR(128) NOT NULL, "
            "app_name VARCHAR(128) NOT NULL, "
            "user_id VARCHAR(128) NOT NULL, "
            "session_id VARCHAR(128) NOT NULL, "
            "invocation_id VARCHAR(128) NOT NULL, "
            "timestamp TIMESTAMPTZ NOT NULL, "
            "event_data JSONB NOT NULL, "
            f"{shard_col}"
            f"{pk}"
            f") INTERLEAVE IN PARENT {parent} ON DELETE CASCADE "
            f"TTL INTERVAL '{ttl_days} days' ON timestamp"
        )
    shard_col = (
        f", shard_id INT64 AS (MOD(FARM_FINGERPRINT(session_id), {shard_count})) STORED" if shard_count > 1 else ""
    )
    pk = "PRIMARY KEY (shard_id, session_id, id)" if shard_count > 1 else "PRIMARY KEY (session_id, id)"
    return (
        f"CREATE TABLE IF NOT EXISTS {table} ("
        "id STRING(128) NOT NULL, "
        "app_name STRING(128) NOT NULL, "
        "user_id STRING(128) NOT NULL, "
        "session_id STRING(128) NOT NULL, "
        "invocation_id STRING(128) NOT NULL, "
        "timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true), "
        "event_data JSON NOT NULL"
        f"{shard_col}"
        f") {pk}, "
        f"INTERLEAVE IN PARENT {parent} ON DELETE CASCADE, "
        f"ROW DELETION POLICY (OLDER_THAN(timestamp, INTERVAL {ttl_days} DAY)){options_clause}"
    )


def _build_app_state_ddl(config: Any, table_name: str) -> str:
    table = validate_spanner_identifier(table_name, "app_state_table")
    dialect = _config_dialect(config)
    if dialect == "spangres":
        return (
            f"CREATE TABLE IF NOT EXISTS {table} ("
            "app_name VARCHAR(128) NOT NULL, "
            "state JSONB NOT NULL, "
            "update_time TIMESTAMPTZ NOT NULL, "
            "PRIMARY KEY (app_name)"
            ")"
        )
    return (
        f"CREATE TABLE IF NOT EXISTS {table} ("
        "app_name STRING(128) NOT NULL, "
        "state JSON NOT NULL, "
        "update_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true)"
        ") PRIMARY KEY (app_name)"
    )


def _build_user_state_ddl(config: Any, table_name: str) -> str:
    table = validate_spanner_identifier(table_name, "user_state_table")
    dialect = _config_dialect(config)
    if dialect == "spangres":
        return (
            f"CREATE TABLE IF NOT EXISTS {table} ("
            "app_name VARCHAR(128) NOT NULL, "
            "user_id VARCHAR(128) NOT NULL, "
            "state JSONB NOT NULL, "
            "update_time TIMESTAMPTZ NOT NULL, "
            "PRIMARY KEY (app_name, user_id)"
            ")"
        )
    return (
        f"CREATE TABLE IF NOT EXISTS {table} ("
        "app_name STRING(128) NOT NULL, "
        "user_id STRING(128) NOT NULL, "
        "state JSON NOT NULL, "
        "update_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true)"
        ") PRIMARY KEY (app_name, user_id)"
    )


def _build_metadata_ddl(config: Any, table_name: str) -> str:
    table = validate_spanner_identifier(table_name, "metadata_table")
    dialect = _config_dialect(config)
    key_col = _metadata_key_col(dialect)
    if dialect == "spangres":
        return (
            f"CREATE TABLE IF NOT EXISTS {table} ("
            f"{key_col} VARCHAR(256) NOT NULL, "
            "value TEXT NOT NULL, "
            "update_time TIMESTAMPTZ NOT NULL, "
            f"PRIMARY KEY ({key_col})"
            ")"
        )
    return (
        f"CREATE TABLE IF NOT EXISTS {table} ("
        f"{key_col} STRING(256) NOT NULL, "
        "value STRING(MAX) NOT NULL, "
        "update_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true)"
        f") PRIMARY KEY ({key_col})"
    )


def _row_to_stored_session(
    row: Mapping[str, Any],
    *,
    deserializer: Callable[[str], Any] = from_json,
    app_state: dict[str, Any] | None = None,
    user_state: dict[str, Any] | None = None,
    merge_scopes: bool = False,
) -> StoredSession:
    session_state = _decode_json_dict(row.get("state"), deserializer=deserializer)
    if merge_scopes:
        session_state = merge_scoped_state(session_state, app_state, user_state)
    return cast(
        "StoredSession",
        {
            "id": str(row.get("id") or ""),
            "app_name": str(row.get("app_name") or ""),
            "user_id": str(row.get("user_id") or ""),
            "state": session_state,
            "create_time": parse_utc_timestamp(row.get("create_time")),
            "update_time": parse_utc_timestamp(row.get("update_time")),
        },
    )


def _row_to_stored_event(
    row: Mapping[str, Any],
    *,
    deserializer: Callable[[str], Any] = from_json,
) -> StoredEvent:
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


def _row_to_stored_memory(
    row: Mapping[str, Any],
    *,
    deserializer: Callable[[str], Any] = from_json,
) -> StoredMemory:
    emb_raw = row.get("embedding")
    embedding: list[float] | None
    if isinstance(emb_raw, str):
        parsed_emb = deserializer(emb_raw)
        embedding = [float(x) for x in parsed_emb] if isinstance(parsed_emb, Sequence) else None
    elif isinstance(emb_raw, Sequence) and not isinstance(emb_raw, (bytes, bytearray, memoryview)):
        embedding = [float(x) for x in emb_raw]
    else:
        embedding = None
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
            "metadata_json": (
                _decode_json_dict(row.get("metadata_json"), deserializer=deserializer)
                if row.get("metadata_json") is not None
                else None
            ),
            "inserted_at": parse_utc_timestamp(row.get("inserted_at")),
            "embedding": embedding,
        },
    )


def _row_to_stored_artifact(
    row: Mapping[str, Any],
    *,
    deserializer: Callable[[str], Any] = from_json,
) -> StoredArtifact:
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
            "custom_metadata": (
                _decode_json_dict(custom_raw, deserializer=deserializer) if custom_raw is not None else None
            ),
            "created_at": parse_utc_timestamp(row.get("created_at")),
        },
    )


def _upsert_app_state_sql(
    table: str,
    dialect: Literal["googlesql", "spangres"],
) -> str:
    if dialect == "spangres":
        return (
            f"INSERT INTO {table} (app_name, state, update_time) "
            "VALUES (@app_name, CAST(@state AS JSONB), @update_time) "
            "ON CONFLICT (app_name) DO UPDATE SET state = EXCLUDED.state, update_time = EXCLUDED.update_time"
        )
    return (
        f"MERGE INTO {table} AS target "
        "USING (SELECT @app_name AS app_name, PARSE_JSON(@state) AS state, @update_time AS update_time) AS source "
        "ON target.app_name = source.app_name "
        "WHEN MATCHED THEN UPDATE SET state = source.state, update_time = source.update_time "
        "WHEN NOT MATCHED THEN INSERT (app_name, state, update_time) "
        "VALUES (source.app_name, source.state, source.update_time)"
    )


def _upsert_user_state_sql(
    table: str,
    dialect: Literal["googlesql", "spangres"],
) -> str:
    if dialect == "spangres":
        return (
            f"INSERT INTO {table} (app_name, user_id, state, update_time) "
            "VALUES (@app_name, @user_id, CAST(@state AS JSONB), @update_time) "
            "ON CONFLICT (app_name, user_id) DO UPDATE SET state = EXCLUDED.state, update_time = EXCLUDED.update_time"
        )
    return (
        f"MERGE INTO {table} AS target "
        "USING (SELECT @app_name AS app_name, @user_id AS user_id, PARSE_JSON(@state) AS state, "
        "@update_time AS update_time) AS source "
        "ON target.app_name = source.app_name AND target.user_id = source.user_id "
        "WHEN MATCHED THEN UPDATE SET state = source.state, update_time = source.update_time "
        "WHEN NOT MATCHED THEN INSERT (app_name, user_id, state, update_time) "
        "VALUES (source.app_name, source.user_id, source.state, source.update_time)"
    )


def _set_metadata_sql(
    table: str,
    dialect: Literal["googlesql", "spangres"],
) -> str:
    key_col = _metadata_key_col(dialect)
    if dialect == "spangres":
        return (
            f"INSERT INTO {table} ({key_col}, value, update_time) "
            "VALUES (@key, @value, @update_time) "
            f"ON CONFLICT ({key_col}) DO UPDATE SET value = EXCLUDED.value, update_time = EXCLUDED.update_time"
        )
    return (
        f"MERGE INTO {table} AS target "
        f"USING (SELECT @key AS {key_col}, @value AS value, @update_time AS update_time) AS source "
        f"ON target.{key_col} = source.{key_col} "
        "WHEN MATCHED THEN UPDATE SET value = source.value, update_time = source.update_time "
        f"WHEN NOT MATCHED THEN INSERT ({key_col}, value, update_time) "
        f"VALUES (source.{key_col}, source.value, source.update_time)"
    )


def _rows_affected_count(result: Any) -> int:
    return int(getattr(result, "rows_affected", 0) or getattr(result, "num_rows", 0) or 0)


class _SpannerADKSessionMixin:
    """Shared DDL, SQL, parameter builders, and state delta helpers for Spanner ADK session stores."""

    __slots__ = ()

    adapter_name: ClassVar[str | None] = "vertebra_spanner"
    extension_config_options: ClassVar[frozenset[str]] = _SPANNER_ADK_EXTENSION_OPTIONS

    def _build_sessions_table_ddl(self) -> str:
        store = cast("Any", self)
        return _build_session_ddl(
            store._config,
            store._session_table,
            store._owner_id_column_ddl,
            shard_count=store._shard_count,
            table_options=store._session_table_options,
            ttl_days=store._session_ttl_days,
        )

    def _build_events_table_ddl(self) -> str:
        store = cast("Any", self)
        return _build_events_ddl(
            store._config,
            store._session_table,
            store._events_table,
            shard_count=store._shard_count,
            table_options=store._events_table_options,
        )

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
            f"DROP TABLE IF EXISTS {validate_spanner_identifier(store._events_table, 'events_table')}",
            f"DROP TABLE IF EXISTS {validate_spanner_identifier(store._session_table, 'session_table')}",
        ]

    def _drop_app_states_table_sql(self) -> str:
        store = cast("Any", self)
        return f"DROP TABLE IF EXISTS {validate_spanner_identifier(store._app_state_table, 'app_state_table')}"

    def _drop_user_states_table_sql(self) -> str:
        store = cast("Any", self)
        return f"DROP TABLE IF EXISTS {validate_spanner_identifier(store._user_state_table, 'user_state_table')}"

    def _drop_metadata_table_sql(self) -> str:
        store = cast("Any", self)
        return f"DROP TABLE IF EXISTS {validate_spanner_identifier(store._metadata_table, 'metadata_table')}"

    def _build_create_session_sql_and_params(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
        state: dict[str, Any],
        owner_id: Any | None,
        now_iso: str,
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table = validate_spanner_identifier(store._session_table, "session_table")
        dialect = _config_dialect(store._config)
        state_expr = _json_expr("state", dialect)
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
            owner_col = validate_spanner_identifier(store._owner_id_column_name, "owner_id_column")
            params["owner_id"] = owner_id
            sql = (
                f"INSERT INTO {table} (id, app_name, user_id, {owner_col}, state, create_time, update_time) "
                f"VALUES (@id, @app_name, @user_id, @owner_id, {state_expr}, @create_time, @update_time)"
            )
            return sql, params
        sql = (
            f"INSERT INTO {table} (id, app_name, user_id, state, create_time, update_time) "
            f"VALUES (@id, @app_name, @user_id, {state_expr}, @create_time, @update_time)"
        )
        return sql, params

    def _build_get_session_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table = validate_spanner_identifier(store._session_table, "session_table")
        sql = (
            f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table} "
            "WHERE id = @id AND app_name = @app_name AND user_id = @user_id LIMIT 1"
        )
        return sql, {"id": session_id, "app_name": app_name, "user_id": user_id}

    def _build_touch_session_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table = validate_spanner_identifier(store._session_table, "session_table")
        sql = (
            f"UPDATE {table} SET update_time = @update_time "
            "WHERE id = @id AND app_name = @app_name AND user_id = @user_id"
        )
        return sql, {
            "id": session_id,
            "app_name": app_name,
            "user_id": user_id,
            "update_time": datetime.now(UTC).isoformat(),
        }

    def _build_update_session_state_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        state: dict[str, Any],
        now_iso: str | None = None,
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table = validate_spanner_identifier(store._session_table, "session_table")
        dialect = _config_dialect(store._config)
        state_expr = _json_expr("state", dialect)
        sql = (
            f"UPDATE {table} SET state = {state_expr}, update_time = @update_time "
            "WHERE id = @id AND app_name = @app_name AND user_id = @user_id"
        )
        params = {
            "id": session_id,
            "app_name": app_name,
            "user_id": user_id,
            "state": _encode_json_text(state, serializer=store._json_serializer),
            "update_time": now_iso or datetime.now(UTC).isoformat(),
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
        table = validate_spanner_identifier(store._session_table, "session_table")
        where = "app_name = @app_name" + (" AND user_id = @user_id" if user_id is not None else "")
        params: dict[str, Any] = {"app_name": app_name}
        if user_id is not None:
            params["user_id"] = user_id
        limit_sql = ""
        if page_limit is not None:
            limit_sql = " LIMIT @limit OFFSET @offset"
            params["limit"] = page_limit
            params["offset"] = page_offset
        elif page_offset:
            limit_sql = " OFFSET @offset"
            params["offset"] = page_offset
        sql = (
            f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table} "
            f"WHERE {where} ORDER BY {column} {direction}, id {direction}{limit_sql}"
        )
        return sql, params

    def _build_delete_session_sqls_and_params(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
    ) -> tuple[str, str, dict[str, Any]]:
        store = cast("Any", self)
        sessions_table = validate_spanner_identifier(store._session_table, "session_table")
        events_table = validate_spanner_identifier(store._events_table, "events_table")
        params = {"id": session_id, "app_name": app_name, "user_id": user_id}
        events_sql = (
            f"DELETE FROM {events_table} WHERE session_id = @id AND app_name = @app_name AND user_id = @user_id"
        )
        sessions_sql = f"DELETE FROM {sessions_table} WHERE id = @id AND app_name = @app_name AND user_id = @user_id"
        return events_sql, sessions_sql, params

    def _build_append_event_sql_and_params(self, event_record: StoredEvent) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        events_table = validate_spanner_identifier(store._events_table, "events_table")
        dialect = _config_dialect(store._config)
        data_expr = _json_expr("event_data", dialect)
        sql = (
            f"INSERT INTO {events_table} "
            "(id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
            f"VALUES (@id, @app_name, @user_id, @session_id, @invocation_id, @timestamp, {data_expr})"
        )
        params = {
            "id": event_record["id"],
            "app_name": event_record["app_name"],
            "user_id": event_record["user_id"],
            "session_id": event_record["session_id"],
            "invocation_id": event_record["invocation_id"],
            "timestamp": parse_utc_timestamp(event_record["timestamp"]).isoformat(),
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
        events_table = validate_spanner_identifier(store._events_table, "events_table")
        where = "app_name = @app_name AND user_id = @user_id AND session_id = @session_id"
        params: dict[str, Any] = {"app_name": app_name, "user_id": user_id, "session_id": session_id}
        if after_timestamp is not None:
            where += " AND timestamp > @after_timestamp"
            params["after_timestamp"] = parse_utc_timestamp(after_timestamp).isoformat()
        limit_sql = " LIMIT @limit" if limit is not None else ""
        if limit is not None:
            params["limit"] = int(limit)
        sql = (
            "SELECT id, app_name, user_id, session_id, invocation_id, timestamp, event_data "
            f"FROM {events_table} WHERE {where} ORDER BY timestamp ASC{limit_sql}"
        )
        return sql, params

    def _build_get_app_state_sql_and_params(self, app_name: str) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table = validate_spanner_identifier(store._app_state_table, "app_state_table")
        return f"SELECT state FROM {table} WHERE app_name = @app_name LIMIT 1", {"app_name": app_name}

    def _build_upsert_app_state_sql_and_params(
        self,
        app_name: str,
        state: dict[str, Any],
        now_iso: str | None = None,
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table = validate_spanner_identifier(store._app_state_table, "app_state_table")
        dialect = _config_dialect(store._config)
        return _upsert_app_state_sql(table, dialect), {
            "app_name": app_name,
            "state": _encode_json_text(state, serializer=store._json_serializer),
            "update_time": now_iso or datetime.now(UTC).isoformat(),
        }

    def _build_get_user_state_sql_and_params(self, app_name: str, user_id: str) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table = validate_spanner_identifier(store._user_state_table, "user_state_table")
        return (
            f"SELECT state FROM {table} WHERE app_name = @app_name AND user_id = @user_id LIMIT 1",
            {"app_name": app_name, "user_id": user_id},
        )

    def _build_upsert_user_state_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        state: dict[str, Any],
        now_iso: str | None = None,
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table = validate_spanner_identifier(store._user_state_table, "user_state_table")
        dialect = _config_dialect(store._config)
        return _upsert_user_state_sql(table, dialect), {
            "app_name": app_name,
            "user_id": user_id,
            "state": _encode_json_text(state, serializer=store._json_serializer),
            "update_time": now_iso or datetime.now(UTC).isoformat(),
        }

    def _build_get_metadata_sql_and_params(self, key: str) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table = validate_spanner_identifier(store._metadata_table, "metadata_table")
        key_col = _metadata_key_col(_config_dialect(store._config))
        return f"SELECT value FROM {table} WHERE {key_col} = @key LIMIT 1", {"key": key}

    def _build_set_metadata_sql_and_params(self, key: str, value: str) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table = validate_spanner_identifier(store._metadata_table, "metadata_table")
        dialect = _config_dialect(store._config)
        return _set_metadata_sql(table, dialect), {
            "key": key,
            "value": value,
            "update_time": datetime.now(UTC).isoformat(),
        }

    def _build_delete_expired_events_sql_and_params(
        self,
        before: datetime,
        app_name: str | None,
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        events_table = validate_spanner_identifier(store._events_table, "events_table")
        where = "timestamp < @before" + (" AND app_name = @app_name" if app_name is not None else "")
        params: dict[str, Any] = {"before": parse_utc_timestamp(before).isoformat()}
        if app_name is not None:
            params["app_name"] = app_name
        return f"DELETE FROM {events_table} WHERE {where}", params

    def _build_delete_idle_sessions_sql_and_params(
        self,
        updated_before: datetime,
        app_name: str | None,
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        sessions_table = validate_spanner_identifier(store._session_table, "session_table")
        where = "update_time < @before" + (" AND app_name = @app_name" if app_name is not None else "")
        params: dict[str, Any] = {"before": parse_utc_timestamp(updated_before).isoformat()}
        if app_name is not None:
            params["app_name"] = app_name
        return f"DELETE FROM {sessions_table} WHERE {where}", params

    def _build_delete_idle_user_states_sql_and_params(
        self,
        updated_before: datetime,
        app_name: str | None,
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table = validate_spanner_identifier(store._user_state_table, "user_state_table")
        where = "update_time < @before" + (" AND app_name = @app_name" if app_name is not None else "")
        params: dict[str, Any] = {"before": parse_utc_timestamp(updated_before).isoformat()}
        if app_name is not None:
            params["app_name"] = app_name
        return f"DELETE FROM {table} WHERE {where}", params

    @staticmethod
    def _validate_event_scope(event_record: StoredEvent, app_name: str, user_id: str, session_id: str) -> None:
        if (event_record["app_name"], event_record["user_id"], event_record["session_id"]) != (
            app_name,
            user_id,
            session_id,
        ):
            msg = "event_record scope does not match target session arguments."
            raise ValueError(msg)


def _extract_session_store_options(config: Any) -> tuple[int, str | None, str | None, int | None]:
    adk = _adk_settings(config)
    raw_shard = adk.get("shard_count")
    shard_count = int(raw_shard) if isinstance(raw_shard, int) and raw_shard > 0 else 0
    return (
        shard_count,
        cast("str | None", adk.get("session_table_options")),
        cast("str | None", adk.get("events_table_options")),
        _retention_ttl_days(adk, "session_ttl_seconds"),
    )


class SpannerSyncADKStore(_SpannerADKSessionMixin, BaseSyncADKStore["SpannerSyncConfig"]):
    """Cloud Spanner synchronous ADK session and event store using lock-free snapshots for reads."""

    __slots__ = (
        "_events_table_options",
        "_json_deserializer",
        "_json_serializer",
        "_session_table_options",
        "_session_ttl_days",
        "_shard_count",
    )

    def __init__(self, config: SpannerSyncConfig, *, read_config: SpannerSyncConfig | None = None) -> None:
        super().__init__(config, read_config=read_config)
        (
            self._shard_count,
            self._session_table_options,
            self._events_table_options,
            self._session_ttl_days,
        ) = _extract_session_store_options(config)
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

    def ddl_statements(self) -> tuple[str, ...]:
        """Return all Cloud Spanner DDL statements required for ADK sessions, events, and state."""
        return (
            self._sessions_table_ddl(),
            self._events_table_ddl(),
            self._app_states_table_ddl(),
            self._user_states_table_ddl(),
            self._metadata_table_ddl(),
        )

    def create_tables(self) -> None:
        """Create all Spanner ADK tables inside a writable session."""
        with self._config.provide_session(transaction=False) as driver:
            for ddl in self.ddl_statements():
                driver.execute(ddl)

    def _read_app_state_with_driver(self, driver: Any, app_name: str) -> dict[str, Any] | None:
        sql, params = self._build_get_app_state_sql_and_params(app_name)
        try:
            row = driver.select_one_or_none(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return None
            raise
        return _decode_json_dict(row.get("state"), deserializer=self._json_deserializer) if row is not None else None

    def _read_user_state_with_driver(self, driver: Any, app_name: str, user_id: str) -> dict[str, Any] | None:
        sql, params = self._build_get_user_state_sql_and_params(app_name, user_id)
        try:
            row = driver.select_one_or_none(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return None
            raise
        return _decode_json_dict(row.get("state"), deserializer=self._json_deserializer) if row is not None else None

    def _apply_app_state_delta_with_driver(
        self,
        driver: Any,
        app_name: str,
        delta: Mapping[str, Any],
        now_iso: str,
    ) -> dict[str, Any]:
        existing = self._read_app_state_with_driver(driver, app_name) or {}
        merged = {**existing, **delta}
        sql, params = self._build_upsert_app_state_sql_and_params(app_name, merged, now_iso=now_iso)
        driver.execute(sql, params)
        return merged

    def _apply_user_state_delta_with_driver(
        self,
        driver: Any,
        app_name: str,
        user_id: str,
        delta: Mapping[str, Any],
        now_iso: str,
    ) -> dict[str, Any]:
        existing = self._read_user_state_with_driver(driver, app_name, user_id) or {}
        merged = {**existing, **delta}
        sql, params = self._build_upsert_user_state_sql_and_params(app_name, user_id, merged, now_iso=now_iso)
        driver.execute(sql, params)
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
        """Insert a new ADK session row and apply scoped state deltas inside a ``ReadWrite`` transaction."""
        now = datetime.now(UTC)
        now_iso = now.isoformat()
        session_state = dict(state)
        sql, params = self._build_create_session_sql_and_params(
            session_id, app_name, user_id, session_state, owner_id, now_iso
        )
        with self._config.provide_session(transaction=True) as driver:
            driver.execute(sql, params)
            resolved_app = (
                self._apply_app_state_delta_with_driver(driver, app_name, app_state_delta, now_iso)
                if app_state_delta
                else self._read_app_state_with_driver(driver, app_name)
            )
            resolved_user = (
                self._apply_user_state_delta_with_driver(driver, app_name, user_id, user_state_delta, now_iso)
                if user_state_delta
                else self._read_user_state_with_driver(driver, app_name, user_id)
            )
        return cast(
            "StoredSession",
            {
                "id": session_id,
                "app_name": app_name,
                "user_id": user_id,
                "state": merge_scoped_state(session_state, resolved_app, resolved_user),
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
        """Load an ADK session via a lock-free ``ReadOnly`` snapshot."""
        if self._is_renewal_requested(renew_for):
            touch_sql, touch_params = self._build_touch_session_sql_and_params(app_name, user_id, session_id)
            with self._config.provide_session(transaction=True) as driver:
                driver.execute(touch_sql, touch_params)
        sql, params = self._build_get_session_sql_and_params(app_name, user_id, session_id)
        try:
            with self._provide_replica_session(
                primary=renew_for is not None, transaction=False, read_only=True
            ) as driver:
                row = driver.select_one_or_none(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return None
            raise
        return _row_to_stored_session(dict(row), deserializer=self._json_deserializer) if row is not None else None

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Update JSON state and ``update_time`` inside a ``ReadWrite`` transaction."""
        sql, params = self._build_update_session_state_sql_and_params(app_name, user_id, session_id, state)
        with self._config.provide_session(transaction=True) as driver:
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
        """List ADK sessions via a lock-free ``ReadOnly`` snapshot."""
        built = self._build_list_sessions_sql_and_params(
            app_name, user_id, order_by=order_by, descending=descending, limit=limit, offset=offset
        )
        if built is None:
            return []
        sql, params = built
        try:
            with self._provide_replica_session(primary=False, transaction=False, read_only=True) as driver:
                rows = driver.select(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return []
            raise
        return [_row_to_stored_session(dict(row), deserializer=self._json_deserializer) for row in rows]

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete a session and its interleaved events inside a ``ReadWrite`` transaction."""
        events_sql, sessions_sql, params = self._build_delete_session_sqls_and_params(app_name, user_id, session_id)
        with self._config.provide_session(transaction=True) as driver:
            driver.execute(events_sql, params)
            driver.execute(sessions_sql, params)

    def append_event(self, event_record: StoredEvent) -> None:
        """Append an ADK event row inside a ``ReadWrite`` transaction."""
        sql, params = self._build_append_event_sql_and_params(event_record)
        with self._config.provide_session(transaction=True) as driver:
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
        """Atomically append an event and update session, app, and user state deltas in one transaction."""
        self._validate_event_scope(event_record, app_name, user_id, session_id)
        now = datetime.now(UTC)
        now_iso = now.isoformat()
        session_state = dict(state)
        get_sql, get_params = self._build_get_session_sql_and_params(app_name, user_id, session_id)
        update_sql, update_params = self._build_update_session_state_sql_and_params(
            app_name, user_id, session_id, session_state, now_iso=now_iso
        )
        event_sql, event_params = self._build_append_event_sql_and_params(event_record)
        with self._config.provide_session(transaction=True) as driver:
            existing_row = driver.select_one_or_none(get_sql, get_params)
            if existing_row is None:
                msg = f"Session {session_id} not found during append_event_and_update_state."
                raise ValueError(msg)
            driver.execute(update_sql, update_params)
            driver.execute(event_sql, event_params)
            merged_app = (
                self._apply_app_state_delta_with_driver(driver, app_name, app_state_delta, now_iso)
                if app_state_delta
                else None
            )
            merged_user = (
                self._apply_user_state_delta_with_driver(driver, app_name, user_id, user_state_delta, now_iso)
                if user_state_delta
                else None
            )
        return cast(
            "StoredSession",
            {
                "id": session_id,
                "app_name": app_name,
                "user_id": user_id,
                "state": merge_scoped_state(session_state, merged_app, merged_user),
                "create_time": parse_utc_timestamp(existing_row.get("create_time")),
                "update_time": now,
            },
        )

    def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> list[StoredEvent]:
        """Fetch chronological ADK events for a session via a lock-free snapshot."""
        built = self._build_get_events_sql_and_params(app_name, user_id, session_id, after_timestamp, limit)
        if built is None:
            return []
        sql, params = built
        try:
            with self._provide_replica_session(primary=False, transaction=False, read_only=True) as driver:
                rows = driver.select(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return []
            raise
        return [_row_to_stored_event(dict(row), deserializer=self._json_deserializer) for row in rows]

    def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Load shared application-scoped ADK state via a lock-free snapshot."""
        sql, params = self._build_get_app_state_sql_and_params(app_name)
        try:
            with self._provide_replica_session(primary=False, transaction=False, read_only=True) as driver:
                row = driver.select_one_or_none(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return None
            raise
        return _decode_json_dict(row.get("state"), deserializer=self._json_deserializer) if row is not None else None

    def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Upsert application-scoped ADK state inside a ``ReadWrite`` transaction."""
        sql, params = self._build_upsert_app_state_sql_and_params(app_name, state)
        with self._config.provide_session(transaction=True) as driver:
            driver.execute(sql, params)

    def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Load user-scoped ADK state via a lock-free snapshot."""
        sql, params = self._build_get_user_state_sql_and_params(app_name, user_id)
        try:
            with self._provide_replica_session(primary=False, transaction=False, read_only=True) as driver:
                row = driver.select_one_or_none(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return None
            raise
        return _decode_json_dict(row.get("state"), deserializer=self._json_deserializer) if row is not None else None

    def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Upsert user-scoped ADK state inside a ``ReadWrite`` transaction."""
        sql, params = self._build_upsert_user_state_sql_and_params(app_name, user_id, state)
        with self._config.provide_session(transaction=True) as driver:
            driver.execute(sql, params)

    def get_metadata(self, key: str) -> str | None:
        """Retrieve scalar ADK store metadata via a lock-free snapshot."""
        sql, params = self._build_get_metadata_sql_and_params(key)
        try:
            with self._provide_replica_session(primary=False, transaction=False, read_only=True) as driver:
                row = driver.select_one_or_none(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return None
            raise
        return str(row["value"]) if row is not None and row.get("value") is not None else None

    def set_metadata(self, key: str, value: str) -> None:
        """Persist scalar ADK store metadata inside a ``ReadWrite`` transaction."""
        sql, params = self._build_set_metadata_sql_and_params(key, value)
        with self._config.provide_session(transaction=True) as driver:
            driver.execute(sql, params)

    def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Delete ADK events older than ``before``."""
        sql, params = self._build_delete_expired_events_sql_and_params(before, app_name)
        with self._config.provide_session(transaction=True) as driver:
            return _rows_affected_count(driver.execute(sql, params))

    def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete ADK sessions inactive since ``updated_before``."""
        sql, params = self._build_delete_idle_sessions_sql_and_params(updated_before, app_name)
        with self._config.provide_session(transaction=True) as driver:
            return _rows_affected_count(driver.execute(sql, params))

    def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete user state rows inactive since ``updated_before``."""
        sql, params = self._build_delete_idle_user_states_sql_and_params(updated_before, app_name)
        with self._config.provide_session(transaction=True) as driver:
            return _rows_affected_count(driver.execute(sql, params))


class SpannerAsyncADKStore(_SpannerADKSessionMixin, BaseAsyncADKStore["SpannerAsyncConfig"]):
    """Cloud Spanner asynchronous ADK session and event store using lock-free snapshots for reads."""

    __slots__ = (
        "_events_table_options",
        "_json_deserializer",
        "_json_serializer",
        "_session_table_options",
        "_session_ttl_days",
        "_shard_count",
    )

    def __init__(self, config: SpannerAsyncConfig, *, read_config: SpannerAsyncConfig | None = None) -> None:
        super().__init__(config, read_config=read_config)
        (
            self._shard_count,
            self._session_table_options,
            self._events_table_options,
            self._session_ttl_days,
        ) = _extract_session_store_options(config)
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
        """Create all Spanner ADK tables asynchronously."""
        ddls = [
            await self._sessions_table_ddl(),
            await self._events_table_ddl(),
            await self._app_states_table_ddl(),
            await self._user_states_table_ddl(),
            await self._metadata_table_ddl(),
        ]
        async with self._config.provide_session(transaction=False) as driver:
            for ddl in ddls:
                await driver.execute(ddl)

    async def _read_app_state_with_driver(self, driver: Any, app_name: str) -> dict[str, Any] | None:
        sql, params = self._build_get_app_state_sql_and_params(app_name)
        try:
            row = await driver.select_one_or_none(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return None
            raise
        return _decode_json_dict(row.get("state"), deserializer=self._json_deserializer) if row is not None else None

    async def _read_user_state_with_driver(self, driver: Any, app_name: str, user_id: str) -> dict[str, Any] | None:
        sql, params = self._build_get_user_state_sql_and_params(app_name, user_id)
        try:
            row = await driver.select_one_or_none(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return None
            raise
        return _decode_json_dict(row.get("state"), deserializer=self._json_deserializer) if row is not None else None

    async def _apply_app_state_delta_with_driver(
        self,
        driver: Any,
        app_name: str,
        delta: Mapping[str, Any],
        now_iso: str,
    ) -> dict[str, Any]:
        existing = await self._read_app_state_with_driver(driver, app_name) or {}
        merged = {**existing, **delta}
        sql, params = self._build_upsert_app_state_sql_and_params(app_name, merged, now_iso=now_iso)
        await driver.execute(sql, params)
        return merged

    async def _apply_user_state_delta_with_driver(
        self,
        driver: Any,
        app_name: str,
        user_id: str,
        delta: Mapping[str, Any],
        now_iso: str,
    ) -> dict[str, Any]:
        existing = await self._read_user_state_with_driver(driver, app_name, user_id) or {}
        merged = {**existing, **delta}
        sql, params = self._build_upsert_user_state_sql_and_params(app_name, user_id, merged, now_iso=now_iso)
        await driver.execute(sql, params)
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
        """Insert a new ADK session row and apply scoped state deltas asynchronously."""
        now = datetime.now(UTC)
        now_iso = now.isoformat()
        session_state = dict(state)
        sql, params = self._build_create_session_sql_and_params(
            session_id, app_name, user_id, session_state, owner_id, now_iso
        )
        async with self._config.provide_session(transaction=True) as driver:
            await driver.execute(sql, params)
            resolved_app = (
                await self._apply_app_state_delta_with_driver(driver, app_name, app_state_delta, now_iso)
                if app_state_delta
                else await self._read_app_state_with_driver(driver, app_name)
            )
            resolved_user = (
                await self._apply_user_state_delta_with_driver(driver, app_name, user_id, user_state_delta, now_iso)
                if user_state_delta
                else await self._read_user_state_with_driver(driver, app_name, user_id)
            )
        return cast(
            "StoredSession",
            {
                "id": session_id,
                "app_name": app_name,
                "user_id": user_id,
                "state": merge_scoped_state(session_state, resolved_app, resolved_user),
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
        """Load an ADK session asynchronously via a lock-free ``ReadOnly`` snapshot."""
        if self._is_renewal_requested(renew_for):
            touch_sql, touch_params = self._build_touch_session_sql_and_params(app_name, user_id, session_id)
            async with self._config.provide_session(transaction=True) as driver:
                await driver.execute(touch_sql, touch_params)
        sql, params = self._build_get_session_sql_and_params(app_name, user_id, session_id)
        try:
            async with self._provide_replica_session(
                primary=renew_for is not None, transaction=False, read_only=True
            ) as driver:
                row = await driver.select_one_or_none(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return None
            raise
        return _row_to_stored_session(dict(row), deserializer=self._json_deserializer) if row is not None else None

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Update JSON state asynchronously inside a ``ReadWrite`` transaction."""
        sql, params = self._build_update_session_state_sql_and_params(app_name, user_id, session_id, state)
        async with self._config.provide_session(transaction=True) as driver:
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
        """List ADK sessions asynchronously via a lock-free ``ReadOnly`` snapshot."""
        built = self._build_list_sessions_sql_and_params(
            app_name, user_id, order_by=order_by, descending=descending, limit=limit, offset=offset
        )
        if built is None:
            return []
        sql, params = built
        try:
            async with self._provide_replica_session(primary=False, transaction=False, read_only=True) as driver:
                rows = await driver.select(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return []
            raise
        return [_row_to_stored_session(dict(row), deserializer=self._json_deserializer) for row in rows]

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete a session and its interleaved events asynchronously."""
        events_sql, sessions_sql, params = self._build_delete_session_sqls_and_params(app_name, user_id, session_id)
        async with self._config.provide_session(transaction=True) as driver:
            await driver.execute(events_sql, params)
            await driver.execute(sessions_sql, params)

    async def append_event(self, event_record: StoredEvent) -> None:
        """Append an ADK event row asynchronously."""
        sql, params = self._build_append_event_sql_and_params(event_record)
        async with self._config.provide_session(transaction=True) as driver:
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
        """Atomically append an event and update session, app, and user state deltas asynchronously."""
        self._validate_event_scope(event_record, app_name, user_id, session_id)
        now = datetime.now(UTC)
        now_iso = now.isoformat()
        session_state = dict(state)
        get_sql, get_params = self._build_get_session_sql_and_params(app_name, user_id, session_id)
        update_sql, update_params = self._build_update_session_state_sql_and_params(
            app_name, user_id, session_id, session_state, now_iso=now_iso
        )
        event_sql, event_params = self._build_append_event_sql_and_params(event_record)
        async with self._config.provide_session(transaction=True) as driver:
            existing_row = await driver.select_one_or_none(get_sql, get_params)
            if existing_row is None:
                msg = f"Session {session_id} not found during append_event_and_update_state."
                raise ValueError(msg)
            await driver.execute(update_sql, update_params)
            await driver.execute(event_sql, event_params)
            merged_app = (
                await self._apply_app_state_delta_with_driver(driver, app_name, app_state_delta, now_iso)
                if app_state_delta
                else None
            )
            merged_user = (
                await self._apply_user_state_delta_with_driver(driver, app_name, user_id, user_state_delta, now_iso)
                if user_state_delta
                else None
            )
        return cast(
            "StoredSession",
            {
                "id": session_id,
                "app_name": app_name,
                "user_id": user_id,
                "state": merge_scoped_state(session_state, merged_app, merged_user),
                "create_time": parse_utc_timestamp(existing_row.get("create_time")),
                "update_time": now,
            },
        )

    async def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> list[StoredEvent]:
        """Fetch chronological ADK events asynchronously via a lock-free snapshot."""
        built = self._build_get_events_sql_and_params(app_name, user_id, session_id, after_timestamp, limit)
        if built is None:
            return []
        sql, params = built
        try:
            async with self._provide_replica_session(primary=False, transaction=False, read_only=True) as driver:
                rows = await driver.select(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return []
            raise
        return [_row_to_stored_event(dict(row), deserializer=self._json_deserializer) for row in rows]

    async def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Load shared application-scoped ADK state asynchronously via a lock-free snapshot."""
        sql, params = self._build_get_app_state_sql_and_params(app_name)
        try:
            async with self._provide_replica_session(primary=False, transaction=False, read_only=True) as driver:
                row = await driver.select_one_or_none(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return None
            raise
        return _decode_json_dict(row.get("state"), deserializer=self._json_deserializer) if row is not None else None

    async def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Upsert application-scoped ADK state asynchronously."""
        sql, params = self._build_upsert_app_state_sql_and_params(app_name, state)
        async with self._config.provide_session(transaction=True) as driver:
            await driver.execute(sql, params)

    async def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Load user-scoped ADK state asynchronously via a lock-free snapshot."""
        sql, params = self._build_get_user_state_sql_and_params(app_name, user_id)
        try:
            async with self._provide_replica_session(primary=False, transaction=False, read_only=True) as driver:
                row = await driver.select_one_or_none(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return None
            raise
        return _decode_json_dict(row.get("state"), deserializer=self._json_deserializer) if row is not None else None

    async def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Upsert user-scoped ADK state asynchronously."""
        sql, params = self._build_upsert_user_state_sql_and_params(app_name, user_id, state)
        async with self._config.provide_session(transaction=True) as driver:
            await driver.execute(sql, params)

    async def get_metadata(self, key: str) -> str | None:
        """Retrieve scalar ADK store metadata asynchronously via a lock-free snapshot."""
        sql, params = self._build_get_metadata_sql_and_params(key)
        try:
            async with self._provide_replica_session(primary=False, transaction=False, read_only=True) as driver:
                row = await driver.select_one_or_none(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return None
            raise
        return str(row["value"]) if row is not None and row.get("value") is not None else None

    async def set_metadata(self, key: str, value: str) -> None:
        """Persist scalar ADK store metadata asynchronously."""
        sql, params = self._build_set_metadata_sql_and_params(key, value)
        async with self._config.provide_session(transaction=True) as driver:
            await driver.execute(sql, params)

    async def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Delete ADK events older than ``before`` asynchronously."""
        sql, params = self._build_delete_expired_events_sql_and_params(before, app_name)
        async with self._config.provide_session(transaction=True) as driver:
            return _rows_affected_count(await driver.execute(sql, params))

    async def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete ADK sessions inactive since ``updated_before`` asynchronously."""
        sql, params = self._build_delete_idle_sessions_sql_and_params(updated_before, app_name)
        async with self._config.provide_session(transaction=True) as driver:
            return _rows_affected_count(await driver.execute(sql, params))

    async def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete user state rows inactive since ``updated_before`` asynchronously."""
        sql, params = self._build_delete_idle_user_states_sql_and_params(updated_before, app_name)
        async with self._config.provide_session(transaction=True) as driver:
            return _rows_affected_count(await driver.execute(sql, params))


def _memory_ddl_statements(
    config: Any,
    memory_table: str,
    owner_id_ddl: str | None,
    *,
    vector_dimensions: int,
    scann_num_leaves: int,
    scann_tree_depth: int,
    vector_distance_type: str,
    use_fts: bool,
    graph_name: str | None,
    table_options: str | None,
    ttl_days: int | None,
) -> list[str]:
    table = validate_spanner_identifier(memory_table, "memory_table")
    dialect = _config_dialect(config)
    owner_col = f"{owner_id_ddl}, " if owner_id_ddl else ""
    dims = max(1, int(vector_dimensions))
    leaves = max(1, int(scann_num_leaves))
    ttl_clause = _row_deletion_suffix(dialect, ttl_days, "inserted_at")
    options_clause = f" OPTIONS ({table_options})" if table_options else ""
    if dialect == "spangres":
        return [
            f"CREATE TABLE IF NOT EXISTS {table} ("
            "id VARCHAR(128) NOT NULL, "
            "session_id VARCHAR(128) NOT NULL, "
            "app_name VARCHAR(128) NOT NULL, "
            "user_id VARCHAR(128) NOT NULL, "
            f"{owner_col}"
            "scope VARCHAR(32) NOT NULL DEFAULT 'user', "
            "event_id VARCHAR(128) NOT NULL, "
            "author VARCHAR(128), "
            "timestamp TIMESTAMPTZ NOT NULL, "
            "content_json JSONB NOT NULL, "
            "content_text TEXT NOT NULL, "
            "metadata_json JSONB, "
            "inserted_at TIMESTAMPTZ NOT NULL, "
            "embedding REAL[], "
            "PRIMARY KEY (id)"
            f"){ttl_clause}"
        ]
    base_name = table.replace(".", "_")
    statements = [
        f"CREATE TABLE IF NOT EXISTS {table} ("
        "id STRING(128) NOT NULL, "
        "session_id STRING(128) NOT NULL, "
        "app_name STRING(128) NOT NULL, "
        "user_id STRING(128) NOT NULL, "
        f"{owner_col}"
        "scope STRING(32) NOT NULL DEFAULT 'user', "
        "event_id STRING(128) NOT NULL, "
        "author STRING(128), "
        "timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true), "
        "content_json JSON NOT NULL, "
        "content_text STRING(MAX) NOT NULL, "
        "content_tokens TOKENLIST AS (TOKENIZE_FULLTEXT(content_text)) HIDDEN, "
        "metadata_json JSON, "
        "inserted_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true), "
        f"embedding ARRAY<FLOAT32>(vector_length=>{dims})"
        f") PRIMARY KEY (id){options_clause}{ttl_clause}",
        f"CREATE VECTOR INDEX IF NOT EXISTS {base_name}_vec_idx ON {table}(embedding) "
        "WHERE embedding IS NOT NULL "
        f"OPTIONS (distance_type = '{vector_distance_type}', tree_depth = {scann_tree_depth}, num_leaves = {leaves})",
    ]
    if use_fts:
        statements.append(f"CREATE SEARCH INDEX IF NOT EXISTS {base_name}_fts_idx ON {table}(content_tokens)")
    if graph_name:
        safe_graph = validate_spanner_identifier(graph_name, "graph_name")
        statements.append(f"CREATE OR REPLACE PROPERTY GRAPH {safe_graph} NODE TABLES ({table} KEY (id) LABEL Memory)")
    return statements


def _scope_sql_and_params(app_name: str, user_id: str, scope_filter: str) -> tuple[str, dict[str, Any]]:
    if scope_filter == "user":
        return "app_name = @app_name AND scope = 'user' AND user_id = @user_id", {
            "app_name": app_name,
            "user_id": user_id,
        }
    if scope_filter == "app":
        return "app_name = @app_name AND scope = 'app'", {"app_name": app_name}
    return "app_name = @app_name AND ((scope = 'user' AND user_id = @user_id) OR scope = 'app')", {
        "app_name": app_name,
        "user_id": user_id,
    }


def _memory_insert_sql(
    table: str,
    dialect: Literal["googlesql", "spangres"],
    owner_col: str | None,
) -> str:
    content_expr = _json_expr("content_json", dialect)
    meta_expr = _json_expr("metadata_json", dialect)
    cols = (
        "id, session_id, app_name, user_id"
        + (f", {owner_col}" if owner_col else "")
        + ", scope, event_id, author, timestamp, content_json, content_text, metadata_json, inserted_at, embedding"
    )
    vals = (
        "@id, @session_id, @app_name, @user_id"
        + (", @owner_id" if owner_col else "")
        + f", @scope, @event_id, @author, @timestamp, {content_expr}, @content_text, {meta_expr}, @inserted_at, @embedding"
    )
    if dialect == "spangres":
        return f"INSERT INTO {table} ({cols}) VALUES ({vals}) ON CONFLICT (id) DO NOTHING"
    return (
        f"MERGE INTO {table} AS target "
        "USING (SELECT @id AS id) AS source ON target.id = source.id "
        f"WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})"
    )


def _extract_memory_store_options(
    config: Any,
    memory_table: str,
) -> tuple[int, str | None, int | None, str, bool, int, bool, bool, str]:
    adk = _adk_settings(config)
    raw_shard = adk.get("shard_count")
    shard_count = int(raw_shard) if isinstance(raw_shard, int) and raw_shard > 0 else 0
    raw_distance = str(adk.get("vector_distance_type") or "COSINE").upper()
    if raw_distance not in _SPANNER_DISTANCE_FUNCTIONS:
        msg = f"Unsupported Spanner vector_distance_type: {raw_distance!r}"
        raise ValueError(msg)
    raw_depth = adk.get("scann_tree_depth", 2)
    scann_tree_depth = int(raw_depth) if isinstance(raw_depth, int) and not isinstance(raw_depth, bool) else 2
    if scann_tree_depth not in {2, 3}:
        msg = f"scann_tree_depth must be 2 or 3, got {scann_tree_depth!r}"
        raise ValueError(msg)
    raw_graph = adk.get("memory_graph_name")
    base_name = validate_spanner_identifier(memory_table, "memory_table").replace(".", "_")
    graph_name = str(raw_graph) if raw_graph else f"{base_name}_graph"
    validate_spanner_identifier(graph_name, "memory_graph_name")
    return (
        shard_count,
        cast("str | None", adk.get("memory_table_options")),
        _retention_ttl_days(adk, "memory_ttl_seconds"),
        raw_distance,
        bool(adk.get("vector_index_enabled", False)),
        scann_tree_depth,
        bool(adk.get("enable_hybrid_search", True)),
        bool(adk.get("enable_memory_graph", False)),
        graph_name,
    )


class _SpannerADKMemoryMixin:
    """Shared ScaNN vector, full-text ``TOKENIZE_FULLTEXT``, SQL RRF, and Spanner Graph helpers."""

    __slots__ = ()

    adapter_name: ClassVar[str | None] = "vertebra_spanner"
    extension_config_options: ClassVar[frozenset[str]] = _SPANNER_ADK_EXTENSION_OPTIONS

    def _resolved_graph_name(self) -> str:
        store = cast("Any", self)
        return str(
            getattr(
                store,
                "_memory_graph_name",
                f"{validate_spanner_identifier(store._memory_table, 'memory_table').replace('.', '_')}_graph",
            )
        )

    def _vector_distance_expression(self, dialect: Literal["googlesql", "spangres"]) -> str:
        store = cast("Any", self)
        if dialect == "spangres":
            return "spanner.cosine_distance(embedding, @query_embedding)"
        distance_type = getattr(store, "_vector_distance_type", "COSINE")
        if distance_type == "COSINE":
            return (
                "APPROX_COSINE_DISTANCE(embedding, @query_embedding, options => JSON '{\"num_leaves_to_search\": 10}')"
            )
        distance_fn = _SPANNER_DISTANCE_FUNCTIONS.get(distance_type, "APPROX_COSINE_DISTANCE")
        return f"{distance_fn}(embedding, @query_embedding)"

    def _build_memory_table_ddl(self) -> list[str]:
        store = cast("Any", self)
        return _memory_ddl_statements(
            store._config,
            store._memory_table,
            store._owner_id_column_ddl,
            vector_dimensions=int(getattr(store, "_vector_dimensions", 768)),
            scann_num_leaves=int(getattr(store, "_scann_num_leaves", 100)),
            scann_tree_depth=int(getattr(store, "_scann_tree_depth", 2)),
            vector_distance_type=str(getattr(store, "_vector_distance_type", "COSINE")),
            use_fts=bool(getattr(store, "_use_fts", False)),
            graph_name=self._resolved_graph_name(),
            table_options=getattr(store, "_memory_table_options", None),
            ttl_days=getattr(store, "_memory_ttl_days", None),
        )

    def _drop_memory_table_sql(self) -> list[str]:
        store = cast("Any", self)
        return [f"DROP TABLE IF EXISTS {validate_spanner_identifier(store._memory_table, 'memory_table')}"]

    def build_vector_index_sql(self, index_name: str | None = None) -> str:
        """Return a GoogleSQL ``CREATE VECTOR INDEX`` DDL using Spanner ``SCANN``."""
        store = cast("Any", self)
        table = validate_spanner_identifier(store._memory_table, "memory_table")
        resolved = validate_spanner_identifier(index_name or f"{table.replace('.', '_')}_vec_idx", "vector_index")
        leaves = max(1, int(getattr(store, "_scann_num_leaves", 100)))
        depth = int(getattr(store, "_scann_tree_depth", 2))
        distance_type = str(getattr(store, "_vector_distance_type", "COSINE"))
        return (
            f"CREATE VECTOR INDEX IF NOT EXISTS {resolved} ON {table}(embedding) "
            "WHERE embedding IS NOT NULL "
            f"OPTIONS (distance_type = '{distance_type}', tree_type = 'SCANN', tree_depth = {depth}, num_leaves = {leaves})"
        )

    def build_search_index_sql(self, index_name: str | None = None) -> str:
        """Return a GoogleSQL ``CREATE SEARCH INDEX`` DDL over ``content_tokens``."""
        store = cast("Any", self)
        table = validate_spanner_identifier(store._memory_table, "memory_table")
        resolved = validate_spanner_identifier(index_name or f"{table.replace('.', '_')}_fts_idx", "search_index")
        return f"CREATE SEARCH INDEX IF NOT EXISTS {resolved} ON {table}(content_tokens)"

    def build_property_graph_sql(self, graph_name: str | None = None) -> str:
        """Return a Spanner Graph DDL associating memory nodes via ``PROPERTY GRAPH``."""
        store = cast("Any", self)
        table = validate_spanner_identifier(store._memory_table, "memory_table")
        resolved = validate_spanner_identifier(graph_name or self._resolved_graph_name(), "graph_name")
        return f"CREATE OR REPLACE PROPERTY GRAPH {resolved} NODE TABLES ({table} KEY (id) LABEL Memory)"

    def build_vector_search_sql(
        self,
        *,
        app_name: str,
        user_id: str,
        scope_filter: str = "all",
        top_k: int = 20,
    ) -> tuple[str, dict[str, Any]]:
        """Build a Spanner SQL vector similarity search query ordered by distance."""
        store = cast("Any", self)
        table = validate_spanner_identifier(store._memory_table, "memory_table")
        dialect = _config_dialect(store._config)
        where, params = _scope_sql_and_params(app_name, user_id, scope_filter)
        params["top_k"] = int(top_k)
        distance_expr = self._vector_distance_expression(dialect)
        sql = (
            "SELECT id, session_id, app_name, user_id, scope, event_id, author, timestamp, "
            "content_json, content_text, metadata_json, inserted_at, embedding, "
            f"{distance_expr} AS distance "
            f"FROM {table} WHERE {where} AND embedding IS NOT NULL "
            "ORDER BY distance ASC, timestamp DESC LIMIT @top_k"
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
        """Build a Spanner full-text search query ordered by relevance score."""
        store = cast("Any", self)
        table = validate_spanner_identifier(store._memory_table, "memory_table")
        dialect = _config_dialect(store._config)
        where, params = _scope_sql_and_params(app_name, user_id, scope_filter)
        params["query"] = query
        params["top_k"] = int(top_k)
        if dialect == "spangres":
            sql = (
                "SELECT id, session_id, app_name, user_id, scope, event_id, author, timestamp, "
                "content_json, content_text, metadata_json, inserted_at, embedding "
                f"FROM {table} "
                f"WHERE {where} AND to_tsvector('simple', content_text) @@ plainto_tsquery('simple', @query) "
                "ORDER BY ts_rank(to_tsvector('simple', content_text), plainto_tsquery('simple', @query)) DESC, "
                "timestamp DESC LIMIT @top_k"
            )
            return sql, params
        sql = (
            "SELECT id, session_id, app_name, user_id, scope, event_id, author, timestamp, "
            "content_json, content_text, metadata_json, inserted_at, embedding "
            f"FROM {table} WHERE {where} AND SEARCH(content_tokens, @query) "
            "ORDER BY SCORE(content_tokens, @query) DESC, timestamp DESC LIMIT @top_k"
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
        """Build a case-insensitive ``LIKE`` text match query ordered by timestamp."""
        store = cast("Any", self)
        table = validate_spanner_identifier(store._memory_table, "memory_table")
        where, params = _scope_sql_and_params(app_name, user_id, scope_filter)
        stripped = query.strip()
        params["pattern"] = f"%{stripped.lower()}%" if stripped else "%"
        params["top_k"] = int(top_k)
        sql = (
            "SELECT id, session_id, app_name, user_id, scope, event_id, author, timestamp, "
            "content_json, content_text, metadata_json, inserted_at, embedding "
            f"FROM {table} WHERE {where} AND LOWER(content_text) LIKE @pattern "
            "ORDER BY timestamp DESC LIMIT @top_k"
        )
        return sql, params

    def build_hybrid_rrf_search_sql(
        self,
        *,
        app_name: str,
        user_id: str,
        query: str,
        scope_filter: str = "all",
        top_k: int = 10,
        rrf_k: int = 60,
    ) -> tuple[str, dict[str, Any]]:
        """Build a Spanner SQL CTE Reciprocal Rank Fusion query combining vector and text rankings."""
        store = cast("Any", self)
        table = validate_spanner_identifier(store._memory_table, "memory_table")
        dialect = _config_dialect(store._config)
        where, params = _scope_sql_and_params(app_name, user_id, scope_filter)
        candidate_limit = max(int(top_k) * 2, 50)
        distance_expr = self._vector_distance_expression(dialect)
        params["candidate_limit"] = candidate_limit
        params["top_k"] = int(top_k)
        params["rrf_k"] = max(1, int(rrf_k))
        if dialect == "spangres":
            if store._use_fts:
                params["query"] = query
                text_cte = (
                    "SELECT id, RANK() OVER ("
                    "ORDER BY ts_rank(to_tsvector('simple', content_text), plainto_tsquery('simple', @query)) DESC"
                    ") AS rank_txt "
                    f"FROM {table} "
                    f"WHERE {where} AND to_tsvector('simple', content_text) @@ plainto_tsquery('simple', @query) "
                    "LIMIT @candidate_limit"
                )
            else:
                params["pattern"] = f"%{query.strip().lower()}%"
                text_cte = (
                    "SELECT id, RANK() OVER (ORDER BY timestamp DESC) AS rank_txt "
                    f"FROM {table} "
                    f"WHERE {where} AND LOWER(content_text) LIKE @pattern "
                    "LIMIT @candidate_limit"
                )
        else:
            params["query"] = query
            text_cte = (
                "SELECT id, RANK() OVER (ORDER BY SCORE(content_tokens, @query) DESC) AS rank_txt "
                f"FROM {table} "
                f"WHERE {where} AND SEARCH(content_tokens, @query) "
                "LIMIT @candidate_limit"
            )
        sql = (
            "WITH vector_matches AS ("
            f"SELECT id, RANK() OVER (ORDER BY {distance_expr} ASC) AS rank_vec "
            f"FROM {table} "
            f"WHERE {where} AND embedding IS NOT NULL "
            "LIMIT @candidate_limit"
            "), "
            "text_matches AS ("
            f"{text_cte}"
            ") "
            "SELECT m.id, m.session_id, m.app_name, m.user_id, m.scope, m.event_id, m.author, "
            "m.timestamp, m.content_json, m.content_text, m.metadata_json, m.inserted_at, m.embedding, "
            "(COALESCE(1.0 / (@rrf_k + v.rank_vec), 0.0) + COALESCE(1.0 / (@rrf_k + t.rank_txt), 0.0)) AS rrf_score "
            f"FROM {table} m "
            "LEFT JOIN vector_matches v ON m.id = v.id "
            "LEFT JOIN text_matches t ON m.id = t.id "
            "WHERE v.id IS NOT NULL OR t.id IS NOT NULL "
            "ORDER BY rrf_score DESC, m.timestamp DESC "
            "LIMIT @top_k"
        )
        return sql, params

    def build_graph_search_sql(
        self,
        *,
        app_name: str,
        user_id: str,
        graph_name: str | None = None,
        top_k: int = 20,
    ) -> tuple[str, dict[str, Any]]:
        """Build a Spanner Graph ``GRAPH ... MATCH`` traversal query."""
        resolved = validate_spanner_identifier(graph_name or self._resolved_graph_name(), "graph_name")
        sql = (
            f"GRAPH {resolved} MATCH (m:Memory) "
            "WHERE m.app_name = @app_name AND m.user_id = @user_id "
            "RETURN m.id AS id, m.session_id AS session_id, m.app_name AS app_name, "
            "m.user_id AS user_id, m.scope AS scope, m.event_id AS event_id, m.author AS author, "
            "m.timestamp AS timestamp, m.content_json AS content_json, m.content_text AS content_text, "
            "m.metadata_json AS metadata_json, m.inserted_at AS inserted_at, m.embedding AS embedding "
            "ORDER BY m.timestamp DESC LIMIT @top_k"
        )
        return sql, {"app_name": app_name, "user_id": user_id, "top_k": int(top_k)}

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
        if embedding is not None and store._use_fts and has_query and store._enable_hybrid_search:
            sql, params = self.build_hybrid_rrf_search_sql(
                app_name=app_name,
                user_id=user_id,
                query=query,
                scope_filter=scope_filter,
                top_k=top_k,
            )
            params["query_embedding"] = [float(x) for x in embedding]
            return sql, params
        if embedding is not None:
            sql, params = self.build_vector_search_sql(
                app_name=app_name,
                user_id=user_id,
                scope_filter=scope_filter,
                top_k=top_k,
            )
            params["query_embedding"] = [float(x) for x in embedding]
            return sql, params
        if store._use_fts and has_query:
            return self.build_fts_search_sql(
                app_name=app_name,
                user_id=user_id,
                query=query,
                scope_filter=scope_filter,
                top_k=top_k,
            )
        return self.build_text_search_sql(
            app_name=app_name,
            user_id=user_id,
            query=query,
            scope_filter=scope_filter,
            top_k=top_k,
        )

    def _build_hybrid_rrf_sql_and_params(
        self,
        *,
        app_name: str,
        user_id: str,
        query: str,
        embedding: Sequence[float] | None,
        scope_filter: Literal["all", "user", "app"],
        top_k: int,
        rrf_k: int,
    ) -> tuple[str, dict[str, Any]] | None:
        store = cast("Any", self)
        if not store._enabled or top_k <= 0:
            return None
        has_query = bool(query and query.strip())
        if not has_query and embedding is None:
            return None
        if embedding is not None and has_query:
            sql, params = self.build_hybrid_rrf_search_sql(
                app_name=app_name,
                user_id=user_id,
                query=query,
                scope_filter=scope_filter,
                top_k=top_k,
                rrf_k=rrf_k,
            )
            params["query_embedding"] = [float(x) for x in embedding]
            return sql, params
        if embedding is not None:
            sql, params = self.build_vector_search_sql(
                app_name=app_name,
                user_id=user_id,
                scope_filter=scope_filter,
                top_k=top_k,
            )
            params["query_embedding"] = [float(x) for x in embedding]
            return sql, params
        if store._use_fts:
            return self.build_fts_search_sql(
                app_name=app_name,
                user_id=user_id,
                query=query,
                scope_filter=scope_filter,
                top_k=top_k,
            )
        return self.build_text_search_sql(
            app_name=app_name,
            user_id=user_id,
            query=query,
            scope_filter=scope_filter,
            top_k=top_k,
        )

    def _build_insert_entry_sql_and_params(
        self,
        entry: StoredMemory,
        owner_id: object | None,
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table = validate_spanner_identifier(store._memory_table, "memory_table")
        dialect = _config_dialect(store._config)
        owner_col = (
            validate_spanner_identifier(store._owner_id_column_name, "owner_id_column")
            if store._owner_id_column_name
            else None
        )
        sql = _memory_insert_sql(table, dialect, owner_col)
        emb_raw = entry.get("embedding")
        params: dict[str, Any] = {
            "id": entry["id"],
            "session_id": entry["session_id"],
            "app_name": entry["app_name"],
            "user_id": entry["user_id"],
            "scope": entry.get("scope", "user"),
            "event_id": entry["event_id"],
            "author": entry.get("author"),
            "timestamp": parse_utc_timestamp(entry["timestamp"]).isoformat(),
            "content_json": _encode_json_text(entry.get("content_json", {}), serializer=store._json_serializer),
            "content_text": entry.get("content_text", ""),
            "metadata_json": (
                _encode_json_text(entry["metadata_json"], serializer=store._json_serializer)
                if entry.get("metadata_json") is not None
                else None
            ),
            "inserted_at": parse_utc_timestamp(entry["inserted_at"]).isoformat(),
            "embedding": [float(x) for x in emb_raw] if emb_raw is not None else None,
        }
        if owner_col:
            params["owner_id"] = owner_id
        return sql, params

    def _build_delete_by_session_sql_and_params(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table = validate_spanner_identifier(store._memory_table, "memory_table")
        sql = f"DELETE FROM {table} WHERE session_id = @session_id AND app_name = @app_name AND user_id = @user_id"
        return sql, {"session_id": session_id, "app_name": app_name, "user_id": user_id}

    def _build_delete_older_than_sql_and_params(
        self,
        days: int,
        app_name: str | None,
        scope: str | None,
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table = validate_spanner_identifier(store._memory_table, "memory_table")
        cutoff = (datetime.now(UTC) - timedelta(days=days)).isoformat()
        clauses = ["inserted_at < @cutoff"]
        params: dict[str, Any] = {"cutoff": cutoff}
        if app_name is not None:
            clauses.append("app_name = @app_name")
            params["app_name"] = app_name
        if scope is not None:
            clauses.append("scope = @scope")
            params["scope"] = scope
        return f"DELETE FROM {table} WHERE {' AND '.join(clauses)}", params


class SpannerSyncADKMemoryStore(_SpannerADKMemoryMixin, _BaseSyncADKMemoryStore["SpannerSyncConfig"]):
    """Cloud Spanner synchronous ADK memory store with ScaNN, FTS, SQL RRF, and Property Graph support."""

    __slots__ = (
        "_enable_hybrid_search",
        "_enable_memory_graph",
        "_json_deserializer",
        "_json_serializer",
        "_memory_graph_name",
        "_memory_table_options",
        "_memory_ttl_days",
        "_scann_tree_depth",
        "_shard_count",
        "_vector_distance_type",
        "_vector_index_enabled",
    )

    def __init__(self, config: SpannerSyncConfig, *, read_config: SpannerSyncConfig | None = None) -> None:
        super().__init__(config, read_config=read_config)
        adk = _adk_settings(config)
        if "use_fts" in adk and "memory_use_fts" not in adk:
            self._use_fts = bool(adk["use_fts"])
        (
            self._shard_count,
            self._memory_table_options,
            self._memory_ttl_days,
            self._vector_distance_type,
            self._vector_index_enabled,
            self._scann_tree_depth,
            self._enable_hybrid_search,
            self._enable_memory_graph,
            self._memory_graph_name,
        ) = _extract_memory_store_options(config, self._memory_table)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    def _memory_table_ddl(self) -> list[str]:
        return self._build_memory_table_ddl()

    def create_tables(self) -> None:
        """Create the Spanner memory table, ScaNN vector index, search index, and property graph."""
        if not self._enabled:
            return
        with self._config.provide_session(transaction=False) as driver:
            for stmt in self._memory_table_ddl():
                driver.execute(stmt)

    def insert_memory_entries(self, entries: list[StoredMemory], owner_id: object | None = None) -> int:
        """Insert memory records inside a ``ReadWrite`` transaction with deduplication on ``id``."""
        if not self._enabled or not entries:
            return 0
        inserted = 0
        with self._config.provide_session(transaction=True) as driver:
            for entry in entries:
                sql, params = self._build_insert_entry_sql_and_params(entry, owner_id)
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
        """Search ADK memories via Spanner SQL ScaNN vector search, FTS, simple text, or CTE RRF."""
        built = self._build_search_sql_and_params(query, app_name, user_id, limit, scope_filter, embedding)
        if built is None:
            return []
        sql, params = built
        try:
            with self._provide_replica_session(primary=False, transaction=False, read_only=True) as driver:
                rows = driver.select(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return []
            raise
        return [_row_to_stored_memory(dict(row), deserializer=self._json_deserializer) for row in rows]

    def hybrid_rrf_search(
        self,
        *,
        app_name: str,
        user_id: str,
        query: str = "",
        embedding: Sequence[float] | None = None,
        scope_filter: Literal["all", "user", "app"] = "all",
        top_k: int = 10,
        rrf_k: int = 60,
    ) -> list[StoredMemory]:
        """Combine vector and full-text rankings directly in Spanner SQL via CTE Reciprocal Rank Fusion."""
        built = self._build_hybrid_rrf_sql_and_params(
            app_name=app_name,
            user_id=user_id,
            query=query,
            embedding=embedding,
            scope_filter=scope_filter,
            top_k=top_k,
            rrf_k=rrf_k,
        )
        if built is None:
            return []
        sql, params = built
        try:
            with self._provide_replica_session(primary=False, transaction=False, read_only=True) as driver:
                rows = driver.select(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return []
            raise
        return [_row_to_stored_memory(dict(row), deserializer=self._json_deserializer) for row in rows]

    def delete_entries_by_session(self, session_id: str, app_name: str, user_id: str) -> int:
        """Delete memory entries for ``session_id`` owned by ``app_name`` and ``user_id``."""
        sql, params = self._build_delete_by_session_sql_and_params(session_id, app_name, user_id)
        try:
            with self._config.provide_session(transaction=True) as driver:
                return _rows_affected_count(driver.execute(sql, params))
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return 0
            raise

    def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
        """Delete memory entries inserted more than ``days`` ago."""
        sql, params = self._build_delete_older_than_sql_and_params(days, app_name, scope)
        try:
            with self._config.provide_session(transaction=True) as driver:
                return _rows_affected_count(driver.execute(sql, params))
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return 0
            raise


class SpannerAsyncADKMemoryStore(_SpannerADKMemoryMixin, _BaseAsyncADKMemoryStore["SpannerAsyncConfig"]):
    """Cloud Spanner asynchronous ADK memory store with ScaNN, FTS, SQL RRF, and Property Graph support."""

    __slots__ = (
        "_enable_hybrid_search",
        "_enable_memory_graph",
        "_json_deserializer",
        "_json_serializer",
        "_memory_graph_name",
        "_memory_table_options",
        "_memory_ttl_days",
        "_scann_tree_depth",
        "_shard_count",
        "_vector_distance_type",
        "_vector_index_enabled",
    )

    def __init__(self, config: SpannerAsyncConfig, *, read_config: SpannerAsyncConfig | None = None) -> None:
        super().__init__(config, read_config=read_config)
        adk = _adk_settings(config)
        if "use_fts" in adk and "memory_use_fts" not in adk:
            self._use_fts = bool(adk["use_fts"])
        (
            self._shard_count,
            self._memory_table_options,
            self._memory_ttl_days,
            self._vector_distance_type,
            self._vector_index_enabled,
            self._scann_tree_depth,
            self._enable_hybrid_search,
            self._enable_memory_graph,
            self._memory_graph_name,
        ) = _extract_memory_store_options(config, self._memory_table)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    async def _memory_table_ddl(self) -> list[str]:
        return self._build_memory_table_ddl()

    async def create_tables(self) -> None:
        """Create the Spanner memory table, ScaNN vector index, search index, and property graph."""
        if not self._enabled:
            return
        ddls = await self._memory_table_ddl()
        async with self._config.provide_session(transaction=False) as driver:
            for stmt in ddls:
                await driver.execute(stmt)

    async def insert_memory_entries(self, entries: list[StoredMemory], owner_id: object | None = None) -> int:
        """Insert memory records asynchronously inside a ``ReadWrite`` transaction."""
        if not self._enabled or not entries:
            return 0
        inserted = 0
        async with self._config.provide_session(transaction=True) as driver:
            for entry in entries:
                sql, params = self._build_insert_entry_sql_and_params(entry, owner_id)
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
        """Search ADK memories asynchronously via Spanner SQL ScaNN vector search, FTS, simple text, or CTE RRF."""
        built = self._build_search_sql_and_params(query, app_name, user_id, limit, scope_filter, embedding)
        if built is None:
            return []
        sql, params = built
        try:
            async with self._provide_replica_session(primary=False, transaction=False, read_only=True) as driver:
                rows = await driver.select(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return []
            raise
        return [_row_to_stored_memory(dict(row), deserializer=self._json_deserializer) for row in rows]

    async def hybrid_rrf_search(
        self,
        *,
        app_name: str,
        user_id: str,
        query: str = "",
        embedding: Sequence[float] | None = None,
        scope_filter: Literal["all", "user", "app"] = "all",
        top_k: int = 10,
        rrf_k: int = 60,
    ) -> list[StoredMemory]:
        """Combine vector and full-text rankings asynchronously in Spanner SQL via CTE Reciprocal Rank Fusion."""
        built = self._build_hybrid_rrf_sql_and_params(
            app_name=app_name,
            user_id=user_id,
            query=query,
            embedding=embedding,
            scope_filter=scope_filter,
            top_k=top_k,
            rrf_k=rrf_k,
        )
        if built is None:
            return []
        sql, params = built
        try:
            async with self._provide_replica_session(primary=False, transaction=False, read_only=True) as driver:
                rows = await driver.select(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return []
            raise
        return [_row_to_stored_memory(dict(row), deserializer=self._json_deserializer) for row in rows]

    async def delete_entries_by_session(self, session_id: str, app_name: str, user_id: str) -> int:
        """Delete memory entries for ``session_id`` owned by ``app_name`` and ``user_id`` asynchronously."""
        sql, params = self._build_delete_by_session_sql_and_params(session_id, app_name, user_id)
        try:
            async with self._config.provide_session(transaction=True) as driver:
                return _rows_affected_count(await driver.execute(sql, params))
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return 0
            raise

    async def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
        """Delete aged memory entries asynchronously."""
        sql, params = self._build_delete_older_than_sql_and_params(days, app_name, scope)
        try:
            async with self._config.provide_session(transaction=True) as driver:
                return _rows_affected_count(await driver.execute(sql, params))
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return 0
            raise


def _artifact_ddl(config: Any, artifact_table: str) -> str:
    table = validate_spanner_identifier(artifact_table, "artifact_table")
    dialect = _config_dialect(config)
    ttl_days = _retention_ttl_days(_adk_settings(config), "artifact_ttl_seconds")
    ttl_clause = _row_deletion_suffix(dialect, ttl_days, "created_at")
    if dialect == "spangres":
        return (
            f"CREATE TABLE IF NOT EXISTS {table} ("
            "app_name VARCHAR(128) NOT NULL, "
            "user_id VARCHAR(128) NOT NULL, "
            "session_id VARCHAR(128), "
            "filename VARCHAR(512) NOT NULL, "
            "version BIGINT NOT NULL, "
            "mime_type VARCHAR(256), "
            "canonical_uri VARCHAR(1024) NOT NULL, "
            "custom_metadata JSONB, "
            "created_at TIMESTAMPTZ NOT NULL, "
            "PRIMARY KEY (app_name, user_id, filename, version)"
            f"){ttl_clause}"
        )
    return (
        f"CREATE TABLE IF NOT EXISTS {table} ("
        "app_name STRING(128) NOT NULL, "
        "user_id STRING(128) NOT NULL, "
        "session_id STRING(128), "
        "filename STRING(512) NOT NULL, "
        "version INT64 NOT NULL, "
        "mime_type STRING(256), "
        "canonical_uri STRING(1024) NOT NULL, "
        "custom_metadata JSON, "
        "created_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true)"
        f") PRIMARY KEY (app_name, user_id, filename, version){ttl_clause}"
    )


def _artifact_scope_where(
    app_name: str,
    user_id: str,
    filename: str,
    session_id: str | None = None,
) -> tuple[str, dict[str, Any]]:
    params: dict[str, Any] = {"app_name": app_name, "user_id": user_id, "filename": filename}
    if session_id is None:
        return "app_name = @app_name AND user_id = @user_id AND filename = @filename AND session_id IS NULL", params
    params["session_id"] = session_id
    return "app_name = @app_name AND user_id = @user_id AND filename = @filename AND session_id = @session_id", params


class _SpannerADKArtifactMixin:
    """Shared ``ArtifactReference``, SQL builder, and ``ObjectStoreBackend`` wiring without BigQuery coupling."""

    __slots__ = ()

    adapter_name: ClassVar[str | None] = "vertebra_spanner"

    def _init_storage(
        self,
        config: Any,
        storage_backend: ObjectStoreBackend | str | None = None,
    ) -> ObjectStoreBackend | None:
        candidate = storage_backend or getattr(config, "storage_backend", None)
        if candidate is None:
            adk = _adk_settings(config)
            if isinstance(adk.get("artifact_storage_uri"), str):
                candidate = adk["artifact_storage_uri"]
        return resolve_storage_backend(candidate)

    @staticmethod
    def to_artifact_reference(
        uri: str,
        version: int,
        *,
        content_type: str | None = None,
        size: int | None = None,
        updated: datetime | str | None = None,
        signed_url: str | None = None,
    ) -> ArtifactReference:
        """Construct a storage-neutral ``vertebra.storage.ArtifactReference`` directly."""
        return object_to_artifact_reference(
            uri,
            version,
            content_type=content_type,
            size=size,
            updated=updated,
            signed_url=signed_url,
        )

    @staticmethod
    def _stored_artifact_to_ref(record: Mapping[str, Any], size: int | None = None) -> ArtifactReference:
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
        table = validate_spanner_identifier(store._artifact_table, "artifact_table")
        dialect = _config_dialect(store._config)
        meta_expr = _json_expr("custom_metadata", dialect)
        sql = (
            f"INSERT INTO {table} "
            "(app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at) "
            f"VALUES (@app_name, @user_id, @session_id, @filename, @version, @mime_type, @canonical_uri, "
            f"{meta_expr}, @created_at)"
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
            "created_at": parse_utc_timestamp(record.get("created_at") or datetime.now(UTC)).isoformat(),
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
        table = validate_spanner_identifier(store._artifact_table, "artifact_table")
        where, params = _artifact_scope_where(app_name, user_id, filename, session_id)
        if version is not None:
            where += " AND version = @version"
            params["version"] = int(version)
        sql = (
            "SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
            f"custom_metadata, created_at FROM {table} WHERE {where} ORDER BY version DESC LIMIT 1"
        )
        return sql, params

    def _build_list_artifact_keys_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        session_id: str | None,
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table = validate_spanner_identifier(store._artifact_table, "artifact_table")
        params: dict[str, Any] = {"app_name": app_name, "user_id": user_id}
        if session_id is None:
            where = "app_name = @app_name AND user_id = @user_id AND session_id IS NULL"
        else:
            where = "app_name = @app_name AND user_id = @user_id AND (session_id = @session_id OR session_id IS NULL)"
            params["session_id"] = session_id
        return f"SELECT DISTINCT filename FROM {table} WHERE {where} ORDER BY filename ASC", params

    def _build_list_artifact_versions_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None,
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table = validate_spanner_identifier(store._artifact_table, "artifact_table")
        where, params = _artifact_scope_where(app_name, user_id, filename, session_id)
        sql = (
            "SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
            f"custom_metadata, created_at FROM {table} WHERE {where} ORDER BY version ASC"
        )
        return sql, params

    def _build_delete_artifact_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None,
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table = validate_spanner_identifier(store._artifact_table, "artifact_table")
        where, params = _artifact_scope_where(app_name, user_id, filename, session_id)
        return f"DELETE FROM {table} WHERE {where}", params

    def _build_get_next_version_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None,
    ) -> tuple[str, dict[str, Any]]:
        store = cast("Any", self)
        table = validate_spanner_identifier(store._artifact_table, "artifact_table")
        where, params = _artifact_scope_where(app_name, user_id, filename, session_id)
        return f"SELECT COALESCE(MAX(version), -1) AS max_ver FROM {table} WHERE {where}", params

    def _build_delete_older_than_sqls_and_params(
        self,
        before: datetime,
        app_name: str | None,
    ) -> tuple[str, str, dict[str, Any]]:
        store = cast("Any", self)
        table = validate_spanner_identifier(store._artifact_table, "artifact_table")
        cutoff = parse_utc_timestamp(before).isoformat()
        where = "created_at < @before" + (" AND app_name = @app_name" if app_name is not None else "")
        params: dict[str, Any] = {"before": cutoff}
        if app_name is not None:
            params["app_name"] = app_name
        select_sql = (
            "SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
            f"custom_metadata, created_at FROM {table} WHERE {where} ORDER BY created_at ASC, version ASC"
        )
        return select_sql, f"DELETE FROM {table} WHERE {where}", params


class SpannerSyncADKArtifactStore(_SpannerADKArtifactMixin, _BaseSyncADKArtifactStore["SpannerSyncConfig"]):
    """Cloud Spanner synchronous ADK artifact metadata store returning ``ArtifactReference``."""

    __slots__ = ("_json_deserializer", "_json_serializer", "_storage_backend")

    def __init__(
        self,
        config: SpannerSyncConfig,
        *,
        read_config: SpannerSyncConfig | None = None,
        storage_backend: ObjectStoreBackend | str | None = None,
    ) -> None:
        super().__init__(config, read_config=read_config)
        self._storage_backend = self._init_storage(config, storage_backend)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    @property
    def storage_backend(self) -> ObjectStoreBackend | None:
        """Return the configured ``ObjectStoreBackend``, if any."""
        return self._storage_backend

    def create_table(self) -> None:
        """Create the Spanner artifact metadata table inside a writable session."""
        ddl = _artifact_ddl(self._config, self._artifact_table)
        with self._config.provide_session(transaction=False) as driver:
            driver.execute(ddl)

    def insert_artifact(self, record: StoredArtifact) -> None:
        """Persist an artifact metadata row inside a ``ReadWrite`` transaction."""
        sql, params = self._build_insert_artifact_sql_and_params(record)
        with self._config.provide_session(transaction=True) as driver:
            driver.execute(sql, params)

    def get_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> StoredArtifact | None:
        """Fetch artifact version metadata via a lock-free snapshot."""
        sql, params = self._build_get_artifact_sql_and_params(app_name, user_id, filename, session_id, version)
        try:
            with self._provide_replica_session(primary=False, transaction=False, read_only=True) as driver:
                row = driver.select_one_or_none(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return None
            raise
        return _row_to_stored_artifact(dict(row), deserializer=self._json_deserializer) if row is not None else None

    def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """List distinct artifact filenames via a lock-free snapshot."""
        sql, params = self._build_list_artifact_keys_sql_and_params(app_name, user_id, session_id)
        try:
            with self._provide_replica_session(primary=False, transaction=False, read_only=True) as driver:
                rows = driver.select(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return []
            raise
        return [str(row["filename"]) for row in rows if row.get("filename") is not None]

    def list_artifact_versions(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[StoredArtifact]:
        """List all version metadata rows via a lock-free snapshot."""
        sql, params = self._build_list_artifact_versions_sql_and_params(app_name, user_id, filename, session_id)
        try:
            with self._provide_replica_session(primary=False, transaction=False, read_only=True) as driver:
                rows = driver.select(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return []
            raise
        return [_row_to_stored_artifact(dict(row), deserializer=self._json_deserializer) for row in rows]

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
            with self._config.provide_session(transaction=True) as driver:
                driver.execute(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return existing
            raise
        return existing

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
            with self._provide_replica_session(primary=True, transaction=False, read_only=True) as driver:
                row = driver.select_one_or_none(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return 0
            raise
        max_ver = int(row.get("max_ver", -1)) if row is not None and row.get("max_ver") is not None else -1
        return max_ver + 1

    def delete_artifacts_older_than(self, before: datetime, app_name: str | None = None) -> list[StoredArtifact]:
        """Delete artifact metadata rows created strictly before ``before``."""
        select_sql, delete_sql, params = self._build_delete_older_than_sqls_and_params(before, app_name)
        try:
            with self._config.provide_session(transaction=True) as driver:
                rows = [dict(row) for row in driver.select(select_sql, params)]
                driver.execute(delete_sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return []
            raise
        return [_row_to_stored_artifact(row, deserializer=self._json_deserializer) for row in rows]

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
        """Write binary payload to ``storage_backend``, record Spanner metadata, and return ``ArtifactReference``."""
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
        """Return a storage-neutral ``ArtifactReference`` for a stored artifact."""
        record = self.get_artifact(app_name, user_id, filename, session_id=session_id, version=version)
        return self._stored_artifact_to_ref(dict(record)) if record is not None else None


class SpannerAsyncADKArtifactStore(_SpannerADKArtifactMixin, _BaseAsyncADKArtifactStore["SpannerAsyncConfig"]):
    """Cloud Spanner asynchronous ADK artifact metadata store returning ``ArtifactReference``."""

    __slots__ = ("_json_deserializer", "_json_serializer", "_storage_backend")

    def __init__(
        self,
        config: SpannerAsyncConfig,
        *,
        read_config: SpannerAsyncConfig | None = None,
        storage_backend: ObjectStoreBackend | str | None = None,
    ) -> None:
        super().__init__(config, read_config=read_config)
        self._storage_backend = self._init_storage(config, storage_backend)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    @property
    def storage_backend(self) -> ObjectStoreBackend | None:
        """Return the configured ``ObjectStoreBackend``, if any."""
        return self._storage_backend

    async def create_table(self) -> None:
        """Create the Spanner artifact metadata table asynchronously."""
        ddl = _artifact_ddl(self._config, self._artifact_table)
        async with self._config.provide_session(transaction=False) as driver:
            await driver.execute(ddl)

    async def insert_artifact(self, record: StoredArtifact) -> None:
        """Persist an artifact metadata row asynchronously."""
        sql, params = self._build_insert_artifact_sql_and_params(record)
        async with self._config.provide_session(transaction=True) as driver:
            await driver.execute(sql, params)

    async def get_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> StoredArtifact | None:
        """Fetch artifact version metadata asynchronously via a lock-free snapshot."""
        sql, params = self._build_get_artifact_sql_and_params(app_name, user_id, filename, session_id, version)
        try:
            async with self._provide_replica_session(primary=False, transaction=False, read_only=True) as driver:
                row = await driver.select_one_or_none(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return None
            raise
        return _row_to_stored_artifact(dict(row), deserializer=self._json_deserializer) if row is not None else None

    async def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """List distinct artifact filenames asynchronously via a lock-free snapshot."""
        sql, params = self._build_list_artifact_keys_sql_and_params(app_name, user_id, session_id)
        try:
            async with self._provide_replica_session(primary=False, transaction=False, read_only=True) as driver:
                rows = await driver.select(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return []
            raise
        return [str(row["filename"]) for row in rows if row.get("filename") is not None]

    async def list_artifact_versions(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[StoredArtifact]:
        """List all version metadata rows asynchronously via a lock-free snapshot."""
        sql, params = self._build_list_artifact_versions_sql_and_params(app_name, user_id, filename, session_id)
        try:
            async with self._provide_replica_session(primary=False, transaction=False, read_only=True) as driver:
                rows = await driver.select(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return []
            raise
        return [_row_to_stored_artifact(dict(row), deserializer=self._json_deserializer) for row in rows]

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
            async with self._config.provide_session(transaction=True) as driver:
                await driver.execute(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return existing
            raise
        return existing

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
            async with self._provide_replica_session(primary=True, transaction=False, read_only=True) as driver:
                row = await driver.select_one_or_none(sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return 0
            raise
        max_ver = int(row.get("max_ver", -1)) if row is not None and row.get("max_ver") is not None else -1
        return max_ver + 1

    async def delete_artifacts_older_than(self, before: datetime, app_name: str | None = None) -> list[StoredArtifact]:
        """Delete aged artifact metadata rows asynchronously."""
        select_sql, delete_sql, params = self._build_delete_older_than_sqls_and_params(before, app_name)
        try:
            async with self._config.provide_session(transaction=True) as driver:
                raw_rows = await driver.select(select_sql, params)
                rows = [dict(row) for row in raw_rows]
                await driver.execute(delete_sql, params)
        except Exception as exc:
            if is_spanner_table_missing(exc):
                return []
            raise
        return [_row_to_stored_artifact(row, deserializer=self._json_deserializer) for row in rows]

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
        """Write binary payload to ``storage_backend`` and record metadata asynchronously."""
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
        return self._stored_artifact_to_ref(dict(record)) if record is not None else None
