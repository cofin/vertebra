"""Cloud Spanner synchronous and asynchronous ADK session, memory, and artifact stores."""

import math
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal, TypedDict, cast

from sqlspec.extensions.adk.store import BaseAsyncADKStore, BaseSyncADKStore
from sqlspec.utils.serializers import from_json, to_json

from vertebra.extensions.sqlspec.adapters.spanner.core import (
    normalize_spanner_dialect,
    object_to_artifact_reference,
    parse_utc_timestamp,
    resolve_storage_backend,
    validate_spanner_identifier,
)
from vertebra.extensions.sqlspec.storage import ObjectStoreBackend
from vertebra.storage import ArtifactReference

if TYPE_CHECKING:
    from sqlspec.extensions.adk.artifact.store import BaseAsyncADKArtifactStore as _BaseAsyncADKArtifactStore
    from sqlspec.extensions.adk.artifact.store import BaseSyncADKArtifactStore as _BaseSyncADKArtifactStore
    from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore as _BaseAsyncADKMemoryStore
    from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore as _BaseSyncADKMemoryStore
else:
    try:
        from sqlspec.extensions.adk.artifact.store import BaseAsyncADKArtifactStore as _BaseAsyncADKArtifactStore
        from sqlspec.extensions.adk.artifact.store import BaseSyncADKArtifactStore as _BaseSyncADKArtifactStore
    except ImportError:

        class _FallbackArtifactStoreBase[ConfigT]:
            """Fallback ADK artifact store base used when ``google.adk`` optional types are absent."""

            __slots__ = ("_artifact_table", "_config")

            def __init__(self, config: ConfigT) -> None:
                self._config = config
                ext = cast("dict[str, Any]", getattr(config, "extension_config", {}) or {})
                adk = cast("dict[str, Any]", ext.get("adk", {}) or {})
                self._artifact_table = str(adk.get("artifact_table") or "adk_artifact")

            @property
            def config(self) -> ConfigT:
                """Return the underlying database configuration."""
                return self._config

            @property
            def artifact_table(self) -> str:
                """Return the configured artifact metadata table name."""
                return self._artifact_table

        class _FallbackSyncADKArtifactStore[ConfigT](_FallbackArtifactStoreBase[ConfigT]):
            """Fallback synchronous ADK artifact store base."""

        class _FallbackAsyncADKArtifactStore[ConfigT](_FallbackArtifactStoreBase[ConfigT]):
            """Fallback asynchronous ADK artifact store base."""

        _BaseSyncADKArtifactStore = _FallbackSyncADKArtifactStore
        _BaseAsyncADKArtifactStore = _FallbackAsyncADKArtifactStore

    try:
        from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore as _BaseAsyncADKMemoryStore
        from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore as _BaseSyncADKMemoryStore
    except ImportError:

        class _FallbackMemoryStoreBase[ConfigT]:
            """Fallback ADK memory store base used when SQLSpec memory extras are absent."""

            __slots__ = (
                "_config",
                "_enable_bm25",
                "_enabled",
                "_max_results",
                "_memory_table",
                "_owner_id_column_ddl",
                "_owner_id_column_name",
                "_scann_num_leaves",
                "_scann_quantizer",
                "_use_fts",
                "_vector_dimensions",
                "_vector_index_type",
            )

            def __init__(self, config: ConfigT) -> None:
                self._config = config
                ext = cast("dict[str, Any]", getattr(config, "extension_config", {}) or {})
                adk = cast("dict[str, Any]", ext.get("adk", {}) or {})
                self._enabled = bool(adk.get("enable_memory", True))
                self._memory_table = str(adk.get("memory_table") or "adk_memory")
                self._use_fts = bool(adk.get("memory_use_fts", False))
                self._max_results = int(adk.get("memory_max_results") or 20)
                self._vector_index_type = str(adk.get("vector_index_type") or "scann")
                self._vector_dimensions = int(adk.get("vector_dimensions") or 768)
                self._enable_bm25 = bool(adk.get("enable_bm25", False))
                self._scann_num_leaves = int(adk.get("scann_num_leaves") or 100)
                self._scann_quantizer = str(adk.get("scann_quantizer") or "SQ8")
                owner_col = adk.get("owner_id_column")
                self._owner_id_column_ddl = str(owner_col) if owner_col else None
                self._owner_id_column_name = self._owner_id_column_ddl.split()[0] if self._owner_id_column_ddl else None

            @property
            def config(self) -> ConfigT:
                """Return the underlying database configuration."""
                return self._config

            @property
            def memory_table(self) -> str:
                """Return the configured memory table name."""
                return self._memory_table

            @property
            def is_enabled(self) -> bool:
                """Return whether ADK memory storage is enabled."""
                return self._enabled

        class _FallbackSyncADKMemoryStore[ConfigT](_FallbackMemoryStoreBase[ConfigT]):
            """Fallback synchronous ADK memory store base."""

        class _FallbackAsyncADKMemoryStore[ConfigT](_FallbackMemoryStoreBase[ConfigT]):
            """Fallback asynchronous ADK memory store base."""

        _BaseSyncADKMemoryStore = _FallbackSyncADKMemoryStore
        _BaseAsyncADKMemoryStore = _FallbackAsyncADKMemoryStore

type StoredSession = dict[str, Any]
type StoredEvent = dict[str, Any]
type StoredMemory = dict[str, Any]
type StoredArtifact = dict[str, Any]
type SessionOrderBy = Literal["create_time", "update_time"]

__all__ = (
    "SpannerADKConfig",
    "SpannerAsyncADKArtifactStore",
    "SpannerAsyncADKMemoryStore",
    "SpannerAsyncADKStore",
    "SpannerSyncADKArtifactStore",
    "SpannerSyncADKMemoryStore",
    "SpannerSyncADKStore",
)


class SpannerADKConfig(TypedDict, total=False):
    """Typed ADK extension configuration keys honored by Cloud Spanner stores."""

    session_table: str
    """Cloud Spanner table name for ADK sessions."""
    events_table: str
    """Cloud Spanner table name for ADK session events."""
    app_state_table: str
    """Cloud Spanner table name for application-scoped ADK state."""
    user_state_table: str
    """Cloud Spanner table name for user-scoped ADK state."""
    metadata_table: str
    """Cloud Spanner table name for ADK session metadata."""
    memory_table: str
    """Cloud Spanner table name for ADK memory entries."""
    artifact_table: str
    """Cloud Spanner table name for ADK artifact metadata."""
    owner_id_column: str
    """Column name referencing the tenant or owner identifier."""
    enable_sessions: bool
    """Enable ADK session persistence in Cloud Spanner."""
    enable_memory: bool
    """Enable ADK long-term memory persistence in Cloud Spanner."""
    memory_use_fts: bool
    """Enable Cloud Spanner full-text search indexes for memory retrieval."""
    memory_max_results: int
    """Default maximum number of memory entries returned by search queries."""
    vector_index_type: str
    """Cloud Spanner vector index type for ADK memory embeddings."""
    vector_dimensions: int
    """Embedding dimensionality for ADK memory vector similarity columns."""
    scann_num_leaves: int
    """Number of ScaNN index tree leaves for Cloud Spanner vector search."""
    scann_quantizer: str
    """Quantizer configuration string for Cloud Spanner ScaNN indexes."""
    manage_schema: bool
    """Create or migrate ADK tables automatically on startup."""
    create_schema: bool
    """Create ADK tables when they do not already exist."""


def _config_dialect(config: Any) -> Literal["googlesql", "spangres"]:
    return normalize_spanner_dialect(
        getattr(config, "spanner_dialect", None) or getattr(config, "dialect", "googlesql")
    )


def _driver_features(config: Any) -> dict[str, Any]:
    features = getattr(config, "driver_features", None)
    return dict(features) if isinstance(features, Mapping) else {}


def _event_ttl_days(config: Any) -> int:
    features = _driver_features(config)
    raw = features.get("adk_event_ttl_days", 30)
    try:
        parsed = int(raw)
    except (TypeError, ValueError):
        return 30
    return parsed if parsed > 0 else 30


def _decode_json_dict(value: Any) -> dict[str, Any]:
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
        parsed = from_json(stripped)
        return parsed if isinstance(parsed, dict) else {"value": parsed}
    return dict(value) if hasattr(value, "items") else {}


def _encode_json_text(value: Any) -> str:
    if value is None:
        return "{}"
    if isinstance(value, str):
        return value
    return to_json(value)


def _decode_embedding(value: Any) -> list[float] | None:
    if value is None:
        return None
    if isinstance(value, str):
        parsed = from_json(value)
        return [float(x) for x in parsed] if isinstance(parsed, Sequence) else None
    if isinstance(value, Sequence):
        return [float(x) for x in value]
    return None


def _cosine_similarity(vec_a: Sequence[float], vec_b: Sequence[float]) -> float:
    if not vec_a or not vec_b or len(vec_a) != len(vec_b):
        return 0.0
    dot = sum(float(a) * float(b) for a, b in zip(vec_a, vec_b, strict=True))
    norm_a = math.sqrt(sum(float(a) * float(a) for a in vec_a))
    norm_b = math.sqrt(sum(float(val) * float(val) for val in vec_b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)


def _keyword_score(query: str, text: str) -> float:
    terms = [term for term in query.lower().split() if term]
    if not terms:
        return 1.0
    haystack = text.lower()
    hits = sum(1 for term in terms if term in haystack)
    return hits / len(terms)


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
) -> str:
    table = validate_spanner_identifier(session_table, "session_table")
    dialect = _config_dialect(config)
    owner_col = f"{owner_id_ddl}, " if owner_id_ddl else ""
    if dialect == "spangres":
        return (
            f"CREATE TABLE IF NOT EXISTS {table} ("
            "id VARCHAR(128) NOT NULL, "
            "app_name VARCHAR(128) NOT NULL, "
            "user_id VARCHAR(128) NOT NULL, "
            f"{owner_col}"
            "state JSONB NOT NULL, "
            "create_time TIMESTAMPTZ NOT NULL, "
            "update_time TIMESTAMPTZ NOT NULL, "
            "PRIMARY KEY (id)"
            ")"
        )
    return (
        f"CREATE TABLE IF NOT EXISTS {table} ("
        "id STRING(128) NOT NULL, "
        "app_name STRING(128) NOT NULL, "
        "user_id STRING(128) NOT NULL, "
        f"{owner_col}"
        "state JSON NOT NULL, "
        "create_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true), "
        "update_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true)"
        ") PRIMARY KEY (id)"
    )


def _build_events_ddl(
    config: Any,
    session_table: str,
    events_table: str,
) -> str:
    parent = validate_spanner_identifier(session_table, "session_table")
    table = validate_spanner_identifier(events_table, "events_table")
    dialect = _config_dialect(config)
    ttl_days = _event_ttl_days(config)
    if dialect == "spangres":
        return (
            f"CREATE TABLE IF NOT EXISTS {table} ("
            "id VARCHAR(128) NOT NULL, "
            "app_name VARCHAR(128) NOT NULL, "
            "user_id VARCHAR(128) NOT NULL, "
            "session_id VARCHAR(128) NOT NULL, "
            "invocation_id VARCHAR(128) NOT NULL, "
            "timestamp TIMESTAMPTZ NOT NULL, "
            "event_data JSONB NOT NULL, "
            "PRIMARY KEY (session_id, id)"
            f") INTERLEAVE IN PARENT {parent} ON DELETE CASCADE "
            f"TTL INTERVAL '{ttl_days} days' ON timestamp"
        )
    return (
        f"CREATE TABLE IF NOT EXISTS {table} ("
        "id STRING(128) NOT NULL, "
        "app_name STRING(128) NOT NULL, "
        "user_id STRING(128) NOT NULL, "
        "session_id STRING(128) NOT NULL, "
        "invocation_id STRING(128) NOT NULL, "
        "timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true), "
        "event_data JSON NOT NULL"
        f") PRIMARY KEY (session_id, id), "
        f"INTERLEAVE IN PARENT {parent} ON DELETE CASCADE, "
        f"ROW DELETION POLICY (OLDER_THAN(timestamp, INTERVAL {ttl_days} DAY))"
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


def _row_to_stored_session(row: Mapping[str, Any]) -> StoredSession:
    return {
        "id": str(row.get("id") or ""),
        "app_name": str(row.get("app_name") or ""),
        "user_id": str(row.get("user_id") or ""),
        "state": _decode_json_dict(row.get("state")),
        "create_time": parse_utc_timestamp(row.get("create_time")),
        "update_time": parse_utc_timestamp(row.get("update_time")),
    }


def _row_to_stored_event(row: Mapping[str, Any]) -> StoredEvent:
    event_data = _decode_json_dict(row.get("event_data"))
    if not event_data and row.get("event_json") is not None:
        event_data = _decode_json_dict(row.get("event_json"))
    return {
        "id": str(row.get("id") or ""),
        "app_name": str(row.get("app_name") or ""),
        "user_id": str(row.get("user_id") or ""),
        "session_id": str(row.get("session_id") or ""),
        "invocation_id": str(row.get("invocation_id") or ""),
        "timestamp": parse_utc_timestamp(row.get("timestamp")),
        "event_data": event_data,
    }


def _row_to_stored_memory(row: Mapping[str, Any]) -> StoredMemory:
    return {
        "id": str(row.get("id") or ""),
        "session_id": str(row.get("session_id") or ""),
        "app_name": str(row.get("app_name") or ""),
        "user_id": str(row.get("user_id") or ""),
        "scope": str(row.get("scope") or "user"),
        "event_id": str(row.get("event_id") or ""),
        "author": row.get("author"),
        "timestamp": parse_utc_timestamp(row.get("timestamp")),
        "content_json": _decode_json_dict(row.get("content_json")),
        "content_text": str(row.get("content_text") or ""),
        "metadata_json": _decode_json_dict(row.get("metadata_json")) if row.get("metadata_json") is not None else None,
        "inserted_at": parse_utc_timestamp(row.get("inserted_at")),
        "embedding": _decode_embedding(row.get("embedding")),
    }


def _row_to_stored_artifact(row: Mapping[str, Any]) -> StoredArtifact:
    custom_raw = row.get("custom_metadata")
    return {
        "app_name": str(row.get("app_name") or ""),
        "user_id": str(row.get("user_id") or ""),
        "session_id": row.get("session_id"),
        "filename": str(row.get("filename") or ""),
        "version": int(row.get("version", 0) or 0),
        "mime_type": row.get("mime_type"),
        "canonical_uri": str(row.get("canonical_uri") or ""),
        "custom_metadata": _decode_json_dict(custom_raw) if custom_raw is not None else None,
        "created_at": parse_utc_timestamp(row.get("created_at")),
    }


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


class SpannerSyncADKStore(BaseSyncADKStore[Any]):
    """Cloud Spanner synchronous ADK session and event store using lock-free snapshots for reads."""

    __slots__ = ()

    adapter_name = "vertebra_spanner"

    def _sessions_table_ddl(self) -> str:
        return _build_session_ddl(self._config, self._session_table, self._owner_id_column_ddl)

    def _events_table_ddl(self) -> str:
        return _build_events_ddl(self._config, self._session_table, self._events_table)

    def _app_states_table_ddl(self) -> str:
        return _build_app_state_ddl(self._config, self._app_state_table)

    def _user_states_table_ddl(self) -> str:
        return _build_user_state_ddl(self._config, self._user_state_table)

    def _metadata_table_ddl(self) -> str:
        return _build_metadata_ddl(self._config, self._metadata_table)

    def _drop_tables_sql(self) -> list[str]:
        return [
            f"DROP TABLE IF EXISTS {validate_spanner_identifier(self._events_table, 'events_table')}",
            f"DROP TABLE IF EXISTS {validate_spanner_identifier(self._session_table, 'session_table')}",
        ]

    def _drop_app_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {validate_spanner_identifier(self._app_state_table, 'app_state_table')}"

    def _drop_user_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {validate_spanner_identifier(self._user_state_table, 'user_state_table')}"

    def _drop_metadata_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {validate_spanner_identifier(self._metadata_table, 'metadata_table')}"

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
        with self._config.provide_session(transaction=True) as driver:
            for ddl in self.ddl_statements():
                driver.execute(ddl)

    def create_session(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
        state: dict[str, Any],
        owner_id: Any | None = None,
    ) -> Any:
        """Insert a new ADK session row inside a ``ReadWrite`` transaction."""
        now = datetime.now(UTC)
        now_iso = now.isoformat()
        table = validate_spanner_identifier(self._session_table, "session_table")
        dialect = _config_dialect(self._config)
        state_expr = _json_expr("state", dialect)
        state_json = _encode_json_text(state)
        if self._owner_id_column_name:
            owner_col = validate_spanner_identifier(self._owner_id_column_name, "owner_id_column")
            sql = (
                f"INSERT INTO {table} (id, app_name, user_id, {owner_col}, state, create_time, update_time) "
                f"VALUES (@id, @app_name, @user_id, @owner_id, {state_expr}, @create_time, @update_time)"
            )
            params: dict[str, Any] = {
                "id": session_id,
                "app_name": app_name,
                "user_id": user_id,
                "owner_id": owner_id,
                "state": state_json,
                "create_time": now_iso,
                "update_time": now_iso,
            }
        else:
            sql = (
                f"INSERT INTO {table} (id, app_name, user_id, state, create_time, update_time) "
                f"VALUES (@id, @app_name, @user_id, {state_expr}, @create_time, @update_time)"
            )
            params = {
                "id": session_id,
                "app_name": app_name,
                "user_id": user_id,
                "state": state_json,
                "create_time": now_iso,
                "update_time": now_iso,
            }
        with self._config.provide_session(transaction=True) as driver:
            driver.execute(sql, params)
        return cast(
            "Any",
            {
                "id": session_id,
                "app_name": app_name,
                "user_id": user_id,
                "state": dict(state),
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
    ) -> Any:
        """Load an ADK session via a lock-free ``ReadOnly`` snapshot."""
        _ = renew_for
        table = validate_spanner_identifier(self._session_table, "session_table")
        sql = (
            f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table} "
            "WHERE id = @id AND app_name = @app_name AND user_id = @user_id LIMIT 1"
        )
        with self._config.provide_session(transaction=False) as driver:
            row = driver.select_one_or_none(sql, {"id": session_id, "app_name": app_name, "user_id": user_id})
        return cast("Any", _row_to_stored_session(dict(row))) if row is not None else None

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Update JSON state and ``update_time`` inside a ``ReadWrite`` transaction."""
        table = validate_spanner_identifier(self._session_table, "session_table")
        dialect = _config_dialect(self._config)
        state_expr = _json_expr("state", dialect)
        sql = (
            f"UPDATE {table} SET state = {state_expr}, update_time = @update_time "
            "WHERE id = @id AND app_name = @app_name AND user_id = @user_id"
        )
        with self._config.provide_session(transaction=True) as driver:
            driver.execute(
                sql,
                {
                    "id": session_id,
                    "app_name": app_name,
                    "user_id": user_id,
                    "state": _encode_json_text(state),
                    "update_time": datetime.now(UTC).isoformat(),
                },
            )

    def list_sessions(
        self,
        app_name: str,
        user_id: str | None = None,
        *,
        order_by: SessionOrderBy = "update_time",
        descending: bool = True,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[Any]:
        """List ADK sessions via a lock-free ``ReadOnly`` snapshot."""
        table = validate_spanner_identifier(self._session_table, "session_table")
        order_col = "create_time" if order_by == "create_time" else "update_time"
        direction = "DESC" if descending else "ASC"
        where = "app_name = @app_name" + (" AND user_id = @user_id" if user_id is not None else "")
        limit_sql = " LIMIT @limit" if limit is not None else ""
        offset_sql = " OFFSET @offset" if offset is not None else ""
        sql = (
            f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table} "
            f"WHERE {where} ORDER BY {order_col} {direction}{limit_sql}{offset_sql}"
        )
        params: dict[str, Any] = {"app_name": app_name}
        if user_id is not None:
            params["user_id"] = user_id
        if limit is not None:
            params["limit"] = int(limit)
        if offset is not None:
            params["offset"] = int(offset)
        with self._config.provide_session(transaction=False) as driver:
            rows = driver.select(sql, params)
        return [cast("Any", _row_to_stored_session(dict(row))) for row in rows]

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete a session and its interleaved events inside a ``ReadWrite`` transaction."""
        sessions_table = validate_spanner_identifier(self._session_table, "session_table")
        events_table = validate_spanner_identifier(self._events_table, "events_table")
        params = {"id": session_id, "app_name": app_name, "user_id": user_id}
        with self._config.provide_session(transaction=True) as driver:
            driver.execute(
                f"DELETE FROM {events_table} WHERE session_id = @id AND app_name = @app_name AND user_id = @user_id",
                params,
            )
            driver.execute(
                f"DELETE FROM {sessions_table} WHERE id = @id AND app_name = @app_name AND user_id = @user_id",
                params,
            )

    def append_event(self, event_record: Any) -> None:
        """Append an ADK event row inside a ``ReadWrite`` transaction."""
        events_table = validate_spanner_identifier(self._events_table, "events_table")
        dialect = _config_dialect(self._config)
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
            "event_data": _encode_json_text(event_record.get("event_data", {})),
        }
        with self._config.provide_session(transaction=True) as driver:
            driver.execute(sql, params)

    def append_event_and_update_state(
        self,
        event_record: Any,
        app_name: str,
        user_id: str,
        session_id: str,
        state: dict[str, Any],
        *,
        app_state: dict[str, Any] | None = None,
        user_state: dict[str, Any] | None = None,
    ) -> Any:
        """Atomically append an event and update session, app, and user state in one transaction."""
        dialect = _config_dialect(self._config)
        events_table = validate_spanner_identifier(self._events_table, "events_table")
        sessions_table = validate_spanner_identifier(self._session_table, "session_table")
        now = datetime.now(UTC)
        now_iso = now.isoformat()
        with self._config.provide_session(transaction=True) as driver:
            driver.execute(
                f"INSERT INTO {events_table} "
                "(id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
                f"VALUES (@id, @app_name, @user_id, @session_id, @invocation_id, @timestamp, "
                f"{_json_expr('event_data', dialect)})",
                {
                    "id": event_record["id"],
                    "app_name": event_record["app_name"],
                    "user_id": event_record["user_id"],
                    "session_id": event_record["session_id"],
                    "invocation_id": event_record["invocation_id"],
                    "timestamp": parse_utc_timestamp(event_record["timestamp"]).isoformat(),
                    "event_data": _encode_json_text(event_record.get("event_data", {})),
                },
            )
            driver.execute(
                f"UPDATE {sessions_table} SET state = {_json_expr('state', dialect)}, update_time = @update_time "
                "WHERE id = @id AND app_name = @app_name AND user_id = @user_id",
                {
                    "id": session_id,
                    "app_name": app_name,
                    "user_id": user_id,
                    "state": _encode_json_text(state),
                    "update_time": now_iso,
                },
            )
            if app_state is not None:
                app_table = validate_spanner_identifier(self._app_state_table, "app_state_table")
                driver.execute(
                    _upsert_app_state_sql(app_table, dialect),
                    {"app_name": app_name, "state": _encode_json_text(app_state), "update_time": now_iso},
                )
            if user_state is not None:
                user_table = validate_spanner_identifier(self._user_state_table, "user_state_table")
                driver.execute(
                    _upsert_user_state_sql(user_table, dialect),
                    {
                        "app_name": app_name,
                        "user_id": user_id,
                        "state": _encode_json_text(user_state),
                        "update_time": now_iso,
                    },
                )
        session = self.get_session(app_name, user_id, session_id)
        if session is not None:
            return session
        return cast(
            "Any",
            {
                "id": session_id,
                "app_name": app_name,
                "user_id": user_id,
                "state": dict(state),
                "create_time": now,
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
    ) -> list[Any]:
        """Fetch chronological ADK events for a session via a lock-free snapshot."""
        events_table = validate_spanner_identifier(self._events_table, "events_table")
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
        with self._config.provide_session(transaction=False) as driver:
            rows = driver.select(sql, params)
        return [cast("Any", _row_to_stored_event(dict(row))) for row in rows]

    def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Load shared application-scoped ADK state via a lock-free snapshot."""
        table = validate_spanner_identifier(self._app_state_table, "app_state_table")
        sql = f"SELECT state FROM {table} WHERE app_name = @app_name LIMIT 1"
        with self._config.provide_session(transaction=False) as driver:
            row = driver.select_one_or_none(sql, {"app_name": app_name})
        return _decode_json_dict(row.get("state")) if row is not None else None

    def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Upsert application-scoped ADK state inside a ``ReadWrite`` transaction."""
        table = validate_spanner_identifier(self._app_state_table, "app_state_table")
        dialect = _config_dialect(self._config)
        sql = _upsert_app_state_sql(table, dialect)
        with self._config.provide_session(transaction=True) as driver:
            driver.execute(
                sql,
                {"app_name": app_name, "state": _encode_json_text(state), "update_time": datetime.now(UTC).isoformat()},
            )

    def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Load user-scoped ADK state via a lock-free snapshot."""
        table = validate_spanner_identifier(self._user_state_table, "user_state_table")
        sql = f"SELECT state FROM {table} WHERE app_name = @app_name AND user_id = @user_id LIMIT 1"
        with self._config.provide_session(transaction=False) as driver:
            row = driver.select_one_or_none(sql, {"app_name": app_name, "user_id": user_id})
        return _decode_json_dict(row.get("state")) if row is not None else None

    def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Upsert user-scoped ADK state inside a ``ReadWrite`` transaction."""
        table = validate_spanner_identifier(self._user_state_table, "user_state_table")
        dialect = _config_dialect(self._config)
        sql = _upsert_user_state_sql(table, dialect)
        with self._config.provide_session(transaction=True) as driver:
            driver.execute(
                sql,
                {
                    "app_name": app_name,
                    "user_id": user_id,
                    "state": _encode_json_text(state),
                    "update_time": datetime.now(UTC).isoformat(),
                },
            )

    def get_metadata(self, key: str) -> str | None:
        """Retrieve scalar ADK store metadata via a lock-free snapshot."""
        table = validate_spanner_identifier(self._metadata_table, "metadata_table")
        key_col = _metadata_key_col(_config_dialect(self._config))
        sql = f"SELECT value FROM {table} WHERE {key_col} = @key LIMIT 1"
        with self._config.provide_session(transaction=False) as driver:
            row = driver.select_one_or_none(sql, {"key": key})
        return str(row["value"]) if row is not None and row.get("value") is not None else None

    def set_metadata(self, key: str, value: str) -> None:
        """Persist scalar ADK store metadata inside a ``ReadWrite`` transaction."""
        table = validate_spanner_identifier(self._metadata_table, "metadata_table")
        dialect = _config_dialect(self._config)
        sql = _set_metadata_sql(table, dialect)
        with self._config.provide_session(transaction=True) as driver:
            driver.execute(sql, {"key": key, "value": value, "update_time": datetime.now(UTC).isoformat()})

    def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Delete ADK events older than ``before``."""
        events_table = validate_spanner_identifier(self._events_table, "events_table")
        where = "timestamp < @before" + (" AND app_name = @app_name" if app_name is not None else "")
        params: dict[str, Any] = {"before": parse_utc_timestamp(before).isoformat()}
        if app_name is not None:
            params["app_name"] = app_name
        with self._config.provide_session(transaction=True) as driver:
            res = driver.execute(f"DELETE FROM {events_table} WHERE {where}", params)
        return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)

    def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete ADK sessions inactive since ``updated_before``."""
        sessions_table = validate_spanner_identifier(self._session_table, "session_table")
        where = "update_time < @before" + (" AND app_name = @app_name" if app_name is not None else "")
        params: dict[str, Any] = {"before": parse_utc_timestamp(updated_before).isoformat()}
        if app_name is not None:
            params["app_name"] = app_name
        with self._config.provide_session(transaction=True) as driver:
            res = driver.execute(f"DELETE FROM {sessions_table} WHERE {where}", params)
        return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)

    def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete user state rows inactive since ``updated_before``."""
        table = validate_spanner_identifier(self._user_state_table, "user_state_table")
        where = "update_time < @before" + (" AND app_name = @app_name" if app_name is not None else "")
        params: dict[str, Any] = {"before": parse_utc_timestamp(updated_before).isoformat()}
        if app_name is not None:
            params["app_name"] = app_name
        with self._config.provide_session(transaction=True) as driver:
            res = driver.execute(f"DELETE FROM {table} WHERE {where}", params)
        return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)


class SpannerAsyncADKStore(BaseAsyncADKStore[Any]):
    """Cloud Spanner asynchronous ADK session and event store using lock-free snapshots for reads."""

    __slots__ = ()

    adapter_name = "vertebra_spanner"

    async def _sessions_table_ddl(self) -> str:
        return _build_session_ddl(self._config, self._session_table, self._owner_id_column_ddl)

    async def _events_table_ddl(self) -> str:
        return _build_events_ddl(self._config, self._session_table, self._events_table)

    async def _app_states_table_ddl(self) -> str:
        return _build_app_state_ddl(self._config, self._app_state_table)

    async def _user_states_table_ddl(self) -> str:
        return _build_user_state_ddl(self._config, self._user_state_table)

    async def _metadata_table_ddl(self) -> str:
        return _build_metadata_ddl(self._config, self._metadata_table)

    def _drop_tables_sql(self) -> list[str]:
        return [
            f"DROP TABLE IF EXISTS {validate_spanner_identifier(self._events_table, 'events_table')}",
            f"DROP TABLE IF EXISTS {validate_spanner_identifier(self._session_table, 'session_table')}",
        ]

    def _drop_app_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {validate_spanner_identifier(self._app_state_table, 'app_state_table')}"

    def _drop_user_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {validate_spanner_identifier(self._user_state_table, 'user_state_table')}"

    def _drop_metadata_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {validate_spanner_identifier(self._metadata_table, 'metadata_table')}"

    async def create_tables(self) -> None:
        """Create all Spanner ADK tables asynchronously."""
        ddls = [
            await self._sessions_table_ddl(),
            await self._events_table_ddl(),
            await self._app_states_table_ddl(),
            await self._user_states_table_ddl(),
            await self._metadata_table_ddl(),
        ]
        async with self._config.provide_session(transaction=True) as driver:
            for ddl in ddls:
                await driver.execute(ddl)

    async def create_session(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
        state: dict[str, Any],
        owner_id: Any | None = None,
    ) -> Any:
        """Insert a new ADK session row asynchronously inside a ``ReadWrite`` transaction."""
        now = datetime.now(UTC)
        now_iso = now.isoformat()
        table = validate_spanner_identifier(self._session_table, "session_table")
        dialect = _config_dialect(self._config)
        state_expr = _json_expr("state", dialect)
        state_json = _encode_json_text(state)
        if self._owner_id_column_name:
            owner_col = validate_spanner_identifier(self._owner_id_column_name, "owner_id_column")
            sql = (
                f"INSERT INTO {table} (id, app_name, user_id, {owner_col}, state, create_time, update_time) "
                f"VALUES (@id, @app_name, @user_id, @owner_id, {state_expr}, @create_time, @update_time)"
            )
            params: dict[str, Any] = {
                "id": session_id,
                "app_name": app_name,
                "user_id": user_id,
                "owner_id": owner_id,
                "state": state_json,
                "create_time": now_iso,
                "update_time": now_iso,
            }
        else:
            sql = (
                f"INSERT INTO {table} (id, app_name, user_id, state, create_time, update_time) "
                f"VALUES (@id, @app_name, @user_id, {state_expr}, @create_time, @update_time)"
            )
            params = {
                "id": session_id,
                "app_name": app_name,
                "user_id": user_id,
                "state": state_json,
                "create_time": now_iso,
                "update_time": now_iso,
            }
        async with self._config.provide_session(transaction=True) as driver:
            await driver.execute(sql, params)
        return cast(
            "Any",
            {
                "id": session_id,
                "app_name": app_name,
                "user_id": user_id,
                "state": dict(state),
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
    ) -> Any:
        """Load an ADK session asynchronously via a lock-free ``ReadOnly`` snapshot."""
        _ = renew_for
        table = validate_spanner_identifier(self._session_table, "session_table")
        sql = (
            f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table} "
            "WHERE id = @id AND app_name = @app_name AND user_id = @user_id LIMIT 1"
        )
        async with self._config.provide_session(transaction=False) as driver:
            row = await driver.select_one_or_none(sql, {"id": session_id, "app_name": app_name, "user_id": user_id})
        return cast("Any", _row_to_stored_session(dict(row))) if row is not None else None

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Update JSON state asynchronously inside a ``ReadWrite`` transaction."""
        table = validate_spanner_identifier(self._session_table, "session_table")
        dialect = _config_dialect(self._config)
        state_expr = _json_expr("state", dialect)
        sql = (
            f"UPDATE {table} SET state = {state_expr}, update_time = @update_time "
            "WHERE id = @id AND app_name = @app_name AND user_id = @user_id"
        )
        async with self._config.provide_session(transaction=True) as driver:
            await driver.execute(
                sql,
                {
                    "id": session_id,
                    "app_name": app_name,
                    "user_id": user_id,
                    "state": _encode_json_text(state),
                    "update_time": datetime.now(UTC).isoformat(),
                },
            )

    async def list_sessions(
        self,
        app_name: str,
        user_id: str | None = None,
        *,
        order_by: SessionOrderBy = "update_time",
        descending: bool = True,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[Any]:
        """List ADK sessions asynchronously via a lock-free ``ReadOnly`` snapshot."""
        table = validate_spanner_identifier(self._session_table, "session_table")
        order_col = "create_time" if order_by == "create_time" else "update_time"
        direction = "DESC" if descending else "ASC"
        where = "app_name = @app_name" + (" AND user_id = @user_id" if user_id is not None else "")
        limit_sql = " LIMIT @limit" if limit is not None else ""
        offset_sql = " OFFSET @offset" if offset is not None else ""
        sql = (
            f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table} "
            f"WHERE {where} ORDER BY {order_col} {direction}{limit_sql}{offset_sql}"
        )
        params: dict[str, Any] = {"app_name": app_name}
        if user_id is not None:
            params["user_id"] = user_id
        if limit is not None:
            params["limit"] = int(limit)
        if offset is not None:
            params["offset"] = int(offset)
        async with self._config.provide_session(transaction=False) as driver:
            rows = await driver.select(sql, params)
        return [cast("Any", _row_to_stored_session(dict(row))) for row in rows]

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete a session and its interleaved events asynchronously."""
        sessions_table = validate_spanner_identifier(self._session_table, "session_table")
        events_table = validate_spanner_identifier(self._events_table, "events_table")
        params = {"id": session_id, "app_name": app_name, "user_id": user_id}
        async with self._config.provide_session(transaction=True) as driver:
            await driver.execute(
                f"DELETE FROM {events_table} WHERE session_id = @id AND app_name = @app_name AND user_id = @user_id",
                params,
            )
            await driver.execute(
                f"DELETE FROM {sessions_table} WHERE id = @id AND app_name = @app_name AND user_id = @user_id",
                params,
            )

    async def append_event(self, event_record: Any) -> None:
        """Append an ADK event row asynchronously."""
        events_table = validate_spanner_identifier(self._events_table, "events_table")
        dialect = _config_dialect(self._config)
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
            "event_data": _encode_json_text(event_record.get("event_data", {})),
        }
        async with self._config.provide_session(transaction=True) as driver:
            await driver.execute(sql, params)

    async def append_event_and_update_state(
        self,
        event_record: Any,
        app_name: str,
        user_id: str,
        session_id: str,
        state: dict[str, Any],
        *,
        app_state: dict[str, Any] | None = None,
        user_state: dict[str, Any] | None = None,
    ) -> Any:
        """Atomically append an event and update session, app, and user state asynchronously."""
        dialect = _config_dialect(self._config)
        events_table = validate_spanner_identifier(self._events_table, "events_table")
        sessions_table = validate_spanner_identifier(self._session_table, "session_table")
        now = datetime.now(UTC)
        now_iso = now.isoformat()
        async with self._config.provide_session(transaction=True) as driver:
            await driver.execute(
                f"INSERT INTO {events_table} "
                "(id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
                f"VALUES (@id, @app_name, @user_id, @session_id, @invocation_id, @timestamp, "
                f"{_json_expr('event_data', dialect)})",
                {
                    "id": event_record["id"],
                    "app_name": event_record["app_name"],
                    "user_id": event_record["user_id"],
                    "session_id": event_record["session_id"],
                    "invocation_id": event_record["invocation_id"],
                    "timestamp": parse_utc_timestamp(event_record["timestamp"]).isoformat(),
                    "event_data": _encode_json_text(event_record.get("event_data", {})),
                },
            )
            await driver.execute(
                f"UPDATE {sessions_table} SET state = {_json_expr('state', dialect)}, update_time = @update_time "
                "WHERE id = @id AND app_name = @app_name AND user_id = @user_id",
                {
                    "id": session_id,
                    "app_name": app_name,
                    "user_id": user_id,
                    "state": _encode_json_text(state),
                    "update_time": now_iso,
                },
            )
            if app_state is not None:
                app_table = validate_spanner_identifier(self._app_state_table, "app_state_table")
                await driver.execute(
                    _upsert_app_state_sql(app_table, dialect),
                    {"app_name": app_name, "state": _encode_json_text(app_state), "update_time": now_iso},
                )
            if user_state is not None:
                user_table = validate_spanner_identifier(self._user_state_table, "user_state_table")
                await driver.execute(
                    _upsert_user_state_sql(user_table, dialect),
                    {
                        "app_name": app_name,
                        "user_id": user_id,
                        "state": _encode_json_text(user_state),
                        "update_time": now_iso,
                    },
                )
        session = await self.get_session(app_name, user_id, session_id)
        if session is not None:
            return session
        return cast(
            "Any",
            {
                "id": session_id,
                "app_name": app_name,
                "user_id": user_id,
                "state": dict(state),
                "create_time": now,
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
    ) -> list[Any]:
        """Fetch chronological ADK events asynchronously via a lock-free snapshot."""
        events_table = validate_spanner_identifier(self._events_table, "events_table")
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
        async with self._config.provide_session(transaction=False) as driver:
            rows = await driver.select(sql, params)
        return [cast("Any", _row_to_stored_event(dict(row))) for row in rows]

    async def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Load shared application-scoped ADK state asynchronously via a lock-free snapshot."""
        table = validate_spanner_identifier(self._app_state_table, "app_state_table")
        sql = f"SELECT state FROM {table} WHERE app_name = @app_name LIMIT 1"
        async with self._config.provide_session(transaction=False) as driver:
            row = await driver.select_one_or_none(sql, {"app_name": app_name})
        return _decode_json_dict(row.get("state")) if row is not None else None

    async def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Upsert application-scoped ADK state asynchronously."""
        table = validate_spanner_identifier(self._app_state_table, "app_state_table")
        dialect = _config_dialect(self._config)
        sql = _upsert_app_state_sql(table, dialect)
        async with self._config.provide_session(transaction=True) as driver:
            await driver.execute(
                sql,
                {"app_name": app_name, "state": _encode_json_text(state), "update_time": datetime.now(UTC).isoformat()},
            )

    async def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Load user-scoped ADK state asynchronously via a lock-free snapshot."""
        table = validate_spanner_identifier(self._user_state_table, "user_state_table")
        sql = f"SELECT state FROM {table} WHERE app_name = @app_name AND user_id = @user_id LIMIT 1"
        async with self._config.provide_session(transaction=False) as driver:
            row = await driver.select_one_or_none(sql, {"app_name": app_name, "user_id": user_id})
        return _decode_json_dict(row.get("state")) if row is not None else None

    async def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Upsert user-scoped ADK state asynchronously."""
        table = validate_spanner_identifier(self._user_state_table, "user_state_table")
        dialect = _config_dialect(self._config)
        sql = _upsert_user_state_sql(table, dialect)
        async with self._config.provide_session(transaction=True) as driver:
            await driver.execute(
                sql,
                {
                    "app_name": app_name,
                    "user_id": user_id,
                    "state": _encode_json_text(state),
                    "update_time": datetime.now(UTC).isoformat(),
                },
            )

    async def get_metadata(self, key: str) -> str | None:
        """Retrieve scalar ADK store metadata asynchronously via a lock-free snapshot."""
        table = validate_spanner_identifier(self._metadata_table, "metadata_table")
        key_col = _metadata_key_col(_config_dialect(self._config))
        sql = f"SELECT value FROM {table} WHERE {key_col} = @key LIMIT 1"
        async with self._config.provide_session(transaction=False) as driver:
            row = await driver.select_one_or_none(sql, {"key": key})
        return str(row["value"]) if row is not None and row.get("value") is not None else None

    async def set_metadata(self, key: str, value: str) -> None:
        """Persist scalar ADK store metadata asynchronously."""
        table = validate_spanner_identifier(self._metadata_table, "metadata_table")
        dialect = _config_dialect(self._config)
        sql = _set_metadata_sql(table, dialect)
        async with self._config.provide_session(transaction=True) as driver:
            await driver.execute(sql, {"key": key, "value": value, "update_time": datetime.now(UTC).isoformat()})

    async def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Delete ADK events older than ``before`` asynchronously."""
        events_table = validate_spanner_identifier(self._events_table, "events_table")
        where = "timestamp < @before" + (" AND app_name = @app_name" if app_name is not None else "")
        params: dict[str, Any] = {"before": parse_utc_timestamp(before).isoformat()}
        if app_name is not None:
            params["app_name"] = app_name
        async with self._config.provide_session(transaction=True) as driver:
            res = await driver.execute(f"DELETE FROM {events_table} WHERE {where}", params)
        return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)

    async def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete ADK sessions inactive since ``updated_before`` asynchronously."""
        sessions_table = validate_spanner_identifier(self._session_table, "session_table")
        where = "update_time < @before" + (" AND app_name = @app_name" if app_name is not None else "")
        params: dict[str, Any] = {"before": parse_utc_timestamp(updated_before).isoformat()}
        if app_name is not None:
            params["app_name"] = app_name
        async with self._config.provide_session(transaction=True) as driver:
            res = await driver.execute(f"DELETE FROM {sessions_table} WHERE {where}", params)
        return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)

    async def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete user state rows inactive since ``updated_before`` asynchronously."""
        table = validate_spanner_identifier(self._user_state_table, "user_state_table")
        where = "update_time < @before" + (" AND app_name = @app_name" if app_name is not None else "")
        params: dict[str, Any] = {"before": parse_utc_timestamp(updated_before).isoformat()}
        if app_name is not None:
            params["app_name"] = app_name
        async with self._config.provide_session(transaction=True) as driver:
            res = await driver.execute(f"DELETE FROM {table} WHERE {where}", params)
        return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)


def _memory_ddl_statements(
    config: Any,
    memory_table: str,
    owner_id_ddl: str | None,
    *,
    vector_dimensions: int,
    scann_num_leaves: int,
    use_fts: bool,
    graph_name: str | None,
) -> list[str]:
    table = validate_spanner_identifier(memory_table, "memory_table")
    dialect = _config_dialect(config)
    owner_col = f"{owner_id_ddl}, " if owner_id_ddl else ""
    dims = max(1, int(vector_dimensions))
    leaves = max(1, int(scann_num_leaves))
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
            ")"
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
        ") PRIMARY KEY (id)",
        f"CREATE VECTOR INDEX IF NOT EXISTS {base_name}_vec_idx ON {table}(embedding) "
        f"WHERE embedding IS NOT NULL "
        f"OPTIONS (distance_type = 'COSINE', tree_type = 'SCANN', num_leaves = {leaves})",
    ]
    if use_fts:
        statements.append(f"CREATE SEARCH INDEX IF NOT EXISTS {base_name}_fts_idx ON {table}(content_tokens)")
    if graph_name:
        safe_graph = validate_spanner_identifier(graph_name, "graph_name")
        statements.append(f"CREATE OR REPLACE PROPERTY GRAPH {safe_graph} NODE TABLES ({table} KEY (id) LABEL Memory)")
    return statements


def _scope_sql_and_params(app_name: str, user_id: str, scope_filter: str) -> tuple[str, dict[str, Any]]:
    params: dict[str, Any] = {"app_name": app_name, "user_id": user_id}
    if scope_filter == "user":
        return "app_name = @app_name AND user_id = @user_id AND scope = 'user'", params
    if scope_filter == "app":
        return "app_name = @app_name AND scope = 'app'", params
    return "app_name = @app_name AND (user_id = @user_id OR scope = 'app')", params


def _rank_memory_rows(
    rows: list[dict[str, Any]],
    *,
    query: str,
    embedding: Sequence[float] | None,
    top_k: int,
) -> list[Any]:
    scored: list[tuple[float, dict[str, Any]]] = []
    for row in rows:
        dist = row.get("distance")
        if embedding is not None:
            row_emb = _decode_embedding(row.get("embedding"))
            if row_emb is not None:
                score = _cosine_similarity(embedding, row_emb)
            elif dist is not None:
                score = 1.0 / (1.0 + float(dist))
            else:
                score = 0.0
        else:
            score = _keyword_score(query, str(row.get("content_text") or ""))
        scored.append((score, row))
    scored.sort(key=lambda item: item[0], reverse=True)
    return [cast("Any", _row_to_stored_memory(item[1])) for item in scored[:top_k]]


class _SpannerADKMemoryMixin:
    """Shared ScaNN vector, full-text ``TOKENIZE_FULLTEXT``, hybrid RRF, and Spanner Graph helpers."""

    def _resolved_graph_name(self) -> str:
        return f"{validate_spanner_identifier(cast(Any, self)._memory_table, 'memory_table').replace('.', '_')}_graph"

    def build_vector_index_sql(self, index_name: str | None = None) -> str:
        """Return a GoogleSQL ``CREATE VECTOR INDEX`` DDL using Spanner ``SCANN``."""
        table = validate_spanner_identifier(cast(Any, self)._memory_table, "memory_table")
        resolved = validate_spanner_identifier(index_name or f"{table.replace('.', '_')}_vec_idx", "vector_index")
        leaves = max(1, int(getattr(self, "_scann_num_leaves", 100)))
        return (
            f"CREATE VECTOR INDEX IF NOT EXISTS {resolved} ON {table}(embedding) "
            "WHERE embedding IS NOT NULL "
            f"OPTIONS (distance_type = 'COSINE', tree_type = 'SCANN', num_leaves = {leaves})"
        )

    def build_search_index_sql(self, index_name: str | None = None) -> str:
        """Return a GoogleSQL ``CREATE SEARCH INDEX`` DDL over ``content_tokens``."""
        table = validate_spanner_identifier(cast(Any, self)._memory_table, "memory_table")
        resolved = validate_spanner_identifier(index_name or f"{table.replace('.', '_')}_fts_idx", "search_index")
        return f"CREATE SEARCH INDEX IF NOT EXISTS {resolved} ON {table}(content_tokens)"

    def build_property_graph_sql(self, graph_name: str | None = None) -> str:
        """Return a Spanner Graph DDL associating memory nodes via ``PROPERTY GRAPH``."""
        table = validate_spanner_identifier(cast(Any, self)._memory_table, "memory_table")
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
        """Build a GoogleSQL ``APPROX_COSINE_DISTANCE`` ScaNN search query."""
        table = validate_spanner_identifier(cast(Any, self)._memory_table, "memory_table")
        where, params = _scope_sql_and_params(app_name, user_id, scope_filter)
        params["top_k"] = int(top_k)
        sql = (
            "SELECT id, session_id, app_name, user_id, scope, event_id, author, timestamp, "
            "content_json, content_text, metadata_json, inserted_at, embedding, "
            "APPROX_COSINE_DISTANCE(embedding, @query_embedding, options => JSON '{\"num_leaves_to_search\": 10}') "
            "AS distance "
            f"FROM {table} WHERE {where} AND embedding IS NOT NULL "
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
        """Build a GoogleSQL ``SEARCH(content_tokens, @query)`` query."""
        table = validate_spanner_identifier(cast(Any, self)._memory_table, "memory_table")
        where, params = _scope_sql_and_params(app_name, user_id, scope_filter)
        params["query"] = query
        params["top_k"] = int(top_k)
        sql = (
            "SELECT id, session_id, app_name, user_id, scope, event_id, author, timestamp, "
            "content_json, content_text, metadata_json, inserted_at, embedding "
            f"FROM {table} WHERE {where} AND SEARCH(content_tokens, @query) "
            "ORDER BY timestamp DESC LIMIT @top_k"
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

    def _candidate_select_sql(
        self,
        *,
        app_name: str,
        user_id: str,
        scope_filter: str = "all",
    ) -> tuple[str, dict[str, Any]]:
        table = validate_spanner_identifier(cast(Any, self)._memory_table, "memory_table")
        where, params = _scope_sql_and_params(app_name, user_id, scope_filter)
        sql = (
            "SELECT id, session_id, app_name, user_id, scope, event_id, author, timestamp, "
            "content_json, content_text, metadata_json, inserted_at, embedding "
            f"FROM {table} WHERE {where} ORDER BY timestamp DESC"
        )
        return sql, params


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


class SpannerSyncADKMemoryStore(_SpannerADKMemoryMixin, _BaseSyncADKMemoryStore[Any]):
    """Cloud Spanner synchronous ADK memory store with ScaNN, FTS, and Property Graph support."""

    __slots__ = ()

    adapter_name = "vertebra_spanner"

    def _memory_table_ddl(self) -> list[str]:
        return _memory_ddl_statements(
            self._config,
            self._memory_table,
            self._owner_id_column_ddl,
            vector_dimensions=int(getattr(self, "_vector_dimensions", 768)),
            scann_num_leaves=int(getattr(self, "_scann_num_leaves", 100)),
            use_fts=bool(getattr(self, "_use_fts", False)),
            graph_name=self._resolved_graph_name(),
        )

    def _drop_memory_table_sql(self) -> list[str]:
        return [f"DROP TABLE IF EXISTS {validate_spanner_identifier(self._memory_table, 'memory_table')}"]

    def create_tables(self) -> None:
        """Create the Spanner memory table, ScaNN vector index, search index, and property graph."""
        if not self._enabled:
            return
        with self._config.provide_session(transaction=True) as driver:
            for stmt in self._memory_table_ddl():
                driver.execute(stmt)

    def insert_memory_entries(self, entries: list[Any], owner_id: object | None = None) -> int:
        """Insert memory records inside a ``ReadWrite`` transaction with deduplication on ``id``."""
        if not self._enabled or not entries:
            return 0
        table = validate_spanner_identifier(self._memory_table, "memory_table")
        dialect = _config_dialect(self._config)
        owner_col = (
            validate_spanner_identifier(self._owner_id_column_name, "owner_id_column")
            if self._owner_id_column_name
            else None
        )
        sql = _memory_insert_sql(table, dialect, owner_col)
        inserted = 0
        with self._config.provide_session(transaction=True) as driver:
            for entry in entries:
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
                    "content_json": _encode_json_text(entry.get("content_json", {})),
                    "content_text": entry.get("content_text", ""),
                    "metadata_json": (
                        _encode_json_text(entry["metadata_json"]) if entry.get("metadata_json") is not None else None
                    ),
                    "inserted_at": parse_utc_timestamp(entry["inserted_at"]).isoformat(),
                    "embedding": list(emb_raw) if emb_raw is not None else None,
                }
                if owner_col:
                    params["owner_id"] = owner_id
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
    ) -> list[Any]:
        """Search ADK memories via ScaNN vector search, FTS, and reciprocal rank fusion."""
        if not self._enabled:
            return []
        top_k = limit if limit is not None else self._max_results
        with self._config.provide_session(transaction=False) as driver:
            if embedding is not None:
                vec_sql, vec_params = self.build_vector_search_sql(
                    app_name=app_name, user_id=user_id, scope_filter=scope_filter, top_k=top_k
                )
                vec_params["query_embedding"] = [float(x) for x in embedding]
                try:
                    rows = [dict(row) for row in driver.select(vec_sql, vec_params)]
                    return _rank_memory_rows(rows, query=query, embedding=embedding, top_k=top_k)
                except Exception:
                    pass
            if self._use_fts and query.strip():
                fts_sql, fts_params = self.build_fts_search_sql(
                    app_name=app_name, user_id=user_id, query=query, scope_filter=scope_filter, top_k=top_k
                )
                try:
                    rows = [dict(row) for row in driver.select(fts_sql, fts_params)]
                    return _rank_memory_rows(rows, query=query, embedding=None, top_k=top_k)
                except Exception:
                    pass
            cand_sql, cand_params = self._candidate_select_sql(
                app_name=app_name, user_id=user_id, scope_filter=scope_filter
            )
            rows = [dict(row) for row in driver.select(cand_sql, cand_params)]
        return _rank_memory_rows(rows, query=query, embedding=embedding, top_k=top_k)

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
        """Combine vector and full-text candidate rankings via reciprocal rank fusion."""
        cand_sql, cand_params = self._candidate_select_sql(
            app_name=app_name, user_id=user_id, scope_filter=scope_filter
        )
        with self._config.provide_session(transaction=False) as driver:
            rows = [dict(row) for row in driver.select(cand_sql, cand_params)]
        if not rows:
            return []
        vec_order: dict[str, int] = {}
        if embedding is not None:
            ranked_vec = sorted(
                rows,
                key=lambda r: _cosine_similarity(embedding, _decode_embedding(r.get("embedding")) or ()),
                reverse=True,
            )
            vec_order = {str(r["id"]): idx for idx, r in enumerate(ranked_vec, start=1)}
        fts_order: dict[str, int] = {}
        if query.strip():
            ranked_fts = sorted(
                rows,
                key=lambda r: _keyword_score(query, str(r.get("content_text") or "")),
                reverse=True,
            )
            fts_order = {str(r["id"]): idx for idx, r in enumerate(ranked_fts, start=1)}
        fused: list[tuple[float, dict[str, Any]]] = []
        for row in rows:
            rid = str(row["id"])
            score = 0.0
            if rid in vec_order:
                score += 1.0 / (rrf_k + vec_order[rid])
            if rid in fts_order:
                score += 1.0 / (rrf_k + fts_order[rid])
            fused.append((score, row))
        fused.sort(key=lambda item: item[0], reverse=True)
        return [_row_to_stored_memory(item[1]) for item in fused[:top_k]]

    def delete_entries_by_session(self, session_id: str) -> int:
        """Delete memory entries associated with ``session_id``."""
        table = validate_spanner_identifier(self._memory_table, "memory_table")
        with self._config.provide_session(transaction=True) as driver:
            res = driver.execute(f"DELETE FROM {table} WHERE session_id = @session_id", {"session_id": session_id})
        return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)

    def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
        """Delete memory entries inserted more than ``days`` ago."""
        table = validate_spanner_identifier(self._memory_table, "memory_table")
        cutoff = (datetime.now(UTC) - timedelta(days=days)).isoformat()
        clauses = ["inserted_at < @cutoff"]
        params: dict[str, Any] = {"cutoff": cutoff}
        if app_name is not None:
            clauses.append("app_name = @app_name")
            params["app_name"] = app_name
        if scope is not None:
            clauses.append("scope = @scope")
            params["scope"] = scope
        with self._config.provide_session(transaction=True) as driver:
            res = driver.execute(f"DELETE FROM {table} WHERE {' AND '.join(clauses)}", params)
        return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)


class SpannerAsyncADKMemoryStore(_SpannerADKMemoryMixin, _BaseAsyncADKMemoryStore[Any]):
    """Cloud Spanner asynchronous ADK memory store with ScaNN, FTS, and Property Graph support."""

    __slots__ = ()

    adapter_name = "vertebra_spanner"

    async def _memory_table_ddl(self) -> list[str]:
        return _memory_ddl_statements(
            self._config,
            self._memory_table,
            self._owner_id_column_ddl,
            vector_dimensions=int(getattr(self, "_vector_dimensions", 768)),
            scann_num_leaves=int(getattr(self, "_scann_num_leaves", 100)),
            use_fts=bool(getattr(self, "_use_fts", False)),
            graph_name=self._resolved_graph_name(),
        )

    def _drop_memory_table_sql(self) -> list[str]:
        return [f"DROP TABLE IF EXISTS {validate_spanner_identifier(self._memory_table, 'memory_table')}"]

    async def create_tables(self) -> None:
        """Create the Spanner memory table, ScaNN vector index, search index, and property graph."""
        if not self._enabled:
            return
        ddls = await self._memory_table_ddl()
        async with self._config.provide_session(transaction=True) as driver:
            for stmt in ddls:
                await driver.execute(stmt)

    async def insert_memory_entries(self, entries: list[Any], owner_id: object | None = None) -> int:
        """Insert memory records asynchronously inside a ``ReadWrite`` transaction."""
        if not self._enabled or not entries:
            return 0
        table = validate_spanner_identifier(self._memory_table, "memory_table")
        dialect = _config_dialect(self._config)
        owner_col = (
            validate_spanner_identifier(self._owner_id_column_name, "owner_id_column")
            if self._owner_id_column_name
            else None
        )
        sql = _memory_insert_sql(table, dialect, owner_col)
        inserted = 0
        async with self._config.provide_session(transaction=True) as driver:
            for entry in entries:
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
                    "content_json": _encode_json_text(entry.get("content_json", {})),
                    "content_text": entry.get("content_text", ""),
                    "metadata_json": (
                        _encode_json_text(entry["metadata_json"]) if entry.get("metadata_json") is not None else None
                    ),
                    "inserted_at": parse_utc_timestamp(entry["inserted_at"]).isoformat(),
                    "embedding": list(emb_raw) if emb_raw is not None else None,
                }
                if owner_col:
                    params["owner_id"] = owner_id
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
    ) -> list[Any]:
        """Search ADK memories asynchronously via ScaNN vector search, FTS, and reciprocal rank fusion."""
        if not self._enabled:
            return []
        top_k = limit if limit is not None else self._max_results
        async with self._config.provide_session(transaction=False) as driver:
            if embedding is not None:
                vec_sql, vec_params = self.build_vector_search_sql(
                    app_name=app_name, user_id=user_id, scope_filter=scope_filter, top_k=top_k
                )
                vec_params["query_embedding"] = [float(x) for x in embedding]
                try:
                    raw_rows = await driver.select(vec_sql, vec_params)
                    rows = [dict(row) for row in raw_rows]
                    return _rank_memory_rows(rows, query=query, embedding=embedding, top_k=top_k)
                except Exception:
                    pass
            if self._use_fts and query.strip():
                fts_sql, fts_params = self.build_fts_search_sql(
                    app_name=app_name, user_id=user_id, query=query, scope_filter=scope_filter, top_k=top_k
                )
                try:
                    raw_rows = await driver.select(fts_sql, fts_params)
                    rows = [dict(row) for row in raw_rows]
                    return _rank_memory_rows(rows, query=query, embedding=None, top_k=top_k)
                except Exception:
                    pass
            cand_sql, cand_params = self._candidate_select_sql(
                app_name=app_name, user_id=user_id, scope_filter=scope_filter
            )
            raw_rows = await driver.select(cand_sql, cand_params)
            rows = [dict(row) for row in raw_rows]
        return _rank_memory_rows(rows, query=query, embedding=embedding, top_k=top_k)

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
        """Combine vector and full-text candidate rankings asynchronously via reciprocal rank fusion."""
        cand_sql, cand_params = self._candidate_select_sql(
            app_name=app_name, user_id=user_id, scope_filter=scope_filter
        )
        async with self._config.provide_session(transaction=False) as driver:
            raw_rows = await driver.select(cand_sql, cand_params)
            rows = [dict(row) for row in raw_rows]
        if not rows:
            return []
        vec_order: dict[str, int] = {}
        if embedding is not None:
            ranked_vec = sorted(
                rows,
                key=lambda r: _cosine_similarity(embedding, _decode_embedding(r.get("embedding")) or ()),
                reverse=True,
            )
            vec_order = {str(r["id"]): idx for idx, r in enumerate(ranked_vec, start=1)}
        fts_order: dict[str, int] = {}
        if query.strip():
            ranked_fts = sorted(
                rows,
                key=lambda r: _keyword_score(query, str(r.get("content_text") or "")),
                reverse=True,
            )
            fts_order = {str(r["id"]): idx for idx, r in enumerate(ranked_fts, start=1)}
        fused: list[tuple[float, dict[str, Any]]] = []
        for row in rows:
            rid = str(row["id"])
            score = 0.0
            if rid in vec_order:
                score += 1.0 / (rrf_k + vec_order[rid])
            if rid in fts_order:
                score += 1.0 / (rrf_k + fts_order[rid])
            fused.append((score, row))
        fused.sort(key=lambda item: item[0], reverse=True)
        return [_row_to_stored_memory(item[1]) for item in fused[:top_k]]

    async def delete_entries_by_session(self, session_id: str) -> int:
        """Delete memory entries associated with ``session_id`` asynchronously."""
        table = validate_spanner_identifier(self._memory_table, "memory_table")
        async with self._config.provide_session(transaction=True) as driver:
            res = await driver.execute(
                f"DELETE FROM {table} WHERE session_id = @session_id", {"session_id": session_id}
            )
        return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)

    async def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
        """Delete aged memory entries asynchronously."""
        table = validate_spanner_identifier(self._memory_table, "memory_table")
        cutoff = (datetime.now(UTC) - timedelta(days=days)).isoformat()
        clauses = ["inserted_at < @cutoff"]
        params: dict[str, Any] = {"cutoff": cutoff}
        if app_name is not None:
            clauses.append("app_name = @app_name")
            params["app_name"] = app_name
        if scope is not None:
            clauses.append("scope = @scope")
            params["scope"] = scope
        async with self._config.provide_session(transaction=True) as driver:
            res = await driver.execute(f"DELETE FROM {table} WHERE {' AND '.join(clauses)}", params)
        return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)


def _artifact_ddl(config: Any, artifact_table: str) -> str:
    table = validate_spanner_identifier(artifact_table, "artifact_table")
    dialect = _config_dialect(config)
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
            ")"
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
        ") PRIMARY KEY (app_name, user_id, filename, version)"
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
    """Shared ``ArtifactReference`` and ``ObjectStoreBackend`` wiring without BigQuery coupling."""

    def _init_storage(
        self,
        config: Any,
        storage_backend: ObjectStoreBackend | str | None = None,
    ) -> ObjectStoreBackend | None:
        candidate = storage_backend or getattr(config, "storage_backend", None)
        if candidate is None:
            ext = getattr(config, "extension_config", None)
            if isinstance(ext, Mapping):
                adk = ext.get("adk")
                if isinstance(adk, Mapping) and isinstance(adk.get("artifact_storage_uri"), str):
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


class SpannerSyncADKArtifactStore(_SpannerADKArtifactMixin, _BaseSyncADKArtifactStore[Any]):
    """Cloud Spanner synchronous ADK artifact metadata store returning ``ArtifactReference``."""

    __slots__ = ("_storage_backend",)

    adapter_name = "vertebra_spanner"

    def __init__(
        self,
        config: Any,
        *,
        storage_backend: ObjectStoreBackend | str | None = None,
    ) -> None:
        super().__init__(config)
        self._storage_backend = self._init_storage(config, storage_backend)

    @property
    def storage_backend(self) -> ObjectStoreBackend | None:
        """Return the configured ``ObjectStoreBackend``, if any."""
        return self._storage_backend

    def create_table(self) -> None:
        """Create the Spanner artifact metadata table inside a writable session."""
        ddl = _artifact_ddl(self._config, self._artifact_table)
        with self._config.provide_session(transaction=True) as driver:
            driver.execute(ddl)

    def insert_artifact(self, record: Any) -> None:
        """Persist an artifact metadata row inside a ``ReadWrite`` transaction."""
        table = validate_spanner_identifier(self._artifact_table, "artifact_table")
        dialect = _config_dialect(self._config)
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
                _encode_json_text(record["custom_metadata"]) if record.get("custom_metadata") is not None else None
            ),
            "created_at": parse_utc_timestamp(record.get("created_at") or datetime.now(UTC)).isoformat(),
        }
        with self._config.provide_session(transaction=True) as driver:
            driver.execute(sql, params)

    def get_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> Any:
        """Fetch artifact version metadata via a lock-free snapshot."""
        table = validate_spanner_identifier(self._artifact_table, "artifact_table")
        where, params = _artifact_scope_where(app_name, user_id, filename, session_id)
        if version is not None:
            where += " AND version = @version"
            params["version"] = int(version)
        sql = (
            "SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
            f"custom_metadata, created_at FROM {table} WHERE {where} ORDER BY version DESC LIMIT 1"
        )
        with self._config.provide_session(transaction=False) as driver:
            row = driver.select_one_or_none(sql, params)
        return cast("Any", _row_to_stored_artifact(dict(row))) if row is not None else None

    def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """List distinct artifact filenames via a lock-free snapshot."""
        table = validate_spanner_identifier(self._artifact_table, "artifact_table")
        params: dict[str, Any] = {"app_name": app_name, "user_id": user_id}
        if session_id is None:
            where = "app_name = @app_name AND user_id = @user_id AND session_id IS NULL"
        else:
            where = "app_name = @app_name AND user_id = @user_id AND (session_id = @session_id OR session_id IS NULL)"
            params["session_id"] = session_id
        sql = f"SELECT DISTINCT filename FROM {table} WHERE {where} ORDER BY filename ASC"
        with self._config.provide_session(transaction=False) as driver:
            rows = driver.select(sql, params)
        return [str(row["filename"]) for row in rows if row.get("filename") is not None]

    def list_artifact_versions(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[Any]:
        """List all version metadata rows via a lock-free snapshot."""
        table = validate_spanner_identifier(self._artifact_table, "artifact_table")
        where, params = _artifact_scope_where(app_name, user_id, filename, session_id)
        sql = (
            "SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
            f"custom_metadata, created_at FROM {table} WHERE {where} ORDER BY version ASC"
        )
        with self._config.provide_session(transaction=False) as driver:
            rows = driver.select(sql, params)
        return [cast("Any", _row_to_stored_artifact(dict(row))) for row in rows]

    def delete_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[Any]:
        """Delete all metadata rows for an artifact and return the removed records."""
        existing = self.list_artifact_versions(app_name, user_id, filename, session_id)
        table = validate_spanner_identifier(self._artifact_table, "artifact_table")
        where, params = _artifact_scope_where(app_name, user_id, filename, session_id)
        with self._config.provide_session(transaction=True) as driver:
            driver.execute(f"DELETE FROM {table} WHERE {where}", params)
        return existing

    def get_next_version(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> int:
        """Compute the next zero-based integer artifact version."""
        table = validate_spanner_identifier(self._artifact_table, "artifact_table")
        where, params = _artifact_scope_where(app_name, user_id, filename, session_id)
        sql = f"SELECT COALESCE(MAX(version), -1) AS max_ver FROM {table} WHERE {where}"
        with self._config.provide_session(transaction=False) as driver:
            row = driver.select_one_or_none(sql, params)
        max_ver = int(row.get("max_ver", -1)) if row is not None and row.get("max_ver") is not None else -1
        return max_ver + 1

    def delete_artifacts_older_than(self, before: datetime, app_name: str | None = None) -> list[Any]:
        """Delete artifact metadata rows created strictly before ``before``."""
        table = validate_spanner_identifier(self._artifact_table, "artifact_table")
        cutoff = parse_utc_timestamp(before).isoformat()
        where = "created_at < @before" + (" AND app_name = @app_name" if app_name is not None else "")
        params: dict[str, Any] = {"before": cutoff}
        if app_name is not None:
            params["app_name"] = app_name
        select_sql = (
            "SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
            f"custom_metadata, created_at FROM {table} WHERE {where} ORDER BY created_at ASC, version ASC"
        )
        with self._config.provide_session(transaction=True) as driver:
            rows = [dict(row) for row in driver.select(select_sql, params)]
            driver.execute(f"DELETE FROM {table} WHERE {where}", params)
        return [cast("Any", _row_to_stored_artifact(row)) for row in rows]

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
        record: StoredArtifact = {
            "app_name": app_name,
            "user_id": user_id,
            "session_id": session_id,
            "filename": filename,
            "version": version,
            "mime_type": mime_type,
            "canonical_uri": canonical_uri,
            "custom_metadata": custom_metadata,
            "created_at": now,
        }
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


class SpannerAsyncADKArtifactStore(_SpannerADKArtifactMixin, _BaseAsyncADKArtifactStore[Any]):
    """Cloud Spanner asynchronous ADK artifact metadata store returning ``ArtifactReference``."""

    __slots__ = ("_storage_backend",)

    adapter_name = "vertebra_spanner"

    def __init__(
        self,
        config: Any,
        *,
        storage_backend: ObjectStoreBackend | str | None = None,
    ) -> None:
        super().__init__(config)
        self._storage_backend = self._init_storage(config, storage_backend)

    @property
    def storage_backend(self) -> ObjectStoreBackend | None:
        """Return the configured ``ObjectStoreBackend``, if any."""
        return self._storage_backend

    async def create_table(self) -> None:
        """Create the Spanner artifact metadata table asynchronously."""
        ddl = _artifact_ddl(self._config, self._artifact_table)
        async with self._config.provide_session(transaction=True) as driver:
            await driver.execute(ddl)

    async def insert_artifact(self, record: Any) -> None:
        """Persist an artifact metadata row asynchronously."""
        table = validate_spanner_identifier(self._artifact_table, "artifact_table")
        dialect = _config_dialect(self._config)
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
                _encode_json_text(record["custom_metadata"]) if record.get("custom_metadata") is not None else None
            ),
            "created_at": parse_utc_timestamp(record.get("created_at") or datetime.now(UTC)).isoformat(),
        }
        async with self._config.provide_session(transaction=True) as driver:
            await driver.execute(sql, params)

    async def get_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> Any:
        """Fetch artifact version metadata asynchronously via a lock-free snapshot."""
        table = validate_spanner_identifier(self._artifact_table, "artifact_table")
        where, params = _artifact_scope_where(app_name, user_id, filename, session_id)
        if version is not None:
            where += " AND version = @version"
            params["version"] = int(version)
        sql = (
            "SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
            f"custom_metadata, created_at FROM {table} WHERE {where} ORDER BY version DESC LIMIT 1"
        )
        async with self._config.provide_session(transaction=False) as driver:
            row = await driver.select_one_or_none(sql, params)
        return cast("Any", _row_to_stored_artifact(dict(row))) if row is not None else None

    async def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """List distinct artifact filenames asynchronously via a lock-free snapshot."""
        table = validate_spanner_identifier(self._artifact_table, "artifact_table")
        params: dict[str, Any] = {"app_name": app_name, "user_id": user_id}
        if session_id is None:
            where = "app_name = @app_name AND user_id = @user_id AND session_id IS NULL"
        else:
            where = "app_name = @app_name AND user_id = @user_id AND (session_id = @session_id OR session_id IS NULL)"
            params["session_id"] = session_id
        sql = f"SELECT DISTINCT filename FROM {table} WHERE {where} ORDER BY filename ASC"
        async with self._config.provide_session(transaction=False) as driver:
            rows = await driver.select(sql, params)
        return [str(row["filename"]) for row in rows if row.get("filename") is not None]

    async def list_artifact_versions(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[Any]:
        """List all version metadata rows asynchronously via a lock-free snapshot."""
        table = validate_spanner_identifier(self._artifact_table, "artifact_table")
        where, params = _artifact_scope_where(app_name, user_id, filename, session_id)
        sql = (
            "SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
            f"custom_metadata, created_at FROM {table} WHERE {where} ORDER BY version ASC"
        )
        async with self._config.provide_session(transaction=False) as driver:
            rows = await driver.select(sql, params)
        return [cast("Any", _row_to_stored_artifact(dict(row))) for row in rows]

    async def delete_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[Any]:
        """Delete all metadata rows for an artifact asynchronously and return them."""
        existing = await self.list_artifact_versions(app_name, user_id, filename, session_id)
        table = validate_spanner_identifier(self._artifact_table, "artifact_table")
        where, params = _artifact_scope_where(app_name, user_id, filename, session_id)
        async with self._config.provide_session(transaction=True) as driver:
            await driver.execute(f"DELETE FROM {table} WHERE {where}", params)
        return existing

    async def get_next_version(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> int:
        """Compute the next zero-based integer artifact version asynchronously."""
        table = validate_spanner_identifier(self._artifact_table, "artifact_table")
        where, params = _artifact_scope_where(app_name, user_id, filename, session_id)
        sql = f"SELECT COALESCE(MAX(version), -1) AS max_ver FROM {table} WHERE {where}"
        async with self._config.provide_session(transaction=False) as driver:
            row = await driver.select_one_or_none(sql, params)
        max_ver = int(row.get("max_ver", -1)) if row is not None and row.get("max_ver") is not None else -1
        return max_ver + 1

    async def delete_artifacts_older_than(self, before: datetime, app_name: str | None = None) -> list[Any]:
        """Delete aged artifact metadata rows asynchronously."""
        table = validate_spanner_identifier(self._artifact_table, "artifact_table")
        cutoff = parse_utc_timestamp(before).isoformat()
        where = "created_at < @before" + (" AND app_name = @app_name" if app_name is not None else "")
        params: dict[str, Any] = {"before": cutoff}
        if app_name is not None:
            params["app_name"] = app_name
        select_sql = (
            "SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
            f"custom_metadata, created_at FROM {table} WHERE {where} ORDER BY created_at ASC, version ASC"
        )
        async with self._config.provide_session(transaction=True) as driver:
            raw_rows = await driver.select(select_sql, params)
            rows = [dict(row) for row in raw_rows]
            await driver.execute(f"DELETE FROM {table} WHERE {where}", params)
        return [cast("Any", _row_to_stored_artifact(row)) for row in rows]

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
        record: StoredArtifact = {
            "app_name": app_name,
            "user_id": user_id,
            "session_id": session_id,
            "filename": filename,
            "version": version,
            "mime_type": mime_type,
            "canonical_uri": canonical_uri,
            "custom_metadata": custom_metadata,
            "created_at": now,
        }
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
