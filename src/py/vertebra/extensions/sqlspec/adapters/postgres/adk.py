"""PostgreSQL ADK session, vector/FTS memory, and artifact metadata stores."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Final, Literal, NotRequired, cast

from sqlspec.config import ADKConfig
from sqlspec.extensions.adk import BaseAsyncADKStore, BaseSyncADKStore, StoredEvent, StoredSession
from sqlspec.extensions.adk.store import normalize_session_list_options

from vertebra.extensions.sqlspec.adapters.postgres.core import (
    POSTGRES_JSON_FALLBACK,
    is_pg_table_missing,
    pg_ensure_utc,
    pg_index_name,
    pg_json_dumps,
    pg_json_loads,
    pg_row_value,
    pg_table_ref,
    pg_vector_literal,
    postgres_storage_params_sql,
)
from vertebra.storage import ArtifactReference
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from sqlspec.extensions.adk import SessionOrderBy, StoredMemory
    from sqlspec.extensions.adk.artifact import StoredArtifact
    from sqlspec.extensions.adk.artifact.store import BaseAsyncADKArtifactStore as _BaseAsyncADKArtifactStore
    from sqlspec.extensions.adk.artifact.store import BaseSyncADKArtifactStore as _BaseSyncADKArtifactStore
    from sqlspec.extensions.adk.converters import merge_scoped_state
    from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore as _BaseAsyncADKMemoryStore
    from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore as _BaseSyncADKMemoryStore

    from vertebra.extensions.sqlspec.adapters.postgres.config import PostgresAsyncConfig, PostgresSyncConfig
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
    "PostgresADKConfig",
    "PostgresAsyncADKArtifactStore",
    "PostgresAsyncADKMemoryStore",
    "PostgresAsyncADKStore",
    "PostgresSyncADKArtifactStore",
    "PostgresSyncADKMemoryStore",
    "PostgresSyncADKStore",
)

logger = logging.getLogger("vertebra.extensions.sqlspec.adapters.postgres.adk")

_POSTGRES_ADK_EXTENSION_OPTIONS: Final[frozenset[str]] = frozenset(
    {
        "enable_event_generated_columns",
        "enable_covering_indexes",
        "fillfactor",
        "autovacuum_vacuum_scale_factor",
        "autovacuum_analyze_scale_factor",
        "vector_index_type",
        "vector_dimensions",
        "enable_bm25",
        "scann_num_leaves",
        "scann_quantizer",
    }
)
_VALID_SCANN_QUANTIZERS: Final[frozenset[str]] = frozenset({"SQ8", "FP32", "AH"})


class PostgresADKConfig(ADKConfig, total=False):
    """PostgreSQL-specific ADK extension settings under ``extension_config['adk']``."""

    enable_event_generated_columns: NotRequired[bool]
    """Create PostgreSQL generated columns and indexes for common ADK event JSON paths."""

    enable_covering_indexes: NotRequired[bool]
    """Add PostgreSQL INCLUDE columns to ADK event replay indexes."""

    fillfactor: NotRequired[int]
    """Table fillfactor (10-100). Defaults to 80."""

    autovacuum_vacuum_scale_factor: NotRequired[float]
    """Optional event-table autovacuum vacuum scale factor."""

    autovacuum_analyze_scale_factor: NotRequired[float]
    """Optional event-table autovacuum analyze scale factor."""

    vector_index_type: NotRequired[Literal["hnsw", "ivfflat", "scann"]]
    """Vector index algorithm for memory embeddings (``'hnsw'``, ``'ivfflat'``, ``'scann'``)."""

    vector_dimensions: NotRequired[int]
    """Dimensionality of embedding vectors stored in pgvector ``VECTOR(N)`` columns."""

    enable_bm25: NotRequired[bool]
    """Enable native BM25 full-text indexing via ``pg_textsearch``."""

    scann_num_leaves: NotRequired[int]
    """Number of partition leaves (clusters) for ScaNN tree quantization."""

    scann_quantizer: NotRequired[str]
    """Quantization method for ScaNN index (``'SQ8'``, ``'FP32'``, ``'AH'``)."""


def _adk_settings(config: Any) -> dict[str, Any]:
    """Extract ``extension_config['adk']`` as a mutable dictionary."""
    ext = getattr(config, "extension_config", {})
    if not isinstance(ext, Mapping):
        return {}
    adk = ext.get("adk", {})
    return dict(adk) if isinstance(adk, Mapping) else {}


def _resolve_json_codecs(config: Any) -> tuple[Any, Any]:
    """Resolve optional JSON serializer/deserializer hooks from driver features."""
    features = getattr(config, "driver_features", {}) or {}
    return features.get("json_serializer"), features.get("json_deserializer")


def _decode_json_dict(raw: Any, *, deserializer: Any = None) -> dict[str, Any]:
    """Decode a JSONB column payload into a dictionary."""
    if raw is None:
        return {}
    decoded = pg_json_loads(raw, deserializer=deserializer)
    return dict(decoded) if isinstance(decoded, Mapping) else {}


def _sessions_ddl(session_table: str, owner_id_ddl: str | None, adk_cfg: Mapping[str, Any]) -> str:
    """Build idempotent PostgreSQL DDL for the ADK sessions table."""
    table_ref = pg_table_ref(session_table)
    owner_col = f"{owner_id_ddl}, " if owner_id_ddl else ""
    tuning = postgres_storage_params_sql(adk_cfg, default_fillfactor=80)
    idx_app_user = pg_index_name("idx", session_table, "app_user")
    idx_update = pg_index_name("idx", session_table, "update_time")
    idx_state = pg_index_name("idx", session_table, "state")
    return (
        f"CREATE TABLE IF NOT EXISTS {table_ref} ("
        "id VARCHAR(128) PRIMARY KEY, "
        "app_name VARCHAR(128) NOT NULL, "
        "user_id VARCHAR(128) NOT NULL, "
        f"{owner_col}"
        f"state {POSTGRES_JSON_FALLBACK} NOT NULL DEFAULT '{{}}'::jsonb, "
        "create_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, "
        "update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP"
        f"){tuning};\n"
        f"CREATE INDEX IF NOT EXISTS {idx_app_user} ON {table_ref} (app_name, user_id);\n"
        f"CREATE INDEX IF NOT EXISTS {idx_update} ON {table_ref} (update_time DESC);\n"
        f"CREATE INDEX IF NOT EXISTS {idx_state} ON {table_ref} USING GIN (state) WHERE state != '{{}}'::jsonb;"
    )


def _events_ddl(events_table: str, session_table: str, adk_cfg: Mapping[str, Any]) -> str:
    """Build idempotent PostgreSQL DDL for the ADK events table."""
    events_ref = pg_table_ref(events_table)
    sessions_ref = pg_table_ref(session_table)
    fk_name = pg_index_name("fk", events_table, "session")
    idx_session = pg_index_name("idx", events_table, "session")
    idx_timestamp = pg_index_name("idx", events_table, "app_timestamp")
    idx_gin = pg_index_name("idx", events_table, "event_data_gin")
    tuning = postgres_storage_params_sql(adk_cfg, default_fillfactor=80, include_autovacuum=True)
    generated_cols = ""
    if adk_cfg.get("enable_event_generated_columns", False):
        generated_cols = (
            ", author_gc VARCHAR(256) GENERATED ALWAYS AS (event_data->>'author') STORED, "
            "node_path_gc TEXT GENERATED ALWAYS AS (event_data->'node_info'->>'path') STORED"
        )
    covering = " INCLUDE (invocation_id)" if adk_cfg.get("enable_covering_indexes", False) else ""
    statements = [
        f"CREATE TABLE IF NOT EXISTS {events_ref} ("
        "id VARCHAR(128) PRIMARY KEY, "
        "app_name VARCHAR(128) NOT NULL, "
        "user_id VARCHAR(128) NOT NULL, "
        "session_id VARCHAR(128) NOT NULL, "
        "invocation_id VARCHAR(256), "
        "timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, "
        f"event_data {POSTGRES_JSON_FALLBACK} NOT NULL"
        f"{generated_cols}, "
        f"CONSTRAINT {fk_name} FOREIGN KEY (session_id) REFERENCES {sessions_ref}(id) ON DELETE CASCADE"
        f"){tuning};",
        f"CREATE INDEX IF NOT EXISTS {idx_session} ON {events_ref} (session_id, timestamp ASC){covering};",
        f"CREATE INDEX IF NOT EXISTS {idx_timestamp} ON {events_ref} (app_name, timestamp ASC);",
        f"CREATE INDEX IF NOT EXISTS {idx_gin} ON {events_ref} USING GIN (event_data);",
    ]
    if adk_cfg.get("enable_event_generated_columns", False):
        idx_author = pg_index_name("idx", events_table, "author_gc")
        idx_node_path = pg_index_name("idx", events_table, "node_path_gc")
        statements.append(
            f"CREATE INDEX IF NOT EXISTS {idx_author} ON {events_ref} (session_id, author_gc, timestamp ASC);"
        )
        statements.append(
            f"CREATE INDEX IF NOT EXISTS {idx_node_path} ON {events_ref} (session_id, node_path_gc, timestamp ASC);"
        )
    return "\n".join(statements)


def _app_states_ddl(app_state_table: str, adk_cfg: Mapping[str, Any]) -> str:
    """Build idempotent PostgreSQL DDL for the ADK app state table."""
    table_ref = pg_table_ref(app_state_table)
    tuning = postgres_storage_params_sql(adk_cfg, default_fillfactor=80)
    return (
        f"CREATE TABLE IF NOT EXISTS {table_ref} ("
        "app_name VARCHAR(128) PRIMARY KEY, "
        f"state {POSTGRES_JSON_FALLBACK} NOT NULL DEFAULT '{{}}'::jsonb, "
        "update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP"
        f"){tuning};"
    )


def _user_states_ddl(user_state_table: str, adk_cfg: Mapping[str, Any]) -> str:
    """Build idempotent PostgreSQL DDL for the ADK user state table."""
    table_ref = pg_table_ref(user_state_table)
    tuning = postgres_storage_params_sql(adk_cfg, default_fillfactor=80)
    return (
        f"CREATE TABLE IF NOT EXISTS {table_ref} ("
        "app_name VARCHAR(128) NOT NULL, "
        "user_id VARCHAR(128) NOT NULL, "
        f"state {POSTGRES_JSON_FALLBACK} NOT NULL DEFAULT '{{}}'::jsonb, "
        "update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, "
        "PRIMARY KEY (app_name, user_id)"
        f"){tuning};"
    )


def _metadata_ddl(metadata_table: str) -> str:
    """Build idempotent PostgreSQL DDL for the ADK internal metadata table."""
    table_ref = pg_table_ref(metadata_table)
    return f"CREATE TABLE IF NOT EXISTS {table_ref} (key VARCHAR(128) PRIMARY KEY, value VARCHAR(512) NOT NULL);"


def _row_to_stored_session(
    row: Any,
    *,
    deserializer: Any = None,
    app_state: dict[str, Any] | None = None,
    user_state: dict[str, Any] | None = None,
    merge_scopes: bool = False,
) -> StoredSession:
    """Decode a PostgreSQL session row mapping into a ``StoredSession`` dictionary."""
    persisted_state = _decode_json_dict(pg_row_value(row, "state", 3), deserializer=deserializer)
    resolved_state = merge_scoped_state(persisted_state, app_state, user_state) if merge_scopes else persisted_state
    return StoredSession(
        id=str(pg_row_value(row, "id", 0)),
        app_name=str(pg_row_value(row, "app_name", 1)),
        user_id=str(pg_row_value(row, "user_id", 2)),
        state=resolved_state,
        create_time=pg_ensure_utc(pg_row_value(row, "create_time", 4)),
        update_time=pg_ensure_utc(pg_row_value(row, "update_time", 5)),
    )


def _row_to_stored_event(row: Any, *, deserializer: Any = None) -> StoredEvent:
    """Decode a PostgreSQL event row mapping into a ``StoredEvent`` dictionary."""
    raw_json = pg_row_value(row, "event_data", 6)
    decoded = pg_json_loads(raw_json, deserializer=deserializer) if raw_json is not None else {}
    return StoredEvent(
        id=str(pg_row_value(row, "id", 0)),
        session_id=str(pg_row_value(row, "session_id", 1)),
        app_name=str(pg_row_value(row, "app_name", 2)),
        user_id=str(pg_row_value(row, "user_id", 3)),
        invocation_id=pg_row_value(row, "invocation_id", 4),
        timestamp=pg_ensure_utc(pg_row_value(row, "timestamp", 5)),
        event_data=decoded if isinstance(decoded, dict) else {},
    )


def _memory_index_statements(
    memory_table: str,
    *,
    use_fts: bool,
    enable_bm25: bool,
    vector_index_type: str,
    scann_num_leaves: int,
    scann_quantizer: str,
) -> list[str]:
    """Build companion index DDL statements for the PostgreSQL ADK memory table."""
    table_ref = pg_table_ref(memory_table)
    stmts = [
        f"CREATE INDEX IF NOT EXISTS {pg_index_name('idx', memory_table, 'app_scope_user_time')} "
        f"ON {table_ref} (app_name, scope, user_id, timestamp DESC);",
        f"CREATE INDEX IF NOT EXISTS {pg_index_name('idx', memory_table, 'scope')} ON {table_ref} (app_name, scope);",
        f"CREATE INDEX IF NOT EXISTS {pg_index_name('idx', memory_table, 'session')} ON {table_ref} (session_id);",
    ]
    if use_fts:
        stmts.append(
            f"CREATE INDEX IF NOT EXISTS {pg_index_name('idx', memory_table, 'fts')} "
            f"ON {table_ref} USING GIN (to_tsvector('english', content_text));"
        )
    if enable_bm25:
        stmts.append(
            f"CREATE INDEX IF NOT EXISTS {pg_index_name('idx', memory_table, 'bm25')} "
            f"ON {table_ref} USING bm25 (content_text) WITH (text_config = 'english');"
        )
    if vector_index_type == "scann":
        if not isinstance(scann_num_leaves, int) or isinstance(scann_num_leaves, bool) or scann_num_leaves <= 0:
            msg = "extension_config['adk']['scann_num_leaves'] must be a positive integer"
            raise ImproperConfigurationError(msg)
        if scann_quantizer.upper() not in _VALID_SCANN_QUANTIZERS:
            msg = f"Invalid SCaNN quantizer '{scann_quantizer}'. Must be one of: SQ8, FP32, AH"
            raise ImproperConfigurationError(msg)
        stmts.append(
            f"CREATE INDEX IF NOT EXISTS {pg_index_name('idx', memory_table, 'scann')} "
            f"ON {table_ref} USING scann (embedding) "
            f"WITH (num_leaves = {scann_num_leaves}, quantizer = '{scann_quantizer}');"
        )
    elif vector_index_type == "ivfflat":
        stmts.append(
            f"CREATE INDEX IF NOT EXISTS {pg_index_name('idx', memory_table, 'ivfflat')} "
            f"ON {table_ref} USING ivfflat (embedding vector_cosine_ops);"
        )
    elif vector_index_type == "hnsw":
        stmts.append(
            f"CREATE INDEX IF NOT EXISTS {pg_index_name('idx', memory_table, 'hnsw')} "
            f"ON {table_ref} USING hnsw (embedding vector_cosine_ops);"
        )
    else:
        msg = f"Invalid vector_index_type '{vector_index_type}'. Must be one of: hnsw, ivfflat, scann"
        raise ImproperConfigurationError(msg)
    return stmts


def _memory_table_ddl(
    memory_table: str,
    owner_id_ddl: str | None,
    *,
    vector_dimensions: int,
    vector_index_type: str,
    scann_num_leaves: int,
    scann_quantizer: str,
    enable_bm25: bool,
    use_fts: bool,
) -> str:
    """Build PostgreSQL DDL for ADK memory entries with pgvector and FTS/BM25 indexes."""
    table_ref = pg_table_ref(memory_table)
    owner_col = f"{owner_id_ddl}, " if owner_id_ddl else ""
    indexes = _memory_index_statements(
        memory_table,
        use_fts=use_fts,
        enable_bm25=enable_bm25,
        vector_index_type=vector_index_type,
        scann_num_leaves=scann_num_leaves,
        scann_quantizer=scann_quantizer,
    )
    create_stmt = (
        f"CREATE TABLE IF NOT EXISTS {table_ref} ("
        "id VARCHAR(128) PRIMARY KEY, "
        "session_id VARCHAR(128) NOT NULL, "
        "app_name VARCHAR(128) NOT NULL, "
        "user_id VARCHAR(128) NOT NULL, "
        "scope VARCHAR(16) NOT NULL DEFAULT 'user', "
        "event_id VARCHAR(128) NOT NULL, "
        "author VARCHAR(256), "
        f"{owner_col}"
        "timestamp TIMESTAMPTZ NOT NULL, "
        f"embedding VECTOR({int(vector_dimensions)}), "
        f"content_json {POSTGRES_JSON_FALLBACK} NOT NULL, "
        "content_text TEXT NOT NULL, "
        f"metadata_json {POSTGRES_JSON_FALLBACK}, "
        "inserted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, "
        "UNIQUE (app_name, user_id, event_id)"
        ");"
    )
    return "\n".join((create_stmt, *indexes))


def _build_memory_scope_where(
    app_name: str, user_id: str, scope_filter: Literal["all", "user", "app"]
) -> tuple[str, tuple[Any, ...]]:
    """Build ``$N`` PostgreSQL scope predicates matching user, app, or combined memory scopes."""
    if scope_filter == "user":
        return "app_name = $1 AND scope = 'user' AND user_id = $2", (app_name, user_id)
    if scope_filter == "app":
        return "app_name = $1 AND scope = 'app'", (app_name,)
    return "app_name = $1 AND ((scope = 'user' AND user_id = $2) OR scope = 'app')", (app_name, user_id)


def _row_to_stored_memory(row: Any, *, deserializer: Any = None) -> StoredMemory:
    """Decode a PostgreSQL memory row mapping into a ``StoredMemory`` dictionary."""
    raw_content = pg_row_value(row, "content_json", 8)
    raw_meta = pg_row_value(row, "metadata_json", 10)
    raw_embedding = pg_row_value(row, "embedding", 12)
    decoded_embedding: list[float] | None = None
    if raw_embedding is not None:
        if isinstance(raw_embedding, str):
            trimmed = raw_embedding.strip().strip("[]")
            decoded_embedding = [float(item) for item in trimmed.split(",") if item.strip()] if trimmed else []
        elif isinstance(raw_embedding, Sequence) and not isinstance(raw_embedding, (bytes, bytearray)):
            decoded_embedding = [float(item) for item in raw_embedding]
    return cast(
        "StoredMemory",
        {
            "id": str(pg_row_value(row, "id", 0)),
            "session_id": str(pg_row_value(row, "session_id", 1)),
            "app_name": str(pg_row_value(row, "app_name", 2)),
            "user_id": str(pg_row_value(row, "user_id", 3)),
            "scope": str(pg_row_value(row, "scope", 4)),
            "event_id": str(pg_row_value(row, "event_id", 5)),
            "author": pg_row_value(row, "author", 6),
            "timestamp": pg_ensure_utc(pg_row_value(row, "timestamp", 7)),
            "embedding": decoded_embedding,
            "content_json": pg_json_loads(raw_content, deserializer=deserializer) or {},
            "content_text": str(pg_row_value(row, "content_text", 9) or ""),
            "metadata_json": pg_json_loads(raw_meta, deserializer=deserializer) if raw_meta is not None else None,
            "inserted_at": pg_ensure_utc(pg_row_value(row, "inserted_at", 11)),
        },
    )


def _artifact_table_ddl(artifact_table: str) -> str:
    """Build PostgreSQL DDL for ADK artifacts and object storage references."""
    table_ref = pg_table_ref(artifact_table)
    return (
        f"CREATE TABLE IF NOT EXISTS {table_ref} ("
        "app_name VARCHAR(128) NOT NULL, "
        "user_id VARCHAR(128) NOT NULL, "
        "session_id VARCHAR(128) NOT NULL DEFAULT '', "
        "filename VARCHAR(512) NOT NULL, "
        "version INTEGER NOT NULL, "
        "mime_type VARCHAR(255), "
        "canonical_uri TEXT NOT NULL, "
        f"custom_metadata {POSTGRES_JSON_FALLBACK}, "
        "size BIGINT, "
        "updated TIMESTAMPTZ, "
        "signed_url TEXT, "
        "created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, "
        "PRIMARY KEY (app_name, user_id, session_id, filename, version)"
        ");"
    )


def _row_to_stored_artifact(row: Any, *, deserializer: Any = None) -> StoredArtifact:
    """Decode a PostgreSQL artifact row into a ``StoredArtifact`` dictionary."""
    session_raw = pg_row_value(row, "session_id", 2)
    mime_raw = pg_row_value(row, "mime_type", 5)
    meta_raw = pg_row_value(row, "custom_metadata", 7)
    return cast(
        "StoredArtifact",
        {
            "app_name": str(pg_row_value(row, "app_name", 0)),
            "user_id": str(pg_row_value(row, "user_id", 1)),
            "session_id": str(session_raw) if session_raw else None,
            "filename": str(pg_row_value(row, "filename", 3)),
            "version": int(pg_row_value(row, "version", 4)),
            "mime_type": str(mime_raw) if mime_raw is not None else None,
            "canonical_uri": str(pg_row_value(row, "canonical_uri", 6)),
            "custom_metadata": pg_json_loads(meta_raw, deserializer=deserializer) if meta_raw is not None else None,
            "created_at": pg_ensure_utc(pg_row_value(row, "created_at", 8)),
        },
    )


def _row_to_artifact_reference(row: Any) -> ArtifactReference:
    """Decode an artifact row into a ``vertebra.storage.ArtifactReference``."""
    mime_raw = pg_row_value(row, "mime_type", 5)
    size_raw = pg_row_value(row, "size", 8)
    updated_raw = pg_row_value(row, "updated", 9)
    signed_raw = pg_row_value(row, "signed_url", 10)
    return ArtifactReference(
        uri=str(pg_row_value(row, "canonical_uri", 6)),
        version=int(pg_row_value(row, "version", 4)),
        content_type=str(mime_raw) if mime_raw is not None else None,
        size=int(size_raw) if size_raw is not None else None,
        updated=pg_ensure_utc(updated_raw) if updated_raw is not None else None,
        signed_url=str(signed_raw) if signed_raw is not None else None,
    )


class _PostgresADKSessionSQLMixin:
    """Shared PostgreSQL DDL and parameterized SQL builders for sync/async ADK session stores."""

    __slots__ = ()

    if TYPE_CHECKING:
        _app_state_table: str
        _config: Any
        _events_table: str
        _json_deserializer: Any
        _json_serializer: Any
        _metadata_table: str
        _owner_id_column_ddl: str | None
        _owner_id_column_name: str | None
        _session_table: str
        _user_state_table: str

    def _build_sessions_ddl(self) -> str:
        return _sessions_ddl(self._session_table, self._owner_id_column_ddl, _adk_settings(self._config))

    def _build_events_ddl(self) -> str:
        return _events_ddl(self._events_table, self._session_table, _adk_settings(self._config))

    def _build_app_states_ddl(self) -> str:
        return _app_states_ddl(self._app_state_table, _adk_settings(self._config))

    def _build_user_states_ddl(self) -> str:
        return _user_states_ddl(self._user_state_table, _adk_settings(self._config))

    def _build_metadata_ddl(self) -> str:
        return _metadata_ddl(self._metadata_table)

    def _drop_app_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {pg_table_ref(self._app_state_table)};"

    def _drop_user_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {pg_table_ref(self._user_state_table)};"

    def _drop_metadata_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {pg_table_ref(self._metadata_table)};"

    def _drop_tables_sql(self) -> list[str]:
        return [
            self._drop_metadata_table_sql(),
            self._drop_user_states_table_sql(),
            self._drop_app_states_table_sql(),
            f"DROP TABLE IF EXISTS {pg_table_ref(self._events_table)};",
            f"DROP TABLE IF EXISTS {pg_table_ref(self._session_table)};",
        ]

    def _create_session_sql_and_params(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
        state: Mapping[str, Any],
        owner_id: Any | None,
    ) -> tuple[str, tuple[Any, ...]]:
        table_ref = pg_table_ref(self._session_table)
        state_json = pg_json_dumps(dict(state), serializer=self._json_serializer)
        if self._owner_id_column_name:
            owner_col = pg_table_ref(self._owner_id_column_name, "owner column")
            sql = (
                f"INSERT INTO {table_ref} (id, app_name, user_id, {owner_col}, state, create_time, update_time) "
                "VALUES ($1, $2, $3, $4, $5::jsonb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) "
                "RETURNING id, app_name, user_id, state, create_time, update_time"
            )
            return sql, (session_id, app_name, user_id, owner_id, state_json)
        sql = (
            f"INSERT INTO {table_ref} (id, app_name, user_id, state, create_time, update_time) "
            "VALUES ($1, $2, $3, $4::jsonb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) "
            "RETURNING id, app_name, user_id, state, create_time, update_time"
        )
        return sql, (session_id, app_name, user_id, state_json)

    def _get_session_sql(self, *, touch: bool) -> str:
        table_ref = pg_table_ref(self._session_table)
        if touch:
            return (
                f"UPDATE {table_ref} SET update_time = CURRENT_TIMESTAMP "
                "WHERE app_name = $1 AND user_id = $2 AND id = $3 "
                "RETURNING id, app_name, user_id, state, create_time, update_time"
            )
        return (
            f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table_ref} "
            "WHERE app_name = $1 AND user_id = $2 AND id = $3"
        )

    def _update_session_state_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        state: Mapping[str, Any],
    ) -> tuple[str, tuple[Any, ...]]:
        table_ref = pg_table_ref(self._session_table)
        state_json = pg_json_dumps(dict(state), serializer=self._json_serializer)
        sql = (
            f"UPDATE {table_ref} SET state = $1::jsonb, update_time = CURRENT_TIMESTAMP "
            "WHERE app_name = $2 AND user_id = $3 AND id = $4"
        )
        return sql, (state_json, app_name, user_id, session_id)

    def _list_sessions_sql_and_params(
        self,
        app_name: str,
        user_id: str | None,
        *,
        order_by: SessionOrderBy,
        descending: bool,
        limit: int | None,
        offset: int | None,
    ) -> tuple[str, list[Any]] | None:
        col, direction, norm_limit, norm_offset = normalize_session_list_options(order_by, descending, limit, offset)
        if norm_limit == 0:
            return None
        table_ref = pg_table_ref(self._session_table)
        params: list[Any] = [app_name]
        where_sql = "app_name = $1"
        if user_id is not None:
            params.append(user_id)
            where_sql = f"{where_sql} AND user_id = ${len(params)}"
        sql = (
            f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table_ref} "
            f"WHERE {where_sql} ORDER BY {col} {direction}, id {direction}"
        )
        if norm_limit is not None:
            params.append(norm_limit)
            sql += f" LIMIT ${len(params)}"
        if norm_offset > 0:
            params.append(norm_offset)
            sql += f" OFFSET ${len(params)}"
        return sql, params

    def _delete_session_sql(self) -> str:
        table_ref = pg_table_ref(self._session_table)
        return f"DELETE FROM {table_ref} WHERE app_name = $1 AND user_id = $2 AND id = $3"

    def _append_event_sql_and_params(self, event_record: StoredEvent) -> tuple[str, tuple[Any, ...]]:
        events_ref = pg_table_ref(self._events_table)
        sessions_ref = pg_table_ref(self._session_table)
        event_json = pg_json_dumps(event_record["event_data"], serializer=self._json_serializer)
        sql = (
            f"INSERT INTO {events_ref} (id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
            "SELECT $1, $2::varchar(128), $3::varchar(128), $4::varchar(128), $5, $6, $7::jsonb "
            f"WHERE EXISTS (SELECT 1 FROM {sessions_ref} WHERE id = $4 AND app_name = $2 AND user_id = $3)"
        )
        params = (
            event_record["id"],
            event_record["app_name"],
            event_record["user_id"],
            event_record["session_id"],
            event_record.get("invocation_id"),
            pg_ensure_utc(event_record["timestamp"]),
            event_json,
        )
        return sql, params

    def _insert_event_sql_and_params(self, event_record: StoredEvent) -> tuple[str, tuple[Any, ...]]:
        events_ref = pg_table_ref(self._events_table)
        event_json = pg_json_dumps(event_record["event_data"], serializer=self._json_serializer)
        sql = (
            f"INSERT INTO {events_ref} (id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)"
        )
        params = (
            event_record["id"],
            event_record["app_name"],
            event_record["user_id"],
            event_record["session_id"],
            event_record.get("invocation_id"),
            pg_ensure_utc(event_record["timestamp"]),
            event_json,
        )
        return sql, params

    def _append_event_update_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        state: Mapping[str, Any],
    ) -> tuple[str, tuple[Any, ...]]:
        table_ref = pg_table_ref(self._session_table)
        state_json = pg_json_dumps(dict(state), serializer=self._json_serializer)
        sql = (
            f"UPDATE {table_ref} SET state = $1::jsonb, update_time = CURRENT_TIMESTAMP "
            "WHERE app_name = $2 AND user_id = $3 AND id = $4 "
            "RETURNING id, app_name, user_id, state, create_time, update_time"
        )
        return sql, (state_json, app_name, user_id, session_id)

    def _get_events_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None,
        limit: int | None,
    ) -> tuple[str, list[Any]]:
        events_ref = pg_table_ref(self._events_table)
        sessions_ref = pg_table_ref(self._session_table)
        where_clauses = ["s.app_name = $1", "s.user_id = $2", "s.id = $3"]
        params: list[Any] = [app_name, user_id, session_id]
        if after_timestamp is not None:
            params.append(pg_ensure_utc(after_timestamp))
            where_clauses.append(f"e.timestamp > ${len(params)}")
        if limit is not None:
            params.append(limit)
            order_clause = f"ORDER BY e.timestamp DESC, e.id DESC LIMIT ${len(params)}"
        else:
            order_clause = "ORDER BY e.timestamp ASC, e.id ASC"
        sql = (
            "SELECT e.id, e.session_id, s.app_name, s.user_id, e.invocation_id, e.timestamp, e.event_data "
            f"FROM {events_ref} e "
            f"JOIN {sessions_ref} s ON e.session_id = s.id AND e.app_name = s.app_name AND e.user_id = s.user_id "
            f"WHERE {' AND '.join(where_clauses)} {order_clause}"
        )
        return sql, params

    def _delete_before_sql_and_params(
        self,
        table_name: str,
        time_column: str,
        before: datetime,
        app_name: str | None,
    ) -> tuple[str, tuple[Any, ...]]:
        table_ref = pg_table_ref(table_name)
        cutoff = pg_ensure_utc(before)
        if app_name is not None:
            return f"DELETE FROM {table_ref} WHERE {time_column} < $1 AND app_name = $2", (cutoff, app_name)
        return f"DELETE FROM {table_ref} WHERE {time_column} < $1", (cutoff,)

    def _select_state_sql(self, table_name: str, key_columns: tuple[str, ...]) -> str:
        table_ref = pg_table_ref(table_name)
        where_sql = " AND ".join(f"{col} = ${idx}" for idx, col in enumerate(key_columns, start=1))
        return f"SELECT state FROM {table_ref} WHERE {where_sql}"

    def _delta_state_sqls(self, table_name: str, key_columns: tuple[str, ...]) -> tuple[str, str, str]:
        table_ref = pg_table_ref(table_name)
        cols = ", ".join((*key_columns, "state", "update_time"))
        insert_placeholders = ", ".join(f"${idx}" for idx in range(1, len(key_columns) + 1))
        state_idx = len(key_columns) + 1
        conflict_cols = ", ".join(key_columns)
        ensure_sql = (
            f"INSERT INTO {table_ref} ({cols}) VALUES ({insert_placeholders}, ${state_idx}::jsonb, CURRENT_TIMESTAMP) "
            f"ON CONFLICT ({conflict_cols}) DO NOTHING"
        )
        select_where = " AND ".join(f"{col} = ${idx}" for idx, col in enumerate(key_columns, start=1))
        lock_sql = f"SELECT state FROM {table_ref} WHERE {select_where} FOR UPDATE"
        update_where = " AND ".join(f"{col} = ${idx}" for idx, col in enumerate(key_columns, start=2))
        update_sql = f"UPDATE {table_ref} SET state = $1::jsonb, update_time = CURRENT_TIMESTAMP WHERE {update_where}"
        return ensure_sql, lock_sql, update_sql

    def _upsert_app_state_sql_and_params(self, app_name: str, state: Mapping[str, Any]) -> tuple[str, tuple[Any, ...]]:
        table_ref = pg_table_ref(self._app_state_table)
        state_json = pg_json_dumps(dict(state), serializer=self._json_serializer)
        sql = (
            f"INSERT INTO {table_ref} (app_name, state, update_time) VALUES ($1, $2::jsonb, CURRENT_TIMESTAMP) "
            "ON CONFLICT (app_name) DO UPDATE SET state = EXCLUDED.state, update_time = CURRENT_TIMESTAMP"
        )
        return sql, (app_name, state_json)

    def _upsert_user_state_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        state: Mapping[str, Any],
    ) -> tuple[str, tuple[Any, ...]]:
        table_ref = pg_table_ref(self._user_state_table)
        state_json = pg_json_dumps(dict(state), serializer=self._json_serializer)
        sql = (
            f"INSERT INTO {table_ref} (app_name, user_id, state, update_time) "
            "VALUES ($1, $2, $3::jsonb, CURRENT_TIMESTAMP) "
            "ON CONFLICT (app_name, user_id) DO UPDATE SET state = EXCLUDED.state, update_time = CURRENT_TIMESTAMP"
        )
        return sql, (app_name, user_id, state_json)

    def _get_metadata_sql(self) -> str:
        return f"SELECT value FROM {pg_table_ref(self._metadata_table)} WHERE key = $1"

    def _set_metadata_sql(self) -> str:
        table_ref = pg_table_ref(self._metadata_table)
        return (
            f"INSERT INTO {table_ref} (key, value) VALUES ($1, $2) "
            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
        )


class PostgresSyncADKStore(_PostgresADKSessionSQLMixin, BaseSyncADKStore["PostgresSyncConfig"]):
    """Synchronous PostgreSQL ADK session and event store backed by ``PostgresSyncConfig``."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    extension_config_options = _POSTGRES_ADK_EXTENSION_OPTIONS

    def __init__(self, config: PostgresSyncConfig, *, read_config: PostgresSyncConfig | None = None) -> None:
        """Initialize the synchronous PostgreSQL ADK session store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    def _sessions_table_ddl(self) -> str:
        return self._build_sessions_ddl()

    def _events_table_ddl(self) -> str:
        return self._build_events_ddl()

    def _app_states_table_ddl(self) -> str:
        return self._build_app_states_ddl()

    def _user_states_table_ddl(self) -> str:
        return self._build_user_states_ddl()

    def _metadata_table_ddl(self) -> str:
        return self._build_metadata_ddl()

    def create_tables(self) -> None:
        """Create ADK sessions, events, app state, user state, and metadata tables idempotently."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return
        with self._config.provide_session() as driver:
            driver.execute_script(self._sessions_table_ddl())
            driver.execute_script(self._events_table_ddl())
            driver.execute_script(self._app_states_table_ddl())
            driver.execute_script(self._user_states_table_ddl())
            driver.execute_script(self._metadata_table_ddl())
            driver.commit()

    def _select_state_with_driver(
        self,
        driver: Any,
        table_name: str,
        key_columns: tuple[str, ...],
        key_values: tuple[Any, ...],
    ) -> dict[str, Any] | None:
        try:
            row = driver.select_one_or_none(self._select_state_sql(table_name, key_columns), *key_values)
        except Exception as exc:
            if is_pg_table_missing(exc):
                return None
            raise
        if row is None:
            return None
        return _decode_json_dict(pg_row_value(row, "state", 0), deserializer=self._json_deserializer)

    def _apply_state_delta_with_driver(
        self,
        driver: Any,
        table_name: str,
        key_columns: tuple[str, ...],
        key_values: tuple[Any, ...],
        delta: Mapping[str, Any],
    ) -> dict[str, Any]:
        ensure_sql, lock_sql, update_sql = self._delta_state_sqls(table_name, key_columns)
        empty_json = pg_json_dumps({}, serializer=self._json_serializer)
        driver.execute(ensure_sql, *key_values, empty_json)
        row = driver.select_one_or_none(lock_sql, *key_values)
        existing = (
            _decode_json_dict(pg_row_value(row, "state", 0), deserializer=self._json_deserializer)
            if row is not None
            else {}
        )
        merged = dict(existing)
        merged.update(delta)
        merged_json = pg_json_dumps(merged, serializer=self._json_serializer)
        driver.execute(update_sql, merged_json, *key_values)
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
        """Create a new ADK session and atomically apply optional scoped state deltas."""
        insert_sql, insert_params = self._create_session_sql_and_params(session_id, app_name, user_id, state, owner_id)
        with self._config.provide_session() as driver:
            row = driver.select_one_or_none(insert_sql, *insert_params)
            resolved_app = (
                self._apply_state_delta_with_driver(
                    driver, self._app_state_table, ("app_name",), (app_name,), app_state_delta
                )
                if app_state_delta
                else self._select_state_with_driver(driver, self._app_state_table, ("app_name",), (app_name,))
            )
            resolved_user = (
                self._apply_state_delta_with_driver(
                    driver, self._user_state_table, ("app_name", "user_id"), (app_name, user_id), user_state_delta
                )
                if user_state_delta
                else self._select_state_with_driver(
                    driver, self._user_state_table, ("app_name", "user_id"), (app_name, user_id)
                )
            )
            driver.commit()
        if row is not None:
            return _row_to_stored_session(
                row,
                deserializer=self._json_deserializer,
                app_state=resolved_app,
                user_state=resolved_user,
                merge_scopes=True,
            )
        created = self.get_session(app_name, user_id, session_id)
        if created is None:
            msg = f"Failed to reload newly created ADK session {session_id!r}"
            raise RuntimeError(msg)
        created["state"] = merge_scoped_state(created["state"], resolved_app, resolved_user)
        return created

    def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: int | timedelta | None = None
    ) -> StoredSession | None:
        """Fetch an ADK session by composite key, sliding ``update_time`` if requested."""
        should_touch = self._is_renewal_requested(renew_for)
        sql = self._get_session_sql(touch=should_touch)
        try:
            with self._provide_replica_session(primary=renew_for is not None) as driver:
                row = driver.select_one_or_none(sql, app_name, user_id, session_id)
                if should_touch:
                    driver.commit()
                if row is None:
                    return None
                return _row_to_stored_session(row, deserializer=self._json_deserializer)
        except Exception as exc:
            if is_pg_table_missing(exc):
                return None
            raise

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Update JSONB session state."""
        sql, params = self._update_session_state_sql_and_params(app_name, user_id, session_id, state)
        with self._config.provide_session() as driver:
            driver.execute(sql, *params)
            driver.commit()

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
        """List ADK sessions with validated keyset/offset sorting."""
        built = self._list_sessions_sql_and_params(
            app_name, user_id, order_by=order_by, descending=descending, limit=limit, offset=offset
        )
        if built is None:
            return []
        sql, params = built
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, *params)
            return [_row_to_stored_session(row, deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_pg_table_missing(exc):
                return []
            raise

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete an ADK session row (cascading event deletions via FK)."""
        sql = self._delete_session_sql()
        with self._config.provide_session() as driver:
            driver.execute(sql, app_name, user_id, session_id)
            driver.commit()

    def append_event(self, event_record: StoredEvent) -> None:
        """Append a single ADK event JSONB row after verifying session existence."""
        sql, params = self._append_event_sql_and_params(event_record)
        with self._config.provide_session() as driver:
            res = driver.execute(sql, *params)
            driver.commit()
        if int(getattr(res, "rows_affected", 0) or 0) == 0:
            msg = (
                f"Session {event_record['session_id']!r} not found "
                f"for app={event_record['app_name']!r}, user={event_record['user_id']!r}"
            )
            raise ValueError(msg)

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
        """Atomically append an event, update session state, and apply scoped deltas."""
        if (event_record["app_name"], event_record["user_id"], event_record["session_id"]) != (
            app_name,
            user_id,
            session_id,
        ):
            msg = "event_record scope does not match target session arguments."
            raise ValueError(msg)
        update_sql, update_params = self._append_event_update_sql_and_params(app_name, user_id, session_id, state)
        insert_sql, insert_params = self._insert_event_sql_and_params(event_record)
        with self._config.provide_session() as driver:
            row = driver.select_one_or_none(update_sql, *update_params)
            if row is None:
                msg = f"Session {session_id!r} not found during append_event_and_update_state."
                raise ValueError(msg)
            driver.execute(insert_sql, *insert_params)
            merged_app = (
                self._apply_state_delta_with_driver(
                    driver, self._app_state_table, ("app_name",), (app_name,), app_state_delta
                )
                if app_state_delta
                else None
            )
            merged_user = (
                self._apply_state_delta_with_driver(
                    driver, self._user_state_table, ("app_name", "user_id"), (app_name, user_id), user_state_delta
                )
                if user_state_delta
                else None
            )
            driver.commit()
        return _row_to_stored_session(
            row,
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
        """Retrieve ADK events for a session ordered chronologically."""
        if limit == 0:
            return []
        sql, params = self._get_events_sql_and_params(app_name, user_id, session_id, after_timestamp, limit)
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, *params)
            if limit is not None:
                rows = list(reversed(rows))
            return [_row_to_stored_event(row, deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_pg_table_missing(exc):
                return []
            raise

    def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Delete events older than ``before``, optionally restricted to one app."""
        sql, params = self._delete_before_sql_and_params(self._events_table, "timestamp", before, app_name)
        try:
            with self._config.provide_session() as driver:
                res = driver.execute(sql, *params)
                driver.commit()
                return int(getattr(res, "rows_affected", 0) or 0)
        except Exception as exc:
            if is_pg_table_missing(exc):
                return 0
            raise

    def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete user state rows whose ``update_time`` is older than ``updated_before``."""
        sql, params = self._delete_before_sql_and_params(
            self._user_state_table, "update_time", updated_before, app_name
        )
        try:
            with self._config.provide_session() as driver:
                res = driver.execute(sql, *params)
                driver.commit()
                return int(getattr(res, "rows_affected", 0) or 0)
        except Exception as exc:
            if is_pg_table_missing(exc):
                return 0
            raise

    def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete sessions whose ``update_time`` is older than ``updated_before``."""
        sql, params = self._delete_before_sql_and_params(self._session_table, "update_time", updated_before, app_name)
        try:
            with self._config.provide_session() as driver:
                res = driver.execute(sql, *params)
                driver.commit()
                return int(getattr(res, "rows_affected", 0) or 0)
        except Exception as exc:
            if is_pg_table_missing(exc):
                return 0
            raise

    def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Fetch app-scoped JSONB state for ``app_name``."""
        with self._config.provide_session() as driver:
            return self._select_state_with_driver(driver, self._app_state_table, ("app_name",), (app_name,))

    def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Fetch user-scoped JSONB state for ``(app_name, user_id)``."""
        with self._config.provide_session() as driver:
            return self._select_state_with_driver(
                driver, self._user_state_table, ("app_name", "user_id"), (app_name, user_id)
            )

    def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Upsert app-scoped ADK state via PostgreSQL ``ON CONFLICT``."""
        sql, params = self._upsert_app_state_sql_and_params(app_name, state)
        with self._config.provide_session() as driver:
            driver.execute(sql, *params)
            driver.commit()

    def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Upsert user-scoped ADK state via PostgreSQL ``ON CONFLICT``."""
        sql, params = self._upsert_user_state_sql_and_params(app_name, user_id, state)
        with self._config.provide_session() as driver:
            driver.execute(sql, *params)
            driver.commit()

    def get_metadata(self, key: str) -> str | None:
        """Return a metadata value from ``adk_internal_metadata`` or ``None`` if absent."""
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(self._get_metadata_sql(), key)
            return str(pg_row_value(row, "value", 0)) if row is not None else None
        except Exception as exc:
            if is_pg_table_missing(exc):
                return None
            raise

    def set_metadata(self, key: str, value: str) -> None:
        """Upsert a metadata key-value entry via PostgreSQL ``ON CONFLICT``."""
        with self._config.provide_session() as driver:
            driver.execute(self._set_metadata_sql(), key, value)
            driver.commit()


class PostgresAsyncADKStore(_PostgresADKSessionSQLMixin, BaseAsyncADKStore["PostgresAsyncConfig"]):
    """Asynchronous PostgreSQL ADK session and event store backed by ``PostgresAsyncConfig``."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    extension_config_options = _POSTGRES_ADK_EXTENSION_OPTIONS

    def __init__(self, config: PostgresAsyncConfig, *, read_config: PostgresAsyncConfig | None = None) -> None:
        """Initialize the asynchronous PostgreSQL ADK session store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    async def _sessions_table_ddl(self) -> str:
        return self._build_sessions_ddl()

    async def _events_table_ddl(self) -> str:
        return self._build_events_ddl()

    async def _app_states_table_ddl(self) -> str:
        return self._build_app_states_ddl()

    async def _user_states_table_ddl(self) -> str:
        return self._build_user_states_ddl()

    async def _metadata_table_ddl(self) -> str:
        return self._build_metadata_ddl()

    async def create_tables(self) -> None:
        """Create ADK session, event, state, and metadata tables asynchronously."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._sessions_table_ddl())
            await driver.execute_script(await self._events_table_ddl())
            await driver.execute_script(await self._app_states_table_ddl())
            await driver.execute_script(await self._user_states_table_ddl())
            await driver.execute_script(await self._metadata_table_ddl())
            await driver.commit()

    async def _select_state_with_driver(
        self,
        driver: Any,
        table_name: str,
        key_columns: tuple[str, ...],
        key_values: tuple[Any, ...],
    ) -> dict[str, Any] | None:
        try:
            row = await driver.select_one_or_none(self._select_state_sql(table_name, key_columns), *key_values)
        except Exception as exc:
            if is_pg_table_missing(exc):
                return None
            raise
        if row is None:
            return None
        return _decode_json_dict(pg_row_value(row, "state", 0), deserializer=self._json_deserializer)

    async def _apply_state_delta_with_driver(
        self,
        driver: Any,
        table_name: str,
        key_columns: tuple[str, ...],
        key_values: tuple[Any, ...],
        delta: Mapping[str, Any],
    ) -> dict[str, Any]:
        ensure_sql, lock_sql, update_sql = self._delta_state_sqls(table_name, key_columns)
        empty_json = pg_json_dumps({}, serializer=self._json_serializer)
        await driver.execute(ensure_sql, *key_values, empty_json)
        row = await driver.select_one_or_none(lock_sql, *key_values)
        existing = (
            _decode_json_dict(pg_row_value(row, "state", 0), deserializer=self._json_deserializer)
            if row is not None
            else {}
        )
        merged = dict(existing)
        merged.update(delta)
        merged_json = pg_json_dumps(merged, serializer=self._json_serializer)
        await driver.execute(update_sql, merged_json, *key_values)
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
        """Asynchronously create a new ADK session and atomically apply scoped state deltas."""
        insert_sql, insert_params = self._create_session_sql_and_params(session_id, app_name, user_id, state, owner_id)
        async with self._config.provide_session() as driver:
            row = await driver.select_one_or_none(insert_sql, *insert_params)
            resolved_app = (
                await self._apply_state_delta_with_driver(
                    driver, self._app_state_table, ("app_name",), (app_name,), app_state_delta
                )
                if app_state_delta
                else await self._select_state_with_driver(driver, self._app_state_table, ("app_name",), (app_name,))
            )
            resolved_user = (
                await self._apply_state_delta_with_driver(
                    driver, self._user_state_table, ("app_name", "user_id"), (app_name, user_id), user_state_delta
                )
                if user_state_delta
                else await self._select_state_with_driver(
                    driver, self._user_state_table, ("app_name", "user_id"), (app_name, user_id)
                )
            )
            await driver.commit()
        if row is not None:
            return _row_to_stored_session(
                row,
                deserializer=self._json_deserializer,
                app_state=resolved_app,
                user_state=resolved_user,
                merge_scopes=True,
            )
        created = await self.get_session(app_name, user_id, session_id)
        if created is None:
            msg = f"Failed to reload newly created ADK session {session_id!r}"
            raise RuntimeError(msg)
        created["state"] = merge_scoped_state(created["state"], resolved_app, resolved_user)
        return created

    async def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: int | timedelta | None = None
    ) -> StoredSession | None:
        """Fetch an ADK session asynchronously, sliding ``update_time`` when requested."""
        should_touch = self._is_renewal_requested(renew_for)
        sql = self._get_session_sql(touch=should_touch)
        try:
            async with self._provide_replica_session(primary=renew_for is not None) as driver:
                row = await driver.select_one_or_none(sql, app_name, user_id, session_id)
                if should_touch:
                    await driver.commit()
                if row is None:
                    return None
                return _row_to_stored_session(row, deserializer=self._json_deserializer)
        except Exception as exc:
            if is_pg_table_missing(exc):
                return None
            raise

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Asynchronously update JSONB session state."""
        sql, params = self._update_session_state_sql_and_params(app_name, user_id, session_id, state)
        async with self._config.provide_session() as driver:
            await driver.execute(sql, *params)
            await driver.commit()

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
        built = self._list_sessions_sql_and_params(
            app_name, user_id, order_by=order_by, descending=descending, limit=limit, offset=offset
        )
        if built is None:
            return []
        sql, params = built
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, *params)
            return [_row_to_stored_session(row, deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_pg_table_missing(exc):
                return []
            raise

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Asynchronously delete an ADK session row."""
        sql = self._delete_session_sql()
        async with self._config.provide_session() as driver:
            await driver.execute(sql, app_name, user_id, session_id)
            await driver.commit()

    async def append_event(self, event_record: StoredEvent) -> None:
        """Asynchronously append an ADK event row after verifying session existence."""
        sql, params = self._append_event_sql_and_params(event_record)
        async with self._config.provide_session() as driver:
            res = await driver.execute(sql, *params)
            await driver.commit()
        if int(getattr(res, "rows_affected", 0) or 0) == 0:
            msg = (
                f"Session {event_record['session_id']!r} not found "
                f"for app={event_record['app_name']!r}, user={event_record['user_id']!r}"
            )
            raise ValueError(msg)

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
        """Atomically append an event, update session state, and apply scoped deltas asynchronously."""
        if (event_record["app_name"], event_record["user_id"], event_record["session_id"]) != (
            app_name,
            user_id,
            session_id,
        ):
            msg = "event_record scope does not match target session arguments."
            raise ValueError(msg)
        update_sql, update_params = self._append_event_update_sql_and_params(app_name, user_id, session_id, state)
        insert_sql, insert_params = self._insert_event_sql_and_params(event_record)
        async with self._config.provide_session() as driver:
            row = await driver.select_one_or_none(update_sql, *update_params)
            if row is None:
                msg = f"Session {session_id!r} not found during append_event_and_update_state."
                raise ValueError(msg)
            await driver.execute(insert_sql, *insert_params)
            merged_app = (
                await self._apply_state_delta_with_driver(
                    driver, self._app_state_table, ("app_name",), (app_name,), app_state_delta
                )
                if app_state_delta
                else None
            )
            merged_user = (
                await self._apply_state_delta_with_driver(
                    driver, self._user_state_table, ("app_name", "user_id"), (app_name, user_id), user_state_delta
                )
                if user_state_delta
                else None
            )
            await driver.commit()
        return _row_to_stored_session(
            row,
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
        """Asynchronously retrieve ADK events for a session."""
        if limit == 0:
            return []
        sql, params = self._get_events_sql_and_params(app_name, user_id, session_id, after_timestamp, limit)
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, *params)
            if limit is not None:
                rows = list(reversed(rows))
            return [_row_to_stored_event(row, deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_pg_table_missing(exc):
                return []
            raise

    async def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Asynchronously delete events older than ``before``."""
        sql, params = self._delete_before_sql_and_params(self._events_table, "timestamp", before, app_name)
        try:
            async with self._config.provide_session() as driver:
                res = await driver.execute(sql, *params)
                await driver.commit()
                return int(getattr(res, "rows_affected", 0) or 0)
        except Exception as exc:
            if is_pg_table_missing(exc):
                return 0
            raise

    async def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Asynchronously delete idle user states older than ``updated_before``."""
        sql, params = self._delete_before_sql_and_params(
            self._user_state_table, "update_time", updated_before, app_name
        )
        try:
            async with self._config.provide_session() as driver:
                res = await driver.execute(sql, *params)
                await driver.commit()
                return int(getattr(res, "rows_affected", 0) or 0)
        except Exception as exc:
            if is_pg_table_missing(exc):
                return 0
            raise

    async def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Asynchronously delete idle sessions older than ``updated_before``."""
        sql, params = self._delete_before_sql_and_params(self._session_table, "update_time", updated_before, app_name)
        try:
            async with self._config.provide_session() as driver:
                res = await driver.execute(sql, *params)
                await driver.commit()
                return int(getattr(res, "rows_affected", 0) or 0)
        except Exception as exc:
            if is_pg_table_missing(exc):
                return 0
            raise

    async def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Asynchronously fetch app-scoped state."""
        async with self._config.provide_session() as driver:
            return await self._select_state_with_driver(driver, self._app_state_table, ("app_name",), (app_name,))

    async def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Asynchronously fetch user-scoped state."""
        async with self._config.provide_session() as driver:
            return await self._select_state_with_driver(
                driver, self._user_state_table, ("app_name", "user_id"), (app_name, user_id)
            )

    async def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Asynchronously upsert app-scoped ADK state."""
        sql, params = self._upsert_app_state_sql_and_params(app_name, state)
        async with self._config.provide_session() as driver:
            await driver.execute(sql, *params)
            await driver.commit()

    async def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Asynchronously upsert user-scoped ADK state."""
        sql, params = self._upsert_user_state_sql_and_params(app_name, user_id, state)
        async with self._config.provide_session() as driver:
            await driver.execute(sql, *params)
            await driver.commit()

    async def get_metadata(self, key: str) -> str | None:
        """Asynchronously fetch a metadata entry."""
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(self._get_metadata_sql(), key)
            return str(pg_row_value(row, "value", 0)) if row is not None else None
        except Exception as exc:
            if is_pg_table_missing(exc):
                return None
            raise

    async def set_metadata(self, key: str, value: str) -> None:
        """Asynchronously upsert a metadata entry."""
        async with self._config.provide_session() as driver:
            await driver.execute(self._set_metadata_sql(), key, value)
            await driver.commit()


class _PostgresADKMemorySQLMixin:
    """Shared PostgreSQL DDL and query builders for sync/async ADK memory stores."""

    __slots__ = ()

    if TYPE_CHECKING:
        _enable_bm25: bool
        _enabled: bool
        _json_deserializer: Any
        _json_serializer: Any
        _max_results: int
        _memory_table: str
        _owner_id_column_ddl: str | None
        _owner_id_column_name: str | None
        _scann_num_leaves: int
        _scann_quantizer: str
        _use_fts: bool
        _vector_dimensions: int
        _vector_index_type: str

    def _build_memory_table_ddl(self) -> str:
        return _memory_table_ddl(
            self._memory_table,
            self._owner_id_column_ddl,
            vector_dimensions=self._vector_dimensions,
            vector_index_type=self._vector_index_type,
            scann_num_leaves=self._scann_num_leaves,
            scann_quantizer=self._scann_quantizer,
            enable_bm25=self._enable_bm25,
            use_fts=self._use_fts,
        )

    def _drop_memory_table_sql(self) -> list[str]:
        return [f"DROP TABLE IF EXISTS {pg_table_ref(self._memory_table)};"]

    def _build_insert_sql_and_params(self, entry: StoredMemory, owner_id: object | None) -> tuple[str, tuple[Any, ...]]:
        table_ref = pg_table_ref(self._memory_table)
        emb = entry.get("embedding")
        emb_bind: Any = pg_vector_literal(emb) if emb is not None else None
        content_json = pg_json_dumps(entry["content_json"], serializer=self._json_serializer)
        metadata_val = entry.get("metadata_json")
        metadata_json = (
            pg_json_dumps(metadata_val, serializer=self._json_serializer) if metadata_val is not None else None
        )
        if self._owner_id_column_name:
            owner_col = pg_table_ref(self._owner_id_column_name, "owner column")
            sql = (
                f"INSERT INTO {table_ref} (id, session_id, app_name, user_id, scope, event_id, author, "
                f"{owner_col}, timestamp, embedding, content_json, content_text, metadata_json, inserted_at) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::vector, $11::jsonb, $12, $13::jsonb, $14) "
                "ON CONFLICT (app_name, user_id, event_id) DO NOTHING"
            )
            params: tuple[Any, ...] = (
                entry["id"],
                entry["session_id"],
                entry["app_name"],
                entry["user_id"],
                entry.get("scope", "user"),
                entry["event_id"],
                entry.get("author"),
                owner_id,
                pg_ensure_utc(entry["timestamp"]),
                emb_bind,
                content_json,
                entry["content_text"],
                metadata_json,
                pg_ensure_utc(entry["inserted_at"]),
            )
            return sql, params
        sql = (
            f"INSERT INTO {table_ref} (id, session_id, app_name, user_id, scope, event_id, author, "
            "timestamp, embedding, content_json, content_text, metadata_json, inserted_at) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::vector, $10::jsonb, $11, $12::jsonb, $13) "
            "ON CONFLICT (app_name, user_id, event_id) DO NOTHING"
        )
        params = (
            entry["id"],
            entry["session_id"],
            entry["app_name"],
            entry["user_id"],
            entry.get("scope", "user"),
            entry["event_id"],
            entry.get("author"),
            pg_ensure_utc(entry["timestamp"]),
            emb_bind,
            content_json,
            entry["content_text"],
            metadata_json,
            pg_ensure_utc(entry["inserted_at"]),
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
    ) -> tuple[str, tuple[Any, ...]] | None:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        has_query = bool(query and query.strip())
        if not has_query and embedding is None:
            return None
        effective_limit = limit if limit is not None else self._max_results
        if effective_limit <= 0:
            return None
        table_ref = pg_table_ref(self._memory_table)
        where_scope, scope_params = _build_memory_scope_where(app_name, user_id, scope_filter)
        scope_count = len(scope_params)
        columns = (
            "id, session_id, app_name, user_id, scope, event_id, author, "
            "timestamp, content_json, content_text, metadata_json, inserted_at, embedding"
        )
        if embedding is not None and self._enable_bm25 and has_query:
            p_vec = f"${scope_count + 1}"
            p_txt = f"${scope_count + 2}"
            p_cand = f"${scope_count + 3}"
            p_lim = f"${scope_count + 4}"
            candidate_limit = max(effective_limit * 2, 50)
            m_cols = ", ".join(f"m.{col.strip()}" for col in columns.split(","))
            sql = (
                f"WITH vector_matches AS ("
                f"SELECT id, RANK() OVER (ORDER BY embedding <=> {p_vec}::vector) AS rank_vec "
                f"FROM {table_ref} WHERE {where_scope} AND embedding IS NOT NULL LIMIT {p_cand}"
                "), text_matches AS ("
                f"SELECT id, RANK() OVER (ORDER BY content_text <@> {p_txt}) AS rank_txt "
                f"FROM {table_ref} WHERE {where_scope} LIMIT {p_cand}"
                ") "
                f"SELECT {m_cols} FROM {table_ref} m "
                "LEFT JOIN vector_matches v ON m.id = v.id "
                "LEFT JOIN text_matches t ON m.id = t.id "
                "WHERE v.id IS NOT NULL OR t.id IS NOT NULL "
                "ORDER BY (COALESCE(1.0 / (60 + v.rank_vec), 0.0) + COALESCE(1.0 / (60 + t.rank_txt), 0.0)) DESC, "
                f"m.timestamp DESC LIMIT {p_lim}"
            )
            return sql, (*scope_params, pg_vector_literal(embedding), query, candidate_limit, effective_limit)
        if embedding is not None:
            p_vec = f"${scope_count + 1}"
            p_lim = f"${scope_count + 2}"
            sql = (
                f"SELECT {columns} FROM {table_ref} "
                f"WHERE {where_scope} AND embedding IS NOT NULL "
                f"ORDER BY embedding <=> {p_vec}::vector ASC, timestamp DESC LIMIT {p_lim}"
            )
            return sql, (*scope_params, pg_vector_literal(embedding), effective_limit)
        if self._use_fts:
            p_q = f"${scope_count + 1}"
            p_lim = f"${scope_count + 2}"
            sql = (
                f"SELECT {columns} FROM {table_ref} WHERE {where_scope} "
                f"AND to_tsvector('english', content_text) @@ plainto_tsquery('english', {p_q}) "
                f"ORDER BY ts_rank(to_tsvector('english', content_text), plainto_tsquery('english', {p_q})) DESC, "
                f"timestamp DESC LIMIT {p_lim}"
            )
            return sql, (*scope_params, query, effective_limit)
        p_q = f"${scope_count + 1}"
        p_lim = f"${scope_count + 2}"
        sql = (
            f"SELECT {columns} FROM {table_ref} WHERE {where_scope} AND content_text ILIKE {p_q} "
            f"ORDER BY timestamp DESC LIMIT {p_lim}"
        )
        return sql, (*scope_params, f"%{query}%", effective_limit)

    def _delete_by_session_sql_and_params(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
    ) -> tuple[str, tuple[str, str, str]]:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        table_ref = pg_table_ref(self._memory_table)
        sql = f"DELETE FROM {table_ref} WHERE session_id = $1 AND app_name = $2 AND user_id = $3"
        return sql, (session_id, app_name, user_id)

    def _delete_older_than_sql_and_params(
        self,
        days: int,
        app_name: str | None,
        scope: str | None,
    ) -> tuple[str, list[Any]]:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        table_ref = pg_table_ref(self._memory_table)
        cutoff = datetime.now(UTC) - timedelta(days=days)
        params: list[Any] = [cutoff]
        clauses = ["inserted_at < $1"]
        if app_name is not None:
            params.append(app_name)
            clauses.append(f"app_name = ${len(params)}")
        if scope in {"user", "app"}:
            params.append(scope)
            clauses.append(f"scope = ${len(params)}")
        sql = f"DELETE FROM {table_ref} WHERE {' AND '.join(clauses)}"
        return sql, params


class PostgresSyncADKMemoryStore(_PostgresADKMemorySQLMixin, _BaseSyncADKMemoryStore["PostgresSyncConfig"]):
    """Synchronous PostgreSQL ADK memory store supporting pgvector, BM25, and ``tsvector`` FTS."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    extension_config_options = _POSTGRES_ADK_EXTENSION_OPTIONS

    def __init__(self, config: PostgresSyncConfig, *, read_config: PostgresSyncConfig | None = None) -> None:
        """Initialize synchronous PostgreSQL memory store settings."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    def _memory_table_ddl(self) -> str:
        return self._build_memory_table_ddl()

    def create_tables(self) -> None:
        """Create PostgreSQL memory table and vector/FTS indexes idempotently."""
        if not self._enabled:
            return
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return
        with self._config.provide_session() as driver:
            driver.execute_script(self._memory_table_ddl())
            driver.commit()

    def insert_memory_entries(self, entries: list[StoredMemory], owner_id: object | None = None) -> int:
        """Bulk-insert memory entries with deduplication."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0
        inserted = 0
        with self._config.provide_session() as driver:
            for entry in entries:
                sql, params = self._build_insert_sql_and_params(entry, owner_id)
                res = driver.execute(sql, *params)
                inserted += int(getattr(res, "rows_affected", 0) or 0)
            driver.commit()
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
        """Search memory entries via hybrid RRF, pgvector cosine distance, FTS, or ILIKE."""
        built = self._build_search_sql_and_params(query, app_name, user_id, limit, scope_filter, embedding)
        if built is None:
            return []
        sql, params = built
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, *params)
            return [_row_to_stored_memory(row, deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_pg_table_missing(exc):
                return []
            raise

    def delete_entries_by_session(self, session_id: str, app_name: str, user_id: str) -> int:
        """Delete all memory entries for ``session_id`` owned by ``app_name`` and ``user_id``."""
        sql, params = self._delete_by_session_sql_and_params(session_id, app_name, user_id)
        try:
            with self._config.provide_session() as driver:
                res = driver.execute(sql, *params)
                driver.commit()
                return int(getattr(res, "rows_affected", 0) or 0)
        except Exception as exc:
            if is_pg_table_missing(exc):
                return 0
            raise

    def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
        """Delete memory entries older than ``days`` days filtered optionally by app and scope."""
        sql, params = self._delete_older_than_sql_and_params(days, app_name, scope)
        try:
            with self._config.provide_session() as driver:
                res = driver.execute(sql, *params)
                driver.commit()
                return int(getattr(res, "rows_affected", 0) or 0)
        except Exception as exc:
            if is_pg_table_missing(exc):
                return 0
            raise


class PostgresAsyncADKMemoryStore(_PostgresADKMemorySQLMixin, _BaseAsyncADKMemoryStore["PostgresAsyncConfig"]):
    """Asynchronous PostgreSQL ADK memory store supporting pgvector, BM25, and ``tsvector`` FTS."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    extension_config_options = _POSTGRES_ADK_EXTENSION_OPTIONS

    def __init__(self, config: PostgresAsyncConfig, *, read_config: PostgresAsyncConfig | None = None) -> None:
        """Initialize asynchronous PostgreSQL memory store settings."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    async def _memory_table_ddl(self) -> str:
        return self._build_memory_table_ddl()

    async def create_tables(self) -> None:
        """Asynchronously create PostgreSQL memory table and indexes idempotently."""
        if not self._enabled:
            return
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._memory_table_ddl())
            await driver.commit()

    async def insert_memory_entries(self, entries: list[StoredMemory], owner_id: object | None = None) -> int:
        """Asynchronously bulk-insert memory entries with deduplication."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0
        inserted = 0
        async with self._config.provide_session() as driver:
            for entry in entries:
                sql, params = self._build_insert_sql_and_params(entry, owner_id)
                res = await driver.execute(sql, *params)
                inserted += int(getattr(res, "rows_affected", 0) or 0)
            await driver.commit()
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
        """Asynchronously search memory entries via hybrid RRF, pgvector cosine distance, FTS, or ILIKE."""
        built = self._build_search_sql_and_params(query, app_name, user_id, limit, scope_filter, embedding)
        if built is None:
            return []
        sql, params = built
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, *params)
            return [_row_to_stored_memory(row, deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_pg_table_missing(exc):
                return []
            raise

    async def delete_entries_by_session(self, session_id: str, app_name: str, user_id: str) -> int:
        """Asynchronously delete all memory entries for ``session_id`` owned by ``app_name`` and ``user_id``."""
        sql, params = self._delete_by_session_sql_and_params(session_id, app_name, user_id)
        try:
            async with self._config.provide_session() as driver:
                res = await driver.execute(sql, *params)
                await driver.commit()
                return int(getattr(res, "rows_affected", 0) or 0)
        except Exception as exc:
            if is_pg_table_missing(exc):
                return 0
            raise

    async def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
        """Asynchronously delete memory entries older than ``days`` days."""
        sql, params = self._delete_older_than_sql_and_params(days, app_name, scope)
        try:
            async with self._config.provide_session() as driver:
                res = await driver.execute(sql, *params)
                await driver.commit()
                return int(getattr(res, "rows_affected", 0) or 0)
        except Exception as exc:
            if is_pg_table_missing(exc):
                return 0
            raise


class _PostgresADKArtifactSQLMixin:
    """Shared PostgreSQL DDL and query builders for sync/async ADK artifact stores."""

    __slots__ = ()

    if TYPE_CHECKING:
        _artifact_table: str
        _json_deserializer: Any
        _json_serializer: Any

    def _build_artifact_table_ddl(self) -> str:
        return _artifact_table_ddl(self._artifact_table)

    def _insert_artifact_sql_and_params(self, record: StoredArtifact) -> tuple[str, tuple[Any, ...]]:
        table_ref = pg_table_ref(self._artifact_table)
        meta_raw = (
            pg_json_dumps(record["custom_metadata"], serializer=self._json_serializer)
            if record.get("custom_metadata") is not None
            else None
        )
        sql = (
            f"INSERT INTO {table_ref} (app_name, user_id, session_id, filename, version, mime_type, "
            "canonical_uri, custom_metadata, created_at) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9) "
            "ON CONFLICT (app_name, user_id, session_id, filename, version) DO UPDATE SET "
            "mime_type = EXCLUDED.mime_type, canonical_uri = EXCLUDED.canonical_uri, "
            "custom_metadata = EXCLUDED.custom_metadata"
        )
        params = (
            record["app_name"],
            record["user_id"],
            record.get("session_id") or "",
            record["filename"],
            record["version"],
            record.get("mime_type"),
            record["canonical_uri"],
            meta_raw,
            pg_ensure_utc(record.get("created_at")),
        )
        return sql, params

    def _insert_artifact_ref_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        reference: ArtifactReference,
        session_id: str | None,
        custom_metadata: dict[str, Any] | None,
    ) -> tuple[str, tuple[Any, ...]]:
        table_ref = pg_table_ref(self._artifact_table)
        meta_raw = pg_json_dumps(custom_metadata, serializer=self._json_serializer) if custom_metadata else None
        sql = (
            f"INSERT INTO {table_ref} (app_name, user_id, session_id, filename, version, mime_type, "
            "canonical_uri, custom_metadata, size, updated, signed_url, created_at) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11, CURRENT_TIMESTAMP) "
            "ON CONFLICT (app_name, user_id, session_id, filename, version) DO UPDATE SET "
            "mime_type = EXCLUDED.mime_type, canonical_uri = EXCLUDED.canonical_uri, "
            "custom_metadata = EXCLUDED.custom_metadata, size = EXCLUDED.size, "
            "updated = EXCLUDED.updated, signed_url = EXCLUDED.signed_url"
        )
        params = (
            app_name,
            user_id,
            session_id or "",
            filename,
            int(reference.version) if reference.version is not None else 0,
            reference.content_type,
            reference.uri,
            meta_raw,
            reference.size,
            pg_ensure_utc(reference.updated) if reference.updated is not None else None,
            reference.signed_url,
        )
        return sql, params

    def _get_artifact_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None,
        version: int | None,
        *,
        include_ref_cols: bool,
    ) -> tuple[str, tuple[Any, ...]]:
        table_ref = pg_table_ref(self._artifact_table)
        sess_val = session_id or ""
        cols = (
            "app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
            "custom_metadata, size, updated, signed_url, created_at"
            if include_ref_cols
            else "app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
            "custom_metadata, created_at"
        )
        if version is not None:
            sql = (
                f"SELECT {cols} FROM {table_ref} "
                "WHERE app_name = $1 AND user_id = $2 AND session_id = $3 AND filename = $4 AND version = $5"
            )
            return sql, (app_name, user_id, sess_val, filename, version)
        sql = (
            f"SELECT {cols} FROM {table_ref} "
            "WHERE app_name = $1 AND user_id = $2 AND session_id = $3 AND filename = $4 "
            "ORDER BY version DESC LIMIT 1"
        )
        return sql, (app_name, user_id, sess_val, filename)

    def _list_keys_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        session_id: str | None,
    ) -> tuple[str, tuple[Any, ...]]:
        table_ref = pg_table_ref(self._artifact_table)
        if session_id is not None:
            sql = (
                f"SELECT DISTINCT filename FROM {table_ref} "
                "WHERE app_name = $1 AND user_id = $2 AND session_id IN ('', $3) ORDER BY filename ASC"
            )
            return sql, (app_name, user_id, session_id)
        sql = (
            f"SELECT DISTINCT filename FROM {table_ref} "
            "WHERE app_name = $1 AND user_id = $2 AND session_id = '' ORDER BY filename ASC"
        )
        return sql, (app_name, user_id)

    def _list_versions_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None,
    ) -> tuple[str, tuple[str, str, str, str]]:
        table_ref = pg_table_ref(self._artifact_table)
        sql = (
            f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
            f"custom_metadata, created_at FROM {table_ref} "
            "WHERE app_name = $1 AND user_id = $2 AND session_id = $3 AND filename = $4 ORDER BY version ASC"
        )
        return sql, (app_name, user_id, session_id or "", filename)

    def _delete_artifact_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None,
    ) -> tuple[str, tuple[str, str, str, str]]:
        table_ref = pg_table_ref(self._artifact_table)
        sql = f"DELETE FROM {table_ref} WHERE app_name = $1 AND user_id = $2 AND session_id = $3 AND filename = $4"
        return sql, (app_name, user_id, session_id or "", filename)

    def _get_next_version_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None,
    ) -> tuple[str, tuple[str, str, str, str]]:
        table_ref = pg_table_ref(self._artifact_table)
        sql = (
            f"SELECT MAX(version) FROM {table_ref} "
            "WHERE app_name = $1 AND user_id = $2 AND session_id = $3 AND filename = $4"
        )
        return sql, (app_name, user_id, session_id or "", filename)

    def _delete_older_than_sqls_and_params(
        self,
        before: datetime,
        app_name: str | None,
    ) -> tuple[str, str, tuple[Any, ...]]:
        table_ref = pg_table_ref(self._artifact_table)
        cutoff = pg_ensure_utc(before)
        cols = "app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at"
        if app_name is not None:
            select_sql = (
                f"SELECT {cols} FROM {table_ref} WHERE created_at < $1 AND app_name = $2 ORDER BY created_at ASC"
            )
            delete_sql = f"DELETE FROM {table_ref} WHERE created_at < $1 AND app_name = $2"
            return select_sql, delete_sql, (cutoff, app_name)
        select_sql = f"SELECT {cols} FROM {table_ref} WHERE created_at < $1 ORDER BY created_at ASC"
        delete_sql = f"DELETE FROM {table_ref} WHERE created_at < $1"
        return select_sql, delete_sql, (cutoff,)


class PostgresSyncADKArtifactStore(_PostgresADKArtifactSQLMixin, _BaseSyncADKArtifactStore["PostgresSyncConfig"]):
    """Synchronous PostgreSQL ADK artifact metadata store returning ``ArtifactReference``."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    def __init__(self, config: PostgresSyncConfig, *, read_config: PostgresSyncConfig | None = None) -> None:
        """Initialize the synchronous PostgreSQL ADK artifact store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    def create_table(self) -> None:
        """Create the ADK artifact metadata table in PostgreSQL."""
        with self._config.provide_session() as driver:
            driver.execute_script(self._build_artifact_table_ddl())
            driver.commit()

    def insert_artifact(self, record: StoredArtifact) -> None:
        """Upsert an ADK artifact metadata entry using ``$1..$N`` binds."""
        sql, params = self._insert_artifact_sql_and_params(record)
        with self._config.provide_session() as driver:
            driver.execute(sql, *params)
            driver.commit()

    def insert_artifact_reference(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        reference: ArtifactReference,
        *,
        session_id: str | None = None,
        custom_metadata: dict[str, Any] | None = None,
    ) -> None:
        """Persist a ``vertebra.storage.ArtifactReference`` record into the artifact metadata table."""
        sql, params = self._insert_artifact_ref_sql_and_params(
            app_name, user_id, filename, reference, session_id, custom_metadata
        )
        with self._config.provide_session() as driver:
            driver.execute(sql, *params)
            driver.commit()

    def get_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> StoredArtifact | None:
        """Fetch an ADK artifact metadata dictionary by key and optional version."""
        sql, params = self._get_artifact_sql_and_params(
            app_name, user_id, filename, session_id, version, include_ref_cols=False
        )
        try:
            with self._provide_replica_session(primary=False) as driver:
                row = driver.select_one_or_none(sql, *params)
            return _row_to_stored_artifact(row, deserializer=self._json_deserializer) if row is not None else None
        except Exception as exc:
            if is_pg_table_missing(exc):
                return None
            raise

    def get_artifact_reference(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        *,
        session_id: str | None = None,
        version: int | None = None,
    ) -> ArtifactReference | None:
        """Retrieve stored object metadata as a ``vertebra.storage.ArtifactReference``."""
        sql, params = self._get_artifact_sql_and_params(
            app_name, user_id, filename, session_id, version, include_ref_cols=True
        )
        try:
            with self._provide_replica_session(primary=False) as driver:
                row = driver.select_one_or_none(sql, *params)
            return _row_to_artifact_reference(row) if row is not None else None
        except Exception as exc:
            if is_pg_table_missing(exc):
                return None
            raise

    def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """Return distinct sorted filenames stored under an app/user/session scope."""
        sql, params = self._list_keys_sql_and_params(app_name, user_id, session_id)
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, *params)
            return [str(pg_row_value(r, "filename", 0)) for r in rows]
        except Exception as exc:
            if is_pg_table_missing(exc):
                return []
            raise

    def list_artifact_versions(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None
    ) -> list[StoredArtifact]:
        """Return ascending ``StoredArtifact`` versions for a filename."""
        sql, params = self._list_versions_sql_and_params(app_name, user_id, filename, session_id)
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, *params)
            return [_row_to_stored_artifact(r, deserializer=self._json_deserializer) for r in rows]
        except Exception as exc:
            if is_pg_table_missing(exc):
                return []
            raise

    def delete_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None
    ) -> list[StoredArtifact]:
        """Delete all stored versions of an artifact and return removed records."""
        existing = self.list_artifact_versions(app_name, user_id, filename, session_id=session_id)
        sql, params = self._delete_artifact_sql_and_params(app_name, user_id, filename, session_id)
        with self._config.provide_session() as driver:
            driver.execute(sql, *params)
            driver.commit()
        return existing

    def get_next_version(self, app_name: str, user_id: str, filename: str, session_id: str | None = None) -> int:
        """Return the next sequential artifact version (``MAX(version) + 1`` or ``0``)."""
        sql, params = self._get_next_version_sql_and_params(app_name, user_id, filename, session_id)
        with self._config.provide_session() as driver:
            val = driver.select_value_or_none(sql, *params)
        return 0 if val is None else int(val) + 1

    def delete_artifacts_older_than(self, before: datetime, app_name: str | None = None) -> list[StoredArtifact]:
        """Delete artifact rows older than ``before`` and return the removed records."""
        select_sql, delete_sql, params = self._delete_older_than_sqls_and_params(before, app_name)
        try:
            with self._config.provide_session() as driver:
                rows = driver.select(select_sql, *params)
                driver.execute(delete_sql, *params)
                driver.commit()
            return [_row_to_stored_artifact(r, deserializer=self._json_deserializer) for r in rows]
        except Exception as exc:
            if is_pg_table_missing(exc):
                return []
            raise


class PostgresAsyncADKArtifactStore(_PostgresADKArtifactSQLMixin, _BaseAsyncADKArtifactStore["PostgresAsyncConfig"]):
    """Asynchronous PostgreSQL ADK artifact metadata store returning ``ArtifactReference``."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    def __init__(self, config: PostgresAsyncConfig, *, read_config: PostgresAsyncConfig | None = None) -> None:
        """Initialize the asynchronous PostgreSQL ADK artifact store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    async def create_table(self) -> None:
        """Asynchronously create the ADK artifact metadata table."""
        async with self._config.provide_session() as driver:
            await driver.execute_script(self._build_artifact_table_ddl())
            await driver.commit()

    async def insert_artifact(self, record: StoredArtifact) -> None:
        """Asynchronously upsert an ADK artifact metadata record."""
        sql, params = self._insert_artifact_sql_and_params(record)
        async with self._config.provide_session() as driver:
            await driver.execute(sql, *params)
            await driver.commit()

    async def insert_artifact_reference(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        reference: ArtifactReference,
        *,
        session_id: str | None = None,
        custom_metadata: dict[str, Any] | None = None,
    ) -> None:
        """Asynchronously store a ``vertebra.storage.ArtifactReference`` record."""
        sql, params = self._insert_artifact_ref_sql_and_params(
            app_name, user_id, filename, reference, session_id, custom_metadata
        )
        async with self._config.provide_session() as driver:
            await driver.execute(sql, *params)
            await driver.commit()

    async def get_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> StoredArtifact | None:
        """Asynchronously fetch an ADK artifact record."""
        sql, params = self._get_artifact_sql_and_params(
            app_name, user_id, filename, session_id, version, include_ref_cols=False
        )
        try:
            async with self._provide_replica_session(primary=False) as driver:
                row = await driver.select_one_or_none(sql, *params)
            return _row_to_stored_artifact(row, deserializer=self._json_deserializer) if row is not None else None
        except Exception as exc:
            if is_pg_table_missing(exc):
                return None
            raise

    async def get_artifact_reference(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        *,
        session_id: str | None = None,
        version: int | None = None,
    ) -> ArtifactReference | None:
        """Asynchronously retrieve stored object metadata as ``ArtifactReference``."""
        sql, params = self._get_artifact_sql_and_params(
            app_name, user_id, filename, session_id, version, include_ref_cols=True
        )
        try:
            async with self._provide_replica_session(primary=False) as driver:
                row = await driver.select_one_or_none(sql, *params)
            return _row_to_artifact_reference(row) if row is not None else None
        except Exception as exc:
            if is_pg_table_missing(exc):
                return None
            raise

    async def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """Asynchronously list distinct artifact filenames."""
        sql, params = self._list_keys_sql_and_params(app_name, user_id, session_id)
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, *params)
            return [str(pg_row_value(r, "filename", 0)) for r in rows]
        except Exception as exc:
            if is_pg_table_missing(exc):
                return []
            raise

    async def list_artifact_versions(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None
    ) -> list[StoredArtifact]:
        """Asynchronously list stored ``StoredArtifact`` versions for an artifact."""
        sql, params = self._list_versions_sql_and_params(app_name, user_id, filename, session_id)
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, *params)
            return [_row_to_stored_artifact(r, deserializer=self._json_deserializer) for r in rows]
        except Exception as exc:
            if is_pg_table_missing(exc):
                return []
            raise

    async def delete_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None
    ) -> list[StoredArtifact]:
        """Asynchronously delete all stored versions of an artifact and return removed records."""
        existing = await self.list_artifact_versions(app_name, user_id, filename, session_id=session_id)
        sql, params = self._delete_artifact_sql_and_params(app_name, user_id, filename, session_id)
        async with self._config.provide_session() as driver:
            await driver.execute(sql, *params)
            await driver.commit()
        return existing

    async def get_next_version(self, app_name: str, user_id: str, filename: str, session_id: str | None = None) -> int:
        """Asynchronously compute the next sequential artifact version."""
        sql, params = self._get_next_version_sql_and_params(app_name, user_id, filename, session_id)
        async with self._config.provide_session() as driver:
            val = await driver.select_value_or_none(sql, *params)
        return 0 if val is None else int(val) + 1

    async def delete_artifacts_older_than(self, before: datetime, app_name: str | None = None) -> list[StoredArtifact]:
        """Asynchronously delete artifact metadata rows older than ``before`` and return removed records."""
        select_sql, delete_sql, params = self._delete_older_than_sqls_and_params(before, app_name)
        try:
            async with self._config.provide_session() as driver:
                rows = await driver.select(select_sql, *params)
                await driver.execute(delete_sql, *params)
                await driver.commit()
            return [_row_to_stored_artifact(r, deserializer=self._json_deserializer) for r in rows]
        except Exception as exc:
            if is_pg_table_missing(exc):
                return []
            raise
