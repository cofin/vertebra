"""PostgreSQL ADK session, vector/FTS memory, and artifact metadata stores without requiring `google-adk`."""

from __future__ import annotations

import logging
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Final, Literal, cast

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

if TYPE_CHECKING:
    from sqlspec.extensions.adk import StoredMemory
    from sqlspec.extensions.adk.artifact._types import StoredArtifact
    from sqlspec.extensions.adk.artifact.store import BaseAsyncADKArtifactStore as _BaseAsyncADKArtifactStore
    from sqlspec.extensions.adk.artifact.store import BaseSyncADKArtifactStore as _BaseSyncADKArtifactStore
    from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore as _BaseAsyncADKMemoryStore
    from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore as _BaseSyncADKMemoryStore

    from vertebra.extensions.sqlspec.adapters.postgres.config import PostgresAsyncConfig, PostgresSyncConfig
else:
    StoredArtifact = dict[str, Any]
    StoredMemory = dict[str, Any]

    try:
        from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore as _BaseAsyncADKMemoryStore
        from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore as _BaseSyncADKMemoryStore
    except ImportError:

        class _FallbackADKMemoryStoreCommon:
            """SDK-free ADK memory store base used when optional `google-adk` is absent."""

            __slots__ = (
                "_config",
                "_enabled",
                "_max_results",
                "_memory_table",
                "_owner_id_column_ddl",
                "_owner_id_column_name",
                "_retention_days",
                "_use_fts",
            )

            @classmethod
            def __class_getitem__(cls, _item: Any) -> type[Any]:
                """Support generic parameterization at runtime without external dependencies."""
                return cls

            def __init__(self, config: Any) -> None:
                """Initialize ADK memory table configuration from `extension_config['adk']`."""
                self._config = config
                ext = getattr(config, "extension_config", {})
                adk = ext.get("adk", {}) if isinstance(ext, Mapping) else {}
                adk_map = adk if isinstance(adk, Mapping) else {}
                self._memory_table: str = str(adk_map.get("memory_table", "adk_memory_entries"))
                self._enabled: bool = bool(adk_map.get("memory_enabled", True))
                self._use_fts: bool = bool(adk_map.get("memory_use_fts", False))
                self._max_results: int = int(adk_map.get("memory_max_results", 20))
                self._retention_days: int | None = adk_map.get("memory_retention_days")
                owner_ddl = adk_map.get("owner_id_column")
                self._owner_id_column_ddl: str | None = str(owner_ddl) if owner_ddl else None
                self._owner_id_column_name: str | None = (
                    str(owner_ddl).strip().split()[0].strip('"') if owner_ddl else None
                )

            @property
            def config(self) -> Any:
                """Return the owning SQLSpec database configuration."""
                return self._config

            @property
            def memory_table(self) -> str:
                """Return the configured memory table name."""
                return self._memory_table

            @property
            def enabled(self) -> bool:
                """Return whether memory operations are enabled."""
                return self._enabled

            @property
            def create_schema_enabled(self) -> bool:
                """Return whether schema auto-creation is enabled."""
                ext = getattr(self._config, "extension_config", {})
                adk = ext.get("adk", {}) if isinstance(ext, Mapping) else {}
                if not isinstance(adk, Mapping):
                    return True
                return bool(adk.get("manage_schema", True)) and bool(adk.get("create_schema", True))

            def reconcile_schema(self, *, assume_existing: bool = False) -> Any:
                """No-op schema reconciliation fallback when `google-adk` is absent."""
                _ = assume_existing
                return None

        class _BaseSyncADKMemoryStore(_FallbackADKMemoryStoreCommon):
            """Fallback synchronous memory store base when `google-adk` is absent."""

            def drop_tables(self) -> None:
                """Execute drop statements sequentially inside a sync session."""
                statements = self._drop_memory_table_sql()
                with self._config.provide_session() as driver:
                    for statement in statements:
                        driver.execute(statement)
                    driver.commit()

            def ensure_tables(self) -> None:
                """Ensure memory tables exist when `create_schema_enabled` is True."""
                if not self._enabled:
                    return
                if self.create_schema_enabled:
                    self.create_tables()

            def _drop_memory_table_sql(self) -> list[str]:
                raise NotImplementedError

            def create_tables(self) -> None:
                """Create the memory table."""
                raise NotImplementedError

        class _BaseAsyncADKMemoryStore(_FallbackADKMemoryStoreCommon):
            """Fallback asynchronous memory store base when `google-adk` is absent."""

            async def drop_tables(self) -> None:
                """Execute drop statements sequentially inside an async session."""
                statements = self._drop_memory_table_sql()
                async with self._config.provide_session() as driver:
                    for statement in statements:
                        await driver.execute(statement)
                    await driver.commit()

            async def ensure_tables(self) -> None:
                """Ensure memory tables exist when `create_schema_enabled` is True."""
                if not self._enabled:
                    return
                if self.create_schema_enabled:
                    await self.create_tables()

            def _drop_memory_table_sql(self) -> list[str]:
                raise NotImplementedError

            async def create_tables(self) -> None:
                """Create the memory table asynchronously."""
                raise NotImplementedError

    try:
        from sqlspec.extensions.adk.artifact.store import BaseAsyncADKArtifactStore as _BaseAsyncADKArtifactStore
        from sqlspec.extensions.adk.artifact.store import BaseSyncADKArtifactStore as _BaseSyncADKArtifactStore
    except ImportError:

        class _FallbackADKArtifactStoreCommon:
            """SDK-free ADK artifact metadata store base used when optional `google-adk` is absent."""

            __slots__ = ("_artifact_table", "_config")

            @classmethod
            def __class_getitem__(cls, _item: Any) -> type[Any]:
                """Support generic parameterization at runtime without external dependencies."""
                return cls

            def __init__(self, config: Any) -> None:
                """Initialize ADK artifact store metadata from configuration."""
                self._config = config
                ext = getattr(config, "extension_config", {})
                adk = ext.get("adk", {}) if isinstance(ext, Mapping) else {}
                self._artifact_table: str = (
                    str(adk.get("artifact_table", "adk_artifacts")) if isinstance(adk, Mapping) else "adk_artifacts"
                )

            @property
            def config(self) -> Any:
                """Return the owning SQLSpec database configuration."""
                return self._config

            @property
            def artifact_table(self) -> str:
                """Return the configured artifact metadata table name."""
                return self._artifact_table

        class _BaseSyncADKArtifactStore(_FallbackADKArtifactStoreCommon):
            """Fallback synchronous ADK artifact store base when `google-adk` is absent."""

            def ensure_table(self) -> None:
                """Create the artifact table if missing."""
                self.create_table()

            def create_table(self) -> None:
                """Create the artifact metadata table."""
                raise NotImplementedError

        class _BaseAsyncADKArtifactStore(_FallbackADKArtifactStoreCommon):
            """Fallback asynchronous ADK artifact store base when `google-adk` is absent."""

            async def ensure_table(self) -> None:
                """Asynchronously create the artifact table if missing."""
                await self.create_table()

            async def create_table(self) -> None:
                """Create the artifact metadata table asynchronously."""
                raise NotImplementedError


APP_STATE_PREFIX: Final[str] = "app:"
TEMP_STATE_PREFIX: Final[str] = "temp:"
USER_STATE_PREFIX: Final[str] = "user:"

__all__ = (
    "PostgresAsyncADKArtifactStore",
    "PostgresAsyncADKMemoryStore",
    "PostgresAsyncADKStore",
    "PostgresSyncADKArtifactStore",
    "PostgresSyncADKMemoryStore",
    "PostgresSyncADKStore",
)

logger = logging.getLogger("vertebra.extensions.sqlspec.adapters.postgres.adk")


def _adk_settings(config: Any) -> dict[str, Any]:
    """Extract `extension_config['adk']` as a mutable dictionary."""
    ext = getattr(config, "extension_config", {})
    if not isinstance(ext, Mapping):
        return {}
    adk = ext.get("adk", {})
    return dict(adk) if isinstance(adk, Mapping) else {}


def _resolve_json_codecs(config: Any) -> tuple[Any, Any]:
    """Resolve optional JSON serializer/deserializer hooks from driver features."""
    features = getattr(config, "driver_features", {}) or {}
    return features.get("json_serializer"), features.get("json_deserializer")


def _split_state_scopes(state: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Split session state into app-scoped, user-scoped, and stripped session dictionaries."""
    app_updates: dict[str, Any] = {}
    user_updates: dict[str, Any] = {}
    session_state: dict[str, Any] = {}
    for key, value in state.items():
        if key.startswith(APP_STATE_PREFIX):
            app_updates[key.removeprefix(APP_STATE_PREFIX)] = value
        elif key.startswith(USER_STATE_PREFIX):
            user_updates[key.removeprefix(USER_STATE_PREFIX)] = value
        elif not key.startswith(TEMP_STATE_PREFIX):
            session_state[key] = value
    return app_updates, user_updates, session_state


def _sessions_ddl(session_table: str, owner_id_ddl: str | None, adk_cfg: Mapping[str, Any]) -> str:
    """Build idempotent PostgreSQL DDL for the ADK sessions table."""
    table_ref = pg_table_ref(session_table)
    owner_col = f"{owner_id_ddl}, " if owner_id_ddl else ""
    tuning = postgres_storage_params_sql(
        adk_cfg,
        default_fillfactor=80,
        default_vacuum_scale=0.05,
        default_analyze_scale=0.02,
        include_autovacuum=True,
    )
    idx_app_user = pg_index_name("idx", session_table, "app_user")
    idx_update = pg_index_name("idx", session_table, "update_time")
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
        f"CREATE INDEX IF NOT EXISTS {idx_update} ON {table_ref} (update_time DESC);"
    )


def _events_ddl(events_table: str, session_table: str, adk_cfg: Mapping[str, Any]) -> str:
    """Build idempotent PostgreSQL DDL for the ADK events table."""
    events_ref = pg_table_ref(events_table)
    sessions_ref = pg_table_ref(session_table)
    fk_name = pg_index_name("fk", events_table, "session")
    idx_session = pg_index_name("idx", events_table, "session")
    idx_timestamp = pg_index_name("idx", events_table, "timestamp")
    idx_gin = pg_index_name("idx", events_table, "event_data_gin")
    tuning = postgres_storage_params_sql(adk_cfg, default_fillfactor=90)
    return (
        f"CREATE TABLE IF NOT EXISTS {events_ref} ("
        "id VARCHAR(128) PRIMARY KEY, "
        "session_id VARCHAR(128) NOT NULL, "
        "app_name VARCHAR(128) NOT NULL, "
        "user_id VARCHAR(128) NOT NULL, "
        "invocation_id VARCHAR(256), "
        "timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, "
        f"event_data {POSTGRES_JSON_FALLBACK} NOT NULL, "
        f"CONSTRAINT {fk_name} FOREIGN KEY (session_id) REFERENCES {sessions_ref}(id) ON DELETE CASCADE"
        f"){tuning};\n"
        f"CREATE INDEX IF NOT EXISTS {idx_session} ON {events_ref} (session_id, timestamp ASC);\n"
        f"CREATE INDEX IF NOT EXISTS {idx_timestamp} ON {events_ref} (app_name, timestamp ASC);\n"
        f"CREATE INDEX IF NOT EXISTS {idx_gin} ON {events_ref} USING GIN (event_data);"
    )


def _app_states_ddl(app_state_table: str) -> str:
    """Build idempotent PostgreSQL DDL for the ADK app state table."""
    table_ref = pg_table_ref(app_state_table)
    return (
        f"CREATE TABLE IF NOT EXISTS {table_ref} ("
        "app_name VARCHAR(128) PRIMARY KEY, "
        f"state {POSTGRES_JSON_FALLBACK} NOT NULL DEFAULT '{{}}'::jsonb, "
        "update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP"
        ");"
    )


def _user_states_ddl(user_state_table: str) -> str:
    """Build idempotent PostgreSQL DDL for the ADK user state table."""
    table_ref = pg_table_ref(user_state_table)
    return (
        f"CREATE TABLE IF NOT EXISTS {table_ref} ("
        "app_name VARCHAR(128) NOT NULL, "
        "user_id VARCHAR(128) NOT NULL, "
        f"state {POSTGRES_JSON_FALLBACK} NOT NULL DEFAULT '{{}}'::jsonb, "
        "update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, "
        "PRIMARY KEY (app_name, user_id)"
        ");"
    )


def _metadata_ddl(metadata_table: str) -> str:
    """Build idempotent PostgreSQL DDL for the ADK internal metadata table."""
    table_ref = pg_table_ref(metadata_table)
    return (
        f"CREATE TABLE IF NOT EXISTS {table_ref} ("
        "key VARCHAR(255) PRIMARY KEY, "
        "value TEXT NOT NULL, "
        "update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP"
        ");"
    )


def _row_to_stored_session(row: Any, *, deserializer: Any = None) -> StoredSession:
    """Decode a PostgreSQL session row mapping into a `StoredSession` dictionary."""
    raw_state = pg_row_value(row, "state", 3)
    state_val = pg_json_loads(raw_state, deserializer=deserializer) if raw_state is not None else {}
    return StoredSession(
        id=str(pg_row_value(row, "id", 0)),
        app_name=str(pg_row_value(row, "app_name", 1)),
        user_id=str(pg_row_value(row, "user_id", 2)),
        state=state_val if isinstance(state_val, dict) else {},
        create_time=pg_ensure_utc(pg_row_value(row, "create_time", 4)),
        update_time=pg_ensure_utc(pg_row_value(row, "update_time", 5)),
    )


def _row_to_stored_event(row: Any, *, deserializer: Any = None) -> StoredEvent:
    """Decode a PostgreSQL event row mapping into a `StoredEvent` dictionary."""
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


def _memory_table_ddl(
    memory_table: str,
    owner_id_ddl: str | None,
    *,
    vector_dimensions: int | None,
    enable_hnsw: bool,
    enable_bm25: bool,
    use_fts: bool,
) -> str:
    """Build PostgreSQL DDL for ADK memory entries with optional pgvector and FTS indexes."""
    table_ref = pg_table_ref(memory_table)
    owner_col = f"{owner_id_ddl}, " if owner_id_ddl else ""
    embedding_col = f"embedding VECTOR({int(vector_dimensions)}), " if vector_dimensions else "embedding FLOAT8[], "
    stmts = [
        f"CREATE TABLE IF NOT EXISTS {table_ref} ("
        "id VARCHAR(128) PRIMARY KEY, "
        "session_id VARCHAR(128) NOT NULL, "
        "app_name VARCHAR(128) NOT NULL, "
        "user_id VARCHAR(128) NOT NULL, "
        "scope VARCHAR(16) NOT NULL DEFAULT 'user', "
        "event_id VARCHAR(128) NOT NULL UNIQUE, "
        "author VARCHAR(128), "
        f"{owner_col}"
        "timestamp TIMESTAMPTZ NOT NULL, "
        f"{embedding_col}"
        f"content_json {POSTGRES_JSON_FALLBACK} NOT NULL, "
        "content_text TEXT NOT NULL, "
        f"metadata_json {POSTGRES_JSON_FALLBACK}, "
        "inserted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP"
        ");",
        f"CREATE INDEX IF NOT EXISTS {pg_index_name('idx', memory_table, 'session')} ON {table_ref} (session_id);",
        f"CREATE INDEX IF NOT EXISTS {pg_index_name('idx', memory_table, 'scope')} "
        f"ON {table_ref} (app_name, scope, user_id, timestamp DESC);",
    ]
    if use_fts:
        stmts.append(
            f"CREATE INDEX IF NOT EXISTS {pg_index_name('idx', memory_table, 'fts')} "
            f"ON {table_ref} USING GIN (to_tsvector('english', content_text));"
        )
    if vector_dimensions and enable_hnsw:
        stmts.append(
            f"CREATE INDEX IF NOT EXISTS {pg_index_name('idx', memory_table, 'embedding_hnsw')} "
            f"ON {table_ref} USING hnsw (embedding vector_cosine_ops);"
        )
    if enable_bm25:
        stmts.append(
            f"CREATE INDEX IF NOT EXISTS {pg_index_name('idx', memory_table, 'bm25')} "
            f"ON {table_ref} USING bm25 (content_text) WITH (text_config = 'english');"
        )
    return "\n".join(stmts)


def _cosine_similarity(left: Sequence[float], right: Sequence[float]) -> float:
    """Compute cosine similarity between two equal-length dense vectors."""
    if not left or len(left) != len(right):
        return 0.0
    dot = sum(float(a) * float(b) for a, b in zip(left, right, strict=True))
    norm_left = math.sqrt(sum(float(a) * float(a) for a in left))
    norm_right = math.sqrt(sum(float(b) * float(b) for b in right))
    if norm_left == 0.0 or norm_right == 0.0:
        return 0.0
    return dot / (norm_left * norm_right)


def _build_memory_scope_where(
    app_name: str, user_id: str, scope_filter: Literal["all", "user", "app"]
) -> tuple[str, tuple[Any, ...]]:
    """Build `$N` PostgreSQL scope predicates matching user, app, or combined memory scopes."""
    if scope_filter == "user":
        return "app_name = $1 AND scope = 'user' AND user_id = $2", (app_name, user_id)
    if scope_filter == "app":
        return "app_name = $1 AND scope = 'app'", (app_name,)
    return "app_name = $1 AND ((scope = 'user' AND user_id = $2) OR scope = 'app')", (app_name, user_id)


def _row_to_stored_memory(row: Any, *, deserializer: Any = None) -> StoredMemory:
    """Decode a PostgreSQL memory row mapping into a `StoredMemory` dictionary."""
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
    """Decode a PostgreSQL artifact row into a `StoredArtifact` dictionary."""
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
    """Decode an artifact row into a `vertebra.storage.ArtifactReference`."""
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


class PostgresSyncADKStore(BaseSyncADKStore["PostgresSyncConfig"]):
    """Synchronous PostgreSQL ADK session and event store backed by `PostgresSyncConfig`."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    def __init__(self, config: PostgresSyncConfig) -> None:
        """Initialize the synchronous PostgreSQL ADK session store."""
        super().__init__(config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    def _sessions_table_ddl(self) -> str:
        return _sessions_ddl(self._session_table, self._owner_id_column_ddl, _adk_settings(self._config))

    def _events_table_ddl(self) -> str:
        return _events_ddl(self._events_table, self._session_table, _adk_settings(self._config))

    def _app_states_table_ddl(self) -> str:
        return _app_states_ddl(self._app_state_table)

    def _user_states_table_ddl(self) -> str:
        return _user_states_ddl(self._user_state_table)

    def _metadata_table_ddl(self) -> str:
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

    def create_session(
        self, session_id: str, app_name: str, user_id: str, state: dict[str, Any], owner_id: Any | None = None
    ) -> StoredSession:
        """Insert a new ADK session row using `$1..$N` binds and return `StoredSession`."""
        table_ref = pg_table_ref(self._session_table)
        app_updates, user_updates, session_state = _split_state_scopes(state)
        state_json = pg_json_dumps(session_state, serializer=self._json_serializer)
        with self._config.provide_session() as driver:
            if app_updates:
                self._upsert_app_state_with_driver(driver, app_name, app_updates)
            if user_updates:
                self._upsert_user_state_with_driver(driver, app_name, user_id, user_updates)
            if self._owner_id_column_name:
                owner_col = pg_table_ref(self._owner_id_column_name, "owner column")
                sql = (
                    f"INSERT INTO {table_ref} (id, app_name, user_id, {owner_col}, state, create_time, update_time) "
                    "VALUES ($1, $2, $3, $4, $5::jsonb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) "
                    "RETURNING id, app_name, user_id, state, create_time, update_time"
                )
                row = driver.select_one_or_none(sql, session_id, app_name, user_id, owner_id, state_json)
            else:
                sql = (
                    f"INSERT INTO {table_ref} (id, app_name, user_id, state, create_time, update_time) "
                    "VALUES ($1, $2, $3, $4::jsonb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) "
                    "RETURNING id, app_name, user_id, state, create_time, update_time"
                )
                row = driver.select_one_or_none(sql, session_id, app_name, user_id, state_json)
            driver.commit()
        if row is not None:
            return _row_to_stored_session(row, deserializer=self._json_deserializer)
        created = self.get_session(app_name, user_id, session_id)
        if created is None:
            msg = f"Failed to reload newly created ADK session {session_id!r}"
            raise RuntimeError(msg)
        return created

    def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: int | timedelta | None = None
    ) -> StoredSession | None:
        """Fetch an ADK session by composite key, sliding `update_time` if requested."""
        table_ref = pg_table_ref(self._session_table)
        should_touch = renew_for is not None and self._calculate_expires_at(renew_for) is not None
        try:
            with self._config.provide_session() as driver:
                if should_touch:
                    row = driver.select_one_or_none(
                        f"UPDATE {table_ref} SET update_time = CURRENT_TIMESTAMP "
                        "WHERE app_name = $1 AND user_id = $2 AND id = $3 "
                        "RETURNING id, app_name, user_id, state, create_time, update_time",
                        app_name,
                        user_id,
                        session_id,
                    )
                    driver.commit()
                else:
                    row = driver.select_one_or_none(
                        f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table_ref} "
                        "WHERE app_name = $1 AND user_id = $2 AND id = $3",
                        app_name,
                        user_id,
                        session_id,
                    )
                if row is None:
                    return None
                return _row_to_stored_session(row, deserializer=self._json_deserializer)
        except Exception as exc:
            if is_pg_table_missing(exc):
                return None
            raise

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Update JSONB session state and scoped app/user state dictionaries."""
        table_ref = pg_table_ref(self._session_table)
        app_updates, user_updates, session_state = _split_state_scopes(state)
        state_json = pg_json_dumps(session_state, serializer=self._json_serializer)
        with self._config.provide_session() as driver:
            if app_updates:
                self._upsert_app_state_with_driver(driver, app_name, app_updates)
            if user_updates:
                self._upsert_user_state_with_driver(driver, app_name, user_id, user_updates)
            driver.execute(
                f"UPDATE {table_ref} SET state = $1::jsonb, update_time = CURRENT_TIMESTAMP "
                "WHERE app_name = $2 AND user_id = $3 AND id = $4",
                state_json,
                app_name,
                user_id,
                session_id,
            )
            driver.commit()

    def list_sessions(
        self,
        app_name: str,
        user_id: str | None = None,
        *,
        order_by: Any = "update_time",
        descending: bool = True,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[StoredSession]:
        """List ADK sessions with validated keyset/offset sorting."""
        col, direction, norm_limit, norm_offset = normalize_session_list_options(order_by, descending, limit, offset)
        if norm_limit == 0:
            return []
        table_ref = pg_table_ref(self._session_table)
        params: list[Any] = [app_name]
        where_sql = "app_name = $1"
        if user_id is not None:
            where_sql += " AND user_id = $2"
            params.append(user_id)
        sql = f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table_ref} WHERE {where_sql} ORDER BY {col} {direction}, id {direction}"
        if norm_limit is not None:
            params.append(norm_limit)
            sql += f" LIMIT ${len(params)}"
        if norm_offset > 0:
            params.append(norm_offset)
            sql += f" OFFSET ${len(params)}"
        try:
            with self._config.provide_session() as driver:
                rows = driver.select(sql, *params)
            return [_row_to_stored_session(row, deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_pg_table_missing(exc):
                return []
            raise

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete an ADK session row (cascading event deletions via FK)."""
        table_ref = pg_table_ref(self._session_table)
        with self._config.provide_session() as driver:
            driver.execute(
                f"DELETE FROM {table_ref} WHERE app_name = $1 AND user_id = $2 AND id = $3",
                app_name,
                user_id,
                session_id,
            )
            driver.commit()

    def _insert_event_with_driver(self, driver: Any, event_record: StoredEvent) -> None:
        events_ref = pg_table_ref(self._events_table)
        event_json = pg_json_dumps(event_record["event_data"], serializer=self._json_serializer)
        driver.execute(
            f"INSERT INTO {events_ref} (id, session_id, app_name, user_id, invocation_id, timestamp, event_data) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb) ON CONFLICT (id) DO NOTHING",
            event_record["id"],
            event_record["session_id"],
            event_record["app_name"],
            event_record["user_id"],
            event_record.get("invocation_id"),
            pg_ensure_utc(event_record["timestamp"]),
            event_json,
        )

    def append_event(self, event_record: StoredEvent) -> None:
        """Append a single ADK event JSONB row."""
        with self._config.provide_session() as driver:
            self._insert_event_with_driver(driver, event_record)
            driver.commit()

    def append_event_and_update_state(
        self,
        event_record: StoredEvent,
        app_name: str,
        user_id: str,
        session_id: str,
        state: dict[str, Any],
        *,
        app_state: dict[str, Any] | None = None,
        user_state: dict[str, Any] | None = None,
    ) -> StoredSession:
        """Atomically append an event and persist session/app/user state in one transaction."""
        table_ref = pg_table_ref(self._session_table)
        app_updates, user_updates, session_state = _split_state_scopes(state)
        if app_state:
            app_updates.update(app_state)
        if user_state:
            user_updates.update(user_state)
        state_json = pg_json_dumps(session_state, serializer=self._json_serializer)
        with self._config.provide_session() as driver:
            self._insert_event_with_driver(driver, event_record)
            if app_updates:
                self._upsert_app_state_with_driver(driver, app_name, app_updates)
            if user_updates:
                self._upsert_user_state_with_driver(driver, app_name, user_id, user_updates)
            row = driver.select_one_or_none(
                f"UPDATE {table_ref} SET state = $1::jsonb, update_time = CURRENT_TIMESTAMP "
                "WHERE app_name = $2 AND user_id = $3 AND id = $4 "
                "RETURNING id, app_name, user_id, state, create_time, update_time",
                state_json,
                app_name,
                user_id,
                session_id,
            )
            driver.commit()
        if row is None:
            msg = f"Session {session_id!r} not found for app={app_name!r}, user={user_id!r}"
            raise RuntimeError(msg)
        return _row_to_stored_session(row, deserializer=self._json_deserializer)

    def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> list[StoredEvent]:
        """Retrieve ADK events for a session ordered ascending by timestamp."""
        events_ref = pg_table_ref(self._events_table)
        params: list[Any] = [app_name, user_id, session_id]
        sql = (
            f"SELECT id, session_id, app_name, user_id, invocation_id, timestamp, event_data "
            f"FROM {events_ref} WHERE app_name = $1 AND user_id = $2 AND session_id = $3"
        )
        if after_timestamp is not None:
            params.append(pg_ensure_utc(after_timestamp))
            sql += f" AND timestamp > ${len(params)}"
        sql += " ORDER BY timestamp ASC, id ASC"
        if limit is not None:
            params.append(limit)
            sql += f" LIMIT ${len(params)}"
        try:
            with self._config.provide_session() as driver:
                rows = driver.select(sql, *params)
            return [_row_to_stored_event(row, deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_pg_table_missing(exc):
                return []
            raise

    def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Delete events older than `before`, optionally restricted to one app."""
        events_ref = pg_table_ref(self._events_table)
        cutoff = pg_ensure_utc(before)
        with self._config.provide_session() as driver:
            if app_name is not None:
                res = driver.execute(
                    f"DELETE FROM {events_ref} WHERE timestamp < $1 AND app_name = $2", cutoff, app_name
                )
            else:
                res = driver.execute(f"DELETE FROM {events_ref} WHERE timestamp < $1", cutoff)
            driver.commit()
            return int(res.rows_affected or 0)

    def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete user state rows whose `update_time` is older than `updated_before`."""
        table_ref = pg_table_ref(self._user_state_table)
        cutoff = pg_ensure_utc(updated_before)
        with self._config.provide_session() as driver:
            if app_name is not None:
                res = driver.execute(
                    f"DELETE FROM {table_ref} WHERE update_time < $1 AND app_name = $2", cutoff, app_name
                )
            else:
                res = driver.execute(f"DELETE FROM {table_ref} WHERE update_time < $1", cutoff)
            driver.commit()
            return int(res.rows_affected or 0)

    def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete sessions whose `update_time` is older than `updated_before`."""
        table_ref = pg_table_ref(self._session_table)
        cutoff = pg_ensure_utc(updated_before)
        with self._config.provide_session() as driver:
            if app_name is not None:
                res = driver.execute(
                    f"DELETE FROM {table_ref} WHERE update_time < $1 AND app_name = $2", cutoff, app_name
                )
            else:
                res = driver.execute(f"DELETE FROM {table_ref} WHERE update_time < $1", cutoff)
            driver.commit()
            return int(res.rows_affected or 0)

    def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Fetch app-scoped JSONB state for `app_name`."""
        table_ref = pg_table_ref(self._app_state_table)
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(f"SELECT state FROM {table_ref} WHERE app_name = $1", app_name)
            if row is None:
                return None
            decoded = pg_json_loads(pg_row_value(row, "state", 0), deserializer=self._json_deserializer)
            return decoded if isinstance(decoded, dict) else {}
        except Exception as exc:
            if is_pg_table_missing(exc):
                return None
            raise

    def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Fetch user-scoped JSONB state for `(app_name, user_id)`."""
        table_ref = pg_table_ref(self._user_state_table)
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(
                    f"SELECT state FROM {table_ref} WHERE app_name = $1 AND user_id = $2",
                    app_name,
                    user_id,
                )
            if row is None:
                return None
            decoded = pg_json_loads(pg_row_value(row, "state", 0), deserializer=self._json_deserializer)
            return decoded if isinstance(decoded, dict) else {}
        except Exception as exc:
            if is_pg_table_missing(exc):
                return None
            raise

    def _upsert_app_state_with_driver(self, driver: Any, app_name: str, state: dict[str, Any]) -> None:
        table_ref = pg_table_ref(self._app_state_table)
        state_json = pg_json_dumps(state, serializer=self._json_serializer)
        driver.execute(
            f"INSERT INTO {table_ref} (app_name, state, update_time) VALUES ($1, $2::jsonb, CURRENT_TIMESTAMP) "
            "ON CONFLICT (app_name) DO UPDATE SET state = EXCLUDED.state, update_time = CURRENT_TIMESTAMP",
            app_name,
            state_json,
        )

    def _upsert_user_state_with_driver(self, driver: Any, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        table_ref = pg_table_ref(self._user_state_table)
        state_json = pg_json_dumps(state, serializer=self._json_serializer)
        driver.execute(
            f"INSERT INTO {table_ref} (app_name, user_id, state, update_time) VALUES ($1, $2, $3::jsonb, CURRENT_TIMESTAMP) "
            "ON CONFLICT (app_name, user_id) DO UPDATE SET state = EXCLUDED.state, update_time = CURRENT_TIMESTAMP",
            app_name,
            user_id,
            state_json,
        )

    def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Upsert app-scoped ADK state via PostgreSQL `ON CONFLICT`."""
        with self._config.provide_session() as driver:
            self._upsert_app_state_with_driver(driver, app_name, state)
            driver.commit()

    def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Upsert user-scoped ADK state via PostgreSQL `ON CONFLICT`."""
        with self._config.provide_session() as driver:
            self._upsert_user_state_with_driver(driver, app_name, user_id, state)
            driver.commit()

    def get_metadata(self, key: str) -> str | None:
        """Return a metadata value from `adk_metadata` or `None` if absent."""
        table_ref = pg_table_ref(self._metadata_table)
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(f"SELECT value FROM {table_ref} WHERE key = $1", key)
            return str(pg_row_value(row, "value", 0)) if row is not None else None
        except Exception as exc:
            if is_pg_table_missing(exc):
                return None
            raise

    def set_metadata(self, key: str, value: str) -> None:
        """Upsert a metadata key-value entry via PostgreSQL `ON CONFLICT`."""
        table_ref = pg_table_ref(self._metadata_table)
        with self._config.provide_session() as driver:
            driver.execute(
                f"INSERT INTO {table_ref} (key, value, update_time) VALUES ($1, $2, CURRENT_TIMESTAMP) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, update_time = CURRENT_TIMESTAMP",
                key,
                value,
            )
            driver.commit()


class PostgresAsyncADKStore(BaseAsyncADKStore["PostgresAsyncConfig"]):
    """Asynchronous PostgreSQL ADK session and event store backed by `PostgresAsyncConfig`."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    def __init__(self, config: PostgresAsyncConfig) -> None:
        """Initialize the asynchronous PostgreSQL ADK session store."""
        super().__init__(config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    async def _sessions_table_ddl(self) -> str:
        return _sessions_ddl(self._session_table, self._owner_id_column_ddl, _adk_settings(self._config))

    async def _events_table_ddl(self) -> str:
        return _events_ddl(self._events_table, self._session_table, _adk_settings(self._config))

    async def _app_states_table_ddl(self) -> str:
        return _app_states_ddl(self._app_state_table)

    async def _user_states_table_ddl(self) -> str:
        return _user_states_ddl(self._user_state_table)

    async def _metadata_table_ddl(self) -> str:
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

    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: dict[str, Any], owner_id: Any | None = None
    ) -> StoredSession:
        """Asynchronously insert a new ADK session row and return `StoredSession`."""
        table_ref = pg_table_ref(self._session_table)
        app_updates, user_updates, session_state = _split_state_scopes(state)
        state_json = pg_json_dumps(session_state, serializer=self._json_serializer)
        async with self._config.provide_session() as driver:
            if app_updates:
                await self._upsert_app_state_with_driver(driver, app_name, app_updates)
            if user_updates:
                await self._upsert_user_state_with_driver(driver, app_name, user_id, user_updates)
            if self._owner_id_column_name:
                owner_col = pg_table_ref(self._owner_id_column_name, "owner column")
                sql = (
                    f"INSERT INTO {table_ref} (id, app_name, user_id, {owner_col}, state, create_time, update_time) "
                    "VALUES ($1, $2, $3, $4, $5::jsonb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) "
                    "RETURNING id, app_name, user_id, state, create_time, update_time"
                )
                row = await driver.select_one_or_none(sql, session_id, app_name, user_id, owner_id, state_json)
            else:
                sql = (
                    f"INSERT INTO {table_ref} (id, app_name, user_id, state, create_time, update_time) "
                    "VALUES ($1, $2, $3, $4::jsonb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) "
                    "RETURNING id, app_name, user_id, state, create_time, update_time"
                )
                row = await driver.select_one_or_none(sql, session_id, app_name, user_id, state_json)
            await driver.commit()
        if row is not None:
            return _row_to_stored_session(row, deserializer=self._json_deserializer)
        created = await self.get_session(app_name, user_id, session_id)
        if created is None:
            msg = f"Failed to reload newly created ADK session {session_id!r}"
            raise RuntimeError(msg)
        return created

    async def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: int | timedelta | None = None
    ) -> StoredSession | None:
        """Fetch an ADK session asynchronously, sliding `update_time` when requested."""
        table_ref = pg_table_ref(self._session_table)
        should_touch = renew_for is not None and self._calculate_expires_at(renew_for) is not None
        try:
            async with self._config.provide_session() as driver:
                if should_touch:
                    row = await driver.select_one_or_none(
                        f"UPDATE {table_ref} SET update_time = CURRENT_TIMESTAMP "
                        "WHERE app_name = $1 AND user_id = $2 AND id = $3 "
                        "RETURNING id, app_name, user_id, state, create_time, update_time",
                        app_name,
                        user_id,
                        session_id,
                    )
                    await driver.commit()
                else:
                    row = await driver.select_one_or_none(
                        f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table_ref} "
                        "WHERE app_name = $1 AND user_id = $2 AND id = $3",
                        app_name,
                        user_id,
                        session_id,
                    )
                if row is None:
                    return None
                return _row_to_stored_session(row, deserializer=self._json_deserializer)
        except Exception as exc:
            if is_pg_table_missing(exc):
                return None
            raise

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Asynchronously update JSONB session state and scoped state stores."""
        table_ref = pg_table_ref(self._session_table)
        app_updates, user_updates, session_state = _split_state_scopes(state)
        state_json = pg_json_dumps(session_state, serializer=self._json_serializer)
        async with self._config.provide_session() as driver:
            if app_updates:
                await self._upsert_app_state_with_driver(driver, app_name, app_updates)
            if user_updates:
                await self._upsert_user_state_with_driver(driver, app_name, user_id, user_updates)
            await driver.execute(
                f"UPDATE {table_ref} SET state = $1::jsonb, update_time = CURRENT_TIMESTAMP "
                "WHERE app_name = $2 AND user_id = $3 AND id = $4",
                state_json,
                app_name,
                user_id,
                session_id,
            )
            await driver.commit()

    async def list_sessions(
        self,
        app_name: str,
        user_id: str | None = None,
        *,
        order_by: Any = "update_time",
        descending: bool = True,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[StoredSession]:
        """List ADK sessions asynchronously."""
        col, direction, norm_limit, norm_offset = normalize_session_list_options(order_by, descending, limit, offset)
        if norm_limit == 0:
            return []
        table_ref = pg_table_ref(self._session_table)
        params: list[Any] = [app_name]
        where_sql = "app_name = $1"
        if user_id is not None:
            where_sql += " AND user_id = $2"
            params.append(user_id)
        sql = f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table_ref} WHERE {where_sql} ORDER BY {col} {direction}, id {direction}"
        if norm_limit is not None:
            params.append(norm_limit)
            sql += f" LIMIT ${len(params)}"
        if norm_offset > 0:
            params.append(norm_offset)
            sql += f" OFFSET ${len(params)}"
        try:
            async with self._config.provide_session() as driver:
                rows = await driver.select(sql, *params)
            return [_row_to_stored_session(row, deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_pg_table_missing(exc):
                return []
            raise

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Asynchronously delete an ADK session row."""
        table_ref = pg_table_ref(self._session_table)
        async with self._config.provide_session() as driver:
            await driver.execute(
                f"DELETE FROM {table_ref} WHERE app_name = $1 AND user_id = $2 AND id = $3",
                app_name,
                user_id,
                session_id,
            )
            await driver.commit()

    async def _insert_event_with_driver(self, driver: Any, event_record: StoredEvent) -> None:
        events_ref = pg_table_ref(self._events_table)
        event_json = pg_json_dumps(event_record["event_data"], serializer=self._json_serializer)
        await driver.execute(
            f"INSERT INTO {events_ref} (id, session_id, app_name, user_id, invocation_id, timestamp, event_data) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb) ON CONFLICT (id) DO NOTHING",
            event_record["id"],
            event_record["session_id"],
            event_record["app_name"],
            event_record["user_id"],
            event_record.get("invocation_id"),
            pg_ensure_utc(event_record["timestamp"]),
            event_json,
        )

    async def append_event(self, event_record: StoredEvent) -> None:
        """Asynchronously append an ADK event row."""
        async with self._config.provide_session() as driver:
            await self._insert_event_with_driver(driver, event_record)
            await driver.commit()

    async def append_event_and_update_state(
        self,
        event_record: StoredEvent,
        app_name: str,
        user_id: str,
        session_id: str,
        state: dict[str, Any],
        *,
        app_state: dict[str, Any] | None = None,
        user_state: dict[str, Any] | None = None,
    ) -> StoredSession:
        """Atomically append an event and update durable session/app/user state asynchronously."""
        table_ref = pg_table_ref(self._session_table)
        app_updates, user_updates, session_state = _split_state_scopes(state)
        if app_state:
            app_updates.update(app_state)
        if user_state:
            user_updates.update(user_state)
        state_json = pg_json_dumps(session_state, serializer=self._json_serializer)
        async with self._config.provide_session() as driver:
            await self._insert_event_with_driver(driver, event_record)
            if app_updates:
                await self._upsert_app_state_with_driver(driver, app_name, app_updates)
            if user_updates:
                await self._upsert_user_state_with_driver(driver, app_name, user_id, user_updates)
            row = await driver.select_one_or_none(
                f"UPDATE {table_ref} SET state = $1::jsonb, update_time = CURRENT_TIMESTAMP "
                "WHERE app_name = $2 AND user_id = $3 AND id = $4 "
                "RETURNING id, app_name, user_id, state, create_time, update_time",
                state_json,
                app_name,
                user_id,
                session_id,
            )
            await driver.commit()
        if row is None:
            msg = f"Session {session_id!r} not found for app={app_name!r}, user={user_id!r}"
            raise RuntimeError(msg)
        return _row_to_stored_session(row, deserializer=self._json_deserializer)

    async def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> list[StoredEvent]:
        """Asynchronously retrieve ADK events for a session."""
        events_ref = pg_table_ref(self._events_table)
        params: list[Any] = [app_name, user_id, session_id]
        sql = (
            f"SELECT id, session_id, app_name, user_id, invocation_id, timestamp, event_data "
            f"FROM {events_ref} WHERE app_name = $1 AND user_id = $2 AND session_id = $3"
        )
        if after_timestamp is not None:
            params.append(pg_ensure_utc(after_timestamp))
            sql += f" AND timestamp > ${len(params)}"
        sql += " ORDER BY timestamp ASC, id ASC"
        if limit is not None:
            params.append(limit)
            sql += f" LIMIT ${len(params)}"
        try:
            async with self._config.provide_session() as driver:
                rows = await driver.select(sql, *params)
            return [_row_to_stored_event(row, deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_pg_table_missing(exc):
                return []
            raise

    async def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Asynchronously delete events older than `before`."""
        events_ref = pg_table_ref(self._events_table)
        cutoff = pg_ensure_utc(before)
        async with self._config.provide_session() as driver:
            if app_name is not None:
                res = await driver.execute(
                    f"DELETE FROM {events_ref} WHERE timestamp < $1 AND app_name = $2", cutoff, app_name
                )
            else:
                res = await driver.execute(f"DELETE FROM {events_ref} WHERE timestamp < $1", cutoff)
            await driver.commit()
            return int(res.rows_affected or 0)

    async def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Asynchronously delete idle user states older than `updated_before`."""
        table_ref = pg_table_ref(self._user_state_table)
        cutoff = pg_ensure_utc(updated_before)
        async with self._config.provide_session() as driver:
            if app_name is not None:
                res = await driver.execute(
                    f"DELETE FROM {table_ref} WHERE update_time < $1 AND app_name = $2", cutoff, app_name
                )
            else:
                res = await driver.execute(f"DELETE FROM {table_ref} WHERE update_time < $1", cutoff)
            await driver.commit()
            return int(res.rows_affected or 0)

    async def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Asynchronously delete idle sessions older than `updated_before`."""
        table_ref = pg_table_ref(self._session_table)
        cutoff = pg_ensure_utc(updated_before)
        async with self._config.provide_session() as driver:
            if app_name is not None:
                res = await driver.execute(
                    f"DELETE FROM {table_ref} WHERE update_time < $1 AND app_name = $2", cutoff, app_name
                )
            else:
                res = await driver.execute(f"DELETE FROM {table_ref} WHERE update_time < $1", cutoff)
            await driver.commit()
            return int(res.rows_affected or 0)

    async def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Asynchronously fetch app-scoped state."""
        table_ref = pg_table_ref(self._app_state_table)
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(f"SELECT state FROM {table_ref} WHERE app_name = $1", app_name)
            if row is None:
                return None
            decoded = pg_json_loads(pg_row_value(row, "state", 0), deserializer=self._json_deserializer)
            return decoded if isinstance(decoded, dict) else {}
        except Exception as exc:
            if is_pg_table_missing(exc):
                return None
            raise

    async def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Asynchronously fetch user-scoped state."""
        table_ref = pg_table_ref(self._user_state_table)
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(
                    f"SELECT state FROM {table_ref} WHERE app_name = $1 AND user_id = $2",
                    app_name,
                    user_id,
                )
            if row is None:
                return None
            decoded = pg_json_loads(pg_row_value(row, "state", 0), deserializer=self._json_deserializer)
            return decoded if isinstance(decoded, dict) else {}
        except Exception as exc:
            if is_pg_table_missing(exc):
                return None
            raise

    async def _upsert_app_state_with_driver(self, driver: Any, app_name: str, state: dict[str, Any]) -> None:
        table_ref = pg_table_ref(self._app_state_table)
        state_json = pg_json_dumps(state, serializer=self._json_serializer)
        await driver.execute(
            f"INSERT INTO {table_ref} (app_name, state, update_time) VALUES ($1, $2::jsonb, CURRENT_TIMESTAMP) "
            "ON CONFLICT (app_name) DO UPDATE SET state = EXCLUDED.state, update_time = CURRENT_TIMESTAMP",
            app_name,
            state_json,
        )

    async def _upsert_user_state_with_driver(
        self, driver: Any, app_name: str, user_id: str, state: dict[str, Any]
    ) -> None:
        table_ref = pg_table_ref(self._user_state_table)
        state_json = pg_json_dumps(state, serializer=self._json_serializer)
        await driver.execute(
            f"INSERT INTO {table_ref} (app_name, user_id, state, update_time) VALUES ($1, $2, $3::jsonb, CURRENT_TIMESTAMP) "
            "ON CONFLICT (app_name, user_id) DO UPDATE SET state = EXCLUDED.state, update_time = CURRENT_TIMESTAMP",
            app_name,
            user_id,
            state_json,
        )

    async def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Asynchronously upsert app-scoped ADK state."""
        async with self._config.provide_session() as driver:
            await self._upsert_app_state_with_driver(driver, app_name, state)
            await driver.commit()

    async def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Asynchronously upsert user-scoped ADK state."""
        async with self._config.provide_session() as driver:
            await self._upsert_user_state_with_driver(driver, app_name, user_id, state)
            await driver.commit()

    async def get_metadata(self, key: str) -> str | None:
        """Asynchronously fetch a metadata entry."""
        table_ref = pg_table_ref(self._metadata_table)
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(f"SELECT value FROM {table_ref} WHERE key = $1", key)
            return str(pg_row_value(row, "value", 0)) if row is not None else None
        except Exception as exc:
            if is_pg_table_missing(exc):
                return None
            raise

    async def set_metadata(self, key: str, value: str) -> None:
        """Asynchronously upsert a metadata entry."""
        table_ref = pg_table_ref(self._metadata_table)
        async with self._config.provide_session() as driver:
            await driver.execute(
                f"INSERT INTO {table_ref} (key, value, update_time) VALUES ($1, $2, CURRENT_TIMESTAMP) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, update_time = CURRENT_TIMESTAMP",
                key,
                value,
            )
            await driver.commit()


class PostgresSyncADKMemoryStore(_BaseSyncADKMemoryStore["PostgresSyncConfig"]):
    """Synchronous PostgreSQL ADK memory store supporting pgvector `<=>`, HNSW, BM25, and `tsvector` FTS."""

    __slots__ = ("_enable_bm25", "_enable_hnsw", "_json_deserializer", "_json_serializer", "_vector_dim_override")

    def __init__(self, config: PostgresSyncConfig) -> None:
        """Initialize synchronous PostgreSQL memory store settings."""
        super().__init__(config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        adk_cfg = _adk_settings(config)
        dims = adk_cfg.get("vector_dimensions")
        self._vector_dim_override: int | None = int(dims) if dims is not None else None
        self._enable_hnsw: bool = bool(adk_cfg.get("enable_hnsw", False))
        self._enable_bm25: bool = bool(adk_cfg.get("enable_bm25", False))

    def _memory_table_ddl(self) -> str:
        return _memory_table_ddl(
            self._memory_table,
            self._owner_id_column_ddl,
            vector_dimensions=self._vector_dim_override,
            enable_hnsw=self._enable_hnsw,
            enable_bm25=self._enable_bm25,
            use_fts=self._use_fts,
        )

    def _drop_memory_table_sql(self) -> list[str]:
        return [f"DROP TABLE IF EXISTS {pg_table_ref(self._memory_table)};"]

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

    def _build_insert_sql_and_params(self, entry: StoredMemory, owner_id: object | None) -> tuple[str, tuple[Any, ...]]:
        table_ref = pg_table_ref(self._memory_table)
        emb = entry.get("embedding")
        emb_bind: Any = (
            pg_vector_literal(emb)
            if (self._vector_dim_override and emb is not None)
            else ([float(x) for x in emb] if emb is not None else None)
        )
        emb_cast = "$9::vector" if self._vector_dim_override else "$9"
        if self._owner_id_column_name:
            owner_col = pg_table_ref(self._owner_id_column_name, "owner column")
            emb_cast_owner = "$10::vector" if self._vector_dim_override else "$10"
            sql = (
                f"INSERT INTO {table_ref} (id, session_id, app_name, user_id, scope, event_id, author, "
                f"{owner_col}, timestamp, embedding, content_json, content_text, metadata_json, inserted_at) "
                f"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, {emb_cast_owner}, $11::jsonb, $12, $13::jsonb, $14) "
                "ON CONFLICT (event_id) DO NOTHING"
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
                pg_json_dumps(entry["content_json"], serializer=self._json_serializer),
                entry["content_text"],
                pg_json_dumps(entry.get("metadata_json"), serializer=self._json_serializer)
                if entry.get("metadata_json") is not None
                else None,
                pg_ensure_utc(entry["inserted_at"]),
            )
            return sql, params
        sql = (
            f"INSERT INTO {table_ref} (id, session_id, app_name, user_id, scope, event_id, author, "
            "timestamp, embedding, content_json, content_text, metadata_json, inserted_at) "
            f"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, {emb_cast}, $10::jsonb, $11, $12::jsonb, $13) "
            "ON CONFLICT (event_id) DO NOTHING"
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
            pg_json_dumps(entry["content_json"], serializer=self._json_serializer),
            entry["content_text"],
            pg_json_dumps(entry.get("metadata_json"), serializer=self._json_serializer)
            if entry.get("metadata_json") is not None
            else None,
            pg_ensure_utc(entry["inserted_at"]),
        )
        return sql, params

    def insert_memory_entries(self, entries: list[StoredMemory], owner_id: object | None = None) -> int:
        """Bulk-insert memory entries with `ON CONFLICT (event_id) DO NOTHING` deduplication."""
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
                inserted += int(res.rows_affected or 0)
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
        """Search memory entries via pgvector cosine distance, BM25, `tsvector` FTS, or ILIKE."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        has_query = bool(query and query.strip())
        if not has_query and embedding is None:
            return []
        effective_limit = limit if limit is not None else self._max_results
        if effective_limit <= 0:
            return []
        table_ref = pg_table_ref(self._memory_table)
        where_scope, scope_params = _build_memory_scope_where(app_name, user_id, scope_filter)
        try:
            with self._config.provide_session() as driver:
                if embedding is not None:
                    if self._vector_dim_override:
                        vec_idx = len(scope_params) + 1
                        lim_idx = len(scope_params) + 2
                        sql = (
                            f"SELECT id, session_id, app_name, user_id, scope, event_id, author, "
                            f"timestamp, content_json, content_text, metadata_json, inserted_at, embedding "
                            f"FROM {table_ref} WHERE {where_scope} AND embedding IS NOT NULL "
                            f"ORDER BY embedding <=> ${vec_idx}::vector ASC, timestamp DESC LIMIT ${lim_idx}"
                        )
                        rows = driver.select(sql, *scope_params, pg_vector_literal(embedding), effective_limit)
                        return [_row_to_stored_memory(r, deserializer=self._json_deserializer) for r in rows]
                    sql = (
                        f"SELECT id, session_id, app_name, user_id, scope, event_id, author, "
                        f"timestamp, content_json, content_text, metadata_json, inserted_at, embedding "
                        f"FROM {table_ref} WHERE {where_scope} AND embedding IS NOT NULL ORDER BY timestamp DESC"
                    )
                    rows = driver.select(sql, *scope_params)
                    records = [_row_to_stored_memory(r, deserializer=self._json_deserializer) for r in rows]
                    scored = sorted(
                        records,
                        key=lambda rec: _cosine_similarity(embedding, rec.get("embedding") or ()),
                        reverse=True,
                    )
                    return scored[:effective_limit]
                if self._use_fts:
                    q_idx = len(scope_params) + 1
                    lim_idx = len(scope_params) + 2
                    sql = (
                        f"SELECT id, session_id, app_name, user_id, scope, event_id, author, "
                        f"timestamp, content_json, content_text, metadata_json, inserted_at, embedding "
                        f"FROM {table_ref} WHERE {where_scope} "
                        f"AND to_tsvector('english', content_text) @@ plainto_tsquery('english', ${q_idx}) "
                        f"ORDER BY ts_rank(to_tsvector('english', content_text), plainto_tsquery('english', ${q_idx})) DESC, "
                        f"timestamp DESC LIMIT ${lim_idx}"
                    )
                    rows = driver.select(sql, *scope_params, query, effective_limit)
                    return [_row_to_stored_memory(r, deserializer=self._json_deserializer) for r in rows]
                pat_idx = len(scope_params) + 1
                lim_idx = len(scope_params) + 2
                sql = (
                    f"SELECT id, session_id, app_name, user_id, scope, event_id, author, "
                    f"timestamp, content_json, content_text, metadata_json, inserted_at, embedding "
                    f"FROM {table_ref} WHERE {where_scope} AND content_text ILIKE ${pat_idx} "
                    f"ORDER BY timestamp DESC LIMIT ${lim_idx}"
                )
                rows = driver.select(sql, *scope_params, f"%{query}%", effective_limit)
                return [_row_to_stored_memory(r, deserializer=self._json_deserializer) for r in rows]
        except Exception as exc:
            if is_pg_table_missing(exc):
                return []
            raise

    def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for `session_id`."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        table_ref = pg_table_ref(self._memory_table)
        with self._config.provide_session() as driver:
            res = driver.execute(f"DELETE FROM {table_ref} WHERE session_id = $1", session_id)
            driver.commit()
            return int(res.rows_affected or 0)

    def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = "all") -> int:
        """Delete memory entries older than `days` days filtered optionally by app and scope."""
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
        with self._config.provide_session() as driver:
            res = driver.execute(f"DELETE FROM {table_ref} WHERE {' AND '.join(clauses)}", *params)
            driver.commit()
            return int(res.rows_affected or 0)


class PostgresAsyncADKMemoryStore(_BaseAsyncADKMemoryStore["PostgresAsyncConfig"]):
    """Asynchronous PostgreSQL ADK memory store supporting pgvector `<=>`, HNSW, BM25, and `tsvector` FTS."""

    __slots__ = ("_enable_bm25", "_enable_hnsw", "_json_deserializer", "_json_serializer", "_vector_dim_override")

    def __init__(self, config: PostgresAsyncConfig) -> None:
        """Initialize asynchronous PostgreSQL memory store settings."""
        super().__init__(config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        adk_cfg = _adk_settings(config)
        dims = adk_cfg.get("vector_dimensions")
        self._vector_dim_override: int | None = int(dims) if dims is not None else None
        self._enable_hnsw: bool = bool(adk_cfg.get("enable_hnsw", False))
        self._enable_bm25: bool = bool(adk_cfg.get("enable_bm25", False))

    async def _memory_table_ddl(self) -> str:
        return _memory_table_ddl(
            self._memory_table,
            self._owner_id_column_ddl,
            vector_dimensions=self._vector_dim_override,
            enable_hnsw=self._enable_hnsw,
            enable_bm25=self._enable_bm25,
            use_fts=self._use_fts,
        )

    def _drop_memory_table_sql(self) -> list[str]:
        return [f"DROP TABLE IF EXISTS {pg_table_ref(self._memory_table)};"]

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
        """Asynchronously bulk-insert memory entries with `event_id` deduplication."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0
        helper = PostgresSyncADKMemoryStore(cast("PostgresSyncConfig", self._config))
        inserted = 0
        async with self._config.provide_session() as driver:
            for entry in entries:
                sql, params = helper._build_insert_sql_and_params(entry, owner_id)
                res = await driver.execute(sql, *params)
                inserted += int(res.rows_affected or 0)
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
        """Asynchronously search memory entries via pgvector cosine distance, FTS, or ILIKE."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        has_query = bool(query and query.strip())
        if not has_query and embedding is None:
            return []
        effective_limit = limit if limit is not None else self._max_results
        if effective_limit <= 0:
            return []
        table_ref = pg_table_ref(self._memory_table)
        where_scope, scope_params = _build_memory_scope_where(app_name, user_id, scope_filter)
        try:
            async with self._config.provide_session() as driver:
                if embedding is not None:
                    if self._vector_dim_override:
                        vec_idx = len(scope_params) + 1
                        lim_idx = len(scope_params) + 2
                        sql = (
                            f"SELECT id, session_id, app_name, user_id, scope, event_id, author, "
                            f"timestamp, content_json, content_text, metadata_json, inserted_at, embedding "
                            f"FROM {table_ref} WHERE {where_scope} AND embedding IS NOT NULL "
                            f"ORDER BY embedding <=> ${vec_idx}::vector ASC, timestamp DESC LIMIT ${lim_idx}"
                        )
                        rows = await driver.select(sql, *scope_params, pg_vector_literal(embedding), effective_limit)
                        return [_row_to_stored_memory(r, deserializer=self._json_deserializer) for r in rows]
                    sql = (
                        f"SELECT id, session_id, app_name, user_id, scope, event_id, author, "
                        f"timestamp, content_json, content_text, metadata_json, inserted_at, embedding "
                        f"FROM {table_ref} WHERE {where_scope} AND embedding IS NOT NULL ORDER BY timestamp DESC"
                    )
                    rows = await driver.select(sql, *scope_params)
                    records = [_row_to_stored_memory(r, deserializer=self._json_deserializer) for r in rows]
                    scored = sorted(
                        records,
                        key=lambda rec: _cosine_similarity(embedding, rec.get("embedding") or ()),
                        reverse=True,
                    )
                    return scored[:effective_limit]
                if self._use_fts:
                    q_idx = len(scope_params) + 1
                    lim_idx = len(scope_params) + 2
                    sql = (
                        f"SELECT id, session_id, app_name, user_id, scope, event_id, author, "
                        f"timestamp, content_json, content_text, metadata_json, inserted_at, embedding "
                        f"FROM {table_ref} WHERE {where_scope} "
                        f"AND to_tsvector('english', content_text) @@ plainto_tsquery('english', ${q_idx}) "
                        f"ORDER BY ts_rank(to_tsvector('english', content_text), plainto_tsquery('english', ${q_idx})) DESC, "
                        f"timestamp DESC LIMIT ${lim_idx}"
                    )
                    rows = await driver.select(sql, *scope_params, query, effective_limit)
                    return [_row_to_stored_memory(r, deserializer=self._json_deserializer) for r in rows]
                pat_idx = len(scope_params) + 1
                lim_idx = len(scope_params) + 2
                sql = (
                    f"SELECT id, session_id, app_name, user_id, scope, event_id, author, "
                    f"timestamp, content_json, content_text, metadata_json, inserted_at, embedding "
                    f"FROM {table_ref} WHERE {where_scope} AND content_text ILIKE ${pat_idx} "
                    f"ORDER BY timestamp DESC LIMIT ${lim_idx}"
                )
                rows = await driver.select(sql, *scope_params, f"%{query}%", effective_limit)
                return [_row_to_stored_memory(r, deserializer=self._json_deserializer) for r in rows]
        except Exception as exc:
            if is_pg_table_missing(exc):
                return []
            raise

    async def delete_entries_by_session(self, session_id: str) -> int:
        """Asynchronously delete all memory entries for `session_id`."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        table_ref = pg_table_ref(self._memory_table)
        async with self._config.provide_session() as driver:
            res = await driver.execute(f"DELETE FROM {table_ref} WHERE session_id = $1", session_id)
            await driver.commit()
            return int(res.rows_affected or 0)

    async def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = "all") -> int:
        """Asynchronously delete memory entries older than `days` days."""
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
        async with self._config.provide_session() as driver:
            res = await driver.execute(f"DELETE FROM {table_ref} WHERE {' AND '.join(clauses)}", *params)
            await driver.commit()
            return int(res.rows_affected or 0)


class PostgresSyncADKArtifactStore(_BaseSyncADKArtifactStore["PostgresSyncConfig"]):
    """Synchronous PostgreSQL ADK artifact metadata store returning `ArtifactReference`."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    def __init__(self, config: PostgresSyncConfig) -> None:
        """Initialize the synchronous PostgreSQL ADK artifact store."""
        super().__init__(config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    def create_table(self) -> None:
        """Create the ADK artifact metadata table in PostgreSQL."""
        with self._config.provide_session() as driver:
            driver.execute_script(_artifact_table_ddl(self._artifact_table))
            driver.commit()

    def insert_artifact(self, record: StoredArtifact) -> None:
        """Upsert an ADK artifact metadata entry using `$1..$N` binds."""
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
        with self._config.provide_session() as driver:
            driver.execute(
                sql,
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
        """Persist a `vertebra.storage.ArtifactReference` record into the artifact metadata table."""
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
        with self._config.provide_session() as driver:
            driver.execute(
                sql,
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
        table_ref = pg_table_ref(self._artifact_table)
        sess_val = session_id or ""
        try:
            with self._config.provide_session() as driver:
                if version is not None:
                    row = driver.select_one_or_none(
                        f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                        f"custom_metadata, created_at FROM {table_ref} "
                        "WHERE app_name = $1 AND user_id = $2 AND session_id = $3 AND filename = $4 AND version = $5",
                        app_name,
                        user_id,
                        sess_val,
                        filename,
                        version,
                    )
                else:
                    row = driver.select_one_or_none(
                        f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                        f"custom_metadata, created_at FROM {table_ref} "
                        "WHERE app_name = $1 AND user_id = $2 AND session_id = $3 AND filename = $4 "
                        "ORDER BY version DESC LIMIT 1",
                        app_name,
                        user_id,
                        sess_val,
                        filename,
                    )
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
        """Retrieve stored object metadata as a `vertebra.storage.ArtifactReference`."""
        table_ref = pg_table_ref(self._artifact_table)
        sess_val = session_id or ""
        try:
            with self._config.provide_session() as driver:
                if version is not None:
                    row = driver.select_one_or_none(
                        f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                        f"custom_metadata, size, updated, signed_url, created_at FROM {table_ref} "
                        "WHERE app_name = $1 AND user_id = $2 AND session_id = $3 AND filename = $4 AND version = $5",
                        app_name,
                        user_id,
                        sess_val,
                        filename,
                        version,
                    )
                else:
                    row = driver.select_one_or_none(
                        f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                        f"custom_metadata, size, updated, signed_url, created_at FROM {table_ref} "
                        "WHERE app_name = $1 AND user_id = $2 AND session_id = $3 AND filename = $4 "
                        "ORDER BY version DESC LIMIT 1",
                        app_name,
                        user_id,
                        sess_val,
                        filename,
                    )
            return _row_to_artifact_reference(row) if row is not None else None
        except Exception as exc:
            if is_pg_table_missing(exc):
                return None
            raise

    def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """Return distinct sorted filenames stored under an app/user/session scope."""
        table_ref = pg_table_ref(self._artifact_table)
        try:
            with self._config.provide_session() as driver:
                rows = driver.select(
                    f"SELECT DISTINCT filename FROM {table_ref} "
                    "WHERE app_name = $1 AND user_id = $2 AND session_id = $3 ORDER BY filename ASC",
                    app_name,
                    user_id,
                    session_id or "",
                )
            return [str(pg_row_value(r, "filename", 0)) for r in rows]
        except Exception as exc:
            if is_pg_table_missing(exc):
                return []
            raise

    def list_artifact_versions(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None
    ) -> list[StoredArtifact]:
        """Return ascending `StoredArtifact` versions for a filename."""
        table_ref = pg_table_ref(self._artifact_table)
        try:
            with self._config.provide_session() as driver:
                rows = driver.select(
                    f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                    f"custom_metadata, created_at FROM {table_ref} "
                    "WHERE app_name = $1 AND user_id = $2 AND session_id = $3 AND filename = $4 ORDER BY version ASC",
                    app_name,
                    user_id,
                    session_id or "",
                    filename,
                )
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
        table_ref = pg_table_ref(self._artifact_table)
        with self._config.provide_session() as driver:
            driver.execute(
                f"DELETE FROM {table_ref} WHERE app_name = $1 AND user_id = $2 AND session_id = $3 AND filename = $4",
                app_name,
                user_id,
                session_id or "",
                filename,
            )
            driver.commit()
        return existing

    def get_next_version(self, app_name: str, user_id: str, filename: str, session_id: str | None = None) -> int:
        """Return the next sequential artifact version (`MAX(version) + 1` or `0`)."""
        table_ref = pg_table_ref(self._artifact_table)
        with self._config.provide_session() as driver:
            val = driver.select_value_or_none(
                f"SELECT MAX(version) FROM {table_ref} "
                "WHERE app_name = $1 AND user_id = $2 AND session_id = $3 AND filename = $4",
                app_name,
                user_id,
                session_id or "",
                filename,
            )
        return 0 if val is None else int(val) + 1

    def delete_artifacts_older_than(self, before: datetime, app_name: str | None = None) -> list[StoredArtifact]:
        """Delete artifact rows older than `before` and return the removed records."""
        table_ref = pg_table_ref(self._artifact_table)
        cutoff = pg_ensure_utc(before)
        with self._config.provide_session() as driver:
            if app_name is not None:
                rows = driver.select(
                    f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                    f"custom_metadata, created_at FROM {table_ref} "
                    "WHERE created_at < $1 AND app_name = $2 ORDER BY created_at ASC",
                    cutoff,
                    app_name,
                )
                driver.execute(f"DELETE FROM {table_ref} WHERE created_at < $1 AND app_name = $2", cutoff, app_name)
            else:
                rows = driver.select(
                    f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                    f"custom_metadata, created_at FROM {table_ref} "
                    "WHERE created_at < $1 ORDER BY created_at ASC",
                    cutoff,
                )
                driver.execute(f"DELETE FROM {table_ref} WHERE created_at < $1", cutoff)
            driver.commit()
        return [_row_to_stored_artifact(r, deserializer=self._json_deserializer) for r in rows]


class PostgresAsyncADKArtifactStore(_BaseAsyncADKArtifactStore["PostgresAsyncConfig"]):
    """Asynchronous PostgreSQL ADK artifact metadata store returning `ArtifactReference`."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    def __init__(self, config: PostgresAsyncConfig) -> None:
        """Initialize the asynchronous PostgreSQL ADK artifact store."""
        super().__init__(config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    async def create_table(self) -> None:
        """Asynchronously create the ADK artifact metadata table."""
        async with self._config.provide_session() as driver:
            await driver.execute_script(_artifact_table_ddl(self._artifact_table))
            await driver.commit()

    async def insert_artifact(self, record: StoredArtifact) -> None:
        """Asynchronously upsert an ADK artifact metadata record."""
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
        async with self._config.provide_session() as driver:
            await driver.execute(
                sql,
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
        """Asynchronously store a `vertebra.storage.ArtifactReference` record."""
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
        async with self._config.provide_session() as driver:
            await driver.execute(
                sql,
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
        table_ref = pg_table_ref(self._artifact_table)
        sess_val = session_id or ""
        try:
            async with self._config.provide_session() as driver:
                if version is not None:
                    row = await driver.select_one_or_none(
                        f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                        f"custom_metadata, created_at FROM {table_ref} "
                        "WHERE app_name = $1 AND user_id = $2 AND session_id = $3 AND filename = $4 AND version = $5",
                        app_name,
                        user_id,
                        sess_val,
                        filename,
                        version,
                    )
                else:
                    row = await driver.select_one_or_none(
                        f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                        f"custom_metadata, created_at FROM {table_ref} "
                        "WHERE app_name = $1 AND user_id = $2 AND session_id = $3 AND filename = $4 "
                        "ORDER BY version DESC LIMIT 1",
                        app_name,
                        user_id,
                        sess_val,
                        filename,
                    )
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
        """Asynchronously retrieve stored object metadata as `ArtifactReference`."""
        table_ref = pg_table_ref(self._artifact_table)
        sess_val = session_id or ""
        try:
            async with self._config.provide_session() as driver:
                if version is not None:
                    row = await driver.select_one_or_none(
                        f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                        f"custom_metadata, size, updated, signed_url, created_at FROM {table_ref} "
                        "WHERE app_name = $1 AND user_id = $2 AND session_id = $3 AND filename = $4 AND version = $5",
                        app_name,
                        user_id,
                        sess_val,
                        filename,
                        version,
                    )
                else:
                    row = await driver.select_one_or_none(
                        f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                        f"custom_metadata, size, updated, signed_url, created_at FROM {table_ref} "
                        "WHERE app_name = $1 AND user_id = $2 AND session_id = $3 AND filename = $4 "
                        "ORDER BY version DESC LIMIT 1",
                        app_name,
                        user_id,
                        sess_val,
                        filename,
                    )
            return _row_to_artifact_reference(row) if row is not None else None
        except Exception as exc:
            if is_pg_table_missing(exc):
                return None
            raise

    async def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """Asynchronously list distinct artifact filenames."""
        table_ref = pg_table_ref(self._artifact_table)
        try:
            async with self._config.provide_session() as driver:
                rows = await driver.select(
                    f"SELECT DISTINCT filename FROM {table_ref} "
                    "WHERE app_name = $1 AND user_id = $2 AND session_id = $3 ORDER BY filename ASC",
                    app_name,
                    user_id,
                    session_id or "",
                )
            return [str(pg_row_value(r, "filename", 0)) for r in rows]
        except Exception as exc:
            if is_pg_table_missing(exc):
                return []
            raise

    async def list_artifact_versions(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None
    ) -> list[StoredArtifact]:
        """Asynchronously list stored `StoredArtifact` versions for an artifact."""
        table_ref = pg_table_ref(self._artifact_table)
        try:
            async with self._config.provide_session() as driver:
                rows = await driver.select(
                    f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                    f"custom_metadata, created_at FROM {table_ref} "
                    "WHERE app_name = $1 AND user_id = $2 AND session_id = $3 AND filename = $4 ORDER BY version ASC",
                    app_name,
                    user_id,
                    session_id or "",
                    filename,
                )
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
        table_ref = pg_table_ref(self._artifact_table)
        async with self._config.provide_session() as driver:
            await driver.execute(
                f"DELETE FROM {table_ref} WHERE app_name = $1 AND user_id = $2 AND session_id = $3 AND filename = $4",
                app_name,
                user_id,
                session_id or "",
                filename,
            )
            await driver.commit()
        return existing

    async def get_next_version(self, app_name: str, user_id: str, filename: str, session_id: str | None = None) -> int:
        """Asynchronously compute the next sequential artifact version."""
        table_ref = pg_table_ref(self._artifact_table)
        async with self._config.provide_session() as driver:
            val = await driver.select_value_or_none(
                f"SELECT MAX(version) FROM {table_ref} "
                "WHERE app_name = $1 AND user_id = $2 AND session_id = $3 AND filename = $4",
                app_name,
                user_id,
                session_id or "",
                filename,
            )
        return 0 if val is None else int(val) + 1

    async def delete_artifacts_older_than(self, before: datetime, app_name: str | None = None) -> list[StoredArtifact]:
        """Asynchronously delete artifact metadata rows older than `before` and return removed records."""
        table_ref = pg_table_ref(self._artifact_table)
        cutoff = pg_ensure_utc(before)
        async with self._config.provide_session() as driver:
            if app_name is not None:
                rows = await driver.select(
                    f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                    f"custom_metadata, created_at FROM {table_ref} "
                    "WHERE created_at < $1 AND app_name = $2 ORDER BY created_at ASC",
                    cutoff,
                    app_name,
                )
                await driver.execute(
                    f"DELETE FROM {table_ref} WHERE created_at < $1 AND app_name = $2", cutoff, app_name
                )
            else:
                rows = await driver.select(
                    f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                    f"custom_metadata, created_at FROM {table_ref} "
                    "WHERE created_at < $1 ORDER BY created_at ASC",
                    cutoff,
                )
                await driver.execute(f"DELETE FROM {table_ref} WHERE created_at < $1", cutoff)
            await driver.commit()
        return [_row_to_stored_artifact(r, deserializer=self._json_deserializer) for r in rows]
