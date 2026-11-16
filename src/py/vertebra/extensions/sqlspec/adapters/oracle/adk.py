"""Oracle ADK session, 23ai vector/FTS memory, and artifact stores without requiring `google-adk`."""

from __future__ import annotations

import array
import logging
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Final, Literal, cast

from sqlspec.extensions.adk import BaseAsyncADKStore, BaseSyncADKStore, StoredEvent, StoredSession
from sqlspec.extensions.adk.store import normalize_session_list_options

from vertebra.extensions.sqlspec.adapters.oracle.core import (
    escape_oracle_literal,
    is_oracle_table_missing,
    is_oracle_unique_violation,
    oracle_ensure_utc,
    oracle_index_name,
    oracle_json_column_ddl,
    oracle_json_dumps,
    oracle_json_loads,
    oracle_resolve_json_storage_mode,
    oracle_row_value,
    oracle_table_feature_clause,
    oracle_table_ref,
    sanitize_oracle_text_query,
)
from vertebra.storage import ArtifactReference

if TYPE_CHECKING:
    from sqlspec.extensions.adk import StoredMemory
    from sqlspec.extensions.adk.artifact._types import StoredArtifact
    from sqlspec.extensions.adk.artifact.store import BaseAsyncADKArtifactStore as _BaseAsyncADKArtifactStore
    from sqlspec.extensions.adk.artifact.store import BaseSyncADKArtifactStore as _BaseSyncADKArtifactStore
    from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore as _BaseAsyncADKMemoryStore
    from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore as _BaseSyncADKMemoryStore

    from vertebra.extensions.sqlspec.adapters.oracle.config import OracleAsyncConfig, OracleSyncConfig
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
                """Initialize ADK memory table settings from configuration."""
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
                        driver.execute_script(statement)
                    driver.commit()

            def ensure_tables(self) -> None:
                """Create tables when auto-creation is enabled."""
                if not self._enabled:
                    return
                if self.create_schema_enabled:
                    self.create_tables()

            def _drop_memory_table_sql(self) -> list[str]:
                raise NotImplementedError

            def create_tables(self) -> None:
                """Create memory tables."""
                raise NotImplementedError

        class _BaseAsyncADKMemoryStore(_FallbackADKMemoryStoreCommon):
            """Fallback asynchronous memory store base when `google-adk` is absent."""

            async def drop_tables(self) -> None:
                """Execute drop statements sequentially inside an async session."""
                statements = self._drop_memory_table_sql()
                async with self._config.provide_session() as driver:
                    for statement in statements:
                        await driver.execute_script(statement)
                    await driver.commit()

            async def ensure_tables(self) -> None:
                """Create tables when auto-creation is enabled."""
                if not self._enabled:
                    return
                if self.create_schema_enabled:
                    await self.create_tables()

            def _drop_memory_table_sql(self) -> list[str]:
                raise NotImplementedError

            async def create_tables(self) -> None:
                """Create memory tables asynchronously."""
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
                """Initialize ADK artifact metadata store settings."""
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
    "OracleAsyncADKArtifactStore",
    "OracleAsyncADKMemoryStore",
    "OracleAsyncADKStore",
    "OracleSyncADKArtifactStore",
    "OracleSyncADKMemoryStore",
    "OracleSyncADKStore",
)

logger = logging.getLogger("vertebra.extensions.sqlspec.adapters.oracle.adk")


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


def _plsql_ignore_exists(stmt: str) -> str:
    """Wrap an Oracle DDL statement to ignore `ORA-00955` name-already-used."""
    escaped = escape_oracle_literal(stmt)
    return (
        "BEGIN\n"
        f"    EXECUTE IMMEDIATE '{escaped}';\n"
        "EXCEPTION\n"
        "    WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF;\n"
        "END;"
    )


def _plsql_ignore_missing(stmt: str, *, code: int = -942) -> str:
    """Wrap an Oracle DROP statement to ignore `ORA-00942` (`-942`) or `ORA-01418` (`-1418`)."""
    escaped = escape_oracle_literal(stmt)
    return (
        "BEGIN\n"
        f"    EXECUTE IMMEDIATE '{escaped}';\n"
        "EXCEPTION\n"
        f"    WHEN OTHERS THEN IF SQLCODE != {code} THEN RAISE; END IF;\n"
        "END;"
    )


def _detect_storage_mode_sync(config: Any, override: str | None) -> str:
    """Resolve Oracle JSON storage mode synchronously using pool version cache or data dictionary."""
    if override is not None:
        return oracle_resolve_json_storage_mode(None, override=override)
    cache = getattr(config, "_oracle_version_cache", None)
    if cache is not None and cache.resolved:
        return oracle_resolve_json_storage_mode(cache.version, compatible_major=cache.compatible_major)
    try:
        with config.provide_session() as driver:
            version_info = driver.data_dictionary.get_version(driver)
        if cache is not None:
            cache.set(version_info)
        return oracle_resolve_json_storage_mode(version_info)
    except Exception:
        return oracle_resolve_json_storage_mode(None)


async def _detect_storage_mode_async(config: Any, override: str | None) -> str:
    """Resolve Oracle JSON storage mode asynchronously using pool version cache or data dictionary."""
    if override is not None:
        return oracle_resolve_json_storage_mode(None, override=override)
    cache = getattr(config, "_oracle_version_cache", None)
    if cache is not None and cache.resolved:
        return oracle_resolve_json_storage_mode(cache.version, compatible_major=cache.compatible_major)
    try:
        async with config.provide_session() as driver:
            version_info = await driver.data_dictionary.get_version(driver)
        if cache is not None:
            cache.set(version_info)
        return oracle_resolve_json_storage_mode(version_info)
    except Exception:
        return oracle_resolve_json_storage_mode(None)


def _sessions_ddl(session_table: str, owner_id_ddl: str | None, storage_mode: str, adk_cfg: Mapping[str, Any]) -> str:
    """Build PL/SQL idempotent DDL for Oracle ADK sessions table."""
    table_ref = oracle_table_ref(session_table)
    owner_col = f"{owner_id_ddl}, " if owner_id_ddl else ""
    state_col = oracle_json_column_ddl("state", storage_mode, not_null=True)
    features = oracle_table_feature_clause(adk_cfg)
    idx_app = oracle_index_name("idx", session_table, "app_usr")
    idx_upd = oracle_index_name("idx", session_table, "upd")
    return "\n".join(
        [
            _plsql_ignore_exists(
                f"CREATE TABLE {table_ref} ("
                "id VARCHAR2(128) PRIMARY KEY, "
                "app_name VARCHAR2(128) NOT NULL, "
                "user_id VARCHAR2(128) NOT NULL, "
                f"{owner_col}"
                f"{state_col}, "
                "create_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL, "
                "update_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL"
                f"){features}"
            ),
            _plsql_ignore_exists(f"CREATE INDEX {idx_app} ON {table_ref} (app_name, user_id)"),
            _plsql_ignore_exists(f"CREATE INDEX {idx_upd} ON {table_ref} (update_time DESC)"),
        ]
    )


def _events_ddl(events_table: str, session_table: str, storage_mode: str, adk_cfg: Mapping[str, Any]) -> str:
    """Build PL/SQL idempotent DDL for Oracle ADK events table."""
    events_ref = oracle_table_ref(events_table)
    sessions_ref = oracle_table_ref(session_table)
    event_col = oracle_json_column_ddl("event_data", storage_mode, not_null=True)
    fk_name = oracle_index_name("fk", events_table, "sess")
    idx_sess = oracle_index_name("idx", events_table, "sess")
    idx_inv = oracle_index_name("idx", events_table, "inv")
    idx_ts = oracle_index_name("idx", events_table, "ts")
    features = oracle_table_feature_clause(adk_cfg)
    return "\n".join(
        [
            _plsql_ignore_exists(
                f"CREATE TABLE {events_ref} ("
                "id VARCHAR2(128) PRIMARY KEY, "
                "app_name VARCHAR2(128) NOT NULL, "
                "user_id VARCHAR2(128) NOT NULL, "
                "session_id VARCHAR2(128) NOT NULL, "
                "invocation_id VARCHAR2(256), "
                "timestamp TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL, "
                f"{event_col}, "
                f"CONSTRAINT {fk_name} FOREIGN KEY (session_id) REFERENCES {sessions_ref}(id) ON DELETE CASCADE"
                f"){features}"
            ),
            _plsql_ignore_exists(f"CREATE INDEX {idx_sess} ON {events_ref} (session_id, timestamp ASC)"),
            _plsql_ignore_exists(f"CREATE INDEX {idx_inv} ON {events_ref} (invocation_id)"),
            _plsql_ignore_exists(f"CREATE INDEX {idx_ts} ON {events_ref} (app_name, timestamp ASC)"),
        ]
    )


def _app_states_ddl(app_state_table: str, storage_mode: str, adk_cfg: Mapping[str, Any]) -> str:
    """Build PL/SQL idempotent DDL for Oracle app states table."""
    table_ref = oracle_table_ref(app_state_table)
    state_col = oracle_json_column_ddl("state", storage_mode, not_null=True)
    features = oracle_table_feature_clause(adk_cfg)
    return _plsql_ignore_exists(
        f"CREATE TABLE {table_ref} ("
        "app_name VARCHAR2(128) PRIMARY KEY, "
        f"{state_col}, "
        "update_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL"
        f"){features}"
    )


def _user_states_ddl(user_state_table: str, storage_mode: str, adk_cfg: Mapping[str, Any]) -> str:
    """Build PL/SQL idempotent DDL for Oracle user states table."""
    table_ref = oracle_table_ref(user_state_table)
    state_col = oracle_json_column_ddl("state", storage_mode, not_null=True)
    features = oracle_table_feature_clause(adk_cfg)
    return _plsql_ignore_exists(
        f"CREATE TABLE {table_ref} ("
        "app_name VARCHAR2(128) NOT NULL, "
        "user_id VARCHAR2(128) NOT NULL, "
        f"{state_col}, "
        "update_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL, "
        "PRIMARY KEY (app_name, user_id)"
        f"){features}"
    )


def _metadata_ddl(metadata_table: str) -> str:
    """Build PL/SQL idempotent DDL for Oracle ADK metadata table."""
    table_ref = oracle_table_ref(metadata_table)
    return _plsql_ignore_exists(
        f"CREATE TABLE {table_ref} ("
        "key VARCHAR2(255) PRIMARY KEY, "
        "value CLOB NOT NULL, "
        "update_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL"
        ")"
    )


def _row_to_stored_session(row: Any, *, deserializer: Any = None) -> StoredSession:
    """Decode an Oracle session row into a `StoredSession` dictionary."""
    raw_state = oracle_row_value(row, "state", 3)
    decoded = oracle_json_loads(raw_state, deserializer=deserializer) if raw_state is not None else {}
    return StoredSession(
        id=str(oracle_row_value(row, "id", 0)),
        app_name=str(oracle_row_value(row, "app_name", 1)),
        user_id=str(oracle_row_value(row, "user_id", 2)),
        state=decoded if isinstance(decoded, dict) else {},
        create_time=oracle_ensure_utc(oracle_row_value(row, "create_time", 4)),
        update_time=oracle_ensure_utc(oracle_row_value(row, "update_time", 5)),
    )


def _row_to_stored_event(row: Any, *, deserializer: Any = None) -> StoredEvent:
    """Decode an Oracle event row into a `StoredEvent` dictionary."""
    raw_data = oracle_row_value(row, "event_data", 6)
    decoded = oracle_json_loads(raw_data, deserializer=deserializer) if raw_data is not None else {}
    return StoredEvent(
        id=str(oracle_row_value(row, "id", 0)),
        session_id=str(oracle_row_value(row, "session_id", 1)),
        app_name=str(oracle_row_value(row, "app_name", 2)),
        user_id=str(oracle_row_value(row, "user_id", 3)),
        invocation_id=oracle_row_value(row, "invocation_id", 4),
        timestamp=oracle_ensure_utc(oracle_row_value(row, "timestamp", 5)),
        event_data=decoded if isinstance(decoded, dict) else {},
    )


def _memory_table_ddl(
    memory_table: str,
    owner_id_ddl: str | None,
    storage_mode: str,
    *,
    vector_dimensions: int | None,
    use_fts: bool,
) -> str:
    """Build PL/SQL idempotent DDL for Oracle ADK memory entries including optional 23ai `VECTOR` column."""
    table_ref = oracle_table_ref(memory_table)
    owner_col = f"{owner_id_ddl}, " if owner_id_ddl else ""
    emb_col = f"embedding VECTOR({int(vector_dimensions)}, FLOAT32), " if vector_dimensions else ""
    content_col = oracle_json_column_ddl("content_json", storage_mode, not_null=True)
    metadata_col = oracle_json_column_ddl("metadata_json", storage_mode, not_null=False)
    stmts = [
        _plsql_ignore_exists(
            f"CREATE TABLE {table_ref} ("
            "id VARCHAR2(128) PRIMARY KEY, "
            "session_id VARCHAR2(128) NOT NULL, "
            "app_name VARCHAR2(128) NOT NULL, "
            "user_id VARCHAR2(128) NOT NULL, "
            "scope VARCHAR2(16) DEFAULT 'user' NOT NULL, "
            "event_id VARCHAR2(128) NOT NULL UNIQUE, "
            "author VARCHAR2(128), "
            f"{owner_col}"
            "timestamp TIMESTAMP WITH TIME ZONE NOT NULL, "
            f"{emb_col}"
            f"{content_col}, "
            "content_text CLOB NOT NULL, "
            f"{metadata_col}, "
            "inserted_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL"
            ")"
        ),
        _plsql_ignore_exists(
            f"CREATE INDEX {oracle_index_name('idx', memory_table, 'sess')} ON {table_ref} (session_id)"
        ),
        _plsql_ignore_exists(
            f"CREATE INDEX {oracle_index_name('idx', memory_table, 'scope')} ON {table_ref} (app_name, scope, user_id, timestamp DESC)"
        ),
    ]
    if use_fts:
        stmts.append(
            _plsql_ignore_exists(
                f"CREATE INDEX {oracle_index_name('idx', memory_table, 'fts')} "
                f"ON {table_ref} (content_text) INDEXTYPE IS CTXSYS.CONTEXT"
            )
        )
    return "\n".join(stmts)


def _build_memory_scope_where(
    app_name: str, user_id: str, scope_filter: Literal["all", "user", "app"]
) -> tuple[str, dict[str, Any]]:
    """Build named-bind Oracle scope predicates for user/app/combined memory queries."""
    if scope_filter == "user":
        return "app_name = :app_name AND scope = 'user' AND user_id = :user_id", {
            "app_name": app_name,
            "user_id": user_id,
        }
    if scope_filter == "app":
        return "app_name = :app_name AND scope = 'app'", {"app_name": app_name}
    return "app_name = :app_name AND ((scope = 'user' AND user_id = :user_id) OR scope = 'app')", {
        "app_name": app_name,
        "user_id": user_id,
    }


def _cosine_similarity(left: Sequence[float], right: Sequence[float]) -> float:
    """Compute cosine similarity between two equal-length dense float sequences."""
    if not left or len(left) != len(right):
        return 0.0
    dot = sum(float(a) * float(b) for a, b in zip(left, right, strict=True))
    norm_l = math.sqrt(sum(float(a) * float(a) for a in left))
    norm_r = math.sqrt(sum(float(b) * float(b) for b in right))
    return 0.0 if norm_l == 0.0 or norm_r == 0.0 else dot / (norm_l * norm_r)


def _row_to_stored_memory(row: Any, *, deserializer: Any = None) -> StoredMemory:
    """Decode an Oracle memory row into a `StoredMemory` dictionary."""
    raw_content = oracle_row_value(row, "content_json", 8)
    raw_meta = oracle_row_value(row, "metadata_json", 10)
    raw_emb = oracle_row_value(row, "embedding", 12)
    embedding = [float(x) for x in raw_emb] if raw_emb is not None else None
    return cast(
        "StoredMemory",
        {
            "id": str(oracle_row_value(row, "id", 0)),
            "session_id": str(oracle_row_value(row, "session_id", 1)),
            "app_name": str(oracle_row_value(row, "app_name", 2)),
            "user_id": str(oracle_row_value(row, "user_id", 3)),
            "scope": str(oracle_row_value(row, "scope", 4)),
            "event_id": str(oracle_row_value(row, "event_id", 5)),
            "author": oracle_row_value(row, "author", 6),
            "timestamp": oracle_ensure_utc(oracle_row_value(row, "timestamp", 7)),
            "content_json": oracle_json_loads(raw_content, deserializer=deserializer) or {},
            "content_text": str(oracle_row_value(row, "content_text", 9) or ""),
            "metadata_json": oracle_json_loads(raw_meta, deserializer=deserializer) if raw_meta is not None else None,
            "inserted_at": oracle_ensure_utc(oracle_row_value(row, "inserted_at", 11)),
            "embedding": embedding,
        },
    )


def _artifact_table_ddl(artifact_table: str, storage_mode: str) -> str:
    """Build PL/SQL idempotent DDL for Oracle ADK artifacts and object references."""
    table_ref = oracle_table_ref(artifact_table)
    meta_col = oracle_json_column_ddl("custom_metadata", storage_mode, not_null=False)
    return _plsql_ignore_exists(
        f"CREATE TABLE {table_ref} ("
        "app_name VARCHAR2(128) NOT NULL, "
        "user_id VARCHAR2(128) NOT NULL, "
        "session_id VARCHAR2(128) DEFAULT '' NOT NULL, "
        "filename VARCHAR2(512) NOT NULL, "
        "version NUMBER(10) NOT NULL, "
        "mime_type VARCHAR2(255), "
        "canonical_uri VARCHAR2(4000) NOT NULL, "
        f"{meta_col}, "
        "size NUMBER(19), "
        "updated TIMESTAMP WITH TIME ZONE, "
        "signed_url VARCHAR2(4000), "
        "created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL, "
        "PRIMARY KEY (app_name, user_id, session_id, filename, version)"
        ")"
    )


def _row_to_stored_artifact(row: Any, *, deserializer: Any = None) -> StoredArtifact:
    """Decode an Oracle artifact row into a `StoredArtifact` dictionary."""
    sess_raw = oracle_row_value(row, "session_id", 2)
    mime_raw = oracle_row_value(row, "mime_type", 5)
    meta_raw = oracle_row_value(row, "custom_metadata", 7)
    return cast(
        "StoredArtifact",
        {
            "app_name": str(oracle_row_value(row, "app_name", 0)),
            "user_id": str(oracle_row_value(row, "user_id", 1)),
            "session_id": str(sess_raw) if sess_raw else None,
            "filename": str(oracle_row_value(row, "filename", 3)),
            "version": int(oracle_row_value(row, "version", 4)),
            "mime_type": str(mime_raw) if mime_raw is not None else None,
            "canonical_uri": str(oracle_row_value(row, "canonical_uri", 6)),
            "custom_metadata": oracle_json_loads(meta_raw, deserializer=deserializer) if meta_raw is not None else None,
            "created_at": oracle_ensure_utc(oracle_row_value(row, "created_at", 8)),
        },
    )


def _row_to_artifact_reference(row: Any) -> ArtifactReference:
    """Decode an Oracle artifact row into a `vertebra.storage.ArtifactReference`."""
    mime_raw = oracle_row_value(row, "mime_type", 5)
    size_raw = oracle_row_value(row, "size", 8)
    updated_raw = oracle_row_value(row, "updated", 9)
    signed_raw = oracle_row_value(row, "signed_url", 10)
    return ArtifactReference(
        uri=str(oracle_row_value(row, "canonical_uri", 6)),
        version=int(oracle_row_value(row, "version", 4)),
        content_type=str(mime_raw) if mime_raw is not None else None,
        size=int(size_raw) if size_raw is not None else None,
        updated=oracle_ensure_utc(updated_raw) if updated_raw is not None else None,
        signed_url=str(signed_raw) if signed_raw is not None else None,
    )


class OracleSyncADKStore(BaseSyncADKStore["OracleSyncConfig"]):
    """Synchronous Oracle ADK session and event store using `MERGE INTO` and PL/SQL guards."""

    __slots__ = ("_json_deserializer", "_json_serializer", "_json_storage_override")

    def __init__(self, config: OracleSyncConfig) -> None:
        """Initialize the synchronous Oracle ADK session store."""
        super().__init__(config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        self._json_storage_override: str | None = _adk_settings(config).get("json_storage")

    def _storage_mode(self) -> str:
        return _detect_storage_mode_sync(self._config, self._json_storage_override)

    def _sessions_table_ddl(self) -> str:
        return _sessions_ddl(
            self._session_table, self._owner_id_column_ddl, self._storage_mode(), _adk_settings(self._config)
        )

    def _events_table_ddl(self) -> str:
        return _events_ddl(self._events_table, self._session_table, self._storage_mode(), _adk_settings(self._config))

    def _app_states_table_ddl(self) -> str:
        return _app_states_ddl(self._app_state_table, self._storage_mode(), _adk_settings(self._config))

    def _user_states_table_ddl(self) -> str:
        return _user_states_ddl(self._user_state_table, self._storage_mode(), _adk_settings(self._config))

    def _metadata_table_ddl(self) -> str:
        return _metadata_ddl(self._metadata_table)

    def _drop_app_states_table_sql(self) -> str:
        return _plsql_ignore_missing(f"DROP TABLE {oracle_table_ref(self._app_state_table)}")

    def _drop_user_states_table_sql(self) -> str:
        return _plsql_ignore_missing(f"DROP TABLE {oracle_table_ref(self._user_state_table)}")

    def _drop_metadata_table_sql(self) -> str:
        return _plsql_ignore_missing(f"DROP TABLE {oracle_table_ref(self._metadata_table)}")

    def _drop_tables_sql(self) -> list[str]:
        return [
            self._drop_metadata_table_sql(),
            self._drop_user_states_table_sql(),
            self._drop_app_states_table_sql(),
            _plsql_ignore_missing(f"DROP TABLE {oracle_table_ref(self._events_table)}"),
            _plsql_ignore_missing(f"DROP TABLE {oracle_table_ref(self._session_table)}"),
        ]

    def create_tables(self) -> None:
        """Create ADK session, event, app state, user state, and metadata tables idempotently."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return
        mode = self._storage_mode()
        adk_cfg = _adk_settings(self._config)
        with self._config.provide_session() as driver:
            driver.execute_script(_sessions_ddl(self._session_table, self._owner_id_column_ddl, mode, adk_cfg))
            driver.execute_script(_events_ddl(self._events_table, self._session_table, mode, adk_cfg))
            driver.execute_script(_app_states_ddl(self._app_state_table, mode, adk_cfg))
            driver.execute_script(_user_states_ddl(self._user_state_table, mode, adk_cfg))
            driver.execute_script(_metadata_ddl(self._metadata_table))
            driver.commit()

    def create_session(
        self, session_id: str, app_name: str, user_id: str, state: dict[str, Any], owner_id: Any | None = None
    ) -> StoredSession:
        """Insert a new Oracle ADK session row and return `StoredSession`."""
        table_ref = oracle_table_ref(self._session_table)
        app_updates, user_updates, session_state = _split_state_scopes(state)
        state_json = oracle_json_dumps(session_state, serializer=self._json_serializer)
        with self._config.provide_session() as driver:
            if app_updates:
                self._upsert_app_state_with_driver(driver, app_name, app_updates)
            if user_updates:
                self._upsert_user_state_with_driver(driver, app_name, user_id, user_updates)
            if self._owner_id_column_name:
                owner_col = oracle_table_ref(self._owner_id_column_name, "owner column")
                driver.execute(
                    f"INSERT INTO {table_ref} (id, app_name, user_id, {owner_col}, state, create_time, update_time) "
                    "VALUES (:id, :app_name, :user_id, :owner_id, :state, SYSTIMESTAMP, SYSTIMESTAMP)",
                    {
                        "id": session_id,
                        "app_name": app_name,
                        "user_id": user_id,
                        "owner_id": owner_id,
                        "state": state_json,
                    },
                )
            else:
                driver.execute(
                    f"INSERT INTO {table_ref} (id, app_name, user_id, state, create_time, update_time) "
                    "VALUES (:id, :app_name, :user_id, :state, SYSTIMESTAMP, SYSTIMESTAMP)",
                    {"id": session_id, "app_name": app_name, "user_id": user_id, "state": state_json},
                )
            driver.commit()
        created = self.get_session(app_name, user_id, session_id)
        if created is None:
            msg = f"Failed to reload newly created ADK session {session_id!r}"
            raise RuntimeError(msg)
        return created

    def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: int | timedelta | None = None
    ) -> StoredSession | None:
        """Fetch an Oracle ADK session row by key, renewing `update_time` if requested."""
        table_ref = oracle_table_ref(self._session_table)
        should_touch = renew_for is not None and self._calculate_expires_at(renew_for) is not None
        try:
            with self._config.provide_session() as driver:
                if should_touch:
                    driver.execute(
                        f"UPDATE {table_ref} SET update_time = SYSTIMESTAMP "
                        "WHERE app_name = :app_name AND user_id = :user_id AND id = :id",
                        {"app_name": app_name, "user_id": user_id, "id": session_id},
                    )
                    driver.commit()
                row = driver.select_one_or_none(
                    f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table_ref} "
                    "WHERE app_name = :app_name AND user_id = :user_id AND id = :id",
                    {"app_name": app_name, "user_id": user_id, "id": session_id},
                )
                return _row_to_stored_session(row, deserializer=self._json_deserializer) if row is not None else None
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Update session state and scoped app/user state dicts inside a single transaction."""
        table_ref = oracle_table_ref(self._session_table)
        app_updates, user_updates, session_state = _split_state_scopes(state)
        state_json = oracle_json_dumps(session_state, serializer=self._json_serializer)
        with self._config.provide_session() as driver:
            if app_updates:
                self._upsert_app_state_with_driver(driver, app_name, app_updates)
            if user_updates:
                self._upsert_user_state_with_driver(driver, app_name, user_id, user_updates)
            driver.execute(
                f"UPDATE {table_ref} SET state = :state, update_time = SYSTIMESTAMP "
                "WHERE app_name = :app_name AND user_id = :user_id AND id = :id",
                {"state": state_json, "app_name": app_name, "user_id": user_id, "id": session_id},
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
        """List Oracle ADK sessions using `OFFSET` / `FETCH NEXT` pagination."""
        col, direction, norm_limit, norm_offset = normalize_session_list_options(order_by, descending, limit, offset)
        if norm_limit == 0:
            return []
        table_ref = oracle_table_ref(self._session_table)
        params: dict[str, Any] = {"app_name": app_name}
        where_sql = "app_name = :app_name"
        if user_id is not None:
            where_sql += " AND user_id = :user_id"
            params["user_id"] = user_id
        sql = f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table_ref} WHERE {where_sql} ORDER BY {col} {direction}, id {direction}"
        if norm_offset > 0:
            params["offset_val"] = norm_offset
            sql += " OFFSET :offset_val ROWS"
        if norm_limit is not None:
            params["limit_val"] = norm_limit
            sql += " FETCH NEXT :limit_val ROWS ONLY"
        try:
            with self._config.provide_session() as driver:
                rows = driver.select(sql, params)
            return [_row_to_stored_session(row, deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return []
            raise

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete an ADK session row by composite key."""
        table_ref = oracle_table_ref(self._session_table)
        with self._config.provide_session() as driver:
            driver.execute(
                f"DELETE FROM {table_ref} WHERE app_name = :app_name AND user_id = :user_id AND id = :id",
                {"app_name": app_name, "user_id": user_id, "id": session_id},
            )
            driver.commit()

    def _insert_event_with_driver(self, driver: Any, event_record: StoredEvent) -> None:
        events_ref = oracle_table_ref(self._events_table)
        event_json = oracle_json_dumps(event_record["event_data"], serializer=self._json_serializer)
        try:
            driver.execute(
                f"INSERT INTO {events_ref} (id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
                "VALUES (:id, :app_name, :user_id, :session_id, :invocation_id, :ts, :event_data)",
                {
                    "id": event_record["id"],
                    "app_name": event_record["app_name"],
                    "user_id": event_record["user_id"],
                    "session_id": event_record["session_id"],
                    "invocation_id": event_record.get("invocation_id"),
                    "ts": oracle_ensure_utc(event_record["timestamp"]),
                    "event_data": event_json,
                },
            )
        except Exception as exc:
            if is_oracle_unique_violation(exc):
                return
            raise

    def append_event(self, event_record: StoredEvent) -> None:
        """Append a single Oracle ADK event row safely skipping duplicate primary keys."""
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
        """Atomically append an event and update session/app/user state inside one Oracle transaction."""
        table_ref = oracle_table_ref(self._session_table)
        app_updates, user_updates, session_state = _split_state_scopes(state)
        if app_state:
            app_updates.update(app_state)
        if user_state:
            user_updates.update(user_state)
        state_json = oracle_json_dumps(session_state, serializer=self._json_serializer)
        with self._config.provide_session() as driver:
            self._insert_event_with_driver(driver, event_record)
            if app_updates:
                self._upsert_app_state_with_driver(driver, app_name, app_updates)
            if user_updates:
                self._upsert_user_state_with_driver(driver, app_name, user_id, user_updates)
            res = driver.execute(
                f"UPDATE {table_ref} SET state = :state, update_time = SYSTIMESTAMP "
                "WHERE app_name = :app_name AND user_id = :user_id AND id = :id",
                {"state": state_json, "app_name": app_name, "user_id": user_id, "id": session_id},
            )
            driver.commit()
            if (res.rows_affected or 0) == 0:
                msg = f"Session {session_id!r} not found for app={app_name!r}, user={user_id!r}"
                raise RuntimeError(msg)
        updated = self.get_session(app_name, user_id, session_id)
        if updated is None:
            msg = f"Session {session_id!r} not found after state update"
            raise RuntimeError(msg)
        return updated

    def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> list[StoredEvent]:
        """Fetch Oracle ADK events ordered ascending by timestamp."""
        events_ref = oracle_table_ref(self._events_table)
        params: dict[str, Any] = {"app_name": app_name, "user_id": user_id, "session_id": session_id}
        sql = (
            f"SELECT id, session_id, app_name, user_id, invocation_id, timestamp, event_data "
            f"FROM {events_ref} WHERE app_name = :app_name AND user_id = :user_id AND session_id = :session_id"
        )
        if after_timestamp is not None:
            params["after_ts"] = oracle_ensure_utc(after_timestamp)
            sql += " AND timestamp > :after_ts"
        sql += " ORDER BY timestamp ASC, id ASC"
        if limit is not None:
            params["limit_val"] = limit
            sql += " FETCH NEXT :limit_val ROWS ONLY"
        try:
            with self._config.provide_session() as driver:
                rows = driver.select(sql, params)
            return [_row_to_stored_event(row, deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return []
            raise

    def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Delete Oracle events older than `before`."""
        events_ref = oracle_table_ref(self._events_table)
        cutoff = oracle_ensure_utc(before)
        with self._config.provide_session() as driver:
            if app_name is not None:
                res = driver.execute(
                    f"DELETE FROM {events_ref} WHERE timestamp < :before_ts AND app_name = :app_name",
                    {"before_ts": cutoff, "app_name": app_name},
                )
            else:
                res = driver.execute(
                    f"DELETE FROM {events_ref} WHERE timestamp < :before_ts",
                    {"before_ts": cutoff},
                )
            driver.commit()
            return int(res.rows_affected or 0)

    def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete user states whose `update_time` predates `updated_before`."""
        table_ref = oracle_table_ref(self._user_state_table)
        cutoff = oracle_ensure_utc(updated_before)
        with self._config.provide_session() as driver:
            if app_name is not None:
                res = driver.execute(
                    f"DELETE FROM {table_ref} WHERE update_time < :cutoff AND app_name = :app_name",
                    {"cutoff": cutoff, "app_name": app_name},
                )
            else:
                res = driver.execute(f"DELETE FROM {table_ref} WHERE update_time < :cutoff", {"cutoff": cutoff})
            driver.commit()
            return int(res.rows_affected or 0)

    def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete sessions whose `update_time` predates `updated_before`."""
        table_ref = oracle_table_ref(self._session_table)
        cutoff = oracle_ensure_utc(updated_before)
        with self._config.provide_session() as driver:
            if app_name is not None:
                res = driver.execute(
                    f"DELETE FROM {table_ref} WHERE update_time < :cutoff AND app_name = :app_name",
                    {"cutoff": cutoff, "app_name": app_name},
                )
            else:
                res = driver.execute(f"DELETE FROM {table_ref} WHERE update_time < :cutoff", {"cutoff": cutoff})
            driver.commit()
            return int(res.rows_affected or 0)

    def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Fetch app-scoped ADK state from Oracle."""
        table_ref = oracle_table_ref(self._app_state_table)
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(
                    f"SELECT state FROM {table_ref} WHERE app_name = :app_name", {"app_name": app_name}
                )
            if row is None:
                return None
            decoded = oracle_json_loads(oracle_row_value(row, "state", 0), deserializer=self._json_deserializer)
            return decoded if isinstance(decoded, dict) else {}
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise

    def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Fetch user-scoped ADK state from Oracle."""
        table_ref = oracle_table_ref(self._user_state_table)
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(
                    f"SELECT state FROM {table_ref} WHERE app_name = :app_name AND user_id = :user_id",
                    {"app_name": app_name, "user_id": user_id},
                )
            if row is None:
                return None
            decoded = oracle_json_loads(oracle_row_value(row, "state", 0), deserializer=self._json_deserializer)
            return decoded if isinstance(decoded, dict) else {}
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise

    def _upsert_app_state_with_driver(self, driver: Any, app_name: str, state: dict[str, Any]) -> None:
        table_ref = oracle_table_ref(self._app_state_table)
        state_json = oracle_json_dumps(state, serializer=self._json_serializer)
        driver.execute(
            f"MERGE INTO {table_ref} target USING (SELECT :app_name AS app_name, :state AS state FROM DUAL) src "
            "ON (target.app_name = src.app_name) "
            "WHEN MATCHED THEN UPDATE SET target.state = src.state, target.update_time = SYSTIMESTAMP "
            "WHEN NOT MATCHED THEN INSERT (app_name, state, update_time) VALUES (src.app_name, src.state, SYSTIMESTAMP)",
            {"app_name": app_name, "state": state_json},
        )

    def _upsert_user_state_with_driver(self, driver: Any, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        table_ref = oracle_table_ref(self._user_state_table)
        state_json = oracle_json_dumps(state, serializer=self._json_serializer)
        driver.execute(
            f"MERGE INTO {table_ref} target "
            "USING (SELECT :app_name AS app_name, :user_id AS user_id, :state AS state FROM DUAL) src "
            "ON (target.app_name = src.app_name AND target.user_id = src.user_id) "
            "WHEN MATCHED THEN UPDATE SET target.state = src.state, target.update_time = SYSTIMESTAMP "
            "WHEN NOT MATCHED THEN INSERT (app_name, user_id, state, update_time) "
            "VALUES (src.app_name, src.user_id, src.state, SYSTIMESTAMP)",
            {"app_name": app_name, "user_id": user_id, "state": state_json},
        )

    def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Upsert app-scoped ADK state via `MERGE INTO`."""
        with self._config.provide_session() as driver:
            self._upsert_app_state_with_driver(driver, app_name, state)
            driver.commit()

    def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Upsert user-scoped ADK state via `MERGE INTO`."""
        with self._config.provide_session() as driver:
            self._upsert_user_state_with_driver(driver, app_name, user_id, state)
            driver.commit()

    def get_metadata(self, key: str) -> str | None:
        """Fetch an internal metadata entry by key."""
        table_ref = oracle_table_ref(self._metadata_table)
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(f"SELECT value FROM {table_ref} WHERE key = :key", {"key": key})
            return str(oracle_row_value(row, "value", 0)) if row is not None else None
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise

    def set_metadata(self, key: str, value: str) -> None:
        """Upsert an internal metadata entry via `MERGE INTO`."""
        table_ref = oracle_table_ref(self._metadata_table)
        with self._config.provide_session() as driver:
            driver.execute(
                f"MERGE INTO {table_ref} target USING (SELECT :key AS key, :value AS value FROM DUAL) src "
                "ON (target.key = src.key) "
                "WHEN MATCHED THEN UPDATE SET target.value = src.value, target.update_time = SYSTIMESTAMP "
                "WHEN NOT MATCHED THEN INSERT (key, value, update_time) VALUES (src.key, src.value, SYSTIMESTAMP)",
                {"key": key, "value": value},
            )
            driver.commit()


class OracleAsyncADKStore(BaseAsyncADKStore["OracleAsyncConfig"]):
    """Asynchronous Oracle ADK session and event store using `MERGE INTO` and PL/SQL guards."""

    __slots__ = ("_json_deserializer", "_json_serializer", "_json_storage_override")

    def __init__(self, config: OracleAsyncConfig) -> None:
        """Initialize the asynchronous Oracle ADK session store."""
        super().__init__(config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        self._json_storage_override: str | None = _adk_settings(config).get("json_storage")

    async def _storage_mode(self) -> str:
        return await _detect_storage_mode_async(self._config, self._json_storage_override)

    async def _sessions_table_ddl(self) -> str:
        return _sessions_ddl(
            self._session_table, self._owner_id_column_ddl, await self._storage_mode(), _adk_settings(self._config)
        )

    async def _events_table_ddl(self) -> str:
        return _events_ddl(
            self._events_table, self._session_table, await self._storage_mode(), _adk_settings(self._config)
        )

    async def _app_states_table_ddl(self) -> str:
        return _app_states_ddl(self._app_state_table, await self._storage_mode(), _adk_settings(self._config))

    async def _user_states_table_ddl(self) -> str:
        return _user_states_ddl(self._user_state_table, await self._storage_mode(), _adk_settings(self._config))

    async def _metadata_table_ddl(self) -> str:
        return _metadata_ddl(self._metadata_table)

    def _drop_app_states_table_sql(self) -> str:
        return _plsql_ignore_missing(f"DROP TABLE {oracle_table_ref(self._app_state_table)}")

    def _drop_user_states_table_sql(self) -> str:
        return _plsql_ignore_missing(f"DROP TABLE {oracle_table_ref(self._user_state_table)}")

    def _drop_metadata_table_sql(self) -> str:
        return _plsql_ignore_missing(f"DROP TABLE {oracle_table_ref(self._metadata_table)}")

    def _drop_tables_sql(self) -> list[str]:
        return [
            self._drop_metadata_table_sql(),
            self._drop_user_states_table_sql(),
            self._drop_app_states_table_sql(),
            _plsql_ignore_missing(f"DROP TABLE {oracle_table_ref(self._events_table)}"),
            _plsql_ignore_missing(f"DROP TABLE {oracle_table_ref(self._session_table)}"),
        ]

    async def create_tables(self) -> None:
        """Asynchronously create Oracle ADK tables with detected storage type."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        mode = await self._storage_mode()
        adk_cfg = _adk_settings(self._config)
        async with self._config.provide_session() as driver:
            await driver.execute_script(_sessions_ddl(self._session_table, self._owner_id_column_ddl, mode, adk_cfg))
            await driver.execute_script(_events_ddl(self._events_table, self._session_table, mode, adk_cfg))
            await driver.execute_script(_app_states_ddl(self._app_state_table, mode, adk_cfg))
            await driver.execute_script(_user_states_ddl(self._user_state_table, mode, adk_cfg))
            await driver.execute_script(_metadata_ddl(self._metadata_table))
            await driver.commit()

    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: dict[str, Any], owner_id: Any | None = None
    ) -> StoredSession:
        """Asynchronously insert a new Oracle ADK session row."""
        table_ref = oracle_table_ref(self._session_table)
        app_updates, user_updates, session_state = _split_state_scopes(state)
        state_json = oracle_json_dumps(session_state, serializer=self._json_serializer)
        async with self._config.provide_session() as driver:
            if app_updates:
                await self._upsert_app_state_with_driver(driver, app_name, app_updates)
            if user_updates:
                await self._upsert_user_state_with_driver(driver, app_name, user_id, user_updates)
            if self._owner_id_column_name:
                owner_col = oracle_table_ref(self._owner_id_column_name, "owner column")
                await driver.execute(
                    f"INSERT INTO {table_ref} (id, app_name, user_id, {owner_col}, state, create_time, update_time) "
                    "VALUES (:id, :app_name, :user_id, :owner_id, :state, SYSTIMESTAMP, SYSTIMESTAMP)",
                    {
                        "id": session_id,
                        "app_name": app_name,
                        "user_id": user_id,
                        "owner_id": owner_id,
                        "state": state_json,
                    },
                )
            else:
                await driver.execute(
                    f"INSERT INTO {table_ref} (id, app_name, user_id, state, create_time, update_time) "
                    "VALUES (:id, :app_name, :user_id, :state, SYSTIMESTAMP, SYSTIMESTAMP)",
                    {"id": session_id, "app_name": app_name, "user_id": user_id, "state": state_json},
                )
            await driver.commit()
        created = await self.get_session(app_name, user_id, session_id)
        if created is None:
            msg = f"Failed to reload newly created ADK session {session_id!r}"
            raise RuntimeError(msg)
        return created

    async def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: int | timedelta | None = None
    ) -> StoredSession | None:
        """Asynchronously fetch an Oracle ADK session row."""
        table_ref = oracle_table_ref(self._session_table)
        should_touch = renew_for is not None and self._calculate_expires_at(renew_for) is not None
        try:
            async with self._config.provide_session() as driver:
                if should_touch:
                    await driver.execute(
                        f"UPDATE {table_ref} SET update_time = SYSTIMESTAMP "
                        "WHERE app_name = :app_name AND user_id = :user_id AND id = :id",
                        {"app_name": app_name, "user_id": user_id, "id": session_id},
                    )
                    await driver.commit()
                row = await driver.select_one_or_none(
                    f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table_ref} "
                    "WHERE app_name = :app_name AND user_id = :user_id AND id = :id",
                    {"app_name": app_name, "user_id": user_id, "id": session_id},
                )
                return _row_to_stored_session(row, deserializer=self._json_deserializer) if row is not None else None
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Asynchronously update Oracle ADK session state and scoped state rows."""
        table_ref = oracle_table_ref(self._session_table)
        app_updates, user_updates, session_state = _split_state_scopes(state)
        state_json = oracle_json_dumps(session_state, serializer=self._json_serializer)
        async with self._config.provide_session() as driver:
            if app_updates:
                await self._upsert_app_state_with_driver(driver, app_name, app_updates)
            if user_updates:
                await self._upsert_user_state_with_driver(driver, app_name, user_id, user_updates)
            await driver.execute(
                f"UPDATE {table_ref} SET state = :state, update_time = SYSTIMESTAMP "
                "WHERE app_name = :app_name AND user_id = :user_id AND id = :id",
                {"state": state_json, "app_name": app_name, "user_id": user_id, "id": session_id},
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
        """Asynchronously list Oracle ADK sessions."""
        col, direction, norm_limit, norm_offset = normalize_session_list_options(order_by, descending, limit, offset)
        if norm_limit == 0:
            return []
        table_ref = oracle_table_ref(self._session_table)
        params: dict[str, Any] = {"app_name": app_name}
        where_sql = "app_name = :app_name"
        if user_id is not None:
            where_sql += " AND user_id = :user_id"
            params["user_id"] = user_id
        sql = f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table_ref} WHERE {where_sql} ORDER BY {col} {direction}, id {direction}"
        if norm_offset > 0:
            params["offset_val"] = norm_offset
            sql += " OFFSET :offset_val ROWS"
        if norm_limit is not None:
            params["limit_val"] = norm_limit
            sql += " FETCH NEXT :limit_val ROWS ONLY"
        try:
            async with self._config.provide_session() as driver:
                rows = await driver.select(sql, params)
            return [_row_to_stored_session(row, deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return []
            raise

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Asynchronously delete an ADK session row."""
        table_ref = oracle_table_ref(self._session_table)
        async with self._config.provide_session() as driver:
            await driver.execute(
                f"DELETE FROM {table_ref} WHERE app_name = :app_name AND user_id = :user_id AND id = :id",
                {"app_name": app_name, "user_id": user_id, "id": session_id},
            )
            await driver.commit()

    async def _insert_event_with_driver(self, driver: Any, event_record: StoredEvent) -> None:
        events_ref = oracle_table_ref(self._events_table)
        event_json = oracle_json_dumps(event_record["event_data"], serializer=self._json_serializer)
        try:
            await driver.execute(
                f"INSERT INTO {events_ref} (id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
                "VALUES (:id, :app_name, :user_id, :session_id, :invocation_id, :ts, :event_data)",
                {
                    "id": event_record["id"],
                    "app_name": event_record["app_name"],
                    "user_id": event_record["user_id"],
                    "session_id": event_record["session_id"],
                    "invocation_id": event_record.get("invocation_id"),
                    "ts": oracle_ensure_utc(event_record["timestamp"]),
                    "event_data": event_json,
                },
            )
        except Exception as exc:
            if is_oracle_unique_violation(exc):
                return
            raise

    async def append_event(self, event_record: StoredEvent) -> None:
        """Asynchronously append an Oracle ADK event row."""
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
        """Atomically append an event and update state inside one async Oracle transaction."""
        table_ref = oracle_table_ref(self._session_table)
        app_updates, user_updates, session_state = _split_state_scopes(state)
        if app_state:
            app_updates.update(app_state)
        if user_state:
            user_updates.update(user_state)
        state_json = oracle_json_dumps(session_state, serializer=self._json_serializer)
        async with self._config.provide_session() as driver:
            await self._insert_event_with_driver(driver, event_record)
            if app_updates:
                await self._upsert_app_state_with_driver(driver, app_name, app_updates)
            if user_updates:
                await self._upsert_user_state_with_driver(driver, app_name, user_id, user_updates)
            res = await driver.execute(
                f"UPDATE {table_ref} SET state = :state, update_time = SYSTIMESTAMP "
                "WHERE app_name = :app_name AND user_id = :user_id AND id = :id",
                {"state": state_json, "app_name": app_name, "user_id": user_id, "id": session_id},
            )
            await driver.commit()
            if (res.rows_affected or 0) == 0:
                msg = f"Session {session_id!r} not found for app={app_name!r}, user={user_id!r}"
                raise RuntimeError(msg)
        updated = await self.get_session(app_name, user_id, session_id)
        if updated is None:
            msg = f"Session {session_id!r} not found after state update"
            raise RuntimeError(msg)
        return updated

    async def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> list[StoredEvent]:
        """Asynchronously retrieve Oracle ADK events."""
        events_ref = oracle_table_ref(self._events_table)
        params: dict[str, Any] = {"app_name": app_name, "user_id": user_id, "session_id": session_id}
        sql = (
            f"SELECT id, session_id, app_name, user_id, invocation_id, timestamp, event_data "
            f"FROM {events_ref} WHERE app_name = :app_name AND user_id = :user_id AND session_id = :session_id"
        )
        if after_timestamp is not None:
            params["after_ts"] = oracle_ensure_utc(after_timestamp)
            sql += " AND timestamp > :after_ts"
        sql += " ORDER BY timestamp ASC, id ASC"
        if limit is not None:
            params["limit_val"] = limit
            sql += " FETCH NEXT :limit_val ROWS ONLY"
        try:
            async with self._config.provide_session() as driver:
                rows = await driver.select(sql, params)
            return [_row_to_stored_event(row, deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return []
            raise

    async def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Asynchronously delete events older than `before`."""
        events_ref = oracle_table_ref(self._events_table)
        cutoff = oracle_ensure_utc(before)
        async with self._config.provide_session() as driver:
            if app_name is not None:
                res = await driver.execute(
                    f"DELETE FROM {events_ref} WHERE timestamp < :before_ts AND app_name = :app_name",
                    {"before_ts": cutoff, "app_name": app_name},
                )
            else:
                res = await driver.execute(
                    f"DELETE FROM {events_ref} WHERE timestamp < :before_ts",
                    {"before_ts": cutoff},
                )
            await driver.commit()
            return int(res.rows_affected or 0)

    async def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Asynchronously delete idle user states."""
        table_ref = oracle_table_ref(self._user_state_table)
        cutoff = oracle_ensure_utc(updated_before)
        async with self._config.provide_session() as driver:
            if app_name is not None:
                res = await driver.execute(
                    f"DELETE FROM {table_ref} WHERE update_time < :cutoff AND app_name = :app_name",
                    {"cutoff": cutoff, "app_name": app_name},
                )
            else:
                res = await driver.execute(f"DELETE FROM {table_ref} WHERE update_time < :cutoff", {"cutoff": cutoff})
            await driver.commit()
            return int(res.rows_affected or 0)

    async def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Asynchronously delete idle sessions."""
        table_ref = oracle_table_ref(self._session_table)
        cutoff = oracle_ensure_utc(updated_before)
        async with self._config.provide_session() as driver:
            if app_name is not None:
                res = await driver.execute(
                    f"DELETE FROM {table_ref} WHERE update_time < :cutoff AND app_name = :app_name",
                    {"cutoff": cutoff, "app_name": app_name},
                )
            else:
                res = await driver.execute(f"DELETE FROM {table_ref} WHERE update_time < :cutoff", {"cutoff": cutoff})
            await driver.commit()
            return int(res.rows_affected or 0)

    async def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Asynchronously fetch app state."""
        table_ref = oracle_table_ref(self._app_state_table)
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(
                    f"SELECT state FROM {table_ref} WHERE app_name = :app_name", {"app_name": app_name}
                )
            if row is None:
                return None
            decoded = oracle_json_loads(oracle_row_value(row, "state", 0), deserializer=self._json_deserializer)
            return decoded if isinstance(decoded, dict) else {}
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise

    async def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Asynchronously fetch user state."""
        table_ref = oracle_table_ref(self._user_state_table)
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(
                    f"SELECT state FROM {table_ref} WHERE app_name = :app_name AND user_id = :user_id",
                    {"app_name": app_name, "user_id": user_id},
                )
            if row is None:
                return None
            decoded = oracle_json_loads(oracle_row_value(row, "state", 0), deserializer=self._json_deserializer)
            return decoded if isinstance(decoded, dict) else {}
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise

    async def _upsert_app_state_with_driver(self, driver: Any, app_name: str, state: dict[str, Any]) -> None:
        table_ref = oracle_table_ref(self._app_state_table)
        state_json = oracle_json_dumps(state, serializer=self._json_serializer)
        await driver.execute(
            f"MERGE INTO {table_ref} target USING (SELECT :app_name AS app_name, :state AS state FROM DUAL) src "
            "ON (target.app_name = src.app_name) "
            "WHEN MATCHED THEN UPDATE SET target.state = src.state, target.update_time = SYSTIMESTAMP "
            "WHEN NOT MATCHED THEN INSERT (app_name, state, update_time) VALUES (src.app_name, src.state, SYSTIMESTAMP)",
            {"app_name": app_name, "state": state_json},
        )

    async def _upsert_user_state_with_driver(
        self, driver: Any, app_name: str, user_id: str, state: dict[str, Any]
    ) -> None:
        table_ref = oracle_table_ref(self._user_state_table)
        state_json = oracle_json_dumps(state, serializer=self._json_serializer)
        await driver.execute(
            f"MERGE INTO {table_ref} target "
            "USING (SELECT :app_name AS app_name, :user_id AS user_id, :state AS state FROM DUAL) src "
            "ON (target.app_name = src.app_name AND target.user_id = src.user_id) "
            "WHEN MATCHED THEN UPDATE SET target.state = src.state, target.update_time = SYSTIMESTAMP "
            "WHEN NOT MATCHED THEN INSERT (app_name, user_id, state, update_time) "
            "VALUES (src.app_name, src.user_id, src.state, SYSTIMESTAMP)",
            {"app_name": app_name, "user_id": user_id, "state": state_json},
        )

    async def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Asynchronously upsert app state via `MERGE INTO`."""
        async with self._config.provide_session() as driver:
            await self._upsert_app_state_with_driver(driver, app_name, state)
            await driver.commit()

    async def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Asynchronously upsert user state via `MERGE INTO`."""
        async with self._config.provide_session() as driver:
            await self._upsert_user_state_with_driver(driver, app_name, user_id, state)
            await driver.commit()

    async def get_metadata(self, key: str) -> str | None:
        """Asynchronously fetch an internal metadata value."""
        table_ref = oracle_table_ref(self._metadata_table)
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(f"SELECT value FROM {table_ref} WHERE key = :key", {"key": key})
            return str(oracle_row_value(row, "value", 0)) if row is not None else None
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise

    async def set_metadata(self, key: str, value: str) -> None:
        """Asynchronously upsert an internal metadata value."""
        table_ref = oracle_table_ref(self._metadata_table)
        async with self._config.provide_session() as driver:
            await driver.execute(
                f"MERGE INTO {table_ref} target USING (SELECT :key AS key, :value AS value FROM DUAL) src "
                "ON (target.key = src.key) "
                "WHEN MATCHED THEN UPDATE SET target.value = src.value, target.update_time = SYSTIMESTAMP "
                "WHEN NOT MATCHED THEN INSERT (key, value, update_time) VALUES (src.key, src.value, SYSTIMESTAMP)",
                {"key": key, "value": value},
            )
            await driver.commit()


class OracleSyncADKMemoryStore(_BaseSyncADKMemoryStore["OracleSyncConfig"]):
    """Synchronous Oracle ADK memory store with 23ai `VECTOR_DISTANCE`, RRF fusion, and Oracle Text FTS."""

    __slots__ = (
        "_fts_operator",
        "_json_deserializer",
        "_json_serializer",
        "_json_storage_override",
        "_vector_dim_override",
    )

    def __init__(self, config: OracleSyncConfig) -> None:
        """Initialize the synchronous Oracle ADK memory store."""
        super().__init__(config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        adk_cfg = _adk_settings(config)
        dims = adk_cfg.get("vector_dimensions")
        self._vector_dim_override: int | None = int(dims) if dims is not None else None
        self._json_storage_override: str | None = adk_cfg.get("json_storage")
        self._fts_operator: str = str(adk_cfg.get("fts_operator", "ACCUM"))

    def _storage_mode(self) -> str:
        return _detect_storage_mode_sync(self._config, self._json_storage_override)

    def _memory_table_ddl(self) -> str:
        return _memory_table_ddl(
            self._memory_table,
            self._owner_id_column_ddl,
            self._storage_mode(),
            vector_dimensions=self._vector_dim_override,
            use_fts=self._use_fts,
        )

    def _drop_memory_table_sql(self) -> list[str]:
        stmts = [
            _plsql_ignore_missing(f"DROP INDEX {oracle_index_name('idx', self._memory_table, 'sess')}", code=-1418),
            _plsql_ignore_missing(f"DROP INDEX {oracle_index_name('idx', self._memory_table, 'scope')}", code=-1418),
        ]
        if self._use_fts:
            stmts.append(
                _plsql_ignore_missing(f"DROP INDEX {oracle_index_name('idx', self._memory_table, 'fts')}", code=-1418)
            )
        stmts.append(_plsql_ignore_missing(f"DROP TABLE {oracle_table_ref(self._memory_table)}"))
        return stmts

    def create_tables(self) -> None:
        """Create Oracle ADK memory table and indexes idempotently."""
        if not self._enabled:
            return
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return
        with self._config.provide_session() as driver:
            driver.execute_script(self._memory_table_ddl())
            driver.commit()

    def _build_insert_params(self, entry: StoredMemory, owner_id: object | None) -> tuple[str, dict[str, Any]]:
        table_ref = oracle_table_ref(self._memory_table)
        emb_col = ", embedding" if self._vector_dim_override else ""
        emb_bind = ", :embedding" if self._vector_dim_override else ""
        owner_col = (
            f", {oracle_table_ref(self._owner_id_column_name, 'owner column')}" if self._owner_id_column_name else ""
        )
        owner_bind = ", :owner_id" if self._owner_id_column_name else ""
        sql = (
            f"INSERT INTO {table_ref} "
            f"(id, session_id, app_name, user_id, scope, event_id, author{owner_col}, "
            f"timestamp{emb_col}, content_json, content_text, metadata_json, inserted_at) "
            f"VALUES (:id, :session_id, :app_name, :user_id, :scope, :event_id, :author{owner_bind}, "
            f":ts{emb_bind}, :content_json, :content_text, :metadata_json, :inserted_at)"
        )
        params: dict[str, Any] = {
            "id": entry["id"],
            "session_id": entry["session_id"],
            "app_name": entry["app_name"],
            "user_id": entry["user_id"],
            "scope": entry.get("scope", "user"),
            "event_id": entry["event_id"],
            "author": entry.get("author"),
            "ts": oracle_ensure_utc(entry["timestamp"]),
            "content_json": oracle_json_dumps(entry["content_json"], serializer=self._json_serializer),
            "content_text": entry["content_text"],
            "metadata_json": oracle_json_dumps(entry.get("metadata_json"), serializer=self._json_serializer)
            if entry.get("metadata_json") is not None
            else None,
            "inserted_at": oracle_ensure_utc(entry["inserted_at"]),
        }
        if self._vector_dim_override:
            emb_val = entry.get("embedding")
            params["embedding"] = array.array("f", [float(x) for x in emb_val]) if emb_val is not None else None
        if self._owner_id_column_name:
            params["owner_id"] = owner_id
        return sql, params

    def insert_memory_entries(self, entries: list[StoredMemory], owner_id: object | None = None) -> int:
        """Bulk-insert memory entries ignoring `ORA-00001` duplicate `event_id` violations."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0
        inserted = 0
        with self._config.provide_session() as driver:
            for entry in entries:
                sql, params = self._build_insert_params(entry, owner_id)
                try:
                    res = driver.execute(sql, params)
                    inserted += int(res.rows_affected or 1)
                except Exception as exc:
                    if is_oracle_unique_violation(exc):
                        continue
                    raise
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
        """Search Oracle memory entries using 23ai `VECTOR_DISTANCE`, RRF, Oracle Text FTS, or `LIKE`."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        effective_limit = limit if limit is not None else self._max_results
        if effective_limit <= 0:
            return []
        table_ref = oracle_table_ref(self._memory_table)
        where_scope, scope_params = _build_memory_scope_where(app_name, user_id, scope_filter)
        emb_col = "embedding" if self._vector_dim_override else "NULL AS embedding"
        try:
            with self._config.provide_session() as driver:
                if embedding is not None and self._vector_dim_override:
                    vec_bind = array.array("f", [float(x) for x in embedding])
                    if self._use_fts and query.strip():
                        fts_query = sanitize_oracle_text_query(query, operator=self._fts_operator)
                        sql = (
                            f"WITH vec AS ("
                            f"  SELECT id, ROW_NUMBER() OVER (ORDER BY VECTOR_DISTANCE(embedding, :query_vector, COSINE)) AS r_v "
                            f"  FROM {table_ref} WHERE {where_scope} AND embedding IS NOT NULL FETCH FIRST :top_k ROWS ONLY"
                            f"), txt AS ("
                            f"  SELECT id, ROW_NUMBER() OVER (ORDER BY SCORE(1) DESC) AS r_t "
                            f"  FROM {table_ref} WHERE {where_scope} AND CONTAINS(content_text, :text_query, 1) > 0 FETCH FIRST :top_k ROWS ONLY"
                            f") "
                            f"SELECT m.id, m.session_id, m.app_name, m.user_id, m.scope, m.event_id, m.author, "
                            f"m.timestamp, m.content_json, m.content_text, m.metadata_json, m.inserted_at, m.{emb_col} "
                            f"FROM {table_ref} m LEFT JOIN vec ON m.id = vec.id LEFT JOIN txt ON m.id = txt.id "
                            f"WHERE vec.id IS NOT NULL OR txt.id IS NOT NULL "
                            f"ORDER BY (NVL(1.0 / (60 + vec.r_v), 0) + NVL(1.0 / (60 + txt.r_t), 0)) DESC "
                            f"FETCH FIRST :top_k ROWS ONLY"
                        )
                        params = {
                            **scope_params,
                            "query_vector": vec_bind,
                            "text_query": fts_query,
                            "top_k": effective_limit,
                        }
                    else:
                        sql = (
                            f"SELECT id, session_id, app_name, user_id, scope, event_id, author, "
                            f"timestamp, content_json, content_text, metadata_json, inserted_at, {emb_col} "
                            f"FROM {table_ref} WHERE {where_scope} AND embedding IS NOT NULL "
                            f"ORDER BY VECTOR_DISTANCE(embedding, :query_vector, COSINE) ASC "
                            f"FETCH FIRST :top_k ROWS ONLY"
                        )
                        params = {**scope_params, "query_vector": vec_bind, "top_k": effective_limit}
                    rows = driver.select(sql, params)
                    return [_row_to_stored_memory(r, deserializer=self._json_deserializer) for r in rows]

                if self._use_fts:
                    fts_query = sanitize_oracle_text_query(query, operator=self._fts_operator)
                    if not fts_query:
                        return []
                    sql = (
                        f"SELECT id, session_id, app_name, user_id, scope, event_id, author, "
                        f"timestamp, content_json, content_text, metadata_json, inserted_at, {emb_col} "
                        f"FROM {table_ref} WHERE {where_scope} AND CONTAINS(content_text, :text_query, 1) > 0 "
                        f"ORDER BY SCORE(1) DESC, timestamp DESC FETCH FIRST :limit_val ROWS ONLY"
                    )
                    rows = driver.select(sql, {**scope_params, "text_query": fts_query, "limit_val": effective_limit})
                    return [_row_to_stored_memory(r, deserializer=self._json_deserializer) for r in rows]

                sql = (
                    f"SELECT id, session_id, app_name, user_id, scope, event_id, author, "
                    f"timestamp, content_json, content_text, metadata_json, inserted_at, {emb_col} "
                    f"FROM {table_ref} WHERE {where_scope} AND LOWER(content_text) LIKE :pattern "
                    f"ORDER BY timestamp DESC FETCH FIRST :limit_val ROWS ONLY"
                )
                rows = driver.select(
                    sql, {**scope_params, "pattern": f"%{query.lower()}%", "limit_val": effective_limit}
                )
                records = [_row_to_stored_memory(r, deserializer=self._json_deserializer) for r in rows]
                if embedding is not None:
                    return sorted(
                        records,
                        key=lambda rec: _cosine_similarity(embedding, rec.get("embedding") or ()),
                        reverse=True,
                    )[:effective_limit]
                return records
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return []
            raise

    def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all Oracle memory rows associated with `session_id`."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        table_ref = oracle_table_ref(self._memory_table)
        with self._config.provide_session() as driver:
            res = driver.execute(f"DELETE FROM {table_ref} WHERE session_id = :session_id", {"session_id": session_id})
            driver.commit()
            return int(res.rows_affected or 0)

    def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = "all") -> int:
        """Delete Oracle memory entries older than `days` days."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        table_ref = oracle_table_ref(self._memory_table)
        cutoff = datetime.now(UTC) - timedelta(days=days)
        params: dict[str, Any] = {"cutoff": cutoff}
        clauses = ["inserted_at < :cutoff"]
        if app_name is not None:
            params["app_name"] = app_name
            clauses.append("app_name = :app_name")
        if scope in {"user", "app"}:
            params["scope"] = scope
            clauses.append("scope = :scope")
        with self._config.provide_session() as driver:
            res = driver.execute(f"DELETE FROM {table_ref} WHERE {' AND '.join(clauses)}", params)
            driver.commit()
            return int(res.rows_affected or 0)


class OracleAsyncADKMemoryStore(_BaseAsyncADKMemoryStore["OracleAsyncConfig"]):
    """Asynchronous Oracle ADK memory store supporting 23ai vectors, RRF fusion, and Oracle Text."""

    __slots__ = (
        "_fts_operator",
        "_json_deserializer",
        "_json_serializer",
        "_json_storage_override",
        "_vector_dim_override",
    )

    def __init__(self, config: OracleAsyncConfig) -> None:
        """Initialize the asynchronous Oracle ADK memory store."""
        super().__init__(config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        adk_cfg = _adk_settings(config)
        dims = adk_cfg.get("vector_dimensions")
        self._vector_dim_override: int | None = int(dims) if dims is not None else None
        self._json_storage_override: str | None = adk_cfg.get("json_storage")
        self._fts_operator: str = str(adk_cfg.get("fts_operator", "ACCUM"))

    async def _memory_table_ddl(self) -> str:
        mode = await _detect_storage_mode_async(self._config, self._json_storage_override)
        return _memory_table_ddl(
            self._memory_table,
            self._owner_id_column_ddl,
            mode,
            vector_dimensions=self._vector_dim_override,
            use_fts=self._use_fts,
        )

    def _drop_memory_table_sql(self) -> list[str]:
        stmts = [
            _plsql_ignore_missing(f"DROP INDEX {oracle_index_name('idx', self._memory_table, 'sess')}", code=-1418),
            _plsql_ignore_missing(f"DROP INDEX {oracle_index_name('idx', self._memory_table, 'scope')}", code=-1418),
        ]
        if self._use_fts:
            stmts.append(
                _plsql_ignore_missing(f"DROP INDEX {oracle_index_name('idx', self._memory_table, 'fts')}", code=-1418)
            )
        stmts.append(_plsql_ignore_missing(f"DROP TABLE {oracle_table_ref(self._memory_table)}"))
        return stmts

    async def create_tables(self) -> None:
        """Asynchronously create Oracle ADK memory table and indexes."""
        if not self._enabled:
            return
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        ddl = await self._memory_table_ddl()
        async with self._config.provide_session() as driver:
            await driver.execute_script(ddl)
            await driver.commit()

    async def insert_memory_entries(self, entries: list[StoredMemory], owner_id: object | None = None) -> int:
        """Asynchronously bulk-insert memory entries with duplicate suppression."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0
        helper = OracleSyncADKMemoryStore(cast("OracleSyncConfig", self._config))
        inserted = 0
        async with self._config.provide_session() as driver:
            for entry in entries:
                sql, params = helper._build_insert_params(entry, owner_id)
                try:
                    res = await driver.execute(sql, params)
                    inserted += int(res.rows_affected or 1)
                except Exception as exc:
                    if is_oracle_unique_violation(exc):
                        continue
                    raise
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
        """Asynchronously search Oracle memory entries via vectors, RRF, FTS, or `LIKE`."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        effective_limit = limit if limit is not None else self._max_results
        if effective_limit <= 0:
            return []
        table_ref = oracle_table_ref(self._memory_table)
        where_scope, scope_params = _build_memory_scope_where(app_name, user_id, scope_filter)
        emb_col = "embedding" if self._vector_dim_override else "NULL AS embedding"
        try:
            async with self._config.provide_session() as driver:
                if embedding is not None and self._vector_dim_override:
                    vec_bind = array.array("f", [float(x) for x in embedding])
                    if self._use_fts and query.strip():
                        fts_query = sanitize_oracle_text_query(query, operator=self._fts_operator)
                        sql = (
                            f"WITH vec AS ("
                            f"  SELECT id, ROW_NUMBER() OVER (ORDER BY VECTOR_DISTANCE(embedding, :query_vector, COSINE)) AS r_v "
                            f"  FROM {table_ref} WHERE {where_scope} AND embedding IS NOT NULL FETCH FIRST :top_k ROWS ONLY"
                            f"), txt AS ("
                            f"  SELECT id, ROW_NUMBER() OVER (ORDER BY SCORE(1) DESC) AS r_t "
                            f"  FROM {table_ref} WHERE {where_scope} AND CONTAINS(content_text, :text_query, 1) > 0 FETCH FIRST :top_k ROWS ONLY"
                            f") "
                            f"SELECT m.id, m.session_id, m.app_name, m.user_id, m.scope, m.event_id, m.author, "
                            f"m.timestamp, m.content_json, m.content_text, m.metadata_json, m.inserted_at, m.{emb_col} "
                            f"FROM {table_ref} m LEFT JOIN vec ON m.id = vec.id LEFT JOIN txt ON m.id = txt.id "
                            f"WHERE vec.id IS NOT NULL OR txt.id IS NOT NULL "
                            f"ORDER BY (NVL(1.0 / (60 + vec.r_v), 0) + NVL(1.0 / (60 + txt.r_t), 0)) DESC "
                            f"FETCH FIRST :top_k ROWS ONLY"
                        )
                        params = {
                            **scope_params,
                            "query_vector": vec_bind,
                            "text_query": fts_query,
                            "top_k": effective_limit,
                        }
                    else:
                        sql = (
                            f"SELECT id, session_id, app_name, user_id, scope, event_id, author, "
                            f"timestamp, content_json, content_text, metadata_json, inserted_at, {emb_col} "
                            f"FROM {table_ref} WHERE {where_scope} AND embedding IS NOT NULL "
                            f"ORDER BY VECTOR_DISTANCE(embedding, :query_vector, COSINE) ASC "
                            f"FETCH FIRST :top_k ROWS ONLY"
                        )
                        params = {**scope_params, "query_vector": vec_bind, "top_k": effective_limit}
                    rows = await driver.select(sql, params)
                    return [_row_to_stored_memory(r, deserializer=self._json_deserializer) for r in rows]

                if self._use_fts:
                    fts_query = sanitize_oracle_text_query(query, operator=self._fts_operator)
                    if not fts_query:
                        return []
                    sql = (
                        f"SELECT id, session_id, app_name, user_id, scope, event_id, author, "
                        f"timestamp, content_json, content_text, metadata_json, inserted_at, {emb_col} "
                        f"FROM {table_ref} WHERE {where_scope} AND CONTAINS(content_text, :text_query, 1) > 0 "
                        f"ORDER BY SCORE(1) DESC, timestamp DESC FETCH FIRST :limit_val ROWS ONLY"
                    )
                    rows = await driver.select(
                        sql, {**scope_params, "text_query": fts_query, "limit_val": effective_limit}
                    )
                    return [_row_to_stored_memory(r, deserializer=self._json_deserializer) for r in rows]

                sql = (
                    f"SELECT id, session_id, app_name, user_id, scope, event_id, author, "
                    f"timestamp, content_json, content_text, metadata_json, inserted_at, {emb_col} "
                    f"FROM {table_ref} WHERE {where_scope} AND LOWER(content_text) LIKE :pattern "
                    f"ORDER BY timestamp DESC FETCH FIRST :limit_val ROWS ONLY"
                )
                rows = await driver.select(
                    sql, {**scope_params, "pattern": f"%{query.lower()}%", "limit_val": effective_limit}
                )
                records = [_row_to_stored_memory(r, deserializer=self._json_deserializer) for r in rows]
                if embedding is not None:
                    return sorted(
                        records,
                        key=lambda rec: _cosine_similarity(embedding, rec.get("embedding") or ()),
                        reverse=True,
                    )[:effective_limit]
                return records
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return []
            raise

    async def delete_entries_by_session(self, session_id: str) -> int:
        """Asynchronously delete memory entries for `session_id`."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        table_ref = oracle_table_ref(self._memory_table)
        async with self._config.provide_session() as driver:
            res = await driver.execute(
                f"DELETE FROM {table_ref} WHERE session_id = :session_id", {"session_id": session_id}
            )
            await driver.commit()
            return int(res.rows_affected or 0)

    async def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = "all") -> int:
        """Asynchronously delete memory entries older than `days` days."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        table_ref = oracle_table_ref(self._memory_table)
        cutoff = datetime.now(UTC) - timedelta(days=days)
        params: dict[str, Any] = {"cutoff": cutoff}
        clauses = ["inserted_at < :cutoff"]
        if app_name is not None:
            params["app_name"] = app_name
            clauses.append("app_name = :app_name")
        if scope in {"user", "app"}:
            params["scope"] = scope
            clauses.append("scope = :scope")
        async with self._config.provide_session() as driver:
            res = await driver.execute(f"DELETE FROM {table_ref} WHERE {' AND '.join(clauses)}", params)
            await driver.commit()
            return int(res.rows_affected or 0)


class OracleSyncADKArtifactStore(_BaseSyncADKArtifactStore["OracleSyncConfig"]):
    """Synchronous Oracle ADK artifact metadata store returning `ArtifactReference`."""

    __slots__ = ("_json_deserializer", "_json_serializer", "_json_storage_override")

    def __init__(self, config: OracleSyncConfig) -> None:
        """Initialize synchronous Oracle artifact metadata store."""
        super().__init__(config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        self._json_storage_override: str | None = _adk_settings(config).get("json_storage")

    def create_table(self) -> None:
        """Create the Oracle artifact metadata table idempotently."""
        mode = _detect_storage_mode_sync(self._config, self._json_storage_override)
        with self._config.provide_session() as driver:
            driver.execute_script(_artifact_table_ddl(self._artifact_table, mode))
            driver.commit()

    def insert_artifact(self, record: StoredArtifact) -> None:
        """Upsert an ADK artifact metadata entry using Oracle `MERGE INTO`."""
        table_ref = oracle_table_ref(self._artifact_table)
        meta_raw = (
            oracle_json_dumps(record["custom_metadata"], serializer=self._json_serializer)
            if record.get("custom_metadata") is not None
            else None
        )
        sql = (
            f"MERGE INTO {table_ref} target "
            "USING (SELECT :app_name AS app_name, :user_id AS user_id, :session_id AS session_id, "
            ":filename AS filename, :version AS version, :mime_type AS mime_type, "
            ":canonical_uri AS canonical_uri, :custom_metadata AS custom_metadata, :created_at AS created_at FROM DUAL) src "
            "ON (target.app_name = src.app_name AND target.user_id = src.user_id "
            "AND target.session_id = src.session_id AND target.filename = src.filename AND target.version = src.version) "
            "WHEN MATCHED THEN UPDATE SET target.mime_type = src.mime_type, target.canonical_uri = src.canonical_uri, "
            "target.custom_metadata = src.custom_metadata "
            "WHEN NOT MATCHED THEN INSERT (app_name, user_id, session_id, filename, version, mime_type, "
            "canonical_uri, custom_metadata, created_at) "
            "VALUES (src.app_name, src.user_id, src.session_id, src.filename, src.version, src.mime_type, "
            "src.canonical_uri, src.custom_metadata, src.created_at)"
        )
        with self._config.provide_session() as driver:
            driver.execute(
                sql,
                {
                    "app_name": record["app_name"],
                    "user_id": record["user_id"],
                    "session_id": record.get("session_id") or "",
                    "filename": record["filename"],
                    "version": record["version"],
                    "mime_type": record.get("mime_type"),
                    "canonical_uri": record["canonical_uri"],
                    "custom_metadata": meta_raw,
                    "created_at": oracle_ensure_utc(record.get("created_at")),
                },
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
        """Persist a `vertebra.storage.ArtifactReference` record into Oracle."""
        table_ref = oracle_table_ref(self._artifact_table)
        meta_raw = oracle_json_dumps(custom_metadata, serializer=self._json_serializer) if custom_metadata else None
        sql = (
            f"MERGE INTO {table_ref} target "
            "USING (SELECT :app_name AS app_name, :user_id AS user_id, :session_id AS session_id, "
            ":filename AS filename, :version AS version, :mime_type AS mime_type, "
            ":canonical_uri AS canonical_uri, :custom_metadata AS custom_metadata, "
            ":size_val AS size_val, :updated_at AS updated_at, :signed_url AS signed_url FROM DUAL) src "
            "ON (target.app_name = src.app_name AND target.user_id = src.user_id "
            "AND target.session_id = src.session_id AND target.filename = src.filename AND target.version = src.version) "
            "WHEN MATCHED THEN UPDATE SET target.mime_type = src.mime_type, target.canonical_uri = src.canonical_uri, "
            "target.custom_metadata = src.custom_metadata, target.size = src.size_val, "
            "target.updated = src.updated_at, target.signed_url = src.signed_url "
            "WHEN NOT MATCHED THEN INSERT (app_name, user_id, session_id, filename, version, mime_type, "
            "canonical_uri, custom_metadata, size, updated, signed_url, created_at) "
            "VALUES (src.app_name, src.user_id, src.session_id, src.filename, src.version, src.mime_type, "
            "src.canonical_uri, src.custom_metadata, src.size_val, src.updated_at, src.signed_url, SYSTIMESTAMP)"
        )
        with self._config.provide_session() as driver:
            driver.execute(
                sql,
                {
                    "app_name": app_name,
                    "user_id": user_id,
                    "session_id": session_id or "",
                    "filename": filename,
                    "version": int(reference.version) if reference.version is not None else 0,
                    "mime_type": reference.content_type,
                    "canonical_uri": reference.uri,
                    "custom_metadata": meta_raw,
                    "size_val": reference.size,
                    "updated_at": oracle_ensure_utc(reference.updated) if reference.updated is not None else None,
                    "signed_url": reference.signed_url,
                },
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
        """Fetch an ADK artifact dictionary from Oracle."""
        table_ref = oracle_table_ref(self._artifact_table)
        params: dict[str, Any] = {
            "app_name": app_name,
            "user_id": user_id,
            "session_id": session_id or "",
            "filename": filename,
        }
        if version is not None:
            params["version"] = version
            sql = (
                f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                f"custom_metadata, created_at FROM {table_ref} "
                "WHERE app_name = :app_name AND user_id = :user_id AND session_id = :session_id "
                "AND filename = :filename AND version = :version"
            )
        else:
            sql = (
                f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                f"custom_metadata, created_at FROM {table_ref} "
                "WHERE app_name = :app_name AND user_id = :user_id AND session_id = :session_id "
                "AND filename = :filename ORDER BY version DESC FETCH FIRST 1 ROWS ONLY"
            )
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(sql, params)
            return _row_to_stored_artifact(row, deserializer=self._json_deserializer) if row is not None else None
        except Exception as exc:
            if is_oracle_table_missing(exc):
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
        """Fetch stored artifact metadata as a `vertebra.storage.ArtifactReference`."""
        table_ref = oracle_table_ref(self._artifact_table)
        params: dict[str, Any] = {
            "app_name": app_name,
            "user_id": user_id,
            "session_id": session_id or "",
            "filename": filename,
        }
        if version is not None:
            params["version"] = version
            sql = (
                f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                f"custom_metadata, size, updated, signed_url, created_at FROM {table_ref} "
                "WHERE app_name = :app_name AND user_id = :user_id AND session_id = :session_id "
                "AND filename = :filename AND version = :version"
            )
        else:
            sql = (
                f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                f"custom_metadata, size, updated, signed_url, created_at FROM {table_ref} "
                "WHERE app_name = :app_name AND user_id = :user_id AND session_id = :session_id "
                "AND filename = :filename ORDER BY version DESC FETCH FIRST 1 ROWS ONLY"
            )
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(sql, params)
            return _row_to_artifact_reference(row) if row is not None else None
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise

    def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """List distinct artifact filenames for `(app_name, user_id, session_id)`."""
        table_ref = oracle_table_ref(self._artifact_table)
        try:
            with self._config.provide_session() as driver:
                rows = driver.select(
                    f"SELECT DISTINCT filename FROM {table_ref} "
                    "WHERE app_name = :app_name AND user_id = :user_id AND session_id = :session_id ORDER BY filename ASC",
                    {"app_name": app_name, "user_id": user_id, "session_id": session_id or ""},
                )
            return [str(oracle_row_value(r, "filename", 0)) for r in rows]
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return []
            raise

    def list_artifact_versions(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None
    ) -> list[StoredArtifact]:
        """List ascending `StoredArtifact` versions for a stored artifact."""
        table_ref = oracle_table_ref(self._artifact_table)
        try:
            with self._config.provide_session() as driver:
                rows = driver.select(
                    f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                    f"custom_metadata, created_at FROM {table_ref} "
                    "WHERE app_name = :app_name AND user_id = :user_id AND session_id = :session_id "
                    "AND filename = :filename ORDER BY version ASC",
                    {"app_name": app_name, "user_id": user_id, "session_id": session_id or "", "filename": filename},
                )
            return [_row_to_stored_artifact(r, deserializer=self._json_deserializer) for r in rows]
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return []
            raise

    def delete_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None
    ) -> list[StoredArtifact]:
        """Delete all stored versions of an Oracle artifact and return removed records."""
        existing = self.list_artifact_versions(app_name, user_id, filename, session_id=session_id)
        table_ref = oracle_table_ref(self._artifact_table)
        with self._config.provide_session() as driver:
            driver.execute(
                f"DELETE FROM {table_ref} WHERE app_name = :app_name AND user_id = :user_id "
                "AND session_id = :session_id AND filename = :filename",
                {"app_name": app_name, "user_id": user_id, "session_id": session_id or "", "filename": filename},
            )
            driver.commit()
        return existing

    def get_next_version(self, app_name: str, user_id: str, filename: str, session_id: str | None = None) -> int:
        """Compute the next sequential version integer (`MAX(version) + 1` or `0`)."""
        table_ref = oracle_table_ref(self._artifact_table)
        with self._config.provide_session() as driver:
            val = driver.select_value_or_none(
                f"SELECT MAX(version) FROM {table_ref} "
                "WHERE app_name = :app_name AND user_id = :user_id AND session_id = :session_id AND filename = :filename",
                {"app_name": app_name, "user_id": user_id, "session_id": session_id or "", "filename": filename},
            )
        return 0 if val is None else int(val) + 1

    def delete_artifacts_older_than(self, before: datetime, app_name: str | None = None) -> list[StoredArtifact]:
        """Delete artifact rows older than `before` and return removed records."""
        table_ref = oracle_table_ref(self._artifact_table)
        cutoff = oracle_ensure_utc(before)
        with self._config.provide_session() as driver:
            if app_name is not None:
                rows = driver.select(
                    f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                    f"custom_metadata, created_at FROM {table_ref} "
                    "WHERE created_at < :before_ts AND app_name = :app_name ORDER BY created_at ASC",
                    {"before_ts": cutoff, "app_name": app_name},
                )
                driver.execute(
                    f"DELETE FROM {table_ref} WHERE created_at < :before_ts AND app_name = :app_name",
                    {"before_ts": cutoff, "app_name": app_name},
                )
            else:
                rows = driver.select(
                    f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                    f"custom_metadata, created_at FROM {table_ref} "
                    "WHERE created_at < :before_ts ORDER BY created_at ASC",
                    {"before_ts": cutoff},
                )
                driver.execute(f"DELETE FROM {table_ref} WHERE created_at < :before_ts", {"before_ts": cutoff})
            driver.commit()
        return [_row_to_stored_artifact(r, deserializer=self._json_deserializer) for r in rows]


class OracleAsyncADKArtifactStore(_BaseAsyncADKArtifactStore["OracleAsyncConfig"]):
    """Asynchronous Oracle ADK artifact metadata store returning `ArtifactReference`."""

    __slots__ = ("_json_deserializer", "_json_serializer", "_json_storage_override")

    def __init__(self, config: OracleAsyncConfig) -> None:
        """Initialize asynchronous Oracle artifact metadata store."""
        super().__init__(config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        self._json_storage_override: str | None = _adk_settings(config).get("json_storage")

    async def create_table(self) -> None:
        """Asynchronously create Oracle artifact metadata table idempotently."""
        mode = await _detect_storage_mode_async(self._config, self._json_storage_override)
        async with self._config.provide_session() as driver:
            await driver.execute_script(_artifact_table_ddl(self._artifact_table, mode))
            await driver.commit()

    async def insert_artifact(self, record: StoredArtifact) -> None:
        """Asynchronously upsert an ADK artifact metadata entry using `MERGE INTO`."""
        table_ref = oracle_table_ref(self._artifact_table)
        meta_raw = (
            oracle_json_dumps(record["custom_metadata"], serializer=self._json_serializer)
            if record.get("custom_metadata") is not None
            else None
        )
        sql = (
            f"MERGE INTO {table_ref} target "
            "USING (SELECT :app_name AS app_name, :user_id AS user_id, :session_id AS session_id, "
            ":filename AS filename, :version AS version, :mime_type AS mime_type, "
            ":canonical_uri AS canonical_uri, :custom_metadata AS custom_metadata, :created_at AS created_at FROM DUAL) src "
            "ON (target.app_name = src.app_name AND target.user_id = src.user_id "
            "AND target.session_id = src.session_id AND target.filename = src.filename AND target.version = src.version) "
            "WHEN MATCHED THEN UPDATE SET target.mime_type = src.mime_type, target.canonical_uri = src.canonical_uri, "
            "target.custom_metadata = src.custom_metadata "
            "WHEN NOT MATCHED THEN INSERT (app_name, user_id, session_id, filename, version, mime_type, "
            "canonical_uri, custom_metadata, created_at) "
            "VALUES (src.app_name, src.user_id, src.session_id, src.filename, src.version, src.mime_type, "
            "src.canonical_uri, src.custom_metadata, src.created_at)"
        )
        async with self._config.provide_session() as driver:
            await driver.execute(
                sql,
                {
                    "app_name": record["app_name"],
                    "user_id": record["user_id"],
                    "session_id": record.get("session_id") or "",
                    "filename": record["filename"],
                    "version": record["version"],
                    "mime_type": record.get("mime_type"),
                    "canonical_uri": record["canonical_uri"],
                    "custom_metadata": meta_raw,
                    "created_at": oracle_ensure_utc(record.get("created_at")),
                },
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
        """Asynchronously store a `vertebra.storage.ArtifactReference` record into Oracle."""
        table_ref = oracle_table_ref(self._artifact_table)
        meta_raw = oracle_json_dumps(custom_metadata, serializer=self._json_serializer) if custom_metadata else None
        sql = (
            f"MERGE INTO {table_ref} target "
            "USING (SELECT :app_name AS app_name, :user_id AS user_id, :session_id AS session_id, "
            ":filename AS filename, :version AS version, :mime_type AS mime_type, "
            ":canonical_uri AS canonical_uri, :custom_metadata AS custom_metadata, "
            ":size_val AS size_val, :updated_at AS updated_at, :signed_url AS signed_url FROM DUAL) src "
            "ON (target.app_name = src.app_name AND target.user_id = src.user_id "
            "AND target.session_id = src.session_id AND target.filename = src.filename AND target.version = src.version) "
            "WHEN MATCHED THEN UPDATE SET target.mime_type = src.mime_type, target.canonical_uri = src.canonical_uri, "
            "target.custom_metadata = src.custom_metadata, target.size = src.size_val, "
            "target.updated = src.updated_at, target.signed_url = src.signed_url "
            "WHEN NOT MATCHED THEN INSERT (app_name, user_id, session_id, filename, version, mime_type, "
            "canonical_uri, custom_metadata, size, updated, signed_url, created_at) "
            "VALUES (src.app_name, src.user_id, src.session_id, src.filename, src.version, src.mime_type, "
            "src.canonical_uri, src.custom_metadata, src.size_val, src.updated_at, src.signed_url, SYSTIMESTAMP)"
        )
        async with self._config.provide_session() as driver:
            await driver.execute(
                sql,
                {
                    "app_name": app_name,
                    "user_id": user_id,
                    "session_id": session_id or "",
                    "filename": filename,
                    "version": int(reference.version) if reference.version is not None else 0,
                    "mime_type": reference.content_type,
                    "canonical_uri": reference.uri,
                    "custom_metadata": meta_raw,
                    "size_val": reference.size,
                    "updated_at": oracle_ensure_utc(reference.updated) if reference.updated is not None else None,
                    "signed_url": reference.signed_url,
                },
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
        """Asynchronously fetch an ADK artifact metadata dictionary."""
        table_ref = oracle_table_ref(self._artifact_table)
        params: dict[str, Any] = {
            "app_name": app_name,
            "user_id": user_id,
            "session_id": session_id or "",
            "filename": filename,
        }
        if version is not None:
            params["version"] = version
            sql = (
                f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                f"custom_metadata, created_at FROM {table_ref} "
                "WHERE app_name = :app_name AND user_id = :user_id AND session_id = :session_id "
                "AND filename = :filename AND version = :version"
            )
        else:
            sql = (
                f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                f"custom_metadata, created_at FROM {table_ref} "
                "WHERE app_name = :app_name AND user_id = :user_id AND session_id = :session_id "
                "AND filename = :filename ORDER BY version DESC FETCH FIRST 1 ROWS ONLY"
            )
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(sql, params)
            return _row_to_stored_artifact(row, deserializer=self._json_deserializer) if row is not None else None
        except Exception as exc:
            if is_oracle_table_missing(exc):
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
        """Asynchronously fetch stored artifact metadata as `ArtifactReference`."""
        table_ref = oracle_table_ref(self._artifact_table)
        params: dict[str, Any] = {
            "app_name": app_name,
            "user_id": user_id,
            "session_id": session_id or "",
            "filename": filename,
        }
        if version is not None:
            params["version"] = version
            sql = (
                f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                f"custom_metadata, size, updated, signed_url, created_at FROM {table_ref} "
                "WHERE app_name = :app_name AND user_id = :user_id AND session_id = :session_id "
                "AND filename = :filename AND version = :version"
            )
        else:
            sql = (
                f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                f"custom_metadata, size, updated, signed_url, created_at FROM {table_ref} "
                "WHERE app_name = :app_name AND user_id = :user_id AND session_id = :session_id "
                "AND filename = :filename ORDER BY version DESC FETCH FIRST 1 ROWS ONLY"
            )
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(sql, params)
            return _row_to_artifact_reference(row) if row is not None else None
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise

    async def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """Asynchronously list distinct artifact filenames."""
        table_ref = oracle_table_ref(self._artifact_table)
        try:
            async with self._config.provide_session() as driver:
                rows = await driver.select(
                    f"SELECT DISTINCT filename FROM {table_ref} "
                    "WHERE app_name = :app_name AND user_id = :user_id AND session_id = :session_id ORDER BY filename ASC",
                    {"app_name": app_name, "user_id": user_id, "session_id": session_id or ""},
                )
            return [str(oracle_row_value(r, "filename", 0)) for r in rows]
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return []
            raise

    async def list_artifact_versions(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None
    ) -> list[StoredArtifact]:
        """Asynchronously list ascending `StoredArtifact` versions for a stored artifact."""
        table_ref = oracle_table_ref(self._artifact_table)
        try:
            async with self._config.provide_session() as driver:
                rows = await driver.select(
                    f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                    f"custom_metadata, created_at FROM {table_ref} "
                    "WHERE app_name = :app_name AND user_id = :user_id AND session_id = :session_id "
                    "AND filename = :filename ORDER BY version ASC",
                    {"app_name": app_name, "user_id": user_id, "session_id": session_id or "", "filename": filename},
                )
            return [_row_to_stored_artifact(r, deserializer=self._json_deserializer) for r in rows]
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return []
            raise

    async def delete_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None
    ) -> list[StoredArtifact]:
        """Asynchronously delete all stored versions of an Oracle artifact and return removed records."""
        existing = await self.list_artifact_versions(app_name, user_id, filename, session_id=session_id)
        table_ref = oracle_table_ref(self._artifact_table)
        async with self._config.provide_session() as driver:
            await driver.execute(
                f"DELETE FROM {table_ref} WHERE app_name = :app_name AND user_id = :user_id "
                "AND session_id = :session_id AND filename = :filename",
                {"app_name": app_name, "user_id": user_id, "session_id": session_id or "", "filename": filename},
            )
            await driver.commit()
        return existing

    async def get_next_version(self, app_name: str, user_id: str, filename: str, session_id: str | None = None) -> int:
        """Asynchronously compute the next sequential version integer."""
        table_ref = oracle_table_ref(self._artifact_table)
        async with self._config.provide_session() as driver:
            val = await driver.select_value_or_none(
                f"SELECT MAX(version) FROM {table_ref} "
                "WHERE app_name = :app_name AND user_id = :user_id AND session_id = :session_id AND filename = :filename",
                {"app_name": app_name, "user_id": user_id, "session_id": session_id or "", "filename": filename},
            )
        return 0 if val is None else int(val) + 1

    async def delete_artifacts_older_than(self, before: datetime, app_name: str | None = None) -> list[StoredArtifact]:
        """Asynchronously delete artifact rows older than `before` and return removed records."""
        table_ref = oracle_table_ref(self._artifact_table)
        cutoff = oracle_ensure_utc(before)
        async with self._config.provide_session() as driver:
            if app_name is not None:
                rows = await driver.select(
                    f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                    f"custom_metadata, created_at FROM {table_ref} "
                    "WHERE created_at < :before_ts AND app_name = :app_name ORDER BY created_at ASC",
                    {"before_ts": cutoff, "app_name": app_name},
                )
                await driver.execute(
                    f"DELETE FROM {table_ref} WHERE created_at < :before_ts AND app_name = :app_name",
                    {"before_ts": cutoff, "app_name": app_name},
                )
            else:
                rows = await driver.select(
                    f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
                    f"custom_metadata, created_at FROM {table_ref} "
                    "WHERE created_at < :before_ts ORDER BY created_at ASC",
                    {"before_ts": cutoff},
                )
                await driver.execute(f"DELETE FROM {table_ref} WHERE created_at < :before_ts", {"before_ts": cutoff})
            await driver.commit()
        return [_row_to_stored_artifact(r, deserializer=self._json_deserializer) for r in rows]
