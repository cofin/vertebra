"""Oracle ADK session, 23ai vector/FTS memory, and artifact stores."""

from __future__ import annotations

import array
import logging
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Final, Literal, NotRequired, TypedDict, cast

from sqlspec.config import ADKConfig
from sqlspec.exceptions import SerializationConflictError
from sqlspec.extensions.adk import BaseAsyncADKStore, BaseSyncADKStore, StoredEvent, StoredSession
from sqlspec.extensions.adk.store import adk_primary_read_scope, normalize_session_list_options

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
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from sqlspec.extensions.adk import SessionOrderBy, StoredMemory
    from sqlspec.extensions.adk.artifact import StoredArtifact
    from sqlspec.extensions.adk.artifact.store import BaseAsyncADKArtifactStore as _BaseAsyncADKArtifactStore
    from sqlspec.extensions.adk.artifact.store import BaseSyncADKArtifactStore as _BaseSyncADKArtifactStore
    from sqlspec.extensions.adk.converters import merge_scoped_state
    from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore as _BaseAsyncADKMemoryStore
    from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore as _BaseSyncADKMemoryStore

    from vertebra.extensions.sqlspec.adapters.oracle.config import OracleAsyncConfig, OracleSyncConfig
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
    "OracleADKCompressionConfig",
    "OracleADKConfig",
    "OracleADKPartitionConfig",
    "OracleAsyncADKArtifactStore",
    "OracleAsyncADKMemoryStore",
    "OracleAsyncADKStore",
    "OracleSyncADKArtifactStore",
    "OracleSyncADKMemoryStore",
    "OracleSyncADKStore",
)

logger = logging.getLogger("vertebra.extensions.sqlspec.adapters.oracle.adk")

_MAX_STATE_MERGE_ATTEMPTS: Final[int] = 8
_ORACLE_COMPRESSION_CLAUSES: Final[dict[str, str]] = {
    "basic": "ROW STORE COMPRESS BASIC",
    "oltp": "ROW STORE COMPRESS ADVANCED",
    "advanced": "ROW STORE COMPRESS ADVANCED",
    "query_low": "COLUMN STORE COMPRESS FOR QUERY LOW",
    "query_high": "COLUMN STORE COMPRESS FOR QUERY HIGH",
    "archive_low": "COLUMN STORE COMPRESS FOR ARCHIVE LOW",
    "archive_high": "COLUMN STORE COMPRESS FOR ARCHIVE HIGH",
}
_ORACLE_ADK_EXTENSION_OPTIONS: Final[frozenset[str]] = frozenset(
    {
        "app_state_table_options",
        "compress_events",
        "compression",
        "events_table_options",
        "fts_operator",
        "in_memory",
        "inmemory",
        "json_storage",
        "memory_retention_days",
        "memory_table_options",
        "partitioning",
        "retention_days",
        "session_table_options",
        "table_options",
        "use_fts",
        "user_state_table_options",
        "vector_dimensions",
        "vector_index_type",
    }
)


class OracleADKCompressionConfig(TypedDict):
    """Oracle-specific ADK table compression settings."""

    enabled: NotRequired[bool]
    """Enable Oracle table compression clauses."""

    algorithm: NotRequired[str]
    """Oracle compression algorithm key."""


class OracleADKPartitionConfig(TypedDict):
    """Oracle-specific ADK table partitioning settings."""

    strategy: NotRequired[str]
    """Partitioning strategy, such as ``hash`` or ``range``."""

    partition_count: NotRequired[int]
    """Hash partition count."""

    partitions: NotRequired[int]
    """Alias for ``partition_count``."""

    interval: NotRequired[str]
    """Range partition interval key."""

    initial_less_than: NotRequired[str]
    """Initial range partition upper bound expression."""

    partition_key: NotRequired[str]
    """Default partition key for all ADK tables."""

    session_partition_key: NotRequired[str]
    """Partition key override for the session table."""

    events_partition_key: NotRequired[str]
    """Partition key override for the events table."""

    app_state_partition_key: NotRequired[str]
    """Partition key override for the app state table."""

    user_state_partition_key: NotRequired[str]
    """Partition key override for the user state table."""

    memory_partition_key: NotRequired[str]
    """Partition key override for the memory table."""


class OracleADKConfig(ADKConfig, total=False):
    """Oracle-specific ADK extension settings under ``extension_config['adk']``."""

    compress_events: NotRequired[bool]
    """Enable Oracle row-store compression on the ADK events table."""

    retention_days: NotRequired[int | None]
    """Default retention window in days for ADK memory and event maintenance."""

    memory_retention_days: NotRequired[int | None]
    """Optional memory-specific retention window in days."""

    use_fts: NotRequired[bool]
    """Enable Oracle Text ``CTXSYS.CONTEXT`` full-text indexing on memory content."""

    fts_operator: NotRequired[str]
    """Oracle Text boolean combination operator (defaults to ``'ACCUM'``)."""

    vector_index_type: NotRequired[str]
    """Vector index algorithm for Oracle 23ai memory embeddings."""

    vector_dimensions: NotRequired[int]
    """Dimensionality of embedding vectors stored in Oracle 23ai ``VECTOR`` columns."""

    inmemory: NotRequired[bool]
    """Enable Oracle ``INMEMORY`` table clauses on ADK tables."""

    in_memory: NotRequired[bool]
    """Upstream alias enabling Oracle ``INMEMORY`` clauses on ADK tables."""

    json_storage: NotRequired[str]
    """Override Oracle JSON column storage mode (``'json'``, ``'blob_json'``, ``'blob_plain'``)."""

    compression: NotRequired[OracleADKCompressionConfig | str]
    """Oracle ADK table compression clause or configuration mapping."""

    partitioning: NotRequired[OracleADKPartitionConfig]
    """Oracle ADK table partitioning settings."""

    table_options: NotRequired[str]
    """Default raw Oracle table storage options applied across ADK tables."""

    session_table_options: NotRequired[str]
    """Raw Oracle table options for the ADK session table."""

    events_table_options: NotRequired[str]
    """Raw Oracle table options for the ADK events table."""

    app_state_table_options: NotRequired[str]
    """Raw Oracle table options for the ADK app state table."""

    user_state_table_options: NotRequired[str]
    """Raw Oracle table options for the ADK user state table."""

    memory_table_options: NotRequired[str]
    """Raw Oracle table options for the ADK memory table."""


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
    """Decode an Oracle JSON, BLOB, or LOB payload into a dictionary."""
    if raw is None:
        return {}
    read_fn = getattr(raw, "read", None)
    if callable(read_fn):
        raw = read_fn()
    decoded = oracle_json_loads(raw, deserializer=deserializer)
    return dict(decoded) if isinstance(decoded, Mapping) else {}


def _plsql_ignore_exists(stmt: str) -> str:
    """Wrap an Oracle DDL statement to ignore ``ORA-00955`` name-already-used."""
    escaped = escape_oracle_literal(stmt)
    return (
        "BEGIN\n"
        f"    EXECUTE IMMEDIATE '{escaped}';\n"
        "EXCEPTION\n"
        "    WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF;\n"
        "END;"
    )


def _plsql_ignore_missing(stmt: str, *, code: int = -942) -> str:
    """Wrap an Oracle DROP statement to ignore ``ORA-00942`` (``-942``) or ``ORA-01418`` (``-1418``)."""
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


def _oracle_adk_table_features(adk_cfg: Mapping[str, Any], table_kind: str) -> str:
    """Normalize Oracle ``INMEMORY``, compression, and per-table option DDL clauses."""
    normalized: dict[str, Any] = {}
    if adk_cfg.get("in_memory") or adk_cfg.get("inmemory"):
        normalized["in_memory"] = True
    compression = adk_cfg.get("compression")
    if isinstance(compression, str) and compression.strip():
        normalized["compression"] = compression.strip()
    elif isinstance(compression, Mapping) and compression.get("enabled"):
        algorithm = str(compression.get("algorithm") or "advanced").lower()
        normalized["compression"] = _ORACLE_COMPRESSION_CLAUSES.get(algorithm, "ROW STORE COMPRESS ADVANCED")
    elif table_kind == "events" and adk_cfg.get("compress_events"):
        normalized["compression"] = "ROW STORE COMPRESS ADVANCED"
    table_options = adk_cfg.get(f"{table_kind}_table_options") or adk_cfg.get("table_options")
    if isinstance(table_options, str) and table_options.strip():
        normalized["table_options"] = table_options.strip()
    return oracle_table_feature_clause(normalized)


def _sessions_ddl(session_table: str, owner_id_ddl: str | None, storage_mode: str, adk_cfg: Mapping[str, Any]) -> str:
    """Build PL/SQL idempotent DDL for Oracle ADK sessions table."""
    table_ref = oracle_table_ref(session_table)
    owner_col = f"{owner_id_ddl}, " if owner_id_ddl else ""
    state_col = oracle_json_column_ddl("state", storage_mode, not_null=True)
    features = _oracle_adk_table_features(adk_cfg, "session")
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
    features = _oracle_adk_table_features(adk_cfg, "events")
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
    features = _oracle_adk_table_features(adk_cfg, "app_state")
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
    features = _oracle_adk_table_features(adk_cfg, "user_state")
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


def _row_to_stored_session(
    row: Any,
    *,
    deserializer: Any = None,
    app_state: dict[str, Any] | None = None,
    user_state: dict[str, Any] | None = None,
    merge_scopes: bool = False,
) -> StoredSession:
    """Decode an Oracle session row into a ``StoredSession`` dictionary."""
    persisted_state = _decode_json_dict(oracle_row_value(row, "state", 3), deserializer=deserializer)
    resolved_state = merge_scoped_state(persisted_state, app_state, user_state) if merge_scopes else persisted_state
    return StoredSession(
        id=str(oracle_row_value(row, "id", 0)),
        app_name=str(oracle_row_value(row, "app_name", 1)),
        user_id=str(oracle_row_value(row, "user_id", 2)),
        state=resolved_state,
        create_time=oracle_ensure_utc(oracle_row_value(row, "create_time", 4)),
        update_time=oracle_ensure_utc(oracle_row_value(row, "update_time", 5)),
    )


def _row_to_stored_event(row: Any, *, deserializer: Any = None) -> StoredEvent:
    """Decode an Oracle event row into a ``StoredEvent`` dictionary."""
    raw_data = oracle_row_value(row, "event_data", 6)
    read_fn = getattr(raw_data, "read", None)
    if callable(read_fn):
        raw_data = read_fn()
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
    adk_cfg: Mapping[str, Any],
    *,
    vector_dimensions: int | None,
    use_fts: bool,
) -> str:
    """Build PL/SQL idempotent DDL for Oracle ADK memory entries including optional 23ai ``VECTOR`` column."""
    table_ref = oracle_table_ref(memory_table)
    owner_col = f"{owner_id_ddl}, " if owner_id_ddl else ""
    emb_col = f"embedding VECTOR({int(vector_dimensions)}, FLOAT32), " if vector_dimensions else ""
    content_col = oracle_json_column_ddl("content_json", storage_mode, not_null=True)
    metadata_col = oracle_json_column_ddl("metadata_json", storage_mode, not_null=False)
    features = _oracle_adk_table_features(adk_cfg, "memory")
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
            f"){features}"
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
    """Decode an Oracle memory row into a ``StoredMemory`` dictionary."""
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
    """Decode an Oracle artifact row into a ``StoredArtifact`` dictionary."""
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
    """Decode an Oracle artifact row into a ``vertebra.storage.ArtifactReference``."""
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


class _OracleADKSessionSQLMixin:
    """Shared Oracle DDL and named-bind SQL builders for sync/async ADK session stores."""

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

    def _build_sessions_ddl(self, storage_mode: str) -> str:
        return _sessions_ddl(self._session_table, self._owner_id_column_ddl, storage_mode, _adk_settings(self._config))

    def _build_events_ddl(self, storage_mode: str) -> str:
        return _events_ddl(self._events_table, self._session_table, storage_mode, _adk_settings(self._config))

    def _build_app_states_ddl(self, storage_mode: str) -> str:
        return _app_states_ddl(self._app_state_table, storage_mode, _adk_settings(self._config))

    def _build_user_states_ddl(self, storage_mode: str) -> str:
        return _user_states_ddl(self._user_state_table, storage_mode, _adk_settings(self._config))

    def _build_metadata_ddl(self) -> str:
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

    def _create_session_sql_and_params(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
        state: Mapping[str, Any],
        owner_id: Any | None,
    ) -> tuple[str, dict[str, Any]]:
        table_ref = oracle_table_ref(self._session_table)
        state_json = oracle_json_dumps(dict(state), serializer=self._json_serializer)
        if self._owner_id_column_name:
            owner_col = oracle_table_ref(self._owner_id_column_name, "owner column")
            sql = (
                f"INSERT INTO {table_ref} (id, app_name, user_id, {owner_col}, state, create_time, update_time) "
                "VALUES (:id, :app_name, :user_id, :owner_id, :state, SYSTIMESTAMP, SYSTIMESTAMP)"
            )
            params: dict[str, Any] = {
                "id": session_id,
                "app_name": app_name,
                "user_id": user_id,
                "owner_id": owner_id,
                "state": state_json,
            }
            return sql, params
        sql = (
            f"INSERT INTO {table_ref} (id, app_name, user_id, state, create_time, update_time) "
            "VALUES (:id, :app_name, :user_id, :state, SYSTIMESTAMP, SYSTIMESTAMP)"
        )
        return sql, {"id": session_id, "app_name": app_name, "user_id": user_id, "state": state_json}

    def _touch_session_sql(self) -> str:
        table_ref = oracle_table_ref(self._session_table)
        return (
            f"UPDATE {table_ref} SET update_time = SYSTIMESTAMP "
            "WHERE app_name = :app_name AND user_id = :user_id AND id = :id"
        )

    def _get_session_sql(self) -> str:
        table_ref = oracle_table_ref(self._session_table)
        return (
            f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table_ref} "
            "WHERE app_name = :app_name AND user_id = :user_id AND id = :id"
        )

    def _update_session_state_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        state: Mapping[str, Any],
    ) -> tuple[str, dict[str, Any]]:
        table_ref = oracle_table_ref(self._session_table)
        state_json = oracle_json_dumps(dict(state), serializer=self._json_serializer)
        sql = (
            f"UPDATE {table_ref} SET state = :state, update_time = SYSTIMESTAMP "
            "WHERE app_name = :app_name AND user_id = :user_id AND id = :id"
        )
        return sql, {"state": state_json, "app_name": app_name, "user_id": user_id, "id": session_id}

    def _list_sessions_sql_and_params(
        self,
        app_name: str,
        user_id: str | None,
        *,
        order_by: SessionOrderBy,
        descending: bool,
        limit: int | None,
        offset: int | None,
    ) -> tuple[str, dict[str, Any]] | None:
        col, direction, norm_limit, norm_offset = normalize_session_list_options(order_by, descending, limit, offset)
        if norm_limit == 0:
            return None
        table_ref = oracle_table_ref(self._session_table)
        params: dict[str, Any] = {"app_name": app_name}
        where_sql = "app_name = :app_name"
        if user_id is not None:
            where_sql += " AND user_id = :user_id"
            params["user_id"] = user_id
        sql = (
            f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table_ref} "
            f"WHERE {where_sql} ORDER BY {col} {direction}, id {direction}"
        )
        if norm_offset > 0:
            params["offset_val"] = norm_offset
            sql += " OFFSET :offset_val ROWS"
        if norm_limit is not None:
            params["limit_val"] = norm_limit
            sql += " FETCH NEXT :limit_val ROWS ONLY"
        return sql, params

    def _delete_session_sql(self) -> str:
        table_ref = oracle_table_ref(self._session_table)
        return f"DELETE FROM {table_ref} WHERE app_name = :app_name AND user_id = :user_id AND id = :id"

    def _insert_event_sql_and_params(self, event_record: StoredEvent) -> tuple[str, dict[str, Any]]:
        events_ref = oracle_table_ref(self._events_table)
        event_json = oracle_json_dumps(event_record["event_data"], serializer=self._json_serializer)
        sql = (
            f"INSERT INTO {events_ref} (id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
            "VALUES (:id, :app_name, :user_id, :session_id, :invocation_id, :ts, :event_data)"
        )
        params: dict[str, Any] = {
            "id": event_record["id"],
            "app_name": event_record["app_name"],
            "user_id": event_record["user_id"],
            "session_id": event_record["session_id"],
            "invocation_id": event_record.get("invocation_id"),
            "ts": oracle_ensure_utc(event_record["timestamp"]),
            "event_data": event_json,
        }
        return sql, params

    def _get_events_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None,
        limit: int | None,
    ) -> tuple[str, dict[str, Any]]:
        events_ref = oracle_table_ref(self._events_table)
        sessions_ref = oracle_table_ref(self._session_table)
        where_clauses = ["s.app_name = :app_name", "s.user_id = :user_id", "s.id = :session_id"]
        params: dict[str, Any] = {"app_name": app_name, "user_id": user_id, "session_id": session_id}
        if after_timestamp is not None:
            params["after_ts"] = oracle_ensure_utc(after_timestamp)
            where_clauses.append("e.timestamp > :after_ts")
        if limit is not None:
            params["limit_val"] = limit
            order_clause = "ORDER BY e.timestamp DESC, e.id DESC FETCH NEXT :limit_val ROWS ONLY"
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
    ) -> tuple[str, dict[str, Any]]:
        table_ref = oracle_table_ref(table_name)
        cutoff = oracle_ensure_utc(before)
        if app_name is not None:
            return (
                f"DELETE FROM {table_ref} WHERE {time_column} < :cutoff AND app_name = :app_name",
                {"cutoff": cutoff, "app_name": app_name},
            )
        return f"DELETE FROM {table_ref} WHERE {time_column} < :cutoff", {"cutoff": cutoff}

    def _select_state_sql(self, table_name: str, key_columns: tuple[str, ...]) -> str:
        table_ref = oracle_table_ref(table_name)
        where_sql = " AND ".join(f"{col} = :{col}" for col in key_columns)
        return f"SELECT state FROM {table_ref} WHERE {where_sql}"

    def _delta_state_sqls(self, table_name: str, key_columns: tuple[str, ...]) -> tuple[str, str, str]:
        table_ref = oracle_table_ref(table_name)
        where_sql = " AND ".join(f"{col} = :{col}" for col in key_columns)
        select_for_update_sql = f"SELECT state FROM {table_ref} WHERE {where_sql} FOR UPDATE"
        cols = ", ".join((*key_columns, "state", "update_time"))
        binds = ", ".join(f":{col}" for col in key_columns)
        insert_sql = f"INSERT INTO {table_ref} ({cols}) VALUES ({binds}, :state, SYSTIMESTAMP)"
        update_sql = f"UPDATE {table_ref} SET state = :state, update_time = SYSTIMESTAMP WHERE {where_sql}"
        return select_for_update_sql, insert_sql, update_sql

    def _upsert_app_state_sql_and_params(self, app_name: str, state: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
        table_ref = oracle_table_ref(self._app_state_table)
        state_json = oracle_json_dumps(dict(state), serializer=self._json_serializer)
        sql = (
            f"MERGE INTO {table_ref} target USING (SELECT :app_name AS app_name, :state AS state FROM DUAL) src "
            "ON (target.app_name = src.app_name) "
            "WHEN MATCHED THEN UPDATE SET target.state = src.state, target.update_time = SYSTIMESTAMP "
            "WHEN NOT MATCHED THEN INSERT (app_name, state, update_time) VALUES (src.app_name, src.state, SYSTIMESTAMP)"
        )
        return sql, {"app_name": app_name, "state": state_json}

    def _upsert_user_state_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        state: Mapping[str, Any],
    ) -> tuple[str, dict[str, Any]]:
        table_ref = oracle_table_ref(self._user_state_table)
        state_json = oracle_json_dumps(dict(state), serializer=self._json_serializer)
        sql = (
            f"MERGE INTO {table_ref} target "
            "USING (SELECT :app_name AS app_name, :user_id AS user_id, :state AS state FROM DUAL) src "
            "ON (target.app_name = src.app_name AND target.user_id = src.user_id) "
            "WHEN MATCHED THEN UPDATE SET target.state = src.state, target.update_time = SYSTIMESTAMP "
            "WHEN NOT MATCHED THEN INSERT (app_name, user_id, state, update_time) "
            "VALUES (src.app_name, src.user_id, src.state, SYSTIMESTAMP)"
        )
        return sql, {"app_name": app_name, "user_id": user_id, "state": state_json}

    def _get_metadata_sql(self) -> str:
        return f"SELECT value FROM {oracle_table_ref(self._metadata_table)} WHERE key = :key"

    def _set_metadata_sql(self) -> str:
        table_ref = oracle_table_ref(self._metadata_table)
        return (
            f"MERGE INTO {table_ref} target USING (SELECT :key AS key, :value AS value FROM DUAL) src "
            "ON (target.key = src.key) "
            "WHEN MATCHED THEN UPDATE SET target.value = src.value, target.update_time = SYSTIMESTAMP "
            "WHEN NOT MATCHED THEN INSERT (key, value, update_time) VALUES (src.key, src.value, SYSTIMESTAMP)"
        )


class OracleSyncADKStore(_OracleADKSessionSQLMixin, BaseSyncADKStore["OracleSyncConfig"]):
    """Synchronous Oracle ADK session and event store using ``MERGE INTO`` and PL/SQL guards."""

    __slots__ = ("_json_deserializer", "_json_serializer", "_json_storage_override")

    extension_config_options = _ORACLE_ADK_EXTENSION_OPTIONS

    def __init__(self, config: OracleSyncConfig, *, read_config: OracleSyncConfig | None = None) -> None:
        """Initialize the synchronous Oracle ADK session store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        self._json_storage_override: str | None = _adk_settings(config).get("json_storage")

    def _storage_mode(self) -> str:
        return _detect_storage_mode_sync(self._config, self._json_storage_override)

    def _sessions_table_ddl(self) -> str:
        return self._build_sessions_ddl(self._storage_mode())

    def _events_table_ddl(self) -> str:
        return self._build_events_ddl(self._storage_mode())

    def _app_states_table_ddl(self) -> str:
        return self._build_app_states_ddl(self._storage_mode())

    def _user_states_table_ddl(self) -> str:
        return self._build_user_states_ddl(self._storage_mode())

    def _metadata_table_ddl(self) -> str:
        return self._build_metadata_ddl()

    def create_tables(self) -> None:
        """Create ADK session, event, app state, user state, and metadata tables idempotently."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return
        mode = self._storage_mode()
        with self._config.provide_session() as driver:
            driver.execute_script(self._build_sessions_ddl(mode))
            driver.execute_script(self._build_events_ddl(mode))
            driver.execute_script(self._build_app_states_ddl(mode))
            driver.execute_script(self._build_user_states_ddl(mode))
            driver.execute_script(self._build_metadata_ddl())
            driver.commit()

    def _select_state_with_driver(
        self,
        driver: Any,
        table_name: str,
        key_columns: tuple[str, ...],
        key_params: dict[str, Any],
    ) -> dict[str, Any] | None:
        try:
            row = driver.select_one_or_none(self._select_state_sql(table_name, key_columns), key_params)
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise
        if row is None:
            return None
        return _decode_json_dict(oracle_row_value(row, "state", 0), deserializer=self._json_deserializer)

    def _apply_state_delta_with_driver(
        self,
        driver: Any,
        table_name: str,
        key_columns: tuple[str, ...],
        key_params: dict[str, Any],
        delta: Mapping[str, Any],
    ) -> dict[str, Any]:
        select_for_update_sql, insert_sql, update_sql = self._delta_state_sqls(table_name, key_columns)
        for _ in range(_MAX_STATE_MERGE_ATTEMPTS):
            row = driver.select_one_or_none(select_for_update_sql, key_params)
            if row is None:
                merged = dict(delta)
                merged_json = oracle_json_dumps(merged, serializer=self._json_serializer)
                try:
                    driver.execute(insert_sql, {**key_params, "state": merged_json})
                except Exception as exc:
                    if is_oracle_unique_violation(exc):
                        continue
                    raise
                return merged
            existing = _decode_json_dict(oracle_row_value(row, "state", 0), deserializer=self._json_deserializer)
            merged = {**existing, **delta}
            merged_json = oracle_json_dumps(merged, serializer=self._json_serializer)
            driver.execute(update_sql, {**key_params, "state": merged_json})
            return merged
        msg = f"Failed to lock and merge Oracle scoped state after {_MAX_STATE_MERGE_ATTEMPTS} attempts."
        raise SerializationConflictError(msg)

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
        """Create a new Oracle ADK session and atomically apply optional scoped state deltas."""
        insert_sql, insert_params = self._create_session_sql_and_params(session_id, app_name, user_id, state, owner_id)
        with self._config.provide_session() as driver:
            driver.execute(insert_sql, insert_params)
            resolved_app = (
                self._apply_state_delta_with_driver(
                    driver, self._app_state_table, ("app_name",), {"app_name": app_name}, app_state_delta
                )
                if app_state_delta
                else self._select_state_with_driver(
                    driver, self._app_state_table, ("app_name",), {"app_name": app_name}
                )
            )
            resolved_user = (
                self._apply_state_delta_with_driver(
                    driver,
                    self._user_state_table,
                    ("app_name", "user_id"),
                    {"app_name": app_name, "user_id": user_id},
                    user_state_delta,
                )
                if user_state_delta
                else self._select_state_with_driver(
                    driver,
                    self._user_state_table,
                    ("app_name", "user_id"),
                    {"app_name": app_name, "user_id": user_id},
                )
            )
            driver.commit()
        with adk_primary_read_scope():
            created = self.get_session(app_name, user_id, session_id)
        if created is None:
            msg = f"Failed to reload newly created ADK session {session_id!r}"
            raise RuntimeError(msg)
        created["state"] = merge_scoped_state(created["state"], resolved_app, resolved_user)
        return created

    def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: int | timedelta | None = None
    ) -> StoredSession | None:
        """Fetch an Oracle ADK session row by key, renewing ``update_time`` if requested."""
        should_touch = renew_for is not None and self._calculate_expires_at(renew_for) is not None
        params = {"app_name": app_name, "user_id": user_id, "id": session_id}
        try:
            with self._provide_replica_session(primary=should_touch) as driver:
                if should_touch:
                    driver.execute(self._touch_session_sql(), params)
                    driver.commit()
                row = driver.select_one_or_none(self._get_session_sql(), params)
                return _row_to_stored_session(row, deserializer=self._json_deserializer) if row is not None else None
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Update Oracle ADK session state."""
        sql, params = self._update_session_state_sql_and_params(app_name, user_id, session_id, state)
        with self._config.provide_session() as driver:
            driver.execute(sql, params)
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
        """List Oracle ADK sessions using ``OFFSET`` / ``FETCH NEXT`` pagination."""
        built = self._list_sessions_sql_and_params(
            app_name, user_id, order_by=order_by, descending=descending, limit=limit, offset=offset
        )
        if built is None:
            return []
        sql, params = built
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, params)
            return [_row_to_stored_session(row, deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return []
            raise

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete an ADK session row by composite key."""
        sql = self._delete_session_sql()
        with self._config.provide_session() as driver:
            driver.execute(sql, {"app_name": app_name, "user_id": user_id, "id": session_id})
            driver.commit()

    def _insert_event_with_driver(self, driver: Any, event_record: StoredEvent) -> None:
        sql, params = self._insert_event_sql_and_params(event_record)
        try:
            driver.execute(sql, params)
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
        app_state_delta: dict[str, Any] | None = None,
        user_state_delta: dict[str, Any] | None = None,
    ) -> StoredSession:
        """Atomically append an event, update session state, and merge scoped deltas."""
        if (event_record["app_name"], event_record["user_id"], event_record["session_id"]) != (
            app_name,
            user_id,
            session_id,
        ):
            msg = "event_record scope does not match target session arguments."
            raise ValueError(msg)
        update_sql, update_params = self._update_session_state_sql_and_params(app_name, user_id, session_id, state)
        select_sql = self._get_session_sql()
        select_params = {"app_name": app_name, "user_id": user_id, "id": session_id}
        with self._config.provide_session() as driver:
            driver.execute(update_sql, update_params)
            row = driver.select_one_or_none(select_sql, select_params)
            if row is None:
                msg = f"Session {session_id!r} not found during append_event_and_update_state."
                raise ValueError(msg)
            self._insert_event_with_driver(driver, event_record)
            merged_app = (
                self._apply_state_delta_with_driver(
                    driver, self._app_state_table, ("app_name",), {"app_name": app_name}, app_state_delta
                )
                if app_state_delta
                else None
            )
            merged_user = (
                self._apply_state_delta_with_driver(
                    driver,
                    self._user_state_table,
                    ("app_name", "user_id"),
                    {"app_name": app_name, "user_id": user_id},
                    user_state_delta,
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
        """Fetch Oracle ADK events ordered chronologically."""
        if limit == 0:
            return []
        sql, params = self._get_events_sql_and_params(app_name, user_id, session_id, after_timestamp, limit)
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, params)
            if limit is not None:
                rows = list(reversed(rows))
            return [_row_to_stored_event(row, deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return []
            raise

    def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Delete Oracle events older than ``before``."""
        sql, params = self._delete_before_sql_and_params(self._events_table, "timestamp", before, app_name)
        try:
            with self._config.provide_session() as driver:
                res = driver.execute(sql, params)
                driver.commit()
                return int(getattr(res, "rows_affected", 0) or 0)
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return 0
            raise

    def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete user states whose ``update_time`` predates ``updated_before``."""
        sql, params = self._delete_before_sql_and_params(
            self._user_state_table, "update_time", updated_before, app_name
        )
        try:
            with self._config.provide_session() as driver:
                res = driver.execute(sql, params)
                driver.commit()
                return int(getattr(res, "rows_affected", 0) or 0)
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return 0
            raise

    def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete sessions whose ``update_time`` predates ``updated_before``."""
        sql, params = self._delete_before_sql_and_params(self._session_table, "update_time", updated_before, app_name)
        try:
            with self._config.provide_session() as driver:
                res = driver.execute(sql, params)
                driver.commit()
                return int(getattr(res, "rows_affected", 0) or 0)
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return 0
            raise

    def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Fetch app-scoped ADK state from Oracle."""
        try:
            with self._provide_replica_session(primary=False) as driver:
                return self._select_state_with_driver(
                    driver, self._app_state_table, ("app_name",), {"app_name": app_name}
                )
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise

    def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Fetch user-scoped ADK state from Oracle."""
        try:
            with self._provide_replica_session(primary=False) as driver:
                return self._select_state_with_driver(
                    driver,
                    self._user_state_table,
                    ("app_name", "user_id"),
                    {"app_name": app_name, "user_id": user_id},
                )
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise

    def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Upsert app-scoped ADK state via ``MERGE INTO``."""
        sql, params = self._upsert_app_state_sql_and_params(app_name, state)
        with self._config.provide_session() as driver:
            driver.execute(sql, params)
            driver.commit()

    def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Upsert user-scoped ADK state via ``MERGE INTO``."""
        sql, params = self._upsert_user_state_sql_and_params(app_name, user_id, state)
        with self._config.provide_session() as driver:
            driver.execute(sql, params)
            driver.commit()

    def get_metadata(self, key: str) -> str | None:
        """Fetch an internal metadata entry by key."""
        try:
            with self._provide_replica_session(primary=False) as driver:
                row = driver.select_one_or_none(self._get_metadata_sql(), {"key": key})
            return str(oracle_row_value(row, "value", 0)) if row is not None else None
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise

    def set_metadata(self, key: str, value: str) -> None:
        """Upsert an internal metadata entry via ``MERGE INTO``."""
        with self._config.provide_session() as driver:
            driver.execute(self._set_metadata_sql(), {"key": key, "value": value})
            driver.commit()


class OracleAsyncADKStore(_OracleADKSessionSQLMixin, BaseAsyncADKStore["OracleAsyncConfig"]):
    """Asynchronous Oracle ADK session and event store using ``MERGE INTO`` and PL/SQL guards."""

    __slots__ = ("_json_deserializer", "_json_serializer", "_json_storage_override")

    extension_config_options = _ORACLE_ADK_EXTENSION_OPTIONS

    def __init__(self, config: OracleAsyncConfig, *, read_config: OracleAsyncConfig | None = None) -> None:
        """Initialize the asynchronous Oracle ADK session store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        self._json_storage_override: str | None = _adk_settings(config).get("json_storage")

    async def _storage_mode(self) -> str:
        return await _detect_storage_mode_async(self._config, self._json_storage_override)

    async def _sessions_table_ddl(self) -> str:
        return self._build_sessions_ddl(await self._storage_mode())

    async def _events_table_ddl(self) -> str:
        return self._build_events_ddl(await self._storage_mode())

    async def _app_states_table_ddl(self) -> str:
        return self._build_app_states_ddl(await self._storage_mode())

    async def _user_states_table_ddl(self) -> str:
        return self._build_user_states_ddl(await self._storage_mode())

    async def _metadata_table_ddl(self) -> str:
        return self._build_metadata_ddl()

    async def create_tables(self) -> None:
        """Asynchronously create Oracle ADK tables with detected storage type."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        mode = await self._storage_mode()
        async with self._config.provide_session() as driver:
            await driver.execute_script(self._build_sessions_ddl(mode))
            await driver.execute_script(self._build_events_ddl(mode))
            await driver.execute_script(self._build_app_states_ddl(mode))
            await driver.execute_script(self._build_user_states_ddl(mode))
            await driver.execute_script(self._build_metadata_ddl())
            await driver.commit()

    async def _select_state_with_driver(
        self,
        driver: Any,
        table_name: str,
        key_columns: tuple[str, ...],
        key_params: dict[str, Any],
    ) -> dict[str, Any] | None:
        try:
            row = await driver.select_one_or_none(self._select_state_sql(table_name, key_columns), key_params)
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise
        if row is None:
            return None
        return _decode_json_dict(oracle_row_value(row, "state", 0), deserializer=self._json_deserializer)

    async def _apply_state_delta_with_driver(
        self,
        driver: Any,
        table_name: str,
        key_columns: tuple[str, ...],
        key_params: dict[str, Any],
        delta: Mapping[str, Any],
    ) -> dict[str, Any]:
        select_for_update_sql, insert_sql, update_sql = self._delta_state_sqls(table_name, key_columns)
        for _ in range(_MAX_STATE_MERGE_ATTEMPTS):
            row = await driver.select_one_or_none(select_for_update_sql, key_params)
            if row is None:
                merged = dict(delta)
                merged_json = oracle_json_dumps(merged, serializer=self._json_serializer)
                try:
                    await driver.execute(insert_sql, {**key_params, "state": merged_json})
                except Exception as exc:
                    if is_oracle_unique_violation(exc):
                        continue
                    raise
                return merged
            existing = _decode_json_dict(oracle_row_value(row, "state", 0), deserializer=self._json_deserializer)
            merged = {**existing, **delta}
            merged_json = oracle_json_dumps(merged, serializer=self._json_serializer)
            await driver.execute(update_sql, {**key_params, "state": merged_json})
            return merged
        msg = f"Failed to lock and merge Oracle scoped state after {_MAX_STATE_MERGE_ATTEMPTS} attempts."
        raise SerializationConflictError(msg)

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
        """Asynchronously create an Oracle ADK session and atomically apply optional scoped deltas."""
        insert_sql, insert_params = self._create_session_sql_and_params(session_id, app_name, user_id, state, owner_id)
        async with self._config.provide_session() as driver:
            await driver.execute(insert_sql, insert_params)
            resolved_app = (
                await self._apply_state_delta_with_driver(
                    driver, self._app_state_table, ("app_name",), {"app_name": app_name}, app_state_delta
                )
                if app_state_delta
                else await self._select_state_with_driver(
                    driver, self._app_state_table, ("app_name",), {"app_name": app_name}
                )
            )
            resolved_user = (
                await self._apply_state_delta_with_driver(
                    driver,
                    self._user_state_table,
                    ("app_name", "user_id"),
                    {"app_name": app_name, "user_id": user_id},
                    user_state_delta,
                )
                if user_state_delta
                else await self._select_state_with_driver(
                    driver,
                    self._user_state_table,
                    ("app_name", "user_id"),
                    {"app_name": app_name, "user_id": user_id},
                )
            )
            await driver.commit()
        with adk_primary_read_scope():
            created = await self.get_session(app_name, user_id, session_id)
        if created is None:
            msg = f"Failed to reload newly created ADK session {session_id!r}"
            raise RuntimeError(msg)
        created["state"] = merge_scoped_state(created["state"], resolved_app, resolved_user)
        return created

    async def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: int | timedelta | None = None
    ) -> StoredSession | None:
        """Asynchronously fetch an Oracle ADK session row."""
        should_touch = renew_for is not None and self._calculate_expires_at(renew_for) is not None
        params = {"app_name": app_name, "user_id": user_id, "id": session_id}
        try:
            async with self._provide_replica_session(primary=should_touch) as driver:
                if should_touch:
                    await driver.execute(self._touch_session_sql(), params)
                    await driver.commit()
                row = await driver.select_one_or_none(self._get_session_sql(), params)
                return _row_to_stored_session(row, deserializer=self._json_deserializer) if row is not None else None
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Asynchronously update Oracle ADK session state."""
        sql, params = self._update_session_state_sql_and_params(app_name, user_id, session_id, state)
        async with self._config.provide_session() as driver:
            await driver.execute(sql, params)
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
        """Asynchronously list Oracle ADK sessions."""
        built = self._list_sessions_sql_and_params(
            app_name, user_id, order_by=order_by, descending=descending, limit=limit, offset=offset
        )
        if built is None:
            return []
        sql, params = built
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, params)
            return [_row_to_stored_session(row, deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return []
            raise

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Asynchronously delete an ADK session row."""
        sql = self._delete_session_sql()
        async with self._config.provide_session() as driver:
            await driver.execute(sql, {"app_name": app_name, "user_id": user_id, "id": session_id})
            await driver.commit()

    async def _insert_event_with_driver(self, driver: Any, event_record: StoredEvent) -> None:
        sql, params = self._insert_event_sql_and_params(event_record)
        try:
            await driver.execute(sql, params)
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
        app_state_delta: dict[str, Any] | None = None,
        user_state_delta: dict[str, Any] | None = None,
    ) -> StoredSession:
        """Atomically append an event, update session state, and merge scoped deltas asynchronously."""
        if (event_record["app_name"], event_record["user_id"], event_record["session_id"]) != (
            app_name,
            user_id,
            session_id,
        ):
            msg = "event_record scope does not match target session arguments."
            raise ValueError(msg)
        update_sql, update_params = self._update_session_state_sql_and_params(app_name, user_id, session_id, state)
        select_sql = self._get_session_sql()
        select_params = {"app_name": app_name, "user_id": user_id, "id": session_id}
        async with self._config.provide_session() as driver:
            await driver.execute(update_sql, update_params)
            row = await driver.select_one_or_none(select_sql, select_params)
            if row is None:
                msg = f"Session {session_id!r} not found during append_event_and_update_state."
                raise ValueError(msg)
            await self._insert_event_with_driver(driver, event_record)
            merged_app = (
                await self._apply_state_delta_with_driver(
                    driver, self._app_state_table, ("app_name",), {"app_name": app_name}, app_state_delta
                )
                if app_state_delta
                else None
            )
            merged_user = (
                await self._apply_state_delta_with_driver(
                    driver,
                    self._user_state_table,
                    ("app_name", "user_id"),
                    {"app_name": app_name, "user_id": user_id},
                    user_state_delta,
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
        """Asynchronously retrieve Oracle ADK events."""
        if limit == 0:
            return []
        sql, params = self._get_events_sql_and_params(app_name, user_id, session_id, after_timestamp, limit)
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, params)
            if limit is not None:
                rows = list(reversed(rows))
            return [_row_to_stored_event(row, deserializer=self._json_deserializer) for row in rows]
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return []
            raise

    async def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Asynchronously delete events older than ``before``."""
        sql, params = self._delete_before_sql_and_params(self._events_table, "timestamp", before, app_name)
        try:
            async with self._config.provide_session() as driver:
                res = await driver.execute(sql, params)
                await driver.commit()
                return int(getattr(res, "rows_affected", 0) or 0)
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return 0
            raise

    async def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Asynchronously delete idle user states."""
        sql, params = self._delete_before_sql_and_params(
            self._user_state_table, "update_time", updated_before, app_name
        )
        try:
            async with self._config.provide_session() as driver:
                res = await driver.execute(sql, params)
                await driver.commit()
                return int(getattr(res, "rows_affected", 0) or 0)
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return 0
            raise

    async def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Asynchronously delete idle sessions."""
        sql, params = self._delete_before_sql_and_params(self._session_table, "update_time", updated_before, app_name)
        try:
            async with self._config.provide_session() as driver:
                res = await driver.execute(sql, params)
                await driver.commit()
                return int(getattr(res, "rows_affected", 0) or 0)
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return 0
            raise

    async def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Asynchronously fetch app state."""
        try:
            async with self._provide_replica_session(primary=False) as driver:
                return await self._select_state_with_driver(
                    driver, self._app_state_table, ("app_name",), {"app_name": app_name}
                )
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise

    async def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Asynchronously fetch user state."""
        try:
            async with self._provide_replica_session(primary=False) as driver:
                return await self._select_state_with_driver(
                    driver,
                    self._user_state_table,
                    ("app_name", "user_id"),
                    {"app_name": app_name, "user_id": user_id},
                )
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise

    async def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Asynchronously upsert app state via ``MERGE INTO``."""
        sql, params = self._upsert_app_state_sql_and_params(app_name, state)
        async with self._config.provide_session() as driver:
            await driver.execute(sql, params)
            await driver.commit()

    async def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Asynchronously upsert user state via ``MERGE INTO``."""
        sql, params = self._upsert_user_state_sql_and_params(app_name, user_id, state)
        async with self._config.provide_session() as driver:
            await driver.execute(sql, params)
            await driver.commit()

    async def get_metadata(self, key: str) -> str | None:
        """Asynchronously fetch an internal metadata value."""
        try:
            async with self._provide_replica_session(primary=False) as driver:
                row = await driver.select_one_or_none(self._get_metadata_sql(), {"key": key})
            return str(oracle_row_value(row, "value", 0)) if row is not None else None
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise

    async def set_metadata(self, key: str, value: str) -> None:
        """Asynchronously upsert an internal metadata value."""
        async with self._config.provide_session() as driver:
            await driver.execute(self._set_metadata_sql(), {"key": key, "value": value})
            await driver.commit()


class _OracleADKMemorySQLMixin:
    """Shared Oracle DDL and named-bind SQL builders for sync/async ADK memory stores."""

    __slots__ = ()

    if TYPE_CHECKING:
        _config: Any
        _enabled: bool
        _fts_operator: str
        _json_deserializer: Any
        _json_serializer: Any
        _max_results: int
        _memory_table: str
        _owner_id_column_ddl: str | None
        _owner_id_column_name: str | None
        _use_fts: bool
        _vector_dim_override: int | None

    def _build_memory_table_ddl(self, storage_mode: str) -> str:
        return _memory_table_ddl(
            self._memory_table,
            self._owner_id_column_ddl,
            storage_mode,
            _adk_settings(self._config),
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

    def _build_insert_sql_and_params(self, entry: StoredMemory, owner_id: object | None) -> tuple[str, dict[str, Any]]:
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

    def _build_search_sql_and_params(
        self,
        query: str,
        app_name: str,
        user_id: str,
        limit: int | None,
        scope_filter: Literal["all", "user", "app"],
        embedding: Sequence[float] | None,
    ) -> tuple[str, dict[str, Any], bool, int] | None:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        has_query = bool(query and query.strip())
        if not has_query and embedding is None:
            return None
        effective_limit = limit if limit is not None else self._max_results
        if effective_limit <= 0:
            return None
        table_ref = oracle_table_ref(self._memory_table)
        where_scope, scope_params = _build_memory_scope_where(app_name, user_id, scope_filter)
        emb_col = "embedding" if self._vector_dim_override else "NULL AS embedding"

        if embedding is not None and self._vector_dim_override:
            vec_bind = array.array("f", [float(x) for x in embedding])
            if self._use_fts and has_query:
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
                return (
                    sql,
                    {**scope_params, "query_vector": vec_bind, "text_query": fts_query, "top_k": effective_limit},
                    False,
                    effective_limit,
                )
            sql = (
                f"SELECT id, session_id, app_name, user_id, scope, event_id, author, "
                f"timestamp, content_json, content_text, metadata_json, inserted_at, {emb_col} "
                f"FROM {table_ref} WHERE {where_scope} AND embedding IS NOT NULL "
                f"ORDER BY VECTOR_DISTANCE(embedding, :query_vector, COSINE) ASC "
                f"FETCH FIRST :top_k ROWS ONLY"
            )
            return sql, {**scope_params, "query_vector": vec_bind, "top_k": effective_limit}, False, effective_limit

        if self._use_fts:
            fts_query = sanitize_oracle_text_query(query, operator=self._fts_operator)
            if not fts_query:
                return None
            sql = (
                f"SELECT id, session_id, app_name, user_id, scope, event_id, author, "
                f"timestamp, content_json, content_text, metadata_json, inserted_at, {emb_col} "
                f"FROM {table_ref} WHERE {where_scope} AND CONTAINS(content_text, :text_query, 1) > 0 "
                f"ORDER BY SCORE(1) DESC, timestamp DESC FETCH FIRST :limit_val ROWS ONLY"
            )
            return (
                sql,
                {**scope_params, "text_query": fts_query, "limit_val": effective_limit},
                False,
                effective_limit,
            )

        sql = (
            f"SELECT id, session_id, app_name, user_id, scope, event_id, author, "
            f"timestamp, content_json, content_text, metadata_json, inserted_at, {emb_col} "
            f"FROM {table_ref} WHERE {where_scope} AND LOWER(content_text) LIKE :pattern "
            f"ORDER BY timestamp DESC FETCH FIRST :limit_val ROWS ONLY"
        )
        return (
            sql,
            {**scope_params, "pattern": f"%{query.lower()}%", "limit_val": effective_limit},
            embedding is not None,
            effective_limit,
        )

    def _delete_by_session_sql_and_params(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
    ) -> tuple[str, dict[str, str]]:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        table_ref = oracle_table_ref(self._memory_table)
        sql = f"DELETE FROM {table_ref} WHERE session_id = :session_id AND app_name = :app_name AND user_id = :user_id"
        return sql, {"session_id": session_id, "app_name": app_name, "user_id": user_id}

    def _delete_older_than_sql_and_params(
        self,
        days: int,
        app_name: str | None,
        scope: str | None,
    ) -> tuple[str, dict[str, Any]]:
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
        return f"DELETE FROM {table_ref} WHERE {' AND '.join(clauses)}", params


class OracleSyncADKMemoryStore(_OracleADKMemorySQLMixin, _BaseSyncADKMemoryStore["OracleSyncConfig"]):
    """Synchronous Oracle ADK memory store with 23ai ``VECTOR_DISTANCE``, RRF fusion, and Oracle Text FTS."""

    __slots__ = (
        "_fts_operator",
        "_json_deserializer",
        "_json_serializer",
        "_json_storage_override",
        "_retention_days",
        "_vector_dim_override",
    )

    extension_config_options = _ORACLE_ADK_EXTENSION_OPTIONS

    def __init__(self, config: OracleSyncConfig, *, read_config: OracleSyncConfig | None = None) -> None:
        """Initialize the synchronous Oracle ADK memory store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        adk_cfg = _adk_settings(config)
        if "use_fts" in adk_cfg and "memory_use_fts" not in adk_cfg:
            self._use_fts = bool(adk_cfg["use_fts"])
        dims = adk_cfg.get("vector_dimensions")
        self._vector_dim_override: int | None = int(dims) if dims is not None else None
        self._json_storage_override: str | None = adk_cfg.get("json_storage")
        self._fts_operator: str = str(adk_cfg.get("fts_operator", "ACCUM"))
        retention = adk_cfg.get("retention_days", adk_cfg.get("memory_retention_days"))
        self._retention_days: int | None = int(retention) if retention is not None else None

    def _storage_mode(self) -> str:
        return _detect_storage_mode_sync(self._config, self._json_storage_override)

    def _memory_table_ddl(self) -> str:
        return self._build_memory_table_ddl(self._storage_mode())

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

    def insert_memory_entries(self, entries: list[StoredMemory], owner_id: object | None = None) -> int:
        """Bulk-insert memory entries ignoring ``ORA-00001`` duplicate ``event_id`` violations."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0
        inserted = 0
        with self._config.provide_session() as driver:
            for entry in entries:
                sql, params = self._build_insert_sql_and_params(entry, owner_id)
                try:
                    res = driver.execute(sql, params)
                    inserted += int(getattr(res, "rows_affected", 1) or 1)
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
        """Search Oracle memory entries using 23ai ``VECTOR_DISTANCE``, RRF, Oracle Text FTS, or ``LIKE``."""
        built = self._build_search_sql_and_params(query, app_name, user_id, limit, scope_filter, embedding)
        if built is None:
            return []
        sql, params, post_sort_cosine, effective_limit = built
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, params)
            records = [_row_to_stored_memory(r, deserializer=self._json_deserializer) for r in rows]
            if post_sort_cosine and embedding is not None:
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

    def delete_entries_by_session(self, session_id: str, app_name: str, user_id: str) -> int:
        """Delete all Oracle memory rows for ``session_id`` owned by ``app_name`` and ``user_id``."""
        sql, params = self._delete_by_session_sql_and_params(session_id, app_name, user_id)
        try:
            with self._config.provide_session() as driver:
                res = driver.execute(sql, params)
                driver.commit()
                return int(getattr(res, "rows_affected", 0) or 0)
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return 0
            raise

    def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = "all") -> int:
        """Delete Oracle memory entries older than ``days`` days."""
        sql, params = self._delete_older_than_sql_and_params(days, app_name, scope)
        try:
            with self._config.provide_session() as driver:
                res = driver.execute(sql, params)
                driver.commit()
                return int(getattr(res, "rows_affected", 0) or 0)
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return 0
            raise


class OracleAsyncADKMemoryStore(_OracleADKMemorySQLMixin, _BaseAsyncADKMemoryStore["OracleAsyncConfig"]):
    """Asynchronous Oracle ADK memory store supporting 23ai vectors, RRF fusion, and Oracle Text."""

    __slots__ = (
        "_fts_operator",
        "_json_deserializer",
        "_json_serializer",
        "_json_storage_override",
        "_retention_days",
        "_vector_dim_override",
    )

    extension_config_options = _ORACLE_ADK_EXTENSION_OPTIONS

    def __init__(self, config: OracleAsyncConfig, *, read_config: OracleAsyncConfig | None = None) -> None:
        """Initialize the asynchronous Oracle ADK memory store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        adk_cfg = _adk_settings(config)
        if "use_fts" in adk_cfg and "memory_use_fts" not in adk_cfg:
            self._use_fts = bool(adk_cfg["use_fts"])
        dims = adk_cfg.get("vector_dimensions")
        self._vector_dim_override: int | None = int(dims) if dims is not None else None
        self._json_storage_override: str | None = adk_cfg.get("json_storage")
        self._fts_operator: str = str(adk_cfg.get("fts_operator", "ACCUM"))
        retention = adk_cfg.get("retention_days", adk_cfg.get("memory_retention_days"))
        self._retention_days: int | None = int(retention) if retention is not None else None

    async def _memory_table_ddl(self) -> str:
        mode = await _detect_storage_mode_async(self._config, self._json_storage_override)
        return self._build_memory_table_ddl(mode)

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
        inserted = 0
        async with self._config.provide_session() as driver:
            for entry in entries:
                sql, params = self._build_insert_sql_and_params(entry, owner_id)
                try:
                    res = await driver.execute(sql, params)
                    inserted += int(getattr(res, "rows_affected", 1) or 1)
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
        """Asynchronously search Oracle memory entries via vectors, RRF, FTS, or ``LIKE``."""
        built = self._build_search_sql_and_params(query, app_name, user_id, limit, scope_filter, embedding)
        if built is None:
            return []
        sql, params, post_sort_cosine, effective_limit = built
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, params)
            records = [_row_to_stored_memory(r, deserializer=self._json_deserializer) for r in rows]
            if post_sort_cosine and embedding is not None:
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

    async def delete_entries_by_session(self, session_id: str, app_name: str, user_id: str) -> int:
        """Asynchronously delete memory entries for ``session_id`` owned by ``app_name`` and ``user_id``."""
        sql, params = self._delete_by_session_sql_and_params(session_id, app_name, user_id)
        try:
            async with self._config.provide_session() as driver:
                res = await driver.execute(sql, params)
                await driver.commit()
                return int(getattr(res, "rows_affected", 0) or 0)
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return 0
            raise

    async def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = "all") -> int:
        """Asynchronously delete memory entries older than ``days`` days."""
        sql, params = self._delete_older_than_sql_and_params(days, app_name, scope)
        try:
            async with self._config.provide_session() as driver:
                res = await driver.execute(sql, params)
                await driver.commit()
                return int(getattr(res, "rows_affected", 0) or 0)
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return 0
            raise


class _OracleADKArtifactSQLMixin:
    """Shared Oracle DDL and named-bind SQL builders for sync/async ADK artifact stores."""

    __slots__ = ()

    if TYPE_CHECKING:
        _artifact_table: str
        _json_deserializer: Any
        _json_serializer: Any

    def _build_artifact_table_ddl(self, storage_mode: str) -> str:
        return _artifact_table_ddl(self._artifact_table, storage_mode)

    def _insert_artifact_sql_and_params(self, record: StoredArtifact) -> tuple[str, dict[str, Any]]:
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
        params: dict[str, Any] = {
            "app_name": record["app_name"],
            "user_id": record["user_id"],
            "session_id": record.get("session_id") or "",
            "filename": record["filename"],
            "version": record["version"],
            "mime_type": record.get("mime_type"),
            "canonical_uri": record["canonical_uri"],
            "custom_metadata": meta_raw,
            "created_at": oracle_ensure_utc(record.get("created_at")),
        }
        return sql, params

    def _insert_artifact_ref_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        reference: ArtifactReference,
        session_id: str | None,
        custom_metadata: dict[str, Any] | None,
    ) -> tuple[str, dict[str, Any]]:
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
        params: dict[str, Any] = {
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
        }
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
    ) -> tuple[str, dict[str, Any]]:
        table_ref = oracle_table_ref(self._artifact_table)
        cols = (
            "app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
            "custom_metadata, size, updated, signed_url, created_at"
            if include_ref_cols
            else "app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
            "custom_metadata, created_at"
        )
        params: dict[str, Any] = {
            "app_name": app_name,
            "user_id": user_id,
            "session_id": session_id or "",
            "filename": filename,
        }
        if version is not None:
            params["version"] = version
            sql = (
                f"SELECT {cols} FROM {table_ref} "
                "WHERE app_name = :app_name AND user_id = :user_id AND session_id = :session_id "
                "AND filename = :filename AND version = :version"
            )
            return sql, params
        sql = (
            f"SELECT {cols} FROM {table_ref} "
            "WHERE app_name = :app_name AND user_id = :user_id AND session_id = :session_id "
            "AND filename = :filename ORDER BY version DESC FETCH FIRST 1 ROWS ONLY"
        )
        return sql, params

    def _list_keys_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        session_id: str | None,
    ) -> tuple[str, dict[str, Any]]:
        table_ref = oracle_table_ref(self._artifact_table)
        if session_id is not None:
            sql = (
                f"SELECT DISTINCT filename FROM {table_ref} "
                "WHERE app_name = :app_name AND user_id = :user_id "
                "AND session_id IN ('', :session_id) ORDER BY filename ASC"
            )
            return sql, {"app_name": app_name, "user_id": user_id, "session_id": session_id}
        sql = (
            f"SELECT DISTINCT filename FROM {table_ref} "
            "WHERE app_name = :app_name AND user_id = :user_id AND session_id = '' ORDER BY filename ASC"
        )
        return sql, {"app_name": app_name, "user_id": user_id}

    def _list_versions_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None,
    ) -> tuple[str, dict[str, str]]:
        table_ref = oracle_table_ref(self._artifact_table)
        sql = (
            f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
            f"custom_metadata, created_at FROM {table_ref} "
            "WHERE app_name = :app_name AND user_id = :user_id AND session_id = :session_id "
            "AND filename = :filename ORDER BY version ASC"
        )
        return sql, {"app_name": app_name, "user_id": user_id, "session_id": session_id or "", "filename": filename}

    def _delete_artifact_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None,
    ) -> tuple[str, dict[str, str]]:
        table_ref = oracle_table_ref(self._artifact_table)
        sql = (
            f"DELETE FROM {table_ref} WHERE app_name = :app_name AND user_id = :user_id "
            "AND session_id = :session_id AND filename = :filename"
        )
        return sql, {"app_name": app_name, "user_id": user_id, "session_id": session_id or "", "filename": filename}

    def _get_next_version_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None,
    ) -> tuple[str, dict[str, str]]:
        table_ref = oracle_table_ref(self._artifact_table)
        sql = (
            f"SELECT MAX(version) FROM {table_ref} "
            "WHERE app_name = :app_name AND user_id = :user_id AND session_id = :session_id AND filename = :filename"
        )
        return sql, {"app_name": app_name, "user_id": user_id, "session_id": session_id or "", "filename": filename}

    def _delete_older_than_sqls_and_params(
        self,
        before: datetime,
        app_name: str | None,
    ) -> tuple[str, str, dict[str, Any]]:
        table_ref = oracle_table_ref(self._artifact_table)
        cutoff = oracle_ensure_utc(before)
        cols = "app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at"
        if app_name is not None:
            select_sql = (
                f"SELECT {cols} FROM {table_ref} "
                "WHERE created_at < :before_ts AND app_name = :app_name ORDER BY created_at ASC"
            )
            delete_sql = f"DELETE FROM {table_ref} WHERE created_at < :before_ts AND app_name = :app_name"
            return select_sql, delete_sql, {"before_ts": cutoff, "app_name": app_name}
        select_sql = f"SELECT {cols} FROM {table_ref} WHERE created_at < :before_ts ORDER BY created_at ASC"
        delete_sql = f"DELETE FROM {table_ref} WHERE created_at < :before_ts"
        return select_sql, delete_sql, {"before_ts": cutoff}


class OracleSyncADKArtifactStore(_OracleADKArtifactSQLMixin, _BaseSyncADKArtifactStore["OracleSyncConfig"]):
    """Synchronous Oracle ADK artifact metadata store returning ``ArtifactReference``."""

    __slots__ = ("_json_deserializer", "_json_serializer", "_json_storage_override")

    def __init__(self, config: OracleSyncConfig, *, read_config: OracleSyncConfig | None = None) -> None:
        """Initialize synchronous Oracle artifact metadata store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        self._json_storage_override: str | None = _adk_settings(config).get("json_storage")

    def create_table(self) -> None:
        """Create the Oracle artifact metadata table idempotently."""
        mode = _detect_storage_mode_sync(self._config, self._json_storage_override)
        with self._config.provide_session() as driver:
            driver.execute_script(self._build_artifact_table_ddl(mode))
            driver.commit()

    def insert_artifact(self, record: StoredArtifact) -> None:
        """Upsert an ADK artifact metadata entry using Oracle ``MERGE INTO``."""
        sql, params = self._insert_artifact_sql_and_params(record)
        with self._config.provide_session() as driver:
            driver.execute(sql, params)
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
        """Persist a ``vertebra.storage.ArtifactReference`` record into Oracle."""
        sql, params = self._insert_artifact_ref_sql_and_params(
            app_name, user_id, filename, reference, session_id, custom_metadata
        )
        with self._config.provide_session() as driver:
            driver.execute(sql, params)
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
        sql, params = self._get_artifact_sql_and_params(
            app_name, user_id, filename, session_id, version, include_ref_cols=False
        )
        try:
            with self._provide_replica_session(primary=False) as driver:
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
        """Fetch stored artifact metadata as a ``vertebra.storage.ArtifactReference``."""
        sql, params = self._get_artifact_sql_and_params(
            app_name, user_id, filename, session_id, version, include_ref_cols=True
        )
        try:
            with self._provide_replica_session(primary=False) as driver:
                row = driver.select_one_or_none(sql, params)
            return _row_to_artifact_reference(row) if row is not None else None
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise

    def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """List distinct artifact filenames for ``(app_name, user_id)`` across user and optional session scope."""
        sql, params = self._list_keys_sql_and_params(app_name, user_id, session_id)
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, params)
            return [str(oracle_row_value(r, "filename", 0)) for r in rows]
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return []
            raise

    def list_artifact_versions(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None
    ) -> list[StoredArtifact]:
        """List ascending ``StoredArtifact`` versions for a stored artifact."""
        sql, params = self._list_versions_sql_and_params(app_name, user_id, filename, session_id)
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, params)
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
        sql, params = self._delete_artifact_sql_and_params(app_name, user_id, filename, session_id)
        with self._config.provide_session() as driver:
            driver.execute(sql, params)
            driver.commit()
        return existing

    def get_next_version(self, app_name: str, user_id: str, filename: str, session_id: str | None = None) -> int:
        """Compute the next sequential version integer (``MAX(version) + 1`` or ``0``)."""
        sql, params = self._get_next_version_sql_and_params(app_name, user_id, filename, session_id)
        with self._config.provide_session() as driver:
            val = driver.select_value_or_none(sql, params)
        return 0 if val is None else int(val) + 1

    def delete_artifacts_older_than(self, before: datetime, app_name: str | None = None) -> list[StoredArtifact]:
        """Delete artifact rows older than ``before`` and return removed records."""
        select_sql, delete_sql, params = self._delete_older_than_sqls_and_params(before, app_name)
        try:
            with self._config.provide_session() as driver:
                rows = driver.select(select_sql, params)
                driver.execute(delete_sql, params)
                driver.commit()
            return [_row_to_stored_artifact(r, deserializer=self._json_deserializer) for r in rows]
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return []
            raise


class OracleAsyncADKArtifactStore(_OracleADKArtifactSQLMixin, _BaseAsyncADKArtifactStore["OracleAsyncConfig"]):
    """Asynchronous Oracle ADK artifact metadata store returning ``ArtifactReference``."""

    __slots__ = ("_json_deserializer", "_json_serializer", "_json_storage_override")

    def __init__(self, config: OracleAsyncConfig, *, read_config: OracleAsyncConfig | None = None) -> None:
        """Initialize asynchronous Oracle artifact metadata store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        self._json_storage_override: str | None = _adk_settings(config).get("json_storage")

    async def create_table(self) -> None:
        """Asynchronously create Oracle artifact metadata table idempotently."""
        mode = await _detect_storage_mode_async(self._config, self._json_storage_override)
        async with self._config.provide_session() as driver:
            await driver.execute_script(self._build_artifact_table_ddl(mode))
            await driver.commit()

    async def insert_artifact(self, record: StoredArtifact) -> None:
        """Asynchronously upsert an ADK artifact metadata entry using ``MERGE INTO``."""
        sql, params = self._insert_artifact_sql_and_params(record)
        async with self._config.provide_session() as driver:
            await driver.execute(sql, params)
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
        """Asynchronously store a ``vertebra.storage.ArtifactReference`` record into Oracle."""
        sql, params = self._insert_artifact_ref_sql_and_params(
            app_name, user_id, filename, reference, session_id, custom_metadata
        )
        async with self._config.provide_session() as driver:
            await driver.execute(sql, params)
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
        sql, params = self._get_artifact_sql_and_params(
            app_name, user_id, filename, session_id, version, include_ref_cols=False
        )
        try:
            async with self._provide_replica_session(primary=False) as driver:
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
        """Asynchronously fetch stored artifact metadata as ``ArtifactReference``."""
        sql, params = self._get_artifact_sql_and_params(
            app_name, user_id, filename, session_id, version, include_ref_cols=True
        )
        try:
            async with self._provide_replica_session(primary=False) as driver:
                row = await driver.select_one_or_none(sql, params)
            return _row_to_artifact_reference(row) if row is not None else None
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return None
            raise

    async def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """Asynchronously list distinct artifact filenames across user and optional session scope."""
        sql, params = self._list_keys_sql_and_params(app_name, user_id, session_id)
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, params)
            return [str(oracle_row_value(r, "filename", 0)) for r in rows]
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return []
            raise

    async def list_artifact_versions(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None
    ) -> list[StoredArtifact]:
        """Asynchronously list ascending ``StoredArtifact`` versions for a stored artifact."""
        sql, params = self._list_versions_sql_and_params(app_name, user_id, filename, session_id)
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, params)
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
        sql, params = self._delete_artifact_sql_and_params(app_name, user_id, filename, session_id)
        async with self._config.provide_session() as driver:
            await driver.execute(sql, params)
            await driver.commit()
        return existing

    async def get_next_version(self, app_name: str, user_id: str, filename: str, session_id: str | None = None) -> int:
        """Asynchronously compute the next sequential version integer."""
        sql, params = self._get_next_version_sql_and_params(app_name, user_id, filename, session_id)
        async with self._config.provide_session() as driver:
            val = await driver.select_value_or_none(sql, params)
        return 0 if val is None else int(val) + 1

    async def delete_artifacts_older_than(self, before: datetime, app_name: str | None = None) -> list[StoredArtifact]:
        """Asynchronously delete artifact rows older than ``before`` and return removed records."""
        select_sql, delete_sql, params = self._delete_older_than_sqls_and_params(before, app_name)
        try:
            async with self._config.provide_session() as driver:
                rows = await driver.select(select_sql, params)
                await driver.execute(delete_sql, params)
                await driver.commit()
            return [_row_to_stored_artifact(r, deserializer=self._json_deserializer) for r in rows]
        except Exception as exc:
            if is_oracle_table_missing(exc):
                return []
            raise
