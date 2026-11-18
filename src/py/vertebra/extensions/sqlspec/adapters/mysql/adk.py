"""MySQL and MariaDB ADK session, event, memory (`FULLTEXT`/native `VECTOR`), and artifact metadata stores."""

from __future__ import annotations

import contextlib
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, NotRequired, cast

from sqlspec.config import ADKConfig
from sqlspec.extensions.adk import (
    BaseAsyncADKStore,
    BaseSyncADKStore,
    StoredEvent,
    StoredSession,
    normalize_session_list_options,
)
from sqlspec.utils.serializers import from_json, to_json

from vertebra.extensions.sqlspec.adapters.mysql.core import (
    build_mysql_scope_where,
    cosine_similarity,
    format_mysql_vector_literal,
    is_mysql_table_missing,
    mysql_json_dict,
    mysql_json_for_storage,
    mysql_owner_id_column_parts,
    mysql_supports_native_vector,
    mysql_table_options,
    parse_mysql_vector,
)
from vertebra.storage import ArtifactReference
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from sqlspec.extensions.adk import SessionOrderBy, StoredMemory
    from sqlspec.extensions.adk.artifact._types import StoredArtifact
    from sqlspec.extensions.adk.artifact.store import BaseAsyncADKArtifactStore as _BaseAsyncADKArtifactStore
    from sqlspec.extensions.adk.artifact.store import BaseSyncADKArtifactStore as _BaseSyncADKArtifactStore
    from sqlspec.extensions.adk.converters import merge_scoped_state
    from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore as _BaseAsyncADKMemoryStore
    from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore as _BaseSyncADKMemoryStore

    from vertebra.extensions.sqlspec.adapters.mysql.config import MySQLAsyncConfig, MySQLSyncConfig
else:
    _memory_store_mod = ensure_module("sqlspec.extensions.adk.memory.store", package="google-adk", extra="adk")
    _artifact_store_mod = ensure_module("sqlspec.extensions.adk.artifact.store", package="google-adk", extra="adk")
    _converters_mod = ensure_module("sqlspec.extensions.adk.converters", package="google-adk", extra="adk")
    _artifact_types_mod = ensure_module("sqlspec.extensions.adk.artifact._types", package="google-adk", extra="adk")
    _memory_types_mod = ensure_module("sqlspec.extensions.adk.memory._types", package="google-adk", extra="adk")
    _BaseAsyncADKMemoryStore = _memory_store_mod.BaseAsyncADKMemoryStore
    _BaseSyncADKMemoryStore = _memory_store_mod.BaseSyncADKMemoryStore
    _BaseAsyncADKArtifactStore = _artifact_store_mod.BaseAsyncADKArtifactStore
    _BaseSyncADKArtifactStore = _artifact_store_mod.BaseSyncADKArtifactStore
    merge_scoped_state = _converters_mod.merge_scoped_state
    StoredArtifact = _artifact_types_mod.StoredArtifact
    StoredMemory = _memory_types_mod.StoredMemory

__all__ = (
    "MySQLADKConfig",
    "MySQLAsyncADKArtifactStore",
    "MySQLAsyncADKMemoryStore",
    "MySQLAsyncADKStore",
    "MySQLSyncADKArtifactStore",
    "MySQLSyncADKMemoryStore",
    "MySQLSyncADKStore",
)

_EXPLICIT_VECTOR_MODES: Final[frozenset[str]] = frozenset({"mysql_vector", "vector", "native"})
_MYSQL_ADK_EXTENSION_OPTIONS: Final[frozenset[str]] = frozenset(
    {
        "app_state_table_options",
        "artifact_table_options",
        "enable_covering_indexes",
        "enable_event_generated_columns",
        "enable_vector_index",
        "events_table_options",
        "memory_retention_days",
        "memory_table_options",
        "server_version",
        "session_table_options",
        "use_fts",
        "use_vector_type",
        "user_state_table_options",
        "vector_dimensions",
    }
)


class MySQLADKConfig(ADKConfig, total=False):
    """MySQL-specific ADK extension configuration keys under ``extension_config['adk']``."""

    enable_event_generated_columns: NotRequired[bool]
    """Create MySQL generated columns and indexes for event author and node path JSON fields."""

    enable_covering_indexes: NotRequired[bool]
    """Include ``invocation_id`` inside ADK event replay indexes."""

    session_table_options: NotRequired[str]
    """Raw MySQL table options appended to the ADK sessions table DDL."""

    events_table_options: NotRequired[str]
    """Raw MySQL table options appended to the ADK events table DDL."""

    app_state_table_options: NotRequired[str]
    """Raw MySQL table options appended to the ADK app state table DDL."""

    user_state_table_options: NotRequired[str]
    """Raw MySQL table options appended to the ADK user state table DDL."""

    memory_table_options: NotRequired[str]
    """Raw MySQL table options appended to the ADK memory table DDL."""

    artifact_table_options: NotRequired[str]
    """Raw MySQL table options appended to the ADK artifact metadata table DDL."""

    enable_vector_index: NotRequired[bool]
    """Enable native ``VECTOR(dims)`` column and distance search when server supports it."""

    use_vector_type: NotRequired[bool]
    """Enable native ``VECTOR(dims)`` column and SQL cosine distance when supported by server version."""

    vector_dimensions: NotRequired[int]
    """Dimensionality of embedding vectors stored in native ``VECTOR(N)`` columns."""

    memory_retention_days: NotRequired[int]
    """Optional retention window in days for ADK memory entries."""

    use_fts: NotRequired[bool]
    """Optional ``FULLTEXT`` search flag aliased to ``memory_use_fts``."""

    server_version: NotRequired[str]
    """Optional explicit MySQL or MariaDB version string overriding ``SELECT VERSION()`` probing."""


def _extract_adk_config(config: Any) -> dict[str, Any]:
    """Return ``extension_config['adk']`` as a mutable dictionary."""
    ext = getattr(config, "extension_config", {})
    if not isinstance(ext, Mapping):
        return {}
    adk = ext.get("adk", {})
    return dict(adk) if isinstance(adk, Mapping) else {}


def _resolve_json_codecs(config: Any) -> tuple[Any, Any]:
    """Resolve optional JSON serializer and deserializer callables from driver features."""
    features = getattr(config, "driver_features", None)
    if not isinstance(features, Mapping):
        return to_json, from_json
    serializer = features.get("json_serializer")
    deserializer = features.get("json_deserializer")
    return (serializer if callable(serializer) else to_json, deserializer if callable(deserializer) else from_json)


def _naive_utc(value: datetime) -> datetime:
    """Convert an aware datetime into a naive UTC datetime for MySQL timestamp binding."""
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _ensure_utc(value: Any) -> datetime:
    """Normalize a database datetime or ISO string to an aware UTC datetime."""
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value)
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    return datetime.now(UTC)


def _split_table(table: str) -> tuple[str | None, str]:
    """Split an optionally schema-qualified table name into ``(schema, bare_table)``."""
    if "." in table:
        schema_name, bare_table = table.split(".", 1)
        return schema_name, bare_table
    return None, table


def _index_name(table: str, suffix: str) -> str:
    """Derive an index identifier from the bare table name."""
    _, bare_table = _split_table(table)
    return f"idx_{bare_table}_{suffix}"


def _row_to_session(row: Mapping[str, Any], *, deserializer: Any = from_json) -> StoredSession:
    """Decode a MySQL session row mapping into a ``StoredSession``."""
    return {
        "id": str(row["id"]),
        "app_name": str(row["app_name"]),
        "user_id": str(row["user_id"]),
        "state": mysql_json_dict(row.get("state"), deserializer=deserializer),
        "create_time": _ensure_utc(row["create_time"]),
        "update_time": _ensure_utc(row["update_time"]),
    }


def _row_to_event(row: Mapping[str, Any], *, deserializer: Any = from_json) -> StoredEvent:
    """Decode a MySQL event row mapping into a ``StoredEvent``."""
    return {
        "id": str(row["id"]),
        "app_name": str(row["app_name"]),
        "user_id": str(row["user_id"]),
        "session_id": str(row["session_id"]),
        "invocation_id": str(row["invocation_id"]),
        "timestamp": _ensure_utc(row["timestamp"]),
        "event_data": mysql_json_dict(row.get("event_data"), deserializer=deserializer),
    }


def _memory_row_to_record(row: Mapping[str, Any], *, deserializer: Any = from_json) -> StoredMemory:
    """Decode a MySQL memory row mapping into a ``StoredMemory``."""
    metadata_val = row.get("metadata_json")
    return cast(
        "StoredMemory",
        {
            "id": str(row["id"]),
            "session_id": str(row["session_id"]),
            "app_name": str(row["app_name"]),
            "user_id": str(row["user_id"]),
            "scope": cast("Literal['user', 'app']", str(row.get("scope") or "user")),
            "event_id": str(row["event_id"]),
            "author": str(row["author"]) if row.get("author") is not None else None,
            "timestamp": _ensure_utc(row["timestamp"]),
            "content_json": mysql_json_dict(row.get("content_json"), deserializer=deserializer),
            "content_text": str(row["content_text"]),
            "metadata_json": mysql_json_dict(metadata_val, deserializer=deserializer)
            if metadata_val is not None
            else None,
            "inserted_at": _ensure_utc(row["inserted_at"]),
            "embedding": parse_mysql_vector(row.get("embedding")),
        },
    )


def _row_to_stored_artifact(row: Mapping[str, Any], *, deserializer: Any = from_json) -> StoredArtifact:
    """Decode a MySQL artifact row mapping into a ``StoredArtifact``."""
    session_raw = row.get("session_id")
    custom_raw = row.get("custom_metadata")
    return cast(
        "StoredArtifact",
        {
            "app_name": str(row["app_name"]),
            "user_id": str(row["user_id"]),
            "session_id": str(session_raw) if session_raw else None,
            "filename": str(row["filename"]),
            "version": int(row["version"]),
            "mime_type": str(row["mime_type"]) if row.get("mime_type") is not None else None,
            "canonical_uri": str(row["canonical_uri"]),
            "custom_metadata": mysql_json_dict(custom_raw, deserializer=deserializer)
            if custom_raw is not None
            else None,
            "created_at": _ensure_utc(row["created_at"]),
        },
    )


def _row_to_artifact_reference(row: Mapping[str, Any]) -> ArtifactReference:
    """Decode a MySQL artifact row mapping into an ``ArtifactReference``."""
    raw_updated = row.get("updated")
    return ArtifactReference(
        uri=str(row["canonical_uri"]),
        version=int(row["version"]),
        content_type=str(row["mime_type"]) if row.get("mime_type") is not None else None,
        size=int(row["size"]) if row.get("size") is not None else None,
        updated=_ensure_utc(raw_updated) if raw_updated is not None else None,
        signed_url=str(row["signed_url"]) if row.get("signed_url") is not None else None,
    )


def _item_embedding(item: Mapping[str, Any]) -> Sequence[float] | None:
    """Extract an embedding sequence from a decoded memory entry when present."""
    raw = item.get("embedding")
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        return raw
    return None


class _MySQLADKSessionSQLMixin:
    """Shared MySQL DDL and query builders for synchronous and asynchronous ADK session stores."""

    __slots__ = ()

    _config: Any
    _session_table: str
    _events_table: str
    _app_state_table: str
    _user_state_table: str
    _metadata_table: str
    _owner_id_column_ddl: str | None
    _owner_id_column_name: str | None
    _json_serializer: Any
    _json_deserializer: Any

    def _build_sessions_ddl(self) -> str:
        adk_cfg = _extract_adk_config(self._config)
        table_options = mysql_table_options(adk_cfg, "session_table_options")
        owner_line = ""
        fk_clause = ""
        if self._owner_id_column_ddl:
            col_def, fk_def = mysql_owner_id_column_parts(self._owner_id_column_ddl)
            owner_line = f"\n    {col_def},"
            if fk_def:
                fk_clause = f",\n    {fk_def}"
        idx_app_user = _index_name(self._session_table, "app_user")
        idx_update = _index_name(self._session_table, "update_time")
        return (
            f"CREATE TABLE IF NOT EXISTS {self._session_table} (\n"
            "    id VARCHAR(128) PRIMARY KEY,\n"
            "    app_name VARCHAR(128) NOT NULL,\n"
            f"    user_id VARCHAR(128) NOT NULL,{owner_line}\n"
            "    state JSON NOT NULL,\n"
            "    create_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\n"
            "    update_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n"
            f"    INDEX {idx_app_user} (app_name, user_id),\n"
            f"    INDEX {idx_update} (update_time DESC){fk_clause}\n"
            f") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{table_options};"
        )

    def _build_events_ddl(self) -> str:
        adk_cfg = _extract_adk_config(self._config)
        generated_cols = ""
        generated_idxs = ""
        if adk_cfg.get("enable_event_generated_columns", False):
            generated_cols = (
                ",\n"
                "    author_gc VARCHAR(256) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(event_data, '$.author'))) STORED,\n"
                "    node_path_gc VARCHAR(512) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(event_data, '$.node_info.path'))) STORED"
            )
            generated_idxs = (
                f",\n    INDEX {_index_name(self._events_table, 'author_gc')} (session_id, author_gc, timestamp ASC),\n"
                f"    INDEX {_index_name(self._events_table, 'node_path_gc')} (session_id, node_path_gc, timestamp ASC)"
            )
        covering = ", invocation_id" if adk_cfg.get("enable_covering_indexes", False) else ""
        table_options = mysql_table_options(adk_cfg, "events_table_options")
        return (
            f"CREATE TABLE IF NOT EXISTS {self._events_table} (\n"
            "    id VARCHAR(128) PRIMARY KEY,\n"
            "    app_name VARCHAR(128) NOT NULL,\n"
            "    user_id VARCHAR(128) NOT NULL,\n"
            "    session_id VARCHAR(128) NOT NULL,\n"
            "    invocation_id VARCHAR(256) NOT NULL,\n"
            "    timestamp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\n"
            f"    event_data JSON NOT NULL{generated_cols},\n"
            f"    FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE,\n"
            f"    INDEX {_index_name(self._events_table, 'scope')} (app_name, user_id, session_id, timestamp ASC{covering}),\n"
            f"    INDEX {_index_name(self._events_table, 'session')} (session_id, timestamp ASC{covering}),\n"
            f"    INDEX {_index_name(self._events_table, 'app_timestamp')} (app_name, timestamp ASC){generated_idxs}\n"
            f") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{table_options};"
        )

    def _build_app_states_ddl(self) -> str:
        adk_cfg = _extract_adk_config(self._config)
        table_options = mysql_table_options(adk_cfg, "app_state_table_options")
        return (
            f"CREATE TABLE IF NOT EXISTS {self._app_state_table} (\n"
            "    app_name VARCHAR(128) PRIMARY KEY,\n"
            "    state JSON NOT NULL,\n"
            "    update_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)\n"
            f") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{table_options};"
        )

    def _build_user_states_ddl(self) -> str:
        adk_cfg = _extract_adk_config(self._config)
        table_options = mysql_table_options(adk_cfg, "user_state_table_options")
        return (
            f"CREATE TABLE IF NOT EXISTS {self._user_state_table} (\n"
            "    app_name VARCHAR(128) NOT NULL,\n"
            "    user_id VARCHAR(128) NOT NULL,\n"
            "    state JSON NOT NULL,\n"
            "    update_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n"
            "    PRIMARY KEY (app_name, user_id)\n"
            f") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{table_options};"
        )

    def _build_metadata_ddl(self) -> str:
        return (
            f"CREATE TABLE IF NOT EXISTS {self._metadata_table} (\n"
            "    `key` VARCHAR(128) PRIMARY KEY,\n"
            "    value VARCHAR(512) NOT NULL\n"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        )

    def _drop_app_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self._app_state_table}"

    def _drop_user_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self._user_state_table}"

    def _drop_metadata_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self._metadata_table}"

    def _drop_tables_sql(self) -> list[str]:
        return [
            self._drop_metadata_table_sql(),
            self._drop_user_states_table_sql(),
            self._drop_app_states_table_sql(),
            f"DROP TABLE IF EXISTS {self._events_table}",
            f"DROP TABLE IF EXISTS {self._session_table}",
        ]

    def _create_session_sql_and_params(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
        state: dict[str, Any],
        owner_id: Any | None,
    ) -> tuple[str, tuple[Any, ...]]:
        state_json = self._json_serializer(state)
        if self._owner_id_column_name:
            sql = (
                f"INSERT INTO {self._session_table} "
                f"(id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time) "
                "VALUES (?, ?, ?, ?, ?, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))"
            )
            return sql, (session_id, app_name, user_id, owner_id, state_json)
        sql = (
            f"INSERT INTO {self._session_table} "
            "(id, app_name, user_id, state, create_time, update_time) "
            "VALUES (?, ?, ?, ?, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))"
        )
        return sql, (session_id, app_name, user_id, state_json)

    def _get_session_select_sql(self) -> str:
        return (
            f"SELECT id, app_name, user_id, state, create_time, update_time FROM {self._session_table} "
            "WHERE app_name = ? AND user_id = ? AND id = ?"
        )

    def _get_session_touch_sql(self) -> str:
        return (
            f"UPDATE {self._session_table} SET update_time = UTC_TIMESTAMP(6) "
            "WHERE app_name = ? AND user_id = ? AND id = ?"
        )

    def _update_session_state_sql(self) -> str:
        return (
            f"UPDATE {self._session_table} SET state = ?, update_time = UTC_TIMESTAMP(6) "
            "WHERE app_name = ? AND user_id = ? AND id = ?"
        )

    def _list_sessions_sql_and_params(
        self,
        app_name: str,
        user_id: str | None,
        order_by: SessionOrderBy,
        descending: bool,
        limit: int | None,
        offset: int | None,
    ) -> tuple[str, tuple[Any, ...]] | None:
        column, direction, page_limit, page_offset = normalize_session_list_options(order_by, descending, limit, offset)
        if page_limit == 0:
            return None
        params: list[Any] = [app_name]
        where_clause = "app_name = ?"
        if user_id is not None:
            params.append(user_id)
            where_clause = f"{where_clause} AND user_id = ?"
        page_clause = ""
        if page_limit is not None:
            params.extend((page_limit, page_offset))
            page_clause = " LIMIT ? OFFSET ?"
        sql = (
            f"SELECT id, app_name, user_id, state, create_time, update_time FROM {self._session_table} "
            f"WHERE {where_clause} ORDER BY {column} {direction}, id {direction}{page_clause}"
        )
        return sql, tuple(params)

    def _delete_session_sql(self) -> str:
        return f"DELETE FROM {self._session_table} WHERE app_name = ? AND user_id = ? AND id = ?"

    def _append_event_sql_and_params(self, event_record: StoredEvent) -> tuple[str, tuple[Any, ...]]:
        app_name = event_record["app_name"]
        user_id = event_record["user_id"]
        session_id = event_record["session_id"]
        sql = (
            f"INSERT INTO {self._events_table} "
            "(id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
            "SELECT ?, ?, ?, ?, ?, ?, ? "
            f"FROM {self._session_table} "
            "WHERE id = ? AND app_name = ? AND user_id = ?"
        )
        params = (
            event_record["id"],
            app_name,
            user_id,
            session_id,
            event_record["invocation_id"],
            _naive_utc(event_record["timestamp"]),
            mysql_json_for_storage(event_record["event_data"], serializer=self._json_serializer),
            session_id,
            app_name,
            user_id,
        )
        return sql, params

    def _insert_event_sql_and_params(self, event_record: StoredEvent) -> tuple[str, tuple[Any, ...]]:
        sql = (
            f"INSERT INTO {self._events_table} "
            "(id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        params = (
            event_record["id"],
            event_record["app_name"],
            event_record["user_id"],
            event_record["session_id"],
            event_record["invocation_id"],
            _naive_utc(event_record["timestamp"]),
            mysql_json_for_storage(event_record["event_data"], serializer=self._json_serializer),
        )
        return sql, params

    def _get_events_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None,
        limit: int | None,
    ) -> tuple[str, tuple[Any, ...]]:
        clauses = ["s.app_name = ?", "s.user_id = ?", "s.id = ?"]
        params: list[Any] = [app_name, user_id, session_id]
        if after_timestamp is not None:
            clauses.append("e.timestamp > ?")
            params.append(_naive_utc(after_timestamp))
        order_direction = "DESC" if limit is not None else "ASC"
        limit_sql = ""
        if limit is not None:
            limit_sql = " LIMIT ?"
            params.append(int(limit))
        sql = (
            "SELECT e.id, e.app_name, e.user_id, e.session_id, e.invocation_id, e.timestamp, e.event_data "
            f"FROM {self._events_table} e "
            f"JOIN {self._session_table} s "
            "ON e.session_id = s.id AND e.app_name = s.app_name AND e.user_id = s.user_id "
            f"WHERE {' AND '.join(clauses)} ORDER BY e.timestamp {order_direction}{limit_sql}"
        )
        return sql, tuple(params)

    def _delete_before_sql_and_params(
        self,
        table_name: str,
        column_name: str,
        threshold: datetime,
        app_name: str | None,
    ) -> tuple[str, tuple[Any, ...]]:
        sql = f"DELETE FROM {table_name} WHERE {column_name} < ?"
        params: list[Any] = [_naive_utc(threshold)]
        if app_name is not None:
            sql = f"{sql} AND app_name = ?"
            params.append(app_name)
        return sql, tuple(params)

    def _ensure_app_state_row_sql(self) -> str:
        return (
            f"INSERT IGNORE INTO {self._app_state_table} (app_name, state, update_time) VALUES (?, ?, UTC_TIMESTAMP(6))"
        )

    def _select_app_state_sql(self) -> str:
        return f"SELECT state FROM {self._app_state_table} WHERE app_name = ?"

    def _select_app_state_for_update_sql(self) -> str:
        return f"SELECT state FROM {self._app_state_table} WHERE app_name = ? FOR UPDATE"

    def _update_app_state_sql(self) -> str:
        return f"UPDATE {self._app_state_table} SET state = ?, update_time = UTC_TIMESTAMP(6) WHERE app_name = ?"

    def _ensure_user_state_row_sql(self) -> str:
        return (
            f"INSERT IGNORE INTO {self._user_state_table} (app_name, user_id, state, update_time) "
            "VALUES (?, ?, ?, UTC_TIMESTAMP(6))"
        )

    def _select_user_state_sql(self) -> str:
        return f"SELECT state FROM {self._user_state_table} WHERE app_name = ? AND user_id = ?"

    def _select_user_state_for_update_sql(self) -> str:
        return f"SELECT state FROM {self._user_state_table} WHERE app_name = ? AND user_id = ? FOR UPDATE"

    def _update_user_state_sql(self) -> str:
        return (
            f"UPDATE {self._user_state_table} SET state = ?, update_time = UTC_TIMESTAMP(6) "
            "WHERE app_name = ? AND user_id = ?"
        )

    def _upsert_app_state_sql(self) -> str:
        return (
            f"INSERT INTO {self._app_state_table} (app_name, state, update_time) "
            "VALUES (?, ?, UTC_TIMESTAMP(6)) "
            "ON DUPLICATE KEY UPDATE state = VALUES(state), update_time = UTC_TIMESTAMP(6)"
        )

    def _upsert_user_state_sql(self) -> str:
        return (
            f"INSERT INTO {self._user_state_table} (app_name, user_id, state, update_time) "
            "VALUES (?, ?, ?, UTC_TIMESTAMP(6)) "
            "ON DUPLICATE KEY UPDATE state = VALUES(state), update_time = UTC_TIMESTAMP(6)"
        )

    def _get_metadata_sql(self) -> str:
        return f"SELECT value FROM {self._metadata_table} WHERE `key` = ?"

    def _set_metadata_sql(self) -> str:
        return f"INSERT INTO {self._metadata_table} (`key`, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value)"

    @staticmethod
    def _validate_event_scope(event_record: StoredEvent, app_name: str, user_id: str, session_id: str) -> None:
        if (event_record["app_name"], event_record["user_id"], event_record["session_id"]) != (
            app_name,
            user_id,
            session_id,
        ):
            msg = (
                "event_record scope "
                f"({event_record['app_name']!r}, {event_record['user_id']!r}, {event_record['session_id']!r}) "
                f"does not match ({app_name!r}, {user_id!r}, {session_id!r})"
            )
            raise ValueError(msg)


class MySQLSyncADKStore(_MySQLADKSessionSQLMixin, BaseSyncADKStore["MySQLSyncConfig"]):
    """Synchronous MySQL/MariaDB ADK session, event, state, and metadata store using ``?`` binds."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    adapter_name = "vertebra_mysql"
    _max_table_name_parts: ClassVar[int] = 2
    extension_config_options = _MYSQL_ADK_EXTENSION_OPTIONS

    def __init__(self, config: MySQLSyncConfig, *, read_config: MySQLSyncConfig | None = None) -> None:
        """Initialize the synchronous MySQL ADK session/event store."""
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
        """Create ADK session, event, app state, user state, and metadata tables."""
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

    def _select_app_state_sync(self, driver: Any, app_name: str) -> dict[str, Any] | None:
        row = driver.select_one_or_none(self._select_app_state_sql(), app_name)
        return mysql_json_dict(row.get("state"), deserializer=self._json_deserializer) if row is not None else None

    def _select_user_state_sync(self, driver: Any, app_name: str, user_id: str) -> dict[str, Any] | None:
        row = driver.select_one_or_none(self._select_user_state_sql(), app_name, user_id)
        return mysql_json_dict(row.get("state"), deserializer=self._json_deserializer) if row is not None else None

    def _apply_app_state_delta_sync(self, driver: Any, app_name: str, delta: Mapping[str, Any]) -> dict[str, Any]:
        driver.execute(self._ensure_app_state_row_sql(), app_name, self._json_serializer({}))
        row = driver.select_one_or_none(self._select_app_state_for_update_sql(), app_name)
        current = mysql_json_dict(row.get("state"), deserializer=self._json_deserializer) if row is not None else {}
        merged = {**current, **delta}
        driver.execute(self._update_app_state_sql(), self._json_serializer(merged), app_name)
        return merged

    def _apply_user_state_delta_sync(
        self,
        driver: Any,
        app_name: str,
        user_id: str,
        delta: Mapping[str, Any],
    ) -> dict[str, Any]:
        driver.execute(self._ensure_user_state_row_sql(), app_name, user_id, self._json_serializer({}))
        row = driver.select_one_or_none(self._select_user_state_for_update_sql(), app_name, user_id)
        current = mysql_json_dict(row.get("state"), deserializer=self._json_deserializer) if row is not None else {}
        merged = {**current, **delta}
        driver.execute(self._update_user_state_sql(), self._json_serializer(merged), app_name, user_id)
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
        """Create a new ADK session row and atomically apply/read scoped state."""
        insert_sql, insert_params = self._create_session_sql_and_params(session_id, app_name, user_id, state, owner_id)
        select_sql = self._get_session_select_sql()
        with self._config.provide_session() as driver:
            driver.begin()
            try:
                driver.execute(insert_sql, *insert_params)
                row = driver.select_one_or_none(select_sql, app_name, user_id, session_id)
                if row is None:
                    msg = f"Failed to reload newly created MySQL ADK session {session_id}"
                    raise RuntimeError(msg)
                resolved_app = (
                    self._apply_app_state_delta_sync(driver, app_name, app_state_delta)
                    if app_state_delta
                    else self._select_app_state_sync(driver, app_name)
                )
                resolved_user = (
                    self._apply_user_state_delta_sync(driver, app_name, user_id, user_state_delta)
                    if user_state_delta
                    else self._select_user_state_sync(driver, app_name, user_id)
                )
                driver.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    driver.rollback()
                raise
        session = _row_to_session(row, deserializer=self._json_deserializer)
        session["state"] = merge_scoped_state(session["state"], resolved_app, resolved_user)
        return session

    def get_session(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        *,
        renew_for: int | timedelta | None = None,
    ) -> StoredSession | None:
        """Fetch an ADK session by composite key and optionally touch ``update_time``."""
        should_renew = self._is_renewal_requested(renew_for)
        try:
            with self._provide_replica_session(primary=should_renew) as driver:
                if should_renew:
                    driver.execute(self._get_session_touch_sql(), app_name, user_id, session_id)
                    driver.commit()
                row = driver.select_one_or_none(self._get_session_select_sql(), app_name, user_id, session_id)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return None
            raise
        return _row_to_session(row, deserializer=self._json_deserializer) if row is not None else None

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Replace stored session JSON state."""
        with self._config.provide_session() as driver:
            driver.execute(
                self._update_session_state_sql(), self._json_serializer(state), app_name, user_id, session_id
            )
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
        """List ADK sessions scoped by app and optional user."""
        built = self._list_sessions_sql_and_params(app_name, user_id, order_by, descending, limit, offset)
        if built is None:
            return []
        sql, params = built
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, *params)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return []
            raise
        return [_row_to_session(row, deserializer=self._json_deserializer) for row in rows]

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete an ADK session row (cascading to child event rows)."""
        with self._config.provide_session() as driver:
            driver.execute(self._delete_session_sql(), app_name, user_id, session_id)
            driver.commit()

    def append_event(self, event_record: StoredEvent) -> None:
        """Insert a single ADK event record after verifying owning session existence."""
        sql, params = self._append_event_sql_and_params(event_record)
        with self._config.provide_session() as driver:
            res = driver.execute(sql, *params)
            if int(getattr(res, "rows_affected", 0) or 0) <= 0:
                with contextlib.suppress(Exception):
                    driver.rollback()
                msg = (
                    f"Session {event_record['session_id']} not found "
                    f"for app={event_record['app_name']!r}, user={event_record['user_id']!r}"
                )
                raise ValueError(msg)
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
        """Atomically append an event and merge session, app, and user state deltas."""
        self._validate_event_scope(event_record, app_name, user_id, session_id)
        insert_sql, insert_params = self._insert_event_sql_and_params(event_record)
        with self._config.provide_session() as driver:
            driver.begin()
            try:
                driver.execute(
                    self._update_session_state_sql(),
                    self._json_serializer(state),
                    app_name,
                    user_id,
                    session_id,
                )
                row = driver.select_one_or_none(self._get_session_select_sql(), app_name, user_id, session_id)
                if row is None:
                    msg = f"Session {session_id} not found during append_event_and_update_state."
                    raise ValueError(msg)
                driver.execute(insert_sql, *insert_params)
                merged_app = (
                    self._apply_app_state_delta_sync(driver, app_name, app_state_delta) if app_state_delta else None
                )
                merged_user = (
                    self._apply_user_state_delta_sync(driver, app_name, user_id, user_state_delta)
                    if user_state_delta
                    else None
                )
                driver.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    driver.rollback()
                raise
        session = _row_to_session(row, deserializer=self._json_deserializer)
        if merged_app is not None or merged_user is not None:
            session["state"] = merge_scoped_state(session["state"], merged_app, merged_user)
        return session

    def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> list[StoredEvent]:
        """Fetch ordered events for an ADK session."""
        if limit == 0:
            return []
        sql, params = self._get_events_sql_and_params(app_name, user_id, session_id, after_timestamp, limit)
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, *params)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return []
            raise
        if limit is not None:
            rows = list(reversed(rows))
        return [_row_to_event(row, deserializer=self._json_deserializer) for row in rows]

    def _delete_before(self, table_name: str, column_name: str, threshold: datetime, app_name: str | None) -> int:
        sql, params = self._delete_before_sql_and_params(table_name, column_name, threshold, app_name)
        try:
            with self._config.provide_session() as driver:
                res = driver.execute(sql, *params)
                driver.commit()
                return int(res.rows_affected or 0)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return 0
            raise

    def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Delete events older than ``before``."""
        return self._delete_before(self._events_table, "timestamp", before, app_name)

    def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete sessions older than ``updated_before``."""
        return self._delete_before(self._session_table, "update_time", updated_before, app_name)

    def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete user state rows older than ``updated_before``."""
        return self._delete_before(self._user_state_table, "update_time", updated_before, app_name)

    def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Return app-scoped ADK state for ``app_name``."""
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(self._select_app_state_sql(), app_name)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return None
            raise
        return mysql_json_dict(row["state"], deserializer=self._json_deserializer) if row is not None else None

    def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Return user-scoped ADK state for ``(app_name, user_id)``."""
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(self._select_user_state_sql(), app_name, user_id)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return None
            raise
        return mysql_json_dict(row["state"], deserializer=self._json_deserializer) if row is not None else None

    def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Upsert app-scoped ADK state via ``ON DUPLICATE KEY UPDATE``."""
        with self._config.provide_session() as driver:
            driver.execute(self._upsert_app_state_sql(), app_name, self._json_serializer(state))
            driver.commit()

    def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Upsert user-scoped ADK state via ``ON DUPLICATE KEY UPDATE``."""
        with self._config.provide_session() as driver:
            driver.execute(self._upsert_user_state_sql(), app_name, user_id, self._json_serializer(state))
            driver.commit()

    def get_metadata(self, key: str) -> str | None:
        """Return an ADK metadata table value by key."""
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(self._get_metadata_sql(), key)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return None
            raise
        return str(row["value"]) if row is not None and row.get("value") is not None else None

    def set_metadata(self, key: str, value: str) -> None:
        """Upsert an ADK metadata key-value entry via ``ON DUPLICATE KEY UPDATE``."""
        with self._config.provide_session() as driver:
            driver.execute(self._set_metadata_sql(), key, value)
            driver.commit()


class MySQLAsyncADKStore(_MySQLADKSessionSQLMixin, BaseAsyncADKStore["MySQLAsyncConfig"]):
    """Asynchronous MySQL/MariaDB ADK session, event, state, and metadata store using ``?`` binds."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    adapter_name = "vertebra_mysql"
    _max_table_name_parts: ClassVar[int] = 2
    extension_config_options = _MYSQL_ADK_EXTENSION_OPTIONS

    def __init__(self, config: MySQLAsyncConfig, *, read_config: MySQLAsyncConfig | None = None) -> None:
        """Initialize the asynchronous MySQL ADK session/event store."""
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
        """Create ADK session, event, app state, user state, and metadata tables asynchronously."""
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

    async def _select_app_state_async(self, driver: Any, app_name: str) -> dict[str, Any] | None:
        row = await driver.select_one_or_none(self._select_app_state_sql(), app_name)
        return mysql_json_dict(row.get("state"), deserializer=self._json_deserializer) if row is not None else None

    async def _select_user_state_async(self, driver: Any, app_name: str, user_id: str) -> dict[str, Any] | None:
        row = await driver.select_one_or_none(self._select_user_state_sql(), app_name, user_id)
        return mysql_json_dict(row.get("state"), deserializer=self._json_deserializer) if row is not None else None

    async def _apply_app_state_delta_async(
        self, driver: Any, app_name: str, delta: Mapping[str, Any]
    ) -> dict[str, Any]:
        await driver.execute(self._ensure_app_state_row_sql(), app_name, self._json_serializer({}))
        row = await driver.select_one_or_none(self._select_app_state_for_update_sql(), app_name)
        current = mysql_json_dict(row.get("state"), deserializer=self._json_deserializer) if row is not None else {}
        merged = {**current, **delta}
        await driver.execute(self._update_app_state_sql(), self._json_serializer(merged), app_name)
        return merged

    async def _apply_user_state_delta_async(
        self,
        driver: Any,
        app_name: str,
        user_id: str,
        delta: Mapping[str, Any],
    ) -> dict[str, Any]:
        await driver.execute(self._ensure_user_state_row_sql(), app_name, user_id, self._json_serializer({}))
        row = await driver.select_one_or_none(self._select_user_state_for_update_sql(), app_name, user_id)
        current = mysql_json_dict(row.get("state"), deserializer=self._json_deserializer) if row is not None else {}
        merged = {**current, **delta}
        await driver.execute(self._update_user_state_sql(), self._json_serializer(merged), app_name, user_id)
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
        """Create a new ADK session row asynchronously and atomically apply/read scoped state."""
        insert_sql, insert_params = self._create_session_sql_and_params(session_id, app_name, user_id, state, owner_id)
        select_sql = self._get_session_select_sql()
        async with self._config.provide_session() as driver:
            await driver.begin()
            try:
                await driver.execute(insert_sql, *insert_params)
                row = await driver.select_one_or_none(select_sql, app_name, user_id, session_id)
                if row is None:
                    msg = f"Failed to reload newly created MySQL ADK session {session_id}"
                    raise RuntimeError(msg)
                resolved_app = (
                    await self._apply_app_state_delta_async(driver, app_name, app_state_delta)
                    if app_state_delta
                    else await self._select_app_state_async(driver, app_name)
                )
                resolved_user = (
                    await self._apply_user_state_delta_async(driver, app_name, user_id, user_state_delta)
                    if user_state_delta
                    else await self._select_user_state_async(driver, app_name, user_id)
                )
                await driver.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    await driver.rollback()
                raise
        session = _row_to_session(row, deserializer=self._json_deserializer)
        session["state"] = merge_scoped_state(session["state"], resolved_app, resolved_user)
        return session

    async def get_session(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        *,
        renew_for: int | timedelta | None = None,
    ) -> StoredSession | None:
        """Fetch an ADK session asynchronously and optionally renew ``update_time``."""
        should_renew = self._is_renewal_requested(renew_for)
        try:
            async with self._provide_replica_session(primary=should_renew) as driver:
                if should_renew:
                    await driver.execute(self._get_session_touch_sql(), app_name, user_id, session_id)
                    await driver.commit()
                row = await driver.select_one_or_none(self._get_session_select_sql(), app_name, user_id, session_id)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return None
            raise
        return _row_to_session(row, deserializer=self._json_deserializer) if row is not None else None

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Update session JSON state asynchronously."""
        async with self._config.provide_session() as driver:
            await driver.execute(
                self._update_session_state_sql(),
                self._json_serializer(state),
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
        order_by: SessionOrderBy = "update_time",
        descending: bool = True,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[StoredSession]:
        """List ADK sessions asynchronously."""
        built = self._list_sessions_sql_and_params(app_name, user_id, order_by, descending, limit, offset)
        if built is None:
            return []
        sql, params = built
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, *params)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return []
            raise
        return [_row_to_session(row, deserializer=self._json_deserializer) for row in rows]

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete an ADK session asynchronously."""
        async with self._config.provide_session() as driver:
            await driver.execute(self._delete_session_sql(), app_name, user_id, session_id)
            await driver.commit()

    async def append_event(self, event_record: StoredEvent) -> None:
        """Append an ADK event record asynchronously after verifying owning session existence."""
        sql, params = self._append_event_sql_and_params(event_record)
        async with self._config.provide_session() as driver:
            res = await driver.execute(sql, *params)
            if int(getattr(res, "rows_affected", 0) or 0) <= 0:
                with contextlib.suppress(Exception):
                    await driver.rollback()
                msg = (
                    f"Session {event_record['session_id']} not found "
                    f"for app={event_record['app_name']!r}, user={event_record['user_id']!r}"
                )
                raise ValueError(msg)
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
        """Atomically append an event and merge session, app, and user state deltas asynchronously."""
        self._validate_event_scope(event_record, app_name, user_id, session_id)
        insert_sql, insert_params = self._insert_event_sql_and_params(event_record)
        async with self._config.provide_session() as driver:
            await driver.begin()
            try:
                await driver.execute(
                    self._update_session_state_sql(),
                    self._json_serializer(state),
                    app_name,
                    user_id,
                    session_id,
                )
                row = await driver.select_one_or_none(self._get_session_select_sql(), app_name, user_id, session_id)
                if row is None:
                    msg = f"Session {session_id} not found during append_event_and_update_state."
                    raise ValueError(msg)
                await driver.execute(insert_sql, *insert_params)
                merged_app = (
                    await self._apply_app_state_delta_async(driver, app_name, app_state_delta)
                    if app_state_delta
                    else None
                )
                merged_user = (
                    await self._apply_user_state_delta_async(driver, app_name, user_id, user_state_delta)
                    if user_state_delta
                    else None
                )
                await driver.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    await driver.rollback()
                raise
        session = _row_to_session(row, deserializer=self._json_deserializer)
        if merged_app is not None or merged_user is not None:
            session["state"] = merge_scoped_state(session["state"], merged_app, merged_user)
        return session

    async def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> list[StoredEvent]:
        """Fetch ordered events for an ADK session asynchronously."""
        if limit == 0:
            return []
        sql, params = self._get_events_sql_and_params(app_name, user_id, session_id, after_timestamp, limit)
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, *params)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return []
            raise
        if limit is not None:
            rows = list(reversed(rows))
        return [_row_to_event(row, deserializer=self._json_deserializer) for row in rows]

    async def _delete_before(self, table_name: str, column_name: str, threshold: datetime, app_name: str | None) -> int:
        sql, params = self._delete_before_sql_and_params(table_name, column_name, threshold, app_name)
        try:
            async with self._config.provide_session() as driver:
                res = await driver.execute(sql, *params)
                await driver.commit()
                return int(res.rows_affected or 0)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return 0
            raise

    async def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Delete events older than ``before`` asynchronously."""
        return await self._delete_before(self._events_table, "timestamp", before, app_name)

    async def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete sessions older than ``updated_before`` asynchronously."""
        return await self._delete_before(self._session_table, "update_time", updated_before, app_name)

    async def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete user state rows older than ``updated_before`` asynchronously."""
        return await self._delete_before(self._user_state_table, "update_time", updated_before, app_name)

    async def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Return app-scoped ADK state asynchronously."""
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(self._select_app_state_sql(), app_name)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return None
            raise
        return mysql_json_dict(row["state"], deserializer=self._json_deserializer) if row is not None else None

    async def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Return user-scoped ADK state asynchronously."""
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(self._select_user_state_sql(), app_name, user_id)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return None
            raise
        return mysql_json_dict(row["state"], deserializer=self._json_deserializer) if row is not None else None

    async def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Upsert app-scoped ADK state asynchronously."""
        async with self._config.provide_session() as driver:
            await driver.execute(self._upsert_app_state_sql(), app_name, self._json_serializer(state))
            await driver.commit()

    async def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Upsert user-scoped ADK state asynchronously."""
        async with self._config.provide_session() as driver:
            await driver.execute(self._upsert_user_state_sql(), app_name, user_id, self._json_serializer(state))
            await driver.commit()

    async def get_metadata(self, key: str) -> str | None:
        """Return an ADK metadata table value asynchronously."""
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(self._get_metadata_sql(), key)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return None
            raise
        return str(row["value"]) if row is not None and row.get("value") is not None else None

    async def set_metadata(self, key: str, value: str) -> None:
        """Upsert an ADK metadata key-value entry asynchronously."""
        async with self._config.provide_session() as driver:
            await driver.execute(self._set_metadata_sql(), key, value)
            await driver.commit()


def _resolve_memory_init_state(config: Any, default_use_fts: bool) -> tuple[bool, bool, bool, bool, bool]:
    """Compute initial `(use_fts, vector_requested, vector_resolved, native_vector, is_mariadb)` state."""
    adk_cfg = _extract_adk_config(config)
    use_fts = bool(adk_cfg["use_fts"]) if "memory_use_fts" not in adk_cfg and "use_fts" in adk_cfg else default_use_fts
    use_vector_type = adk_cfg.get("use_vector_type")
    enable_vector_index = adk_cfg.get("enable_vector_index")
    explicit_vtype = adk_cfg.get("vector_index_type")
    explicit_native = isinstance(explicit_vtype, str) and explicit_vtype.strip().lower() in _EXPLICIT_VECTOR_MODES
    requested = False if use_vector_type is False else bool(use_vector_type or enable_vector_index or explicit_native)
    if not requested:
        return use_fts, False, True, False, False
    raw_version = adk_cfg.get("server_version")
    if raw_version is not None:
        version_text = str(raw_version)
        return use_fts, True, True, mysql_supports_native_vector(version_text), "mariadb" in version_text.lower()
    return use_fts, True, False, False, False


def _parse_probed_vector_support(raw_version: Any) -> tuple[bool, bool]:
    """Return `(native_vector, is_mariadb)` from a probed `SELECT VERSION()` scalar."""
    version_text = str(raw_version) if raw_version is not None else None
    return mysql_supports_native_vector(version_text), bool(version_text and "mariadb" in version_text.lower())


class _MySQLADKMemorySQLMixin:
    """Shared MySQL/MariaDB DDL, vector/FTS query builders, and pruning SQL for ADK memory stores."""

    __slots__ = ()

    _config: Any
    _enabled: bool
    _memory_table: str
    _use_fts: bool
    _max_results: int
    _vector_dimensions: int
    _owner_id_column_ddl: str | None
    _owner_id_column_name: str | None
    _json_serializer: Any
    _json_deserializer: Any
    _vector_requested: bool
    _vector_resolved: bool
    _native_vector: bool
    _is_mariadb: bool

    def _build_memory_table_ddl(self) -> str:
        adk_cfg = _extract_adk_config(self._config)
        table_options = mysql_table_options(adk_cfg, "memory_table_options")
        owner_line = ""
        fk_clause = ""
        if self._owner_id_column_ddl:
            col_def, fk_def = mysql_owner_id_column_parts(self._owner_id_column_ddl)
            owner_line = f"\n    {col_def},"
            if fk_def:
                fk_clause = f",\n    {fk_def}"
        fts_clause = (
            f",\n    FULLTEXT INDEX {_index_name(self._memory_table, 'fts')} (content_text)" if self._use_fts else ""
        )
        embedding_col = f"VECTOR({max(1, int(self._vector_dimensions))})" if self._native_vector else "JSON"
        unique_key_name = _index_name(self._memory_table, "app_user_event")
        return (
            f"CREATE TABLE IF NOT EXISTS {self._memory_table} (\n"
            "    id VARCHAR(128) PRIMARY KEY,\n"
            "    session_id VARCHAR(128) NOT NULL,\n"
            "    app_name VARCHAR(128) NOT NULL,\n"
            "    user_id VARCHAR(128) NOT NULL,\n"
            "    scope VARCHAR(16) NOT NULL DEFAULT 'user',\n"
            "    event_id VARCHAR(128) NOT NULL,\n"
            f"    author VARCHAR(256),{owner_line}\n"
            "    timestamp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\n"
            "    content_json JSON NOT NULL,\n"
            "    content_text TEXT NOT NULL,\n"
            "    metadata_json JSON,\n"
            "    inserted_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\n"
            f"    embedding {embedding_col},\n"
            f"    UNIQUE KEY {unique_key_name} (app_name, user_id, event_id),\n"
            f"    INDEX {_index_name(self._memory_table, 'app_scope_user_time')} (app_name, scope, user_id, timestamp),\n"
            f"    INDEX {_index_name(self._memory_table, 'scope')} (app_name, scope),\n"
            f"    INDEX {_index_name(self._memory_table, 'session')} (session_id){fts_clause}{fk_clause}\n"
            f") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{table_options};"
        )

    def _drop_memory_table_sql(self) -> list[str]:
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

    def _build_memory_insert_statement(
        self,
        entries: list[StoredMemory],
        owner_id: object | None,
    ) -> tuple[str, list[tuple[Any, ...]]]:
        if not self._native_vector:
            emb_expr = "?"
        elif self._is_mariadb:
            emb_expr = "VEC_FromText(?)"
        else:
            emb_expr = "STRING_TO_VECTOR(?)"
        if self._owner_id_column_name:
            sql = (
                f"INSERT IGNORE INTO {self._memory_table} "
                f"(id, session_id, app_name, user_id, scope, event_id, author, {self._owner_id_column_name}, "
                "timestamp, content_json, content_text, metadata_json, inserted_at, embedding) "
                f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, {emb_expr})"
            )
            params_list: list[tuple[Any, ...]] = [
                (
                    entry["id"],
                    entry["session_id"],
                    entry["app_name"],
                    entry["user_id"],
                    entry.get("scope", "user"),
                    entry["event_id"],
                    entry.get("author"),
                    owner_id,
                    _naive_utc(entry["timestamp"]),
                    self._json_serializer(entry["content_json"]),
                    entry["content_text"],
                    self._json_serializer(entry["metadata_json"]) if entry.get("metadata_json") is not None else None,
                    _naive_utc(entry["inserted_at"]),
                    format_mysql_vector_literal(entry.get("embedding")),
                )
                for entry in entries
            ]
            return sql, params_list
        sql = (
            f"INSERT IGNORE INTO {self._memory_table} "
            "(id, session_id, app_name, user_id, scope, event_id, author, "
            "timestamp, content_json, content_text, metadata_json, inserted_at, embedding) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, {emb_expr})"
        )
        params_list = [
            (
                entry["id"],
                entry["session_id"],
                entry["app_name"],
                entry["user_id"],
                entry.get("scope", "user"),
                entry["event_id"],
                entry.get("author"),
                _naive_utc(entry["timestamp"]),
                self._json_serializer(entry["content_json"]),
                entry["content_text"],
                self._json_serializer(entry["metadata_json"]) if entry.get("metadata_json") is not None else None,
                _naive_utc(entry["inserted_at"]),
                format_mysql_vector_literal(entry.get("embedding")),
            )
            for entry in entries
        ]
        return sql, params_list

    def _build_memory_search_query(
        self,
        query: str,
        has_query: bool,
        app_name: str,
        user_id: str,
        limit_value: int,
        scope_filter: Literal["all", "user", "app"],
        embedding: Sequence[float] | None,
    ) -> tuple[str, tuple[Any, ...]]:
        where_scope, scope_params = build_mysql_scope_where(app_name, user_id, scope_filter)
        base_cols = (
            "id, session_id, app_name, user_id, scope, event_id, author, timestamp, "
            "content_json, content_text, metadata_json, inserted_at"
        )
        if self._native_vector and embedding is not None:
            vec_literal = format_mysql_vector_literal(embedding)
            dist_expr = (
                "VEC_DISTANCE_COSINE(embedding, VEC_FromText(?))"
                if self._is_mariadb
                else "DISTANCE(embedding, STRING_TO_VECTOR(?), 'COSINE')"
            )
            if has_query:
                if self._use_fts:
                    sql = (
                        f"SELECT {base_cols}, NULL AS embedding, {dist_expr} AS vector_distance "
                        f"FROM {self._memory_table} WHERE {where_scope} AND embedding IS NOT NULL "
                        "AND MATCH(content_text) AGAINST (? IN NATURAL LANGUAGE MODE) "
                        "ORDER BY vector_distance ASC LIMIT ?"
                    )
                    return sql, (vec_literal, *scope_params, query, limit_value)
                sql = (
                    f"SELECT {base_cols}, NULL AS embedding, {dist_expr} AS vector_distance "
                    f"FROM {self._memory_table} WHERE {where_scope} AND embedding IS NOT NULL "
                    "AND content_text LIKE ? "
                    "ORDER BY vector_distance ASC LIMIT ?"
                )
                return sql, (vec_literal, *scope_params, f"%{query}%", limit_value)
            sql = (
                f"SELECT {base_cols}, NULL AS embedding, {dist_expr} AS vector_distance "
                f"FROM {self._memory_table} WHERE {where_scope} AND embedding IS NOT NULL "
                "ORDER BY vector_distance ASC LIMIT ?"
            )
            return sql, (vec_literal, *scope_params, limit_value)

        fetch_limit = limit_value if embedding is None else max(limit_value * 5, 50)
        if has_query:
            if self._use_fts:
                sql = (
                    f"SELECT {base_cols}, embedding "
                    f"FROM {self._memory_table} WHERE {where_scope} "
                    "AND MATCH(content_text) AGAINST (? IN NATURAL LANGUAGE MODE) "
                    "ORDER BY MATCH(content_text) AGAINST (? IN NATURAL LANGUAGE MODE) DESC, timestamp DESC LIMIT ?"
                )
                return sql, (*scope_params, query, query, fetch_limit)
            sql = (
                f"SELECT {base_cols}, embedding "
                f"FROM {self._memory_table} WHERE {where_scope} AND content_text LIKE ? "
                "ORDER BY CASE WHEN content_text LIKE ? THEN 0 ELSE 1 END, timestamp DESC LIMIT ?"
            )
            return sql, (*scope_params, f"%{query}%", f"{query}%", fetch_limit)

        sql = (
            f"SELECT {base_cols}, embedding "
            f"FROM {self._memory_table} WHERE {where_scope} AND embedding IS NOT NULL "
            "ORDER BY timestamp DESC LIMIT ?"
        )
        return sql, (*scope_params, fetch_limit)

    def _decode_and_finalize_memory(
        self,
        rows: Sequence[Mapping[str, Any]],
        embedding: Sequence[float] | None,
        limit_value: int,
    ) -> list[StoredMemory]:
        records = [_memory_row_to_record(row, deserializer=self._json_deserializer) for row in rows]
        if not self._native_vector and embedding is not None and records:
            query_vec = [float(x) for x in embedding]
            records.sort(
                key=lambda item: (
                    cosine_similarity(query_vec, vec) if (vec := _item_embedding(item)) is not None else -1.0
                ),
                reverse=True,
            )
            return records[:limit_value]
        return records

    def _delete_by_session_sql_and_params(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
    ) -> tuple[str, tuple[str, str, str]]:
        sql = f"DELETE FROM {self._memory_table} WHERE session_id = ? AND app_name = ? AND user_id = ?"
        return sql, (session_id, app_name, user_id)

    def _delete_older_than_sql_and_params(
        self,
        days: int,
        app_name: str | None,
        scope: str | None,
    ) -> tuple[str, tuple[Any, ...]]:
        clauses = ["inserted_at < (UTC_TIMESTAMP(6) - INTERVAL ? DAY)"]
        params: list[Any] = [int(days)]
        if app_name is not None:
            clauses.append("app_name = ?")
            params.append(app_name)
        if scope is not None:
            clauses.append("scope = ?")
            params.append(scope)
        sql = f"DELETE FROM {self._memory_table} WHERE {' AND '.join(clauses)}"
        return sql, tuple(params)


class MySQLSyncADKMemoryStore(_MySQLADKMemorySQLMixin, _BaseSyncADKMemoryStore["MySQLSyncConfig"]):
    """Synchronous MySQL/MariaDB ADK memory store with ``FULLTEXT`` and native ``VECTOR`` search."""

    __slots__ = (
        "_is_mariadb",
        "_json_deserializer",
        "_json_serializer",
        "_native_vector",
        "_vector_requested",
        "_vector_resolved",
    )

    adapter_name = "vertebra_mysql"
    _max_table_name_parts: ClassVar[int] = 2
    extension_config_options = _MYSQL_ADK_EXTENSION_OPTIONS

    def __init__(self, config: MySQLSyncConfig, *, read_config: MySQLSyncConfig | None = None) -> None:
        """Initialize the synchronous MySQL ADK memory store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        (
            self._use_fts,
            self._vector_requested,
            self._vector_resolved,
            self._native_vector,
            self._is_mariadb,
        ) = _resolve_memory_init_state(config, self._use_fts)

    def _resolve_native_vector_sync(self, driver: Any) -> bool:
        if self._vector_resolved:
            return self._native_vector
        if not self._vector_requested:
            self._vector_resolved = True
            self._native_vector = False
            return False
        raw_version: Any = None
        try:
            raw_version = driver.select_value_or_none("SELECT VERSION()")
        except Exception:
            raw_version = None
        self._native_vector, self._is_mariadb = _parse_probed_vector_support(raw_version)
        self._vector_resolved = True
        return self._native_vector

    def _memory_table_ddl(self) -> str:
        return self._build_memory_table_ddl()

    def create_tables(self) -> None:
        """Create the MySQL memory table and optional ``FULLTEXT`` / ``VECTOR`` columns."""
        if not self._enabled:
            return
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return
        with self._config.provide_session() as driver:
            self._resolve_native_vector_sync(driver)
            driver.execute_script(self._memory_table_ddl())
            driver.commit()

    def insert_memory_entries(self, entries: list[StoredMemory], owner_id: object | None = None) -> int:
        """Bulk insert memory entries ignoring duplicate ``(app_name, user_id, event_id)`` rows."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0
        with self._config.provide_session() as driver:
            self._resolve_native_vector_sync(driver)
            sql, params_list = self._build_memory_insert_statement(entries, owner_id)
            res = driver.execute_many(sql, params_list)
            driver.commit()
            return int(res.rows_affected or 0)

    def search_entries(
        self,
        query: str,
        app_name: str,
        user_id: str,
        limit: int | None = None,
        scope_filter: Literal["all", "user", "app"] = "all",
        embedding: Sequence[float] | None = None,
    ) -> list[StoredMemory]:
        """Search memory entries using native ``VEC_DISTANCE_COSINE``, ``FULLTEXT``, ``LIKE``, or cosine fallback."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        has_query = bool(query and query.strip())
        if not has_query and embedding is None:
            return []
        limit_value = limit if limit is not None else self._max_results
        if limit_value <= 0:
            return []
        try:
            with self._provide_replica_session(primary=False) as driver:
                self._resolve_native_vector_sync(driver)
                sql, params = self._build_memory_search_query(
                    query, has_query, app_name, user_id, limit_value, scope_filter, embedding
                )
                rows = driver.select(sql, *params)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return []
            raise
        return self._decode_and_finalize_memory(rows, embedding, limit_value)

    def delete_entries_by_session(self, session_id: str, app_name: str, user_id: str) -> int:
        """Delete memory entries for a session scoped to ``app_name`` and ``user_id``."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        sql, params = self._delete_by_session_sql_and_params(session_id, app_name, user_id)
        try:
            with self._config.provide_session() as driver:
                res = driver.execute(sql, *params)
                driver.commit()
                return int(res.rows_affected or 0)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return 0
            raise

    def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
        """Delete memory entries older than ``days`` days."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        sql, params = self._delete_older_than_sql_and_params(days, app_name, scope)
        try:
            with self._config.provide_session() as driver:
                res = driver.execute(sql, *params)
                driver.commit()
                return int(res.rows_affected or 0)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return 0
            raise


class MySQLAsyncADKMemoryStore(_MySQLADKMemorySQLMixin, _BaseAsyncADKMemoryStore["MySQLAsyncConfig"]):
    """Asynchronous MySQL/MariaDB ADK memory store with ``FULLTEXT`` and native ``VECTOR`` search."""

    __slots__ = (
        "_is_mariadb",
        "_json_deserializer",
        "_json_serializer",
        "_native_vector",
        "_vector_requested",
        "_vector_resolved",
    )

    adapter_name = "vertebra_mysql"
    _max_table_name_parts: ClassVar[int] = 2
    extension_config_options = _MYSQL_ADK_EXTENSION_OPTIONS

    def __init__(self, config: MySQLAsyncConfig, *, read_config: MySQLAsyncConfig | None = None) -> None:
        """Initialize the asynchronous MySQL ADK memory store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        (
            self._use_fts,
            self._vector_requested,
            self._vector_resolved,
            self._native_vector,
            self._is_mariadb,
        ) = _resolve_memory_init_state(config, self._use_fts)

    async def _resolve_native_vector_async(self, driver: Any) -> bool:
        if self._vector_resolved:
            return self._native_vector
        if not self._vector_requested:
            self._vector_resolved = True
            self._native_vector = False
            return False
        raw_version: Any = None
        try:
            raw_version = await driver.select_value_or_none("SELECT VERSION()")
        except Exception:
            raw_version = None
        self._native_vector, self._is_mariadb = _parse_probed_vector_support(raw_version)
        self._vector_resolved = True
        return self._native_vector

    async def _memory_table_ddl(self) -> str:
        return self._build_memory_table_ddl()

    async def create_tables(self) -> None:
        """Create the MySQL memory table asynchronously."""
        if not self._enabled:
            return
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        async with self._config.provide_session() as driver:
            await self._resolve_native_vector_async(driver)
            await driver.execute_script(await self._memory_table_ddl())
            await driver.commit()

    async def insert_memory_entries(self, entries: list[StoredMemory], owner_id: object | None = None) -> int:
        """Bulk insert memory entries asynchronously ignoring duplicates."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0
        async with self._config.provide_session() as driver:
            await self._resolve_native_vector_async(driver)
            sql, params_list = self._build_memory_insert_statement(entries, owner_id)
            res = await driver.execute_many(sql, params_list)
            await driver.commit()
            return int(res.rows_affected or 0)

    async def search_entries(
        self,
        query: str,
        app_name: str,
        user_id: str,
        limit: int | None = None,
        scope_filter: Literal["all", "user", "app"] = "all",
        embedding: Sequence[float] | None = None,
    ) -> list[StoredMemory]:
        """Search memory entries asynchronously via native ``VEC_DISTANCE_COSINE``, ``FULLTEXT``, or ``LIKE``."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        has_query = bool(query and query.strip())
        if not has_query and embedding is None:
            return []
        limit_value = limit if limit is not None else self._max_results
        if limit_value <= 0:
            return []
        try:
            async with self._provide_replica_session(primary=False) as driver:
                await self._resolve_native_vector_async(driver)
                sql, params = self._build_memory_search_query(
                    query, has_query, app_name, user_id, limit_value, scope_filter, embedding
                )
                rows = await driver.select(sql, *params)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return []
            raise
        return self._decode_and_finalize_memory(rows, embedding, limit_value)

    async def delete_entries_by_session(self, session_id: str, app_name: str, user_id: str) -> int:
        """Delete memory entries for a session scoped to ``app_name`` and ``user_id`` asynchronously."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        sql, params = self._delete_by_session_sql_and_params(session_id, app_name, user_id)
        try:
            async with self._config.provide_session() as driver:
                res = await driver.execute(sql, *params)
                await driver.commit()
                return int(res.rows_affected or 0)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return 0
            raise

    async def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
        """Delete memory entries older than ``days`` days asynchronously."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        sql, params = self._delete_older_than_sql_and_params(days, app_name, scope)
        try:
            async with self._config.provide_session() as driver:
                res = await driver.execute(sql, *params)
                await driver.commit()
                return int(res.rows_affected or 0)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return 0
            raise


class _MySQLADKArtifactSQLMixin:
    """Shared MySQL DDL and query builders for synchronous and asynchronous ADK artifact stores."""

    __slots__ = ()

    _config: Any
    _artifact_table: str
    _json_serializer: Any
    _json_deserializer: Any

    def _artifact_table_ddl(self) -> str:
        adk_cfg = _extract_adk_config(self._config)
        table_options = mysql_table_options(adk_cfg, "artifact_table_options")
        return (
            f"CREATE TABLE IF NOT EXISTS {self._artifact_table} (\n"
            "    app_name VARCHAR(128) NOT NULL,\n"
            "    user_id VARCHAR(128) NOT NULL,\n"
            "    session_id VARCHAR(128) NOT NULL DEFAULT '',\n"
            "    filename VARCHAR(512) NOT NULL,\n"
            "    version INTEGER NOT NULL,\n"
            "    mime_type VARCHAR(255),\n"
            "    canonical_uri TEXT NOT NULL,\n"
            "    custom_metadata JSON,\n"
            "    size BIGINT,\n"
            "    updated DATETIME(6),\n"
            "    signed_url TEXT,\n"
            "    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\n"
            "    PRIMARY KEY (app_name, user_id, session_id, filename, version)\n"
            f") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{table_options};"
        )

    def _insert_artifact_sql_and_params(self, record: StoredArtifact) -> tuple[str, tuple[Any, ...]]:
        session_val = record["session_id"] if record["session_id"] is not None else ""
        meta = record.get("custom_metadata")
        sql = (
            f"INSERT INTO {self._artifact_table} "
            "(app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON DUPLICATE KEY UPDATE mime_type = VALUES(mime_type), canonical_uri = VALUES(canonical_uri), "
            "custom_metadata = VALUES(custom_metadata)"
        )
        params = (
            record["app_name"],
            record["user_id"],
            session_val,
            record["filename"],
            int(record["version"]),
            record.get("mime_type"),
            record["canonical_uri"],
            self._json_serializer(meta) if meta is not None else None,
            _naive_utc(record["created_at"]),
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
        session_val = session_id if session_id is not None else ""
        now_dt = reference.updated or datetime.now(UTC)
        sql = (
            f"INSERT INTO {self._artifact_table} "
            "(app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, size, updated, signed_url, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON DUPLICATE KEY UPDATE mime_type = VALUES(mime_type), canonical_uri = VALUES(canonical_uri), "
            "custom_metadata = VALUES(custom_metadata), size = VALUES(size), updated = VALUES(updated), "
            "signed_url = VALUES(signed_url)"
        )
        params = (
            app_name,
            user_id,
            session_val,
            filename,
            int(reference.version),
            reference.content_type,
            reference.uri,
            self._json_serializer(custom_metadata) if custom_metadata is not None else None,
            reference.size,
            _naive_utc(reference.updated) if reference.updated is not None else None,
            reference.signed_url,
            _naive_utc(now_dt),
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
        session_val = session_id if session_id is not None else ""
        cols = (
            "*"
            if include_ref_cols
            else "app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at"
        )
        if version is not None:
            sql = (
                f"SELECT {cols} FROM {self._artifact_table} "
                "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? AND version = ? LIMIT 1"
            )
            return sql, (app_name, user_id, session_val, filename, version)
        sql = (
            f"SELECT {cols} FROM {self._artifact_table} "
            "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? "
            "ORDER BY version DESC LIMIT 1"
        )
        return sql, (app_name, user_id, session_val, filename)

    def _list_artifact_keys_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        session_id: str | None,
    ) -> tuple[str, tuple[Any, ...]]:
        if session_id is not None:
            sql = (
                f"SELECT DISTINCT filename FROM {self._artifact_table} "
                "WHERE app_name = ? AND user_id = ? AND session_id IN ('', ?) ORDER BY filename ASC"
            )
            return sql, (app_name, user_id, session_id)
        sql = (
            f"SELECT DISTINCT filename FROM {self._artifact_table} "
            "WHERE app_name = ? AND user_id = ? AND session_id = '' ORDER BY filename ASC"
        )
        return sql, (app_name, user_id)

    def _list_artifact_versions_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None,
    ) -> tuple[str, tuple[str, str, str, str]]:
        session_val = session_id if session_id is not None else ""
        sql = (
            "SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at "
            f"FROM {self._artifact_table} "
            "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? ORDER BY version ASC"
        )
        return sql, (app_name, user_id, session_val, filename)

    def _delete_artifact_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None,
    ) -> tuple[str, tuple[str, str, str, str]]:
        session_val = session_id if session_id is not None else ""
        sql = (
            f"DELETE FROM {self._artifact_table} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ?"
        )
        return sql, (app_name, user_id, session_val, filename)

    def _get_next_version_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None,
    ) -> tuple[str, tuple[str, str, str, str]]:
        session_val = session_id if session_id is not None else ""
        sql = (
            f"SELECT MAX(version) FROM {self._artifact_table} "
            "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ?"
        )
        return sql, (app_name, user_id, session_val, filename)

    def _delete_older_than_sqls_and_params(
        self,
        before: datetime,
        app_name: str | None,
    ) -> tuple[str, str, tuple[Any, ...]]:
        cutoff = _naive_utc(before)
        cols = "app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at"
        if app_name is not None:
            select_sql = (
                f"SELECT {cols} FROM {self._artifact_table} "
                "WHERE created_at < ? AND app_name = ? ORDER BY created_at ASC"
            )
            delete_sql = f"DELETE FROM {self._artifact_table} WHERE created_at < ? AND app_name = ?"
            return select_sql, delete_sql, (cutoff, app_name)
        select_sql = f"SELECT {cols} FROM {self._artifact_table} WHERE created_at < ? ORDER BY created_at ASC"
        delete_sql = f"DELETE FROM {self._artifact_table} WHERE created_at < ?"
        return select_sql, delete_sql, (cutoff,)


class MySQLSyncADKArtifactStore(_MySQLADKArtifactSQLMixin, _BaseSyncADKArtifactStore["MySQLSyncConfig"]):
    """Synchronous MySQL ADK artifact metadata store supporting ``ArtifactReference`` records."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    adapter_name = "vertebra_mysql"
    extension_config_options = _MYSQL_ADK_EXTENSION_OPTIONS

    def __init__(self, config: MySQLSyncConfig, *, read_config: MySQLSyncConfig | None = None) -> None:
        """Initialize the synchronous MySQL ADK artifact store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    def create_table(self) -> None:
        """Create the MySQL ADK artifact metadata table if absent."""
        with self._config.provide_session() as driver:
            driver.execute_script(self._artifact_table_ddl())
            driver.commit()

    def insert_artifact(self, record: StoredArtifact) -> None:
        """Insert or update a ``StoredArtifact`` metadata record."""
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
        """Upsert an artifact revision backed by a storage-neutral ``ArtifactReference`` struct."""
        sql, params = self._insert_artifact_ref_sql_and_params(
            app_name, user_id, filename, reference, session_id, custom_metadata
        )
        with self._config.provide_session() as driver:
            driver.execute(sql, *params)
            driver.commit()

    def get_artifact_reference(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> ArtifactReference | None:
        """Fetch an ``ArtifactReference`` revision by exact version or latest."""
        sql, params = self._get_artifact_sql_and_params(
            app_name, user_id, filename, session_id, version, include_ref_cols=True
        )
        try:
            with self._provide_replica_session(primary=False) as driver:
                row = driver.select_one_or_none(sql, *params)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return None
            raise
        return _row_to_artifact_reference(row) if row is not None else None

    def get_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> StoredArtifact | None:
        """Fetch a ``StoredArtifact`` revision by exact version or latest."""
        sql, params = self._get_artifact_sql_and_params(
            app_name, user_id, filename, session_id, version, include_ref_cols=False
        )
        try:
            with self._provide_replica_session(primary=False) as driver:
                row = driver.select_one_or_none(sql, *params)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return None
            raise
        return _row_to_stored_artifact(row, deserializer=self._json_deserializer) if row is not None else None

    def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """List distinct artifact filenames within user or session scope."""
        sql, params = self._list_artifact_keys_sql_and_params(app_name, user_id, session_id)
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, *params)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return []
            raise
        return [str(row["filename"]) for row in rows]

    def list_artifact_versions(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[StoredArtifact]:
        """List all stored versions for an artifact sorted ascending."""
        sql, params = self._list_artifact_versions_sql_and_params(app_name, user_id, filename, session_id)
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, *params)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return []
            raise
        return [_row_to_stored_artifact(row, deserializer=self._json_deserializer) for row in rows]

    def delete_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[StoredArtifact]:
        """Delete all metadata versions for an artifact atomically and return removed records."""
        select_sql, select_params = self._list_artifact_versions_sql_and_params(app_name, user_id, filename, session_id)
        delete_sql, delete_params = self._delete_artifact_sql_and_params(app_name, user_id, filename, session_id)
        try:
            with self._config.provide_session() as driver:
                driver.begin()
                try:
                    rows = driver.select(select_sql, *select_params)
                    driver.execute(delete_sql, *delete_params)
                    driver.commit()
                except BaseException:
                    with contextlib.suppress(Exception):
                        driver.rollback()
                    raise
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return []
            raise
        return [_row_to_stored_artifact(row, deserializer=self._json_deserializer) for row in rows]

    def get_next_version(self, app_name: str, user_id: str, filename: str, session_id: str | None = None) -> int:
        """Compute the next zero-based artifact version number."""
        sql, params = self._get_next_version_sql_and_params(app_name, user_id, filename, session_id)
        try:
            with self._config.provide_session() as driver:
                max_ver = driver.select_value_or_none(sql, *params)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return 0
            raise
        return int(max_ver) + 1 if max_ver is not None else 0

    def delete_artifacts_older_than(self, before: datetime, app_name: str | None = None) -> list[StoredArtifact]:
        """Delete artifact rows recorded strictly before ``before`` and return them."""
        select_sql, delete_sql, params = self._delete_older_than_sqls_and_params(before, app_name)
        try:
            with self._config.provide_session() as driver:
                driver.begin()
                try:
                    rows = driver.select(select_sql, *params)
                    driver.execute(delete_sql, *params)
                    driver.commit()
                except BaseException:
                    with contextlib.suppress(Exception):
                        driver.rollback()
                    raise
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return []
            raise
        return [_row_to_stored_artifact(row, deserializer=self._json_deserializer) for row in rows]


class MySQLAsyncADKArtifactStore(_MySQLADKArtifactSQLMixin, _BaseAsyncADKArtifactStore["MySQLAsyncConfig"]):
    """Asynchronous MySQL ADK artifact metadata store supporting ``ArtifactReference`` records."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    adapter_name = "vertebra_mysql"
    extension_config_options = _MYSQL_ADK_EXTENSION_OPTIONS

    def __init__(self, config: MySQLAsyncConfig, *, read_config: MySQLAsyncConfig | None = None) -> None:
        """Initialize the asynchronous MySQL ADK artifact store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    async def create_table(self) -> None:
        """Create the MySQL ADK artifact metadata table asynchronously."""
        async with self._config.provide_session() as driver:
            await driver.execute_script(self._artifact_table_ddl())
            await driver.commit()

    async def insert_artifact(self, record: StoredArtifact) -> None:
        """Insert or update a ``StoredArtifact`` metadata record asynchronously."""
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
        """Upsert an artifact revision backed by ``ArtifactReference`` asynchronously."""
        sql, params = self._insert_artifact_ref_sql_and_params(
            app_name, user_id, filename, reference, session_id, custom_metadata
        )
        async with self._config.provide_session() as driver:
            await driver.execute(sql, *params)
            await driver.commit()

    async def get_artifact_reference(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> ArtifactReference | None:
        """Fetch an ``ArtifactReference`` revision asynchronously."""
        sql, params = self._get_artifact_sql_and_params(
            app_name, user_id, filename, session_id, version, include_ref_cols=True
        )
        try:
            async with self._provide_replica_session(primary=False) as driver:
                row = await driver.select_one_or_none(sql, *params)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return None
            raise
        return _row_to_artifact_reference(row) if row is not None else None

    async def get_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> StoredArtifact | None:
        """Fetch a ``StoredArtifact`` revision asynchronously."""
        sql, params = self._get_artifact_sql_and_params(
            app_name, user_id, filename, session_id, version, include_ref_cols=False
        )
        try:
            async with self._provide_replica_session(primary=False) as driver:
                row = await driver.select_one_or_none(sql, *params)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return None
            raise
        return _row_to_stored_artifact(row, deserializer=self._json_deserializer) if row is not None else None

    async def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """List distinct artifact filenames asynchronously."""
        sql, params = self._list_artifact_keys_sql_and_params(app_name, user_id, session_id)
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, *params)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return []
            raise
        return [str(row["filename"]) for row in rows]

    async def list_artifact_versions(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[StoredArtifact]:
        """List all stored versions for an artifact sorted ascending asynchronously."""
        sql, params = self._list_artifact_versions_sql_and_params(app_name, user_id, filename, session_id)
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, *params)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return []
            raise
        return [_row_to_stored_artifact(row, deserializer=self._json_deserializer) for row in rows]

    async def delete_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[StoredArtifact]:
        """Delete all metadata versions for an artifact atomically asynchronously and return removed records."""
        select_sql, select_params = self._list_artifact_versions_sql_and_params(app_name, user_id, filename, session_id)
        delete_sql, delete_params = self._delete_artifact_sql_and_params(app_name, user_id, filename, session_id)
        try:
            async with self._config.provide_session() as driver:
                await driver.begin()
                try:
                    rows = await driver.select(select_sql, *select_params)
                    await driver.execute(delete_sql, *delete_params)
                    await driver.commit()
                except BaseException:
                    with contextlib.suppress(Exception):
                        await driver.rollback()
                    raise
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return []
            raise
        return [_row_to_stored_artifact(row, deserializer=self._json_deserializer) for row in rows]

    async def get_next_version(self, app_name: str, user_id: str, filename: str, session_id: str | None = None) -> int:
        """Compute the next zero-based artifact version number asynchronously."""
        sql, params = self._get_next_version_sql_and_params(app_name, user_id, filename, session_id)
        try:
            async with self._config.provide_session() as driver:
                max_ver = await driver.select_value_or_none(sql, *params)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return 0
            raise
        return int(max_ver) + 1 if max_ver is not None else 0

    async def delete_artifacts_older_than(self, before: datetime, app_name: str | None = None) -> list[StoredArtifact]:
        """Delete artifact rows recorded strictly before ``before`` asynchronously and return them."""
        select_sql, delete_sql, params = self._delete_older_than_sqls_and_params(before, app_name)
        try:
            async with self._config.provide_session() as driver:
                await driver.begin()
                try:
                    rows = await driver.select(select_sql, *params)
                    await driver.execute(delete_sql, *params)
                    await driver.commit()
                except BaseException:
                    with contextlib.suppress(Exception):
                        await driver.rollback()
                    raise
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return []
            raise
        return [_row_to_stored_artifact(row, deserializer=self._json_deserializer) for row in rows]
