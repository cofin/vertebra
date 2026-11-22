"""DuckDB ADK session, event, memory (`fts` + `list_cosine_similarity`), and artifact stores."""

from __future__ import annotations

import contextlib
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Final, Literal, NotRequired, TypedDict, cast

from sqlspec.config import ADKConfig
from sqlspec.extensions.adk import (
    BaseAsyncADKStore,
    BaseSyncADKStore,
    StoredEvent,
    StoredSession,
    normalize_session_list_options,
)
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json

from vertebra.extensions.sqlspec.adapters.duckdb.core import (
    build_duckdb_scope_where,
    duckdb_json_dict,
    duckdb_row_value,
    format_duckdb_identifier,
    is_duckdb_table_missing,
    split_schema_and_table,
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

    from vertebra.extensions.sqlspec.adapters.duckdb.config import DuckDBAsyncConfig, DuckDBSyncConfig
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
    "DuckDBADKConfig",
    "DuckDBADKFTSOptions",
    "DuckDBAsyncADKArtifactStore",
    "DuckDBAsyncADKMemoryStore",
    "DuckDBAsyncADKStore",
    "DuckDBSyncADKArtifactStore",
    "DuckDBSyncADKMemoryStore",
    "DuckDBSyncADKStore",
)

logger = get_logger("vertebra.extensions.sqlspec.adapters.duckdb.adk")

_DUCKDB_FTS_ALLOWED_OPTIONS: Final[frozenset[str]] = frozenset(
    {"stemmer", "stopwords", "ignore", "strip_accents", "lower", "overwrite"}
)
_DUCKDB_FTS_CREATE_OPTION_ORDER: Final[tuple[str, ...]] = (
    "stemmer",
    "stopwords",
    "ignore",
    "strip_accents",
    "lower",
    "overwrite",
)
_DUCKDB_FTS_REFRESH_OPTION_ORDER: Final[tuple[str, ...]] = (
    "overwrite",
    "stemmer",
    "stopwords",
    "ignore",
    "strip_accents",
    "lower",
)
_DUCKDB_FTS_DEFAULT_OPTIONS: Final[dict[str, str | int]] = {
    "stemmer": "porter",
    "stopwords": "english",
    "strip_accents": 1,
    "lower": 1,
}
_DUCKDB_AUTHOR_EXTRACT_RE: Final[str] = r'"author"\s*:\s*"([^"]*)"'
_DUCKDB_NODE_PATH_EXTRACT_RE: Final[str] = r'"node_info"\s*:\s*\{[^}]*"path"\s*:\s*"([^"]*)"'
_DUCKDB_ADK_EXTENSION_OPTIONS: Final[frozenset[str]] = frozenset(
    {
        "artifact_enabled",
        "enable_event_generated_column_indexes",
        "enable_event_generated_columns",
        "enable_vector",
        "fts_options",
        "memory_fts_options",
        "memory_retention_days",
        "use_fts",
        "vector_dimensions",
    }
)


class DuckDBADKFTSOptions(TypedDict):
    """DuckDB FTS ``PRAGMA create_fts_index`` options for ADK memory search."""

    stemmer: NotRequired[str]
    """Stemmer name. Defaults to ``"porter"``."""

    stopwords: NotRequired[str]
    """Stopword table or language name. Defaults to ``"english"``."""

    ignore: NotRequired[str]
    """Regular expression ignored by DuckDB tokenization."""

    strip_accents: NotRequired[bool | int]
    """Whether DuckDB strips accents during tokenization. Defaults to ``1``."""

    lower: NotRequired[bool | int]
    """Whether DuckDB lowercases text during tokenization. Defaults to ``1``."""

    overwrite: NotRequired[bool | int]
    """Whether initial FTS index creation overwrites an existing index. Defaults to ``0``."""


class DuckDBADKConfig(ADKConfig, total=False):
    """DuckDB-specific ADK extension configuration keys under ``extension_config['adk']``."""

    enable_event_generated_columns: NotRequired[bool]
    """Create DuckDB virtual generated columns for common ADK event JSON paths."""

    enable_event_generated_column_indexes: NotRequired[bool]
    """Index generated event projection columns when generated columns are enabled."""

    memory_fts_options: NotRequired[DuckDBADKFTSOptions]
    """DuckDB FTS ``PRAGMA create_fts_index`` options for ADK memory search."""

    fts_options: NotRequired[DuckDBADKFTSOptions]
    """Alias for ``memory_fts_options`` for DuckDB FTS tokenization options."""

    use_fts: NotRequired[bool]
    """Alias for ``memory_use_fts`` enabling DuckDB FTS BM25 memory search."""

    enable_vector: NotRequired[bool]
    """Enable DuckDB ``list_cosine_similarity`` vector search over ``FLOAT[]`` embeddings."""

    vector_dimensions: NotRequired[int]
    """Configured embedding vector dimensionality for DuckDB ADK memory."""

    memory_retention_days: NotRequired[int]
    """Optional retention window in days for ADK memory entries."""


def _ensure_utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    if value is None:
        return datetime.now(UTC)
    parsed = datetime.fromisoformat(str(value))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _ensure_optional_utc(value: Any) -> datetime | None:
    if value is None:
        return None
    return _ensure_utc(value)


def _duckdb_index_name(table_name: str, suffix: str) -> str:
    _, bare_name = split_schema_and_table(table_name)
    return format_duckdb_identifier(f"idx_{bare_name}_{suffix}")


def _adk_config(config: Any) -> dict[str, Any]:
    extension_config = getattr(config, "extension_config", None)
    if isinstance(extension_config, Mapping):
        adk_cfg = extension_config.get("adk")
        if isinstance(adk_cfg, Mapping):
            return dict(adk_cfg)
    return {}


def _format_duckdb_fts_option(value: object) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    msg = f"DuckDB ADK memory_fts_options values must be str, int, or bool; got {type(value).__name__}"
    raise TypeError(msg)


def _render_fts_options(config: Any, *, overwrite: bool) -> str:
    options: dict[str, object] = dict(_DUCKDB_FTS_DEFAULT_OPTIONS)
    adk_cfg = _adk_config(config)
    configured = adk_cfg.get("memory_fts_options") or adk_cfg.get("fts_options") or {}
    if isinstance(configured, Mapping):
        unknown = sorted(set(configured) - _DUCKDB_FTS_ALLOWED_OPTIONS)
        if unknown:
            msg = f"Unsupported DuckDB ADK memory_fts_options: {', '.join(unknown)}"
            raise ValueError(msg)
        options.update(configured)
    if overwrite:
        options["overwrite"] = 1
    order = _DUCKDB_FTS_REFRESH_OPTION_ORDER if overwrite else _DUCKDB_FTS_CREATE_OPTION_ORDER
    return ", ".join(f"{name}={_format_duckdb_fts_option(options[name])}" for name in order if name in options)


def _row_to_session(row: Any, *, deserializer: Callable[[Any], Any] = from_json) -> StoredSession:
    return cast(
        "StoredSession",
        {
            "id": str(duckdb_row_value(row, "id", 0)),
            "app_name": str(duckdb_row_value(row, "app_name", 1)),
            "user_id": str(duckdb_row_value(row, "user_id", 2)),
            "state": duckdb_json_dict(duckdb_row_value(row, "state", 3), deserializer=deserializer),
            "create_time": _ensure_utc(duckdb_row_value(row, "create_time", 4)),
            "update_time": _ensure_utc(duckdb_row_value(row, "update_time", 5)),
        },
    )


def _row_to_event(row: Any, *, deserializer: Callable[[Any], Any] = from_json) -> StoredEvent:
    inv = duckdb_row_value(row, "invocation_id", 2)
    return cast(
        "StoredEvent",
        {
            "id": str(duckdb_row_value(row, "id", 0)),
            "session_id": str(duckdb_row_value(row, "session_id", 1)),
            "invocation_id": str(inv) if inv is not None else None,
            "timestamp": _ensure_utc(duckdb_row_value(row, "timestamp", 3)),
            "event_data": duckdb_json_dict(duckdb_row_value(row, "event_data", 4), deserializer=deserializer),
            "app_name": str(duckdb_row_value(row, "app_name", 5)),
            "user_id": str(duckdb_row_value(row, "user_id", 6)),
        },
    )


def _decode_embedding(raw: Any) -> list[float] | None:
    if raw is None:
        return None
    if isinstance(raw, str):
        raw = from_json(raw) if raw else None
    if isinstance(raw, (list, tuple)):
        return [float(x) for x in raw]
    return None


def _row_to_memory(row: Any, *, deserializer: Callable[[Any], Any] = from_json) -> StoredMemory:
    author = duckdb_row_value(row, "author", 6)
    meta_raw = duckdb_row_value(row, "metadata_json", 10)
    emb_raw = duckdb_row_value(row, "embedding", 11)
    return cast(
        "StoredMemory",
        {
            "id": str(duckdb_row_value(row, "id", 0)),
            "session_id": str(duckdb_row_value(row, "session_id", 1)),
            "app_name": str(duckdb_row_value(row, "app_name", 2)),
            "user_id": str(duckdb_row_value(row, "user_id", 3)),
            "scope": str(duckdb_row_value(row, "scope", 4)),
            "event_id": str(duckdb_row_value(row, "event_id", 5)),
            "author": str(author) if author is not None else None,
            "timestamp": _ensure_utc(duckdb_row_value(row, "timestamp", 7)),
            "content_json": duckdb_json_dict(duckdb_row_value(row, "content_json", 8), deserializer=deserializer),
            "content_text": str(duckdb_row_value(row, "content_text", 9)),
            "metadata_json": duckdb_json_dict(meta_raw, deserializer=deserializer) if meta_raw is not None else None,
            "inserted_at": _ensure_utc(duckdb_row_value(row, "inserted_at", 12)),
            "embedding": _decode_embedding(emb_raw),
        },
    )


def _row_to_artifact_reference(row: Mapping[str, Any]) -> ArtifactReference:
    size_val = row.get("size")
    updated_val = row.get("updated") or row.get("created_at")
    return ArtifactReference(
        uri=str(row["canonical_uri"]),
        version=int(row["version"]),
        content_type=str(row["mime_type"]) if row.get("mime_type") is not None else None,
        size=int(size_val) if size_val is not None else None,
        updated=_ensure_optional_utc(updated_val),
        signed_url=str(row["signed_url"]) if row.get("signed_url") is not None else None,
    )


def _row_to_artifact_record(row: Mapping[str, Any]) -> StoredArtifact:
    meta_raw = row.get("custom_metadata")
    return cast(
        "StoredArtifact",
        {
            "app_name": str(row["app_name"]),
            "user_id": str(row["user_id"]),
            "session_id": str(row["session_id"]) if row.get("session_id") else None,
            "filename": str(row["filename"]),
            "version": int(row["version"]),
            "mime_type": str(row["mime_type"]) if row.get("mime_type") is not None else None,
            "canonical_uri": str(row["canonical_uri"]),
            "custom_metadata": duckdb_json_dict(meta_raw) if meta_raw is not None else None,
            "created_at": _ensure_utc(row.get("created_at")),
        },
    )


def _raise_session_not_found(session_id: str) -> None:
    msg = f"Session not found: {session_id}"
    raise ValueError(msg)


def _embedding_bind_pair(entry: StoredMemory) -> tuple[str | None, str]:
    raw_emb = entry.get("embedding")
    if raw_emb is None:
        return None, "[]"
    encoded = to_json([float(x) for x in raw_emb])
    return encoded, encoded


class _DuckDBADKSessionSQLMixin:
    """Shared DuckDB DDL and SQL query builders for sync and async ADK session stores."""

    __slots__ = ()

    _session_table: str
    _events_table: str
    _app_state_table: str
    _user_state_table: str
    _metadata_table: str
    _owner_id_column_ddl: str | None
    _owner_id_column_name: str | None
    _config: Any

    def _session_index_statements(self) -> list[str]:
        session_ref = format_duckdb_identifier(self._session_table)
        return [
            f"CREATE INDEX IF NOT EXISTS {_duckdb_index_name(self._session_table, 'app_user')} ON {session_ref}(app_name, user_id)",
            f"CREATE INDEX IF NOT EXISTS {_duckdb_index_name(self._session_table, 'update_time')} ON {session_ref}(update_time DESC)",
        ]

    def _event_generated_column_index_statements(self) -> list[str]:
        adk_cfg = _adk_config(self._config)
        if not (
            adk_cfg.get("enable_event_generated_columns", False)
            and adk_cfg.get("enable_event_generated_column_indexes", False)
        ):
            return []
        events_ref = format_duckdb_identifier(self._events_table)
        return [
            f"CREATE INDEX IF NOT EXISTS {_duckdb_index_name(self._events_table, 'author_gc')} ON {events_ref}(session_id, author_gc, timestamp ASC)",
            f"CREATE INDEX IF NOT EXISTS {_duckdb_index_name(self._events_table, 'node_path_gc')} ON {events_ref}(session_id, node_path_gc, timestamp ASC)",
        ]

    def _event_index_statements(self) -> list[str]:
        events_ref = format_duckdb_identifier(self._events_table)
        return [
            f"CREATE INDEX IF NOT EXISTS {_duckdb_index_name(self._events_table, 'session')} ON {events_ref}(session_id, timestamp ASC)",
            f"CREATE INDEX IF NOT EXISTS {_duckdb_index_name(self._events_table, 'app_timestamp')} ON {events_ref}(app_name, timestamp ASC)",
            *self._event_generated_column_index_statements(),
        ]

    def _companion_index_statements(self) -> list[str]:
        return [*self._session_index_statements(), *self._event_index_statements()]

    def _build_sessions_ddl(self) -> str:
        session_ref = format_duckdb_identifier(self._session_table)
        owner_line = f",\n    {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        index_lines = ";\n".join(self._session_index_statements())
        return (
            f"CREATE TABLE IF NOT EXISTS {session_ref} (\n"
            "    id VARCHAR PRIMARY KEY,\n"
            "    app_name VARCHAR NOT NULL,\n"
            f"    user_id VARCHAR NOT NULL{owner_line},\n"
            "    state VARCHAR NOT NULL,\n"
            "    create_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            "    update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP\n"
            ");\n"
            f"{index_lines};"
        )

    def _build_events_ddl(self) -> str:
        events_ref = format_duckdb_identifier(self._events_table)
        adk_cfg = _adk_config(self._config)
        gen_cols = ""
        if adk_cfg.get("enable_event_generated_columns", False):
            gen_cols = (
                ",\n    author_gc VARCHAR GENERATED ALWAYS AS "
                f"(NULLIF(regexp_extract(event_data, '{_DUCKDB_AUTHOR_EXTRACT_RE}', 1), '')) VIRTUAL,\n"
                "    node_path_gc VARCHAR GENERATED ALWAYS AS "
                f"(NULLIF(regexp_extract(event_data, '{_DUCKDB_NODE_PATH_EXTRACT_RE}', 1), '')) VIRTUAL"
            )
        index_lines = ";\n".join(self._event_index_statements())
        return (
            f"CREATE TABLE IF NOT EXISTS {events_ref} (\n"
            "    id VARCHAR PRIMARY KEY,\n"
            "    app_name VARCHAR NOT NULL,\n"
            "    user_id VARCHAR NOT NULL,\n"
            "    session_id VARCHAR NOT NULL,\n"
            "    invocation_id VARCHAR,\n"
            "    timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            f"    event_data VARCHAR NOT NULL{gen_cols}\n"
            ");\n"
            f"{index_lines};"
        )

    def _build_app_states_ddl(self) -> str:
        app_ref = format_duckdb_identifier(self._app_state_table)
        return (
            f"CREATE TABLE IF NOT EXISTS {app_ref} (\n"
            "    app_name VARCHAR PRIMARY KEY,\n"
            "    state VARCHAR NOT NULL,\n"
            "    update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP\n"
            ");"
        )

    def _build_user_states_ddl(self) -> str:
        user_ref = format_duckdb_identifier(self._user_state_table)
        return (
            f"CREATE TABLE IF NOT EXISTS {user_ref} (\n"
            "    app_name VARCHAR NOT NULL,\n"
            "    user_id VARCHAR NOT NULL,\n"
            "    state VARCHAR NOT NULL,\n"
            "    update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            "    PRIMARY KEY (app_name, user_id)\n"
            ");"
        )

    def _build_metadata_ddl(self) -> str:
        meta_ref = format_duckdb_identifier(self._metadata_table)
        return f"CREATE TABLE IF NOT EXISTS {meta_ref} (\n    key VARCHAR PRIMARY KEY,\n    value VARCHAR NOT NULL\n);"

    def _drop_app_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {format_duckdb_identifier(self._app_state_table)}"

    def _drop_user_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {format_duckdb_identifier(self._user_state_table)}"

    def _drop_metadata_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {format_duckdb_identifier(self._metadata_table)}"

    def _drop_tables_sql(self) -> list[str]:
        return [
            self._drop_metadata_table_sql(),
            self._drop_user_states_table_sql(),
            self._drop_app_states_table_sql(),
            f"DROP TABLE IF EXISTS {format_duckdb_identifier(self._events_table)}",
            f"DROP TABLE IF EXISTS {format_duckdb_identifier(self._session_table)}",
        ]

    @staticmethod
    def _validate_event_scope(event_record: StoredEvent, app_name: str, user_id: str, session_id: str) -> None:
        if (event_record["app_name"], event_record["user_id"], event_record["session_id"]) != (
            app_name,
            user_id,
            session_id,
        ):
            msg = (
                f"event_record scope ({event_record['app_name']!r}, {event_record['user_id']!r}, "
                f"{event_record['session_id']!r}) does not match target ({app_name!r}, {user_id!r}, {session_id!r})"
            )
            raise ValueError(msg)

    def _create_session_sql_and_params(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
        state: dict[str, Any],
        owner_id: Any | None,
        now_iso: str,
    ) -> tuple[str, tuple[Any, ...]]:
        session_ref = format_duckdb_identifier(self._session_table)
        state_json = to_json(state)
        if self._owner_id_column_name:
            owner_ident = format_duckdb_identifier(self._owner_id_column_name)
            sql = (
                f"INSERT INTO {session_ref} "
                f"(id, app_name, user_id, {owner_ident}, state, create_time, update_time) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)"
            )
            return sql, (session_id, app_name, user_id, owner_id, state_json, now_iso, now_iso)
        sql = (
            f"INSERT INTO {session_ref} (id, app_name, user_id, state, create_time, update_time) "
            "VALUES (?, ?, ?, ?, ?, ?)"
        )
        return sql, (session_id, app_name, user_id, state_json, now_iso, now_iso)

    def _get_session_select_sql(self) -> str:
        session_ref = format_duckdb_identifier(self._session_table)
        return (
            f"SELECT id, app_name, user_id, state, create_time, update_time FROM {session_ref} "
            "WHERE app_name = ? AND user_id = ? AND id = ?"
        )

    def _get_session_renew_sql(self) -> str:
        session_ref = format_duckdb_identifier(self._session_table)
        return (
            f"UPDATE {session_ref} SET update_time = ? "
            "WHERE app_name = ? AND user_id = ? AND id = ? "
            "RETURNING id, app_name, user_id, state, create_time, update_time"
        )

    def _update_session_state_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        state: dict[str, Any],
    ) -> tuple[str, tuple[Any, ...]]:
        session_ref = format_duckdb_identifier(self._session_table)
        now_iso = datetime.now(UTC).isoformat()
        sql = f"UPDATE {session_ref} SET state = ?, update_time = ? WHERE app_name = ? AND user_id = ? AND id = ?"
        return sql, (to_json(state), now_iso, app_name, user_id, session_id)

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
        table_ref = format_duckdb_identifier(self._session_table)
        params: list[Any] = [app_name]
        where = "WHERE app_name = ?"
        if user_id is not None:
            where += " AND user_id = ?"
            params.append(user_id)
        sql = (
            f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table_ref} "
            f"{where} ORDER BY {column} {direction}, id {direction}"
        )
        if page_limit is not None:
            sql += " LIMIT ? OFFSET ?"
            params.extend((int(page_limit), int(page_offset)))
        return sql, tuple(params)

    def _delete_session_sqls(self) -> tuple[str, str]:
        events_ref = format_duckdb_identifier(self._events_table)
        session_ref = format_duckdb_identifier(self._session_table)
        delete_events_sql = f"DELETE FROM {events_ref} WHERE app_name = ? AND user_id = ? AND session_id = ?"
        delete_session_sql = f"DELETE FROM {session_ref} WHERE app_name = ? AND user_id = ? AND id = ?"
        return delete_events_sql, delete_session_sql

    def _append_event_sql_and_params(self, event_record: StoredEvent) -> tuple[str, tuple[Any, ...]]:
        events_ref = format_duckdb_identifier(self._events_table)
        session_ref = format_duckdb_identifier(self._session_table)
        app_name = event_record["app_name"]
        user_id = event_record["user_id"]
        session_id = event_record["session_id"]
        sql = (
            f"INSERT INTO {events_ref} "
            "(id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
            "SELECT ?, ?, ?, ?, ?, ?, ? "
            f"WHERE EXISTS (SELECT 1 FROM {session_ref} WHERE id = ? AND app_name = ? AND user_id = ?) "
            "RETURNING 1 AS inserted"
        )
        params = (
            event_record["id"],
            app_name,
            user_id,
            session_id,
            event_record["invocation_id"],
            _ensure_utc(event_record["timestamp"]).isoformat(),
            to_json(event_record["event_data"]),
            session_id,
            app_name,
            user_id,
        )
        return sql, params

    def _insert_event_sql_and_params(self, event_record: StoredEvent) -> tuple[str, tuple[Any, ...]]:
        events_ref = format_duckdb_identifier(self._events_table)
        sql = (
            f"INSERT INTO {events_ref} "
            "(id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        params = (
            event_record["id"],
            event_record["app_name"],
            event_record["user_id"],
            event_record["session_id"],
            event_record["invocation_id"],
            _ensure_utc(event_record["timestamp"]).isoformat(),
            to_json(event_record["event_data"]),
        )
        return sql, params

    def _update_session_returning_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        state: dict[str, Any],
        now_iso: str,
    ) -> tuple[str, tuple[Any, ...]]:
        session_ref = format_duckdb_identifier(self._session_table)
        sql = (
            f"UPDATE {session_ref} SET state = ?, update_time = ? "
            "WHERE app_name = ? AND user_id = ? AND id = ? "
            "RETURNING id, app_name, user_id, state, create_time, update_time"
        )
        return sql, (to_json(state), now_iso, app_name, user_id, session_id)

    def _get_events_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None,
        limit: int | None,
    ) -> tuple[str, tuple[Any, ...]]:
        events_ref = format_duckdb_identifier(self._events_table)
        session_ref = format_duckdb_identifier(self._session_table)
        clauses = ["s.app_name = ?", "s.user_id = ?", "s.id = ?"]
        params: list[Any] = [app_name, user_id, session_id]
        if after_timestamp is not None:
            clauses.append("e.timestamp > ?")
            params.append(after_timestamp.astimezone(UTC).isoformat())
        if limit is not None:
            order_and_limit = "ORDER BY e.timestamp DESC, e.rowid DESC LIMIT ?"
            params.append(int(limit))
        else:
            order_and_limit = "ORDER BY e.timestamp ASC"
        sql = (
            "SELECT e.id, e.session_id, e.invocation_id, e.timestamp, e.event_data, s.app_name, s.user_id "
            f"FROM {events_ref} e "
            f"JOIN {session_ref} s ON e.session_id = s.id AND e.app_name = s.app_name AND e.user_id = s.user_id "
            f"WHERE {' AND '.join(clauses)} {order_and_limit}"
        )
        return sql, tuple(params)

    def _delete_before_sql_and_params(
        self,
        table_name: str,
        column: str,
        cutoff: datetime,
        app_name: str | None,
    ) -> tuple[str, tuple[Any, ...]]:
        ref = format_duckdb_identifier(table_name)
        where = f"WHERE {column} < ?"
        params: list[Any] = [cutoff.astimezone(UTC).isoformat()]
        if app_name is not None:
            where += " AND app_name = ?"
            params.append(app_name)
        return f"DELETE FROM {ref} {where} RETURNING 1 AS deleted", tuple(params)

    def _delete_idle_sessions_sqls_and_params(
        self,
        updated_before: datetime,
        app_name: str | None,
    ) -> tuple[str, str, tuple[Any, ...]]:
        events_ref = format_duckdb_identifier(self._events_table)
        session_ref = format_duckdb_identifier(self._session_table)
        where = "WHERE update_time < ?"
        params: list[Any] = [updated_before.astimezone(UTC).isoformat()]
        if app_name is not None:
            where += " AND app_name = ?"
            params.append(app_name)
        delete_events_sql = f"DELETE FROM {events_ref} WHERE session_id IN (SELECT id FROM {session_ref} {where})"
        delete_sessions_sql = f"DELETE FROM {session_ref} {where} RETURNING 1 AS deleted"
        return delete_events_sql, delete_sessions_sql, tuple(params)

    @staticmethod
    def _select_state_sql(table_name: str, key_columns: tuple[str, ...]) -> str:
        table_ref = format_duckdb_identifier(table_name)
        where = " AND ".join(f"{format_duckdb_identifier(col)} = ?" for col in key_columns)
        return f"SELECT state FROM {table_ref} WHERE {where}"

    @staticmethod
    def _upsert_state_sql(table_name: str, key_columns: tuple[str, ...]) -> str:
        table_ref = format_duckdb_identifier(table_name)
        cols = ", ".join(format_duckdb_identifier(col) for col in (*key_columns, "state", "update_time"))
        placeholders = ", ".join("?" for _ in range(len(key_columns) + 2))
        conflict = ", ".join(format_duckdb_identifier(col) for col in key_columns)
        return (
            f"INSERT INTO {table_ref} ({cols}) VALUES ({placeholders}) "
            f"ON CONFLICT({conflict}) DO UPDATE SET state = excluded.state, update_time = excluded.update_time"
        )

    def _get_metadata_sql(self) -> str:
        meta_ref = format_duckdb_identifier(self._metadata_table)
        return f"SELECT value FROM {meta_ref} WHERE key = ?"

    def _set_metadata_sql(self) -> str:
        meta_ref = format_duckdb_identifier(self._metadata_table)
        return (
            f"INSERT INTO {meta_ref} (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        )


class DuckDBSyncADKStore(_DuckDBADKSessionSQLMixin, BaseSyncADKStore["DuckDBSyncConfig"]):
    """Synchronous DuckDB ADK session and event store over Vertebra `DuckDBSyncDriver`."""

    __slots__ = ()

    adapter_name = "vertebra_duckdb"
    _max_table_name_parts = 2
    extension_config_options = _DUCKDB_ADK_EXTENSION_OPTIONS

    def __init__(self, config: DuckDBSyncConfig, *, read_config: DuckDBSyncConfig | None = None) -> None:
        """Initialize the synchronous DuckDB ADK session store."""
        super().__init__(config, read_config=read_config)

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

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Create missing session and event companion indexes without committing."""
        for statement in self._companion_index_statements():
            driver.execute(statement)

    def create_tables(self) -> None:
        """Create ADK session, event, state, and metadata tables synchronously."""
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
        self.reconcile_schema(assume_existing=True)

    def _select_state_sync(
        self,
        driver: Any,
        table_name: str,
        key_columns: tuple[str, ...],
        key_values: tuple[Any, ...],
    ) -> dict[str, Any] | None:
        try:
            raw = driver.select_value_or_none(self._select_state_sql(table_name, key_columns), *key_values)
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return None
            raise
        return duckdb_json_dict(raw) if raw is not None else None

    def _apply_state_delta_sync(
        self,
        driver: Any,
        table_name: str,
        key_columns: tuple[str, ...],
        key_values: tuple[Any, ...],
        delta: Mapping[str, Any],
        now_iso: str,
    ) -> dict[str, Any]:
        existing = self._select_state_sync(driver, table_name, key_columns, key_values)
        merged = dict(existing) if existing else {}
        merged.update(delta)
        driver.execute(self._upsert_state_sql(table_name, key_columns), *key_values, to_json(merged), now_iso)
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
        """Create a new ADK session synchronously and merge optional scoped state deltas."""
        now = datetime.now(UTC)
        now_iso = now.isoformat()
        session_state = dict(state)
        sql, params = self._create_session_sql_and_params(
            session_id, app_name, user_id, session_state, owner_id, now_iso
        )
        with self._config.provide_session() as driver:
            driver.begin()
            try:
                driver.execute(sql, *params)
                resolved_app = (
                    self._apply_state_delta_sync(
                        driver, self._app_state_table, ("app_name",), (app_name,), app_state_delta, now_iso
                    )
                    if app_state_delta
                    else self._select_state_sync(driver, self._app_state_table, ("app_name",), (app_name,))
                )
                resolved_user = (
                    self._apply_state_delta_sync(
                        driver,
                        self._user_state_table,
                        ("app_name", "user_id"),
                        (app_name, user_id),
                        user_state_delta,
                        now_iso,
                    )
                    if user_state_delta
                    else self._select_state_sync(
                        driver, self._user_state_table, ("app_name", "user_id"), (app_name, user_id)
                    )
                )
                driver.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    driver.rollback()
                raise
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
        """Fetch an ADK session synchronously, optionally renewing `update_time`."""
        should_renew = self._is_renewal_requested(renew_for)
        try:
            with self._provide_replica_session(primary=renew_for is not None) as driver:
                if should_renew:
                    now_iso = datetime.now(UTC).isoformat()
                    row = driver.select_one_or_none(
                        self._get_session_renew_sql(), now_iso, app_name, user_id, session_id
                    )
                    driver.commit()
                else:
                    row = driver.select_one_or_none(self._get_session_select_sql(), app_name, user_id, session_id)
                return _row_to_session(row, deserializer=from_json) if row is not None else None
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return None
            raise

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Update stored session JSON state synchronously."""
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
        """List stored ADK sessions synchronously."""
        built = self._list_sessions_sql_and_params(app_name, user_id, order_by, descending, limit, offset)
        if built is None:
            return []
        sql, params = built
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, *params)
            return [_row_to_session(row, deserializer=from_json) for row in rows]
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return []
            raise

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete a session and its associated events synchronously."""
        delete_events_sql, delete_session_sql = self._delete_session_sqls()
        with self._config.provide_session() as driver:
            driver.begin()
            try:
                driver.execute(delete_events_sql, app_name, user_id, session_id)
                driver.execute(delete_session_sql, app_name, user_id, session_id)
                driver.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    driver.rollback()
                raise

    def append_event(self, event_record: StoredEvent) -> None:
        """Append a single ADK event synchronously after verifying owning session existence."""
        sql, params = self._append_event_sql_and_params(event_record)
        with self._config.provide_session() as driver:
            inserted = driver.select_one_or_none(sql, *params)
            driver.commit()
        if inserted is None:
            msg = (
                f"Session {event_record['session_id']} not found "
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
        """Atomically append an event, update session state, and apply scoped deltas synchronously."""
        self._validate_event_scope(event_record, app_name, user_id, session_id)
        now_iso = datetime.now(UTC).isoformat()
        update_sql, update_params = self._update_session_returning_sql_and_params(
            app_name, user_id, session_id, state, now_iso
        )
        insert_sql, insert_params = self._insert_event_sql_and_params(event_record)
        with self._config.provide_session() as driver:
            driver.begin()
            try:
                row = driver.select_one_or_none(update_sql, *update_params)
                if row is None:
                    _raise_session_not_found(session_id)
                driver.execute(insert_sql, *insert_params)
                merged_app = (
                    self._apply_state_delta_sync(
                        driver, self._app_state_table, ("app_name",), (app_name,), app_state_delta, now_iso
                    )
                    if app_state_delta
                    else None
                )
                merged_user = (
                    self._apply_state_delta_sync(
                        driver,
                        self._user_state_table,
                        ("app_name", "user_id"),
                        (app_name, user_id),
                        user_state_delta,
                        now_iso,
                    )
                    if user_state_delta
                    else None
                )
                driver.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    driver.rollback()
                raise
        session_record = _row_to_session(row, deserializer=from_json)
        session_record["state"] = merge_scoped_state(session_record["state"], merged_app, merged_user)
        return session_record

    def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> list[StoredEvent]:
        """Fetch ADK events for a session ordered ascending by timestamp."""
        if limit == 0:
            return []
        sql, params = self._get_events_sql_and_params(app_name, user_id, session_id, after_timestamp, limit)
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, *params)
            if limit is not None:
                rows = list(reversed(rows))
            return [_row_to_event(row, deserializer=from_json) for row in rows]
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return []
            raise

    def _delete_by_cutoff(
        self,
        table_name: str,
        column: str,
        cutoff: datetime,
        app_name: str | None,
    ) -> int:
        sql, params = self._delete_before_sql_and_params(table_name, column, cutoff, app_name)
        try:
            with self._config.provide_session() as driver:
                rows = driver.select(sql, *params)
                driver.commit()
            return len(rows)
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return 0
            raise

    def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Delete events older than `before` synchronously."""
        return self._delete_by_cutoff(self._events_table, "timestamp", before, app_name)

    def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete sessions (and child events) whose `update_time` predates `updated_before`."""
        delete_events_sql, delete_sessions_sql, params = self._delete_idle_sessions_sqls_and_params(
            updated_before, app_name
        )
        try:
            with self._config.provide_session() as driver:
                driver.begin()
                try:
                    driver.execute(delete_events_sql, *params)
                    rows = driver.select(delete_sessions_sql, *params)
                    driver.commit()
                except BaseException:
                    with contextlib.suppress(Exception):
                        driver.rollback()
                    raise
            return len(rows)
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return 0
            raise

    def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete user state rows whose `update_time` predates `updated_before`."""
        return self._delete_by_cutoff(self._user_state_table, "update_time", updated_before, app_name)

    def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Return app-scoped ADK state dictionary synchronously."""
        sql = self._select_state_sql(self._app_state_table, ("app_name",))
        try:
            with self._provide_replica_session(primary=False) as driver:
                raw = driver.select_value_or_none(sql, app_name)
            return duckdb_json_dict(raw) if raw is not None else None
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return None
            raise

    def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Return user-scoped ADK state dictionary synchronously."""
        sql = self._select_state_sql(self._user_state_table, ("app_name", "user_id"))
        try:
            with self._provide_replica_session(primary=False) as driver:
                raw = driver.select_value_or_none(sql, app_name, user_id)
            return duckdb_json_dict(raw) if raw is not None else None
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return None
            raise

    def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Insert or update app-scoped ADK state synchronously."""
        sql = self._upsert_state_sql(self._app_state_table, ("app_name",))
        with self._config.provide_session() as driver:
            driver.execute(sql, app_name, to_json(state), datetime.now(UTC).isoformat())
            driver.commit()

    def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Insert or update user-scoped ADK state synchronously."""
        sql = self._upsert_state_sql(self._user_state_table, ("app_name", "user_id"))
        with self._config.provide_session() as driver:
            driver.execute(sql, app_name, user_id, to_json(state), datetime.now(UTC).isoformat())
            driver.commit()

    def get_metadata(self, key: str) -> str | None:
        """Fetch an internal ADK metadata string synchronously."""
        try:
            with self._provide_replica_session(primary=False) as driver:
                val = driver.select_value_or_none(self._get_metadata_sql(), key)
            return str(val) if val is not None else None
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return None
            raise

    def set_metadata(self, key: str, value: str) -> None:
        """Store or overwrite an internal ADK metadata entry synchronously."""
        with self._config.provide_session() as driver:
            driver.execute(self._set_metadata_sql(), key, value)
            driver.commit()


class DuckDBAsyncADKStore(_DuckDBADKSessionSQLMixin, BaseAsyncADKStore["DuckDBAsyncConfig"]):
    """Asynchronous DuckDB ADK session and event store over Vertebra `DuckDBAsyncDriver`."""

    __slots__ = ()

    adapter_name = "vertebra_duckdb"
    _max_table_name_parts = 2
    extension_config_options = _DUCKDB_ADK_EXTENSION_OPTIONS

    def __init__(self, config: DuckDBAsyncConfig, *, read_config: DuckDBAsyncConfig | None = None) -> None:
        """Initialize the asynchronous DuckDB ADK session store."""
        super().__init__(config, read_config=read_config)

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

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Create missing session and event companion indexes without committing."""
        for statement in self._companion_index_statements():
            await driver.execute(statement)

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
        await self.reconcile_schema(assume_existing=True)

    async def _select_state_async(
        self,
        driver: Any,
        table_name: str,
        key_columns: tuple[str, ...],
        key_values: tuple[Any, ...],
    ) -> dict[str, Any] | None:
        try:
            raw = await driver.select_value_or_none(self._select_state_sql(table_name, key_columns), *key_values)
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return None
            raise
        return duckdb_json_dict(raw) if raw is not None else None

    async def _apply_state_delta_async(
        self,
        driver: Any,
        table_name: str,
        key_columns: tuple[str, ...],
        key_values: tuple[Any, ...],
        delta: Mapping[str, Any],
        now_iso: str,
    ) -> dict[str, Any]:
        existing = await self._select_state_async(driver, table_name, key_columns, key_values)
        merged = dict(existing) if existing else {}
        merged.update(delta)
        await driver.execute(self._upsert_state_sql(table_name, key_columns), *key_values, to_json(merged), now_iso)
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
        """Create a new ADK session asynchronously and merge optional scoped state deltas."""
        now = datetime.now(UTC)
        now_iso = now.isoformat()
        session_state = dict(state)
        sql, params = self._create_session_sql_and_params(
            session_id, app_name, user_id, session_state, owner_id, now_iso
        )
        async with self._config.provide_session() as driver:
            await driver.begin()
            try:
                await driver.execute(sql, *params)
                resolved_app = (
                    await self._apply_state_delta_async(
                        driver, self._app_state_table, ("app_name",), (app_name,), app_state_delta, now_iso
                    )
                    if app_state_delta
                    else await self._select_state_async(driver, self._app_state_table, ("app_name",), (app_name,))
                )
                resolved_user = (
                    await self._apply_state_delta_async(
                        driver,
                        self._user_state_table,
                        ("app_name", "user_id"),
                        (app_name, user_id),
                        user_state_delta,
                        now_iso,
                    )
                    if user_state_delta
                    else await self._select_state_async(
                        driver, self._user_state_table, ("app_name", "user_id"), (app_name, user_id)
                    )
                )
                await driver.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    await driver.rollback()
                raise
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
        """Fetch an ADK session asynchronously, optionally renewing `update_time`."""
        should_renew = self._is_renewal_requested(renew_for)
        try:
            async with self._provide_replica_session(primary=renew_for is not None) as driver:
                if should_renew:
                    now_iso = datetime.now(UTC).isoformat()
                    row = await driver.select_one_or_none(
                        self._get_session_renew_sql(), now_iso, app_name, user_id, session_id
                    )
                    await driver.commit()
                else:
                    row = await driver.select_one_or_none(self._get_session_select_sql(), app_name, user_id, session_id)
                return _row_to_session(row, deserializer=from_json) if row is not None else None
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return None
            raise

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Update stored session JSON state asynchronously."""
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
        """List stored ADK sessions asynchronously."""
        built = self._list_sessions_sql_and_params(app_name, user_id, order_by, descending, limit, offset)
        if built is None:
            return []
        sql, params = built
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, *params)
            return [_row_to_session(row, deserializer=from_json) for row in rows]
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return []
            raise

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete a session and its associated events asynchronously."""
        delete_events_sql, delete_session_sql = self._delete_session_sqls()
        async with self._config.provide_session() as driver:
            await driver.begin()
            try:
                await driver.execute(delete_events_sql, app_name, user_id, session_id)
                await driver.execute(delete_session_sql, app_name, user_id, session_id)
                await driver.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    await driver.rollback()
                raise

    async def append_event(self, event_record: StoredEvent) -> None:
        """Append a single ADK event asynchronously after verifying owning session existence."""
        sql, params = self._append_event_sql_and_params(event_record)
        async with self._config.provide_session() as driver:
            inserted = await driver.select_one_or_none(sql, *params)
            await driver.commit()
        if inserted is None:
            msg = (
                f"Session {event_record['session_id']} not found "
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
        self._validate_event_scope(event_record, app_name, user_id, session_id)
        now_iso = datetime.now(UTC).isoformat()
        update_sql, update_params = self._update_session_returning_sql_and_params(
            app_name, user_id, session_id, state, now_iso
        )
        insert_sql, insert_params = self._insert_event_sql_and_params(event_record)
        async with self._config.provide_session() as driver:
            await driver.begin()
            try:
                row = await driver.select_one_or_none(update_sql, *update_params)
                if row is None:
                    _raise_session_not_found(session_id)
                await driver.execute(insert_sql, *insert_params)
                merged_app = (
                    await self._apply_state_delta_async(
                        driver, self._app_state_table, ("app_name",), (app_name,), app_state_delta, now_iso
                    )
                    if app_state_delta
                    else None
                )
                merged_user = (
                    await self._apply_state_delta_async(
                        driver,
                        self._user_state_table,
                        ("app_name", "user_id"),
                        (app_name, user_id),
                        user_state_delta,
                        now_iso,
                    )
                    if user_state_delta
                    else None
                )
                await driver.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    await driver.rollback()
                raise
        session_record = _row_to_session(row, deserializer=from_json)
        session_record["state"] = merge_scoped_state(session_record["state"], merged_app, merged_user)
        return session_record

    async def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> list[StoredEvent]:
        """Fetch ADK events asynchronously ordered ascending by timestamp."""
        if limit == 0:
            return []
        sql, params = self._get_events_sql_and_params(app_name, user_id, session_id, after_timestamp, limit)
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, *params)
            if limit is not None:
                rows = list(reversed(rows))
            return [_row_to_event(row, deserializer=from_json) for row in rows]
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return []
            raise

    async def _delete_by_cutoff(
        self,
        table_name: str,
        column: str,
        cutoff: datetime,
        app_name: str | None,
    ) -> int:
        sql, params = self._delete_before_sql_and_params(table_name, column, cutoff, app_name)
        try:
            async with self._config.provide_session() as driver:
                rows = await driver.select(sql, *params)
                await driver.commit()
            return len(rows)
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return 0
            raise

    async def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Delete events older than `before` asynchronously."""
        return await self._delete_by_cutoff(self._events_table, "timestamp", before, app_name)

    async def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete sessions (and child events) whose `update_time` predates `updated_before`."""
        delete_events_sql, delete_sessions_sql, params = self._delete_idle_sessions_sqls_and_params(
            updated_before, app_name
        )
        try:
            async with self._config.provide_session() as driver:
                await driver.begin()
                try:
                    await driver.execute(delete_events_sql, *params)
                    rows = await driver.select(delete_sessions_sql, *params)
                    await driver.commit()
                except BaseException:
                    with contextlib.suppress(Exception):
                        await driver.rollback()
                    raise
            return len(rows)
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return 0
            raise

    async def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete user state rows whose `update_time` predates `updated_before`."""
        return await self._delete_by_cutoff(self._user_state_table, "update_time", updated_before, app_name)

    async def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Return app-scoped ADK state dictionary asynchronously."""
        sql = self._select_state_sql(self._app_state_table, ("app_name",))
        try:
            async with self._provide_replica_session(primary=False) as driver:
                raw = await driver.select_value_or_none(sql, app_name)
            return duckdb_json_dict(raw) if raw is not None else None
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return None
            raise

    async def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Return user-scoped ADK state dictionary asynchronously."""
        sql = self._select_state_sql(self._user_state_table, ("app_name", "user_id"))
        try:
            async with self._provide_replica_session(primary=False) as driver:
                raw = await driver.select_value_or_none(sql, app_name, user_id)
            return duckdb_json_dict(raw) if raw is not None else None
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return None
            raise

    async def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Insert or update app-scoped ADK state asynchronously."""
        sql = self._upsert_state_sql(self._app_state_table, ("app_name",))
        async with self._config.provide_session() as driver:
            await driver.execute(sql, app_name, to_json(state), datetime.now(UTC).isoformat())
            await driver.commit()

    async def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Insert or update user-scoped ADK state asynchronously."""
        sql = self._upsert_state_sql(self._user_state_table, ("app_name", "user_id"))
        async with self._config.provide_session() as driver:
            await driver.execute(sql, app_name, user_id, to_json(state), datetime.now(UTC).isoformat())
            await driver.commit()

    async def get_metadata(self, key: str) -> str | None:
        """Fetch an internal ADK metadata string asynchronously."""
        try:
            async with self._provide_replica_session(primary=False) as driver:
                val = await driver.select_value_or_none(self._get_metadata_sql(), key)
            return str(val) if val is not None else None
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return None
            raise

    async def set_metadata(self, key: str, value: str) -> None:
        """Store or overwrite an internal ADK metadata entry asynchronously."""
        async with self._config.provide_session() as driver:
            await driver.execute(self._set_metadata_sql(), key, value)
            await driver.commit()


class _DuckDBADKMemorySQLMixin:
    """Shared DuckDB DDL and SQL query builders for sync and async ADK memory stores."""

    __slots__ = ()

    _enabled: bool
    _memory_table: str
    _use_fts: bool
    _max_results: int
    _owner_id_column_ddl: str | None
    _owner_id_column_name: str | None
    _config: Any

    def _memory_index_statements(self) -> list[str]:
        table_ref = format_duckdb_identifier(self._memory_table)
        idx_app_scope_user_time = _duckdb_index_name(self._memory_table, "app_scope_user_time")
        idx_scope = _duckdb_index_name(self._memory_table, "scope")
        idx_session = _duckdb_index_name(self._memory_table, "session")
        return [
            f"CREATE INDEX IF NOT EXISTS {idx_app_scope_user_time} ON {table_ref}(app_name, scope, user_id, timestamp DESC)",
            f"CREATE INDEX IF NOT EXISTS {idx_scope} ON {table_ref}(app_name, scope)",
            f"CREATE INDEX IF NOT EXISTS {idx_session} ON {table_ref}(session_id)",
        ]

    def _build_memory_table_ddl(self) -> str:
        table_ref = format_duckdb_identifier(self._memory_table)
        owner_line = f",\n    {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        index_lines = ";\n".join(self._memory_index_statements())
        return (
            f"CREATE TABLE IF NOT EXISTS {table_ref} (\n"
            "    id VARCHAR(128) PRIMARY KEY,\n"
            "    session_id VARCHAR(128) NOT NULL,\n"
            "    app_name VARCHAR(128) NOT NULL,\n"
            "    user_id VARCHAR(128) NOT NULL,\n"
            "    scope VARCHAR(16) NOT NULL DEFAULT 'user',\n"
            "    event_id VARCHAR(128) NOT NULL,\n"
            f"    author VARCHAR(256){owner_line},\n"
            "    timestamp TIMESTAMPTZ NOT NULL,\n"
            "    content_json VARCHAR NOT NULL,\n"
            "    content_text TEXT NOT NULL,\n"
            "    metadata_json VARCHAR,\n"
            "    embedding FLOAT[],\n"
            "    inserted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            "    UNIQUE (app_name, user_id, event_id)\n"
            ");\n"
            f"{index_lines};"
        )

    def _drop_memory_table_sql(self) -> list[str]:
        return [f"DROP TABLE IF EXISTS {format_duckdb_identifier(self._memory_table)}"]

    def _fts_schema_name(self) -> str:
        schema, bare_table = split_schema_and_table(self._memory_table)
        return f"fts_{schema}_{bare_table}"

    def _fts_index_ddl(self, *, overwrite: bool) -> str:
        opts = _render_fts_options(self._config, overwrite=overwrite)
        escaped_table = self._memory_table.replace("'", "''")
        return f"PRAGMA create_fts_index('{escaped_table}', 'id', 'content_text', {opts})"

    def _build_memory_insert_sql_and_params(
        self,
        entries: Sequence[StoredMemory],
        owner_id: object | None,
    ) -> tuple[str, list[tuple[Any, ...]]]:
        table_ref = format_duckdb_identifier(self._memory_table)
        emb_expr = "CASE WHEN ? IS NULL THEN NULL ELSE CAST(? AS FLOAT[]) END"
        if self._owner_id_column_name:
            owner_ident = format_duckdb_identifier(self._owner_id_column_name)
            sql = (
                f"INSERT INTO {table_ref} ("
                "id, session_id, app_name, user_id, scope, event_id, author, "
                f"{owner_ident}, timestamp, content_json, content_text, metadata_json, embedding, inserted_at"
                f") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, {emb_expr}, ?) "
                "ON CONFLICT (app_name, user_id, event_id) DO NOTHING RETURNING 1 AS inserted"
            )
        else:
            sql = (
                f"INSERT INTO {table_ref} ("
                "id, session_id, app_name, user_id, scope, event_id, author, "
                "timestamp, content_json, content_text, metadata_json, embedding, inserted_at"
                f") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, {emb_expr}, ?) "
                "ON CONFLICT (app_name, user_id, event_id) DO NOTHING RETURNING 1 AS inserted"
            )
        rows: list[tuple[Any, ...]] = []
        for entry in entries:
            emb_null, emb_val = _embedding_bind_pair(entry)
            metadata_json = entry.get("metadata_json")
            meta_str = to_json(metadata_json) if metadata_json is not None else None
            ts_iso = _ensure_utc(entry["timestamp"]).isoformat()
            ins_iso = _ensure_utc(entry["inserted_at"]).isoformat()
            content_str = to_json(entry["content_json"])
            if self._owner_id_column_name:
                rows.append(
                    (
                        entry["id"],
                        entry["session_id"],
                        entry["app_name"],
                        entry["user_id"],
                        entry.get("scope", "user"),
                        entry["event_id"],
                        entry.get("author"),
                        owner_id,
                        ts_iso,
                        content_str,
                        entry["content_text"],
                        meta_str,
                        emb_null,
                        emb_val,
                        ins_iso,
                    )
                )
            else:
                rows.append(
                    (
                        entry["id"],
                        entry["session_id"],
                        entry["app_name"],
                        entry["user_id"],
                        entry.get("scope", "user"),
                        entry["event_id"],
                        entry.get("author"),
                        ts_iso,
                        content_str,
                        entry["content_text"],
                        meta_str,
                        emb_null,
                        emb_val,
                        ins_iso,
                    )
                )
        return sql, rows

    def _cosine_search_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        scope_filter: Literal["all", "user", "app"],
        embedding: Sequence[float],
        limit_value: int,
    ) -> tuple[str, tuple[Any, ...]]:
        dim = len(embedding)
        table_ref = format_duckdb_identifier(self._memory_table)
        where_scope, scope_params = build_duckdb_scope_where(app_name, user_id, scope_filter)
        vec_json = to_json([float(x) for x in embedding])
        sql = (
            "SELECT id, session_id, app_name, user_id, scope, event_id, author, "
            "timestamp, content_json, content_text, metadata_json, CAST(embedding AS VARCHAR) AS embedding, inserted_at, "
            "list_cosine_similarity(embedding, CAST(? AS FLOAT[])) AS score "
            f"FROM {table_ref} "
            f"WHERE {where_scope} AND embedding IS NOT NULL AND len(embedding) = ? "
            "ORDER BY score DESC LIMIT ?"
        )
        return sql, (vec_json, *scope_params, dim, limit_value)

    def _fts_search_sql_and_params(
        self,
        query: str,
        app_name: str,
        user_id: str,
        scope_filter: Literal["all", "user", "app"],
        limit_value: int,
    ) -> tuple[str, tuple[Any, ...]]:
        table_ref = format_duckdb_identifier(self._memory_table)
        fts_macro = f"{format_duckdb_identifier(self._fts_schema_name())}.match_bm25"
        where_scope, scope_params = build_duckdb_scope_where(app_name, user_id, scope_filter, prefix="m")
        sql = (
            "SELECT m.id, m.session_id, m.app_name, m.user_id, m.scope, m.event_id, m.author, "
            "m.timestamp, m.content_json, m.content_text, m.metadata_json, "
            "CAST(m.embedding AS VARCHAR) AS embedding, m.inserted_at "
            f"FROM {table_ref} m "
            f"JOIN (SELECT id, {fts_macro}(id, ?, fields := 'content_text') AS score FROM {table_ref}) fts "
            f"ON m.id = fts.id WHERE {where_scope} AND fts.score IS NOT NULL "
            "ORDER BY fts.score DESC LIMIT ?"
        )
        return sql, (query, *scope_params, limit_value)

    def _ilike_search_sql_and_params(
        self,
        query: str,
        app_name: str,
        user_id: str,
        scope_filter: Literal["all", "user", "app"],
        limit_value: int,
    ) -> tuple[str, tuple[Any, ...]]:
        table_ref = format_duckdb_identifier(self._memory_table)
        where_scope, scope_params = build_duckdb_scope_where(app_name, user_id, scope_filter)
        has_query = bool(query and query.strip())
        if has_query:
            sql = (
                "SELECT id, session_id, app_name, user_id, scope, event_id, author, "
                "timestamp, content_json, content_text, metadata_json, "
                "CAST(embedding AS VARCHAR) AS embedding, inserted_at "
                f"FROM {table_ref} WHERE {where_scope} AND content_text ILIKE ? ORDER BY timestamp DESC LIMIT ?"
            )
            return sql, (*scope_params, f"%{query}%", limit_value)
        sql = (
            "SELECT id, session_id, app_name, user_id, scope, event_id, author, "
            "timestamp, content_json, content_text, metadata_json, "
            "CAST(embedding AS VARCHAR) AS embedding, inserted_at "
            f"FROM {table_ref} WHERE {where_scope} ORDER BY timestamp DESC LIMIT ?"
        )
        return sql, (*scope_params, limit_value)

    def _delete_by_session_sql_and_params(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
    ) -> tuple[str, tuple[str, str, str]]:
        table_ref = format_duckdb_identifier(self._memory_table)
        sql = f"DELETE FROM {table_ref} WHERE session_id = ? AND app_name = ? AND user_id = ? RETURNING 1 AS deleted"
        return sql, (session_id, app_name, user_id)

    def _delete_older_than_sql_and_params(
        self,
        days: int,
        app_name: str | None,
        scope: str | None,
    ) -> tuple[str, tuple[Any, ...]]:
        table_ref = format_duckdb_identifier(self._memory_table)
        cutoff = (datetime.now(UTC) - timedelta(days=int(days))).isoformat()
        clauses = ["inserted_at < ?"]
        params: list[Any] = [cutoff]
        if app_name is not None:
            clauses.append("app_name = ?")
            params.append(app_name)
        if scope is not None:
            clauses.append("scope = ?")
            params.append(scope)
        sql = f"DELETE FROM {table_ref} WHERE {' AND '.join(clauses)} RETURNING 1 AS deleted"
        return sql, tuple(params)


class DuckDBSyncADKMemoryStore(_DuckDBADKMemorySQLMixin, _BaseSyncADKMemoryStore["DuckDBSyncConfig"]):
    """Synchronous DuckDB ADK memory store with `list_cosine_similarity` vector ranking and BM25 `fts`."""

    __slots__ = ("_enable_vector",)

    adapter_name = "vertebra_duckdb"
    _max_table_name_parts = 2
    extension_config_options = _DUCKDB_ADK_EXTENSION_OPTIONS

    def __init__(self, config: DuckDBSyncConfig, *, read_config: DuckDBSyncConfig | None = None) -> None:
        """Initialize the synchronous DuckDB ADK memory store."""
        super().__init__(config, read_config=read_config)
        adk_cfg = _adk_config(config)
        if "memory_use_fts" not in adk_cfg and "use_fts" in adk_cfg:
            self._use_fts = bool(adk_cfg["use_fts"])
        self._enable_vector = bool(adk_cfg.get("enable_vector", True))

    def _memory_table_ddl(self) -> str:
        return self._build_memory_table_ddl()

    def _ensure_fts_extension(self, driver: Any) -> bool:
        with contextlib.suppress(Exception):
            driver.execute("LOAD fts")
            return True
        with contextlib.suppress(Exception):
            driver.execute("INSTALL fts")
        try:
            driver.execute("LOAD fts")
        except Exception as exc:
            logger.debug("DuckDB FTS extension unavailable: %s", exc)
            return False
        return True

    def _refresh_fts_index(self, driver: Any, *, overwrite: bool) -> None:
        if not self._ensure_fts_extension(driver):
            return
        try:
            driver.execute(self._fts_index_ddl(overwrite=overwrite))
            driver.commit()
        except Exception as exc:
            logger.debug("DuckDB FTS index build skipped: %s", exc)

    def _fts_index_exists(self, driver: Any) -> bool:
        rows = driver.select(
            "SELECT 1 AS present FROM information_schema.schemata WHERE lower(schema_name) = lower(?)",
            self._fts_schema_name(),
        )
        return bool(rows)

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Create missing DuckDB memory companion indexes and FTS index without committing."""
        if not self._enabled:
            return
        for statement in self._memory_index_statements():
            driver.execute(statement)
        if self._use_fts and not self._fts_index_exists(driver) and self._ensure_fts_extension(driver):
            with contextlib.suppress(Exception):
                driver.execute(self._fts_index_ddl(overwrite=False))

    def create_tables(self) -> None:
        """Create the DuckDB memory table and optional FTS index synchronously."""
        if not self._enabled:
            return
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return
        with self._config.provide_session() as driver:
            driver.execute_script(self._memory_table_ddl())
            driver.commit()
            if self._use_fts:
                self._refresh_fts_index(driver, overwrite=False)
        self.reconcile_schema(assume_existing=True)

    def insert_memory_entries(self, entries: list[StoredMemory], owner_id: object | None = None) -> int:
        """Bulk insert ADK memory entries with `(app_name, user_id, event_id)` deduplication synchronously."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0
        sql, rows_params = self._build_memory_insert_sql_and_params(entries, owner_id)
        inserted = 0
        with self._config.provide_session() as driver:
            driver.begin()
            try:
                for params in rows_params:
                    res = driver.select(sql, *params)
                    inserted += len(res)
                driver.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    driver.rollback()
                raise
            if self._use_fts and inserted > 0:
                self._refresh_fts_index(driver, overwrite=True)
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
        """Search ADK memory via SQL `list_cosine_similarity`, DuckDB BM25 `fts`, or `ILIKE`."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        has_query = bool(query and query.strip())
        if not has_query and embedding is None:
            return []
        limit_value = limit if limit is not None else self._max_results
        if limit_value <= 0:
            return []

        rows: list[dict[str, Any]] | None = None
        if self._enable_vector and embedding is not None and embedding:
            vec_sql, vec_params = self._cosine_search_sql_and_params(
                app_name, user_id, scope_filter, embedding, limit_value
            )
            try:
                with self._provide_replica_session(primary=False) as driver:
                    rows = driver.select(vec_sql, *vec_params)
            except Exception as exc:
                if is_duckdb_table_missing(exc):
                    return []
                raise

        if rows is None and has_query and self._use_fts:
            try:
                with self._provide_replica_session(primary=False) as driver:
                    if self._ensure_fts_extension(driver):
                        fts_sql, fts_params = self._fts_search_sql_and_params(
                            query, app_name, user_id, scope_filter, limit_value
                        )
                        rows = driver.select(fts_sql, *fts_params)
            except Exception as exc:
                if is_duckdb_table_missing(exc):
                    return []
                raise

        if rows is None:
            if not has_query:
                return []
            ilike_sql, ilike_params = self._ilike_search_sql_and_params(
                query, app_name, user_id, scope_filter, limit_value
            )
            try:
                with self._provide_replica_session(primary=False) as driver:
                    rows = driver.select(ilike_sql, *ilike_params)
            except Exception as exc:
                if is_duckdb_table_missing(exc):
                    return []
                raise

        return [_row_to_memory(row, deserializer=from_json) for row in (rows or ())]

    def delete_entries_by_session(self, session_id: str, app_name: str, user_id: str) -> int:
        """Delete memory entries for `session_id` owned by `app_name` and `user_id` synchronously."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        sql, params = self._delete_by_session_sql_and_params(session_id, app_name, user_id)
        try:
            with self._config.provide_session() as driver:
                rows = driver.select(sql, *params)
                driver.commit()
                count = len(rows)
                if self._use_fts and count > 0:
                    self._refresh_fts_index(driver, overwrite=True)
                return count
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return 0
            raise

    def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
        """Delete memory entries older than `days` days synchronously."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        sql, params = self._delete_older_than_sql_and_params(days, app_name, scope)
        try:
            with self._config.provide_session() as driver:
                rows = driver.select(sql, *params)
                driver.commit()
                count = len(rows)
                if self._use_fts and count > 0:
                    self._refresh_fts_index(driver, overwrite=True)
                return count
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return 0
            raise


class DuckDBAsyncADKMemoryStore(_DuckDBADKMemorySQLMixin, _BaseAsyncADKMemoryStore["DuckDBAsyncConfig"]):
    """Asynchronous DuckDB ADK memory store with `list_cosine_similarity` vector ranking and BM25 `fts`."""

    __slots__ = ("_enable_vector",)

    adapter_name = "vertebra_duckdb"
    _max_table_name_parts = 2
    extension_config_options = _DUCKDB_ADK_EXTENSION_OPTIONS

    def __init__(self, config: DuckDBAsyncConfig, *, read_config: DuckDBAsyncConfig | None = None) -> None:
        """Initialize the asynchronous DuckDB ADK memory store."""
        super().__init__(config, read_config=read_config)
        adk_cfg = _adk_config(config)
        if "memory_use_fts" not in adk_cfg and "use_fts" in adk_cfg:
            self._use_fts = bool(adk_cfg["use_fts"])
        self._enable_vector = bool(adk_cfg.get("enable_vector", True))

    async def _memory_table_ddl(self) -> str:
        return self._build_memory_table_ddl()

    async def _ensure_fts_extension(self, driver: Any) -> bool:
        with contextlib.suppress(Exception):
            await driver.execute("LOAD fts")
            return True
        with contextlib.suppress(Exception):
            await driver.execute("INSTALL fts")
        try:
            await driver.execute("LOAD fts")
        except Exception as exc:
            logger.debug("DuckDB FTS extension unavailable: %s", exc)
            return False
        return True

    async def _refresh_fts_index(self, driver: Any, *, overwrite: bool) -> None:
        if not await self._ensure_fts_extension(driver):
            return
        try:
            await driver.execute(self._fts_index_ddl(overwrite=overwrite))
            await driver.commit()
        except Exception as exc:
            logger.debug("DuckDB FTS index build skipped: %s", exc)

    async def _fts_index_exists(self, driver: Any) -> bool:
        rows = await driver.select(
            "SELECT 1 AS present FROM information_schema.schemata WHERE lower(schema_name) = lower(?)",
            self._fts_schema_name(),
        )
        return bool(rows)

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Create missing DuckDB memory companion indexes and FTS index without committing."""
        if not self._enabled:
            return
        for statement in self._memory_index_statements():
            await driver.execute(statement)
        if self._use_fts and not await self._fts_index_exists(driver) and await self._ensure_fts_extension(driver):
            with contextlib.suppress(Exception):
                await driver.execute(self._fts_index_ddl(overwrite=False))

    async def create_tables(self) -> None:
        """Create the DuckDB memory table and optional FTS index asynchronously."""
        if not self._enabled:
            return
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        ddl = await self._memory_table_ddl()
        async with self._config.provide_session() as driver:
            await driver.execute_script(ddl)
            await driver.commit()
            if self._use_fts:
                await self._refresh_fts_index(driver, overwrite=False)
        await self.reconcile_schema(assume_existing=True)

    async def insert_memory_entries(self, entries: list[StoredMemory], owner_id: object | None = None) -> int:
        """Bulk insert ADK memory entries with `(app_name, user_id, event_id)` deduplication asynchronously."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0
        sql, rows_params = self._build_memory_insert_sql_and_params(entries, owner_id)
        inserted = 0
        async with self._config.provide_session() as driver:
            await driver.begin()
            try:
                for params in rows_params:
                    res = await driver.select(sql, *params)
                    inserted += len(res)
                await driver.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    await driver.rollback()
                raise
            if self._use_fts and inserted > 0:
                await self._refresh_fts_index(driver, overwrite=True)
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
        """Search ADK memory asynchronously via SQL `list_cosine_similarity`, BM25 `fts`, or `ILIKE`."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        has_query = bool(query and query.strip())
        if not has_query and embedding is None:
            return []
        limit_value = limit if limit is not None else self._max_results
        if limit_value <= 0:
            return []

        rows: list[dict[str, Any]] | None = None
        if self._enable_vector and embedding is not None and embedding:
            vec_sql, vec_params = self._cosine_search_sql_and_params(
                app_name, user_id, scope_filter, embedding, limit_value
            )
            try:
                async with self._provide_replica_session(primary=False) as driver:
                    rows = await driver.select(vec_sql, *vec_params)
            except Exception as exc:
                if is_duckdb_table_missing(exc):
                    return []
                raise

        if rows is None and has_query and self._use_fts:
            try:
                async with self._provide_replica_session(primary=False) as driver:
                    if await self._ensure_fts_extension(driver):
                        fts_sql, fts_params = self._fts_search_sql_and_params(
                            query, app_name, user_id, scope_filter, limit_value
                        )
                        rows = await driver.select(fts_sql, *fts_params)
            except Exception as exc:
                if is_duckdb_table_missing(exc):
                    return []
                raise

        if rows is None:
            if not has_query:
                return []
            ilike_sql, ilike_params = self._ilike_search_sql_and_params(
                query, app_name, user_id, scope_filter, limit_value
            )
            try:
                async with self._provide_replica_session(primary=False) as driver:
                    rows = await driver.select(ilike_sql, *ilike_params)
            except Exception as exc:
                if is_duckdb_table_missing(exc):
                    return []
                raise

        return [_row_to_memory(row, deserializer=from_json) for row in (rows or ())]

    async def delete_entries_by_session(self, session_id: str, app_name: str, user_id: str) -> int:
        """Delete memory entries for `session_id` owned by `app_name` and `user_id` asynchronously."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        sql, params = self._delete_by_session_sql_and_params(session_id, app_name, user_id)
        try:
            async with self._config.provide_session() as driver:
                rows = await driver.select(sql, *params)
                await driver.commit()
                count = len(rows)
                if self._use_fts and count > 0:
                    await self._refresh_fts_index(driver, overwrite=True)
                return count
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return 0
            raise

    async def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
        """Delete memory entries older than `days` days asynchronously."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        sql, params = self._delete_older_than_sql_and_params(days, app_name, scope)
        try:
            async with self._config.provide_session() as driver:
                rows = await driver.select(sql, *params)
                await driver.commit()
                count = len(rows)
                if self._use_fts and count > 0:
                    await self._refresh_fts_index(driver, overwrite=True)
                return count
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return 0
            raise


class _DuckDBADKArtifactSQLMixin:
    """Shared DuckDB DDL and SQL query builders for sync and async ADK artifact stores."""

    __slots__ = ()

    _artifact_table: str

    def _artifact_table_ddl(self) -> str:
        table_ref = format_duckdb_identifier(self._artifact_table)
        return (
            f"CREATE TABLE IF NOT EXISTS {table_ref} (\n"
            "    app_name VARCHAR NOT NULL,\n"
            "    user_id VARCHAR NOT NULL,\n"
            "    session_id VARCHAR NOT NULL DEFAULT '',\n"
            "    filename VARCHAR NOT NULL,\n"
            "    version BIGINT NOT NULL,\n"
            "    mime_type VARCHAR,\n"
            "    canonical_uri VARCHAR NOT NULL,\n"
            "    custom_metadata VARCHAR,\n"
            "    size BIGINT,\n"
            "    updated TIMESTAMPTZ,\n"
            "    signed_url VARCHAR,\n"
            "    created_at TIMESTAMPTZ NOT NULL,\n"
            "    PRIMARY KEY (app_name, user_id, session_id, filename, version)\n"
            ");"
        )

    def _insert_artifact_sql_and_params(self, record: StoredArtifact) -> tuple[str, tuple[Any, ...]]:
        table_ref = format_duckdb_identifier(self._artifact_table)
        session_val = record.get("session_id") or ""
        meta = record.get("custom_metadata")
        sql = (
            f"INSERT INTO {table_ref} "
            "(app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        params = (
            record["app_name"],
            record["user_id"],
            session_val,
            record["filename"],
            int(record["version"]),
            record.get("mime_type"),
            record["canonical_uri"],
            to_json(meta) if meta is not None else None,
            _ensure_utc(record.get("created_at")).isoformat(),
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
        table_ref = format_duckdb_identifier(self._artifact_table)
        session_val = session_id if session_id is not None else ""
        now_dt = reference.updated or datetime.now(UTC)
        sql = (
            f"INSERT INTO {table_ref} "
            "(app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, size, updated, signed_url, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        params = (
            app_name,
            user_id,
            session_val,
            filename,
            int(reference.version),
            reference.content_type,
            reference.uri,
            to_json(custom_metadata) if custom_metadata is not None else None,
            reference.size,
            _ensure_utc(reference.updated).isoformat() if reference.updated is not None else None,
            reference.signed_url,
            _ensure_utc(now_dt).isoformat(),
        )
        return sql, params

    def _get_artifact_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None,
        version: int | None,
    ) -> tuple[str, tuple[Any, ...]]:
        table_ref = format_duckdb_identifier(self._artifact_table)
        session_val = session_id if session_id is not None else ""
        if version is not None:
            sql = (
                f"SELECT * FROM {table_ref} "
                "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? AND version = ? LIMIT 1"
            )
            return sql, (app_name, user_id, session_val, filename, int(version))
        sql = (
            f"SELECT * FROM {table_ref} "
            "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? ORDER BY version DESC LIMIT 1"
        )
        return sql, (app_name, user_id, session_val, filename)

    def _list_artifact_keys_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        session_id: str | None,
    ) -> tuple[str, tuple[Any, ...]]:
        table_ref = format_duckdb_identifier(self._artifact_table)
        if session_id is not None:
            sql = (
                f"SELECT DISTINCT filename FROM {table_ref} "
                "WHERE app_name = ? AND user_id = ? AND session_id IN ('', ?) ORDER BY filename ASC"
            )
            return sql, (app_name, user_id, session_id)
        sql = (
            f"SELECT DISTINCT filename FROM {table_ref} "
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
        table_ref = format_duckdb_identifier(self._artifact_table)
        session_val = session_id if session_id is not None else ""
        sql = (
            f"SELECT * FROM {table_ref} "
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
        table_ref = format_duckdb_identifier(self._artifact_table)
        session_val = session_id if session_id is not None else ""
        sql = f"DELETE FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ?"
        return sql, (app_name, user_id, session_val, filename)

    def _get_next_version_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None,
    ) -> tuple[str, tuple[str, str, str, str]]:
        table_ref = format_duckdb_identifier(self._artifact_table)
        session_val = session_id if session_id is not None else ""
        sql = (
            f"SELECT MAX(version) FROM {table_ref} "
            "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ?"
        )
        return sql, (app_name, user_id, session_val, filename)

    def _delete_older_than_sqls_and_params(
        self,
        before: datetime,
        app_name: str | None,
    ) -> tuple[str, str, tuple[Any, ...]]:
        table_ref = format_duckdb_identifier(self._artifact_table)
        cutoff = _ensure_utc(before).isoformat()
        if app_name is not None:
            select_sql = f"SELECT * FROM {table_ref} WHERE created_at < ? AND app_name = ? ORDER BY created_at ASC"
            delete_sql = f"DELETE FROM {table_ref} WHERE created_at < ? AND app_name = ?"
            return select_sql, delete_sql, (cutoff, app_name)
        select_sql = f"SELECT * FROM {table_ref} WHERE created_at < ? ORDER BY created_at ASC"
        delete_sql = f"DELETE FROM {table_ref} WHERE created_at < ?"
        return select_sql, delete_sql, (cutoff,)


class DuckDBSyncADKArtifactStore(_DuckDBADKArtifactSQLMixin, _BaseSyncADKArtifactStore["DuckDBSyncConfig"]):
    """Synchronous DuckDB ADK artifact metadata store backed by `ArtifactReference`."""

    __slots__ = ()

    adapter_name = "vertebra_duckdb"
    extension_config_options = _DUCKDB_ADK_EXTENSION_OPTIONS

    def __init__(self, config: DuckDBSyncConfig, *, read_config: DuckDBSyncConfig | None = None) -> None:
        """Initialize the synchronous DuckDB ADK artifact store."""
        super().__init__(config, read_config=read_config)

    def create_table(self) -> None:
        """Create the DuckDB artifact metadata table if absent."""
        with self._config.provide_session() as driver:
            driver.execute_script(self._artifact_table_ddl())
            driver.commit()

    def insert_artifact(self, record: StoredArtifact) -> None:
        """Insert a `StoredArtifact` metadata record synchronously."""
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
        """Persist an artifact version backed by `ArtifactReference` synchronously."""
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
        """Fetch an artifact revision as an `ArtifactReference` struct synchronously."""
        sql, params = self._get_artifact_sql_and_params(app_name, user_id, filename, session_id, version)
        try:
            with self._provide_replica_session(primary=False) as driver:
                row = driver.select_one_or_none(sql, *params)
            return _row_to_artifact_reference(row) if row is not None else None
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return None
            raise

    def get_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> StoredArtifact | None:
        """Fetch a `StoredArtifact` metadata record synchronously."""
        sql, params = self._get_artifact_sql_and_params(app_name, user_id, filename, session_id, version)
        try:
            with self._provide_replica_session(primary=False) as driver:
                row = driver.select_one_or_none(sql, *params)
            return _row_to_artifact_record(row) if row is not None else None
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return None
            raise

    def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """List distinct artifact filenames within user or session scope synchronously."""
        sql, params = self._list_artifact_keys_sql_and_params(app_name, user_id, session_id)
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, *params)
            return [str(row["filename"]) for row in rows]
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return []
            raise

    def list_artifact_versions(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[StoredArtifact]:
        """List ascending `StoredArtifact` records for a stored artifact synchronously."""
        sql, params = self._list_artifact_versions_sql_and_params(app_name, user_id, filename, session_id)
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, *params)
            return [_row_to_artifact_record(row) for row in rows]
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return []
            raise

    def delete_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[StoredArtifact]:
        """Delete all stored versions of an artifact and return removed records synchronously."""
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
            return [_row_to_artifact_record(row) for row in rows]
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return []
            raise

    def get_next_version(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> int:
        """Compute the next monotonically increasing revision integer synchronously."""
        sql, params = self._get_next_version_sql_and_params(app_name, user_id, filename, session_id)
        try:
            with self._config.provide_session() as driver:
                max_ver = driver.select_value_or_none(sql, *params)
            return (int(max_ver) + 1) if max_ver is not None else 0
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return 0
            raise

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
            return [_row_to_artifact_record(row) for row in rows]
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return []
            raise


class DuckDBAsyncADKArtifactStore(_DuckDBADKArtifactSQLMixin, _BaseAsyncADKArtifactStore["DuckDBAsyncConfig"]):
    """Asynchronous DuckDB ADK artifact metadata store backed by `ArtifactReference`."""

    __slots__ = ()

    adapter_name = "vertebra_duckdb"
    extension_config_options = _DUCKDB_ADK_EXTENSION_OPTIONS

    def __init__(self, config: DuckDBAsyncConfig, *, read_config: DuckDBAsyncConfig | None = None) -> None:
        """Initialize the asynchronous DuckDB ADK artifact store."""
        super().__init__(config, read_config=read_config)

    async def create_table(self) -> None:
        """Create the DuckDB artifact metadata table asynchronously."""
        async with self._config.provide_session() as driver:
            await driver.execute_script(self._artifact_table_ddl())
            await driver.commit()

    async def insert_artifact(self, record: StoredArtifact) -> None:
        """Insert a `StoredArtifact` metadata record asynchronously."""
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
        """Persist an artifact version backed by `ArtifactReference` asynchronously."""
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
        """Fetch an artifact revision as an `ArtifactReference` struct asynchronously."""
        sql, params = self._get_artifact_sql_and_params(app_name, user_id, filename, session_id, version)
        try:
            async with self._provide_replica_session(primary=False) as driver:
                row = await driver.select_one_or_none(sql, *params)
            return _row_to_artifact_reference(row) if row is not None else None
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return None
            raise

    async def get_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> StoredArtifact | None:
        """Fetch a `StoredArtifact` metadata record asynchronously."""
        sql, params = self._get_artifact_sql_and_params(app_name, user_id, filename, session_id, version)
        try:
            async with self._provide_replica_session(primary=False) as driver:
                row = await driver.select_one_or_none(sql, *params)
            return _row_to_artifact_record(row) if row is not None else None
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return None
            raise

    async def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """List distinct artifact filenames within user or session scope asynchronously."""
        sql, params = self._list_artifact_keys_sql_and_params(app_name, user_id, session_id)
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, *params)
            return [str(row["filename"]) for row in rows]
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return []
            raise

    async def list_artifact_versions(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[StoredArtifact]:
        """List ascending `StoredArtifact` records for a stored artifact asynchronously."""
        sql, params = self._list_artifact_versions_sql_and_params(app_name, user_id, filename, session_id)
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, *params)
            return [_row_to_artifact_record(row) for row in rows]
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return []
            raise

    async def delete_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[StoredArtifact]:
        """Delete all stored versions of an artifact and return removed records asynchronously."""
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
            return [_row_to_artifact_record(row) for row in rows]
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return []
            raise

    async def get_next_version(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> int:
        """Compute the next monotonically increasing revision integer asynchronously."""
        sql, params = self._get_next_version_sql_and_params(app_name, user_id, filename, session_id)
        try:
            async with self._config.provide_session() as driver:
                max_ver = await driver.select_value_or_none(sql, *params)
            return (int(max_ver) + 1) if max_ver is not None else 0
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return 0
            raise

    async def delete_artifacts_older_than(self, before: datetime, app_name: str | None = None) -> list[StoredArtifact]:
        """Delete artifact rows recorded strictly before ``before`` and return them asynchronously."""
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
            return [_row_to_artifact_record(row) for row in rows]
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return []
            raise
