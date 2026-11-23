"""SQLite ADK session, event, memory (FTS5), and artifact metadata stores."""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, NotRequired, cast

from sqlspec.config import ADKConfig
from sqlspec.exceptions import SQLParsingError
from sqlspec.extensions.adk import (
    StoredEvent,
    StoredSession,
    normalize_session_list_options,
)
from sqlspec.utils.serializers import from_json, to_json

from vertebra.extensions.sqlspec._adk import (
    AdapterAsyncADKArtifactStore,
    AdapterAsyncADKMemoryStore,
    AdapterAsyncADKStore,
    AdapterSyncADKArtifactStore,
    AdapterSyncADKMemoryStore,
    AdapterSyncADKStore,
    StoredArtifact,
    StoredMemory,
    merge_scoped_state,
)
from vertebra.extensions.sqlspec.adapters.sqlite.core import (
    JULIAN_EPOCH,
    SECONDS_PER_DAY,
    SQLITE_TABLE_NOT_FOUND_TEXT,
    apply_pragmas_async,
    apply_pragmas_sync,
    build_sqlite_scope_clause,
    cosine_similarity,
    datetime_to_julian,
    extension_pragma_statements,
    format_fts_options,
    julian_to_datetime,
    pack_embedding,
    unpack_embedding,
    validate_fts_options,
)
from vertebra.storage import ArtifactReference
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from sqlspec.extensions.adk import SessionOrderBy, StoredMemory
    from sqlspec.extensions.adk.artifact import StoredArtifact
    from sqlspec.extensions.adk.converters import merge_scoped_state

    from vertebra.extensions.sqlspec.adapters.sqlite.config import SQLiteAsyncConfig, SQLiteSyncConfig
else:
    ensure_module("sqlspec.extensions.adk.memory.store", package="google-adk", extra="adk")

__all__ = (
    "JULIAN_EPOCH",
    "SECONDS_PER_DAY",
    "SQLiteADKConfig",
    "SQLiteAsyncADKArtifactStore",
    "SQLiteAsyncADKMemoryStore",
    "SQLiteAsyncADKStore",
    "SQLiteSyncADKArtifactStore",
    "SQLiteSyncADKMemoryStore",
    "SQLiteSyncADKStore",
)

logger = logging.getLogger("vertebra.extensions.sqlspec.adapters.sqlite.adk")

_SESSION_BASE_PRAGMAS: Final[tuple[str, ...]] = (
    "PRAGMA foreign_keys = ON",
    "PRAGMA cache_size = -64000",
    "PRAGMA mmap_size = 30000000",
    "PRAGMA journal_size_limit = 67108864",
)
_SQLITE_ADK_EXTENSION_OPTIONS: Final[frozenset[str]] = frozenset(
    {
        "pragma_overrides",
        "fts_tokenize",
        "fts_detail",
        "memory_retention_days",
        "use_fts",
    }
)


def _adk_settings(config: Any) -> dict[str, Any]:
    """Return ``extension_config['adk']`` as a mutable dictionary."""
    ext = getattr(config, "extension_config", {})
    if not isinstance(ext, Mapping):
        return {}
    adk_cfg = ext.get("adk", {})
    return dict(adk_cfg) if isinstance(adk_cfg, Mapping) else {}


def _is_missing_table_error(exc: BaseException) -> bool:
    """Return ``True`` when an exception reports a missing SQLite table."""
    return SQLITE_TABLE_NOT_FOUND_TEXT in str(exc).lower()


def _adk_pragma_statements(config: Any) -> list[str]:
    """Build validated SQLite PRAGMA statements for ADK connections."""
    adk_cfg = _adk_settings(config)
    overrides = adk_cfg.get("pragma_overrides")
    if overrides is not None and not isinstance(overrides, Mapping):
        msg = "extension_config['adk']['pragma_overrides'] must be a mapping of PRAGMA names to values"
        raise ImproperConfigurationError(msg)
    merged: list[str] = list(_SESSION_BASE_PRAGMAS)
    if overrides is not None:
        synthetic_cfg = cast(
            "Any", type("_TmpCfg", (), {"extension_config": {"adk": {"pragma_overrides": overrides}}})()
        )
        merged.extend(extension_pragma_statements(synthetic_cfg, "adk"))
    return merged


def _decode_state_json(raw: Any) -> dict[str, Any]:
    """Decode a persisted JSON state payload into a dictionary."""
    if not raw:
        return {}
    decoded = from_json(raw)
    return dict(decoded) if isinstance(decoded, Mapping) else {}


def _row_to_stored_session(row: Mapping[str, Any]) -> StoredSession:
    """Convert a SQLite session row mapping into a ``StoredSession`` record."""
    return StoredSession(
        id=str(row["id"]),
        app_name=str(row["app_name"]),
        user_id=str(row["user_id"]),
        state=_decode_state_json(row.get("state")),
        create_time=julian_to_datetime(row["create_time"]) or datetime.now(UTC),
        update_time=julian_to_datetime(row["update_time"]) or datetime.now(UTC),
    )


def _row_to_stored_event(row: Mapping[str, Any]) -> StoredEvent:
    """Convert a SQLite event row mapping into a ``StoredEvent`` record."""
    raw_event = row.get("event_data")
    return StoredEvent(
        id=str(row["id"]),
        app_name=str(row["app_name"]),
        user_id=str(row["user_id"]),
        session_id=str(row["session_id"]),
        invocation_id=cast("str", row.get("invocation_id")),
        timestamp=julian_to_datetime(row["timestamp"]) or datetime.now(UTC),
        event_data=from_json(raw_event) if raw_event else {},
    )


def _row_to_stored_memory(row: Mapping[str, Any]) -> StoredMemory:
    """Convert a SQLite memory row mapping into a ``StoredMemory`` record."""
    raw_content = row.get("content_json")
    raw_meta = row.get("metadata_json")
    return cast(
        "StoredMemory",
        {
            "id": str(row["id"]),
            "session_id": str(row["session_id"]),
            "app_name": str(row["app_name"]),
            "user_id": str(row["user_id"]),
            "scope": str(row.get("scope") or "user"),
            "event_id": str(row["event_id"]),
            "author": row.get("author"),
            "timestamp": julian_to_datetime(row["timestamp"]),
            "content_json": from_json(raw_content) if raw_content else {},
            "content_text": str(row["content_text"]),
            "metadata_json": from_json(raw_meta) if raw_meta else None,
            "inserted_at": julian_to_datetime(row["inserted_at"]),
            "embedding": unpack_embedding(row.get("embedding")),
        },
    )


def _row_to_stored_artifact(row: Mapping[str, Any]) -> StoredArtifact:
    """Convert a SQLite artifact row mapping into a ``StoredArtifact`` record."""
    raw_session = row.get("session_id")
    raw_meta = row.get("custom_metadata")
    return cast(
        "StoredArtifact",
        {
            "app_name": str(row["app_name"]),
            "user_id": str(row["user_id"]),
            "session_id": str(raw_session) if raw_session else None,
            "filename": str(row["filename"]),
            "version": int(row["version"]),
            "mime_type": row.get("mime_type"),
            "canonical_uri": str(row["canonical_uri"]),
            "custom_metadata": from_json(raw_meta) if raw_meta else None,
            "created_at": julian_to_datetime(row["created_at"]) or datetime.now(UTC),
        },
    )


def _row_to_artifact_reference(row: Mapping[str, Any]) -> ArtifactReference:
    """Convert a SQLite artifact row mapping into an ``ArtifactReference``."""
    updated_val = row.get("updated") if row.get("updated") is not None else row.get("created_at")
    raw_size = row.get("size")
    return ArtifactReference(
        uri=str(row["canonical_uri"]),
        version=int(row["version"]),
        content_type=str(row["mime_type"]) if row.get("mime_type") is not None else None,
        size=int(raw_size) if raw_size is not None else None,
        updated=julian_to_datetime(updated_val) if updated_val is not None else None,
        signed_url=str(row["signed_url"]) if row.get("signed_url") is not None else None,
    )


def _item_embedding(item: Mapping[str, Any]) -> Sequence[float] | None:
    """Return a memory entry's embedding vector when present."""
    raw = item.get("embedding")
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        return raw
    return None


def _rank_by_embedding(
    records: list[StoredMemory],
    embedding: Sequence[float] | None,
    limit: int,
) -> list[StoredMemory]:
    """Re-rank retrieved memory records by cosine similarity when an embedding is provided."""
    if embedding is not None and records:
        query_vec = [float(x) for x in embedding]
        records.sort(
            key=lambda item: cosine_similarity(query_vec, vec) if (vec := _item_embedding(item)) is not None else -1.0,
            reverse=True,
        )
        return records[:limit]
    return records[:limit]


class SQLiteADKConfig(ADKConfig, total=False):
    """SQLite-specific ADK configuration adding PRAGMA, retention, and FTS5 tuning fields."""

    pragma_overrides: NotRequired[Mapping[str, str | int | bool]]
    """Additional validated SQLite PRAGMA overrides applied after defaults."""

    fts_tokenize: NotRequired[str]
    """Optional FTS5 tokenizer specification used when ``memory_use_fts`` is enabled."""

    fts_detail: NotRequired[Literal["full", "column", "none"]]
    """Optional FTS5 indexing detail level (``'full'``, ``'column'``, or ``'none'``)."""

    memory_retention_days: NotRequired[int]
    """Optional retention window in days for ADK memory entries."""

    use_fts: NotRequired[bool]
    """Optional FTS5 enablement flag aliased to ``memory_use_fts``."""


class _SQLiteADKSessionSQLMixin:
    """Shared SQLite DDL and query builders for synchronous and asynchronous ADK session stores."""

    __slots__ = ()

    _session_table: str
    _events_table: str
    _app_state_table: str
    _user_state_table: str
    _metadata_table: str
    _owner_id_column_ddl: str | None
    _owner_id_column_name: str | None

    def _session_index_statements(self) -> list[str]:
        return [
            f"CREATE INDEX IF NOT EXISTS idx_{self._session_table}_app_user ON {self._session_table}(app_name, user_id)",
            f"CREATE INDEX IF NOT EXISTS idx_{self._session_table}_update_time ON {self._session_table}(update_time DESC)",
        ]

    def _event_index_statements(self) -> list[str]:
        return [
            f"CREATE INDEX IF NOT EXISTS idx_{self._events_table}_session ON {self._events_table}(app_name, user_id, session_id, timestamp ASC)",
            f"CREATE INDEX IF NOT EXISTS idx_{self._events_table}_invocation ON {self._events_table}(invocation_id)",
            f"CREATE INDEX IF NOT EXISTS idx_{self._events_table}_timestamp ON {self._events_table}(timestamp ASC)",
            f"CREATE INDEX IF NOT EXISTS idx_{self._events_table}_app_timestamp ON {self._events_table}(app_name, timestamp ASC)",
        ]

    def _companion_index_statements(self) -> list[str]:
        return [*self._session_index_statements(), *self._event_index_statements()]

    def _build_sessions_ddl(self) -> str:
        owner_line = f",\n    {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        index_lines = ";\n".join(self._session_index_statements())
        return (
            f"CREATE TABLE IF NOT EXISTS {self._session_table} (\n"
            f"    id TEXT PRIMARY KEY,\n"
            f"    app_name TEXT NOT NULL,\n"
            f"    user_id TEXT NOT NULL{owner_line},\n"
            f"    state TEXT NOT NULL DEFAULT '{{}}',\n"
            f"    create_time REAL NOT NULL,\n"
            f"    update_time REAL NOT NULL\n"
            f");\n"
            f"{index_lines};"
        )

    def _build_events_ddl(self) -> str:
        index_lines = ";\n".join(self._event_index_statements())
        return (
            f"CREATE TABLE IF NOT EXISTS {self._events_table} (\n"
            f"    id TEXT PRIMARY KEY,\n"
            f"    app_name TEXT NOT NULL,\n"
            f"    user_id TEXT NOT NULL,\n"
            f"    session_id TEXT NOT NULL,\n"
            f"    invocation_id TEXT,\n"
            f"    timestamp REAL NOT NULL,\n"
            f"    event_data TEXT NOT NULL,\n"
            f"    FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE\n"
            f");\n"
            f"{index_lines};"
        )

    def _build_app_states_ddl(self) -> str:
        return (
            f"CREATE TABLE IF NOT EXISTS {self._app_state_table} (\n"
            f"    app_name TEXT PRIMARY KEY,\n"
            f"    state TEXT NOT NULL DEFAULT '{{}}',\n"
            f"    update_time REAL NOT NULL\n"
            f");"
        )

    def _build_user_states_ddl(self) -> str:
        return (
            f"CREATE TABLE IF NOT EXISTS {self._user_state_table} (\n"
            f"    app_name TEXT NOT NULL,\n"
            f"    user_id TEXT NOT NULL,\n"
            f"    state TEXT NOT NULL DEFAULT '{{}}',\n"
            f"    update_time REAL NOT NULL,\n"
            f"    PRIMARY KEY (app_name, user_id)\n"
            f");"
        )

    def _build_metadata_ddl(self) -> str:
        return (
            f"CREATE TABLE IF NOT EXISTS {self._metadata_table} (\n"
            f"    key TEXT PRIMARY KEY,\n"
            f"    value TEXT NOT NULL\n"
            f");"
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
        now_julian: float,
    ) -> tuple[str, tuple[Any, ...]]:
        state_json = to_json(state)
        if self._owner_id_column_name:
            sql = (
                f"INSERT INTO {self._session_table} "
                f"(id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)"
            )
            return sql, (session_id, app_name, user_id, owner_id, state_json, now_julian, now_julian)
        sql = (
            f"INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time) "
            "VALUES (?, ?, ?, ?, ?, ?)"
        )
        return sql, (session_id, app_name, user_id, state_json, now_julian, now_julian)

    def _select_state_sql(self, table_name: str, key_columns: tuple[str, ...]) -> str:
        where_clause = " AND ".join(f"{col} = ?" for col in key_columns)
        return f"SELECT state FROM {table_name} WHERE {where_clause}"

    def _upsert_state_delta_sql(self, table_name: str, key_columns: tuple[str, ...]) -> str:
        cols = ", ".join((*key_columns, "state", "update_time"))
        placeholders = ", ".join("?" for _ in range(len(key_columns) + 2))
        conflict_cols = ", ".join(key_columns)
        return (
            f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders}) "
            f"ON CONFLICT({conflict_cols}) DO UPDATE SET state = excluded.state, update_time = excluded.update_time"
        )

    def _get_session_select_sql(self) -> str:
        return (
            f"SELECT id, app_name, user_id, state, create_time, update_time FROM {self._session_table} "
            "WHERE app_name = ? AND user_id = ? AND id = ?"
        )

    def _get_session_touch_sql(self) -> str:
        return f"UPDATE {self._session_table} SET update_time = ? WHERE app_name = ? AND user_id = ? AND id = ?"

    def _update_session_state_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        state: dict[str, Any],
    ) -> tuple[str, tuple[Any, ...]]:
        now_julian = datetime_to_julian(datetime.now(UTC))
        sql = (
            f"UPDATE {self._session_table} SET state = ?, update_time = ? WHERE app_name = ? AND user_id = ? AND id = ?"
        )
        return sql, (to_json(state), now_julian, app_name, user_id, session_id)

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
        where_clause = "app_name = ? AND user_id = ?" if user_id is not None else "app_name = ?"
        params: list[Any] = [app_name, user_id] if user_id is not None else [app_name]
        limit_clause = ""
        if page_limit is not None:
            limit_clause = " LIMIT ? OFFSET ?" if page_offset > 0 else " LIMIT ?"
            params.append(page_limit)
            if page_offset > 0:
                params.append(page_offset)
        sql = (
            f"SELECT id, app_name, user_id, state, create_time, update_time FROM {self._session_table} "
            f"WHERE {where_clause} ORDER BY {column} {direction}, id {direction}{limit_clause}"
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
            event_record.get("invocation_id"),
            datetime_to_julian(event_record["timestamp"]),
            to_json(event_record["event_data"]),
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
            event_record.get("invocation_id"),
            datetime_to_julian(event_record["timestamp"]),
            to_json(event_record["event_data"]),
        )
        return sql, params

    def _update_session_returning_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        state: dict[str, Any],
        now_julian: float,
    ) -> tuple[str, tuple[Any, ...]]:
        sql = (
            f"UPDATE {self._session_table} SET state = ?, update_time = ? "
            "WHERE app_name = ? AND user_id = ? AND id = ? "
            "RETURNING id, app_name, user_id, state, create_time, update_time"
        )
        return sql, (to_json(state), now_julian, app_name, user_id, session_id)

    def _get_events_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None,
        limit: int | None,
    ) -> tuple[str, tuple[Any, ...]]:
        where_parts = ["s.app_name = ?", "s.user_id = ?", "s.id = ?"]
        params: list[Any] = [app_name, user_id, session_id]
        if after_timestamp is not None:
            where_parts.append("e.timestamp > ?")
            params.append(datetime_to_julian(after_timestamp))
        if limit is not None:
            order_clause = "ORDER BY e.timestamp DESC, e.id DESC LIMIT ?"
            params.append(int(limit))
        else:
            order_clause = "ORDER BY e.timestamp ASC, e.id ASC"
        sql = (
            "SELECT e.id, e.app_name, e.user_id, e.session_id, e.invocation_id, e.timestamp, e.event_data "
            f"FROM {self._events_table} e "
            f"JOIN {self._session_table} s ON e.session_id = s.id AND e.app_name = s.app_name AND e.user_id = s.user_id "
            f"WHERE {' AND '.join(where_parts)} {order_clause}"
        )
        return sql, tuple(params)

    def _delete_before_sql_and_params(
        self,
        table_name: str,
        time_column: str,
        before: datetime,
        app_name: str | None,
    ) -> tuple[str, tuple[Any, ...]]:
        sql = f"DELETE FROM {table_name} WHERE {time_column} < ?"
        params: list[Any] = [datetime_to_julian(before)]
        if app_name is not None:
            sql += " AND app_name = ?"
            params.append(app_name)
        return sql, tuple(params)

    def _upsert_app_state_sql_and_params(self, app_name: str, state: dict[str, Any]) -> tuple[str, tuple[Any, ...]]:
        sql = (
            f"INSERT INTO {self._app_state_table} (app_name, state, update_time) VALUES (?, ?, ?) "
            "ON CONFLICT(app_name) DO UPDATE SET state = excluded.state, update_time = excluded.update_time"
        )
        return sql, (app_name, to_json(state), datetime_to_julian(datetime.now(UTC)))

    def _upsert_user_state_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        state: dict[str, Any],
    ) -> tuple[str, tuple[Any, ...]]:
        sql = (
            f"INSERT INTO {self._user_state_table} (app_name, user_id, state, update_time) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(app_name, user_id) DO UPDATE SET state = excluded.state, update_time = excluded.update_time"
        )
        return sql, (app_name, user_id, to_json(state), datetime_to_julian(datetime.now(UTC)))

    def _get_metadata_sql(self) -> str:
        return f"SELECT value FROM {self._metadata_table} WHERE key = ?"

    def _set_metadata_sql(self) -> str:
        return (
            f"INSERT INTO {self._metadata_table} (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        )

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

    def _is_table_missing(self, exc: Exception) -> bool:
        return _is_missing_table_error(exc)

    def _prepare_driver_sync(self, driver: Any) -> None:
        cast("Any", self)._ensure_pragmas(driver)

    async def _prepare_driver_async(self, driver: Any) -> None:
        await cast("Any", self)._ensure_pragmas(driver)

    def _decode_session_row(
        self,
        row: Any,
        *,
        app_state: dict[str, Any] | None = None,
        user_state: dict[str, Any] | None = None,
        merge_scopes: bool = False,
    ) -> StoredSession:
        rec = _row_to_stored_session(row)
        if merge_scopes:
            rec["state"] = merge_scoped_state(rec["state"], app_state, user_state)
        return rec

    def _decode_event_row(self, row: Any) -> StoredEvent:
        return _row_to_stored_event(row)

    def _delete_session_sqls_and_params(
        self, app_name: str, user_id: str, session_id: str
    ) -> tuple[tuple[str, tuple[str, str, str]]]:
        return ((self._delete_session_sql(), (app_name, user_id, session_id)),)

    def _get_metadata_sql_and_params(self, key: str) -> tuple[str, tuple[str]]:
        return self._get_metadata_sql(), (key,)

    def _set_metadata_sql_and_params(self, key: str, value: str) -> tuple[str, tuple[str, str]]:
        return self._set_metadata_sql(), (key, value)


class SQLiteSyncADKStore(_SQLiteADKSessionSQLMixin, AdapterSyncADKStore):
    """Synchronous SQLite ADK session and event store over Vertebra ``SQLiteSyncDriver``."""

    __slots__ = ("_configured_driver_ids", "_pragma_statements")

    adapter_name = "vertebra_sqlite"
    extension_config_options = _SQLITE_ADK_EXTENSION_OPTIONS

    def __init__(self, config: SQLiteSyncConfig, *, read_config: SQLiteSyncConfig | None = None) -> None:
        """Initialize the synchronous SQLite ADK session store."""
        super().__init__(config, read_config=read_config)
        self._pragma_statements = _adk_pragma_statements(config)
        self._configured_driver_ids: set[int] = set()

    def _ensure_pragmas(self, driver: Any) -> None:
        if id(driver) not in self._configured_driver_ids:
            apply_pragmas_sync(driver, self._pragma_statements)
            self._configured_driver_ids.add(id(driver))

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Create missing SQLite session and event companion indexes without committing."""
        for statement in self._companion_index_statements():
            driver.execute(statement)

    def create_tables(self) -> None:
        """Create ADK sessions, events, app state, user state, and metadata tables."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return
        with self._config.provide_session() as driver:
            self._ensure_pragmas(driver)
            driver.execute_script(self._sessions_table_ddl())
            driver.execute_script(self._events_table_ddl())
            driver.execute_script(self._app_states_table_ddl())
            driver.execute_script(self._user_states_table_ddl())
            driver.execute_script(self._metadata_table_ddl())
            driver.commit()

    def _select_state_sync(
        self,
        driver: Any,
        table_name: str,
        key_columns: tuple[str, ...],
        key_values: tuple[Any, ...],
    ) -> dict[str, Any] | None:
        raw = driver.select_value_or_none(self._select_state_sql(table_name, key_columns), *key_values)
        return _decode_state_json(raw) if raw else None

    def _apply_state_delta_sync(
        self,
        driver: Any,
        table_name: str,
        key_columns: tuple[str, ...],
        key_values: tuple[Any, ...],
        delta: Mapping[str, Any],
        now_julian: float,
    ) -> dict[str, Any]:
        existing = self._select_state_sync(driver, table_name, key_columns, key_values)
        merged = dict(existing) if existing else {}
        merged.update(delta)
        driver.execute(self._upsert_state_delta_sql(table_name, key_columns), *key_values, to_json(merged), now_julian)
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
        """Create a new ADK session row and atomically merge optional scoped state deltas."""
        now_julian = datetime_to_julian(datetime.now(UTC))
        if now_julian is None:
            now_julian = JULIAN_EPOCH
        stored_time = julian_to_datetime(now_julian) or datetime.now(UTC)
        sql, params = self._create_session_sql_and_params(session_id, app_name, user_id, state, owner_id, now_julian)
        with self._config.provide_session() as driver:
            self._ensure_pragmas(driver)
            driver.begin()
            try:
                driver.execute(sql, *params)
                resolved_app = (
                    self._apply_state_delta_sync(
                        driver, self._app_state_table, ("app_name",), (app_name,), app_state_delta, now_julian
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
                        now_julian,
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
        merged_state = merge_scoped_state(state, resolved_app, resolved_user)
        return StoredSession(
            id=session_id,
            app_name=app_name,
            user_id=user_id,
            state=merged_state,
            create_time=stored_time,
            update_time=stored_time,
        )

    def get_session(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        *,
        renew_for: int | timedelta | None = None,
    ) -> StoredSession | None:
        """Fetch an ADK session by composite key, optionally bumping ``update_time``."""
        select_sql = self._get_session_select_sql()
        should_renew = self._is_renewal_requested(renew_for)
        try:
            with self._provide_replica_session(primary=renew_for is not None) as driver:
                self._ensure_pragmas(driver)
                if should_renew:
                    now_julian = datetime_to_julian(datetime.now(UTC))
                    driver.execute(self._get_session_touch_sql(), now_julian, app_name, user_id, session_id)
                    driver.commit()
                row = driver.select_one_or_none(select_sql, app_name, user_id, session_id)
                return _row_to_stored_session(row) if row is not None else None
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return None
            raise

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
        self._validate_event_scope(event_record, app_name, user_id, session_id)
        now_julian = datetime_to_julian(datetime.now(UTC))
        if now_julian is None:
            now_julian = JULIAN_EPOCH
        update_sql, update_params = self._update_session_returning_sql_and_params(
            app_name, user_id, session_id, state, now_julian
        )
        insert_sql, insert_params = self._insert_event_sql_and_params(event_record)
        with self._config.provide_session() as driver:
            self._ensure_pragmas(driver)
            driver.begin()
            try:
                row = driver.select_one_or_none(update_sql, *update_params)
                if row is None:
                    msg = f"Session {session_id} not found during append_event_and_update_state."
                    raise ValueError(msg)
                driver.execute(insert_sql, *insert_params)
                merged_app = (
                    self._apply_state_delta_sync(
                        driver, self._app_state_table, ("app_name",), (app_name,), app_state_delta, now_julian
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
                        now_julian,
                    )
                    if user_state_delta
                    else None
                )
                driver.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    driver.rollback()
                raise
        session_record = _row_to_stored_session(row)
        session_record["state"] = merge_scoped_state(session_record["state"], merged_app, merged_user)
        return session_record

    def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Return app-scoped ADK state dictionary if present."""
        sql = self._select_state_sql(self._app_state_table, ("app_name",))
        try:
            with self._config.provide_session() as driver:
                self._ensure_pragmas(driver)
                raw = driver.select_value_or_none(sql, app_name)
                return cast("dict[str, Any]", from_json(raw)) if raw else None
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return None
            raise

    def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Return user-scoped ADK state dictionary if present."""
        sql = self._select_state_sql(self._user_state_table, ("app_name", "user_id"))
        try:
            with self._config.provide_session() as driver:
                self._ensure_pragmas(driver)
                raw = driver.select_value_or_none(sql, app_name, user_id)
                return cast("dict[str, Any]", from_json(raw)) if raw else None
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return None
            raise


class SQLiteAsyncADKStore(_SQLiteADKSessionSQLMixin, AdapterAsyncADKStore):
    """Asynchronous SQLite ADK session and event store over Vertebra ``SQLiteAsyncDriver``."""

    __slots__ = ("_configured_driver_ids", "_pragma_statements")

    adapter_name = "vertebra_sqlite"
    extension_config_options = _SQLITE_ADK_EXTENSION_OPTIONS

    def __init__(self, config: SQLiteAsyncConfig, *, read_config: SQLiteAsyncConfig | None = None) -> None:
        """Initialize the asynchronous SQLite ADK session store."""
        super().__init__(config, read_config=read_config)
        self._pragma_statements = _adk_pragma_statements(config)
        self._configured_driver_ids: set[int] = set()

    async def _ensure_pragmas(self, driver: Any) -> None:
        if id(driver) not in self._configured_driver_ids:
            await apply_pragmas_async(driver, self._pragma_statements)
            self._configured_driver_ids.add(id(driver))

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Create missing SQLite session and event companion indexes asynchronously."""
        for statement in self._companion_index_statements():
            await driver.execute(statement)

    async def create_tables(self) -> None:
        """Create ADK session and state tables asynchronously."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        async with self._config.provide_session() as driver:
            await self._ensure_pragmas(driver)
            await driver.execute_script(await self._sessions_table_ddl())
            await driver.execute_script(await self._events_table_ddl())
            await driver.execute_script(await self._app_states_table_ddl())
            await driver.execute_script(await self._user_states_table_ddl())
            await driver.execute_script(await self._metadata_table_ddl())
            await driver.commit()

    async def _select_state_async(
        self,
        driver: Any,
        table_name: str,
        key_columns: tuple[str, ...],
        key_values: tuple[Any, ...],
    ) -> dict[str, Any] | None:
        raw = await driver.select_value_or_none(self._select_state_sql(table_name, key_columns), *key_values)
        return _decode_state_json(raw) if raw else None

    async def _apply_state_delta_async(
        self,
        driver: Any,
        table_name: str,
        key_columns: tuple[str, ...],
        key_values: tuple[Any, ...],
        delta: Mapping[str, Any],
        now_julian: float,
    ) -> dict[str, Any]:
        existing = await self._select_state_async(driver, table_name, key_columns, key_values)
        merged = dict(existing) if existing else {}
        merged.update(delta)
        await driver.execute(
            self._upsert_state_delta_sql(table_name, key_columns), *key_values, to_json(merged), now_julian
        )
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
        """Create a new ADK session asynchronously and atomically merge optional scoped deltas."""
        now_julian = datetime_to_julian(datetime.now(UTC))
        if now_julian is None:
            now_julian = JULIAN_EPOCH
        stored_time = julian_to_datetime(now_julian) or datetime.now(UTC)
        sql, params = self._create_session_sql_and_params(session_id, app_name, user_id, state, owner_id, now_julian)
        async with self._config.provide_session() as driver:
            await self._ensure_pragmas(driver)
            await driver.begin()
            try:
                await driver.execute(sql, *params)
                resolved_app = (
                    await self._apply_state_delta_async(
                        driver, self._app_state_table, ("app_name",), (app_name,), app_state_delta, now_julian
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
                        now_julian,
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
        merged_state = merge_scoped_state(state, resolved_app, resolved_user)
        return StoredSession(
            id=session_id,
            app_name=app_name,
            user_id=user_id,
            state=merged_state,
            create_time=stored_time,
            update_time=stored_time,
        )

    async def get_session(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        *,
        renew_for: int | timedelta | None = None,
    ) -> StoredSession | None:
        """Fetch an ADK session asynchronously, optionally renewing ``update_time``."""
        select_sql = self._get_session_select_sql()
        should_renew = self._is_renewal_requested(renew_for)
        try:
            async with self._provide_replica_session(primary=renew_for is not None) as driver:
                await self._ensure_pragmas(driver)
                if should_renew:
                    now_julian = datetime_to_julian(datetime.now(UTC))
                    await driver.execute(self._get_session_touch_sql(), now_julian, app_name, user_id, session_id)
                    await driver.commit()
                row = await driver.select_one_or_none(select_sql, app_name, user_id, session_id)
                return _row_to_stored_session(row) if row is not None else None
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return None
            raise

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
        now_julian = datetime_to_julian(datetime.now(UTC))
        if now_julian is None:
            now_julian = JULIAN_EPOCH
        update_sql, update_params = self._update_session_returning_sql_and_params(
            app_name, user_id, session_id, state, now_julian
        )
        insert_sql, insert_params = self._insert_event_sql_and_params(event_record)
        async with self._config.provide_session() as driver:
            await self._ensure_pragmas(driver)
            await driver.begin()
            try:
                row = await driver.select_one_or_none(update_sql, *update_params)
                if row is None:
                    msg = f"Session {session_id} not found during append_event_and_update_state."
                    raise ValueError(msg)
                await driver.execute(insert_sql, *insert_params)
                merged_app = (
                    await self._apply_state_delta_async(
                        driver, self._app_state_table, ("app_name",), (app_name,), app_state_delta, now_julian
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
                        now_julian,
                    )
                    if user_state_delta
                    else None
                )
                await driver.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    await driver.rollback()
                raise
        session_record = _row_to_stored_session(row)
        session_record["state"] = merge_scoped_state(session_record["state"], merged_app, merged_user)
        return session_record

    async def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Return app-scoped ADK state asynchronously."""
        sql = self._select_state_sql(self._app_state_table, ("app_name",))
        try:
            async with self._config.provide_session() as driver:
                await self._ensure_pragmas(driver)
                raw = await driver.select_value_or_none(sql, app_name)
                return cast("dict[str, Any]", from_json(raw)) if raw else None
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return None
            raise

    async def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Return user-scoped ADK state asynchronously."""
        sql = self._select_state_sql(self._user_state_table, ("app_name", "user_id"))
        try:
            async with self._config.provide_session() as driver:
                await self._ensure_pragmas(driver)
                raw = await driver.select_value_or_none(sql, app_name, user_id)
                return cast("dict[str, Any]", from_json(raw)) if raw else None
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return None
            raise


class _SQLiteADKMemorySQLMixin:
    """Shared SQLite DDL and query builders for synchronous and asynchronous ADK memory stores."""

    __slots__ = ()

    _enabled: bool
    _memory_table: str
    _use_fts: bool
    _max_results: int
    _owner_id_column_ddl: str | None
    _owner_id_column_name: str | None
    _fts_options: dict[str, Any]

    def _memory_index_statements(self) -> list[str]:
        return [
            f"CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_app_scope_user_time ON {self._memory_table}(app_name, scope, user_id, timestamp DESC)",
            f"CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_scope ON {self._memory_table}(app_name, scope)",
            f"CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_session ON {self._memory_table}(session_id)",
        ]

    def _memory_fts_statements(self) -> dict[str, str]:
        if not self._use_fts:
            return {}
        opts = format_fts_options(self._fts_options)
        table = self._memory_table
        return {
            f"{table}_fts": (
                f"CREATE VIRTUAL TABLE IF NOT EXISTS {table}_fts USING fts5("
                f"content_text, content={table}, content_rowid=rowid{opts})"
            ),
            f"{table}_ai": (
                f"CREATE TRIGGER IF NOT EXISTS {table}_ai AFTER INSERT ON {table} BEGIN\n"
                f"    INSERT INTO {table}_fts(rowid, content_text) VALUES (new.rowid, new.content_text);\n"
                f"END"
            ),
            f"{table}_ad": (
                f"CREATE TRIGGER IF NOT EXISTS {table}_ad AFTER DELETE ON {table} BEGIN\n"
                f"    INSERT INTO {table}_fts({table}_fts, rowid, content_text) VALUES('delete', old.rowid, old.content_text);\n"
                f"END"
            ),
            f"{table}_au": (
                f"CREATE TRIGGER IF NOT EXISTS {table}_au AFTER UPDATE ON {table} BEGIN\n"
                f"    INSERT INTO {table}_fts({table}_fts, rowid, content_text) VALUES('delete', old.rowid, old.content_text);\n"
                f"    INSERT INTO {table}_fts(rowid, content_text) VALUES (new.rowid, new.content_text);\n"
                f"END"
            ),
        }

    def _memory_fts_rebuild_statement(self) -> str:
        return f"INSERT INTO {self._memory_table}_fts({self._memory_table}_fts) VALUES('rebuild')"

    def _build_memory_table_ddl(self) -> str:
        owner_line = f",\n    {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        index_lines = ";\n".join(self._memory_index_statements())
        fts_statements = self._memory_fts_statements()
        fts_clause = f"\n{';\n'.join(fts_statements.values())};" if fts_statements else ""
        return (
            f"CREATE TABLE IF NOT EXISTS {self._memory_table} (\n"
            f"    id TEXT PRIMARY KEY,\n"
            f"    session_id TEXT NOT NULL,\n"
            f"    app_name TEXT NOT NULL,\n"
            f"    user_id TEXT NOT NULL,\n"
            f"    scope TEXT NOT NULL DEFAULT 'user',\n"
            f"    event_id TEXT NOT NULL,\n"
            f"    author TEXT{owner_line},\n"
            f"    timestamp REAL NOT NULL,\n"
            f"    content_json TEXT NOT NULL,\n"
            f"    content_text TEXT NOT NULL,\n"
            f"    metadata_json TEXT,\n"
            f"    inserted_at REAL NOT NULL,\n"
            f"    embedding BLOB,\n"
            f"    UNIQUE (app_name, user_id, event_id)\n"
            f");\n"
            f"{index_lines};{fts_clause}"
        )

    def _drop_memory_table_sql(self) -> list[str]:
        if self._use_fts:
            return [
                f"DROP TRIGGER IF EXISTS {self._memory_table}_ai",
                f"DROP TRIGGER IF EXISTS {self._memory_table}_ad",
                f"DROP TRIGGER IF EXISTS {self._memory_table}_au",
                f"DROP TABLE IF EXISTS {self._memory_table}_fts",
                f"DROP TABLE IF EXISTS {self._memory_table}",
            ]
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

    def _build_insert_sql_and_params(
        self,
        entries: Sequence[StoredMemory],
        owner_id: object | None,
    ) -> tuple[str, list[tuple[Any, ...]]]:
        if self._owner_id_column_name:
            sql = (
                f"INSERT OR IGNORE INTO {self._memory_table} "
                f"(id, session_id, app_name, user_id, scope, event_id, author, {self._owner_id_column_name}, "
                "timestamp, content_json, content_text, metadata_json, inserted_at, embedding) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
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
                    datetime_to_julian(entry["timestamp"]),
                    to_json(entry["content_json"]),
                    entry["content_text"],
                    to_json(entry["metadata_json"]) if entry.get("metadata_json") is not None else None,
                    datetime_to_julian(entry["inserted_at"]),
                    pack_embedding(entry.get("embedding")),
                )
                for entry in entries
            ]
            return sql, params_list
        sql = (
            f"INSERT OR IGNORE INTO {self._memory_table} "
            "(id, session_id, app_name, user_id, scope, event_id, author, "
            "timestamp, content_json, content_text, metadata_json, inserted_at, embedding) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
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
                datetime_to_julian(entry["timestamp"]),
                to_json(entry["content_json"]),
                entry["content_text"],
                to_json(entry["metadata_json"]) if entry.get("metadata_json") is not None else None,
                datetime_to_julian(entry["inserted_at"]),
                pack_embedding(entry.get("embedding")),
            )
            for entry in entries
        ]
        return sql, params_list

    def _build_fts_search_sql_and_params(
        self,
        query: str,
        app_name: str,
        user_id: str,
        fetch_limit: int,
        scope_filter: Literal["all", "user", "app"],
    ) -> tuple[str, tuple[Any, ...]]:
        where_scope, scope_params = build_sqlite_scope_clause("m.", app_name, user_id, scope_filter)
        fts_sql = (
            f"SELECT m.* FROM {self._memory_table} m "
            f"JOIN {self._memory_table}_fts fts ON m.rowid = fts.rowid "
            f"WHERE {where_scope} AND fts.content_text MATCH ? ORDER BY m.timestamp DESC LIMIT ?"
        )
        return fts_sql, (*scope_params, query, fetch_limit)

    def _build_fallback_search_sql_and_params(
        self,
        query: str,
        has_query: bool,
        app_name: str,
        user_id: str,
        fetch_limit: int,
        scope_filter: Literal["all", "user", "app"],
    ) -> tuple[str, tuple[Any, ...]]:
        where_scope, scope_params = build_sqlite_scope_clause("", app_name, user_id, scope_filter)
        if has_query:
            sql = (
                f"SELECT * FROM {self._memory_table} WHERE {where_scope} AND content_text LIKE ? "
                "ORDER BY timestamp DESC LIMIT ?"
            )
            return sql, (*scope_params, f"%{query}%", fetch_limit)
        sql = (
            f"SELECT * FROM {self._memory_table} WHERE {where_scope} AND embedding IS NOT NULL "
            "ORDER BY timestamp DESC LIMIT ?"
        )
        return sql, (*scope_params, fetch_limit)

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
        cutoff_julian = (datetime_to_julian(datetime.now(UTC)) or JULIAN_EPOCH) - days
        sql = f"DELETE FROM {self._memory_table} WHERE inserted_at < ?"
        params: list[Any] = [cutoff_julian]
        if app_name is not None:
            sql += " AND app_name = ?"
            params.append(app_name)
        if scope is not None:
            sql += " AND scope = ?"
            params.append(scope)
        return sql, tuple(params)

    def _is_table_missing(self, exc: Exception) -> bool:
        return _is_missing_table_error(exc)

    def _prepare_driver_sync(self, driver: Any) -> None:
        cast("Any", self)._ensure_pragmas(driver)

    async def _prepare_driver_async(self, driver: Any) -> None:
        await cast("Any", self)._ensure_pragmas(driver)


class SQLiteSyncADKMemoryStore(_SQLiteADKMemorySQLMixin, AdapterSyncADKMemoryStore):
    """Synchronous SQLite ADK memory store with FTS5 and embedding cosine ranking."""

    __slots__ = ("_configured_driver_ids", "_fts_options", "_pragma_statements")

    adapter_name = "vertebra_sqlite"
    extension_config_options = _SQLITE_ADK_EXTENSION_OPTIONS

    def __init__(self, config: SQLiteSyncConfig, *, read_config: SQLiteSyncConfig | None = None) -> None:
        """Initialize the synchronous SQLite ADK memory store."""
        super().__init__(config, read_config=read_config)
        adk_cfg = _adk_settings(config)
        if "memory_use_fts" not in adk_cfg and "use_fts" in adk_cfg:
            self._use_fts = bool(adk_cfg["use_fts"])
        self._fts_options = validate_fts_options(config)
        self._pragma_statements = _adk_pragma_statements(config)
        self._configured_driver_ids: set[int] = set()

    def _ensure_pragmas(self, driver: Any) -> None:
        if id(driver) not in self._configured_driver_ids:
            apply_pragmas_sync(driver, self._pragma_statements)
            self._configured_driver_ids.add(id(driver))

    def _memory_table_ddl(self) -> str:
        return self._build_memory_table_ddl()

    def _missing_fts_objects(self, driver: Any) -> list[str]:
        expected = tuple(self._memory_fts_statements())
        if not expected:
            return []
        placeholders = ", ".join("?" for _ in expected)
        rows = driver.select(
            f"SELECT name FROM sqlite_master WHERE type IN ('table', 'trigger') AND lower(name) IN ({placeholders})",
            *(name.lower() for name in expected),
        )
        present = {str(row["name"]).lower() for row in rows}
        return [name for name in expected if name.lower() not in present]

    def _has_memory_rows(self, driver: Any) -> bool:
        return bool(driver.select(f"SELECT 1 AS present FROM {self._memory_table} LIMIT 1"))

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Create missing SQLite memory indexes and FTS5 companions without committing."""
        if not self._enabled:
            return
        for statement in self._memory_index_statements():
            driver.execute(statement)
        if not self._use_fts:
            return
        fts_statements = self._memory_fts_statements()
        missing = self._missing_fts_objects(driver)
        for name in missing:
            driver.execute(fts_statements[name])
        if missing and self._has_memory_rows(driver):
            driver.execute(self._memory_fts_rebuild_statement())

    def create_tables(self) -> None:
        """Create the SQLite ADK memory table, indexes, and optional FTS5 virtual table."""
        if not self._enabled:
            return
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return
        with self._config.provide_session() as driver:
            self._ensure_pragmas(driver)
            missing = self._missing_fts_objects(driver) if self._use_fts else []
            driver.execute_script(self._memory_table_ddl())
            if missing and self._has_memory_rows(driver):
                driver.execute(self._memory_fts_rebuild_statement())
            driver.commit()

    def insert_memory_entries(self, entries: list[StoredMemory], owner_id: object | None = None) -> int:
        """Bulk insert memory entries ignoring duplicate ``(app_name, user_id, event_id)`` rows."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0
        sql, params_list = self._build_insert_sql_and_params(entries, owner_id)
        with self._config.provide_session() as driver:
            self._ensure_pragmas(driver)
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
        """Search memory entries via FTS5 ``MATCH``, SQL ``LIKE``, or cosine vector reranking."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        has_query = bool(query and query.strip())
        if not has_query and embedding is None:
            return []
        effective_limit = limit if limit is not None else self._max_results
        fetch_limit = effective_limit if embedding is None else max(effective_limit * 5, 50)
        rows: list[dict[str, Any]] | None = None
        if has_query and self._use_fts:
            fts_sql, fts_params = self._build_fts_search_sql_and_params(
                query, app_name, user_id, fetch_limit, scope_filter
            )
            try:
                with self._provide_replica_session(primary=False) as driver:
                    self._ensure_pragmas(driver)
                    rows = driver.select(fts_sql, *fts_params)
            except Exception as exc:
                if _is_missing_table_error(exc):
                    if f"{self._memory_table}_fts" in str(exc).lower():
                        logger.warning("FTS search failed; falling back to simple search: %s", exc)
                    else:
                        return []
                elif "no such module: fts5" in str(exc).lower():
                    logger.warning("FTS5 unavailable; falling back to simple search: %s", exc)
                else:
                    raise
        if rows is None:
            fallback_sql, fallback_params = self._build_fallback_search_sql_and_params(
                query, has_query, app_name, user_id, fetch_limit, scope_filter
            )
            try:
                with self._provide_replica_session(primary=False) as driver:
                    self._ensure_pragmas(driver)
                    rows = driver.select(fallback_sql, *fallback_params)
            except SQLParsingError as exc:
                if _is_missing_table_error(exc):
                    return []
                raise
        records = [_row_to_stored_memory(row) for row in (rows or ())]
        return _rank_by_embedding(records, embedding, effective_limit)


class SQLiteAsyncADKMemoryStore(_SQLiteADKMemorySQLMixin, AdapterAsyncADKMemoryStore):
    """Asynchronous SQLite ADK memory store with FTS5 and embedding cosine ranking."""

    __slots__ = ("_configured_driver_ids", "_fts_options", "_pragma_statements")

    adapter_name = "vertebra_sqlite"
    extension_config_options = _SQLITE_ADK_EXTENSION_OPTIONS

    def __init__(self, config: SQLiteAsyncConfig, *, read_config: SQLiteAsyncConfig | None = None) -> None:
        """Initialize the asynchronous SQLite ADK memory store."""
        super().__init__(config, read_config=read_config)
        adk_cfg = _adk_settings(config)
        if "memory_use_fts" not in adk_cfg and "use_fts" in adk_cfg:
            self._use_fts = bool(adk_cfg["use_fts"])
        self._fts_options = validate_fts_options(config)
        self._pragma_statements = _adk_pragma_statements(config)
        self._configured_driver_ids: set[int] = set()

    async def _ensure_pragmas(self, driver: Any) -> None:
        if id(driver) not in self._configured_driver_ids:
            await apply_pragmas_async(driver, self._pragma_statements)
            self._configured_driver_ids.add(id(driver))

    async def _memory_table_ddl(self) -> str:
        return self._build_memory_table_ddl()

    async def _missing_fts_objects(self, driver: Any) -> list[str]:
        expected = tuple(self._memory_fts_statements())
        if not expected:
            return []
        placeholders = ", ".join("?" for _ in expected)
        rows = await driver.select(
            f"SELECT name FROM sqlite_master WHERE type IN ('table', 'trigger') AND lower(name) IN ({placeholders})",
            *(name.lower() for name in expected),
        )
        present = {str(row["name"]).lower() for row in rows}
        return [name for name in expected if name.lower() not in present]

    async def _has_memory_rows(self, driver: Any) -> bool:
        return bool(await driver.select(f"SELECT 1 AS present FROM {self._memory_table} LIMIT 1"))

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Create missing SQLite memory indexes and FTS5 companions asynchronously."""
        if not self._enabled:
            return
        for statement in self._memory_index_statements():
            await driver.execute(statement)
        if not self._use_fts:
            return
        fts_statements = self._memory_fts_statements()
        missing = await self._missing_fts_objects(driver)
        for name in missing:
            await driver.execute(fts_statements[name])
        if missing and await self._has_memory_rows(driver):
            await driver.execute(self._memory_fts_rebuild_statement())

    async def create_tables(self) -> None:
        """Create the SQLite ADK memory table asynchronously."""
        if not self._enabled:
            return
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        ddl = await self._memory_table_ddl()
        async with self._config.provide_session() as driver:
            await self._ensure_pragmas(driver)
            missing = await self._missing_fts_objects(driver) if self._use_fts else []
            await driver.execute_script(ddl)
            if missing and await self._has_memory_rows(driver):
                await driver.execute(self._memory_fts_rebuild_statement())
            await driver.commit()

    async def insert_memory_entries(self, entries: list[StoredMemory], owner_id: object | None = None) -> int:
        """Bulk insert memory entries asynchronously ignoring duplicate ``(app_name, user_id, event_id)`` rows."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0
        sql, params_list = self._build_insert_sql_and_params(entries, owner_id)
        async with self._config.provide_session() as driver:
            await self._ensure_pragmas(driver)
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
        """Search memory entries asynchronously via FTS5 ``MATCH``, SQL ``LIKE``, or cosine reranking."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        has_query = bool(query and query.strip())
        if not has_query and embedding is None:
            return []
        effective_limit = limit if limit is not None else self._max_results
        fetch_limit = effective_limit if embedding is None else max(effective_limit * 5, 50)
        rows: list[dict[str, Any]] | None = None
        if has_query and self._use_fts:
            fts_sql, fts_params = self._build_fts_search_sql_and_params(
                query, app_name, user_id, fetch_limit, scope_filter
            )
            try:
                async with self._provide_replica_session(primary=False) as driver:
                    await self._ensure_pragmas(driver)
                    rows = await driver.select(fts_sql, *fts_params)
            except Exception as exc:
                if _is_missing_table_error(exc):
                    if f"{self._memory_table}_fts" in str(exc).lower():
                        logger.warning("FTS search failed; falling back to simple search: %s", exc)
                    else:
                        return []
                elif "no such module: fts5" in str(exc).lower():
                    logger.warning("FTS5 unavailable; falling back to simple search: %s", exc)
                else:
                    raise
        if rows is None:
            fallback_sql, fallback_params = self._build_fallback_search_sql_and_params(
                query, has_query, app_name, user_id, fetch_limit, scope_filter
            )
            try:
                async with self._provide_replica_session(primary=False) as driver:
                    await self._ensure_pragmas(driver)
                    rows = await driver.select(fallback_sql, *fallback_params)
            except SQLParsingError as exc:
                if _is_missing_table_error(exc):
                    return []
                raise
        records = [_row_to_stored_memory(row) for row in (rows or ())]
        return _rank_by_embedding(records, embedding, effective_limit)


class _SQLiteADKArtifactSQLMixin:
    _transactional_artifact_delete: ClassVar[bool] = True
    """Shared SQLite DDL and query builders for synchronous and asynchronous ADK artifact stores."""

    __slots__ = ()

    _artifact_table: str

    def _artifact_table_ddl(self) -> str:
        return (
            f"CREATE TABLE IF NOT EXISTS {self._artifact_table} (\n"
            "    app_name TEXT NOT NULL,\n"
            "    user_id TEXT NOT NULL,\n"
            "    session_id TEXT NOT NULL DEFAULT '',\n"
            "    filename TEXT NOT NULL,\n"
            "    version INTEGER NOT NULL,\n"
            "    mime_type TEXT,\n"
            "    canonical_uri TEXT NOT NULL,\n"
            "    custom_metadata TEXT,\n"
            "    size INTEGER,\n"
            "    updated REAL,\n"
            "    signed_url TEXT,\n"
            "    created_at REAL NOT NULL,\n"
            "    PRIMARY KEY (app_name, user_id, session_id, filename, version)\n"
            ");"
        )

    def _insert_artifact_sql_and_params(self, record: StoredArtifact) -> tuple[str, tuple[Any, ...]]:
        session_val = record["session_id"] if record["session_id"] is not None else ""
        meta = record.get("custom_metadata")
        sql = (
            f"INSERT INTO {self._artifact_table} "
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
            datetime_to_julian(record["created_at"]),
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
            datetime_to_julian(reference.updated) if reference.updated is not None else None,
            reference.signed_url,
            datetime_to_julian(now_dt),
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
            "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? ORDER BY version DESC LIMIT 1"
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
            f"FROM {self._artifact_table} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? ORDER BY version ASC"
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
        cutoff = datetime_to_julian(before)
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

    def _is_table_missing(self, exc: Exception) -> bool:
        return _is_missing_table_error(exc)

    def _prepare_driver_sync(self, driver: Any) -> None:
        cast("Any", self)._ensure_pragmas(driver)

    async def _prepare_driver_async(self, driver: Any) -> None:
        await cast("Any", self)._ensure_pragmas(driver)

    def _build_artifact_table_ddl(self) -> str:
        return self._artifact_table_ddl()

    def _decode_artifact_row(self, row: Any) -> StoredArtifact:
        return _row_to_stored_artifact(row)

    def _decode_artifact_ref_row(self, row: Any) -> ArtifactReference:
        return _row_to_artifact_reference(row)


class SQLiteSyncADKArtifactStore(_SQLiteADKArtifactSQLMixin, AdapterSyncADKArtifactStore):
    """Synchronous SQLite ADK artifact metadata store supporting ``ArtifactReference`` records."""

    _transactional_artifact_delete: ClassVar[bool] = True

    __slots__ = ("_configured_driver_ids", "_pragma_statements")

    adapter_name = "vertebra_sqlite"

    def __init__(self, config: SQLiteSyncConfig, *, read_config: SQLiteSyncConfig | None = None) -> None:
        """Initialize the synchronous SQLite ADK artifact store."""
        super().__init__(config, read_config=read_config)
        self._pragma_statements = _adk_pragma_statements(config)
        self._configured_driver_ids: set[int] = set()

    def _ensure_pragmas(self, driver: Any) -> None:
        if id(driver) not in self._configured_driver_ids:
            apply_pragmas_sync(driver, self._pragma_statements)
            self._configured_driver_ids.add(id(driver))


class SQLiteAsyncADKArtifactStore(_SQLiteADKArtifactSQLMixin, AdapterAsyncADKArtifactStore):
    """Asynchronous SQLite ADK artifact metadata store supporting ``ArtifactReference`` records."""

    _transactional_artifact_delete: ClassVar[bool] = True

    __slots__ = ("_configured_driver_ids", "_pragma_statements")

    adapter_name = "vertebra_sqlite"

    def __init__(self, config: SQLiteAsyncConfig, *, read_config: SQLiteAsyncConfig | None = None) -> None:
        """Initialize the asynchronous SQLite ADK artifact store."""
        super().__init__(config, read_config=read_config)
        self._pragma_statements = _adk_pragma_statements(config)
        self._configured_driver_ids: set[int] = set()

    async def _ensure_pragmas(self, driver: Any) -> None:
        if id(driver) not in self._configured_driver_ids:
            await apply_pragmas_async(driver, self._pragma_statements)
            self._configured_driver_ids.add(id(driver))
