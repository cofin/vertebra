"""DuckDB ADK session, event, memory (`fts` + `array_cosine_similarity`), and artifact stores."""

import contextlib
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, cast

from sqlspec.extensions.adk import BaseAsyncADKStore, BaseSyncADKStore, normalize_session_list_options
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json

from vertebra.extensions.sqlspec.adapters.duckdb.core import (
    build_duckdb_scope_where,
    cosine_similarity,
    duckdb_json_dict,
    duckdb_row_value,
    format_duckdb_identifier,
    is_duckdb_table_missing,
    split_schema_and_table,
)
from vertebra.storage import ArtifactReference

if TYPE_CHECKING:
    from sqlspec.extensions.adk import StoredEvent, StoredSession
    from sqlspec.extensions.adk.memory._types import StoredMemory
    from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore as _BaseAsyncADKMemoryStore
    from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore as _BaseSyncADKMemoryStore
else:
    try:
        from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore as _BaseAsyncADKMemoryStore
        from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore as _BaseSyncADKMemoryStore
    except ImportError:

        class _FallbackADKMemoryStoreCommon:
            """SDK-free ADK memory store base when optional `google-adk` is absent."""

            __slots__ = (
                "_config",
                "_enabled",
                "_max_results",
                "_memory_table",
                "_owner_id_column_ddl",
                "_owner_id_column_name",
                "_use_fts",
            )
            adapter_name: ClassVar[str | None] = None

            @classmethod
            def __class_getitem__(cls, _item: Any) -> type[Any]:
                return cls

            def __init__(self, config: Any) -> None:
                self._config = config
                adk = _adk_config(config)
                self._enabled: bool = bool(adk.get("enable_memory", adk.get("memory_enabled", True)))
                self._memory_table: str = str(adk.get("memory_table", "adk_memory_entries"))
                self._use_fts: bool = bool(adk.get("use_fts", adk.get("memory_use_fts", False)))
                self._max_results: int = int(adk.get("max_results", 20))
                self._owner_id_column_ddl: str | None = adk.get("owner_id_column")
                self._owner_id_column_name: str | None = (
                    str(self._owner_id_column_ddl).split()[0] if self._owner_id_column_ddl else None
                )

            @property
            def config(self) -> Any:
                return self._config

            @property
            def is_enabled(self) -> bool:
                return self._enabled

            @property
            def memory_table(self) -> str:
                return self._memory_table

            @property
            def use_fts(self) -> bool:
                return self._use_fts

            @property
            def create_schema_enabled(self) -> bool:
                adk = _adk_config(self._config)
                return bool(adk.get("manage_schema", True)) and bool(adk.get("create_schema", True))

        class _BaseSyncADKMemoryStore(_FallbackADKMemoryStoreCommon):
            """Fallback synchronous memory store base when `google-adk` is absent."""

            def reconcile_schema(self, *, assume_existing: bool = False) -> Any:
                _ = assume_existing
                return None

            def drop_tables(self) -> None:
                statements = self._drop_memory_table_sql()
                with self._config.provide_session() as driver:
                    for statement in statements:
                        driver.execute_script(statement)
                    driver.commit()

            def ensure_tables(self) -> None:
                if self._enabled and self.create_schema_enabled:
                    self.create_tables()

            def _drop_memory_table_sql(self) -> list[str]:
                raise NotImplementedError

            def create_tables(self) -> None:
                raise NotImplementedError

        class _BaseAsyncADKMemoryStore(_FallbackADKMemoryStoreCommon):
            """Fallback asynchronous memory store base when `google-adk` is absent."""

            async def reconcile_schema(self, *, assume_existing: bool = False) -> Any:
                _ = assume_existing
                return None

            async def drop_tables(self) -> None:
                statements = self._drop_memory_table_sql()
                async with self._config.provide_session() as driver:
                    for statement in statements:
                        await driver.execute_script(statement)
                    await driver.commit()

            async def ensure_tables(self) -> None:
                if self._enabled and self.create_schema_enabled:
                    await self.create_tables()

            def _drop_memory_table_sql(self) -> list[str]:
                raise NotImplementedError

            async def create_tables(self) -> None:
                raise NotImplementedError


__all__ = (
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
_DUCKDB_EVENT_PROJECTION_STRUCT: Final[str] = "STRUCT(author VARCHAR, node_info STRUCT(path VARCHAR))"


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
    raise ValueError(msg)


def _render_fts_options(config: Any, *, overwrite: bool) -> str:
    options: dict[str, object] = dict(_DUCKDB_FTS_DEFAULT_OPTIONS)
    configured = _adk_config(config).get("memory_fts_options", {})
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


def _row_to_session(row: Any, *, deserializer: Callable[[Any], Any]) -> "StoredSession":
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


def _row_to_event(row: Any, *, deserializer: Callable[[Any], Any]) -> "StoredEvent":
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


def _row_to_memory(row: Any, *, deserializer: Callable[[Any], Any]) -> "StoredMemory":
    author = duckdb_row_value(row, "author", 6)
    meta_raw = duckdb_row_value(row, "metadata_json", 9)
    emb_raw = duckdb_row_value(row, "embedding", 10)
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
            "inserted_at": _ensure_utc(duckdb_row_value(row, "inserted_at", 11)),
            "embedding": _decode_embedding(emb_raw),
        },
    )


def _rank_memory_records(
    records: list["StoredMemory"],
    embedding: Sequence[float] | None,
    limit: int,
) -> list["StoredMemory"]:
    if embedding is None:
        return records[:limit]
    scored = sorted(
        records,
        key=lambda rec: cosine_similarity(rec.get("embedding") or (), embedding),
        reverse=True,
    )
    return scored[:limit]


def _raise_session_not_found(session_id: str) -> None:
    msg = f"Session not found: {session_id}"
    raise ValueError(msg)


def _session_list_sql(
    session_table: str,
    app_name: str,
    user_id: str | None,
    column: str,
    direction: str,
    page_limit: int | None,
    page_offset: int | None,
) -> tuple[str, tuple[Any, ...]]:
    table_ref = format_duckdb_identifier(session_table)
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
        sql += " LIMIT ?"
        params.append(int(page_limit))
    if page_offset is not None:
        sql += " OFFSET ?"
        params.append(int(page_offset))
    return sql, tuple(params)


def _events_query_sql(
    events_table: str,
    session_table: str,
    app_name: str,
    user_id: str,
    session_id: str,
    after_timestamp: datetime | None,
    limit: int | None,
) -> tuple[str, tuple[Any, ...]]:
    clauses = ["s.app_name = ?", "s.user_id = ?", "e.session_id = ?"]
    params: list[Any] = [app_name, user_id, session_id]
    if after_timestamp is not None:
        clauses.append("e.timestamp > ?")
        params.append(after_timestamp.astimezone(UTC).isoformat())
    limit_sql = f" LIMIT {int(limit)}" if limit is not None else ""
    sql = (
        "SELECT e.id, e.session_id, e.invocation_id, e.timestamp, e.event_data, s.app_name, s.user_id "
        f"FROM {format_duckdb_identifier(events_table)} e "
        f"JOIN {format_duckdb_identifier(session_table)} s ON e.session_id = s.id "
        f"WHERE {' AND '.join(clauses)} ORDER BY e.timestamp ASC{limit_sql}"
    )
    return sql, tuple(params)


class _DuckDBADKSessionDDL:
    _session_table: str
    _events_table: str
    _app_state_table: str
    _user_state_table: str
    _metadata_table: str
    _owner_id_column_ddl: str | None
    _config: Any

    def _build_sessions_ddl(self) -> str:
        session_ref = format_duckdb_identifier(self._session_table)
        owner_line = f",\n    {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return (
            f"CREATE TABLE IF NOT EXISTS {session_ref} (\n"
            "    id VARCHAR PRIMARY KEY,\n"
            "    app_name VARCHAR NOT NULL,\n"
            f"    user_id VARCHAR NOT NULL{owner_line},\n"
            "    state VARCHAR NOT NULL,\n"
            "    create_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            "    update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP\n"
            ");\n"
            f"CREATE INDEX IF NOT EXISTS {_duckdb_index_name(self._session_table, 'app_user')} "
            f"ON {session_ref}(app_name, user_id);\n"
            f"CREATE INDEX IF NOT EXISTS {_duckdb_index_name(self._session_table, 'update_time')} "
            f"ON {session_ref}(update_time DESC);"
        )

    def _build_events_ddl(self) -> str:
        events_ref = format_duckdb_identifier(self._events_table)
        adk_cfg = _adk_config(self._config)
        gen_cols = ""
        gen_idx = ""
        if adk_cfg.get("enable_event_generated_columns", False):
            gen_cols = (
                f",\n    author_gc VARCHAR GENERATED ALWAYS AS ((event_data::{_DUCKDB_EVENT_PROJECTION_STRUCT}).author) VIRTUAL,\n"
                f"    node_path_gc VARCHAR GENERATED ALWAYS AS ((event_data::{_DUCKDB_EVENT_PROJECTION_STRUCT}).node_info.path) VIRTUAL"
            )
            if adk_cfg.get("enable_event_generated_column_indexes", False):
                gen_idx = (
                    f"\nCREATE INDEX IF NOT EXISTS {_duckdb_index_name(self._events_table, 'author_gc')} "
                    f"ON {events_ref}(session_id, author_gc, timestamp ASC);\n"
                    f"CREATE INDEX IF NOT EXISTS {_duckdb_index_name(self._events_table, 'node_path_gc')} "
                    f"ON {events_ref}(session_id, node_path_gc, timestamp ASC);"
                )
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
            f"CREATE INDEX IF NOT EXISTS {_duckdb_index_name(self._events_table, 'session')} "
            f"ON {events_ref}(session_id, timestamp ASC);\n"
            f"CREATE INDEX IF NOT EXISTS {_duckdb_index_name(self._events_table, 'app_timestamp')} "
            f"ON {events_ref}(app_name, timestamp ASC);{gen_idx}"
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


class DuckDBSyncADKStore(_DuckDBADKSessionDDL, BaseSyncADKStore[Any]):
    """Synchronous DuckDB ADK session and event store over Vertebra `DuckDBSyncDriver`."""

    __slots__ = ()

    adapter_name = "vertebra_duckdb"
    _max_table_name_parts = 2

    def __init__(self, config: Any) -> None:
        """Initialize the synchronous DuckDB ADK session store."""
        super().__init__(config)

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

    def create_session(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
        state: dict[str, Any],
        owner_id: Any | None = None,
    ) -> "StoredSession":
        """Create a new ADK session synchronously."""
        now = datetime.now(UTC)
        now_iso = now.isoformat()
        state_json = to_json(state)
        session_ref = format_duckdb_identifier(self._session_table)
        if self._owner_id_column_name:
            sql = (
                f"INSERT INTO {session_ref} "
                f"(id, app_name, user_id, {format_duckdb_identifier(self._owner_id_column_name)}, state, create_time, update_time) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)"
            )
            params: tuple[Any, ...] = (session_id, app_name, user_id, owner_id, state_json, now_iso, now_iso)
        else:
            sql = (
                f"INSERT INTO {session_ref} (id, app_name, user_id, state, create_time, update_time) "
                "VALUES (?, ?, ?, ?, ?, ?)"
            )
            params = (session_id, app_name, user_id, state_json, now_iso, now_iso)
        with self._config.provide_session() as driver:
            driver.execute(sql, *params)
            driver.commit()
        return cast(
            "StoredSession",
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
    ) -> "StoredSession | None":
        """Fetch an ADK session synchronously, optionally renewing `update_time`."""
        session_ref = format_duckdb_identifier(self._session_table)
        try:
            with self._config.provide_session() as driver:
                if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
                    now_iso = datetime.now(UTC).isoformat()
                    sql = (
                        f"UPDATE {session_ref} SET update_time = ? "
                        "WHERE app_name = ? AND user_id = ? AND id = ? "
                        "RETURNING id, app_name, user_id, state, create_time, update_time"
                    )
                    row = driver.select_one_or_none(sql, now_iso, app_name, user_id, session_id)
                    driver.commit()
                else:
                    sql = (
                        f"SELECT id, app_name, user_id, state, create_time, update_time FROM {session_ref} "
                        "WHERE app_name = ? AND user_id = ? AND id = ?"
                    )
                    row = driver.select_one_or_none(sql, app_name, user_id, session_id)
                return _row_to_session(row, deserializer=from_json) if row is not None else None
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return None
            raise

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Update stored session JSON state synchronously."""
        session_ref = format_duckdb_identifier(self._session_table)
        now_iso = datetime.now(UTC).isoformat()
        with self._config.provide_session() as driver:
            driver.execute(
                f"UPDATE {session_ref} SET state = ?, update_time = ? WHERE app_name = ? AND user_id = ? AND id = ?",
                to_json(state),
                now_iso,
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
    ) -> list["StoredSession"]:
        """List stored ADK sessions synchronously."""
        column, direction, page_limit, page_offset = normalize_session_list_options(order_by, descending, limit, offset)
        if page_limit == 0:
            return []
        sql, params = _session_list_sql(
            self._session_table,
            app_name,
            user_id,
            column,
            direction,
            page_limit,
            page_offset,
        )
        try:
            with self._config.provide_session() as driver:
                rows = driver.select(sql, *params)
            return [_row_to_session(row, deserializer=from_json) for row in rows]
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return []
            raise

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete a session and its associated events synchronously."""
        events_ref = format_duckdb_identifier(self._events_table)
        session_ref = format_duckdb_identifier(self._session_table)
        with self._config.provide_session() as driver:
            driver.execute(f"DELETE FROM {events_ref} WHERE session_id = ?", session_id)
            driver.execute(
                f"DELETE FROM {session_ref} WHERE app_name = ? AND user_id = ? AND id = ?",
                app_name,
                user_id,
                session_id,
            )
            driver.commit()

    def append_event(self, event_record: "StoredEvent") -> None:
        """Append a single ADK event synchronously."""
        events_ref = format_duckdb_identifier(self._events_table)
        sql = (
            f"INSERT INTO {events_ref} "
            "(id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        with self._config.provide_session() as driver:
            driver.execute(
                sql,
                event_record["id"],
                event_record["app_name"],
                event_record["user_id"],
                event_record["session_id"],
                event_record["invocation_id"],
                _ensure_utc(event_record["timestamp"]).isoformat(),
                to_json(event_record["event_data"]),
            )
            driver.commit()

    def append_event_and_update_state(
        self,
        event_record: "StoredEvent",
        app_name: str,
        user_id: str,
        session_id: str,
        state: dict[str, Any],
        *,
        app_state: dict[str, Any] | None = None,
        user_state: dict[str, Any] | None = None,
    ) -> "StoredSession":
        """Atomically append an event and update session/app/user durable state."""
        now_iso = datetime.now(UTC).isoformat()
        events_ref = format_duckdb_identifier(self._events_table)
        session_ref = format_duckdb_identifier(self._session_table)
        app_ref = format_duckdb_identifier(self._app_state_table)
        user_ref = format_duckdb_identifier(self._user_state_table)
        insert_sql = (
            f"INSERT INTO {events_ref} "
            "(id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        update_sql = (
            f"UPDATE {session_ref} SET state = ?, update_time = ? "
            "WHERE app_name = ? AND user_id = ? AND id = ? "
            "RETURNING id, app_name, user_id, state, create_time, update_time"
        )
        app_sql = (
            f"INSERT INTO {app_ref} (app_name, state, update_time) VALUES (?, ?, ?) "
            "ON CONFLICT(app_name) DO UPDATE SET state = excluded.state, update_time = excluded.update_time"
        )
        user_sql = (
            f"INSERT INTO {user_ref} (app_name, user_id, state, update_time) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(app_name, user_id) DO UPDATE SET state = excluded.state, update_time = excluded.update_time"
        )
        with self._config.provide_session() as driver:
            driver.begin()
            try:
                driver.execute(
                    insert_sql,
                    event_record["id"],
                    event_record["app_name"],
                    event_record["user_id"],
                    event_record["session_id"],
                    event_record["invocation_id"],
                    _ensure_utc(event_record["timestamp"]).isoformat(),
                    to_json(event_record["event_data"]),
                )
                row = driver.select_one_or_none(update_sql, to_json(state), now_iso, app_name, user_id, session_id)
                if row is None:
                    _raise_session_not_found(session_id)
                if app_state is not None:
                    driver.execute(app_sql, app_name, to_json(app_state), now_iso)
                if user_state is not None:
                    driver.execute(user_sql, app_name, user_id, to_json(user_state), now_iso)
                driver.commit()
            except Exception:
                driver.rollback()
                raise
        return _row_to_session(row, deserializer=from_json)

    def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> list["StoredEvent"]:
        """Fetch ADK events for a session ordered ascending by timestamp."""
        if limit == 0:
            return []
        sql, params = _events_query_sql(
            self._events_table,
            self._session_table,
            app_name,
            user_id,
            session_id,
            after_timestamp,
            limit,
        )
        try:
            with self._config.provide_session() as driver:
                rows = driver.select(sql, *params)
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
        ref = format_duckdb_identifier(table_name)
        where = f"WHERE {column} < ?"
        params: list[Any] = [cutoff.astimezone(UTC).isoformat()]
        if app_name is not None:
            where += " AND app_name = ?"
            params.append(app_name)
        try:
            with self._config.provide_session() as driver:
                rows = driver.select(f"DELETE FROM {ref} {where} RETURNING 1 AS deleted", *params)
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
        events_ref = format_duckdb_identifier(self._events_table)
        session_ref = format_duckdb_identifier(self._session_table)
        where = "WHERE update_time < ?"
        params: list[Any] = [updated_before.astimezone(UTC).isoformat()]
        if app_name is not None:
            where += " AND app_name = ?"
            params.append(app_name)
        try:
            with self._config.provide_session() as driver:
                driver.execute(
                    f"DELETE FROM {events_ref} WHERE session_id IN (SELECT id FROM {session_ref} {where})",
                    *params,
                )
                rows = driver.select(f"DELETE FROM {session_ref} {where} RETURNING 1 AS deleted", *params)
                driver.commit()
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
        app_ref = format_duckdb_identifier(self._app_state_table)
        try:
            with self._config.provide_session() as driver:
                raw = driver.select_value_or_none(f"SELECT state FROM {app_ref} WHERE app_name = ?", app_name)
            return duckdb_json_dict(raw) if raw is not None else None
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return None
            raise

    def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Return user-scoped ADK state dictionary synchronously."""
        user_ref = format_duckdb_identifier(self._user_state_table)
        try:
            with self._config.provide_session() as driver:
                raw = driver.select_value_or_none(
                    f"SELECT state FROM {user_ref} WHERE app_name = ? AND user_id = ?",
                    app_name,
                    user_id,
                )
            return duckdb_json_dict(raw) if raw is not None else None
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return None
            raise

    def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Insert or update app-scoped ADK state synchronously."""
        app_ref = format_duckdb_identifier(self._app_state_table)
        sql = (
            f"INSERT INTO {app_ref} (app_name, state, update_time) VALUES (?, ?, ?) "
            "ON CONFLICT(app_name) DO UPDATE SET state = excluded.state, update_time = excluded.update_time"
        )
        with self._config.provide_session() as driver:
            driver.execute(sql, app_name, to_json(state), datetime.now(UTC).isoformat())
            driver.commit()

    def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Insert or update user-scoped ADK state synchronously."""
        user_ref = format_duckdb_identifier(self._user_state_table)
        sql = (
            f"INSERT INTO {user_ref} (app_name, user_id, state, update_time) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(app_name, user_id) DO UPDATE SET state = excluded.state, update_time = excluded.update_time"
        )
        with self._config.provide_session() as driver:
            driver.execute(sql, app_name, user_id, to_json(state), datetime.now(UTC).isoformat())
            driver.commit()

    def get_metadata(self, key: str) -> str | None:
        """Fetch an internal ADK metadata string synchronously."""
        meta_ref = format_duckdb_identifier(self._metadata_table)
        try:
            with self._config.provide_session() as driver:
                val = driver.select_value_or_none(f"SELECT value FROM {meta_ref} WHERE key = ?", key)
            return str(val) if val is not None else None
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return None
            raise

    def set_metadata(self, key: str, value: str) -> None:
        """Store or overwrite an internal ADK metadata entry synchronously."""
        meta_ref = format_duckdb_identifier(self._metadata_table)
        sql = f"INSERT INTO {meta_ref} (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        with self._config.provide_session() as driver:
            driver.execute(sql, key, value)
            driver.commit()


class DuckDBAsyncADKStore(_DuckDBADKSessionDDL, BaseAsyncADKStore[Any]):
    """Asynchronous DuckDB ADK session and event store over Vertebra `DuckDBAsyncDriver`."""

    __slots__ = ()

    adapter_name = "vertebra_duckdb"
    _max_table_name_parts = 2

    def __init__(self, config: Any) -> None:
        """Initialize the asynchronous DuckDB ADK session store."""
        super().__init__(config)

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

    async def create_session(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
        state: dict[str, Any],
        owner_id: Any | None = None,
    ) -> "StoredSession":
        """Create a new ADK session asynchronously."""
        now = datetime.now(UTC)
        now_iso = now.isoformat()
        state_json = to_json(state)
        session_ref = format_duckdb_identifier(self._session_table)
        if self._owner_id_column_name:
            sql = (
                f"INSERT INTO {session_ref} "
                f"(id, app_name, user_id, {format_duckdb_identifier(self._owner_id_column_name)}, state, create_time, update_time) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)"
            )
            params: tuple[Any, ...] = (session_id, app_name, user_id, owner_id, state_json, now_iso, now_iso)
        else:
            sql = (
                f"INSERT INTO {session_ref} (id, app_name, user_id, state, create_time, update_time) "
                "VALUES (?, ?, ?, ?, ?, ?)"
            )
            params = (session_id, app_name, user_id, state_json, now_iso, now_iso)
        async with self._config.provide_session() as driver:
            await driver.execute(sql, *params)
            await driver.commit()
        return cast(
            "StoredSession",
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
    ) -> "StoredSession | None":
        """Fetch an ADK session asynchronously, optionally renewing `update_time`."""
        session_ref = format_duckdb_identifier(self._session_table)
        try:
            async with self._config.provide_session() as driver:
                if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
                    now_iso = datetime.now(UTC).isoformat()
                    sql = (
                        f"UPDATE {session_ref} SET update_time = ? "
                        "WHERE app_name = ? AND user_id = ? AND id = ? "
                        "RETURNING id, app_name, user_id, state, create_time, update_time"
                    )
                    row = await driver.select_one_or_none(sql, now_iso, app_name, user_id, session_id)
                    await driver.commit()
                else:
                    sql = (
                        f"SELECT id, app_name, user_id, state, create_time, update_time FROM {session_ref} "
                        "WHERE app_name = ? AND user_id = ? AND id = ?"
                    )
                    row = await driver.select_one_or_none(sql, app_name, user_id, session_id)
                return _row_to_session(row, deserializer=from_json) if row is not None else None
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return None
            raise

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Update stored session JSON state asynchronously."""
        session_ref = format_duckdb_identifier(self._session_table)
        now_iso = datetime.now(UTC).isoformat()
        async with self._config.provide_session() as driver:
            await driver.execute(
                f"UPDATE {session_ref} SET state = ?, update_time = ? WHERE app_name = ? AND user_id = ? AND id = ?",
                to_json(state),
                now_iso,
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
    ) -> list["StoredSession"]:
        """List stored ADK sessions asynchronously."""
        column, direction, page_limit, page_offset = normalize_session_list_options(order_by, descending, limit, offset)
        if page_limit == 0:
            return []
        sql, params = _session_list_sql(
            self._session_table,
            app_name,
            user_id,
            column,
            direction,
            page_limit,
            page_offset,
        )
        try:
            async with self._config.provide_session() as driver:
                rows = await driver.select(sql, *params)
            return [_row_to_session(row, deserializer=from_json) for row in rows]
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return []
            raise

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete a session and its associated events asynchronously."""
        events_ref = format_duckdb_identifier(self._events_table)
        session_ref = format_duckdb_identifier(self._session_table)
        async with self._config.provide_session() as driver:
            await driver.execute(f"DELETE FROM {events_ref} WHERE session_id = ?", session_id)
            await driver.execute(
                f"DELETE FROM {session_ref} WHERE app_name = ? AND user_id = ? AND id = ?",
                app_name,
                user_id,
                session_id,
            )
            await driver.commit()

    async def append_event(self, event_record: "StoredEvent") -> None:
        """Append a single ADK event asynchronously."""
        events_ref = format_duckdb_identifier(self._events_table)
        sql = (
            f"INSERT INTO {events_ref} "
            "(id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        async with self._config.provide_session() as driver:
            await driver.execute(
                sql,
                event_record["id"],
                event_record["app_name"],
                event_record["user_id"],
                event_record["session_id"],
                event_record["invocation_id"],
                _ensure_utc(event_record["timestamp"]).isoformat(),
                to_json(event_record["event_data"]),
            )
            await driver.commit()

    async def append_event_and_update_state(
        self,
        event_record: "StoredEvent",
        app_name: str,
        user_id: str,
        session_id: str,
        state: dict[str, Any],
        *,
        app_state: dict[str, Any] | None = None,
        user_state: dict[str, Any] | None = None,
    ) -> "StoredSession":
        """Atomically append an event and update session/app/user durable state asynchronously."""
        now_iso = datetime.now(UTC).isoformat()
        events_ref = format_duckdb_identifier(self._events_table)
        session_ref = format_duckdb_identifier(self._session_table)
        app_ref = format_duckdb_identifier(self._app_state_table)
        user_ref = format_duckdb_identifier(self._user_state_table)
        insert_sql = (
            f"INSERT INTO {events_ref} "
            "(id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        update_sql = (
            f"UPDATE {session_ref} SET state = ?, update_time = ? "
            "WHERE app_name = ? AND user_id = ? AND id = ? "
            "RETURNING id, app_name, user_id, state, create_time, update_time"
        )
        app_sql = (
            f"INSERT INTO {app_ref} (app_name, state, update_time) VALUES (?, ?, ?) "
            "ON CONFLICT(app_name) DO UPDATE SET state = excluded.state, update_time = excluded.update_time"
        )
        user_sql = (
            f"INSERT INTO {user_ref} (app_name, user_id, state, update_time) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(app_name, user_id) DO UPDATE SET state = excluded.state, update_time = excluded.update_time"
        )
        async with self._config.provide_session() as driver:
            await driver.begin()
            try:
                await driver.execute(
                    insert_sql,
                    event_record["id"],
                    event_record["app_name"],
                    event_record["user_id"],
                    event_record["session_id"],
                    event_record["invocation_id"],
                    _ensure_utc(event_record["timestamp"]).isoformat(),
                    to_json(event_record["event_data"]),
                )
                row = await driver.select_one_or_none(
                    update_sql, to_json(state), now_iso, app_name, user_id, session_id
                )
                if row is None:
                    _raise_session_not_found(session_id)
                if app_state is not None:
                    await driver.execute(app_sql, app_name, to_json(app_state), now_iso)
                if user_state is not None:
                    await driver.execute(user_sql, app_name, user_id, to_json(user_state), now_iso)
                await driver.commit()
            except Exception:
                await driver.rollback()
                raise
        return _row_to_session(row, deserializer=from_json)

    async def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> list["StoredEvent"]:
        """Fetch ADK events asynchronously ordered ascending by timestamp."""
        if limit == 0:
            return []
        sql, params = _events_query_sql(
            self._events_table,
            self._session_table,
            app_name,
            user_id,
            session_id,
            after_timestamp,
            limit,
        )
        try:
            async with self._config.provide_session() as driver:
                rows = await driver.select(sql, *params)
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
        ref = format_duckdb_identifier(table_name)
        where = f"WHERE {column} < ?"
        params: list[Any] = [cutoff.astimezone(UTC).isoformat()]
        if app_name is not None:
            where += " AND app_name = ?"
            params.append(app_name)
        try:
            async with self._config.provide_session() as driver:
                rows = await driver.select(f"DELETE FROM {ref} {where} RETURNING 1 AS deleted", *params)
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
        events_ref = format_duckdb_identifier(self._events_table)
        session_ref = format_duckdb_identifier(self._session_table)
        where = "WHERE update_time < ?"
        params: list[Any] = [updated_before.astimezone(UTC).isoformat()]
        if app_name is not None:
            where += " AND app_name = ?"
            params.append(app_name)
        try:
            async with self._config.provide_session() as driver:
                await driver.execute(
                    f"DELETE FROM {events_ref} WHERE session_id IN (SELECT id FROM {session_ref} {where})",
                    *params,
                )
                rows = await driver.select(f"DELETE FROM {session_ref} {where} RETURNING 1 AS deleted", *params)
                await driver.commit()
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
        app_ref = format_duckdb_identifier(self._app_state_table)
        try:
            async with self._config.provide_session() as driver:
                raw = await driver.select_value_or_none(f"SELECT state FROM {app_ref} WHERE app_name = ?", app_name)
            return duckdb_json_dict(raw) if raw is not None else None
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return None
            raise

    async def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Return user-scoped ADK state dictionary asynchronously."""
        user_ref = format_duckdb_identifier(self._user_state_table)
        try:
            async with self._config.provide_session() as driver:
                raw = await driver.select_value_or_none(
                    f"SELECT state FROM {user_ref} WHERE app_name = ? AND user_id = ?",
                    app_name,
                    user_id,
                )
            return duckdb_json_dict(raw) if raw is not None else None
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return None
            raise

    async def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Insert or update app-scoped ADK state asynchronously."""
        app_ref = format_duckdb_identifier(self._app_state_table)
        sql = (
            f"INSERT INTO {app_ref} (app_name, state, update_time) VALUES (?, ?, ?) "
            "ON CONFLICT(app_name) DO UPDATE SET state = excluded.state, update_time = excluded.update_time"
        )
        async with self._config.provide_session() as driver:
            await driver.execute(sql, app_name, to_json(state), datetime.now(UTC).isoformat())
            await driver.commit()

    async def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Insert or update user-scoped ADK state asynchronously."""
        user_ref = format_duckdb_identifier(self._user_state_table)
        sql = (
            f"INSERT INTO {user_ref} (app_name, user_id, state, update_time) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(app_name, user_id) DO UPDATE SET state = excluded.state, update_time = excluded.update_time"
        )
        async with self._config.provide_session() as driver:
            await driver.execute(sql, app_name, user_id, to_json(state), datetime.now(UTC).isoformat())
            await driver.commit()

    async def get_metadata(self, key: str) -> str | None:
        """Fetch an internal ADK metadata string asynchronously."""
        meta_ref = format_duckdb_identifier(self._metadata_table)
        try:
            async with self._config.provide_session() as driver:
                val = await driver.select_value_or_none(f"SELECT value FROM {meta_ref} WHERE key = ?", key)
            return str(val) if val is not None else None
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return None
            raise

    async def set_metadata(self, key: str, value: str) -> None:
        """Store or overwrite an internal ADK metadata entry asynchronously."""
        meta_ref = format_duckdb_identifier(self._metadata_table)
        sql = f"INSERT INTO {meta_ref} (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        async with self._config.provide_session() as driver:
            await driver.execute(sql, key, value)
            await driver.commit()


def _build_memory_table_ddl(memory_table: str, owner_id_ddl: str | None) -> str:
    table_ref = format_duckdb_identifier(memory_table)
    owner_line = f",\n    {owner_id_ddl}" if owner_id_ddl else ""
    return (
        f"CREATE TABLE IF NOT EXISTS {table_ref} (\n"
        "    id VARCHAR(128) PRIMARY KEY,\n"
        "    session_id VARCHAR(128) NOT NULL,\n"
        "    app_name VARCHAR(128) NOT NULL,\n"
        "    user_id VARCHAR(128) NOT NULL,\n"
        "    scope VARCHAR(16) NOT NULL DEFAULT 'user',\n"
        "    event_id VARCHAR(128) NOT NULL UNIQUE,\n"
        f"    author VARCHAR(256){owner_line},\n"
        "    timestamp TIMESTAMPTZ NOT NULL,\n"
        "    content_json VARCHAR NOT NULL,\n"
        "    content_text TEXT NOT NULL,\n"
        "    metadata_json VARCHAR,\n"
        "    embedding FLOAT[],\n"
        "    inserted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP\n"
        ");\n"
        f"CREATE INDEX IF NOT EXISTS {_duckdb_index_name(memory_table, 'app_scope_user_time')} "
        f"ON {table_ref}(app_name, scope, user_id, timestamp DESC);\n"
        f"CREATE INDEX IF NOT EXISTS {_duckdb_index_name(memory_table, 'scope')} "
        f"ON {table_ref}(app_name, scope);\n"
        f"CREATE INDEX IF NOT EXISTS {_duckdb_index_name(memory_table, 'session')} "
        f"ON {table_ref}(session_id);"
    )


def _embedding_bind_pair(entry: "StoredMemory") -> tuple[str | None, str]:
    raw_emb = entry.get("embedding")
    if raw_emb is None:
        return None, "[]"
    encoded = to_json([float(x) for x in raw_emb])
    return encoded, encoded


def _memory_insert_statement(
    memory_table: str,
    owner_id_col: str | None,
    entries: list["StoredMemory"],
    owner_id: object | None,
) -> tuple[str, list[tuple[Any, ...]]]:
    table_ref = format_duckdb_identifier(memory_table)
    emb_expr = "CAST(? AS FLOAT[])"
    if owner_id_col:
        owner_ident = format_duckdb_identifier(owner_id_col)
        sql = (
            f"INSERT INTO {table_ref} ("
            "id, session_id, app_name, user_id, scope, event_id, author, "
            f"{owner_ident}, timestamp, content_json, content_text, metadata_json, embedding, inserted_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
            f"CASE WHEN ? IS NULL THEN NULL ELSE {emb_expr} END, ?) "
            "ON CONFLICT (event_id) DO NOTHING RETURNING 1 AS inserted"
        )
        rows: list[tuple[Any, ...]] = []
        for entry in entries:
            emb_null, emb_val = _embedding_bind_pair(entry)
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
                    _ensure_utc(entry["timestamp"]).isoformat(),
                    to_json(entry["content_json"]),
                    entry["content_text"],
                    to_json(entry.get("metadata_json")) if entry.get("metadata_json") is not None else None,
                    emb_null,
                    emb_val,
                    _ensure_utc(entry["inserted_at"]).isoformat(),
                )
            )
        return sql, rows

    sql = (
        f"INSERT INTO {table_ref} ("
        "id, session_id, app_name, user_id, scope, event_id, author, "
        "timestamp, content_json, content_text, metadata_json, embedding, inserted_at"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
        f"CASE WHEN ? IS NULL THEN NULL ELSE {emb_expr} END, ?) "
        "ON CONFLICT (event_id) DO NOTHING RETURNING 1 AS inserted"
    )
    plain_rows: list[tuple[Any, ...]] = []
    for entry in entries:
        emb_null, emb_val = _embedding_bind_pair(entry)
        plain_rows.append(
            (
                entry["id"],
                entry["session_id"],
                entry["app_name"],
                entry["user_id"],
                entry.get("scope", "user"),
                entry["event_id"],
                entry.get("author"),
                _ensure_utc(entry["timestamp"]).isoformat(),
                to_json(entry["content_json"]),
                entry["content_text"],
                to_json(entry.get("metadata_json")) if entry.get("metadata_json") is not None else None,
                emb_null,
                emb_val,
                _ensure_utc(entry["inserted_at"]).isoformat(),
            )
        )
    return sql, plain_rows


def _cosine_search_sql(
    memory_table: str,
    app_name: str,
    user_id: str,
    scope_filter: str,
    embedding: Sequence[float],
    limit_value: int,
) -> tuple[str, tuple[Any, ...]]:
    dim = len(embedding)
    table_ref = format_duckdb_identifier(memory_table)
    where_scope, scope_params = build_duckdb_scope_where(app_name, user_id, scope_filter)
    vec_json = to_json([float(x) for x in embedding])
    sql = (
        "SELECT id, session_id, app_name, user_id, scope, event_id, author, "
        "timestamp, content_json, content_text, metadata_json, CAST(embedding AS VARCHAR) AS embedding, inserted_at, "
        f"array_cosine_similarity(CAST(embedding AS FLOAT[{dim}]), CAST(? AS FLOAT[{dim}])) AS score "
        f"FROM {table_ref} "
        f"WHERE {where_scope} AND embedding IS NOT NULL AND len(embedding) = ? "
        "ORDER BY score DESC LIMIT ?"
    )
    return sql, (vec_json, *scope_params, dim, limit_value)


def _fts_search_sql(
    memory_table: str,
    query: str,
    app_name: str,
    user_id: str,
    scope_filter: str,
    limit_value: int,
) -> tuple[str, tuple[Any, ...]]:
    schema, bare_table = split_schema_and_table(memory_table)
    table_ref = format_duckdb_identifier(memory_table)
    fts_macro = f"{format_duckdb_identifier(f'fts_{schema}_{bare_table}')}.match_bm25"
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


def _ilike_search_sql(
    memory_table: str,
    query: str,
    app_name: str,
    user_id: str,
    scope_filter: str,
    limit_value: int,
) -> tuple[str, tuple[Any, ...]]:
    table_ref = format_duckdb_identifier(memory_table)
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


class DuckDBSyncADKMemoryStore(_BaseSyncADKMemoryStore[Any]):
    """Synchronous DuckDB ADK memory store with `array_cosine_similarity` vector ranking and BM25 `fts`."""

    __slots__ = ()

    adapter_name = "vertebra_duckdb"
    _max_table_name_parts = 2

    def __init__(self, config: Any) -> None:
        """Initialize the synchronous DuckDB ADK memory store."""
        super().__init__(config)

    def _memory_table_ddl(self) -> str:
        return _build_memory_table_ddl(self._memory_table, self._owner_id_column_ddl)

    def _drop_memory_table_sql(self) -> list[str]:
        return [f"DROP TABLE IF EXISTS {format_duckdb_identifier(self._memory_table)}"]

    def _fts_index_ddl(self, *, overwrite: bool) -> str:
        opts = _render_fts_options(self._config, overwrite=overwrite)
        escaped_table = self._memory_table.replace("'", "''")
        return f"PRAGMA create_fts_index('{escaped_table}', 'id', 'content_text', {opts})"

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

    def insert_memory_entries(self, entries: list["StoredMemory"], owner_id: object | None = None) -> int:
        """Bulk insert ADK memory entries with event deduplication synchronously."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0
        sql, rows_params = _memory_insert_statement(
            self._memory_table,
            self._owner_id_column_name,
            entries,
            owner_id,
        )
        inserted = 0
        with self._config.provide_session() as driver:
            driver.begin()
            try:
                for params in rows_params:
                    res = driver.select(sql, *params)
                    inserted += len(res)
                driver.commit()
            except Exception:
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
    ) -> list["StoredMemory"]:
        """Search ADK memory via `array_cosine_similarity`, DuckDB BM25 `fts`, or `ILIKE`."""
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
        if embedding is not None and embedding:
            vec_sql, vec_params = _cosine_search_sql(
                self._memory_table,
                app_name,
                user_id,
                scope_filter,
                embedding,
                limit_value,
            )
            try:
                with self._config.provide_session() as driver:
                    rows = driver.select(vec_sql, *vec_params)
            except Exception as exc:
                if is_duckdb_table_missing(exc):
                    return []
                rows = None

        if rows is None and has_query and self._use_fts:
            with self._config.provide_session() as driver:
                if self._ensure_fts_extension(driver):
                    fts_sql, fts_params = _fts_search_sql(
                        self._memory_table,
                        query,
                        app_name,
                        user_id,
                        scope_filter,
                        limit_value if embedding is None else max(limit_value * 5, 50),
                    )
                    with contextlib.suppress(Exception):
                        rows = driver.select(fts_sql, *fts_params)

        if rows is None:
            fetch_limit = limit_value if embedding is None else max(limit_value * 5, 50)
            ilike_sql, ilike_params = _ilike_search_sql(
                self._memory_table,
                query,
                app_name,
                user_id,
                scope_filter,
                fetch_limit,
            )
            try:
                with self._config.provide_session() as driver:
                    rows = driver.select(ilike_sql, *ilike_params)
            except Exception as exc:
                if is_duckdb_table_missing(exc):
                    return []
                raise

        records = [_row_to_memory(row, deserializer=from_json) for row in rows]
        return _rank_memory_records(records, embedding, limit_value)

    def delete_entries_by_session(self, session_id: str) -> int:
        """Delete memory entries matching `session_id` synchronously."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        table_ref = format_duckdb_identifier(self._memory_table)
        try:
            with self._config.provide_session() as driver:
                rows = driver.select(
                    f"DELETE FROM {table_ref} WHERE session_id = ? RETURNING 1 AS deleted",
                    session_id,
                )
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
        try:
            with self._config.provide_session() as driver:
                rows = driver.select(
                    f"DELETE FROM {table_ref} WHERE {' AND '.join(clauses)} RETURNING 1 AS deleted",
                    *params,
                )
                driver.commit()
                count = len(rows)
                if self._use_fts and count > 0:
                    self._refresh_fts_index(driver, overwrite=True)
                return count
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return 0
            raise


class DuckDBAsyncADKMemoryStore(_BaseAsyncADKMemoryStore[Any]):
    """Asynchronous DuckDB ADK memory store with `array_cosine_similarity` vector ranking and BM25 `fts`."""

    __slots__ = ()

    adapter_name = "vertebra_duckdb"
    _max_table_name_parts = 2

    def __init__(self, config: Any) -> None:
        """Initialize the asynchronous DuckDB ADK memory store."""
        super().__init__(config)

    async def _memory_table_ddl(self) -> str:
        return _build_memory_table_ddl(self._memory_table, self._owner_id_column_ddl)

    def _drop_memory_table_sql(self) -> list[str]:
        return [f"DROP TABLE IF EXISTS {format_duckdb_identifier(self._memory_table)}"]

    def _fts_index_ddl(self, *, overwrite: bool) -> str:
        opts = _render_fts_options(self._config, overwrite=overwrite)
        escaped_table = self._memory_table.replace("'", "''")
        return f"PRAGMA create_fts_index('{escaped_table}', 'id', 'content_text', {opts})"

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

    async def insert_memory_entries(self, entries: list["StoredMemory"], owner_id: object | None = None) -> int:
        """Bulk insert ADK memory entries with event deduplication asynchronously."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0
        sql, rows_params = _memory_insert_statement(
            self._memory_table,
            self._owner_id_column_name,
            entries,
            owner_id,
        )
        inserted = 0
        async with self._config.provide_session() as driver:
            await driver.begin()
            try:
                for params in rows_params:
                    res = await driver.select(sql, *params)
                    inserted += len(res)
                await driver.commit()
            except Exception:
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
    ) -> list["StoredMemory"]:
        """Search ADK memory asynchronously via `array_cosine_similarity`, BM25 `fts`, or `ILIKE`."""
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
        if embedding is not None and embedding:
            vec_sql, vec_params = _cosine_search_sql(
                self._memory_table,
                app_name,
                user_id,
                scope_filter,
                embedding,
                limit_value,
            )
            try:
                async with self._config.provide_session() as driver:
                    rows = await driver.select(vec_sql, *vec_params)
            except Exception as exc:
                if is_duckdb_table_missing(exc):
                    return []
                rows = None

        if rows is None and has_query and self._use_fts:
            async with self._config.provide_session() as driver:
                if await self._ensure_fts_extension(driver):
                    fts_sql, fts_params = _fts_search_sql(
                        self._memory_table,
                        query,
                        app_name,
                        user_id,
                        scope_filter,
                        limit_value if embedding is None else max(limit_value * 5, 50),
                    )
                    with contextlib.suppress(Exception):
                        rows = await driver.select(fts_sql, *fts_params)

        if rows is None:
            fetch_limit = limit_value if embedding is None else max(limit_value * 5, 50)
            ilike_sql, ilike_params = _ilike_search_sql(
                self._memory_table,
                query,
                app_name,
                user_id,
                scope_filter,
                fetch_limit,
            )
            try:
                async with self._config.provide_session() as driver:
                    rows = await driver.select(ilike_sql, *ilike_params)
            except Exception as exc:
                if is_duckdb_table_missing(exc):
                    return []
                raise

        records = [_row_to_memory(row, deserializer=from_json) for row in rows]
        return _rank_memory_records(records, embedding, limit_value)

    async def delete_entries_by_session(self, session_id: str) -> int:
        """Delete memory entries matching `session_id` asynchronously."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        table_ref = format_duckdb_identifier(self._memory_table)
        try:
            async with self._config.provide_session() as driver:
                rows = await driver.select(
                    f"DELETE FROM {table_ref} WHERE session_id = ? RETURNING 1 AS deleted",
                    session_id,
                )
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
        try:
            async with self._config.provide_session() as driver:
                rows = await driver.select(
                    f"DELETE FROM {table_ref} WHERE {' AND '.join(clauses)} RETURNING 1 AS deleted",
                    *params,
                )
                await driver.commit()
                count = len(rows)
                if self._use_fts and count > 0:
                    await self._refresh_fts_index(driver, overwrite=True)
                return count
        except Exception as exc:
            if is_duckdb_table_missing(exc):
                return 0
            raise


class _DuckDBArtifactStoreCommon:
    __slots__ = ("_artifact_table", "_config", "_enabled")

    adapter_name = "vertebra_duckdb"

    def __init__(self, config: Any) -> None:
        self._config = config
        adk_cfg = _adk_config(config)
        self._artifact_table = str(adk_cfg.get("artifact_table", "adk_artifacts"))
        self._enabled = bool(adk_cfg.get("artifact_enabled", True))

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

    @staticmethod
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

    @staticmethod
    def _row_to_artifact_record(row: Mapping[str, Any]) -> dict[str, Any]:
        meta_raw = row.get("custom_metadata")
        return {
            "app_name": str(row["app_name"]),
            "user_id": str(row["user_id"]),
            "session_id": str(row["session_id"]) if row.get("session_id") else None,
            "filename": str(row["filename"]),
            "version": int(row["version"]),
            "mime_type": str(row["mime_type"]) if row.get("mime_type") is not None else None,
            "canonical_uri": str(row["canonical_uri"]),
            "custom_metadata": duckdb_json_dict(meta_raw) if meta_raw is not None else None,
            "created_at": _ensure_utc(row.get("created_at")),
        }


class DuckDBSyncADKArtifactStore(_DuckDBArtifactStoreCommon):
    """Synchronous DuckDB ADK artifact metadata store backed by `ArtifactReference`."""

    __slots__ = ()

    def create_table(self) -> None:
        """Create the DuckDB artifact metadata table if absent."""
        with self._config.provide_session() as driver:
            driver.execute_script(self._artifact_table_ddl())
            driver.commit()

    def insert_artifact(self, record: dict[str, Any]) -> None:
        """Insert a raw artifact dictionary record synchronously."""
        table_ref = format_duckdb_identifier(self._artifact_table)
        session_val = record.get("session_id") or ""
        meta = record.get("custom_metadata")
        sql = (
            f"INSERT INTO {table_ref} "
            "(app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        with self._config.provide_session() as driver:
            driver.execute(
                sql,
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
        table_ref = format_duckdb_identifier(self._artifact_table)
        session_val = session_id if session_id is not None else ""
        now_dt = reference.updated or datetime.now(UTC)
        sql = (
            f"INSERT INTO {table_ref} "
            "(app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, size, updated, signed_url, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        with self._config.provide_session() as driver:
            driver.execute(
                sql,
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
        table_ref = format_duckdb_identifier(self._artifact_table)
        session_val = session_id if session_id is not None else ""
        with self._config.provide_session() as driver:
            if version is not None:
                row = driver.select_one_or_none(
                    f"SELECT * FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? AND version = ? LIMIT 1",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                    int(version),
                )
            else:
                row = driver.select_one_or_none(
                    f"SELECT * FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? ORDER BY version DESC LIMIT 1",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                )
        return self._row_to_artifact_reference(row) if row is not None else None

    def get_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> dict[str, Any] | None:
        """Fetch an artifact dictionary record synchronously."""
        table_ref = format_duckdb_identifier(self._artifact_table)
        session_val = session_id if session_id is not None else ""
        with self._config.provide_session() as driver:
            if version is not None:
                row = driver.select_one_or_none(
                    f"SELECT * FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? AND version = ? LIMIT 1",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                    int(version),
                )
            else:
                row = driver.select_one_or_none(
                    f"SELECT * FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? ORDER BY version DESC LIMIT 1",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                )
        return self._row_to_artifact_record(row) if row is not None else None

    def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """List distinct artifact filenames within user or session scope synchronously."""
        table_ref = format_duckdb_identifier(self._artifact_table)
        with self._config.provide_session() as driver:
            if session_id is not None:
                rows = driver.select(
                    f"SELECT DISTINCT filename FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id IN ('', ?) ORDER BY filename ASC",
                    app_name,
                    user_id,
                    session_id,
                )
            else:
                rows = driver.select(
                    f"SELECT DISTINCT filename FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = '' ORDER BY filename ASC",
                    app_name,
                    user_id,
                )
        return [str(r["filename"]) for r in rows]

    def list_artifact_versions(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[int]:
        """List ascending version numbers for a stored artifact synchronously."""
        table_ref = format_duckdb_identifier(self._artifact_table)
        session_val = session_id if session_id is not None else ""
        with self._config.provide_session() as driver:
            rows = driver.select(
                f"SELECT version FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? ORDER BY version ASC",
                app_name,
                user_id,
                session_val,
                filename,
            )
        return [int(r["version"]) for r in rows]

    def delete_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Delete all stored versions of an artifact and return removed records synchronously."""
        table_ref = format_duckdb_identifier(self._artifact_table)
        session_val = session_id if session_id is not None else ""
        with self._config.provide_session() as driver:
            rows = driver.select(
                f"SELECT * FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ?",
                app_name,
                user_id,
                session_val,
                filename,
            )
            driver.execute(
                f"DELETE FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ?",
                app_name,
                user_id,
                session_val,
                filename,
            )
            driver.commit()
        return [self._row_to_artifact_record(row) for row in rows]

    def get_next_version(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> int:
        """Compute the next monotonically increasing revision integer synchronously."""
        table_ref = format_duckdb_identifier(self._artifact_table)
        session_val = session_id if session_id is not None else ""
        with self._config.provide_session() as driver:
            max_ver = driver.select_value_or_none(
                f"SELECT MAX(version) FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ?",
                app_name,
                user_id,
                session_val,
                filename,
            )
        return (int(max_ver) + 1) if max_ver is not None else 0


class DuckDBAsyncADKArtifactStore(_DuckDBArtifactStoreCommon):
    """Asynchronous DuckDB ADK artifact metadata store backed by `ArtifactReference`."""

    __slots__ = ()

    async def create_table(self) -> None:
        """Create the DuckDB artifact metadata table asynchronously."""
        async with self._config.provide_session() as driver:
            await driver.execute_script(self._artifact_table_ddl())
            await driver.commit()

    async def insert_artifact(self, record: dict[str, Any]) -> None:
        """Insert a raw artifact dictionary record asynchronously."""
        table_ref = format_duckdb_identifier(self._artifact_table)
        session_val = record.get("session_id") or ""
        meta = record.get("custom_metadata")
        sql = (
            f"INSERT INTO {table_ref} "
            "(app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        async with self._config.provide_session() as driver:
            await driver.execute(
                sql,
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
        table_ref = format_duckdb_identifier(self._artifact_table)
        session_val = session_id if session_id is not None else ""
        now_dt = reference.updated or datetime.now(UTC)
        sql = (
            f"INSERT INTO {table_ref} "
            "(app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, size, updated, signed_url, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        async with self._config.provide_session() as driver:
            await driver.execute(
                sql,
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
        table_ref = format_duckdb_identifier(self._artifact_table)
        session_val = session_id if session_id is not None else ""
        async with self._config.provide_session() as driver:
            if version is not None:
                row = await driver.select_one_or_none(
                    f"SELECT * FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? AND version = ? LIMIT 1",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                    int(version),
                )
            else:
                row = await driver.select_one_or_none(
                    f"SELECT * FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? ORDER BY version DESC LIMIT 1",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                )
        return self._row_to_artifact_reference(row) if row is not None else None

    async def get_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> dict[str, Any] | None:
        """Fetch an artifact dictionary record asynchronously."""
        table_ref = format_duckdb_identifier(self._artifact_table)
        session_val = session_id if session_id is not None else ""
        async with self._config.provide_session() as driver:
            if version is not None:
                row = await driver.select_one_or_none(
                    f"SELECT * FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? AND version = ? LIMIT 1",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                    int(version),
                )
            else:
                row = await driver.select_one_or_none(
                    f"SELECT * FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? ORDER BY version DESC LIMIT 1",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                )
        return self._row_to_artifact_record(row) if row is not None else None

    async def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """List distinct artifact filenames within user or session scope asynchronously."""
        table_ref = format_duckdb_identifier(self._artifact_table)
        async with self._config.provide_session() as driver:
            if session_id is not None:
                rows = await driver.select(
                    f"SELECT DISTINCT filename FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id IN ('', ?) ORDER BY filename ASC",
                    app_name,
                    user_id,
                    session_id,
                )
            else:
                rows = await driver.select(
                    f"SELECT DISTINCT filename FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = '' ORDER BY filename ASC",
                    app_name,
                    user_id,
                )
        return [str(r["filename"]) for r in rows]

    async def list_artifact_versions(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[int]:
        """List ascending version numbers for a stored artifact asynchronously."""
        table_ref = format_duckdb_identifier(self._artifact_table)
        session_val = session_id if session_id is not None else ""
        async with self._config.provide_session() as driver:
            rows = await driver.select(
                f"SELECT version FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? ORDER BY version ASC",
                app_name,
                user_id,
                session_val,
                filename,
            )
        return [int(r["version"]) for r in rows]

    async def delete_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Delete all stored versions of an artifact and return removed records asynchronously."""
        table_ref = format_duckdb_identifier(self._artifact_table)
        session_val = session_id if session_id is not None else ""
        async with self._config.provide_session() as driver:
            rows = await driver.select(
                f"SELECT * FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ?",
                app_name,
                user_id,
                session_val,
                filename,
            )
            await driver.execute(
                f"DELETE FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ?",
                app_name,
                user_id,
                session_val,
                filename,
            )
            await driver.commit()
        return [self._row_to_artifact_record(row) for row in rows]

    async def get_next_version(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> int:
        """Compute the next monotonically increasing revision integer asynchronously."""
        table_ref = format_duckdb_identifier(self._artifact_table)
        session_val = session_id if session_id is not None else ""
        async with self._config.provide_session() as driver:
            max_ver = await driver.select_value_or_none(
                f"SELECT MAX(version) FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ?",
                app_name,
                user_id,
                session_val,
                filename,
            )
        return (int(max_ver) + 1) if max_ver is not None else 0
