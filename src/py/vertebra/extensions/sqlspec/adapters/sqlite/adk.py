"""SQLite ADK session, event, memory (FTS5), and artifact metadata stores."""

import contextlib
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, NotRequired, cast

from sqlspec.config import ADKConfig
from sqlspec.exceptions import SQLParsingError
from sqlspec.extensions.adk import BaseAsyncADKStore, BaseSyncADKStore, normalize_session_list_options
from sqlspec.utils.serializers import from_json, to_json

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

if TYPE_CHECKING:
    from sqlspec.extensions.adk import SessionOrderBy, StoredEvent, StoredMemory, StoredSession
    from sqlspec.extensions.adk.artifact._types import StoredArtifact
    from sqlspec.extensions.adk.artifact.store import BaseAsyncADKArtifactStore as _BaseAsyncADKArtifactStore
    from sqlspec.extensions.adk.artifact.store import BaseSyncADKArtifactStore as _BaseSyncADKArtifactStore
    from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore as _BaseAsyncADKMemoryStore
    from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore as _BaseSyncADKMemoryStore
else:
    try:
        from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore as _BaseAsyncADKMemoryStore
        from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore as _BaseSyncADKMemoryStore
    except ImportError:

        class _FallbackADKMemoryStoreCommon:
            """SDK-free ADK memory store base used when optional `google-adk` is absent."""

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
            adapter_name: ClassVar[str | None] = None

            @classmethod
            def __class_getitem__(cls, _item: Any) -> type[Any]:
                return cls

            def __init__(self, config: Any) -> None:
                self._config = config
                ext = getattr(config, "extension_config", {})
                adk = ext.get("adk", {}) if isinstance(ext, Mapping) else {}
                if not isinstance(adk, Mapping):
                    adk = {}
                self._enabled: bool = bool(adk.get("enable_memory", True))
                self._memory_table: str = str(adk.get("memory_table", "adk_memory_entries"))
                self._use_fts: bool = bool(adk.get("use_fts", False))
                self._max_results: int = int(adk.get("max_results", 20))
                self._vector_index_type: str = str(adk.get("vector_index_type", "hnsw"))
                self._vector_dimensions: int = int(adk.get("vector_dimensions", 768))
                self._enable_bm25: bool = bool(adk.get("enable_bm25", False))
                self._scann_num_leaves: int = int(adk.get("scann_num_leaves", 100))
                self._scann_quantizer: str = str(adk.get("scann_quantizer", "SQ8"))
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
                ext = getattr(self._config, "extension_config", {})
                adk = ext.get("adk", {}) if isinstance(ext, Mapping) else {}
                if not isinstance(adk, Mapping):
                    return True
                return bool(adk.get("manage_schema", True)) and bool(adk.get("create_schema", True))

            def reconcile_schema(self, *, assume_existing: bool = False) -> Any:
                _ = assume_existing
                return None

        class _BaseSyncADKMemoryStore(_FallbackADKMemoryStoreCommon):
            """Fallback synchronous memory store base when `google-adk` is not installed."""

            def drop_tables(self) -> None:
                statements = self._drop_memory_table_sql()
                with self._config.provide_session() as driver:
                    for statement in statements:
                        driver.execute(statement)
                    driver.commit()

            def ensure_tables(self) -> None:
                if not self._enabled:
                    return
                if self.create_schema_enabled:
                    self.create_tables()

            def _drop_memory_table_sql(self) -> list[str]:
                raise NotImplementedError

            def create_tables(self) -> None:
                raise NotImplementedError

        class _BaseAsyncADKMemoryStore(_FallbackADKMemoryStoreCommon):
            """Fallback asynchronous memory store base when `google-adk` is not installed."""

            async def drop_tables(self) -> None:
                statements = self._drop_memory_table_sql()
                async with self._config.provide_session() as driver:
                    for statement in statements:
                        await driver.execute(statement)
                    await driver.commit()

            async def ensure_tables(self) -> None:
                if not self._enabled:
                    return
                if self.create_schema_enabled:
                    await self.create_tables()

            def _drop_memory_table_sql(self) -> list[str]:
                raise NotImplementedError

            async def create_tables(self) -> None:
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
                return cls

            def __init__(self, config: Any) -> None:
                self._config = config
                ext = getattr(config, "extension_config", {})
                adk = ext.get("adk", {}) if isinstance(ext, Mapping) else {}
                self._artifact_table: str = (
                    str(adk.get("artifact_table", "adk_artifacts")) if isinstance(adk, Mapping) else "adk_artifacts"
                )

            @property
            def config(self) -> Any:
                return self._config

            @property
            def artifact_table(self) -> str:
                return self._artifact_table

        class _BaseSyncADKArtifactStore(_FallbackADKArtifactStoreCommon):
            """Fallback synchronous ADK artifact store base when `google-adk` is absent."""

            def ensure_table(self) -> None:
                self.create_table()

            def create_table(self) -> None:
                raise NotImplementedError

        class _BaseAsyncADKArtifactStore(_FallbackADKArtifactStoreCommon):
            """Fallback asynchronous ADK artifact store base when `google-adk` is absent."""

            async def ensure_table(self) -> None:
                await self.create_table()

            async def create_table(self) -> None:
                raise NotImplementedError


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

_SESSION_BASE_PRAGMAS: Final[tuple[str, ...]] = (
    "PRAGMA foreign_keys = ON",
    "PRAGMA cache_size = -64000",
    "PRAGMA mmap_size = 30000000",
    "PRAGMA journal_size_limit = 67108864",
)


def _is_missing_table_error(exc: BaseException) -> bool:
    return SQLITE_TABLE_NOT_FOUND_TEXT in str(exc).lower()


def _adk_pragma_statements(config: Any) -> list[str]:
    ext = getattr(config, "extension_config", {})
    adk_cfg = ext.get("adk", {}) if isinstance(ext, Mapping) else {}
    overrides = adk_cfg.get("pragma_overrides") if isinstance(adk_cfg, Mapping) else None
    if overrides is not None and not isinstance(overrides, Mapping):
        msg = "extension_config['adk']['pragma_overrides'] must be a mapping of PRAGMA names to values"
        raise ImproperConfigurationError(msg)
    merged = list(_SESSION_BASE_PRAGMAS)
    if overrides is not None:
        synthetic_cfg = cast(
            "Any", type("_TmpCfg", (), {"extension_config": {"adk": {"pragma_overrides": overrides}}})()
        )
        merged.extend(extension_pragma_statements(synthetic_cfg, "adk"))
    return merged


def _build_session_list_query(
    table_name: str,
    app_name: str,
    user_id: str | None,
    column: str,
    direction: str,
    page_limit: int | None,
    page_offset: int,
) -> tuple[str, tuple[Any, ...]]:
    where_clause = "app_name = ? AND user_id = ?" if user_id is not None else "app_name = ?"
    params: list[Any] = [app_name, user_id] if user_id is not None else [app_name]
    limit_clause = ""
    if page_limit is not None:
        limit_clause = " LIMIT ? OFFSET ?" if page_offset > 0 else " LIMIT ?"
        params.append(page_limit)
        if page_offset > 0:
            params.append(page_offset)
    sql = (
        f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table_name} "
        f"WHERE {where_clause} ORDER BY {column} {direction}, id {direction}{limit_clause}"
    )
    return sql, tuple(params)


def _row_to_stored_session(row: Mapping[str, Any]) -> "StoredSession":
    raw_state = row.get("state")
    return cast(
        "StoredSession",
        {
            "id": str(row["id"]),
            "app_name": str(row["app_name"]),
            "user_id": str(row["user_id"]),
            "state": from_json(raw_state) if raw_state else {},
            "create_time": julian_to_datetime(row["create_time"]),
            "update_time": julian_to_datetime(row["update_time"]),
        },
    )


def _row_to_stored_event(row: Mapping[str, Any]) -> "StoredEvent":
    raw_event = row.get("event_data")
    return cast(
        "StoredEvent",
        {
            "id": str(row["id"]),
            "app_name": str(row["app_name"]),
            "user_id": str(row["user_id"]),
            "session_id": str(row["session_id"]),
            "invocation_id": row.get("invocation_id"),
            "timestamp": julian_to_datetime(row["timestamp"]),
            "event_data": from_json(raw_event) if raw_event else {},
        },
    )


def _row_to_stored_memory(row: Mapping[str, Any]) -> "StoredMemory":
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


def _row_to_stored_artifact(row: Mapping[str, Any]) -> "StoredArtifact":
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


class SQLiteADKConfig(ADKConfig):
    """SQLite-specific ADK configuration adding PRAGMA and FTS5 tuning fields."""

    pragma_overrides: NotRequired[Mapping[str, str | int | bool]]
    """Additional validated SQLite PRAGMA overrides applied after defaults."""

    fts_tokenize: NotRequired[str]
    """Optional FTS5 tokenizer specification used when `use_fts` is enabled."""

    fts_detail: NotRequired[Literal["full", "column", "none"]]
    """Optional FTS5 indexing detail level (`'full'`, `'column'`, or `'none'`)."""


class SQLiteSyncADKStore(BaseSyncADKStore[Any]):
    """Synchronous SQLite ADK session and event store over Vertebra `SQLiteSyncDriver`."""

    __slots__ = ("_configured_driver_ids", "_pragma_statements")

    adapter_name = "vertebra_sqlite"

    def __init__(self, config: Any) -> None:
        """Initialize the synchronous SQLite ADK session store."""
        super().__init__(config)
        self._pragma_statements = _adk_pragma_statements(config)
        self._configured_driver_ids: set[int] = set()

    def _ensure_pragmas(self, driver: Any) -> None:
        if id(driver) not in self._configured_driver_ids:
            apply_pragmas_sync(driver, self._pragma_statements)
            self._configured_driver_ids.add(id(driver))

    def _sessions_table_ddl(self) -> str:
        owner_line = f",\n    {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return (
            f"CREATE TABLE IF NOT EXISTS {self._session_table} (\n"
            f"    id TEXT PRIMARY KEY,\n"
            f"    app_name TEXT NOT NULL,\n"
            f"    user_id TEXT NOT NULL{owner_line},\n"
            f"    state TEXT NOT NULL DEFAULT '{{}}',\n"
            f"    create_time REAL NOT NULL,\n"
            f"    update_time REAL NOT NULL\n"
            f");\n"
            f"CREATE INDEX IF NOT EXISTS idx_{self._session_table}_app_user ON {self._session_table}(app_name, user_id);\n"
            f"CREATE INDEX IF NOT EXISTS idx_{self._session_table}_update_time ON {self._session_table}(update_time DESC);"
        )

    def _events_table_ddl(self) -> str:
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
            f"CREATE INDEX IF NOT EXISTS idx_{self._events_table}_session ON {self._events_table}(app_name, user_id, session_id, timestamp ASC);\n"
            f"CREATE INDEX IF NOT EXISTS idx_{self._events_table}_invocation ON {self._events_table}(invocation_id);\n"
            f"CREATE INDEX IF NOT EXISTS idx_{self._events_table}_timestamp ON {self._events_table}(timestamp ASC);\n"
            f"CREATE INDEX IF NOT EXISTS idx_{self._events_table}_app_timestamp ON {self._events_table}(app_name, timestamp ASC);"
        )

    def _app_states_table_ddl(self) -> str:
        return (
            f"CREATE TABLE IF NOT EXISTS {self._app_state_table} (\n"
            f"    app_name TEXT PRIMARY KEY,\n"
            f"    state TEXT NOT NULL DEFAULT '{{}}',\n"
            f"    update_time REAL NOT NULL\n"
            f");"
        )

    def _user_states_table_ddl(self) -> str:
        return (
            f"CREATE TABLE IF NOT EXISTS {self._user_state_table} (\n"
            f"    app_name TEXT NOT NULL,\n"
            f"    user_id TEXT NOT NULL,\n"
            f"    state TEXT NOT NULL DEFAULT '{{}}',\n"
            f"    update_time REAL NOT NULL,\n"
            f"    PRIMARY KEY (app_name, user_id)\n"
            f");"
        )

    def _metadata_table_ddl(self) -> str:
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

    def create_session(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
        state: dict[str, Any],
        owner_id: Any | None = None,
    ) -> "StoredSession":
        """Create a new ADK session row and return the persisted `StoredSession`."""
        now = datetime.now(UTC)
        now_julian = datetime_to_julian(now)
        state_json = to_json(state)
        if self._owner_id_column_name:
            sql = (
                f"INSERT INTO {self._session_table} "
                f"(id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)"
            )
            params: tuple[Any, ...] = (session_id, app_name, user_id, owner_id, state_json, now_julian, now_julian)
        else:
            sql = (
                f"INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time) "
                "VALUES (?, ?, ?, ?, ?, ?)"
            )
            params = (session_id, app_name, user_id, state_json, now_julian, now_julian)
        with self._config.provide_session() as driver:
            self._ensure_pragmas(driver)
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
        """Fetch an ADK session by composite key, optionally bumping `update_time`."""
        select_sql = (
            f"SELECT id, app_name, user_id, state, create_time, update_time FROM {self._session_table} "
            "WHERE app_name = ? AND user_id = ? AND id = ?"
        )
        try:
            with self._config.provide_session() as driver:
                self._ensure_pragmas(driver)
                if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
                    now_julian = datetime_to_julian(datetime.now(UTC))
                    driver.execute(
                        f"UPDATE {self._session_table} SET update_time = ? WHERE app_name = ? AND user_id = ? AND id = ?",
                        now_julian,
                        app_name,
                        user_id,
                        session_id,
                    )
                    driver.commit()
                row = driver.select_one_or_none(select_sql, app_name, user_id, session_id)
                return _row_to_stored_session(row) if row is not None else None
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return None
            raise

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Replace stored session JSON state and touch `update_time`."""
        now_julian = datetime_to_julian(datetime.now(UTC))
        state_json = to_json(state)
        sql = (
            f"UPDATE {self._session_table} SET state = ?, update_time = ? WHERE app_name = ? AND user_id = ? AND id = ?"
        )
        with self._config.provide_session() as driver:
            self._ensure_pragmas(driver)
            driver.execute(sql, state_json, now_julian, app_name, user_id, session_id)
            driver.commit()

    def list_sessions(
        self,
        app_name: str,
        user_id: str | None = None,
        *,
        order_by: "SessionOrderBy" = "update_time",
        descending: bool = True,
        limit: int | None = None,
        offset: int | None = None,
    ) -> "list[StoredSession]":
        """List ADK sessions ordered by timestamp with optional pagination."""
        column, direction, page_limit, page_offset = normalize_session_list_options(order_by, descending, limit, offset)
        if page_limit == 0:
            return []
        sql, params = _build_session_list_query(
            self._session_table, app_name, user_id, column, direction, page_limit, page_offset
        )
        try:
            with self._config.provide_session() as driver:
                self._ensure_pragmas(driver)
                rows = driver.select(sql, *params)
                return [_row_to_stored_session(row) for row in rows]
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return []
            raise

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete an ADK session and cascade-delete associated events."""
        sql = f"DELETE FROM {self._session_table} WHERE app_name = ? AND user_id = ? AND id = ?"
        with self._config.provide_session() as driver:
            self._ensure_pragmas(driver)
            driver.execute(sql, app_name, user_id, session_id)
            driver.commit()

    def append_event(self, event_record: "StoredEvent") -> None:
        """Insert a single ADK event row."""
        sql = (
            f"INSERT INTO {self._events_table} "
            "(id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        with self._config.provide_session() as driver:
            self._ensure_pragmas(driver)
            driver.execute(
                sql,
                event_record["id"],
                event_record["app_name"],
                event_record["user_id"],
                event_record["session_id"],
                event_record.get("invocation_id"),
                datetime_to_julian(event_record["timestamp"]),
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
        """Atomically append an event and update durable session, app, and user state."""
        now_julian = datetime_to_julian(datetime.now(UTC))
        insert_sql = (
            f"INSERT INTO {self._events_table} "
            "(id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        update_sql = (
            f"UPDATE {self._session_table} SET state = ?, update_time = ? "
            "WHERE app_name = ? AND user_id = ? AND id = ? "
            "RETURNING id, app_name, user_id, state, create_time, update_time"
        )
        app_sql = (
            f"INSERT INTO {self._app_state_table} (app_name, state, update_time) VALUES (?, ?, ?) "
            "ON CONFLICT(app_name) DO UPDATE SET state = excluded.state, update_time = excluded.update_time"
        )
        user_sql = (
            f"INSERT INTO {self._user_state_table} (app_name, user_id, state, update_time) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(app_name, user_id) DO UPDATE SET state = excluded.state, update_time = excluded.update_time"
        )
        with self._config.provide_session() as driver:
            self._ensure_pragmas(driver)
            driver.begin()
            try:
                driver.execute(
                    insert_sql,
                    event_record["id"],
                    event_record["app_name"],
                    event_record["user_id"],
                    event_record["session_id"],
                    event_record.get("invocation_id"),
                    datetime_to_julian(event_record["timestamp"]),
                    to_json(event_record["event_data"]),
                )
                row = driver.select_one_or_none(update_sql, to_json(state), now_julian, app_name, user_id, session_id)
                if row is None:
                    msg = f"Session not found while appending event: {session_id}"
                    raise ValueError(msg)
                if app_state is not None:
                    driver.execute(app_sql, app_name, to_json(app_state), now_julian)
                if user_state is not None:
                    driver.execute(user_sql, app_name, user_id, to_json(user_state), now_julian)
                driver.commit()
                return _row_to_stored_session(row)
            except BaseException:
                with contextlib.suppress(Exception):
                    driver.rollback()
                raise

    def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> "list[StoredEvent]":
        """Retrieve events for a session ordered chronologically."""
        where_parts = ["app_name = ?", "user_id = ?", "session_id = ?"]
        params: list[Any] = [app_name, user_id, session_id]
        if after_timestamp is not None:
            where_parts.append("timestamp > ?")
            params.append(datetime_to_julian(after_timestamp))
        limit_clause = ""
        if limit is not None:
            limit_clause = " LIMIT ?"
            params.append(int(limit))
        sql = (
            f"SELECT id, app_name, user_id, session_id, invocation_id, timestamp, event_data "
            f"FROM {self._events_table} WHERE {' AND '.join(where_parts)} ORDER BY timestamp ASC{limit_clause}"
        )
        try:
            with self._config.provide_session() as driver:
                self._ensure_pragmas(driver)
                rows = driver.select(sql, *params)
                return [_row_to_stored_event(row) for row in rows]
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return []
            raise

    def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Delete event rows created strictly before `before`."""
        sql = f"DELETE FROM {self._events_table} WHERE timestamp < ?"
        params: list[Any] = [datetime_to_julian(before)]
        if app_name is not None:
            sql += " AND app_name = ?"
            params.append(app_name)
        try:
            with self._config.provide_session() as driver:
                self._ensure_pragmas(driver)
                res = driver.execute(sql, *params)
                driver.commit()
                return int(res.rows_affected or 0)
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return 0
            raise

    def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete session rows whose `update_time` predates `updated_before`."""
        sql = f"DELETE FROM {self._session_table} WHERE update_time < ?"
        params: list[Any] = [datetime_to_julian(updated_before)]
        if app_name is not None:
            sql += " AND app_name = ?"
            params.append(app_name)
        try:
            with self._config.provide_session() as driver:
                self._ensure_pragmas(driver)
                res = driver.execute(sql, *params)
                driver.commit()
                return int(res.rows_affected or 0)
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return 0
            raise

    def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete user state rows whose `update_time` predates `updated_before`."""
        sql = f"DELETE FROM {self._user_state_table} WHERE update_time < ?"
        params: list[Any] = [datetime_to_julian(updated_before)]
        if app_name is not None:
            sql += " AND app_name = ?"
            params.append(app_name)
        try:
            with self._config.provide_session() as driver:
                self._ensure_pragmas(driver)
                res = driver.execute(sql, *params)
                driver.commit()
                return int(res.rows_affected or 0)
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return 0
            raise

    def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Return app-scoped ADK state dictionary if present."""
        sql = f"SELECT state FROM {self._app_state_table} WHERE app_name = ?"
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
        sql = f"SELECT state FROM {self._user_state_table} WHERE app_name = ? AND user_id = ?"
        try:
            with self._config.provide_session() as driver:
                self._ensure_pragmas(driver)
                raw = driver.select_value_or_none(sql, app_name, user_id)
                return cast("dict[str, Any]", from_json(raw)) if raw else None
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return None
            raise

    def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Upsert app-scoped ADK state using `ON CONFLICT(app_name) DO UPDATE`."""
        sql = (
            f"INSERT INTO {self._app_state_table} (app_name, state, update_time) VALUES (?, ?, ?) "
            "ON CONFLICT(app_name) DO UPDATE SET state = excluded.state, update_time = excluded.update_time"
        )
        with self._config.provide_session() as driver:
            self._ensure_pragmas(driver)
            driver.execute(sql, app_name, to_json(state), datetime_to_julian(datetime.now(UTC)))
            driver.commit()

    def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Upsert user-scoped ADK state using `ON CONFLICT(app_name, user_id) DO UPDATE`."""
        sql = (
            f"INSERT INTO {self._user_state_table} (app_name, user_id, state, update_time) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(app_name, user_id) DO UPDATE SET state = excluded.state, update_time = excluded.update_time"
        )
        with self._config.provide_session() as driver:
            self._ensure_pragmas(driver)
            driver.execute(sql, app_name, user_id, to_json(state), datetime_to_julian(datetime.now(UTC)))
            driver.commit()

    def get_metadata(self, key: str) -> str | None:
        """Fetch an internal ADK metadata string by key."""
        sql = f"SELECT value FROM {self._metadata_table} WHERE key = ?"
        try:
            with self._config.provide_session() as driver:
                self._ensure_pragmas(driver)
                val = driver.select_value_or_none(sql, key)
                return str(val) if val is not None else None
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return None
            raise

    def set_metadata(self, key: str, value: str) -> None:
        """Store or overwrite an internal ADK metadata key-value entry."""
        sql = (
            f"INSERT INTO {self._metadata_table} (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        )
        with self._config.provide_session() as driver:
            self._ensure_pragmas(driver)
            driver.execute(sql, key, value)
            driver.commit()


class SQLiteAsyncADKStore(BaseAsyncADKStore[Any]):
    """Asynchronous SQLite ADK session and event store over Vertebra `SQLiteAsyncDriver`."""

    __slots__ = ("_configured_driver_ids", "_pragma_statements")

    adapter_name = "vertebra_sqlite"

    def __init__(self, config: Any) -> None:
        """Initialize the asynchronous SQLite ADK session store."""
        super().__init__(config)
        self._pragma_statements = _adk_pragma_statements(config)
        self._configured_driver_ids: set[int] = set()

    async def _ensure_pragmas(self, driver: Any) -> None:
        if id(driver) not in self._configured_driver_ids:
            await apply_pragmas_async(driver, self._pragma_statements)
            self._configured_driver_ids.add(id(driver))

    async def _sessions_table_ddl(self) -> str:
        owner_line = f",\n    {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return (
            f"CREATE TABLE IF NOT EXISTS {self._session_table} (\n"
            f"    id TEXT PRIMARY KEY,\n"
            f"    app_name TEXT NOT NULL,\n"
            f"    user_id TEXT NOT NULL{owner_line},\n"
            f"    state TEXT NOT NULL DEFAULT '{{}}',\n"
            f"    create_time REAL NOT NULL,\n"
            f"    update_time REAL NOT NULL\n"
            f");\n"
            f"CREATE INDEX IF NOT EXISTS idx_{self._session_table}_app_user ON {self._session_table}(app_name, user_id);\n"
            f"CREATE INDEX IF NOT EXISTS idx_{self._session_table}_update_time ON {self._session_table}(update_time DESC);"
        )

    async def _events_table_ddl(self) -> str:
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
            f"CREATE INDEX IF NOT EXISTS idx_{self._events_table}_session ON {self._events_table}(app_name, user_id, session_id, timestamp ASC);\n"
            f"CREATE INDEX IF NOT EXISTS idx_{self._events_table}_invocation ON {self._events_table}(invocation_id);\n"
            f"CREATE INDEX IF NOT EXISTS idx_{self._events_table}_timestamp ON {self._events_table}(timestamp ASC);\n"
            f"CREATE INDEX IF NOT EXISTS idx_{self._events_table}_app_timestamp ON {self._events_table}(app_name, timestamp ASC);"
        )

    async def _app_states_table_ddl(self) -> str:
        return (
            f"CREATE TABLE IF NOT EXISTS {self._app_state_table} (\n"
            f"    app_name TEXT PRIMARY KEY,\n"
            f"    state TEXT NOT NULL DEFAULT '{{}}',\n"
            f"    update_time REAL NOT NULL\n"
            f");"
        )

    async def _user_states_table_ddl(self) -> str:
        return (
            f"CREATE TABLE IF NOT EXISTS {self._user_state_table} (\n"
            f"    app_name TEXT NOT NULL,\n"
            f"    user_id TEXT NOT NULL,\n"
            f"    state TEXT NOT NULL DEFAULT '{{}}',\n"
            f"    update_time REAL NOT NULL,\n"
            f"    PRIMARY KEY (app_name, user_id)\n"
            f");"
        )

    async def _metadata_table_ddl(self) -> str:
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
        now_julian = datetime_to_julian(now)
        state_json = to_json(state)
        if self._owner_id_column_name:
            sql = (
                f"INSERT INTO {self._session_table} "
                f"(id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)"
            )
            params: tuple[Any, ...] = (session_id, app_name, user_id, owner_id, state_json, now_julian, now_julian)
        else:
            sql = (
                f"INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time) "
                "VALUES (?, ?, ?, ?, ?, ?)"
            )
            params = (session_id, app_name, user_id, state_json, now_julian, now_julian)
        async with self._config.provide_session() as driver:
            await self._ensure_pragmas(driver)
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
        select_sql = (
            f"SELECT id, app_name, user_id, state, create_time, update_time FROM {self._session_table} "
            "WHERE app_name = ? AND user_id = ? AND id = ?"
        )
        try:
            async with self._config.provide_session() as driver:
                await self._ensure_pragmas(driver)
                if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
                    now_julian = datetime_to_julian(datetime.now(UTC))
                    await driver.execute(
                        f"UPDATE {self._session_table} SET update_time = ? WHERE app_name = ? AND user_id = ? AND id = ?",
                        now_julian,
                        app_name,
                        user_id,
                        session_id,
                    )
                    await driver.commit()
                row = await driver.select_one_or_none(select_sql, app_name, user_id, session_id)
                return _row_to_stored_session(row) if row is not None else None
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return None
            raise

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Replace session JSON state asynchronously."""
        now_julian = datetime_to_julian(datetime.now(UTC))
        state_json = to_json(state)
        sql = (
            f"UPDATE {self._session_table} SET state = ?, update_time = ? WHERE app_name = ? AND user_id = ? AND id = ?"
        )
        async with self._config.provide_session() as driver:
            await self._ensure_pragmas(driver)
            await driver.execute(sql, state_json, now_julian, app_name, user_id, session_id)
            await driver.commit()

    async def list_sessions(
        self,
        app_name: str,
        user_id: str | None = None,
        *,
        order_by: "SessionOrderBy" = "update_time",
        descending: bool = True,
        limit: int | None = None,
        offset: int | None = None,
    ) -> "list[StoredSession]":
        """List ADK sessions asynchronously."""
        column, direction, page_limit, page_offset = normalize_session_list_options(order_by, descending, limit, offset)
        if page_limit == 0:
            return []
        sql, params = _build_session_list_query(
            self._session_table, app_name, user_id, column, direction, page_limit, page_offset
        )
        try:
            async with self._config.provide_session() as driver:
                await self._ensure_pragmas(driver)
                rows = await driver.select(sql, *params)
                return [_row_to_stored_session(row) for row in rows]
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return []
            raise

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete an ADK session asynchronously."""
        sql = f"DELETE FROM {self._session_table} WHERE app_name = ? AND user_id = ? AND id = ?"
        async with self._config.provide_session() as driver:
            await self._ensure_pragmas(driver)
            await driver.execute(sql, app_name, user_id, session_id)
            await driver.commit()

    async def append_event(self, event_record: "StoredEvent") -> None:
        """Append an ADK event row asynchronously."""
        sql = (
            f"INSERT INTO {self._events_table} "
            "(id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        async with self._config.provide_session() as driver:
            await self._ensure_pragmas(driver)
            await driver.execute(
                sql,
                event_record["id"],
                event_record["app_name"],
                event_record["user_id"],
                event_record["session_id"],
                event_record.get("invocation_id"),
                datetime_to_julian(event_record["timestamp"]),
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
        """Atomically append an event and update durable session, app, and user state asynchronously."""
        now_julian = datetime_to_julian(datetime.now(UTC))
        insert_sql = (
            f"INSERT INTO {self._events_table} "
            "(id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        update_sql = (
            f"UPDATE {self._session_table} SET state = ?, update_time = ? "
            "WHERE app_name = ? AND user_id = ? AND id = ? "
            "RETURNING id, app_name, user_id, state, create_time, update_time"
        )
        app_sql = (
            f"INSERT INTO {self._app_state_table} (app_name, state, update_time) VALUES (?, ?, ?) "
            "ON CONFLICT(app_name) DO UPDATE SET state = excluded.state, update_time = excluded.update_time"
        )
        user_sql = (
            f"INSERT INTO {self._user_state_table} (app_name, user_id, state, update_time) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(app_name, user_id) DO UPDATE SET state = excluded.state, update_time = excluded.update_time"
        )
        async with self._config.provide_session() as driver:
            await self._ensure_pragmas(driver)
            await driver.begin()
            try:
                await driver.execute(
                    insert_sql,
                    event_record["id"],
                    event_record["app_name"],
                    event_record["user_id"],
                    event_record["session_id"],
                    event_record.get("invocation_id"),
                    datetime_to_julian(event_record["timestamp"]),
                    to_json(event_record["event_data"]),
                )
                row = await driver.select_one_or_none(
                    update_sql, to_json(state), now_julian, app_name, user_id, session_id
                )
                if row is None:
                    msg = f"Session not found while appending event: {session_id}"
                    raise ValueError(msg)
                if app_state is not None:
                    await driver.execute(app_sql, app_name, to_json(app_state), now_julian)
                if user_state is not None:
                    await driver.execute(user_sql, app_name, user_id, to_json(user_state), now_julian)
                await driver.commit()
                return _row_to_stored_session(row)
            except BaseException:
                with contextlib.suppress(Exception):
                    await driver.rollback()
                raise

    async def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> "list[StoredEvent]":
        """Fetch session events asynchronously in chronological order."""
        where_parts = ["app_name = ?", "user_id = ?", "session_id = ?"]
        params: list[Any] = [app_name, user_id, session_id]
        if after_timestamp is not None:
            where_parts.append("timestamp > ?")
            params.append(datetime_to_julian(after_timestamp))
        limit_clause = ""
        if limit is not None:
            limit_clause = " LIMIT ?"
            params.append(int(limit))
        sql = (
            f"SELECT id, app_name, user_id, session_id, invocation_id, timestamp, event_data "
            f"FROM {self._events_table} WHERE {' AND '.join(where_parts)} ORDER BY timestamp ASC{limit_clause}"
        )
        try:
            async with self._config.provide_session() as driver:
                await self._ensure_pragmas(driver)
                rows = await driver.select(sql, *params)
                return [_row_to_stored_event(row) for row in rows]
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return []
            raise

    async def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Delete expired events asynchronously."""
        sql = f"DELETE FROM {self._events_table} WHERE timestamp < ?"
        params: list[Any] = [datetime_to_julian(before)]
        if app_name is not None:
            sql += " AND app_name = ?"
            params.append(app_name)
        try:
            async with self._config.provide_session() as driver:
                await self._ensure_pragmas(driver)
                res = await driver.execute(sql, *params)
                await driver.commit()
                return int(res.rows_affected or 0)
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return 0
            raise

    async def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete idle sessions asynchronously."""
        sql = f"DELETE FROM {self._session_table} WHERE update_time < ?"
        params: list[Any] = [datetime_to_julian(updated_before)]
        if app_name is not None:
            sql += " AND app_name = ?"
            params.append(app_name)
        try:
            async with self._config.provide_session() as driver:
                await self._ensure_pragmas(driver)
                res = await driver.execute(sql, *params)
                await driver.commit()
                return int(res.rows_affected or 0)
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return 0
            raise

    async def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete idle user state entries asynchronously."""
        sql = f"DELETE FROM {self._user_state_table} WHERE update_time < ?"
        params: list[Any] = [datetime_to_julian(updated_before)]
        if app_name is not None:
            sql += " AND app_name = ?"
            params.append(app_name)
        try:
            async with self._config.provide_session() as driver:
                await self._ensure_pragmas(driver)
                res = await driver.execute(sql, *params)
                await driver.commit()
                return int(res.rows_affected or 0)
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return 0
            raise

    async def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Return app-scoped ADK state asynchronously."""
        sql = f"SELECT state FROM {self._app_state_table} WHERE app_name = ?"
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
        sql = f"SELECT state FROM {self._user_state_table} WHERE app_name = ? AND user_id = ?"
        try:
            async with self._config.provide_session() as driver:
                await self._ensure_pragmas(driver)
                raw = await driver.select_value_or_none(sql, app_name, user_id)
                return cast("dict[str, Any]", from_json(raw)) if raw else None
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return None
            raise

    async def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Upsert app-scoped ADK state asynchronously."""
        sql = (
            f"INSERT INTO {self._app_state_table} (app_name, state, update_time) VALUES (?, ?, ?) "
            "ON CONFLICT(app_name) DO UPDATE SET state = excluded.state, update_time = excluded.update_time"
        )
        async with self._config.provide_session() as driver:
            await self._ensure_pragmas(driver)
            await driver.execute(sql, app_name, to_json(state), datetime_to_julian(datetime.now(UTC)))
            await driver.commit()

    async def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Upsert user-scoped ADK state asynchronously."""
        sql = (
            f"INSERT INTO {self._user_state_table} (app_name, user_id, state, update_time) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(app_name, user_id) DO UPDATE SET state = excluded.state, update_time = excluded.update_time"
        )
        async with self._config.provide_session() as driver:
            await self._ensure_pragmas(driver)
            await driver.execute(sql, app_name, user_id, to_json(state), datetime_to_julian(datetime.now(UTC)))
            await driver.commit()

    async def get_metadata(self, key: str) -> str | None:
        """Fetch an internal ADK metadata string asynchronously."""
        sql = f"SELECT value FROM {self._metadata_table} WHERE key = ?"
        try:
            async with self._config.provide_session() as driver:
                await self._ensure_pragmas(driver)
                val = await driver.select_value_or_none(sql, key)
                return str(val) if val is not None else None
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return None
            raise

    async def set_metadata(self, key: str, value: str) -> None:
        """Upsert an internal ADK metadata string asynchronously."""
        sql = (
            f"INSERT INTO {self._metadata_table} (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        )
        async with self._config.provide_session() as driver:
            await self._ensure_pragmas(driver)
            await driver.execute(sql, key, value)
            await driver.commit()


def _build_sqlite_memory_ddl(
    table_name: str,
    owner_id_column_ddl: str | None,
    use_fts: bool,
    fts_options: Mapping[str, Any],
) -> str:
    owner_line = f",\n    {owner_id_column_ddl}" if owner_id_column_ddl else ""
    fts_clause = ""
    if use_fts:
        opts = format_fts_options(fts_options)
        fts_clause = (
            f"\nCREATE VIRTUAL TABLE IF NOT EXISTS {table_name}_fts USING fts5("
            f"content_text, content={table_name}, content_rowid=rowid{opts});\n"
            f"CREATE TRIGGER IF NOT EXISTS {table_name}_ai AFTER INSERT ON {table_name} BEGIN\n"
            f"    INSERT INTO {table_name}_fts(rowid, content_text) VALUES (new.rowid, new.content_text);\n"
            f"END;\n"
            f"CREATE TRIGGER IF NOT EXISTS {table_name}_ad AFTER DELETE ON {table_name} BEGIN\n"
            f"    INSERT INTO {table_name}_fts({table_name}_fts, rowid, content_text) VALUES('delete', old.rowid, old.content_text);\n"
            f"END;\n"
            f"CREATE TRIGGER IF NOT EXISTS {table_name}_au AFTER UPDATE ON {table_name} BEGIN\n"
            f"    INSERT INTO {table_name}_fts({table_name}_fts, rowid, content_text) VALUES('delete', old.rowid, old.content_text);\n"
            f"    INSERT INTO {table_name}_fts(rowid, content_text) VALUES (new.rowid, new.content_text);\n"
            f"END;"
        )
    return (
        f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        f"    id TEXT PRIMARY KEY,\n"
        f"    session_id TEXT NOT NULL,\n"
        f"    app_name TEXT NOT NULL,\n"
        f"    user_id TEXT NOT NULL,\n"
        f"    scope TEXT NOT NULL DEFAULT 'user',\n"
        f"    event_id TEXT NOT NULL UNIQUE,\n"
        f"    author TEXT{owner_line},\n"
        f"    timestamp REAL NOT NULL,\n"
        f"    content_json TEXT NOT NULL,\n"
        f"    content_text TEXT NOT NULL,\n"
        f"    metadata_json TEXT,\n"
        f"    inserted_at REAL NOT NULL,\n"
        f"    embedding BLOB\n"
        f");\n"
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_app_scope_user_time ON {table_name}(app_name, scope, user_id, timestamp DESC);\n"
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_scope ON {table_name}(app_name, scope);\n"
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_session ON {table_name}(session_id);{fts_clause}"
    )


def _item_embedding(item: Mapping[str, Any]) -> Sequence[float] | None:
    raw = item.get("embedding")
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        return raw
    return None


def _rank_by_embedding(
    records: list["StoredMemory"],
    embedding: Sequence[float] | None,
    limit: int,
) -> list["StoredMemory"]:
    if embedding is not None and records:
        query_vec = [float(x) for x in embedding]
        records.sort(
            key=lambda item: cosine_similarity(query_vec, vec) if (vec := _item_embedding(item)) is not None else -1.0,
            reverse=True,
        )
        return records[:limit]
    return records[:limit]


class SQLiteSyncADKMemoryStore(_BaseSyncADKMemoryStore[Any]):
    """Synchronous SQLite ADK memory store with FTS5 and embedding cosine ranking."""

    __slots__ = ("_configured_driver_ids", "_fts_options", "_pragma_statements")

    adapter_name = "vertebra_sqlite"

    def __init__(self, config: Any) -> None:
        """Initialize the synchronous SQLite ADK memory store."""
        super().__init__(config)
        self._fts_options = validate_fts_options(config)
        self._pragma_statements = _adk_pragma_statements(config)
        self._configured_driver_ids: set[int] = set()

    def _ensure_pragmas(self, driver: Any) -> None:
        if id(driver) not in self._configured_driver_ids:
            apply_pragmas_sync(driver, self._pragma_statements)
            self._configured_driver_ids.add(id(driver))

    def _memory_table_ddl(self) -> str:
        return _build_sqlite_memory_ddl(self._memory_table, self._owner_id_column_ddl, self._use_fts, self._fts_options)

    def _drop_memory_table_sql(self) -> list[str]:
        statements = [f"DROP TABLE IF EXISTS {self._memory_table}"]
        if self._use_fts:
            statements = [
                f"DROP TRIGGER IF EXISTS {self._memory_table}_ai",
                f"DROP TRIGGER IF EXISTS {self._memory_table}_ad",
                f"DROP TRIGGER IF EXISTS {self._memory_table}_au",
                f"DROP TABLE IF EXISTS {self._memory_table}_fts",
                f"DROP TABLE IF EXISTS {self._memory_table}",
            ]
        return statements

    def create_tables(self) -> None:
        """Create the SQLite ADK memory table, indexes, and optional FTS5 virtual table."""
        if not self._enabled:
            return
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return
        with self._config.provide_session() as driver:
            self._ensure_pragmas(driver)
            driver.execute_script(self._memory_table_ddl())
            driver.commit()

    def insert_memory_entries(self, entries: "list[StoredMemory]", owner_id: object | None = None) -> int:
        """Bulk insert memory entries ignoring duplicate `event_id` rows."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0
        if self._owner_id_column_name:
            sql = (
                f"INSERT OR IGNORE INTO {self._memory_table} "
                f"(id, session_id, app_name, user_id, scope, event_id, author, {self._owner_id_column_name}, "
                "timestamp, content_json, content_text, metadata_json, inserted_at, embedding) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )
            params_list: list[tuple[Any, ...]] = [
                (
                    e["id"],
                    e["session_id"],
                    e["app_name"],
                    e["user_id"],
                    e.get("scope", "user"),
                    e["event_id"],
                    e.get("author"),
                    owner_id,
                    datetime_to_julian(e["timestamp"]),
                    to_json(e["content_json"]),
                    e["content_text"],
                    to_json(e["metadata_json"]) if e.get("metadata_json") is not None else None,
                    datetime_to_julian(e["inserted_at"]),
                    pack_embedding(e.get("embedding")),
                )
                for e in entries
            ]
        else:
            sql = (
                f"INSERT OR IGNORE INTO {self._memory_table} "
                "(id, session_id, app_name, user_id, scope, event_id, author, "
                "timestamp, content_json, content_text, metadata_json, inserted_at, embedding) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )
            params_list = [
                (
                    e["id"],
                    e["session_id"],
                    e["app_name"],
                    e["user_id"],
                    e.get("scope", "user"),
                    e["event_id"],
                    e.get("author"),
                    datetime_to_julian(e["timestamp"]),
                    to_json(e["content_json"]),
                    e["content_text"],
                    to_json(e["metadata_json"]) if e.get("metadata_json") is not None else None,
                    datetime_to_julian(e["inserted_at"]),
                    pack_embedding(e.get("embedding")),
                )
                for e in entries
            ]
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
    ) -> "list[StoredMemory]":
        """Search memory entries via FTS5 `MATCH`, SQL `LIKE`, or cosine vector reranking."""
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
            where_scope, scope_params = build_sqlite_scope_clause("m.", app_name, user_id, scope_filter)
            fts_sql = (
                f"SELECT m.* FROM {self._memory_table} m "
                f"JOIN {self._memory_table}_fts fts ON m.rowid = fts.rowid "
                f"WHERE {where_scope} AND fts.content_text MATCH ? ORDER BY m.timestamp DESC LIMIT ?"
            )
            try:
                with self._config.provide_session() as driver:
                    self._ensure_pragmas(driver)
                    rows = driver.select(fts_sql, *scope_params, query, fetch_limit)
            except SQLParsingError as exc:
                if _is_missing_table_error(exc):
                    return []
                raise
        if rows is None:
            where_scope, scope_params = build_sqlite_scope_clause("", app_name, user_id, scope_filter)
            if has_query:
                fallback_sql = (
                    f"SELECT * FROM {self._memory_table} WHERE {where_scope} AND content_text LIKE ? "
                    "ORDER BY timestamp DESC LIMIT ?"
                )
                fallback_params: tuple[Any, ...] = (*scope_params, f"%{query}%", fetch_limit)
            else:
                fallback_sql = (
                    f"SELECT * FROM {self._memory_table} WHERE {where_scope} AND embedding IS NOT NULL "
                    "ORDER BY timestamp DESC LIMIT ?"
                )
                fallback_params = (*scope_params, fetch_limit)
            try:
                with self._config.provide_session() as driver:
                    self._ensure_pragmas(driver)
                    rows = driver.select(fallback_sql, *fallback_params)
            except SQLParsingError as exc:
                if _is_missing_table_error(exc):
                    return []
                raise
        records = [_row_to_stored_memory(row) for row in rows]
        return _rank_by_embedding(records, embedding, effective_limit)

    def delete_entries_by_session(self, session_id: str) -> int:
        """Delete memory entries associated with a given session."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        sql = f"DELETE FROM {self._memory_table} WHERE session_id = ?"
        try:
            with self._config.provide_session() as driver:
                self._ensure_pragmas(driver)
                res = driver.execute(sql, session_id)
                driver.commit()
                return int(res.rows_affected or 0)
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return 0
            raise

    def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
        """Delete memory entries older than `days` days."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        cutoff_julian = (datetime_to_julian(datetime.now(UTC)) or JULIAN_EPOCH) - days
        sql = f"DELETE FROM {self._memory_table} WHERE inserted_at < ?"
        params: list[Any] = [cutoff_julian]
        if app_name is not None:
            sql += " AND app_name = ?"
            params.append(app_name)
        if scope is not None:
            sql += " AND scope = ?"
            params.append(scope)
        try:
            with self._config.provide_session() as driver:
                self._ensure_pragmas(driver)
                res = driver.execute(sql, *params)
                driver.commit()
                return int(res.rows_affected or 0)
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return 0
            raise


class SQLiteAsyncADKMemoryStore(_BaseAsyncADKMemoryStore[Any]):
    """Asynchronous SQLite ADK memory store with FTS5 and embedding cosine ranking."""

    __slots__ = ("_configured_driver_ids", "_fts_options", "_pragma_statements")

    adapter_name = "vertebra_sqlite"

    def __init__(self, config: Any) -> None:
        """Initialize the asynchronous SQLite ADK memory store."""
        super().__init__(config)
        self._fts_options = validate_fts_options(config)
        self._pragma_statements = _adk_pragma_statements(config)
        self._configured_driver_ids: set[int] = set()

    async def _ensure_pragmas(self, driver: Any) -> None:
        if id(driver) not in self._configured_driver_ids:
            await apply_pragmas_async(driver, self._pragma_statements)
            self._configured_driver_ids.add(id(driver))

    async def _memory_table_ddl(self) -> str:
        return _build_sqlite_memory_ddl(self._memory_table, self._owner_id_column_ddl, self._use_fts, self._fts_options)

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
            await driver.execute_script(ddl)
            await driver.commit()

    async def insert_memory_entries(self, entries: "list[StoredMemory]", owner_id: object | None = None) -> int:
        """Bulk insert memory entries asynchronously ignoring duplicate `event_id` rows."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0
        if self._owner_id_column_name:
            sql = (
                f"INSERT OR IGNORE INTO {self._memory_table} "
                f"(id, session_id, app_name, user_id, scope, event_id, author, {self._owner_id_column_name}, "
                "timestamp, content_json, content_text, metadata_json, inserted_at, embedding) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )
            params_list: list[tuple[Any, ...]] = [
                (
                    e["id"],
                    e["session_id"],
                    e["app_name"],
                    e["user_id"],
                    e.get("scope", "user"),
                    e["event_id"],
                    e.get("author"),
                    owner_id,
                    datetime_to_julian(e["timestamp"]),
                    to_json(e["content_json"]),
                    e["content_text"],
                    to_json(e["metadata_json"]) if e.get("metadata_json") is not None else None,
                    datetime_to_julian(e["inserted_at"]),
                    pack_embedding(e.get("embedding")),
                )
                for e in entries
            ]
        else:
            sql = (
                f"INSERT OR IGNORE INTO {self._memory_table} "
                "(id, session_id, app_name, user_id, scope, event_id, author, "
                "timestamp, content_json, content_text, metadata_json, inserted_at, embedding) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )
            params_list = [
                (
                    e["id"],
                    e["session_id"],
                    e["app_name"],
                    e["user_id"],
                    e.get("scope", "user"),
                    e["event_id"],
                    e.get("author"),
                    datetime_to_julian(e["timestamp"]),
                    to_json(e["content_json"]),
                    e["content_text"],
                    to_json(e["metadata_json"]) if e.get("metadata_json") is not None else None,
                    datetime_to_julian(e["inserted_at"]),
                    pack_embedding(e.get("embedding")),
                )
                for e in entries
            ]
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
    ) -> "list[StoredMemory]":
        """Search memory entries asynchronously via FTS5 `MATCH`, SQL `LIKE`, or cosine reranking."""
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
            where_scope, scope_params = build_sqlite_scope_clause("m.", app_name, user_id, scope_filter)
            fts_sql = (
                f"SELECT m.* FROM {self._memory_table} m "
                f"JOIN {self._memory_table}_fts fts ON m.rowid = fts.rowid "
                f"WHERE {where_scope} AND fts.content_text MATCH ? ORDER BY m.timestamp DESC LIMIT ?"
            )
            try:
                async with self._config.provide_session() as driver:
                    await self._ensure_pragmas(driver)
                    rows = await driver.select(fts_sql, *scope_params, query, fetch_limit)
            except SQLParsingError as exc:
                if _is_missing_table_error(exc):
                    return []
                raise
        if rows is None:
            where_scope, scope_params = build_sqlite_scope_clause("", app_name, user_id, scope_filter)
            if has_query:
                fallback_sql = (
                    f"SELECT * FROM {self._memory_table} WHERE {where_scope} AND content_text LIKE ? "
                    "ORDER BY timestamp DESC LIMIT ?"
                )
                fallback_params: tuple[Any, ...] = (*scope_params, f"%{query}%", fetch_limit)
            else:
                fallback_sql = (
                    f"SELECT * FROM {self._memory_table} WHERE {where_scope} AND embedding IS NOT NULL "
                    "ORDER BY timestamp DESC LIMIT ?"
                )
                fallback_params = (*scope_params, fetch_limit)
            try:
                async with self._config.provide_session() as driver:
                    await self._ensure_pragmas(driver)
                    rows = await driver.select(fallback_sql, *fallback_params)
            except SQLParsingError as exc:
                if _is_missing_table_error(exc):
                    return []
                raise
        records = [_row_to_stored_memory(row) for row in rows]
        return _rank_by_embedding(records, embedding, effective_limit)

    async def delete_entries_by_session(self, session_id: str) -> int:
        """Delete memory entries for a session asynchronously."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        sql = f"DELETE FROM {self._memory_table} WHERE session_id = ?"
        try:
            async with self._config.provide_session() as driver:
                await self._ensure_pragmas(driver)
                res = await driver.execute(sql, session_id)
                await driver.commit()
                return int(res.rows_affected or 0)
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return 0
            raise

    async def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
        """Delete memory entries older than `days` days asynchronously."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        cutoff_julian = (datetime_to_julian(datetime.now(UTC)) or JULIAN_EPOCH) - days
        sql = f"DELETE FROM {self._memory_table} WHERE inserted_at < ?"
        params: list[Any] = [cutoff_julian]
        if app_name is not None:
            sql += " AND app_name = ?"
            params.append(app_name)
        if scope is not None:
            sql += " AND scope = ?"
            params.append(scope)
        try:
            async with self._config.provide_session() as driver:
                await self._ensure_pragmas(driver)
                res = await driver.execute(sql, *params)
                await driver.commit()
                return int(res.rows_affected or 0)
        except SQLParsingError as exc:
            if _is_missing_table_error(exc):
                return 0
            raise


class SQLiteSyncADKArtifactStore(_BaseSyncADKArtifactStore[Any]):
    """Synchronous SQLite ADK artifact metadata store supporting `ArtifactReference` records."""

    __slots__ = ("_configured_driver_ids", "_pragma_statements")

    adapter_name = "vertebra_sqlite"

    def __init__(self, config: Any) -> None:
        """Initialize the synchronous SQLite ADK artifact store."""
        super().__init__(config)
        self._pragma_statements = _adk_pragma_statements(config)
        self._configured_driver_ids: set[int] = set()

    def _ensure_pragmas(self, driver: Any) -> None:
        if id(driver) not in self._configured_driver_ids:
            apply_pragmas_sync(driver, self._pragma_statements)
            self._configured_driver_ids.add(id(driver))

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

    def create_table(self) -> None:
        """Create the SQLite artifact metadata table if absent."""
        with self._config.provide_session() as driver:
            self._ensure_pragmas(driver)
            driver.execute_script(self._artifact_table_ddl())
            driver.commit()

    def insert_artifact(self, record: "StoredArtifact") -> None:
        """Insert a `StoredArtifact` metadata record."""
        session_val = record["session_id"] if record["session_id"] is not None else ""
        meta = record.get("custom_metadata")
        sql = (
            f"INSERT INTO {self._artifact_table} "
            "(app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        with self._config.provide_session() as driver:
            self._ensure_pragmas(driver)
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
                datetime_to_julian(record["created_at"]),
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
        """Insert an artifact revision backed by a storage-neutral `ArtifactReference` struct."""
        session_val = session_id if session_id is not None else ""
        now_dt = reference.updated or datetime.now(UTC)
        sql = (
            f"INSERT INTO {self._artifact_table} "
            "(app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, size, updated, signed_url, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        with self._config.provide_session() as driver:
            self._ensure_pragmas(driver)
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
                datetime_to_julian(reference.updated) if reference.updated is not None else None,
                reference.signed_url,
                datetime_to_julian(now_dt),
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
        """Fetch the requested artifact revision as an `ArtifactReference` value object."""
        session_val = session_id if session_id is not None else ""
        with self._config.provide_session() as driver:
            self._ensure_pragmas(driver)
            if version is not None:
                row = driver.select_one_or_none(
                    f"SELECT * FROM {self._artifact_table} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? AND version = ? LIMIT 1",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                    version,
                )
            else:
                row = driver.select_one_or_none(
                    f"SELECT * FROM {self._artifact_table} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? ORDER BY version DESC LIMIT 1",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                )
        return _row_to_artifact_reference(row) if row is not None else None

    def get_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> "StoredArtifact | None":
        """Fetch a specific or latest `StoredArtifact` record."""
        session_val = session_id if session_id is not None else ""
        with self._config.provide_session() as driver:
            self._ensure_pragmas(driver)
            if version is not None:
                row = driver.select_one_or_none(
                    f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at "
                    f"FROM {self._artifact_table} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? AND version = ? LIMIT 1",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                    version,
                )
            else:
                row = driver.select_one_or_none(
                    f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at "
                    f"FROM {self._artifact_table} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? ORDER BY version DESC LIMIT 1",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                )
        return _row_to_stored_artifact(row) if row is not None else None

    def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """List distinct artifact filenames within user or session scope."""
        with self._config.provide_session() as driver:
            self._ensure_pragmas(driver)
            if session_id is not None:
                rows = driver.select(
                    f"SELECT DISTINCT filename FROM {self._artifact_table} WHERE app_name = ? AND user_id = ? AND session_id IN ('', ?) ORDER BY filename ASC",
                    app_name,
                    user_id,
                    session_id,
                )
            else:
                rows = driver.select(
                    f"SELECT DISTINCT filename FROM {self._artifact_table} WHERE app_name = ? AND user_id = ? AND session_id = '' ORDER BY filename ASC",
                    app_name,
                    user_id,
                )
        return [str(row["filename"]) for row in rows]

    def list_artifact_versions(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> "list[StoredArtifact]":
        """List all stored versions for an artifact sorted ascending by version."""
        session_val = session_id if session_id is not None else ""
        with self._config.provide_session() as driver:
            self._ensure_pragmas(driver)
            rows = driver.select(
                f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at "
                f"FROM {self._artifact_table} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? ORDER BY version ASC",
                app_name,
                user_id,
                session_val,
                filename,
            )
        return [_row_to_stored_artifact(row) for row in rows]

    def delete_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> "list[StoredArtifact]":
        """Delete all metadata versions for an artifact and return the removed records."""
        records = self.list_artifact_versions(app_name, user_id, filename, session_id=session_id)
        session_val = session_id if session_id is not None else ""
        with self._config.provide_session() as driver:
            self._ensure_pragmas(driver)
            driver.execute(
                f"DELETE FROM {self._artifact_table} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ?",
                app_name,
                user_id,
                session_val,
                filename,
            )
            driver.commit()
        return records

    def get_next_version(self, app_name: str, user_id: str, filename: str, session_id: str | None = None) -> int:
        """Compute the next zero-based artifact version number."""
        session_val = session_id if session_id is not None else ""
        with self._config.provide_session() as driver:
            self._ensure_pragmas(driver)
            max_ver = driver.select_value_or_none(
                f"SELECT MAX(version) FROM {self._artifact_table} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ?",
                app_name,
                user_id,
                session_val,
                filename,
            )
        return int(max_ver) + 1 if max_ver is not None else 0

    def delete_artifacts_older_than(self, before: datetime, app_name: str | None = None) -> "list[StoredArtifact]":
        """Delete artifact rows recorded strictly before `before` and return them."""
        cutoff = datetime_to_julian(before)
        with self._config.provide_session() as driver:
            self._ensure_pragmas(driver)
            if app_name is not None:
                rows = driver.select(
                    f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at "
                    f"FROM {self._artifact_table} WHERE created_at < ? AND app_name = ? ORDER BY created_at ASC",
                    cutoff,
                    app_name,
                )
                driver.execute(
                    f"DELETE FROM {self._artifact_table} WHERE created_at < ? AND app_name = ?", cutoff, app_name
                )
            else:
                rows = driver.select(
                    f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at "
                    f"FROM {self._artifact_table} WHERE created_at < ? ORDER BY created_at ASC",
                    cutoff,
                )
                driver.execute(f"DELETE FROM {self._artifact_table} WHERE created_at < ?", cutoff)
            driver.commit()
        return [_row_to_stored_artifact(row) for row in rows]


class SQLiteAsyncADKArtifactStore(_BaseAsyncADKArtifactStore[Any]):
    """Asynchronous SQLite ADK artifact metadata store supporting `ArtifactReference` records."""

    __slots__ = ("_configured_driver_ids", "_pragma_statements")

    adapter_name = "vertebra_sqlite"

    def __init__(self, config: Any) -> None:
        """Initialize the asynchronous SQLite ADK artifact store."""
        super().__init__(config)
        self._pragma_statements = _adk_pragma_statements(config)
        self._configured_driver_ids: set[int] = set()

    async def _ensure_pragmas(self, driver: Any) -> None:
        if id(driver) not in self._configured_driver_ids:
            await apply_pragmas_async(driver, self._pragma_statements)
            self._configured_driver_ids.add(id(driver))

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

    async def create_table(self) -> None:
        """Create the SQLite artifact metadata table asynchronously."""
        async with self._config.provide_session() as driver:
            await self._ensure_pragmas(driver)
            await driver.execute_script(self._artifact_table_ddl())
            await driver.commit()

    async def insert_artifact(self, record: "StoredArtifact") -> None:
        """Insert a `StoredArtifact` metadata record asynchronously."""
        session_val = record["session_id"] if record["session_id"] is not None else ""
        meta = record.get("custom_metadata")
        sql = (
            f"INSERT INTO {self._artifact_table} "
            "(app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        async with self._config.provide_session() as driver:
            await self._ensure_pragmas(driver)
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
                datetime_to_julian(record["created_at"]),
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
        """Insert an artifact revision backed by a storage-neutral `ArtifactReference` asynchronously."""
        session_val = session_id if session_id is not None else ""
        now_dt = reference.updated or datetime.now(UTC)
        sql = (
            f"INSERT INTO {self._artifact_table} "
            "(app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, size, updated, signed_url, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        async with self._config.provide_session() as driver:
            await self._ensure_pragmas(driver)
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
                datetime_to_julian(reference.updated) if reference.updated is not None else None,
                reference.signed_url,
                datetime_to_julian(now_dt),
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
        """Fetch an `ArtifactReference` value object asynchronously."""
        session_val = session_id if session_id is not None else ""
        async with self._config.provide_session() as driver:
            await self._ensure_pragmas(driver)
            if version is not None:
                row = await driver.select_one_or_none(
                    f"SELECT * FROM {self._artifact_table} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? AND version = ? LIMIT 1",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                    version,
                )
            else:
                row = await driver.select_one_or_none(
                    f"SELECT * FROM {self._artifact_table} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? ORDER BY version DESC LIMIT 1",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                )
        return _row_to_artifact_reference(row) if row is not None else None

    async def get_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> "StoredArtifact | None":
        """Fetch a specific or latest `StoredArtifact` record asynchronously."""
        session_val = session_id if session_id is not None else ""
        async with self._config.provide_session() as driver:
            await self._ensure_pragmas(driver)
            if version is not None:
                row = await driver.select_one_or_none(
                    f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at "
                    f"FROM {self._artifact_table} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? AND version = ? LIMIT 1",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                    version,
                )
            else:
                row = await driver.select_one_or_none(
                    f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at "
                    f"FROM {self._artifact_table} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? ORDER BY version DESC LIMIT 1",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                )
        return _row_to_stored_artifact(row) if row is not None else None

    async def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """List distinct artifact filenames asynchronously."""
        async with self._config.provide_session() as driver:
            await self._ensure_pragmas(driver)
            if session_id is not None:
                rows = await driver.select(
                    f"SELECT DISTINCT filename FROM {self._artifact_table} WHERE app_name = ? AND user_id = ? AND session_id IN ('', ?) ORDER BY filename ASC",
                    app_name,
                    user_id,
                    session_id,
                )
            else:
                rows = await driver.select(
                    f"SELECT DISTINCT filename FROM {self._artifact_table} WHERE app_name = ? AND user_id = ? AND session_id = '' ORDER BY filename ASC",
                    app_name,
                    user_id,
                )
        return [str(row["filename"]) for row in rows]

    async def list_artifact_versions(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> "list[StoredArtifact]":
        """List all stored versions for an artifact asynchronously."""
        session_val = session_id if session_id is not None else ""
        async with self._config.provide_session() as driver:
            await self._ensure_pragmas(driver)
            rows = await driver.select(
                f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at "
                f"FROM {self._artifact_table} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? ORDER BY version ASC",
                app_name,
                user_id,
                session_val,
                filename,
            )
        return [_row_to_stored_artifact(row) for row in rows]

    async def delete_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> "list[StoredArtifact]":
        """Delete all metadata versions for an artifact asynchronously."""
        records = await self.list_artifact_versions(app_name, user_id, filename, session_id=session_id)
        session_val = session_id if session_id is not None else ""
        async with self._config.provide_session() as driver:
            await self._ensure_pragmas(driver)
            await driver.execute(
                f"DELETE FROM {self._artifact_table} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ?",
                app_name,
                user_id,
                session_val,
                filename,
            )
            await driver.commit()
        return records

    async def get_next_version(self, app_name: str, user_id: str, filename: str, session_id: str | None = None) -> int:
        """Compute the next zero-based artifact version asynchronously."""
        session_val = session_id if session_id is not None else ""
        async with self._config.provide_session() as driver:
            await self._ensure_pragmas(driver)
            max_ver = await driver.select_value_or_none(
                f"SELECT MAX(version) FROM {self._artifact_table} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ?",
                app_name,
                user_id,
                session_val,
                filename,
            )
        return int(max_ver) + 1 if max_ver is not None else 0

    async def delete_artifacts_older_than(
        self, before: datetime, app_name: str | None = None
    ) -> "list[StoredArtifact]":
        """Delete artifact rows recorded before `before` asynchronously."""
        cutoff = datetime_to_julian(before)
        async with self._config.provide_session() as driver:
            await self._ensure_pragmas(driver)
            if app_name is not None:
                rows = await driver.select(
                    f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at "
                    f"FROM {self._artifact_table} WHERE created_at < ? AND app_name = ? ORDER BY created_at ASC",
                    cutoff,
                    app_name,
                )
                await driver.execute(
                    f"DELETE FROM {self._artifact_table} WHERE created_at < ? AND app_name = ?",
                    cutoff,
                    app_name,
                )
            else:
                rows = await driver.select(
                    f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at "
                    f"FROM {self._artifact_table} WHERE created_at < ? ORDER BY created_at ASC",
                    cutoff,
                )
                await driver.execute(f"DELETE FROM {self._artifact_table} WHERE created_at < ?", cutoff)
            await driver.commit()
        return [_row_to_stored_artifact(row) for row in rows]
