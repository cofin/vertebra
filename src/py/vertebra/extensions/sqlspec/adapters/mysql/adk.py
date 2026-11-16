"""MySQL and MariaDB ADK session, event, memory (`FULLTEXT`/MySQL 9 `VECTOR`), and artifact metadata stores."""

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NotRequired, cast

from sqlspec.config import ADKConfig
from sqlspec.extensions.adk import BaseAsyncADKStore, BaseSyncADKStore, normalize_session_list_options
from sqlspec.utils.serializers import from_json, to_json

from vertebra.extensions.sqlspec.adapters.mysql.core import (
    build_mysql_scope_where,
    cosine_similarity,
    format_mysql_vector_literal,
    is_mysql_table_missing,
    mysql_json_dict,
    mysql_json_for_storage,
    mysql_owner_id_column_parts,
    mysql_table_options,
    parse_mysql_vector,
)
from vertebra.storage._store import ArtifactReference

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
                self._vector_index_type: str = str(adk.get("vector_index_type", "json"))
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
            """Fallback synchronous memory store base when `google-adk` is absent."""

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
            """Fallback asynchronous memory store base when `google-adk` is absent."""

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
    "MySQLADKConfig",
    "MySQLAsyncADKArtifactStore",
    "MySQLAsyncADKMemoryStore",
    "MySQLAsyncADKStore",
    "MySQLSyncADKArtifactStore",
    "MySQLSyncADKMemoryStore",
    "MySQLSyncADKStore",
)

_VECTOR_MODES = frozenset({"mysql_vector", "vector", "native", "hnsw"})


class MySQLADKConfig(ADKConfig):
    """MySQL-specific ADK extension configuration keys under `extension_config['adk']`."""

    enable_event_generated_columns: NotRequired[bool]
    """Create MySQL generated columns and indexes for event author and node path JSON fields."""

    enable_covering_indexes: NotRequired[bool]
    """Include `invocation_id` inside ADK event replay indexes."""

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

    enable_vector_index: NotRequired[bool]
    """Enable MySQL 9 native `VECTOR(dims)` column and `DISTANCE(..., 'COSINE')` search."""


def _extract_adk_config(config: Any) -> dict[str, Any]:
    ext = getattr(config, "extension_config", {})
    if not isinstance(ext, Mapping):
        return {}
    adk = ext.get("adk", {})
    return dict(adk) if isinstance(adk, Mapping) else {}


def _use_native_mysql_vector(config: Any, vector_index_type: str) -> bool:
    adk_cfg = _extract_adk_config(config)
    if bool(adk_cfg.get("enable_vector_index", False)):
        return True
    return vector_index_type.strip().lower() in _VECTOR_MODES


def _resolve_json_codecs(config: Any) -> tuple[Any, Any]:
    features = getattr(config, "driver_features", None)
    if not isinstance(features, Mapping):
        return to_json, from_json
    serializer = features.get("json_serializer")
    deserializer = features.get("json_deserializer")
    return (serializer if callable(serializer) else to_json, deserializer if callable(deserializer) else from_json)


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _ensure_utc(value: Any) -> datetime:
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value)
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    return datetime.now(UTC)


def _sessions_ddl(session_table: str, owner_id_ddl: str | None, table_options: str = "") -> str:
    owner_line = ""
    fk_clause = ""
    if owner_id_ddl:
        col_def, fk_def = mysql_owner_id_column_parts(owner_id_ddl)
        owner_line = f"\n    {col_def},"
        if fk_def:
            fk_clause = f",\n    {fk_def}"
    return (
        f"CREATE TABLE IF NOT EXISTS {session_table} (\n"
        "    id VARCHAR(128) PRIMARY KEY,\n"
        "    app_name VARCHAR(128) NOT NULL,\n"
        f"    user_id VARCHAR(128) NOT NULL,{owner_line}\n"
        "    state JSON NOT NULL,\n"
        "    create_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\n"
        "    update_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n"
        f"    INDEX idx_{session_table}_app_user (app_name, user_id),\n"
        f"    INDEX idx_{session_table}_update_time (update_time DESC){fk_clause}\n"
        f") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{table_options};"
    )


def _events_ddl(events_table: str, session_table: str, adk_cfg: Mapping[str, Any]) -> str:
    generated_cols = ""
    generated_idxs = ""
    if adk_cfg.get("enable_event_generated_columns", False):
        generated_cols = (
            ",\n"
            "    author_gc VARCHAR(256) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(event_data, '$.author'))) STORED,\n"
            "    node_path_gc VARCHAR(512) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(event_data, '$.node_info.path'))) STORED"
        )
        generated_idxs = (
            f",\n    INDEX idx_{events_table}_author_gc (session_id, author_gc, timestamp ASC),\n"
            f"    INDEX idx_{events_table}_node_path_gc (session_id, node_path_gc, timestamp ASC)"
        )
    covering = ", invocation_id" if adk_cfg.get("enable_covering_indexes", False) else ""
    table_options = mysql_table_options(adk_cfg, "events_table_options")
    return (
        f"CREATE TABLE IF NOT EXISTS {events_table} (\n"
        "    id VARCHAR(128) PRIMARY KEY,\n"
        "    app_name VARCHAR(128) NOT NULL,\n"
        "    user_id VARCHAR(128) NOT NULL,\n"
        "    session_id VARCHAR(128) NOT NULL,\n"
        "    invocation_id VARCHAR(256) NOT NULL,\n"
        "    timestamp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\n"
        f"    event_data JSON NOT NULL{generated_cols},\n"
        f"    FOREIGN KEY (session_id) REFERENCES {session_table}(id) ON DELETE CASCADE,\n"
        f"    INDEX idx_{events_table}_scope (app_name, user_id, session_id, timestamp ASC{covering}),\n"
        f"    INDEX idx_{events_table}_session (session_id, timestamp ASC{covering}),\n"
        f"    INDEX idx_{events_table}_app_timestamp (app_name, timestamp ASC){generated_idxs}\n"
        f") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{table_options};"
    )


def _app_state_ddl(app_state_table: str, table_options: str = "") -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {app_state_table} (\n"
        "    app_name VARCHAR(128) PRIMARY KEY,\n"
        "    state JSON NOT NULL,\n"
        "    update_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)\n"
        f") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{table_options};"
    )


def _user_state_ddl(user_state_table: str, table_options: str = "") -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {user_state_table} (\n"
        "    app_name VARCHAR(128) NOT NULL,\n"
        "    user_id VARCHAR(128) NOT NULL,\n"
        "    state JSON NOT NULL,\n"
        "    update_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n"
        "    PRIMARY KEY (app_name, user_id)\n"
        f") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{table_options};"
    )


def _metadata_ddl(metadata_table: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {metadata_table} (\n"
        "    `key` VARCHAR(128) PRIMARY KEY,\n"
        "    value VARCHAR(512) NOT NULL\n"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
    )


def _upsert_app_state_sql(app_state_table: str) -> str:
    return (
        f"INSERT INTO {app_state_table} (app_name, state, update_time) "
        "VALUES (?, ?, UTC_TIMESTAMP(6)) "
        "ON DUPLICATE KEY UPDATE state = VALUES(state), update_time = UTC_TIMESTAMP(6)"
    )


def _upsert_user_state_sql(user_state_table: str) -> str:
    return (
        f"INSERT INTO {user_state_table} (app_name, user_id, state, update_time) "
        "VALUES (?, ?, ?, UTC_TIMESTAMP(6)) "
        "ON DUPLICATE KEY UPDATE state = VALUES(state), update_time = UTC_TIMESTAMP(6)"
    )


def _row_to_session(row: Mapping[str, Any], *, deserializer: Any = from_json) -> "StoredSession":
    return {
        "id": str(row["id"]),
        "app_name": str(row["app_name"]),
        "user_id": str(row["user_id"]),
        "state": mysql_json_dict(row.get("state"), deserializer=deserializer),
        "create_time": _ensure_utc(row["create_time"]),
        "update_time": _ensure_utc(row["update_time"]),
    }


def _row_to_event(row: Mapping[str, Any], *, deserializer: Any = from_json) -> "StoredEvent":
    return {
        "id": str(row["id"]),
        "app_name": str(row["app_name"]),
        "user_id": str(row["user_id"]),
        "session_id": str(row["session_id"]),
        "invocation_id": str(row["invocation_id"]),
        "timestamp": _ensure_utc(row["timestamp"]),
        "event_data": mysql_json_dict(row.get("event_data"), deserializer=deserializer),
    }


def _build_session_list_query(
    session_table: str,
    app_name: str,
    user_id: str | None,
    column: str,
    direction: str,
    page_limit: int | None,
    page_offset: int,
) -> tuple[str, tuple[Any, ...]]:
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
        f"SELECT id, app_name, user_id, state, create_time, update_time FROM {session_table} "
        f"WHERE {where_clause} ORDER BY {column} {direction}, id {direction}{page_clause}"
    )
    return sql, tuple(params)


class MySQLSyncADKStore(BaseSyncADKStore[Any]):
    """Synchronous MySQL/MariaDB ADK session, event, state, and metadata store using `?` binds."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    adapter_name = "vertebra_mysql"

    def __init__(self, config: Any) -> None:
        """Initialize the synchronous MySQL ADK session/event store."""
        super().__init__(config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    def _sessions_table_ddl(self) -> str:
        adk_cfg = _extract_adk_config(self._config)
        return _sessions_ddl(
            self._session_table,
            self._owner_id_column_ddl,
            mysql_table_options(adk_cfg, "session_table_options"),
        )

    def _events_table_ddl(self) -> str:
        return _events_ddl(self._events_table, self._session_table, _extract_adk_config(self._config))

    def _app_states_table_ddl(self) -> str:
        adk_cfg = _extract_adk_config(self._config)
        return _app_state_ddl(self._app_state_table, mysql_table_options(adk_cfg, "app_state_table_options"))

    def _user_states_table_ddl(self) -> str:
        adk_cfg = _extract_adk_config(self._config)
        return _user_state_ddl(self._user_state_table, mysql_table_options(adk_cfg, "user_state_table_options"))

    def _metadata_table_ddl(self) -> str:
        return _metadata_ddl(self._metadata_table)

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
        self.reconcile_schema(assume_existing=True)

    def create_session(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
        state: dict[str, Any],
        owner_id: Any | None = None,
    ) -> "StoredSession":
        """Insert a new ADK session row and return its persisted record."""
        state_json = self._json_serializer(state)
        if self._owner_id_column_name:
            sql = (
                f"INSERT INTO {self._session_table} "
                f"(id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time) "
                "VALUES (?, ?, ?, ?, ?, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))"
            )
            params: tuple[Any, ...] = (session_id, app_name, user_id, owner_id, state_json)
        else:
            sql = (
                f"INSERT INTO {self._session_table} "
                "(id, app_name, user_id, state, create_time, update_time) "
                "VALUES (?, ?, ?, ?, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))"
            )
            params = (session_id, app_name, user_id, state_json)
        with self._config.provide_session() as driver:
            driver.execute(sql, *params)
            driver.commit()
        session = self.get_session(app_name, user_id, session_id)
        if session is None:
            msg = f"Failed to reload newly created MySQL ADK session {session_id}"
            raise RuntimeError(msg)
        return session

    def get_session(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        *,
        renew_for: int | timedelta | None = None,
    ) -> "StoredSession | None":
        """Fetch an ADK session by composite key and optionally touch `update_time`."""
        try:
            with self._config.provide_session() as driver:
                if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
                    driver.execute(
                        f"UPDATE {self._session_table} SET update_time = UTC_TIMESTAMP(6) "
                        "WHERE app_name = ? AND user_id = ? AND id = ?",
                        app_name,
                        user_id,
                        session_id,
                    )
                    driver.commit()
                row = driver.select_one_or_none(
                    f"SELECT id, app_name, user_id, state, create_time, update_time FROM {self._session_table} "
                    "WHERE app_name = ? AND user_id = ? AND id = ?",
                    app_name,
                    user_id,
                    session_id,
                )
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return None
            raise
        return _row_to_session(row, deserializer=self._json_deserializer) if row is not None else None

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Replace stored session JSON state."""
        with self._config.provide_session() as driver:
            driver.execute(
                f"UPDATE {self._session_table} SET state = ?, update_time = UTC_TIMESTAMP(6) "
                "WHERE app_name = ? AND user_id = ? AND id = ?",
                self._json_serializer(state),
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
        order_by: "SessionOrderBy" = "update_time",
        descending: bool = True,
        limit: int | None = None,
        offset: int | None = None,
    ) -> "list[StoredSession]":
        """List ADK sessions scoped by app and optional user."""
        column, direction, page_limit, page_offset = normalize_session_list_options(order_by, descending, limit, offset)
        if page_limit == 0:
            return []
        sql, params = _build_session_list_query(
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
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return []
            raise
        return [_row_to_session(row, deserializer=self._json_deserializer) for row in rows]

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete an ADK session row (cascading to child event rows)."""
        with self._config.provide_session() as driver:
            driver.execute(
                f"DELETE FROM {self._session_table} WHERE app_name = ? AND user_id = ? AND id = ?",
                app_name,
                user_id,
                session_id,
            )
            driver.commit()

    def append_event(self, event_record: "StoredEvent") -> None:
        """Insert a single ADK event record."""
        sql = (
            f"INSERT INTO {self._events_table} "
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
                _naive_utc(event_record["timestamp"]),
                mysql_json_for_storage(event_record["event_data"], serializer=self._json_serializer),
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
        """Atomically append an event and update session, app, and user state within one transaction."""
        with self._config.provide_session() as driver:
            try:
                driver.begin()
                driver.execute(
                    f"UPDATE {self._session_table} SET state = ?, update_time = UTC_TIMESTAMP(6) "
                    "WHERE app_name = ? AND user_id = ? AND id = ?",
                    self._json_serializer(state),
                    app_name,
                    user_id,
                    session_id,
                )
                row = driver.select_one_or_none(
                    f"SELECT id, app_name, user_id, state, create_time, update_time FROM {self._session_table} "
                    "WHERE app_name = ? AND user_id = ? AND id = ?",
                    app_name,
                    user_id,
                    session_id,
                )
                if row is None:
                    msg = f"Session {session_id} not found during append_event_and_update_state."
                    raise ValueError(msg)
                driver.execute(
                    f"INSERT INTO {self._events_table} "
                    "(id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    event_record["id"],
                    app_name,
                    user_id,
                    session_id,
                    event_record["invocation_id"],
                    _naive_utc(event_record["timestamp"]),
                    mysql_json_for_storage(event_record["event_data"], serializer=self._json_serializer),
                )
                if app_state is not None:
                    driver.execute(
                        _upsert_app_state_sql(self._app_state_table), app_name, self._json_serializer(app_state)
                    )
                if user_state is not None:
                    driver.execute(
                        _upsert_user_state_sql(self._user_state_table),
                        app_name,
                        user_id,
                        self._json_serializer(user_state),
                    )
                driver.commit()
            except Exception:
                driver.rollback()
                raise
        return _row_to_session(row, deserializer=self._json_deserializer)

    def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> "list[StoredEvent]":
        """Fetch ordered events for an ADK session."""
        if limit == 0:
            return []
        clauses = ["app_name = ?", "user_id = ?", "session_id = ?"]
        params: list[Any] = [app_name, user_id, session_id]
        if after_timestamp is not None:
            clauses.append("timestamp > ?")
            params.append(_naive_utc(after_timestamp))
        limit_sql = ""
        if limit is not None:
            limit_sql = " LIMIT ?"
            params.append(limit)
        sql = (
            f"SELECT id, app_name, user_id, session_id, invocation_id, timestamp, event_data "
            f"FROM {self._events_table} WHERE {' AND '.join(clauses)} ORDER BY timestamp ASC{limit_sql}"
        )
        try:
            with self._config.provide_session() as driver:
                rows = driver.select(sql, *params)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return []
            raise
        return [_row_to_event(row, deserializer=self._json_deserializer) for row in rows]

    def _delete_before(self, table_name: str, column_name: str, threshold: datetime, app_name: str | None) -> int:
        sql = f"DELETE FROM {table_name} WHERE {column_name} < ?"
        params: list[Any] = [_naive_utc(threshold)]
        if app_name is not None:
            sql = f"{sql} AND app_name = ?"
            params.append(app_name)
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
        """Delete events older than `before`."""
        return self._delete_before(self._events_table, "timestamp", before, app_name)

    def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete sessions older than `updated_before`."""
        return self._delete_before(self._session_table, "update_time", updated_before, app_name)

    def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete user state rows older than `updated_before`."""
        return self._delete_before(self._user_state_table, "update_time", updated_before, app_name)

    def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Return app-scoped ADK state for `app_name`."""
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(
                    f"SELECT state FROM {self._app_state_table} WHERE app_name = ?",
                    app_name,
                )
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return None
            raise
        return mysql_json_dict(row["state"], deserializer=self._json_deserializer) if row is not None else None

    def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Return user-scoped ADK state for `(app_name, user_id)`."""
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(
                    f"SELECT state FROM {self._user_state_table} WHERE app_name = ? AND user_id = ?",
                    app_name,
                    user_id,
                )
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return None
            raise
        return mysql_json_dict(row["state"], deserializer=self._json_deserializer) if row is not None else None

    def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Upsert app-scoped ADK state via `ON DUPLICATE KEY UPDATE`."""
        with self._config.provide_session() as driver:
            driver.execute(_upsert_app_state_sql(self._app_state_table), app_name, self._json_serializer(state))
            driver.commit()

    def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Upsert user-scoped ADK state via `ON DUPLICATE KEY UPDATE`."""
        with self._config.provide_session() as driver:
            driver.execute(
                _upsert_user_state_sql(self._user_state_table),
                app_name,
                user_id,
                self._json_serializer(state),
            )
            driver.commit()

    def get_metadata(self, key: str) -> str | None:
        """Return an ADK metadata table value by key."""
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(
                    f"SELECT value FROM {self._metadata_table} WHERE `key` = ?",
                    key,
                )
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return None
            raise
        return str(row["value"]) if row is not None and row.get("value") is not None else None

    def set_metadata(self, key: str, value: str) -> None:
        """Upsert an ADK metadata key-value entry via `ON DUPLICATE KEY UPDATE`."""
        sql = f"INSERT INTO {self._metadata_table} (`key`, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value)"
        with self._config.provide_session() as driver:
            driver.execute(sql, key, value)
            driver.commit()


class MySQLAsyncADKStore(BaseAsyncADKStore[Any]):
    """Asynchronous MySQL/MariaDB ADK session, event, state, and metadata store using `?` binds."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    adapter_name = "vertebra_mysql"

    def __init__(self, config: Any) -> None:
        """Initialize the asynchronous MySQL ADK session/event store."""
        super().__init__(config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    async def _sessions_table_ddl(self) -> str:
        adk_cfg = _extract_adk_config(self._config)
        return _sessions_ddl(
            self._session_table,
            self._owner_id_column_ddl,
            mysql_table_options(adk_cfg, "session_table_options"),
        )

    async def _events_table_ddl(self) -> str:
        return _events_ddl(self._events_table, self._session_table, _extract_adk_config(self._config))

    async def _app_states_table_ddl(self) -> str:
        adk_cfg = _extract_adk_config(self._config)
        return _app_state_ddl(self._app_state_table, mysql_table_options(adk_cfg, "app_state_table_options"))

    async def _user_states_table_ddl(self) -> str:
        adk_cfg = _extract_adk_config(self._config)
        return _user_state_ddl(self._user_state_table, mysql_table_options(adk_cfg, "user_state_table_options"))

    async def _metadata_table_ddl(self) -> str:
        return _metadata_ddl(self._metadata_table)

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
        await self.reconcile_schema(assume_existing=True)

    async def create_session(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
        state: dict[str, Any],
        owner_id: Any | None = None,
    ) -> "StoredSession":
        """Insert a new ADK session row asynchronously."""
        state_json = self._json_serializer(state)
        if self._owner_id_column_name:
            sql = (
                f"INSERT INTO {self._session_table} "
                f"(id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time) "
                "VALUES (?, ?, ?, ?, ?, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))"
            )
            params: tuple[Any, ...] = (session_id, app_name, user_id, owner_id, state_json)
        else:
            sql = (
                f"INSERT INTO {self._session_table} "
                "(id, app_name, user_id, state, create_time, update_time) "
                "VALUES (?, ?, ?, ?, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))"
            )
            params = (session_id, app_name, user_id, state_json)
        async with self._config.provide_session() as driver:
            await driver.execute(sql, *params)
            await driver.commit()
        session = await self.get_session(app_name, user_id, session_id)
        if session is None:
            msg = f"Failed to reload newly created MySQL ADK session {session_id}"
            raise RuntimeError(msg)
        return session

    async def get_session(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        *,
        renew_for: int | timedelta | None = None,
    ) -> "StoredSession | None":
        """Fetch an ADK session asynchronously and optionally renew `update_time`."""
        try:
            async with self._config.provide_session() as driver:
                if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
                    await driver.execute(
                        f"UPDATE {self._session_table} SET update_time = UTC_TIMESTAMP(6) "
                        "WHERE app_name = ? AND user_id = ? AND id = ?",
                        app_name,
                        user_id,
                        session_id,
                    )
                    await driver.commit()
                row = await driver.select_one_or_none(
                    f"SELECT id, app_name, user_id, state, create_time, update_time FROM {self._session_table} "
                    "WHERE app_name = ? AND user_id = ? AND id = ?",
                    app_name,
                    user_id,
                    session_id,
                )
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return None
            raise
        return _row_to_session(row, deserializer=self._json_deserializer) if row is not None else None

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Update session JSON state asynchronously."""
        async with self._config.provide_session() as driver:
            await driver.execute(
                f"UPDATE {self._session_table} SET state = ?, update_time = UTC_TIMESTAMP(6) "
                "WHERE app_name = ? AND user_id = ? AND id = ?",
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
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return []
            raise
        return [_row_to_session(row, deserializer=self._json_deserializer) for row in rows]

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete an ADK session asynchronously."""
        async with self._config.provide_session() as driver:
            await driver.execute(
                f"DELETE FROM {self._session_table} WHERE app_name = ? AND user_id = ? AND id = ?",
                app_name,
                user_id,
                session_id,
            )
            await driver.commit()

    async def append_event(self, event_record: "StoredEvent") -> None:
        """Append an ADK event record asynchronously."""
        sql = (
            f"INSERT INTO {self._events_table} "
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
                _naive_utc(event_record["timestamp"]),
                mysql_json_for_storage(event_record["event_data"], serializer=self._json_serializer),
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
        """Atomically append an event and update session and scoped state asynchronously."""
        async with self._config.provide_session() as driver:
            try:
                await driver.begin()
                await driver.execute(
                    f"UPDATE {self._session_table} SET state = ?, update_time = UTC_TIMESTAMP(6) "
                    "WHERE app_name = ? AND user_id = ? AND id = ?",
                    self._json_serializer(state),
                    app_name,
                    user_id,
                    session_id,
                )
                row = await driver.select_one_or_none(
                    f"SELECT id, app_name, user_id, state, create_time, update_time FROM {self._session_table} "
                    "WHERE app_name = ? AND user_id = ? AND id = ?",
                    app_name,
                    user_id,
                    session_id,
                )
                if row is None:
                    msg = f"Session {session_id} not found during append_event_and_update_state."
                    raise ValueError(msg)
                await driver.execute(
                    f"INSERT INTO {self._events_table} "
                    "(id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    event_record["id"],
                    app_name,
                    user_id,
                    session_id,
                    event_record["invocation_id"],
                    _naive_utc(event_record["timestamp"]),
                    mysql_json_for_storage(event_record["event_data"], serializer=self._json_serializer),
                )
                if app_state is not None:
                    await driver.execute(
                        _upsert_app_state_sql(self._app_state_table),
                        app_name,
                        self._json_serializer(app_state),
                    )
                if user_state is not None:
                    await driver.execute(
                        _upsert_user_state_sql(self._user_state_table),
                        app_name,
                        user_id,
                        self._json_serializer(user_state),
                    )
                await driver.commit()
            except Exception:
                await driver.rollback()
                raise
        return _row_to_session(row, deserializer=self._json_deserializer)

    async def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> "list[StoredEvent]":
        """Fetch ordered events for an ADK session asynchronously."""
        if limit == 0:
            return []
        clauses = ["app_name = ?", "user_id = ?", "session_id = ?"]
        params: list[Any] = [app_name, user_id, session_id]
        if after_timestamp is not None:
            clauses.append("timestamp > ?")
            params.append(_naive_utc(after_timestamp))
        limit_sql = ""
        if limit is not None:
            limit_sql = " LIMIT ?"
            params.append(limit)
        sql = (
            f"SELECT id, app_name, user_id, session_id, invocation_id, timestamp, event_data "
            f"FROM {self._events_table} WHERE {' AND '.join(clauses)} ORDER BY timestamp ASC{limit_sql}"
        )
        try:
            async with self._config.provide_session() as driver:
                rows = await driver.select(sql, *params)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return []
            raise
        return [_row_to_event(row, deserializer=self._json_deserializer) for row in rows]

    async def _delete_before(self, table_name: str, column_name: str, threshold: datetime, app_name: str | None) -> int:
        sql = f"DELETE FROM {table_name} WHERE {column_name} < ?"
        params: list[Any] = [_naive_utc(threshold)]
        if app_name is not None:
            sql = f"{sql} AND app_name = ?"
            params.append(app_name)
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
        """Delete events older than `before` asynchronously."""
        return await self._delete_before(self._events_table, "timestamp", before, app_name)

    async def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete sessions older than `updated_before` asynchronously."""
        return await self._delete_before(self._session_table, "update_time", updated_before, app_name)

    async def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete user state rows older than `updated_before` asynchronously."""
        return await self._delete_before(self._user_state_table, "update_time", updated_before, app_name)

    async def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Return app-scoped ADK state asynchronously."""
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(
                    f"SELECT state FROM {self._app_state_table} WHERE app_name = ?",
                    app_name,
                )
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return None
            raise
        return mysql_json_dict(row["state"], deserializer=self._json_deserializer) if row is not None else None

    async def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Return user-scoped ADK state asynchronously."""
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(
                    f"SELECT state FROM {self._user_state_table} WHERE app_name = ? AND user_id = ?",
                    app_name,
                    user_id,
                )
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return None
            raise
        return mysql_json_dict(row["state"], deserializer=self._json_deserializer) if row is not None else None

    async def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Upsert app-scoped ADK state asynchronously."""
        async with self._config.provide_session() as driver:
            await driver.execute(
                _upsert_app_state_sql(self._app_state_table),
                app_name,
                self._json_serializer(state),
            )
            await driver.commit()

    async def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Upsert user-scoped ADK state asynchronously."""
        async with self._config.provide_session() as driver:
            await driver.execute(
                _upsert_user_state_sql(self._user_state_table),
                app_name,
                user_id,
                self._json_serializer(state),
            )
            await driver.commit()

    async def get_metadata(self, key: str) -> str | None:
        """Return an ADK metadata table value asynchronously."""
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(
                    f"SELECT value FROM {self._metadata_table} WHERE `key` = ?",
                    key,
                )
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return None
            raise
        return str(row["value"]) if row is not None and row.get("value") is not None else None

    async def set_metadata(self, key: str, value: str) -> None:
        """Upsert an ADK metadata key-value entry asynchronously."""
        sql = f"INSERT INTO {self._metadata_table} (`key`, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value)"
        async with self._config.provide_session() as driver:
            await driver.execute(sql, key, value)
            await driver.commit()


def _build_mysql_memory_ddl(
    table_name: str,
    owner_id_ddl: str | None,
    use_fts: bool,
    native_vector: bool,
    vector_dimensions: int,
    table_options: str,
) -> str:
    owner_line = ""
    fk_clause = ""
    if owner_id_ddl:
        col_def, fk_def = mysql_owner_id_column_parts(owner_id_ddl)
        owner_line = f"\n    {col_def},"
        if fk_def:
            fk_clause = f",\n    {fk_def}"
    fts_clause = f",\n    FULLTEXT INDEX idx_{table_name}_fts (content_text)" if use_fts else ""
    embedding_col = f"VECTOR({max(1, int(vector_dimensions))})" if native_vector else "JSON"
    return (
        f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        "    id VARCHAR(128) PRIMARY KEY,\n"
        "    session_id VARCHAR(128) NOT NULL,\n"
        "    app_name VARCHAR(128) NOT NULL,\n"
        "    user_id VARCHAR(128) NOT NULL,\n"
        "    scope VARCHAR(16) NOT NULL DEFAULT 'user',\n"
        "    event_id VARCHAR(128) NOT NULL UNIQUE,\n"
        f"    author VARCHAR(256),{owner_line}\n"
        "    timestamp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\n"
        "    content_json JSON NOT NULL,\n"
        "    content_text TEXT NOT NULL,\n"
        "    metadata_json JSON,\n"
        "    inserted_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\n"
        f"    embedding {embedding_col},\n"
        f"    INDEX idx_{table_name}_app_scope_user_time (app_name, scope, user_id, timestamp),\n"
        f"    INDEX idx_{table_name}_scope (app_name, scope),\n"
        f"    INDEX idx_{table_name}_session (session_id){fts_clause}{fk_clause}\n"
        f") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{table_options};"
    )


def _item_embedding(item: Mapping[str, Any]) -> Sequence[float] | None:
    raw = item.get("embedding")
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        return raw
    return None


def _rank_memory_records(
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


def _memory_row_to_record(row: Mapping[str, Any], *, deserializer: Any = from_json) -> "StoredMemory":
    metadata_val = row.get("metadata_json")
    return {
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
        "metadata_json": mysql_json_dict(metadata_val, deserializer=deserializer) if metadata_val is not None else None,
        "inserted_at": _ensure_utc(row["inserted_at"]),
        "embedding": parse_mysql_vector(row.get("embedding")),
    }


def _build_memory_insert_statement(
    table_name: str,
    owner_col: str | None,
    entries: "list[StoredMemory]",
    owner_id: object | None,
    *,
    serializer: Any,
    native_vector: bool,
) -> tuple[str, list[tuple[Any, ...]]]:
    emb_expr = "STRING_TO_VECTOR(?)" if native_vector else "?"
    if owner_col:
        sql = (
            f"INSERT IGNORE INTO {table_name} "
            f"(id, session_id, app_name, user_id, scope, event_id, author, {owner_col}, "
            "timestamp, content_json, content_text, metadata_json, inserted_at, embedding) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, {emb_expr})"
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
                _naive_utc(e["timestamp"]),
                serializer(e["content_json"]),
                e["content_text"],
                serializer(e["metadata_json"]) if e.get("metadata_json") is not None else None,
                _naive_utc(e["inserted_at"]),
                format_mysql_vector_literal(e.get("embedding")),
            )
            for e in entries
        ]
        return sql, params_list
    sql = (
        f"INSERT IGNORE INTO {table_name} "
        "(id, session_id, app_name, user_id, scope, event_id, author, "
        "timestamp, content_json, content_text, metadata_json, inserted_at, embedding) "
        f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, {emb_expr})"
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
            _naive_utc(e["timestamp"]),
            serializer(e["content_json"]),
            e["content_text"],
            serializer(e["metadata_json"]) if e.get("metadata_json") is not None else None,
            _naive_utc(e["inserted_at"]),
            format_mysql_vector_literal(e.get("embedding")),
        )
        for e in entries
    ]
    return sql, params_list


def _build_memory_search_query(
    table_name: str,
    query: str,
    app_name: str,
    user_id: str,
    limit_value: int,
    scope_filter: Literal["all", "user", "app"],
    embedding: Sequence[float] | None,
    *,
    use_fts: bool,
    native_vector: bool,
) -> tuple[str, tuple[Any, ...]]:
    where_scope, scope_params = build_mysql_scope_where(app_name, user_id, scope_filter)
    if native_vector and embedding is not None:
        vec_literal = format_mysql_vector_literal(embedding)
        if query:
            if use_fts:
                sql = (
                    f"SELECT id, session_id, app_name, user_id, scope, event_id, author, timestamp, "
                    "content_json, content_text, metadata_json, inserted_at, NULL AS embedding, "
                    "DISTANCE(embedding, STRING_TO_VECTOR(?), 'COSINE') AS vector_distance "
                    f"FROM {table_name} WHERE {where_scope} AND embedding IS NOT NULL "
                    "AND MATCH(content_text) AGAINST (? IN NATURAL LANGUAGE MODE) "
                    "ORDER BY vector_distance ASC LIMIT ?"
                )
                return sql, (vec_literal, *scope_params, query, limit_value)
            sql = (
                f"SELECT id, session_id, app_name, user_id, scope, event_id, author, timestamp, "
                "content_json, content_text, metadata_json, inserted_at, NULL AS embedding, "
                "DISTANCE(embedding, STRING_TO_VECTOR(?), 'COSINE') AS vector_distance "
                f"FROM {table_name} WHERE {where_scope} AND embedding IS NOT NULL AND content_text LIKE ? "
                "ORDER BY vector_distance ASC LIMIT ?"
            )
            return sql, (vec_literal, *scope_params, f"%{query}%", limit_value)
        sql = (
            f"SELECT id, session_id, app_name, user_id, scope, event_id, author, timestamp, "
            "content_json, content_text, metadata_json, inserted_at, NULL AS embedding, "
            "DISTANCE(embedding, STRING_TO_VECTOR(?), 'COSINE') AS vector_distance "
            f"FROM {table_name} WHERE {where_scope} AND embedding IS NOT NULL "
            "ORDER BY vector_distance ASC LIMIT ?"
        )
        return sql, (vec_literal, *scope_params, limit_value)

    candidate_limit = max(limit_value * 5, limit_value) if embedding is not None else limit_value
    if query:
        if use_fts:
            sql = (
                f"SELECT id, session_id, app_name, user_id, scope, event_id, author, timestamp, "
                "content_json, content_text, metadata_json, inserted_at, embedding "
                f"FROM {table_name} WHERE {where_scope} "
                "AND MATCH(content_text) AGAINST (? IN NATURAL LANGUAGE MODE) "
                "ORDER BY timestamp DESC LIMIT ?"
            )
            return sql, (*scope_params, query, candidate_limit)
        sql = (
            f"SELECT id, session_id, app_name, user_id, scope, event_id, author, timestamp, "
            "content_json, content_text, metadata_json, inserted_at, embedding "
            f"FROM {table_name} WHERE {where_scope} AND content_text LIKE ? "
            "ORDER BY timestamp DESC LIMIT ?"
        )
        return sql, (*scope_params, f"%{query}%", candidate_limit)

    sql = (
        f"SELECT id, session_id, app_name, user_id, scope, event_id, author, timestamp, "
        "content_json, content_text, metadata_json, inserted_at, embedding "
        f"FROM {table_name} WHERE {where_scope} ORDER BY timestamp DESC LIMIT ?"
    )
    return sql, (*scope_params, candidate_limit)


class MySQLSyncADKMemoryStore(_BaseSyncADKMemoryStore[Any]):
    """Synchronous MySQL/MariaDB ADK memory store with `FULLTEXT` and MySQL 9 `VECTOR` search."""

    __slots__ = ("_json_deserializer", "_json_serializer", "_native_vector")

    adapter_name = "vertebra_mysql"

    def __init__(self, config: Any) -> None:
        """Initialize the synchronous MySQL ADK memory store."""
        super().__init__(config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        self._native_vector = _use_native_mysql_vector(config, self._vector_index_type)

    def _memory_table_ddl(self) -> str:
        adk_cfg = _extract_adk_config(self._config)
        return _build_mysql_memory_ddl(
            self._memory_table,
            self._owner_id_column_ddl,
            self._use_fts,
            self._native_vector,
            self._vector_dimensions,
            mysql_table_options(adk_cfg, "memory_table_options"),
        )

    def _drop_memory_table_sql(self) -> list[str]:
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

    def create_tables(self) -> None:
        """Create the MySQL memory table and optional `FULLTEXT` / `VECTOR` columns."""
        if not self._enabled:
            return
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return
        with self._config.provide_session() as driver:
            driver.execute_script(self._memory_table_ddl())
            driver.commit()
        self.reconcile_schema(assume_existing=True)

    def insert_memory_entries(self, entries: "list[StoredMemory]", owner_id: object | None = None) -> int:
        """Bulk insert memory entries ignoring duplicate `event_id` rows."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0
        sql, params_list = _build_memory_insert_statement(
            self._memory_table,
            self._owner_id_column_name,
            entries,
            owner_id,
            serializer=self._json_serializer,
            native_vector=self._native_vector,
        )
        with self._config.provide_session() as driver:
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
        """Search memory entries using MySQL `FULLTEXT`, `LIKE`, MySQL 9 `DISTANCE`, or cosine similarity."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not query and embedding is None:
            return []
        limit_value = limit or self._max_results
        if limit_value <= 0:
            return []
        sql, params = _build_memory_search_query(
            self._memory_table,
            query,
            app_name,
            user_id,
            limit_value,
            scope_filter,
            embedding,
            use_fts=self._use_fts,
            native_vector=self._native_vector,
        )
        try:
            with self._config.provide_session() as driver:
                rows = driver.select(sql, *params)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return []
            raise
        records = [_memory_row_to_record(row, deserializer=self._json_deserializer) for row in rows]
        if self._native_vector and embedding is not None:
            return records[:limit_value]
        return _rank_memory_records(records, embedding, limit_value)

    def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries associated with `session_id`."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        try:
            with self._config.provide_session() as driver:
                res = driver.execute(f"DELETE FROM {self._memory_table} WHERE session_id = ?", session_id)
                driver.commit()
                return int(res.rows_affected or 0)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return 0
            raise

    def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
        """Delete memory entries older than `days` days."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        clauses = ["inserted_at < (UTC_TIMESTAMP(6) - INTERVAL ? DAY)"]
        params: list[Any] = [int(days)]
        if app_name is not None:
            clauses.append("app_name = ?")
            params.append(app_name)
        if scope is not None:
            clauses.append("scope = ?")
            params.append(scope)
        sql = f"DELETE FROM {self._memory_table} WHERE {' AND '.join(clauses)}"
        try:
            with self._config.provide_session() as driver:
                res = driver.execute(sql, *params)
                driver.commit()
                return int(res.rows_affected or 0)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return 0
            raise


class MySQLAsyncADKMemoryStore(_BaseAsyncADKMemoryStore[Any]):
    """Asynchronous MySQL/MariaDB ADK memory store with `FULLTEXT` and MySQL 9 `VECTOR` search."""

    __slots__ = ("_json_deserializer", "_json_serializer", "_native_vector")

    adapter_name = "vertebra_mysql"

    def __init__(self, config: Any) -> None:
        """Initialize the asynchronous MySQL ADK memory store."""
        super().__init__(config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        self._native_vector = _use_native_mysql_vector(config, self._vector_index_type)

    async def _memory_table_ddl(self) -> str:
        adk_cfg = _extract_adk_config(self._config)
        return _build_mysql_memory_ddl(
            self._memory_table,
            self._owner_id_column_ddl,
            self._use_fts,
            self._native_vector,
            self._vector_dimensions,
            mysql_table_options(adk_cfg, "memory_table_options"),
        )

    def _drop_memory_table_sql(self) -> list[str]:
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

    async def create_tables(self) -> None:
        """Create the MySQL memory table asynchronously."""
        if not self._enabled:
            return
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._memory_table_ddl())
            await driver.commit()
        await self.reconcile_schema(assume_existing=True)

    async def insert_memory_entries(self, entries: "list[StoredMemory]", owner_id: object | None = None) -> int:
        """Bulk insert memory entries asynchronously ignoring duplicates."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0
        sql, params_list = _build_memory_insert_statement(
            self._memory_table,
            self._owner_id_column_name,
            entries,
            owner_id,
            serializer=self._json_serializer,
            native_vector=self._native_vector,
        )
        async with self._config.provide_session() as driver:
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
        """Search memory entries asynchronously via MySQL `FULLTEXT`, `LIKE`, or `VECTOR` distance."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not query and embedding is None:
            return []
        limit_value = limit or self._max_results
        if limit_value <= 0:
            return []
        sql, params = _build_memory_search_query(
            self._memory_table,
            query,
            app_name,
            user_id,
            limit_value,
            scope_filter,
            embedding,
            use_fts=self._use_fts,
            native_vector=self._native_vector,
        )
        try:
            async with self._config.provide_session() as driver:
                rows = await driver.select(sql, *params)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return []
            raise
        records = [_memory_row_to_record(row, deserializer=self._json_deserializer) for row in rows]
        if self._native_vector and embedding is not None:
            return records[:limit_value]
        return _rank_memory_records(records, embedding, limit_value)

    async def delete_entries_by_session(self, session_id: str) -> int:
        """Delete memory entries by session identifier asynchronously."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        try:
            async with self._config.provide_session() as driver:
                res = await driver.execute(f"DELETE FROM {self._memory_table} WHERE session_id = ?", session_id)
                await driver.commit()
                return int(res.rows_affected or 0)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return 0
            raise

    async def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
        """Delete memory entries older than `days` days asynchronously."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        clauses = ["inserted_at < (UTC_TIMESTAMP(6) - INTERVAL ? DAY)"]
        params: list[Any] = [int(days)]
        if app_name is not None:
            clauses.append("app_name = ?")
            params.append(app_name)
        if scope is not None:
            clauses.append("scope = ?")
            params.append(scope)
        sql = f"DELETE FROM {self._memory_table} WHERE {' AND '.join(clauses)}"
        try:
            async with self._config.provide_session() as driver:
                res = await driver.execute(sql, *params)
                await driver.commit()
                return int(res.rows_affected or 0)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return 0
            raise


def _artifact_table_ddl(table_name: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
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
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
    )


def _row_to_stored_artifact(row: Mapping[str, Any], *, deserializer: Any = from_json) -> "StoredArtifact":
    session_raw = row.get("session_id")
    custom_raw = row.get("custom_metadata")
    return {
        "app_name": str(row["app_name"]),
        "user_id": str(row["user_id"]),
        "session_id": str(session_raw) if session_raw else None,
        "filename": str(row["filename"]),
        "version": int(row["version"]),
        "mime_type": str(row["mime_type"]) if row.get("mime_type") is not None else None,
        "canonical_uri": str(row["canonical_uri"]),
        "custom_metadata": mysql_json_dict(custom_raw, deserializer=deserializer) if custom_raw is not None else None,
        "created_at": _ensure_utc(row["created_at"]),
    }


def _row_to_artifact_reference(row: Mapping[str, Any]) -> ArtifactReference:
    raw_updated = row.get("updated")
    return ArtifactReference(
        uri=str(row["canonical_uri"]),
        version=int(row["version"]),
        content_type=str(row["mime_type"]) if row.get("mime_type") is not None else None,
        size=int(row["size"]) if row.get("size") is not None else None,
        updated=_ensure_utc(raw_updated) if raw_updated is not None else None,
        signed_url=str(row["signed_url"]) if row.get("signed_url") is not None else None,
    )


class MySQLSyncADKArtifactStore(_BaseSyncADKArtifactStore[Any]):
    """Synchronous MySQL ADK artifact metadata store supporting `ArtifactReference` records."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    adapter_name = "vertebra_mysql"

    def __init__(self, config: Any) -> None:
        """Initialize the synchronous MySQL ADK artifact store."""
        super().__init__(config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    def _artifact_table_ddl(self) -> str:
        return _artifact_table_ddl(self._artifact_table)

    def create_table(self) -> None:
        """Create the MySQL ADK artifact metadata table if absent."""
        with self._config.provide_session() as driver:
            driver.execute_script(self._artifact_table_ddl())
            driver.commit()

    def insert_artifact(self, record: "StoredArtifact") -> None:
        """Insert or update a `StoredArtifact` metadata record."""
        session_val = record["session_id"] if record["session_id"] is not None else ""
        meta = record.get("custom_metadata")
        sql = (
            f"INSERT INTO {self._artifact_table} "
            "(app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON DUPLICATE KEY UPDATE mime_type = VALUES(mime_type), canonical_uri = VALUES(canonical_uri), "
            "custom_metadata = VALUES(custom_metadata)"
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
                self._json_serializer(meta) if meta is not None else None,
                _naive_utc(record["created_at"]),
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
        """Upsert an artifact revision backed by a storage-neutral `ArtifactReference` struct."""
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
                self._json_serializer(custom_metadata) if custom_metadata is not None else None,
                reference.size,
                _naive_utc(reference.updated) if reference.updated is not None else None,
                reference.signed_url,
                _naive_utc(now_dt),
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
        """Fetch an `ArtifactReference` revision by exact version or latest."""
        session_val = session_id if session_id is not None else ""
        with self._config.provide_session() as driver:
            if version is not None:
                row = driver.select_one_or_none(
                    f"SELECT * FROM {self._artifact_table} "
                    "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? AND version = ? LIMIT 1",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                    version,
                )
            else:
                row = driver.select_one_or_none(
                    f"SELECT * FROM {self._artifact_table} "
                    "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? "
                    "ORDER BY version DESC LIMIT 1",
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
        """Fetch a `StoredArtifact` revision by exact version or latest."""
        session_val = session_id if session_id is not None else ""
        with self._config.provide_session() as driver:
            if version is not None:
                row = driver.select_one_or_none(
                    f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at "
                    f"FROM {self._artifact_table} "
                    "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? AND version = ? LIMIT 1",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                    version,
                )
            else:
                row = driver.select_one_or_none(
                    f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at "
                    f"FROM {self._artifact_table} "
                    "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? "
                    "ORDER BY version DESC LIMIT 1",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                )
        return _row_to_stored_artifact(row, deserializer=self._json_deserializer) if row is not None else None

    def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """List distinct artifact filenames within user or session scope."""
        with self._config.provide_session() as driver:
            if session_id is not None:
                rows = driver.select(
                    f"SELECT DISTINCT filename FROM {self._artifact_table} "
                    "WHERE app_name = ? AND user_id = ? AND session_id IN ('', ?) ORDER BY filename ASC",
                    app_name,
                    user_id,
                    session_id,
                )
            else:
                rows = driver.select(
                    f"SELECT DISTINCT filename FROM {self._artifact_table} "
                    "WHERE app_name = ? AND user_id = ? AND session_id = '' ORDER BY filename ASC",
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
        """List all stored versions for an artifact sorted ascending."""
        session_val = session_id if session_id is not None else ""
        with self._config.provide_session() as driver:
            rows = driver.select(
                f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at "
                f"FROM {self._artifact_table} "
                "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? ORDER BY version ASC",
                app_name,
                user_id,
                session_val,
                filename,
            )
        return [_row_to_stored_artifact(row, deserializer=self._json_deserializer) for row in rows]

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
            max_ver = driver.select_value_or_none(
                f"SELECT MAX(version) FROM {self._artifact_table} "
                "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ?",
                app_name,
                user_id,
                session_val,
                filename,
            )
        return int(max_ver) + 1 if max_ver is not None else 0


class MySQLAsyncADKArtifactStore(_BaseAsyncADKArtifactStore[Any]):
    """Asynchronous MySQL ADK artifact metadata store supporting `ArtifactReference` records."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    adapter_name = "vertebra_mysql"

    def __init__(self, config: Any) -> None:
        """Initialize the asynchronous MySQL ADK artifact store."""
        super().__init__(config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    def _artifact_table_ddl(self) -> str:
        return _artifact_table_ddl(self._artifact_table)

    async def create_table(self) -> None:
        """Create the MySQL ADK artifact metadata table asynchronously."""
        async with self._config.provide_session() as driver:
            await driver.execute_script(self._artifact_table_ddl())
            await driver.commit()

    async def insert_artifact(self, record: "StoredArtifact") -> None:
        """Insert or update a `StoredArtifact` metadata record asynchronously."""
        session_val = record["session_id"] if record["session_id"] is not None else ""
        meta = record.get("custom_metadata")
        sql = (
            f"INSERT INTO {self._artifact_table} "
            "(app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON DUPLICATE KEY UPDATE mime_type = VALUES(mime_type), canonical_uri = VALUES(canonical_uri), "
            "custom_metadata = VALUES(custom_metadata)"
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
                self._json_serializer(meta) if meta is not None else None,
                _naive_utc(record["created_at"]),
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
        """Upsert an artifact revision backed by `ArtifactReference` asynchronously."""
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
                self._json_serializer(custom_metadata) if custom_metadata is not None else None,
                reference.size,
                _naive_utc(reference.updated) if reference.updated is not None else None,
                reference.signed_url,
                _naive_utc(now_dt),
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
        """Fetch an `ArtifactReference` revision asynchronously."""
        session_val = session_id if session_id is not None else ""
        async with self._config.provide_session() as driver:
            if version is not None:
                row = await driver.select_one_or_none(
                    f"SELECT * FROM {self._artifact_table} "
                    "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? AND version = ? LIMIT 1",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                    version,
                )
            else:
                row = await driver.select_one_or_none(
                    f"SELECT * FROM {self._artifact_table} "
                    "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? "
                    "ORDER BY version DESC LIMIT 1",
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
        """Fetch a `StoredArtifact` revision asynchronously."""
        session_val = session_id if session_id is not None else ""
        async with self._config.provide_session() as driver:
            if version is not None:
                row = await driver.select_one_or_none(
                    f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at "
                    f"FROM {self._artifact_table} "
                    "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? AND version = ? LIMIT 1",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                    version,
                )
            else:
                row = await driver.select_one_or_none(
                    f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at "
                    f"FROM {self._artifact_table} "
                    "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? "
                    "ORDER BY version DESC LIMIT 1",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                )
        return _row_to_stored_artifact(row, deserializer=self._json_deserializer) if row is not None else None

    async def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """List distinct artifact filenames asynchronously."""
        async with self._config.provide_session() as driver:
            if session_id is not None:
                rows = await driver.select(
                    f"SELECT DISTINCT filename FROM {self._artifact_table} "
                    "WHERE app_name = ? AND user_id = ? AND session_id IN ('', ?) ORDER BY filename ASC",
                    app_name,
                    user_id,
                    session_id,
                )
            else:
                rows = await driver.select(
                    f"SELECT DISTINCT filename FROM {self._artifact_table} "
                    "WHERE app_name = ? AND user_id = ? AND session_id = '' ORDER BY filename ASC",
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
        """List all stored versions for an artifact sorted ascending asynchronously."""
        session_val = session_id if session_id is not None else ""
        async with self._config.provide_session() as driver:
            rows = await driver.select(
                f"SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at "
                f"FROM {self._artifact_table} "
                "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? ORDER BY version ASC",
                app_name,
                user_id,
                session_val,
                filename,
            )
        return [_row_to_stored_artifact(row, deserializer=self._json_deserializer) for row in rows]

    async def delete_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> "list[StoredArtifact]":
        """Delete all metadata versions for an artifact asynchronously and return removed records."""
        records = await self.list_artifact_versions(app_name, user_id, filename, session_id=session_id)
        session_val = session_id if session_id is not None else ""
        async with self._config.provide_session() as driver:
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
        """Compute the next zero-based artifact version number asynchronously."""
        session_val = session_id if session_id is not None else ""
        async with self._config.provide_session() as driver:
            max_ver = await driver.select_value_or_none(
                f"SELECT MAX(version) FROM {self._artifact_table} "
                "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ?",
                app_name,
                user_id,
                session_val,
                filename,
            )
        return int(max_ver) + 1 if max_ver is not None else 0
