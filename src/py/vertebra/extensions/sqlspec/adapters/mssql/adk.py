"""SQL Server (MSSQL) ADK session, event, memory, and artifact stores."""

from __future__ import annotations

import contextlib
from collections.abc import Mapping
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

from vertebra.extensions.sqlspec.adapters.mssql.core import (
    build_mssql_scope_where,
    escape_mssql_like,
    escape_sql_literal,
    is_mssql_table_missing,
    mssql_constraint_ref,
    mssql_ensure_utc,
    mssql_json_dict,
    mssql_row_value,
    mssql_table_ref,
    quote_bracket_identifier,
    split_schema_and_table,
)
from vertebra.storage import ArtifactReference
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.extensions.adk import SessionOrderBy
    from sqlspec.extensions.adk.artifact import StoredArtifact
    from sqlspec.extensions.adk.artifact.store import BaseAsyncADKArtifactStore as _BaseAsyncADKArtifactStore
    from sqlspec.extensions.adk.artifact.store import BaseSyncADKArtifactStore as _BaseSyncADKArtifactStore
    from sqlspec.extensions.adk.converters import merge_scoped_state
    from sqlspec.extensions.adk.memory import StoredMemory
    from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore as _BaseAsyncADKMemoryStore
    from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore as _BaseSyncADKMemoryStore

    from vertebra.extensions.sqlspec.adapters.mssql.config import MSSQLAsyncConfig, MSSQLSyncConfig
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
    "MSSQLADKConfig",
    "MSSQLAsyncADKArtifactStore",
    "MSSQLAsyncADKMemoryStore",
    "MSSQLAsyncADKStore",
    "MSSQLSyncADKArtifactStore",
    "MSSQLSyncADKMemoryStore",
    "MSSQLSyncADKStore",
)

_MSSQL_DEFAULT_SCHEMA: Final[str] = "dbo"
_JSON_FALLBACK_TYPE: Final[str] = "NVARCHAR(MAX)"
_JSON_NATIVE_TYPE: Final[str] = "JSON"
_MSSQL_ADK_EXTENSION_OPTIONS: Final[frozenset[str]] = frozenset(
    {
        "json_type",
        "memory_retention_days",
        "native_json",
        "use_fts",
    }
)


class MSSQLADKConfig(ADKConfig, total=False):
    """SQL Server ADK extension settings under ``extension_config['adk']``."""

    native_json: NotRequired[bool]
    """Force native ``JSON`` column storage when ``True`` or ``NVARCHAR(MAX)`` when ``False``."""

    json_type: NotRequired[Literal["JSON", "NVARCHAR(MAX)"]]
    """Explicit SQL Server JSON column type (``'JSON'`` or ``'NVARCHAR(MAX)'``)."""

    memory_retention_days: NotRequired[int]
    """Optional retention window in days for ADK memory entries."""

    use_fts: NotRequired[bool]
    """Optional full-text enablement flag aliased to ``memory_use_fts``."""


def _adk_settings(config: Any) -> Mapping[str, Any]:
    ext = getattr(config, "extension_config", {})
    if not isinstance(ext, Mapping):
        return {}
    adk = ext.get("adk", {})
    return adk if isinstance(adk, Mapping) else {}


def _resolve_json_init(config: Any) -> tuple[bool | None, str | None]:
    adk_cfg = _adk_settings(config)
    raw_native = adk_cfg.get("native_json")
    native_json: bool | None = raw_native if isinstance(raw_native, bool) else None
    raw_json_type = adk_cfg.get("json_type")
    if isinstance(raw_json_type, str) and raw_json_type.strip():
        normalized = raw_json_type.strip().upper()
        if normalized == _JSON_NATIVE_TYPE:
            return native_json, _JSON_NATIVE_TYPE
        if normalized in {_JSON_FALLBACK_TYPE, "NVARCHAR", "TEXT"}:
            return native_json, _JSON_FALLBACK_TYPE
        msg = f"Unsupported MSSQL ADK json_type: {raw_json_type!r}; expected 'JSON' or 'NVARCHAR(MAX)'"
        raise ImproperConfigurationError(msg)
    return native_json, _configured_json_type(native_json)


def _configured_json_type(native_json: bool | None) -> str | None:
    if native_json is True:
        return _JSON_NATIVE_TYPE
    if native_json is False:
        return _JSON_FALLBACK_TYPE
    return None


def _resolve_json_codecs(config: Any) -> tuple[Any, Any]:
    features = getattr(config, "driver_features", None)
    if isinstance(features, Mapping):
        serializer = features.get("json_serializer")
        deserializer = features.get("json_deserializer")
        if callable(serializer) or callable(deserializer):
            return (
                serializer if callable(serializer) else to_json,
                deserializer if callable(deserializer) else from_json,
            )
    statement_config = getattr(config, "statement_config", None)
    param_cfg = getattr(statement_config, "parameter_config", None)
    serializer = getattr(param_cfg, "json_serializer", None) or to_json
    deserializer = getattr(param_cfg, "json_deserializer", None) or from_json
    return serializer, deserializer


def _bare_index_name(name: str) -> str:
    return name.rsplit(".", 1)[-1].casefold()


def _existing_index_names_sync(driver: Any, index_specs: list[tuple[str, str, str]]) -> set[str]:
    dd = getattr(driver, "data_dictionary", None)
    if dd is None:
        return set()
    schemas = {split_schema_and_table(tbl)[0] for _, tbl, _ in index_specs} or {_MSSQL_DEFAULT_SCHEMA}
    existing: set[str] = set()
    for schema_name in schemas:
        try:
            rows = dd.get_indexes(driver, schema=schema_name)
        except Exception:
            rows = []
        for row in rows:
            idx_name = mssql_row_value(row, "index_name")
            if idx_name:
                existing.add(_bare_index_name(str(idx_name)))
    return existing


async def _existing_index_names_async(driver: Any, index_specs: list[tuple[str, str, str]]) -> set[str]:
    dd = getattr(driver, "data_dictionary", None)
    if dd is None:
        return set()
    schemas = {split_schema_and_table(tbl)[0] for _, tbl, _ in index_specs} or {_MSSQL_DEFAULT_SCHEMA}
    existing: set[str] = set()
    for schema_name in schemas:
        try:
            rows = await dd.get_indexes(driver, schema=schema_name)
        except Exception:
            rows = []
        for row in rows:
            idx_name = mssql_row_value(row, "index_name")
            if idx_name:
                existing.add(_bare_index_name(str(idx_name)))
    return existing


def _json_type_from_version(version_info: Any) -> str:
    if (
        version_info is not None
        and callable(getattr(version_info, "supports_native_json", None))
        and version_info.supports_native_json()
    ):
        return _JSON_NATIVE_TYPE
    return _JSON_FALLBACK_TYPE


def _detect_json_type_sync(driver: Any, native_json: bool | None) -> str:
    configured = _configured_json_type(native_json)
    if configured is not None:
        return configured
    dd = getattr(driver, "data_dictionary", None)
    if dd is None:
        return _JSON_FALLBACK_TYPE
    try:
        version_info = dd.get_version(driver)
    except Exception:
        return _JSON_FALLBACK_TYPE
    return _json_type_from_version(version_info)


async def _detect_json_type_async(driver: Any, native_json: bool | None) -> str:
    configured = _configured_json_type(native_json)
    if configured is not None:
        return configured
    dd = getattr(driver, "data_dictionary", None)
    if dd is None:
        return _JSON_FALLBACK_TYPE
    try:
        version_info = await dd.get_version(driver)
    except Exception:
        return _JSON_FALLBACK_TYPE
    return _json_type_from_version(version_info)


def _create_index_sql(table: str, index_name: str, columns: str) -> str:
    return f"CREATE INDEX {quote_bracket_identifier(index_name)} ON {mssql_table_ref(table)} ({columns})"


def _drop_if_exists_sql(table: str) -> str:
    ref = mssql_table_ref(table)
    return f"IF OBJECT_ID(N'{escape_sql_literal(ref)}', N'U') IS NOT NULL DROP TABLE {ref};"


def _row_to_session(row: Any, *, deserializer: Any = from_json) -> StoredSession:
    return {
        "id": str(mssql_row_value(row, "id", 0)),
        "app_name": str(mssql_row_value(row, "app_name", 1)),
        "user_id": str(mssql_row_value(row, "user_id", 2)),
        "state": mssql_json_dict(mssql_row_value(row, "state", 3), deserializer=deserializer),
        "create_time": mssql_ensure_utc(mssql_row_value(row, "create_time", 4)),
        "update_time": mssql_ensure_utc(mssql_row_value(row, "update_time", 5)),
    }


def _row_to_event(row: Any, *, deserializer: Any = from_json) -> StoredEvent:
    inv = mssql_row_value(row, "invocation_id", 4)
    return {
        "id": str(mssql_row_value(row, "id", 0)),
        "app_name": str(mssql_row_value(row, "app_name", 1)),
        "user_id": str(mssql_row_value(row, "user_id", 2)),
        "session_id": str(mssql_row_value(row, "session_id", 3)),
        "invocation_id": str(inv) if inv is not None else "",
        "timestamp": mssql_ensure_utc(mssql_row_value(row, "timestamp", 5)),
        "event_data": mssql_json_dict(mssql_row_value(row, "event_data", 6), deserializer=deserializer),
    }


def _row_to_memory(row: Any, *, deserializer: Any = from_json) -> StoredMemory:
    metadata_raw = mssql_row_value(row, "metadata_json", 10)
    author_raw = mssql_row_value(row, "author", 6)
    return cast(
        "StoredMemory",
        {
            "id": str(mssql_row_value(row, "id", 0)),
            "session_id": str(mssql_row_value(row, "session_id", 1)),
            "app_name": str(mssql_row_value(row, "app_name", 2)),
            "user_id": str(mssql_row_value(row, "user_id", 3)),
            "scope": cast("Literal['user', 'app']", str(mssql_row_value(row, "scope", 4) or "user")),
            "event_id": str(mssql_row_value(row, "event_id", 5)),
            "author": str(author_raw) if author_raw is not None else None,
            "timestamp": mssql_ensure_utc(mssql_row_value(row, "timestamp", 7)),
            "content_json": mssql_json_dict(mssql_row_value(row, "content_json", 8), deserializer=deserializer),
            "content_text": str(mssql_row_value(row, "content_text", 9)),
            "metadata_json": mssql_json_dict(metadata_raw, deserializer=deserializer)
            if metadata_raw is not None
            else None,
            "inserted_at": mssql_ensure_utc(mssql_row_value(row, "inserted_at", 11)),
            "embedding": None,
        },
    )


def _row_to_stored_artifact(row: Any, *, deserializer: Any = from_json) -> StoredArtifact:
    session_raw = mssql_row_value(row, "session_id", 2)
    custom_raw = mssql_row_value(row, "custom_metadata", 7)
    mime_raw = mssql_row_value(row, "mime_type", 5)
    return cast(
        "StoredArtifact",
        {
            "app_name": str(mssql_row_value(row, "app_name", 0)),
            "user_id": str(mssql_row_value(row, "user_id", 1)),
            "session_id": str(session_raw) if session_raw else None,
            "filename": str(mssql_row_value(row, "filename", 3)),
            "version": int(mssql_row_value(row, "version", 4)),
            "mime_type": str(mime_raw) if mime_raw is not None else None,
            "canonical_uri": str(mssql_row_value(row, "canonical_uri", 6)),
            "custom_metadata": mssql_json_dict(custom_raw, deserializer=deserializer)
            if custom_raw is not None
            else None,
            "created_at": mssql_ensure_utc(mssql_row_value(row, "created_at", 8)),
        },
    )


def _row_to_artifact_reference(row: Any) -> ArtifactReference:
    mime_raw = mssql_row_value(row, "mime_type")
    size_raw = mssql_row_value(row, "size")
    updated_raw = mssql_row_value(row, "updated")
    signed_raw = mssql_row_value(row, "signed_url")
    return ArtifactReference(
        uri=str(mssql_row_value(row, "canonical_uri")),
        version=int(mssql_row_value(row, "version")),
        content_type=str(mime_raw) if mime_raw is not None else None,
        size=int(size_raw) if size_raw is not None else None,
        updated=mssql_ensure_utc(updated_raw) if updated_raw is not None else None,
        signed_url=str(signed_raw) if signed_raw is not None else None,
    )


class _MSSQLADKSessionSQLMixin:
    """Shared T-SQL DDL and query builders for synchronous and asynchronous ADK session stores."""

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
    _native_json: bool | None
    _json_column_type: str | None

    def _build_sessions_ddl(self, json_col_type: str) -> str:
        owner_line = f",\n        {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        schema_name, table_name = split_schema_and_table(self._session_table)
        return (
            "IF NOT EXISTS ("
            f"SELECT 1 FROM sys.tables WHERE name = N'{escape_sql_literal(table_name)}' "
            f"AND schema_id = SCHEMA_ID(N'{escape_sql_literal(schema_name)}')"
            ")\nBEGIN\n"
            f"    CREATE TABLE {mssql_table_ref(self._session_table)} (\n"
            f"        row_id UNIQUEIDENTIFIER NOT NULL CONSTRAINT {mssql_constraint_ref('df', self._session_table, 'row_id')} DEFAULT NEWSEQUENTIALID(),\n"
            "        id NVARCHAR(128) NOT NULL,\n"
            "        app_name NVARCHAR(128) NOT NULL,\n"
            f"        user_id NVARCHAR(128) NOT NULL{owner_line},\n"
            f"        state {json_col_type} NOT NULL,\n"
            f"        create_time DATETIMEOFFSET(6) NOT NULL CONSTRAINT {mssql_constraint_ref('df', self._session_table, 'create_time')} DEFAULT SYSDATETIMEOFFSET(),\n"
            f"        update_time DATETIMEOFFSET(6) NOT NULL CONSTRAINT {mssql_constraint_ref('df', self._session_table, 'update_time')} DEFAULT SYSDATETIMEOFFSET(),\n"
            f"        CONSTRAINT {mssql_constraint_ref('pk', self._session_table, 'row_id')} PRIMARY KEY (row_id),\n"
            f"        CONSTRAINT {mssql_constraint_ref('uq', self._session_table, 'id')} UNIQUE (id)\n"
            "    );\n"
            "END;"
        )

    def _build_events_ddl(self, json_col_type: str) -> str:
        schema_name, table_name = split_schema_and_table(self._events_table)
        return (
            "IF NOT EXISTS ("
            f"SELECT 1 FROM sys.tables WHERE name = N'{escape_sql_literal(table_name)}' "
            f"AND schema_id = SCHEMA_ID(N'{escape_sql_literal(schema_name)}')"
            ")\nBEGIN\n"
            f"    CREATE TABLE {mssql_table_ref(self._events_table)} (\n"
            f"        row_id UNIQUEIDENTIFIER NOT NULL CONSTRAINT {mssql_constraint_ref('df', self._events_table, 'row_id')} DEFAULT NEWSEQUENTIALID(),\n"
            "        id NVARCHAR(128) NOT NULL,\n"
            "        app_name NVARCHAR(128) NOT NULL,\n"
            "        user_id NVARCHAR(128) NOT NULL,\n"
            "        session_id NVARCHAR(128) NOT NULL,\n"
            "        invocation_id NVARCHAR(256),\n"
            "        timestamp DATETIMEOFFSET(6) NOT NULL,\n"
            f"        event_data {json_col_type} NOT NULL,\n"
            f"        CONSTRAINT {mssql_constraint_ref('pk', self._events_table, 'row_id')} PRIMARY KEY (row_id),\n"
            f"        CONSTRAINT {mssql_constraint_ref('uq', self._events_table, 'id')} UNIQUE (id),\n"
            f"        CONSTRAINT {mssql_constraint_ref('fk', self._events_table, 'session')} FOREIGN KEY (session_id) "
            f"REFERENCES {mssql_table_ref(self._session_table)}(id) ON DELETE CASCADE\n"
            "    );\n"
            "END;"
        )

    def _build_app_states_ddl(self, json_col_type: str) -> str:
        schema_name, table_name = split_schema_and_table(self._app_state_table)
        return (
            "IF NOT EXISTS ("
            f"SELECT 1 FROM sys.tables WHERE name = N'{escape_sql_literal(table_name)}' "
            f"AND schema_id = SCHEMA_ID(N'{escape_sql_literal(schema_name)}')"
            ")\nBEGIN\n"
            f"    CREATE TABLE {mssql_table_ref(self._app_state_table)} (\n"
            "        app_name NVARCHAR(128) NOT NULL,\n"
            f"        state {json_col_type} NOT NULL,\n"
            f"        update_time DATETIMEOFFSET(6) NOT NULL CONSTRAINT {mssql_constraint_ref('df', self._app_state_table, 'update_time')} DEFAULT SYSDATETIMEOFFSET(),\n"
            f"        CONSTRAINT {mssql_constraint_ref('pk', self._app_state_table, 'app_name')} PRIMARY KEY (app_name)\n"
            "    );\n"
            "END;"
        )

    def _build_user_states_ddl(self, json_col_type: str) -> str:
        schema_name, table_name = split_schema_and_table(self._user_state_table)
        return (
            "IF NOT EXISTS ("
            f"SELECT 1 FROM sys.tables WHERE name = N'{escape_sql_literal(table_name)}' "
            f"AND schema_id = SCHEMA_ID(N'{escape_sql_literal(schema_name)}')"
            ")\nBEGIN\n"
            f"    CREATE TABLE {mssql_table_ref(self._user_state_table)} (\n"
            "        app_name NVARCHAR(128) NOT NULL,\n"
            "        user_id NVARCHAR(128) NOT NULL,\n"
            f"        state {json_col_type} NOT NULL,\n"
            f"        update_time DATETIMEOFFSET(6) NOT NULL CONSTRAINT {mssql_constraint_ref('df', self._user_state_table, 'update_time')} DEFAULT SYSDATETIMEOFFSET(),\n"
            f"        CONSTRAINT {mssql_constraint_ref('pk', self._user_state_table, 'app_user')} PRIMARY KEY (app_name, user_id)\n"
            "    );\n"
            "END;"
        )

    def _build_metadata_ddl(self) -> str:
        schema_name, table_name = split_schema_and_table(self._metadata_table)
        return (
            "IF NOT EXISTS ("
            f"SELECT 1 FROM sys.tables WHERE name = N'{escape_sql_literal(table_name)}' "
            f"AND schema_id = SCHEMA_ID(N'{escape_sql_literal(schema_name)}')"
            ")\nBEGIN\n"
            f"    CREATE TABLE {mssql_table_ref(self._metadata_table)} (\n"
            "        [key] NVARCHAR(128) NOT NULL,\n"
            "        value NVARCHAR(512) NOT NULL,\n"
            f"        CONSTRAINT {mssql_constraint_ref('pk', self._metadata_table, 'key')} PRIMARY KEY ([key])\n"
            "    );\n"
            "END;"
        )

    def _session_index_specs(self) -> list[tuple[str, str, str]]:
        _, sess_name = split_schema_and_table(self._session_table)
        _, ev_name = split_schema_and_table(self._events_table)
        return [
            (f"idx_{sess_name}_app_user", self._session_table, "app_name, user_id"),
            (f"idx_{sess_name}_update_time", self._session_table, "update_time DESC"),
            (f"idx_{ev_name}_scope", self._events_table, "app_name, user_id, session_id, timestamp ASC"),
            (f"idx_{ev_name}_session", self._events_table, "session_id, timestamp ASC"),
            (f"idx_{ev_name}_invocation", self._events_table, "invocation_id"),
            (f"idx_{ev_name}_timestamp", self._events_table, "timestamp ASC"),
            (f"idx_{ev_name}_app_timestamp", self._events_table, "app_name, timestamp ASC"),
        ]

    def _drop_app_states_table_sql(self) -> str:
        return _drop_if_exists_sql(self._app_state_table)

    def _drop_user_states_table_sql(self) -> str:
        return _drop_if_exists_sql(self._user_state_table)

    def _drop_metadata_table_sql(self) -> str:
        return _drop_if_exists_sql(self._metadata_table)

    def _drop_tables_sql(self) -> list[str]:
        return [
            self._drop_metadata_table_sql(),
            self._drop_user_states_table_sql(),
            self._drop_app_states_table_sql(),
            _drop_if_exists_sql(self._events_table),
            _drop_if_exists_sql(self._session_table),
        ]

    def _create_session_sql_and_params(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
        state: dict[str, Any],
        owner_id: Any | None,
    ) -> tuple[str, tuple[Any, ...]]:
        table_ref = mssql_table_ref(self._session_table)
        state_json = self._json_serializer(state)
        if self._owner_id_column_name:
            sql = (
                f"INSERT INTO {table_ref} "
                f"(id, app_name, user_id, {quote_bracket_identifier(self._owner_id_column_name)}, state, create_time, update_time) "
                "VALUES (?, ?, ?, ?, ?, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET())"
            )
            return sql, (session_id, app_name, user_id, owner_id, state_json)
        sql = (
            f"INSERT INTO {table_ref} (id, app_name, user_id, state, create_time, update_time) "
            "VALUES (?, ?, ?, ?, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET())"
        )
        return sql, (session_id, app_name, user_id, state_json)

    def _get_session_touch_sql(self) -> str:
        return (
            f"UPDATE {mssql_table_ref(self._session_table)} SET update_time = SYSDATETIMEOFFSET() "
            "WHERE app_name = ? AND user_id = ? AND id = ?"
        )

    def _get_session_select_sql(self) -> str:
        return (
            "SELECT TOP (1) id, app_name, user_id, state, create_time, update_time "
            f"FROM {mssql_table_ref(self._session_table)} WHERE app_name = ? AND user_id = ? AND id = ?"
        )

    def _update_session_state_sql(self) -> str:
        return (
            f"UPDATE {mssql_table_ref(self._session_table)} SET state = ?, update_time = SYSDATETIMEOFFSET() "
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
            params.extend((page_offset, page_limit))
            page_clause = " OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
        sql = (
            "SELECT id, app_name, user_id, state, create_time, update_time "
            f"FROM {mssql_table_ref(self._session_table)} "
            f"WHERE {where_clause} ORDER BY {column} {direction}, id {direction}{page_clause}"
        )
        return sql, tuple(params)

    def _delete_session_sql(self) -> str:
        return f"DELETE FROM {mssql_table_ref(self._session_table)} WHERE app_name = ? AND user_id = ? AND id = ?"

    def _append_event_sql_and_params(self, event_record: StoredEvent) -> tuple[str, tuple[Any, ...]]:
        app_name = event_record["app_name"]
        user_id = event_record["user_id"]
        session_id = event_record["session_id"]
        sql = (
            f"INSERT INTO {mssql_table_ref(self._events_table)} "
            "(id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
            "SELECT ?, ?, ?, ?, ?, ?, ? "
            f"FROM {mssql_table_ref(self._session_table)} "
            "WHERE id = ? AND app_name = ? AND user_id = ?"
        )
        params = (
            event_record["id"],
            app_name,
            user_id,
            session_id,
            event_record.get("invocation_id"),
            mssql_ensure_utc(event_record["timestamp"]),
            self._json_serializer(event_record["event_data"]),
            session_id,
            app_name,
            user_id,
        )
        return sql, params

    def _insert_event_sql_and_params(self, event_record: StoredEvent) -> tuple[str, tuple[Any, ...]]:
        sql = (
            f"INSERT INTO {mssql_table_ref(self._events_table)} "
            "(id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        params = (
            event_record["id"],
            event_record["app_name"],
            event_record["user_id"],
            event_record["session_id"],
            event_record.get("invocation_id"),
            mssql_ensure_utc(event_record["timestamp"]),
            self._json_serializer(event_record["event_data"]),
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
        top_clause = "TOP (?) " if limit is not None else ""
        params: list[Any] = [limit] if limit is not None else []
        params.extend([app_name, user_id, session_id])
        after_clause = ""
        if after_timestamp is not None:
            after_clause = " AND e.timestamp > ?"
            params.append(mssql_ensure_utc(after_timestamp))
        order_direction = "DESC" if limit is not None else "ASC"
        sql = (
            f"SELECT {top_clause}e.id, e.app_name, e.user_id, e.session_id, e.invocation_id, e.timestamp, e.event_data "
            f"FROM {mssql_table_ref(self._events_table)} e "
            f"JOIN {mssql_table_ref(self._session_table)} s "
            "ON e.session_id = s.id AND e.app_name = s.app_name AND e.user_id = s.user_id "
            f"WHERE s.app_name = ? AND s.user_id = ? AND s.id = ?{after_clause} "
            f"ORDER BY e.timestamp {order_direction}"
        )
        return sql, tuple(params)

    def _delete_before_sql_and_params(
        self,
        table: str,
        column: str,
        threshold: datetime,
        app_name: str | None,
    ) -> tuple[str, tuple[Any, ...]]:
        sql = f"DELETE FROM {mssql_table_ref(table)} WHERE {column} < ?"
        params: list[Any] = [mssql_ensure_utc(threshold)]
        if app_name is not None:
            sql = f"{sql} AND app_name = ?"
            params.append(app_name)
        return sql, tuple(params)

    def _select_app_state_sql(self, *, for_update: bool = False) -> str:
        lock_hint = " WITH (UPDLOCK, HOLDLOCK)" if for_update else ""
        return f"SELECT TOP (1) state FROM {mssql_table_ref(self._app_state_table)}{lock_hint} WHERE app_name = ?"

    def _insert_app_state_sql(self) -> str:
        return (
            f"INSERT INTO {mssql_table_ref(self._app_state_table)} (app_name, state, update_time) "
            "VALUES (?, ?, SYSDATETIMEOFFSET())"
        )

    def _update_app_state_sql(self) -> str:
        return (
            f"UPDATE {mssql_table_ref(self._app_state_table)} SET state = ?, update_time = SYSDATETIMEOFFSET() "
            "WHERE app_name = ?"
        )

    def _select_user_state_sql(self, *, for_update: bool = False) -> str:
        lock_hint = " WITH (UPDLOCK, HOLDLOCK)" if for_update else ""
        return (
            f"SELECT TOP (1) state FROM {mssql_table_ref(self._user_state_table)}{lock_hint} "
            "WHERE app_name = ? AND user_id = ?"
        )

    def _insert_user_state_sql(self) -> str:
        return (
            f"INSERT INTO {mssql_table_ref(self._user_state_table)} (app_name, user_id, state, update_time) "
            "VALUES (?, ?, ?, SYSDATETIMEOFFSET())"
        )

    def _update_user_state_sql(self) -> str:
        return (
            f"UPDATE {mssql_table_ref(self._user_state_table)} SET state = ?, update_time = SYSDATETIMEOFFSET() "
            "WHERE app_name = ? AND user_id = ?"
        )

    def _upsert_state_sql(self, table: str, key_columns: tuple[str, ...]) -> str:
        source_keys = ", ".join(f"? AS {quote_bracket_identifier(col)}" for col in key_columns)
        source_cols = f"{source_keys}, ? AS state"
        insert_cols = ", ".join(quote_bracket_identifier(col) for col in (*key_columns, "state", "update_time"))
        insert_vals = ", ".join(f"source.{quote_bracket_identifier(col)}" for col in (*key_columns, "state"))
        match_clause = " AND ".join(
            f"target.{quote_bracket_identifier(col)} = source.{quote_bracket_identifier(col)}" for col in key_columns
        )
        return (
            f"MERGE INTO {mssql_table_ref(table)} WITH (HOLDLOCK) AS target "
            f"USING (SELECT {source_cols}) AS source "
            f"ON ({match_clause}) "
            "WHEN MATCHED THEN UPDATE SET state = source.state, update_time = SYSDATETIMEOFFSET() "
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals}, SYSDATETIMEOFFSET());"
        )

    def _get_metadata_sql(self) -> str:
        return f"SELECT TOP (1) value FROM {mssql_table_ref(self._metadata_table)} WHERE [key] = ?"

    def _upsert_metadata_sql(self) -> str:
        return (
            f"MERGE INTO {mssql_table_ref(self._metadata_table)} WITH (HOLDLOCK) AS target "
            "USING (SELECT ? AS [key], ? AS value) AS source "
            "ON (target.[key] = source.[key]) "
            "WHEN MATCHED THEN UPDATE SET value = source.value "
            "WHEN NOT MATCHED THEN INSERT ([key], value) VALUES (source.[key], source.value);"
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


class MSSQLSyncADKStore(_MSSQLADKSessionSQLMixin, BaseSyncADKStore["MSSQLSyncConfig"]):
    """Synchronous SQL Server ADK session, event, app/user state, and metadata store."""

    __slots__ = ("_json_column_type", "_json_deserializer", "_json_serializer", "_native_json")

    adapter_name = "vertebra_mssql"
    _max_table_name_parts: ClassVar[int] = 2
    extension_config_options = _MSSQL_ADK_EXTENSION_OPTIONS

    def __init__(self, config: MSSQLSyncConfig, *, read_config: MSSQLSyncConfig | None = None) -> None:
        """Initialize the synchronous SQL Server ADK store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        self._native_json, self._json_column_type = _resolve_json_init(config)

    def prepare_schema_sync(self, driver: Any) -> None:
        """Resolve the SQL Server JSON column type on the supplied synchronous driver."""
        if self._json_column_type is None:
            self._json_column_type = _detect_json_type_sync(driver, self._native_json)

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Create missing ADK session and event companion indexes without committing."""
        index_specs = self._session_index_specs()
        existing = _existing_index_names_sync(driver, index_specs)
        for idx_name, idx_table, cols in index_specs:
            if _bare_index_name(idx_name) not in existing:
                driver.execute(_create_index_sql(idx_table, idx_name, cols))

    def _resolve_json_type(self) -> str:
        if self._json_column_type is not None:
            return self._json_column_type
        try:
            with self._config.provide_session() as driver:
                self._json_column_type = _detect_json_type_sync(driver, self._native_json)
        except Exception:
            return _JSON_FALLBACK_TYPE
        return self._json_column_type

    def _sessions_table_ddl(self) -> str:
        return self._build_sessions_ddl(self._resolve_json_type())

    def _events_table_ddl(self) -> str:
        return self._build_events_ddl(self._resolve_json_type())

    def _app_states_table_ddl(self) -> str:
        return self._build_app_states_ddl(self._resolve_json_type())

    def _user_states_table_ddl(self) -> str:
        return self._build_user_states_ddl(self._resolve_json_type())

    def _metadata_table_ddl(self) -> str:
        return self._build_metadata_ddl()

    def create_tables(self) -> None:
        """Create idempotent T-SQL ADK tables and missing indexes via DataDictionary inspection."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return
        with self._config.provide_session() as driver:
            self.prepare_schema_sync(driver)
            json_type = self._json_column_type or _JSON_FALLBACK_TYPE
            driver.execute_script(self._build_sessions_ddl(json_type))
            driver.execute_script(self._build_events_ddl(json_type))
            driver.execute_script(self._build_app_states_ddl(json_type))
            driver.execute_script(self._build_user_states_ddl(json_type))
            driver.execute_script(self._build_metadata_ddl())
            self.reconcile_companion_objects_sync(driver)
            driver.commit()
        self.reconcile_schema(assume_existing=True)

    def _select_app_state_sync(self, driver: Any, app_name: str) -> dict[str, Any] | None:
        row = driver.select_one_or_none(self._select_app_state_sql(), app_name)
        return (
            mssql_json_dict(mssql_row_value(row, "state", 0), deserializer=self._json_deserializer)
            if row is not None
            else None
        )

    def _select_user_state_sync(self, driver: Any, app_name: str, user_id: str) -> dict[str, Any] | None:
        row = driver.select_one_or_none(self._select_user_state_sql(), app_name, user_id)
        return (
            mssql_json_dict(mssql_row_value(row, "state", 0), deserializer=self._json_deserializer)
            if row is not None
            else None
        )

    def _apply_app_state_delta_sync(self, driver: Any, app_name: str, delta: Mapping[str, Any]) -> dict[str, Any]:
        row = driver.select_one_or_none(self._select_app_state_sql(for_update=True), app_name)
        if row is not None:
            current = mssql_json_dict(mssql_row_value(row, "state", 0), deserializer=self._json_deserializer)
            merged = {**current, **delta}
            driver.execute(self._update_app_state_sql(), self._json_serializer(merged), app_name)
            return merged
        merged = dict(delta)
        driver.execute(self._insert_app_state_sql(), app_name, self._json_serializer(merged))
        return merged

    def _apply_user_state_delta_sync(
        self,
        driver: Any,
        app_name: str,
        user_id: str,
        delta: Mapping[str, Any],
    ) -> dict[str, Any]:
        row = driver.select_one_or_none(self._select_user_state_sql(for_update=True), app_name, user_id)
        if row is not None:
            current = mssql_json_dict(mssql_row_value(row, "state", 0), deserializer=self._json_deserializer)
            merged = {**current, **delta}
            driver.execute(self._update_user_state_sql(), self._json_serializer(merged), app_name, user_id)
            return merged
        merged = dict(delta)
        driver.execute(self._insert_user_state_sql(), app_name, user_id, self._json_serializer(merged))
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
        """Insert a new ADK session row atomically, apply state deltas, and return the merged record."""
        insert_sql, insert_params = self._create_session_sql_and_params(session_id, app_name, user_id, state, owner_id)
        select_sql = self._get_session_select_sql()
        with self._config.provide_session() as driver:
            driver.begin()
            try:
                driver.execute(insert_sql, *insert_params)
                row = driver.select_one_or_none(select_sql, app_name, user_id, session_id)
                if row is None:
                    msg = f"Failed to reload newly created MSSQL ADK session {session_id}"
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
        """Fetch an ADK session by composite key and optionally refresh ``update_time``."""
        should_renew = self._is_renewal_requested(renew_for)
        try:
            with self._provide_replica_session(primary=should_renew) as driver:
                if should_renew:
                    driver.execute(self._get_session_touch_sql(), app_name, user_id, session_id)
                    driver.commit()
                row = driver.select_one_or_none(self._get_session_select_sql(), app_name, user_id, session_id)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return None
            raise
        return _row_to_session(row, deserializer=self._json_deserializer) if row is not None else None

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Replace durable JSON state on an ADK session row."""
        with self._config.provide_session() as driver:
            driver.execute(
                self._update_session_state_sql(),
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
        order_by: SessionOrderBy = "update_time",
        descending: bool = True,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[StoredSession]:
        """List ADK sessions with ``OFFSET ... FETCH NEXT`` T-SQL pagination."""
        built = self._list_sessions_sql_and_params(app_name, user_id, order_by, descending, limit, offset)
        if built is None:
            return []
        sql, params = built
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, *params)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return []
            raise
        return [_row_to_session(row, deserializer=self._json_deserializer) for row in rows]

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete an ADK session row (cascading to events)."""
        with self._config.provide_session() as driver:
            driver.execute(self._delete_session_sql(), app_name, user_id, session_id)
            driver.commit()

    def append_event(self, event_record: StoredEvent) -> None:
        """Append an ADK event record after verifying owning session existence."""
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
        """Atomically update session state, append event, and merge scoped state deltas."""
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
        """Return ordered ADK events for a session."""
        if limit == 0:
            return []
        sql, params = self._get_events_sql_and_params(app_name, user_id, session_id, after_timestamp, limit)
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, *params)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return []
            raise
        if limit is not None:
            rows = list(reversed(rows))
        return [_row_to_event(row, deserializer=self._json_deserializer) for row in rows]

    def _delete_before(self, table: str, column: str, threshold: datetime, app_name: str | None) -> int:
        sql, params = self._delete_before_sql_and_params(table, column, threshold, app_name)
        try:
            with self._config.provide_session() as driver:
                res = driver.execute(sql, *params)
                driver.commit()
                return int(res.rows_affected or 0)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return 0
            raise

    def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Delete events older than ``before``."""
        return self._delete_before(self._events_table, "timestamp", before, app_name)

    def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete idle sessions older than ``updated_before``."""
        return self._delete_before(self._session_table, "update_time", updated_before, app_name)

    def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete idle user states older than ``updated_before``."""
        return self._delete_before(self._user_state_table, "update_time", updated_before, app_name)

    def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Fetch app-scoped JSON state for ``app_name``."""
        try:
            with self._config.provide_session() as driver:
                return self._select_app_state_sync(driver, app_name)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return None
            raise

    def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Fetch user-scoped JSON state for ``(app_name, user_id)``."""
        try:
            with self._config.provide_session() as driver:
                return self._select_user_state_sync(driver, app_name, user_id)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return None
            raise

    def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Merge app-scoped JSON state using ``MERGE ... WITH (HOLDLOCK)``."""
        with self._config.provide_session() as driver:
            driver.execute(
                self._upsert_state_sql(self._app_state_table, ("app_name",)),
                app_name,
                self._json_serializer(state),
            )
            driver.commit()

    def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Merge user-scoped JSON state using ``MERGE ... WITH (HOLDLOCK)``."""
        with self._config.provide_session() as driver:
            driver.execute(
                self._upsert_state_sql(self._user_state_table, ("app_name", "user_id")),
                app_name,
                user_id,
                self._json_serializer(state),
            )
            driver.commit()

    def get_metadata(self, key: str) -> str | None:
        """Return a metadata value by key."""
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(self._get_metadata_sql(), key)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return None
            raise
        val = mssql_row_value(row, "value", 0) if row is not None else None
        return str(val) if val is not None else None

    def set_metadata(self, key: str, value: str) -> None:
        """Upsert a metadata key-value entry via ``MERGE ... WITH (HOLDLOCK)``."""
        with self._config.provide_session() as driver:
            driver.execute(self._upsert_metadata_sql(), key, value)
            driver.commit()


class MSSQLAsyncADKStore(_MSSQLADKSessionSQLMixin, BaseAsyncADKStore["MSSQLAsyncConfig"]):
    """Asynchronous SQL Server ADK session, event, app/user state, and metadata store."""

    __slots__ = ("_json_column_type", "_json_deserializer", "_json_serializer", "_native_json")

    adapter_name = "vertebra_mssql"
    _max_table_name_parts: ClassVar[int] = 2
    extension_config_options = _MSSQL_ADK_EXTENSION_OPTIONS

    def __init__(self, config: MSSQLAsyncConfig, *, read_config: MSSQLAsyncConfig | None = None) -> None:
        """Initialize the asynchronous SQL Server ADK store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        self._native_json, self._json_column_type = _resolve_json_init(config)

    async def prepare_schema_async(self, driver: Any) -> None:
        """Resolve the SQL Server JSON column type on the supplied asynchronous driver."""
        if self._json_column_type is None:
            self._json_column_type = await _detect_json_type_async(driver, self._native_json)

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Create missing ADK session and event companion indexes asynchronously without committing."""
        index_specs = self._session_index_specs()
        existing = await _existing_index_names_async(driver, index_specs)
        for idx_name, idx_table, cols in index_specs:
            if _bare_index_name(idx_name) not in existing:
                await driver.execute(_create_index_sql(idx_table, idx_name, cols))

    async def _resolve_json_type(self) -> str:
        if self._json_column_type is not None:
            return self._json_column_type
        try:
            async with self._config.provide_session() as driver:
                self._json_column_type = await _detect_json_type_async(driver, self._native_json)
        except Exception:
            return _JSON_FALLBACK_TYPE
        return self._json_column_type

    async def _sessions_table_ddl(self) -> str:
        return self._build_sessions_ddl(await self._resolve_json_type())

    async def _events_table_ddl(self) -> str:
        return self._build_events_ddl(await self._resolve_json_type())

    async def _app_states_table_ddl(self) -> str:
        return self._build_app_states_ddl(await self._resolve_json_type())

    async def _user_states_table_ddl(self) -> str:
        return self._build_user_states_ddl(await self._resolve_json_type())

    async def _metadata_table_ddl(self) -> str:
        return self._build_metadata_ddl()

    async def create_tables(self) -> None:
        """Create idempotent T-SQL ADK tables and missing indexes asynchronously."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        async with self._config.provide_session() as driver:
            await self.prepare_schema_async(driver)
            json_type = self._json_column_type or _JSON_FALLBACK_TYPE
            await driver.execute_script(self._build_sessions_ddl(json_type))
            await driver.execute_script(self._build_events_ddl(json_type))
            await driver.execute_script(self._build_app_states_ddl(json_type))
            await driver.execute_script(self._build_user_states_ddl(json_type))
            await driver.execute_script(self._build_metadata_ddl())
            await self.reconcile_companion_objects_async(driver)
            await driver.commit()
        await self.reconcile_schema(assume_existing=True)

    async def _select_app_state_async(self, driver: Any, app_name: str) -> dict[str, Any] | None:
        row = await driver.select_one_or_none(self._select_app_state_sql(), app_name)
        return (
            mssql_json_dict(mssql_row_value(row, "state", 0), deserializer=self._json_deserializer)
            if row is not None
            else None
        )

    async def _select_user_state_async(self, driver: Any, app_name: str, user_id: str) -> dict[str, Any] | None:
        row = await driver.select_one_or_none(self._select_user_state_sql(), app_name, user_id)
        return (
            mssql_json_dict(mssql_row_value(row, "state", 0), deserializer=self._json_deserializer)
            if row is not None
            else None
        )

    async def _apply_app_state_delta_async(
        self, driver: Any, app_name: str, delta: Mapping[str, Any]
    ) -> dict[str, Any]:
        row = await driver.select_one_or_none(self._select_app_state_sql(for_update=True), app_name)
        if row is not None:
            current = mssql_json_dict(mssql_row_value(row, "state", 0), deserializer=self._json_deserializer)
            merged = {**current, **delta}
            await driver.execute(self._update_app_state_sql(), self._json_serializer(merged), app_name)
            return merged
        merged = dict(delta)
        await driver.execute(self._insert_app_state_sql(), app_name, self._json_serializer(merged))
        return merged

    async def _apply_user_state_delta_async(
        self,
        driver: Any,
        app_name: str,
        user_id: str,
        delta: Mapping[str, Any],
    ) -> dict[str, Any]:
        row = await driver.select_one_or_none(self._select_user_state_sql(for_update=True), app_name, user_id)
        if row is not None:
            current = mssql_json_dict(mssql_row_value(row, "state", 0), deserializer=self._json_deserializer)
            merged = {**current, **delta}
            await driver.execute(self._update_user_state_sql(), self._json_serializer(merged), app_name, user_id)
            return merged
        merged = dict(delta)
        await driver.execute(self._insert_user_state_sql(), app_name, user_id, self._json_serializer(merged))
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
        """Insert a new ADK session row asynchronously, apply state deltas, and return the merged record."""
        insert_sql, insert_params = self._create_session_sql_and_params(session_id, app_name, user_id, state, owner_id)
        select_sql = self._get_session_select_sql()
        async with self._config.provide_session() as driver:
            await driver.begin()
            try:
                await driver.execute(insert_sql, *insert_params)
                row = await driver.select_one_or_none(select_sql, app_name, user_id, session_id)
                if row is None:
                    msg = f"Failed to reload newly created MSSQL ADK session {session_id}"
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
        """Fetch an ADK session asynchronously by composite key."""
        should_renew = self._is_renewal_requested(renew_for)
        try:
            async with self._provide_replica_session(primary=should_renew) as driver:
                if should_renew:
                    await driver.execute(self._get_session_touch_sql(), app_name, user_id, session_id)
                    await driver.commit()
                row = await driver.select_one_or_none(self._get_session_select_sql(), app_name, user_id, session_id)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return None
            raise
        return _row_to_session(row, deserializer=self._json_deserializer) if row is not None else None

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Replace session state asynchronously."""
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
        """List ADK sessions asynchronously with ``OFFSET ... FETCH NEXT`` pagination."""
        built = self._list_sessions_sql_and_params(app_name, user_id, order_by, descending, limit, offset)
        if built is None:
            return []
        sql, params = built
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, *params)
        except Exception as exc:
            if is_mssql_table_missing(exc):
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
        """Atomically update session state, append event, and merge scoped state deltas asynchronously."""
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
        """Return ordered ADK events for a session asynchronously."""
        if limit == 0:
            return []
        sql, params = self._get_events_sql_and_params(app_name, user_id, session_id, after_timestamp, limit)
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, *params)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return []
            raise
        if limit is not None:
            rows = list(reversed(rows))
        return [_row_to_event(row, deserializer=self._json_deserializer) for row in rows]

    async def _delete_before(self, table: str, column: str, threshold: datetime, app_name: str | None) -> int:
        sql, params = self._delete_before_sql_and_params(table, column, threshold, app_name)
        try:
            async with self._config.provide_session() as driver:
                res = await driver.execute(sql, *params)
                await driver.commit()
                return int(res.rows_affected or 0)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return 0
            raise

    async def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Delete events older than ``before`` asynchronously."""
        return await self._delete_before(self._events_table, "timestamp", before, app_name)

    async def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete idle sessions older than ``updated_before`` asynchronously."""
        return await self._delete_before(self._session_table, "update_time", updated_before, app_name)

    async def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete idle user states older than ``updated_before`` asynchronously."""
        return await self._delete_before(self._user_state_table, "update_time", updated_before, app_name)

    async def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Fetch app-scoped state asynchronously."""
        try:
            async with self._config.provide_session() as driver:
                return await self._select_app_state_async(driver, app_name)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return None
            raise

    async def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Fetch user-scoped state asynchronously."""
        try:
            async with self._config.provide_session() as driver:
                return await self._select_user_state_async(driver, app_name, user_id)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return None
            raise

    async def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Merge app-scoped JSON state asynchronously."""
        async with self._config.provide_session() as driver:
            await driver.execute(
                self._upsert_state_sql(self._app_state_table, ("app_name",)),
                app_name,
                self._json_serializer(state),
            )
            await driver.commit()

    async def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Merge user-scoped JSON state asynchronously."""
        async with self._config.provide_session() as driver:
            await driver.execute(
                self._upsert_state_sql(self._user_state_table, ("app_name", "user_id")),
                app_name,
                user_id,
                self._json_serializer(state),
            )
            await driver.commit()

    async def get_metadata(self, key: str) -> str | None:
        """Fetch metadata table value asynchronously."""
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(self._get_metadata_sql(), key)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return None
            raise
        val = mssql_row_value(row, "value", 0) if row is not None else None
        return str(val) if val is not None else None

    async def set_metadata(self, key: str, value: str) -> None:
        """Upsert a metadata key-value entry asynchronously."""
        async with self._config.provide_session() as driver:
            await driver.execute(self._upsert_metadata_sql(), key, value)
            await driver.commit()


class _MSSQLADKMemorySQLMixin:
    """Shared T-SQL DDL and query builders for synchronous and asynchronous ADK memory stores."""

    __slots__ = ()

    _config: Any
    _enabled: bool
    _memory_table: str
    _use_fts: bool
    _max_results: int
    _owner_id_column_ddl: str | None
    _owner_id_column_name: str | None
    _json_serializer: Any
    _json_deserializer: Any
    _native_json: bool | None
    _json_column_type: str | None

    def _build_memory_ddl(self, json_col_type: str) -> str:
        owner_line = f",\n        {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        schema_name, table_name = split_schema_and_table(self._memory_table)
        return (
            "IF NOT EXISTS ("
            f"SELECT 1 FROM sys.tables WHERE name = N'{escape_sql_literal(table_name)}' "
            f"AND schema_id = SCHEMA_ID(N'{escape_sql_literal(schema_name)}')"
            ")\nBEGIN\n"
            f"    CREATE TABLE {mssql_table_ref(self._memory_table)} (\n"
            "        id NVARCHAR(128) NOT NULL,\n"
            "        session_id NVARCHAR(128) NOT NULL,\n"
            "        app_name NVARCHAR(128) NOT NULL,\n"
            "        user_id NVARCHAR(128) NOT NULL,\n"
            f"        scope NVARCHAR(16) NOT NULL CONSTRAINT {mssql_constraint_ref('df', self._memory_table, 'scope')} DEFAULT N'user',\n"
            "        event_id NVARCHAR(128) NOT NULL,\n"
            "        author NVARCHAR(256) NULL,\n"
            "        timestamp DATETIMEOFFSET(6) NOT NULL,\n"
            f"        content_json {json_col_type} NOT NULL,\n"
            "        content_text NVARCHAR(MAX) NOT NULL,\n"
            f"        metadata_json {json_col_type} NULL,\n"
            f"        inserted_at DATETIMEOFFSET(6) NOT NULL CONSTRAINT {mssql_constraint_ref('df', self._memory_table, 'inserted_at')} DEFAULT SYSDATETIMEOFFSET(){owner_line},\n"
            f"        CONSTRAINT {mssql_constraint_ref('pk', self._memory_table, 'id')} PRIMARY KEY (id),\n"
            f"        CONSTRAINT {mssql_constraint_ref('uq', self._memory_table, 'event_id')} UNIQUE (app_name, user_id, event_id)\n"
            "    );\n"
            "END;"
        )

    def _memory_index_specs(self) -> list[tuple[str, str, str]]:
        _, table_name = split_schema_and_table(self._memory_table)
        return [
            (f"idx_{table_name}_app_scope_user_time", self._memory_table, "app_name, scope, user_id, timestamp DESC"),
            (f"idx_{table_name}_scope", self._memory_table, "app_name, scope"),
            (f"idx_{table_name}_session", self._memory_table, "session_id"),
            (f"idx_{table_name}_timestamp", self._memory_table, "timestamp DESC"),
        ]

    def _drop_memory_table_sql(self) -> list[str]:
        return [_drop_if_exists_sql(self._memory_table)]

    def _build_memory_insert_statement(
        self,
        entries: list[StoredMemory],
        owner_id: object | None,
    ) -> tuple[str, list[tuple[Any, ...]]]:
        owner_col = f", {quote_bracket_identifier(self._owner_id_column_name)}" if self._owner_id_column_name else ""
        owner_param = ", ?" if self._owner_id_column_name else ""
        table_ref = mssql_table_ref(self._memory_table)
        sql = (
            f"INSERT INTO {table_ref} "
            f"(id, session_id, app_name, user_id, scope, event_id, author, timestamp, content_json, content_text, metadata_json{owner_col}) "
            f"SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?{owner_param} "
            f"WHERE NOT EXISTS (SELECT 1 FROM {table_ref} WITH (UPDLOCK, HOLDLOCK) "
            "WHERE app_name = ? AND user_id = ? AND event_id = ?)"
        )
        batch: list[tuple[Any, ...]] = []
        for entry in entries:
            meta = entry.get("metadata_json")
            row_params: tuple[Any, ...] = (
                entry["id"],
                entry["session_id"],
                entry["app_name"],
                entry["user_id"],
                entry.get("scope", "user"),
                entry["event_id"],
                entry.get("author"),
                mssql_ensure_utc(entry["timestamp"]),
                self._json_serializer(entry["content_json"]),
                entry["content_text"],
                self._json_serializer(meta) if meta is not None else None,
            )
            if self._owner_id_column_name:
                row_params = (*row_params, owner_id)
            batch.append((*row_params, entry["app_name"], entry["user_id"], entry["event_id"]))
        return sql, batch

    def _build_memory_search_query(
        self,
        query: str,
        app_name: str,
        user_id: str,
        limit_value: int,
        scope_filter: Literal["all", "user", "app"],
    ) -> tuple[str, tuple[Any, ...]]:
        where_scope, scope_params = build_mssql_scope_where(app_name, user_id, scope_filter)
        escaped = escape_mssql_like(query)
        sql = (
            "SELECT TOP (?) id, session_id, app_name, user_id, scope, event_id, author, timestamp, "
            "content_json, content_text, metadata_json, inserted_at "
            f"FROM {mssql_table_ref(self._memory_table)} "
            f"WHERE {where_scope} AND content_text LIKE ? ESCAPE N'\\' ORDER BY timestamp DESC"
        )
        return sql, (limit_value, *scope_params, f"%{escaped}%")

    def _delete_by_session_sql_and_params(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
    ) -> tuple[str, tuple[str, str, str]]:
        sql = f"DELETE FROM {mssql_table_ref(self._memory_table)} WHERE session_id = ? AND app_name = ? AND user_id = ?"
        return sql, (session_id, app_name, user_id)

    def _delete_older_than_sql_and_params(
        self,
        days: int,
        app_name: str | None,
        scope: str | None,
    ) -> tuple[str, tuple[Any, ...]]:
        clauses = ["inserted_at < DATEADD(day, -?, SYSDATETIMEOFFSET())"]
        params: list[Any] = [int(days)]
        if app_name is not None:
            clauses.append("app_name = ?")
            params.append(app_name)
        if scope is not None:
            clauses.append("scope = ?")
            params.append(scope)
        sql = f"DELETE FROM {mssql_table_ref(self._memory_table)} WHERE {' AND '.join(clauses)}"
        return sql, tuple(params)


class MSSQLSyncADKMemoryStore(_MSSQLADKMemorySQLMixin, _BaseSyncADKMemoryStore["MSSQLSyncConfig"]):
    """Synchronous SQL Server ADK memory store with ``UPDLOCK, HOLDLOCK`` deduplication and ``LIKE`` search."""

    __slots__ = ("_json_column_type", "_json_deserializer", "_json_serializer", "_native_json")

    adapter_name = "vertebra_mssql"
    _max_table_name_parts: ClassVar[int] = 2
    extension_config_options = _MSSQL_ADK_EXTENSION_OPTIONS

    def __init__(self, config: MSSQLSyncConfig, *, read_config: MSSQLSyncConfig | None = None) -> None:
        """Initialize the synchronous SQL Server ADK memory store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        self._native_json, self._json_column_type = _resolve_json_init(config)
        adk_cfg = _adk_settings(config)
        if "memory_use_fts" not in adk_cfg and "use_fts" in adk_cfg:
            self._use_fts = bool(adk_cfg["use_fts"])

    def prepare_schema_sync(self, driver: Any) -> None:
        """Resolve the SQL Server JSON column type for enabled memory stores on the supplied driver."""
        if self._enabled and self._json_column_type is None:
            self._json_column_type = _detect_json_type_sync(driver, self._native_json)

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Create missing ADK memory companion indexes without committing when enabled."""
        if not self._enabled:
            return
        index_specs = self._memory_index_specs()
        existing = _existing_index_names_sync(driver, index_specs)
        for idx_name, idx_table, cols in index_specs:
            if _bare_index_name(idx_name) not in existing:
                driver.execute(_create_index_sql(idx_table, idx_name, cols))

    def _memory_table_ddl(self) -> str:
        return self._build_memory_ddl(self._json_column_type or _JSON_FALLBACK_TYPE)

    def create_tables(self) -> None:
        """Create idempotent T-SQL memory table and missing indexes."""
        if not self._enabled:
            return
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return
        with self._config.provide_session() as driver:
            self.prepare_schema_sync(driver)
            driver.execute_script(self._memory_table_ddl())
            self.reconcile_companion_objects_sync(driver)
            driver.commit()
        self.reconcile_schema(assume_existing=True)

    def insert_memory_entries(self, entries: list[StoredMemory], owner_id: object | None = None) -> int:
        """Bulk-insert memory entries with ``UPDLOCK, HOLDLOCK`` deduplication."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0
        sql, batch = self._build_memory_insert_statement(entries, owner_id)
        with self._config.provide_session() as driver:
            res = driver.execute_many(sql, batch)
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
        """Search memory entries matching ``query``."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if embedding is not None:
            msg = "Vector embeddings are not configured for MSSQLSyncADKMemoryStore"
            raise ImproperConfigurationError(msg)
        if not query or not query.strip():
            return []
        limit_value = limit if limit is not None else self._max_results
        if limit_value <= 0:
            return []
        sql, params = self._build_memory_search_query(query, app_name, user_id, limit_value, scope_filter)
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, *params)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return []
            raise
        return [_row_to_memory(row, deserializer=self._json_deserializer) for row in rows[:limit_value]]

    def delete_entries_by_session(self, session_id: str, app_name: str, user_id: str) -> int:
        """Delete memory entries for ``session_id`` scoped to ``app_name`` and ``user_id``."""
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
            if is_mssql_table_missing(exc):
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
            if is_mssql_table_missing(exc):
                return 0
            raise


class MSSQLAsyncADKMemoryStore(_MSSQLADKMemorySQLMixin, _BaseAsyncADKMemoryStore["MSSQLAsyncConfig"]):
    """Asynchronous SQL Server ADK memory store."""

    __slots__ = ("_json_column_type", "_json_deserializer", "_json_serializer", "_native_json")

    adapter_name = "vertebra_mssql"
    _max_table_name_parts: ClassVar[int] = 2
    extension_config_options = _MSSQL_ADK_EXTENSION_OPTIONS

    def __init__(self, config: MSSQLAsyncConfig, *, read_config: MSSQLAsyncConfig | None = None) -> None:
        """Initialize the asynchronous SQL Server ADK memory store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        self._native_json, self._json_column_type = _resolve_json_init(config)
        adk_cfg = _adk_settings(config)
        if "memory_use_fts" not in adk_cfg and "use_fts" in adk_cfg:
            self._use_fts = bool(adk_cfg["use_fts"])

    async def prepare_schema_async(self, driver: Any) -> None:
        """Resolve the SQL Server JSON column type asynchronously for enabled memory stores."""
        if self._enabled and self._json_column_type is None:
            self._json_column_type = await _detect_json_type_async(driver, self._native_json)

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Create missing ADK memory companion indexes asynchronously without committing when enabled."""
        if not self._enabled:
            return
        index_specs = self._memory_index_specs()
        existing = await _existing_index_names_async(driver, index_specs)
        for idx_name, idx_table, cols in index_specs:
            if _bare_index_name(idx_name) not in existing:
                await driver.execute(_create_index_sql(idx_table, idx_name, cols))

    async def _memory_table_ddl(self) -> str:
        return self._build_memory_ddl(self._json_column_type or _JSON_FALLBACK_TYPE)

    async def create_tables(self) -> None:
        """Create idempotent T-SQL memory table and missing indexes asynchronously."""
        if not self._enabled:
            return
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        async with self._config.provide_session() as driver:
            await self.prepare_schema_async(driver)
            await driver.execute_script(await self._memory_table_ddl())
            await self.reconcile_companion_objects_async(driver)
            await driver.commit()
        await self.reconcile_schema(assume_existing=True)

    async def insert_memory_entries(self, entries: list[StoredMemory], owner_id: object | None = None) -> int:
        """Bulk-insert memory entries asynchronously with ``UPDLOCK, HOLDLOCK`` deduplication."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0
        sql, batch = self._build_memory_insert_statement(entries, owner_id)
        async with self._config.provide_session() as driver:
            res = await driver.execute_many(sql, batch)
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
        """Search memory entries asynchronously."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if embedding is not None:
            msg = "Vector embeddings are not configured for MSSQLAsyncADKMemoryStore"
            raise ImproperConfigurationError(msg)
        if not query or not query.strip():
            return []
        limit_value = limit if limit is not None else self._max_results
        if limit_value <= 0:
            return []
        sql, params = self._build_memory_search_query(query, app_name, user_id, limit_value, scope_filter)
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, *params)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return []
            raise
        return [_row_to_memory(row, deserializer=self._json_deserializer) for row in rows[:limit_value]]

    async def delete_entries_by_session(self, session_id: str, app_name: str, user_id: str) -> int:
        """Delete memory entries for ``session_id`` scoped to ``app_name`` and ``user_id`` asynchronously."""
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
            if is_mssql_table_missing(exc):
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
            if is_mssql_table_missing(exc):
                return 0
            raise


class _MSSQLADKArtifactSQLMixin:
    """Shared T-SQL DDL and query builders for synchronous and asynchronous ADK artifact stores."""

    __slots__ = ()

    _config: Any
    _artifact_table: str
    _json_serializer: Any
    _json_deserializer: Any
    _native_json: bool | None
    _json_column_type: str | None

    def _build_artifact_ddl(self, json_col_type: str) -> str:
        schema_name, table_name = split_schema_and_table(self._artifact_table)
        return (
            "IF NOT EXISTS ("
            f"SELECT 1 FROM sys.tables WHERE name = N'{escape_sql_literal(table_name)}' "
            f"AND schema_id = SCHEMA_ID(N'{escape_sql_literal(schema_name)}')"
            ")\nBEGIN\n"
            f"    CREATE TABLE {mssql_table_ref(self._artifact_table)} (\n"
            "        app_name NVARCHAR(128) NOT NULL,\n"
            "        user_id NVARCHAR(128) NOT NULL,\n"
            "        session_id NVARCHAR(128) NOT NULL DEFAULT N'',\n"
            "        filename NVARCHAR(512) NOT NULL,\n"
            "        version INT NOT NULL,\n"
            "        mime_type NVARCHAR(255) NULL,\n"
            "        canonical_uri NVARCHAR(MAX) NOT NULL,\n"
            f"        custom_metadata {json_col_type} NULL,\n"
            "        size BIGINT NULL,\n"
            "        updated DATETIMEOFFSET(6) NULL,\n"
            "        signed_url NVARCHAR(MAX) NULL,\n"
            f"        created_at DATETIMEOFFSET(6) NOT NULL CONSTRAINT {mssql_constraint_ref('df', self._artifact_table, 'created_at')} DEFAULT SYSDATETIMEOFFSET(),\n"
            f"        CONSTRAINT {mssql_constraint_ref('pk', self._artifact_table, 'artifact')} PRIMARY KEY (app_name, user_id, session_id, filename, version)\n"
            "    );\n"
            "END;"
        )

    def _insert_artifact_sql_and_params(self, record: StoredArtifact) -> tuple[str, tuple[Any, ...]]:
        session_val = record["session_id"] if record["session_id"] is not None else ""
        meta = record.get("custom_metadata")
        sql = (
            f"MERGE INTO {mssql_table_ref(self._artifact_table)} WITH (HOLDLOCK) AS target "
            "USING (SELECT ? AS app_name, ? AS user_id, ? AS session_id, ? AS filename, ? AS version, "
            "? AS mime_type, ? AS canonical_uri, ? AS custom_metadata, ? AS created_at) AS src "
            "ON (target.app_name = src.app_name AND target.user_id = src.user_id "
            "AND target.session_id = src.session_id AND target.filename = src.filename AND target.version = src.version) "
            "WHEN MATCHED THEN UPDATE SET mime_type = src.mime_type, canonical_uri = src.canonical_uri, "
            "custom_metadata = src.custom_metadata "
            "WHEN NOT MATCHED THEN INSERT (app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at) "
            "VALUES (src.app_name, src.user_id, src.session_id, src.filename, src.version, src.mime_type, src.canonical_uri, src.custom_metadata, src.created_at);"
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
            mssql_ensure_utc(record["created_at"]),
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
            f"MERGE INTO {mssql_table_ref(self._artifact_table)} WITH (HOLDLOCK) AS target "
            "USING (SELECT ? AS app_name, ? AS user_id, ? AS session_id, ? AS filename, ? AS version, "
            "? AS mime_type, ? AS canonical_uri, ? AS custom_metadata, ? AS size, ? AS updated, ? AS signed_url, ? AS created_at) AS src "
            "ON (target.app_name = src.app_name AND target.user_id = src.user_id "
            "AND target.session_id = src.session_id AND target.filename = src.filename AND target.version = src.version) "
            "WHEN MATCHED THEN UPDATE SET mime_type = src.mime_type, canonical_uri = src.canonical_uri, "
            "custom_metadata = src.custom_metadata, size = src.size, updated = src.updated, signed_url = src.signed_url "
            "WHEN NOT MATCHED THEN INSERT (app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, size, updated, signed_url, created_at) "
            "VALUES (src.app_name, src.user_id, src.session_id, src.filename, src.version, src.mime_type, src.canonical_uri, src.custom_metadata, src.size, src.updated, src.signed_url, src.created_at);"
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
            mssql_ensure_utc(reference.updated) if reference.updated is not None else None,
            reference.signed_url,
            mssql_ensure_utc(now_dt),
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
        table_ref = mssql_table_ref(self._artifact_table)
        cols = (
            "*"
            if include_ref_cols
            else "app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at"
        )
        if version is not None:
            sql = (
                f"SELECT TOP (1) {cols} FROM {table_ref} "
                "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? AND version = ?"
            )
            return sql, (app_name, user_id, session_val, filename, version)
        sql = (
            f"SELECT TOP (1) {cols} FROM {table_ref} "
            "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? ORDER BY version DESC"
        )
        return sql, (app_name, user_id, session_val, filename)

    def _list_artifact_keys_sql_and_params(
        self,
        app_name: str,
        user_id: str,
        session_id: str | None,
    ) -> tuple[str, tuple[Any, ...]]:
        table_ref = mssql_table_ref(self._artifact_table)
        if session_id is not None:
            sql = (
                f"SELECT DISTINCT filename FROM {table_ref} "
                "WHERE app_name = ? AND user_id = ? AND session_id IN (N'', ?) ORDER BY filename ASC"
            )
            return sql, (app_name, user_id, session_id)
        sql = (
            f"SELECT DISTINCT filename FROM {table_ref} "
            "WHERE app_name = ? AND user_id = ? AND session_id = N'' ORDER BY filename ASC"
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
            f"FROM {mssql_table_ref(self._artifact_table)} "
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
            f"DELETE FROM {mssql_table_ref(self._artifact_table)} "
            "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ?"
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
            f"SELECT MAX(version) FROM {mssql_table_ref(self._artifact_table)} "
            "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ?"
        )
        return sql, (app_name, user_id, session_val, filename)

    def _delete_older_than_sqls_and_params(
        self,
        before: datetime,
        app_name: str | None,
    ) -> tuple[str, str, tuple[Any, ...]]:
        cutoff = mssql_ensure_utc(before)
        table_ref = mssql_table_ref(self._artifact_table)
        cols = "app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at"
        if app_name is not None:
            select_sql = f"SELECT {cols} FROM {table_ref} WHERE created_at < ? AND app_name = ? ORDER BY created_at ASC"
            delete_sql = f"DELETE FROM {table_ref} WHERE created_at < ? AND app_name = ?"
            return select_sql, delete_sql, (cutoff, app_name)
        select_sql = f"SELECT {cols} FROM {table_ref} WHERE created_at < ? ORDER BY created_at ASC"
        delete_sql = f"DELETE FROM {table_ref} WHERE created_at < ?"
        return select_sql, delete_sql, (cutoff,)


class MSSQLSyncADKArtifactStore(_MSSQLADKArtifactSQLMixin, _BaseSyncADKArtifactStore["MSSQLSyncConfig"]):
    """Synchronous SQL Server ADK artifact metadata store supporting ``ArtifactReference``."""

    __slots__ = ("_json_column_type", "_json_deserializer", "_json_serializer", "_native_json")

    adapter_name = "vertebra_mssql"
    extension_config_options = _MSSQL_ADK_EXTENSION_OPTIONS

    def __init__(self, config: MSSQLSyncConfig, *, read_config: MSSQLSyncConfig | None = None) -> None:
        """Initialize the synchronous SQL Server ADK artifact store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        self._native_json, self._json_column_type = _resolve_json_init(config)

    def prepare_schema_sync(self, driver: Any) -> None:
        """Resolve the SQL Server JSON column type on the supplied synchronous driver."""
        if self._json_column_type is None:
            self._json_column_type = _detect_json_type_sync(driver, self._native_json)

    def create_table(self) -> None:
        """Create the SQL Server artifact metadata table if absent."""
        with self._config.provide_session() as driver:
            self.prepare_schema_sync(driver)
            driver.execute_script(self._build_artifact_ddl(self._json_column_type or _JSON_FALLBACK_TYPE))
            driver.commit()

    def insert_artifact(self, record: StoredArtifact) -> None:
        """Insert or merge a ``StoredArtifact`` revision."""
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
            if is_mssql_table_missing(exc):
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
            if is_mssql_table_missing(exc):
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
            if is_mssql_table_missing(exc):
                return []
            raise
        return [str(mssql_row_value(row, "filename", 0)) for row in rows]

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
            if is_mssql_table_missing(exc):
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
        """Delete all versions for an artifact atomically and return the deleted records."""
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
            if is_mssql_table_missing(exc):
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
            if is_mssql_table_missing(exc):
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
            if is_mssql_table_missing(exc):
                return []
            raise
        return [_row_to_stored_artifact(row, deserializer=self._json_deserializer) for row in rows]


class MSSQLAsyncADKArtifactStore(_MSSQLADKArtifactSQLMixin, _BaseAsyncADKArtifactStore["MSSQLAsyncConfig"]):
    """Asynchronous SQL Server ADK artifact metadata store supporting ``ArtifactReference``."""

    __slots__ = ("_json_column_type", "_json_deserializer", "_json_serializer", "_native_json")

    adapter_name = "vertebra_mssql"
    extension_config_options = _MSSQL_ADK_EXTENSION_OPTIONS

    def __init__(self, config: MSSQLAsyncConfig, *, read_config: MSSQLAsyncConfig | None = None) -> None:
        """Initialize the asynchronous SQL Server ADK artifact store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        self._native_json, self._json_column_type = _resolve_json_init(config)

    async def prepare_schema_async(self, driver: Any) -> None:
        """Resolve the SQL Server JSON column type on the supplied asynchronous driver."""
        if self._json_column_type is None:
            self._json_column_type = await _detect_json_type_async(driver, self._native_json)

    async def create_table(self) -> None:
        """Create the SQL Server artifact metadata table asynchronously."""
        async with self._config.provide_session() as driver:
            await self.prepare_schema_async(driver)
            await driver.execute_script(self._build_artifact_ddl(self._json_column_type or _JSON_FALLBACK_TYPE))
            await driver.commit()

    async def insert_artifact(self, record: StoredArtifact) -> None:
        """Insert or merge a ``StoredArtifact`` revision asynchronously."""
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
        """Upsert an artifact revision asynchronously backed by ``ArtifactReference``."""
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
        """Fetch an ``ArtifactReference`` revision asynchronously by exact version or latest."""
        sql, params = self._get_artifact_sql_and_params(
            app_name, user_id, filename, session_id, version, include_ref_cols=True
        )
        try:
            async with self._provide_replica_session(primary=False) as driver:
                row = await driver.select_one_or_none(sql, *params)
        except Exception as exc:
            if is_mssql_table_missing(exc):
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
        """Fetch a ``StoredArtifact`` revision asynchronously by exact version or latest."""
        sql, params = self._get_artifact_sql_and_params(
            app_name, user_id, filename, session_id, version, include_ref_cols=False
        )
        try:
            async with self._provide_replica_session(primary=False) as driver:
                row = await driver.select_one_or_none(sql, *params)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return None
            raise
        return _row_to_stored_artifact(row, deserializer=self._json_deserializer) if row is not None else None

    async def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """List distinct artifact filenames asynchronously within user or session scope."""
        sql, params = self._list_artifact_keys_sql_and_params(app_name, user_id, session_id)
        try:
            async with self._provide_replica_session(primary=False) as driver:
                rows = await driver.select(sql, *params)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return []
            raise
        return [str(mssql_row_value(row, "filename", 0)) for row in rows]

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
            if is_mssql_table_missing(exc):
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
        """Delete all versions for an artifact atomically asynchronously and return deleted records."""
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
            if is_mssql_table_missing(exc):
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
            if is_mssql_table_missing(exc):
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
            if is_mssql_table_missing(exc):
                return []
            raise
        return [_row_to_stored_artifact(row, deserializer=self._json_deserializer) for row in rows]
