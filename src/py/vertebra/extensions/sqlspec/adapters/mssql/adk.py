"""SQL Server (MSSQL) ADK session, event, memory, and artifact stores."""

import contextlib
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, NotRequired, cast

from sqlspec.config import ADKConfig
from sqlspec.extensions.adk import BaseAsyncADKStore, BaseSyncADKStore, normalize_session_list_options
from sqlspec.utils.serializers import from_json, to_json

from vertebra.extensions.sqlspec.adapters.mssql.core import (
    build_mssql_scope_where,
    cosine_similarity,
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
from vertebra.storage._store import ArtifactReference
from vertebra.typing import ImproperConfigurationError

if TYPE_CHECKING:
    from sqlspec.extensions.adk import SessionOrderBy, StoredEvent, StoredSession
    from sqlspec.extensions.adk.artifact._types import StoredArtifact
    from sqlspec.extensions.adk.artifact.store import BaseAsyncADKArtifactStore as _BaseAsyncADKArtifactStore
    from sqlspec.extensions.adk.artifact.store import BaseSyncADKArtifactStore as _BaseSyncADKArtifactStore
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
            """Fallback synchronous memory store base when `google-adk` is absent."""

            def drop_tables(self) -> None:
                statements = self._drop_memory_table_sql()
                with self._config.provide_session() as driver:
                    for statement in statements:
                        driver.execute_script(statement)
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
                        await driver.execute_script(statement)
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
            """SDK-free ADK artifact store base when optional `google-adk` is absent."""

            __slots__ = ("_artifact_table", "_config", "_max_versions")
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
                self._artifact_table: str = str(adk.get("artifact_table", "adk_artifact_metadata"))
                self._max_versions: int | None = adk.get("max_artifact_versions")

            @property
            def config(self) -> Any:
                return self._config

            @property
            def artifact_table(self) -> str:
                return self._artifact_table

            @property
            def max_versions(self) -> int | None:
                return self._max_versions

        class _BaseSyncADKArtifactStore(_FallbackADKArtifactStoreCommon):
            """Fallback synchronous artifact store base when `google-adk` is absent."""

        class _BaseAsyncADKArtifactStore(_FallbackADKArtifactStoreCommon):
            """Fallback asynchronous artifact store base when `google-adk` is absent."""


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


class MSSQLADKConfig(ADKConfig):
    """SQL Server ADK extension settings."""

    native_json: NotRequired[bool]
    """Force native `JSON` column storage when `True` or `NVARCHAR(MAX)` when `False`."""


def _adk_settings(config: Any) -> Mapping[str, Any]:
    ext = getattr(config, "extension_config", {})
    if not isinstance(ext, Mapping):
        return {}
    adk = ext.get("adk", {})
    return adk if isinstance(adk, Mapping) else {}


def _configured_json_type(native_json: bool | None) -> str | None:
    if native_json is True:
        return _JSON_NATIVE_TYPE
    if native_json is False:
        return _JSON_FALLBACK_TYPE
    return None


def _resolve_json_codecs(config: Any) -> tuple[Any, Any]:
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


def _sessions_table_ddl(table: str, json_col_type: str, owner_id_ddl: str | None) -> str:
    owner_line = f",\n        {owner_id_ddl}" if owner_id_ddl else ""
    schema_name, table_name = split_schema_and_table(table)
    return (
        "IF NOT EXISTS ("
        f"SELECT 1 FROM sys.tables WHERE name = N'{escape_sql_literal(table_name)}' "
        f"AND schema_id = SCHEMA_ID(N'{escape_sql_literal(schema_name)}')"
        ")\nBEGIN\n"
        f"    CREATE TABLE {mssql_table_ref(table)} (\n"
        f"        row_id UNIQUEIDENTIFIER NOT NULL CONSTRAINT {mssql_constraint_ref('df', table, 'row_id')} DEFAULT NEWSEQUENTIALID(),\n"
        "        id NVARCHAR(128) NOT NULL,\n"
        "        app_name NVARCHAR(128) NOT NULL,\n"
        f"        user_id NVARCHAR(128) NOT NULL{owner_line},\n"
        f"        state {json_col_type} NOT NULL,\n"
        f"        create_time DATETIMEOFFSET(6) NOT NULL CONSTRAINT {mssql_constraint_ref('df', table, 'create_time')} DEFAULT SYSDATETIMEOFFSET(),\n"
        f"        update_time DATETIMEOFFSET(6) NOT NULL CONSTRAINT {mssql_constraint_ref('df', table, 'update_time')} DEFAULT SYSDATETIMEOFFSET(),\n"
        f"        CONSTRAINT {mssql_constraint_ref('pk', table, 'row_id')} PRIMARY KEY (row_id),\n"
        f"        CONSTRAINT {mssql_constraint_ref('uq', table, 'id')} UNIQUE (id)\n"
        "    );\n"
        "END;"
    )


def _events_table_ddl(table: str, session_table: str, json_col_type: str) -> str:
    schema_name, table_name = split_schema_and_table(table)
    return (
        "IF NOT EXISTS ("
        f"SELECT 1 FROM sys.tables WHERE name = N'{escape_sql_literal(table_name)}' "
        f"AND schema_id = SCHEMA_ID(N'{escape_sql_literal(schema_name)}')"
        ")\nBEGIN\n"
        f"    CREATE TABLE {mssql_table_ref(table)} (\n"
        f"        row_id UNIQUEIDENTIFIER NOT NULL CONSTRAINT {mssql_constraint_ref('df', table, 'row_id')} DEFAULT NEWSEQUENTIALID(),\n"
        "        id NVARCHAR(128) NOT NULL,\n"
        "        app_name NVARCHAR(128) NOT NULL,\n"
        "        user_id NVARCHAR(128) NOT NULL,\n"
        "        session_id NVARCHAR(128) NOT NULL,\n"
        "        invocation_id NVARCHAR(256),\n"
        "        timestamp DATETIMEOFFSET(6) NOT NULL,\n"
        f"        event_data {json_col_type} NOT NULL,\n"
        f"        CONSTRAINT {mssql_constraint_ref('pk', table, 'row_id')} PRIMARY KEY (row_id),\n"
        f"        CONSTRAINT {mssql_constraint_ref('uq', table, 'id')} UNIQUE (id),\n"
        f"        CONSTRAINT {mssql_constraint_ref('fk', table, 'session')} FOREIGN KEY (session_id) "
        f"REFERENCES {mssql_table_ref(session_table)}(id) ON DELETE CASCADE\n"
        "    );\n"
        "END;"
    )


def _app_states_table_ddl(table: str, json_col_type: str) -> str:
    schema_name, table_name = split_schema_and_table(table)
    return (
        "IF NOT EXISTS ("
        f"SELECT 1 FROM sys.tables WHERE name = N'{escape_sql_literal(table_name)}' "
        f"AND schema_id = SCHEMA_ID(N'{escape_sql_literal(schema_name)}')"
        ")\nBEGIN\n"
        f"    CREATE TABLE {mssql_table_ref(table)} (\n"
        "        app_name NVARCHAR(128) NOT NULL,\n"
        f"        state {json_col_type} NOT NULL,\n"
        f"        update_time DATETIMEOFFSET(6) NOT NULL CONSTRAINT {mssql_constraint_ref('df', table, 'update_time')} DEFAULT SYSDATETIMEOFFSET(),\n"
        f"        CONSTRAINT {mssql_constraint_ref('pk', table, 'app_name')} PRIMARY KEY (app_name)\n"
        "    );\n"
        "END;"
    )


def _user_states_table_ddl(table: str, json_col_type: str) -> str:
    schema_name, table_name = split_schema_and_table(table)
    return (
        "IF NOT EXISTS ("
        f"SELECT 1 FROM sys.tables WHERE name = N'{escape_sql_literal(table_name)}' "
        f"AND schema_id = SCHEMA_ID(N'{escape_sql_literal(schema_name)}')"
        ")\nBEGIN\n"
        f"    CREATE TABLE {mssql_table_ref(table)} (\n"
        "        app_name NVARCHAR(128) NOT NULL,\n"
        "        user_id NVARCHAR(128) NOT NULL,\n"
        f"        state {json_col_type} NOT NULL,\n"
        f"        update_time DATETIMEOFFSET(6) NOT NULL CONSTRAINT {mssql_constraint_ref('df', table, 'update_time')} DEFAULT SYSDATETIMEOFFSET(),\n"
        f"        CONSTRAINT {mssql_constraint_ref('pk', table, 'app_user')} PRIMARY KEY (app_name, user_id)\n"
        "    );\n"
        "END;"
    )


def _metadata_table_ddl(table: str) -> str:
    schema_name, table_name = split_schema_and_table(table)
    return (
        "IF NOT EXISTS ("
        f"SELECT 1 FROM sys.tables WHERE name = N'{escape_sql_literal(table_name)}' "
        f"AND schema_id = SCHEMA_ID(N'{escape_sql_literal(schema_name)}')"
        ")\nBEGIN\n"
        f"    CREATE TABLE {mssql_table_ref(table)} (\n"
        "        [key] NVARCHAR(128) NOT NULL,\n"
        "        value NVARCHAR(512) NOT NULL,\n"
        f"        CONSTRAINT {mssql_constraint_ref('pk', table, 'key')} PRIMARY KEY ([key])\n"
        "    );\n"
        "END;"
    )


def _sessions_index_specs(table: str) -> list[tuple[str, str, str]]:
    _, table_name = split_schema_and_table(table)
    return [
        (f"idx_{table_name}_app_user", table, "app_name, user_id"),
        (f"idx_{table_name}_update_time", table, "update_time DESC"),
    ]


def _events_index_specs(table: str) -> list[tuple[str, str, str]]:
    _, table_name = split_schema_and_table(table)
    return [
        (f"idx_{table_name}_scope", table, "app_name, user_id, session_id, timestamp ASC"),
        (f"idx_{table_name}_session", table, "session_id, timestamp ASC"),
        (f"idx_{table_name}_invocation", table, "invocation_id"),
        (f"idx_{table_name}_timestamp", table, "timestamp ASC"),
        (f"idx_{table_name}_app_timestamp", table, "app_name, timestamp ASC"),
    ]


def _drop_if_exists_sql(table: str) -> str:
    ref = mssql_table_ref(table)
    return f"IF OBJECT_ID(N'{escape_sql_literal(ref)}', N'U') IS NOT NULL DROP TABLE {ref};"


def _upsert_state_sql(table: str, key_columns: tuple[str, ...]) -> str:
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


def _upsert_metadata_sql(table: str) -> str:
    return (
        f"MERGE INTO {mssql_table_ref(table)} WITH (HOLDLOCK) AS target "
        "USING (SELECT ? AS [key], ? AS value) AS source "
        "ON (target.[key] = source.[key]) "
        "WHEN MATCHED THEN UPDATE SET value = source.value "
        "WHEN NOT MATCHED THEN INSERT ([key], value) VALUES (source.[key], source.value);"
    )


def _session_list_query(
    session_table: str,
    app_name: str,
    user_id: str | None,
    column: str,
    direction: str,
    limit: int | None,
    offset: int,
) -> tuple[str, tuple[Any, ...]]:
    params: list[Any] = [app_name]
    where_clause = "app_name = ?"
    if user_id is not None:
        params.append(user_id)
        where_clause = f"{where_clause} AND user_id = ?"
    page_clause = ""
    if limit is not None:
        params.extend((offset, limit))
        page_clause = " OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
    sql = (
        f"SELECT id, app_name, user_id, state, create_time, update_time "
        f"FROM {mssql_table_ref(session_table)} "
        f"WHERE {where_clause} ORDER BY {column} {direction}, id {direction}{page_clause}"
    )
    return sql, tuple(params)


def _events_query(
    events_table: str,
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
        after_clause = " AND timestamp > ?"
        params.append(mssql_ensure_utc(after_timestamp))
    sql = (
        f"SELECT {top_clause}id, app_name, user_id, session_id, invocation_id, timestamp, event_data "
        f"FROM {mssql_table_ref(events_table)} "
        f"WHERE app_name = ? AND user_id = ? AND session_id = ?{after_clause} "
        "ORDER BY timestamp ASC"
    )
    return sql, tuple(params)


def _row_to_session(row: Any, *, deserializer: Any = from_json) -> "StoredSession":
    return {
        "id": str(mssql_row_value(row, "id", 0)),
        "app_name": str(mssql_row_value(row, "app_name", 1)),
        "user_id": str(mssql_row_value(row, "user_id", 2)),
        "state": mssql_json_dict(mssql_row_value(row, "state", 3), deserializer=deserializer),
        "create_time": mssql_ensure_utc(mssql_row_value(row, "create_time", 4)),
        "update_time": mssql_ensure_utc(mssql_row_value(row, "update_time", 5)),
    }


def _row_to_event(row: Any, *, deserializer: Any = from_json) -> "StoredEvent":
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


def _row_to_memory(row: Any, *, deserializer: Any = from_json) -> "StoredMemory":
    metadata_raw = mssql_row_value(row, "metadata_json", 10)
    author_raw = mssql_row_value(row, "author", 6)
    return cast(
        "StoredMemory",
        {
            "id": str(mssql_row_value(row, "id", 0)),
            "session_id": str(mssql_row_value(row, "session_id", 1)),
            "app_name": str(mssql_row_value(row, "app_name", 2)),
            "user_id": str(mssql_row_value(row, "user_id", 3)),
            "scope": str(mssql_row_value(row, "scope", 4)),
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


class MSSQLSyncADKStore(BaseSyncADKStore[Any]):
    """Synchronous SQL Server ADK session, event, app/user state, and metadata store."""

    __slots__ = ("_json_column_type", "_json_deserializer", "_json_serializer", "_native_json")

    adapter_name = "vertebra_mssql"
    _max_table_name_parts = 2

    def __init__(self, config: Any) -> None:
        """Initialize the synchronous SQL Server ADK store."""
        super().__init__(config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        native_json = _adk_settings(config).get("native_json")
        self._native_json: bool | None = native_json if isinstance(native_json, bool) else None
        self._json_column_type: str | None = _configured_json_type(self._native_json)

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
        return _sessions_table_ddl(self._session_table, self._resolve_json_type(), self._owner_id_column_ddl)

    def _events_table_ddl(self) -> str:
        return _events_table_ddl(self._events_table, self._session_table, self._resolve_json_type())

    def _app_states_table_ddl(self) -> str:
        return _app_states_table_ddl(self._app_state_table, self._resolve_json_type())

    def _user_states_table_ddl(self) -> str:
        return _user_states_table_ddl(self._user_state_table, self._resolve_json_type())

    def _metadata_table_ddl(self) -> str:
        return _metadata_table_ddl(self._metadata_table)

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

    def create_tables(self) -> None:
        """Create idempotent T-SQL ADK tables and missing indexes via DataDictionary inspection."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return
        with self._config.provide_session() as driver:
            if self._json_column_type is None:
                self._json_column_type = _detect_json_type_sync(driver, self._native_json)
            json_type = self._json_column_type
            driver.execute_script(_sessions_table_ddl(self._session_table, json_type, self._owner_id_column_ddl))
            driver.execute_script(_events_table_ddl(self._events_table, self._session_table, json_type))
            driver.execute_script(_app_states_table_ddl(self._app_state_table, json_type))
            driver.execute_script(_user_states_table_ddl(self._user_state_table, json_type))
            driver.execute_script(_metadata_table_ddl(self._metadata_table))
            index_specs = [*_sessions_index_specs(self._session_table), *_events_index_specs(self._events_table)]
            existing = _existing_index_names_sync(driver, index_specs)
            for idx_name, idx_table, cols in index_specs:
                if _bare_index_name(idx_name) not in existing:
                    driver.execute(_create_index_sql(idx_table, idx_name, cols))
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
        """Insert a new ADK session row into SQL Server and return the persisted record."""
        table_ref = mssql_table_ref(self._session_table)
        state_json = self._json_serializer(state)
        if self._owner_id_column_name:
            sql = (
                f"INSERT INTO {table_ref} "
                f"(id, app_name, user_id, {quote_bracket_identifier(self._owner_id_column_name)}, state, create_time, update_time) "
                "VALUES (?, ?, ?, ?, ?, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET())"
            )
            params: tuple[Any, ...] = (session_id, app_name, user_id, owner_id, state_json)
        else:
            sql = (
                f"INSERT INTO {table_ref} (id, app_name, user_id, state, create_time, update_time) "
                "VALUES (?, ?, ?, ?, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET())"
            )
            params = (session_id, app_name, user_id, state_json)
        with self._config.provide_session() as driver:
            driver.execute(sql, *params)
            driver.commit()
        session = self.get_session(app_name, user_id, session_id)
        if session is None:
            msg = f"Failed to reload newly created MSSQL ADK session {session_id}"
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
        """Fetch an ADK session by composite key and optionally refresh `update_time`."""
        table_ref = mssql_table_ref(self._session_table)
        try:
            with self._config.provide_session() as driver:
                if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
                    driver.execute(
                        f"UPDATE {table_ref} SET update_time = SYSDATETIMEOFFSET() "
                        "WHERE app_name = ? AND user_id = ? AND id = ?",
                        app_name,
                        user_id,
                        session_id,
                    )
                    driver.commit()
                row = driver.select_one_or_none(
                    f"SELECT TOP (1) id, app_name, user_id, state, create_time, update_time "
                    f"FROM {table_ref} WHERE app_name = ? AND user_id = ? AND id = ?",
                    app_name,
                    user_id,
                    session_id,
                )
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return None
            raise
        return _row_to_session(row, deserializer=self._json_deserializer) if row is not None else None

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Replace durable JSON state on an ADK session row."""
        table_ref = mssql_table_ref(self._session_table)
        with self._config.provide_session() as driver:
            driver.execute(
                f"UPDATE {table_ref} SET state = ?, update_time = SYSDATETIMEOFFSET() "
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
        """List ADK sessions with `OFFSET ... FETCH NEXT` T-SQL pagination."""
        column, direction, page_limit, page_offset = normalize_session_list_options(order_by, descending, limit, offset)
        if page_limit == 0:
            return []
        sql, params = _session_list_query(
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
            if is_mssql_table_missing(exc):
                return []
            raise
        return [_row_to_session(row, deserializer=self._json_deserializer) for row in rows]

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete an ADK session row (cascading to events)."""
        table_ref = mssql_table_ref(self._session_table)
        with self._config.provide_session() as driver:
            driver.execute(
                f"DELETE FROM {table_ref} WHERE app_name = ? AND user_id = ? AND id = ?",
                app_name,
                user_id,
                session_id,
            )
            driver.commit()

    def append_event(self, event_record: "StoredEvent") -> None:
        """Append an ADK event record."""
        sql = (
            f"INSERT INTO {mssql_table_ref(self._events_table)} "
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
                event_record.get("invocation_id"),
                mssql_ensure_utc(event_record["timestamp"]),
                self._json_serializer(event_record["event_data"]),
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
        """Atomically update session state, append event, and merge scoped states inside a transaction."""
        sess_ref = mssql_table_ref(self._session_table)
        ev_ref = mssql_table_ref(self._events_table)
        with self._config.provide_session() as driver:
            driver.begin()
            try:
                driver.execute(
                    f"UPDATE {sess_ref} SET state = ?, update_time = SYSDATETIMEOFFSET() "
                    "WHERE app_name = ? AND user_id = ? AND id = ?",
                    self._json_serializer(state),
                    app_name,
                    user_id,
                    session_id,
                )
                row = driver.select_one_or_none(
                    f"SELECT TOP (1) id, app_name, user_id, state, create_time, update_time "
                    f"FROM {sess_ref} WHERE app_name = ? AND user_id = ? AND id = ?",
                    app_name,
                    user_id,
                    session_id,
                )
                if row is None:
                    msg = f"Session {session_id} not found during append_event_and_update_state."
                    raise ValueError(msg)
                driver.execute(
                    f"INSERT INTO {ev_ref} (id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    event_record["id"],
                    app_name,
                    user_id,
                    session_id,
                    event_record.get("invocation_id"),
                    mssql_ensure_utc(event_record["timestamp"]),
                    self._json_serializer(event_record["event_data"]),
                )
                if app_state is not None:
                    driver.execute(
                        _upsert_state_sql(self._app_state_table, ("app_name",)),
                        app_name,
                        self._json_serializer(app_state),
                    )
                if user_state is not None:
                    driver.execute(
                        _upsert_state_sql(self._user_state_table, ("app_name", "user_id")),
                        app_name,
                        user_id,
                        self._json_serializer(user_state),
                    )
                driver.commit()
                return _row_to_session(row, deserializer=self._json_deserializer)
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
        """Return ordered ADK events for a session."""
        if limit == 0:
            return []
        sql, params = _events_query(self._events_table, app_name, user_id, session_id, after_timestamp, limit)
        try:
            with self._config.provide_session() as driver:
                rows = driver.select(sql, *params)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return []
            raise
        return [_row_to_event(row, deserializer=self._json_deserializer) for row in rows]

    def _delete_before(self, table: str, column: str, threshold: datetime, app_name: str | None) -> int:
        sql = f"DELETE FROM {mssql_table_ref(table)} WHERE {column} < ?"
        params: list[Any] = [mssql_ensure_utc(threshold)]
        if app_name is not None:
            sql = f"{sql} AND app_name = ?"
            params.append(app_name)
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
        """Delete events older than `before`."""
        return self._delete_before(self._events_table, "timestamp", before, app_name)

    def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete idle sessions older than `updated_before`."""
        return self._delete_before(self._session_table, "update_time", updated_before, app_name)

    def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete idle user states older than `updated_before`."""
        return self._delete_before(self._user_state_table, "update_time", updated_before, app_name)

    def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Fetch app-scoped JSON state for `app_name`."""
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(
                    f"SELECT TOP (1) state FROM {mssql_table_ref(self._app_state_table)} WHERE app_name = ?",
                    app_name,
                )
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return None
            raise
        return (
            mssql_json_dict(mssql_row_value(row, "state", 0), deserializer=self._json_deserializer)
            if row is not None
            else None
        )

    def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Fetch user-scoped JSON state for `(app_name, user_id)`."""
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(
                    f"SELECT TOP (1) state FROM {mssql_table_ref(self._user_state_table)} "
                    "WHERE app_name = ? AND user_id = ?",
                    app_name,
                    user_id,
                )
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return None
            raise
        return (
            mssql_json_dict(mssql_row_value(row, "state", 0), deserializer=self._json_deserializer)
            if row is not None
            else None
        )

    def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Merge app-scoped JSON state using `MERGE ... WITH (HOLDLOCK)`."""
        with self._config.provide_session() as driver:
            driver.execute(
                _upsert_state_sql(self._app_state_table, ("app_name",)),
                app_name,
                self._json_serializer(state),
            )
            driver.commit()

    def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Merge user-scoped JSON state using `MERGE ... WITH (HOLDLOCK)`."""
        with self._config.provide_session() as driver:
            driver.execute(
                _upsert_state_sql(self._user_state_table, ("app_name", "user_id")),
                app_name,
                user_id,
                self._json_serializer(state),
            )
            driver.commit()

    def get_metadata(self, key: str) -> str | None:
        """Return a metadata value by key."""
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(
                    f"SELECT TOP (1) value FROM {mssql_table_ref(self._metadata_table)} WHERE [key] = ?",
                    key,
                )
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return None
            raise
        val = mssql_row_value(row, "value", 0) if row is not None else None
        return str(val) if val is not None else None

    def set_metadata(self, key: str, value: str) -> None:
        """Upsert a metadata key-value entry via `MERGE ... WITH (HOLDLOCK)`."""
        with self._config.provide_session() as driver:
            driver.execute(_upsert_metadata_sql(self._metadata_table), key, value)
            driver.commit()


class MSSQLAsyncADKStore(BaseAsyncADKStore[Any]):
    """Asynchronous SQL Server ADK session, event, app/user state, and metadata store."""

    __slots__ = ("_json_column_type", "_json_deserializer", "_json_serializer", "_native_json")

    adapter_name = "vertebra_mssql"
    _max_table_name_parts = 2

    def __init__(self, config: Any) -> None:
        """Initialize the asynchronous SQL Server ADK store."""
        super().__init__(config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        native_json = _adk_settings(config).get("native_json")
        self._native_json: bool | None = native_json if isinstance(native_json, bool) else None
        self._json_column_type: str | None = _configured_json_type(self._native_json)

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
        return _sessions_table_ddl(self._session_table, await self._resolve_json_type(), self._owner_id_column_ddl)

    async def _events_table_ddl(self) -> str:
        return _events_table_ddl(self._events_table, self._session_table, await self._resolve_json_type())

    async def _app_states_table_ddl(self) -> str:
        return _app_states_table_ddl(self._app_state_table, await self._resolve_json_type())

    async def _user_states_table_ddl(self) -> str:
        return _user_states_table_ddl(self._user_state_table, await self._resolve_json_type())

    async def _metadata_table_ddl(self) -> str:
        return _metadata_table_ddl(self._metadata_table)

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

    async def create_tables(self) -> None:
        """Create idempotent T-SQL ADK tables and missing indexes asynchronously."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        async with self._config.provide_session() as driver:
            if self._json_column_type is None:
                self._json_column_type = await _detect_json_type_async(driver, self._native_json)
            json_type = self._json_column_type
            await driver.execute_script(_sessions_table_ddl(self._session_table, json_type, self._owner_id_column_ddl))
            await driver.execute_script(_events_table_ddl(self._events_table, self._session_table, json_type))
            await driver.execute_script(_app_states_table_ddl(self._app_state_table, json_type))
            await driver.execute_script(_user_states_table_ddl(self._user_state_table, json_type))
            await driver.execute_script(_metadata_table_ddl(self._metadata_table))
            index_specs = [*_sessions_index_specs(self._session_table), *_events_index_specs(self._events_table)]
            existing = await _existing_index_names_async(driver, index_specs)
            for idx_name, idx_table, cols in index_specs:
                if _bare_index_name(idx_name) not in existing:
                    await driver.execute(_create_index_sql(idx_table, idx_name, cols))
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
        table_ref = mssql_table_ref(self._session_table)
        state_json = self._json_serializer(state)
        if self._owner_id_column_name:
            sql = (
                f"INSERT INTO {table_ref} "
                f"(id, app_name, user_id, {quote_bracket_identifier(self._owner_id_column_name)}, state, create_time, update_time) "
                "VALUES (?, ?, ?, ?, ?, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET())"
            )
            params: tuple[Any, ...] = (session_id, app_name, user_id, owner_id, state_json)
        else:
            sql = (
                f"INSERT INTO {table_ref} (id, app_name, user_id, state, create_time, update_time) "
                "VALUES (?, ?, ?, ?, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET())"
            )
            params = (session_id, app_name, user_id, state_json)
        async with self._config.provide_session() as driver:
            await driver.execute(sql, *params)
            await driver.commit()
        session = await self.get_session(app_name, user_id, session_id)
        if session is None:
            msg = f"Failed to reload newly created MSSQL ADK session {session_id}"
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
        """Fetch an ADK session asynchronously by composite key."""
        table_ref = mssql_table_ref(self._session_table)
        try:
            async with self._config.provide_session() as driver:
                if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
                    await driver.execute(
                        f"UPDATE {table_ref} SET update_time = SYSDATETIMEOFFSET() "
                        "WHERE app_name = ? AND user_id = ? AND id = ?",
                        app_name,
                        user_id,
                        session_id,
                    )
                    await driver.commit()
                row = await driver.select_one_or_none(
                    f"SELECT TOP (1) id, app_name, user_id, state, create_time, update_time "
                    f"FROM {table_ref} WHERE app_name = ? AND user_id = ? AND id = ?",
                    app_name,
                    user_id,
                    session_id,
                )
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return None
            raise
        return _row_to_session(row, deserializer=self._json_deserializer) if row is not None else None

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Replace session state asynchronously."""
        table_ref = mssql_table_ref(self._session_table)
        async with self._config.provide_session() as driver:
            await driver.execute(
                f"UPDATE {table_ref} SET state = ?, update_time = SYSDATETIMEOFFSET() "
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
        """List ADK sessions asynchronously with `OFFSET ... FETCH NEXT` pagination."""
        column, direction, page_limit, page_offset = normalize_session_list_options(order_by, descending, limit, offset)
        if page_limit == 0:
            return []
        sql, params = _session_list_query(
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
            if is_mssql_table_missing(exc):
                return []
            raise
        return [_row_to_session(row, deserializer=self._json_deserializer) for row in rows]

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete an ADK session asynchronously."""
        table_ref = mssql_table_ref(self._session_table)
        async with self._config.provide_session() as driver:
            await driver.execute(
                f"DELETE FROM {table_ref} WHERE app_name = ? AND user_id = ? AND id = ?",
                app_name,
                user_id,
                session_id,
            )
            await driver.commit()

    async def append_event(self, event_record: "StoredEvent") -> None:
        """Append an ADK event record asynchronously."""
        sql = (
            f"INSERT INTO {mssql_table_ref(self._events_table)} "
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
                event_record.get("invocation_id"),
                mssql_ensure_utc(event_record["timestamp"]),
                self._json_serializer(event_record["event_data"]),
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
        """Atomically update session state, append event, and merge scoped states asynchronously."""
        sess_ref = mssql_table_ref(self._session_table)
        ev_ref = mssql_table_ref(self._events_table)
        async with self._config.provide_session() as driver:
            await driver.begin()
            try:
                await driver.execute(
                    f"UPDATE {sess_ref} SET state = ?, update_time = SYSDATETIMEOFFSET() "
                    "WHERE app_name = ? AND user_id = ? AND id = ?",
                    self._json_serializer(state),
                    app_name,
                    user_id,
                    session_id,
                )
                row = await driver.select_one_or_none(
                    f"SELECT TOP (1) id, app_name, user_id, state, create_time, update_time "
                    f"FROM {sess_ref} WHERE app_name = ? AND user_id = ? AND id = ?",
                    app_name,
                    user_id,
                    session_id,
                )
                if row is None:
                    msg = f"Session {session_id} not found during append_event_and_update_state."
                    raise ValueError(msg)
                await driver.execute(
                    f"INSERT INTO {ev_ref} (id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    event_record["id"],
                    app_name,
                    user_id,
                    session_id,
                    event_record.get("invocation_id"),
                    mssql_ensure_utc(event_record["timestamp"]),
                    self._json_serializer(event_record["event_data"]),
                )
                if app_state is not None:
                    await driver.execute(
                        _upsert_state_sql(self._app_state_table, ("app_name",)),
                        app_name,
                        self._json_serializer(app_state),
                    )
                if user_state is not None:
                    await driver.execute(
                        _upsert_state_sql(self._user_state_table, ("app_name", "user_id")),
                        app_name,
                        user_id,
                        self._json_serializer(user_state),
                    )
                await driver.commit()
                return _row_to_session(row, deserializer=self._json_deserializer)
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
        """Return ordered ADK events for a session asynchronously."""
        if limit == 0:
            return []
        sql, params = _events_query(self._events_table, app_name, user_id, session_id, after_timestamp, limit)
        try:
            async with self._config.provide_session() as driver:
                rows = await driver.select(sql, *params)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return []
            raise
        return [_row_to_event(row, deserializer=self._json_deserializer) for row in rows]

    async def _delete_before(self, table: str, column: str, threshold: datetime, app_name: str | None) -> int:
        sql = f"DELETE FROM {mssql_table_ref(table)} WHERE {column} < ?"
        params: list[Any] = [mssql_ensure_utc(threshold)]
        if app_name is not None:
            sql = f"{sql} AND app_name = ?"
            params.append(app_name)
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
        """Delete events older than `before` asynchronously."""
        return await self._delete_before(self._events_table, "timestamp", before, app_name)

    async def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete idle sessions older than `updated_before` asynchronously."""
        return await self._delete_before(self._session_table, "update_time", updated_before, app_name)

    async def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete idle user states older than `updated_before` asynchronously."""
        return await self._delete_before(self._user_state_table, "update_time", updated_before, app_name)

    async def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Fetch app-scoped state asynchronously."""
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(
                    f"SELECT TOP (1) state FROM {mssql_table_ref(self._app_state_table)} WHERE app_name = ?",
                    app_name,
                )
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return None
            raise
        return (
            mssql_json_dict(mssql_row_value(row, "state", 0), deserializer=self._json_deserializer)
            if row is not None
            else None
        )

    async def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Fetch user-scoped state asynchronously."""
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(
                    f"SELECT TOP (1) state FROM {mssql_table_ref(self._user_state_table)} "
                    "WHERE app_name = ? AND user_id = ?",
                    app_name,
                    user_id,
                )
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return None
            raise
        return (
            mssql_json_dict(mssql_row_value(row, "state", 0), deserializer=self._json_deserializer)
            if row is not None
            else None
        )

    async def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Merge app-scoped JSON state asynchronously."""
        async with self._config.provide_session() as driver:
            await driver.execute(
                _upsert_state_sql(self._app_state_table, ("app_name",)),
                app_name,
                self._json_serializer(state),
            )
            await driver.commit()

    async def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Merge user-scoped JSON state asynchronously."""
        async with self._config.provide_session() as driver:
            await driver.execute(
                _upsert_state_sql(self._user_state_table, ("app_name", "user_id")),
                app_name,
                user_id,
                self._json_serializer(state),
            )
            await driver.commit()

    async def get_metadata(self, key: str) -> str | None:
        """Fetch metadata table value asynchronously."""
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(
                    f"SELECT TOP (1) value FROM {mssql_table_ref(self._metadata_table)} WHERE [key] = ?",
                    key,
                )
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return None
            raise
        val = mssql_row_value(row, "value", 0) if row is not None else None
        return str(val) if val is not None else None

    async def set_metadata(self, key: str, value: str) -> None:
        """Upsert a metadata key-value entry asynchronously."""
        async with self._config.provide_session() as driver:
            await driver.execute(_upsert_metadata_sql(self._metadata_table), key, value)
            await driver.commit()


def _memory_table_ddl(table: str, json_col_type: str, owner_id_ddl: str | None) -> str:
    owner_line = f",\n        {owner_id_ddl}" if owner_id_ddl else ""
    schema_name, table_name = split_schema_and_table(table)
    return (
        "IF NOT EXISTS ("
        f"SELECT 1 FROM sys.tables WHERE name = N'{escape_sql_literal(table_name)}' "
        f"AND schema_id = SCHEMA_ID(N'{escape_sql_literal(schema_name)}')"
        ")\nBEGIN\n"
        f"    CREATE TABLE {mssql_table_ref(table)} (\n"
        "        id NVARCHAR(128) NOT NULL,\n"
        "        session_id NVARCHAR(128) NOT NULL,\n"
        "        app_name NVARCHAR(128) NOT NULL,\n"
        "        user_id NVARCHAR(128) NOT NULL,\n"
        f"        scope NVARCHAR(16) NOT NULL CONSTRAINT {mssql_constraint_ref('df', table, 'scope')} DEFAULT N'user',\n"
        "        event_id NVARCHAR(128) NOT NULL,\n"
        "        author NVARCHAR(256) NULL,\n"
        "        timestamp DATETIMEOFFSET(6) NOT NULL,\n"
        f"        content_json {json_col_type} NOT NULL,\n"
        "        content_text NVARCHAR(MAX) NOT NULL,\n"
        f"        metadata_json {json_col_type} NULL,\n"
        f"        inserted_at DATETIMEOFFSET(6) NOT NULL CONSTRAINT {mssql_constraint_ref('df', table, 'inserted_at')} DEFAULT SYSDATETIMEOFFSET(){owner_line},\n"
        f"        CONSTRAINT {mssql_constraint_ref('pk', table, 'id')} PRIMARY KEY (id),\n"
        f"        CONSTRAINT {mssql_constraint_ref('uq', table, 'event_id')} UNIQUE (event_id)\n"
        "    );\n"
        "END;"
    )


def _memory_index_specs(table: str) -> list[tuple[str, str, str]]:
    _, table_name = split_schema_and_table(table)
    return [
        (f"idx_{table_name}_app_scope_user_time", table, "app_name, scope, user_id, timestamp DESC"),
        (f"idx_{table_name}_scope", table, "app_name, scope"),
        (f"idx_{table_name}_session", table, "session_id"),
        (f"idx_{table_name}_timestamp", table, "timestamp DESC"),
    ]


def _build_memory_insert(
    memory_table: str,
    owner_col_name: str | None,
    entries: "list[StoredMemory]",
    owner_id: object | None,
    *,
    serializer: Any,
) -> tuple[str, list[tuple[Any, ...]]]:
    owner_col = f", {quote_bracket_identifier(owner_col_name)}" if owner_col_name else ""
    owner_param = ", ?" if owner_col_name else ""
    table_ref = mssql_table_ref(memory_table)
    sql = (
        f"INSERT INTO {table_ref} "
        f"(id, session_id, app_name, user_id, scope, event_id, author, timestamp, content_json, content_text, metadata_json{owner_col}) "
        f"SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?{owner_param} "
        f"WHERE NOT EXISTS (SELECT 1 FROM {table_ref} WITH (UPDLOCK, HOLDLOCK) WHERE event_id = ?)"
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
            serializer(entry["content_json"]),
            entry["content_text"],
            serializer(meta) if meta is not None else None,
        )
        if owner_col_name:
            row_params = (*row_params, owner_id)
        batch.append((*row_params, entry["event_id"]))
    return sql, batch


def _build_memory_search(
    memory_table: str,
    query: str,
    app_name: str,
    user_id: str,
    limit_value: int,
    scope_filter: Literal["all", "user", "app"],
) -> tuple[str, tuple[Any, ...]]:
    where_scope, scope_params = build_mssql_scope_where(app_name, user_id, scope_filter)
    escaped = escape_mssql_like(query)
    sql = (
        f"SELECT TOP (?) id, session_id, app_name, user_id, scope, event_id, author, timestamp, "
        "content_json, content_text, metadata_json, inserted_at "
        f"FROM {mssql_table_ref(memory_table)} "
        f"WHERE {where_scope} AND content_text LIKE ? ESCAPE N'\\' ORDER BY timestamp DESC"
    )
    return sql, (limit_value, *scope_params, f"%{escaped}%")


def _rank_memory(records: "list[StoredMemory]", embedding: Sequence[float] | None, limit: int) -> "list[StoredMemory]":
    if embedding is None:
        return records[:limit]
    scored: list[tuple[float, StoredMemory]] = []
    for rec in records:
        vec = rec.get("embedding")
        score = cosine_similarity(embedding, vec) if vec is not None else 0.0
        scored.append((score, rec))
    scored.sort(key=lambda item: item[0], reverse=True)
    return [item[1] for item in scored[:limit]]


class MSSQLSyncADKMemoryStore(_BaseSyncADKMemoryStore[Any]):
    """Synchronous SQL Server ADK memory store with HOLDLOCK deduplication and `LIKE` search."""

    __slots__ = ("_json_column_type", "_json_deserializer", "_json_serializer", "_native_json")

    adapter_name = "vertebra_mssql"
    _max_table_name_parts = 2

    def __init__(self, config: Any) -> None:
        """Initialize the synchronous SQL Server ADK memory store."""
        super().__init__(config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        native_json = _adk_settings(config).get("native_json")
        self._native_json: bool | None = native_json if isinstance(native_json, bool) else None
        self._json_column_type: str | None = _configured_json_type(self._native_json)

    def _memory_table_ddl(self) -> str:
        json_type = self._json_column_type or _JSON_FALLBACK_TYPE
        return _memory_table_ddl(self._memory_table, json_type, self._owner_id_column_ddl)

    def _drop_memory_table_sql(self) -> list[str]:
        return [_drop_if_exists_sql(self._memory_table)]

    def create_tables(self) -> None:
        """Create idempotent T-SQL memory table and missing indexes."""
        if not self._enabled:
            return
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return
        with self._config.provide_session() as driver:
            if self._json_column_type is None:
                self._json_column_type = _detect_json_type_sync(driver, self._native_json)
            driver.execute_script(
                _memory_table_ddl(self._memory_table, self._json_column_type, self._owner_id_column_ddl)
            )
            index_specs = _memory_index_specs(self._memory_table)
            existing = _existing_index_names_sync(driver, index_specs)
            for idx_name, idx_table, cols in index_specs:
                if _bare_index_name(idx_name) not in existing:
                    driver.execute(_create_index_sql(idx_table, idx_name, cols))
            driver.commit()
        self.reconcile_schema(assume_existing=True)

    def insert_memory_entries(self, entries: "list[StoredMemory]", owner_id: object | None = None) -> int:
        """Bulk-insert memory entries with `UPDLOCK, HOLDLOCK` `event_id` deduplication."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0
        sql, batch = _build_memory_insert(
            self._memory_table,
            self._owner_id_column_name,
            entries,
            owner_id,
            serializer=self._json_serializer,
        )
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
    ) -> "list[StoredMemory]":
        """Search memory entries matching `query`."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if embedding is not None:
            msg = "Vector embeddings are not configured for MSSQLSyncADKMemoryStore"
            raise ImproperConfigurationError(msg)
        limit_value = limit or self._max_results
        if limit_value <= 0:
            return []
        sql, params = _build_memory_search(
            self._memory_table,
            query,
            app_name,
            user_id,
            limit_value,
            scope_filter,
        )
        try:
            with self._config.provide_session() as driver:
                rows = driver.select(sql, *params)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return []
            raise
        records = [_row_to_memory(row, deserializer=self._json_deserializer) for row in rows]
        return _rank_memory(records, None, limit_value)

    def delete_entries_by_session(self, session_id: str) -> int:
        """Delete memory entries matching `session_id`."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        try:
            with self._config.provide_session() as driver:
                res = driver.execute(
                    f"DELETE FROM {mssql_table_ref(self._memory_table)} WHERE session_id = ?",
                    session_id,
                )
                driver.commit()
                return int(res.rows_affected or 0)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return 0
            raise

    def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
        """Delete memory entries older than `days` days."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        clauses = ["inserted_at < DATEADD(day, -?, SYSDATETIMEOFFSET())"]
        params: list[Any] = [int(days)]
        if app_name is not None:
            clauses.append("app_name = ?")
            params.append(app_name)
        if scope is not None:
            clauses.append("scope = ?")
            params.append(scope)
        sql = f"DELETE FROM {mssql_table_ref(self._memory_table)} WHERE {' AND '.join(clauses)}"
        try:
            with self._config.provide_session() as driver:
                res = driver.execute(sql, *params)
                driver.commit()
                return int(res.rows_affected or 0)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return 0
            raise


class MSSQLAsyncADKMemoryStore(_BaseAsyncADKMemoryStore[Any]):
    """Asynchronous SQL Server ADK memory store."""

    __slots__ = ("_json_column_type", "_json_deserializer", "_json_serializer", "_native_json")

    adapter_name = "vertebra_mssql"
    _max_table_name_parts = 2

    def __init__(self, config: Any) -> None:
        """Initialize the asynchronous SQL Server ADK memory store."""
        super().__init__(config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        native_json = _adk_settings(config).get("native_json")
        self._native_json: bool | None = native_json if isinstance(native_json, bool) else None
        self._json_column_type: str | None = _configured_json_type(self._native_json)

    async def _memory_table_ddl(self) -> str:
        json_type = self._json_column_type or _JSON_FALLBACK_TYPE
        return _memory_table_ddl(self._memory_table, json_type, self._owner_id_column_ddl)

    def _drop_memory_table_sql(self) -> list[str]:
        return [_drop_if_exists_sql(self._memory_table)]

    async def create_tables(self) -> None:
        """Create idempotent T-SQL memory table and missing indexes asynchronously."""
        if not self._enabled:
            return
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        async with self._config.provide_session() as driver:
            if self._json_column_type is None:
                self._json_column_type = await _detect_json_type_async(driver, self._native_json)
            await driver.execute_script(
                _memory_table_ddl(self._memory_table, self._json_column_type, self._owner_id_column_ddl)
            )
            index_specs = _memory_index_specs(self._memory_table)
            existing = await _existing_index_names_async(driver, index_specs)
            for idx_name, idx_table, cols in index_specs:
                if _bare_index_name(idx_name) not in existing:
                    await driver.execute(_create_index_sql(idx_table, idx_name, cols))
            await driver.commit()
        await self.reconcile_schema(assume_existing=True)

    async def insert_memory_entries(self, entries: "list[StoredMemory]", owner_id: object | None = None) -> int:
        """Bulk-insert memory entries asynchronously with HOLDLOCK deduplication."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0
        sql, batch = _build_memory_insert(
            self._memory_table,
            self._owner_id_column_name,
            entries,
            owner_id,
            serializer=self._json_serializer,
        )
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
    ) -> "list[StoredMemory]":
        """Search memory entries asynchronously."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if embedding is not None:
            msg = "Vector embeddings are not configured for MSSQLAsyncADKMemoryStore"
            raise ImproperConfigurationError(msg)
        limit_value = limit or self._max_results
        if limit_value <= 0:
            return []
        sql, params = _build_memory_search(
            self._memory_table,
            query,
            app_name,
            user_id,
            limit_value,
            scope_filter,
        )
        try:
            async with self._config.provide_session() as driver:
                rows = await driver.select(sql, *params)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return []
            raise
        records = [_row_to_memory(row, deserializer=self._json_deserializer) for row in rows]
        return _rank_memory(records, None, limit_value)

    async def delete_entries_by_session(self, session_id: str) -> int:
        """Delete memory entries matching `session_id` asynchronously."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        try:
            async with self._config.provide_session() as driver:
                res = await driver.execute(
                    f"DELETE FROM {mssql_table_ref(self._memory_table)} WHERE session_id = ?",
                    session_id,
                )
                await driver.commit()
                return int(res.rows_affected or 0)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return 0
            raise

    async def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
        """Delete memory entries older than `days` days asynchronously."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        clauses = ["inserted_at < DATEADD(day, -?, SYSDATETIMEOFFSET())"]
        params: list[Any] = [int(days)]
        if app_name is not None:
            clauses.append("app_name = ?")
            params.append(app_name)
        if scope is not None:
            clauses.append("scope = ?")
            params.append(scope)
        sql = f"DELETE FROM {mssql_table_ref(self._memory_table)} WHERE {' AND '.join(clauses)}"
        try:
            async with self._config.provide_session() as driver:
                res = await driver.execute(sql, *params)
                await driver.commit()
                return int(res.rows_affected or 0)
        except Exception as exc:
            if is_mssql_table_missing(exc):
                return 0
            raise


def _artifact_table_ddl(table: str, json_col_type: str) -> str:
    schema_name, table_name = split_schema_and_table(table)
    return (
        "IF NOT EXISTS ("
        f"SELECT 1 FROM sys.tables WHERE name = N'{escape_sql_literal(table_name)}' "
        f"AND schema_id = SCHEMA_ID(N'{escape_sql_literal(schema_name)}')"
        ")\nBEGIN\n"
        f"    CREATE TABLE {mssql_table_ref(table)} (\n"
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
        f"        created_at DATETIMEOFFSET(6) NOT NULL CONSTRAINT {mssql_constraint_ref('df', table, 'created_at')} DEFAULT SYSDATETIMEOFFSET(),\n"
        f"        CONSTRAINT {mssql_constraint_ref('pk', table, 'artifact')} PRIMARY KEY (app_name, user_id, session_id, filename, version)\n"
        "    );\n"
        "END;"
    )


def _merge_artifact_sql(table: str) -> str:
    return (
        f"MERGE INTO {mssql_table_ref(table)} WITH (HOLDLOCK) AS target "
        "USING (SELECT ? AS app_name, ? AS user_id, ? AS session_id, ? AS filename, ? AS version, "
        "? AS mime_type, ? AS canonical_uri, ? AS custom_metadata, ? AS created_at) AS src "
        "ON (target.app_name = src.app_name AND target.user_id = src.user_id "
        "AND target.session_id = src.session_id AND target.filename = src.filename AND target.version = src.version) "
        "WHEN MATCHED THEN UPDATE SET mime_type = src.mime_type, canonical_uri = src.canonical_uri, "
        "custom_metadata = src.custom_metadata "
        "WHEN NOT MATCHED THEN INSERT (app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at) "
        "VALUES (src.app_name, src.user_id, src.session_id, src.filename, src.version, src.mime_type, src.canonical_uri, src.custom_metadata, src.created_at);"
    )


def _merge_artifact_ref_sql(table: str) -> str:
    return (
        f"MERGE INTO {mssql_table_ref(table)} WITH (HOLDLOCK) AS target "
        "USING (SELECT ? AS app_name, ? AS user_id, ? AS session_id, ? AS filename, ? AS version, "
        "? AS mime_type, ? AS canonical_uri, ? AS custom_metadata, ? AS size, ? AS updated, ? AS signed_url, ? AS created_at) AS src "
        "ON (target.app_name = src.app_name AND target.user_id = src.user_id "
        "AND target.session_id = src.session_id AND target.filename = src.filename AND target.version = src.version) "
        "WHEN MATCHED THEN UPDATE SET mime_type = src.mime_type, canonical_uri = src.canonical_uri, "
        "custom_metadata = src.custom_metadata, size = src.size, updated = src.updated, signed_url = src.signed_url "
        "WHEN NOT MATCHED THEN INSERT (app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, size, updated, signed_url, created_at) "
        "VALUES (src.app_name, src.user_id, src.session_id, src.filename, src.version, src.mime_type, src.canonical_uri, src.custom_metadata, src.size, src.updated, src.signed_url, src.created_at);"
    )


def _row_to_stored_artifact(row: Any, *, deserializer: Any = from_json) -> "StoredArtifact":
    session_raw = mssql_row_value(row, "session_id", 2)
    custom_raw = mssql_row_value(row, "custom_metadata", 7)
    mime_raw = mssql_row_value(row, "mime_type", 5)
    return {
        "app_name": str(mssql_row_value(row, "app_name", 0)),
        "user_id": str(mssql_row_value(row, "user_id", 1)),
        "session_id": str(session_raw) if session_raw else None,
        "filename": str(mssql_row_value(row, "filename", 3)),
        "version": int(mssql_row_value(row, "version", 4)),
        "mime_type": str(mime_raw) if mime_raw is not None else None,
        "canonical_uri": str(mssql_row_value(row, "canonical_uri", 6)),
        "custom_metadata": mssql_json_dict(custom_raw, deserializer=deserializer) if custom_raw is not None else None,
        "created_at": mssql_ensure_utc(mssql_row_value(row, "created_at", 8)),
    }


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


class MSSQLSyncADKArtifactStore(_BaseSyncADKArtifactStore[Any]):
    """Synchronous SQL Server ADK artifact metadata store supporting `ArtifactReference`."""

    __slots__ = ("_json_column_type", "_json_deserializer", "_json_serializer", "_native_json")

    adapter_name = "vertebra_mssql"

    def __init__(self, config: Any) -> None:
        """Initialize the synchronous SQL Server ADK artifact store."""
        super().__init__(config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        native_json = _adk_settings(config).get("native_json")
        self._native_json: bool | None = native_json if isinstance(native_json, bool) else None
        self._json_column_type: str | None = _configured_json_type(self._native_json)

    def _artifact_table_ddl(self) -> str:
        return _artifact_table_ddl(self._artifact_table, self._json_column_type or _JSON_FALLBACK_TYPE)

    def create_table(self) -> None:
        """Create the SQL Server artifact metadata table if absent."""
        with self._config.provide_session() as driver:
            if self._json_column_type is None:
                self._json_column_type = _detect_json_type_sync(driver, self._native_json)
            driver.execute_script(_artifact_table_ddl(self._artifact_table, self._json_column_type))
            driver.commit()

    def insert_artifact(self, record: "StoredArtifact") -> None:
        """Insert or merge a `StoredArtifact` revision."""
        session_val = record["session_id"] if record["session_id"] is not None else ""
        meta = record.get("custom_metadata")
        with self._config.provide_session() as driver:
            driver.execute(
                _merge_artifact_sql(self._artifact_table),
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
        now_dt = reference.updated or datetime.now()
        with self._config.provide_session() as driver:
            driver.execute(
                _merge_artifact_ref_sql(self._artifact_table),
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
        table_ref = mssql_table_ref(self._artifact_table)
        with self._config.provide_session() as driver:
            if version is not None:
                row = driver.select_one_or_none(
                    f"SELECT TOP (1) * FROM {table_ref} "
                    "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? AND version = ?",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                    version,
                )
            else:
                row = driver.select_one_or_none(
                    f"SELECT TOP (1) * FROM {table_ref} "
                    "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? "
                    "ORDER BY version DESC",
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
        table_ref = mssql_table_ref(self._artifact_table)
        with self._config.provide_session() as driver:
            if version is not None:
                row = driver.select_one_or_none(
                    "SELECT TOP (1) app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at "
                    f"FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? AND version = ?",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                    version,
                )
            else:
                row = driver.select_one_or_none(
                    "SELECT TOP (1) app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at "
                    f"FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? "
                    "ORDER BY version DESC",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                )
        return _row_to_stored_artifact(row, deserializer=self._json_deserializer) if row is not None else None

    def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """List distinct artifact filenames within user or session scope."""
        table_ref = mssql_table_ref(self._artifact_table)
        with self._config.provide_session() as driver:
            if session_id is not None:
                rows = driver.select(
                    f"SELECT DISTINCT filename FROM {table_ref} "
                    "WHERE app_name = ? AND user_id = ? AND session_id IN (N'', ?) ORDER BY filename ASC",
                    app_name,
                    user_id,
                    session_id,
                )
            else:
                rows = driver.select(
                    f"SELECT DISTINCT filename FROM {table_ref} "
                    "WHERE app_name = ? AND user_id = ? AND session_id = N'' ORDER BY filename ASC",
                    app_name,
                    user_id,
                )
        return [str(mssql_row_value(row, "filename", 0)) for row in rows]

    def list_artifact_versions(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> "list[StoredArtifact]":
        """List all stored versions for an artifact sorted ascending."""
        session_val = session_id if session_id is not None else ""
        table_ref = mssql_table_ref(self._artifact_table)
        with self._config.provide_session() as driver:
            rows = driver.select(
                "SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at "
                f"FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? "
                "ORDER BY version ASC",
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
        """Delete all versions for an artifact and return the deleted records."""
        records = self.list_artifact_versions(app_name, user_id, filename, session_id=session_id)
        session_val = session_id if session_id is not None else ""
        table_ref = mssql_table_ref(self._artifact_table)
        with self._config.provide_session() as driver:
            driver.execute(
                f"DELETE FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ?",
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
        table_ref = mssql_table_ref(self._artifact_table)
        with self._config.provide_session() as driver:
            max_ver = driver.select_value_or_none(
                f"SELECT MAX(version) FROM {table_ref} "
                "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ?",
                app_name,
                user_id,
                session_val,
                filename,
            )
        return (int(max_ver) + 1) if max_ver is not None else 0


class MSSQLAsyncADKArtifactStore(_BaseAsyncADKArtifactStore[Any]):
    """Asynchronous SQL Server ADK artifact metadata store supporting `ArtifactReference`."""

    __slots__ = ("_json_column_type", "_json_deserializer", "_json_serializer", "_native_json")

    adapter_name = "vertebra_mssql"

    def __init__(self, config: Any) -> None:
        """Initialize the asynchronous SQL Server ADK artifact store."""
        super().__init__(config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)
        native_json = _adk_settings(config).get("native_json")
        self._native_json: bool | None = native_json if isinstance(native_json, bool) else None
        self._json_column_type: str | None = _configured_json_type(self._native_json)

    async def _artifact_table_ddl(self) -> str:
        return _artifact_table_ddl(self._artifact_table, self._json_column_type or _JSON_FALLBACK_TYPE)

    async def create_table(self) -> None:
        """Create the SQL Server artifact metadata table asynchronously."""
        async with self._config.provide_session() as driver:
            if self._json_column_type is None:
                self._json_column_type = await _detect_json_type_async(driver, self._native_json)
            await driver.execute_script(_artifact_table_ddl(self._artifact_table, self._json_column_type))
            await driver.commit()

    async def insert_artifact(self, record: "StoredArtifact") -> None:
        """Insert or merge a `StoredArtifact` revision asynchronously."""
        session_val = record["session_id"] if record["session_id"] is not None else ""
        meta = record.get("custom_metadata")
        async with self._config.provide_session() as driver:
            await driver.execute(
                _merge_artifact_sql(self._artifact_table),
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
        """Upsert an artifact revision asynchronously backed by `ArtifactReference`."""
        session_val = session_id if session_id is not None else ""
        now_dt = reference.updated or datetime.now()
        async with self._config.provide_session() as driver:
            await driver.execute(
                _merge_artifact_ref_sql(self._artifact_table),
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
            await driver.commit()

    async def get_artifact_reference(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> ArtifactReference | None:
        """Fetch an `ArtifactReference` revision asynchronously by exact version or latest."""
        session_val = session_id if session_id is not None else ""
        table_ref = mssql_table_ref(self._artifact_table)
        async with self._config.provide_session() as driver:
            if version is not None:
                row = await driver.select_one_or_none(
                    f"SELECT TOP (1) * FROM {table_ref} "
                    "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? AND version = ?",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                    version,
                )
            else:
                row = await driver.select_one_or_none(
                    f"SELECT TOP (1) * FROM {table_ref} "
                    "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? "
                    "ORDER BY version DESC",
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
        """Fetch a `StoredArtifact` revision asynchronously by exact version or latest."""
        session_val = session_id if session_id is not None else ""
        table_ref = mssql_table_ref(self._artifact_table)
        async with self._config.provide_session() as driver:
            if version is not None:
                row = await driver.select_one_or_none(
                    "SELECT TOP (1) app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at "
                    f"FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? AND version = ?",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                    version,
                )
            else:
                row = await driver.select_one_or_none(
                    "SELECT TOP (1) app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at "
                    f"FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? "
                    "ORDER BY version DESC",
                    app_name,
                    user_id,
                    session_val,
                    filename,
                )
        return _row_to_stored_artifact(row, deserializer=self._json_deserializer) if row is not None else None

    async def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """List distinct artifact filenames asynchronously within user or session scope."""
        table_ref = mssql_table_ref(self._artifact_table)
        async with self._config.provide_session() as driver:
            if session_id is not None:
                rows = await driver.select(
                    f"SELECT DISTINCT filename FROM {table_ref} "
                    "WHERE app_name = ? AND user_id = ? AND session_id IN (N'', ?) ORDER BY filename ASC",
                    app_name,
                    user_id,
                    session_id,
                )
            else:
                rows = await driver.select(
                    f"SELECT DISTINCT filename FROM {table_ref} "
                    "WHERE app_name = ? AND user_id = ? AND session_id = N'' ORDER BY filename ASC",
                    app_name,
                    user_id,
                )
        return [str(mssql_row_value(row, "filename", 0)) for row in rows]

    async def list_artifact_versions(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> "list[StoredArtifact]":
        """List all stored versions for an artifact sorted ascending asynchronously."""
        session_val = session_id if session_id is not None else ""
        table_ref = mssql_table_ref(self._artifact_table)
        async with self._config.provide_session() as driver:
            rows = await driver.select(
                "SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at "
                f"FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? "
                "ORDER BY version ASC",
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
        """Delete all versions for an artifact asynchronously and return deleted records."""
        records = await self.list_artifact_versions(app_name, user_id, filename, session_id=session_id)
        session_val = session_id if session_id is not None else ""
        table_ref = mssql_table_ref(self._artifact_table)
        async with self._config.provide_session() as driver:
            await driver.execute(
                f"DELETE FROM {table_ref} WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ?",
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
        table_ref = mssql_table_ref(self._artifact_table)
        async with self._config.provide_session() as driver:
            max_ver = await driver.select_value_or_none(
                f"SELECT MAX(version) FROM {table_ref} "
                "WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ?",
                app_name,
                user_id,
                session_val,
                filename,
            )
        return (int(max_ver) + 1) if max_ver is not None else 0
