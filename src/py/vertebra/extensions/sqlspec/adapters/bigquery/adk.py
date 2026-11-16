"""BigQuery ADK session, memory (VECTOR_SEARCH/SEARCH), and ObjectRef->ArtifactReference stores."""

import math
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypedDict, cast

from sqlspec.extensions.adk.store import BaseAsyncADKStore, BaseSyncADKStore
from sqlspec.utils.serializers import from_json, to_json

from vertebra.db.adapters.bigquery import ObjectRef, ObjectTableConfig
from vertebra.extensions.sqlspec.adapters.bigquery.core import (
    bigquery_partition_clauses,
    is_bigquery_table_missing,
    object_ref_to_artifact_reference,
    parse_utc_timestamp,
    positive_int_or_none,
    qualify_store_table,
    quote_bigquery_identifier,
    resolve_storage_backend,
)
from vertebra.extensions.sqlspec.storage import ObjectStoreBackend
from vertebra.storage import ArtifactReference

if TYPE_CHECKING:
    from sqlspec.extensions.adk._types import StoredEvent, StoredSession
    from sqlspec.extensions.adk.artifact._types import StoredArtifact
    from sqlspec.extensions.adk.artifact.store import (
        BaseAsyncADKArtifactStore as _BaseAsyncADKArtifactStore,
    )
    from sqlspec.extensions.adk.artifact.store import (
        BaseSyncADKArtifactStore as _BaseSyncADKArtifactStore,
    )
    from sqlspec.extensions.adk.memory._types import StoredMemory
    from sqlspec.extensions.adk.memory.store import (
        BaseAsyncADKMemoryStore as _BaseAsyncADKMemoryStore,
    )
    from sqlspec.extensions.adk.memory.store import (
        BaseSyncADKMemoryStore as _BaseSyncADKMemoryStore,
    )

    SessionOrderBy = Literal["create_time", "update_time"]
else:
    try:
        from sqlspec.extensions.adk.memory.store import (
            BaseAsyncADKMemoryStore as _BaseAsyncADKMemoryStore,
        )
        from sqlspec.extensions.adk.memory.store import (
            BaseSyncADKMemoryStore as _BaseSyncADKMemoryStore,
        )
    except ImportError:

        class _FallbackADKMemoryCommon[ConfigT]:
            """Lightweight fallback ADK memory base when optional ``google.adk`` is absent."""

            adapter_name: ClassVar[str | None] = None

            def __init__(self, config: Any) -> None:
                self._config = config
                ext = getattr(config, "extension_config", {})
                adk = dict(ext.get("adk", {})) if isinstance(ext, dict) and isinstance(ext.get("adk"), dict) else {}
                self._enabled = bool(adk.get("enable_memory", True))
                self._memory_table = str(adk.get("memory_table", "adk_memory_entries"))
                self._use_fts = bool(adk.get("memory_use_fts", adk.get("use_fts", False)))
                self._max_results = int(adk.get("memory_max_results", adk.get("max_results", 20)))
                self._vector_index_type = str(adk.get("vector_index_type", "ivf"))
                self._vector_dimensions = int(adk.get("vector_dimensions", 768))
                self._enable_bm25 = bool(adk.get("enable_bm25", False))
                self._scann_num_leaves = int(adk.get("scann_num_leaves", 100))
                self._scann_quantizer = str(adk.get("scann_quantizer", "SQ8"))
                self._owner_id_column_ddl = adk.get("owner_id_column")
                self._owner_id_column_name = (
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
            def max_results(self) -> int:
                return self._max_results

        class _BaseSyncADKMemoryStore[ConfigT](_FallbackADKMemoryCommon[ConfigT]):
            """Fallback sync ADK memory store base."""

        class _BaseAsyncADKMemoryStore[ConfigT](_FallbackADKMemoryCommon[ConfigT]):
            """Fallback async ADK memory store base."""

    try:
        from sqlspec.extensions.adk.artifact.store import (
            BaseAsyncADKArtifactStore as _BaseAsyncADKArtifactStore,
        )
        from sqlspec.extensions.adk.artifact.store import (
            BaseSyncADKArtifactStore as _BaseSyncADKArtifactStore,
        )
    except ImportError:

        class _FallbackADKArtifactCommon[ArtifactConfigT]:
            """Lightweight fallback ADK artifact base when optional ``google.adk`` is absent."""

            adapter_name: ClassVar[str | None] = None

            def __init__(self, config: Any) -> None:
                self._config = config
                ext = getattr(config, "extension_config", {})
                adk = dict(ext.get("adk", {})) if isinstance(ext, dict) and isinstance(ext.get("adk"), dict) else {}
                self._artifact_table = str(adk.get("artifact_table", "adk_artifacts"))

            @property
            def config(self) -> Any:
                return self._config

            @property
            def artifact_table(self) -> str:
                return self._artifact_table

        class _BaseSyncADKArtifactStore[ArtifactConfigT](_FallbackADKArtifactCommon[ArtifactConfigT]):
            """Fallback sync ADK artifact store base."""

            def ensure_table(self) -> None:
                create_fn = getattr(self, "create_table", None)
                if callable(create_fn):
                    create_fn()

        class _BaseAsyncADKArtifactStore[ArtifactConfigT](_FallbackADKArtifactCommon[ArtifactConfigT]):
            """Fallback async ADK artifact store base."""

            async def ensure_table(self) -> None:
                create_fn = getattr(self, "create_table", None)
                if callable(create_fn):
                    await create_fn()


__all__ = (
    "BigQueryADKConfig",
    "BigQueryAsyncADKArtifactStore",
    "BigQueryAsyncADKMemoryStore",
    "BigQueryAsyncADKStore",
    "BigQuerySyncADKArtifactStore",
    "BigQuerySyncADKMemoryStore",
    "BigQuerySyncADKStore",
)


class BigQueryADKConfig(TypedDict, total=False):
    """Typed extension configuration for BigQuery ADK stores."""

    session_table: str
    """Qualified BigQuery table name for ADK sessions."""
    events_table: str
    """Qualified BigQuery table name for ADK session events."""
    app_state_table: str
    """Qualified BigQuery table name for application-scoped ADK state."""
    user_state_table: str
    """Qualified BigQuery table name for user-scoped ADK state."""
    metadata_table: str
    """Qualified BigQuery table name for ADK session metadata."""
    memory_table: str
    """Qualified BigQuery table name for ADK memory entries."""
    artifact_table: str
    """Qualified BigQuery table name for ADK artifact metadata."""
    artifact_storage_uri: str
    """Base object storage URI used for offloaded ADK artifact payloads."""
    memory_use_fts: bool
    """Enable BigQuery SEARCH index usage for memory retrieval queries."""
    memory_max_results: int
    """Default maximum number of memory entries returned by search queries."""
    owner_id_column: str
    """Column name referencing the tenant or owner identifier."""
    manage_schema: bool
    """Create or migrate ADK tables automatically on startup."""
    create_schema: bool
    """Create ADK dataset tables when they do not already exist."""
    with_partitioning: bool
    """Enable BigQuery time-based table partitioning on ADK tables."""
    partition_expiration_days: int
    """Partition retention window in days when partitioning is enabled."""
    require_partition_filter: bool
    """Require partition filter predicates in generated BigQuery table DDL."""
    lookup_window_days: int
    """Default lookback window in days when querying partitioned ADK tables."""
    vector_dimensions: int
    """Embedding dimensionality for ADK memory vector similarity columns."""
    vector_index_type: str
    """BigQuery vector index algorithm name for memory embeddings."""


def _extract_adk_extension(config: Any) -> dict[str, Any]:
    ext = getattr(config, "extension_config", {})
    if isinstance(ext, dict) and isinstance(ext.get("adk"), dict):
        return dict(ext["adk"])
    return {}


def _partition_settings(config: Any) -> tuple[bool, int | None, bool, int | None]:
    adk = _extract_adk_extension(config)
    with_partitioning = bool(adk.get("with_partitioning", True))
    expiration_days = positive_int_or_none(adk.get("partition_expiration_days"), "partition_expiration_days")
    require_filter = bool(adk.get("require_partition_filter", False))
    lookup_window_days = positive_int_or_none(adk.get("lookup_window_days"), "lookup_window_days")
    return with_partitioning, expiration_days, require_filter, lookup_window_days


def _decode_json_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode("utf-8")
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return {}
        parsed = from_json(stripped)
        return parsed if isinstance(parsed, dict) else {"value": parsed}
    return dict(value) if hasattr(value, "items") else {}


def _encode_json_text(value: Any) -> str:
    if value is None:
        return "{}"
    if isinstance(value, str):
        return value
    return to_json(value)


def _decode_embedding(value: Any) -> list[float] | None:
    if value is None:
        return None
    if isinstance(value, str):
        parsed = from_json(value)
        return [float(x) for x in parsed] if isinstance(parsed, Sequence) else None
    if isinstance(value, Sequence):
        return [float(x) for x in value]
    return None


def _cosine_similarity(vec_a: Sequence[float], vec_b: Sequence[float]) -> float:
    if not vec_a or not vec_b or len(vec_a) != len(vec_b):
        return 0.0
    dot = sum(float(a) * float(b) for a, b in zip(vec_a, vec_b, strict=True))
    norm_a = math.sqrt(sum(float(a) * float(a) for a in vec_a))
    norm_b = math.sqrt(sum(float(b) * float(b) for b in vec_b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)


def _keyword_score(query: str, text: str) -> float:
    terms = [term for term in query.lower().split() if term]
    if not terms:
        return 1.0
    haystack = text.lower()
    hits = sum(1 for term in terms if term in haystack)
    return hits / len(terms)


def _build_session_ddl(config: Any, session_table: str, owner_id_ddl: str | None) -> str:
    with_partitioning, expiration_days, require_filter, _ = _partition_settings(config)
    part_sql, opt_sql = bigquery_partition_clauses(
        "create_time",
        enabled=with_partitioning,
        expiration_days=expiration_days,
        require_filter=require_filter,
        cluster_by=("app_name", "user_id", "id"),
    )
    table_ref = qualify_store_table(config, session_table)
    owner_col = f"{owner_id_ddl}, " if owner_id_ddl else ""
    return (
        f"CREATE TABLE IF NOT EXISTS {table_ref} ("
        "id STRING NOT NULL, "
        "app_name STRING NOT NULL, "
        "user_id STRING NOT NULL, "
        f"{owner_col}"
        "state JSON NOT NULL, "
        "create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(), "
        "update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()"
        f"){part_sql}{opt_sql}"
    )


def _build_events_ddl(config: Any, events_table: str) -> str:
    with_partitioning, expiration_days, require_filter, _ = _partition_settings(config)
    part_sql, opt_sql = bigquery_partition_clauses(
        "timestamp",
        enabled=with_partitioning,
        expiration_days=expiration_days,
        require_filter=require_filter,
        cluster_by=("app_name", "user_id", "session_id"),
    )
    table_ref = qualify_store_table(config, events_table)
    return (
        f"CREATE TABLE IF NOT EXISTS {table_ref} ("
        "id STRING NOT NULL, "
        "app_name STRING NOT NULL, "
        "user_id STRING NOT NULL, "
        "session_id STRING NOT NULL, "
        "invocation_id STRING NOT NULL, "
        "timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(), "
        "event_data JSON NOT NULL"
        f"){part_sql}{opt_sql}"
    )


def _build_app_state_ddl(config: Any, table_name: str) -> str:
    table_ref = qualify_store_table(config, table_name)
    return (
        f"CREATE TABLE IF NOT EXISTS {table_ref} ("
        "app_name STRING NOT NULL, "
        "state JSON NOT NULL, "
        "update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()"
        ")"
    )


def _build_user_state_ddl(config: Any, table_name: str) -> str:
    table_ref = qualify_store_table(config, table_name)
    return (
        f"CREATE TABLE IF NOT EXISTS {table_ref} ("
        "app_name STRING NOT NULL, "
        "user_id STRING NOT NULL, "
        "state JSON NOT NULL, "
        "update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()"
        ")"
    )


def _build_metadata_ddl(config: Any, table_name: str) -> str:
    table_ref = qualify_store_table(config, table_name)
    return (
        f"CREATE TABLE IF NOT EXISTS {table_ref} ("
        "`key` STRING NOT NULL, "
        "value STRING NOT NULL, "
        "update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()"
        ")"
    )


def _row_to_stored_session(row: dict[str, Any]) -> "StoredSession":
    return cast(
        "StoredSession",
        {
            "id": str(row.get("id") or ""),
            "app_name": str(row.get("app_name") or ""),
            "user_id": str(row.get("user_id") or ""),
            "state": _decode_json_dict(row.get("state")),
            "create_time": parse_utc_timestamp(row.get("create_time")),
            "update_time": parse_utc_timestamp(row.get("update_time")),
        },
    )


def _row_to_stored_event(row: dict[str, Any]) -> "StoredEvent":
    event_data = _decode_json_dict(row.get("event_data"))
    if not event_data and row.get("event_json") is not None:
        event_data = _decode_json_dict(row.get("event_json"))
    return cast(
        "StoredEvent",
        {
            "id": str(row.get("id") or ""),
            "app_name": str(row.get("app_name") or ""),
            "user_id": str(row.get("user_id") or ""),
            "session_id": str(row.get("session_id") or ""),
            "invocation_id": str(row.get("invocation_id") or ""),
            "timestamp": parse_utc_timestamp(row.get("timestamp")),
            "event_data": event_data,
        },
    )


def _row_to_stored_memory(row: dict[str, Any]) -> "StoredMemory":
    return cast(
        "StoredMemory",
        {
            "id": str(row.get("id") or ""),
            "session_id": str(row.get("session_id") or ""),
            "app_name": str(row.get("app_name") or ""),
            "user_id": str(row.get("user_id") or ""),
            "scope": str(row.get("scope") or "user"),
            "event_id": str(row.get("event_id") or ""),
            "author": row.get("author"),
            "timestamp": parse_utc_timestamp(row.get("timestamp")),
            "content_json": _decode_json_dict(row.get("content_json")),
            "content_text": str(row.get("content_text") or ""),
            "metadata_json": _decode_json_dict(row.get("metadata_json"))
            if row.get("metadata_json") is not None
            else None,
            "inserted_at": parse_utc_timestamp(row.get("inserted_at")),
            "embedding": _decode_embedding(row.get("embedding")),
        },
    )


def _row_to_stored_artifact(row: dict[str, Any]) -> "StoredArtifact":
    custom_raw = row.get("custom_metadata")
    return cast(
        "StoredArtifact",
        {
            "app_name": str(row.get("app_name") or ""),
            "user_id": str(row.get("user_id") or ""),
            "session_id": row.get("session_id"),
            "filename": str(row.get("filename") or ""),
            "version": int(row.get("version", 0) or 0),
            "mime_type": row.get("mime_type"),
            "canonical_uri": str(row.get("canonical_uri") or ""),
            "custom_metadata": _decode_json_dict(custom_raw) if custom_raw is not None else None,
            "created_at": parse_utc_timestamp(row.get("created_at")),
        },
    )


class BigQuerySyncADKStore(BaseSyncADKStore[Any]):
    """BigQuery synchronous ADK store using partitioned/clustered tables and GoogleSQL ``MERGE``."""

    __slots__ = ()

    adapter_name = "vertebra_bigquery"

    def _sessions_table_ddl(self) -> str:
        return _build_session_ddl(self._config, self._session_table, self._owner_id_column_ddl)

    def _events_table_ddl(self) -> str:
        return _build_events_ddl(self._config, self._events_table)

    def _app_states_table_ddl(self) -> str:
        return _build_app_state_ddl(self._config, self._app_state_table)

    def _user_states_table_ddl(self) -> str:
        return _build_user_state_ddl(self._config, self._user_state_table)

    def _metadata_table_ddl(self) -> str:
        return _build_metadata_ddl(self._config, self._metadata_table)

    def _drop_tables_sql(self) -> list[str]:
        return [
            f"DROP TABLE IF EXISTS {qualify_store_table(self._config, self._events_table)}",
            f"DROP TABLE IF EXISTS {qualify_store_table(self._config, self._session_table)}",
        ]

    def _drop_app_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {qualify_store_table(self._config, self._app_state_table)}"

    def _drop_user_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {qualify_store_table(self._config, self._user_state_table)}"

    def _drop_metadata_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {qualify_store_table(self._config, self._metadata_table)}"

    def create_tables(self) -> None:
        """Create all ADK session, event, app-state, user-state, and metadata tables."""
        ddls = [
            self._sessions_table_ddl(),
            self._events_table_ddl(),
            self._app_states_table_ddl(),
            self._user_states_table_ddl(),
            self._metadata_table_ddl(),
        ]
        with self._config.provide_session() as driver:
            for ddl in ddls:
                driver.execute(ddl)

    def create_session(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
        state: dict[str, Any],
        owner_id: Any | None = None,
    ) -> "StoredSession":
        """Insert a new ADK session row."""
        now = datetime.now(UTC)
        now_iso = now.isoformat()
        table_ref = qualify_store_table(self._config, self._session_table)
        state_json = _encode_json_text(state)
        if self._owner_id_column_name:
            owner_col = quote_bigquery_identifier(self._owner_id_column_name)
            sql = (
                f"INSERT INTO {table_ref} (id, app_name, user_id, {owner_col}, state, create_time, update_time) "
                "VALUES (@id, @app_name, @user_id, @owner_id, PARSE_JSON(@state), @create_time, @update_time)"
            )
            params: dict[str, Any] = {
                "id": session_id,
                "app_name": app_name,
                "user_id": user_id,
                "owner_id": owner_id,
                "state": state_json,
                "create_time": now_iso,
                "update_time": now_iso,
            }
        else:
            sql = (
                f"INSERT INTO {table_ref} (id, app_name, user_id, state, create_time, update_time) "
                "VALUES (@id, @app_name, @user_id, PARSE_JSON(@state), @create_time, @update_time)"
            )
            params = {
                "id": session_id,
                "app_name": app_name,
                "user_id": user_id,
                "state": state_json,
                "create_time": now_iso,
                "update_time": now_iso,
            }
        with self._config.provide_session() as driver:
            driver.execute(sql, params)
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
        """Load an ADK session optionally refreshing its ``update_time``."""
        _ = renew_for
        table_ref = qualify_store_table(self._config, self._session_table)
        _, _, _, lookup_window_days = _partition_settings(self._config)
        window_clause = ""
        params: dict[str, Any] = {"id": session_id, "app_name": app_name, "user_id": user_id}
        if lookup_window_days is not None:
            cutoff = (datetime.now(UTC) - timedelta(days=lookup_window_days)).isoformat()
            window_clause = " AND create_time >= @window_start"
            params["window_start"] = cutoff
        sql = (
            f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table_ref} "
            f"WHERE id = @id AND app_name = @app_name AND user_id = @user_id{window_clause} LIMIT 1"
        )
        with self._config.provide_session() as driver:
            row = driver.select_one_or_none(sql, params)
        return _row_to_stored_session(dict(row)) if row is not None else None

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Update JSON state and ``update_time`` for an existing ADK session."""
        table_ref = qualify_store_table(self._config, self._session_table)
        sql = (
            f"UPDATE {table_ref} SET state = PARSE_JSON(@state), update_time = @update_time "
            "WHERE id = @id AND app_name = @app_name AND user_id = @user_id"
        )
        with self._config.provide_session() as driver:
            driver.execute(
                sql,
                {
                    "id": session_id,
                    "app_name": app_name,
                    "user_id": user_id,
                    "state": _encode_json_text(state),
                    "update_time": datetime.now(UTC).isoformat(),
                },
            )

    def list_sessions(
        self,
        app_name: str,
        user_id: str | None = None,
        *,
        order_by: "SessionOrderBy" = "update_time",
        descending: bool = True,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list["StoredSession"]:
        """List ADK sessions filtered by application and optional user."""
        table_ref = qualify_store_table(self._config, self._session_table)
        order_col = "create_time" if order_by == "create_time" else "update_time"
        direction = "DESC" if descending else "ASC"
        where = "app_name = @app_name" + (" AND user_id = @user_id" if user_id is not None else "")
        limit_sql = " LIMIT @limit" if limit is not None else ""
        offset_sql = " OFFSET @offset" if offset is not None else ""
        sql = (
            f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table_ref} "
            f"WHERE {where} ORDER BY {order_col} {direction}{limit_sql}{offset_sql}"
        )
        params: dict[str, Any] = {"app_name": app_name}
        if user_id is not None:
            params["user_id"] = user_id
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        with self._config.provide_session() as driver:
            rows = driver.select(sql, params)
        return [_row_to_stored_session(dict(row)) for row in rows]

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete a session and its associated events."""
        sessions_ref = qualify_store_table(self._config, self._session_table)
        events_ref = qualify_store_table(self._config, self._events_table)
        with self._config.provide_session() as driver:
            driver.execute(
                f"DELETE FROM {events_ref} WHERE session_id = @id AND app_name = @app_name AND user_id = @user_id",
                {"id": session_id, "app_name": app_name, "user_id": user_id},
            )
            driver.execute(
                f"DELETE FROM {sessions_ref} WHERE id = @id AND app_name = @app_name AND user_id = @user_id",
                {"id": session_id, "app_name": app_name, "user_id": user_id},
            )

    def append_event(self, event_record: "StoredEvent") -> None:
        """Append a single ADK event row."""
        events_ref = qualify_store_table(self._config, self._events_table)
        ts = parse_utc_timestamp(event_record["timestamp"]).isoformat()
        sql = (
            f"INSERT INTO {events_ref} "
            "(id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
            "VALUES (@id, @app_name, @user_id, @session_id, @invocation_id, @timestamp, PARSE_JSON(@event_data))"
        )
        params = {
            "id": event_record["id"],
            "app_name": event_record["app_name"],
            "user_id": event_record["user_id"],
            "session_id": event_record["session_id"],
            "invocation_id": event_record["invocation_id"],
            "timestamp": ts,
            "event_data": _encode_json_text(event_record.get("event_data", {})),
        }
        with self._config.provide_session() as driver:
            driver.execute(sql, params)

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
        """Append an event and update session, app, and user state maps."""
        self.append_event(event_record)
        self.update_session_state(app_name, user_id, session_id, state)
        if app_state is not None:
            self.upsert_app_state(app_name, app_state)
        if user_state is not None:
            self.upsert_user_state(app_name, user_id, user_state)
        session = self.get_session(app_name, user_id, session_id)
        if session is not None:
            return session
        now = datetime.now(UTC)
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

    def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> list["StoredEvent"]:
        """Fetch chronological ADK events for a session."""
        events_ref = qualify_store_table(self._config, self._events_table)
        where = "app_name = @app_name AND user_id = @user_id AND session_id = @session_id"
        params: dict[str, Any] = {"app_name": app_name, "user_id": user_id, "session_id": session_id}
        if after_timestamp is not None:
            where += " AND timestamp > @after_timestamp"
            params["after_timestamp"] = parse_utc_timestamp(after_timestamp).isoformat()
        limit_sql = " LIMIT @limit" if limit is not None else ""
        if limit is not None:
            params["limit"] = limit
        sql = (
            f"SELECT id, app_name, user_id, session_id, invocation_id, timestamp, event_data "
            f"FROM {events_ref} WHERE {where} ORDER BY timestamp ASC{limit_sql}"
        )
        with self._config.provide_session() as driver:
            rows = driver.select(sql, params)
        return [_row_to_stored_event(dict(row)) for row in rows]

    def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Load shared application-scoped ADK state."""
        table_ref = qualify_store_table(self._config, self._app_state_table)
        sql = f"SELECT state FROM {table_ref} WHERE app_name = @app_name LIMIT 1"
        with self._config.provide_session() as driver:
            row = driver.select_one_or_none(sql, {"app_name": app_name})
        return _decode_json_dict(row.get("state")) if row is not None else None

    def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Upsert shared application-scoped ADK state using GoogleSQL ``MERGE``."""
        table_ref = qualify_store_table(self._config, self._app_state_table)
        sql = (
            f"MERGE INTO {table_ref} AS target "
            "USING (SELECT @app_name AS app_name, PARSE_JSON(@state) AS state) AS source "
            "ON target.app_name = source.app_name "
            "WHEN MATCHED THEN UPDATE SET state = source.state, update_time = CURRENT_TIMESTAMP() "
            "WHEN NOT MATCHED THEN INSERT (app_name, state, update_time) "
            "VALUES (source.app_name, source.state, CURRENT_TIMESTAMP())"
        )
        with self._config.provide_session() as driver:
            driver.execute(sql, {"app_name": app_name, "state": _encode_json_text(state)})

    def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Load user-scoped ADK state for an application."""
        table_ref = qualify_store_table(self._config, self._user_state_table)
        sql = f"SELECT state FROM {table_ref} WHERE app_name = @app_name AND user_id = @user_id LIMIT 1"
        with self._config.provide_session() as driver:
            row = driver.select_one_or_none(sql, {"app_name": app_name, "user_id": user_id})
        return _decode_json_dict(row.get("state")) if row is not None else None

    def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Upsert user-scoped ADK state using GoogleSQL ``MERGE``."""
        table_ref = qualify_store_table(self._config, self._user_state_table)
        sql = (
            f"MERGE INTO {table_ref} AS target "
            "USING (SELECT @app_name AS app_name, @user_id AS user_id, PARSE_JSON(@state) AS state) AS source "
            "ON target.app_name = source.app_name AND target.user_id = source.user_id "
            "WHEN MATCHED THEN UPDATE SET state = source.state, update_time = CURRENT_TIMESTAMP() "
            "WHEN NOT MATCHED THEN INSERT (app_name, user_id, state, update_time) "
            "VALUES (source.app_name, source.user_id, source.state, CURRENT_TIMESTAMP())"
        )
        with self._config.provide_session() as driver:
            driver.execute(sql, {"app_name": app_name, "user_id": user_id, "state": _encode_json_text(state)})

    def get_metadata(self, key: str) -> str | None:
        """Retrieve a scalar ADK store metadata value."""
        table_ref = qualify_store_table(self._config, self._metadata_table)
        sql = f"SELECT value FROM {table_ref} WHERE `key` = @key LIMIT 1"
        with self._config.provide_session() as driver:
            row = driver.select_one_or_none(sql, {"key": key})
        return str(row["value"]) if row is not None and row.get("value") is not None else None

    def set_metadata(self, key: str, value: str) -> None:
        """Persist a scalar ADK store metadata value using ``MERGE``."""
        table_ref = qualify_store_table(self._config, self._metadata_table)
        sql = (
            f"MERGE INTO {table_ref} AS target "
            "USING (SELECT @key AS `key`, @value AS value) AS source "
            "ON target.`key` = source.`key` "
            "WHEN MATCHED THEN UPDATE SET value = source.value, update_time = CURRENT_TIMESTAMP() "
            "WHEN NOT MATCHED THEN INSERT (`key`, value, update_time) "
            "VALUES (source.`key`, source.value, CURRENT_TIMESTAMP())"
        )
        with self._config.provide_session() as driver:
            driver.execute(sql, {"key": key, "value": value})

    def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Delete ADK events older than ``before``."""
        events_ref = qualify_store_table(self._config, self._events_table)
        where = "timestamp < @before" + (" AND app_name = @app_name" if app_name is not None else "")
        params: dict[str, Any] = {"before": parse_utc_timestamp(before).isoformat()}
        if app_name is not None:
            params["app_name"] = app_name
        with self._config.provide_session() as driver:
            res = driver.execute(f"DELETE FROM {events_ref} WHERE {where}", params)
        return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)

    def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete ADK sessions inactive since ``updated_before``."""
        sessions_ref = qualify_store_table(self._config, self._session_table)
        where = "update_time < @before" + (" AND app_name = @app_name" if app_name is not None else "")
        params: dict[str, Any] = {"before": parse_utc_timestamp(updated_before).isoformat()}
        if app_name is not None:
            params["app_name"] = app_name
        with self._config.provide_session() as driver:
            res = driver.execute(f"DELETE FROM {sessions_ref} WHERE {where}", params)
        return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)

    def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete user state rows inactive since ``updated_before``."""
        table_ref = qualify_store_table(self._config, self._user_state_table)
        where = "update_time < @before" + (" AND app_name = @app_name" if app_name is not None else "")
        params: dict[str, Any] = {"before": parse_utc_timestamp(updated_before).isoformat()}
        if app_name is not None:
            params["app_name"] = app_name
        with self._config.provide_session() as driver:
            res = driver.execute(f"DELETE FROM {table_ref} WHERE {where}", params)
        return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)


class BigQueryAsyncADKStore(BaseAsyncADKStore[Any]):
    """BigQuery asynchronous ADK store using partitioned/clustered tables and GoogleSQL ``MERGE``."""

    __slots__ = ()

    adapter_name = "vertebra_bigquery"

    async def _sessions_table_ddl(self) -> str:
        return _build_session_ddl(self._config, self._session_table, self._owner_id_column_ddl)

    async def _events_table_ddl(self) -> str:
        return _build_events_ddl(self._config, self._events_table)

    async def _app_states_table_ddl(self) -> str:
        return _build_app_state_ddl(self._config, self._app_state_table)

    async def _user_states_table_ddl(self) -> str:
        return _build_user_state_ddl(self._config, self._user_state_table)

    async def _metadata_table_ddl(self) -> str:
        return _build_metadata_ddl(self._config, self._metadata_table)

    def _drop_tables_sql(self) -> list[str]:
        return [
            f"DROP TABLE IF EXISTS {qualify_store_table(self._config, self._events_table)}",
            f"DROP TABLE IF EXISTS {qualify_store_table(self._config, self._session_table)}",
        ]

    def _drop_app_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {qualify_store_table(self._config, self._app_state_table)}"

    def _drop_user_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {qualify_store_table(self._config, self._user_state_table)}"

    def _drop_metadata_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {qualify_store_table(self._config, self._metadata_table)}"

    async def create_tables(self) -> None:
        """Create all ADK tables asynchronously."""
        ddls = [
            await self._sessions_table_ddl(),
            await self._events_table_ddl(),
            await self._app_states_table_ddl(),
            await self._user_states_table_ddl(),
            await self._metadata_table_ddl(),
        ]
        async with self._config.provide_session() as driver:
            for ddl in ddls:
                await driver.execute(ddl)

    async def create_session(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
        state: dict[str, Any],
        owner_id: Any | None = None,
    ) -> "StoredSession":
        """Insert a new ADK session row asynchronously."""
        now = datetime.now(UTC)
        now_iso = now.isoformat()
        table_ref = qualify_store_table(self._config, self._session_table)
        state_json = _encode_json_text(state)
        if self._owner_id_column_name:
            owner_col = quote_bigquery_identifier(self._owner_id_column_name)
            sql = (
                f"INSERT INTO {table_ref} (id, app_name, user_id, {owner_col}, state, create_time, update_time) "
                "VALUES (@id, @app_name, @user_id, @owner_id, PARSE_JSON(@state), @create_time, @update_time)"
            )
            params: dict[str, Any] = {
                "id": session_id,
                "app_name": app_name,
                "user_id": user_id,
                "owner_id": owner_id,
                "state": state_json,
                "create_time": now_iso,
                "update_time": now_iso,
            }
        else:
            sql = (
                f"INSERT INTO {table_ref} (id, app_name, user_id, state, create_time, update_time) "
                "VALUES (@id, @app_name, @user_id, PARSE_JSON(@state), @create_time, @update_time)"
            )
            params = {
                "id": session_id,
                "app_name": app_name,
                "user_id": user_id,
                "state": state_json,
                "create_time": now_iso,
                "update_time": now_iso,
            }
        async with self._config.provide_session() as driver:
            await driver.execute(sql, params)
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
        """Load an ADK session asynchronously."""
        _ = renew_for
        table_ref = qualify_store_table(self._config, self._session_table)
        _, _, _, lookup_window_days = _partition_settings(self._config)
        window_clause = ""
        params: dict[str, Any] = {"id": session_id, "app_name": app_name, "user_id": user_id}
        if lookup_window_days is not None:
            cutoff = (datetime.now(UTC) - timedelta(days=lookup_window_days)).isoformat()
            window_clause = " AND create_time >= @window_start"
            params["window_start"] = cutoff
        sql = (
            f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table_ref} "
            f"WHERE id = @id AND app_name = @app_name AND user_id = @user_id{window_clause} LIMIT 1"
        )
        async with self._config.provide_session() as driver:
            row = await driver.select_one_or_none(sql, params)
        return _row_to_stored_session(dict(row)) if row is not None else None

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Update JSON session state asynchronously."""
        table_ref = qualify_store_table(self._config, self._session_table)
        sql = (
            f"UPDATE {table_ref} SET state = PARSE_JSON(@state), update_time = @update_time "
            "WHERE id = @id AND app_name = @app_name AND user_id = @user_id"
        )
        async with self._config.provide_session() as driver:
            await driver.execute(
                sql,
                {
                    "id": session_id,
                    "app_name": app_name,
                    "user_id": user_id,
                    "state": _encode_json_text(state),
                    "update_time": datetime.now(UTC).isoformat(),
                },
            )

    async def list_sessions(
        self,
        app_name: str,
        user_id: str | None = None,
        *,
        order_by: "SessionOrderBy" = "update_time",
        descending: bool = True,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list["StoredSession"]:
        """List ADK sessions asynchronously."""
        table_ref = qualify_store_table(self._config, self._session_table)
        order_col = "create_time" if order_by == "create_time" else "update_time"
        direction = "DESC" if descending else "ASC"
        where = "app_name = @app_name" + (" AND user_id = @user_id" if user_id is not None else "")
        limit_sql = " LIMIT @limit" if limit is not None else ""
        offset_sql = " OFFSET @offset" if offset is not None else ""
        sql = (
            f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table_ref} "
            f"WHERE {where} ORDER BY {order_col} {direction}{limit_sql}{offset_sql}"
        )
        params: dict[str, Any] = {"app_name": app_name}
        if user_id is not None:
            params["user_id"] = user_id
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        async with self._config.provide_session() as driver:
            rows = await driver.select(sql, params)
        return [_row_to_stored_session(dict(row)) for row in rows]

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete a session and its events asynchronously."""
        sessions_ref = qualify_store_table(self._config, self._session_table)
        events_ref = qualify_store_table(self._config, self._events_table)
        async with self._config.provide_session() as driver:
            await driver.execute(
                f"DELETE FROM {events_ref} WHERE session_id = @id AND app_name = @app_name AND user_id = @user_id",
                {"id": session_id, "app_name": app_name, "user_id": user_id},
            )
            await driver.execute(
                f"DELETE FROM {sessions_ref} WHERE id = @id AND app_name = @app_name AND user_id = @user_id",
                {"id": session_id, "app_name": app_name, "user_id": user_id},
            )

    async def append_event(self, event_record: "StoredEvent") -> None:
        """Append an ADK event row asynchronously."""
        events_ref = qualify_store_table(self._config, self._events_table)
        ts = parse_utc_timestamp(event_record["timestamp"]).isoformat()
        sql = (
            f"INSERT INTO {events_ref} "
            "(id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
            "VALUES (@id, @app_name, @user_id, @session_id, @invocation_id, @timestamp, PARSE_JSON(@event_data))"
        )
        params = {
            "id": event_record["id"],
            "app_name": event_record["app_name"],
            "user_id": event_record["user_id"],
            "session_id": event_record["session_id"],
            "invocation_id": event_record["invocation_id"],
            "timestamp": ts,
            "event_data": _encode_json_text(event_record.get("event_data", {})),
        }
        async with self._config.provide_session() as driver:
            await driver.execute(sql, params)

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
        """Append an event and update session, app, and user state maps asynchronously."""
        await self.append_event(event_record)
        await self.update_session_state(app_name, user_id, session_id, state)
        if app_state is not None:
            await self.upsert_app_state(app_name, app_state)
        if user_state is not None:
            await self.upsert_user_state(app_name, user_id, user_state)
        session = await self.get_session(app_name, user_id, session_id)
        if session is not None:
            return session
        now = datetime.now(UTC)
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

    async def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> list["StoredEvent"]:
        """Fetch chronological ADK events asynchronously."""
        events_ref = qualify_store_table(self._config, self._events_table)
        where = "app_name = @app_name AND user_id = @user_id AND session_id = @session_id"
        params: dict[str, Any] = {"app_name": app_name, "user_id": user_id, "session_id": session_id}
        if after_timestamp is not None:
            where += " AND timestamp > @after_timestamp"
            params["after_timestamp"] = parse_utc_timestamp(after_timestamp).isoformat()
        limit_sql = " LIMIT @limit" if limit is not None else ""
        if limit is not None:
            params["limit"] = limit
        sql = (
            f"SELECT id, app_name, user_id, session_id, invocation_id, timestamp, event_data "
            f"FROM {events_ref} WHERE {where} ORDER BY timestamp ASC{limit_sql}"
        )
        async with self._config.provide_session() as driver:
            rows = await driver.select(sql, params)
        return [_row_to_stored_event(dict(row)) for row in rows]

    async def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Load shared application state asynchronously."""
        table_ref = qualify_store_table(self._config, self._app_state_table)
        sql = f"SELECT state FROM {table_ref} WHERE app_name = @app_name LIMIT 1"
        async with self._config.provide_session() as driver:
            row = await driver.select_one_or_none(sql, {"app_name": app_name})
        return _decode_json_dict(row.get("state")) if row is not None else None

    async def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Upsert shared application state asynchronously using ``MERGE``."""
        table_ref = qualify_store_table(self._config, self._app_state_table)
        sql = (
            f"MERGE INTO {table_ref} AS target "
            "USING (SELECT @app_name AS app_name, PARSE_JSON(@state) AS state) AS source "
            "ON target.app_name = source.app_name "
            "WHEN MATCHED THEN UPDATE SET state = source.state, update_time = CURRENT_TIMESTAMP() "
            "WHEN NOT MATCHED THEN INSERT (app_name, state, update_time) "
            "VALUES (source.app_name, source.state, CURRENT_TIMESTAMP())"
        )
        async with self._config.provide_session() as driver:
            await driver.execute(sql, {"app_name": app_name, "state": _encode_json_text(state)})

    async def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Load user-scoped ADK state asynchronously."""
        table_ref = qualify_store_table(self._config, self._user_state_table)
        sql = f"SELECT state FROM {table_ref} WHERE app_name = @app_name AND user_id = @user_id LIMIT 1"
        async with self._config.provide_session() as driver:
            row = await driver.select_one_or_none(sql, {"app_name": app_name, "user_id": user_id})
        return _decode_json_dict(row.get("state")) if row is not None else None

    async def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Upsert user-scoped ADK state asynchronously using ``MERGE``."""
        table_ref = qualify_store_table(self._config, self._user_state_table)
        sql = (
            f"MERGE INTO {table_ref} AS target "
            "USING (SELECT @app_name AS app_name, @user_id AS user_id, PARSE_JSON(@state) AS state) AS source "
            "ON target.app_name = source.app_name AND target.user_id = source.user_id "
            "WHEN MATCHED THEN UPDATE SET state = source.state, update_time = CURRENT_TIMESTAMP() "
            "WHEN NOT MATCHED THEN INSERT (app_name, user_id, state, update_time) "
            "VALUES (source.app_name, source.user_id, source.state, CURRENT_TIMESTAMP())"
        )
        async with self._config.provide_session() as driver:
            await driver.execute(sql, {"app_name": app_name, "user_id": user_id, "state": _encode_json_text(state)})

    async def get_metadata(self, key: str) -> str | None:
        """Retrieve a scalar ADK store metadata value asynchronously."""
        table_ref = qualify_store_table(self._config, self._metadata_table)
        sql = f"SELECT value FROM {table_ref} WHERE `key` = @key LIMIT 1"
        async with self._config.provide_session() as driver:
            row = await driver.select_one_or_none(sql, {"key": key})
        return str(row["value"]) if row is not None and row.get("value") is not None else None

    async def set_metadata(self, key: str, value: str) -> None:
        """Persist a scalar ADK store metadata value asynchronously using ``MERGE``."""
        table_ref = qualify_store_table(self._config, self._metadata_table)
        sql = (
            f"MERGE INTO {table_ref} AS target "
            "USING (SELECT @key AS `key`, @value AS value) AS source "
            "ON target.`key` = source.`key` "
            "WHEN MATCHED THEN UPDATE SET value = source.value, update_time = CURRENT_TIMESTAMP() "
            "WHEN NOT MATCHED THEN INSERT (`key`, value, update_time) "
            "VALUES (source.`key`, source.value, CURRENT_TIMESTAMP())"
        )
        async with self._config.provide_session() as driver:
            await driver.execute(sql, {"key": key, "value": value})

    async def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Delete expired ADK events asynchronously."""
        events_ref = qualify_store_table(self._config, self._events_table)
        where = "timestamp < @before" + (" AND app_name = @app_name" if app_name is not None else "")
        params: dict[str, Any] = {"before": parse_utc_timestamp(before).isoformat()}
        if app_name is not None:
            params["app_name"] = app_name
        async with self._config.provide_session() as driver:
            res = await driver.execute(f"DELETE FROM {events_ref} WHERE {where}", params)
        return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)

    async def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete idle ADK sessions asynchronously."""
        sessions_ref = qualify_store_table(self._config, self._session_table)
        where = "update_time < @before" + (" AND app_name = @app_name" if app_name is not None else "")
        params: dict[str, Any] = {"before": parse_utc_timestamp(updated_before).isoformat()}
        if app_name is not None:
            params["app_name"] = app_name
        async with self._config.provide_session() as driver:
            res = await driver.execute(f"DELETE FROM {sessions_ref} WHERE {where}", params)
        return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)

    async def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete idle user state rows asynchronously."""
        table_ref = qualify_store_table(self._config, self._user_state_table)
        where = "update_time < @before" + (" AND app_name = @app_name" if app_name is not None else "")
        params: dict[str, Any] = {"before": parse_utc_timestamp(updated_before).isoformat()}
        if app_name is not None:
            params["app_name"] = app_name
        async with self._config.provide_session() as driver:
            res = await driver.execute(f"DELETE FROM {table_ref} WHERE {where}", params)
        return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)


def _memory_ddl_statements(config: Any, memory_table: str, owner_id_ddl: str | None, *, use_fts: bool) -> list[str]:
    with_partitioning, expiration_days, require_filter, _ = _partition_settings(config)
    part_sql, opt_sql = bigquery_partition_clauses(
        "timestamp",
        enabled=with_partitioning,
        expiration_days=expiration_days,
        require_filter=require_filter,
        cluster_by=("app_name", "user_id", "session_id"),
    )
    table_ref = qualify_store_table(config, memory_table)
    owner_col = f"{owner_id_ddl}, " if owner_id_ddl else ""
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {table_ref} ("
        "id STRING NOT NULL, "
        "session_id STRING NOT NULL, "
        "app_name STRING NOT NULL, "
        "user_id STRING NOT NULL, "
        f"{owner_col}"
        "scope STRING NOT NULL DEFAULT 'user', "
        "event_id STRING NOT NULL, "
        "author STRING, "
        "timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(), "
        "content_json JSON NOT NULL, "
        "content_text STRING NOT NULL, "
        "metadata_json JSON, "
        "inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(), "
        "embedding ARRAY<FLOAT64>"
        f"){part_sql}{opt_sql}"
    )
    statements = [ddl]
    if use_fts:
        idx_name = quote_bigquery_identifier(f"{memory_table.replace('.', '_')}_fts_idx")
        statements.append(f"CREATE SEARCH INDEX IF NOT EXISTS {idx_name} ON {table_ref}(content_text)")
    return statements


def _scope_sql_and_params(app_name: str, user_id: str, scope_filter: str) -> tuple[str, dict[str, Any]]:
    params: dict[str, Any] = {"app_name": app_name, "user_id": user_id}
    if scope_filter == "user":
        return "app_name = @app_name AND user_id = @user_id AND scope = 'user'", params
    if scope_filter == "app":
        return "app_name = @app_name AND scope = 'app'", params
    return "app_name = @app_name AND (user_id = @user_id OR scope = 'app')", params


def _rank_memory_rows(
    rows: list[dict[str, Any]],
    *,
    query: str,
    embedding: Sequence[float] | None,
    top_k: int,
) -> list["StoredMemory"]:
    scored: list[tuple[float, dict[str, Any]]] = []
    for row in rows:
        dist = row.get("distance")
        if embedding is not None:
            row_emb = _decode_embedding(row.get("embedding"))
            if row_emb is not None:
                score = _cosine_similarity(embedding, row_emb)
            elif dist is not None:
                score = 1.0 / (1.0 + float(dist))
            else:
                score = 0.0
        else:
            score = _keyword_score(query, str(row.get("content_text") or ""))
        if embedding is None and query.strip() and score <= 0.0 and not rows:
            continue
        scored.append((score, row))
    scored.sort(key=lambda item: item[0], reverse=True)
    return [_row_to_stored_memory(item[1]) for item in scored[:top_k]]


class _BigQueryADKMemoryMixin:
    """Shared SQL generation for BigQuery ``VECTOR_SEARCH`` and ``SEARCH`` ADK memory stores."""

    def build_vector_index_sql(self, index_name: str | None = None) -> str:
        """Return a GoogleSQL ``CREATE VECTOR INDEX`` statement for the memory embedding column."""
        table_name = cast(Any, self)._memory_table
        table_ref = qualify_store_table(cast(Any, self)._config, table_name)
        resolved = quote_bigquery_identifier(index_name or f"{table_name.replace('.', '_')}_vec_idx")
        return (
            f"CREATE VECTOR INDEX IF NOT EXISTS {resolved} ON {table_ref}(embedding) "
            "OPTIONS(index_type = 'IVF', distance_type = 'COSINE')"
        )

    def build_search_index_sql(self, index_name: str | None = None) -> str:
        """Return a GoogleSQL ``CREATE SEARCH INDEX`` statement for full-text memory content."""
        table_name = cast(Any, self)._memory_table
        table_ref = qualify_store_table(cast(Any, self)._config, table_name)
        resolved = quote_bigquery_identifier(index_name or f"{table_name.replace('.', '_')}_fts_idx")
        return f"CREATE SEARCH INDEX IF NOT EXISTS {resolved} ON {table_ref}(content_text)"

    def build_vector_search_sql(
        self,
        *,
        app_name: str,
        user_id: str,
        scope_filter: str = "all",
        top_k: int = 20,
    ) -> tuple[str, dict[str, Any]]:
        """Build a GoogleSQL ``VECTOR_SEARCH`` query with ``top_k`` and scope filtering."""
        table_ref = qualify_store_table(cast(Any, self)._config, cast(Any, self)._memory_table)
        where, params = _scope_sql_and_params(app_name, user_id, scope_filter)
        params["top_k"] = top_k
        sql = (
            "SELECT base.id, base.session_id, base.app_name, base.user_id, base.scope, base.event_id, "
            "base.author, base.timestamp, base.content_json, base.content_text, base.metadata_json, "
            "base.inserted_at, base.embedding, distance "
            f"FROM VECTOR_SEARCH((SELECT * FROM {table_ref} WHERE {where}), "
            "'embedding', (SELECT @query_embedding AS embedding), top_k => @top_k, distance_type => 'COSINE') "
            "ORDER BY distance ASC"
        )
        return sql, params

    def build_fts_search_sql(
        self,
        *,
        app_name: str,
        user_id: str,
        query: str,
        scope_filter: str = "all",
        top_k: int = 20,
    ) -> tuple[str, dict[str, Any]]:
        """Build a GoogleSQL ``SEARCH`` full-text query over ``content_text``."""
        table_ref = qualify_store_table(cast(Any, self)._config, cast(Any, self)._memory_table)
        where, params = _scope_sql_and_params(app_name, user_id, scope_filter)
        params["query"] = query
        params["top_k"] = top_k
        sql = (
            "SELECT id, session_id, app_name, user_id, scope, event_id, author, timestamp, "
            "content_json, content_text, metadata_json, inserted_at, embedding "
            f"FROM {table_ref} WHERE {where} AND SEARCH(content_text, @query) "
            "ORDER BY timestamp DESC LIMIT @top_k"
        )
        return sql, params

    def _candidate_select_sql(
        self,
        *,
        app_name: str,
        user_id: str,
        scope_filter: str = "all",
    ) -> tuple[str, dict[str, Any]]:
        table_ref = qualify_store_table(cast(Any, self)._config, cast(Any, self)._memory_table)
        where, params = _scope_sql_and_params(app_name, user_id, scope_filter)
        sql = (
            "SELECT id, session_id, app_name, user_id, scope, event_id, author, timestamp, "
            "content_json, content_text, metadata_json, inserted_at, embedding "
            f"FROM {table_ref} WHERE {where} ORDER BY timestamp DESC"
        )
        return sql, params


class BigQuerySyncADKMemoryStore(_BigQueryADKMemoryMixin, _BaseSyncADKMemoryStore[Any]):
    """BigQuery synchronous ADK memory store with ``VECTOR_SEARCH`` and ``SEARCH`` support."""

    __slots__ = ()

    adapter_name = "vertebra_bigquery"

    def _memory_table_ddl(self) -> list[str]:
        return _memory_ddl_statements(
            self._config,
            self._memory_table,
            self._owner_id_column_ddl,
            use_fts=self._use_fts,
        )

    def _drop_memory_table_sql(self) -> list[str]:
        return [f"DROP TABLE IF EXISTS {qualify_store_table(self._config, self._memory_table)}"]

    def create_tables(self) -> None:
        """Create the BigQuery memory table and optional search index if enabled."""
        if not self._enabled:
            return
        with self._config.provide_session() as driver:
            for stmt in self._memory_table_ddl():
                driver.execute(stmt)

    def insert_memory_entries(self, entries: list["StoredMemory"], owner_id: object | None = None) -> int:
        """Insert memory records using ``MERGE`` deduplication on ``id``."""
        if not self._enabled or not entries:
            return 0
        table_ref = qualify_store_table(self._config, self._memory_table)
        owner_col = quote_bigquery_identifier(self._owner_id_column_name) if self._owner_id_column_name else None
        inserted = 0
        with self._config.provide_session() as driver:
            for entry in entries:
                ts = parse_utc_timestamp(entry["timestamp"]).isoformat()
                ins_ts = parse_utc_timestamp(entry["inserted_at"]).isoformat()
                emb_raw = entry.get("embedding")
                emb = list(emb_raw) if emb_raw is not None else None
                params: dict[str, Any] = {
                    "id": entry["id"],
                    "session_id": entry["session_id"],
                    "app_name": entry["app_name"],
                    "user_id": entry["user_id"],
                    "scope": entry.get("scope", "user"),
                    "event_id": entry["event_id"],
                    "author": entry.get("author"),
                    "timestamp": ts,
                    "content_json": _encode_json_text(entry.get("content_json", {})),
                    "content_text": entry.get("content_text", ""),
                    "metadata_json": (
                        _encode_json_text(entry["metadata_json"]) if entry.get("metadata_json") is not None else None
                    ),
                    "inserted_at": ins_ts,
                    "embedding": emb,
                }
                if owner_col:
                    params["owner_id"] = owner_id
                    sql = (
                        f"MERGE INTO {table_ref} AS target "
                        "USING (SELECT @id AS id) AS source ON target.id = source.id "
                        "WHEN NOT MATCHED THEN INSERT "
                        f"(id, session_id, app_name, user_id, {owner_col}, scope, event_id, author, "
                        "timestamp, content_json, content_text, metadata_json, inserted_at, embedding) "
                        "VALUES (@id, @session_id, @app_name, @user_id, @owner_id, @scope, @event_id, @author, "
                        "@timestamp, PARSE_JSON(@content_json), @content_text, "
                        "PARSE_JSON(@metadata_json), @inserted_at, @embedding)"
                    )
                else:
                    sql = (
                        f"MERGE INTO {table_ref} AS target "
                        "USING (SELECT @id AS id) AS source ON target.id = source.id "
                        "WHEN NOT MATCHED THEN INSERT "
                        "(id, session_id, app_name, user_id, scope, event_id, author, "
                        "timestamp, content_json, content_text, metadata_json, inserted_at, embedding) "
                        "VALUES (@id, @session_id, @app_name, @user_id, @scope, @event_id, @author, "
                        "@timestamp, PARSE_JSON(@content_json), @content_text, "
                        "PARSE_JSON(@metadata_json), @inserted_at, @embedding)"
                    )
                res = driver.execute(sql, params)
                affected = getattr(res, "rows_affected", None)
                inserted += int(affected) if isinstance(affected, int) and affected > 0 else 1
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
        """Search ADK memory entries via ``VECTOR_SEARCH`` or ``SEARCH``."""
        if not self._enabled:
            return []
        top_k = limit if limit is not None else self._max_results
        try:
            with self._config.provide_session() as driver:
                if embedding is not None:
                    vec_sql, vec_params = self.build_vector_search_sql(
                        app_name=app_name, user_id=user_id, scope_filter=scope_filter, top_k=top_k
                    )
                    vec_params["query_embedding"] = [float(x) for x in embedding]
                    rows = [dict(row) for row in driver.select(vec_sql, vec_params)]
                    return _rank_memory_rows(rows, query=query, embedding=embedding, top_k=top_k)
                if self._use_fts and query.strip():
                    fts_sql, fts_params = self.build_fts_search_sql(
                        app_name=app_name, user_id=user_id, query=query, scope_filter=scope_filter, top_k=top_k
                    )
                    rows = [dict(row) for row in driver.select(fts_sql, fts_params)]
                    return _rank_memory_rows(rows, query=query, embedding=None, top_k=top_k)
                cand_sql, cand_params = self._candidate_select_sql(
                    app_name=app_name, user_id=user_id, scope_filter=scope_filter
                )
                rows = [dict(row) for row in driver.select(cand_sql, cand_params)]
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return []
            raise
        return _rank_memory_rows(rows, query=query, embedding=embedding, top_k=top_k)

    def delete_entries_by_session(self, session_id: str) -> int:
        """Delete memory entries associated with ``session_id``."""
        table_ref = qualify_store_table(self._config, self._memory_table)
        with self._config.provide_session() as driver:
            res = driver.execute(f"DELETE FROM {table_ref} WHERE session_id = @session_id", {"session_id": session_id})
        return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)

    def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
        """Delete memory entries inserted more than ``days`` ago."""
        table_ref = qualify_store_table(self._config, self._memory_table)
        cutoff = (datetime.now(UTC) - timedelta(days=days)).isoformat()
        clauses = ["inserted_at < @cutoff"]
        params: dict[str, Any] = {"cutoff": cutoff}
        if app_name is not None:
            clauses.append("app_name = @app_name")
            params["app_name"] = app_name
        if scope is not None:
            clauses.append("scope = @scope")
            params["scope"] = scope
        with self._config.provide_session() as driver:
            res = driver.execute(f"DELETE FROM {table_ref} WHERE {' AND '.join(clauses)}", params)
        return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)


class BigQueryAsyncADKMemoryStore(_BigQueryADKMemoryMixin, _BaseAsyncADKMemoryStore[Any]):
    """BigQuery asynchronous ADK memory store with ``VECTOR_SEARCH`` and ``SEARCH`` support."""

    __slots__ = ()

    adapter_name = "vertebra_bigquery"

    async def _memory_table_ddl(self) -> list[str]:
        return _memory_ddl_statements(
            self._config,
            self._memory_table,
            self._owner_id_column_ddl,
            use_fts=self._use_fts,
        )

    def _drop_memory_table_sql(self) -> list[str]:
        return [f"DROP TABLE IF EXISTS {qualify_store_table(self._config, self._memory_table)}"]

    async def create_tables(self) -> None:
        """Create the BigQuery memory table and optional search index asynchronously."""
        if not self._enabled:
            return
        ddls = await self._memory_table_ddl()
        async with self._config.provide_session() as driver:
            for stmt in ddls:
                await driver.execute(stmt)

    async def insert_memory_entries(self, entries: list["StoredMemory"], owner_id: object | None = None) -> int:
        """Insert memory records asynchronously using ``MERGE`` deduplication on ``id``."""
        if not self._enabled or not entries:
            return 0
        table_ref = qualify_store_table(self._config, self._memory_table)
        owner_col = quote_bigquery_identifier(self._owner_id_column_name) if self._owner_id_column_name else None
        inserted = 0
        async with self._config.provide_session() as driver:
            for entry in entries:
                ts = parse_utc_timestamp(entry["timestamp"]).isoformat()
                ins_ts = parse_utc_timestamp(entry["inserted_at"]).isoformat()
                emb_raw = entry.get("embedding")
                emb = list(emb_raw) if emb_raw is not None else None
                params: dict[str, Any] = {
                    "id": entry["id"],
                    "session_id": entry["session_id"],
                    "app_name": entry["app_name"],
                    "user_id": entry["user_id"],
                    "scope": entry.get("scope", "user"),
                    "event_id": entry["event_id"],
                    "author": entry.get("author"),
                    "timestamp": ts,
                    "content_json": _encode_json_text(entry.get("content_json", {})),
                    "content_text": entry.get("content_text", ""),
                    "metadata_json": (
                        _encode_json_text(entry["metadata_json"]) if entry.get("metadata_json") is not None else None
                    ),
                    "inserted_at": ins_ts,
                    "embedding": emb,
                }
                if owner_col:
                    params["owner_id"] = owner_id
                    sql = (
                        f"MERGE INTO {table_ref} AS target "
                        "USING (SELECT @id AS id) AS source ON target.id = source.id "
                        "WHEN NOT MATCHED THEN INSERT "
                        f"(id, session_id, app_name, user_id, {owner_col}, scope, event_id, author, "
                        "timestamp, content_json, content_text, metadata_json, inserted_at, embedding) "
                        "VALUES (@id, @session_id, @app_name, @user_id, @owner_id, @scope, @event_id, @author, "
                        "@timestamp, PARSE_JSON(@content_json), @content_text, "
                        "PARSE_JSON(@metadata_json), @inserted_at, @embedding)"
                    )
                else:
                    sql = (
                        f"MERGE INTO {table_ref} AS target "
                        "USING (SELECT @id AS id) AS source ON target.id = source.id "
                        "WHEN NOT MATCHED THEN INSERT "
                        "(id, session_id, app_name, user_id, scope, event_id, author, "
                        "timestamp, content_json, content_text, metadata_json, inserted_at, embedding) "
                        "VALUES (@id, @session_id, @app_name, @user_id, @scope, @event_id, @author, "
                        "@timestamp, PARSE_JSON(@content_json), @content_text, "
                        "PARSE_JSON(@metadata_json), @inserted_at, @embedding)"
                    )
                res = await driver.execute(sql, params)
                affected = getattr(res, "rows_affected", None)
                inserted += int(affected) if isinstance(affected, int) and affected > 0 else 1
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
        """Search ADK memory entries asynchronously via ``VECTOR_SEARCH`` or ``SEARCH``."""
        if not self._enabled:
            return []
        top_k = limit if limit is not None else self._max_results
        try:
            async with self._config.provide_session() as driver:
                if embedding is not None:
                    vec_sql, vec_params = self.build_vector_search_sql(
                        app_name=app_name, user_id=user_id, scope_filter=scope_filter, top_k=top_k
                    )
                    vec_params["query_embedding"] = [float(x) for x in embedding]
                    raw_rows = await driver.select(vec_sql, vec_params)
                    rows = [dict(row) for row in raw_rows]
                    return _rank_memory_rows(rows, query=query, embedding=embedding, top_k=top_k)
                if self._use_fts and query.strip():
                    fts_sql, fts_params = self.build_fts_search_sql(
                        app_name=app_name, user_id=user_id, query=query, scope_filter=scope_filter, top_k=top_k
                    )
                    raw_rows = await driver.select(fts_sql, fts_params)
                    rows = [dict(row) for row in raw_rows]
                    return _rank_memory_rows(rows, query=query, embedding=None, top_k=top_k)
                cand_sql, cand_params = self._candidate_select_sql(
                    app_name=app_name, user_id=user_id, scope_filter=scope_filter
                )
                raw_rows = await driver.select(cand_sql, cand_params)
                rows = [dict(row) for row in raw_rows]
        except Exception as exc:
            if is_bigquery_table_missing(exc):
                return []
            raise
        return _rank_memory_rows(rows, query=query, embedding=embedding, top_k=top_k)

    async def delete_entries_by_session(self, session_id: str) -> int:
        """Delete memory entries associated with ``session_id`` asynchronously."""
        table_ref = qualify_store_table(self._config, self._memory_table)
        async with self._config.provide_session() as driver:
            res = await driver.execute(
                f"DELETE FROM {table_ref} WHERE session_id = @session_id", {"session_id": session_id}
            )
        return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)

    async def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
        """Delete aged memory entries asynchronously."""
        table_ref = qualify_store_table(self._config, self._memory_table)
        cutoff = (datetime.now(UTC) - timedelta(days=days)).isoformat()
        clauses = ["inserted_at < @cutoff"]
        params: dict[str, Any] = {"cutoff": cutoff}
        if app_name is not None:
            clauses.append("app_name = @app_name")
            params["app_name"] = app_name
        if scope is not None:
            clauses.append("scope = @scope")
            params["scope"] = scope
        async with self._config.provide_session() as driver:
            res = await driver.execute(f"DELETE FROM {table_ref} WHERE {' AND '.join(clauses)}", params)
        return int(getattr(res, "rows_affected", 0) or getattr(res, "num_rows", 0) or 0)


def _artifact_ddl(config: Any, artifact_table: str) -> str:
    with_partitioning, expiration_days, require_filter, _ = _partition_settings(config)
    part_sql, opt_sql = bigquery_partition_clauses(
        "created_at",
        enabled=with_partitioning,
        expiration_days=expiration_days,
        require_filter=require_filter,
        cluster_by=("app_name", "user_id", "filename"),
    )
    table_ref = qualify_store_table(config, artifact_table)
    return (
        f"CREATE TABLE IF NOT EXISTS {table_ref} ("
        "app_name STRING NOT NULL, "
        "user_id STRING NOT NULL, "
        "session_id STRING, "
        "filename STRING NOT NULL, "
        "version INT64 NOT NULL, "
        "mime_type STRING, "
        "canonical_uri STRING NOT NULL, "
        "custom_metadata JSON, "
        "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()"
        f"){part_sql}{opt_sql}"
    )


def _artifact_scope_where(
    app_name: str,
    user_id: str,
    filename: str,
    session_id: str | None = None,
) -> tuple[str, dict[str, Any]]:
    params: dict[str, Any] = {"app_name": app_name, "user_id": user_id, "filename": filename}
    if session_id is None:
        return "app_name = @app_name AND user_id = @user_id AND filename = @filename AND session_id IS NULL", params
    params["session_id"] = session_id
    return "app_name = @app_name AND user_id = @user_id AND filename = @filename AND session_id = @session_id", params


class _BigQueryADKArtifactMixin:
    """Shared ObjectRef->ArtifactReference and ObjectStoreBackend wiring for BigQuery ADK artifacts."""

    def _init_storage(
        self,
        config: Any,
        storage_backend: ObjectStoreBackend | str | None = None,
    ) -> ObjectStoreBackend | None:
        adk = _extract_adk_extension(config)
        uri = adk.get("artifact_storage_uri")
        return resolve_storage_backend(
            storage_backend,
            getattr(config, "driver_features", None),
            artifact_storage_uri=str(uri) if isinstance(uri, str) else None,
        )

    @staticmethod
    def to_artifact_reference(ref: ObjectRef) -> ArtifactReference:
        """Translate a native BigQuery ``ObjectRef`` to a storage-neutral ``ArtifactReference``."""
        return object_ref_to_artifact_reference(ref)

    @staticmethod
    def _stored_artifact_to_ref(record: "StoredArtifact", size: int | None = None) -> ArtifactReference:
        return ArtifactReference(
            uri=record["canonical_uri"],
            version=int(record["version"]),
            content_type=record.get("mime_type"),
            size=size,
            updated=record.get("created_at"),
            signed_url=None,
        )

    @staticmethod
    def _object_key(app_name: str, user_id: str, filename: str, session_id: str | None, version: int) -> str:
        scope = session_id if session_id is not None else "user"
        return f"adk-artifacts/{app_name}/{user_id}/{scope}/{filename}/v{version}"


class BigQuerySyncADKArtifactStore(_BigQueryADKArtifactMixin, _BaseSyncADKArtifactStore[Any]):
    """BigQuery synchronous ADK artifact metadata store bridged to ``ArtifactReference``."""

    __slots__ = ("_storage_backend",)

    adapter_name = "vertebra_bigquery"

    def __init__(
        self,
        config: Any,
        *,
        storage_backend: ObjectStoreBackend | str | None = None,
    ) -> None:
        super().__init__(config)
        self._storage_backend = self._init_storage(config, storage_backend)

    @property
    def storage_backend(self) -> ObjectStoreBackend | None:
        """Return the configured object storage backend, if any."""
        return self._storage_backend

    def create_table(self) -> None:
        """Create the BigQuery artifact metadata table if missing."""
        ddl = _artifact_ddl(self._config, self._artifact_table)
        with self._config.provide_session() as driver:
            driver.execute(ddl)

    def insert_artifact(self, record: "StoredArtifact") -> None:
        """Persist a ``StoredArtifact`` version metadata row."""
        table_ref = qualify_store_table(self._config, self._artifact_table)
        sql = (
            f"INSERT INTO {table_ref} "
            "(app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at) "
            "VALUES (@app_name, @user_id, @session_id, @filename, @version, @mime_type, @canonical_uri, "
            "PARSE_JSON(@custom_metadata), @created_at)"
        )
        params = {
            "app_name": record["app_name"],
            "user_id": record["user_id"],
            "session_id": record.get("session_id"),
            "filename": record["filename"],
            "version": int(record["version"]),
            "mime_type": record.get("mime_type"),
            "canonical_uri": record["canonical_uri"],
            "custom_metadata": (
                _encode_json_text(record["custom_metadata"]) if record.get("custom_metadata") is not None else None
            ),
            "created_at": parse_utc_timestamp(record.get("created_at")).isoformat(),
        }
        with self._config.provide_session() as driver:
            driver.execute(sql, params)

    def get_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> "StoredArtifact | None":
        """Fetch metadata for the requested or latest artifact version."""
        table_ref = qualify_store_table(self._config, self._artifact_table)
        where, params = _artifact_scope_where(app_name, user_id, filename, session_id)
        if version is not None:
            where += " AND version = @version"
            params["version"] = int(version)
        sql = (
            "SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
            f"custom_metadata, created_at FROM {table_ref} WHERE {where} ORDER BY version DESC LIMIT 1"
        )
        with self._config.provide_session() as driver:
            row = driver.select_one_or_none(sql, params)
        return _row_to_stored_artifact(dict(row)) if row is not None else None

    def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """List distinct artifact filenames within user or session+user scope."""
        table_ref = qualify_store_table(self._config, self._artifact_table)
        params: dict[str, Any] = {"app_name": app_name, "user_id": user_id}
        if session_id is None:
            where = "app_name = @app_name AND user_id = @user_id AND session_id IS NULL"
        else:
            where = "app_name = @app_name AND user_id = @user_id AND (session_id = @session_id OR session_id IS NULL)"
            params["session_id"] = session_id
        sql = f"SELECT DISTINCT filename FROM {table_ref} WHERE {where} ORDER BY filename ASC"
        with self._config.provide_session() as driver:
            rows = driver.select(sql, params)
        return [str(row["filename"]) for row in rows if row.get("filename") is not None]

    def list_artifact_versions(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list["StoredArtifact"]:
        """List all artifact version rows ordered ascending."""
        table_ref = qualify_store_table(self._config, self._artifact_table)
        where, params = _artifact_scope_where(app_name, user_id, filename, session_id)
        sql = (
            "SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
            f"custom_metadata, created_at FROM {table_ref} WHERE {where} ORDER BY version ASC"
        )
        with self._config.provide_session() as driver:
            rows = driver.select(sql, params)
        return [_row_to_stored_artifact(dict(row)) for row in rows]

    def delete_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list["StoredArtifact"]:
        """Delete all metadata rows for an artifact and return the removed records."""
        existing = self.list_artifact_versions(app_name, user_id, filename, session_id)
        table_ref = qualify_store_table(self._config, self._artifact_table)
        where, params = _artifact_scope_where(app_name, user_id, filename, session_id)
        with self._config.provide_session() as driver:
            driver.execute(f"DELETE FROM {table_ref} WHERE {where}", params)
        return existing

    def get_next_version(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> int:
        """Compute the next zero-based integer artifact version."""
        table_ref = qualify_store_table(self._config, self._artifact_table)
        where, params = _artifact_scope_where(app_name, user_id, filename, session_id)
        sql = f"SELECT COALESCE(MAX(version), -1) AS max_ver FROM {table_ref} WHERE {where}"
        with self._config.provide_session() as driver:
            row = driver.select_one_or_none(sql, params)
        max_ver = int(row.get("max_ver", -1)) if row is not None and row.get("max_ver") is not None else -1
        return max_ver + 1

    def delete_artifacts_older_than(
        self,
        before: datetime,
        app_name: str | None = None,
    ) -> list["StoredArtifact"]:
        """Delete artifact metadata rows created before ``before`` and return them."""
        table_ref = qualify_store_table(self._config, self._artifact_table)
        cutoff = parse_utc_timestamp(before).isoformat()
        where = "created_at < @before" + (" AND app_name = @app_name" if app_name is not None else "")
        params: dict[str, Any] = {"before": cutoff}
        if app_name is not None:
            params["app_name"] = app_name
        select_sql = (
            "SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
            f"custom_metadata, created_at FROM {table_ref} WHERE {where} ORDER BY created_at ASC, version ASC"
        )
        with self._config.provide_session() as driver:
            rows = [dict(row) for row in driver.select(select_sql, params)]
            driver.execute(f"DELETE FROM {table_ref} WHERE {where}", params)
        return [_row_to_stored_artifact(row) for row in rows]

    def save_artifact_object(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        data: bytes,
        *,
        session_id: str | None = None,
        mime_type: str = "application/octet-stream",
        custom_metadata: dict[str, Any] | None = None,
    ) -> ArtifactReference:
        """Write binary payload to ``storage_backend``, record SQL metadata, and return ``ArtifactReference``."""
        version = self.get_next_version(app_name, user_id, filename, session_id)
        key = self._object_key(app_name, user_id, filename, session_id, version)
        if self._storage_backend is not None:
            self._storage_backend.write_bytes_sync(key, data)
            base_uri = str(getattr(self._storage_backend, "store_uri", "") or "").rstrip("/")
            canonical_uri = f"{base_uri}/{key}" if base_uri else key
        else:
            canonical_uri = key
        now = datetime.now(UTC)
        record = cast(
            "StoredArtifact",
            {
                "app_name": app_name,
                "user_id": user_id,
                "session_id": session_id,
                "filename": filename,
                "version": version,
                "mime_type": mime_type,
                "canonical_uri": canonical_uri,
                "custom_metadata": custom_metadata,
                "created_at": now,
            },
        )
        self.insert_artifact(record)
        return self._stored_artifact_to_ref(record, size=len(data))

    def get_artifact_reference(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> ArtifactReference | None:
        """Return a storage-neutral ``ArtifactReference`` for a persisted artifact."""
        record = self.get_artifact(app_name, user_id, filename, session_id=session_id, version=version)
        return self._stored_artifact_to_ref(record) if record is not None else None

    def list_object_table_references(
        self,
        config: ObjectTableConfig,
        *,
        store: Any | None = None,
        where: str | None = None,
        limit: int | None = None,
        presign: bool = True,
        expires_in_secs: int = 3600,
    ) -> list[ArtifactReference]:
        """List BigQuery Object Table entries translated from native ``ObjectRef`` to ``ArtifactReference``."""
        with self._config.provide_session() as driver:
            refs = driver.list_object_table_refs(
                config,
                store=store,
                where=where,
                limit=limit,
                presign=presign,
                expires_in_secs=expires_in_secs,
            )
        return [self.to_artifact_reference(ref) for ref in refs]


class BigQueryAsyncADKArtifactStore(_BigQueryADKArtifactMixin, _BaseAsyncADKArtifactStore[Any]):
    """BigQuery asynchronous ADK artifact metadata store bridged to ``ArtifactReference``."""

    __slots__ = ("_storage_backend",)

    adapter_name = "vertebra_bigquery"

    def __init__(
        self,
        config: Any,
        *,
        storage_backend: ObjectStoreBackend | str | None = None,
    ) -> None:
        super().__init__(config)
        self._storage_backend = self._init_storage(config, storage_backend)

    @property
    def storage_backend(self) -> ObjectStoreBackend | None:
        """Return the configured object storage backend, if any."""
        return self._storage_backend

    async def create_table(self) -> None:
        """Create the BigQuery artifact metadata table asynchronously."""
        ddl = _artifact_ddl(self._config, self._artifact_table)
        async with self._config.provide_session() as driver:
            await driver.execute(ddl)

    async def insert_artifact(self, record: "StoredArtifact") -> None:
        """Persist a ``StoredArtifact`` version metadata row asynchronously."""
        table_ref = qualify_store_table(self._config, self._artifact_table)
        sql = (
            f"INSERT INTO {table_ref} "
            "(app_name, user_id, session_id, filename, version, mime_type, canonical_uri, custom_metadata, created_at) "
            "VALUES (@app_name, @user_id, @session_id, @filename, @version, @mime_type, @canonical_uri, "
            "PARSE_JSON(@custom_metadata), @created_at)"
        )
        params = {
            "app_name": record["app_name"],
            "user_id": record["user_id"],
            "session_id": record.get("session_id"),
            "filename": record["filename"],
            "version": int(record["version"]),
            "mime_type": record.get("mime_type"),
            "canonical_uri": record["canonical_uri"],
            "custom_metadata": (
                _encode_json_text(record["custom_metadata"]) if record.get("custom_metadata") is not None else None
            ),
            "created_at": parse_utc_timestamp(record.get("created_at")).isoformat(),
        }
        async with self._config.provide_session() as driver:
            await driver.execute(sql, params)

    async def get_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> "StoredArtifact | None":
        """Fetch metadata asynchronously for the requested or latest artifact version."""
        table_ref = qualify_store_table(self._config, self._artifact_table)
        where, params = _artifact_scope_where(app_name, user_id, filename, session_id)
        if version is not None:
            where += " AND version = @version"
            params["version"] = int(version)
        sql = (
            "SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
            f"custom_metadata, created_at FROM {table_ref} WHERE {where} ORDER BY version DESC LIMIT 1"
        )
        async with self._config.provide_session() as driver:
            row = await driver.select_one_or_none(sql, params)
        return _row_to_stored_artifact(dict(row)) if row is not None else None

    async def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """List distinct artifact filenames asynchronously."""
        table_ref = qualify_store_table(self._config, self._artifact_table)
        params: dict[str, Any] = {"app_name": app_name, "user_id": user_id}
        if session_id is None:
            where = "app_name = @app_name AND user_id = @user_id AND session_id IS NULL"
        else:
            where = "app_name = @app_name AND user_id = @user_id AND (session_id = @session_id OR session_id IS NULL)"
            params["session_id"] = session_id
        sql = f"SELECT DISTINCT filename FROM {table_ref} WHERE {where} ORDER BY filename ASC"
        async with self._config.provide_session() as driver:
            rows = await driver.select(sql, params)
        return [str(row["filename"]) for row in rows if row.get("filename") is not None]

    async def list_artifact_versions(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list["StoredArtifact"]:
        """List all artifact version rows asynchronously."""
        table_ref = qualify_store_table(self._config, self._artifact_table)
        where, params = _artifact_scope_where(app_name, user_id, filename, session_id)
        sql = (
            "SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
            f"custom_metadata, created_at FROM {table_ref} WHERE {where} ORDER BY version ASC"
        )
        async with self._config.provide_session() as driver:
            rows = await driver.select(sql, params)
        return [_row_to_stored_artifact(dict(row)) for row in rows]

    async def delete_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list["StoredArtifact"]:
        """Delete all metadata rows for an artifact asynchronously and return them."""
        existing = await self.list_artifact_versions(app_name, user_id, filename, session_id)
        table_ref = qualify_store_table(self._config, self._artifact_table)
        where, params = _artifact_scope_where(app_name, user_id, filename, session_id)
        async with self._config.provide_session() as driver:
            await driver.execute(f"DELETE FROM {table_ref} WHERE {where}", params)
        return existing

    async def get_next_version(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> int:
        """Compute the next zero-based integer artifact version asynchronously."""
        table_ref = qualify_store_table(self._config, self._artifact_table)
        where, params = _artifact_scope_where(app_name, user_id, filename, session_id)
        sql = f"SELECT COALESCE(MAX(version), -1) AS max_ver FROM {table_ref} WHERE {where}"
        async with self._config.provide_session() as driver:
            row = await driver.select_one_or_none(sql, params)
        max_ver = int(row.get("max_ver", -1)) if row is not None and row.get("max_ver") is not None else -1
        return max_ver + 1

    async def delete_artifacts_older_than(
        self,
        before: datetime,
        app_name: str | None = None,
    ) -> list["StoredArtifact"]:
        """Delete aged artifact metadata rows asynchronously and return them."""
        table_ref = qualify_store_table(self._config, self._artifact_table)
        cutoff = parse_utc_timestamp(before).isoformat()
        where = "created_at < @before" + (" AND app_name = @app_name" if app_name is not None else "")
        params: dict[str, Any] = {"before": cutoff}
        if app_name is not None:
            params["app_name"] = app_name
        select_sql = (
            "SELECT app_name, user_id, session_id, filename, version, mime_type, canonical_uri, "
            f"custom_metadata, created_at FROM {table_ref} WHERE {where} ORDER BY created_at ASC, version ASC"
        )
        async with self._config.provide_session() as driver:
            raw_rows = await driver.select(select_sql, params)
            rows = [dict(row) for row in raw_rows]
            await driver.execute(f"DELETE FROM {table_ref} WHERE {where}", params)
        return [_row_to_stored_artifact(row) for row in rows]

    async def save_artifact_object(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        data: bytes,
        *,
        session_id: str | None = None,
        mime_type: str = "application/octet-stream",
        custom_metadata: dict[str, Any] | None = None,
    ) -> ArtifactReference:
        """Write binary payload to ``storage_backend``, record metadata, and return ``ArtifactReference``."""
        version = await self.get_next_version(app_name, user_id, filename, session_id)
        key = self._object_key(app_name, user_id, filename, session_id, version)
        if self._storage_backend is not None:
            await self._storage_backend.write_bytes_async(key, data)
            base_uri = str(getattr(self._storage_backend, "store_uri", "") or "").rstrip("/")
            canonical_uri = f"{base_uri}/{key}" if base_uri else key
        else:
            canonical_uri = key
        now = datetime.now(UTC)
        record = cast(
            "StoredArtifact",
            {
                "app_name": app_name,
                "user_id": user_id,
                "session_id": session_id,
                "filename": filename,
                "version": version,
                "mime_type": mime_type,
                "canonical_uri": canonical_uri,
                "custom_metadata": custom_metadata,
                "created_at": now,
            },
        )
        await self.insert_artifact(record)
        return self._stored_artifact_to_ref(record, size=len(data))

    async def get_artifact_reference(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> ArtifactReference | None:
        """Return a storage-neutral ``ArtifactReference`` asynchronously."""
        record = await self.get_artifact(app_name, user_id, filename, session_id=session_id, version=version)
        return self._stored_artifact_to_ref(record) if record is not None else None

    async def list_object_table_references(
        self,
        config: ObjectTableConfig,
        *,
        store: Any | None = None,
        where: str | None = None,
        limit: int | None = None,
        presign: bool = True,
        expires_in_secs: int = 3600,
    ) -> list[ArtifactReference]:
        """List BigQuery Object Table entries asynchronously translated to ``ArtifactReference``."""
        async with self._config.provide_session() as driver:
            refs = await driver.list_object_table_refs(
                config,
                store=store,
                where=where,
                limit=limit,
                presign=presign,
                expires_in_secs=expires_in_secs,
            )
        return [self.to_artifact_reference(ref) for ref in refs]
