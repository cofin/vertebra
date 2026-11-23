"""Oracle event queue store and AQ / TxEventQ backends over native notification sources."""

from __future__ import annotations

import asyncio
import collections
import contextlib
import threading
import time
import uuid
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, ClassVar, Final, cast

from sqlspec.exceptions import EventChannelError, MigrationError
from sqlspec.extensions.events import (
    AsyncEventBackendProtocol,
    BaseEventQueueStore,
    EventMessage,
    SyncEventBackendProtocol,
    decode_notify_payload,
    encode_notify_payload,
    normalize_event_channel_name,
)
from sqlspec.migrations.schema import DropStatement, SchemaTarget, metadata_has_index

from vertebra.db.adapters.oracle.notify import AsyncOracleNotificationSource, SyncOracleNotificationSource
from vertebra.extensions.sqlspec._driver import _build_companion_index
from vertebra.extensions.sqlspec.adapters.oracle.core import (
    escape_oracle_literal,
    oracle_ensure_utc,
    oracle_identifier,
    oracle_index_name,
    oracle_json_column_ddl,
    oracle_json_dumps,
    oracle_json_loads,
    oracle_resolve_json_storage_mode,
    oracle_row_value,
    oracle_table_feature_clause,
    oracle_table_ref,
    validate_oracle_table_name,
)
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.serializers import decode_json, encode_json

if TYPE_CHECKING:
    from sqlspec.extensions.litestar.store import CompanionIndex

    from vertebra.extensions.sqlspec.adapters.oracle.config import OracleAsyncConfig, OracleSyncConfig

__all__ = (
    "OracleAsyncEventsBackend",
    "OracleEventQueueStore",
    "OracleSyncEventsBackend",
)


_ORACLE_NATIVE_BACKENDS: Final[frozenset[str]] = frozenset({"aq", "txeventq"})
_ORACLE_JSON_QUEUE_MIN_MAJOR: Final[int] = 21
_DEFAULT_AQ_QUEUE: Final[str] = "SQLSPEC_EVENTS_QUEUE"
_PROBE_USER_QUEUE_SQL: Final[str] = (
    "SELECT name, TRIM(enqueue_enabled), TRIM(dequeue_enabled) FROM user_queues WHERE name = :name"
)
_PROBE_USER_QUEUE_TABLE_SQL: Final[str] = "SELECT queue_table FROM user_queue_tables WHERE queue_table = :name"
_CREATE_QUEUE_TABLE_PLSQL: Final[str] = (
    "BEGIN DBMS_AQADM.CREATE_QUEUE_TABLE(queue_table => :queue_table, queue_payload_type => 'JSON'); END;"
)
_CREATE_QUEUE_PLSQL: Final[str] = (
    "BEGIN DBMS_AQADM.CREATE_QUEUE(queue_name => :queue_name, queue_table => :queue_table); END;"
)
_CREATE_TXEVENTQ_PLSQL: Final[str] = (
    "BEGIN DBMS_AQADM.CREATE_TRANSACTIONAL_EVENT_QUEUE(queue_name => :queue_name, queue_payload_type => 'JSON'); END;"
)
_START_QUEUE_PLSQL: Final[str] = "BEGIN DBMS_AQADM.START_QUEUE(queue_name => :queue_name); END;"


def _plsql_ignore_exists(stmt: str, *, allow_existing_column_index: bool = False) -> str:
    """Wrap an Oracle DDL statement to suppress `ORA-00955` (and optionally `ORA-01408`)."""
    escaped = escape_oracle_literal(stmt)
    cond = "SQLCODE NOT IN (-955, -1408)" if allow_existing_column_index else "SQLCODE != -955"
    return f"BEGIN\n    EXECUTE IMMEDIATE '{escaped}';\nEXCEPTION WHEN OTHERS THEN IF {cond} THEN RAISE; END IF;\nEND;"


def _plsql_ignore_missing(stmt: str, *, code: int = -942) -> str:
    """Wrap an Oracle DROP statement to suppress `ORA-00942` or `ORA-01418`."""
    escaped = escape_oracle_literal(stmt)
    return (
        "BEGIN\n"
        f"    EXECUTE IMMEDIATE '{escaped}';\n"
        "EXCEPTION WHEN OTHERS THEN IF SQLCODE != "
        f"{code} THEN RAISE; END IF;\n"
        "END;"
    )


def _validate_bare_oracle_queue_name(queue_name: str) -> str:
    """Validate that an expanded Oracle queue name is a bare unquoted SQL identifier."""
    if "." in queue_name:
        msg = (
            f"Invalid Oracle aq_queue '{queue_name}': configure a bare unquoted queue name "
            "without schema qualification."
        )
        raise MigrationError(msg)
    try:
        return oracle_identifier(queue_name, "aq_queue")
    except ImproperConfigurationError as exc:
        msg = (
            f"Invalid Oracle aq_queue '{queue_name}': configure a bare unquoted queue name "
            "without schema qualification."
        )
        raise MigrationError(msg) from exc


def _expand_channel_queue_name(template: str, channel: str) -> str:
    """Expand an Oracle AQ queue-name template containing `{channel}`."""
    if "{" in template:
        return template.format(channel=channel.upper())
    return template


def _oracle_native_backend_targets(config: Any, settings: Mapping[str, Any], backend_name: str) -> tuple[str, ...]:
    """Validate and expand Oracle AQ or TxEventQ queue names for reconcile provisioning."""
    if backend_name not in _ORACLE_NATIVE_BACKENDS:
        return ()

    cache = getattr(config, "_oracle_version_cache", None)
    if (
        cache is not None
        and cache.resolved
        and cache.version is not None
        and cache.version.major < _ORACLE_JSON_QUEUE_MIN_MAJOR
    ):
        msg = (
            f"Oracle events backend '{backend_name}' requires Oracle 21c or newer for JSON payload queues "
            f"(detected major version {cache.version.major}); upgrade to Oracle 21c+ or use "
            'migration_mode="versioned".'
        )
        raise MigrationError(msg)

    raw_template = settings.get("aq_queue", _DEFAULT_AQ_QUEUE)
    if raw_template is None:
        template = _DEFAULT_AQ_QUEUE
    elif isinstance(raw_template, str):
        template = raw_template
    else:
        msg = f"Invalid Oracle aq_queue {raw_template!r}: configure a bare unquoted queue name."
        raise MigrationError(msg)

    raw_channels = settings.get("aq_channels")
    if raw_channels is None:
        channels: list[Any] = []
    elif isinstance(raw_channels, str) or not isinstance(raw_channels, Sequence):
        msg = "Oracle events setting 'aq_channels' must be a sequence of channel names."
        raise MigrationError(msg)
    else:
        channels = list(raw_channels)

    has_channel_placeholder = "{channel}" in template
    if has_channel_placeholder and not channels:
        msg = (
            "Oracle events aq_queue template contains '{channel}', which requires non-empty 'aq_channels' "
            'during reconcile; configure aq_channels or use migration_mode="versioned".'
        )
        raise MigrationError(msg)
    if channels and not has_channel_placeholder:
        msg = (
            "Oracle events 'aq_channels' requires '{channel}' in 'aq_queue'; "
            "omit aq_channels for a fixed queue name or include '{channel}' in aq_queue."
        )
        raise MigrationError(msg)

    if has_channel_placeholder:
        resolved_names: list[str] = []
        for raw_channel in channels:
            if not isinstance(raw_channel, str):
                msg = f"Invalid events channel name in 'aq_channels': {raw_channel!r}"
                raise MigrationError(msg)
            try:
                normalized_channel = normalize_event_channel_name(raw_channel)
            except EventChannelError as exc:
                raise MigrationError(str(exc)) from exc
            expanded = _expand_channel_queue_name(template, normalized_channel).upper()
            _validate_bare_oracle_queue_name(expanded)
            resolved_names.append(expanded)
        return tuple(dict.fromkeys(resolved_names))

    expanded = _expand_channel_queue_name(template, "").upper()
    _validate_bare_oracle_queue_name(expanded)
    return (expanded,)


def _extract_queue_flags(row: Any) -> tuple[str, str]:
    """Extract upper-cased enqueue and dequeue enabled flags from a `user_queues` row."""
    if isinstance(row, Mapping):
        lower_map = {str(key).lower(): val for key, val in row.items()}
        enq = lower_map.get("enqueue_enabled", lower_map.get("trim(enqueue_enabled)"))
        deq = lower_map.get("dequeue_enabled", lower_map.get("trim(dequeue_enabled)"))
        if enq is not None and deq is not None:
            return str(enq).strip().upper(), str(deq).strip().upper()
        values = tuple(row.values())
        return str(values[1]).strip().upper(), str(values[2]).strip().upper()
    return str(row[1]).strip().upper(), str(row[2]).strip().upper()


def _is_dbms_aqadm_privilege_error(exc: Exception) -> bool:
    """Return whether a driver error indicates missing `DBMS_AQADM` privileges."""
    text = str(exc).upper()
    if "ORA-01031" in text:
        return True
    return "ORA-06550" in text and "PLS-00201" in text and "DBMS_AQADM" in text


def _wrap_aqadm_privilege_error(queue_name: str) -> MigrationError:
    """Build a `MigrationError` naming required Oracle queue-administration grants."""
    msg = (
        f"Oracle events queue provisioning for '{queue_name}' failed due to missing privileges; "
        "grant AQ_ADMINISTRATOR_ROLE and GRANT EXECUTE ON DBMS_AQADM to the database user."
    )
    return MigrationError(msg)


def _exec_aqadm_sync(driver: Any, queue_name: str, statement: str, parameters: dict[str, Any]) -> None:
    """Execute one `DBMS_AQADM` PL/SQL block synchronously, translating privilege refusals."""
    try:
        driver.execute(statement, parameters)
    except Exception as exc:
        if _is_dbms_aqadm_privilege_error(exc):
            raise _wrap_aqadm_privilege_error(queue_name) from exc
        raise


async def _exec_aqadm_async(driver: Any, queue_name: str, statement: str, parameters: dict[str, Any]) -> None:
    """Execute one `DBMS_AQADM` PL/SQL block asynchronously, translating privilege refusals."""
    try:
        await driver.execute(statement, parameters)
    except Exception as exc:
        if _is_dbms_aqadm_privilege_error(exc):
            raise _wrap_aqadm_privilege_error(queue_name) from exc
        raise


def _reconcile_oracle_native_backend_sync(
    driver: Any, backend_name: str, targets: tuple[str, ...], *, create_schema: bool
) -> None:
    """Provision missing or stopped Oracle AQ or TxEventQ queues on a synchronous driver."""
    if not targets or backend_name not in _ORACLE_NATIVE_BACKENDS:
        return

    for raw_name in targets:
        queue_name = raw_name.upper()
        rows = driver.select(_PROBE_USER_QUEUE_SQL, {"name": queue_name})
        if rows:
            enq_enabled, deq_enabled = _extract_queue_flags(rows[0])
            if enq_enabled == "YES" and deq_enabled == "YES":
                continue
            if not create_schema:
                msg = (
                    f"Oracle events queue '{queue_name}' is stopped while create_schema=False; "
                    "start the queue manually or set create_schema=True."
                )
                raise MigrationError(msg)
            _exec_aqadm_sync(driver, queue_name, _START_QUEUE_PLSQL, {"queue_name": queue_name})
            continue

        if not create_schema:
            msg = (
                f"Oracle events queue '{queue_name}' does not exist while create_schema=False; "
                "provision the queue manually or set create_schema=True."
            )
            raise MigrationError(msg)

        if backend_name == "aq":
            queue_table = f"{queue_name[:125]}_QT"
            qt_rows = driver.select(_PROBE_USER_QUEUE_TABLE_SQL, {"name": queue_table})
            if not qt_rows:
                _exec_aqadm_sync(driver, queue_name, _CREATE_QUEUE_TABLE_PLSQL, {"queue_table": queue_table})
            _exec_aqadm_sync(
                driver, queue_name, _CREATE_QUEUE_PLSQL, {"queue_name": queue_name, "queue_table": queue_table}
            )
            _exec_aqadm_sync(driver, queue_name, _START_QUEUE_PLSQL, {"queue_name": queue_name})
        else:
            _exec_aqadm_sync(driver, queue_name, _CREATE_TXEVENTQ_PLSQL, {"queue_name": queue_name})
            _exec_aqadm_sync(driver, queue_name, _START_QUEUE_PLSQL, {"queue_name": queue_name})


async def _reconcile_oracle_native_backend_async(
    driver: Any, backend_name: str, targets: tuple[str, ...], *, create_schema: bool
) -> None:
    """Provision missing or stopped Oracle AQ or TxEventQ queues on an asynchronous driver."""
    if not targets or backend_name not in _ORACLE_NATIVE_BACKENDS:
        return

    for raw_name in targets:
        queue_name = raw_name.upper()
        rows = await driver.select(_PROBE_USER_QUEUE_SQL, {"name": queue_name})
        if rows:
            enq_enabled, deq_enabled = _extract_queue_flags(rows[0])
            if enq_enabled == "YES" and deq_enabled == "YES":
                continue
            if not create_schema:
                msg = (
                    f"Oracle events queue '{queue_name}' is stopped while create_schema=False; "
                    "start the queue manually or set create_schema=True."
                )
                raise MigrationError(msg)
            await _exec_aqadm_async(driver, queue_name, _START_QUEUE_PLSQL, {"queue_name": queue_name})
            continue

        if not create_schema:
            msg = (
                f"Oracle events queue '{queue_name}' does not exist while create_schema=False; "
                "provision the queue manually or set create_schema=True."
            )
            raise MigrationError(msg)

        if backend_name == "aq":
            queue_table = f"{queue_name[:125]}_QT"
            qt_rows = await driver.select(_PROBE_USER_QUEUE_TABLE_SQL, {"name": queue_table})
            if not qt_rows:
                await _exec_aqadm_async(driver, queue_name, _CREATE_QUEUE_TABLE_PLSQL, {"queue_table": queue_table})
            await _exec_aqadm_async(
                driver, queue_name, _CREATE_QUEUE_PLSQL, {"queue_name": queue_name, "queue_table": queue_table}
            )
            await _exec_aqadm_async(driver, queue_name, _START_QUEUE_PLSQL, {"queue_name": queue_name})
        else:
            await _exec_aqadm_async(driver, queue_name, _CREATE_TXEVENTQ_PLSQL, {"queue_name": queue_name})
            await _exec_aqadm_async(driver, queue_name, _START_QUEUE_PLSQL, {"queue_name": queue_name})


class OracleEventQueueStore(BaseEventQueueStore["OracleSyncConfig | OracleAsyncConfig"]):
    """Oracle durable event queue store supporting 23ai native `JSON` and 19c `BLOB CHECK (... IS JSON)`."""

    __slots__ = ()

    extension_config_options = BaseEventQueueStore.extension_config_options | frozenset(
        {
            "aq_channels",
            "aq_queue",
            "aq_visibility",
            "aq_wait_seconds",
            "compression",
            "in_memory",
            "json_storage",
            "partitioning",
            "table_options",
        }
    )

    def _split_table_name(self) -> tuple[str | None, str]:
        parts = validate_oracle_table_name(self.table_name)
        if len(parts) == 2:
            return parts[0], parts[1]
        return None, parts[0]

    def _storage_mode(self) -> str:
        override = self.settings.get("json_storage")
        cache = getattr(self._config, "_oracle_version_cache", None)
        if cache is not None and cache.resolved and override is None:
            return oracle_resolve_json_storage_mode(cache.version, compatible_major=cache.compatible_major)
        return oracle_resolve_json_storage_mode(None, override=override)

    def _column_types(self) -> tuple[str, str, str]:
        mode = self._storage_mode()
        if mode == "json":
            return "JSON", "JSON", "TIMESTAMP WITH TIME ZONE"
        return "BLOB", "BLOB", "TIMESTAMP WITH TIME ZONE"

    def _string_type(self, length: int) -> str:
        return f"VARCHAR2({length})"

    def _index_name(self) -> str:
        return oracle_index_name("idx", self.table_name, "ch_st")

    def _index_ddl(self) -> str:
        table_ref = oracle_table_ref(self.table_name)
        idx_name = self._index_name()
        return f"CREATE INDEX {idx_name} ON {table_ref} (channel, status, available_at)"

    def _index_existence_target(self) -> tuple[str | None, str]:
        return self._split_table_name()

    def _wrap_create_statement(self, statement: str, object_type: str) -> str:
        if object_type == "index":
            return _plsql_ignore_exists(statement, allow_existing_column_index=True)
        return _plsql_ignore_exists(statement)

    def create_statements(self) -> list[str]:
        """Build Oracle PL/SQL blocks creating event queue table and pending index idempotently."""
        mode = self._storage_mode()
        table_ref = oracle_table_ref(self.table_name)
        payload_col = oracle_json_column_ddl("payload_json", mode, not_null=True)
        metadata_col = oracle_json_column_ddl("metadata_json", mode, not_null=False)
        features = oracle_table_feature_clause(self.settings)
        create_tbl = _plsql_ignore_exists(
            f"CREATE TABLE {table_ref} ("
            "event_id VARCHAR2(64) PRIMARY KEY, "
            "channel VARCHAR2(128) NOT NULL, "
            f"{payload_col}, "
            f"{metadata_col}, "
            "status VARCHAR2(32) DEFAULT 'pending' NOT NULL, "
            "available_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL, "
            "lease_expires_at TIMESTAMP WITH TIME ZONE, "
            "attempts NUMBER(10) DEFAULT 0 NOT NULL, "
            "created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL"
            f"){features}"
        )
        create_idx = self.companion_index_statement()
        return [create_tbl, create_idx]

    def drop_statements(self) -> list[str]:
        """Build Oracle PL/SQL DROP blocks ignoring missing index/table errors."""
        table_ref = oracle_table_ref(self.table_name)
        idx_name = self._index_name()
        return [
            _plsql_ignore_missing(f"DROP INDEX {idx_name}", code=-1418),
            _plsql_ignore_missing(f"DROP TABLE {table_ref}", code=-942),
        ]

    def prepare_schema_sync(self, driver: Any) -> None:
        """Populate the pool-scoped Oracle version cache on an active synchronous driver."""
        if (
            self.settings.get("json_storage") is not None
            and self.settings.get("backend") not in _ORACLE_NATIVE_BACKENDS
        ):
            return
        cache = getattr(self._config, "_oracle_version_cache", None)
        if cache is None or cache.resolved:
            return
        data_dict: Any = getattr(driver, "data_dictionary", None)
        get_version: Any = getattr(data_dict, "get_version", None)
        if callable(get_version):
            cache.set(get_version(driver))

    async def prepare_schema_async(self, driver: Any) -> None:
        """Populate the pool-scoped Oracle version cache on an active asynchronous driver."""
        if (
            self.settings.get("json_storage") is not None
            and self.settings.get("backend") not in _ORACLE_NATIVE_BACKENDS
        ):
            return
        cache = getattr(self._config, "_oracle_version_cache", None)
        if cache is None or cache.resolved:
            return
        data_dict: Any = getattr(driver, "data_dictionary", None)
        get_version = getattr(data_dict, "get_version", None)
        if callable(get_version):
            cache.set(await cast("Any", get_version)(driver))

    def _schema_target(self) -> SchemaTarget:
        schema_name, bare_table = self._split_table_name()
        return SchemaTarget.from_ddl(
            bare_table,
            self.create_statements()[0],
            schema=schema_name,
            dialect="oracle",
        )

    def get_schema_targets(self) -> list[SchemaTarget]:
        """Return ordered `SchemaTarget` definitions for the Oracle event queue table."""
        return [self._schema_target()]

    def companion_index_statement(self) -> str:
        """Return PL/SQL-guarded companion index DDL for the Oracle event queue table."""
        return self._wrap_create_statement(self._index_ddl(), "index")

    def get_companion_indexes(self) -> list[CompanionIndex]:
        """Return companion index descriptors for the Oracle event queue table."""
        schema_name, bare_table = self._split_table_name()
        return [
            _build_companion_index(
                name=self._index_name(),
                table=bare_table,
                schema=schema_name,
                ddl=self.companion_index_statement(),
            )
        ]

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Create missing companion index on a synchronous driver without committing."""
        for index in self.get_companion_indexes():
            rows = driver.data_dictionary.get_indexes(driver, table=index.table, schema=index.schema)
            if not metadata_has_index(rows, index.table, index.name, schema_name=index.schema):
                driver.execute_script(index.ddl)

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Create missing companion index on an asynchronous driver without committing."""
        for index in self.get_companion_indexes():
            rows = await driver.data_dictionary.get_indexes(driver, table=index.table, schema=index.schema)
            if not metadata_has_index(rows, index.table, index.name, schema_name=index.schema):
                await driver.execute_script(index.ddl)

    def downgrade_drops(self) -> list[DropStatement]:
        """Return downgrade drop statements for the Oracle event queue index and table."""
        return [DropStatement(statement) for statement in self.drop_statements()]

    def native_backend_targets(self, backend_name: str) -> tuple[str, ...]:
        """Return validated Oracle AQ or TxEventQ queue names for reconcile provisioning."""
        return _oracle_native_backend_targets(self._config, self.settings, backend_name)

    def reconcile_native_backend_sync(
        self, driver: Any, backend_name: str, targets: tuple[str, ...], *, create_schema: bool
    ) -> None:
        """Provision missing or stopped Oracle AQ or TxEventQ queues on a synchronous driver."""
        _reconcile_oracle_native_backend_sync(driver, backend_name, targets, create_schema=create_schema)

    async def reconcile_native_backend_async(
        self, driver: Any, backend_name: str, targets: tuple[str, ...], *, create_schema: bool
    ) -> None:
        """Provision missing or stopped Oracle AQ or TxEventQ queues on an asynchronous driver."""
        await _reconcile_oracle_native_backend_async(driver, backend_name, targets, create_schema=create_schema)


def _encode_aq_frame(event_id: str, payload: dict[str, Any], metadata: dict[str, Any] | None) -> str:
    """Serialize an Oracle AQ notification envelope payload."""
    try:
        return encode_notify_payload(event_id, payload, metadata)
    except Exception:
        raw = encode_json({"event_id": event_id, "payload": payload, "metadata": metadata})
        return raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)


def _decode_aq_event(
    event_channel: str, raw_payload: Any, *, queue_store: OracleEventQueueStore | None = None
) -> EventMessage:
    """Decode an AQ `NotificationEvent` payload into an `EventMessage`."""
    _ = queue_store
    now = datetime.now(UTC)
    if isinstance(raw_payload, dict):
        ev_id = str(raw_payload.get("event_id") or uuid.uuid4().hex)
        body = raw_payload.get("payload")
        meta = raw_payload.get("metadata")
        return EventMessage(
            event_id=ev_id,
            channel=event_channel,
            payload=body if isinstance(body, dict) else dict(raw_payload),
            metadata=meta if isinstance(meta, dict) else None,
            attempts=1,
            available_at=now,
            lease_expires_at=None,
            created_at=now,
        )
    text = (
        bytes(raw_payload).decode("utf-8")
        if isinstance(raw_payload, (bytes, bytearray, memoryview))
        else str(raw_payload)
    )
    try:
        decoded = decode_notify_payload(event_channel, text)
        ev_id = decoded.event_id
        body = decoded.payload
        meta = decoded.metadata
    except Exception:
        parsed = decode_json(text)
        if isinstance(parsed, dict):
            ev_id = str(parsed.get("event_id") or uuid.uuid4().hex)
            body = parsed.get("payload") if isinstance(parsed.get("payload"), dict) else parsed
            meta = parsed.get("metadata") if isinstance(parsed.get("metadata"), dict) else None
        else:
            ev_id = uuid.uuid4().hex
            body = {"value": parsed}
            meta = None
    return EventMessage(
        event_id=ev_id,
        channel=event_channel,
        payload=body if isinstance(body, dict) else {"value": body},
        metadata=meta if isinstance(meta, dict) else None,
        attempts=1,
        available_at=now,
        lease_expires_at=None,
        created_at=now,
    )


class OracleSyncEventsBackend(SyncEventBackendProtocol):
    """Synchronous Oracle AQ / TxEventQ event backend over `SyncOracleNotificationSource`."""

    __slots__ = (
        "_buffers",
        "_closed",
        "_cond",
        "_config",
        "_owns_source",
        "_pump_done",
        "_pump_error",
        "_pump_thread",
        "_queue",
        "_raw_queues",
        "_source",
        "_subscribed",
        "_unacked",
    )

    supports_sync: ClassVar[bool] = True
    backend_name: ClassVar[str] = "oracle_aq"

    def __init__(
        self,
        config: OracleSyncConfig,
        *,
        source: SyncOracleNotificationSource | None = None,
        queue: OracleEventQueueStore | None = None,
    ) -> None:
        """Initialize the synchronous Oracle AQ event backend."""
        self._config = config
        self._source = source
        self._owns_source = False
        self._queue = queue or OracleEventQueueStore(config)
        self._subscribed: set[str] = set()
        self._cond = threading.Condition()
        self._raw_queues: dict[str, collections.deque[Any]] = collections.defaultdict(collections.deque)
        self._pump_thread: threading.Thread | None = None
        self._pump_done = False
        self._pump_error: BaseException | None = None
        self._buffers: dict[str, collections.deque[EventMessage]] = collections.defaultdict(collections.deque)
        self._unacked: dict[str, tuple[str, EventMessage]] = {}
        self._closed = False

    def _ensure_source(self) -> SyncOracleNotificationSource:
        if self._source is not None:
            return self._source
        provide_conn = getattr(self._config, "provide_connection_instance", None)
        native_driver: Any = (
            provide_conn() if callable(provide_conn) else self._config._driver_cls(self._config.driver_config)
        )
        self._source = SyncOracleNotificationSource(native_driver)
        self._owns_source = True
        return self._source

    def publish(self, channel: str, payload: dict[str, Any], metadata: dict[str, Any] | None = None) -> str:
        """Enqueue an event message onto Oracle AQ or durable event queue storage."""
        if self._closed:
            msg = "OracleSyncEventsBackend has been shut down"
            raise RuntimeError(msg)
        norm_channel = normalize_event_channel_name(channel)
        event_id = uuid.uuid4().hex
        encoded = _encode_aq_frame(event_id, payload, metadata)
        if self._source is not None:
            self._source.publish(norm_channel, encoded)
        else:
            table_ref = oracle_table_ref(self._queue.table_name)
            with self._config.provide_session() as driver:
                driver.execute(
                    f"INSERT INTO {table_ref} (event_id, channel, payload_json, metadata_json, status, available_at) "
                    "VALUES (:event_id, :channel, :payload_json, :metadata_json, 'pending', SYSTIMESTAMP)",
                    {
                        "event_id": event_id,
                        "channel": norm_channel,
                        "payload_json": oracle_json_dumps(payload),
                        "metadata_json": oracle_json_dumps(metadata) if metadata is not None else None,
                    },
                )
                driver.commit()
        return event_id

    def publish_many(self, events: Sequence[tuple[str, dict[str, Any], dict[str, Any] | None]]) -> list[str]:
        """Enqueue multiple Oracle events preserving input order."""
        return [self.publish(channel, payload, metadata) for channel, payload, metadata in events]

    def _pump_sync_stream(self, source: SyncOracleNotificationSource) -> None:
        try:
            for raw_ev in source.listen():
                if self._closed:
                    break
                ev_ch = normalize_event_channel_name(raw_ev.channel) if getattr(raw_ev, "channel", None) else ""
                with self._cond:
                    self._raw_queues[ev_ch].append(raw_ev)
                    self._cond.notify_all()
        except BaseException as exc:
            with self._cond:
                self._pump_error = exc
                self._cond.notify_all()
        finally:
            with self._cond:
                self._pump_done = True
                self._cond.notify_all()

    def _claim_table_row_sync(self, table_ref: str, norm_channel: str) -> tuple[Any, str] | None:
        with self._config.provide_session() as driver:
            row = driver.select_one_or_none(
                f"SELECT event_id, channel, payload_json, metadata_json, attempts, available_at, created_at "
                f"FROM {table_ref} WHERE channel = :channel AND status = 'pending' AND available_at <= SYSTIMESTAMP "
                "ORDER BY available_at ASC FOR UPDATE SKIP LOCKED",
                {"channel": norm_channel},
            )
            if row is None:
                driver.rollback()
                return None
            ev_id = str(oracle_row_value(row, "event_id", 0))
            res = driver.execute(
                f"UPDATE {table_ref} SET status = 'leased', attempts = attempts + 1 "
                "WHERE event_id = :event_id AND status = 'pending'",
                {"event_id": ev_id},
            )
            rows_affected = getattr(res, "rows_affected", None)
            affected = int(rows_affected) if rows_affected is not None else (int(res) if isinstance(res, int) else 1)
            driver.commit()
            if affected <= 0:
                return None
            return row, ev_id

    def dequeue(self, channel: str, poll_interval: float) -> EventMessage | None:
        """Dequeue the next Oracle AQ event or pending queue row for `channel`."""
        if self._closed:
            return None
        norm_channel = normalize_event_channel_name(channel)
        buffered = self._buffers.get(norm_channel)
        if buffered:
            msg = buffered.popleft()
            self._unacked[msg.event_id] = (norm_channel, msg)
            return msg

        if self._source is not None:
            if norm_channel not in self._subscribed:
                self._source.subscribe([norm_channel])
                self._subscribed.add(norm_channel)
            raw_ev: Any = None
            with self._cond:
                raw_q = self._raw_queues[norm_channel]
                if raw_q:
                    raw_ev = raw_q.popleft()
                else:
                    if self._pump_thread is None or not self._pump_thread.is_alive():
                        self._pump_done = False
                        self._pump_error = None
                        self._pump_thread = threading.Thread(
                            target=self._pump_sync_stream,
                            args=(self._source,),
                            daemon=True,
                        )
                        self._pump_thread.start()
                    if poll_interval <= 0:
                        if not raw_q and not self._pump_done:
                            self._cond.wait(timeout=0.01)
                    else:
                        deadline = time.monotonic() + float(poll_interval)
                        while not raw_q and not self._pump_done and not self._closed:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                break
                            self._cond.wait(timeout=remaining)
                    if self._pump_error is not None:
                        err = self._pump_error
                        self._pump_error = None
                        raise err
                    if raw_q:
                        raw_ev = raw_q.popleft()
            if raw_ev is None:
                return None
            msg = _decode_aq_event(raw_ev.channel or norm_channel, raw_ev.payload, queue_store=self._queue)
            self._unacked[msg.event_id] = (norm_channel, msg)
            return msg

        table_ref = oracle_table_ref(self._queue.table_name)
        deadline = time.monotonic() + max(0.0, float(poll_interval))
        claimed: tuple[Any, str] | None = None
        while not self._closed:
            claimed = self._claim_table_row_sync(table_ref, norm_channel)
            if claimed is not None or poll_interval <= 0:
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(0.05, remaining))
        if claimed is None:
            return None
        row, ev_id = claimed
        payload_dec = oracle_json_loads(oracle_row_value(row, "payload_json", 2))
        meta_dec = oracle_json_loads(oracle_row_value(row, "metadata_json", 3))
        msg = EventMessage(
            event_id=ev_id,
            channel=str(oracle_row_value(row, "channel", 1) or norm_channel),
            payload=payload_dec if isinstance(payload_dec, dict) else {"value": payload_dec},
            metadata=meta_dec if isinstance(meta_dec, dict) else None,
            attempts=int(oracle_row_value(row, "attempts", 4) or 0) + 1,
            available_at=oracle_ensure_utc(oracle_row_value(row, "available_at", 5)),
            lease_expires_at=None,
            created_at=oracle_ensure_utc(oracle_row_value(row, "created_at", 6)),
        )
        self._unacked[msg.event_id] = (norm_channel, msg)
        return msg

    def ack(self, event_id: str) -> None:
        """Acknowledge an Oracle event message."""
        self._unacked.pop(event_id, None)
        table_ref = oracle_table_ref(self._queue.table_name)
        with contextlib.suppress(Exception), self._config.provide_session() as driver:
            driver.execute(
                f"UPDATE {table_ref} SET status = 'acked' WHERE event_id = :event_id", {"event_id": event_id}
            )
            driver.commit()

    def nack(self, event_id: str) -> None:
        """Return an unacknowledged Oracle event message for redelivery."""
        entry = self._unacked.pop(event_id, None)
        if entry is None:
            return
        channel, msg = entry
        msg.attempts += 1
        self._buffers[channel].append(msg)

    def shutdown(self) -> None:
        """Close owned Oracle notification sources and clear buffered messages."""
        with self._cond:
            self._closed = True
            self._cond.notify_all()
        if self._source is not None and self._owns_source:
            with contextlib.suppress(Exception):
                self._source.close()
        if self._pump_thread is not None and self._pump_thread.is_alive():
            self._pump_thread.join(timeout=0.2)
        with self._cond:
            self._pump_thread = None
            self._raw_queues.clear()
            self._unacked.clear()
            self._buffers.clear()


class OracleAsyncEventsBackend(AsyncEventBackendProtocol):
    """Asynchronous Oracle AQ / TxEventQ event backend over `AsyncOracleNotificationSource`."""

    __slots__ = (
        "_buffers",
        "_channel_queues",
        "_closed",
        "_config",
        "_owns_source",
        "_pump_error",
        "_pump_task",
        "_queue",
        "_source",
        "_subscribed",
        "_unacked",
    )

    supports_async: ClassVar[bool] = True
    backend_name: ClassVar[str] = "oracle_aq"

    def __init__(
        self,
        config: OracleAsyncConfig,
        *,
        source: AsyncOracleNotificationSource | None = None,
        queue: OracleEventQueueStore | None = None,
    ) -> None:
        """Initialize the asynchronous Oracle AQ event backend."""
        self._config = config
        self._source = source
        self._owns_source = False
        self._queue = queue or OracleEventQueueStore(config)
        self._subscribed: set[str] = set()
        self._channel_queues: dict[str, asyncio.Queue[Any]] = {}
        self._pump_task: asyncio.Task[None] | None = None
        self._pump_error: BaseException | None = None
        self._buffers: dict[str, collections.deque[EventMessage]] = collections.defaultdict(collections.deque)
        self._unacked: dict[str, tuple[str, EventMessage]] = {}
        self._closed = False

    async def publish(self, channel: str, payload: dict[str, Any], metadata: dict[str, Any] | None = None) -> str:
        """Asynchronously publish an event onto Oracle AQ or durable queue storage."""
        if self._closed:
            msg = "OracleAsyncEventsBackend has been shut down"
            raise RuntimeError(msg)
        norm_channel = normalize_event_channel_name(channel)
        event_id = uuid.uuid4().hex
        encoded = _encode_aq_frame(event_id, payload, metadata)
        if self._source is not None:
            await self._source.publish(norm_channel, encoded)
        else:
            table_ref = oracle_table_ref(self._queue.table_name)
            async with self._config.provide_session() as driver:
                await driver.execute(
                    f"INSERT INTO {table_ref} (event_id, channel, payload_json, metadata_json, status, available_at) "
                    "VALUES (:event_id, :channel, :payload_json, :metadata_json, 'pending', SYSTIMESTAMP)",
                    {
                        "event_id": event_id,
                        "channel": norm_channel,
                        "payload_json": oracle_json_dumps(payload),
                        "metadata_json": oracle_json_dumps(metadata) if metadata is not None else None,
                    },
                )
                await driver.commit()
        return event_id

    async def publish_many(self, events: Sequence[tuple[str, dict[str, Any], dict[str, Any] | None]]) -> list[str]:
        """Asynchronously publish multiple Oracle events preserving input order."""
        ids: list[str] = []
        for channel, payload, metadata in events:
            ids.append(await self.publish(channel, payload, metadata))
        return ids

    async def _pump_stream(self, source: AsyncOracleNotificationSource) -> None:
        try:
            async for raw_ev in source.listen():
                if self._closed:
                    break
                ev_ch = normalize_event_channel_name(raw_ev.channel) if getattr(raw_ev, "channel", None) else ""
                q = self._channel_queues.setdefault(ev_ch, asyncio.Queue())
                q.put_nowait(raw_ev)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            self._pump_error = exc

    async def _claim_table_row_async(self, table_ref: str, norm_channel: str) -> tuple[Any, str] | None:
        async with self._config.provide_session() as driver:
            row = await driver.select_one_or_none(
                f"SELECT event_id, channel, payload_json, metadata_json, attempts, available_at, created_at "
                f"FROM {table_ref} WHERE channel = :channel AND status = 'pending' AND available_at <= SYSTIMESTAMP "
                "ORDER BY available_at ASC FOR UPDATE SKIP LOCKED",
                {"channel": norm_channel},
            )
            if row is None:
                await driver.rollback()
                return None
            ev_id = str(oracle_row_value(row, "event_id", 0))
            res = await driver.execute(
                f"UPDATE {table_ref} SET status = 'leased', attempts = attempts + 1 "
                "WHERE event_id = :event_id AND status = 'pending'",
                {"event_id": ev_id},
            )
            rows_affected = getattr(res, "rows_affected", None)
            affected = int(rows_affected) if rows_affected is not None else (int(res) if isinstance(res, int) else 1)
            await driver.commit()
            if affected <= 0:
                return None
            return row, ev_id

    async def dequeue(self, channel: str, poll_interval: float) -> EventMessage | None:
        """Asynchronously dequeue the next Oracle AQ event or pending queue row."""
        if self._closed:
            return None
        norm_channel = normalize_event_channel_name(channel)
        buffered = self._buffers.get(norm_channel)
        if buffered:
            msg = buffered.popleft()
            self._unacked[msg.event_id] = (norm_channel, msg)
            return msg

        if self._source is not None:
            if norm_channel not in self._subscribed:
                await self._source.subscribe([norm_channel])
                self._subscribed.add(norm_channel)
            q = self._channel_queues.setdefault(norm_channel, asyncio.Queue())
            raw_ev: Any = None
            if not q.empty():
                raw_ev = q.get_nowait()
            else:
                if self._pump_task is None or self._pump_task.done():
                    if self._pump_error is not None:
                        err = self._pump_error
                        self._pump_error = None
                        raise err
                    self._pump_task = asyncio.create_task(self._pump_stream(self._source))
                if poll_interval <= 0:
                    await asyncio.sleep(0)
                    if self._pump_error is not None:
                        err = self._pump_error
                        self._pump_error = None
                        raise err
                    if not q.empty():
                        raw_ev = q.get_nowait()
                else:
                    get_task = asyncio.create_task(q.get())
                    done, _ = await asyncio.wait(
                        {get_task, self._pump_task},
                        timeout=poll_interval,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if get_task in done:
                        raw_ev = get_task.result()
                    else:
                        get_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await get_task
                        if self._pump_error is not None:
                            err = self._pump_error
                            self._pump_error = None
                            raise err
                        if not q.empty():
                            raw_ev = q.get_nowait()
            if raw_ev is None:
                return None
            msg = _decode_aq_event(raw_ev.channel or norm_channel, raw_ev.payload, queue_store=self._queue)
            self._unacked[msg.event_id] = (norm_channel, msg)
            return msg

        table_ref = oracle_table_ref(self._queue.table_name)
        deadline = time.monotonic() + max(0.0, float(poll_interval))
        claimed: tuple[Any, str] | None = None
        while not self._closed:
            claimed = await self._claim_table_row_async(table_ref, norm_channel)
            if claimed is not None or poll_interval <= 0:
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            await asyncio.sleep(min(0.05, remaining))
        if claimed is None:
            return None
        row, ev_id = claimed
        payload_dec = oracle_json_loads(oracle_row_value(row, "payload_json", 2))
        meta_dec = oracle_json_loads(oracle_row_value(row, "metadata_json", 3))
        msg = EventMessage(
            event_id=ev_id,
            channel=str(oracle_row_value(row, "channel", 1) or norm_channel),
            payload=payload_dec if isinstance(payload_dec, dict) else {"value": payload_dec},
            metadata=meta_dec if isinstance(meta_dec, dict) else None,
            attempts=int(oracle_row_value(row, "attempts", 4) or 0) + 1,
            available_at=oracle_ensure_utc(oracle_row_value(row, "available_at", 5)),
            lease_expires_at=None,
            created_at=oracle_ensure_utc(oracle_row_value(row, "created_at", 6)),
        )
        self._unacked[msg.event_id] = (norm_channel, msg)
        return msg

    async def ack(self, event_id: str) -> None:
        """Asynchronously acknowledge an Oracle event message."""
        self._unacked.pop(event_id, None)
        table_ref = oracle_table_ref(self._queue.table_name)
        with contextlib.suppress(Exception):
            async with self._config.provide_session() as driver:
                await driver.execute(
                    f"UPDATE {table_ref} SET status = 'acked' WHERE event_id = :event_id", {"event_id": event_id}
                )
                await driver.commit()

    async def nack(self, event_id: str) -> None:
        """Asynchronously return an unacknowledged Oracle event message."""
        entry = self._unacked.pop(event_id, None)
        if entry is None:
            return
        channel, msg = entry
        msg.attempts += 1
        self._buffers[channel].append(msg)

    async def shutdown(self) -> None:
        """Asynchronously close owned Oracle notification sources and clear buffered work."""
        self._closed = True
        if self._pump_task is not None:
            self._pump_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._pump_task
            self._pump_task = None
        self._channel_queues.clear()
        self._unacked.clear()
        self._buffers.clear()
        if self._source is not None and self._owns_source:
            with contextlib.suppress(Exception):
                await self._source.aclose()
