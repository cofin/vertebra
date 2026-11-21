"""Type stubs for the vertebra._oracle native extension module."""

from collections.abc import Awaitable, Callable, Mapping, Sequence
from types import TracebackType
from typing import Any, final

from vertebra._vertebra import ArrowCursor

__version__: str
FREE_THREADED: bool
GIL_USED: bool
ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED: bool

@final
class OracleColumn:
    @property
    def name(self) -> str: ...
    @property
    def type_code(self) -> int: ...
    @property
    def type_name(self) -> str: ...
    @property
    def oracle_type(self) -> str: ...
    @property
    def db_size(self) -> int | None: ...
    @property
    def precision(self) -> int | None: ...
    @property
    def scale(self) -> int | None: ...
    @property
    def nullable(self) -> bool: ...
    def __new__(
        cls,
        name: str,
        type_name: str,
        type_code: int = 0,
        db_size: int | None = None,
        precision: int | None = None,
        scale: int | None = None,
        nullable: bool = True,
    ) -> OracleColumn: ...

@final
class OracleQueryResult:
    @property
    def columns(self) -> tuple[OracleColumn, ...]: ...
    @property
    def rows(self) -> list[tuple[Any, ...]]: ...
    @property
    def rowcount(self) -> int: ...
    @property
    def lastrowid(self) -> str | None: ...
    @property
    def out_binds(self) -> tuple[Any, ...] | dict[str, Any] | None: ...
    def __new__(
        cls,
        columns: Sequence[OracleColumn],
        rows: Sequence[tuple[Any, ...]],
        rowcount: int = 0,
        out_binds: tuple[Any, ...] | dict[str, Any] | None = None,
        lastrowid: str | None = None,
    ) -> OracleQueryResult: ...
    def to_dicts(self) -> list[dict[str, Any]]: ...
    def __len__(self) -> int: ...
    def is_empty(self) -> bool: ...

@final
class OraclePipelineOutcome:
    @property
    def status(self) -> str: ...
    @property
    def result(self) -> OracleQueryResult | None: ...
    @property
    def error(self) -> Any | None: ...
    def __new__(
        cls,
        status: str,
        result: OracleQueryResult | None = None,
        error: Any | None = None,
    ) -> OraclePipelineOutcome: ...

@final
class OracleNotification:
    @property
    def queue_name(self) -> str: ...
    @property
    def consumer_name(self) -> str | None: ...
    @property
    def msg_id(self) -> bytes: ...
    @property
    def payload(self) -> bytes: ...
    @property
    def enqueued_at_epoch_ms(self) -> int | None: ...
    def __new__(
        cls,
        queue_name: str,
        msg_id: bytes,
        payload: bytes,
        consumer_name: str | None = None,
        enqueued_at_epoch_ms: int | None = None,
    ) -> OracleNotification: ...

@final
class AsyncOracleListener:
    @property
    def queue_name(self) -> str: ...
    @property
    def consumer_name(self) -> str | None: ...
    @property
    def is_closed(self) -> bool: ...
    @property
    def dropped_count(self) -> int: ...
    def next_notification(self, timeout: float | None = None) -> Awaitable[OracleNotification | None]: ...
    def close(self) -> Awaitable[None]: ...

@final
class SyncOracleListener:
    @property
    def queue_name(self) -> str: ...
    @property
    def consumer_name(self) -> str | None: ...
    @property
    def is_closed(self) -> bool: ...
    @property
    def dropped_count(self) -> int: ...
    def next_notification(self, timeout: float | None = None) -> OracleNotification | None: ...
    def close(self) -> None: ...
    def __enter__(self) -> SyncOracleListener: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> bool: ...

@final
class SyncOracleClient:
    @property
    def closed(self) -> bool: ...
    @property
    def in_transaction(self) -> bool: ...
    @property
    def cclass(self) -> str | None: ...
    @property
    def purity(self) -> str: ...
    @property
    def connection_tag(self) -> str | None: ...
    @property
    def tag_matched(self) -> bool: ...
    @property
    def pending_session_attrs(self) -> dict[str, str | None] | None: ...
    @property
    def active_session_attrs(self) -> dict[str, str | None]: ...
    @property
    def last_piggyback_frame(self) -> bytes | None: ...
    def __new__(
        cls,
        *,
        user: str | None = None,
        password: str | None = None,
        dsn: str | None = None,
        host: str = "localhost",
        port: int = 1521,
        service_name: str = "FREEPDB1",
        sid: str | None = None,
        tns_admin: str | None = None,
        connect_descriptor: str | None = None,
        protocol: str = "tcp",
        ssl_server_dn_match: bool = True,
        ssl_server_cert_dn: str | None = None,
        wallet_location: str | None = None,
        wallet_password: str | None = None,
        pem_cert_data: bytes | None = None,
        cwallet_sso_data: bytes | None = None,
        auth_mode: str = "password",
        access_token: str | None = None,
        token_auth_options: Mapping[str, str] | None = None,
        cclass: str | None = None,
        purity: str = "SELF",
        connection_tag: str | None = None,
        stmtcachesize: int = 64,
        prefetchrows: int = 256,
        arraysize: int = 256,
        call_timeout_ms: int | None = None,
        connect_timeout: float = 10.0,
        enable_lowercase_column_names: bool = True,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
        enable_reset_relay: bool = True,
    ) -> SyncOracleClient: ...
    def set_session_attrs(
        self,
        *,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> None: ...
    def connect(self) -> None: ...
    def close(self) -> None: ...
    def ping(self) -> None: ...
    def cancel(self) -> None: ...
    def begin(self) -> None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def savepoint(self, name: str) -> None: ...
    def rollback_to_savepoint(self, name: str) -> None: ...
    def execute(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        out_binds: Sequence[str] | Mapping[str, str] | None = None,
        fetch_decimals: bool = False,
        enable_lowercase_column_names: bool | None = None,
    ) -> OracleQueryResult: ...
    def execute_many(
        self,
        sql: str,
        params_seq: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> int: ...
    def query_arrow(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 8192,
        force_shm: bool | None = None,
        enable_lowercase_column_names: bool | None = None,
    ) -> ArrowCursor: ...
    def execute_pipeline_batch(
        self,
        operations: Sequence[tuple[Any, ...]],
        *,
        continue_on_error: bool = False,
    ) -> list[OraclePipelineOutcome]: ...
    def run_pipeline(
        self,
        operations: Sequence[tuple[Any, ...]],
        *,
        continue_on_error: bool = False,
    ) -> list[OraclePipelineOutcome]: ...
    def open_listener(
        self,
        *,
        queue_name: str,
        consumer_name: str | None = None,
        wait_timeout_seconds: int = 5,
        max_buffered: int = 1024,
    ) -> SyncOracleListener: ...
    def enqueue_notification(
        self,
        *,
        queue_name: str,
        payload: bytes,
        consumer_name: str | None = None,
        correlation_id: str | None = None,
    ) -> bytes: ...
    def attach_lease(self, release: Callable[[], None]) -> None: ...
    def release_lease(self) -> None: ...
    def cursor(
        self,
        *,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
        lazy_begin: bool = False,
    ) -> SyncOracleCursor: ...
    def fetch_rows(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]: ...
    def fetch_dicts(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> list[dict[str, Any]]: ...
    def fetch_value(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> Any: ...
    def execute_dml(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> tuple[int, str | None, dict[str, Any] | tuple[Any, ...] | None, bool]: ...

@final
class OracleOutBindVar:
    type_code: Any
    size: int
    arraysize: int
    outconverter: Callable[[Any], Any] | None
    values: list[Any]
    def __new__(
        cls,
        type_code: Any,
        size: int = 0,
        arraysize: int = 1,
        outconverter: Callable[[Any], Any] | None = None,
    ) -> OracleOutBindVar: ...
    def getvalue(self, pos: int = 0) -> Any: ...
    def setvalue(self, pos: int, value: Any) -> None: ...

@final
class SyncOracleCursor:
    arraysize: int
    @property
    def description(self) -> tuple[tuple[str, str, None, None, None, None, None], ...] | None: ...
    @property
    def rowcount(self) -> int: ...
    @property
    def lastrowid(self) -> str | None: ...
    @property
    def touches_session_context(self) -> bool: ...
    @property
    def out_binds(self) -> dict[str, Any] | tuple[Any, ...] | None: ...
    def execute(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
    ) -> SyncOracleCursor: ...
    def executemany(
        self,
        sql: str,
        seq_of_params: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> SyncOracleCursor: ...
    def fetchone(self) -> tuple[Any, ...] | None: ...
    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]: ...
    def fetchall(self) -> list[tuple[Any, ...]]: ...
    def var(
        self,
        type_code: Any,
        size: int = 0,
        arraysize: int = 1,
        outconverter: Callable[[Any], Any] | None = None,
    ) -> OracleOutBindVar: ...
    def setinputsizes(self, *sizes: Any) -> None: ...
    def setoutputsize(self, size: int, column: int | None = None) -> None: ...
    def close(self) -> None: ...
    def __enter__(self) -> SyncOracleCursor: ...
    def __exit__(self, exc_type: Any = None, exc_val: Any = None, exc_tb: Any = None) -> bool: ...

@final
class AsyncOracleClient:
    @property
    def closed(self) -> bool: ...
    @property
    def in_transaction(self) -> bool: ...
    @property
    def cclass(self) -> str | None: ...
    @property
    def purity(self) -> str: ...
    @property
    def connection_tag(self) -> str | None: ...
    @property
    def tag_matched(self) -> bool: ...
    @property
    def pending_session_attrs(self) -> dict[str, str | None] | None: ...
    @property
    def active_session_attrs(self) -> dict[str, str | None]: ...
    @property
    def last_piggyback_frame(self) -> bytes | None: ...
    def __new__(
        cls,
        *,
        user: str | None = None,
        password: str | None = None,
        dsn: str | None = None,
        host: str = "localhost",
        port: int = 1521,
        service_name: str = "FREEPDB1",
        sid: str | None = None,
        tns_admin: str | None = None,
        connect_descriptor: str | None = None,
        protocol: str = "tcp",
        ssl_server_dn_match: bool = True,
        ssl_server_cert_dn: str | None = None,
        wallet_location: str | None = None,
        wallet_password: str | None = None,
        pem_cert_data: bytes | None = None,
        cwallet_sso_data: bytes | None = None,
        auth_mode: str = "password",
        access_token: str | None = None,
        token_auth_options: Mapping[str, str] | None = None,
        cclass: str | None = None,
        purity: str = "SELF",
        connection_tag: str | None = None,
        stmtcachesize: int = 64,
        prefetchrows: int = 256,
        arraysize: int = 256,
        call_timeout_ms: int | None = None,
        connect_timeout: float = 10.0,
        enable_lowercase_column_names: bool = True,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
        enable_reset_relay: bool = True,
    ) -> AsyncOracleClient: ...
    def set_session_attrs(
        self,
        *,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> None: ...
    def connect(self) -> Awaitable[None]: ...
    def close(self) -> Awaitable[None]: ...
    def ping(self) -> Awaitable[None]: ...
    def cancel(self) -> None: ...
    def begin(self) -> Awaitable[None]: ...
    def commit(self) -> Awaitable[None]: ...
    def rollback(self) -> Awaitable[None]: ...
    def savepoint(self, name: str) -> Awaitable[None]: ...
    def rollback_to_savepoint(self, name: str) -> Awaitable[None]: ...
    def execute(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        out_binds: Sequence[str] | Mapping[str, str] | None = None,
        fetch_decimals: bool = False,
        enable_lowercase_column_names: bool | None = None,
    ) -> Awaitable[OracleQueryResult]: ...
    def execute_many(
        self,
        sql: str,
        params_seq: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> Awaitable[int]: ...
    def query_arrow(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 8192,
        force_shm: bool | None = None,
        enable_lowercase_column_names: bool | None = None,
    ) -> Awaitable[ArrowCursor]: ...
    def execute_pipeline_batch(
        self,
        operations: Sequence[tuple[Any, ...]],
        *,
        continue_on_error: bool = False,
    ) -> Awaitable[list[OraclePipelineOutcome]]: ...
    def run_pipeline(
        self,
        operations: Sequence[tuple[Any, ...]],
        *,
        continue_on_error: bool = False,
    ) -> Awaitable[list[OraclePipelineOutcome]]: ...
    def open_listener(
        self,
        *,
        queue_name: str,
        consumer_name: str | None = None,
        wait_timeout_seconds: int = 5,
        max_buffered: int = 1024,
    ) -> Awaitable[AsyncOracleListener]: ...
    def enqueue_notification(
        self,
        *,
        queue_name: str,
        payload: bytes,
        consumer_name: str | None = None,
        correlation_id: str | None = None,
    ) -> Awaitable[bytes]: ...
    def attach_lease(self, release: Callable[[], None]) -> None: ...
    def release_lease(self) -> None: ...
    def cursor(
        self,
        *,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
        lazy_begin: bool = False,
    ) -> AsyncOracleCursor: ...
    def fetch_rows(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> Awaitable[tuple[list[tuple[Any, ...]], tuple[str, ...]]]: ...
    def fetch_dicts(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> Awaitable[list[dict[str, Any]]]: ...
    def fetch_value(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> Awaitable[Any]: ...
    def execute_dml(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> Awaitable[tuple[int, str | None, dict[str, Any] | tuple[Any, ...] | None, bool]]: ...

@final
class AsyncOracleCursor:
    arraysize: int
    @property
    def description(self) -> tuple[tuple[str, str, None, None, None, None, None], ...] | None: ...
    @property
    def rowcount(self) -> int: ...
    @property
    def lastrowid(self) -> str | None: ...
    @property
    def touches_session_context(self) -> bool: ...
    @property
    def out_binds(self) -> dict[str, Any] | tuple[Any, ...] | None: ...
    def execute(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
    ) -> Awaitable[AsyncOracleCursor]: ...
    def executemany(
        self,
        sql: str,
        seq_of_params: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> Awaitable[AsyncOracleCursor]: ...
    def fetchone(self) -> Awaitable[tuple[Any, ...] | None]: ...
    def fetchmany(self, size: int | None = None) -> Awaitable[list[tuple[Any, ...]]]: ...
    def fetchall(self) -> Awaitable[list[tuple[Any, ...]]]: ...
    def var(
        self,
        type_code: Any,
        size: int = 0,
        arraysize: int = 1,
        outconverter: Callable[[Any], Any] | None = None,
    ) -> OracleOutBindVar: ...
    def setinputsizes(self, *sizes: Any) -> None: ...
    def setoutputsize(self, size: int, column: int | None = None) -> None: ...
    def close(self) -> Awaitable[None]: ...

@final
class SyncOraclePool:
    @property
    def min_size(self) -> int: ...
    @property
    def max_size(self) -> int: ...
    @property
    def size(self) -> int: ...
    @property
    def idle_size(self) -> int: ...
    @property
    def borrowed_size(self) -> int: ...
    @property
    def closed(self) -> bool: ...
    def __new__(
        cls,
        *,
        user: str | None = None,
        password: str | None = None,
        dsn: str | None = None,
        host: str = "localhost",
        port: int = 1521,
        service_name: str = "FREEPDB1",
        sid: str | None = None,
        tns_admin: str | None = None,
        connect_descriptor: str | None = None,
        protocol: str = "tcp",
        ssl_server_dn_match: bool = True,
        ssl_server_cert_dn: str | None = None,
        wallet_location: str | None = None,
        wallet_password: str | None = None,
        pem_cert_data: bytes | None = None,
        cwallet_sso_data: bytes | None = None,
        auth_mode: str = "password",
        access_token: str | None = None,
        token_auth_options: Mapping[str, str] | None = None,
        cclass: str | None = None,
        purity: str = "SELF",
        connection_tag: str | None = None,
        stmtcachesize: int = 64,
        prefetchrows: int = 256,
        arraysize: int = 256,
        call_timeout_ms: int | None = None,
        connect_timeout: float = 10.0,
        enable_lowercase_column_names: bool = True,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
        min_size: int = 1,
        max_size: int = 10,
        acquire_timeout: float = 30.0,
        reset_timeout: float = 10.0,
        close_timeout: float = 30.0,
        max_lifetime_seconds: float = 3600.0,
        idle_timeout_seconds: float = 600.0,
        health_check_interval_seconds: float = 30.0,
        enable_reset_relay: bool = True,
    ) -> SyncOraclePool: ...
    def acquire(
        self,
        *,
        timeout: float | None = None,
        connection_tag: str | None = None,
        purity: str | None = None,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> SyncOracleClient: ...
    def release(
        self,
        client: SyncOracleClient,
        *,
        connection_tag: str | None = None,
        discard: bool = False,
    ) -> None: ...
    def acquire_from_pool(
        self,
        *,
        timeout: float | None = None,
        connection_tag: str | None = None,
        purity: str | None = None,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> SyncOracleClient: ...
    def release_to_pool(
        self,
        client: SyncOracleClient,
        *,
        connection_tag: str | None = None,
        discard: bool = False,
    ) -> None: ...
    def evict(self, client: SyncOracleClient) -> None: ...
    def stats(self) -> dict[str, int]: ...
    def close(self) -> None: ...
    def fetch_rows(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]: ...
    def fetch_dicts(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> list[dict[str, Any]]: ...
    def fetch_value(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> Any: ...
    def execute_dml(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> tuple[int, str | None, dict[str, Any] | tuple[Any, ...] | None, bool]: ...

@final
class AsyncOraclePool:
    @property
    def min_size(self) -> int: ...
    @property
    def max_size(self) -> int: ...
    @property
    def size(self) -> int: ...
    @property
    def idle_size(self) -> int: ...
    @property
    def borrowed_size(self) -> int: ...
    @property
    def closed(self) -> bool: ...
    def __new__(
        cls,
        *,
        user: str | None = None,
        password: str | None = None,
        dsn: str | None = None,
        host: str = "localhost",
        port: int = 1521,
        service_name: str = "FREEPDB1",
        sid: str | None = None,
        tns_admin: str | None = None,
        connect_descriptor: str | None = None,
        protocol: str = "tcp",
        ssl_server_dn_match: bool = True,
        ssl_server_cert_dn: str | None = None,
        wallet_location: str | None = None,
        wallet_password: str | None = None,
        pem_cert_data: bytes | None = None,
        cwallet_sso_data: bytes | None = None,
        auth_mode: str = "password",
        access_token: str | None = None,
        token_auth_options: Mapping[str, str] | None = None,
        cclass: str | None = None,
        purity: str = "SELF",
        connection_tag: str | None = None,
        stmtcachesize: int = 64,
        prefetchrows: int = 256,
        arraysize: int = 256,
        call_timeout_ms: int | None = None,
        connect_timeout: float = 10.0,
        enable_lowercase_column_names: bool = True,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
        min_size: int = 1,
        max_size: int = 10,
        acquire_timeout: float = 30.0,
        reset_timeout: float = 10.0,
        close_timeout: float = 30.0,
        max_lifetime_seconds: float = 3600.0,
        idle_timeout_seconds: float = 600.0,
        health_check_interval_seconds: float = 30.0,
        enable_reset_relay: bool = True,
    ) -> AsyncOraclePool: ...
    def acquire(
        self,
        *,
        timeout: float | None = None,
        connection_tag: str | None = None,
        purity: str | None = None,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> Awaitable[AsyncOracleClient]: ...
    def release(
        self,
        client: AsyncOracleClient,
        *,
        connection_tag: str | None = None,
        discard: bool = False,
    ) -> Awaitable[None]: ...
    def acquire_from_pool(
        self,
        *,
        timeout: float | None = None,
        connection_tag: str | None = None,
        purity: str | None = None,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> Awaitable[AsyncOracleClient]: ...
    def release_to_pool(
        self,
        client: AsyncOracleClient,
        *,
        connection_tag: str | None = None,
        discard: bool = False,
    ) -> Awaitable[None]: ...
    def evict(self, client: AsyncOracleClient) -> Awaitable[None]: ...
    def stats(self) -> dict[str, int]: ...
    def close(self) -> Awaitable[None]: ...
    def fetch_rows(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> Awaitable[tuple[list[tuple[Any, ...]], tuple[str, ...]]]: ...
    def fetch_dicts(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> Awaitable[list[dict[str, Any]]]: ...
    def fetch_value(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> Awaitable[Any]: ...
    def execute_dml(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> Awaitable[tuple[int, str | None, dict[str, Any] | tuple[Any, ...] | None, bool]]: ...

__all__ = [
    "AsyncOracleClient",
    "AsyncOracleCursor",
    "AsyncOracleListener",
    "AsyncOraclePool",
    "FREE_THREADED",
    "GIL_USED",
    "ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED",
    "OracleColumn",
    "OracleNotification",
    "OracleOutBindVar",
    "OraclePipelineOutcome",
    "OracleQueryResult",
    "SyncOracleClient",
    "SyncOracleCursor",
    "SyncOracleListener",
    "SyncOraclePool",
    "__version__",
    "broker_engine_capsule",
]

def broker_engine_capsule() -> object: ...
