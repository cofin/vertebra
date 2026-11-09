"""Type stubs for the vertebra._oracle native extension module."""

from collections.abc import Awaitable, Mapping, Sequence
from typing import Any, final

__version__: str
FREE_THREADED: bool
GIL_USED: bool

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
    def out_binds(self) -> Any: ...
    def __new__(
        cls,
        columns: Sequence[OracleColumn],
        rows: Sequence[tuple[Any, ...]],
        rowcount: int = 0,
        out_binds: Any = None,
        lastrowid: str | None = None,
    ) -> OracleQueryResult: ...
    def to_dicts(self) -> list[dict[str, Any]]: ...
    def __len__(self) -> int: ...
    def is_empty(self) -> bool: ...

@final
class SyncOracleClient:
    @property
    def closed(self) -> bool: ...
    @property
    def in_transaction(self) -> bool: ...
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
    ) -> SyncOracleClient: ...
    def connect(self) -> None: ...
    def close(self) -> None: ...
    def ping(self) -> None: ...
    def cancel(self) -> None: ...
    def begin(self) -> None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def savepoint(self, name: str) -> None: ...
    def rollback_to_savepoint(self, name: str) -> None: ...
    def execute(self, sql: str) -> int: ...

@final
class AsyncOracleClient:
    @property
    def closed(self) -> bool: ...
    @property
    def in_transaction(self) -> bool: ...
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
    ) -> AsyncOracleClient: ...
    def connect(self) -> Awaitable[None]: ...
    def close(self) -> Awaitable[None]: ...
    def ping(self) -> Awaitable[None]: ...
    def cancel(self) -> None: ...
    def begin(self) -> Awaitable[None]: ...
    def commit(self) -> Awaitable[None]: ...
    def rollback(self) -> Awaitable[None]: ...
    def savepoint(self, name: str) -> Awaitable[None]: ...
    def rollback_to_savepoint(self, name: str) -> Awaitable[None]: ...
    def execute(self, sql: str) -> Awaitable[int]: ...

__all__ = [
    "AsyncOracleClient",
    "FREE_THREADED",
    "GIL_USED",
    "OracleColumn",
    "OracleQueryResult",
    "SyncOracleClient",
    "__version__",
]
