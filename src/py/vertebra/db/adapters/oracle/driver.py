"""Synchronous and asynchronous native Oracle 26ai Thin drivers."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any, ClassVar, Self

from vertebra.db.adapters.oracle.config import (
    DEFAULT_ORACLE_DSN,
    OracleConfig,
    OracleSessionAttrs,
)
from vertebra.db.adapters.oracle.core import ORACLE_DIALECT_TAG, OracleCore
from vertebra.db.arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    _hydrate_rows_with_schema,
    _table_to_rows_and_cols,
)
from vertebra.db.driver import AsyncDatabaseDriver, SyncDatabaseDriver
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.text import validate_identifier

__all__ = (
    "AsyncOracleDriver",
    "SyncOracleDriver",
)


def _load_oracle_module() -> Any:
    """Import and return the native `vertebra._oracle` extension module."""
    return ensure_module(
        "vertebra._oracle",
        package="vertebra._oracle",
        install_package="vertebra-core[oracle]",
        extra="oracle",
    )


def _build_client_kwargs(config: OracleConfig) -> dict[str, Any]:
    """Build keyword arguments for `SyncOracleClient` and `AsyncOracleClient` from `OracleConfig`."""
    desc = config.connect_descriptor()
    is_memory_dsn = config.dsn in (":memory:", "oracle://:memory:") or config.host.lower() == "memory"
    is_unconfigured_default = (
        config.dsn == DEFAULT_ORACLE_DSN
        and config.host == "localhost"
        and config.port == 1521
        and config.service_name == "FREEPDB1"
        and config.sid is None
        and config.user in ("system", None, "")
        and config.password in ("", None)
        and config.access_token is None
        and config.wallet_zip is None
        and config.wallet_path is None
        and config.connect_descriptor_str is None
        and config.topology is None
    )
    host = "memory" if (is_memory_dsn or is_unconfigured_default) else desc.host
    service_name = desc.service_name or config.service_name or "FREEPDB1"
    connect_descriptor: str | None = None
    if not (is_memory_dsn or is_unconfigured_default) and (
        config.connect_descriptor_str is not None or config.topology is not None or config.tns_alias is not None
    ):
        connect_descriptor = desc.to_tns_descriptor()
    wallet_location: str | None = str(config.wallet_path) if config.wallet_path is not None else None
    if wallet_location is None and isinstance(config.wallet_zip, (str, Path)):
        wallet_location = str(config.wallet_zip)
    token_opts = dict(config.token_auth_options) if config.token_auth_options is not None else None
    connect_timeout = float(desc.transport_connect_timeout) if desc.transport_connect_timeout > 0 else 10.0
    stmtcachesize = max(0, int(config.statement_cache_size))
    return {
        "user": config.user or None,
        "password": config.password or None,
        "dsn": None if (is_memory_dsn or is_unconfigured_default) else config.dsn,
        "host": host,
        "port": desc.port,
        "service_name": service_name,
        "sid": desc.sid,
        "tns_admin": None,
        "connect_descriptor": connect_descriptor,
        "protocol": desc.protocol.lower(),
        "ssl_server_dn_match": desc.security_cert_dn is not None or desc.protocol.upper() == "TCPS",
        "ssl_server_cert_dn": desc.security_cert_dn,
        "wallet_location": wallet_location,
        "wallet_password": None,
        "pem_cert_data": None,
        "cwallet_sso_data": bytes(config.wallet_zip) if isinstance(config.wallet_zip, (bytes, bytearray)) else None,
        "auth_mode": config.auth_mode,
        "access_token": config.access_token,
        "token_auth_options": token_opts,
        "cclass": config.drcp.cclass,
        "purity": config.drcp.purity,
        "connection_tag": config.drcp.connection_tag,
        "stmtcachesize": stmtcachesize,
        "prefetchrows": config.driver_features.prefetchrows,
        "arraysize": config.driver_features.arraysize,
        "call_timeout_ms": None,
        "connect_timeout": connect_timeout,
        "enable_lowercase_column_names": config.driver_features.enable_lowercase_column_names,
        "client_identifier": config.client_identifier,
        "module": config.module,
        "action": config.action,
        "ecid": config.ecid,
    }


def _is_dml_or_plsql(sql: str) -> bool:
    """Return `True` when `sql` begins with a DML, DDL, or PL/SQL keyword rather than `SELECT`/`WITH`."""
    trimmed = sql.lstrip()
    while trimmed.startswith("/*"):
        end = trimmed.find("*/")
        if end == -1:
            break
        trimmed = trimmed[end + 2 :].lstrip()
    upper = trimmed.upper()
    return not (upper.startswith("SELECT") or upper.startswith("WITH"))


class SyncOracleDriver(SyncDatabaseDriver[OracleConfig]):
    """Synchronous Oracle 26ai Thin driver backed by `vertebra._oracle.SyncOracleClient`."""

    dialect: ClassVar[str] = "oracle"
    dialect_tag: ClassVar[int] = ORACLE_DIALECT_TAG

    def __init__(
        self,
        config: OracleConfig | None = None,
        *,
        broker: Any = None,
        client: Any = None,
    ) -> None:
        resolved = config or OracleConfig()
        super().__init__(resolved, broker=broker)
        if client is not None:
            self._client = client
        else:
            mod = _load_oracle_module()
            self._client = mod.SyncOracleClient(**_build_client_kwargs(resolved))
        self._in_transaction: bool = False
        self._session_context: dict[tuple[str, str], str] = {}
        self._pending_session_attrs: OracleSessionAttrs | None = self.config.session_attrs()
        self._last_piggyback_frame: bytes = b""

    @property
    def client(self) -> Any:
        """Return the underlying `SyncOracleClient` instance."""
        return self._client

    @property
    def in_transaction(self) -> bool:
        """Return `True` when an explicit transaction is active on this driver."""
        return bool(getattr(self._client, "in_transaction", self._in_transaction))

    @property
    def last_piggyback_frame(self) -> bytes:
        """Return the most recently piggybacked TTC `0x11` session attribute frame."""
        return self._last_piggyback_frame

    def set_session_attrs(
        self,
        *,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> None:
        """Queue updated session tracing attributes to be piggybacked onto the next SQL round-trip."""
        self._pending_session_attrs = OracleSessionAttrs(
            client_identifier=client_identifier,
            module=module,
            action=action,
            ecid=ecid,
        )

    def connect(self) -> Self:
        """Establish or validate the underlying Oracle session."""
        self._client.connect()
        self._closed = False
        return self

    def ping(self) -> None:
        """Verify liveness of the underlying Oracle session."""
        self._client.ping()

    def begin(self) -> None:
        """Begin an explicit Oracle transaction."""
        self._client.begin()
        self._in_transaction = True

    def commit(self) -> None:
        """Commit the active Oracle transaction."""
        try:
            self._client.commit()
        finally:
            self._in_transaction = False

    def rollback(self) -> None:
        """Roll back the active Oracle transaction."""
        try:
            self._client.rollback()
        finally:
            self._in_transaction = False

    def savepoint(self, name: str) -> None:
        """Create a transaction savepoint with identifier validation."""
        self._client.savepoint(name)
        self._in_transaction = True

    def rollback_to_savepoint(self, name: str) -> None:
        """Roll back to a previously established savepoint."""
        self._client.rollback_to_savepoint(name)
        self._in_transaction = True

    def cancel(self) -> None:
        """Trigger an out-of-band `break_execution()` on any in-flight command."""
        if not self._closed:
            self._client.cancel()

    def close(self) -> None:
        """Close the underlying native Oracle client and release worker resources."""
        self._closed = True
        self._in_transaction = False
        self._client.close()

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML, DDL, or PL/SQL statement and return the affected row count."""
        out_binds = kwargs.get("out_binds")
        coerced = self._coerce_parameters(parameters)
        param_count = len(coerced) if coerced is not None else 0
        grant = self._acquire_oracle_broker_lease(sql, param_count)
        try:
            self._consume_piggyback()
            self._track_session_context_sql(sql, parameters)
            result = self._client.execute(
                sql,
                coerced,
                out_binds=out_binds,
                fetch_decimals=self.config.driver_features.fetch_decimals,
                enable_lowercase_column_names=self.config.driver_features.enable_lowercase_column_names,
            )
            if isinstance(result, int):
                return max(result, 0)
            return max(int(result.rowcount), 0)
        finally:
            self._release_oracle_broker_lease(grant)

    def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a DML statement across a batch of parameter sets and return total affected rows."""
        _ = kwargs
        if not parameter_sets:
            return 0
        coerced_seq = [self._coerce_parameters(p) or () for p in parameter_sets]
        first_len = len(coerced_seq[0]) if coerced_seq else 0
        grant = self._acquire_oracle_broker_lease(sql, first_len)
        try:
            self._consume_piggyback()
            return int(self._client.execute_many(sql, coerced_seq))
        finally:
            self._release_oracle_broker_lease(grant)

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query or `DML ... RETURNING ... INTO` statement and return positional tuples."""
        rows, _ = self.execute_with_description(sql, parameters, **kwargs)
        return rows

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute SQL and return `(rows, column_names)` with configured column-casing and cell coercion."""
        out_binds = kwargs.get("out_binds")
        coerced = self._coerce_parameters(parameters)
        param_count = len(coerced) if coerced is not None else 0
        grant = self._acquire_oracle_broker_lease(sql, param_count)
        try:
            self._consume_piggyback()
            self._track_session_context_sql(sql, parameters)
            lowercase = self.config.driver_features.enable_lowercase_column_names
            fetch_decimals = self.config.driver_features.fetch_decimals
            if _is_dml_or_plsql(sql):
                result = self._client.execute(
                    sql,
                    coerced,
                    out_binds=out_binds,
                    fetch_decimals=fetch_decimals,
                    enable_lowercase_column_names=lowercase,
                )
            else:
                result = self._client.query(
                    sql,
                    coerced,
                    out_binds=out_binds,
                    fetch_decimals=fetch_decimals,
                    enable_lowercase_column_names=lowercase,
                )
            return self._coerce_result_rows_and_cols(result)
        finally:
            self._release_oracle_broker_lease(grant)

    def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `SELECT` or `DML ... RETURNING` query and hydrate dictionaries or `schema_type` instances."""
        rows, cols = self.execute_with_description(sql, parameters, **kwargs)
        if not rows:
            return []
        return _hydrate_rows_with_schema(rows, cols, schema_type)

    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int | None = None,
        force_shm: bool | None = None,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a query and materialize the result set as a zero-copy `ArrowCursor`."""
        _ = (batch_size, force_shm)
        rows, raw_cols = self.execute_with_description(sql, parameters, **kwargs)
        cols = [col.lower() if self.config.driver_features.enable_lowercase_column_names else col for col in raw_cols]
        return ArrowCursor.from_rows(rows, cols, threshold_rows=threshold_rows)

    def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 8192,
        **kwargs: Any,
    ) -> Iterator[ArrowCursor]:
        """Stream query results as a sequence of `ArrowCursor` batches of at most `batch_size` rows."""
        chunk = max(1, int(batch_size))
        rows, cols = self.execute_with_description(sql, parameters, **kwargs)
        for idx in range(0, len(rows), chunk):
            yield ArrowCursor.from_rows(rows[idx : idx + chunk], cols)

    def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        direct_path: bool | None = None,
        **kwargs: Any,
    ) -> int:
        """Bulk-ingest an Arrow Table, `ArrowCursor`, or row sequence into `table_name`."""
        _ = kwargs
        validate_identifier(table_name, "table_name", allow_dots=True)
        if columns is not None:
            for col in columns:
                validate_identifier(str(col), "column_name", allow_dots=False)
        rows, cols = _table_to_rows_and_cols(data, columns)
        for col in cols:
            validate_identifier(str(col), "column_name", allow_dots=False)
        if not rows or not cols:
            return 0
        use_direct_path = (
            self.config.driver_features.enable_direct_path_load
            if direct_path is None
            else bool(direct_path)
        )
        hint = "/*+ APPEND_VALUES */ " if use_direct_path else ""
        placeholders = ", ".join(f":{i + 1}" for i in range(len(cols)))
        col_list = ", ".join(str(col) for col in cols)
        sql = f"INSERT {hint}INTO {table_name} ({col_list}) VALUES ({placeholders})"
        return self.execute_many(sql, rows)

    def _coerce_parameters(
        self,
        parameters: Sequence[Any] | Mapping[str, Any] | None,
    ) -> list[Any] | dict[str, Any] | None:
        if parameters is None:
            return None
        features = self.config.driver_features
        if isinstance(parameters, Mapping):
            return {
                str(k).lstrip(":"): OracleCore.coerce_bind_parameter(v, features=features)
                for k, v in parameters.items()
            }
        if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes, bytearray)):
            return [OracleCore.coerce_bind_parameter(v, features=features) for v in parameters]
        return [OracleCore.coerce_bind_parameter(v, features=features) for v in parameters]

    def _coerce_result_rows_and_cols(
        self,
        result: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        features = self.config.driver_features
        lowercase = features.enable_lowercase_column_names
        raw_columns = getattr(result, "columns", ()) or ()
        cols = tuple(
            str(col.name).lower() if lowercase else str(col.name)
            for col in raw_columns
        )
        col_types = tuple(
            str(getattr(col, "oracle_type", getattr(col, "type_name", "VARCHAR2")))
            for col in raw_columns
        )
        raw_rows = getattr(result, "rows", ()) or ()
        if raw_rows:
            coerced_rows = [
                tuple(
                    OracleCore.coerce_row_cell(
                        cell,
                        col_types[idx] if idx < len(col_types) else None,
                        features=features,
                    )
                    for idx, cell in enumerate(row)
                )
                for row in raw_rows
            ]
            return coerced_rows, cols

        out_binds = getattr(result, "out_binds", None)
        if out_binds is not None:
            if isinstance(out_binds, Mapping):
                out_cols = cols or tuple(
                    str(k).lstrip(":").lower() if lowercase else str(k).lstrip(":")
                    for k in out_binds
                )
                row_tuple = tuple(
                    OracleCore.coerce_row_cell(
                        val,
                        col_types[idx] if idx < len(col_types) else None,
                        features=features,
                    )
                    for idx, val in enumerate(out_binds.values())
                )
                return [row_tuple], out_cols
            if isinstance(out_binds, Sequence) and not isinstance(out_binds, (str, bytes, bytearray)):
                out_cols = cols or tuple(f"out_{i + 1}" for i in range(len(out_binds)))
                row_tuple = tuple(
                    OracleCore.coerce_row_cell(
                        val,
                        col_types[idx] if idx < len(col_types) else None,
                        features=features,
                    )
                    for idx, val in enumerate(out_binds)
                )
                return [row_tuple], out_cols

        return [], cols

    def _track_session_context_sql(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None,
    ) -> None:
        upper = sql.strip().upper()
        if "SET_CONTEXT" in upper and isinstance(parameters, Mapping):
            norm = {str(k).lstrip(":").lower(): v for k, v in parameters.items()}
            if all(k in norm for k in ("namespace", "attribute", "value")):
                self._session_context[(str(norm["namespace"]), str(norm["attribute"]))] = str(norm["value"])

    def _acquire_oracle_broker_lease(self, sql: str, param_count: int) -> Any:
        if self._broker is None:
            return None
        if hasattr(self._broker, "statement_cache"):
            sc_attr = self._broker.statement_cache
            sc = sc_attr() if callable(sc_attr) else sc_attr
            if sc is not None and hasattr(sc, "lookup") and hasattr(sc, "insert"):
                sql_bytes = sql.encode("utf-8")
                try:
                    cached = sc.lookup(ORACLE_DIALECT_TAG, sql)
                    if cached is None:
                        sc.insert(
                            ORACLE_DIALECT_TAG,
                            sql,
                            param_count,
                            sql_bytes,
                            b"\x00\x01",
                        )
                except TypeError:
                    cached = sc.lookup(ORACLE_DIALECT_TAG, sql_bytes)
                    if cached is None:
                        sc.insert(
                            ORACLE_DIALECT_TAG,
                            sql_bytes,
                            param_count,
                            sql_bytes,
                            b"\x00\x01",
                        )
        if hasattr(self._broker, "lease_table") and self._broker.lease_table is not None:
            return self._broker.lease_table.acquire(os.getpid())
        if hasattr(self._broker, "acquire"):
            try:
                return self._broker.acquire(os.getpid())
            except TypeError:
                return self._broker.acquire(bind_key="oracle")
        return None

    def _release_oracle_broker_lease(self, grant: Any) -> None:
        if grant is not None and self._broker is not None:
            if hasattr(self._broker, "lease_table") and self._broker.lease_table is not None:
                self._broker.lease_table.release(grant.slot_index, grant.generation)
            elif hasattr(self._broker, "release") and hasattr(grant, "slot_index"):
                self._broker.release(grant.slot_index, grant.generation)

    def _consume_piggyback(self) -> None:
        if self._pending_session_attrs is not None:
            attrs = self._pending_session_attrs
            if any((attrs.client_identifier, attrs.module, attrs.action, attrs.ecid)):
                self._last_piggyback_frame = attrs.encode_ttc_piggyback()
            self._pending_session_attrs = None


class AsyncOracleDriver(AsyncDatabaseDriver[OracleConfig]):
    """Asynchronous Oracle 26ai Thin driver backed by `vertebra._oracle.AsyncOracleClient`."""

    dialect: ClassVar[str] = "oracle"
    dialect_tag: ClassVar[int] = ORACLE_DIALECT_TAG

    def __init__(
        self,
        config: OracleConfig | None = None,
        *,
        broker: Any = None,
        client: Any = None,
    ) -> None:
        resolved = config or OracleConfig()
        super().__init__(resolved, broker=broker)
        if client is not None:
            self._client = client
        else:
            mod = _load_oracle_module()
            self._client = mod.AsyncOracleClient(**_build_client_kwargs(resolved))
        self._in_transaction: bool = False
        self._session_context: dict[tuple[str, str], str] = {}
        self._pending_session_attrs: OracleSessionAttrs | None = self.config.session_attrs()
        self._last_piggyback_frame: bytes = b""

    @property
    def client(self) -> Any:
        """Return the underlying `AsyncOracleClient` instance."""
        return self._client

    @property
    def in_transaction(self) -> bool:
        """Return `True` when an explicit transaction is active on this driver."""
        return bool(getattr(self._client, "in_transaction", self._in_transaction))

    @property
    def last_piggyback_frame(self) -> bytes:
        """Return the most recently piggybacked TTC `0x11` session attribute frame."""
        return self._last_piggyback_frame

    def set_session_attrs(
        self,
        *,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> None:
        """Queue updated session tracing attributes for the next SQL round-trip."""
        self._pending_session_attrs = OracleSessionAttrs(
            client_identifier=client_identifier,
            module=module,
            action=action,
            ecid=ecid,
        )

    async def connect(self) -> Self:
        """Establish or validate the underlying asynchronous Oracle session."""
        await self._client.connect()
        self._closed = False
        return self

    async def ping(self) -> None:
        """Verify liveness of the underlying asynchronous Oracle session."""
        await self._client.ping()

    async def begin(self) -> None:
        """Begin an explicit asynchronous Oracle transaction."""
        await self._client.begin()
        self._in_transaction = True

    async def commit(self) -> None:
        """Commit the active asynchronous Oracle transaction."""
        try:
            await self._client.commit()
        finally:
            self._in_transaction = False

    async def rollback(self) -> None:
        """Roll back the active asynchronous Oracle transaction."""
        try:
            await self._client.rollback()
        finally:
            self._in_transaction = False

    async def savepoint(self, name: str) -> None:
        """Create a transaction savepoint asynchronously."""
        await self._client.savepoint(name)
        self._in_transaction = True

    async def rollback_to_savepoint(self, name: str) -> None:
        """Roll back to a previously established savepoint asynchronously."""
        await self._client.rollback_to_savepoint(name)
        self._in_transaction = True

    async def cancel(self) -> None:
        """Trigger an out-of-band `break_execution()` on any in-flight command."""
        if not self._closed:
            self._client.cancel()

    async def close(self) -> None:
        """Close the underlying native asynchronous Oracle client."""
        self._closed = True
        self._in_transaction = False
        await self._client.close()

    async def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML, DDL, or PL/SQL statement asynchronously and return affected row count."""
        out_binds = kwargs.get("out_binds")
        coerced = self._coerce_parameters(parameters)
        param_count = len(coerced) if coerced is not None else 0
        grant = self._acquire_oracle_broker_lease(sql, param_count)
        try:
            self._consume_piggyback()
            self._track_session_context_sql(sql, parameters)
            result = await self._client.execute(
                sql,
                coerced,
                out_binds=out_binds,
                fetch_decimals=self.config.driver_features.fetch_decimals,
                enable_lowercase_column_names=self.config.driver_features.enable_lowercase_column_names,
            )
            if isinstance(result, int):
                return max(result, 0)
            return max(int(result.rowcount), 0)
        finally:
            self._release_oracle_broker_lease(grant)

    async def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a DML statement across a batch of parameter sets asynchronously."""
        _ = kwargs
        if not parameter_sets:
            return 0
        coerced_seq = [self._coerce_parameters(p) or () for p in parameter_sets]
        first_len = len(coerced_seq[0]) if coerced_seq else 0
        grant = self._acquire_oracle_broker_lease(sql, first_len)
        try:
            self._consume_piggyback()
            return int(await self._client.execute_many(sql, coerced_seq))
        finally:
            self._release_oracle_broker_lease(grant)

    async def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query or `DML ... RETURNING ... INTO` statement and return positional tuples."""
        rows, _ = await self.execute_with_description(sql, parameters, **kwargs)
        return rows

    async def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute SQL asynchronously and return `(rows, column_names)`."""
        out_binds = kwargs.get("out_binds")
        coerced = self._coerce_parameters(parameters)
        param_count = len(coerced) if coerced is not None else 0
        grant = self._acquire_oracle_broker_lease(sql, param_count)
        try:
            self._consume_piggyback()
            self._track_session_context_sql(sql, parameters)
            lowercase = self.config.driver_features.enable_lowercase_column_names
            fetch_decimals = self.config.driver_features.fetch_decimals
            if _is_dml_or_plsql(sql):
                result = await self._client.execute(
                    sql,
                    coerced,
                    out_binds=out_binds,
                    fetch_decimals=fetch_decimals,
                    enable_lowercase_column_names=lowercase,
                )
            else:
                result = await self._client.query(
                    sql,
                    coerced,
                    out_binds=out_binds,
                    fetch_decimals=fetch_decimals,
                    enable_lowercase_column_names=lowercase,
                )
            return self._coerce_result_rows_and_cols(result)
        finally:
            self._release_oracle_broker_lease(grant)

    async def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `SELECT` or `DML ... RETURNING` query asynchronously and hydrate rows."""
        rows, cols = await self.execute_with_description(sql, parameters, **kwargs)
        if not rows:
            return []
        return _hydrate_rows_with_schema(rows, cols, schema_type)

    async def exec_returning_async(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `DML ... RETURNING ... INTO` statement in one round-trip and return hydrated rows."""
        return await self.select(sql, parameters, schema_type=schema_type, **kwargs)

    async def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int | None = None,
        force_shm: bool | None = None,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a query asynchronously and return a zero-copy `ArrowCursor`."""
        _ = (batch_size, force_shm)
        rows, raw_cols = await self.execute_with_description(sql, parameters, **kwargs)
        cols = [col.lower() if self.config.driver_features.enable_lowercase_column_names else col for col in raw_cols]
        return ArrowCursor.from_rows(rows, cols, threshold_rows=threshold_rows)

    async def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 8192,
        **kwargs: Any,
    ) -> AsyncIterator[ArrowCursor]:
        """Asynchronously stream query results as a sequence of `ArrowCursor` batches."""
        chunk = max(1, int(batch_size))
        rows, cols = await self.execute_with_description(sql, parameters, **kwargs)
        for idx in range(0, len(rows), chunk):
            yield ArrowCursor.from_rows(rows[idx : idx + chunk], cols)

    async def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        direct_path: bool | None = None,
        **kwargs: Any,
    ) -> int:
        """Asynchronously bulk-ingest an Arrow Table or `ArrowCursor` into `table_name`."""
        _ = kwargs
        validate_identifier(table_name, "table_name", allow_dots=True)
        if columns is not None:
            for col in columns:
                validate_identifier(str(col), "column_name", allow_dots=False)
        rows, cols = _table_to_rows_and_cols(data, columns)
        for col in cols:
            validate_identifier(str(col), "column_name", allow_dots=False)
        if not rows or not cols:
            return 0
        use_direct_path = (
            self.config.driver_features.enable_direct_path_load
            if direct_path is None
            else bool(direct_path)
        )
        hint = "/*+ APPEND_VALUES */ " if use_direct_path else ""
        placeholders = ", ".join(f":{i + 1}" for i in range(len(cols)))
        col_list = ", ".join(str(col) for col in cols)
        sql = f"INSERT {hint}INTO {table_name} ({col_list}) VALUES ({placeholders})"
        return await self.execute_many(sql, rows)

    def _coerce_parameters(
        self,
        parameters: Sequence[Any] | Mapping[str, Any] | None,
    ) -> list[Any] | dict[str, Any] | None:
        if parameters is None:
            return None
        features = self.config.driver_features
        if isinstance(parameters, Mapping):
            return {
                str(k).lstrip(":"): OracleCore.coerce_bind_parameter(v, features=features)
                for k, v in parameters.items()
            }
        if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes, bytearray)):
            return [OracleCore.coerce_bind_parameter(v, features=features) for v in parameters]
        return [OracleCore.coerce_bind_parameter(v, features=features) for v in parameters]

    def _coerce_result_rows_and_cols(
        self,
        result: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        features = self.config.driver_features
        lowercase = features.enable_lowercase_column_names
        raw_columns = getattr(result, "columns", ()) or ()
        cols = tuple(
            str(col.name).lower() if lowercase else str(col.name)
            for col in raw_columns
        )
        col_types = tuple(
            str(getattr(col, "oracle_type", getattr(col, "type_name", "VARCHAR2")))
            for col in raw_columns
        )
        raw_rows = getattr(result, "rows", ()) or ()
        if raw_rows:
            coerced_rows = [
                tuple(
                    OracleCore.coerce_row_cell(
                        cell,
                        col_types[idx] if idx < len(col_types) else None,
                        features=features,
                    )
                    for idx, cell in enumerate(row)
                )
                for row in raw_rows
            ]
            return coerced_rows, cols

        out_binds = getattr(result, "out_binds", None)
        if out_binds is not None:
            if isinstance(out_binds, Mapping):
                out_cols = cols or tuple(
                    str(k).lstrip(":").lower() if lowercase else str(k).lstrip(":")
                    for k in out_binds
                )
                row_tuple = tuple(
                    OracleCore.coerce_row_cell(
                        val,
                        col_types[idx] if idx < len(col_types) else None,
                        features=features,
                    )
                    for idx, val in enumerate(out_binds.values())
                )
                return [row_tuple], out_cols
            if isinstance(out_binds, Sequence) and not isinstance(out_binds, (str, bytes, bytearray)):
                out_cols = cols or tuple(f"out_{i + 1}" for i in range(len(out_binds)))
                row_tuple = tuple(
                    OracleCore.coerce_row_cell(
                        val,
                        col_types[idx] if idx < len(col_types) else None,
                        features=features,
                    )
                    for idx, val in enumerate(out_binds)
                )
                return [row_tuple], out_cols

        return [], cols

    def _track_session_context_sql(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None,
    ) -> None:
        upper = sql.strip().upper()
        if "SET_CONTEXT" in upper and isinstance(parameters, Mapping):
            norm = {str(k).lstrip(":").lower(): v for k, v in parameters.items()}
            if all(k in norm for k in ("namespace", "attribute", "value")):
                self._session_context[(str(norm["namespace"]), str(norm["attribute"]))] = str(norm["value"])

    def _acquire_oracle_broker_lease(self, sql: str, param_count: int) -> Any:
        if self._broker is None:
            return None
        if hasattr(self._broker, "statement_cache"):
            sc_attr = self._broker.statement_cache
            sc = sc_attr() if callable(sc_attr) else sc_attr
            if sc is not None and hasattr(sc, "lookup") and hasattr(sc, "insert"):
                sql_bytes = sql.encode("utf-8")
                try:
                    cached = sc.lookup(ORACLE_DIALECT_TAG, sql)
                    if cached is None:
                        sc.insert(
                            ORACLE_DIALECT_TAG,
                            sql,
                            param_count,
                            sql_bytes,
                            b"\x00\x01",
                        )
                except TypeError:
                    cached = sc.lookup(ORACLE_DIALECT_TAG, sql_bytes)
                    if cached is None:
                        sc.insert(
                            ORACLE_DIALECT_TAG,
                            sql_bytes,
                            param_count,
                            sql_bytes,
                            b"\x00\x01",
                        )
        if hasattr(self._broker, "lease_table") and self._broker.lease_table is not None:
            return self._broker.lease_table.acquire(os.getpid())
        if hasattr(self._broker, "acquire"):
            try:
                return self._broker.acquire(os.getpid())
            except TypeError:
                return self._broker.acquire(bind_key="oracle")
        return None

    def _release_oracle_broker_lease(self, grant: Any) -> None:
        if grant is not None and self._broker is not None:
            if hasattr(self._broker, "lease_table") and self._broker.lease_table is not None:
                self._broker.lease_table.release(grant.slot_index, grant.generation)
            elif hasattr(self._broker, "release") and hasattr(grant, "slot_index"):
                self._broker.release(grant.slot_index, grant.generation)

    def _consume_piggyback(self) -> None:
        if self._pending_session_attrs is not None:
            attrs = self._pending_session_attrs
            if any((attrs.client_identifier, attrs.module, attrs.action, attrs.ecid)):
                self._last_piggyback_frame = attrs.encode_ttc_piggyback()
            self._pending_session_attrs = None
