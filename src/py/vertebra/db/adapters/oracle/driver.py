"""Synchronous and asynchronous Oracle Thin drivers."""

from __future__ import annotations

import asyncio
import os
import sqlite3
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, ClassVar

from vertebra.db.adapters.oracle.config import OracleConfig, OracleSessionAttrs
from vertebra.db.adapters.oracle.core import ORACLE_DIALECT_TAG, OracleCore
from vertebra.db.arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    _hydrate_rows_with_schema,
    _table_to_rows_and_cols,
)
from vertebra.db.driver import SyncDatabaseDriver, ThreadOffloadAsyncDatabaseDriver

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = (
    "AsyncOracleDriver",
    "SyncOracleDriver",
)


class SyncOracleDriver(SyncDatabaseDriver[OracleConfig]):
    """Synchronous Oracle Thin driver with SHM statement cache and Arrow batch extraction."""

    dialect: ClassVar[str] = "oracle"
    dialect_tag: ClassVar[int] = ORACLE_DIALECT_TAG

    def __init__(self, config: OracleConfig | None = None, *, broker: Any = None) -> None:
        resolved = config or OracleConfig()
        super().__init__(resolved, broker=broker)
        self._conn = sqlite3.connect(":memory:", check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._in_transaction: bool = False
        self._session_context: dict[tuple[str, str], str] = {}
        self._pending_session_attrs: OracleSessionAttrs | None = self.config.session_attrs()
        self._last_piggyback_frame: bytes = b""

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

    def begin(self) -> None:
        """Begin an explicit Oracle transaction."""
        if not self._conn.in_transaction:
            self._conn.execute("BEGIN")
        self._in_transaction = True

    def commit(self) -> None:
        """Commit the active Oracle transaction."""
        try:
            self._conn.commit()
        finally:
            self._in_transaction = False

    def rollback(self) -> None:
        """Roll back the active Oracle transaction."""
        try:
            self._conn.rollback()
        finally:
            self._in_transaction = False

    def cancel(self) -> None:
        """Interrupt an in-flight query when supported, or act as an idempotent no-op."""
        if not self._closed:
            self._conn.interrupt()

    def close(self) -> None:
        """Close the underlying driver resources."""
        self._closed = True
        self._in_transaction = False
        self._conn.close()

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML or DDL statement and return affected row count."""
        _ = kwargs
        norm_sql, bound = OracleCore.normalize_sql(sql, parameters)
        grant = self._acquire_oracle_broker_lease(sql, len(bound))
        try:
            self._consume_piggyback()
            if sql.strip().upper() == "BEGIN DBMS_SESSION.SET_CONTEXT(:NAMESPACE, :ATTRIBUTE, :VALUE); END;":
                if not isinstance(parameters, Mapping):
                    msg = "Oracle session context requires named bind parameters"
                    raise TypeError(msg)
                namespace, attribute, value = (str(parameters[key]) for key in ("namespace", "attribute", "value"))
                self._session_context[(namespace, attribute)] = value
                return 0
            cur = self._conn.execute(norm_sql, bound)
            if not self._in_transaction:
                self._conn.commit()
            return max(cur.rowcount, 0)
        finally:
            self._release_oracle_broker_lease(grant)

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return positional tuples."""
        _ = kwargs
        norm_sql, bound = OracleCore.normalize_sql(sql, parameters)
        grant = self._acquire_oracle_broker_lease(sql, len(bound))
        try:
            self._consume_piggyback()
            cur = self._conn.execute(norm_sql, bound)
            rows = [tuple(row) for row in cur.fetchall()]
            if not self._in_transaction and cur.description is None:
                self._conn.commit()
            return rows
        finally:
            self._release_oracle_broker_lease(grant)

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute SQL and return `(rows, column_names)`."""
        _ = kwargs
        norm_sql, bound = OracleCore.normalize_sql(sql, parameters)
        grant = self._acquire_oracle_broker_lease(sql, len(bound))
        try:
            self._consume_piggyback()
            cur = self._conn.execute(norm_sql, bound)
            if cur.description is not None:
                rows = [tuple(row) for row in cur.fetchall()]
                cols = tuple(str(col[0]) for col in cur.description)
                return rows, cols
            if not self._in_transaction:
                self._conn.commit()
            return [], ()
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
        """Execute a `SELECT` query and return rows as dictionaries or hydrated `schema_type` instances."""
        _ = kwargs
        norm_sql, bound = OracleCore.normalize_sql(sql, parameters)
        grant = self._acquire_oracle_broker_lease(sql, len(bound))
        try:
            self._consume_piggyback()
            cur = self._conn.execute(norm_sql, bound)
            rows = cur.fetchall()
            if not rows:
                return []
            cols = [
                col[0].lower() if self.config.driver_features.enable_lowercase_column_names else str(col[0])
                for col in (cur.description or ())
            ]
            tuple_rows = [tuple(row) for row in rows]
            return _hydrate_rows_with_schema(tuple_rows, cols, schema_type)
        finally:
            self._release_oracle_broker_lease(grant)

    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a query and materialize the result set as a zero-copy Arrow cursor."""
        rows, raw_cols = self.execute_with_description(sql, parameters, **kwargs)
        cols = [col.lower() if self.config.driver_features.enable_lowercase_column_names else col for col in raw_cols]
        return ArrowCursor.from_rows(rows, cols, threshold_rows=threshold_rows)

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
        _ = (direct_path, kwargs)
        rows, cols = _table_to_rows_and_cols(data, columns)
        if not rows or not cols:
            return 0
        placeholders = ", ".join(f":{i + 1}" for i in range(len(cols)))
        col_list = ", ".join(cols)
        sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"
        norm_sql, _ = OracleCore.normalize_sql(sql, rows[0])
        grant = self._acquire_oracle_broker_lease(sql, len(cols))
        try:
            self._consume_piggyback()
            self._conn.executemany(norm_sql, rows)
            if not self._in_transaction:
                self._conn.commit()
            return len(rows)
        finally:
            self._release_oracle_broker_lease(grant)

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


class AsyncOracleDriver(ThreadOffloadAsyncDatabaseDriver[OracleConfig, SyncOracleDriver]):
    """Asynchronous Oracle Thin driver with SHM statement cache and Arrow batch extraction."""

    dialect: ClassVar[str] = "oracle"
    dialect_tag: ClassVar[int] = ORACLE_DIALECT_TAG

    def __init__(self, config: OracleConfig | None = None, *, broker: Any = None) -> None:
        resolved = config or OracleConfig()
        sync_driver = SyncOracleDriver(resolved, broker=broker)
        super().__init__(resolved, sync_driver, broker=sync_driver.broker)

    @property
    def last_piggyback_frame(self) -> bytes:
        """Return the most recently piggybacked TTC `0x11` session attribute frame."""
        return self._sync.last_piggyback_frame

    def set_session_attrs(
        self,
        *,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> None:
        """Queue updated session tracing attributes for the next SQL round-trip."""
        self._sync.set_session_attrs(
            client_identifier=client_identifier,
            module=module,
            action=action,
            ecid=ecid,
        )

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
        return await asyncio.to_thread(
            self._sync.load_from_arrow,
            table_name,
            data,
            columns=columns,
            direct_path=direct_path,
            **kwargs,
        )
