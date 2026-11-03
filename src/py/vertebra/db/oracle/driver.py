"""Synchronous and asynchronous Oracle Thin drivers."""

from __future__ import annotations

import asyncio
import os
import sqlite3
from typing import TYPE_CHECKING, Any, ClassVar

from vertebra.db._arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    _hydrate_rows_with_schema,
    _table_to_rows_and_cols,
)
from vertebra.db.driver import SyncDatabaseDriver, ThreadOffloadAsyncDatabaseDriver
from vertebra.db.oracle.config import OracleConfig, OracleSessionAttrs
from vertebra.db.oracle.core import ORACLE_DIALECT_TAG, OracleCore

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

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

    def execute(  # type: ignore[override]
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> int:
        """Execute a DML or DDL statement and return affected row count."""
        norm_sql, bound = OracleCore.normalize_sql(sql, params)
        grant = self._acquire_oracle_broker_lease(sql, len(bound))
        try:
            self._consume_piggyback()
            cur = self._conn.execute(norm_sql, bound)
            self._conn.commit()
            return max(cur.rowcount, 0)
        finally:
            self._release_oracle_broker_lease(grant)

    def select(  # type: ignore[override]
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute a `SELECT` query and return rows as dictionaries or hydrated `schema_type` instances."""
        norm_sql, bound = OracleCore.normalize_sql(sql, params)
        grant = self._acquire_oracle_broker_lease(sql, len(bound))
        try:
            self._consume_piggyback()
            cur = self._conn.execute(norm_sql, bound)
            rows = cur.fetchall()
            if not rows:
                return []
            cols = [
                col[0].lower() if self.config.driver_features.enable_lowercase_column_names else col[0]
                for col in (cur.description or ())
            ]
            tuple_rows = [tuple(row) for row in rows]
            return _hydrate_rows_with_schema(tuple_rows, cols, schema_type)
        finally:
            self._release_oracle_broker_lease(grant)

    def select_one(  # type: ignore[override]
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any:
        """Execute a `SELECT` query and return the first row or raise `ValueError`."""
        rows = self.select(sql, params, schema_type=schema_type)
        if not rows:
            msg = "No row found for select_one"
            raise ValueError(msg)
        return rows[0]

    def select_one_or_none(  # type: ignore[override]
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Execute a `SELECT` query and return the first row or `None`."""
        rows = self.select(sql, params, schema_type=schema_type)
        return rows[0] if rows else None

    def load_from_arrow(  # type: ignore[override]
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        direct_path: bool | None = None,
    ) -> int:
        """Bulk-ingest an Arrow Table, `ArrowCursor`, or row sequence into `table_name`."""
        _ = direct_path
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
            self._conn.commit()
            return len(rows)
        finally:
            self._release_oracle_broker_lease(grant)

    def execute_tuples(  # type: ignore[override]
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return positional tuples."""
        norm_sql, bound = OracleCore.normalize_sql(sql, params)
        grant = self._acquire_oracle_broker_lease(sql, len(bound))
        try:
            self._consume_piggyback()
            cur = self._conn.execute(norm_sql, bound)
            rows = [tuple(row) for row in cur.fetchall()]
            self._conn.commit()
            return rows
        finally:
            self._release_oracle_broker_lease(grant)

    def execute_with_description(  # type: ignore[override]
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[Any, ...], ...] | None, int]:
        """Execute SQL and return `(rows, description, rowcount)` for DBAPI compatibility."""
        norm_sql, bound = OracleCore.normalize_sql(sql, params)
        grant = self._acquire_oracle_broker_lease(sql, len(bound))
        try:
            self._consume_piggyback()
            cur = self._conn.execute(norm_sql, bound)
            if cur.description is not None:
                rows = [tuple(row) for row in cur.fetchall()]
                desc = tuple((col[0], None, None, None, None, None, None) for col in cur.description)
                return rows, desc, len(rows)
            self._conn.commit()
            return [], None, max(cur.rowcount, 0)
        finally:
            self._release_oracle_broker_lease(grant)

    def select_to_arrow(  # type: ignore[override]
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        shm_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
    ) -> ArrowCursor:
        """Execute a query and materialize the result set as a zero-copy Arrow cursor."""
        rows = self.select(sql, params)
        return ArrowCursor.from_rows(rows, threshold_rows=shm_threshold_rows)

    def close(self) -> None:
        """Close the underlying driver resources."""
        self._closed = True
        self._conn.close()

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

    async def execute(  # type: ignore[override]
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> int:
        """Execute a DML or DDL statement asynchronously."""
        return await asyncio.to_thread(self._sync.execute, sql, params)

    async def select(  # type: ignore[override]
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute a `SELECT` query asynchronously and return dictionaries or `schema_type` instances."""
        return await asyncio.to_thread(self._sync.select, sql, params, schema_type=schema_type)

    async def select_one(  # type: ignore[override]
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any:
        """Execute a `SELECT` query asynchronously and return the first row or raise `ValueError`."""
        return await asyncio.to_thread(self._sync.select_one, sql, params, schema_type=schema_type)

    async def select_one_or_none(  # type: ignore[override]
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Execute a `SELECT` query asynchronously and return the first row or `None`."""
        return await asyncio.to_thread(self._sync.select_one_or_none, sql, params, schema_type=schema_type)

    async def load_from_arrow(  # type: ignore[override]
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        direct_path: bool | None = None,
    ) -> int:
        """Asynchronously bulk-ingest an Arrow Table or `ArrowCursor` into `table_name`."""
        return await asyncio.to_thread(
            self._sync.load_from_arrow,
            table_name,
            data,
            columns=columns,
            direct_path=direct_path,
        )

    async def select_to_arrow(  # type: ignore[override]
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        shm_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
    ) -> ArrowCursor:
        """Execute a query asynchronously and return a zero-copy `ArrowCursor`."""
        return await asyncio.to_thread(
            self._sync.select_to_arrow,
            sql,
            params,
            shm_threshold_rows=shm_threshold_rows,
        )
