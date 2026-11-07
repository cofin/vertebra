"""Adapter-owned Spanner operations and execution policies for tool consumers."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, cast

from vertebra.db.adapters.spanner.config import SpannerConfig, StalenessBound
from vertebra.db.adapters.spanner.driver import AsyncSpannerDriver, SyncSpannerDriver
from vertebra.db.driver import AsyncDatabaseDriver
from vertebra.db.registry import DatabaseRegistry, provide_driver
from vertebra.typing import ToolPolicyError, ToolProvider

__all__ = ("SpannerToolProvider",)

_MUTATING_VERBS = frozenset(
    {
        "INSERT",
        "UPDATE",
        "DELETE",
        "MERGE",
        "CREATE",
        "ALTER",
        "DROP",
        "TRUNCATE",
        "EXECUTE",
        "CALL",
        "GRANT",
        "REVOKE",
        "EXPORT",
        "LOAD",
    }
)
_DDL_VERBS = frozenset({"CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT", "REVOKE"})
_LEADING_COMMENT_RE = re.compile(r"^\s*(?:--[^\n]*(?:\n|$)\s*|#[^\n]*(?:\n|$)\s*|/\*.*?\*/\s*)+", re.DOTALL)


class SpannerToolProvider(ToolProvider):
    """Expose typed native Spanner operations with lazy driver resolution."""

    def __init__(
        self,
        driver: AsyncSpannerDriver | SyncSpannerDriver | None = None,
        *,
        config: SpannerConfig | str | None = None,
        registry: DatabaseRegistry | None = None,
        read_only_staleness_seconds: float | None = None,
    ) -> None:
        self._driver = driver
        self._config = config
        self._registry = registry
        self._read_only_staleness_seconds = read_only_staleness_seconds

    def operations(self) -> Mapping[str, Callable[..., Awaitable[Any]]]:
        """Return original bound operations without creating a driver or connection."""
        return {
            "spanner_query": self.spanner_query,
            "spanner_schema_inspect": self.spanner_schema_inspect,
        }

    def confirmation_reason(self, operation_name: str, arguments: Mapping[str, Any]) -> str | None:
        """Require approval for mutations and ambiguous executable SQL without doing I/O."""
        if operation_name == "spanner_query":
            sql = arguments.get("sql")
            if not isinstance(sql, str) or not sql.strip():
                return None
            verb = self._leading_verb(sql)
            if verb and verb != "SELECT":
                return f"{verb} statement requires human confirmation."
        return None

    async def spanner_query(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        staleness_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Execute a governed Cloud Spanner query using lock-free ReadOnly snapshots for SELECTs."""
        drv = self._resolve_driver()
        verb = self._leading_verb(sql)
        if verb in _MUTATING_VERBS:
            if verb in _DDL_VERBS:
                op_meta = (
                    await drv.update_ddl([sql])
                    if isinstance(drv, AsyncDatabaseDriver)
                    else await asyncio.to_thread(drv.update_ddl, [sql])
                )
                return {
                    "mode": "ddl",
                    "operation": op_meta,
                }
            affected = (
                await drv.execute(sql, params)
                if isinstance(drv, AsyncDatabaseDriver)
                else await asyncio.to_thread(drv.execute, sql, params)
            )
            return {
                "mode": "dml",
                "affected_rows": int(affected),
            }

        eff_staleness = staleness_seconds if staleness_seconds is not None else self._read_only_staleness_seconds
        bound = (
            StalenessBound.exact_staleness(int(eff_staleness * 1000))
            if eff_staleness is not None
            else StalenessBound.strong()
        )
        if isinstance(drv, AsyncDatabaseDriver):
            rows = await drv.snapshot(bound).select(sql, params)
        else:
            rows = await asyncio.to_thread(drv.snapshot(bound).select, sql, params)
        return {
            "mode": "read_only_snapshot",
            "staleness_seconds": eff_staleness,
            "rows": list(rows),
            "row_count": len(rows),
        }

    async def spanner_schema_inspect(
        self,
        table_name: str | None = None,
    ) -> dict[str, Any]:
        """Inspect Cloud Spanner `INFORMATION_SCHEMA` tables and columns using a lock-free ReadOnly snapshot."""
        drv = self._resolve_driver()
        bound = (
            StalenessBound.exact_staleness(int(self._read_only_staleness_seconds * 1000))
            if self._read_only_staleness_seconds is not None
            else StalenessBound.strong()
        )
        is_spangres = (
            getattr(drv, "dialect", None) == "spangres"
            or getattr(getattr(drv, "config", None), "dialect", "googlesql") == "spangres"
        )
        query_params: Sequence[Any] | Mapping[str, Any] | None
        if is_spangres:
            if table_name:
                tables_sql = (
                    "SELECT table_name, parent_table_name, on_delete_action "
                    "FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1"
                )
                cols_sql = (
                    "SELECT table_name, column_name, spanner_type, is_nullable "
                    "FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 "
                    "ORDER BY ordinal_position"
                )
                query_params = [table_name]
            else:
                tables_sql = (
                    "SELECT table_name, parent_table_name, on_delete_action "
                    "FROM information_schema.tables WHERE table_schema = 'public'"
                )
                cols_sql = (
                    "SELECT table_name, column_name, spanner_type, is_nullable "
                    "FROM information_schema.columns WHERE table_schema = 'public' "
                    "ORDER BY table_name, ordinal_position"
                )
                query_params = None
        elif table_name:
            tables_sql = (
                "SELECT table_name, parent_table_name, on_delete_action "
                "FROM information_schema.tables WHERE table_name = @table_name"
            )
            cols_sql = (
                "SELECT table_name, column_name, spanner_type, is_nullable "
                "FROM information_schema.columns WHERE table_name = @table_name "
                "ORDER BY ordinal_position"
            )
            query_params = {"table_name": table_name}
        else:
            tables_sql = (
                "SELECT table_name, parent_table_name, on_delete_action "
                "FROM information_schema.tables WHERE table_schema = ''"
            )
            cols_sql = (
                "SELECT table_name, column_name, spanner_type, is_nullable "
                "FROM information_schema.columns WHERE table_schema = '' "
                "ORDER BY table_name, ordinal_position"
            )
            query_params = None

        if isinstance(drv, AsyncDatabaseDriver):
            async_snapshot = drv.snapshot(bound)
            tables = await async_snapshot.select(tables_sql, query_params)
            columns = await async_snapshot.select(cols_sql, query_params)
        else:
            sync_snapshot = drv.snapshot(bound)
            tables = await asyncio.to_thread(sync_snapshot.select, tables_sql, query_params)
            columns = await asyncio.to_thread(sync_snapshot.select, cols_sql, query_params)
        return {
            "table_name": table_name,
            "tables": list(tables),
            "columns": list(columns),
        }

    def _resolve_driver(self) -> AsyncSpannerDriver | SyncSpannerDriver:
        if self._driver is not None:
            return self._driver
        return cast(
            "AsyncSpannerDriver | SyncSpannerDriver", provide_driver(self._config or "default", registry=self._registry)
        )

    @staticmethod
    def _leading_verb(sql: str) -> str:
        body = sql.rstrip().rstrip(";").rstrip()
        if ";" in body:
            raise ToolPolicyError(
                "Multi-statement SQL is not allowed; submit one statement per call and pass values "
                "containing ';' as query parameters"
            )
        cleaned = _LEADING_COMMENT_RE.sub("", body).lstrip(" (;\t\r\n")
        return cleaned.split(None, 1)[0].rstrip(";(,").upper() if cleaned else ""
