"""Governed Spanner agent tools backed by the engine adapter."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, cast

from vertebra.agent.core import Tool
from vertebra.agent.runtime import ConfirmationGate
from vertebra.agent.tools import (
    _DDL_VERBS,
    GCPDatabaseToolPolicy,
    _extract_leading_verb,
    _require_confirmation,
    is_mutating_sql,
)
from vertebra.db.adapters.spanner import AsyncSpannerDriver, SpannerConfig, StalenessBound, SyncSpannerDriver
from vertebra.db.driver import AsyncDatabaseDriver
from vertebra.db.registry import provide_driver

if TYPE_CHECKING:
    from collections.abc import Mapping

    from vertebra.db.registry import DatabaseRegistry

__all__ = ("build_spanner_tools",)


def build_spanner_tools(
    *,
    driver: AsyncSpannerDriver | SyncSpannerDriver | None = None,
    config: SpannerConfig | str | None = None,
    policy: GCPDatabaseToolPolicy | None = None,
    confirmation_gate: ConfirmationGate | None = None,
    registry: DatabaseRegistry | None = None,
) -> dict[str, Tool]:
    """Build Spanner tools resolving a configured driver only when a tool is invoked."""
    policy = policy if policy is not None else GCPDatabaseToolPolicy()
    gate = confirmation_gate if confirmation_gate is not None else ConfirmationGate()

    def _resolve_driver() -> AsyncSpannerDriver | SyncSpannerDriver:
        if driver is not None:
            return driver
        return cast("AsyncSpannerDriver | SyncSpannerDriver", provide_driver(config or "default", registry=registry))

    async def spanner_query(
        sql: str,
        params: dict[str, Any] | None = None,
        staleness_seconds: float | None = None,
        confirmation_token: str | None = None,
    ) -> dict[str, Any]:
        """Execute a governed Cloud Spanner query using lock-free ReadOnly snapshots for SELECTs."""
        drv = _resolve_driver()
        if is_mutating_sql(sql):
            verb = _extract_leading_verb(sql)
            _require_confirmation(gate, "spanner_query", sql, confirmation_token)
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

        eff_staleness = staleness_seconds if staleness_seconds is not None else policy.read_only_staleness_seconds
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
        table_name: str | None = None,
    ) -> dict[str, Any]:
        """Inspect Cloud Spanner `INFORMATION_SCHEMA` tables and columns using a lock-free ReadOnly snapshot."""
        drv = _resolve_driver()
        bound = (
            StalenessBound.exact_staleness(int(policy.read_only_staleness_seconds * 1000))
            if policy.read_only_staleness_seconds is not None
            else StalenessBound.strong()
        )
        if table_name:
            tables_sql = (
                "SELECT table_name, parent_table_name, on_delete_action "
                "FROM information_schema.tables WHERE table_name = @table_name"
            )
            cols_sql = (
                "SELECT table_name, column_name, spanner_type, is_nullable "
                "FROM information_schema.columns WHERE table_name = @table_name "
                "ORDER BY ordinal_position"
            )
            query_params: Mapping[str, Any] | None = {"table_name": table_name}
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

    return {
        "spanner_query": Tool(
            spanner_query,
            name="spanner_query",
            description="Execute a governed Cloud Spanner query using lock-free ReadOnly snapshots.",
            requires_confirmation=False,
        ),
        "spanner_schema_inspect": Tool(
            spanner_schema_inspect,
            name="spanner_schema_inspect",
            description="Inspect Cloud Spanner INFORMATION_SCHEMA tables and columns.",
            requires_confirmation=False,
        ),
    }
