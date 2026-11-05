"""Governed BigQuery and Spanner tools owned by the agent layer."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from vertebra.agent.core import PermissionDeniedError, Tool
from vertebra.agent.runtime import ConfirmationGate
from vertebra.typing import FrozenBaseStruct
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from vertebra.agent.tools.bigquery import build_bigquery_tools
    from vertebra.agent.tools.spanner import build_spanner_tools
    from vertebra.db.adapters.bigquery import AsyncBigQueryDriver, BigQueryConfig, SyncBigQueryDriver
    from vertebra.db.adapters.spanner import AsyncSpannerDriver, SpannerConfig, SyncSpannerDriver
    from vertebra.db.registry import DatabaseRegistry

__all__ = (
    "GCPDatabaseToolPolicy",
    "PermissionDeniedError",
    "build_bigquery_tools",
    "build_spanner_tools",
    "is_mutating_sql",
    "register_gcp_database_tools",
)

_MUTATING_VERBS: frozenset[str] = frozenset(
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
_DDL_VERBS: frozenset[str] = frozenset({"CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT", "REVOKE"})
_MULTI_STATEMENT_MESSAGE: str = (
    "Multi-statement SQL is not allowed; submit one statement per call and pass values containing ';' "
    "as query parameters"
)
_LEADING_COMMENT_RE: re.Pattern[str] = re.compile(r"^\s*(?:--[^\n]*\n|/\*.*?\*/\s*)+", re.DOTALL)


def is_mutating_sql(sql: str) -> bool:
    """Return True when ``sql`` changes data, schema, or privileges, or runs procedural code.

    ``sql`` must hold exactly one statement. Any semicolon before the trailing terminator is
    rejected, including one inside a literal or comment, because GoogleSQL and Spangres quote
    literals differently and a dialect-specific parse could hide a second statement.

    Raises:
        PermissionDeniedError: When ``sql`` contains a semicolon before its trailing terminator.
    """
    body = (sql or "").rstrip().rstrip(";").rstrip()
    if ";" in body:
        raise PermissionDeniedError(_MULTI_STATEMENT_MESSAGE)
    return _extract_leading_verb(body) in _MUTATING_VERBS


class GCPDatabaseToolPolicy(FrozenBaseStruct, frozen=True, gc=False):
    """Governance policy for BigQuery and Cloud Spanner MCP and A2A tools."""

    maximum_bytes_billed: int = 10 * 1024 * 1024 * 1024
    """Hard upper bound on bytes billed per BigQuery query execution (default 10 GiB)."""
    enforce_dry_run_limit: bool = True
    """Whether `bigquery_query` executes a `dry_run` preflight before running SELECT queries."""
    read_only_staleness_seconds: float | None = None
    """Optional exact staleness in seconds for lock-free Spanner `ReadOnly` snapshots."""
    protocol_version: str = "2026-07-28"
    """Target Model Context Protocol specification version."""
    a2a_version: str = "1.0"
    """Target Agent-to-Agent protocol specification version."""


def register_gcp_database_tools(
    target: Any = None,
    *,
    bigquery_driver: AsyncBigQueryDriver | SyncBigQueryDriver | None = None,
    spanner_driver: AsyncSpannerDriver | SyncSpannerDriver | None = None,
    bigquery_config: BigQueryConfig | str | None = None,
    spanner_config: SpannerConfig | str | None = None,
    maximum_bytes_billed: int = 10 * 1024 * 1024 * 1024,
    enforce_dry_run_limit: bool = True,
    read_only_staleness_seconds: float | None = None,
    confirmation_gate: ConfirmationGate | None = None,
    registry: DatabaseRegistry | None = None,
) -> dict[str, Tool]:
    """Build and optionally register governed BigQuery and Cloud Spanner MCP/A2A tools."""
    gate = confirmation_gate if confirmation_gate is not None else ConfirmationGate()
    policy = GCPDatabaseToolPolicy(
        maximum_bytes_billed=maximum_bytes_billed,
        enforce_dry_run_limit=enforce_dry_run_limit,
        read_only_staleness_seconds=read_only_staleness_seconds,
    )

    tools: dict[str, Tool] = {}
    tools.update(
        ensure_module("vertebra.agent.tools.bigquery").build_bigquery_tools(
            driver=bigquery_driver,
            config=bigquery_config,
            policy=policy,
            confirmation_gate=gate,
            registry=registry,
        )
    )
    tools.update(
        ensure_module("vertebra.agent.tools.spanner").build_spanner_tools(
            driver=spanner_driver,
            config=spanner_config,
            policy=policy,
            confirmation_gate=gate,
            registry=registry,
        )
    )
    if target is not None:
        if isinstance(target, dict):
            target.update(tools)
        elif hasattr(target, "register_tool") and callable(target.register_tool):
            for t_obj in tools.values():
                target.register_tool(t_obj)
        elif hasattr(target, "add_tool") and callable(target.add_tool):
            for t_obj in tools.values():
                target.add_tool(t_obj)
        elif hasattr(target, "register") and callable(target.register):
            for t_obj in tools.values():
                target.register(t_obj)
        elif hasattr(target, "tools") and isinstance(target.tools, list):
            target.tools.extend(tools.values())

    return tools


def _extract_leading_verb(sql: str) -> str:
    """Extract the uppercase leading SQL keyword after stripping comments and parentheses."""
    cleaned = _LEADING_COMMENT_RE.sub("", sql or "").lstrip(" (;\t\r\n")
    if not cleaned:
        return ""
    first_token = cleaned.split(None, 1)[0].rstrip(";(,")
    return first_token.upper()


def _require_confirmation(
    gate: ConfirmationGate,
    tool_name: str,
    sql: str,
    confirmation_token: str | None,
) -> None:
    """Require an approved ticket before an engine tool executes a mutation."""
    verb = _extract_leading_verb(sql)
    if not gate.is_approved(confirmation_token):
        ticket = gate.request_confirmation(
            tool_name=tool_name,
            call_id=f"call-{tool_name}",
            arguments={"sql": sql},
            reason=f"Mutating {verb} statement requires human confirmation.",
        )
        msg = f"Mutating statement ({verb}) blocked by ConfirmationGate; approval required (ticket={ticket.token})"
        raise PermissionDeniedError(msg)


def __getattr__(name: str) -> Any:
    """Load engine builders without importing their adapters at package import time."""
    if name in {"build_bigquery_tools", "build_spanner_tools"}:
        engine = "bigquery" if name == "build_bigquery_tools" else "spanner"
        value = getattr(ensure_module(f"vertebra.agent.tools.{engine}"), name)
        globals()[name] = value
        return value
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
