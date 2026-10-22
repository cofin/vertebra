"""Planner-unfenced PostgreSQL tenant isolation and session GUC defense-in-depth."""

from __future__ import annotations

import re
from typing import Any

import msgspec

__all__ = (
    "TenantPredicate",
    "TenantScopeConfig",
)

_IDENTIFIER_PATTERN: re.Pattern[str] = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")


def _validate_sql_identifier(identifier: str, field_name: str = "identifier") -> str:
    """Validate that identifier matches safe SQL naming syntax."""
    if not _IDENTIFIER_PATTERN.match(identifier):
        msg = f"Invalid SQL identifier for {field_name}: {identifier!r}"
        raise ValueError(msg)
    return identifier


class TenantScopeConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for dual-layer PostgreSQL tenant isolation (planner predicate + RLS GUC)."""

    guc_name: str = "app.tenant_id"
    tenant_column: str = "tenant_id"
    parameter_cast: str | None = "uuid"

    def __post_init__(self) -> None:
        """Validate configured SQL identifiers."""
        _validate_sql_identifier(self.tenant_column, "tenant_column")
        if self.parameter_cast is not None:
            _validate_sql_identifier(self.parameter_cast, "parameter_cast")


class TenantPredicate(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Positional tenant predicate allowing PostgreSQL index scans without ``security_barrier`` fencing."""

    table_alias: str
    tenant_value: Any
    config: TenantScopeConfig = TenantScopeConfig()

    def __post_init__(self) -> None:
        """Validate table alias identifier when supplied."""
        if self.table_alias:
            _validate_sql_identifier(self.table_alias, "table_alias")

    def to_sql_fragment(self, param_index: int = 1) -> str:
        """Render ``<alias>.<tenant_column> = $<N>[::<cast>]`` with cast on the parameter, never the column."""
        col_ref = f"{self.table_alias}.{self.config.tenant_column}" if self.table_alias else self.config.tenant_column
        cast_suffix = f"::{self.config.parameter_cast}" if self.config.parameter_cast else ""
        return f"{col_ref} = ${param_index}{cast_suffix}"

    @staticmethod
    def build_session_guc_sql(
        tenant_id: str,
        config: TenantScopeConfig | None = None,
        *,
        is_local: bool = True,
    ) -> tuple[str, tuple[str, str, bool]]:
        """Build a parameterized ``set_config($1, $2, $3)`` statement for transaction-local RLS enforcement."""
        cfg = config or TenantScopeConfig()
        return "SELECT set_config($1, $2, $3)", (cfg.guc_name, tenant_id, is_local)
