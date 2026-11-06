"""Dialect-agnostic tenant predicates and session-setting configuration."""

from __future__ import annotations

from typing import Any

from vertebra.typing import FrozenBaseStruct
from vertebra.utils.text import validate_identifier

__all__ = ("TenantPredicate", "TenantScopeConfig")


class TenantScopeConfig(FrozenBaseStruct, frozen=True, gc=False, kw_only=True):
    """Tenant column, optional parameter cast, and adapter-owned session setting."""

    setting_name: str = "app.tenant_id"
    """Session setting or context attribute name bound by the owning adapter."""

    tenant_column: str = "tenant_id"
    """Tenant-scoped column compared against the bound tenant value."""

    parameter_cast: str | None = None
    """Optional SQL type identifier applied to the parameter instead of the column."""

    def __post_init__(self) -> None:
        validate_identifier(self.tenant_column, "tenant_column", allow_dots=False)
        if self.parameter_cast is not None:
            validate_identifier(self.parameter_cast, "parameter_cast", allow_dots=False)


class TenantPredicate(FrozenBaseStruct, frozen=True, gc=False, kw_only=True):
    """Render a tenant predicate with a bound value and an optional ANSI cast."""

    table_alias: str = ""
    """Optional table alias prefixing the tenant column."""

    tenant_value: Any
    """Tenant value supplied separately through the query's bind parameters."""

    config: TenantScopeConfig = TenantScopeConfig()
    """Tenant column, session setting name, and optional parameter cast."""

    def __post_init__(self) -> None:
        if self.table_alias:
            validate_identifier(self.table_alias, "table_alias", allow_dots=False)

    def column_reference(self) -> str:
        """Return the validated column reference, including its optional alias."""
        return f"{self.table_alias}.{self.config.tenant_column}" if self.table_alias else self.config.tenant_column

    def to_sql_fragment(self, placeholder: str = "?") -> str:
        """Render equality with an optional cast on the supplied bind placeholder."""
        parameter = (
            f"CAST({placeholder} AS {self.config.parameter_cast})" if self.config.parameter_cast else placeholder
        )
        return f"{self.column_reference()} = {parameter}"
