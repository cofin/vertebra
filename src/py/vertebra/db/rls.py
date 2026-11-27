"""Dialect-agnostic tenant predicates and session-setting configuration."""

from __future__ import annotations

from contextlib import suppress
from inspect import isawaitable
from typing import TYPE_CHECKING, Any

from vertebra.typing import FrozenBaseStruct
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from types import TracebackType

__all__ = (
    "AsyncTenantConnectionScope",
    "SyncTenantConnectionScope",
    "TenantPredicate",
    "TenantScopeConfig",
)


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


_DEFAULT_TENANT_SCOPE = TenantScopeConfig()


class SyncTenantConnectionScope:
    """Synchronous tenant connection scope enforcing mandatory context reset or physical session eviction."""

    __slots__ = ("_config", "_driver", "_tenant_value")

    def __init__(
        self,
        driver: Any,
        tenant_value: str,
        config: TenantScopeConfig = _DEFAULT_TENANT_SCOPE,
    ) -> None:
        self._driver = driver
        self._tenant_value = tenant_value
        self._config = config

    def __enter__(self) -> Any:
        self._driver.enter_tenant_scope(self._tenant_value, self._config)
        return self._driver

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        _ = (exc_type, exc_tb)
        try:
            self._driver.exit_tenant_scope(self._config)
        except Exception as reset_exc:
            evict_fn = getattr(self._driver, "evict_connection", None)
            with suppress(Exception):
                if callable(evict_fn):
                    evict_fn()
                else:
                    close_fn = getattr(self._driver, "close", None)
                    if callable(close_fn):
                        close_fn()
            if exc_val is not None:
                raise reset_exc from exc_val
            raise


class AsyncTenantConnectionScope:
    """Asynchronous tenant connection scope enforcing mandatory context reset or physical session eviction."""

    __slots__ = ("_config", "_driver", "_tenant_value")

    def __init__(
        self,
        driver: Any,
        tenant_value: str,
        config: TenantScopeConfig = _DEFAULT_TENANT_SCOPE,
    ) -> None:
        self._driver = driver
        self._tenant_value = tenant_value
        self._config = config

    async def __aenter__(self) -> Any:
        result = self._driver.enter_tenant_scope(self._tenant_value, self._config)
        if isawaitable(result):
            await result
        return self._driver

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        _ = (exc_type, exc_tb)
        try:
            result = self._driver.exit_tenant_scope(self._config)
            if isawaitable(result):
                await result
        except Exception as reset_exc:
            evict_fn = getattr(self._driver, "evict_connection", None)
            with suppress(Exception):
                if callable(evict_fn):
                    evicted = evict_fn()
                    if isawaitable(evicted):
                        await evicted
                else:
                    close_fn = getattr(self._driver, "close", None)
                    if callable(close_fn):
                        closed = close_fn()
                        if isawaitable(closed):
                            await closed
            if exc_val is not None:
                raise reset_exc from exc_val
            raise
