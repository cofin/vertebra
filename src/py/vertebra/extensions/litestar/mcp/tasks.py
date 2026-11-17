"""Task configuration for litestar-mcp tasks using Vertebra ResponseCacheStore."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from vertebra.extensions.litestar import store as litestar_store
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from litestar.stores.base import Store
    from litestar_mcp import MCPTaskConfig

__all__ = ("create_shm_mcp_task_config",)


def create_shm_mcp_task_config(
    store: Store | None = None,
    *,
    default_ttl_ms: int = 300_000,
    max_ttl_ms: int = 3_600_000,
    poll_interval_ms: int = 1_000,
) -> MCPTaskConfig:
    """Create an MCPTaskConfig backed by Vertebra ResponseCacheStore."""
    litestar_mcp_mod = ensure_module("litestar_mcp", extra="mcp")
    resolved_store = store if store is not None else litestar_store.ResponseCacheStore()
    return cast(
        "MCPTaskConfig",
        litestar_mcp_mod.MCPTaskConfig(
            store=resolved_store,
            default_ttl_ms=default_ttl_ms,
            max_ttl_ms=max_ttl_ms,
            poll_interval_ms=poll_interval_ms,
        ),
    )
