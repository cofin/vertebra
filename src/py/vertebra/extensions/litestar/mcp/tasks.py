"""Task configuration for litestar-mcp tasks using Vertebra ResponseCacheStore."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.extensions.litestar.store import ResponseCacheStore
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module

try:
    from litestar_mcp import MCPTaskConfig
except ImportError as exc:
    raise MissingDependencyError(
        "litestar-mcp",
        install_package="litestar-mcp",
        extra="mcp",
    ) from exc

if TYPE_CHECKING:
    from litestar.stores.base import Store


def create_shm_mcp_task_config(
    store: Store | None = None,
    *,
    default_ttl_ms: int = 300_000,
    max_ttl_ms: int = 3_600_000,
    poll_interval_ms: int = 1_000,
) -> MCPTaskConfig:
    """Create an MCPTaskConfig backed by Vertebra ResponseCacheStore."""
    ensure_module("litestar_mcp", extra="mcp")
    resolved_store = store if store is not None else ResponseCacheStore()
    return MCPTaskConfig(
        store=resolved_store,
        default_ttl_ms=default_ttl_ms,
        max_ttl_ms=max_ttl_ms,
        poll_interval_ms=poll_interval_ms,
    )
