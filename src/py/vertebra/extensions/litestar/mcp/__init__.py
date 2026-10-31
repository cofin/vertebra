"""Vertebra MCP extension for litestar-mcp."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.extensions.litestar.mcp.accelerator import JSONRPCFrameAccelerator
    from vertebra.extensions.litestar.mcp.card import WellKnownAgentCardOffload
    from vertebra.extensions.litestar.mcp.plugin import create_mcp_plugin
    from vertebra.extensions.litestar.mcp.skills import ObjectStoreSkillSource
    from vertebra.extensions.litestar.mcp.tasks import create_shm_mcp_task_config

__all__ = (
    "JSONRPCFrameAccelerator",
    "ObjectStoreSkillSource",
    "WellKnownAgentCardOffload",
    "create_mcp_plugin",
    "create_shm_mcp_task_config",
)

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "JSONRPCFrameAccelerator": ("vertebra.extensions.litestar.mcp.accelerator", "JSONRPCFrameAccelerator"),
    "ObjectStoreSkillSource": ("vertebra.extensions.litestar.mcp.skills", "ObjectStoreSkillSource"),
    "WellKnownAgentCardOffload": ("vertebra.extensions.litestar.mcp.card", "WellKnownAgentCardOffload"),
    "create_mcp_plugin": ("vertebra.extensions.litestar.mcp.plugin", "create_mcp_plugin"),
    "create_shm_mcp_task_config": ("vertebra.extensions.litestar.mcp.tasks", "create_shm_mcp_task_config"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
