"""Model Context Protocol helpers for Vertebra."""

from vertebra.mcp.decorators import mcp_tool
from vertebra.mcp.handler import MCPHandler, yield_progress
from vertebra.mcp.registry import ProcedureRegistry
from vertebra.mcp.resources import mcp_resource
from vertebra.mcp.schema import MCPSchemaGenerator


def get_registry() -> ProcedureRegistry:
    return ProcedureRegistry.get_instance()


__all__ = ("MCPHandler", "MCPSchemaGenerator", "get_registry", "mcp_resource", "mcp_tool", "yield_progress")
