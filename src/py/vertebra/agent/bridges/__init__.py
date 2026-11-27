"""Bidirectional bridges between Model Context Protocol (MCP) and Agent-to-Agent (A2A) 1.0.

This package provides translation adapters allowing MCP tools/skills to be exposed
as A2A agents and remote A2A agents/skills to be consumed as local MCP tools.
"""

from vertebra.agent.bridges.a2a import A2AAgentBridge, mcp_service_to_a2a
from vertebra.agent.bridges.mcp import MCPToolBridge, a2a_card_to_mcp_tools

__all__ = [
    "A2AAgentBridge",
    "MCPToolBridge",
    "a2a_card_to_mcp_tools",
    "mcp_service_to_a2a",
]
