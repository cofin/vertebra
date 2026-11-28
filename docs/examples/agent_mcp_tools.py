"""Serve a typed stock lookup tool through the native MCP transport."""

__all__ = ("app", "check_stock")

# --8<-- [start:example]
from vertebra import App
from vertebra.agent import AgentPlugin, AgentPluginConfig
from vertebra.agent.core import tool
from vertebra.agent.mcp import MCPConfig

STOCK = {"VTB-100": 5, "VTB-200": 12}


@tool(description="Look up the number of units available for a SKU.")
def check_stock(sku: str) -> dict[str, str | int]:
    """Return the stock count for an inventory item."""
    return {"sku": sku, "available": STOCK.get(sku, 0)}


plugin = AgentPlugin(AgentPluginConfig(mcp=MCPConfig(server_name="inventory")))
plugin.mcp_registry.register_tool(check_stock)
app = App()
plugin.mount_native(app)

# --8<-- [end:example]
