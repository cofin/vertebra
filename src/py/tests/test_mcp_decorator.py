import pytest
import asyncio
from vertebra.mcp.decorators import mcp_tool
from vertebra.mcp.registry import ProcedureRegistry

@pytest.fixture
def registry():
    return ProcedureRegistry()

# Hack: Reset singleton for testing if necessary, or pass registry to decorator
# For now, assuming global registry, but let's see how implementation goes.
# Ideally we can inspect the global registry.

def test_mcp_tool_registration():
    registry = ProcedureRegistry.get_instance()
    registry.clear()  # Ensure clean state

    @mcp_tool()
    def simple_tool(x: int) -> int:
        """A simple tool."""
        return x * 2

    tool = registry.get_tool("simple_tool")
    assert tool is not None
    assert tool.name == "simple_tool"
    assert tool.description == "A simple tool."
    assert tool.handler == simple_tool

def test_mcp_tool_custom_name():
    registry = ProcedureRegistry.get_instance()
    registry.clear()

    @mcp_tool(name="custom_name", description="Custom desc")
    def my_tool():
        pass

    tool = registry.get_tool("custom_name")
    assert tool is not None
    assert tool.name == "custom_name"
    assert tool.description == "Custom desc"

@pytest.mark.asyncio
async def test_async_tool_execution():
    registry = ProcedureRegistry.get_instance()
    registry.clear()

    @mcp_tool()
    async def async_echo(msg: str) -> str:
        await asyncio.sleep(0.01)
        return msg

    tool = registry.get_tool("async_echo")
    assert tool is not None
    result = await tool.handler("hello")
    assert result == "hello"

def test_metadata_extraction():
    registry = ProcedureRegistry.get_instance()
    registry.clear()

    @mcp_tool()
    def complex_tool(a: int, b: str = "default") -> bool:
        pass
    
    tool = registry.get_tool("complex_tool")
    # We expect the TypeExtractor to populate params/return_type in the future
    # For now just checking registration matches
    assert tool.name == "complex_tool"
