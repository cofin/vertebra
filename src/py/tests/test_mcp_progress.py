import pytest
import msgspec
from vertebra.mcp.decorators import mcp_tool
from vertebra.mcp.handler import MCPHandler, yield_progress
from vertebra.mcp.registry import ProcedureRegistry

@pytest.mark.asyncio
async def test_progress_notification():
    registry = ProcedureRegistry.get_instance()
    registry.clear()

    @mcp_tool()
    async def long_task():
        yield_progress(50, 100)
        return "done"

    side_channel_messages = []
    def mock_side_channel(msg):
        side_channel_messages.append(msgspec.msgpack.decode(msg))

    handler = MCPHandler(send_side_channel=mock_side_channel)
    
    payload = msgspec.msgpack.encode({
        "type": "tool_call",
        "name": "long_task",
        "progress_token": "token-123"
    })
    
    await handler.handle_message(payload)
    
    assert len(side_channel_messages) == 1
    msg = side_channel_messages[0]
    assert msg["type"] == "progress"
    assert msg["token"] == "token-123"
    assert msg["current"] == 50
    assert msg["total"] == 100
