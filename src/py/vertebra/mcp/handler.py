import msgspec
import asyncio
import logging
from vertebra.mcp.registry import ProcedureRegistry

logger = logging.getLogger(__name__)

class ToolCall(msgspec.Struct, array_like=False):
    type: str
    name: str
    args: dict | None = None

class ToolResult(msgspec.Struct, array_like=False):
    result: object = None
    error: str | None = None

class MCPHandler:
    def __init__(self):
        self.registry = ProcedureRegistry.get_instance()

    async def handle_message(self, payload: bytes) -> bytes:
        try:
            req = msgspec.msgpack.decode(payload, type=ToolCall)
        except msgspec.DecodeError as e:
            # Try to decode as map to return structured error? 
            # Or just return generic error.
            return msgspec.msgpack.encode(ToolResult(error=f"Invalid request: {e}"))

        if req.type != "tool_call":
            return msgspec.msgpack.encode(ToolResult(error=f"Unknown message type: {req.type}"))

        tool = self.registry.get_tool(req.name)
        if not tool:
            return msgspec.msgpack.encode(ToolResult(error=f"Tool not found: {req.name}"))

        try:
            # Handle async vs sync
            args = req.args or {}
            if asyncio.iscoroutinefunction(tool.handler):
                result = await tool.handler(**args)
            else:
                result = tool.handler(**args)
            
            return msgspec.msgpack.encode(ToolResult(result=result))
        except Exception as e:
            logger.exception(f"Tool execution failed: {req.name}")
            return msgspec.msgpack.encode(ToolResult(error=str(e)))