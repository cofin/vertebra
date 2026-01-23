import asyncio
import logging

import msgspec

from vertebra.mcp.registry import ProcedureRegistry
from vertebra.serializers import decode_msgpack, encode_msgpack

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
            req = decode_msgpack(payload, type_hint=ToolCall)
        except msgspec.DecodeError as e:
            return encode_msgpack(ToolResult(error=f"Invalid request: {e}"))

        if req.type != "tool_call":
            return encode_msgpack(ToolResult(error=f"Unknown message type: {req.type}"))

        tool = self.registry.get_tool(req.name)
        if not tool:
            return encode_msgpack(ToolResult(error=f"Tool not found: {req.name}"))

        try:
            # Handle async vs sync
            args = req.args or {}
            if asyncio.iscoroutinefunction(tool.handler):
                result = await tool.handler(**args)
            else:
                result = tool.handler(**args)

            return encode_msgpack(ToolResult(result=result))
        except Exception as e:
            logger.exception(f"Tool execution failed: {req.name}")
            return encode_msgpack(ToolResult(error=str(e)))