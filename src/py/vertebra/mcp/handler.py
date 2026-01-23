import asyncio
import logging
from collections.abc import Callable
from contextvars import ContextVar

import msgspec

from vertebra.mcp.registry import ProcedureRegistry
from vertebra.serializers import decode_msgpack, encode_msgpack

logger = logging.getLogger(__name__)

# Context variable to store the current progress callback
_progress_callback: ContextVar[Callable | None] = ContextVar("_progress_callback", default=None)


def yield_progress(current: float, total: float | None = None) -> None:
    """Yield progress information for the current tool execution.

    Args:
        current: Current progress value.
        total: Total progress value (optional).
    """
    callback = _progress_callback.get()
    if callback:
        callback(current, total)


class ToolCall(msgspec.Struct, array_like=False):
    type: str
    name: str
    args: dict | None = None
    progress_token: str | int | None = None


class ToolResult(msgspec.Struct, array_like=False):
    result: object = None
    error: str | None = None


class ProgressNotification(msgspec.Struct, array_like=False):
    type: str = "progress"
    token: str | int | None = None
    current: float = 0
    total: float | None = None


class MCPHandler:
    def __init__(self, send_side_channel: Callable | None = None) -> None:
        """Args:
        send_side_channel: Callback to send unsolicited messages (like progress) back to TS.
        """
        self.registry = ProcedureRegistry.get_instance()
        self.send_side_channel = send_side_channel

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

        # Setup progress callback if token and side channel are available
        token = req.progress_token

        def report_progress(current: float, total: float | None) -> None:
            if self.send_side_channel and token is not None:
                notification = ProgressNotification(token=token, current=current, total=total)
                self.send_side_channel(encode_msgpack(notification))

        token_reset = _progress_callback.set(report_progress)

        try:
            args = req.args or {}
            if asyncio.iscoroutinefunction(tool.handler):
                result = await tool.handler(**args)
            else:
                result = tool.handler(**args)

            return encode_msgpack(ToolResult(result=result))
        except Exception as e:
            logger.exception("Tool execution failed: %s", req.name)
            return encode_msgpack(ToolResult(error=str(e)))
        finally:
            _progress_callback.reset(token_reset)
