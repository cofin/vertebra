from typing import Never

import msgspec

from vertebra.mcp.decorators import mcp_tool
from vertebra.mcp.handler import MCPHandler
from vertebra.mcp.registry import ProcedureRegistry


async def test_mcp_handler_invocation() -> None:
    registry = ProcedureRegistry.get_instance()
    registry.clear()

    @mcp_tool()
    def add(a: int, b: int) -> int:
        return a + b

    handler = MCPHandler()

    # Simulate IPC payload from TS
    # Note: msgspec structs serialize to array/tuples by default unless array_like=False?
    # No, msgspec.Struct serializes to array by default if using msgpack?
    # Wait, msgspec defaults to array-like for Structs in msgpack?
    # Let's check msgspec behavior.
    # "By default, structs are serialized as MessagePack arrays."
    # But ToolCall definition in handler.py doesn't specify array_like=True/False.
    # If TS sends a map (object), decoding into Struct might fail if Struct expects array.
    # TS `encode({ type: "..." })` sends a map.
    # Python `msgspec.Struct` defaults to `array_like=True` (tuples) for performance?
    # No, msgspec default is `array_like=False` (maps) for JSON, but what about msgpack?
    # "By default msgspec structures are serialized as arrays." -> Documentation.
    # If TS sends map, Python needs to expect map.

    # Let's verify defaults.
    # I should explicitly set `array_like=False` (map) on structs if I want to interop with generic JS maps easily.
    # Or TS needs to send arrays.

    # I'll update handler.py to use `array_like=False` just to be safe/compatible with JS objects.

    payload = msgspec.msgpack.encode({"type": "tool_call", "name": "add", "args": {"a": 5, "b": 3}})

    response_bytes = await handler.handle_message(payload)

    # Result comes back as map-like msgpack?
    # Let's decode as dict first to inspect
    result_dict = msgspec.msgpack.decode(response_bytes)
    assert result_dict["error"] is None
    assert result_dict["result"] == 8


async def test_mcp_handler_error() -> None:
    registry = ProcedureRegistry.get_instance()
    registry.clear()

    @mcp_tool()
    def fail() -> Never:
        msg = "Oops"
        raise ValueError(msg)

    handler = MCPHandler()
    payload = msgspec.msgpack.encode({"type": "tool_call", "name": "fail"})

    response_bytes = await handler.handle_message(payload)

    result_dict = msgspec.msgpack.decode(response_bytes)
    assert result_dict["error"] == "Oops"
    assert result_dict["result"] is None
