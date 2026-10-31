"""Agent tool JSON Schema derivation, JSON-RPC routing, and concurrent execution example."""

import asyncio
from typing import Any

from vertebra.agent.core import JSONRPCRouter, ToolCall, execute_tool_calls, tool
from vertebra.utils.serializers import decode_json, encode_json_bytes

__all__ = (
    "add_numbers",
    "build_router",
    "main",
    "run_tool_workflow",
)


# --8<-- [start:example]
@tool(description="Add two integers and return their sum.")
def add_numbers(a: int, b: int) -> dict[str, int]:
    """Compute the sum of two integers."""
    return {"sum": a + b}


def build_router() -> JSONRPCRouter:
    """Create a JSON-RPC 2.0 router exposing tools/list and tools/call."""
    router = JSONRPCRouter()
    tools = {add_numbers.name: add_numbers}

    async def handle_list(_params: Any = None, _ctx: Any = None) -> dict[str, Any]:
        return {
            "tools": [
                {
                    "name": item.name,
                    "description": item.description,
                    "inputSchema": item.input_schema,
                }
                for item in tools.values()
            ]
        }

    router.register("tools/list", handle_list)
    return router


async def run_tool_workflow() -> None:
    """Validate JSON-RPC tool discovery and execute a typed ToolCall."""
    assert add_numbers.input_schema["required"] == ["a", "b"]

    router = build_router()
    raw_req = encode_json_bytes({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    raw_res = await router.dispatch_payload(raw_req)
    assert raw_res is not None
    decoded = decode_json(raw_res)
    assert decoded["result"]["tools"][0]["name"] == "add_numbers"

    results = await execute_tool_calls(
        {add_numbers.name: add_numbers},
        [ToolCall(id="call-1", name="add_numbers", arguments={"a": 19, "b": 23})],
    )
    assert len(results) == 1
    assert not results[0].is_error
    assert results[0].output == {"sum": 42}


def main() -> None:
    """Execute the agent tool schema and JSON-RPC router workflow."""
    asyncio.run(run_tool_workflow())


# --8<-- [end:example]


if __name__ == "__main__":
    main()
