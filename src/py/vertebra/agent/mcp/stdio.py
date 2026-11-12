"""Standard input/output (stdio) transport runner for MCP 2026-07-28 services.

This module provides run_stdio_server, allowing an MCPService to be hosted over standard I/O pipes.
"""

import sys
import uuid
from collections.abc import AsyncIterable, Awaitable, Callable

import anyio
import anyio.to_thread

from vertebra.agent.core.context import AgentContext
from vertebra.agent.mcp.service import MCPService
from vertebra.utils.serializers import encode_json_bytes


async def run_stdio_server(
    service: MCPService,
    *,
    stdin: AsyncIterable[bytes] | None = None,
    stdout: Callable[[bytes], Awaitable[None]] | None = None,
    max_message_bytes: int = 4_194_304,
    context: AgentContext | None = None,
) -> int:
    """Read newline-delimited JSON-RPC frames from stdin, dispatching to service."""
    session_context = context if context is not None else AgentContext(session_id=f"stdio-{uuid.uuid4().hex}")
    write_fn = stdout

    if write_fn is None:

        async def _default_stdout(data: bytes) -> None:
            def _write() -> None:
                sys.stdout.buffer.write(data)
                sys.stdout.buffer.write(b"\n")
                sys.stdout.buffer.flush()

            await anyio.to_thread.run_sync(_write)

        write_fn = _default_stdout

    if stdin is not None:
        async for raw_line in stdin:
            line = raw_line.strip()
            if not line:
                continue
            if len(line) > max_message_bytes:
                err_resp = encode_json_bytes(
                    {
                        "jsonrpc": "2.0",
                        "id": None,
                        "error": {
                            "code": -32600,
                            "message": f"Message exceeded max size of {max_message_bytes} bytes",
                        },
                    }
                )
                await write_fn(err_resp)
                continue

            resp = await service.handle_payload(line, session_context)
            if resp is not None:
                await write_fn(resp)
    else:

        def _read_lines() -> list[bytes]:
            lines: list[bytes] = []
            for line in sys.stdin.buffer:
                lines.append(line)
            return lines

        all_lines = await anyio.to_thread.run_sync(_read_lines)
        for raw_line in all_lines:
            line = raw_line.strip()
            if not line:
                continue
            if len(line) > max_message_bytes:
                err_resp = encode_json_bytes(
                    {
                        "jsonrpc": "2.0",
                        "id": None,
                        "error": {
                            "code": -32600,
                            "message": f"Message exceeded max size of {max_message_bytes} bytes",
                        },
                    }
                )
                await write_fn(err_resp)
                continue

            resp = await service.handle_payload(line, session_context)
            if resp is not None:
                await write_fn(resp)

    return 0


__all__ = [
    "run_stdio_server",
]
