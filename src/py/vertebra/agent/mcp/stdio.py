"""Standard input/output (stdio) transport runner for MCP services.

This module provides run_stdio_server, dispatching newline-delimited JSON-RPC frames
incrementally and concurrently over standard I/O pipes with request cancellation and
legacy session notification relay.
"""

from __future__ import annotations

import contextlib
import sys
import uuid
from collections.abc import AsyncIterable, Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any, cast

import anyio
import anyio.lowlevel
import anyio.to_thread

from vertebra.agent.core.context import AgentContext
from vertebra.utils.serializers import decode_json, encode_json_bytes

if TYPE_CHECKING:
    from vertebra.agent.mcp.service import MCPCore

__all__ = [
    "run_stdio_server",
]


async def run_stdio_server(
    service: MCPCore,
    *,
    stdin: AsyncIterable[bytes] | None = None,
    stdout: Callable[[bytes], Awaitable[None]] | None = None,
    max_message_bytes: int = 4_194_304,
    context: AgentContext | None = None,
) -> int:
    """Read newline-delimited JSON-RPC frames incrementally and dispatch concurrently."""
    session_context = context if context is not None else AgentContext(session_id=f"stdio-{uuid.uuid4().hex}")
    write_fn = stdout if stdout is not None else _default_stdout_writer
    write_lock = anyio.Lock()
    active_scopes: dict[tuple[type[str] | type[int], str | int], anyio.CancelScope] = {}
    relay_started = False
    relay_scope: anyio.CancelScope | None = None
    created_session_id: str | None = None
    created_owner_id = service._caller_owner(session_context)

    async def _write_locked(payload: bytes) -> None:
        async with write_lock:
            await write_fn(payload)

    async def _relay_session_notifications(session_id: str, owner_id: str | None) -> None:
        nonlocal relay_scope
        stream = service.stream_broker.open_stream(
            session_id,
            owner_id=owner_id,
            heartbeat_seconds=service.config.sse_heartbeat_seconds,
            max_streams_per_owner=service.config.max_streams_per_owner,
        )
        with anyio.CancelScope() as scope:
            relay_scope = scope
            try:
                async for chunk in stream:
                    for frame_bytes in _extract_sse_data_lines(chunk):
                        if frame_bytes.strip():
                            await _write_locked(frame_bytes)
            finally:
                with contextlib.suppress(BaseException):
                    aclose = getattr(stream, "aclose", None)
                    if callable(aclose):
                        await cast("Awaitable[Any]", aclose())

    async with anyio.create_task_group() as session_tg:

        async def _dispatch_frame(line: bytes, decoded: Any) -> None:
            nonlocal relay_started, created_session_id, created_owner_id
            req_key = _extract_request_key(decoded)
            resp: bytes | None = None
            with anyio.CancelScope() as scope:
                if req_key is not None:
                    active_scopes[req_key] = scope
                try:
                    resp = await service.handle_payload(line, session_context)
                finally:
                    if req_key is not None and active_scopes.get(req_key) is scope:
                        active_scopes.pop(req_key, None)

            if scope.cancelled_caught:
                return

            new_sid = session_context.metadata.get("mcp_created_session_id")
            if isinstance(new_sid, str) and new_sid and not relay_started:
                relay_started = True
                created_session_id = new_sid
                created_owner_id = service._caller_owner(session_context)
                session_tg.start_soon(_relay_session_notifications, created_session_id, created_owner_id)

            if resp is not None:
                await _write_locked(resp)

        async def _handle_line(raw_line: bytes) -> None:
            line = raw_line.strip()
            if not line:
                return
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
                await _write_locked(err_resp)
                return

            decoded: Any = None
            with contextlib.suppress(Exception):
                decoded = decode_json(line)

            cancelled_key = _extract_cancelled_key(decoded)
            if cancelled_key is not None:
                target_scope = active_scopes.get(cancelled_key)
                if target_scope is not None:
                    target_scope.cancel()
                return

            request_tg.start_soon(_dispatch_frame, line, decoded)

        async with anyio.create_task_group() as request_tg:
            if stdin is not None:
                async for raw_line in stdin:
                    await _handle_line(raw_line)
            else:
                while True:
                    raw_line = await anyio.to_thread.run_sync(sys.stdin.buffer.readline)
                    if not raw_line:
                        break
                    await _handle_line(raw_line)

        if relay_started and created_session_id is not None:
            await anyio.lowlevel.checkpoint()
            if relay_scope is not None:
                relay_scope.cancel()
            service.stream_broker.close_session(created_session_id)
            await service.session_store.delete(created_session_id, created_owner_id)

    return 0


def _extract_request_key(decoded: Any) -> tuple[type[str] | type[int], str | int] | None:
    if isinstance(decoded, Mapping):
        req_id = decoded.get("id")
        if isinstance(req_id, (str, int)) and not isinstance(req_id, bool):
            return (type(req_id), req_id)
    return None


def _extract_cancelled_key(decoded: Any) -> tuple[type[str] | type[int], str | int] | None:
    if not isinstance(decoded, Mapping) or decoded.get("method") != "notifications/cancelled":
        return None
    params = decoded.get("params")
    if not isinstance(params, Mapping):
        return None
    req_id = params.get("requestId")
    if isinstance(req_id, (str, int)) and not isinstance(req_id, bool):
        return (type(req_id), req_id)
    return None


def _extract_sse_data_lines(chunk: bytes) -> list[bytes]:
    payloads: list[bytes] = []
    data_buf: list[str] = []
    for raw_line in chunk.decode("utf-8", errors="replace").splitlines():
        line = raw_line.rstrip("\r")
        if not line:
            if data_buf:
                payloads.append("\n".join(data_buf).encode("utf-8"))
                data_buf.clear()
            continue
        if line.startswith(":"):
            continue
        if line.startswith("data:"):
            data_buf.append(line[5:].lstrip())
    if data_buf:
        payloads.append("\n".join(data_buf).encode("utf-8"))
    return payloads


async def _default_stdout_writer(data: bytes) -> None:
    def _write() -> None:
        sys.stdout.buffer.write(data)
        sys.stdout.buffer.write(b"\n")
        sys.stdout.buffer.flush()

    await anyio.to_thread.run_sync(_write)
