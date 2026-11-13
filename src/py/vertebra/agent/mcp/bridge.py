"""Standard I/O to HTTP bridge for Model Context Protocol 2026-07-28 and legacy services.

This module provides run_stdio_http_bridge, dual-mode version negotiation, background GET
SSE relay with Last-Event-ID reconnection, EOF DELETE session teardown, and OIDC ID token
authentication backed by AsyncGcpIdTokenCache.
"""

from __future__ import annotations

import contextlib
import inspect
import os
import sys
import urllib.error
import urllib.request
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable, Mapping
from typing import Any, Protocol, TextIO, cast, runtime_checkable

import anyio
import anyio.to_thread
import msgspec

from vertebra._vertebra import AsyncGcpIdTokenCache
from vertebra.agent.mcp.client import mcp_request_headers
from vertebra.agent.mcp.models import LEGACY_PROTOCOL_VERSIONS, MCP_PROTOCOL_VERSION
from vertebra.utils.serializers import decode_json, encode_json_bytes

__all__ = [
    "BridgeResponse",
    "BridgeTokenSource",
    "BridgeTransport",
    "OidcTokenSource",
    "StaticTokenSource",
    "UrllibBridgeTransport",
    "run_stdio_http_bridge",
]

_MAX_SSE_RECONNECTS = 5
_PRE_NEGOTIATION_HEADERLESS_METHODS = frozenset({"ping", "notifications/initialized"})


class BridgeResponse(msgspec.Struct, frozen=True, gc=False):
    """Normalized HTTP response returned by a BridgeTransport."""

    status: int
    headers: dict[str, str]
    body: bytes = b""
    stream: AsyncIterable[bytes] | Iterable[bytes] | None = None


@runtime_checkable
class BridgeTokenSource(Protocol):
    """Protocol providing bearer or OIDC tokens with invalidation support."""

    def get_token(self) -> Awaitable[str | None] | str | None:
        """Return a valid bearer token for outbound MCP HTTP requests."""
        ...

    def invalidate(self) -> Awaitable[None] | None:
        """Invalidate the most recently issued token after an HTTP 401 rejection."""
        ...


class StaticTokenSource:
    """Static bearer token source supporting BridgeTokenSource."""

    def __init__(self, token: str | None = None) -> None:
        """Store a static bearer token."""
        self.token = token

    def get_token(self) -> str | None:
        """Return the configured static token."""
        return self.token

    def invalidate(self) -> None:
        """Keep static tokens immutable on HTTP 401."""
        return None


class OidcTokenSource:
    """Audience-bound OIDC identity token source backed by AsyncGcpIdTokenCache."""

    def __init__(
        self,
        audience: str,
        *,
        cache: AsyncGcpIdTokenCache | None = None,
    ) -> None:
        """Initialize the OIDC token source and validate audience eagerly."""
        self.audience = audience
        self.cache = cache if cache is not None else AsyncGcpIdTokenCache()
        self.cache.needs_refresh(audience)
        self._last_generation: int | None = None

    async def get_token(self) -> str:
        """Fetch or reuse a cached OIDC ID token for the configured audience."""
        snapshot = await self.cache.get_token(self.audience)
        self._last_generation = int(snapshot.generation)
        return str(snapshot.id_token)

    def invalidate(self) -> None:
        """Invalidate the failed token generation so the next call refreshes."""
        if self._last_generation is not None:
            self.cache.invalidate(self.audience, self._last_generation)


class _CallableTokenSource:
    """Adapter wrapping a legacy zero-arg token_provider callable."""

    def __init__(self, provider: Callable[[], Awaitable[str | None] | str | None]) -> None:
        self._provider = provider

    async def get_token(self) -> str | None:
        resolved = self._provider()
        if inspect.isawaitable(resolved):
            resolved = await resolved
        return str(resolved) if resolved else None

    def invalidate(self) -> None:
        return None


@runtime_checkable
class BridgeTransport(Protocol):
    """HTTP transport abstraction supporting POST, streaming GET SSE, and DELETE."""

    def request(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        body: bytes,
        timeout_s: float,
    ) -> BridgeResponse | Awaitable[BridgeResponse]:
        """Execute an HTTP request and return a normalized BridgeResponse."""
        ...


class UrllibBridgeTransport:
    """Default urllib-backed BridgeTransport with incremental SSE line streaming."""

    def request(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        body: bytes,
        timeout_s: float,
    ) -> BridgeResponse:
        """Execute an HTTP request synchronously or open an SSE byte stream."""
        upper_method = method.upper()
        accept_header = next(
            (v for k, v in headers.items() if k.lower() == "accept"),
            "",
        )
        req_data = body if upper_method == "POST" else None
        req = urllib.request.Request(url, data=req_data, headers=dict(headers), method=upper_method)
        try:
            resp = urllib.request.urlopen(req, timeout=timeout_s)
        except urllib.error.HTTPError as exc:
            resp_headers = dict(exc.headers)
            resp_bytes = exc.read()
            exc.close()
            return BridgeResponse(status=int(exc.code), headers=resp_headers, body=resp_bytes)

        resp_headers = dict(resp.headers)
        content_type = _header_ci(resp_headers, "content-type") or ""
        if (
            upper_method == "GET"
            and "text/event-stream" in accept_header.lower()
            and "text/event-stream" in content_type
        ):
            return BridgeResponse(
                status=int(resp.status),
                headers=resp_headers,
                body=b"",
                stream=_iter_urllib_stream(resp),
            )
        try:
            resp_bytes = resp.read()
        finally:
            resp.close()
        return BridgeResponse(status=int(resp.status), headers=resp_headers, body=resp_bytes)


async def _iter_urllib_stream(resp: Any) -> AsyncIterator[bytes]:
    try:
        while True:
            chunk = await anyio.to_thread.run_sync(resp.readline)
            if not chunk:
                break
            yield bytes(chunk)
    finally:
        with contextlib.suppress(Exception):
            resp.close()


def _header_ci(headers: Mapping[str, str], target: str) -> str | None:
    lower_target = target.lower()
    for key, value in headers.items():
        if key.lower() == lower_target:
            return value
    return None


def _parse_sse_events(data: bytes) -> list[str]:
    """Parse text/event-stream bytes and extract all data payload blocks."""
    lines = data.decode("utf-8", errors="replace").splitlines()
    events: list[str] = []
    current_data: list[str] = []

    for line in lines:
        stripped = line.rstrip("\r")
        if not stripped:
            if current_data:
                events.append("\n".join(current_data))
                current_data = []
            continue
        if stripped.startswith(":"):
            continue
        if stripped.startswith("data:"):
            payload = stripped[5:].lstrip()
            current_data.append(payload)

    if current_data:
        events.append("\n".join(current_data))
    return events


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _normalize_legacy_response(raw: Any) -> BridgeResponse:
    if isinstance(raw, BridgeResponse):
        return raw
    if isinstance(raw, tuple):
        status, headers, body = raw
        return BridgeResponse(status=int(status), headers=dict(headers), body=bytes(body))
    return BridgeResponse(status=200, headers={}, body=bytes(raw))


def _decode_frame_metadata(payload: bytes) -> tuple[str | None, Any]:
    try:
        decoded = decode_json(payload)
    except Exception:
        return None, None
    if isinstance(decoded, Mapping):
        method = decoded.get("method")
        req_id = decoded.get("id")
        return (method if isinstance(method, str) else None), req_id
    return None, None


def _extract_protocol_version(resp: BridgeResponse) -> str | None:
    header_ver = _header_ci(resp.headers, "mcp-protocol-version")
    if header_ver:
        return header_ver
    if resp.body:
        try:
            decoded = decode_json(resp.body)
        except Exception:
            decoded = None
        if isinstance(decoded, Mapping):
            result = decoded.get("result")
            if isinstance(result, Mapping):
                proto = result.get("protocolVersion")
                if isinstance(proto, str) and proto:
                    return proto
    return None


def _build_jsonrpc_error_frame(req_id: Any, code: int, message: str) -> bytes:
    return encode_json_bytes(
        {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": code, "message": message},
        }
    )


async def _consume_sse_stream(
    resp: BridgeResponse,
    *,
    last_event_id: str | None,
    emit_payload: Callable[[bytes], Awaitable[None]],
) -> tuple[str | None, bool]:
    current_id = last_event_id
    received_any = False
    event_data: list[str] = []
    event_id_pending: str | None = None

    async def _flush_event() -> None:
        nonlocal current_id, event_id_pending, received_any
        if event_id_pending is not None:
            current_id = event_id_pending
            event_id_pending = None
        if event_data:
            payload = "\n".join(event_data).encode("utf-8")
            event_data.clear()
            if payload.strip():
                received_any = True
                await emit_payload(payload)

    async def _process_text_chunk(text: str) -> None:
        nonlocal event_id_pending
        for raw_line in text.splitlines():
            line = raw_line.rstrip("\r")
            if not line:
                await _flush_event()
                continue
            if line.startswith(":"):
                continue
            if line.startswith("id:"):
                event_id_pending = line[3:].lstrip()
            elif line.startswith("data:"):
                event_data.append(line[5:].lstrip())

    if resp.stream is not None:
        if isinstance(resp.stream, AsyncIterable):
            async for chunk in resp.stream:
                await _process_text_chunk(bytes(chunk).decode("utf-8", errors="replace"))
        else:
            for chunk in resp.stream:
                await _process_text_chunk(bytes(chunk).decode("utf-8", errors="replace"))
        await _flush_event()
    elif resp.body:
        await _process_text_chunk(resp.body.decode("utf-8", errors="replace"))
        await _flush_event()

    return current_id, received_any


async def _run_bridge_async(
    *,
    url: str,
    token_source: BridgeTokenSource,
    timeout_s: float,
    max_message_bytes: int,
    stdin: TextIO | AsyncIterable[bytes] | None,
    stdout: TextIO | Callable[[bytes], Awaitable[None]] | None,
    transport: BridgeTransport | Callable[..., Any] | None,
    reconnect_base_delay_s: float,
) -> int:
    in_stream: TextIO | AsyncIterable[bytes] = stdin if stdin is not None else sys.stdin
    out_target: TextIO | Callable[[bytes], Awaitable[None]] = stdout if stdout is not None else sys.stdout
    resolved_transport: BridgeTransport | Callable[..., Any] = (
        transport if transport is not None else UrllibBridgeTransport()
    )
    supports_full_http = isinstance(resolved_transport, BridgeTransport)

    write_lock = anyio.Lock()
    active_session_id: str | None = None
    negotiated_version: str | None = None
    relay_started = False
    relay_scope: anyio.CancelScope | None = None

    async def _emit_line(payload: bytes) -> None:
        text = payload.decode("utf-8", errors="replace").strip()
        if not text:
            return
        async with write_lock:
            if callable(out_target):
                await _maybe_await(out_target(text.encode("utf-8")))
            else:
                out_target.write(f"{text}\n")
                out_target.flush()

    async def _send_http(method: str, headers: Mapping[str, str], body: bytes) -> BridgeResponse:
        if isinstance(resolved_transport, BridgeTransport):
            return cast(
                BridgeResponse,
                await _maybe_await(resolved_transport.request(method, url, headers, body, timeout_s)),
            )
        raw = await _maybe_await(resolved_transport(url, dict(headers), body))
        return _normalize_legacy_response(raw)

    async def _send_with_auth_retry(
        *,
        method: str,
        payload: bytes,
        protocol_version: str | None,
        extra_headers: Mapping[str, str] | None = None,
    ) -> BridgeResponse:
        token = await _maybe_await(token_source.get_token())
        for attempt in range(2):
            headers = mcp_request_headers(
                payload,
                protocol_version=protocol_version,
                session_id=active_session_id,
                bearer_token=token,
            )
            if extra_headers:
                headers.update(extra_headers)
            if method.upper() == "GET":
                headers["Accept"] = "text/event-stream"
            resp = await _send_http(method, headers, payload)
            if resp.status != 401 or attempt == 1:
                return resp
            await _maybe_await(token_source.invalidate())
            token = await _maybe_await(token_source.get_token())
        raise AssertionError("unreachable")

    async def _sse_relay_loop() -> None:
        nonlocal relay_scope
        last_event_id: str | None = None
        reconnect_attempts = 0
        with anyio.CancelScope() as scope:
            relay_scope = scope
            while True:
                extra: dict[str, str] = {"Accept": "text/event-stream"}
                if last_event_id is not None:
                    extra["Last-Event-ID"] = last_event_id
                try:
                    resp = await _send_with_auth_retry(
                        method="GET",
                        payload=b"",
                        protocol_version=negotiated_version or MCP_PROTOCOL_VERSION,
                        extra_headers=extra,
                    )
                except Exception:
                    if reconnect_attempts >= _MAX_SSE_RECONNECTS:
                        return
                    await anyio.sleep(reconnect_base_delay_s * (2**reconnect_attempts))
                    reconnect_attempts += 1
                    continue

                if resp.status in (404, 405):
                    return
                if resp.status == 401:
                    await _emit_line(
                        _build_jsonrpc_error_frame(None, -32001, "Bridge authentication failed (HTTP 401)")
                    )
                    return
                if resp.status >= 400:
                    if reconnect_attempts >= _MAX_SSE_RECONNECTS:
                        return
                    await anyio.sleep(reconnect_base_delay_s * (2**reconnect_attempts))
                    reconnect_attempts += 1
                    continue

                try:
                    last_event_id, received_any = await _consume_sse_stream(
                        resp,
                        last_event_id=last_event_id,
                        emit_payload=_emit_line,
                    )
                except Exception:
                    received_any = False

                if received_any:
                    reconnect_attempts = 0
                if reconnect_attempts >= _MAX_SSE_RECONNECTS:
                    return
                await anyio.sleep(reconnect_base_delay_s * (2**reconnect_attempts))
                reconnect_attempts += 1

    async def _next_stdin_line() -> str | None:
        if isinstance(in_stream, AsyncIterable):
            return None
        line = await anyio.to_thread.run_sync(in_stream.readline)
        return line if line else None

    async with anyio.create_task_group() as session_tg:

        async def _handle_input_line(raw_text: str) -> None:
            nonlocal active_session_id, negotiated_version, relay_started
            stripped = raw_text.strip()
            if not stripped:
                return
            payload = stripped.encode("utf-8")

            if len(payload) > max_message_bytes:
                await _emit_line(
                    _build_jsonrpc_error_frame(
                        None,
                        -32600,
                        f"Message exceeded max size of {max_message_bytes} bytes",
                    )
                )
                return

            method_name, req_id = _decode_frame_metadata(payload)
            if method_name == "initialize":
                req_protocol_version: str | None = None
            elif method_name in _PRE_NEGOTIATION_HEADERLESS_METHODS and negotiated_version is None:
                req_protocol_version = None
            else:
                req_protocol_version = negotiated_version or MCP_PROTOCOL_VERSION

            resp = await _send_with_auth_retry(
                method="POST",
                payload=payload,
                protocol_version=req_protocol_version,
            )
            if resp.status == 401:
                await _emit_line(_build_jsonrpc_error_frame(req_id, -32001, "Bridge authentication failed (HTTP 401)"))
                return

            resp_session = _header_ci(resp.headers, "mcp-session-id")
            if resp_session:
                active_session_id = resp_session

            if method_name == "initialize" and resp.status < 400:
                detected_ver = _extract_protocol_version(resp)
                if detected_ver:
                    negotiated_version = detected_ver
                if (
                    supports_full_http
                    and active_session_id is not None
                    and not relay_started
                    and (negotiated_version is None or negotiated_version in LEGACY_PROTOCOL_VERSIONS)
                ):
                    relay_started = True
                    session_tg.start_soon(_sse_relay_loop)

            if len(resp.body) > max_message_bytes:
                await _emit_line(
                    _build_jsonrpc_error_frame(
                        None,
                        -32600,
                        f"Response exceeded max size of {max_message_bytes} bytes",
                    )
                )
                return

            c_type = _header_ci(resp.headers, "content-type") or ""
            if "text/event-stream" in c_type:
                for event_text in _parse_sse_events(resp.body):
                    await _emit_line(event_text.encode("utf-8"))
            else:
                await _emit_line(resp.body)

        if isinstance(in_stream, AsyncIterable):
            async for raw_bytes in in_stream:
                await _handle_input_line(bytes(raw_bytes).decode("utf-8", errors="replace"))
        else:
            while True:
                raw_line = await _next_stdin_line()
                if raw_line is None:
                    break
                await _handle_input_line(raw_line)

        if relay_scope is not None:
            relay_scope.cancel()

    if supports_full_http and active_session_id is not None:
        token = await _maybe_await(token_source.get_token())
        delete_headers: dict[str, str] = {
            "Mcp-Session-Id": active_session_id,
            "MCP-Protocol-Version": negotiated_version or MCP_PROTOCOL_VERSION,
        }
        if token:
            delete_headers["Authorization"] = f"Bearer {token}"
        with contextlib.suppress(Exception):
            await _send_http("DELETE", delete_headers, b"")

    return 0


def run_stdio_http_bridge(
    *,
    url: str,
    bearer_token: str | None = None,
    token_source: BridgeTokenSource | None = None,
    token_provider: Callable[[], Awaitable[str | None] | str | None] | None = None,
    audience: str | None = None,
    timeout_s: float = 30.0,
    max_message_bytes: int = 4_194_304,
    stdin: TextIO | AsyncIterable[bytes] | None = None,
    stdout: TextIO | Callable[[bytes], Awaitable[None]] | None = None,
    transport: BridgeTransport | Callable[..., Any] | None = None,
    reconnect_base_delay_s: float = 0.5,
) -> int:
    """Forward newline-delimited JSON-RPC frames over HTTP/SSE with optional OIDC auth."""
    if token_source is not None:
        resolved_source = token_source
    elif audience is not None:
        resolved_source = OidcTokenSource(audience)
    elif token_provider is not None:
        resolved_source = _CallableTokenSource(token_provider)
    else:
        resolved_source = StaticTokenSource(bearer_token or os.environ.get("VERTEBRA_MCP_TOKEN"))

    return anyio.run(
        lambda: _run_bridge_async(
            url=url,
            token_source=resolved_source,
            timeout_s=timeout_s,
            max_message_bytes=max_message_bytes,
            stdin=stdin,
            stdout=stdout,
            transport=transport,
            reconnect_base_delay_s=reconnect_base_delay_s,
        )
    )
