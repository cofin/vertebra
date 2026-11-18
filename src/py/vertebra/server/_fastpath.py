"""Shared zero-closure ASGI/WSGI fastpath wrappers, direct raw send, and multi-framework mount attachment."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Buffer, Callable
from contextlib import suppress
from typing import Any, Protocol, cast

from vertebra._vertebra import ArrowCursor as RustArrowCursor
from vertebra.db.arrow import ArrowCursor
from vertebra.server._fastpath_core import (
    BODYLESS_HTTP_METHODS,
    get_attached_fastpath,
    resolve_fastpath_provider,
    set_attached_fastpath,
)
from vertebra.storage._streaming import StreamingStorageResponse
from vertebra.typing import VertebraError
from vertebra.utils.type_guards import is_coroutine_callable

__all__ = (
    "ARROW_STREAM_MEDIA_TYPE",
    "DEFAULT_MAX_BODY_SIZE",
    "PayloadTooLargeError",
    "attach_framework_fastpath",
    "extract_scope_body_fast",
    "resolve_relative_path",
    "send_asgi_direct",
    "serialize_fastpath_special_result",
    "wrap_asgi_fastpath",
)

ARROW_STREAM_MEDIA_TYPE: bytes = b"application/vnd.apache.arrow.stream"
DEFAULT_MAX_BODY_SIZE: int = 10 * 1024 * 1024
_PAYLOAD_TOO_LARGE_BODY: bytes = b"Payload Too Large"
_PAYLOAD_TOO_LARGE_HEADERS: list[tuple[bytes, bytes]] = [
    (b"content-type", b"text/plain; charset=utf-8"),
    (b"content-length", b"17"),
]


class _BodyBytesFn(Protocol):
    """Callable protocol for zero-copy prebuffered body byte extractors."""

    def __call__(self) -> bytes:
        """Return prebuffered request body bytes."""
        ...


class _BufferFn(Protocol):
    """Callable protocol for deferred buffer producers cached in scope."""

    def __call__(self) -> Buffer:
        """Return a readable byte buffer."""
        ...


class PayloadTooLargeError(VertebraError):
    """Raised when an incoming request payload exceeds the configured byte limit."""

    status_code: int = 413

    def __init__(self, limit: int) -> None:
        self.max_body_size = limit
        super().__init__(f"Request payload exceeds limit of {limit} bytes")


def serialize_fastpath_special_result(
    result: Any,
    *,
    default_status: int = 200,
) -> tuple[int, list[tuple[bytes, bytes]], bytes | memoryview] | None:
    """Serialize `ArrowCursor` or `StreamingStorageResponse` directly without JSON encoding."""
    if result is None:
        return None
    if isinstance(result, ArrowCursor | RustArrowCursor):
        raw_arrow: memoryview = result.buffer_view()
        return (
            default_status,
            [
                (b"content-type", ARROW_STREAM_MEDIA_TYPE),
                (b"content-length", str(len(raw_arrow)).encode("ascii")),
            ],
            raw_arrow,
        )
    if isinstance(result, StreamingStorageResponse):
        return result.to_fastpath_tuple()
    return None


def resolve_relative_path(scope: Any, mount_prefix: str = "") -> str:
    """Resolve the route-matching path from an ASGI scope by stripping mount_prefix or root_path."""
    raw_path = str(scope.get("path", "/") if hasattr(scope, "get") else scope["path"])
    prefix = mount_prefix or str((scope.get("root_path", "") if hasattr(scope, "get") else "") or "")
    if prefix:
        norm = prefix.rstrip("/")
        if norm:
            if raw_path == norm:
                return "/"
            if raw_path.startswith(f"{norm}/"):
                return raw_path[len(norm) :]
    return raw_path or "/"


async def extract_scope_body_fast(
    scope: Any,
    receive: Any,
    body: bytes | None = None,
    *,
    method: str = "POST",
    max_body_size: int | None = DEFAULT_MAX_BODY_SIZE,
) -> bytes:
    """Extract request body bytes synchronously from AsgiScope/AsgiProtocol before falling back to receive()."""
    if body is not None:
        return _enforce_body_limit(body, max_body_size)
    if method in BODYLESS_HTTP_METHODS:
        return b""

    scope_checked = False
    if hasattr(scope, "__contains__") and "_vertebra_body_bytes" in scope:
        scope_checked = True
        cached = scope["_vertebra_body_bytes"]
        if callable(cached):
            resolved_cached = bytes(cast("_BufferFn", cached)())
            _cache_scope_body(scope, resolved_cached)
            return _enforce_body_limit(resolved_cached, max_body_size)
        if isinstance(cached, bytes):
            return _enforce_body_limit(cached, max_body_size)
        if isinstance(cached, bytearray | memoryview):
            resolved_buf = bytes(cached)
            _cache_scope_body(scope, resolved_buf)
            return _enforce_body_limit(resolved_buf, max_body_size)

    if not scope_checked:
        body_bytes_fn = getattr(scope, "body_bytes", None)
        if callable(body_bytes_fn):
            snap_bytes: bytes = cast("_BodyBytesFn", body_bytes_fn)()
            if snap_bytes:
                _cache_scope_body(scope, snap_bytes)
                return _enforce_body_limit(snap_bytes, max_body_size)

    recv_owner = getattr(receive, "__self__", None)
    owner_body_fn = getattr(recv_owner, "body_bytes", None)
    if callable(owner_body_fn):
        owner_bytes: bytes = cast("_BodyBytesFn", owner_body_fn)()
        _cache_scope_body(scope, owner_bytes)
        return _enforce_body_limit(owner_bytes, max_body_size)
    if not callable(receive):
        return b""

    receive_fn = cast("Callable[[], Awaitable[Any]]", receive)
    chunks: list[bytes] = []
    total_bytes = 0
    while True:
        message = await receive_fn()
        if not isinstance(message, dict):
            break
        if message.get("type") == "http.disconnect":
            break
        chunk = message.get("body", b"")
        if chunk:
            chunk_bytes = chunk if isinstance(chunk, bytes) else bytes(chunk)
            total_bytes += len(chunk_bytes)
            if max_body_size is not None and total_bytes > max_body_size:
                raise PayloadTooLargeError(max_body_size)
            chunks.append(chunk_bytes)
        if not message.get("more_body", False):
            break
    assembled = b"" if not chunks else (chunks[0] if len(chunks) == 1 else b"".join(chunks))
    _cache_scope_body(scope, assembled)
    return assembled


async def send_asgi_direct(
    send: Any,
    status_code: int,
    encoded_headers: list[tuple[bytes, bytes]],
    raw_body: bytes | memoryview,
) -> None:
    """Emit an HTTP response via single-call send_raw_response when supported, or standard 2-dict ASGI send."""
    raw_send = getattr(send, "send_raw_response", None)
    if raw_send is None:
        send_owner = getattr(send, "__self__", None)
        if send_owner is not None:
            raw_send = getattr(send_owner, "send_raw_response", None)
    if callable(raw_send):
        res = raw_send(status_code, encoded_headers, raw_body)
        if inspect.isawaitable(res):
            await res
        return

    body_payload = bytes(raw_body) if isinstance(raw_body, memoryview) else raw_body
    await send(
        {
            "type": "http.response.start",
            "status": status_code,
            "headers": encoded_headers,
        }
    )
    await send(
        {
            "type": "http.response.body",
            "body": body_payload,
            "more_body": False,
        }
    )


def wrap_asgi_fastpath(
    bypass: Any,
    fallback_app: Callable[..., Any],
    *,
    max_body_size: int | None = DEFAULT_MAX_BODY_SIZE,
) -> Callable[..., Any]:
    """Wrap an ASGI 3.0 callable with zero-closure C-API fastpath dispatch and replay fallback."""
    dispatch_fn = getattr(bypass, "dispatch_async", None) or bypass.dispatch

    async def _fastpath_asgi_app(scope: Any, receive: Any, send: Any) -> None:
        send_owner = getattr(send, "__self__", None)
        has_native_send_state = hasattr(send, "started") or (send_owner is not None and hasattr(send_owner, "started"))
        method = str(scope.get("method", "GET") if hasattr(scope, "get") else "GET").upper()
        try:
            has_prebuffered_body = _probe_and_cache_prebuffered_body(
                scope,
                receive,
                method=method,
                max_body_size=max_body_size,
            )
        except PayloadTooLargeError:
            await send_asgi_direct(send, 413, list(_PAYLOAD_TOO_LARGE_HEADERS), _PAYLOAD_TOO_LARGE_BODY)
            return

        if has_native_send_state and has_prebuffered_body:
            try:
                direct = await dispatch_fn(scope, receive, send)
            except PayloadTooLargeError:
                await send_asgi_direct(send, 413, list(_PAYLOAD_TOO_LARGE_HEADERS), _PAYLOAD_TOO_LARGE_BODY)
                return
            if direct is not None:
                status_code, encoded_headers, raw_body = direct
                await send_asgi_direct(send, status_code, encoded_headers, raw_body)
                return
            already_started = bool(
                getattr(send, "started", False) or (send_owner is not None and getattr(send_owner, "started", False))
            )
            if already_started:
                return
            await fallback_app(scope, receive, send)
            return

        sent_via_callback = False
        buffered_messages: list[Any] = []
        buffered_bytes = 0
        upstream_exhausted = False

        async def _buffering_receive() -> Any:
            nonlocal buffered_bytes, upstream_exhausted
            msg = await receive()
            if isinstance(msg, dict) and msg.get("type") == "http.request":
                chunk = msg.get("body", b"")
                if chunk:
                    buffered_bytes += len(chunk)
                    if max_body_size is not None and buffered_bytes > max_body_size:
                        raise PayloadTooLargeError(max_body_size)
                if not msg.get("more_body", False):
                    upstream_exhausted = True
            buffered_messages.append(msg)
            return msg

        async def _tracked_send(message: Any) -> None:
            nonlocal sent_via_callback
            sent_via_callback = True
            await send(message)

        try:
            direct = await dispatch_fn(scope, _buffering_receive, _tracked_send)
        except PayloadTooLargeError:
            if not sent_via_callback:
                await send_asgi_direct(send, 413, list(_PAYLOAD_TOO_LARGE_HEADERS), _PAYLOAD_TOO_LARGE_BODY)
            return
        if direct is not None:
            status_code, encoded_headers, raw_body = direct
            await send_asgi_direct(send, status_code, encoded_headers, raw_body)
            return
        if sent_via_callback:
            return
        if not buffered_messages:
            await fallback_app(scope, receive, send)
            return

        replay_idx = 0

        async def _replay_receive() -> Any:
            nonlocal replay_idx
            if replay_idx < len(buffered_messages):
                item = buffered_messages[replay_idx]
                replay_idx += 1
                return item
            if upstream_exhausted:
                return {"type": "http.request", "body": b"", "more_body": False}
            return await receive()

        await fallback_app(scope, _replay_receive, send)

    dyn_app = cast("Any", _fastpath_asgi_app)
    set_attached_fastpath(dyn_app, bypass)
    dyn_app.__vertebra_fastpath_attached__ = True
    return _fastpath_asgi_app


def attach_framework_fastpath(
    app: Any,
    *,
    mount_prefix: str = "",
    protocol: str = "auto",
) -> Any:
    """Idempotently detect and attach a framework FastPath wrapper to an ASGI or WSGI application."""
    if app is None:
        return app
    norm_prefix = mount_prefix.rstrip("/")

    if getattr(app, "__vertebra_fastpath_attached__", False):
        existing_fp = get_attached_fastpath(app)
        if existing_fp is not None and norm_prefix and hasattr(existing_fp, "_mount_prefix"):
            existing_fp._mount_prefix = norm_prefix
        wsgi_inner = getattr(app, "wsgi_app", None)
        if (
            protocol != "asgi"
            and callable(wsgi_inner)
            and (not callable(app) or getattr(wsgi_inner, "__vertebra_fastpath_attached__", False))
        ):
            return wsgi_inner
        return app

    if hasattr(app, "wrap_asgi") and hasattr(app, "dispatch"):
        if norm_prefix and hasattr(app, "_mount_prefix"):
            app._mount_prefix = norm_prefix
        if protocol == "wsgi" and hasattr(app, "wrap_wsgi"):
            return app.wrap_wsgi()
        return app.wrap_asgi()

    for candidate_app in (app, getattr(app, "_inner", None)):
        if candidate_app is None:
            continue
        cand_state = getattr(candidate_app, "state", None)
        if cand_state is not None:
            plugin = (
                cand_state.get("vertebra_plugin")
                if hasattr(cand_state, "get")
                else getattr(cand_state, "vertebra_plugin", None)
            )
            ensure_fn = getattr(plugin, "ensure_finalized", None)
            if callable(ensure_fn):
                ensure_fn(candidate_app)

    existing_fp = get_attached_fastpath(app)

    if existing_fp is None:
        provider = resolve_fastpath_provider(app)
        if provider is None:
            return app
        existing_fp = provider.build(app, mount_prefix=norm_prefix)
        if existing_fp is None:
            return app
        with suppress(TypeError):
            set_attached_fastpath(app, existing_fp)

    if norm_prefix and hasattr(existing_fp, "_mount_prefix"):
        existing_fp._mount_prefix = norm_prefix

    if hasattr(existing_fp, "wrap_wsgi"):
        if protocol == "asgi" or (protocol == "auto" and is_coroutine_callable(app)):
            return existing_fp.wrap_asgi(app if is_coroutine_callable(app) else None)
        fallback_wsgi = (
            app.wsgi_app
            if callable(getattr(app, "wsgi_app", None))
            else (
                getattr(app, "_wsgi_app", None)
                if callable(getattr(app, "_wsgi_app", None))
                else (app if callable(app) else None)
            )
        )
        wrapped_wsgi = existing_fp.wrap_wsgi(fallback_wsgi)
        dyn_wsgi = cast("Any", wrapped_wsgi)
        set_attached_fastpath(dyn_wsgi, existing_fp)
        dyn_wsgi.__vertebra_fastpath_attached__ = True
        if callable(getattr(app, "wsgi_app", None)):
            with suppress(AttributeError, TypeError):
                cast("Any", app).wsgi_app = wrapped_wsgi
                cast("Any", app).__vertebra_fastpath_attached__ = True
        return wrapped_wsgi

    if hasattr(existing_fp, "wrap_asgi"):
        fallback_asgi = app if callable(app) else None
        return existing_fp.wrap_asgi(fallback_asgi)

    return app


def _enforce_body_limit(payload: bytes, max_body_size: int | None) -> bytes:
    """Raise ``PayloadTooLargeError`` when ``payload`` exceeds ``max_body_size``."""
    if max_body_size is not None and len(payload) > max_body_size:
        raise PayloadTooLargeError(max_body_size)
    return payload


def _cache_scope_body(scope: Any, payload: bytes | None) -> None:
    """Store resolved body bytes in ``scope['_vertebra_body_bytes']`` when ``scope`` is mutable."""
    if hasattr(scope, "__setitem__"):
        with suppress(TypeError, AttributeError):
            scope["_vertebra_body_bytes"] = payload


def _probe_and_cache_prebuffered_body(
    scope: Any,
    receive: Any,
    *,
    method: str,
    max_body_size: int | None,
) -> bool:
    """Inspect and cache any prebuffered body on ``scope`` or ``receive`` at most once."""
    body_bytes_fn = getattr(scope, "body_bytes", None)
    owner_body_fn = getattr(getattr(receive, "__self__", None), "body_bytes", None)
    if hasattr(scope, "__contains__") and "_vertebra_body_bytes" in scope:
        cached = scope["_vertebra_body_bytes"]
        if callable(cached):
            resolved = bytes(cast("_BufferFn", cached)())
            _cache_scope_body(scope, resolved)
            _enforce_body_limit(resolved, max_body_size)
            return True
        if isinstance(cached, bytes):
            _enforce_body_limit(cached, max_body_size)
            return True
        if isinstance(cached, bytearray | memoryview):
            resolved = bytes(cached)
            _cache_scope_body(scope, resolved)
            _enforce_body_limit(resolved, max_body_size)
            return True
        return False

    if method in BODYLESS_HTTP_METHODS:
        return callable(body_bytes_fn) or callable(owner_body_fn)

    if callable(body_bytes_fn):
        snap_bytes: bytes = cast("_BodyBytesFn", body_bytes_fn)()
        if snap_bytes:
            _cache_scope_body(scope, snap_bytes)
            _enforce_body_limit(snap_bytes, max_body_size)
            return True
        if callable(owner_body_fn):
            owner_bytes: bytes = cast("_BodyBytesFn", owner_body_fn)()
            _cache_scope_body(scope, owner_bytes)
            _enforce_body_limit(owner_bytes, max_body_size)
            return True
        _cache_scope_body(scope, None)
        return False

    if callable(owner_body_fn):
        owner_bytes = cast("_BodyBytesFn", owner_body_fn)()
        _cache_scope_body(scope, owner_bytes)
        _enforce_body_limit(owner_bytes, max_body_size)
        return True

    return False
