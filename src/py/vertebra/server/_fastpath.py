"""Shared zero-closure ASGI/WSGI fastpath wrappers, direct raw send, and multi-framework mount attachment."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from contextlib import suppress
from typing import Any, cast

from vertebra.server._fastpath_core import BODYLESS_HTTP_METHODS, resolve_fastpath_provider
from vertebra.utils.type_guards import is_coroutine_callable

__all__ = (
    "ARROW_STREAM_MEDIA_TYPE",
    "attach_framework_fastpath",
    "extract_scope_body_fast",
    "resolve_relative_path",
    "send_asgi_direct",
    "serialize_fastpath_special_result",
    "wrap_asgi_fastpath",
)

ARROW_STREAM_MEDIA_TYPE: bytes = b"application/vnd.apache.arrow.stream"


def serialize_fastpath_special_result(
    result: Any,
    *,
    default_status: int = 200,
) -> tuple[int, list[tuple[bytes, bytes]], bytes] | None:
    """Serialize `ArrowCursor` or `StreamingStorageResponse` directly without JSON encoding."""
    if result is None:
        return None
    cls_name = type(result).__name__
    if cls_name == "ArrowCursor":
        buffer_view_fn = getattr(result, "buffer_view", None)
        if callable(buffer_view_fn):
            raw_arrow = bytes(buffer_view_fn())
            return (
                default_status,
                [
                    (b"content-type", ARROW_STREAM_MEDIA_TYPE),
                    (b"content-length", str(len(raw_arrow)).encode("ascii")),
                ],
                raw_arrow,
            )
    if cls_name == "StreamingStorageResponse":
        to_tuple_fn = getattr(result, "to_fastpath_tuple", None)
        if callable(to_tuple_fn):
            return cast("tuple[int, list[tuple[bytes, bytes]], bytes]", to_tuple_fn())
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
) -> bytes:
    """Extract request body bytes synchronously from AsgiScope/AsgiProtocol before falling back to receive()."""
    if body is not None:
        return body
    if method in BODYLESS_HTTP_METHODS:
        return b""
    body_bytes_fn = getattr(scope, "body_bytes", None)
    if callable(body_bytes_fn):
        snap_bytes: bytes = body_bytes_fn()
        if snap_bytes:
            return snap_bytes
    recv_owner = getattr(receive, "__self__", None)
    owner_body_fn = getattr(recv_owner, "body_bytes", None)
    if callable(owner_body_fn):
        owner_bytes: bytes = owner_body_fn()
        return owner_bytes
    if not callable(receive):
        return b""

    chunks: list[bytes] = []
    while True:
        message = await receive()
        if not isinstance(message, dict):
            break
        if message.get("type") == "http.disconnect":
            break
        chunk = message.get("body", b"")
        if chunk:
            chunks.append(chunk if isinstance(chunk, bytes) else bytes(chunk))
        if not message.get("more_body", False):
            break
    return b"" if not chunks else (chunks[0] if len(chunks) == 1 else b"".join(chunks))


async def send_asgi_direct(
    send: Any,
    status_code: int,
    encoded_headers: list[tuple[bytes, bytes]],
    raw_body: bytes,
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
            "body": raw_body,
            "more_body": False,
        }
    )


def wrap_asgi_fastpath(bypass: Any, fallback_app: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap an ASGI 3.0 callable with zero-closure C-API fastpath dispatch and replay fallback."""
    dispatch_fn = getattr(bypass, "dispatch_async", None) or bypass.dispatch

    async def _fastpath_asgi_app(scope: Any, receive: Any, send: Any) -> None:
        send_owner = getattr(send, "__self__", None)
        has_native_send_state = hasattr(send, "started") or (send_owner is not None and hasattr(send_owner, "started"))
        body_bytes_fn = getattr(scope, "body_bytes", None)
        method = str(scope.get("method", "GET") if hasattr(scope, "get") else "GET").upper()
        has_prebuffered_body = callable(body_bytes_fn) and (
            method in BODYLESS_HTTP_METHODS
            or bool(body_bytes_fn())
            or callable(getattr(getattr(receive, "__self__", None), "body_bytes", None))
        )

        if has_native_send_state and has_prebuffered_body:
            direct = await dispatch_fn(scope, receive, send)
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
        upstream_exhausted = False

        async def _buffering_receive() -> Any:
            nonlocal upstream_exhausted
            msg = await receive()
            buffered_messages.append(msg)
            if isinstance(msg, dict) and msg.get("type") == "http.request" and not msg.get("more_body", False):
                upstream_exhausted = True
            return msg

        async def _tracked_send(message: Any) -> None:
            nonlocal sent_via_callback
            sent_via_callback = True
            await send(message)

        direct = await dispatch_fn(scope, _buffering_receive, _tracked_send)
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
    dyn_app.__vertebra_fastpath__ = bypass
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
        existing_fp = getattr(app, "__vertebra_fastpath__", None) or getattr(app, "fastpath", None)
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

    existing_fp = getattr(app, "__vertebra_fastpath__", None) or getattr(app, "fastpath", None)
    if existing_fp is None:
        state = getattr(app, "state", None)
        if state is not None:
            existing_fp = (
                state.get("vertebra_fastpath_bypass")
                if hasattr(state, "get")
                else getattr(state, "vertebra_fastpath_bypass", None)
            )
    if existing_fp is None:
        extensions = getattr(app, "extensions", None)
        if isinstance(extensions, dict) and "vertebra" in extensions:
            existing_fp = getattr(extensions["vertebra"], "fastpath", None)

    if existing_fp is None:
        provider = resolve_fastpath_provider(app)
        if provider is None:
            return app
        existing_fp = provider.build(app, mount_prefix=norm_prefix)
        if existing_fp is None:
            return app
        state = getattr(app, "state", None)
        if state is not None:
            with suppress(TypeError, AttributeError):
                if hasattr(state, "__setitem__"):
                    state["vertebra_fastpath_bypass"] = existing_fp
                else:
                    state.vertebra_fastpath_bypass = existing_fp
        with suppress(AttributeError, TypeError):
            cast("Any", app).__vertebra_fastpath__ = existing_fp

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
        dyn_wsgi.__vertebra_fastpath__ = existing_fp
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
