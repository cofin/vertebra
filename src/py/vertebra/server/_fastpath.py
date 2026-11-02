"""Shared zero-closure ASGI/WSGI fastpath wrappers, direct raw send, and multi-framework mount attachment."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

from vertebra.utils.module_loader import import_optional

__all__ = (
    "attach_framework_fastpath",
    "extract_scope_body_fast",
    "resolve_relative_path",
    "send_asgi_direct",
    "wrap_asgi_fastpath",
)

_BODYLESS_HTTP_METHODS: frozenset[str] = frozenset({"GET", "HEAD", "DELETE", "OPTIONS"})


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
    if method in _BODYLESS_HTTP_METHODS:
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

    async def _fastpath_asgi_app(scope: Any, receive: Any, send: Any) -> None:
        send_owner = getattr(send, "__self__", None)
        has_native_send_state = hasattr(send, "started") or (
            send_owner is not None and hasattr(send_owner, "started")
        )
        body_bytes_fn = getattr(scope, "body_bytes", None)
        method = str(scope.get("method", "GET") if hasattr(scope, "get") else "GET").upper()
        has_prebuffered_body = callable(body_bytes_fn) and (
            method in _BODYLESS_HTTP_METHODS
            or bool(body_bytes_fn())
            or callable(getattr(getattr(receive, "__self__", None), "body_bytes", None))
        )

        if has_native_send_state and has_prebuffered_body:
            direct = await bypass.dispatch(scope, receive, send)
            if direct is not None:
                status_code, encoded_headers, raw_body = direct
                await send_asgi_direct(send, status_code, encoded_headers, raw_body)
                return
            already_started = bool(
                getattr(send, "started", False)
                or (send_owner is not None and getattr(send_owner, "started", False))
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

        direct = await bypass.dispatch(scope, _buffering_receive, _tracked_send)
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

    setattr(_fastpath_asgi_app, "_vertebra_fastpath", bypass)
    setattr(_fastpath_asgi_app, "__vertebra_fastpath__", bypass)
    setattr(_fastpath_asgi_app, "__vertebra_fastpath_attached__", True)
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
    if getattr(app, "__vertebra_fastpath_attached__", False):
        existing_fp = getattr(app, "_vertebra_fastpath", None) or getattr(app, "__vertebra_fastpath__", None)
        if existing_fp is not None and mount_prefix and hasattr(existing_fp, "_mount_prefix"):
            existing_fp._mount_prefix = mount_prefix
        return app

    existing_fp = getattr(app, "_vertebra_fastpath", None) or getattr(app, "__vertebra_fastpath__", None)
    if existing_fp is not None:
        if mount_prefix and hasattr(existing_fp, "_mount_prefix"):
            existing_fp._mount_prefix = mount_prefix
        if protocol == "wsgi" and hasattr(existing_fp, "wrap_wsgi"):
            wrapped_wsgi = existing_fp.wrap_wsgi(app)
            wrapped_wsgi.__vertebra_fastpath_attached__ = True
            return wrapped_wsgi
        if hasattr(existing_fp, "wrap_asgi"):
            return existing_fp.wrap_asgi(app)

    cls_name = type(app).__name__
    mod_name = str(getattr(type(app), "__module__", "") or "")

    if cls_name == "Litestar" or mod_name.startswith("litestar"):
        ls_mod = import_optional("vertebra.extensions.litestar.fastpath")
        if ls_mod is not None:
            fp = ls_mod.FastPath.from_app(app, mount_prefix=mount_prefix)
            app._vertebra_fastpath = fp
            app.__vertebra_fastpath__ = fp
            return fp.wrap_asgi(app)

    if cls_name == "FastAPI" or mod_name.startswith("fastapi"):
        fa_mod = import_optional("vertebra.extensions.fastapi.fastpath")
        if fa_mod is not None:
            fp = fa_mod.FastPath.from_app(app, mount_prefix=mount_prefix)
            app._vertebra_fastpath = fp
            app.__vertebra_fastpath__ = fp
            return fp.wrap_asgi(app)

    if cls_name == "Starlette" or mod_name.startswith("starlette"):
        st_mod = import_optional("vertebra.extensions.starlette.fastpath")
        if st_mod is not None:
            fp = st_mod.FastPath.from_app(app, mount_prefix=mount_prefix)
            app._vertebra_fastpath = fp
            app.__vertebra_fastpath__ = fp
            return fp.wrap_asgi(app)

    if cls_name == "Flask" or mod_name.startswith("flask"):
        fl_fp_mod = import_optional("vertebra.extensions.flask.fastpath")
        fl_rt_mod = import_optional("vertebra.extensions.flask.router")
        if fl_fp_mod is not None and fl_rt_mod is not None:
            compiler = fl_rt_mod.RouteCompiler(app)
            fp = fl_fp_mod.FastPath(compiler, mount_prefix=mount_prefix)
            app._vertebra_fastpath = fp
            app.__vertebra_fastpath__ = fp
            if protocol == "asgi":
                return fp.wrap_asgi()
            target_wsgi = getattr(app, "wsgi_app", app)
            app.wsgi_app = fp.wrap_wsgi(target_wsgi)
            app.__vertebra_fastpath_attached__ = True
            return app

    if cls_name in {"WSGIHandler", "ASGIHandler"} or mod_name.startswith("django"):
        dj_fp_mod = import_optional("vertebra.extensions.django.fastpath")
        dj_rt_mod = import_optional("vertebra.extensions.django.router")
        if dj_fp_mod is not None and dj_rt_mod is not None:
            urlconf = getattr(app, "urlconf", None)
            compiler = dj_rt_mod.RouteCompiler(urlconf=urlconf)
            fp = dj_fp_mod.FastPath(compiler, mount_prefix=mount_prefix)
            if protocol == "asgi" or cls_name == "ASGIHandler":
                return fp.wrap_asgi(app)
            return fp.wrap_wsgi(app)

    return app
