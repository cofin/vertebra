"""WSGI framework wrappers (Django, Flask) and large-body streaming helpers."""

import io
import logging
import os
import traceback
from collections.abc import Callable, Iterable
from typing import Any, Protocol, cast

from vertebra._vertebra import WsgiFileWrapper, WsgiInput
from vertebra.utils.module_loader import import_optional

__all__ = (
    "DjangoWsgiApp",
    "FlaskWsgiApp",
    "LargeBodyBypass",
    "WsgiApp",
    "resolve_wsgi_file_wrapper",
    "wrap_with_error_handler",
)

_logger = logging.getLogger("vertebra.wsgi")

_LARGE_BODY_THRESHOLD = 1024 * 1024


class WsgiApp(Protocol):
    """PEP 3333 WSGI application callable protocol."""

    def __call__(
        self,
        environ: dict[str, Any],
        start_response: Callable[..., Callable[[bytes], object]],
    ) -> Iterable[bytes]:
        """Invoke the WSGI application."""
        ...


def resolve_wsgi_file_wrapper(
    filelike: Any,
    block_size: int = 65536,
) -> Any:
    """Resolve the filesystem path of a ``WsgiFileWrapper`` or wrap a file-like object."""
    if isinstance(filelike, WsgiFileWrapper):
        return filelike.path
    return WsgiFileWrapper(filelike, block_size)


class LargeBodyBypass:
    """Zero-copy large request body bypass for WSGI applications and environments.

    When ``CONTENT_LENGTH`` exceeds ``threshold``, ``environ['wsgi.input']``
    is left bound directly to the slab-backed ``WsgiInput`` stream without
    eagerly reading the payload into a Python ``bytes`` buffer. Can be used
    both as a WSGI middleware wrapper and as a context manager around an
    ``environ`` mapping.
    """

    __slots__ = ("_app", "_environ", "_threshold")

    def __init__(
        self,
        target: Any = None,
        *,
        threshold: int = _LARGE_BODY_THRESHOLD,
    ) -> None:
        self._threshold = threshold
        if callable(target):
            self._app: Callable[..., Iterable[bytes]] | None = cast("Callable[..., Iterable[bytes]]", target)
            self._environ: Any = None
        else:
            self._app = None
            self._environ = target

    def _apply(self, environ: Any) -> Any:
        """Mark whether the request exceeds the large-body threshold without reading wsgi.input."""
        raw_cl = environ.get("CONTENT_LENGTH", "0")
        try:
            content_length = int(raw_cl) if raw_cl else 0
        except (ValueError, TypeError):
            content_length = 0

        if content_length > self._threshold:
            raw_stream = environ.get("wsgi.input")
            if isinstance(raw_stream, (bytes, bytearray, memoryview)):
                environ["wsgi.input"] = WsgiInput(bytes(raw_stream))
            environ["vertebra.large_body"] = True
        else:
            environ["vertebra.large_body"] = False
        return environ

    def __call__(self, *args: Any) -> Any:
        """Wrap a WSGI application or execute the wrapped WSGI application."""
        if self._app is None and len(args) == 1 and callable(args[0]):
            return LargeBodyBypass(args[0], threshold=self._threshold)
        if len(args) == 2:
            environ, start_response = args
            self._apply(environ)
            if self._app is None:
                msg = "LargeBodyBypass has no wrapped WSGI application"
                raise RuntimeError(msg)
            return self._app(environ, start_response)
        msg = "LargeBodyBypass expected either (app) or (environ, start_response)"
        raise TypeError(msg)

    def __enter__(self) -> Any:
        """Apply large-body bypass metadata to the bound WSGI environ."""
        if self._environ is None:
            msg = "LargeBodyBypass context manager requires an environ mapping"
            raise RuntimeError(msg)
        return self._apply(self._environ)

    def __exit__(self, *exc: object) -> None:
        """Exit the context manager without suppressing exceptions."""


def wrap_with_error_handler(
    app: Callable[..., Iterable[bytes]],
    *,
    debug: bool = False,
) -> Callable[..., Iterable[bytes]]:
    """Wrap a WSGI callable with an unhandled-exception safety net."""

    def _wrapped(
        environ: dict[str, Any],
        start_response: Callable[..., Any],
    ) -> Iterable[bytes]:
        headers_sent = False
        real_start_response = start_response

        def _tracking_start_response(
            status: str,
            headers: list[tuple[str, str]],
            exc_info: Any = None,
        ) -> Any:
            nonlocal headers_sent
            result = real_start_response(status, headers, exc_info)
            headers_sent = True
            return result

        try:
            app_iter = app(environ, _tracking_start_response)
            chunks = list(app_iter)
            close = getattr(app_iter, "close", None)
            if callable(close):
                close()
            return chunks
        except Exception:
            tb_text = traceback.format_exc()
            err_stream = environ.get("wsgi.errors")
            if isinstance(err_stream, io.TextIOBase):
                err_stream.write(tb_text)
            else:
                _logger.error("Unhandled WSGI exception:\n%s", tb_text)

            if headers_sent:
                raise

            body = tb_text.encode("utf-8") if debug else b"Internal Server Error"
            real_start_response(
                "500 Internal Server Error",
                [
                    ("Content-Type", "text/plain; charset=utf-8"),
                    ("Content-Length", str(len(body))),
                ],
            )
            return [body]

    return _wrapped


class DjangoWsgiApp:
    """Convenience wrapper for Django's ``get_wsgi_application()``."""

    __slots__ = ("_app", "_debug")

    def __init__(self, app: Any, *, debug: bool = False) -> None:
        self._app = wrap_with_error_handler(
            LargeBodyBypass(app),
            debug=debug,
        )
        self._debug = debug

    @classmethod
    def from_settings(
        cls,
        settings_module: str,
        *,
        debug: bool = False,
    ) -> "DjangoWsgiApp":
        """Initialise Django with ``settings_module`` and return a wrapped app."""
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", settings_module)
        django_wsgi = import_optional("django.core.wsgi")
        if django_wsgi is None:
            msg = "Django is required for DjangoWsgiApp.from_settings()"
            raise ImportError(msg)
        get_wsgi_application = django_wsgi.get_wsgi_application
        return cls(get_wsgi_application(), debug=debug)

    def __call__(
        self,
        environ: dict[str, Any],
        start_response: Callable[..., Any],
    ) -> Iterable[bytes]:
        return self._app(environ, start_response)


class FlaskWsgiApp:
    """Convenience wrapper for a Flask (or Werkzeug-compatible) WSGI application."""

    __slots__ = ("_app", "_debug")

    def __init__(self, flask_app: Any, *, debug: bool = False) -> None:
        wsgi_callable = getattr(flask_app, "wsgi_app", flask_app)
        self._app = wrap_with_error_handler(
            LargeBodyBypass(wsgi_callable),
            debug=debug,
        )
        self._debug = debug

    def __call__(
        self,
        environ: dict[str, Any],
        start_response: Callable[..., Any],
    ) -> Iterable[bytes]:
        return self._app(environ, start_response)
