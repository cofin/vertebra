"""WSGI framework extensions — zero-copy WsgiFileWrapper resolution and Django/Flask integrations."""

from __future__ import annotations

import importlib
import logging
import os
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, cast

from vertebra._vertebra import WsgiFileWrapper

__all__ = (
    "DjangoWsgiApp",
    "FlaskWsgiApp",
    "LargeBodyBypass",
    "WsgiFileWrapper",
    "configure_django",
    "create_flask_app",
    "resolve_wsgi_file_wrapper",
    "wrap_with_error_handler",
)

log = logging.getLogger(__name__)

_LARGE_BODY_THRESHOLD = 16 * 1024 * 1024
type _FlaskFactory = Callable[[], Any]


def resolve_wsgi_file_wrapper(result_iterable: Any) -> str | None:
    """Return the underlying filesystem path if result_iterable is a filesystem-backed WsgiFileWrapper."""
    if isinstance(result_iterable, WsgiFileWrapper):
        path = result_iterable.path
        if path and Path(path).is_file():
            return str(path)
    filelike = getattr(result_iterable, "filelike", None)
    if filelike is not None:
        name = getattr(filelike, "name", None)
        if isinstance(name, str) and Path(name).is_file():
            return name
    return None


class LargeBodyBypass:
    """Context manager that flags oversized request bodies."""

    __slots__ = ("_environ", "_patched")

    def __init__(self, environ: dict[str, Any]) -> None:
        self._environ = environ
        self._patched = False

    def __enter__(self) -> dict[str, Any]:
        content_length_str = self._environ.get("CONTENT_LENGTH", "")
        try:
            content_length = int(content_length_str)
        except (ValueError, TypeError):
            content_length = 0
        if content_length > _LARGE_BODY_THRESHOLD:
            self._patched = True
            log.debug("LargeBodyBypass active: Content-Length=%d", content_length)
        return self._environ

    def __exit__(self, *_args: object) -> None:
        if self._patched:
            log.debug("LargeBodyBypass exiting.")


class DjangoWsgiApp:
    """Lazy WSGI wrapper for a Django application."""

    __slots__ = ("_handler", "_settings_module")

    def __init__(self, settings_module: str | None = None) -> None:
        self._settings_module: str | None = settings_module
        self._handler: Callable[..., Iterable[bytes]] | None = None

    def __call__(self, environ: dict[str, Any], start_response: Callable[..., None]) -> Iterable[bytes]:
        return self._get_handler()(environ, start_response)

    def _get_handler(self) -> Callable[..., Iterable[bytes]]:
        if self._handler is None:
            try:
                from django.core.wsgi import get_wsgi_application
            except ImportError as exc:
                msg = "Django is not installed. Install it with: uv add django"
                raise ImportError(msg) from exc
            if self._settings_module is not None:
                configure_django(self._settings_module)
            self._handler = get_wsgi_application()
            log.info("Django WSGIHandler initialized.")
        return self._handler


class FlaskWsgiApp:
    """WSGI wrapper for a Flask application instance or factory."""

    __slots__ = ("_app", "_app_or_factory")

    def __init__(self, app_or_factory: Any) -> None:
        self._app_or_factory: Any = app_or_factory
        if hasattr(app_or_factory, "wsgi_app"):
            self._app: Any = app_or_factory
        else:
            self._app = None

    def __call__(self, environ: dict[str, Any], start_response: Callable[..., None]) -> Iterable[bytes]:
        return cast("Iterable[bytes]", self._get_app()(environ, start_response))

    def _get_app(self) -> Any:
        if self._app is None:
            try:
                self._app = self._app_or_factory()
            except Exception as exc:
                msg = "Flask app factory raised an exception."
                raise RuntimeError(msg) from exc
            log.info("Flask application initialized via factory.")
        return self._app


def wrap_with_error_handler(
    app: Callable[..., Iterable[bytes]],
    *,
    logger: logging.Logger | None = None,
) -> Callable[..., Iterable[bytes]]:
    """Wrap a WSGI app with structured exception logging."""
    active_logger = logger or log

    def _wrapped(
        environ: dict[str, Any],
        start_response: Callable[..., None],
    ) -> Iterable[bytes]:
        try:
            return app(environ, start_response)
        except Exception:
            active_logger.exception(
                "Unhandled exception in WSGI app: method=%s path=%s",
                environ.get("REQUEST_METHOD", "?"),
                environ.get("PATH_INFO", "?"),
            )
            raise

    return _wrapped


def configure_django(settings_module: str) -> None:
    """Set DJANGO_SETTINGS_MODULE if not already set."""
    current = os.environ.get("DJANGO_SETTINGS_MODULE")
    if current and current != settings_module:
        log.warning("DJANGO_SETTINGS_MODULE already set to %r; ignoring %r.", current, settings_module)
        return
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", settings_module)


def create_flask_app(module_path: str, factory_name: str = "create_app") -> FlaskWsgiApp:
    """Import a Flask application factory from a module path."""
    module = importlib.import_module(module_path)
    factory: _FlaskFactory = getattr(module, factory_name)
    return FlaskWsgiApp(factory)
