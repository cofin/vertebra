"""Sub-interpreter worker pool using Vertebra's ring buffer as the IPC bridge.

Each sub-interpreter:

1. Imports the user's WSGI app independently (full module isolation).
2. Reads raw requests from a :class:`~vertebra.server._ipc._ring_reader.RawRequest`
   batch delivered via ``concurrent.interpreters.Queue`` (no ``_vertebra``
   import needed in the worker).
3. Builds a standard PEP 3333 WSGI ``environ`` dict from parsed data.
4. Calls the app's WSGI callable **completely unmodified**.
5. Sends responses back via the shared queue.

Requires Python 3.14+ (``concurrent.interpreters``).
"""

import io
import sys
import urllib.parse
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from vertebra.server._ipc._ring_reader import RawRequest


def interpreters_available() -> bool:
    """Check whether the ``concurrent.interpreters`` module is available."""
    if sys.version_info < (3, 14):
        return False
    try:
        from concurrent import interpreters  # noqa: F401

        return True
    except ImportError:
        return False


def build_wsgi_environ(
    req: "RawRequest",
    headers: list[tuple[str, str]],
    server_name: str = "localhost",
    server_port: int = 8000,
) -> dict[str, Any]:
    """Build a PEP 3333 compliant WSGI environ dict from a :class:`RawRequest`.

    This produces the same environ that :class:`vertebra.server._wsgi._environ.WsgiEnviron`
    creates, but from raw parsed data instead of from a ``VertebraRequest``
    proxy.  The result can be passed directly to any WSGI application.
    """
    environ: dict[str, Any] = {
        "REQUEST_METHOD": req.method,
        "SCRIPT_NAME": "",
        "PATH_INFO": urllib.parse.unquote(req.path),
        "QUERY_STRING": req.query or "",
        "SERVER_NAME": server_name,
        "SERVER_PORT": str(server_port),
        "SERVER_PROTOCOL": f"HTTP/{req.http_version}",
        "wsgi.version": (1, 0),
        "wsgi.url_scheme": "http",
        "wsgi.input": io.BytesIO(req.body),
        "wsgi.errors": sys.stderr,
        "wsgi.multithread": True,
        "wsgi.multiprocess": False,
        "wsgi.run_once": False,
    }

    for name, value in headers:
        key = "HTTP_" + name.upper().replace("-", "_")
        if key == "HTTP_CONTENT_TYPE":
            environ["CONTENT_TYPE"] = value
        elif key == "HTTP_CONTENT_LENGTH":
            environ["CONTENT_LENGTH"] = value
        else:
            environ[key] = value

    return environ


def call_wsgi_app(
    app: Any,
    environ: dict[str, Any],
) -> tuple[int, list[tuple[str, str]], bytes]:
    """Invoke a WSGI application and collect the response.

    Returns:
        ``(status_code, response_headers, body_bytes)``
    """
    response_started: list[str] = []
    response_headers: list[list[tuple[str, str]]] = []

    def start_response(
        status: str,
        headers: list[tuple[str, str]],
        exc_info: Any = None,
    ) -> None:
        response_started.append(status)
        response_headers.append(headers)

    try:
        result = app(environ, start_response)
        body = b"".join(result)
        if hasattr(result, "close"):
            result.close()
    except Exception as exc:
        return 500, [("content-type", "text/plain")], str(exc).encode("utf-8")

    if not response_started:
        return 500, [("content-type", "text/plain")], b"WSGI app did not call start_response"

    status_code = int(response_started[0].split(" ", 1)[0])
    return status_code, response_headers[0], body


__all__ = (
    "build_wsgi_environ",
    "call_wsgi_app",
    "interpreters_available",
)
