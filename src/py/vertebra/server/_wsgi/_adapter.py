"""WsgiAdapter: wraps one WSGI app callable."""

from collections.abc import Callable, Iterable
from typing import Any

from vertebra.server._wsgi._environ import WsgiEnviron

__all__ = ["WsgiAdapter", "WsgiError"]

_WsgiApp = Callable[[dict[str, Any], Callable[..., None]], Iterable[bytes]]

# Deferred import cached at module level on first use.
_VertebraResponse: type | None = None


def _get_response_cls() -> type:
    global _VertebraResponse  # noqa: PLW0603
    if _VertebraResponse is None:
        from vertebra._vertebra import VertebraResponse
        _VertebraResponse = VertebraResponse
    return _VertebraResponse


class WsgiError(Exception):
    """Raised when the WSGI app violates the PEP 3333 contract."""


class WsgiAdapter:
    """Wraps a WSGI callable and translates VertebraRequest → VertebraResponse."""

    __slots__ = ("_app", "_url_scheme", "_response_cls")

    def __init__(self, app: _WsgiApp, *, url_scheme: str = "http") -> None:
        self._app: _WsgiApp = app
        self._url_scheme: str = url_scheme
        self._response_cls: type = _get_response_cls()

    def __call__(
        self,
        request: Any,
        *,
        server_name: str = "localhost",
        server_port: int = 8000,
    ) -> Any:
        """Run the WSGI app against *request* and return a response."""
        environ = WsgiEnviron(
            request,
            server_name=server_name,
            server_port=server_port,
            url_scheme=self._url_scheme,
        )

        _response_state: list[tuple[str, list[tuple[str, str]]] | None] = [None]

        def start_response(
            status: str,
            response_headers: list[tuple[str, str]],
            exc_info: Any = None,
        ) -> None:
            if exc_info is not None:
                try:
                    if _response_state[0] is not None:
                        raise exc_info[1].with_traceback(exc_info[2])
                finally:
                    exc_info = None
            if not isinstance(status, str):
                msg = f"start_response status must be str, got {type(status)!r}"
                raise WsgiError(msg)
            _response_state[0] = (status, list(response_headers))

        result_iterable: Iterable[bytes] = self._app(environ, start_response)  # type: ignore[arg-type]

        try:
            chunks: list[bytes] = list(result_iterable)
        finally:
            close = getattr(result_iterable, "close", None)
            if close is not None:
                close()

        if _response_state[0] is None:
            msg = "WSGI app returned without calling start_response"
            raise WsgiError(msg)

        status_line, response_headers = _response_state[0]

        try:
            status_code = int(status_line.split(" ", 1)[0])
        except (ValueError, IndexError) as exc:
            msg = f"Cannot parse status code from {status_line!r}"
            raise WsgiError(msg) from exc

        body = b"".join(chunks)

        return self._response_cls(
            status=status_code,
            headers=response_headers,
            body=body,
            request_id=getattr(request, "request_id", 0),
        )
