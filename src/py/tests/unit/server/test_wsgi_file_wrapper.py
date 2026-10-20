"""Unit tests for zero-copy WsgiInput, WsgiFileWrapper pathsend delegation, and WsgiAdapter."""

from __future__ import annotations

import io
from pathlib import Path
from typing import Any

from vertebra._vertebra import WsgiEnviron, WsgiFileWrapper, WsgiInput, run_wsgi_callable
from vertebra.server._wsgi._adapter import WsgiAdapter
from vertebra.server._wsgi._extensions import resolve_wsgi_file_wrapper


def test_wsgi_input_read_readline_readlines_and_iter() -> None:
    """Verify Rust WsgiInput implements PEP 3333 stream operations without io.BytesIO."""
    stream = WsgiInput(b"first-line\nsecond-line\nthird")
    assert type(stream).__name__ == "WsgiInput"
    assert stream.readline() == b"first-line\n"
    assert stream.readline(6) == b"second"
    assert stream.readline() == b"-line\n"
    assert stream.read(5) == b"third"
    assert stream.read() == b""

    stream_lines = WsgiInput(b"a\nb\nc\n")
    assert stream_lines.readlines() == [b"a\n", b"b\n", b"c\n"]

    stream_iter = WsgiInput(b"x\ny\n")
    assert list(stream_iter) == [b"x\n", b"y\n"]


def test_wsgi_environ_exposes_wsgi_input_and_file_wrapper() -> None:
    """Verify WsgiEnviron provides native WsgiInput and WsgiFileWrapper in STATIC_KEYS."""
    environ = WsgiEnviron.from_parts(
        method="POST",
        path="/upload",
        query_string="v=1",
        headers=[(b"content-type", b"text/plain")],
        body=b"hello\nworld\n",
    )
    assert "wsgi.input" in environ
    assert "wsgi.file_wrapper" in environ
    wsgi_input = environ["wsgi.input"]
    assert isinstance(wsgi_input, WsgiInput)
    assert wsgi_input.readline() == b"hello\n"
    assert wsgi_input.read() == b"world\n"


def test_wsgi_file_wrapper_filesystem_pathsend_and_memory_fallback(tmp_path: Path) -> None:
    """Verify WsgiFileWrapper delegates real files to pathsend and iterates in-memory streams."""
    sample = tmp_path / "report.txt"
    sample.write_bytes(b"0123456789abcdef")

    def file_wsgi_app(environ: Any, start_response: Any) -> Any:
        start_response("200 OK", [("Content-Type", "text/plain")])
        wrapper_cls = environ["wsgi.file_wrapper"]
        fh = sample.open("rb")
        return wrapper_cls(fh, 4)

    adapter = WsgiAdapter(file_wsgi_app)
    environ = WsgiEnviron.from_parts(method="GET", path="/download")
    resp = adapter.run_environ(environ, request_id=7)
    assert resp.status == 200
    assert resp.body == b"0123456789abcdef"
    header_map = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in resp.headers}
    assert header_map["x-vertebra-sendfile"] == str(sample)

    def memory_wsgi_app(environ: Any, start_response: Any) -> Any:
        start_response("200 OK", [("Content-Type", "text/plain")])
        wrapper_cls = environ["wsgi.file_wrapper"]
        mem = io.BytesIO(b"in-memory-payload")
        return wrapper_cls(mem, 6)

    mem_adapter = WsgiAdapter(memory_wsgi_app)
    mem_environ = WsgiEnviron.from_parts(method="GET", path="/mem")
    mem_resp = mem_adapter.run_environ(mem_environ, request_id=8)
    assert mem_resp.status == 200
    assert mem_resp.body == b"in-memory-payload"

    with sample.open("rb") as fh:
        wrapped = WsgiFileWrapper(fh, 8)
        resolved = resolve_wsgi_file_wrapper(wrapped)
        assert resolved == str(sample)


def test_run_wsgi_callable_native_worker() -> None:
    """Verify native wsgi_worker.rs executes a PEP 3333 callable and returns a Response."""

    def simple_app(environ: Any, start_response: Any) -> list[bytes]:
        body = environ["wsgi.input"].read()
        start_response("201 Created", [("X-Method", environ["REQUEST_METHOD"])])
        return [b"echo:", body]

    environ = WsgiEnviron.from_parts(method="POST", path="/items", body=b"item-42")
    resp = run_wsgi_callable(simple_app, environ, 99)
    assert resp.status == 201
    assert resp.body == b"echo:item-42"
    assert resp.request_id == 99
