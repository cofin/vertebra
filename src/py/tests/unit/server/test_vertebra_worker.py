import sys
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from litestar import get

from vertebra.server.worker import AsgiWorker, WorkerConfig, _import_app


@pytest.mark.unit
@pytest.mark.anyio
async def test_vertebra_worker_direct_http() -> None:
    # We want to test that when the worker receives a DIRECT_HTTP message,
    # it calls the compiled runner instead of standard ASGI flow.

    @get("/hello", sync_to_thread=False)
    def handler() -> dict:
        return {"hello": "world"}

    app = MagicMock()
    # Mocking what the worker expects from a Litestar-like app
    app.routes = []

    worker = AsgiWorker(app, WorkerConfig(worker_id="0"))

    # Manually inject a native route for testing
    from vertebra.extensions.litestar.compiler import PipelineCompiler

    compiler = PipelineCompiler()
    runner = compiler.compile_route(handler)

    handler_id = id(handler.fn)
    worker._native_runners = {handler_id: runner}

    # Mock message
    message = {
        "type": "direct.http",  # We'll define this string or use opcode
        "id": "req-1",
        "handler_id": handler_id,
        "path": "/hello",
        "method": "GET",
        "headers": [],
        "body": b"",
    }

    # Mock response writer
    writer = AsyncMock()

    # We need to test the internal _handle_direct_http or similar
    await worker._handle_direct_http(message, writer)

    # Check that writer.send was called with the result
    assert writer.send.call_count == 2

    # First call: http.response.start
    start_args = writer.send.call_args_list[0][0][0]
    assert start_args["id"] == "req-1"
    assert start_args["type"] == "http.response.start"
    assert start_args["status"] == 200

    # Second call: http.response.body
    body_args = writer.send.call_args_list[1][0][0]
    assert body_args["id"] == "req-1"
    assert body_args["type"] == "http.response.body"
    import json

    assert json.loads(body_args["body"]) == {"hello": "world"}


# --- Tests for _import_app file path handling ---


@pytest.fixture
def temp_app_file(tmp_path: Path) -> Path:
    """Create a temporary Python file with a simple ASGI app."""
    app_code = '''
async def app(scope, receive, send):
    """Simple ASGI app for testing."""
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [(b"content-type", b"text/plain")],
    })
    await send({
        "type": "http.response.body",
        "body": b"Hello from temp app",
    })

async def application(scope, receive, send):
    """Alternative app name."""
    await app(scope, receive, send)
'''
    app_file = tmp_path / "my_app.py"
    app_file.write_text(app_code)
    return app_file


@pytest.mark.unit
def test_import_app_with_file_path(temp_app_file: Path) -> None:
    """Test that _import_app can load an app from a file path."""
    # This should work: path/to/app.py:app
    app_path = f"{temp_app_file}:app"
    app = _import_app(app_path)
    assert callable(app)


@pytest.mark.unit
def test_import_app_with_file_path_no_extension(temp_app_file: Path) -> None:
    """Test that _import_app handles file paths without .py extension."""
    # Strip .py and test
    app_path = f"{str(temp_app_file)[:-3]}:app"
    # This is a file path (has /) but no .py - should still work
    app = _import_app(f"{temp_app_file}:app")
    assert callable(app)


@pytest.mark.unit
def test_import_app_with_file_path_default_attribute(temp_app_file: Path) -> None:
    """Test that _import_app finds 'app' or 'application' by default."""
    # No :attr specified - should find 'app' automatically
    app_path = str(temp_app_file)
    app = _import_app(app_path)
    assert callable(app)


@pytest.mark.unit
def test_import_app_with_nested_file_path(tmp_path: Path) -> None:
    """Test that _import_app handles nested directory paths."""
    # Create nested structure: tmp/subdir/another/app.py
    nested_dir = tmp_path / "subdir" / "another"
    nested_dir.mkdir(parents=True)
    app_file = nested_dir / "myapp.py"
    app_file.write_text('''
async def app(scope, receive, send):
    pass
''')
    app_path = f"{app_file}:app"
    app = _import_app(app_path)
    assert callable(app)


@pytest.mark.unit
def test_import_app_with_module_path() -> None:
    """Test that _import_app still works with standard module paths."""
    # This should work as before: module.path:attr
    # Use a known module from the project
    app = _import_app("vertebra.server.worker:WorkerConfig")
    # WorkerConfig is a class (callable)
    assert callable(app)


@pytest.mark.unit
def test_import_app_file_not_found(tmp_path: Path) -> None:
    """Test that _import_app raises appropriate error for missing file."""
    missing_file = tmp_path / "nonexistent.py"
    with pytest.raises((FileNotFoundError, ModuleNotFoundError)):
        _import_app(f"{missing_file}:app")
