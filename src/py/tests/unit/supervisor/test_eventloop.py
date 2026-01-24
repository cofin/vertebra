"""Tests for Supervisor compatibility with custom event loops (uvloop)."""

import sys
import unittest.mock

import pytest

try:
    import uvloop
except ImportError:
    uvloop = None

from vertebra.supervisor import Supervisor


@pytest.mark.skipif(uvloop is None, reason="uvloop not installed")
def test_supervisor_with_uvloop() -> None:
    """Verify Supervisor instantiates correctly when uvloop is active."""
    # Install uvloop
    if uvloop:
        uvloop.install()

    # Mock the native supervisor to avoid spawning actual processes
    with unittest.mock.patch("vertebra.supervisor.NativeSupervisor") as mock_native_cls:
        mock_native_instance = mock_native_cls.return_value

        supervisor = Supervisor()
        supervisor.start("bun", ["run", "server.ts"])

        mock_native_cls.assert_called_once()
        mock_native_instance.start.assert_called_once()

        # Verify arguments passed to native start
        # engine_path, engine_args, python_executable, config, workers
        args = mock_native_instance.start.call_args[0]
        assert args[0] == "bun"
        assert args[1] == ["run", "server.ts"]
        assert args[2] == sys.executable
        assert args[3] == {}  # config
        assert args[4] == 1  # workers


def test_supervisor_args_passing() -> None:
    """Verify arguments are passed correctly to native supervisor."""
    with unittest.mock.patch("vertebra.supervisor.NativeSupervisor") as mock_native_cls:
        mock_native_instance = mock_native_cls.return_value

        config = {"foo": "bar"}
        supervisor = Supervisor(config)
        supervisor.start("my-engine", ["--flag"], workers=4)

        mock_native_instance.start.assert_called_once_with("my-engine", ["--flag"], sys.executable, config, 4)
