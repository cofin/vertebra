"""Tests for SSR execution backends."""

from unittest.mock import MagicMock, patch

import pytest

from vertebra.framework._ssr._backends import (
    SidecarSsrClient,
    SsrResult,
    create_ssr_client,
)

# ---------------------------------------------------------------------------
# SsrResult
# ---------------------------------------------------------------------------


def test_ssr_result_basic() -> None:
    r = SsrResult(head=["<title>Test</title>"], body="<div>hello</div>")
    assert r.head == ["<title>Test</title>"]
    assert r.body == "<div>hello</div>"


def test_ssr_result_empty() -> None:
    r = SsrResult(head=[], body="")
    assert r.head == []
    assert r.body == ""


# ---------------------------------------------------------------------------
# create_ssr_client factory
# ---------------------------------------------------------------------------


def test_create_ssr_client_no_entry_returns_none() -> None:
    assert create_ssr_client(entry=None) is None


def test_create_ssr_client_empty_entry_returns_none() -> None:
    assert create_ssr_client(entry="") is None


def test_create_ssr_client_native_mode_fallback_on_import_error() -> None:
    with (
        patch("vertebra.framework._ssr._backends.NativeSsrClient", side_effect=ImportError),
        patch("vertebra.framework._ssr._backends.SidecarSsrClient") as mock_sidecar,
    ):
        mock_sidecar.return_value = MagicMock()
        client = create_ssr_client(mode="native", entry="/tmp/ssr.js", sidecar_port=19999)
        assert client is not None
        mock_sidecar.assert_called_once_with("/tmp/ssr.js", 19999)


def test_create_ssr_client_sidecar_mode_creates_sidecar() -> None:
    with patch("vertebra.framework._ssr._backends.SidecarSsrClient") as mock:
        mock.return_value = MagicMock()
        client = create_ssr_client(mode="sidecar", entry="/tmp/ssr.js", sidecar_port=19999)
        assert client is not None
        mock.assert_called_once_with("/tmp/ssr.js", 19999)


def test_create_ssr_client_native_mode_with_init_error_falls_back() -> None:
    with (
        patch("vertebra.framework._ssr._backends.NativeSsrClient", side_effect=RuntimeError("boom")),
        patch("vertebra.framework._ssr._backends.SidecarSsrClient") as mock_sidecar,
    ):
        mock_sidecar.return_value = MagicMock()
        client = create_ssr_client(mode="native", entry="/tmp/ssr.js")
        assert client is not None
        mock_sidecar.assert_called_once()


# ---------------------------------------------------------------------------
# SidecarSsrClient
# ---------------------------------------------------------------------------


def _make_sidecar_client(port: int = 13714) -> SidecarSsrClient:
    """Create a SidecarSsrClient without starting a process."""
    client = SidecarSsrClient.__new__(SidecarSsrClient)
    client._port = port
    client._process = None
    client._entry = "ssr.js"
    return client


def test_sidecar_ssr_client_base_url() -> None:
    client = _make_sidecar_client()
    assert client._base_url == "http://127.0.0.1:13714"


def test_sidecar_ssr_client_base_url_custom_port() -> None:
    client = _make_sidecar_client(port=9999)
    assert client._base_url == "http://127.0.0.1:9999"


def test_sidecar_ssr_client_stop_no_process() -> None:
    client = _make_sidecar_client()
    client.stop()  # should not raise


def test_sidecar_ssr_client_stop_already_exited() -> None:
    client = _make_sidecar_client()
    mock_proc = MagicMock()
    mock_proc.poll.return_value = 0  # already exited
    client._process = mock_proc
    client.stop()
    mock_proc.terminate.assert_not_called()


def test_sidecar_ssr_client_invalidate_is_noop() -> None:
    client = _make_sidecar_client()
    client.invalidate(["file.ts"])  # should not raise


def test_sidecar_ssr_client_start_no_runtime_raises() -> None:
    with (
        patch("shutil.which", return_value=None),
        pytest.raises(RuntimeError, match="Node.js or Bun required"),
    ):
        SidecarSsrClient("ssr.js", port=19999)
