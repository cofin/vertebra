"""Tests for SSR execution backends."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from vertebra.framework.ssr import (
    SSRResult,
    SyncEmbeddedSSRClient,
    SyncSidecarSSRClient,
    create_ssr_client,
)


def test_ssr_result_basic() -> None:
    r = SSRResult(head=["<title>Test</title>"], body="<div>hello</div>")
    assert r.head == ["<title>Test</title>"]
    assert r.body == "<div>hello</div>"


def test_ssr_result_empty() -> None:
    r = SSRResult(head=[], body="")
    assert r.head == []
    assert r.body == ""


def test_create_ssr_client_no_entry_returns_none() -> None:
    assert create_ssr_client(entry=None) is None


def test_create_ssr_client_empty_entry_returns_none() -> None:
    assert create_ssr_client(entry="") is None


def test_create_ssr_client_native_mode_fallback_on_import_error() -> None:
    with (
        patch("vertebra.framework.ssr.SyncEmbeddedSSRClient", side_effect=ImportError),
        patch("vertebra.framework.ssr.SyncSidecarSSRClient") as mock_sidecar,
    ):
        mock_sidecar.return_value = MagicMock()
        client = create_ssr_client(mode="native", entry="/tmp/ssr.js", sidecar_port=19999)
        assert client is not None
        mock_sidecar.assert_called_once_with("/tmp/ssr.js", 19999)


def test_create_ssr_client_sidecar_mode_creates_sidecar() -> None:
    with patch("vertebra.framework.ssr.SyncSidecarSSRClient") as mock:
        mock.return_value = MagicMock()
        client = create_ssr_client(mode="sidecar", entry="/tmp/ssr.js", sidecar_port=19999)
        assert client is not None
        mock.assert_called_once_with("/tmp/ssr.js", 19999)


def test_create_ssr_client_native_mode_with_init_error_falls_back() -> None:
    with (
        patch("vertebra.framework.ssr.SyncEmbeddedSSRClient", side_effect=RuntimeError("boom")),
        patch("vertebra.framework.ssr.SyncSidecarSSRClient") as mock_sidecar,
    ):
        mock_sidecar.return_value = MagicMock()
        client = create_ssr_client(mode="native", entry="/tmp/ssr.js")
        assert client is not None
        mock_sidecar.assert_called_once()


def _make_sidecar_client(port: int = 13714) -> SyncSidecarSSRClient:
    """Create a SyncSidecarSSRClient without starting a process."""
    client = SyncSidecarSSRClient.__new__(SyncSidecarSSRClient)
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
    client.stop()


def test_sidecar_ssr_client_stop_already_exited() -> None:
    client = _make_sidecar_client()
    mock_proc = MagicMock()
    mock_proc.poll.return_value = 0
    client._process = mock_proc
    client.stop()
    mock_proc.terminate.assert_not_called()


def test_sidecar_ssr_client_invalidate_is_noop() -> None:
    client = _make_sidecar_client()
    client.invalidate(["file.ts"])


def test_sidecar_ssr_client_start_no_runtime_raises() -> None:
    with (
        patch("shutil.which", return_value=None),
        pytest.raises(RuntimeError, match="Node.js or Bun required"),
    ):
        SyncSidecarSSRClient("ssr.js", port=19999)


def test_native_ssr_client_with_real_pool(tmp_path: Path) -> None:
    """SyncEmbeddedSSRClient should use real SSRPool to render component and return SSRResult."""
    bundle_file = tmp_path / "ssr.js"
    bundle_file.write_text(
        """
        globalThis.__ssr_render = function(component, props) {
            return {
                head: ["<title>" + (props.title || "Default") + "</title>"],
                body: "<h1>" + component + "</h1>"
            };
        };
        """,
        encoding="utf-8",
    )
    client = SyncEmbeddedSSRClient(str(bundle_file), pool_size=2)
    try:
        res = client.render({"component": "Home", "title": "My Title"})
        assert res.head == ["<title>My Title</title>"]
        assert res.body == "<h1>Home</h1>"
        client.invalidate(["ssr.js"])
    finally:
        client.stop()
