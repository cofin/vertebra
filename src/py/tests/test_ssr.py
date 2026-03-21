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


class TestSsrResult:
    def test_basic(self) -> None:
        r = SsrResult(head=["<title>Test</title>"], body="<div>hello</div>")
        assert r.head == ["<title>Test</title>"]
        assert r.body == "<div>hello</div>"

    def test_empty(self) -> None:
        r = SsrResult(head=[], body="")
        assert r.head == []
        assert r.body == ""


# ---------------------------------------------------------------------------
# create_ssr_client factory
# ---------------------------------------------------------------------------


class TestCreateSsrClient:
    def test_no_entry_returns_none(self) -> None:
        assert create_ssr_client(entry=None) is None

    def test_empty_entry_returns_none(self) -> None:
        assert create_ssr_client(entry="") is None

    def test_native_mode_fallback_on_import_error(self) -> None:
        with (
            patch("vertebra.framework._ssr._backends.NativeSsrClient", side_effect=ImportError),
            patch("vertebra.framework._ssr._backends.SidecarSsrClient") as mock_sidecar,
        ):
            mock_sidecar.return_value = MagicMock()
            client = create_ssr_client(mode="native", entry="/tmp/ssr.js", sidecar_port=19999)
            assert client is not None
            mock_sidecar.assert_called_once_with("/tmp/ssr.js", 19999)

    def test_sidecar_mode_creates_sidecar(self) -> None:
        with patch("vertebra.framework._ssr._backends.SidecarSsrClient") as mock:
            mock.return_value = MagicMock()
            client = create_ssr_client(mode="sidecar", entry="/tmp/ssr.js", sidecar_port=19999)
            assert client is not None
            mock.assert_called_once_with("/tmp/ssr.js", 19999)

    def test_native_mode_with_init_error_falls_back(self) -> None:
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


class TestSidecarSsrClient:
    def _make_client(self, port: int = 13714) -> SidecarSsrClient:
        """Create a SidecarSsrClient without starting a process."""
        client = SidecarSsrClient.__new__(SidecarSsrClient)
        client._port = port
        client._process = None
        client._entry = "ssr.js"
        return client

    def test_base_url(self) -> None:
        client = self._make_client()
        assert client._base_url == "http://127.0.0.1:13714"

    def test_base_url_custom_port(self) -> None:
        client = self._make_client(port=9999)
        assert client._base_url == "http://127.0.0.1:9999"

    def test_stop_no_process(self) -> None:
        client = self._make_client()
        client.stop()  # should not raise

    def test_stop_already_exited(self) -> None:
        client = self._make_client()
        mock_proc = MagicMock()
        mock_proc.poll.return_value = 0  # already exited
        client._process = mock_proc
        client.stop()
        mock_proc.terminate.assert_not_called()

    def test_invalidate_is_noop(self) -> None:
        client = self._make_client()
        client.invalidate(["file.ts"])  # should not raise

    def test_start_no_runtime_raises(self) -> None:
        with (
            patch("shutil.which", return_value=None),
            pytest.raises(RuntimeError, match="Node.js or Bun required"),
        ):
            SidecarSsrClient("ssr.js", port=19999)
