"""Tests for banner collector module."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from vertebra.cli.banner.collector import ComponentStatus, ServerStatus, StatusCollector


class TestComponentStatus:
    """Test ComponentStatus dataclass."""

    def test_minimal_status(self) -> None:
        status = ComponentStatus(status="ready")
        assert status.status == "ready"
        assert status.version is None
        assert status.uptime_ms is None

    def test_full_status(self) -> None:
        status = ComponentStatus(
            status="ready",
            version="1.1.38",
            uptime_ms=12345,
            details={"key": "value"},
            clients=5,
            pools={"sql": 10, "redis": 5},
            channels=4,
            latency_p99_us=8,
        )
        assert status.status == "ready"
        assert status.version == "1.1.38"
        assert status.uptime_ms == 12345
        assert status.clients == 5
        assert status.pools == {"sql": 10, "redis": 5}
        assert status.channels == 4
        assert status.latency_p99_us == 8


class TestServerStatus:
    """Test ServerStatus dataclass."""

    def test_all_components(self) -> None:
        status = ServerStatus(
            bun=ComponentStatus(status="ready"),
            broker=ComponentStatus(status="starting"),
            ipc=ComponentStatus(status="error"),
        )
        assert status.bun.status == "ready"
        assert status.broker.status == "starting"
        assert status.ipc.status == "error"


class TestStatusCollector:
    """Test StatusCollector class."""

    def test_init_sets_base_url(self) -> None:
        collector = StatusCollector(host="localhost", port=3000)
        assert collector.base_url == "http://localhost:3000"

    def test_init_default_timeout(self) -> None:
        collector = StatusCollector()
        assert collector.timeout == 1.0

    def test_fetch_returns_none_on_connection_error(self) -> None:
        collector = StatusCollector()

        with patch.object(collector, "_get_client") as mock_client:
            mock_client.return_value.get.side_effect = OSError("Connection refused")
            result = collector.fetch()

        assert result is None

    def test_fetch_returns_none_without_httpx(self) -> None:
        collector = StatusCollector()

        with patch.object(collector, "_get_client", return_value=None):
            result = collector.fetch()

        assert result is None

    def test_fetch_parses_response(self) -> None:
        collector = StatusCollector()

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "bun": {"status": "ready", "version": "1.1.38"},
            "broker": {"status": "ready", "clients": 3},
            "ipc": {"status": "ready", "channels": 4},
        }

        with patch.object(collector, "_get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_client.get.return_value = mock_response
            mock_get_client.return_value = mock_client

            result = collector.fetch()

        assert result is not None
        assert result.bun.status == "ready"
        assert result.bun.version == "1.1.38"
        assert result.broker.status == "ready"
        assert result.ipc.status == "ready"
        assert result.ipc.channels == 4

    def test_context_manager(self) -> None:
        with StatusCollector() as collector:
            assert collector.base_url == "http://127.0.0.1:8000"

    def test_close_clears_client(self) -> None:
        collector = StatusCollector()
        collector._client = MagicMock()

        collector.close()

        assert collector._client is None


class TestStatusCollectorParseComponent:
    """Test the _parse_component method."""

    def test_parses_minimal_data(self) -> None:
        collector = StatusCollector()
        data = {"status": "ready"}

        result = collector._parse_component(data)

        assert result.status == "ready"
        assert result.version is None

    def test_parses_full_data(self) -> None:
        collector = StatusCollector()
        data = {
            "status": "ready",
            "version": "1.0.0",
            "uptime_ms": 5000,
            "clients": 3,
            "pools": {"sql": 10},
            "channels": 4,
        }

        result = collector._parse_component(data)

        assert result.status == "ready"
        assert result.version == "1.0.0"
        assert result.uptime_ms == 5000
        assert result.clients == 3
        assert result.pools == {"sql": 10}
        assert result.channels == 4

    def test_handles_missing_status(self) -> None:
        collector = StatusCollector()
        data: dict = {}

        result = collector._parse_component(data)

        assert result.status == "unknown"


class TestStatusCollectorAsync:
    """Test async methods of StatusCollector."""

    @pytest.mark.anyio
    async def test_fetch_async_returns_none_without_httpx(self) -> None:
        collector = StatusCollector()

        # Mock the import to raise ImportError
        import sys
        with patch.dict(sys.modules, {"httpx": None}):
            with patch("builtins.__import__", side_effect=ImportError):
                result = await collector.fetch_async()

        assert result is None

    @pytest.mark.anyio
    async def test_fetch_async_returns_none_on_connection_error(self) -> None:
        # Import httpx here to patch it
        import httpx

        collector = StatusCollector()

        mock_client = AsyncMock()
        mock_client.__aenter__.return_value.get.side_effect = OSError("Connection refused")

        with patch.object(httpx, "AsyncClient", return_value=mock_client):
            result = await collector.fetch_async()

        assert result is None

    @pytest.mark.anyio
    async def test_fetch_async_parses_response(self) -> None:
        # Import httpx here to patch it
        import httpx

        collector = StatusCollector()

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "bun": {"status": "ready", "version": "1.1.38"},
            "broker": {"status": "ready", "clients": 3},
            "ipc": {"status": "ready", "channels": 4},
        }

        mock_client = AsyncMock()
        mock_client.__aenter__.return_value.get.return_value = mock_response

        with patch.object(httpx, "AsyncClient", return_value=mock_client):
            result = await collector.fetch_async()

        assert result is not None
        assert result.bun.status == "ready"
        assert result.bun.version == "1.1.38"
        assert result.broker.status == "ready"
        assert result.ipc.status == "ready"
        assert result.ipc.channels == 4

    @pytest.mark.anyio
    async def test_fetch_async_handles_json_error(self) -> None:
        # Import httpx here to patch it
        import httpx

        collector = StatusCollector()

        mock_response = MagicMock()
        mock_response.json.side_effect = ValueError("Invalid JSON")

        mock_client = AsyncMock()
        mock_client.__aenter__.return_value.get.return_value = mock_response

        with patch.object(httpx, "AsyncClient", return_value=mock_client):
            result = await collector.fetch_async()

        assert result is None


class TestStatusCollectorGetClient:
    """Test _get_client method."""

    def test_get_client_caches_client(self) -> None:
        # Import httpx here to patch it
        import httpx

        collector = StatusCollector()

        with patch.object(httpx, "Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            # First call should create client
            client1 = collector._get_client()
            # Second call should return cached client
            client2 = collector._get_client()

            assert client1 is client2
            assert mock_client_class.call_count == 1

    def test_get_client_returns_none_without_httpx(self) -> None:
        collector = StatusCollector()

        # Mock the import to raise ImportError
        import sys
        with patch.dict(sys.modules, {"httpx": None}):
            with patch("builtins.__import__", side_effect=ImportError):
                client = collector._get_client()

        assert client is None

    def test_fetch_handles_value_error(self) -> None:
        collector = StatusCollector()

        with patch.object(collector, "_get_client") as mock_client:
            mock_client.return_value.get.return_value.json.side_effect = ValueError("Bad JSON")
            result = collector.fetch()

        assert result is None

    def test_fetch_handles_key_error(self) -> None:
        collector = StatusCollector()

        with patch.object(collector, "_get_client") as mock_client:
            # Return malformed data that will cause KeyError during parsing
            mock_client.return_value.get.return_value.json.return_value = {"bad": "data"}
            # This should not raise, just return None
            result = collector.fetch()

        # Should handle the error and return None or a partial result
        # Based on the implementation, it should return a ServerStatus with defaults
        assert result is not None
